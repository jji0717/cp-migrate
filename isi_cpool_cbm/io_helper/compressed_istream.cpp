#include "compressed_istream.h"

// to access the guid of the node
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_ilog/ilog.h>
#include <dirent.h>
#include <vector>
static const char *strm_dir = "/ifs/.ifsvar/modules/cloud/tmp";
static char node_id_str[IFSCONFIG_GUID_SIZE * 2 + 1];

void cloudpools_cleanup_compression_tmp_file()
{
	struct fmt FMT_INIT_CLEAN(full_path);
	DIR *dirp = NULL;
	int dir_fd = -1;
	const size_t ent_cnt = 64;
	struct dirent dir_ent[ent_cnt];
	struct stat stat_ent[ent_cnt];
	unsigned stat_cnt = 0;
	int nread = 0;

	// open dir, create if not exists
	dirp = opendir(strm_dir);
	if (!dirp && errno == ENOENT) {
		int res = mkdir(strm_dir, 0600);
		if (res != 0) {
			ilog(IL_ERR, "failed to create dir: %s %s",
			    strm_dir, strerror(errno));
			return;
		}
		dirp = opendir(strm_dir);
	}

	if (!dirp || (dir_fd = dirfd(dirp)) < 0) {
		ilog(IL_ERR, "failed to open dir: %s %s",
		    strm_dir, strerror(errno));

		if (dirp)
			closedir(dirp);
		return;
	}

	// read through the files and unlink it
	do {
		struct dirent *ptr = dir_ent;
		stat_cnt = ent_cnt;
		nread = readdirplus(dir_fd, 0, NULL, 0, (char *)dir_ent,
		    ent_cnt * sizeof(dir_ent[0]), &stat_cnt, stat_ent, NULL);

		for (size_t i = 0; i < stat_cnt;
		    ++i, ptr = (struct dirent *)((char *)ptr + ptr->d_reclen)) {
			if (ptr->d_namlen == 0 ||
			    strcmp(ptr->d_name, ".") == 0 ||
			    strcmp(ptr->d_name, "..") == 0 ||
			    strncmp(ptr->d_name, node_id_str,
			        strlen(node_id_str)) == 0) {
				continue;
			}

			fmt_clean(&full_path);
			fmt_print(&full_path, "%s/%s", strm_dir, ptr->d_name);
			unlink(fmt_string(&full_path));
		}
	} while (stat_cnt != 0);

	closedir(dirp);
}

namespace
{
// 16 KB buffer for compression
const size_t BUF_SZ = 16 * 1024;
// above 1M goes to tmp file
const size_t CACHE_THRESHOLD = 1024 * 1024;

size_t cache_threshold = CACHE_THRESHOLD;


/*
 * all the efforts to clean up the stream tmp files
 */
struct tmp_file_cleaner {
	tmp_file_cleaner()
	{
		// initialize the node guid string
		node_id = get_handling_node_id();
		bzero(node_id_str, sizeof(node_id_str));

		// libisi_cpool_cbm.so referenced by python script
		// which runs when they configure the cluster, at which time
		// cluster is not ready
		if (!node_id)
			return;

		for (int i = 0; i < IFSCONFIG_GUID_SIZE; ++i) {
			node_id_str[i * 2] = (node_id[i] >> 4);
			node_id_str[i * 2 + 1] = (node_id[i] & 0x0F);

			if (node_id_str[i * 2] < 10)
				node_id_str[i * 2] += '0';
			else
				node_id_str[i * 2] += 'a' - 10;

			if (node_id_str[i * 2 + 1] < 10)
				node_id_str[i * 2 + 1] += '0';
			else
				node_id_str[i * 2 + 1] += 'a' - 10;
		}
	}
	~tmp_file_cleaner()
	{
	}

	const uint8_t *node_id;
} cleaner_;
}

compressed_istream::compressed_istream(const std::string &fname,
    idata_stream &istrm, size_t len)
    : tmpfd(-1), length(0)
{
	int err = Z_OK;

	bzero(&c_strm, sizeof c_strm);
	// initialize zlib data structure
	c_strm.zalloc = (alloc_func) NULL;
	c_strm.zfree = (free_func) NULL;
	c_strm.opaque = (voidpf) NULL;

	err = deflateInit(&c_strm, Z_DEFAULT_COMPRESSION);
	if (err != Z_OK)
		throw isi_exception("Unable to initialize zlib");

	if (len > cache_threshold) {
		struct fmt FMT_INIT_CLEAN(full_path);
		fmt_print(&full_path, "%s/%s_%s_XXXXXX",
		    strm_dir, node_id_str, fname.c_str());

		tmpfd = mkstemp((char *) fmt_string(&full_path));

		if (tmpfd < 0)
			throw isi_exception("failed to create tmp file");

		tmp_fpath = fmt_string(&full_path);

		compress_to_file(istrm, len);

		// reset the file pointer for read
		lseek(tmpfd, 0, SEEK_SET);
	}
	else
		compress_to_mem(istrm, len);

	this->compressed = (c_strm.total_out < len);
	this->length = c_strm.total_out;

	istrm.reset();
}

compressed_istream::~compressed_istream()
{
	deflateEnd(&c_strm);

	if (tmpfd >= 0) {
		close(tmpfd);
		unlink(tmp_fpath.c_str());
	}
}

int
compressed_istream::deflate_to_file(int flush, void *buf, int buf_sz)
{
	int bytes_avail = 0;
	int res = Z_OK;

	// compress the data
	do {
		c_strm.next_out = (Byte *) buf;
		c_strm.avail_out = (uInt) buf_sz;

		if ((res = deflate(&c_strm, flush)) < 0)
			throw isi_exception("Failed to compress data");

		bytes_avail = buf_sz - c_strm.avail_out;

		if (write(tmpfd, buf, bytes_avail) != bytes_avail)
			throw isi_exception("Failed to write temp file");
	} while (c_strm.avail_out == 0 && res >= 0);

	return res;
}

void
compressed_istream::compress_to_file(idata_stream &istrm, size_t len)
{
	std::vector<char> ibuf(BUF_SZ);
	// allocate output buffer here so to reuse it in the loop
	std::vector<char> obuf(BUF_SZ);
	size_t ilen = 0;

	while (len > 0 &&
	    (ilen = istrm.read(ibuf.data(), MIN(BUF_SZ, len))) > 0) {
		c_strm.next_in   = (Byte *) ibuf.data();
		c_strm.avail_in  = (uInt) ilen;
		len -= ilen;

		if (deflate_to_file(Z_NO_FLUSH, obuf.data(), BUF_SZ) < 0)
			throw isi_exception("Unable to deflate data");
	}
	// flush out all the data
	if (deflate_to_file(Z_FINISH, obuf.data(), BUF_SZ) != Z_STREAM_END)
		throw isi_exception("Unable to deflate data");
}

void
compressed_istream::compress_to_mem(idata_stream &istrm, size_t len)
{
	std::vector<char> ibuf(MIN(BUF_SZ, len));
	size_t total_ilen = 0;

	uLong bound_sz = deflateBound(&c_strm, len);
	compressed_buf.reset(new isi_cbm_buffer(bound_sz));

	c_strm.avail_out = bound_sz;
	c_strm.next_out = (Byte *) compressed_buf->ptr();

	while (total_ilen < len) {
		size_t bytes = MIN(ibuf.size(), len - total_ilen);
		size_t ilen = istrm.read(ibuf.data(), bytes);

		if (ilen <= 0) {
			ilog(IL_NOTICE, "expect %ld bytes, actual %ld", bytes, ilen);
			break;
		}

		c_strm.next_in = (Byte *) ibuf.data();
		c_strm.avail_in = ilen;
		total_ilen += ilen;

		if (deflate(&c_strm, Z_NO_FLUSH) < 0)
			throw isi_exception("Unable to deflate data");
	}

	if (total_ilen > 0 && deflate(&c_strm, Z_FINISH) < 0)
		throw isi_exception("Unable to deflate data");

	compressed_buf->set_data_size(c_strm.total_out);
}

// for unit test purpose to simulate the tmp file implementation
void
compressed_istream::set_threshold(int sz)
{
	if (sz >= 0)
		cache_threshold = sz;
}

void
compressed_istream::reset_threshold()
{
	cache_threshold = CACHE_THRESHOLD;
}
