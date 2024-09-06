
#include "compress_filter.h"
#include <isi_ilog/ilog.h>

#define BUF_SZ (16 * 1024)

compress_filter::compress_filter(odata_stream &ostrm)
    : ostrm(ostrm), local_buf(BUF_SZ)
{
	ASSERT(local_buf.data(), "out of memory");

	// initialization
	d_strm.zalloc = (alloc_func) NULL;
	d_strm.zfree  = (free_func) NULL;
	d_strm.opaque = (voidpf) NULL;

	if (inflateInit(&d_strm) != Z_OK)
		throw isi_exception("Unable to init zlib");
}

/*
 * decompress the data in buffer, and write back to the output
 */
size_t
compress_filter::write(void *buff, size_t len)
{
	d_strm.next_in = (Byte *) buff;
	d_strm.avail_in = (uInt)len;

	while (d_strm.avail_in > 0) {
		if (!do_inflate()) {
			ilog(IL_ERR, "Abort write back due to inflate failure\n");
			return CODE_ABORT;
		}
	}

	// indicate all the data is taken
	return len;
}

bool
compress_filter::reset()
{
	inflateEnd(&d_strm);

	if (inflateInit(&d_strm) != Z_OK) {
		ilog(IL_NOTICE, "Unable to init zlib");
		return false;
	}

	return ostrm.reset();
}

void
compress_filter::flush()
{
	// make sure all the input data is decompressed without being
	// buffered
	do {
		if (!do_inflate()) {
			inflateEnd(&d_strm);
			throw isi_exception("failed to inflate data");
		}
	} while (d_strm.avail_out == 0);

	inflateEnd(&d_strm);
}

bool
compress_filter::do_inflate()
{
	int err = Z_OK;
	size_t bytes = 0;

	d_strm.next_out = (Byte *)local_buf.data();
	d_strm.avail_out = local_buf.size();

	err = inflate(&d_strm, Z_SYNC_FLUSH);

	if (err != Z_OK && err != Z_STREAM_END) {
		ilog(IL_ERR, "failed to inflate data");
		return false;
	}

	bytes = local_buf.size() - d_strm.avail_out;

	if (bytes > 0 && ostrm.write(local_buf.data(), bytes) != bytes) {
		ilog(IL_ERR, "failed to write %ld bytes", bytes);
		return false;
	}

	return true;
}
