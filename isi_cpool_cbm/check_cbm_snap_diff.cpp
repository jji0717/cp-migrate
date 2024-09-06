#include <ifs/ifs_constants.h>

#include "ifs/ifs_lin_open.h"

#include <fcntl.h>
#include <stdio.h>
#include <check.h>
#include <sys/types.h>
#include <sys/extattr.h>

#include <vector>

#include <isi_util_cpp/scoped_lock.h>
#include <isi_snapshot/snapshot.h>
#include <isi_cpool_security/cpool_protect.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_gc.h"
#include "isi_cbm_invalidate.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_sync.h"
#include "isi_cbm_snap_diff_int.h"

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF

#define TST_FILE_PATH "/ifs/test_cbm_snap_diff.txt"

using namespace isi_cloud;

#define KNOWN_LEAK 0

static bool ran_only_g = true;
static bool policies = true;  // create or clean

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(ran_only_g, policies);
	cbm_test_common_setup(ran_only_g, policies);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");

	cpool_fd_store::getInstance();
	isi_cbm_get_wbi(isi_error_suppress());
	isi_cbm_get_coi(isi_error_suppress());
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(ran_only_g, policies);
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_cbm_snap_diff,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 60,
    .fixture_timeout = 1200);

enum test_file_ops {
	TFO_NOOP,
	TFO_CREATE_STUB,
	TFO_CREATE_REG,
	TFO_RECALL,
	TFO_STUB_REG
};

static void
do_sync(uint64_t lin, uint64_t snap_id)
{
	// system ("isi_test_cpool_cbm --dump-cache " TST_FILE_PATH);
	struct isi_error *error = NULL;
	isi_cbm_sync_option opt =
			    {blocking: false,
			     skip_settle: true};
	isi_cbm_sync_opt(lin, snap_id, NULL, NULL, NULL, opt, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));
}

static void
do_invalidate(uint64_t lin, uint64_t snap_id)
{
	struct isi_error *error = NULL;
	isi_cbm_invalidate_cached_file_opt(lin, snap_id, NULL,
	    NULL, NULL, true, false, NULL, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));
}


static void
write_content(int fd, bool is_stub, char write_char, off_t start_off, size_t size)
{
	const int buffer_size = 8192;
	char buffer[buffer_size];
	struct isi_cbm_file *file = NULL;
	struct isi_error *error = NULL;

	memset(buffer, write_char, sizeof(buffer));
	size_t written;
	for (written = 0; written < size; ) {
		size_t write_size = (size - written) % buffer_size;
		write_size = (write_size == 0 ? buffer_size : write_size);
		ssize_t wrote;

		if (is_stub) {
			if (!file) {
				file = isi_cbm_file_get(fd, &error);
				fail_if(error, "isi_cbm_file_get() failed: %#{}",
				    isi_error_fmt(error));
			}

			wrote = isi_cbm_file_write(file, buffer, start_off,
			     write_size, &error);
			fail_if(error, "isi_cbm_file_get() failed: %#{}",
			    isi_error_fmt(error));
		} else {
			wrote = pwrite(fd, buffer, write_size, start_off);
			fail_if(wrote == -1, "write failure, errno %d", errno);
		}
		written += wrote;
		start_off += wrote;
	}
	ASSERT(written == size);

	if (file) {
		isi_cbm_file_close(file, &error);
		fail_if(error, "error: %s (%d)", strerror(errno), errno);
	}
}

static void
append_content(uint64_t lin, char write_char, size_t size)
{
	struct stat sb = {0};
	int fd, status;
	bool is_stub;
	off_t start_off;

	fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDWR);
	fail_if(fd < 0, "ifs_lin_open() failed with error: %s (%d)",
	    strerror(errno), errno);
	status = fstat(fd, &sb);
	fail_if((status == -1), "Stat of file failed with error: %s (%d)",
	    strerror(errno), errno);

	is_stub = (sb.st_flags & SF_FILE_STUBBED);
	start_off = sb.st_size;
	write_content(fd, is_stub, write_char, start_off, size);
	close(fd);
	if (is_stub)
		do_sync(lin, HEAD_SNAPID);
}



static void
truncate_content(uint64_t lin, size_t size)
{
	struct stat sb = {0};
	int fd, status, ret;
	bool is_stub;
	struct isi_error *error = NULL;

	fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDWR);
	fail_if(fd == -1, "ifs_lin_open() failed with error: %s (%d)",
	    strerror(errno), errno);
	status = fstat(fd, &sb);
	fail_if((status == -1),
	    "Stat of file failed with error: %s (%d)",
	    strerror(errno), errno);

	is_stub = (sb.st_flags & SF_FILE_STUBBED);

	if (is_stub) {
		isi_cbm_file_truncate(lin, HEAD_SNAPID, size, &error);
		fail_if(error, "isi_cbm_file_truncate() failed: %#{}",
		    isi_error_fmt(error));
		do_sync(lin, HEAD_SNAPID);
	} else {
		ret = ftruncate(fd, size);
		fail_if(ret == -1, "ftruncate failed errno %d", errno);
	}
	close(fd);
}

static uint64_t
create_input_file(const char *path, size_t filesize,
    test_file_ops op, char write_char, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat sb = {0};
	int fd, status;
	uint64_t lin;

	ASSERT(path);
	ASSERT(error_out);

	// create or open file at path
	if (op == TFO_CREATE_STUB || op == TFO_CREATE_REG)
		fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	else // (op == TFO_NOOP || op == TFO_RECALL || op = TFO_STUB_REG)
		fd = open(path, O_RDWR);
	fail_if((fd == -1),
	    "open of file %s with op %d failed with error: %s (%d)",
	    path, op, strerror(errno), errno);


	// if file created, write content to it
	if (op == TFO_CREATE_STUB || op == TFO_CREATE_REG) {
		write_content(fd, false, write_char, 0, filesize);
	}

	status = fstat(fd, &sb);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	lin = sb.st_ino;
	fsync(fd);
	close(fd);


	if (op == TFO_CREATE_STUB || op == TFO_STUB_REG) {
		error = cbm_test_archive_file_with_pol(lin,
		    CBM_TEST_COMMON_POLICY_RAN, false);
		fail_if(error, "Failed to archive the file: %#{}.\n",
		    isi_error_fmt(error));
	} else if (op == TFO_RECALL) {
		isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
		    &error);
		fail_if(error, "Failed to recall the file: %#{}.\n",
		    isi_error_fmt(error));

	}

	// adjust size if needed
	ssize_t add_length = filesize - sb.st_size;
	if (add_length > 0)
		append_content(lin, write_char, (size_t) add_length);
	else if (add_length < 0)
		truncate_content(lin, filesize);

	return lin;
}


static void
delete_head_file(const char *path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat sb = {0};
	int status;
	uint64_t lin;
	bool deleted = true;

	status = stat(path, &sb);
	if (status == -1) {
		error = isi_system_error_new(errno,
		    "Stat of file %s failed", path);
		goto out;
	}
	lin = sb.st_ino;

	if (unlink(path) == 0)
		deleted = true;

 out:
	isi_error_handle(error, error_out);
}




typedef std::vector<isi_cbm_file_region> test_file_regions;

typedef std::vector<snap_diff_region> test_diff_file_regions;

enum test_file_type {TFT_UNKNOWN, TFT_STUB, TFT_REGULAR};

struct test_file_input
{
	test_file_input(): filetype(TFT_UNKNOWN), filesize(0), invalidate(false),
	    sync(false), invalid_snap(false), write_char('A') {}
	void add_file_region(isi_cbm_file_region_type type, off_t start,
	    size_t length);

	test_file_type filetype;
	size_t filesize;
	test_file_regions regions;
	bool invalidate;
	bool sync;
	bool invalid_snap;
	char write_char;
};

void
test_file_input::add_file_region(isi_cbm_file_region_type type, off_t start,
    size_t length)
{
	regions.push_back(isi_cbm_file_region(type, start, length));
}




void
test_cache_read(struct isi_cbm_file &file, isi_cbm_file_region &fr,
    struct isi_error **err_out)
{
	struct isi_error *error = NULL;
	bool valid;
	off_t first_reg, last_reg;
	cpool_cache & cache =  *(file.cacheinfo);

	isi_cbm_file_open_and_or_init_cache(&file, false, true, &error);
	if (error) {
		isi_error_add_context(error, "Failed to open cache for %{}",
		    isi_cbm_file_fmt(&file));
		goto out;
	}
	valid = cache.convert_bytes_to_regions(fr.offset, fr.length,
	    &first_reg, &last_reg, &error);
	if (error) {
		isi_error_add_context(error, "Failed to convert bytes to "
		    " regions for %{}", isi_cbm_file_fmt(&file));
		goto out;
	}
	ASSERT(valid);
	// read first byte off each region so that the cache for
	// the entire input range can be considered read
	for (off_t i = first_reg; i <= last_reg; i++) {
		char buf[1];
		off_t first_off = i*cache.get_regionsize();
		size_t len = isi_cbm_file_read(&file, buf,
		    first_off, sizeof(buf),
		    CO_CACHE, &error);
		if (error) {
			isi_error_add_context(error,
			    "reading byte %ld from cache region %ld "
			    " range [%ld, %ld)", first_off, i, fr.offset, fr.length);
			goto out;
		}
		ASSERT(len == sizeof(buf));
	}

 out:
	isi_error_handle(error, err_out);
}

void
test_cache_write(struct isi_cbm_file &file, isi_cbm_file_region &fr,
    char write_char, struct isi_error **err_out)
{
	struct isi_error *error = NULL;
//	bool valid;
//	off_t first_reg, last_reg;
//	cpool_cache & cache =  *(file.cacheinfo);
	int new_fd;

	isi_cbm_file_open_and_or_init_cache(&file, false, false, &error);
	if (error) {
		isi_error_add_context(error, "Failed to open cache for %{}",
		    isi_cbm_file_fmt(&file));
		goto out;
	}
	//valid = cache.convert_bytes_to_regions(fr.offset, fr.length,
	//    &first_reg, &last_reg);
	//ASSERT(valid);
	// write first byte to each region so that the cache for
	// the entire input range can be considered written
	/* for (off_t i = first_reg; i <= last_reg; i++) {
		size_t size = cache.get_regionsize();
		off_t first_off = i*size; */
		new_fd = dup(file.fd);
		write_content(new_fd, true, write_char, fr.offset, fr.length);
		close(new_fd);  // XXX double fd close -- fix
		// first_off, size);
		/*
		char buf[1] = {write_char};

		isi_cbm_file_write(&file, buf, first_off, sizeof(buf), &error);
		if (error) {
			isi_error_add_context(error,
			    "writing byte %ld to cache region %ld "
			    " range [%ld, %ld)", first_off, i, fr.offset, fr.length);
			goto out;
		} */
	//}

 out:
	isi_error_handle(error, err_out);
}

const char *frt_c_str(isi_cbm_file_region_type frt)
{
	const char *s = NULL;

	if (frt == FRT_INVALID)
		s = "FRT_INVALID";
	else if (frt == FRT_UNCACHED)
		s = "FRT_UNCACHED";
	else if (frt == FRT_CACHE_READ)
		s = "FRT_CACHE_READ";
	else if (frt == FRT_CACHE_WRITE)
		s = "FRT_CACHE_WRITE";
	else if (frt == FRT_ORDINARY)
		s = "FRT_ORDINARY";
	else
		ASSERT(0, "Unhandled file region type %d", frt);

	return s;
}

const char *sdrt_c_str(int32_t drt)
{
	const char *s = NULL;

	if (drt == RT_SPARSE)
		s = "RT_SPARSE";
	else if (drt == RT_DATA)
		s = "RT_DATA";
	else if (drt == RT_UNCHANGED)
		s = "RT_UNCHANGED";
	else
		ASSERT(0, "Unhandled  snap diff region type %d", drt);

	return s;
}


void
print_file_regions(test_file_regions &fr)
{
	test_file_regions::iterator it;

	for (it = fr.begin(); it != fr.end(); ++it) {
		printf(" \tfile region_type = %s start_offset = %ld, "
		    " byte_count = %lu\n",
		    frt_c_str(it->frt), it->offset,
		    it->length);
	}
}

void
print_diff_file_regions(test_diff_file_regions &drs)
{
	test_diff_file_regions::iterator it;

	for (it = drs.begin(); it != drs.end(); ++it) {
		printf(" \tdiff region_type = %s start_offset = %ld, "
		    " byte_count = %lu\n",
		    sdrt_c_str(it->region_type), it->start_offset,
		    it->byte_count);
	}
}


void
set_test_regions(ifs_lin_t lin, test_file_input &input, struct isi_error **err_out)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file = NULL;
	size_t num_ordinary = 0, // num_invalid = 0,
	    num_uncached = 0,
	    num_cache_read = 0, num_cache_write = 0;
	test_file_regions &regions = input.regions;
	bool invalidate = input.invalidate;
	bool sync = input.sync;
	char write_char = input.write_char;
	int flags = 0;

	// Check if we have a stub file
	file = isi_cbm_file_open(lin, HEAD_SNAPID, flags, &error);
	if (error) {
 		if (!isi_cbm_error_is_a(error, CBM_NOT_A_STUB)) {
			isi_error_add_context(error, "opening stub file");
			goto out;
		}
		isi_error_free(error);
		error = NULL;
	}

	for (test_file_regions::iterator it = regions.begin();
	    it != regions.end(); ++it) {
		switch (it->frt)
		{
			case FRT_UNCACHED:
			{
				num_uncached += 1;
				break;
			}
			case FRT_CACHE_READ:
			{
				num_cache_read += 1;
				break;
			}
			case FRT_CACHE_WRITE:
			{
				num_cache_write += 1;
				break;
			}
			//case FRT_INVALID:
			//{
				//num_invalid += 1;
				//break;
			//}
			case FRT_ORDINARY:
			{
				num_ordinary += 1;
				break;
			}
			default:
			{
				// noop
				break;
			}
		}
	}

	// There can be 1 ordinary region in a regular file; never in a stub
	// file.
	// There can be 1 invalid region in a regular or stub file; such
	// a region must always be the last region beginning at offset
	// file.size
	ASSERT(num_ordinary <= 1);
//	ASSERT(num_invalid <= 1);
	if (num_ordinary == 1) {
		ASSERT(!file);
		ASSERT((num_uncached + num_cache_read + num_cache_write) == 0);
		goto out; // no other ops needed for regular file
	}
	// In order to create arbitrary uncached regions we need to
	// start with a file that is completely invalidated
	if (num_uncached) {
		ASSERT(invalidate);
	}
	if (sync) {
		do_sync(file->lin, file->snap_id);
	}
	if (invalidate) {
		do_invalidate(file->lin, file->snap_id);
	}
	// Now, set the regions
	for (test_file_regions::iterator it = regions.begin();
	    it != regions.end(); ++it) {
		switch (it->frt)
		{
			case FRT_CACHE_READ:
			{
				test_cache_read(*file, *it, &error);
				if (error)
					goto out;
				break;
			}
			case FRT_CACHE_WRITE:
			{
				test_cache_write(*file, *it, write_char, &error);
				if (error)
					goto out;
				break;
			}
			//case FRT_INVALID:
			//{
				//isi_cbm_file_truncate(file->lin, file->snap_id,
				    //it->offset, &error);
				//if (error)
					//goto out;
				//break;
			//}
			case FRT_UNCACHED:
			case FRT_ORDINARY:
			default:
			{
				// noop
				break;
			}
		}
	}

 out:
	if (file)
		isi_cbm_file_close(file, &error);
	isi_error_handle(error, err_out);
}

bool
operator==(const struct snap_diff_region &r1, const struct snap_diff_region &r2)
{
	return (r1.region_type == r2.region_type) && (r1.start_offset ==
	    r2.start_offset) && (r1.byte_count == r2.byte_count);
}



bool
compare_diff_regions(test_diff_file_regions &diff_regions1,
    test_diff_file_regions &diff_regions2)
{
	bool same = false;
	test_diff_file_regions::iterator it1, it2;

	printf("\nExpected:\n");
	print_diff_file_regions(diff_regions1);
	printf("Found:\n");
	print_diff_file_regions(diff_regions2);

	if (!(diff_regions1.size() == diff_regions2.size()))
		goto out;

	for (it1 = diff_regions1.begin(),it2 = diff_regions2.begin();
	    it1 != diff_regions1.end() && it2 != diff_regions2.end();
	    ++it1, ++it2) {
		if (!(*it1 == *it2))
			goto out;
	}

	same = true;

 out:
	return same;
}

const size_t CACHE_REGIONSIZE = 128*1024;



struct snap_diff_region make_sdr(enum snap_diff_region_type region_type,
    off_t start_offset, size_t byte_count)
{
	struct snap_diff_region sdr = {region_type, start_offset, byte_count};
	return sdr;
}

void
test_cbm_snap_diff(test_file_input input[],
    test_diff_file_regions & expect_regions, int expect_errno = 0,
    bool local_only = false);

void
test_cbm_snap_diff(test_file_input input[],
    test_diff_file_regions & expect_regions, int expect_errno,
    bool local_only)
{
	struct isi_error *error = NULL;
	ifs_lin_t lin;
	const char *path = TST_FILE_PATH;
	test_file_ops ops[2] = {TFO_NOOP};
	int fd;

	delete_head_file(path, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	ASSERT(input[0].filetype != TFT_UNKNOWN);
	ASSERT(input[1].filetype != TFT_UNKNOWN);

	// find out how to [re]create the file for the two snaps
	if (input[0].filetype == TFT_STUB) {
		ops[0] = TFO_CREATE_STUB;
		if (input[1].filetype == TFT_REGULAR)
			ops[1] = TFO_RECALL;
		else
			ops[1] = TFO_NOOP;
	}
	if (input[0].filetype == TFT_REGULAR) {
		ops[0] = TFO_CREATE_REG;
		if (input[1].filetype == TFT_REGULAR)
			ops[1] = TFO_NOOP;
		else
			ops[1] = TFO_STUB_REG;
	}

	// create file per input, set test regions, and snap the result
	std::string snap_name[2];
	ifs_snapid_t snapid[2];
	printf("\n");
	for (int i = 0; i < 2; i++) {
		lin = create_input_file(path, input[i].filesize, ops[i],
		    input[i].write_char, &error);
		fail_if (error, "Failed to create input file: %#{}",
		    isi_error_fmt(error));
		printf("File regions snap[%d]:\n", i);
		print_file_regions(input[i].regions);
		set_test_regions(lin, input[i], &error);
		fail_if (error, "Failed to set test regions: %#{}",
		    isi_error_fmt(error));
		snap_name[i] = "test_cbm_snap_diff_";
		snap_name[i] += (i) ? "1" : "0";
		remove_snapshot(snap_name[i]);
		fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDWR);
		// flush any cache writes in coalescer
		fsync(fd);
		close(fd);
		if (input[i].invalid_snap)
			snapid[i] = INVALID_SNAPID;
		else
			snapid[i] = take_snapshot("/ifs", snap_name[i],
			    false, &error);
		fail_if (error, "Failed to snapshot the file: %#{}",
		    isi_error_fmt(error));
	}

	// diff snaps
	test_diff_file_regions found_regions;
	struct isi_cbm_snap_diff_iterator *it;
	bool done = false;
	it = isi_cbm_snap_diff_create(lin, 0, snapid[0], snapid[1], local_only);
	fail_if(!it);
	do {
		struct snap_diff_region out_region;
		bzero(&out_region, sizeof(out_region));
		errno = 0;
		int ret = isi_cbm_snap_diff_next(it, &out_region);
		int found_errno = errno;
		if (expect_errno) {
			fail_if(ret == 0);
			fail_if(expect_errno != found_errno,
			    "errno: expected %d found %d", expect_errno,
			    found_errno);
		} else {
			fail_if(ret != 0);
		}
		found_regions.push_back(out_region);
		if (out_region.byte_count == 0)
			done = true;
	} while (!done);
	isi_cbm_snap_diff_destroy(it);
	if (!expect_errno) {
		bool same = compare_diff_regions(expect_regions, found_regions);
		fail_if(!same);
	}

	// delete snaps
	for (int i = 0; i < 2; i++) {
		if (snapid[i] != INVALID_SNAPID)
			remove_snapshot(snap_name[i]);
	}
	// delete head version of stub
	delete_head_file(path, &error);
	fail_if(error, "deleting head version of %s %#{}", path,
	    isi_error_fmt(error));

}


#if 1


//TEST(test_cbm_snap_diff_ord_ord,
    //mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
//{
//}


TEST(test_cbm_snap_diff_1invalid_2invalid,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
//	const  size_t TST_FILESIZE = 2*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a single invalid region
	// snap2: same as above

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = 0;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = 0;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}


TEST(test_cbm_snap_diff_1invalid_2ordinary,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const  size_t TST_FILESIZE = 2*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a single invalid region
	// snap2: the second file as a recalled ordinary file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = 0;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = TST_FILESIZE;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1invalid_2uncached_read_write,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE, write_length = CACHE_REGIONSIZE;
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;

	// specify test file input:
	// snap1: the whole file as a single invalid region
	// invalidation (to allow uncached region in snap2 to be created)
	// snap2: first part of the file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = 0;
	tfi[1].sync = true; // XXX unnecessary
	tfi[1].invalidate = true; // so that we can add an uncached region
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[1].add_file_region(FRT_CACHE_WRITE, write_off, write_length);

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off,
	    uncached_length));
	expect_regions.push_back(make_sdr(RT_DATA, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1ordinary_2invalid,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 4*CACHE_REGIONSIZE;


	// specify test file input:
	// snap1: the whole file as a ordinary file
	// snap2: the whole file stubbed, and as a single invalid region


	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = 0;


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
//	expect_regions.push_back(make_sdr(RT_DATA, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, 0, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1uncached_read_write_2invalid,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE, write_length = CACHE_REGIONSIZE;
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;

	// specify test file input:
	// invalidation (to allow uncached in snap1 to be created)
	// snap1: first part of the file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	// sync (to commit the written region in snap1)
	// snap2: the whole file as a single invalid region
	test_file_input tfi[2];

	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].invalidate = true; // to create an uncached region below
	tfi[0].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[0].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].sync = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = 0;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_SPARSE, 0, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

#endif

TEST(test_cbm_snap_diff_1ordinary_2ordinary_t1,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	// Choose a test filesize that allows truncation to happen on
	// a IFS block boundary.  Otherwise the unchanged (RT_UNCHANGED)
	// region will be smaller than expected and the truncated block will
	// appear as part of the next (RT_DATA) region
	const  size_t TST_FILESIZE = 10*IFS_BSIZE;
	size_t truncate_length = TST_FILESIZE/2;

	// specify test file input:
	// snap1: the whole file as a ordinary file
	// snap2: truncated version of the previous file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = truncate_length;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, truncate_length));
	//expect_regions.push_back(make_sdr(RT_DATA, 0 + truncate_length,
	    //TST_FILESIZE - truncate_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, truncate_length, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1ordinary_2ordinary_t2,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	// Choose a test filesize that allows extension to happen on
	// a IFS block boundary.  Otherwise the unchanged (RT_UNCHANGED)
	// region will be smaller than expected and the block straddling the
	// addition will appear as part of the next (RT_DATA) region
	const  size_t TST_FILESIZE = 4*IFS_BSIZE;
	size_t extend_length = 2*TST_FILESIZE;

	// specify test file input:
	// snap1: the whole file as a ordinary file
	// snap2: extended version of the previous file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = extend_length;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_DATA, 0 + TST_FILESIZE,
	    extend_length - TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, extend_length, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

// XXXPD need test_cbm_snap_diff_1ordinary_2ordinary_t3 for when archiving +
// recall is done between snap1 and snap2


TEST(test_cbm_snap_diff_1ordinary_2uncached_read_write,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE, write_length = CACHE_REGIONSIZE;
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;


	// specify test file input:
	// snap1: an orfinary file
	// invalidation (to allow uncached region in snap2 to be created)
	// snap2: first part of the stubbed file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].invalidate = true;
	tfi[1].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[1].add_file_region(FRT_CACHE_WRITE, write_off, write_length);

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off,
	    uncached_length));
	expect_regions.push_back(make_sdr(RT_DATA, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1uncached_read_write_2ordinary,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE, write_length = CACHE_REGIONSIZE;
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;


	// specify test file input:

	// invalidation (to allow uncached region in snap1 to be created)
	// snap1: first part of the file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	// snap2: the whole file as a single ordinary file

	test_file_input tfi[2];

	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].invalidate = true;
	tfi[0].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[0].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = TST_FILESIZE;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off,
	    uncached_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1uncached_read_write_2ordinary_local_only,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE, write_length = CACHE_REGIONSIZE;
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;


	// specify test file input:

	// invalidation (to allow uncached region in snap1 to be created)
	// snap1: first part of the file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	// snap2: the whole file as a single ordinary file

	test_file_input tfi[2];

	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].invalidate = true;
	tfi[0].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[0].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = TST_FILESIZE;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions, 0, true); // local-only
}

#if 1

TEST(test_cbm_snap_diff_1read_invalidate,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;

	// specify test file input: the whole file as a single cache-read
	// region  in snap1 and cache invalidation
	// performed after snap1 is taken; no cache content in snap2
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].invalidate = true;

	// specify expected output test file regions: no change to the single
	// region
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}








TEST(test_cbm_snap_diff_1read_2read,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;

	// specify test file input: the whole file as a single, and,
	// same cache-read region in both snaps (snap1, snap2)
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);

	// specify expected output test file regions: no change to the single
	// region
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1read_invalidate_2read,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;

	// specify test file input: the whole file as a single, and,
	// same cache-read region in both snaps (snap1, snap2) with
	// invalidation in between
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].invalidate = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);

	// specify expected output test file regions: no change to the single
	// region
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1read_invalidate_2read_local_only,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;

	// specify test file input: the whole file as a single, and,
	// same cache-read region in both snaps (snap1, snap2) with
	// invalidation in between
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].invalidate = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);

	// specify expected output test file regions: changed single file
	// region
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions, 0, true);
}

TEST(test_cbm_snap_diff_1read_sync_2read,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;

	// specify test file input: the whole file as a single, and,
	// same cache-read region in both snaps, with sync
	// performed after snap1 is taken
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].sync = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	// tfi[1].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);

	// specify expected output test file regions: no change to the single
	// region
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}


#endif


TEST(test_cbm_snap_diff_1uncached_2uncached_read_write,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE,
	    write_length = TST_FILESIZE - (uncached_length + read_length);
	off_t uncached_off = 0, read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;


	// snap1: an orfinary file
	// snap2: first part of the stubbed file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[1].add_file_region(FRT_CACHE_WRITE, write_off, write_length);

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}



TEST(test_cbm_snap_diff_1read_2uncached_read_write,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE,
	    write_length = TST_FILESIZE - (uncached_length + read_length);
	off_t uncached_off = 0,
	    read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;

	// snap1: a cache-read file
	// invalidation (to allow uncached region in snap2 to be created)
	// snap2: first part of the stubbed file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);
	tfi[1].invalidate = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_UNCACHED, uncached_off, uncached_length);
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[1].add_file_region(FRT_CACHE_WRITE, write_off, write_length);

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1uncached_read_write_2read,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE,
	    write_length = TST_FILESIZE - (uncached_length + read_length);
	off_t uncached_off = 0,
	    read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;

	// snap1: first part of the stubbed file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	// sync
	// snap2: a cache-read file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].sync = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	// tfi[1].add_file_region(FRT_CACHE_READ, 0, TST_FILESIZE);


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}


TEST(test_cbm_snap_diff_1uncached_read_write_2uncached,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    read_length = CACHE_REGIONSIZE,
	    write_length = TST_FILESIZE - (uncached_length + read_length);
	off_t uncached_off = 0,
	    read_off = uncached_off + uncached_length,
	    write_off = read_off + read_length;

	// snap1: first part of the stubbed file as uncached, second part of the
	//        file as cache read, third part of the file as
	//        cache written
	// sync
	// invalidate (to create uncached file)
	// snap2: an uncached file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_READ, read_off, read_length);
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].sync = true;
	tfi[1].invalidate = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_DATA, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}



TEST(test_cbm_snap_diff_1write_2uncached_read_write__no_change,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    write_length = CACHE_REGIONSIZE,
	    read_length = TST_FILESIZE - (uncached_length + write_length);
	off_t uncached_off = 0, write_off = uncached_off + uncached_length,
	    read_off = write_off + write_length;

	// specify test file input:
	// snap1: a partly cache-written stub file
	// snap2: first part of stubbed file retained as uncached, second part
	//        of the file as cache written retained, third part of the file
	//        as cache read

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}


TEST(test_cbm_snap_diff_1write_2uncached_read_write__change,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE + 10;
	size_t uncached_length = CACHE_REGIONSIZE,
	    write_length = CACHE_REGIONSIZE,
	    read_length = TST_FILESIZE - (uncached_length + write_length);
	off_t uncached_off = 0, write_off = uncached_off + uncached_length,
	    read_off = write_off + write_length;

	// specify test file input:
	// snap1: a partly cache-written stub file
	// snap2: first stubbed file retained as uncached, second part modified
	//        and as a cache written region, third part of the file as
	//        cache read

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;
	tfi[1].add_file_region(FRT_CACHE_WRITE, write_off, write_length);
	tfi[1].add_file_region(FRT_CACHE_READ, read_off, read_length);

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, uncached_off, uncached_length));
	expect_regions.push_back(make_sdr(RT_DATA, write_off, write_length));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, read_off, read_length));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_stub_1invalid_snap_stub_2invalid_snap,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	// specify test file input:
	// snap1: the whole file as a single invalid snap
	// snap2: same as above

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = 0;
	tfi[0].invalid_snap = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = 0;
	tfi[1].invalid_snap = true;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;

	// test
	test_cbm_snap_diff(tfi, expect_regions, EINVAL);
}

TEST(test_cbm_snap_diff_ordinary_1invalid_snap_2invalid_snap,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
//	const  size_t TST_FILESIZE = 2*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a single invalid snap
	// snap2: same as above

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = 0;
	tfi[0].invalid_snap = true;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = 0;
	tfi[1].invalid_snap = true;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;

	// test
	test_cbm_snap_diff(tfi, expect_regions, EINVAL);
}

TEST(test_cbm_snap_diff_1invalid_snap_2ordinary,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const  size_t TST_FILESIZE = 2*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a single invalid snap
	// snap2: a regular file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].invalid_snap = true;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = TST_FILESIZE;

	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_1invalid_snap_2uncached,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3; // *CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a single invalid snap
	// snap2: a stub file

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[0].invalid_snap = true;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_DATA, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_SPARSE, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_unmodified_ordinary,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a regular file
	// snap2: same as above

	test_file_input tfi[2];
	tfi[0].filetype = TFT_REGULAR;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_REGULAR;
	tfi[1].filesize = TST_FILESIZE;


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}

TEST(test_cbm_snap_diff_unmodified_stub,
    mem_check: CK_MEM_DEFAULT, mem_hint: KNOWN_LEAK)
{
	const  size_t TST_FILESIZE = 3*CACHE_REGIONSIZE;

	// specify test file input:
	// snap1: the whole file as a stub file
	// snap2: same as above

	test_file_input tfi[2];
	tfi[0].filetype = TFT_STUB;
	tfi[0].filesize = TST_FILESIZE;
	tfi[1].filetype = TFT_STUB;
	tfi[1].filesize = TST_FILESIZE;


	// specify expected output test file regions
	test_diff_file_regions expect_regions;
	expect_regions.push_back(make_sdr(RT_UNCHANGED, 0, TST_FILESIZE));
	expect_regions.push_back(make_sdr(RT_UNCHANGED, TST_FILESIZE, 0));

	// test
	test_cbm_snap_diff(tfi, expect_regions);
}
