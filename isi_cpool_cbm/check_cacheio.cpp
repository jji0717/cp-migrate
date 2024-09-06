#include <fcntl.h>
#include <stdio.h>
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"

#include <isi_gconfig/main_gcfg.h>
#include <isi_gconfig/gconfig_unit_testing.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>

#include <check.h>

#define TEST_OUTPUT_FILE 	"/ifs/data/test_cpool_write.tmp.XXXXXX"

#define REGIONSIZE	(128*1024)  //128k
#define CHUNKSIZE	(1024*1024) //1M
#define READSIZE	REGIONSIZE

using namespace isi_cloud;

TEST_FIXTURE(suite_setup)
{
	cbm_test_common_setup(true, true);
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(true, true);
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

SUITE_DEFINE_FOR_FILE(check_cbm_cacheio,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .runner_data = NULL,
    .timeout = 60,
    .fixture_timeout = 1200);

static struct test_file_info test_files[] = {
	{"/boot/kernel.amd64/kernel.gz", "/ifs/data/_cbm_test_file.txt", REPLACE_FILE},
};

enum test_file_input_op {
    TFI_READ,
    TFI_WRITE,
    TFI_INVALIDATE,
    TFI_TRUNCATE,
};

MAKE_ENUM_FMT(test_file_input_op, enum test_file_input_op,
    ENUM_VAL(TFI_READ,		"Read"),
    ENUM_VAL(TFI_WRITE,		"Write"),
    ENUM_VAL(TFI_INVALIDATE,	"Invalidate"),
    ENUM_VAL(TFI_TRUNCATE,	"Truncate"),
);

enum test_file_correction {
	C_NONE = 0,
	C_OFF_FILE = 1,
	C_OFF_FILE_MOD_READ = 2,
	C_OFF_FILE_MOD_CHUNK = 3,
	C_SZ_MOD_READ = 4,
	C_SZ_MOD_CHUNK = 5,
};

MAKE_ENUM_FMT(test_file_correction, enum test_file_correction,
    ENUM_VAL(C_NONE,			"None"),
    ENUM_VAL(C_OFF_FILE,		"Filesize"),
    ENUM_VAL(C_OFF_FILE_MOD_READ,	"EOF - Filesize % READSIZE"),
    ENUM_VAL(C_OFF_FILE_MOD_CHUNK,	"EOF - Filesize % CHUNKSIZE"),
    ENUM_VAL(C_SZ_MOD_READ,		"Filesize % READSIZE"),
    ENUM_VAL(C_SZ_MOD_CHUNK,		"Filesize % CHUNKSIZE"),
);
	    
enum test_file_range_value {
	RNG_ERROR = -1,
	RNG_OLD = 1,
	RNG_NEW = 2,
	RNG_ZERO = 3,
};

MAKE_ENUM_FMT(test_file_range_value, enum test_file_range_value,
    ENUM_VAL(RNG_ERROR,	"Error expected"),
    ENUM_VAL(RNG_OLD,	"Original data"),
    ENUM_VAL(RNG_NEW,	"Updated data"),
    ENUM_VAL(RNG_ZERO,	"Data is Zero"),
);

struct cp_test_range {
	enum test_file_range_value result;
	off_t offset;
	enum test_file_correction offset_correct;
	ssize_t length;
	enum test_file_correction length_correct;
};
	
struct test_file_input {
	enum test_file_input_op op;
	off_t offset;
	enum test_file_correction offset_correct;
	ssize_t length;
	enum test_file_correction length_correct;
	bool success;
	ssize_t expected;
	bool reset;
	int num_ranges;
	struct cp_test_range ranges[5];
};

#define CPREAD(s,l,r,e,rg) \
	{TFI_READ, (s), C_NONE, (l), C_NONE, (r), (e), true, rg}
#define CPWRIT(s,cs,l,cl,r,e,rst,rg) \
	{TFI_WRITE, (s), (cs), (l), (cl), (r), (e), (rst), rg}
#define CPIVAL(s,l,r,e) \
	{TFI_INVALIDATE, (s), (l), (r), (e)}
#define CPTRUN(s,cs,r,rst,rg) \
	{TFI_TRUNCATE, (s), (cs), 0, C_NONE, (r), -1, (rst), rg}

#define CP_RANGE(o,l,t)		{(t), (o), C_NONE, (l), C_NONE}
#define CP_CRANGE(o,oc,l,lc,t) {(t), (o), (oc), (l), (lc)}
#define CP_DEF_RANGE		CP_RANGE(0,0,RNG_OLD)
#define RRANGE \
	0,{CP_DEF_RANGE, CP_DEF_RANGE, CP_DEF_RANGE, CP_DEF_RANGE, CP_DEF_RANGE}
#define WRANGE(n, r1, r2, r3, r4, r5)	n, {r1, r2, r3, r4, r5}

static struct test_file_input read_test[] = {
	CPREAD(0, READSIZE, true, -1, RRANGE),
	CPREAD(READSIZE/2, READSIZE, true, -1, RRANGE),
	CPREAD(READSIZE-1, 1, true, -1, RRANGE),
	CPREAD(READSIZE-1, READSIZE+1, true, -1, RRANGE),
	CPREAD(READSIZE-1, READSIZE+2, true, -1, RRANGE),	
	CPREAD(-READSIZE, READSIZE, true, -1, RRANGE),
	CPREAD(-READSIZE, READSIZE+1, false, READSIZE, RRANGE),
	CPREAD(CHUNKSIZE-READSIZE, READSIZE, true, -1, RRANGE),
	CPREAD(CHUNKSIZE-READSIZE, READSIZE-1, true, -1, RRANGE),
	CPREAD(CHUNKSIZE-READSIZE, READSIZE+1, true, -1, RRANGE),
};

#define NUMBER_OF_R_TESTS (sizeof(read_test)/sizeof(test_file_input))

static struct test_file_input write_test[] = 
{
	CPWRIT(READSIZE, C_NONE, READSIZE, C_NONE, true, -1, false,
	    WRANGE(1,
		CP_RANGE(READSIZE, READSIZE, RNG_NEW),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPWRIT(CHUNKSIZE-READSIZE, C_NONE, READSIZE+1, C_NONE, true, -1, true,
	    WRANGE(2,
		CP_RANGE(CHUNKSIZE-READSIZE, READSIZE+1, RNG_NEW),
		CP_RANGE(CHUNKSIZE+1, READSIZE-1, RNG_OLD),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPWRIT(READSIZE/2, C_NONE, READSIZE, C_NONE, true, -1, true,
	    WRANGE(3,
		CP_RANGE(0, READSIZE/2, RNG_OLD),
		CP_RANGE(READSIZE/2, READSIZE, RNG_NEW),
		CP_RANGE(READSIZE+READSIZE/2, READSIZE/2, RNG_OLD),
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPWRIT(-127, C_OFF_FILE, 127, C_NONE, true, -1, true,
	    WRANGE(2,
		CP_CRANGE(0, C_OFF_FILE_MOD_READ, -127, C_SZ_MOD_READ, RNG_OLD),
		CP_CRANGE(-127, C_OFF_FILE, 127, C_NONE, RNG_NEW),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPTRUN(0, C_OFF_FILE, true, false, 
	    WRANGE(2,
		CP_CRANGE(0, C_OFF_FILE_MOD_READ, -127, C_SZ_MOD_READ, RNG_OLD),
		CP_CRANGE(-127, C_OFF_FILE, 127, C_NONE, RNG_NEW),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPTRUN(1, C_OFF_FILE, true, false,
	    WRANGE(3,
		CP_CRANGE(0, C_OFF_FILE_MOD_READ, -127, C_SZ_MOD_READ, RNG_OLD),
		CP_CRANGE(-127, C_OFF_FILE, 127, C_NONE, RNG_NEW),
		CP_CRANGE(0, C_OFF_FILE, 1, C_NONE, RNG_ZERO),
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPTRUN(8193, C_OFF_FILE, true, false,
	    WRANGE(3,
		CP_CRANGE(0, C_OFF_FILE_MOD_READ, -127, C_SZ_MOD_READ, RNG_OLD),
		CP_CRANGE(-127, C_OFF_FILE, 127, C_NONE, RNG_NEW),
		CP_CRANGE(0, C_OFF_FILE, 8193, C_NONE, RNG_ZERO),
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPTRUN(0, C_NONE, true, true,
	    WRANGE(1,
		CP_CRANGE(0, C_NONE, 1, C_NONE, RNG_ERROR),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
	CPWRIT(0, C_NONE, 1, C_NONE, true, -1, false,
	    WRANGE(2,
		CP_CRANGE(0, C_NONE, 1, C_NONE, RNG_NEW),
		CP_CRANGE(1, C_NONE, 1, C_NONE, RNG_ERROR),
		CP_DEF_RANGE,
		CP_DEF_RANGE,
		CP_DEF_RANGE)),
};

#define NUMBER_OF_W_TESTS (sizeof(write_test)/sizeof(test_file_input))

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF
#define RM_STRING	CBM_ACCESS_ON "rm -f %s" CBM_ACCESS_OFF
			
static void 
test_cbm_file_create_archive_open(struct test_file_info *finfo, bool compress,
    struct isi_cbm_file **file_handle)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file_obj = NULL;
	ifs_lin_t lin;
	int status = -1;
	struct stat statbuf;

	struct fmt FMT_INIT_CLEAN(str);

	fmt_print(&str, CP_STRING, finfo->input_file, finfo->output_file);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not create test file: %s", fmt_string(&str));

	status = stat(finfo->output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    finfo->output_file, strerror(errno), errno);
	lin = statbuf.st_ino;

	error = cbm_test_archive_file_with_pol(lin, CBM_TEST_COMMON_POLICY_RAN,
	    true);
	if (error) {
		fail_if(!isi_system_error_is_a(error, EALREADY),
		    "test stubbed %s not created, lin %#{} got %#{}",
		    finfo->output_file, lin_fmt(lin),
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}

	// CBM file open
	file_obj = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(!file_obj, "cannot open file %s, %#{}", finfo->output_file,
	    isi_error_fmt(error));

	*file_handle = file_obj;
}

static void
compare_read_data(const char *test_file, const char *orig_file, int index,
    off_t offset, size_t length)
{
	struct isi_error *error = NULL;
	ssize_t result = 0;
	int fd1 = -1;
	int fd2 = -1;
	char b1[3*READSIZE];
	char b2[3*READSIZE];

	fail_if(length > 3*READSIZE, "request length for comparison (%ld) "
	    "exceeds internal buffer size (%ld), index %d", 
	    length, 3*READSIZE, index);

	fd1 = open(test_file, O_RDWR);
	if (fd1 == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open test output file %s, index %d", 
		    test_file, index);
		fail_if (error, "%#{}", isi_error_fmt(error));
	}

	fd2 = open(orig_file, O_RDWR);
	if (fd2 == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open test input file %s, index %d",
		    orig_file, index);
		fail_if (error, "%#{}", isi_error_fmt(error));
	}

	fail_if(read(fd1, b1, length) != (ssize_t)length, 
	    "Could not read %ld from %s, index %d",
	    length, test_file, index);

	fail_if(pread(fd2, b2, length, offset) != (ssize_t)length, 
	    "Could not read %ld from %s, index %d",
	    length, orig_file, index);

	result = memcmp(b1, b2, length);

	fail_if(result, 
	    "read test compare for %s index %d failed at %ld for offset %ld, "
	    "length %ld failed",
	    orig_file, index, result, offset, length);

	close(fd1);
	close(fd2);
}


static void
generate_data_buf(char *buf, ssize_t length)
{
	char *pbuf = buf;
	ssize_t len = 0;
	off_t offset = 0;
	ssize_t left = length;
	int i;

	for (i = 0, pbuf = buf, left = length; 
	     i < length && left > 0; 
	     i += sizeof(off_t), pbuf += sizeof(off_t), offset++,
		 left -= len) {
		len = snprintf(pbuf, left, "%7lx%c", 
		    offset, ((offset+1)%10)?' ':'\n');
	}
	CHECK_P("\nsize of generated data was %ld\n", 8*offset);
}

static ssize_t
fixup_size(ssize_t insize, enum test_file_correction correction, 
    size_t filesize)
{
	ssize_t retval = insize;

	switch(correction) {
	case C_NONE:
		retval = insize;
		break;
	case C_OFF_FILE:
		retval = filesize + insize;
		break;
	case C_OFF_FILE_MOD_READ:
		retval = filesize - (filesize % READSIZE) + insize;
		break;
	case C_OFF_FILE_MOD_CHUNK:
		retval = filesize - (filesize % CHUNKSIZE) + insize;
		break;
	case C_SZ_MOD_READ:
		retval = filesize % READSIZE + insize;
		break;
	case C_SZ_MOD_CHUNK:
		retval = filesize % CHUNKSIZE + insize;
		break;
	default:
		fail_if(1, 
		    "Invalid correction factor %ld found for fixup_size()",
		    correction);
		break;
	}

	return retval;
}

static void
compare_write_data(const char *buf, const char *ofile, const char *ifile,
    int index, struct test_file_input *test, ssize_t filesize)
{
	int fp_cache = -1;
	int fp_orig = -1;
	int i;

	int status = -1;
	ssize_t sz = -1;
	struct fmt FMT_INIT_CLEAN(str);

	off_t test_offset;
	ssize_t test_length;

	char cbuf[3*READSIZE];
	char obuf[2*READSIZE];
	char zbuf[2*READSIZE];

	struct isi_error *error = NULL;

	memset(zbuf, 0, 2*READSIZE);

	fp_cache = open(ofile, O_RDONLY);
	if (fp_cache == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open cache file %s, test %d", ofile, index);
		fail_if(error, "%#{}", isi_error_fmt(error));
	}
	
	fp_orig = open(ifile, O_RDONLY);
	if (fp_orig == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open original file %s, test %d", ifile, index);
		fail_if(error, "%#{}", isi_error_fmt(error));
	}
	
	for(i = 0; i < test->num_ranges; i++) {
		test_length = fixup_size(test->ranges[i].length,
		    test->ranges[i].length_correct, filesize);
		test_offset = fixup_size(test->ranges[i].offset,
		    test->ranges[i].offset_correct, filesize);

		fmt_print(&str, "\n\trange %d,\n\t\t"
		    "offset, corrected: %ld, %ld (correction %{}),\n\t\t"
		    "length, corrected: %ld, %ld (correction: %{}),\n\t\t"
		    "type %{}\n", 
		    i, test->ranges[i].offset, test_offset,
		    test_file_correction_fmt(test->ranges[i].offset_correct),
		    test->ranges[i].length, test_length, 
		    test_file_correction_fmt(test->ranges[i].length_correct),
		    test_file_range_value_fmt(test->ranges[i].result));
		CHECK_P( fmt_string(&str));
		fmt_truncate(&str);

		sz = pread(fp_cache, cbuf, test_length, test_offset);
		if (sz == -1) {
			error = isi_system_error_new(errno, 
			    "Could not read cached file %s, test %d, range %d", 
			    ofile, index, i);
			fail_if(error, "%#{}", isi_error_fmt(error));
		}
		if (test->ranges[i].result == RNG_ERROR) {
			fail_if((sz == test_length), 
			    "Expected failure case but succeeded for index "
			    "%d, range %d", index, i);
		} else {
			fail_if((sz != test_length),
			    "Read of cached file %s (test %d, range %d) "
			    "failed, expected %ld, got %ld", 
			    ofile, index, i, test_length, sz);
		}

		if (test->ranges[i].result == RNG_NEW) {
			status = memcmp(cbuf, buf, test_length);
			fail_if(status, "New data at %ld, %ld "
			    "for test %d, range %d does not match at %d ",
			    test_offset, test_length,
			    index, i, status);
		} else if (test->ranges[i].result == RNG_OLD) {
			sz = pread(fp_orig, obuf, test_length,
			    test_offset);
			if (sz == -1) {
				error = isi_system_error_new(errno, 
				    "Could not read original file %s, "
				    "test %d, range %d", 
				    ifile, index, i);
				fail_if(error, "%#{}", isi_error_fmt(error));
			}
			fail_if((sz != test_length),
			    "Read of original file %s (test %d, range %d) "
			    "failed, expected %ld, got %ld", 
			    ifile, index, i, test_length, sz);

			status = memcmp(cbuf, obuf, test_length);
			fail_if(status, "Original data at %ld, %ld "
			    "for test %d, range %d, does not match at %d ",
			    test_offset, test_length,
			    index, i, status);
		} else if (test->ranges[i].result == RNG_ZERO) {
			status = memcmp(cbuf, zbuf, test_length);
			fail_if(status, "Expected read to return 0 for test "
			    "%d, range %d", index, i);
		} else if (test->ranges[i].result != RNG_ERROR) {
			fail_if(1, "Unexpected result code requested for test "
			    "%d, range %d", index, i);
		}
	}
}

/* declaring this here */
void isi_cbm_invalidate_cache_i(struct isi_cbm_file *file_obj, ifs_lin_t lin,
    bool *dirty_data, bool override, bool cpool_d, 
    struct isi_error **error_out);

static void
test_cbm_file_write_compare(struct test_file_info *finfo,
    struct test_file_input *tests, int num_tests)
{
	struct isi_cbm_file *file_obj = NULL;
	struct isi_error *error = NULL;
	int status = -1;
	struct stat statbuf;
	struct fmt FMT_INIT_CLEAN(str);
	off_t this_offset;
	ssize_t this_length;
	int i;
	size_t total_io;
	ssize_t bio;
	uint64_t boff;
	off_t blen;
	bool dirty;
	ifs_lin_t lin;

	char *buf = (char *) malloc(3*READSIZE);
	fail_if(buf == NULL, "Couldn't allocate buffer for write test");

	generate_data_buf(buf, 3*READSIZE);

	test_cbm_file_create_archive_open(finfo, false, &file_obj);

	status = stat(finfo->output_file, &statbuf);
	if (status == -1) {
		error = isi_system_error_new(errno, 
		    "Could not stat file %s", finfo->output_file);
		fail_if(error, "%#{}", isi_error_fmt(error));
	}
	lin = statbuf.st_ino;

	CHECK_P( "\n\nNumber of tests %d, filesize %ld\n", 
	    num_tests, statbuf.st_size);

	for (i = 0; i < num_tests; i++) {
		this_offset = fixup_size(tests[i].offset, 
		    tests[i].offset_correct, statbuf.st_size);
		this_length = fixup_size(tests[i].length, 
		    tests[i].length_correct, statbuf.st_size);

		if (i > 0 && tests[i].reset) {
			if (tests[i-1].op == TFI_TRUNCATE) {
				isi_cbm_file_close(file_obj, &error);
				fail_if(error, 
				    "Failed closing cached file %s: %#{}", 
				    test_files[0].output_file, 
				    isi_error_fmt(error));

				status = unlink(finfo->output_file);
				if (status == -1) {
					/*
					 * from the code I can see in
					 * isi_system_error_new, it can handle
					 * only 2 args to the format string.
					 */ 
					error = isi_system_error_new(errno, 
					    "Could not unlink test output file "
					    "%s for reset",
					    finfo->output_file);
					fail_if (error,
					    "%#{}, index %d, test %d",
					    isi_error_fmt(error), index, i);
				}

				test_cbm_file_create_archive_open(finfo, false, 
				    &file_obj);

				status = stat(finfo->output_file, &statbuf);
				if (status == -1) {
					error = isi_system_error_new(errno, 
					    "Could not stat file %s", 
					    finfo->output_file);
					fail_if(error, "%#{}", 
					    isi_error_fmt(error));
				}
				lin = statbuf.st_ino;

			} else {
				struct isi_cbm_file *file_obj = NULL;
				file_obj = isi_cbm_file_open(
				    statbuf.st_ino, 
				    HEAD_SNAPID, 
                                    0,
				    &error);
				if (error) {
					isi_error_add_context(error,
    					    "Could not open file %s", 
					    finfo->output_file);
					fail_if(error, "%#{}", 
					    isi_error_fmt(error));
				}

				isi_cbm_invalidate_cache_i(file_obj, 
				    statbuf.st_ino, 
				    &dirty, 
				    true /* override dirty check */, 
				    false /* not from cpool_d */,
				    &error);
				fail_if(error, "Could not reset for test %d "
				    "using isi_cbm_file_invalidate: %#{}", 
				    i, isi_error_fmt(error));
				isi_cbm_file_close(file_obj, 
				    isi_error_suppress());
			}
		}

		fmt_print(&str, "\nindex %d, op: %{}\n\t"
		    "offset, corrected to: %ld, %ld (correction %{}),\n\t"
		    "length, corrected to: %ld, %ld (correction: %{}),\n\t",
		    i, test_file_input_op_fmt(tests[i].op), 
		    tests[i].offset, this_offset,
		    test_file_correction_fmt(tests[i].offset_correct),
		    tests[i].length, this_length, 
		    test_file_correction_fmt(tests[i].length_correct));
		CHECK_P( fmt_string(&str));
		fmt_truncate(&str);

		switch(tests[i].op) {
		case TFI_WRITE:
			ASSERT(this_offset >= 0);
			bio = 1;
			total_io = 0;
			boff = (uint64_t)this_offset;
			blen = this_length;
			while ((total_io < this_length) && (bio > 0)) {
				bio = isi_cbm_file_write(file_obj, buf,
				    boff, blen, &error);
				fail_if(error, "Failure on write for %ld, %ld "
				    "at %lu, %ld for index %d: %#{}",
				    this_offset, this_length, boff, blen, i,
				    isi_error_fmt(error));
				total_io += bio;
				boff += bio;
				blen -= bio;
			}
			break;
		case TFI_TRUNCATE:
			isi_cbm_file_truncate(lin, HEAD_SNAPID, this_offset,
			    &error);
			fail_if(error, "Failure on truncate for %ld "
				    "for index %d: %#{}",
				    this_offset, i, isi_error_fmt(error));
			break;
		default:
			fail_if(1, "Unrecognized OP %d for test %d",
			    tests[i].op, i);
			break;
		}
			
		compare_write_data(buf, finfo->output_file, finfo->input_file,
		    i, &tests[i], statbuf.st_size);
	}

	isi_cbm_file_close(file_obj, &error);
	fail_if(error, "Failed closing cached file %s: %#{}", 
	    finfo->output_file, isi_error_fmt(error));

	if (buf) {
		free(buf);
		buf = NULL;
	}
}

static void 
test_cbm_file_read_compare(struct test_file_info *finfo,
    struct test_file_input *tests, int num_tests, struct isi_cbm_file *file_obj)
{
	struct isi_error *error = NULL;
	int status = -1;
	struct stat statbuf;
	struct fmt FMT_INIT_CLEAN(str);
	off_t this_offset;
	ssize_t this_length;
	ssize_t expected_length;
	int i;
	ssize_t total_read;
	ssize_t bw, br;
	off_t bo;
	uint64_t boff;
	off_t blen;

	char *buf = (char *) malloc(3 * READSIZE);

	status = stat(finfo->output_file, &statbuf);
	if (status == -1) {
		error = isi_system_error_new(errno, 
		    "Could not stat file %s", finfo->output_file);
		fail_if(error, "%#{}", isi_error_fmt(error));
	}

	int fp_out = open(TEST_OUTPUT_FILE, O_RDWR|O_CREAT|O_TRUNC, 0666);
	if (fp_out == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open test output file %s", TEST_OUTPUT_FILE);
		fail_if (error, "%#{}", isi_error_fmt(error));
	}

	CHECK_P( "\n\nNumber of tests %d, filesize %ld\n", 
	    num_tests, statbuf.st_size);

	for (i = 0; i < num_tests; i++) {
		this_offset = tests[i].offset;
		if (this_offset < 0)
			this_offset += statbuf.st_size;
		this_length = tests[i].length;
		if (this_length < 0)
			this_length += statbuf.st_size;
		if (tests[i].success)
			expected_length = this_length;
		else {
			expected_length = tests[i].expected;
			if (expected_length < 0)
				expected_length += statbuf.st_size;
		}

		fmt_print(&str, "\nindex %d, op: %{}, offset: %ld(%ld), "
		    "length %ld(%ld) expected %ld\n", 
		    i, test_file_input_op_fmt(tests[i].op), 
		    tests[i].offset, this_offset, 
		    tests[i].length, this_length,
		    expected_length);
		CHECK_P(fmt_string(&str));
		fmt_truncate(&str);

		if (lseek(fp_out, 0, SEEK_SET) == -1) {
			error = isi_system_error_new(errno,
			    "Seeking in test output file %s", TEST_OUTPUT_FILE);
			fail_if(error, "%#{}", isi_error_fmt(error));
		}

		if (ftruncate(fp_out, 0) == -1) {
			error = isi_system_error_new(errno, 
			    "Truncating test output file %s", TEST_OUTPUT_FILE);
			fail_if(error, "%#{}", isi_error_fmt(error));
		}
		
		br = 1;
		total_read = 0;
		boff = this_offset;
		blen = this_length;
		while (total_read < this_length && br > 0) {
			br = isi_cbm_file_read(file_obj, buf,
			    boff, blen, CO_CACHE, &error);
			fail_if(error,
			    "Could not perform a cached read for test "
			    "%d, offset %ld, length %ld, error: %#{}",
			    i, this_offset, this_length, 
			    isi_error_fmt(error));
			for (bo = 0; bo < br; bo += bw) {
				bw = write(fp_out, buf, br);
				if (bw == -1) {
					error = isi_system_error_new(errno, 
					    "Could not write test output file "
					    "%s,", TEST_OUTPUT_FILE);
					fail_if (error, "%#{}",
					    isi_error_fmt(error));
				}
			}
			total_read += br;
			boff += br;
			blen -= br;
		}
		if (total_read != this_length) {
			fail_if((tests[i].success && 
				total_read != expected_length),
			    "Read for test %d for offset %ld, "
			    "length %ld expected %ld and got read %ld",
			    i, this_offset, this_length, 
			    expected_length, total_read);
		} else {
			fail_if(!tests[i].success, 
			    "Expected to read %ld for test %d, read %ld",
			    expected_length, i, total_read);
		}
		compare_read_data(TEST_OUTPUT_FILE, finfo->input_file, i,
		    this_offset, expected_length);
	}
	if (fp_out != -1)
		close(fp_out);

	if (buf) {
		free(buf);
		buf = NULL;
	}

	status = unlink(TEST_OUTPUT_FILE);
	fail_if(status, "Cannot remove test file %s", TEST_OUTPUT_FILE);
}


TEST(test_cache_read, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_cbm_file *file_obj = NULL;
	struct isi_error *error = NULL;
	int status;

	test_cbm_file_create_archive_open(&test_files[0], false, &file_obj);

	test_cbm_file_read_compare(&test_files[0], read_test, 
	    NUMBER_OF_R_TESTS, file_obj);

	isi_cbm_file_close(file_obj, &error);
	fail_if(error, "Failed closing cached file %s: %#{}", 
	    test_files[0].output_file, isi_error_fmt(error));

	status = unlink(test_files[0].output_file);
}


TEST(test_cache_write, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	int status;

	test_cbm_file_write_compare(&test_files[0], write_test, 
	    NUMBER_OF_W_TESTS);

	status = unlink(test_files[0].output_file);
	fail_if(status, "Cannot remove test file %s", test_files[0].output_file);
}
