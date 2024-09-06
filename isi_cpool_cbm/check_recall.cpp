#include "isi_cpool_cbm.h"
#include <check.h>
#include <list>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <unistd.h>

#include <isi_snapshot/snapshot.h>
#include <isi_snapshot/snapshot_manage.h>
#include <isi_util/check_helper.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_fd_store.h>

#include "check_cbm_common.h"
#include "isi_cbm_cache.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_sparse.h"

using namespace isi_cloud;

static isi_cbm_test_sizes small_size(8 * 1024, 8 * 1024, 16 * 1024);
static cbm_test_env env;
#define SPEC cbm_test_policy_spec

TEST_FIXTURE(suite_setup)
{
	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	cbm_test_policy_spec specs[] = {
		SPEC(CPT_RAN, false, false, small_size),
		SPEC(CPT_RAN, true, false, small_size),
		SPEC(CPT_AZURE, false, false, small_size),
		SPEC(CPT_AZURE, true, false, small_size),
		SPEC(CPT_RAN, false, false, small_size,
		    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_FULL),
		SPEC(CPT_AWS, false, false, small_size),
	};
	env.setup_env(specs, sizeof(specs)/sizeof(cbm_test_policy_spec));

	cbm_test_common_setup(false, true);

	cpool_fd_store::getInstance();
}

TEST_FIXTURE(suite_teardown)
{
	env.cleanup();
	cbm_test_common_cleanup(false, true);

	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
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

	/**
	 * Release any memory allocated by the store.
	 */
	cpool_fd_store::getInstance().close_sbt_fds();
}

SUITE_DEFINE_FOR_FILE(check_recall,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 300,
    .fixture_timeout = 1200);

static void
test_recall_unmod_file_x(const char *pol_name)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_recall_unmod_file.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const char *buffer = "This is a test file";
	size_t len = strlen(buffer);
	write(fd, buffer, len);


	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin, pol_name, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	fail_if(error, "Failed to recall the file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed.
 */
TEST(test_recall_unmod_file, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_recall_unmod_file_x(CBM_TEST_COMMON_POLICY_RAN);
}

#define MAX_SEGMENTS			32
#define SIZE_1M_BYTES			(1024*1024)
#define MAX_MAP_SIZE			64
struct sparse_file {
	const char *name;
	size_t resolution;
	unsigned char map[MAX_MAP_SIZE];
	size_t map_size;
	off_t  offset;
	size_t filesize;    // for these tests this should be <= region_size
	size_t region_size; // multiple of block resolution size
	size_t num_segments;// number of segments below
	struct segment segments[MAX_SEGMENTS];
};
#define TF_1Ma \
	SVAL_BLK(0, 4), \
	SVAL_BLK(8, 4), \
	SVAL_BLK(32,2), \
	SVAL_BLK(68,1), \
	SVAL_BLK(88,8),

struct sparse_file sp_test = {
	"1Ma",			// test name
	IFS_BSIZE,		// resolution
	{0xf0,0xf0,0xff,0xff,0xfc,0xff,0xff,0xff,
	 0xef,0xff,0xff,0x00,0xff,0xff,0xff,0xff},
	16,			// 16 bytes in map
	0,			// offset
	SIZE_1M_BYTES,		// filesise = 1M
	SIZE_1M_BYTES,		// 1M region size
	5,			// number of segments to write
	SEGMENTS(TF_1Ma)	// the segments
};

static void
compare_file(const char *a, const char *b)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int status = -1;

	fmt_print(&fmt, "diff %s %s", a, b);

	status = system(fmt_string(&fmt));

	fail_if(status, "Files %s and %s do not compare", a, b);
}

/**
 * Test recalling without cache
 *
 */
TEST(test_recall_without_cache, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	int status = 0;
	struct stat statbuf;
	struct isi_error *error = NULL;
	int map_size = sp_test.map_size;
	unsigned char spa_map[64];
	memset(spa_map, 0, 64);
	const char *filename = "/ifs/test_recall_sparse_file.txt";
	const char *base_filename = "/ifs/base_test_recall_sparse_file.txt";

	remove(filename);
	remove(base_filename);

	cbm_sparse_test_file_helper test_file(filename, sp_test.filesize,
	    sp_test.num_segments, sp_test.segments);

	fail_if(stat(filename, &statbuf), "Could not stat %s for %s: %s (%d)",
	    filename, sp_test.name, strerror(errno), errno);

	isi_cbm_build_sparse_area_map(statbuf.st_ino, statbuf.st_snapid,
	    0, statbuf.st_size, sp_test.resolution, sp_test.region_size,
	    spa_map, map_size, &error);
	fail_if(error, "Failed to create sparse area map for %s: %{}",
	    filename, isi_error_fmt(error));

	status = memcmp(spa_map, &(sp_test.map), map_size);

	fail_if(status, "Sparse maps do not compare for %s",
	    sp_test.name);

	uint64_t lin = statbuf.st_ino;

	error = cbm_test_archive_file_with_pol(lin, CBM_TEST_COMMON_POLICY_RAN, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_fail_point ufp("cbm_recall_fail_to_open_cache");

	ufp.set("return(1)");
	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	ufp.set("off");

	fail_if(error, "Failed to recall the file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_build_sparse_area_map(statbuf.st_ino, statbuf.st_snapid,
	    0, statbuf.st_size, sp_test.resolution, sp_test.region_size,
	    spa_map, map_size, &error);
	fail_if(error, "Failed to create sparse area map after recall for %s: %{}",
	    filename, isi_error_fmt(error));

	status = memcmp(spa_map, &(sp_test.map), map_size);

	fail_if(status, "Sparse maps do not compare after recall for %s",
	    sp_test.name);

	cbm_sparse_test_file_helper base_file(base_filename, sp_test.filesize,
	    sp_test.num_segments, sp_test.segments);

	compare_file(base_filename, filename);

	remove(filename);
	remove(base_filename);

}


/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed.
 */
TEST(test_recall_unmod_file_comp, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	test_recall_unmod_file_x(CBM_TEST_COMMON_POLICY_RAN_C);
}

TEST(test_recall_unmod_file_az, mem_check : CK_MEM_DEFAULT, mem_hint :0)
{
	test_recall_unmod_file_x(CBM_TEST_COMMON_POLICY_AZ);
}


TEST(test_recall_unmod_file_az_comp, mem_check : CK_MEM_DEFAULT,
    mem_hint :0)
{
	test_recall_unmod_file_x(CBM_TEST_COMMON_POLICY_AZ_C);
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed.
 */
TEST(test_recall_unmod_file_s3, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	cbm_test_policy_spec spec(CPT_AWS, false, false, small_size);

	test_recall_unmod_file_x(spec.policy_name_.c_str());
}

static void
test_recall_unmod_zero_lengthed_file_x(const char *pol_name)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_recall_unmod_zero_lengthed_file.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin, pol_name, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	fail_if(error, "Failed to recall the file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_recall_unmod_zero_lengthed_file)
{
	test_recall_unmod_zero_lengthed_file_x(CBM_TEST_COMMON_POLICY_RAN);
}

TEST(test_recall_unmod_zero_lengthed_file_comp)
{
	test_recall_unmod_zero_lengthed_file_x(CBM_TEST_COMMON_POLICY_RAN_C);
}

TEST(test_recall_unmod_zero_lengthed_file_az)
{
	test_recall_unmod_zero_lengthed_file_x(CBM_TEST_COMMON_POLICY_AZ);
}

TEST(test_recall_unmod_zero_lengthed_file_comp_az)
{
	test_recall_unmod_zero_lengthed_file_x(CBM_TEST_COMMON_POLICY_AZ_C);
}

static void
test_recall_mod_file_x(const char *pol_name)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_recall_mod_file.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const char *buffer = "This is a test file";
	size_t len = strlen(buffer);
	write(fd, buffer, len);


	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin, pol_name, false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char *byte = "that";

	isi_cbm_file_write(file, byte, 0, strlen(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	fail_if(error, "Failed to recall the file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);

	char buf[256] = {0};

	read(fd, buf, sizeof(buf));

	const char *expected = "that is a test file";
	fail_if(strcmp(buf, expected),
	    "Failed: The content after recall is not expected. "
	    "expected: \"%s\", actual: \"%s\"", expected, buf);
	close(fd);
	remove(path);
}

/**
 * simple test case to synchronize a file which is modified after
 * stubbing. Shall succeed.
 */
TEST(test_recall_mod_file, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_recall_mod_file_x(CBM_TEST_COMMON_POLICY_RAN);
}

TEST(test_recall_mod_file_sparse_check, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	cbm_test_fail_point ufp("cbm_set_all_sparse");

	ufp.set("return(1)");
	test_recall_mod_file_x(CBM_TEST_COMMON_POLICY_RAN);
	ufp.set("off");
}

/**
 * simple test case to synchronize a file which is modified after
 * stubbing. Shall succeed.
 */
TEST(test_recall_mod_file_comp, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_recall_mod_file_x(CBM_TEST_COMMON_POLICY_RAN_C);
}

TEST(test_recall_mod_file_az, mem_check : CK_MEM_DEFAULT, mem_hint :0)
{
	test_recall_mod_file_x(CBM_TEST_COMMON_POLICY_AZ);
}

TEST(test_recall_mod_file_az_comp, mem_check : CK_MEM_DEFAULT,
    mem_hint :0)
{
	test_recall_mod_file_x(CBM_TEST_COMMON_POLICY_AZ_C);
}

static void
test_recall_mod_file_two_chunks_small_x(cl_provider_type ptype,
    bool compress, bool encrypt)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size + 1];
	memset(buffer, 'A', buffer_size);
	write(fd, buffer, buffer_size);

	memset(buffer, 'B', buffer_size);
	write(fd, buffer, buffer_size);

	cbm_test_policy_spec spec(ptype, compress, encrypt, small_size);

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the first chunk

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	fail_if(error, "Failed to reall the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after recall
	fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);

	char actual[buffer_size + 1] = {0};

	read(fd, actual, buffer_size);
	memset(buffer, 'A', buffer_size);
	buffer[0] = 'B';
	buffer[buffer_size] = 0;
	actual[buffer_size] = 0;

	fail_if(strncmp(actual, buffer, buffer_size),
	    "Failed: The content after recall is not expected. "
	    "expected: \"%s\", actual: \"%s\"", buffer, actual);

	read(fd, actual, sizeof(actual));
	memset(buffer, 'B', sizeof(buffer));
	buffer[buffer_size] = 0;
	actual[buffer_size] = 0;

	fail_if(strncmp(actual, buffer, buffer_size),
	    "Failed: The content after recall is not expected. "
	    "expected: \"%s\", actual: \"%s\"", buffer, actual);

	close(fd);
	remove(path);
}

TEST(test_recall_mod_2_chunks)
{
	test_recall_mod_file_two_chunks_small_x(CPT_RAN, false, false);
}

TEST(test_recall_mod_2_chunks_comp)
{
	test_recall_mod_file_two_chunks_small_x(CPT_RAN, true, false);
}

TEST(test_recall_mod_2_chunks_az)
{
	test_recall_mod_file_two_chunks_small_x(CPT_AZURE, false, false);
}

TEST(test_recall_mod_2_chunks_az_comp)
{
	test_recall_mod_file_two_chunks_small_x(CPT_AZURE, true, false);
}

static void
test_access_triggered_recall_2chunks_x(cl_provider_type ptype,
    bool compress, bool encrypt)
{
	struct isi_error *error = NULL;

	/*
	 * Make sure the recall SBTs exist.
	 */
	const cpool_operation_type *op_type = OT_RECALL;
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    0, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 0: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    1, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL,
	    "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size + 1];
	memset(buffer, 'A', buffer_size);
	write(fd, buffer, buffer_size);

	memset(buffer, 'B', buffer_size);
	write(fd, buffer, buffer_size);

	cbm_test_policy_spec spec(ptype, compress, encrypt, small_size,
	    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_FULL);

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char byte[2] = {'B'}; // read the first byte

	isi_cbm_file_read(file, &byte, 0, sizeof(byte), CO_DEFAULT, &error);

	fail_if(error, "Failed to read the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after recall
	fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);

	char actual[buffer_size + 1] = {0};

	read(fd, actual, buffer_size);
	memset(buffer, 'A', buffer_size);
	buffer[buffer_size] = 0;
	actual[buffer_size] = 0;

	fail_if(strncmp(actual, buffer, buffer_size),
	    "Failed: The content for 1 after full cache is not expected. "
	    "expected: \"%s\", actual: \"%s\"", buffer, actual);
	/*
	 * the following cannot be reliably tested due to asynchronous
	 * processing.
	read(fd, actual, sizeof(actual));
	memset(buffer, 'B', sizeof(buffer));
	buffer[buffer_size] = 0;
	actual[buffer_size] = 0;

	fail_if(strncmp(actual, buffer, buffer_size),
	    "Failed: The content for 2 after full cache is not expected. "
	    "expected: \"%s\", actual: \"%s\"", buffer, actual);
	*/
	close(fd);
	remove(path);

	/*
	 * Cleanup the gconfig context to prevent a bogus memory leak message.
	 */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(test_access_triggered_recall_2chunks_ran, .mem_hint = 0)
{
	test_access_triggered_recall_2chunks_x(CPT_RAN, false, false);
}
