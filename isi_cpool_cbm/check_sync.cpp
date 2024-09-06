#include "isi_cpool_cbm.h"
#include <list>
#include <fcntl.h>
#include <map>
#include <memory>
#include <stdio.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>

#include <isi_ilog/ilog.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/ckpt.h>
#include <isi_gconfig/main_gcfg.h>
#include <isi_gconfig/gconfig_unit_testing.h>
#include <isi_snapshot/snapshot.h>
#include <isi_snapshot/snapshot_manage.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/check_helper.h>
#include <isi_cpool_security/cpool_protect.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>

#include "check_cbm_common.h"
#include "isi_cbm_cache.h"
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include "isi_cbm_invalidate.h"
#include "isi_cbm_sync.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_util.h"
#include "isi_cbm_gc.h"

#include <check.h>

using namespace isi_cloud;

static cbm_test_env env;

static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

static void delete_all_cfg()
{
	env.cleanup();
}

#define SPEC cbm_test_policy_spec
#ifdef ENABLE_ILOG_USE
#define ANNOUNCE_TEST_START \
	ilog(IL_DEBUG, "Entered TEST %s (line %d)",  __FUNCTION__, __LINE__);\
	CHECK_TRACE("Entered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#else
#define ANNOUNCE_TEST_START \
	CHECK_TRACE("Entered TEST %s (line %d)\n",  __FUNCTION__, __LINE__);
#endif

struct timeval suite_start_time;
TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_sync",			// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_sync",			// syslog program
		IL_TRACE_PLUS|IL_CP_EXT|IL_DETAILS,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_sync.log",	// log file
		IL_TRACE_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif
	gettimeofday(&suite_start_time, NULL);
	struct isi_error *error = NULL;

	cbm_test_common_setup_gconfig();

	cbm_test_policy_spec specs[] = {
		SPEC(CPT_RAN, false, false, small_size),
		SPEC(CPT_RAN, true, false, small_size),
		SPEC(CPT_RAN, false, true, small_size),
		SPEC(CPT_RAN, true, true, small_size),
		SPEC(CPT_RAN, false, false, small_size,
		    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL, 0,
		    3600),
		SPEC(CPT_AZURE, false, false, small_size),
		SPEC(CPT_AZURE, true, false, small_size),
		SPEC(CPT_AZURE, false, true, small_size),
		SPEC(CPT_AZURE, true, true, small_size),
		SPEC(CPT_AWS, false, false, small_size),
		SPEC(CPT_AWS, true, false, small_size),
		SPEC(CPT_AWS, true, true, small_size),
		// policy for delayed truncation
		SPEC(CPT_AWS, false, false, small_size,
		    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL,
		    0, 0, 500),
	};

	env.setup_env(specs, sizeof(specs)/sizeof(cbm_test_policy_spec));
	// now load the config
	isi_cbm_reload_config(&error);

	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));

	// for encryption
	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_leak_check_primer(true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
	restart_leak_detection(0);
	struct timeval setup_end_time;
	gettimeofday(&setup_end_time, NULL);
	struct timeval diff;
	timersub(&setup_end_time, &suite_start_time, &diff);
	printf("\nLast suite setup elapsed time sec: %ld, ms: %ld\n",
	    diff.tv_sec, diff.tv_usec);
	isi_cbm_get_wbi(isi_error_suppress());
	isi_cbm_get_coi(isi_error_suppress());
}

TEST_FIXTURE(suite_teardown)
{
	delete_all_cfg();
	// ensure good for next test since each test may have destroyed it
	cl_provider_module::init();
	isi_cbm_unload_conifg();

	struct timeval suite_end_time;
	gettimeofday(&suite_end_time, NULL);
	struct timeval diff;
	timersub(&suite_end_time, &suite_start_time, &diff);
	printf("\nLast suite elapsed time sec: %ld, ms: %ld\n",
	    diff.tv_sec, diff.tv_usec);

	cbm_test_common_cleanup_gconfig();
}

struct timeval test_start_time;
TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
	gettimeofday(&test_start_time, NULL);
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();
	struct timeval test_end_time;
	gettimeofday(&test_end_time, NULL);
	struct timeval diff;
	timersub(&test_end_time, &test_start_time, &diff);
	printf("    Last test elapsed time sec: %ld, ms: %ld\n",
	    diff.tv_sec, diff.tv_usec);

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_sync,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 300,
    .fixture_timeout = 1200);

static void
cbm_test_sync(uint64_t lin, uint64_t snapid, get_ckpt_data_func_t get_ckpt_cb,
    set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, isi_error **error_out)
{
	isi_cbm_sync_option opt = {blocking: true, skip_settle: false};
	isi_cbm_sync_opt(lin, snapid, get_ckpt_cb, set_ckpt_cb, ctx,
	    opt, error_out);
}

static void
test_sync_zero_lengthed_unmod_file_common(cl_provider_type ptype)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_zero_lengthed_unmod_file.txt";

	cbm_test_policy_spec spec(ptype, false, false, small_size);
	cbm_test_file_helper test_fo(path, 0, "");
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. The file is zero-lengthed. Shall succeed.
 */
TEST(test_sync_zero_lengthed_unmod_file, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_zero_lengthed_unmod_file_common(CPT_RAN);

	test_sync_zero_lengthed_unmod_file_common(CPT_AWS);
}

/**
 * simple test case to synchronize a file which is modified after
 * stubbing. Shall succeed.
 */
TEST(test_sync_zero_lengthed_mod_file, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_zero_lengthed_unmod_file.txt";

	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	cbm_test_file_helper test_fo(path, 0, "");
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	struct stat st = {0};
	fstat(file->fd, &st);
	fail_if(st.st_flags & SF_CACHED_STUB, "SF_CACHED_STUB flag should not"
	    "be set after invalidation.");

	char out_buf[1];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(&byte, out_buf, outlen),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL,
	    &error);
	fail_if(error, "Failed to recall the file: %#{}.\n",
	    isi_error_fmt(error));
}

static void
test_sync_unmod_file_x(bool compress, bool sparse)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_unmod_file.txt";

	const char *buffer = "This is a test file";
	size_t len = 0;
	cbm_test_policy_spec spec(CPT_RAN, compress, false, small_size);

	cbm_sparse_test_file_helper *test_fo_sparse = NULL;
	cbm_test_file_helper *test_fo = NULL;
	ifs_lin_t lin = 0;
	if (sparse) {
		len = sparse_alt_even_byte.filesize;
		test_fo_sparse = new cbm_sparse_test_file_helper (
		    path,
		    sparse_alt_even_byte.filesize,
		    sparse_alt_even_byte.num_segments,
		    sparse_alt_even_byte.segments);
		lin = test_fo_sparse->get_lin();
	} else {
		len = strlen(buffer);
		test_fo = new cbm_test_file_helper(path, len, buffer);
		lin = test_fo->get_lin();
	}

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);

	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(
	    lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	char out_buf[256];
	size_t total_read = 0;
	size_t read_len = 0;
	size_t outlen = 0;

	while (total_read < len) {
		memset(out_buf, 0, sizeof(out_buf));
		read_len = MIN(sizeof(out_buf), (len - total_read));
		outlen = isi_cbm_file_read(file, out_buf, total_read,
		    sizeof(out_buf), CO_CACHE, &error);
		fail_if(error, "cannot read file %s, %#{}", path,
		    isi_error_fmt(error));

		fail_if(outlen != read_len,
		    "Read length mismatch: expected %d, got %d, total %d",
		    read_len, outlen, total_read);

		if (!sparse) {
			fail_if(strncmp(buffer, out_buf, len),
			   "The data is not matched after stubbing.");
		}
		total_read += outlen;
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));



	total_read = 0;
	read_len = 0;
	outlen = 0;

	while (total_read < len) {
		memset(out_buf, 0, sizeof(out_buf));
		read_len = MIN(sizeof(out_buf), (len - total_read));
		outlen = isi_cbm_file_read(file, out_buf, total_read,
		    sizeof(out_buf), CO_CACHE, &error);
		fail_if(error, "cannot read file %s after invalidate, %#{}",
		    path, isi_error_fmt(error));

		fail_if(outlen != read_len,
		    "Read length mismatch after invalidate: "
		    "expected %d, got %d, total %d",
		    read_len, outlen, total_read);

		if (!sparse) {
			fail_if(strncmp(buffer, out_buf, len),
			   "The data mismatch after invalidate.");
		}
		total_read += outlen;
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	if (test_fo_sparse) {
		int retc = test_fo_sparse->remove_file();
		// file may be removed by caller explicitly
		fail_if(retc < 0 && errno != ENOENT,
		    "removing sparse file failed %s", strerror(errno));
		delete test_fo_sparse;
	}
	if (test_fo) {
		delete test_fo;
	}

}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed.
 */
TEST(test_sync_unmod_file, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_unmod_file_x(false, false);

	test_sync_unmod_file_x(true, false); // with compression


	// Now the same for a sparse file
	test_sync_unmod_file_x(false, true);

	test_sync_unmod_file_x(true, true); // with compression

}

static void
test_sync_mod_file_common(cl_provider_type ptype, bool sparse)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_mod_file.txt";
	cbm_test_policy_spec spec(ptype, false, false, small_size);

	cbm_sparse_test_file_helper *test_fo_sparse = NULL;
	cbm_test_file_helper *test_fo = NULL;
	ifs_lin_t lin = 0;
	if (sparse) {
		test_fo_sparse = new cbm_sparse_test_file_helper (
		    path,
		    sparse_alt_odd_byte.filesize,
		    sparse_alt_odd_byte.num_segments,
		    sparse_alt_odd_byte.segments);
		lin = test_fo_sparse->get_lin();
	} else {
		test_fo = new cbm_test_file_helper(path, 17, "AAAAA");
		lin = test_fo->get_lin();
	}

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	sync();

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[1];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(&byte, out_buf, outlen),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	if (test_fo_sparse) {
		int retc = test_fo_sparse->remove_file();
		// file may be removed by caller explicitly
		fail_if(retc < 0 && errno != ENOENT,
		    "removing sparse file failed %s", strerror(errno));
		delete test_fo_sparse;
	}
	if (test_fo) {
		delete test_fo;
	}
}

/**
 * RAN: simple case to sync a file which only has one chunk. And the chunk is
 * modified.
 */
TEST(test_sync_mod_file_ran, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_mod_file_common(CPT_RAN, false);

	test_sync_mod_file_common(CPT_RAN, true); //sparse

	//test_sync_mod_file_common(CPT_AWS);

}

static void
test_sync_mod_file_two_chunks_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	char out_buf[buffer_size];


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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the first chunk

	if (spec.delayted_trunc_time_ > 0) {
		status = fstat(file->fd, &sb);

		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    path, strerror(errno), errno);

		fail_if((sb.st_flags & SF_CACHED_STUB) == 0,
		    "Expected to have the SF_CACHED_STUB flag on: flag: %x",
		    sb.st_flags);

		cbm_test_fail_point ufp(
		    "cbm_generate_celog_event_authentication");
		ufp.set("return(1)"); // fail any network read

		memset(out_buf, 0, sizeof(out_buf));

		size_t outlen = isi_cbm_file_read(file, out_buf,
		    0, // the first CDO
		    sizeof(out_buf), CO_CACHE, &error);
		fail_if(error, "cannot read file %s, %#{}", path,
		    isi_error_fmt(error));

		fail_if(outlen != sizeof(out_buf),
		    "The read length is not matched %d %d.",
		    sizeof(out_buf), outlen);

		memset(buffer, 'A', sizeof(buffer));
		fail_if(strncmp(buffer, out_buf, buffer_size),
		   "The data is not matched after stubbing.");

		// since it is fully cached, read shall still succeed
		ufp.set("off");
	}

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	memset(out_buf, 0, sizeof(out_buf));


	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the first CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	memset(buffer, 'A', sizeof(buffer));
	buffer[0] = 'B';
	fail_if(strncmp(buffer, out_buf, buffer_size),
	   "The data is not matched after stubbing.");

	outlen = isi_cbm_file_read(file, out_buf,
	    small_size.chunksize_, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	memset(buffer, 'B', sizeof(buffer));

	fail_if(strncmp(buffer, out_buf, buffer_size),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));
	remove(path);
}

/**
 * simple case to sync a file which has two chunks. And one chunk is
 * modified. The file is then synchronized back.
 */
TEST(test_sync_mod_file_two_chunks, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_mod_file_two_chunks_x(spec);
	spec = SPEC(CPT_RAN, true, false, small_size);
	test_sync_mod_file_two_chunks_x(spec);

	spec = SPEC(CPT_AWS, false, false, small_size);
	test_sync_mod_file_two_chunks_x(spec);

	spec = SPEC(CPT_AWS, true, false, small_size);
	test_sync_mod_file_two_chunks_x(spec);

	spec = SPEC(CPT_AWS, true, true, small_size);
	test_sync_mod_file_two_chunks_x(spec);

	spec = SPEC(CPT_AWS, false, false, small_size,
	    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL,
	    0, 0, 500);
	test_sync_mod_file_two_chunks_x(spec);
}

static void
test_sync_mod_file_three_chunks_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_three_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'C', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'A'; // turn the first byte to A in the second chunk

	isi_cbm_file_write(file, &byte, small_size.chunksize_,
	    sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo;
	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	off_t sz = 3 * small_size.chunksize_;
	fail_if(mapinfo.get_filesize() != sz, "File size in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo.get_filesize(), sz);

	fail_if(mapinfo.get_count() != 3, "The entries count in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo.get_count(), 3);

	isi_cfm_mapinfo::iterator it = mapinfo.begin(&error);
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	int i = 0;
	for (; it != mapinfo.end(); it.next(&error), ++i) {

		if (it->second.get_length() != (size_t)small_size.chunksize_) {
			fail("File size in the mapping entry is not "
			    "expected: %ld %ld. pol: %s\n",
			    it->second.get_length(), small_size.chunksize_,
			    spec.policy_name_.c_str());
		}
		uint64_t snapid = it->second.get_object_id().get_snapid();

		if (i == 0 || i == 2) {
			fail_if(snapid != 0, "The version id is not "
			    "expected: %d %d", snapid, 0);
		} else {
			fail_if(snapid != 1, "The version id is not "
			    "expected: %d %d", snapid, 1);
		}
	}
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));


	size_t outlen = isi_cbm_file_read(file, out_buf,
	    small_size.chunksize_, // the second CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	memset(buffer, 'B', sizeof(buffer));
	buffer[0] = 'A';
	fail_if(strncmp(buffer, out_buf, buffer_size),
	   "The data is not matched after stubbing.");

	outlen = isi_cbm_file_read(file, out_buf,
	    2 * small_size.chunksize_, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	memset(buffer, 'C', sizeof(buffer));

	fail_if(strncmp(buffer, out_buf, buffer_size),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));
	remove(path);
}

TEST(test_sync_mod_file_three_chunks, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_AWS, false, false, small_size);
	test_sync_mod_file_three_chunks_x(spec);
}

static ifs_snapid_t
get_snapshot_id_by_name(const std::string &snap)
{
	ifs_snapid_t id;
	struct isi_str snap_name;
	isi_str_init(&snap_name, (char *)snap.c_str(),
	    snap.length() + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	isi_error *error = NULL;
	SNAP * sn = snapshot_open_by_name(&snap_name, &error);
	fail_if_error(error, "Failed to get snapshot %s, %#{}",
	    snap.c_str(), isi_error_fmt(error));

	snapshot_get_snapid(sn, &id);

	snapshot_close(sn);
	return id;
}

static void
test_sync_mod_file_snap_two_chunks_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_snap_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	std::string snap_name1 = "test_sync_mod_file_snap_two_chunks_s1";
	remove_snapshot(snap_name1);
	take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the first chunk

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	std::string snap_name = "test_sync_mod_file_snap_two_chunks";
	remove_snapshot(snap_name);
	take_snapshot("/ifs", snap_name, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove_snapshot(snap_name);
	remove_snapshot(snap_name1);

	remove(path);
}

/**
 * Case to sync a file which has two chunks. And one chunk is
 * modified. A snapshot is taken The file is then synchronized back.
 */
TEST(test_sync_mod_file_snap_two_chunks, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_mod_file_snap_two_chunks_x(spec);
}

static void
test_sync_snap_mod_two_chunks_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_snap_mod_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

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

	std::string snap_name = "test_sync_snap_mod_two_chunks";
	remove_snapshot(snap_name);
	ifs_snapid_t snapid = take_snapshot("/ifs", snap_name, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char byte = 'A'; // turn the first byte to A in the second chunk

	isi_cbm_file_write(file, &byte, 8192*4, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// now verify the older snapshots, it should still be
	// available

	file = isi_cbm_file_open(lin, snapid, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    8192*4, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);

	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after sync for snapshot.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	memset(out_buf, 0, sizeof(out_buf));
	outlen = isi_cbm_file_read(file, out_buf,
	    8192*4, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);

	buffer[0] = byte;
	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after sync for head.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	remove_snapshot(snap_name);

	remove(path);
}

/**
 * Case to sync a file which has two chunks. After stubbing a snap is taken.
 * And one chunk is modified. The file is then synchronized back.
 */
TEST(test_sync_snap_mod_two_chunks, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_snap_mod_two_chunks_x(spec);
	spec = SPEC(CPT_AWS, false, false, small_size);
	test_sync_snap_mod_two_chunks_x(spec);
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed. Sync to azure. This first write full 512 bytes
 */
TEST(test_sync_unmod_file_az, mem_check : CK_MEM_DEFAULT, mem_hint :0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_unmod_file_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	char buffer[513];
	memset(buffer, 'T', sizeof(buffer));
	buffer[512] ='\0';
	size_t len = strlen(buffer);
	write(fd, buffer, len);

	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);

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

	fail_if(error != NULL, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	char out_buf[1024];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf, 0, sizeof(out_buf),
	    CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != len, "The read length is not matched %d %d.",
	    len, outlen);

	fail_if(strncmp(buffer, out_buf, len),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

/**
 * simple case to sync a file which only has one chunk. And the chunk is
 * modified.
 */
TEST(test_sync_mod_file_az, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	char buffer[513];
	memset(buffer, 'T', sizeof(buffer));
	buffer[512] ='\0';
	write(fd, buffer, strlen(buffer));

	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);
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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed. Sync to azure. The file size is not aligned
 * to 512 bytes.
 */
TEST(test_sync_unmod_file_unaligned_az, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_unmod_file_unaligned_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	char buffer[17];
	memset(buffer, 'T', sizeof(buffer));
	buffer[sizeof(buffer) - 1] ='\0';
	write(fd, buffer, strlen(buffer));

	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);
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

	fail_if(error != NULL, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

static void
test_sync_mod_file_unaligned_az_x(bool compress)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_unaligned_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 17;
	char buffer[buffer_size];
	// non-null terminated string, no strlen can be used
	memset(buffer, 'T', sizeof(buffer));
	write(fd, buffer, buffer_size);

	cbm_test_policy_spec spec(CPT_AZURE, compress, false, small_size);

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

	fail_if(error != NULL, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	buffer[0] = byte;
	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(buffer, out_buf, buffer_size),
	   "The data is not matched after stubbing: %17s %17s.",
	   buffer, out_buf);

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);

}
/**
 * The file size is not aligned to 512 bytes.
 * It is first stubbed and then modified and synchronized.
 */
TEST(test_sync_mod_file_unaligned_az, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_mod_file_unaligned_az_x(false);

	test_sync_mod_file_unaligned_az_x(true); // with compression
}

static void
test_sync_mod_file_snap_two_chunks_az_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_snap_two_chunks_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192 * (spec.size_.chunksize_ ? 4 : 128);
	char *buffer = (char *) malloc(buffer_size);
	std::auto_ptr<char> buffer_del(buffer);
	memset(buffer, 'A', buffer_size);
	write(fd, buffer, buffer_size);

	if (spec.compress_) {
		// make the last MB hard to compress by encrypting it
		std::vector<unsigned char>  enc_buf;
		enc_buf.resize(buffer_size);
		size_t enc_len = 0;
		cbm_test_encrypt_string(buffer, buffer_size, enc_buf.data(),
		    &enc_len, (unsigned char *)"I do not care", NULL);
		memcpy(buffer, enc_buf.data(), buffer_size);
	} else
		memset(buffer, 'B', buffer_size);

	write(fd, buffer, buffer_size);

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

	char out_buf[buffer_size];

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the first chunk

	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    (uint64_t)buffer_size, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);

	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after archive.");

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	std::string snap_name = "test_sync_mod_file_snap_two_chunks_az";
	remove_snapshot(snap_name);
	take_snapshot("/ifs", snap_name, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	memset(out_buf, 0, sizeof(out_buf));
	outlen = isi_cbm_file_read(file, out_buf,
	    (uint64_t)buffer_size, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);

	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after sync.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove_snapshot(snap_name);

	remove(path);
}

/**
 * Case to sync a file which has two chunks. And one chunk is
 * modified. A snapshot is taken The file is then synchronized back.
 */
TEST(test_sync_mod_file_snap_two_chunks_az, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);
	test_sync_mod_file_snap_two_chunks_az_x(spec);

	/*
	 * Case to sync a file which has two chunks. And one chunk is
	 * modified. A snapshot is taken The file is then synchronized back.
	 * With compression.
	 */
	spec = SPEC(CPT_AZURE, true, false, small_size);
	test_sync_mod_file_snap_two_chunks_az_x(spec);

	/*
	 * Case to sync a file which has two chunks. And one chunk is
	 * modified. A snapshot is taken The file is then synchronized back.
	 * Without compression.
	 * chunksize 32K, readsize 8k, master_cdo_size: 1MB. It shall handle
	 * 32 chunks in one page blob
	 */
	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_mod_file_snap_two_chunks_az_x(spec);

	/*
	 * Case to sync a file which has two chunks. And one chunk is
	 * modified. A snapshot is taken The file is then synchronized back.
	 * With compression.
	 * chunksize 32K, readsize 8k, master_cdo_size: 1MB. It shall handle
	 * 32 chunks in one page blob
	 */
	spec = SPEC(CPT_AZURE, true, false, small_size);
	test_sync_mod_file_snap_two_chunks_az_x(spec);
}

/**
 * simple test case to synchronize a file which is not modified after
 * stubbing. Shall succeed.
 */
TEST(test_sync_zero_lengthed_mod_file_az, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_zero_lengthed_unmod_file_az.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	const int buffer_size = 1;
	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(&byte, out_buf, buffer_size),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

// --------------- test cases for encryption added --------------------
static void
test_sync_with_encryption(const char *path, int file_size,
    bool compress, bool encrypt, cl_provider_type provider)
{
	struct isi_error *error = NULL;
	int ec = 0;

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	std::auto_ptr<char> buffer((char *)malloc(file_size));
	memset(buffer.get(), 'T', file_size);
	write(fd, buffer.get(), file_size);

	cbm_test_policy_spec spec(provider, compress, encrypt, small_size);

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

	fail_if(error != NULL, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file the file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file the file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file the file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	std::auto_ptr<char> out_buf((char *)malloc(file_size));
	memset(out_buf.get(), 0, file_size);
	size_t outlen = isi_cbm_file_read(file, out_buf.get(),
	    0,
	    file_size, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if((int)outlen != file_size,
	    "The read length is not matched %d %d.",
	    file_size, outlen);

	buffer.get()[0] = 'B';
	if (strncmp(buffer.get(), out_buf.get(), file_size)) {
		fail("The data is not matched after stubbing %s %ld %s $s.",
		   path, file_size, buffer.get(), out_buf.get());
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_sync_mod_file_encrypt,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	ANNOUNCE_TEST_START;
	test_sync_with_encryption(
	    "/ifs/data/ran_sync_encrypt.txt", 17,
	    false, true, CPT_RAN);

	test_sync_with_encryption(
	    "/ifs/data/ran_sync_encrypt.txt", small_size.chunksize_,
	    false, true, CPT_RAN);

	test_sync_with_encryption(
	    "/ifs/data/ran_sync_compress_encrypt.txt", 17,
	    true, true, CPT_RAN);

	test_sync_with_encryption(
	    "/ifs/data/ran_sync_compress_encrypt.txt", small_size.chunksize_,
	    true, true, CPT_RAN);

	test_sync_with_encryption(
	    "/ifs/data/azure_sync_compress_encrypt.txt", 17,
	    false, true, CPT_AZURE);

	test_sync_with_encryption(
	    "/ifs/data/azure_sync_compress_encrypt.txt", small_size.chunksize_,
	    false, true, CPT_AZURE);

	test_sync_with_encryption(
	    "/ifs/data/azure_sync_compress_encrypt.txt", 17,
	    true, true, CPT_AZURE);

	test_sync_with_encryption(
	    "/ifs/data/azure_sync_compress_encrypt.txt", small_size.chunksize_,
	    true, true, CPT_AZURE);
}

static void
test_sync_extend_file_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_extend_file.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}


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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the second chunk

	isi_cbm_file_write(file, &byte, 8192 * 4, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	const int out_buffer_size = 1;
	char out_buf[out_buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    8192 * 4,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(&byte, out_buf, out_buffer_size),
	   "The data is not matched after stubbing.");

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_sync_extend_file)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);

	test_sync_extend_file_x(spec);
	spec = SPEC(CPT_AWS, false, false, small_size);
	test_sync_extend_file_x(spec);
	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_extend_file_x(spec);
}

static void
test_sync_extend_file_wth_holes_x(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_extend_file_wth_holes_x.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < (spec.size_.chunksize_ ? 2 : 64); ++i) {
		write(fd, buffer, buffer_size);
	}

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B'; // turn the first byte to B in the second chunk

	uint64_t offset =  8192 * (spec.size_.chunksize_ ? 4 : 128);
	isi_cbm_file_write(file, &byte, offset, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[1];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    offset,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(&byte, out_buf, 1),
	   "The data is not matched after stubbing %1s %1s.", &byte, out_buf);

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_sync_extend_with_holes_small_ran)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_extend_file_wth_holes_x(spec);

	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_extend_file_wth_holes_x(spec);
	spec = SPEC(CPT_AZURE, true, false, small_size);
	test_sync_extend_file_wth_holes_x(spec);

	spec = SPEC(CPT_AWS, false, false, small_size);

	test_sync_extend_file_wth_holes_x(spec);
}

static void
test_sync_truncate_file_x(cbm_test_policy_spec &spec, bool trunc_to_0 = false)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_truncate_file_x.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < (spec.size_.chunksize_ ? 4 : 128); ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));
	for (int i = 0; i < (spec.size_.chunksize_ ? 4 : 128); ++i) {
		write(fd, buffer, buffer_size);
	}

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	off_t new_sz = trunc_to_0 ? 0 : (8192 * (spec.size_.chunksize_ ?
	    4 : 128));

	isi_cbm_file_ftruncate(file, new_sz, &error);

	fail_if(error, "Failed to truncate the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo;
	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(mapinfo.get_filesize() != new_sz, "File size in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo.get_filesize(), new_sz);

	isi_cfm_mapinfo::iterator it = mapinfo.begin(&error);
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	if (it != mapinfo.end()) {

		if (it->second.get_length() != (size_t)new_sz) {
			fail("File size in the mapping entry is not "
			    "expected: %ld %ld. pol: %s\n",
			    it->second.get_length(), new_sz,
			    spec.policy_name_.c_str());
		}
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	remove(path);
}

TEST(test_sync_truncate_file)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_truncate_file_x(spec);
	test_sync_truncate_file_x(spec, true); // trunc to 0

	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_truncate_file_x(spec);
	test_sync_truncate_file_x(spec, true); // trunc to 0

	spec = SPEC(CPT_AZURE, true, false, small_size);
	test_sync_truncate_file_x(spec);

	spec = SPEC(CPT_AWS, false, false, small_size);
	test_sync_truncate_file_x(spec);
	test_sync_truncate_file_x(spec, true); // trunc to 0
}

static void
test_sync_truncate_up_file_x(cbm_test_policy_spec &spec,
    bool same_region = true)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_truncate_file_x.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 1; ++i) {
		write(fd, buffer, buffer_size);
	}

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	off_t new_sz = same_region ? small_size.readsize_ + 513 :
	    small_size.chunksize_ + 513;
	isi_cbm_file_ftruncate(file, new_sz, &error);

	fail_if(error, "Failed to truncate the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	const int out_buffer_size = 2;
	char expected[out_buffer_size] = {'A', 0};
	char out_buf[out_buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    8191,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(expected, out_buf, out_buffer_size),
	   "The data is not matched after stubbing.");

	isi_cfm_mapinfo mapinfo;
	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(mapinfo.get_filesize() != new_sz, "File size in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo.get_filesize(), new_sz);

	isi_cfm_mapinfo::iterator it = mapinfo.last(&error);
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	if (it->second.get_length() != (size_t)new_sz) {
		fail("File size in the mapping entry is not "
		    "expected: %ld %ld. pol: %s\n",
		    it->second.get_length(), new_sz,
		    spec.policy_name_.c_str());
	}

	if (!same_region && spec.ptype_ == CPT_AWS) {
		// one entry is expected: the two changed entries are merged
		fail_if(mapinfo.get_count() != 1, "Expected mapentry count "
		    "is not met: %d %d", mapinfo.get_count(), 1);

		uint64_t snapid = it->second.get_object_id().get_snapid();
		fail_if(snapid != 1, "Expected version "
		    "is not met: %ld %d", snapid, 1);
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_sync_truncate_up_file)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_truncate_up_file_x(spec);
	test_sync_truncate_up_file_x(spec, false); //different region

	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_truncate_up_file_x(spec);
	test_sync_truncate_up_file_x(spec, false); //different region

	spec = SPEC(CPT_AWS, false, false, small_size);
	//test_sync_truncate_up_file_x(spec);
	test_sync_truncate_up_file_x(spec, false); //different region
}

static void
test_sync_truncate_down_file_x(cbm_test_policy_spec &spec,
    bool same_region = true)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_truncate_file_x.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = same_region ? small_size.readsize_ + 513 :
	    small_size.chunksize_ + 513;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	write(fd, buffer, buffer_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	off_t new_sz = 8192;
	isi_cbm_file_ftruncate(file, 8192, &error);

	fail_if(error, "Failed to truncate the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	const int out_buffer_size = 2;
	char expected[out_buffer_size] = {'A', 'A'};
	char out_buf[out_buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    8191,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 1,
	    "The read length is not matched: expected: %d got: %d.",
	    1, outlen);

	fail_if(strncmp(expected, out_buf, 1),
	   "The data is not matched after stubbing.");

	isi_cfm_mapinfo mapinfo;
	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(mapinfo.get_filesize() != new_sz, "File size in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo.get_filesize(), new_sz);

	isi_cfm_mapinfo::iterator it = mapinfo.begin(&error);
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	if (it != mapinfo.end()) {

		if (it->second.get_length() != (size_t)new_sz) {
			fail("File size in the mapping entry is not "
			    "expected: %ld %ld. pol: %s\n",
			    it->second.get_length(), new_sz,
			    spec.policy_name_.c_str());
		}
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_sync_truncate_down_file)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
	test_sync_truncate_down_file_x(spec);
	test_sync_truncate_down_file_x(spec, false); //different region

	spec = SPEC(CPT_AZURE, false, false, small_size);
	test_sync_truncate_down_file_x(spec);
	test_sync_truncate_down_file_x(spec, false); //different region

	spec = SPEC(CPT_AWS, false, false, small_size);
	test_sync_truncate_down_file_x(spec);
	test_sync_truncate_down_file_x(spec, false); //different region
}

enum TEST_SYNC_CHANGE_TYPE
{
	TSCT_EXTEND,
	TSCT_SHRINK
};

static void
test_sync_change_filesize_x(cbm_test_policy_spec &spec,
    TEST_SYNC_CHANGE_TYPE first_change,
    TEST_SYNC_CHANGE_TYPE second_change,
    cbm_error_type expected_error = CBM_SUCCESS)
{
	struct isi_error *error = NULL;
	int fd = -1, ec = 0;
	const char *path = "/ifs/test_sync_extend_truncate_file.txt";
	const int buffer_size = 8192;
	int count = 4;
	size_t filesize = buffer_size * count;
	char buffer[buffer_size];
	struct stat sb = {0};
	cbm_test_fail_point ufp("sync_modified_regions_err_1");
	char ufp_str[100];
	uint64_t lin;

	// Create file
	remove(path);
	fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	fail_if(fd == -1, "Open of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < count; ++i) {
		ec = write(fd, buffer, buffer_size);
		fail_if (ec == -1, "Write of file %s failed with error: %s (%d)",
		    path, strerror(errno), errno);
	}
	ec = fstat(fd, &sb);
	fail_if(ec == -1, "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	lin = sb.st_ino;
	ec = fsync(fd);
	fail_if(ec == -1,
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	close(fd);

	// Archive it
	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file %s: %#{}.\n",
	    path, isi_error_fmt(error));

	// First change: make it happen here
	if (first_change == TSCT_EXTEND) {
		filesize += 1;
	} else {// TSCT_SHRINK
		ASSERT(filesize);
		filesize -= 1;
	}
	isi_cbm_file_truncate(lin, HEAD_SNAPID, filesize, &error);
	fail_if(error, "Failed to truncate the CBM file %s: %#{}.\n",
	    path, isi_error_fmt(error));
	fd = open(path, O_RDWR);
	fail_if(fd == -1, "Open of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	ec = fsync(fd);
	fail_if(ec == -1, "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	close(fd);

	// Second change: set a failpoint to trigger it during writeback
	if (second_change == TSCT_EXTEND) {
		filesize += 1;
	} else {// TSCT_SHRINK
		ASSERT(filesize);
		filesize -= 1;
	}
	snprintf(ufp_str, sizeof(ufp_str), "return(%lu)", filesize);
	ufp.set(ufp_str);

	// Writeback
	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	if (expected_error != CBM_SUCCESS) {
		fail_if(!error, "Expected but got no error syncing file %s",
		    path);
		fail_if(!isi_cbm_error_is_a(error, expected_error),
		    "Unexpected error syncing file %s: %#{}.\n",
		    path, isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	} else {
		fail_if(error, "Failed to sync the file %s: %#{}.\n",
		    path, isi_error_fmt(error));
	}

	// Cleanup
	ufp.set("off");
	remove(path);
}

// Change filesize during writeback
TEST(test_sync_change_filesize)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);

	test_sync_change_filesize_x(spec, TSCT_EXTEND, TSCT_SHRINK);
	test_sync_change_filesize_x(spec, TSCT_EXTEND, TSCT_EXTEND);
	test_sync_change_filesize_x(spec, TSCT_SHRINK, TSCT_EXTEND);

	// When a file shrinks before and during writeback, curl callback fails
	// to read data intended for writing to the cloud.  This results in an
	// expected retryable failure.
	test_sync_change_filesize_x(spec, TSCT_SHRINK, TSCT_SHRINK,
	    CBM_CLAPI_ABORTED_BY_CALLBACK);
}


class sync_test_check_point {
public:
	static bool get_ckpt_data(const void *context,
	    void **data, size_t *ckpt_size,
	    djob_op_job_task_state *t_state, off_t offset);

	static bool set_ckpt_data(void *context, void *data, size_t ckpt_size,
	    djob_op_job_task_state *t_state, off_t offset);

	sync_test_check_point(ifs_lin_t lin , ifs_snapid_t snapid);

	explicit sync_test_check_point(const std::string &fname);

	void reset();

	~sync_test_check_point();

private:
	std::auto_ptr<ckpt_intf> impl;
};

bool
sync_test_check_point::get_ckpt_data(const void *context,
    void **data, size_t *ckpt_size,
    djob_op_job_task_state *t_state, off_t offset)
{
	sync_test_check_point *ckpt = (sync_test_check_point *)context;
	*t_state = DJOB_OJT_RUNNING;

	return ckpt->impl->get(*data, *ckpt_size, offset);
}

bool
sync_test_check_point::set_ckpt_data(void *context,
    void *data, size_t ckpt_size,
    djob_op_job_task_state *t_state, off_t offset)
{
	sync_test_check_point *ckpt = (sync_test_check_point *)context;
	*t_state = DJOB_OJT_RUNNING;

	return ckpt->impl->set(data, ckpt_size, offset);
}

sync_test_check_point::sync_test_check_point(ifs_lin_t lin ,
    ifs_snapid_t snapid)
{
	impl.reset(new file_ckpt(lin, snapid));
}

sync_test_check_point::sync_test_check_point(const std::string &fname)
{
	impl.reset(new file_ckpt(fname));
}


sync_test_check_point::~sync_test_check_point()
{
}

void
sync_test_check_point::reset()
{
	impl->remove();
}

static void
sync_with_fail_point_and_resume(uint64_t lin, const char *fail_point,
    isi_error *&error)
{
	cbm_test_fail_point ufp(fail_point);
	ufp.set("return(5)");

	sync_test_check_point ckpt(lin, HEAD_SNAPID);

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt.get_ckpt_data,
	    ckpt.set_ckpt_data,
	    &ckpt,
	    &error);

	fail_if(!error || !isi_system_error_is_a(error, EIO),
	    "Expected fail point %s not met: error: %#{}.\n",
	    fail_point, isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	ufp.set("off");

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt.get_ckpt_data,
	    ckpt.set_ckpt_data,
	    &ckpt,
	    &error);
	fail_if(error, "Failed to sync resume the CBM file: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * two chunks. both chunks are modified then synced
 */
static void
test_checkpoint_with_resume(cl_provider_type ptype,
    const char *fail_point, bool small = true,
    bool change_two_chunks = true)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);
	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < (small ? 4 : 128); ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i <  (small ? 4 : 128); ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(ptype, false, false, small_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char byte = 'B'; // turn the first byte to B in the first chunk

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	uint64_t offset = 8192*( small ? 4 : 128);

	if (change_two_chunks) {
		byte = 'A'; // turn the first byte to A in the second chunk

		isi_cbm_file_write(file, &byte, offset,
		    sizeof(byte), &error);

		fail_if(error, "Failed to write the CBM file: %#{}.\n",
		    isi_error_fmt(error));
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	sync_with_fail_point_and_resume(lin, fail_point, error);

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));

	memset(buffer, 'A', sizeof(buffer));
	buffer[0] = 'B';
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(buffer, out_buf, buffer_size),
	    "The data is not matched in 1st chunk after stubbing exp:%10s "
	    "actual:%10s.",
	    buffer, out_buf);

	if (change_two_chunks) {
		memset(buffer, 'B', sizeof(buffer));
		buffer[0] = 'A';
		memset(out_buf, 0, sizeof(out_buf));
		size_t outlen = isi_cbm_file_read(file, out_buf,
		    offset, // the next CDO
		    sizeof(out_buf), CO_CACHE, &error);
		fail_if(error, "cannot read file %s, %#{}", path,
		    isi_error_fmt(error));

		fail_if(outlen != sizeof(out_buf),
		    "The read length is not matched %d %d.",
		    sizeof(out_buf), outlen);

		fail_if(strncmp(buffer, out_buf, buffer_size),
		    "The data is not matched in 2nd chunk after stubbing "
		    "exp:%10s act:%10s.", buffer, out_buf);
	}

	isi_cbm_coi_sptr coi;
	coi = isi_cbm_get_coi(&error);
	fail_if(error, "%#{}", isi_error_fmt(error));

	std::set<ifs_lin_t> references;

	coi->get_references(file->mapinfo->get_object_id(), references, &error);
	fail_if(error, "%#{}", isi_error_fmt(error));

	fail_if(references.size() != 1 || *(references.begin()) != file->lin,
	    "%{} should be the one and only reference for %{}",
	    lin_fmt(file->lin),
	    isi_cloud_object_id_fmt(&(file->mapinfo->get_object_id())));

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}

/**
 * This test expects a fail point to be fired and resume.
 *
 */
TEST(test_checkpoint_with_resume_sync_pre_sync_sub_cdo_err_1)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pre_sync_sub_cdo_err_1");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_sub_cdo_err_1)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pos_sync_sub_cdo_err_1");
}

TEST(test_checkpoint_with_resume_sync_pre_sync_sub_cdo_err_2)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pre_sync_sub_cdo_err_2");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_sub_cdo_err_2)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pos_sync_sub_cdo_err_2");
}

TEST(test_checkpoint_with_resume_sync_pre_sync_update_cmo_err)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pre_sync_update_cmo_err");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_update_cmo_err)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_RAN, "pos_sync_update_cmo_err");
}

TEST(test_checkpoint_with_resume_sync_sync_persist_mapping_info_err)
{
	test_checkpoint_with_resume(CPT_RAN, "pre_sync_persist_mapping_info_err");
}

TEST(test_checkpoint_with_resume_sync_interrupted_checkpoint1)
{
	test_checkpoint_with_resume(CPT_RAN, "sync_interrupted_checkpoint1");
}

TEST(test_checkpoint_with_resume_sync_interrupted_checkpoint2)
{
	test_checkpoint_with_resume(CPT_RAN, "sync_interrupted_checkpoint2");
}

TEST(test_checkpoint_with_resume_sync_pre_sync_sub_cdo_err_1_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pre_sync_sub_cdo_err_1");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_sub_cdo_err_1_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pos_sync_sub_cdo_err_1");
}

TEST(test_checkpoint_with_resume_sync_pre_sync_sub_cdo_err_2_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pre_sync_sub_cdo_err_2");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_sub_cdo_err_2_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pos_sync_sub_cdo_err_2");
}

TEST(test_checkpoint_with_resume_sync_pre_sync_update_cmo_err_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pre_sync_update_cmo_err");
}

TEST(test_checkpoint_with_resume_sync_pos_sync_update_cmo_err_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pos_sync_update_cmo_err");
}

TEST(test_checkpoint_with_resume_sync_sync_persist_mapping_info_err_az)
{
	test_checkpoint_with_resume(CPT_AZURE, "pre_sync_persist_mapping_info_err");
}
/**
 * this creates a file of 2 MBs, requiring two mastr cdos, the first MB
 * is then modified and synced. the fail point is hit and resumed.
 * We are supposed to see the the collection retried
 */
TEST(test_checkpoint_with_resume_sync_pre_sync_collect_objs_err_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pre_sync_collect_objs_err",
	    false, false);
}

/**
 * post fail point for the above case
 */
TEST(test_checkpoint_with_resume_sync_pos_sync_collect_objs_err_az)
{
	ANNOUNCE_TEST_START;
	test_checkpoint_with_resume(CPT_AZURE, "pos_sync_collect_objs_err",
	    false, false);
}

/**
 * simple case to sync a file which only has one chunk. And the chunk is
 * modified. Try to writeback within the settle time, it shall fail.
 */
TEST(test_sync_mod_file_timed, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_sync_mod_file_timed.txt";
	uint64_t settle_time = 3600; // in seconds
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size,
	    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL, 0,
	    settle_time);

	cbm_test_file_helper test_fo(path, 17, "AAAAA");
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	sync();

	/*
	 * Even though the settle time hasn't been reached, this function
	 * should not return an error (which it used to do, see Bug161589).
	 * Instead, it sets up the next attempt by adding a WBI entry, which
	 * we'll check for after calling the function.
	 */
	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if (error != NULL,
	    "cbm_test_sync error: %#{}", isi_error_fmt(error));

	/*
	 * We'll verify the existence of the newly-created WBI item by
	 * removing it.  ENOENT is returned if the item doesn't exist in an
	 * entry associated with what should be the new settle time.
	 */
	isi_cbm_wbi_sptr wbi = isi_cbm_get_wbi(&error);
	fail_if (error != NULL,
	    "isi_cbm_get_wbi error: %#{}", isi_error_fmt(error));

	struct stat stat_buf;
	int stat_ret = ::stat(path, &stat_buf);
	fail_if (stat_ret != 0,
	    "stat failed: %d %s", errno, strerror(errno));

	struct timeval future_settle_time = {
	    .tv_sec = stat_buf.st_mtime + settle_time, .tv_usec = 0
	};

	wbi->remove_cached_file(lin, HEAD_SNAPID, future_settle_time, &error);
	fail_if (error != NULL,
	    "remove_cached_file error: %#{}", isi_error_fmt(error));
}

void take_snap_before_write_map(int val)
{
	std::string snap_name = "test_sync_snap_before_wrt_map";
	remove_snapshot(snap_name);
	take_snapshot("/ifs", snap_name, false, isi_error_suppress());

}

/**
 * simple case to sync a file which only has one chunk. And the chunk is
 * modified.
 */
static void
test_sync_snap_before_wrt_map_x(cl_provider_type ptype)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_snap_before_wrt_map.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	cbm_test_policy_spec spec(ptype, false, false, small_size);

	const int buffer_size = 8192 * (spec.size_.chunksize_ ? 4 : 128);
	char *buffer = (char *)malloc(buffer_size);
	std::auto_ptr<char> buffer_del(buffer);
	memset(buffer, 'A', buffer_size);
	write(fd, buffer, buffer_size);

	memset(buffer, 'B', buffer_size);

	write(fd, buffer, buffer_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	const char byte = 'B';

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_sync_register_pre_persist_map_callback(
	    take_snap_before_write_map);

	cbm_test_fail_point ufp("pre_write_map_takesnap");
	ufp.set("return(0)");

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	ufp.set("off");

	ifs_snapid_t snapid = get_snapshot_id_by_name(
	    "test_sync_snap_before_wrt_map");

	file = isi_cbm_file_open(lin, snapid, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    (uint64_t)buffer_size, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);
	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after sync for snapshot.");

	memset(buffer, 'A', buffer_size);
	buffer[0] = 'B';

	outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    8192, CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != 8192,
	    "The read length is not matched %d %d.",
	    8192, outlen);
	fail_if(strncmp(buffer, out_buf, 8192),
	   "The data is not matched after sync for snapshot.");

	isi_cbm_file_close(file, &error);

	remove_snapshot("test_sync_snap_before_wrt_map");
	remove(path);
}


TEST(test_sync_snap_before_wrt_map_az, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_snap_before_wrt_map_x(CPT_AZURE);
}

TEST(test_sync_snap_before_wrt_map_s3, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_snap_before_wrt_map_x(CPT_AWS);
}

/**
 * Bug 114881
 * Verify snapshots taken after truncation and before writeback
 */
static void
test_sync_trunc_snaps_x(cl_provider_type ptype)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_snap_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < (int)small_size.chunksize_/buffer_size; ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(ptype, false, false, small_size);
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

	std::string snap_name1 = "test_sync_mod_file_snap_two_chunks_s1";
	remove_snapshot(snap_name1);
	take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_ftruncate(file, small_size.readsize_, &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	std::string snap_name = "test_sync_mod_file_snap_two_chunks";
	remove_snapshot(snap_name);
	take_snapshot("/ifs", snap_name, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove_snapshot(snap_name);
	remove_snapshot(snap_name1);

	remove(path);
}


TEST(test_sync_trunc_snaps, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_trunc_snaps_x(CPT_RAN);
	test_sync_trunc_snaps_x(CPT_AZURE);
	test_sync_trunc_snaps_x(CPT_AWS);
}

/**
 * Bug 114879
 * Verify snapshots taken after truncation and before writeback
 */
static void
test_sync_trunc_up_snaps_x(cl_provider_type ptype)
{
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_snap_two_chunks.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < (int)small_size.chunksize_/buffer_size; ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(ptype, false, false, small_size);
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

	std::string snap_name1 = "test_sync_mod_file_snap_two_chunks_s1";
	remove_snapshot(snap_name1);
	take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_ftruncate(file, 2*small_size.chunksize_, &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	std::string snap_name = "test_sync_mod_file_snap_two_chunks";
	remove_snapshot(snap_name);
	take_snapshot("/ifs", snap_name, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the file: %#{}.\n",
	    isi_error_fmt(error));

	remove_snapshot(snap_name);
	remove_snapshot(snap_name1);

	remove(path);
}

TEST(test_sync_trunc_up_snaps, mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	ANNOUNCE_TEST_START;
	test_sync_trunc_up_snaps_x(CPT_RAN);
	test_sync_trunc_up_snaps_x(CPT_AZURE);
	test_sync_trunc_up_snaps_x(CPT_AWS);
}

/**
 * Two chunks. both chunks are modified then synced, the sync failed in
 * the middle due to truncation (simulated via the failpoint). The sync is
 * resumed, and is expected to succeed.
 */
TEST(sync_resume_on_aborted_upload)
{
	ANNOUNCE_TEST_START;
	const char *fail_point = "sync_abort_upload";
	cl_provider_type ptype = CPT_RAN;
	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);
	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i <  4; ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(ptype, false, false, small_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char byte = 'B'; // turn the first byte to B in the first chunk

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	uint64_t offset = 8192 * 4;

	byte = 'A'; // turn the first byte to A in the second chunk

	isi_cbm_file_write(file, &byte, offset,
	    sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	cbm_test_fail_point ufp(fail_point);
	ufp.set("return(1)"); // the second chunk

	sync_test_check_point ckpt(lin, HEAD_SNAPID);

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt.get_ckpt_data,
	    ckpt.set_ckpt_data,
	    &ckpt,
	    &error);

	fail_if(!error || !isi_cbm_error_is_a(error,
	    CBM_CLAPI_ABORTED_BY_CALLBACK),
	    "Expected fail point %s not met: error: %#{}.\n",
	    fail_point, isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	ufp.set("off");

	off_t trunc_len = offset + 523;
	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	isi_cbm_file_ftruncate(file, trunc_len, &error);

	fail_if(error, "Failed to truncate the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt.get_ckpt_data,
	    ckpt.set_ckpt_data,
	    &ckpt,
	    &error);
	fail_if(error, "Failed to sync resume the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	// test read it after sync
	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));

	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));

	memset(buffer, 'A', sizeof(buffer));
	buffer[0] = 'B';
	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(buffer, out_buf, buffer_size),
	    "The data is not matched in 1st chunk after stubbing exp:%10s "
	    "actual:%10s.",
	    buffer, out_buf);

	memset(buffer, 'B', sizeof(buffer));
	buffer[0] = 'A';
	memset(out_buf, 0, sizeof(out_buf));
	outlen = isi_cbm_file_read(file, out_buf,
	    offset, // the next CDO
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != (size_t)(trunc_len - offset),
	    "The read length is not matched %d %d.",
	    trunc_len - offset, outlen);

	fail_if(strncmp(buffer, out_buf, trunc_len - offset),
	    "The data is not matched in 2nd chunk after stubbing "
	    "exp:%10s act:%10s.", buffer, out_buf);

	isi_cbm_file_close(file, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
}


TEST(test_copy_stub_file)
{
	ANNOUNCE_TEST_START;
	isi_error *error = NULL;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";

	remove(path);
	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);
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

	const char *tgt = "/ifs/test_sync_mod_file_two_chunks-2.txt";

	remove(tgt);
	isi_cbm_copy_stub_file(path, tgt, &error);

	fail_if(error, "Error %#{}", isi_error_fmt(error));

	remove(path);
	remove(tgt);
}

void test_sync_invariant_x(cbm_test_policy_spec &spec)
{
	isi_error *error = NULL;
	const char *path = "/ifs/test_sync_mod_file_two_chunks.txt";
	ifs_lin_t lin1, lin2;
	remove(path);
	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];

	// start with three chunks
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}
	memset(buffer, 'B', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

	memset(buffer, 'C', sizeof(buffer));

	for (int i = 0; i < 4; ++i) {
		write(fd, buffer, buffer_size);
	}

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	lin1 = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin1,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	// copy the stub file
	const char *tgt = "/ifs/test_sync_mod_file_two_chunks-2.txt";

	remove(tgt);
	isi_cbm_copy_stub_file(path, tgt, &error);

	fail_if(error, "Error %#{}", isi_error_fmt(error));

	struct isi_cbm_file *file1 = NULL;
	// now truncate the source file to only one CDO
	off_t trunc_len = small_size.chunksize_;
	file1 = isi_cbm_file_open(lin1, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	isi_cbm_file_ftruncate(file1, trunc_len, &error);

	fail_if(error, "Failed to truncate the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file1, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_sync(lin1, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the CBM file: %#{}.\n",
	    isi_error_fmt(error));


	int fd2 = open(tgt, O_RDONLY);

	fstat(fd2, &sb);
	lin2 = sb.st_ino;

	close(fd2);
	struct isi_cbm_file *file2 = NULL;
	// now modify and sync the target file
	file2 = isi_cbm_file_open(lin2, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	const char byte = 'B';

	// write the beginning of the first chunk
	isi_cbm_file_write(file2, &byte, 2 * small_size.chunksize_,
	    sizeof(byte), &error);

	isi_cbm_file_close(file2, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	cbm_test_sync(lin2, HEAD_SNAPID, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to sync the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	// now it should have CDO1_1, CDO2_3, CDO3_3
	file2 = isi_cbm_file_open(lin2, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", path,
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo2;
	isi_cph_get_stubmap(file2->fd, mapinfo2, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	off_t sz = 3 * small_size.chunksize_;
	fail_if(mapinfo2.get_filesize() != sz, "File size in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo2.get_filesize(), sz);

	fail_if(mapinfo2.get_count() != 2, "The entries count in the mapping "
	    "info is not expected: %ld %ld",
	    mapinfo2.get_count(), 2);

	isi_cfm_mapinfo::iterator it = mapinfo2.begin(&error);
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	int i = 0;
	for (; it != mapinfo2.end(); it.next(&error), ++i) {
		uint64_t snapid = it->second.get_object_id().get_snapid();

		if (i == 0) {
			fail_if(snapid != 0, "The version id is not "
			    "expected: %d %d", snapid, 0);
		} else {
			fail_if(snapid != 2, "The version id is not "
			    "expected: %d %d", snapid, 2);
		}

		size_t len = ((i == 0) ? 1 : 2) * (size_t)small_size.chunksize_;

		if (it->second.get_length() != len) {
			fail("File size in the mapping entry %d is not "
			    "expected: %ld %ld. pol: %s\n", i,
			    it->second.get_length(), len,
			    spec.policy_name_.c_str());
		}
	}
	fail_if(error, "Failed to get the map iterator: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_file_close(file2, &error);
	fail_if(error, "cannot close file %s, %#{}", path,
	    isi_error_fmt(error));

	remove(path);
	remove(tgt);

}

TEST(test_sync_invariant_s3)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_AWS, false, false, small_size);

	test_sync_invariant_x(spec);
}

TEST(test_sync_invariant_az)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_AZURE, false, false, small_size);

	test_sync_invariant_x(spec);
}

TEST(test_sync_invariant_ran)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);

	test_sync_invariant_x(spec);
}

static bool
check_cloud_object(isi_cloud_object_id &oid, isi_cbm_ioh_base_sptr ioh,
    int ilst[], int n, bool exists)
{
	struct isi_error *error = NULL;
	isi_cloud::str_map_t attr_map;
	bool ret = true;

	for (int i = 0; i < n && ret; ++i) {
		try {
			struct cl_object_name obj_name;

			obj_name.container_cmo = "unused";
			obj_name.container_cdo =
			    ioh->get_account().container_cdo;
			obj_name.obj_base_name = oid;

			ioh->get_cdo_attrs(obj_name, ilst[i], attr_map);
		}
		CATCH_CLAPI_EXCEPTION(NULL, error, false);

		if (exists != !error)
			ret = false;

		if (error)
			isi_error_free(error);
	}

	return ret;
}

static isi_cbm_account
get_stub_account(struct isi_cbm_file *file)
{
	struct isi_error *error = NULL;
	scoped_ppi_reader ppi_reader;
	isi_cbm_id aid = file->mapinfo->get_account();
	isi_cbm_account acct;

	struct isi_cfm_policy_provider *ppi =
	    (struct isi_cfm_policy_provider *) ppi_reader.get_ppi(&error);
	fail_if(error, "failed to get ppi: %#{}", isi_error_fmt(error));

	isi_cfm_get_account((isi_cfm_policy_provider *)ppi, aid, acct, &error);
	fail_if(error, "failed to get account: %#{}", isi_error_fmt(error));

	return acct;
}

void
ckpt_rollback_after_broken_sync(const std::string &fpath,
    const std::string &fname, cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;

	isi_cbm_test_sizes &sizes = spec.size_;

	cbm_test_file_helper test_fo(
	    (fpath + fname).c_str(), 3 * sizes.chunksize_, "");
	ifs_lin_t lin = test_fo.get_lin();

	error = cbm_test_archive_file_with_pol(
	    lin, spec.policy_name_.c_str(), false);
	fail_if(error, "failed to archive file: %#{}", isi_error_fmt(error));

	// take a snapshot so to verify the data integrity
	remove_snapshot("firstsnap");
	ifs_snapid_t snapid = take_snapshot(fpath, "firstsnap", false, &error);
	fail_if(error, "failed to take snapshot: %{}", isi_error_fmt(error));

	// open & write cdo 1/2
	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open file: %#{}", isi_error_fmt(error));

	isi_cbm_account acct = get_stub_account(file);

	isi_cloud_object_id v0 = file->mapinfo->get_object_id();
	isi_cloud_object_id v1 = v0;

	v1.set_snapid(v0.get_snapid() + 1); // only s3 works

	isi_cbm_file_write(file,
	    "hello, world", 0 * sizes.chunksize_, 11, &error);
	fail_if(error, "failed to write stub file: %#{}", isi_error_fmt(error));

	isi_cbm_file_write(file,
	    "hello, world", 1 * sizes.chunksize_, 11, &error);
	fail_if(error, "failed to write stub file: %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close stub file: %#{}", isi_error_fmt(error));

	// sync cdo 1, fail on cdo 2
	sync_test_check_point ckpt(lin, HEAD_SNAPID);
	{
		// sync fail on second CDO
		cbm_test_fail_point ufp(
		    "sync_abort_upload");
		ufp.set("return(1)");

		cbm_test_sync(lin, HEAD_SNAPID,
		    ckpt.get_ckpt_data, ckpt.set_ckpt_data, &ckpt, &error);
		fail_if(!error, "error expected here on failpoint");

		if (error) {
			isi_error_free(error);
			error = NULL;
		}

		ufp.set("off");
	}

	isi_cbm_ioh_base_sptr ioh(
	    isi_cbm_ioh_creator::get_io_helper(acct.type, acct));
	int cdo_idx[] = {1, 2, 3};

	// v0 contains CDO: 1, 2, 3
	fail_if(!check_cloud_object(v0, ioh, cdo_idx, 3, true),
	    "expect 3 CDOs on v0");
	// v1 contains CDO: 1
	fail_if(!check_cloud_object(v1, ioh, cdo_idx, 1, true),
	    "expect 1 CDOs on v1");
	fail_if(!check_cloud_object(v1, ioh, &cdo_idx[1], 2, false),
	    "expect 1 CDOs on v1");

	// remove stub
	system(("isi_run -c rm " + fpath + fname).c_str());

	// sync again
	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt.get_ckpt_data, ckpt.set_ckpt_data, &ckpt, &error);
	fail_if(error, "error expected here on failpoint");

	// v0 contains CDO: 1, 2, 3
	fail_if(!check_cloud_object(v0, ioh, cdo_idx, 3, true),
	    "expect 3 CDOs on v0");
	// after rollback, v1 contains CDO: N/A
	//if (spec.ptype_ == CPT_AWS)
		//fail_if(!check_cloud_object(v1, ioh, cdo_idx, 3, false),
		    //"expect 0 CDOs on v0");

	// verify the cloud object integrity on v0
	file = isi_cbm_file_open(lin, snapid, 0, &error);
	fail_if(error, "failed to open file: %{}", isi_error_fmt(error));

	std::vector<char> buf(sizes.chunksize_);
	for (int i = 0; i < 3; ++i) {
		size_t ret = isi_cbm_file_read(file,
		    buf.data(), i * buf.size(), buf.size(), CO_NOCACHE, &error);

		fail_if(error, "failed to read data: %{}", isi_error_fmt(error));
		fail_if(ret != buf.size(),
		    "data size doesn't match: %ld %ld", ret, buf.size());
	}
	isi_cbm_file_close(file, &error);
	fail_if(error, "unexpected: %{}", isi_error_fmt(error));

	// remove the snapshot
	remove_snapshot("firstsnap");
}

TEST(test_gc_after_broken_sync, mem_check: CK_MEM_DEFAULT)
{
	ANNOUNCE_TEST_START;
	cbm_test_policy_spec s3_spec(CPT_AWS,   false, false, small_size);

	// Intentional failures come from this test - disable verbose logging
	override_clapi_verbose_logging_enabled(false);

	ckpt_rollback_after_broken_sync(
	    "/ifs/data/", "check_gc_after_broken_sync_s3.txt", s3_spec);

	revert_clapi_verbose_logging();
}

/**
 * The test simulate the following
 * 1. Syncer 1, did some work and suspended due to errors
 * 2. Syncer 2, try to sync, should fail
 * 3. Syncer 1, resume syncing, should succeed
 * 4. Syncer 2, retry sync, should succeed
 */
void test_durable_sync_lock_impl(cl_provider_type ptype)
{

	struct isi_error *error = NULL;
	int ec = 0;
	const char *path = "/ifs/test_durable_sync_lock.txt";

	remove(path);
	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i <  4; ++i) {
		write(fd, buffer, buffer_size);
	}

	cbm_test_policy_spec spec(ptype, false, false, small_size);

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

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	char byte = 'B'; // turn the first byte to B in the first chunk

	isi_cbm_file_write(file, &byte, 0, sizeof(byte), &error);

	fail_if(error, "Failed to write the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo;
	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	isi_cloud_object_id oid = mapinfo.get_object_id();
	isi_cbm_file_close(file, &error);

	isi_cbm_coi_sptr coi;

	coi = isi_cbm_get_coi(&error);
	fail_if (error != NULL, "failure to get COI: %#{}",
	    isi_error_fmt(error));

	fd = open(path, O_RDWR);
	fail_if(fd < 0, "Failed to open the file: errno:%d.\n", errno);

	ec = fsync(fd);
	fail_if(ec, "Failed to fsync the file: errno:%d.\n", errno);

	close(fd);

	sync_test_check_point ckpt1("test_durable_sync_lock_ckpt1");
	sync_test_check_point ckpt2("test_durable_sync_lock_ckpt2");

	ckpt1.reset();
	ckpt2.reset();

	cbm_test_fail_point ufp("sync_stublock_contention");
	ufp.set("return(35)");

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt1.get_ckpt_data,
	    ckpt1.set_ckpt_data,
	    &ckpt1,
	    &error);

	fail_if(!error ||
	    !isi_cbm_error_is_a(error, CBM_DOMAIN_LOCK_CONTENTION),
	    "Expected error CBM_DOMAIN_LOCK_CONTENTION not found: %#{}.\n",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	cmoi_entry cmo_entry;
	coi->get_cmo(oid, cmo_entry, &error);
	fail_if (error != NULL, "failure to get the CMO: %#{}",
	    isi_error_fmt(error));

	fail_if (cmo_entry.get_syncing() != true, "Sync bit should have been "
	    "turned on");

	ufp.set("off");

	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt2.get_ckpt_data,
	    ckpt2.set_ckpt_data,
	    &ckpt2,
	    &error);

	fail_if(!error ||
	    !isi_cbm_error_is_a(error, CBM_SYNC_ALREADY_IN_PROGRESS),
	    "Expected error CBM_SYNC_ALREADY_IN_PROGRESS not found: %#{}.\n",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	coi->get_cmo(oid, cmo_entry, &error);
	fail_if (error != NULL, "failure to get the CMO: %#{}",
	    isi_error_fmt(error));

	fail_if (cmo_entry.get_syncing() != true, "Sync bit should have been "
	    "turned on");
	fail_if (cmo_entry.get_syncer_id() != 1, "Syncer id should be 1");

	// now resume sync 1:
	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt1.get_ckpt_data,
	    ckpt1.set_ckpt_data,
	    &ckpt1,
	    &error);

	fail_if(error, "Failed to sync the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	coi->get_cmo(oid, cmo_entry, &error);
	fail_if (error != NULL, "failure to get the CMO: %#{}",
	    isi_error_fmt(error));

	fail_if (cmo_entry.get_syncing() != false, "Sync bit should have been "
	    "turned off");
	fail_if (cmo_entry.get_syncer_id() != 1, "Syncer id should be 1");

	// now resume sync 2:
	cbm_test_sync(lin, HEAD_SNAPID,
	    ckpt2.get_ckpt_data,
	    ckpt2.set_ckpt_data,
	    &ckpt2,
	    &error);

	fail_if(error, "Failed to sync the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	coi->get_cmo(oid, cmo_entry, &error);
	fail_if (error != NULL, "failure to get the CMO: %#{}",
	    isi_error_fmt(error));

	fail_if (cmo_entry.get_syncing() != false, "Sync bit should have been "
	    "turned off");
	fail_if (cmo_entry.get_syncer_id() != 2, "Syncer id should be 2");
	remove(path);
}

TEST(test_durable_sync_lock)
{
	ANNOUNCE_TEST_START;
	test_durable_sync_lock_impl(CPT_AWS);
}

