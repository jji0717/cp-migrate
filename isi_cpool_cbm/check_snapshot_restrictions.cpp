#include <check.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <isi_snapshot/snapshot.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_ufp/isi_ufp.h>
#include "isi_cpool_cbm/isi_cbm_gc.h"
#include "isi_cpool_cbm/isi_cbm_archive_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_coi.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_scoped_ppi.h"
#include "isi_cpool_cbm/isi_cbm_account_util.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"


static cbm_test_env env;

static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

static void delete_all_cfg()
{
	env.cleanup();
}

#define SPEC cbm_test_policy_spec
#define MEMORY_CHECK_SBT_SIZE 8192

static void toss_your_cookies(void)
{
	throw cl_exception(CL_PARTIAL_FILE, "PreLeak memory from throw");
}

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	// now load the config
	isi_cbm_reload_config(& error);

	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));

	cpool_regenerate_mek(& error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(& error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(false, true);
	cbm_test_common_setup(false, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Failed to init failpoints");

	cbm_test_leak_check_primer(true);
	try {
		// Throw an exception before running any tests so that
		// the mutexes used by throw will be preallocated and
		// in the memory snapshot used to look for leaks.  In the
		// normal case the memory is cleaned up by image rundown
		// but we do not run down between tests here.
		toss_your_cookies();
	}
	catch (...) {
		CHECK_P("threw and caught an exception to preAllocate memory\n");
	}

	restart_leak_detection(0);
}

#define CREATE_STRING	\
    "dd if=/dev/random of=%s bs=$(( 1024 )) count=1"
#define TEST_FILE "/ifs/data/check_snapshot_restrictions.test"

TEST_FIXTURE(suite_teardown)
{
	delete_all_cfg();
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
	// remove the snap
	std::string snap_name1 = "test_cbm_snapshot_restrictions";
	remove_snapshot(snap_name1);
	unlink(TEST_FILE);
}

SUITE_DEFINE_FOR_FILE(check_snapshot_restrictions,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);

TEST(test_normal_operation)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	ifs_snapid_t snapid;
	SNAP *snap = NULL;
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status && errno != ENOENT,
	    "Failed to delete file %d",
	    errno);

	// Create a 1KB file
	fmt_print(&str, CREATE_STRING, TEST_FILE);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);
	// take a snap
	std::string snap_name1 = "test_cbm_snapshot_restrictions";
	remove_snapshot(snap_name1);
	snapid = take_snapshot("/ifs/data", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}",
	    isi_error_fmt(error));
	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed", snapid);

	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;

	// Archive the file
	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, true);
	fail_if(error != NULL,
	    "Archive should succeed!  %#{}",
	    isi_error_fmt(error));
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status,
	    "Failed to delete file %d",
	    errno);
	// begin snap deletion
	remove_snapshot(snap_name1);

	// finish snap deletion
	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to finish destroying snapshot: %#{}",
	    isi_error_fmt(error));
	// cleanup
	snapshot_close(snap);

}

TEST(test_restricted_operation)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	ifs_snapid_t snapid;
	SNAP *snap = NULL;
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status && errno != ENOENT,
	    "Failed to delete file %d",
	    errno);

	// Create a 1KB file
	fmt_print(&str, CREATE_STRING, TEST_FILE);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);
	// take a snap
	std::string snap_name1 = "test_cbm_snapshot_restrictions";
	remove_snapshot(snap_name1);
	snapid = take_snapshot("/ifs/data", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}",
	    isi_error_fmt(error));
	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed", snapid);

	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;

	// Archive the file
	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN_NO_SNAPSHOT, true);
	fail_if(error == NULL || !isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "Archive should fail with CBM_PERM_ERROR!  %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status,
	    "Failed to delete file %d",
	    errno);
	// begin snap deletion
	remove_snapshot(snap_name1);

	// finish snap deletion
	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to finish destroying snapshot: %#{}",
	    isi_error_fmt(error));
	// cleanup
	snapshot_close(snap);

}
