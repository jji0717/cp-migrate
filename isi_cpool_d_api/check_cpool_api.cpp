#include <ifs/ifs_types.h>
#include <ifs/bam/bam_cpool.h>
#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/pq/pq.h>
#include <sys/stat.h>
#include <sys/proc.h>
#include <sys/module.h>
#include <sys/isi_lwext.h>

#include <check.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>

#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/task_key.h>

#include "cpool_api.h"

const int TEST_FAKE_POLICY_ID = 9999;

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_cpool_api,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.timeout = 60,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	// to prevent memory leak messages resulting from static
	// heap variables.
	isi_cloud_object_id object_id;
	cpool_fd_store::getInstance();
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
}

/**
 * Deletes SBTs for all task types up to priority DEL_PRIORITY_MAX.
 */
#define DEL_PRIORITY_MAX 50
#define DEL_JOBS_MAX 50
TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = NULL;
	char *name_buf = NULL;

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < DEL_PRIORITY_MAX; ++pri) {
			name_buf = get_pending_sbt_name(op_type->get_type(),
			    pri);
			delete_sbt(name_buf, &error);
			fail_if (error != NULL, "failed to delete SBT: %#{}",
			    isi_error_fmt(error));
			free(name_buf);
		}

		name_buf = get_inprogress_sbt_name(op_type->get_type());
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete SBT: %#{}",
		    isi_error_fmt(error));
		free(name_buf);
	}

	for (djob_id_t job_id = 0; job_id < DEL_JOBS_MAX; ++job_id) {
		name_buf = get_djob_file_list_sbt_name(job_id);
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete SBT: %#{}",
		    isi_error_fmt(error));
		free(name_buf);
	}

	name_buf = get_daemon_jobs_sbt_name();
	delete_sbt(name_buf, &error);
	fail_if (error != NULL, "failed to delete SBT: %#{}",
	    isi_error_fmt(error));
	free(name_buf);

	/*
	 * Enable stub access for this test.
	 */
	struct procoptions po = PROCOPTIONS_INIT;

	if (getprocoptions(&po)) {
		fail("Could not get current process options from kernel "
		    "%d-%s", errno, strerror(errno));
	}

	po.po_isi_flag_on |= PI_CPOOL_ALLOW_ACCESS_MASK;
	if (setprocoptions(&po) < 0) {
		fail("Failed to enable access to stubbed files - %d-%s",
		    errno, strerror(errno));
	}
}

/**
 * test archive a simple file. should succeed
 */
TEST(simple_archive, .mem_check = CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task_map.
	 */
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    0, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 0: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL,
	    "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create a file to archive. */
	const char *path = "/ifs/simple_file.txt";
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

	fsync(fd);

	/* Archive the file. */
	djob_id_t djob_id = DJOB_RID_INVALID;

	archive_file(&djob_id, fd, "test file name", TEST_FAKE_POLICY_ID,
	    ISI_CPOOL_D_PRI_LOW, false, false, &error);
	fail_if(error, "Failed to archive_file. Error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id == DJOB_RID_INVALID);

	close(fd);

	remove(path);
}

/**
 * This case try to archive a file with ADS and then try to archive the ADS
 * The first should succeed, the second should fail.
 */
TEST(archive_ads, .mem_check = CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task_map.
	 */
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    0, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 0: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL,
	    "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create a file to archive. */
	const char *path = "/ifs/file_with_ads.txt";
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
	fsync(fd);

	/*
	 * Open the ADS directory holding all streams.
	 */
	int adsd_fd = enc_openat(fd, "." , ENC_DEFAULT, O_RDONLY|O_XATTR);
	fail_if(adsd_fd < 0, "Opening ADS stream directory");

	int ads_fd = enc_openat(adsd_fd, "test_ads_stream",
	    ENC_DEFAULT, O_CREAT);
	fail_if(ads_fd < 0, "Opening ADS stream");

	djob_id_t djob_id = DJOB_RID_INVALID;

	archive_file(&djob_id, fd, "test file name", TEST_FAKE_POLICY_ID,
	    ISI_CPOOL_D_PRI_LOW, false, false, &error);
	fail_if(error, "Failed to archive the main file with ADS. "
	    "Error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_id == DJOB_RID_INVALID);

	djob_id = DJOB_RID_INVALID;
	archive_file(&djob_id, ads_fd, "test ads name", TEST_FAKE_POLICY_ID,
	    ISI_CPOOL_D_PRI_LOW, false, false, &error);
	fail_if(!error || !isi_system_error_is_a(error, EINVAL),
	    "Archive of ADS shall fail. Error: %#{}",
	    isi_error_fmt(error));

	close(adsd_fd);
	close(ads_fd);
	close(fd);

	remove(path);

	isi_error_free(error);
}

TEST(get_daemon_job_id_for_smartpools__no_djob)
{
	struct isi_error *error = NULL;
	jd_jid_t je_id = 100001;	// an unlikely-to-be-there value
	djob_id_t djob_id = DJOB_RID_INVALID;
	daemon_job_record *djob_record = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	/*
	 * Don't create any daemon jobs - this should force the creation of one
	 * by get_daemon_job_id_for_smartpools.
	 */
	djob_id = get_daemon_job_id_for_smartpools(je_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (djob_id == DJOB_RID_INVALID);

	djob_record = isi_cloud::daemon_job_manager::get_daemon_job_record(
	    djob_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	fail_if (djob_record->get_operation_type() != op_type,
	    "operation type mismatch (exp: %{} act: %{})",
	    task_type_fmt(op_type->get_type()),
	    task_type_fmt(djob_record->get_operation_type()->get_type()));
	fail_if (djob_record->get_job_engine_job_id() != je_id,
	    "JE job ID mismatch (exp: %u act: %u)",
	    je_id, djob_record->get_job_engine_job_id());
	fail_if (djob_record->get_job_engine_state() != STATE_RUNNING,
	    "JE job state mismatch (exp: %{} act: %{})",
	    enum_jobstat_state_fmt(STATE_RUNNING),
	    enum_jobstat_state_fmt(djob_record->get_job_engine_state()));

	delete djob_record;
	djob_record = NULL;

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(get_daemon_job_id_for_smartpools__running_djob)
{
	struct isi_error *error = NULL;
	jd_jid_t je_id = 100001;	// an unlikely-to-be-there value
	djob_id_t djob_id = DJOB_RID_INVALID, djob_id_2 = DJOB_RID_INVALID;
	daemon_job_record *djob_record = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	/*
	 * Use get_daemon_job_id_for_smartpools to create a daemon job.  We can
	 * then call get_daemon_job_id_for_smartpools again with the same JE
	 * job ID and verify we get the same daemon job ID back.
	 */
	djob_id = get_daemon_job_id_for_smartpools(je_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (djob_id == DJOB_RID_INVALID);

	djob_record = isi_cloud::daemon_job_manager::get_daemon_job_record(
	    djob_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	fail_if (djob_record->get_operation_type() != op_type,
	    "operation type mismatch (exp: %{} act: %{})",
	    task_type_fmt(op_type->get_type()),
	    task_type_fmt(djob_record->get_operation_type()->get_type()));
	fail_if (djob_record->get_job_engine_job_id() != je_id,
	    "JE job ID mismatch (exp: %u act: %u)",
	    je_id, djob_record->get_job_engine_job_id());
	fail_if (djob_record->get_job_engine_state() != STATE_RUNNING,
	    "JE job state mismatch (exp: %{} act: %{})",
	    enum_jobstat_state_fmt(STATE_RUNNING),
	    enum_jobstat_state_fmt(djob_record->get_job_engine_state()));

	delete djob_record;
	djob_record = NULL;

	djob_id_2 = get_daemon_job_id_for_smartpools(je_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (djob_id_2 == DJOB_RID_INVALID);
	fail_if (djob_id != djob_id_2,
	    "daemon job ID mismatch (exp: %u act: %u)", djob_id, djob_id_2);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

TEST(get_daemon_job_id_for_smartpools__non_running_djob)
{
	struct isi_error *error = NULL;
	jd_jid_t je_id = 100001;	// an unlikely-to-be-there value
	djob_id_t djob_id = DJOB_RID_INVALID, djob_id_2 = DJOB_RID_INVALID;
	daemon_job_record *djob_record = NULL;
	const void *old_rec_ser, *new_rec_ser;
	size_t old_rec_ser_size, new_rec_ser_size;
	int djob_sbt_fd = -1;
	btree_key_t djob_key;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	isi_cloud::daemon_job_manager::at_startup(&error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	djob_id = get_daemon_job_id_for_smartpools(je_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (djob_id == DJOB_RID_INVALID);

	djob_record = isi_cloud::daemon_job_manager::get_daemon_job_record(
	    djob_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	djob_record->save_serialization();

	fail_if (djob_record->get_operation_type() != op_type,
	    "operation type mismatch (exp: %{} act: %{})",
	    task_type_fmt(op_type->get_type()),
	    task_type_fmt(djob_record->get_operation_type()->get_type()));
	fail_if (djob_record->get_job_engine_job_id() != je_id,
	    "JE job ID mismatch (exp: %u act: %u)",
	    je_id, djob_record->get_job_engine_job_id());
	fail_if (djob_record->get_job_engine_state() != STATE_RUNNING,
	    "JE job state mismatch (exp: %{} act: %{})",
	    enum_jobstat_state_fmt(STATE_RUNNING),
	    enum_jobstat_state_fmt(djob_record->get_job_engine_state()));

	/* Modify the daemon job record so that it is no longer running. */
	djob_record->set_state(DJOB_OJT_COMPLETED);

	djob_record->get_saved_serialization(old_rec_ser, old_rec_ser_size);
	djob_record->get_serialization(new_rec_ser, new_rec_ser_size);

	djob_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false,
	    &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));

	djob_key = djob_record->get_key();

	fail_if (ifs_sbt_cond_mod_entry(djob_sbt_fd, &djob_key, new_rec_ser,
	    new_rec_ser_size, NULL, BT_CM_BUF, old_rec_ser, old_rec_ser_size,
	    NULL) != 0,
	    "failed to update daemon job record: %s (%d)",
	    strerror(errno), errno);

	delete djob_record;
	djob_record = NULL;

	/*
	 * Although we're using the same JE job ID, we shouldn't get back the
	 * initial (and now non-running) daemon job ID.
	 */
	djob_id_2 = get_daemon_job_id_for_smartpools(je_id, &error);
	fail_if (error != NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (djob_id_2 == DJOB_RID_INVALID);
	fail_if (djob_id == djob_id_2,
	    "should have different daemon job ID (both: %u)", djob_id);

	/* Cleanup the gconfig context to prevent bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}
