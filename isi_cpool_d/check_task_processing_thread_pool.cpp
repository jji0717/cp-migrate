#include <iostream>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_util/isi_error.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_api/client_task_map.h>
#include "server_task_map.h"
#include "task_processing_thread_pool.cpp"
#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cloud_api/cl_provider_common.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_test_fail_point.h>

// not expect any memory leak
#define EXPECTED_LEAKS   0
#define THROTTLED_ILOG_BYTES_LEAKED 64
#define CLEANUP_CONFIG_CONTEXT \
	struct isi_error *error = NULL;\
	scoped_cfg_writer writer; \
	writer.reset_cfg(&error); \
	fail_if (error != NULL, "unexpected error: %#{}", isi_error_fmt(error));

#define INIT_SERVER_TASK_MAP_HELPER(stm) \
	stm =  new server_task_map(op_type, num_priorities); \
	stm->initialize(&error); \
	fail_if (error != NULL, "unexpected error: %#{}", isi_error_fmt(error));


using namespace isi_cloud;
using namespace std;


int TEST_POLICY_ID = 0;
const char *TEST_POLICY_RENAME = "cbm_test_common_ran_policy_rename";
const char *TEST_POLICY = CBM_TEST_COMMON_POLICY_RAN;

void
check_cpool_rename_policy_helper(const char * name, const char * new_name, bool error_if_no_cur_pol)
{
	ASSERT(name != NULL);
	ASSERT(new_name != NULL);

	isi_error *error = NULL;
	smartpools_context *sp_ctx = NULL;
	fp_policy *policy = NULL;

	smartpools_open_context(&sp_ctx, &error);
	fail_if (error != NULL, "opening_sp_context unexpected error: %#{}",
	    isi_error_fmt(error));

	smartpools_get_policy_by_name(name, &policy, sp_ctx, &error);
	if (error != NULL) {
		if (error_if_no_cur_pol) {
			fail ("fail to get policy name %s: %#{}",
			    name, isi_error_fmt(error));
		} else {
			// if the old policy does not exist
			// we just exit and do not treat it as an error
			isi_error_free(error);
			error = NULL;
			smartpools_close_context(&sp_ctx, &error);
			fail_if (error != NULL, "smartpools_close_context unexpected error: %#{}",
			    isi_error_fmt(error));
			return;
		}
	}

	free(policy->name);
	policy->name = strdup(new_name);
	smartpools_mark_updated_policy(policy, &error);
	fail_if (error != NULL, "smartpools_mark_updated_policy unexpected error: %#{}",
	isi_error_fmt(error));

	smartpools_save_all_policies(sp_ctx, &error);
	fail_if (error != NULL, "smartpools_save_all_policies unexpected error: %#{}",
	isi_error_fmt(error));

	smartpools_commit_changes(sp_ctx, &error);
	fail_if (error != NULL, "smartpools_commit_changes unexpected error: %#{}",
	isi_error_fmt(error));

	smartpools_close_context(&sp_ctx, &error);
	fail_if (error != NULL, "smartpools_close_context unexpected error: %#{}",
	isi_error_fmt(error));

	//have to call unload the config after rename
	//otherwise the operation failed
	isi_cbm_unload_conifg();

}

void check_archive_task_upgrade_helper(bool is_upgraded)
{
	if (is_upgraded) {
		int fd_tmp = open(CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE, O_RDWR | O_TRUNC | O_CREAT, 0600);
		fail_if (fd_tmp < 0, "cannot create CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE for write");
	} else {
		int retc = remove(CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE);
		fail_if ((retc < 0 && errno != ENOENT),
		    "removing file failed %s", strerror(errno));
	}
}

TEST_FIXTURE(suite_setup)
{
	// make sure env is clean
	cbm_test_common_delete_policy(TEST_POLICY_RENAME);
	cbm_test_common_cleanup(true,true);
	cbm_test_common_setup(true,true);

	fail_if(!cbm_test_cpool_ufp_init(), "failed to int failpoints");

	cpool_fd_store::getInstance();
	// disable cpool_d to make sure the task created in the test case
	// won't be affected by it
	system("isi services -a isi_cpool_d disable");
	system("killall -9 isi_cpool_d");
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_delete_policy(TEST_POLICY_RENAME);

	cbm_test_common_cleanup(true,true);
	system("isi services -a isi_cpool_d enable");
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
	struct isi_error *error = NULL;
	char *name_buf = NULL;
	const cpool_operation_type *op_type = NULL;

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < 50; ++pri) {
			name_buf = get_pending_sbt_name(op_type->get_type(),
			    pri);
			delete_sbt(name_buf, &error);
			fail_if (error != NULL,
			    "failed to delete SBT (%s): %#{}",
			    name_buf, isi_error_fmt(error));
			free(name_buf);
		}

		name_buf = get_inprogress_sbt_name(op_type->get_type());
		delete_sbt(name_buf, &error);
		fail_if (error != NULL,
		    "failed to delete SBT (%s): %#{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}

	for (djob_id_t job_id = 0; job_id < 50; ++job_id) {
		name_buf = get_djob_file_list_sbt_name(job_id);
		delete_sbt(name_buf, &error);
		fail_if (error != NULL,
		    "failed to delete SBT (%s): %#{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}

	name_buf = get_daemon_jobs_sbt_name();
	delete_sbt(name_buf, &error);
	fail_if (error != NULL,
	    "failed to delete SBT (%s): %#{}",
	    name_buf, isi_error_fmt(error));
	free(name_buf);

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();

	/**
	 * Release any memory allocated by the store.
	 */
	cpool_fd_store::getInstance().close_sbt_fds();

	/* if renamed policy exist, rename it back */
	check_cpool_rename_policy_helper(TEST_POLICY_RENAME, TEST_POLICY, false);

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		CLEANUP_CONFIG_CONTEXT;
	}
}

SUITE_DEFINE_FOR_FILE(check_task_processing_thread_pool,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = suite_setup,
	.suite_teardown = suite_teardown,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 300,
	.fixture_timeout = 1200);

task_process_completion_reason
check_process_task_helper(const int expected_version,
    const task_process_completion_reason expected_reason,
    server_task_map *stm,
    task * started_task,
    archive_task * arc_task)
{
	struct isi_error *error = NULL;

	task_ckpt_helper tch(stm, started_task);

	task_process_completion_reason reason;

	process_task(stm, &tch, arc_task, reason, &error);
	fail_if (error != NULL, "process_task unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (reason != expected_reason,
	    "mismatch task proccess completion reason (exp (%{}) act (%{}))",
	    task_process_completion_reason_fmt(expected_reason),
	    task_process_completion_reason_fmt(reason));
	task *retrieved_task = stm->find_task(started_task->get_key(),
	    true, &error);
	fail_if (retrieved_task == NULL, "retrieved_task is NULL");
	fail_if (error != NULL, "retrieved_task unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (retrieved_task->get_version() != expected_version,
	    "task version mismatch (exp %d act %d)",
	    expected_version, retrieved_task->get_version());

	delete retrieved_task;
	retrieved_task = NULL;
	return reason;
}

void check_finish_task_helper(task_process_completion_reason reason,
    server_task_map *stm,
    task * started_task)
{
	struct isi_error *error = NULL;
	pre_finish_task(started_task, stm, reason, &error);
	fail_if (error != NULL, "pre_finish_task unexpected error: %#{}",
	    isi_error_fmt(error));

	stm->finish_task(started_task->get_key(), reason, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
}

void check_djob_rec_helper(djob_id_t djob_id,
    int expected_num_members, int expected_num_pending,
    int expected_num_inprogress, int expected_num_cancelled,
    int expected_num_succeeded, djob_op_job_task_state_value expected_state)
{
	struct isi_error *error = NULL;
	daemon_job_record * djob_rec = NULL;
	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_rec == NULL);
	fail_if (djob_rec->get_stats()->get_num_members() != expected_num_members,
	    "num_members mismatch (exp: %d act: %d)", expected_num_members,
	    djob_rec->get_stats()->get_num_members());
	fail_if (djob_rec->get_stats()->get_num_pending() != expected_num_pending,
	    "num_pending mismatch (exp: %d act: %d)", expected_num_pending,
	    djob_rec->get_stats()->get_num_pending());
	fail_if (djob_rec->get_stats()->get_num_inprogress() != expected_num_inprogress,
	    "num_inprogress mismatch (exp: %d act: %d)", expected_num_inprogress,
	    djob_rec->get_stats()->get_num_inprogress());
	fail_if (djob_rec->get_stats()->get_num_cancelled() != expected_num_cancelled,
	    "num_cancelled mismatch (exp: %d act: %d)", expected_num_cancelled,
	    djob_rec->get_stats()->get_num_cancelled());
	fail_if (djob_rec->get_stats()->get_num_succeeded() != expected_num_succeeded,
	    "num_succeeded mismatch (exp: %d act: %d)", expected_num_succeeded,
	    djob_rec->get_stats()->get_num_succeeded());
	fail_if (djob_rec->get_state() != expected_state,
	    "djob state mismatch (exp %{} act %{})",
	    djob_op_job_task_state_fmt(expected_state),
	    djob_op_job_task_state_fmt(djob_rec->get_state()));
	delete djob_rec;
}

// 64 bytes mem leak caused by THROTTLED_ILOG
TEST(test_archive_task_v1_commit_rename, mem_check : CK_MEM_DEFAULT, mem_hint : THROTTLED_ILOG_BYTES_LEAKED)
{
	check_archive_task_upgrade_helper(false);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task *started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v1_commit_rename_file.txt",
	    20, "test_archive_task_v1_commit_rename_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name(TEST_POLICY);

	/* commit */
	check_archive_task_upgrade_helper(true);

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* rename policy */
	check_cpool_rename_policy_helper(TEST_POLICY, TEST_POLICY_RENAME, true);

	/* process task */
	task_process_completion_reason reason = check_process_task_helper(CPOOL_ARCHIVE_TASK_V1,
	    CPOOL_TASK_CANCELLED, stm, started_task, &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled + 1, num_succeeded, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

// 64 bytes mem leak caused by THROTTLED_ILOG
TEST(test_archive_task_v1_rename, mem_check : CK_MEM_DEFAULT, mem_hint : THROTTLED_ILOG_BYTES_LEAKED)
{
	check_archive_task_upgrade_helper(false);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task *started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v1_rename_file.txt",
	    20, "test_archive_task_v1_rename_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name(TEST_POLICY);

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	delete djob_rec;
	djob_rec = NULL;

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1,
	    num_inprogress + 1, num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	delete djob_rec;
	djob_rec = NULL;
	/* rename policy */
	check_cpool_rename_policy_helper(TEST_POLICY, TEST_POLICY_RENAME, true);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V1, CPOOL_TASK_CANCELLED, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1,
	    num_inprogress, num_cancelled +1 , num_succeeded, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

// 64 bytes mem leak caused by THROTTLED_ILOG
TEST(test_start_archive_task_v1_policy_deleted, mem_check : CK_MEM_DEFAULT, mem_hint : THROTTLED_ILOG_BYTES_LEAKED)
{
	check_archive_task_upgrade_helper(false);
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task *started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v1_policy_deleted_file.txt",
	    20, "test_archive_task_v1_policy_deleted_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name("A_POLICY_DOESNOT_EXIST");

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V1, CPOOL_TASK_CANCELLED, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled + 1, num_succeeded, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

TEST(test_archive_task_v1, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	check_archive_task_upgrade_helper(false);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task * started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v1_file.txt",
	    20, "test_archive_task_v1_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name(TEST_POLICY);

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V1, CPOOL_TASK_SUCCESS, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled, num_succeeded + 1, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}


TEST(test_archive_task_v2, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	check_archive_task_upgrade_helper(true);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task * started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v2_file.txt",
	    20, "test_archive_task_v2_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V2,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V2, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);

	/* get policy id and set task.policy_id */
	struct smartpools_context * sp_ctx = NULL;
	smartpools_open_context(&sp_ctx, &error);
	fail_if (error != NULL, "smartpools_open_context unexpected error: %#{}",
	    isi_error_fmt(error));

	struct fp_policy *policy = NULL;
	smartpools_get_policy_by_name(TEST_POLICY, &policy, sp_ctx, &error);
	fail_if (error != NULL, "smartpools_get_policy_by_name unexpected error: %#{}",
	    isi_error_fmt(error));

	uint32_t policy_id = policy->id;
	arc_task.set_policy_id(policy_id);

	smartpools_close_context(&sp_ctx, &error);
	fail_if (error != NULL, "smartpools_close_context unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V2, CPOOL_TASK_SUCCESS, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled, num_succeeded + 1, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

TEST(test_archive_task_v2_rename, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	check_archive_task_upgrade_helper(true);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task * started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v2_rename_file.txt",
	    20, "test_archive_task_v2_rename_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V2,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V2, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);

	/* get policy id and set task.policy_id */
	struct smartpools_context * sp_ctx = NULL;
	smartpools_open_context(&sp_ctx, &error);
	fail_if (error != NULL, "smartpools_open_context unexpected error: %#{}",
	    isi_error_fmt(error));

	struct fp_policy *policy = NULL;
	smartpools_get_policy_by_name(TEST_POLICY, &policy, sp_ctx, &error);
	fail_if (error != NULL, "smartpools_get_policy_by_name unexpected error: %#{}",
	    isi_error_fmt(error));

	uint32_t policy_id = policy->id;
	arc_task.set_policy_id(policy_id);

	smartpools_close_context(&sp_ctx, &error);
	fail_if (error != NULL, "smartpools_close_context unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* rename policy */
	check_cpool_rename_policy_helper(TEST_POLICY, TEST_POLICY_RENAME, true);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V2, CPOOL_TASK_SUCCESS, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled, num_succeeded + 1, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

TEST(test_archive_task_v1_commit, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	check_archive_task_upgrade_helper(false);
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task * started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_v1_commit_file.txt",
	    20, "test_archive_task_v1_commit_file");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name(TEST_POLICY);

	check_archive_task_upgrade_helper(true);
	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	/* process archive task */
	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V1, CPOOL_TASK_SUCCESS, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled, num_succeeded + 1, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}

// 64 bytes mem leak caused by THROTTLED_ILOG
TEST(test_archive_task_spans_commit, mem_check : CK_MEM_DEFAULT, mem_hint : THROTTLED_ILOG_BYTES_LEAKED)
{
	check_archive_task_upgrade_helper(false);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;
	daemon_job_record *djob_rec = NULL;
	task * started_task = NULL;
	server_task_map *stm = NULL;

	INIT_SERVER_TASK_MAP_HELPER(stm);

	cbm_test_file_helper test_file("/ifs/test_archive_task_spans_commit.txt",
	    20, "test_archive_task_spans_commit");

	archive_task arc_task;
	fail_if (arc_task.get_version() != CPOOL_ARCHIVE_TASK_V1,
	    "task version mismatch (exp %d act %d)",
	    CPOOL_ARCHIVE_TASK_V1, arc_task.get_version());
	arc_task.set_lin(test_file.get_lin());
	arc_task.set_snapid(test_file.get_stat().st_snapid);
	arc_task.set_priority(0);
	arc_task.set_policy_name(TEST_POLICY);

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&arc_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	int num_succeeded = djob_rec->get_stats()->get_num_succeeded();
	int num_cancelled = djob_rec->get_stats()->get_num_cancelled();

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;

	/* start task */
	started_task = stm->start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	fail_if (NULL == started_task, "started_task is NULL");

	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress + 1,
	    num_cancelled, num_succeeded, DJOB_OJT_RUNNING);

	{
		cbm_test_fail_point archive_task_fp("cbm_archive_write_nospace");
		archive_task_fp.set("return(1)");

		check_process_task_helper(CPOOL_ARCHIVE_TASK_V1,
		    CPOOL_TASK_RETRYABLE_ERROR, stm, started_task, &arc_task);

	}

	check_archive_task_upgrade_helper(true);

	task_process_completion_reason reason = check_process_task_helper(
	    CPOOL_ARCHIVE_TASK_V1, CPOOL_TASK_SUCCESS, stm, started_task,
	    &arc_task);

	/* finish task */
	check_finish_task_helper(reason, stm, started_task);

	/* Verify the stats have been updated correctly. */
	check_djob_rec_helper(djob_id, num_members, num_pending - 1, num_inprogress,
	    num_cancelled, num_succeeded + 1, DJOB_OJT_COMPLETED);

	delete djob_rec;
	delete started_task;
	delete stm;
}