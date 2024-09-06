#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>

#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_member.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/unit_test_helpers.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_job/jobstatus_gcfg.h>
#include <isi_sbtree/sbtree.h>
#include <isi_ufp/isi_ufp.h>

#include "client_task_map.h"

using namespace isi_cloud;

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_client_task_map,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	isi_cloud_object_id object_id;
	cpool_fd_store::getInstance();
	fail_unless (UFAIL_POINT_INIT("isi_check") == 0);

	enable_cpool_d(false);
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	enable_cpool_d(true);
}

/**
 * Deletes SBTs for all task types up to priority DEL_PRIORITY_MAX and for all
 * daemon jobs (and their member SBTs) up to job id DEL_JOBS_MAX.
 */
#define DEL_PRIORITY_MAX 50
#define DEL_JOBS_MAX 50
TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	char *name_buf = NULL;
	const cpool_operation_type *op_type = NULL;

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < DEL_PRIORITY_MAX; ++pri) {
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

	for (djob_id_t job_id = 0; job_id < DEL_JOBS_MAX; ++job_id) {
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
}

TEST_FIXTURE(teardown_test)
{
	cpool_fd_store::getInstance().close_sbt_fds();
}

/*
 * Verify that the daemon job record for the specified daemon job ID is
 * equivalent to the expected record.  Optionally verify the creation, last
 * modification, and completion times as they can be difficult to predict.
 */
static void
check_daemon_job_record(djob_id_t djob_id, const daemon_job_record *ex,
    bool check_times)
{
	struct isi_error *error = NULL;

	daemon_job_record *retrieved =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "error getting record for daemon job %d: %#{}",
	    djob_id, isi_error_fmt(error));

	fail_if (retrieved->get_version() != ex->get_version(),
	    "version mismatch (exp: %d act: %d)",
	    ex->get_version(), retrieved->get_version());
	fail_if (retrieved->get_daemon_job_id() != ex->get_daemon_job_id(),
	    "daemon job ID mismatch (exp: %d act: %d)",
	    ex->get_daemon_job_id(), retrieved->get_daemon_job_id());
	fail_if (retrieved->get_operation_type() != ex->get_operation_type(),
	    "operation type mismatch (exp: %{} act: %{})",
	    ex->get_operation_type(), retrieved->get_operation_type());

	if (check_times) {
		fail_if (retrieved->get_creation_time() !=
		    ex->get_creation_time(),
		    "creation time mismatch (exp: %lu act: %lu)",
		    ex->get_creation_time(), retrieved->get_creation_time());
		fail_if (retrieved->get_latest_modification_time() !=
		    ex->get_latest_modification_time(),
		    "latest modification time mismatch (exp: %lu, act: %lu)",
		    ex->get_latest_modification_time(),
		    retrieved->get_latest_modification_time());
		fail_if (retrieved->get_completion_time() !=
		    ex->get_completion_time(),
		    "completion time mismatch (exp: %lu act: %lu)",
		    ex->get_completion_time(),
		    retrieved->get_completion_time());
	}

	fail_if (retrieved->get_state() != ex->get_state(),
	    "state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(ex->get_state()),
	    djob_op_job_task_state_fmt(retrieved->get_state()));
	fail_if (retrieved->get_job_engine_job_id() !=
	    ex->get_job_engine_job_id(),
	    "job engine job ID mismatch (exp: %u act: %u)",
	    ex->get_job_engine_job_id(), retrieved->get_job_engine_job_id());
	fail_if (retrieved->get_job_engine_state() !=
	    ex->get_job_engine_state(),
	    "job engine state mismatch (exp: %{} act: %{})",
	    enum_jobstat_state_fmt(ex->get_job_engine_state()),
	    enum_jobstat_state_fmt(retrieved->get_job_engine_state()));

	const daemon_job_stats *ret_stats = retrieved->get_stats();
	const daemon_job_stats *ex_stats = ex->get_stats();
	fail_if (*ret_stats != *ex_stats, "stats mismatch (exp: %{} act: %{})",
	    cpool_djob_stats_fmt(ex_stats), cpool_djob_stats_fmt(ret_stats));

	const char *ret_desc = retrieved->get_description();
	const char *ex_desc = ex->get_description();
	if (ret_desc == NULL) {
		fail_if (ex_desc != NULL,
		    "description mismatch (exp: \"%s\" act: NULL)", ex_desc);
	}
	else {
		fail_if (ex_desc == NULL,
		    "description mismatch (exp: NULL act: \"%s\")", ret_desc);
		fail_if (strcmp(ret_desc, ex_desc) != 0,
		    "description mismatch (exp: \"%s\" act: \"%s\")",
		    ex_desc, ret_desc);
	}

	delete retrieved;
}

/*
 * Verify that the daemon job member for the specified daemon job ID and task
 * is equivalent to the expected member.
 */
static void
check_daemon_job_member(djob_id_t djob_id, const isi_cloud::task *task,
    const daemon_job_member *ex)
{
	struct isi_error *error = NULL;

	daemon_job_member *retrieved =
	    daemon_job_manager::get_daemon_job_member(djob_id, task, &error);
	fail_if (error != NULL,
	    "failed to retrieve daemon job member (job: %d task: %{}): %#{}",
	    djob_id, cpool_task_fmt(task), isi_error_fmt(error));
	fail_if (retrieved == NULL);

	fail_if (retrieved->get_version() != ex->get_version(),
	    "version mismatch (exp: %d act: %d)",
	    ex->get_version(), retrieved->get_version());

	if (retrieved->get_name() == NULL) {
		fail_if (ex->get_name() != NULL,
		    "name mismatch (exp: \"%s\" act: NULL)", ex->get_name());
	}
	else {
		fail_if (ex->get_name() == NULL,
		    "name mismatch (exp: NULL act: \"%s\")",
		    retrieved->get_name());
		fail_if (strcmp(retrieved->get_name(), ex->get_name()) != 0,
		    "name mismatch (exp: \"%s\" act: \"%s\")",
		    ex->get_name(), retrieved->get_name());
	}

	delete retrieved;
}

class client_task_map_ut : public client_task_map
{
public:
	client_task_map_ut(const cpool_operation_type *op_type)
		: client_task_map(op_type) { }

	isi_cloud::task *
	find_task(const task_key *key, bool ignore_ENOENT,
	    struct isi_error **error_out) const
	{
		return task_map::find_task(key, ignore_ENOENT, error_out);
	}
};

/*
 * Add a task to a multi-job where it is not yet a member.
 */
TEST(test_add_task__multi_job__nonexistent_member)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task_map.
	 */
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    1, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(12345);
	archive_task->set_snapid(54321);
	archive_task->set_priority(1);
	archive_task->set_policy_id(11111);

	client_task_map tm(OT_ARCHIVE);
	tm.add_task(archive_task, djob_id, "theFilename", &error);
	fail_if (error != NULL, "failed to add %{} task: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* There should now be an entry in the pending SBT ... */
	int pending_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(archive_task->get_op_type()->get_type(),
	    archive_task->get_priority(), false, &error);
	fail_if (error != NULL,
	    "error getting pending SBT fd for %{} priority %d: %#{}",
	    task_type_fmt(archive_task->get_op_type()->get_type()),
	    archive_task->get_priority(), isi_error_fmt(error));

	const void *key;
	size_t key_length;
	task->get_key()->get_serialized_key(key, key_length);

	fail_if (key_length != sizeof(btree_key_t),
	    "key_length mismatch (exp: %zu act: %zu)", sizeof(btree_key_t),
	    key_length);
	const btree_key_t *bt_key = static_cast<const btree_key_t *>(key);
	fail_if (bt_key == NULL);

	char entry_buf[8192];
	size_t entry_size_out;
	fail_if (ifs_sbt_get_entry_at(pending_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), entry_buf, sizeof entry_buf,
	    NULL, &entry_size_out) != 0,
	    "failed to read entry for task (%{}) from pending sbt fd: %s",
	    cpool_task_fmt(archive_task), strerror(errno));

	isi_cloud::archive_task retrieved_task;
	retrieved_task.set_serialized_task(entry_buf, entry_size_out, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (retrieved_task.get_lin() != archive_task->get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(archive_task->get_lin()),
	    lin_fmt(retrieved_task.get_lin()));
	fail_if (retrieved_task.get_snapid() != archive_task->get_snapid(),
	    "snapid mismatch (exp: %{} act: %lu)",
	    snapid_fmt(archive_task->get_snapid()),
	    snapid_fmt(retrieved_task.get_snapid()));
	fail_if (retrieved_task.get_priority() != archive_task->get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    archive_task->get_priority(), retrieved_task.get_priority());
	fail_if (retrieved_task.get_policy_id() != archive_task->get_policy_id(),
	    "policy id mismatch (exp: %d act: %d)",
	    archive_task->get_policy_id(),
	    retrieved_task.get_policy_id());

	const std::set<djob_id_t> job_ids = retrieved_task.get_daemon_jobs();
	fail_if (job_ids.size() != 1,
	    "daemon job number mismatch (exp: 1 act: %zu)", job_ids.size());

	const std::set<djob_id_t>::const_iterator job_ids_iter =
	    job_ids.begin();
	fail_if (*job_ids_iter != djob_id,
	    "daemon job ID mismatch (exp: %d act: %d)", djob_id,
	    *job_ids_iter);

	/* ... the daemon jobs SBT ... */
	daemon_job_record ex_record;
	ex_record.set_daemon_job_id(djob_id);
	ex_record.set_operation_type(op_type);
	ex_record.set_state(DJOB_OJT_RUNNING);
	/*
	 * Since set_job_engine_job_id and set_job_engine_state assert if given
	 * NULL_JID or STATE_NULL respectively, rely on the fact that the id
	 * and state are initialized to those values by the daemon_job_record
	 * constructor.
	 */
	ex_record.increment_num_members();
	ex_record.increment_num_pending();
	check_daemon_job_record(djob_id, &ex_record, false);

	/* ... and the (now-created) member SBT. */
	daemon_job_member ex_member(task->get_key());
	ex_member.set_name("theFilename");
	check_daemon_job_member(djob_id, task, &ex_member);

	delete task;

	/* test of query_job_for_members */
	resume_key start_key;
	std::vector<struct job_member *> job_members;
	std::vector<struct job_member *>::iterator job_member_it;

	const unsigned int START_INDEX = 0;
	const unsigned int MAX_FILES = 100000;

	tm.query_job_for_members(djob_id, START_INDEX, start_key, MAX_FILES,
	    job_members, &error);
	fail_if(error != NULL, "Unexpected error querying job_members: %#{}",
	    isi_error_fmt(error));
	fail_if(job_members.size() != 1,
	    "Incorrect number of job_members.  Expected: 1, Got: %d",
	    job_members.size());
	fail_if(!start_key.empty(),
	    "Start key must be empty after reading all entries");

	/* Clean up after query_job_for_members */
	for (job_member_it = job_members.begin();
	    job_member_it != job_members.end(); ++job_member_it) {

		struct job_member *member = *job_member_it;

		free(member->key_);
		free(member->name_);

		delete member;
	}

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a multi-job where it is already a member.
 */
TEST(test_add_task__multi_job__existing_member)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task_map.
	 */
	cpool_fd_store::getInstance().get_pending_sbt_fd(op_type->get_type(),
	    1, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(12345);
	archive_task->set_snapid(54321);
	archive_task->set_priority(1);
	archive_task->set_policy_id(11111);

	client_task_map tm(OT_ARCHIVE);
	tm.add_task(archive_task, djob_id, "theFilename", &error);
	fail_if (error != NULL, "failed to add %{} task: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add the task to the same daemon job again. */
	tm.add_task(archive_task, djob_id, "theFilename", &error);
	fail_if (error == NULL, "expected EEXIST");
	fail_if (!isi_system_error_is_a(error, EEXIST),
	    "expected EEXIST: %#{}", isi_error_fmt(error));
	isi_error_free(error);

	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a daemon job, then add the same task to a different daemon
 * job.
 */
TEST(test_add_task__multi_job__existing_member_different_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task map.
	 */
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
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create the task that will be added to two different daemon jobs. */
	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::recall_task *recall_task =
	    static_cast<isi_cloud::recall_task *>(task);
	fail_if (recall_task == NULL);

	recall_task->set_lin(2468);
	recall_task->set_snapid(1357);
	recall_task->set_retain_stub(false);
	recall_task->set_priority(1);

	/* Create the two daemon jobs to which this task will be added. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/*
	 * Add the task to each daemon job, with different filenames just for
	 * fun.
	 */
	client_task_map tm(op_type);
	tm.add_task(task, djob_id_1, "filename1", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_1, isi_error_fmt(error));

	tm.add_task(task, djob_id_2, "filename2", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_2, isi_error_fmt(error));

	/*
	 * Retrieve the added task from the pending SBT and verify its
	 * contents.
	 */
	int pending_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 1, false, &error);
	fail_if (error != NULL,
	    "failed to get %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	const void *key;
	size_t key_length;
	task->get_key()->get_serialized_key(key, key_length);

	fail_if (key_length != sizeof(btree_key_t),
	    "key_length mismatch (exp: %zu act: %zu)", sizeof(btree_key_t),
	    key_length);
	const btree_key_t *bt_key = static_cast<const btree_key_t *>(key);
	fail_if (bt_key == NULL);

	char entry_buf[8192];
	size_t entry_size_out;
	fail_if (ifs_sbt_get_entry_at(pending_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), entry_buf, sizeof entry_buf,
	    NULL, &entry_size_out) != 0,
	    "failed to read entry for task (%{}) from pending sbt fd: %s",
	    cpool_task_fmt(task), strerror(errno));

	isi_cloud::recall_task retrieved_task;
	retrieved_task.set_serialized_task(entry_buf, entry_size_out, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (retrieved_task.get_lin() != recall_task->get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(recall_task->get_lin()),
	    lin_fmt(retrieved_task.get_lin()));
	fail_if (retrieved_task.get_snapid() != recall_task->get_snapid(),
	    "snapid mismatch (exp: %{} act: %lu)",
	    snapid_fmt(recall_task->get_snapid()),
	    snapid_fmt(retrieved_task.get_snapid()));
	fail_if (retrieved_task.get_priority() != recall_task->get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    recall_task->get_priority(), retrieved_task.get_priority());

	const std::set<djob_id_t> &daemon_jobs =
	    retrieved_task.get_daemon_jobs();
	fail_if (daemon_jobs.size() != 2,
	    "daemon job number mismatch (exp: 2 act: %d)", daemon_jobs.size());
	fail_if (daemon_jobs.find(djob_id_1) == daemon_jobs.end(),
	    "did not find %d in daemon job set", djob_id_1);
	fail_if (daemon_jobs.find(djob_id_2) == daemon_jobs.end(),
	    "did not find %d in daemon job set", djob_id_2);

	/*
	 * Verify the two daemon job records.  Ignore the times as they are
	 * difficult to predict.  Since set_job_engine_job_id and
	 * set_job_engine_state assert if given NULL_JID or STATE_NULL
	 * respectively, rely on the fact that the id and state are initialized
	 * to those values by the daemon_job_record constructor.
	 */
	daemon_job_record ex_record_djob_1;
	ex_record_djob_1.set_daemon_job_id(djob_id_1);
	ex_record_djob_1.set_operation_type(op_type);
	ex_record_djob_1.set_state(DJOB_OJT_RUNNING);
	ex_record_djob_1.increment_num_members();
	ex_record_djob_1.increment_num_pending();
	check_daemon_job_record(djob_id_1, &ex_record_djob_1, false);

	daemon_job_record ex_record_djob_2;
	ex_record_djob_2.set_daemon_job_id(djob_id_2);
	ex_record_djob_2.set_operation_type(op_type);
	ex_record_djob_2.set_state(DJOB_OJT_RUNNING);
	ex_record_djob_2.increment_num_members();
	ex_record_djob_2.increment_num_pending();
	check_daemon_job_record(djob_id_2, &ex_record_djob_2, false);

	/* Verify the daemon job members. */
	daemon_job_member djob_member_1(task->get_key());
	djob_member_1.set_name("filename1");
	check_daemon_job_member(djob_id_1, &retrieved_task, &djob_member_1);

	daemon_job_member djob_member_2(task->get_key());
	djob_member_2.set_name("filename2");
	check_daemon_job_member(djob_id_2, &retrieved_task, &djob_member_2);

	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a daemon job, then add the same task with a higher priority to
 * a different daemon job; the task should be "promoted" to the higher
 * priority.
 */
TEST(test_add_task__multi_job__existing_member_higher_pri)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task map.
	 */
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
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create the task that will be added to two different daemon jobs. */
	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(345);
	archive_task->set_snapid(784923);
	archive_task->set_policy_id(11111);

	/*
	 * First add this task at a lower priority, then later will add it
	 * again with the higher priority.
	 */
	archive_task->set_priority(0);

	/* Create the two daemon jobs to which this task will be added. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add the task to the first daemon job. */
	client_task_map_ut tm(op_type);
	tm.add_task(task, djob_id_1, "filename1", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_1, isi_error_fmt(error));

	/* Increase the priority and add to the second daemon job. */
	archive_task->set_priority(1);

	tm.add_task(task, djob_id_2, "filename2", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_2, isi_error_fmt(error));

	/* Find and verify the task. */
	isi_cloud::task *found_task = tm.find_task(archive_task->get_key(),
	    false, &error);
	fail_if (error != NULL, "failed to find task: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);

	isi_cloud::archive_task *found_archive_task =
	    static_cast<isi_cloud::archive_task *>(found_task);
	fail_if (found_archive_task == NULL);

	fail_if (found_archive_task->get_priority() != 1,
	    "priority mismatch (exp: 1 act: %d)",
	    found_archive_task->get_priority());

	delete found_task;
	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a daemon job, then add the same task with a lower priority to
 * a different daemon job.  The task should stay at the higher priority.
 */
TEST(test_add_task__multi_job__existing_member_lower_pri)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task map.
	 */
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
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create the task that will be added to two different daemon jobs. */
	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(345);
	archive_task->set_snapid(784923);
	archive_task->set_policy_id(11111);

	/*
	 * First add this task at a higher priority, then later will add it
	 * again with the lower priority.
	 */
	archive_task->set_priority(1);

	/* Create the two daemon jobs to which this task will be added. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add the task to the first daemon job. */
	client_task_map_ut tm(op_type);
	tm.add_task(task, djob_id_1, "filename1", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_1, isi_error_fmt(error));

	/* Decrease the priority and add to the second daemon job. */
	archive_task->set_priority(0);

	tm.add_task(task, djob_id_2, "filename2", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_2, isi_error_fmt(error));

	/* Find and verify the task. */
	isi_cloud::task *found_task = tm.find_task(archive_task->get_key(),
	    false, &error);
	fail_if (error != NULL, "failed to find task: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);

	isi_cloud::archive_task *found_archive_task =
	    static_cast<isi_cloud::archive_task *>(found_task);
	fail_if (found_archive_task == NULL);

	fail_if (found_archive_task->get_priority() != 1,
	    "priority mismatch (exp: 1 act: %d)",
	    found_archive_task->get_priority());

	delete found_task;
	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a daemon job, then add the same task with a higher priority to
 * a different daemon job; the task should be "promoted" to the higher
 * priority.
 */
TEST(test_add_task__multi_job__inprogress_member_higher_pri)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task map.
	 */
	int pending_0_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 0, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 0: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 1, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	int inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create the task that will be added to two different daemon jobs. */
	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(345);
	archive_task->set_snapid(784923);
	archive_task->set_policy_id(11111);

	/*
	 * First add this task at a lower priority, then later will add it
	 * again with the higher priority.
	 */
	archive_task->set_priority(0);

	/* Create the two daemon jobs to which this task will be added. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add the task to the first daemon job. */
	client_task_map_ut tm(op_type);
	tm.add_task(task, djob_id_1, "filename1", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_1, isi_error_fmt(error));

	/*
	 * Move the task from the pending SBT to the in-progress SBT.
	 * (Normally this would happen when the task is started.)
	 */
	const void *key;
	size_t key_size;
	archive_task->get_key()->get_serialized_key(key, key_size);

	fail_if (key_size != sizeof(btree_key_t),
	    "key size mismatch (exp: %zu act: %zu)",
	    sizeof(btree_key_t), key_size);
	const btree_key_t *bt_key = static_cast<const btree_key_t *>(key);
	fail_if (bt_key == NULL);

	/* Assumes SBT entry size can't exceed 8K. */
	char entry_buf[8192];
	size_t entry_size_out;
	fail_if (ifs_sbt_get_entry_at(pending_0_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), entry_buf, sizeof entry_buf,
	    NULL, &entry_size_out) != 0,
	    "failed to read entry from pending SBT "
	    "in preparation for moving to in-progress SBT: %s",
	    strerror(errno));

	isi_cloud::archive_task temp_task;
	temp_task.set_serialized_task(entry_buf, entry_size_out, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	temp_task.set_state(DJOB_OJT_RUNNING);
	temp_task.set_location(TASK_LOC_INPROGRESS);

	const void *serialized_temp_task;
	size_t serialized_temp_task_size;
	temp_task.get_serialized_task(serialized_temp_task,
	    serialized_temp_task_size);

	/*
	 * Add the task to the inprogress SBT and remove it from the pending
	 * SBT.
	 */
	fail_if (ifs_sbt_add_entry(inprogress_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), serialized_temp_task,
	    serialized_temp_task_size, NULL) != 0,
	    "failed to add task to in-progress SBT: %s", strerror(errno));

	fail_if (ifs_sbt_cond_remove_entry(pending_0_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), BT_CM_NONE, NULL, 0, NULL) != 0,
	    "failed to remove task from pending SBT: %s", strerror(errno));

	/* Increase the priority and add to the second daemon job. */
	archive_task->set_priority(1);

	tm.add_task(task, djob_id_2, "filename2", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_2, isi_error_fmt(error));

	/* Find and verify the task. */
	isi_cloud::task *found_task = tm.find_task(archive_task->get_key(),
	    false, &error);
	fail_if (error != NULL, "failed to find task: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);

	isi_cloud::archive_task *found_archive_task =
	    static_cast<isi_cloud::archive_task *>(found_task);
	fail_if (found_archive_task == NULL);

	fail_if (found_archive_task->get_state() != DJOB_OJT_RUNNING,
	    "state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(found_archive_task->get_state()));
	fail_if (found_archive_task->get_priority() != 1,
	    "priority mismatch (exp: 1 act: %d)",
	    found_archive_task->get_priority());

	delete found_task;
	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to a daemon job where the task is already being processed and the
 * in-progress task has a higher priority than the added task.  The task should
 * remain in the in-progress SBT and have the original (higher) priority.
 */
TEST(test_add_task__multi_job__inprogress_member_lower_pri)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;

	fail_if (!op_type->is_multi_job(), "%{} is not a multi-job",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the pending and in-progress SBTs that would have been created
	 * by the server task map.
	 */
	cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 0, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 0: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	int pending_1_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 1, true, &error);
	fail_if (error != NULL,
	    "failed to create %{} pending SBT priority 1: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	int inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create the task that will be added to two different daemon jobs. */
	isi_cloud::task *task =
	    isi_cloud::task::create_task(op_type);
	fail_if (task == NULL);
	isi_cloud::archive_task *archive_task =
	    static_cast<isi_cloud::archive_task *>(task);
	fail_if (archive_task == NULL);

	archive_task->set_lin(345);
	archive_task->set_snapid(784923);
	archive_task->set_policy_id(11111);

	/*
	 * First add this task at a higher priority, then later will add it
	 * again with the lower priority.
	 */
	archive_task->set_priority(1);

	/* Create the two daemon jobs to which this task will be added. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Add the task to the first daemon job. */
	client_task_map_ut tm(op_type);
	tm.add_task(task, djob_id_1, "filename1", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_1, isi_error_fmt(error));

	/*
	 * Move the task from the pending SBT to the in-progress SBT.
	 * (Normally this would happen when the task is started.)
	 */
	const void *key;
	size_t key_size;
	archive_task->get_key()->get_serialized_key(key, key_size);

	fail_if (key_size != sizeof(btree_key_t),
	    "key size mismatch (exp: %zu act: %zu)",
	    sizeof(btree_key_t), key_size);
	const btree_key_t *bt_key = static_cast<const btree_key_t *>(key);
	fail_if (bt_key == NULL);

	/* Assumes SBT entry size can't exceed 8K. */
	char entry_buf[8192];
	size_t entry_size_out;
	fail_if (ifs_sbt_get_entry_at(pending_1_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), entry_buf, sizeof entry_buf,
	    NULL, &entry_size_out) != 0,
	    "failed to read entry from pending SBT "
	    "in preparation for moving to in-progress SBT: %s",
	    strerror(errno));

	isi_cloud::archive_task temp_task;
	temp_task.set_serialized_task(entry_buf, entry_size_out, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	temp_task.set_state(DJOB_OJT_RUNNING);
	temp_task.set_location(TASK_LOC_INPROGRESS);

	const void *serialized_temp_task;
	size_t serialized_temp_task_size;
	temp_task.get_serialized_task(serialized_temp_task,
	    serialized_temp_task_size);

	/*
	 * Add the task to the inprogress SBT and remove it from the pending
	 * SBT.
	 */
	fail_if (ifs_sbt_add_entry(inprogress_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), serialized_temp_task,
	    serialized_temp_task_size, NULL) != 0,
	    "failed to add task to in-progress SBT: %s", strerror(errno));

	fail_if (ifs_sbt_cond_remove_entry(pending_1_sbt_fd,
	    const_cast<btree_key_t *>(bt_key), BT_CM_NONE, NULL, 0, NULL) != 0,
	    "failed to remove task from pending SBT: %s", strerror(errno));

	/* Decrease the priority and add to the second daemon job. */
	archive_task->set_priority(0);

	tm.add_task(task, djob_id_2, "filename2", &error);
	fail_if (error != NULL, "failed to add task to daemon job %d: %#{}",
	    djob_id_2, isi_error_fmt(error));

	/* Find and verify the task. */
	isi_cloud::task *found_task = tm.find_task(archive_task->get_key(),
	    false, &error);
	fail_if (error != NULL, "failed to find task: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);

	isi_cloud::archive_task *found_archive_task =
	    static_cast<isi_cloud::archive_task *>(found_task);
	fail_if (found_archive_task == NULL);

	fail_if (found_archive_task->get_state() != DJOB_OJT_RUNNING,
	    "state mismatch (exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(found_archive_task->get_state()));
	fail_if (found_archive_task->get_priority() != 1,
	    "priority mismatch (exp: 1 act: %d)",
	    found_archive_task->get_priority());

	delete found_task;
	delete task;

	/* Cleanup the gconfig context to avoid a bogus memory leak message. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to the daemon job of an operation that only supports single jobs
 * where the task does not already exist, and then add it again.
 */
TEST(test_add_task__single_job)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_WRITEBACK;

	fail_if (op_type->is_multi_job(), "%{} is not a single job operation",
	    task_type_fmt(op_type->get_type()));

	daemon_job_manager::at_startup(&error);
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
	fail_if (error != NULL, "failed to create %{} in-progress SBT: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "failed to start %{} job: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));

	/* Create and add the non-existent task. */
	writeback_task wb_task;
	wb_task.set_lin(45678);
	wb_task.set_snapid(123456);
	wb_task.set_priority(0);

	client_task_map_ut ctm(op_type);
	ctm.add_task(&wb_task, djob_id, "/ifs/this/is/the/filename", &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Retrieve and verify the daemon job stats. */
	daemon_job_record *djob_record =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	const daemon_job_stats *djob_stats = djob_record->get_stats();
	fail_if (djob_stats == NULL);

	fail_if (djob_stats->get_num_members() != 1,
	    "num_members mismatch (exp: 1 act: %d)",
	    djob_stats->get_num_members());
	fail_if (djob_stats->get_num_succeeded() != 0,
	    "num_succeeded mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_succeeded());
	fail_if (djob_stats->get_num_failed() != 0,
	    "num_failed mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_failed());
	fail_if (djob_stats->get_num_pending() != 1,
	    "num_pending mismatch (exp: 1 act: %d)",
	    djob_stats->get_num_pending());
	fail_if (djob_stats->get_num_inprogress() != 0,
	    "num_inprogress mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_inprogress());

	delete djob_record;

	/* Add the task again, and verify the stats haven't changed. */
	ctm.add_task(&wb_task, djob_id, "/ifs/this/is/the/filename", &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Retrieve and verify the daemon job stats. */
	djob_record =
	    daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_stats = djob_record->get_stats();
	fail_if (djob_stats == NULL);

	fail_if (djob_stats->get_num_members() != 1,
	    "num_members mismatch (exp: 1 act: %d)",
	    djob_stats->get_num_members());
	fail_if (djob_stats->get_num_succeeded() != 0,
	    "num_succeeded mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_succeeded());
	fail_if (djob_stats->get_num_failed() != 0,
	    "num_failed mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_failed());
	fail_if (djob_stats->get_num_pending() != 1,
	    "num_pending mismatch (exp: 1 act: %d)",
	    djob_stats->get_num_pending());
	fail_if (djob_stats->get_num_inprogress() != 0,
	    "num_inprogress mismatch (exp: 0 act: %d)",
	    djob_stats->get_num_inprogress());

	delete djob_record;
}

/*
 * Find tasks in the pending or in-progress SBTs, and attempt to find tasks not
 * in any SBT.
 */
TEST(test_find_task)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_LOCAL_GC;
	const int num_priorities = 32;
	const unsigned int num_test_tasks = 2;
	int inprogress_sbt_fd;
	int pending_sbt_fds[num_priorities] = { 0 };
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::local_gc_task *retrieved_lgc_task = NULL;

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "get_inprogress_sbt_fd for %{} failed: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (inprogress_sbt_fd == -1);

	for (int i = 0; i < num_priorities; ++i) {
		pending_sbt_fds[i] = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type->get_type(), i, true, &error);
		fail_if (error != NULL,
		    "get_pending_sbt_fd for %{} failed (i: %d): %#{}",
		    task_type_fmt(op_type->get_type()), i,
		    isi_error_fmt(error));
		fail_if (pending_sbt_fds[i] == -1);
	}

	client_task_map_ut ctm(op_type);

	/* Create a task to be added to the in-progress SBT. */
	isi_cloud::local_gc_task inprog_task;

	ifs_lin_t inprog_task__lin = 235689;
	inprog_task.set_lin(inprog_task__lin);

	ifs_snapid_t inprog_task__snapid = 134679;
	inprog_task.set_snapid(inprog_task__snapid);

	isi_cloud_object_id inprog_task__object_id;
	inprog_task.set_object_id(inprog_task__object_id);

	int inprog_task__priority = 30;
	inprog_task.set_priority(inprog_task__priority);

	inprog_task.set_state(DJOB_OJT_RUNNING);
	inprog_task.set_location(TASK_LOC_INPROGRESS);

	const void *inprog_task__serialization;
	size_t inprog_task__serialization_size;
	inprog_task.get_serialized_task(inprog_task__serialization,
	    inprog_task__serialization_size);

	add_sbt_entry_ut(inprogress_sbt_fd, inprog_task.get_key(),
	    inprog_task__serialization, inprog_task__serialization_size);

	/* Create a task to be added to a pending SBT. */
	isi_cloud::local_gc_task pend_task;

	ifs_lin_t pend_task__lin = 13243546;
	pend_task.set_lin(pend_task__lin);

	ifs_snapid_t pend_task__snapid = 143658;
	pend_task.set_snapid(pend_task__snapid);

	isi_cloud_object_id pend_task__object_id;
	pend_task.set_object_id(pend_task__object_id);

	int pend_task__priority = 25;
	pend_task.set_priority(pend_task__priority);

	pend_task.set_state(DJOB_OJT_RUNNING);
	pend_task.set_location(TASK_LOC_PENDING);

	const void *pend_task__serialization;
	size_t pend_task__serialization_size;
	pend_task.get_serialized_task(pend_task__serialization,
	    pend_task__serialization_size);

	add_sbt_entry_ut(pending_sbt_fds[pend_task.get_priority()],
	    pend_task.get_key(), pend_task__serialization,
	    pend_task__serialization_size);

	/* Create a task to not be added to any SBT. */
	isi_cloud::local_gc_task none_task;

	none_task.set_lin(42);
	none_task.set_snapid(1098);
	none_task.set_priority(14);

	const void *none_task__serialization;
	size_t none_task__serialization_size;
	none_task.get_serialized_task(none_task__serialization,
	    none_task__serialization_size);

	/*
	 * Attempt to retrieve each of the four tasks, both while ignoring and
	 * not ignoring ENOENT when we don't expect the task to be found.
	 * First, the one from the pending SBT.
	 */
	retrieved_task = ctm.find_task(pend_task.get_key(), false, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task in pending SBT: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_lgc_task =
	    static_cast<isi_cloud::local_gc_task *>(retrieved_task);
	fail_if(retrieved_lgc_task == NULL);

	fail_if(retrieved_lgc_task->get_lin() != pend_task__lin,
	    "lin mismatch (exp: %{} act: %{})", lin_fmt(pend_task__lin),
	    lin_fmt(retrieved_lgc_task->get_lin()));
	fail_if(retrieved_lgc_task->get_snapid() != pend_task__snapid,
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(pend_task__snapid),
	    snapid_fmt(retrieved_lgc_task->get_snapid()));
	fail_if(retrieved_lgc_task->get_object_id() != pend_task__object_id,
	    "object ID mismatch (exp: %s act: %s)",
	    pend_task__object_id.to_c_string(),
	    retrieved_lgc_task->get_object_id().to_c_string());
	fail_if(retrieved_lgc_task->get_priority() != pend_task__priority,
	    "priority mismatch (exp: %d act: %d)", pend_task__priority,
	    retrieved_lgc_task->get_priority());

	delete retrieved_lgc_task;

	/*
	 * Next, the task that was not added to any SBT, both ignoring and not
	 * ignoring ENOENT.
	 */
	retrieved_task = ctm.find_task(none_task.get_key(), false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	retrieved_task = ctm.find_task(none_task.get_key(), true, &error);
	fail_if(error != NULL, "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	/* Finally, the task that was added to the in-progress SBT. */
	retrieved_task = ctm.find_task(inprog_task.get_key(), false, &error);
	fail_if(error != NULL, "unexpected error "
	    "when searching for task in in-progress SBT: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_lgc_task =
	    static_cast<isi_cloud::local_gc_task *>(retrieved_task);
	fail_if(retrieved_lgc_task == NULL);

	fail_if(retrieved_lgc_task->get_lin() != inprog_task__lin,
	    "lin mismatch (exp: %{} act: %{})", lin_fmt(inprog_task__lin),
	    lin_fmt(retrieved_lgc_task->get_lin()));
	fail_if(retrieved_lgc_task->get_snapid() != inprog_task__snapid,
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(inprog_task__snapid),
	    snapid_fmt(retrieved_lgc_task->get_snapid()));
	fail_if(retrieved_lgc_task->get_object_id() != inprog_task__object_id,
	    "object ID mismatch (exp: %s act: %s)",
	    inprog_task__object_id.to_c_string(),
	    retrieved_lgc_task->get_object_id().to_c_string());
	fail_if(retrieved_lgc_task->get_priority() != inprog_task__priority,
	    "priority mismatch (exp: %d act: %d)", inprog_task__priority,
	    retrieved_lgc_task->get_priority());

	delete retrieved_lgc_task;

	/*
	 * Verify that we can identify that there are 4 incomplete tasks in the
	 * map
	 */
	unsigned int n_incomplete_tasks = ctm.get_incomplete_task_count(&error);
	fail_if(error != NULL, "Error getting incomplete task count : %#{}",
	    isi_error_fmt(error));
	fail_if(n_incomplete_tasks != num_test_tasks,
	    "Got incorrect number of tasks back.  Expected %d, got %d",
	    num_test_tasks, n_incomplete_tasks);

	close_sbt(inprogress_sbt_fd);
	for (int i = 0; i < num_priorities; ++i) {
		close_sbt(pending_sbt_fds[i]);
	}
}
