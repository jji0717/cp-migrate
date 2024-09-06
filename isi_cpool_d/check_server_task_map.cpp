#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_d_api/client_task_map.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_common/ifs_cpool_flock.h>
#include <isi_cpool_d_common/unit_test_helpers.h>
#include <isi_cpool_d_common/task.h>
#include <isi_ufp/isi_ufp.h>

#include "server_task_map.h"

using namespace isi_cloud;

extern int task_randomization_factor;

// not expect any memory leak
#define EXPECTED_LEAKS   0

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_server_task_map,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

static void *do_nothing(void *unused)
{
	pthread_exit(NULL);
}

TEST_FIXTURE(setup_suite)
{
	// We need predictability for unit tests.  Dial down task randomization
	task_randomization_factor = 1;

	// Pre-initialize node ID to avoid false-positive leaks
	get_handling_node_id();
	cpool_fd_store::getInstance();

	struct isi_error *error = NULL;
	get_devid(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");

	pthread_t do_nothing_thread;
	fail_unless (pthread_create(&do_nothing_thread, NULL, do_nothing,
	    NULL) == 0, "failed to create do_nothing thread: %s",
	    strerror(errno));
	fail_unless (pthread_join(do_nothing_thread, NULL) == 0,
	    "failed to join do_nothing thread: %s", strerror(errno));
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
}

/**
 * Deletes SBTs for all task types up to priority DEL_PRIORITY_MAX.
 */
#define DEL_JOBS_MAX 50
#define DEL_PRIORITY_MAX 50
TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	char *name_buf = NULL;
	const cpool_operation_type *op_type = NULL;

	for (djob_id_t job_id = 0; job_id < DEL_JOBS_MAX; ++job_id) {
		name_buf = get_djob_file_list_sbt_name(job_id);
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete SBT (%s): %#{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}

	name_buf = get_daemon_jobs_sbt_name();
	delete_sbt(name_buf, &error);
	fail_if (error != NULL, "failed to delete SBT (%s): %#{}",
	    name_buf, isi_error_fmt(error));
	free(name_buf);

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
}

TEST_FIXTURE(teardown_test)
{
	cpool_fd_store::getInstance().close_sbt_fds();
}

class server_task_map_ut : public server_task_map
{
public:
	server_task_map_ut(const cpool_operation_type *op_type,
	    int num_priorities)
		: server_task_map(op_type, num_priorities) { }

	~server_task_map_ut() { }

	bool
	is_initialized(void) const
	{
		return initialized_;
	}

	task *
	find_task(const task_key *key, bool ignore_ENOENT,
	    struct isi_error **error_out) const
	{
		return task_map::find_task(key, ignore_ENOENT, error_out);
	}

	bool
	claim_task(const isi_cloud::task *task,
	    struct isi_error **error_out) const
	{
		return task_map::claim_task(task, error_out);
	}

	bool
	relinquish_task(const isi_cloud::task *task,
	    struct isi_error **error_out) const
	{
		return task_map::relinquish_task(task, error_out);
	}

	isi_cloud::task *
	start_task_helper__sbt_ent_to_claimed_locked_task(int sbt_fd,
	    struct sbt_entry *sbt_ent, struct isi_error **error_out) const
	{
		return server_task_map::\
		    start_task_helper__sbt_ent_to_claimed_locked_task(sbt_fd,
		    sbt_ent, error_out);
	}
};

/*
 * Helper functions for some of the unit tests.
 */
static struct {
	const task *task_to_claim_;
	struct isi_error **error_;
	bool success_;
} claim_work_item;

static void *claimer(void *arg)
{
	struct isi_error *error = NULL;
	server_task_map_ut *stm = NULL;

	if (arg == NULL) {
		error = isi_system_error_new(EINVAL,
		    "passed-in arg must be non-NULL");
		goto out;
	}

	stm = static_cast<server_task_map_ut *>(arg);
	if (arg == NULL) {
		error = isi_system_error_new(EINVAL,
		    "failed to cast passed-in arg as server_task_map_ut");
		goto out;
	}

	claim_work_item.success_ =
	    stm->claim_task(claim_work_item.task_to_claim_, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, claim_work_item.error_);

	pthread_exit(NULL);
}

static bool
claim_task_as_someone_else(const server_task_map_ut *stm,
    const task *task_to_claim, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	claim_work_item.task_to_claim_ = task_to_claim;
	claim_work_item.error_ = &error;

	pthread_t cr_thread;
	pthread_attr_t cr_thread_attr;

	/*
	 * Initialize thread creation attributes.  We don't define anything
	 * for these attributes, but this reduces memory leak noise.
	 */
	fail_if (pthread_attr_init(&cr_thread_attr) != 0,
	    "failed to initialize thread attributes: %s", strerror(errno));
	fail_if (pthread_create(&cr_thread, &cr_thread_attr, claimer,
	    &stm) != 0, "failed to create claimer thread: %s",
	    strerror(errno));
	fail_if (pthread_join(cr_thread, NULL) != 0,
	    "failed to join claimer thread: %s", strerror(errno));
	fail_if (pthread_attr_destroy(&cr_thread_attr) != 0,
	    "failed to destroy thread attributes: %s", strerror(errno));

	isi_error_handle(error, error_out);

	return claim_work_item.success_;
}

TEST(test_initialize, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 7;

	server_task_map_ut stm(op_type, num_priorities);
	fail_if (stm.is_initialized());

	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (!stm.is_initialized());

	// verify existence of SBTs here?
}

TEST(test_start_task__empty, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_LOCAL_GC;
	int num_priorities = 3;
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *task = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* false: non-recovery */
	task = stm.start_task(ctx, max_task_reads, false, task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (task != NULL, "task: %{}", cpool_task_fmt(task));
	fail_if (ctx != NULL);

	/* true: recovery */
	task = stm.start_task(ctx, max_task_reads, true, task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (task != NULL, "task: %{}", cpool_task_fmt(task));
	fail_if (ctx != NULL);
}

/*
 * Add a single element to the pending SBT of a server_task_map with one
 * priority and attempt to start it using start_task.
 */
TEST(test_start_task__single_element, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	int num_priorities = 1;
	daemon_job_record *djob_rec = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job to which the task will be added. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a task and add it to the pending SBT. */
	recall_task task_to_start;
	task_to_start.set_priority(0);
	task_to_start.set_location(TASK_LOC_PENDING);
	task_to_start.set_lin(12345);
	task_to_start.set_snapid(54321);
	task_to_start.set_retain_stub(false);

	client_task_map ctm(op_type);
	ctm.add_task(&task_to_start, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Retrieve the current stats of interest for this job. */
	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members = djob_rec->get_stats()->get_num_members();
	int num_pending = djob_rec->get_stats()->get_num_pending();
	int num_inprogress = djob_rec->get_stats()->get_num_inprogress();
	delete djob_rec;

	/*
	 * Start a task; verify the task that is started and that it has moved
	 * from the pending SBT to the in-progress SBT.
	 */
	/* false: non-recovery */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	recall_task *started_recall_task =
	    dynamic_cast<recall_task *>(started_task);
	fail_if (started_recall_task == NULL);

	fail_if (started_recall_task->get_priority() !=
	    task_to_start.get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    task_to_start.get_priority(), started_recall_task->get_priority());
	fail_if (started_recall_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_recall_task->get_location()));
	fail_if (started_recall_task->get_lin() != task_to_start.get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(task_to_start.get_lin()),
	    lin_fmt(started_recall_task->get_lin()));
	fail_if (started_recall_task->get_snapid() !=
	    task_to_start.get_snapid(),
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(task_to_start.get_snapid()),
	    snapid_fmt(started_recall_task->get_snapid()));
	fail_if (started_recall_task->get_retain_stub() !=
	    task_to_start.get_retain_stub(),
	    "retain stub mismatch (exp: %s act: %s)",
	    task_to_start.get_retain_stub() ? "yes" : "no",
	    started_recall_task->get_retain_stub() ? "yes" : "no");

	/* Verify the stats have been updated correctly. */
	djob_rec = daemon_job_manager::get_daemon_job_record(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_rec == NULL);
	fail_if (djob_rec->get_stats()->get_num_members() != num_members,
	    "num_members mismatch (exp: %d act: %d)", num_members,
	    djob_rec->get_stats()->get_num_members());
	fail_if (djob_rec->get_stats()->get_num_pending() != num_pending - 1,
	    "num_pending mismatch (exp: %d act: %d)", num_pending - 1,
	    djob_rec->get_stats()->get_num_pending());
	fail_if (djob_rec->get_stats()->get_num_inprogress() !=
	    num_inprogress + 1, "num_inprogress mismatch (exp: %d act: %d)",
	    num_inprogress + 1, djob_rec->get_stats()->get_num_inprogress());
	delete djob_rec;

	/*
	 * Attempt to get entry from pending SBT; it should no longer be there.
	 */
	int pending_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), 0, false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* true: ignore ENOENT */
	get_sbt_entry_ut(pending_sbt_fd, task_to_start.get_key(), NULL, 0,
	    NULL, true);

	int inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* false: don't ignore ENOENT */
	get_sbt_entry_ut(inprogress_sbt_fd, task_to_start.get_key(), NULL, 0,
	    NULL, false);

	delete started_task;

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a single element to each pending SBT of a server_task_map with two
 * priorities.  Start a task and verify the higher priority task was started.
 */
TEST(test_start_task__single_element_multiple_priorities, CK_MEM_LEAKS,
    EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	int num_priorities = 2;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job to which the task will be added. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create the tasks and add them to the pending SBTs. */
	cache_invalidation_task low_pri_task;
	low_pri_task.set_priority(0);
	low_pri_task.set_location(TASK_LOC_PENDING);
	low_pri_task.set_lin(12345);
	low_pri_task.set_snapid(54321);

	cache_invalidation_task high_pri_task;
	high_pri_task.set_priority(1);
	high_pri_task.set_location(TASK_LOC_PENDING);
	high_pri_task.set_lin(2468);
	high_pri_task.set_snapid(1357);

	client_task_map ctm(op_type);
	ctm.add_task(&low_pri_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	ctm.add_task(&high_pri_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Start a task; verify the task that is started and that it has moved
	 * from the pending SBT to the in-progress SBT.
	 */
	/* false: non-recovery */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	cache_invalidation_task *started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_priority() !=
	    high_pri_task.get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    high_pri_task.get_priority(), started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));
	fail_if (started_ci_task->get_lin() != high_pri_task.get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(high_pri_task.get_lin()),
	    lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() !=
	    high_pri_task.get_snapid(),
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(high_pri_task.get_snapid()),
	    snapid_fmt(started_ci_task->get_snapid()));

	/* Find both tasks and verify their locations. */
	/* false: don't ignore ENOENT */
	task *found_task = stm.find_task(high_pri_task.get_key(), false,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	/* false: don't ignore ENOENT */
	found_task = stm.find_task(low_pri_task.get_key(), false,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_PENDING,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_PENDING),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	delete started_task;
}

/*
 * Add a single element to each of the lower two pending SBTs of a
 * server_task_map with three priorities.  Start a task and verify the higher
 * priority task was started.  Add an element to the highest priority pending
 * SBT.  Start a task a verify this new task was started.
 */
TEST(test_start_task__single_element_multiple_priorities_insert, CK_MEM_LEAKS,
    EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	int num_priorities = 3;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job to which the task will be added. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create the tasks and add the lower two to the pending SBTs. */
	recall_task low_pri_task;
	low_pri_task.set_priority(0);
	low_pri_task.set_location(TASK_LOC_PENDING);
	low_pri_task.set_lin(12345);
	low_pri_task.set_snapid(54321);

	recall_task mid_pri_task;
	mid_pri_task.set_priority(1);
	mid_pri_task.set_location(TASK_LOC_PENDING);
	mid_pri_task.set_lin(23456);
	mid_pri_task.set_snapid(65432);

	recall_task high_pri_task;
	high_pri_task.set_priority(2);
	high_pri_task.set_location(TASK_LOC_PENDING);
	high_pri_task.set_lin(2468);
	high_pri_task.set_snapid(1357);

	client_task_map ctm(op_type);
	ctm.add_task(&low_pri_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	ctm.add_task(&mid_pri_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Start a task; verify the task that is started and that it has moved
	 * from the pending SBT to the in-progress SBT.
	 */
	task *started_task = NULL;
	recall_task *started_recall_task = NULL;

	/* false: non-recovery */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_recall_task = dynamic_cast<recall_task *>(started_task);
	fail_if (started_recall_task == NULL);

	fail_if (started_recall_task->get_priority() !=
	    mid_pri_task.get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    mid_pri_task.get_priority(), started_recall_task->get_priority());
	fail_if (started_recall_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_recall_task->get_location()));
	fail_if (started_recall_task->get_lin() != mid_pri_task.get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(mid_pri_task.get_lin()),
	    lin_fmt(started_recall_task->get_lin()));
	fail_if (started_recall_task->get_snapid() !=
	    mid_pri_task.get_snapid(),
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(mid_pri_task.get_snapid()),
	    snapid_fmt(started_recall_task->get_snapid()));

	delete started_task;

	/* Find both tasks and verify their locations. */
	task *found_task = NULL;

	/* false: don't ignore ENOENT */
	found_task = stm.find_task(mid_pri_task.get_key(), false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	/* false: don't ignore ENOENT */
	found_task = stm.find_task(low_pri_task.get_key(), false,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_PENDING,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_PENDING),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	/* Add the highest priority task and start another task. */
	ctm.add_task(&high_pri_task, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* false: non-recovery */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_recall_task = dynamic_cast<recall_task *>(started_task);
	fail_if (started_recall_task == NULL);

	fail_if (started_recall_task->get_priority() !=
	    high_pri_task.get_priority(),
	    "priority mismatch (exp: %d act: %d)",
	    high_pri_task.get_priority(), started_recall_task->get_priority());
	fail_if (started_recall_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_recall_task->get_location()));
	fail_if (started_recall_task->get_lin() != high_pri_task.get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(high_pri_task.get_lin()),
	    lin_fmt(started_recall_task->get_lin()));
	fail_if (started_recall_task->get_snapid() !=
	    high_pri_task.get_snapid(),
	    "snapid mismatch (exp: %{} act: %{})",
	    snapid_fmt(high_pri_task.get_snapid()),
	    snapid_fmt(started_recall_task->get_snapid()));

	delete started_task;

	/* Find all tasks and verify their locations. */
	/* false: don't ignore ENOENT */
	found_task = stm.find_task(high_pri_task.get_key(), false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	found_task = stm.find_task(mid_pri_task.get_key(), false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	/* false: don't ignore ENOENT */
	found_task = stm.find_task(low_pri_task.get_key(), false,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (found_task == NULL);
	fail_if (found_task->get_location() != TASK_LOC_PENDING,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_PENDING),
	    task_location_fmt(found_task->get_location()));
	delete found_task;

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/* A helper function for the next unit test. */
static void
_add_task(const server_task_map_ut *stm, int task_index, djob_id_t djob_id,
    int priority, bool claimed, bool too_soon, int call_linenum)
{
	fail_if (stm == NULL);
	fail_if (stm->get_op_type() != OT_CACHE_INVALIDATION,
	    "test requires cache invalidation operation type as coded");
	fail_if (task_index < 0, "task_index: %d", task_index);
	fail_if (djob_id == DJOB_RID_INVALID);
	fail_if (priority < 0, "priority: %d", priority);

	struct isi_error *error = NULL;

	cache_invalidation_task task_to_add;
	task_to_add.set_lin(1000 + task_index);
	task_to_add.set_snapid(HEAD_SNAPID);
	task_to_add.set_priority(priority);
	task_to_add.set_location(TASK_LOC_PENDING);

	if (too_soon){
		struct timeval later;
		gettimeofday(&later, NULL);
		later.tv_sec += 3600;

		task_to_add.set_wait_until_time(later);
	}

	client_task_map ctm(stm->get_op_type());
	ctm.add_task(&task_to_add, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	if (claimed) {
		bool task_claimed = claim_task_as_someone_else(stm,
		    &task_to_add, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (!task_claimed);
	}
}

#define ut_add_task_helper(stm, index, djob_id, priority, claimed, too_soon) \
	_add_task(stm, index, djob_id, priority, claimed, too_soon, __LINE__)

/*
 * Test start_task in normal mode where there are various entries in various
 * priority SBTs with various states ([un]claimed, wait_until time [not yet]
 * reached.
 */
TEST(test_start_task__various_everything, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	int num_priorities = 4;
	task *started_task = NULL;
	cache_invalidation_task *started_ci_task = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * setup:
	 * [priority]:[num entries] (array_index:entry) ...
	 * 3:2 (0:claimed) (1:available)
	 * 2:0
	 * 1:5 (2:too soon) (3:claimed) (4:too soon) (5:available) (6:claimed)
	 * 0:6 (7:available) (8:too soon) (9:claimed) (10:claimed)
	 *     (11:available) (12:too soon)
	 */

	/* 1st boolean: claimed/unclaimed -- 2nd boolean: too soon/ready */
	ut_add_task_helper(&stm,  0, djob_id, 3,  true, false);
	ut_add_task_helper(&stm,  1, djob_id, 3, false, false);
	ut_add_task_helper(&stm,  2, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  3, djob_id, 1,  true, false);
	ut_add_task_helper(&stm,  4, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  5, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  6, djob_id, 1,  true, false);
	ut_add_task_helper(&stm,  7, djob_id, 0, false, false);
	ut_add_task_helper(&stm,  8, djob_id, 0, false,  true);
	ut_add_task_helper(&stm,  9, djob_id, 0,  true, false);
	ut_add_task_helper(&stm, 10, djob_id, 0,  true, false);
	ut_add_task_helper(&stm, 11, djob_id, 0, false, false);
	ut_add_task_helper(&stm, 12, djob_id, 0, false,  true);

	/* The first task returned by start_task should be index 1. */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_lin() != 1001,
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(1001), lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() != HEAD_SNAPID,
	    "snapid mismatch (exp: %{} act: %{})", snapid_fmt(HEAD_SNAPID),
	    snapid_fmt(started_ci_task->get_snapid()));
	fail_if (started_ci_task->get_priority() != 3,
	    "priority mismatch (exp: 3 act: %d)",
	    started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));

	delete started_task;

	/* The second task returned by start_task should be index 5. */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_lin() != 1005,
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(1005), lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() != HEAD_SNAPID,
	    "snapid mismatch (exp: %{} act: %{})", snapid_fmt(HEAD_SNAPID),
	    snapid_fmt(started_ci_task->get_snapid()));
	fail_if (started_ci_task->get_priority() != 1,
	    "priority mismatch (exp: 1 act: %d)",
	    started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));

	delete started_task;

	/* The third task returned by start_task should be index 7. */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_lin() != 1007,
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(1007), lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() != HEAD_SNAPID,
	    "snapid mismatch (exp: %{} act: %{})", snapid_fmt(HEAD_SNAPID),
	    snapid_fmt(started_ci_task->get_snapid()));
	fail_if (started_ci_task->get_priority() != 0,
	    "priority mismatch (exp: 0 act: %d)",
	    started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));

	delete started_task;

	/* The final task returned by start_task should be index 11. */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_lin() != 1011,
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(1011), lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() != HEAD_SNAPID,
	    "snapid mismatch (exp: %{} act: %{})", snapid_fmt(HEAD_SNAPID),
	    snapid_fmt(started_ci_task->get_snapid()));
	fail_if (started_ci_task->get_priority() != 0,
	    "priority mismatch (exp: 0 act: %d)",
	    started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));

	delete started_task;

	/*
	 * Additional calls to start_task should return NULL as there are no
	 * more eligible tasks to start.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx != NULL);
}

/*
 * Test start_task in normal mode where several tasks exist, but none are
 * qualified for processing due to their job being paused.  (Note: tasks could
 * have been made ineligible due to a too-soon wait_until time; there's nothing
 * special about pausing the job in this case.)
 */
TEST(test_start_task__no_qualified_tasks, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	int num_priorities = 4;
	task *started_task = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * All tasks are unclaimed and eligible for processing (from a
	 * wait_until perspective).
	 */
	ut_add_task_helper(&stm,  0, djob_id, 3, false, false);
	ut_add_task_helper(&stm,  1, djob_id, 3, false, false);
	ut_add_task_helper(&stm,  2, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  3, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  4, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  5, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  6, djob_id, 1, false, false);
	ut_add_task_helper(&stm,  7, djob_id, 0, false, false);
	ut_add_task_helper(&stm,  8, djob_id, 0, false, false);
	ut_add_task_helper(&stm,  9, djob_id, 0, false, false);
	ut_add_task_helper(&stm, 10, djob_id, 0, false, false);
	ut_add_task_helper(&stm, 11, djob_id, 0, false, false);
	ut_add_task_helper(&stm, 12, djob_id, 0, false, false);

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 5;
	djob_op_job_task_state task_state;

	/*
	 * The first start_task call should return no task (since none are
	 * qualified) but should return EAGAIN to signal that not all tasks
	 * were considered before returning and that there might be a qualified
	 * task that wasn't inspected yet.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * The second call to start_task should act the same as the first since
	 * we haven't inspected all tasks yet.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * The third call to start_task should act slightly differently as to
	 * inform that no qualified tasks exist to start.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx != NULL);
}

/*
 * Test start_task in normal mode where several tasks exist, but only one is
 * qualified for processing and multiple attempts are needed to start it due to
 * the max_task_reads variable.
 */
TEST(test_start_task__one_qualified_task, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_CACHE_INVALIDATION;
	int num_priorities = 4;
	task *started_task = NULL;
	cache_invalidation_task *started_ci_task = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
        fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* 1st boolean: claimed/unclaimed -- 2nd boolean: too soon/ready */
	ut_add_task_helper(&stm,  0, djob_id, 3, false,  true);
	ut_add_task_helper(&stm,  1, djob_id, 3, false,  true);
	ut_add_task_helper(&stm,  2, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  3, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  4, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  5, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  6, djob_id, 1, false,  true);
	ut_add_task_helper(&stm,  7, djob_id, 0, false,  true);
	ut_add_task_helper(&stm,  8, djob_id, 0, false,  true);
	ut_add_task_helper(&stm,  9, djob_id, 0, false,  true);
	ut_add_task_helper(&stm, 10, djob_id, 0, false,  true);
	ut_add_task_helper(&stm, 11, djob_id, 0, false,  true);
	ut_add_task_helper(&stm, 12, djob_id, 0, false, false);

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 3;
	djob_op_job_task_state task_state;

	/*
	 * The first start_task call should return no task (since none are
	 * qualified) but should return EAGAIN to signal that not all tasks
	 * were considered before returning and that there might be a qualified
	 * task that wasn't inspected yet.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * The second, third, and fourth calls to start_task should act the
	 * same as the first since we haven't inspected all tasks yet.
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	// 3
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	// 4
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error == NULL || !isi_system_error_is_a(error, EAGAIN),
	    "expected EAGAIN: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL);
	fail_if (ctx == NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * The fifth call to start_task should return a task (and the right
	 * one, of course).
	 */
	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	started_ci_task =
	    dynamic_cast<cache_invalidation_task *>(started_task);
	fail_if (started_ci_task == NULL);

	fail_if (started_ci_task->get_lin() != 1012,
	    "lin mismatch (exp: %{} act: %{})",
	    lin_fmt(1011), lin_fmt(started_ci_task->get_lin()));
	fail_if (started_ci_task->get_snapid() != HEAD_SNAPID,
	    "snapid mismatch (exp: %{} act: %{})", snapid_fmt(HEAD_SNAPID),
	    snapid_fmt(started_ci_task->get_snapid()));
	fail_if (started_ci_task->get_priority() != 0,
	    "priority mismatch (exp: 0 act: %d)",
	    started_ci_task->get_priority());
	fail_if (started_ci_task->get_location() != TASK_LOC_INPROGRESS,
	    "location mismatch (exp: %{} act: %{})",
	    task_location_fmt(TASK_LOC_INPROGRESS),
	    task_location_fmt(started_ci_task->get_location()));

	delete started_task;
}

/*
 * Add a task to a job, pause the job, and start the task; the task should not
 * be started.  Cancel the job then start the task again; the task should be
 * started.
 */
TEST(test_start_task__pause_cancel, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_RECALL;
	int num_priorities = 4;

	/* Initialization. */
	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a job. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create a task to add to the job. */
	recall_task r_t;
	r_t.set_lin(34567);
	r_t.set_snapid(76543);
	r_t.set_retain_stub(false);
	r_t.set_priority(0);

	client_task_map ctm(stm.get_op_type());

	ctm.add_task(&r_t, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Pause the job before starting the task; the paused task should not
	 * be returned from start_task().
	 */
	daemon_job_manager::pause_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task != NULL, "expected no task to be returned: %{}",
	    cpool_task_fmt(started_task));
	fail_if (ctx != NULL);

	/*
	 * Cancel the job and attempt to start the task again, this time it
	 * should be started.
	 */
	daemon_job_manager::cancel_job(djob_id, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	started_task = stm.start_task(ctx, max_task_reads, false, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL, "expected task to be returned");
	fail_if (ctx != NULL);
	fail_if (task_state != DJOB_OJT_CANCELLED, "expected %{} (not %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_CANCELLED),
	    djob_op_job_task_state_fmt(task_state));

	delete started_task;

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Add a task to two different jobs, start the task, and see that the stats are
 * updated properly.
 */
TEST(test_start_task__one_task_two_jobs, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 2;
	daemon_job_record *djob_rec = NULL;

	fail_if (!op_type->is_multi_job(),
	    "operation type must support multiple jobs "
	    "for this test to be valid");

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create the two jobs. */
	djob_id_t djob_id_1 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	djob_id_t djob_id_2 =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create the task to add to both jobs.  Give it the higher priority so
	 * it will get started over other task (see below).
	 */
	archive_task arc_task_1;
	arc_task_1.set_lin(369);
	arc_task_1.set_snapid(741);
	arc_task_1.set_priority(1);
	arc_task_1.set_policy_id(11111);

	/*
	 * Create a second task to add to one of the jobs, to make its stats
	 * different from those of the other job.
	 */
	archive_task arc_task_2;
	arc_task_2.set_lin(482);
	arc_task_2.set_snapid(864);
	arc_task_2.set_priority(0);
	arc_task_2.set_policy_id(11112);

	/*
	 * Add the higher priority task to both jobs and the lower priority
	 * task to either job.
	 */
	client_task_map ctm(stm.get_op_type());

	ctm.add_task(&arc_task_1, djob_id_1, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	ctm.add_task(&arc_task_1, djob_id_2, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	ctm.add_task(&arc_task_2, djob_id_2, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Retrieve the stats for each job before starting. */
	djob_rec =
	    daemon_job_manager::get_daemon_job_record(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members_1 = djob_rec->get_stats()->get_num_members();
	int num_pending_1 = djob_rec->get_stats()->get_num_pending();
	int num_inprogress_1 = djob_rec->get_stats()->get_num_inprogress();
	delete djob_rec;

	djob_rec =
	    daemon_job_manager::get_daemon_job_record(djob_id_2, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	int num_members_2 = djob_rec->get_stats()->get_num_members();
	int num_pending_2 = djob_rec->get_stats()->get_num_pending();
	int num_inprogress_2 = djob_rec->get_stats()->get_num_inprogress();
	delete djob_rec;

	/* Start the task and verify its the one we expect. */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task->get_priority() != 1, "wrong task retrieved?");
	fail_if (ctx != NULL);

	/* Retrieve the stats again and verify. */
	djob_rec =
	    daemon_job_manager::get_daemon_job_record(djob_id_1, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_rec == NULL);
	fail_if (djob_rec->get_stats()->get_num_members() != num_members_1,
	    "num_members mismatch (exp: %d act: %d)", num_members_1,
	    djob_rec->get_stats()->get_num_members());
	fail_if (djob_rec->get_stats()->get_num_pending() != num_pending_1 - 1,
	    "num_pending mismatch (exp: %d act: %d)", num_pending_1 - 1,
	    djob_rec->get_stats()->get_num_pending());
	fail_if (djob_rec->get_stats()->get_num_inprogress() !=
	    num_inprogress_1 + 1, "num_inprogress mismatch (exp: %d act: %d)",
	    num_inprogress_1 + 1, djob_rec->get_stats()->get_num_inprogress());
	delete djob_rec;

	djob_rec =
	    daemon_job_manager::get_daemon_job_record(djob_id_2, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (djob_rec == NULL);
	fail_if (djob_rec->get_stats()->get_num_members() != num_members_2,
	    "num_members mismatch (exp: %d act: %d)", num_members_2,
	    djob_rec->get_stats()->get_num_members());
	fail_if (djob_rec->get_stats()->get_num_pending() != num_pending_2 - 1,
	    "num_pending mismatch (exp: %d act: %d)", num_pending_2 - 1,
	    djob_rec->get_stats()->get_num_pending());
	fail_if (djob_rec->get_stats()->get_num_inprogress() !=
	    num_inprogress_2 + 1, "num_inprogress mismatch (exp: %d act: %d)",
	    num_inprogress_2 + 1, djob_rec->get_stats()->get_num_inprogress());
	delete djob_rec;

	delete started_task;

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Test start_task in a recovery scenario.
 *
 * Memory leak check disabled on this test because the amount leaked varies
 * when running as part of the suite versus by itself.
 */
TEST(test_start_task__recovery, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_WRITEBACK;
	int num_priorities = 1;
	task *started_task = NULL;
	task *relinquished_task = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * Create five tasks overall, three of which will be started.  We'll
	 * relinquish the claim on one of these three tasks to simulate a
	 * crash by isi_cpool_d, then start a task in recovery mode; the
	 * started task should be the unclaimed one.
	 */
	client_task_map ctm(stm.get_op_type());
	writeback_task wb_tasks[5];
	for (size_t i = 0; i < sizeof wb_tasks/sizeof wb_tasks[0]; ++i) {
		wb_tasks[i].set_lin(20000 + i);
		wb_tasks[i].set_snapid(HEAD_SNAPID);
		wb_tasks[i].set_priority(0);

		ctm.add_task(&(wb_tasks[i]), djob_id, NULL, &error);
		fail_if (error != NULL, "unexpected error (i = %d): %#{}",
		    i, isi_error_fmt(error));
	}

	/*
	 * Start two of them (doesn't matter which).  We cheat a little bit
	 * here and use another thread to claim the task, which means we need
	 * to relinquish our claim first.
	 */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	for (int i = 0; i < 2; ++i) {
		started_task = stm.start_task(ctx, max_task_reads, false,
		    task_state, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (ctx != NULL);

		bool relinquished = stm.relinquish_task(started_task, &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (!relinquished);

		/* Spin off another thread to claim the task. */
		bool claimed = claim_task_as_someone_else(&stm, started_task,
		    &error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
		fail_if (!claimed);

		delete started_task;
	}

	/*
	 * Save the third started task; this is the one on which we're going to
	 * relinquish the claim and will need it later for comparison purposes.
	 */
	relinquished_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);
	stm.relinquish_task(relinquished_task, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	writeback_task *relinquished_wb_task =
	    dynamic_cast<writeback_task *>(relinquished_task);
	fail_if (relinquished_wb_task == NULL);

	/*
	 * Recap: at this point, there are two tasks in the pending SBT and
	 * three tasks in the in-progress SBT, one of which is unclaimed.
	 *
	 * Start a task in recovery mode, which should be equivalent to the one
	 * pointed to by relinquished_task.
	 */
	started_task = stm.start_task(ctx, max_task_reads, true, task_state,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (started_task == NULL);
	fail_if (ctx != NULL);

	writeback_task *started_wb_task =
	    dynamic_cast<writeback_task *>(started_task);
	fail_if (started_wb_task == NULL);

	fail_if (started_wb_task->get_lin() != relinquished_wb_task->get_lin(),
	    "lin mismatch (exp: %{} act: %{})",
	    relinquished_wb_task->get_lin(), started_wb_task->get_lin());

	delete started_task;
	delete relinquished_task;
}

static void
test_start_task_ufp_helper(const char *failpoint)
{
	fail_if (failpoint == NULL);

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 3, ret;
	struct ufp *ufp = NULL;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create and add some tasks. */
	client_task_map ctm(stm.get_op_type());

	archive_task task_to_add_1;
	task_to_add_1.set_priority(2);
	task_to_add_1.set_location(TASK_LOC_PENDING);
	task_to_add_1.set_lin(123);
	task_to_add_1.set_snapid(321);
	task_to_add_1.set_policy_id(11111);

	archive_task task_to_add_2;
	task_to_add_2.set_priority(0);
	task_to_add_2.set_location(TASK_LOC_PENDING);
	task_to_add_2.set_lin(456);
	task_to_add_2.set_snapid(654);
	task_to_add_2.set_policy_id(22222);

	ctm.add_task(&task_to_add_1, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	ctm.add_task(&task_to_add_2, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Enable the failpoint. */
	ufp_globals_rd_lock();
	ufp = ufail_point_lookup(failpoint);
	ufp_globals_rd_unlock();
	fail_if (ufp == NULL);

	ufp_globals_wr_lock();
	ret = ufail_point_set(ufp, "return(1)");
	ufp_globals_wr_unlock();
	fail_if (ret != 0, "%s", strerror(ret));

	/* Start a task and verify the error that is returned. */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error == NULL, "expected (injected) ENOTSOCK");
	fail_if (!isi_system_error_is_a(error, ENOTSOCK),
	    "expected (injected) ENOTSOCK: %#{}", isi_error_fmt(error));
	fail_if (started_task != NULL, "expected NULL %{}",
	    cpool_task_fmt(started_task));
	fail_if (ctx != NULL);

	isi_error_free(error);

	ufp_globals_wr_lock();
	ufail_point_destroy(ufp);
	ufp_globals_wr_unlock();

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Test start_task with injected failures.
 *
 */
TEST(test_start_task__failed_read, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_start_task_ufp_helper("cpool_server_task_map__read_task_failure");
}

TEST(test_start_task__failed_qualify, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_start_task_ufp_helper(
	    "cpool_server_task_map__qualify_task_failure");
}

TEST(test_start_task__failed_finalize, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_start_task_ufp_helper("cpool_server_task_map__finalize_failure");
}

static void
test_finish_task_helper(task_process_completion_reason reason)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = OT_ARCHIVE;
	int num_priorities = 4;

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Create the task that we will eventually finish. */
	archive_task task_to_finish;
	task_to_finish.set_lin(12345);
	task_to_finish.set_snapid(54321);
	task_to_finish.set_priority(0);
	task_to_finish.set_policy_id(11111);

	/* Create a job to which we'll add the task. */
	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(&task_to_finish, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/* Start the task. */
	/* false: non-recovery */
	server_task_map::read_task_ctx *ctx = NULL;
	int max_task_reads = 10;
	djob_op_job_task_state task_state;
	task *started_task = stm.start_task(ctx, max_task_reads, false,
	    task_state, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (ctx != NULL);

	/* Finish the task with the specified reason. */
	stm.finish_task(started_task->get_key(), reason, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * If the reason is CPOOL_TASK_SUCCESS, CPOOL_TASK_CANCELLED, or
	 * CPOOL_TASK_NON_RETRYABLE_ERROR, the task shouldn't exist anywhere in
	 * the task map.  If the reason is CPOOL_TASK_STOPPED, the task should
	 * still be in the in-progress SBT.  Otherwise (for CPOOL_TASK_PAUSED
	 * and CPOOL_TASK_RETRYABLE_ERROR), we expect to find the task back in
	 * the pending SBT.  Furthermore, the number of attempts should
	 * increase if the reason is CPOOL_TASK_RETRYABLE_ERROR (but not for
	 * CPOOL_TASK_PAUSED).
	 */

	/* true: don't return ENOENT as an error */
	task *retrieved_task = stm.find_task(started_task->get_key(),
	    true, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

	if (reason == CPOOL_TASK_SUCCESS || reason == CPOOL_TASK_CANCELLED ||
	    reason == CPOOL_TASK_NON_RETRYABLE_ERROR) {
		fail_if (retrieved_task != NULL, "found task (%{})",
		    cpool_task_fmt(retrieved_task));
	}
	else if (reason == CPOOL_TASK_STOPPED) {
		fail_if (retrieved_task == NULL);
		fail_if (retrieved_task->get_location() != TASK_LOC_INPROGRESS,
		    "location mismatch (exp: %{} act: %{})",
		    task_location_fmt(TASK_LOC_INPROGRESS),
		    task_location_fmt(retrieved_task->get_location()));

		delete retrieved_task;
	}
	else {
		fail_if (retrieved_task == NULL);
		fail_if (retrieved_task->get_location() != TASK_LOC_PENDING,
		    "location mismatch (exp: %{} act: %{})",
		    task_location_fmt(TASK_LOC_PENDING),
		    task_location_fmt(retrieved_task->get_location()));

		fail_if (reason == CPOOL_TASK_RETRYABLE_ERROR &&
		    retrieved_task->get_num_attempts() != 1,
		    "num_attempts mismatch (exp: 1 act: %d)",
		    retrieved_task->get_num_attempts());

		delete retrieved_task;
	}

	delete started_task;

	/* Cleanup the gconfig context. */
	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * Test finish_task for a successful task.
 *
 */
TEST(test_finish_task__success, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_SUCCESS);
}

/*
 * Test finish_task for a paused task.
 *
 */
TEST(test_finish_task__paused, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_PAUSED);
}

/*
 * Test finish_task for a cancelled task.
 */
TEST(test_finish_task__cancelled, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_CANCELLED);
}

/*
 * Test finish_task for a stopped task.
 */
TEST(test_finish_task__stopped, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_STOPPED);
}

/*
 * Test finish_task for a task with a retryable error.
 */
TEST(test_finish_task__retryable_error, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_RETRYABLE_ERROR);
}

/*
 * Test finish_task for a task with a non-retryable error.
 */
TEST(test_finish_task__non_retryable_error, CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	test_finish_task_helper(CPOOL_TASK_NON_RETRYABLE_ERROR);
}

/*
 * Helper functions for some of the unit tests.
 */
static struct {
	const task_key *task_to_lock_;
	bool block_;
	bool exclusive_;
	struct isi_error **error_;
} lock_work_item;

static void *locker(void *arg)
{
	struct isi_error *error = NULL;
	server_task_map_ut *stm = NULL;

	if (arg == NULL) {
		error = isi_system_error_new(EINVAL,
		    "passed-in arg must be non-NULL");
		goto out;
	}

	stm = static_cast<server_task_map_ut *>(arg);
	if (arg == NULL) {
		error = isi_system_error_new(EINVAL,
		    "failed to cast passed-in arg as server_task_map_ut");
		goto out;
	}

	stm->lock_task(lock_work_item.task_to_lock_, lock_work_item.block_,
	    lock_work_item.exclusive_, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, lock_work_item.error_);

	pthread_exit(NULL);
}

static void
lock_task_as_someone_else(const server_task_map_ut *stm,
    const task_key *task_to_lock, bool block, bool exclusive,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	lock_work_item.task_to_lock_ = task_to_lock;
	lock_work_item.block_ = block;
	lock_work_item.exclusive_ = exclusive;
	lock_work_item.error_ = &error;

	pthread_t cr_thread;
	pthread_attr_t cr_thread_attr;

	/*
	 * Initialize thread creation attributes.  We don't define anything
	 * for these attributes, but this reduces memory leak noise.
	 */
	fail_if (pthread_attr_init(&cr_thread_attr) != 0,
	    "failed to initialize thread attributes: %s", strerror(errno));
	fail_if (pthread_create(&cr_thread, &cr_thread_attr, locker,
	    &stm) != 0, "failed to create locker thread: %s",
	    strerror(errno));
	fail_if (pthread_join(cr_thread, NULL) != 0,
	    "failed to join locker thread: %s", strerror(errno));
	fail_if (pthread_attr_destroy(&cr_thread_attr) != 0,
	    "failed to destroy thread attributes: %s", strerror(errno));

	isi_error_handle(error, error_out);
}

static void start_task_helper__sbt_ent_to_claimed_locked_task__helper(
    bool use_hsbt, bool change_entry_after_reading, bool change_to_bogus_task,
    bool remove_entry_after_reading)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type =
	    use_hsbt ? OT_CLOUD_GC : OT_ARCHIVE;

	/*
	 * Create a task to add to the SBT; this is the task that we will read,
	 * potentially alter, and then use to create a locked task object.
	 */
	isi_cloud::task *task_to_add = isi_cloud::task::create_task(op_type);
	fail_if (task_to_add == NULL);

	/* used with both operation types */
	int priority = 0;

	/* used with OT_ARCHIVE */
	ifs_lin_t lin = 12345;
	ifs_snapid_t snapid = 23456;
	uint32_t policy_id = 22222;

	/* used with OT_CLOUD_GC */
	isi_cloud_object_id object_id(1, 2);
	object_id.set_snapid(3);

	task_to_add->set_priority(priority);

	if (op_type == OT_ARCHIVE) {
		isi_cloud::archive_task *archive_task_to_add =
		    static_cast<isi_cloud::archive_task *>(task_to_add);
		fail_if (archive_task_to_add == NULL);

		archive_task_to_add->set_lin(lin);
		archive_task_to_add->set_snapid(snapid);
		archive_task_to_add->set_policy_id(policy_id);
	}
	else if (op_type == OT_CLOUD_GC) {
		isi_cloud::cloud_gc_task *cloud_gc_task_to_add =
		    static_cast<isi_cloud::cloud_gc_task *>(task_to_add);
		fail_if (cloud_gc_task_to_add == NULL);

		cloud_gc_task_to_add->set_cloud_object_id(object_id);
	}
	else {
		/* This is a fault with the test, not the code. */
		fail ("invalid op_type");
	}

	daemon_job_manager::at_startup(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	djob_id_t djob_id =
	    daemon_job_manager::start_job_no_job_engine(op_type, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	client_task_map ctm(op_type);
	ctm.add_task(task_to_add, djob_id, NULL, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	/*
	 * At this point we've added the task to the SBT, so we'll simulate
	 * part of the start_task function by reading this entry from the SBT.
	 */
	int sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type->get_type(), priority, true, &error);
	fail_if (error != NULL, "failed to get SBT fd: %#{}",
	    isi_error_fmt(error));

	btree_key_t start_key = {{ 0, 0 }};
	char *entry_buf = NULL;
	struct get_sbt_entry_ctx *ctx = NULL;
	struct sbt_entry *sbt_ent = get_sbt_entry(sbt_fd, &start_key,
	    &entry_buf, &ctx, &error);
	fail_if (error != NULL, "get_sbt_entry failed: %#{}",
	    isi_error_fmt(error));
	fail_if (sbt_ent == NULL);

	get_sbt_entry_ctx_destroy(&ctx);

	/* Sanity check - verify we read what we just added to the SBT. */
	const task_key *added_task_key = task_to_add->get_key();
	fail_if (added_task_key == NULL);

	const void *added_task_serialized_key;
	size_t added_task_serialized_key_size;
	added_task_key->get_serialized_key(added_task_serialized_key,
	    added_task_serialized_key_size);

	if (use_hsbt) {
		struct hsbt_entry hsbt_ent;
		sbt_entry_to_hsbt_entry(sbt_ent, &hsbt_ent);
		fail_if (!hsbt_ent.valid_);

		fail_if (
		    hsbt_ent.key_length_ != added_task_serialized_key_size,
		    "unexpected key size (exp: %zu act: %zu)",
		    hsbt_ent.key_length_, added_task_serialized_key_size);

		fail_if (memcmp(hsbt_ent.key_, added_task_serialized_key,
		    added_task_serialized_key_size) != 0, "key mismatch");

		hsbt_entry_destroy(&hsbt_ent);
	}
	else {
		fail_if (added_task_serialized_key_size != sizeof(btree_key_t),
		    "unexpected key size (exp: %zu act: %zu)",
		    sizeof(btree_key_t), added_task_serialized_key_size);

		const btree_key_t *added_task_btree_key =
		    static_cast<const btree_key_t *>(
		    added_task_serialized_key);
		fail_if (added_task_btree_key == NULL);

		fail_if (
		    added_task_btree_key->keys[0] != sbt_ent->key.keys[0] ||
		    added_task_btree_key->keys[1] != sbt_ent->key.keys[1],
		    "task key mismatch (exp: %lu/%lu act: %lu/%lu)",
		    added_task_btree_key->keys[0],
		    added_task_btree_key->keys[1],
		    sbt_ent->key.keys[0], sbt_ent->key.keys[1]);
	}

	/*
	 * Having confirmed that we've read what we think we've read, proceed
	 * to change or delete the entry on disk according to the incoming
	 * parameters.
	 */
	if (change_entry_after_reading) {
		if (change_to_bogus_task) {
			uint32_t vers = CPOOL_TASK_VERSION;
			const char *bogus_task_serialization =
			    "bogus task serialization";

			size_t tbuffer_size = strlen(bogus_task_serialization) +
			    sizeof vers + 1;
			void *tbuffer = calloc(1, tbuffer_size);
			ASSERT(tbuffer != NULL);
			memcpy(tbuffer, (void *) &vers, sizeof vers);
			char *temp = (char *) tbuffer + sizeof vers;
			memcpy(temp, bogus_task_serialization,
			    strlen(bogus_task_serialization));

			fail_if (ifs_sbt_cond_mod_entry(sbt_fd,
			    &(sbt_ent->key), tbuffer,
			    tbuffer_size, NULL, BT_CM_NONE,
			    NULL, 0, NULL) != 0,
			    "failed to modify task on-disk "
			    "with a bogus serialization: [%d] %s",
			    errno, strerror(errno));

			free(tbuffer);
		}
		else {
			/*
			 * In order to change the task on-disk in a non-bogus
			 * way, we can add this task to a second daemon job,
			 * but this won't work if use_hsbt is true.  Why?  It's
			 * not a conceptual issue; there's nothing inherent to
			 * HSBTs that would prohibit us from doing this.
			 * Currently, the only operation that uses HSBTs is
			 * cloud GC, and that is not a multi-job (meaning there
			 * is only one of them).  To add a second daemon job ID
			 * to any task, we'd first create a new daemon job, but
			 * because cloud GC is not a multi-job, the new daemon
			 * job ID would be the same one as the original (and
			 * only) cloud GC daemon job.  In fact, there's no way
			 * to update a cloud GC task without updating the key,
			 * which is invalid for this test.
			 *
			 * So for now, we'll fail the test when this invalid
			 * combination occurs.
			 */
			if (use_hsbt)
				/* see comment above */
				fail ("invalid test case");

			/*
			 * Add another daemon job to the task in order to
			 * change its on-disk version.
			 */
			djob_id_t new_djob_id =
			    daemon_job_manager::start_job_no_job_engine(
			    op_type, &error);
			fail_if (error != NULL, "unexpected error: %#{}",
			    isi_error_fmt(error));

			ctm.add_task(task_to_add, new_djob_id, NULL, &error);
			fail_if (error != NULL, "unexpected error: %#{}",
			    isi_error_fmt(error));
		}
	}

	if (remove_entry_after_reading)
		fail_if (ifs_sbt_cond_remove_entry(sbt_fd, &(sbt_ent->key),
		    BT_CM_NONE, NULL, 0, NULL) != 0,
		    "failed to remove entry after reading: [%d] %s",
		    errno, strerror(errno));

	/*
	 * With the on-disk task properly messed with (or not, as determined by
	 * the input parameters), attempt to convert what we read previously
	 * into a claimed and locked task.
	 */
	int num_priorities = 1;
	server_task_map_ut stm(op_type, num_priorities);
	stm.initialize(&error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	isi_cloud::task *created_task =
	    stm.start_task_helper__sbt_ent_to_claimed_locked_task(sbt_fd,
	    sbt_ent, &error);

	if (change_to_bogus_task) {
		/*
		 * If we updated the on-disk task with a bogus serialization,
		 * we should get an error from
		 * start_task_helper__sbt_ent_to_claimed_locked_task and not
		 * get a task.  Also, we shouldn't hold a claim nor a lock on
		 * the task we originally read.
		 */
		if (use_hsbt) {
			fail_if (error == NULL, "expected EIO");
			fail_if (!isi_system_error_is_a(error, EIO),
			    "expected EIO: %#{}", isi_error_fmt(error));
		}
		else {
			fail_if (error == NULL, "expected EINVAL");
			fail_if (!isi_system_error_is_a(error, EINVAL),
			    "expected EINVAL: %#{}", isi_error_fmt(error));
		}

		isi_error_free(error);
		error = NULL;

		fail_if (created_task != NULL, "unexpected task returned: %{}",
		    cpool_task_fmt(created_task));

		claim_task_as_someone_else(&stm, task_to_add, &error);
		fail_if (error != NULL,
		    "unexpected error attempting to determine "
		    "if task is claimed: %#{}", isi_error_fmt(error));
		fail_if (!claim_work_item.success_,
		    "failed to take claim when it should have succeeded");

		/* false: don't block / true: exclusive lock */
		lock_task_as_someone_else(&stm, added_task_key, false, true,
		    &error);
		fail_if (error != NULL,
		    "failed to take lock when it should have succeeded: %#{}",
		    isi_error_fmt(error));
	}
	else if (remove_entry_after_reading) {
		/*
		 * If we removed the on-disk task, we should get ENOENT from
		 * start_task_helper__sbt_ent_to_claimed_locked_task but we
		 * shouldn't get a task.  Similar to above, we should hold
		 * neither claim nor lock on the task we originally read.
		 */
		fail_if (error == NULL, "expected ENOENT");
		fail_if (!isi_system_error_is_a(error, ENOENT),
		    "expected ENOENT: %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;

		fail_if (created_task != NULL, "unexpected task returned: %{}",
		    cpool_task_fmt(created_task));

		claim_task_as_someone_else(&stm, task_to_add, &error);
		fail_if (error != NULL,
		    "unexpected error attempting to determine "
		    "if task is claimed: %#{}", isi_error_fmt(error));
		fail_if (!claim_work_item.success_,
		    "failed to take claim when it should have succeeded");

		/* false: don't block / true: exclusive lock */
		lock_task_as_someone_else(&stm, added_task_key, false, true,
		    &error);
		fail_if (error != NULL,
		    "failed to take lock when it should have succeeded: %#{}",
		    isi_error_fmt(error));
	}
	else {
		/*
		 * In this case, we re-read either the same or a different task
		 * after locking.  If we re-read the same task, the number of
		 * daemon jobs to which the task belongs will be 1, otherwise
		 * it will be 2.  (Note, this is specific to this unit test,
		 * not inherent to tasks or daemon jobs at any conceptual
		 * level.)
		 *
		 * In either case, we'll verify that no error was returned and
		 * that the task we created is what we expect; we'll also make
		 * sure we're holding both a claim and a lock on the task that
		 * was returned.
		 */
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));

		fail_if (created_task == NULL);

		if (change_entry_after_reading)
			fail_if (created_task->get_daemon_jobs().size() != 2,
			    "expected 2 daemon jobs, not %zu",
			    created_task->get_daemon_jobs().size());
		else
			fail_if (created_task->get_daemon_jobs().size() != 1,
			    "expected 1 daemon jobs, not %zu",
			    created_task->get_daemon_jobs().size());

		if (use_hsbt) {
			isi_cloud::cloud_gc_task *created_cloud_gc_task =
			    dynamic_cast<isi_cloud::cloud_gc_task *>(
			    created_task);
			fail_if (created_cloud_gc_task == NULL);

			fail_if (
			    created_cloud_gc_task->get_cloud_object_id() !=
			    object_id, "object id mismatch "
			    "(exp: %s %lu act: %s %lu)",
			    object_id.to_c_string(), object_id.get_snapid(),
			    created_cloud_gc_task->get_cloud_object_id().\
			    to_c_string(),
			    created_cloud_gc_task->get_cloud_object_id().\
			    get_snapid());
		}
		else {
			isi_cloud::archive_task *created_archive_task =
			    dynamic_cast<isi_cloud::archive_task *>(
			    created_task);
			fail_if (created_archive_task == NULL);

			fail_if (
			    created_archive_task->get_lin() != lin ||
			    created_archive_task->get_snapid() != snapid,
			    "lin/snapid mismatch (exp: %{} act %{})",
			    lin_snapid_fmt(lin, snapid),
			    lin_snapid_fmt(created_archive_task->get_lin(),
			    created_archive_task->get_snapid()));
			fail_if(
				created_archive_task->get_policy_id() != policy_id,
				"policy id mismatch (exp: %d act: %d)",
				policy_id,
				created_archive_task->get_policy_id());
		}

		claim_task_as_someone_else(&stm, task_to_add, &error);
		fail_if (error != NULL,
		    "unexpected error attempting to determine "
		    "if task is claimed: %#{}", isi_error_fmt(error));
		fail_if (claim_work_item.success_,
		    "incorrectly succeeded in taking claim "
		    "when it should have failed");

		/* false: don't block / true: exclusive lock */
		lock_task_as_someone_else(&stm, added_task_key, false, true,
		    &error);
		fail_if (error == NULL, "expected EWOULDBLOCK");
		fail_if (!isi_system_error_is_a(error, EWOULDBLOCK),
		    "expected EWOULDBLOCK: %#{}", isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;
	}

	/* Cleanup our allocated memory before exiting. */
	delete task_to_add;
	delete created_task;
	free(entry_buf);

	{
		struct isi_error *error = NULL;
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		fail_if (error != NULL, "unexpected error: %#{}",
		    isi_error_fmt(error));
	}
}

/*
 * This next set exercises start_task_helper__sbt_ent_to_claimed_locked_task in
 * various ways, specifically: whether to use SBTs or HSBTs, whether or not to
 * update the entry on-disk after reading it, whether or not to update with a
 * bogus serialization, and whether or not to delete the entry on disk after
 * reading.
 */

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__01,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    false,	/* use sbt, not hsbt */
	    true,	/* update entry on disk after read */
	    true,	/* update entry on disk with bogus task */
	    false);	/* don't delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__02,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    false,	/* use sbt, not hsbt */
	    true,	/* update entry on disk after read */
	    false,	/* don't update entry on disk with bogus task */
	    false);	/* don't delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__03,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    false,	/* use sbt, not hsbt */
	    false,	/* don't update entry on disk after read */
	    false,	/* don't update entry on disk with bogus task */
	    true);	/* delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__04,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    false,	/* use sbt, not hsbt */
	    false,	/* don't update entry on disk after read */
	    false,	/* don't update entry on disk with bogus task */
	    false);	/* don't delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__05,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    true,	/* use hsbt, not sbt */
	    true,	/* update entry on disk after read */
	    true,	/* update entry on disk with bogus task */
	    false);	/* don't delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__06,
    CK_MEM_LEAKS, 0)
{
	/*
	 * This is an invalid test case based on the current business logic.
	 * Test kept for completeness, but since it doesn't do anything it
	 * should never fail.
	 *
	 * start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	 *     true,	// use hsbt, not sbt
	 *     true,	// update entry on disk after read
	 *     false,	// don't update entry on disk with bogus task
	 *     false);	// don't delete entry on disk after read
	 */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__07,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    true,	/* use hsbt, not sbt */
	    false,	/* don't update entry on disk after read */
	    false,	/* don't update entry on disk with bogus task */
	    true);	/* delete entry on disk after read */
}

TEST(test_start_task_helper__sbt_ent_to_claimed_locked_task__08,
    CK_MEM_LEAKS, EXPECTED_LEAKS)
{
	start_task_helper__sbt_ent_to_claimed_locked_task__helper(
	    true,	/* use hsbt, not sbt */
	    false,	/* don't update entry on disk after read */
	    false,	/* don't update entry on disk with bogus task */
	    false);	/* don't delete entry on disk after read */
}
