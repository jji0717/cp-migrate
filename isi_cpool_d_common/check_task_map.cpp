#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/task.h>

#include "cpool_fd_store.h"
#include "daemon_job_manager.h"
#include "unit_test_helpers.h"

#include "task_map.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_task_map,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
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

class task_map_ut : public task_map
{
public:
	task_map_ut(const cpool_operation_type *op_type)
		: task_map(op_type) { }
	~task_map_ut() { }

	isi_cloud::task *find_task(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const
	{
		return task_map::find_task(key, ignore_ENOENT, error_out);
	}

	isi_cloud::task *find_task_pending(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const
	{
		return task_map::find_task_pending(key, ignore_ENOENT,
		    error_out);
	}

	isi_cloud::task *find_task_inprogress(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const
	{
		return task_map::find_task_inprogress(key, ignore_ENOENT,
		    error_out);
	}
};

TEST(test_find_task_inprogress_stub)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_ARCHIVE;
	int inprogress_sbt_fd = -1;
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::archive_task *retrieved_archive_task = NULL;

	/* Use the cpool_fd_store to create SBTs we need for this test. */
	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "get_inprogress_sbt_fd for %{} failed: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (inprogress_sbt_fd == -1);

	task_map_ut tm(op_type);

	/*
	 * Create two tasks, add one to to the in-progress SBT, then try to
	 * find each one.
	 */
	ifs_lin_t task_to_find__lin = 12345;
	ifs_snapid_t task_to_find__snapid = 98765;

	isi_cloud::archive_task *task_to_find =
	    new isi_cloud::archive_task(task_to_find__lin,
	    task_to_find__snapid);
	fail_if(task_to_find == NULL);

	task_to_find->set_policy_id(11111);

	int task_to_find__priority = 4;
	task_to_find->set_priority(task_to_find__priority);

	task_to_find->set_state(DJOB_OJT_RUNNING);
	task_to_find->set_location(TASK_LOC_INPROGRESS);

	const void *task_to_find__serialization;
	size_t task_to_find__serialization_size;
	task_to_find->get_serialized_task(task_to_find__serialization,
	    task_to_find__serialization_size);

	add_sbt_entry_ut(inprogress_sbt_fd, task_to_find->get_key(),
	    task_to_find__serialization, task_to_find__serialization_size);

	isi_cloud::archive_task *task_to_not_find =
	    new isi_cloud::archive_task;
	fail_if(task_to_not_find == NULL);

	/*
	 * We don't need to track the values set on task_to_not_find; we don't
	 * expect to find it and therefore won't need to compare what we added
	 * with what we found.
	 */
	task_to_not_find->set_lin(24680);
	task_to_not_find->set_snapid(13579);
	task_to_not_find->set_priority(2);
	task_to_not_find->set_policy_id(22222);

	/*
	 * With both tasks created and only one added to the SBT, search for
	 * each.
	 */

	/* false: don't ignore ENOENT */
	retrieved_task = tm.find_task_inprogress(task_to_find->get_key(),
	    false, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_archive_task =
	    static_cast<isi_cloud::archive_task *>(retrieved_task);
	fail_if(retrieved_archive_task == NULL);

	fail_if(retrieved_archive_task->get_lin() != task_to_find__lin,
	    "lin mismatch (exp: %{} act: %{})", task_to_find__lin,
	    retrieved_archive_task->get_lin());
	fail_if(retrieved_archive_task->get_snapid() != task_to_find__snapid,
	    "snapid mismatch (exp: %{} act: %{})", task_to_find__snapid,
	    retrieved_archive_task->get_snapid());
	fail_if(retrieved_archive_task->get_policy_id() != task_to_find->get_policy_id(),
	    "policy id mismatch (exp: %d act: %d)",
	    task_to_find->get_policy_id(),
	    retrieved_archive_task->get_policy_id());
	fail_if(retrieved_archive_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_archive_task->get_priority());

	delete retrieved_task;

	/* true: ignore ENOENT (included for the sake of completeness) */
	retrieved_task = tm.find_task_inprogress(task_to_find->get_key(), true,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_archive_task =
	    static_cast<isi_cloud::archive_task *>(retrieved_task);
	fail_if(retrieved_archive_task == NULL);

	fail_if(retrieved_archive_task->get_lin() != task_to_find__lin,
	    "lin mismatch (exp: %{} act: %{})", task_to_find__lin,
	    retrieved_archive_task->get_lin());
	fail_if(retrieved_archive_task->get_snapid() != task_to_find__snapid,
	    "snapid mismatch (exp: %{} act: %{})", task_to_find__snapid,
	    retrieved_archive_task->get_snapid());
	fail_if( retrieved_archive_task->get_policy_id() != task_to_find->get_policy_id(),
	    "policy id mismatch (exp: %d act: %d)",
	    task_to_find->get_policy_id(),
	    retrieved_archive_task->get_policy_id());
	fail_if(retrieved_archive_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_archive_task->get_priority());

	delete retrieved_task;

	delete task_to_find;

	/*
	 * Try to retrieve the not-added task without ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_inprogress(task_to_not_find->get_key(),
	    false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * Try to retrieve the not-added task again, this time ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_inprogress(task_to_not_find->get_key(),
	    true, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	delete task_to_not_find;

	close_sbt(inprogress_sbt_fd);
}

/*
 * 20 bytes "leaked" from the static cpool_fd_store instance.
 */
TEST(test_find_task_pending_stub, CK_MEM_LEAKS, 20)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_ARCHIVE;
	const int num_priorities = 4;
	int pending_sbt_fds[num_priorities] = { 0 };
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::archive_task *retrieved_archive_task = NULL;

	for (int i = 0; i < num_priorities; ++i) {
		pending_sbt_fds[i] = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type->get_type(), i, true, &error);
		fail_if (error != NULL,
		    "get_pending_sbt_fd for %{} failed (i: %d): %#{}",
		    task_type_fmt(op_type->get_type()), i,
		    isi_error_fmt(error));
		fail_if (pending_sbt_fds[i] == -1, "i: %d", i);
	}

	task_map_ut tm(op_type);

	/*
	 * Create two tasks, add one to to the pending SBT, then try to
	 * find each one.
	 */
	isi_cloud::archive_task *task_to_find = new isi_cloud::archive_task;
	fail_if(task_to_find == NULL);
	isi_cloud::archive_task *task_to_not_find =
	    new isi_cloud::archive_task;
	fail_if(task_to_not_find == NULL);

	ifs_lin_t task_to_find__lin = 12345;
	task_to_find->set_lin(task_to_find__lin);

	ifs_snapid_t task_to_find__snapid = 98765;
	task_to_find->set_snapid(task_to_find__snapid);
	task_to_find->set_policy_id(11111);

	int task_to_find__priority = 3;
	task_to_find->set_priority(task_to_find__priority);

	task_to_find->set_state(DJOB_OJT_RUNNING);
	task_to_find->set_location(TASK_LOC_PENDING);

	const void *task_to_find__serialization;
	size_t task_to_find__serialization_size;
	task_to_find->get_serialized_task(task_to_find__serialization,
	    task_to_find__serialization_size);

	add_sbt_entry_ut(pending_sbt_fds[task_to_find__priority],
	    task_to_find->get_key(), task_to_find__serialization,
	    task_to_find__serialization_size);

	/*
	 * We don't need to track the values set on task_to_not_find; we don't
	 * expect to find it and therefore won't need to compare what we added
	 * with what we found.
	 */
	task_to_not_find->set_lin(24680);
	task_to_not_find->set_snapid(13579);
	task_to_not_find->set_priority(2);
	task_to_not_find->set_policy_id(22222);

	/*
	 * With both tasks created and only one added to the SBT, search for
	 * each.
	 */

	/* false: don't ignore ENOENT */
	retrieved_task = tm.find_task_pending(task_to_find->get_key(), false,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_archive_task =
	    static_cast<isi_cloud::archive_task *>(retrieved_task);
	fail_if(retrieved_archive_task == NULL);

	fail_if(retrieved_archive_task->get_lin() != task_to_find__lin,
	    "lin mismatch (exp: %{} act: %{})", task_to_find__lin,
	    retrieved_archive_task->get_lin());
	fail_if(retrieved_archive_task->get_snapid() != task_to_find__snapid,
	    "snapid mismatch (exp: %{} act: %{})", task_to_find__snapid,
	    retrieved_archive_task->get_snapid());
	fail_if(retrieved_archive_task->get_policy_id() != task_to_find->get_policy_id(),
	    "policy id mismatch (exp: %d act: %d)",
	    task_to_find->get_policy_id(),
	    retrieved_archive_task->get_policy_id());
	fail_if(retrieved_archive_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_archive_task->get_priority());

	delete retrieved_task;

	/* true: ignore ENOENT (included for the sake of completeness) */
	retrieved_task = tm.find_task_pending(task_to_find->get_key(), true,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_archive_task =
	    static_cast<isi_cloud::archive_task *>(retrieved_task);
	fail_if(retrieved_archive_task == NULL);

	fail_if(retrieved_archive_task->get_lin() != task_to_find__lin,
	    "lin mismatch (exp: %{} act: %{})", task_to_find__lin,
	    retrieved_archive_task->get_lin());
	fail_if(retrieved_archive_task->get_snapid() != task_to_find__snapid,
	    "snapid mismatch (exp: %{} act: %{})", task_to_find__snapid,
	    retrieved_archive_task->get_snapid());
	fail_if( retrieved_archive_task->get_policy_id() != task_to_find->get_policy_id(),
	    "policy id mismatch (exp: %d act: %d)",
		task_to_find->get_policy_id(),
	    retrieved_archive_task->get_policy_id());
	fail_if(retrieved_archive_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_archive_task->get_priority());

	delete retrieved_task;

	delete task_to_find;

	/*
	 * Try to retrieve the not-added task without ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_pending(task_to_not_find->get_key(),
	    false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * Try to retrieve the not-added task again, this time ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_pending(task_to_not_find->get_key(),
	    true, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	delete task_to_not_find;

	for (int i = 0; i < num_priorities; ++i)
		close_sbt(pending_sbt_fds[i]);
}

TEST(test_find_task_inprogress_cloud_gc)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_CLOUD_GC;
	int inprogress_sbt_fd;
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::cloud_gc_task *retrieved_cloud_gc_task = NULL;

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type->get_type(), true, &error);
	fail_if (error != NULL, "get_inprogress_sbt_fd for %{} failed: %#{}",
	    task_type_fmt(op_type->get_type()), isi_error_fmt(error));
	fail_if (inprogress_sbt_fd == -1);

	task_map_ut tm(op_type);

	/*
	 * Create two tasks, add one to to the in-progress SBT, then try to
	 * find each one.
	 */
	isi_cloud::cloud_gc_task *task_to_find = new isi_cloud::cloud_gc_task;
	fail_if(task_to_find == NULL);
	isi_cloud::cloud_gc_task *task_to_not_find =
	    new isi_cloud::cloud_gc_task;
	fail_if(task_to_not_find == NULL);

	isi_cloud_object_id task_to_find__object_id(3692581470, 7418529630);
	task_to_find__object_id.set_snapid(135);
	task_to_find->set_cloud_object_id(task_to_find__object_id);

	int task_to_find__priority = 9;
	task_to_find->set_priority(task_to_find__priority);

	task_to_find->set_state(DJOB_OJT_RUNNING);
	task_to_find->set_location(TASK_LOC_INPROGRESS);

	const void *task_to_find__serialization;
	size_t task_to_find__serialization_size;
	task_to_find->get_serialized_task(task_to_find__serialization,
	    task_to_find__serialization_size);

	add_sbt_entry_ut(inprogress_sbt_fd, task_to_find->get_key(),
	    task_to_find__serialization, task_to_find__serialization_size);

	/*
	 * We don't need to track the values set on task_to_not_find; we don't
	 * expect to find it and therefore won't need to compare what we added
	 * with what we found.
	 */
	isi_cloud_object_id task_to_not_find__object_id(123456789, 567891234);
	task_to_not_find__object_id.set_snapid(246);
	task_to_not_find->set_cloud_object_id(task_to_not_find__object_id);
	task_to_not_find->set_priority(4);

	/*
	 * With both tasks created and only one added to the SBT, search for
	 * each.
	 */

	/* false: don't ignore ENOENT */
	retrieved_task = tm.find_task_inprogress(task_to_find->get_key(),
	    false, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_cloud_gc_task =
	    static_cast<isi_cloud::cloud_gc_task *>(retrieved_task);
	fail_if(retrieved_cloud_gc_task == NULL);

	fail_if(retrieved_cloud_gc_task->get_cloud_object_id() !=
	    task_to_find__object_id,
	    "object ID mismatch (exp: %s/%d act: %s/%d)",
	    task_to_find__object_id.to_c_string(),
	    task_to_find__object_id.get_snapid(),
	    retrieved_cloud_gc_task->get_cloud_object_id().to_c_string(),
	    retrieved_cloud_gc_task->get_cloud_object_id().get_snapid());
	fail_if(retrieved_cloud_gc_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_cloud_gc_task->get_priority());

	delete retrieved_task;

	/* true: ignore ENOENT (included for the sake of completeness) */
	retrieved_task = tm.find_task_inprogress(task_to_find->get_key(), true,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_cloud_gc_task =
	    static_cast<isi_cloud::cloud_gc_task *>(retrieved_task);
	fail_if(retrieved_cloud_gc_task == NULL);

	fail_if(retrieved_cloud_gc_task->get_cloud_object_id() !=
	    task_to_find__object_id,
	    "object ID mismatch (exp: %s/%d act: %s/%d)",
	    task_to_find__object_id.to_c_string(),
	    task_to_find__object_id.get_snapid(),
	    retrieved_cloud_gc_task->get_cloud_object_id().to_c_string(),
	    retrieved_cloud_gc_task->get_cloud_object_id().get_snapid());
	fail_if(retrieved_cloud_gc_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_cloud_gc_task->get_priority());

	delete retrieved_task;

	delete task_to_find;

	/*
	 * Try to retrieve the not-added task without ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_inprogress(task_to_not_find->get_key(),
	    false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * Try to retrieve the not-added task again, this time ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_inprogress(task_to_not_find->get_key(),
	    true, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	delete task_to_not_find;

	close_sbt(inprogress_sbt_fd);
}

/*
 * 48 bytes "leaked" from the static cpool_fd_store instance.
 */
TEST(test_find_task_pending_cloud_gc, CK_MEM_LEAKS, 48)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_CLOUD_GC;
	const int num_priorities = 11;
	int pending_sbt_fds[num_priorities] = { 0 };
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::cloud_gc_task *retrieved_cloud_gc_task = NULL;

	for (int i = 0; i < num_priorities; ++i) {
		pending_sbt_fds[i] = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type->get_type(), i, true, &error);
		fail_if (error != NULL,
		    "get_pending_sbt_fd for %{} failed (i: %d): %#{}",
		    task_type_fmt(op_type->get_type()), i,
		    isi_error_fmt(error));
		fail_if (pending_sbt_fds[i] == -1, "i: %d", i);
	}

	task_map_ut tm(op_type);

	/*
	 * Create two tasks, add one to to the pending SBT, then try to
	 * find each one.
	 */
	isi_cloud::cloud_gc_task *task_to_find = new isi_cloud::cloud_gc_task;
	fail_if(task_to_find == NULL);
	isi_cloud::cloud_gc_task *task_to_not_find =
	    new isi_cloud::cloud_gc_task;
	fail_if(task_to_not_find == NULL);

	isi_cloud_object_id task_to_find__object_id(3692581470, 7418529630);
	task_to_find__object_id.set_snapid(135);
	task_to_find->set_cloud_object_id(task_to_find__object_id);

	int task_to_find__priority = 3;
	task_to_find->set_priority(task_to_find__priority);

	task_to_find->set_state(DJOB_OJT_RUNNING);
	task_to_find->set_location(TASK_LOC_PENDING);

	const void *task_to_find__serialization;
	size_t task_to_find__serialization_size;
	task_to_find->get_serialized_task(task_to_find__serialization,
	    task_to_find__serialization_size);

	add_sbt_entry_ut(pending_sbt_fds[task_to_find__priority],
	    task_to_find->get_key(), task_to_find__serialization,
	    task_to_find__serialization_size);

	/*
	 * We don't need to track the values set on task_to_not_find; we don't
	 * expect to find it and therefore won't need to compare what we added
	 * with what we found.
	 */
	isi_cloud_object_id task_to_not_find__object_id(123456789, 567891234);
	task_to_not_find__object_id.set_snapid(246);
	task_to_not_find->set_cloud_object_id(task_to_not_find__object_id);
	task_to_not_find->set_priority(4);

	/*
	 * With both tasks created and only one added to the SBT, search for
	 * each.
	 */

	/* false: don't ignore ENOENT */
	retrieved_task = tm.find_task_pending(task_to_find->get_key(), false,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_cloud_gc_task =
	    static_cast<isi_cloud::cloud_gc_task *>(retrieved_task);
	fail_if(retrieved_cloud_gc_task == NULL);

	fail_if(retrieved_cloud_gc_task->get_cloud_object_id() !=
	    task_to_find__object_id,
	    "object ID mismatch (exp: %s/%d act: %s/%d)",
	    task_to_find__object_id.to_c_string(),
	    task_to_find__object_id.get_snapid(),
	    retrieved_cloud_gc_task->get_cloud_object_id().to_c_string(),
	    retrieved_cloud_gc_task->get_cloud_object_id().get_snapid());
	fail_if(retrieved_cloud_gc_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_cloud_gc_task->get_priority());

	delete retrieved_task;

	/* true: ignore ENOENT (included for the sake of completeness) */
	retrieved_task = tm.find_task_pending(task_to_find->get_key(), true,
	    &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_cloud_gc_task =
	    static_cast<isi_cloud::cloud_gc_task *>(retrieved_task);
	fail_if(retrieved_cloud_gc_task == NULL);

	fail_if(retrieved_cloud_gc_task->get_cloud_object_id() !=
	    task_to_find__object_id,
	    "object ID mismatch (exp: %s/%d act: %s/%d)",
	    task_to_find__object_id.to_c_string(),
	    task_to_find__object_id.get_snapid(),
	    retrieved_cloud_gc_task->get_cloud_object_id().to_c_string(),
	    retrieved_cloud_gc_task->get_cloud_object_id().get_snapid());
	fail_if(retrieved_cloud_gc_task->get_priority() !=
	    task_to_find__priority, "priority mismatch (exp: %d act: %d)",
	    task_to_find__priority, retrieved_cloud_gc_task->get_priority());

	delete retrieved_task;

	delete task_to_find;

	/*
	 * Try to retrieve the not-added task without ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_pending(task_to_not_find->get_key(),
	    false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * Try to retrieve the not-added task again, this time ignoring ENOENT.
	 */
	retrieved_task = tm.find_task_pending(task_to_not_find->get_key(),
	    true, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task_to_not_find: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	delete task_to_not_find;

	for (int i = 0; i < num_priorities; ++i)
		close_sbt(pending_sbt_fds[i]);
}

/*
 * 132 bytes "leaked" from the static cpool_fd_store instance.
 */
TEST(test_find_task_recall, CK_MEM_LEAKS, 132)
{
	struct isi_error *error = NULL;

	const cpool_operation_type *op_type = OT_RECALL;
	const int num_priorities = 32;
	int inprogress_sbt_fd;
	int pending_sbt_fds[num_priorities] = { 0 };
	isi_cloud::task *retrieved_task = NULL;
	isi_cloud::recall_task *retrieved_recall_task = NULL;

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

	task_map_ut tm(op_type);

	/* Create a task to be added to the in-progress SBT. */
	isi_cloud::recall_task *inprog_task = new isi_cloud::recall_task;
	ASSERT(inprog_task != NULL);

	ifs_lin_t inprog_task__lin = 235689;
	inprog_task->set_lin(inprog_task__lin);

	ifs_snapid_t inprog_task__snapid = 134679;
	inprog_task->set_snapid(inprog_task__snapid);

	bool inprog_task__retain_stub = false;
	inprog_task->set_retain_stub(inprog_task__retain_stub);

	int inprog_task__priority = 30;
	inprog_task->set_priority(inprog_task__priority);

	inprog_task->set_state(DJOB_OJT_RUNNING);
	inprog_task->set_location(TASK_LOC_INPROGRESS);

	const void *inprog_task__serialization;
	size_t inprog_task__serialization_size;
	inprog_task->get_serialized_task(inprog_task__serialization,
	    inprog_task__serialization_size);

	add_sbt_entry_ut(inprogress_sbt_fd, inprog_task->get_key(),
	    inprog_task__serialization, inprog_task__serialization_size);

	/* Create a task to be added to a pending SBT. */
	isi_cloud::recall_task *pend_task = new isi_cloud::recall_task;
	ASSERT(pend_task != NULL);

	ifs_lin_t pend_task__lin = 13243546;
	pend_task->set_lin(pend_task__lin);

	ifs_snapid_t pend_task__snapid = 143658;
	pend_task->set_snapid(pend_task__snapid);

	bool pend_task__retain_stub = true;
	pend_task->set_retain_stub(pend_task__retain_stub);

	int pend_task__priority = 25;
	pend_task->set_priority(pend_task__priority);

	pend_task->set_state(DJOB_OJT_RUNNING);
	pend_task->set_location(TASK_LOC_PENDING);

	const void *pend_task__serialization;
	size_t pend_task__serialization_size;
	pend_task->get_serialized_task(pend_task__serialization,
	    pend_task__serialization_size);

	add_sbt_entry_ut(pending_sbt_fds[pend_task->get_priority()],
	    pend_task->get_key(), pend_task__serialization,
	    pend_task__serialization_size);

	/* Create a task to not be added to any SBT. */
	isi_cloud::recall_task *none_task = new isi_cloud::recall_task;
	ASSERT(none_task != NULL);

	none_task->set_lin(42);
	none_task->set_snapid(1098);
	none_task->set_retain_stub(false);
	none_task->set_priority(14);

	const void *none_task__serialization;
	size_t none_task__serialization_size;
	none_task->get_serialized_task(none_task__serialization,
	    none_task__serialization_size);

	/*
	 * Attempt to retrieve each of the three tasks, both while ignoring and
	 * not ignoring ENOENT when we don't expect the task to be found.
	 * First, the one from the pending SBT.
	 */
	retrieved_task = tm.find_task(pend_task->get_key(), false, &error);
	fail_if(error != NULL,
	    "unexpected error when searching for task in pending SBT: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_recall_task =
	    static_cast<isi_cloud::recall_task *>(retrieved_task);
	fail_if(retrieved_recall_task == NULL);

	fail_if(retrieved_recall_task->get_lin() != pend_task__lin,
	    "lin mismatch (exp: %{} act: %{})", pend_task__lin,
	    retrieved_recall_task->get_lin());
	fail_if(retrieved_recall_task->get_snapid() != pend_task__snapid,
	    "snapid mismatch (exp: %{} act: %{})", pend_task__snapid,
	    retrieved_recall_task->get_snapid());
	fail_if(retrieved_recall_task->get_retain_stub() !=
	    pend_task__retain_stub,
	    "retain_stub mismatch (exp: %s act: %s)",
	    pend_task__retain_stub ? "true" : "false",
	    retrieved_recall_task->get_retain_stub() ? "true" : "false");
	fail_if(retrieved_recall_task->get_priority() != pend_task__priority,
	    "priority mismatch (exp: %d act: %d)", pend_task__priority,
	    retrieved_recall_task->get_priority());

	delete retrieved_recall_task;

	/*
	 * Next, the task that was not added to any SBT, both ignoring and not
	 * ignoring ENOENT.
	 */
	retrieved_task = tm.find_task(none_task->get_key(), false, &error);
	fail_if(error == NULL);
	fail_if(!isi_system_error_is_a(error, ENOENT),
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	isi_error_free(error);
	error = NULL;

	retrieved_task = tm.find_task(none_task->get_key(), true, &error);
	fail_if(error != NULL, "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(retrieved_task != NULL);

	/* Finally, the task that was added to the in-progress SBT. */
	retrieved_task = tm.find_task(inprog_task->get_key(), false, &error);
	fail_if(error != NULL, "unexpected error "
	    "when searching for task in in-progress SBT: %#{}",
	    isi_error_fmt(error));
	fail_if(retrieved_task == NULL);

	retrieved_recall_task =
	    static_cast<isi_cloud::recall_task *>(retrieved_task);
	fail_if(retrieved_recall_task == NULL);

	fail_if(retrieved_recall_task->get_lin() != inprog_task__lin,
	    "lin mismatch (exp: %{} act: %{})", inprog_task__lin,
	    retrieved_recall_task->get_lin());
	fail_if(retrieved_recall_task->get_snapid() != inprog_task__snapid,
	    "snapid mismatch (exp: %{} act: %{})", inprog_task__snapid,
	    retrieved_recall_task->get_snapid());
	fail_if(retrieved_recall_task->get_retain_stub() !=
	    inprog_task__retain_stub,
	    "retain_stub mismatch (exp: %s act: %s)",
	    inprog_task__retain_stub ? "true" : "false",
	    retrieved_recall_task->get_retain_stub() ? "true" : "false");
	fail_if(retrieved_recall_task->get_priority() != inprog_task__priority,
	    "priority mismatch (exp: %d act: %d)", inprog_task__priority,
	    retrieved_recall_task->get_priority());

	delete retrieved_recall_task;

	delete inprog_task;
	delete pend_task;
	delete none_task;

	close_sbt(inprogress_sbt_fd);
	for (int i = 0; i < num_priorities; ++i)
		close_sbt(pending_sbt_fds[i]);
}
