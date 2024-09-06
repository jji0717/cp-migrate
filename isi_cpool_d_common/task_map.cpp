#include <isi_sbtree/sbtree.h>

#include "cpool_d_debug.h"
#include "cpool_fd_store.h"
#include "hsbt.h"
#include "ifs_cpool_flock.h"
#include "task.h"

#include "task_map.h"

task_map::task_map(const cpool_operation_type *type)
	: op_type_(type), uses_hsbt_(false)
{
	ASSERT(op_type_ != NULL);

	/* We only allow OT_NONE because of query_job_for_members(). */
	if (op_type_ != OT_NONE)
		uses_hsbt_ = op_type_->uses_hsbt();
}

task_map::~task_map()
{
}

const cpool_operation_type *
task_map::get_op_type(void) const
{
	return op_type_;
}

void
task_map::set_op_type(const cpool_operation_type *op_type)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);

	op_type_ = op_type;
	uses_hsbt_ = op_type->uses_hsbt();
}

void
task_map::lock_task(const task_key *key, bool block, bool exclusive,
    struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	int sbt_fd, result;
	struct flock params;

	sbt_fd = cpool_fd_store::getInstance().get_move_lock_fd(
	    key->get_op_type()->get_type(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	populate_flock_params(key->get_identifier(), &params);
	if (!exclusive)
		params.l_type = CPOOL_LK_SR;

	result = ifs_cpool_flock(sbt_fd, LOCK_DOMAIN_CPOOL_JOBS, F_SETLK,
	    &params, block ? F_SETLKW : 0);
	if (result != 0) {
		error = isi_system_error_new(errno,
		    "failed to lock task (key: %{})", cpool_task_key_fmt(key));
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
task_map::unlock_task(const task_key *key, struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	int sbt_fd, result;
	struct flock params;

	sbt_fd = cpool_fd_store::getInstance().get_move_lock_fd(
	    key->get_op_type()->get_type(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	populate_flock_params(key->get_identifier(), &params);

	result = ifs_cpool_flock(sbt_fd, LOCK_DOMAIN_CPOOL_JOBS, F_UNLCK,
	    &params, 0);
	if (result != 0) {
		error = isi_system_error_new(errno,
		    "failed to unlock task (key: %{})",
		    cpool_task_key_fmt(key));
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

bool
task_map::claim_task(const isi_cloud::task *task,
    struct isi_error **error_out) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;
	bool task_claimed = false;
	int sbt_fd, result;
	struct flock params;

	populate_flock_params(task->get_identifier(), &params);

	sbt_fd = cpool_fd_store::getInstance().get_lock_fd(
	    task->get_op_type()->get_type(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	task_claimed = true; /* temporarily... */
	result = ifs_cpool_flock(sbt_fd, LOCK_DOMAIN_CPOOL_JOBS, F_SETLK,
	    &params, 0);
	if (result != 0) {
		/*
		 * Ignore EWOULDBLOCK.  In either normal or recovery mode, it
		 * just means that another thread has claimed this task, and
		 * the return value will reflect that this thread didn't "win"
		 * the claim.
		 */
		if (errno != EWOULDBLOCK)
			error = isi_system_error_new(errno,
			    "failed to claim task %{}", cpool_task_fmt(task));

		task_claimed = false;
		goto out;
	}

 out:
	isi_error_handle(error, error_out);

	return task_claimed;
}

bool
task_map::relinquish_task(const isi_cloud::task *task,
    struct isi_error **error_out) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;
	bool task_relinquished = false;
	int sbt_fd, result;
	struct flock params;

	sbt_fd = cpool_fd_store::getInstance().get_lock_fd(
	    task->get_op_type()->get_type(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	populate_flock_params(task->get_identifier(), &params);

	task_relinquished = true; /* temporarily... */
	result = ifs_cpool_flock(sbt_fd, LOCK_DOMAIN_CPOOL_JOBS, F_UNLCK,
	    &params, 0);
	if (result != 0) {
// if lock was not taken, is that an error?  we don't want it to be, or we need
// to specifically designate this fact
		task_relinquished = false;
		error = isi_system_error_new(errno,
		    "failed to relinquish claim on task %{}",
		    cpool_task_fmt(task));
		goto out;
	}

 out:
	isi_error_handle(error, error_out);

	return task_relinquished;
}

isi_cloud::task *
task_map::find_task(const task_key *key, bool ignore_ENOENT,
    struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	isi_cloud::task *found_task = NULL;

	/*
	 * For these "intermediate" find_task_* calls, ignore ENOENT,
	 * regardless of the incoming ignore_ENOENT value.
	 */
	found_task = find_task_inprogress(key, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	if (found_task != NULL)
		goto out;

	found_task = find_task_pending(key, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	if (found_task != NULL)
		goto out;

	if (!ignore_ENOENT) {
		error = isi_system_error_new(ENOENT,
		    "task with key %{} does not exist in task map",
		    cpool_task_key_fmt(key));
	}

 out:
	/*
	 * If we're ignoring ENOENT, ensure we are not returning an error and a
	 * task, otherwise ensure we're returning an error or a task but not
	 * both (i.e. XOR).  Do this before isi_error_handle since error will
	 * always be NULL after calling that function.
	 */
	if (ignore_ENOENT)
		ASSERT(found_task == NULL || error == NULL);
	else
		ASSERT((found_task == NULL) != (error == NULL));

	isi_error_handle(error, error_out);

	return found_task;
}

isi_cloud::task *
task_map::find_task_pending(const task_key *key, bool ignore_ENOENT,
    struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	isi_cloud::task *found_task = NULL;
	int priority, pending_sbt_fd;
	void *serialized_task = NULL;
	size_t serialized_task_size;

	for (priority = 0; found_task == NULL; ++priority) {
		pending_sbt_fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type_->get_type(), priority, false,
		    &error);
		if (error != NULL) {
			if (isi_system_error_is_a(error, ENOENT))
				/* We've run out of SBTs in which to look. */
				break;

			isi_error_add_context(error);
			goto out;
		}

		read_entry_from_sbt(pending_sbt_fd, key, serialized_task,
		    serialized_task_size, &error);
		if (error != NULL) {
			if (isi_system_error_is_a(error, ENOENT)) {
				/*
				 * Didn't find it in this priority's pending
				 * SBT, try the next one.
				 */
				isi_error_free(error);
				error = NULL;
				continue;
			}

			isi_error_add_context(error);
			goto out;
		}

		found_task =
		    isi_cloud::task::create_task(op_type_);
		ASSERT(found_task != NULL);

		found_task->set_serialized_task(serialized_task,
		    serialized_task_size, &error);
		if (error != NULL) {
			delete found_task;
			found_task = NULL;

			ON_ISI_ERROR_GOTO(out, error);
		}

		ASSERT(found_task->get_location() == TASK_LOC_PENDING,
		    "invalid location (%{}) for task (%{})",
		    task_location_fmt(found_task->get_location()),
		    cpool_task_fmt(found_task));
	}

 out:
	free(serialized_task);

	/*
	 * Ensure we have either a task or an error, but not both.  Check this
	 * before possibly freeing the error because of ignore_ENOENT.
	 */
	ASSERT((found_task == NULL) != (error == NULL));

	if (error != NULL && isi_system_error_is_a(error, ENOENT) &&
	    ignore_ENOENT) {
		isi_error_free(error);
		error = NULL;
	}

	isi_error_handle(error, error_out);

	return found_task;
}

isi_cloud::task *
task_map::find_task_inprogress(const task_key *key, bool ignore_ENOENT,
    struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	isi_cloud::task *found_task = NULL;
	int inprogress_sbt_fd;
	void *serialized_task = NULL;
	size_t serialized_task_size;

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type_->get_type(), false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	read_entry_from_sbt(inprogress_sbt_fd, key, serialized_task,
	    serialized_task_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

	found_task = isi_cloud::task::create_task(op_type_);
	ASSERT(found_task != NULL);

	found_task->set_serialized_task(serialized_task, serialized_task_size,
	    &error);
	if (error != NULL) {
		delete found_task;
		found_task = NULL;

		ON_ISI_ERROR_GOTO(out, error);
	}

	ASSERT(found_task->get_location() == TASK_LOC_INPROGRESS,
	    "invalid location (%{}) for task (%{})",
	    task_location_fmt(found_task->get_location()),
	    cpool_task_fmt(found_task));

 out:
	free(serialized_task);

	/*
	 * Ensure we have either a task or an error, but not both.  Check this
	 * before possibly freeing the error because of ignore_ENOENT.
	 */
	ASSERT((found_task == NULL) != (error == NULL));

	if (error != NULL && isi_system_error_is_a(error, ENOENT) &&
	    ignore_ENOENT) {
		isi_error_free(error);
		error = NULL;
	}

	isi_error_handle(error, error_out);

	return found_task;
}

void
task_map::read_entry_from_sbt(int sbt_fd, const task_key *key,
    void *&entry_buf, size_t &entry_size, struct isi_error **error_out) const
{
	ASSERT(key != NULL);
	ASSERT(entry_buf == NULL);

	struct isi_error *error = NULL;
	bool found;
	int result;

	/* Assumes SBT entries cannot exceed 8K. */
	const int max_sbt_entry_size = 8192;
	entry_buf = malloc(max_sbt_entry_size);
	ASSERT(entry_buf != NULL);
	entry_size = max_sbt_entry_size;

	const void *serialized_key;
	size_t serialized_key_size;
	key->get_serialized_key(serialized_key, serialized_key_size);

	if (uses_hsbt_) {
		found = hsbt_get_entry(sbt_fd, serialized_key,
		    serialized_key_size, entry_buf, &entry_size, NULL, &error);
		if (error == NULL && !found)
			error = isi_system_error_new(ENOENT,
			    "entry does not exist in hashed sbt");
		ON_ISI_ERROR_GOTO(err, error);
	}
	else {
		ASSERT(serialized_key_size == sizeof(btree_key_t),
		    "%zu != %zu", serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		result = ifs_sbt_get_entry_at(sbt_fd,
		    const_cast<btree_key_t *>(bt_key), entry_buf,
		    max_sbt_entry_size, NULL, &entry_size);
		if (result != 0) {
			error = isi_system_error_new(errno,
			    "ifs_sbt_get_entry_at failed");
			goto err;
		}
	}

	goto out;

 err:
	free(entry_buf);
	entry_buf = NULL;
	entry_size = 0;

 out:
	isi_error_handle(error, error_out);
}
