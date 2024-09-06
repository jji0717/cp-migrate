#include <ifs/ifs_syscalls.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_configuration.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_member.h>
#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/scoped_object_list.h>
#include <isi_cpool_d_common/task.h>

#include "client_task_map.h"

using namespace isi_cloud;

client_task_map::client_task_map(const cpool_operation_type *op_type)
	: task_map(op_type)
{
}

client_task_map::~client_task_map()
{
}

void
client_task_map::add_task(isi_cloud::task *task, djob_id_t daemon_job_id,
    const char *filename, struct isi_error **error_out) const
{
	ASSERT(task != NULL);
	ASSERT(task->get_op_type() != NULL);
	ASSERT(task->get_op_type() == op_type_,
	    "task op_type (%{}) doesn't match task_map op_type (%{})",
	    task_type_fmt(task->get_op_type()->get_type()),
	    task_type_fmt(op_type_->get_type()));
	ASSERT(daemon_job_id != DJOB_RID_INVALID);

	struct isi_error *error = NULL;
	isi_cloud::task *retrieved_task = NULL;
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	/* true: block / true: exclusive */
	lock_task(task->get_key(), true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	objects_to_delete.add_locked_task(task);

	/* true: don't return ENOENT as an error */
	retrieved_task = find_task(task->get_key(), true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	if (retrieved_task != NULL)
		objects_to_delete.add_task(retrieved_task);

	if (retrieved_task != NULL)
		add_task_helper__existing(task, retrieved_task, daemon_job_id,
		    filename, sbt_ops, objects_to_delete, &error);
	else
		add_task_helper__nonexisting(task, daemon_job_id, filename,
		    sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * It's OK to use hsbt_bulk_op here, even if we aren't interacting with
	 * any hashed SBTs - hsbt_bulk_op doesn't alter non-hashed SBT bulk
	 * entries.
	 */
	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
client_task_map::add_task_helper__existing(const isi_cloud::task *new_task,
    isi_cloud::task *existing_task, djob_id_t daemon_job_id,
    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(existing_task != NULL);
	ASSERT(existing_task->get_location() == TASK_LOC_PENDING ||
	    existing_task->get_location() == TASK_LOC_INPROGRESS,
	    "illegal location value (%{}) for task (%{})",
	    task_location_fmt(existing_task->get_location()),
	    cpool_task_fmt(existing_task));

	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = existing_task->get_op_type();

	existing_task->add_daemon_job(daemon_job_id, &error);
	if (error != NULL) {
		if (isi_system_error_is_a(error, EEXIST) &&
		    !op_type->is_multi_job()) {
			/*
			 * Adding an existing daemon job ID to a non-multi-job
			 * operation is OK -- for example, multiple updates to
			 * a stubbed file can yield multiple writeback tasks --
			 * but we don't need to do anything further.
			 */
			isi_error_free(error);
			error = NULL;

			goto out;
		}

		ON_ISI_ERROR_GOTO(out, error);
	}

	existing_task->save_serialization();

	/* Add operations to manipulate pending / in-progress SBTs. */
	_add_task_helper__existing(new_task, existing_task, daemon_job_id,
	    filename, sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Add daemon-job-manager-related SBT operations. */
	daemon_job_manager::add_task(existing_task, filename, daemon_job_id,
	    sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
client_task_map::add_task_helper__nonexisting(isi_cloud::task *new_task,
    djob_id_t daemon_job_id, const char *filename,
    std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(new_task != NULL);

	struct isi_error *error = NULL;

	new_task->add_daemon_job(daemon_job_id, &error);
	ON_ISI_ERROR_GOTO(out, error);

	new_task->set_location(TASK_LOC_PENDING);

	/* Add operations to manipulate pending SBT. */
	_add_task_helper__nonexisting(new_task, daemon_job_id, filename,
	    sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Add daemon-job-manager-related SBT operations. */
	daemon_job_manager::add_task(new_task, filename, daemon_job_id,
	    sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
client_task_map::_add_task_helper__existing(const isi_cloud::task *new_task,
    isi_cloud::task *existing_task, djob_id_t daemon_job_id,
    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(new_task != NULL);
	ASSERT(existing_task != NULL);

	struct isi_error *error = NULL;
	int src_sbt_fd, dst_sbt_fd;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	const void *serialized_key;
	size_t serialized_key_size;
	struct hsbt_bulk_entry sbt_op;

	/* Sanity check - these tasks should have identical keys. */
	ASSERT(new_task->get_key()->equals(existing_task->get_key()),
	    "key mismatch -- new: %{} existing: %{}",
	    cpool_task_fmt(new_task), cpool_task_fmt(existing_task));

	/*
	 * No more than five SBT operations are needed to add a task that
	 * currently exists in a task map:
	 *  - exactly one of:
	 *     - remove from lower priority SBT *AND* add to higher priority
	 *       SBT
	 *     - modify existing SBT entry
	 *  - update daemon job stats
	 *  - update daemon job SBT entry for given daemon job ID
	 *  - add to daemon job member list
	 */

	/* Retrieve the source SBT file descriptor. */
	if (existing_task->get_location() == TASK_LOC_PENDING)
		src_sbt_fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type_->get_type(),
		    existing_task->get_priority(), true, &error);
	else // existing_task->get_location() == TASK_LOC_INPROGRESS
		src_sbt_fd = cpool_fd_store::getInstance().\
		    get_inprogress_sbt_fd(op_type_->get_type(), true, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to retrieve source file descriptor");

	/*
	 * If the priority of the existing task is lower than that of the new
	 * task, "promote" the existing task.  This is still applicable for
	 * tasks that are in-progress; should the in-progress task fail it
	 * should be moved back to the appropriate priority's pending SBT.
	 */
	if (existing_task->get_priority() < new_task->get_priority())
		existing_task->set_priority(new_task->get_priority());

	/* Retrieve the destination SBT file descriptor. */
	if (existing_task->get_location() == TASK_LOC_PENDING)
		dst_sbt_fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(op_type_->get_type(),
		    existing_task->get_priority(), true, &error);
	else // existing_task->get_location() == TASK_LOC_INPROGRESS
		dst_sbt_fd = cpool_fd_store::getInstance().\
		    get_inprogress_sbt_fd(op_type_->get_type(), true, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to retrieve destination file descriptor");

	existing_task->get_saved_serialization(old_serialization,
	    old_serialization_size);
	existing_task->get_serialized_task(new_serialization,
	    new_serialization_size);
	existing_task->get_key()->get_serialized_key(serialized_key,
	    serialized_key_size);

	if (src_sbt_fd == dst_sbt_fd) {
		/* Modify entry in place. */
		memset(&sbt_op, 0, sizeof sbt_op);

		sbt_op.is_hash_btree = uses_hsbt_;
		sbt_op.bulk_entry.fd = src_sbt_fd;
		sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
		sbt_op.bulk_entry.cm = BT_CM_BUF;
		sbt_op.bulk_entry.old_entry_buf =
		    const_cast<void *>(old_serialization);
		sbt_op.bulk_entry.old_entry_size = old_serialization_size;
		sbt_op.bulk_entry.entry_buf =
		    const_cast<void *>(new_serialization);
		sbt_op.bulk_entry.entry_size = new_serialization_size;

		if (uses_hsbt_) {
			sbt_op.key = serialized_key;
			sbt_op.key_len = serialized_key_size;
		}
		else {
			ASSERT(serialized_key_size == sizeof(btree_key_t),
			    "%zu != %zu", serialized_key_size,
			    sizeof(btree_key_t));

			const btree_key_t *bt_key =
			    static_cast<const btree_key_t *>(serialized_key);
			ASSERT(bt_key != NULL);

			sbt_op.bulk_entry.key = *bt_key;
		}

		sbt_ops.push_back(sbt_op);
	}
	else {
		/* Remove from lower priority ... */
		memset(&sbt_op, 0, sizeof sbt_op);

		sbt_op.is_hash_btree = uses_hsbt_;
		sbt_op.bulk_entry.fd = src_sbt_fd;
		sbt_op.bulk_entry.op_type = SBT_SYS_OP_DEL;
		sbt_op.bulk_entry.cm = BT_CM_BUF;
		sbt_op.bulk_entry.old_entry_buf =
		    const_cast<void *>(old_serialization);
		sbt_op.bulk_entry.old_entry_size = old_serialization_size;

		if (uses_hsbt_) {
			sbt_op.key = serialized_key;
			sbt_op.key_len = serialized_key_size;
		}
		else {
			ASSERT(serialized_key_size == sizeof(btree_key_t),
			    "%zu != %zu", serialized_key_size,
			    sizeof(btree_key_t));

			const btree_key_t *bt_key =
			    static_cast<const btree_key_t *>(serialized_key);
			ASSERT(bt_key != NULL);

			sbt_op.bulk_entry.key = *bt_key;
		}

		sbt_ops.push_back(sbt_op);

		/* ... and add to higher priority. */
		memset(&sbt_op, 0, sizeof sbt_op);

		sbt_op.is_hash_btree = uses_hsbt_;
		sbt_op.bulk_entry.fd = dst_sbt_fd;
		sbt_op.bulk_entry.op_type = SBT_SYS_OP_ADD;
		sbt_op.bulk_entry.entry_buf =
		    const_cast<void *>(new_serialization);
		sbt_op.bulk_entry.entry_size = new_serialization_size;

		if (uses_hsbt_) {
			sbt_op.key = serialized_key;
			sbt_op.key_len = serialized_key_size;
		}
		else {
			ASSERT(serialized_key_size == sizeof(btree_key_t),
			    "%zu != %zu", serialized_key_size,
			    sizeof(btree_key_t));

			const btree_key_t *bt_key =
			    static_cast<const btree_key_t *>(serialized_key);
			ASSERT(bt_key != NULL);

			sbt_op.bulk_entry.key = *bt_key;
		}

		sbt_ops.push_back(sbt_op);
	}

 out:
	isi_error_handle(error, error_out);
}

void
client_task_map::_add_task_helper__nonexisting(isi_cloud::task *new_task,
    djob_id_t daemon_job_id, const char *filename,
    std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(new_task != NULL);
	ASSERT(new_task->get_location() == TASK_LOC_PENDING,
	    "illegal location (%{}) for task (%{})",
	    task_location_fmt(new_task->get_location()),
	    cpool_task_fmt(new_task));

	struct isi_error *error = NULL;
	const void *serialized_task;
	size_t serialized_task_size;
	const void *serialized_key;
	size_t serialized_key_size;
	int sbt_fd;
	struct hsbt_bulk_entry sbt_op;

	new_task->set_state(DJOB_OJT_RUNNING);

	new_task->get_serialized_task(serialized_task, serialized_task_size);
	new_task->get_key()->get_serialized_key(serialized_key,
	    serialized_key_size);

	sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type_->get_type(),
	    new_task->get_priority(), true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Add the task to the appropriate SBT. */
	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = sbt_fd;
	sbt_op.bulk_entry.op_type = SBT_SYS_OP_ADD;
	sbt_op.bulk_entry.entry_buf = const_cast<void *>(serialized_task);
	sbt_op.bulk_entry.entry_size = serialized_task_size;

	if (uses_hsbt_) {
		sbt_op.key = serialized_key;
		sbt_op.key_len = serialized_key_size;
	}
	else {
		ASSERT(serialized_key_size == sizeof(btree_key_t),
		    "%zu != %zu", serialized_key_size,
		    sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		sbt_op.bulk_entry.key = *bt_key;
	}

	sbt_ops.push_back(sbt_op);

 out:
	isi_error_handle(error, error_out);
}

#ifdef PRC_TASK
void
client_task_map::prc_task(const task_key *key,
    djob_op_job_task_state new_state, struct isi_error **error_out) const
{
	ASSERT(key != NULL);
	ASSERT(key->get_op_type() == op_type_,
	    "operation type mismatch (%{} != %{})",
	    task_type_fmt(key->get_op_type()->get_type()),
	    task_type_fmt(op_type_->get_type()));
	ASSERT(new_state == DJOB_OJT_PAUSED || new_state == DJOB_OJT_RUNNING ||
	    new_state == DJOB_OJT_CANCELLED, "invalid state: %{}",
	    djob_op_job_task_state_fmt(new_state));

	struct isi_error *error = NULL;
	bool task_locked = false;
	isi_cloud::task *task = NULL;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	const void *serialized_key;
	size_t serialized_key_size;
	int sbt_fd;
	struct hsbt_bulk_entry sbt_op;
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	/* true: block / true: exclusive lock */
	lock_task(key, true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	/* false: return ENOENT as error if necessary */
	task = find_task(key, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	task->save_serialization();

	/* Validate state change. */
	if (new_state == DJOB_OJT_PAUSED) {
		if (task->get_state() != DJOB_OJT_RUNNING)
			error = isi_system_error_new(EINVAL,
			    "invalid state change requested (from %{} to %{})",
			    djob_op_job_task_state_fmt(task->get_state()),
			    djob_op_job_task_state_fmt(new_state));
	}
	else if (new_state == DJOB_OJT_RUNNING) {
		if (task->get_state() != DJOB_OJT_PAUSED)
			error = isi_system_error_new(EINVAL,
			    "invalid state change requested (from %{} to %{})",
			    djob_op_job_task_state_fmt(task->get_state()),
			    djob_op_job_task_state_fmt(new_state));
	}
	else { // if (new_state == DJOB_OJT_CANCELLED), see ASSERT above
		if (task->get_state() == DJOB_OJT_CANCELLED)
			error = isi_system_error_new(EINVAL,
			    "invalid state change requested (from %{} to %{})",
			    djob_op_job_task_state_fmt(task->get_state()),
			    djob_op_job_task_state_fmt(new_state));
	}
	ON_ISI_ERROR_GOTO(out, error);
// ^ what about other states that the task may be in?

	/*
	 * Change the state of the task.  If cancelling a task, there is the
	 * potential for completing a job.
	 */
	task->set_state(new_state);
// ^ should above logic be in this function?

	if (task->get_location() == TASK_LOC_STAGED) {
		sbt_fd = cpool_fd_store::getInstance().\
		    get_staging_sbt_fd(key->get_op_type()->get_type(),
		    task->get_priority(), false, &error);
	}
	else if (task->get_location() == TASK_LOC_PENDING) {
		sbt_fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(key->get_op_type()->get_type(),
		    task->get_priority(), false, &error);
	}
	else { // task->get_location() == TASK_LOC_INPROGRESS
		sbt_fd = cpool_fd_store::getInstance().\
		    get_inprogress_sbt_fd(key->get_op_type()->get_type(),
		    false, &error);
	}
	ON_ISI_ERROR_GOTO(out, error, "task location: %{}",
	    task_location_fmt(task->get_location()));

	task->get_saved_serialization(old_serialization,
	    old_serialization_size);
	task->get_serialized_task(new_serialization, new_serialization_size);
	key->get_serialized_key(serialized_key, serialized_key_size);

	/* Modify the task in place. */
	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = sbt_fd;
	sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
	sbt_op.bulk_entry.cm = BT_CM_BUF;
	sbt_op.bulk_entry.entry_buf = const_cast<void *>(new_serialization);
	sbt_op.bulk_entry.entry_size = new_serialization_size;
	sbt_op.bulk_entry.old_entry_buf =
	    const_cast<void *>(old_serialization);
	sbt_op.bulk_entry.old_entry_size = old_serialization_size;

	if (uses_hsbt_) {
		sbt_op.key = serialized_key;
		sbt_op.key_len = serialized_key_size;
	}
	else {
		ASSERT(serialized_key_size == sizeof(btree_key_t),
		    "expected btree_key_t (size: %zu != %zu)",
		    serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		sbt_op.bulk_entry.key = *bt_key;
	}

	sbt_ops.push_back(sbt_op);

	if (new_state == DJOB_OJT_PAUSED) {
		isi_cloud::daemon_job_manager::pause_task(task, sbt_ops,
		    objects_to_delete, &error);
	}
	else if (new_state == DJOB_OJT_RUNNING) {
		isi_cloud::daemon_job_manager::resume_task(task, sbt_ops,
		    objects_to_delete, &error);
	}
	else { // new_state == DJOB_OJT_CANCELLED
		isi_cloud::daemon_job_manager::cancel_task(task, sbt_ops,
		    objects_to_delete, &error);
	}
	ON_ISI_ERROR_GOTO(out, error, "new state: %{}",
	    djob_op_job_task_state_fmt(new_state));

	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (task_locked)
		unlock_task(key, isi_error_suppress(IL_NOTICE));

	if (task != NULL)
		delete task;

	isi_error_handle(error, error_out);
}
#endif // PRC_TASK

void
client_task_map::query_job_for_members(djob_id_t daemon_job_id,
    unsigned int start, resume_key &start_key, unsigned int max,
    std::vector<struct job_member *> &job_members, struct isi_error **error_out)
{
	ASSERT(daemon_job_id != DJOB_RID_INVALID);

	struct isi_error *error = NULL;
	std::vector<daemon_job_member *> members;
	std::vector<daemon_job_member *>::const_iterator members_iter;
	const daemon_job_member *current_member = NULL;
	bool task_locked = false;
	isi_cloud::task *current_task = NULL;

	isi_cloud::daemon_job_manager::query_job_for_members(daemon_job_id,
	    start, start_key, max, members, get_op_type(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	members_iter = members.begin();
	for (; members_iter != members.end(); ++members_iter) {
		current_member = *members_iter;

		/* true: block / false: shared lock */
		lock_task(current_member->get_key(), true, false, &error);
		ON_ISI_ERROR_GOTO(out, error);
		task_locked = true;

		djob_op_job_task_state current_task_state =
		    current_member->get_final_state();

		/*
		 * If the member doesn't have a final state yet, we query
		 * daemon_job_member to calculate a state based on the states
		 * of parent jobs
		 */
		if (current_task_state == DJOB_OJT_NONE) {
			/* true: ignore ENOENT */
			current_task = find_task(current_member->get_key(),
			    true, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (current_task != NULL) {
				current_task_state =
				    isi_cloud::daemon_job_manager::get_state(
				    current_task, &error);
				ON_ISI_ERROR_GOTO(out, error);

				/*
				 * If a task has a state of DJOB_OJT_RUNNING,
				 * check its location to determine if it is
				 * currently being processed.
				 */
				if (current_task_state == DJOB_OJT_RUNNING &&
				    current_task->get_location() ==
					TASK_LOC_PENDING)
					current_task_state = DJOB_OJT_PENDING;
			}
		}

		struct job_member *new_job_member = new struct job_member;

		struct fmt FMT_INIT_CLEAN(key_fmt);
		fmt_print(&key_fmt, "%{}",
		    cpool_task_key_fmt(current_member->get_key()));
		new_job_member->key_ = fmt_detach(&key_fmt);

		new_job_member->name_ = NULL;
		if (current_member->get_name() != NULL)
			new_job_member->name_ =
			    strdup(current_member->get_name());

		new_job_member->state_ = current_task_state;

		job_members.push_back(new_job_member);

		if (current_task != NULL) {
			delete current_task;
			current_task = NULL;
		}

		unlock_task(current_member->get_key(), &error);
		ON_ISI_ERROR_GOTO(out, error);
		task_locked = false;

		current_member = NULL;
	}

 out:
	if (current_task != NULL)
		delete current_task;

	if (task_locked)
		unlock_task(current_member->get_key(),
		    isi_error_suppress(IL_NOTICE));

	std::vector<daemon_job_member *>::iterator iter = members.begin();
	for (; iter != members.end(); ++iter)
		delete *iter;

	/*
	 * If error is non-NULL, the cpool API will delete any pointers in
	 * job_members.
	 */

	isi_error_handle(error, error_out);
}

unsigned int
client_task_map::get_incomplete_task_count(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	unsigned int count = 0;
	int priority = 0;
	int sbt_fd;

	sbt_fd = cpool_fd_store::getInstance().get_inprogress_sbt_fd(
	    op_type_->get_type(), false, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to retrieve in-progress SBT file descriptor");

	// true ==> only count tasks marked 'done'
	count = get_sbt_task_count(sbt_fd, true, &error);
	ON_ISI_ERROR_GOTO(out, error, "Could not get incomplete task count");

	/*
	 * Count up tasks in each priority queue.  In practice, there should
	 * only be one or two priority queues, but we don't know that here.
	 * Keep going until we run out of queues
	 */
	do {
		int sbt_fd = cpool_fd_store::getInstance().get_pending_sbt_fd(
		    op_type_->get_type(), priority, false, &error);

		/*
		 * We should get an ENOENT trying to open the SBT when we hit
		 * the end of the priority queues.  That's ok, just quit the
		 * loop
		 */
		if (error != NULL && isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			error = NULL;
			break;
		}
		ON_ISI_ERROR_GOTO(out, error,
		    "failed to retrieve pending SBT file descriptor %d",
		    priority);

		// false ==> don't check whether tasks are marked 'done'
		count += get_sbt_task_count(sbt_fd, false, &error);
		ON_ISI_ERROR_GOTO(out, error,
		    "failed to get pending task count for priority %d",
		    priority);

		priority++;
	} while (true);

 out:
	isi_error_handle(error, error_out);
	return count;
}

const size_t MAX_SBT_ENT_SIZE = 8192;
unsigned int
client_task_map::get_sbt_task_count(int sbt_fd,
    bool only_count_tasks_marked_done, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	unsigned int num_tasks = 0;
	int sbt_result = -1;
	btree_key_t start_key = {{ 0, 0 }}, *curr_key = &start_key, temp_key;
	const size_t num_entries_to_get = 8;
	size_t entry_buf_size = MAX_SBT_ENT_SIZE * num_entries_to_get;
	char entry_buf [entry_buf_size];
	size_t num_ents = 0;
	isi_cloud::task *task = NULL;
	ASSERT(entry_buf != NULL);

	if (only_count_tasks_marked_done) {
		task = isi_cloud::task::create_task(op_type_);
		ASSERT(task != NULL);
	}

	do {
		sbt_result = ifs_sbt_get_entries(sbt_fd, curr_key, &temp_key,
		    entry_buf_size, entry_buf, num_entries_to_get, &num_ents);
		if (sbt_result != 0) {
			error = isi_system_error_new(errno,
			    "error retrieving tasks");
			goto out;
		}

		/*
		 * If we're only counting tasks marked as done, we need to
		 * deserialize each task and check its 'done_processing' flag
		 */
		if (only_count_tasks_marked_done) {
			ASSERT(task != NULL);

			char *temp = entry_buf;
			for (size_t i = 0; i < num_ents; i++) {

				struct sbt_entry *current_entry =
				    (struct sbt_entry *)temp;
				ASSERT(current_entry != NULL);

				if (uses_hsbt_) {
					struct hsbt_entry hsbt_ent;
					sbt_entry_to_hsbt_entry(current_entry,
					    &hsbt_ent);
					ASSERT(hsbt_ent.valid_);

					task->set_serialized_task(
					    hsbt_ent.value_,
					    hsbt_ent.value_length_, &error);

					hsbt_entry_destroy(&hsbt_ent);
				} else {
					task->set_serialized_task(
					    current_entry->buf,
					    current_entry->size_out, &error);
				}
				ON_ISI_ERROR_GOTO(out, error);

				/*
				* Note, at this point the sbt entry buffer could be
				* stale (e.g., if a change had been made to the SBT
				* by another thread/process after the
				* ifs_sbt_get_entries call above).  We don't really
				* care right now - close enough is good enough
				*/
				if (!task->is_done_processing())
					num_tasks++;

				temp += (sizeof(struct sbt_entry) +
				    current_entry->size_out);
			}
		/*
		 * If we don't care whether a task is marked as
		 * 'done_processing', save some cycles and don't deserialize
		 */
		} else {
			num_tasks += num_ents;
		}

		curr_key = &temp_key;
	} while (num_ents > 0);

 out:
	if (task != NULL)
		delete(task);

	isi_error_handle(error, error_out);

	return num_tasks;
}
