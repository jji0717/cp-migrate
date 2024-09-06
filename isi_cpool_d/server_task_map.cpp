#include <ifs/ifs_syscalls.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/scoped_object_list.h>
#include <isi_cpool_d_common/task.h>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

#include "thread_pool.h"
#include "server_task_map_ufp.h"
#include "server_task_map.h"

int task_randomization_factor = 20;

static void
randomize_key(btree_key_t *key)
{
	ASSERT(key != NULL);

	static int fd = -1;
	if (fd == -1) {
		fd = open("/dev/random", O_RDONLY);
		if (fd == -1) {
			/*
			 * Failed to open a file descriptor to read random
			 * data, so use a default value.  On the next call
			 * we'll try to open the file descriptor again.
			 */
			ilog(IL_NOTICE,
			    "failed to open file descriptor "
			    "to /dev/random (%s), using default value",
			    strerror(errno));

			key->keys[0] = 0;
			key->keys[1] = 0;

			goto out;
		}
	}

	for (unsigned int i = 0; i < 2; ++i) {
		ssize_t num_read =
		    read(fd, &(key->keys[i]), sizeof key->keys[i]);
		if (num_read != sizeof key->keys[i]) {
			ilog(IL_NOTICE, "failed to read enough random data "
			    "to populate keys[%d] (%s), "
			    "using 0 as a default value",
			    i, strerror(errno));
			key->keys[i] = 0;
		}
	}

	/*
	 * We leave fd open here, as it will stay for the lifetime of the
	 * daemon, so while it is a "leaked" file descriptor, it's deliberate
	 * and OK.
	 */

 out:
	return;
}

server_task_map::read_task_ctx::read_task_ctx(
    const server_task_map *task_map)
	: sbt_ctx_(NULL)
{
	ASSERT(task_map != NULL);

	/* The number of priorities is zero-based. */
	priority_ = task_map->get_num_priorities() - 1;

	randomize_key(&iter_key_);
}

server_task_map::read_task_ctx::~read_task_ctx()
{
	get_sbt_entry_ctx_destroy(&sbt_ctx_);
}

server_task_map::server_task_map(const cpool_operation_type *op_type,
    int num_priorities)
	: task_map(op_type), num_priorities_(num_priorities)
	, initialized_(false), shutdown_pending_(false), map_lock_(NULL)
{
	ASSERT(op_type_ != NULL);
	ASSERT(op_type_ != OT_NONE);
	ASSERT(num_priorities > 0, "num_priorities: %d", num_priorities);
}

server_task_map::~server_task_map()
{
}

void
server_task_map::initialize(struct isi_error **error_out)
{
	ASSERT(!initialized_);

	struct isi_error *error = NULL;

	cpool_fd_store::getInstance().\
	    create_pending_sbt_fds(op_type_->get_type(), num_priorities_,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type_->get_type(), true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	initialized_ = true;

 out:
	isi_error_handle(error, error_out);
}

void
server_task_map::shutdown(bool down)
{
	scoped_wrlock lock(map_lock_);

	shutdown_pending_ = down;
}

bool
server_task_map::is_shutdown_pending(void) const
{
	bool shutdown_pending = false;

	scoped_rdlock lock(map_lock_);

	shutdown_pending = shutdown_pending_;

	return shutdown_pending;
}

int
server_task_map::get_num_priorities(void) const
{
	return num_priorities_;
}

isi_cloud::task *
server_task_map::start_task(read_task_ctx *&ctx_out, int max_task_reads,
    bool recovery, djob_op_job_task_state &task_state,
    struct isi_error **error_out) const
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	isi_cloud::task *task = NULL;
	bool ok_to_start;
	int num_task_reads = 0;

	if (ctx_out == NULL) {
		ctx_out = new read_task_ctx(this);
		ASSERT(ctx_out != NULL);
	}

	/*
	 * Three main steps for starting a task: read a task, qualify it (i.e.
	 * decide whether it should be processed or not), and finalize it.
	 */
	enum {
		READ_TASK,
		QUALIFY_TASK,
		FINALIZE,
		DONE
	} state = READ_TASK;

	while (state != DONE) {
		switch (state) {
		case READ_TASK:
			/*
			 * Read a task from the appropriate SBT.  A task found
			 * and returned by start_task_helper__read_task will be
			 * claimed and locked.
			 */
			++num_task_reads;
			task = start_task_helper__read_task(recovery, ctx_out,
			    &error);
			UFP_READ_TASK_ERROR
			if (error != NULL) {
				/*
				 * start_task_helper__read_task is guaranteed
				 * to not return both a task and an error, so
				 * we don't need to delete task here.
				 */
				isi_error_add_context(error,
				    "error attempting to read task");
				state = DONE;
			}
			else if (task != NULL) {
				/*
				 * We successfully read a task from the SBT;
				 * the next step is qualifying it for
				 * processing.
				 */
				state = QUALIFY_TASK;
			}
			else {
				/*
				 * There were no tasks to read this time
				 * (although that doesn't imply there are no
				 * tasks, see start_task_helper__read_task_*
				 * and get_sbt_entry), so we're done.
				 */
				state = DONE;
			}

			break;
		case QUALIFY_TASK:
			/*
			 * Qualify the task as being eligible for processing.
			 */
			ok_to_start = start_task_helper__qualify_task(task,
			    recovery, task_state, &error);
			UFP_QUALIFY_TASK_ERROR
			if (error != NULL) {
				/*
				 * An error occurred, so clean up and bail out.
				 */
				isi_error_add_context(error,
				    "error attempting to qualify task (%{})",
				    cpool_task_fmt(task));

				unlock_task(task->get_key(),
				    isi_error_suppress(IL_NOTICE));
				relinquish_task(task,
				    isi_error_suppress(IL_NOTICE));

				delete task;
				task = NULL;

				state = DONE;
			}
			else if (!ok_to_start) {
				/*
				 * No error, but this task shouldn't be
				 * processed at this time.  Clean up and read
				 * another task, unless we've exceeded the max
				 * limit.
				 */
				unlock_task(task->get_key(),
				    isi_error_suppress(IL_NOTICE));
				relinquish_task(task,
				    isi_error_suppress(IL_NOTICE));

				delete task;
				task = NULL;

				if (num_task_reads >= max_task_reads) {
					error =
					    isi_system_error_new(EAGAIN,
					    "failed to find qualified task "
					    "after %d attempts",
					    num_task_reads);
					state = DONE;
				} else {
					state = READ_TASK;
				}
			}
			else {
				/*
				 * No error, and this thread is eligible for
				 * processing, so complete the starting of the
				 * task.
				 */
				state = FINALIZE;
			}

			break;
		case FINALIZE:
			/*
			 * Finalize the start of an eligible task.  The task
			 * will be unlocked by start_task_helper__finalize
			 * regardless of error and will remain claimed only on
			 * success.
			 */
			start_task_helper__finalize(task, recovery, &error);
			UFP_FINALIZE_ERROR
			if (error != NULL) {
				/*
				 * An error occurred, so relinquish the claim
				 * on the task before bailing out -- the task
				 * has already been unlocked by
				 * start_task_helper__finalize().
				 */
				isi_error_add_context(error,
				    "error attempting to finalize task (%{})",
				    cpool_task_fmt(task));

				relinquish_task(task,
				    isi_error_suppress(IL_NOTICE));

				delete task;
				task = NULL;
			}

			state = DONE;
			break;

		case DONE:
			/*
			 * This case statement will never be reached. The
			 * WHILE statement above will break when the state
			 * is DONE. This case is present so that the compiler
			 * will complain when new, unhandled states are added
			 * to the states enum.
			 */
			break;
		}
	}

	/* Ensure we're not returning both an error and a task. */
	ASSERT((error == NULL) || (task == NULL));

	if (error == NULL || !isi_system_error_is_a(error, EAGAIN)) {
		delete ctx_out;
		ctx_out = NULL;
	}

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__read_task(bool recovery,
    read_task_ctx *ctx, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;

	isi_cloud::task *task = recovery ?
	    start_task_helper__read_task_recovery(ctx, &error) :
	    start_task_helper__read_task_normal(ctx, &error);
	/* NB: t is now claimed and locked */

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__read_task_recovery(
    read_task_ctx *ctx, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	isi_cloud::task *task = NULL;

	int inprogress_sbt_fd;
	char *ent_buf = NULL;
	struct sbt_entry *sbt_ent = NULL;

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type_->get_type(), false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	do {
		sbt_ent = get_sbt_entry_randomize(inprogress_sbt_fd,
		    &ctx->iter_key_, &ent_buf, task_randomization_factor,
		    &ctx->sbt_ctx_, &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (sbt_ent == NULL)
			goto out;

		task = start_task_helper__sbt_ent_to_claimed_locked_task(
		    inprogress_sbt_fd, sbt_ent, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/*
		 * Having used it to populate a task, we no longer need sbt_ent
		 * so we can delete it (or more precisely, the buffer to which
		 * it pointed).
		 */
		free(ent_buf);
		ent_buf = NULL;
	} while (task == NULL);

 out:
	free(ent_buf);

	/* Ensure we're not returning both a task and an error. */
	ASSERT((task == NULL) || (error == NULL),
	    "task: (%{}) error: %#{}",
	    cpool_task_fmt(task), isi_error_fmt(error));

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__read_task_normal(
    read_task_ctx *ctx, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;

	isi_cloud::task *task = NULL;
	bool startable;
	int pending_sbt_fd;
	char *ent_buf = NULL;
	struct sbt_entry *sbt_ent = NULL;

	startable =
	    isi_cloud::daemon_job_manager::is_operation_startable(op_type_,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!startable)
		goto out;

	do {
		do {
			pending_sbt_fd = cpool_fd_store::getInstance().\
			    get_pending_sbt_fd(op_type_->get_type(),
			    ctx->priority_, false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			sbt_ent = get_sbt_entry_randomize(pending_sbt_fd,
			    &ctx->iter_key_, &ent_buf,
			    task_randomization_factor, &ctx->sbt_ctx_, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (sbt_ent == NULL) {
				/*
				 * We didn't read an entry from the current
				 * priority's pending SBT, so reset and try the
				 * next priority (if it exists).
				 */
				--ctx->priority_;
				randomize_key(&ctx->iter_key_);
				get_sbt_entry_ctx_destroy(&ctx->sbt_ctx_);
			}
		} while (ctx->priority_ >= 0 && sbt_ent == NULL);

		if (sbt_ent == NULL)
			goto out;

		task =
		    start_task_helper__sbt_ent_to_claimed_locked_task(
		    pending_sbt_fd, sbt_ent, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/*
		 * Having used it to populate a task, we no longer need sbt_ent
		 * so we can delete it (or more precisely, the buffer to which
		 * it pointed).
		 */
		free(ent_buf);
		ent_buf = NULL;
	} while (task == NULL);

 out:
	free(ent_buf);

	/* Ensure we're not returning both a task and an error. */
	ASSERT((task == NULL) || (error == NULL),
	    "task: (%{}) error: %#{}",
	    cpool_task_fmt(task), isi_error_fmt(error));

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__sbt_ent_to_claimed_locked_task(int sbt_fd,
    struct sbt_entry *sbt_ent, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;

	isi_cloud::task *task = op_type_->uses_hsbt() ?
	    start_task_helper__sbt_ent_to_claimed_locked_task__hsbt(sbt_fd,
	    sbt_ent, &error) :
	    start_task_helper__sbt_ent_to_claimed_locked_task__sbt(sbt_fd,
	    sbt_ent, &error);

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__sbt_ent_to_claimed_locked_task__sbt(
    int sbt_fd, struct sbt_entry *sbt_ent, struct isi_error **error_out) const
{
	ASSERT(sbt_fd > 0, "sbt_fd: %d", sbt_fd);
	ASSERT(sbt_ent != NULL);

	struct isi_error *error = NULL;
	bool task_claimed = false, task_locked = false;
	char entry_buf[8192];
	size_t entry_size_out;

	/*
	 * Create and populate a task object using the entry read from the SBT.
	 * Save this version of the task serialization to use with SBT
	 * conditional modifications later on.  There is a potential race
	 * condition with a different thread modifying this task after we read
	 * it from the SBT but before we claim it for processing and lock for
	 * edit; the solution is to re-read the task from the SBT after locking
	 * to ensure the in-memory version matches what is persisted to disk.
	 */

	isi_cloud::task *task = isi_cloud::task::create_task(op_type_);
	ASSERT(task != NULL);

	task->set_serialized_task(sbt_ent->buf, sbt_ent->size_out, &error);
	ON_ISI_ERROR_GOTO(out, error);

	task->save_serialization();

	/*
	 * Attempt to claim this task for this thread.  Lock ordering rules
	 * require if a task is to be claimed and locked, the claim must be
	 * done first.
	 */
	task_claimed = claim_task(task, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!task_claimed) {
		delete task;
		task = NULL;

		goto out;
	}

	/*
	 * With the claim held, lock the task and re-read to ensure the on-disk
	 * and in-memory versions are the same.
	 */
	/* true: block / true: exclusive lock */
	lock_task(task->get_key(), true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	if (ifs_sbt_get_entry_at(sbt_fd, &(sbt_ent->key), entry_buf,
	    sizeof entry_buf, NULL, &entry_size_out) != 0) {
		error = isi_system_error_new(errno,
		    "failed to re-read SBT entry for task (key: %#{})",
		    cpool_task_key_fmt(task->get_key()));

		goto out;
	}

	if (sbt_ent->size_out == entry_size_out &&
	    memcmp(sbt_ent->buf, entry_buf, sbt_ent->size_out) == 0)
		goto out;

	/*
	 * The persisted version is newer, so repopulate the task with this
	 * version.
	 */
	task->set_serialized_task(entry_buf, entry_size_out, &error);
	ON_ISI_ERROR_GOTO(out, error);

	task->save_serialization();

 out:
	if (error != NULL) {
		if (task_locked)
			unlock_task(task->get_key(),
			    isi_error_suppress(IL_NOTICE));

		if (task_claimed)
			relinquish_task(task, isi_error_suppress(IL_NOTICE));

		delete task;
		task = NULL;
	}

	isi_error_handle(error, error_out);

	return task;
}

isi_cloud::task *
server_task_map::start_task_helper__sbt_ent_to_claimed_locked_task__hsbt(
    int sbt_fd, struct sbt_entry *sbt_ent, struct isi_error **error_out) const
{
	ASSERT(sbt_fd > 0, "sbt_fd: %d", sbt_fd);
	ASSERT(sbt_ent != NULL);

	struct isi_error *error = NULL;
	struct hsbt_entry hsbt_ent;
	bool task_claimed = false, task_locked = false, entry_found = false;
	char entry_buf[8192];
	size_t entry_size = sizeof entry_buf;

	/*
	 * Create and populate a task object using the entry read from the SBT.
	 * Save this version of the task serialization to use with SBT
	 * conditional modifications later on.  There is a potential race
	 * condition with a different thread modifying this task after we read
	 * it from the SBT but before we claim it for processing and lock for
	 * edit; the solution is to re-read the task from the SBT after locking
	 * to ensure the in-memory version matches what is persisted to disk.
	 */

	isi_cloud::task *task = isi_cloud::task::create_task(op_type_);
	ASSERT(task != NULL);

	sbt_entry_to_hsbt_entry(sbt_ent, &hsbt_ent);
	ASSERT(hsbt_ent.valid_);

	task->set_serialized_task(hsbt_ent.value_, hsbt_ent.value_length_,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	task->save_serialization();

	/*
	 * Attempt to claim this task for this thread.  Lock ordering rules
	 * require if a task is to be claimed and locked, the claim must be
	 * done first.
	 */
	task_claimed = claim_task(task, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!task_claimed) {
		delete task;
		task = NULL;

		goto out;
	}

	/*
	 * With the claim held, lock the task and re-read to ensure the on-disk
	 * and in-memory versions are the same.
	 */
	/* true: block / true: exclusive lock */
	lock_task(task->get_key(), true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	/*
	 * With the task locked, re-read to ensure the on-disk and in-memory
	 * copies are the same.
	 */
	entry_found = hsbt_get_entry(sbt_fd, hsbt_ent.key_,
	    hsbt_ent.key_length_, entry_buf, &entry_size, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!entry_found) {
		error = isi_system_error_new(ENOENT,
		    "failed to re-read HSBT entry for task (key: %#{})",
		    cpool_task_key_fmt(task->get_key()));
		goto out;
	}

	if (hsbt_ent.value_length_ == entry_size &&
	    memcmp(hsbt_ent.value_, entry_buf, hsbt_ent.value_length_) == 0)
		goto out;

	/*
	 * The persisted version is newer, so repopulate the task with this
	 * version.
	 */
	task->set_serialized_task(entry_buf, entry_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

	task->save_serialization();

 out:
	hsbt_entry_destroy(&hsbt_ent);

	if (error != NULL) {
		if (task_locked)
			unlock_task(task->get_key(),
			    isi_error_suppress(IL_NOTICE));

		if (task_claimed)
			relinquish_task(task, isi_error_suppress(IL_NOTICE));

		delete task;
		task = NULL;
	}

	isi_error_handle(error, error_out);

	return task;
}

bool
server_task_map::start_task_helper__qualify_task(isi_cloud::task *task,
    bool recovery, djob_op_job_task_state &task_state,
    struct isi_error **error_out) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;
	bool ok_to_start = false;

	/*
	 * Determine whether or not this task is eligible for processing based
	 * on the mode (normal vs. recovery) of this thread, the task's state,
	 * and (optionally) the task's wait_until time.
	 *
	 * Regardless of the thread's mode, any task that is in the running or
	 * canceled state is potentially eligible for processing.  Canceled
	 * tasks are allowed to proceed as this is the mechanism for removing
	 * them from the pending SBT; in other words, we don't actively remove
	 * them when a job is canceled.
	 *
	 * In recovery mode, a task is also eligible if its state is paused;
	 * this allows the task to migrate back to the appropriate pending SBT.
	 *
	 * In normal mode, a task cannot be processed before its wait_until
	 * time, which is used to ensure that failing tasks are processed less
	 * frequently.
	 */
	task_state = isi_cloud::daemon_job_manager::get_state(task, &error);
	if (error != NULL) {
		task_state = DJOB_OJT_NONE;

		isi_error_add_context(error);
		goto out;
	}

	if (!recovery) {
		if (task_state != DJOB_OJT_RUNNING &&
		    task_state != DJOB_OJT_CANCELLED)
		    goto out;

		struct timeval now;
		gettimeofday(&now, NULL);

		struct timeval wait_until = task->get_wait_until_time();
		if (wait_until.tv_sec > now.tv_sec)
			goto out;
	}
	else {
		if (task_state != DJOB_OJT_RUNNING &&
		    task_state != DJOB_OJT_CANCELLED &&
		    task_state != DJOB_OJT_PAUSED)
		    goto out;
	}

	ok_to_start = true;

 out:
	isi_error_handle(error, error_out);

	return ok_to_start;
}

void
server_task_map::start_task_helper__finalize(isi_cloud::task *task,
    bool recovery, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	recovery ?
	    start_task_helper__finalize_recovery(task, sbt_ops,
		objects_to_delete, &error) :
	    start_task_helper__finalize_normal(task, sbt_ops,
		objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * It's OK to use hsbt_bulk_op here, even if we aren't interacting with
	 * any hashed SBTs - hsbt_bulk_op doesn't alter non-hashed SBT bulk
	 * entries.
	 */
	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	unlock_task(task->get_key(), isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}

void
server_task_map::start_task_helper__finalize_recovery(isi_cloud::task *task,
    std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;

	int inprogress_sbt_fd;
	struct hsbt_bulk_entry sbt_op;

	const void *new_serialization;
	size_t new_serialization_size;
	task->get_serialized_task(new_serialization, new_serialization_size);

	const void *old_serialization;
	size_t old_serialization_size;
	task->get_saved_serialization(old_serialization,
	    old_serialization_size);

	const void *serialized_key;
	size_t serialized_key_size;
	task->get_key()->get_serialized_key(serialized_key,
	    serialized_key_size);

	uint32_t node_id = get_devid(&error);
	ON_ISI_ERROR_GOTO(out, error);
	task->set_handling_node_id(node_id);

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type_->get_type(), false, &error);

	/*
	 * For recovery, all that is needed is to update the entry in the
	 * in-progress SBT (no daemon job changes required).
	 */

	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = inprogress_sbt_fd;
	sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
	sbt_op.bulk_entry.cm = BT_CM_BUF;
	sbt_op.bulk_entry.old_entry_buf =
	    const_cast<void *>(old_serialization);
	sbt_op.bulk_entry.old_entry_size = old_serialization_size;
	sbt_op.bulk_entry.entry_buf = const_cast<void *>(new_serialization);
	sbt_op.bulk_entry.entry_size = new_serialization_size;

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

 out:
	isi_error_handle(error, error_out);
}

void
server_task_map::start_task_helper__finalize_normal(isi_cloud::task *task,
    std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;

	int pending_sbt_fd, inprogress_sbt_fd;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	const void *serialized_key;
	size_t serialized_key_size;
	uint32_t node_id;
	struct hsbt_bulk_entry sbt_op;

	/*
	 * From the task map perspective, two operations are needed to start a
	 * task "normally":
	 *  - delete from pending SBT
	 *  - add to in-progress SBT
	 */

	node_id = get_devid(&error);
	ON_ISI_ERROR_GOTO(out, error);
	task->set_handling_node_id(node_id);

	task->set_location(TASK_LOC_INPROGRESS);

	pending_sbt_fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(op_type_->get_type(), task->get_priority(),
	    false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(op_type_->get_type(), false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * For non-recovery, we need to delete the task from the pending SBT
	 * and add it to the in-progress SBT.  We may also need to do some
	 * bookkeeping related to daemon jobs.
	 */

	task->get_saved_serialization(old_serialization,
	    old_serialization_size);
	task->get_serialized_task(new_serialization, new_serialization_size);
	task->get_key()->get_serialized_key(serialized_key,
	    serialized_key_size);

	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = pending_sbt_fd;
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
		    "expected btree_key_t (size: %zu != %zu)",
		    serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		sbt_op.bulk_entry.key = *bt_key;
	}

	sbt_ops.push_back(sbt_op);

	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = inprogress_sbt_fd;
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
		    "expected btree_key_t (size: %zu != %zu)",
		    serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		sbt_op.bulk_entry.key = *bt_key;
	}

	sbt_ops.push_back(sbt_op);

	isi_cloud::daemon_job_manager::start_task(task, sbt_ops,
	    objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
server_task_map::finish_task(const task_key *key,
    task_process_completion_reason reason, struct isi_error **error_out) const
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	bool task_locked = false;
	isi_cloud::task *retrieved_task = NULL;
	int pending_sbt_fd, inprogress_sbt_fd;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	const void *serialized_key;
	size_t serialized_key_size;
	struct hsbt_bulk_entry sbt_op;
	std::vector<struct hsbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	/* true: block / true: exclusive lock */
	lock_task(key, true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	/* false: don't ignore ENOENT */
	retrieved_task = find_task(key, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(retrieved_task != NULL);
	ASSERT(retrieved_task->get_location() == TASK_LOC_INPROGRESS,
	    "unexpected location (%{}) for task (%{})",
	    task_location_fmt(retrieved_task->get_location()),
	    cpool_task_fmt(retrieved_task));

	/*
	 * Shortcut: since stopped tasks are left in-place (in the in-progress
	 * SBT) with no change to daemon jobs bookkeeping, this function is
	 * basically a no-op for stopped tasks.  We still need to relinquish
	 * the claim on this task, so we jump here after retrieving it.
	 */
	if (reason == CPOOL_TASK_STOPPED)
		goto out;

	retrieved_task->save_serialization();

	/*
	 * Remove this task from the in-progress SBT and potentially add it
	 * back to the pending SBT, depending on the reason.
	 */
	inprogress_sbt_fd = cpool_fd_store::getInstance().\
	    get_inprogress_sbt_fd(retrieved_task->get_op_type()->get_type(),
	    false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	retrieved_task->get_saved_serialization(old_serialization,
	    old_serialization_size);
	key->get_serialized_key(serialized_key, serialized_key_size);

	memset(&sbt_op, 0, sizeof sbt_op);

	sbt_op.is_hash_btree = uses_hsbt_;
	sbt_op.bulk_entry.fd = inprogress_sbt_fd;
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
		    "expected btree_key_t (size: %zu != %zu)",
		    serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		sbt_op.bulk_entry.key = *bt_key;
	}

	sbt_ops.push_back(sbt_op);

	/*
	 * If the task has been paused or has failed due to a retryable error,
	 * it goes back to one of the pending SBTs.  In the case of a retryable
	 * error, we also update the wait_until time, which prevents the
	 * reprocessing of the task for a period of time.
	 */
	if (reason == CPOOL_TASK_PAUSED ||
	    reason == CPOOL_TASK_RETRYABLE_ERROR) {
		pending_sbt_fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(
		    retrieved_task->get_op_type()->get_type(),
		    retrieved_task->get_priority(), false, &error);
		ON_ISI_ERROR_GOTO(out, error);

		retrieved_task->set_location(TASK_LOC_PENDING);

		if (reason == CPOOL_TASK_RETRYABLE_ERROR) {
			retrieved_task->inc_num_attempts();
			get_new_wait_until_time(retrieved_task);
		}

		retrieved_task->get_serialized_task(new_serialization,
		    new_serialization_size);

		memset(&sbt_op, 0, sizeof sbt_op);

		sbt_op.is_hash_btree = uses_hsbt_;
		sbt_op.bulk_entry.fd = pending_sbt_fd;
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
			    "expected btree_key_t (size: %zu != %zu)",
			    serialized_key_size, sizeof(btree_key_t));

			const btree_key_t *bt_key =
			    static_cast<const btree_key_t *>(serialized_key);
			ASSERT(bt_key != NULL);

			sbt_op.bulk_entry.key = *bt_key;
		}

		sbt_ops.push_back(sbt_op);
	}
	else {
		if (!retrieved_task->has_ckpt_within_task()) {
			const file_task_key *file_key =
			    dynamic_cast<const file_task_key *>(key);
			ASSERT(file_key, "expecting file_task_key");

			file_ckpt ckpt(file_key->get_lin(),
			    file_key->get_snapid());

			ckpt.remove();
		}
	}

	isi_cloud::daemon_job_manager::finish_task(retrieved_task, reason,
	    sbt_ops, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (task_locked)
		unlock_task(key, isi_error_suppress(IL_NOTICE));

	if (retrieved_task != NULL) {
		/*
		 * Relinquish the claim of this task, even if we reach this
		 * statement due to error; in such a scenario, the recovery
		 * thread will process it later on.
		 */
		relinquish_task(retrieved_task, isi_error_suppress(IL_NOTICE));
		delete retrieved_task;
	}

	isi_error_handle(error, error_out);
}

void
server_task_map::get_new_wait_until_time(isi_cloud::task *task) const
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;
	int num_attempts = task->get_num_attempts();
	const struct cpool_d_retry_settings *retry_settings = NULL;
	char *retry_coefficients = NULL;
	int default_wait_until_interval_in_seconds = 30;
	int wait_until_interval_in_seconds, current_coefficient;
	char *current_coefficient_str = NULL, *saveptr = NULL;
	struct timeval now, new_wait_until_time;

	/*
	 * Set a default value for wait_until_interval_in_seconds in case we
	 * fail to read the configuration data.
	 */
	wait_until_interval_in_seconds =
	    default_wait_until_interval_in_seconds;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
		    reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		retry_settings = config_settings->get_retry_settings(
		    get_op_type()->get_type());
		ASSERT(retry_settings != NULL);

		retry_coefficients =
		    strdup(retry_settings->retry_function_coefficients);
		ASSERT(retry_coefficients != NULL);
	}

	/*
	 * Time for some math!
	 *
	 * The retry_coefficients variable currently holds a list of
	 * coefficients that define a polynomial; this polynomial defines the
	 * interval which must expire before the task can be attempted again,
	 * in terms of the number of attempts already made.
	 *
	 * For example, a retry_coefficients string of "3:2:1" translates to:
	 *
	 *	f(x) = 3x^2 + 2x + 1
	 *
	 * If 4 attempts have been made on this task, the interval to wait
	 * until the 5th attempt is f(4) = 57 seconds.
	 */
	wait_until_interval_in_seconds = 0;
	current_coefficient_str = strtok_r(retry_coefficients, ":", &saveptr);
	while(current_coefficient_str != NULL) {
		current_coefficient = atoi(current_coefficient_str);
		wait_until_interval_in_seconds *= num_attempts;
		wait_until_interval_in_seconds += current_coefficient;

		current_coefficient_str = strtok_r(NULL, ":", &saveptr);
	}
	ASSERT(wait_until_interval_in_seconds >= 0,
	    "wait_until_interval_in_seconds: %d",
	    wait_until_interval_in_seconds);

 out:
	if (error != NULL) {
		ilog(IL_INFO,
		    "error determining new wait_until time, "
		    "using default value of %d seconds: %#{}",
		    default_wait_until_interval_in_seconds,
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}

	gettimeofday(&now, NULL);
	new_wait_until_time = now;
	new_wait_until_time.tv_sec += wait_until_interval_in_seconds;
	task->set_wait_until_time(new_wait_until_time);

	free(retry_coefficients);
}

bool
server_task_map::get_task_checkpoint(const task_key *key,
    void *&checkpoint_data, size_t &checkpoint_data_size,
    djob_op_job_task_state &task_state, off_t offset, file_ckpt *ckpt) const
{
	ASSERT(initialized_);
	ASSERT(key != NULL);
	ASSERT(checkpoint_data == NULL);

	struct isi_error *error = NULL;
	bool task_locked = false, ret = true;
	isi_cloud::task *retrieved_task = NULL;

	/* true: block / true: exclusive lock */
	lock_task(key, true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	/* false: don't ignore ENOENT */
	retrieved_task = find_task(key, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(retrieved_task != NULL);
	ASSERT(retrieved_task->get_location() == TASK_LOC_INPROGRESS,
	    "unexpected location (%{}) for task (%{})",
	    task_location_fmt(retrieved_task->get_location()),
	    cpool_task_fmt(retrieved_task));

	task_state = is_shutdown_pending() ?
	    DJOB_OJT_STOPPED :
	    isi_cloud::daemon_job_manager::get_state(retrieved_task, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// get checkpoint data from task sbt
	if (retrieved_task->has_ckpt_within_task()) {
		ASSERT(ckpt == NULL, "task does not use file checkpoint");
		const void *local_checkpoint_data;

		retrieved_task->get_checkpoint_data(local_checkpoint_data,
		    checkpoint_data_size);
		if (local_checkpoint_data && checkpoint_data_size > 0) {
			/*
			 * If the checkpoint information is stored in the SBT,
			 * make a copy of it since checkpoint_data currently
			 * points to a buffer inside of task.
			 */
			checkpoint_data = malloc(checkpoint_data_size);
			ASSERT(checkpoint_data, "failed to allocate memory");

			memcpy(const_cast<void *>(checkpoint_data),
			    local_checkpoint_data, checkpoint_data_size);
		}
	}
	// get the checkpoint data from temp file
	else {
		const file_task_key *file_key =
		    dynamic_cast<const file_task_key *>(key);
		ASSERT(file_key, "expecting file_task_key");
		ASSERT(ckpt != NULL);

		ret = ckpt->get(checkpoint_data, checkpoint_data_size, offset);
	}

 out:
	if (error != NULL) {
		if (retrieved_task != NULL)
			ilog(get_loglevel(error, false),
			    "error getting checkpoint information "
			    "for task (%{}): %#{}",
			    cpool_task_fmt(retrieved_task),
			    isi_error_fmt(error));
		else
			ilog(get_loglevel(error, false),
			    "error getting checkpoint information: %#{}",
			    isi_error_fmt(error));

		isi_error_free(error);

		task_state = DJOB_OJT_NONE;
		ret = false;
	}

	if (retrieved_task != NULL)
		delete retrieved_task;

	if (task_locked)
		unlock_task(key, isi_error_suppress(IL_NOTICE));

	return ret;
}

bool
server_task_map::set_task_checkpoint(const task_key *key,
    void *checkpoint_data, size_t checkpoint_data_size,
    djob_op_job_task_state &task_state, off_t offset, file_ckpt *ckpt) const
{
	ASSERT(initialized_);
	ASSERT(key != NULL);
	ASSERT(checkpoint_data != NULL);

	struct isi_error *error = NULL;

	bool task_locked = false, ret = true;
	isi_cloud::task *retrieved_task = NULL;
	int inprogress_sbt_fd;
	const void *serialized_key;
	size_t serialized_key_size;
	const void *old_task_serialization, *new_task_serialization;
	size_t old_task_serialization_size, new_task_serialization_size;

	/* true: block / true: exclusive lock */
	lock_task(key, true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	/* false: don't ignore ENOENT */
	retrieved_task = find_task(key, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(retrieved_task != NULL);
	ASSERT(retrieved_task->get_location() == TASK_LOC_INPROGRESS,
	    "unexpected location (%{}) for task (%{})",
	    task_location_fmt(retrieved_task->get_location()),
	    cpool_task_fmt(retrieved_task));

	retrieved_task->save_serialization();

	task_state = is_shutdown_pending() ?
	    DJOB_OJT_STOPPED :
	    isi_cloud::daemon_job_manager::get_state(retrieved_task, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// set checkpoint data to task sbt
	if (retrieved_task->has_ckpt_within_task()) {
		ASSERT(ckpt == NULL, "task does not use file checkpoint");
		retrieved_task->set_checkpoint_data(checkpoint_data,
		    checkpoint_data_size);

		inprogress_sbt_fd = cpool_fd_store::getInstance().
		    get_inprogress_sbt_fd(get_op_type()->get_type(),
		    false, &error);
		ON_ISI_ERROR_GOTO(out, error);

		key->get_serialized_key(serialized_key, serialized_key_size);

		retrieved_task->get_saved_serialization(old_task_serialization,
		    old_task_serialization_size);
		retrieved_task->get_serialized_task(new_task_serialization,
		    new_task_serialization_size);

		if (uses_hsbt_) {
			hsbt_cond_mod_entry(inprogress_sbt_fd, serialized_key,
			    serialized_key_size, new_task_serialization,
			    new_task_serialization_size, NULL, BT_CM_BUF,
			    old_task_serialization,
			    old_task_serialization_size, NULL, &error);
		}
		else {
			ASSERT(serialized_key_size == sizeof(btree_key_t),
			    "expected btree_key_t (size: %zu != %zu)",
			    serialized_key_size, sizeof(btree_key_t));

			const btree_key_t *bt_key =
			    static_cast<const btree_key_t *>(serialized_key);
			ASSERT(bt_key != NULL);

			if (ifs_sbt_cond_mod_entry(inprogress_sbt_fd,
			    const_cast<btree_key_t *>(bt_key),
			    new_task_serialization,
			    new_task_serialization_size, NULL, BT_CM_BUF,
			    old_task_serialization,
			    old_task_serialization_size, NULL) != 0) {
				error = isi_system_error_new(errno,
				    "failed to modify task (%{}) "
				    "with updated checkpoint information",
				    cpool_task_fmt(retrieved_task));
			}
		}
		ON_ISI_ERROR_GOTO(out, error);
	}
	// set the checkpoint data to temp file
	else {
		const file_task_key *file_key =
		    dynamic_cast<const file_task_key *>(key);
		ASSERT(file_key, "expecting file_task_key");
		ASSERT(ckpt != NULL);

		ret = ckpt->set(checkpoint_data, checkpoint_data_size, offset);
	}

 out:
	if (error != NULL) {
		if (retrieved_task != NULL) {
			ilog(get_loglevel(error, false),
			    "error setting checkpoint information "
			    "for task (%{}): %#{}",
			    cpool_task_fmt(retrieved_task),
			    isi_error_fmt(error));
		}
		else {
			ilog(get_loglevel(error, false),
			    "error setting checkpoint information "
			    "for task (<null>): %#{}", isi_error_fmt(error));
		}

		isi_error_free(error);

		task_state = DJOB_OJT_NONE;
		ret = false;
	}

	if (retrieved_task != NULL)
		delete retrieved_task;

	if (task_locked)
		unlock_task(key, isi_error_suppress(IL_NOTICE));

	return ret;
}
