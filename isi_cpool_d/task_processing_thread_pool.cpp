#include <inttypes.h>

#include <isi_ilog/ilog.h>
#include <isi_util/isi_error.h>

#include <signal.h>

#include <tuple>

#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_gc.h>
#include <isi_cpool_cbm/isi_cbm_invalidate.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cbm_restore_coi.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_d_api/cpool_api_c.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_tch/task_ckpt_helper.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_licensing/licensing.h>

#include "server_task_map.h"
#include "thread_pool.h"

#include "task_processing_thread_pool.h"

task_processing_thread_pool::task_processing_thread_pool(server_task_map *tm,
														 int num_normal_threads, int num_recovery_threads)
	: normal_args_(NULL), recovery_args_(NULL), num_normal_threads_(num_normal_threads), num_recovery_threads_(num_recovery_threads), task_map_(tm)
{
	ASSERT(num_normal_threads_ >= 0);
	ASSERT(num_recovery_threads_ >= 0);
	ASSERT(task_map_ != NULL);

	start_threads();
}

task_processing_thread_pool::~task_processing_thread_pool()
{
	shutdown();

	if (normal_args_ != NULL)
		delete normal_args_;
	if (recovery_args_ != NULL)
		delete recovery_args_;
}

void task_processing_thread_pool::shutdown(void)
{
	/*
	 * Prior to calling the parent method, shutdown the server task map to
	 * indicate to tasks (in particular, long-running tasks) to stop
	 * processing.
	 */
	task_map_->shutdown(true);

	thread_pool::shutdown();
}

void task_processing_thread_pool::start_threads(void)
{
	normal_args_ = new thread_args;
	recovery_args_ = new thread_args;

	normal_args_->tp = this;
	normal_args_->recovery = false;
	recovery_args_->tp = this;
	recovery_args_->recovery = true;

	// reset the shutdown_pending flag
	task_map_->shutdown(false);

	for (int i = 0; i < num_normal_threads_; ++i)
		add_thread(NULL, start_thread, normal_args_);
	for (int i = 0; i < num_recovery_threads_; ++i)
		add_thread(NULL, start_thread, recovery_args_);
}

/*
 * THROTTLED_ILOG
 *
 * The point of this macro is to prevent log spam by dynamically selecting the
 * level at which to log an error, based on the frequency at which that error
 * is being logged.  It's a little hacky, and is meant to be a good-enough fix
 * for now; see Bug 191284.
 *
 * An error will be logged by a node at desired_level if it hasn't recently
 * been logged by that node at that level, or logged at the more verbose of
 * desired_level and fallback_level otherwise.  The delay parameter specifies
 * what is considered "recently" and each node calculates this independently.
 *
 * An error is identified by three aspects:
 * (1) the error class (system, CBM, etc.)
 * (2) the integer representation of the error (code, type, etc.)
 * (3) the source code line number at which the error is logged
 *
 * Note that locking is used to prevent corruption, but not strict correctness.
 * For example, two threads could read the time at which a particular error was
 * last logged and use it to determine that they both should log the error at
 * desired_level rather than fallback_level thus leading to more than 1 log
 * message at desired_level in the "recent" window; this is an acceptable
 * defect.
 */
#define THROTTLED_ILOG(error, desired_level, fallback_level, delay, args...) \
	do                                                                       \
	{                                                                        \
		if ((error) == NULL)                                                 \
			break;                                                           \
		if (!ilog_will_log_level(desired_level))                             \
			break;                                                           \
                                                                             \
		struct timeval last_logged, next_log, now;                           \
		last_log_time_key key = std::make_tuple(NULL, 0, 0);                 \
                                                                             \
		last_logged = last_log_time(error, __LINE__, &key);                  \
		timeradd(&last_logged, &(delay), &next_log);                         \
		gettimeofday(&now, NULL);                                            \
                                                                             \
		if (timercmp(&next_log, &now, <))                                    \
		{                                                                    \
			pthread_rwlock_wrlock(&log_times_map_lock);                      \
			last_log_times[key] = now;                                       \
			pthread_rwlock_unlock(&log_times_map_lock);                      \
                                                                             \
			ilog(desired_level, args);                                       \
		}                                                                    \
		else                                                                 \
		{                                                                    \
			/*                                                               \
			 * Use MAX here since higher level value means lower             \
			 * (more verbose) log level.                                     \
			 */                                                              \
			ilog(MAX(desired_level, fallback_level), args);                  \
		}                                                                    \
	} while (false);

typedef std::tuple<const struct isi_error_class *, int, int> last_log_time_key;
static std::map<last_log_time_key, struct timeval> last_log_times;
static pthread_rwlock_t log_times_map_lock = PTHREAD_RWLOCK_INITIALIZER;

static struct timeval
last_log_time(const struct isi_error *error, int linenum,
			  last_log_time_key *key)
{
	int err_as_int;
	struct timeval last_log_time = {0};
	std::map<last_log_time_key, struct timeval>::const_iterator it;

	/*
	 * If an unknown class is encountered, treat the error as never having
	 * been logged.
	 */
	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
		err_as_int = isi_system_error_get_error(
			(const struct isi_system_error *)error);
	else if (isi_error_is_a(error, CBM_ERROR_CLASS))
		err_as_int = (int)isi_cbm_error_get_type(error);
	else
		goto out;

	*key =
		std::make_tuple(isi_error_get_class(error), err_as_int, linenum);

	pthread_rwlock_rdlock(&log_times_map_lock);

	it = last_log_times.find(*key);
	if (it != last_log_times.end())
		last_log_time = it->second;

	pthread_rwlock_unlock(&log_times_map_lock);

out:
	return last_log_time;
}

static bool
is_retryable(const struct isi_error *error)
{
	bool retryable = true;
	ASSERT(error != NULL);

	if (isi_cbm_error_is_a(error, CBM_ALREADY_STUBBED) ||
		isi_cbm_error_is_a(error, CBM_NOT_A_STUB) ||
		isi_system_error_is_a(error, ENOENT) ||
		isi_cbm_error_is_a(error, CBM_PERM_ERROR) ||
		isi_cbm_error_is_a(error, CBM_CHECKSUM_ERROR) ||
		isi_cbm_error_is_a(error, CBM_LIN_DOES_NOT_EXIST) ||
		isi_cbm_error_is_a(error, CBM_NOT_SUPPORTED_ERROR) ||
		isi_cbm_error_is_a(error, CBM_CLAPI_BAD_REQUEST) ||
		isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND) ||
		isi_error_is_a(error, CPOOL_NOT_FOUND_ERROR_CLASS))
	{
		retryable = false;
	}

	ilog(IL_DEBUG, "%s error: %#{}",
		 retryable ? "RETRYABLE" : "NON-RETRYABLE", isi_error_fmt(error));

	return retryable;
}

static void
run_consumer_function(server_task_map *tm, task_ckpt_helper *tch,
					  isi_cloud::task *t, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_lin_t lin = INVALID_LIN;
	ifs_snapid_t snapid = HEAD_SNAPID;

	bool (*get_func)(const void *, void **, size_t *,
					 djob_op_job_task_state *, off_t) = tch_get_checkpoint_data;
	bool (*set_func)(void *, void *, size_t,
					 djob_op_job_task_state *, off_t) = tch_set_checkpoint_data;

	switch (tm->get_op_type()->get_type())
	{
	case CPOOL_TASK_TYPE_ARCHIVE:
	{
		/*
		 * Check that smartpools is licensed - if not, no new archives
		 * are permitted.
		 */
		if (isi_licensing_module_status(
				ISI_LICENSING_CLOUDPOOLS) != ISI_LICENSING_LICENSED)
		{
			error = isi_cbm_error_new(CBM_PERM_ERROR,
									  "CloudPools is not licensed");
			break;
		}

		isi_cloud::archive_task *a_t =
			dynamic_cast<isi_cloud::archive_task *>(t);
		ASSERT(a_t != NULL);

		lin = a_t->get_lin();
		if (a_t->has_policyid())
		{
			ilog(IL_INFO, "%s called lin:%ld policy_id:%d will call isi_cbm_archive_by_id", __func__, lin, a_t->get_policy_id());
			isi_cbm_archive_by_id(lin, a_t->get_policy_id(), NULL,
								  tch, get_func, set_func, true, &error);
		}
		else
		{
			isi_cbm_archive_by_name(lin, a_t->get_policy_name(), NULL,
									tch, get_func, set_func, true, &error);
		}

		break;
	}
	case CPOOL_TASK_TYPE_RECALL:
	{
		isi_cloud::recall_task *r_t =
			dynamic_cast<isi_cloud::recall_task *>(t);
		ASSERT(r_t != NULL);

		lin = r_t->get_lin();
		snapid = r_t->get_snapid();
		isi_cbm_recall(lin, snapid,
					   r_t->get_retain_stub(), NULL, tch, get_func, set_func,
					   &error);

		break;
	}
	case CPOOL_TASK_TYPE_WRITEBACK:
	{
		isi_cloud::writeback_task *wb_t =
			dynamic_cast<isi_cloud::writeback_task *>(t);
		ASSERT(wb_t != NULL);

		lin = wb_t->get_lin();
		snapid = wb_t->get_snapid();
		isi_cbm_sync(lin, snapid,
					 get_func, set_func, tch, &error);

		break;
	}
	/////kdq_scanning_thread_pool.cpp调用
	case CPOOL_TASK_TYPE_LOCAL_GC:
	{
		isi_cloud::local_gc_task *lgc_t =
			dynamic_cast<isi_cloud::local_gc_task *>(t);
		ASSERT(lgc_t != NULL);

		lin = lgc_t->get_lin();
		snapid = lgc_t->get_snapid();
		isi_cbm_local_gc(lin, snapid,
						 lgc_t->get_object_id(), lgc_t->get_date_of_death(), NULL,
						 tch, get_func, set_func, &error);

		break;
	}
	////////ooi_scannning_thread_pool.cpp调用
	case CPOOL_TASK_TYPE_CLOUD_GC:
	{
		isi_cloud::cloud_gc_task *cgc_t =
			dynamic_cast<isi_cloud::cloud_gc_task *>(t);
		ASSERT(cgc_t != NULL);

		isi_cbm_cloud_gc(cgc_t->get_cloud_object_id(), NULL, tch,
						 get_func, set_func, &error);

		break;
	}
	case CPOOL_TASK_TYPE_CACHE_INVALIDATION:
	{
		isi_cloud::cache_invalidation_task *ci_t =
			dynamic_cast<isi_cloud::cache_invalidation_task *>(t);
		ASSERT(ci_t != NULL);

		lin = ci_t->get_lin();
		snapid = ci_t->get_snapid();
		isi_cbm_invalidate_cached_file(lin, snapid,
									   get_func, set_func, tch, &error);

		break;
	}
	case CPOOL_TASK_TYPE_RESTORE_COI:
	{
		isi_cloud::restore_coi_task *restore_coi_t =
			dynamic_cast<isi_cloud::restore_coi_task *>(t);
		ASSERT(restore_coi_t != NULL);
		const task_account_id &id = restore_coi_t->get_account_id();
		isi_cbm_restore_coi_for_acct(id.cluster_.c_str(),
									 id.account_id_, restore_coi_t->get_dod(),
									 tch, get_func, set_func, &error);

		break;
	}

	case CPOOL_TASK_TYPE_NONE:
	case CPOOL_TASK_TYPE_MAX_TASK_TYPE:
		ASSERT(false, "invalid task type: %d",
			   tm->get_op_type()->get_type());
	}

	if (error != NULL)
	{
		bool retryable = is_retryable(error);
		uint32_t log_level = get_loglevel(error, retryable,
										  lin, snapid);
		struct timeval log_delay = {
			.tv_sec = 10,
			.tv_usec = 0};

		/*
		 * ENOENT is NOT retryable so log_level will be IL_ERR.
		 * We do not want to spam the log file with
		 * ENOENT in case someone deleted the file on the side.
		 */
		if (isi_system_error_is_a(error, ENOENT))
		{
			log_level = IL_INFO;
		}

		THROTTLED_ILOG(error, log_level, IL_DEBUG, log_delay,
					   "run_consumer_function complete "
					   "for task (%{}): %#{}",
					   cpool_task_fmt(t), isi_error_fmt(error));
	}

	isi_error_handle(error, error_out);
}

static void
process_task(server_task_map *tm, task_ckpt_helper *tch,
			 isi_cloud::task *t, task_process_completion_reason &reason,
			 struct isi_error **error_out)
{
	struct isi_error *consumer_error = NULL, *processing_error = NULL;
	int inprogress_sbt_fd;
	bool task_locked = false;
	isi_cloud::task *retrieved_task = NULL;
	const void *serialized_key;
	size_t serialized_key_size;
	const void *old_task_serialization, *new_task_serialization;
	size_t old_task_serialization_size, new_task_serialization_size;

	run_consumer_function(tm, tch, t, &consumer_error);

	inprogress_sbt_fd = cpool_fd_store::getInstance().get_inprogress_sbt_fd(t->get_op_type()->get_type(), false,
																			&processing_error);
	ON_ISI_ERROR_GOTO(out, processing_error);

	/* true: block / true: exclusive */
	tm->lock_task(t->get_key(), true, true, &processing_error);
	ON_ISI_ERROR_GOTO(out, processing_error);
	task_locked = true;

	/*
	 * Re-read the task from disk as it may have been changed by either
	 * the consumer function (checkpoint updates) or a client via the API
	 * (adding daemon jobs).
	 */
	/* false: don't ignore ENOENT */
	retrieved_task = tm->find_task(t->get_key(), false, &processing_error);
	ON_ISI_ERROR_GOTO(out, processing_error);
	retrieved_task->save_serialization();

	if (consumer_error == NULL)
	{
		reason = CPOOL_TASK_SUCCESS;
		retrieved_task->set_most_recent_status(
			isi_cloud::task::SUCCESS, 0);
	}
	else
	{
		if (isi_cbm_error_get_type(consumer_error) ==
			CBM_FILE_PAUSED)
		{
			reason = CPOOL_TASK_PAUSED;
			retrieved_task->set_most_recent_status(
				isi_cloud::task::CBM, CBM_FILE_PAUSED);
		}
		else if (isi_cbm_error_get_type(consumer_error) ==
				 CBM_FILE_CANCELLED)
		{
			reason = CPOOL_TASK_CANCELLED;
			retrieved_task->set_most_recent_status(
				isi_cloud::task::CBM, CBM_FILE_CANCELLED);
		}
		else if (isi_cbm_error_get_type(consumer_error) ==
				 CBM_FILE_STOPPED)
		{
			reason = CPOOL_TASK_STOPPED;
			/*
			 * Don't (re)set the most recent status on this task.
			 * When a task is stopped everything is left as is.
			 */
		}
		else
		{
			reason = is_retryable(consumer_error) ? CPOOL_TASK_RETRYABLE_ERROR : CPOOL_TASK_NON_RETRYABLE_ERROR;

			if (isi_error_is_a(consumer_error, CBM_ERROR_CLASS))
				retrieved_task->set_most_recent_status(
					isi_cloud::task::CBM,
					isi_cbm_error_get_type(consumer_error));
			else if (isi_error_is_a(consumer_error,
									ISI_SYSTEM_ERROR_CLASS))
				retrieved_task->set_most_recent_status(
					isi_cloud::task::SYSTEM,
					isi_system_error_get_error(
						(const struct isi_system_error *)
							consumer_error));
			else
				retrieved_task->set_most_recent_status(
					isi_cloud::task::OTHER,
					0); /* status value is unused here */
		}

		/*
		 * Free any error from the consumer function here - we've
		 * already logged it in run_consumer_function and have used it
		 * to set the task_process_completion_reason.
		 */
		isi_error_free(consumer_error);
	}

	/* Persist the updated version of the task to disk. */
	retrieved_task->get_key()->get_serialized_key(serialized_key,
												  serialized_key_size);

	retrieved_task->get_saved_serialization(old_task_serialization,
											old_task_serialization_size);
	retrieved_task->get_serialized_task(new_task_serialization,
										new_task_serialization_size);

	if (retrieved_task->get_op_type()->uses_hsbt())
	{
		hsbt_cond_mod_entry(inprogress_sbt_fd, serialized_key,
							serialized_key_size, new_task_serialization,
							new_task_serialization_size, NULL, BT_CM_BUF,
							old_task_serialization, old_task_serialization_size, NULL,
							&processing_error);
	}
	else
	{
		ASSERT(serialized_key_size == sizeof(btree_key_t),
			   "expected btree_key_t (size: %zu != %zu)",
			   serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
			static_cast<const btree_key_t *>(serialized_key);
		ASSERT(bt_key != NULL);

		if (ifs_sbt_cond_mod_entry(inprogress_sbt_fd,
								   const_cast<btree_key_t *>(bt_key), new_task_serialization,
								   new_task_serialization_size, NULL, BT_CM_BUF,
								   old_task_serialization, old_task_serialization_size,
								   NULL) != 0)
		{
			processing_error = isi_system_error_new(errno,
													"failed to modify task (%{}) "
													"with updated status information",
													cpool_task_fmt(retrieved_task));
		}
	}
	ON_ISI_ERROR_GOTO(out, processing_error);

out:
	if (task_locked)
		tm->unlock_task(t->get_key(), isi_error_suppress(IL_ERR));

	if (retrieved_task != NULL)
		delete retrieved_task;

	isi_error_handle(processing_error, error_out);
}

uint32_t
task_processing_thread_pool::get_max_task_reads(void) const
{
	struct isi_error *error = NULL;

	/*
	 * Default to 10 (to be used if there is an error reading from
	 * gconfig).
	 */
	uint32_t max_tasks = 10;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
			reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		max_tasks = config_settings->get_max_start_task_reads();
	}

out:
	if (error != NULL)
	{
		ilog(IL_INFO,
			 "failed to get max start_task reads, "
			 "using default of %" PRIu32 ": %#{}",
			 max_tasks, isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;
	}

	return max_tasks;
}

float task_processing_thread_pool::get_sleep_interval(task_type type, bool recovery,
													  bool tasks_remain)
{
	struct isi_error *error = NULL;
	const struct cpool_d_thread_intervals *sleep_intervals = NULL;

	/*
	 * Set a default interval of 30 seconds to be used if there is an error
	 * reading the configuration from gconfig.
	 */
	float ret_interval = 30.0;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
			reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		sleep_intervals = config_settings->get_thread_intervals(type);

		if (recovery)
		{
			ret_interval = tasks_remain ? sleep_intervals->recovery_with_tasks_remaining : sleep_intervals->recovery_without_tasks_remaining;
		}
		else
		{
			ret_interval = tasks_remain ? sleep_intervals->normal_with_tasks_remaining : sleep_intervals->normal_without_tasks_remaining;
		}
	}

out:
	if (error != NULL)
	{
		/* ilog doesn't handle floats */
		char float_str[64];
		snprintf(float_str, sizeof float_str, "%f", ret_interval);

		ilog(IL_INFO,
			 "failed to get sleep interval, "
			 "using default of %s seconds: %#{}",
			 float_str, isi_error_fmt(error));
		isi_error_free(error);
	}

	return ret_interval;
}

static void
pre_finish_task(const isi_cloud::task *task, server_task_map *tm,
				task_process_completion_reason reason, struct isi_error **error_out)
{
	ASSERT(task != NULL);

	const int max_gconfig_conflict_attempts = 5;
	struct isi_error *error = NULL;
	bool task_locked = false;

	/* block: true, exclusive: true */
	tm->lock_task(task->get_key(), true, true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	task_locked = true;

	switch (task->get_op_type()->get_type())
	{
	case CPOOL_TASK_TYPE_RESTORE_COI:
		if (reason == CPOOL_TASK_SUCCESS)
		{
			finish_adding_eligible_pending_guids(
				max_gconfig_conflict_attempts, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * A restore_coi task failed, cancel the guid addition of the
			 * associated task
			 */
		}
		else if (reason == CPOOL_TASK_NON_RETRYABLE_ERROR)
		{

			const isi_cloud::restore_coi_task *rcoi_task =
				dynamic_cast<const isi_cloud::restore_coi_task *>(
					task);

			ASSERT(rcoi_task != NULL);

			cancel_guid_addition(
				rcoi_task->get_account_id().cluster_.c_str(),
				max_gconfig_conflict_attempts, &error);
			ON_ISI_ERROR_GOTO(out, error);
		}
		break;
	case CPOOL_TASK_TYPE_WRITEBACK:
	case CPOOL_TASK_TYPE_CLOUD_GC:
		/*
		 * If this task won't be repeated, check for guids pending
		 * removal
		 */
		if (reason != CPOOL_TASK_RETRYABLE_ERROR)
		{
			finish_disabling_eligible_guids(
				max_gconfig_conflict_attempts, &error);
			ON_ISI_ERROR_GOTO(out, error);
		}
		break;
	default:
		break;
	}
out:

	if (task_locked)
		tm->unlock_task(task->get_key(), isi_error_suppress(IL_ERR));

	isi_error_handle(error, error_out);
}

void *
task_processing_thread_pool::start_thread(void *arg)
{
	sigset_t s;
	djob_op_job_task_state cached_task_state;
	isi_cloud::task *task = NULL;
	struct isi_error *error = NULL;
	bool recovery, tasks_remain;
	task_process_completion_reason reason;
	float sleep_interval;
	server_task_map::read_task_ctx *start_task_ctx = NULL;

	thread_args *ta = static_cast<thread_args *>(arg);
	ASSERT(ta != NULL);

	task_processing_thread_pool *tp = ta->tp;
	ASSERT(tp != NULL);

	server_task_map *tm = tp->task_map_;
	ASSERT(tm != NULL);

	recovery = ta->recovery;

	ilog(IL_NOTICE, "starting %s thread for type %{}",
		 recovery ? "recovery" : "normal",
		 task_type_fmt(tm->get_op_type()->get_type()));

	/*
	 * To prevent log spam, errors that can log at or above IL_INFO will do
	 * so only once in the period defined here.  (Well, "once-ish", see
	 * THROTTLED_ILOG above.)
	 */
	struct timeval log_delay = {
		.tv_sec = 10,
		.tv_usec = 0};

	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	while (!tp->is_shutdown_pending())
	{
		tasks_remain = false;

		if (fs_is_fully_accessible())
		{
			int max_task_reads = tp->get_max_task_reads();
			task = tm->start_task(start_task_ctx, max_task_reads,
								  recovery, cached_task_state, &error);
			if (error != NULL &&
				isi_system_error_is_a(error, EAGAIN))
			{
				/*
				 * EAGAIN means that there could be some
				 * uninspected tasks.
				 */
				tasks_remain = true;

				isi_error_free(error);
				error = NULL;

				goto sleep;
			}
			ON_ISI_ERROR_GOTO(sleep, error);
		}

		if (task != NULL)
		{
			ilog(IL_DEBUG, "found a %{} task to start (%{})",
				 djob_op_job_task_state_fmt(cached_task_state),
				 cpool_task_fmt(task));

			tasks_remain = true;

			/*
			 * A started task isn't necessarily one that should be
			 * processed, so bypass the processing of a non-running
			 * task and go directly to finishing it, supplying the
			 * appropriate reason for doing so.  Note that we also
			 * have to account for paused tasks that are started
			 * (consider the case where a task is recovered but was
			 * paused before the recovery).
			 */
			if (cached_task_state == DJOB_OJT_CANCELLED)
				reason = CPOOL_TASK_CANCELLED;
			else if (cached_task_state == DJOB_OJT_PAUSED)
				reason = CPOOL_TASK_PAUSED;
			else if (cached_task_state == DJOB_OJT_RUNNING)
			{
				task_ckpt_helper tch(tm, task);
				process_task(tm, &tch, task, reason, &error);
			}

			/*
			 * If an error occurred while processing the task (vs.
			 * in the consumer function), ignore the reason set by
			 * process_task and treat as a retryable error.
			 */
			if (error != NULL)
			{
				bool retryable = is_retryable(error);
				THROTTLED_ILOG(error,
							   get_loglevel(error, retryable),
							   IL_DEBUG, log_delay,
							   "error processing task (%{}), "
							   "will retry: %#{}",
							   cpool_task_fmt(task),
							   isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;

				reason = CPOOL_TASK_RETRYABLE_ERROR;
			}

			pre_finish_task(task, tm, reason, &error);
			if (error != NULL)
			{
				bool retryable = is_retryable(error);
				THROTTLED_ILOG(error,
							   get_loglevel(error, retryable),
							   IL_DEBUG, log_delay,
							   "error from pre_finish_task (%{}), "
							   "will retry: %#{}",
							   cpool_task_fmt(task),
							   isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;

				reason = CPOOL_TASK_RETRYABLE_ERROR;
			}

			tm->finish_task(task->get_key(), reason, &error);
			ON_ISI_ERROR_GOTO(sleep, error);
		}

	sleep:
		sleep_interval = tp->get_sleep_interval(
			tm->get_op_type()->get_type(), recovery, tasks_remain);

		if (error != NULL)
		{
			bool retryable = is_retryable(error);
			if (task != NULL)
			{
				THROTTLED_ILOG(error,
							   get_loglevel(error, retryable),
							   IL_DEBUG, log_delay,
							   "task (%{}) processing failure: %#{}",
							   cpool_task_fmt(task),
							   isi_error_fmt(error));
			}
			else
			{
				THROTTLED_ILOG(error,
							   get_loglevel(error, retryable),
							   IL_DEBUG, log_delay,
							   "task (<none>) processing failure: %#{}",
							   isi_error_fmt(error));
			}

			isi_error_free(error);
			error = NULL;
		}

		if (task != NULL)
		{
			delete task;
			task = NULL;
		}

		tp->sleep_for(sleep_interval);
	}

	if (start_task_ctx != NULL)
	{
		delete start_task_ctx;
		start_task_ctx = NULL;
	}

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(NULL);
}
