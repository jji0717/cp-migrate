#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <stdio.h>

#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cbm_util.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_gmp/isi_gmp.h>


#include "thread_mastership.h"
#include "thread_pool.h"

pthread_cond_t thread_pool::s_notification_ = PTHREAD_COND_INITIALIZER;

thread_pool::thread_pool()
	: shutdown_pending_(false), mastership_lock_fd_(-1)
{
	pthread_mutex_init(&pool_lock_, NULL);
}

thread_pool::~thread_pool()
{
	shutdown();
	cleanup_mastership();
	pthread_mutex_destroy(&pool_lock_);
}

thread_context *
thread_pool::get_thread_context()
{
	pthread_t tid = pthread_self();
	thread_context_map::iterator iter = thread_data_.find(tid);

	if (iter == thread_data_.end())
		return NULL;

	ASSERT (iter->second != NULL);

	return iter->second;
}

int
thread_pool::add_thread(const pthread_attr_t *attr,
    void *(*start_func)(void *), void *start_func_arg)
{
	// attr can be NULL
	ASSERT(start_func != NULL);
	// start_func_arg can be NULL

	int ret = 0, result;
	pthread_t new_thread;

	scoped_lock sl(pool_lock_);

	if (shutdown_pending_) {
		/* New threads cannot be added if a shutdown is pending. */
		ret = EBUSY;
		goto out;
	}

	result = pthread_create(&new_thread, attr, start_func,
	    start_func_arg);
	if (result == 0) {
		/*
		 * Upon successfully adding the new thread, add its entry to
		 * the thread data map.
		 */
		ASSERT(thread_data_.find(new_thread) == thread_data_.end(),
		    "thread (%p)", new_thread);

		thread_context *context = new thread_context();
		ASSERT(context != NULL);

		thread_data_[new_thread] = context;
	}
	else
		ret = errno;

 out:
	return ret;
}

void
thread_pool::shutdown(void)
{
	{
		scoped_lock sl(pool_lock_);

		if (shutdown_pending_)
			return;

		ilog(IL_NOTICE, "thread pool shutting down");

		shutdown_pending_ = true;
		pthread_cond_broadcast(&s_notification_);
	}

	/*
	 * We can't take pool_lock_ here because threads that are just starting
	 * to sleep may contend which would cause a deadlock (see sleep_until).
	 * Instead, we guard against modifying thread_data_map_ if
	 * shutdown_pending_ is true (see add_thread).
	 */

	thread_context_map::iterator iter = thread_data_.begin();
	for(; iter != thread_data_.end(); ++iter) {
		pthread_join(iter->first, NULL);
		delete iter->second;
		iter->second = NULL;
	}

	thread_data_.clear();
}

bool
thread_pool::is_shutdown_pending(void) const
{
	scoped_lock sl(pool_lock_);

	bool shutdown_pending = shutdown_pending_;

	return shutdown_pending;
}

void
thread_pool::notify(void)
{
	bool broadcast_needed = false;
	scoped_lock sl(pool_lock_);

	thread_context_map::iterator it;
	for (it = thread_data_.begin(); it != thread_data_.end(); ++it) {
		ASSERT(it->second != NULL, "thread: %p", it->first);

		if (!it->second->notification_pending_) {
			broadcast_needed = true;
			it->second->notification_pending_ = true;
		}
	}

	if (broadcast_needed)
		pthread_cond_broadcast(&s_notification_);
}

bool
thread_pool::sleep_for(const float &duration_in_seconds)
{
	if (duration_in_seconds < 0) {
		/* ASSERT can't handle floats */
		char float_str[64];
		snprintf(float_str, sizeof float_str, "%f",
		    duration_in_seconds);

		ASSERT(duration_in_seconds >= 0,
		    "duration_in_seconds: %s", float_str);
	}

	/* This should handle durations of up to 9.2 trillion seconds. */
	int64_t int_duration = duration_in_seconds * 1000000;

	struct timeval duration = {
		.tv_sec = int_duration / 1000000,
		.tv_usec = int_duration % 1000000
	};

	return sleep_for(duration);
}

bool
thread_pool::sleep_for(const struct timeval &duration)
{
	static struct timeval epoch;
	timerclear(&epoch);

	/* Make sure duration is non-negative. */
	ASSERT(timercmp(&duration, &epoch, >=),
	    "duration: %ld.%06ld", duration.tv_sec, duration.tv_usec);

	struct timeval now;
	gettimeofday(&now, NULL);

	struct timeval wakeup_time;
	timeradd(&now, &duration, &wakeup_time);

	return sleep_until(wakeup_time);
}

bool
thread_pool::sleep_until(const struct timeval &wakeup_time)
{
	int wait_ret = 0;

	scoped_lock sl(pool_lock_);

	thread_context *context = get_thread_context();
	ASSERT(context != NULL);

	struct timespec wakeup_time_ns;
	TIMEVAL_TO_TIMESPEC(&wakeup_time, &wakeup_time_ns);

	/*
	 * pthread_cond_timedwait() can erroneously return, so we need to check
	 * the return value and whether or not there is a pending shutdown or
	 * notification before continuing.
	 */
	while (!shutdown_pending_ && !context->notification_pending_ &&
	    wait_ret != ETIMEDOUT) {
		wait_ret = pthread_cond_timedwait(&s_notification_,
		    &pool_lock_, &wakeup_time_ns);
		ASSERT(wait_ret == 0 || wait_ret == ETIMEDOUT,
		    "unhandled error returned "
		    "from pthread_cond_timedwait: [%d] %s",
		    wait_ret, strerror(wait_ret));
	}

	if (context->notification_pending_)
		context->notification_pending_ = false;

	return (wait_ret == ETIMEDOUT);
}

void
thread_pool::block_sigterm()
{
	/*
	 * Block SIGTERM in this thread.  When isi_cpool_d receives SIGTERM, it
	 * will use s_shutdown to signal the threads to exit.
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);
}

void
thread_pool::cleanup_mastership()
{
	if (mastership_lock_fd_ != -1) {
		close(mastership_lock_fd_);
		mastership_lock_fd_ = -1;
	}
}

void
thread_pool::wait_for_mastership(const char *lockfile,
    const char *masterfile, int sleep_interval)
{
	const unsigned int buffer_size = 512;
	const unsigned int lockdir_len = strlen(CPOOL_LOCKS_DIRECTORY);

	ASSERT(mastership_lock_fd_ == -1);
	ASSERT(strlen(lockfile) + lockdir_len < buffer_size);
	ASSERT(strlen(masterfile) + lockdir_len < buffer_size);

	char lockfile_buffer[buffer_size];
	char masterfile_buffer[buffer_size];

	strcpy(lockfile_buffer, CPOOL_LOCKS_DIRECTORY);
	strcpy(masterfile_buffer, CPOOL_LOCKS_DIRECTORY);

	strcpy(lockfile_buffer + lockdir_len, lockfile);
	strcpy(masterfile_buffer + lockdir_len, masterfile);

	/* Try to acquire mastership */
	while (!is_shutdown_pending()) {

		if (acquire_mastership(&mastership_lock_fd_, lockfile_buffer,
			masterfile_buffer))  ////////如果获得了mastership,返回true.否则,返回false
			break;

		sleep_for(sleep_interval);/////没拿到mastership,就sleep.默认是60s
	}
}

#define SYSCTL_NAME_SHUTDOWN_READ_ONLY              "efs.gmp.shutdown_read_only"

static bool
fs_is_shutdown_read_only(struct isi_error **error_out)
{
        int read_only = 0;
	size_t lread_only;
	struct isi_error *error = NULL;

	lread_only = sizeof(read_only);
	if (sysctlbyname(SYSCTL_NAME_SHUTDOWN_READ_ONLY, &read_only, &lread_only,
	    NULL, 0) != 0) {

		read_only = 0;

		/*
		 * The shutdown_read_only sysctl doesn't exist on accelerator
		 * nodes, so we could get back an ENOENT here.  That's ok.  We
		 * always want accelerator nodes to return false, so we can
		 * safely ignore the ENOENT.  Otherwise, we need to return an
		 * error
		 */
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Failed checking shutdown_read_only state");
		}

		goto out;
	}

out:
	isi_error_handle(error, error_out);

	return read_only == 1;
}

bool fs_is_fully_accessible(void)
{
	struct isi_error *error = NULL;
	bool has_quorum = false;
	bool has_sb_quorum = false;
	bool is_shutdown_read_only = false;
	bool is_fully_accessible = true;

	has_quorum = gmp_has_quorum(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!has_quorum)
		goto out;

	has_sb_quorum = gmp_has_sb_quorum(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!has_sb_quorum)
		goto out;

	is_shutdown_read_only = fs_is_shutdown_read_only(&error);
	ON_ISI_ERROR_GOTO(out, error);

	is_fully_accessible = (has_quorum && has_sb_quorum && (!is_shutdown_read_only));

 out:
	 if (error != NULL) {
		 ilog(IL_ERR,
		     "fs_is_fully_accessible failed:  %#{}",
		     isi_error_fmt(error));
		 isi_error_free(error);
	 }

	 return is_fully_accessible;
}


unsigned int
get_loglevel(const struct isi_error *error, bool retryable, ifs_lin_t lin,
    ifs_snapid_t snapid)
{
	if (isi_cbm_error_is_a(error, CBM_DOMAIN_LOCK_CONTENTION) ||
	    isi_cbm_error_is_a(error, CBM_SUCCESS) ||
	    isi_cbm_error_is_a(error, CBM_CLAPI_ABORTED_BY_CALLBACK) ||
	    isi_cbm_error_is_a(error, CBM_INVALIDATE_FILE_DIRTY) ||
	    isi_cbm_error_is_a(error, CBM_STALE_STUB_ERROR) ||
	    isi_cbm_error_is_a(error, CBM_LIN_EXISTS) ||
	    isi_cbm_error_is_a(error, CBM_FILE_TRUNCATED) ||
	    isi_cbm_error_is_a(error, CBM_ALREADY_STUBBED) ||
	    isi_cbm_error_is_a(error, CBM_SYNC_ALREADY_IN_PROGRESS) ||
	    isi_cbm_error_is_a(error, CBM_LIN_DOES_NOT_EXIST) ||
	    isi_cbm_error_is_a(error, CBM_NOT_IN_PERMIT_LIST) ||
	    isi_cbm_error_is_a(error, CBM_RESTRICTED_MODIFY) ||
	    ((isi_cbm_error_is_a(error, CBM_READ_ONLY_ERROR) ||
		isi_system_error_is_a(error, EROFS)) &&
		    (!fs_is_fully_accessible() ||
			linsnap_is_write_restricted(lin, snapid))) ||
	    (isi_system_error_is_a(error, EIO) &&
		(!gmp_has_quorum(isi_error_suppress()) ||
		    !gmp_has_sb_quorum(isi_error_suppress())))) {
		return IL_DEBUG;
	} else {
		if (retryable) {
			return IL_INFO;
		} else {
			return IL_ERR;
		}
	}

}
