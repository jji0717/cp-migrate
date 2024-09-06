#ifndef __ISI_CPOOL_D_THREADPOOL_H__
#define __ISI_CPOOL_D_THREADPOOL_H__

#include <pthread.h>

#include <map>

class thread_context;

typedef std::map<pthread_t, thread_context*> thread_context_map;

/**
 * A generic thread pool class.
 */
class thread_pool
{
private:
	bool				shutdown_pending_;
	mutable pthread_mutex_t		pool_lock_;
	static pthread_cond_t		s_notification_;
	thread_context_map		thread_data_;

	/**
	 * Returns the context associated with the current thread or NULL if no
	 * such context exists.  Caller must hold pool_lock_ prior to calling
	 * this function.
	 */
	thread_context *get_thread_context();

protected:
	int				mastership_lock_fd_;

	/* Copy and assignment aren't allowed. */
	thread_pool(const thread_pool&);
	thread_pool operator=(const thread_pool&);

	/**
	 * Sleep for the specified duration.  See sleep_until().
	 */
	bool sleep_for(const float &duration_in_seconds);
	bool sleep_for(const struct timeval &duration);

	/**
	 * Sleep until the specified time.
	 *
	 * @param[in]  wakeup_time	thread will sleep until this time,
	 * unless interrupted by thread pool state change
	 *
	 * @return false if thread was interrupted, else true
	 */
	bool sleep_until(const struct timeval &wakeup_time);

	/**
	 * This routing block SIGTERM
	 * 
	 * @author ssasson (4/30/2014)
	 */
	static void block_sigterm();

	/**
	 * Releases mastership lock if it was taken
	 */
	void cleanup_mastership();

	/**
	 * Blocks until able to acquire mastership for the given lockfile_name
	 * and masterfile_name.  Only one node per cluster can attain mastership
	 * at any given time
	 *
	 * @param lockfile[in]	path to the file an exclusive lock is taken on.
	 * @param masterfile[in]	path to the file where node id of the
	 * master is written
	 * @param sleep_interval[in]	Number of seconds to sleep between calls
	 * to get mastership
	 */
	/////如果不能获得指定lockfile_name和masterfile__name的主控权,就阻塞.
	//每个cluster中就一个node可以获得主控权
	void wait_for_mastership(const char *lockfile_name,
	    const char *masterfile_name, int sleep_interval);

public:
	thread_pool();
	virtual ~thread_pool();

	/**
	 * Add a thread to this pool.  Never throws.
	 * @param[in]  attr		an initialized and configured pthread
	 * attribute set; this set will not be destroyed and can be NULL
	 * @param[in]  start_func	the entry point of the new thread
	 * @param[in]  start_func_arg	the parameter passed to start_func, can
	 * be NULL
	 *
	 * @return			errno
	 */
	int add_thread(const pthread_attr_t *attr, void *(*start_func)(void *),
	    void *start_func_arg);

	/**
	 * Shuts down this thread_pool.  Never throws.
	 */
	virtual void shutdown(void);

	/**
	 * @return true if shutdown is pending, else false
	 */
	bool is_shutdown_pending(void) const;

	/**
	 * Notify this thread_pool of some event (e.g. configuration change).
	 */
	void notify(void);
};

class thread_context {
protected:
	/* Copy and assignment aren't allowed. */
	thread_context(const thread_context&);
	thread_context operator=(const thread_context&);

public:
	thread_context() : notification_pending_(false) {};

	bool notification_pending_;
};


/**
 * Can CPOOL process tasks/ sbt entries?
 * Basically during shutdown the FS is put to read only.
 * then when nodes are rebooted we lose quorum and the FS
 * becomes unavailable. In this situation we should not do any
 * processing. The symptom was that we were flooding the log
 * in this use case since we were trying to access the FS.
 */
bool
fs_is_fully_accessible(void);

unsigned int
get_loglevel(const struct isi_error *error, bool retryable,
    ifs_lin_t lin = INVALID_LIN, ifs_snapid_t snapid = INVALID_SNAPID);

#endif // __ISI_CPOOL_D_THREADPOOL_H__
