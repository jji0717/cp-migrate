#ifndef __TIME_BASED_SCANNING_THREAD_POOL_H__
#define __TIME_BASED_SCANNING_THREAD_POOL_H__

#include <signal.h>

#include <set>

#include <memory>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

#include "thread_pool.h"

struct cpool_d_index_scan_thread_intervals;
class cpool_d_config_settings;

template<class index_type, class entry_type, class item_type>
class tbi_scanning_thread_pool : public thread_pool
{
protected:
	/* Copy and assignment are not allowed. */
	tbi_scanning_thread_pool(const tbi_scanning_thread_pool &);
	tbi_scanning_thread_pool &operator=(const tbi_scanning_thread_pool &);

	float get_sleep_interval(bool get_short_interval) const;

	void process_entry(entry_type *entry, index_type &index, //////void process_entry前不能加virtual。 虚函数不能被模板化
	    struct timeval failed_item_retry_time);

	virtual std::shared_ptr<index_type> get_index(
	    struct isi_error **error_out) const = 0;
	virtual const struct cpool_d_index_scan_thread_intervals *
	    get_sleep_intervals(const cpool_d_config_settings *) const = 0;
	virtual void process_item(const item_type &item,
	    struct isi_error **error_out) const = 0;
	virtual bool is_retryable(const struct isi_error *error) const;
	virtual bool can_process_entries(struct isi_error **error_out) const;

public:
	tbi_scanning_thread_pool();
	virtual ~tbi_scanning_thread_pool();

	void start_threads(int num_threads);

	void do_scan(void);
};

/*
 *
 * TEMPLATE IMPLEMENTATIONS
 *
 */

template<class index_type, class entry_type, class item_type>
void *start_thread(void *arg);

template<class index_type, class entry_type, class item_type>
tbi_scanning_thread_pool<index_type, entry_type, item_type>::
    tbi_scanning_thread_pool()
{
}

template<class index_type, class entry_type, class item_type>
tbi_scanning_thread_pool<index_type, entry_type, item_type>::
    ~tbi_scanning_thread_pool()
{
	shutdown();
}

template<class index_type, class entry_type, class item_type>
void
tbi_scanning_thread_pool<index_type, entry_type, item_type>::start_threads(
    int num_threads)
{
	ASSERT(num_threads > 0, "num_threads: %d", num_threads);

	for (int i = 0; i < num_threads; ++i)
		add_thread(NULL,
		    start_thread<index_type, entry_type, item_type>, this);
}

template<class index_type, class entry_type, class item_type>
void*
start_thread(void *arg)
{
	ASSERT(arg != NULL);

	tbi_scanning_thread_pool<index_type, entry_type, item_type> *tp =
	    static_cast<
	    tbi_scanning_thread_pool<index_type, entry_type, item_type> *
	    >(arg);
	ASSERT(tp != NULL);

	tp->do_scan();

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(NULL);
}

template<class index_type, class entry_type, class item_type>
bool
tbi_scanning_thread_pool<index_type, entry_type, item_type>::can_process_entries(
    isi_error **error_out) const
{
	return fs_is_fully_accessible();
}

template<class index_type, class entry_type, class item_type>
void
tbi_scanning_thread_pool<index_type, entry_type, item_type>::do_scan(void)
{
	struct isi_error *error = NULL;

	std::shared_ptr<index_type> index;
	entry_type *entry = NULL;
	struct timeval start_time, *start_time_ptr = NULL;
	struct timeval failed_items_retry_time = {0};
	bool more_eligible_entries;
	float sleep_interval;

	index = get_index(&error);
	if (error != NULL) {
		ilog(get_loglevel(error, false),
		    "thread exiting due to initialization failure: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		pthread_exit(NULL);
	}

	/*
	 * Block SIGTERM and SIGHUP in this thread as they are handled
	 * elsewhere.
should this be done higher up?
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGHUP);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	while (!is_shutdown_pending()) {
		bool ok_to_get_entry = false;

		if (start_time_ptr == NULL) {
			gettimeofday(&start_time, NULL);
			start_time_ptr = &start_time;  /////先初始给start_time_prt当前时间

			/*
			 * The retry time for failed items MUST be later than
			 * start_time.
			 */
			failed_items_retry_time.tv_sec = start_time.tv_sec + 1;
			failed_items_retry_time.tv_usec = start_time.tv_usec;
		}

		more_eligible_entries = false;

		ok_to_get_entry = can_process_entries(&error);
		if (error != NULL) {
			ilog(get_loglevel(error, false),
			    "can_process_entries failed: %#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;

			ok_to_get_entry = false;
		}

		if (ok_to_get_entry)
			////////从kernel中找比当前时间更早的tbi entry
			entry = index->get_entry_before_or_at(start_time.tv_sec,
			    more_eligible_entries, &error);

		if (error != NULL) {
			ASSERT(entry == NULL);

			/*
			 * In case of an error, sleep for a short period of
			 * time before trying again.
			 */
			sleep_interval = get_sleep_interval(true);

			ilog(get_loglevel(error, false),
			    "get_entry_before_or_at failed: %#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}
		else if (entry != NULL) {
			/*
			 * If an entry was retrieved, process it, destroy it,
			 * and sleep for a short period of time before looking
			 * for the next entry.  Note that the retrieved entry
			 * was locked by get_entry_before_or_at() before being
			 * returned.
			 */
			sleep_interval = get_sleep_interval(true);

			process_entry(entry, *index, failed_items_retry_time);
			/* Note: entry is now unlocked. */

			delete entry;
			entry = NULL;
		}
		else if (more_eligible_entries) {
			/*
			 * This thread failed to determine that there are no
			 * more eligible entries to process, so sleep for a
			 * short period of time before looking for another
			 * entry.
			 */
			sleep_interval = get_sleep_interval(true);
		}
		else {
			/*
			 * No entries exist that are eligible for processing at
			 * this time.  Before sleeping for a long period of
			 * time, prepare the reset of the start time.
			 */
			sleep_interval = get_sleep_interval(false);
			start_time_ptr = NULL;
		}

		sleep_for(5/*sleep_interval*/);////jji test
	}
}

template<class index_type, class entry_type, class item_type>
float
tbi_scanning_thread_pool<index_type, entry_type, item_type>::
    get_sleep_interval(bool get_short_interval) const
{
	struct isi_error *error = NULL;
	const struct cpool_d_index_scan_thread_intervals
	    *sleep_intervals = NULL;

	/*
	 * Set a default interval of 30 seconds to be used if there is an error
	 * reading the configuration.
	 */
	float ret_interval = 30.0;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
		    reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);
		ASSERT(config_settings != NULL);

		sleep_intervals = get_sleep_intervals(config_settings);
		ASSERT(sleep_intervals != NULL);

		ret_interval =
		    get_short_interval ?
		    sleep_intervals->with_skipped_locked_entries :
		    sleep_intervals->without_skipped_locked_entries;
	}

 out:
	if (error != NULL) {
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

template<class index_type, class entry_type, class item_type>
void //////对于ooi_scanning_thread_pool而言,process_entry(ooi_entry,ooi,timeval)
tbi_scanning_thread_pool<index_type, entry_type, item_type>::process_entry(
    entry_type *entry, index_type &index, struct timeval failed_item_retry_time)
{
	ASSERT(entry != NULL);

	struct isi_error *error = NULL;
	std::set<item_type> items;
	typename std::set<item_type>::const_iterator iter;

	entry_type failed_items_entry;
	failed_items_entry.set_process_time(failed_item_retry_time);

	items = entry->get_items();
	for (iter = items.begin(); iter != items.end(); ++iter) {
		process_item(*iter/*isi_cloud_object_id*/, &error);
		if (error != NULL) {
			bool retryable = is_retryable(error);
			uint32_t log_level = get_loglevel(error, retryable);

			/*
			 * ENOENT is NOT retryable so log_level will be
			 * IL_ERR.  We do not want to spam the log
			 * file with this in case someone deleted the
			 * file on the side.
			 */
			if (isi_system_error_is_a(error, ENOENT)) {
				log_level = IL_INFO;
			}

			ilog(log_level,
			    "failed to process item (%{}), will %sretry: %#{}",
			    iter->get_print_fmt(), retryable ? "" : "NOT ",
			    isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;

			if (retryable) {
				/*
				 * Make sure the failed item is retried.  Any
				 * error returned signifies a bug, thus the use
				 * of ASSERT.
				 */
				failed_items_entry.add_item(*iter, &error);
				ASSERT(error == NULL, "unexpected error: %#{}",
				    isi_error_fmt(error));
			}
		}
	}

	index.remove_entry_and_handle_failures(entry, &failed_items_entry,
	    isi_error_suppress(IL_NOTICE));
	index.unlock_entry(entry, &error);
	if (error != NULL) {
		ilog(IL_ERR, "failed to unlock entry: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
}

template<class index_type, class entry_type, class item_type>
bool
tbi_scanning_thread_pool<index_type, entry_type, item_type>::is_retryable(
    const struct isi_error *error) const
{
	/* All errors are retryable ... */
	bool retryable = true;

	/*
	 * ... unless they do not exist,
	 * or are ENOENT (file doesn't exist)
	 * or EINVAL (file isn't stubbed)
	 */
	if ((error == NULL) ||
	    isi_system_error_is_a(error, ENOENT) ||
	    isi_system_error_is_a(error, EINVAL)) {
		retryable = false;
	}

	return retryable;
}

#endif // __TIME_BASED_SCANNING_THREAD_POOL_H__
