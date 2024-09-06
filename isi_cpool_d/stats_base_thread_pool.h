#ifndef __STATS_BASE_THREAD_POOL_H__
#define __STATS_BASE_THREAD_POOL_H__

#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_stats/cpool_stat_manager.h>

#include "thread_pool.h"

/**
 * A base class for all cpool stats thread pools.  Encapsulates logic for
 * managing an instance of cpool_stat_manager
 */
class stats_base_thread_pool : public thread_pool
{
protected:

	/**
	 * Returns local copy of cpool_stat_manager.  If it doesn't exist,
	 * stat_manager_ will be created.
	 *
	 * @param error_out  Standard Isilon error_out parameter.
	 * @return  Local instance of cpool_stat_manager.  If *error_out is
	 *          populated, returns NULL
	 */
	cpool_stat_manager *get_stat_manager(struct isi_error **error_out);

	/**
	 * Frees instance of stat_manager_, allowing it to be recreated on next
	 * call to get_stat_manager
	 */
	void clean_stat_manager();

	/**
	 * Internal implementation of thread "action".  Called by do_action.
	 * Must be overridden by descendant classes.
	 *
	 * @error_out  Standard Isilon error_out parameter.
	 */
	virtual void do_action_impl(struct isi_error **error_out) = 0;

	/**
	 * Thin wrapper around do_action_impl.  Calls do_action_impl and, if
	 * there is an error, refreshes the instance of stat_manager_ and tries
	 * again to guard against failures by a stale stat_manager_.  Stale
	 * stat_manager_ issues can happen if resources pointed to by the
	 * instance are modified outside of normal operation.  (E.g., a user
	 * deletes a database file).
	 *
	 * Note* No errors are returned by this function.  Stats operations are
	 * not allowed to return failure, so any errors are internally logged
	 * and destroyed.
	 */
	void do_action();

	/**
	 * The callback executed when a thread is started.
	 *
	 * @param arg  A void* passed in by the thread creator.  In this case,
	 *             arg is an instance of stats_base_thread_pool
	 */
	static void *thread_callback(void *arg);

	/**
	 * Returns an appropriate default sleep interval (in seconds) for this
	 * instance.  The default value will only be used when there is a
	 * failure to read a "correct" value from gconfig.
	 * MUST be overridden by descendants
	 *
	 * @return the default sleep interval for this instance
	 */
	virtual float get_default_sleep_interval() const = 0;

	/**
	 * Returns the configured sleep interval for this instance from gconfig
	 * cpool_d_config_settings structure.  Effectively, this way that
	 * derived classes indicate where sleep values are stored in gconfig
	 * structure
	 * MUST be overridden by descendants.
	 *
	 * @param config_settings  A reference to the cpool_d_config_settings
	 *                         instance in which sleep values are stored
	 * @return  The sleep value for this instance as indicated in
	 * config_settings
	 */
	virtual float get_sleep_interval_from_config_settings(
	    const cpool_d_config_settings &config_settings) const = 0;

	/**
	 * Called before 'while' loop in thread execution starts (e.g., before
	 * do_action/do_action_impl are ever called).  Can be used to do any
	 * extra initialization before starting thread execution.
	 */
	virtual void do_thread_startup_initialization() {};

	/**
	 * Called after 'while' loop exits during a shutdown (e.g., after
	 * do_action/do_action_impl have been called for the last time).  Can
	 * be used to do any extra cleanup just before thread exits.
	 */
	virtual void do_thread_shutdown_cleanup() {};

private:
	cpool_stat_manager *stat_manager_;

	/**
	 * Fetches an appropriate sleep interval (in seconds) for the period
	 * between thread actions.  Relies on get_default_sleep_interval and
	 * get_sleep_interval_from_config_settings to determine correct value
	 * for any given base class
	 *
	 * @return an appropriate sleep interval
	 */
	float get_sleep_interval() const;

public:
	stats_base_thread_pool();
	~stats_base_thread_pool();

	/**
	 * Create worker threads.
	 */
	void start_threads(int num_threads);
};

#endif // __STATS_BASE_THREAD_POOL_H__
