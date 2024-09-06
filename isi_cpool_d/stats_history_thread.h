#ifndef __STATS_HISTORY_THREAD_H__
#define __STATS_HISTORY_THREAD_H__

#include "stats_base_thread_pool.h"

const int STATS_HISTORY_THREAD_DEFAULT_SLEEP = 300;

/**
 * stats_history thread is the thread responsible for populating
 * the database for every five minutes data points.
 * It is also responsible of cleaning the old data base entries.
 * There is only one active thread like that in the cluster.
 * That is why acquire_mastership makes sure that there is only
 * one thread in the cluster runninfg that does the history.
 * The rest are on stand by.
 *
 * @author ssasson (4/30/2014)
 */
class stats_history_thread : public stats_base_thread_pool/////一个cluster中只有一个stats_history_thread
{
protected:
	/* See comments in base class */
	float get_default_sleep_interval() const
	{
		return STATS_HISTORY_THREAD_DEFAULT_SLEEP;
	};

	/* See comments in base class */
	float get_sleep_interval_from_config_settings(
	    const cpool_d_config_settings &config_settings) const
	{
		return config_settings.get_history_stats_interval()->interval;
	};

	/**
	 * Performs the action of updating saved historical totals and culling
	 * out older values
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void do_action_impl(struct isi_error **error_out);

	/**
	 * Called before 'while' loop in thread execution starts (e.g., before
	 * do_action/do_action_impl are ever called).  In this case, acquires
	 * single-thread-per-cluster mastership before continuing
	 */
	void do_thread_startup_initialization();

	/* Copy, and assignment constructions aren't allowed. */
	stats_history_thread(const stats_history_thread&);
	stats_history_thread operator=(const stats_history_thread&);

public:
	stats_history_thread();
	~stats_history_thread();
};

#endif // _STATS_HISTORY_THREAD_H__
