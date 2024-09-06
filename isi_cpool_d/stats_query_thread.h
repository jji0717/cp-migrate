#ifndef __STATS_QUERY_THREAD_H__
#define __STATS_QUERY_THREAD_H__

#include "stats_base_thread_pool.h"

const int STATS_QUERY_THREAD_DEFAULT_SLEEP = 300;
/**
 * stats_query_thread queries the DB for populating the various
 * stats requests.
 *
 * @author ssasson (4/30/2014)
 */
class stats_query_thread : public stats_base_thread_pool
{
protected:

	/* See comments in base class */
	float get_default_sleep_interval() const
	{
		return STATS_QUERY_THREAD_DEFAULT_SLEEP;
	};

	/* See comments in base class */
	float get_sleep_interval_from_config_settings(
	    const cpool_d_config_settings &config_settings) const
	{
		return config_settings.get_query_stats_interval()->interval;
	};

	/**
	 * Performs the action of querying saved stats and caching them in
	 * memory
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void do_action_impl(struct isi_error **error_out);

	/* Copy, and assignment constructions aren't allowed. */
	stats_query_thread(const stats_query_thread&);
	stats_query_thread operator=(const stats_query_thread&);

public:
	stats_query_thread();
	~stats_query_thread();

	/**
	 * A listener callback for when gconfig changes are made.  In this case,
	 * will be used to re-register cached queries
	 */
	void on_reload_config();
};

#endif // _STATS_QUERY_THREAD_H__
