#ifndef __STATS_DELTA_THREAD_H__
#define __STATS_DELTA_THREAD_H__

#include "stats_base_thread_pool.h"

const int STATS_DELTA_THREAD_DEFAULT_SLEEP = 300;
/**
 * A class for the delta stats thread.
 * This thread takes the statistics from the shared
 * memory and saves it to the data base.
 *
 * @author ssasson (4/30/2014)
 */
/////把libcurl上传下载删除的数据，累计到内存中stat_map_, 再把内存中的数据持久化到db. update total_table set bytes_in = bytes_in + ? .....
///一个cluster上启动一个线程
class stats_delta_thread : public stats_base_thread_pool
{
protected:

	/* See comments in base class */
	float get_default_sleep_interval() const
	{
		return STATS_DELTA_THREAD_DEFAULT_SLEEP;
	};

	/* See comments in base class */
	float get_sleep_interval_from_config_settings(
	    const cpool_d_config_settings &config_settings) const;

	/**
	 * Performs the action of saving the current stats and zeroing out
	 * in-memory copies
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void do_action_impl(struct isi_error **error_out);

	/* Copy, and assignment constructions aren't allowed. */
	stats_delta_thread(const stats_delta_thread&);
	stats_delta_thread operator=(const stats_delta_thread&);

public:
	stats_delta_thread();
	~stats_delta_thread();
};

#endif // _STATS_DELTA_THREAD_H__
