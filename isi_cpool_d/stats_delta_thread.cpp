#include <isi_cpool_stats/cpool_stat_manager.h>

#include "stats_delta_thread.h"

stats_delta_thread::stats_delta_thread()
	: stats_base_thread_pool()
{
}

stats_delta_thread::~stats_delta_thread()
{
}

void
stats_delta_thread::do_action_impl(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_manager *stat_mgr = get_stat_manager(&error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(stat_mgr != NULL);

	stat_mgr->save_current_stats(&error);////把cpool_stat_shared_map_中的stats加到total_table
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

float
stats_delta_thread::get_sleep_interval_from_config_settings(
    const cpool_d_config_settings &config_settings) const
{
	return config_settings.get_delta_stats_interval()->interval;
}
