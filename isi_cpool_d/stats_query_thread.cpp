#include <isi_ilog/ilog.h>
#include "stats_query_thread.h"

stats_query_thread::stats_query_thread()
	: stats_base_thread_pool()
{
}

stats_query_thread::~stats_query_thread()
{
}

void
stats_query_thread::do_action_impl(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_manager *stat_mgr = get_stat_manager(&error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(stat_mgr != NULL);

	stat_mgr->execute_cached_queries(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
stats_query_thread::on_reload_config()
{
	struct isi_error *error = NULL;

	cpool_stat_manager *stat_mgr = get_stat_manager(&error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(stat_mgr != NULL);

	stat_mgr->clear_cached_queries(&error);
	ON_ISI_ERROR_GOTO(out, error);

// TODO:: When throttling has limits defined in gconfig, iterate through
//   accounts and add each limit as a query
out:
	/* Can't return an error from here, just log it, free it and leave */
	if (error != NULL) {

		ilog(IL_ERR,
		    "Error while reloading cpool_stat cached queries: %#{}",
		    isi_error_fmt(error));

		isi_error_free(error);
	}
}
