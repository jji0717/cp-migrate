#include <isi_cpool_stats/cpool_stat_manager.h>

#include "thread_mastership.h"
#include "stats_history_thread.h"

#define MASTER_FILE_NAME "stats_history_master.txt"
#define MASTER_LOCKFILE_NAME "stats_history_master.lock"
////一个cluster就一个node启动一个线程执行

stats_history_thread::stats_history_thread()
	: stats_base_thread_pool()
{
}

stats_history_thread::~stats_history_thread()
{
}

void
stats_history_thread::do_action_impl(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_manager *stat_mgr = get_stat_manager(&error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(stat_mgr != NULL);

	stat_mgr->update_stats_history(&error);
	ON_ISI_ERROR_GOTO(out, error);

	stat_mgr->cull_old_stats(&error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

void
stats_history_thread::do_thread_startup_initialization()
{
	wait_for_mastership(MASTER_LOCKFILE_NAME, MASTER_FILE_NAME,
	    ACQUIRE_MASTERSHIP_SLEEP_INTERVAL);
}
