#include <signal.h>

#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_cbm/isi_cbm_account_statistics.h>
#include <isi_cpool_cbm/isi_cbm_account_util.h>
#include <isi_cpool_cbm/isi_cbm_id.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_stats/cpool_stat_manager.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_assert.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_proxy/isi_proxy.h>

#include "scheduler.h"
#include "telemetry_collection_thread_pool.h"
#include "thread_mastership.h"

#define MASTER_FILE_NAME "telemetry_collection_master.txt"
#define MASTER_LOCKFILE_NAME MASTER_FILE_NAME ".lock"

telemetry_collection_thread_pool::telemetry_collection_thread_pool(
    int num_threads)
{
	ASSERT(num_threads == 1,
	    "only 1 thread is supported (not %d)", num_threads);

	for (int i = 0; i < num_threads; ++i)
		add_thread(NULL, start_thread, this);
}

telemetry_collection_thread_pool::~telemetry_collection_thread_pool()
{
	shutdown();
}

void *
telemetry_collection_thread_pool::start_thread(void *arg)
{
	ASSERT(arg != NULL);

	telemetry_collection_thread_pool *tp =
	    static_cast<telemetry_collection_thread_pool *>(arg);
	ASSERT(tp != NULL);

	const int seconds_between_failed_parse_attempts = 60;

	/*
	 * Block SIGTERM and SIGHUP in this thread as they are handled by the
	 * isi_cpool_d main thread (and not here).
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGHUP);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	tp->wait_for_mastership(MASTER_LOCKFILE_NAME, MASTER_FILE_NAME,
	    ACQUIRE_MASTERSHIP_SLEEP_INTERVAL);

	while (!tp->is_shutdown_pending()) {
		struct isi_error *error = NULL;

		struct timeval next_event_tm = {
			.tv_sec = tp->get_next_event_time(&error),
			.tv_usec = 0
		};

		if (error != NULL) {
			ilog(IL_ERR,
			    "error determining next "
			    "telemetry collection event time: %#{}",
			    isi_error_fmt(error));

			tp->sleep_for(seconds_between_failed_parse_attempts);

			isi_error_free(error);
			error = NULL;
		}
		else if (tp->sleep_until(next_event_tm)) {
			tp->process_event(next_event_tm.tv_sec, &error);
			if (error != NULL) {
				ilog(IL_ERR,
				    "error processing "
				    "telemetry collection event: %#{}",
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			}
		}
	}

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(NULL);
}

time_t
telemetry_collection_thread_pool::get_next_event_time(
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	time_t next_retrieval_time = 0;

	const cpool_d_config_settings *config_settings = NULL;
	const struct cpool_d_thread_schedule_info *retrieval_info = NULL;
	time_t starting_from = time(NULL);
	scheduler scheduler;

	scoped_cfg_reader reader;
	config_settings = reader.get_cfg(&error);
	ON_ISI_ERROR_GOTO(out, error);

	retrieval_info = config_settings->get_usage_retrieval_schedule_info();

	next_retrieval_time =
	    scheduler.get_next_time(retrieval_info->schedule_info,
	    starting_from, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);

	return next_retrieval_time;
}

void
telemetry_collection_thread_pool::retrieve_cloud_usage(
    const struct cpool_account *acct,
    struct cl_account_statistics &account_stats,
    const isi_cbm_account &cbm_account,
    struct isi_error **error_out)
{
	ASSERT(acct != NULL);

	struct isi_error *error = NULL;

	isi_cloud::cl_account_statistics_param
	    account_stats_param;

	struct tm timestamp;
	memset(&timestamp, 0, sizeof(struct tm));

	account_stats_param.timestamp = timestamp;
	account_stats_param.stats_unit = BYTES;
	account_stats_param.op = ACCOUNT_CAPACITY;
	account_stats_param.billing_bucket.assign(
	    acct->billing_bucket == NULL ? "" : acct->billing_bucket);

	account_stats_param.capacity_response_format = "";
	account_stats_param.capacity_response_path = "";
	account_stats_param.capacity_response_value = "";

	account_stats.used_capacity = 0;

	isi_cbm_get_account_statistics(cbm_account, account_stats_param,
	    account_stats, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
telemetry_collection_thread_pool::process_event(time_t event_time,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpool_config_context *cp_context = NULL;
	struct isi_proxy_context *proxy_context = NULL;

	cpool_stat_manager *mgr = NULL;
	std::vector<acct_stat*> stat_vec;
	cpool_d_config_settings config_settings;
	scoped_cfg_writer writer;

	cp_context = cpool_context_open(&error);
	ON_ISI_ERROR_GOTO(out, error);

	isi_proxy_context_open(&proxy_context, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Get usage for all accounts into one vector */
	for (struct cpool_account *acct = cpool_account_get_iterator(cp_context);
	    acct != NULL; acct = cpool_account_get_next(cp_context, acct)) {

		/* We cannot retrieve telemetry data for all account types */
		if (cpool_account_get_type(acct) == CPT_RAN ||
		    cpool_account_get_type(acct)== CPT_ECS ||
		    cpool_account_get_type(acct)== CPT_ECS2 ||
		    cpool_account_get_type(acct)== CPT_GOOGLE_XML)
			continue;

		cpool_stat cpool_stats;
		struct cl_account_statistics cbm_stats;
		cpool_stat_account_key key(
		    cpool_account_get_local_key(acct));

		isi_cbm_account cbm_account;
		isi_cfm_cpool_acct_to_cbm_acct(*acct, cbm_account,
		    cp_context, proxy_context, &error);

		if(error == NULL)
			retrieve_cloud_usage(acct, cbm_stats, cbm_account,
			    &error);

		/*
		 * Let's not let one little error stop the collection for all
		 * accounts
		 */
		if (error != NULL) {
			ilog(IL_ERR, "error: %#{}", isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		cpool_stats.set_total_usage(cbm_stats.used_capacity);

		stat_vec.push_back(new acct_stat(key, cpool_stats));
	}

	/* Push usage vector into cpool_stats */
	mgr = cpool_stat_manager::get_new_instance(&error);
	ON_ISI_ERROR_GOTO(out, error);

	mgr->set_current_stats(stat_vec, CP_STAT_TOTAL_USAGE, &error);
	ON_ISI_ERROR_GOTO(out, error);

	config_settings.reload(&error);
	ON_ISI_ERROR_GOTO(out, error);

	config_settings.update_last_usage_retrieval_time();

	config_settings.commit(&error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Since we just wrote to config, make sure that it is refreshed */
	writer.reset_cfg(&error);
	ON_ISI_ERROR_GOTO(out, error);
 out:

	if (mgr != NULL)
		delete mgr;

	std::vector<acct_stat*>::iterator it;
	for (it = stat_vec.begin(); it != stat_vec.end(); it++)
		delete(*it);
	stat_vec.clear();

	if (proxy_context != NULL)
		isi_proxy_context_close(&proxy_context,
		    isi_error_suppress(IL_ERR));

	if (cp_context != NULL)
		cpool_context_close(cp_context);

	isi_error_handle(error, error_out);
}
