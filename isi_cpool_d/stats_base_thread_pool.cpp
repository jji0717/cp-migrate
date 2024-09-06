#include <signal.h>

#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_ilog/ilog.h>

#include "stats_base_thread_pool.h"

stats_base_thread_pool::stats_base_thread_pool() : stat_manager_(NULL)
{
}

stats_base_thread_pool::~stats_base_thread_pool()
{
	shutdown();
	clean_stat_manager();
}

void
stats_base_thread_pool::start_threads(int num_threads)
{
	ilog(IL_TRACE, "Adding %d thread(s) to stats thread pool",
	    num_threads);
	for (int i = 0; i < num_threads; i++)
		add_thread(NULL, thread_callback, this);
}

void *
stats_base_thread_pool::thread_callback(void *arg)
{
	ASSERT(arg != NULL);

	stats_base_thread_pool *tp =
	    static_cast<stats_base_thread_pool *>(arg);
	ASSERT(tp != NULL);

	int sleep_interval = -1;

	block_sigterm();

	tp->do_thread_startup_initialization();

	while (!tp->is_shutdown_pending()) {
		tp->do_action();

		sleep_interval = tp->get_sleep_interval();

		tp->sleep_for(sleep_interval);
	}

	tp->do_thread_shutdown_cleanup();

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(NULL);
}

cpool_stat_manager *
stats_base_thread_pool::get_stat_manager(struct isi_error **error_out)
{
	if (stat_manager_ == NULL) {
		struct isi_error *error = NULL;

		stat_manager_ =
		    cpool_stat_manager::get_new_instance(&error);

		/*
		 * We'd better either have a NULL error or a NULL stat_manager_
		 * but not both
		 */
		ASSERT((error == NULL) != (stat_manager_ == NULL));

		isi_error_handle(error, error_out);
	}

	return stat_manager_;
}

void
stats_base_thread_pool::clean_stat_manager()
{
	if (stat_manager_ != NULL) {
		delete stat_manager_;
		stat_manager_ = NULL;
	}
}

void
stats_base_thread_pool::do_action()
{
	struct isi_error *error = NULL;
	do_action_impl(&error);

	/*
	 * May have failed due to recoverable issues (e.g., database file
	 * handle becomes stale during sleep period), clean manager and try
	 * a second time
	 */
	if (error != NULL) {
		ilog(IL_ERR, "Failed to perform cpool_stats action.  "
		    "Retrying.: %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;

		clean_stat_manager();

		do_action_impl(&error);
	}

	if (error != NULL) {
		ilog(IL_ERR,
		    "Failed retrying cpool_stats action: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
}

float
stats_base_thread_pool::get_sleep_interval() const
{
	struct isi_error *error = NULL;

	/*
	 * Set a default interval to be used if there is an error
	 * reading the configuration from gconfig.
	 */
	float ret_interval = get_default_sleep_interval();

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
		    reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		if (config_settings == NULL) {
			error = isi_system_error_new(ENOENT,
			    "Failed to read config_settings from gconfig");
			goto out;
		}

		ret_interval =
		    get_sleep_interval_from_config_settings(*config_settings);
	}

 out:
	if (error != NULL) {
		/* ilog doesn't handle floats */
		char float_str[64];
		snprintf(float_str, sizeof float_str, "%f", ret_interval);

		ilog(IL_ERR,
		    "failed to get sleep interval, "
		    "using default of %s seconds: %#{}",
		    float_str, isi_error_fmt(error));
		isi_error_free(error);
	}

	return ret_interval;
}
