#include <signal.h>

#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/scoped_lock.h>

#include "jobs_cleanup_thread_pool.h"

jobs_cleanup_thread_pool::jobs_cleanup_thread_pool()
{
}

jobs_cleanup_thread_pool::~jobs_cleanup_thread_pool()
{
	shutdown();
}

void jobs_cleanup_thread_pool::start_threads(int num_threads)
{
	ASSERT(num_threads > 0, "num_threads: %d", num_threads);

	for (int i = 0; i < num_threads; ++i)
		add_thread(NULL, cleanup_daemon_jobs, this);
}

void *
jobs_cleanup_thread_pool::cleanup_daemon_jobs(void *arg)
{
	ASSERT(arg != NULL);

	struct isi_error *error = NULL;

	jobs_cleanup_thread_pool *tp =
		static_cast<jobs_cleanup_thread_pool *>(arg);

	ASSERT(tp != NULL);

	struct cpool_d_cleanup_settings settings;
	time_t deletion_time;
	int n_desired = 10; /* max. number of jobs deleted before
				 thread is put to short sleep, used to mitigate
				 load on CPU */
	int n_success;
	float sleep_interval;

	ilog(IL_NOTICE, "starting daemon jobs cleanup thread");

	/*
	 * Block SIGTERM in this thread.  When isi_cpool_d receives SIGTERM, it
	 * will use s_shutdown to signal the threads to exit.
	 */
	sigset_t s;
	sigemptyset(&s);
	sigaddset(&s, SIGTERM);
	sigaddset(&s, SIGUSR1);
	pthread_sigmask(SIG_BLOCK, &s, NULL);

	while (!tp->is_shutdown_pending())
	{
		tp->read_cleanup_settings(settings);
		deletion_time = time(NULL) - settings.delete_jobs_older_than; // 默认配置文件中是7天
		n_success =
			isi_cloud::daemon_job_manager::cleanup_old_jobs(
				n_desired, deletion_time, &error);
		// n_success = 1;
		// isi_cloud::daemon_job_manager::cleanup_jobs(n_desired, deletion_time, &error);

		sleep_interval = (n_success == n_desired && error == NULL) ? settings.cleanup_interval_with_daemon_jobs_remaining : settings.cleanup_interval_without_daemon_jobs_remaining;

		if (error)
		{
			ilog(IL_ERR,
				 "failure cleaning up finished daemon jobs "
				 "(%d requested, %d removed): %#{}",
				 n_desired, n_success, isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}

		// tp->sleep_for(sleep_interval);
		tp->sleep_for(30);
	}

	ilog(IL_NOTICE, "thread exiting due to shutdown");

	pthread_exit(0);
}

void jobs_cleanup_thread_pool::read_cleanup_settings(
	struct cpool_d_cleanup_settings &cleanup_settings)
{
	struct isi_error *error = NULL;
	const struct cpool_d_cleanup_settings
		*cleanup_settings_ptr = NULL;

	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings =
			reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		cleanup_settings_ptr = config_settings->get_cleanup_settings();
		ASSERT(cleanup_settings_ptr != NULL);

		cleanup_settings = *cleanup_settings_ptr;
	}

out:
	if (error != NULL)
	{
		ilog(IL_ERR, "failed to read cleanup settings: %#{}",
			 isi_error_fmt(error));
		isi_error_free(error);
	}
}
