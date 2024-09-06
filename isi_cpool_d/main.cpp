#include <stdio.h>
#include <libgen.h>
#include <pthread.h>
#include <signal.h>

#include <sys/isi_celog.h>
#include <sys/isi_celog_eventids.h>
#include <isi_cloud_common/isi_cloud_app.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d/auth_capability_refresh_thread_pool.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d/csi_scanning_thread_pool.h>
#include <isi_cpool_d/kdq_scanning_thread_pool.h>
#include <isi_cpool_d/ooi_scanning_thread_pool.h>
#include <isi_cpool_d/server_task_map.h>
#include <isi_cpool_d/task_processing_thread_pool.h>
#include <isi_cpool_d/telemetry_collection_thread_pool.h>
#include <isi_cpool_d/telemetry_posting_thread_pool.h>
#include <isi_cpool_d/wbi_scanning_thread_pool.h>
#include <isi_cpool_d_api/cpool_api_c.h>
#include <isi_cpool_d_api/cpool_message.h>
#include <isi_cpool_d/jobs_cleanup_thread_pool.h>
#include <isi_cpool_d/stats_delta_thread.h>
#include <isi_cpool_d/stats_query_thread.h>
#include <isi_cpool_d/stats_history_thread.h>
#include <isi_daemon/isi_daemon.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_ilog/ilog.h>
#include <isi_licensing/licensing.h>
#include <isi_cpool_security/cpool_protect.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_cbm/io_helper/compressed_istream.h>
#include <isi_ufp/isi_ufp.h>

#include <lwds/ds.h>

#define NETWORK_CHECK_FREQUENCY			60
#define CONFIG_SETTINGS_MAX_LOAD_ATTEMPTS	10
#define CONFIG_SETTINGS_SLEEP_IN_SEC		1

//cause jemalloc to abort on out of memory
const char * malloc_conf = "xmalloc:true";

bool foreground = false;
static bool g_debug = false;
unsigned int received_signals = 0;

task_processing_thread_pool *archive_thread_pool = NULL;
task_processing_thread_pool *recall_thread_pool = NULL;
task_processing_thread_pool *writeback_thread_pool = NULL;
task_processing_thread_pool *cache_inval_thread_pool = NULL;
task_processing_thread_pool *local_gc_thread_pool = NULL;
task_processing_thread_pool *cloud_gc_thread_pool = NULL;
task_processing_thread_pool *restore_coi_thread_pool = NULL;
telemetry_posting_thread_pool *telemetry_posting_pool = NULL;
telemetry_collection_thread_pool *telemetry_collection_pool = NULL;
stats_query_thread *p_stats_query_thread = NULL;

server_task_map *archive_task_map = NULL;
server_task_map *recall_task_map = NULL;
server_task_map *writeback_task_map = NULL;
server_task_map *cache_inval_task_map = NULL;
server_task_map *cloud_gc_task_map = NULL;
server_task_map *local_gc_task_map = NULL;
server_task_map *restore_coi_task_map = NULL;

static void
usage(const char* program)
{
	fprintf(stderr, "Usage: %s [-f]\n", program);
	fprintf(stderr, "\t-f: run in foreground (do not daemonize)\n");
	fprintf(stderr, "\t-d: enable memory debug mode\n");
}

static bool
parse_options(int argc, char *argv[], const char *program_name)
{
	bool ret = true;
	int c;

	while ((c = getopt(argc, argv, "fdhH?")) != -1) {
		switch (c) {
		case 'f':
			foreground = true;
			break;
		case 'd':
			g_debug = true;
			break;
		case 'h':
		case 'H':
		case '?':
		default:
			usage(program_name);
			ret = false;
		}
	}

	return ret;
}

static void
signal_handler(int sig)
{
	// do not make blocking calls in sig handler, they are known to cause
	// deadlocks.
	switch (sig) {
	case SIGTERM:
	case SIGHUP:
	case SIGINFO:
	case SIGUSR1:
		received_signals |= (1 << sig);
		break;
	default:
		break;
	}
}

static void
start_stop_network_thread_pools(bool start, struct isi_error **error_out)
{
	static bool network_thread_pools_up = false;

	struct isi_error *error = NULL;

	if (start) {
		ASSERT(!network_thread_pools_up,
		    "can't start already started network thread pools");

		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings = NULL;
		const cpool_d_thread_counts *thread_counts = NULL;

		config_settings = reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_ARCHIVE);
		archive_thread_pool =
		    new task_processing_thread_pool(archive_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_RECALL);
		recall_thread_pool =
		    new task_processing_thread_pool(recall_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_WRITEBACK);
		writeback_thread_pool =
		    new task_processing_thread_pool(writeback_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_CLOUD_GC);
		cloud_gc_thread_pool =
		    new task_processing_thread_pool(cloud_gc_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_RESTORE_COI);
		restore_coi_thread_pool =
		    new task_processing_thread_pool(restore_coi_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		telemetry_posting_pool = new telemetry_posting_thread_pool(1);
		telemetry_collection_pool =
		    new telemetry_collection_thread_pool(1);

		network_thread_pools_up = true;
	}
	else if (network_thread_pools_up) {
		archive_thread_pool->shutdown();
		delete archive_thread_pool;
		archive_thread_pool = NULL;

		recall_thread_pool->shutdown();
		delete recall_thread_pool;
		recall_thread_pool = NULL;

		writeback_thread_pool->shutdown();
		delete writeback_thread_pool;
		writeback_thread_pool = NULL;

		cloud_gc_thread_pool->shutdown();
		delete cloud_gc_thread_pool;
		cloud_gc_thread_pool = NULL;

		restore_coi_thread_pool->shutdown();
		delete restore_coi_thread_pool;
		restore_coi_thread_pool = NULL;

		/*
		 * Since the telemetry thread_pool pointers can be used
		 * directly when a SIGHUP is handled, change the pointers to
		 * NULL (an atomic operation) before deleting to avoid a
		 * non-NULL pointer pointing to a bogus thread_pool.
		 */
		thread_pool *temp_telem_thread_pool = telemetry_posting_pool;
		telemetry_posting_pool = NULL;
		temp_telem_thread_pool->shutdown();
		delete temp_telem_thread_pool;

		temp_telem_thread_pool = telemetry_collection_pool;
		telemetry_collection_pool = NULL;
		temp_telem_thread_pool->shutdown();
		delete temp_telem_thread_pool;

		network_thread_pools_up = false;
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * A one-shot attempt to reload the CBM configuration and reset the cpool_d
 * configuration (which will force the next reader to reload).  If successful,
 * SIGHUP bit is cleared, else configuration remains unchanged.  Thread pools
 * are notified as needed.
 */
static void
reload_config(void)
{
	/**
	 * There is a possibility that gconfig may be unavailable for a
	 * period of time. When unavailable, messages will be written
	 * to the log. To prevent log overload, updates will be restricted
	 * to once every 30 seconds.
	 */
	static time_t	time_last_reported       = 0;
	const double	MIN_REPORT_INTERVAL_SECS = 30.0;

	time_t	now = ::time(NULL);
	bool	ok_to_report =
	    (difftime(now, time_last_reported) > MIN_REPORT_INTERVAL_SECS);

	/* ************************************************************* */


	ilog(IL_DEBUG, "reloading configuration");

	struct isi_error *error = NULL;

	isi_cbm_reload_config(&error);
	ON_ISI_ERROR_GOTO(out, error);

	{
		scoped_cfg_writer writer;
		writer.reset_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	if (telemetry_posting_pool != NULL)
		telemetry_posting_pool->notify();

	if (telemetry_collection_pool != NULL)
		telemetry_collection_pool->notify();

	/*
	 * Notify stats query thread about the change.  Note* this could/should
	 * be made more generic by allowing any thread pool to register itself
	 * in a list of listeners.  For now, since there's only one listener,
	 * it doesn't seem worthwhile...
	 */
	if (p_stats_query_thread != NULL)
		p_stats_query_thread->on_reload_config();

	/*
	 * With all reload operations successfully done, reset the signal bit.
	 */
	received_signals &= ~(1 << SIGHUP);
	received_signals &= ~(1 << SIGINFO);

 out:
	if (error != NULL) {
		/**
		 * Only report this condition once every 30 seconds or more.
		 */
		ilog((ok_to_report ? IL_ERR : IL_DEBUG),
		    "failed to reset configuration: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
	}

	if (ok_to_report) {
		time_last_reported = now;
	}
}

static void
initialize_task_maps(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	archive_task_map = new server_task_map(OT_ARCHIVE, 2);
	ASSERT(archive_task_map != NULL);
	archive_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize archive task map");

	recall_task_map = new server_task_map(OT_RECALL, 2);
	ASSERT(recall_task_map != NULL);
	recall_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize recall task map");

	writeback_task_map = new server_task_map(OT_WRITEBACK, 1);
	ASSERT(writeback_task_map != NULL);
	writeback_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize writeback task map");

	cache_inval_task_map = new server_task_map(OT_CACHE_INVALIDATION, 1);
	ASSERT(cache_inval_task_map != NULL);
	cache_inval_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize cache invalidation task map");

	local_gc_task_map = new server_task_map(OT_LOCAL_GC, 1);
	ASSERT(local_gc_task_map != NULL);
	local_gc_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize local GC task map");

	cloud_gc_task_map = new server_task_map(OT_CLOUD_GC, 1);
	ASSERT(cloud_gc_task_map != NULL);
	cloud_gc_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize cloud GC task map");

	restore_coi_task_map = new server_task_map(OT_RESTORE_COI, 1);
	ASSERT(restore_coi_task_map != NULL);
	restore_coi_task_map->initialize(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to initialize restore COI task map");

 out:
	isi_error_handle(error, error_out);
}

static bool
cloudpools_is_in_use()
{
	struct isi_error *error = NULL;
	bool in_use = false;
	isi_cbm_coi_sptr coi;

	if (isi_licensing_module_status(ISI_LICENSING_CLOUDPOOLS) ==
	    ISI_LICENSING_LICENSED) {

		in_use = true;
		goto out;
	}

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	in_use = coi->has_entries(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (error) {
		ilog(IL_ERR, "Failed to check whether cloudpools is being "
		    "used: %#{}", isi_error_fmt(error));

		// Err on the side of using cloudpools
		in_use = true;

		isi_error_free(error);
	}

	return in_use;
}

static void
network_connectivity_check(struct isi_error **error_out)
{
	/* force a check during the initial call */
	static int network_check_throttle = NETWORK_CHECK_FREQUENCY;
	static bool network_up_now = false;
	static bool network_up_before = false;

	struct isi_error *error = NULL;

	++network_check_throttle;
	if (network_check_throttle <= NETWORK_CHECK_FREQUENCY)
		goto out;

	network_up_before = network_up_now;
	check_network_connectivity(&network_up_now, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (network_up_now && !network_up_before) {
		/*
		 * We have (re)gained network connectivity, so send an
		 * anonymous end to the CELOG network connection event and
		 * start the thread pools which require network connectivity.
		 */
		int ret = celog_event_end(CEF_END | CEF_ANONYMOUS,
		    CPOOL_NW_FAILED, "");
		if (ret != 0) {
			ilog(IL_ERR, "celog event end failed ev: %d errno: %d",
			    CPOOL_NW_FAILED, ret);
		}

		/* true: start */
		start_stop_network_thread_pools(true, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	else if (!network_up_now && network_up_before) {

		/*
		 * Create a celog event to notify that network connectivity is
		 * lost, but *only* if cloudpools is either licensed OR has
		 * archived files.  Otherwise, the celog event is notifying user
		 * of something that does not matter and just creates confusion
		 */
		if (cloudpools_is_in_use()) {
			ifs_devid_t devid = get_devid(&error);
			ON_ISI_ERROR_GOTO(out, error);

			int ret = celog_event_create(NULL, CCF_NONE,
			    CPOOL_NW_FAILED, CS_CRIT, 0, "CloudPools network "
			    "connection failed cloudpooltype: '{cloudpooltype}', "
			    "devid: '{devid}', msg: '{msg}' ", "cloudpooltype "
			    "%s, devid %d, msg %s", "All", devid,
			    "Interface down");

			if (ret != 0)
				ilog(IL_ERR, "celog event create failed ev-%d, "
				"errno-%d", CPOOL_NW_FAILED, ret);

			ilog(IL_ERR,
			    "X#X# CPOOL NW FAILED error-MAIN "
			    "CloudPools Network connection failed "
			    "cloudpooltype: %s, devid: %d, msg: %s ",
			    "All", devid, "Interface down");
		}

		/* false: stop */
		start_stop_network_thread_pools(false, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	network_check_throttle = 0;

 out:
	isi_error_handle(error, error_out);
}

static void
check_for_messages(struct isi_error **error_out)
{
	ASSERT (restore_coi_thread_pool != NULL);

	struct isi_error *error = NULL;
	cpool_envelope envelope;
	const std::vector<cpool_message*> &messages = envelope.get_messages();

	envelope.check(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!envelope.has_messages())
		goto out;

	for (std::vector<cpool_message*>::const_iterator it = messages.begin();
	    it != messages.end(); ++it) {

		const cpool_message *message = *it;

		switch (message->get_recipient()) {
			case CPM_RECIPIENT_RESTORE_COI:
				if (message->get_action() == CPM_ACTION_NOTIFY)
				{

					restore_coi_thread_pool->notify();
				}
				break;
			case CPM_RECIPIENT_NONE:
				break;
		}
	}

	/* Make sure we don't handle the same messages again */
	envelope.delete_envelope_from_disk();

 out:
	isi_error_handle(error, error_out);
}

int
main(int argc, char* argv[])
{
	const char *program_name = NULL;
	struct isi_error *error = NULL;
	struct isi_daemon *daemon = NULL;
	struct sigaction sigact = { };

	program_name = getprogname();
	if (!parse_options(argc, argv, program_name))
		// parse_options will print usage before returning false
		return EXIT_FAILURE;

	// initialize logging
	struct ilog_app_init init =
	{
		.full_app_name = program_name,
		.component = "",
		.job = "",
		.syslog_program = program_name,
		.default_level = IL_NOTICE_PLUS | IL_DETAILS,
		.use_syslog = true,
		.use_stderr = false,
		.log_thread_id = true,
		.syslog_facility = LOG_DAEMON,
		.log_file = "/var/log/isi_cpool_d.log",
		.syslog_threshold = IL_ERR_PLUS | IL_DETAILS,
	};

	ilog_init(&init, true, true);

	//logging preamble
	ilog(IL_NOTICE, "starting %s, waiting on quorum...", program_name);

	/*
	 * Wait until part of a quorum, and also register to be killed on
	 * quorum change events.
	 */
	gmp_wait_on_quorum(NULL, &error);
	if (error != NULL) {
		ilog(IL_ERR, "gmp_wait_on_quorum error: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		return EXIT_FAILURE;
	}
	ilog(IL_NOTICE, "quorum obtained");

	if (!foreground) {
		daemon = isi_daemon_init(program_name, true, &error);
		if (error != NULL) {
			ilog(IL_ERR, "Error initializing %s: %{}",
			    program_name, isi_error_fmt(error));
			isi_error_free(error);
			return EXIT_FAILURE;
		}
	}

	if (g_debug) {
		LwDsSetMemoryMode(LWDS_MEMORY_MODE_DEBUG);
	} else {
		LwDsSetMemoryMode(LWDS_MEMORY_MODE_RELEASE);
	}

	/*
	 *
	 * Now that we've (possibly) daemonized, errors should be handled by
	 * the out block rather than returning EXIT_FAILURE.
	 *
	 */

	ooi_scanning_thread_pool ooi_thread_pool;
	csi_scanning_thread_pool csi_thread_pool;
	wbi_scanning_thread_pool wbi_thread_pool;
	jobs_cleanup_thread_pool daemon_job_cleanup_thread_pool;
	kdq_scanning_thread_pool kdq_thread_pool;

	stats_delta_thread stats_delta_thrd;
	stats_query_thread stats_query_thrd;
	stats_history_thread stats_history_thrd;

	auth_capability_refresh_thread auth_cap_refresh_thread_pool;

	// ufp offers no thread safe when it is in a forked child process
	// so we move the ufp initialization after fork
	UFAIL_POINT_INIT("isi_cpool_d");

	sigact.sa_handler = signal_handler;
	sigaction(SIGHUP, &sigact, 0);
	sigaction(SIGUSR1, &sigact, 0);
	sigaction(SIGTERM, &sigact, 0);
	sigaction(SIGINFO, &sigact, 0);

	// Generate MEK if one does not exist, else read from cache
	cpool_d_get_mek(&error);
	ON_ISI_ERROR_GOTO(out, error, "error getting MEK in cpool_d");

	// Generate a Pool of DEKs
	cpool_generate_dek(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "error creating pool of DEKs in cpool_d");

	isi_cloud::daemon_job_manager::at_startup(&error);
	ON_ISI_ERROR_GOTO(out, error, "daemon job manager startup failed");

	/*
	 * Load the cpool_d configuration settings to be used by
	 * various components (thread pools, task maps, etc.).  Failing to read
	 * the configuration here allows us to shutdown the daemon rather than
	 * crash later when trying to dereference a NULL pointer.
	 */
	for (int i = 1; i <= CONFIG_SETTINGS_MAX_LOAD_ATTEMPTS; ++i) {
		if (error != NULL) {
			isi_error_free(error);
			error = NULL;
		}

		scoped_cfg_reader reader;
		reader.get_cfg(&error); /* can ignore return value */

		if (error != NULL) {
			ilog(IL_DEBUG,
			    "failed to load configuration settings "
			    "on attempt %d of %d: %#{}",
			    i, CONFIG_SETTINGS_MAX_LOAD_ATTEMPTS,
			    isi_error_fmt(error));

			/* Sleep until next attempt. */
			struct timeval timeout;
			gettimeofday(&timeout, NULL);
			timeout.tv_sec += CONFIG_SETTINGS_SLEEP_IN_SEC;
			select(0, NULL, NULL, NULL, &timeout);
		}
	}
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to load configuration settings after "
	    "maximum number of attempts (%d)",
	    CONFIG_SETTINGS_MAX_LOAD_ATTEMPTS);

	/*
	 * Create and initialize task maps.  (The initialization of a task map
	 * doesn't rely on network connectivity.)
	 */
	initialize_task_maps(&error);
	ON_ISI_ERROR_GOTO(out, error, "error initializing task maps");

	/*
	 * Before starting any threads, mark this process so it will have
	 * access to stubbed files.
	 */
	isi_cbm_enable_stub_access(&error);
	ON_ISI_ERROR_GOTO(out, error, "error enabling access to stub files");

	/*
	 * Before starting any threads, disable atime updates on all files.
	 */
	isi_cbm_disable_atime_updates(&error);
	ON_ISI_ERROR_GOTO(out, error, "error disabling atime_updates");

	/*
	 * Create / start the thread pools that don't rely on network
	 * connectivity.
	 */
	{
		scoped_cfg_reader reader;
		const cpool_d_config_settings *config_settings = NULL;
		const cpool_d_thread_counts *thread_counts = NULL;

		config_settings = reader.get_cfg(&error);
		ON_ISI_ERROR_GOTO(out, error);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_LOCAL_GC);
		local_gc_thread_pool =
		    new task_processing_thread_pool(local_gc_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		thread_counts = config_settings->get_thread_counts(
		    CPOOL_TASK_TYPE_CACHE_INVALIDATION);
		cache_inval_thread_pool =
		    new task_processing_thread_pool(cache_inval_task_map,
		    thread_counts->normal_threads,
		    thread_counts->recovery_threads);

		ooi_thread_pool.start_threads(
		    config_settings->get_num_ooi_scanning_threads());
		csi_thread_pool.start_threads(
		    config_settings->get_num_csi_scanning_threads());
		wbi_thread_pool.start_threads(
		    config_settings->get_num_wbi_scanning_threads());
		daemon_job_cleanup_thread_pool.start_threads(
		    config_settings->get_num_job_cleanup_threads());
		kdq_thread_pool.start_threads(
		    config_settings->get_num_kdq_scanning_threads());

		stats_delta_thrd.start_threads(1);
		stats_query_thrd.start_threads(1);
		p_stats_query_thread = &stats_query_thrd;
		stats_history_thrd.start_threads(1);
		auth_cap_refresh_thread_pool.start_threads(1);
	}

	cloudpools_cleanup_compression_tmp_file();

	while (true) {
		/*
		 * Check for network connectivity and start/shutdown the thread
		 * pools that rely on network connectivity accordingly.
		 */
		network_connectivity_check(&error);
		if (error != NULL) {
			ilog(IL_ERR,
			    "failed to determine network connectivity: %#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}

		if (received_signals) {
			bool signal_handled = false;

			if (received_signals & (1 << SIGTERM)) {
				ilog(IL_TRACE, "handling SIGTERM");
				break;
			}

			if (received_signals & (1 << SIGHUP)) {
				ilog(IL_TRACE, "handling SIGHUP");

				/* reload_config() updates received_signals. */
				reload_config();

				signal_handled = true;
			}

			if (received_signals & (1 << SIGINFO)) {
				ilog(IL_TRACE, "handling SIGINFO");

				/* reload_config() updates received_signals. */
				reload_config();

				signal_handled = true;
			}

			if (received_signals & (1 << SIGUSR1)) {
				ilog(IL_TRACE, "handling SIGUSR1");

				check_for_messages(&error);

				if (error != NULL) {
					ilog(IL_ERR, "failed to check for "
					    "cpool_message: %#{}",
					    isi_error_fmt(error));
					isi_error_free(error);
					error = NULL;
				} else
					received_signals &= ~(1 << SIGUSR1);

				signal_handled = true;
			}

			// if you hit this, you have a signal that is
			// being caught but not doing anything with it,
			// which is probably a coding error...
			ASSERT(signal_handled,
			    "unhandled signal(s): %u", received_signals);
		}

		sleep(1);
	}

 out:
	/* false: stop */
	start_stop_network_thread_pools(false, isi_error_suppress());

	/*
	 * Mark the daemon as being shutdown and as such indefinite retry code
	 * can gracefully exit.
	 */
	isi_cloud_common_mark_shutdown(true);

	if (local_gc_thread_pool != NULL)
		local_gc_thread_pool->shutdown();
	if (cache_inval_thread_pool != NULL)
		cache_inval_thread_pool->shutdown();
	ooi_thread_pool.shutdown();
	csi_thread_pool.shutdown();
	wbi_thread_pool.shutdown();
	stats_delta_thrd.shutdown();
	p_stats_query_thread = NULL;
	stats_query_thrd.shutdown();
	stats_history_thrd.shutdown();
	auth_cap_refresh_thread_pool.shutdown();
	daemon_job_cleanup_thread_pool.shutdown();
        kdq_thread_pool.shutdown();

	if (daemon != NULL)
		isi_daemon_destroy(&daemon);

	if (error != NULL) {
		ilog(IL_ERR,
		    "isi_cpool_d shutting down due to error: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
