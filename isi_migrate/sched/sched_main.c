#include "sched_includes.h"
#include "sched_event.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/sched/siq_jobctl.h"
#include "isi_ufp/isi_ufp.h"
#include "isi_sra_gcfg/dbmgmt.h"

#define MAX_KEVENT_TIMEOUT 60
#define SIQ_POLICIES_FILE SIQ_IFS_CONFIG_DIR "siq-policies.gc"


static struct sched_ctx g_ctx = {};


/*  ____  _                   _   _   _                 _ _               
 * / ___|(_) __ _ _ __   __ _| | | | | | __ _ _ __   __| | | ___ _ __ ___ 
 * \___ \| |/ _` | '_ \ / _` | | | |_| |/ _` | '_ \ / _` | |/ _ \ '__/ __|
 *  ___) | | (_| | | | | (_| | | |  _  | (_| | | | | (_| | |  __/ |  \__ \
 * |____/|_|\__, |_| |_|\__,_|_| |_| |_|\__,_|_| |_|\__,_|_|\___|_|  |___/
 *          |___/                                                         
 */

static void
sig_cleanup(int signo)
{
	log(TRACE, "sig_cleanup: caught signal %d, exiting", signo);
	exit(1);
}

static void
sig_child(int signo)
{
	int	status;
	int	child_val;
	pid_t	pid;

	log(TRACE, "%s", __func__);

	/* Wait for any child without blocking */
	errno = 0;
	while (((pid = waitpid((pid_t)-1, &status, WNOHANG)) > 0) ||
	    (errno == EINTR)) {
		if (WIFEXITED(status)) {
			/* get child's exit status */
			child_val = WEXITSTATUS(status);
			log(TRACE, "child %d has exited "
			    "normally with status %d", pid, child_val);
		} else {
			log(TRACE, "child %d exited abnormally",
			    pid);
		}
	}
}


/*  _   _      _                     
 * | | | | ___| |_ __   ___ _ __ ___ 
 * | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * |  _  |  __/ | |_) |  __/ |  \__ \
 * |_| |_|\___|_| .__/ \___|_|  |___/
 *              |_|                  
 */

static int
setup_dirs(struct sched_ctx *sctx, const char *policy_id)
{
	char rep_buf[MAXPATHLEN + 1];
	char pol_buf[MAXPATHLEN + 1];
	char job_buf[MAXPATHLEN + 1];
	int res = -1;
	int fd = -1;

	sprintf(rep_buf, "%s/%s", sched_get_reports_dir(), policy_id);
	if (sched_make_directory(rep_buf) != 0)
		goto out;

	sprintf(pol_buf, "%s/%s", sctx->edit_dir, policy_id);
	if (sched_make_directory(pol_buf) != 0)
		goto out;

	sprintf(job_buf, "%s/%s/%s", sctx->edit_dir, policy_id, "0_0");
	fd = open(job_buf, O_CREAT | O_WRONLY, 0666);
	if (fd < 0) {
		log(ERROR, "%s: open(%s) error: %s", __func__, job_buf,
		    strerror(errno));
		goto out;
	}
	close(fd);

	sprintf(job_buf, "%s/%s", sched_get_jobs_dir(), policy_id);
	if (siq_dir_move(pol_buf, job_buf) != 0)
		goto out;	

	res = 0;
out:
	return res;
}

static int
reload_policy_list(struct sched_ctx *sctx)
{
	struct siq_policies *policies = NULL;
	struct siq_policy *current = NULL;
	struct isi_error *error = NULL;
	struct stat st = {};
	int res = -1;

	sctx->plist_check_time = time(NULL);

	if (stat(SIQ_POLICIES_FILE, &st) != 0) {
		/* a fresh node has no siq-policies.gc file until a user
		 * creates a policy. Skip reload. */
		res = 0;
		goto out;
	}

	/* skip reload if policy file's mtime hasn't changed since last time */
	if (st.st_mtimespec.tv_sec == sctx->plist_mtime.tv_sec &&
	    st.st_mtimespec.tv_nsec == sctx->plist_mtime.tv_nsec) {
		log(DEBUG, "skipping policy list reload");
		res = 0;
		goto out;
	}
	
	log(DEBUG, "reloading policy list");

	if (sctx->policy_list != NULL) {
		siq_policies_free(sctx->policy_list);
		sctx->policy_list = NULL;
	}

	policies = siq_policies_load_readonly(&error);
	if (policies == NULL) {
		log(ERROR, "Failed to load policies: %s",
		    isi_error_get_message(error));
		goto out;
	}

	SLIST_FOREACH(current, &policies->root->policy, next) {
		if (setup_dirs(sctx, current->common->pid) != 0)
			goto out;
	}

	/* store new mtime */
	sctx->plist_mtime.tv_sec = st.st_mtimespec.tv_sec;
	sctx->plist_mtime.tv_nsec = st.st_mtimespec.tv_nsec;

	ASSERT(policies != NULL);
	sctx->policy_list = policies;
	res = 0;

out:
	isi_error_free(error);
	return res;
}

static int
create_pid_file(void)
{
	int pid;
	int pfd = -1;
	int res = -1;
	int nbytes = 0;
	char pid_str[128];

	log(TRACE, "%s", __func__);

	pfd = open(sched_get_pidfile(), O_CREAT | O_EXCL | O_RDWR, 0666);
	if (pfd < 0 && errno == EEXIST) {
		/* check pid in file is stale or if sched is still running */
		pfd = open(sched_get_pidfile(), O_RDONLY);
		if (pfd == -1) {
			log(ERROR, "error opening pid file %s: %s",
			    sched_get_pidfile(), strerror(errno));
			goto out;
		}

		nbytes = read(pfd, pid_str, 128);
		if (nbytes < 0) {
			log(ERROR, "error reading pid file %s: %s", 
			    sched_get_pidfile(), strerror(errno));
			goto out;
		}
		
		pid = (int)strtoul(pid_str, NULL, 10);
		if (kill(pid, 0) == 0) {
			log(INFO, "sched already running (pid %d)", pid);
			goto out;
		}
		
		close(pfd);
		pfd = open(sched_get_pidfile(), O_RDWR | O_TRUNC, 0666);
		if (pfd < 0) {
			log(ERROR, "error opening pid file %s: %s",
			    sched_get_pidfile(), strerror(errno));
			goto out;
		}
	} else if (pfd < 0) {
		log(ERROR, "error opening/creating pid file %s: %s",
		    sched_get_pidfile(), strerror(errno));
		goto out;
	}

	snprintf(pid_str, 128, "%d", getpid());
	nbytes = write(pfd, pid_str, strlen(pid_str)); 
	if (nbytes != strlen(pid_str)) {
		log(ERROR, "error writing pid %d to file %s: %s",
		    getpid(), sched_get_pidfile(), strerror(errno));
		goto out;       
	}
	res = 0;

out:
	if (pfd != -1)
		close(pfd);
	return res;
}

static int
clean_run_dir(struct sched_ctx *sctx)
{
	int res = -1;
	int count;
	char policy_id[MAXPATHLEN + 1];
	char node_str[MAXPATHLEN + 1];
	char pid_str[MAXPATHLEN + 1];
	char remove_dir[MAXPATHLEN + 1];
	char work_dir[MAXPATHLEN + 1];
	DIR *dirp = NULL;
	struct dirent *dp = NULL;

	log(TRACE, "%s", __func__);

	dirp = opendir(sched_get_run_dir());
	if (!dirp) {
		log(ERROR, "opendir(%s) error: %s",
		    sched_get_run_dir(), strerror(errno));
		goto out;
	}

	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR)
			continue;

		count = sched_decomp_name(dp->d_name, policy_id, node_str,
		    pid_str);

		if ((count == 2 || count == 3) && atoi(node_str) == 0) {
			sprintf(remove_dir, "%s/%s",
			    sched_get_run_dir(), dp->d_name);
			sprintf(work_dir, "%s/%s",
			    sctx->edit_run_dir, dp->d_name);
			log(DEBUG, "%s: removing old run dir %s",
			    __func__, remove_dir);
			if (sched_remove_dir(sctx, remove_dir, work_dir) != 0)
				goto out;
		}
	}
	res = 0;

out:
	if (dirp)
		closedir(dirp);
	return res;
}

static int
create_sched_dirs(struct sched_ctx *sctx)
{
	int res = -1;

	/* set up shared scheduler directories (ok if they exist) */
	if (sched_make_directory(sched_get_siq_dir()) != 0 ||
	    sched_make_directory(sched_get_sched_dir()) != 0 ||
	    sched_make_directory(sched_get_jobs_dir()) != 0 ||
	    sched_make_directory(sched_get_run_dir()) != 0 ||
	    sched_make_directory(sched_get_trash_dir()) != 0 ||
	    sched_make_directory(sched_get_reports_dir()) != 0 ||
	    sched_make_directory(sched_get_log_dir()) != 0 ||
	    sched_make_directory(sched_get_edit_dir()) != 0 ||
	    sched_make_directory(sched_get_lock_dir()) != 0 ||
	    sched_make_directory(sched_get_snap_dir()) != 0 ||
	    sched_make_directory(SIQ_IFS_CONFIG_DIR) != 0 ||
	    sched_make_directory(SIQ_CONFLICT_LOG_DIR) != 0 ||
	    sched_make_directory(SIQ_WORK_LOCK_DIR) != 0 ||
	    sched_make_directory(SIQ_TARGET_WORK_LOCK_DIR) != 0 ||
	    sched_make_directory(SIQ_FP_DIR) != 0 ||
	    sched_make_directory(SIQ_SNAPSHOT_LOCK_DIR) != 0)
		goto out;

	/* set up node specific scheduler directories */
	sprintf(sctx->edit_dir, "%s/%d", sched_get_edit_dir(), sctx->node_num);
	sched_unlink(sctx, sctx->edit_dir);
	if (sched_make_directory(sctx->edit_dir) != 0)
		goto out;

	sprintf(sctx->edit_jobs_dir, "%s/jobs", sctx->edit_dir);
	if (sched_make_directory(sctx->edit_jobs_dir) != 0)
		goto out;

	sprintf(sctx->edit_run_dir, "%s/run", sctx->edit_dir);
	if (sched_make_directory(sctx->edit_run_dir) != 0)
		goto out;

	res = 0;

out:
	return res;
}

static int
create_test_file(struct sched_ctx *sctx)
{
	int res = -1;
	int fd = -1;
	int test_value = 123456789;

	sprintf(sctx->test_file, "%s/%d", sched_get_sched_dir(),
	    sctx->node_num);
	if (sched_make_directory(sctx->test_file) != 0)
		goto out;
	
	sprintf(sctx->test_file, "%s/%s", sctx->test_file, "test_file");
	fd = open(sctx->test_file, O_CREAT | O_WRONLY, 0666);
	if (fd < 0) {
		log(ERROR, "test_file_create: open(%s) error %s",
		    sctx->test_file, strerror(errno));
		goto out;
	}

	if (write(fd, (void *)&test_value, sizeof(int)) != sizeof(int)) {
		log(ERROR, "test_file_create: can not write(%s): %s",
		    sctx->test_file, strerror(errno));
		goto out;
	}
	res = 0;

out:
	if (fd != -1)
		close(fd);
	return res;
}

static void
empty_trash_dir(void)
{
	DIR *dirp = NULL;
	struct dirent *dp = NULL;
	char command[MAXPATHLEN + 8];

	dirp = opendir(sched_get_trash_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: error opendir(%s) error: %s",
		    __func__, sched_get_trash_dir(), strerror(errno));
		goto out;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		sprintf(command, "rm -rf %s/%s", sched_get_trash_dir(),
		    dp->d_name);
		log(DEBUG, "%s: system(\"%s\")", __func__, command);
		if (system(command) == 0) {
			log(DEBUG, "%s: removed %s from trash dir",
			    __func__, dp->d_name);
		} else {
			log(ERROR, "%s: error system(\"%s\"): %s",
			    __func__, command, strerror(errno));
		}
	}
	
out:
	if (dirp)
		closedir(dirp);
}

static void
exiting(void)
{
	if (g_ctx.rm_pidfile_on_exit)
		unlink(sched_get_pidfile());
}

static void
exit_if_upgrade_committed(void)
{
	struct version_status	ver_stat;

	get_version_status(false, &ver_stat);
	if (!ver_stat.local_node_upgraded) {
		/* A commit does not necessarily imply that
		 * the SIQ version changed, hence "<=". */
		ASSERT(g_ctx.siq_ver <= ver_stat.committed_version);
		log(NOTICE, "Upgrade committed (old version: "
		    "0x%x, new version: 0x%x), restarting",
		    g_ctx.siq_ver,
		    ver_stat.committed_version);
		log_version_ufp(ver_stat.committed_version, SIQ_SCHEDULER,
		    __func__);
		exit(EXIT_SUCCESS);
	}
}

/*
 * Returns the appropriate timeout for the main scheduler loop or -1 on failure.
 */
static int
calculate_timeout(struct sched_ctx *sctx, struct isi_error **error_out)
{
	int timeout = -1;
	time_t next_start, now;
	struct isi_error *error = NULL;

	if (!sctx->siq_license_enabled) {
		/* Sleep if the SIQ license is not enabled */
		timeout = SIQ_BASE_INTERVAL;
	} else {
		next_start = get_next_policy_start_time(sctx, &error);
		if (error)
			goto out;

		now = time(NULL);
		if (next_start == 0) {
			/* no scheduled jobs. wait for event */
			timeout = MAX_KEVENT_TIMEOUT;
			log(DEBUG, "No scheduled jobs");
		} else if (next_start <= now) {
			if (get_running_jobs_count(sctx) >=
			    sched_get_max_concurrent_jobs()) {
				/* there may be overdue jobs if max
				 * concurrent limit is reached. wait
				 * for an opening */
				if (sctx->max_jobs_running == false) {
					log(INFO, "One or more jobs postponed "
					    "due to max concurrent job limit "
					    "(%d)",
					    sched_get_max_concurrent_jobs());
					sctx->max_jobs_running = true;
				}
				log(DEBUG, "Max concurrent jobs running");
				timeout = MAX_KEVENT_TIMEOUT;
			} else {
				/* another job is due */
				timeout = 0;
			}
		} else {
			timeout = next_start - now;
			log(DEBUG, "Next scheduled job starts in %ds",
			    timeout);
		}
	}

	if (timeout > MAX_KEVENT_TIMEOUT)
		timeout = MAX_KEVENT_TIMEOUT;

out:
	isi_error_handle(error, error_out);
	return timeout;
}

/*  __  __       _       
 * |  \/  | __ _(_)_ __  
 * | |\/| |/ _` | | '_ \ 
 * | |  | | (_| | | | | |
 * |_|  |_|\__,_|_|_| |_|
 */

int
main(int argc,char *argv[])
{
	bool 			loop_failure = false;
	bool			pause_loop = true;
	bool			prev_license_check;
	struct sched_ctx	*sctx = &g_ctx;
	struct stat		upgrade_stat;
	int			ret;
	int			timeout;
	struct isi_error	*error = NULL;
	struct version_status	ver_stat;
	struct ilog_app_init init = {
		.full_app_name = "siq_sched",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file= "",
		.syslog_threshold = IL_INFO_PLUS,   ///// IL_TRACE_PLUS -> IL_INFO_PLUS
		.component = "sched",
		.job = "",
	};
	
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);
	atexit(exiting);

	sctx->rm_pidfile_on_exit = false;

	sctx->policy_list = NULL;

	sctx->siq_state = sched_get_siq_state();
	if (sctx->siq_state == SIQ_ST_OFF) {
		printf("main: siq_state SIQ_ST_OFF\n");
		goto out;
	}

	sctx->max_jobs_running = false;

	/* daemonize */
	if (SIQ_DAEMON(1, 0) < 0) {
		log(ERROR, "daemon failed: %s", strerror(errno));
		goto out;
	}

	log(INFO, "pid %d", getpid());

	if (UFAIL_POINT_INIT("isi_migr_sched", isi_migr_sched_ufp) != 0)
		log(ERROR, "Failed to init ufp (%s)", strerror(errno));

	check_for_dont_run(true);
	
	/* Ensure upgrade has completed */
	ret = -1;
	while (ret != 0) {
		ret = stat("/ifs/.ifsvar/modules/tsm/backup/.ript-upgrade-done",
		    &upgrade_stat);
		if (ret != 0) {
			log(INFO,
			    "waiting for successful completion of upgrade...");
			siq_nanosleep(60, 0);
		}
	}

	/* Set per-process encoding to UTF-8. */
	if (enc_set_proc(ENC_UTF8)) {
		log(ERROR, "Couldn't set encoding to UTF-8 (%d)", errno);
		goto out;
	}

	/* bail if scheduler is already running on this node */
	if (create_pid_file() != 0)
		goto out;

	sctx->rm_pidfile_on_exit = true;

	signal(SIGTERM, sig_cleanup);
	signal(SIGCHLD, sig_child);

	sctx->log_level = sched_get_log_level();
	if (sctx->log_level != INFO) {
		set_siq_log_level(sctx->log_level);
		log(ERROR, "Log level: %d", sctx->log_level);
	}

	sctx->node_num = sched_get_my_node_number();
	if (sctx->node_num <= 0) {
		log(ERROR, "sched_get_my_node_number error");
		goto out;
	}

	if (access(ISI_ROOT, F_OK) != 0) {
		log(ERROR, "%s stat error: %s",
		    ISI_ROOT, strerror(errno));
		goto out;
	}

	get_version_status(false, &ver_stat);
	sctx->siq_ver = ver_stat.committed_version;
	log_version_ufp(sctx->siq_ver, SIQ_SCHEDULER, __func__);

	if (create_sched_dirs(sctx) != 0)
		goto out;

	if (create_test_file(sctx) != 0)
		goto out;
	
	if (sched_quorum_wait(sctx) != 0)
		goto out;

	gmp_kill_on_quorum_change_register(NULL, &error);
	if (error) {
		log(ERROR, "gmp_kill_on_quorum_change_register error");
		goto out;
	}

	if (clean_run_dir(sctx) != 0) {
		log(ERROR, "sched_run_dir_clean error");
		goto out;
	}

	init_event_queue(sctx, &error);
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		goto out;
	}

	/* Initialize our policy list.
	 * On a fresh node there is no siq-policies.gc file, but that is
	 * okay. This function doesn't return NULL in that case, just an
	 * empty list of policies. */
	sctx->policy_list = siq_policies_load_readonly(&error);
	if (sctx->policy_list == NULL) {
		log(ERROR, "Failed to load policies: %s",
		    isi_error_get_message(error));
		goto out;
	}

	/* Assume that SIQ is initially licensed so that we properly log
	 * that an error the moment we detect that it is unlicensed */
	sctx->siq_license_enabled = true;

	/* mainloop */
	for (;;) {
		ASSERT(siq_job_locked() == false);
		pause_loop = true;
		//delete the old sra entries which got a clear state:
		//completed or failed
		delete_sra_old_entries();
		while (pause_loop) {
			pause_loop = false;
			UFAIL_POINT_CODE(main_loop_stop,
				siq_nanosleep(RETURN_VALUE, 0);
				pause_loop = true;);
		}

		if (loop_failure) {
			/* sleep after error to avoid tight loops */
			siq_nanosleep(5, 0);
			log(DEBUG, "sched mainloop loop failure");
			while (sched_quorum_wait(sctx) != 0)
				siq_nanosleep(10, 0);

			if (sched_my_jobs_remove(sctx) != 0) {
				log(ERROR, "sched_my_jobs_remove error");
				goto out;
			}
			loop_failure = false;
			if (ver_stat.local_node_upgraded) {
				exit_if_upgrade_committed();
			}
		}

		/* cleanup job directories moved to trash dir by coord */
		empty_trash_dir();

		/* licensing test */
		prev_license_check = sctx->siq_license_enabled;
		sctx->siq_license_enabled = SIQ_LICOK;
		if (!sctx->siq_license_enabled && prev_license_check)
			log(ERROR, "SyncIQ license is missing or expired");

		if (reload_policy_list(sctx) != 0) {
			loop_failure = true;
			continue;
		}

		if (ver_stat.local_node_upgraded) {
			exit_if_upgrade_committed();
		}

		if (sched_main_node_work(sctx) != 0) {
			loop_failure = true;
			continue;
		}

		if (sched_local_node_work(sctx) != 0) {
			loop_failure = true;
			continue;
		}

		if (reload_policy_list(sctx) != 0) {
			loop_failure = true;
			continue;
		}

		timeout = calculate_timeout(sctx, &error);
		if (error != NULL) {
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			loop_failure = true;
			continue;
		}

		if (timeout <= 0)
			continue;

		log(DEBUG, "Timeout %ds", timeout);

		wait_for_event(sctx, timeout, &error);
		if (error)
			loop_failure = true;
	}

out:
	isi_error_free(error);
	exit(1);
}
