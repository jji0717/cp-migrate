#include "siq_glob_param_load.h"
#include "siq_log.h"
#include "isi_migrate/migr/isirep.h"

#define DEFAULT_MAX_JOBS 5

/* conf params */
/* "tsm/sched_edit" (SCHED_EDIT_DIR) */
static char		siq_edit_dir[MAXPATHLEN + 1] = "";
/* "tsm/sched/jobs" (SCHED_JOBS_DIR) */
static char		sched_jobs_dir[MAXPATHLEN + 1] = "";
/* "tsm/sched/run" (SCHED_RUN_DIR) */
static char		sched_run_dir[MAXPATHLEN + 1] = "";
/* "tsm/sched/trash" (SCHED_TRASH_DIR) */
static char		sched_trash_dir[MAXPATHLEN + 1] = "";
/* "tsm/sched/lock" (SCHED_LOCK_DIR) */
static char		sched_lock_dir[MAXPATHLEN + 1] = "";
/* "tsm/sched/snapshot_management" (SCHED_SNAP_DIR) */
static char		sched_snap_dir[MAXPATHLEN + 1] = "";
static char		full_log_name[MAXPATHLEN + 1] = "";

/* Set in set_fake_dirs() during unit tests */
static char		fake_base_dir[MAXPATHLEN + 1] = "";

static int		load_need = 1;
static int		is_loaded = 0;
static int		is_fake = 0;
static struct siq_gc_conf	*tc;

//Forward declaration
static int global_params_load(void);


enum siq_state
sched_get_siq_state(void)
{
	load_need = 1;
	if (global_params_load() == FAIL) {
		return SIQ_ST_OFF;
	}
	return tc->root->state;
} /* sched_get_siq_state */

time_t
sched_get_quorum_poll_interval(void)
{
	if (global_params_load() == FAIL) {
		return -1;
	}
	return tc->root->scheduler->quorum_poll_interval;
} /* sched_get_quorum_poll_interval */

time_t
sched_get_policies_poll_interval(void) 
{
	if (global_params_load() == FAIL) {
		return -1;
	}
	return tc->root->scheduler->policies_poll_interval;
} /* sched_get_policies_poll_interval */

const char *
sched_get_siq_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}
	return SIQ_APP_WORKING_DIR;
} /* sched_get_siq_dir */

const char *
sched_get_log_dir(void)
{
	return SIQ_APP_LOG_DIR;
} /* sched_get_log_dir */

const char *
sched_get_log_name(void)
{
	return SIQ_APP_LOG_FILE;
} /* sched_get_log_name */

const char *
sched_get_del_files(void)
{
	return SIQ_SCHED_DELETED_FILES_LOG_FILE;
} /* sched_get_del_files */

const char *
sched_get_sched_dir(void)
{
	return SIQ_SCHED_WORKING_DIR;
} /* sched_get_sched_dir */

const char *
sched_get_reports_dir(void)
{
	return SIQ_SCHED_REPORTS_DIR;
} /* sched_get_reports_dir */

const char *
sched_get_edit_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return siq_edit_dir;
} /* sched_get_edit_dir */

const char *
sched_get_jobs_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return sched_jobs_dir;
} /* sched_get_jobs_dir */


const char *
sched_get_run_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return sched_run_dir;
} /* sched_get_run_dir */

const char *
sched_get_trash_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return sched_trash_dir;
}

const char *
sched_get_lock_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return sched_lock_dir;
}

const char *
sched_get_snap_dir(void)
{
	if (global_params_load() == FAIL) {
		return NULL;
	}

	return sched_snap_dir;
}

const char *
sched_get_pidfile(void)
{
	if (global_params_load() == FAIL) {
		return (char *)NULL;
	}
	return tc->root->scheduler->pid_file;
} /* sched_get_pidfile */

const char *
sched_get_version(void)
{
	return SIQ_APP_NAME;
} /* sched_get_version */

const char *
sched_get_full_log_name(void)
{
	if (global_params_load() == FAIL) {
		return (char *)NULL;
	}
	return &full_log_name[0];
} /* sched_get_full_log_name */

int
sched_get_min_workers_per_policy(void)
{
	/* Default to 1 */
	if (global_params_load() == FAIL) {
		return 1;
	}
	return tc->root->scheduler->min_workers_per_policy;
}

int
get_max_concurrent_jobs(struct siq_gc_conf *conf)
{
	int max_jobs = -1, total_workers = 0, supported_jobs = 0;

	if (conf == NULL)
		goto out;

	max_jobs = conf->root->scheduler->max_concurrent_jobs;
	read_total_workers(&total_workers);

	/* If worker pools values aren't initialized, fall back to a default */
	if (total_workers <= 0 || conf->root->coordinator->skip_work_host) {
		if (max_jobs == -1)
			max_jobs = DEFAULT_MAX_JOBS;
		goto out;
	}

	/* If the cluster can't support the max jobs, or if the max jobs has
	 * not been set, return the max it can support given the number of
	 * workers available. */
	supported_jobs =
		total_workers / conf->root->scheduler->min_workers_per_policy;
	if (max_jobs == -1 || supported_jobs < max_jobs)
		max_jobs = supported_jobs;

out:
	return max_jobs;
}


int
sched_get_max_concurrent_jobs(void)
{
	int max_jobs = -1;

	if (global_params_load() == FAIL) {
		goto out;
	}

	max_jobs = get_max_concurrent_jobs(tc);

out:
	return max_jobs;
}

int
sched_get_log_level(void)
{
	if (global_params_load() == FAIL) {
		return -1;
	}
	if (tc->root->scheduler->log_level < 0)
		tc->root->scheduler->log_level = 0;
	if (tc->root->scheduler->log_level >= SEVERITY_TOTAL)
		tc->root->scheduler->log_level = SEVERITY_TOTAL - 1;
	return tc->root->scheduler->log_level;
}

static int
global_params_load(void)
{
	struct isi_error *error = NULL;

	if (load_need == 0) {
		return SUCCESS;
	}
	if (is_loaded == 1) {
		siq_gc_conf_free(tc);
		is_loaded = 0;
	}

	tc = siq_gc_conf_load(&error);
	if (error) {
		log(ERROR, "%s: error in siq_conf_load: %s", __func__, 
		    isi_error_get_message(error));
		isi_error_free(error);
		return FAIL;
	}
	siq_gc_conf_close(tc);

	sprintf(siq_edit_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_APP_WORKING_DIR,
	    SCHED_NAME_EDIT_DIR);
	sprintf(sched_jobs_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_SCHED_WORKING_DIR,
	    SCHED_NAME_JOBS_DIR);
	sprintf(sched_run_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_SCHED_WORKING_DIR,
	    SCHED_NAME_RUN_DIR);
	sprintf(sched_trash_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_SCHED_WORKING_DIR,
	    SCHED_NAME_TRASH_DIR);
	sprintf(sched_lock_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_SCHED_WORKING_DIR,
	    SCHED_NAME_LOCK_DIR);
	sprintf(sched_snap_dir, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_SCHED_WORKING_DIR,
	    SCHED_NAME_SNAP_DIR);
	sprintf(full_log_name, "%s/%s",
	    is_fake ? fake_base_dir : SIQ_APP_LOG_DIR,
	    SIQ_APP_LOG_FILE);
	load_need = 0;
	is_loaded = 1;

	return SUCCESS;
}

void
set_fake_dirs(const char *new_fake_base_dir)
{
	is_fake = 1;

	strncpy(fake_base_dir, new_fake_base_dir, sizeof(fake_base_dir) - 1);
}
