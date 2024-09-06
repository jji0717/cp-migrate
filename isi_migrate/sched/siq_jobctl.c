#include <isi_util/isi_assert.h>

#include "isi_util/util.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_migrate/migr/isirep.h"
#include "siq_jobctl_private.h"
#include "sched_utils.h"

static struct search_file search_file_set[SEARCH_FILE_COUNT] = {
	{SCHED_JOB_PAUSE, SIQ_JS_PAUSED},
	{SCHED_JOB_CANCEL, SIQ_JS_CANCELLED},
	{SCHED_JOB_PREEMPT, SIQ_JS_PREEMPTED}
};

struct job_lock {
	char policy_id[MAXPATHLEN + 1];
	int fd;
	int refcount;
};

static char	job_id[MAXPATHLEN + 1];
static char	old_snap_name[MAXPATHLEN + 1];
static char	new_snap_name[MAXPATHLEN + 1];
static char	tmp_snap_name[MAXPATHLEN + 1];
struct job_lock job_lock = { .policy_id[0] = 0, .fd = -1, .refcount = 0 };

/**
 * Returns the job_id. 
 * Note: use it after siq_job_grab or siq_job_create only.
 */
char *
siq_get_job_id(void)
{
	return (char *) job_id;
}

/**
 * Cancels a job for a policy @pid. Return 0 on success, or EINVAL if
 * the specified job is not running.
 * Q: Behavior on an attempt to cancel already canceled job.
 */
int
siq_job_cancel(char *pid)
{
	 int	ret;

	 ret = job_file_add_rm(pid, SIQ_JC_CANCEL);
	 return ret;
} /* siq_job_cancel */

/**
 * Pauses a job for a policy @pid. Return 0 on success, or EINVAL if
 * the specified job is not running.
 * Q: Behavior on an attempt to pause already paused job.
 */
int
siq_job_pause(char *pid)
{
	 int	ret;

	 ret = job_file_add_rm(pid, SIQ_JC_PAUSE);
	 return ret;
} /* siq_job_pause */

/**
 * Resumes a job for a policy @pid. Return 0 on success, or EINVAL if
 * the specified job has not been paused.
 */
int
siq_job_resume(char *pid)
{
	 int	ret;

	 ret = job_file_add_rm(pid, SIQ_JC_RESUME);
	 return ret;
} /* siq_job_resume */


/**
 * Loads running job report, for progress tracking purposes.
 * @param pid : valid policy id
 * @param report : job execution report 
 * 
 * @returns
 * 0        on success
 * EFAULT   on invalid inputs
 * ENOENT   if unable to find a running job with id @pid.
 */
int
siq_job_report_load(char *pid, struct siq_job_summary **report)
{
	char		job_dir[MAXPATHLEN + 1];
	char		filename[MAXPATHLEN + 1];
	enum sched_ret	control_ret;
	int		ret = 0;
	int		retry_count;
	bool		found = false;
	struct		isi_error *error = NULL;

	retry_count = 0;
	control_ret = SCH_RETRY;
	while ((control_ret == SCH_RETRY) && (retry_count < 2)) {
		control_ret = job_dir_search(pid, job_dir);
		switch (control_ret) {
		case SCH_YES:
			sprintf(filename, "%s/%s", job_dir, REPORT_FILENAME);
			*report = siq_job_summary_load_readonly(
			    filename, &error);
			if (error || *report == NULL) {
				isi_error_free(error);
				error = NULL;
				control_ret = SCH_RETRY;
				retry_count++;
			} else {
				found = true;
			}
			break;

		case SCH_NO:
			retry_count = 2;
			ret = ENOENT;
			break;

		case SCH_ERR:
			return errno;

		default:
			return EINVAL;
		}
	}
	if (found) {
		ret = 0;
	}
	return ret;
}

/**
 * Loads list of all running job reports, for progress tracking purposes.
 * @param jobs		List of running jobs. Each item in the list contains
 *			the policy_id and corresponding report.
 */
void
siq_job_report_load_running(struct siq_running_jobs **jobs,
    struct isi_error **error_out)
{
	int			ret;
	DIR			*dirp;
	char			filename[MAXPATHLEN + 1];
	char			policy_id[MAXPATHLEN + 1];
	struct dirent		*dp;
	struct siq_running_job	*job = NULL, *cur = NULL;
	struct siq_running_jobs *head = NULL;
	struct siq_job_summary	*summary = NULL;
	struct isi_error	*error = NULL;

	head = calloc(1, sizeof(struct siq_running_jobs));
	ASSERT(head != NULL);

	/* If the run dir doesn't exist, then there are no running jobs. We
	 * should not ignore other errors */
	if ((dirp = opendir(sched_get_run_dir())) == NULL) {
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Unable to open run directory: %s",
			    sched_get_run_dir());
		}
		goto out;
	}

	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR || dp->d_name[0] == '.')
			continue;

		sched_decomp_name(dp->d_name, policy_id, NULL, NULL);

		/* Not probable, but skip unexpected directory names in the
		 * run dir */
		if (strlen(policy_id) != POLICY_ID_LEN)
			continue;

		ret = snprintf(filename, MAXPATHLEN + 1, "%s/%s/%s",
		    sched_get_run_dir(), dp->d_name, REPORT_FILENAME);

		if (ret < 0 || (size_t)ret >= MAXPATHLEN + 1) {
			error = isi_system_error_new(EINVAL, "Failed to get "
			    "path to job summary: path too long (%s/%s/%s)",
			    sched_get_run_dir(), dp->d_name, REPORT_FILENAME);
			goto out;
		}

		/* Errors are okay here. Job could be just starting/cleaning
		 * up and job summary doesn't exist. Just move on. */
		summary = siq_job_summary_load_readonly(filename, &error);
		if (error) {
			isi_error_free(error);
			error = NULL;
			siq_job_summary_free(summary);
			summary = NULL;
			continue;
		}

		job = calloc(1, sizeof(struct siq_running_job));
		ASSERT(job != NULL);

		job->policy_id = strdup(policy_id);
		job->job_summary = summary;
		summary = NULL;

		/* add job to the list */
		if (cur == NULL) {
			head->job = job;
			cur = head->job;
		} else {
			cur->next = job;
			cur = cur->next;
		}
	}

out:
	if (dirp != NULL)
		closedir(dirp);

	*jobs = head;

	isi_error_handle(error, error_out);
}

void
siq_running_jobs_free(struct siq_running_jobs *jobs)
{
	struct siq_running_job *current = NULL, *next = NULL;

	if (jobs == NULL)
		return;

	current = jobs->job;

	while (current != NULL) {
		next = current->next;
		FREE_AND_NULL(current->policy_id);
		siq_job_summary_free(current->job_summary);
		FREE_AND_NULL(current);
		current = next;
	}

	FREE_AND_NULL(jobs);
}

static struct siq_gc_spec *
check_and_load_spec(const char *policy_id, struct isi_error ** error_out)
{
	struct siq_gc_spec *gcspec = NULL;
	enum sched_ret ret;
	char job_dir[MAXPATHLEN + 1];
	char spec_path[MAXPATHLEN + 1];
	struct stat st;
	struct isi_error *error = NULL;

	ret = job_dir_search(policy_id, job_dir);
	if (ret) 
		goto out;

	SZSPRINTF(spec_path, "%s/%s", job_dir, SPEC_FILENAME);
	
	if (stat(spec_path, &st) != -1) {
		gcspec = siq_gc_spec_load(spec_path, &error);
		if (error || !gcspec)
			goto out;
	}
	
out:
	if (error) {
		siq_gc_spec_free(gcspec);
		gcspec = NULL;
	}

	isi_error_handle(error, error_out);
	return gcspec;

}

struct siq_gc_spec *
siq_job_spec_load(const char *policy_id, struct isi_error **error_out)
{
	struct siq_gc_spec *gcspec = NULL;
	struct isi_error *error = NULL;

	gcspec = check_and_load_spec(policy_id, &error);
	if (error || gcspec) 
		goto out;

out:
	if (error) { 
		siq_gc_spec_free(gcspec);
		gcspec = NULL;
	}

	isi_error_handle(error, error_out);
	return gcspec;
}

/**
 * Returns job @pid status.
 * If the job specified is not found, SIQ_JS_NONE is returned.
 */
enum siq_job_state
siq_job_state(char *pid)
{
	enum siq_job_state	j_state = SIQ_JS_NONE;
	struct siq_job_summary	*job_summary;
	int			ret;

	ret = siq_job_report_load(pid, &job_summary);
	if (ret == 0) {
		j_state = job_summary->total->state;
		siq_job_summary_free(job_summary);
	}
	return j_state;
}

/* Sets the state of an UNGRABBED job in the run directory. If the
 * ungrabbed job is not found, or there is an error modifying its report,
 * an error is returned. */
void
siq_ungrabbed_job_set_state(char *pid, enum siq_job_state state,
    struct isi_error **error_out)
{
	struct siq_gc_job_summary *job_summ = NULL;
	char report_file[MAXPATHLEN + 1]; 
	struct isi_error *error = NULL;

	snprintf(report_file, MAXPATHLEN + 1, "%s/%s/%s", sched_get_run_dir(),
	    pid, REPORT_FILENAME);
	job_summ = siq_gc_job_summary_load(report_file, &error);
	if (error)
		goto out;

	job_summ->root->summary->total->state = state;
	siq_gc_job_summary_save(job_summ, &error);

out:
 	siq_gc_job_summary_free(job_summ);
	isi_error_handle(error, error_out);
}

void
siq_job_grab(const char *orig_job_dir, char **policy_id_p,
    char **new_job_dir_p)
{
	int count;
	int ret = -1;
	char *last_slash;
	const char *COORD_FAKE_READ_ONLY_FILE = "/tmp/coord_fake_read_only";

	log(TRACE, "siq_job_grab: enter dir=%s", orig_job_dir);

	/* From orig_job_dir, extract the policy id dir should be
	 * "/ifs/.ifsvar/modules/tsm/sched/run/POLICYID_NODENUM" */
	last_slash = strrchr(orig_job_dir, '/');
	ASSERT(last_slash);

	count = sched_decomp_name(last_slash + 1, job_id, NULL, NULL);
	ASSERT(count == 2);
	*policy_id_p = job_id;

	/* New job directory include coord pid */
	asprintf(new_job_dir_p, "%s_%d", orig_job_dir, getpid());
	ASSERT(new_job_dir_p);

	if (access(COORD_FAKE_READ_ONLY_FILE, R_OK) == 0) {
		log(ERROR, "Faking read-only cluster as file %s exists",
		    COORD_FAKE_READ_ONLY_FILE);
		errno = EROFS;
	} else {
		ret = rename(orig_job_dir, *new_job_dir_p);
	}

	if (ret != 0) {
		/* Failure here is generally caused by the cluster being in
		 * read-only mode. Regardless, of the error, if the policy
		 * directory can't be renamed, there's nothing more that can
		 * be done for this policy, especially as there's no way to
		 * update a report to show this error. Force an exit with
		 * _exit() so that the coordinator's exiting() function is not
		 * called (exiting() will attempt to write a report which
		 * can't be one on this rename failure) */
		log(ERROR, "%s: Can't start policy %s: %s", __func__,
		    job_id, strerror(errno));
		_exit(-1);
	}
}

int
siq_job_create(char *name, char **policy_id_p, char **job_dir_p)
{
	DIR *dirp = NULL;
	struct dirent *dp = NULL;
	char *tmp_job_dir = NULL, tmp_job_id[MAXPATHLEN + 1];
	char report_filename[MAXPATHLEN + 1];
	char spec_file[MAXPATHLEN + 1];
	struct siq_policies *policies = NULL;
	struct siq_policy *policy = NULL;
	struct siq_job_summary *summary = NULL;
	struct siq_gc_job_summary *gcs = NULL;
	int ret = -1;
	bool locked = false;
	struct isi_error *isierror = NULL;

	policies = siq_policies_load_readonly(&isierror);
	if (!policies) {
		log(ERROR, "Can't start policy %s. Can't load policies. ",
		    name);
		goto out;
	}

	/* First get policy by name */
	policy = siq_policy_get_by_name(policies, name);
	if (!policy) {
		log(ERROR, "Can't start policy %s: policy not found", name);
		goto out;
	}

	/* Now scan to see if this policy is already being run or scheduled
	 * to be run */
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "Can't start policy %s. Can't open dir %s: %s",
		    name, sched_get_run_dir(), strerror(errno));
		goto out;
	}

	strncpy(job_id, policy->common->pid, sizeof(job_id) - 1);
	job_id[sizeof(job_id) - 1] = '\0';

	locked = siq_job_lock(job_id, &isierror);
	if (isierror)
		goto out;

	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR)
			continue;

		sched_decomp_name(dp->d_name, tmp_job_id, NULL, NULL);
		if (strcmp(tmp_job_id, policy->common->pid) == 0) {
			log(ERROR, "Can't start policy %s. "
			    "Already scheduled to run now.", name);
			goto out;
		}
	}

	closedir(dirp);
	dirp = NULL;
	*policy_id_p = strdup(policy->common->pid);

	asprintf(&tmp_job_dir, "%s/%s",
	    sched_get_edit_dir(), policy->common->pid);
	ASSERT(tmp_job_dir);

	asprintf(job_dir_p, "%s/%s_%d_%d",
	    sched_get_run_dir(), policy->common->pid,
	    sched_get_my_node_number(), getpid());
	ASSERT(*job_dir_p);

	if (mkdir(tmp_job_dir, (mode_t)(S_IRWXU | S_IRWXG | S_IRWXO)) != 0) {
		log(ERROR,
		    "Can't start policy %s. Can't create directory %s: %s",
		    name, *job_dir_p, strerror(errno));
		goto out;
	}

	summary = siq_job_summary_create();
	summary->action = SIQ_ACT_SYNC;
	summary->total->state = SIQ_JS_SCHEDULED;
	summary->total->start = time(0);
	summary->total->end = time(0) - 1000; // XXX What should this be????
	summary->retry = false;
	summary->dead_node = false;

	sprintf(report_filename, "%s/%s", tmp_job_dir, REPORT_FILENAME);
	gcs = siq_job_summary_save_new(summary, report_filename, &isierror);
	if (isierror) {
		siq_job_summary_free(summary);
		unlink(tmp_job_dir);
		log(ERROR, "Can't start policy %s. Can't create summary.",
		    name);
		goto out;
	}
	/* <sched_edit_run_dir>/<policy_id>/ spec file create */
	sprintf(spec_file, "%s/%s", tmp_job_dir, SPEC_FILENAME);
	siq_spec_save(policy, spec_file, &isierror);
	if (isierror) {
		unlink(tmp_job_dir);
		log(ERROR, "Failed to save spec: %s", 
		    isi_error_get_message(isierror));
		goto out;
	}

	/* <sched_edit_run_dir>/<policy_id> -> <sched_run_dir>/<policy_id> */
	if (rename(tmp_job_dir, *job_dir_p) != 0) {
		unlink(tmp_job_dir);
		log(ERROR, "Can't start policy %s. Can't rename %s to %s: %s.",
		    name, tmp_job_dir, *job_dir_p, strerror(errno));
		goto out;
	}
	ret = 0;

 out:
 	if (locked)
		siq_job_unlock();
	siq_gc_job_summary_free(gcs);
	siq_policies_free(policies);
	if (tmp_job_dir)
		free(tmp_job_dir);
	if (dirp)
		closedir(dirp);
	isi_error_free(isierror);
	return ret;
}

int
siq_job_ungrab(const char *job_dir, struct siq_policy* policy, char *err_msg)
{
	int ret = -1;
 	struct siq_gc_job_summary *job_summ = NULL;
 	char report_filename[MAXPATHLEN + 1];
	int jid = 0;
	struct isi_error *error = NULL;
	struct siq_policy *pol_copy = NULL;

	log(TRACE, "%s: ungrabbing %s for %s", __func__, job_dir, job_id);
	sprintf(report_filename, "%s/%s", job_dir, REPORT_FILENAME);
	job_summ = siq_gc_job_summary_load(report_filename, &error);
	if (error) {
		log(ERROR, "Failed to load report %s from %s for ungrab: %s",
		    job_id, job_dir, isi_error_get_message(error));
		isi_error_free(error);
		goto out;
	}
	
	if (!job_summ->root->summary->total) {
		CALLOC_AND_ASSERT(job_summ->root->summary->total, 
		    struct siq_job_total);
	}
	job_summ->root->summary->total->start = time(0);
	/* Avoid a divide by zero error for average throughput */
	job_summ->root->summary->total->end = time(0) + 1; 
	job_summ->root->summary->total->state = SIQ_JS_FAILED;
	if (!job_summ->root->summary->chunks) {
		CALLOC_AND_ASSERT(job_summ->root->summary->chunks, 
		    struct siq_job_chunks);
	}
	job_summ->root->summary->chunks->running = 0;
	job_summ->root->summary->chunks->failed = 0;
	job_summ->root->summary->chunks->success = 0;
	job_summ->root->summary->chunks->total = 0;
	siq_job_error_msg_append(job_summ->root->summary, err_msg);
	job_summ->root->summary->retry = 0;
	job_summ->root->summary->dead_node = 0;

	if (policy) {
		pol_copy = siq_policy_copy(policy);
		siq_report_spec_free(job_summ->root->summary->job_spec);
		job_summ->root->summary->job_spec = 
		    siq_spec_create_from_policy(pol_copy);
	}
	
	/* Handle job_id */
	jid = siq_reports_status_check_inc_job_id(job_id, &error);
	if (error) {
		log(ERROR, "Failed to get job_id for ungrab (%s): %s",
		    job_id, isi_error_get_message(error));
		goto out;
	}

	job_summ->root->summary->job_id = jid;
	
	siq_gc_job_summary_save(job_summ, &error);
	if (error) {
		log(ERROR, "Failed to save job report %s under %s: %s",
		    job_id, job_dir, isi_error_get_message(error));
		goto out;
	}
	
	siq_job_cleanup(job_summ, job_id, job_dir, &error);
	if (error)
		goto out;

out:
	if (pol_copy)
		free(pol_copy);
 	siq_gc_job_summary_free(job_summ);
	isi_error_free(error);
 
	return ret;
}

int
siq_job_kill(char *this_job_id, struct siq_policy* policy, int node_num,
    char *err_msg)
{
	char *job_dir1 = NULL, *job_dir2 = NULL, *policy_id, *job_dir3 = NULL;
	int ret = 0;
	bool locked = false;
	struct isi_error *error = NULL;

	log(TRACE, "%s: this_job_id=%s  err_msg=%s",
	    __func__, this_job_id, err_msg);

	log(ERROR, "Killing policy: %s", err_msg);

	locked = siq_job_lock(this_job_id, &error);
	if (error)
		goto out;

	/* node_num of zero implies that a node hasn't yet grabbed the job */
	if (node_num == 0) {
		asprintf(&job_dir1, "%s/%s", sched_get_run_dir(), this_job_id);
		asprintf(&job_dir2, "%s/%s_0", sched_get_run_dir(),
		    this_job_id);

		/* Rename to include node number */
		ret = rename(job_dir1, job_dir2);
		if (ret != 0) {
			if (errno != ENOENT) {
				log(ERROR, "Rename error %s ==> %s : %s",
				    job_dir1, job_dir2, strerror(errno));
				ret = -1;
			}
			goto out;
		}
	} else {
		asprintf(&job_dir2, "%s/%s_%d",
		    sched_get_run_dir(), this_job_id, node_num);
	}

	siq_job_grab(job_dir2, &policy_id, &job_dir3);
	log(TRACE, "%s: policy_id=%s   new_job_dir=%s",
	    __func__, policy_id, job_dir3);
	ret = siq_job_ungrab(job_dir3, policy, err_msg);

 out:
	isi_error_free(error);
 	if (locked)
		siq_job_unlock();
	free(job_dir1);
	free(job_dir2);
	free(job_dir3);
	return ret;
}

static void
siq_policy_report_populate_from_pid_jid(sqlite3 *db, 
    struct reportdb_pid_jid_record **pj_records, int num_pj_records, 
    struct siq_composite_report ***reports_out, int *num_reports_out, 
    struct isi_error **error_out)
{
	struct reportdb_pid_jid_record *current_pj_record = NULL;
	struct siq_composite_report **to_return = NULL;
	struct siq_composite_report *new_report = NULL;
	struct siq_job_summary **current_job_reports = NULL;
	int num_job_summ = 0;
	int counter = 0;
	struct isi_error *error = NULL;
	
	to_return = calloc(num_pj_records + 1, 
	    sizeof(struct siq_composite_report *));
	ASSERT(to_return);
	
	for (counter = 0; counter < num_pj_records; counter++) {
		current_pj_record = pj_records[counter];
		ASSERT(current_pj_record);
		
		num_job_summ = siq_reportdb_get_reports_by_job_id(db, 
		    current_pj_record->policy_id, current_pj_record->job_id,
		    &current_job_reports, &error);
		if (error) {
			siq_composite_reports_free(to_return);
			goto out;
		}
		
		siq_reportdb_create_composite_report(current_job_reports, 
		    num_job_summ, &new_report, &error);
		if (error) {
			siq_composite_reports_free(to_return);
			goto out;
		}
		
		to_return[counter] = new_report;
		current_job_reports = NULL;
		new_report = NULL;
	}
	
	*reports_out = to_return;
	*num_reports_out = num_pj_records;
out:
	isi_error_handle(error, error_out);
}

/**
 * Loads all existing policy reports.
 * @input: 
 * pid: valid policy id
 * @output:
 * report: pointer to array of length *reports of siq_job_summary_t 
 *	structures containing requested reports.
  * reports: number of reports returned
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_load(char *pid, struct siq_composite_report ***report, 
    int *reports)
{
	sqlite3 *db = NULL;
	struct reportdb_pid_jid_record **pj_records = NULL;
	int num_pj_records = 0;
	struct isi_error *error = NULL;
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	num_pj_records = siq_reportdb_get_all_reports_by_policy_id(db, pid, 
	    &pj_records, &error);
	if (error) {
		goto out;
	}
	
	siq_policy_report_populate_from_pid_jid(db, pj_records, num_pj_records, 
	    report, reports, &error);
	
out:
	siq_reportdb_pid_jid_records_free(pj_records);
	siq_reportdb_close(db);
	if (error) {
		isi_error_free(error);
		return -1;
	}
	
	return 0;
} /* siq_policy_report_load */

/**
 * Loads all policy reports within specified date range (inclusive)
 * using policy end time
 * @input: 
 * pid		valid policy id
 * start	start time of range
 * end		end time of range
 * @output:
 * report	pointer to array of length *reports of siq_job_summary_t 
 *		structures containing requested reports.
 * reports	On successful return is set to the number of
 *		reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_load_date_range(char *pid, time_t start, time_t end,
		       struct siq_composite_report ***report, int *reports)
{
	sqlite3 *db = NULL;
	struct reportdb_pid_jid_record **pj_records = NULL;
	int num_pj_records = 0;
	struct isi_error *error = NULL;
	
	//Check args
	if (end < start) {
		return -1;
	}
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	num_pj_records = siq_reportdb_get_daterange_reports_by_policy_id(db, 
	    pid, start, end, &pj_records, &error);
	if (error) {
		goto out;
	}
	
	siq_policy_report_populate_from_pid_jid(db, pj_records, num_pj_records, 
	    report, reports, &error);
	
out:
	siq_reportdb_pid_jid_records_free(pj_records);
	siq_reportdb_close(db);
	if (error) {
		isi_error_free(error);
		return -1;
	}
	
	return 0;
} /* siq_policy_report_load_date_range */

/**
 * Loads the last n policy reports from db
 * @input: 
 * db		SQLite3 database handle
 * pid		valid policy id
 * n		number of reports
 * @output:
 * report	pointer to array of length *reports of siq_job_summary_t
 *		structures containing requested reports.
 * reports	On successful return is set to the number of
 *		reports loaded.
 * error_out	On failure is set to internal error
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_reportdb_load_recent(sqlite3 *db, char *pid, int n,
    struct siq_composite_report ***report, int *reports,
    struct isi_error **error_out)
{
	int num_pj_records = 0;
	struct reportdb_pid_jid_record **pj_records = NULL;
	struct isi_error *error = NULL;

	num_pj_records = siq_reportdb_get_latest_reports_by_policy_id(db, pid,
	    n, &pj_records, &error);
	if (error) {
		goto out;
	}

	siq_policy_report_populate_from_pid_jid(db, pj_records, num_pj_records,
	    report, reports, &error);
out:
	siq_reportdb_pid_jid_records_free(pj_records);

	if (error) {
		isi_error_handle(error, error_out);
		return -1;
	}

	return 0;
} /* siq_reportdb_load_recent */

/**
 * Loads the last n policy reports from db
 * @input:
 * pid		valid policy id
 * n		number of reports
 * @output:
 * report	pointer to array of length *reports of siq_job_summary_t 
 *		structures containing requested reports.
 * reports	On successful return is set to the number of
 *		reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_load_recent(char *pid, int n, 
    struct siq_composite_report ***report, int *reports)
{
	sqlite3 *db = NULL;
	struct isi_error *error = NULL;
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	siq_reportdb_load_recent(db, pid, n, report, reports, &error);
	if (error) {
		goto out;
	}

out:
	siq_reportdb_close(db);
	
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		log(ERROR, "ERRNO = %d", errno);
		isi_error_free(error);
		return -1;
	}
	
	return 0;
} /* siq_policy_report_load_recent */

/**
 * Returns the total number of reports for a given policy using database
 * @input: 
 * pid		valid policy id
 * @output:
 * reports	On successful return is set to the number of
 *		reports found.
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_get_count(char *pid, int *reports)
{
	sqlite3 *db = NULL;
	int to_return = -1;
	struct isi_error *error = NULL;
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	siq_reportdb_get_count_by_policy_id(db, pid, &to_return, &error);
	if (error) {
		goto out;
	}
	
	*reports = to_return;
out:
	siq_reportdb_close(db);	
	if (error) {
		isi_error_free(error);
		return -1;
	}
	return 0;
} /* siq_policy_report_get_count */

/**
 * Loads all policy reports for a given job_id
 * using policy end time
 * @input: 
 * pid		valid policy id
 * job_id	valid job_id
 * @output:
 * report	pointer to array of length *reports of siq_job_summary_t 
 *		structures containing requested reports.
 * reports	On successful return is set to the number of
 *		reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_load_job_id(char *pid, char *job_id,
    struct siq_composite_report ***report, int *reports)
{
	sqlite3 *db = NULL;
	int num_job_summ = 0;
	struct siq_composite_report **to_return = NULL;
	struct siq_composite_report *new_report = NULL;
	struct siq_job_summary **current_job_reports = NULL;
	struct isi_error *error = NULL;
	
	//Check args
	if (job_id == NULL) {
		return -1;
	}
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	num_job_summ = siq_reportdb_get_reports_by_job_id(db, 
	    pid, job_id, &current_job_reports, &error);
	if (error) {
		goto out;
	}
	if (num_job_summ == 0) {
		*report = NULL;
		*reports = 0;
		goto out;
	}
	
	siq_reportdb_create_composite_report(current_job_reports, 
	    num_job_summ, &new_report, &error);
	if (error) {
		goto out;
	}
	
	to_return = calloc(2, sizeof(struct siq_composite_report *));
	ASSERT(to_return);
	
	to_return[0] = new_report;
	*report = to_return;
	*reports = 1;
out:
	siq_reportdb_close(db);
	if (error) {
		if (current_job_reports) {
			siq_job_summary_free_array(current_job_reports, 
			    num_job_summ);
			free(current_job_reports);
		}
		isi_error_free(error);
		return -1;
	}
	
	return 0;
} /* siq_policy_report_load_date_range */

int
siq_policy_report_load_all_recent(int n, struct siq_composite_report ***report, 
    struct isi_error **error_out)
{
	sqlite3 *db = NULL;
	int num_pj_records = 0;
	struct reportdb_pid_jid_record **pj_records = NULL;
	struct isi_error *error = NULL;
	int num_reports = 0;
	
	db = siq_reportdb_open(NULL, &error);
	if (error) {
		goto out;
	}
	
	num_pj_records = siq_reportdb_get_all_latest_reports(db, n, &pj_records,
	    &error);
	if (error) {
		goto out;
	}
	
	siq_policy_report_populate_from_pid_jid(db, pj_records, num_pj_records, 
	    report, &num_reports, &error);
out:
	siq_reportdb_pid_jid_records_free(pj_records);
	siq_reportdb_close(db);
	
	isi_error_handle(error, error_out);
	
	return num_reports;
} /* siq_policy_report_load_date_range */

/**
 * Returns current job status.
 * @path = .../run/<job id>_<node>_<coord pid> (siq_job_grab() return)
 *
 * If the path inaccessible, SIQ_JS_NONE is returned.
 */
enum siq_job_state
siq_job_current_state(const char *path)
 {
	enum siq_job_state	j_state = SIQ_JS_RUNNING;
	enum sched_ret		is_file_ret;
	int			ii;

	if (path == (char *)NULL) {
		return SIQ_JS_NONE;
	}

	for (ii = 0; ii < SEARCH_FILE_COUNT; ii++) {
		is_file_ret = is_file_exist(path,
		    search_file_set[ii].file_name);
		switch (is_file_ret) {
			case SCH_YES:
				j_state = search_file_set[ii].job_state;
				break;
			case SCH_NO:
				break;
			case SCH_ERR:
				return SIQ_JS_NONE;
			default:
				return SIQ_JS_NONE;
		} /* switch end */
	} /* for end */
	return j_state;
 } /* siq_job_current_state */

char *
siq_get_old_snap_name(char *pol_id)
{
	sprintf(old_snap_name, "SIQ-%s-latest", pol_id);
	return old_snap_name;
}

char *
siq_get_new_snap_name(char *pol_id)
{
	sprintf(new_snap_name, "SIQ-%s-new", pol_id);
	return new_snap_name;
}

char *
siq_get_tmp_snap_name(char *pol_id)
{
	sprintf(tmp_snap_name, "SIQ-%s-tmp", pol_id);
	return tmp_snap_name;
}

static void
remove_all_locks(SNAP *snap)
{
	int i;
	struct snap_lock_info *locks;
	locks = snapshot_get_locks(snap);
	for (i = 0; locks[i].lockid != (ifs_lockid_t)0; i++) {
		snapshot_rmlock(snap, &locks[i].lockid, NULL);
	}
	free(locks);
}

void
siq_remove_snapshot(char *name, int remove_locks,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_str i_name;
	SNAP *this_snap = NULL;

	isi_str_init(&i_name, name, strlen(name)+1,
	    ENC_DEFAULT, ISI_STR_NO_MALLOC);

	if (remove_locks) {
		this_snap = snapshot_open_by_name(&i_name, &error);
		if (error)
			goto out;
		remove_all_locks(this_snap);
		snapshot_commit(this_snap, &error);
		if (error)
			goto out;
		snapshot_close(this_snap);
		this_snap = NULL;
	}

	snapshot_destroy(&i_name, &error);
	if (error)
		goto out;

out:
	if (this_snap)
		snapshot_close(this_snap);

	isi_error_handle(error, error_out);
}

void
siq_set_snapshot_expiration(char *name, time_t expiration,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_str i_name;
	SNAP *this_snap = NULL;

	isi_str_init(&i_name, name, strlen(name)+1,
	    ENC_DEFAULT, ISI_STR_NO_MALLOC);

	this_snap = snapshot_open_by_name(&i_name, &error);
	if (error)
		goto out;

	snapshot_set_expiration(this_snap, &expiration);
	snapshot_commit(this_snap, &error);
	if (error)
		goto out;

out:
	if (this_snap)
		snapshot_close(this_snap);

	isi_error_handle(error, error_out);
}

static int
job_file_add_rm(char *pid, siq_job_control_t jc)
{
	char		job_dir[MAXPATHLEN + 1];
	enum sched_ret	control_ret = SCH_RETRY;
	int		ret = 0;

	while (control_ret == SCH_RETRY) {
		control_ret = job_dir_search(pid, &job_dir[0]);
		
		switch (control_ret) {
		case SCH_YES:
			control_ret = job_control(&job_dir[0], jc);
			break;
		case SCH_NO:
			ret = EINVAL;
			break;
		default:
			ret = EINVAL;
			break;
		}
	}
	return ret;
}

static enum sched_ret
job_dir_search(const char *pid, char *job_dir)
{
	char		_job_id[MAXPATHLEN + 1];
	char		filename[MAXPATHLEN + 1];
	DIR		*dirp;
	struct dirent	*dp;
	enum sched_ret	ret = SCH_NO;
	int		count;
	struct siq_job_summary *job_summary = NULL;
	struct isi_error *error = NULL;

	if ((dirp = opendir(sched_get_run_dir())) == NULL) {
		ret = SCH_ERR;
		goto out;
	}

	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR) 
			continue;

		_job_id[0] = 0;
		count = sched_decomp_name(dp->d_name, &_job_id[0], NULL, NULL);
		if (count >= 1 && strcmp(pid, _job_id) == 0) {
			sprintf(job_dir, "%s/%s", sched_get_run_dir(),
			    dp->d_name);
			sprintf(filename, "%s/%s", job_dir, REPORT_FILENAME);
			job_summary = siq_job_summary_load_readonly(filename,
			    &error);
			if (error) {
				isi_error_free(error);
				continue;
			}
			siq_job_summary_free(job_summary);
			ret = SCH_YES;
			break;
		}
	}

out:
	if (dirp)
		closedir(dirp);
	return ret;
}

static enum sched_ret
is_file_exist(const char *path, const char *file_name)
{
	int		ret;
	struct stat	sb;
	char		buffer[MAXPATHLEN + 1];

	sprintf(buffer, "%s/%s", path, file_name);
	ret = stat(buffer, &sb);
	if (ret == 0) {
		return SCH_YES;
	}
	if (errno == ENOENT) {
		return SCH_NO;
	}
	return SCH_ERR;
} /* sched_is_file_exist */

static enum sched_ret
job_control(const char *job_dir, siq_job_control_t jc)
{
	char		work_file[MAXPATHLEN + 1];
	char		target_file[MAXPATHLEN + 1];
	const char	*file_name;
	int		act_type; /* 0 - move, 1 - remove */
	int		pfd;
	int		ret;
	enum sched_ret	control_ret;

	switch (jc) {
		case SIQ_JC_PAUSE:
			file_name = SCHED_JOB_PAUSE;
			act_type = 0;
			break;
		case SIQ_JC_RESUME:
			file_name = SCHED_JOB_PAUSE;
			act_type = 1;
			break;
		case SIQ_JC_CANCEL:
			file_name = SCHED_JOB_CANCEL;
			act_type = 0;
			break;
		default:
			return SCH_ERR;
	} /* switch end */

	sprintf(&work_file[0], "%s/%s", sched_get_edit_dir(), file_name);
	sprintf(&target_file[0], "%s/%s", job_dir, file_name);

	if (act_type == 0) {
		//if (is_file_exist(&target_file[0]) != SCH_YES) {
			unlink(&work_file[0]);
			pfd = open(&work_file[0], O_CREAT | O_EXCL | O_RDWR,
			    0666);
			if(pfd < 0) {
				return SCH_ERR;
			}
			close(pfd);
			ret = rename(&work_file[0], &target_file[0]);
		/* else {
			// need to set control_ret = SCH_RETRY
			ret = 1;
			errno = ENOENT;
		}*/
		if (ret == 0) {
			control_ret = SCH_YES;
		} else if (errno == ENOENT) {
			control_ret = SCH_RETRY;
		} else {
			control_ret = SCH_ERR;
		}
	} else {
		ret = rename(&target_file[0], &work_file[0]);
		if ((ret == 0) || (errno == ENOENT)) {
			control_ret = SCH_YES;
		} else {
			control_ret = SCH_ERR;
		}
	}
	unlink(&work_file[0]);
	return control_ret;
} /* job_control */

/* @return: true, false */
void
siq_job_time(struct siq_source_record *source_rec, time_t *start_time,
    time_t *end_time)
{
	siq_source_get_sched_times(source_rec, start_time, end_time);
}


/*      _       _        ____ _                              
 *     | | ___ | |__    / ___| | ___  __ _ _ __  _   _ _ __  
 *  _  | |/ _ \| '_ \  | |   | |/ _ \/ _` | '_ \| | | | '_ \ 
 * | |_| | (_) | |_) | | |___| |  __/ (_| | | | | |_| | |_) |
 *  \___/ \___/|_.__/   \____|_|\___|\__,_|_| |_|\__,_| .__/ 
 *                                                    |_|    
 */

static void
state_update(struct siq_source_record *srec, const char *policy_id,
    const struct siq_job_summary *jc, struct isi_error **error_out)
{
	bool do_domain_mark = true;
	ifs_domainid_t domain_id = 0;
	uint32_t gen = 0;
	int ret = -1;
	struct domain_entry dom_entry;
	enum siq_failback_state fb_state = SIQ_FBS_NONE;
	ifs_snapid_t latest_snap = INVALID_SNAPID;
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	switch (jc->action) {
	case SIQ_ACT_FAILBACK_PREP:
		log(NOTICE, "Next prep phase: Domain Mark.");
		if (!siq_source_manual_failback_prep(srec)) {
			siq_source_set_pending_job(srec,
			    SIQ_JOB_FAILBACK_PREP_DOMAIN_MARK);
		}
		siq_source_set_failback_state(srec,
		    SIQ_FBS_PREP_DOM_TREEWALK);
		break;

	case SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK:
		/* If domain mark was started by  'accelerated_failback',
		 * then don't continue to failback restore operation. */
		siq_source_get_failback_state(srec, &fb_state);
		if (jc->job_spec->coordinator->accelerated_failback &&
		    !jc->job_spec->datapath->disable_stf &&
		    !jc->job_spec->datapath->disable_fofb &&
		    fb_state != SIQ_FBS_PREP_DOM_TREEWALK)
		{
			/* Bug 138523: Additionally remove any tmp target
			   record that was made for domain_mark. We can tell if
			   it should be removed because the policy_name field will
			   be empty. */
			lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK,
			    &error);
			if (error)
				goto out;
			trec = siq_target_record_read(policy_id, &error);
			if (error)
				goto out;
			if (trec != NULL && trec->policy_name == NULL) {
				siq_target_delete_record(policy_id, true, true,
				    &error);
				if (error)
					goto out;
			}
			break;
		}

		log(NOTICE, "Next prep phase: Restore.");
		siq_source_set_failback_state(srec, SIQ_FBS_PREP_RESTORE);
		if (!siq_source_manual_failback_prep(srec)) {
			siq_source_set_pending_job(srec,
			    SIQ_JOB_FAILBACK_PREP_RESTORE);
		}
		siq_source_get_latest(srec, &latest_snap);
		siq_source_set_restore_latest(srec, latest_snap);
		break;

	case SIQ_ACT_FAILBACK_PREP_RESTORE:
		log(NOTICE, "Next prep phase: Finalize.");
		siq_source_set_failback_state(srec, SIQ_FBS_PREP_FINALIZE);
		if (!siq_source_manual_failback_prep(srec)) {
			siq_source_set_pending_job(srec,
			    SIQ_JOB_FAILBACK_PREP_FINALIZE);
		}
		break;

	case SIQ_ACT_FAILBACK_PREP_FINALIZE:
		siq_source_set_failback_state(srec, SIQ_FBS_SYNC_READY);
		break;

	case SIQ_ACT_SYNC:
		/*
		 * If the accelerated_failback option is defined, check if
		 * the additional domain mark work needs to be done
		 */
		if (jc->job_spec->coordinator->accelerated_failback &&
		    !jc->job_spec->datapath->disable_stf &&
		    !jc->job_spec->datapath->disable_fofb) {
			find_domain(jc->job_spec->datapath->root_path,
			    DT_SYNCIQ, false, &domain_id, &gen, &error);
			if (error) {
				goto out;
			} else if (domain_id != 0) {
				ret = dom_get_entry(domain_id, &dom_entry);
				if (ret != 0) {
					error = isi_system_error_new(errno,
					    "Unable to get domain id %llx on "
					    "path %s", domain_id,
					    jc->job_spec->datapath->root_path);
					goto out;
				} else if (dom_entry.d_flags & DOM_READY)
					do_domain_mark = false;
			}

			if (do_domain_mark) {
				siq_source_set_pending_job(srec,
				    SIQ_JOB_FAILBACK_PREP_DOMAIN_MARK);
			}
		}
	default:
		siq_source_set_last_known_good_time(srec, jc->total->start);
		break;
	}

out:
	if (lock_fd != -1)
		close(lock_fd);
	siq_target_record_free(trec);
	isi_error_handle(error, error_out);
}

static int
generate_unique_report_name(const char *policy_id, char *buf, int size,
    char *fmt, int seed, struct isi_error **error_out)
{

	int count = 0;
	int new_seed = seed;
	char to_path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	/*
	 * report files could collide if two consecutive jobs
	 * could finish within same second. So try to generate
	 * unique name but only few times i.e 1000 if this does not
	 * work then there is something wrong.
	 */
	while(count < 1000) {
		snprintf(buf, size, fmt, new_seed);
		snprintf(to_path, sizeof(to_path), "%s/%s/%s",
		    sched_get_reports_dir(), policy_id, buf);
		
		if (access(to_path, F_OK) != 0) {
			if (errno == ENOENT) {
				goto out;
			}
			error = isi_system_error_new(errno,
			    "Unable to stat %s", to_path);
			goto out;
		}
		count++;
		new_seed = seed + count;
	}

	if (count >= 1000) {
		error = isi_system_error_new(EINVAL,
		    "Unable to find unique report name for policy %s",
		    policy_id);
	}

out:
	isi_error_handle(error, error_out);
	return new_seed;
}

static void
write_report(struct siq_gc_job_summary *gcjs, const char *policy_id,
    const char *job_dir, struct isi_error **error_out)
{
	sqlite3* report_db = NULL;	
	char report_name[MAXPATHLEN + 1];
	char from_path[MAXPATHLEN + 1];
	char to_path[MAXPATHLEN + 1];
	char job_id_str[20];
	char lin_str[30];
	struct siq_job_summary *summary = gcjs->root->summary;
	struct stat report_stat;
	struct isi_error *error = NULL;
	int res;
	int exists = 0;
	int end_time = 0;

	snprintf(from_path, sizeof(from_path), "%s/%s", job_dir, 
	    REPORT_FILENAME);
	end_time = generate_unique_report_name(policy_id, report_name,
	    sizeof(report_name), SIQ_POLICY_REPORT_NAMEFMT, 
	    summary->total->end, &error);
	if (error)
		goto out;
	/* Match the end time with the one generated for report file name */
	summary->total->end = end_time;

	snprintf(to_path, sizeof(to_path), "%s/%s/%s", sched_get_reports_dir(), 
	    policy_id, report_name);

	/* Clean out worker gcfg tree */
	siq_workers_free(&summary->workers);
	
	siq_gc_job_summary_save(gcjs, &error);
	if (error)
		goto out;
	summary = gcjs->root->summary;

	if (link(from_path, to_path) != 0 && access(to_path, F_OK) != 0) {
		error = isi_system_error_new(errno, "%s: failed to move job "
		    "summary to report directory", __func__);
		goto out;
	}

	log(DEBUG, "%s: wrote new report at %s", __func__, to_path);

	report_db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error) {
		log(ERROR, "%s: failed to open report database: %s",
		    __func__, isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;	
	} else {
		snprintf(job_id_str, sizeof(job_id_str), "%d", summary->job_id);
		res = stat(to_path, &report_stat);
		if (res != 0) {
			log(ERROR, "%s: failed to stat report", __func__);
			goto out;
		}
		snprintf(lin_str, sizeof(lin_str), "%lld", report_stat.st_ino);
		siq_reportdb_get_count_by_lin(report_db, lin_str, &exists,
		    &error);
		if (error)
			goto out;
		if (!exists) {
			siq_reportdb_insert(report_db, lin_str,
			    (char *)policy_id, job_id_str, 
			    summary->total->end, &error);
			if (error)
				goto out;
		}
	}

	/* move delete list if one was generated */
	snprintf(from_path, sizeof(from_path), "%s/%s", job_dir, 
	    sched_get_del_files());
	if (access(from_path, F_OK) == 0) {
		snprintf(to_path, sizeof(to_path), "%s/%s/%s-%ld", 
		    sched_get_reports_dir(), policy_id, sched_get_del_files(), 
		    summary->total->end);
		res = rename(from_path, to_path);
		if (res != 0) {
			log(ERROR, "%s: failed to rename delete list",
			    __func__);
			goto out;
		}
	}
		
out:
	if (report_db)
		siq_reportdb_close(report_db);
	isi_error_handle(error, error_out);
}

static bool
policy_exists(const char *policy_id, char *policy_name, size_t n,
    struct isi_error **error_out)
{
	struct siq_policies *policies = NULL;
	struct siq_policy *policy = NULL;
	struct isi_error *error = NULL;
	bool found = false;

	policies = siq_policies_load_nolock(&error);
	if (error)
		goto out;

	policy = siq_policy_get_by_pid(policies, (char *)policy_id);
	if (policy != NULL) {
		found = true;
		if (strlen(policy->common->name) >= n) {
			error = isi_system_error_new(EINVAL,
			    "Policy name %s too long", policy->common->name);
			goto out;
		}
		strncpy(policy_name, policy->common->name, n);
	}

out:
	siq_policies_free(policies);
	isi_error_handle(error, error_out);
	return found;
}

static bool
is_job_successful(enum siq_job_state state)
{
	return (state == SIQ_JS_SUCCESS) || (state == SIQ_JS_SUCCESS_ATTN) ||
	    (state == SIQ_JS_SKIPPED);
}

int
trash_job_dir(const char *job_dir, const char *policy_id)
{
	char trash_path[MAXPATHLEN + 1];
	char rm_command[MAXPATHLEN + 8];
	char *tmp = NULL;
	int res;

	log(DEBUG, "%s: trashing job dir %s", __func__, job_dir);

	tmp = strstr(job_dir, policy_id);
	ASSERT(tmp);

	/* move the dir before deleting in case the delete is incomplete
	 * so the sched doesn't try to start a brand new run */
	snprintf(trash_path, sizeof(trash_path), "%s/%s",
	    sched_get_trash_dir(), tmp);
	res = rename(job_dir, trash_path);
	if (res != 0) {
		log(ERROR, "error moving job dir %s to %s: %s",
		    job_dir, trash_path, strerror(errno));
		goto out;
	}

	/* scheduler periodically empties the trash dir and might beat us */
	snprintf(rm_command, sizeof(rm_command), "rm -rf %s", trash_path);
	system(rm_command);

out:
	return res;
}

static int
make_job_available(const char *job_dir, const char *policy_id) {
	char available_path[MAXPATHLEN + 1];
	int res;

	snprintf(available_path, MAXPATHLEN + 1, "%s/%s", sched_get_run_dir(),
	    policy_id);

	res = rename(job_dir, available_path);	
	if (res != 0) {
		log(ERROR, "error moving job dir %s to %s: %s",
		    job_dir, available_path, strerror(errno));
	}

	return res;
}

void
siq_job_cleanup(struct siq_gc_job_summary *gcjs, const char *policy_id,
    const char *job_dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_job_summary *summary = gcjs->root->summary;
	bool skip_times_update = false;
	bool rec_exists = false;
	bool rec_has_owner = false;
	bool pol_exists = false;
	bool pending = false;
	char policy_name[MAXNAMLEN];
	struct siq_source_record *srec = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(policy_id);

	pol_exists = policy_exists(policy_id, policy_name,
	    sizeof(policy_name), &error);
	if (error)
		goto out;

	rec_exists = siq_source_has_record(policy_id);
	if (rec_exists) {
		srec = siq_source_record_load(policy_id, &error);
		if (error)
			goto out;
		ASSERT(srec != NULL);
		rec_has_owner = siq_source_has_owner(srec);
	}

	switch (summary->total->state) {
	case SIQ_JS_SUCCESS:
	case SIQ_JS_SUCCESS_ATTN:
		/* On policy success, clear record of nodes that failed to
		 * connect to the target */
		if (rec_exists)
			siq_source_clear_node_conn_failed(srec);

		/* decision whether to delete source rec depends on if this
		 * is a local sync. don't delete if local! */
		if (!pol_exists && rec_exists &&
		    summary->action == SIQ_ACT_FAILOVER_REVERT) {
			siq_source_delete(policy_id, &error);
			if (error)
				goto out;
			rec_exists = false;
		}
		break;
	
	case SIQ_JS_CANCELLED:
		break;

	case SIQ_JS_SKIPPED:
		break;

	case SIQ_JS_PAUSED:
	case SIQ_JS_PREEMPTED:
		skip_times_update = true;
		pending = true;
		break;

	case SIQ_JS_FAILED:
		/* If we failed, but the user also requested a cancel, don't
		 * retry this job. */
		if (get_job_state(job_dir) == SIQ_JS_CANCELLED)
			summary->retry = false;

		if (summary->retry)
			skip_times_update = true;
		break;
	
	default:
		log(ERROR, "Invalid state %d", summary->total->state);
		summary->total->state = SIQ_JS_FAILED;
		break;
	}

	if (rec_exists && skip_times_update == false) {
		siq_source_set_sched_times(srec, summary->total->start,
		    summary->total->end);
		siq_source_set_pending_job(srec, SIQ_JOB_NONE);

		if (is_job_successful(summary->total->state)) {
			/* last known time is updated in this func. */
			state_update(srec, policy_id, summary, &error);
			if (error)
				goto out;
		}
	}

	/* move summary (and delete list if it exists) to reports dir. Don't
	 * write a report for preempted or paused jobs, they're technically
	 * still running. */
	if (!pending) {
		write_report(gcjs, policy_id, job_dir, &error);
		if (error)
			goto out;
	} else {
		/* Clean out worker gcfg tree */
		siq_workers_free(&gcjs->root->summary->workers);

		siq_gc_job_summary_save(gcjs, &error);
		if (error)
			goto out;
	}

	/* Reassign summary, as it is freed and reallocated in both
	 * write_report() and siq_gc_job_summary_save(). */
	summary = gcjs->root->summary;

	/* coordinator releases ownership prior to calling this. still need
	 * to check in case this is scheduler 'killing' a job. bypassing move
	 * of job dir to trash will cause another cleanup attempt later */
	if (rec_has_owner)
		goto out;

	if (rec_exists) {
		siq_source_record_save(srec, &error);
		if (error)
			goto out;

		if (is_job_successful(summary->total->state) && pol_exists)
			siq_cancel_alert(SIQ_ALERT_RPO_EXCEEDED, policy_name,
			    policy_id);
	}

	/* if trash_job_dir fails, another coordinator will be started
	 * that will attempt to remove the job dir after seeing that the
	 * job is completed. We don't trash preempted or paused policies, we
	 * make them available to be started again. */
	if (!pending)
		trash_job_dir(job_dir, policy_id);
	else
		make_job_available(job_dir, policy_id);

out:
	siq_source_record_free(srec);
	isi_error_handle(error, error_out);
}


/*      _       _       _               _    
 *     | | ___ | |__   | |    ___   ___| | __
 *  _  | |/ _ \| '_ \  | |   / _ \ / __| |/ /
 * | |_| | (_) | |_) | | |__| (_) | (__|   < 
 *  \___/ \___/|_.__/  |_____\___/ \___|_|\_\
 */

static bool
siq_job_lock_internal(const char *policy_id, bool block,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char lock_filename[MAXPATHLEN + 1];
	bool locked = false;
	int flags = O_EXLOCK | O_RDONLY | O_CREAT;

	ASSERT(policy_id);
	ASSERT(job_lock.refcount >= 0);

	log(TRACE, "%s: block = %s", __func__, block ? "true" : "false");

	if (strlen(policy_id) != POLICY_ID_LEN) {
		error = isi_system_error_new(errno,
		    "%s called with invalid policy id %s", __func__,
		    policy_id);
		goto out;
	}

	if (job_lock.refcount == 0) {
		ASSERT(job_lock.policy_id[0] == 0);
		ASSERT(job_lock.fd == -1);

		sprintf(lock_filename, "%s/.%s.lock", sched_get_lock_dir(),
		    policy_id);
		if (!block)
			flags |= O_NONBLOCK;
		job_lock.fd = open(lock_filename, flags, 0400);
		if (job_lock.fd == -1) {
			if (errno == EWOULDBLOCK && block == false) {
				goto out;
			} else {
				error = isi_system_error_new(errno,
				    "failed to open job lock (%s) for policy "
				    "id %s", lock_filename, policy_id);
				goto out;
			}
		}

		/* sched holds lock while forking the coordinator. this
		 * prevents the child from holding the lock */
		if (isi_set_close_on_exec(job_lock.fd) != 0) {
			error = isi_system_error_new(errno,
			    "failed to set close on exec flag for job lock fd");
			close(job_lock.fd);
			job_lock.fd = -1;
			goto out;
		}

		strcpy(job_lock.policy_id, policy_id);
	} else {
		ASSERT(strcmp(job_lock.policy_id, policy_id) == 0);
		ASSERT(job_lock.fd != -1);
	}

	job_lock.refcount++;
	locked = true;

	log(DEBUG, "%s: polid %s refcount %d->%d", __func__,
	    policy_id, job_lock.refcount - 1, job_lock.refcount);

out:    
	isi_error_handle(error, error_out);
	return locked;
}


bool
siq_job_lock(const char *policy_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool res;

	res = siq_job_lock_internal(policy_id, true, &error);

	isi_error_handle(error, error_out);
	return res;
}

bool
siq_job_try_lock(const char *policy_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool res;

	res = siq_job_lock_internal(policy_id, false, &error);

	isi_error_handle(error, error_out);
	return res;
}

void
siq_job_unlock()
{
	ASSERT(job_lock.refcount > 0);
	ASSERT(job_lock.fd != -1);

	log(TRACE, "%s", __func__);

	if (job_lock.refcount == 1) {
		close(job_lock.fd);
		job_lock.fd = -1;
		job_lock.policy_id[0] = 0;
	}

	job_lock.refcount--;
	
	log(DEBUG, "%s: polid %s refcount %d->%d", __func__,
	    job_lock.policy_id, job_lock.refcount + 1, job_lock.refcount);
}

bool
siq_job_locked()
{
	bool locked;

	if (job_lock.fd == -1) {
		ASSERT(job_lock.refcount == 0);
		ASSERT(job_lock.policy_id[0] == 0);
		locked = false;
	} else {
		ASSERT(job_lock.refcount > 0);
		ASSERT(job_lock.policy_id[9] != 0);
		locked = true;
	}

	log(DEBUG, "%s: job is %s", __func__, locked ? "locked" : "unlocked");
	return locked;
}

void
siq_job_rm_lock_file(const char *policy_id)
{
	char lock_filename[MAXPATHLEN + 1];

	ASSERT(policy_id && *policy_id);
	ASSERT(strlen(policy_id) == POLICY_ID_LEN);

	log(TRACE, "%s", __func__);

	sprintf(lock_filename, "%s/.%s.lock", sched_get_lock_dir(), policy_id);
	if (unlink(lock_filename) != 0) {
		if (errno == ENOENT) {
			log(DEBUG, "job lock file '%s' does not exist",
			    lock_filename);
			goto out;
		}
		log(ERROR, "failed to delete job lock file '%s': %s",
		    lock_filename, strerror(errno));
	}

out:
	return;
}
