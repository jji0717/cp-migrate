#include <sys/queue.h>
#include <netdb.h>
#include <arpa/inet.h>
#include "sched_includes.h"
#include "sched_timeout.h"
#include "sched_cluster.h"
#include "sched_grabbed.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_alert.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_ufp/isi_ufp.h"

#define ALERT_MIN_INTERVAL		600 /* 10 min */

struct grabbed_list g_grabbed_list;

/* tracks already sent alerts for policies so we don't spam celog */
static struct ptr_vec alerts_list = PVEC_INITIALIZER;

static int coord_start(struct sched_ctx *, struct sched_job *job);
static enum sched_ret resolve_race_with_other_node(struct sched_ctx *sctx,
    struct sched_job *job, const char *dir_name);

static int coord_restart(struct sched_ctx *, struct sched_job *job);
static int process_jobs(struct sched_ctx *, struct ptr_vec *jobs,
     int max_jobs);
static int collect_jobs(struct sched_ctx *, struct ptr_vec *jobs,
    int max_jobs, struct int_set *up_nodes);
static bool is_job_running(const char *pattern_policy_id_str,
    const char *test_job_str);
static int
get_num_jobs_pool_supports(struct sched_ctx *sctx, char *restrict_by,
    struct isi_error **error_out);

static void
exec_policy_local_delete(char *policy_id, char *policy_name)
{
	struct siq_policies *policies = NULL;
	struct siq_policy *policy = NULL;
	struct isi_error *error = NULL;

	policies = siq_policies_load(&error);
	if (error) {
		log(ERROR, "%s: failed to load policies: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}
	policy = siq_policy_get_by_pid(policies, policy_id);
	if (!policy) {
		log(ERROR, "%s: failed to remove policy %s: Policy not found", 
		    __func__, policy_name);
		goto out;
	}
	siq_policy_remove_local(policies, policy, NULL, &error);
	if (error) {
		log(ERROR, "%s: failed to remove policy %s: %s", __func__, 
		    policy_name, isi_error_get_message(error));
		goto out;
	} else {
		log(ERROR, "%s: removed policy %s locally only.  "
		    "Failed to connect to target.", __func__, 
		    policy_name);
	}

out:
	siq_policies_free(policies);
	isi_error_free(error);
}

struct alert_entry {
	char policy_id[POLICY_ID_LEN+1];
	time_t last_alert;
};

/* block an alert for a policy from being created more often than once
 * every ALERT_MIN_INTERVAL seconds. alleviates celog queue overflow
 * (bug 71872) */
static void
create_alert(char *policy_id, char *policy_name, char *target, char *err_msg)
{
	void **ptr = NULL;
	struct alert_entry *cur = NULL;
	bool match = false;
	time_t now = time(NULL);

	/* clean up expired alert entries. list is ordered by last alert time
	 * (increasing) so we stop at an unexpired entry */
	while (pvec_empty(&alerts_list) == false) {
		cur = *(struct alert_entry **)pvec_first(&alerts_list);

		if (cur->last_alert + ALERT_MIN_INTERVAL > now)
			break;

		pvec_remove(&alerts_list, 0);
		free(cur);
		cur = NULL;
	}

	PVEC_FOREACH(ptr, &alerts_list) {
		cur = *ptr;

		if (strcmp(policy_id, cur->policy_id) == 0) {
			match = true;
			break;
		}
	}

	if (match) {
		log(DEBUG, "Skipping sched alert for policy %s", policy_name);
	} else {
		siq_create_alert(SIQ_ALERT_POLICY_START, policy_name,
		    policy_id, target, NULL, err_msg);
		cur = calloc(1, sizeof(struct alert_entry));
		ASSERT(cur);
		strncpy(cur->policy_id, policy_id, POLICY_ID_LEN);
		cur->last_alert = now;
		pvec_append(&alerts_list, cur);
	}
}

static void
kill_job(char *policy_id, char *policy_name, struct siq_policy *policy, 
    int node_num, char *err_msg)
{
	log(TRACE, "%s: killing job for policy %s (%d)", __func__,
	    policy_name, node_num);
	create_alert(policy_id, policy_name,
	    policy ? policy->target->dst_cluster_name : "", err_msg);
	siq_job_kill(policy_id, policy, node_num, err_msg);

	/* As the job won't be starting, no longer keep the
	 * job in the watched list */
	remove_from_watched(policy_id);
}

void
delete_or_kill_job(char *policy_id, char *policy_name, 
    struct siq_policy *policy, int node_num, char *err_msg, bool is_delete)
{
	if (is_delete) {
		exec_policy_local_delete(policy_id, policy_name);
	} else {
		kill_job(policy_id, policy_name, policy, node_num, err_msg);
	}
}

static int
check_policy_can_run_on_local_node(struct sched_job *job)
{
	struct sched_cluster_health health = {};
	enum start_status status;
	bool is_delete;
	int res = -1;
	struct siq_source_record *srec = NULL;
	struct isi_error *error = NULL, *error2 = NULL;

	/* kind of dangerous but failover revert can delete source record at
	 * end of run since policy doesn't really exist. check for record
	 * before proceeding */
	srec = siq_source_record_load(job->policy_id, &error2);
	if (error2) {
		log(DEBUG, "%s", isi_error_get_message(error2));
		goto out;
	}

	/* Check to determine if the pool used for this policy has
	 * nodes that are usable in the cluster.*/
	cluster_check(job, &health, srec, &error);
	if (error) {
		/* Set status to force killing of job */
		log(DEBUG, "%s", isi_error_get_message(error));
		status = SCHED_NO_NODE_OK;

		/* Job will be killed. Reset the failed connection flags so
		 * we can retry next time the job is started manually or
		 * scheduled to run */
		siq_source_clear_node_conn_failed(srec);
		siq_source_record_save(srec, &error2);
		if (error2)
			goto out;
	} else {
		/* There are nodes usable for this policy. Now check to see if
		 * the local node can be used to start this policy. */
		log(TRACE, "%s: has_at_least_one_node_ok=%d  "
		    "local_node_ok=%d  in_pool=%d",
		    __func__, health.has_at_least_one_node_ok,
		    health.local_node_ok, health.local_in_pool);
		status = policy_local_node_check(job, &health, &error);
	}

	job->is_delete = siq_source_is_policy_deleted(srec);

	/* Defer to other node by going to next active policy (if
	 * there's one) and let any other node decide to run the
	 * policy. */
	if (status == SCHED_DEFER_TO_OTHER_NODE) {
		log(TRACE, "%s: deferring %s", __func__, job->policy_name);
		add_to_deferred_list(job, false);
		siq_source_set_node_conn_failed(srec, get_local_node_id());
		siq_source_record_save(srec, &error2);
		goto out;
	}

	/* Kill any policy that can't start */
	if (error) {
		ASSERT(status == SCHED_TIMEOUT || status == SCHED_NO_NODE_OK);
		is_delete = (status == SCHED_NO_NODE_OK && job->is_delete);
		delete_or_kill_job(job->policy_id, job->policy_name,
		    job->policy, 0, (char *)isi_error_get_message(error),
		    is_delete);
		goto out;
	}

	ASSERT(status == SCHED_NODE_OK);
	res = 0;

out:
	siq_source_record_free(srec);
	isi_error_free(error);
	isi_error_free(error2);
	return res;
}	

enum sched_action
grabbed_action(struct sched_job *job)
{ 
	struct grabbed_entry *prev_grabbed;
	
	log(TRACE, "%s: enter", __func__);
	ASSERT(job->progress == PROGRESS_GRABBED && !job->local);
	
	prev_grabbed = grabbed_list_find(&g_grabbed_list,
	    job->current_dir);
	
	/* If grabbed job wasn't in grabbed_list, keep for the next time we
	 * investigate jobs stuck in GRABBED */
	if (!prev_grabbed) {
		log(TRACE, "%s: %s not previously grabbed. Inserting", __func__,
		    job->policy_name);
		grabbed_list_insert(&g_grabbed_list, job->current_dir,
		    job->ino);
		return ACTION_SKIP;
	}
	log(TRACE, "%s: %s in grabbed list. Time diff=%lu", __func__,
	    job->policy_name, time(0) - prev_grabbed->timestamp);
		
	/* Same directory name but different inode indicates the previous run
	 * directory has been replaced by a new one. Replace it in the
	 * grabbed_list to start the timer for it */
	if (prev_grabbed->ino != job->ino) {
		log(TRACE, "%s: %s different inode. Reinserting",
		    __func__, job->policy_name);
		grabbed_list_remove(&g_grabbed_list, prev_grabbed);
		grabbed_list_insert(&g_grabbed_list, job->current_dir,
		    job->ino);
		return ACTION_SKIP;
	}

	if (time(0) - prev_grabbed->timestamp > SCHED_GRABBED_TIMEOUT) {
		log(INFO, "%s: %s timed out in grabbed list. Will grab",
		    __func__, job->policy_name);
		grabbed_list_remove(&g_grabbed_list, prev_grabbed);
		job->restoring = true;
		return ACTION_GRAB;
	} 
	log(TRACE, "%s: %s not timed out in grabbed list. Will leave in list",
	    __func__, job->policy_name);
	
	/* The job has been seen in the grabbed state before, but it hasn't
	 * time out yet. Mark it so it will left in the list */
	prev_grabbed->seen = true;
 	 
	/* And skip any processing on this node */
	return ACTION_SKIP;
}

enum sched_action
remote_action(struct sched_job *job, struct int_set *up_nodes)
{ 
	log(TRACE, "%s: enter", __func__);
	
	ASSERT(!job->local && job->progress != PROGRESS_AVAILABLE);

	if (!int_set_contains(up_nodes, job->node_num)) {
		log(INFO, "%s: Policy %s (%s) is on down node. "
		    "This node taking over.", __func__,
		    job->policy_name, job->current_dir);
		job->restoring = true;
		return ACTION_GRAB;
	}

	if (job->progress == PROGRESS_STARTED) 
		return ACTION_SKIP;
	
	/* Must be in GRABBED state */
	return grabbed_action(job);
}
	
static enum sched_action
local_action(struct sched_ctx *sctx, struct sched_job *job)
{
	enum sched_action res = ACTION_ERR;
	
	log(TRACE, "%s: enter", __func__);
	ASSERT(job->local && job->progress != PROGRESS_AVAILABLE);
	
	if (job->progress == PROGRESS_STARTED) { 
		/* Will restart the coordinator if needed */
		if (coord_test(sctx, job) != 0)
			goto out;
	} else if (job->progress == PROGRESS_GRABBED) {
	/* Must be in GRABBED state. As the scheduler is supposed to initiate
	 * the coordinator immediately after putting it in the GRABBED state
	 * (putting in the STARTED state), if it was previously grabbed
	 * locally, try to start it again */
		if (coord_start(sctx, job) != 0)
			goto out;
	} else {
		ASSERT(0, "Invalid job progress state %d for '%s'",
		    job->progress, job->policy_name);
	}
	
	/* At this point the coordinator has been started successfully */
	res = ACTION_SKIP;

out:
	return res;
}

static int
populate_policy_info_from_spec(struct sched_job* job)
{
	struct siq_gc_spec *gcspec = NULL;
	struct isi_error *error = NULL;
	char spec_file[MAXPATHLEN + 1];
	int res = FAIL;
	bool record_exists = false;
	int lock_fd = -1;
	enum siq_action_type action;

	SZSPRINTF(spec_file, "%s/%s", job->current_dir, SPEC_FILENAME);
	spec_file[MAXPATHLEN] = '\0';

	gcspec = siq_gc_spec_load(spec_file, &error);
	if (!gcspec || error) 
		goto out;

	/* any actions that can be run without a policy (spec based only)
	 * need to be added to this list otherwise the scheduler will trash
	 * the run directory */
	action = gcspec->spec->common->action;
	if (action != SIQ_ACT_FAILOVER &&
	    action != SIQ_ACT_FAILOVER_REVERT &&
	    action != SIQ_ACT_FAILBACK_SYNC &&
	    action != SIQ_ACT_SNAP_REVERT_DOMAIN_MARK &&
	    action != SIQ_ACT_SYNCIQ_DOMAIN_MARK &&
	    action != SIQ_ACT_WORM_DOMAIN_MARK)
		goto out;

	lock_fd = siq_target_acquire_lock(job->policy_id, O_SHLOCK, &error);
	if (-1 == lock_fd || error)
		goto out;

	record_exists = siq_target_record_exists(job->policy_id);

	if (action != SIQ_ACT_SNAP_REVERT_DOMAIN_MARK &&
	    action != SIQ_ACT_SYNCIQ_DOMAIN_MARK &&
	    action != SIQ_ACT_WORM_DOMAIN_MARK && !record_exists)
		goto out;

	SZSPRINTF(job->target, "%s", gcspec->spec->target->dst_cluster_name);
	SZSPRINTF(job->policy_name, "%s", gcspec->spec->common->name);
	SZSPRINTF(job->restrict_by, "%s", 
	    gcspec->spec->target->restrict_by ?: "");
	job->force_interface = gcspec->spec->target->force_interface;	
	res = SUCCESS;

out:
	if (error)
		log(ERROR, "Error: %s", isi_error_get_message(error));

	if (-1 != lock_fd)
		close(lock_fd);

	isi_error_free(error);
	siq_gc_spec_free(gcspec);
	return res;
}

/* Get the policy associated with the job */
static int
populate_policy_info(struct siq_policies *policies, struct sched_job *job)
{
	struct siq_policy *policy = NULL;
	struct isi_error *isierror = NULL;
	int res = SUCCESS;

	policy = siq_policy_get(policies, job->policy_id, &isierror);
	if (!policy) {
		res = populate_policy_info_from_spec(job);
	        if (SUCCESS != res) 
			log(INFO, "Policy %s no longer exists", job->policy_id);
		goto out;
	}

	SZSPRINTF(job->target, "%s", policy->target->dst_cluster_name);
	SZSPRINTF(job->policy_name, "%s", policy->common->name);
	SZSPRINTF(job->restrict_by, "%s", policy->target->restrict_by ?: "");
	job->force_interface = policy->target->force_interface;
	job->priority = policy->scheduler->priority;
	job->policy = policy;

	log(TRACE, "%s: Read policy: name=%s  target=%s  restrict_by=%s  "
	    "force_interface=%d", __func__, job->policy_name, job->target,
	    policy->target->restrict_by ? job->restrict_by : "(none)",
	    job->force_interface);

out:
	isi_error_free(isierror);
	return res;
}

static bool
job_is_preempted(const char *run_dir, char *job_dir, char *policy_id)
{
	char path[MAXPATHLEN + 1];
	bool res = false;

	/* Check if the job is in the process of being preempted */
	snprintf(path, sizeof(path), "%s/%s/%s", run_dir, job_dir,
	    SCHED_JOB_PREEMPT);
	if (access(path, F_OK) == 0) {
		res = true;
		goto out;
	}

	/* Check if job has become available (the coordinator has
	 * finished preemption of the job dir) */
	snprintf(path, sizeof(path), "%s/%s", run_dir, policy_id);
	if (access(path, F_OK) == 0) {
		res = true;
		goto out;
	}

out:
	return res;
}

static bool
job_is_stopping(const char *run_dir, char *job_dir)
{
	char path[MAXPATHLEN + 1];
	bool res = false;

	/* Check if the job is in the process of being preempted */
	snprintf(path, sizeof(path), "%s/%s/%s", run_dir, job_dir,
	    SCHED_JOB_PREEMPT);
	if (access(path, F_OK) == 0) {
		res = true;
		goto out;
	}

	/* Check if the job is in the process of being canceled */
	snprintf(path, sizeof(path), "%s/%s/%s", run_dir, job_dir,
	    SCHED_JOB_CANCEL);
	if (access(path, F_OK) == 0) {
		res = true;
		goto out;
	}

	/* Check if the given job dir still exists. The above checks may have
	 * failed because the coordinator trashed/renamed the job dir. The
	 * order matters here, this check should always come after checking
	 * for flags first. */
	snprintf(path, sizeof(path), "%s/%s", run_dir, job_dir);
	if (access(path, F_OK) != 0) {
		res = true;
		goto out;
	}

out:
	return res;
}

/**
 * Initialize a job structure, filling it in as much as possible.
 * Parameters:
 *    dirp: An pointer to the open base run directory.
 *    dp: The just read directory entry from the base run directory.
 *    policies: Contents of the policy DB. Can be NULL to skip any filling in
 *              any policy specification field in the job.
 *    job: The job structure to populate.
 * Returns:
 *    SCH_YES: Job filled in succesfully.
 *    SCH_NO: The directory entry isn't for a job, or has since been removed.
 *    SCH_ERR: An I/O error while getting the directory entry's stats.
 */
static enum sched_ret
init_job(struct sched_ctx *sctx, DIR *dirp, struct dirent *dp,
    struct siq_policies *policies, struct sched_job *job)
{
	char node_str[MAXPATHLEN + 1], pid_str[MAXPATHLEN + 1];
	int ret = SCH_YES;
	struct stat st;

	bzero(job, sizeof(*job));

	job->ino = dp->d_fileno;

	job->progress = sched_decomp_name(dp->d_name,
	    job->policy_id, node_str, pid_str);
	if (strlen(job->policy_id) != POLICY_ID_LEN) {
		log(DEBUG, "%s: In run dir found invalid dir '%s'",
		    __func__, dp->d_name);
		job->progress = PROGRESS_NONE;
		ret = SCH_NO;
		goto done;
	}

	if (enc_fstatat(dirfd(dirp), dp->d_name,
	    dp->d_encoding, &st, AT_SYMLINK_NOFOLLOW) == -1) {
		/*
		 * The directory can be stolen by another node by the
		 * time we get here.
		 */
		if (errno == ENOENT) {
			log(DEBUG, "%s: Looks like another node has grabbed %s",
			    __func__, dp->d_name);
			ret = SCH_NO;
			goto done;
		}

		log(ERROR, "%s :fstat(%s): %s", __func__, dp->d_name,
		    strerror(errno));
		ret = SCH_ERR;
		goto done;
	}
	job->job_birth = st.st_birthtime;

	/* Pre-initialize all possible known run directories so we don't have
	 * to have to do it later */

	SZSPRINTF(job->current_dir, "%s/%s", sched_get_run_dir(), dp->d_name);

	/* The grabbed directory, if local ends ups grabbing it */
	SZSPRINTF(job->local_grabbed_dir, "%s/%s_%d",
	    sched_get_run_dir(), job->policy_id, sctx->node_num);

	SZSPRINTF(job->available_dir, "%s/%s",
	    sched_get_run_dir(), job->policy_id);

	if (job->progress == PROGRESS_GRABBED ||
	    job->progress == PROGRESS_STARTED) {
		job->node_num = atoi(node_str);
		if (job->node_num == sctx->node_num)
			job->local = true;
		if (job->progress == PROGRESS_STARTED)
			job->coord_pid = atoi(pid_str);
	}

	if (populate_policy_info(policies, job) == FAIL) {
		/* If can't get the policy, nothing can be done with this
		 * job. Its run directory needs to be cleaned up. But any
		 * other run directories should still be scanned.
		 * It's possible that the job was created and started since
		 * our last load of the policies list. In that case, the policy
		 * may actually exist and we shouldn't clean up the run dir.
		 * Only clean the run dir if it existed before our last load
		 * of the policies list */
		if (job->job_birth < sctx->plist_check_time)
			sched_unlink(sctx, job->current_dir);
		ret = SCH_NO;
		goto done;
	}

 done:
	log(INFO, "%s: dir=%s  progress=%d node=%d  ret=%d", __func__,
	    dp->d_name, job->progress, job->node_num, ret);  //// TRACE -> INFO
	return ret;
}

/* Either add a new sched_pool struct to "pools" for the given pool name
 * or increment the number of running jobs if the given pool already exists
 * in "pools" */
/////往pools中添加一个sched_pool,或者在一个已经存在的sched_pool中增加running jobs的数量
static void
increment_jobs_in_pool(char *restrict_by, struct ptr_vec *pools)
{
	void **cur = NULL;
	bool found = false;
	struct sched_pool *pool = NULL;

	/* See if we already know about this pool */
	PVEC_FOREACH(cur, pools) {
		pool = *cur;
		if (!strcmp(pool->restrict_by, restrict_by)) {
			found = true;
			break;
		}
	}

	/* Increment the number of jobs in the pool */
	if (!found) {
		pool = calloc(1, sizeof(struct sched_pool));
		ASSERT(pool != NULL);
		SZSPRINTF(pool->restrict_by, "%s", restrict_by);
		pvec_append(pools, (void *)pool);
	}
	pool->num_jobs++;
}

/* Populate "pools" with sched_pool structs(给这个sched_pool填充数据). One sched_pool struct will
 * be appended for each unique pool that running jobs are operating in. Each
 * struct will contain the pool name and the number of jobs in that pool */
static int
get_pool_stats(struct sched_ctx *sctx, struct ptr_vec *pools)
{
	DIR *dirp;
	int job_count = 0;
	char policy_id[MAXPATHLEN + 1];
	enum job_progress progress;
	struct dirent *dp;
	struct siq_policy *pol = NULL;
	struct siq_policies *policies = sctx->policy_list;

	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
		    sched_get_run_dir(), strerror(errno));
		return -1;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		progress = sched_decomp_name(dp->d_name, policy_id, 0, 0);
		if (progress < PROGRESS_STARTED)
			continue;

		/* Skip if job is preempted or canceled */
		if (job_is_stopping(sched_get_run_dir(), dp->d_name))
			continue;

		/* Skip if policy doesn't exist */
		pol = siq_policy_get_by_pid(policies, policy_id);
		if (pol == NULL)
			continue;

		increment_jobs_in_pool(pol->target->restrict_by ?: "", pools);

		job_count++;
	}

	closedir(dirp);
	return job_count;

}

/* If there are excess jobs, preempt(抢占) the lowest priority jobs to get back down
 * to the limit. First, preempt jobs from pools that don't have enough workers
 * to support all the jobs in it. Second, preempt any remaining excess jobs
 * so that the max concurrent job limit isn't violated.
 *
 * Note:
 * Passing (MAX_POL_PRIORITY+1) to preempt_lower_priority_job() makes every
 * running job a preemption candidate, but the function preempts jobs with the
 * lowest priority and smallest run duration first. This puts the excess jobs
 * in a "pending" state; they will be started first when another spot opens up.
 */
static void
sched_preempt_excess_jobs(struct sched_ctx *sctx)
{
	int excess_count = 0, pool_excess = 0, num_supported = 0, max_jobs;
	void **cur = NULL;
	struct ptr_vec pools = PVEC_INITIALIZER;
	struct sched_pool *pool = NULL;
	struct isi_error *error = NULL;

	max_jobs = sched_get_max_concurrent_jobs();
	if (max_jobs == -1)
		return;

	pvec_init(&pools);

	/* Get list of pools and num jobs in those pools. get_pool_stats()
	 * returns the total number of running jobs */
	excess_count = get_pool_stats(sctx, &pools) - max_jobs;

	/* Preempt excess jobs from pools that have more than they can
	 * support. Deduct preempted jobs from the overall excess count. */
	PVEC_FOREACH(cur, &pools) {
		pool = *cur;
		num_supported = get_num_jobs_pool_supports(sctx,
		    pool->restrict_by, &error);

		/* Don't make a decision about this pool if we hit an error */
		if (error) {
			isi_error_free(error);
			continue;
		}

		/* Move on if this pool is okay */
		if (pool->num_jobs <= num_supported)
			continue;

		/* This pool has more jobs than it can support. Preempt the
		 * pool's excess jobs and deduct it from our overall excess
		 * count. Assume all jobs are successfully preempted. The only
		 * way a preemption fails is if the run directory goes away or
		 * or if another scheduler already preempted the job. Either
		 * way, we can deduct the job from our excess count. */
		pool_excess = pool->num_jobs - num_supported;
		preempt_lower_priority_job(sctx, sched_get_run_dir(),
		    MAX_POL_PRIORITY + 1, pool_excess, pool->restrict_by);
		excess_count -= pool_excess;
	}

	/* If there are still excess jobs, preempt them from anywhere */
	if (excess_count > 0)
		preempt_lower_priority_job(sctx, sched_get_run_dir(),
		    MAX_POL_PRIORITY + 1, excess_count, NULL);

	free_and_clean_pvec(&pools);
}

static void
enforce_concurrent_job_limit(struct sched_ctx *sctx)
{
	char lock_filename[MAXPATHLEN + 1];
	int fd = -1;
	struct isi_error *error = NULL;

	/* Only one scheduler can do this at a time */
	snprintf(lock_filename, MAXPATHLEN + 1,  "%s/.check_max_jobs.lock",
	    sched_get_lock_dir());
	fd = acquire_lock(lock_filename, O_EXLOCK | O_NONBLOCK, &error);
	if (error) {
		log(DEBUG, "enforce_concurrent_job_limit: "
		    "failed to obtain lock, skipping");
		goto out;
	}

	sched_preempt_excess_jobs(sctx);

out:
	if (fd >= 0)
		close(fd);
	if (error)
		isi_error_free(error);
}

/* Entry: case L_TIMER
 * @return: SUCCESS, FAIL
 */
int
sched_local_node_work(struct sched_ctx *sctx)
{
	int ret = SUCCESS, max_jobs;
	struct ptr_vec jobs = PVEC_INITIALIZER;
	struct int_set INT_SET_INIT_CLEAN(up_nodes);
	struct isi_error *error = NULL;
	bool space_for_jobs;

	/* "jobs" is the queue of jobs this scheduler will attempt to start */

	grabbed_list_mark_all_unseen(&g_grabbed_list);

	update_node_max_workers(sctx, &error);
	if (error)
		goto done;

	/* Make sure the max concurrent job count isn't being violated. The
	 * max concurrent jobs allowed can change depending on worker pool
	 * settings, so we may have previously started an acceptable amount
	 * of jobs, but now we have too many. We need to preempt the lowest
	 * priority excess jobs. Only one scheduler may do this at a time to
	 * ensure the correct amount are preempted. */
	enforce_concurrent_job_limit(sctx);

/*
 * 1) Lookup of <policy_id> subdirectories within run. When lookup is finished,
 * the scheduler switches to an idle mode. 
 * 2) If <policy_id> directory is found then the scheduler attempts to rename
 * it to <policy_id>_<node_id> with help of rename() function. If this attempt
 * is failed then goto item 1.
 * 3) If renaming attempt is successful then the scheduler loads 
 * coordinator-process and sends to the coordinator the name of the renamed 
 * directory as an input. The coordinator renames the given directory to 
 * <policy_id>_<node_id>_<coordinator_id> and then coordinates job execution 
 * by distributing sub-jobs between worker-processes. 
 * 4) If <policy_id>_<node_id>_<coordinator_id> is found and node_id is
 * the node where the scheduler works then the scheduler checks whether 
 * there is a process with coordinator_id. If there is no such process_id 
 * we need to handle coordinator's error. 
 */
	log(TRACE, "sched_local_node_work: enter");

	get_up_devids(&up_nodes);
	if (int_set_size(&up_nodes) == 0) {
		log(ERROR, "There are no nodes that are up on the cluster");
		ret = FAIL;
		goto done;
	}
	if (!int_set_contains(&up_nodes, sctx->node_num)) {
		log(ERROR, "This node is not listed as an up node "
		    "on the cluster");
		ret = FAIL;
		goto done;
	}

	max_jobs = sched_get_max_concurrent_jobs(); ////默认配置50个jobs

	ret = collect_jobs(sctx, &jobs, max_jobs, &up_nodes);
	if (ret == FAIL)
		goto done;

	ret = process_jobs(sctx, &jobs, max_jobs);

	space_for_jobs = (get_running_jobs_count(sctx) <
	    sched_get_max_concurrent_jobs());
	timeout_deferred_jobs(space_for_jobs);
	clear_deferred_list();
	grabbed_list_remove_all_unseen(&g_grabbed_list);

 done:
	free_and_clean_pvec(&jobs);

	if (error) {
		log(ERROR, "sched_local_work error: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		ret = FAIL;
	}
	return ret;
}

/* Returns true if job1 should be scheduled before job2. This is used as the
 * comparison function when add_vec_elem is called for queue_job. */
static bool
sched_job_first(void *job1, void *job2)
{
	struct sched_job *s_job1 = (struct sched_job *)job1;
	struct sched_job *s_job2 = (struct sched_job *)job2;
	/*
	 * Return true if job1 should be scheduled before job2
	 *
	 * if Both have the same priority:
	 *        Sort by start time
	 * else:
	 *     Sort by priority
	 */

	if (s_job1->priority == s_job2->priority)
		return (s_job1->job_birth < s_job2->job_birth);
	return (s_job1->priority > s_job2->priority);
}

/* Add the job to our queue of jobs to start. Use sched_job_first() as the
 * comparison function to determine if this job should start before another
 * job in the queue. */
void
queue_job(struct ptr_vec *jobs, struct sched_job *job, int max_jobs)
{
	add_vec_elem(jobs, job, sizeof(*job), max_jobs, sched_job_first);
}

static void
update_paused_state(char *current_dir)
{
	char summary_path[MAXPATHLEN];
	struct siq_gc_job_summary *js = NULL;
	struct isi_error *error = NULL;


	snprintf(summary_path, sizeof(summary_path), "%s/%s", current_dir,
	    REPORT_FILENAME);
	js = siq_gc_job_summary_load(summary_path, &error);
	if (error) {
		log(ERROR, "Failed to open report: %s", summary_path);
		goto out;
	}

	if (js->root->summary->total->state != SIQ_JS_PAUSED) {
		js->root->summary->total->state = SIQ_JS_PAUSED;
		siq_gc_job_summary_save(js, &error);
		if (error) {
			log(ERROR, "Failed to save report: %s", summary_path);
			goto out;
		}
	}

out:
	siq_gc_job_summary_free(js);
	isi_error_free(error);
}

int
collect_jobs(struct sched_ctx *sctx, struct ptr_vec *jobs, int max_jobs,
    struct int_set *up_nodes)
{
	DIR		*dirp;
	struct dirent	*dp;
	struct sched_job job = {};
	enum sched_action action;
	enum grab_result  gret;
	int sret = SUCCESS;
	int ret = SUCCESS;
	struct siq_policies *policies = sctx->policy_list;
	bool locked = false;
	char cur_policy_id[MAXPATHLEN + 1] = "";
	struct isi_error *error = NULL;

	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "sched_local_node_work: opendir(%s) error %s",
			sched_get_run_dir(), strerror(errno));
		ret = FAIL;
		goto done;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp))) {
		/* unlock job that was locked in previous loop */
		if (locked) {
			siq_job_unlock();
			locked = false;
		}

		if (dp->d_type != DT_DIR)
			continue;		

		sched_decomp_name(dp->d_name, cur_policy_id, NULL, NULL);
		locked = siq_job_lock(cur_policy_id, &error);
		if (error) {
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		ret = init_job(sctx, dirp, dp, policies, &job);
		if (ret == SCH_NO) {
			log(DEBUG, "Unknown run directory entry %s/%s",
			    sched_get_run_dir(), dp->d_name);
			continue;
		} else if (ret == SCH_ERR) {
			log(ERROR, "Error scanning run directories");
			ret = FAIL;
			goto done;
		}

		log(TRACE, "%s: policy=%s dir='%s'", __func__, job.policy_name,
		    dp->d_name);

		action = ACTION_GRAB;

		if (job.progress != PROGRESS_AVAILABLE) {
			if (job.local)
				action = local_action(sctx, &job);
			else
				action = remote_action(&job, up_nodes);
			
			if (action == ACTION_ERR) {
				log(ERROR, "Problem while processing job "
				    "for policy '%s'", job.policy_name);
				job.policy = NULL;
				continue;
			}

			if (action == ACTION_SKIP) {
				log(TRACE, "%s: Skipping %s / %s", __func__,
				    job.policy_name, job.policy_id);
				job.policy = NULL;
				continue;
			}
		}

		log(TRACE, "%s: grabbing", __func__); ////TRACE -> INFO

		/* action must be ACTION_GRAB */

		/* Either no node has grabbed the job yet, or the job got
		 * timed out in the GRABBED state for another node. This node
		 * will try to grab it. */
		
		if (check_policy_can_run_on_local_node(&job) == FAIL) {
			job.policy = NULL;
			continue;
		}

		/* Since this policy can be started, clear any can't start
		 * alert for it */
		siq_cancel_alert(SIQ_ALERT_POLICY_START, job.policy_name,
		    job.policy_id);

		log(TRACE, "%s: Starting policy %s", __func__,       
		    job.policy_name);     //////TRACE  -> INFO

		if (sched_check_status(dp->d_name, SCHED_JOB_CANCEL)) {
			/*
			 * A canceled job still needs to have a
			 * coordinator properly clean up.
			 */
			gret = job_grab(sctx, &job, true);
			if (gret == GRAB_RESULT_START) {
				sret = coord_start(sctx, &job);
			} else if (gret != GRAB_RESULT_ALREADY_STARTED) {
				log(INFO, "Failed to cancel job %s: "
				    "failed job_grab_for_cancel (%d)",
				    dp->d_name, gret);
			}
			
			if (sret == FAIL) {
				log(INFO, "Failed to cancel job %s: "
				    "failed coord_start",
				    dp->d_name);
			}
			job.policy = NULL;
			continue;
		}

		/* Do not try to start paused jobs */
		if (sched_check_status(dp->d_name, SCHED_JOB_PAUSE)) {
			update_paused_state(job.current_dir);
			job.policy = NULL;
			continue;
		}

		/* Insert this potential job into the queue. If the queue is
		 * full, the potential job is just discarded, to be tried
		 * again by another node or this node the next time this
		 * function is called. However, if the queue is full and this
		 * job has priority, a non-priority job may be dequeued to
		 * make room for this job instead.
		 */
		queue_job(jobs, &job, max_jobs);

		job.policy = NULL;
	}

done:
	isi_error_free(error);
	if (locked)
		siq_job_unlock();
	if (dirp)
		closedir(dirp);

	return ret;
}

/* Remove flags from a run dir that may have caused the job to be "pending"
 * during a previous run. The flags include:
 * 1. preempted
 */
static void
remove_pending_flags(const char *job_dir)
{
	char flag_file[MAXPATHLEN + 1];

	/* We don't care about success of unlink - the file may not exist */
	snprintf(flag_file, MAXPATHLEN + 1, "%s/%s", job_dir,
	    SCHED_JOB_PREEMPT);
	unlink(flag_file);
}

static int
run_job(struct sched_ctx *sctx, struct sched_job *job, enum grab_result *grab_result)
{
	int ret = SUCCESS;
	enum grab_result sret;

	sret = job_grab(sctx, job, false);
	switch (sret) {
	case GRAB_RESULT_START:
		/* The job was successfully grabbed and needs to be
		 * started */
		ret = coord_start(sctx, job);
		if (ret == SUCCESS) {
			log(INFO, "Policy %s starting",
			    job->policy_name);
			break;
		}
		/* Fall through if coord_start() returned FAIL */

	case GRAB_RESULT_ERROR:
		log(ERROR, "Policy %s failed to start",
		    job->policy_name);
		ret = FAIL;
		break;

	case GRAB_RESULT_ALREADY_STARTED:
		log(INFO, "Policy %s already running",
		    job->policy_name);
		break;

	case GRAB_RESULT_KILLED:
		log(INFO, "Policy %s killed", job->policy_name);
		break;

	case GRAB_RESULT_NO_SPACE:
		log(DEBUG, "Policy %s held back due to "
		    "concurrent job limit", job->policy_name);
		break;

	case GRAB_RESULT_NO_POOL_SPACE:
		log(DEBUG, "Policy %s held back due to lack of space within"
		    "its assigned subnet:pool", job->policy_name);
		break;

	default:
		ASSERT(0, "Should never get here");
	}

	if (grab_result)
		*grab_result = sret;

	return ret;
}

static int
process_jobs(struct sched_ctx *sctx, struct ptr_vec *jobs, int max_jobs)
{
	int ret = SUCCESS;
	bool locked = false, restores_only = false;
	void **vjob = NULL;
	enum grab_result grab_result;
	struct sched_job *job = NULL;
	struct isi_error *error = NULL;

	PVEC_FOREACH(vjob, jobs) {
		job = (struct sched_job *)*vjob;
		if (locked) {
			siq_job_unlock();
			locked = false;
		}

		if (restores_only && !job->restoring)
			continue;

		locked = siq_job_lock(job->policy_id, &error);
		if (error) {
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		ret = run_job(sctx, job, &grab_result);
		if (ret == FAIL)
			goto out;

		/* If any job fails to start due to the concurrent job limit,
		 * don't try to start any other job in the queue (except
		 * restore jobs). Jobs are queued in the order they should
		 * start; we don't want a lower priority job taking a spot
		 * if one opens up later in this loop. We will re-loop when
		 * a spot opens up. */
		if (grab_result == GRAB_RESULT_NO_SPACE)
			restores_only = true;
	}

out:
	isi_error_free(error);
	if (locked)
		siq_job_unlock();
	return ret;
}

/* @return: SCH_YES, SCH_NO, SCH_ERR - i/o error */
enum sched_ret
is_job_grabbed_or_running(const char *policy_id_str)
{
	DIR		*dirp;
	struct dirent	*dp;
	enum sched_ret	pre_ret = SCH_NO;

	log(TRACE, "%s: enter", __func__);
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
			sched_get_run_dir(), strerror(errno));
		return SCH_ERR;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		if (is_job_running(policy_id_str, dp->d_name)) {
			pre_ret = SCH_YES;
			break;
		}
	}
	closedir(dirp);
	log(TRACE, "%s: exit %d", __func__, pre_ret);
	return pre_ret;
}

/* Includes both grabbed and actually started jobs */
int get_running_jobs_count(struct sched_ctx *sctx) {
	return get_job_count(sctx, PROGRESS_GRABBED);
}

/* Get count of jobs that have made at least min_progress. */
int
get_job_count(struct sched_ctx *sctx, int min_progress)
{
	DIR *dirp;
	struct dirent *dp;
	char policy_id_str[MAXPATHLEN + 1] = "";
	int job_count = 0;
	enum job_progress progress;

	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
		    sched_get_run_dir(), strerror(errno));
		return -1;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;
		
		progress = sched_decomp_name(dp->d_name, policy_id_str, 0, 0);
		if (progress >= min_progress) {
			job_count++;
		}
	}

	closedir(dirp);

	if (job_count < sched_get_max_concurrent_jobs() &&
	    min_progress >= PROGRESS_GRABBED)
		sctx->max_jobs_running = false;
	return job_count;
}

/* Returns true if job1 should be preempted before job2. This is used as the
 * comparison function when add_vec_elem is called for queue_running_job. */
static bool
preempt_job_first(void *job1, void *job2)
{
	struct sched_running_job *s_job1 = (struct sched_running_job *)job1;
	struct sched_running_job *s_job2 = (struct sched_running_job *)job2;
	/*
	 * Return true if job1 should be preempted before job2
	 *
	 * if Both have the same priority:
	 *     Preempt most recent start time
	 * else:
	 *     Preempt lowest priority
	 */

	if (s_job1->priority == s_job2->priority)
		return (s_job1->start_time > s_job2->start_time);
	return (s_job1->priority < s_job2->priority);
}

/* Add a job to our queue of preemption candidates. Use preempt_job_first()
 * as the comparison function to determine if this job should be preempted
 * before another job in the queue. */
void queue_running_job(struct ptr_vec *running_jobs,
    struct sched_running_job *job, int max_jobs)
{
	add_vec_elem(running_jobs, job, sizeof(*job), max_jobs,
	    preempt_job_first);
}

/* Try to preempt running jobs that have a lower priority level than the given
 * priority level. Up to num_to_preempt jobs with a priority level < priority
 * will be preempted. if restrict_by_pool is not NULL or "", only jobs running
 * in that subnet:pool will be considered for preemption.
 * The number of preempted jobs is returned.
 *
 * Note: jobs are preempted in order of lowest priority and shortest run time.
 * This order can be changed by modifying the comparison function
 * preempt_job_first().
 */
int
preempt_lower_priority_job(struct sched_ctx *sctx, const char *run_dir,
    int priority, int num_to_preempt, char *restrict_by_pool)
{
	DIR *dirp;
	int num_preempted = 0, i;
	char policy_id[MAXPATHLEN + 1];
	bool ret = false;
	enum job_progress progress;
	enum siq_action_type running_act;
	void **job_pp = NULL;
	struct siq_policies *policies = sctx->policy_list;
	struct siq_policy *policy;
	struct dirent *dp;
	struct sched_running_job running_job = {};
	struct stat st;
	struct ptr_vec running_jobs = PVEC_INITIALIZER;
	struct sched_running_job *job_p = NULL;
	struct isi_error *error = NULL;

	dirp = opendir(run_dir);
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
		    run_dir, strerror(errno));
		goto out;
	}

	if (priority == 0 || num_to_preempt == 0)
		goto out;


	/* Find the policy with the lowest priority and the shortest run-time,
	 * then try to preempt it. */
	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		/* Skip if job isn't running */
		progress = sched_decomp_name(dp->d_name, policy_id, 0, 0);
		if (progress != PROGRESS_STARTED)
			continue;

		/* Skip if job is already preempted */
		if (job_is_preempted(run_dir, dp->d_name, policy_id))
			continue;

		/* Skip if policy doesn't exist */
		policy = siq_policy_get_by_pid(policies, policy_id);
		if (policy == NULL)
			continue;

		/* Only preempt jobs with a lower priority level than the one
		 * given */
		if (policy->scheduler->priority >= priority)
			continue;

		/* If a restrict by pool was given, only preempt jobs that are
		 * in that pool */
		if (restrict_by_pool && restrict_by_pool[0] &&
		    (policy->target->restrict_by == NULL ||
		    strcmp(restrict_by_pool, policy->target->restrict_by)))
				continue;

		/* Don't preempt failover/failback jobs */
		running_act = get_spec_action(run_dir, dp->d_name);
		if (running_act != SIQ_ACT_REPLICATE &&
		    running_act != SIQ_ACT_SYNC)
			continue;

		/* Stat the run dir to get its start time. If this fails, the
		 * job has finished already so just move on. */
		if (enc_fstatat(dirfd(dirp), dp->d_name, dp->d_encoding, &st,
		    AT_SYMLINK_NOFOLLOW) == -1)
			continue;

		/* This is a valid lower priority job. Add it to our list of
		 * preemption candidates, which is sorted by:
		 * 1. the lowest priority of all running jobs.
		 * 2. the shortest run time of all running jobs. */
		SZSPRINTF(running_job.job_dir, "%s", dp->d_name);
		SZSPRINTF(running_job.policy_name, "%s", policy->common->name);
		running_job.priority = policy->scheduler->priority;
		running_job.start_time = st.st_birthtime;

		/* Queue a maximum of num_to_preempt for preemption */
		queue_running_job(&running_jobs, &running_job, num_to_preempt);
	}

	for (i = 0; i < pvec_size(&running_jobs); i++) {
		job_pp = pvec_element(&running_jobs, i);
		job_p = (struct sched_running_job *)*job_pp;
		ret = sched_job_preempt(sctx, run_dir, job_p->job_dir, &error);
		if (error) {
			/* Errors are not a big deal, the job probably
			 * finished and the directory has been renamed. */
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		/* Already preempted by another scheduler, don't count it as
		 * a successful preemption for this scheduler */
		if (!ret)
			continue;

		/* Success */
		num_preempted++;

		log(NOTICE, "Preempted policy: %s", job_p->policy_name);
	}

out:
	if (dirp != NULL)
		closedir(dirp);

	/* pops off a pointer to the pointer we need to free */
	free_and_clean_pvec(&running_jobs);
	
	return num_preempted;
}

/* Given a subnet:pool (restrict_by), the number of workers available on nodes
 * in that pool is returned. */
static int
get_num_workers_in_pool(struct sched_ctx *sctx, char *restrict_by,
    struct isi_error **error_out)
{
	int total_workers = 0;
	int const *node_id;
	struct int_set INT_SET_INIT_CLEAN(nodes_in_pool);
	struct sched_node *node = NULL;
	struct isi_error *error = NULL;

	get_restricted_nodes(restrict_by, &nodes_in_pool, &error);
	if (error) {
		log(ERROR, "Failed to get nodes in pool: %s",
		    isi_error_get_message(error));
		goto out;
	}

	/* For each node in the pool, add the max workers that node can
	 * support to the total amount of workers available in the pool. */
	INT_SET_FOREACH(node_id, &nodes_in_pool) {
		node = sched_get_node_info(sctx, *node_id);

		/* A node may have joined the group since we last loaded node
		 * limits. Assume the node can't contribute any workers to our
		 * pool; its workers will be factored in on the next scheduler
		 * loop */
		if (node == NULL)
			continue;

		total_workers += node->max_workers;
	}

out:
	isi_error_handle(error, error_out);
	return total_workers;
}

/* Given a subnet:pool (restrict_by), the number of jobs the nodes in that
 * pool can support is returned. */
static int
get_num_jobs_pool_supports(struct sched_ctx *sctx, char *restrict_by,
    struct isi_error **error_out)
{
	int total_workers = 0, supported_jobs = 0;
	struct isi_error *error = NULL;

	total_workers = get_num_workers_in_pool(sctx, restrict_by, &error);
	if (error) {
		log(ERROR, "Failed to get num workers in pool: %s",
		    isi_error_get_message(error));
		goto out;
	}

	supported_jobs = total_workers / sched_get_min_workers_per_policy();

out:
	isi_error_handle(error, error_out);
	return supported_jobs;
}

/* Returns true if there is space in the given job's pool to run another
 * job. A pool has space if there is at least min_workers_per_job workers
 * available in the pool. If the job does not have a pool, the whole cluster
 * is the pool. It is assumed that the job being passed has already been
 * grabbed. Thus, there is space for the job in the pool if grabbing it did
 * not overflow the amount of jobs supported by the pool. */
static bool
space_in_pool(struct sched_ctx *sctx, struct sched_job *job)
{
	int total_jobs = 0, num_supported_jobs = 0;
	DIR *dirp = NULL;
	bool res = false;
	char policy_id[MAXPATHLEN + 1] = "";
	char node_num[4] = {};
	struct dirent *dp;
	struct siq_policy *pol = NULL;
	struct siq_policies *policies = sctx->policy_list;
	struct isi_error *error = NULL;


	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
		    sched_get_run_dir(), strerror(errno));
		goto out;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		node_num[0] = 0;
		sched_decomp_name(dp->d_name, policy_id, node_num, 0);

		/* Skip if job not grabbed */
		if (node_num[0] == 0)
			continue;

		/* Skip if policy doesn't exist */
		pol = siq_policy_get_by_pid(policies, policy_id);
		if (pol == NULL)
			continue;

		/* Skip if job is not assigned to the given job's pool */
		if (pol->target->restrict_by == NULL ||
		    strcmp(job->restrict_by, pol->target->restrict_by))
				continue;
		total_jobs++;
	}

	num_supported_jobs = get_num_jobs_pool_supports(sctx,
	    job->restrict_by, &error);
	if (error) {
		log(ERROR, "Failed to determine space in pool: %s",
		    isi_error_get_message(error));
		goto out;
	}

	if (total_jobs <= num_supported_jobs)
		res = true;

out:
	if (dirp)
		closedir(dirp);
	isi_error_free(error);
	return res;
}

/*
 * This function adds the node number to the directory name of the scheduled
 * job to indicate that this node has grabbed the job. That is, directory XXXX
 * is renamed to XXXX_n (where XXXX is the policy id and n is the node
 * number).
 *
 * @return: GRAB_RESULT_ALREADY_STARTED: The job is already started.
 *          GRAB_RESULT_START: The job is ready to and needs to be started.
 *          GRAB_RESULT_NO_SPACE: job can't start due to too many other jobs
 *              running.
 *          GRAB_RESULT_ERROR: Error while grabbing job.
 */
enum grab_result
job_grab(struct sched_ctx *sctx, struct sched_job *job, bool cancel)
{
	int		ret;
	enum sched_ret	pre_ret;
	enum sched_ret	post_ret;
	bool		retry = false, must_preempt = false, max_jobs = false;
	int		count, preempted = 0;

	log(TRACE, "%s: enter for %s to target %s", __func__,
	    job->policy_name, job->target);

	UFAIL_POINT_CODE(no_grab,
	    log(ERROR,
		"UFP no_grab is set. This node will never grab a policy");
	    return GRAB_RESULT_ALREADY_STARTED;
	);

	/* If a job is canceled or doesn't need restoring, it's supposed to be
	 * new; check to be sure it isn't already running */
	if (!cancel && !job->restoring) {
		pre_ret = is_job_grabbed_or_running(job->policy_id);
		if (pre_ret != SCH_NO) {
			/* SCH_YES: The job is running so it can't be
			 * grabbed */
			if (pre_ret == SCH_YES)
				return GRAB_RESULT_ALREADY_STARTED;
			/* Otherwise, SCH_ERR: Error checking job progress
			 * state */
			return GRAB_RESULT_ERROR;
		}
	}

	ret = job_make_grabbed(sctx, job);

	/* No, another scheduler beat us to it */
	if (ret == SCH_NO)
		return GRAB_RESULT_ALREADY_STARTED;

	if (ret == SCH_ERR) {
		log(ERROR, "%s: Failed to grab job %s", __func__,
		    job->policy_name);
		return GRAB_RESULT_ERROR;
	}

	/* Cancel jobs are always started regardless of current job count.
 	 * Jobs that are being restored (former holding node dead, or stuck in
 	 * grabbed) will always start because they they are included in job
 	 * counts already. */
	if (!cancel && !job->restoring) {
		count = get_running_jobs_count(sctx);

		/* If there was an error checking the job count, make
		 * a last-ditch effort to put the job back the
		 * PROGRESS_AVAILABLE state for later evaluation.
		 */
		if (count == -1) {
			job_make_available(job);
			return GRAB_RESULT_ERROR;
		}

		max_jobs = (count > sched_get_max_concurrent_jobs());

		/* Determine if we must preempt another job in order for this
		 * job to start. We must preempt another job in two cases:
		 * 1. Our pool doesn't have enough workers to support another
		 *    job.
		 * 2. There are too many concurrent jobs running.
		 *
		 * For case 1, we must preempt a running job that shares our
		 * same subnet:pool settings - this is the only way to
		 * that this job will have enough workers in its pool to use.
		 *
		 * For case 2, we can preempt any running job - we have
		 * already determined that our pool has enough workers.
		 *
		 * Note: preemption is atomic - a successful preemption of the
		 * same running policy is not possible between two schedulers.
		 * Therefore, we can assume one successfully preempted job ==
		 * one new priority job running, no more or less. */
		if (job->restrict_by[0] && !space_in_pool(sctx, job)) {
			must_preempt = true;
			/* Preempt from pool */
			preempted = preempt_lower_priority_job(sctx,
			    sched_get_run_dir(), job->priority, 1,
			    job->restrict_by);
		} else if (max_jobs) {
			must_preempt = true;
			/* Preempt anything */
			preempted = preempt_lower_priority_job(sctx,
			    sched_get_run_dir(), job->priority, 1, NULL);
		}

		if (must_preempt && !preempted) {
			job_make_available(job);

			if (max_jobs) {
				return GRAB_RESULT_NO_SPACE;
			} else {
				return GRAB_RESULT_NO_POOL_SPACE;
			}
		}
	}

	do {
		post_ret = check_race_with_other_node(sctx, job);
		switch (post_ret) {
		case SCH_YES:
			/* Either no race condition, or there was one and this
			 * node won out. Start the job */
			ret = GRAB_RESULT_START;
			break;

		case SCH_NO:
			/* There was a race condition with another node
			 * starting the policy, and the local node lost out
			 * and stopped its job, but the remote continues. */
			ret = GRAB_RESULT_ALREADY_STARTED;
			break;

		case SCH_RETRY:
			/* SCH_RETRY only happens if there was another node
			 * running the same policy, and this node won the
			 * decision to keep running the job, but there was
			 * failure in canceling the other job */

			/* Going to check the race condition one more time */
			if (retry == false) {
				retry = true;
			} else {
			/* If the second attempt to cancel the other's node
			 * job for this policy failed, give up and return
			 * SCH_YES as there is a job for the policy running */
				ret = GRAB_RESULT_ALREADY_STARTED;
				retry = false;
			}
			break;

		case SCH_ERR:
			ret = GRAB_RESULT_ERROR;
			break;

		default:
			ASSERT(0, "Invalid return of %d",  post_ret);

		}

	} while (retry);

	return ret;
}

bool
is_job_running(const char *pattern_policy_id_str, const char *test_job_str)
{
	char	policy_id[MAXPATHLEN + 1];
	char	node_num_str[MAXPATHLEN + 1];
	char	coord_id_str[MAXPATHLEN + 1];
	enum job_progress progress;
	bool res = false;

	progress = sched_decomp_name(test_job_str, policy_id,
	    node_num_str, coord_id_str);
	/* Consider a job running if the policy_id matches and a node has
	 * grabbed or started it */
	if (progress == PROGRESS_AVAILABLE ||
	    strcmp(pattern_policy_id_str, policy_id) != 0) {
		log(TRACE, "%s: %s is not running", __func__, policy_id);
		goto out;
	}
	log(TRACE, "%s: %s _is_ running", __func__, policy_id);
	res = true;

out:
	return res;
}

/* @return: SCH_YES, SCH_NO, SCH_ERR - i/o error */
enum sched_ret
check_race_with_other_node(struct sched_ctx *sctx, struct sched_job *job)
{
	DIR		*dirp;
	struct dirent	*dp;
	enum sched_ret	rm_ret;
	enum sched_ret	func_ret = SCH_YES;

	log(TRACE, "%s: enter", __func__);
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "%s: opendir(%s) error %s", __func__,
			sched_get_run_dir(), strerror(errno));
		return SCH_ERR;
	}

	opendir_skip_dots(dirp);
	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR)
			continue;

		rm_ret = resolve_race_with_other_node(sctx, job, dp->d_name);
		switch (rm_ret) {
		case SCH_YES:
			break;
		case SCH_NO:
			if (func_ret != SCH_RETRY) {
				func_ret = SCH_NO;
			}
			break;
		case SCH_RETRY:
		case SCH_ERR:
			func_ret = rm_ret;
			break;
		default:
			log(ERROR, "%s: switch(%d) error",
			    __func__, rm_ret);
			func_ret = SCH_ERR;
			break;
		}
		if (func_ret == SCH_ERR)
			break;
	}
	closedir(dirp);
	return func_ret;
}

static int
coord_start(struct sched_ctx *sctx, struct sched_job *job)
{
	pid_t coord_pid;
	int i;
	struct isi_error *error = NULL;
	bool exists;
	char coord_rename_dir[MAXPATHLEN + 1];

	ilog(IL_INFO, "%s called", __func__);

	UFAIL_POINT_CODE(grab_but_no_coord,
	    log(ERROR, "UFP grab_but_no_coord is set. "
		"This node will grab a policy, but never start it");
	    return SUCCESS;
	);

	remove_pending_flags(job->local_grabbed_dir);

	coord_pid = fork();
	if (coord_pid == (pid_t)-1) {
		log(ERROR, "coord_start: fork error %s",
			strerror(errno));
		return FAIL;
	}

	if (coord_pid != 0) {
		/* parent process */
		exists = set_proc_watch(sctx, coord_pid, &error);
		if (error || exists == FALSE) {
			isi_error_free(error);
			log(ERROR, "Failed to set watch on forked coordinator "
			    "process %d", coord_pid);
		}

		/* the dir name the coord will rename the job dir to */
		snprintf(coord_rename_dir, MAXPATHLEN, "%s_%d",
		    job->local_grabbed_dir, coord_pid);

		/* give coord a chance to rename job dir before returning
		 * (and releasing job lock) */
		log(INFO, "waiting for coord to rename job dir from %s to %s",
		    job->local_grabbed_dir, coord_rename_dir);
		for (i = 0; i < 100; i++) {
			siq_nanosleep(0, 1000000);
			if (access(coord_rename_dir, F_OK) == 0) {
				log(DEBUG, "coord renamed job dir");
				break;
			}
		}

		return SUCCESS;
	}

	/* child process */
	execl(COORDNAME, COORDNAME, "-D", job->local_grabbed_dir, NULL);
	log(ERROR, "coord_start: execl error %s", strerror(errno));
	exit(EXIT_FAILURE);
}

/* @job,
 * @dir_name - test job dir
 * @return: SCH_YES - run need,
 *          SCH_NO - job canceled,
 *          SCH_RETRY - retry opendir,
 *          SCH_ERR - i/o error
 */
static enum sched_ret
resolve_race_with_other_node(struct sched_ctx *sctx, struct sched_job *job,
    const char *dir_name)
{
	char		job_name[MAXPATHLEN + 1];
	char		other_node_num_str[MAXPATHLEN + 1];
	char		edit_dir[MAXPATHLEN + 1];
	enum job_progress progress;
	int		test_node_num;
	enum sched_ret	cancel_ret;
	enum sched_ret	func_ret = SCH_YES;
	int		ret;

	log(TRACE, "%s: enter", __func__);
	progress = sched_decomp_name(dir_name, job_name,
	    other_node_num_str, NULL);

	/* If other run directory is for another ID, then all is OK */
	if (strcmp(job->policy_id, job_name) != 0) {
		return SCH_YES;
	}

	if (sched_check_status(dir_name, SCHED_JOB_CANCEL)) {
		return SCH_YES;
	}

	// XXX Shouldn't we clean up a duplicate run directory?
	if (progress == PROGRESS_AVAILABLE) {
		return SCH_YES;
	}

	test_node_num = atoi(other_node_num_str);

	/* Hey, this is our job!!!. XXX We really could avoid this by
	 * checking the inode of the directory */
	if (sctx->node_num == test_node_num)
		func_ret = SCH_YES;

	/* If we've gone this far, there is another node working on the
	 * identical job we are working on. This is not good. Either, the job
	 * we're trying to start needs to be canceled, or the other job needs
	 * to be canceled. The choice is made by node id order. The lowest
	 * node wins. */

	/* This node wins: Cancel the other job */
	if (sctx->node_num < test_node_num) {
		log(INFO, "%s: Other node %d is also running policy %s. "
		    "Will cancel other job", __func__,
		    test_node_num, job->policy_name);

		cancel_ret = sched_job_cancel(sctx, dir_name);
		switch (cancel_ret) {
			case SCH_YES:
			case SCH_ERR:
				func_ret = cancel_ret;
				break;
			case SCH_NO:
				func_ret = SCH_RETRY;
				break;
			default:
				log(ERROR, "%s: cancel_ret error (%d)",
				    __func__, cancel_ret);
				func_ret = SCH_ERR;
				break;
		}
	}

	/* Other node wins. This node loses: Wipeout any of our job's working
	 * directory */
	if (sctx->node_num > test_node_num) {
		log(INFO, "%s: Other node %d is also running policy %s. "
		    "Removing local job", __func__, test_node_num,
		    job->policy_name);

		SZSPRINTF(edit_dir, "%s/%s_%d",
		    sctx->edit_run_dir, job->policy_id, sctx->node_num);

		/* Get rid of any old edit directory with the same name */
		sched_unlink(sctx, edit_dir);

		/* Remove our directory (by first moving it to the
		 * edit_dir) */
		ret = sched_remove_dir(sctx, job->current_dir, edit_dir);
		if (ret == FAIL) {
			func_ret = SCH_ERR;
		}
		else
			func_ret = SCH_NO;
	}

	return func_ret;
}

/*
 * @return: SUCCESS, FAIL
 */
int
coord_test(struct sched_ctx *sctx, struct sched_job *job)
{
	char		summary_path[MAXPATHLEN + 1];
	int		ret;
	struct siq_job_summary *js = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "coord_test: enter");

	ASSERT(job->progress == PROGRESS_STARTED);
	if (job->node_num != sctx->node_num) {
		return SUCCESS;
	}
	ret = kill(job->coord_pid, 0);
	if (ret == 0) {
		return SUCCESS;
	}

	// XXX What is the purpose of this? The summary is loaded, then freed
	// with nothing done with it here. Is there some side-effect of
	// loading the summary?
	sprintf(summary_path, "%s/%s", job->current_dir, REPORT_FILENAME);
	js = siq_job_summary_load_readonly(summary_path, &error);
	if (error) {
		if (errno != ENOENT) {
			log(FATAL, "Couldn't load summary from %s in %s: %s",
			    summary_path, __func__, strerror(errno));
		} else {
			//File no longer exists, could be a race condition
			log(INFO, "Couldn't load summary from %s in %s: %s "
			    "(possible race)", summary_path, __func__, 
			    strerror(errno));
			ret = SUCCESS;
		}
	} else {
		log(NOTICE, "Restarting coordinator for %s", job->policy_name);
		ret = coord_restart(sctx, job);
		siq_job_summary_free(js);
	}
	
	return ret;
}

/*
 * Trigger restart of policy by renaming directory from XXXX_n to XXXX (where
 * XXXX is the policy id and n is the node number). Restart will occur when
 * any node's scheduler detects the directory without the trailing _n.
 */
static int
coord_restart(struct sched_ctx *sctx, struct sched_job *job)
{
	int		ret = SUCCESS;

	log(TRACE, "coord_restart: enter");
	ASSERT(job->progress == PROGRESS_STARTED);
	ret = job_make_grabbed(sctx, job);

	/* Another node restarted it */
	if (ret == SCH_NO) {
		log(TRACE, "%s: Restarted by another node", __func__);
		return SUCCESS;
	}

	if (ret == SCH_ERR) {
		log(ERROR, "%s: failure", __func__);
		return FAIL;
	}
	ret = coord_start(sctx, job);
	return ret;
}

/**
 * Handle the failure of a rename of run directory
 */
static enum sched_ret
rename_failure(struct sched_job *job, const char *from, const char *to,
    int _errno)
{
	if (_errno == ENOENT) {
		if (is_job_grabbed_or_running(job->policy_id) == SCH_YES) {
			log(DEBUG,"Another node grabbed the job "
			    "before local node");
			return SCH_NO;
		}
		log(ERROR, "Rename of %s to %s failed due to bad path",
		    from, to);
		return SCH_ERR;
	}
	log(ERROR, "%s: Rename of %s to %s failed: %s", __func__,
	    from, to, strerror(_errno));
	return SCH_ERR;
}

/**
 * Put a job in the AVAILABLE state by renaming its run directory to its
 * available name.
 * Returns:
 *   SCH_YES: Job successfully made available.
 *   SCH_NO: Could not rename the directory, but it appears another scheduler
 *           has. 
 *   SCH_ERR: Error renaming file
 */
enum sched_ret
job_make_available(struct sched_job *job)
{
	bool locked = false;
	int ret = SCH_YES;
	struct isi_error *error = NULL;

	locked = siq_job_lock(job->policy_id, &error);
	if (error)
		goto out;
	if (rename(job->current_dir, job->available_dir) == -1) {
		ret = rename_failure(job, job->current_dir,
		    job->available_dir, errno);
		goto out;
	}

	job->progress = PROGRESS_AVAILABLE;
	SZSPRINTF(job->current_dir, job->available_dir);
	job->node_num = 0;
	job->coord_pid = 0;

out:
	if (locked)
		siq_job_unlock();
	isi_error_free(error);
	return ret;
}

/**
 * Put a job in the GRABBED state by renaming its run directory to its
 * grabbed name with the local node ID.
 * Returns:
 *   SCH_YES: Job successfully made GRABBED.
 *   SCH_NO: Could not rename the directory, but it appears another scheduler
 *           has. 
 *   SCH_ERR: Error renaming file.
 */
enum sched_ret
job_make_grabbed(struct sched_ctx *sctx, struct sched_job *job)
{
	bool locked = false;
	int ret = SCH_YES;
	struct isi_error *error = NULL;

	locked = siq_job_lock(job->policy_id, &error);
	if (error)
		goto out;
	if (rename(job->current_dir, job->local_grabbed_dir) == -1) {
		ret = rename_failure(job, job->current_dir,
		    job->local_grabbed_dir, errno);
		goto out;
	}

	job->progress = PROGRESS_GRABBED;
	job->node_num = sctx->node_num;
	SZSPRINTF(job->current_dir, "%s", job->local_grabbed_dir);
	job->coord_pid = 0;

out:
	if (locked)
		siq_job_unlock();
	return ret;
}
