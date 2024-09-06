#include <ifs/ifs_lin_open.h>

#include "sched_includes.h"
#include "sched_main_work.h"
#include "sched_change_based.h"
#include "isi_migrate/config/siq_source.h"

enum stf_category {
	STF_CAT_ERROR,
	STF_CAT_NOT_STF,
	STF_CAT_UNRELATED,
	STF_CAT_MISSING,
	STF_CAT_PATHLESS,
	STF_CAT_OVERLAPPING,
	STF_CAT_CHANGE_DETECTED,
};

/**
 *  Get the status for the named STF. 
 *
 *  N.B. It is assumed that ifs_snap_stat() will never return an stf_stat
 *      structure with a paths_num value greater than one (1).
 *
 *  @param dfd          (IN) Snapshot directory file descriptor.
 *  @param stf_name     (IN) Name (a sid string) of an STF.
 *  @param sst          (OUT) STF status structure.
 *  @param path         (OUT) Buffer for root path of STF status.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return True if the STF status was retrieved, false otherwise.
 */
static bool
get_stf_stat(int dfd, char *stf_name, struct stf_stat *sst, char *path,
    struct isi_error **error_out)
{
	int	sfd, path_enc, ret;
	bool	result = false;
	char	*paths[1] = { path };

	struct isi_error	*error = NULL;

	memset(sst, 0, sizeof(*sst));
	path[0] = '\0';

	sfd = enc_openat(dfd, stf_name, ENC_DEFAULT, O_RDONLY);
	if (sfd < 0) {
		//  Non-existence is acceptable, other errors aren't.
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Error opening %s", stf_name);
		}
		goto out;
	}

	sst->paths = paths;
	sst->paths_num = 1;
	sst->paths_enc = &path_enc;
	ret = ifs_snap_stat(sfd, sst);

	if (ret == 0) {
		ASSERT(sst->paths_num < 2);
		result = true;
	} else {
		error = isi_system_error_new(errno,
		    "Error retrieving snap stat for %s", stf_name);
	}

	close(sfd);

out:
	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Categorize a Snapshot Tracking File based on its relation to the current
 *  change-based policy. STFs of interest include those whose id is >= that
 *  of the STF associated with the policy, and which have either no root path
 *  (potentially interesting) or a root path which overlaps that of the
 *  policy (definitely interesting).
 *
 *  Change detection is predicated upon LIN counts in overlapping STFs;
 *  specifically, when the number of LINs in a given STF equals or exceeds
 *  the number of parts in the root path, then change is understood to have
 *  occured. For example, if the current policy has a root path of
 *  /ifs/data/test (3 parts), then its base STF will contain that same path
 *  as well as an initial LIN count of two (2), where the two LINs represent
 *  the /ifs and /ifs/data directories. When a change occurs at or below
 *  /ifs/data/test, resulting in additional LINs being added to the STF, the
 *  path-parts/LINs threshold is crossed and sync-worthy activity is
 *  indicated.
 *
 *  N.B. LIN count changes in overlapping STFs whose root paths are supersets
 *      of that of the policy can result in false-positive evaluations,
 *      i.e. when changes occur that fall outside the scope of the root path
 *      of the policy. This is acceptable given usage expectations.
 *
 *  @param dfd          (IN) Snapshot directory file descriptor.
 *  @param stf_name     (IN) Name (a sid string) of target STF.
 *  @param pol_root     (IN) Root path of the policy (no trailing slash).
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return STF category.
 */
static enum stf_category
categorize_stf(int dfd, char *stf_name, char *pol_root,
    struct isi_error **error_out)
{
	char			stf_root[MAXPATHLEN];
	size_t			stf_root_len, i;
	uint64_t		path_parts;
	struct stf_stat		sst;
	struct isi_error	*error = NULL;
	enum stf_category	result;

	if (!get_stf_stat(dfd, stf_name, &sst, stf_root, &error)) {
		result = (error ? STF_CAT_ERROR : STF_CAT_MISSING);
		goto out;
	}

	if (sst.sf_type != SF_STF || sst.system != 0) {
		result = STF_CAT_NOT_STF;
		goto out;
	}

	if (sst.paths_num == 0) {
		result = STF_CAT_PATHLESS;
		goto out;
	}

	if (path_overlap(pol_root, stf_root) == NO_OVERLAP) {
		result = STF_CAT_UNRELATED;
		goto out;
	}

	stf_root_len = strlen(stf_root);

	for (i = path_parts = 0; i < stf_root_len; i++) {
		if (stf_root[i] == '/')
			path_parts++;
	}

	result = (sst.snap_lins < path_parts ? STF_CAT_OVERLAPPING :
	    STF_CAT_CHANGE_DETECTED);

out:
	isi_error_handle(error, error_out);
	return result;
}

/**
 *  (See categorize_stf description.)
 *
 *  @param dfd          (IN) Snapshot directory file descriptor.
 *  @param sid          (IN) Snapshot id corresponding to STF to categorize.
 *  @param job_root     (IN) Root path of the current job.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return STF category.
 */
static enum stf_category
categorize_stf_by_sid(int dfd, ifs_snapid_t sid, char *job_root,
    struct isi_error **error_out)
{
	char			stf_name[24];
	struct isi_error	*error = NULL;
	enum stf_category	result;

	snprintf(stf_name, sizeof(stf_name), "%lu", sid);

	result = categorize_stf(dfd, stf_name, job_root, &error);

	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Free the change-detection state for a change-based policy.
 *
 *  @param pctx     (INOUT) The change-detection/etc context for a policy.
 *
 *  @return N/A.
 */
static void
free_pol_ctx(struct pol_ctx *pctx)
{
	snapid_set_clean(&pctx->interesting_sids);
	free(pctx);
}

/**
 *  Check if a job should be delayed according to the policy's job-delay
 *  configuration, if any.
 *
 *  @param srec      (INOUT) The source record for a policy.
 *  @param curr_time (IN) The current time in seconds.
 *  @param job_delay (IN) The time in seconds that the job is to be delayed by.
 *  @param pol_name  (IN) The name of the policy.
 *  @param error_out (OUT) Resulting error if any.
 *  @return True if the job should be delayed, false otherwise.
 */
static bool
check_for_job_delay(struct siq_source_record *srec,
    time_t curr_time, time_t job_delay, char *pol_name,
    struct isi_error **error_out)
{
	bool			result = false, srec_modified = false;
	time_t			delay_start, time_elapsed;
	struct isi_error	*error = NULL;

	siq_source_get_job_delay_start(srec, &delay_start);
	time_elapsed = curr_time - delay_start;

	if (job_delay > 0) {
		if (delay_start == 0) {
			// Set the job delay marker.
			delay_start = curr_time;
			siq_source_set_job_delay_start(srec, delay_start);
			log(INFO, "policy %s: job delay detected, delaying"
			    " run by %ld seconds", pol_name, job_delay);
			srec_modified = true;
			result = true;
		} else if (time_elapsed < job_delay) {
			// Delay the job.
			log(DEBUG, "policy %s: job delay detected, delaying"
			    " %ld more seconds", pol_name,
			    job_delay - time_elapsed);
			result = true;
		} else {
			// Reset the job delay marker.
			siq_source_clear_job_delay_start(srec);
			srec_modified = true;
		}
	} else if (job_delay == 0 && delay_start != 0) {
		// In the middle of processing a job delay, the job-delay
		// attribute for the policy was set back to 0, so clear
		// the job_delay_start field in the srec.
		siq_source_clear_job_delay_start(srec);
		srec_modified = true;
	}

	if (srec_modified) {
		siq_source_record_save(srec, &error);
		if (error)
			log(ERROR, "Failed to siq_source_record_save: %s",
			    isi_error_get_message(error));
	}

	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Initialize change-detection/etc state for a change-based policy.
 *
 *  @param pol          (IN) The policy object.
 *  @param pol_sid      (IN) The snapshot identifier corresponding to the
 *                      snapshot tracking file currently associated with
 *                      the policy.
 *  @param curr_time    (IN) The current time.
 *  @param new_pctx     (OUT) The new policy change-detection/etc context.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return True if change was detected, false otherwise.
 */
static bool
check_for_change_initialize(struct siq_policy *pol, ifs_snapid_t pol_sid,
    time_t curr_time, struct pol_ctx **new_pctx, struct isi_error **error_out)
{
	int		dfd = -1, ret;
	DIR		*dir = NULL;
	bool		result = false;
	char		*end_ptr, *root = pol->datapath->root_path;
	size_t		len;
	unsigned int	num_elements;
	ifs_snapid_t	sid = INVALID_SNAPID, tmp_sid, max_sid;
	ifs_snapid_t	*elements, window_adjustment;
	struct dirent	*entry;
	struct pol_ctx	*pctx = NULL;

	struct isi_error	*error = NULL;
	struct snapid_vec	sid_vec = SNAPID_VEC_INITIALIZER;

	//  Allocate & initialize the policy context.
	pctx = malloc(sizeof(*pctx));
	ASSERT(pctx);

	memset(pctx, 0, sizeof(*pctx));
	pctx->sid = pol_sid;
	pctx->last_update = curr_time;
	strcpy(pctx->pid, pol->common->pid);
	snapid_set_init(&pctx->interesting_sids);

	//  Bypass change detection for initial syncs.
	if (pctx->sid == INVALID_SNAPID) {
		result = true;
		goto out;
	}

	//  Get the max sid currently in use in the system.
	len = sizeof(max_sid);
	ret = sysctlbyname("efs.bam.last_snapid", &max_sid, &len, NULL, 0); 
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Error retrieving last_snapid");
		goto out;
	}

	//  Open the hidden snapshot directory.
	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);
	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir(1)");
		goto out;
	}

	dir = fdopendir(dfd);
	if (dir == NULL) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir for enumeration");
		goto out;
	}

	//  Since fdopendir takes ownership of the file descriptor,
	//  get a second one to use for sid categorization.
	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);
	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir(2)");
		goto out;
	}

	//  Use a single loop to find & categorize STFs via two methods
	//  concurrently:
	//      1) incrementally, over the range [cbj_ctx->job_sid, last_sid]
	//      2) enumeration of snapshot directory content
	//  If a change is detected in the process then the job can run
	//  immediately; otherwise, use the results of the method that
	//  finishes first.
	for (sid = pctx->sid, entry = readdir(dir);
	    sid <= max_sid && entry != NULL;
	    sid++, entry = readdir(dir)) {

		//  Method 1: categorize the next STF in the range.
		switch (categorize_stf_by_sid(dfd, sid, root, &error)) {
		case  STF_CAT_ERROR:
			goto out;
		case  STF_CAT_NOT_STF:
		case  STF_CAT_UNRELATED:
		case  STF_CAT_MISSING:
			break;
		case STF_CAT_PATHLESS:
		case STF_CAT_OVERLAPPING:
			snapid_set_append(&pctx->interesting_sids, sid);
			break;
		case STF_CAT_CHANGE_DETECTED:
			result = true;
			goto out;
		default:
			ASSERT(false);
			break;
		}

		//  Method 2: categorize the next dir-enum'd STF.

		//  If the entry name doesn't look like an STF or the
		//  corresponding sid is below this job's sid, ignore it.
		tmp_sid = strtoul(entry->d_name, &end_ptr, 10);
		if (tmp_sid < pctx->sid || !end_ptr || *end_ptr)
			continue;

		//  Leverage method 1's results when possible.
		if (tmp_sid <= sid) {
			if (snapid_set_contains(&pctx->interesting_sids,
			    tmp_sid))
				snapid_vec_push(&sid_vec, tmp_sid);
			continue;
		}

		switch (categorize_stf(dfd, entry->d_name, root, &error)) {
		case  STF_CAT_ERROR:
			goto out;
		case STF_CAT_NOT_STF:
		case STF_CAT_UNRELATED:
		case STF_CAT_MISSING:
			break;
		case STF_CAT_PATHLESS:
		case STF_CAT_OVERLAPPING:
			snapid_vec_push(&sid_vec, tmp_sid);
			break;
		case STF_CAT_CHANGE_DETECTED:
			result = true;
			sid = tmp_sid;
			goto out;
		default:
			ASSERT(false);
			break;
		}
	}

	if (sid < max_sid) {
		//  Method 2 completed first. Flush the interesting_sids set,
		//  then sort the sids in vector collection and copy them to
		//  the interesting_sids set.
		snapid_set_truncate(&pctx->interesting_sids);

		snapid_vec_sort(&sid_vec);

		elements = snapid_vec_first(&sid_vec);
		num_elements = snapid_vec_size(&sid_vec);
		snapid_set_copy_from_array(&pctx->interesting_sids,
		    elements, num_elements);
	}

	//  Initialize the sid history window. The window adjustment is to
	//  mitigate the possibility that an STF corresponding to the max_sid
	//  value (and/or other STFs close to that value) returned by the
	//  kernel hasn't been created yet, in which case the logic above will
	//  have dismissed the sid as non-interesting. The mitigation is to
	//  re-examine the sids/STFs within the window on the next pass.
	//  The adjustment is computed as follows:
	//
	//      2 (sids/job) * 2 (full passes of max_concurrent_jobs) * 
	//          max_concurrent_jobs.
	//
	window_adjustment = 2 * 2 * sched_get_max_concurrent_jobs();

	if ((pctx->sid + window_adjustment) < max_sid)
		pctx->prev_max_sid = max_sid - window_adjustment;
	else
		pctx->prev_max_sid = pctx->sid;

out:
	if (dir)
		closedir(dir);

	if (dfd >= 0)
		close(dfd);

	snapid_vec_clean(&sid_vec);

	if (result) {
		pctx->change_detected = true;
		log(INFO, "policy %s: change detected on snapid %lu",
		    pol->common->name, sid);
	} else {
		log(DEBUG, "%s: policy %s: no change detected",
		    __func__, pol->common->name);
	}

	if (error) {
		free_pol_ctx(pctx);
		pctx = NULL;
	}

	*new_pctx = pctx;

	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Update change-detection state for a change-based policy.
 *
 *  @param pol          (IN) The policy object.
 *  @param pctx         (INOUT) The policy's change-detection/etc context.
 *  @param curr_time    (IN) The current time.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return True if change was detected, false otherwise.
 */
static bool
check_for_change_update(struct siq_policy *pol, struct pol_ctx *pctx,
    time_t curr_time, struct isi_error **error_out)
{
	int	dfd = -1, ret;
	bool	result = false;
	char	*root = pol->datapath->root_path;
	size_t	len;

	ifs_snapid_t		sid = INVALID_SNAPID, max_sid;
	struct isi_error	*error = NULL;
	struct snapid_set	sid_set = SNAPID_SET_INITIALIZER;
	const ifs_snapid_t	*sid_enum;

	//  Don't update any more often the necessary.
	if ((curr_time - pctx->last_update) < CHANGE_BASED_POLICY_INTERVAL)
		goto out;

	pctx->last_update = curr_time;

	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);

	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir");
		goto out;
	}

	//  Step through interesting_sids to see if any changes occured,
	//  tracking non-interesting sids in the temp set for removal later.
	//
	//  N.B. STFs which are categorized as MISSING are only removed after
	//      a CHANGE_BASED_POLICY_INTERVAL (at minimum) has elapsed and
	//      status has been checked at least twice; this is to mitigate
	//      the race condition where the kernel increments last_snapid
	//      prior to creating the corresponding STF.
	SNAPID_SET_FOREACH(sid_enum, &pctx->interesting_sids) {

		sid = *sid_enum;
		switch (categorize_stf_by_sid(dfd, sid, root, &error)) {
		case  STF_CAT_ERROR:
			goto out;
		case  STF_CAT_NOT_STF:
		case  STF_CAT_UNRELATED:
			snapid_set_append(&sid_set, sid);
			break;
		case  STF_CAT_MISSING:
			if (sid < pctx->prev_max_sid)
				snapid_set_append(&sid_set, sid);
			break;
		case STF_CAT_PATHLESS:
		case STF_CAT_OVERLAPPING:
			break;
		case STF_CAT_CHANGE_DETECTED:
			result = true;
			goto out;
		default:
			ASSERT(false);
			break;
		}
	}

	//  Get the max sid currently in use in the system, to
	//  be used as the upper bound on the history window.
	len = sizeof(max_sid);
	ret = sysctlbyname("efs.bam.last_snapid", &max_sid, &len, NULL, 0); 
        if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Error retrieving last_snapid");
		goto out;
	}

	//  Step through sids in the history window, categorizing
	//  any which aren't recognized.
	for (sid = pctx->prev_max_sid; sid <= max_sid; sid++) {

		if (snapid_set_contains(&pctx->interesting_sids, sid))
			continue;

		switch (categorize_stf_by_sid(dfd, sid, root, &error)) {
		case  STF_CAT_ERROR:
			goto out;
		case STF_CAT_NOT_STF:
		case STF_CAT_UNRELATED:
			break;
		case STF_CAT_MISSING:
		case STF_CAT_PATHLESS:
		case STF_CAT_OVERLAPPING:
			snapid_set_add(&pctx->interesting_sids, sid);
			break;
		case STF_CAT_CHANGE_DETECTED:
			result = true;
			goto out;
		default:
			ASSERT(false);
			break;
		}
	}

	//  Remove any sids that were identified as non-interesting above.
	if (snapid_set_size(&sid_set) < 1) {
		//  Nothing to remove.
	} else if (snapid_set_size(&sid_set) < 5) {
		//  A few block copies are cheaper than a diff.
		SNAPID_SET_FOREACH_REVERSE(sid_enum, &sid_set) {
			snapid_set_remove(&pctx->interesting_sids,
			    *sid_enum);
		}
	} else {
		//  A diff is cheaper than alot of block copies.
		snapid_set_difference(&pctx->interesting_sids,
		    &pctx->interesting_sids, &sid_set);
	}

	//  Slide the sid history window forward a notch
	pctx->prev_max_sid = max_sid;

out:
	if (dfd >= 0)
		close(dfd);

	snapid_set_clean(&sid_set);

	if (result) {
		pctx->change_detected = true;
		log(INFO, "policy %s: change detected on snapid %lu",
		    pol->common->name, sid);
	} else {
		log(DEBUG, "%s: policy %s: no change detected",
		    __func__, pol->common->name);
	}

	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Checks whether there is work to do for a change-based policy, based
 *  on detection of changes in the policy's root path and the policy's
 *  execution history.
 *
 *  @param sctx         (IN) The scheduler context.
 *  @param pol          (IN) The policy object.
 *  @param srec         (IN) The current policy's source record.
 *  @param curr_time    (IN) The current time.
 *  @param skip_backoff_delays	(IN) If true, skip processing backoff delays.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return N/A.
 */
bool
check_for_change_based_work(struct sched_ctx *sctx, struct siq_policy *pol,
    struct siq_source_record *srec, time_t curr_time, bool skip_backoff_delays,
    struct isi_error **error_out)
{
	bool			result = false;
	ifs_snapid_t		sid = INVALID_SNAPID;
	struct pol_ctx		*entry, *pctx = NULL;
	struct isi_error	*error = NULL;

	//  Get the snapshot identifier currently associated
	//  with the policy.
	siq_source_get_latest(srec, &sid);

	//  Try to find an existing context for the policy.
	SLIST_FOREACH(entry, &sctx->pctx_list, next) {
		if (strcmp(entry->pid, pol->common->pid) == 0) {
			pctx = entry;
			break;
		}
	}

	//  If a context was found and the saved snapshot identifier
	//  doesn't match the current one then free the existing
	//  context and begin anew. (Alternatively, the context could
	//  be reset, but this is simpler and does not incur much of
	//  a performance penalty on average.)
	if (pctx && pctx->sid != sid) {
		SLIST_REMOVE(&sctx->pctx_list, pctx, pol_ctx, next);
		free_pol_ctx(pctx);
		pctx = NULL;
	}

	//  Perform the check appropriate to the context state.
	//  When the first change is detected since a successful sync,
	//  either !pctx or !pctx->change_detected will be true and one
	//  of the the first two branches will be executed.
	if (!pctx) {
		result = check_for_change_initialize(pol, sid, curr_time,
		    &pctx, &error);
		if (pctx)
			SLIST_INSERT_HEAD(&sctx->pctx_list, pctx, next);
	} else if (!pctx->change_detected) {
		result = check_for_change_update(pol, pctx, curr_time, &error);
	} else {
		// A change has already been detected.
		result = true;
	}

	if (result) {
		if (!skip_backoff_delays)
			result = !check_for_backoff_delay(pol,
			    &pctx->delay_ctx, &error);

		//  !result or delay_ctx.finished_delay indicate that we are
		//  currently or have just finished processing a backoff
		//  delay so ignore job delays.
		if (error || !result ||
		    (pctx != NULL && pctx->delay_ctx.finished_delay))
			goto out;

		//  Skip job delays for initial syncs.
		if (sid == INVALID_SNAPID)
			goto out;

		//  Perform job delay logic.
		result = !check_for_job_delay(srec, curr_time,
		    pol->scheduler->job_delay, pol->common->name, &error);
	}

out:
	if (pctx)
		pctx->last_check = curr_time;
	isi_error_handle(error, error_out);
	return result;
}

/**
 *  Determines whether a policy is a change-based one according its schedule
 *  and action.
 *
 *  @param pol          (IN) The policy object.
 *  @param recur_str    (IN) The recurrance component of a policy schedule.
 *
 *  @return True if the policy is a valid change-based one, false otherwise.
 */
bool
is_policy_change_based(struct siq_policy *pol, char *recur_str)
{
	if (strcasecmp(recur_str, SCHED_ALWAYS) == 0 &&
	    (pol->common->action == SIQ_ACT_SYNC ||
	    pol->common->action == SIQ_ACT_REPLICATE))
		return true;

	return false;
}

/**
 *  Reaps stale change-based policy contexts, i.e. those for policies
 *  which have been deleted, disabled, or modified schedulewise.
 *
 *  @param sctx         (IN) The scheduler context.
 *  @param curr_time    (IN) The current time.
 *
 *  @return N/A.
 */
void
reap_stale_pol_ctxs(struct sched_ctx *sctx, time_t curr_time)
{
	bool			stale;
	char			*pol_name, *recur_str;
	time_t			base_time;
	struct pol_ctx		*pctx, *tmp;
	struct siq_policy	*pol;

	SLIST_FOREACH_SAFE(pctx, &sctx->pctx_list, next, tmp) {
		if (pctx->last_check >= curr_time)
			continue;

		//  If here, the policy is either starting/running,
		//  disabled/deleted, or its schedule has changed.
		//  In either of the latter two cases, the context
		//  is stale and can be freed.
		stale = true;
		pol_name = "<unknown>";
		SLIST_FOREACH(pol, &sctx->policy_list->root->policy, next) {
			if (strcmp(pol->common->pid, pctx->pid) != 0)
				continue;
			
			pol_name = pol->common->name;

			if (pol->common->state == SIQ_ST_OFF)
				break;

			recur_str = NULL;

			if (!get_schedule_info(pol->scheduler->schedule,
			    &recur_str, &base_time))
				break;

			if (is_policy_change_based(pol, recur_str))
				stale = false;

			free(recur_str);
			break;
		}

		if (!stale)
			continue;

		log(DEBUG, "%s: reaping stale context, policy %s, id %s",
		    __func__, pol_name, pctx->pid);

		SLIST_REMOVE(&sctx->pctx_list, pctx, pol_ctx, next);
		free_pol_ctx(pctx);
	}
}

