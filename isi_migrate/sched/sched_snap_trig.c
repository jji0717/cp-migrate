#include <ifs/ifs_lin_open.h>
#include <ifs/stf/stf_on_disk.h>
#include <fnmatch.h>

#include "sched_includes.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_snapshot/snapshot.h"
#include "sched_main_work.h"
#include "sched_snap_trig.h"

/*
 * Determines whether a policy is a snapshot-triggered one based on its
 * schedule and action.
 *
 * @param pol		(IN) The policy object.
 * @param recur_str	(IN) The recurrance component of a policy schedule.
 *
 * @return True if the policy is snapshot-triggered, false otherwise.
 */
bool
is_policy_snap_trig(struct siq_policy *pol, char *recur_str)
{
	if (strcasecmp(recur_str, SCHED_SNAP) == 0 &&
	    (pol->common->action == SIQ_ACT_SYNC ||
	    pol->common->action == SIQ_ACT_REPLICATE))
		return true;

	return false;
}

/*
 * Get the snapshot-triggered sync context for a given policy. If the policy
 * does not have an existing snapshot-triggered sync context, a new one will
 * be created.
 *
 * @param sctx			(IN) The schedule context.
 * @param pol			(IN) The policy object.
 * @param loaded_snap_ctx	(OUT) The snapshot-triggered sync context for
 *				      the given policy.
 *
 * @return None.
 */
static void
load_snap_ctx(struct sched_ctx *sctx, struct siq_policy *pol, time_t curr_time,
    struct pol_snap_ctx **loaded_snap_ctx)
{
	struct pol_snap_ctx *entry = NULL, *snap_ctx = NULL;

	SLIST_FOREACH(entry, &sctx->snap_ctx_list, next) {
		if (!strcmp(entry->pid, pol->common->pid)) {
			snap_ctx = entry;
			break;
		}
	}

	/* Initialize the snapshot context for this policy if it doesn't
	 * exist. */
	if (snap_ctx == NULL) {
		snap_ctx = calloc(1, sizeof(*snap_ctx));
		ASSERT(snap_ctx);
		strcpy(snap_ctx->pid, pol->common->pid);
		snap_ctx->last_check = 0;
		SLIST_INSERT_HEAD(&sctx->snap_ctx_list, snap_ctx, next);
	}

	*loaded_snap_ctx = snap_ctx;
}

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
    char *name, struct isi_error **error_out)
{
	int sfd, path_enc, ret;
	bool result = false;
	char *paths[1] = { path };
	struct isi_error *error = NULL;

	memset(sst, 0, sizeof(*sst));

	sfd = enc_openat(dfd, stf_name, ENC_DEFAULT, O_RDONLY);
	if (sfd < 0) {
		/* Non-existence is acceptable, other errors aren't. */
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Error opening %s", stf_name);
		}
		goto out;
	}

	sst->paths = paths;
	sst->paths_num = 1;
	sst->paths_enc = &path_enc;
	sst->name = name;
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

/*
 * Helper function for update_snap_sync_list. This function determines if a
 * STF satisfies the conditions necessary to trigger a sync for the current
 * policy.
 *
 * @param dfd		(IN) File descriptor for the hidden snapshot lin dir.
 * @param snap_ctx	(INOUT) The snapshot-triggered context for the policy.
 * @param latest_id	(IN) The "latest" sid from the policy's source record.
 * @param curr_time	(IN) The current time.
 * @param error_out	(OUT) The resulting error, if any.
 *
 * @return None.
 */
static bool
snap_meets_criteria(int dfd, char *snapid, char *pol_root, char *pattern,
    struct isi_error **error_out)
{
	bool result = false;
	char stf_root[MAXPATHLEN] = "";
	char stf_name[MAXPATHLEN] = "";
	struct stf_stat sst = {};
	struct isi_error *error = NULL;


	if (!get_stf_stat(dfd, snapid, &sst, stf_root, stf_name, &error))
		goto out;

	if (sst.state == STF_DELETE)
		goto out;

	if (sst.sf_type != SF_STF || sst.system != 0)
		goto out;

	if (sst.paths_num == 0)
		goto out;

	if (strcmp(stf_root, pol_root))
		goto out;

	if (fnmatch(pattern, stf_name, 0) || !strncmp("SIQ-", stf_name, 4))
		goto out;

	result = true;
out:
	isi_error_handle(error, error_out);
	return result;
}

/*
 * This function looks for the next snapshot that meets the criteria to
 * trigger a sync for the given policy.
 *
 * @param latest_sid	(IN) The "latest" sid from the policy's source record.
 * @param find_max	(IN) If true, the function looks for the max sid that
 *			     can trigger a sync. Otherwise the minimum valid
 *			     sid is chosen.
 * @param pol		(IN) The current policy.
 * @param error_out	(OUT) The resulting error, if any.
 *
 * @return The SID of the next snapshot that can trigger a sync. If none are
 *	   found, INVALID_SNAPID is returned.
 */
static ifs_snapid_t
find_next_valid_sid(ifs_snapid_t latest_sid, bool find_max,
    struct siq_policy *pol, struct isi_error **error_out)
{
	int		dfd = -1, ret = -1, inc;
	DIR		*dir = NULL;
	char		*pattern = pol->scheduler->snapshot_sync_pattern;
	char		*src_dir = pol->datapath->root_path, *end_ptr;
	char		sid_str[21] = "";
	bool		res = false;
	size_t		len;
	ifs_snapid_t	sid, sid_bound, max_sid, tmp_sid;
	ifs_snapid_t	next_sid = INVALID_SNAPID;
	struct dirent	*entry;

	struct isi_error *error = NULL;

	/* Open the hidden snapshot directory. */
	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);
	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir(1)");
		goto out;
	}

	dir = fdopendir(dfd);
	if (dir == NULL) {
		/* If fdopendir() fails it will close dfd. */
		dfd = -1;
		error = isi_system_error_new(errno,
		    "Error opening snapids dir for enumeration");
		goto out;
	}

	/*  Since fdopendir takes ownership of the file descriptor,
	 *  get a second one to use for snapshot analysis. */
	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);
	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error opening snapids dir(2)");
		goto out;
	}

	/* Get the max sid currently in use in the system. */
	len = sizeof(max_sid);
	ret = sysctlbyname("efs.bam.last_snapid", &max_sid, &len, NULL, 0);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Error retrieving last_snapid");
		goto out;
	}

	if (find_max) {
		/* look through the range [max_sid, 0) */
		sid = max_sid;
		sid_bound = 0;
		inc = -1;
	} else {
		/* look through the range [latest_sid +1, max_sid +1) */
		sid = latest_sid + 1;
		sid_bound = max_sid + 1;
		inc = 1;
	}

	/*  Use a single loop to find & analyze STFs via two methods
	 *  concurrently:
	 *      1) incrementally, over the range [sid, sid_bound)
	 *      2) enumeration of snapshot directory content
	 *  We can use the result of whichever method finishes first.
	 */
	for (entry = readdir(dir);
	    sid != sid_bound && entry != NULL;
	    sid += inc, entry = readdir(dir)) {

		/* Method 1: Check if the next STF in the range exists and is
		 * a candidate to trigger a sync. */
		snprintf(sid_str, sizeof(sid_str), "%lu", sid);
		res = snap_meets_criteria(dfd, sid_str, src_dir, pattern,
		    &error);

		if (error)
			goto out;

		if (res) {
			next_sid = sid;
			break;
		}

		/* Method 2: Check if the next entry in the directory is a
		 * candidate to trigger a sync. */
		tmp_sid = strtoul(entry->d_name, &end_ptr, 10);
		if (tmp_sid <= latest_sid || !end_ptr || *end_ptr)
			continue;

		if (find_max) {
			/* Already found one bigger, or already checked by
			 * method 1. */
			if (tmp_sid < next_sid || tmp_sid >= sid)
				continue;
		} else {
			/* Already found one smaller, or already checked by
			 * method 1. */
			if ((next_sid && tmp_sid > next_sid) ||
			    tmp_sid <= sid)
				continue;
		}

		res = snap_meets_criteria(dfd, entry->d_name, src_dir, pattern,
		    &error);

		if (error)
			goto out;

		if (res)
			next_sid = tmp_sid;
	}

	ASSERT(next_sid == INVALID_SNAPID || next_sid > latest_sid);

out:

	if (dir)
		closedir(dir);

	if (dfd >= 0)
		close(dfd);

	isi_error_handle(error, error_out);
	return next_sid;
}

/*
 * Check if there are any snapshots that should trigger a sync for the given
 * policy. If any such snapshots exist, modify the policy's source record so
 * it will use the next valid snapshot on its next sync.
 *
 * @param sctx		(IN) The scheduler context.
 * @param pol		(IN) The policy object.
 * @param srec		(IN) The policy's source record
 * @param error_out	(OUT) The resulting error, if any.
 *
 * @return True if a sync should be started, false otherwise.
 */
bool
check_for_snap_trig_work(struct sched_ctx *sctx, struct siq_policy *pol,
    struct siq_source_record *srec, struct isi_error **error_out)
{
	ifs_snapid_t latest_sid = 0, next_sid = 0, man_sid = 0;
	bool result = false, find_max = false;
	time_t curr_time = time(NULL);
	struct pol_snap_ctx *snap_ctx = NULL;
	struct isi_error *error = NULL;

	load_snap_ctx(sctx, pol, curr_time, &snap_ctx);

	/* Impose a delay if the previous run failed. */
	if (check_for_backoff_delay(pol, &snap_ctx->delay_ctx, &error)
	    || error)
		goto out;

	/* Don't run more than necessary, take a small break. */
	if ((curr_time - snap_ctx->last_check) < SNAP_TRIG_POLICY_INTERVAL)
		goto out;

	snap_ctx->last_check = curr_time;

	/* Clean up old calculations that could exist from previous failed
	 * runs, we should recalculate */
	siq_source_get_manual(srec, &man_sid);
	if (man_sid != INVALID_SNAPID) {
		siq_source_set_manual_snap(srec, INVALID_SNAPID);
		siq_source_record_save(srec, &error);
	}

	siq_source_get_latest(srec, &latest_sid);

	/* If the user chose not to sync all existing snapshots, start
	 * with the most recent snapshot on the initial sync. */
	if (latest_sid == 0 &&
	    !pol->scheduler->snapshot_sync_existing)
		find_max = true;

	next_sid = find_next_valid_sid(latest_sid, find_max, pol, &error);
	if (error || next_sid == INVALID_SNAPID)
		goto out;

	siq_source_set_manual_snap(srec, next_sid);
	siq_source_record_save(srec, &error);
	result = true;

out:
	isi_error_handle(error, error_out);
	return result;
}

/*
 *  Reaps stale snapshot-triggered policy contexts, i.e. those for policies
 *  which have been deleted, disabled, or modified schedulewise.
 *
 *  @param sctx         (IN) The scheduler context.
 *
 *  @return None.
 */
void reap_stale_snap_ctxs(struct sched_ctx *sctx)
{
	bool stale;
	char *pol_name, *recur_str;
	time_t base_time;
	struct pol_snap_ctx *snap_ctx, *tmp;
	struct siq_policy *pol;

	SLIST_FOREACH_SAFE(snap_ctx, &sctx->snap_ctx_list, next, tmp) {

		/* If here, the policy is either starting/running,
		 * disabled/deleted, or its schedule has changed.
		 * In either of the latter two cases, the context
		 * is stale and can be freed. */
		stale = true;
		pol_name = "<unknown>";

		SLIST_FOREACH(pol, &sctx->policy_list->root->policy, next) {
			if (strcmp(pol->common->pid, snap_ctx->pid) != 0)
				continue;

			pol_name = pol->common->name;

			if (pol->common->state == SIQ_ST_OFF)
				break;

			recur_str = NULL;

			if (!get_schedule_info(pol->scheduler->schedule,
			    &recur_str, &base_time))
				break;

			if (is_policy_snap_trig(pol, recur_str))
				stale = false;

			free(recur_str);
			break;
		}

		if (!stale)
			continue;

		log(INFO,
		    "%s: reaping stale snapshot-triggered context,"
		    " policy %s, id %s", __func__, pol_name, snap_ctx->pid);

		SLIST_REMOVE(&sctx->snap_ctx_list, snap_ctx, pol_snap_ctx, next);
		free(snap_ctx);
	}
}
