#include <sys/types.h>
#include <sys/wait.h>
#include <sys/uio.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <errno.h>
#include <netdb.h>
#include <assert.h>
#include <arpa/inet.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_procoptions.h>
#include <ifs/ifs_lin_open.h>

#include <isi_ufp/isi_ufp.h>
#include <isi_celog/isi_celog.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_domain/worm.h>

#include "isi_snapshot/snapshot.h"
#include "coord.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_alert.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/siq_error.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/selectors.h"
#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/lin_list.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/sched/siq_jobctl.h"

static struct timeval bwt_timeout = {10, 0};

void
failback_prep_restore_rep_rename(bool pre_restore,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN], restore_rep_name[MAXNAMLEN];
	char cpss_name[MAXNAMLEN], restore_cpss_name[MAXNAMLEN];

	get_base_rep_name(rep_name, g_ctx.rc.job_id, false);
	get_base_rep_name(restore_rep_name, g_ctx.rc.job_id, true);

	get_base_cpss_name(cpss_name, g_ctx.rc.job_id, false);
	get_base_cpss_name(restore_cpss_name, g_ctx.rc.job_id, true);

	if (pre_restore) {
		/*
		 * We have to rename rep from base policy i.e
		 * polid_snap_rep_base to restore rep base i.e
		 * polid_snap_rep_base_restore.
		 */
		siq_safe_btree_rename(siq_btree_repstate, g_ctx.rc.job_id,
		    rep_name, g_ctx.rc.job_id, restore_rep_name, &error);
		if (error)
			goto out;

		siq_safe_btree_rename(siq_btree_cpools_sync_state,
		    g_ctx.rc.job_id, cpss_name, g_ctx.rc.job_id,
		    restore_cpss_name, &error);
		if (error)
			goto out;
	} else {
		/*
		 * We have to rename rep from restore base policy i.e
		 * polid_snap_rep_base_restore to policy rep base i.e
		 * polid_snap_rep_base.
		 */
		siq_safe_btree_rename(siq_btree_repstate, g_ctx.rc.job_id,
		    restore_rep_name, g_ctx.rc.job_id, rep_name, &error);
		if (error)
			goto out;

		siq_safe_btree_rename(siq_btree_cpools_sync_state,
		    g_ctx.rc.job_id, restore_cpss_name, g_ctx.rc.job_id,
		    cpss_name, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

void
on_failover_restore_done(struct isi_error **error_out)
{
	int rv = 0;
	int lock_fd = -1;
	struct target_record *trec = NULL;
	char name[MAXNAMLEN + 1];
	struct siq_policies *pl = NULL;
	struct siq_policy *mirror_pol = NULL;
	struct siq_source_record *mirror_srec = NULL;
	struct isi_error *error = NULL;
	ifs_snapid_t restore_new_id = INVALID_SNAPID;
	ifs_snapid_t snapid = INVALID_SNAPID;
	ifs_snapid_t restore_latest_id = INVALID_SNAPID;
	char ssname[MAXPATHLEN];
	bool found_alias;

	ASSERT(g_ctx.restore == true);

	lock_fd = siq_target_acquire_lock(g_ctx.rc.job_id, O_EXLOCK, &error);

	if (-1 == lock_fd || error)
		goto out;

	trec = siq_target_record_read(g_ctx.rc.job_id, &error);
	if (error)
		goto out;

	trec->fofb_state = FOFB_FAILOVER_DONE;

	if (!g_ctx.snap_revert_op) {
		/* Create failover snap which is used to for reverse
		 * incremental syncs */
		get_failover_snap_name(g_ctx.rc.job_name, ssname);
		snapid = takesnap(ssname, g_ctx.rc.path, 0, NULL, &error);
		if (error)
			goto out;

		trec->latest_snap = snapid;
	}

	if (g_ctx.failback_action != SIQ_ACT_SNAP_REVERT_DOMAIN_MARK &&
	    g_ctx.failback_action != SIQ_ACT_SYNCIQ_DOMAIN_MARK &&
	    g_ctx.failback_action != SIQ_ACT_WORM_DOMAIN_MARK &&
	    !g_ctx.snap_revert_op) {
		dom_set_siq(trec->domain_id);
	}

	rv = dom_set_readonly(trec->domain_id, false);
	if (rv != 0) {
		error = isi_system_error_new(errno,
		    "Unable to set read only flag for domain %llu",
		    trec->domain_id);
		goto out;
	}

	/*
	 * Snapshot Alias Exports:
	 * move the target snap alias to HEAD if it is there
	 */
	if (trec->latest_archive_snap_alias != INVALID_SNAPID) {
		move_snapshot_alias_helper(HEAD_SNAPID,
		    trec->latest_archive_snap_alias, NULL, &found_alias,
		    &error);
		/*
		 * Bug 184117
		 * Ignore a missing snap alias during allow-write
		 */
		if (error) {
			if (isi_error_is_a(error,
			    SNAP_NOTFOUND_ERROR_CLASS) && !found_alias) {
				isi_error_free(error);
				error = NULL;
				trec->latest_archive_snap_alias =
				    INVALID_SNAPID;
			} else
			    goto out;
		}
	}

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	close(lock_fd);
	lock_fd = -1;

	if (g_ctx.record == NULL) {
		g_ctx.record = siq_source_record_load(g_ctx.rc.job_id, &error);
		if (error)
			goto out;
	}

	siq_source_get_restore_new(g_ctx.record, &restore_new_id); 
	if (restore_new_id != INVALID_SNAPID) {
		siq_source_get_restore_latest(g_ctx.record,
		    &restore_latest_id);
		siq_source_set_restore_latest(g_ctx.record, restore_new_id);
		siq_source_clear_restore_new(g_ctx.record);
		siq_source_record_save(g_ctx.record, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
			    "Error clearing new restore snapshot: %s",
			    isi_error_get_message(error));

		if (restore_latest_id != INVALID_SNAPID &&
		    !g_ctx.snap_revert_op) {
			delete_snapshot_helper(restore_latest_id);
		}
	}

	post_success_prune_btrees(g_ctx.prev_snap.id, g_ctx.curr_snap.id);

	/* Remove restore base repstate */
	get_base_rep_name(name, g_ctx.rc.job_id, g_ctx.restore);
	remove_repstate(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	/* Remove restore linmaps */
	get_lmap_name(name, g_ctx.rc.job_id, g_ctx.restore);
	remove_linmap(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	if (!g_ctx.snap_revert_op) {
		siq_failover_snap_rename_new_to_latest(g_ctx.rc.job_id, 
		    g_ctx.rc.job_name, &error);
	}

	/*
	 * If we have mirror policy , make sure we clear failback
	 * states so that future failbacks can happen.
	 */
	pl = siq_policies_load_nolock(&error);
	if (error)
		goto out;

	mirror_pol = siq_policy_get_by_mirror_pid(pl, g_ctx.rc.job_id);

	if (mirror_pol) {
		if (siq_source_has_record(mirror_pol->common->pid)) {
			mirror_srec = siq_source_record_load(
			    mirror_pol->common->pid, &error);
			if (error)
				goto out;

			siq_source_set_failback_state(mirror_srec,
			    SIQ_FBS_NONE);

			siq_source_record_save(mirror_srec, &error);
			if (error)
				goto out;
		}
	}

	set_job_state(SIQ_JS_SUCCESS);

out:
	if (pl)
		siq_policies_free(pl);

	if (-1 != lock_fd)
		close(lock_fd);

	siq_target_record_free(trec);

	siq_source_record_free(mirror_srec);

	if (error)
		set_job_state(SIQ_JS_FAILED);

	isi_error_handle(error, error_out);
}

void
on_failover_revert_done(struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	ifs_snapid_t latest_id = INVALID_SNAPID;
	ifs_snapid_t new_id = INVALID_SNAPID;
	bool found_target, found_alias;

	struct siq_policies *pl = NULL;

	char name[MAXNAMELEN + 1];

	ASSERT(g_ctx.restore == true);

	set_job_state(SIQ_JS_SUCCESS);

	pl = siq_policies_load_nolock(&error);
	if (error)
		goto out;

	if (g_ctx.record == NULL) {
		g_ctx.record = siq_source_record_load(g_ctx.rc.job_id, &error);
		if (error)
			goto out;
	}

	// Preserve the base repstate for local policies. Failover doesn't 
	// touch it.
	get_base_rep_name(name, g_ctx.rc.job_id, g_ctx.restore);
	remove_repstate(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	siq_source_get_restore_new(g_ctx.record, &new_id);
	siq_source_clear_restore_new(g_ctx.record);
	siq_source_get_restore_latest(g_ctx.record, &latest_id);
	siq_source_clear_restore_latest(g_ctx.record);
	siq_source_record_save(g_ctx.record, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error clearing new and old restore(revert) snapshot: %s",
		    isi_error_get_message(error));

	if (!delete_snapshot_helper(new_id)) {
		log(ERROR, "%s Failover revert: Could not delete new "
		    "restore snapshot.", g_ctx.rc.job_name);
	}

	if (!delete_snapshot_helper(latest_id)) {
		log(ERROR, "%s Failover revert: Could not delete latest "
		    "restore snapshot.", g_ctx.rc.job_name);
	}

	post_success_prune_btrees(g_ctx.prev_snap.id, g_ctx.curr_snap.id);

	get_repeated_rep_name(name, g_ctx.rc.job_id, new_id, g_ctx.restore);
	remove_repstate(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	/* Remove restore linmaps */
	get_lmap_name(name, g_ctx.rc.job_id, g_ctx.restore);
	remove_linmap(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	lock_fd = siq_target_acquire_lock(g_ctx.rc.job_id, O_EXLOCK, &error);
	if (-1 == lock_fd || error)
		goto out;

	trec = siq_target_record_read(g_ctx.rc.job_id, &error);
	if (error)
		goto out;

	/* Snapshot Alias Exports:
	 *      revert the target snap alias back to the target archive snap */
	if (trec->latest_archive_snap_alias != INVALID_SNAPID &&
	    trec->latest_archive_snap != INVALID_SNAPID)
	{
		move_snapshot_alias_helper(trec->latest_archive_snap,
		    trec->latest_archive_snap_alias, &found_target,
		    &found_alias, &error);
		if (error) {
			if (isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS) &&
			    !(found_target && found_alias)) {
				isi_error_free(error);
				error = NULL;
				if (!found_target)
					trec->latest_archive_snap =
					    INVALID_SNAPID;
				if (!found_alias)
					trec->latest_archive_snap_alias =
					    INVALID_SNAPID;
			} else
				goto out;
		}
	}

	trec->fofb_state = FOFB_NONE;
	siq_target_record_write(trec, &error);

	if (error)
		goto out;

out:
	if (pl)
		siq_policies_free(pl);

	if (-1 != lock_fd)
		close(lock_fd);

	siq_target_record_free(trec);

	if (error) 
		set_job_state(SIQ_JS_FAILED);

	isi_error_handle(error, error_out);
}

static int
verify_failover_done_resp_callback(struct generic_msg *m, void *ctx)
{
	g_ctx.waiting_for_tgt_msg = false;
	migr_force_process_stop();

	if (!m->body.verify_failover_resp.failover_done) {
		fatal_error(false, SIQ_ALERT_POLICY_FAIL,
		    "Cannot start resync prep job for %s: "
		    "Allow writes not done on the target.", g_ctx.rc.job_name);
	}

	return 0;
}

void
verify_failover_done()
{
	struct generic_msg msg;
	msg.head.type = VERIFY_FAILOVER_MSG;

	if (send_tgt_msg_with_retry(&msg, VERIFY_FAILOVER_RESP_MSG,
	    verify_failover_done_resp_callback) != TGT_SEND_OK) {
		fatal_error(true, SIQ_ALERT_TARGET_CONNECTIVITY,
		    "Unable to verify failover complete on target: %s",
		    g_ctx.rc.target);
	}
}

static void
do_domain_setup(bool *tw_done, struct isi_error **error_out) 
{
	uint32_t gen = 0;
	ifs_domainid_t domid = 0;
	int ret = -1;
	struct stat st;
	struct domain_entry dom_entry;
	bool new_domain = false;
	bool has_worm;

	struct isi_error *error = NULL;

	switch (g_ctx.failback_action) {
	case SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK:
		find_domain(JOB_SPEC->datapath->root_path, DT_SYNCIQ, false,
		    &domid, &gen, &error);
		if (error)
			goto out;

		/*
		 * Bug 263493
		 * job hangs if the source dir is deleted (domain not found)
		 * and the policy is reset after FOFB.
		 */
		if (domid == 0 && JOB_SPEC->common->mirror_pid) {
			error = isi_system_error_new(EINVAL,
			    "%s: Domain not found. Please delete mirror policy %s%s "
			    "and restart job %s", __func__, JOB_SPEC->common->name,
			    "_mirror", JOB_SPEC->common->name);
			goto out;
		}
		break;

	case SIQ_ACT_SYNCIQ_DOMAIN_MARK:
		find_domain(JOB_SPEC->datapath->root_path, DT_SYNCIQ, false,
		    &domid, &gen, &error);
		break;

	case SIQ_ACT_SNAP_REVERT_DOMAIN_MARK:
		find_domain(JOB_SPEC->datapath->root_path, DT_SNAP_REVERT,
		    false, &domid, &gen, &error);
		break;

	case SIQ_ACT_WORM_DOMAIN_MARK:
		find_domain(JOB_SPEC->datapath->root_path, DT_WORM, false,
		    &domid, &gen, &error);
		break;

	default:
		ASSERT(!"invalid action for domain marking");
	}
	if (error)
		goto out;

	ret = stat(JOB_SPEC->datapath->root_path, &st);
	if (ret) {
		error = isi_system_error_new(errno, "Failed to stat %s",
		    JOB_SPEC->datapath->root_path);
		goto out;
	}

	if (!domid) {
		ASSERT(!JOB_SPEC->common->mirror_pid);
		new_domain = true;

		if (g_ctx.failback_action == SIQ_ACT_WORM_DOMAIN_MARK) {
			ret = worm_enforce_valid_path(
				JOB_SPEC->datapath->root_path);
			if (ret == -1) {
				error = isi_system_error_new(errno,
				    "%s: call to worm_enforce_valid_path() "
				    "failed", __func__);
				goto out;
			}

			ret = worm_lin_has_domain(st.st_ino, HEAD_SNAPID,
			    &has_worm);
			if (ret == -1) {
				error = isi_system_error_new(errno,
				    "%s: call to worm_lin_has_domain() "
				    "failed", __func__);
				goto out;
			}

			if (has_worm) {
				error = isi_system_error_new(EEXIST,
				    "%s is already a member or ancestor of "
				    "a WORM domain",
				    JOB_SPEC->datapath->root_path);
				goto out;
			}
		}

		ret = dom_make_domain(st.st_ino, &domid, &gen);
		if (ret) {
			error = isi_system_error_new(errno, 
			    "Failed to create domain at %s",
			    JOB_SPEC->datapath->root_path);
			goto out;
		} 
		
		switch (g_ctx.failback_action) {
		case SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK:
		case SIQ_ACT_SYNCIQ_DOMAIN_MARK:
			ret = dom_set_siq(domid);
			break;
			
		case SIQ_ACT_SNAP_REVERT_DOMAIN_MARK:
			ret = dom_set_snaprevert(domid);
			break;

		case SIQ_ACT_WORM_DOMAIN_MARK:
			ret = dom_set_worm(domid);
			break;

		default:
			ASSERT(!"invalid action for domain marking");
		}
		if (ret) {
			error = isi_system_error_new(errno,
			    "Failed to update domain at %s",
			    JOB_SPEC->datapath->root_path);
			goto out;
		}
	}

	ASSERT(domid);

	g_ctx.rc.domain_id = domid;
	g_ctx.rc.domain_generation = gen;

	ret = dom_get_entry(domid, &dom_entry);
	if (ret) {
		error = isi_system_error_new(errno, 
		    "Failed to get domain entry for %s", 
		    JOB_SPEC->datapath->root_path);
		goto out;
	}

	if (dom_entry.d_flags & DOM_READY) {
		if (tw_done)
			*tw_done = true;
		goto out;
	}

	log(DEBUG, "Adding %s to domain %llu.",
	    JOB_SPEC->datapath->root_path, domid);
	ret = ifs_domain_add_bylin(st.st_ino, domid);

	if (ret && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to domain mark %s",
		    JOB_SPEC->datapath->root_path);
		goto out;
	}

out:
	/*
	 * Bug 144520
	 * Delete the new domain if there is an error during domain creation
	 */
	if (error && new_domain && domid != 0) {
		log(NOTICE, "%s: Failed domain creation. Removing domain id "
		    "%lld", __func__, domid);
		dom_remove_domain(domid);
	}
	isi_error_handle(error, error_out);
}

// Create temporary target record for failback prep restore, if needed.
void
check_create_prep_restore_trec(struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;	

	lock_fd = siq_target_acquire_lock(g_ctx.rc.job_id, O_EXLOCK, &error);
	if (lock_fd == -1 || error)
		goto out;

	if (siq_target_record_exists(g_ctx.rc.job_id)) {
		trec = siq_target_record_read(g_ctx.rc.job_id, &error);
		if (error)
			goto out;
	} else {
		trec = calloc(1, sizeof(struct target_record));
		ASSERT(trec);

		log(DEBUG, "Creating temp target record for %s",
		    g_ctx.rc.job_name);
	}

	if (!trec->id)
		trec->id = strdup(g_ctx.rc.job_id);

	if (!trec->policy_name)
		trec->policy_name = strdup(g_ctx.rc.job_name);

	/* need to include target path to prevent policy overlap errors when
	 * running other policies */
	if (!trec->target_dir)
		trec->target_dir = strdup(g_ctx.rc.targetpath);

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

out:
	if (lock_fd != -1)
		close(lock_fd);

	siq_target_record_free(trec);

	isi_error_handle(error, error_out);
}

void
on_failback_prep_restore_done(struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	char name[MAXNAMELEN + 1];

	ifs_snapid_t restore_new_id = INVALID_SNAPID;

	struct isi_error *error = NULL;

	if (g_ctx.record == NULL) {
		g_ctx.record = siq_source_record_load(g_ctx.rc.job_id, &error);
		if (error)
			goto out;
	}

	siq_source_get_restore_new(g_ctx.record, &restore_new_id); 
	siq_source_clear_restore_new(g_ctx.record);
	siq_source_clear_restore_latest(g_ctx.record);
	siq_source_record_save(g_ctx.record, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error clearing new and old restore(revert) snapshot: %s",
		    isi_error_get_message(error));

	if (restore_new_id != INVALID_SNAPID) {
		if (!delete_snapshot_helper(restore_new_id)) {
			log(ERROR, "%s Failover revert: Could not delete new "
			    "restore snapshot.", g_ctx.rc.job_name);
		}
	}

	post_success_prune_btrees(g_ctx.prev_snap.id, g_ctx.curr_snap.id);

	/* Rename base repstate from restore to normal mode */
	failback_prep_restore_rep_rename(false, &error);
	if (error) {
		fatal_error(false, SIQ_ALERT_POLICY_FAIL, 
		    isi_error_get_message(error));
	}


	/* Remove restore linmaps */
	get_lmap_name(name, g_ctx.rc.job_id, g_ctx.restore);
	remove_linmap(g_ctx.rc.job_id, name, true, &error);
	if (error)
		goto out;

	set_job_state(SIQ_JS_SUCCESS);

out:
	if (error) 
		set_job_state(SIQ_JS_FAILED);

	isi_error_handle(error, error_out);
}

// Associate failback policy with original policy via mirror_pid field.
static void
associate_fbpol_id(const char *fbpol_id, struct isi_error **error_out)
{
	struct siq_policies *policies = NULL;
	struct siq_policy *pol = NULL;

	struct isi_error *error = NULL;	
	policies = siq_policies_load(&error);
	if (error) 
		goto out;

	pol = siq_policy_get_by_pid(policies, g_ctx.rc.job_id);
	if (!pol) {
		error = isi_siq_error_new(E_SIQ_CONF_NOENT, 
		    "Could not load policy data for %s!", g_ctx.rc.job_name);
		goto out;
	}

	ASSERT(pol->common);
	ASSERT(!pol->common->mirror_pid || 
	    !strcmp(pol->common->mirror_pid, fbpol_id));

	pol->common->mirror_pid = strdup(fbpol_id);
	pol->common->mirror_state = SIQ_MIRROR_ON;
	pol->common->state = SIQ_ST_OFF;	

	siq_policies_save(policies, &error);
	if (error)
		goto out;
	
out:
	if (policies)
		siq_policies_free(policies);

	isi_error_handle(error, error_out);	

}

static ifs_snapid_t
take_initial_failback_target_snapshot(const char *fbpol_name,
    const char *fbpol_src_cluster_name)
{
	char *fb_snap_pattern;
	char *fb_snap_alias;
	char fb_snap_name[MAXNAMELEN + 1];

	time_t now;
	struct tm *tm_now = NULL;
	struct isi_error *error = NULL;

	ifs_snapid_t snapid = INVALID_SNAPID;

	time(&now);
	tm_now = localtime(&now);

	fb_snap_pattern = strdup(DEF_TARGET_SNAP_PATTERN);
	ASSERT(fb_snap_pattern != NULL);
	expand_snapshot_patterns(&fb_snap_pattern,
	    fbpol_name, fbpol_src_cluster_name);
	strftime(fb_snap_name, MAXNAMELEN, fb_snap_pattern, tm_now);

	fb_snap_alias = strdup(DEF_TARGET_SNAP_ALIAS);
	expand_snapshot_patterns(&fb_snap_alias,
	    fbpol_name, fbpol_src_cluster_name);

	log(TRACE, "takesnap(%s)", fb_snap_name);
	snapid = takesnap(fb_snap_name, g_ctx.rc.path,
	    time(NULL) + g_ctx.rc.expiration, fb_snap_alias, &error);

	//XXXDPL Create a regular isi_error here.
	if (error) {
		fatal_error(false, SIQ_ALERT_TARGET_SNAPSHOT,
		    "Could not take initial target snapshot for %s: %s",
		    fbpol_name, isi_error_get_message(error));
	}

	if (fb_snap_pattern)
	    free(fb_snap_pattern);
	if (fb_snap_alias)
	    free(fb_snap_alias);

	return snapid;
}

static void
check_create_fbpol_trec(const struct failback_prep_finalize_resp_msg *resp, 
    struct isi_error **error_out) 
{
	int lock_fd = -1;

	struct target_record *fb_trec = NULL;

	uint32_t gen = 0;
	ifs_domainid_t domid = 0;

	struct isi_error *error = NULL;	

	ASSERT(resp->fbpol_id);
	ASSERT(resp->fbpol_name);
	ASSERT(resp->fbpol_src_cluster_name);
	ASSERT(resp->fbpol_src_cluster_id);

	lock_fd = siq_target_acquire_lock(resp->fbpol_id, O_EXLOCK, &error);
	if (lock_fd == -1 || error)
		goto out;
	
	if (siq_target_record_exists(resp->fbpol_id)) {
		fb_trec = siq_target_record_read(resp->fbpol_id, &error);
		if (error)
			goto out;
	} else {
		fb_trec = calloc(1, sizeof(struct target_record));
		ASSERT(fb_trec);

		log(NOTICE, "Creating failback target record for %s",
		    resp->fbpol_name);
	}

	find_domain(JOB_SPEC->datapath->root_path, DT_SYNCIQ, false, &domid,
	    &gen, &error);
	if (error)
		goto out;

	FREE_AND_NULL(fb_trec->id);
	fb_trec->id = strdup(resp->fbpol_id);

	FREE_AND_NULL(fb_trec->policy_name);
	fb_trec->policy_name = strdup(resp->fbpol_name);

	FREE_AND_NULL(fb_trec->target_dir);
	fb_trec->target_dir = strdup(JOB_SPEC->datapath->root_path);

	FREE_AND_NULL(fb_trec->src_cluster_name);
	fb_trec->src_cluster_name = strdup(resp->fbpol_src_cluster_name);

	FREE_AND_NULL(fb_trec->src_cluster_id);
	fb_trec->src_cluster_id = strdup(resp->fbpol_src_cluster_id);

	fb_trec->domain_id = domid;
	fb_trec->domain_generation = gen;
	fb_trec->linmap_created = true;	
	fb_trec->fofb_state = FOFB_NONE;


	fb_trec->latest_snap = take_initial_failback_target_snapshot(
	    resp->fbpol_name,
	    resp->fbpol_src_cluster_name);

	ASSERT(fb_trec->latest_snap);

	siq_target_record_write(fb_trec, &error);
	if (error)
		goto out;

out:
	if (lock_fd != -1)
		close(lock_fd);

	siq_target_record_free(fb_trec);

	isi_error_handle(error, error_out);
}

static void
do_failback_database_renames(const char *orig_pid, const char *failback_pid,
    bool compliance_v2, struct isi_error **error_out)
{
	char orig_base_rep_name[MAXNAMLEN];
	char fb_base_rep_name[MAXNAMLEN];
	char orig_linmap_name[MAXNAMLEN];
	char fb_linmap_name[MAXNAMLEN];
	char orig_base_cpss_name[MAXNAMLEN];
	char fb_base_cpss_name[MAXNAMLEN];
	char orig_resync_name[MAXNAMLEN];
	char fb_resync_name[MAXNAMLEN];
	char orig_comp_map_name[MAXNAMLEN];
	char fb_comp_map_name[MAXNAMLEN];

	struct isi_error *error = NULL;

	get_base_rep_name(orig_base_rep_name, orig_pid, false);
	get_mirrored_rep_name(fb_base_rep_name, failback_pid);

	siq_safe_btree_rename(siq_btree_repstate, orig_pid, orig_base_rep_name,
	    failback_pid, fb_base_rep_name, &error);
	if (error)
		goto out;

	get_base_cpss_name(orig_base_cpss_name, orig_pid, false);
	get_mirrored_cpss_name(fb_base_cpss_name, failback_pid);
	siq_safe_btree_rename(siq_btree_cpools_sync_state, orig_pid,
	    orig_base_cpss_name, failback_pid, fb_base_cpss_name, &error);
	if (error)
		goto out;

	get_mirrored_lmap_name(orig_linmap_name, orig_pid);
	get_lmap_name(fb_linmap_name, failback_pid, false);

	siq_safe_btree_rename(siq_btree_linmap, orig_pid, orig_linmap_name,
	    failback_pid, fb_linmap_name, &error);
	if (error)
		goto out;

	if (compliance_v2) {
		/* Rename the source resync list to use the mirror policy id
		 * so that it will be processed upon the failback resync. */
		get_resync_name(orig_resync_name, sizeof(orig_resync_name),
		    orig_pid, true);
		get_resync_name(fb_resync_name, sizeof(fb_resync_name),
		    failback_pid, false);

		siq_safe_btree_rename(siq_btree_lin_list, orig_pid,
		    orig_resync_name, failback_pid, fb_resync_name, &error);
		if (error)
			goto out;

		get_compliance_map_name(orig_comp_map_name,
		    sizeof(orig_comp_map_name), orig_pid, true);
		get_compliance_map_name(fb_comp_map_name,
		    sizeof(fb_comp_map_name), failback_pid, false);

		siq_safe_btree_rename(siq_btree_compliance_map, orig_pid,
		    orig_comp_map_name, failback_pid, fb_comp_map_name, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
failback_prep_finalize_resp_msg_unpack(struct generic_msg *msg,
    uint32_t prot_ver, struct failback_prep_finalize_resp_msg *ret,
    struct isi_error **error_out)
{
	struct old_failback_prep_finalize_resp_msg *fpfr =
	    &msg->body.old_failback_prep_finalize_resp;
	struct siq_ript_msg *ript = &msg->body.ript;
	struct isi_error *error = NULL;

	if (prot_ver < MSG_VERSION_RIPTIDE) {
		ret->error = fpfr->error;
		ret->fbpol_id = fpfr->fbpol_id;
		ret->fbpol_name = fpfr->fbpol_name;
		ret->fbpol_src_cluster_name = fpfr->fbpol_src_cluster_name;
		ret->fbpol_src_cluster_id = fpfr->fbpol_src_cluster_id;
	} else {
		ript_msg_get_field_str(ript, RMF_ERROR, 1, &ret->error,
		    &error);
		if (error != NULL) {
			if (!isi_system_error_is_a(error, ENOENT))
				goto out;
			isi_error_free(error);
			error = NULL;
		} else {
			// The message contains an error so propagate it up.
			goto out;
		}

		RIPT_MSG_GET_FIELD(str, ript, RMF_FBPOL_ID, 1, &ret->fbpol_id,
		    &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_FBPOL_NAME, 1,
		    &ret->fbpol_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_FBPOL_SRC_CLUSTER_NAME, 1,
		    &ret->fbpol_src_cluster_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_FBPOL_SRC_CLUSTER_ID, 1,
		    &ret->fbpol_src_cluster_id, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

static int
on_target_finalize_resp(struct generic_msg *m, void *ctx)
{
	struct failback_prep_finalize_resp_msg fpfr = {};
	struct isi_error *error = NULL;
	char *fb_pid = NULL;

	g_ctx.waiting_for_tgt_msg = false;
	migr_force_process_stop();

	failback_prep_finalize_resp_msg_unpack(m, g_ctx.job_ver.common, &fpfr,
	    &error);
	if (error != NULL)
		goto out;

	if (fpfr.error != NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_NET, "%s: Error: %s",
		    __func__, fpfr.error);
		goto out;
	}

	fb_pid = fpfr.fbpol_id;

	associate_fbpol_id(fb_pid, &error);
	if (error)
		goto out;

	check_create_fbpol_trec(&fpfr, &error);
	if (error)
		goto out;

	log(NOTICE, "Failback policy \"%s\" available for failback syncs.",
	    fpfr.fbpol_name);

	do_failback_database_renames(g_ctx.rc.job_id, fb_pid,
	    g_ctx.rc.compliance_v2, &error);
	if (error)
		goto out;

	delete_prep_restore_trec(g_ctx.rc.job_id, &error);
	if (error)
		goto out;

	/* Snapshot Alias Exports:
	 *      move the target snap alias from HEAD to the latest good
	 *      snapshot, which is the mirror policy's source failover snap */
	move_snapshot_alias_from_head(g_ctx.rc.job_id, fb_pid, &error);
	if (error)
		goto out;

out:

	if (error) {
		fatal_error_from_isi_error(false, error);
		isi_error_free(error);
	}

	return 0;
}

static void
do_target_finalize(void)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	uint32_t mesg_type;

	if (g_ctx.job_ver.common < MSG_VERSION_RIPTIDE) {
		msg.head.type = OLD_FAILBACK_PREP_FINALIZE_MSG;

		// Skip if we are, or already have, a failback policy.
		if (!JOB_SPEC->common->mirror_pid) {
			msg.body.old_failback_prep_finalize.create_fbpol =
			    true;
			msg.body.old_failback_prep_finalize.fbpol_dst_path =
			    JOB_SPEC->datapath->root_path;
		}
		mesg_type = OLD_FAILBACK_PREP_FINALIZE_RESP_MSG;
	} else {
		msg.head.type =
		    build_ript_msg_type(FAILBACK_PREP_FINALIZE_MSG);
		ript_msg_init(&msg.body.ript);

		// Skip if we are, or already have, a failback policy.
		ript_msg_set_field_str(&msg.body.ript,
		    RMF_FBPOL_DST_PATH, 1,
		    JOB_SPEC->datapath->root_path);
		ript_msg_set_field_uint32(&msg.body.ript,
		    RMF_DEEP_COPY, 1, g_ctx.rc.deep_copy);
		mesg_type =
		    build_ript_msg_type(FAILBACK_PREP_FINALIZE_RESP_MSG);
	}

	if (send_tgt_msg_with_retry(&msg, mesg_type, on_target_finalize_resp)
	    != TGT_SEND_OK) {
		fatal_error(true, SIQ_ALERT_TARGET_CONNECTIVITY,
		    "Unable to finalize resync prep on target.");
	}
}

void
do_failback_prep_finalize(void)
{
	struct isi_error *error = NULL;

	if (g_ctx.rc.noop) {
		exit(EXIT_SUCCESS);
	}

	JOB_SUMM->retry = true; /* from now on we're running */
	save_report();

	log(DEBUG, "Starting failback prep finalize");

	setup_restrictions(JOB_SPEC->target->restrict_by,
	    JOB_SPEC->target->force_interface);

	test_connection();

	/*
	 * Get the remote cluster IPs, and make sure everything is in place
	 * on the target to run the policy, the linmap and read-only domain
	 */
	// XXXDPL HACK - Prevent any snapshot renaming/deleting at job end.
	g_ctx.restore = true;

	connect_to_tmonitor(&error);
	if (error) {
		/* record this node's connection failure so the scheduler(s)
		 * can tell if all viable nodes failed to connect. in that case
		 * the job should be killed immediately instead of waiting for
		 * it to timeout */
		siq_source_set_node_conn_failed(g_ctx.record,
		    get_local_node_id());
		log(NOTICE, "%s", isi_error_get_message(error));
		exit(EXIT_FAILURE);
	}

	if (!(g_ctx.curr_ver & FLAG_VER_3_5)) {
		fatal_error(false, SIQ_ALERT_VERSION,
		    "Target for %s does not support Failback.",
		    g_ctx.rc.job_name);
		exit(EXIT_FAILURE);
	}

	do_target_finalize();

	/* Begin job processing, for pruning and repeats. */
	g_ctx.start = time(0);
	g_ctx.conn_alert = time(0);

	post_primary_success_work();
	/* if a prior job set an alert, cancel it since we have
	 * succeeded (exclude rpo alerts) */
	siq_cancel_alerts_for_policy(g_ctx.rc.job_name, g_ctx.rc.job_id,
	    (enum siq_alert_type []) {SIQ_ALERT_RPO_EXCEEDED}, 1);

	exit(EXIT_SUCCESS);
}

/*
 * Cleanup remote repstate mirror and local linmap mirror
 * before we start with RESYNC phase. The RESYNC phase would
 * rebuilt the database.
 */
void
mirrored_repstate_cleanup()
{
	int i;
	struct generic_msg m = {};
	char lmap_name[MAXNAMELEN + 1];
	struct isi_error *error = NULL;

	log(TRACE, "mirrored_cleanup_complete");

	/*
	 * Cleanup remote repstate mirror
	 */
	g_ctx.waiting_for_tgt_msg = true;
	for (i = 0; i < MAX_TGT_TRIES && g_ctx.waiting_for_tgt_msg; i++) {
		migr_register_callback(g_ctx.tctx.sock, GENERIC_ACK_MSG,
		    task_complete_ack_callback);
		m.head.type = CLEANUP_DATABASE_MIRROR_MSG;
		m.body.cleanup_database_mirror.policy_id = g_ctx.rc.job_id;
		m.body.cleanup_database_mirror.flags = CLEANUP_REP;
		msg_send(g_ctx.tctx.sock, &m);
		if (migr_process() < 0) {
			set_job_state(SIQ_JS_FAILED);
			return;
		}
	}

	if (i == MAX_TGT_TRIES) {
		fatal_error(true, SIQ_ALERT_TARGET_FS,
		    "Unable to complete cleanup of mirrored repstate"
		    " at the target");
	} else {
		/* Cleanup local linmap mirror */
		get_mirrored_lmap_name(lmap_name, g_ctx.rc.job_id);
		remove_linmap(g_ctx.rc.job_id, lmap_name, true, &error);
		if (error) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    "Failed to mirrored linmap %s : %s", lmap_name,
			   isi_error_get_message(error));
			isi_error_free(error);
		}
	}
}

bool
allow_failback_snap_rename(struct coord_ctx *ctx)
{
	ifs_snapid_t new_snap;
	struct isi_error *error = NULL;


	/*
	 * We will do rename from new to latest snap only when we have
	 * executed the flip phase. If jtype field is NULL then we 
	 * did not perform any task, so we should not rename.
	 */
	if ((ctx->failback_action == SIQ_ACT_FAILBACK_PREP) && 
	    (ctx->job_phase.jtype == 0 ||
	    stf_job_phase_disabled(&ctx->job_phase, STF_PHASE_FLIP_LINS))) {
		/*
		 * Make sure we remove the new snapshot since it will
		 * not be required anymore. In this case new snapshot
		 * only served purpose to uniquely name repeated repstate
		 * which would have work checkpoints.
		 */
		if (ctx->record == NULL) {
			ctx->record = siq_source_record_load(ctx->rc.job_id,
			    &error);
			if (error) {
				fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
				    "Unable to load source record: %s",
				    isi_error_get_message(error));
			}
		}

		siq_source_get_new(ctx->record, &new_snap);
		if (new_snap != INVALID_SNAPID) {
			siq_source_set_new_snap(ctx->record, INVALID_SNAPID);
			siq_source_record_save(ctx->record, &error);
			if (error)
				fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
				    "Error clearing new snapshot: %s",
				    isi_error_get_message(error));

			delete_snapshot_helper(new_snap);
		}
	
		return false;

	} else {
		return true;
	}
}

/* Remove (if any) all the work items belonging to current executing phase */
void
remove_all_work_items(struct rep_ctx *rep, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_iter  *rep_iter = NULL;
	struct work_restart_state twork;
	ifs_lin_t wi_lin = 0;
	bool got_one;

	rep_iter = new_rep_iter(rep);
	ASSERT(rep_iter != NULL);
	while (1) {
		got_one = work_iter_next(rep_iter, &wi_lin, &twork, &error);
		if (error)
			goto out;

		if (!got_one)
			break;

		log(TRACE, "Found work item %llx during cleanup", wi_lin);
		remove_work_entry(wi_lin, &twork, rep, &error);
		if (error)
			goto out;
	}

out:
	close_rep_iter(rep_iter);
	isi_error_handle(error, error_out);
}

static void
add_selected_work_item(struct coord_ctx *ctx,
    struct stf_job_state *old_job_phase, struct stf_job_state *new_job_phase,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum work_restart_type rt;
	struct work_restart_state work;
	char *buf = NULL, *old_buf = NULL;
	size_t jsize = 0, old_jsize = 0 ;
	bool success = false;

	log(TRACE, "add_selected_work_item");

	if (old_job_phase) {
		old_buf = fill_job_state(old_job_phase, &old_jsize);
	}

	ctx->job_phase = *new_job_phase;
	record_phase_start(ctx->job_phase.cur_phase);
	rt = get_work_restart_type(&ctx->job_phase);
	allocate_entire_work(&work, rt);
	ctx->cur_work_lin = WORK_RES_RANGE_MIN;

	buf = fill_job_state(&ctx->job_phase, &jsize);

	success = update_job_phase(ctx->rep, STF_JOB_PHASE_LIN,
	    buf, jsize, old_buf, old_jsize, WORK_RES_RANGE_MIN, &work,
	    NULL, 0, (old_job_phase == NULL), &error);

	if (error)
		goto out;

	/* We cannot have somebody else updating */
	ASSERT(success == true);

out:
	if (buf)
		free(buf);

	if (old_buf)
		free(old_buf);
	isi_error_handle(error, error_out);
}

bool
check_or_repair_for_failback_prep(struct coord_ctx *ctx, bool found,
    struct isi_error **error_out)
{
	char *jbuf = NULL;
	size_t jsize;
	bool add_work_item = false;
	bool done = false;
	bool success;
	bool old_job = false;
	bool is_compliance_v2 = (ctx->rc.flags & FLAG_COMPLIANCE) &&
	    (ctx->rc.flags & FLAG_COMPLIANCE_V2);
	struct isi_error *error = NULL;
	struct stf_job_state old_job_phase = {}, new_job_phase = {};
	int jtype = 0;
	int phase_to_check = 0;

	ASSERT(ctx != NULL, "Coordinator context is NULL");
	jtype = (ctx->rc.compliance_v2) ? JOB_STF_LIN_COMP :
	    JOB_STF_LIN_V3_5;
	init_job_phase_ctx(jtype, &new_job_phase);
	disable_all_phases(&new_job_phase);

	/*
	 * If its initial sync or upgrade sync or non stf, then error out.
	 */
	if (ctx->initial_sync || ctx->stf_upgrade_sync || !ctx->stf_sync) {
		fatal_error(false, SIQ_ALERT_POLICY_FAIL,
		    "Failback prep cannot be done for a policy which has not"
		    " finished initial or upgrade sync.");
	}

	/*
	 * If we found active work items, then make sure they
	 * are in flip phase otherwise take corrective action.
	 */
	if (found) {
		jsize = job_phase_max_size();
		jbuf = malloc(jsize);
		memset(jbuf, 0, jsize);
		get_state_entry(STF_JOB_PHASE_LIN, jbuf, &jsize,
		    ctx->rep, &error);
		if (error)
			goto out;
		read_job_state(&old_job_phase, jbuf, jsize);

		/*
		 * If the current running job is pre 3.5, then 
		 * fail 
		 */
		if (old_job_phase.jtype == JOB_STF_LIN) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    "Failback prep cannot be done for the policy"
			    " since the policy has pre upgrade sync job"
			    " running");
		}

		/*
		 * If its already in flip phase (or comp commit),
		 * then we are fine.
		 * Else remove all the work items from the phase.
		 * Then move to database correct phase. If the 
		 * mirroring is required, then we would have 
		 * already set the phase above.
		 */
		phase_to_check = is_compliance_v2 ?
		    STF_PHASE_COMP_COMMIT : STF_PHASE_FLIP_LINS;
		if (is_phase_before(&old_job_phase, old_job_phase.cur_phase,
		    phase_to_check)) {
			remove_all_work_items(ctx->rep, &error);
			if (error)
				goto out;
			stf_job_phase_enable(&new_job_phase,
			    STF_PHASE_DATABASE_CORRECT);
			if (!ctx->rc.database_mirrored) {
				stf_job_phase_enable(&new_job_phase,
				    STF_PHASE_DATABASE_RESYNC);
			}
			new_job_phase.cur_phase = STF_PHASE_DATABASE_CORRECT;
			old_job = true;
			add_work_item = true;
		} else {
			/*
			 * There are valid work items to be processed.
			 * Make sure we enabled resync if its not already
			 * set in current state diagram.
			 */
			if (!ctx->rc.database_mirrored) {
				do {
					success = stf_job_phase_cond_enable(
					    STF_JOB_PHASE_LIN,
					    STF_PHASE_DATABASE_RESYNC,
					    ctx->rep, &error);
					if (error)
						goto out;
				} while (!success);
			}

			/*
			 * This could be enabled twice if !done but it doesn't
			 * matter.
			 */
			if (is_compliance_v2) {
				stf_job_phase_enable(&new_job_phase,
				    STF_PHASE_COMP_MAP_PROCESS);
			}

			done = load_recovery_items(&error);
			if (error)
				goto out;

			UFAIL_POINT_CODE(load_recover_items_done,
			    done = false;
			);

			if (done)
				goto out;
		}
	} else {
		/* If there are no phases, we are done */
		if (ctx->rc.database_mirrored) {
			done = true;
		} else {
			stf_job_phase_enable(&new_job_phase,
				STF_PHASE_DATABASE_RESYNC);
			new_job_phase.cur_phase = STF_PHASE_DATABASE_RESYNC;
			add_work_item = true;

			/*
			 * Cleanup the mirrored database before starting
			 * the Database resync.
			 */
			log(NOTICE, "Cleanup mirrored database before job "
			    "phase %s ", job_phase_str(&new_job_phase));
			mirrored_repstate_cleanup();
		}
	}

	if (is_compliance_v2) {
		stf_job_phase_enable(&new_job_phase,
		    STF_PHASE_COMP_MAP_PROCESS);
		if (done) {
			add_work_item = true;
			done = false;
		}
	}

	if (add_work_item) {
		add_selected_work_item(ctx, old_job ? &old_job_phase : NULL,
		    &new_job_phase, &error);
		if (error)
			goto out;
	}

out:
	if (jbuf)
		free(jbuf);
	isi_error_handle(error, error_out);
	return done;
}

static void
dom_mark_clear_old_work_items(struct isi_error **error_out)
{	
	char *jbuf = NULL;
	size_t jsize = 0;
	struct stf_job_state job_phase = {};
	struct isi_error *error = NULL;

	if (!entry_exists(STF_JOB_PHASE_LIN, g_ctx.rep, &error) || error) {
		goto out;
	}

	jsize = job_phase_max_size();
	jbuf = calloc(1, jsize);
	get_state_entry(STF_JOB_PHASE_LIN, jbuf, &jsize, g_ctx.rep, &error);
	if (error)
		goto out;
	read_job_state(&job_phase, jbuf, jsize);

	ASSERT(job_phase.jtype == JOB_TW_V3_5 || job_phase.jtype == JOB_TW ||
	    job_phase.jtype == JOB_TW_COMP);

	if (job_phase.cur_phase != STF_PHASE_DOMAIN_MARK) {
		log(NOTICE, "Domain mark: Clearing previous job state/work "
		    "items.");
		remove_all_work_items(g_ctx.rep, &error);
		if (error)
			goto out;

		remove_entry(STF_JOB_PHASE_LIN, g_ctx.rep, &error);
		if (error)
			goto out;
	}
	
out:

	if (jbuf)
		free(jbuf);
	isi_error_handle(error, error_out);
}

static void
dom_mark_cleanup_base_repstate(struct isi_error **error_out)
{
	char name[MAXNAMELEN];
	struct isi_error *error = NULL;
	struct rep_ctx *rep = NULL;

	get_base_rep_name(name, g_ctx.rc.job_id, false);
	open_repstate(g_ctx.rc.job_id, name, &rep, &error);
	if (error)
		goto out;

	remove_all_work_items(rep, &error);
	if (error)
		goto out;

	remove_entry(STF_JOB_PHASE_LIN, rep, &error);
	if (error)
		goto out;

out:
	close_repstate(rep);
	isi_error_handle(error, error_out);
}

void
do_domain_treewalk()
{
	struct isi_error *error = NULL;
	bool success = false;
	bool tw_done = false;
	char rep_name[MAXNAMLEN];
	int rc = -1;

	srandom(getpid());
	srandom(random() * time(0) * getpid());
	srandom(random() * random() * time(0) * getpid());
	JOB_SUMM->retry = true; /* from now on we're running */
	save_report();

	setup_restrictions(JOB_SPEC->target->restrict_by, 
	    JOB_SPEC->target->force_interface);

	/* Before we get too far, check connection with sworker, and make sure
	 * we can actually connect with POL_DENY_FALLBACK. If the connection
	 * fails, we either have a handshake negotiation failure, or a general
	 * connectivity issue. If handshake failure, it's fatal. If
	 * connectivity issue, then try again a few times below in call to
	 * initiate_target_cluster */
	test_connection();
	
	/* initialize source and target base paths */
	path_init(&g_ctx.source_base, g_ctx.rc.path, NULL);
	path_init(&g_ctx.target_base, g_ctx.rc.targetpath, NULL);

	// Use dummy snap info for domain mark.
	g_ctx.curr_snap.id = HEAD_SNAPID;
	g_ctx.curr_snap.root_fd = ifs_lin_open(ROOT_LIN, HEAD_SNAPID,
	    O_RDONLY);
	if (g_ctx.curr_snap.root_fd == -1) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Unable to open root lin for HEAD!");
	}

	/* Process includes/excludes. */
	process_paths(global_config->root, &g_ctx.rc, HEAD_SNAPID);

	/* Verify that the repstate(s) we expect to exists are present, and
	 * that we succeed in creating the new repstates for this run. If this
	 * is a restart, the new repstates may already exist */
	get_base_rep_name(rep_name, g_ctx.rc.job_id, g_ctx.restore);
	open_repstate(g_ctx.rc.job_id, rep_name, &g_ctx.rep, &error);
	if (error)
		goto out;

	dom_mark_clear_old_work_items(&error);
	if (error)
		fatal_error_from_isi_error(false, error);

	/* 
	 * Get the remote cluster IPs, and make sure everything is in place
	 * on the target to run the policy, the linmap and read-only domain
	 */
	connect_to_tmonitor(&error);
	if (error) {
		/* record this node's connection failure so the scheduler(s)
		 * can tell if all viable nodes failed to connect. in that case
		 * the job should be killed immediately instead of waiting for
		 * it to timeout */
		siq_source_set_node_conn_failed(g_ctx.record,
		    get_local_node_id());
		log(NOTICE, "%s", isi_error_get_message(error));
		exit(EXIT_FAILURE);
	}
#if 0
	/*
	 * If it is local sync job, try creating "running" folder 
	 * which will have policy_id/timestamp to detect multiple jobs for
	 * same slice
	 */
	if (strcmp(g_ctx.rc.target, "localhost") == 0 ||
	    strcmp(g_ctx.rc.target, "127.0.0.1") == 0) {
		create_syncrun_file(g_ctx.rc.job_id, g_ctx.run_id, &error);
		if (error)
			fatal_error_from_isi_error(false, error);
	}
#endif

	migr_register_group_callback(group_callback, NULL);

	/* Begin job processing, for pruning and repeats. */
	g_ctx.start = time(0);

	log(DEBUG, "Starting domain mark treewalk job.");

	g_ctx.conn_alert = time(0);

	do_domain_setup(&tw_done, &error);

	UFAIL_POINT_CODE(fail_domain_setup,
	    if (!error) {
		    error = isi_system_error_new(EINVAL,
		        "Domain setup error: UFP");
	    }
	);

	if (error)
		fatal_error_from_isi_error(false, error);

	if (tw_done) {
		log(NOTICE, "Domain marking already done for %s, skipping.",
		    JOB_SPEC->datapath->root_path);
		success = true;
		goto out;
	}

	/* Get a list of local cluster node IPs */
	get_source_ip_list();

	/* At this point we have a list of source and target cluster node ips.
	 * Allocate and distribute pworkers, pair them with sworkers, etc.
	 * This arrangement may change dynamically if the source or target
	 * cluster experiences a group change during the sync. */
	configure_workers(true);

	/* Reallocate workers after configuring since the worker callback may
	 * occur prior to configure_workers(). */
	reallocate_worker();

	/*
	 * If its new sync, then create first work item,
	 * otherwise load the existing recovery state.
	 */

	success = load_or_create_recovery_state(&g_ctx, &error);
	if (error)
		fatal_error_from_isi_error(false, error);
	if (success)
		goto out;

	/* connect to bandwidth and throttle hosts, and start sending stats */
	connect_worker_host();
	connect_bandwidth_host();
	connect_throttle_host();
	migr_register_timeout(&bwt_timeout, bwt_update_timer_callback, 0);

	/* register redistribute timeout */
	register_redistribute_work();

	ifs_setnoatime(true); /* Ignore errors, if any */

	/* initiate jobctl timeouts */
	jobctl_callback(NULL);

	if (migr_process() < 0) {
		set_job_state(SIQ_JS_FAILED);
	} else {
		UFAIL_POINT_CODE(do_domain_treewalk_zero_domain_id,
		    g_ctx.rc.domain_id = 0;);
		if (g_ctx.failback_action == SIQ_ACT_WORM_DOMAIN_MARK) {
			rc = worm_finish_setup(JOB_SPEC->datapath->root_path,
			    g_ctx.rc.domain_id);
		} else
			rc = dom_mark_ready(g_ctx.rc.domain_id);

		if (rc == -1) {
			error = isi_system_error_new(errno,
			    "Could not domain mark %s",
			    JOB_SPEC->datapath->root_path);
			goto out;
		}
		success = true;
	}

 out:
	if (error) {
		set_job_state(SIQ_JS_FAILED);
		log(ERROR, "%s", isi_error_get_message(error));
	}
	if (success) {
		set_job_state(SIQ_JS_SUCCESS);
		disconnect_from_tmonitor();
		dom_mark_cleanup_base_repstate(&error);
		if (error)
			log(ERROR, "%s", isi_error_get_message(error));
		/* if a prior job set an alert, cancel it since we have
		 * succeeded (exclude rpo alerts) */
		siq_cancel_alerts_for_policy(g_ctx.rc.job_name,
		    g_ctx.rc.job_id,
		    (enum siq_alert_type []) {SIQ_ALERT_RPO_EXCEEDED}, 1);
	}

	exit(success ? EXIT_SUCCESS : EXIT_FAILURE);
}
