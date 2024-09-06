#include <isi_util/isi_guid.h>
#include <isi_ufp/isi_ufp.h>

#include <isi_snapshot/snapshot.h>

#include <isi_domain/dom.h>
#include <isi_domain/worm.h>
#include <isi_domain/dom_util.h>

#include <isi_migrate/config/siq_source.h>
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/linmap.h"
#include "sworker.h"

#define CANCEL_SCAN_INTERVAL_SEC  5

static struct timeval cancel_scan_interval = {CANCEL_SCAN_INTERVAL_SEC, 0};

static void
tmonitor_cleanup(struct migr_tmonitor_ctx *tm_ctx)
{
	struct isi_error *error = NULL;

	if (tm_ctx->cleaned_up)
		return;
	
	if (tm_ctx->cancel_state == CANCEL_ACKED) {
		log(NOTICE, "Target cancel completed");
	}

	// If we requested a cancel, but are now cleaning up,
	// set it back to CANCEL_REQUESTED so we try again upon next connection
	if (tm_ctx->cancel_state == CANCEL_PENDING ||
	    tm_ctx->cancel_state == CANCEL_REQUESTED) {
		tm_ctx->cancel_state = CANCEL_REQUESTED;
	} else {
		tm_ctx->cancel_state = CANCEL_DONE;
	}

	siq_target_states_set(tm_ctx->policy_id,
	    CHANGE_JOB_STATE | CHANGE_CANCEL_STATE, tm_ctx->job_state,
	    tm_ctx->cancel_state, false, tm_ctx->restore, &error);
	if (error) {
		log(ERROR, "Error setting job states: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}
	tm_ctx->cleaned_up = true;
}

#define target_monitor_shutdown(tm_ctx, msg) \
    _target_monitor_shutdown(__func__, tm_ctx, msg)

static void
_target_monitor_shutdown(const char *origin, struct migr_tmonitor_ctx *tm_ctx,
    const char *msg)
{
	struct generic_msg m = {};

	log(ERROR, "From %s: Target monitor for %s (%s) shutting down: %s",
	    origin, tm_ctx->policy_name, tm_ctx->policy_id, msg);

	m.head.type = TARGET_MONITOR_SHUTDOWN_MSG;
	// Have to unconst msg because msg_send can't take a const parameter
	m.body.target_monitor_shutdown.msg = (char *)msg;
	msg_send(tm_ctx->coord_fd, &m);

	tmonitor_cleanup(tm_ctx);
	exit(-1);
}

static int
tm_disconnect_callback(struct generic_msg *m, void *ctx)
{
	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;
	struct siq_policies *policies = NULL;
	struct siq_policy *mirror_pol = NULL;
	struct isi_error *error = NULL;

	log(INFO, "Coord disconnects from target_monitor for %s",
	    tm_ctx->policy_name);
	if (tm_ctx->discon_enable_mirror) {
		policies = siq_policies_load(&error);
		if (error)
			goto out;

		mirror_pol = siq_policy_get_by_mirror_pid(policies,
		    tm_ctx->policy_id);

		if (!mirror_pol)
			goto out;

		mirror_pol->common->state = SIQ_ST_ON;
		siq_policies_save(policies, &error);
		if (error)
			goto out;
	}
out:
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		isi_error_free(error);
	}
	if (policies)
		siq_policies_free(policies);

	tmonitor_cleanup(tm_ctx);
	exit(EXIT_SUCCESS);
}

static int
cleanup_sync_run_callback(struct generic_msg *m, void *ctx)
{
	struct cleanup_sync_run_msg *cleanup =  &m->body.cleanup_sync_run;
	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;

	ASSERT(strcmp(tm_ctx->policy_id, cleanup->policy_id) == 0);
	/*
	 * No need to cleanup here, it adds unnecessary contention
	 * among workers (when there lot of jobs)to cleanup the file 
	 * in same running directory.
	 * A better approach is to cleanup when deleting policy.
	 */
	//remove_syncrun_file(cleanup->policy_id);

	return 0;
}

static int
cleanup_target_tmp_files_callback(struct generic_msg *m, void *ctx)
{
	char comp_map_name[MAXNAMLEN];
	struct isi_error *error = NULL;
	struct isi_error *tmp_rmdir_error = NULL;
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct migr_tmonitor_ctx *tm_ctx = &sw_ctx->tmonitor;
	struct generic_msg ack = {};

	UFAIL_POINT_CODE(cleanup_target_tmp_files_callback_error,
	    error = isi_system_error_new(RETURN_VALUE, "%s ufp error",
	        __func__);
	    goto out;
	);

	struct cleanup_target_tmp_files_msg *cleanup =
	    &m->body.cleanup_target_tmp_files;

	ASSERT(strcmp(tm_ctx->policy_id, cleanup->policy_id) == 0);

	/* We have successfully processed the target compliance map and can
	 * remove it.*/
	if (tm_ctx->failback_prep && tm_ctx->stf_sync && !tm_ctx->restore) {
		get_compliance_map_name(comp_map_name, sizeof(comp_map_name),
		    tm_ctx->policy_id, false);
		remove_compliance_map(tm_ctx->policy_id, comp_map_name, true,
		    &error);
		if (error)
			goto out;
	}

	/* If we are doing comp commit finish, we need to delete both */
	if (tm_ctx->doing_comp_commit_finish) {
		cleanup_tmp_dirs(cleanup->tgt_path, !tm_ctx->restore,
		    sw_ctx, &error);
		if (error)
		    goto out;
	}

	cleanup_tmp_dirs(cleanup->tgt_path, tm_ctx->restore,
	    sw_ctx, &error);
	if (error)
	    goto out;

	ack.head.type = GENERIC_ACK_MSG;
	msg_send(sw_ctx->tmonitor.coord_fd, &ack);

out:
	if (error) {
		tmp_rmdir_error = isi_siq_error_new(E_SIQ_TMP_RMDIR,
		    "Unable to cleanup tmp working directory, error is %s",
		    isi_error_get_message(error));
		siq_send_error(tm_ctx->coord_fd,
		    (struct isi_siq_error *) tmp_rmdir_error, EL_SIQ_DEST);
		isi_error_free(error);
		isi_error_free(tmp_rmdir_error);
	}

	return 0;
}

static int
verify_failover_callback(struct generic_msg *m, void *ctx)
{
	int lock_fd = -1;
	
	struct target_record *trec = NULL;

	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;

	struct generic_msg resp;

	struct isi_error *error = NULL;

	resp.head.type = VERIFY_FAILOVER_RESP_MSG;
	resp.body.verify_failover_resp.failover_done = false;

	lock_fd = siq_target_acquire_lock(tm_ctx->policy_id, O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(tm_ctx->policy_id, &error);
	if (error)
		goto out;

	if (trec->fofb_state < FOFB_FAILOVER_DONE) 
		goto out;


	trec->fofb_state = FOFB_FAILBACK_PREP_STARTED;
	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	resp.body.verify_failover_resp.failover_done = true;

out:
	if (-1 != lock_fd)
		close(lock_fd);

	siq_target_record_free(trec);

	isi_error_free(error);

	// Always send back something to the coord.
	msg_send(tm_ctx->coord_fd, &resp);

	return 0;
}

static struct siq_policy *
create_failback_policy(struct siq_policies *policies,
    struct target_record *trec, struct migr_tmonitor_ctx *tm_ctx,
    const char *dst_path, struct isi_error **error_out)
{
	struct siq_policy *fbpol = NULL;
	struct siq_gc_conf *conf = NULL;
	struct isi_error *error = NULL;

	fbpol = siq_policy_create();

	if (!fbpol) {
		error = isi_system_error_new(ENOMEM,
		    "Could not allocate policy.");
		goto out;
	}

	conf = siq_gc_conf_load(&error);
	if (error)
		goto out;

	fbpol->common->name =
	    siq_generate_failback_policy_name(tm_ctx->policy_name,
		policies);

	fbpol->common->pid = siq_create_policy_id(true);
	fbpol->common->action = SIQ_ACT_SYNC;
	fbpol->common->state = SIQ_ST_OFF;
	fbpol->common->mirror_state = SIQ_MIRROR_OFF;
	fbpol->datapath->root_path = strdup(trec->target_dir);
	fbpol->target->path = strdup(dst_path);
	fbpol->common->mirror_pid = strdup(tm_ctx->policy_id);
	fbpol->target->dst_cluster_name =
	    strdup(trec->last_src_coord_ip);
	fbpol->coordinator->dst_snapshot_mode = SIQ_SNAP_NONE;
	tm_ctx->discon_enable_mirror = true;

	if (conf->root->coordinator->default_restrict_by != NULL) {
		fbpol->target->restrict_by =
			strdup(conf->root->coordinator->default_restrict_by);
	}

	if (conf->root->coordinator->default_target_restrict != 0) {
		fbpol->target->target_restrict =
			conf->root->coordinator->default_target_restrict;
	}

	if (conf->root->coordinator->default_force_interface != 0) {
		fbpol->target->force_interface =
			conf->root->coordinator->default_force_interface;
	}

	siq_policy_add(policies, fbpol, &error);
	if (error)
		goto out;

	siq_policies_save(policies, &error);
	if (error) {
		/* XXX Temporary assert to catch 91587 XXX */
		ASSERT(0);
		goto out;
	}

	fbpol = siq_policy_get_by_mirror_pid(policies,
	    tm_ctx->policy_id);

	ASSERT(fbpol);
	log(NOTICE, "Created failback policy: %s", fbpol->common->pid);
out:
	isi_error_handle(error, error_out);
	return fbpol;
}

static void
do_failback_database_renames(const char *orig_pid, const char *failback_pid,
    bool compliance_v2, struct isi_error **error_out)
{
	char orig_base_rep_name[MAXNAMLEN];
	char fb_base_rep_name[MAXNAMLEN];
	char orig_base_cpss_name[MAXNAMLEN];
	char fb_base_cpss_name[MAXNAMLEN];
	char orig_linmap_name[MAXNAMLEN];
	char fb_linmap_name[MAXNAMLEN];
	char orig_resync_name[MAXNAMLEN];
	char fb_resync_name[MAXNAMLEN];
	char orig_dir_dels_name[MAXNAMELEN];

	struct isi_error *error = NULL;

	get_mirrored_rep_name(orig_base_rep_name, orig_pid);
	get_base_rep_name(fb_base_rep_name, failback_pid, false);

	siq_safe_btree_rename(siq_btree_repstate, orig_pid, orig_base_rep_name,
	    failback_pid, fb_base_rep_name, &error);
	if (error)
		goto out;

	get_mirrored_cpss_name(orig_base_cpss_name, orig_pid);
	get_base_cpss_name(fb_base_cpss_name, failback_pid, false);

	siq_safe_btree_rename(siq_btree_cpools_sync_state, orig_pid,
	    orig_base_cpss_name, failback_pid, fb_base_cpss_name, &error);
	if (error)
		goto out;

	get_lmap_name(orig_linmap_name, orig_pid, false);
	get_mirrored_lmap_name(fb_linmap_name, failback_pid);

	siq_safe_btree_rename(siq_btree_linmap, orig_pid, orig_linmap_name,
	    failback_pid, fb_linmap_name, &error);
	if (error)
		goto out;

	if (compliance_v2) {
		/* Rename the target resync list to use the mirror policy id
		 * so that it will be processed upon the failback resync. */
		get_resync_name(orig_resync_name, sizeof(orig_resync_name),
		    orig_pid, false);
		get_resync_name(fb_resync_name, sizeof(fb_resync_name),
		    failback_pid, true);

		siq_safe_btree_rename(siq_btree_lin_list, orig_pid,
		    orig_resync_name, failback_pid, fb_resync_name, &error);
		if (error)
			goto out;

		/* We do not need to rename <pid>_comp_map_dst ->
		 * <mpid>_comp_map_src since the former does not exist on this
		 * cluster. */

		/* We need to delete the dir_dels lin list since it no longer makes sense to keep */
		remove_lin_list(orig_pid, LIN_LIST_NAME_DIR_DELS, true, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static bool
verify_linmap_for_failback(struct migr_tmonitor_ctx *tm_ctx, 
    struct isi_error **error_out)
{
	char orig_linmap_name[MAXNAMLEN];
	char fb_linmap_name[MAXNAMLEN];

	struct siq_policies *policies = NULL;
	struct siq_policy *fbpol = NULL;

	bool exists = false;
	bool res = false;

	struct isi_error *error = NULL;

	policies = siq_policies_load_nolock(&error);
	if (error)
		goto out;
	
	fbpol = siq_policy_get_by_mirror_pid(policies, tm_ctx->policy_id);

	get_lmap_name(orig_linmap_name, tm_ctx->policy_id, false);
	exists = linmap_exists(tm_ctx->policy_id, orig_linmap_name, &error);
	if (error) 
		goto out;

	if (exists) {
		// Original linmap exists. All set.
		res = true;
		goto out;
	} else if (!fbpol) { // No orig linmap, and no failback policy.
		goto out;
	}

	get_mirrored_lmap_name(fb_linmap_name, fbpol->common->pid);

	exists = linmap_exists(fbpol->common->pid, fb_linmap_name, &error);
	if (error)
		goto out;

	res = exists; 
	
	
out:
	siq_policies_free(policies);

	isi_error_handle(error, error_out);

	return res;
}

static void
failback_policy_setup_snapshots(struct siq_source_record *srec,
    const char *fbpol_id, ifs_snapid_t fb_latest_snap,
    struct isi_error **error_out)
{
	ifs_snapid_t old_latest_snap = INVALID_SNAPID;
	ifs_snapid_t new_snap = INVALID_SNAPID;

	SNAP *snap = NULL;
	struct isi_str *old_snapname = NULL;
	struct isi_str new_snapname;
	struct fmt FMT_INIT_CLEAN(fmt);

	struct isi_error *error = NULL;

	UFAIL_POINT_CODE(failback_delete_latest_snap,
		/*
		 * Bug 197003
		 * This failpoint repros the bug condition.
		 * It will make the mirror policy unable to run
		 * by deleting the latest snapshot.
		 */
		siq_source_get_latest(srec, &old_latest_snap);
		delete_snapshot_helper(old_latest_snap);
		siq_source_detach_latest_snap(srec);

		siq_source_set_latest(srec, fb_latest_snap);
		error = isi_system_error_new(RETURN_VALUE,
		    "failback_delete_latest_snap");
		goto out;
	);

	snap = snapshot_open_by_sid(fb_latest_snap, NULL);
	if (snap) {
		fmt_print(&fmt, "SIQ-%s-latest", fbpol_id);
		isi_str_init(&new_snapname, __DECONST(char *, fmt_string(&fmt)),
		    fmt_length(&fmt) + 1, ENC_UTF8, ISI_STR_NO_MALLOC);

		old_snapname = snapshot_get_name(snap);
		snapshot_close(snap);
		snap = NULL;

		if (!old_snapname) {
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, 
			    "Latest snapshot does not exist.");
			goto out;
		}

		if (isi_str_compare(old_snapname, &new_snapname)) {
			/*
			 * Bug 197003
			 * Only delete the latest snapshot if the failover
			 * snapshot exists
			 */
			siq_source_get_latest(srec, &old_latest_snap);
			delete_snapshot_helper(old_latest_snap);
			siq_source_detach_latest_snap(srec);
			siq_source_set_latest(srec, fb_latest_snap);

			UFAIL_POINT_CODE(fail_post_delete_latest_snap,
				error = isi_system_error_new(RETURN_VALUE,
				    "fail_post_delete_latest_snap");
				goto out;
			);
			snapshot_rename(old_snapname, &new_snapname, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to rename latest snapshot: %s",
				    ISI_STR_GET_STR(old_snapname));
				goto out;
			} else {
				log(NOTICE, "Renamed snapshot '%s' to '%s'",
				    ISI_STR_GET_STR(old_snapname),
				    ISI_STR_GET_STR(&new_snapname));
			}			    
		}
	}
	siq_source_get_new(srec, &new_snap);
	if (new_snap != INVALID_SNAPID) 
		delete_snapshot_helper(new_snap);

	siq_source_set_new_snap(srec, INVALID_SNAPID);
		
out:
	if (old_snapname)
		isi_str_free(old_snapname);

	isi_error_handle(error, error_out);
}

static void
update_rep_info(char *pol_id, ifs_snapid_t snap, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN];
	struct rep_ctx *rep = NULL;
	struct rep_info_entry rie;

	log(TRACE, "update_rep_info");

	get_base_rep_name(rep_name, pol_id, false);

	open_repstate(pol_id, rep_name, &rep, &error);
	if (error)
		goto out;

	/*
	 * Assign the latest target snap to rep info.
	 * Also we dont support failback for copy policies.
	 */
	rie.initial_sync_snap = snap;
	rie.is_sync_mode = true;

	set_rep_info(&rie, rep, true, &error);
	if (error)
		goto out;

out:
	if (rep)
		close_repstate(rep);

	isi_error_handle(error, error_out);
}

static void
failback_prep_finalize_msg_unpack(struct generic_msg *m,
    unsigned source_msg_version, struct failback_prep_finalize_msg *ret,
    struct isi_error **error_out)
{
	struct old_failback_prep_finalize_msg *fpf =
	    &m->body.old_failback_prep_finalize;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (source_msg_version < MSG_VERSION_RIPTIDE) {
		ret->create_fbpol = fpf->create_fbpol;
		ret->fbpol_dst_path = fpf->fbpol_dst_path;
	} else {
		RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_FBPOL_DST_PATH, 1,
		    &ret->fbpol_dst_path, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint32, ript, RMF_DEEP_COPY, 1,
		    &ret->deep_copy, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

static int
failback_prep_finalize_callback(struct generic_msg *m, void *ctx)
{
	struct failback_prep_finalize_msg msg = {};

	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_tmonitor_ctx *tm_ctx = &sw_ctx->tmonitor;

	struct generic_msg GEN_MSG_INIT_CLEAN(resp);

	int lock_fd = -1;
	struct target_record *trec = NULL; 

	struct siq_policies *policies = NULL;
	struct siq_policy *fbpol = NULL;
	struct siq_policy *origpol = NULL;
	ifs_snapid_t restore_snap = INVALID_SNAPID;

	struct siq_source_record *srec = NULL;	
	struct siq_source_record *orig_srec = NULL;	

	struct isi_error *error = NULL;

	failback_prep_finalize_msg_unpack(m, sw_ctx->global.job_ver.common,
	    &msg, &error);
	if (error != NULL)
		goto out;

	lock_fd = siq_target_acquire_lock(tm_ctx->policy_id, O_EXLOCK, &error);
	if (error)
		goto out;

	if (siq_target_record_exists(tm_ctx->policy_id)) {
		trec = siq_target_record_read(tm_ctx->policy_id, &error);
		if (error)
			goto out;
	} else {
		error = isi_siq_error_new(E_SIQ_TGT_ASSOC_BRK, 
		    "No target record found for %s", tm_ctx->policy_name);
		goto out;
	}

	policies = siq_policies_load(&error);
	if (error)
		goto out;

	fbpol = siq_policy_get_by_mirror_pid(policies, tm_ctx->policy_id);

	if (!fbpol) {
		fbpol = create_failback_policy(policies, trec,
		    tm_ctx, msg.fbpol_dst_path, &error);
		if (error)
			goto out;
	} else {
		/*
		 * Make sure we reset the mirror state since this policy
		 * is now active.
		 */
		fbpol->common->mirror_state = SIQ_MIRROR_OFF;
		fbpol->common->state = SIQ_ST_OFF;
		tm_ctx->discon_enable_mirror = true;
	}

	/* Only update the deep copy setting if the source is at least
	 * Riptide and knows about deep copy.*/
	if (sw_ctx->global.job_ver.common >= MSG_VERSION_RIPTIDE)
		fbpol->coordinator->deep_copy = msg.deep_copy;

	siq_policies_save(policies, &error);
	if (error) {
		/* XXX Temporary assert to catch 91587 XXX */
		ASSERT(0);
		goto out;
	}
	fbpol = siq_policy_get_by_mirror_pid(policies,
	    tm_ctx->policy_id);
	ASSERT(fbpol);

	if (!siq_source_has_record(fbpol->common->pid)) {
		siq_source_create(fbpol->common->pid, &error);
		if (error)
			goto out;
	}

	srec = siq_source_record_load(fbpol->common->pid, &error);
	if (error)
		goto out;
	
	siq_source_set_pol_stf_ready(srec);
	/* By this time database is already mirrored. */
	siq_source_set_database_mirrored(srec);

	failback_policy_setup_snapshots(srec, fbpol->common->pid,
	    trec->latest_snap, &error);

	origpol = siq_policy_get_by_pid(policies, tm_ctx->policy_id);
	if (!origpol && siq_source_has_record(tm_ctx->policy_id)) {
		/*
		 * We know that failover has happened at target and
		 * we need to cleanup temp restore snapshot created
		 */
		orig_srec = siq_source_record_load(tm_ctx->policy_id, &error);
		if (error)
			goto out;

		siq_source_get_restore_latest(orig_srec, &restore_snap);
		if (restore_snap != INVALID_SNAPID) {
			if (!delete_snapshot_helper(restore_snap)) {
				log(ERROR, "Unable to delete failover"
				    " restore snap %llu for policy"
				    "%s", restore_snap, tm_ctx->policy_id);
			}
		}
		siq_source_record_free(orig_srec);
		orig_srec = NULL;
		siq_source_delete(tm_ctx->policy_id, &error);
		if (error)
			goto out;
	}

	/* Adding target_guid to source_record.xml
	 * for resync prep srm workflows. Populates the target_guid
	 * in source_record.xml on resync mirror policy creation
	 * without having to start sync from the target site.
	 * Tracked with Bug 102116
	 */
	siq_source_set_target_guid(srec, trec->src_cluster_id);

	siq_source_record_save(srec, &error);
	if (error)
		goto out;

	/* Type must be set for the coord to understand a resp or err resp */
	if (sw_ctx->global.job_ver.common < MSG_VERSION_RIPTIDE) {
		resp.head.type = OLD_FAILBACK_PREP_FINALIZE_RESP_MSG;

		// XXXDPL In the case that a failback pol DOES exist, should we
		// reset the basic settings (above) in it or leave the pol
		// alone?

		resp.body.old_failback_prep_finalize_resp.fbpol_id =
		    fbpol->common->pid;
		resp.body.old_failback_prep_finalize_resp.fbpol_name =
		    fbpol->common->name;
		resp.body.old_failback_prep_finalize_resp.fbpol_src_cluster_name
		    = sw_ctx->global.cluster_name;
		resp.body.old_failback_prep_finalize_resp.fbpol_src_cluster_id
		    = get_cluster_guid();
	} else {
		resp.head.type =
		    build_ript_msg_type(FAILBACK_PREP_FINALIZE_RESP_MSG);
		ript_msg_init(&resp.body.ript);

		ript_msg_set_field_str(&resp.body.ript, RMF_FBPOL_ID, 1,
		    fbpol->common->pid);
		ript_msg_set_field_str(&resp.body.ript, RMF_FBPOL_NAME, 1,
		    fbpol->common->name);
		ript_msg_set_field_str(&resp.body.ript,
		    RMF_FBPOL_SRC_CLUSTER_NAME, 1,
		    sw_ctx->global.cluster_name);
		ript_msg_set_field_str(&resp.body.ript,
		    RMF_FBPOL_SRC_CLUSTER_ID, 1, get_cluster_guid());
	}

	do_failback_database_renames(tm_ctx->policy_id, fbpol->common->pid,
	    sw_ctx->global.compliance_v2, &error);
	if (error)
		goto out;

	/*
	 * Make sure that we update rep info structure to latest snap.
	 */
	update_rep_info(fbpol->common->pid, trec->latest_snap, &error);
	if (error)
		goto out;

	trec->fofb_state = FOFB_FAILBACK_PREP_DONE;
	siq_target_record_write(trec, &error);
	if (error)
		goto out;

out:
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));

		//XXXDPL HACK - Breaks const correctness.
		if (sw_ctx->global.job_ver.common < MSG_VERSION_RIPTIDE) {
			resp.body.old_failback_prep_finalize_resp.error =
			    (char *)isi_error_get_message(error);
		} else {
			ript_msg_set_field_str(&resp.body.ript, RMF_ERROR, 1,
			    (char *)isi_error_get_message(error));
		}
	}

	msg_send(tm_ctx->coord_fd, &resp);

	if (lock_fd != -1)
		close(lock_fd);

	siq_target_record_free(trec);

	if (policies)
		siq_policies_free(policies);

	siq_source_record_free(srec);

	isi_error_free(error);

	return 0;
}

static int
ack_callback(struct generic_msg *m, void *ctx)
{
	struct generic_ack_msg *ack = &m->body.generic_ack;
	struct isi_error *error = NULL;
	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;

	ASSERT(tm_ctx->cancel_state == CANCEL_PENDING);

	if (ack->code == ACK_OK) {
		log(DEBUG, "Coordinator has successfully acked the cancel");
		tm_ctx->cancel_state = CANCEL_ACKED;
	}
	else {
		log(ERROR, "Coordinator responded with an error %d to the "
		    "cancel request: %s", ack->code,
		    ack->msg ? ack->msg : "(No error message returned)");
		tm_ctx->cancel_state = CANCEL_ERROR;
	}

	siq_target_states_set(tm_ctx->policy_id, CHANGE_CANCEL_STATE,
	    0, tm_ctx->cancel_state, false, tm_ctx->restore, &error);
	if (error) {
		target_monitor_shutdown(tm_ctx, isi_error_get_message(error));
		isi_error_free(error);
	}

	return 0;
}

static int
job_status_callback(struct generic_msg *m, void *ctx)
{
	struct job_status_msg *js = &m->body.job_status;
	struct isi_error *error = NULL;
	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;
	char *state = NULL;
	
	state = job_state_to_text(js->job_status, false);
	log(DEBUG, "%s: job state is now %s", __func__, state);
	free(state);

	tm_ctx->job_state = js->job_status;

	siq_target_states_set(tm_ctx->policy_id, CHANGE_JOB_STATE,
	    js->job_status, 0, false, tm_ctx->restore, &error);
	if (error) {
		target_monitor_shutdown(tm_ctx, isi_error_get_message(error));
		isi_error_free(error);
	}

	return 0;
}

static void
cancel(struct migr_tmonitor_ctx *tm_ctx)
{
	struct generic_msg cancel_msg = {};
	struct isi_error *error = NULL;

	tm_ctx->cancel_state = CANCEL_PENDING;
	siq_target_states_set(tm_ctx->policy_id, CHANGE_CANCEL_STATE,
	    0, tm_ctx->cancel_state, false, tm_ctx->restore, &error);

	if (error) {
		target_monitor_shutdown(tm_ctx, isi_error_get_message(error));
		isi_error_free(error);
	}

	cancel_msg.head.type = TARGET_CANCEL_MSG;
	cancel_msg.body.target_cancel.policy_id = tm_ctx->policy_id;
	msg_send(tm_ctx->coord_fd, &cancel_msg);
}

static int
scan_for_cancel(void *ctx)
{
	enum siq_target_cancel_state cstate;
	struct isi_error *error = NULL;
	struct migr_tmonitor_ctx *tm_ctx =
	    &((struct migr_sworker_ctx *)ctx)->tmonitor;

	if (tm_ctx->coord_fd > 0) {
		check_connection_with_peer(tm_ctx->coord_fd, true, 1, &error);
		if (error)
			goto out;
	}

	cstate = siq_target_cancel_state_get(tm_ctx->policy_id, false, &error);
	if (error)
		goto out;

	switch (cstate) {
	case CANCEL_REQUESTED:
		log(INFO, "%s: Cancel requested", __func__);
		cancel(tm_ctx);
		break;

	case CANCEL_INVALID:
		error = isi_siq_error_new(E_SIQ_TGT_ASSOC_BRK, 
		    "Target association missing; canceling policy");
		goto out;
		//break;

	default:
		break;
	}


	// Rescan for cancel
	migr_register_timeout(&cancel_scan_interval, scan_for_cancel, ctx);

out:
	if (error)
		target_monitor_shutdown(tm_ctx, isi_error_get_message(error));

	return 0;
}

static void
send_snap_resp_mesg(char *msg, int fd, unsigned result)
{
	struct generic_msg m;

	log(TRACE, "%s", __func__);
	m.head.type = SNAP_RESP_MSG;
	m.body.snap_resp.result = result;
	m.body.snap_resp.msg = msg;
	msg_send(fd, &m);
}

static void
log_and_senderror(unsigned result, int fd, char *fmt, ...)
    __printflike(3, 4);

static void
log_and_senderror(unsigned result, int fd, char *fmt, ...)
{
	char *msg;
	va_list ap;

	log(TRACE, "%s", __func__);
	va_start(ap, fmt);
	vasprintf(&msg, fmt, ap);
	va_end(ap);
	log(ERROR, "%s", msg);
	send_snap_resp_mesg(msg, fd, result);
}

/*
 * Stores the snapshot alias sid in the target_record.
 * It is used to move the alias to HEAD in failover/failback.
 */
static void
store_archival_snap_info(struct migr_sworker_ctx *sw_ctx,
    ifs_snapid_t snap_sid, char *snapshot_alias_name,
    struct isi_error **error_out)
{
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	int lock_fd = -1;
	struct isi_str latest_snap_alias_name;
	SNAP *snap_alias = NULL;
	ifs_snapid_t alias_sid;

	log(TRACE, "%s", __func__);

	if (snapshot_alias_name == NULL &&
            snap_sid == INVALID_SNAPID)
		goto out;

	lock_fd = siq_target_acquire_lock(sw_ctx->tmonitor.policy_id,
	    O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(sw_ctx->tmonitor.policy_id, &error);
	if (!trec || error)
		goto out;

        if (snap_sid != INVALID_SNAPID)
                trec->latest_archive_snap = snap_sid;

	isi_str_init(&latest_snap_alias_name, snapshot_alias_name,
		strlen(snapshot_alias_name), ENC_DEFAULT, ISI_STR_NO_MALLOC);
	snap_alias = snapshot_open_by_name(&latest_snap_alias_name, NULL);
	if (snap_alias) {
		snapshot_get_snapid(snap_alias, &(alias_sid));
		trec->latest_archive_snap_alias = alias_sid;
	}

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

out:
	if (-1 != lock_fd)
		close(lock_fd);

	if (snap_alias)
		snapshot_close(snap_alias);

	isi_error_handle(error, error_out);
	siq_target_record_free(trec);
}

/*
 * Snapshot created for D2D backup
 */
static void
archival_snap_create(struct migr_sworker_ctx *sw_ctx, struct generic_msg *m,
    struct isi_error **error_out)
{
	char ssname[MAXPATHLEN];
	struct tm *create_tm;
	time_t create_time;
	ifs_snapid_t snapid;
	struct isi_error *error = NULL;
	struct isi_str *issname = NULL;

	/* if this is a legacy callback, use the coord fd from the
	 * sworker context */
	int fd = (sw_ctx->tmonitor.legacy) ?
	    sw_ctx->global.primary : sw_ctx->tmonitor.coord_fd;

	log(TRACE, "archival_snap_create");
	log(INFO, "Trying to create snapshot with name %s on path %s",
	    m->body.snap.name, m->body.snap.path);
	if (isi_licensing_module_status(ISI_LICENSING_SNAPSHOTS) !=
	    ISI_LICENSING_LICENSED) {
		log_and_senderror(SNAP_MSG_RESULT_ERROR,
		    fd, "Snapshots are unlicensed");
		goto out;
	}

	time(&create_time);
	create_tm = localtime(&create_time);
	strftime(ssname, MAXPATHLEN, m->body.snap.name, create_tm);

	UFAIL_POINT_CODE(fail_archive_target_snapshot,
	    log_and_senderror(SNAP_MSG_RESULT_ERROR, fd,
	    "Can`t create snapshot '%s' at %s: %s",
	    ssname, m->body.snap.path, 
	    "Failure due to failpoint fail_archive_target_snapshot");
	    goto out);

	issname = get_unique_snap_name(ssname, &error);
	if (error) {
		log_and_senderror(SNAP_MSG_RESULT_ERROR, fd,
		    "Can`t create unique snapshot name for '%s': %s",
		    ssname, isi_error_get_message(error));
		goto out;
	}

	snapid = takesnap(ISI_STR_GET_STR(issname), m->body.snap.path,
	    m->body.snap.expiration ? time(NULL) + m->body.snap.expiration : 0,
	    m->body.snap.alias, &error);
	if (error) {
		if (!isi_error_is_a(error,
		    SNAP_PATHNOTEXIST_ERROR_CLASS)) {
			log_and_senderror(SNAP_MSG_RESULT_ERROR, fd,
			    "Can`t create snapshot '%s' at %s: %s",
			    ssname, m->body.snap.path,
			    isi_error_get_message(error));
		} else {
			log(INFO, "Skipping snapshot - %s does not exist",
		    	    m->body.snap.path);
			send_snap_resp_mesg("", fd, SNAP_MSG_RESULT_SKIP);
		}
		goto out;
	}

	store_archival_snap_info(sw_ctx, snapid,
	    m->body.snap.alias, &error);

	log(INFO, "Snapshot %s taken at %s", ISI_STR_GET_STR(issname),
	    m->body.snap.path);

	send_snap_resp_mesg(ssname, fd, SNAP_MSG_RESULT_OK);

out:
	isi_error_handle(error, error_out);
	if (issname)
		isi_str_free(issname);
	if (sw_ctx->tmonitor.legacy)
		exit(EXIT_SUCCESS);

	return;
}

/*
 * A snapshot created by default for failover/failback
 */
static void
failover_snap_create(struct migr_sworker_ctx *sw_ctx, struct generic_msg *m,
    struct isi_error **error_out)
{
	char ssname[MAXPATHLEN];
	struct isi_str *issname = NULL;
	ifs_snapid_t snapid = 0;
	ifs_snapid_t prev_snapid = 0;
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	struct migr_tmonitor_ctx *tm_ctx = &sw_ctx->tmonitor;

	int fd = sw_ctx->tmonitor.coord_fd;

	log(TRACE, "failover_snap_create");

	get_failover_snap_name(tm_ctx->policy_name, ssname);
	log(INFO, "Trying to create failover snapshot with name %s on path %s",
	    ssname, m->body.snap.path);

	UFAIL_POINT_CODE(fail_failover_target_snapshot,
	    log_and_senderror(FAILOVER_SNAP_MSG_RESULT_ERROR, fd,
	    "Can`t create snapshot '%s' at %s: %s",
	    ssname, m->body.snap.path, 
	    "Failure due to failpoint fail_failover_target_snapshot");
	    goto out);

	issname = get_unique_snap_name(ssname, &error);
	if (error) {
		log_and_senderror(FAILOVER_SNAP_MSG_RESULT_ERROR, fd,
		    "Can`t create unique snapshot name for '%s': %s",
		    ssname, isi_error_get_message(error));
		goto out;
	}
	strncpy(ssname, ISI_STR_GET_STR(issname), MAXPATHLEN);

	snapid = takesnap(ssname, m->body.snap.path, 0, NULL, &error);
	if (error) {
		log_and_senderror(FAILOVER_SNAP_MSG_RESULT_ERROR, fd,
		    "Can`t create snapshot '%s' at %s: %s",
		    ssname, m->body.snap.path, 
		    isi_error_get_message(error));
		goto out;
	} 

	log(INFO, "Snapshot %s taken at %s", ssname, m->body.snap.path);

	lock_fd = siq_target_acquire_lock(sw_ctx->tmonitor.policy_id,
	    O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(sw_ctx->tmonitor.policy_id, &error);
	if (!trec || error)
		goto out;

	if (trec->fofb_state > FOFB_NONE && 
	    (!sw_ctx->tmonitor.restore || sw_ctx->tmonitor.failback_prep)) {
		log_and_senderror(FAILOVER_SNAP_MSG_RESULT_ERROR, fd,
		    "Can't create snapshot at %s: failover job in progress", 
		    m->body.snap.path);
		goto out;
	}
	prev_snapid = trec->latest_snap;

	trec->latest_snap = snapid;

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	close(lock_fd);
	lock_fd = -1;

	if (prev_snapid != INVALID_SNAPID) {
		delete_snapshot_helper(prev_snapid);
	}

	send_snap_resp_mesg(ssname, fd, SNAP_MSG_RESULT_OK);

out:
	isi_error_handle(error, error_out);

	if (-1 != lock_fd)
		close(lock_fd);

	if (issname)
		isi_str_free(issname);
	siq_target_record_free(trec);

	if (sw_ctx->tmonitor.legacy)
		exit(EXIT_SUCCESS);
}

/* delete an existing FOFB snapshot if 'disable FOFB' has been turned on for
 * the policy */
static void
failover_snap_delete(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	int lock_fd = -1;
	int fd;
       
	log(TRACE, "%s", __func__);

	fd = sw_ctx->tmonitor.coord_fd;

	lock_fd = siq_target_acquire_lock(sw_ctx->tmonitor.policy_id,
	    O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(sw_ctx->tmonitor.policy_id, &error);
	if (!trec || error)
		goto out;

	if (trec->fofb_state > FOFB_NONE && 
	    (!sw_ctx->tmonitor.restore || sw_ctx->tmonitor.failback_prep)) {
		log_and_senderror(FAILOVER_SNAP_MSG_RESULT_ERROR, fd,
		    "Can't delete snapshot: failover job in progress");
		goto out;
	}

	if (trec->latest_snap != INVALID_SNAPID)
		delete_snapshot_helper(trec->latest_snap);

	trec->latest_snap = INVALID_SNAPID;

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	send_snap_resp_mesg(NULL, fd, SNAP_MSG_RESULT_OK);

out:
	if (lock_fd != -1) {
		close(lock_fd);
	}
	isi_error_handle(error, error_out);
	siq_target_record_free(trec);
}

int
snap_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct isi_error *error = NULL;
	struct stat st;
	int fd;

	log(TRACE, "snap_callback");

	/* if this is a legacy callback, use the coord fd from the
	 * sworker context */
	fd = (sw_ctx->tmonitor.legacy) ?
	    sw_ctx->global.primary : sw_ctx->tmonitor.coord_fd;

	if (lstat(m->body.snap.path, &st)) {
		log(INFO, "Skipping snapshot - %s does not exist",
		    m->body.snap.path);
		send_snap_resp_mesg("", fd, SNAP_MSG_RESULT_SKIP);
		goto out;
	}

	if (m->body.snap.name == NULL) {
		failover_snap_create(sw_ctx, m, &error);
	} else if (strncmp(m->body.snap.name, FOFB_SNAP_DEL,
	    strlen(FOFB_SNAP_DEL)) == 0) {
		failover_snap_delete(sw_ctx, &error);
	} else {
		archival_snap_create(sw_ctx, m, &error);
	}

out:
	if (error) {
		log(ERROR, "Error: %s", isi_error_get_message(error));
		isi_error_free(error);
	}

	return 0;
}

static int
create_domain(struct migr_sworker_ctx *sw_ctx, struct target_record *trec,
    struct isi_error **error_out)
{
	int dir_fd = -1;
	struct path target_dir;
	struct stat st;
	int ret = -1;
	ifs_domainid_t stale_domain_id = 0;
	ifs_domainid_t new_domain_id = 0;
	uint32_t generation = 0;
	struct isi_error *error = NULL;

	log(TRACE, "create_domain begin: %s", trec->target_dir);
	// Find the directory or create it if it doesn't exist
	path_init(&target_dir, trec->target_dir, NULL);
	ret = smkchdirfd(&target_dir, AT_FDCWD, &dir_fd);
	if (ret != 0) {
		goto out;
	}

	// Get the LIN for the directory
	ret = fstat(dir_fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "create_domain: failed to fstat");
		goto out;
	}

	// Make sure we don't already have a stale/incomplete SyncIQ
	// domain at this location
	find_domain_by_lin(st.st_ino, DT_SYNCIQ, false, &stale_domain_id,
	    NULL, &error);
	if (error)
		goto out;
	if (stale_domain_id != 0) {
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "An existing SyncIQ domain (id %lld) was found at or "
		    "overlapping the target path. Please contact Isilon "
		    "support for assistance removing old domain before "
		    "proceeding", stale_domain_id);
		goto out;
	}

	ret = dom_make_domain(st.st_ino, &new_domain_id, &generation);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "create_domain: failed to dom_make_domain");
		goto out;
	}

	trec->domain_id = new_domain_id;
	trec->domain_generation = generation;

	ret = ifs_domain_add_bylin(st.st_ino, new_domain_id);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "create_domain: failed to ifs_domain_add");
		goto out;
	}

	/*
	 * If target path is also compliance v2 domain, then add
	 * compliance store root name and cluster GUID to domain.
	 *
	 * Ignore if it is a fake compliance v2 domain
	 */
	if (!sw_ctx->global.is_fake_compliance &&
	    sw_ctx->global.compliance_dom_ver == 2) {
		path_add(&target_dir, COMPLIANCE_STORE_ROOT_NAME,
		    strlen(COMPLIANCE_STORE_ROOT_NAME), ENC_DEFAULT);
		ret = stat(target_dir.path, &st);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "stat failed for %s", target_dir.path);
			goto out;
		}
		ret = ifs_domain_add_bylin(st.st_ino, new_domain_id);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "create_domain: failed to ifs_domain_add for %{}",
			    lin_fmt(st.st_ino));
			goto out;
		}
		path_add(&target_dir, get_cluster_guid() ,
		    strlen(get_cluster_guid()), ENC_DEFAULT);
		ret = stat(target_dir.path, &st);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "stat failed for %s", target_dir.path);
			goto out;
		}
		ret = ifs_domain_add_bylin(st.st_ino, new_domain_id);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "create_domain: failed to ifs_domain_add for %{}",
			    lin_fmt(st.st_ino));
			goto out;
		}
	}

	ret = dom_set_siq(trec->domain_id);
	if (ret != 0) {
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "create_domain failed to dom_set_siq (%d)", ret);
		goto out;
	}

	ret = dom_set_readonly(trec->domain_id, true);
	if (ret != 0) {
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "create_domain failed to dom_set_readonly (%d)", ret);
		goto out;
	}

	log(TRACE, "create_domain end: %lld", trec->domain_id);

	ret = 0;
out:
	if (error && new_domain_id != 0) {
		log(NOTICE, "%s: Failed domain creation. Removing domain id "
		    "%lld", __func__, new_domain_id);
		dom_remove_domain(new_domain_id);
	}
	isi_error_handle(error, error_out);
	if (dir_fd != -1) {
		close(dir_fd);
	}

	return ret;
}

static int
get_domain(struct target_record *trec)
{
	int ret = -1;
	uint32_t domain_generation;

	log(TRACE, "get_domain begin");

	ASSERT(trec->domain_id != 0);

	ret = dom_inc_generation(trec->domain_id, &domain_generation);
	if (ret != 0) {
		log(ERROR, "get_domain: failed to dom_inc_generation (%d)",
		    errno);
		return ret;
	}
	trec->domain_generation = domain_generation;

	log(TRACE, "get_domain end: %d", trec->domain_generation);

	return 0;
}

static int
upgrade_complete_callback(struct generic_msg *m, void *ctx)
{
	struct upgrade_complete_msg *msg = &m->body.upgrade_complete;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	int ret = 0;
	struct generic_msg ack = {};
	struct isi_error *error;

	ASSERT(msg->domain_id == sw_ctx->global.domain_id);
	ASSERT(msg->domain_generation == sw_ctx->global.domain_generation);

	ret = dom_mark_ready(sw_ctx->global.domain_id);
	if (ret != 0) {
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "Failed to dom_mark_ready (%d)", errno);
		target_monitor_shutdown(&sw_ctx->tmonitor,
		    isi_error_get_message(error));
		goto out;
	}

	ack.head.type = GENERIC_ACK_MSG;
	msg_send(sw_ctx->tmonitor.coord_fd, &ack);

out:
	return 0;
}

static void 
setup_restore_domain(struct migr_sworker_ctx *sw_ctx, 
    const char *root_dir, bool is_restart, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_domainid_t domid = 0;
	uint32_t gen = 0;
	int res;

	log(TRACE, "setup_restore_domain");

	find_domain(root_dir, DT_SYNCIQ, true, &domid, &gen, &error);
	if (error)
		goto out;

	if (!domid) {
		find_domain(root_dir, DT_SNAP_REVERT, true, &domid, &gen,
		    &error);
		if (error)
			goto out;
	}

	if (!domid) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "%s: Cannot "
		    "begin restore without domain.", __func__);
		goto out;
	}

	sw_ctx->global.domain_id = domid;
	sw_ctx->global.domain_generation = gen;
	res = ifs_domain_allowwrite(sw_ctx->global.domain_id,
	    sw_ctx->global.domain_generation);		
	if (res != 0) {
		log(ERROR, "Failed to set domain allow write (%d)", errno);
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "Failed to set ifs_domain_allowwrite");
		goto out;
	}
	
out:
	isi_error_handle(error, error_out);
}

static void
setup_domain(struct migr_sworker_ctx *sw_ctx, struct target_record *trec,
    bool is_restart, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int res;

	log(TRACE, "setup_domain");

	/* Get new domain generation from file system */
	if (trec->domain_id == 0) {
		/* Create new domain + get domain ID from file system */
		trec->domain_generation = 0;
		create_domain(sw_ctx, trec, &error);
		if (error) {
			log(ERROR, "Failed to create domain: %s",
			    isi_error_get_message(error));
			goto out;
		}
	}

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	/*
	 * Allocate a new domain generation unless this is just a restart of
	 * the target monitor.
	 */
	if (!is_restart) {
		if (get_domain(trec) == -1) {
			log(ERROR, "Failed to get domain");
			error = isi_siq_error_new(E_SIQ_UPGRADE,
			    "Failed to get domain");
			goto out;
		}
	}

	sw_ctx->global.domain_id = trec->domain_id;
	sw_ctx->global.domain_generation = trec->domain_generation;
	res = ifs_domain_allowwrite(sw_ctx->global.domain_id,
	    sw_ctx->global.domain_generation);		
	if (res != 0) {
		log(ERROR, "Failed to set domain allow write (%d)", errno);
		error = isi_siq_error_new(E_SIQ_UPGRADE,
		    "Failed to set ifs_domain_allowwrite");
		goto out;
	}
	siq_target_record_write(trec, &error);
	if (error)
		goto out;
	
out:
	isi_error_handle(error, error_out);
}

/* skip linmap creation if one is expected to already exist */
static void
linmap_setup(char *policy_id, struct target_record *trec,
    bool linmap_remove, bool initial_sync, bool stf_sync,
    bool stf_upgrade_sync, bool restore, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool exists;
	bool fail_if_linmap_not_empty = false;
	bool empty;
	struct map_ctx *mctx = NULL;
	char lmap_name[MAXNAMELEN];

	get_lmap_name(lmap_name, policy_id, restore);

	if (linmap_remove) {
		exists = linmap_exists(policy_id, lmap_name, &error);
		if (error)
			goto out;
		if (exists) {
			log(NOTICE, "Policy '%s' deleting stale linmap",
			    lmap_name);
			remove_linmap(policy_id, lmap_name, true, &error);
			if (error)
				goto out;
		}
	}

	if ((linmap_remove || !trec->linmap_created) &&
	    (initial_sync || stf_upgrade_sync || restore || !stf_sync)) {
		exists = linmap_exists(policy_id, lmap_name, &error);
		if (error)
			goto out;
		if (exists)
			fail_if_linmap_not_empty = true;
		else {
			create_linmap(policy_id, lmap_name, false,
			    &error);
			if (error)
				goto out;
			log(DEBUG, "Created linmap %s", lmap_name);
		}
		trec->linmap_created = true;
	}

	/* make sure a linmap exists */
	open_linmap(policy_id, lmap_name, &mctx, &error);
	if (error)
		goto out;
	else {
		if (fail_if_linmap_not_empty) {
			empty = linmap_empty(mctx, &error);
			if (error)
				goto out;
			if (!empty) {
				error = isi_siq_error_new(E_SIQ_UPGRADE,
			    	    "Non-empty linmap %s found on target",
				    lmap_name);
				goto out;
			}
		}

		log(DEBUG, "Found linmap %s", lmap_name);
		close_linmap(mctx);
	}

out:
	isi_error_handle(error, error_out);
}

static void
handle_target_path_change(struct migr_sworker_ctx *sw_ctx,
    struct target_record *trec, struct target_init_msg *tim,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct target_record *trec_dummy = NULL;
	char *idstr = NULL;
	int lock_fd = -1;

	log(DEBUG, "Target path changed"); 

	trec_dummy = calloc(1, sizeof(struct target_record));
	idstr = siq_create_policy_id(false);

	lock_fd = siq_target_acquire_lock(idstr, O_EXLOCK, &error);
	if (error)
		goto out;

	trec_dummy->id = strdup(idstr);
	trec_dummy->target_dir = strdup(trec->target_dir);
	trec_dummy->policy_name = strdup(trec->policy_name);
	trec_dummy->src_cluster_name = strdup(trec->src_cluster_name);
	trec_dummy->src_cluster_id = strdup(trec->src_cluster_id);
	trec_dummy->last_src_coord_ip = strdup(trec->last_src_coord_ip);
	trec_dummy->job_status = SIQ_JS_NONE;
	trec_dummy->job_status_timestamp = trec->job_status_timestamp;
	trec_dummy->domain_id = trec->domain_id;
	trec_dummy->domain_generation = trec->domain_generation;

	siq_target_record_write(trec_dummy, &error);
	if (error)
		goto out;

	/* Clear domain info so setup_domain will create a new one */
	trec->domain_id = 0;
	trec->domain_generation = 0;

	/* Clear version info so target_monitor will begin anew. */
	memset(&trec->composite_job_ver, 0, sizeof(trec->composite_job_ver));
	memset(&trec->prev_composite_job_ver, 0,
	    sizeof(trec->prev_composite_job_ver));

out:
	if (lock_fd != -1)
		close(lock_fd);
	free(idstr);
	siq_target_record_free(trec_dummy);
	isi_error_handle(error, error_out);
}

static int
group_callback(void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct migr_tmonitor_ctx *tctx = &sw_ctx->tmonitor;
	char **new_ips = NULL;
	int new_num_nodes = 0;
	struct isi_error *error = NULL;

	new_num_nodes = get_local_cluster_info(NULL, &new_ips, NULL, NULL,
	    &g_dst_addr, tctx->restrict_name, false, false, NULL, &error);
	if (error) {
		log(ERROR, "%s: %s", __func__, isi_error_get_message(error));
		exit(EXIT_SUCCESS);
	}

	if (ip_lists_match(new_num_nodes, new_ips, tctx->num_nodes, tctx->ips)
	    == false) {
		/* coord establishes a new connection to get new cluster IPs
		 * after a group change that involves node changes */
		log(INFO,
		    "Target monitor disconnecting from coord on group change");
		exit(EXIT_SUCCESS);
	}
	return 0;
}

static int
cleanup_database_mirror_callback(struct generic_msg *m, void *ctx)
{

	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct cleanup_database_mirror_msg *cleanup =
	    &m->body.cleanup_database_mirror;
	struct generic_msg cleanup_ack = {};
	char rep_mirror_name[MAXNAMLEN];
	char cpss_mirror_name[MAXNAMLEN];
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(global->curr_ver >= FLAG_VER_3_5);
	ASSERT(cleanup->flags & CLEANUP_REP);
	ASSERT(!strcmp(cleanup->policy_id, sw_ctx->tmonitor.policy_id));

	/*
	 * ASSERT that we have not opened any repstate
	 */
	ASSERT(global->rep_mirror_ctx == NULL);

	get_mirrored_rep_name(rep_mirror_name, sw_ctx->tmonitor.policy_id);
	remove_repstate(sw_ctx->tmonitor.policy_id, rep_mirror_name,
	    true, &error);
	if (error)
		goto out;

	get_mirrored_cpss_name(cpss_mirror_name, sw_ctx->tmonitor.policy_id);
	remove_cpss(sw_ctx->tmonitor.policy_id, cpss_mirror_name, true,
	    &error);
	if (error)
		goto out;

	/* Now send ACK back to source. */
	cleanup_ack.head.type = GENERIC_ACK_MSG;
	msg_send(sw_ctx->tmonitor.coord_fd, &cleanup_ack);

out:
	handle_stf_error_fatal(global->primary, error, E_SIQ_DATABASE_ENTRIES,
	    EL_SIQ_DEST, 0, 0);
	return 0;
}

static int
tm_dump_status(void *ctx)
{
	char *output_file = NULL;
	FILE *f;
	int ret = 1;
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct migr_tmonitor_ctx *tm = &sw_ctx->tmonitor;
	
	asprintf(&output_file, "/var/tmp/tmonitor_status_%d.txt", getpid());
	
	f = fopen(output_file, "w");
	if (f == NULL) {
		log(ERROR,"Status generation failed: %s", strerror(errno));
		goto out;
	}
	
	fprintf(f, "SyncIQ job '%s' (%s)\n", tm->policy_name, tm->policy_id);
	fprintf(f, "\n****Global Context****\n");
	
	fprintf(f, "coord_fd: %d\n", tm->coord_fd);
	fprintf(f, "cancel_state: %d\n", tm->cancel_state);
	fprintf(f, "job_state: %d\n", tm->job_state);
	fprintf(f, "cleaned_up: %s\n", PRINT_BOOL(tm->cleaned_up));
	fprintf(f, "legacy: %s\n", PRINT_BOOL(tm->legacy));
	fprintf(f, "restore: %s\n", PRINT_BOOL(tm->restore));
	fprintf(f, "failback_prep: %s\n", PRINT_BOOL(tm->failback_prep));
	fprintf(f, "num_nodes: %d\n", tm->num_nodes);
	fprintf(f, "ips:\n");
	for (int i = 0; i < tm->num_nodes; i++) {
		fprintf(f, " %s\n", tm->ips[i]);
	}
	fprintf(f, "restrict_name: %s\n", tm->restrict_name);
	
	print_net_state(f);
	
	log(NOTICE, "Status information dumped to %s", output_file);
	ret = 0;
out:
	if (f) {
		fclose(f);
		f = NULL;
	}
	free(output_file);
	return ret;
}

static void
tm_sigusr2(int signo)
{
	struct timeval tv = {};

	migr_register_timeout(&tv, tm_dump_status, g_ctx);
}

static int
comp_commit_end_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_tmonitor_ctx *tmonitor = &sw_ctx->tmonitor;
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter", __func__);

	//Set late stage commit flag so we properly cleanup temp dirs
	if (tmonitor->restore) {
		log(TRACE, "%s: setting doing_comp_commit_finish", __func__);
		sw_ctx->tmonitor.doing_comp_commit_finish = true;
	}

	// Unset target record
	edit_target_record_comp_commit(tmonitor->policy_id, false, &error);
	if (error) {
		goto out;
	}
	// Delete lin_list
	remove_lin_list(tmonitor->policy_id, LIN_LIST_NAME_WORM_COMMIT,
	    true, &error);

out:
	if (error) {
		send_comp_target_error(tmonitor->coord_fd,
		    "Failure occurred ending compliance commits",
		    E_SIQ_COMP_COMMIT, error);
		isi_error_free(error);
	}
	log(TRACE, "%s: exit", __func__);
	return 0;
}

static void
initial_sync_cleanup(char *pid, struct isi_error **error_out)
{
	struct siq_policies *policies = NULL;
	struct siq_policy *policy = NULL;
	struct siq_source_record *srec = NULL;
	struct isi_error *error = NULL;
	char rep_mirror_name[MAXNAMLEN];
	char cpss_mirror_name[MAXNAMLEN];

	/*
	 * Defect 22759
	 * Remove mirrored repstate on target side after policy reset
	 * on source side
	 */
	get_mirrored_rep_name(rep_mirror_name, pid);
	remove_repstate(pid, rep_mirror_name, true, &error);
	log(TRACE, "%s: remove %s", __func__, rep_mirror_name);
	if (error)
		goto out;

	/*
	 * Bug 263447
	 * Remove mirrored CPSS on target side after policy reset
	 */
	get_mirrored_cpss_name(cpss_mirror_name, pid);
	remove_cpss(pid, cpss_mirror_name, true, &error);
	log(TRACE, "%s: remove %s", __func__, cpss_mirror_name);
	if (error)
		goto out;

	policies = siq_policies_load_nolock(&error);
	if (error)
		goto out;

	policy = siq_policy_get_by_pid(policies, pid);
	if (!policy)
		goto out;

	/* Reset the mirror pol if it exists. */
	if (policy->common->mirror_pid &&
	    siq_source_has_record(policy->common->mirror_pid)) {
		srec = siq_source_record_load(policy->common->mirror_pid,
		    &error);
		if (error)
			goto out;

		siq_source_force_full_sync(srec, &error);
		if (error)
			goto out;

		siq_source_record_save(srec, &error);
		if (error)
			goto out;
	}

out:
	siq_source_record_free(srec);
	siq_policies_free(policies);

	isi_error_handle(error, error_out);
}

void
target_monitor(int coord_fd, struct migr_sworker_ctx *sw_ctx,
    struct target_init_msg *tim, struct generic_msg *resp)
{
	char proctitle[100];
	char *polid_5_0 = NULL;
	char *context;
	int lock_fd = -1;
	int lock_fd_5_0 = -1;
	int tmp_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	struct version_status ver_stat;
	struct migr_tmonitor_ctx *tm_ctx = &sw_ctx->tmonitor;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	bool initial_sync = tim->flags & FLAG_CLEAR;
	bool stf_sync = tim->flags & FLAG_STF_SYNC;
	bool stf_upgrade_sync = tim->flags & FLAG_STF_UPGRADE_SYNC;
	bool linmap_remove = tim->flags & FLAG_STF_LINMAP_DELETE;
	bool restore = tim->flags & FLAG_RESTORE;
	bool failback_prep = tim->flags & FLAG_FAILBACK_PREP;
	bool domain_mark = tim->flags & FLAG_DOMAIN_MARK;
	bool new_record = false;
	bool delete_5_0_record = false;
	struct siq_job_version *job_ver, *prev_job_ver;
	char resync_name[MAXNAMELEN + 1];
	struct lin_list_ctx *resync_ctx = NULL;
	struct lin_list_info_entry llie = {};
	char cpss_mirror_name[MAXNAMLEN];


	log(TRACE, "%s: %s  %d", __func__, tim->policy_name, tim->option);

	if (stf_sync)
		ASSERT(global->job_ver.common >= MSG_VERSION_CHOPU);

	if (linmap_remove)
		ASSERT(initial_sync || stf_upgrade_sync || restore);

	// Now running as target-monitor
	snprintf(proctitle, sizeof(proctitle), "monitor %s.%s",
	    tim->src_cluster_name, tim->policy_name);
	setproctitle(proctitle);
	tm_ctx->legacy = false;

	tm_ctx->coord_fd = coord_fd;
	tm_ctx->policy_id = strdup(tim->policy_id);
	tm_ctx->policy_name = strdup(tim->policy_name);
	tm_ctx->restore = restore;
	tm_ctx->stf_sync = stf_sync;
	tm_ctx->failback_prep = failback_prep;
	tm_ctx->restrict_name = (tim->restricted_target_name) ?
	    strdup(tim->restricted_target_name) : NULL;
	tm_ctx->discon_enable_mirror = false;

	migr_unregister_all_callbacks(coord_fd);

	migr_register_callback(coord_fd, DISCON_MSG, tm_disconnect_callback);
	migr_register_callback(coord_fd, SNAP_MSG, snap_callback);
	migr_register_callback(coord_fd, CLEANUP_SYNC_RUN_MSG,
	    cleanup_sync_run_callback);
	migr_register_callback(coord_fd, GENERIC_ACK_MSG, ack_callback);
	migr_register_callback(coord_fd, JOB_STATUS_MSG,
	   job_status_callback);
	migr_register_callback(coord_fd, CLEANUP_TARGET_TMP_FILES_MSG,
	    cleanup_target_tmp_files_callback);
	migr_register_callback(coord_fd, VERIFY_FAILOVER_MSG,
	    verify_failover_callback);
	migr_register_callback(coord_fd, UPGRADE_COMPLETE_MSG,
	    upgrade_complete_callback);
	migr_register_callback(coord_fd, OLD_FAILBACK_PREP_FINALIZE_MSG,
	    failback_prep_finalize_callback);
	migr_register_callback(coord_fd,
	    build_ript_msg_type(FAILBACK_PREP_FINALIZE_MSG),
	    failback_prep_finalize_callback);
	migr_register_callback(coord_fd, CLEANUP_DATABASE_MIRROR_MSG,
	    cleanup_database_mirror_callback);
	
	if (tim->flags & FLAG_COMPLIANCE_V2 &&
	    global->compliance_dom_ver == 2) {
		migr_register_callback(coord_fd, 
		    build_ript_msg_type(COMP_COMMIT_END_MSG),
		    comp_commit_end_callback);
		/*
		 * Create a resync SBT on the destination cluster.
		 * For restores, the local coordinator would have
		 * taken care to create it.
		 *
		 * Always open the _dst resync list unless this is resync prep.
		 */
		get_resync_name(resync_name, sizeof(resync_name),
		    tim->policy_id, failback_prep);

		if (!restore) {
			create_lin_list(tim->policy_id, resync_name, true,
			    &error);
			if (error)
				goto out;
		}

		open_lin_list(tim->policy_id, resync_name, &resync_ctx, &error);
		if (error)
			goto out;

		llie.type = RESYNC;
		set_lin_list_info(&llie, resync_ctx, false, &error);
		if (error)
			goto out;
	}

	/* dynamic worker allocation uses tmonitor disconnect on group change
	 * to alert the coordinator */
	migr_register_group_callback(group_callback, sw_ctx);

	tm_ctx->cancel_state = CANCEL_WAITING_FOR_REQUEST;
	tm_ctx->job_state = SIQ_JS_RUNNING;

	/* Remove duplicate 5.0 target record if it exists. This can happen
	 * if the tmonitor crashed last time in between saving the new 6.5+
	 * target record and deleting the old 5.0 record. */
	remove_duplicate_5_0_policy(tim->policy_id, &error);
	if (error)
		goto out;

	if (initial_sync) {
		initial_sync_cleanup(tim->policy_id, &error);
		if (error)
			goto out;
	}

	lock_fd = siq_target_acquire_lock(tim->policy_id, O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(tim->policy_id, &error);
	if (error) {
		/* non-existence is acceptable */
		if (isi_siq_error_get_siqerr((struct isi_siq_error *)error)
		    == E_SIQ_CONF_NOENT) {
			isi_error_free(error);
			error = NULL;
		} else {
			goto out;
		}
	}

	// If not yet in target DB, or the policy is in the TDB, but it was
	// run from an older version of SIQ that doesn't have target
	// awareness, fill in what needs to be added.
	if (!trec || trec->cant_cancel) {
		if (trec)
			log(DEBUG, "Policy %s.%s is from a source cluster "
			    "that has upgraded from 5.0 or 5.5 to 6.0",
			    tim->src_cluster_name, tim->policy_name);
		else {
			// If we can't find the policy it's either a new one,
			// or a 5.0 one where we didn't have a policy ID to
			// store in the TDB.
			polid_5_0 = make_5_0_policy_id(tim->src_cluster_name,
			    tim->policy_name, tim->target_dir);
			lock_fd_5_0 = siq_target_acquire_lock(polid_5_0,
			    O_EXLOCK, &error);
			if (error)
				goto out;

			if (!siq_target_record_exists(polid_5_0)) {
				log(DEBUG, "Policy %s.%s is a new policy",
				    tim->src_cluster_name, tim->policy_name);
				trec = calloc(1, sizeof(struct target_record));
				ASSERT(trec);
			} else {
				log(DEBUG, "Policy %s.%s was last run from a"
				    " 5.0 cluster. Upgrading target record"
				    " policy id.", tim->src_cluster_name,
				    tim->target_dir);
				trec = siq_target_record_read(polid_5_0,
				    &error);
				if (error)
					goto out;

				/* We're converting to post 5.0 format, delete
				 * the 5.0 record when we're done */
				delete_5_0_record = true;
			}
			// If it's a new policy, the record needs its ID.
			// If it's a 5.0 policy, we need to change the record
			// ID from the pseudo-ID used for 5.0 policies.
			trec->id = strdup(tim->policy_id);
			ASSERT(trec->id);
		}

		trec->src_cluster_id = strdup(tim->src_cluster_id);
		trec->job_status = SIQ_JS_RUNNING;
		trec->cancel_state = CANCEL_WAITING_FOR_REQUEST;
		trec->cant_cancel = false;
		new_record = true;
	} else if (trec && trec->fofb_state != FOFB_NONE && 
	    !restore && !failback_prep) {
		log(ERROR, 
		    "Cannot run %s.%s during Failover/Failback.",
		    tim->src_cluster_name, tim->policy_name);
		error = isi_siq_error_new(E_SIQ_TGT_DIR,
		    "%s.%s cannot run during Failover/Failback.",
		    tim->src_cluster_name, tim->policy_name);
		goto out;
	}

	// Record already exists; the policy has been run before. If it's
	// _not_ a restart (OPTION_INIT), we have to set the initial job and
	// cancel state for a new job. If it's a restart (OPTION_RESTART),
	// leave the job and cancel state as they were.
	else if (tim->option == OPTION_INIT) {
		trec->job_status = SIQ_JS_RUNNING;
		trec->cancel_state = CANCEL_WAITING_FOR_REQUEST;
	}

	/*
	 * If target dir changed, we need to create a domain for the new
	 * path, and create a false target record for the old one so the user
	 * can break the target record and delete the defunct domain.
	 */
	if (trec->target_dir && !restore && !failback_prep &&
	    strcmp(trec->target_dir, tim->target_dir) != 0) {
		handle_target_path_change(sw_ctx, trec, tim, &error);
		if (error)
			goto out;
	}

	// Things to change regardless of whether this is a new record in the
	// target DB or not.
	if (!domain_mark) {
		trec->target_dir = strdup(tim->target_dir);
		trec->policy_name = strdup(tim->policy_name);
		if (!restore)
			trec->src_cluster_name = strdup(tim->src_cluster_name);
		trec->last_src_coord_ip = strdup(inx_addr_to_str(&g_src_addr));
		trec->job_status_timestamp = time(0);
		trec->monitor_node_id = get_local_node_id();
		if (trec->monitor_node_id == -1) {
			error = isi_siq_error_new(E_SIQ_TDB_ERR,
			    "Can't get target node ID");
			goto out;
		}
		trec->monitor_pid = getpid();
	}

	/* Bug 235866
	 * Set enable_hash_dir from target init msg flags.
	 */

	trec->enable_hash_tmpdir = (tim->flags & FLAG_ENABLE_HASH_TMPDIR) != 0;

	/*
	 * If we're a restart, there's no need to check or create
	 * a linmap.
	 */
	if (tim->option == OPTION_INIT) {
		if (!failback_prep || restore) {
			linmap_setup(tim->policy_id, trec, linmap_remove,
		    	    initial_sync, stf_sync, stf_upgrade_sync, restore, 
			    &error);
		} else {
			verify_linmap_for_failback(&sw_ctx->tmonitor, &error);
		}

		if (error)
			goto out;
	}

	if (stf_sync) {
		if (restore || failback_prep) {
			setup_restore_domain(sw_ctx, tim->target_dir,
			    (tim->option == OPTION_RESTART), &error);
		} else {
			setup_domain(sw_ctx, trec,
			    (tim->option == OPTION_RESTART), &error);
		}
		if (error)
			goto out;
	} else {
		sw_ctx->global.domain_id = 0;
		sw_ctx->global.domain_generation = 0;
	}

	if (sw_ctx->global.job_ver.common < MSG_VERSION_RIPTIDE) {
		resp->body.old_target_resp2.domain_id = sw_ctx->global.domain_id;
		resp->body.old_target_resp2.domain_generation =
		    sw_ctx->global.domain_generation;
	} else {
		ript_msg_set_field_uint64(&resp->body.ript, RMF_DOMAIN_ID, 1,
		    sw_ctx->global.domain_id);
		ript_msg_set_field_uint32(&resp->body.ript,
		    RMF_DOMAIN_GENERATION, 1,
		    sw_ctx->global.domain_generation);
	}

	/*
	 * Initialize the version info for this job. Note that
	 * global->job_ver.common was set after the accept handshake.
	 */
	get_version_status(false, &ver_stat);
	job_ver = &trec->composite_job_ver;
	prev_job_ver = &trec->prev_composite_job_ver;

	if (global->restore) {
		/* Restore path; don't modify target record job vers. */

		/* If the most recent non-restore common job version is
		 * 0 (i.e. the last non-restore job succeeded) then use the
		 * previous non-restore job version for consistency;
		 * otherwise, use the the most recent non-restore job ver. */
		if (job_ver->common == 0) {
			context = "previous (restore)";
			ASSERT(job_ver->local == 0);
			job_ver = prev_job_ver;
		} else {
			context = "existing (restore)";
			ASSERT(job_ver->local > 0 &&
			    job_ver->local <= ver_stat.committed_version);
		}
		if (global->job_ver.common != job_ver->common &&
		    job_ver->common >= MSG_VERSION_RIPTIDE) {
			error = isi_siq_error_new(E_SIQ_TDB_ERR,
			    "Common job version (%s) mismatch: "
			    "expected >= 0x%x, actual = 0x%x", context,
			    job_ver->common, global->job_ver.common);
			goto out;
		}

	} else if (job_ver->common == 0) {
		/* The previous job (if any) succeeded; choose a new ver. */
		context = "new";
		ASSERT(job_ver->local == 0);
		if (global->job_ver.common < prev_job_ver->common) {
			error = isi_siq_error_new(E_SIQ_TDB_ERR,
			    "Common job version (new) mismatch: "
			    "expected >= 0x%x, actual = 0x%x",
			    prev_job_ver->common, global->job_ver.common);
			goto out;
		}
		job_ver->local = ver_stat.committed_version;
		job_ver->common = global->job_ver.common;

	} else {
		/* The previous job did not succeed; use the existing ver. */
		context = "existing";
		ASSERT(job_ver->local > 0 &&
		    job_ver->local <= ver_stat.committed_version);
		if (global->job_ver.common < job_ver->common) {
			/* Handshaking at less than the existing version is an
			 * error. (Cluster downgrades are not a concern.) */
			error = isi_siq_error_new(E_SIQ_TDB_ERR,
			    "Common job version (existing) mismatch: "
			    "expected = 0x%x, actual = 0x%x",
			    job_ver->common, global->job_ver.common);
			goto out;

		} else if (global->job_ver.common > job_ver->common) {
			/* Generally speaking, the common version SHOULD be
			 * maintained until a job has successfully completed.
			 * However, pre-Riptide source clusters can't enforce
			 * that and there may be a need to allow for this in
			 * the future, so just log a message and continue. */
			log(ERROR, "Common job version upgraded from 0x%x to"
			    " 0x%x", job_ver->common, global->job_ver.common);
			job_ver->common = global->job_ver.common;
		}
	}

	global->job_ver.local = job_ver->local;
	global->local_job_ver_update =
	    (prev_job_ver->local < job_ver->local && prev_job_ver->local > 0);
	global->prev_local_job_ver = prev_job_ver->local;

	log(NOTICE, "Using %s job version: local=0x%x, common=0x%x", context,
	    job_ver->common, job_ver->local);
	log_job_version_ufp(&global->job_ver, SIQ_TMONITOR, context);

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	/* This is true if we loaded a target record from disk that had the
	 * 5.0 policy id, and re-saved it with the actual post 5.0 policy id.
	 * We need to get rid of the now unused 5.0 target record */
	if (delete_5_0_record) {
		ASSERT(polid_5_0 != NULL);
		ASSERT(lock_fd_5_0 != -1);

		siq_target_delete_record(polid_5_0, true, true, &error);
		if (error)
			goto out;
	}

	global->wi_lock_mod = tim->wi_lock_mod;

	/* Create the target side work item locking
	 * directories for this policy */
	create_wi_locking_dir(tim->policy_id, global->wi_lock_mod, &error);
	if (error)
		goto out;

	create_syncrun_file(tim->policy_id, tim->run_id, &error);
	UFAIL_POINT_CODE(fail_syncrun_create,
	    error = isi_system_error_new(EIO,"sync run fail"));
	if (error)
		goto out;

	/*
	 * Create tmp working directory during incremental syncs.
	 * We are probably breaking the convention by creating 
	 * directory before auth callbacks. But given that we already
	 * do some other operations like creating sync run file, 
	 * we should be not be making it worse.
	 */
	if (stf_sync && !initial_sync && (tim->option == OPTION_INIT) &&
	    (!failback_prep || (failback_prep && (domain_mark || restore)))) {
		global->enable_hash_tmpdir = trec->enable_hash_tmpdir;
		tmp_fd = initialize_tmp_map(tim->target_dir, tim->policy_id,
		    &sw_ctx->global, restore, true, &error);
		if (error) {
			ASSERT(tmp_fd < 0);
			goto out;
		}

		ASSERT(tmp_fd >= 0);
		close(tmp_fd);
	}

	signal(SIGUSR2, tm_sigusr2);
	
	/* Everything looks ok, send response to the coordinator */
	msg_send(coord_fd, resp);

	//if (!failback_prep) {
		migr_register_timeout(&cancel_scan_interval, scan_for_cancel, 
		    sw_ctx);
	//}

 out:
	if (lock_fd != -1)
		close(lock_fd);

	if (lock_fd_5_0 != -1)
		close(lock_fd_5_0);

	if (resync_ctx != NULL)
		close_lin_list(resync_ctx);

	siq_target_record_free(trec);

	if (error) {
		log(ERROR, "Error initializing target monitor: %s",
		    isi_error_get_message(error));
		if (global->job_ver.common < MSG_VERSION_CHOPU) {
			resp->body.old_target_resp.error =
			    (char *)isi_error_get_message(error);
		} else if (global->job_ver.common < MSG_VERSION_RIPTIDE) {
			resp->body.old_target_resp2.error =
			    (char *)isi_error_get_message(error);
			if (isi_error_is_a(error, ISI_SIQ_ERROR_CLASS)) {
				resp->body.old_target_resp2.domain_generation
				    = isi_siq_error_get_siqerr(
				    (struct isi_siq_error *)error);
			} else {
				resp->body.old_target_resp2.domain_generation
				    = 0;
			}
		} else {
			ript_msg_set_field_str(&resp->body.ript, RMF_ERROR, 1,
			    (char *)isi_error_get_message(error));
			ript_msg_set_field_uint32(&resp->body.ript,
			    RMF_DOMAIN_GENERATION, 1,
			    isi_error_is_a(error, ISI_SIQ_ERROR_CLASS) ?
			    isi_siq_error_get_siqerr(
			    (struct isi_siq_error *)error) : 0);
		}
		msg_send(coord_fd, resp);
		tmonitor_cleanup(tm_ctx);
	}
}
