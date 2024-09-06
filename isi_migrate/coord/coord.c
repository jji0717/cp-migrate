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
#include <isi_domain/dom_util.h>

#include "isi_snapshot/snapshot.h"

#include "coord.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_alert.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/migr/comp_conflict_log.h"
#include "isi_migrate/siq_error.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/selectors.h"
#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/lin_list.h"
#include "isi_migrate/migr/compliance_map.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/sched/siq_jobctl.h"
#include "isi_sra_gcfg/isi_sra_gcfg.h"
#include "isi_sra_gcfg/dbmgmt.h"
#include "isi_cpool_cbm/isi_cbm_file_versions.h"

#define BW_TH_TIMEOUT 900 /* 15 min */

enum safe_copy_results {
	SAFE_COPY_SUCCESS = 0,
	SAFE_COPY_ERROR,
	SAFE_COPY_NO_SRC_FILE,
};

static struct timeval jobctl_timeout = {3, 0};
static struct timeval bwt_timeout = {10, 0};

struct coord_ctx g_ctx;

static int	timer_callback(void *ctx);////redistribute_workers中设置好g_ctx.workers,调用timer_callback,立刻去connect_worker,创建pworker子进程
static void	reallocate_bandwidth(void);/////coord.c: connect_bandwidth_host中注册的回调函数.
static void	reallocate_throttle(void);
static void 	cleanup_post_assess(void);
static void	disable_policy(char *pol_id);


/*  _   _ _   _ _     
 * | | | | |_(_) |___ 
 * | | | | __| | / __|
 * | |_| | |_| | \__ \
 *  \___/ \__|_|_|___/
 */

void
fatal_error(bool retry, enum siq_alert_type type, const char *fmt, ...)
{
	va_list args;
	char *msg;
	char *alert_msg = NULL;
	char *error_msg = NULL;
	int ret;

	if (gc_job_summ && gc_job_summ->root && gc_job_summ->root->summary)
		gc_job_summ->root->summary->retry = retry;

	va_start(args, fmt);
	ret = vasprintf(&msg, fmt, args);
	ASSERT(ret != -1);
	va_end(args);

	if (type == SIQ_ALERT_SIQ_CONFIG)
		siq_create_config_alert(&alert_msg, msg);
	else {
		siq_create_alert(type, g_ctx.rc.job_name, g_ctx.rc.job_id,
		    g_ctx.rc.target, &alert_msg, msg);
	}

	error_msg = alert_msg ? alert_msg : msg;

	if (gc_job_summ && gc_job_summ->root && gc_job_summ->root->summary)
		siq_job_error_msg_append(JOB_SUMM, error_msg);
	log(FATAL, "%s", error_msg);
}

void
fatal_error_from_isi_error(bool retry, struct isi_error *error_in)
{
	char *alert_msg = NULL;
	char *error_msg = NULL;

	ASSERT(error_in);
	JOB_SUMM->retry = retry;
	siq_create_alert_from_error(error_in, g_ctx.rc.job_name,
	    g_ctx.rc.job_id, g_ctx.rc.target, &alert_msg);

	error_msg = alert_msg ? alert_msg :
	    (char *)isi_error_get_message(error_in);
	siq_job_error_msg_append(JOB_SUMM, error_msg);
	log(FATAL, "%s", error_msg);
}

static void
sigusr2(int signo)
{
	struct timeval tv = {};

	migr_register_timeout(&tv, dump_status, NULL);
}

static void
update_workers(void)
{
	int i = 0;
	struct worker *w;
	struct siq_worker *worker = NULL;
	
	//Clear all entries
	siq_workers_free(&JOB_SUMM->workers);
	
	if (!g_ctx.workers)
		goto out;
	
	for (i = 0; i < g_ctx.workers->count; i++) {
		CALLOC_AND_ASSERT(worker, struct siq_worker);
		w = g_ctx.workers->workers[i];
		
		worker->worker_id = w->id;
		worker->process_id = w->process_id;
		worker->connected = (w->socket > 0);
		if (w->work) {
			worker->lin = w->work->wi_lin;
		}
		worker->last_split = w->last_split;
		worker->last_work = w->last_work;
		worker->source_host = strdup(w->s_node->host);
		worker->target_host = strdup(w->t_node->host);
		worker->lnn = w->s_node->lnn;
		
		SLIST_INSERT_HEAD(&JOB_SUMM->workers, worker, next);
	}

out:
	return;
}

void
save_report_func(const char *func)
{
	uint32_t failed = 0, delta;
	struct isi_error *error = NULL;

	JOB_SUMM->total->end = time(0);
	if (JOB_SUMM->chunks->total > JOB_SUMM->chunks->running) {
		delta = JOB_SUMM->chunks->total - JOB_SUMM->chunks->running;
		if (delta > JOB_SUMM->chunks->success) {
			failed = delta - JOB_SUMM->chunks->success;
		}
	}
	JOB_SUMM->chunks->failed = failed;
	
	update_workers();
	
	siq_gc_job_summary_save(gc_job_summ, &error);
	if (error) {
		log(ERROR, "Failed to save job %s report under %s, in %s: %s",
		    SIQ_JNAME(&g_ctx.rc), g_ctx.rc.job_dir, func,
		    isi_error_get_message(error));
		isi_error_free(error);
		_exit(EXIT_FAILURE);
	}
}

void
register_timer_callback(int delay)
{
	ilog(IL_INFO, "%s called");
	struct timeval tv = {delay, 0};	

	if (g_ctx.timer)
		goto out;
	
	migr_register_timeout(&tv, timer_callback, 0);
	g_ctx.timer = true;

out:
	return;
}

void
set_job_state(enum siq_job_state state)
{
	char *state1 = NULL;
	char *state2 = NULL;
	struct sra_job_entry *entry = NULL;
	
	state1 = job_state_to_text(JOB_SUMM->total->state, false);
	state2 = job_state_to_text(state, false);
	
	log(DEBUG, "%s: Job state changed from %s to %s", __func__,
	    state1, state2);
	
	free(state1);
	free(state2);

	JOB_SUMM->total->state = state;
	send_status_update_to_tmonitor();

	/*if sra_id is not null, it  means that this is a sra manual
	 * job, sra will make use of the sra job guid and to track the
	 * job status, update the job status in the database
	 */
	if (JOB_SUMM->sra_id != NULL) {
		entry = get_sra_job_entry(JOB_SUMM->sra_id);
		if (entry == NULL) {
			log(ERROR,"Failed to find job entry with"
			    "sra id %s", JOB_SUMM->sra_id);
			return;
		}
		entry->state = state;
		if (write_sra_job(JOB_SUMM->sra_id, entry) <0) {
			// still, log error
			log(ERROR,"Failed to write job entry with"
			    "sra id %s", JOB_SUMM->sra_id);
			return;
		}
	}
}

static void
try_populate_job_summ_on_error(void)
{
	// Try and fill in the job_id (and possibly other values in the
	// future) if the policy fails too soon to fill these values in
	// otherwise.

	struct isi_error* error = NULL;
	uint32_t jid = siq_reports_status_check_inc_job_id(g_ctx.rc.job_id,
	    &error);
	if (error) {
		log(ERROR, "Could not update report status: %s",
		    isi_error_get_message(error));
		goto out;
	}

	JOB_SUMM->job_id = jid;
out:
	return;
}

static bool
do_job_summary_save(void)
{
	struct isi_error *error = NULL;

	if (!JOB_SUMM->job_id) {
		try_populate_job_summ_on_error();
	}

	siq_gc_job_summary_save(gc_job_summ, &error);
	if (error) {
		log(ERROR, "Failed to save job %s report under %s: %s",
		    SIQ_JNAME(&g_ctx.rc), g_ctx.rc.job_dir,
		    isi_error_get_message(error));
		isi_error_free(error);
		return false;
	}

	return true;
}

void
clear_composite_job_ver(struct siq_source_record *srec, bool restore)
{
	if (restore) {
		siq_source_clear_restore_composite_job_ver(g_ctx.record);
	} else {
		siq_source_clear_composite_job_ver(g_ctx.record);
	}
}

void
get_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver)
{
	if (restore) {
		siq_source_get_restore_composite_job_ver(g_ctx.record,
		    job_ver);
	} else {
		siq_source_get_composite_job_ver(g_ctx.record, job_ver);
	}
}

void
get_prev_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver)
{
	if (restore) {
		siq_source_get_restore_prev_composite_job_ver(g_ctx.record,
		    job_ver);
	} else {
		siq_source_get_prev_composite_job_ver(g_ctx.record, job_ver);
	}
}

void
set_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver)
{
	if (restore) {
		siq_source_set_restore_composite_job_ver(g_ctx.record,
		    job_ver);
	} else {
		siq_source_set_composite_job_ver(g_ctx.record, job_ver);
	}
}

void
set_prev_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver)
{
	if (restore) {
		siq_source_set_restore_prev_composite_job_ver(g_ctx.record,
		    job_ver);
	} else {
		siq_source_set_prev_composite_job_ver(g_ctx.record, job_ver);
	}
}

static void
exiting(void)
{
	struct isi_error *error = NULL;
	time_t duration;
	char *state = NULL;
	char *finish_msg = NULL;
	char *conflict_msg = NULL, *conflict_log = NULL;
	struct siq_job_version job_ver;

	UFAIL_POINT_DISABLE();

	if (g_ctx.record == NULL) {
		g_ctx.record = siq_source_record_load(g_ctx.rc.job_id, &error);
		if (error)
			goto out;
	}

	if (gc_job_summ == NULL)
		goto out;

	log(TRACE, "exiting: enter");
	if (!(SIQ_JS(SUCCESS) || SIQ_JS(CANCELLED) || SIQ_JS(PREEMPTED) ||
	    SIQ_JS(PAUSED)))
		set_job_state(SIQ_JS_FAILED);

	/* If there were compliance conflicts, and the job is finishing, then
	 * create an alert guiding users to a log of the conflicts. */
	if (JOB_SUMM->total->stat->compliance->conflicts > 0 &&
	    !(SIQ_JS(PREEMPTED) || SIQ_JS(PAUSED))) {
		ASSERT(g_ctx.comp_conflict_log_dir != NULL);

		finalize_comp_conflict_log(g_ctx.comp_conflict_log_dir,
		    &conflict_log, &error);
		if (error)
			goto out;

		asprintf(&conflict_msg, "A log of compliance mode WORM "
		    "committed file conflicts can be found at %s",
		    conflict_log);
		siq_create_alert(SIQ_ALERT_COMP_CONFLICTS, g_ctx.rc.job_name,
		    g_ctx.rc.job_id, g_ctx.rc.target, NULL, conflict_msg);
	}

	JOB_SUMM->total->end = time(0);
	duration = JOB_SUMM->total->end - JOB_SUMM->total->start;

	if (g_ctx.rc.flags & FLAG_ASSESS) {
		cleanup_post_assess();
	}
	
	state = job_state_to_text(JOB_SUMM->total->state, true);

	if (g_ctx.rc.is_delete_job) {
		asprintf(&finish_msg, "Finished policy delete job '%s' (%s) "
		    "with status %s", SIQ_JNAME(&g_ctx.rc), g_ctx.rc.job_id,
		    state);
		if (!SIQ_JS(SUCCESS)) {
			do_job_summary_save();

			if (siq_source_has_owner(g_ctx.record))
				siq_source_release_ownership(g_ctx.record);
			siq_source_record_save(g_ctx.record, &error);
			if (error)
				goto out;
			siq_source_record_free(g_ctx.record);
			g_ctx.record = NULL;

			siq_job_cleanup(gc_job_summ, g_ctx.rc.job_id,
			    g_ctx.rc.job_dir, &error);
			if (error)
				goto out;
			siq_job_rm_lock_file(g_ctx.rc.job_id);
		}
		goto out;
	}

	asprintf(&finish_msg, "Finished job '%s' (%s) to %s in %ldh %ldm %lds "
	    "with status %s and %d checksum errors", SIQ_JNAME(&g_ctx.rc),
	    g_ctx.rc.job_id, g_ctx.rc.target, duration / 3600,
	    (duration % 3600) / 60, duration % 60, state,
	    JOB_SUMM->total->stat->files->skipped->error_checksum);
	
	/* close file for log deleted files */
	if (g_ctx.rc.fd_deleted > 0)
		close(g_ctx.rc.fd_deleted);

	remove_snapshot_locks();

	/* Bug 54479 - Write more useful error messages if we exit during
	 * loading of the job spec */
	if (g_ctx.rc.conf_load_error != 0) {
		JOB_SUMM->total->start = JOB_SUMM->total->end;
		// Avoid a divide by 0 error
		JOB_SUMM->total->end++;
		siq_job_error_msg_append(JOB_SUMM, "Failed to load job spec");
		if (!do_job_summary_save())
			goto out;
	} else if (g_ctx.rc.job_dir) {
		JOB_SUMM->chunks->running = 0;
		JOB_SUMM->chunks->failed =
		    (JOB_SUMM->chunks->total - JOB_SUMM->chunks->success > 0 ?
		    JOB_SUMM->chunks->total - JOB_SUMM->chunks->success : 0);
		if (!do_job_summary_save())
			goto out;
	}

	if (((g_ctx.initial_sync && g_ctx.stf_sync) || g_ctx.stf_upgrade_sync)
	    && JOB_SUMM->total->state == SIQ_JS_SUCCESS) {
		/* record successful stf initial or upgrade sync in source
		 * record so we don't attempt an upgrade next time */
		log(DEBUG, "STF upgrade complete");
		siq_source_set_pol_stf_ready(g_ctx.record);
	}
	
	if (JOB_SUMM->total->state == SIQ_JS_SUCCESS) {
		// Successful run, increment job_id
		siq_reports_status_set_inc_job_id(g_ctx.rc.job_id, &error);
		if (error) {
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
		}

		// XXX Review: should this be limited to certain scenarios,
		//     e.g. JOB_SUMM->sync_type == LEG | INIT | INCR | UPGR ?
		//
		// Slide the job version window forward. Also, if it's a
		// non-restore job then set both restore versions to the
		// current non-restore version so any future restore job
		// will operate at a matching version (and without any
		// indication of upgrade).
		get_composite_job_ver(g_ctx.record,
		    g_ctx.use_restore_job_vers, &job_ver);
		if (job_ver.local > 0 && job_ver.common > 0) {
			set_prev_composite_job_ver(g_ctx.record,
			    g_ctx.use_restore_job_vers, &job_ver);
			clear_composite_job_ver(g_ctx.record,
			    g_ctx.use_restore_job_vers);
			if (!g_ctx.use_restore_job_vers) {
				set_composite_job_ver(g_ctx.record,
				    true, &job_ver);
				set_prev_composite_job_ver(g_ctx.record,
				    true, &job_ver);
			}
		}
	}

	if (siq_source_has_owner(g_ctx.record))
		siq_source_release_ownership(g_ctx.record);
	siq_source_record_save(g_ctx.record, &error);
	if (error)
		goto out;
	siq_source_record_free(g_ctx.record);
	g_ctx.record = NULL;

	/* standard synciq policy cleanup */
	siq_job_cleanup(gc_job_summ, g_ctx.rc.job_id, g_ctx.rc.job_dir,
	    &error);
	if (error)
		goto out;

out:
	if (state)
		free(state);

	if (conflict_msg)
		free(conflict_msg);

	if (conflict_log)
		free(conflict_log);

	siq_source_record_free(g_ctx.record);

	isi_cbm_file_versions_destroy(g_ctx.common_cbm_file_vers);
	if (error) {
		log(ERROR, "%s: %s", __func__, isi_error_get_message(error));
		isi_error_free(error);
	}
	siq_gc_spec_free(gc_job_spec);
	if (finish_msg) {
		log(NOTICE, "%s", finish_msg);
		free(finish_msg);
	}
}

static int
snap_resp_callback(struct generic_msg *m, void *ctx)
{
	struct coord_ctx *coord_ctx = (struct coord_ctx *) ctx;

	coord_ctx->waiting_for_tgt_msg = false;

	if (m->body.snap_resp.result == SNAP_MSG_RESULT_ERROR) {
		 log(ERROR, "Secondary: %s", m->body.snap_resp.msg);
		siq_create_alert(SIQ_ALERT_TARGET_SNAPSHOT,
		    g_ctx.rc.job_name, g_ctx.rc.job_id, g_ctx.rc.target, NULL,
		    m->body.snap_resp.msg);
		goto out;
	} else if (m->body.snap_resp.result == FAILOVER_SNAP_MSG_RESULT_ERROR) {
		fatal_error(false, SIQ_ALERT_TARGET_SNAPSHOT,
		    m->body.snap_resp.msg);
		goto out;
	} else if (m->body.snap_resp.result == SNAP_MSG_RESULT_OK) {
		if (g_ctx.rc.disable_fofb == false) {
			log(TRACE, "Snapshot %s created on target",
			    m->body.snap_resp.msg);
			siq_snapshots_target_append(JOB_SUMM,
			    m->body.snap_resp.msg);
		}
	} else if (m->body.snap_resp.result == SNAP_MSG_RESULT_SKIP) {
		log(INFO, "Secondary skipping snapshot creation");
	} else {
		log(FATAL, "Unknown snapshot result %d",
		    m->body.snap_resp.result);
	}

	/* if a prior job set an alert, cancel it since we have
	 * succeeded */
	siq_cancel_alert(SIQ_ALERT_TARGET_SNAPSHOT, g_ctx.rc.job_name,
	    g_ctx.rc.job_id);

out:
	/* Need to stop migr_process(), but still need connection to tmonitor
	 * left open. */
	migr_force_process_stop();

	return 0;
}

static int
send_snap_msg(char *name, char *path, unsigned expiration, char *alias)
{
	struct generic_msg snapmsg;
	struct isi_error *error = NULL;

	if (path == NULL)
		return EINVAL;

	migr_register_callback(g_ctx.tctx.sock, SNAP_RESP_MSG,
	    snap_resp_callback);
	log(TRACE, "Send snap create msg, name=%s, path=%s",
	    name ? name : "failover snap", path);
	snapmsg.head.type = SNAP_MSG;
	snapmsg.body.snap.name = name;
	snapmsg.body.snap.alias = alias;
	snapmsg.body.snap.path = path;
	snapmsg.body.snap.expiration = expiration;
	msg_send(g_ctx.tctx.sock, &snapmsg);
	isi_error_free(error);
	return 0;
}

void
do_target_snapshot(char *snapshot_name)
{
	int i, r;

	log(TRACE, "%s(%s)", __func__,
	    snapshot_name ? snapshot_name : "failover snap");
	g_ctx.waiting_for_tgt_msg = true;

	if (g_ctx.rc.disable_fofb && snapshot_name == NULL) {
		/* just skip this step for initial syncs if FOFB is
		 * disabled */
		if (g_ctx.initial_sync)
			goto out;

		/* for update syncs, get the target to delete an existing
		 * FOFB snapshot if one exists. the FOFB disable flag can
		 * be turned on and off, and we don't track target FOFB
		 * snapshot status on the source, so just send the delete
		 * instruction */
		snapshot_name = FOFB_SNAP_DEL;
	}

	for (i = 0; i < MAX_TGT_TRIES && g_ctx.waiting_for_tgt_msg; i++) {
		if ((r = send_snap_msg(snapshot_name, g_ctx.rc.targetpath,
		    g_ctx.rc.expiration, g_ctx.rc.snapshot_alias))) {
			switch (r) {
			case ENOTSUP:
				log(ERROR, "Target snapshots "
				    "are not supported");
				break;
			case EINVAL:
				log(ERROR, "Invalid target "
				    "snapshot name or path");
				break;
			}
			break;
		}
		if (migr_process() < 0)
			set_job_state(SIQ_JS_FAILED);
	}

	if (i == MAX_TGT_TRIES) {
		if (!snapshot_name) {
			// failover/failback at least one reference snap
			fatal_error(false, SIQ_ALERT_TARGET_SNAPSHOT,
			    "Can't create snapshot at target: network "
			    "problems.");
		} else {
			log(ERROR, "Can't create snapshot at target: network "
			    "problems.");
		}
	}

out:
	return;
}

/**
 * Receive an GENERIC_ACK message for UPGRADE_COMPLETE_MSG
 */
int
task_complete_ack_callback(struct generic_msg *m, void *ctx)
{
	struct coord_ctx *coord_ctx = (struct coord_ctx *) ctx;
	coord_ctx->waiting_for_tgt_msg = false;
	migr_force_process_stop();
	return 0;
}

static void
target_upgrade_complete(void)
{
	int i;
	struct generic_msg m = {};

	log(TRACE, "target_upgrade_complete");
	g_ctx.waiting_for_tgt_msg = true;
	for (i = 0; i < MAX_TGT_TRIES && g_ctx.waiting_for_tgt_msg; i++) {
		migr_register_callback(g_ctx.tctx.sock, GENERIC_ACK_MSG,
		    task_complete_ack_callback);
		m.head.type = UPGRADE_COMPLETE_MSG;
		m.body.upgrade_complete.domain_id = g_ctx.rc.domain_id;
		m.body.upgrade_complete.domain_generation =
		    g_ctx.rc.domain_generation;
		msg_send(g_ctx.tctx.sock, &m);
		if (migr_process() < 0) {
			set_job_state(SIQ_JS_FAILED);
			return;
		}

	}

	if (i == MAX_TGT_TRIES) {
		fatal_error(true, SIQ_ALERT_POLICY_UPGRADE,
		    "Unable to get upgrade complete ack from target: %s",
		    g_ctx.rc.target);
	}
}

void
target_cleanup_complete(void)
{
	int i;
	struct generic_msg m = {};

	log(TRACE, "target_cleanup_complete");
	g_ctx.waiting_for_tgt_msg = true;
	for (i = 0; i < MAX_TGT_TRIES && g_ctx.waiting_for_tgt_msg; i++) {
		migr_register_callback(g_ctx.tctx.sock, GENERIC_ACK_MSG,
		    task_complete_ack_callback);
		m.head.type = CLEANUP_TARGET_TMP_FILES_MSG;
		m.body.cleanup_target_tmp_files.policy_id = g_ctx.rc.job_id;
		m.body.cleanup_target_tmp_files.tgt_path = g_ctx.rc.targetpath;
		msg_send(g_ctx.tctx.sock, &m);
		if (migr_process() < 0) {
			set_job_state(SIQ_JS_FAILED);
			return;
		}
	}

	if (i == MAX_TGT_TRIES) {
		fatal_error(true, SIQ_ALERT_TARGET_FS,
		    "Unable to complete tmp-working-dir cleanup at target");
	}
}

/*
 * "schedules" must be a NULL-terminated list of strings.
 *
 * If any of the strings in the argument list match the policy's recurring
 * schedule, the index of the matching argument is returned; index 0
 * is the first string in the char** list. Otherwise, -1 is returned.
 */
static int
is_sched_eq(char **schedules)
{
	int result = -1, i = 0;
	char *recur_buf = NULL;
	char *recur_ptr;
	struct isi_error *error = NULL;
	struct siq_policy *pol = NULL;
	struct siq_policies *pl = NULL;

	if (g_ctx.rc.action != SIQ_ACT_SYNC &&
	    g_ctx.rc.action != SIQ_ACT_REPLICATE)
		goto out;

	pl = siq_policies_load_readonly(&error);
	if (error) {
		log (ERROR, "Failed to load policies: %s",
		    isi_error_get_message(error));
		goto out;
	}

	pol = siq_policy_get_by_pid(pl, g_ctx.rc.job_id);
	if (pol == NULL)
		goto out;

	recur_buf = strdup(pol->scheduler->schedule);
	ASSERT(recur_buf);

	/* Manual policy */
	if (strcmp(recur_buf, "") == 0)
		goto out;

	recur_ptr = strchr(recur_buf, '|');
	if (recur_ptr == NULL) {
		log(ERROR, "illegal format for recurring date: %s", recur_buf);
		goto out;
	}
	recur_ptr++;

	for (i = 0; schedules[i] != NULL; i++) {
		if (strcasecmp(recur_ptr, schedules[i]) == 0) {
			result = i;
			goto out;
		}
	}

out:
	free(recur_buf);
	siq_policies_free(pl);
	isi_error_free(error);

	return result;
}

int
jobctl_callback(__unused void *ctx)
{
	int ret = 0;
	char *special_scheds[] = {SCHED_ALWAYS, SCHED_SNAP, NULL};
	char *state_str = NULL;
	enum siq_job_state state = SIQ_JS_NONE;

	state_str = job_state_to_text(JOB_SUMM->total->state, false);
	log(TRACE, "jctl@%ld, %s, done %d conn workers %d",
	    (long)time(0), state_str, g_ctx.done,
	    g_ctx.workers ? g_ctx.workers->connected : 0);
	free(state_str);

	if (!SIQ_JS(RUNNING))
		goto out;

	if (g_ctx.done) {
		log(INFO, "Job %s complete", SIQ_JNAME(&g_ctx.rc));
		/* Need to stop migr_process(), but still need connection to
		 * tmonitor left open. */
		migr_force_process_stop();
		goto out;
	}

	if (SIQ_JS_CANCELLED == (state = get_job_state(g_ctx.rc.job_dir))) {
		/* Disable change-based and snapshot-triggered policies on
		 * cancel, otherwise another job instance will start
		 * immediately. */
		ret = is_sched_eq(special_scheds);
		if (ret >= 0) {
			disable_policy(g_ctx.rc.job_id);
			log(INFO, "Disabled policy %s with special schedule: "
			    "\"%s\" per cancellation request",
			    JOB_SPEC->common->name, special_scheds[ret]);
		}
		set_job_state(SIQ_JS_CANCELLED);
		log(INFO, "Job %s is canceled", SIQ_JNAME(&g_ctx.rc));
		exit(EXIT_FAILURE);
	}

	if (SIQ_JS_PREEMPTED == state) {
		set_job_state(SIQ_JS_PREEMPTED);
		log(INFO, "Job %s is preempted by a higher priority job",
		    SIQ_JNAME(&g_ctx.rc));
		exit(EXIT_FAILURE);
	}

	if (SIQ_JS_PAUSED == state) {
		set_job_state(state);
		log(NOTICE, "Job %s paused", SIQ_JNAME(&g_ctx.rc));
		exit(EXIT_FAILURE);
	}

	save_report();

	ASSERT(SIQ_JS(RUNNING) && !g_ctx.done);

	migr_register_timeout(&jobctl_timeout, jobctl_callback, 0);

out:
	return 0;
}

////当coord_worker.c: redistribute_workers重新设置好g_ctx.workers后,需要立刻注册定时器调用connect_worker去fork pworker子进程
///如果没创建pworker成功,重新注册定时器,再次连接
{
int
timer_callback(void *ctx)
	struct worker *w;
	int i;
	bool reschedule = false;
	bool new_connects = false;

	ilog(IL_INFO, "timer_callback called", __func__);

	g_ctx.timer = false;

	if (g_ctx.done)
		goto out;
	
	if (SIQ_JS(PAUSED)) {
		reschedule = true;
		goto out;
	}
	
	shuffle_offline_workers();

	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];

		if (w->socket != -1)
	       		continue;

		connect_worker(w);/////连接pworker daemon,pworker daemon会fork daemon进程
		if (w->socket == -1)
			reschedule = true;
		else
			new_connects = true;
	}

	if (new_connects) {
		reallocate_throttle();
		reallocate_bandwidth();
	}

out:
	if (reschedule)
		register_timer_callback(TIMER_DELAY);//////5s后再次尝试connect_worker(让pworker去fork 一个子进程)
	return 0;
}

static void
lnn_list_to_char(void)
{
	int i;
	int curr;

	if (g_ctx.init_node_mask != NULL)
		free(g_ctx.init_node_mask);
	g_ctx.init_node_mask = malloc(SIQ_MAX_NODES + 1);
	ASSERT(g_ctx.init_node_mask);
	init_node_mask(g_ctx.init_node_mask, 'F', SIQ_MAX_NODES + 1);

	for (i = 0; i < g_ctx.work_lnn_list_len; i++) {
		//Recall that LNN lists node numbers from 1 and not 0
		//But to_return is 0 indexed.
		curr = g_ctx.work_lnn_list[i];
		g_ctx.init_node_mask[curr - 1] = '.';
	}

	log(TRACE, "%s: to_return = %s", __func__, g_ctx.init_node_mask);

	free(g_ctx.disconnected_node_mask);
	g_ctx.disconnected_node_mask = NULL;
}

static void
send_updated_node_mask(void)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = &msg.body.ript;

	if (g_ctx.work_host == -1)
		return;

	msg.head.type = build_ript_msg_type(WORKER_NODE_UPDATE_MSG);
	ript_msg_init(ript);

	lnn_list_to_char();
	ript_msg_set_field_str(ript, RMF_NODE_MASK, 1, g_ctx.init_node_mask);

	msg_send(g_ctx.work_host, &msg);
}

int
group_callback(void *ctx)
{
	/* don't attempt to reconfigure workers if we haven't done initial
	 * configuration yet, i.e. in temporary migr_process loop */
	if (g_ctx.workers_configured == false)
		goto out;

	/* Dynamically reallocate workers in case of source group change */
	get_source_ip_list();

	/* If group change did not affect node configuration, do nothing */
	if (ip_lists_match(g_ctx.src_nodes->count, g_ctx.src_nodes->ips,
	    g_ctx.new_src_nodes->count, g_ctx.new_src_nodes->ips)) {
		cleanup_node_list(g_ctx.new_src_nodes);
		g_ctx.new_src_nodes = NULL;
	} else {
		/* Recalculate the init_node_mask and send to the bandwidth
		 * daemon. */
		send_updated_node_mask();

		configure_workers(false);
	}

out:
	return 0;
}

static int
bandwidth_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "bandwidth_callback: bandwidth ration updated from %d to %d",
	    g_ctx.bw_ration, m->body.bandwidth.ration);
	g_ctx.bw_ration = m->body.bandwidth.ration;   /////当前这个job分到的ration(master bandwidth传送过来的)
	reallocate_bandwidth();

	return 0;
}

static int
throttle_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "throttle_callback: throttle ration updated from %d to %d",
	    g_ctx.th_ration, m->body.throttle.ration);
	g_ctx.th_ration = m->body.throttle.ration;
	reallocate_throttle();

	return 0;
}

static int
worker_callback(struct generic_msg *m, void *ctx)///////master bandwidth回调给coord
{
	struct siq_ript_msg *ript = &m->body.ript;
	char *allowed_nodes = NULL;
	int ret = -1;
	uint32_t max_allowed_workers;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(str, ript, RMF_ALLOWED_NODES, 1, &allowed_nodes,
	    &error, out)
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_MAX_ALLOWED_WORKERS, 1,  ///////master bandwidth分给每个job的ration
	    &max_allowed_workers, &error, out)

	if (g_ctx.work_node_mask != NULL) {
		free(g_ctx.work_node_mask);
	}

	/* Force the situation for bug 153553 - the worker daemon has sent us
	 * an increased ration but an all 'F' node mask. */
	UFAIL_POINT_CODE(increase_workers_no_available_nodes,
	    memset(allowed_nodes, 'F', strlen(allowed_nodes));
	    max_allowed_workers = g_ctx.wk_ration + 2;
	    );

	/*
	 * Bug 153553.
	 * The coordinator only sends an worker count update to the worker
	 * daemon every 10 seconds. So if the worker daemon does multiple
	 * recalculations within 10 seconds, and the coordinator is in the
	 * process of killing workers, it could think that the coordinator
	 * has more workers than it actually does. So it could send us a
	 * ration that it thinks is lower than the amount of workers we have,
	 * when it is actually greater. When the worker daemon asks a job to
	 * increase its ration, it always sends a node mask of all '.'s.
	 * So to avoid the error case, we can assume that if we are asked to
	 * increase our ration, the correct node mask is all '.'s. Otherwise,
	 * we should used the mask the worker daemon sent to figure out which
	 * workers to kill.
	 */
	if (max_allowed_workers >= g_ctx.wk_ration)
		g_ctx.work_node_mask = strdup(default_node_mask);
	else
		g_ctx.work_node_mask = strdup(allowed_nodes);
	g_ctx.wk_ration = max_allowed_workers;    ////这个job允许运行的最大ration值

	log(DEBUG, "%s: max num workers: %d.  Node mask: %s", __func__,
	    max_allowed_workers, allowed_nodes);
	if (g_ctx.workers_configured) {
		reallocate_worker();
	}

	ret = 0;

out:
	if (error) {
		log(FATAL, "%s: Error parsing ript msg: %s", __func__,
		    isi_error_get_message(error));
	}

	return ret;
}

void//////coord拿到master bandwidth的ration,并且更新后,把ration传给每个pworker
update_bandwidth(struct worker *w, bool force)
{
	struct generic_msg msg = { .head.type = BANDWIDTH_MSG };

	/* unless this is a forced update, we only update if the value has
	 * changed for the worker */
	if (w->socket != -1 &&
	    (force || (w->bw_ration != w->bw_last_sent))) {
		w->bw_last_sent = w->bw_ration;
		msg.body.bandwidth.ration = w->bw_ration;
		msg_send(w->socket, &msg);
	}
}

static void
reallocate_bandwidth()/////一个coord对应一个job.将这个job获得的ration平均分给这个job中的每一个worker
{
	int i;
	int active, idle, adjusted_ration;
	int quotient, remainder;
	struct worker *w = NULL;

	log(TRACE, "reallocate_bandwidth");

	/* Skip if we haven't received workers from the worker daemon. */
	if (g_ctx.workers->count == 0)
		return;

	if (g_ctx.bw_ration > 0) {
		quotient = g_ctx.bw_ration / g_ctx.workers->count;///////当前这个job分到的bw_ration / 当前这个job中的workers数量
		remainder = g_ctx.bw_ration % g_ctx.workers->count;

		/* prevent starvation. if ration is less than worker count, not
		 * all workers would get a ration. in that case, round up so
		 * each worker gets a single unit */
		if (quotient == 0) {
			quotient = 1;
			remainder = 0;
		}
	} else if (g_ctx.bw_ration == 0) {
		quotient = 0;
		remainder = 0;
	} else {
		quotient = -1;
		remainder = 0;
	}

	active = queue_size(&splittable_queue) +
	    queue_size(&split_pending_queue);	
	idle = queue_size(&idle_queue) +
	    queue_size(&offline_queue);

	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		w->bw_ration = quotient + (remainder > 0);
		if (remainder > 0)
			remainder--;

		if (active == 0 || idle == 0 || w->work == NULL)
		 	continue;

		/* cheap dynamic allocation: if there are idle/offline workers, //////优化的地方
		 * give active workers an inflated ration. basically, instead
		 * of every worker getting (ration/num_workers), active ones
		 * get (ration/num_active). we still give idle/offile workers
		 * the normal ration so they don't stall right after becoming
		 * active */
		adjusted_ration = w->bw_ration;
		if (w->bw_ration >= 0) {
			adjusted_ration *= (active + idle); ////adjusted_ration = adjusted_ration *[(active+idle)/active]
			adjusted_ration /= active; /////相当于 8个active,2个idle,那么adjusted_ration = ctx->ration / 10 * (10/8)
		}
		log(DEBUG, "worker %d bandwidth ration bumped from %d to %d",
		    w->id, w->bw_ration, adjusted_ration);
		w->bw_ration = adjusted_ration;
	}

	for (i = 0; i < g_ctx.workers->count; i++)
		update_bandwidth(g_ctx.workers->workers[i], false);
}

void
update_throttle(struct worker *w, bool force)
{
	struct generic_msg msg = { .head.type = THROTTLE_MSG };

	/* unless this is a forced update, we only update if the value has
	 * changed for the worker */
	if (w->socket != -1 &&
	    (force || (w->th_ration != w->th_last_sent))) {
		w->th_last_sent = w->th_ration;
		msg.body.throttle.ration = w->th_ration;
		msg_send(w->socket, &msg);
	}
}

static void
reallocate_throttle()
{
	int i;
	int quotient = -1, remainder = 0;

	log(TRACE, "reallocate_throttle");

	/* Skip if we haven't received workers from the worker daemon. */
	if (g_ctx.workers->count == 0)
		return;

	if (g_ctx.th_ration > 0) {
		quotient = g_ctx.th_ration / g_ctx.workers->count;
		remainder = g_ctx.th_ration % g_ctx.workers->count;

		/* prevent starvation. if ration is less than worker count, not
		 * all workers would get a ration. in that case, round up so
		 * each worker gets a single unit */
		if (quotient == 0) {
			quotient = 1;
			remainder = 0;
		}
	} else if (g_ctx.th_ration == 0) {
		quotient = 0;
		remainder = 0;
	} else {
		quotient = -1;
		remainder = 0;
	}

	for (i = 0; i < g_ctx.workers->count; i++) {
		g_ctx.workers->workers[i]->th_ration = quotient + (remainder > 0);
		if (remainder > 0)
			remainder--;
	}

	for (i = 0; i < g_ctx.workers->count; i++)
		update_throttle(g_ctx.workers->workers[i], false);
}

void
reallocate_worker()
{
	log(TRACE, "reallocate_worker");

	//Create or destroy workers as directed
	redistribute_workers(NULL);
}

static int
bandwidth_disconnect_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "bandwidth_disconnect_callback");

	/* cut bandwidth until we reconnect to host and receive a grant*/
	migr_rm_fd(g_ctx.bw_host);
	close(g_ctx.bw_host);
	g_ctx.bw_ration = 0;
	g_ctx.bw_host = -1;
	reallocate_bandwidth();
	g_ctx.bw_last_conn = time(0);

	connect_bandwidth_host();

	return 0;
}

static int
throttle_disconnect_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "throttle_disconnect_callback");

	/* cut throttle until we reconnect to host and receive a grant*/
	migr_rm_fd(g_ctx.th_host);
	close(g_ctx.th_host);
	g_ctx.th_ration = 0;
	g_ctx.th_host = -1;
	reallocate_throttle();
	g_ctx.th_last_conn = time(0);

	connect_throttle_host();

	return 0;
}

static int
worker_disconnect_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "worker_disconnect_callback");

	/* cut workers until we reconnect to host and receive a grant*/
	migr_rm_fd(g_ctx.work_host);
	close(g_ctx.work_host);
	g_ctx.work_fast_start_count = 1;
	g_ctx.work_fast_kill_count = 1;
	g_ctx.wk_ration = 0;
	g_ctx.work_host = -1;
	if (g_ctx.work_node_mask) {
		free(g_ctx.work_node_mask);
		g_ctx.work_node_mask = NULL;
	}
	if (g_ctx.work_lnn_list) {
		free(g_ctx.work_lnn_list);
		g_ctx.work_lnn_list = NULL;
		g_ctx.work_lnn_list_len = 0;
	}
	if (g_ctx.init_node_mask) {
		free(g_ctx.init_node_mask);
		g_ctx.init_node_mask = NULL;
	}
	reallocate_worker();
	g_ctx.work_last_conn = time(0);

	connect_worker_host();

	return 0;
}

int
bwt_update_timer_callback(void *ctx)/////coord定时给bw_host发送"job_total",给work_host发送g_ctx.workers->count
{
	struct generic_msg bw_msg = {};
	struct generic_msg th_msg = {};
	struct generic_msg GEN_MSG_INIT_CLEAN(work_msg);
	int i;
	struct node *s_node;
	char job_total[] = "job_total";
	unsigned long long transferred = 0;
	unsigned int total = 0;
	bool treewalk;
	int active_workers;

	ilog(IL_INFO, "bwt_update_timer_callback called");

	bw_msg.head.type = BANDWIDTH_STAT_MSG;
	th_msg.head.type = THROTTLE_STAT_MSG;
	work_msg.head.type = build_ript_msg_type(WORKER_STAT_MSG);

	/* try to connect to the hosts if the connections were lost. for
	 * bandwidth, only connect if the host was specified */
	if (g_ctx.bw_host == -1 && g_ctx.rc.bwhost)
		connect_bandwidth_host();
	if (g_ctx.th_host == -1)
		connect_throttle_host();
	if (g_ctx.work_host == -1)
		connect_worker_host();

	/* calculate bytes sent and files sent deltas since last update and
	 * send them to the bandwidth and throttle hosts */

	if (g_ctx.bw_host == -1 && g_ctx.th_host == -1 && g_ctx.work_host == -1)
		goto out;

	/* determine which stats to use */
	treewalk = (JOB_SUMM->sync_type == ST_INITIAL ||
	    JOB_SUMM->sync_type == ST_UPGRADE ||
	    JOB_SUMM->sync_type == ST_LEGACY);

	/* first send bw/th stats for the job */
	if (g_ctx.bw_host != -1) {
		bw_msg.body.bandwidth_stat.name = job_total;
		transferred = treewalk ?
		    JOB_SUMM->total->stat->bytes->transferred :
		    JOB_SUMM->total->stat->bytes->network->total;
		bw_msg.body.bandwidth_stat.bytes_sent =
		    transferred - g_ctx.bw_last;
		g_ctx.bw_last = transferred;
		msg_send(g_ctx.bw_host, &bw_msg);
	}

	if (g_ctx.th_host != -1) {
		th_msg.body.throttle_stat.name = job_total;
		total = JOB_SUMM->total->stat->files->total;
		th_msg.body.throttle_stat.files_sent = total - g_ctx.th_last;
		g_ctx.th_last = total;
		msg_send(g_ctx.th_host, &th_msg);
	}

	if (g_ctx.work_host != -1) {
		ript_msg_init(&work_msg.body.ript);
		ript_msg_set_field_uint32(&work_msg.body.ript,
		    RMF_WORKERS_USED, 1, (uint32_t)g_ctx.workers->count);
		ript_msg_set_field_int32(&work_msg.body.ript,
		    RMF_REQUESTED_WORKERS, 1, g_ctx.total_wpn);/////不考虑,每个node中最多workers数量,配置中删除 每个node上最多的workers * node的个数
		msg_send(g_ctx.work_host, &work_msg);
	}

	/* now send individual bw/th stat messages for each node */
	for (i = 0; i < g_ctx.src_nodes->count; i++) {
		s_node = g_ctx.src_nodes->nodes[i];
		ASSERT(s_node->host);

		if (g_ctx.bw_host != -1) {
			bw_msg.body.bandwidth_stat.name = s_node->host;
			bw_msg.body.bandwidth_stat.bytes_sent =
			    s_node->bytes_sent - s_node->bytes_sent_last;
			s_node->bytes_sent_last = s_node->bytes_sent;
			msg_send(g_ctx.bw_host, &bw_msg);
		}

		if (g_ctx.th_host != -1) {
			th_msg.body.throttle_stat.name = s_node->host;
			th_msg.body.throttle_stat.files_sent =
			    s_node->files_sent - s_node->files_sent_last;
			s_node->files_sent_last = s_node->files_sent;
			msg_send(g_ctx.th_host, &th_msg);
		}
	}

	/* if active/idle worker counts changed, reallocate bandwidth ration */
	active_workers = queue_size(&splittable_queue) +
	    queue_size(&split_pending_queue);
	if (active_workers != g_ctx.active_workers) {
		reallocate_bandwidth();
		g_ctx.active_workers = active_workers;
	}

out:
	if (!g_ctx.done)
		migr_register_timeout(&bwt_timeout,
		    bwt_update_timer_callback, 0);

	return 0;
}

static void
quorum_handler(int sig)
{
	log(ERROR, "Lost quorum, exiting without failure");

	/* Remove snapshot locks. Don't worry about errors, they could already
	 * be unlocked depending on where the quorum interrupt occured. */
	if (g_ctx.curr_snap.snap != NULL &&
	    g_ctx.curr_snap.lock > 0)
		snapshot_rmlock(g_ctx.curr_snap.snap, &g_ctx.curr_snap.lock,
		    NULL);

	if (g_ctx.prev_snap.snap != NULL &&
	    g_ctx.prev_snap.lock > 0)
		snapshot_rmlock(g_ctx.prev_snap.snap, &g_ctx.prev_snap.lock,
		    NULL);

	/* No need to fail the job by going through the exiting func */
	_exit(-1);
}


/*  ____  _        _             
 * / ___|| |_ __ _| |_ _   _ ___ 
 * \___ \| __/ _` | __| | | / __|
 *  ___) | || (_| | |_| |_| \__ \
 * |____/ \__\__,_|\__|\__,_|___/
 */

static int
sigusr1_timer(void *ctx)
{
	int serr = errno;

	if (!g_ctx.rc.job_dir)
		return 0;

	save_report();
	errno = serr;

	return 0;
}

static void
sigusr1(int signo)
{
	struct timeval tv = {};

	migr_register_timeout(&tv, sigusr1_timer, TIMER_SIGNAL);
}

static void
disable_policy(char *pol_id)
{
	struct siq_policies *pl = NULL;
	struct siq_policy *pol = NULL;
	struct isi_error *error = NULL;
	bool succeeded = false;

	ASSERT(pol_id);

	pl = siq_policies_load(&error);
	if (pl == NULL)
		goto out;

	pol = siq_policy_get_by_pid(pl, pol_id);
	if (pol == NULL)
		goto out;

	pol->common->state = SIQ_ST_OFF;

	if (siq_policies_save(pl, &error) != 0)
		goto out;

	succeeded = true;

out:
	if (!succeeded) {
		log(ERROR, "Failed to disable policy %s%s%s", pol_id,
		    error ? ": " : "",
		    error ? isi_error_get_message(error) : "");
	}
	
	if (pl)
		siq_policies_free(pl);

	isi_error_free(error);
}


/*  __  __       _         _   _      _                     
 * |  \/  | __ _(_)_ __   | | | | ___| |_ __   ___ _ __ ___ 
 * | |\/| |/ _` | | '_ \  | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * | |  | | (_| | | | | | |  _  |  __/ | |_) |  __/ |  \__ \
 * |_|  |_|\__,_|_|_| |_| |_| |_|\___|_| .__/ \___|_|  |___/
 *                                     |_|                  
 */

/**
 * Do early initialization steps.  This does a lot of the basic
 * data structure initialization, reads in the global_config,
 * and sets signal handlers.
 */
static void
do_initialization(int argc, char **argv)
{
	int ret = 0;
	sigset_t sigset;
	struct isi_error *error = NULL;
	struct ilog_app_init init = {
		.full_app_name = "siq_coord",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.syslog_threshold = IL_INFO_PLUS, ////IL_TRACE -> IL_INFO_PLUS
		.component = "coord",
		.job = "",
	};
	time_t now = time(0);
	char *job_name = NULL;
	if (argc == 2 && !strcmp(argv[1], "-v")) {
		siq_print_version(PROGNAME);
		exit(0);
	}

	g_ctx.run_id = now;
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);

	if (check_for_dont_run(false)) {
		ilog_running_app_stderr_set(true);
		log(NOTICE,  "Can't run isi_migrate.");
		_exit(EXIT_FAILURE);
	}

	initialize_queues();
	init_node_mask(default_node_mask, '.', sizeof(default_node_mask));

	if (UFAIL_POINT_INIT("isi_migrate", isi_migrate_ufp) != 0)
		log(ERROR, "Failed to init ufp (%s)", strerror(errno));

	global_config = siq_gc_conf_load(&error);
	if (error)
		fatal_error(false, SIQ_ALERT_SIQ_CONFIG,
		    "Failed to load global configuration: %s",
		    isi_error_get_message(error));
	siq_gc_conf_close(global_config);

	UFAIL_POINT_CODE(fail_conf_load,
	    fatal_error(false, SIQ_ALERT_SIQ_CONFIG,
	        "Failed to load global configuration: UFP");
	);

	siq_cancel_alert(SIQ_ALERT_SIQ_CONFIG, GLOBAL_SIQ_ALERT,
	    GLOBAL_SIQ_ALERT);

	/* Downgrade STF policy to treewalk style for escalation/policy merge
	 * support */
	if (argc == 3 && !strcmp(argv[1], "-stfdown")) {
		log(NOTICE, "STF downgrade operation");
		g_ctx.stf_downgrade_op = true;
		strcpy(argv[1], "-j");
	}

	gc_job_spec = NULL;

	/* From this point on we are in charge */
	atexit(exiting);
	sigemptyset(&sigset);
	sigprocmask(SIG_SETMASK, &sigset, 0);
	signal(SIGHUP, SIG_IGN);
	signal(SIGUSR1, sigusr1);
	signal(SIGUSR2, sigusr2);
	ilog(IL_INFO, "%s called migr_readconf", __func__);
	/* Returns with job status of 'running' */
	if (migr_readconf(argc, argv, global_config->root, &g_ctx.rc,
	    &g_ctx.record)) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Failed to load configuration data");
	}

	UFAIL_POINT_CODE(coord_fatal_error,
	    fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
	        "Fatal error failpoint enabled, failing job"););

	gmp_kill_on_quorum_change_register(quorum_handler, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_POLICY_FAIL,
		    "main: gmp_kill_on_quorum_change_register error");

	ret = set_siq_log_level(g_ctx.rc.loglevel);

	get_version_status(false, &g_ctx.ver_stat);

	/* Set ILOG job name to "policy name":"run id" */
	asprintf(&job_name, "%s:%llu", g_ctx.rc.job_name, g_ctx.run_id);
	ilog_running_app_job_set(job_name);
	//ilog(IL_INFO, "%s called migr_readconf", __func__);
	ilog(IL_INFO, "Starting job '%s' (%s)", JOB_SPEC->common->name,
	    g_ctx.rc.job_id);

	ilog(IL_INFO, "Action %d", g_ctx.rc.action); ///test log level: TRACE -> NOTICE
	ilog(IL_INFO, "Integrity option is %s",
	    (g_ctx.rc.flags & FLAG_NO_INTEGRITY) ? "off" : "on");
	ilog(IL_INFO, "g_ctx.rc.path=%s", g_ctx.rc.path);
	g_ctx.last_time = 0;
	g_ctx.started = false;
	g_ctx.done = false;
	g_ctx.job_had_error = false;

	/* bandwidth and throttle limiting */
	g_ctx.bw_host = -1;
	g_ctx.th_host = -1;
	g_ctx.work_host = -1;
	g_ctx.bw_last = 0;
	g_ctx.th_last = 0;
	g_ctx.bw_ration = 0;
	g_ctx.th_ration = 0;
	g_ctx.wk_ration = 0;
	g_ctx.work_node_mask = NULL;
	g_ctx.work_fast_start_count = 1;
	g_ctx.work_fast_kill_count = 1;
	g_ctx.work_lnn_list = NULL;
	g_ctx.work_lnn_list_len = 0;
	g_ctx.work_last_worker_inc = 0;
	g_ctx.bw_last_conn = now;
	g_ctx.th_last_conn = now;
	g_ctx.work_last_conn = now;
	g_ctx.bw_last_conn_attempt = 0;
	g_ctx.th_last_conn_attempt = 0;
	g_ctx.work_last_conn_attempt = 0;
	g_ctx.bw_conn_failures = 0;
	g_ctx.th_conn_failures = 0;
	g_ctx.work_conn_failures = 0;
	g_ctx.tctx.sock = -1;
	g_ctx.workers = NULL;
	g_ctx.workers_configured = false;
	g_ctx.active_workers = 0;
	g_ctx.comp_conflict_log_dir = NULL;

	if (strcmp(JOB_SPEC->common->name, "snapshot_revert") == 0)
		g_ctx.snap_revert_op = true;
	else
		g_ctx.snap_revert_op = false;


	if (g_ctx.failback_action == SIQ_ACT_SNAP_REVERT_DOMAIN_MARK) {
		log(INFO, "Snapshot revert domain mark job");
	}

	if (g_ctx.failback_action == SIQ_ACT_SYNCIQ_DOMAIN_MARK) {
		log(INFO, "SyncIQ domain mark job");
	}

	if (job_name)
		free(job_name);
}

/**
 * check whether we are recording deletes, and if so, open
 * the file into which we are logging.
 */
static void
open_deleted_list_file(void)
{
	DIR *dir;

	if (g_ctx.rc.flags & FLAG_LOGDELETES) {
		if ((dir = opendir(g_ctx.rc.job_dir))) {
			g_ctx.rc.fd_deleted = enc_openat(dirfd(dir),
			    g_ctx.rc.log_removed_files_name, ENC_UTF8,
			    O_APPEND | O_WRONLY | O_NOFOLLOW | O_CREAT, 0755);
			if (g_ctx.rc.fd_deleted < 0) {
				log(ERROR, "Error opening log for deleted "
				    "files");
				g_ctx.rc.flags &= ~FLAG_LOGDELETES;
			}
			closedir(dir);
		} else
			log(ERROR, "Error opening job directory");
	}
}

/**
 * If we are enforcing node/interface restrictions, setup that
 * state in the g_ctx.
 */
void
setup_restrictions(char *restrict_by, bool force_interface)
{
	struct isi_error *error = NULL;
	int flags = FLAG_RESTORE | FLAG_DOMAIN_MARK;
	if (!restrict_by)
		goto out;

	g_ctx.restrict_by = strdup(restrict_by);
	ASSERT(g_ctx.restrict_by);

	/*
	 * Bug 197435
	 * skip forced_interface for resync, restore, or domain mark
	 */
	if (!force_interface || (g_ctx.rc.flags & flags) != 0) {
		log(DEBUG, "%s: no configured node restrictions", __func__);
		g_ctx.forced_addr = false;
		goto out;
	}

	g_ctx.forced_addr = get_local_node_pool_addr(restrict_by, &error);
	if (error) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Coordinator can't run in "
		    "pool %s with force_interface set: %s",
		    restrict_by, isi_error_get_message(error)); 
	}

out:
	isi_error_free(error);
}


/*  ____                              _      
 * |  _ \ _   _ _ __   __ _ _ __ ___ (_) ___ 
 * | | | | | | | '_ \ / _` | '_ ` _ \| |/ __|
 * | |_| | |_| | | | | (_| | | | | | | | (__ 
 * |____/ \__, |_| |_|\__,_|_| |_| |_|_|\___|
 *        |___/                              
 * __        __         _             
 * \ \      / /__  _ __| | _____ _ __ 
 *  \ \ /\ / / _ \| '__| |/ / _ \ '__|
 *   \ V  V / (_) | |  |   <  __/ |   
 *    \_/\_/ \___/|_|  |_|\_\___|_|   
 *     _    _ _                 _   _             
 *    / \  | | | ___   ___ __ _| |_(_) ___  _ __  
 *   / _ \ | | |/ _ \ / __/ _` | __| |/ _ \| '_ \ 
 *  / ___ \| | | (_) | (_| (_| | |_| | (_) | | | |
 * /_/   \_\_|_|\___/ \___\__,_|\__|_|\___/|_| |_|
 */

void
get_source_ip_list()/////pworker放在哪个node上,获得src_nodes->ips
{
	struct isi_error *error = NULL;
	char *msg = NULL;

	log(TRACE, "%s", __func__);

	if (g_ctx.new_src_nodes) {
		free(g_ctx.new_src_nodes);
		g_ctx.new_src_nodes = NULL;
	}

	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	ASSERT(g_ctx.new_src_nodes);

	if (strcmp(g_ctx.rc.target, "localhost") == 0 ||
	    strcmp(g_ctx.rc.target, "127.0.0.1") == 0 ||
	    strcmp(g_ctx.rc.target, "::1") == 0) {
		g_ctx.new_src_nodes->count = get_local_cluster_info(NULL,
		    &g_ctx.new_src_nodes->ips, &g_ctx.work_lnn_list,
		    &g_ctx.work_lnn_list_len, 0, JOB_SPEC->target->restrict_by,
		    true, true, &msg, &error);
	} else {
		g_ctx.new_src_nodes->count = get_local_cluster_info(NULL,
		    &g_ctx.new_src_nodes->ips, &g_ctx.work_lnn_list,
		    &g_ctx.work_lnn_list_len, 0, JOB_SPEC->target->restrict_by,
		    false, false, &msg, &error);
	}
	
	if (g_ctx.new_src_nodes->count == 0) {
		fatal_error(true, SIQ_ALERT_LOCAL_CONNECTIVITY,
		    "Error retrieving IP addresses for primary cluster: %s",
		    isi_error_get_message(error));
	}

	log(NOTICE, "source nodes: %s", msg);
	free(msg);
}

/**
 * Take a pre-sync snapshot
 */
static void
pre_sync_snap(void)
{
	time_t start_time = 0;
	time_t end_time = 0;

	siq_job_time(g_ctx.record, &start_time, &end_time);
	if (start_time == 0) {
		/* Job has never been run, try to take a snapshot */
		char *snapshot_name = NULL;
		int size = strlen(g_ctx.rc.snap_sec_name) + 10;

		snapshot_name = calloc(size, 1);
		ASSERT(snapshot_name != NULL);
		strcpy(snapshot_name, g_ctx.rc.snap_sec_name);
		strlcat(snapshot_name, "-initial", size);

		do_target_snapshot(snapshot_name);

		free(snapshot_name);

	}
}

static void
eat_error(struct isi_error *error)
{
	if (error) {
		log(ERROR, "Failed to prune btree: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}
}

void
post_success_prune_btrees(ifs_snapid_t kept_prev_snap,
    ifs_snapid_t kept_cur_snap)
{
	char pattern_trim[MAXNAMLEN];
	struct isi_error *error = NULL;

	get_repeated_rep_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	prune_old_repstate(g_ctx.rc.job_id, pattern_trim, NULL, 0, &error);

	/* Eat this error.  We'll try cleaning up every run */
	eat_error(error);
	error = NULL;

	get_select_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	if (!g_ctx.restore) {
		prune_old_repstate(g_ctx.rc.job_id, pattern_trim,
		    &kept_cur_snap, 1, &error);
	} else {
		prune_old_repstate(g_ctx.rc.job_id, pattern_trim,
		    NULL, 0, &error);
	}
	/* Eat this error.  We'll try cleaning up every run */
	eat_error(error);
	error = NULL;

	get_worklist_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	prune_old_worklist(g_ctx.rc.job_id, pattern_trim,
	    NULL, 0, &error);
	/* Eat this error.  We'll try cleaning up every run */
	eat_error(error);
	error = NULL;

	get_summ_stf_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	prune_old_summary_stf(g_ctx.rc.job_id, pattern_trim,
	    NULL, 0, &error);
	/* Eat this error.  We'll try cleaning up every run */
	eat_error(error);
	error = NULL;

	get_repeated_cpss_pattern(pattern_trim, g_ctx.rc.job_id,
	    g_ctx.restore);
	prune_old_cpss(g_ctx.rc.job_id, pattern_trim, NULL, 0, &error);
	eat_error(error);
	error = NULL;

	/* If this is not a restore, then we have either successfully processed
	 * the resync list or it is empty and able to be removed. */
	if (g_ctx.rc.compliance_v2 && !g_ctx.restore) {
		get_resync_name(pattern_trim, sizeof(pattern_trim),
		    g_ctx.rc.job_id, true);
		remove_lin_list(g_ctx.rc.job_id, pattern_trim, true,
		    &error);
		/* Eat this error.  We'll try cleaning up every run */
		eat_error(error);
		error = NULL;
	}
}

/**
 * Do the work that occurs after the main sync job has succeeded.
 * Remove snapshot locks, take a target snapshot,
 * cleanup the target syncrun file, and mark as a success.
 */
void
post_primary_success_work(void)
{
	char old_name[MAXPATHLEN], new_name[MAXPATHLEN];
	struct isi_error *error = NULL;
	struct generic_msg m = {};
	ifs_snapid_t kept_prev_snap, kept_curr_snap;
	kept_curr_snap = g_ctx.curr_snap.id;
	kept_prev_snap = g_ctx.prev_snap.id;

	/*
	 * Set domain ready only on upgrades or initial sync
	 * but according to PSCALE-52608 Assess sync should not mark domain ready
	 */
	if ((g_ctx.initial_sync && g_ctx.stf_sync &&
	     !(g_ctx.rc.flags & FLAG_ASSESS)) || g_ctx.stf_upgrade_sync) {
	    target_upgrade_complete();
	}

	remove_snapshot_locks();

	/*
	 * If Database resync phase was run, then make sure we record
	 * the fact that we have mirrored the repstate and linmap.
	 * This would avoid future resyncs for database.
	 */
	if (g_ctx.job_phase.jtype && 
	    contains_phase(&g_ctx.job_phase, STF_PHASE_DATABASE_RESYNC) &&
	    !stf_job_phase_disabled(&g_ctx.job_phase,
	    STF_PHASE_DATABASE_RESYNC)) {
		siq_source_set_database_mirrored(g_ctx.record);
	}

	if ((g_ctx.rc.source_snapmode == SIQ_SNAP_MAKE) &&
	    !(g_ctx.rc.flags & FLAG_ASSESS) && !g_ctx.restore) {
		if (allow_failback_snap_rename(&g_ctx)) {
			delete_old_snapshot();
		}

		if (g_ctx.changelist_op && !g_ctx.job_had_error) {
			get_repeated_rep_name(old_name, g_ctx.rc.job_id,
			    kept_curr_snap, g_ctx.restore);

			get_changelist_rep_name(new_name, kept_prev_snap,
			    kept_curr_snap);

			siq_safe_btree_rename(siq_btree_repstate,
			    g_ctx.rc.job_id, old_name, CHANGELIST_REP_DIR,
			    new_name, &error);

			if (error)
				fatal_error_from_isi_error(false, error);

			log(DEBUG, "changelist: renamed repstate %s to %s",
			    old_name, new_name);
		}

		post_success_prune_btrees(kept_prev_snap, kept_curr_snap);
	}

	/*
	 * Cleanup tmp-working-dir at the target for initial/upgrade syncs.
	 * For incremental syncs we do the same in checkpoint_next_phase()
	 * before FLIP phase.
	 */
	if (!g_ctx.tgt_tmp_removed && 
	    (g_ctx.rc.flags & FLAG_STF_SYNC) &&
	    !(g_ctx.rc.flags & FLAG_CLEAR)) {
		target_cleanup_complete();	
	}

	/* Make a snapshot on the secondary, if configured */
	if (g_ctx.rc.flags & FLAG_SNAPSHOT_SEC) {
		do_target_snapshot(g_ctx.rc.snap_sec_name);
	}

	/*
	 * A treewalk initial/upgrade sync or failover should take
	 * target snapshot meant for failover. Avoid calling this for
	 * snapshot revert operations or the target will delete the
	 * snapshot we reverted
	 */
	if (g_ctx.curr_ver >= FLAG_VER_3_5 && 
	    (g_ctx.stf_sync &&
	    (g_ctx.initial_sync || g_ctx.stf_upgrade_sync))) {
		do_target_snapshot(NULL);
	}

	/* Cleanup sync run files */
	m.head.type = CLEANUP_SYNC_RUN_MSG;
	m.body.cleanup_sync_run.policy_id = g_ctx.rc.job_id;
	msg_send(g_ctx.tctx.sock, &m);

	if (g_ctx.failover)
		on_failover_restore_done(&error);
	else if (g_ctx.failover_revert)
		on_failover_revert_done(&error);
	else if (JOB_SPEC->common->action == SIQ_ACT_FAILBACK_PREP_RESTORE)
		on_failback_prep_restore_done(&error);
	else if (JOB_SPEC->common->action == SIQ_ACT_FAILBACK_PREP) {
		ifs_snapid_t restore_latest;

		/* Remove restore_latest snapshot for local syncs (generated by
		   allow_write) since resync prep restore will generate one 
		   too. */
		siq_source_get_restore_latest(g_ctx.record, &restore_latest);
		if (restore_latest != INVALID_SNAPID) {
			log(DEBUG, "Removing restore-latest snapshot leftover "
			    "from allow_write.");
			delete_snapshot_helper(restore_latest);
		}

	}


	if (error)
		fatal_error_from_isi_error(false, error);

	/* Status is SUCCESS if no errors occurred or used
	 * MigrationIQ semantic (Migrate or copy with archive bit) */
	set_job_state(g_ctx.job_had_error ? SIQ_JS_FAILED : SIQ_JS_SUCCESS);

	/* Now done with tmonitor */
	disconnect_from_tmonitor();
}

/**
 * Check for loopback syncs with illegal destinations (ie, source/target
 * overlap).
 */
static void
check_for_illegal_loopback(void)
{
	int i;
	int path_len, target_len, shortest_len;
	char *longer_path;

	if (g_ctx.src_nodes->count != g_ctx.tgt_nodes->count)
		goto out;

	for (i = 0; i < g_ctx.src_nodes->count; i++) {
		if (strcmp(g_ctx.tgt_nodes->nodes[i]->host,
		    g_ctx.src_nodes->ips[i]))
			goto out;
	}

	if ((g_ctx.rc.flags & FLAG_SYSTEM) &&
	    (g_ctx.rc.action == SIQ_ACT_REMOVE))
		goto out;
	
	path_len = strlen(g_ctx.rc.path);
	target_len = strlen(g_ctx.rc.targetpath);
	shortest_len = path_len < target_len ? path_len : target_len;
	longer_path = path_len > target_len ? g_ctx.rc.path :
	    g_ctx.rc.targetpath;

	if (!strncmp(g_ctx.rc.targetpath, g_ctx.rc.path, shortest_len)) {
		if (longer_path[shortest_len] == '/' ||
		    longer_path[shortest_len] == '\0') {
			fatal_error(false, SIQ_ALERT_SELF_PATH_OVERLAP,
			    "Source path '%s' overlaps target path "
			    "'%s' on the same cluster",
			    g_ctx.rc.path, g_ctx.rc.targetpath);
		}
	}

out:
	return;
}

/**
 * Test the connection to the target, returning the ver
 * value returned by connect_to if successful.
 * If the connection fails, we either have a handshake negotiation
 * failure, or a general connectivity issue. If handshake failure,
 * it's fatal. If connectivity issue, it will be retried later.
 */
unsigned
test_connection()
{
	int conn_test_fd = -1;
	unsigned ver;
	struct isi_error *error = NULL;

	conn_test_fd = connect_to(g_ctx.rc.target,
	    global_config->root->coordinator->ports->sworker,
	    POL_DENY_FALLBACK, g_ctx.ver_stat.committed_version, &ver,
	    g_ctx.forced_addr, false, &error);
	if (conn_test_fd == -1) {
		if (ver == SHAKE_LOCAL_REJ || ver == SHAKE_PEER_REJ)
			fatal_error(false, SIQ_ALERT_VERSION, "");
	} else
		close(conn_test_fd);

	return ver;
}

static int
delete_policy_ack_callback(struct generic_msg *m, void *ctx)
{
	int return_code = m->body.generic_ack.code;

	if (return_code == ACK_ERR) {
		/* An actual error occurred */
		log(ERROR, "Delete job failed: %s", m->body.generic_ack.msg);
	} else if (return_code == ACK_WARN) {
		/* Target record not found */
		log(NOTICE, "Target warning: %s", m->body.generic_ack.msg);
	} else if (return_code == ACK_OK) {
		log(DEBUG, "delete_policy_ack_callback reply: ACK_OK");
	}

	return 0;
}

static int
delete_policy_discon_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "Received DISCON_MSG during Delete Job");
	migr_force_process_stop();
	return 0;
}

/**
 * Deletes the policy specified in the g_ctx.  This means...
 * a) Contacting the target and deleting the target record
 * b) Deleting reports locally
 * c) Deleting snapshots locally
 * d) Actually deleting the policy from tsm-policies.xml
 */
static void
delete_policy(void)
{
	int target_sworker_fd = -1;
	unsigned ver;
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	int ret = 0;
	struct siq_policies *policies = NULL;
	struct siq_policy *policy = NULL;
	bool delete_failed = true;

	log(NOTICE, "Deleting policy %s (%s)", g_ctx.rc.job_name,
	    g_ctx.rc.job_id);

	/* Clean up the work item lock file */
	delete_work_item_lock(g_ctx.rc.job_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	policies = siq_policies_load_nolock(&error);
	if (error != NULL) {
		log(ERROR, "delete_policy: failed to load policies: %s",
		    isi_error_get_message(error));
		goto done;
	}

	policy = siq_policy_get_by_pid(policies, g_ctx.rc.job_id);
	if (!policy) {
		log(ERROR, "delete_policy: policy %s not found",
		    g_ctx.rc.job_id);
		goto done;
	}

	/* Check the source db target uuid field, if blank,
	 * then skip connection step */
	if (siq_source_has_associated(g_ctx.record) ||
	    policy->common->mirror_pid) {
		log(DEBUG, "delete_policy: deleting target record");
		/* Connect to sworker */
		target_sworker_fd = connect_to(g_ctx.rc.target,
		    global_config->root->coordinator->ports->sworker,
		    POL_DENY_FALLBACK, g_ctx.ver_stat.committed_version, &ver,
		    g_ctx.forced_addr, false, &error);
		if (target_sworker_fd == -1) {
			/* Cannot connect to sworker -> fail this job
			 * Users can force the delete via another method */
			log(ERROR, "%s failed: %s", __func__,
			    isi_error_get_message(error));
			goto done;
		}

		migr_add_fd(target_sworker_fd, &g_ctx, "target_delete_sworker");
		migr_register_callback(target_sworker_fd, GENERIC_ACK_MSG,
		    delete_policy_ack_callback);
		migr_register_callback(target_sworker_fd, DISCON_MSG,
		    delete_policy_discon_callback);

		/* Send TARGET_POLICY_DELETE_MSG */
		msg.head.type = TARGET_POLICY_DELETE_MSG;
		msg.body.target_policy_delete.policy_id =
		    strdup(g_ctx.rc.job_id);
		msg_send(target_sworker_fd, &msg);

		/* Wait for GENERIC_ACK */
		if (migr_process() < 0) {
			log(ERROR, "delete_policy: failed migr_process");
			goto done;
		}

		/* Close connection */
		close(target_sworker_fd);
		target_sworker_fd = -1;
	} else {
		log(DEBUG, "delete_policy: skipping target record delete");
	}

	siq_policies_free(policies);
	policies = NULL;
	policy = NULL;

	/* Do local cleanup */
	policies = siq_policies_load(&error);
	if (error != NULL) {
		log(ERROR, "delete_policy: failed to load policies: %s",
		    isi_error_get_message(error));
		goto done;
	}
	policy = siq_policy_get_by_pid(policies, g_ctx.rc.job_id);
	if (!policy) {
		log(ERROR, "delete_policy: policy %s not found for "
		    "local cleanup", g_ctx.rc.job_id);
		goto done;
	}
	ret = siq_policy_remove_local(policies, policy, g_ctx.rc.job_dir,
	    &error);
	if (error != NULL) {
		log(ERROR, "delete_policy: failed to delete policy: %s",
		    isi_error_get_message(error));
		goto done;
	}

	delete_failed = false;

done:
	/* Cleanup */
	if (target_sworker_fd != -1) {
		close(target_sworker_fd);
		target_sworker_fd = -1;
	}

	if (policies) {
		siq_policies_free(policies);
	}

	isi_error_free(error);

	/* Mark job as successful or not... */
	if (delete_failed) {
		set_job_state(SIQ_JS_FAILED);
	} else {
		set_job_state(SIQ_JS_SUCCESS);
	}

	exit(EXIT_SUCCESS);
}

static int
stf_policy_downgrade_ack_callback(struct generic_msg *m, void *ctx)
{
	int return_code = m->body.generic_ack.code;

	if (return_code == ACK_ERR)
		log(ERROR, "Target policy downgrade failed: %s",
		    m->body.generic_ack.msg);
	else if (return_code == ACK_OK)
		((struct coord_ctx *)ctx)->stf_target_downgrade_ok = true;

	return 0;
}

static int
stf_policy_downgrade_discon_callback(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "Received DISCON_MSG during policy downgrade");
	migr_force_process_stop();
	return 0;
}

static int
delete_sbt(const char *policy, char *name)
{
	struct isi_error *error = NULL;
	bool exists;

	exists = repstate_exists(policy, name, &error);
	if (error)
		goto error;
	if (exists) {
		remove_repstate(policy, name, false, &error);
		if (error) {
			log(ERROR, "Couldn't remove %s", name);
			goto error;
		} else {
			log(NOTICE, "Removed %s", name);
		}
	} else {
		log(NOTICE, "%s does not exist", name);
	}

	return 0;

error:
	if (error)
		isi_error_free(error);
	return -1;
}

static void
stf_policy_downgrade(void)
{
	char name[MAXNAMELEN];
	int target_fd = -1;
	int res;
	uint32_t ver;
	ifs_snapid_t snapid;
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	char lmap_name[MAXNAMELEN + 1];
	char cpss_mirror_name[MAXNAMLEN];

	log(DEBUG, "STF policy downgrade");

	if (!(bool)(g_ctx.rc.flags & FLAG_STF_SYNC)) {
		log(ERROR, "Policy is not currently stf enabled");
		goto out;
	}

	/* delete base repstate */
	get_base_rep_name(name, g_ctx.rc.job_id, g_ctx.restore);
	res = delete_sbt(g_ctx.rc.job_id, name);
	if (res == -1)
		goto out;

	siq_source_get_latest(g_ctx.record, &snapid);
	if (snapid <= 0) {
		/* downgrading a policy before initial sync has finished */
		log(DEBUG, "Couldn't determine repeated repstate name");
	} else {
		/* delete repeated repstate */
		get_repeated_rep_name(name, g_ctx.rc.job_id, snapid, g_ctx.restore);
		res = delete_sbt(g_ctx.rc.job_id, name);
		if (res == -1)
			goto out;

		/* delete select */
		get_select_name(name, g_ctx.rc.job_id, snapid, g_ctx.restore);
		res = delete_sbt(g_ctx.rc.job_id, name);
		if (res == -1)
			goto out;
	}

	/* send downgrade request to target monitor */
	if (!siq_source_has_associated(g_ctx.record)) {
		log(ERROR, "Target GUID is missing");
		goto out;
	}
	target_fd = connect_to(g_ctx.rc.target,
	    global_config->root->coordinator->ports->sworker, POL_DENY_FALLBACK,
	    g_ctx.ver_stat.committed_version, &ver, g_ctx.forced_addr, false,
	    &error);
	if (error) {
		log(ERROR, "Failed to connect to target %s",
		    isi_error_get_message(error));
		goto out;
	}
	migr_add_fd(target_fd, &g_ctx, "target_stf_downgrade");
	migr_register_callback(target_fd, GENERIC_ACK_MSG,
	    stf_policy_downgrade_ack_callback);
	migr_register_callback(target_fd, DISCON_MSG,
	    stf_policy_downgrade_discon_callback);

	msg.head.type = TARGET_POL_STF_DOWNGRADE_MSG;
	msg.body.target_pol_stf_downgrade.policy_id = strdup(g_ctx.rc.job_id);
	msg_send(target_fd, &msg);

	/* wait for ack */
	g_ctx.stf_target_downgrade_ok = false;
	if (migr_process() < 0) {
		log(ERROR, "Failed migr_process");
		goto out;
	}

	if (!g_ctx.stf_target_downgrade_ok)
		goto out;

	/**
	 * Bug 175709
	 * Remove mirrored linmap.
	 */
	if (g_ctx.rc.database_mirrored) {
		/* Cleanup local linmap mirror */
		get_mirrored_lmap_name(lmap_name, g_ctx.rc.job_id);
		remove_linmap(g_ctx.rc.job_id, lmap_name, true, &error);
		if (error) {
			log(ERROR,
			    "Failed to remove mirrored linmap %s : %s",
			    lmap_name, isi_error_get_message(error));
			goto out;
		}
		siq_source_clear_database_mirrored(g_ctx.record);
	}

	get_mirrored_cpss_name(cpss_mirror_name, g_ctx.rc.job_id);
	remove_cpss(g_ctx.rc.job_id, cpss_mirror_name, true, &error);
	if (error) {
		log(ERROR, "Failed to delete cpss mirror %s for policy %s - "
			"%s", cpss_mirror_name, g_ctx.rc.job_id,
			isi_error_get_message(error));
		goto out;
	}

	/* clear this state so we can run an upgrade in the future */
	siq_source_clear_pol_stf_ready(g_ctx.record);
	log(NOTICE, "STF policy downgrade complete");

out:
	if (g_ctx.stf_target_downgrade_ok)
		set_job_state(SIQ_JS_SUCCESS);
	else
		set_job_state(SIQ_JS_FAILED);

	isi_error_free(error);

	/* close connection */
	close(target_fd);
	target_fd = -1;
	exit(EXIT_SUCCESS);
}

static void
get_initial_rep_info(struct coord_ctx *coord_ctx,
    struct rep_info_entry *rie_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN];
	struct rep_ctx *rep = NULL;

	ASSERT(rie_out != NULL);
	get_base_rep_name(rep_name, coord_ctx->rc.job_id, coord_ctx->restore);

	open_repstate(coord_ctx->rc.job_id, rep_name, &rep, &error);
	if (error)
		goto out;

	get_rep_info(rie_out, rep, &error);
	if (error)
		goto out;

 out:
	if (rep)
		close_repstate(rep);

	isi_error_handle(error, error_out);
}

/*
 * Create compliance related SBTs.
 * resync list SBT.
 * compliance map SBT.
 */
static void
create_or_open_compliance_list(struct coord_ctx *coord_ctx,
    struct isi_error **error_out)
{
	bool src;
	char sbt_name[MAXNAMLEN];
	struct lin_list_ctx *resync_ctx = NULL;
	struct lin_list_info_entry llie = {};
	struct isi_error *error = NULL;

	if (!g_ctx.rc.compliance_v2)
		return;

	src = (coord_ctx->rc.job_type != SIQ_JOB_FAILOVER) &&
	    (coord_ctx->rc.job_type != SIQ_JOB_FAILOVER_REVERT);
	get_resync_name(sbt_name, sizeof(sbt_name), coord_ctx->rc.job_id, src);
	create_lin_list(coord_ctx->rc.job_id, sbt_name, true, &error);
	if (error)
		goto out;

	open_lin_list(coord_ctx->rc.job_id, sbt_name, &resync_ctx, &error);
	if (error)
		goto out;

	llie.type = RESYNC;
	set_lin_list_info(&llie, resync_ctx, false, &error);
	if (error)
		goto out;

	if (coord_ctx->restore) {
		get_compliance_map_name(sbt_name, sizeof(sbt_name),
		    coord_ctx->rc.job_id, src);
		create_compliance_map(coord_ctx->rc.job_id, sbt_name, true,
		    &error);
		if (error)
			goto out;
	}

out:
	if (resync_ctx != NULL)
		close_lin_list(resync_ctx);

	isi_error_handle(error, error_out);
}

static void
create_or_open_initial_rep(struct coord_ctx *coord_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN];
	char cpss_name[MAXNAMLEN];
	char pattern_trim[MAXNAMLEN];
	char cpss_pattern[MAXNAMLEN];
	struct rep_info_entry rie = {};
	bool found;
	bool is_sync = false;
	bool reset_pending = false;

	is_sync = (coord_ctx->rc.action == SIQ_ACT_SYNC);

	get_base_rep_name(rep_name, coord_ctx->rc.job_id, coord_ctx->restore);

	/* Create and open the repstate */
	create_repstate(coord_ctx->rc.job_id, rep_name, true, &error);
	if (error)
		goto out;

	open_repstate(coord_ctx->rc.job_id, rep_name, &coord_ctx->rep, &error);
	if (error)
		goto out;

	found = get_rep_info(&rie, coord_ctx->rep, &error);
	if (error)
		goto out;

	get_base_cpss_name(cpss_name, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	/* Create and open the repstate */
	create_cpss(coord_ctx->rc.job_id, cpss_name, true, &error);
	if (error)
		goto out;

	open_cpss(coord_ctx->rc.job_id, cpss_name, &coord_ctx->cpss,
	    &error);
	if (error)
		goto out;

	create_or_open_compliance_list(coord_ctx, &error);
	if (error)
		goto out;

	reset_pending = siq_source_get_reset_pending(coord_ctx->record);

	if (found && (reset_pending ||
	    (coord_ctx->curr_snap.id != rie.initial_sync_snap))) {
		/* repstate must be thrown out to perform a full sync */
		close_repstate(coord_ctx->rep);
		coord_ctx->rep = NULL;
		log(NOTICE, "Removing stale base repstate (snapid %lld)",
		    rie.initial_sync_snap);
		remove_repstate(coord_ctx->rc.job_id, rep_name, false, &error);
		if (error)
			goto out;
		create_repstate(coord_ctx->rc.job_id, rep_name, false, &error);
		if (error)
			goto out;
		open_repstate(coord_ctx->rc.job_id, rep_name, &coord_ctx->rep,
		    &error);
		if (error)
			goto out;

		close_cpss(coord_ctx->cpss);
		remove_cpss(coord_ctx->rc.job_id, cpss_name, false, &error);
		if (error)
			goto out;

		create_cpss(coord_ctx->rc.job_id, cpss_name, false, &error);
		if (error)
			goto out;

		open_cpss(coord_ctx->rc.job_id, cpss_name, &coord_ctx->cpss,
		    &error);
		if (error)
			goto out;
	}

	/* clear reset flag if set so we don't trash repstate on a restart */
	if (reset_pending) {
		siq_source_clear_reset_pending(coord_ctx->record);
		siq_source_record_save(coord_ctx->record, &error);
		if (error)
			goto out;
	}

	rie.initial_sync_snap = coord_ctx->curr_snap.id;
	rie.is_sync_mode = is_sync;

	set_rep_info(&rie, coord_ctx->rep, false, &error);
	if (error)
		goto out;

	get_repeated_rep_pattern(pattern_trim, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	prune_old_repstate(coord_ctx->rc.job_id, pattern_trim, NULL, 0,
	    &error);
	if (error)
		goto out;

	get_repeated_cpss_pattern(cpss_pattern, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	prune_old_cpss(coord_ctx->rc.job_id, cpss_pattern, NULL, 0, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
pre_run_prune_worklist_and_summ_stf(ifs_snapid_t curr_snapid,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char pattern_trim[MAXNAMLEN];

	get_worklist_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	prune_old_worklist(g_ctx.rc.job_id, pattern_trim,
	    &curr_snapid, 1, &error);
	if (error)
		goto out;

	get_summ_stf_pattern(pattern_trim, g_ctx.rc.job_id, g_ctx.restore);
	prune_old_summary_stf(g_ctx.rc.job_id, pattern_trim,
	    &curr_snapid, 1, &error);
	/* Eat this error.  We'll try cleaning up every run */
	if (error)
		goto out;
 out:
	isi_error_handle(error, error_out);
}

static void
create_or_open_repeated_rep(struct coord_ctx *coord_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN];
	char base_rep_name[MAXNAMLEN];
	char pattern_trim[MAXNAMLEN];
	bool exists;
	struct rep_info_entry rie = {};
	bool is_sync;

	is_sync = (coord_ctx->rc.action == SIQ_ACT_SYNC);

	/*
	 * Sanity check that base repstate exists so we don't have
	 * to wait until the pworker dies to find out
	 */
	get_base_rep_name(base_rep_name, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	exists = repstate_exists(coord_ctx->rc.job_id, base_rep_name,
	    &error);
	if (error)
		goto out;
	if (!exists) {
		if (!coord_ctx->restore) {
			error = isi_system_error_new(ENOENT,
			    "Repstate %s does not exist", base_rep_name);
			log(ERROR, "Repstate %s does not exist", base_rep_name);
			goto out;
		}
		/* Base repstate may not exist , create it */
		create_repstate(coord_ctx->rc.job_id, base_rep_name, true,
		    &error);
		if (error)
			goto out;
	}

	if (exists && !coord_ctx->restore) {
		get_initial_rep_info(coord_ctx, &rie, &error);
		if (error)
			goto out;
	}

	get_repeated_rep_name(rep_name, coord_ctx->rc.job_id,
 	    coord_ctx->curr_snap.id, coord_ctx->restore);

	/* Create and open the repstate */	
	create_repstate(coord_ctx->rc.job_id, rep_name, true, &error);
	if (error)
		goto out;

	open_repstate(coord_ctx->rc.job_id, rep_name, &coord_ctx->rep, &error);
	if (error)
		goto out;

	rie.is_sync_mode = is_sync;
	set_rep_info(&rie, coord_ctx->rep, false, &error);
	if (error)
		goto out;

	/* Trim anything other than this repstate */
	get_repeated_rep_pattern(pattern_trim, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	prune_old_repstate(coord_ctx->rc.job_id, pattern_trim,
	    &coord_ctx->curr_snap.id, 1, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);

}

static void
create_or_open_repeated_cpss(struct coord_ctx *coord_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char cpss_name[MAXNAMLEN];
	char base_cpss_name[MAXNAMLEN];
	char pattern_trim[MAXNAMLEN];

	/*
	 * Sanity check that base cpss exists so we don't have
	 * to wait until the pworker dies to find out
	 */
	get_base_cpss_name(base_cpss_name, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	// Create an empty base cpss in case of upgrade.
	create_cpss(coord_ctx->rc.job_id, base_cpss_name, true,
	    &error);
	if (error)
		goto out;

	get_repeated_cpss_name(cpss_name, coord_ctx->rc.job_id,
	    coord_ctx->curr_snap.id, coord_ctx->restore);

	/* Create and open the cpss */
	create_cpss(coord_ctx->rc.job_id, cpss_name, true, &error);
	if (error)
		goto out;

	open_cpss(coord_ctx->rc.job_id, cpss_name, &coord_ctx->cpss, &error);
	if (error)
		goto out;

	/* Trim anything other than this cpss */
	get_repeated_cpss_pattern(pattern_trim, coord_ctx->rc.job_id,
	    coord_ctx->restore);
	prune_old_cpss(coord_ctx->rc.job_id, pattern_trim,
	    &coord_ctx->curr_snap.id, 1, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

/*
 * Add all select associated nodes to exclusion btree
 */
static bool
add_selects(struct select *s, int parent_fd, struct rep_ctx *sel,
    struct siq_snap *snap, struct isi_error **error_out)
{
	int rv;
	int kept_errno;
	struct stat dir_st = {};
	struct stat my_st;
	bool has_includes = false;
	bool has_child_includes = false;
	bool has_peer_includes = false;
	struct isi_error *error = NULL;
	struct select *temp;
	struct sel_entry sentry = {};
	int my_fd = -1;
	struct path p;
	char name[MAXNAMELEN + 1];
	bool first_call = false;

	log(TRACE, "add_selects");
	ASSERT(s != NULL);
	ASSERT(s->path != NULL && s->len > 0);

	if (parent_fd == -1) {
		first_call = true;
		/* Open the snapshot root */
		path_init(&p, g_ctx.source_base.path, NULL);
		my_fd = parent_fd = fd_rel_to_ifs(snap->root_fd, &p);
		if (my_fd == -1) {
			kept_errno = errno;
			log(ERROR, "Unable to open job root directory : %s",
			    strerror(kept_errno));
			error = isi_system_error_new(kept_errno);
			goto out;
		}
		rv = fstat(my_fd, &my_st);
		if (rv == -1) {
			kept_errno = errno;
			log(ERROR, "Unable to stat root path %s : %s", s->path,
			    strerror(errno));
			error = isi_system_error_new(kept_errno);
			goto out;
		}

		ASSERT(my_fd != 0);
	} else {
		/* Lets stat my entry to see if its suitable */
		ASSERT(s->len <= MAXNAMELEN);
		name[0] = '\0';
		strncat(name, s->path, s->len);
		rv = enc_fstatat(parent_fd, name, ENC_DEFAULT, &my_st,
		    AT_SYMLINK_NOFOLLOW);
		if (rv == -1) {
			kept_errno = errno;
			log(ERROR, "Unable to stat entry %s : %s", name,
			    strerror(errno));
			error = isi_system_error_new(kept_errno);
			goto out;
		}
		/* If its not reg file/dir , then we return, */
		if (!S_ISDIR(my_st.st_mode) && !S_ISREG(my_st.st_mode)) {
			log(ERROR, "Unsupported entries in the exclusion"
			    "/inclusion rules");
			goto out;
		}

		/* Lets open my fd */
		my_fd = enc_openat(parent_fd, name, ENC_DEFAULT,
		    O_RDONLY | O_NOFOLLOW);
		/* To be safer added O_NOFOLLOW */
		
		if (my_fd == -1) {
			kept_errno = errno;
			log(ERROR, "Unable to open entry %s: %s", name,
			    strerror(kept_errno));
			error = isi_system_error_new(kept_errno);
			goto out;
		}
		ASSERT(my_fd != 0);
	}

	/* lets stat parent directory also */
	rv = fstat(parent_fd, &dir_st);
	if (rv == -1) {
		kept_errno = errno;
		log(ERROR, "Unable to stat parent directory: %s",
		    strerror(kept_errno));
		error = isi_system_error_new(kept_errno);
		goto out;
	}

	ASSERT(S_ISDIR(dir_st.st_mode));

	if (S_ISDIR(my_st.st_mode)) {
		temp = s;
		ASSERT(my_fd > 0);
		if (temp->child) {
			/* Recursively go through each child */
			has_child_includes = add_selects(temp->child,
			    my_fd, sel, snap, &error);
			if (error)
				goto out;
		}
		
		temp = temp->next;
		if (temp) {
			/* Recursively go through next node */
			has_includes = add_selects(temp, parent_fd,
			    sel, snap, &error);
			if (error)
				goto out;
			if (has_includes)
				has_peer_includes = true;
		}
	}

	/* Now we need to add our own entry to select */
	if (s->select) {
		sentry.includes = true;
		has_includes = true;
	} else if (has_child_includes) {
		sentry.for_child = true;
	} else {
		sentry.excludes = true;
	}
	strlcpy(sentry.name, s->path, sizeof(sentry.name));
	sentry.plin = dir_st.st_ino;
	sentry.enc = ENC_DEFAULT;	/* variable not used */
	/*
	 * 	ENCODING BEHAVIOUR
	 *
	 * 1) Directory encoding : We always consider the first dirent
	 *   read from the file system when there two or more dirents
	 *   with the same name and different encoding in same directory.
	 *
	 * 2) When a file has two or more links from same directory
	 *    and they have same name but different encoding. In this
	 *    we would include or exclude both the links. Since when
	 *    specifying the path the user did not mention which
	 *    encoding is used for file name, I think its okay to have
	 *    this behaviour.
	 */
	
	set_sel_entry(my_st.st_ino, &sentry, sel, &error);
	if (error)
		goto out;
	
	log(TRACE, "added %s with parent inode %llx and includes %d "
	    "for-child %d excludes %d to select tree", sentry.name,
	    dir_st.st_ino, sentry.includes, sentry.for_child, sentry.excludes);

out:
	if (my_fd > 0)
		close(my_fd);
	if (first_call && parent_fd > -1)
		close(parent_fd);

	isi_error_handle(error, error_out);
	return (has_includes || has_child_includes || has_peer_includes);
}

/*
 * Create and add select entries for particular snapshot
 */
static void
create_and_add_select_entries(struct coord_ctx *coord_ctx, char *sel_name,
    struct siq_snap *snap, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct select *s = NULL;
	struct rep_ctx *sel_ctx = NULL;

	remove_repstate(coord_ctx->rc.job_id, sel_name, true, &error);
	if (error)
		goto out;

	/* Create exclusion tree */
	create_repstate(coord_ctx->rc.job_id, sel_name,
	    true, &error);
	if (error)
		goto out;
		
	/* Open exclusion tree */
	open_repstate(coord_ctx->rc.job_id, sel_name, &sel_ctx,
	    &error);
	if (error)
		goto out;
	/*
	 * XXX
	 * We should only use snap related valid paths.
	 * There is bug in conf.c/add_paths() which
	 * always looks at HEAD version. Will fix it soon.
	 * XXX
	 */	
	s = sel_build(coord_ctx->source_base.path,
	    (const char **)coord_ctx->rc.paths.paths);
	ASSERT(s != NULL && s->path != NULL);

	add_selects(s, -1, sel_ctx, snap, &error);
	if (error)
		goto out;
out:

	if (s)	
		sel_free(s);
	close_repstate(sel_ctx);	
	isi_error_handle(error, error_out);
}

static void
create_sel_tree(struct coord_ctx *coord_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char sel_name[MAXNAMLEN];
	char pattern_trim[MAXNAMLEN];
	ifs_snapid_t snaps[2];
	bool found;

	/*
	 * First check if the prev snap select tree exists for
	 * valid incremental/restore syncs. If not we need to create one.
	 */
	if (coord_ctx->prev_snap.id != INVALID_SNAPID) {
		get_select_name(sel_name, coord_ctx->rc.job_id,
		    coord_ctx->prev_snap.id, coord_ctx->restore);
		found = repstate_exists(coord_ctx->rc.job_id, sel_name, &error);
		if (error)
			goto out;
		if (!found) {
			create_and_add_select_entries(coord_ctx, sel_name,
			    &coord_ctx->prev_snap, &error);
			if (error)
				goto out;
		}
	}

	get_select_name(sel_name, coord_ctx->rc.job_id,
	    coord_ctx->curr_snap.id, coord_ctx->restore);

	create_and_add_select_entries(coord_ctx, sel_name,
	    &coord_ctx->curr_snap, &error);
	if (error)
		goto out;

	/*
	 * Trim anything other than the relevant select trees
	 */
	snaps[0] = coord_ctx->curr_snap.id;
	snaps[1] = coord_ctx->prev_snap.id;

	get_select_pattern(pattern_trim, g_ctx.rc.job_id, coord_ctx->restore);
	prune_old_repstate(coord_ctx->rc.job_id, pattern_trim, snaps, 2,
	    &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

/*
 * This function creates the repstate if it is not already created or if
 * the initial repstate is incorrect.
 */
static void
create_or_open_rep(struct coord_ctx *coord_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	
	if (is_treewalk(coord_ctx)) {
		// Cpools sync state created inside
		// create_or_open_initial_rep() for treewalk.
		create_or_open_initial_rep(coord_ctx, &error);
		if (error)
			goto out;
	} else {
		create_or_open_repeated_rep(coord_ctx, &error);
		if (error)
			goto out;

		create_or_open_repeated_cpss(coord_ctx, &error);
		if (error)
			goto out;

		create_or_open_compliance_list(coord_ctx, &error);
		if (error)
			goto out;
	}

	upgrade_work_entries(coord_ctx->rep, &error);

out:
	isi_error_handle(error, error_out);
}

static char *
serialize_initial_work(struct coord_ctx *ctx, size_t *tw_size,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	struct select *select_tree = NULL;
	char *selectbuf = NULL;
	int selectlen = 0;
	struct migr_tw tw = {};
	struct dir_slice slice;
	void *data = NULL;

	log(TRACE, "serialize_initial_work");
	path_stat(ctx->curr_snap.root_fd, &ctx->source_base, &st, &error);

	ASSERT(error == NULL, "path = %s, error = %s", g_ctx.source_base.path,
	    isi_error_get_message(error));

	/* Build the select tree */
	select_tree = sel_build(g_ctx.source_base.path,
	    (const char **)g_ctx.rc.paths.paths);
	selectbuf = sel_export(select_tree, &selectlen);
	sel_free(select_tree);

	slice.begin = DIR_SLICE_MIN;
	slice.end = DIR_SLICE_MAX;

	migr_tw_init(&tw, ctx->curr_snap.root_fd, &ctx->source_base, &slice,
	    selectbuf, false, true, &error);
	if (error)
		goto out;

	data = migr_tw_serialize(&tw, tw_size);

out:
	if (selectbuf)
		free(selectbuf);

	isi_error_handle(error, error_out);
	return (char *)data;
}

static void
check_compliance_commit_phase(struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	//This function conditionally enables STF_PHASE_COMP_FINISH_COMMIT
	log(TRACE, "%s: cv2: %d fo: %d for: %d", __func__,
	    g_ctx.rc.compliance_v2, g_ctx.failover, g_ctx.failover_revert);

	//If we are doing compliance v2 sync, enable late stage commit phase
	if (g_ctx.rc.compliance_v2) {
		if (g_ctx.stf_sync || g_ctx.stf_upgrade_sync)
			stf_job_phase_enable(&g_ctx.job_phase,
			    STF_PHASE_COMP_COMMIT);

		//For failover/failover revert, if we have an incomplete late
		//stage commit phase from the last sync, then disable all
		//phases and run LSC only.
		if (g_ctx.failover || g_ctx.failover_revert) {
			log(TRACE, "%s: checking for target record", __func__);
			//Look for the target record flag
			lock_fd = siq_target_acquire_lock(g_ctx.rc.job_id,
			    O_SHLOCK, &error);
			if (error) {
				goto out;
			}
			trec = siq_target_record_read(g_ctx.rc.job_id, &error);
			if (error) {
				goto out;
			}
			log(TRACE, "%s: trec: %d", __func__,
			    trec->do_comp_commit_work);
			if (trec->do_comp_commit_work) {
				//Disable all phases
				disable_all_phases(&g_ctx.job_phase);
				//Enable only comp commit phase
				stf_job_phase_enable(&g_ctx.job_phase,
				    STF_PHASE_COMP_FINISH_COMMIT);
				//Since this is the first phase, change that
				find_first_enabled_phase(&g_ctx.job_phase);
			}
		}
	}

out:
	siq_target_record_free(trec);
	if (lock_fd != -1) {
		close(lock_fd);
		lock_fd = -1;
	}
	isi_error_handle(error, error_out);
	log(TRACE, "%s: exit", __func__);
}

void
add_first_work_item(struct coord_ctx *ctx, struct isi_error **error_out)
{
	char *tw_data = NULL;
	size_t tw_size = 0;
	struct isi_error *error = NULL;
	int jtype;
	struct rep_info_entry rie;
	bool do_idmap = true;
	bool is_compliance_v2 = false;

	log(TRACE, "add_first_work_item");

	ASSERT(ctx != NULL, "coordinator context is NULL");

	/* Make sure we have committed the Halfpipe upgrade. */
	is_compliance_v2 = ctx->rc.compliance_v2 &&
	    ctx->job_ver.common >= MSG_VERSION_HALFPIPE;

	if (is_treewalk(ctx)) {
		/*
		 * Compliance syncs will go through a separate state
		 * transition.
		 */
		jtype = is_compliance_v2 ? JOB_TW_COMP: JOB_TW_V3_5;
		tw_data = serialize_initial_work(ctx, &tw_size, &error);
		if (error)
			goto out;
	} else {
		jtype = is_compliance_v2 ? JOB_STF_LIN_COMP: JOB_STF_LIN_V3_5;
		if (jtype == JOB_STF_LIN_COMP && ctx->stf_upgrade_sync)
			jtype = JOB_TW_COMP;
	}

	/* Get state transition */
	init_job_phase_ctx(jtype, &ctx->job_phase);
	if (JOB_SPEC->common->action == SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK ||
	    JOB_SPEC->common->action == SIQ_ACT_SNAP_REVERT_DOMAIN_MARK ||
	    JOB_SPEC->common->action == SIQ_ACT_SYNCIQ_DOMAIN_MARK ||
	    JOB_SPEC->common->action == SIQ_ACT_WORM_DOMAIN_MARK) {
		ASSERT(jtype == JOB_TW_V3_5 || jtype == JOB_TW_COMP);
		disable_all_phases(&ctx->job_phase);
		stf_job_phase_enable(&ctx->job_phase, STF_PHASE_DOMAIN_MARK);
		ctx->job_phase.cur_phase = STF_PHASE_DOMAIN_MARK;
		do_idmap = false;
	} else if (jtype == JOB_STF_LIN_V3_5 || jtype == JOB_STF_LIN_COMP) {
		if (!ctx->restore) {
			get_initial_rep_info(ctx, &rie, &error);
			if (error)
				goto out;
			if ((!rie.is_sync_mode) &&
			    (ctx->rc.action == SIQ_ACT_SYNC)) {
				log(NOTICE, "Enabling stale dirs phase");
				stf_job_phase_enable(&ctx->job_phase,
				    STF_PHASE_CT_STALE_DIRS);
			}
			if (is_compliance_v2) {
				stf_job_phase_enable(&ctx->job_phase,
				    STF_PHASE_CC_COMP_RESYNC);
				stf_job_phase_enable(&ctx->job_phase,
				    STF_PHASE_CT_COMP_DIR_LINKS);
			}
		} else {
			stf_job_phase_disable(&ctx->job_phase,
			    STF_PHASE_FLIP_LINS);
			if (is_compliance_v2) {
				stf_job_phase_enable(&ctx->job_phase,
				    STF_PHASE_COMP_CONFLICT_CLEANUP);
				stf_job_phase_enable(&ctx->job_phase,
				    STF_PHASE_COMP_CONFLICT_LIST);
			}
		}
	}

	if (global_config->root->coordinator->samba_backup_enabled && do_idmap)
		stf_job_phase_enable(&ctx->job_phase, STF_PHASE_IDMAP_SEND);

	/*
	 * If the accelerated_failback option is defined, check if
	 * the additional mirror work needs to be done
	 */
	if (JOB_SPEC->coordinator->accelerated_failback &&
	    JOB_SPEC->common->action == SIQ_ACT_SYNC &&
	    !JOB_SPEC->datapath->disable_stf &&
	    !JOB_SPEC->datapath->disable_fofb) {
		if (!ctx->rc.database_mirrored) {
			stf_job_phase_enable(&ctx->job_phase,
			    STF_PHASE_DATABASE_RESYNC);
		}
	}

	/* Conditionally enable late stage commit phases */
	check_compliance_commit_phase(&error);
	if (error) {
		goto out;
	}

	//Conditionally enable comp dir dels phase
	//Only for incremental comp v2 syncs
	if (ctx->rc.compliance_v2 && !ctx->initial_sync && ctx->stf_sync &&
		!ctx->stf_upgrade_sync) {
		log(DEBUG, "Enabling STF_PHASE_COMP_DIR_DELS");
		stf_job_phase_enable(&ctx->job_phase,
		    STF_PHASE_CT_COMP_DIR_DELS);
	}

	checkpoint_next_phase(ctx, false, tw_data, tw_size,
	    true, &error);

out:
	if (tw_data)
		free(tw_data);
	isi_error_handle(error, error_out);
	return;
}

/*
 * return true if the job is completely finished false otherwise.
 */
bool
load_or_create_recovery_state(struct coord_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool found;
	bool done = false;

	found = entry_exists(STF_JOB_PHASE_LIN, ctx->rep, &error);
	if (error)
		goto out;

	if (ctx->failback_action &&
	    ctx->failback_action == SIQ_ACT_FAILBACK_PREP) {
		done = check_or_repair_for_failback_prep(ctx, found, &error);
		if (error)
			goto out;
	} else {
		if (!found) {
			add_first_work_item(&g_ctx, &error);
			if (error)
				goto out;
		} else {
			done = load_recovery_items(&error);
			if (error)
				goto out;
		}
	}

 out:
	isi_error_handle(error, error_out);
	return done;
}

/*
 * Function to check if policy root directory lins match between
 * old and new snapshots.
 */
static bool
snaproot_lin_match(struct siq_snap *prev_snap, struct siq_snap *curr_snap,
    struct isi_error **error_out)
{
	int ret;
	bool match = false;
	struct isi_error *error = NULL;
	struct stat prev_st, curr_st;
	struct path p;
	int prev_snap_fd = -1, curr_snap_fd = -1;

	ASSERT(prev_snap->root_fd != -1);
	ASSERT(curr_snap->root_fd != -1);
	/* Open the snapshot root */
	path_init(&p, g_ctx.source_base.path, NULL);

	prev_snap_fd = fd_rel_to_ifs(prev_snap->root_fd, &p);
	if (prev_snap_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open job root directory %s for snapshot:"
		    " %llu", p.path, g_ctx.prev_snap.id);
		goto out;
	}
	ret = fstat(prev_snap_fd, &prev_st);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Unable to stat root lin for snap %llu", prev_snap->id);
		goto out;
	}
	curr_snap_fd = fd_rel_to_ifs(curr_snap->root_fd, &p);
	if (curr_snap_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open job root directory %s for snapshot:"
		    " %llu", p.path, g_ctx.curr_snap.id);
		goto out;
	}
	ret = fstat(curr_snap_fd, &curr_st);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Unable to stat root lin for snap %llu", curr_snap->id);
		goto out;
	}

	match = prev_st.st_ino == curr_st.st_ino;

out:
	path_clean(&p);
	if (prev_snap_fd != -1) {
		close(prev_snap_fd);
	}
	if (curr_snap_fd != -1) {
		close(curr_snap_fd);
	}

	isi_error_handle(error, error_out);
	return match;
}

/**
 * Cleanup after an assess run.
 */

static void
cleanup_post_assess(void)
{
	char sel_rep_name[MAXNAMELEN];
	char base_rep_name[MAXNAMELEN];

	ifs_snapid_t snapid;

	if (!(g_ctx.rc.flags & FLAG_ASSESS)) {
		return;
	}
	
	siq_source_get_new(g_ctx.record, &snapid);

	if (snapid != INVALID_SNAPID) {
		get_base_rep_name(base_rep_name, g_ctx.rc.job_id,
		    g_ctx.restore);
		get_select_name(sel_rep_name, g_ctx.rc.job_id, snapid,
		    g_ctx.restore);

		delete_sbt(g_ctx.rc.job_id, base_rep_name);
		delete_sbt(g_ctx.rc.job_id, sel_rep_name);
	}

	delete_new_snapshot();
}

/*
 * Function to check if there is stale tmp-working-dir left
 * in the source tree which is possible when the source was
 * used as previous sync job target.
 */
static void
check_for_stale_tmp_dir(struct coord_ctx *ctx)
{
	int ret;
	struct isi_error *error = NULL;
	struct stat curr_st;
	struct path p;
	int curr_snap_fd = -1;

	ASSERT(ctx->curr_snap.root_fd != -1);

	/* Open the snapshot root */
	path_init(&p, ctx->source_base.path, NULL);

	curr_snap_fd = fd_rel_to_ifs(ctx->curr_snap.root_fd, &p);
	if (curr_snap_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open job root directory %s for snapshot:"
		    " %llu", p.path, g_ctx.curr_snap.id);
		goto out;
	}

	ret = enc_fstatat(curr_snap_fd, TMP_WORKING_DIR, ENC_DEFAULT,
	    &curr_st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to stat tmp working directory in"
		    "root directory %s for snap %llu",
		    ctx->source_base.path, ctx->curr_snap.id);
		goto out;
	}
	if (ret == -1)
		goto out;

	remove_snapshot_locks();
	delete_new_snapshot();
	fatal_error(false, SIQ_ALERT_TARGET_PATH_OVERLAP,
	    "Policy root path directory %s was previously used as sync target "
	    "for another policy and has stale %s directory remaining in root "
	    "directory. Please rename, move or remove %s from policy root "
	    "directory",
	    ctx->source_base.path, TMP_WORKING_DIR, TMP_WORKING_DIR);

out:
	path_clean(&p);
	if (curr_snap_fd != -1) {
		close(curr_snap_fd);
	}
	if (error)
		fatal_error_from_isi_error(false, error);
}

static void
lock_source_domain(struct coord_ctx *ctx, struct isi_error **error_out)
{

	int rv = 0;
	uint32_t gen = 0;
	ifs_domainid_t domain_id = 0;
	struct isi_error *error = NULL;

	find_domain(JOB_SPEC->datapath->root_path, DT_SYNCIQ, false,
	    &domain_id, &gen, &error);
	if (error)
		goto out;

	if (!domain_id) {
		error = isi_system_error_new(ENOENT,
		    "Unable to find SyncIQ domain for policy root %s",
		    JOB_SPEC->datapath->root_path);
		goto out;
	}

	rv = dom_set_readonly(domain_id, true);
	if (rv != 0) {
		error = isi_system_error_new(errno,
		    "Unable to set read only flag for domain %llu",
		    domain_id);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
state_pre_execution(void)
{
	bool used_failpoint = false;
	struct isi_error *error = NULL;
	unsigned d_flags = 0;
	/*
	 * Check if we are dealing with compliance domain and make sure that
	 * the sync policy is rooted at a compliance domain root.
	 */
	d_flags = check_compliance_domain_root(g_ctx.rc.path, &error);
	if (error) {
		fatal_error_from_isi_error(false, error);
	}
	/* We will assume that setting fake_compliance without a return value
	 * will mean that we want a v1 domain. */
	UFAIL_POINT_CODE(fake_compliance,
		g_ctx.rc.flags |= FLAG_COMPLIANCE;

		if (RETURN_VALUE >= 2) {
			g_ctx.rc.flags |= FLAG_COMPLIANCE_V2;
			g_ctx.rc.compliance_v2 = true;
		}
		d_flags = 0;
	);

	if (d_flags & DOM_COMPLIANCE) {
		g_ctx.rc.flags |= FLAG_COMPLIANCE;
		if (d_flags & DOM_COMPLIANCE_V2) {
			g_ctx.rc.flags |= FLAG_COMPLIANCE_V2;
			g_ctx.rc.compliance_v2 = true;
		}
	}

	UFAIL_POINT_CODE(create_conflict_log,
		used_failpoint = true;
		create_comp_conflict_log_dir(g_ctx.rc.job_id, JOB_SUMM->job_id,
		    &g_ctx.comp_conflict_log_dir, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_SIQ_CONFIG,
			    "Failed to create compliance conflict log dir: %s",
			    isi_error_get_message(error));
		g_ctx.rc.flags |= FLAG_LOG_CONFLICTS;
	);

	/*
	 * If it's compliance V2, and we have to potential to run into WORM
	 * committed file conflicts (during failover/failback), then create
	 * the WORM conflict log dir.
	 */
	if (g_ctx.rc.compliance_v2 &&
	    (g_ctx.rc.action == SIQ_ACT_FAILOVER ||
	    g_ctx.rc.action == SIQ_ACT_FAILOVER_REVERT ||
	    g_ctx.rc.action == SIQ_ACT_FAILBACK_PREP_RESTORE) &&
	    !used_failpoint) {
		create_comp_conflict_log_dir(g_ctx.rc.job_id, JOB_SUMM->job_id,
		    &g_ctx.comp_conflict_log_dir, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_SIQ_CONFIG,
			    "Failed to create compliance conflict log dir: %s",
			    isi_error_get_message(error));
		g_ctx.rc.flags |= FLAG_LOG_CONFLICTS;
	}

	/*
	 * Here we would change the action to sync and set the 
	 * restore bit. This would be done later when integrating
	 * with failover job.
	 */

	switch (g_ctx.rc.action) {
	case SIQ_ACT_FAILOVER:
		g_ctx.rc.action = SIQ_ACT_SYNC;
		g_ctx.rc.flags |= FLAG_RESTORE;
		ASSERT(!(g_ctx.rc.flags & FLAG_SNAPSHOT_SEC));
		g_ctx.restore = true;
		g_ctx.use_restore_job_vers = true;
		g_ctx.failover = true;
		JOB_SUMM->sync_type = ST_FOFB;
		break;

	case SIQ_ACT_FAILOVER_REVERT:
		g_ctx.rc.action = SIQ_ACT_SYNC;
		g_ctx.rc.flags |= FLAG_RESTORE;
		ASSERT(!(g_ctx.rc.flags & FLAG_SNAPSHOT_SEC));
		g_ctx.restore = true;
		g_ctx.use_restore_job_vers = true;
		g_ctx.failover_revert = true;
		JOB_SUMM->sync_type = ST_FOFB;
		break;

	case SIQ_ACT_FAILBACK_PREP:
		g_ctx.failback_action = SIQ_ACT_FAILBACK_PREP;
		g_ctx.rc.action = SIQ_ACT_SYNC;
		g_ctx.rc.flags |= FLAG_FAILBACK_PREP;
		g_ctx.rc.flags &= ~FLAG_SNAPSHOT_SEC;
		check_create_prep_restore_trec(&error);
		if (error)
			fatal_error_from_isi_error(false, error);
		JOB_SUMM->sync_type = ST_FOFB;
		break;

	case SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK:
	case SIQ_ACT_SNAP_REVERT_DOMAIN_MARK:
	case SIQ_ACT_SYNCIQ_DOMAIN_MARK:
	case SIQ_ACT_WORM_DOMAIN_MARK:
		g_ctx.failback_action = g_ctx.rc.action;
		g_ctx.rc.action = SIQ_ACT_SYNC;
		g_ctx.rc.flags |= FLAG_FAILBACK_PREP;
		g_ctx.rc.flags &= ~FLAG_SNAPSHOT_SEC;
		g_ctx.rc.flags &= ~FLAG_STF_SYNC;
		g_ctx.rc.flags |= FLAG_DOMAIN_MARK;
		JOB_SUMM->sync_type = ST_DOMAINMARK;
		// This fn exit()s.
		do_domain_treewalk();
		break;

	case SIQ_ACT_FAILBACK_PREP_RESTORE:
		g_ctx.rc.action = SIQ_ACT_SYNC;
		g_ctx.rc.flags |= FLAG_RESTORE;
		g_ctx.rc.flags |= FLAG_FAILBACK_PREP;
		g_ctx.rc.flags &= ~FLAG_SNAPSHOT_SEC;
		g_ctx.failback_action = SIQ_ACT_FAILBACK_PREP_RESTORE;
		g_ctx.restore = true;
		g_ctx.use_restore_job_vers = true;
		JOB_SUMM->sync_type = ST_FOFB;
		lock_source_domain(&g_ctx, &error);
		if (error) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    isi_error_get_message(error));
		}
		failback_prep_restore_rep_rename(true, &error);
		if (error) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    isi_error_get_message(error));
		}
		break;

	case SIQ_ACT_FAILBACK_PREP_FINALIZE:
		g_ctx.failback_action = SIQ_ACT_FAILBACK_PREP_FINALIZE;
		g_ctx.rc.flags |= FLAG_FAILBACK_PREP;
		g_ctx.rc.flags &= ~FLAG_STF_SYNC;
		g_ctx.rc.flags &= ~FLAG_SNAPSHOT_SEC;
		JOB_SUMM->sync_type = ST_FOFB;
		// This fn exit()s.
		do_failback_prep_finalize();
		break;

	/* If its Migrate/Remove, then disable it */
	case SIQ_ACT_MIGRATE:
	case SIQ_ACT_REMOVE:
		disable_policy(g_ctx.rc.job_id);
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "SyncIQ migrate/remove policies are no longer supported");
		break;

	case SIQ_ACT_REPLICATE:
	case SIQ_ACT_SYNC:
		/* For now no special action here */
	default:
		break;
	}
}

static void
check_for_failback(struct coord_ctx *ctx, struct isi_error **error_out)
{
	enum siq_failback_state fb_state = SIQ_FBS_NONE;
	struct isi_error *error = NULL;

	/* Only do the check for non resync-prep actions. */
	if ((ctx->rc.flags & FLAG_FAILBACK_PREP) == FLAG_FAILBACK_PREP)
		return;

	siq_source_get_failback_state(ctx->record, &fb_state);
	if (fb_state > SIQ_FBS_NONE && fb_state < SIQ_FBS_SYNC_READY) {
		error = isi_siq_error_new(E_SIQ_TGT_DIR,
		    "Policy %s cannot run during Failover/Failback.",
		    ctx->rc.job_name);
		goto out;
	}
out:
	isi_error_handle(error, error_out);
}

/*  __  __       _       
 * |  \/  | __ _(_)_ __  
 * | |\/| |/ _` | | '_ \ 
 * | |  | | (_| | | | | |
 * |_|  |_|\__,_|_|_| |_|
 */

int
main(int argc, char **argv)
{
	siq_plan_t *plan = NULL;
	struct isi_error *error = NULL;
	bool success = false;
	bool match = false;
	ifs_snapid_t path_snapid;

	/* NOTE: this can call exit(0) */
	do_initialization(argc, argv);

	setproctitle(g_ctx.rc.job_name);
	
	/* Temporary workaround to allow UFP to work */
	UFAIL_POINT_CODE(TEMPFAIL, true);

	if (g_ctx.rc.is_delete_job) {
		delete_policy();
	}

	if (g_ctx.stf_downgrade_op) {
		stf_policy_downgrade();
	}

	/* If doing an assess, diff sync makes no sense */
	if (g_ctx.rc.flags & FLAG_ASSESS)
		g_ctx.rc.flags &= ~FLAG_DIFF_SYNC;

	/*
	 * Assign all the supported versions to be exchanged with
	 * the target. Later we exchange common version which is
	 * is used by both. All the other version bits will be reset.
	 */
	g_ctx.rc.flags |= FLAG_VER_3_5;
	g_ctx.rc.flags |= FLAG_VER_CLOUDPOOLS;

	/*
	 * Execute pre routine for state transition.
	 */
	state_pre_execution();

	/* set last run time to emulate conditional delete with mtime */
	g_ctx.last_time = JOB_SPEC->datapath->siq_v1_lastrun;

	if (g_ctx.rc.predicate && strlen(g_ctx.rc.predicate) > 0) {
		log(TRACE, "%s uses criteria \"%s\"", SIQ_JNAME(&g_ctx.rc),
		    g_ctx.rc.predicate);
		plan = siq_parser_parse(g_ctx.rc.predicate, time(NULL));
		if (plan == NULL)
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Failed to compile predicate %s",
			    g_ctx.rc.predicate);
		log(DEBUG, "info req for plan: fname %d dname %d size %d "
		    "path %d type %d date %d", plan->need_fname,
		    plan->need_dname, plan->need_size, plan->need_path,
		    plan->need_type, plan->need_date);
	}

	/*
	 * In case its sync job and time/path predicates are set,
	 * quit the job, alert the user, also disable policy
	 */
	if (plan && (plan->need_date || plan->need_path)
	    && (g_ctx.rc.action == SIQ_ACT_SYNC)) {
		disable_policy(g_ctx.rc.job_id);
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Policy contains predicates based on time "
		    "or path which are no longer supported with sync jobs");
	}

	if (g_ctx.rc.noop) {
		exit(EXIT_SUCCESS);
	}

	/* open file for log deleted files */
	g_ctx.rc.fd_deleted = -1;
	open_deleted_list_file();

	srandom(getpid());
	srandom(random() * time(0) * getpid());
	srandom(random() * random() * time(0) * getpid());
	JOB_SUMM->retry = true; /* from now on we're running */
	save_report();

	setup_restrictions(JOB_SPEC->target->restrict_by,
	    JOB_SPEC->target->force_interface);

	/* initiate jobctl timeouts */
	jobctl_callback(NULL);

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

	/* Create and lock snapshots as needed */
	snapshot_set_dotsnap(1, 0, NULL);
	check_and_create_snap();

	/* HACK: Fail assess for non-initial sync for now. */
	if (!g_ctx.initial_sync && (g_ctx.rc.flags & FLAG_ASSESS))
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "failing assess for non-initial sync.");

	/* Store snapid for snapshot restore jobs */
	if (g_ctx.snap_revert_op)
		g_ctx.snap_revert_snapid = g_ctx.prev_snap.id;

	if (JOB_SPEC->common->action == SIQ_ACT_REPLICATE ||
	    JOB_SPEC->common->action == SIQ_ACT_SYNC)
		path_snapid = g_ctx.curr_snap.id;
	else
		path_snapid = HEAD_SNAPID;

	/* Process includes/excludes. */
	process_paths(global_config->root, &g_ctx.rc, path_snapid);

	/* STF info */
	g_ctx.stf_sync = (bool)(g_ctx.rc.flags & FLAG_STF_SYNC);
	if (g_ctx.stf_sync && g_ctx.initial_sync)
		siq_source_set_pol_stf_ready(g_ctx.record);

	if (g_ctx.stf_sync && !g_ctx.initial_sync &&
	    !siq_source_pol_stf_ready(g_ctx.record)) {
		g_ctx.stf_upgrade_sync = true;
		g_ctx.rc.flags |= FLAG_STF_UPGRADE_SYNC;
	} else
	    	g_ctx.stf_upgrade_sync = false;

	if ((g_ctx.stf_sync && g_ctx.initial_sync) || g_ctx.stf_upgrade_sync) {
		if (g_ctx.rc.disable_bulk_lin_map && g_ctx.rc.doing_diff_sync) {
			log(DEBUG, "Disabling bulk lin map updates");
		} else {
			g_ctx.rc.flags |= FLAG_LIN_MAP_BLK;
		}
	}

	/* might want to add asserts here to catch invalid combinations */
	if (JOB_SUMM->sync_type == ST_INVALID) {
		if (g_ctx.initial_sync)
			JOB_SUMM->sync_type = ST_INITIAL;
		else if (g_ctx.stf_sync && !g_ctx.initial_sync &&
		    !g_ctx.stf_upgrade_sync)
			JOB_SUMM->sync_type = ST_INCREMENTAL;
		else if (g_ctx.stf_upgrade_sync && !g_ctx.initial_sync)
			JOB_SUMM->sync_type = ST_UPGRADE;
		else if (!g_ctx.initial_sync && !g_ctx.stf_sync &&
		    !g_ctx.stf_upgrade_sync && (g_ctx.rc.flags & FLAG_EXPECTED_DATALOSS))
			JOB_SUMM->sync_type = ST_INCREMENTAL;
		else if (!g_ctx.initial_sync && !g_ctx.stf_sync &&
		    !g_ctx.stf_upgrade_sync)
			JOB_SUMM->sync_type = ST_LEGACY;
		else
			ASSERT(!"bad job configuration detected");
	}
	
	if (JOB_SUMM->sync_type == ST_INCREMENTAL) {
		/* Make sure that the policy root lins match */
		match = snaproot_lin_match(&g_ctx.prev_snap, &g_ctx.curr_snap,
		    &error);
		if (error)
			fatal_error_from_isi_error(false, error);
		if (!match) {
			remove_snapshot_locks();
			delete_new_snapshot();
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Policy root path directory %s does not match "
			    "between old and new snapshots",
			    g_ctx.source_base.path);
		}
	}

	/* Check if the source has stale tmp working dir */	
	if (!(g_ctx.rc.flags & FLAG_RESTORE))
		check_for_stale_tmp_dir(&g_ctx);

	/* Check if we are in the middle of resync prep. */
	check_for_failback(&g_ctx, &error);
	if (error)
		fatal_error_from_isi_error(false, error);

	/* Verify that the repstate(s) we expect to exists are present, and
	 * that we succeed in creating the new repstates for this run. If this
	 * is a restart, the new repstates may already exist */
	create_or_open_rep(&g_ctx, &error);
	if (error)
		fatal_error_from_isi_error(false, error);

	pre_run_prune_worklist_and_summ_stf(g_ctx.curr_snap.id, &error);
	if (error)
		goto out;

	/* If --changelist option is set, retain snapshots and
	 * incremental repstate for use by the changelist job. */
	if (JOB_SPEC->coordinator->changelist && !g_ctx.initial_sync) {
		g_ctx.changelist_op = true;
	}

	/**
	 * Check to see whether we should tell the other side to
	 * delete a linmap if it already exists.  For an initial
	 * sync or upgrade sync, if the repstate doesn't list
	 * a job state we haven't gotten past the coordinator initialization,
	 * so it's ok to just delete and recreate the linmap.  We NEED to
	 * do this in some cases (such as a policy reset) where there's already
	 * a linmap on the target but it's no longer relevant, and it doesn't
	 * hurt in other cases.
	 */
	if (g_ctx.initial_sync || g_ctx.stf_upgrade_sync || g_ctx.restore) {
		bool found_phase_lin;
		found_phase_lin = entry_exists(STF_JOB_PHASE_LIN, g_ctx.rep,
		    &error);
		if (error)
			fatal_error_from_isi_error(false, error);
		if (!found_phase_lin)
			g_ctx.rc.flags |= FLAG_STF_LINMAP_DELETE;
	}

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
		siq_source_set_node_conn_failed(g_ctx.record, get_local_node_id());
		siq_job_error_msg_append(JOB_SUMM,
		    "Source node could not connect to target cluster.");
		log(NOTICE, "%s", isi_error_get_message(error));

		/* we no longer blacklist nodes that have previously failed
		 * -- any node is still free to retry. schedulers will spin
		 * on restarting failed coordinators unless we introduce a
		 * small delay on exit */
		sleep(5);
		exit(EXIT_FAILURE);
	}

	if (g_ctx.failback_action && !(g_ctx.curr_ver & FLAG_VER_3_5)) {
		fatal_error(false, SIQ_ALERT_VERSION,
		    "Target for %s does not support Failback.",
		    g_ctx.rc.job_name);
		exit(EXIT_FAILURE);
	}

	//XXX HACK
	if (JOB_SPEC->common->action == SIQ_ACT_FAILBACK_PREP)
		verify_failover_done();

	/*
	 * If it is local sync job, try creating "running" folder
	 * which will have policy_id/timestamp to detect multiple jobs for
	 * same slice
	 */
	if (strcmp(g_ctx.rc.target, "localhost") == 0 ||
	    strcmp(g_ctx.rc.target, "127.0.0.1") == 0 ||
	    strcmp(g_ctx.rc.target, "::1") == 0) {
		create_syncrun_file(g_ctx.rc.job_id, g_ctx.run_id, &error);
		if (error)
			fatal_error_from_isi_error(false, error);
	}

	migr_register_group_callback(group_callback, NULL);

	/* Begin job processing, for pruning and repeats. */
	g_ctx.start = time(0);

	log(DEBUG, "Starting %s %s sync",
	    g_ctx.stf_sync ? "stf" : "normal",
	    g_ctx.stf_upgrade_sync ? "upgrade" :
	    (g_ctx.initial_sync ? "initial" : "update"));

	create_sel_tree(&g_ctx, &error);
	if (error)
		fatal_error_from_isi_error(false, error);

	log(DEBUG, "sync source base: %s", g_ctx.source_base.path);
	log(DEBUG, "sync target base: %s", g_ctx.target_base.path);

	g_ctx.conn_alert = time(0);

	/* Make a snapshot on the secondary before we begin iff:
	 * -This is an initial sync
	 * -Target Snapshot is enabled */
	if ((g_ctx.rc.flags & FLAG_SNAPSHOT_SEC) &&
	    !(g_ctx.rc.flags & FLAG_RESTORE))
		pre_sync_snap();

	/*
	 * If its new sync, then create first work item,
	 * otherwise load the existing recovery state.
	 */
	success = load_or_create_recovery_state(&g_ctx, &error);
	if (error)
		fatal_error_from_isi_error(false, error);
	if (success)
		goto out;

	/* Get a list of local cluster node IPs */
	get_source_ip_list();/////获得当前这个cluster中所有node的ip

	/* At this point we have a list of source and target cluster node ips.
	 * Allocate and distribute pworkers, pair them with sworkers, etc.
	 * This arrangement may change dynamically if the source or target
	 * cluster experiences a group change during the sync. */
	configure_workers(true);

	/* connect to bandwidth and throttle hosts, and start sending stats */
	connect_worker_host();
	connect_bandwidth_host();
	connect_throttle_host();  /////不考虑
	migr_register_timeout(&bwt_timeout, bwt_update_timer_callback, 0);

	/* Enforce loopback replication safety with IP and path checks. */
	if ((g_ctx.rc.flags & FLAG_RESTORE) == false)
		check_for_illegal_loopback();

	/* register redistribute timeout */
	register_redistribute_work();
	
	ifs_setnoatime(true); /* Ignore errors, if any */

	if (migr_process() < 0)
		set_job_state(SIQ_JS_FAILED);
	else
		success = true;

	/* Simulate compliance conflicts during a sync. This should show up in
	 * the report and in a CELOG event. */
	UFAIL_POINT_CODE(force_comp_conflicts,
		JOB_SUMM->total->stat->compliance->conflicts = RETURN_VALUE;
	);

 out:
	if (success) {
		post_primary_success_work();
		/* if a prior job set an alert, cancel it since we have
		 * succeeded (exclude rpo alerts and compliance conflicts,
		 * these are not part of the synciq event group). */
		siq_cancel_alerts_for_policy(g_ctx.rc.job_name,
		    g_ctx.rc.job_id,
		    (enum siq_alert_type []) {SIQ_ALERT_RPO_EXCEEDED,
		    SIQ_ALERT_COMP_CONFLICTS}, 2);
	}

	exit(EXIT_SUCCESS);
}

void
connect_bandwidth_host()
{
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	int i;
	time_t now = time(0);
	char **ip_list = NULL;
	struct version_status ver_stat;
	uint32_t ver = 0;

	/* connect to bandwidth host. the bandwidth host may or may not be
	 * specified, and may be on a remote cluster */
	ASSERT(g_ctx.bw_host == -1);

	if (g_ctx.bw_conn_failures && now - g_ctx.bw_last_conn_attempt <
	    CONNECT_WAIT[g_ctx.bw_conn_failures - 1])
		goto out;

	g_ctx.bw_last_conn_attempt = now;

	if (g_ctx.rc.bwhost == NULL || g_ctx.rc.disable_bw) {
		/* no bandwidth limit */
		g_ctx.bw_ration = -1;
		reallocate_bandwidth();
		goto out;
	}

	/* Use the most recently-committed version for throttle host
	 * connections. (Could use g_ctx.ver_stat.committed_version if
	 * .local_node_upgraded isn't set, but this is simpler and helps
	 * isolate bandwidth/throttling connection upgrade testing.) */
	get_version_status(true, &ver_stat);

	if (get_cluster_ips(g_ctx.rc.bwhost/*忽略*/,                  ////为了获得整个cluster上各个node的ip
	    global_config->root->coordinator->ports->sworker/*忽略*/,
	    ver_stat.committed_version, &ip_list, &error) <= 0) {
		log(FATAL, "could not retrieve IP addresses for "
		    "bandwidth host %s: %s", g_ctx.rc.bwhost,
		    isi_error_get_message(error));
	}

	for (i = 0; ip_list[i]; i++) {
		if (error) {
			isi_error_free(error);
			error = NULL;
		}

		if ((g_ctx.bw_host = connect_to(ip_list[i],                   //////第一个在3148端口上连接成功的就是master bandwidth
		    global_config->root->bwt->port/*3148*/, POL_MATCH_OR_FAIL,
		    ver_stat.committed_version, &ver, NULL, false,
		    &error)) > 0) {
			log(TRACE, "connected to bandwidth host at %s",
			    ip_list[i]);
			log_version_ufp(ver, SIQ_COORDINATOR, __func__);
			break;
		}
	}

	if (g_ctx.bw_host == -1) {
		ASSERT(error);
		if (now - g_ctx.bw_last_conn >= BW_TH_TIMEOUT) {
			fatal_error(true,
			    SIQ_ALERT_DAEMON_CONNECTIVITY,
			    "Unable to connect to bandwidth host for "
			    "last %d seconds", now - g_ctx.bw_last_conn);
		} else {
			log(ERROR,
			    "Failed to connect to bandwidth host: %s",
		    	    isi_error_get_message(error));
			g_ctx.bw_conn_failures++;
			if (g_ctx.bw_conn_failures > NUM_WAIT)
			    g_ctx.bw_conn_failures = NUM_WAIT;
			log(DEBUG, "Delaying bandwidth host reconnect "
			    "at %d failures to %d seconds",
			    g_ctx.bw_conn_failures,
			    CONNECT_WAIT[g_ctx.bw_conn_failures - 1]);
		}
		goto out;
	}

	g_ctx.bw_conn_failures = 0;

	/* bandwidth ration is left at 0 until we hear from bw host */

	migr_add_fd(g_ctx.bw_host, &g_ctx, "bw_host");
	migr_register_callback(g_ctx.bw_host, BANDWIDTH_MSG,
	    bandwidth_callback);
	migr_register_callback(g_ctx.bw_host, DISCON_MSG,
	    bandwidth_disconnect_callback);

	msg.head.type = BANDWIDTH_INIT_MSG;
	msg.body.bandwidth_init.policy_name = g_ctx.rc.job_name;////policy_name + time(0)
	msg.body.bandwidth_init.num_workers = g_ctx.workers->count;////初始workers->count = 0;以后workers->count在coord_worker.c中修改(修改worker pool)
	msg.body.bandwidth_init.priority = 0;
	msg_send(g_ctx.bw_host, &msg);

out:
	if (ip_list)
		free_ip_list(ip_list);
	isi_error_free(error);
	return;
}

void
connect_throttle_host()
{
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	int i;
	time_t now = time(0);
	char **ip_list = NULL;
	struct version_status ver_stat;
	uint32_t ver = 0;

	/* connect to throttle host which is always on the local cluster */
	ASSERT(g_ctx.th_host == -1);

	if (g_ctx.src_nodes == NULL)
		goto out;

	if (g_ctx.th_conn_failures && now - g_ctx.th_last_conn_attempt <
	    CONNECT_WAIT[g_ctx.th_conn_failures - 1])
		goto out;
	g_ctx.th_last_conn_attempt = now;
	
	if (g_ctx.rc.disable_th) {
		/* no bandwidth limit */
		g_ctx.th_ration = -1;
		reallocate_throttle();
		goto out;
	}

	/* Use the most recently-committed version for throttle host
	 * connections. (Could use g_ctx.ver_stat.committed_version if
	 * .local_node_upgraded isn't set, but this is simpler and helps
	 * isolate bandwidth/throttling connection upgrade testing.) */
	get_version_status(true, &ver_stat);

	/* get back end IPs */
	if (get_cluster_ips("localhost", 0, ver_stat.committed_version,
	    &ip_list, &error) <= 0) {
		log(FATAL, "could not retrieve IP addresses for "
		    "throttle host: %s", isi_error_get_message(error));
	}

	for (i = 0; ip_list[i]; i++) {
		if (error) {
			isi_error_free(error);
			error = NULL;
		}

		if ((g_ctx.th_host = connect_to(ip_list[i],
		    global_config->root->bwt->port, POL_MATCH_OR_FAIL,
		    ver_stat.committed_version, &ver, NULL, false,
		    &error)) > 0) {
			log(TRACE, "connected to throttle host at %s",
			    g_ctx.src_nodes->ips[i]);
			log_version_ufp(ver, SIQ_COORDINATOR, __func__);
			break;
		}
	}

	if (g_ctx.th_host == -1) {
		ASSERT(error);
		if (now - g_ctx.th_last_conn >= BW_TH_TIMEOUT)
			fatal_error(true, SIQ_ALERT_DAEMON_CONNECTIVITY,
			    "Unable to connect to throttle host for last %d "
			    "seconds", now - g_ctx.th_last_conn);
		else {
			log(ERROR,
			    "Failed to connect to throttle host: %s",
			    isi_error_get_message(error));
			g_ctx.th_conn_failures++;
			if (g_ctx.th_conn_failures > NUM_WAIT)
			    g_ctx.th_conn_failures = NUM_WAIT;
			log(DEBUG, "Delaying throttle host reconnect at %d "
			    "failures to %d seconds", g_ctx.th_conn_failures,
			    CONNECT_WAIT[g_ctx.th_conn_failures - 1]);
		}
		goto out;
	}

	g_ctx.th_conn_failures = 0;

	migr_add_fd(g_ctx.th_host, &g_ctx, "th_host");
	migr_register_callback(g_ctx.th_host, THROTTLE_MSG, throttle_callback);
	migr_register_callback(g_ctx.th_host, DISCON_MSG,
	    throttle_disconnect_callback);

	msg.head.type = THROTTLE_INIT_MSG;
	msg.body.throttle_init.policy_name = g_ctx.rc.job_name;
	msg.body.throttle_init.num_workers = g_ctx.workers->count;
	msg_send(g_ctx.th_host, &msg);

out:
	if (ip_list)
		free_ip_list(ip_list);

	isi_error_free(error);
	return;
}

void
connect_worker_host()
{
	struct isi_error *error = NULL;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;
	int i;
	time_t now = time(0);
	char **ip_list = NULL;
	struct version_status ver_stat;
	uint32_t ver = 0;

	/* connect to bandwidth host. the bandwidth host may or may not be
	 * specified, and may be on a remote cluster */
	ASSERT(g_ctx.work_host == -1);

	if (g_ctx.work_conn_failures && now - g_ctx.work_last_conn_attempt <
	    CONNECT_WAIT[g_ctx.work_conn_failures - 1])
		goto out;

	g_ctx.work_last_conn_attempt = now;

	if (g_ctx.rc.bwhost == NULL || g_ctx.rc.disable_work) {
		/* no worker limit */
		g_ctx.wk_ration = 0;
		if (g_ctx.work_node_mask) {
			free(g_ctx.work_node_mask);
			g_ctx.work_node_mask = NULL;
		}
		if (g_ctx.work_lnn_list) {
			free(g_ctx.work_lnn_list);
			g_ctx.work_lnn_list = NULL;
			g_ctx.work_lnn_list_len = 0;
		}
		g_ctx.work_last_worker_inc = 0;
		
		goto out;
	}

	UFAIL_POINT_CODE(fail_connect_worker_host,
	    log(ERROR, "fail_connect_worker_host failpoint enabled: "
	        "skipping worker host connection.");
	    goto out;);

	/* Use the most recently-committed version for worker host
	 * connections. (Could use g_ctx.ver_stat.committed_version if
	 * .local_node_upgraded isn't set, but this is simpler and helps
	 * isolate bandwidth/throttling connection upgrade testing.) */
	get_version_status(true, &ver_stat);

	if (get_cluster_ips(g_ctx.rc.bwhost,
	    global_config->root->coordinator->ports->sworker,
	    ver_stat.committed_version, &ip_list, &error) <= 0) {
		log(FATAL, "could not retrieve IP addresses for "
		    "worker host %s: %s", g_ctx.rc.bwhost,
		    isi_error_get_message(error));
	}

	for (i = 0; ip_list[i]; i++) {
		if (error != NULL) {
			isi_error_free(error);
			error = NULL;
		}

		if ((g_ctx.work_host = connect_to(ip_list[i],
		    global_config->root->bwt->port, POL_MATCH_OR_FAIL,
		    ver_stat.committed_version, &ver, NULL, false,
		    &error)) > 0) {
			log(TRACE, "connected to worker host at %s",
			    ip_list[i]);
			log_version_ufp(ver, SIQ_COORDINATOR, __func__);
			break;
		}
	}

	if (g_ctx.work_host == -1) {
		ASSERT(error != NULL);
		if (now - g_ctx.work_last_conn >= BW_TH_TIMEOUT) {
			fatal_error(true,
			    SIQ_ALERT_DAEMON_CONNECTIVITY,
			    "Unable to connect to worker host for "
			    "last %d seconds", now - g_ctx.work_last_conn);
		} else {
			log(ERROR,
			    "Failed to connect to worker host: %s",
		    	    isi_error_get_message(error));
			g_ctx.work_conn_failures++;
			if (g_ctx.work_conn_failures > NUM_WAIT)
			    g_ctx.work_conn_failures = NUM_WAIT;
			log(DEBUG, "Delaying worker host reconnect "
			    "at %d failures to %d seconds",
			    g_ctx.work_conn_failures,
			    CONNECT_WAIT[g_ctx.work_conn_failures - 1]);
		}
		goto out;
	}

	g_ctx.work_conn_failures = 0;

	/* init tracking variables */
	g_ctx.wk_ration = 0; /////当前这个job的worker ration
	if (g_ctx.work_node_mask) {
		free(g_ctx.work_node_mask);
		g_ctx.work_node_mask = NULL;
	}

	migr_add_fd(g_ctx.work_host, &g_ctx, "work_host");
	migr_register_callback(g_ctx.work_host,
	    build_ript_msg_type(WORKER_MSG), worker_callback);
	migr_register_callback(g_ctx.work_host, DISCON_MSG,
	    worker_disconnect_callback);

	/* Get the list of valid nodes in the subnet pool if we are
	 * reconnecting to the worker host. */
	if (g_ctx.work_lnn_list == NULL || g_ctx.work_lnn_list_len == 0)
		get_source_ip_list();

	msg.head.type = build_ript_msg_type(WORKER_INIT_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);

	ript_msg_set_field_str(ript, RMF_POLICY_NAME, 1,
	    g_ctx.rc.job_name);

	lnn_list_to_char();
	ript_msg_set_field_str(ript, RMF_NODE_MASK, 1,
	    g_ctx.init_node_mask);

	msg_send(g_ctx.work_host, &msg);

out:
	if (ip_list)
		free_ip_list(ip_list);

	/*
	 * Free the memory allocated by get_source_ip_list() (called earlier
	 * to get source IPs in the case where work_lnn_list is empty) for
	 * new_src_nodes in the coordinator context.
	 */
	if (g_ctx.new_src_nodes) {
		free(g_ctx.new_src_nodes);
		g_ctx.new_src_nodes = NULL;
	}

	isi_error_free(error);
	return;
}

