#include <time.h>

#include "isi_snapshot/snapshot.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_report.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/sched/sched_reports.h"
#include "isi_migrate/sched/sched_snap_locks.h"
#include "isi_sra_gcfg/isi_sra_gcfg.h"
#include "isi_sra_gcfg/dbmgmt.h"
#include "isi_date/date.h"
#include "isi_ufp/isi_ufp.h"
#include "sched_includes.h"
#include "sched_change_based.h"
#include "sched_snap_trig.h"

static int start_job(struct sched_ctx *, struct siq_policy *pol,
    enum siq_job_action pending_job, time_t curr_time, time_t end_time);

static int generate_skip_when_source_unmodified_report(struct sched_ctx *,
    struct siq_policy *, struct siq_source_record *srec, enum siq_job_action,
    time_t);

bool
get_schedule_info(char *schedule,
    char **recur_str, time_t *base_time)
{
	bool result = false;
	char *recur_buf = NULL, *recur_ptr = NULL;

	recur_buf = strdup(schedule);
	ASSERT(recur_buf);

	//  Recurring string for a schedule entry has the format:
	//      UTC time | recurring date
	recur_ptr = strchr(recur_buf, '|');
	if (recur_ptr == NULL) {
		log(ERROR, "illegal format for recurring date: %s", recur_buf);
		goto out;
	}

	*recur_ptr = '\0';
	recur_ptr++;
	*base_time = strtoul(recur_buf, (char **)NULL, 10);
	*recur_str = strdup(recur_ptr);
	ASSERT(recur_str);

	result = true;
out:
	free(recur_buf);

	return result;
}

static bool
sched_check_for_work(struct sched_ctx *sctx, struct siq_policy *pol,
    struct siq_source_record *srec, time_t curr_time, time_t start_time,
    bool *skip_sync, struct isi_error **error_out)
{
	time_t base_time;
	char *recur_str = NULL;
	struct date_pat *recur_date_pat = NULL;
	struct dp_record *date_record = NULL;
	struct isi_error *error = NULL;
	time_t  next_time;
	bool work = false;

	log(TRACE, "check_for_work_sched: enter");

	if (!get_schedule_info(pol->scheduler->schedule, &recur_str,
	    &base_time))
		goto out;

	log(TRACE, "Date: %s", recur_str);

	/* change-based policies are a special case */
	if (is_policy_change_based(pol, recur_str)) {
		work = check_for_change_based_work(sctx, pol, srec,
		    curr_time, false, &error);
		goto out;
	}

	/* snapshot-triggered policies are a special case */
	if (is_policy_snap_trig(pol, recur_str)) {
		work = check_for_snap_trig_work(sctx, pol, srec, &error);
		goto out;
	}

	/* schedule changed */
	if (start_time < base_time)
		start_time = 0;

	parse_date(recur_str, &base_time, &recur_date_pat, &date_record,
	    &error);
	if (error) {
		log(ERROR, "check_for_work_sched: Parse_date(%s): %s\n",
		    recur_str, isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
		goto out;
	}

	if (start_time == 0) {
		if (date_record->type != DP_REC_NON_RECURRING)
			start_time = base_time;
	} else {
		start_time += 60; // XXX: isi_date rounding
	}

	log(TRACE, "last_start_time: %s", ctime(&start_time));
	log(TRACE, "curr_time: %s", ctime(&curr_time));

	if (date_pat_get_next_match(recur_date_pat, start_time,
	    curr_time, &next_time) != 0) {
		log(TRACE, "Hit: next_time = %s", ctime(&next_time));

		// Only check for changes if we are doing a copy or sync.
		if (pol->scheduler->skip_when_source_unmodified &&
		    (pol->common->action == SIQ_ACT_REPLICATE ||
		    pol->common->action == SIQ_ACT_SYNC)) {
			ASSERT(skip_sync != NULL);
			*skip_sync = !check_for_change_based_work(sctx,
			    pol, srec, curr_time, true, &error);
			if (*skip_sync) {
				log(TRACE, "No changes detected for %s",
				    pol->common->name);
				work = false;
				goto out;
			}
		}

		work = true;
		goto out;
	} else
		log(TRACE, "No Hits.");

out:
	if (recur_date_pat)
		date_pat_free(recur_date_pat);
	if (date_record)
		dp_record_free(date_record);
	free(recur_str);
	isi_error_handle(error, error_out);

	return work;
}

static bool
policy_should_run(struct sched_ctx *sctx, struct siq_policy *pol,
    struct siq_source_record *srec, time_t curr_time, time_t *end_time,
    enum siq_job_action *pending_job, bool *skip_sync,
    struct isi_error **error_out)
{
	time_t start_time;
	bool run = false;
	struct isi_error *error = NULL;

	*pending_job = SIQ_JOB_NONE;
	siq_source_get_pending_job(srec, pending_job);
	siq_source_get_sched_times(srec, &start_time, end_time);

	/* skip if policy is being deleted or is unrunnable */
	if (!siq_source_should_run(srec))
		goto out;

	if (pol->common->state != SIQ_ST_ON &&
	    !siq_source_is_policy_deleted(srec))
		goto out;

	if (!SIQ_LIC_OK(pol->tasks->action.type))
		goto out;

	if (*pending_job != SIQ_JOB_NONE) {
		run = true;
		goto out;
	}

	if (pol->scheduler->schedule == NULL ||
	    strcmp(pol->scheduler->schedule, "") == 0)
		goto out;

	if (!sched_check_for_work(sctx, pol, srec, curr_time, start_time,
	    skip_sync, &error))
		goto out;

	run = true;

out:
	isi_error_handle(error, error_out);
	return run;
}

static void
sched_main_cleanup_snap_locks(void)
{
	char lock_filename[MAXPATHLEN + 1];
	int fd = -1;
	struct isi_error *error = NULL;
	
	/* Only one scheduler can do this at a time */
	snprintf(lock_filename, MAXPATHLEN + 1,  "%s/.snap_locks_cleanup.lock",
	    sched_get_lock_dir());
	fd = acquire_lock(lock_filename, O_EXLOCK | O_NONBLOCK, &error);
	if (error) {
		log(DEBUG, "sched_main_cleanup_snap_locks: "
		    "failed to obtain lock, skipping");
		goto out;
	}

	sched_snapshot_lock_cleanup();

out:
	if (fd >= 0)
		close(fd);

	if (error)
		isi_error_free(error);
}

static void
sched_main_rotate_reports(struct sched_ctx *sctx)
{
	pid_t rr_pid;
	char lock_filename[MAXPATHLEN + 1];
	int fd = -1;
	struct siq_gc_conf *global_config = NULL;
	struct isi_error *error = NULL;
	
	log(TRACE, "sched_main_rotate_report: enter");
	
	sprintf(lock_filename, "%s/.rotate_report.lock", sched_get_lock_dir());
	fd = acquire_lock(lock_filename, 
	    O_EXLOCK | O_NONBLOCK, &error);
	if (error) {
		log(DEBUG, "sched_main_rotate_report: "
		    "failed to obtain lock, skipping");
		log(TRACE, "sched_main_rotate_report: %s", 
		    isi_error_get_message(error));
		isi_error_free(error);
		return;
	}
	ASSERT(fd >= 0);

	rr_pid = fork();
	if (rr_pid == -1) {
		//Failed
		log(ERROR, "sched_main_rotate_report: "
		    "failed to fork: %d", errno);
	} else if (rr_pid != 0)	{
		//Parent
		log(DEBUG, "sched_main_rotate_report: forked child %d",
		    rr_pid);
	} else {
		//Child
		sctx->rm_pidfile_on_exit = false;
		log(TRACE, "sched_main_rotate_report: "
		    "starting report rotation");
		setproctitle("isi_migr_sched_rotate_report");
		ASSERT(ilog_running_app_choose("siq_sched_rotate_reports")
		    != 1);
		global_config = siq_gc_conf_load(&error);
		if (error) {
			log(ERROR, "sched_main_rotate_reports: "
			    "error saving global config: %s",
			    isi_error_get_message(error));
			isi_error_free(error);
		} else {
			global_config->root->scheduler->last_rotate_reports_run 
			    = time(NULL);
			siq_gc_conf_save(global_config, &error);
			siq_gc_conf_free(global_config);
		}
		rotate_report_periodic_cleanup(false);
		log(TRACE, "sched_main_rotate_report: "
		    "completed report rotation");
		exit(SUCCESS);
	}
	close(fd); //child owns this fd now
	fd = -1;
}

static void
process_rpo_alerts(struct siq_policy *pol,
    struct siq_source_record *srec, bool rpo_enabled, time_t now)
{
	bool			alert_exists, create_alert, pol_del_enabled;
	char			time_str[35];
	int			err;
	time_t			last_successful_start, time_since_sync;
	struct siq_source_record *source_rec = NULL;
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error	*error = NULL;

	UFAIL_POINT_CODE(rpo_roll_ahead,
		now += RETURN_VALUE;
		log(INFO, "%s: rpo rolled ahead by %d seconds to %ld",
		    __func__, RETURN_VALUE, now););

	active_alerts = siq_gc_active_alert_load(pol->common->pid, &error);
	if (error != NULL) {
		log(ERROR,
		    "%s: Failed to load active alerts for policy %s "
		    "(%s): %s", __func__, pol->common->name, pol->common->pid,
		    isi_error_get_message(error));
		goto out;
	}

	/* Only create an alert if there is not a preexisting alert.
	 * If an alert exists, we do not need to reload the srec since
	 * we will not be creating a new alert which requires the updated lkg.
	 * If source_rec is NULL we must load it. */
	alert_exists = siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_RPO_EXCEEDED);
	if (alert_exists && srec != NULL) {
		/* Just use the given srec. */
		source_rec = srec;
	} else {
		source_rec = siq_source_record_load(pol->common->pid, &error);
		if (error != NULL) {
			/* If we fail to load the source record, then
			 * do not process rpo alerts since we cannot
			 * get a reliable lkg. */
			err = isi_system_error_get_error(
			    (const struct isi_system_error *) error);

			/* If ENOENT, the policy has been deleted since our
			 * last policy list reload. No big deal. */
			if (err != ENOENT) {
				log(ERROR, "%s: Failed to load source_record "
				    "for policy %s (%s): %s", __func__,
				    pol->common->name, pol->common->pid,
				    isi_error_get_message(error));
			}
			isi_error_free(error);
			goto out;
		}
	}

	siq_source_get_last_known_good_time(source_rec,
	    &last_successful_start);
	if (last_successful_start > 0)
		/* Non-initial sync. */
		time_since_sync = now - last_successful_start;
	else if (pol->common->creation_time > 0)
		/* Initial sync. */
		time_since_sync = now - pol->common->creation_time;
	else
		/* Initial sync with no creation time. */
		time_since_sync = 0;

	/* Only create rpo alerts if a policy is enabled. */
	pol_del_enabled = (pol->common->state == SIQ_ST_ON) &&
	    !siq_source_is_policy_deleted(source_rec);

	create_alert = (rpo_enabled && pol_del_enabled &&
	    pol->scheduler->rpo_alert > 0 &&
	    time_since_sync >= pol->scheduler->rpo_alert);

	if (create_alert && !alert_exists) {
		strncpy(time_str, "Last success: ", sizeof(time_str) - 1);
		strncat(time_str, last_successful_start > 0 ?
		    asctime(localtime(&last_successful_start)) : "-",
		    sizeof(time_str) - strlen(time_str) - 1);

		if (siq_create_rpo_alert(pol->common->name, pol->common->pid,
		    pol->target->dst_cluster_name, last_successful_start,
		    NULL, active_alerts, time_str)) {
			log(NOTICE, "RPO exceeded for policy %s: %s. "
			    "Last success %ld seconds ago.",
			    pol->common->name, pol->common->pid,
			    time_since_sync);
		}
	} else if (!create_alert && alert_exists) {
		/* Cancel any active rpo alert for pol. */
		if (siq_cancel_alert_internal(SIQ_ALERT_RPO_EXCEEDED,
		    pol->common->name, pol->common->pid, active_alerts)) {
			log(NOTICE,
			    "Ending RPO alert for policy %s: %s",
			    pol->common->name, pol->common->pid);
		}
	}

out:
	siq_gc_active_alert_free(active_alerts);
	if (!alert_exists || srec == NULL)
		free(source_rec);
}

int
sched_main_node_work(struct sched_ctx *sctx)
{
	struct siq_policy	*cur_pol = NULL;
	int			ret = -1, err = -1;
	time_t			now;
	time_t			end_time;
	bool			locked = false, run = false;
	bool			rpo_enabled = true;
	bool			skip_sync = false;
	struct siq_gc_conf	*global_config = NULL;
	struct isi_error 	*error = NULL;
	struct siq_source_record 	*srec = NULL;
	enum siq_job_action	pending_job;
	ifs_snapid_t		man_sid = 0;
	bool			exists = false;

	ilog(IL_INFO, "%s called", __func__);

	now = time(NULL);

	/*
	 * Every day we scan once to look for report logs to be removed.
	 */
	global_config = siq_gc_conf_load(&error);
	if (error) {
		log(ERROR, "sched_main_node_work: "
		    "Failed to load global config: %s", 
		    isi_error_get_message(error));
		goto done;
	}
	siq_gc_conf_close(global_config);
	if (!global_config->root->scheduler->disable_rotate_reports &&
	    (now - global_config->root->scheduler->last_rotate_reports_run) 
	    > global_config->root->scheduler->rotate_reports_interval) {
		sched_main_rotate_reports(sctx);
	}

	switch (global_config->root->scheduler->rpo_alerts) {
	case RPO_ST_ON:
		break;
	case RPO_ST_OFF:
		rpo_enabled = false;
		break;
	default:
		log(ERROR, "Invalid rpo_alerts state");
		rpo_enabled = false;
	}

	siq_gc_conf_free(global_config);

	/* licensing test */
	if (!sctx->siq_license_enabled) {
		ret = 0;
		goto done;
	}

	/* state test */
	if (sched_check_state(sctx) != 0) {
		log(ERROR, "Failed sched_check_state");
		goto done;
	}

	switch (sctx->siq_state) {
	case SIQ_ST_ON:
		break;

	case SIQ_ST_OFF:
		log(ERROR, "Scheduler exiting");
		exit(1);

	case SIQ_ST_PAUSED:
		ret = 0;
		goto done;

	default:
		ASSERT(0);
	}

	now = time(NULL);
	for (cur_pol = SLIST_FIRST(&sctx->policy_list->root->policy);
	    cur_pol != NULL; cur_pol = SLIST_NEXT(cur_pol, next)) {
		pending_job = SIQ_JOB_NONE;
		if (locked) {
			siq_job_unlock();
			locked = false;
		}

		locked = siq_job_try_lock(cur_pol->common->pid, &error);
		if (error) {
			log(ERROR, "%s: siq_job_lock error %s", __func__,
			    isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		if (!locked) {
			/* Job is already locked. */
			log(TRACE, "Job for policy %s is already locked: "
			    "skipping", cur_pol->common->pid);
			continue;
		}

		/* if a run dir exists for the job, skip it. job lock prevents
		 * another scheduler from creating one while we check */
		if (siq_job_exists(cur_pol->common->pid)) {
			log(DEBUG, "Run dir already exists for policy %s",
			    cur_pol->common->pid);

			/* Check to see if the current policy has exceeded
			 * its rpo */
			process_rpo_alerts(cur_pol, NULL, rpo_enabled, now);
			continue;
		}

		siq_source_record_free(srec);
		srec = NULL;

		/* Grab the policy's source record */
		srec = siq_source_record_load(cur_pol->common->pid, &error);
		if (error) {
			err = isi_system_error_get_error(
			    (const struct isi_system_error *)error);

			/* if ENOENT, the policy has been deleted since our
			 * last policy list reload. No big deal. */
			if (err == ENOENT) {
				isi_error_free(error);
				error = NULL;
				continue;
			} else {
				log(ERROR, "%s", isi_error_get_message(error));
				goto done;
			}
		}

		/* Check to see if the current policy has exceeded its rpo */
		process_rpo_alerts(cur_pol, srec, rpo_enabled, now);

		/* See if this is a manually started run, or if the job is
		 * due to run per the policy schedule.
		 */
		run = policy_should_run(sctx, cur_pol, srec, now,
		    &end_time, &pending_job, &skip_sync, &error);
		if (error) {
			log(ERROR, "%s", isi_error_get_message(error));
			if (isi_error_is_a(error, ISI_SIQ_ERROR_CLASS) &&
			    (isi_siq_error_get_siqerr(
			    (struct isi_siq_error *) error) ==
			    E_SIQ_BACKOFF_RETRY)) {
				/* Continue processing the remaining policies.
				 * Another scheduler will try this policy
				 * again later.
				 */
				isi_error_free(error);
				error = NULL;
				continue;
			}

			goto done;
		}
		if (!run) {
			if (skip_sync) {
				generate_skip_when_source_unmodified_report(
				    sctx, cur_pol, srec, pending_job, now);
				skip_sync = false;
			}
			continue;
		}

		/* Lock manual snapshots to guarantee their existence until
		 * SIQ no longer needs them */
		siq_source_get_manual(srec, &man_sid);
		if (man_sid != INVALID_SNAPID) {
			exists = sched_lock_manual_snapshot(man_sid, &error);
			if (error) {
				log(ERROR, "Cannot start policy: %s using "
					"snapshot id %lu: %s",
					cur_pol->common->pid, man_sid,
					isi_error_get_message(error));

				/* Avoid error loop, stop trying to use a
				 * manual snapshot that doesn't exist. */
				if (!exists) {
					siq_source_set_manual_snap(srec, 0);
					siq_source_record_save(srec, &error);
				}

				isi_error_free(error);
				error = NULL;
				continue;
			}
		}

		start_job(sctx, cur_pol, pending_job, now, end_time);
	}

	/* Cleanup manual snapshot locks */
	sched_main_cleanup_snap_locks();

	/* Remove stale change-based and snapshot-triggered contexts */
	reap_stale_pol_ctxs(sctx, now);
	reap_stale_snap_ctxs(sctx);

	ret = 0;

done:
	if (locked)
		siq_job_unlock();
	if (error)
		isi_error_free(error);
	siq_source_record_free(srec);

	return ret;
}

static enum siq_action_type 
pending_job_to_action_type(enum siq_action_type default_action, 
    enum siq_job_action pending_job)
{
	switch (pending_job) {
	case SIQ_JOB_FAILBACK_PREP:
		return SIQ_ACT_FAILBACK_PREP;

	case SIQ_JOB_FAILBACK_PREP_DOMAIN_MARK:
		return SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK;

	case SIQ_JOB_FAILBACK_PREP_RESTORE:
		return SIQ_ACT_FAILBACK_PREP_RESTORE;

	case SIQ_JOB_FAILBACK_PREP_FINALIZE:
		return SIQ_ACT_FAILBACK_PREP_FINALIZE;

	default:
		return default_action;
	}
}

static int
start_job(struct sched_ctx *sctx, struct siq_policy *pol,
    enum siq_job_action pending_job, time_t curr_time, time_t end_time)
{
	char			edit_buffer[MAXPATHLEN + 1];
	char			work_buffer[MAXPATHLEN + 1];
	char			report_filename[MAXPATHLEN + 1];
	char			spec_filename[MAXPATHLEN + 1];
	char			*pol_id = NULL;
	struct siq_job_summary	*job_summary = NULL;
	struct siq_gc_job_summary *gc_job_summ = NULL;
	enum siq_action_type act = SIQ_ACT_NOP;
	struct siq_policy	*pol_temp = NULL;
	struct isi_error	*error = NULL;
	int res = -1;

	ilog(IL_INFO, "%s called", __func__);

	pol_id = pol->common->pid;

	// Create a copy of the policy so we can propagate a failback action to
	// the job spec. 
	act = pending_job_to_action_type(pol->common->action,
	    pending_job);

	pol_temp = siq_policy_copy(pol);
	pol_temp->common->action = act;
	if (act == SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK || 
	    act == SIQ_ACT_FAILBACK_PREP_RESTORE) {
		FREE_AND_NULL(pol_temp->target->dst_cluster_name);
		pol_temp->target->dst_cluster_name = strdup("localhost");

		/**
		 * Bug 103788
		 * disable target_restrict during local domain mark or local
		 * snap revert
		 */
		pol_temp->target->target_restrict = 0;

		FREE_AND_NULL(pol_temp->target->path);
		pol_temp->target->path = 
		    strdup(pol->datapath->root_path);
	}

	log(NOTICE, "Starting policy '%s' (%s), trigger: %s",
	    pol->common->name, pol_id,
	    pending_job == SIQ_JOB_NONE ? "schedule" : "on demand");

	sprintf(edit_buffer, "%s/%s", sctx->edit_run_dir, pol_id);
	sprintf(report_filename, "%s/%s/%s", sctx->edit_run_dir,
	    pol_id, REPORT_FILENAME);
	sprintf(spec_filename, "%s/%s/%s", sctx->edit_run_dir,
	    pol_id, SPEC_FILENAME);

	if (sched_make_directory(edit_buffer) != 0) {
		log(ERROR, "Failed to sched_make_directory (%s)", edit_buffer);
		goto out;
	}

	/* <edit_run_dir>/<policy_id>/ job_summary file create */
	job_summary = siq_job_summary_create();
	job_summary->action = act;
	job_summary->assess = (pending_job == SIQ_JOB_ASSESS) ? true : false;
	job_summary->sync_type = ST_INVALID;
	job_summary->total->state = SIQ_JS_SCHEDULED;
	job_summary->total->start = curr_time;
	job_summary->retry = false;
	job_summary->dead_node = false;
	job_summary->total->end = end_time;
	/* when create the job summary, find the sra job guid
	 * if sra_id is NULL it means that it is a scheduled 
	 * job so it is NOT an error
  	 * assign sra_id only for on-demand sync job (tracked with Bug 104704)
	 */
	if (pending_job == SIQ_JOB_RUN)
		job_summary->sra_id = find_sra_job_entry_by_pid(pol_id);

	gc_job_summ = siq_job_summary_save_new(job_summary, report_filename,
	    &error);
	siq_gc_job_summary_free(gc_job_summ);
	if (error) {
		log(ERROR, "Failed to siq_job_summary_save_new to %s: %s",
		    report_filename, isi_error_get_message(error));
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}

	/* <edit_run_dir>/<policy_id>/ spec file create */
	siq_spec_save(pol_temp, spec_filename, &error);
	if (error) {
		log(ERROR, "Failed to save spec to %s: %s", spec_filename,
		    isi_error_get_message(error));
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}

	/* move <edit_run_dir>/<policy_id> to <sched_run_dir>/<policy_id> */
	sprintf(work_buffer, "%s/%s", sched_get_run_dir(),
	    pol_id);
	if (siq_dir_move(edit_buffer, work_buffer) != 0) {
		log(ERROR, "Failed to siq_dir_move");
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}
	res = 0;

out:
	if (res != 0) {
		log(ERROR, "Failed to start policy %s",
		    pol->common->name);
	}
	siq_policy_free(pol_temp);
	isi_error_free(error);
	return res;
}

static int
generate_skip_when_source_unmodified_report(struct sched_ctx *sctx,
    struct siq_policy *pol, struct siq_source_record *srec,
    enum siq_job_action pending_job, time_t curr_time)
{
	struct siq_job_summary *job_summary = NULL;
	struct siq_gc_job_summary *gc_job_summ = NULL;
	bool stf_sync = false;
	bool stf_upgrade_sync = false;
	bool expected_dataloss = false;
	char *pol_id = NULL;
	char edit_buffer[MAXPATHLEN + 1];
	char report_filename[MAXPATHLEN + 1];
	ifs_snapid_t latest_snapid = INVALID_SNAPID;
	int jid = 0;
	int res = -1;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(sctx != NULL);
	ASSERT(pol != NULL);
	ASSERT(srec != NULL);
	ASSERT(pol->common->action == SIQ_ACT_REPLICATE ||
	    pol->common->action == SIQ_ACT_SYNC);

	pol_id = pol->common->pid;

	log(NOTICE, "Skipping policy '%s' (%s), trigger: no changes detected",
	    pol->common->name, pol_id);

	snprintf(edit_buffer, sizeof(edit_buffer), "%s/%s",
	    sctx->edit_run_dir, pol_id);
	snprintf(report_filename, sizeof(report_filename), "%s/%s/%s",
	    sctx->edit_run_dir, pol_id, REPORT_FILENAME);

	job_summary = siq_job_summary_create();
	job_summary->action = pol->common->action;
	job_summary->total->state = SIQ_JS_SKIPPED;
	job_summary->total->start = curr_time;
	job_summary->total->end = curr_time;

	/*
	 * Handle job_spec.
	 */
	if (pol != NULL) {
		siq_report_spec_free(job_summary->job_spec);
		job_summary->job_spec = siq_spec_copy_from_policy(pol);
	}

	/*
	 * Handle sync_type.
	 */
	siq_source_get_latest(srec, &latest_snapid);
	ASSERT(latest_snapid != INVALID_SNAPID);

	if (!pol->datapath->disable_stf)
		stf_sync = true;

	if (pol->coordinator->expected_dataloss) {
		expected_dataloss = true;
		stf_sync = false;
	}

	if (stf_sync && !siq_source_pol_stf_ready(srec))
		stf_upgrade_sync = true;

	if (stf_sync && !stf_upgrade_sync)
		job_summary->sync_type = ST_INCREMENTAL;
	else if (stf_upgrade_sync)
		job_summary->sync_type = ST_UPGRADE;
	else if (!stf_sync && !stf_upgrade_sync && expected_dataloss)
		job_summary->sync_type = ST_INCREMENTAL;
	else
		job_summary->sync_type = ST_LEGACY;

	/*
	 * Make the sched edit_run_dir.
	 */
	if (sched_make_directory(edit_buffer) != 0) {
		log(ERROR, "Failed to sched_make_directory (%s)",
		    edit_buffer);
		goto out;
	}

	gc_job_summ = siq_job_summary_save_new(job_summary, report_filename,
	    &error);
	if (error != NULL) {
		log(ERROR, "Failed to siq_job_summary_save_new to %s: %s",
		    report_filename, isi_error_get_message(error));
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}

	/*
	 * Handle job_id.
	 *
	 * Note: In the event of a failure after this point, the job id
	 * count will still be incremented in the db. In this unlikely
	 * occurrence, users might see a skipped job id in reports.
	 * This is the same behavior as any other sync.
	 */
	jid = siq_reports_status_check_and_set_inc_job_id(pol_id, &error);
	if (error != NULL) {
		log(ERROR, "Failed to get job_id for %s: %s",
		    pol_id, isi_error_get_message(error));
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}

	gc_job_summ->root->summary->job_id = jid;

	/*
	 * Write skip-when-source-unmodified report.
	 */
	siq_job_cleanup(gc_job_summ, pol_id, edit_buffer, &error);
	if (error != NULL) {
		log(ERROR, "Failed to siq_job_cleanup policy %s: %s",
		    pol->common->name, isi_error_get_message(error));
		trash_job_dir(edit_buffer, pol_id);
		goto out;
	}

	res = 0;

out:
	if (res != 0)
		log(ERROR, "Failed to generate skipped report for policy %s",
		    pol->common->name);
	siq_gc_job_summary_free(gc_job_summ);
	isi_error_free(error);
	return res;
}
