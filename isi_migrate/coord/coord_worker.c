#include <isi_migrate/config/siq_alert.h>
#include <isi_migrate/migr/treewalk.h>
#include <isi_migrate/migr/work_item.h>
#include <isi_migrate/migr/selectors.h>
#include <isi_migrate/config/siq_target.h>
#include <isi_ufp/isi_ufp.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_error.h>
#include <isi_util/util_adt.h>
#include "isi_cpool_cbm/isi_cbm_error.h"
#include "isi_cpool_cbm/isi_cbm_file_versions.h"

#include "coord.h"

#define MIN_SPLIT_INTERVAL_SEC 10
#define REDIST_INTERVAL_SEC 10

#define WORK_LOCK_TIMEOUT 30

#define SPLITTABLE_QUEUE "splittable_queue"
#define IDLE_QUEUE "idle_queue"
#define SPLIT_PENDING_QUEUE "split_pending_queue"
#define OFFLINE_QUEUE "offline_queue"
#define DYING_QUEUE "dying_queue"

static struct timeval redist_timeout = {REDIST_INTERVAL_SEC, 0};
static void *redist_tout = NULL;
static void *workers_tout = NULL;
static void kill_worker(struct worker *w);
static void finish_job(struct coord_ctx *ctx);


/* __        __         _                ___                             
 * \ \      / /__  _ __| | _____ _ __   / _ \ _   _  ___ _   _  ___  ___ 
 *  \ \ /\ / / _ \| '__| |/ / _ \ '__| | | | | | | |/ _ \ | | |/ _ \/ __|
 *   \ V  V / (_) | |  |   <  __/ |    | |_| | |_| |  __/ |_| |  __/\__ \
 *    \_/\_/ \___/|_|  |_|\_\___|_|     \__\_\\__,_|\___|\__,_|\___||___/
 */

/* splittable_queue contains all workers that are believed to be splittable */
struct ptr_vec splittable_queue;

/* idle_queue contains all workers that are waiting for a work item. newly
 * connected workers must send a work request before they are promoted to the 
 * idle queue */
struct ptr_vec idle_queue;

/* split_pending_queue contains all workers that have a split request in 
 * flight */
struct ptr_vec split_pending_queue;

/* offline_queue initially contains all the workers. workers are moved off the
 * queue when we receive a work request. workers that disconnect unexpectedly
 * are moved back to this queue */
struct ptr_vec offline_queue;

/* dying_queue contains workers in the dying state and will imminently be 
 * cleaned up */
struct ptr_vec dying_queue;


/*  ____           _             _      ___                        
 * |  _ \ ___  ___| |_ __ _ _ __| |_   / _ \ _   _  ___ _   _  ___ 
 * | |_) / _ \/ __| __/ _` | '__| __| | | | | | | |/ _ \ | | |/ _ \
 * |  _ <  __/\__ \ || (_| | |  | |_  | |_| | |_| |  __/ |_| |  __/
 * |_| \_\___||___/\__\__,_|_|   \__|  \__\_\\__,_|\___|\__,_|\___|
 *
 * restarts_queue contains a list of work items which need to be reassigned to
 * pworkers.  This may happen when a pworker dies unexpectedly, or when reading
 * an exception file for processing.
 */

static struct work_item_queue *restarts_queue;

static int	error_callback(struct generic_msg *, void *);
static void	reset_worker(struct worker *);
static void	queue_enq(struct ptr_vec *q, struct worker *w);
static void	queue_deq(struct ptr_vec *q, struct worker **out);
static void	queue_peek(struct ptr_vec *q, struct worker **out);
static void	queue_rem(struct ptr_vec *q, struct worker *w);
static void	queue_rem_all(struct worker *w);
static bool	queue_contains(struct ptr_vec *q, struct worker *w);
static void	configure_worker(struct worker *w);

/*   ___                             _    ____ ___ 
 *  / _ \ _   _  ___ _   _  ___     / \  |  _ \_ _|
 * | | | | | | |/ _ \ | | |/ _ \   / _ \ | |_) | | 
 * | |_| | |_| |  __/ |_| |  __/  / ___ \|  __/| | 
 *  \__\_\\__,_|\___|\__,_|\___| /_/   \_\_|  |___|
 */

static const char *
queue_name(struct ptr_vec *q)
{
	if (q == &splittable_queue)
		return SPLITTABLE_QUEUE;
	else if (q == &idle_queue)
		return IDLE_QUEUE;
	else if (q == &offline_queue)
		return OFFLINE_QUEUE;
	else if (q == &split_pending_queue)
		return SPLIT_PENDING_QUEUE;
	else if (q == &dying_queue)
		return DYING_QUEUE;

	ASSERT(0, "unknown queue");
	return NULL;
}

static void
queue_enq(struct ptr_vec *q, struct worker *w)
{
	ASSERT(q != NULL && w != NULL);

	/* worker must be removed from its previous queue before being
	 * inserted in a different one. this sanity check will prevent workers
	 * from existing in multiple queues (i.e. bug 85287) */
	ASSERT(queue_contains(&splittable_queue, w) == false);
	ASSERT(queue_contains(&split_pending_queue, w) == false);
	ASSERT(queue_contains(&offline_queue, w) == false);
	ASSERT(queue_contains(&idle_queue, w) == false);
	ASSERT(queue_contains(&dying_queue, w) == false);

	if (q == &idle_queue)
		ASSERT(w->work == NULL);
	
	pvec_insert(q, 0, (void *)w);

	log(TRACE, "queue_enq worker %d (%p) onto %s", w->id, w, queue_name(q));
}

static void
queue_deq(struct ptr_vec *q, struct worker **out)
{
	ASSERT(q != NULL && out != NULL);
	*out = NULL;

	if (pvec_pop(q, (void **)out))
		log(TRACE, "queue_deq worker %d (%p) from %s", (*out)->id,
		    (*out), queue_name(q));
	else
		log(TRACE, "queue_deq failed because %s is empty",
		    queue_name(q));
}

static void
queue_peek(struct ptr_vec *q, struct worker **out)
{
	ASSERT(q != NULL && out != NULL);
	*out = NULL;

	if (pvec_pop(q, (void **)out)) {
		log(TRACE, "queue_peek at worker %d from %s", (*out)->id,
		    queue_name(q));
		pvec_push(q, (void *)*out);
	} else
		log(TRACE, "queue_peek failed because %s is empty",
		    queue_name(q));
}

static void
queue_rem(struct ptr_vec *q, struct worker *w)
{
	int index;

	ASSERT(q != NULL && w != NULL);

	if (pvec_find(q, w, &index)) {
		pvec_remove(q, index);
		log(TRACE, "queue_rem worker %d (%p) from %s", w->id,
		    w, queue_name(q));
	} else
		log(TRACE, "queue_rem did not find worker %d in %s", w->id,
		    queue_name(q));
}

static void
queue_rem_all(struct worker *w)
{
	ASSERT(w != NULL);

	queue_rem(&splittable_queue, w);
	queue_rem(&split_pending_queue, w);
	queue_rem(&idle_queue, w);
	queue_rem(&offline_queue, w);
	queue_rem(&dying_queue, w);
}

int
queue_size(struct ptr_vec *q)
{
	int size;

	ASSERT(q != NULL);

	size = pvec_size(q);
	log(TRACE, "queue_size of %s is %d", queue_name(q), size);

	return size;
}

static bool
queue_contains(struct ptr_vec *q, struct worker *w)
{
	int index;

	if (pvec_find(q, w, &index)) {
		log(TRACE, "%s contains worker %d", queue_name(q), w->id);
		return true;
	}

	log(TRACE, "%s does not contain worker %d", queue_name(q), w->id);
	return false;
}


/* __        __         _      ___ _                     
 * \ \      / /__  _ __| | __ |_ _| |_ ___ _ __ ___  ___ 
 *  \ \ /\ / / _ \| '__| |/ /  | || __/ _ \ '_ ` _ \/ __|
 *   \ V  V / (_) | |  |   <   | || ||  __/ | | | | \__ \
 *    \_/\_/ \___/|_|  |_|\_\ |___|\__\___|_| |_| |_|___/
 */

static void
work_item_insert(struct work_item_queue *queue, struct work_item *work)
{
	struct work_item_node *new_node =
	    calloc(1, sizeof(struct work_item_node));
	ASSERT(new_node);

	new_node->work = work;
	TAILQ_INSERT_TAIL(queue, new_node, ENTRIES);
}

static struct work_item *
work_item_new(ifs_lin_t wi_lin, bool count_retry, const char *fmt, ...)
{
	struct work_item *ret;
	va_list args;

	ret = calloc(1, sizeof(struct work_item));

	ret->wi_lin = wi_lin;
	/**
	 * Bug 179841
	 * If there are no idle workers, a newly created split work item gets
	 * placed on the restart queue. In that case we'll count a restart.
	 * Initialize count_retry to false to avoid that case.
	 */
	ret->count_retry = count_retry;

	va_start(args, fmt);
	fmt_vprint(&ret->history, fmt, args);
	va_end(args);

	return ret;
}

static void
work_item_clean(struct work_item *item)
{
	fmt_clean(&item->history);
}

void
record_phase_start(enum stf_job_phase phase)
{
	time_t now;
	struct siq_phase_history *new_phase = NULL;
	
	CALLOC_AND_ASSERT(new_phase, struct siq_phase_history);
	now = time(0);
	new_phase->phase = phase;
	new_phase->start = now;
	
	if (!SLIST_EMPTY(&JOB_SUMM->phases)) {
		SLIST_FIRST(&JOB_SUMM->phases)->end = now;
	}
	SLIST_INSERT_HEAD(&JOB_SUMM->phases, new_phase, next);
	
	/* Update total number of phases in report, in case it changed */
	JOB_SUMM->total_phases = get_total_enabled_phases(&g_ctx.job_phase);
}

static void
record_last_phase_end(void)
{
	time_t now;
	
	now = time(0);
	
	if (!SLIST_EMPTY(&JOB_SUMM->phases)) {
		SLIST_FIRST(&JOB_SUMM->phases)->end = now;
	}
	
	/* Update total number of phases in report, in case it changed */
	JOB_SUMM->total_phases = get_total_enabled_phases(&g_ctx.job_phase);
}

static void
send_comp_phase_finish(struct coord_ctx *ctx, enum ript_messages msg_type)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;

	log(TRACE, "%s: enter", __func__);

	resp.head.type = build_ript_msg_type(msg_type);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	msg_send(ctx->tctx.sock, &resp);

	log(TRACE, "%s: exit", __func__);
}

static void
do_comp_allow_write_finish(struct coord_ctx *ctx, struct isi_error **error_out)
{
	struct target_record *trec = NULL;
	ifs_snapid_t saved_snap_id = INVALID_SNAPID;
	int lock_fd = -1;
	SNAP *snap = NULL;
	struct isi_str new_snap_name;
	struct fmt FMT_INIT_CLEAN(fmt_new);
	struct isi_str *cur_tgt_snap_name = NULL;
	struct isi_error *error = NULL;

	//Cleanup comp commit
	send_comp_phase_finish(ctx, COMP_COMMIT_END_MSG);

	//Do some cleanup on the target
	target_cleanup_complete();
	ctx->tgt_tmp_removed = true;

	//Clear current snapshots and take a new one
	saved_snap_id = ctx->curr_snap.id;
	log(DEBUG, "%s: latest: %lld new: %lld", __func__, ctx->prev_snap.id,
	    saved_snap_id);
	remove_snapshot_locks();
	do_target_snapshot(NULL);

	//Open target record and read latest snapid
	lock_fd = siq_target_acquire_lock(ctx->rc.job_id, O_SHLOCK, &error);
	if (error)
		goto out;
	trec = siq_target_record_read(ctx->rc.job_id, &error);
	if (!trec || error)
		goto out;

	//Write new source records
	log(DEBUG, "%s: updating to latest: %lld new: %d", __func__,
	    saved_snap_id, trec->latest_snap);
	siq_source_set_restore_latest(ctx->record, saved_snap_id); // = new
	siq_source_clear_restore_new(ctx->record);
	siq_source_add_restore_new_snap(ctx->record, trec->latest_snap);
	    // = above
	siq_source_record_save(ctx->record, &error);
	if (error) {
		goto out;
	}

	//Rename new failover snap to be the latest failover snap
	siq_failover_snap_rename_new_to_latest(ctx->rc.job_id, ctx->rc.job_name,
	    &error);
	if (error) {
		goto out;
	}
	log(TRACE, "%s: renamed restore-new snap to restore-latest", __func__);

	//Rename the target snap we just took to be the new snap
	snap = snapshot_open_by_sid(trec->latest_snap, &error);
	if (error) {
		goto out;
	}
	cur_tgt_snap_name = snapshot_get_name(snap);
	snapshot_close(snap);
	fmt_print(&fmt_new, "SIQ-%s-restore-new", ctx->rc.job_id);
	isi_str_init(&new_snap_name, __DECONST(char *,
	    fmt_string(&fmt_new)), fmt_length(&fmt_new) + 1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	snapshot_rename(cur_tgt_snap_name, &new_snap_name, &error);
	if (error) {
		goto out;
	}
	log(TRACE, "%s: renamed target snap to restore-new", __func__);

	//Reload snapshots
	check_and_create_snap();
	log(TRACE, "%s: snapshots reloaded", __func__);

out:
	if (lock_fd != -1) {
		close(lock_fd);
		lock_fd = -1;
	}
	siq_target_record_free(trec);

	if (cur_tgt_snap_name) {
		isi_str_free(cur_tgt_snap_name);
	}

	isi_error_handle(error, error_out);
	log(TRACE, "%s: exit", __func__);
}

void
checkpoint_next_phase(struct coord_ctx *ctx,
    bool update, char *tw_data, size_t tw_size,
    bool job_init, struct isi_error **error_out)
{
	int i;
	struct worker *w = NULL;
	enum work_restart_type rt;
	struct work_restart_state work;
	struct isi_error *error = NULL;
	char *buf = NULL, *old_buf = NULL;
	size_t jsize = 0, old_jsize = 0 ;
	bool success = false;

	/* If update of state transition */
	if (update) {
		//Before we change phases, do end of phase cleanup

		//End of STF_PHASE_COMP_COMMIT, send end message
		if ((ctx->curr_ver >= FLAG_VER_3_5)) {
			if (ctx->job_phase.cur_phase ==
			    STF_PHASE_COMP_COMMIT || 
			    ctx->job_phase.cur_phase ==
			    STF_PHASE_COMP_FINISH_COMMIT) {
				//Send message coord->tmonitor to unset target
				//record flag and delete lin_list
				log(DEBUG, "%s: Finishing compliance commit "
				    "phase", __func__);
				send_comp_phase_finish(ctx,
				    COMP_COMMIT_END_MSG);
			}
		}

		old_buf = fill_job_state(&ctx->job_phase, &old_jsize);
		next_job_phase(&ctx->job_phase);
	}

	/*
	 * We take a snapshot at the target just before flip phase.
	 * This snapshot will be used for failover.
	 * The failover is only supported for sync policies.
	 */
	if ((ctx->curr_ver >= FLAG_VER_3_5) &&
	    (ctx->job_phase.cur_phase == STF_PHASE_FLIP_LINS) &&
	    !(ctx->rc.flags & FLAG_FAILBACK_PREP) ) {
		target_cleanup_complete();
		ctx->tgt_tmp_removed = true;
		log(NOTICE, "Taking target snapshot before job "
		    "phase %s ", job_phase_str(&ctx->job_phase));
		do_target_snapshot(NULL);
	}

	if ((ctx->curr_ver >= FLAG_VER_3_5) &&
	    (ctx->job_phase.cur_phase == STF_PHASE_DATABASE_RESYNC)) {
		log(NOTICE, "Cleanup mirrored database before job "
		    "phase %s ", job_phase_str(&ctx->job_phase));
		mirrored_repstate_cleanup();
	}

	UFAIL_POINT_CODE(phase_start,
	    if (RETURN_VALUE == ctx->job_phase.cur_phase) {
	    	error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, 
		    "Failpoint triggered at phase %s.",
		    job_phase_str(&ctx->job_phase));
		goto out;
	    }
	);

	log(NOTICE, "Next job phase %s ", job_phase_str(&ctx->job_phase));
	record_phase_start(ctx->job_phase.cur_phase);
	rt = get_work_restart_type(&ctx->job_phase);
	allocate_entire_work(&work, rt);
	ctx->cur_work_lin = WORK_RES_RANGE_MIN;

	buf = fill_job_state(&ctx->job_phase, &jsize);

	success = update_job_phase(ctx->rep, STF_JOB_PHASE_LIN,
	    buf, jsize, old_buf, old_jsize, WORK_RES_RANGE_MIN, &work,
	    tw_data, tw_size, job_init, &error);

	if (error)
		goto out;

	UFAIL_POINT_CODE(phase_start_checkpoint,
	    if (RETURN_VALUE == ctx->job_phase.cur_phase) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Failpoint triggered at phase %s.",
		    job_phase_str(&ctx->job_phase));
		goto out;
	    }
	);

	/* We cannot have somebody else updating */
	ASSERT(success == true);

	/*
	 * Failpoint to kill all workers before starting new phase.
	 */
	UFAIL_POINT_CODE(post_checkpoint_phase_kill_all,
	    if (RETURN_VALUE == ctx->job_phase.cur_phase) {
		for (i = 0; i < ctx->workers->count; i++) {
			w = ctx->workers->workers[i];
			reset_worker(w);
		}
		register_timer_callback(TIMER_DELAY);
		goto out;
	    }
	);

	/*
	 * Failpoint to kill one worker before starting new phase.
	 */
	UFAIL_POINT_CODE(post_checkpoint_phase_kill_one,
	    if (RETURN_VALUE == ctx->job_phase.cur_phase) {
		for (i = 0; i < ctx->workers->count; i++) {
			w = ctx->workers->workers[i];
			if (w->reset)
				continue;
			reset_worker(w);
			register_timer_callback(TIMER_DELAY);
			break;
		}
		goto out;
	    }
	);

out:
	if (buf)
		free(buf);

	if (old_buf)
		free(old_buf);
	isi_error_handle(error, error_out);
}


void
path_stat(int ifs_fd, struct path *path, struct stat *st,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	int ret;

	log(TRACE, "path_stat");

	fd = fd_rel_to_ifs(ifs_fd, path);
	if (fd == -1) {
		error = isi_system_error_new(errno, "Failed to get fd for %s "
		    "under ifs fd %d (path_stat)", path->path, ifs_fd);
		goto out;
	}

	ret = fstat(fd, st);
	if (ret == -1) {
		error = isi_system_error_new(errno, "Failed to stat %s",
		    path->path);
		goto out;
	}

out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
}

static const char *
queue_search_all(struct worker *w)
{
	struct ptr_vec *queue = NULL;
	
	if (queue_contains(&splittable_queue, w)) {
		queue = &splittable_queue;
	} else if (queue_contains(&idle_queue, w)) {
		queue = &idle_queue;
	} else if (queue_contains(&split_pending_queue, w)) {
		queue = &split_pending_queue;
	} else if (queue_contains(&offline_queue, w)) {
		queue = &offline_queue;
	} else if (queue_contains(&dying_queue, w)) {
		queue = &dying_queue;
	} else {
		return "orphaned";
	}
	return queue_name(queue);
}

static void
dump_queue(FILE *f, struct ptr_vec *queue)
{
	void **ptr = NULL;
	struct worker *w;
	
	PVEC_FOREACH(ptr, queue) {
		w = *ptr;
		fprintf(f, "    %d (%p)\n", w->id, w);
	}
}

/* XXX TBD XXX */
int
dump_status(void *ctx)
{
	const char status_file[] = "/var/tmp/migrate_status.txt";
	struct worker *w;
	FILE *f;
	int i;

	f = fopen(status_file, "w");
	if (f == NULL) {
		log(ERROR,"Status generation failed: %s", strerror(errno));
		return 1;
	}

	fprintf(f, "SyncIQ job '%s' (%s) started at %.24s.\n", 
	    JOB_SPEC->common->name, g_ctx.rc.job_id, ctime(&g_ctx.start));

	fprintf(f, "\nCurrent worker status:\n");
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		fprintf(f, "[%d] %d: ", i, w->s_node->lnn);
		if (w->socket == -1) {
			fprintf(f, "%d: %s disconnected (%s)\n", i, 
			    w->s_node->host, queue_search_all(w));
		} else {
			fprintf(f, "%p %s -> %s: (last change %lds, "
			    "last split %lds, %s)\n",
			    w, w->s_node->host, w->t_node->host,
			    time(0) - w->last_work, time(0) - w->last_split,
			    queue_search_all(w));
			fprintf(f, "\n");
		}
	}
	
	fprintf(f, "Queue status:\n");
	fprintf(f, "  splittable_queue size %d\n", 
	    queue_size(&splittable_queue));
	dump_queue(f, &splittable_queue);
	fprintf(f, "  idle_queue size %d\n", queue_size(&idle_queue));
	dump_queue(f, &idle_queue);
	fprintf(f, "  split_pending_queue size %d\n", 
	    queue_size(&split_pending_queue));
	dump_queue(f, &split_pending_queue);
	fprintf(f, "  offline_queue size %d\n", queue_size(&offline_queue));
	dump_queue(f, &offline_queue);
	fprintf(f, "  dying_queue size %d\n", queue_size(&dying_queue));
	dump_queue(f, &dying_queue);
	fprintf(f, "\n");
	
	print_net_state(f);

	fclose(f);

	log(NOTICE, "Status information dumped to %s", status_file);
	return 0;
}

/* Makes an encoding vector of enc for the given path.
 * path must be absolute */
static void
assign_restart(struct worker *worker, struct work_item *work)
{
	struct generic_msg m;

	/* Check retries, bail if too many */
	if (g_ctx.rc.limit_work_item_retry && work->retries >= 
	    global_config->root->coordinator->max_work_item_retry) {
		bool exists = false;
		struct work_restart_state ws;
		exists = get_work_entry(work->wi_lin, &ws, NULL, NULL, g_ctx.rep, NULL);
		log(ERROR, "Work item %llx:%s has been restarted too many times.",
		    work->wi_lin, work_type_str(exists ? ws.rt:
			get_work_restart_type(&g_ctx.job_phase)));
		fatal_error(false, SIQ_ALERT_POLICY_FAIL,
		    "A work item has been restarted too many times.  "
		    "This is usually caused by a network failure or a "
		    "persistent worker crash.");
	} else {
		/**
		 * Bug 179841
		 * Don't count work item restarts when we intentionally killed
		 * the last assigned pworker.
		 */
		if (work->count_retry)
			work->retries++;
		else
			work->count_retry = true;
		ilog(IL_INFO, "%s called, Restarting work item %llx for worker %d (Retry "
		    "count: %d)", __func__, work->wi_lin, worker->id, work->retries);
	}

	m.head.type = WORK_RESP_MSG3;
	m.body.work_resp3.wi_lin = work->wi_lin;
	validate_work_lin(work->wi_lin);

	worker->work = work;
	msg_send(worker->socket, &m);
}

/*
 * Checks that for every ondisk work item we have
 * corresponding work lin in memory. Also makes sure that
 * none of the other work items have same work lin.
 */
static void
assert_not_overlapping(struct work_item *work)
{
	struct worker *w;
	int i;
	bool exists = false;
	struct isi_error *error = NULL;
	struct work_item_node *item, *tmp;

	exists = entry_exists(work->wi_lin, g_ctx.rep,
	    &error);
	if (error)
		fatal_error_from_isi_error(false, error);
	ASSERT(exists == true);
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		if (w->work == NULL)
			continue;

		/* Bug 140776
		 * If the assertion is going to fire, we need to log some
		 * additional information. The core that is produced optimizes
		 * out most local variables */
		if (w->work->wi_lin == work->wi_lin) {
			log(ERROR, "%s: Found overlapping work item. "
			    "w->work->wi_lin = %llu, work->wi_lin = %llu, "
			    "w->id = %d, i = %d", __func__, w->work->wi_lin,
			    work->wi_lin, w->id, i);
		}
		ASSERT(w->work->wi_lin != work->wi_lin);
	}

	/* Also make sure that work lin is not found in restarts queue */
	if (!TAILQ_EMPTY(restarts_queue)) {
		TAILQ_FOREACH_SAFE(item, restarts_queue, ENTRIES, tmp) {
			ASSERT(item->work->wi_lin != work->wi_lin);
		}
	}
}

static bool
job_phase_is_finished(void)
{
	bool finished = false;
	int i;
	ifs_lin_t wi_lin;
	struct work_restart_state *debug_work = NULL;
	char *debug_treewalk = NULL;
	size_t debug_size;
	bool debug_exists = false;
	struct isi_error *error =NULL;

	UFAIL_POINT_CODE(stall_on_dir_links,
	    if (g_ctx.job_phase.cur_phase == STF_PHASE_CT_DIR_LINKS)
	            goto out;
	);

	if (!g_ctx.started)
		goto out;

	if (queue_size(&dying_queue) > 0)
		goto out;

	if (!TAILQ_EMPTY(restarts_queue))
		goto out;

	for (i = 0; i < g_ctx.workers->count; i++) {
		if (g_ctx.workers->workers[i]->work)
			goto out;
	}

	/* Make sure there are no work items on disk */
	wi_lin = get_first_work_lin(g_ctx.rep, &error);
	if (error)
		fatal_error_from_isi_error(false, error);
	/* Bug 112843 - Debugging code follows */
	if (wi_lin != 0) {
		/* Read output into memory to figure out the cause */
		debug_work = calloc(1, sizeof(struct work_restart_state));
		debug_exists = get_work_entry(wi_lin, debug_work, &debug_treewalk, 
		    &debug_size, g_ctx.rep, &error);
		if (debug_exists) {
			log(ERROR, "Found orphaned work item: 0x%016llx (0x%llx - 0x%llx)", wi_lin, 
			    debug_work->min_lin, debug_work->max_lin);
		} else {
			log(ERROR, "Found orphaned work item but failed to open it");
		}
	}
	/* End Bug 112843 */
	ASSERT(wi_lin == 0);

	finished = true;

out:
	if (finished)
		log(DEBUG, "no work items or restarts, job is finished");

	return finished;
}

static void
start_new_phase(void)
{
	struct isi_error *error = NULL;
	struct generic_msg m = {};
	struct worker *w = NULL;

	/* pass the entire work item to the first worker and add it
	 * to the active queue */
	m.head.type = WORK_RESP_MSG3;
	m.body.work_resp3.wi_lin = WORK_RES_RANGE_MIN;

	/* Update the new phase on disk */
	checkpoint_next_phase(&g_ctx, true, NULL, 0, false, &error);
	if (error)
		fatal_error_from_isi_error(false, error);

	/* For now pick the first worker available in the array */
	queue_deq(&idle_queue, &w);
	if (w == NULL) {
		/*
		 * There can be 0 idle workers available if an idle workers
		 * die during checkpoint_next_phase() in target snapshot
		 * creation loop. Add the work item to restart queue
		 * to be picked up later.
		 */
		work_item_insert(restarts_queue,
		    work_item_new(WORK_RES_RANGE_MIN, false,
		    "Recover work item during start_new_phase"));
		ilog(IL_INFO, "%s called, Added first work item %llu into restarts queue",__func__, 
		    (uint64_t)WORK_RES_RANGE_MIN);
		goto out;
	}

	/* first worker shouldn't have work yet */
	ASSERT(w->work == NULL);
	w->work = work_item_new(m.body.work_resp3.wi_lin, true,
	    "FIRST POST %p", w);

	msg_send(w->socket, &m);

	w->last_split = time(0);
	queue_enq(&splittable_queue, w);

out:
	return;
}

/*
 * Cleanup all job associated traces and make job complete
 */
void
finish_job(struct coord_ctx *ctx)
{
	int i;
	int n_workers;
	struct isi_error *error = NULL;

	ilog(IL_INFO, "finish_job called");
	
	/* If this is STF_PHASE_COMP_FINISH_COMMIT, take the failover snapshot
	 * in order to properly complete the job
	 */
	if (ctx->job_phase.cur_phase ==
	    STF_PHASE_COMP_FINISH_COMMIT) {
		ilog(IL_INFO, "%s called, Taking target snapshot "
		    "after compliance commit phase", __func__);
		do_comp_allow_write_finish(ctx, &error);
		if (error) {
			fatal_error_from_isi_error(true, error);
		}
	}

	record_last_phase_end();

	n_workers = g_ctx.workers->count;
	for (i = 0; i < n_workers; i++)
		reset_worker(g_ctx.workers->workers[i]);

	if (g_ctx.snaptimer) {
		migr_cancel_timeout(g_ctx.snaptimer);
		g_ctx.snaptimer = 0;
	}

	/* disconnect from bandwidth and throttle hosts */
	if (g_ctx.bw_host > 0) {
		migr_rm_fd(g_ctx.bw_host);
		close(g_ctx.bw_host);
		g_ctx.bw_host = -1;
	}
	if (g_ctx.th_host > 0) {
		migr_rm_fd(g_ctx.th_host);
		close(g_ctx.th_host);
		g_ctx.th_host = -1;
	}

	g_ctx.done = true;
}

/*   ____      _ _ _                _        
 *  / ___|__ _| | | |__   __ _  ___| | _____ 
 * | |   / _` | | | '_ \ / _` |/ __| |/ / __|
 * | |__| (_| | | | |_) | (_| | (__|   <\__ \
 *  \____\__,_|_|_|_.__/ \__,_|\___|_|\_\___/
 */

int
redistribute_work_callback(void *ctx)
{
	struct isi_error *error = NULL;
	struct worker *w_active;
	struct worker *w_idle;
	time_t now;
	int num_avail;
	struct generic_msg m;
	struct work_item_node *item, *tmp;
	bool fast_split = false, dont_split = false;

	ilog(IL_INFO, "redistribute_work_callback called");

	redist_tout = NULL;

	UFAIL_POINT_CODE(fast_work_item_split,
	    redist_timeout.tv_sec = 1;
	    fast_split = true;
	);

	UFAIL_POINT_CODE(dont_split,
	    dont_split = true;
	);

	/* periodic accounting sanity check */
	//If we assert, then 
	if (queue_size(&splittable_queue) +
	    queue_size(&idle_queue) +
	    queue_size(&split_pending_queue) +
	    queue_size(&offline_queue) !=
	    g_ctx.workers->count) {
		dump_status(&g_ctx);
		ASSERT(false, 
		    "redistribute_work_callback: queue size mismatch");
	}

	if (job_phase_is_finished()) {///如果前一个job phase完成了,马上开始下一个phase.或者这个job phase需要split
		/* Refresh the job phase inmemory */
		refresh_job_state(&g_ctx.job_phase, STF_JOB_PHASE_LIN,
		    g_ctx.rep, &error);
		if (error)
			goto out;

		if (next_job_phase_valid(&g_ctx.job_phase)) {
			start_new_phase();
			goto out;
		}

		/* Finish the job */
		ilog(IL_INFO, "%s called, all workers are in idle queue. finish up", __func__);
		finish_job(&g_ctx);

		goto out;
	}

	now = time(0);

	/* Best guess at the number of available workers.  All the idle workers
	 * minus any in flight split requests.  This may be negative if idle
	 * workers have died and not respawned yet.  */
	num_avail = queue_size(&idle_queue) - queue_size(&split_pending_queue);

	/* this is where we give restart work items to available idle 
	 * workers, if any exist. if after handing out restart items there are 
	 * still available idle workers, we will send split requests on their 
	 * behalf */
	if (!TAILQ_EMPTY(restarts_queue)) {
		ilog(IL_INFO, "%s called, Processing restarts queue", __func__);
		TAILQ_FOREACH_SAFE(item, restarts_queue, ENTRIES, tmp) {
			w_idle = NULL;
			if (num_avail-- > 0) {
				ilog(IL_INFO, "%s called, Reassigning work item", __func__);
				queue_deq(&idle_queue, &w_idle);
				TAILQ_REMOVE(restarts_queue, item, ENTRIES);
				assert_not_overlapping(item->work);
				assign_restart(w_idle, item->work);
				w_idle->last_split = now;
				queue_enq(&splittable_queue, w_idle);
				free(item);
			} else {
				break;
			}
		}
	}
	/////////当前这个job phase没有完成,那么需要split
	while (num_avail-- > 0) {
		w_active = NULL;
		queue_peek(&splittable_queue, &w_active);

		if (!w_active || dont_split)
			break;

		if (now - w_active->last_split < MIN_SPLIT_INTERVAL_SEC &&
		    fast_split == false)
			/* this worker (and all others remaining in the queue) 
			 * have been split too recently. no more work can be
			 * redistributed at this time */
			break;

		/*
		 * send a split request to the active worker and take it off
		 * the active queue. Also send the split lin to be used to
		 * create new work item. Increment the in-memory cur_work_lin.
		 */
		m.head.type = SPLIT_REQ_MSG;
		ASSERT(w_active->work != NULL);
		if (w_active->work->split_wi_lin) {
			m.body.split_req.split_wi_lin =
			   w_active->work->split_wi_lin;
		} else {
			m.body.split_req.split_wi_lin = g_ctx.cur_work_lin =
			    w_active->work->split_wi_lin =
			    get_next_work_lin(g_ctx.cur_work_lin);
			validate_work_lin(m.body.split_req.split_wi_lin);
		}
		msg_send(w_active->socket, &m);
		queue_deq(&splittable_queue, &w_active);
		queue_enq(&split_pending_queue, w_active);
		ilog(IL_INFO, "%s called, split request sent to worker %d", __func__, w_active->id);
	}

out:
	redist_tout = migr_register_timeout(&redist_timeout,
		redistribute_work_callback, 0);

	if (error)
		fatal_error_from_isi_error(true, error);

	return 0;
}

static int
split_resp_callback(struct generic_msg *srm, void *ctx)
{
	struct worker *w_active = (struct worker *)ctx;
	struct worker *w_idle;
	struct work_item *new_item;
	time_t now;
	struct generic_msg m = {};
	ifs_lin_t split_lin;

	UFAIL_POINT_CODE(ignore_split_resp, goto out;);

	ilog(IL_INFO, "split_resp_callback called");
	g_ctx.conn_alert = time(0);

	/*
	 * It's possible we crossed split request and work request messages in
	 * which case we can leave it alone and expect the work response to
	 * work out which queue this worker should actually be in.
	 */
	if (!queue_contains(&split_pending_queue, w_active)) {
		ilog(IL_INFO, "%s called, Worker %d changed state while the split request "
		    "was pending", __func__, w_active->id);
		goto out;
	}

	queue_rem(&split_pending_queue, w_active);
	ASSERT(w_active->work != NULL);

	/* Always mark the worker's last split. */
	now = time(0);
	w_active->last_split = now;

	/* Put the worker back on the end of the active queue */
	queue_enq(&splittable_queue, w_active);

	/*
	 * If the split failed on the active worker then it is either
	 * unable to figure out a good split (and needs more time) or
	 * nearing its last file.  Either way we'll give it time to
	 * make more progress.
	 */
	if (!srm->body.split_resp.succeeded) {
		ilog(IL_INFO, "%s called, Splitting worker %d failed", __func__, w_active->id);
		goto out;
	}

	ASSERT(w_active->work->split_wi_lin > 0);
	split_lin = w_active->work->split_wi_lin;
	/* Reset the split_lin in current work item */
	w_active->work->split_wi_lin = 0;

	/* There can be 0 idle workers available if an idle worker dies while
	 * the split request is in flight.  Just put the work item on the
	 * restarts list instead of sending it to an idle worker.
	 */
	if (pvec_empty(&idle_queue)) {
		work_item_insert(restarts_queue,
		    work_item_new(split_lin, false,
		    "split-no-idle(%p)[%s]", w_active,
		    fmt_string(&w_active->work->history)));
		goto out;
	}

	/* get a worker from the idle queue and pass it the new work item */
	queue_deq(&idle_queue, &w_idle);
	m.head.type = WORK_RESP_MSG3;
	m.body.work_resp3.wi_lin = split_lin;
	validate_work_lin(split_lin);

	new_item = work_item_new(split_lin, true, "split(%p)[%s]",
	    w_active, fmt_string(&w_active->work->history));

	assert_not_overlapping(new_item);

	/* idle workers should have no work associated with them */
	ASSERT(w_idle->work == NULL);
	w_idle->work = new_item;
	msg_send(w_idle->socket, &m);

	/* add the newly active worker to the end of the active queue */
	w_idle->last_split = now;
	queue_enq(&splittable_queue, w_idle);

	ilog(IL_INFO, "%s called, Successful split from work item %llu to new work "
	    "item %llu", __func__ , w_active->work->wi_lin, new_item->wi_lin);

out:
	return 0;
}

static void
start_first_worker(struct worker *w)
{
	struct isi_error *error = NULL;
	struct generic_msg m = {};

	m.body.work_resp3.wi_lin = get_first_work_lin(g_ctx.rep, &error);
	if (error)
		fatal_error_from_isi_error(false, error);
	validate_work_lin(m.body.work_resp3.wi_lin);
	if (!m.body.work_resp3.wi_lin) {
		error = isi_system_error_new(ENOENT,
		    "missing work lin");
		fatal_error_from_isi_error(false, error);
		isi_error_free(error);
	}

	ASSERT(w->work == NULL);
	w->work = work_item_new( m.body.work_resp3.wi_lin, true,
	    "FIRST POST %p", w);
	m.head.type = WORK_RESP_MSG3;
	msg_send(w->socket, &m);

	w->last_work = w->last_split = time(0);
	queue_rem(&offline_queue, w);
	queue_enq(&splittable_queue, w);
}

static int ////pworker(worker)发送过来的回调函数,请求分配一个workitem
work_req_callback(struct generic_msg *wrm, void *ctx)
{
	struct worker *w = (struct worker *)ctx;////先前coord连接pworker daemon创建的pworker子进程
	struct work_item_node *item;

	ilog(IL_INFO, "work_req_callback called", __func__);
	g_ctx.conn_alert = time(0);

	w->connect_failures = 0;

	if (w->dying) {
		ilog(IL_INFO, "%s called,Dying worker requested work",__func__);
		reset_worker(w);
		queue_rem(&dying_queue, w);
		free(w);
		w = NULL;
		goto dead;
	}

	/* first worker to send work request gets the entire work item */
	if (!g_ctx.started) {
		g_ctx.started = true;
		start_first_worker(w);
		goto out;
	}

	/* record worker's previous task stats */
	g_ctx.stats.errors += wrm->body.work_req.errors;
	g_ctx.stats.dirs += wrm->body.work_req.dirs;
	g_ctx.stats.files += wrm->body.work_req.files;
	g_ctx.stats.transfered += wrm->body.work_req.transfered;
	g_ctx.stats.net += wrm->body.work_req.net;

	queue_rem_all(w);

	if (w->work) {///////既然上一个job phase完成了,就把上一个job phase的workitem删掉.
		work_item_clean(w->work);
		free(w->work);
		w->work = NULL;
	}

	/* assign restarts right away, if available. otherwise, add the worker 
	 * to the idle queue. it will be given some work next time work is 
	 * redistributed */
	if (!TAILQ_EMPTY(restarts_queue)) {
		ilog(IL_INFO, "%s called,restart work item assigned to worker %d", __func__, w->id);
		item = TAILQ_FIRST(restarts_queue);
		TAILQ_REMOVE(restarts_queue, item, ENTRIES);
		assert_not_overlapping(item->work);
		assign_restart(w, item->work);
		free(item);

		w->last_split = time(0);
		queue_enq(&splittable_queue, w);
	} else {
		ilog(IL_INFO, "%s called, moving worker %d to the idle queue", __func__,w->id);
		queue_enq(&idle_queue, w);///一个worker发出了WORK_REQ_MSG之后,应该被加入到idle_queue中
	}

out:
	/* worker may have connected after most recent reallocate calls. in
	 * that case, it missed the last bw/th update. send it now */
	update_bandwidth(w, true);
	update_throttle(w, true);

	/* If the job seems to be finished, reschedule the work timeout to
	 * occur right away to finish off. */
	if (job_phase_is_finished()) {
		static struct timeval tmp_timeout = {0, 10};
		if (redist_tout != NULL) {
			migr_cancel_timeout(redist_tout);
			redist_tout = migr_register_timeout(&tmp_timeout,
				redistribute_work_callback, 0);
		}
	}

dead:
	return 0;
}

void
pworker_tw_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct pworker_tw_stat_msg *ret, struct isi_error **error_out)
{
	struct old_pworker_tw_stat_msg *stat_msg =
	    &m->body.old_pworker_tw_stat;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		if (m->head.type != OLD_PWORKER_TW_STAT_MSG) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->dirs_visited = stat_msg->dirs_visited;
		ret->dirs_dst_deleted = stat_msg->dirs_dst_deleted;
		ret->files_total = stat_msg->files_total;
		ret->files_replicated_selected =
		    stat_msg->files_replicated_selected;
		ret->files_replicated_transferred =
		    stat_msg->files_replicated_transferred;
		ret->files_replicated_new_files =
		    stat_msg->files_replicated_new_files;
		ret->files_replicated_updated_files =
		    stat_msg->files_replicated_updated_files;
		ret->files_replicated_with_ads =
		    stat_msg->files_replicated_files_with_ads;
		ret->files_replicated_ads_streams =
		    stat_msg->files_replicated_ads_streams;
		ret->files_replicated_symlinks =
		    stat_msg->files_replicated_symlinks;
		ret->files_replicated_block_specs =
		    stat_msg->files_replicated_block_specs;
		ret->files_replicated_char_specs =
		    stat_msg->files_replicated_char_specs;
		ret->files_replicated_sockets =
		    stat_msg->files_replicated_sockets;
		ret->files_replicated_fifos = stat_msg->files_replicated_fifos;
		ret->files_replicated_hard_links =
		    stat_msg->files_replicated_hard_links;
		ret->files_deleted_src = stat_msg->files_deleted_src;
		ret->files_deleted_dst = stat_msg->files_deleted_dst;
		ret->files_skipped_up_to_date =
		    stat_msg->files_skipped_up_to_date;
		ret->files_skipped_user_conflict =
		    stat_msg->files_skipped_user_conflict;
		ret->files_skipped_error_io = stat_msg->files_skipped_error_io;
		ret->files_skipped_error_net =
		    stat_msg->files_skipped_error_net;
		ret->files_skipped_error_checksum =
		    stat_msg->files_skipped_error_checksum;
		ret->bytes_transferred = stat_msg->bytes_transferred;
		ret->bytes_data_total = stat_msg->bytes_data_total;
		ret->bytes_data_file = stat_msg->bytes_data_file;
		ret->bytes_data_sparse = stat_msg->bytes_data_sparse;
		ret->bytes_data_unchanged = stat_msg->bytes_data_unchanged;
		ret->bytes_network_total = stat_msg->bytes_network_total;
		ret->bytes_network_to_target =
		    stat_msg->bytes_network_to_target;
		ret->bytes_network_to_source =
		    stat_msg->bytes_network_to_source;
	} else {
		if (m->head.type != build_ript_msg_type(PWORKER_TW_STAT_MSG)) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_VISITED, 1,
		&ret->dirs_visited, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_DELETED_DST, 1,
		&ret->dirs_dst_deleted, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_TOTAL, 1,
		&ret->files_total, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SELECTED,
		    1, &ret->files_replicated_selected, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_TRANSFERRED, 1,
		    &ret->files_replicated_transferred, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_NEW_FILES,
		    1, &ret->files_replicated_new_files, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_UPDATED_FILES, 1,
		    &ret->files_replicated_updated_files, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_WITH_ADS,
		    1, &ret->files_replicated_with_ads, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_ADS_STREAMS, 1,
		    &ret->files_replicated_ads_streams, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SYMLINKS,
		    1, &ret->files_replicated_symlinks, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_BLOCK_SPECS, 1,
		    &ret->files_replicated_block_specs, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_CHAR_SPECS, 1,
		    &ret->files_replicated_char_specs, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SOCKETS,
		    1, &ret->files_replicated_sockets, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_FIFOS, 1,
		    &ret->files_replicated_fifos, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_HARD_LINKS, 1,
		    &ret->files_replicated_hard_links, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_SRC, 1,
		    &ret->files_deleted_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_DST, 1,
		    &ret->files_deleted_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_UP_TO_DATE,
		    1, &ret->files_skipped_up_to_date, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_SKIPPED_USER_CONFLICT, 1,
		    &ret->files_skipped_user_conflict, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_ERROR_IO, 1,
		    &ret->files_skipped_error_io, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_ERROR_NET, 1,
		    &ret->files_skipped_error_net, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_SKIPPED_ERROR_CHECKSUM, 1,
		    &ret->files_skipped_error_checksum, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_TRANSFERRED, 1,
		    &ret->bytes_transferred, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_TOTAL, 1,
		    &ret->bytes_data_total, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_FILE, 1,
		    &ret->bytes_data_file, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_SPARSE, 1,
		    &ret->bytes_data_sparse, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_UNCHANGED, 1,
		    &ret->bytes_data_unchanged, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TOTAL, 1,
		    &ret->bytes_network_total, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TO_TARGET, 1,
		    &ret->bytes_network_to_target, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TO_SOURCE, 1,
		    &ret->bytes_network_to_source, &error, out);
	}

	log(TRACE, "%s: dirs=%d files=%d", __func__, ret->dirs_visited,
	    ret->files_total);

out:
	isi_error_handle(error, error_out);
}

void
sworker_tw_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct sworker_tw_stat_msg *ret, struct isi_error **error_out)
{
	struct old_sworker_tw_stat_msg *stat_msg =
	    &m->body.old_sworker_tw_stat;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		if (m->head.type != OLD_SWORKER_TW_STAT_MSG) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->dirs_visited = stat_msg->dirs_visited;
		ret->dirs_dst_deleted = stat_msg->dirs_dst_deleted;
		ret->files_total = stat_msg->files_total;
		ret->files_replicated_selected =
		    stat_msg->files_replicated_selected;
		ret->files_replicated_transferred =
		    stat_msg->files_replicated_transferred;
		ret->files_deleted_src = stat_msg->files_deleted_src;
		ret->files_deleted_dst = stat_msg->files_deleted_dst;
		ret->files_skipped_up_to_date =
		    stat_msg->files_skipped_up_to_date;
		ret->files_skipped_user_conflict =
		    stat_msg->files_skipped_user_conflict;
		ret->files_skipped_error_io = stat_msg->files_skipped_error_io;
		ret->files_skipped_error_net =
		    stat_msg->files_skipped_error_net;
		ret->files_skipped_error_checksum =
		    stat_msg->files_skipped_error_checksum;
	} else {
		if (m->head.type != build_ript_msg_type(SWORKER_TW_STAT_MSG)) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_VISITED, 1,
		    &ret->dirs_visited, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_DELETED_DST, 1,
		    &ret->dirs_dst_deleted, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_TOTAL, 1,
		    &ret->files_total, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SELECTED,
		    1, &ret->files_replicated_selected, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_TRANSFERRED, 1,
		    &ret->files_replicated_transferred, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_SRC, 1,
		    &ret->files_deleted_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_DST, 1,
		    &ret->files_deleted_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_UP_TO_DATE,
		    1, &ret->files_skipped_up_to_date, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_SKIPPED_USER_CONFLICT, 1,
		    &ret->files_skipped_user_conflict, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_ERROR_IO, 1,
		    &ret->files_skipped_error_io, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_SKIPPED_ERROR_NET, 1,
		    &ret->files_skipped_error_net, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_SKIPPED_ERROR_CHECKSUM, 1,
		    &ret->files_skipped_error_checksum, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_COMP_COMMITTED_FILES, 1,
		    &ret->committed_files, &error, out);
	}

	log(TRACE, "%s: dirs=%d diles=%d", __func__, ret->dirs_visited,
	    ret->files_total);

out:
	isi_error_handle(error, error_out);
}

void
pworker_stf_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct pworker_stf_stat_msg *ret, struct isi_error **error_out)
{
	struct old_pworker_stf_stat_msg *stat_msg =
	    &m->body.old_pworker_stf_stat;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		if (m->head.type != OLD_PWORKER_STF_STAT_MSG) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->dirs_created_src = stat_msg->dirs_created_src;
		ret->dirs_deleted_src = stat_msg->dirs_deleted_src;
		ret->dirs_linked_src = stat_msg->dirs_linked_src;
		ret->dirs_unlinked_src = stat_msg->dirs_unlinked_src;
		ret->dirs_replicated = stat_msg->dirs_replicated;
		ret->files_total = stat_msg->files_total;
		ret->files_replicated_new_files =
		    stat_msg->files_replicated_new_files;
		ret->files_replicated_updated_files =
		    stat_msg->files_replicated_updated_files;
		ret->files_replicated_regular =
		    stat_msg->files_replicated_regular;
		ret->files_replicated_with_ads =
		    stat_msg->files_replicated_with_ads;
		ret->files_replicated_ads_streams =
		    stat_msg->files_replicated_ads_streams;
		ret->files_replicated_symlinks =
		    stat_msg->files_replicated_symlinks;
		ret->files_replicated_block_specs =
		    stat_msg->files_replicated_block_specs;
		ret->files_replicated_char_specs =
		    stat_msg->files_replicated_char_specs;
		ret->files_replicated_sockets =
		    stat_msg->files_replicated_sockets;
		ret->files_replicated_fifos =
		    stat_msg->files_replicated_fifos;
		ret->files_replicated_hard_links =
		    stat_msg->files_replicated_hard_links;
		ret->files_deleted_src = stat_msg->files_deleted_src;
		ret->files_linked_src = stat_msg->files_linked_src;
		ret->files_unlinked_src = stat_msg->files_unlinked_src;
		ret->bytes_data_total = stat_msg->bytes_data_total;
		ret->bytes_data_file = stat_msg->bytes_data_file;
		ret->bytes_data_sparse = stat_msg->bytes_data_sparse;
		ret->bytes_data_unchanged = stat_msg->bytes_data_unchanged;
		ret->bytes_network_total = stat_msg->bytes_network_total;
		ret->bytes_network_to_target =
		    stat_msg->bytes_network_to_target;
		ret->bytes_network_to_source =
		    stat_msg->bytes_network_to_source;
		ret->cc_lins_total = stat_msg->cc_lins_total;
		ret->cc_dirs_new = stat_msg->cc_dirs_new;
		ret->cc_dirs_deleted = stat_msg->cc_dirs_deleted;
		ret->cc_dirs_moved = stat_msg->cc_dirs_moved;
		ret->cc_dirs_changed = stat_msg->cc_dirs_changed;
		ret->cc_files_new = stat_msg->cc_files_new;
		ret->cc_files_linked = stat_msg->cc_files_linked;
		ret->cc_files_unlinked = stat_msg->cc_files_unlinked;
		ret->cc_files_changed = stat_msg->cc_files_changed;
		ret->ct_hash_exceptions_found =
		    stat_msg->ct_hash_exceptions_found;
		ret->ct_hash_exceptions_fixed =
		    stat_msg->ct_hash_exceptions_fixed;
		ret->ct_flipped_lins = stat_msg->ct_flipped_lins;
		ret->ct_corrected_lins = stat_msg->ct_corrected_lins;
		ret->ct_resynced_lins = stat_msg->ct_resynced_lins;
	} else {
		if (m->head.type != build_ript_msg_type(PWORKER_STF_STAT_MSG)) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_CREATED_SRC, 1,
		    &ret->dirs_created_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_DELETED_SRC, 1,
		    &ret->dirs_deleted_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_LINKED_SRC, 1,
		    &ret->dirs_linked_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_UNLINKED_SRC, 1,
		    &ret->dirs_unlinked_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_REPLICATED, 1,
		    &ret->dirs_replicated, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_TOTAL, 1,
		    &ret->files_total, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_NEW_FILES,
		    1, &ret->files_replicated_new_files, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_UPDATED_FILES, 1,
		    &ret->files_replicated_updated_files, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_REGULAR,
		    1, &ret->files_replicated_regular, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_WITH_ADS,
		    1, &ret->files_replicated_with_ads, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_ADS_STREAMS, 1,
		    &ret->files_replicated_ads_streams, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SYMLINKS,
		    1, &ret->files_replicated_symlinks, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_BLOCK_SPECS, 1,
		    &ret->files_replicated_block_specs, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_CHAR_SPECS, 1,
		    &ret->files_replicated_char_specs, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_SOCKETS,
		    1, &ret->files_replicated_sockets, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_REPLICATED_FIFOS, 1,
		    &ret->files_replicated_fifos, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_FILES_REPLICATED_HARD_LINKS, 1,
		    &ret->files_replicated_hard_links, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_SRC, 1,
		    &ret->files_deleted_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_LINKED_SRC, 1,
		    &ret->files_linked_src, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_UNLINKED_SRC, 1,
		    &ret->files_unlinked_src, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_TOTAL, 1,
		    &ret->bytes_data_total, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_FILE, 1,
		    &ret->bytes_data_file, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_SPARSE, 1,
		    &ret->bytes_data_sparse, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_DATA_UNCHANGED, 1,
		    &ret->bytes_data_unchanged, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TOTAL, 1,
		    &ret->bytes_network_total, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TO_TARGET, 1,
		    &ret->bytes_network_to_target, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_BYTES_NETWORK_TO_SOURCE, 1,
		    &ret->bytes_network_to_source, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_LINS_TOTAL, 1,
		    &ret->cc_lins_total, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_DIRS_NEW, 1,
		    &ret->cc_dirs_new, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_DIRS_DELETED, 1,
		    &ret->cc_dirs_deleted, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_DIRS_MOVED, 1,
		    &ret->cc_dirs_moved, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_DIRS_CHANGED, 1,
		    &ret->cc_dirs_changed, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_FILES_NEW, 1,
		    &ret->cc_files_new, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_FILES_LINKED, 1,
		    &ret->cc_files_linked, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_FILES_UNLINKED, 1,
		    &ret->cc_files_unlinked, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CC_FILES_CHANGED, 1,
		    &ret->cc_files_changed, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CT_HASH_EXCEPTIONS_FOUND,
		    1, &ret->ct_hash_exceptions_found, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CT_HASH_EXCEPTIONS_FIXED,
		    1, &ret->ct_hash_exceptions_fixed, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CT_FLIPPED_LINS, 1,
		    &ret->ct_flipped_lins, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CT_CORRECTED_LINS, 1,
		    &ret->ct_corrected_lins, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_CT_RESYNCED_LINS, 1,
		    &ret->ct_resynced_lins, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_RESYNC_COMPLIANCE_DIRS_NEW,
		    1, &ret->resync_compliance_dirs_new, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_RESYNC_COMPLIANCE_DIRS_LINKED, 1,
		    &ret->resync_compliance_dirs_linked, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_RESYNC_CONFLICT_FILES_NEW,
		    1, &ret->resync_conflict_files_new, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript,
		    RMF_RESYNC_CONFLICT_FILES_LINKED, 1,
		    &ret->resync_conflict_files_linked, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

void
sworker_stf_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct sworker_stf_stat_msg *ret, struct isi_error **error_out)
{
	struct old_sworker_stf_stat_msg *stat_msg =
	    &m->body.old_sworker_stf_stat;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		if (m->head.type != OLD_SWORKER_STF_STAT_MSG) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->dirs_created_dst = stat_msg->dirs_created_dst;
		ret->dirs_deleted_dst = stat_msg->dirs_deleted_dst;
		ret->dirs_linked_dst = stat_msg->dirs_linked_dst;
		ret->dirs_unlinked_dst = stat_msg->dirs_unlinked_dst;
		ret->files_deleted_dst = stat_msg->files_deleted_dst;
		ret->files_linked_dst = stat_msg->files_linked_dst;
		ret->files_unlinked_dst = stat_msg->files_unlinked_dst;
	} else {
		if (m->head.type != build_ript_msg_type(SWORKER_STF_STAT_MSG)) {
			log(FATAL, "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_CREATED_DST, 1,
		    &ret->dirs_created_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_DELETED_DST, 1,
		    &ret->dirs_deleted_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_LINKED_DST, 1,
		    &ret->dirs_linked_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DIRS_UNLINKED_DST, 1,
		    &ret->dirs_unlinked_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_DELETED_DST, 1,
		    &ret->files_deleted_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_LINKED_DST, 1,
		    &ret->files_linked_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FILES_UNLINKED_DST, 1,
		    &ret->files_unlinked_dst, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_COMPLIANCE_CONFLICTS,
		    1, &ret->compliance_conflicts, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_COMP_COMMITTED_FILES, 1,
		    &ret->committed_files, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}


static int
pworker_tw_stat_callback(struct generic_msg *m, void *ctx)
{
	struct pworker_tw_stat_msg stat = {};
	struct worker *w = ctx;
	struct isi_error *error = NULL;

	/* node may be NULL due to group change and we're receiving a stat
	 * update from a 'dying' worker */
	pworker_tw_stat_msg_unpack(m, g_ctx.job_ver.common, &stat, &error);
	if (error) {
		/* It's a stat unpacking error...who cares? */
		log(ERROR, "%s: Error unpacking message: %s", __func__,
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	JOB_SUMM->total->stat->dirs->src->visited += stat.dirs_visited;
	JOB_SUMM->total->stat->dirs->dst->deleted += stat.dirs_dst_deleted;
	JOB_SUMM->total->stat->files->total += stat.files_total;
	JOB_SUMM->total->stat->files->replicated->selected +=
	    stat.files_replicated_selected;
	JOB_SUMM->total->stat->files->replicated->transferred +=
	    stat.files_replicated_transferred;
	JOB_SUMM->total->stat->files->replicated->new_files +=
	    stat.files_replicated_new_files;
	JOB_SUMM->total->stat->files->replicated->updated_files +=
	    stat.files_replicated_updated_files;
	JOB_SUMM->total->stat->files->replicated->files_with_ads +=
	    stat.files_replicated_with_ads;
	JOB_SUMM->total->stat->files->replicated->ads_streams +=
	    stat.files_replicated_ads_streams;
	JOB_SUMM->total->stat->files->replicated->symlinks +=
	    stat.files_replicated_symlinks;
	JOB_SUMM->total->stat->files->replicated->block_specs +=
	    stat.files_replicated_block_specs;
	JOB_SUMM->total->stat->files->replicated->char_specs +=
	    stat.files_replicated_char_specs;
	JOB_SUMM->total->stat->files->replicated->sockets +=
	    stat.files_replicated_sockets;
	JOB_SUMM->total->stat->files->replicated->fifos +=
	    stat.files_replicated_fifos;
	JOB_SUMM->total->stat->files->replicated->hard_links +=
	    stat.files_replicated_hard_links;
	JOB_SUMM->total->stat->files->deleted->src += stat.files_deleted_src;
	JOB_SUMM->total->stat->files->deleted->dst += stat.files_deleted_dst;
	JOB_SUMM->total->stat->files->skipped->up_to_date +=
	    stat.files_skipped_up_to_date;
	JOB_SUMM->total->stat->files->skipped->user_conflict +=
	    stat.files_skipped_user_conflict;
	JOB_SUMM->total->stat->files->skipped->error_io +=
	    stat.files_skipped_error_io;
	JOB_SUMM->total->stat->files->skipped->error_net +=
	    stat.files_skipped_error_net;
	JOB_SUMM->total->stat->files->skipped->error_checksum +=
	    stat.files_skipped_error_checksum;
	JOB_SUMM->total->stat->bytes->transferred += stat.bytes_transferred;
	JOB_SUMM->total->stat->bytes->data->total += stat.bytes_data_total;
	JOB_SUMM->total->stat->bytes->data->file += stat.bytes_data_file;
	JOB_SUMM->total->stat->bytes->data->sparse += stat.bytes_data_sparse;
	JOB_SUMM->total->stat->bytes->data->unchanged +=
	    stat.bytes_data_unchanged;
	JOB_SUMM->total->stat->bytes->network->total +=
	    stat.bytes_network_total;
	JOB_SUMM->total->stat->bytes->network->to_target +=
	    stat.bytes_network_to_target;
	JOB_SUMM->total->stat->bytes->network->to_source +=
	    stat.bytes_network_to_source;

	/* node may be NULL due to group change and we're receiving a stat
	 * update from a 'dying' worker */
	if (w->s_node) {
		w->s_node->bytes_sent += stat.bytes_transferred;
		w->s_node->files_sent += stat.files_total;
	}

	return 0;
}

static int
sworker_tw_stat_callback(struct generic_msg *m, void *ctx)
{
	struct sworker_tw_stat_msg stat = {};
	struct isi_error *error = NULL;

	sworker_tw_stat_msg_unpack(m, g_ctx.job_ver.common, &stat, &error);
	if (error) {
		/* It's a stat unpacking error...who cares? */
		log(ERROR, "%s: Error unpacking message: %s", __func__,
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	JOB_SUMM->total->stat->dirs->src->visited += stat.dirs_visited;
	JOB_SUMM->total->stat->dirs->dst->deleted += stat.dirs_dst_deleted;
	JOB_SUMM->total->stat->files->total += stat.files_total;
	JOB_SUMM->total->stat->files->replicated->selected +=
	    stat.files_replicated_selected;
	JOB_SUMM->total->stat->files->replicated->transferred +=
	    stat.files_replicated_transferred;
	JOB_SUMM->total->stat->files->deleted->src += stat.files_deleted_src;
	JOB_SUMM->total->stat->files->deleted->dst += stat.files_deleted_dst;
	JOB_SUMM->total->stat->files->skipped->up_to_date +=
	    stat.files_skipped_up_to_date;
	JOB_SUMM->total->stat->files->skipped->user_conflict +=
	    stat.files_skipped_user_conflict;
	JOB_SUMM->total->stat->files->skipped->error_io +=
	    stat.files_skipped_error_io;
	JOB_SUMM->total->stat->files->skipped->error_net +=
	    stat.files_skipped_error_net;
	JOB_SUMM->total->stat->files->skipped->error_checksum +=
	    stat.files_skipped_error_checksum;
	JOB_SUMM->total->stat->compliance->committed_files +=
	    stat.committed_files;

	return 0;
}

static int
pworker_stf_stat_callback(struct generic_msg *m, void *ctx)
{
	struct pworker_stf_stat_msg stat = {};
	struct worker *w = ctx;
	struct isi_error *error = NULL;

	pworker_stf_stat_msg_unpack(m, g_ctx.job_ver.common, &stat, &error);
	if (error) {
		/* It's a stat unpacking error...who cares? */
		log(ERROR, "%s: Error unpacking message: %s", __func__,
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	JOB_SUMM->total->stat->dirs->created->src += stat.dirs_created_src;
	JOB_SUMM->total->stat->dirs->deleted->src += stat.dirs_deleted_src;
	JOB_SUMM->total->stat->dirs->linked->src += stat.dirs_linked_src;
	JOB_SUMM->total->stat->dirs->unlinked->src += stat.dirs_unlinked_src;
	JOB_SUMM->total->stat->dirs->replicated += stat.dirs_replicated;
	JOB_SUMM->total->stat->files->total += stat.files_total;
	JOB_SUMM->total->stat->files->replicated->new_files +=
	    stat.files_replicated_new_files;
	JOB_SUMM->total->stat->files->replicated->updated_files +=
	    stat.files_replicated_updated_files;
	JOB_SUMM->total->stat->files->replicated->regular +=
	    stat.files_replicated_regular;
	JOB_SUMM->total->stat->files->replicated->files_with_ads +=
	    stat.files_replicated_with_ads;
	JOB_SUMM->total->stat->files->replicated->ads_streams +=
	    stat.files_replicated_ads_streams;
	JOB_SUMM->total->stat->files->replicated->symlinks +=
	    stat.files_replicated_symlinks;
	JOB_SUMM->total->stat->files->replicated->block_specs +=
	    stat.files_replicated_block_specs;
	JOB_SUMM->total->stat->files->replicated->char_specs +=
	    stat.files_replicated_char_specs;
	JOB_SUMM->total->stat->files->replicated->sockets +=
	    stat.files_replicated_sockets;
	JOB_SUMM->total->stat->files->replicated->fifos +=
	    stat.files_replicated_fifos;
	JOB_SUMM->total->stat->files->replicated->hard_links +=
	    stat.files_replicated_hard_links;
	JOB_SUMM->total->stat->files->deleted->src +=
	    stat.files_deleted_src;
	JOB_SUMM->total->stat->files->linked->src += stat.files_linked_src;
	JOB_SUMM->total->stat->files->unlinked->src += stat.files_unlinked_src;
	JOB_SUMM->total->stat->bytes->data->total += stat.bytes_data_total;
	JOB_SUMM->total->stat->bytes->data->file += stat.bytes_data_file;
	JOB_SUMM->total->stat->bytes->data->sparse += stat.bytes_data_sparse;
	JOB_SUMM->total->stat->bytes->data->unchanged +=
	    stat.bytes_data_unchanged;
	JOB_SUMM->total->stat->bytes->network->total +=
	    stat.bytes_network_total;
	JOB_SUMM->total->stat->bytes->network->to_target +=
	    stat.bytes_network_to_target;
	JOB_SUMM->total->stat->bytes->network->to_source +=
	    stat.bytes_network_to_source;
	JOB_SUMM->total->stat->change_compute->lins_total += stat.cc_lins_total;
	JOB_SUMM->total->stat->change_compute->dirs_new += stat.cc_dirs_new;
	JOB_SUMM->total->stat->change_compute->dirs_deleted +=
	    stat.cc_dirs_deleted;
	JOB_SUMM->total->stat->change_compute->dirs_moved += stat.cc_dirs_moved;
	JOB_SUMM->total->stat->change_compute->dirs_changed +=
	    stat.cc_dirs_changed;
	JOB_SUMM->total->stat->change_compute->files_new += stat.cc_files_new;
	JOB_SUMM->total->stat->change_compute->files_linked +=
	    stat.cc_files_linked;
	JOB_SUMM->total->stat->change_compute->files_unlinked +=
	    stat.cc_files_unlinked;
	JOB_SUMM->total->stat->change_compute->files_changed +=
	    stat.cc_files_changed;
	JOB_SUMM->total->stat->change_transfer->hash_exceptions_found +=
	    stat.ct_hash_exceptions_found;
	JOB_SUMM->total->stat->change_transfer->hash_exceptions_fixed +=
	    stat.ct_hash_exceptions_fixed;
	JOB_SUMM->total->stat->change_transfer->flipped_lins +=
	    stat.ct_flipped_lins;
	JOB_SUMM->total->stat->change_transfer->corrected_lins +=
	    stat.ct_corrected_lins;
	JOB_SUMM->total->stat->change_transfer->resynced_lins +=
	    stat.ct_resynced_lins;
	JOB_SUMM->total->stat->compliance->resync_compliance_dirs_new +=
	    stat.resync_compliance_dirs_new;
	JOB_SUMM->total->stat->compliance->resync_compliance_dirs_linked +=
	    stat.resync_compliance_dirs_linked;
	JOB_SUMM->total->stat->compliance->resync_conflict_files_new +=
	    stat.resync_conflict_files_new;
	JOB_SUMM->total->stat->compliance->resync_conflict_files_linked +=
	    stat.resync_conflict_files_linked;

	if (w->s_node) {
		w->s_node->bytes_sent += stat.bytes_network_total;
		w->s_node->files_sent += stat.files_total;
	}

	return 0;
}

static int
sworker_stf_stat_callback(struct generic_msg *m, void *ctx)
{
	struct sworker_stf_stat_msg stat = {};
	struct isi_error *error = NULL;

	sworker_stf_stat_msg_unpack(m, g_ctx.job_ver.common, &stat, &error);
	if (error) {
		/* It's a stat unpacking error...who cares? */
		log(ERROR, "%s: Error unpacking message: %s", __func__,
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	JOB_SUMM->total->stat->dirs->created->dst += stat.dirs_created_dst;
	JOB_SUMM->total->stat->dirs->deleted->dst += stat.dirs_deleted_dst;
	JOB_SUMM->total->stat->dirs->linked->dst += stat.dirs_linked_dst;
	JOB_SUMM->total->stat->dirs->unlinked->dst += stat.dirs_unlinked_dst;
	JOB_SUMM->total->stat->files->deleted->dst += stat.files_deleted_dst;
	JOB_SUMM->total->stat->files->linked->dst += stat.files_linked_dst;
	JOB_SUMM->total->stat->files->unlinked->dst += stat.files_unlinked_dst;
	JOB_SUMM->total->stat->compliance->conflicts +=
	    stat.compliance_conflicts;
	JOB_SUMM->total->stat->compliance->committed_files +=
	    stat.committed_files;

	return 0;
}

static int
list_callback(struct generic_msg *m, void *ctx)
{
	log(TRACE, "list callback in coordinator, dump msg to file");

	if (g_ctx.rc.fd_deleted > 0) {
		dump_listmsg_to_file(g_ctx.rc.fd_deleted, m->body.list.list,
		    m->body.list.used);
	}
	return 0;
}

static int
general_signal_callback(struct generic_msg *m, void *ctx)
{
	struct worker *w = (struct worker *)ctx;
	ASSERT(m->body.general_signal.signal == SIQ_GENSIG_BUSY_EXIT);
	log(DEBUG, "%s: setting shuffle", __func__);
	w->shuffle = true;
	return 0;
}

static int
disconnect_callback(struct generic_msg *m, void *ctx)
{
	struct worker *w = (struct worker *)ctx;

	if (w->s_node) {
		log(DEBUG, "Primary %s disconnected", w->s_node->host);
	}

	w->shuffle = true;

	//Only increment connect_failures if we are not using
	//worker pools shuffling to try other nodes.
	if (w->work == NULL && g_ctx.rc.disable_work) {
		w->connect_failures++;
	}

	reset_worker(w);

	if (w->dying) {
		log(DEBUG, "Trashing dead worker");
		queue_rem(&dying_queue, w);
		free(w);
		w = NULL;
		goto out;
	}

	if (!g_ctx.done)
		register_timer_callback(TIMER_DELAY);

out:
	return 0;
}

/*
 * Add all work items except first one to the recovery queue
 */
static void
add_work_to_restart(struct coord_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct work_item_queue *recovery_items = NULL, *tmp;
	struct rep_iter  *rep_iter = NULL;
	struct work_restart_state twork;
	struct work_item *new_item;
	struct work_item_node *node;
	bool first_work = true;
	ifs_lin_t wi_lin, last_wi_lin = 0;
	bool got_one;
	bool exists;

	log(TRACE, "add_work_to_restart");
	ASSERT(TAILQ_EMPTY(restarts_queue));

	recovery_items = calloc(1, sizeof(*recovery_items));
	ASSERT(recovery_items);
	TAILQ_INIT(recovery_items);

	rep_iter = new_rep_iter(ctx->rep);
	ASSERT(rep_iter != NULL);
	while (1) {
		got_one = work_iter_next(rep_iter, &wi_lin, &twork, &error);
		if (error)
			goto out;

		if (!got_one)
			break;

		/* Increment work_restart_state coordinator restart count
		 * and resave to disk */
		twork.num_coord_restarts++;
		set_work_entry(wi_lin, &twork, NULL, 0, ctx->rep, &error);
		if (error)
			goto out;

		/* Ensure the on-disk entry for the LIN has not been removed by
		 * a dangling pworker from a previous job. If it has, keep it from
		 * processing the entry by moving onto the next item */
		exists = entry_exists(wi_lin, ctx->rep, &error);
		if (error)
			goto out;
		else if (!exists)
			continue;

		log(TRACE, "Found work item %llx during recovery", wi_lin);
		last_wi_lin = wi_lin;
		if(first_work) {
			first_work = false;
			continue;
		}
		log(TRACE, "Adding work item lin %llx to restart queue",
		    wi_lin);
		new_item = work_item_new(wi_lin, false, "Recover work");

		work_item_insert(recovery_items, new_item);
	}

	/* We should find atleast one work item */
	ASSERT(last_wi_lin > 0);
	ctx->cur_work_lin = last_wi_lin;
	tmp = restarts_queue;
	restarts_queue = recovery_items;
	recovery_items = tmp;
out:
	if (error) {
		if (recovery_items) {
			while (!TAILQ_EMPTY(recovery_items)) {
				node = TAILQ_FIRST(recovery_items);
				TAILQ_REMOVE(recovery_items, node, ENTRIES);
				work_item_clean(node->work);
				free(node->work);
				free(node);
			}
			free(recovery_items);
		}
	}
	close_rep_iter(rep_iter);
	isi_error_handle(error, error_out);
}

static void
remove_3_0_tw_checkpoint(struct coord_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char rep_name[MAXNAMLEN];

	get_base_rep_name(rep_name, ctx->rc.job_id, false);
	remove_repstate(ctx->rc.job_id, rep_name, false, &error);
	if (error)
		goto out;
	log(ERROR, "Removed previous version treewalk checkpoint and "
	    "restarting job");
	_exit(-1);

out:
	isi_error_handle(error, error_out);
}

/*
 *	 	STF RECOVERY FUNCTION
 * 
 * This function is main recovery loop for stf repeated syncs.
 * The logic of checkpoint and recovery is as follows
 *
 * Checkpoint
 * ---------------------------
 * 1) When the job is initiated for new snapshot, the coord would 
 *    create job_state and initial work item in rep2 in one transaction.
 * 2) After every phase of job, the coord updates in memory g_ctx.job_phase
 *    and records the same on-disk. Also it adds new work item for the phase.
 * 3) Every time coord starts new job phase, the number of work items
 *    should be 0 before it creates new work item for phase. 
 *
 * Restart
 * ---------------------------
 * On restart the job could be in following states
 * 1) Neither job_state nor work item present: This means we crashed before
 *    we created initial job state. So start from first phase of job.
 * 2) job_state present but no work items : This means that all workers have 
 *    finished their jobs and deleted their work items. So we crashed 
 *    just before we could move to next job phase. Hence coord would read 
 *    current job_phase and update it to move to next phase. It also creates
 *    new work item for new phase.
 * 3) job_state and work_items are present: This is most common case where 
 *    we crashed somewhere middle of job. The coord would just read the 
 *    job_state and then it reads all work items (except first one) and 
 *    move them to restart queue. This way the job would start from point
 *    it crashed.
 */
static bool
load_stf_work_items(struct coord_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_lin_t lin;
	bool recovered = false;
	char *jbuf = NULL;
	size_t jsize;
	int lock_fd = -1;

	log(TRACE, "load_stf_work_items");

	/* Obtain an exclusive lock on this policy's work items. This will hang
	 * until all dangling pworkers for this policy die. Once we have the
	 * lock, we know all dangling pworkers are dead, so we can immediately
	 * release it. This helps us avoid race conditions where dangling
	 * pworkers remove/modify work items from disk after we've read them.
	 * It also prevents us from handing out work items that overlap with
	 * dangling workers. */
	lock_fd = lock_all_work_items(ctx->rc.job_id, WORK_LOCK_TIMEOUT,
	    &error);
	if (error)
		goto out;
	close(lock_fd);

	lin = get_first_work_lin(ctx->rep, &error);
	if (error)
		goto out;

	jsize = job_phase_max_size();
	jbuf = malloc(jsize);
	memset(jbuf, 0, jsize);
	get_state_entry(STF_JOB_PHASE_LIN, jbuf, &jsize,
	    ctx->rep, &error);
	if (error)
		goto out;
	read_job_state(&ctx->job_phase, jbuf, jsize);
	JOB_SUMM->total_phases = get_total_enabled_phases(&g_ctx.job_phase);

	/*
	 * We will clear the previous version checkpoint if its
	 * treewalk. Then we will die. The next coordinator will
	 * recreate the repstate and restart the job.
	 */

	if (ctx->job_phase.jtype == JOB_TW) {
		remove_3_0_tw_checkpoint(ctx, &error);
		if (error)
			goto out;	
	}

	/*
	 * If there are no work items, then it means we crashed
	 * at the end of phase. So we could move to next phase and
	 * add new work item
	 */
	if (!lin) {
		if(next_job_phase_valid(&g_ctx.job_phase)) {
			checkpoint_next_phase(&g_ctx, true, NULL, 0,
			    false, &error);
			if (error)
				goto out;
		} else {
			/* Job done, so no need to work anymore  */
			recovered = false;
			goto out;
		}
	} else {
		/*
		 * Normal crash,
		 * Get all work items except from first one.
		 * Put each work item into restart queue
		 * The first work item will be picked up by
		 * start_first_worker().
		 */
		add_work_to_restart(ctx, &error);
		if (error)
			goto out;
		record_phase_start(ctx->job_phase.cur_phase);
	}

	recovered = true;
out:
	if (jbuf)
		free(jbuf);

	isi_error_handle(error, error_out);
	return recovered;
}

bool
load_recovery_items(struct isi_error **error_out)
{
	bool done;
	struct isi_error *error = NULL;

	done = !load_stf_work_items(&g_ctx, &error);

	isi_error_handle(error, error_out);
	return done;
}

void
initialize_queues(void)
{
	pvec_init(&splittable_queue);
	pvec_init(&idle_queue);
	pvec_init(&split_pending_queue);
	pvec_init(&offline_queue);
	pvec_init(&dying_queue);

	restarts_queue = calloc(1, sizeof(struct work_item_queue));
	ASSERT(restarts_queue);
	TAILQ_INIT(restarts_queue);
}

static int
position_callback(struct generic_msg *m, void *ctx)
{
	return 0;
}

static char *
get_lin_path(ifs_lin_t lin, ifs_snapid_t snapid)
{
	struct fmt FMT_INIT_CLEAN(ls_fmt);
	char *path = NULL, *lin_path = NULL;

	fmt_print(&ls_fmt, "%{}", lin_snapid_fmt(lin, snapid));
	path = get_valid_rep_path(lin, snapid);
	asprintf(&lin_path, "%s (%s)", path, fmt_string(&ls_fmt));
	ASSERT(lin_path);
	free(path);
	return lin_path;
}

void
get_error_str(struct generic_msg *m, struct coord_ctx *ctx, char **return_str)
{
	char *local_error = NULL;
	char *error_str = NULL;
	char *path = NULL;
	char *from_path = NULL;

	ASSERT(return_str != NULL);

	if (m->body.siq_err.errstr != NULL) {
		asprintf(&local_error, "%s", m->body.siq_err.errstr);
	} else {
		asprintf(&local_error, "%s", strerror(m->body.siq_err.error));
	}

	switch(m->body.siq_err.siqerr) {
	case E_SIQ_STALE_DIRS:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->curr_snap.id);
		asprintf(&error_str, "Unable to clean up stale"
		    " directory entries when switching from copy to sync"
		    " for %s", path);
		break;
	case E_SIQ_LIN_UPDT:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->curr_snap.id);
		asprintf(&error_str, "Unable to update metadata"
		    " (inode changes) information for %s", path);
		break;

	case E_SIQ_LIN_COMMIT:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->curr_snap.id);
		asprintf(&error_str, "Unable to commit metadata"
		    " (inode changes) information for %s", path);
		break;

	case E_SIQ_DEL_LIN:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->prev_snap.id);
		asprintf(&error_str, "Unable to delete %s", path);
		break;

	case E_SIQ_UNLINK_ENT:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->prev_snap.id);
		from_path = get_lin_path(m->body.siq_err.dirlin,
		    ctx->prev_snap.id);
		asprintf(&error_str, "Unable to unlink %s from directory %s",
		    path, from_path);
		break;

	case E_SIQ_LINK_ENT:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->curr_snap.id);
		from_path = get_lin_path(m->body.siq_err.dirlin,
		    ctx->curr_snap.id);
		asprintf(&error_str, "Unable to link %s in directory %s",
		    path, from_path);
		break;

	case E_SIQ_WORK_INIT:
		asprintf(&error_str, "Unable to read and initialize work "
		    " context %llx", m->body.siq_err.filelin);
		break;

	case E_SIQ_WORK_SPLIT:
		asprintf(&error_str, "Unable to split work item %llx to new "
		    " work item %llx", m->body.siq_err.dirlin,
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_PPRED_REC:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->prev_snap.id);
		asprintf(&error_str, "Operation failed while detecting path"
		    " predicate changes for %s", path);
		break;

	case E_SIQ_CC_DEL_LIN:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->prev_snap.id);
		asprintf(&error_str, "Operation failed while trying to detect"
		    " all deleted lins in %s", path);
		break;

	case E_SIQ_CC_DIR_CHG:
		path = get_lin_path(m->body.siq_err.filelin,
		    ctx->prev_snap.id);
		asprintf(&error_str, "Operation failed while trying to detect"
		    " all directory entry changes in %s", path);
		break;

	case E_SIQ_FLIP_LINS:
		asprintf(&error_str, "Operation failed while trying update"
		    " replication database with changed entry %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_IDMAP_SEND:
		asprintf(&error_str, "Operation failed while transferring"
		    " idmap");
		break;

	case E_SIQ_WORK_CLEANUP:
		asprintf(&error_str, "Operation failed while trying cleanup"
		    " worker job slice(work) %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_WORK_SAVE:
		asprintf(&error_str, "Operation failed while trying save"
		    " worker job slice(work) %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_SUMM_TREE:
		asprintf(&error_str, "Operation failed while constructing "
		    "list of lins changed between snapshots %llu and %llu",
		    ctx->prev_snap.id, ctx->curr_snap.id);
		break;

	case E_SIQ_DOMAIN_MARK:
		asprintf(&error_str, "Operation failed while trying domain"
		    " mark on) %llx",
		    m->body.siq_err.filelin);
		break;
        
	case E_SIQ_COMP_COMMIT:
		asprintf(&error_str, "Operation failed while trying to commit"
		    " files");
		break;

	case E_SIQ_CC_COMP_RESYNC:
		asprintf(&error_str, "Operation failed while trying to detect"
		    "compliance resyncs for compliance entry %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_COMP_CONFLICT_CLEANUP:
		asprintf(&error_str, "Operation failed while trying cleanup"
		    " list of conflicting files before snapshot restore %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_COMP_CONFLICT_LIST:
		asprintf(&error_str, "Operation failed while trying populate"
		    " list of conflicting files before snapshot restore %llx",
		    m->body.siq_err.filelin);
		break;

	case E_SIQ_TGT_WORK_LOCK:
		asprintf(&error_str, "Operation failed while trying to grab a "
		    "target work item lock.");
		break;

	default:
		if(m->body.siq_err.errstr == NULL) {
			asprintf(&error_str, "%s", "SyncIQ application error");
		}
	}

	asprintf(return_str, "%s, Local error : %s", error_str, local_error);

	if (local_error)
		free(local_error);

	if (error_str)
		free(error_str);

	if (path)
		free(path);

	if (from_path)
		free(from_path);
}

int
siq_error_callback(struct generic_msg *m, void *ctx)
{
	char *error_str = NULL;
	char *global_error = NULL;
	unsigned int siqerr = m->body.siq_err.siqerr;
	int errloc = m->body.siq_err.errloc;
	struct isi_error *error = NULL;

	get_error_str(m, &g_ctx, &global_error);

	asprintf(&error_str, "Error at %s cluster on node [%s]:  %s ",
	    (errloc == EL_SIQ_SOURCE ? "source" : "target"),
	    m->body.siq_err.nodename, global_error);

	log(ERROR, "%s", global_error);

	if (siq_err_has_attr(siqerr, EA_FS_ERROR)) {
		g_ctx.job_had_error = true;

		/* worker fs error alerts indicate where they happened */
		fatal_error(false, errloc ==
		    EL_SIQ_DEST ? SIQ_ALERT_TARGET_FS : SIQ_ALERT_SOURCE_FS,
		    "%s", error_str);
	} else if (siq_err_has_attr(siqerr, EA_JOB_FAIL)) {
		g_ctx.job_had_error = true;

		/* create alert from msg to pass to fatal error */
		error = isi_siq_error_new(siqerr, "%s", error_str);
		fatal_error_from_isi_error(false, error);
	}

	if (siq_err_has_attr(siqerr, EA_SIQ_REPORT))
		siq_job_error_msg_append(JOB_SUMM, error_str);

	if (global_error)
		free(global_error);

	if (error_str)
		free(error_str);

	isi_error_free(error);

	return 0;
}

static int
worker_pid_callback(struct generic_msg *m, void *ctx)
{
	struct worker *w = (struct worker *)ctx;
	
	w->process_id = m->body.worker_pid.process_id;
	
	return 0;
}


/* __        __         _             
 * \ \      / /__  _ __| | _____ _ __ 
 *  \ \ /\ / / _ \| '__| |/ / _ \ '__|
 *   \ V  V / (_) | |  |   <  __/ |   
 *    \_/\_/ \___/|_|  |_|\_\___|_|   
 */

void
cleanup_node_list(struct nodes *ns)
{
	int i;

	ASSERT(ns);
	if (ns->ips)
		free_ip_list(ns->ips);
	if (ns->nodes) {
		for (i = 0; i < ns->count; i++) {
			if (ns->nodes[i]) {
				free(ns->nodes[i]->host);
				free(ns->nodes[i]);
			}
		}
		free(ns->nodes);
	}
	free(ns);
}

static void
handle_group_change(struct workers *new_workers)
{
	bool found;
	int i, j;
	struct worker *w = NULL;
	int w_survived = 0;

	log(TRACE, "%s", __func__);

	ASSERT(g_ctx.new_src_nodes || g_ctx.new_tgt_nodes);
	ASSERT(g_ctx.src_nodes && g_ctx.tgt_nodes);
	ASSERT(g_ctx.workers);

	/* move 'surviving' source nodes to new list, flag 'dead' ones */
	if (g_ctx.new_src_nodes) {
		/* existing node is dead if its IP isn't in new list */
		for (i = 0; i < g_ctx.src_nodes->count; i++) {
			found = false;

			for (j = 0; j < g_ctx.new_src_nodes->count; j++) {
				if (strcmp(g_ctx.src_nodes->nodes[i]->host,
				    g_ctx.new_src_nodes->nodes[j]->host) == 0) {
					found = true;
					break;
				}
			}

			if (found) {
				/* overwrite entry in new list */
				free(g_ctx.new_src_nodes->nodes[j]->host);
				free(g_ctx.new_src_nodes->nodes[j]);
				g_ctx.new_src_nodes->nodes[j] =
				    g_ctx.src_nodes->nodes[i];
				g_ctx.src_nodes->nodes[i] = NULL;
			} else
				g_ctx.src_nodes->nodes[i]->down = true;
		}
	}

	/* move 'surviving' target nodes to new list, flag 'dead' ones */
	if (g_ctx.new_tgt_nodes) {
		/* existing node is dead if its IP isn't in new list */
		for (i = 0; i < g_ctx.tgt_nodes->count; i++) {
			found = false;

			for (j = 0; j < g_ctx.new_tgt_nodes->count; j++) {
				if (strcmp(g_ctx.tgt_nodes->nodes[i]->host,
				    g_ctx.new_tgt_nodes->nodes[j]->host) == 0) {
					found = true;
					break;
				}
			}

			if (found) {
				/* overwrite entry in new list */
				free(g_ctx.new_tgt_nodes->nodes[j]->host);
				free(g_ctx.new_tgt_nodes->nodes[j]);
				g_ctx.new_tgt_nodes->nodes[j] =
				    g_ctx.tgt_nodes->nodes[i];
				g_ctx.tgt_nodes->nodes[i] = NULL;
			} else
				g_ctx.tgt_nodes->nodes[i]->down = true;
		}
	}

	/* move surviving workers to new list. workers that lost their source
	 * or target node will be go away. if a worker that lost its source or
	 * target node has already disconnected, we clean it up now. otherwise
	 * we send an exit instruction and postpone cleanup until we receive
	 * the disconnect callback */
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];

		if (w->s_node->down || w->t_node->down) {
			log(DEBUG,
			    "Worker %d lost its %s node in group change",
			    w->id, w->s_node->down ? "source" : "target");

			w->s_node->n_workers--;
			w->t_node->n_workers--;

			if (w->socket == -1) {
				/* clean up immediately */
				queue_rem_all(w);
				free(w);
				w = NULL;
			} else {
				/* mark as dying and send exit signal. clean
				 * up on disconnect callback */
				kill_worker(w);
			}
		} else {
			log(DEBUG,
			    "Worker %d (new id %d) survived group change",
			    w->id, new_workers->workers[w_survived]->id);
			w->id = new_workers->workers[w_survived]->id;
			free(new_workers->workers[w_survived]);
			new_workers->workers[w_survived] = w;
			w_survived++;
		}
	}
	new_workers->connected = g_ctx.workers->connected;
	free(g_ctx.workers->workers);
	free(g_ctx.workers);
	g_ctx.workers = NULL;

	if (g_ctx.new_src_nodes) {
		/* on source gc cleanup stale source node list and 'dead' 
		 * nodes */
		cleanup_node_list(g_ctx.src_nodes);
		g_ctx.src_nodes = NULL;
	}

	if (g_ctx.new_tgt_nodes) {
		/* cleanup old target node list and 'dead' nodes */
		cleanup_node_list(g_ctx.tgt_nodes);
		g_ctx.tgt_nodes = NULL;
	}
}

static void/////所谓配置worker:将这个worker(pworker)运行在哪个node上(host:ip)
configure_worker_node_mask(struct worker *w, char *node_mask)
{
	int i;
	int s = -1;
	int t = -1;
	int off;
	int n_src_nodes = g_ctx.src_nodes->count;
	int n_tgt_nodes = g_ctx.tgt_nodes->count;
	bool found_snode = false;
	bool found_tnode = false;

	log(TRACE, "%s worker id: %d n_src_nodes: "
	    "%d n_tgt_nodes: %d", __func__, w->id, n_src_nodes, n_tgt_nodes);

	if (w->configured)
		goto out;

	/* If worker pools, and we haven't received allowed nodes from the
	 * worker daemon, don't configure workers */
	if (!g_ctx.rc.disable_work && !node_mask)
		goto out;

	/* find an available source node for the worker */
	off = random() % n_src_nodes;
	for (i = off; i < off + n_src_nodes; i++) {
		s = i % n_src_nodes;
		log(TRACE, "src - s: %d i: %d n_workers: %d "
		    "node_worker_limit: %d", s, i,
		    g_ctx.src_nodes->nodes[s]->n_workers,
		    g_ctx.src_nodes->node_worker_limit);
		log(DEBUG, "%s: disable_work: %d, lnn: %d "
		    "lnn_list_val: %c", __func__,
		    g_ctx.rc.disable_work,
		    g_ctx.src_nodes->nodes[s]->lnn,
		    node_mask ? node_mask[g_ctx.src_nodes->nodes[s]->lnn - 1] :
		    '\0');
		//If worker pools, we don't care about node_worker_limit
		if (!g_ctx.rc.disable_work) {
			if (g_ctx.src_nodes->nodes[s]->lnn > 0 &&
			    node_mask[g_ctx.src_nodes->nodes[s]->lnn - 1] != 'F') {
				found_snode = true;
				break;
			}
		} else {
			if (g_ctx.src_nodes->nodes[s]->n_workers <
			    g_ctx.src_nodes->node_worker_limit) {
				found_snode = true;
				break;
			}
		}
	}

	/* find an available target node for the worker */
	off = random() % n_tgt_nodes;
	for (i = off; i < off + n_tgt_nodes; i++) {
		t = i % n_tgt_nodes;
		log(TRACE, "tgt - t: %d i: %d n_workers: %d "
		    "node_worker_limit: %d", t, i,
		    g_ctx.tgt_nodes->nodes[t]->n_workers,
		    g_ctx.tgt_nodes->node_worker_limit);
		if (g_ctx.tgt_nodes->nodes[t]->n_workers <
		    g_ctx.tgt_nodes->node_worker_limit) {
			found_tnode = true;
			break;
		}
	}

	ASSERT(found_snode);
	ASSERT(found_tnode);
	ASSERT(s != -1);
	ASSERT(t != -1);

	w->s_node = g_ctx.src_nodes->nodes[s];  ////这个worker部署在g_ctx.src_nodes->nodes[s]这台机器上,它的ip是: g_ctx.src_nodes->nodes[s]->host
	w->s_node->n_workers++;
	w->t_node = g_ctx.tgt_nodes->nodes[t];
	w->t_node->n_workers++;

	log(DEBUG, "%s: worker %d src node %d (%d) tgt node %d (%d)",
	    __func__, w->id, w->s_node->lnn, w->s_node->n_workers,
	    w->t_node->lnn, w->t_node->n_workers);

	/* new workers go on the offline queue and don't leave until they
	 * ask for work */
	if (!queue_contains(&offline_queue, w)) {
		queue_rem_all(w);
		queue_enq(&offline_queue, w);
	}

	w->configured = true;

out:
	return;
}

void
configure_worker(struct worker *w)
{
	/* Use default_node_mask here since we are trying to connect to
	 * a worker (or already have). Since the bandwidth daemon sets
	 * g_ctx.work_node_mask to all .'s when creating workers, ensure
	 * that we use a valid mask when configuring a new worker while
	 * still decreasing our ration. This can occur when a worker dies
	 * due to disconnect while we are decreasing our ration in the           ///////coord降低ration,会导致worker die(disconnect)
	 * coordinator. */
	configure_worker_node_mask(w, default_node_mask);
}

static bool
is_worker_disconnected(struct worker *w)
{
	return w->configured && w->socket == -1 && !w->dying &&
	    queue_contains(&offline_queue, w) && w->shuffle;
}

void
shuffle_offline_workers(void)
{
	int i;
	int index;
	int node_mask_size = SIQ_MAX_NODES + 1;
	struct worker *w = NULL;
	bool configured_worker = false;

	if (g_ctx.rc.disable_work || g_ctx.init_node_mask == NULL)
		return;

	if (g_ctx.disconnected_node_mask == NULL) {
		g_ctx.disconnected_node_mask = malloc(node_mask_size);
		ASSERT(g_ctx.disconnected_node_mask);
		ASSERT(node_mask_size > strlen(g_ctx.init_node_mask));
		strncpy(g_ctx.disconnected_node_mask, g_ctx.init_node_mask,
		    node_mask_size);
	}

	//Now mark the disconnected nodes
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		index = w->s_node->lnn - 1;
		if (is_worker_disconnected(w)) {
			g_ctx.disconnected_node_mask[index] = 'F';
		}
	}

	//If all of the entries in g_ctx.disconnected_node_mask are 'F',
	//then use the init_node_mask so that we try a connection on the
	//nodes allowed by the subnet pools/bandwidth configuration. We use
	//init_node_mask over g_ctx.work_node_mask since there could be all
	//F's for the nodes in the cluster if the bandwidth daemon recently
	//sent a message to decrease worker ration.
	i = 0;
	while (g_ctx.disconnected_node_mask[i] == 'F')
		i++;
	if (g_ctx.disconnected_node_mask[i] == '\0') {
		ASSERT(node_mask_size > strlen(g_ctx.init_node_mask));
		strncpy(g_ctx.disconnected_node_mask,
		    g_ctx.init_node_mask, node_mask_size);
	}

	//Change source nodes for all disconnected workers to something
	//different and not on the above list
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		if (is_worker_disconnected(w)) {
			configured_worker = true;
			w->s_node->n_workers--;
			w->t_node->n_workers--;
			w->configured = false;
			configure_worker_node_mask(w,
			    g_ctx.disconnected_node_mask);
		}
	}

	//If we didn't need to shuffle any worker, then reset
	//g_ctx.disconnected_node_mask
	if (!configured_worker && memcmp(g_ctx.init_node_mask,
	    g_ctx.disconnected_node_mask, node_mask_size) != 0) {
		ASSERT(node_mask_size > strlen(g_ctx.init_node_mask));
		strncpy(g_ctx.disconnected_node_mask, g_ctx.init_node_mask,
		    node_mask_size);
	}
}

static void
calculate_worker_limits(struct workers *new_workers,
    int n_src_nodes, int n_tgt_nodes)
{
	/* per node worker limit = ceiling(new_workers->count / n_nodes)
	 * Note: with worker pools, it is possible and acceptable to have
	 * 0 workers. This will be the case before we've received a ration
	 * from the worker daemon. */

	g_ctx.src_nodes->node_worker_limit =
	    (new_workers->count + n_src_nodes - 1) / n_src_nodes;
	g_ctx.tgt_nodes->node_worker_limit =
	    (new_workers->count + n_tgt_nodes - 1) / n_tgt_nodes;
}

/* On initial configuration, new_src_nodes and new_tgt_nodes will point to
 * structures containing IP lists (no node array), src_nodes and tgt_nodes
 * will be NULL.
 *
 * For reconfiguration (following source or target group change), src_nodes
 * and tgt_nodes will contain the current configuration, and either
 * new_src_nodes or new_tgt_nodes (not both) will exist and will contain the
 * up to date IP list for the cluster that had the group change */
void
configure_workers(bool initial) /////把 new_src_nodes信息 赋值给src_nodes
{
	int i;
	int n_src_nodes = 0;
	int n_tgt_nodes = 0;
	struct worker *w = NULL;
	struct workers *new_workers = NULL;

	if (initial) {
		ASSERT(g_ctx.new_src_nodes && g_ctx.new_tgt_nodes);
		ASSERT(!g_ctx.src_nodes && !g_ctx.tgt_nodes && !g_ctx.workers);
		ilog(IL_INFO, "%s: initial", __func__);
		n_src_nodes = g_ctx.new_src_nodes->count;
		n_tgt_nodes = g_ctx.new_tgt_nodes->count;
	} else {
		ASSERT(g_ctx.workers_configured);
		ASSERT(g_ctx.workers);
		ASSERT(g_ctx.src_nodes && g_ctx.tgt_nodes);
		ASSERT(g_ctx.new_src_nodes || g_ctx.new_tgt_nodes);
		ASSERT(!g_ctx.new_src_nodes || !g_ctx.new_tgt_nodes);
		
		if (g_ctx.new_src_nodes) {
			ASSERT(!g_ctx.new_tgt_nodes);
			ilog(IL_INFO "%s: source group change", __func__);
			n_src_nodes = g_ctx.new_src_nodes->count;
			n_tgt_nodes = g_ctx.tgt_nodes->count;
		}
		if (g_ctx.new_tgt_nodes) {
			ASSERT(!g_ctx.new_src_nodes);
			ilog(IL_INFO, "%s: target group change", __func__);
			n_src_nodes = g_ctx.src_nodes->count;
			n_tgt_nodes = g_ctx.new_tgt_nodes->count;
		}
	}
	ASSERT(n_src_nodes > 0);
	ASSERT(n_tgt_nodes > 0);
	
	new_workers = calloc(1, sizeof(struct workers));
	ASSERT(new_workers);
	if (initial && !g_ctx.rc.disable_work) {
		//Worker pool enabled
		//Wait for a ration from the worker daemon to start
		//configuring workers
		new_workers->count = 0;
		g_ctx.work_fast_start_count = 1;
		g_ctx.work_fast_kill_count = 1;
	} else if (g_ctx.rc.disable_work) {
		//Worker pool disabled, do old behavior
		new_workers->count = (n_src_nodes > n_tgt_nodes) ?
		    n_tgt_nodes * g_ctx.rc.wperhost :
		    n_src_nodes * g_ctx.rc.wperhost;
	} else {
		//Leave the worker count alone
		new_workers->count = g_ctx.workers->count;
	}

	/* kind of takes the fun out of dynamic worker allocation since this
	 * limit isn't dynamically configurable.
	 * Leave the count alone if worker pools is taking care of it. */
	if (new_workers->count > g_ctx.rc.max_wperpolicy &&
	    g_ctx.rc.disable_work)
		new_workers->count = g_ctx.rc.max_wperpolicy;
	ASSERT(new_workers->count >= 0);

	ilog(IL_INFO, "%s: workers %d src nodes %d tgt nodes %d",
	    __func__, new_workers->count, n_src_nodes, n_tgt_nodes);

	/* allocate and initialize new worker list */
	if (new_workers->count > 0) {
		new_workers->workers =
		    calloc(new_workers->count, sizeof(struct worker *));
		ASSERT(new_workers->workers);
	}

	for (i = 0; i < new_workers->count; i++) {
		w = new_workers->workers[i] = calloc(1, sizeof(struct worker));
		ASSERT(w);
		w->socket = -1;
		w->id = i;
		w->last_work = time(0);
	}

	/* allocate and initialize new source node list */
	if (g_ctx.new_src_nodes) {
		g_ctx.new_src_nodes->nodes =
		    calloc(n_src_nodes, sizeof(struct node *));
		ASSERT(g_ctx.new_src_nodes->nodes);
		for (i = 0; i < n_src_nodes; i++) {
			g_ctx.new_src_nodes->nodes[i] =
			    calloc(1, sizeof(struct node));
			ASSERT(g_ctx.new_src_nodes->nodes[i]);
			g_ctx.new_src_nodes->nodes[i]->host =
			    strdup(g_ctx.new_src_nodes->ips[i]);
			ASSERT(g_ctx.new_src_nodes->nodes[i]->host);
			if (g_ctx.work_lnn_list) {
				g_ctx.new_src_nodes->nodes[i]->lnn =
				    g_ctx.work_lnn_list[i];
			}
		}
	}

	/* allocate and initialize new target node list */
	if (g_ctx.new_tgt_nodes) {
		g_ctx.new_tgt_nodes->nodes =
		    calloc(n_tgt_nodes, sizeof(struct node *));
		ASSERT(g_ctx.new_tgt_nodes->nodes);
		for (i = 0; i < n_tgt_nodes; i++) {
			g_ctx.new_tgt_nodes->nodes[i] =
			    calloc(1, sizeof(struct node));
			ASSERT(g_ctx.new_tgt_nodes->nodes[i]);
			g_ctx.new_tgt_nodes->nodes[i]->host =
			    strdup(g_ctx.new_tgt_nodes->ips[i]);
			ASSERT(g_ctx.new_tgt_nodes->nodes[i]);
		}
	}

	if (initial == false) {
		/* identify source or target nodes that left the groups. flag
		 * workers that lost their source or target nodes and move them
		 * to a temporary list where they will be allowed to run until
		 * they disconnect */
		handle_group_change(new_workers);

		/* we now have complete and up to date source and target node
		 * lists. we also have a new list of workers, some of which
		 * are unconfigured */
	}

	/* move new lists into place */
	g_ctx.workers = new_workers;
	if (g_ctx.new_src_nodes) {
		ASSERT(!g_ctx.src_nodes);
		g_ctx.src_nodes = g_ctx.new_src_nodes;   /////src_nodes[i]->host 每个node的ip
		g_ctx.new_src_nodes = NULL;
	}
	if (g_ctx.new_tgt_nodes) {
		ASSERT(!g_ctx.tgt_nodes);
		g_ctx.tgt_nodes = g_ctx.new_tgt_nodes;
		g_ctx.new_tgt_nodes = NULL;
	}

	calculate_worker_limits(new_workers, n_src_nodes, n_tgt_nodes);

	ilog(IL_INFO, "%s: per node worker limits: src %d tgt %d",
	    __func__, g_ctx.src_nodes->node_worker_limit,
	    g_ctx.tgt_nodes->node_worker_limit);

	/* find available source and target nodes for each worker */ //把worker部署在src_node(有ip的node)上,连接这些src_nodes上的pworker daemon,fork pworker
	for (i = 0; i < new_workers->count; i++)
		configure_worker(g_ctx.workers->workers[i]);

	/* newly configured workers will be connected next timer_callback */
	register_timer_callback(initial ? 0 : TIMER_DELAY);/////connect_workers

	if (initial)
		g_ctx.workers_configured = true;
}

static void
kill_worker_helper(struct worker *w, char *type)
{
	log(TRACE, "killing %s worker %d", type, w->id);
	if (w->s_node) {
		w->s_node->n_workers--;
	}
	if (w->t_node) {
		w->t_node->n_workers--;
	}
	kill_worker(w);
}

int
redistribute_workers(void *ctx)
{
	int i;
	int n_src_nodes = 0;
	int n_tgt_nodes = 0;
	int workers_to_copy = 0;
	int new_workers_pointer = 0;
	int min_split_window = 10;
	struct worker *w = NULL;
	struct workers *new_workers = NULL;
	time_t now = time(0);
	struct timeval tv = {10, 0};
	bool changed = false;
	int effective_ration = 0;

	ASSERT(g_ctx.workers_configured);
	ASSERT(g_ctx.workers);
	ASSERT(g_ctx.src_nodes && g_ctx.tgt_nodes);
	n_src_nodes = g_ctx.src_nodes->count;
	n_tgt_nodes = g_ctx.tgt_nodes->count;
	ASSERT(n_src_nodes > 0);
	ASSERT(n_tgt_nodes > 0);

	new_workers = calloc(1, sizeof(struct workers));
	ASSERT(new_workers);

	log(DEBUG, "%s: workers %d src nodes %d tgt nodes %d "
	    "work_fast_start_count %d work_fast_kill_count %d "
	    "work_last_worker_inc %lld ration %d",
	    __func__, g_ctx.workers->count, n_src_nodes, n_tgt_nodes,
	    g_ctx.work_fast_start_count, g_ctx.work_fast_kill_count,
	    g_ctx.work_last_worker_inc, g_ctx.wk_ration);

	UFAIL_POINT_CODE(fast_worker_split, {
	    min_split_window = 1;
	    tv.tv_sec = 1;
	    TIMER_DELAY = 1;
	});

	if (g_ctx.wk_ration > 0) {
		//Increase or decrease workers every 10s
		//exponentially until ration is met
		//OR when we first change between increasing and decreasing
		//workers
		if (g_ctx.rc.force_wpn) {
			//If we are forcing workers per node, then take the min
			//of the ration and wpn
			g_ctx.total_wpn = n_src_nodes * g_ctx.rc.wperhost;
			effective_ration = g_ctx.total_wpn < g_ctx.wk_ration ?
			    g_ctx.total_wpn : g_ctx.wk_ration;
		} else {
			effective_ration = g_ctx.wk_ration;
		}
		if (g_ctx.workers->count > effective_ration) { ////需要减少workers
			if ((g_ctx.work_fast_kill_count == 1) ||
			    (now - g_ctx.work_last_worker_inc >=
			    min_split_window)) {
				//Decrease workers
				new_workers->count = g_ctx.workers->count -
				    g_ctx.work_fast_kill_count;
				new_workers->count =
				    MAX(new_workers->count,
				    effective_ration);
				log(DEBUG, "%s: decrementing worker count to "
				    "%d", __func__, new_workers->count);
				g_ctx.work_fast_start_count = 1;
				g_ctx.work_fast_kill_count =
				    g_ctx.work_fast_kill_count * 2;

				changed = true;
				g_ctx.work_last_worker_inc = now;
			}
		} else if (g_ctx.workers->count < effective_ration) {/////需要增加workers
			if ((g_ctx.work_fast_start_count == 1) ||
			    (now - g_ctx.work_last_worker_inc >=
			    min_split_window)) {
				//Increase workers
				new_workers->count = g_ctx.workers->count +
				    g_ctx.work_fast_start_count;
				new_workers->count =
				    MIN(new_workers->count,
				    effective_ration);
				log(DEBUG, "%s: incrementing worker count to "
				    "%d", __func__, new_workers->count);
				g_ctx.work_fast_start_count =
				    g_ctx.work_fast_start_count * 2;
				g_ctx.work_fast_kill_count = 1;

				changed = true;
				g_ctx.work_last_worker_inc = now;
			}
		} else {
			//At ration, so reset limits
			g_ctx.work_fast_start_count = 1;
			g_ctx.work_fast_kill_count = 1;
		}
	}

	if (!changed) {
		free(new_workers);
		new_workers = NULL;
		goto out;
	}

	/* allocate and initialize new worker list */
	ASSERT(new_workers->count >= 0, "Count == %d", new_workers->count);
	new_workers->workers =
	    calloc(new_workers->count, sizeof(struct worker *));
	ASSERT(new_workers->workers);
	for (i = 0; i < new_workers->count; i++) {
		w = new_workers->workers[i] = calloc(1, sizeof(struct worker));
		ASSERT(w);
		w->socket = -1;
		w->id = i;
		w->last_work = time(0);
	}

	workers_to_copy = g_ctx.workers->count;
	log(TRACE, "workers_to_copy before: %d", workers_to_copy);
	//If we need to kill workers, it is because our ration got reduced
	if (new_workers->count < g_ctx.workers->count) {

		UFAIL_POINT_CODE(kill_split_pending_workers,
			while (workers_to_copy > new_workers->count &&
			    queue_size(&split_pending_queue) > 0)  {
				queue_deq(&split_pending_queue, &w);
				kill_worker_helper(w, "split_pending");
				//Iterate
				workers_to_copy--;
			});

		//First look for workers that are being killed.
		for (i = 0; i < g_ctx.workers->count; i++) {
			w = g_ctx.workers->workers[i];
			if (w->dying)
				workers_to_copy--;
		}

		//Then look for workers that are offline
		while (workers_to_copy > new_workers->count &&
		    queue_size(&offline_queue) > 0)  {
			queue_deq(&offline_queue, &w);
			kill_worker_helper(w, "offline");
			//Iterate
			workers_to_copy--;
		}
		//Next look for workers that are idle
		while (workers_to_copy > new_workers->count &&
		    queue_size(&idle_queue) > 0)  {
			queue_deq(&idle_queue, &w);
			kill_worker_helper(w, "idle");
			//Iterate
			workers_to_copy--;
		}
		//Kill the workers that are masked out first
		if (g_ctx.work_node_mask) {
			for (i = 0; i < g_ctx.workers->count; i++) {
				if (workers_to_copy <= new_workers->count) {
					break;
				}
				w = g_ctx.workers->workers[i];
				if (!w->dying &&
				    g_ctx.work_node_mask[w->s_node->lnn - 1]
				    == 'F') {
					//Kill them, mark as dying
					kill_worker_helper(w, "masked");
					//Iterate
					workers_to_copy--;
				}
			}
		}
		//If we still need more workers to kill,
		//then just pick the last workers in the array
		i = g_ctx.workers->count;
		while (workers_to_copy > new_workers->count && i >= 0) {
			i--;
			//Find them, skip dying
			w = g_ctx.workers->workers[i];
			if (!w->dying) {
				//Kill them, mark as dying
				kill_worker_helper(w, "working");
				//Iterate
				workers_to_copy--;
			}
		}
	}
	log(TRACE, "workers_to_copy after: %d", workers_to_copy);

	ASSERT(new_workers->count >= workers_to_copy);

	calculate_worker_limits(new_workers, n_src_nodes, n_tgt_nodes);

	//Copy over existing workers that are not dying
	for (i = 0; i < g_ctx.workers->count; i++) {
		w = g_ctx.workers->workers[i];
		UFAIL_POINT_CODE(kill_first_worker, {
		    if (i == 0)
			    kill_worker_helper(w, "ufp: kill_first_worker");
		});

		if (!w->dying) {
			w->id = new_workers->workers[new_workers_pointer]->id;
			free(new_workers->workers[new_workers_pointer]);
			new_workers->workers[new_workers_pointer] = w;
			new_workers_pointer++;
			configure_worker(w);
		}
	}
	//Create new workers, if necessary
	for (i = new_workers_pointer; i < new_workers->count; i++) {
		configure_worker(new_workers->workers[i]);
	}

	/* move new lists into place */
	new_workers->connected = g_ctx.workers->connected;
	free(g_ctx.workers->workers);
	free(g_ctx.workers);
	g_ctx.workers = new_workers;

out:
	/* newly configured workers will be connected next timer_callback */
	register_timer_callback(0);///////connect_worker

	/* Cancel the timeout if it exists and restart the worker
	 * incrementation window from right now. This function is called
	 * multiple times and we don't want multiple recurring timeouts. */
	if (workers_tout != NULL)
		migr_cancel_timeout(workers_tout);
	workers_tout = migr_register_timeout(&tv, redistribute_workers, 0);

	return 0;
}

int
connect_worker(struct worker *w)  /////发送work_init_msg,调用pworker daemon去fork pworker
{
	struct isi_error *error = NULL;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;
	time_t now = time(0);
	int res = -1;
	uint8_t *common_cbm_vers_buf = NULL;
	size_t common_cbm_vers_buf_len = 64;

	if (w->connect_failures > NUM_WAIT)
	    w->connect_failures = NUM_WAIT;

	if (w->connect_failures && now - w->last_connect <
	    CONNECT_WAIT[w->connect_failures - 1]) {
		log(DEBUG, "Delaying connect at %d failures to %d seconds",
		    w->connect_failures, CONNECT_WAIT[w->connect_failures - 1]);
		goto out;
	}

	w->last_connect = now;

	if (g_ctx.rc.noop ||
	    g_ctx.workers->connected == g_ctx.workers->count) {
		res = 0;
		goto out;
	}

	/* Check if connect alert should be raised */
	if (g_ctx.workers->connected == 0 && g_ctx.conn_alert &&
	    time(0) > g_ctx.conn_alert + 15 * 60) {
		/* Turn conn alert off for now */
		g_ctx.conn_alert = 0;
		fatal_error(false, SIQ_ALERT_TARGET_CONNECTIVITY,
		    "No live connections for over 15 minutes");
	}

	w->socket = connect_to(w->s_node->host,
	    global_config->root->coordinator->ports->pworker,
	    POL_DENY_FALLBACK, g_ctx.job_ver.local, NULL, NULL, false, &error);
	if (w->socket == -1 || error != NULL) {
		if (g_ctx.rc.disable_work)
			w->connect_failures++;
		w->shuffle = true;
		log(ERROR, "Failed to connect to worker %d (%s): %s",
		    w->id, w->s_node->host,
		    error ? isi_error_get_message(error) : "<unknown>");
		isi_error_free(error);
		error = NULL;
		goto out;
	}

	migr_add_fd(w->socket, w, "pworker");
	if (g_ctx.stf_sync &&
	    !(g_ctx.initial_sync || g_ctx.stf_upgrade_sync)) {
		migr_register_callback(w->socket, OLD_PWORKER_STF_STAT_MSG,
		    pworker_stf_stat_callback);
		migr_register_callback(w->socket,
		    build_ript_msg_type(PWORKER_STF_STAT_MSG),
		    pworker_stf_stat_callback);
		migr_register_callback(w->socket, OLD_SWORKER_STF_STAT_MSG,
		    sworker_stf_stat_callback);
		migr_register_callback(w->socket,
		    build_ript_msg_type(SWORKER_STF_STAT_MSG),
		    sworker_stf_stat_callback);
	} else {
		migr_register_callback(w->socket, OLD_PWORKER_TW_STAT_MSG,
		    pworker_tw_stat_callback);
		migr_register_callback(w->socket,
		    build_ript_msg_type(PWORKER_TW_STAT_MSG),
		    pworker_tw_stat_callback);
		migr_register_callback(w->socket, OLD_SWORKER_TW_STAT_MSG,
		    sworker_tw_stat_callback);
		migr_register_callback(w->socket,
		    build_ript_msg_type(SWORKER_TW_STAT_MSG),
		    sworker_tw_stat_callback);
	}

	migr_register_callback(w->socket, DISCON_MSG, disconnect_callback);
	migr_register_callback(w->socket, WORK_REQ_MSG, work_req_callback);
	migr_register_callback(w->socket, LIST_MSG, list_callback);
	migr_register_callback(w->socket, ERROR_MSG, error_callback);
	migr_register_callback(w->socket, SIQ_ERROR_MSG, siq_error_callback);
	migr_register_callback(w->socket, POSITION_MSG, position_callback);
	migr_register_callback(w->socket, SPLIT_RESP_MSG, split_resp_callback);
	migr_register_callback(w->socket, WORKER_PID_MSG, worker_pid_callback);
	migr_register_callback(w->socket, GENERAL_SIGNAL_MSG, 
	    general_signal_callback);

	g_ctx.workers->connected++;
	JOB_SUMM->chunks->running++;

	msg.head.type = build_ript_msg_type(WORK_INIT_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);

	ript_msg_set_field_str(ript, RMF_TARGET, 1,
	    (g_ctx.rc.flags & FLAG_MKLINKS) ? g_ctx.rc.link_target :
	    g_ctx.rc.target);
	ript_msg_set_field_str(ript, RMF_PEER, 1, w->t_node->host);

	ript_msg_set_field_str(ript, RMF_SOURCE_BASE, 1,
	    g_ctx.source_base.path);
	ript_msg_set_field_str(ript, RMF_TARGET_BASE, 1,
	    g_ctx.target_base.path);

	ript_msg_set_field_uint64(ript, RMF_CUR_SNAPID, 1, g_ctx.curr_snap.id);
	ript_msg_set_field_uint64(ript, RMF_PREV_SNAPID, 1,
	    g_ctx.prev_snap.id);

	ilog(IL_INFO, "%s called, previous snapid: %llu current snapid: %llu", __func__,
	    g_ctx.prev_snap.id, g_ctx.curr_snap.id);

	ript_msg_set_field_uint32(ript, RMF_OLD_TIME, 1, g_ctx.old_time);
	ript_msg_set_field_uint32(ript, RMF_NEW_TIME, 1, g_ctx.new_time);

	ript_msg_set_field_str(ript, RMF_BWHOST, 1, g_ctx.rc.bwhost);
	ript_msg_set_field_str(ript, RMF_PREDICATE, 1, g_ctx.rc.predicate);
	ript_msg_set_field_uint32(ript, RMF_LASTRUN_TIME, 1, g_ctx.last_time);
	ript_msg_set_field_str(ript, RMF_PASS, 1, g_ctx.rc.passwd);
	ript_msg_set_field_uint32(ript, RMF_FLAGS, 1, g_ctx.rc.flags);
	ript_msg_set_field_str(ript, RMF_JOB_NAME, 1, SIQ_JNAME(&g_ctx.rc));
	ript_msg_set_field_uint32(ript, RMF_LOGLEVEL, 1, g_ctx.rc.loglevel);
	ript_msg_set_field_uint32(ript, RMF_ACTION, 1, g_ctx.rc.action);
	ript_msg_set_field_str(ript, RMF_SYNC_ID, 1, g_ctx.rc.job_id);
	ript_msg_set_field_uint64(ript, RMF_RUN_ID, 1, g_ctx.run_id);
	ript_msg_set_field_uint64(ript, RMF_RUN_NUMBER, 1, JOB_SUMM->job_id);

	/* For coord->pworker communication anything but HASH_NONE
	 * indicates diff sync. The pworker doesn't use the value in
	 * hash_type as the hash type to use is determined by the
	 * pworker */
	ript_msg_set_field_uint32(ript, RMF_HASH_TYPE, 1,
	    g_ctx.rc.doing_diff_sync ? HASH_MD4 : HASH_NONE);

	ript_msg_set_field_uint32(ript, RMF_SKIP_BB_HASH, 1,
	    g_ctx.rc.skip_bb_hash);
	ript_msg_set_field_uint32(ript, RMF_WORKER_ID, 1, w->id);

	/* Set Domain ID/Generation */
	ript_msg_set_field_uint64(ript, RMF_DOMAIN_ID, 1, g_ctx.rc.domain_id);
	ript_msg_set_field_uint32(ript, RMF_DOMAIN_GENERATION, 1,
	    g_ctx.rc.domain_generation);

	/* Giving restrict_by to the pworker implies that
	 * force_interface is being used */
	if (g_ctx.restrict_by && g_ctx.forced_addr)
		ript_msg_set_field_str(ript, RMF_RESTRICT_BY, 1,
		    g_ctx.restrict_by);

	/* Cloudpools common version information */
	common_cbm_vers_buf = malloc(common_cbm_vers_buf_len);
	common_cbm_vers_buf_len = isi_cbm_file_versions_serialize(
	    g_ctx.common_cbm_file_vers, (void *)common_cbm_vers_buf,
	    common_cbm_vers_buf_len, &error);
	if (error != NULL) {
		if (isi_cbm_error_is_a(error, CBM_LIMIT_EXCEEDED)) {
			isi_error_free(error);
			error = NULL;

			common_cbm_vers_buf = realloc(common_cbm_vers_buf,
			    common_cbm_vers_buf_len);
			common_cbm_vers_buf_len =
			    isi_cbm_file_versions_serialize(
			    g_ctx.common_cbm_file_vers,
			    common_cbm_vers_buf,
			    common_cbm_vers_buf_len, &error);
			if (error != NULL)
				goto out;
		} else {
			goto out;
		}
	}
	ript_msg_set_field_bytestream(ript, RMF_CBM_FILE_VERSIONS, 1,
	    common_cbm_vers_buf, common_cbm_vers_buf_len);

	ript_msg_set_field_uint32(ript, RMF_DEEP_COPY, 1,
	    g_ctx.rc.deep_copy);
	ript_msg_set_field_uint8(ript, RMF_TARGET_WORK_LOCKING_PATCH, 1,
	    g_ctx.rc.target_work_locking_patch);
	ript_msg_set_field_uint64(ript, RMF_WI_LOCK_MOD, 1,
	    g_ctx.rc.target_wi_lock_mod);

	msg_send(w->socket, &msg);  ////发送work_init_msg 给pworker

	/* set bandwidth/throttle last sent values with invalid number to force
	 * resending of rations on next reallocate */
	w->bw_last_sent = -2;
	w->th_last_sent = -2;

	w->reset = false;
	w->shuffle = false;

out:
	free(common_cbm_vers_buf);
	if (error != NULL) {
		if (gc_job_summ && gc_job_summ->root &&
		    gc_job_summ->root->summary)
			gc_job_summ->root->summary->retry = false;
		log(FATAL, "%s", isi_error_get_message(error));
	}

	return res;
}

static void
check_for_split_work(struct coord_ctx *ctx, struct worker *w)
{
	bool exists;
	struct work_item *new_item;
	struct isi_error *error = NULL;
	struct work_item *work = w->work;

	/* If split_wi_lin is non-zero, it means we asked the worker to split
	 * but have yet to receive a split response. Let's check to see if it
	 * succeeded in splitting before dying. */
	if (work->split_wi_lin > 0) {

		exists = entry_exists(work->split_wi_lin, ctx->rep,
		    &error);
		if (error)
			fatal_error_from_isi_error(false, error);

		if (exists) {
			/*
			 * It means worker had split and died after that.
			 * So we need to create new work item and add it to
			 * restart queue.
			 */
			new_item = work_item_new(work->split_wi_lin, false,
			    "Split work from %p", w);
			work_item_insert(restarts_queue, new_item);
			log(TRACE, "Added new work item %llx to restart queue",
			    work->split_wi_lin);
		}
	}
	work->split_wi_lin = 0; 
}

static void
reset_worker(struct worker *w)
{
	bool exists = false;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	/* don't accidentally do this twice on gc/disconnect corner cases */
	if (w->reset)
		goto out;

	w->reset = true;
	w->process_id = 0;
	w->last_work = time(0);

	if (w->socket != -1) {
		log(TRACE, "reset_worker: close worker's socket");
		migr_rm_fd(w->socket);
		close(w->socket);
		w->socket = -1;
		g_ctx.workers->connected--;
	}

	/* move the worker's work to the restarts queue for later processing */
	if (w->work) {
		ASSERT(w->work->wi_lin > 0);
		validate_work_lin(w->work->wi_lin);
		check_for_split_work(&g_ctx, w);
		/* Check if the work item still exists */
		exists = entry_exists(w->work->wi_lin, g_ctx.rep, &error);
		if (error)
			fatal_error_from_isi_error(false, error);

		if (exists) {
			work_item_insert(restarts_queue, w->work);
		} else {
			work_item_clean(w->work);
			free(w->work);
		}
		w->work = NULL;
		/* only update the live time if the worker was busy.
		 * this helps detect workers who die immediately */
		g_ctx.conn_alert = time(0);
	}

	/* put the worker on the offline queue */
	if (!w->dying) {
		queue_rem_all(w);
		queue_enq(&offline_queue, w);
	}

	if (gc_job_summ && JOB_SUMM->chunks->running > 0)
		JOB_SUMM->chunks->running--;
	else
		log(DEBUG, "Inconsistent state of running chunks");

	log(TRACE, "reset_worker: exit with %d connections",
	    g_ctx.workers->connected);

out:
	return;
}

static int
error_callback(struct generic_msg *m, void *ctx)
{
	struct worker *w = ctx;
	char *error_str = NULL;

	g_ctx.job_had_error = true;
	if (m->body.error.io) {
		g_ctx.stats.errors++;
		asprintf(&error_str, "%s: %s", m->body.error.str,
		    strerror(m->body.error.error));
		log(ERROR, "File system error from %sworker %d: %s",
		    w->dying ? "dying " : "", w->id, error_str);
		siq_job_error_msg_append(JOB_SUMM, error_str);
		JOB_SUMM->chunks->running = 0;
		fatal_error(false, SIQ_ALERT_FS,
		    "Failure due to file system error(s): %s", error_str);
	}

	JOB_SUMM->chunks->running = 0;

	switch (m->body.error.error) {
	case ENOTSUP:
		asprintf(&error_str,
		    "Target unconfigured, unlicensed, or off");
		break;
	case EAUTH:
		asprintf(&error_str, "Authentication with target failed");
		break;
	case EPROTOTYPE:
		asprintf(&error_str, "%s",
		    m->body.error.str ? m->body.error.str :
		    "Protocol version mismatch");
		break;
	case ENOTDIR:
		asprintf(&error_str, "Target directory is not available: %s",
		    m->body.error.str ? m->body.error.str : "Unknown error");
		break;
	default:
		asprintf(&error_str, "Unexpected non-I/O error");
		break;
	}

	siq_job_error_msg_append(JOB_SUMM, error_str);
	fatal_error(false, SIQ_ALERT_POLICY_FAIL, error_str);
	free(error_str);

	return 0;
}

void
register_redistribute_work(void)
{
	/* register redistribute timeout */
	redist_tout = migr_register_timeout(&redist_timeout,
	    redistribute_work_callback, 0);

}

void
kill_worker(struct worker *w)
{
	ASSERT(w != NULL);

	if (w->dying) {
		return;
	}

	/**
	 * Bug 179841
	 * Don't count the next restart of this work item.
	 */
	if (w->work)
		w->work->count_retry = false;
	w->dying = true;
	w->id = -1;
	w->s_node = w->t_node = NULL;
	queue_rem_all(w);
	queue_enq(&dying_queue, w);

	if (w->socket < 0) {
		/* clean up immediately */
		queue_rem_all(w);
		free(w);
		w = NULL;
	} else {
		general_signal_send(w->socket,
		    SIQ_GENSIG_EXIT);
	}
}
