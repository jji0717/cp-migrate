#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/acl.h>

#include <sys/isi_enc.h>
#include <isi_domain/worm.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>
#include <isi_acl/sd_acl.h>
#include <ifs/ifs_lin_open.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/alg.h"
#include "pworker.h"


static void 
send_extra_file(struct migr_pworker_ctx *pw_ctx, ifs_lin_t slin,
    ifs_snapid_t ssnap, char *tgt_name, enc_t tgt_enc,
    struct isi_error **error_out);
static void
stf_continue_stale_dirs(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
free_work_item(struct migr_pworker_ctx *pw_ctx, struct rep_ctx *rep,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(pw_ctx->work->tw_blk_cnt > 0 || pw_ctx->stf_sync ||
	    pw_ctx->work->rt == WORK_CT_LIN_HASH_EXCPT ||
	    pw_ctx->work->rt == WORK_IDMAP_SEND);

	/*
	 * Check connection with the coordinator before 
	 * cleaning up work item
	 */
	check_connection_with_peer(pw_ctx->coord, true, 1, &error);
	if (error) {
		log(FATAL, "%s", isi_error_get_message(error));
	}

	/* Tell the sworker to release the work item lock */
	if (pw_ctx->send_work_item_lock && pw_ctx->target_work_locking_patch)
		send_wi_lock_msg(pw_ctx->sworker, pw_ctx->wi_lin, false);

	/* Attempt to remove the entry from btree
	 * In the event that a coordinator crashed and restarted as
	 * this pworker was finishing up, the entry will not
	 * be removed from disk and will silently continue
	 * See bug 94213 */
	remove_work_entry_cond(pw_ctx->wi_lin, pw_ctx->work, rep,
	    true, &error);
	if (error)
		goto out;

	/* Free in memory structure */
	ASSERT(pw_ctx->work != NULL);
	free(pw_ctx->work);
	pw_ctx->work = NULL;
	pw_ctx->wi_lin = 0;

	/* Release the work item lock. */
	ASSERT(pw_ctx->wi_lock_fd != -1);
	close(pw_ctx->wi_lock_fd);
	pw_ctx->wi_lock_fd = -1;

out:
	isi_error_handle(error, error_out);
}

void
read_work_item(struct migr_pworker_ctx *pw_ctx, struct generic_msg *m,
    struct rep_ctx *rep, struct isi_error **error_out)
{
	struct work_restart_state *ws = NULL;
	struct isi_error *error = NULL;
	char *tw_data = NULL;
	size_t tw_size = 0;
	char **tw_pointer;
	bool exists;

	log(TRACE, "%s", __func__);
	ASSERT(pw_ctx->stf_rep_iter.iter == NULL);

	ws = calloc(1, sizeof(struct work_restart_state));
	ASSERT(ws);

	tw_pointer = &tw_data;

	pw_ctx->wi_lock_fd = lock_work_item(pw_ctx->policy_id, 0, &error);
	if (error)
		goto out;

	/* Regular work item, just read it from disk */
	exists = get_work_entry(m->body.work_resp3.wi_lin, ws, tw_pointer,
	    &tw_size, rep, &error);
	if (error)
		goto out;

	if (!exists) {
		error = isi_system_error_new(ENOENT,
		    "Unable to read work item %llx", m->body.work_resp3.wi_lin);
		goto out;
	}

	if (ws->rt == WORK_SNAP_TW || ws->rt == WORK_DOMAIN_MARK) {
		ASSERT(ws->tw_blk_cnt > 0);
		pw_ctx->dir_state.dir = NULL;
		migr_tw_clean(&pw_ctx->treewalk);
		migr_tw_init_from_data(&pw_ctx->treewalk, tw_data,
		    tw_size, &error);
		if (error)
			goto out;
	}

	log(NOTICE, "Read work item, work[lin] %llx:%s",
	    m->body.work_resp3.wi_lin, work_type_str(ws->rt));

	UFAIL_POINT_CODE(read_work_item_fail,
	    log(FATAL, "read_work_item_fail"););

	pw_ctx->wi_lin = m->body.work_resp3.wi_lin;

	/* Checkpoint lin should never be less than min lin*/
	ws->chkpt_lin = MAX(ws->chkpt_lin, ws->min_lin);

	ASSERT(pw_ctx->work == NULL);
	pw_ctx->work = ws;

out:
	if (error && ws) {
		free(ws);
		ws = NULL;
	}

	if (tw_data)
		free(tw_data);

	isi_error_handle(error, error_out);
	return;
}

/*
 * Get the first lin from the current level of worklist.
 */
static ifs_lin_t
get_first_worklist_lin(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_lin_t lin = 0;
	struct wl_iter *iter = NULL;
	btree_key_t key;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	bool ret = true;
	
	set_wl_key(&key, pw_ctx->wi_lin, work->wl_cur_level, 0);

	iter = new_wl_iter(cc_ctx->wl, &key);
	ret = wl_iter_next(NULL, iter, &lin, NULL, NULL, &error);
	if (error)
		goto out;
	ASSERT(ret);
out:
	close_wl_iter(iter);
	isi_error_handle(error, error_out);
	return lin;
}

static bool
get_work_split_key(struct migr_pworker_ctx *pw_ctx, unsigned num, unsigned den,
    uint64_t *lin, struct isi_error **error_out)
{
	/* First we need to decide the tree to be used based on work type */
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct rep_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	ASSERT(work != NULL);

	switch (work->rt) {
	case WORK_CC_DIR_DEL:
	case WORK_CC_DIR_CHGS:
		cc_ctx = pw_ctx->chg_ctx;
		ASSERT (cc_ctx != NULL);
		/* Use summary stf tree */
		exists = get_summ_stf_at_loc(num, den,
		    cc_ctx->summ_stf, lin, &error);
		break;

	case WORK_CT_LIN_UPDTS:
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
	case WORK_FLIP_LINS:
	case WORK_DATABASE_CORRECT:
	case WORK_CT_LIN_HASH_EXCPT:
	case WORK_COMP_CONFLICT_CLEANUP:
	case WORK_COMP_CONFLICT_LIST:
		/* Use the repstate tree */
		exists = get_repkey_at_loc(num, den, lin, NULL,
		    pw_ctx->cur_rep_ctx, &error);
		break;
	case WORK_DATABASE_RESYNC:
		if(is_treewalk(pw_ctx)) {
			ctx = pw_ctx->cur_rep_ctx;
		} else {
			ASSERT(cc_ctx != NULL);
			ctx = cc_ctx->rep1;
		}
		exists = get_repkey_at_loc(num, den, lin, NULL,
		    ctx, &error);
		break;
	case WORK_CT_STALE_DIRS:
		cc_ctx = pw_ctx->chg_ctx;
		ASSERT (cc_ctx != NULL);
		/* Use the permanent repstate */
		exists = get_repkey_at_loc(num, den, lin, NULL,
		   cc_ctx->rep1, &error);
		break;
	case WORK_CC_COMP_RESYNC:
	case WORK_CT_COMP_DIR_LINKS:
		exists = get_lin_listkey_at_loc(num, den, cc_ctx->resync,
		    lin, &error);
		break;
	default:
		/* XXX for now ASSERT */
		ASSERT(0);
	}

	isi_error_handle(error, error_out);
	return exists;
}

static bool
condition_met(struct rep_entry  *entryp, enum work_restart_type rt)
{
	bool cond_met = false;

	switch (rt) {
	case WORK_CT_LIN_UPDTS: 
		cond_met = ((entryp->lcount - entryp->excl_lcount) > 0)
		    && ((entryp->changed || entryp->not_on_target));
		break;

	case WORK_CT_FILE_DELS:
		cond_met = ((entryp->lcount - entryp->excl_lcount) ==
		    0) && (!entryp->is_dir) &&
		    (!entryp->not_on_target);
		break;

	case WORK_CT_DIR_DELS:
		cond_met = (entryp->lcount == 0) && (entryp->is_dir);
		break;

	case WORK_CT_DIR_LINKS:
		cond_met = (entryp->lcount > 0) && 
		    (entryp->changed || entryp->not_on_target)
		    && (entryp->is_dir);
		break;

	case WORK_CT_LIN_HASH_EXCPT:
		/* Detect changed files which have exception bit set */
		cond_met = ((entryp->lcount - entryp->excl_lcount) > 0) &&
		    (!entryp->is_dir) && entryp->exception;
		break;

	case WORK_CT_DIR_LINKS_EXCPT:
		/* Detect changed directories with exception bit set */
		cond_met = (entryp->lcount > 0) && 
		    (entryp->changed || entryp->not_on_target)
		    && (entryp->is_dir) && entryp->exception;
		break;
	case WORK_CT_STALE_DIRS:
		cond_met = entryp->is_dir;
		break;

	case WORK_COMP_CONFLICT_CLEANUP:
		cond_met = (entryp->lcount > 0) && (entryp->changed) &&
		    (!entryp->is_dir) && (!entryp->not_on_target);
		break;

	default:
		ASSERT(0);
	}
	return cond_met;
}

/*
 * This adds new lin and associated rep entry to stf rep iter buffer.
 */
static void
add_entry_to_buffer(struct stf_rep_iter *stf_rep_iter, ifs_lin_t *lin,
    struct rep_entry *rep_entry)
{
	int index;	

	index = stf_rep_iter->count;

	stf_rep_iter->lin_rep[index].lin = *lin;
	stf_rep_iter->lin_rep[index].entry = *rep_entry;
	stf_rep_iter->count++;
	ASSERT(stf_rep_iter->count <= MAX_ITER_ENTRIES);
}

/*
 * Read the lins from stf rep iter buffer and send the list to target.
 * The target will call prefetch on all the lins sent.
 */
static void
scan_and_send_prefetch_msg(struct stf_rep_iter *stf_rep_iter, int socket)
{
	int i;
	struct generic_msg msg = {};
	uint64_t lins[TOTAL_STF_LINS];
	struct lin_prefetch_msg *linp = &msg.body.lin_prefetch;

	msg.head.type = LIN_PREFETCH_MSG;
	if (stf_rep_iter->count < 2) {
		/* We dont want to send unnecessary prefetch msg */
		goto out;
	}

	ASSERT(stf_rep_iter->count <= TOTAL_STF_LINS);
	for (i = 0; i < stf_rep_iter->count; i++) {
		lins[i] = stf_rep_iter->lin_rep[i].lin;
	}

	linp->linslen = stf_rep_iter->count * sizeof(uint64_t);
	linp->lins = (char *) lins;
	msg_send(socket, &msg);
out:
	return;
}


/*
 * This function does prefetch (both local and remote) of lins based
 * on the current work type. The following is the list of work types.
 * 
 * WORK_CT_FILE_DELS:
 * WORK_CT_DIR_DELS:
 * For the change transfer delete phase we only send prefetch to target.
 * 
 * WORK_CT_DIR_LINKS:
 * For directory link/unlink functions, we do local prefetch of directory lins.

 * WORK_CT_LIN_UPDTS:
 * For lin update phase, we do both local prefetch of lins and remote prefetch
 * of lins.
 * For all other cases we dont do any type of prefetch.
 */
static void
prefetch_lins(struct migr_pworker_ctx *pw_ctx,
    struct stf_rep_iter *stf_rep_iter, struct isi_error **error_out)
{
	int i, ret;
	struct isi_error *error = NULL;
	ifs_snapid_t snapid = INVALID_SNAPID;
	bool prefetch = false;
	bool send_prefetch = false;
	struct ifs_lin_snapid_portal lins[MAX_ITER_ENTRIES];
	
	switch(pw_ctx->work->rt) {
	case WORK_CT_FILE_DELS: 
	case WORK_CT_DIR_DELS: 
		send_prefetch = true;
		break;
	case WORK_CT_LIN_UPDTS:
		send_prefetch = true;
		snapid = pw_ctx->cur_snap;
		prefetch = true;
		break; 
	case WORK_CT_DIR_LINKS:
		snapid = pw_ctx->cur_snap;
		prefetch = true;
		break;
	default:
		break;	
	}

	/* Do remote prefetch of lins */
	if (send_prefetch) {
		scan_and_send_prefetch_msg(stf_rep_iter, pw_ctx->sworker);
	}

	/* Do local prefetch of lins */	
	if (prefetch) {
		for (i = 0; i < stf_rep_iter->count; i++) {
			lins[i].lin = stf_rep_iter->lin_rep[i].lin;
			lins[i].snapid = snapid;
			lins[i].portal_depth = 0;
		}
		if (stf_rep_iter->count <= 0)
			goto out;

		ret = ifs_prefetch_lin(lins, stf_rep_iter->count, 0);
		if (ret) {
			error = isi_system_error_new(errno,
			    "Unable to prefetch lin range %llx - %llx",
			    lins[0].lin, lins[i - 1].lin);
			goto out;
		}
	
		/* Also prefetch data for new files */
		for(i = 0; i < stf_rep_iter->count; i++) {
			if (!stf_rep_iter->lin_rep[i].entry.is_dir &&
			    stf_rep_iter->lin_rep[i].entry.not_on_target) {
				ret = ifs_prefetch_file(
				    stf_rep_iter->lin_rep[i].lin,
				    pw_ctx->cur_snap, 0, 16/*128K*/);
				if (ret) {
					error = isi_system_error_new(errno,
					    "Unable to prefetch data for "
					    "lin %llx",
					    stf_rep_iter->lin_rep[i].lin);
					goto out;
				}
			}
		}
	}

out:
	isi_error_handle(error, error_out);

}

void
revert_entry(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin,
    struct rep_entry *entryp, struct isi_error **error_out)
{
	struct rep_entry entry = *entryp;
	struct rep_entry base_entry;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	bool found = false;
	struct isi_error *error = NULL;
	

	ASSERT(pw_ctx->restore == true);
	/* All new files need to be deleted */
	if (entryp->not_on_target) {
		entry.lcount = entry.excl_lcount = 0;
		entry.not_on_target = false;
	}

	/* All deleted entries must be created */
	if ((entryp->lcount - entryp->excl_lcount) <= 0) {
		entry.not_on_target = true;
		/* Get lcount entry from rep1 */
		found = get_entry(lin, &base_entry, cc_ctx->rep1,
		   &error);
		if (error)
		       goto out;
		ASSERT(found == true);
		entry.lcount = base_entry.lcount;
	}
	*entryp = entry;
out:
	isi_error_handle(error, error_out);
}

/*
 * This function gets utmost MAX_ITER_ENTRIES from repstate and stores
 * in stf rep iter buffer. The function may read 0 entries if it did not
 * find any valid rep entries (for job phase) in MAX_REP_STATE_SCAN_COUNT
 * number of rep entries. We want to have regular breaks even if we did 
 * not find any valid entry since this will allow us to have effective
 * work splits.
 * Once we read [0 - MAX_ITER_ENTRIES] entries we check if we need to do
 * a local or remote prefetch of lins (prefetch_lins).
 */
static bool
get_next_entries_with_prefetch(struct migr_pworker_ctx *pw_ctx, bool *done,
    struct isi_error **error_out)
{
	int count = 0;
	struct isi_error *error = NULL;
	ifs_lin_t slin;
	bool found = false;
	bool got_one = false;
	struct rep_entry trep_entry;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct rep_iter *rep_iter = stf_rep_iter->iter;
	struct work_restart_state *work = pw_ctx->work;
	enum work_restart_type rt = work->rt;

	ASSERT(stf_rep_iter->count == 0);
	while (count < MAX_REP_STATE_SCAN_COUNT &&
	    stf_rep_iter->count < MAX_ITER_ENTRIES) {
		got_one = rep_iter_next(rep_iter, &slin, &trep_entry, &error);
		if (error)
			goto out;
		if (!got_one || slin >= work->max_lin) {
			*done = true;
			break;
		}

		ASSERT(slin >= work->min_lin);
		if (pw_ctx->restore) {
			revert_entry(pw_ctx, slin, &trep_entry, &error);
			if (error)
				goto out;
		}

		found = condition_met(&trep_entry, rt);

		if (found) {
			add_entry_to_buffer(stf_rep_iter, &slin, &trep_entry);
		}
		count++;
	}

	if (stf_rep_iter->count > 0) {
		*done = false;
		/* Prefetch lins if required */
		prefetch_lins(pw_ctx, stf_rep_iter, &error);
		if (error)
			goto out;
		stf_rep_iter->index = 0;
	}

out:

	isi_error_handle(error, error_out);
	return (stf_rep_iter->count > 0);
}

/*
 * This function reads next available lin and rep entry from
 * the stf rep iter local buffer. It also sets the other variables
 * which include work->lin and pw_ctx->cur_lin  and pw_ctx->rep_entry.
 * These variables are used by each phase to work on that specific lin and
 * rep entry.
 */
static void
read_next_avail_entry(struct migr_pworker_ctx *pw_ctx)
{
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	int index;

	index = stf_rep_iter->index;
	pw_ctx->rep_entry = stf_rep_iter->lin_rep[index].entry;
	pw_ctx->prev_lin = pw_ctx->cur_lin;
	pw_ctx->cur_lin = stf_rep_iter->lin_rep[index].lin;
	pw_ctx->work->lin = stf_rep_iter->lin_rep[index].lin;
	stf_rep_iter->index++;
	stf_rep_iter->count--;
	ASSERT(stf_rep_iter->count >= 0 &&
	    stf_rep_iter->index <= MAX_ITER_ENTRIES);
}

/*
 * Read the next available rep entry from the stf rep iter buffer.
 * If the buffer is emptry then we read next set of rep entries
 * from repstate btree. The done is set to true if we have no more
 * rep entries left to scan or the rep entry key has crossed the 
 * current work range.
 */
static bool
get_next_valid_lin(struct migr_pworker_ctx *pw_ctx,
    bool *done, struct isi_error **error_out)
{
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct rep_iter  *rep_iter;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	bool found = false;

	rep_iter = stf_rep_iter->iter;
	*done = false;
	if (!rep_iter) {
		ASSERT(pw_ctx->cur_rep_ctx != NULL);
		if (pw_ctx->work->rt == WORK_CT_STALE_DIRS) {
			rep_iter = new_rep_range_iter(pw_ctx->chg_ctx->rep1, 
			    work->chkpt_lin);
		} else {
			rep_iter = new_rep_range_iter(pw_ctx->cur_rep_ctx, 
			    work->chkpt_lin);
		}
		ASSERT(rep_iter != NULL);
		stf_rep_iter->iter = rep_iter;
	}

	if (stf_rep_iter->count == 0) {
		/* Read more entries from repstate */
		get_next_entries_with_prefetch(pw_ctx, done, &error);
		if (error)
			goto out;	
	}

	if (stf_rep_iter->count > 0) {
		read_next_avail_entry(pw_ctx);
		if (pw_ctx->cur_lin >= work->max_lin) {
			*done = true;
		} else {
			found = true;
		}
	}

	if (*done) {
		close_rep_iter(rep_iter);
		stf_rep_iter->iter = NULL;
		stf_rep_iter->index = 0;
		stf_rep_iter->count = 0;
		pw_ctx->cur_lin = 0;
	}

out:
	isi_error_handle(error, error_out);
	return found;
}

/*
 * Main Change Transfer (CT) function.
 * It takes next valid lin from rep state (based on ct phase) and
 * executes corresponding function.
 * This requires further changes once we have splitting of rep state work
 */
void
stf_continue_change_transfer(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum work_restart_type rt;
	bool done = false;
	bool found = false;
	struct generic_msg msg = {};

	log(TRACE, "stf_continue_change_transfer");

	ASSERT(pw_ctx->work != NULL);

	UFAIL_POINT_CODE(stop_change_transfer,
		if (RETURN_VALUE > 0) {
			siq_nanosleep(0, RETURN_VALUE * 1000000);
		}

		return;
	);

	/* Bug 150379 - fail point */
	UFAIL_POINT_CODE(ct_ack_count_fail,
	    if (pw_ctx->outstanding_acks >= RETURN_VALUE)
		    log(FATAL, "ct_ack_count_fail"););

	if (pw_ctx->outstanding_acks >= STF_ACK_INTERVAL) {
		/* We will wait for ack from sworker */
		migr_wait_for_response(pw_ctx, MIGR_WAIT_STF_LIN_ACK);
		goto out;
	}
	rt = pw_ctx->work->rt;
	found = get_next_valid_lin(pw_ctx, &done, &error);
	if (error)
		goto out;

	if (done) {
		flush_cpss_log(&pw_ctx->cpss_log, pw_ctx->cur_cpss_ctx,
		    &error);
		if (error)
			goto out;
		migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
		pw_ctx->done = 1;
		pw_ctx->pending_done_msg = true;
		msg.head.type = DONE_MSG;
		msg_send(pw_ctx->sworker, &msg);
		flush_burst_buffer(pw_ctx);
		goto out;
	} else if (!found) {
		goto out;
	}

	switch (rt) {
	case WORK_CT_LIN_UPDTS:
	case WORK_CT_LIN_HASH_EXCPT:
		stf_continue_lin_transfer(pw_ctx, &error);
		break;

	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
		stf_continue_lin_deletes(pw_ctx, &error);
		break;

	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
		if (pw_ctx->chg_ctx)
			clear_migr_dir(pw_ctx, 0);
		migr_call_next(pw_ctx, stf_continue_lin_unlink);
		break;

	case WORK_CT_STALE_DIRS:
		stf_continue_stale_dirs(pw_ctx, &error);
		break;

	case WORK_COMP_CONFLICT_CLEANUP:
		stf_continue_comp_conflict_cleanup(pw_ctx, &error);
		break;

	default:
		ASSERT(0);
	}

out:
	isi_error_handle(error, error_out);
}



void
stf_request_new_work(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;

	if (pw_ctx->sworker > 0) {
		flush_burst_buffer(pw_ctx);
	}

	clear_migr_dir(pw_ctx, 0);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	/*
	 * If we had restart while cleaningup the worklist,
	 * those need to be removed now since we would have 
	 * missed those. We try to cleanup all levels of worklist.
	 */
	wl_remove_entries(cc_ctx->wl, pw_ctx->wi_lin, -1, &error);
	if (error)
		goto out;

	free_work_item(pw_ctx, cc_ctx->rep2, &error);
	if (error)
		goto out;

	ASSERT(pw_ctx->stf_rep_iter.iter == NULL);
	migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
	send_work_request(pw_ctx, false);

out:
	isi_error_handle(error, error_out);
}

/**
 * Process recursively added directories in the worklist.
 */
static void
migr_continue_added_dirs_worklist(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int rv;

	log(TRACE, "migr_continue_added_dirs_worklist");

	rv = process_added_dirs_worklist(pw_ctx, &error);
	if (error)
		goto out;

	if (!rv) {
		stf_request_new_work(pw_ctx, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * Process modified/new file and directory mods directly referenced from stf.
 */
static void
migr_continue_mods_changeset(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct isi_error *error = NULL;
	int rv;
	log(TRACE, "migr_continue_mods_changeset");

	rv = process_dirent_and_file_mods(pw_ctx, &error);
	if (error)
		goto out;

	if (!rv) {
		flush_stf_logs(pw_ctx, &error);
		if (error)
			goto out;
		/* Checkpoint phase to move to next work type */
		pw_ctx->work->rt = WORK_CC_DIR_CHGS_WL;
		pw_ctx->work->lin = 0;
		pw_ctx->work->chkpt_lin = 0;
		clear_checkpoint(pw_ctx->work);
		ASSERT(pw_ctx->work->wl_cur_level == 0);
		pw_ctx->work->wl_cur_level = 1;
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		    cc_ctx->rep2, &error);
		if (error)
			goto out;

		migr_call_next(pw_ctx, migr_continue_added_dirs_worklist);
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * Process the path predicate worklist (if any) created
 * during summary stf creation.
 */
static void
migr_continue_ppath_worklist(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int rv;
	log(TRACE, "migr_continue_ppath_worklist");

	rv = process_ppath_worklist(pw_ctx, &error);
	if (error)
		goto out;

	if (!rv) {
		stf_request_new_work(pw_ctx, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}


/**
 * Process recursively removed directories from worklist.
 */
static void
migr_continue_removed_dirs_worklist(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int rv;
	log(TRACE, "migr_continue_removed_dirs_worklist");

	rv = process_removed_dirs_worklist(pw_ctx, &error);
	if (error)
		goto out;

	if (!rv) {
		stf_request_new_work(pw_ctx, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * Process removed directories directly referenced by the stf.
 */
static void
migr_continue_remove_dirs_changeset(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct isi_error *error = NULL;
	int rv;

	log(TRACE, "migr_continue_remove_dirs_changeset");

	rv = process_removed_dirs(pw_ctx, &error);
	if (error)
		goto out;

	if (!rv) {

		flush_stf_logs(pw_ctx, &error);
		if (error)
			goto out;
		/* Checkpoint phase to move to next work type */
		pw_ctx->work->rt = WORK_CC_DIR_DEL_WL;
		pw_ctx->work->lin = 0;
		pw_ctx->work->chkpt_lin = 0;
		clear_checkpoint(pw_ctx->work);
		ASSERT(pw_ctx->work->wl_cur_level == 0);
		pw_ctx->work->wl_cur_level = 1;
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		    cc_ctx->rep2, &error);
		if (error)
			goto out;

		migr_call_next(pw_ctx, migr_continue_removed_dirs_worklist);
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Add select tree entries to summary stf
 */
bool
check_select_entries(struct changeset_ctx *cc_ctx, struct rep_ctx *sel,
    bool add_to_stf, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct sel_entry sel_entry;
	struct rep_iter *iter = NULL;
	ifs_lin_t new_lin, sel_lin;
	bool got_one, exists;
	struct rep_ctx *rep = cc_ctx->rep1;
	bool has_excludes = false;

	iter = new_rep_iter(sel);
	do {
		got_one = sel_iter_next(iter, &sel_lin, &sel_entry,
		    &error);
		if (error)
			goto out;
		if (got_one) {
			/* I dont think we need includes */
			if (sel_entry.includes)
				continue;

			if (sel_entry.excludes) {
				new_lin = sel_entry.plin;
			} else if (sel_entry.for_child) {
				new_lin = sel_lin;
			} else {
				ASSERT(0);
			}
			has_excludes = true;

			if (!add_to_stf)
				continue;

			/* Check that it is their is repstate1 */
			exists = entry_exists(new_lin, rep, &error);
			if (error)
				goto out;

			if (!exists)
				continue;

			set_stf_entry(new_lin, cc_ctx->summ_stf, &error);
			if (error)
				goto out;
		}
	} while (got_one);

out:
	close_rep_iter(iter);
	isi_error_handle(error, error_out);
	return has_excludes;
}

/**
 * Start changeset computation.  This inits the repstate
 * data structures.
 */
static void
migr_start_changeset(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool done;
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	log(TRACE, "migr_start_changeset");

	if (pw_ctx->prev_snap == INVALID_SNAPID) {
		error = isi_system_error_new(EINVAL,
		    "Previous snapid is invalid");
		goto out;
	}

	/*
	 * First worker will have to find max and min lin 
	 * between two syncIQ snapshots
	 */
	if (work->max_lin == -1) {
		snapset_min_max(cc_ctx->snap1, cc_ctx->snap2, &work->min_lin,
		    &work->max_lin, &error);
		if (error)
			goto out;
	
		/*
		 * Increment the max_lin by one so that we also cover
		 * max_lin in stf code path
		 */
		work->max_lin++;
		work->lin = 0;
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		    cc_ctx->rep2, &error);
		if (error)
			goto out;
		/*
		 * make sure all previous(S1 version) exclusive/inclusive
		 * directory lins are seen in this phase. Note that we are not
		 * actually adding exclusion directory lins but their parents.
		 * This should make sure that they all have valid rep entries in
		 * previous phase i.e S1. Also since for_child are added to rep
		 * state they should already be present.
		 *
		 * Also add all S2 version select entries which have entries in
		 * repstate1. This way tree moves from normal lins to select are
		 * handled.
		 */
		check_select_entries(cc_ctx, cc_ctx->sel1, true, &error);
		if (error)
			goto out;
		check_select_entries(cc_ctx, cc_ctx->sel2, true, &error);
		if (error)
			goto out;

	}

	done = process_stf_lins(pw_ctx, false, &error);
	if (error)
		goto out;

	if (done) {
		flush_stf_logs(pw_ctx, &error);
		if (error)
			goto out;

		/* Checkpoint phase to move to next work type */
		pw_ctx->work->rt = WORK_CC_PPRED_WL;
		pw_ctx->work->lin = 0;
		pw_ctx->work->chkpt_lin = 0;
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		    cc_ctx->rep2, &error);
		if (error)
			goto out;

		migr_call_next(pw_ctx, migr_continue_ppath_worklist);
	}

out:
	isi_error_handle(error, error_out);
}

static void
send_rep_entries(struct migr_pworker_ctx *pw_ctx, ifs_lin_t *linlist,
    struct rep_entry *entries, struct cpss_entry *cpss_entries, int count,
    unsigned flags, struct isi_error **error_out)
{
	int i;
	struct isi_error *error = NULL;
	struct rep_update rep_entries[SBT_BULK_MAX_OPS];
	struct generic_msg rep_updt_msg = {};

	ASSERT(count <= SBT_BULK_MAX_OPS);
	memset(rep_entries, 0, sizeof(struct rep_update) * SBT_BULK_MAX_OPS);
	/* Fill the rep_update entries */
	for (i = 0; i < count; i++) {
		rep_entries[i].slin = linlist[i];
		rep_entries[i].lcount =
		    (entries[i].lcount - entries[i].excl_lcount);
		if (entries[i].is_dir)
			rep_entries[i].flag |= REP_IS_DIR;
		if (entries[i].non_hashed)
			rep_entries[i].flag |= REP_NON_HASHED;
		if (entries[i].not_on_target)
			rep_entries[i].flag |= REP_NOT_ON_TARGET;
		if (!entries[i].is_dir) {
			ASSERT(cpss_entries[i].sync_type < BST_MAX);
			rep_entries[i].flag |=
			    (cpss_entries[i].sync_type & 0x03) << 3;
		}
	}

	rep_updt_msg.head.type = DATABASE_UPDT_MSG;
	rep_updt_msg.body.database_updt.flag = REPSTATE_UPDT | flags;
	rep_updt_msg.body.database_updt.updtlen = 
	    count * sizeof(struct rep_update);
	rep_updt_msg.body.database_updt.updt = (char *)rep_entries;
	msg_send(pw_ctx->sworker, &rep_updt_msg);

	isi_error_handle(error, error_out); 

}

static void
migr_update_flip_phase_stats(struct migr_pworker_ctx *pw_ctx)
{
	if (pw_ctx->flip_flags & FLIP_REGULAR) {
		pw_ctx->stf_cur_stats->change_transfer->flipped_lins++;
	} else if (pw_ctx->flip_flags & REVERT_ENTRIES) {
		pw_ctx->stf_cur_stats->change_transfer->corrected_lins++;
	} else if (pw_ctx->flip_flags & RESYNC_ENTRIES) {
		pw_ctx->stf_cur_stats->change_transfer->resynced_lins++;
	}
}

static void
migr_continue_regular_flip_lins(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{

	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	struct rep_info_entry rie_rep1, rie_rep2;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;

	log(TRACE, "migr_continue_regular_flip_lins");

	if (stf_rep_iter->iter == NULL) {
		get_rep_info(&rie_rep1, cc_ctx->rep1, &error);
		if (error)
			goto out;

		get_rep_info(&rie_rep2, cc_ctx->rep2, &error);
		if (error)
			goto out;

		if (rie_rep1.is_sync_mode != rie_rep2.is_sync_mode) {
			rie_rep1.is_sync_mode = rie_rep2.is_sync_mode;
			set_rep_info(&rie_rep1, cc_ctx->rep1, false, &error);
			if (error)
				goto out;
		}
		stf_rep_iter->iter = new_rep_range_iter(pw_ctx->cur_rep_ctx,
		    work->chkpt_lin);
		
		pw_ctx->flip_flags = 0;
		pw_ctx->flip_flags |= FLIP_REGULAR;
	}

	migr_call_next(pw_ctx, migr_continue_flip_lins);

out:
	isi_error_handle(error, error_out);
}

static void
migr_continue_correct_lins(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{

	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;

	log(TRACE, "migr_continue_correct_lins");

	if (stf_rep_iter->iter == NULL) {
		stf_rep_iter->iter = new_rep_range_iter(cc_ctx->rep2,
		    work->chkpt_lin);
		
		pw_ctx->flip_flags = 0;
		pw_ctx->flip_flags |= REVERT_ENTRIES;
	}

	migr_call_next(pw_ctx, migr_continue_flip_lins);	

	isi_error_handle(error, error_out);
}

static void
migr_continue_resync_lins(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct rep_ctx *ctx = NULL;

	log(TRACE, "migr_continue_resync_lins");

	if (stf_rep_iter->iter == NULL) {
		if (is_treewalk(pw_ctx)) {
			ctx = pw_ctx->cur_rep_ctx;
		} else {
			ASSERT(cc_ctx != NULL);
			ctx = cc_ctx->rep1;
		}
		stf_rep_iter->iter = new_rep_range_iter(ctx, work->chkpt_lin);

		pw_ctx->flip_flags = 0;
		pw_ctx->flip_flags |= RESYNC_ENTRIES;
	}

	migr_call_next(pw_ctx, migr_continue_flip_lins);

	isi_error_handle(error, error_out);
}

/**
 * Copy all differential repstate lins to base repstate
 */
void
migr_continue_flip_lins(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	ifs_lin_t linlist[SBT_BULK_MAX_OPS];
	struct rep_entry entries[SBT_BULK_MAX_OPS];
	struct rep_entry entry;
	struct cpss_entry cpss_entries[SBT_BULK_MAX_OPS];
	struct cpss_entry cpse;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct generic_msg msg = {};
	ifs_lin_t lin;
	bool done = false;
	bool found, cpse_found;
	int i = 0;
	int rep_count;

	log(TRACE, "migr_continue_flip_lins");

	ASSERT(stf_rep_iter->iter != NULL);
	if (pw_ctx->outstanding_acks >= STF_ACK_INTERVAL) {
		/* We will wait for ack from sworker */
		migr_wait_for_response(pw_ctx, MIGR_WAIT_STF_LIN_ACK);
		goto out;
	}

	while (1) {
		/*
		 * Bug 259348 & PSCALE-13853
		 * Force worker to exit if failpoint is set.
		 */
		UFAIL_POINT_CODE(continue_flip_lins_gensig_exit,
			if (pw_ctx->outstanding_acks < (STF_ACK_INTERVAL / 2) &&
			    pw_ctx->outstanding_acks > 0 &&
			    (work->rt == WORK_DATABASE_RESYNC ||
			     work->rt == WORK_FLIP_LINS)) {
			    process_gensig_exit(pw_ctx);
			    return;
			}
		);

		found = rep_iter_next(stf_rep_iter->iter, &lin, &entry, &error);
		if (error)
			goto out;
		if (!found || (lin >= work->max_lin)) {
			done = true;
			break;
		}
		work->lin = lin;
		linlist[i] = lin;
		if (pw_ctx->flip_flags & FLIP_REGULAR) {
			if (entry.hash_computed ||
			    (entry.exception && (!entry.is_dir)))
				entry.non_hashed = false;
			entry.exception = false;
			entry.hash_computed = false;
			entry.changed = false;
			entry.lcount_set = false;
			entry.lcount_reset = false;
			entry.new_dir_parent = false;
			entry.not_on_target = false;
			entry.for_path = false;
		}
		entries[i] = entry;

		cpse_found = false;

		if (pw_ctx->flip_flags & RESYNC_ENTRIES) {
			cpse_found = get_cpss_entry(lin, &cpse,
			    cc_ctx == NULL ? pw_ctx->cur_cpss_ctx :
			    cc_ctx->cpss1, &error);
			if (error)
				goto out;
		} else if (pw_ctx->flip_flags & FLIP_REGULAR) {
			cpse_found = get_cpss_entry(lin, &cpse, cc_ctx->cpss2,
			    &error);
			if (error)
				goto out;
		}

		/*
		 * If we did a sync where a file was a stub and then recalled
		 * that file, its entry will remain in the list of
		 * cpss_entries since the lcount will be > 0. The sync type
		 * for the file cannot change without a policy reset so
		 * upon being restubbed, the entry will be valid again.
		 */
		if (cpse_found)
			cpss_entries[i] = cpse;
		else
			cpss_entries[i].sync_type = BST_NONE;
		cpss_entries[i].del_entry = (entry.lcount == 0);

		i++;
		migr_update_flip_phase_stats(pw_ctx);
		if (i == SBT_BULK_MAX_OPS)
			break;
	}

	rep_count = i;
	if (pw_ctx->work->rt == WORK_DATABASE_RESYNC &&
	    pw_ctx->compliance_v2 && done) {
		linlist[i] = INVALID_LIN;
		cpss_entries[i].sync_type = BST_NONE;
		rep_count++;
	}

	if (rep_count > 0) {
		if (pw_ctx->curr_ver >= FLAG_VER_3_5) {
			/*
			 * Send the rep entries to target so that
			 * target can remove deleted lin entries from
			 * linmap
			 */
			send_rep_entries(pw_ctx, linlist, entries,
			    cpss_entries, rep_count, pw_ctx->flip_flags,
			    &error);
			if (error)
				goto out;
			pw_ctx->outstanding_acks += i;
		}
		if ((pw_ctx->flip_flags & FLIP_REGULAR)) {
			copy_rep_entries(cc_ctx->rep1, linlist, entries, i,
			    &error);
			if (error)
				goto out;

			copy_cpss_entries(cc_ctx->cpss1, linlist,
			    cpss_entries, i, &error);
			if (error)
				goto out;
		}
	}

	if (done) {
		migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
		pw_ctx->done = 1;
		pw_ctx->pending_done_msg = true;
		msg.head.type = DONE_MSG;
		msg_send(pw_ctx->sworker, &msg);
		close_rep_iter(stf_rep_iter->iter);
		stf_rep_iter->iter = NULL;
	}

out:
	isi_error_handle(error, error_out);
}

void
handle_work(struct migr_pworker_ctx *pw_ctx)
{
	log(TRACE, "%s", __func__);
	ASSERT (pw_ctx->work != NULL);

	switch (pw_ctx->work->rt) {
	case WORK_SUMM_TREE:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_start_changeset);
		break;

	case WORK_CC_PPRED_WL:
		migr_call_next(pw_ctx, migr_continue_ppath_worklist);
		break;

	case WORK_CC_DIR_DEL:
		migr_call_next(pw_ctx, migr_continue_remove_dirs_changeset);
		break;

	case WORK_CC_DIR_DEL_WL:
		migr_call_next(pw_ctx, migr_continue_removed_dirs_worklist);
		break;

	case WORK_CC_DIR_CHGS:
		//Bug 215880
		pw_ctx->work->lin = pw_ctx->work->chkpt_lin;
		migr_call_next(pw_ctx, migr_continue_mods_changeset);
		break;

	case WORK_CC_DIR_CHGS_WL:
		migr_call_next(pw_ctx, migr_continue_added_dirs_worklist);
		break;

	case WORK_CT_LIN_UPDTS:
	case WORK_CT_LIN_HASH_EXCPT:
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
	case WORK_CT_STALE_DIRS:
		pw_ctx->send_work_item_lock = true;
		if (pw_ctx->restore && !pw_ctx->snap_switched) {
			snap_switch(&pw_ctx->prev_snap, &pw_ctx->cur_snap);
			snap_switch(&(pw_ctx->chg_ctx->snap1),
			    &(pw_ctx->chg_ctx->snap2));
			pw_ctx->snap_switched = true;
		}
		migr_call_next(pw_ctx, stf_continue_change_transfer);
		break;

	case WORK_FLIP_LINS:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_regular_flip_lins);
		break;

	case WORK_DATABASE_CORRECT:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_correct_lins);
		break;

	case WORK_DATABASE_RESYNC:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_resync_lins);
		break;

	case WORK_COMP_CONFLICT_CLEANUP:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, stf_continue_change_transfer);
		break;

	case WORK_COMP_CONFLICT_LIST:
		migr_call_next(pw_ctx, stf_continue_conflict_list_begin);
		break;

	case WORK_IDMAP_SEND:
		pw_ctx->send_work_item_lock = true;
		pw_ctx->doing_extra_file = true;
		migr_call_next(pw_ctx, migr_send_idmap);
		break;

	case WORK_SNAP_TW:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_start_work);
		break;

	case WORK_DOMAIN_MARK:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, dmk_start_work);
		break;

	case WORK_CC_COMP_RESYNC:
		// Bug 246049
		pw_ctx->work->lin = pw_ctx->work->chkpt_lin;
		migr_call_next(pw_ctx, migr_continue_regular_comp_resync);
		break;

	case WORK_CT_COMP_DIR_LINKS:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_regular_comp_lin_links);
		break;

	case WORK_COMP_COMMIT:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_comp_commit);
		break;

	case WORK_COMP_MAP_PROCESS:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_comp_map_process);
		break;

	case WORK_CT_COMP_DIR_DELS:
		pw_ctx->send_work_item_lock = true;
		migr_call_next(pw_ctx, migr_continue_comp_dir_dels);
		break;

	default:
		/* XXX for now ASSERT */
		ASSERT(0);
	}
}

static void
check_for_dynamic_repstate( struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool found = false;
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct rep_info_entry rie_rep1;

	/*
	 * If there is rep info available then we have
	 * a valid repstate to be used. Otherwise set the
	 * dynamic bit so that all the repstate lookups
	 * directly refer to lin lookup.
	 */
	found = get_rep_info(&rie_rep1, cc_ctx->rep1, &error);
	if (found || error)
		goto out;
	
	set_dynamic_rep_lookup(cc_ctx->rep1,
	    cc_ctx->snap1, pw_ctx->domain_id);

out:
	isi_error_handle(error, error_out);
}

int
work_resp_stf_callback(struct generic_msg *m, void *ctx)////work_resp_msg
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;

	ilog(IL_INFO, "%s", __func__);
	ASSERT(pw_ctx->pending_work_req);
	pw_ctx->pending_work_req = false;
	pw_ctx->done = 0;
	pw_ctx->last_chkpt_time = time(NULL);
	/* Bug 130312
	 * log when there are acks leftover from a prior work item */
	if (pw_ctx->outstanding_acks > 0)
		log(DEBUG, "%s: outstanding_acks: %d", __func__,
		    pw_ctx->outstanding_acks);

	/* We got a new work item, clear remnants of delayed splits */
	pw_ctx->skip_checkpoint = false;
	pw_ctx->split_req_pending = false;
	pw_ctx->send_work_item_lock = false;

	if (!is_treewalk(pw_ctx) && !pw_ctx->chg_ctx) {
		pw_ctx->chg_ctx = create_changeset_resync(pw_ctx->policy_id,
		    pw_ctx->prev_snap, pw_ctx->cur_snap, pw_ctx->restore,
		    pw_ctx->src, pw_ctx->compliance_v2, &error);
		if (error)
			goto out;
		if (pw_ctx->restore) {
			check_for_dynamic_repstate(pw_ctx, &error);
			if (error)
				goto out;
		}
		/* Check whether job has exclusion entries */
		pw_ctx->has_excludes = check_select_entries(pw_ctx->chg_ctx,
		    pw_ctx->chg_ctx->sel1, false, &error);
		if (error)
			goto out;

		pw_ctx->has_excludes |= check_select_entries(pw_ctx->chg_ctx,
		    pw_ctx->chg_ctx->sel2, false, &error);
		if (error)
			goto out;
	}

	read_work_item(pw_ctx, m, pw_ctx->cur_rep_ctx, &error);
	if (error)
		goto out;

	check_connection_with_peer(pw_ctx->coord, true, 1, &error);
	if (error) {
		log(FATAL, "%s", isi_error_get_message(error));
	}

	/* Hang forever after getting a work item lock. Kill this pworker to
	 * release the work item lock. */
	UFAIL_POINT_CODE(lock_work_and_hang,
	    while (1)
	        siq_nanosleep(1, 0);
	);
	
	handle_work(pw_ctx);

	/* Send a work item lock message to the sworker */
	if (pw_ctx->send_work_item_lock && pw_ctx->target_work_locking_patch)
		send_wi_lock_msg(pw_ctx->sworker, pw_ctx->wi_lin, true);

	migr_reschedule_work(pw_ctx);

out:
	if (error)
		handle_stf_error_fatal(pw_ctx->coord, error, E_SIQ_WORK_INIT,
		    EL_SIQ_SOURCE, m->body.work_resp3.wi_lin,
		    m->body.work_resp3.wi_lin);

	return 0;
}

/*
 * Sends a work item lock message to the sworker. If the variable lock is set
 * to true, then it will lock the work item. If the variable lock is set to
 * false, then it will release the work item lock.
 */
void
send_wi_lock_msg(int sworker, uint64_t wi_lin, bool lock)
{
	struct generic_msg msg = {};

	msg.head.type = build_ript_msg_type(TARGET_WORK_LOCK_MSG);
	ript_msg_init(&msg.body.ript);
	ript_msg_set_field_uint64(&msg.body.ript, RMF_WI_LIN, 1, wi_lin);
	ript_msg_set_field_uint8(&msg.body.ript, RMF_LOCK_WORK_ITEM, 1, lock);
	msg_send(sworker, &msg);
}

bool
try_snapset_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	ifs_lin_t mid;
	bool found_split = false;
	struct isi_error *error = NULL;
	struct work_restart_state *work = pw_ctx->work;
	struct work_restart_state twork = {};
	int small_range = 1000;

	/* We have not initialized work range */
	if (work->max_lin == -1 || work->lin == 0) {
		goto out;
	}
	/* We are done with work range */
	if ((work->lin + small_range) >= work->max_lin) {
		goto out;
	}

	/* Find the mid lin value from the curent work->lin */
	mid = (work->lin + work->max_lin) /  2;
	
	twork.rt = work->rt;
	twork.min_lin = mid;
	twork.lin = 0; /* So that we start from mid */
	twork.max_lin = work->max_lin;
	twork.dircookie1 = twork.dircookie2 = DIR_SLICE_MIN;
	work->max_lin = mid;
	/* Add new work item and update current work item */
	add_split_work_entry(pw_ctx, pw_ctx->wi_lin,
	    work, NULL, 0, pw_ctx->split_wi_lin, &twork, NULL, 0, &error);
	if (error)
		goto out;

	log(NOTICE, "Successful split[%s], two ranges are current work(%llu)"
	    "lin1=%llx lin2=%llx, new work(%llu) lin1=%llx lin2=%llx",
	    work_type_str(work->rt), pw_ctx->wi_lin, work->min_lin,
	    work->max_lin, pw_ctx->split_wi_lin, twork.min_lin, twork.max_lin);

	found_split = true;

out:
	isi_error_handle(error, error_out);
	return found_split;
}

/* Get a lin such that min <= lin < max */
static bool
stf_lin_in_range_exists(struct summ_stf_ctx *stf_ctx,
    ifs_lin_t min, ifs_lin_t max, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct summ_stf_iter *stf_iter = NULL;
	ifs_lin_t lin = 0;
	int num_found = 0;
	bool result = false;

	log(TRACE, "%s", __func__);

	ASSERT(stf_ctx != NULL);

	stf_iter = new_summ_stf_iter(stf_ctx, min);
	num_found = get_summ_stf_next_lins(stf_iter, &lin, 1, &error);
	if (error || !num_found)
		goto out;

	ASSERT(lin >= min);

	if (lin < max)
		result = true;

out:
	close_summ_stf_iter(stf_iter);
	isi_error_handle(error, error_out);
	return result;
}

/* Get a lin such that min <= lin < max */
static bool
rep_lin_in_range_exists(struct rep_ctx *ctx, ifs_lin_t min, ifs_lin_t max,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_iter *rep_iter = NULL;
	ifs_lin_t lin = 0;
	struct rep_entry rep_ent;
	bool got_one = false, result = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	rep_iter = new_rep_range_iter(ctx, min);
	got_one = rep_iter_next(rep_iter, &lin, &rep_ent, &error);
	if (error || !got_one)
		goto out;

	ASSERT(lin >= min);

	if (lin < max)
		result = true;

out:
	close_rep_iter(rep_iter);
	isi_error_handle(error, error_out);
	return result;

}

/* Get a lin such that min <= lin < max */
static bool
resync_lin_in_range_exists(struct lin_list_ctx *ctx, ifs_lin_t min,
    ifs_lin_t max, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct lin_list_iter *resync_iter = NULL;
	ifs_lin_t lin = 0;
	struct lin_list_entry resync_ent;
	bool got_one = false, result = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	resync_iter = new_lin_list_range_iter(ctx, min);
	got_one = lin_list_iter_next(resync_iter, &lin, &resync_ent, &error);
	if (error || !got_one)
		goto out;

	ASSERT(lin >= min);

	if (lin < max)
		result = true;

out:
	close_lin_list_iter(resync_iter);
	isi_error_handle(error, error_out);
	return result;

}

static bool
lin_in_range_exists(struct migr_pworker_ctx *pw_ctx, ifs_lin_t min,
    ifs_lin_t max, struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct summ_stf_ctx *stf_ctx = NULL;
	struct rep_ctx *ctx = NULL;
	struct lin_list_ctx *resync_ctx = NULL;
	struct isi_error *error = NULL;
	bool result = false;

	log(TRACE, "%s", __func__);

	ASSERT(work != NULL);

	/* Decide which btree context to use based on the phase */
	switch (work->rt) {
	case WORK_CC_DIR_DEL:
	case WORK_CC_DIR_CHGS:
		/* Use summary stf tree */
		ASSERT(cc_ctx != NULL);
		stf_ctx = cc_ctx->summ_stf;
		break;
	case WORK_CT_LIN_UPDTS:
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
	case WORK_FLIP_LINS:
	case WORK_DATABASE_CORRECT:
	case WORK_CT_LIN_HASH_EXCPT:
	case WORK_COMP_CONFLICT_CLEANUP:
	case WORK_COMP_CONFLICT_LIST:
		/* Use the repstate tree */
		ctx = pw_ctx->cur_rep_ctx;
		break;
	case WORK_DATABASE_RESYNC:
		if(is_treewalk(pw_ctx)) {
			ctx = pw_ctx->cur_rep_ctx;
		} else {
			ASSERT(cc_ctx != NULL);
			ctx = cc_ctx->rep1;
		}
		break;
	case WORK_CT_STALE_DIRS:
		/* Use the permanent repstate */
		ASSERT (cc_ctx != NULL);
		ctx = cc_ctx->rep1;
		break;
	case WORK_CC_COMP_RESYNC:
	case WORK_CT_COMP_DIR_LINKS:
		/* Use the resync list */
		ASSERT(cc_ctx->resync != NULL);
		resync_ctx = cc_ctx->resync;
		break;
	default:
		/* XXX for now ASSERT */
		ASSERT(0);
	}

	/*
	 * Need to find the lin in a repstate btree or summ_stf btree,
	 * one or the other will have been assigned based on the phase.
	 */
	if (ctx) {
		result = rep_lin_in_range_exists(ctx, min, max, &error);
	} else if (stf_ctx) {
		result = stf_lin_in_range_exists(stf_ctx, min, max, &error);
	} else {
		result = resync_lin_in_range_exists(resync_ctx, min, max,
		    &error);
	}

	isi_error_handle(error, error_out);
	return result;
}
static ifs_lin_t
get_work_phase_max_lin(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct summ_stf_ctx *stf_ctx = NULL;
	struct rep_ctx *ctx = NULL;
	struct lin_list_ctx *resync_ctx = NULL;
	struct isi_error *error = NULL;
	ifs_lin_t max_lin = -1;

	log(TRACE, "%s", __func__);

	ASSERT(work != NULL);

	/* Decide which btree context to use based on the phase */
	switch (work->rt) {
	case WORK_CC_DIR_DEL:
	case WORK_CC_DIR_CHGS:
		/* Use summary stf tree */
		ASSERT(cc_ctx != NULL);
		stf_ctx = cc_ctx->summ_stf;
		break;
	case WORK_CT_LIN_UPDTS:
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
	case WORK_FLIP_LINS:
	case WORK_DATABASE_CORRECT:
	case WORK_CT_LIN_HASH_EXCPT:
	case WORK_COMP_CONFLICT_CLEANUP:
	case WORK_COMP_CONFLICT_LIST:
		/* Use the repstate tree */
		ctx = pw_ctx->cur_rep_ctx;
		break;
	case WORK_DATABASE_RESYNC:
		if(is_treewalk(pw_ctx)) {
			ctx = pw_ctx->cur_rep_ctx;
		} else {
			ASSERT(cc_ctx != NULL);
			ctx = cc_ctx->rep1;
		}
		break;
	case WORK_CT_STALE_DIRS:
		/* Use the permanent repstate */
		ASSERT (cc_ctx != NULL);
		ctx = cc_ctx->rep1;
		break;
	case WORK_CC_COMP_RESYNC:
	case WORK_CT_COMP_DIR_LINKS:
		/* Use the resync list */
		ASSERT(cc_ctx->resync != NULL);
		resync_ctx = cc_ctx->resync;
		break;
	default:
		/* XXX for now ASSERT */
		ASSERT(0);
	}

	/*
	 * Need to find the max lin in a repstate btree or summ_stf btree,
	 * one or the other will have been assigned based on the phase.
	 */
	if (ctx) {
		max_lin = get_rep_max_lin(ctx, &error);
	} else if (stf_ctx) {
		max_lin = get_summ_stf_max_lin(stf_ctx, &error);
	} else {
		max_lin = get_lin_list_max_lin(resync_ctx, &error);
	}

	isi_error_handle(error, error_out);
	return max_lin;
}

/*
 * This functions tries to do simple lin division using lin numerical values.
 */
bool
try_lin_division_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct work_restart_state twork = {};
	struct isi_error *error = NULL;
	bool found_split = false;
	ifs_lin_t mid_lin, max_lin;
	int i;

	/*
	 * Lets avoid split for following conditions.
	 * 1) If the work has not yet started the work. work->lin == 0
	 * 2) If the min lin is 2, lets skip
	 * If the max lin is -1, we will keep splitting off empty ranges
	 * until the max_lin of the valid range is corrected.
	 */
	if (work->lin <= ROOT_LIN)
		goto out;

	/* 
	 * Update the default max_lin to the max lin in the btree.
	 * Note: work->max_lin is exclusive, add 1 to the actual max.
	 */
	if(work->max_lin == -1) {
		max_lin = get_work_phase_max_lin(pw_ctx, &error);
		if (error)
			goto out;
		work->max_lin = max_lin + 1;
	}

	/* keep dividing the range in half a max of 20 times until we find a
	 * lin range that has at least one valid lin to work on */
	for (i = 0; i < 20; i++) {
		mid_lin = work->lin + ((work->max_lin - work->lin) / 2);
		if (mid_lin <= work->lin || mid_lin >= work->max_lin)
			goto out;

		if (lin_in_range_exists(pw_ctx, mid_lin, work->max_lin,
		    &error))
			break;

		if (error)
			goto out;

		work->max_lin = mid_lin;
	}

	ASSERT(mid_lin > work->min_lin);

	/* We found valid split */
	memset(&twork, 0, sizeof(struct work_restart_state));
	twork.rt = work->rt;
	/* We will not use btree range values for future splits */
	twork.num1 = work->num1 = 0;
	twork.den1 = work->den1 = 0;
	twork.num2 = work->num2 = 0;
	twork.den2 = work->den2 = 0;
	twork.min_lin = mid_lin;
	twork.chkpt_lin = mid_lin;
	twork.max_lin = work->max_lin;
	twork.dircookie1 = twork.dircookie2 = DIR_SLICE_MIN;
	work->max_lin = mid_lin;


	/* Add new work item and update current work item */
	add_split_work_entry(pw_ctx, pw_ctx->wi_lin,
	    work, NULL, 0, pw_ctx->split_wi_lin, &twork, NULL, 0, &error);
	if (error)
		goto out;

	log(NOTICE, "Successful split[%s], two ranges are "
	    "current work(%llu) lin1=%llx lin2=%llx, "
	    "new work(%llu) lin1=%llx lin2=%llx",
	    work_type_str(work->rt), pw_ctx->wi_lin,
	    work->min_lin, work->max_lin, pw_ctx->split_wi_lin,
	    twork.min_lin, twork.max_lin);

	found_split = true;
	
	
out:

	isi_error_handle(error, error_out);
	return found_split;
}

/*
 * This does the work split based the current work type.
 */
bool
try_btree_split(struct migr_pworker_ctx *pw_ctx, bool *last_file,
    struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct work_restart_state twork = {};
	struct isi_error *error = NULL;
	unsigned num_beg;
	unsigned num_end;
	uint64_t lin;
	bool found_split = false;
	bool found = false;
	bool force_lin_division = false;
	int add_one = 0;

	num_beg = work->num1;
	num_end = work->num2;

	UFAIL_POINT_CODE(force_lin_division_split,
	    force_lin_division = true;);

	/*
	 * We keep dividing the work range until we get valid lin range
	 */
	while (num_end && num_beg < num_end && !force_lin_division) {
		log(TRACE, "looking for key at %u-%u",num_beg,work->den2);
		found = get_work_split_key(pw_ctx, num_beg, work->den2,
		    &lin, &error);
		if (error)
			goto out;
		if (!found)
			goto out;
		if ((lin > work->min_lin) && (lin < work->max_lin) &&
		    (lin > work->lin)) {
			/* Found suitable range so allocate new work item */
			memset(&twork, 0, sizeof(struct work_restart_state));
			twork.rt = work->rt;
			twork.num1 = num_beg;
			twork.den1 = work->den1;
			twork.num2 = work->num2;
			twork.den2 = work->den2;
			twork.min_lin = lin;
			twork.chkpt_lin = lin;
			twork.max_lin = work->max_lin;
			twork.dircookie1 = twork.dircookie2 = DIR_SLICE_MIN;
			work->max_lin = lin;
			work->num2 = num_beg;

			/* Add new work item and update current work item */
			add_split_work_entry(pw_ctx, pw_ctx->wi_lin,
			    work, NULL, 0, pw_ctx->split_wi_lin, &twork,
			    NULL, 0, &error);
			if (error)
				goto out;

			log(NOTICE, "Successful split[%s], two ranges are "
			    "current work(%llu) lin1=%llx lin2=%llx, "
			    "new work(%llu) lin1=%llx lin2=%llx",
			    work_type_str(work->rt), pw_ctx->wi_lin,
			    work->min_lin, work->max_lin, pw_ctx->split_wi_lin,
			    twork.min_lin, twork.max_lin);

			found_split = true;
			break;
		}

		if ((num_beg + num_end) % 2 != 0) {
			add_one = 1;
		} else {
			add_one = 0;
		}
		
		num_beg = (num_beg + num_end)/2 + add_one;
	}

	if (!found_split) {
		/* Try to do lin based split */
		found_split = try_lin_division_split(pw_ctx, &error);
		if (error)
			goto out;
	}

	/* This worker is transferring the last LIN in its range */
	if (!found_split && work->rt == WORK_CT_LIN_UPDTS &&
	    work->lin == (work->max_lin - 1)) {
		*last_file = true;
	}
out:
	UFAIL_POINT_CODE(btree_split_fail,
	    if (work->rt == RETURN_VALUE) {
		    error = isi_system_error_new(EIO, "btree_split_fail");
		    ioerror(pw_ctx->coord, "", NULL, 0, 0, "btree_split_fail");
	    });
	isi_error_handle(error, error_out);
	return found_split;
}

static uint64_t
get_wl_cur_count(struct migr_pworker_ctx *pw_ctx, btree_key_t *key,
    struct isi_error **error_out)
{
	
	bool exists;
	uint64_t count = 0;
	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct wl_iter *iter = NULL;
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	

	ASSERT(key != NULL);

	if (cc_ctx->wl_iter == NULL)
		return 0;

	/*
	 * If there are no more entries in curlevel worklist or we have not
	 * yet started with work (work->lin == 0) , then we cannot split.
	 */
	if (!wl_iter_has_entries(cc_ctx->wl_iter) || work->lin == 0 ) {
		return 0;
	}

	ASSERT(cc_ctx->wl_iter->key.keys[1] > work->lin);
	/* We always leave the next lin for current worker */
	set_wl_key(key, pw_ctx->wi_lin, work->wl_cur_level,
	    cc_ctx->wl_iter->key.keys[1] + 1);

	iter = new_wl_iter(cc_ctx->wl, key);
	while (iter) {
		exists = wl_iter_next(NULL, iter, &lin, NULL, NULL, &error);
		if (error || !exists)
			goto out;
		
		count++;
	}

out:
	if (iter)
		close_wl_iter(iter);
	isi_error_handle(error, error_out);
	return count;
}


/*
 * This does the worklist split.
 */
bool
try_worklist_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state child = {};
	struct isi_error *error = NULL;
	bool found_split = false;
	uint64_t wl_count;
	btree_key_t key;

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	wl_count = get_wl_cur_count(pw_ctx, &key, &error);
	if (error)
		goto out;

	/* We cant split if there are less than 2 entries */
	if (wl_count < 2) {
		return false;
	}

	/* First create new child work item */
	child.rt = work->rt;
	child.dircookie1 = child.dircookie2 = DIR_SLICE_MIN;
	set_work_entry(pw_ctx->split_wi_lin, &child, NULL, 0, cc_ctx->rep2,
	    &error);
	if (error)
		goto out;

	/* Now move wl_count/2 lins to new wid */
	log(TRACE, "Move %llu entries from worklist %llu to worklist %llu",
	    wl_count/2, pw_ctx->wi_lin, pw_ctx->split_wi_lin);

	wl_move_entries(cc_ctx->wl, &key, wl_count/2, pw_ctx->wi_lin,
	    pw_ctx->split_wi_lin, &error);
	if (error)
		goto out;

	log(NOTICE, "Successful split[%s], Moved %llu lins from "
	    "current work(%llu) to new work(%llu)",
	    work_type_str(work->rt), wl_count/2, pw_ctx->wi_lin,
	    pw_ctx->split_wi_lin);

	found_split = true;

out:
	UFAIL_POINT_CODE(worklist_split_fail,
	    if (work->rt == RETURN_VALUE) {
		    error = isi_system_error_new(EIO, "worklist_split_fail");
		    ioerror(pw_ctx->coord, "", NULL, 0, 0, "worklist_split_fail");
	    });
	isi_error_handle(error, error_out);

	return found_split;
}

void
stf_continue_lin_deletes(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct generic_msg msg = {};
	struct delete_lin_msg *dlinp = &msg.body.delete_lin;

	log(TRACE, "stf_continue_lin_deletes");

	msg.head.type = DELETE_LIN_MSG;
	dlinp->slin = pw_ctx->cur_lin;
	msg_send(pw_ctx->sworker, &msg);
	pw_ctx->outstanding_acks++;

	UFAIL_POINT_CODE(gensig_exit_lin_del,
	    if (pw_ctx->outstanding_acks < (STF_ACK_INTERVAL / 2) &&
	        pw_ctx->outstanding_acks > 0) {
	        process_gensig_exit(pw_ctx);
	        return;
	    }
	);

	if (pw_ctx->rep_entry.is_dir)
		pw_ctx->stf_cur_stats->dirs->deleted->src++;
	else
		pw_ctx->stf_cur_stats->files->deleted->src++;
}
static void
stf_continue_stale_delete(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct generic_msg m = {};
	struct isi_error *error = NULL;
	bool finished;
	int fd = -1;
	struct stat st;
	int ret = -1;

	log(TRACE, "%s", __func__);
	finished = migr_continue_delete(pw_ctx, pw_ctx->plan, false, &error);
	if (error)
		goto out;

	if (finished) {
		m.head.type=STALE_DIR_MSG;
		m.body.stale_dir.src_dlin = pw_ctx->cur_lin;
		m.body.stale_dir.is_start = 0;
		fd = ifs_lin_open(pw_ctx->cur_lin, pw_ctx->cur_snap,
		    O_RDONLY);
		if (fd == -1) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %llx", pw_ctx->cur_lin);
			goto out;
		}
		ret = fstat(fd, &st);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "Unable to stat lin %llx", pw_ctx->cur_lin);
			goto out;
		}
		m.body.stale_dir.atime_sec = st.st_atimespec.tv_sec;
		m.body.stale_dir.atime_nsec = st.st_atimespec.tv_nsec;
		m.body.stale_dir.mtime_sec = st.st_mtimespec.tv_sec;
		m.body.stale_dir.mtime_nsec = st.st_mtimespec.tv_nsec;	
		msg_send(pw_ctx->sworker, &m);
		migr_call_next(pw_ctx, stf_continue_change_transfer);
	}

out:
	if (fd != -1) {
		close(fd);
	}

	isi_error_handle(error, error_out);

}

static void
stf_continue_stale_dirs(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool found;
	struct rep_entry rep;
	int fd = -1;
	int tmp_errno;
	struct generic_msg m = {};

	log(TRACE, "stf_continue_stale_dirs");
	found = get_entry(pw_ctx->cur_lin, &rep, pw_ctx->cur_rep_ctx, &error);
	if (error)
		goto out;

	if (!found || rep.lcount > 0) {
		fd = ifs_lin_open(pw_ctx->cur_lin, pw_ctx->cur_snap,
		    O_RDONLY);
		ASSERT(fd != 0);
		if (fd == -1) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Unable to open %llx", pw_ctx->cur_lin);
			goto out;
		}
		pw_ctx->delete_dir = migr_dir_new(fd, NULL, true, &error);
		if (error)
			goto out;
		fd = -1;
		listreset(&pw_ctx->delete_filelist);
		
		migr_call_next(pw_ctx, stf_continue_stale_delete);
		m.head.type=STALE_DIR_MSG;
		m.body.stale_dir.src_dlin = pw_ctx->cur_lin;
		m.body.stale_dir.is_start = 1;
		msg_send(pw_ctx->sworker, &m);
	}


 out:
	if (fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
}

/*
 * Constructs a LINK_MSG.
 */
static void
construct_link_message(struct migr_pworker_ctx *pw_ctx, struct generic_msg *m,
    ifs_lin_t parent_lin, struct dirent *d, bool new_singly_linked,
    bool conflict_res, bool skip_chkpt, bool chkpt_child_lin,
    struct isi_error **error_out)
{
	struct link_msg *lm = &m->body.link;
	struct isi_error *error = NULL;

	m->head.type = LINK_MSG;
	lm->dirlin = parent_lin;
	lm->d_lin = d->d_fileno;
	lm->d_name = d->d_name;
	lm->d_enc = d->d_encoding;
	lm->d_type = d->d_type;

	/* If dirent is special type, then read extra meta data. */
	lm->symlink = NULL;
	is_special_file_link(pw_ctx, lm, &error);
	if (error)
		goto out;

	/* Set flags. */
	if (new_singly_linked)
		lm->flags |= LINK_MSG_NEW_SINGLY_LINKED;
	if (conflict_res)
		lm->flags |= LINK_MSG_CONFLICT_RES;
	if (skip_chkpt)
		lm->flags |= LINK_MSG_SKIP_LIN_CHKPT;
	if (chkpt_child_lin)
		lm->flags |= LINK_MSG_CHKPT_CHILD_LIN;

out:
	isi_error_handle(error, error_out);
}

/*
 * Constructs and sends a LINK_MSG to the sworker.
 */
void
send_link_message(struct migr_pworker_ctx *pw_ctx, ifs_lin_t parent_lin,
    struct dirent *d, bool new_singly_linked, bool conflict_res,
    bool skip_chkpt, bool chkpt_child_lin, struct isi_error **error_out)
{
	struct generic_msg msg = {};
	struct link_msg *lm = &msg.body.link;
	struct isi_error *error = NULL;

	construct_link_message(pw_ctx, &msg, parent_lin, d, new_singly_linked,
	    conflict_res, skip_chkpt, chkpt_child_lin, &error);
	if (error)
		goto out;

	msg_send(pw_ctx->sworker, &msg);
	if (!skip_chkpt)
		pw_ctx->outstanding_acks++;

 out:
	if (lm->symlink) {
		free(lm->symlink);
		lm->symlink = NULL;
	}
	isi_error_handle(error, error_out);
}

/*
 * Function to get extra information when handling link call for special files
 */
void
is_special_file_link(struct migr_pworker_ctx *pw_ctx, struct link_msg *lp,
    struct isi_error **error_out)
{
	int fd = -1;
	int tmp_errno;
	enc_t sym_enc = ENC_DEFAULT;
	int ret;
	char *path = NULL;
	struct stat st;
	char sym_target[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	switch (lp->d_type) {
	case DT_FIFO:
	case DT_DIR:
	case DT_REG:
	case DT_SOCK:
		break;

	case DT_LNK:
		/* Read the symlink information using parent lin*/
		fd = ifs_lin_open(lp->dirlin, pw_ctx->cur_snap, O_RDONLY);
		if (fd < 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(lp->dirlin,
			    pw_ctx->cur_snap);
			error = isi_system_error_new(tmp_errno,
			    "Unable to open %s", path);
			goto out;
		}
		ASSERT(fd != 0);
		ret = enc_readlinkat(fd, lp->d_name,
				    lp->d_enc, sym_target,
				    MAXPATHLEN, &sym_enc);
		if (ret == -1) {
			error = isi_system_error_new(errno,
			    "Failed to read symlink %s", lp->d_name);
			goto out;
		}
		sym_target[ret] = 0;
		lp->symlink = strdup(sym_target);
		ASSERT(lp->symlink);
		lp->senc = sym_enc;
		break;

	case DT_CHR:
	case DT_BLK:
		/* Read the major and minor numbers from inode */
		fd = ifs_lin_open(lp->d_lin, pw_ctx->cur_snap, O_RDONLY);
		if (fd < 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(lp->d_lin, pw_ctx->cur_snap);
			error = isi_system_error_new(tmp_errno,
			    "Unable to open %s", path);
			goto out;
		}
		ASSERT(fd != 0);

		ret = fstat(fd, &st);
		if (ret != 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(lp->d_lin, pw_ctx->cur_snap);
			error = isi_system_error_new(tmp_errno,
			    "Unable to stat %s", path);
			goto out;
		}
			
		sprintf(sym_target, "%u %u",
		    major(st.st_rdev), minor(st.st_rdev));
		lp->symlink = strdup(sym_target);
		ASSERT(lp->symlink);
		break;

	default:
		ASSERT(0);
	}

out:
	if (fd > 0)
		close(fd);
	if (path)
		free(path);
	isi_error_handle(error, error_out);
}

void
stf_continue_lin_unlink(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *chg_ctx = pw_ctx->chg_ctx;
	uint64_t cur_lin = pw_ctx->cur_lin;
	struct rep_entry rep_entry = pw_ctx->rep_entry;
	struct isi_error *error = NULL;
	struct migr_dirent *de = NULL;
	struct migr_dir *md_out = NULL;
	struct generic_msg msg = {};
	struct unlink_msg *ulp = &msg.body.unlink;
	enum dirent_mod_type modtype;
	uint64_t dircookie2, dircookie1;
	bool found = false;
	struct rep_entry entry;
	struct siq_stat *stats = pw_ctx->stf_cur_stats;

	log(TRACE, "stf_continue_lin_unlink");

	dircookie1 = dircookie2 = DIR_SLICE_MIN;

	while ((de = find_modified_dirent(pw_ctx, cur_lin,
	    rep_entry.not_on_target, &dircookie2, false,
	    &dircookie1, &modtype, &md_out, false, &error))) {

		found = get_entry(de->dirent->d_fileno,
		    &entry, chg_ctx->rep2, &error);
		if (error)
			goto out;
		ASSERT(found == true,
		    "Entry for lin %{} not found in incremental repstate. "
		    "Parent dir on target = %d.",
		    lin_fmt(de->dirent->d_fileno), rep_entry.not_on_target);
		if (pw_ctx->restore) {
			revert_entry(pw_ctx, de->dirent->d_fileno, &entry,
			    &error);
			if (error)
				goto out;
		}
		if (modtype == DENT_REMOVED) {
			/*
			 * If the file/dir is deleted from the job
			 * perspective, we dont need to send UNLINK
			 * since
			 * a) for syncs, file/dir delete phase will
			 *    remove lin and all associated links.
			 * b) for copy, we want to preserve them
			 */
			if ((entry.lcount - entry.excl_lcount) == 0) {
				/* No need to send UNLINK */
				migr_dir_unref(md_out, de);
				de = NULL;
				continue;
			}

			msg.head.type = UNLINK_MSG;
			ulp->dirlin = cur_lin;
			ulp->d_lin = de->dirent->d_fileno;
			ulp->d_name = de->dirent->d_name;
			ulp->d_enc = de->dirent->d_encoding;
			ulp->d_type = de->dirent->d_type;
			msg_send(pw_ctx->sworker, &msg);
			pw_ctx->outstanding_acks++;
			if (entry.is_dir)
				stats->dirs->unlinked->src++;
			else
				stats->files->unlinked->src++;
			goto out;
		}
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	if (error)
		goto out;

	/* Clear the directory scan context */	
	clear_migr_dir(pw_ctx, 0);
	migr_call_next(pw_ctx, stf_continue_lin_link);

out:
	if (de) {
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	isi_error_handle(error, error_out);
}

void
stf_continue_lin_link(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *chg_ctx = pw_ctx->chg_ctx;
	uint64_t cur_lin = pw_ctx->cur_lin;
	struct rep_entry rep_entry = pw_ctx->rep_entry;
	struct comp_rplookup *rplookup = &pw_ctx->comp_rplookup;
	struct isi_error *error = NULL;
	struct migr_dirent *de = NULL;
	struct migr_dir *md_out = NULL;
	enum dirent_mod_type modtype;
	uint64_t dircookie2, dircookie1;
	bool found = false;
	struct rep_entry entry;
	struct siq_stat *stats = pw_ctx->stf_cur_stats;
	bool new_singly_linked_file;

	log(TRACE, "%s cur_lin: %{}", __func__, lin_fmt(cur_lin));

	UFAIL_POINT_CODE(pause_lin_link,
		if (RETURN_VALUE > 0) {
			siq_nanosleep(0, RETURN_VALUE * 1000000);
		}
		return;
	);

	UFAIL_POINT_CODE(gensig_exit_dir_links,
		if (pw_ctx->outstanding_acks < (STF_ACK_INTERVAL / 2) &&
		    pw_ctx->outstanding_acks > 0) {
			process_gensig_exit(pw_ctx);
			return;
		}
	);

	dircookie1 = dircookie2 = DIR_SLICE_MIN;

	while ((de = find_modified_dirent(pw_ctx, cur_lin,
	    rep_entry.not_on_target, &dircookie2, false,
	    &dircookie1, &modtype, &md_out, false, &error))) {

		found = get_entry(de->dirent->d_fileno,
		    &entry, chg_ctx->rep2, &error);
		if (error)
			goto out;
		ASSERT(found == true,
		    "Entry for lin %{} not found in incremental repstate. "
		    "Parent dir on target = %d.",
		    lin_fmt(de->dirent->d_fileno), rep_entry.not_on_target);
		if (pw_ctx->restore) {
			revert_entry(pw_ctx, de->dirent->d_fileno, &entry,
			    &error);
			if (error)
				goto out;
		}
		if (modtype == DENT_ADDED) {
			/*
			 * New directories in compliance syncs need
			 * ordering since we dont allow move operation.
			 * Checks done in order are.
			 * If parent is a new lin (not_on_target) and
			 * We have already not processed the parent once i.e
			 * optimization to reduce parent lookup to once per
			 * directory.
			 */
			if ((pw_ctx->compliance_v2 ||
			    (pw_ctx->compliance && pw_ctx->comp_v1_v2_patch)) &&
			    rep_entry.not_on_target &&
			    rplookup->dir_lin != cur_lin) {
				stf_comp_rplookup_lin(pw_ctx, cur_lin,
				    de->dirent, entry.lcount, false);
				goto out;
			}

			new_singly_linked_file = (entry.not_on_target &&
			    !entry.is_dir && entry.lcount == 1);

			send_link_message(pw_ctx, cur_lin, de->dirent,
			    new_singly_linked_file, false, false, false,
			    &error);
			if (error)
				goto out;

			if (entry.is_dir)
				stats->dirs->linked->src++;
			else
				stats->files->linked->src++;
			goto out;
		}
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	if (error)
		goto out;

	/* Clear the directory scan context */	
	clear_migr_dir(pw_ctx, 0);
	migr_call_next(pw_ctx, stf_continue_change_transfer);

out:
	if (de) {
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	isi_error_handle(error, error_out);
}

/*
 * Send LIN_COMMIT message to secondary
 * Also free pw_ctx structures used for lin
 */
static int
send_lin_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct rep_entry  rep_entry  = pw_ctx->rep_entry;
	struct isi_error *error = NULL;
	struct migr_file_state *fstate = &pw_ctx->file_state;
	uint64_t slin = pw_ctx->cur_lin;
	struct generic_msg msg = {};
	struct lin_commit_msg *lcmp = &msg.body.lin_commit;

	// Force the worker to exit if failpoint is set
	UFAIL_POINT_CODE(force_worker_exit_on_cond,
	    if (pw_ctx->outstanding_acks < 5000) process_gensig_exit(pw_ctx);
	    return false;
	);

	log(TRACE, "send_lin_commit");
	ASSERT(slin > 0);

	msg.head.type = LIN_COMMIT_MSG;
	lcmp->slin = slin;

	lcmp->size_changed = fstate->size_changed;

	if (!rep_entry.is_dir && pw_ctx->file_state.hash_str)
		lcmp->hash_str = pw_ctx->file_state.hash_str;

	msg_send(pw_ctx->sworker, &msg);
	pw_ctx->outstanding_acks++;

	if (!pw_ctx->restore && rep_entry.exception) {
		pw_ctx->stf_cur_stats->change_transfer->hash_exceptions_fixed++;
	}

	/* Reset the pw_ctx values */
	if (rep_entry.is_dir) {
		migr_dir_free(pw_ctx->dir_state.dir);
		pw_ctx->dir_state.dir = NULL;
	} else {
		if (fstate->hash_str) {
			/* We did compute hash */
			rep_entry.hash_computed = true;
			set_entry(slin, &rep_entry, pw_ctx->cur_rep_ctx, 0,
			    NULL, &error);
			if (error)
				goto out;
		}
		if (fstate->hash_str)
			free(fstate->hash_str);
		fstate->hash_str = NULL;
	}
out:
	isi_error_handle(error, error_out);
	return 0;
}

/*
 */
static void
send_extra_file_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct generic_msg msg = {};

	log(TRACE, "send_extra_file_commit");

	msg.head.type = EXTRA_FILE_COMMIT_MSG;
	msg_send(pw_ctx->sworker, &msg);

	isi_error_handle(error, error_out);
}

int
extra_file_commit_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;

	ASSERT(pw_ctx->doing_extra_file);

	if (pw_ctx->stf_sync &&
	    !(pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync)) {
		stf_request_new_work(pw_ctx, &error);
		if (error)
			log(FATAL, "Failed to request new work: %s",
			    isi_error_get_message(error));
	} else {
		tw_request_new_work(pw_ctx);
	}

	pw_ctx->doing_extra_file = false;

	return 0;
}

void
stf_continue_lin_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "stf_continue_lin_commit");

	/* Send Commit message */
	send_lin_commit(pw_ctx, &error);
	if (error)
		goto out;

	migr_call_next(pw_ctx, stf_continue_change_transfer);

out:
	isi_error_handle(error, error_out);
}

static void
stf_continue_extra_file_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "stf_continue_lin_commit");

	/* Send Commit message */
	send_extra_file_commit(pw_ctx, &error);
	if (error)
		goto out;

	if (!pw_ctx->assess)
		migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
	else
		extra_file_commit_callback(NULL, pw_ctx);
out:
	isi_error_handle(error, error_out);
}


/*
 * Send all file data information
 */
void
stf_continue_lin_data(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "stf_continue_lin_data");

	migr_continue_generic_file(pw_ctx, &pw_ctx->file_state,
	    stf_continue_lin_commit, error_out);
}

/*
 * Send all file data information
 */
static void
stf_continue_extra_file_data(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "stf_continue_extra_file_data");

	migr_continue_generic_file(pw_ctx, &pw_ctx->file_state,
	    stf_continue_extra_file_commit, error_out);
}



void
stf_continue_lin_ads(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct rep_entry  rep_entry  = pw_ctx->rep_entry;
	struct isi_error *error = NULL;

	log(TRACE, "stf_continue_lin_ads");

	/* If file, migr_continue_ads_dir_for_file */
	/* If dir, migr_continue_ads_dir_for_dir */
	/* Reset next_func in migr_finish_ads_dir */

	if (rep_entry.is_dir)
		migr_continue_ads_dir_for_dir(pw_ctx, &error);
	else
		migr_continue_ads_dir_for_file(pw_ctx, &error);

	isi_error_handle(error, error_out);
}


void
migr_send_idmap(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	int ret;
	char fn[MAXPATHLEN + 1];
	char source_fn[MAXPATHLEN + 1];
	char source_cmd[MAXPATHLEN + 34];

	log(TRACE, "migr_send_idmap");

	sprintf(source_cmd, "mkdir -p %s", SIQ_IFS_IDMAP_DIR);
	/*
	 * Ignore failures.  If there was an issue with the directory, then
	 * it'll probably show up in the dump step.
	 */
	system(source_cmd);

	snprintf(source_fn, sizeof(source_fn), "%s/%s_source.gz",
	    SIQ_IFS_IDMAP_DIR, pw_ctx->policy_id);
	snprintf(source_cmd, sizeof(source_cmd),
	    "isi auth mapping dump | gzip > '%s'", source_fn);
	ret = system(source_cmd);
	if (ret != 0) {
		error = isi_system_error_new(errno, "Bad return %d dumping "
		    "idmap", ret);
		goto out;
	}
	ret = lstat(source_fn, &st);

	if (ret != 0) {
		error = isi_system_error_new(errno, "Didn't find idmap");
		goto out;
	}

	/* Policy names should never contain "/" or "." */
	ASSERT(strchr(pw_ctx->job_name, '.') == NULL);
	ASSERT(strchr(pw_ctx->job_name, '/') == NULL);

	/* Bug 186554 - truncate policy name to 100 characters */
	snprintf(fn, sizeof(fn), "%.100s_%s.gz", pw_ctx->job_name,
	    pw_ctx->policy_id);

	send_extra_file(pw_ctx, st.st_ino, st.st_snapid, fn,
	    ENC_DEFAULT, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}


/*
 * Copy stat attributes to lin_update_msg
 */
static void
copy_stat_attributes(struct lin_update_msg *lump, struct stat *stp)
{
	lump->file_type = stat_mode_to_file_type(stp->st_mode);
	lump->mode = stp->st_mode;
	lump->uid = stp->st_uid;
	lump->gid = stp->st_gid;
	lump->atime_sec = stp->st_atimespec.tv_sec;
	lump->atime_nsec = stp->st_atimespec.tv_nsec;
	lump->mtime_sec = stp->st_mtimespec.tv_sec;
	lump->mtime_nsec = stp->st_mtimespec.tv_nsec;
	lump->size  = stp->st_size;
	lump->di_flags = (unsigned)stp->st_flags;
}

/*
 * Copy stat attributes to lin_update_msg
 */
static void
ef_copy_stat_attributes(struct extra_file_msg *ef, struct stat *stp)
{
	ef->file_type = stat_mode_to_file_type(stp->st_mode);
	ef->mode = stp->st_mode;
	ef->uid = stp->st_uid;
	ef->gid = stp->st_gid;
	ef->atime_sec = stp->st_atimespec.tv_sec;
	ef->atime_nsec = stp->st_atimespec.tv_nsec;
	ef->mtime_sec = stp->st_mtimespec.tv_sec;
	ef->mtime_nsec = stp->st_mtimespec.tv_nsec;
	ef->size  = stp->st_size;
	ef->di_flags = (unsigned)stp->st_flags;
}


/*
 * Send lin_update_msg to sworker.
 * Also set the pw_ctx so that further calls can use
 * the context values.
 */
static int
send_lin_update(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct rep_entry rep_entry = pw_ctx->rep_entry;
	struct generic_msg msg = {};
	struct lin_update_msg *lump = &msg.body.lin_update;
	struct migr_file_state *fstate = &pw_ctx->file_state;
	struct migr_dir_state *dstate = &pw_ctx->dir_state;
	uint64_t slin = pw_ctx->cur_lin;
	struct isi_error *error = NULL;
	uint64_t parent_lin;
	struct dirent dentry;
	enc_t sym_enc = ENC_DEFAULT;
	char sym_target[MAXPATHLEN + 1];
	int fd = -1, oldfd = -1, parent_fd = -1, ret = -1;
	bool found = false;
	struct stat st, oldst;
	enum file_type type;
	struct siq_stat *stats = pw_ctx->stf_cur_stats;
	int tmp_errno;
	int attr_fd = -1;
	char *path = NULL;
	struct worm_state worm = {};
	bool have_worm = false;
	bool commit_n_modify = false;

	enum ifs_security_info secinfo = 0;
	ifs_snapid_t prev_snap = pw_ctx->prev_snap;

	log(TRACE, "send_lin_update");
	ASSERT(slin > 0);

	/* Delay if a files per second limit is active */
	throttle_delay(pw_ctx);
	UFAIL_POINT_CODE(stf_lin_die,
	    if (RETURN_VALUE == (slin & 0xFFFFFFFF)) {
		error = isi_system_error_new(EDOOFUS, "Failpoint triggered "
		    "for LIN %llx", slin);
		goto out;
	    });

	/*
	 * Open the snap2 version of lin and read all attributes.
	 * Also set all pworker context values
	 */
	fd = ifs_lin_open(slin, pw_ctx->cur_snap, O_RDONLY);
	if (fd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(slin, pw_ctx->cur_snap);
		error = isi_system_error_new(tmp_errno,
		    "Unable to open %s", path);
		free(path);
		goto out;
	}
	ASSERT(fd != -1);

	ret = fstat(fd, &st);
	if (ret != 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(slin, pw_ctx->cur_snap);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat %s", path);
		free(path);
		goto out;
	}

	if (!pw_ctx->restore && rep_entry.lcount > st.st_nlink) {
		error = isi_system_error_new(EINVAL, "SyncIQ change compute "
		    "error detected. Computed refcount for LIN %llx (%u) "
		    "is greater than the refcount in the latest snapshot (%u)",
		    slin, rep_entry.lcount, st.st_nlink);
		goto out;
	}

	type = stat_mode_to_file_type(st.st_mode);
	ASSERT(type != SIQ_FT_UNKNOWN);

	msg.head.type = LIN_UPDATE_MSG;

	lump->slin = pw_ctx->cur_lin;
	copy_stat_attributes(lump, &st);

	memset(&worm, 0, sizeof(struct worm_state));
	have_worm = get_worm_state(pw_ctx->cur_lin, pw_ctx->cur_snap, &worm,
	    NULL, &error);
	if (error)
		goto out;

	lump->worm_committed = (unsigned)worm.w_committed;

	// This results in a loss of precision for 64-bit retention dates.
	// WORM_STATE_MSG corrects this for targets that support Mavericks and
	// later. For earlier targets, if the date is later than MAX_INT32,
	// set it to MAX_INT32-- the latest we can safely set it to.
	lump->worm_retention_date = worm.w_retention_date <= INT32_MAX ? 
	    (unsigned)worm.w_retention_date : INT32_MAX;


	if (st.st_flags & (SF_HASNTFSACL | SF_HASNTFSOG)) {
		secinfo = IFS_SEC_INFO_OWNER | IFS_SEC_INFO_GROUP;
		if (st.st_flags & SF_HASNTFSACL)
			secinfo |= IFS_SEC_INFO_DACL | IFS_SEC_INFO_SACL;

		lump->acl = get_sd_text_secinfo(fd, secinfo);
		if (lump->acl == NULL) {
			tmp_errno = errno;
			path = get_valid_rep_path(slin, pw_ctx->cur_snap);
			error = isi_system_error_new(tmp_errno,
			    "Unable to read acl %s", path);
			free(path);
			goto out;
		}
	}
	
	/* Skip computing stats if this is a hash exception 
	 * because it was already counted. */
	if (pw_ctx->restore || !rep_entry.exception) {
		switch (type) {
		case SIQ_FT_DIR:
			stats->dirs->replicated++;
			break;

		case SIQ_FT_REG:
			stats->files->replicated->regular++;
			break;

		case SIQ_FT_SOCK:
			stats->files->replicated->sockets++;
			break;

		case SIQ_FT_FIFO:
			stats->files->replicated->fifos++;
			break;

		case SIQ_FT_SYM:
			found = dirent_lookup_by_lin(pw_ctx->cur_snap, slin, 0,
			    &parent_lin, &dentry, &error);
			if (error)
				goto out;

			if (!found) {
				path = get_valid_rep_path(slin, 
				    pw_ctx->cur_snap);
				errno = ENOENT;
				ret = -1;
				error = isi_system_error_new(errno,
				    "couldnt find parent entry for %s", path);
				free(path);
				goto out;
			}

			parent_fd = ifs_lin_open(parent_lin, pw_ctx->cur_snap,
			    O_RDONLY);
			if (parent_fd < 0) {
				tmp_errno = errno;
				path = get_valid_rep_path(parent_lin, 
				    pw_ctx->cur_snap);
				error = isi_system_error_new(tmp_errno,
				    "Unable to open %s", path);
				free(path);
				goto out;
			}
			ASSERT(parent_fd != 0);
			ret = enc_readlinkat(parent_fd, dentry.d_name,
					    dentry.d_encoding, sym_target,
					    MAXPATHLEN, &sym_enc);
			if (ret == -1) {
				error = isi_system_error_new(errno,
				    "Failed to read symlink %s", dentry.d_name);
				goto out;
			}
			sym_target[ret] = 0;
			lump->symlink = sym_target;
			lump->senc = sym_enc;
			stats->files->replicated->symlinks++;
			break;

		case SIQ_FT_BLOCK:
			sprintf(sym_target, "%u %u",
			    major(st.st_rdev), minor(st.st_rdev));
			lump->symlink = sym_target;
			stats->files->replicated->block_specs++;
			break;

		case SIQ_FT_CHAR:
			sprintf(sym_target, "%u %u",
			    major(st.st_rdev), minor(st.st_rdev));
			lump->symlink = sym_target;
			stats->files->replicated->char_specs++;
			break;

		default:
			error = isi_system_error_new(EINVAL,
			    "Unsupported file type encountered");
			goto out;
		}

		if (type != SIQ_FT_DIR && !is_fsplit_work_item(pw_ctx->work))
			stats->files->total++;
	}

	/* Set the values in pw_ctx */
	if (rep_entry.is_dir) {
		ASSERT(dstate->dir == NULL);
		dstate->dir = migr_dir_new(fd, NULL, true, &error);
		if (error) {
			tmp_errno = errno;
			path = get_valid_rep_path(st.st_ino, pw_ctx->cur_snap);
                        isi_error_free(error);
			error = isi_system_error_new(tmp_errno,
			    "Unable to allocate new directory state for %s",
			    path);
			ret = -1;
			free(path);
			goto out;
		}
		fd = -1;
		dstate->st = st;
		attr_fd = migr_dir_get_fd(dstate->dir);
	} else {
		ASSERT(fstate->fd == -1);
		fstate->fd = fd;
		fd = -1;
		fstate->st = st;
		ASSERT_IMPLIES(!is_fsplit_work_item(pw_ctx->work),
		    pw_ctx->work->f_low == 0);
		fstate->cur_offset = pw_ctx->work->f_low;
		fstate->name = NULL;
		fstate->enc = ENC_NOVAL;
		fstate->type = type;
		fstate->skip_data = false;
		fstate->hard_link = false;

		/* We can skip hash calculation if its already done once */
		lump->skip_bb_hash = fstate->skip_bb_hash = 
		    !rep_entry.non_hashed;

		/*
		 * If its a modified file and committed, then a restore
		 * would have re-created a new lin in earlier phases.
		 * Hence do a full sync of file using old snapshot.
		 * NOTE: There is some magic happening here. The snapshots
		 * that restore uses in cur_snap and prev_snap are actually
		 * flipped. See snap_switched in this file. Hence we need
		 * actually use prev_snap (which points to new snap) to check
		 * if the file was committed. If so we need to go back to
		 * LKG state which is old snapshot which existed prior
		 * to restore.
		 */
		if (pw_ctx->compliance_v2 && pw_ctx->restore &&
		    !rep_entry.not_on_target) {
			commit_n_modify = is_conflict_res_needed(
			    pw_ctx->cur_lin, pw_ctx->cur_snap,
			    pw_ctx->prev_snap, &error);
			if (error)
				goto out;
		}

		/*
		 * If file is not on target (either new file or predicate
		 * change) OR there is hash exception then we better send
		 * entire file
		 */
		if (rep_entry.not_on_target || (!pw_ctx->restore && 
		    rep_entry.exception) || commit_n_modify) {
			fstate->prev_snap = INVALID_SNAPID;
			lump->full_transfer = true;
			pw_ctx->stf_cur_stats->files->replicated->new_files++;
		} else {
			fstate->prev_snap = pw_ctx->prev_snap;
		}

		if (rep_entry.not_on_target ||
		    fstate->prev_snap == INVALID_SNAPID || commit_n_modify) {
			fstate->size_changed = true;
		} else if (rep_entry.changed) {
			if (pw_ctx->restore)
				prev_snap = HEAD_SNAPID;
			oldfd = ifs_lin_open(slin, prev_snap, O_RDONLY);
			if (oldfd < 0) {
				tmp_errno = errno;
				path = get_valid_rep_path(slin, prev_snap);
				error = isi_system_error_new(tmp_errno,
				    "Unable to open %s in snap %llu", path,
				    prev_snap);
				free(path);
				goto out;
			}
			ASSERT(oldfd != 0);
			ret = fstat(oldfd, &oldst);
			if (ret != 0) {
				tmp_errno = errno;
				path = get_valid_rep_path(slin, prev_snap);
				error = isi_system_error_new(tmp_errno,
				    "Unable to stat %s in snap %llu", path,
				    prev_snap);
				free(path);
				goto out;
			}
			fstate->size_changed = (st.st_size != oldst.st_size);
			if (pw_ctx->restore && st.st_size != oldst.st_size) {
				/*
				 * Set exception bit so that we would go
				 * through again since bb hash code would have
				 * missed it. The exception code would again
				 * go through the bb hash code and do the 
				 * the transfer. Note that it would still be 
				 * changes and not fll full transfer.
				 */
				set_hash_exception(pw_ctx, slin, &error);
				if (error)
					goto out;
				fstate->skip_data = true;
			}
			pw_ctx->stf_cur_stats->files->replicated->updated_files++;
		} else {
			fstate->size_changed = false;
		}

		ASSERT(fstate->bb_hash_ctx == NULL);
		attr_fd = fstate->fd;
	}

	log(COPY, "Sending update for %s%s lin %llx",
	    file_type_to_name(type), rep_entry.is_dir ? "" : " file", slin);

	/* This can modify the msg flags on deep copy. */
	set_cloudpools_sync_type(pw_ctx, fstate->fd, fstate, type, &st,
	    &lump->di_flags, &error);
	if (error)
		goto out;

	/* Tell the sworker not to clear its ADS since we won't resend it. */
	if (pw_ctx->compliance_v2 && pw_ctx->restore) {
		lump->flag |= comp_skip_ads(pw_ctx->cur_lin, pw_ctx->cur_snap,
		    pw_ctx->prev_snap, &error) ? LIN_UPDT_MSG_SKIP_ADS : 0;
		if (error)
			goto out;
	}

	/* Don't clear the ADS entries if this worker won't be working on
	 * them. With file split work items, only the worker whose range starts
	 * at 0 will send ads data. */
	if (is_fsplit_work_item(pw_ctx->work) && pw_ctx->work->f_low != 0)
		lump->flag |= LIN_UPDT_MSG_SKIP_ADS;

	msg_send(pw_ctx->sworker, &msg);

	send_user_attrs(pw_ctx, attr_fd, rep_entry.is_dir, &error);
	if (error)
		goto out;

	do_cloudpools_setup(pw_ctx, fstate->fd, fstate, type, &st, &error);
	if (error)
		goto out;

	if (have_worm && (pw_ctx->curr_ver & FLAG_VER_3_5))
		send_worm_state(pw_ctx, &worm);

out:
	if (lump->acl)
		free(lump->acl);

	if (parent_fd > 0)
		close(parent_fd);
	
	if (fd > 0)
		close(fd);

	if (oldfd > 0)
		close(oldfd);

	isi_error_handle(error, error_out);
	return ret;
}


/*
 * Also set the pw_ctx so that further calls can use
 * the context values.
 */
static void
send_extra_file(struct migr_pworker_ctx *pw_ctx, ifs_lin_t slin,
    ifs_snapid_t ssnap, char *tgt_name, enc_t tgt_enc,
    struct isi_error **error_out)
{
	struct generic_msg msg = {};
	struct extra_file_msg *ef = &msg.body.extra_file;
	struct migr_file_state *fstate = &pw_ctx->file_state;
	struct isi_error *error = NULL;
	int fd = -1, ret = -1;
	struct stat st;
	enum file_type type;

	log(TRACE, "send_lin_update");

	fd = ifs_lin_open(slin, ssnap, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open extra file %s", tgt_name);
		goto out;
	}
	ASSERT(fd != -1);

	ret = fstat(fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat extra file %s", tgt_name);
		goto out;
	}
	type = stat_mode_to_file_type(st.st_mode);
	if (type != SIQ_FT_REG) {
		error = isi_system_error_new(EFTYPE, "Wrong filetype for "
		    "extra file %s", tgt_name);
		goto out;
	}

	msg.head.type = EXTRA_FILE_MSG;

	ef->slin = pw_ctx->cur_lin;
	ef_copy_stat_attributes(ef, &st);

	if (st.st_flags & (SF_HASNTFSACL | SF_HASNTFSOG))
		ef->acl = get_sd_text(fd);

	if (!ef->acl)
		ef->acl = get_file_acl(fd);

	ef->d_name = tgt_name;
	/* Set the values in pw_ctx */
	ASSERT(fstate->fd == -1);
	fstate->fd = fd;
	fd = -1;
	fstate->st = st;
	fstate->cur_offset = 0;
	if (fstate->name) {
		free(fstate->name);
	}
	fstate->name = strdup(tgt_name);
	ASSERT(fstate->name);
	fstate->enc = tgt_enc;

	/* Always send the whole file */
	fstate->prev_snap = INVALID_SNAPID;
	fstate->skip_bb_hash = true;
	fstate->size_changed = true;

	ASSERT(fstate->bb_hash_ctx == NULL);
	msg_send(pw_ctx->sworker, &msg);

out:
	if (ef->acl)
		free(ef->acl);

	if (fd > 0)
		close(fd);

	if (!error)
		migr_call_next(pw_ctx, stf_continue_extra_file_data);

	isi_error_handle(error, error_out);
}


/*
 * get the next changed/new lin and send lin update msg to sworker
 */
static int
stf_lin_transfer(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct rep_entry rep_entry;
	struct isi_error *error = NULL;
	struct migr_dir_state *dir_state = &pw_ctx->dir_state;
	struct migr_file_state *file_state = &pw_ctx->file_state;
	int ret = -1;
	bool has_ads;

	ret = send_lin_update(pw_ctx, &error);
	if (error)
		goto out;

	rep_entry = pw_ctx->rep_entry;
	if (rep_entry.is_dir) {
		has_ads = migr_start_ads(pw_ctx,
		    migr_dir_get_fd(dir_state->dir), &dir_state->st, "", 0,
		    &error);
		if (error)
			goto out;
		if (has_ads == false)
			ret = STF_CONT_LIN_COMMIT;
	} else {
		has_ads = migr_start_ads(pw_ctx,
		    file_state->fd, &file_state->st, "", 0, &error);
		if (error)
			goto out;
		if (has_ads == false)
			ret = STF_CONT_LIN_DATA;
	}

	if (has_ads) {
		ret = STF_CONT_LIN_ADS;
		pw_ctx->stf_cur_stats->files->replicated->files_with_ads++;
	}

out:
	isi_error_handle(error, error_out);
	return ret;
}

/*
 * Main function which decided the next function based on state
 * information returned by stf_next_lin_update.
 */
void
stf_continue_lin_transfer(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int ret;

	log(TRACE, "stf_continue_lin_transfer");

	ret = stf_lin_transfer(pw_ctx, &error);
	if (error)
		goto out;

	switch (ret) {
	case STF_CONT_LIN_DATA:
		migr_call_next(pw_ctx, stf_continue_lin_data);
		break;

	case STF_CONT_LIN_ADS:
		migr_call_next(pw_ctx, stf_continue_lin_ads);
		break;

	case STF_CONT_LIN_COMMIT:
		migr_call_next(pw_ctx, stf_continue_lin_commit);
		break;

	default:
		ASSERT(0);
	}

out:
	isi_error_handle(error, error_out);
}

void
flush_stf_logs(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	log(TRACE, "%s", __func__);
	/*
	 * Check connection with the coordinator before 
	 * flushing stf log contents
	 */
	check_connection_with_peer(pw_ctx->coord, true, 1, &error);
	if (error) {
		log(FATAL, "%s", isi_error_get_message(error));
	}

	if (cc_ctx && summ_stf_log_entries(cc_ctx->summ_stf)) {
		/* Flush summary stf */
		flush_summ_stf_log(cc_ctx->summ_stf, &error);
		if (error)
			goto out;
	}

	/* Next we flush the repstate log */
	flush_rep_log(&pw_ctx->rep_log, pw_ctx->cur_rep_ctx, &error);
	if (error)
		goto out;

	/* Next we flush the worklist log */
	if (cc_ctx && wl_log_entries(&pw_ctx->wl_log)) {
		flush_wl_log(&pw_ctx->wl_log, cc_ctx->wl,
		    pw_ctx->wi_lin, work->wl_cur_level, &error);
		if (error)
			goto out;
	}

	/*
	 * Check connection with the coordinator before 
	 * flushing stf log contents
	 */
	check_connection_with_peer(pw_ctx->coord, true, 1, &error);
	if (error) {
		log(FATAL, "%s", isi_error_get_message(error));
	}

out:
	isi_error_handle(error, error_out);
}
