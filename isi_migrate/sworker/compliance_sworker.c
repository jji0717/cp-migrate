#include <sys/acl.h>
#include <sys/vnode.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include "isi_migrate/migr/isirep.h"
#include "sworker.h"

static void *comp_tout = NULL;

static void
comp_map_checkpoint_done(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

void
edit_target_record_comp_commit(char *policy_id, bool new_value,
    struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//Set the target record flag
	lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
	if (error) {
		goto out;
	}
	trec = siq_target_record_read(policy_id, &error);
	if (error) {
		goto out;
	}
	if (trec->do_comp_commit_work != new_value) {
		//This is not set, so do so
		log(TRACE, "%s: setting comp_commit", __func__);
		trec->do_comp_commit_work = new_value;
		siq_target_record_write(trec, &error);
		if (error) {
			goto out;
		}
	}

out:
	siq_target_record_free(trec);
	if (lock_fd != -1) {
		close(lock_fd);
		lock_fd = -1;
	}
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

static void
do_worm_commit(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out)
{
	int fd = -1;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct migr_sworker_stats_ctx *stats = &sw_ctx->stats;
	struct lin_list_entry lle = {};
	struct ifs_syncattr_args syncargs = IFS_SYNCATTR_ARGS_INITIALIZER;
	struct isi_error *error = NULL;

	log(TRACE, "%s: lin: %llx", __func__, lin);

	//Open the lin
	fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDWR);
	if (fd <= 0) {
		error = isi_system_error_new(errno,
		    "Failed to open lin (%llx) for commit", lin);
		goto out;
	}

	//Write the commit flag
	syncargs.fd = fd;
	syncargs.worm.w_committed = true;

	//Save it
	apply_attrs_to_fd(fd, NULL, &syncargs, NULL, NULL, NULL, NULL, true,
	    &error);
	if (error) {
		goto out;
	}

	//Mark the entry as done in the SBT
	lle.commit = true;
	lin_list_log_add_entry(comp_ctx->worm_commit_ctx,
	    &comp_ctx->worm_commit_log, lin, true, &lle, NULL, &error);
	if (error) {
		goto out;
	}

	//Update the committed file statistics
	if (global->initial_sync || global->stf_upgrade_sync) {
		stats->tw_cur_stats->compliance->committed_files++;
	} else {
		//This also includes comp commit on target failover
		stats->stf_cur_stats->compliance->committed_files++;
	}

out:
	if (fd >= 0) {
		close(fd);
		fd = -1;
	}
	isi_error_handle(error, error_out);

	log(TRACE, "%s: exit", __func__);
}

static void
set_lli_worm_commit_list(struct lin_list_ctx *llc, struct isi_error **error_out)
{
	struct lin_list_info_entry llie = {};
	struct isi_error *error = NULL;

	llie.type = COMMIT;
	log(TRACE, "%s: setting lin_list info", __func__);
	set_lin_list_info(&llie, llc, false, &error);
	isi_error_handle(error, error_out);
}

static struct lin_list_ctx *
create_or_open_worm_commit_list(struct migr_sworker_ctx *sw_ctx, bool create,
    struct isi_error **error_out)
{
	struct lin_list_ctx *to_return = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	bool ll_exists = false;
	bool res = false;
	struct lin_list_info_entry llie = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//Check existence and create it if it doesn't
	ll_exists = lin_list_exists(global->sync_id, LIN_LIST_NAME_WORM_COMMIT,
	    &error);
	if (error)
		goto out;

	if (!ll_exists && create) {
		log(TRACE, "%s: creating lin_list", __func__);
		create_lin_list(global->sync_id, LIN_LIST_NAME_WORM_COMMIT,
		    true, &error);
		if (error)
			goto out;
	} else if (!ll_exists && !create) {
		error = isi_siq_error_new(E_SIQ_COMP_COMMIT,
		    "Compliance Commit LIN List does not exist");
		goto out;
	}

	//Actually open the list
	log(TRACE, "%s: opening lin_list", __func__);
	open_lin_list(global->sync_id, LIN_LIST_NAME_WORM_COMMIT,
	    &to_return, &error);
	if (error)
		goto out;
	ASSERT(to_return->lin_list_fd > 0);

	if (!ll_exists && create) {
		//Since we had to create the lin_list, also set the type
		set_lli_worm_commit_list(to_return, &error);
		if (error) {
			goto out;
		}
	}

	//Verify the type is correct
	res = get_lin_list_info(&llie, to_return, &error);
	if (error)
		goto out;

	if (res)
		ASSERT(llie.type == COMMIT);
	else {
		//We might be racing the creation
		log(DEBUG, "%s: blank lin_list info, setting now", __func__);
		set_lli_worm_commit_list(to_return, &error);
		if (error) {
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
	return to_return;
}

void
comp_commit_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (!comp_ctx->process_work || comp_ctx->rt != WORK_COMP_COMMIT) {
		goto out;
	}

	//Flush bulk log
	flush_lin_list_log(&comp_ctx->worm_commit_log,
	    comp_ctx->worm_commit_ctx, &error);
	if (error) {
		goto out;
	}

	//Send message
	resp.head.type =
	    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
	    1, comp_ctx->comp_target_current_lin);
	ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 0);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
	    1, INVALID_LIN);
	msg_send(sw_ctx->global.primary, &resp);
	log(DEBUG, "%s: sent checkpoint current lin %llx "
	    "done 0 split_lin: %llx", __func__,
	    comp_ctx->comp_target_current_lin, INVALID_LIN);

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

int
comp_commit_begin_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct lin_list_ctx *comp_commit_ctx = NULL;
	bool ll_exists = false;
	ifs_lin_t min_lin = 0;
	ifs_lin_t max_lin = 0;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	comp_ctx->rt = WORK_COMP_COMMIT;

	//This should be called by the first pworker->sworker interaction

	//Set the target record flag
	edit_target_record_comp_commit(sw_ctx->global.sync_id, true, &error);
	if (error) {
		goto out;
	}

	//Send back a work range message after figuring out what
	//the min/max range is
	//First, if the lin_list doesn't exist, we have nothing to do
	ll_exists = lin_list_exists(sw_ctx->global.sync_id,
	    LIN_LIST_NAME_WORM_COMMIT, &error);
	log(DEBUG, "%s: policy_id: %s ll_exists: %d", __func__,
	    sw_ctx->global.sync_id, ll_exists);
	if (ll_exists) {
		log(TRACE, "%s: open lin_list", __func__);
		comp_commit_ctx =
		    create_or_open_worm_commit_list(sw_ctx, false, &error);
		if (error) {
			goto out;
		}

		min_lin = get_lin_list_min_lin(comp_commit_ctx, &error);
		if (error) {
			goto out;
		}

		max_lin = get_lin_list_max_lin(comp_commit_ctx, &error);
		if (error) {
			goto out;
		}
		log(TRACE, "%s: min_lin: %llx max_lin: %llx", __func__,
		    min_lin, max_lin);

		if (max_lin == INVALID_LIN)
			min_lin = max_lin = 0;
		else {
			// We need to send max_lin + 1 so that the source will
			// process max_lin. In regular change transfer work
			// splitting, max_lin is set to -1 (uint64_t) and not
			// the actual max lin like it is here.
			max_lin++;
			ASSERT(min_lin < max_lin, "%llx > %llx", min_lin,
			    max_lin);
		}
	}
	//If we have nothing to do, min and max lins will be 0

	//Send message
	resp.head.type = build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MIN_LIN,
	    1, min_lin);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MAX_LIN,
	    1, max_lin);
	msg_send(sw_ctx->global.primary, &resp);

	log(DEBUG, "%s: sent work range min %llx max+1 %llx", __func__,
	    min_lin, max_lin);

out:
	if (error) {
		send_comp_target_error(sw_ctx->global.primary,
		    "Failure occurred starting compliance commits",
		    E_SIQ_COMP_COMMIT, error);
		isi_error_free(error);
	}

	if (comp_commit_ctx)
		close_lin_list(comp_commit_ctx);

	log(TRACE, "%s exit", __func__);
	return 0;
}

static void
comp_target_work_range_process(struct generic_msg *m, void *ctx,
    migr_timeout_t func, struct isi_error **error_out)
{
	enum work_restart_type rt;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct siq_ript_msg *ript = &m->body.ript;
	struct timeval timeout = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//We should not have outstanding work right now
	ASSERT(comp_ctx->comp_target_min_lin == INVALID_LIN);
	ASSERT(comp_ctx->comp_target_current_lin == INVALID_LIN);
	ASSERT(comp_ctx->comp_target_max_lin == INVALID_LIN);

	//Get the work type.
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_WORK_TYPE, 1, &rt,
	    &error, out);
	ASSERT_IMPLIES(comp_ctx->rt != 0, comp_ctx->rt == rt, "%d != %d",
	    comp_ctx->rt, rt);
	comp_ctx->rt = rt;

	//Set the new work parameters and call func
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_MIN_LIN, 1,
	    &comp_ctx->comp_target_min_lin, &error, out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_MAX_LIN, 1,
	    &comp_ctx->comp_target_max_lin, &error, out);

	//Set current lin to the min
	comp_ctx->comp_target_current_lin = comp_ctx->comp_target_min_lin;

	log(DEBUG, "%s: received min %llx max %llx", __func__,
	    comp_ctx->comp_target_min_lin, comp_ctx->comp_target_max_lin);

	comp_ctx->process_work = true;
	if (comp_tout)
		migr_cancel_timeout(comp_tout);
	comp_tout = migr_register_timeout(&timeout, func, sw_ctx);

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

int
comp_target_work_range_callback(struct generic_msg *m, void *ctx)
{
	enum work_restart_type rt;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(uint32, &m->body.ript, RMF_WORK_TYPE, 1, &rt,
	    &error, out);

	switch (rt) {
	case WORK_COMP_COMMIT:
		comp_target_work_range_process(m, ctx,
		    process_worm_commits, &error);
		if (error) {
			//Send error back
			send_comp_target_error(global->primary,
			    "Failure occurred receiving compliance commits "
			    "work range", E_SIQ_COMP_COMMIT, error);
			isi_error_free(error);
			error = NULL;
		}
		break;
	case WORK_COMP_MAP_PROCESS:
		comp_target_work_range_process(m, ctx,
		    process_compliance_map, &error);
		if (error) {
			//Send error back
			send_comp_target_error(global->primary,
			    "Failure occurred receiving compliance map work "
			    "range", E_SIQ_COMP_MAP_PROCESS, error);
			isi_error_free(error);
			error = NULL;
		}

		comp_ctx->comp_map_chkpt_lin = comp_ctx->comp_target_min_lin;
		break;
	case WORK_CT_COMP_DIR_DELS:
		comp_target_work_range_process(m, ctx,
		    process_worm_dir_dels, &error);
		if (error) {
			send_comp_target_error(global->primary,
			    "Failure occurred receiving compliance "
			    "directory deletes work range",
			    E_SIQ_DEL_LIN, error);
			isi_error_free(error);
			error = NULL;
		}
		break;
	default:
		log(FATAL, "%s: Invalid work range type: %s", __func__,
		    work_type_str(rt));
	}

out:
	if (error) {
		send_comp_target_error(global->primary,
		    "Failed to get compliance work type",
		    E_SIQ_MSG_FIELD, error);
	}

	return 0;
}

static void
comp_target_finish_work(struct migr_sworker_comp_ctx *comp_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//Flush log, close open handles and clear state variables
	if (comp_ctx->worm_commit_ctx) {
		flush_lin_list_log(&comp_ctx->worm_commit_log,
		    comp_ctx->worm_commit_ctx, &error);
		if (error)
			goto out;
	}

	if (comp_ctx->worm_commit_iter) {
		close_lin_list_iter(comp_ctx->worm_commit_iter);
		comp_ctx->worm_commit_iter = NULL;
	}

	if (comp_ctx->comp_map_iter) {
		close_compliance_map_iter(comp_ctx->comp_map_iter);
		comp_ctx->comp_map_iter = NULL;
	}

	if (comp_ctx->worm_dir_dels_ctx) {
		flush_lin_list_log(&comp_ctx->worm_dir_dels_log,
		    comp_ctx->worm_dir_dels_ctx, &error);
		if (error)
			goto out;
	}

	if (comp_ctx->worm_dir_dels_iter) {
		close_lin_list_iter(comp_ctx->worm_dir_dels_iter);
		comp_ctx->worm_dir_dels_iter = NULL;
	}

	comp_ctx->comp_target_min_lin = INVALID_LIN;
	comp_ctx->comp_target_current_lin = INVALID_LIN;
	comp_ctx->comp_target_max_lin = INVALID_LIN;
	comp_ctx->comp_map_chkpt_lin = INVALID_LIN;
	comp_ctx->done = false;
	comp_ctx->comp_map_split_req = false;
	comp_ctx->process_work = false;
	comp_ctx->curr_dir_delayed_delete = false;

	if (comp_tout) {
		migr_cancel_timeout(comp_tout);
		comp_tout = NULL;
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

ifs_lin_t
comp_find_split(struct migr_sworker_comp_ctx *comp_ctx,
    ifs_lin_t cur_lin, ifs_lin_t max_lin, struct isi_error **error_out)
{
	int i;
	ifs_lin_t cur_max_lin;
	ifs_lin_t split_lin;
	bool res = false;
	struct isi_error *error = NULL;

	log(TRACE, "%s: cur_lin: %llx max_lin: %llx", __func__, cur_lin,
	    max_lin);
	/* keep dividing the range in half a max of 20 times until we find a
	 * lin range that has at least one valid lin to work on */
	cur_max_lin = max_lin;
	for (i = 0; i < 20; i++) {
		split_lin = cur_lin + ((cur_max_lin - cur_lin) / 2);
		log(DEBUG, "%s: Loop %d cur_lin: %llx cur_max_lin: %llx "
		    "split_lin: %llx", __func__, i, cur_lin, cur_max_lin,
		    split_lin);
		if (split_lin <= cur_lin || split_lin >= cur_max_lin) {
			split_lin = INVALID_LIN;
			goto out;
		}

		if (comp_ctx->rt == WORK_COMP_COMMIT) {
			res = get_lin_list_lin_in_range(
			    comp_ctx->worm_commit_ctx, split_lin,
			    cur_max_lin, &error);
		} else if (comp_ctx->rt == WORK_COMP_MAP_PROCESS) {
			res = get_compliance_map_lin_in_range(
			    comp_ctx->comp_map_ctx, split_lin,
			    cur_max_lin, &error);
		} else if (comp_ctx->rt == WORK_CT_COMP_DIR_DELS) {
			res = get_lin_list_lin_in_range(
			    comp_ctx->worm_dir_dels_ctx, split_lin,
			    cur_max_lin, &error);
		} else {
			log(FATAL, "%s: Invalid work range type: %s", __func__,
			    work_type_str(comp_ctx->rt));
		}
		if (error) {
			split_lin = INVALID_LIN;
			goto out;
		}

		if (res)
			break;

		cur_max_lin = split_lin;
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s: exit split_lin: %llx i: %d", __func__, split_lin, i);
	return split_lin;
}

int
comp_target_checkpoint_callback(struct generic_msg *m, void *ctx)
{
	enum work_restart_type rt = WORK_FINISH;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	ifs_lin_t split_lin = -1;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	RIPT_MSG_GET_FIELD(uint32, &m->body.ript, RMF_WORK_TYPE, 1, &rt,
	    &error, out);

	//The source wants us to stop (and split)
	//This could race with a checkpoint and stop request
	//If we are not actively working on anything just do nothing
	if (!comp_ctx->process_work) {
		log(DEBUG, "%s: exit - not processing commits", __func__);
		goto out;
	}

	// If working on the compliance map, set the done flag to true and
	// we will send the checkpoint msg after finishing the current
	// map exchange loop.
	if (rt == WORK_COMP_MAP_PROCESS) {
		comp_ctx->comp_map_split_req = true;
		if (comp_ctx->comp_map_ufp) {
			comp_map_checkpoint_done(sw_ctx, &error);
			if (error)
				goto out;
		}
		goto out;
	}

	//Try to suggest a split point to the source because the work items
	//are on the target and the source cannot compute it
	split_lin = comp_find_split(comp_ctx,
	    comp_ctx->comp_target_current_lin, comp_ctx->comp_target_max_lin,
	    &error);
	if (error)
		goto out;

	//Record the current position and send a checkpoint message
	resp.head.type =
	    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
	    1, comp_ctx->comp_target_current_lin);
	ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 1);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
	    1, split_lin);
	msg_send(sw_ctx->global.primary, &resp);

	//Clear the current state and make sure we don't do any
	//further work in process_worm_commits
	comp_target_finish_work(comp_ctx, &error);
	if (error)
		goto out;

out:
	if (rt == WORK_FINISH && error) {
		send_comp_target_error(global->primary,
		    "Failed to get compliance work type",
		    E_SIQ_MSG_FIELD, error);
		isi_error_free(error);
		error = NULL;
	}

	if (error) {
		send_comp_target_error(global->primary,
		    "Failure occurred checkpointing compliance commits",
		    E_SIQ_COMP_COMMIT, error);
		isi_error_free(error);
		error = NULL;
	}

	log(TRACE, "%s exit", __func__);
	return 0;
}

int
process_worm_commits(void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	bool res = false;
	int num_ops = 0;
	struct lin_list_entry current_lle = {};
	bool done = false;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct timeval timeout = {};
	struct isi_error *error = NULL;

	ASSERT(comp_ctx->comp_target_min_lin && comp_ctx->comp_target_max_lin);
	log(TRACE, "%s: min: %llx cur: %llx max: %llx", __func__,
	    comp_ctx->comp_target_min_lin, comp_ctx->comp_target_current_lin,
	    comp_ctx->comp_target_max_lin);

	//Open the worm commit SBT, if necessary
	if (!comp_ctx->worm_commit_ctx) {
		log(TRACE, "%s: creating lin_list", __func__);
		comp_ctx->worm_commit_ctx =
		    create_or_open_worm_commit_list(sw_ctx, false, &error);
		if (error) {
			goto out;
		}
		//Initialize the bulk structures
		bzero(&comp_ctx->worm_commit_log, sizeof(struct lin_list_log));
	}

	//If there already is an iterator, just keep doing work
	if (!comp_ctx->worm_commit_iter) {
		log(TRACE, "%s: creating lin_list iter", __func__);
		//Given the work range, get an interator to the sbt
		comp_ctx->worm_commit_iter = new_lin_list_range_iter(
		    comp_ctx->worm_commit_ctx, comp_ctx->comp_target_min_lin);
		comp_ctx->comp_target_current_lin =
		    comp_ctx->comp_target_min_lin;
	}
	ASSERT(comp_ctx->worm_commit_iter);

	UFAIL_POINT_CODE(process_worm_commit_skip,
		sleep(1);
		if (comp_tout) {
			migr_cancel_timeout(comp_tout);
			comp_tout = NULL;
		}
		comp_tout = migr_register_timeout(&timeout,
		    process_worm_commits, sw_ctx);
		goto out;
	);

	log(TRACE, "%s: commit loop start", __func__);
	//Up to the bulk op limit, do the commit
	for (num_ops = 0; num_ops < SBT_BULK_MAX_OPS; num_ops++) {
		log(TRACE, "%s: commit loop %d", __func__, num_ops);
		res = lin_list_iter_next(comp_ctx->worm_commit_iter,
		    &comp_ctx->comp_target_current_lin, &current_lle, &error);
		if (error) {
			goto out;
		}

		if (!res) {
			//No more to iterate
			done = true;
			break;
		}

		//Are we doing work for a different work item?
		if (comp_ctx->comp_target_current_lin >=
		    comp_ctx->comp_target_max_lin) {
			done = true;
			break;
		}

		//Skip this entry if it is already marked as done
		if (current_lle.commit) {
			log(ERROR, "%s: skipping %llx", __func__,
			    comp_ctx->comp_target_current_lin);
			continue;
		}

		log(DEBUG, "%s: committing %llx", __func__,
		    comp_ctx->comp_target_current_lin);

		//Do the actual commit
		do_worm_commit(sw_ctx, comp_ctx->comp_target_current_lin,
		    &error);
		if (error) {
			goto out;
		}

		//Failpoint to die after 1 commit
		UFAIL_POINT_CODE(process_worm_commit_die,
			error = isi_system_error_new(RETURN_VALUE,
			    "process_worm_commit_die ufp");
			goto out;
		);
	}
	log(TRACE, "%s: commit loop end", __func__);

	//If we are done with the work item, send back a checkpoint
	if (done) {
		log(DEBUG, "%s: done with work item", __func__);
		//Construct message and send it
		resp.head.type =
		    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
		ript_resp = &resp.body.ript;
		ript_msg_init(ript_resp);
		ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1,
		    comp_ctx->rt);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
		    1, comp_ctx->comp_target_max_lin);
		ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 1);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
		    1, INVALID_LIN);
		msg_send(global->primary, &resp);

		comp_target_finish_work(comp_ctx, &error);
		close_lin_list(comp_ctx->worm_commit_ctx);
		comp_ctx->worm_commit_ctx = NULL;
		if (error) {
			goto out;
		}

		if (comp_tout) {
			migr_cancel_timeout(comp_tout);
			comp_tout = NULL;
		}
	} else {
		log(DEBUG, "%s: adding timer callback", __func__);
		//There is still work to do, we should hit the migr process
		//loop to see if there is incoming messages and then continue
		if (comp_tout)
			migr_cancel_timeout(comp_tout);
		comp_tout = migr_register_timeout(&timeout,
		    process_worm_commits, sw_ctx);
	}

out:
	if (error) {
		send_comp_target_error(global->primary,
		    "Failure occurred during compliance commits",
		    E_SIQ_COMP_COMMIT, error);
		isi_error_free(error);
	}

	log(TRACE, "%s exit", __func__);
	return 0;
}

void
add_worm_commit(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct lin_list_entry lle = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter lin = %llx", __func__, lin);

	//Sanity checks
	//Lin looks sane?
	ASSERT(!is_special(lin));
	//Are we in compliance mode and v2?
	ASSERT(global->compliance_v2);
	//If the worm_commit lin_list isn't open/created yet, make/open it
	if (!comp_ctx->worm_commit_ctx) {
		log(TRACE, "%s: creating lin_list", __func__);
		comp_ctx->worm_commit_ctx =
		    create_or_open_worm_commit_list(sw_ctx, true, &error);
		if (error) {
			goto out;
		}
		//Initialize the bulk structures
		bzero(&comp_ctx->worm_commit_log, sizeof(struct lin_list_log));
	}

	//Add lin to the bulk list
	ASSERT(comp_ctx->worm_commit_ctx);
	lin_list_log_add_entry(comp_ctx->worm_commit_ctx,
	    &comp_ctx->worm_commit_log, lin, true, &lle, NULL, &error);
	if (error) {
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s: exit", __func__);
}

static struct compliance_map_ctx *
create_or_open_compliance_map(struct migr_sworker_ctx *sw_ctx, bool create,
    bool missing_ok, struct isi_error **error_out)
{
	bool cmap_exists = false;
	char cmap_name[MAXNAMLEN];
	struct compliance_map_ctx *to_return = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	// Always get the tgt compliance map since this can only be called
	// during resync prep.
	get_compliance_map_name(cmap_name, sizeof(cmap_name), global->sync_id,
	    false);

	// Check for existence.
	cmap_exists = compliance_map_exists(global->sync_id, cmap_name, &error);
	if (error)
		goto out;

	if (!cmap_exists && missing_ok)
		goto out;
	else if (!cmap_exists && create) {
		log(TRACE, "%s: creating compliance map", __func__);
		create_compliance_map(global->sync_id, cmap_name, true, &error);
		if (error)
			goto out;
	} else if (!cmap_exists && !create) {
		error = isi_siq_error_new(E_SIQ_COMP_MAP_PROCESS,
		    "Compliance Map does not exist.");
		goto out;
	}

	// Actually open the compliance map.
	open_compliance_map(global->sync_id, cmap_name, &to_return, &error);
	if (error)
		goto out;

	ASSERT(to_return->map_fd > 0);

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
	return to_return;
}

static ifs_lin_t
compliance_map_lookup(struct migr_sworker_ctx *sw_ctx,
    ifs_lin_t new_lin, struct isi_error **error_out)
{
	bool found;
	int count = 0;
	ifs_lin_t last_new_lin = new_lin;
	ifs_lin_t tmp_lin;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct rep_entry rep;
	struct isi_error *error = NULL;

	while (true) {
		found = get_compliance_mapping(last_new_lin, &tmp_lin,
		    comp_ctx->comp_map_ctx, &error);
		if (error)
			goto out;

		if (!found)
			break;

		last_new_lin = tmp_lin;
		count++;
		if (count >= MAX_COMP_MAP_LOOKUP_LOOP) {
			error = isi_system_error_new(EINVAL,
			    "Invalid comp map chain for lin %{}",
			    lin_fmt(new_lin));
			goto out;
		}
	}

	found = get_entry(comp_ctx->comp_target_current_lin, &rep,
	    global->rep_mirror_ctx, &error);
	if (error)
		goto out;

	if (found) {
		// Log the mirrored repstate update.
		rep_log_add_entry(global->rep_mirror_ctx,
		    &global->rep_mirror_log, last_new_lin, &rep,
		    NULL, SBT_SYS_OP_ADD, true, &error);
		if (error)
			goto out;

		UFAIL_POINT_CODE(skip_comp_rep_remove,
			goto out;
		);

		rep_log_add_entry(global->rep_mirror_ctx,
		    &global->rep_mirror_log,
		    comp_ctx->comp_target_current_lin, NULL,
		    NULL, SBT_SYS_OP_DEL, true, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s: old_lin: %llx, new_lin: %llx, chain: %d", __func__,
	    comp_ctx->comp_target_current_lin, last_new_lin, count);

	return last_new_lin;
}

int
process_compliance_map(void *ctx)
{
	bool done = false;
	bool found;
	char rep_name[MAXNAMLEN];
	int max_ops = SBT_BULK_MAX_OPS / 2;
	uint32_t num_entries = 0;
	ifs_lin_t new_lin;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct lmap_update cmap_entries[max_ops];
	struct timeval timeout = {};
	struct isi_error *error = NULL;

	// Note: Process a max of SBT_BULK_MAX_OPS / 2 entries at a time since
	// we need two linmap/repstate log entries per compliance map entry.

	ASSERT(comp_ctx->comp_target_min_lin && comp_ctx->comp_target_max_lin);
	log(TRACE, "%s: min: %llx cur: %llx max: %llx chkpt: %llx "
	    "process_work: %d", __func__, comp_ctx->comp_target_min_lin,
	    comp_ctx->comp_target_current_lin, comp_ctx->comp_target_max_lin,
	    comp_ctx->comp_map_chkpt_lin, comp_ctx->process_work);

	// Open the compliance map, if necessary.
	if (comp_ctx->comp_map_ctx == NULL) {
		comp_ctx->comp_map_ctx =
		    create_or_open_compliance_map(sw_ctx, false, false, &error);
		if (error)
			goto out;

		// Initialize the bulk structures.
		bzero(&comp_ctx->comp_map_log,
		    sizeof(struct compliance_map_log));
	}

	// If there already is an iterator, just keep doing work.
	if (comp_ctx->comp_map_iter == NULL) {
		// Given the work range, get an interator to the SBT.
		comp_ctx->comp_map_iter = new_compliance_map_range_iter(
		    comp_ctx->comp_map_ctx, comp_ctx->comp_target_min_lin);
		comp_ctx->comp_target_current_lin =
		    comp_ctx->comp_target_min_lin;
	}

	// We have to set/unset comp_map_ufp so that we do proper work splits
	// when this ufp is enabled.
	UFAIL_POINT_CODE(process_compliance_map_skip,
		comp_ctx->comp_map_ufp = true;
		if (RETURN_VALUE > 0) {
			siq_nanosleep(0, RETURN_VALUE * 1000000);
		}
		goto out;
	);
	comp_ctx->comp_map_ufp = false;

	// Make sure we opened the mirrored repstate.
	get_mirrored_rep_name(rep_name, global->sync_id);
	if (!global->rep_mirror_ctx) {
		open_repstate(global->sync_id, rep_name,
		    &global->rep_mirror_ctx, &error);
		if (error)
			goto out;
	}

	// Initialize the compliance map list.
	bzero(cmap_entries, max_ops * sizeof(*cmap_entries));

	log(TRACE, "%s: begin reading compliance map entries", __func__);
	// Iterate the compliance map up to SBT_BULK_MAX_OPS / 2 entries at a
	// time.
	while (num_entries < max_ops) {
		found = compliance_map_iter_next(comp_ctx->comp_map_iter,
		    &comp_ctx->comp_target_current_lin, &new_lin, &error);
		if (error)
			goto out;

		if (!found) {
			// No more entries to iterate.
			done = true;
			break;
		}

		// Are we doing work for a different work item?
		if (comp_ctx->comp_target_current_lin >=
		    comp_ctx->comp_target_max_lin) {
			done = true;
			break;
		}

		// Continue the lookup for this entry to see if it chains.
		new_lin = compliance_map_lookup(sw_ctx, new_lin, &error);
		if (error)
			goto out;

		// Add the mapping to the list that we will send.
		cmap_entries[num_entries].slin =
		    comp_ctx->comp_target_current_lin;
		cmap_entries[num_entries].dlin = new_lin;

		num_entries++;
	}

	log(TRACE, "%s: found %u compliance map entries (done: %d)", __func__,
	    num_entries, done);

	// We will send a checkpoint msg after the flushing handshake.
	comp_ctx->done = done;

	if (num_entries > 0) {
		send_compliance_map_entries(global->primary, num_entries,
		    cmap_entries, comp_ctx->comp_target_current_lin);
	} else {
		comp_map_checkpoint_done(sw_ctx, &error);
		if (error)
			goto out;
	}

out:
	if (error) {
		send_comp_target_error(global->primary,
		    "Failure occurred during compliance map processing",
		    E_SIQ_COMP_MAP_PROCESS, error);
		isi_error_free(error);
	} else if (!done) {
		log(DEBUG, "%s: adding timer callback", __func__);

		// There is still work to do, we should hit the migr process
		// loop to see if there are incoming messages and then continue.
		if (comp_tout) {
			migr_cancel_timeout(comp_tout);
			comp_tout = NULL;
		}
		comp_tout = migr_register_timeout(&timeout,
		    process_compliance_map, sw_ctx);
	} else if (comp_tout) {
		// Cancel the timeout.
		migr_cancel_timeout(comp_tout);
	}

	log(TRACE, "%s exit", __func__);
	return 0;
}

int
comp_map_begin_callback(struct generic_msg *m, void *ctx)
{
	ifs_lin_t min_lin = 0;
	ifs_lin_t max_lin = 0;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	comp_ctx->rt = WORK_COMP_MAP_PROCESS;

	// This should be called by the first pworker->sworker interaction.

	comp_ctx->comp_map_ctx = create_or_open_compliance_map(sw_ctx, false,
	    true, &error);
	if (error)
		goto out;

	// If we have nothing to do, min and max lins will be 0.
	if (comp_ctx->comp_map_ctx) {
		// Compute the total work range.
		min_lin = get_compliance_map_min_lin(comp_ctx->comp_map_ctx,
		    &error);
		if (error)
			goto out;

		max_lin = get_compliance_map_max_lin(comp_ctx->comp_map_ctx,
		    &error);
		if (error)
			goto out;

		log(TRACE, "%s: max_lin = %llx", __func__, max_lin);
		if (max_lin == INVALID_LIN)
			min_lin = max_lin = 0;
		else {
			// We need to send max_lin + 1 so that the source will
			// process max_lin. In regular change transfer work
			// splitting, max_lin is set to -1 (uint64_t) and not
			// the actual max lin like it is here.
			max_lin++;
		}
	}

	// Send response message.
	msg.head.type = build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);
	ript_msg_set_field_uint32(ript, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript, RMF_COMP_TARGET_MIN_LIN, 1, min_lin);
	ript_msg_set_field_uint64(ript, RMF_COMP_TARGET_MAX_LIN, 1, max_lin);
	msg_send(sw_ctx->global.primary, &msg);

	comp_ctx->comp_map_chkpt_lin = min_lin;
	log(DEBUG, "%s: sent work range min %llx max+1 %llx", __func__,
	    min_lin, max_lin);

out:
	if (error) {
		send_comp_target_error(sw_ctx->global.primary,
		    "Failure occurred starting compliance map processing",
		    E_SIQ_COMP_MAP_PROCESS, error);
		isi_error_free(error);

		if (comp_ctx->comp_map_ctx != NULL) {
			close_compliance_map(comp_ctx->comp_map_ctx);
			comp_ctx->comp_map_ctx = NULL;
		}
	}

	log(TRACE, "%s exit", __func__);
	return 0;
}

static void
comp_map_checkpoint_done(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	ifs_lin_t split_lin = INVALID_LIN;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript_resp = &msg.body.ript;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct isi_error *error = NULL;

	log(TRACE, "%s: done: %d", __func__, comp_ctx->done);

	if (comp_ctx->comp_map_split_req) {
		//Try to suggest a split point to the source because the work
		//items are on the target and the source cannot compute it
		split_lin = comp_find_split(comp_ctx,
		    comp_ctx->comp_map_chkpt_lin,
		    comp_ctx->comp_target_max_lin, &error);
		if (error)
			goto out;
	}

	if (comp_ctx->done || comp_ctx->comp_map_split_req) {
		log(DEBUG, "%s: done with work item", __func__);

		//Construct message and send it
		msg.head.type = build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
		ript_msg_init(ript_resp);
		ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1,
		    comp_ctx->rt);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
		    1, comp_ctx->done ? comp_ctx->comp_target_max_lin :
		    comp_ctx->comp_map_chkpt_lin);
		ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 1);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
		    1, split_lin);
		msg_send(global->primary, &msg);

		comp_target_finish_work(comp_ctx, &error);
		close_compliance_map(comp_ctx->comp_map_ctx);
		comp_ctx->comp_map_ctx = NULL;
		if (error)
			goto out;
	}

out:

	isi_error_handle(error, error_out);
}

int
comp_map_callback(struct generic_msg *m, void *ctx)
{
	bool found;
	char lmap_name[MAXNAMLEN];
	int max_ops = SBT_BULK_MAX_OPS / 2;
	uint8_t *buf;
	uint32_t i;
	uint32_t buf_len;
	uint32_t num_entries;
	ifs_lin_t chkpt_lin;
	ifs_lin_t dlin;
	struct lmap_update entries[max_ops];
	struct siq_ript_msg *ript = &m->body.ript;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct isi_error *error = NULL;

	// Note: Process a max of SBT_BULK_MAX_OPS / 2 entries at a time since
	// we need two linmap/repstate log entries per compliance map entry.

	log(TRACE, "%s", __func__);

	RIPT_MSG_GET_FIELD(uint32, ript, RMF_SIZE, 1, &num_entries, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_CHKPT_LIN, 1, &chkpt_lin, &error,
	    out);

	ript_msg_get_field_bytestream(ript, RMF_COMP_MAP_BUF, 1, &buf,
	    &buf_len, &error);
	if (error)
	    goto out;
	memcpy(entries, buf, buf_len);

	// Make sure we opened the linmap.
	get_lmap_name(lmap_name, global->sync_id, false);
	if (!global->lmap_ctx) {
		open_linmap(global->sync_id, lmap_name, &global->lmap_ctx,
		    &error);
		if (error)
			goto out;
	}

	log(DEBUG, "%s: processing %u compliance map entries", __func__,
	    num_entries);
	for (i = 0; i < num_entries; i++) {
		found = get_mapping(entries[i].slin, &dlin, global->lmap_ctx,
		    &error);
		if (error)
			goto out;

		if (found) {
			// Log the linmap update.
			lmap_log_add_entry(global->lmap_ctx, &global->lmap_log,
			    entries[i].slin, entries[i].dlin, LMAP_SET, 0,
			    &error);
			if (error)
				goto out;
		}
	}

	// Checkpoint work and flush the target linmap.
	flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
	if (error)
		goto out;

	// Flush the target mirrored repstate.
	flush_rep_log(&global->rep_mirror_log, global->rep_mirror_ctx, &error);
	if (error)
		goto out;

	// Flush the target compliance map.
	flush_compliance_map_log(&comp_ctx->comp_map_log,
	    comp_ctx->comp_map_ctx, &error);
	if (error)
		goto out;

	// Update the target-local checkpoint.
	comp_ctx->comp_map_chkpt_lin = chkpt_lin;

	UFAIL_POINT_CODE(exit_comp_map_callback,
		log(ERROR, "Exiting %s from ufp", __func__);
		exit(0)
	);

	// Send the checkpoint message.
	comp_map_checkpoint_done(sw_ctx, &error);
	if (error)
		goto out;

out:
	if (error) {
		send_comp_target_error(sw_ctx->global.primary,
		    "Failure occurred processing compliance map response "
		    "entries", E_SIQ_COMP_MAP_PROCESS, error);
		isi_error_free(error);
		error = NULL;
	}

	return 0;
}

void
comp_map_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = &msg.body.ript;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (!comp_ctx->process_work || comp_ctx->rt != WORK_COMP_MAP_PROCESS)
		goto out;

	//Send message
	msg.head.type = build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
	ript_msg_init(ript);
	ript_msg_set_field_uint32(ript, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript, RMF_COMP_TARGET_CURR_LIN,
	    1, comp_ctx->comp_map_chkpt_lin);
	ript_msg_set_field_uint8(ript, RMF_COMP_TARGET_DONE, 1, 0);
	ript_msg_set_field_uint64(ript, RMF_COMP_TARGET_SPLIT_LIN,
		    1, INVALID_LIN);
	msg_send(sw_ctx->global.primary, &msg);

	log(DEBUG, "%s: sent checkpoint lin %llx done 0", __func__,
	    comp_ctx->comp_map_chkpt_lin);

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

static void
do_worm_dir_del(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out)
{
	int fd = -1;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct migr_sworker_stats_ctx *stats = &sw_ctx->stats;
	struct lin_list_entry lle = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s: lin: %llx", __func__, lin);

	//Call delete_lin again on specified lin
	delete_lin(sw_ctx, lin, &error);
	if (error) {
		goto out;
	}

	//Mark the entry as done in the SBT
	lle.commit = true;
	lin_list_log_add_entry(comp_ctx->worm_dir_dels_ctx,
	    &comp_ctx->worm_dir_dels_log, lin, true, &lle, NULL, &error);
	if (error) {
		goto out;
	}

out:
	if (fd >= 0) {
		close(fd);
		fd = -1;
	}
	isi_error_handle(error, error_out);

	log(TRACE, "%s: exit", __func__);
}

static void
set_lli_worm_dir_dels_list(struct lin_list_ctx *llc,
    struct isi_error **error_out)
{
	struct lin_list_info_entry llie = {};
	struct isi_error *error = NULL;

	llie.type = DIR_DELS;
	log(TRACE, "%s: setting lin_list info", __func__);
	set_lin_list_info(&llie, llc, false, &error);
	isi_error_handle(error, error_out);
}

static struct lin_list_ctx *
create_or_open_worm_dir_dels_list(struct migr_sworker_ctx *sw_ctx, bool create,
    struct isi_error **error_out)
{
	struct lin_list_ctx *to_return = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	bool ll_exists = false;
	bool res = false;
	struct lin_list_info_entry llie = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//Check existence and create it if it doesn't
	ll_exists = lin_list_exists(global->sync_id, LIN_LIST_NAME_DIR_DELS,
	    &error);
	if (error)
		goto out;

	if (!ll_exists && create) {
		log(TRACE, "%s: creating lin_list", __func__);
		create_lin_list(global->sync_id, LIN_LIST_NAME_DIR_DELS,
		    true, &error);
		if (error)
			goto out;
	} else if (!ll_exists && !create) {
		error = isi_siq_error_new(E_SIQ_DEL_LIN,
		    "Compliance Directory Deletes LIN List does not exist");
		goto out;
	}

	//Actually open the list
	log(TRACE, "%s: opening lin_list", __func__);
	open_lin_list(global->sync_id, LIN_LIST_NAME_DIR_DELS,
	    &to_return, &error);
	if (error)
		goto out;
	ASSERT(to_return->lin_list_fd > 0);

	if (!ll_exists && create) {
		//Since we had to create the lin_list, also set the type
		set_lli_worm_dir_dels_list(to_return, &error);
		if (error) {
			goto out;
		}
	}

	//Verify the type is correct
	res = get_lin_list_info(&llie, to_return, &error);
	if (error)
		goto out;

	if (res)
		ASSERT(llie.type == DIR_DELS);
	else {
		//We might be racing the creation
		log(TRACE, "%s: blank lin_list info, setting now", __func__);
		set_lli_worm_dir_dels_list(to_return, &error);
		if (error) {
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
	return to_return;
}

void
comp_dir_dels_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (!comp_ctx->process_work || comp_ctx->rt != WORK_CT_COMP_DIR_DELS) {
		goto out;
	}

	//Flush bulk log
	flush_lin_list_log(&comp_ctx->worm_dir_dels_log,
	    comp_ctx->worm_dir_dels_ctx, &error);
	if (error) {
		goto out;
	}

	//Send message
	resp.head.type =
	    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
	    1, comp_ctx->comp_target_current_lin);
	ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 0);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
	    1, INVALID_LIN);
	msg_send(sw_ctx->global.primary, &resp);
	log(DEBUG, "%s: sent checkpoint current lin %llx "
	    "done 0 split_lin: %llx", __func__,
	    comp_ctx->comp_target_current_lin, INVALID_LIN);

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

int
comp_dir_dels_begin_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct lin_list_ctx *comp_dir_dels_ctx = NULL;
	bool ll_exists = false;
	ifs_lin_t min_lin = 0;
	ifs_lin_t max_lin = 0;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	comp_ctx->rt = WORK_CT_COMP_DIR_DELS;

	//This should be called by the first pworker->sworker interaction

	//Send back a work range message after figuring out what
	//the min/max range is
	//First, if the lin_list doesn't exist, we have nothing to do
	ll_exists = lin_list_exists(sw_ctx->global.sync_id,
	    LIN_LIST_NAME_DIR_DELS, &error);
	log(TRACE, "%s: policy_id: %s ll_exists: %d", __func__,
	    sw_ctx->global.sync_id, ll_exists);
	if (ll_exists) {
		log(TRACE, "%s: open lin_list", __func__);
		comp_dir_dels_ctx =
		    create_or_open_worm_dir_dels_list(sw_ctx, false, &error);
		if (error) {
			goto out;
		}

		min_lin = get_lin_list_min_lin(comp_dir_dels_ctx, &error);
		if (error) {
			goto out;
		}

		max_lin = get_lin_list_max_lin(comp_dir_dels_ctx, &error);
		if (error) {
			goto out;
		}
		log(TRACE, "%s: min_lin: %llx max_lin: %llx", __func__,
		    min_lin, max_lin);

		if (max_lin == INVALID_LIN)
			min_lin = max_lin = 0;
		else {
			// We need to send max_lin + 1 so that the source will
			// process max_lin. In regular change transfer work
			// splitting, max_lin is set to -1 (uint64_t) and not
			// the actual max lin like it is here.
			max_lin++;
			ASSERT(min_lin < max_lin, "%llx > %llx", min_lin,
			    max_lin);
		}
	}
	//If we have nothing to do, min and max lins will be 0

	//Send message
	resp.head.type = build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, comp_ctx->rt);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MIN_LIN,
	    1, min_lin);
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MAX_LIN,
	    1, max_lin);
	msg_send(sw_ctx->global.primary, &resp);

	log(TRACE, "%s: sent work range min %llx max+1 %llx", __func__,
	    min_lin, max_lin);

out:
	if (error) {
		send_comp_target_error(sw_ctx->global.primary,
		    "Failure occurred starting compliance directory deletes",
		    E_SIQ_DEL_LIN, error);
		isi_error_free(error);
	}

	if (comp_dir_dels_ctx)
		close_lin_list(comp_dir_dels_ctx);

	log(TRACE, "%s exit", __func__);
	return 0;
}

int
process_worm_dir_dels(void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	bool res = false;
	int num_ops = 0;
	struct lin_list_entry current_lle = {};
	bool done = false;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct timeval timeout = {};
	struct isi_error *error = NULL;

	ASSERT(comp_ctx->comp_target_min_lin && comp_ctx->comp_target_max_lin);
	log(TRACE, "%s: min: %llx cur: %llx max: %llx", __func__,
	    comp_ctx->comp_target_min_lin, comp_ctx->comp_target_current_lin,
	    comp_ctx->comp_target_max_lin);

	//Open the worm commit SBT, if necessary
	if (!comp_ctx->worm_dir_dels_ctx) {
		log(TRACE, "%s: creating lin_list", __func__);
		comp_ctx->worm_dir_dels_ctx =
		    create_or_open_worm_dir_dels_list(sw_ctx, false, &error);
		if (error) {
			goto out;
		}
		//Initialize the bulk structures
		bzero(&comp_ctx->worm_dir_dels_log, sizeof(struct lin_list_log));
	}

	//If there already is an iterator, just keep doing work
	if (!comp_ctx->worm_dir_dels_iter) {
		log(TRACE, "%s: creating lin_list iter", __func__);
		//Given the work range, get an interator to the sbt
		comp_ctx->worm_dir_dels_iter = new_lin_list_range_iter(
		    comp_ctx->worm_dir_dels_ctx, comp_ctx->comp_target_min_lin);
		comp_ctx->comp_target_current_lin =
		    comp_ctx->comp_target_min_lin;
	}
	ASSERT(comp_ctx->worm_dir_dels_iter);

	UFAIL_POINT_CODE(process_worm_dir_dels_skip,
		sleep(1);
		if (comp_tout) {
			migr_cancel_timeout(comp_tout);
			comp_tout = NULL;
		}
		comp_tout = migr_register_timeout(&timeout,
		    process_worm_dir_dels, sw_ctx);
		goto out;
	);

	log(TRACE, "%s: dir dels loop start", __func__);
	//Up to the bulk op limit, do the commit
	for (num_ops = 0; num_ops < SBT_BULK_MAX_OPS; num_ops++) {
		log(TRACE, "%s: dir dels loop %d", __func__, num_ops);
		res = lin_list_iter_next(comp_ctx->worm_dir_dels_iter,
		    &comp_ctx->comp_target_current_lin, &current_lle, &error);
		if (error) {
			goto out;
		}

		if (!res) {
			//No more to iterate
			done = true;
			break;
		}

		//Are we doing work for a different work item?
		if (comp_ctx->comp_target_current_lin >=
		    comp_ctx->comp_target_max_lin) {
			done = true;
			break;
		}

		//Skip this entry if it is already marked as done
		if (current_lle.commit) {
			log(TRACE, "%s: skipping %llx", __func__,
			    comp_ctx->comp_target_current_lin);
			continue;
		}

		log(TRACE, "%s: deleting %llx", __func__,
		    comp_ctx->comp_target_current_lin);

		//Do the actual delete
		do_worm_dir_del(sw_ctx, comp_ctx->comp_target_current_lin,
		    &error);
		if (error) {
			goto out;
		}

		//Failpoint to die after 1 commit
		UFAIL_POINT_CODE(process_worm_dir_dels_die,
			error = isi_system_error_new(RETURN_VALUE,
			    "process_worm_dir_dels_die ufp");
			goto out;
		);
	}
	log(TRACE, "%s: dir dels loop end", __func__);

	//If we are done with the work item, send back a checkpoint
	if (done) {
		log(TRACE, "%s: done with work item", __func__);
		//Construct message and send it
		resp.head.type =
		    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
		ript_resp = &resp.body.ript;
		ript_msg_init(ript_resp);
		ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1,
		    comp_ctx->rt);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_CURR_LIN,
		    1, comp_ctx->comp_target_max_lin);
		ript_msg_set_field_uint8(ript_resp, RMF_COMP_TARGET_DONE, 1, 1);
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_SPLIT_LIN,
		    1, INVALID_LIN);
		msg_send(global->primary, &resp);

		comp_target_finish_work(comp_ctx, &error);
		close_lin_list(comp_ctx->worm_dir_dels_ctx);
		comp_ctx->worm_dir_dels_ctx = NULL;
		if (error) {
			goto out;
		}

		if (comp_tout) {
			migr_cancel_timeout(comp_tout);
			comp_tout = NULL;
		}
	} else {
		log(TRACE, "%s: adding timer callback", __func__);
		//There is still work to do, we should hit the migr process
		//loop to see if there is incoming messages and then continue
		if (comp_tout)
			migr_cancel_timeout(comp_tout);
		comp_tout = migr_register_timeout(&timeout,
		    process_worm_dir_dels, sw_ctx);
	}

out:
	if (error) {
		send_comp_target_error(global->primary,
		    "Failure occurred during compliance directory deletes",
		    E_SIQ_DEL_LIN, error);
		isi_error_free(error);
	}

	log(TRACE, "%s exit", __func__);
	return 0;
}

void
add_worm_dir_del(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct lin_list_entry lle = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter lin = %llx", __func__, lin);

	//Sanity checks
	//Lin looks sane?
	ASSERT(!is_special(lin));
	//Are we in compliance mode and v2?
	ASSERT(global->compliance_v2);
	//If the worm_dir_dels lin_list isn't open/created yet, make/open it
	if (!comp_ctx->worm_dir_dels_ctx) {
		log(TRACE, "%s: creating lin_list", __func__);
		comp_ctx->worm_dir_dels_ctx =
		    create_or_open_worm_dir_dels_list(sw_ctx, true, &error);
		if (error) {
			goto out;
		}
		//Initialize the bulk structures
		bzero(&comp_ctx->worm_dir_dels_log, sizeof(struct lin_list_log));
	}

	//Add lin to the bulk list
	ASSERT(comp_ctx->worm_dir_dels_ctx);
	lin_list_log_add_entry(comp_ctx->worm_dir_dels_ctx,
	    &comp_ctx->worm_dir_dels_log, lin, true, &lle, NULL, &error);
	if (error) {
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s: exit", __func__);
}
