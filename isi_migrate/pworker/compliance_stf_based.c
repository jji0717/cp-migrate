#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <sys/isi_enc.h>
#include <isi_domain/worm.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_dir.h>
#include <isi_util/isi_extattr.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/bam/bam_pctl.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/compliance_map.h"
#include "isi_migrate/migr/comp_conflict_log.h"
#include "isi_migrate/migr/alg.h"
#include "pworker.h"

static void
migr_continue_comp_resync(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

static void
migr_continue_comp_lin_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

/*
 * Populates the compliance resync stats.
 */
static void
migr_update_comp_resync_stats(struct siq_stat *stats, bool is_dir)
{
	stats->change_compute->lins_total++;
	if (is_dir) {
		stats->change_compute->dirs_new++;
		stats->dirs->created->src++;
		stats->compliance->resync_compliance_dirs_new++;
	} else {
		stats->change_compute->files_new++;
		stats->compliance->resync_conflict_files_new++;
	}
}

/*
 * Populates the compliance link stats.
 */
static void
migr_update_comp_link_stats(struct siq_stat *stats, bool is_dir)
{
	if (is_dir) {
		stats->dirs->linked->src++;
		stats->compliance->resync_compliance_dirs_linked++;
	} else {
		stats->files->linked->src++;
		stats->compliance->resync_conflict_files_linked++;
	}
}

/*
 * Creates the resync iterator if it doesn't exist.
 */
void
migr_continue_regular_comp_resync(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct resync_iter *resync_iter = &pw_ctx->resync_iter;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	UFAIL_POINT_CODE(stop_resync_cc,
		if (RETURN_VALUE > 0) {
			siq_nanosleep(0, RETURN_VALUE * 1000000);
		}

		migr_call_next(pw_ctx, migr_continue_regular_comp_resync);
		return;
	);

	if (resync_iter->iter == NULL) {
		resync_iter->iter = new_lin_list_range_iter(cc_ctx->resync,
		    work->lin);
	}

	migr_call_next(pw_ctx, migr_continue_comp_resync);

	isi_error_handle(error, error_out);
}

/*
 * Populates the rep_entry based on a resync entry.
 */
static void
populate_rep_entry(ifs_lin_t lin, bool is_dir, ifs_snapid_t snap,
    struct rep_entry *rep, struct isi_error **error_out)
{
	bool found;
	struct stat st;
	struct isi_error *error = NULL;

	found = stat_lin_snap(lin, snap, &st, false, &error);
	if (error)
		goto out;

	ASSERT(S_ISDIR(st.st_mode) == is_dir);

	memset(rep, 0, sizeof(*rep));
	rep->lcount = is_dir ? 1 : st.st_nlink;
	rep->is_dir = is_dir;
	rep->changed = false;
	if (!is_dir)
		rep->not_on_target = true;
	else
		rep->changed = true;

 out:
	isi_error_handle(error, error_out);
}

/**
 * Copy all resync lins to the differential repstate.
 */
static void
migr_continue_comp_resync(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool done = false;
	bool found;
	int fd = -1;
	int i;
	int j = 0;
	ifs_lin_t lin;
	ifs_lin_t linlist[SBT_BULK_MAX_OPS];
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct summ_stf_ctx *summ_ctx = cc_ctx->summ_stf;
	struct work_restart_state *work = pw_ctx->work;
	struct rep_entry entries[SBT_BULK_MAX_OPS];
	struct rep_entry new_entry;
	struct lin_list_entry entry;
	struct resync_iter *resync_iter = &pw_ctx->resync_iter;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(resync_iter->iter != NULL);

	for (i = 0; i < SBT_BULK_MAX_OPS; i++) {
		found = lin_list_iter_next(resync_iter->iter, &lin, &entry,
		    &error);
		if (error)
			goto out;
		if (!found || (lin >= work->max_lin)) {
			done = true;
			break;
		}

		// Check to see if this canonical link has been deleted.
		fd = ifs_lin_open(lin, cc_ctx->snap2, O_RDONLY);
		if (fd == -1) {
			if (errno == ENOENT) {
				// The file was deleted, so remove it from the
				// incremental repstate. Ignore ENOENT on
				// remove.
				remove_entry(lin, cc_ctx->rep2, &error);
				if (error)
					goto out;

				continue;
			}

			error = isi_system_error_new(errno, "%s: Failed to "
			    "open lin %llx", __func__, lin);
			goto out;
		}

		close(fd);
		fd = -1;

		work->lin = lin;
		linlist[j] = lin;

		populate_rep_entry(lin, entry.is_dir, cc_ctx->snap2,
		    &new_entry, &error);
		if (error)
			goto out;

		/* Add new directories to summary STF so that change compute
		 * traverses them and adds new entries underneath them to the
		 * incremental repstate. */
		if (entry.is_dir) {
			pw_ctx->stf_cur_stats->change_compute->dirs_changed++;
			set_stf_entry(lin, summ_ctx, &error);
			if (error)
				goto out;
		}

		entries[j] = new_entry;

		migr_update_comp_resync_stats(pw_ctx->stf_cur_stats,
		    entry.is_dir);

		j++;
	}

	if (j) {
		copy_rep_entries(cc_ctx->rep2, linlist, entries, i, &error);
		if (error)
			goto out;
	}

	if (done) {
		/* Flush summary stf */
		if (summ_stf_log_entries(summ_ctx)) {
			flush_summ_stf_log(summ_ctx, &error);
			if (error)
				goto out;
		}

		stf_request_new_work(pw_ctx, &error);
		if (error)
			goto out;

		close_lin_list_iter(resync_iter->iter);
		resync_iter->iter = NULL;
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * For every conflict file, populate entries in
 * resync and compliance_map SBT.
 */
static void
stf_continue_conflict_list(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool conflict_res;
	bool done = false;
	bool found;
	int i;
	int count = 0;
	struct rep_entry entry;
	ifs_lin_t lin, dlin;
	ifs_lin_t resync_linlist[SBT_BULK_MAX_OPS];
	struct work_restart_state *work = pw_ctx->work;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct lin_list_entry resync_entries[SBT_BULK_MAX_OPS];
	struct compliance_map_log  comp_log = {};
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(stf_rep_iter->iter != NULL);

	memset(resync_entries, 0,
	    sizeof(struct lin_list_entry) * SBT_BULK_MAX_OPS);

	for (i = 0; i < SBT_BULK_MAX_OPS; i++) {

		/* Only process one entry. */
		UFAIL_POINT_CODE(conflict_list_stall_after_one,
		    if (work->lin != 0)
			    return;
		);

		found = rep_iter_next(stf_rep_iter->iter, &lin, &entry, &error);
		if (error)
			goto out;
		if (!found || (lin >= work->max_lin)) {
			done = true;
			break;
		}

		work->lin = lin;

		/* Consider only modified or newly created file entries */
		if (entry.is_dir || entry.lcount <= 0 ||
		    !(entry.not_on_target || entry.changed))
			continue;

		/*
		 * See if the lin needs conflict resolution.
		 */
		conflict_res = is_conflict_res_needed(lin, cc_ctx->snap1,
		    cc_ctx->snap2, &error);
		if (error)
			goto out;
		else if (!conflict_res)
			continue;

		/*
		 * New files we just need to populate resync list.
		 * For modified files we need to populate both resync and
		 * compliance map entries.
		 */
		resync_linlist[count] = lin;
		count++;

		if (entry.not_on_target)
			continue;

		if (entry.changed) {
			/*
			 * Consider entries only which have linmap entry which
			 * means those lins were re-created to resolve conflicts.
			 * I dont see need for extra checks around opening lin
			 * and checking for commit bit.
			 */
			found = get_mapping(lin, &dlin, pw_ctx->lmap_restore_ctx,
			    &error);
			if (error)
				goto out;

			if (!found)
				continue;

			compliance_map_log_add_entry(pw_ctx->comp_map_ctx,
			    &comp_log, lin, dlin, CMAP_SET, 0, &error);
			if (error)
				goto out;
		}

		/* XXX TODO : stats for the phase. */
	}

	/*
	 * Flush any outstanding compliance map log entries.
	 */
	flush_compliance_map_log(&comp_log, pw_ctx->comp_map_ctx,
	    &error);
	if (error)
		goto out;

	if (count) {
		copy_lin_list_entries(pw_ctx->resync_ctx, resync_linlist,
		    resync_entries, count, &error);
		if (error)
			goto out;
	}

	if (done) {
		close_rep_iter(stf_rep_iter->iter);
		stf_rep_iter->iter = NULL;
		close_linmap(pw_ctx->lmap_restore_ctx);
		pw_ctx->lmap_restore_ctx = NULL;
		close_lin_list(pw_ctx->resync_ctx);
		pw_ctx->resync_ctx = NULL;
		close_compliance_map(pw_ctx->comp_map_ctx);
		pw_ctx->comp_map_ctx = NULL;
		stf_request_new_work(pw_ctx, &error);
		if (error)
			goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

/**
 * Builds resync SBT and compliance map SBT with
 * List of lins that had conflicts and that had
 * unlinks and re-create in the CONFLICT_CLEANUP
 * phase.
 */
void
stf_continue_conflict_list_begin(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct stf_rep_iter *stf_rep_iter = &pw_ctx->stf_rep_iter;
	struct isi_error *error = NULL;
	char sbt_name[MAXNAMELEN + 1];

	log(TRACE, "%s", __func__);

	if (stf_rep_iter->iter == NULL) {
		stf_rep_iter->iter = new_rep_range_iter(pw_ctx->cur_rep_ctx,
		    work->chkpt_lin);
		ASSERT(stf_rep_iter->iter != NULL);
	}

	if (pw_ctx->lmap_restore_ctx == NULL) {
		get_lmap_name(sbt_name, pw_ctx->policy_id, true);
		open_linmap(pw_ctx->policy_id, sbt_name,
		    &pw_ctx->lmap_restore_ctx, &error);
		if (error)
			goto out;
		ASSERT(pw_ctx->lmap_restore_ctx != NULL);
	}

	if (pw_ctx->resync_ctx == NULL) {
		get_resync_name(sbt_name, sizeof(sbt_name),
		    pw_ctx->policy_id, pw_ctx->src);
		open_lin_list(pw_ctx->policy_id, sbt_name,
		    &pw_ctx->resync_ctx, &error);
		if (error)
			goto out;
		ASSERT(pw_ctx->resync_ctx != NULL);
	}

	if (pw_ctx->comp_map_ctx == NULL && pw_ctx->restore) {
		get_compliance_map_name(sbt_name, sizeof(sbt_name),
		    pw_ctx->policy_id, pw_ctx->src);
		open_compliance_map(pw_ctx->policy_id, sbt_name,
		    &pw_ctx->comp_map_ctx, &error);
		if (error)
			goto out;
		ASSERT(pw_ctx->comp_map_ctx != NULL);
	}

	migr_call_next(pw_ctx, stf_continue_conflict_list);

 out:
	isi_error_handle(error, error_out);
}

/*
 * This adds new lin and associated lin_list entry to resync iter buffer.
 */
static void
add_resync_entry_to_buffer(struct resync_iter *resync_iter, ifs_lin_t *lin,
    struct lin_list_entry *res_entry)
{
	int index;

	index = resync_iter->count;

	resync_iter->lin_lin[index].lin = *lin;
	resync_iter->lin_lin[index].entry = *res_entry;
	resync_iter->count++;
	ASSERT(resync_iter->count <= MAX_ITER_ENTRIES);
}

/*
 * This function does local prefetch of directory lins for compliance failback
 * resyncs.
 */
static void
prefetch_resync_lins(struct migr_pworker_ctx *pw_ctx,
    struct resync_iter *resync_iter, struct isi_error **error_out)
{
	int i, ret;
	struct isi_error *error = NULL;
	ifs_snapid_t snapid = pw_ctx->cur_snap;
	struct ifs_lin_snapid_portal lins[MAX_ITER_ENTRIES];

	/* Do local prefetch of lins. */
	for (i = 0; i < resync_iter->count; i++) {
		lins[i].lin = resync_iter->lin_lin[i].lin;
		lins[i].snapid = snapid;
		lins[i].portal_depth = 0;
	}
	if (resync_iter->count <= 0)
		goto out;

	ret = ifs_prefetch_lin(lins, resync_iter->count, 0);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to prefetch resync lin range %llx - %llx",
		    lins[0].lin, lins[i - 1].lin);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * This function gets utmost MAX_ITER_ENTRIES from the resync list and stores
 * them in the resync iter buffer. The function may read 0 entries if it did not
 * find any valid resync entries in MAX_RESYNC_SCAN_COUNT number of resync
 * entries. We want to have regular breaks even if we did  not find any valid
 * entry since this will allow us to have effective work splits.
 * Once we read [0 - MAX_ITER_ENTRIES] entries we check if we need to do
 * a local prefetch of lins (prefetch_resync_lins).
 */
static bool
get_next_resync_entries_with_prefetch(struct migr_pworker_ctx *pw_ctx,
    bool *done, struct isi_error **error_out)
{
	int count = 0;
	struct isi_error *error = NULL;
	ifs_lin_t lin;
	bool got_one = false;
	struct lin_list_entry res_entry;
	struct resync_iter *resync_iter = &pw_ctx->resync_iter;
	struct lin_list_iter *res_iter = resync_iter->iter;
	struct work_restart_state *work = pw_ctx->work;

	ASSERT(resync_iter->count == 0);
	while (count < MAX_REP_STATE_SCAN_COUNT &&
	    resync_iter->count < MAX_ITER_ENTRIES) {
		got_one = lin_list_iter_next(res_iter, &lin, &res_entry,
		    &error);
		if (error)
			goto out;
		if (!got_one || lin >= work->max_lin) {
			*done = true;
			break;
		}

		ASSERT(lin >= work->min_lin);

		add_resync_entry_to_buffer(resync_iter, &lin, &res_entry);
		count++;
	}

	if (resync_iter->count > 0) {
		*done = false;

		/* Prefetch lins if required */
		prefetch_resync_lins(pw_ctx, resync_iter, &error);
		if (error)
			goto out;

		resync_iter->index = 0;
	}

 out:
	isi_error_handle(error, error_out);
	return (resync_iter->count > 0);
}


/*
 * This function reads next available lin and resync entry from
 * the resync iter local buffer. It also sets the other variables
 * which include work->lin and pw_ctx->cur_lin  and pw_ctx->resync_entry.
 * These variables are used by each phase to work on that specific lin and
 * resync entry.
 */
static void
read_next_avail_resync_entry(struct migr_pworker_ctx *pw_ctx)
{
	struct resync_iter *resync_iter = &pw_ctx->resync_iter;
	int index;

	index = resync_iter->index;
	pw_ctx->resync_entry = resync_iter->lin_lin[index].entry;
	pw_ctx->prev_lin = pw_ctx->cur_lin;
	pw_ctx->cur_lin = resync_iter->lin_lin[index].lin;
	pw_ctx->work->lin = resync_iter->lin_lin[index].lin;
	resync_iter->index++;
	resync_iter->count--;
	ASSERT(resync_iter->count >= 0 &&
	    resync_iter->index <= MAX_ITER_ENTRIES);
}

/*
 * Read the next available resync entry from the resync iter buffer.
 * If the buffer is empty then we read next set of resync entries
 * from the resync btree. The done is set to true if we have no more
 * resync entries left to scan or the resync entry key has crossed the
 * current work range.
 */
static bool
get_next_valid_resync_lin(struct migr_pworker_ctx *pw_ctx,
    bool *done, struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct resync_iter *resync_iter = &pw_ctx->resync_iter;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	bool found = false;

	*done = false;
	if (resync_iter->iter == NULL) {
		ASSERT(cc_ctx->resync != NULL);
		resync_iter->iter = new_lin_list_range_iter(cc_ctx->resync,
		    work->chkpt_lin);
	}

	if (resync_iter->count == 0) {
		/* Read more entries from resync list. */
		get_next_resync_entries_with_prefetch(pw_ctx, done, &error);
		if (error)
			goto out;
	}

	if (resync_iter->count > 0) {
		read_next_avail_resync_entry(pw_ctx);
		if (pw_ctx->cur_lin >= work->max_lin)
			*done = true;
		else
			found = true;
	}

	if (*done) {
		close_lin_list_iter(resync_iter->iter);
		resync_iter->iter = NULL;
		resync_iter->index = 0;
		resync_iter->count = 0;
		pw_ctx->cur_lin = 0;
	}

 out:
	isi_error_handle(error, error_out);
	return found;
}

/*
 * Gets all parents for a given lin and snapid.
 * Returns the parent entries/count as an out param and the number of links.
 * Caller is responsible for freeing parent entries.
 */
static unsigned int
get_all_parent_lins(const ifs_lin_t lin, ifs_snapid_t snapid, bool is_dir,
    struct parent_entry **parents, unsigned int *num_parents_out,
    struct isi_error **error_out)
{
	int fd = -1;
	int i;
	int ret = -1;
	unsigned int expected_parents;
	unsigned int links = 0;
	struct stat st;
	struct isi_error *error = NULL;

	ASSERT(*parents == NULL);

	fd = ifs_lin_open(lin, snapid, O_RDONLY);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "%s could not open %llx", __func__, lin);
		goto out;
	}

	ret = fstat(fd, &st);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "%s could not stat %llx", __func__, lin);
		goto out;
	}

	ASSERT(S_ISDIR(st.st_mode) == is_dir);

	expected_parents = is_dir ? 1 : st.st_nlink;

	*parents = malloc(sizeof(struct parent_entry) * expected_parents);
	ASSERT(*parents != NULL);

	ret = pctl2_lin_get_parents(lin, snapid, *parents, expected_parents,
	    num_parents_out);
	if (ret != 0)
		goto out;

	for (i = 0; i < *num_parents_out; i++)
		links += (*parents)[i].count;

	ASSERT(expected_parents == links);

 out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);

	return links;
}

/*
 * Waits for outstanding acks and then gets the next valid LIN that needs
 * to be resynced. We then get all of the parents for the valid LIN.
 */
void
migr_continue_regular_comp_lin_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool done = false;
	bool found = false;
	struct generic_msg msg = {};
	struct resync_parents_state *pstate = &pw_ctx->resync_parents_state;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->work != NULL);

	UFAIL_POINT_CODE(stop_comp_links,
	    if (RETURN_VALUE > 0) {
		siq_nanosleep(0, RETURN_VALUE * 1000000);
	    }

	    migr_call_next(pw_ctx, migr_continue_regular_comp_lin_links);
	    return;
	);

	if (pw_ctx->outstanding_acks >= STF_ACK_INTERVAL) {
		/* We will wait for ack from sworker. */
		migr_wait_for_response(pw_ctx, MIGR_WAIT_STF_LIN_ACK);
		goto out;
	}

	found = get_next_valid_resync_lin(pw_ctx, &done, &error);
	if (error)
		goto out;

	if (done) {
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

	/* Get all parents for the lin. */
	if (pstate->parents != NULL) {
		free(pstate->parents);
		pstate->parents = NULL;
	}
	pstate->num_parents = 0;
	pstate->skip_comp_store = false;

	pstate->num_links = get_all_parent_lins(pw_ctx->cur_lin,
	    pw_ctx->chg_ctx->snap2, pw_ctx->resync_entry.is_dir,
	    &pstate->parents, &pstate->num_parents, &error);
	if (error)
		goto out;

	pstate->dirent_lin = pw_ctx->cur_lin;
	pstate->is_dir = pw_ctx->resync_entry.is_dir;
	pstate->index = 0;
	pstate->cur_parent.dir_fd = -1;
	pstate->cur_parent.index = 0;

	migr_call_next(pw_ctx, migr_continue_comp_lin_links);

 out:
	isi_error_handle(error, error_out);
}

/*
 * Open the parent fd and set appropriate starting fields for a directory.
 */
static void
initialize_parent(struct resync_dir_state *cur_parent,
    struct parent_entry parent, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (cur_parent->dir_fd != 1)
		close(cur_parent->dir_fd);

	/* Open the parent directory. */
	cur_parent->dir_fd = ifs_lin_open(parent.lin, snapid, O_RDONLY);
	if (cur_parent->dir_fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx: %s",
		    parent.lin, strerror(errno));
		goto out;
	}
	ASSERT(cur_parent->dir_fd != 0);

	cur_parent->dir_lin = parent.lin;
	cur_parent->expected_links = parent.count;
	cur_parent->resume_cookie = 0;

 out:
	isi_error_handle(error, error_out);
}


/*
 * Lookup Dirent by Lin
 * Given (lin, resume_cookie), lookup the dirent
 * If resume_cookie != 0 and this is a single link lookup, then it looks only
 * at a specific hash bin for dirent. Otherwise the whole directory
 * is scanned to check for dirent.
 * Returns the dirent structure to the caller
 *
 * Return value:
 * true if found
 * false if not found or there was an error (in which case error_out will be
 * filled in too).
 */
static bool
resync_dirent_lookup(struct resync_parents_state *pstate,
    struct dirent *d_out, struct isi_error **error_out)
{
	bool found = false;
	char dirents[READ_DIRENTS_SIZE];
	int i, nbytes;
	unsigned int item_count = READ_DIRENTS_NUM;
	uint32_t hash = 0;
	uint64_t cookie_out[READ_DIRENTS_NUM];
	struct dirent *entryp;
	struct resync_dir_state *cur_parent = &pstate->cur_parent;
	struct isi_error *error = NULL;

	ASSERT(cur_parent->dir_fd > 0);
	ASSERT(pstate->dirent_lin > 0);

	if (pstate->num_links == 1 && cur_parent->resume_cookie != 0)
		hash = rdp_cookie63_to_hash(cur_parent->resume_cookie);

	while (true) {
		item_count = READ_DIRENTS_NUM;
		nbytes = readdirplus(cur_parent->dir_fd, RDP_NOTRANS,
		    &cur_parent->resume_cookie, READ_DIRENTS_SIZE, dirents,
		    READ_DIRENTS_SIZE, &item_count, NULL, cookie_out);

		/* We are not getting required information, fail */
		if (nbytes < 0) {
			error = isi_system_error_new(errno,
			    "readdirplus lookup failure: %s", strerror(errno));
			break;
		}

		/* There is nothing to read */
		if (nbytes == 0)
			break;

		entryp = (struct dirent *) dirents;
		for (i = 0; i < item_count ; i++) {
			/*
			 * Check that lin number matches with dirent lin
			 * returned by readdirplus. If so return TRUE.
			 */
			if (pstate->dirent_lin == entryp->d_fileno) {
				*d_out = *entryp;
				found = true;
				cur_parent->resume_cookie = cookie_out[i];
				cur_parent->index++;
				log(DEBUG, "found dirent name: %s enc %d lin "
				    "%llx\n", entryp->d_name,
				    entryp->d_encoding, entryp->d_fileno);
				goto out;
			}
			entryp = (struct dirent*) ((char*) entryp +
			    entryp->d_reclen);
		}

		cur_parent->resume_cookie = cookie_out[item_count - 1];

		/* If the hash value is not same, then exit loop */
		if (hash && (hash !=
		    rdp_cookie63_to_hash(cur_parent->resume_cookie)))
			break;
	}

 out:
	isi_error_handle(error, error_out);
	return found;
}

/*
 * Do the hash-based dirent lookup for a singly linked LIN.
 * Returns the dirent structure to the caller
 *
 * Return value:
 * true if found
 * false if not found or there was an error (in which case error_out will be
 * filled in too).
 */
static bool
single_link_dirent_lookup(struct resync_parents_state *pstate,
    ifs_snapid_t snapid, struct dirent *d_out, struct isi_error **error_out)
{
	bool found = false;
	int fd = -1;
	int ret = -1;
	uint32_t hash;
	ifs_lin_t dirent_lin = pstate->dirent_lin;
	struct resync_dir_state *cur_parent = &pstate->cur_parent;
	struct isi_error *error = NULL;

	/* Get the first parent hash. */
	fd = ifs_lin_open(dirent_lin, snapid, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx: %s", dirent_lin,
		    strerror(errno));
		goto out;
	}
	ASSERT(fd != 0);

	ret = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "first_parent_hash", (void *)&hash, sizeof(hash));
	if (ret != sizeof(hash)) {
		error = isi_system_error_new(errno,
		    "Failed to read first_parent_hash for lin %llx: %s",
		    dirent_lin, strerror(errno));
		goto out;
	}

	/*
	 * Hash is not valid. We need to look for dirent by reading all
	 * entries. Pass resume_cookie as 0 to do search from beginning.
	 */
	cur_parent->resume_cookie = (hash <= 0) ? 0 : rdp_cookie63(hash, 0);

	found = resync_dirent_lookup(pstate, d_out, &error);
	if (error)
		goto out;

 out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
	return found;
}

/*
 * Cleans up the current parent and resync parents state fields if we are done
 * processing.
 * Returns true iff we are done processing the current dirent LIN.
 */
static bool
cleanup_resync_state(struct migr_pworker_ctx *pw_ctx)
{
	bool done = false;
	struct resync_parents_state *pstate = &pw_ctx->resync_parents_state;
	struct resync_dir_state *cur_parent = &pstate->cur_parent;

	ASSERT(cur_parent->index <= cur_parent->expected_links);
	if (cur_parent->index == cur_parent->expected_links) {
		close(cur_parent->dir_fd);
		cur_parent->dir_fd = -1;
		cur_parent->dir_lin = 0;
		cur_parent->expected_links = 0;
		cur_parent->index = 0;
		cur_parent->resume_cookie = 0;

		pstate->index++;
		ASSERT(pstate->index <= pstate->num_parents);
		if (pstate->index == pstate->num_parents) {
			pstate->dirent_lin = 0;
			free(pstate->parents);
			pstate->parents = NULL;
			pstate->num_parents = 0;
			pstate->num_links = 0;
			pstate->index = 0;
			pstate->is_dir = false;
			pstate->skip_comp_store = false;
			/*
			 * Dont see any other cases here.
			 * It should only be called from either
			 * RESYNC_LINKS phase or CLEANUP phase.
			 */
			if (!pw_ctx->restore) {
				migr_call_next(pw_ctx,
				    migr_continue_regular_comp_lin_links);
			} else {
				migr_call_next(pw_ctx,
				    stf_continue_change_transfer);
			}
			done = true;
		}
	}

	return done;
}

/*
 * Processes the next link for a compliance resync LIN.
 * We get the next link for the resync LIN and construct a LINK_MSG.
 * Hash-based lookup is used for singly linked LINs.
 */
static void
migr_continue_comp_lin_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool found = false;
	ifs_lin_t parent_lin;
	ifs_lin_t dirent_lin;
	ifs_snapid_t snapid;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct dirent d_out;
	struct resync_parents_state *pstate = &pw_ctx->resync_parents_state;
	struct resync_dir_state *cur_parent = &pstate->cur_parent;
	struct isi_error *error = NULL;
	bool is_comp_store = false;
	bool processing_links = false;

	dirent_lin = pstate->dirent_lin;
	parent_lin = cur_parent->dir_lin;
	snapid = cc_ctx->snap2;

	log(TRACE, "%s", __func__);

	/* Check for the beginning of a new parent directory. */
	if (cur_parent->index == 0) {
		initialize_parent(cur_parent, pstate->parents[pstate->index],
		    snapid, &error);
		if (error)
			goto out;
	}

	/*
	 * If we need to skip comp store, then check if the current directory
	 * belongs to comp store.
	 */
	is_comp_store = is_compliance_store(cur_parent->dir_fd, &error);
	if (error)
		goto out;

	/* Check for singly linked LINs. */
	if (pstate->num_links == 1) {
		found = single_link_dirent_lookup(pstate, snapid, &d_out,
		    &error);
		if (error)
			goto out;
	}

	if (!found) {
		/* Either the hash-based lookup for single links failed or we
		 * have multiple hard links.*/
		found = resync_dirent_lookup(pstate, &d_out, &error);
		if (error)
			goto out;

		if (!found) {
			error = isi_system_error_new(ENOENT,
			    "Unable to find dirent for parent lin %llx and "
			    "child lin %llx", parent_lin, dirent_lin);
			goto out;
		}
	}

	ASSERT(dirent_lin == d_out.d_fileno);

	if (pstate->skip_comp_store && is_comp_store)
		goto next;

	if (pw_ctx->restore) {
		/* Restore only uses this code path to resync modified
		 * and committed files. Here there is no requirements to
		 * order the parents, since they all must exist.
		 */
		send_link_message(pw_ctx,
		    pw_ctx->resync_parents_state.cur_parent.dir_lin, &d_out,
		    !pstate->is_dir && pstate->num_links == 1, true, false,
		    true, &error);
		if (error)
			goto out;
		/* Update stats. */
		migr_update_comp_link_stats(pw_ctx->stf_cur_stats,
		    pstate->is_dir);
	} else {
		stf_comp_rplookup_lin(pw_ctx,
		    pw_ctx->resync_parents_state.cur_parent.dir_lin, &d_out,
		    pstate->num_links, true);
		processing_links = true;
	}

next:
	if (!processing_links) {
		/* Check to see if we are done with a parent/LIN. */
		cleanup_resync_state(pw_ctx);
	}

 out:
	isi_error_handle(error, error_out);
}

void
stf_continue_comp_conflict_cleanup(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	ifs_lin_t lin = pw_ctx->cur_lin;
	bool committed;
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct resync_parents_state *pstate = &pw_ctx->resync_parents_state;
	struct rep_entry old_entry;

	log(TRACE, __func__);

	/*
	 * See if the lin is committed.
	 *
	 * We assume that the cur_lin has a link_count > 0, and therefore must
	 * exist (the condition_met() check when grabbing the next lin skips
	 * lins with lcount < 1 for this phase). However, during restore, we
	 * also skip compliance directories during change compute. Therefore,
	 * if a user deletes a LIN from the compliance store, we do not
	 * decrement the LINs lcount. So, if a LIN has been removed entirely
	 * from the primary namespace and the compliance store, we've only
	 * decremented lcount for each unlink in the primary namespace, but not
	 * the compliance store. So, we can end up with lcount > 0, even though
	 * the LIN doesn't exist. If a LIN doesn't exist, properly set its
	 * lcount to 0.
	 */
	committed = is_worm_committed(lin, cc_ctx->snap2, &error);

	if (error) {
		if (!isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
			goto out;

		if (isi_system_error_get_error(
		    (const struct isi_system_error *)error) != ENOENT)
			goto out;

		/* LIN doesn't exist in latest snapshot. */
		isi_error_free(error);
		error = NULL;

		/* If we are here, it must be because the file was committed
		 * before, expired, and all primary/canonical links were
		 * deleted. Assert that this is the case. */
		committed = is_worm_committed(lin, cc_ctx->snap1, &error);
		if (error)
			goto out;

		if (!committed) {
			error = isi_system_error_new(EINVAL, "Link count "
			    "mismatch detected. Lin %llx does not exist in "
			    "the latest snapshot, but computed a link count "
			    "greater than 0 (%d)", lin,
			    pw_ctx->rep_entry.lcount);
			goto out;
		}

		/* This file was deleted. Change compute skips compliance
		 * directories and did not decrement lcount to 0. Do it now. */
		old_entry = pw_ctx->rep_entry;
		pw_ctx->rep_entry.lcount = 0;

		pw_ctx->stf_cur_stats->change_compute->files_unlinked =
		    old_entry.lcount;

		set_entry(lin, &pw_ctx->rep_entry, cc_ctx->rep2, 0, NULL,
		    &error);

		goto out;
	}

	if (!committed)
		goto out;
	else {
		committed = is_worm_committed(lin, cc_ctx->snap1, &error);
		if (error && isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			error = NULL;
		} else if (error || committed)
			goto out;
	}

	/* Get all parents for the lin. */
	if (pstate->parents != NULL) {
		free(pstate->parents);
		pstate->parents = NULL;
	}
	pstate->num_parents = 0;
	pstate->skip_comp_store = true;

	pstate->num_links = get_all_parent_lins(pw_ctx->cur_lin,
	    pw_ctx->chg_ctx->snap2, pw_ctx->resync_entry.is_dir,
	    &pstate->parents, &pstate->num_parents, &error);
	if (error)
		goto out;

	pstate->dirent_lin = pw_ctx->cur_lin;
	pstate->is_dir = pw_ctx->resync_entry.is_dir;
	pstate->index = 0;
	pstate->cur_parent.dir_fd = -1;
	pstate->cur_parent.index = 0;

	migr_call_next(pw_ctx, migr_continue_comp_lin_links);

out:
	isi_error_handle(error, error_out);
}

static void
comp_target_finish_work_item(struct migr_pworker_ctx *pw_ctx)
{
	struct generic_msg msg = {};

	log(TRACE, "%s", __func__);

	//Mark the work item as done and get the next one
	migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
	pw_ctx->done = 1;
	pw_ctx->pending_done_msg = true;

	/*
	 * Bug 234439
	 * This failpoint causes the worker to die before sending
	 * the DONE_MSG needed to clean up a WORK_COMP_COMMIT
	 * work item, simulating a network failure/random
	 * death that can cause Bug 234439 by never cleaning
	 * up the work item.
	 */
	UFAIL_POINT_CODE(pw_comp_finish_work_fail,
		send_err_msg(pw_ctx, EIO, "err from pw_comp_finish_work_fail");
		log(FATAL, "Fake EIO pw_comp_finish_work_fail; killing worker");
	);
	msg.head.type = DONE_MSG;
	msg_send(pw_ctx->sworker, &msg);
	/*
	 * We could race a split request, so in this case send a
	 * split failed and abort that process
	 */
	if (pw_ctx->comp_target_split_pending) {
		msg.head.type = SPLIT_RESP_MSG;
		msg.body.split_resp.succeeded = false;
		msg_send(pw_ctx->coord, &msg);
		pw_ctx->comp_target_split_pending = false;
	}
	log(TRACE, "%s exit", __func__);
}

static void
migr_comp_target_send_work_range(struct migr_pworker_ctx *pw_ctx)
{
	struct work_restart_state *work = pw_ctx->work;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;

	log(TRACE, "%s: type: %d min: %llx chkpt: %llx max: %llx", __func__,
	    work->rt, work->min_lin, work->chkpt_lin, work->max_lin);

        /*
         * Bug 234439
         * Recover from a worker crash or network failure in
         * comp_target_finish_work_item(). If we send a
         * COMP_TARGET_WORK_RANGE_MSG with 0 min/max_lin, the
         * sworker will assert. Mark the work item as finished
         * and don't send a COMP_TARGET_WORK_RANGE_MSG.
         */
        if (work->min_lin == 0 && work->max_lin == 0) {
                log(DEBUG, "%s: finishing empty work item", __func__);
                comp_target_finish_work_item(pw_ctx);
                return;
        }

	resp.head.type =
	    build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);

	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1, work->rt);

	if (work->chkpt_lin > work->min_lin) {
		//This is a checkpoint restart, just tell the target a
		//shorter range to work with
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MIN_LIN,
		    1, work->chkpt_lin);
	} else {
		ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MIN_LIN,
		    1, work->min_lin);
	}
	ript_msg_set_field_uint64(ript_resp, RMF_COMP_TARGET_MAX_LIN,
	    1, work->max_lin);
	msg_send(pw_ctx->sworker, &resp);

	migr_wait_for_response(pw_ctx, MIGR_WAIT_COMP_TARGET_CHKPT);

	log(TRACE, "%s exit", __func__);
}

static void
migr_continue_comp_target_process(struct migr_pworker_ctx *pw_ctx,
    enum ript_messages msg_type, struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//If this is the first work item, we need to get the range
	log(DEBUG, "%s: min: %llx max: %llx", __func__,
	    work->min_lin, work->max_lin);
	if (work->max_lin == -1) {
		log(DEBUG, "%s: first work item", __func__);
		//First get the first/last lin from the target
		//Construct the message
		resp.head.type = build_ript_msg_type(msg_type);
		ript_resp = &resp.body.ript;
		ript_msg_init(ript_resp);

		//Send the message and either block and
		//wait for reply or set the callback
		msg_send(pw_ctx->sworker, &resp);

		//When we get the COMP_TARGET_WORK_RANGE_MSG,
		//we will set the work items and such
		migr_wait_for_response(pw_ctx, MIGR_WAIT_COMP_TARGET_BEGIN);
	} else {
		log(DEBUG, "%s: sending work range", __func__);
		//We finished the first handshake, so we should send
		//the COMP_TARGET_WORK_RANGE_MSG and then wait for a split or
		//checkpoint messages
		migr_comp_target_send_work_range(pw_ctx);

	}

	isi_error_handle(error, error_out);

	log(TRACE, "%s exit", __func__);
}



void
migr_continue_comp_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s: enter", __func__);
	//Detect if this phase is being restarted during resync-prep
	if (pw_ctx->flags & FLAG_FAILBACK_PREP) {
		//We can safely skip this, because allow write will have
		//finished all of these work items
		log(DEBUG, "%s: failback prep detected, finishing work item",
		    __func__);
		comp_target_finish_work_item(pw_ctx);
		return;
	}

	migr_continue_comp_target_process(pw_ctx, COMP_COMMIT_BEGIN_MSG,
	    error_out);
	log(TRACE, "%s: exit", __func__);
}

void
migr_continue_comp_dir_dels(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s: enter", __func__);

	migr_continue_comp_target_process(pw_ctx, COMP_DIR_DELS_BEGIN_MSG,
	    error_out);
	log(TRACE, "%s: exit", __func__);
}

static void
comp_target_work_range_process(struct generic_msg *m, void *ctx,
    migr_pworker_func_t func, struct isi_error **error_out)
{
	struct migr_pworker_ctx *pw_ctx = (struct migr_pworker_ctx *)ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	//We receive the range as the result of a begin message
	//Update the first work item and process it

	log(TRACE, "%s", __func__);

	ASSERT(work->min_lin == ROOT_LIN);
	ASSERT(work->max_lin == -1);

	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_MIN_LIN, 1,
	    &work->min_lin, &error, out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_MAX_LIN, 1,
	    &work->max_lin, &error, out);
	work->lin = work->min_lin;
	work->chkpt_lin = work->min_lin;
	set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
	    pw_ctx->cur_rep_ctx, &error);
	if (error) {
		goto out;
	}

	log(TRACE, "%s: min: %llx max: %llx", __func__, work->min_lin, 
	    work->max_lin);
	//If the range we get back is actually 0->0, then that means there
	//is no work to do, so just mark it as finished
	if (work->min_lin == 0 && work->max_lin == 0) {
		log(DEBUG, "%s: finishing empty work item", __func__);
		comp_target_finish_work_item(pw_ctx);
	} else {
		migr_call_next(pw_ctx, func);
		migr_reschedule_work(pw_ctx);
	}

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

int
comp_target_work_range_callback(struct generic_msg *m, void *ctx)
{
	enum work_restart_type rt;
	struct migr_pworker_ctx *pw_ctx = (struct migr_pworker_ctx *)ctx;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(uint32, &m->body.ript, RMF_WORK_TYPE, 1, &rt,
	    &error, out);

	switch (rt) {
	case WORK_COMP_COMMIT:
		comp_target_work_range_process(m, ctx,
		    migr_continue_comp_commit, &error);
		if (error) {
			//Send error back
			send_comp_source_error(pw_ctx->coord,
			    "Failure writing compliance commit work item",
			    E_SIQ_COMP_COMMIT, error);
			isi_error_free(error);
			error = NULL;
		}
		break;
	case WORK_COMP_MAP_PROCESS:
		comp_target_work_range_process(m, ctx,
		    migr_continue_comp_map_process, &error);
		if (error) {
			//Send error back
			send_comp_source_error(pw_ctx->coord,
			    "Failure writing compliance map process work item",
			    E_SIQ_COMP_MAP_PROCESS, error);
			isi_error_free(error);
			error = NULL;
		}
		break;
	case WORK_CT_COMP_DIR_DELS:
		comp_target_work_range_process(m, ctx,
		    migr_continue_comp_dir_dels, &error);
		if (error) {
			//Send error back
			send_comp_source_error(pw_ctx->coord,
			    "Failure writing compliance dir dels work item",
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
		send_comp_source_error(pw_ctx->coord,
		    "Failed to get compliance work type",
		    E_SIQ_MSG_FIELD, error);
	}

	return 0;
}

/*
 * This does the work split specifically for worm commits and compliance map
 * processing, since the work is done on the target side.
 */
bool
try_comp_target_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct siq_ript_msg *ript_resp = NULL;

	log(TRACE, "%s", __func__);

	//We should probably not already be trying to do a split
	ASSERT(!pw_ctx->comp_target_split_pending);

	//First we need to set a flag saying we are doing a split
	pw_ctx->comp_target_split_pending = true;

	//Send the checkpoint message to the target
	resp.head.type = build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG);
	ript_resp = &resp.body.ript;
	ript_msg_init(ript_resp);
	ript_msg_set_field_uint32(ript_resp, RMF_WORK_TYPE, 1,
	    pw_ctx->work->rt);
	msg_send(pw_ctx->sworker, &resp);

	//Wait for it to come back and we will resume this procedure
	//in the checkpoint callback

	//Doesn't actually matter what we actually return here, since
	//we special case it in try_work_split

	log(TRACE, "%s exit", __func__);
	return false;
}

static void
do_comp_target_split(struct migr_pworker_ctx *pw_ctx, ifs_lin_t split_lin,
    struct isi_error **error_out)
{
	struct work_restart_state *work = pw_ctx->work;
	struct generic_msg msg = {};
	struct work_restart_state new_work = {};
	bool split_result = false;
	struct isi_error *error = NULL;

	log(TRACE, "%s: "
	    "min: %llx chkpt: %llx max: %llx split: %llx", __func__,
	    work->min_lin, work->chkpt_lin, work->max_lin, split_lin);

	//Check the work split and send it if it is valid
	bzero(&new_work, sizeof(struct work_restart_state));
	if (split_lin != INVALID_LIN) {
		new_work.rt = work->rt;
		new_work.min_lin = split_lin;
		new_work.chkpt_lin = split_lin;
		new_work.max_lin = work->max_lin;

		work->max_lin = split_lin;

		log(DEBUG, "%s: old work item min: %llx max: %llx "
		    "new work item min: %llx max: %llx", __func__,
		    work->min_lin, work->max_lin, new_work.min_lin,
		    new_work.max_lin);

		add_split_work_entry(pw_ctx, pw_ctx->wi_lin, work, NULL, 0,
		    pw_ctx->split_wi_lin, &new_work, NULL, 0, &error);
		if (error) {
			goto out;
		}
		split_result = true;
	}

	//Send the new work range to the target
	migr_comp_target_send_work_range(pw_ctx);

	//Respond to the coordinator's split request
	msg.head.type = SPLIT_RESP_MSG;
	msg.body.split_resp.succeeded = split_result;
	msg_send(pw_ctx->coord, &msg);

	//Disable the flag
	pw_ctx->comp_target_split_pending = false;

	//Reset other split variables that we skipped earlier
	pw_ctx->split_req_pending = false;
	pw_ctx->split_wi_lin = 0;

out:
	isi_error_handle(error, error_out);
	log(TRACE, "%s exit", __func__);
}

int
comp_target_checkpoint_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = (struct migr_pworker_ctx *)ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct siq_ript_msg *ript = &m->body.ript;
	ifs_lin_t checkpoint_lin = INVALID_LIN;
	ifs_lin_t split_lin = INVALID_LIN;
	uint8_t remote_done = 0;
	enum work_restart_type rt = WORK_FINISH;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	//Get the message fields
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_WORK_TYPE, 1, &rt,
	    &error, out);

	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_CURR_LIN, 1,
	    &checkpoint_lin, &error, out);
	RIPT_MSG_GET_FIELD(uint8, ript, RMF_COMP_TARGET_DONE, 1,
	    &remote_done, &error, out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_COMP_TARGET_SPLIT_LIN, 1,
	    &split_lin, &error, out);

	log(TRACE, "%s: checkpoint_lin: %llx remote_done: %d split_lin: %llx",
	    __func__, checkpoint_lin, remote_done, split_lin);

	//There could be a race condition with split requests
	if (pw_ctx->done || pw_ctx->pending_done_msg) {
		//Ignore
		log(DEBUG, "%s: done detected - early return", __func__);
		goto out;
	}

	//Update the work item with new checkpoint
	ASSERT(checkpoint_lin >= work->min_lin, "%llx < %llx", checkpoint_lin,
	    work->min_lin);
	ASSERT(checkpoint_lin <= work->max_lin, "%llx > %llx", checkpoint_lin,
	    work->max_lin);
	ASSERT(checkpoint_lin >= work->chkpt_lin, "%llx < %llx", checkpoint_lin,
	    work->chkpt_lin);
	work->lin = checkpoint_lin;
	work->chkpt_lin = checkpoint_lin;

	//Write to disk
	set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
	    pw_ctx->cur_rep_ctx, &error);
	if (error) {
		goto out;
	}

	//If the checkpoint is the last item, then we are done
	if (checkpoint_lin >= work->max_lin) {
		log(DEBUG, "%s: finishing work item", __func__);
		//Hopefully we are on the same page as the target
		ASSERT(remote_done);

		comp_target_finish_work_item(pw_ctx);
	} else {
		// If this is in response to us asking for a checkpoint
		// for a split, we should do the split here and then start the
		// work again
		log(DEBUG, "%s: continuing work item", __func__);
		if (pw_ctx->comp_target_split_pending && remote_done) {
			do_comp_target_split(pw_ctx, split_lin, &error);
			if (error) {
				goto out;
			}
		}
	}

out:
	if (rt == WORK_FINISH && error != NULL) {
		send_comp_source_error(pw_ctx->coord,
		    "Failed to get compliance work type",
		    E_SIQ_MSG_FIELD, error);
		isi_error_free(error);
		error = NULL;
	}

	if (error) {
		switch (rt) {
		case WORK_COMP_COMMIT:
			//Send error back
			send_comp_source_error(pw_ctx->coord, "Failure "
			    "checkpointing compliance commit work item",
			    E_SIQ_COMP_COMMIT, error);
			isi_error_free(error);
			error = NULL;
			break;
		case WORK_COMP_MAP_PROCESS:
			//Send error back
			send_comp_source_error(pw_ctx->coord, "Failure "
			    "checkpointing compliance map process work item",
			    E_SIQ_COMP_MAP_PROCESS, error);
			isi_error_free(error);
			error = NULL;
			break;
		case WORK_CT_COMP_DIR_DELS:
			//Send error back
			send_comp_source_error(pw_ctx->coord, "Failure "
			    "checkpointing compliance dir dels work item",
				E_SIQ_DEL_LIN, error);
			isi_error_free(error);
			error = NULL;
			break;
		default:
			log(FATAL, "%s: Invalid work range type: %s", __func__,
			    work_type_str(rt));
		}
	}

	return 0;
}

void
migr_continue_comp_map_process(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	migr_continue_comp_target_process(pw_ctx, COMP_MAP_BEGIN_MSG,
	    error_out);
}

/*
 * Function to send a link message for every new
 * directory found in the path.
 */
static void
stf_comp_rplookup_single_link(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct comp_rplookup *rplookup = &pw_ctx->comp_rplookup;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct rep_entry rep = {};
	struct isi_error *error = NULL;
	bool found = false;
	struct siq_stat *stats = pw_ctx->stf_cur_stats;
	struct dirent *entryp = NULL;
	struct lin_list_entry resync_ent = {};
	bool done = false;

	if (rplookup->entryp == NULL) {
		rplookup->entryp = (struct dirent *)rplookup->buf;
		rplookup->last_parent_lin = pw_ctx->cur_snap_root_lin;
	}

	entryp = rplookup->entryp;

	log(TRACE, " %s: name %s, fileno %llx, encoding %d", __func__,
	    entryp->d_name, entryp->d_fileno, entryp->d_encoding);

	if (rplookup->resync_phase) {
		found = get_lin_list_entry(entryp->d_fileno, &resync_ent,
		    cc_ctx->resync, &error);
		if (error)
			goto out;
	} else {
		found = get_entry(entryp->d_fileno, &rep, pw_ctx->cur_rep_ctx,
		    &error);
		if (error)
			goto out;
	}

	if (pw_ctx->restore) {
		revert_entry(pw_ctx, entryp->d_fileno, &rep,
		    &error);
		if (error)
			goto out;
	}

	/*
	 * Send a link message for all newly created directories.
	 */
	if (found &&
	    ((rplookup->resync_phase && found) || rep.not_on_target)) {
		send_link_message(pw_ctx, rplookup->last_parent_lin, entryp,
		    false, false, true, false, &error);
		if (error)
			goto out;

		stats->dirs->linked->src++;
	}

	rplookup->last_parent_lin = entryp->d_fileno;
	rplookup->entryp = (struct dirent*) ((char*) entryp +
	    entryp->d_reclen);

	/*
	 * If we have reached end of list, then go back regular
	 * link call.
	 */
	if (((char *)rplookup->entryp - (char *)rplookup->buf) >=
	    rplookup->buf_cnt) {
		rplookup->last_parent_lin = 0;
		rplookup->entryp = NULL;
		rplookup->buf_cnt = 0;
		/*
		 * Finally send the link message for current dirent
		 * stored.
		 */
		send_link_message(pw_ctx, rplookup->dir_lin,
		    &rplookup->entry,
		    (rplookup->entry.d_type == DT_REG &&
		    rplookup->lcount == 1), false, false,
		    rplookup->resync_phase, &error);
		if (error)
			goto out;

		if (rplookup->resync_phase) {
			/* Update stats. */
			migr_update_comp_link_stats(pw_ctx->stf_cur_stats,
			    rplookup->entry.d_type == DT_DIR);
			memset(&rplookup->entry, 0, sizeof(struct dirent));
			done = cleanup_resync_state(pw_ctx);
			if (!done)
				migr_call_next(pw_ctx,
				    migr_continue_comp_lin_links);
		} else {
			memset(&rplookup->entry, 0, sizeof(struct dirent));
			migr_call_next(pw_ctx, stf_continue_lin_link);
		}

		rplookup->resync_phase = false;
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Function which gets list of all parents upto root lin.
 */
void
stf_comp_rplookup_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct comp_rplookup *rplookup = &pw_ctx->comp_rplookup;
	struct isi_error *error = NULL;
	size_t buflen_out = 0;
	int MAXRETRIES = 2;
	int INIT_DIR_DEPTH = 100;
	int i;
	int ret = -1;

	log(TRACE, "%s", __func__);

	ASSERT(rplookup->dir_lin > 0);

	/*
	 * Allocate a default buffer to begin.
	 */
	if (rplookup->buf_len <= 0) {
		ASSERT(rplookup->buf == NULL);
		rplookup->buf_len = sizeof(struct dirent) * INIT_DIR_DEPTH;
		rplookup->buf = malloc(rplookup->buf_len);
		ASSERT(rplookup->buf != NULL);
	}

	for (i = 0; i < MAXRETRIES; i++) {
		/*
		 * Get the dirent list upto parent root.
		 */
		ret = pctl2_lin_get_path_plus(pw_ctx->cur_snap_root_lin,
		    rplookup->dir_lin, pw_ctx->cur_snap, 0, 0, ENC_FLAG_NOTRANS,
		    0, NULL, NULL, rplookup->buf_len, rplookup->buf,
		    &buflen_out, 0, NULL, NULL,
		    PCTL2_LIN_GET_PATH_NO_PERM_CHECK);  // bug 220379
		if (ret != 0) {
			if (errno == ENOSPC) {
				rplookup->buf_len = buflen_out;
				rplookup->buf = realloc(rplookup->buf,
				    buflen_out);
				ASSERT(rplookup->buf != NULL);
				continue;
			} else {
				error = isi_system_error_new(errno,
				    "pctl2_lin_get_path_plus failed for "
				    "lin %{}", lin_fmt(rplookup->dir_lin));
				goto out;
			}
		} else {
			rplookup->buf_cnt = buflen_out;
			break;
		}
	}

	if (i == MAXRETRIES) {
		error = isi_system_error_new(ENOSPC,
		    "pctl2_lin_get_path_plus failing for lin %{}",
		    lin_fmt(rplookup->dir_lin));
		goto out;
	}

	/*
	 * Now assign the entryp to the begining of the buffer.
	 */
	migr_call_next(pw_ctx, stf_comp_rplookup_single_link);

out:
	isi_error_handle(error, error_out);
}

int
comp_map_callback(struct generic_msg *m, void *ctx)
{
	bool found;
	char lmap_name[MAXNAMLEN];
	int max_ops = SBT_BULK_MAX_OPS / 2;
	uint8_t *buf = NULL;
	uint32_t i;
	uint32_t num_entries;
	uint32_t buf_len;
	uint32_t num_resp_entries = 0;
	ifs_lin_t chkpt_lin;
	ifs_lin_t dlin;
	struct siq_ript_msg *ript = &m->body.ript;
	struct lmap_update entries[max_ops];
	struct lmap_update resp_entries[max_ops];
	struct migr_pworker_ctx *pw_ctx = (struct migr_pworker_ctx *)ctx;
	struct isi_error *error = NULL;

	// Note: Process a max of SBT_BULK_MAX_OPS / 2 entries at a time since
	// we need two linmap/repstate log entries per compliance map entry.

	RIPT_MSG_GET_FIELD(uint32, ript, RMF_SIZE, 1, &num_entries, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_CHKPT_LIN, 1, &chkpt_lin, &error,
	    out);

	ript_msg_get_field_bytestream(ript, RMF_COMP_MAP_BUF, 1,
	    (uint8_t **)(&buf), &buf_len, &error);
	if (error)
		goto out;
	memcpy(entries, buf, buf_len);

	// Make sure we opened the mirrored linmap.
	get_mirrored_lmap_name(lmap_name, pw_ctx->policy_id);
	if (!pw_ctx->lmap_mirror_ctx) {
		open_linmap(pw_ctx->policy_id, lmap_name,
		    &pw_ctx->lmap_mirror_ctx, &error);
		if (error)
			goto out;
	}

	log(DEBUG, "%s: Processing %u compliance map entries", __func__,
	    num_entries);
	// Process the compliance map entries.
	for (i = 0; i < num_entries; i++) {
		// First check for the new dlin as dynamic lookup for
		// database resync could have already resolved the mirror
		// mapping.
		found = get_mapping(entries[i].slin, &dlin,
		    pw_ctx->lmap_mirror_ctx, &error);
		if (error)
			goto out;

		if (found) {
			// Log the linmap update.

			lmap_log_add_entry(pw_ctx->lmap_mirror_ctx,
			    &pw_ctx->lmap_mirror_log, entries[i].dlin,
			    dlin, LMAP_SET, 0, &error);
			if (error)
				goto out;

			lmap_log_add_entry(pw_ctx->lmap_mirror_ctx,
			    &pw_ctx->lmap_mirror_log, entries[i].slin,
			    0, LMAP_REM, 0, &error);
			if (error)
				goto out;
		} else {
			// Check if the mapping has already been resolved.
			found = get_mapping(entries[i].dlin, &dlin,
			    pw_ctx->lmap_mirror_ctx, &error);
			if (error)
				goto out;
		}

		if (found) {
			// Add flipped entry to the response list.
			resp_entries[num_resp_entries].slin = dlin;
			resp_entries[num_resp_entries].dlin = entries[i].dlin;
			num_resp_entries++;
		}
	}

	// Flush the source mirrored linmap.
	flush_lmap_log(&pw_ctx->lmap_mirror_log, pw_ctx->lmap_mirror_ctx,
	    &error);
	if (error)
		goto out;

	// Respond to the sworker with the updated entries.
	log(DEBUG, "%s: sending %u compliance map entries", __func__,
	    num_resp_entries);
	send_compliance_map_entries(pw_ctx->sworker, num_resp_entries,
	    resp_entries, chkpt_lin);

out:
	if (error) {
		// Send error back.
		send_comp_source_error(pw_ctx->coord, "Failure processing "
		    "compliance map entries", E_SIQ_COMP_MAP_PROCESS, error);
		isi_error_free(error);
		error = NULL;
	}

	return 0;
}

/*
 * Queue a reverse parent lookup call for the current lin 'cur_lin'.
 * This would ensure we call link messages for all the newly created
 * parents before the actual link message for the dirent itself.
 */
void
stf_comp_rplookup_lin(struct migr_pworker_ctx *pw_ctx, ifs_lin_t cur_lin,
    struct dirent *de, int lcount, bool resync_phase)
{
	struct comp_rplookup *rplookup = &pw_ctx->comp_rplookup;
	struct dirent *rp_de = NULL;

	log(TRACE, "%s", __func__);

	rplookup->dir_lin = cur_lin;
	rplookup->resync_phase = resync_phase;

	rp_de = &rplookup->entry;
	rp_de->d_fileno = de->d_fileno;
	strncpy(rp_de->d_name, de->d_name, MAXNAMELEN);
	rp_de->d_encoding = de->d_encoding;
	rp_de->d_type = de->d_type;
	rplookup->lcount = lcount;
	migr_call_next(pw_ctx, stf_comp_rplookup_links);
}
