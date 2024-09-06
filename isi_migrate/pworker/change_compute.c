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
#include <isi_util/syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <isi_util/util_adt.h>
#include <isi_ufp/isi_ufp.h>

#include "pworker.h"

/**
 * repstate wrapper API to get differential entry.
 *
 * return true if an entry was found in either the differential
 * or the main repstate file.
 */
static bool
get_diff_entry(u_int64_t lin, struct rep_entry *rep_entry,
    struct rep_ctx *rep1, struct rep_ctx *rep2, bool *in_diff,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool found;

	ASSERT (rep_entry != NULL);

	/* Look for entry in rep2*/
	found = get_entry(lin, rep_entry, rep2, &error);
	if (error)
		goto out;

	if (in_diff)
		*in_diff = found;

	if (!found) {
		/* Look for entry in rep1*/
		found = get_entry(lin, rep_entry, rep1, &error);
		if (error)
			goto out;	
		if (found) {
			/* Copy entry from rep1 to rep2 */
			rep_entry->new_dir_parent = false;
			rep_entry->not_on_target = false;
			rep_entry->changed = false;
			rep_entry->lcount_set = false;
			rep_entry->lcount_reset = false;
			rep_entry->exception = false;
			rep_entry->not_on_target =
			    !((rep_entry->lcount - rep_entry->excl_lcount) > 0);
			rep_entry->hl_dir_lin = 0;
			rep_entry->hl_dir_offset = 0;
		}
	}
out:
	/*
	 * XXX: we should be clearing these out earlier
	 * in the process.  This indicates stale state left around
	 * from a failed job.
	 */
	ASSERT(!found || !rep_entry->flipped);
	isi_error_handle(error, error_out);
	return found;
	
}

/*
 * This function first clears in-memory work checkpoint fields.
 */
void
clear_checkpoint(struct work_restart_state *work)
{
	work->dircookie1 = work->dircookie2 = DIR_SLICE_MIN;
}

/**
 * If a plan has size or date predicates, find all of the parents of child_lin
 * and add them to the summary stf if they were part of the previous
 * replication set and are not already in the summary stf.  This is something
 * of a sledge hammer approach to dealing with inode-based predicates.
 */
static void
update_parent_exclusion(struct migr_pworker_ctx *pw_ctx,
    ifs_lin_t child_lin, struct isi_error **error_out)
{
	unsigned num_entries;
	struct isi_error *error = NULL;
	struct parent_entry pe_static[10];
	struct parent_entry *pe_dynamic = NULL;
    	struct changeset_ctx *cc_ctx;
	struct parent_entry *pep;
	struct rep_entry p_rep;
	struct stat st;
	bool found;
	int rv;
	int i;

	cc_ctx = pw_ctx->chg_ctx;


	/* If we don't have size or date predicates, no work to do */
	if (!pw_ctx->plan ||
	    (!pw_ctx->plan->need_size && !pw_ctx->plan->need_date))
		goto out;

	found = stat_lin_snap(child_lin, cc_ctx->snap2, &st, true, &error);
	if (error)
		goto out;

	if (!found)
		goto out;

	/**
	 * Use auto-memory for small number's of hard links, malloc'ing for
	 * large.
	 */
	if (st.st_nlink <= 10) {
		pep = pe_static;
		num_entries = 10;
	} else {
		pep = pe_dynamic = malloc(st.st_nlink *
		    sizeof(struct parent_entry));
		num_entries = st.st_nlink;
		ASSERT(pe_dynamic);
	}

	rv = pctl2_lin_get_parents(child_lin, cc_ctx->snap2, pep, num_entries,
	    &num_entries);
	if (rv) {
		error = isi_system_error_new(errno,
		    "Failed to find parents of lin %llx:%lld", child_lin,
		    cc_ctx->snap2);
		goto out;
	}

	/* Add each parent to the STF if they are in the previous repstate */
	for (i = 0; i < num_entries; i++) {
		found = get_entry(pep[i].lin, &p_rep, cc_ctx->rep1, &error);
		if (error)
			goto out;
		if (found) {
			set_stf_entry(pep[i].lin, cc_ctx->summ_stf, &error);
			if (error)
				goto out;
		}
	}

 out:
	if (pe_dynamic)
		free(pe_dynamic);
	isi_error_handle(error, error_out);
}


/**
 * Helper used when scanning STF's if and only if the policy has
 * a path predicate.  This is called on modified directory lins
 * and marks them for_path if the immediate filename pointing
 * at the directory has changed.  If the name has changed, this
 * lin is also enqueued on a worklist to allow all descendent
 * directories to be queued for path scanning as well.
 */
static void
update_parent_path(struct migr_pworker_ctx *pw_ctx,
    ifs_lin_t lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
    	struct changeset_ctx *cc_ctx;
	struct rep_entry rep_entry, old_entry;
	bool found;
	ifs_lin_t p1lin, p2lin;
	char *fname1 = NULL, *fname2 = NULL;
	bool in_diff;

	cc_ctx = pw_ctx->chg_ctx;

	/* If we don't have size or date predicates, no work to do */
	if (!pw_ctx->plan || !pw_ctx->plan->need_path)
		goto out;
	
	get_dir_parent(lin, cc_ctx->snap1, &p1lin, false, &error);
	if (error)
		goto out;
	found = get_dir_parent(lin, cc_ctx->snap2, &p2lin, true, &error);
	if (error)
		goto out;
	/* 
	 * Check whether the parent's are same and the names (when converted
	 * to utf8) are the same.  If so, we didn't move and the rest of
	 * the function can be skipped.
	 */
	if (found && p1lin == p2lin) {
		/* Find the "path" to the immediate parent (ie file name) */
		dir_get_utf8_str_path("", p1lin, lin, cc_ctx->snap1, 0,
		    &fname1, &error);
		if (error)
			goto out;
		dir_get_utf8_str_path("", p2lin, lin, cc_ctx->snap2, 0,
		    &fname2, &error);
		if (error)
			goto out;
		if (0 == strcmp(fname1, fname2))
			goto out;
	}

	/* Get the repstate entry */
	found = get_diff_entry(lin, &rep_entry, cc_ctx->rep1, 
	    cc_ctx->rep2, &in_diff, &error);
	if (error)
		goto out;

	if (!found) {
		error = isi_system_error_new(errno,
		    "expect repstate entry not found %llx", lin);
		goto out;
	}
	ASSERT(rep_entry.is_dir);

	/**
	 * Add a for_path repstate entry indicating we're here
	 * at least in part because of a possible path change.
	 */
	old_entry = rep_entry; 
	rep_entry.for_path = true;
	stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2, lin, &rep_entry,
	    &old_entry, !in_diff, false, &error);
	if (error)
		goto out;

	/**
	 * Add a worklist entry so that we'll recursively mark
	 * All descendant directories as for_path as well.
	 */
	wl_log_add_entry(pw_ctx, lin, &error);
	if (error)
		goto out;

 out:
	if (fname1)
		free(fname1);
	if (fname2)
		free(fname2);
	
	isi_error_handle(error, error_out);
}



/*
 * This function performs following checks
 * 1) Checks if the lin is part of snapshot
 * 2) If so, then checks if the lin is not part of exclusion set
 */
static void
check_if_in_repdir2(struct migr_pworker_ctx *pw_ctx, uint64_t lin,
    bool *inrep, struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	ifs_snapid_t snap = cc_ctx->snap2;
	struct sel_hash_entry sel_entry;
	struct isi_error *error = NULL;
	ifs_lin_t my_lin, parent_lin;
	bool exists = false;

	ASSERT(inrep != NULL);

	check_in_snap(lin, cc_ctx->snap2, inrep, true, &error);
	if (error)
		goto out;

	/*
	 * We could stop now if, the lin is out of snapshot.
	 * Or if there are no excludes, then we are sure that
	 * the lin is part of snapshot and replication directory.
	 */
	if (!*inrep || !pw_ctx->has_excludes)
		goto out;

	/* First check if this lin is used for child or includes*/
	exists = get_sel_hash_entry(lin, &sel_entry, cc_ctx->sel2,
	    &error);
	if (error)
		goto out;

	if (exists) {
		/*
		 * Bug 141584
		 * If the directory moved to an excluded path,
		 * then treat it like an unlinked directory.
		 */
		if (sel_entry.excludes && !sel_entry.for_child
		    && !sel_entry.includes) {
			get_dir_parent(lin, snap, &parent_lin, false,
			    &error);
			if (error)
				goto out;
			if (sel_entry.plin == parent_lin) {
				*inrep = false;
				goto out;
			}
		} else
			goto out;
	}

	my_lin = lin;
	exists = false;
	/* 
	 * Now check if lin belongs to excludes or includes
	 * Note that we are doing parent lin walk but this should be
	 * fast since we have already done painting check above which
	 * on average should have placed all parent lins in-memory.
	 */
	while(!exists) {
		/* We should never hit root lin */
		ASSERT(my_lin != 2);
		get_dir_parent(my_lin, snap, &parent_lin, false, &error);
		if (error)
			goto out;

		exists = get_sel_hash_entry(parent_lin, &sel_entry,
		    cc_ctx->sel2, &error);
		if (error)
			goto out;

		if (exists) {
			if (!sel_entry.includes) {
				*inrep = false;
				ASSERT(sel_entry.excludes ||
				    sel_entry.for_child);
			}
		}
		my_lin = parent_lin;
	}
out:
	isi_error_handle(error, error_out);

}

static void
add_base_entry_to_rep2(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin,
    struct rep_entry *rep, struct isi_error **error_out)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct isi_error *error = NULL;
	
	/* Copy entry from rep1 to rep2 */
	rep->new_dir_parent = false;
	rep->not_on_target = false;
	rep->changed = true;
	rep->lcount_set = false;
	rep->lcount_reset = false;
	rep->exception = false;
	rep->not_on_target =
	    !((rep->lcount 
	    - rep->excl_lcount) > 0);
	rep->hl_dir_lin = 0;
	rep->hl_dir_offset = 0;
	
	stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2,
	    lin, rep, NULL, true, true, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);

}

/*
 * If compliance v2, !restore, and the file is committed, then we may have
 * to update the lcount in the base repstate to prevent deletes from pushing
 * the incremental repstate's lcount <0. If these conditions are met and the
 * actual link count in the base snapshot is inconsistent with that of the
 * base repstate, then update the base repstate and flush to disk immediately.
 *
 * For files that do not meet the above criteria, do nothing and move on.
 */
static void
update_base_entry_for_compliance(struct migr_pworker_ctx *pw_ctx,
    ifs_lin_t lin, struct rep_entry *rep, struct isi_error **error_out)
{
	bool is_committed;
	int fd = -1;
	int ret;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct stat st;
	struct isi_error *error = NULL;

	if (pw_ctx->compliance_v2 && !pw_ctx->restore && !rep->is_dir) {
		is_committed = is_worm_committed(lin, cc_ctx->snap1, &error);
		if (error)
			goto out;

		if (is_committed) {
			fd = ifs_lin_open(lin, cc_ctx->snap1, O_RDONLY);
			if (fd == -1) {
				error = isi_system_error_new(errno,
				    "%s: Failed to open lin %llx", __func__,
				    lin);
				goto out;
			}

			ret = fstat(fd, &st);
			if (ret == -1) {
				error = isi_system_error_new(errno,
				    "%s: Failed to stat lin %llx", __func__,
				    lin);
				goto out;
			}

			if (rep->lcount != st.st_nlink) {
				rep->lcount = st.st_nlink;
				set_entry(lin, rep, cc_ctx->rep1, 0, NULL,
				    &error);
				if (error)
					goto out;
			}
		}
	}

out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
}

/*
 * 	Construct Summary Btree 
 * 
 * This function loads list of snapids between two synciq snapshots.
 * Then it goes through each stf and picks all lins
 * relevant to this Sync (lin belonging to base rep state)
 * This function breaks at two locations, 
 * 1) During end of stf lins
 * 2) if we have read LINS_IN_PASS for stf
 */
bool
process_stf_lins(struct migr_pworker_ctx *pw_ctx, bool force_set,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_lin_t lins[LIN_STF_CNT];
	int i;
	int found;
	bool exists;
	bool is_comp_store = false;
	bool done = false;
	struct rep_entry rep;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct summ_stf_ctx *summ_ctx = cc_ctx->summ_stf;
	struct work_restart_state *work = pw_ctx->work;

	/* We will read lins in the range which includes work->min_lin */
	set_stf_lin_range(summ_ctx, (work->min_lin ? work->min_lin - 1:
	    0), work->max_lin);

	found = stf_iter_next_lins(summ_ctx, cc_ctx->snap1,
	    cc_ctx->snap2, lins, LIN_STF_CNT, &error);
	if (error)
		goto out;
	if (found == 0) {
		done = true;
		work->lin = 0;
		goto out;
	}

	for (i = 0; i < found ; i++) {
		if (lins[i] >= work->max_lin) {
			done = true;
			work->lin = 0;
			goto out;
		}
		ASSERT(lins[i] >= work->min_lin);

		exists = get_entry(lins[i], &rep, cc_ctx->rep1, &error);
		if (error)
		    goto out;

		if (exists || force_set) {
			/*
			 * Skip all compliance store directories during
			 * compliance based restore operations.
			 */
			if (rep.is_dir && pw_ctx->restore &&
			    pw_ctx->compliance_v2) {
				is_comp_store =
				    is_compliance_store_lin(lins[i],
				    cc_ctx->snap1, &error);
				if (error)
					goto out;
				if (is_comp_store) {
					work->lin = lins[i];
					continue;
				}
			}

			pw_ctx->stf_cur_stats->change_compute->lins_total++;
			/*
			 * If its directory, then add it to summary stf.
			 * Else for files we directly update repstate.
			 * force_set is annoying variable used only for
			 * test code. In those cases we only update 
			 * summary stf.
			 */
			if (rep.is_dir || force_set) {
				pw_ctx->stf_cur_stats->change_compute->
				    dirs_changed++;
				set_stf_entry(lins[i], summ_ctx,
				    &error);
				if (error)
					goto out;
			} else {
				update_base_entry_for_compliance(pw_ctx,
				    lins[i], &rep, &error);
				if (error)
					goto out;

				pw_ctx->stf_cur_stats->change_compute->
				    files_changed++;
				add_base_entry_to_rep2(pw_ctx, lins[i],
				    &rep, &error);
				if (error)
					goto out;
			}
			/*
			 * Add parent entries for plan if this was
			 * a file and the first time we added this
			 * lin to the summary stf.
			 */
			if (!rep.is_dir) {
				update_parent_exclusion(pw_ctx,
				    lins[i], &error);
			} else {
				update_parent_path(pw_ctx, lins[i],
				    &error);
			}
		}
		work->lin = lins[i];
	}

out:
	isi_error_handle(error, error_out);
	return done;
}


/**
 * Free a changeset structure and any resources
 * it references.
 */
void
free_changeset(struct changeset_ctx *cc_ctx)
{
	if (cc_ctx) {
		if (cc_ctx->rep1)
			close_repstate(cc_ctx->rep1);
		if (cc_ctx->rep2)
			close_repstate(cc_ctx->rep2);
		if (cc_ctx->summ_stf) {
			summ_stf_free(cc_ctx->summ_stf);
			cc_ctx->summ_stf = NULL;
		}
		if (cc_ctx->wl_iter != NULL) {
			close_wl_iter(cc_ctx->wl_iter);
			cc_ctx->wl_iter = NULL;
		}
		if (cc_ctx->wl) {
			free_worklist(cc_ctx->wl);
			cc_ctx->wl = NULL;
		}
		if (cc_ctx->sel1) {
			close_repstate(cc_ctx->sel1);
			cc_ctx->sel1 = NULL;
		}
		if (cc_ctx->sel2) {
			close_repstate(cc_ctx->sel2);
			cc_ctx->sel2 = NULL;
		}
		if (cc_ctx->md_dir1) {
			migr_dir_free(cc_ctx->md_dir1);
			cc_ctx->md_dir1 = NULL;
		}
		if (cc_ctx->md_dir2) {
			migr_dir_free(cc_ctx->md_dir2);
			cc_ctx->md_dir2 = NULL;
		}
		if (cc_ctx->cpss1) {
			close_cpss(cc_ctx->cpss1);
			cc_ctx->cpss1 = NULL;
		}
		if (cc_ctx->cpss2) {
			close_cpss(cc_ctx->cpss2);
			cc_ctx->cpss2 = NULL;
		}
		if (cc_ctx->resync) {
			close_lin_list(cc_ctx->resync);
			cc_ctx->resync = NULL;
		}
		free(cc_ctx);
	}
}

struct changeset_ctx *
create_changeset(const char *policy, ifs_snapid_t snap1, ifs_snapid_t snap2,
    bool restore, struct isi_error **error_out)
{
	return create_changeset_resync(policy, snap1, snap2, restore, true,
	    false, error_out);
}

/**
 * Create a new changeset_ctx for the sync between snapshot 1 and
 * snapshot 2.  This opens the snap1 repstate, copies the data in it
 * to a newly-created repstate for snap2 (filtering out removed files),
 * and opens the snap1 stf.
 */
struct changeset_ctx *
create_changeset_resync(const char *policy, ifs_snapid_t snap1,
    ifs_snapid_t snap2, bool restore, bool src,
    bool compliance_v2, struct isi_error **error_out)
{
	char tmp_name1[MAXNAMLEN];
	char tmp_name2[MAXNAMLEN];
	char tmp_name3[MAXNAMLEN];
	char tmp_name4[MAXNAMLEN];
	char tmp_name5[MAXNAMLEN];
	char tmp_name6[MAXNAMLEN];
	char tmp_name7[MAXNAMLEN];
	char tmp_name8[MAXNAMLEN];
	char tmp_name9[MAXNAMLEN];
	struct changeset_ctx *cc_ctx = NULL;
	struct lin_list_info_entry llie;
	struct isi_error *error = NULL;

	log(TRACE, "create_changeset");
	log(DEBUG, "%s snap1 %llu snap2 %llu", policy, snap1, snap2);

	cc_ctx = malloc(sizeof(struct changeset_ctx));
	ASSERT(cc_ctx);
	memset(cc_ctx, 0, sizeof(struct changeset_ctx));
	cc_ctx->snap1 = snap1;
	cc_ctx->snap2 = snap2;
	cc_ctx->md_dir1 = NULL;
	cc_ctx->md_dir2 = NULL;
	cc_ctx->summ_stf = NULL;

	get_base_rep_name(tmp_name1, policy, restore);
	get_repeated_rep_name(tmp_name2, policy, snap2, restore);
	get_worklist_name(tmp_name3, policy, snap2, restore);
	get_summ_stf_name(tmp_name4, policy, snap2, restore);
	get_select_name(tmp_name5, policy, snap1, restore);
	get_select_name(tmp_name6, policy, snap2, restore);
	get_base_cpss_name(tmp_name7, policy, restore);
	get_repeated_cpss_name(tmp_name8, policy, snap2, restore);
	get_resync_name(tmp_name9, sizeof(tmp_name9), policy, src);

	open_repstate(policy, tmp_name1, &cc_ctx->rep1, &error);
	if (error)
		goto out;

	/* Rep2 btree */
	create_repstate(policy, tmp_name2, true, &error);
	if (error)
		goto out;

	open_repstate(policy, tmp_name2, &cc_ctx->rep2, &error);
	if (error)
		goto out;

	/* worklist btree */
	create_worklist(policy, tmp_name3, true, &error);
	if (error)
		goto out;
	
	open_worklist(policy, tmp_name3, &cc_ctx->wl, &error);
	if (error)
		goto out;

	/* Summary STF btree */
	create_summary_stf(policy, tmp_name4, true, &error);
	if (error)
		goto out;

	open_summary_stf(policy, tmp_name4, &cc_ctx->summ_stf, &error);
	if (error)
		goto out;

	/* select btree , for excludes/includes*/
	open_repstate(policy, tmp_name5, &cc_ctx->sel1, &error);
	if (error) {
		log(ERROR, "Error opening sel1");
		goto out;
	}

	create_repstate(policy, tmp_name6, true, &error);
	if (error) {
		log(ERROR, "Error creating seltree2");
		goto out;
	}
	open_repstate(policy, tmp_name6, &cc_ctx->sel2, &error);
	if (error) {
		log(ERROR, "Error opening sel2");
		goto out;
	}

	open_cpss(policy, tmp_name7, &cc_ctx->cpss1, &error);
	if (error)
		goto out;

	create_cpss(policy, tmp_name8, true, &error);
	if (error)
		goto out;

	open_cpss(policy, tmp_name8, &cc_ctx->cpss2, &error);
	if (error)
		goto out;

	/* Compliance resync list */
	if (compliance_v2) {
		create_lin_list(policy, tmp_name9, true, &error);
		if (error)
			goto out;

		open_lin_list(policy, tmp_name9, &cc_ctx->resync, &error);
		if (error)
			goto out;

		get_lin_list_info(&llie, cc_ctx->resync, &error);
		if (error)
			goto out;

		if (RESYNC != llie.type) {
			error = isi_system_error_new(EINVAL,
			    "Invalid lin list info type: %d", llie.type);
			log(ERROR, "Invalid lin list info type: %d", llie.type);
			goto out;
		}
	}
 out:
	if (error) {
		free_changeset(cc_ctx);
		cc_ctx = NULL;
	}

	isi_error_handle(error, error_out);

	ASSERT(error || cc_ctx);
	return cc_ctx;
}

/**
 * Creates a dummy repstate for snapid snap containing only LIN root_lin.
 * This is purely for testing/early development.
 */
void
create_dummy_begin(const char *policy, ifs_snapid_t snap, ifs_lin_t root_lin,
    struct isi_error **error_out)
{
	char tmp_name[MAXNAMLEN];
	char tmp_name1[MAXNAMLEN];
	struct isi_error *error = NULL;
	struct rep_entry root_entry = {};
	struct rep_ctx *repctx = NULL;
	
	root_entry.lcount = 1;
	root_entry.is_dir = 1;

	sprintf(tmp_name, "%s_snap_rep_base", policy);
	get_select_name(tmp_name1, policy, snap, false);
	create_repstate(policy, tmp_name, false, &error);
	if (error) {
		log(ERROR, "Error creating base repstate %s",
		    tmp_name);
		goto out;
	}

	open_repstate(policy, tmp_name, &repctx, &error);
	if (error)
		goto out;

	if (root_lin != 0) {
		set_entry(root_lin, &root_entry, repctx, 0, NULL, &error);
		if (error)
			goto out;
	}

	create_repstate(policy, tmp_name1, false, &error);
	if (error) {
		log(ERROR, "Error creating base select tree %s",
		    tmp_name1);
		goto out;
	}
	
 out:
	if (repctx)
		close_repstate(repctx);
	isi_error_handle(error, error_out);
}


/**
 * Check to see whether the parent of lin changed between snap1 and
 * snap2.  If it did, indicate whether the change was due to a changed
 * parent (*pchange) or removal (*removed).
 */
void
check_parent_change(struct changeset_ctx *cc_ctx,
    ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    bool *pchanged, bool *removed, struct isi_error **error_out)
{
	ifs_lin_t par1;
	ifs_lin_t par2;
	struct isi_error *error = NULL;
	bool found;

	get_dir_parent(lin, snap1, &par1, false, &error);
	if (error)
		goto out;

	found = get_dir_parent(lin, snap2, &par2, true, &error);
	if (error)
		goto out;
	if (!found) {
		*pchanged = 0;
		*removed = 1;
	} else {
		*pchanged = (par1 != par2);
		*removed = 0;
	}
 out:
	isi_error_handle(error, error_out);
}

/**
 * Open an fd pointing at the snapshot snap version of dir_lin.
 */
static int
open_snap_dir_fd(ifs_lin_t dir_lin, ifs_snapid_t snap,
    struct isi_error **error_out)
{
	struct ifs_lin_snapid ls = {.lin = dir_lin, .snapid = snap};
	struct isi_error *error = NULL;
	int fd = -1;
	bool found;

	/* Bug 190724 - check that the lin exists in the snapshot */
	check_in_snap(dir_lin, snap, &found, false, &error);
	if (error)
		goto out;

	fd = ifs_lin_open(dir_lin, snap, O_RDONLY);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Error opening dir lin %{}",
		    ifs_lin_snapid_fmt(&ls));
	}
out:
	isi_error_handle(error, error_out);
	return fd;
}

static void
set_incl_for_child_lin( struct changeset_ctx *cc_ctx, ifs_lin_t dir_lin,
    bool newdir, bool removedir, bool has_excludes,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct sel_hash_entry sel_entry;
	bool exists;

	cc_ctx->incl_for_child1 = 0;
	cc_ctx->incl_for_child2 = 0;

	if (!has_excludes)
		goto out;

	if (!newdir) {
		exists = get_sel_hash_entry(dir_lin, &sel_entry, cc_ctx->sel1,
		    &error);
		if (error)
			goto out;

		if (exists && sel_entry.for_child) {
			ASSERT(sel_entry.plin > 0);
			cc_ctx->incl_for_child1 = true;
		}
	}

	if (!removedir) {
		exists = get_sel_hash_entry(dir_lin, &sel_entry, cc_ctx->sel2,
		    &error);
		if (error)
			goto out;

		if (exists && sel_entry.for_child) {
			ASSERT(sel_entry.plin > 0);
			cc_ctx->incl_for_child2 = true;
		}
	}

out:
	isi_error_handle(error, error_out);
}

void
clear_migr_dir(struct migr_pworker_ctx *pw_ctx, ifs_lin_t dir_lin)
{
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;

	/*
	 * cc_ctx->dir_lin == 0 on first execution or on restart.
	 * In those cases we could directly use on disk work dirent
	 * checkpoints. If dir_lin != 0, then we were iterating 
	 * either in the same phase or starting new phase.
	 * In either case (including the case where we have only 
	 * one lin to iterate) we should clear the checkpoint.
	 * Note that we always clear dirent checkpoint when
	 * we transition between phases. Look for other places
	 * where we call clear_checkpoint().
	 */
	if (cc_ctx->dir_lin != 0 && dir_lin != 0 &&
	    cc_ctx->dir_lin != dir_lin) {
		clear_checkpoint(pw_ctx->work);
	}
	    
	cc_ctx->dir_lin = 0;
	if (cc_ctx->md_dir1) {
		migr_dir_free(cc_ctx->md_dir1);
		cc_ctx->md_dir1 = NULL;
	}
	if (cc_ctx->md_dir2) {
		migr_dir_free(cc_ctx->md_dir2);
		cc_ctx->md_dir2 = NULL;
	}
}

/**
 * create the needed migr_dir_new structures if we're on a new
 * dir_lin.
 */
void
setup_migr_dir(struct changeset_ctx *cc_ctx, ifs_lin_t dir_lin, bool newdir,
    uint64_t *resume_cookie2, bool removedir, uint64_t *resume_cookie1,
    bool force, bool has_excludes, bool need_stat, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	struct dir_slice slice1, slice2;

	if ((dir_lin != cc_ctx->dir_lin) || force) {
		if (cc_ctx->md_dir1) {
			migr_dir_free(cc_ctx->md_dir1);
			cc_ctx->md_dir1 = NULL;
		}
		if (cc_ctx->md_dir2) {
			migr_dir_free(cc_ctx->md_dir2);
			cc_ctx->md_dir2 = NULL;
		}

		slice1.begin = *resume_cookie1;
		slice2.begin = *resume_cookie2;
		slice1.end = slice2.end = DIR_SLICE_MAX;

		if (!newdir) {
			fd = open_snap_dir_fd(dir_lin, cc_ctx->snap1, &error);
			if (error)
				goto out;
			cc_ctx->md_dir1 = migr_dir_new(fd, &slice1, need_stat,
			    &error);
			if (error) {
				log(ERROR, "Error in migr_dir_new1");
				goto out;
			}
			ASSERT(cc_ctx->md_dir1);
			fd = -1;
		}

		if (!removedir) {
			fd = open_snap_dir_fd(dir_lin, cc_ctx->snap2, &error);
			if (error)
				goto out;
			ASSERT(fd >= 0);
			cc_ctx->md_dir2 = migr_dir_new(fd, &slice2, need_stat,
			    &error);
			if (error) {
				log(ERROR, "Error in migr_dir_new2");
				goto out;
			}
			ASSERT(cc_ctx->md_dir2);
			fd = -1;
		}
		cc_ctx->dir_lin = dir_lin;
		set_incl_for_child_lin(cc_ctx, dir_lin, newdir, removedir,
		    has_excludes, &error);
		if (error)
			goto out;
	}

  out:
	if (fd != -1) {
		ASSERT(error);
		close(fd);
	}
	isi_error_handle(error, error_out);
}

static inline
bool dirent_match(struct migr_dirent *de1, struct migr_dirent *de2)
{
	return ((de1->dirent->d_fileno == de2->dirent->d_fileno) &&
	    (0 == strcmp(de1->dirent->d_name, de2->dirent->d_name)) &&
	    (de1->dirent->d_encoding == de2->dirent->d_encoding));
}

static inline
bool sel_entry_match(struct sel_hash_entry *sel_entry, ifs_lin_t dir_lin,
    struct migr_dirent *de)
{
	return ((sel_entry->plin == dir_lin) && 
	    (0 == strcmp(sel_entry->name, de->dirent->d_name)));
	/* We dont compare encoding */
}

bool stat_lin_snap(ifs_lin_t lin, ifs_snapid_t snap, struct stat *stat,
    bool missing_ok, struct isi_error **error_out)
{
	struct ifs_lin_snapid ls = {.lin = lin, .snapid = snap};
	int fd = -1;
	int ret;
	bool found = false;
	struct isi_error *error = NULL;
	/* Bug 190724 - check that the lin exists in the snapshot */
	check_in_snap(lin, snap, &found, missing_ok, &error);
	if (error || !found)
		goto out;

	fd = ifs_lin_open(lin, snap, O_RDONLY);
	ASSERT(fd != 0);
	if (fd < 0 && (!missing_ok ||
	    (errno != ESTALE && errno != ENOENT))) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %{}", ifs_lin_snapid_fmt(&ls));
		goto out;
	}
	if (fd == -1) {
		/* We just found out that its missing in snap */
		goto out;
	}
	ret = fstat(fd, stat);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		 "Unable to fstat lin %{}", ifs_lin_snapid_fmt(&ls));
		goto out;
	}
	/* I think now we can be sure that its present in snap */
	if (stat->st_nlink > 0) {
		found = true;
	}

 out:
	if (fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
	return found;
}

/**
 * Execute plan on the lower-cookie dirent among dep1 and dep2 (or
 * both if they have equal cookies).  If the plan excludes the given
 * dirent, unref it, clear the appropriate pointer, and inform the
 * caller that we filtered an entry.
 *
 * Both of *dep1 and *dep2 cannot be passed in as NULL.
 */
static inline void
plan_filtered_dirent(struct changeset_ctx *cc_ctx,
    siq_plan_t *plan, time_t old_time, time_t new_time,
    char *path1, struct migr_dirent **dep1, bool *filt1,
    char *path2, struct migr_dirent **dep2, bool *filt2,
    struct isi_error **error_out)
{
	enum file_type ft;
	struct isi_error *error = NULL;
	bool check_dep1;
	bool check_dep2;
	struct stat st;
	struct stat *stp = NULL;
	bool need_stat;
	bool found;

	ASSERT(*dep1 || *dep2);

	need_stat = (plan && (plan->need_size || plan->need_date));

	/**
	 * Only bother running the plan on the lower cookie
	 * dirent since it's the only one the caller will be
	 * considering in a given pass.
	 */
	if ((!*dep2) || ((*dep1) && ((*dep1)->cookie < (*dep2)->cookie))) {
		check_dep1 = true;
		check_dep2 = false;
	} else if ((!*dep1) || ((*dep2) && ((*dep1)->cookie >
	    (*dep2)->cookie))) {
		check_dep1 = false;
		check_dep2 = true;
	} else {
		check_dep1 = true;
		check_dep2 = true;
	}

	
	/* Apply the policy to dep1 */
	if (check_dep1) {
		ft = dirent_type_to_file_type(
		    (*dep1)->dirent->d_type);

		if (ft == SIQ_FT_DIR && plan)
			need_stat = true;

		if (need_stat) {
			stp = &st;
			found = stat_lin_snap( (*dep1)->dirent->d_fileno,
			    (*dep1)->dirent->d_snapid, stp, false, &error);
			if (error)
				goto out;
			ASSERT(found == true);
		} else {
			stp = NULL;
		}

		*filt1 = !siq_select(plan, (*dep1)->dirent->d_name,
		    (*dep1)->dirent->d_encoding, stp, old_time, path1, ft,
		    &error);

		if (error)
			goto out;
	}

	/* Apply the policy to dep2 */
	if (check_dep2) {
		ft = dirent_type_to_file_type(
		    (*dep2)->dirent->d_type);

		if (ft == SIQ_FT_DIR && plan)
			need_stat = true;

		if (need_stat) {
			stp = &st;
			found = stat_lin_snap( (*dep2)->dirent->d_fileno,
			    (*dep2)->dirent->d_snapid, stp, false, &error);
			if (error)
				goto out;
			ASSERT(found == true);
		} else {
			stp = NULL;
		}


		*filt2 = !siq_select(plan, (*dep2)->dirent->d_name,
		    (*dep2)->dirent->d_encoding, stp, new_time, path2, ft,
		    &error);

		if (error)
			goto out;

	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * Read dirent based on whether directory is normal one or
 * belongs to exclusion/inclusion
 */
static struct migr_dirent *
migr_dir_read_specific(struct migr_dir *md_dir, struct rep_ctx *sel,
    bool has_excludes, bool for_child, struct isi_error **error_out)
{
	struct migr_dirent *de = NULL;
	bool found = false;
	ifs_lin_t dir_lin;
	struct isi_error *error = NULL;
	bool exists;
	struct sel_hash_entry sel_entry;

	ASSERT( md_dir != NULL && sel != NULL, "%p != NULL && %p != NULL",
	    md_dir, sel);
	dir_lin = md_dir->_stat.st_ino;
	/*
	 * Check if this directory needs to be filtered
	 */
	if (has_excludes) {
		/*
		 * We may have to filter entries
		 */
		do {
			de = migr_dir_read(md_dir, &error);
			if (error || !de)
				goto out;

			exists = get_sel_hash_entry(de->dirent->d_fileno,
			    &sel_entry, sel, &error);
			if (error)
				goto out;

			/*
			 * if this for-child, then we need to filter
			 * all except the ones which are for-child or includes
			 */			
			if (for_child) {
				if (!exists || (exists && sel_entry.excludes) ||
				    (exists && !sel_entry_match(&sel_entry,
				    dir_lin, de))) {
					migr_dir_unref(md_dir, de);
					de = NULL;
					continue;
				}
			} else {
				if (exists && sel_entry.excludes &&
				    sel_entry_match(&sel_entry, dir_lin, de)) {
					migr_dir_unref(md_dir, de);
					de = NULL;
					continue;
				} 
			}
			found = true;
		} while(!found);
	} else {
		/*
		 * Normal case without any exclusion rules
		 */
		de = migr_dir_read(md_dir, &error);
	}
out:
	isi_error_handle(error, error_out);
	return de;
}

/**
 * Find the next modified dirent in dir_lin, storing the current location in
 * error_out. The modified dirent may be either an added or a removed dirent,
 * with *isnew == true indicating an added dirent.  *md_out is returned purely
 * for ref counting purposes.
 *
 * newdir == true indicates that the directory is new, so we purely want an
 * iteration of all dirents.
 *
 * This is going to need an escape after a certain number of entries.
 */
struct migr_dirent *
find_modified_dirent(struct migr_pworker_ctx *pw_ctx, ifs_lin_t dir_lin, 
    bool newdir, uint64_t *resume_cookie2, bool removedir,
    uint64_t *resume_cookie1, enum dirent_mod_type *modtype,
    struct migr_dir **md_out, bool need_stat, struct isi_error **error_out)
{
	struct migr_dirent *de1 = NULL;
	struct migr_dirent *de2 = NULL;
	struct isi_error *error = NULL;
	struct migr_dirent *found_de = NULL;
	struct migr_dir *found_md = NULL;
	bool filtered = false;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	siq_plan_t *plan = pw_ctx->plan;
	bool has_excludes = pw_ctx->has_excludes;
	bool filt1 = false, filt2 = false;
	/*XXX: cache these */
	char *path1 = NULL;
	char *path2 = NULL;

	ASSERT(dir_lin != 0);

	if (!need_stat)
		need_stat = (plan && (plan->need_size || plan->need_date));

	/* Create migr_dir structures if needed. */
	setup_migr_dir(cc_ctx, dir_lin, newdir, resume_cookie2, 
	    removedir, resume_cookie1, false, has_excludes, need_stat,
	    &error);
	if (error)
		goto out;

	/* Find the next dirent in each version of the directory. */
	do {
		filtered = false;
		if (!newdir && !de1) {
			de1 = migr_dir_read_specific(cc_ctx->md_dir1,
			    cc_ctx->sel1, has_excludes, cc_ctx->incl_for_child1,
			    &error);
			if (error) {
				log(ERROR, "Error in migr_dir_read1");
				goto out;
			}
		}
		if (!removedir && !de2) {
			de2 = migr_dir_read_specific(cc_ctx->md_dir2,
			    cc_ctx->sel2, has_excludes, cc_ctx->incl_for_child2,
			    &error);
			if (error) {
				log(ERROR, "Error in migr_dir_read2");
				goto out;
			}
		}
			
		if (plan && (de1 || de2)) {
			if (plan->need_path) {
				if (de1 && !path1) {
					dir_get_utf8_str_path("/ifs/",
					    ROOT_LIN, dir_lin, cc_ctx->snap1,
					    0, &path1, &error);
					if (error)
						goto out;
				}
				if (de2 && !path2) {
					dir_get_utf8_str_path("/ifs/",
					    ROOT_LIN, dir_lin, cc_ctx->snap2,
					    0, &path2, &error);
					if (error)
						goto out;
				}
			}
			plan_filtered_dirent(cc_ctx, plan, pw_ctx->old_time,
			    pw_ctx->new_time, path1, &de1, &filt1, path2,
			    &de2, &filt2, &error);
			if (error)
				goto out;
		}

		/*
		 * Choose which cookie if any to return.
		 * Only interested in the lowest cookie each pass and only
		 * interested in dirents with the same cookie if the contents
		 * differ.  If the cookies are the same and do differ, return
		 * the de1 cookie this pass (next pass will return de2).
		 */
		if (de1 && de2) {
			if (de1->cookie < de2->cookie) {
				migr_dir_unread(cc_ctx->md_dir2);
				de2 = NULL;
			} else if (de1->cookie > de2->cookie) {
				migr_dir_unread(cc_ctx->md_dir1);
				de1 = NULL;
			} else if (!dirent_match(de1, de2) || filt1 != filt2) {
				migr_dir_unread(cc_ctx->md_dir2);
				de2 = NULL;
			} else {
				*resume_cookie1 = de1->cookie;
				*resume_cookie2 = de2->cookie;
				migr_dir_unref(cc_ctx->md_dir1, de1);
				migr_dir_unref(cc_ctx->md_dir2, de2);
				de1 = NULL;
				de2 = NULL;
				filtered = true;
				continue;
			}
		}
	} while (filtered);

	/* Record the non-NULL entry (if any) for return */
	if (de1) {
		ASSERT(!de2);
		found_de = de1;
		found_md = cc_ctx->md_dir1;
		*resume_cookie1 = de1->cookie;
		de1 = NULL;
		if (modtype) {
			if (!filt1)
				*modtype = DENT_REMOVED;
			else 
				*modtype = DENT_FILT_REMOVED;
		}
	} else if (de2) {
		ASSERT(!de1);
		found_de = de2;
		found_md = cc_ctx->md_dir2;
		*resume_cookie2 = de2->cookie;
		de2 = NULL;
		if (modtype) {
			if (!filt2)
				*modtype = DENT_ADDED;
			else
				*modtype = DENT_FILT_ADDED;
		}
	} else {
		found_de = NULL;
		found_md = NULL;
	}
 out:
	if (error) {
		if (de1)
			migr_dir_unref(cc_ctx->md_dir1, de1);
		if (de2)
			migr_dir_unref(cc_ctx->md_dir2, de2);
		de1 = NULL;
		de2 = NULL;
		found_de = NULL;
	} else {
		*md_out = found_md;
	}
	if (path1)
		free(path1);
	if (path2)
		free(path2);
	isi_error_handle(error, error_out);
	return found_de;
}

static void
process_added_dir_entry(struct migr_pworker_ctx *pw_ctx,
    struct migr_dirent *de, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry rep_entry;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	bool found;
	bool in_diff;
	
	/* Add new directory entry */
	found = get_diff_entry(de->dirent->d_fileno, &rep_entry,
	    cc_ctx->rep1, cc_ctx->rep2, &in_diff, &error);
	if (error)
		goto out;

	if (!found) {
		/* New entry */
		memset(&rep_entry, 0, sizeof(rep_entry));
		rep_entry.is_dir = true;
		rep_entry.lcount = 1;
		rep_entry.not_on_target = true;

		stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2,
		    de->dirent->d_fileno, &rep_entry, NULL, true, true,
		    &error);
		if (error)
			goto out;
		pw_ctx->stf_cur_stats->change_compute->lins_total++;
		pw_ctx->stf_cur_stats->change_compute->dirs_new++;
		pw_ctx->stf_cur_stats->dirs->created->src++;
	}
	/* If its new directory add it to worklist */
	if (rep_entry.not_on_target) {
		wl_log_add_entry(pw_ctx, de->dirent->d_fileno, &error);
		if (error)
			goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

/**
 * Process a newly-added file or dirent.
 */
static void
process_added_file_entry(struct migr_pworker_ctx *pw_ctx,
    struct migr_dirent *de, bool filtered, bool has_stat, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry rep_entry, old_rep = {};
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	ifs_lin_t wi_lin = pw_ctx->wi_lin;
	struct siq_stat *stats = pw_ctx->stf_cur_stats;
	bool found;
	bool lin_found;
	bool in_diff;
	bool success;
	bool new_file = false;
	struct stat st;
	
	do {
		found = get_diff_entry(de->dirent->d_fileno, &rep_entry,
		    cc_ctx->rep1, cc_ctx->rep2, &in_diff, &error);
		if (error)
			goto out;
		if (!has_stat) {
			lin_found = stat_lin_snap(de->dirent->d_fileno, 
			    cc_ctx->snap2, &st, false, &error);
			if (error)
			    goto out;
			ASSERT(lin_found == true);
		} else {
			st = *(de->stat);
		}
		if (!found) {
			/* New entry */
			memset(&rep_entry, 0, sizeof(rep_entry));
			rep_entry.is_dir = false;
			rep_entry.lcount = 1;
			if (filtered)
				rep_entry.excl_lcount = 1;
			rep_entry.not_on_target = true;
			if (st.st_nlink == 1) {
				/* No need to checkpoint this case */
				wi_lin = 0;
				work = NULL;
			}
			new_file = true;
		} else {
			/* Updated entry */
			old_rep = rep_entry;
			
			/*
			 * If S2 version of file has only one link,
			 * so set lcount 1
			 */
			if (st.st_nlink == 1) {
				rep_entry.lcount = 1;
				rep_entry.lcount_set = 1;
				if (filtered) {
					rep_entry.excl_lcount = 1;
				} else {
					rep_entry.excl_lcount = 0;
				}
				/* No need to checkpoint this case */
				wi_lin = 0;
				work = NULL;
			} else {
				if (filtered)
					rep_entry.excl_lcount++;
				rep_entry.lcount++;
			}
			stats->change_compute->files_linked++;
		}
		/* Write repstate file if needed */
		
		if (work) {
			/* Flush the log before checkpoint */
			flush_stf_logs(pw_ctx, &error);
			if (error)
				goto out;
			success = set_entry_cond(de->dirent->d_fileno,
			    &rep_entry, cc_ctx->rep2, !found || !in_diff,
			    &old_rep, wi_lin, work, NULL, 0, &error);
			if (error)
				goto out;
			if (cc_ctx->wl_iter) {
				wl_checkpoint(&pw_ctx->wl_log, cc_ctx->wl_iter,
				    work->dircookie1, work->dircookie2, &error);
				if (error)
					goto out;
			}
		} else {
			stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2,
			    de->dirent->d_fileno, &rep_entry, &old_rep,
			    !found || !in_diff, false, &error);
			if (error)
				goto out;
			success = true;
		}
	} while (!success);
	
	if (new_file) {
		stats->change_compute->lins_total++;
		stats->change_compute->files_new++;
	}

 out:
	isi_error_handle(error, error_out);
}

/**
 * Process a removed file dirent
 */
static void
process_removed_file_entry(struct migr_pworker_ctx *pw_ctx,
    struct migr_dirent *de, bool filtered, bool has_stat, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry rep_entry, old_entry, base_rep_entry;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	ifs_lin_t wi_lin = pw_ctx->wi_lin;
	bool found;
	bool in_diff;
	bool success;

	do {
		found = get_diff_entry(de->dirent->d_fileno, &rep_entry,
		    cc_ctx->rep1, cc_ctx->rep2, &in_diff, &error);
		if (error)
			goto out;
		if (!found)
			goto out;
		if (rep_entry.lcount_set || rep_entry.lcount_reset)
			goto out;

		found = get_entry(de->dirent->d_fileno, &base_rep_entry,
		    cc_ctx->rep1, &error);
		if (error)
			goto out;
		ASSERT(found == true);
		ASSERT(base_rep_entry.lcount > 0);

		old_entry = rep_entry;
		if (rep_entry.lcount <= 0) {
			error = isi_system_error_new(EINVAL,
			    "SyncIQ database lin %llx refcount error. ",
			    de->dirent->d_fileno);
			goto out;
		}
		rep_entry.lcount--;
		pw_ctx->stf_cur_stats->change_compute->files_unlinked++;
		/* Bug 166951 - do not decrement excl_lcount below zero */
		if (filtered && rep_entry.excl_lcount > 0)
			rep_entry.excl_lcount--;

		/* If S1 version of file has only one link */
		if (base_rep_entry.lcount == 1) {
			rep_entry.lcount_reset = 1;
			/* No need to checkpoint this one */
			wi_lin = 0;
			work = NULL;
		} 
		if (work) {
			/*
			 * Ondisk reference checkpoint stored for snap1
			 * version should be <= to current snap1 version
			 * being stored. This way we will catch Bug 96622.
			 */
			if ((rep_entry.hl_dir_lin == work->lin) &&
			    (rep_entry.hl_dir_offset != 0) &&
			    (rep_entry.hl_dir_offset >= work->dircookie1)) {
				log(FATAL,
				    "SyncIQ checkpoint error, worker is "
				    " overwriting the previous snap dirent"
				    " latest checkpoint %llx with new one %llx"
				    " for lin %llx", rep_entry.hl_dir_offset,
				    work->dircookie1, de->dirent->d_fileno);
				goto out;
			}
			/*
			 * Lets store the last snap1 dirent cookie for
			 * validation.
			 */
			rep_entry.hl_dir_lin = work->lin;
			rep_entry.hl_dir_offset = work->dircookie1;

			/* Flush the log before checkpoint */
			flush_stf_logs(pw_ctx, &error);
			if (error)
				goto out;
			UFAIL_POINT_CODE(log_hardlink_refcount,
				{
				    char log_cmd[256];
				    sprintf(log_cmd, "echo \"refcnt dec "
				    "time %lu work_item %llx dir_lin %llx "
				    "ent_lin %llx ent_cookie %llx\" >> "
				    "/var/crash/siq_log", time(0), wi_lin,
				    work->lin, de->dirent->d_fileno, 
				    work->dircookie1);
				    system(log_cmd);
				}
			);
			success = set_entry_cond(de->dirent->d_fileno,
			    &rep_entry, cc_ctx->rep2, !in_diff, &old_entry,
			    wi_lin, work, NULL, 0, &error);
			if (error)
				goto out;
			if (cc_ctx->wl_iter) {
				wl_checkpoint(&pw_ctx->wl_log, cc_ctx->wl_iter,
				    work->dircookie1, work->dircookie2, &error);
				if (error)
					goto out;
			}
		} else {
			stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2,
			    de->dirent->d_fileno, &rep_entry, &old_entry,
			    !in_diff, false, &error);
			if (error)
				goto out;
			success = true;
		}
	} while (!success);

 out:
	isi_error_handle(error, error_out);
}



/**
 * Process modifications of a single lin.  This is called for
 * all added files and modified or added directories EXCEPT directories that
 * were modified by being removed from the sync job.
 *
 * For modified files or directories (but not added files or directories) the
 * changed bool is set.
 *
 * For added dirents, the link count of the referenced lin is bumped (and the
 * repstate entry added if needed).  For removed files, the link count is
 * decremented.  Removed directories are skipped.
 */
void
process_single_lin_modify(struct migr_pworker_ctx *pw_ctx, 
    ifs_lin_t lin, bool newdir, int num_operations,
    struct isi_error **error_out)
{
	struct rep_entry rep_entry, old_entry;
	struct isi_error *error = NULL;
	struct migr_dirent *de = NULL;
	struct migr_dir *md_out = NULL;
	bool found;
	bool in_diff;
	enum dirent_mod_type modtype;
	int oper_count = 0;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	ASSERT(work != NULL);
	ASSERT(cc_ctx != NULL);

	log(TRACE, "%s: lin: %{}", __func__, lin_fmt(lin));

	check_for_compliance(pw_ctx, lin, &error);
	if (error) {				
		send_err_msg(pw_ctx, EPERM, 
		    isi_error_get_message(error));
		log(FATAL, isi_error_get_message(error));
		goto out;
	}

	if (!newdir) {
		found = get_diff_entry(lin, &rep_entry, cc_ctx->rep1,
		    cc_ctx->rep2, &in_diff, &error);
		if (error)
			goto out;

		if (!found)
			goto out;

		if (rep_entry.is_dir && rep_entry.lcount == 0) {
			UFAIL_POINT_CODE(pslm_chkpt_after_deleted_dir,
			    flush_stf_logs(pw_ctx, &error);
			    if (error != NULL)
				    goto out;
			    set_work_entry(pw_ctx->wi_lin,
				work, NULL, 0,
				pw_ctx->cur_rep_ctx, &error);
			    if (error != NULL)
				    goto out;
			    error = isi_system_error_new(EIO,
				"%s: fail at lin: %{}", __func__,
				lin_fmt(lin)););
			goto out;
		}

		old_entry = rep_entry;
		rep_entry.changed = true;
		stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2, lin,
		    &rep_entry, &old_entry, !in_diff, false, &error);
		if (error)
			goto out;

		if (!rep_entry.is_dir) {
			/* summ_stf should only contain directories */
			ASSERT_DEBUG(rep_entry.is_dir,
			    "unexpected non-directory lin: %{}",
			    lin_fmt(lin));
			goto out;
		}
	}

	clear_migr_dir(pw_ctx, lin);
	/* Needs a check here to skip iterating directories with no changed
	 * dirents. */
	while (true) {
		de = find_modified_dirent(pw_ctx, lin, newdir,
		    &work->dircookie2, false, &work->dircookie1,
		    &modtype, &md_out, newdir, &error);
		if (error) {
			goto out;
		}
		if (!de) {
			if (num_operations != -1)
				work->lin = 0;
			goto out;
		}
		switch(modtype) {
		case DENT_ADDED:
			if (de->dirent->d_type == DT_DIR) {
				process_added_dir_entry(pw_ctx, de, &error);
			} else {
				process_added_file_entry(pw_ctx, de, false,
				    newdir, &error);
			}
			if (error)
				goto out;
			break;
		case DENT_REMOVED:
			if (de->dirent->d_type != DT_DIR) {
				process_removed_file_entry(pw_ctx, de, false,
				    newdir, &error);
				if (error)
					goto out;
			}
			break;
		case DENT_FILT_ADDED:
			if (de->dirent->d_type != DT_DIR) {
				process_added_file_entry(pw_ctx, de, true,
				    newdir, &error);
				if (error)
					goto out;
			}
			break;
		case DENT_FILT_REMOVED:
			if (de->dirent->d_type != DT_DIR) {
				process_removed_file_entry(pw_ctx, de, true,
				    newdir, &error);
				if (error)
					goto out;
			}
			break;
		default:
			ASSERT(0);
			break;

		}
		migr_dir_unref(md_out, de);
		de = NULL;
		oper_count++;
		if (num_operations != -1 && oper_count > num_operations - 1) {
			break;
		}
	}
	/*
	 * Bug 215880
	 * XXX: We need to confirm that if there is a request to exit due to
	 * XXX: worker pool rebalancing that we checkpoint prior to exit.
	 */
 out:
	if (de) {
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	isi_error_handle(error, error_out);
}

/**
 * Process all of the file and dirent modifications (other than
 * directory removes) that have occurred between the two
 * snapshots listed in cc_ctx, updating rep_ctx as appropriate.
 *
 * This deals with file content modifications, files added to existing
 * directories, files removed from existing directories (that have not
 * themselves been removed), files added to new directories, and completely new
 * directories.  This does NOT deal with removed directories, moved (but
 * preexisting) directories, or files contained in removed directories.
 *
 * Each call processes some portion of the work, keeping state in cc_ctx
 * indicating where to restart.  This function should be called repeatedly
 * until it returns false indicating no more work or an error (which will
 * also set error_out);
 */
bool
process_dirent_and_file_mods(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	int i;
	int found;
	bool done = false;
	struct isi_error *error = NULL;
	ifs_lin_t lins[TOTAL_STF_LINS];
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	/* Create new summary stf iterator if there is none */
	if (cc_ctx->summ_iter == NULL) {
		/* 
		 * If there was work->lin, that means we had checkpoint and
		 * and the worker died.
		 */
		cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf,
		    (work->lin ? work->lin : work->min_lin));
	}

	found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins,
	    1, &error);
	if (error)
		goto out;

	if (!found) {
		done = true;
		goto out;
	}
 
	/**
	 * Run through the stf list of modified files, processing each
	 * directory.
	 */
	for (i = 0; i < found; i++) {
		if (lins[i] >= work->max_lin) {
			done = true;
			goto out;
		}
		ASSERT(lins[i] >= work->min_lin);

		/*
		 * Bug 215880
		 *
		 * work->lin, work->dircookie1, and work->dircookie2 are all
		 * written to disk whenever set_work_entry is called in
		 * WORK_CC_DIR_CHGS.
		 *
		 * Note: in process_single_lin_modify we're in WORK_CC_DIR_CHGS
		 * when newdir is false.
		 *
		 * Restarting a work item in phase WORK_CC_DIR_CHGS after
		 * set_work_entry has been called will read work->lin,
		 * work->dircookie1, and work->dircookie2 from disk into
		 * pw_ctx->work.
		 * If work->lin were a deleted LIN, work->dircookie1 and
		 * work->dircookie2 may have been passed through from the last
		 * prior non-deleted directory read in
		 * process_single_lin_modify. And on work restart in
		 * WORK_CC_DIR_CHGS if work->lin were set to a deleted
		 * directory, the on-disk work->dircookie1 and work->dircookie2
		 * would eventually be used to set the migr_dir context for the
		 * first non-deleted directory found after restart.
		 *
		 * This change will clear checkpoint cookies whenever we're
		 * starting a new lin.
		 *
		 * XXX: TODO:
		 * XXX: work->lin should probably not be used from disk.
		 * XXX: Currently chkpt_lin is ignored in WORK_CC_DIR_CHGS.
		 * XXX: There's no point of reference to confirm the validity
		 * XXX:   of the on-disk value of work->dircookie1 and
		 * XXX:   work->dircookie2 in WORK_CC_DIR_CHGS
		 */
		if (work->lin != lins[i]) {
			clear_checkpoint(work);
			work->lin = work->chkpt_lin = lins[i];
		}
		process_single_lin_modify(pw_ctx, lins[i], false, -1,
		    &error);
		if (error)
			goto out;
	}

 out:
	if (done) {
		close_summ_stf_iter(cc_ctx->summ_iter);
		cc_ctx->summ_iter = NULL;
	}

	isi_error_handle(error, error_out);
	return !done;
}

/**
 * Process a single lin in the directory remove path,
 * removing the directory if it was moved out of the
 * replication set or deleted.
 * If recurse_remove is set it means we are executing recursive directories
 * removal phase. 
 */
static void
process_single_lin_remove_dir(struct migr_pworker_ctx *pw_ctx,
    bool recurse_remove, uint64_t wlid, ifs_lin_t lin,
    struct isi_error **error_out)
{
	struct rep_entry rep_entry;
	struct rep_entry old_entry;
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	bool pchanged, removed;
	bool inrep;
	bool found;
	bool in_diff;

	found = get_diff_entry(lin, &rep_entry, cc_ctx->rep1,
	    cc_ctx->rep2, &in_diff, &error);
	if (error)
		goto out;
	if (!found || !rep_entry.is_dir || rep_entry.new_dir_parent)
		goto out;

	old_entry = rep_entry;
	rep_entry.changed = true;
	check_parent_change(cc_ctx, lin, cc_ctx->snap1, cc_ctx->snap2,
	    &pchanged, &removed, &error);
	if (error)
		goto out;
	if (recurse_remove && (pchanged || removed)) {
		/*
		 * We can skip this directory since it
		 * will be processed as part of STF lin
		 * processing. But we got to make sure 
		 * that lin is present in summary stf.
		 * It could happen that directory is moved
		 * out and its painted while we are running
		 * change computation.
		 */
		found = summ_stf_entry_exists(cc_ctx->summ_stf, lin, &error);
		if (error || found)
			goto out;
	}
	if (removed) {
		rep_entry.lcount = 0;
	} else if (pchanged || (pw_ctx->has_excludes && !recurse_remove)) {
		check_if_in_repdir2(pw_ctx, lin, &inrep, &error);
		if (error)
			goto out;
		if (!inrep) {
			rep_entry.lcount = 0;
		} else if (pchanged) {
			rep_entry.new_dir_parent = true;
			pw_ctx->stf_cur_stats->change_compute->dirs_moved++;
		} else
			goto out;
	} else if (recurse_remove) {
		rep_entry.lcount = 0;
	} else {
		/* We dont have any updates here */
		goto out;
	}

	if (rep_entry.lcount == 0) {
		pw_ctx->stf_cur_stats->change_compute->dirs_deleted++;
		wl_log_add_entry(pw_ctx, lin, &error);
		if (error)
			goto out;
	}

	stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2, lin,
	    &rep_entry, &old_entry, !in_diff, false, &error);
	if (error)
		goto out;

 out:
	isi_error_handle(error, error_out);
}

/**
 * Remove the contents of a removed directory.  This decrements all
 * file link counts and queues directories that weren't moved for
 * deletion on the worklist.
 */
static void
process_remove_dir_contents(struct migr_pworker_ctx *pw_ctx, 
    ifs_lin_t lin, struct isi_error **error_out)
{
	int i = 0;
	struct rep_entry rep_entry;
	struct isi_error *error = NULL;
	struct migr_dirent *de = NULL;
	struct migr_dir *md_out = NULL;
	enum dirent_mod_type modtype;
	bool found;
	bool in_diff;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	ASSERT(work != NULL);
	ASSERT(cc_ctx != NULL);

	clear_migr_dir(pw_ctx, lin);
	for (i = 0; i < CC_PROCESS_WL_SIZE; i++) {
		de = find_modified_dirent(pw_ctx, lin, false,
		    &work->dircookie2, true, &work->dircookie1,
		    &modtype, &md_out, false, &error);
		if (error)
			goto out;
		if (!de) {
			work->lin = 0;
			goto out;
		}
		found = get_diff_entry(lin, &rep_entry, cc_ctx->rep1,
		    cc_ctx->rep2, &in_diff, &error);
		if (error)
			goto out;

		if (!found) {
			migr_dir_unref(md_out, de);
			de = NULL;
			continue;
		}

		if (de->dirent->d_type == DT_DIR) {
			process_single_lin_remove_dir(pw_ctx, true,
			    pw_ctx->wi_lin, de->dirent->d_fileno, &error);
			if (error)
				goto out;
		} else { 
			if (modtype == DENT_FILT_REMOVED)
				process_removed_file_entry(pw_ctx, de, true,
				    false, &error);
			else 
				process_removed_file_entry(pw_ctx, de, false,
				    false, &error);
			if (error)
				goto out;
		}

		migr_dir_unref(md_out, de);
		de = NULL;
	}

 out:
	if (de)
		migr_dir_unref(md_out, de);
	isi_error_handle(error, error_out);
}

/**
 * Process a worklist entry for a directory in a changed path
 * worklist when using path predicates.  Scan all dirents in
 * the snap1 version of lin, marking any directories found as
 * for_ppath, add them to the stf, and add them to the worklist
 * for recursive descent.
 */
static void
process_ppath_contents(struct migr_pworker_ctx *pw_ctx, 
    ifs_lin_t lin, struct isi_error **error_out)
{
	int i = 0;
	struct rep_entry old_entry;
	struct rep_entry rep_entry;
	struct isi_error *error = NULL;
	struct migr_dirent *de = NULL;
	struct migr_dir *md_out = NULL;
	enum dirent_mod_type modtype;
	bool found;
	bool in_diff;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	ASSERT(work != NULL);
	ASSERT(cc_ctx != NULL);
	clear_migr_dir(pw_ctx, lin);
	//Process CC_PROCESS_WL_SIZE lins before returning
	for (i = 0; i < CC_PROCESS_WL_SIZE; i++) {
		de = find_modified_dirent(pw_ctx, lin, false,
		    &work->dircookie2, true, &work->dircookie1,
		    &modtype, &md_out, false, &error);
		if (error)
			goto out;
		if (!de) {
			//No more work to do
			work->lin = 0;
			goto out;
		}

		found = get_diff_entry(de->dirent->d_fileno, &old_entry,
		    cc_ctx->rep1, cc_ctx->rep2, &in_diff, &error);
		if (error)
			goto out;

		if (found && de->dirent->d_type == DT_DIR) {
			/* Add to the summary stf */
			set_stf_entry(de->dirent->d_fileno, cc_ctx->summ_stf,
			    &error);
			if (error)
				goto out;
			rep_entry = old_entry;
			/* Add to the differential repstate */
			rep_entry.for_path = true;
			stf_rep_log_add_entry(pw_ctx, cc_ctx->rep2,
			    de->dirent->d_fileno, &rep_entry, &old_entry,
			    !in_diff, false, &error);
			if (error)
				goto out;
			/* Add to the worklist log*/
			wl_log_add_entry(pw_ctx, de->dirent->d_fileno, &error);
			if (error)
				goto out;
		}

		migr_dir_unref(md_out, de);
		de = NULL;
	}

out:
	if (de) {
		migr_dir_unref(md_out, de);
	}
	isi_error_handle(error, error_out);
}

/**
 * Run through the STF, performing the remove pass on
 * directory entries.
 */
bool
process_removed_dirs(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	int i;
	int ret;
	int found;
	bool done = false;
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	ifs_lin_t lins[TOTAL_STF_LINS/2];
	struct ifs_lin_snapid_portal linsnaps[TOTAL_STF_LINS];

	ASSERT(cc_ctx != NULL);
	/* Create new summary stf iterator if there is none */
	if (cc_ctx->summ_iter == NULL) {
		/* 
		 * If there was work->lin, that means we had checkpoint and
		 * and the worker died.
		 */
		cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf,
		    (work->lin ? work->lin : work->min_lin));
	}

	found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins,
	    TOTAL_STF_LINS/2, &error);
	if (error)
		goto out;

	if (!found) {
		done = true;
		goto out;
	} else {
		/* Prefetch lins in snap1 version */
		for (i = 0; i < found; i++) {
				linsnaps[i].lin = lins[i];
				linsnaps[i].snapid = cc_ctx->snap1;
				linsnaps[i].portal_depth = 0;
		}
		ret = ifs_prefetch_lin(linsnaps, found, 0);
		if (ret) {
			error = isi_system_error_new(errno,
			    "Unable to prefetch lin range %llx - %llx",
			    linsnaps[0].lin, linsnaps[i - 1].lin);
			goto out;
		}

	}

	for (i = 0; i < found; i++) {
		if (lins[i] >= work->max_lin) {
			done = true;
			goto out;
		}
		ASSERT (lins[i] >= work->min_lin); 
		work->lin = lins[i];
		process_single_lin_remove_dir(pw_ctx, false, 
		    pw_ctx->wi_lin, lins[i], &error);
		if (error)
			goto out;
	}

 out:
	if (done) {
		close_summ_stf_iter(cc_ctx->summ_iter);
		cc_ctx->summ_iter = NULL;
	}

	isi_error_handle(error, error_out);
	return !done;
}


/**
 * Iterate a worklist of directory lin's that have
 * to be recursively processed if there is a path predicate
 * and an ancestor directory has been renamed.
 */
bool
process_ppath_worklist(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	bool exists = false;
	struct isi_error *error = NULL;
	ifs_lin_t wlist_curlin = 0;
	struct wl_entry *entry = NULL;
	struct work_restart_state *work = pw_ctx->work;

	//If we have a current entry, keep processing it, otherwise keep going
	if (work->lin == 0 || !pw_ctx->chg_ctx->wl_iter) {
		if (work->lin == 0 || (work->lin != 0 && pw_ctx->chg_ctx->wl_iter == 0)) {
			clear_checkpoint(work);
		}
		exists = wl_get_entry(pw_ctx, &wlist_curlin, &entry, &error);
		if (error || !exists)
			goto out;
		work->lin = wlist_curlin;
		if (entry) {
			work->dircookie1 = entry->dircookie1;
			work->dircookie2 = entry->dircookie2;
		}
	} else {
		exists = true;
		wlist_curlin = work->lin;
	}

	/* Process the new worklist lin */
	process_ppath_contents(pw_ctx, wlist_curlin, &error);
	if (error)
		goto out;

 out:
	if (entry)
		free(entry);
	isi_error_handle(error, error_out);
	return exists;
}


/**
 * Run through the worklist of recursively removed directories
 * processing entry (and adding additional new ones).
 */
bool
process_removed_dirs_worklist(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	bool exists = false;
	struct isi_error *error = NULL;
	ifs_lin_t wlist_curlin = 0;
	struct wl_entry *entry = NULL;
	struct work_restart_state *work = pw_ctx->work;

	if (work->lin == 0 || !pw_ctx->chg_ctx->wl_iter) {
		if (work->lin == 0 || (work->lin != 0 && pw_ctx->chg_ctx->wl_iter == 0)) {
			clear_checkpoint(work);
		}
		exists = wl_get_entry(pw_ctx, &wlist_curlin, &entry, &error);
		if (error || !exists)
			goto out;
		work->lin = wlist_curlin;
		if (entry) {
			work->dircookie1 = entry->dircookie1;
			work->dircookie2 = entry->dircookie2;
		}
	} else {
		exists = true;
		wlist_curlin = work->lin;
	}
	/* Process the new worklist lin */
	process_remove_dir_contents(pw_ctx, wlist_curlin, &error);
	if (error)
		goto out;

out:
	if (entry)
		free(entry);
	isi_error_handle(error, error_out);
	return exists;
}

/**
 * Run through the worklist of recursively added directories
 * processing entry (and adding additional new ones).
 */

static void
do_process_added_dirs_worklist_action_after_one_failpoint(
    struct migr_pworker_ctx *pw_ctx)
{
	struct isi_error *error = NULL;
	bool ufp_pause = true;
	while (ufp_pause) {
		ufp_pause = false;
		UFAIL_POINT_CODE(process_added_dirs_worklist_action_after_one,
			static bool padwleao = false;
			if (padwleao) {
				if (RETURN_VALUE == 0) {
					flush_stf_logs(pw_ctx, &error);
					if (error) {
						log(ERROR, "%s: failed to "
						    "flush_stf_logs: %s",
						    __func__,
						    isi_error_get_message(error));
						isi_error_free(error);
						error = NULL;
					}
					wl_checkpoint(&pw_ctx->wl_log,
					   pw_ctx->chg_ctx->wl_iter,
					   pw_ctx->work->dircookie1,
					   pw_ctx->work->dircookie2, &error);
					send_err_msg(pw_ctx, EIO, "FAILPOINT:"
					    "process_added_dirs_worklist_"
					    "action_after_one");
					log(FATAL,
					    "process_added_dirs_worklist_"
					    "action_after_one");
				} else {
					log(NOTICE, "process_added_dirs_"
					    "worklist_action_after_one pause");
					ufp_pause = true;
					siq_nanosleep(0, 1000000);
				}
			} else {
				padwleao = true;
			}
		);
	}
}

bool
process_added_dirs_worklist(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	bool exists = false;
	struct isi_error *error = NULL;
	ifs_lin_t wlist_curlin = 0;
	struct wl_entry *entry = NULL;
	struct work_restart_state *work = pw_ctx->work;
	int num_operations = CC_PROCESS_WL_SIZE;

	UFAIL_POINT_CODE(process_added_dirs_worklist_num_ops,
		num_operations = RETURN_VALUE;
	);

	/**
	 * Run through the worklist of recursively added files.
	 *
	 * work->lin == 0 when it is a new work item
	 * !wl_iter when work item is reloaded
	 */
	if (work->lin == 0 || !pw_ctx->chg_ctx->wl_iter) {
		if (work->lin == 0 || (work->lin != 0 && pw_ctx->chg_ctx->wl_iter == 0)) {
			// This is a new item or a restore, so clear dircookies
			clear_checkpoint(work);
		}
		exists = wl_get_entry(pw_ctx, &wlist_curlin, &entry, &error);
		if (error || !exists)
			goto out;
		work->lin = wlist_curlin;
		if (entry) {
			work->dircookie1 = entry->dircookie1;
			work->dircookie2 = entry->dircookie2;
		}
	} else {
		exists = true;
		wlist_curlin = work->lin;
	}

	/* Process the new worklist lin */
	process_single_lin_modify(pw_ctx, wlist_curlin, true,
	    num_operations, &error);
	if (error)
		goto out;
	//Failpoint to force a checkpoint and pause or exit
	do_process_added_dirs_worklist_action_after_one_failpoint(pw_ctx);

out:
	if (entry)
		free(entry);
	isi_error_handle(error, error_out);
	return exists;
}

/*
 * Get the entry from the worklist
 */
bool
wl_get_entry(struct migr_pworker_ctx *pw_ctx, uint64_t *lin,
    struct wl_entry **entry_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists = false;
	struct wl_entry *entry = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	log(TRACE, "%s", __func__);

	if (cc_ctx->wl_iter == NULL) {
		set_wl_key(&key, pw_ctx->wi_lin, work->wl_cur_level, work->lin);
		cc_ctx->wl_iter = new_wl_iter(cc_ctx->wl, &key);
	}

	/* Flush stf logs if there are things in the repstate because
	 * wl_iter_next could write the worklist to disk */
	if (pw_ctx->rep_log.index > 0) {
		flush_stf_logs(pw_ctx, &error);
		if (error)
			goto out;
	}

	/* Get a worklist entry from current level of worklist */
	exists = wl_iter_next(&pw_ctx->wl_log, cc_ctx->wl_iter, lin, NULL,
	    &entry, &error);
	if (error || exists)
		goto out;

	/* The current level lins are over so we move to next level */
	close_wl_iter(cc_ctx->wl_iter);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	/* Move work state to next level */
	work->wl_cur_level++;
	work->lin = 0;
	clear_checkpoint(work);
	set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0, cc_ctx->rep2,
	    &error);
	if (error)
		goto out;

	/* Now remove all the previous level lin entries */
	wl_remove_entries(cc_ctx->wl, pw_ctx->wi_lin,
	    work->wl_cur_level - 1, &error);
	if (error)
		goto out;

	set_wl_key(&key, pw_ctx->wi_lin, work->wl_cur_level, 0);
	cc_ctx->wl_iter = new_wl_iter(cc_ctx->wl, &key);

	exists = wl_iter_next(NULL, cc_ctx->wl_iter, lin, NULL, &entry, &error);

	if (error || exists)
		goto out;

	close_wl_iter(cc_ctx->wl_iter);
	cc_ctx->wl_iter = NULL;

out:
	if (entry) {
		if (entry_out) {
			*entry_out = entry;
		} else {
			free(entry);
		}
	}
	isi_error_handle(error, error_out);

	return exists;
}

void
wl_log_add_entry(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;

	wl_log_add_entry_1(&pw_ctx->wl_log, cc_ctx->wl,
	    pw_ctx->wi_lin, work->wl_cur_level, lin, &error);

	isi_error_handle(error, error_out);

}

/*
 * Add rep_entry to in-memory log. If the log is full, then we 
 * would flush the log to disk before adding new entry.
 */
void
stf_rep_log_add_entry(struct migr_pworker_ctx *pw_ctx, struct rep_ctx *ctx,
    ifs_lin_t lin, struct rep_entry *new, struct rep_entry *old, bool is_new,
    bool set_value, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry null_entry = {};
	struct rep_log *logp = &pw_ctx->rep_log;

	log(TRACE, "%s", __func__);

	ASSERT(new != NULL);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		/*
		 * Log is full, so we need flush it
		 * Before we flush repstate we need to flush worklist
		 * hence we call flush_stf_logs() which does both
		 * operations
		 */
		flush_stf_logs(pw_ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}

	logp->entries[logp->index].oper = is_new ? SBT_SYS_OP_ADD :
	    SBT_SYS_OP_MOD;
	logp->entries[logp->index].set_value = set_value;
	ASSERT(is_new || old);
	if (old) {	
		logp->entries[logp->index].old_entry = *old;
	} else {
		logp->entries[logp->index].old_entry = null_entry;
	}
	
	logp->entries[logp->index].lin = lin;
	logp->entries[logp->index].new_entry = *new;

	logp->index++;

out:
	isi_error_handle(error, error_out);
}


