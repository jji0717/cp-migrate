#include <string.h>
#include <sys/stat.h>

#include <sys/queue.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <isi_quota/isi_quota.h>
#include <isi_ufp/isi_ufp.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/siq_error.h"
#include "sworker.h"

static void
insert_quota_queue(struct quota_queue_head *head,
    const struct quota_domain_id *new_qdi)
{
	struct qdi_wrapper *new_entry = NULL;
	new_entry = malloc(sizeof(struct qdi_wrapper));
	ASSERT(new_entry != NULL, "failure to allocate memory: %s", __func__);
	memset(new_entry, 0, sizeof(struct qdi_wrapper));
	memcpy(&new_entry->qdi, new_qdi, sizeof(struct quota_domain_id));
	STAILQ_INSERT_HEAD(head, new_entry, next);
}

static bool
find_match_qdi(struct quota_queue_head *to_search,
    const struct quota_domain_id *to_find)
{
	bool found_match = false;
	struct qdi_wrapper *entry, *tmp;
	STAILQ_FOREACH_SAFE(entry, to_search, next, tmp) {
		if (qdi_equal(to_find, &entry->qdi)) {
			found_match = true;
			STAILQ_REMOVE(to_search, entry, qdi_wrapper, next);
			free(entry);
			break;
		}
	}
	return found_match;
}

/* If any of the quotas encountered is new and not ready, fail.
 * The sync needs to be re-run after the QuotaScan job has
 * finished. In the non-ready state we cannot move directories
 * within the quota. */
bool
check_quota_ready(const struct quota_domain *domain,
    struct isi_error **error_out)
{
	bool ready = false;
	char *quota_path = NULL;
	struct isi_error *error = NULL;

	if (domain)
		ready = qc_get_ready(&domain->_qdr.qdr_config);

	if (domain && !ready) {
		quota_path = quota_domain_get_path(domain, NULL);
		error = isi_system_error_new(EINVAL,
		    "A new quota domain that has not finished QuotaScan has "
		    "been found.  Please wait for QuotaScan to finish before "
		    "restarting the sync job.  Quota path: %s",
		    quota_path);
		goto out;
	}

out:
	if (quota_path)
		free(quota_path);
	isi_error_handle(error, error_out);
	return ready;
}

static void
check_quota_delete_helper(ifs_lin_t lin, enum quota_lin_query quota_query,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct qdi_wrapper *entry = NULL, *tmp = NULL;
	struct quota_queue_head *quotas = NULL;
	char *path = NULL;

	quotas = get_quotas_for_lin(lin, quota_query, &error);
	if (error)
		goto out;
	if (quotas == NULL)
		goto out;
	if (!STAILQ_EMPTY(quotas)) {
		path = get_valid_rep_path(lin, HEAD_SNAPID);
		error = isi_system_error_new(EINVAL,
			"Job failed because the job attempted to delete a directory "
			"that a quota has been applied to. You must delete all quotas "
			"applied on or under %s before this job can continue.",
			path);
		free(path);
		goto out;
	}
	STAILQ_FOREACH_SAFE(entry, quotas, next, tmp) {
		STAILQ_REMOVE_HEAD(quotas, next);
		free(entry);
	}

out:
	if (quotas != NULL)
		free(quotas);

	isi_error_handle(error, error_out);
}
/*
 * Checks if there is a quota at or below the lin we want to delete.
 * If so, an error is returned to indicate the delete should not continue.
 */
void
check_quota_delete(ifs_lin_t lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	check_quota_delete_helper(lin, QUOTA_LIN_QUERY_RECURSIVE, &error);
	if (error)
		goto out;
	check_quota_delete_helper(lin, QUOTA_LIN_QUERY_EXACT, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

/* Returns a STAILQ structure of quota_domain_id's that apply to the given
 * lin. If no quotas apply, then an empty set is returned.
 *
 *	QUOTA_LIN_QUERY_RECURSIVE
 *		will fetch all quotas below the given lin
 *	QUOTA_LIN_QUERY_RECURSIVE_UP
 *		will fetch all quotas above or at the given lin
 *	QUOTA_LIN_QUERY_EXACT
 *		will fetch only quotas that are defined on the given lin
 *
 * BEJ: NOTE: XXX: perf in here is not great for 1000's quotas
 */
struct quota_queue_head *
get_quotas_for_lin(ifs_lin_t lin, enum quota_lin_query quota_query,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool exists = true;
	struct quota_domain domain = QD_INITIALIZER;
	struct quota_domain_criteria criteria = {};
	struct quota_domain_iter iter;
	struct quota_domain_filter *filter = NULL;
	const struct quota_domain_id *qdi = NULL;
	struct quota_queue_head *new_set = NULL;

	log(TRACE, "%s(%llx,...)", __func__, lin);

	new_set = malloc(sizeof(struct quota_queue_head));
	ASSERT(new_set != 0, "failure to allocate memory: %s", __func__);
	STAILQ_INIT(new_set);

	/* setup quota iterator */
	quota_domain_criteria_init(&criteria, 0, 1, 0);/*type=0,lin=1,snap=0*/
	criteria.lins[0] = lin;
	criteria.lin_types[0] = quota_query;
	filter = quota_domain_criteria_filter_new(&criteria);
	quota_domain_qdb_iter_init(&iter, filter);

	while (exists) {
		/* iterate */
		exists = quota_domain_iter_next(&iter, &domain, &error);
		if (error)
			goto out;

		if (exists) {
			/* sanity check the quota is setup */
			check_quota_ready(&domain, &error);
			if (error)
				goto out;

			qdi = quota_domain_get_id(&domain);
			insert_quota_queue(new_set, qdi);
		}
	}

out:
	quota_domain_criteria_clean(&criteria);
	quota_domain_iter_clean(&iter);
	quota_domain_clean(&domain);

	isi_error_handle(error, error_out);

	return new_set;
}

/*
 * Compares the quota domains for lins a and b.  If the quota domains do
 * not match exactly then return true, otherwise return false.
 */
bool
crossed_quota(ifs_lin_t a_lin, ifs_lin_t b_lin, struct isi_error **error_out)
{
	bool has_crossed_quota = false;
	struct isi_error *error = NULL;

	struct quota_queue_head *unmatched_a = NULL;
	struct quota_queue_head *unmatched_b = NULL;
	struct quota_queue_head unmatched_diff =
	    STAILQ_HEAD_INITIALIZER(unmatched_diff);

	struct qdi_wrapper *entry, *tmp;

	log(TRACE, "crossed_quota(%llx, %llx)", a_lin, b_lin);

	unmatched_a = get_quotas_for_lin(a_lin, QUOTA_LIN_QUERY_RECURSIVE_UP,
	    &error);
	if (error)
		goto out;
	unmatched_b = get_quotas_for_lin(b_lin, QUOTA_LIN_QUERY_RECURSIVE_UP,
	    &error);
	if (error)
		goto out;

	/* perform a set difference of the domains.
	 * if the difference is non-zero, then the quotas were crossed */
	STAILQ_FOREACH_SAFE(entry, unmatched_a, next, tmp) {
		if (!find_match_qdi(unmatched_b, &entry->qdi))
			insert_quota_queue(&unmatched_diff, &entry->qdi);
		STAILQ_REMOVE_HEAD(unmatched_a, next);
		free(entry);
	}
	STAILQ_FOREACH_SAFE(entry, unmatched_b, next, tmp) {
		if (!find_match_qdi(&unmatched_diff, &entry->qdi))
			insert_quota_queue(&unmatched_diff, &entry->qdi);
		STAILQ_REMOVE_HEAD(unmatched_b, next);
		free(entry);
	}
	while (!STAILQ_EMPTY(&unmatched_diff)) {
		has_crossed_quota = true;
		entry = STAILQ_FIRST(&unmatched_diff);
		STAILQ_REMOVE_HEAD(&unmatched_diff, next);
		free(entry);
	}

out:
	if (error)
		has_crossed_quota = false;

	if (unmatched_a != NULL)
		free(unmatched_a);
	if (unmatched_b != NULL)
		free(unmatched_b);

	isi_error_handle(error, error_out);

	return has_crossed_quota;
}

/*
 * Opens a temporary directory with a name based on the lin of another
 * directory.  This will create a temporary directory if it does not
 * exist, otherwise it will open the existing directory.
 */
static void
treemv_get_tmp_dir(int parent_fd, ifs_lin_t flin, int *tmp_fd,
    struct stat *tmp_st, struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt);

	if (tmp_st == NULL || tmp_fd == NULL) {
		error = isi_system_error_new(EINVAL, "invalid tmp pointers.");
		goto out;
	}

	fmt_print(&fmt, "%s%llx", TW_MV_TMPNAME, flin);
	ret = enc_fstatat(parent_fd, fmt_string(&fmt), ENC_DEFAULT, tmp_st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to stat tmp dir: %s", fmt_string(&fmt));
		goto out;
	} else if (errno == ENOENT) {
		ret = enc_mkdirat(parent_fd, fmt_string(&fmt),
		    ENC_DEFAULT, 0755);
		if (ret < 0 && errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "Unable to create directory %s", fmt_string(&fmt));
			goto out;
		}
		ret = enc_fstatat(parent_fd, fmt_string(&fmt), ENC_DEFAULT,
		    tmp_st, AT_SYMLINK_NOFOLLOW);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "stat of new tmp dir %s failed", fmt_string(&fmt));
			goto out;
		}
	}
	*tmp_fd = ifs_lin_open(tmp_st->st_ino, HEAD_SNAPID, O_RDWR);
	if (*tmp_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open new tmp dir %s", fmt_string(&fmt));
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
clear_enoent(struct isi_error **error)
{
	if (*error && isi_system_error_is_a(*error, ENOENT)) {
		log(ERROR, "%s", isi_error_get_message(*error));
		isi_error_free(*error);
		*error = NULL;
	}
}

/*
 * Takes all of the dirents under from_fd and moves them to tmp_fd.
 * Both file descriptors are assumed to be open.
 */
static bool
treemv_dirents_to_tmp(struct migr_sworker_ctx* sw_ctx, int from_fd, int tmp_fd,
    ifs_lin_t tmp_lin, ifs_lin_t parent_lin, struct isi_error **error_out)
{
	int i, ret = -1;
	struct dirent *entryp = NULL, target_dirent = {};
	unsigned int st_count = READ_DIRENTS_NUM;
	uint64_t cookie_out[READ_DIRENTS_NUM],
	    resume_cookie = READ_DIRENTS_MIN_COOKIE;
	char dirents[READ_DIRENTS_SIZE];
	struct isi_error *error = NULL;
	char *path = NULL, *target_fname = NULL;
	char *secondary_path = NULL;
	bool dirent_found = false, found = false;
	ifs_lin_t target_lin, target_plin;
	struct stat tmp_st = {}, target_pst = {};

	do {
		ret = readdirplus(from_fd, RDP_NOTRANS, &resume_cookie,
		    READ_DIRENTS_SIZE, dirents,
		    READ_DIRENTS_SIZE, &st_count, NULL, cookie_out);

		if (ret == 0) /* 0, means we are at the end */
			break;

		if (ret < 0) {
			error = isi_system_error_new(errno,
			    "readdirplus failure");
			break;
		}

		entryp = (struct dirent *) dirents;
		for (i = 0; i < st_count; i++,
			entryp = (struct dirent*) ((char*) entryp +
			entryp->d_reclen))
		{
			if (entryp->d_fileno == 0)
				continue;

			dirent_found = true;

			/* Potentially there can be a restart of another
			   tree move in here, so check. Bug 138500 */
			if (0 == strncmp(entryp->d_name, TW_MV_TOPTMPNAME,
			    strlen(TW_MV_TOPTMPNAME))) {
				path = get_valid_rep_path(entryp->d_fileno,
					HEAD_SNAPID);
				secondary_path = get_valid_rep_path(tmp_lin,
					HEAD_SNAPID);
				log(INFO, "Additional in-progress tree move "
				    "detected, continuing tree move of %s "
				    "to %s.", path, secondary_path);
				free(path);
				free(secondary_path);
				/* We need to get the parent lin of the target
				 * from from_fd
				 */
				ret = fstat(from_fd, &target_pst);
				if (ret < 0) {
				    ON_SYSERR_GOTO(out, errno, error,
					    "%s: fstat failed for from_fd",
					    __func__);
				}
				treemv_restart(sw_ctx, target_pst.st_ino,
				    entryp->d_fileno,
				    tmp_lin,
				    entryp->d_encoding, entryp->d_name,
				    entryp->d_name, true, &error);
				/* Bug 206186 - ignore ENOENT */
				clear_enoent(&error);
				if (error)
					goto out;
				continue;
			} else if (0 == strncmp(entryp->d_name, TW_MV_TMPNAME,
                                        strlen(TW_MV_TMPNAME)) &&
                                1 == sscanf(entryp->d_name, TW_MV_TMPNAME "%llx",
                                        &target_lin)) {
                                /* This might be part of treemv_restart which
                                 * has already been finished. Check if the file
                                 * exists. If not, continue
                                 */
                                ret = enc_fstatat(from_fd, entryp->d_name,
                                                entryp->d_encoding, &tmp_st,
                                                AT_SYMLINK_NOFOLLOW);
                                if (ret != 0 && errno == ENOENT) {
                                        continue;
                                } else if (ret != 0) {
                                        error = isi_system_error_new(errno,
                                            "Stat of dirent %s failed",
                                            entryp->d_name);
                                        goto out;
                                }

                                /* Find the parent dir of the target dir */
                                found = dirent_lookup_by_lin(HEAD_SNAPID,
                                    target_lin, 0, &target_plin, &target_dirent,
                                    &error);
                                if (error)
                                        goto out;
                                else if (!found) {
                                        error = isi_system_error_new(ENOENT,
                                            "Failed to lookup lin %llx from previous "
                                            "run of treemv.", target_lin);
                                        goto out;
                                }

                                /* Find the file name of target */
                                dir_get_utf8_str_path("", target_plin, target_lin,
				    HEAD_SNAPID, 0, &target_fname, &error);
                                if (error) {
					if (target_fname) {
						free(target_fname);
						target_fname = NULL;
					}
					goto out;
				}
                                if (0 == strncmp(target_fname, TW_MV_TOPTMPNAME,
                                    strlen(TW_MV_TOPTMPNAME))) {
					if (target_plin == parent_lin) {
						/* This will be taken care by the
						 * treemv_restart from the target in
						 * same directory
						 */
						free(target_fname);
						target_fname = NULL;
						continue;
					}
					treemv_restart(sw_ctx, target_plin,
						    target_lin,
						    target_plin,
						    ENC_DEFAULT, target_fname,
						    target_fname, true, &error);
					free(target_fname);
					target_fname = NULL;
					 /* Bug 206186 - ignore ENOENT */
					 clear_enoent(&error);
					 if (error)
						  goto out;
					continue;
				}
				/* else handle this tmp dir below */
				free(target_fname);
				target_fname = NULL;
			}

			siq_enc_renameat(from_fd, entryp->d_name,
			    entryp->d_encoding, tmp_fd,
			    entryp->d_name, entryp->d_encoding, &error);
			if (error != NULL) {
				if (isi_system_error_is_a(error, EPERM)) {
					/* How lucky, you found another quota
					   in the dir you are trying to move,
					   time to call treemv to get the
					   entries out of this dir.
					   bug 138500 */
					isi_error_free(error);
					error = NULL;
					path = get_valid_rep_path(entryp->d_fileno,
						HEAD_SNAPID);
					secondary_path = get_valid_rep_path(tmp_lin,
						HEAD_SNAPID);
					log(INFO, "Directory move in quota "
					    "detected, using additional tree "
					    "move to move %s to %s.",
					    path, secondary_path);
					free(path);
					free(secondary_path);
					treemv(sw_ctx, parent_lin, from_fd,
					    entryp->d_fileno, tmp_fd,
					    entryp->d_encoding,
					    entryp->d_name,
					    entryp->d_name, entryp, true,
					    &error);
					/* Bug 206186 - continue on ENOENT */
					clear_enoent(&error);
					if (error)
						goto out;
				/* Bug 206186 - ignore ENOENT */
				} else {
					clear_enoent(&error);
					if (error != NULL)
						goto out;
				}
			}
		}
		resume_cookie = cookie_out[st_count - 1];
	} while (1);

out:
	isi_error_handle(error, error_out);

	return dirent_found;
}

/*
 * Continues to attempt to rename at directory across a quota. If the rename
 * fails with EPERM but we detect that there were no dirents present, then we
 * should exit with failure.
 *
 * This retry loop is to cover the case where SyncIQ is attempting to move
 * a directory into a quota but another worker keeps linking stuff into the
 * directory that is trying to be moved.
 */
static void
treemv_dirents_rename(struct migr_sworker_ctx* sw_ctx, int from_fd, int tmp_fd,
    ifs_lin_t tmplin, ifs_lin_t parentlin, int oldparent_fd, const char *name,
    enum enc enc, int new_fd, char *error_str, struct isi_error **error_out)
{
	bool dirents_found = true;
	int ret = -1;
	struct stat st;
	struct isi_error *error = NULL;

	treemv_dirents_to_tmp(sw_ctx, from_fd, tmp_fd, tmplin, parentlin, &error);
	if (error)
		goto out;

	while (ret < 0) {
		/* mv this dir to the desired location */
		ret = enc_renameat(oldparent_fd, name, enc, new_fd,
		    name, enc);
		if (ret < 0) {
			if (errno == ENOENT) {
				ret = enc_fstatat(oldparent_fd, name, enc, &st,
				    AT_SYMLINK_NOFOLLOW);
				if (ret != 0 && errno != ENOENT) {
					error = isi_system_error_new(errno,
					    "Stat of dirent %s failed after a "
					    "failed rename", name);
				}

				/* On regular ENOENT we should continue as if
				 * we are done.*/
				goto out;
			} else if (errno != EPERM || !dirents_found) {
				error = isi_system_error_new(errno,
				    "Failed to move %s %s", name,
				    error_str);
				goto out;
			}

			log(DEBUG, "Failed to mv dir %s, retrying", name);
		}

		dirents_found = treemv_dirents_to_tmp(sw_ctx, from_fd, tmp_fd, tmplin,
		    parentlin, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Moves the dirent md into the directory to_fd. There are a couple scenarios
 * of what happens depending on what md is. Cases outlined below:
 *
 *      If md is a directory:
 *	      The dirents of md will be moved into a new tmp dir
 *	      and the tmp dir will be pushed onto the treewalk stack
 *	      to process next.
 *      If md is a file:
 *	      It is simply moved
 *      If md is a .dlin-tmp directory: (only happens on restart runs)
 *	      Check the target of the tmp directory.
 *	      If target is not moved:
 *		      skip this .dlin-tmp
 *	      else target is already moved:
 *		      push this .dlin-tmp on treewalk stack
 *
 * Returns true if a dir was pushed onto the stack.
 */
static bool
treemv_mv_single_dirent(struct migr_sworker_ctx* sw_ctx, struct migr_dirent *md,
    struct migr_dir *current_dir, int to_fd, struct uint64_uint64_map *lin_hash,
    struct migr_tw *tw, struct isi_error **error_out)
{
	int ret = -1, md_fd = -1;
	struct isi_error *error = NULL;
	int from_fd = current_dir->_fd;
	int tmp_fd = -1;
	struct stat tmp_st = {};
	bool pushed = false;
	struct dirent hack_dirent = {};
	struct migr_dirent hack_md = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	ifs_lin_t target_lin;
	ifs_lin_t target_plin;
	bool found = false;

	if (md->dirent->d_type == DT_DIR) {
		/* Check if md is a tmp dir from a restarted run */
		if (0 == strncmp(md->dirent->d_name, TW_MV_TMPNAME,
		    strlen(TW_MV_TMPNAME)) &&
		    1 == sscanf(md->dirent->d_name, TW_MV_TMPNAME "%llx",
		    &target_lin)) {
			/* md is a tmp dir from a restarted treemv run. */

			/* This md tmp dir may have been deleted already,
			 * double check it exists. This scenario happens
			 * if we processed the target of this tmp dir
			 * before encountering the restarted tmp dir itself. */
			ret = enc_fstatat(from_fd, md->dirent->d_name,
				md->dirent->d_encoding, &tmp_st,
				AT_SYMLINK_NOFOLLOW);
			if (ret != 0 && errno == ENOENT) {
				/* already deleted, dont worry about it */
				goto out;
			} else if (ret != 0) {
				error = isi_system_error_new(errno,
				    "Stat of dirent %s(%llx) failed",
				    md->dirent->d_name, md->stat->st_ino);
				goto out;
			}

			/* Find the parent dir of the dir this tmp represents */
			found = dirent_lookup_by_lin(HEAD_SNAPID,
			    target_lin, 0, &target_plin, &hack_dirent,
			    &error);
			if (error)
				goto out;
			else if (!found) {
				error = isi_system_error_new(ENOENT,
				    "Failed to lookup lin %llx from previous "
				    "run of treemv.", target_lin);
				goto out;
			}

			/* compare the parent of the target dir against the
			 * current dir we are processing */
			if (current_dir->_stat.st_ino != target_plin) {
				/* The target dir was fully moved, and we
				 * need to push into the md dir we found to
				 * find other dirents that need to be moved. */
				uint64_uint64_map_add(lin_hash,
				    md->stat->st_ino, &target_lin);
				migr_tw_push(tw, md, &error);
				migr_dir_unref(current_dir, md);
				pushed = true;
				goto out;
			} else {
				/* The target is in the same dir, meaning it
				 * was not fully moved and we
				 * will complete processing of this md
				 * when we work on the target dir.
				 * Skip this dir. */
				goto out;
			}

		} else {
			/* md is indeed a dir that we need to move. */

			/* mk md's tmp folder */
			treemv_get_tmp_dir(from_fd, md->stat->st_ino,
				&tmp_fd, &tmp_st, &error);
			UFAIL_POINT_CODE(fail_quotamv_inprogress,
			    error = isi_system_error_new(EIO,
			    "ufp, treemv_mv_single_dirent fail"););
			if (error)
				goto out;

			/* mv md's children to the tmp folder */
			md_fd = ifs_lin_open(md->stat->st_ino, HEAD_SNAPID,
			    O_RDWR);
			if (md_fd == -1) {
				error = isi_system_error_new(errno,
				    "Failed to open md %llx",
				    md->stat->st_ino);
				goto out;
			}

			treemv_dirents_rename(sw_ctx, md_fd, tmp_fd, tmp_st.st_ino,
			    current_dir->_stat.st_ino, from_fd,
			    md->dirent->d_name, md->dirent->d_encoding, to_fd,
			    "through quota domains", &error);
			if (error)
				goto out;

			/* Push md's tmp dir that we made onto
			 * traversal stack and return
			 * (depth first traversal) */
			uint64_uint64_map_add(lin_hash, tmp_st.st_ino,
			    &md->stat->st_ino);

			/* fake the tmp for md as a migr_dirent
			 *      in order to push. */
			fmt_print(&fmt, "%s%llx", TW_MV_TMPNAME,
			    md->stat->st_ino);
			strncpy(hack_dirent.d_name, fmt_string(&fmt),
			    MAXNAMLEN);
			hack_dirent.d_encoding = ENC_DEFAULT;
			hack_dirent.d_namlen =
			    strlen(hack_dirent.d_name);
			hack_md.dirent = &hack_dirent;
			migr_tw_push(tw, &hack_md, &error);

			migr_dir_unref(current_dir, md);
			pushed = true;
			goto out;
		}
	} else {
		/* this is just a file, rename will work fine */
		siq_enc_renameat(from_fd, md->dirent->d_name,
		    md->dirent->d_encoding, to_fd, md->dirent->d_name,
		    md->dirent->d_encoding, &error);
		if (error != NULL) {
			/* Bug 206186 - ignore ENOENT */
			clear_enoent(&error);
			goto out;
		}
	}

out:
	if (md_fd != -1)
		close(md_fd);
	if (tmp_fd != -1)
		close(tmp_fd);

	isi_error_handle(error, error_out);
	return pushed;
}

/*
 * Move all dirents of a directory to the destination one dirent at a time.
 * The destination is stored in the lin_hash mapping.
 */
static bool
treemv_mv_dirents(struct migr_sworker_ctx* sw_ctx, struct migr_tw *tw,
    struct migr_dir *current_dir, struct uint64_uint64_map *lin_hash,
    struct isi_error **error_out)
{
	struct migr_dirent *md = NULL;
	struct isi_error *error = NULL;
	int to_fd = -1;
	bool dir_completed = false;
	ifs_lin_t from_lin;
	ifs_lin_t *to_linp;
	bool pushed = false;

	/* Ensure we're still connected to the pworker. */
	check_connection_with_primary(sw_ctx, &error);
	UFAIL_POINT_CODE(fail_connection_to_primary,
	    if (error == NULL) {
		    error = isi_system_error_new(RETURN_VALUE,
		    "ufp, no connection to primary worker");
		});
	if (error)
		goto out;

	if (current_dir == NULL) {
		dir_completed = true;
		goto out;
	}

	/*
	 * First get the target lin mapping and open the file.
	 */
	from_lin = current_dir->_stat.st_ino;
	to_linp = uint64_uint64_map_find(lin_hash, from_lin);
	ASSERT(*to_linp > 0);

	to_fd = ifs_lin_open(*to_linp, HEAD_SNAPID, O_RDWR);
	if (to_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx", *to_linp);
		goto out;
	}

	md = migr_dir_read(current_dir, &error);
	if (error)
		goto out;
	while (md != NULL) {
		log(TRACE, "treemv walk:current_dir: %s(%llx), md:%s(%llx)\n",
			current_dir->name, current_dir->_stat.st_ino,
			md->dirent->d_name, md->stat->st_ino);

		pushed = treemv_mv_single_dirent(sw_ctx, md, current_dir, to_fd,
		    lin_hash, tw, &error);
		if (error || pushed)
			goto out;
		/* Load next migr_dirent */
		migr_dir_unref(current_dir, md);
		md = migr_dir_read(current_dir, &error);
		if (error)
			goto out;
	}
	dir_completed = true;
	/* Remove the lin hash mapping */
	uint64_uint64_map_remove(lin_hash, from_lin);

out:
	if (to_fd != -1)
		close(to_fd);

	isi_error_handle(error, error_out);
	return dir_completed;
}

/*
 * Given a temporary directory (root_tmp_lin/root_tmp_fd), treewalk it's
 * contents moving all of its dirents into the root_to_lin, using one dirent
 * at a time moves.
 */
static void
treemv_continue(struct migr_sworker_ctx* sw_ctx, ifs_lin_t root_tmp_lin,
    int root_tmp_fd, ifs_lin_t root_to_lin, struct isi_error **error_out)
{
	struct migr_tw tw = {};
	struct dir_slice slice;
	bool continue_flag = true;
	struct path path = {};
	struct uint64_uint64_map lin_hash;
	bool dir_completed = false;
	struct migr_dir *current_dir = NULL;
	char *fullpath = NULL;
	struct isi_error *error = NULL;
	char *prev_dir = NULL;
	int ret = -1;
	int attempt = 0;
	int max_retries = 100;

	slice.begin = DIR_SLICE_MIN;
	slice.end = DIR_SLICE_MAX;

	/* Now get utf8 path for the lin */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, root_tmp_lin, HEAD_SNAPID,
	    0, &fullpath, &error);
	if (error)
		goto out;

	path_init(&path, fullpath, NULL);

	/* Need to open root_tmp_lin inside here so as to avoid fd leak */
	root_tmp_fd = ifs_lin_open(root_tmp_lin, HEAD_SNAPID, O_RDWR);

	if (root_tmp_fd < 0) {
		ON_SYSERR_GOTO(out, errno, error, "Unable to open lin %llx",
		    root_tmp_lin);
	}

	/* Instantiate treewalker */
	migr_tw_init(&tw, root_tmp_fd, &path, &slice, NULL, false, false,
	    &error);
	if (error)
		goto out;

	root_tmp_fd = -1;

	/* Init the hash map and add the first mapping of root lins */
	uint64_uint64_map_init(&lin_hash);
	uint64_uint64_map_add(&lin_hash, root_tmp_lin, &root_to_lin);

	/* Iterate */
	while (continue_flag) {
		current_dir = migr_tw_get_dir(&tw, &error);
		if (error)
			goto out;

		if (prev_dir != NULL) {
			attempt = 0;
			while (attempt <= max_retries && prev_dir != NULL) {
				/* destroy the previous completed dir,
				* since it should now be an empty tmp directory */
				++attempt;
				ret = enc_unlinkat(current_dir->_fd, prev_dir,
				ENC_DEFAULT, AT_REMOVEDIR);
				UFAIL_POINT_CODE(treemv_continue_set_errno, {
				if (attempt == 1) {
					ret = -1;
					errno = RETURN_VALUE;
				}
				});
				if (ret < 0 && errno == ENOTEMPTY) {
					siq_nanosleep(attempt % 5 + 1, 0);
					continue;
				}

				/* if the previous completed dir doesn't exist anymore
				* (we get ENOENT from enc_unlinkat), assume another worker
				* did it and continue */
				if (ret < 0 && errno != ENOENT) {
					error = isi_system_error_new(errno,
					"Failed to remove tmp dir %s"
					"after treemv", current_dir->name);
					goto out;
				}
				free(prev_dir);
				prev_dir = NULL;
			}
		}

		dir_completed = treemv_mv_dirents(sw_ctx, &tw, current_dir,
		    &lin_hash, &error);
		if (error)
			goto out;

		if (dir_completed) {
			if (strcmp(current_dir->name, "BASE") == 0) {
				continue_flag = false;
			} else {
				prev_dir = strdup(current_dir->name);
				continue_flag = migr_tw_pop(&tw, current_dir);
			}
		}
	}

out:
	uint64_uint64_map_clean(&lin_hash);
	migr_tw_clean(&tw);
	path_clean(&path);
	if (fullpath != NULL)
		free(fullpath);
	if (prev_dir != NULL)
		free(prev_dir);

	isi_error_handle(error, error_out);
}

/*
 * Attempts to call treemv_continue. If a race condition occurs such that a
 * dirent is inserted behind the treemv then we might fail to move a non-empty
 * directory. If this is the case, we should retry exactly one time.
 */
static void
treemv_continue_retry(struct migr_sworker_ctx* sw_ctx, ifs_lin_t tmplin,
    int tmp_fd, ifs_lin_t flin, int oldparent_fd, struct isi_error **error_out)
{
	int attempt = 1;
	int ret = -1;
	int max_retries = 100;
	struct fmt FMT_INIT_CLEAN(fmt_tmp_dir);
	struct isi_error *error = NULL;

	while (attempt <= max_retries) {
		/* continue processing quota move*/
		treemv_continue(sw_ctx, tmplin, tmp_fd, flin, &error);
		if (error)
			goto out;

		/* destroy the final empty tmp directory */
		fmt_print(&fmt_tmp_dir, "%s%llx", TW_MV_TMPNAME, flin);
		ret = enc_unlinkat(oldparent_fd, fmt_string(&fmt_tmp_dir),
		    ENC_DEFAULT, AT_REMOVEDIR);
		UFAIL_POINT_CODE(treemv_continue_retry_set_errno, {
		    if (attempt == 1) {
			ret = -1;
			errno = RETURN_VALUE;
		    }
		});
		if (ret == 0 || errno == ENOENT)
			goto out;
		else if (errno != ENOTEMPTY || attempt == max_retries) {
			error = isi_system_error_new(errno,
			    "Failed to remove final tmp dir %s after treemv",
			    fmt_string(&fmt_tmp_dir));
			goto out;
		}

		log(DEBUG, "Failed to remove final tmp dir %s after "
		    "treemv, retrying", fmt_string(&fmt_tmp_dir));

		attempt++;
	}

out:
	isi_error_handle(error, error_out);
}

static void
treemv_restart_rename_and_move(struct migr_sworker_ctx * sw_ctx,
    ifs_lin_t plin, ifs_lin_t flin, ifs_lin_t tlin, int parent_fd,
    int from_fd, int to_fd, char *new_name, char *cur_name, int new_enc,
    bool save_name, struct isi_error **error_out)
{
        char *final_name = NULL, *saved_dirname = NULL;
        char *path = NULL, *secondary_path = NULL;
        enc_t saved_enc = ENC_DEFAULT;
        struct isi_error *error = NULL;
        struct stat from_st = {}, to_st = {};
        int ret= -1;

        if (save_name) {
                read_treemv_orig_name(parent_fd, cur_name,
                    &saved_dirname, &saved_enc, &error);
                if (error) {
                        goto out;
                }
                ret = enc_renameat(parent_fd, cur_name,
                    ENC_DEFAULT, parent_fd, saved_dirname, saved_enc);
                final_name = strdup(saved_dirname);
                ASSERT(final_name != NULL, "failed to allocate memory: %s",
                                __func__);
                free(saved_dirname);
                saved_dirname = NULL;
        } else {
                ret = enc_renameat(parent_fd, cur_name,
                    ENC_DEFAULT, parent_fd, new_name, new_enc);
                final_name = strdup(new_name);
                ASSERT(final_name != NULL, "failed to allocate memory: %s",
                                __func__);
        }
        if (ret < 0) {
                ON_SYSERR_GOTO(out, errno, error,
                    "Failed to rename %s to %s after treemv",
                    cur_name, new_name);
        }
        if (save_name) {
                ret = enc_unlinkat(from_fd, TW_MV_SAVEDDIRNAMEFILE,
                    ENC_DEFAULT, 0);
                if (ret < 0) {
                        ON_SYSERR_GOTO(out, errno, error,
                            "Failed to unlink %s after treemv",
                            TW_MV_SAVEDDIRNAMEFILE);
                }
        }

        /* Move the flin to to_fd if not yet done */
        if (plin != tlin) {
                siq_enc_renameat(parent_fd, final_name,
                    ENC_DEFAULT, to_fd,
                    final_name, new_enc, &error);
                if (error != NULL) {
                        if (isi_system_error_is_a(error, EPERM)) {
                                /* Need to treemv the entries out
                                 * of this dir.
                                 */
                                isi_error_free(error);
                                error = NULL;
                                ret = fstat(parent_fd, &from_st);
                                if (ret < 0) {
                                        ON_SYSERR_GOTO(out, errno, error,
                                                        "fstat failed");
                                }
                                ret = fstat(to_fd, &to_st);
                                if (ret < 0) {
                                        ON_SYSERR_GOTO(out, errno, error,
                                                        "fstat failed");
                                }
                                path = get_valid_rep_path(from_st.st_ino,
                                                HEAD_SNAPID);
                                secondary_path = get_valid_rep_path(to_st.st_ino,
                                                HEAD_SNAPID);
                                log(INFO, "Directory move in quota detected. "
                                        " Starting treemv of %s to %s",
                                        path, secondary_path);
                                free(path);
                                free(secondary_path);
                                treemv(sw_ctx, plin, parent_fd,
                                        flin, to_fd, new_enc, final_name,
                                        final_name, NULL, true, &error);
                                /* Bug 206186 - continue on ENOENT */
                                clear_enoent(&error);
                                if (error)
                                        goto out;
                        } else {
                               clear_enoent(&error);
                                if (error)
                                        goto out;
                        }
                }
        }

out:
        if (final_name)
                free(final_name);
        if (saved_dirname)
                free(saved_dirname);
        isi_error_handle(error, error_out);
}

/*
 *  Restarts a previous treemv that has stopped or failed previously.
 *  Moves a dirent (flin/cur_name) with a name starting with ".tw-tmp-"
 *  to a target directory (to_fd) with name (new_name) and encoding (new_enc).
 *  The move is done by moving one dirent at a time,
 *  allowing moves of non-empty directories accross quotas.
 *
 *  ASSUMPTION: curfname is of the pattern ".tw-tmp-LINNUMBER" with LINNUMBER
 *      of the tmp dir of a incomplete sync.
 */
void
treemv_restart(struct migr_sworker_ctx* sw_ctx, ifs_lin_t plin, ifs_lin_t flin,
    ifs_lin_t tlin, int new_enc, char *new_name, char *cur_name,
    bool save_name, struct isi_error** error_out)
{
	int tmp_fd = -1, oldparent_fd = -1, from_fd = -1, to_fd = -1;
	int parent_fd = -1;
	bool found = false;
	ifs_lin_t tmplin, oldplin;
	struct dirent tmp_dirent;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	/* open the plin/flin input to make sure they are there/ok */
	from_fd = ifs_lin_open(flin, HEAD_SNAPID, O_RDWR);
	if (from_fd < 0) {
		error = isi_system_error_new(errno, "Unable to open lin %llx",
		    flin);
		goto out;
	}
	to_fd = ifs_lin_open(tlin, HEAD_SNAPID, O_RDWR);
	if (to_fd < 0) {
		error = isi_system_error_new(errno, "Unable to open lin %llx",
		    tlin);
		goto out;
	}

	/* validate the beginning of the cur_name, if it's not right, we
	 * need to fail */
	if (0 != strncmp(cur_name, TW_MV_TOPTMPNAME, strlen(TW_MV_TOPTMPNAME))
	    || 1 != sscanf(cur_name, TW_MV_TOPTMPNAME "%llx", &tmplin)) {
		error = isi_system_error_new(EINVAL, "current filename %s is "
			"does not match the required pattern for quota tree "
			"move restarts.", cur_name);
		goto out;
	}
	tmp_fd = ifs_lin_open(tmplin, HEAD_SNAPID, O_RDWR);
	if (tmp_fd < 0 && errno == ENOENT) {
		/* If the first tmpdir does not exist, Need to rename the flin`
		 * and move it to tlin if needed.
		 */
		parent_fd = ifs_lin_open(plin, HEAD_SNAPID, O_RDWR);
                if (parent_fd < 0) {
                        ON_SYSERR_GOTO(out, errno, error, "Unable to open lin %llx",
                            plin);
                }
                treemv_restart_rename_and_move(sw_ctx, plin, flin, tlin, parent_fd,
                                from_fd, to_fd, new_name,
                                cur_name, new_enc, save_name, &error);
                goto out;
	} else if (tmp_fd < 0) {
                ON_SYSERR_GOTO(out, errno, error, "Unable to open lin %llx",
                    tmplin);
	}

	/* Also need to lookup/open the parent of the tmp dir we just found. */
	found = dirent_lookup_by_lin(HEAD_SNAPID, tmplin, 0, &oldplin,
		    &tmp_dirent, &error);
	if (error)
		goto out;
	else if (!found) {
		error = isi_system_error_new(EINVAL, "Unable to lookup parent "
			    "lin of tmp directory %llx", tmplin);
		goto out;
	}

	/* open the old parent dir */
	oldparent_fd = ifs_lin_open(oldplin, HEAD_SNAPID, O_RDWR);
	if (oldparent_fd < 0) {
		error = isi_system_error_new(errno, "Unable to open lin %llx",
		    oldplin);
		goto out;
	}

	/* check if the first dir was completed */
	if (plin == oldplin) {
		/* mv children to the tmp folder */
		treemv_dirents_rename(sw_ctx, from_fd, tmp_fd, tmplin, plin,
		    oldparent_fd, cur_name, ENC_DEFAULT, to_fd,
		    "to temporary dir", &error);
		if (error)
			goto out;
		parent_fd = to_fd;
	} else {
		/* flin was not moved to to_fd
		 * First complete the treemv and then move the flin to to_fd
		 */
		parent_fd = ifs_lin_open(plin, HEAD_SNAPID, O_RDWR);
	}

	/* Due to restart logic will close the tmp_fd in tw_clean(),
	 * we need to close the tmp_fd here and open it inside
	 * treemv_continue_retry() logic.
	 */
	if (tmp_fd != -1) {
		close(tmp_fd);
		tmp_fd = -1;
	}

	treemv_continue_retry(sw_ctx, tmplin, tmp_fd, flin, oldparent_fd,
	    &error);
	if (error)
		goto out;

        treemv_restart_rename_and_move(sw_ctx, plin, flin, tlin, parent_fd,
                        from_fd, to_fd, new_name, cur_name,
                        new_enc, save_name, &error);
        if (error)
                goto out;
out:
	if (tmp_fd != -1)
		close(tmp_fd);
	if (from_fd != -1)
		close(from_fd);
	if (to_fd != -1)
		close(to_fd);
	if (oldparent_fd != -1)
		close(oldparent_fd);
	if (parent_fd != to_fd)
		close(parent_fd);
	isi_error_handle(error, error_out);
}

/*
 *  Handles moving a dirent (flin/cur_name) with parent fd (parent_fd) to a
 *  target directory (to_fd) with name (new_name) and encoding (new_enc).
 *  The move is done by moving one dirent at a time,
 *  allowing moves of non-empty directories accross quotas.
 *
 *  If for some reason the move crashes/stops while in progress, treemv_restart
 *  can be called to successfully pick up where this leaves off.
 */
void
treemv(struct migr_sworker_ctx* sw_ctx, ifs_lin_t parent_lin, int parent_fd,
    ifs_lin_t flin, int to_fd, int new_enc, char *new_name, char *cur_name,
    struct dirent* de, bool save_name, struct isi_error** error_out)
{
	int from_fd = -1, tmp_fd = -1;
	int ret = -1;
	struct stat from_st = {};
	struct stat tmp_st = {};
	struct fmt FMT_INIT_CLEAN(fmt_tmp_from);
	bool is_dir;
	struct isi_error *error = NULL;
	char *saved_dirname = NULL;
	enc_t saved_enc = ENC_DEFAULT;

	log(TRACE, "%s", __func__);

	/* Ensure we're still connected to the pworker. */
	check_connection_with_primary(sw_ctx, &error);
	if (error)
	{
		log(ERROR, "Lost connection with primary worker.");
		goto out;
	}

	/* validate the flin by trying to open */
	from_fd = ifs_lin_open(flin, HEAD_SNAPID, O_RDWR);
	if (from_fd == -1) {
		error = isi_system_error_new(errno, "Failed to open "
		    "flin %llx", flin);
		goto out;
	}
	if (de)
		is_dir = de->d_type == DT_DIR;
	else {
		/* stat the dirent we are moving */
		ret = enc_fstatat(parent_fd, cur_name, new_enc, &from_st,
		    AT_SYMLINK_NOFOLLOW);
		if (ret != 0) {
			if (errno)
				error = isi_system_error_new(errno,
				    "Stat of dirent %s (%s) "
				    "(parent %llx) failed",
				    cur_name, enc_get_name(new_enc),
				    parent_lin);
			goto out;
		}
		is_dir = S_ISDIR(from_st.st_mode);
	}

	if (is_dir) {
		/* mk root treewalk tmp folder */
		treemv_get_tmp_dir(parent_fd, flin, &tmp_fd, &tmp_st, &error);
		if (error)
			goto out;

		/* save the name/encoding into temp file */
		if (save_name) {
			write_treemv_orig_name(new_name, new_enc, tmp_fd,
			    flin, &error);
			if (error)
				goto out;
		}

		/* rename first dir to flag beginning of move */
		fmt_print(&fmt_tmp_from, "%s%llx", TW_MV_TOPTMPNAME,
		    tmp_st.st_ino);
		ret = enc_renameat(parent_fd, cur_name,
		    de == NULL ? new_enc : de->d_encoding, parent_fd,
		    fmt_string(&fmt_tmp_from), ENC_DEFAULT);
		if (ret < 0) {
			error = isi_system_error_new(errno,
			    "Failed to rename %s to tmp name %s",
			    cur_name, fmt_string(&fmt_tmp_from));
			goto out;
		}

		/* From this point on the move is restartable.*/

		/* mv first dir children to the tmp folder */
		treemv_dirents_rename(sw_ctx, from_fd, tmp_fd, tmp_st.st_ino,
		    parent_lin, parent_fd, fmt_string(&fmt_tmp_from),
		    ENC_DEFAULT, to_fd, "to temporary dir", &error);
		if (error)
			goto out;

		treemv_continue_retry(sw_ctx, tmp_st.st_ino, tmp_fd, flin, parent_fd,
		    &error);
		if (error)
			goto out;

		/* finish the move by renaming the final dir */
		if (save_name) {
			read_treemv_orig_name(to_fd, fmt_string(&fmt_tmp_from),
			    &saved_dirname, &saved_enc, &error);
			if (error) {
				goto out;
			}
			ret = enc_renameat(to_fd, fmt_string(&fmt_tmp_from),
			    ENC_DEFAULT, to_fd, saved_dirname, saved_enc);
			free(saved_dirname);
		} else {
			ret = enc_renameat(to_fd, fmt_string(&fmt_tmp_from),
			    ENC_DEFAULT, to_fd, new_name, new_enc);
		}
		if (ret < 0) {
			error = isi_system_error_new(errno,
			    "Failed to rename %s to %s after treemv",
			    fmt_string(&fmt_tmp_from), new_name);
			goto out;
		}
		if (save_name) {
			ret = enc_unlinkat(from_fd, TW_MV_SAVEDDIRNAMEFILE,
			    ENC_DEFAULT, 0);
			if (ret < 0) {
				error = isi_system_error_new(errno,
				    "Failed to unlink %s after treemv",
				    TW_MV_SAVEDDIRNAMEFILE);
				goto out;
			}
		}
	} else {
		/* simply a file, just move it.
		 * SIQ should have attempted this already. */
		siq_enc_renameat(parent_fd, new_name, new_enc,
			to_fd, new_name, new_enc, &error);
		if (error != NULL)
			goto out;
	}

out:
	if (tmp_fd != -1)
		close(tmp_fd);
	if (from_fd != -1)
		close(from_fd);
	isi_error_handle(error, error_out);
}

