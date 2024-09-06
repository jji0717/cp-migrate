#include <libgen.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>

#include <isi_quota/isi_quota.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/base64enc.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/siq_error.h"
#include "sworker.h"

/* Returns the appropriate tmp dir name based on if this is
 * a restore job or not. */
void
get_tmp_working_dir_name(char *buf, bool restore)
{
	ASSERT(buf != NULL);

	if (restore) {
		snprintf(buf, MAXNAMELEN, "%s_restore", TMP_WORKING_DIR);
	} else {
		snprintf(buf, MAXNAMELEN, "%s", TMP_WORKING_DIR);
	}
}

ifs_lin_t
get_hash_dirname(ifs_lin_t lin, char **dirname_out)
{
	ifs_lin_t mask = lin & TMPDIR_MASK;
	ASSERT(*dirname_out == NULL);
	asprintf(dirname_out, TMPDIR_HASH_FMT, TMPDIR_HASH_PREFIX, mask);
	ASSERT(*dirname_out != NULL);
	return mask;
}

static size_t
tmpdir_hash_mask_keysize(const void *x)
{
	const struct tmpdir_hash_key *key = x;
	return sizeof(*key);
}

static void
tmpdir_hash_free(struct isi_hash *tmpdir_hash)
{
	struct tmpdir_hash_entry *te = NULL;

	if (tmpdir_hash != NULL) {
		ISI_HASH_FOREACH(te, tmpdir_hash, entry) {
			isi_hash_remove(tmpdir_hash, &te->entry);
			free(te);
		}
		isi_hash_destroy(tmpdir_hash);
		free(tmpdir_hash);
	}
}

static void
tmpdir_hash_init(struct migr_sworker_global_ctx *global, int tmp_fd,
    ifs_lin_t tmp_lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_hash *tmpdir_hash = NULL;
	struct tmpdir_hash_entry *te = NULL;
	struct stat st;
	ifs_lin_t i, mask = (TMPDIR_MASK) + 1;
	int ret;
	char *hashdir = NULL;

	/* Bug 215632 - calling twice frees the prior hash */
	if (global->tmpdir_hash != NULL) {
		tmpdir_hash_free(global->tmpdir_hash);
		global->tmpdir_hash = NULL;
	}

	tmpdir_hash = calloc(1, sizeof(struct isi_hash));
	ASSERT(tmpdir_hash != NULL, "calloc failed: %s", strerror(errno));

	ISI_HASH_VAR_INIT(tmpdir_hash, tmpdir_hash_entry, key, entry,
	    tmpdir_hash_mask_keysize, NULL);

	/*
	 * Bug 215632
	 * Look for existing temporary directories
	 * Uninitialized tmp hashdir's will map to INVALID_LIN.
	 */
	for (i = 0; i < mask; i++) {
		te = calloc(1, sizeof(struct tmpdir_hash_entry));
		ASSERT(te != NULL, "calloc failed: %s", strerror(errno));
		te->key.root_lin = tmp_lin;
		te->key.mask = get_hash_dirname(i, &hashdir);
		ret = enc_fstatat(tmp_fd, hashdir, ENC_DEFAULT, &st,
		    AT_SYMLINK_NOFOLLOW);
		if (ret == -1) {
			if (errno != ENOENT) {
				error = isi_system_error_new(errno,
				    "stat failed of hash tmp dir %s",
				    hashdir);
				goto out;
			}
			te->lin = INVALID_LIN;
		} else
			te->lin = st.st_ino;

		isi_hash_add(tmpdir_hash, &te->entry);
		free(hashdir);
		hashdir = NULL;
		te = NULL;
	}

	global->tmpdir_hash = tmpdir_hash;
	tmpdir_hash = NULL;
out:
	if (error && tmpdir_hash != NULL) {
		tmpdir_hash_free(tmpdir_hash);
		tmpdir_hash = NULL;
		global->tmpdir_hash = NULL;
	}
	free(te);
	isi_error_handle(error, error_out);
}

static ifs_lin_t
get_tmpdir_hash_lin(const ifs_lin_t dir_lin, const ifs_lin_t tmp_lin,
    struct isi_hash *tmpdir_hash, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct tmpdir_hash_entry *te = NULL, *new_te = NULL;
	struct isi_hash_entry *he = NULL;
	struct tmpdir_hash_key key = {};
	struct stat st;
	char *hashdir = NULL;
	ifs_lin_t lin = INVALID_LIN;
	int fd = -1, ret;

	log(TRACE, "%s(%{}, %{},...)", __func__, lin_fmt(dir_lin),
	    lin_fmt(tmp_lin));

	key.root_lin = tmp_lin;
	key.mask = get_hash_dirname(dir_lin, &hashdir);
	he = isi_hash_get(tmpdir_hash, &key);
	if (he != NULL) {
		te = container_of(he, struct tmpdir_hash_entry, entry);
		if (te->lin != INVALID_LIN) {
			lin = te->lin;
			goto out;
		}
		ASSERT(te->key.root_lin == tmp_lin && te->key.mask == key.mask,
		    "unexpected root_lin: %{},%{} tmp_lin: %{},%{}",
		    lin_fmt(te->key.root_lin), lin_fmt(te->key.mask),
		    lin_fmt(tmp_lin), lin_fmt(key.mask));
	}
	fd = ifs_lin_open(tmp_lin, HEAD_SNAPID, O_RDONLY);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "unable to open tmp lin %{}",
		    lin_fmt(tmp_lin));
		goto out;
	}
	ret = enc_mkdirat(fd, hashdir, ENC_DEFAULT, 0775);
	if (ret == -1) {
		if (errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "failed to create sub dir %s in lin %{}",
			    hashdir, lin_fmt(tmp_lin));
			goto out;
		}
	}
	ret = enc_fstatat(fd, hashdir, ENC_DEFAULT, &st, 0);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "failed to stat sub dir %s in lin %{}",
		    hashdir, lin_fmt(tmp_lin));
		goto out;
	}
	if (!S_ISDIR(st.st_mode)) {
		error = isi_system_error_new(ENOTDIR,
		    "unexpected file type for lin %{} mode: %o",
		    lin_fmt(st.st_ino), st.st_mode);
		goto out;
	}
	new_te = calloc(1, sizeof(struct tmpdir_hash_entry));
	ASSERT(new_te != NULL, "calloc failed");
	new_te->key.root_lin = tmp_lin;
	new_te->key.mask = key.mask;
	lin = new_te->lin = st.st_ino;
	if (he != NULL) {
		isi_hash_replace(tmpdir_hash, he, &new_te->entry);
		free(te);
	} else
		isi_hash_add(tmpdir_hash, &new_te->entry);
	new_te = NULL;
out:
	if (fd != -1)
		close(fd);
	free(hashdir);
	isi_error_handle(error, error_out);
	return lin;
}

/*
 * Opens and returns the fd for the tmp directory that applies to dir_lin.
 * The caller is responsible for closing the fd.
 *
 * NOTE: This should be called with dir_lins and not file_lins because of
 *       hardlinks. Which do not guarantee what path of quotas we are
 *	 going to get from get_quotas_for_lin() are for the path we desire.
 */
int
get_tmp_fd(ifs_lin_t dir_lin, struct migr_sworker_global_ctx *global,
    struct isi_error **error_out)
{
	int quota_fd = -1;
	ifs_lin_t matched_lin;
	struct isi_error *error = NULL;

	log(TRACE, "%s(%{},...)", __func__, lin_fmt(dir_lin));

	ASSERT(global != NULL);

	matched_lin = get_tmp_lin(dir_lin, global, NULL, &error);
	if (error)
		goto out;

	quota_fd = ifs_lin_open(matched_lin, HEAD_SNAPID, O_RDWR);
	if (quota_fd < 0) {
		error = isi_system_error_new(errno,
		    "Cannot get tmp fd, unable to open quota lin: %llx",
		    matched_lin);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return quota_fd;
}

/*
 * Opens and returns the lin for the tmp directory that applies to dir_lin.
 * The lookup will iterate the quotas that apply to the lin and use the longest
 * matching quota path to lookup the tmp_dir.  If no tmp dir is found through
 * the map, then the root_tmp_lin is returned.
 *
 * NOTE: This should be called with dir_lins and not file_lins because of
 *       hardlinks. Which do not guarantee what path we are
 *	 going to get from lin_get_path(). */
ifs_lin_t
get_tmp_lin(ifs_lin_t dir_lin, struct migr_sworker_global_ctx *global,
    ifs_lin_t *tmp_lin_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *dirents = NULL;
	struct dirent *dir_ptr = NULL;
	ifs_lin_t found_lin = 0, *match_lin = NULL, *ret_lin = NULL, tmp_lin;
	size_t list_len = 0;
	int ret = -1, offset = 0;

	log(TRACE, "%s(%{},...)", __func__, lin_fmt(dir_lin));

	ASSERT(global != NULL);

	if (dir_lin != 0 && global->quotas_present) {
		ret = lin_get_path_plus(ROOT_LIN, dir_lin,
		    HEAD_SNAPID, 0, 0, ENC_DEFAULT, NULL, NULL,
		    &list_len, &dirents);

		if (ret >= 0) {
			for (offset = 0, dir_ptr = (struct dirent *) dirents;
			     offset < list_len;
			     offset += dir_ptr->d_reclen,
			     dir_ptr = (struct dirent *)(dirents + offset)) {
				if (dir_ptr->d_fileno == dir_lin)
					continue;

				ret_lin = tmp_map_find(global,
				    dir_ptr->d_fileno, &error);
				if (error)
					goto out;
				if (ret_lin != NULL && *ret_lin != dir_lin)
					match_lin = ret_lin;
			}
		}
	} /* else { map is empty and match_lin is always NULL } */

	/* match will be NULL if there was no match,
	 * in this case, return the root tmp lin */
	if (match_lin == NULL && global->tmpdir_hash == NULL) {
		log(TRACE, "Match lin not found, using global: %{}.",
		    lin_fmt(global->root_tmp_lin));
		found_lin = global->root_tmp_lin;
		if (tmp_lin_out != NULL)
			*tmp_lin_out = found_lin;
		goto out;
	}

	if (match_lin != NULL) {
		tmp_lin = *match_lin;
		if (global->tmpdir_hash == NULL) {
			found_lin = tmp_lin;
			if (tmp_lin_out != NULL)
				*tmp_lin_out = tmp_lin;
			goto out;
		}
	} else
		tmp_lin = global->root_tmp_lin;

	ASSERT(global->tmpdir_hash != NULL);
	found_lin = get_tmpdir_hash_lin(dir_lin, tmp_lin,
	    global->tmpdir_hash, &error);
	if (error != NULL)
		goto out;
	ASSERT(found_lin != INVALID_LIN,
	    "error is NULL, but found_lin == INVALID_LIN");
	if (tmp_lin_out != NULL)
		*tmp_lin_out = tmp_lin;
out:
	if (dirents != NULL)
		free(dirents);
	isi_error_handle(error, error_out);
	return found_lin;
}

/* Returns true if the dir_lin given is a temporary directory,
 * false otherwise. */
bool
is_tmp_lin(ifs_lin_t dir_lin, struct migr_sworker_global_ctx *global,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_lin_t matched_lin, tmp_lin;
	struct parent_entry pe;
	unsigned int num_entries = 1;
	int ret;
	char *path = NULL, *bname_r = NULL, bname[MAXNAMELEN + 1],
	    tmpdir_prefix[MAXNAMELEN + 1] = TMPDIR_HASH_PREFIX;
	bool is_tmp = false;

	log(TRACE, "%s(%{},...)", __func__, lin_fmt(dir_lin));

	ASSERT(global != NULL);

	/* fast path */
	if (dir_lin == global->root_tmp_lin) {
		is_tmp = true;
		goto out;
	}

	matched_lin = get_tmp_lin(dir_lin, global, &tmp_lin, &error);
	if (error != NULL)
		goto out;
	is_tmp = (dir_lin == matched_lin || dir_lin == tmp_lin);
	if (is_tmp)
		goto out;
	/*
	 * Bug 215632
	 * Check to see if we're in a tmp hash dir.
	 * XXX: maybe we should cache the lins
	 */
	ret = pctl2_lin_get_parents(dir_lin, HEAD_SNAPID,
	    &pe, 1, &num_entries);
	if (ret == -1) {
		if (errno == ENOSPC)
			ASSERT(num_entries > 1, "bad num_entries %d",
			    num_entries);
		else {
			if (errno != ENOENT)
				error = isi_system_error_new(errno,
				    "Failed to get parent lin for %{}",
				lin_fmt(dir_lin));
			goto out;
		}
	}
	dir_get_utf8_str_path("/ifs/",
	    ROOT_LIN, dir_lin, HEAD_SNAPID,
	    pe.lin, &path, &error);
	if (error != NULL) {
		if (isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			error = NULL;
		}
		goto out;
	}
	bname_r = basename_r(path, bname);
	if (bname_r == NULL) {
		error = isi_system_error_new(errno,
		    "Failed to get basename of lin%{}",
		    lin_fmt(dir_lin));
		goto out;
	}
	is_tmp = ((pe.lin == tmp_lin)
	    && (0 == strncmp(bname_r, tmpdir_prefix,
	    strnlen(tmpdir_prefix, sizeof(tmpdir_prefix) - 1))));
out:
	free(path);
	isi_error_handle(error, error_out);
	return is_tmp;
}

static ifs_lin_t
find_quota_dir(ifs_lin_t qlin, char *tmp_name, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat tmp_st;
	ifs_lin_t tlin = 0;
	int quota_fd = -1, ret = 0;

	/* Open the main Quota dir */
	quota_fd = ifs_lin_open(qlin, HEAD_SNAPID, O_RDWR);
	if (quota_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open quota lin: %{}",
		    lin_fmt(qlin));
		goto out;
	}

	/* look for existing tmpdir to add to the tmp map */
	ret = enc_fstatat(quota_fd, tmp_name, ENC_DEFAULT,
	    &tmp_st, AT_SYMLINK_NOFOLLOW);
	if (ret != 0) {
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Unable to get file descriptor"
			    " for tmp quota working "
			    "directory: %{}", lin_fmt(qlin));
			goto out;
		}
		tlin = INVALID_LIN;
	} else
		tlin = tmp_st.st_ino;
out:
	if (quota_fd != -1) {
		close(quota_fd);
		quota_fd = -1;
	}
	isi_error_handle(error, error_out);
	return tlin;
}

/* Initializes the tmp dir variables in the global_ctx.
 * The root tmp dir is always below root_path, other tmp dirs will be added
 * to the tmp_dir_mapping if found.  Currently, additional tmp dirs are created
 * on the root of any quota found below the root_path.
 *
 * If any quota tmp dirs are found, global->quotas_present will be set to true.
 *
 * If create_dirs is set, the tmp dirs are also created.
 */
int
initialize_tmp_map(const char *root_path, const char *policy_id,
    struct migr_sworker_global_ctx *global, bool restore,
    bool create_dirs, struct isi_error **error_out)
{
	int tmp_fd = -1;
	char tmp_name[MAXNAMELEN + 1];
	char tmp_path[MAXPATHLEN + 1];
	struct path tmpdir_path = {};
	struct isi_error *error = NULL;
	int ret = 0;
	struct stat root_st, tmp_st, hdir_st;
	ifs_lin_t qlin, tlin, map_lin;
	struct uint64_uint64_map *new_map = NULL;
	struct quota_queue_head *quotas = NULL;
	struct qdi_wrapper *entry = NULL, *tmp = NULL;
	struct lmap_log tw_log;
	struct map_iter *iter = NULL;
	char tmp_working_map_name[MAXNAMELEN + 1];
	char *hashdir = NULL;
	bool found;
	bool found_hash_tmpdir = false;

	log(TRACE, "%s", __func__);

	ASSERT(global != NULL);
	ASSERT(root_path != NULL);

	get_tmp_working_dir_name(tmp_name, restore);
	snprintf(tmp_path, sizeof(tmp_path), "%s/%s", root_path, tmp_name);
	path_init(&tmpdir_path, tmp_path, 0);
	get_hash_dirname(INVALID_LIN, &hashdir);

	memset(&tmp_st, 0, sizeof(struct stat));

	UFAIL_POINT_CODE(enable_hash_tmpdir,
	    global->enable_hash_tmpdir = true;);

	/* Get the root tmp dir's lin to ensure we have it. */
	if (create_dirs) {
		ret = smkchdirfd(&tmpdir_path, AT_FDCWD, &tmp_fd);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "Unable to create and get file descriptor"
			    " for tmp working directory: %s", tmp_path);
			goto out;
		}
		ASSERT(tmp_fd != -1);
		/* create sentinel hash dir */
		if (global->enable_hash_tmpdir) {
			ret = enc_mkdirat(tmp_fd, hashdir, ENC_DEFAULT, 0755);
			if (ret == -1) {
				if (errno != EEXIST) {
					error = isi_system_error_new(errno,
					    "Unable to create tmp hash dir");
					goto out;
				}
				/* check if it's a directory */
				ret = enc_fstatat(tmp_fd, hashdir, ENC_DEFAULT,
				    &hdir_st, AT_SYMLINK_NOFOLLOW);
				if (ret == -1) {
					error = isi_system_error_new(errno,
					    "Unable to stat tmp hash dir");
					goto out;

				} else if (!S_ISDIR(hdir_st.st_mode)) {
					error = isi_system_error_new(ENOTDIR,
					    "found unexpected file type %{} "
					    "mode: %o",
					    lin_fmt(hdir_st.st_ino),
					    hdir_st.st_mode);
					goto out;
				}
			}
			found_hash_tmpdir = true;
		}
	} else {
		ret = schdirfd(&tmpdir_path, AT_FDCWD, &tmp_fd);
		if (ret != 0) {
			if (errno != ENOENT) {
				error = isi_system_error_new(errno,
				    "Unable to get file descriptor"
				    " for tmp working directory: %s", tmp_path);
			}
			goto out;
		}
		/* look for sentinel hash dir */
		if (tmp_fd != -1) {
			ret = enc_fstatat(tmp_fd, hashdir, ENC_DEFAULT, &hdir_st,
			    AT_SYMLINK_NOFOLLOW);
			if (ret == -1) {
				if (errno != ENOENT) {
					error = isi_system_error_new(errno,
					    "stat failed of hash tmp dir");
					goto out;
				}
			} else if (!S_ISDIR(hdir_st.st_mode)) {
				error = isi_system_error_new(ENOTDIR,
				    "found unexpected file type %{} mode: %o",
				    lin_fmt(hdir_st.st_ino), hdir_st.st_mode);
				goto out;
			} else
				found_hash_tmpdir = true;
		} else
			/* exit if there is no tmp directory */
			goto out;
	}

	ret = fstat(tmp_fd, &tmp_st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat tmp dir: %s", tmp_path);
		goto out;
	}

	global->root_tmp_lin = tmp_st.st_ino;
	/*
	 * Bug 215632
	 * Build the hash tmpdir isi_hash for the global tmp dir.
	 * Quota entries will be added as needed.
	 */
	if (found_hash_tmpdir) {
		tmpdir_hash_init(global, tmp_fd, tmp_st.st_ino, &error);
		if (error)
			goto out;
	}

	/*
	 * Bug 197140
	 * Only initialize tmp_working_map and tmp_working_map_ctx if the
	 * global tmp dir exists.
	 */
	new_map = calloc(1, sizeof(struct uint64_uint64_map));
	ASSERT(new_map != NULL, "failure to allocate memory: %s", __func__);
	uint64_uint64_map_init(new_map);
	global->tmp_working_map = new_map;

	/* tmp working linmap */
	get_tmp_working_map_name(tmp_working_map_name, policy_id);
	if (create_dirs) {
		create_linmap(policy_id, tmp_working_map_name, true, &error);
		if (error)
			goto out;
	}
	ASSERT(global->tmp_working_map_ctx == NULL);
	open_linmap(policy_id, tmp_working_map_name,
	    &global->tmp_working_map_ctx, &error);
	if (error)
		goto out;

	if (create_dirs) {
		/*
		 * iterate over the linmap
		 * for each quota dir key
		 *   confirm directory lin exists
		 *   if the key value is INVALID_LIN, delete entry
		 *   otherwise check if .tmp_working_dir exists in quota dir
		 *     if lin of .tmp_working_dir matches mapping entry, keep
		 *       mapping
		 *     otherwise update entry
		 */
		iter = new_map_iter(global->tmp_working_map_ctx);
		bzero(&tw_log, sizeof(struct lmap_log));
		ASSERT(iter != NULL);
		found = true;
		while (true) {
			found = map_iter_next(iter, &qlin, &map_lin, &error);
			if (error)
				goto out;
			if (!found)
				break;
			/*
			 * Look for a temp dir for any key lin in the linmap
			 * in case a directory was created but the linmap
			 * update failed.
			 */
			tlin = find_quota_dir(qlin, tmp_name, &error);
			if (error) {
				if (isi_system_error_is_a(error, ENOENT)) {
					log(ERROR,
					    isi_error_get_message(error));
					isi_error_free(error);
					error = NULL;
					tlin = INVALID_LIN;

				} else
					goto out;
			}
			ASSERT(tlin != 0);
			/*
			 * delete linmap entry if it was INVALID_LIN or if the
			 * existing working directory lin didn't match the linmap
			 * lin
			 */
			if (tlin == INVALID_LIN || tlin != map_lin) {
				lmap_log_add_entry(
				    global->tmp_working_map_ctx,
				    &tw_log, qlin, 0, LMAP_REM,
				    SOCK_LOCAL_OPERATION, &error);
				if (error)
					goto out;
			}
			if (tlin != INVALID_LIN) {
				lmap_log_add_entry(global->tmp_working_map_ctx,
				    &tw_log, qlin, tlin, LMAP_SET,
				    SOCK_LOCAL_OPERATION, &error);
				if (error)
					goto out;
				uint64_uint64_map_add(new_map, qlin, &tlin);
				/*
				 * set the quotas_present flag if we write a
				 * value into the map
				 */
				if (!global->quotas_present)
					global->quotas_present = true;
			}
		}
		close_map_iter(iter);
		iter = NULL;
		flush_lmap_log(&tw_log, global->tmp_working_map_ctx, &error);
		if (error)
			goto out;

		/* Find Quotas tmp dirs */
		ret = stat(root_path, &root_st);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "Stat of root target dir %s failed", root_path);
			goto out;
		}

		quotas = get_quotas_for_lin(root_st.st_ino,
		    QUOTA_LIN_QUERY_RECURSIVE, &error);
		if (error != NULL)
			goto out;

		STAILQ_FOREACH_SAFE(entry, quotas, next, tmp) {
			/* Get the Quota's main lin */
			qlin = qdi_get_lin(&entry->qdi);

			/*
			 * If a quota exists on the root-path,
			 * we are fine to ignore because root tmp dir covers
			 * it.
			 */
			if (root_st.st_ino == qlin) {
				STAILQ_REMOVE_HEAD(quotas, next);
				free(entry);
				continue;
			}

			/* we officially have a quota on the target sync path */
			if (!global->quotas_present)
				global->quotas_present = true;

			tlin = find_quota_dir(qlin, tmp_name, &error);
			if (error)
				goto out;
			ASSERT(tlin != 0);
			/* add tmp map entry */
			lmap_log_add_entry(global->tmp_working_map_ctx, &tw_log,
			    qlin, tlin, LMAP_SET, SOCK_LOCAL_OPERATION, &error);
			if (error)
				goto out;

			uint64_uint64_map_add(new_map, qlin, &tlin);

			/* Free as we go */
			STAILQ_REMOVE_HEAD(quotas, next);
			free(entry);

		}

		flush_lmap_log(&tw_log, global->tmp_working_map_ctx, &error);
		if (error)
			goto out;
	} else {
		/* store tmp working linmap in memory */
		iter = new_map_iter(global->tmp_working_map_ctx);
		ASSERT(iter != NULL);
		found = true;
		while (true) {
			found = map_iter_next(iter, &qlin, &tlin, &error);
			if (error)
				goto out;
			if (!found)
				break;
			uint64_uint64_map_add(new_map, qlin, &tlin);
			if (!global->quotas_present)
				global->quotas_present = true;
		}
		close_map_iter(iter);
		iter = NULL;
	}

out:
	if (tmp_fd != -1 && (!create_dirs || error != NULL)) {
		close(tmp_fd);
		tmp_fd = -1;
	}

	if (quotas != NULL)
		free(quotas);
	path_clean(&tmpdir_path);
	if (iter != NULL)
		close_map_iter(iter);
	if (hashdir)
		free(hashdir);
	isi_error_handle(error, error_out);

	return tmp_fd;
}

ifs_lin_t *
tmp_map_find(struct migr_sworker_global_ctx *global, ifs_lin_t qlin,
    struct isi_error **error_out)
{
	struct stat tmp_st;
	struct isi_error *error = NULL;
	ifs_lin_t *quota_tmp_lin = NULL, tlin;
	int quota_fd = -1, ret = 0;
	char tmp_name[MAXNAMELEN + 1];
	bool is_compliance;

	quota_tmp_lin = uint64_uint64_map_find(global->tmp_working_map, qlin);
	if (quota_tmp_lin == NULL || *quota_tmp_lin != INVALID_LIN)
		goto out;

	/* check tmp working linmap in case it's been updated */
	ret = get_mapping(qlin, &tlin, global->tmp_working_map_ctx,
	    &error);
	if (error)
		goto out;
	ASSERT(ret, "%{} missing in tmp_working_map", lin_fmt(qlin));
	if (tlin != *quota_tmp_lin)
		goto out_update_map;

	/* create dir */
	get_tmp_working_dir_name(tmp_name, global->restore);

	/* Open the main Quota dir */
	quota_fd = ifs_lin_open(qlin, HEAD_SNAPID, O_RDWR);
	if (quota_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open quota lin: %{}", lin_fmt(qlin));
		goto out;
	}

	/* Check to see if the quota is in the compliance store. */
	if (global->compliance_dom_ver == 2) {
		is_compliance = is_compliance_store(quota_fd, &error);
		if (error != NULL)
			goto out;

		if (is_compliance) {
			error = isi_system_error_new(EINVAL,
			    "Unsupported quota detected in the "
			    "compliance store for lin %{}",
			    lin_fmt(qlin));
			goto out;
		}
	}

	/* Now make a tmp dir in that quota dir */
	ret = enc_mkdirat(quota_fd, tmp_name, ENC_DEFAULT,
	    0755);
	if (ret < 0) {
		if (errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "Unable to create quota tmp working "
			    "directory for lin %{}", lin_fmt(qlin));
			goto out;
		}
	}

	/* stat the tmpdir to add to the tmp map */
	ret = enc_fstatat(quota_fd, tmp_name, ENC_DEFAULT,
	    &tmp_st, AT_SYMLINK_NOFOLLOW);
	if (ret < 0) {
		if (errno == ENOENT) {
			/*
			 * If the tmp directory gets deleted during lookup,
			 * treat it like the directory doesn't have a quota.
			 */
			log(ERROR, "tmp dir for %{} not found", lin_fmt(qlin));
			quota_tmp_lin = NULL;
		} else {
			error = isi_system_error_new(errno,
			    "Failed stat of tmp dir in "
			    "quota working "
			    "directory: %{} (%s)", lin_fmt(qlin), tmp_name);
		}
		goto out;
	}
	tlin = tmp_st.st_ino;
	set_mapping_cond(qlin, tlin,
	    global->tmp_working_map_ctx, false, *quota_tmp_lin,
	    &error);
	if (error)
		goto out;

out_update_map:
	uint64_uint64_map_add(global->tmp_working_map, qlin, &tlin);
	quota_tmp_lin = uint64_uint64_map_find(global->tmp_working_map,
	    qlin);
	ASSERT(quota_tmp_lin, "%{} missing from tmp map", lin_fmt(qlin));
	ASSERT(*quota_tmp_lin == tlin, "mapping for %{}: %{} != %{}", lin_fmt(qlin),
	    lin_fmt(*quota_tmp_lin), lin_fmt(tlin));
out:
	if (quota_fd != -1) {
		close(quota_fd);
		quota_fd = -1;
	}
	isi_error_handle(error, error_out);
	return quota_tmp_lin;
}

/* Handles the cleanup of a single tmp dir */
static void
cleanup_single_tmp_dir(ifs_lin_t plin, struct migr_sworker_ctx *sw_ctx,
    bool restore, struct isi_error **error_out)
{
	int ret;
	int dir_fd = -1;
	struct isi_error *error = NULL;
	struct migr_tmonitor_ctx *tm_ctx = NULL;
	char tmp_name[MAXNAMELEN + 1];
	struct stat st;

	log(TRACE, "%s plin: %{}", __func__, lin_fmt(plin));

	ASSERT(sw_ctx != NULL);

	/* Failpoint to assist in testing of bug 211599. */
	UFAIL_POINT_CODE(cleanup_single_tmp_dir, {
		*error_out = isi_system_error_new(RETURN_VALUE,
			"Failpoint induced failure in cleanup_single_tmp_dir.");
		goto out;
	});

	tm_ctx = &sw_ctx->tmonitor;

	/* Open the main/Quota dir */
	dir_fd = ifs_lin_open(plin, HEAD_SNAPID, O_RDWR);
	if (dir_fd == -1) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno,
			    "Unable to open tmp dir parent lin: %llx", plin);
		goto out;
	}
	ASSERT (dir_fd > 0);

	/*
	 * Look for copy collision merge entries
	 */
	if (tm_ctx->stf_sync && !tm_ctx->restore) {
		merge_copy_collision_entries(dir_fd, tm_ctx->policy_id, &error);
		if (error != NULL) {
			log(ERROR, "Error while merging contents of "
			    " tmp-working-dir : %s",
			    isi_error_get_message(error));
			goto out;
		}
	}

	/*
	 * Remove the contents of tmp-working-dir
	 */
	get_tmp_working_dir_name(tmp_name, restore);
	log(TRACE, "%s: deleting %s", __func__, tmp_name);
	safe_rmrf(dir_fd, tmp_name, ENC_DEFAULT, tm_ctx->coord_fd,
	    sw_ctx->global.domain_id, sw_ctx->global.domain_generation,
	    &error);
	if (error != NULL) {
		log(ERROR, "Error while removing contents of tmp-working-dir");
		goto out;
	}

	/* Double check and make sure we removed the tmp directory */
	ret = enc_fstatat(dir_fd, tmp_name, ENC_DEFAULT, &st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		log(ERROR, "Unable to check the status of tmp directory");
		goto out;
	} else if (ret == 0) {
		log(FATAL, "Tmp directory still exists after cleanup...");
	}

out:
	if (dir_fd != -1)
		close(dir_fd);

	isi_error_handle(error, error_out);
}

/* Cleans and removes all of the temporary directories below the root_path.
 * This is meant to be called at the end of a sync.
 * This function will free and null the tmp_working_map in the global_ctx */
void
cleanup_tmp_dirs(const char *root_path, bool restore,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct map_iter *iter = NULL;
	int ret = 0;
	ifs_lin_t root_lin, qlin, tlin;
	struct stat root_st;
	bool found;

	log(TRACE, "%s", __func__);

	ASSERT(root_path != NULL);
	ASSERT(sw_ctx != NULL);

	/* Remove root tmp dir */
	ret = stat(root_path, &root_st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Stat of root target dir %s failed", root_path);
		goto out;
	}
	root_lin = root_st.st_ino;
	cleanup_single_tmp_dir(root_st.st_ino, sw_ctx, restore, &error);
	if (error != NULL) {
		goto out;
	}

	UFAIL_POINT_GOTO(skip_cleanup_tmp_dirs, ret, out);
	/* If any quotas were present, they will be in the tmp map */
	if (sw_ctx->global.tmp_working_map_ctx != NULL) {
		/* iterate over linmap */
		iter = new_map_iter(sw_ctx->global.tmp_working_map_ctx);
		ASSERT(iter != NULL);
		found = true;
		while (true) {
			found = map_iter_next(iter, &qlin, &tlin, &error);
			if (error)
				goto out;
			if (!found)
				break;
			if (tlin == INVALID_LIN)
				continue;
			cleanup_single_tmp_dir(qlin, sw_ctx, restore, &error);
			if (error)
				goto out;
		}
		close_map_iter(iter);
		iter = NULL;
	}
out:
	if (sw_ctx->global.tmp_working_map != NULL) {
		uint64_uint64_map_clean(sw_ctx->global.tmp_working_map);
		free(sw_ctx->global.tmp_working_map);
		sw_ctx->global.tmp_working_map = NULL;
	}
	if (sw_ctx->global.tmp_working_map_ctx != NULL) {
		close_linmap(sw_ctx->global.tmp_working_map_ctx);
		sw_ctx->global.tmp_working_map_ctx = NULL;
	}
	if (sw_ctx->global.tmpdir_hash != NULL) {
		tmpdir_hash_free(sw_ctx->global.tmpdir_hash);
		sw_ctx->global.tmpdir_hash = NULL;
	}
	if (iter != NULL)
		close_map_iter(iter);
	sw_ctx->global.quotas_present = false;
	isi_error_handle(error, error_out);
}

/* Works around issue where if you attempt to enc_renameat a file that is the
 * same hardlink on source and target dir, nothing happens. */
void
siq_enc_renameat(int from_dfd, const char *from_name, enc_t from_enc,
    int to_dfd, const char *to_name, enc_t to_enc,
    struct isi_error **error_out)
{
	int ret = -1;
	bool skip = false;
	struct stat from_st = {};
	struct stat to_st = {};
	struct isi_error *error = NULL;

	/* Stat the source file */
	ret = enc_fstatat(from_dfd, from_name, from_enc, &from_st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Failed to stat from fd name: %s (%s)",
		    from_name, enc_type2name(from_enc));
		goto out;
	}

	/* Stat the target file, if it exists */
	ret = enc_fstatat(to_dfd, to_name, to_enc, &to_st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Failed to stat to fd name: %s (%s)",
			    to_name, enc_type2name(to_enc));
			goto out;
		} else
			skip = true;
	}

	//Compare
	if (!skip && !S_ISDIR(from_st.st_mode) &&
	    from_st.st_ino == to_st.st_ino) {
		ret = enc_unlinkat(from_dfd, from_name, from_enc, 0);
		if (ret == -1) {
			error = isi_system_error_new(errno,
			    "Failed to unlink from fd name: %s (%s)",
			    from_name, enc_type2name(from_enc));
			goto out;
		}
	} else {
		ret = enc_renameat(from_dfd, from_name, from_enc, to_dfd,
		    to_name, to_enc);
		if (ret == -1) {
			error = isi_system_error_new(errno,
			    "Failed to rename from fd name: %s (%s) "
			    "to fd name: %s (%s)",
			    from_name, enc_type2name(from_enc),
			    to_name, enc_type2name(to_enc));
			goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}

//Writes out the directory name to a temporary file in the .dlin- directory
//<base64 encoded name>
//<actual name length>
//<encoding>
void
write_treemv_orig_name(const char *dir_name, enc_t dir_enc, int dlin_fd,
    ifs_lin_t flin, struct isi_error **error_out)
{
	char *encoded_name = NULL;
	char buf[32];
	int fd = -1;
	ssize_t wret = -1;
	size_t dir_name_len;
	size_t encode_len;
	struct isi_error *error = NULL;

	ASSERT(dir_name);

	//Open the file, create if not exist, truncate if it does
	fd = enc_openat(dlin_fd, TW_MV_SAVEDDIRNAMEFILE, ENC_DEFAULT,
	    O_CREAT | O_TRUNC | O_RDWR);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open treemv dir name file %s in %{}",
		    TW_MV_SAVEDDIRNAMEFILE, lin_fmt(flin));
		goto out;
	}

	dir_name_len = strlen(dir_name);
	encode_len = 4 * ((dir_name_len + 2) / 3);
	encoded_name = malloc(encode_len + 1);
	ASSERT(encoded_name);

	base64_encode(encoded_name, dir_name, dir_name_len);

	//Write out the name to the file
	wret = write(fd, encoded_name, encode_len);
	if (wret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to write dir_name to treemv %s in %{}",
		    TW_MV_SAVEDDIRNAMEFILE, lin_fmt(flin));
		goto out;
	}

	//Write out the expected length of the decoded name.
	snprintf(buf, sizeof(buf), "\n%zu\n", dir_name_len);
	wret = write(fd, buf, strlen(buf));
	if (wret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to write newline to treemv %s in %{}",
		    TW_MV_SAVEDDIRNAMEFILE, lin_fmt(flin));
		goto out;
	}

	//Write the encoding.
	wret = write(fd, &dir_enc, sizeof(enc_t));
	if (wret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to write encoding to treemv %s in %{}",
		    TW_MV_SAVEDDIRNAMEFILE, lin_fmt(flin));
		goto out;
	}

out:
	free(encoded_name);

	if (fd >= 0) {
		close(fd);
	}
	isi_error_handle(error, error_out);
}

//Reads in the directory name from temporary file in the .tw-tmp directory
void
read_treemv_orig_name(int to_fd, const char *twtmp_name, char **dir_name,
    enc_t *dir_enc, struct isi_error **error_out)
{
	char buffer[MAXNAMELEN * 2 + 1];
	char *decoded_name = NULL;
	char *name = NULL;
	char *token = NULL;
	char *save_ptr = NULL;
	int twtmp_fd = -1;
	int fd = -1;
	ssize_t size = 0;
	size_t decode_len;
	size_t expected_len;
	size_t token_len;
	enc_t enc = ENC_DEFAULT;
	struct isi_error *error = NULL;

	//Open the directory
	twtmp_fd = enc_openat(to_fd, twtmp_name, ENC_DEFAULT, O_RDONLY);
	if (twtmp_fd < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open tmp dir treemv %s", twtmp_name);
		goto out;
	}

	//Open the file to read
	fd = enc_openat(twtmp_fd, TW_MV_SAVEDDIRNAMEFILE, ENC_DEFAULT,
	    O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open treemv %s in %s", TW_MV_SAVEDDIRNAMEFILE,
		    twtmp_name);
		goto out;
	}

	//Read the file to a buffer
	size = read(fd, &buffer, sizeof(buffer));
	if (size <= 0) {
		error = isi_system_error_new(errno,
		    "Failed to read treemv %s in %s", TW_MV_SAVEDDIRNAMEFILE,
		    twtmp_name);
		goto out;
	}

	buffer[size] = '\0';

	//Scan the buffer for a \n and use that to separate out the parts
	token = strtok_r(buffer, "\n", &save_ptr);
	if (!token) {
		error = isi_system_error_new(errno,
		    "Failed to find newline in treemv %s in %s: %s",
		    TW_MV_SAVEDDIRNAMEFILE, twtmp_name, buffer);
		goto out;
	}

	//Everything before \n is the name
	token_len = strlen(token);
	name = malloc(token_len + 1);
	ASSERT(name);
	strlcpy(name, token, token_len + 1);

	//Decode the name
	decode_len = base64_decode_length(name, token_len);
	decoded_name = malloc(decode_len + 1);
	ASSERT(decoded_name);
	base64_decode(decoded_name, name, decode_len + 1);
	decoded_name[decode_len] = '\0';
	free(name);

	//Parse out the expected length
	token = strtok_r(NULL, "\n", &save_ptr);
	if (!token) {
		error = isi_system_error_new(errno,
		    "Failed to find newline in treemv %s in %s: %s",
		    TW_MV_SAVEDDIRNAMEFILE, twtmp_name, save_ptr);
		goto out;
	}

	expected_len = strtoul(token, NULL, 10);
	if (expected_len == 0) {
		error = isi_system_error_new(errno,
		    "Failed to read expected length in treemv %s in %s: %s",
		    TW_MV_SAVEDDIRNAMEFILE, twtmp_name, buffer);
		goto out;

	}

	ASSERT(expected_len == strlen(decoded_name), "%zu != %zu", expected_len,
	    strlen(decoded_name));

	//After the \n is the enc vector
	memcpy(&enc, save_ptr, sizeof(enc_t));

	*dir_name = decoded_name;
	*dir_enc = enc;

out:
	if (fd >= 0)
		close(fd);
	if (twtmp_fd >= 0)
		close(twtmp_fd);

	isi_error_handle(error, error_out);
}
