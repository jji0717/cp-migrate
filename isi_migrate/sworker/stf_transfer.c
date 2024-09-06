#include <sys/acl.h>
#include <sys/isi_enc.h>
#include <sys/module.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/sysctl.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_acl/sd_acl.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_bkup/isi_cpool_bkup.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/siq_error.h"
#include "sworker.h"
#include "isi_domain/cstore_links.h"

/*
 * Function to move an entry between two directories to be merged.
 * On collision make sure we enqueue new work item for a directory.
 */
static bool
move_or_queue_entry(struct migr_dirent *md, struct migr_dir *current_dir,
    int to_fd, struct uint64_uint64_map *lin_hash, struct migr_tw *tw,
    struct isi_error **error_out)
{
	int ret;
	struct isi_error *error = NULL;
	struct stat st = {};
	struct dirent *entryp = NULL;
	int from_fd = current_dir->_fd;
	bool pushed = false;

	entryp = md->dirent;

	/*
	 * First check if the target entry exists. If not we are fine
	 * with rename. If it exists, then check if the target entry 
	 * is directory. If not then we can skip rename. If its directory,
	 * then create a new work item and add it to queue.
	 */
	ret = enc_fstatat(to_fd, entryp->d_name, entryp->d_encoding, &st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to stat entry %s at lin %llx", entryp->d_name,
		    current_dir->_stat.st_ino);
		goto out;
	} else if (!ret) {
		/*
		 * On collision we can skip merge if either
		 * 1) if source entry is not directory.
		 * 2) If target entry is not directory.
		 */
		if (entryp->d_type != DT_DIR || !S_ISDIR(st.st_mode)) {
			goto out;
		}

		uint64_uint64_map_add(lin_hash, entryp->d_fileno, &st.st_ino);
		/* Push onto stack and return (depth first traversal) */
		migr_tw_push(tw, md, &error);
		migr_dir_unref(current_dir, md);
		pushed = true;
		goto out;
	}

	ret = enc_renameat(from_fd, entryp->d_name, entryp->d_encoding,
	    to_fd, entryp->d_name, entryp->d_encoding);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to move lin %llx with entry %s",
		    entryp->d_fileno, entryp->d_name);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return pushed;
}

/*
 * Perform single level directory merge.
 */
static bool
merge_directory(struct migr_tw *tw, struct migr_dir *current_dir,
    struct uint64_uint64_map *lin_hash, struct isi_error **error_out)
{
	struct migr_dirent *md = NULL;
	struct isi_error *error = NULL;
	int to_fd = -1;
	bool dir_completed = false;
	ifs_lin_t from_lin;
	ifs_lin_t *to_linp;
	bool pushed = false;

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
		/*
		 * Move dirent to its right place.
		 * If it fails due to collision, then 
		 * 	for files, skip them.
		 *	for dirs, put it into queue
		 */
		pushed = move_or_queue_entry(md, current_dir, to_fd,
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

void
tree_merge(ifs_lin_t from_root_lin, ifs_lin_t to_root_lin,
    struct isi_error **error_out)
{	
	int root_fd = -1;
	struct migr_tw tw = {};
	struct dir_slice slice;
	bool continue_flag = true;
	struct isi_error *error = NULL;
	struct path path = {};
	struct uint64_uint64_map lin_hash;
	bool dir_completed = false;
	struct migr_dir *current_dir = NULL;
	char *fullpath = NULL;

	slice.begin = DIR_SLICE_MIN;
	slice.end = DIR_SLICE_MAX;

	/* Open from directory lin */
	root_fd = ifs_lin_open(from_root_lin, HEAD_SNAPID, O_RDWR);
	if (root_fd == -1) {
		error = isi_system_error_new(errno, "Failed to open "
		    "lin %llx", from_root_lin);
		goto out;
	}

	/* Now get utf8 path for the lin */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, from_root_lin, HEAD_SNAPID,
	    0, &fullpath, &error);
	if (error)
		goto out;

	path_init(&path, fullpath, NULL);

	/* Instantiate treewalker */
	migr_tw_init(&tw, root_fd, &path, &slice, NULL, false, false, &error);
	if (error)
		goto out;

	root_fd = -1;

	/* Init the hash map and add the first mapping of root lins */
	uint64_uint64_map_init(&lin_hash);
	uint64_uint64_map_add(&lin_hash, from_root_lin, &to_root_lin);

	/* Iterate */
	while (continue_flag) {
		current_dir = migr_tw_get_dir(&tw, &error);
		if (error)
			goto out;

		dir_completed = merge_directory(&tw, current_dir,
		    &lin_hash, &error);
		if (error)
			goto out;

		if (dir_completed) {
			if (strcmp(current_dir->name, "BASE") == 0) {
				continue_flag = false;
			} else {
				continue_flag = migr_tw_pop(&tw, current_dir);
			}
		}
	}

out:
	uint64_uint64_map_clean(&lin_hash);
	migr_tw_clean(&tw);
	path_clean(&path);
	if (fullpath)
		free(fullpath);

	if (root_fd >= 0) {
		close(root_fd);
	}
	isi_error_handle(error, error_out);
}

/*
 * Start a merge of single directory entry
 */
static void
check_and_start_dir_merge(int tmpfd, struct map_ctx *ctx,
    struct migr_dirent *md, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char dirent[MAXNAMELEN + 1];
	char to_hex[MAXNAMELEN + 1];
	char *word, *brkt;
	char *sep = "-";
	uint64_t dirlin, slin, dlin;
	struct stat st;
	int ret;
	bool found;

	log(TRACE, "check_and_start_dir_merge : %s", md->dirent->d_name);
	/* If it does not start with prefix then skip it */
	if (strncmp(COPY_COLLISION_PREFIX, md->dirent->d_name,
		    strlen(COPY_COLLISION_PREFIX)) != 0) {
		goto out;
	}

	/* Get the directory lin numbers */
	strncpy(dirent, md->dirent->d_name, MAXNAMELEN);
	strtok_r(dirent, sep, &brkt);
	word = strtok_r(NULL, sep, &brkt);
	dirlin = atoll(word);
	sprintf(to_hex, "%llx", dirlin);

	/* Now check if the directory exists */
	ret = enc_fstatat(tmpfd, to_hex, ENC_DEFAULT, &st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Error to stat directory entry %s in "
		    "tmp working directory", word);
		goto out;
	} else if (ret == -1) {
		goto out;
	}

	word = strtok_r(NULL, sep, &brkt);
	slin = atoll(word);

	/* Make sure that linmap entry exists for it */
	found = get_mapping(slin, &dlin, ctx, &error);
	if (error || !found)
		goto out;

	log(TRACE, "Starting merge for %llu with %llu", dirlin, dlin);
	/* Now call tree merge function to do actual merge */
	tree_merge(dirlin, dlin, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
	
}

/**
 * The passed in tmpfd is closed by this function.
 */
static void
copy_collision_dir_merge(int *tmpfd, struct map_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *md_dir = NULL;
	struct dir_slice slice;
	struct migr_dirent *md = NULL;

	slice.begin = DIR_SLICE_MIN;
	slice.end = DIR_SLICE_MAX;

	md_dir = migr_dir_new(*tmpfd, &slice, true, &error);
	if (error)
		goto out;
	ASSERT(md_dir != NULL);
	md = migr_dir_read(md_dir, &error);
	if (error)
		goto out;
	while (md != NULL) {
		/*
		 * Check if the dirent is due to copy collision.
		 * Those start with copy_collision*
		 */
		check_and_start_dir_merge(*tmpfd, ctx, md, &error);
		if (error)
			goto out;

		/* Load next migr_dirent */
		migr_dir_unref(md_dir, md);
		md = migr_dir_read(md_dir, &error);
		if (error)
			goto out;
	}
out:
	if (md_dir != NULL) {
		migr_dir_free(md_dir);
		*tmpfd = -1;
		md_dir = NULL;
	}
	isi_error_handle(error, error_out);
}

void
merge_copy_collision_entries(int rootfd, char *policy_id,
    struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	int tmpfd = -1;
	struct map_ctx *ctx = NULL;
	char tmp_name[MAXNAMELEN + 1];
	char lmap_name[MAXNAMELEN + 1];
	struct stat st;
	int ret;
	ifs_lin_t i;
	ifs_lin_t mask = (TMPDIR_MASK) + 1;
	char *hashdir = NULL;
	int hash_tmpfd = -1;

	/*
	 * First check and open the tmp directory
	 */
	get_tmp_working_dir_name(tmp_name, false);
	/* Now check if the directory exists */
	ret = enc_fstatat(rootfd, tmp_name, ENC_DEFAULT, &st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Error to stat tmp working directory");
		goto out;
	} else if (ret == -1) {
		goto out;
	}

	tmpfd = ifs_lin_open(st.st_ino, HEAD_SNAPID, O_RDONLY);
	if (tmpfd < 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Error opening tmp working directory %{}",
		    lin_fmt(st.st_ino));
		goto out;
	} else if (tmpfd == -1) {
		goto out;
	}

	/* Now open the linmap context for policy */
	get_lmap_name(lmap_name, policy_id, false);
	/* make sure a linmap exists */
	open_linmap(policy_id, lmap_name, &ctx, &error);
	if (error)
		goto out;

	/* Bug 215632 - iterate over the root tmp dir and tmp hash dirs */

	for (i = 0; i < mask; i++) {
		get_hash_dirname(i, &hashdir);
		hash_tmpfd = enc_openat(tmpfd, hashdir, ENC_DEFAULT,
		    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
		if (hash_tmpfd < 0 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Error opening hash tmp working directory");
			goto out;
		}

		free(hashdir);
		hashdir = NULL;

		if (hash_tmpfd == -1)
			continue;

		copy_collision_dir_merge(&hash_tmpfd, ctx, &error);
		if (error)
			goto out;
		if (hash_tmpfd != -1) {
			close(hash_tmpfd);
			hash_tmpfd = -1;
		}
	}

	copy_collision_dir_merge(&tmpfd, ctx, &error);
	if (error != NULL)
		goto out;

out:
	close_linmap(ctx);

	if (hash_tmpfd != -1)
		close(hash_tmpfd);
	if (tmpfd > 0)
		close(tmpfd);
	free(hashdir);
	isi_error_handle(error, error_out);
}

/*
 * We make a note of collision directory here , format is
 * 
 * copy_collision-<collision directory lin> - <new dir slin>
 *
 * Note that we use slin(source lin) for new directory.
 * This is to always make sure we reference the right 
 * target directory lin (using linmap) since the directory
 * creation/mvs is racy in SyncIQ.
 */
static void
checkpoint_collision_directory(int tmpdirfd, uint64_t collision_lin,
    uint64_t slin, struct isi_error **error_out)
{
	char entry[MAXNAMELEN + 1];
	struct isi_error *error = NULL;
	int fd = -1;
	struct stat st;
	int ret;

	ASSERT (tmpdirfd > 0);

	/* Construct the dirent name in tmp working directory */
	sprintf(entry, "%s-%llu-%llu", COPY_COLLISION_PREFIX, collision_lin,
	    slin);

	fd = enc_openat(tmpdirfd, entry, ENC_DEFAULT,
	    O_RDWR|O_CREAT|O_NOFOLLOW, 0750);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to create file %s in tmp directory", entry);
		goto out;
	}

	log(TRACE, "checkpointed collision %s", entry);
	/*
	 * Paranoia test to make sure we opened a normal file
	 */
	ret = fstat(fd, &st);
	if (ret == -1 || !S_ISREG(st.st_mode)) {
		error = isi_system_error_new(errno,
		    "Failed to stat file %s in tmp directory", entry);
		goto out;
	}

out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
}

/*
 * Function emulates the move operation by performing
 * a explicit link and unlink operation.
 */
static bool
compliance_file_move(int dirfd, char *d_name, unsigned d_enc, int tmpfd,
    char *tmp_entry, struct stat *st, struct isi_error **error_out)
{

	int ret = -1;
	struct isi_error *error = NULL;
	bool moved = false;
	bool have_worm = false;
	bool is_compliance = false;

	ASSERT(st != NULL);

	log(DEBUG, "%s lin %{}", __func__, lin_fmt(st->st_ino));

	/*
	 * Check if its committed, if not then regular move operation should
	 * have worked.
	 */
	have_worm = get_worm_state(st->st_ino, HEAD_SNAPID, NULL,
	    &is_compliance, &error);
	if (error)
		goto out;

	if (!have_worm || !is_compliance)
		goto out;

	/* Perform explicit link and unlink calls. */
	ret = enc_lin_linkat(st->st_ino, tmpfd, tmp_entry, d_enc);
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Unable to link lin %{} with entry name %s at "
		    "tmp working directory", lin_fmt(st->st_ino), tmp_entry);
		goto out;
	}

	/* Perform the unlink call */
	ret = enc_unlinkat(dirfd, d_name, d_enc, 0);
	if (ret == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to unlink lin  %{} with entry name %s",
		    lin_fmt(st->st_ino), d_name);
		goto out;
	}

	moved = true;

out:
	isi_error_handle(error, error_out);
	return moved;
}

/*
 * Move dirent to tmp working directory
 * The format of dirent name in tmp directory is
 * /tmp-working-dir/1024 where
 * direntlin = 1024
 */
static bool
move_to_tmpdir(
    int dirfd,
    uint64_t d_fileno,
    char *d_name,
    unsigned d_enc,
    int tmpfd,
    struct isi_error **error_out)
{
	char entry[MAXNAMELEN + 1];
	int ret;
	struct isi_error *error = NULL;
	char *path = NULL, *tmp_path = NULL;
	int tmp_errno;
	bool moved = false;
	struct stat st = {};

	log(TRACE, "%s: dlin %{}", __func__, lin_fmt(d_fileno));

	ASSERT (dirfd > 0);
	ASSERT (tmpfd > 0);

	/* Construct the dirent name in tmp working directory */
	snprintf(entry, sizeof(entry), "%llx", d_fileno);

	ret = enc_fstatat(dirfd, d_name, d_enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		tmp_errno = errno;
		path = get_valid_rep_path(d_fileno, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Failed to stat %s", path);
		free(path);
		goto out;
	}

	ret = enc_renameat(dirfd, d_name, d_enc, tmpfd, entry, ENC_DEFAULT);
	if (ret == -1 && errno == EROFS && !S_ISDIR(st.st_mode)) {
		tmp_errno = errno;
		/* Try compliance move operation */
		moved = compliance_file_move(dirfd, d_name, d_enc,
		    tmpfd, entry, &st, &error);
		if (error || moved)
			goto out;
		errno = tmp_errno;
	}

	if (ret < 0 && errno != ENOENT) {
		tmp_errno = errno;
		path = get_valid_rep_path(d_fileno, HEAD_SNAPID);
		ret = fstat(tmpfd, &st);
		if (ret < 0) {
			error = isi_system_error_new(tmp_errno,
			    "Failed to move %s from %s to tmp working "
			    "directory. tmpfd stat failed.",
			    d_name, path);
		} else {
			tmp_path = get_valid_rep_path(st.st_ino, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Failed to move %s (lin %{}) from %s to tmp working"
			    " directory %s (lin %{})",
			    d_name, lin_fmt(d_fileno), path, tmp_path,
			    lin_fmt(st.st_ino));
			free(tmp_path);
		}
		ret = -1;
		free(path);
		goto out;
	} else if (!ret) {
		moved = true;
	}

out:
	isi_error_handle(error, error_out);
	return moved;
}

/*
 * Given a lin, make sure a canonical link exists in the compliance store
 * for each parent of the lin and if it doesnt exist, create one.
 */
static void
create_cstore_links_before_unlink(ifs_lin_t lin, int primary_fd,
    struct isi_error **error_out)
{
	struct parent_entry *pentries = NULL;
	uint32_t num_entries = 0;
	int i;
	struct isi_error *error = NULL;

	get_lin_parents(lin, &pentries, &num_entries, &error);
	if (error)
		goto out;

	for (i = 0; i < num_entries; i++) {
		create_cstore_links(lin, &pentries[i], 1, &error);
		if (error)
			goto out;

		/*
		 * Since the sworker has to go through each parent of the
		 * lin to create the canonical links, this might be a lengthy
		 * operation and hence we make sure that the connection
		 * with the corresponding pworker is alive. If not, we
		 * stop the current sworker since the work item will be
		 * taken over by another sworker later on.
		 */
		check_connection_with_peer(primary_fd, true, 1, &error);
		if (error)
			goto out;
	}

out:
	/* Free memory allocated by get_lin_parents() */
	if (pentries)
		free(pentries);

	isi_error_handle(error, error_out);
}

/*
 * If the nlink = 1 or dirent is directory, then move it to tmp working dir.
 * Else remove the link from the parentfd. If the conditional unlink fails,
 * then try again to remove dirent.
 * Returns true if entry is removed.
 */
bool
move_or_remove_dirent(struct migr_sworker_ctx *sw_ctx, int dirfd, int tmpfd,
    struct stat *st, char *d_name, unsigned d_enc,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *path;
	int tmp_errno;
	int ret, tfd = -1;
	ifs_lin_t direntlin;
	bool unlinked = false;
	bool committed = false;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	log(TRACE, "%s", __func__);

	ASSERT(st != NULL);
	direntlin = st->st_ino;

	if (st->st_flags & IMMUT_APP_FLAG_MASK) {
		clear_flags(st, IMMUT_APP_FLAG_MASK, d_name, d_enc,
		    &error);
		if (error) {
			/* Bug 196399 - ignore ENOENT */
			if (isi_system_error_is_a(error, ENOENT)) {
				log(DEBUG,
				    "%s: skipping ENOENT error for %llx",
				    __func__, st->st_ino);
				isi_error_free(error);
				error = NULL;
			}
			goto out;
		}
	}

	UFAIL_POINT_CODE(log_conflict_on_move,
		if (has_comp_conflict_log()) {
			log_comp_conflict("fake/move/path", "faker/move/path",
			    &error);
			if (error)
				goto out;
		}
	);

	/*
	 * If its compliance sync, and the file is committed, then
	 * keep track of all the conflicts.
	 */
	if (global->compliance_v2 && global->restore) {
		committed = is_worm_committed(direntlin, HEAD_SNAPID, &error);
		if (error)
			goto out;
		if (committed) {
			/*
			 * If the file is committed, make sure that it's
			 * canonical link on the compliance store exists.
			 * If not, create one.
			 */
			create_cstore_links_before_unlink(direntlin,
			    global->primary, &error);
			if (error)
				goto out;
		
			log_conflict_path(dirfd, d_name, d_enc, &error);
			if (error)
				goto out;
		}
	}

again:
	/* There is valid dirent entry, so go ahead with move/unlink*/
	if (st->st_nlink == 1 || S_ISDIR(st->st_mode)) {
		unlinked = move_to_tmpdir(dirfd, st->st_ino, d_name,
		    d_enc, tmpfd, &error);
		if (error)
			goto out;
	} else {
		/*
		 * Bug 200136
		 * only allow one sworker at a time to unlink a multiply linked
		 * file
		 */
		if (tfd == -1) {
			tfd = ifs_lin_open(direntlin, HEAD_SNAPID,
			    O_WRONLY|O_EXLOCK);
			if (tfd == -1) {
				if (errno != ENOENT)
					error = isi_system_error_new(errno,
					    "ifs_lin_open failed for %llx",
					    direntlin);
				goto out;
			}
		}
		ret = enc_cond_unlinkat(dirfd, d_name,
		    d_enc, st->st_ino, IFS_COND_UNLINK_MULTILINK);
		if (ret != 0 && errno == ERANGE) {

			/*
			 * Do stat and check that Lin matches.
			 * If so try again.
			 */
			ret = enc_fstatat(dirfd, d_name, d_enc, st,
			    AT_SYMLINK_NOFOLLOW);
			if (ret != 0) {
				tmp_errno = errno;
				path = get_valid_rep_path(
				    st->st_ino, HEAD_SNAPID);
				error = isi_system_error_new(tmp_errno,
				    "stat of %s failed", path);
				free(path);
				goto out;
			}

			/*
			 * We cant have lin mismatch since for a particular
			 * directory , only one thread sends link and unlink
			 * operation at any time.
			 */
			ASSERT(st->st_ino == direntlin,
			    "Mismatched lins %llx %llx", st->st_ino,
			    direntlin);
			close(tfd);
			tfd = -1;
			/* Lets try to move/remove dirent again */
			goto again;
		} else if (ret != 0 && errno != ENOENT) {
			tmp_errno = errno;
			path = get_valid_rep_path(
			    st->st_ino, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "unlink of %s failed", path);
			free(path);
			goto out;
		} else if (ret == 0) {
			unlinked = true;
		}
	}

	if (unlinked && committed)
		sw_ctx->stats.stf_cur_stats->compliance->conflicts += 1;

out:
	if (tfd != -1)
		close(tfd);
	isi_error_handle(error, error_out);
	return unlinked;
}


/*
 *	Move dirents out of current directory 
 *	Scan all dirents in directory,
 *	If the dirent id directory or file with nlinks=1, then
 *		move dirent to tmp working directory
 *	Else
 *		unlink the dirent
 *	Returns number of dirents removed in the move process.
 */
void
move_dirents(struct migr_sworker_ctx *sw_ctx, int dirfd,
    int *dir_unlinks, int *file_unlinks, struct isi_error **error_out)
{
	int i;
	int ret;
	int tmpfd = -1;
	bool unlinked = false;
	struct dirent *entryp;
	struct isi_error *error = NULL;
	unsigned int st_count  = READ_DIRENTS_NUM;
	uint64_t cookie_out[READ_DIRENTS_NUM],
	    resume_cookie = READ_DIRENTS_MIN_COOKIE;
	char dirents[READ_DIRENTS_SIZE];
	struct stat  stats[READ_DIRENTS_NUM];
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct stat parent_st;

	log(TRACE, "%s", __func__);

	ASSERT (dirfd > 0);
	ASSERT (dir_unlinks != NULL);
	ASSERT (file_unlinks != NULL);
	*dir_unlinks  = 0;
	*file_unlinks = 0;

	do  {
		/*
		 * Bug 144299
		 * reset st_count
		 */
		st_count = READ_DIRENTS_NUM;
		ret = readdirplus(dirfd, RDP_NOTRANS, &resume_cookie, 
		    READ_DIRENTS_SIZE, dirents,
		    READ_DIRENTS_SIZE, &st_count, stats, cookie_out);

		if (ret == 0)
			break;

		/* We are not getting required information, fail */
		if (ret < 0) {
			error = isi_system_error_new(errno,
			    "readdirplus failure");
			break;
		}

		/*
		 * Bug 144299
		 * if ret > 0 but st_count == 0, then something went wrong
		 */
		ASSERT(st_count > 0);

		entryp = (struct dirent *) dirents;

		for (i = 0; i < st_count ; i++,
		    entryp = (struct dirent*) ((char*) entryp + 
		    entryp->d_reclen))
		{
			if (entryp->d_fileno == 0)
				continue;

			/* if do_stat failed in readdirplus,
			 * replace st_ino with d_fileno to preserve the lin
			 * info in case of a later failure */
			if (stats[i].st_ino == 0)
				stats[i].st_ino = entryp->d_fileno;

			/*
			 * For compliance syncs, we do not support
			 * mv operation for directories(from client side).
			 * Hence the only case is that this directory needs
			 * to be deleted.
			 */
			if (global->compliance_v2 && S_ISDIR(stats[i].st_mode)) {
				if (sw_ctx->comp.rt != WORK_CT_COMP_DIR_DELS) {
					//Add parent to comp dir del lin list
					ret = fstat(dirfd, &parent_st);
					if (ret < 0) {
						error = isi_system_error_new(
						    errno, "fstat on parent dir");
						goto out;
					}
					add_worm_dir_del(sw_ctx,
					    parent_st.st_ino, &error);
					log(DEBUG, "%s: adding dir %llx to "
					    "worm_dir_del", __func__,
					    parent_st.st_ino);
					if (error)
						goto out;
					sw_ctx->comp.curr_dir_delayed_delete =
					    true;
				} else {
					log(DEBUG, "%s: executing safe_rmrf "
					"on %llx", __func__, stats[i].st_ino);
					safe_rmrf(dirfd, entryp->d_name,
						entryp->d_encoding,
						global->primary,
						global->domain_id,
						global->domain_generation,
						&error);
					if (error)
						goto out;
					(*dir_unlinks)++;
				}
				continue;
			}

			if (tmpfd == -1) {
				tmpfd = get_tmp_fd(entryp->d_fileno,
				    &sw_ctx->global, &error);
				if (error != NULL)
					goto out;
				ASSERT(tmpfd != -1);
			}
			unlinked = move_or_remove_dirent(sw_ctx, dirfd, tmpfd,
			    &stats[i], entryp->d_name,
			    entryp->d_encoding, &error);
			if (error)
				goto out;
			/* Bug 2370964 - Need to close fd if opened
			 * for a hardlink
			 */
			if (sw_ctx->global.tmpdir_hash != NULL ||
			    (global->quotas_present &&
			     !S_ISDIR(stats[i].st_mode) &&
			     stats[i].st_nlink > 1)) {
				close(tmpfd);
				tmpfd = -1;
			}
			if (unlinked) {
				if (S_ISDIR(stats[i].st_mode))
					(*dir_unlinks)++;
				else
					(*file_unlinks)++;
			}
		}
		/* XXX: Do we have to increment this?? */
		resume_cookie = cookie_out[st_count - 1];

	} while(1);

out:
	if (tmpfd != -1)
		close(tmpfd);
	isi_error_handle(error, error_out);
}

void
delete_lin(struct migr_sworker_ctx *sw_ctx, uint64_t dlin,
    struct isi_error **error_out)
{
	int fd = -1;
	int ret;
	struct stat st;
	struct isi_error *error = NULL;
	char *path = NULL;
	int tmp_errno;
	int file_unlinks;
	int dir_unlinks;
	bool committed = false;
	ifs_lin_t *quota_tmp_lin = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	log(TRACE, "%s: dlin %{}", __func__, lin_fmt(dlin));

	ASSERT(dlin > 0);

	fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
	if (fd == -1 && errno != ENOENT) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "ifs_lin_open failed for %s", path);
		free(path);
		goto out;
	}
	/* The lin is already deleted before we could remove its lmap entry */
	if (fd == -1)
		goto out;

	ASSERT(fd != 0);

	ret = fstat(fd, &st);
	if (ret < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "fstat operation failed for %s", path);
		free(path);
		goto out;
	}

	UFAIL_POINT_CODE(delete_lin_unlink,
		remove_all_parent_dirents(fd, &st, global->restore, &error);
		if (error)
			goto out;
		if (RETURN_VALUE) {
			ret = fstat(fd, &st);
			if (ret < 0) {
				error = isi_system_error_new(errno,
				    "fstat failed %llx", dlin);
				goto out;
			}
		});

	/*
	 * Bug 144299
	 * exit if dlin has already been unlinked
	 */
	if (st.st_nlink == 0)
		goto out;

	if (st.st_flags & IMMUT_APP_FLAG_MASK) {
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK, fd, NULL, &error);
		if (error)
			goto out;
	}

	if (S_ISDIR(st.st_mode)) {
		if (sw_ctx->global.quotas_present) {
			/* If this dir lin exists as a key in the
			 * tmp_working_map table, then it has a quota on it,
			 * this must be deleted in order to continue the sync */
			quota_tmp_lin = tmp_map_find(&sw_ctx->global, dlin,
			    &error);
			if (error)
				goto out;
			if (quota_tmp_lin != NULL) {
				check_quota_delete(dlin, &error);
				if (error)
					goto out;
			}
		}

		if (global->compliance_v2) {
			/* Clear the delayed delete flag so that move_dirents
			 * can set it if needed */
			sw_ctx->comp.curr_dir_delayed_delete = false;
		}
		move_dirents(sw_ctx, fd, &dir_unlinks, &file_unlinks, &error);
		if (error)
			goto out;
		sw_ctx->stats.stf_cur_stats->files->unlinked->dst +=
		    file_unlinks;
		sw_ctx->stats.stf_cur_stats->dirs->unlinked->dst +=
		    dir_unlinks;
	}

	/*
	 * If its compliance sync, and the file is committed, then
	 * keep track of all the conflicts.
	 */
	if (global->compliance_v2 && global->restore) {
		committed = is_worm_committed(dlin, HEAD_SNAPID, &error);
		if (error)
			goto out;
		/*
		 * If the file is committed, make sure that it's canonical
		 * link on the compliance store exists. If not, create one.
		 */
		if (committed) {
			create_cstore_links_before_unlink(dlin,
			    global->primary, &error);
			if (error)
				goto out;
		}
	}

	/* If we detected that we had to delay a child directory delete in
	 * compliance mode, skip deleting the parent for now */
	if (!(global->compliance_v2 && sw_ctx->comp.curr_dir_delayed_delete)) {
		file_unlinks = remove_all_parent_dirents(fd, &st, global->restore,
		    &error);
		if (error)
			goto out;
	}

	if (S_ISDIR(st.st_mode)) {
		sw_ctx->stats.stf_cur_stats->dirs->deleted->dst++;
		sw_ctx->stats.stf_cur_stats->dirs->unlinked->dst++;
	} else {
		sw_ctx->stats.stf_cur_stats->files->deleted->dst++;
		sw_ctx->stats.stf_cur_stats->files->unlinked->dst +=
		    file_unlinks;
		if (committed) {
			sw_ctx->stats.stf_cur_stats->compliance->conflicts +=
			    file_unlinks;
		}
	}

out:
	if (fd > 0)
		close(fd);

	isi_error_handle(error, error_out);
}

#define MAX_LINK_RETRIES 300
#define MAX_TREEMV_RETRIES MAX_LINK_RETRIES
void
link_lin(
    struct migr_sworker_ctx *sw_ctx,
    uint64_t dlin,
    uint64_t direntlin,
    uint64_t direntslin,
    char *d_name,
    unsigned d_enc,
    unsigned d_type,
    struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct stat st;
	struct isi_error *error = NULL;
	struct dirent entry;
	bool found;
	uint64_t parentlin = 0;
	int dirfd = -1, direntfd = -1, pfd = -1, ret = -1, tmp_fd = -1;
	struct parent_entry pe;
	unsigned num_entries = 1;
	bool dolink = false;
	char *path = NULL;
	char *secondary_path = NULL;
	int tmp_errno;
	bool unlinked = false;
	bool crossed = false;
	bool parentlin_is_tmp = false;
	bool linkat = false;
	int retries = 0;
	int treemv_retries = 0;

	ASSERT(dlin > 0);
	ASSERT(direntlin > 0);
	dirfd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
	if (dirfd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    " ifs_lin_open failed for %s", path);
		free(path);
		goto out;
	}
	ASSERT(dirfd != 0);

	ret = enc_fstatat(dirfd, d_name, d_enc, &st, AT_SYMLINK_NOFOLLOW);

	if (ret == -1 && errno == ENOENT) {
		dolink = true;
	} else if (ret == -1) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat entry %s in dir %s", d_name,
		    path);
		free(path);
		goto out;
	}

	if (ret == 0) {
		if (direntlin != st.st_ino) {
			/*
			 * If we hit this then we are sure that these
			 * files/dirs are lying due to copy job or we got link 
			 * operation for same dirent before unlink. just
			 * move them to tmp folder to be picked up later or
			 * to be cleaned up at the end of job.
			 */
			tmp_fd = get_tmp_fd(st.st_ino, global, &error);
			if (error)
				goto out;

			if (global->action == SIQ_ACT_REPLICATE &&
			    S_ISDIR(st.st_mode) && d_type == DT_DIR) {
				checkpoint_collision_directory(tmp_fd,
				    st.st_ino, direntslin, &error);
				if (error)
					goto out;
			}

			unlinked = move_or_remove_dirent(sw_ctx, dirfd, tmp_fd,
			    &st, d_name, d_enc, &error);
			if (error)
				goto out;
			if (unlinked) {	
				if (S_ISDIR(st.st_mode)) {
					sw_ctx->stats.stf_cur_stats->dirs->
					    unlinked->dst++;
				} else {
					sw_ctx->stats.stf_cur_stats->files->
					    unlinked->dst++;
				}
			}
			dolink = true;
		}
	}

	if (!dolink)
		goto out;

again:
	/* Get the file descriptors  to handle rename */
	direntfd = ifs_lin_open(direntlin, HEAD_SNAPID,
	    O_RDWR);
	if (direntfd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(direntlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to open %s", path);
		free(path);
		goto out;
	}
	ASSERT(direntfd != 0);

	ret = fstat(direntfd, &st);
	if (ret != 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(direntlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat %s", path);
		free(path);
		goto out;
	}
	if (!S_ISDIR(st.st_mode)) {
		ret = pctl2_lin_get_parents(direntlin, HEAD_SNAPID,
		    &pe, 1, &num_entries);
		if (ret == -1 && errno != ENOSPC) {
			tmp_errno = errno;
			path = get_valid_rep_path(direntlin,
			    HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Failed to get parent lin for %s", path);
			free(path);
			goto out;
		}
		if (ret == -1 && errno == ENOSPC) {
			ASSERT(num_entries > 1, "bad numentries %d",
			    num_entries);
		}
	}

	if (st.st_flags & IMMUT_APP_FLAG_MASK) {
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK, direntfd,
		    d_name, &error);
		if (error)
			goto out;
	}

	if (!S_ISDIR(st.st_mode)) {
		if (num_entries > 1)
			linkat = true;
		else {
			linkat = !is_tmp_lin(pe.lin, global, &error);
			if (error)
				goto out;
		}

	}
	if (linkat) {
		ret = enc_lin_linkat(direntlin, dirfd, d_name, d_enc);
		if (ret != 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(direntlin,
			    HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Unable to link lin %llx: %s", direntlin,
			    strerror(errno));
			free(path);
			goto out;
		}
		sw_ctx->stats.stf_cur_stats->files->linked->dst++;
	} else {
		/*
		 * We need to find exisiting parent of this directory.
		 */
		found = dirent_lookup_by_lin(HEAD_SNAPID, direntlin, 0,
		    &parentlin, &entry, &error);
		/* The dirent lookup should not fail here */
		if (error)
			goto out;

		parentlin_is_tmp = is_tmp_lin(parentlin, global, &error);
		if (error)
			goto out;

		if (!found ||
		    (!S_ISDIR(st.st_mode) &&
		    (!parentlin_is_tmp))) {
			/*
			 * It is possible that the dirent was
			 * moved to tmp or moved out. Retry.
			 */
			close(direntfd);
			direntfd = -1;
			path = get_valid_rep_path(direntlin,
			    HEAD_SNAPID);
			log(ERROR, "Retrying dirent lookup for %s",
			    path);
			free(path);
			goto again;
		}

		pfd = ifs_lin_open(parentlin, HEAD_SNAPID, O_RDWR);
		if (pfd < 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(parentlin,
			    HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Unable to open %s", path);
			free(path);
			goto out;
		}
		ASSERT(pfd != 0);

		if (0 == strncmp(entry.d_name, TW_MV_TOPTMPNAME,
		    strlen(TW_MV_TOPTMPNAME))) {
			/* This is a tree-move in progress */
			path = get_valid_rep_path(direntlin, HEAD_SNAPID);
			secondary_path = get_valid_rep_path(dlin,
				HEAD_SNAPID);
			log(INFO,"In-progress tree move detected, "
			"continuing tree move of %s to %s.",
			path, secondary_path);
			free(path);
			free(secondary_path);
			treemv_restart(sw_ctx, parentlin, direntlin, dlin,
			    d_enc, d_name, entry.d_name, false, &error);
			goto out;
		}

		ret = enc_renameat(pfd, entry.d_name, entry.d_encoding,
		    dirfd, d_name, d_enc);
		if (ret < 0) {
			if (errno == EINVAL &&
			    S_ISDIR(st.st_mode)) {
				error = isi_siq_error_new(
				    E_SIQ_LINK_EXCPT, "Link exception");
				goto out;
			} else if (errno == ENOENT &&
			    retries < MAX_LINK_RETRIES) {
				retries++;
				close(pfd);
				close(direntfd);
				pfd = -1;
				direntfd = -1;
				/*
				 * Its possible that dirent could be
				 * moved to tmp working directory,
				 * so try again.
				 */
				goto again;
			} else if (errno == EPERM &&
				   global->quotas_present) {
				/*
				 * Quotas are likely the reason
				 * for this failure, check if
				 * there is one to worry about.
				 */
				UFAIL_POINT_CODE(sleep_link_lin,
					if (RETURN_VALUE > 0) {
						siq_nanosleep(RETURN_VALUE,0);
					}
				);
				crossed = crossed_quota(dlin, direntlin,
				    &error);
				if (error)
					goto out;

				if (crossed) {
					log(INFO,"Directory move in quota "
					    "detected, using a tree "
					    "move to move %s (%s) (%llx, "
					    "slin: %llx) from %llx to %llx.",
					    d_name, enc_get_name(d_enc),
					    direntlin, direntslin, parentlin,
					    dlin);
					treemv(sw_ctx, parentlin, pfd,
					    direntlin, dirfd,
					    d_enc, d_name, entry.d_name,
					    &entry, false, &error);

					if (error) {
						/* Bug 206186 - retry ENOENT */
						if (isi_system_error_is_a(
						    error, ENOENT) &&
						    treemv_retries < MAX_TREEMV_RETRIES) {
							siq_nanosleep(treemv_retries % 5 + 1, 0);
							treemv_retries++;
							log(ERROR,
							    isi_error_get_message(
							    error));
							isi_error_free(error);
							error = NULL;
							close(pfd);
							close(direntfd);
							pfd = -1;
							direntfd = -1;
							goto again;
						}
						goto out;
					}
				} else {
					if (retries < MAX_LINK_RETRIES) {
						close(pfd);
						close(direntfd);
						pfd = -1;
						direntfd = -1;
						log(INFO, "Possibly complete treemv "
							"detected; retrying rename of "
							"%s (%s) (%llx, slin: %llx) from "
							"%llx to %llx.",
							d_name, enc_get_name(d_enc),
							direntlin, direntslin, parentlin,
							dlin);
						retries++;
						goto again;
					}
					else {
						error = isi_system_error_new(
							EPERM, "Unable to rename "
							"entry %s (%s) (%llx, slin: %llx) "
							"from %llx to %llx",
							d_name,  enc_get_name(d_enc), direntlin,
							direntslin, parentlin, dlin);
						free(secondary_path);
						free(path);
						goto out;
					}
				}
			} else {
				tmp_errno = errno;
				path = get_valid_rep_path(direntlin,
				    HEAD_SNAPID);
				secondary_path = get_valid_rep_path(
				    parentlin, HEAD_SNAPID);
				error = isi_system_error_new(tmp_errno,
				    "Unable to rename entry %s from %s "
				    "to %s", d_name, secondary_path,
				    path);
				free(secondary_path);
				free(path);
				goto out;
			}
		}

		if (S_ISDIR(st.st_mode)) {
			if (!parentlin_is_tmp)
				sw_ctx->stats.stf_cur_stats->dirs->
				    unlinked->dst++;
			sw_ctx->stats.stf_cur_stats->dirs->linked->dst++;
		} else {
			sw_ctx->stats.stf_cur_stats->files->linked->dst++;
		}
	}

out:
	if (tmp_fd > 0)
		close(tmp_fd);

	if (dirfd > 0)
		close(dirfd);

	if (direntfd > 0)
		close(direntfd);

	if (pfd > 0)
		close(pfd);

	isi_error_handle(error, error_out);
}
#undef MAX_LINK_RETRIES
#undef MAX_TREEMV_RETRIES

void
unlink_lin(struct migr_sworker_ctx *sw_ctx, uint64_t dlin, uint64_t direntlin,
    char *d_name, unsigned d_enc, unsigned d_type, struct isi_error **error_out)
{
	int dirfd = -1, tmpfd = -1, ret = -1;
	struct stat st;
	struct isi_error *error = NULL;
	char *path = NULL;
	int tmp_errno;
	bool unlinked = false;

	ASSERT(dlin > 0);
	ASSERT(direntlin > 0);

	dirfd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
	if (dirfd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to open %s", path);
		free(path);
		goto out;
	}
	ASSERT(dirfd > 0);

	ret = enc_fstatat(dirfd, d_name, d_enc,
	    &st, AT_SYMLINK_NOFOLLOW);

	if (ret == -1 && errno == ENOENT) {
		goto out;
	} else if (ret == -1) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat entry %s in dir %s", d_name, path);
		free(path);
		goto out;
	}

	/*
	 * Someone did unlink of old lin and link for new lin,
	 * we can skip unlink.
	 */
	if (direntlin != st.st_ino)
		goto out;

	tmpfd = get_tmp_fd(st.st_ino, &sw_ctx->global, &error);
	if (error)
		goto out;

	unlinked = move_or_remove_dirent(sw_ctx, dirfd, tmpfd,
	    &st, d_name, d_enc, &error);
	if (error)
		goto out;

	if (unlinked) {
		if (S_ISDIR(st.st_mode)) {
			sw_ctx->stats.stf_cur_stats->dirs->unlinked->dst++;
		} else {
			sw_ctx->stats.stf_cur_stats->files->unlinked->dst++;
		}
	}		
out:
	if (dirfd > 0)
		close(dirfd);
	if (tmpfd > 0)
		close(tmpfd);

	isi_error_handle(error, error_out);
}

static int
open_or_create_dir(struct migr_sworker_ctx *sw_ctx, int dirfd, char *dname,
    enc_t enc, mode_t mode, ifs_lin_t *lin_out, struct isi_error **error_out)
{
	int fd = -1;
	int ret;
	struct stat st;
	struct isi_error *error = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	ret = enc_mkdirat(dirfd, dname, enc, mode);
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Unable to create directory %s", dname);
		goto out;
	}
	fd = enc_openat(dirfd, dname, enc,
	    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to create dir %s", dname);
		goto out;
	}

	ASSERT(fd != 0);
	ret = fstat(fd, &st);
	if (ret == -1 || !S_ISDIR(st.st_mode)) {
		error = isi_system_error_new(errno,
		    "Failed to open dir %s/%s",
		    sw_ctx->worker.cur_path.path, dname);
		close(fd);
		fd = -1;
		goto out;
	}
	if (lin_out)
		*lin_out = st.st_ino;

	/*
	 * For compliance mode, make sure we add directory
	 * entry for regular incremental syncs.
	 */
	if (global->compliance_v2 && !global->restore) {
		add_peer_lin_to_resync(fd, global->resync_ctx, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
	return fd;
}

void
reuse_old_lin(struct migr_sworker_ctx *sw_ctx, struct link_msg *lm,
    struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	int dir_fd = -1, tmp_fd = -1;
	int ret;
	int tmp_errno;
	int unlinked = 0;
	bool dolink = false;
	char *path = NULL;
	struct stat st;
	ifs_lin_t plin = 0;
	struct isi_error *error = NULL;
	int mode = dtype_to_ifstype(lm->d_type) | 0755;
	int dev = 0;
	uint32_t major = 0, minor = 0;
	struct timeval now = { 0 };
	struct timeval deadline = { 0 };
	bool tcmp = false;
	bool found = false;
	bool plin_is_tmp;
	bool dirlin_is_tmp;

	/* Always make sure directory lin exists */
	dir_fd = ifs_lin_open(lm->dirlin, HEAD_SNAPID, O_RDWR);
	if (dir_fd < 0) {
		error = isi_system_error_new(errno, "Unable to open lin %llx",
		    lm->dirlin);
		goto out;
	}
	
again:
	
	/* Create or link to the old inode */
	if (dolink) {
		link_lin(sw_ctx, lm->dirlin, lm->d_lin, lm->d_lin, lm->d_name,
		    lm->d_enc, lm->d_type, &error);
		goto out;
	} else {
		if (lm->d_type == DT_CHR || lm->d_type == DT_BLK) {
			ASSERT(sscanf(lm->symlink, "%u %u",
			    &major, &minor) == 2);
			dev = makedev(major, minor);
		}
		ret = enc_lin_create_linkat(lm->d_lin, dir_fd, lm->d_name,
		    lm->d_enc, mode, dev, lm->symlink, lm->senc);
		if (ret != 0 && errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "Failed to link %s lm->d_name with type %d",
			    lm->d_name, lm->d_type);
			goto out;
		}
		if (ret == -1 && errno == EEXIST) {
			/*
			 * Make sure that the lin does exist.
			 * It could happen that dirent is removed but
			 * the file/directory is still held due to
			 * file ref count
			 * XXX Temporary logic to die to see if this
			 * is due to caching issues XXX
			 */
			sleep(1);
			log(ERROR, "Waiting for filesystem cleanup"
			    " of lin %{} with name %s type %d",
			    lin_fmt(lm->d_lin), lm->d_name, lm->d_type);
			/** Since sleep() is unreliable (wakes up on signals),
			 * compare current time to the deadline */
			if (gettimeofday(&now, NULL))
				ON_SYSERR_GOTO(out, errno, error);
			tcmp = timercmp(&now, &deadline, <) != 0;
			found = get_dir_parent(lm->d_lin, HEAD_SNAPID,
			    &plin, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			if (found && lm->d_type == DT_DIR) {
				log(DEBUG, "%s: found %{} in parent %{} while "
				    "trying to create in parent %{}",
				    __func__, lin_fmt(lm->d_lin),
				    lin_fmt(plin), lin_fmt(lm->dirlin));
				if (lm->dirlin == plin)
					goto out;
				plin_is_tmp = is_tmp_lin(plin, global, &error);
				ON_ISI_ERROR_GOTO(out, error);
				dirlin_is_tmp = is_tmp_lin(lm->dirlin, global,
				    &error);
				ON_ISI_ERROR_GOTO(out, error);
				/*
				 * We found d_lin in tmp, but we need to link
				 * the lin.
				 */
				if (plin_is_tmp && !dirlin_is_tmp) {
					dolink = true;
					goto again;
				}
				/*
				 * We found d_lin in the correct parent, so
				 * update our context with the found parent
				 * lin.
				 */
				if (!plin_is_tmp)
					lm->dirlin = plin;
				goto out;
			} else if (tcmp && !found) {
				/*
				 * We still have lin cached for lazy delete.
				 * We should wait for some time to let
				 * the filesystem cleanup the lin.
				 * There is no hard time that we should wait
				 * so lets wait for sec as standard. Note that
				 * we really cant fail the job since its
				 * better to sleep as it will never be
				 * indefinite.
				 */
				goto again;
			}
			ON_SYSERR_GOTO(out, EPERM, error,
			    "Directory/file lin %{} to be restored "
			    "may still have open file handles which "
			    "need to be closed or is waiting for "
			    "async delete. Use sysctl "
			    "efs.bam.busy_vnodes to check for "
			    "processes which have opened this "
			    "directory/file. Kill all processes which "
			    "have open file handles and restart the "
			    "restore process.",
			    lin_fmt(lm->d_lin));
		}
		if (ret == 0)
			goto out;
	}


	/*
	 * Either the link exists or lin already created
	 * due to multiple links. If link exists, then either the
	 * link is valid or is invalid. So first check if the link
	 * already exists (and valid). If so then we are done, else create
	 * a link to the lin
	 */

	ret = enc_fstatat(dir_fd, lm->d_name, lm->d_enc, &st, AT_SYMLINK_NOFOLLOW);

	if (ret == -1 && errno == ENOENT) {
		dolink = true;
		goto again;
	} else if (ret == -1) {
		tmp_errno = errno;
		path = get_valid_rep_path(lm->dirlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat entry %s in dir %s", lm->d_name,
		    path);
		free(path);
		goto out;
	}

	if (ret == 0) {
		if (lm->d_lin != st.st_ino) {
			/*
			 * If we hit this then we are sure that these
			 * files/dirs are lying due to copy job or we got link
			 * operation for same dirent before unlink. just
			 * move them to tmp folder to be picked up later or
			 * to be cleaned up at the end of job.
			 */
			tmp_fd = get_tmp_fd(st.st_ino, &sw_ctx->global, &error);
			if (error)
				goto out;

			unlinked = move_or_remove_dirent(sw_ctx, dir_fd,
			    tmp_fd, &st, lm->d_name, lm->d_enc, &error);
			if (error)
				goto out;
			if (unlinked) {
				if (S_ISDIR(st.st_mode)) {
					sw_ctx->stats.stf_cur_stats->dirs->
					    unlinked->dst++;
				} else {
					sw_ctx->stats.stf_cur_stats->files->
					    unlinked->dst++;
				}
			}
			/* Lets again try to create the lin */
			goto again;
		}
	}

out:
	if (dir_fd > 0)
		close(dir_fd);
	if (tmp_fd > 0)
		close(tmp_fd);
	isi_error_handle(error, error_out);

}

/* Helper function to reuse a conflict resolution LIN. When we detect a
 * conflict on a WORM committed LIN, we can check if we've already resolved
 * a conflict for that LIN in the past. If we have, we should reuse the same
 * conflict resolution LIN (conflict_lin).
 * Given the conflict resolution lin, and the link message containing the file
 * parameters, recreate the conlict resolution lin. */
static void
reuse_conflict_lin(struct migr_sworker_ctx *sw_ctx, ifs_lin_t conflict_lin,
    struct link_msg *lm, struct isi_error **error_out)
{
	int dir_fd = -1;
	int ret;
	int mode = dtype_to_ifstype(lm->d_type) | 0755;
	int dev = 0;
	uint32_t major = 0, minor = 0;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct isi_error *error = NULL;

	/* Open the parent directory. */
	dir_fd = ifs_lin_open(lm->dirlin, HEAD_SNAPID, O_RDWR);
	if (dir_fd < 0) {
		error = isi_system_error_new(errno, "Unable to open lin %llx",
		    lm->dirlin);
		goto out;
	}

	if (lm->d_type == DT_CHR || lm->d_type == DT_BLK) {
		ASSERT(sscanf(lm->symlink, "%u %u",
		    &major, &minor) == 2);
		dev = makedev(major, minor);
	}

	/* Create the conflict_lin under the parent directory. */
	ret = enc_lin_create_linkat(conflict_lin, dir_fd, lm->d_name,
	    lm->d_enc, mode, dev, lm->symlink, lm->senc);

	/* It's okay if it already exists. */
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to reuse conflict "
		    "lin %llx. Failed to link file under parent lin %llx",
		    conflict_lin, lm->dirlin);
		goto out;
	}

	/* Map the committed lin to the conflict resolution lin. */
	set_mapping_cond(lm->d_lin, conflict_lin, global->lmap_ctx, true,
	    0, &error);
	if (error)
		goto out;

out:
	if (dir_fd > 0)
		close(dir_fd);

	isi_error_handle(error, error_out);
}

/*
 * Create a lin in the directory specified by dir_fd.
 * The lin could a directory, file, unix socket, char or block device file.
 * The d_tmpname is used only when the lin is to be created
 * in the tmp working directory.
 */
static void
create_lin_in_dir(struct migr_sworker_ctx *sw_ctx, int dir_fd,
    struct link_msg *lm, char *d_tmpname, ifs_lin_t *dlin,
    struct stat *st, struct isi_error **error_out)
{
	int ret = 0;
	uint64_t lin_out = 0;
	uint32_t dev_major = 0, dev_minor = 0;
	int fd = -1;
	char *d_name = lm->d_name;
	unsigned d_enc = lm->d_enc;
	enum file_type file_type = dirent_type_to_file_type(lm->d_type);
	struct isi_error *error = NULL;

	/* In case we are creating entry in tmpdir */
	if (d_tmpname) {
		d_name = d_tmpname;
		d_enc = ENC_DEFAULT;
	}

	switch (file_type) {
	case SIQ_FT_DIR:
		fd = open_or_create_dir(sw_ctx, dir_fd, d_name, d_enc, 0755,
		    &lin_out, &error);
		if (error)
			goto out;
		sw_ctx->stats.stf_cur_stats->dirs->created->dst++;
		break;

	case SIQ_FT_REG:
		fd = open_or_create_reg(sw_ctx, dir_fd, d_name, d_enc,
		    &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_SYM:
		fd = open_or_create_sym(sw_ctx, dir_fd, lm->symlink,
		    lm->senc, d_name, d_enc, &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_CHAR:
	case SIQ_FT_BLOCK:
		ASSERT(sscanf(lm->symlink, "%u %u",
		    &dev_major, &dev_minor) == 2);
		fd = open_or_create_special(sw_ctx, dir_fd, d_name, d_enc,
		    file_type, dev_major, dev_minor, &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_SOCK:
		fd = open_or_create_socket(sw_ctx, dir_fd, d_name, d_enc,
		    &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_FIFO:
		fd = open_or_create_fifo(sw_ctx, dir_fd, d_name, d_enc,
		    &lin_out, &error);
		if (error)
			goto out;
		break;

	default:
		ASSERT(0);
	}

	ASSERT (fd > 0);
	ret = fstat(fd, st);
	if (ret == -1 ) {
		error = isi_system_error_new(errno,
		    "Failed to stat dirent %s",
		    d_name);
		close(fd);
		fd = -1;
		goto out;
	}

	*dlin = st->st_ino;

out:
	if (fd > 0)
		close(fd);

	isi_error_handle(error, error_out);
}

/*
 * If the parent_lin is 0 then
 *      we create/open lin in tmp working directory.
 * Else we create/open lin in the  parent directory.
 *
 * If the dirent already exists in directory, then we try
 * again create a dirent by moving old dirent to tmp directory.
 */
static uint64_t
create_lin(uint64_t parent_lin, struct link_msg *lm,
    struct migr_sworker_ctx *sw_ctx, bool *do_link,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	uint64_t dlin = 0;
	int parent_fd = -1, ret, tmp_fd = -1;
	struct stat st;
	struct stat parent_st;
	int tmp_errno;
	char *path = NULL;
	ifs_lin_t slin = lm->d_lin;
	bool success, found, reuse = false;
	ifs_lin_t lin_out;
	char entry[MAXNAMELEN + 1];
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	UFAIL_POINT_CODE(create_lin_stall,
	    log(NOTICE, "create_lin_stall failpoint: stalling");
	    create_fp_file(CREATE_LIN_STALL_FP, &error);
	    if (error)
	            goto out;
	    wait_for_deleted_fp_file(CREATE_LIN_STALL_FP);
	    );

	if (do_link != NULL)
		*do_link = false;

	if (parent_lin) {
		parent_fd = ifs_lin_open(parent_lin, HEAD_SNAPID, O_RDWR);
		if (parent_fd < 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(parent_lin, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Unable to open %s", path);
			free(path);
			goto out;
		}
		ASSERT(parent_fd != 0);
		/* Check whether dirent is already present */
		ret = enc_fstatat(parent_fd, lm->d_name,
		    lm->d_enc, &st, AT_SYMLINK_NOFOLLOW);
		if (ret == -1 && errno != ENOENT) {
			tmp_errno = errno;
			path = get_valid_rep_path(parent_lin, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Unable to lookup %s in dir %s", lm->d_name, path);
			free(path);
			goto out;
		}
		if (ret == 0) {
			if (lm->flags & LINK_MSG_NEW_SINGLY_LINKED &&
			    global->action == SIQ_ACT_SYNC) {
				/*
				 * If this is a new file, and we have found a
				 * collision, then we can safely reuse the lin.
				 * Since we do unlinks before links, the only
				 * way a file with the same name could exist is
				 * due to a restarted work item (or a race
				 * condition with a dangling sworker). If the
				 * file types don't match, we have a bug.
				 */
				if (lm->d_type != IFTODT(st.st_mode)) {
					error = isi_system_error_new(EINVAL,
					    "Error trying to reuse LIN %llx "
					    "on target. File types do not "
					    "match.", st.st_ino);
					goto out;
				}
				reuse = true;
			} else if (global->compliance && S_ISDIR(st.st_mode)) {
				/*
				 * If this is a compliance directory ignore
				 * when the dirent is present since localhost
				 * syncs may create a peer directory via mkdir
				 * before it is synced normally.
				 */
				reuse = true;
			} else {
				/*
				 * Either we died while creating the entry or
				 * there is already a dirent that needs to be
				 * moved to tmp directory. This can happen when
				 * there is pending unlink call for this dirent.
				 * Or its copy job and old entry is still around
				 * To be safe, just move it to tmp directory.
				 */
				tmp_fd = get_tmp_fd(st.st_ino, global, &error);
				if (error)
					goto out;

				if (global->action == SIQ_ACT_REPLICATE &&
				    S_ISDIR(st.st_mode) &&
				    lm->d_type == DT_DIR) {

					checkpoint_collision_directory(tmp_fd,
					    st.st_ino, lm->d_lin, &error);
					if (error)
						goto out;
				}

				/*
				 * The file could already exist if two sworkers
				 * are working on the same work item. This can
				 * happen when a pworker dies and an sworker
				 * does not notice in time. Force a check here
				 * to avoid race conditions.
				 */
				check_connection_with_peer(global->primary,
				    true, 1, &error);
				if (error)
					goto out;

				log(NOTICE, "Found existing entry while "
				    "creating lin. source lin: %llx, "
				    "existing target lin: %llx. Moving lin "
				    "%llx to tmp dir", slin, st.st_ino,
				    st.st_ino);

				move_or_remove_dirent(sw_ctx, parent_fd, tmp_fd,
				    &st, lm->d_name, lm->d_enc, &error);
				if (error)
					goto out;
			}
		}

		if (reuse)
			log(DEBUG, "%s: reusing directory dirent for "
			    "parent_lin: %llx, name: %s",
			    __func__, parent_lin, lm->d_name);

		/* Unset immutable flags of the parent dir, if necessary */
		ret = fstat(parent_fd, &parent_st);
		if (ret != 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(parent_lin, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Unable to lookup dir %s", path);
			free(path);
			goto out;
		}
		if (parent_st.st_flags & IMMUT_APP_FLAG_MASK) {
			clear_flags_fd(&parent_st, IMMUT_APP_FLAG_MASK,
			    parent_fd, NULL, &error);
			if (error)
				goto out;
		}
		/*
		 * Now create entry ,this time we
		 * should succeed
		 */
		create_lin_in_dir(sw_ctx, parent_fd, lm,
		    NULL, &dlin, &st, &error);
		if (error)
			goto out;

		ASSERT(dlin != 0, "Missing dlin for slin %llx", slin);

		if (lm->flags & LINK_MSG_NEW_SINGLY_LINKED) {
			lmap_log_add_entry(global->lmap_ctx, &global->lmap_log,
			    slin, dlin, LMAP_SET, global->primary, &error);
			if (error)
				goto out;

			UFAIL_POINT_CODE(flush_after_lin_create,
				siq_nanosleep(RETURN_VALUE, 0);
				flush_lmap_log(&global->lmap_log,
				    global->lmap_ctx, &error);
				if (error)
				        goto out;
			);
		} else {
			success = set_mapping_cond(slin, dlin, global->lmap_ctx,
			    true, 0, &error);
			if (error)
				goto out;
			if (!success) {
				found = get_mapping(slin, &lin_out,
				    global->lmap_ctx, &error);
				if (error)
					goto out;
				ASSERT(found == true, "slin %{} missing from "
					"linmap", lin_fmt(slin));
				if (dlin != lin_out) {
					dlin = lin_out;
					if (global->compliance &&
					    S_ISDIR(st.st_mode)) {
						ASSERT(dlin != 0,
						    "Missing compliance "
						    "dlin for slin %{}",
						    lin_fmt(slin));
						log(NOTICE,
						    "%s: reusing compliance "
						    "domain directory "
						    "dirent %{} for slin: %{} "
						    "name: %s",
						    __func__, lin_fmt(lin_out),
						    lin_fmt(slin), lm->d_name);
						/*
						 * Bug 222508
						 * If the new directory was
						 * already created in the
						 * temporary directory, then
						 * we'll need to move the
						 * new directory to the correct
						 * parent directory.
						 */
						if (!global->compliance_v2 &&
						    do_link != NULL)
							*do_link = true;
						goto out;
					}
					/*
					 * It looks like this lin was created
					 * as a tmp parent because a child link
					 * raced with the creation of this link.
					 *
					 * Remove this link and move the tmp lin
					 * that exists in the linmap into place.
					 */
					tmp_fd = get_tmp_fd(parent_lin, global,
					    &error);
					if (error)
						goto out;

					move_or_remove_dirent(sw_ctx,
					    parent_fd, tmp_fd, &st, lm->d_name,
					    lm->d_enc, &error);
					if (error)
						goto out;
					if (do_link != NULL)
						*do_link = true;
				}
			}
		}
	} else {
		/*
		 * Create the lin in tmp-working-dir
		 * Currently this can happen only we need to create directory.
		 */
		snprintf(entry, sizeof(entry), ".slin-%llx", slin);
		tmp_fd = get_tmp_fd(slin, global, &error);
		if (error)
			goto out;
		create_lin_in_dir(sw_ctx, tmp_fd, lm,
		    entry, &dlin, &st, &error);
		if (error)
			goto out;
		ASSERT(dlin != 0, "Missing dlin in tmp for slin %llx", slin);
		success = set_mapping_cond(slin, dlin, global->lmap_ctx,
		    true, 0, &error);
		if (error)
			goto out;
		if (!success) {
			/* Use the latest lin from linmap  */
			found = get_mapping(slin, &lin_out, global->lmap_ctx,
			    &error);
			if (error)
				goto out;
			ASSERT (found == true);
			dlin = lin_out;
		}
	}

	ASSERT(dlin != 0, "Missing final dlin for slin %llx", slin);
out:
	if (tmp_fd > 0)
		close(tmp_fd);

	if (parent_fd > 0)
		close(parent_fd);
	isi_error_handle(error, error_out);
	return dlin;
}

void
handle_lin_ack(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin,
     struct isi_error **error_out)
{
	int ack_interval = STF_ACK_INTERVAL / 2;
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	global->last_stf_slin = slin;
	global->stf_operations++;

	UFAIL_POINT_CODE(stf_ack_interval,
	    ack_interval = RETURN_VALUE;
	);

	if (global->stf_operations >= ack_interval) {
		/*
		 * Before flush , make sure we check connection
		 * with the source worker. Otherwise it could
		 * lead to extremely odd race conditions.
		 */
		check_connection_with_peer(global->primary, true,
		    1, &error);
		if (error) 
			goto out;

		flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
		if (error)
			goto out;

		send_lmap_acks(sw_ctx, 0, 0, true);

		msg.head.type = LIN_ACK_MSG;
		msg.body.lin_ack.src_lin = global->last_stf_slin;
		msg.body.lin_ack.dst_lin = 0;
		msg.body.lin_ack.result = 0;
		msg.body.lin_ack.siq_err = 0;
		msg.body.lin_ack.oper_count = global->stf_operations;
		msg_send(global->primary, &msg);
		global->stf_operations = 0;
	}
out:
	isi_error_handle(error, error_out);
}

/*
 * This updates the LIN metadata information.
 * If the LIN doesn't exist on target we create one.
 * The LIN metadata information is kept in sw_ctx which
 * will be used later when we get lin_commit msg from source.
 */
int
lin_update_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct lin_update_msg *lin_update_msg = &m->body.lin_update;
	uint64_t dlin = 0, parent_lin = 0;
	int fd  = -1, parent_fd = -1;
	uint64_t slin = lin_update_msg->slin;
	uint64_t splin = lin_update_msg->parent_lin;
	int  ret = -1;
	struct isi_error *error = NULL;
	struct stat st;
	struct file_state *fstatep = NULL;
	struct dir_state *dstatep = NULL;
	struct common_stat_state *new_stat_state;
	char *path = NULL;
	int tmp_errno;

	ASSERT(slin > 0);

	log(TRACE, "%s", __func__);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	get_mapping(slin, &dlin, sw_ctx->global.lmap_ctx, &error);
	if (error)
		goto out;

	if (splin) {
		get_mapping(splin, &parent_lin, sw_ctx->global.lmap_ctx,
		    &error);
		if (error)
			goto out;
	}

	ASSERT(dlin != 0, "Missing update dlin for slin %llx", slin);

	/* Set lin attributes in sworker context */
	fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
	if (fd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to open %s", path);
		free(path);
		goto out;
	}

	ret = fstat(fd, &st);
	if (ret != 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat %s", path);
		free(path);
		goto out;
	}

	/*
	 * Clear immutable/append only flags if set and if the
	 * file is a regular file.
	 */
	if (lin_update_msg->file_type == SIQ_FT_REG &&
	    (st.st_flags & IMMUT_APP_FLAG_MASK)) {
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK, fd, NULL, &error);
		if (error)
			goto out;
	}

	/*
	 * Clear any existing user attributes since new ones (if any) will be
	 * sent following this message.
	 */
	clear_user_attrs(fd, &st, &error);
	if (error)
		goto out;

	/*
	 * If its restore then make sure that all the current entries
	 * are cleared. If the snapshot has any ADS entries they will
	 * all be synced in later phase.
	 */
	if (global->restore &&
	    !(lin_update_msg->flag & LIN_UPDT_MSG_SKIP_ADS)) {
		clear_ads_entries(fd, &st, &error);
		if (error)
			goto out;
	}

	if (lin_update_msg->file_type == SIQ_FT_DIR) {
		dstatep = &sw_ctx->inprog.dstate;
		dstatep->name = NULL;
		dstatep->enc = ENC_NOVAL;
		dstatep->dir_valid = true;
		dstatep->src_dirlin = slin;
		/* Set the current working directory to dir lin */
		ASSERT(sw_ctx->worker.cwd_fd == -1);
		sw_ctx->worker.cwd_fd = fd;
		fd = -1;
		new_stat_state = &dstatep->stat_state;
		sw_ctx->inprog.file_obj = false;

	} else {
		fstatep = &sw_ctx->inprog.fstate;
		fstatep->orig_is_reg_file = S_ISREG(st.st_mode);
		fstatep->name = NULL;
		fstatep->enc = ENC_NOVAL;
		fstatep->src_filelin = slin;
		fstatep->src_dirlin = 0;
		fstatep->src_dir_key = 0;
		fstatep->full_transfer = lin_update_msg->full_transfer;
		fstatep->skip_bb_hash = lin_update_msg->skip_bb_hash;
		fstatep->bb_hash_ctx = NULL;
		fstatep->tgt_lin = dlin;
		/* Set the current fsatte fd to lin */
		ASSERT(fstatep->fd == -1);
		fstatep->fd = fd;
		sw_ctx->worker.cwd_fd = -1;
		fd = -1;
		new_stat_state = &fstatep->stat_state;
		sw_ctx->inprog.file_obj = true;
	}

	new_stat_state->owner = (uid_t)lin_update_msg->uid;
	new_stat_state->group = (gid_t)lin_update_msg->gid;
	new_stat_state->mode  = (mode_t)lin_update_msg->mode;
	new_stat_state->atime.tv_sec = lin_update_msg->atime_sec;
	new_stat_state->atime.tv_nsec = lin_update_msg->atime_nsec;
	new_stat_state->mtime.tv_sec = lin_update_msg->mtime_sec;
	new_stat_state->mtime.tv_nsec = lin_update_msg->mtime_nsec;
	new_stat_state->size  = lin_update_msg->size;
	if (new_stat_state->acl)
		free(new_stat_state->acl);
	new_stat_state->acl = NULL;
	new_stat_state->flags = 0;
	new_stat_state->flags_set = 0;
	if (lin_update_msg->acl != NULL) {
		new_stat_state->acl = strdup(lin_update_msg->acl);
		ASSERT(new_stat_state->acl);
	}
	new_stat_state->flags_set = 1;
	new_stat_state->reap = 0;
	new_stat_state->flags = (fflags_t)lin_update_msg->di_flags;
	new_stat_state->type = lin_update_msg->file_type;
	new_stat_state->worm_committed = lin_update_msg->worm_committed;
	new_stat_state->worm_retention_date =
	    lin_update_msg->worm_retention_date;

	/*
	 * PSCALE-15873 Improve incremental sync performance
	 * We can't do it towards a STUBBED file as it may trigger
	 * unexpected fill req which causes archive failure.
	 */
	if (lin_update_msg->full_transfer &&
	    (new_stat_state->type == SIQ_FT_REG) &&
	    !(st.st_flags & SF_FILE_STUBBED)) {
		ret = ftruncate(fstatep->fd, new_stat_state->size);
		if (ret) {
			ON_SYSERR_DEXIT_GOTO(out, errno, error,
			    "ftruncate error for %{}", lin_fmt(st.st_ino));
		}
	}

out:
	if (fd > 0)
		close(fd);
	if (parent_fd != -1) {
		ASSERT(parent_fd != 0);
		close(parent_fd);
	}

	if (error) {
		handle_stf_error_fatal(global->primary, error, E_SIQ_LIN_UPDT,
		    EL_SIQ_DEST, slin, slin);
	}

	return ret;
}

int
old_lin_update_callback(struct generic_msg *m, void *ctx)
{
	struct lin_update_msg *update = &m->body.lin_update;
	struct old_lin_update_msg *old_update = &m->body.old_lin_update;
	
	//XXXDPL HACK - Copy old lin update into new lin update struct, which
	// is the same plus 2 extra fields.
	*((struct old_lin_update_msg*)update) = *old_update;
	update->worm_committed = 0;
	update->worm_retention_date = 0;

	return lin_update_callback(m, ctx);
}


/*
 * This updates the extra file metadata.
 * If the lin doesnt exist on target we create one.
 * This metadata is committed in the when an extra_file_commit
 * comes from the source.
 */
int
extra_file_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct extra_file_msg *extra_file_msg = &m->body.extra_file;
	int fd  = -1, parent_fd = -1;
	int  ret = -1;
	struct isi_error *error = NULL;
	struct stat st;
	struct file_state *fstatep = NULL;
	struct common_stat_state *new_stat_state;
	char tmp_path[MAXPATHLEN];
	char mkdir_cmd[255];

	ilog(IL_INFO, "%s called", __func__);

	sw_ctx->global.doing_extra_file = true;

	sprintf(mkdir_cmd, "mkdir -p %s", SIQ_IFS_IDMAP_DIR);
	/*
	 * Ignore failures.  If there was an issue with the directory, then
	 * it'll probably show up in the dump step.
	 */
	system(mkdir_cmd);


	sprintf(tmp_path, "/ifs/.ifsvar/modules/tsm/samba-bak/%s.tmp",
	    extra_file_msg->d_name);

	fd = open(tmp_path, O_RDWR|O_CREAT|O_TRUNC, 0600);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open %s", tmp_path);
		goto out;
	}

	ret = fstat(fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat %s", tmp_path);
		goto out;
	}

	/*
	 * Clear immutable/append only flags if set and if the
	 * file is a regular file.
	 */
	if (extra_file_msg->file_type == SIQ_FT_REG &&
	    (st.st_flags & IMMUT_APP_FLAG_MASK)) {
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK, fd, NULL, &error);
		if (error)
			goto out;
	}

	fstatep = &sw_ctx->inprog.fstate;
	fstatep->orig_is_reg_file = S_ISREG(st.st_mode);
	fstatep->enc = ENC_DEFAULT;
	fstatep->src_filelin = 0;
	fstatep->src_dirlin = 0;
	fstatep->src_dir_key = 0;
	fstatep->full_transfer = true;
	fstatep->skip_bb_hash = true;
	fstatep->bb_hash_ctx = NULL;
	fstatep->name = strdup(extra_file_msg->d_name);
	ASSERT(fstatep->fd == -1);
	fstatep->fd = fd;
	sw_ctx->worker.cwd_fd = -1;
	fd = -1;

	new_stat_state = &fstatep->stat_state;
	new_stat_state->owner = (uid_t)extra_file_msg->uid;
	new_stat_state->group = (gid_t)extra_file_msg->gid;
	new_stat_state->mode  = (mode_t)extra_file_msg->mode;
	new_stat_state->atime.tv_sec = extra_file_msg->atime_sec;
	new_stat_state->atime.tv_nsec = extra_file_msg->atime_nsec;
	new_stat_state->mtime.tv_sec = extra_file_msg->mtime_sec;
	new_stat_state->mtime.tv_nsec = extra_file_msg->mtime_nsec;
	new_stat_state->size  = extra_file_msg->size;
	if (new_stat_state->acl)
		free(new_stat_state->acl);
	new_stat_state->acl = NULL;
	new_stat_state->flags = 0;
	new_stat_state->flags_set = 0;
	if (extra_file_msg->acl != NULL) {
		new_stat_state->acl = strdup(extra_file_msg->acl);
		ASSERT(new_stat_state->acl);
	}
	new_stat_state->flags_set = 1;
	new_stat_state->reap = 0;
	new_stat_state->flags = (fflags_t)extra_file_msg->di_flags;
	new_stat_state->type = SIQ_FT_REG;

out:
	if (fd != -1)
		close(fd);
	if (parent_fd != -1) {
		ASSERT(parent_fd != 0);
		close(parent_fd);
	}

	if (error) {
		handle_stf_error_fatal(global->primary, error,
		    E_SIQ_IDMAP_SEND, EL_SIQ_DEST, 0, 0);
	}

	return ret;
}

/*
 * Commit the attributes of the extra file just sent.
 * This is called at the end of each extra file transfer. 
 * This uses the attributes stored in sw_ctx and sets them on disk.
 * This msg marks end of an extra file transfer.
 */
int
extra_file_commit_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct file_state *fstatep = NULL;
	struct isi_error *error = NULL;
	struct common_stat_state *stat_state;
	struct generic_msg msg = {};
	struct stat st;
	int ret = -1;
	char final_path[512];
	char tmp_path[512];
	log(TRACE, "%s", __func__);

	fstatep = &inprog->fstate;

	if (fstatep->error_closed_fd)
		goto out;

	sprintf(tmp_path, "/ifs/.ifsvar/modules/tsm/samba-bak/%s.tmp",
	    fstatep->name);
	sprintf(final_path, "/ifs/.ifsvar/modules/tsm/samba-bak/%s",
	    fstatep->name);

	ASSERT(fstatep->fd > 0);
	ret = fstat(fstatep->fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat extra file");
		goto out;
	}

	fstatep = &inprog->fstate;
	ASSERT(fstatep->fd > 0);

	fstatep = &sw_ctx->inprog.fstate;
	stat_state = &fstatep->stat_state;

	/* Set all new attributes */
	apply_attributes(sw_ctx, fstatep->fd, stat_state, true, true, &error);
	if (error)
		goto out;

	ret = rename(tmp_path, final_path);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Unable to rename extra file %s to %s", tmp_path,
		        final_path);
		goto out;
	}

	ret = close(fstatep->fd);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "close error");
		goto out;
	}

	/* Reset all context values */
	fstatep->fd = -1;
	if (fstatep->stat_state.acl) {
		free(fstatep->stat_state.acl);
		fstatep->stat_state.acl = NULL;
	}

	/* Send a commit message back as an ACK */
	msg.head.type = EXTRA_FILE_COMMIT_MSG;
	msg_send(global->primary, &msg);

	sw_ctx->global.doing_extra_file = false;

out:
	if (fstatep->dsess) {
		struct isi_error *error2 = NULL;
		bool valid = isi_file_dsession_destroy(fstatep->dsess, true,
		    &error2);
		if (error && error2) {
			char *error2_msg =
			    isi_error_get_detailed_message(error2);
			isi_error_add_context(error, "Error destroying "
			    "dsession %s", error2_msg);
			free(error2_msg);
			free(error2);
			error2 = NULL;
		} else if (error2) {
			error = error2;
			error2 = NULL;
		} else if (!valid) {
			error = isi_system_error_new(EIO,
			    "Cloudpools cleanup error");
		}
		fstatep->dsess = NULL;
	}
	if (fstatep->fd)
		close(fstatep->fd);
	fstatep->fd = -1;

	if (fstatep->name)
		free(fstatep->name);
	fstatep->name = NULL;
	fstatep->error_closed_fd = false;
	fstatep->cur_off = 0;
	fstatep->full_transfer = 0;
	fstatep->bytes_changed = 0;

	if (sw_ctx->worker.cwd_fd > 0) {
		ret = close(sw_ctx->worker.cwd_fd);
		sw_ctx->worker.cwd_fd = -1;
		if (ret != 0) {
			if (error) {
				isi_error_add_context(error,
				    "close error: %s", strerror(errno));
			} else {
				error = isi_system_error_new(errno,
				    "close error");
			}
			goto out;
		}
	}

	handle_stf_error_fatal(global->primary, error, E_SIQ_IDMAP_SEND,
	    EL_SIQ_DEST, 0, 0);
	return 0;
}




/*
 * Commit the attributes of lin
 * This is called at the end of each lin transfer. 
 * This uses the attributes stored in sw_ctx and sets them on disk.
 * This msg marks end of lin transfer.
 */
int
lin_commit_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct lin_commit_msg *lcm = &m->body.lin_commit;
	struct generic_msg msg = {};
	struct file_state *fstatep = NULL;
	struct dir_state *dstatep = NULL;
	uint64_t slin = lcm->slin;
	struct isi_error *error = NULL;
	struct common_stat_state *stat_state;
	struct file_done_msg fdone;
	bool hash_used = false;
	bool hash_match = false;
	uint64_t dlin = 0;
	bool data_changed;
	int fd;
	struct stat st;
	int ret = -1;
	char *path = NULL;
	int tmp_errno;

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	get_mapping(slin, &dlin, sw_ctx->global.lmap_ctx, &error);
	if (error)
		goto out;
	ASSERT(dlin > 0, "Missing dlin for commit of slin %llx", slin);

	log(TRACE, "%s", __func__);
	fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
	if (fd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to open %s", path);
		free(path);
		goto out;
	}

	ret = fstat(fd, &st);
	if (ret != 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(dlin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Unable to stat %s", path);
		free(path);
		goto out;
	}

	/* Make sure the lin number matches the one stored in sw_ctx */
	if (S_ISDIR(st.st_mode)) {
		ASSERT(slin == inprog->dstate.src_dirlin, "Mismatched dir "
		    "lins %llx and %llx", slin, inprog->dstate.src_dirlin);
		dstatep = &sw_ctx->inprog.dstate;
		inprog->dstate.dir_valid = false;
		sw_ctx->stats.dirs++;
		stat_state = &dstatep->stat_state;
		/* Set all new attributes */
		apply_attributes(sw_ctx, fd, stat_state, false, true, &error);
		if (error)
			goto out;

		if (dstatep->stat_state.acl) {
			free(dstatep->stat_state.acl);
			dstatep->stat_state.acl = NULL;
		}
	} else {
		ASSERT(slin == inprog->fstate.src_filelin, "Mismatched file "
		    "lins %llx and %llx", slin, inprog->fstate.src_filelin);
		fstatep = &inprog->fstate;
		ASSERT(fstatep->fd > 0);
		/* Compute hash match if desired */
		fdone.size_changed = lcm->size_changed;
		fdone.hash_str = lcm->hash_str;
		hash_used = finish_bb_hash(sw_ctx, &fdone, &hash_match,
		    &error);
		if (error)
			goto out;
		if (hash_used && !hash_match) {
			/*
			 * Send a negative ack about hash mismatch
			 */
			msg.head.type = LIN_ACK_MSG;
			msg.body.lin_ack.src_lin = slin;
			msg.body.lin_ack.dst_lin = dlin;
			msg.body.lin_ack.result = 1;
			msg.body.lin_ack.siq_err = E_SIQ_BBD_CHKSUM;
			msg_send(global->primary, &msg);
		}

		/* Indicate whether any data changed */
		if ((fstatep->bytes_changed || fstatep->full_transfer
		    || lcm->size_changed) && S_ISREG(st.st_mode))
			data_changed = true;
		else
			data_changed = false;

		fstatep = &sw_ctx->inprog.fstate;
		stat_state = &fstatep->stat_state;

		/* Set all new attributes */
		apply_attributes(sw_ctx, fd, stat_state, data_changed, true,
		    &error);
		if (error)
			goto out;

		if (fstatep->dsess) {
			bool valid = isi_file_dsession_destroy(fstatep->dsess,
			    true, &error);
			if (error)
				goto out;
			if (!valid) {
				error = isi_system_error_new(EIO, "Cloudpools "
				    "cleanup error");
				goto out;
			}
			fstatep->dsess = NULL;
		}

		ret = close(fstatep->fd);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "close error");
			goto out;
		}

		/* Reset all context values */
		fstatep->fd = -1;
		if (fstatep->stat_state.acl) {
			free(fstatep->stat_state.acl);
			fstatep->stat_state.acl = NULL;
		}

		hash_free(fstatep->bb_hash_ctx);
		fstatep->bb_hash_ctx = NULL;
		if (fstatep->name)
			free(fstatep->name);
		fstatep->name = NULL;
		fstatep->error_closed_fd = false;
		fstatep->cur_off = 0;
		fstatep->full_transfer = 0;
		fstatep->bytes_changed = 0;
	}

	/* Close all file descriptor */
	if (fd > 0) {
		ret = close(fd);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "close error");
			goto out;
		}
	}

	if (sw_ctx->worker.cwd_fd > 0) {
		ret = close(sw_ctx->worker.cwd_fd);
		sw_ctx->worker.cwd_fd = -1;
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "close error");
			goto out;
		}
	}

	handle_lin_ack(sw_ctx, slin, &error);

out:
	handle_stf_error_fatal(global->primary, error, E_SIQ_LIN_COMMIT,
	    EL_SIQ_DEST, slin, slin);
	return 0;
}

int
delete_lin_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct delete_lin_msg *del_lin_msg = &m->body.delete_lin;
	uint64_t slin = del_lin_msg->slin;
	int ret = -1, fd = -1;
	uint64_t dlin;
	struct isi_error *error = NULL;
	struct stat st = {};
	bool is_tmp = false;

	ASSERT(slin > 0);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	get_mapping(slin, &dlin, sw_ctx->global.lmap_ctx, &error);
	if (error)
		goto out;

	/* XXX This should ideally be assert, but for now skip XXX */
	if (dlin == 0) {
		ret = 0;
		goto out;
	}

	/* Bug 215632 - only check directories with is_tmp_lin */
	fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDONLY);
	if (fd == -1) {
		if (errno != ENOENT && errno != ESTALE) {
			error = isi_system_error_new(errno,
			    "ifs_lin_open failed for %{} "
			    "(slin %{})", lin_fmt(dlin), lin_fmt(slin));
		}
		goto out;
	}
	ret = fstat(fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "fstat operation failed for %{} (slin %{})",
		    lin_fmt(dlin), lin_fmt(slin));
		ret = -1;
		goto out;
	}
	close(fd);
	fd = -1;
	if (S_ISDIR(st.st_mode)) {
		is_tmp = is_tmp_lin(dlin, global, &error);
		if (error)
			goto out;
	}
	if (is_tmp) {
		/* We will allow the tmp working directory to be removed */
		ASSERT(global->restore);
		ret = 0;
		goto out;
	}

	/* Delete lin only for sync jobs */
	if (global->action == SIQ_ACT_SYNC) {
		delete_lin(sw_ctx, dlin, &error);
		if (error)
			goto out;
	}

	if (global->curr_ver < FLAG_VER_3_5) {
		/* Remove the lin map entry */
		lmap_log_add_entry(global->lmap_ctx, &global->lmap_log,
		    slin, 0, LMAP_REM, 0, &error);
	} else if (!global->restore) {
		send_lmap_acks(sw_ctx, slin, dlin, false);
	}

out:
	if (fd != -1)
		close(fd);
	if (!error) {
		handle_lin_ack(sw_ctx, slin, &error);
	}

	handle_stf_error_fatal(global->primary, error, E_SIQ_DEL_LIN,
	    EL_SIQ_DEST, slin, slin);
	return 0;
}

/*
 * Precheck before create lin.
 */
static void
create_lin_precheck(struct migr_sworker_ctx *sw_ctx,
    uint64_t direntlin, uint64_t direntslin, uint64_t parent_lin,
    struct link_msg *lm, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int parent_fd = -1, ret = -1;
	struct stat st;
	int tmp_errno;
	char *path = NULL;
	bool found = false;
	struct dirent entry;
	uint64_t parent_dir_lin = 0;
	ifs_lin_t tmplin;
	int tmp_fd = -1, fd = -1;

	log(DEBUG, "create_lin_precheck() lin exception error for source dir %llx "
	    "dirent %llx", direntlin, lm->dirlin);

	if (parent_lin == 0) return;

	/* Here parent_lin is dlin */
	parent_fd = ifs_lin_open(parent_lin, HEAD_SNAPID, O_RDWR);
	if (parent_fd < 0) {
		tmp_errno = errno;
		path = get_valid_rep_path(parent_lin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
			"Unable to open %s", path);
		free(path);
		goto out;
	}

	ASSERT(parent_fd != 0);
	/* Check whether dirent is already present */
	ret = enc_fstatat(parent_fd, lm->d_name,
		lm->d_enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1 && errno != ENOENT) {
		tmp_errno = errno;
		path = get_valid_rep_path(parent_lin, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
			"Unable to lookup %s in dir %s", lm->d_name, path);
		free(path);
		goto out;
	}

	/*
	* We need to find the name of the parent dir.
	*/
	found = dirent_lookup_by_lin(HEAD_SNAPID, parent_lin, 0,
		&parent_dir_lin, &entry, &error);

	if (found && 0 == strncmp(entry.d_name, TW_MV_TOPTMPNAME,
		strlen(TW_MV_TOPTMPNAME))) {
		/* find the dirent in the top treemv dir, open and remove it */
		fd = enc_openat(parent_fd, lm->d_name, lm->d_enc,
		O_RDONLY | O_NOFOLLOW | O_OPENLINK);
		if (fd == -1 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			"Unable to open file %s", lm->d_name);
			goto out;
		}
		/* Perform the unlink call */
		if (fd > 0) {
			ret = enc_unlinkat(parent_fd, lm->d_name, lm->d_enc, 0);
			if (ret == -1 && errno != ENOENT) {
				error = isi_system_error_new(errno,
				"Unable to unlink file %s", lm->d_name);
				goto out;
			}
		}

		if (fd > 0)
			close(fd);

		/* validate the beginning of the cur_name, if it's not right, we
		 * need to fail */
		if (0 != strncmp(entry.d_name, TW_MV_TOPTMPNAME, strlen(TW_MV_TOPTMPNAME))
			|| 1 != sscanf(entry.d_name, TW_MV_TOPTMPNAME "%llx", &tmplin)) {
			error = isi_system_error_new(EINVAL, "current filename %s is "
				"does not match the required pattern for quota tree "
				"move restarts.", entry.d_name);
			goto out;
		}

		tmp_fd = ifs_lin_open(tmplin, HEAD_SNAPID, O_RDWR);
		if (tmp_fd < 0 && errno == ENOENT) {
			/* We are fine with it. */
			goto out;
		}  else if (tmp_fd < 0) {
			ON_SYSERR_GOTO(out, errno, error, "Unable to open lin %llx",
			tmplin);
		}

		fd = enc_openat(tmp_fd, lm->d_name, lm->d_enc,
		O_RDONLY | O_NOFOLLOW | O_OPENLINK);
		if (fd == -1 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			"Unable to open file %s", lm->d_name);
			goto out;
		}

		/* Perform the unlink call */
		if (fd > 0) {
			ret = enc_unlinkat(tmp_fd, lm->d_name, lm->d_enc, 0);
			if (ret == -1 && errno != ENOENT) {
				error = isi_system_error_new(errno,
				"Unable to unlink file %s", lm->d_name);
				goto out;
			}
		}
	}
out:
	if (fd > 0)
		close(fd);
	if (parent_fd > 0)
		close(parent_fd);
	if (tmp_fd > 0)
		close(tmp_fd);
	isi_error_handle(error, error_out);
}

int
link_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct link_msg *link_msg = &m->body.link;
	struct link_msg tlink_msg = {};
	uint64_t dlin, direntlin, maplin = 0;
	uint64_t slin = link_msg->dirlin;
	ifs_lin_t tmp_lin;
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	bool do_link = false;
	char entry[MAXNAMELEN + 1];
	bool dlin_outside_domain = false, direntlin_outside_domain = false;
	bool found = false;
	bool committed;
	int tmp_errno;
	int retry = 0;

	log(TRACE, "link_callback");

	ASSERT(slin > 0);

	UFAIL_POINT_CODE(stall_on_link_cb,
		log(NOTICE, "Stalling on link callback");
		siq_nanosleep(RETURN_VALUE, 0);
	);

	/* migr_process() processes batches of link messages in a tight loop -
	 * disconnections may not be noticed for a long time. It's on the
	 * sworker to periodically check. */
	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	get_mapping_with_domain_info(link_msg->d_lin, &direntlin,
	    sw_ctx->global.lmap_ctx, &direntlin_outside_domain, &error);
	if (error)
		goto out;

	get_mapping_with_domain_info(slin, &dlin,
	    sw_ctx->global.lmap_ctx, &dlin_outside_domain, &error);
	if (error)
		goto out;
	UFAIL_POINT_CODE(stall_link_cb_post_get_map,
	    if (link_msg->d_type == DT_DIR) {
		    log(NOTICE, "Stalling on link_callback after get mapping "
			" dlin: %{} direntlin: %{}", lin_fmt(slin),
			lin_fmt(link_msg->d_lin));
		    siq_nanosleep(RETURN_VALUE, 0);
	    }
	);
	if (dlin == 0) {
		/*
		 * We need to create parent directory in tmp-working-dir
		 * We will use temporary lin_msg for it
		 */
		snprintf(entry, sizeof(entry), ".slin-%llx", slin);
		tlink_msg.d_lin = link_msg->dirlin;
		tlink_msg.d_type = DT_DIR;
		tlink_msg.d_name = entry;
		tlink_msg.d_enc = ENC_DEFAULT;
		tlink_msg.dirlin = get_tmp_lin(ROOT_LIN, global, NULL, &error);
		if (error)
			goto out;

		if (global->restore && !dlin_outside_domain) {
			reuse_old_lin(sw_ctx, &tlink_msg, &error);
			dlin = link_msg->dirlin;
		} else {
			/*
			 * Bug 206186
			 * If direntlin exists, create the temporary dlin in
			 * the tmpdir closest to direntlin.
			 */
			if (direntlin) {
				tmp_lin = get_tmp_lin(direntlin, global, NULL,
				    &error);
				if (error)
					goto out;
			} else
				tmp_lin = 0;
			dlin = create_lin(tmp_lin, &tlink_msg, sw_ctx,
			    NULL, &error);
		}
		if (error)
			goto out;
		ASSERT(dlin != 0, "Missing link dlin for slin %llx", slin);
	}

	UFAIL_POINT_CODE(only_link_dirs_in_tmp,
	{
		/* If link is for a file, continue.
		 * If parent is in tmp dir, then continue.
		 * Otherwise, neglect the item by returning w/ no ack. */
		int ret = -1;
		char *path = NULL;
		size_t pathlen = 0;

		tmp_lin = get_tmp_lin(dlin, global, NULL, &error);
		if (error)
			goto out;
		ret = lin_get_path(tmp_lin, dlin, HEAD_SNAPID,
		    0, 0, &pathlen, &path);
		if (ret != 0 && errno != EINVAL) {
			error = isi_system_error_new(errno, "Bad get lin path");
			goto out;
		}
		if (link_msg->d_type == DT_DIR && pathlen <= 0) {
			ASSERT(false, "UFP, only_link_dirs_in_tmp. "
			       "Only links within .tmp_working_dir allowed.");
		}
	});

	if (direntlin > 0 && global->restore &&
	    global->compliance_v2 &&
	    ((link_msg->flags & LINK_MSG_CONFLICT_RES) || global->failover)) {
		/*
		 * Check if the file is committed. if so we need
		 * to create links with new lins.
		 */
		committed = is_worm_committed(direntlin, HEAD_SNAPID, &error);
		if (error)
			goto out;

		if (committed) {
			/* Figure out if we have resolved a conflict for this
			 * LIN before. If we have, we can use the same conflict
			 * resolution LIN as before. If not, we'll create a new
			 * one. */
			if (global->comp_map_ctx) {
				found = get_compliance_mapping(direntlin,
				    &maplin, global->comp_map_ctx, &error);
				if (error)
					goto out;
			}

			if (found) {
				/* Reuse old conflict resolution lin. */
				reuse_conflict_lin(sw_ctx, maplin, link_msg,
				    &error);
				goto out;
			} else {
				/* Create new conflict resolution lin. */
				direntlin_outside_domain = true;
				direntlin = 0;
			}
		}
	}

	if (direntlin == 0) {
		/* We also need to create dirent lin */
		do_link = false;
		if (global->restore && !direntlin_outside_domain) {
			reuse_old_lin(sw_ctx, link_msg, &error);
		} else {
			/* If the sworker receives a link_msg and the
			 * linmap lookup for the dirent lin doesn't find
			 * an entry then we know we need to create a new file.
			 * If the directory we are creating the new dirent in
			 * is in the process of a treemv (it's named tw-tmp-<lin>),
			 * then check the treemv tmp dir to see if the dirent
			 * already exists in there.
			 * If the dirent exists, check and unlink it.
			 * If the entry doesn't exist, create the
			 * dirent in the correct directory and create the
			 * linmap entry. */
			int dirfd = -1;
			dirfd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDWR);
			if (dirfd < 0) {
				char *path = NULL;
				tmp_errno = errno;
				path = get_valid_rep_path(dlin, HEAD_SNAPID);
				error = isi_system_error_new(tmp_errno,
				" ifs_lin_open failed for %s", path);
				free(path);
				goto out;
			}
			ASSERT(dirfd != 0);

			create_lin_precheck(sw_ctx, direntlin, 0,
			    dlin, link_msg, &error);

			if (error)
				goto out;

			direntlin = create_lin(dlin, link_msg, sw_ctx,
			    &do_link, &error);

			if (dirfd > 0)
				close(dirfd);
		}
		if (error || !do_link)
			goto out;
	}

retry:
	link_lin(sw_ctx, dlin, direntlin, link_msg->d_lin, link_msg->d_name,
	    link_msg->d_enc, link_msg->d_type, &error);

	if (error && isi_error_is_a(error, ISI_SIQ_ERROR_CLASS) && 
	    isi_siq_error_get_siqerr((struct isi_siq_error *)error)
	    == E_SIQ_LINK_EXCPT) {
		isi_error_free(error);
		error = NULL;
		log(DEBUG, "lin exception error for source dir %llx "
		    "dirent %llx", slin, link_msg->dirlin);
		msg.head.type = LIN_ACK_MSG;
		msg.body.lin_ack.src_lin = slin;
		msg.body.lin_ack.dst_lin = dlin;
		msg.body.lin_ack.result = 1;
		msg.body.lin_ack.siq_err = E_SIQ_LINK_EXCPT;
		msg_send(global->primary, &msg);
		goto out;
	}

	if (error && isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS) &&
	    isi_system_error_get_error((struct isi_system_error *)error) == EEXIST && \
	    retry < 1) {
		log(ERROR, "EEXIST RETRY link_lin");
		retry++;
		isi_error_destroy(&error);
		goto retry;
	}

out:
	if (!error && !(link_msg->flags & LINK_MSG_SKIP_LIN_CHKPT)) {
		handle_lin_ack(sw_ctx,
		    (link_msg->flags & LINK_MSG_CHKPT_CHILD_LIN) ?
		    link_msg->d_lin : slin, &error);
	}

	handle_stf_error_fatal(global->primary, error, E_SIQ_LINK_ENT,
	    EL_SIQ_DEST, slin, link_msg->d_lin);

	return 0;
}


int
unlink_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct unlink_msg *unlink_msg = &m->body.unlink;
	uint64_t dlin, direntlin;
	uint64_t slin = unlink_msg->dirlin;
	struct isi_error *error = NULL;

	log(TRACE, "unlink_callback");

	ASSERT(slin > 0);

	/* migr_process() processes batches of unlink messages in a tight loop,
	 * disconnections may not be noticed for a long time. It's on the
	 * sworker to periodically check. */
	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	get_mapping(slin, &dlin, sw_ctx->global.lmap_ctx, &error);
	if (error)
		goto out;
	get_mapping(unlink_msg->d_lin, &direntlin, sw_ctx->global.lmap_ctx,
	    &error);
	if (error)
		goto out;

	/* If direntlin is zero , then its already deleted */
	if (direntlin == 0) {
		goto out;
	}

	unlink_lin(sw_ctx, dlin, direntlin, unlink_msg->d_name,
	    unlink_msg->d_enc, unlink_msg->d_type, &error);
	if (error)
		goto out;

out:
	if (!error) {
		handle_lin_ack(sw_ctx, slin, &error);
	}

	handle_stf_error_fatal(global->primary, error, E_SIQ_UNLINK_ENT,
	    EL_SIQ_DEST, slin, unlink_msg->d_lin);
	return 0;
}


int
stale_dir_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct stat st;
	struct isi_error *error = NULL;
	int fd = -1;
	time_t cur_time;
	struct generic_msg reply_msg = {};
	struct timespec ts[3] = {};
	int flags = 0;
	char *path = NULL;
	int tmp_errno;
	ifs_lin_t dest_dlin;
	bool found;

	log(TRACE, "%s", __func__);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	if (global->logdeleted && sw_ctx->inprog.deleted_files.list == NULL) {
		init_list_deleted_dst(&sw_ctx->inprog.deleted_files,
		    sw_ctx->worker.cur_path.utf8path, global->cluster_name);
	}

	if (m->body.stale_dir.is_start) {
		/*
		 * Start a new cleanup dir
		 */
		ASSERT(sw_ctx->worker.cwd_fd == -1);

		dest_dlin = 0;
		found = get_mapping(m->body.stale_dir.src_dlin, &dest_dlin,
		    global->lmap_ctx, &error);
		if (error)
			goto out;
		ASSERT(found, "Missing dlin in stale callback for slin %llx",
		    m->body.stale_dir.src_dlin);

		/* Open the directory we're cleaning */
		fd = ifs_lin_open(dest_dlin, HEAD_SNAPID, O_RDONLY);
		if (fd <= 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(dest_dlin,
			     HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno, "Stale dir "
			    "cleanup error: Failed to open %s", path);
			goto out;
		}

		/* Make sure it's really a directory */
		if (0 != fstat(fd, &st)) {
			tmp_errno = errno;
			path = get_valid_rep_path(st.st_ino,
			     HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno, "Stale dir "
			    "cleanup error: Failed to stat %s", path);
			goto out;
		}
		if (!S_ISDIR(st.st_mode)) {
			tmp_errno = errno;
			path = get_valid_rep_path(st.st_ino,
			     HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno, "Stale dir "
			    "cleanup error: Bad file mode %d for %s",
			    st.st_mode, path);
			goto out;
		}

		/* 
		 * Put the directory fd in the context for use by the
		 * list messages
		 */
		sw_ctx->worker.cwd_fd = fd;

		fd = -1;
	} else {
		/* End a cleanup dir */
		fd = sw_ctx->worker.cwd_fd;
		sw_ctx->worker.cwd_fd = -1;

		if (0 != fstat(fd, &st)) {
			error = isi_system_error_new(errno, "Stale dir "
			    "cleanup error: Failed to stat source lin %llx",
			    m->body.stale_dir.src_dlin);
			goto out;
		}
		
		/* Update the times if we broke them */
		if (st.st_atimespec.tv_sec != m->body.stale_dir.atime_sec ||
		    st.st_atimespec.tv_nsec != m->body.stale_dir.atime_nsec) {
			flags |= VT_ATIME;
			ts[0].tv_sec =  m->body.stale_dir.atime_sec;
			ts[0].tv_nsec = m->body.stale_dir.atime_nsec;
		}

		if (st.st_mtimespec.tv_sec != m->body.stale_dir.mtime_sec ||
		    st.st_mtimespec.tv_nsec != m->body.stale_dir.mtime_nsec) {
			flags |= VT_MTIME;
			ts[1].tv_sec =  m->body.stale_dir.mtime_sec;
			ts[1].tv_nsec = m->body.stale_dir.mtime_nsec;
		}

		if (flags && fvtimes(fd, ts, flags) != 0) {
			tmp_errno = errno;
			path = get_valid_rep_path(st.st_ino,
			     HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "Stale dir cleanup error: fvtimes error"
			    " on %s", path);
			goto out;
		}

		/* Send ACKs periodically so that source can checkpoint */
		cur_time = time(0);
		if (cur_time < global->last_ack_time ||
		    (cur_time - global->last_ack_time) > LIN_ACK_INTERVAL) {
			reply_msg.head.type = LIN_ACK_MSG;
			reply_msg.body.lin_ack.src_lin =
			    m->body.stale_dir.src_dlin;
			reply_msg.body.lin_ack.dst_lin = st.st_ino;
			reply_msg.body.lin_ack.result = 0;
			reply_msg.body.lin_ack.siq_err = 0;
			msg_send(global->primary, &reply_msg);
		}
	}
	
	
 out:
	if (path)
		free(path);
	if (error)
		handle_stf_error_fatal(global->primary, error,
		    E_SIQ_STALE_DIRS, EL_SIQ_DEST, m->body.stale_dir.src_dlin,
		    m->body.stale_dir.src_dlin);

	if (fd != -1)
		close(fd);
	return 0;
}

void
target_work_lock_msg_unpack(struct generic_msg *m,
    struct target_work_lock_msg *ret, struct isi_error **error_out)
{
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(uint64, ript,
	    RMF_WI_LIN, 1, &ret->wi_lin, &error, out);
	RIPT_MSG_GET_FIELD(uint8, ript,
	    RMF_LOCK_WORK_ITEM, 1, &ret->lock_work_item, &error, out);

out:
	isi_error_handle(error, error_out);
}

/*
 * Callback that either locks or releases a work item.
 */
int
target_work_lock_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct target_work_lock_msg wl_msg = {};
	struct isi_error *error = NULL, *lock_error = NULL;

	target_work_lock_msg_unpack(m, &wl_msg, &error);
	if (error)
		goto out;

	if (wl_msg.lock_work_item) {
		UFAIL_POINT_CODE(target_dont_lock_work,
		    return 0;
		);

		ASSERT(sw_ctx->global.wi_lock_fd == -1);
		sw_ctx->global.wi_lock_fd =
		    lock_work_item_target(sw_ctx->global.sync_id,
		    sw_ctx->global.wi_lock_mod, wl_msg.wi_lin,
		    sw_ctx->global.wi_lock_timeout, &lock_error);
		if(lock_error) {
			error = isi_siq_error_new(E_SIQ_TGT_WORK_LOCK,
			    "Failure on work item %llx. %s", wl_msg.wi_lin,
			    isi_error_get_message(lock_error));
			isi_error_free(lock_error);
			goto out;
		}
	} else if (sw_ctx->global.wi_lock_fd >= 0) {
		release_target_work_item_lock(&sw_ctx->global.wi_lock_fd,
		    sw_ctx->global.sync_id, sw_ctx->global.wi_lock_mod,
		    wl_msg.wi_lin, &error);
		if (error) {
			/*
			 * The only error that can occur at this point is a
			 * failure while unlinking the lock file. The exclusive
			 * lock is still released, so just log the error and
			 * clean up the file when the policy is deleted.
			 */
			log(ERROR, "%s", isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
		}
	}

out:
	if (error) {
		handle_stf_error_fatal(sw_ctx->global.primary, error,
		    E_SIQ_TGT_WORK_LOCK, EL_SIQ_DEST, 0, 0);
	}

	return 0;
}
