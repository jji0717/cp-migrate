#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_restripe.h>
#include <ifs/bam/bam_pctl.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_malloc_ostream.h>

#include "sbtree.h"
#include "isirep.h"
#include "siq_btree.h"

/**
 * Make sure the "policy" string bears some resemblance
 * to a valid filename.
 */
static void
assert_policy_path_ok(const char *policy)
{
	int len;
	ASSERT(policy);

	len = strlen(policy);
	ASSERT(len > 0);
	ASSERT(len <= 200, "Bad policy len %d: (%s)", len, policy);
	ASSERT(0 != strcmp("policy", "."));
	ASSERT(0 != strcmp("policy", ".."));
	ASSERT(0 != strcmp("policy", ".snapshot"));
	ASSERT(NULL == strchr(policy, '/'));
}

/**
 * Make sure the "name" string bears some resemblance
 * to a valid filename.
 */
static void
assert_name_path_ok(const char *name)
{
	int len;
	ASSERT(name);

	len = strlen(name);
	ASSERT(len > 0);
	ASSERT(len <= MAXNAMLEN, "Bad name len %d: (%s)", len, name);
	ASSERT(0 != strcmp("name", "."));
	ASSERT(0 != strcmp("name", ".."));
	ASSERT(0 != strcmp("name", ".snapshot"));
	ASSERT(NULL == strchr(name, '/'));
}


/**
 * a path relative to the system btree root that will contain btrees
 * of the specified type.  Guaranteed to be of the form:
 * BASEDIR/..../FINALDIR
 * with no leading or trailing '/', no imbedded "." or ".." components,
 * and no double "//".
 */
static void
siq_btree_get_path(enum siq_btree_type bt, const char *policy,
    char **path_out)
{
	char *path;
	path = malloc(MAXPATHLEN);
	assert_policy_path_ok(policy);

	switch (bt) {
	case siq_btree_repstate:
		snprintf(path, MAXPATHLEN, "synciq/src/%s", policy);
		strcat(path, "/repstate");
		break;
	case siq_btree_worklist:
		snprintf(path, MAXPATHLEN, "synciq/src/%s", policy);
		strcat(path, "/worklist");
		break;
	case siq_btree_summ_stf:
		snprintf(path, MAXPATHLEN, "synciq/src/%s", policy);
		strcat(path, "/summ_stf");
		break;
	case siq_btree_linmap:
		snprintf(path, MAXPATHLEN, "synciq/dst/%s", policy);
		strcat(path, "/linmap");
		break;
	case siq_btree_cpools_sync_state:
		snprintf(path, MAXPATHLEN, "synciq/src/%s", policy);
		strcat(path, "/cpss");
		break;
	case siq_btree_lin_list:
		snprintf(path, MAXPATHLEN, "synciq/dst/%s", policy);
		strncat(path, "/lin_list", MAXPATHLEN - strlen(path));
		break;
	case siq_btree_compliance_map:
		snprintf(path, MAXPATHLEN, "synciq/dst/%s", policy);
		strncat(path, "/compliance_map", MAXPATHLEN - strlen(path));
		break;
	default:
		ASSERT(path, "Bad btree type %d", bt);
	}

	*path_out = path;
}


/**
 * Make (if not already made) the path to the root for the
 * provided btree type.  If path_out or fd_out is non-null,
 * return the path relative to the sbt directory
 * and/or the fd of the directory pointed at by the relative
 * path
 */
static void
siq_btree_make_path(enum siq_btree_type bt, const char *policy,
    char **path_out, int *fd_out, struct isi_error **error_out)
{
	char *path = NULL;
	char *curptr, *newptr;
	char name[MAXNAMELEN];
	size_t len;
	struct isi_error *error = NULL;
	int tmp_errno;
	int res;
	int fd = -1;
	int tmp_fd = -1;

	fd = sbt_open_root_dir(O_RDONLY);
	if (fd == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error opening btree root");
		log(ERROR, "Error opening btree root: %s",
		    strerror(tmp_errno));
		goto out;
	} 
	ASSERT(fd != 0);

	siq_btree_get_path(bt, policy, &path);

	/* Iterate each component, making and opening it */
	curptr = path;
	while (curptr) {
		newptr = strchr(curptr, '/');
		if (newptr) {
			len = newptr - curptr;
		} else {
			len = strlen(curptr);
		}
		ASSERT(len > 0 && len <= MAXNAMELEN);
		memcpy(name, curptr, len);
		name[len] = 0;

		res = enc_mkdirat(fd, name, ENC_DEFAULT, 0777);
		if (res == -1 && errno != EEXIST) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error making btree component %s",
			    name);
			log(ERROR, "Error making btree component %s: %s",
			    name, strerror(tmp_errno));
			goto out;
		}

		tmp_fd = enc_openat(fd, name, ENC_DEFAULT, O_RDONLY);
		if (tmp_fd == -1) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error opening btree component %s", name);
			log(ERROR, "Error opening btree component %s: %s",
			    name, strerror(tmp_errno));
			goto out;
		}
		ASSERT(tmp_fd != 0);
		close(fd);
		fd = tmp_fd;
		tmp_fd = -1;

		if (newptr)
			curptr = newptr + 1;
		else
			curptr = NULL;
	}
	if (path_out) {
		*path_out = path;
		path = NULL;
	}
	if (fd_out) {
		*fd_out = fd;
		fd = -1;
	}

 out:
	if (fd != -1)
		close(fd);
	if (path)
		free(path);

	isi_error_handle(error, error_out);
}

/**
 * Open and return the fd of the directory that will
 * contain btrees of the specified type.
 */
int
siq_btree_open_dir(enum siq_btree_type bt, const char *policy,
    struct isi_error **error_out)
{
	int root_fd = -1;
	int fd = -1;
	char *path = NULL;
	struct isi_error *error = NULL;
	int tmp_errno;

	root_fd = sbt_open_root_dir(O_RDONLY);
	if (root_fd == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error opening btree root");
		log(ERROR, "Error opening btree root: %s",
		    strerror(tmp_errno));
		goto out;
	} 
	ASSERT(fd != 0);

	siq_btree_get_path(bt, policy, &path);
	fd = enc_openat(root_fd, path, ENC_DEFAULT, O_RDONLY);
	if (fd == -1) {
		siq_btree_make_path(bt, policy, NULL, &fd, &error);
		if (error)
			goto out;

		log(DEBUG, "Created btree directory path %s", path);
	}

 out:
	if (root_fd != -1)
		close(root_fd);
	if (fd != -1 && error) {
		close(fd);
		fd = -1;
	}
	if (path)
		free(path);
	isi_error_handle(error, error_out);
	return fd;
}


void
siq_btree_create(enum siq_btree_type bt, const char *policy, char *name,
    bool exist_ok, struct isi_error **error_out)
{
	int dir_fd = -1;
	int fd = -1;
	struct pctl2_get_expattr_args ge = PCTL2_GET_EXPATTR_ARGS_INITIALIZER;
	struct pctl2_set_expattr_args se = PCTL2_SET_EXPATTR_ARGS_INITIALIZER;
	struct protection_policy pp = protection_policy_diskpool_default;
	int res = 0;
	struct fmt FMT_INIT_CLEAN(fmt);
	struct isi_error *error = NULL;
	const char *print_type;

	print_type = siq_btree_printable_type(bt);

	log(TRACE, "%s: %s %s", __func__, print_type, name);

	/* name can not be a path */
	ASSERT(strchr(name, '/') == NULL);

	dir_fd = siq_btree_open_dir(bt, policy, &error);
	if (error)
		goto out;

	assert_name_path_ok(name);
	
	res = ifs_sbt_create(dir_fd, name, ENC_DEFAULT,
	    siq_btree_keysize(bt));
	if (res == -1) {
		if (errno != EEXIST || !exist_ok) {
			error = isi_system_error_new(errno,
			    "Error creating %s %s", print_type, name);
			log(ERROR, "Error creating %s %s: %s",
			    print_type, name, strerror(errno));
			goto out;
		}
	} else {
		log(DEBUG, "%s: created %s sbt for %s", __func__, print_type,
		    name);
	}

	/* Check/modify protection policy */
	fd = siq_btree_open(bt, policy, name, &error);
	if (error)
		goto out;

	log(TRACE, "%s: Opened SyncIQ btree %s %s", __func__, print_type,
	    name);

	if (pctl2_get_expattr(fd, &ge) != 0) {
		error = isi_system_error_new(errno, "Failed to get "
		    "protection policy for SyncIQ btree %s", name);
		goto out;
	}

	if (!protection_policy_equal(ge.ge_file_protection_policy, pp)) {
		/* changed attrs */
		se.se_file_protection_policy = &pp;
		se.se_manually_manage_protection = true;

		fmt_print(&fmt, "%{}", protection_policy_fmt(pp));
		log(NOTICE, "%s: Setting protection policy on %s "
		    "to %s",
		    __func__, name, fmt_string(&fmt));
		if (pctl2_set_expattr(fd, &se) != 0) {
			error = isi_system_error_new(errno, "Failed to set "
			    "protection policy for SyncIQ btree %s", name);
			goto out;
		}

		log(NOTICE, "%s: Restriping SyncIQ btree %s %s",
		    __func__, print_type, name);
		restripe_fd(fd, RESTRIPE_REPROTECT, NULL, NULL, &error);
		if (error)
			goto out;
	}
out:
	if (dir_fd != -1)
		close(dir_fd);
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
}

/*
 * This function does rename of btree , If the dest btree directory
 * does not exist, then it would create one. It also avoids sending 
 * error if the rename is already happened.
 */
void
siq_safe_btree_rename(enum siq_btree_type bt, const char *src_policy,
    const char *src_name, const char *dest_policy, const char *dest_name,
    struct isi_error **error_out)
{
	struct stat st;
	int ret;
	int src_fd = -1, dest_fd = -1;
	struct isi_error *error = NULL;

	/*
	 * Open the src policy root directory
	 */
	src_fd = siq_btree_open_dir(bt, src_policy, &error);
	if (error)
		goto out;

	/*
	 * Open the dest policy root directory.
	 */
	dest_fd = siq_btree_open_dir(bt, dest_policy, &error);
	if (error)
		goto out;


	/* Perform the rename */
	ret = enc_renameat(src_fd, src_name, ENC_DEFAULT, dest_fd, dest_name,
	    ENC_DEFAULT);
	if (ret != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno, 
		    "siq_safe_btree_rename: Failed to rename system"
		    " btree from %s/%s to %s/%s", src_policy, src_name,
		    dest_policy, dest_name);
		goto out;
	}

	if (ret != 0 && errno == ENOENT) {

		/*
		 * Check if the renamed btree already exists,
		 */
		ret = enc_fstatat(dest_fd, dest_name, ENC_DEFAULT, &st, 0);
		if (ret != 0) {
			error = isi_system_error_new(errno, 
			    "siq_safe_btree_rename: Failed to stat"
			    " btree %s/%s", dest_policy, dest_name);
			goto out;
		}
	}

out:
	if (src_fd != -1)
		close(src_fd);
	if (dest_fd != -1)
		close(dest_fd);
	isi_error_handle(error, error_out);
}

int
siq_btree_open(enum siq_btree_type bt, const char *policy, char *name,
    struct isi_error **error_out)
{
	int dir_fd = -1;
	int fd = -1;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	const char *print_type;

	print_type = siq_btree_printable_type(bt);

	log(TRACE, "open_siq_btree %s %s", print_type, name);

	dir_fd = siq_btree_open_dir(bt, policy, &error);
	if (error)
		goto out;

	assert_name_path_ok(name);

	fd =  enc_openat(dir_fd, name, ENC_DEFAULT, O_RDWR);
	if (fd == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error opening %s %s", print_type, name);
		if (tmp_errno != ENOENT)
			log(ERROR, "Error opening %s %s: %s",
			    print_type, name, strerror(errno));
		goto out;
	}

	log(DEBUG, "opened %s sbt for %s", print_type, name);

out:
	if (dir_fd != -1)
		close(dir_fd);
	isi_error_handle(error, error_out);
	return fd;
}

void
siq_btree_remove(enum siq_btree_type bt, const char *policy, char *name,
    bool missing_ok, struct isi_error **error_out)
{
	int dir_fd = -1;
	int res = 0;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	const char *print_type;

	print_type = siq_btree_printable_type(bt);

	log(TRACE, "remove_siq_btree %s %s", print_type, name);

	dir_fd = siq_btree_open_dir(bt, policy, &error);
	if (error)
		goto out;

	assert_name_path_ok(name);

	res = enc_unlinkat(dir_fd, name, ENC_DEFAULT, 0);
	if (res == -1 && (errno != ENOENT || !missing_ok)) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error removing %s %s", print_type, name);
		log(ERROR, "Error removing %s %s: %s",
		    print_type, name, strerror(tmp_errno));
		goto out;
	}

	log(DEBUG, "removed %s sbt for %s", print_type, name);

out:
	if (dir_fd != -1)
		close(dir_fd);

	isi_error_handle(error, error_out);
}

/**
 * Remove any entries in the appropriate directory for bt/policy that
 * begin with name_root as a prefix and don't end with any entry in snap_kept
 * as a trailing name integer.
 */
void
siq_btree_prune_old(enum siq_btree_type bt, const char *policy,
    char *name_root, ifs_snapid_t *snap_kept, int num_kept,
    struct isi_error **error_out)
{
	int dir_fd = -1;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	const char *print_type;
	DIR *dir = NULL;
	struct dirent *d;
	char *endptr;
	int dirent_len;
	int name_root_len;
	int i;
	ifs_snapid_t snap;

	print_type = siq_btree_printable_type(bt);

	assert_policy_path_ok(policy);
	assert_policy_path_ok(name_root);
	
	dir_fd = siq_btree_open_dir(bt, policy, &error);
	if (error)
		goto out;
	ASSERT(dir_fd >= 0);

	dir = fdopendir(dir_fd);
	if (dir == NULL) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error in opendir type %s policy %s", print_type, policy);
		log(ERROR, "Error in opendir type %s policy %s: %s",
		    print_type, policy, strerror(tmp_errno));
		goto out;
	}
	/* dir_fd is now owned by dir */
	dir_fd = -1;

	name_root_len = strlen(name_root);
	while ((errno = 0, d = readdir(dir))) {
		dirent_len = strlen(d->d_name);
		/* Shorter than the required root? */
		if (dirent_len <= name_root_len)
			continue;
		/* Root mismatch? */
		if (0 != strncmp(name_root, d->d_name, name_root_len))
			continue;

		/* Trailing part of the name not an integer? */
		for (i = name_root_len; i < dirent_len; i++) {
			if (!isdigit(d->d_name[i]))
				continue;
		}

		/* long long conversion not use all the digits? */
		snap = strtoll(d->d_name + name_root_len, &endptr, 10);
		if (0 != *endptr)
			continue;

		/* Is the id in the list of kept id's? */
		for (i = 0; i < num_kept; i++) {
			if (snap_kept[i] == snap)
				break;
		}
		if (i < num_kept)
			continue;

		/* Remove it */
		siq_btree_remove(bt, policy, d->d_name, true, &error);
		if (error)
			goto out;
	}
	/* Ugh I hate the broken readdir/errno interface */
	if (errno) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error in readdir type %s policy %s", print_type, policy);
		log(ERROR, "Error in readdir type %s policy %s: %s",
		    print_type, policy, strerror(tmp_errno));
		goto out;
	}

 out:
	if (dir)
		closedir(dir);
	if (dir_fd != -1)
		close(dir_fd);
	isi_error_handle(error, error_out);
}
