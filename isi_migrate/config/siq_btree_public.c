#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_malloc_ostream.h>
#include <isi_util/isi_error.h>
#include "isi_migrate/sched/siq_log.h"
#include "sbtree.h"
#include "siq_btree_public.h"

/*
 * Helper functions for deleting btree directories. These are at
 * times called from the command line so they can't use any functionality
 * in the migrate private library.
 */




/**
 * Remove a subdirectory and all of the siq btrees it contains (no
 * check is made to ensure that the contents are actually siq btrees)
 *
 * Tries to continue past errors, but returns the first error seen
 * in error_out.
 */
static void
siq_btree_remove_subdir(int sbt_tree_root_fd, const char *path,
    struct isi_error **error_out)
{
	int fd = -1;
	int tmp_errno;
	DIR *dir = NULL;
	struct dirent *d;
	char del_path[MAXPATHLEN];
	int ret;
	struct isi_error *kept_error = NULL;

	fd = enc_openat(sbt_tree_root_fd, path, ENC_DEFAULT, O_RDONLY);
	if (fd == -1) {
		if (errno != ENOENT) {
			tmp_errno = errno;
			kept_error = isi_system_error_new(tmp_errno,
			    "Error opening btree path %s", path);
			log(ERROR, "Error opening btree path %s: %s",
			    path, strerror(tmp_errno));
			goto out;
		}
		goto out;
	}

	/* If fdopendir() fails it will close fd. */
	dir = fdopendir(fd);
	if (dir == NULL) {
		tmp_errno = errno;
		kept_error = isi_system_error_new(tmp_errno,
		    "Error in opendir of btree path %s", path);
		log(ERROR, "Error in opendir of path %s: %s",
		    path, strerror(tmp_errno));
		goto out;
	}

	/* fd is now owned by dir */
	fd = -1;

	/* Iterate deleting all btrees */
	while ((errno = 0, d = readdir(dir))) {
		sprintf(del_path, "%s/%s", path, d->d_name);
		if ((0 == strcmp(d->d_name, ".")) || (0 == strcmp(d->d_name, "..")) ||
		    (0 == strcmp(d->d_name, ".snapshot")))
			continue;
		ret = enc_unlinkat(sbt_tree_root_fd, del_path, ENC_DEFAULT, 0);
		if (ret != 0 && errno != ENOENT) {
			tmp_errno = errno;
			log(ERROR, "Error removing btree %s: %s",
			    del_path, strerror(tmp_errno));
			if (!kept_error)
				kept_error = isi_system_error_new(tmp_errno,
				    "Error removing btree %s", del_path);
		}
	}
	/* Ugh I hate the broken readdir/errno interface */
	if (errno) {
		tmp_errno = errno;
		log(ERROR, "Error in readdir of btree path %s: %s",
		    path, strerror(tmp_errno));
		if (!kept_error)
			kept_error = isi_system_error_new(tmp_errno,
				"Error in readdir of btree path %s", path);
	}

	ret = enc_unlinkat(sbt_tree_root_fd, path, ENC_DEFAULT, AT_REMOVEDIR);
	if (ret != 0 && errno != ENOENT) {
		tmp_errno = errno;
		log(ERROR, "Error removing btree dir %s: %s",
		    path, strerror(tmp_errno));
		if (!kept_error)
			kept_error = isi_system_error_new(tmp_errno,
			    "Error removing btree dir %s", path);
	}

 out:
	if (fd >= 0)
		close(fd);
	if (dir)
		closedir(dir);
	isi_error_handle(kept_error, error_out);
}

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
 * Remove all of the btrees (and containing directories) for the source
 * side of the specified policy.  This function continues past errors where
 * possible, but returns the first error encountered in it's out parameter.
 */
void
siq_btree_remove_source(const char *policy, struct isi_error **error_out)
{
	char path[MAXPATHLEN];
	int fd = -1;
	int ret;
	struct isi_error *tmp_error = NULL;
	struct isi_error *kept_error = NULL;
	int tmp_errno;

	assert_policy_path_ok(policy);

	fd = sbt_open_root_dir(O_RDONLY);
	if (fd == -1) {
		tmp_errno = errno;
		kept_error = isi_system_error_new(tmp_errno,
		    "Error opening btree root");
		log(ERROR, "Error opening btree root: %s",
		    strerror(tmp_errno));
		goto out;
	}

	snprintf(path, sizeof(path), "synciq/src/%s/repstate", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		kept_error = tmp_error;
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/src/%s/worklist", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/src/%s/summ_stf", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/src/%s/cpss", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/src/%s", policy);
	ret = enc_unlinkat(fd, path, ENC_DEFAULT, AT_REMOVEDIR);
	if (ret != 0 && errno != ENOENT) {
		tmp_errno = errno;
		log(ERROR, "Error removing btree policy dir %s: %s",
		    path, strerror(tmp_errno));
		if (!kept_error)
			kept_error = isi_system_error_new(tmp_errno,
			    "Error removing btree policy dir %s", path);
	}

	/* BEJ: BUG 137768: attempt to remove the mirror linmap (if it exists)*/
	snprintf(path, sizeof(path), "synciq/dst/%s/linmap", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s/lin_list", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s/compliance_map", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

 out:
	if (fd != -1)
		close(fd);
	isi_error_handle(kept_error, error_out);
}

/**
 * Remove all of the btrees (and containing directories) for the target
 * side of the specified policy.  This function continues past errors where
 * possible, but returns the first error encountered in it's out parameter.
 */
void
siq_btree_remove_target(const char *policy, struct isi_error **error_out)
{
	char path[MAXPATHLEN];
	int fd = -1;
	int ret;
	struct isi_error *tmp_error = NULL;
	struct isi_error *kept_error = NULL;
	int tmp_errno;

	assert_policy_path_ok(policy);

	fd = sbt_open_root_dir(O_RDONLY);
	if (fd == -1) {
		tmp_errno = errno;
		kept_error = isi_system_error_new(tmp_errno,
		    "Error opening btree root");
		log(ERROR, "Error opening btree root: %s",
		    strerror(tmp_errno));
		goto out;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s/linmap", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		kept_error = tmp_error;
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s/lin_list", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		kept_error = tmp_error;
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s/compliance_map", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		kept_error = tmp_error;
		tmp_error = NULL;
	}

	snprintf(path, sizeof(path), "synciq/dst/%s", policy);
	ret = enc_unlinkat(fd, path, ENC_DEFAULT, AT_REMOVEDIR);
	if (ret != 0 && errno != ENOENT) {
		tmp_errno = errno;
		log(ERROR, "Error removing btree policy dir %s: %s",
		    path, strerror(tmp_errno));
		if (!kept_error)
			kept_error = isi_system_error_new(tmp_errno,
			    "Error removing btree policy dir %s", path);
	}

	/* BEJ: BUG 137768: attempt to remove the mirror repstate(if it exists)*/
	snprintf(path, sizeof(path), "synciq/src/%s/repstate", policy);
	siq_btree_remove_subdir(fd, path, &tmp_error);
	if (tmp_error) {
		if (!kept_error)
			kept_error = tmp_error;
		else
			isi_error_free(tmp_error);
		tmp_error = NULL;
	}

 out:
	if (fd != -1)
		close(fd);
	isi_error_handle(kept_error, error_out);
}
