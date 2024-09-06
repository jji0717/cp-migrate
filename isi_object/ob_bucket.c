/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	bucket.c __ 2011/08/31 12:03:11 PDT __ ecande
 */
/** @addtogroup ostore_private
 * @{ */
/** @file
*/

#include "ob_common.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <string.h>
#include <dirent.h>

#include <sys/param.h>

#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include <isi_util/isi_file_lock.h>

#include "bucket_common.h"
#include "ostore_internal.h"
#include "iobj_hash.h"
#include "iobj_licensing.h"

#define OSTORE_BUCKET_HASH_DEPTH 3 // the number of parts in this format string

static void
hash_bucket_name(struct fmt *fmt, const char *name, const char *shim_acct_name)
{
	uint32_t hval = 0;

	hval = iobj_hash32(name, strlen(name), 0);

	fmt_print(fmt, "%d/%d/", (0xffff & hval), (hval >> 16));

	if (shim_acct_name && strlen(shim_acct_name))
		fmt_print(fmt, "%s_", shim_acct_name);

	fmt_print(fmt, "%s", name);
}

/*
 * Generate ostore bucket name from shim input to allow for abstraction of on
 * disk naming from shim input.  Naming function only, does not look at disk.
 */
struct ibucket_name *
ob_bucket_name_create(
	struct ostore_handle *ios,
	const char *shim_account,
	const char *shim_bucketname,
	struct isi_error **error_out)
{
	struct ibucket_name *ibn = NULL;
	struct isi_error *error  = NULL;
	struct fmt FMT_INIT_CLEAN(bname);

	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	/*
	 * build the actual bucket_name
	 */
	hash_bucket_name(&bname, shim_bucketname, shim_account);

	ibn = calloc(1, sizeof(struct ibucket_name));
	ASSERT(ibn);

	ibn->bucket_name = strdup(fmt_string(&bname));
	ASSERT(ibn->bucket_name);

	ibn->shim_account = strdup(shim_account);
	ASSERT(ibn->shim_account);

	ibn->shim_bucketname = strrchr(ibn->bucket_name, '/'); // no alloc
	ASSERT(ibn->shim_bucketname);
	++ibn->shim_bucketname; //skip the /
	ibn->ostore = ios;

	IOBJ_SET_BUCKET_NAME_MAGIC(ibn);
 out:
	if (error)
		isi_error_handle(error, error_out);

	return ibn;
}

/*
 * Check to see if a bucket already exists.
 */
bool iobj_ibucket_exists(struct ibucket_name *ibn)
{
	bool exists = false;
	char bpath[MAXPATHLEN];

	snprintf(bpath, MAXPATHLEN, "%s/%s",
	    ibn->ostore->path, ibn->bucket_name);

	if (!eaccess(bpath, F_OK))
		exists = true;

	return exists;
}

/* used for bucket counting in iobj_ibucket_count() */
static bool
list_bucket_cb(void * caller_context, const char * ibucket_name)
{
	return true;
}

static struct isi_error *
iobj_ibucket_count(struct ostore_handle *ios, size_t *count)
{
	struct isi_error * error = NULL;
	size_t count_bucket = 0;

	ASSERT(ios);
	ASSERT(count);

	// count by listing.
	count_bucket = iobj_ibucket_list(ios, list_bucket_cb, NULL, &error);

	if (error == NULL)
		*count = count_bucket;

	return error;
}

/*
 * Create a bucket given the bucket identifier and properties that should be
 * assigned to the bucket.
 */
struct ibucket_handle *
ob_bucket_create(struct ibucket_name *ib, mode_t c_mode,
    const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode,
    bool recursive, bool overwrite, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ib_handle = NULL;
	struct fmt FMT_INIT_CLEAN(suffix);
	struct fmt FMT_INIT_CLEAN(tmp_bpath);
	struct fmt FMT_INIT_CLEAN(bpath);
	int ret;
	char *o_tmp_bpath = NULL, *o_bpath = NULL;
	char *tmp_bpath_end, *bpath_end;
	bool tmp_removal_needed = false;

	/* license check */
	if (iobj_is_license_valid() == false) {
		error = ostore_invalid_license_error_new("invalid license");
		goto out;
	}

	IOBJ_CHECK_BUCKET_NAME(ib, error, out);

	/*
	 * Double check that the object store path is valid
	 */
	if (eaccess(ib->ostore->path, OSTORE_STORE_ACCESS_CHECK)) {
		error = isi_system_error_new(errno,
		    "Object store path %s is not accessible",
		    ib->ostore->path);
		goto out;
	}

	// Create desired bucket path
	fmt_print(&bpath, "%s/%s", ib->ostore->path, ib->bucket_name);

	// Check preexisting bucket
	if (eaccess(fmt_string(&bpath), OSTORE_BUCKET_ACCESS_CHECK) == 0) {
		if (!(ib->ostore->params->recreate_bucket)) {
			error = ostore_bucket_exists_error_new(
			    "Bucket %s already exists",
			    ib->bucket_name);
		} else {
			ib_handle = bucket_handle_create(ib->ostore,
			    ib->bucket_name,
			    fmt_string(&bpath),	-1, attrs, &error);
		}
		goto out;
	}

	/*
	 * Check that we have not exceeded the max bucket limit
	 * Note: This is a sloppy check in that more buckets could have been
	 * added or some deleted while buckets are being counted. Doing
	 * a proper check requires a different implementation (file based
	 * flocked bucketCounter) for iobj_ibucket_count() which will be
	 * done later.
	 */
	{
		size_t curr_bucket_count;
		error = iobj_ibucket_count(ib->ostore, &curr_bucket_count);
		if (error)
			goto out;
		if (curr_bucket_count  >= ib->ostore->params->max_buckets) {
			error = ostore_exceeds_limit_error_new(
				"Max bucket limit %d reached",
				ib->ostore->params->max_buckets);
			goto out;
		}
	}

	// Create tmp bucket path
	fmt_print(&suffix, "%s", ib->bucket_name);
	create_tmp_dir_path(OBJECT_STORE_TMP, &suffix, &tmp_bpath,
	    OSTORE_BUCKET_DEFAULT_MODE, attrs,
	    OSTORE_ACL_NONE, OSTORE_ACL_BUCKET,
	    NULL, &error);
	//printf("\nbucket_delete: tmp_bpath is %s\n", fmt_string(&tmp_bpath));
	if (error)
		goto out;
	tmp_removal_needed = true;

	// Move tmp bucket to actual location
	{
		o_tmp_bpath = fmt_detach(&tmp_bpath);
		o_bpath = fmt_detach(&bpath);
		size_t suffix_len = strlen(fmt_string(&suffix));

		ASSERT(suffix_len);
		// take shared ostore lock
		struct isi_file_lock SCOPED_SH_FILE_LOCK(ml, ib->ostore->path,
							&error);

		if (error)
			goto out;


		char *tmp_bpath_str = o_tmp_bpath + strlen(o_tmp_bpath) -
					suffix_len + 1;
		char *bpath_str = o_bpath + strlen(o_bpath) - suffix_len;

		// traverse the path to be created downwards and try to
		// create it. stop at first success.
		do {
			tmp_bpath_end = strchr(tmp_bpath_str, '/');
			if (tmp_bpath_end)
				*tmp_bpath_end = '\0';
			bpath_end = strchr(bpath_str, '/');
			if (bpath_end)
				*bpath_end = '\0';
			ret = rename(o_tmp_bpath, o_bpath);
			/* if (ret) {
				printf ("\nrename %s to %s failed, errno=%d\n",
				    o_tmp_bpath, o_bpath, errno);
				perror(0);
			} */
			if (bpath_end) {
				*bpath_end = '/';
				bpath_str = bpath_end + 1;
			}
			if (ret == 0)
				break;
			if (tmp_bpath_end) {
				*tmp_bpath_end = '/';
				tmp_bpath_str = tmp_bpath_end + 1;
			}
			if (ret != 0) {
				if (errno != ENOTEMPTY) {
					error = isi_system_error_new(errno,
					"Cannot create bucket %s", ib->bucket_name);
					goto out;
				}
			}
		} while (tmp_bpath_end);
	}

	// check if error is because the bucket already exists and has object
	if (ret != 0) {
		ASSERT(errno == ENOTEMPTY);
	} else {
		// set unused tmp bucket path (one before success element)
		tmp_bpath_end = strrchr(o_tmp_bpath, '/');
		ASSERT(tmp_bpath_end);
		*tmp_bpath_end = '\0';
	}

	// on success add to cache
	ib_handle = bucket_handle_create(ib->ostore,
	    ib->shim_bucketname, o_bpath, -1, attrs,
	    &error);

 out:
	// remove unused tmp bucket path
	if (tmp_removal_needed)
		remove_path_until(o_tmp_bpath, OBJECT_STORE_TMP, &error);

	if (error)
		isi_error_handle(error, error_out);
	if (o_tmp_bpath)
		free(o_tmp_bpath);
	if (o_bpath)
		free(o_bpath);

	return ib_handle;
}

/*
 * Open an existing bucket.  If the bucket doesn't exist, return an error
 * distinctive from any type of access error.
 */
struct ibucket_handle *
ob_bucket_open(struct ibucket_name *ibn,
    enum ifs_ace_rights bucket_ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ib_handle = NULL;
	struct fmt FMT_INIT_CLEAN(bucket_path);

	IOBJ_CHECK_BUCKET_NAME(ibn, error, out);

	/*
	 * Generate the pathname for the bucket
	 */
	fmt_print(&bucket_path, "%s/%s", ibn->ostore->path, ibn->bucket_name);

	ib_handle = bucket_open(ibn, fmt_string(&bucket_path),
	    bucket_ace_rights_open_flg, flags, cf_flags, &error);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return ib_handle;
}

/*
 * Close the bucket
 */
bool
ob_bucket_close(struct ibucket_handle *ib, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	bool succeed = false;

	IOBJ_CHECK_BUCKET_HANDLE(ib, error, out);

	bucket_handle_destroy(ib);
	succeed = true;

 out:
	if (error)
		isi_error_handle(error, error_out);

	return succeed;
}

/** A bucket dir is non-empty if it has more than the following entries:
 * 		.
 * 		..
 * 		[level!=0] child dir
 *  Otherwise it is considered empty.
 */
static bool
iobj_ibucket_empty(const char * bucket_dir_name, int level,
				struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	DIR *dp;
	struct dirent curr_ent;
	struct dirent *ep = 0;
	int count_entries = 0;
	bool empty_bucket = false;
	int ret;

	dp = opendir(bucket_dir_name);
	if (!dp) {
		error = isi_system_error_new(errno,
		        "opendir() error at %s",  bucket_dir_name);
		goto out;
	}

	// check for entries
	while((ret = readdir_r(dp, &curr_ent, &ep)) == 0) {
		if (ep == NULL) { // end of dir
			empty_bucket = true;
			goto out;
		}
		++count_entries;
		if ((level == 0) && count_entries > 2) {
			goto out;
		}
		if (count_entries > 3) {
			goto out;
		}
	}
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "readdir_r() error at %s", bucket_dir_name);
		goto out;
	}


 out:
	if (dp)
		closedir(dp);

	if (error)
		isi_error_handle(error, error_out);

	return empty_bucket;
}

/**
 *  Remove an "empty" (see above) bucket
 */
static struct isi_error *
iobj_ibucket_remove(struct ibucket_name *ibn)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(bucket_path);
	struct fmt FMT_INIT_CLEAN(test_path);
	struct fmt FMT_INIT_CLEAN(tmp_path);
	struct fmt FMT_INIT_CLEAN(suffix);
	const char *path;
	char *path_end = 0;
	int level = 0, ret;
	size_t ostore_path_len = 0, new_bucket_path_len = 0;
	bool tmp_removal_needed = false, path_empty = false;

	ostore_path_len = strlen(ibn->ostore->path);
	fmt_print(&bucket_path, "%s/%s", ibn->ostore->path, ibn->bucket_name);
	fmt_print(&test_path, "%s", fmt_string(&bucket_path));
	path = fmt_string(&test_path);
	new_bucket_path_len = strlen(path);

	{
		// Lock ostore for deletion.  Exclusive lock ensures
		// only single caller can go through at one time
		struct isi_file_lock SCOPED_EX_FILE_LOCK(
			lo, ibn->ostore->path, &error);
		if (error)
			goto out;

		/*
		 * Check to see if the bucket exists and that we can read,
		 * write and traverse through it.
		 */
		if (eaccess(path, OSTORE_BUCKET_ACCESS_CHECK)) {
			switch(errno){
			case ENOENT:
				error = ostore_object_not_found_error_new(
					"Bucket %s does not exist",
					ibn->bucket_name);
				break;
			default:
				error = isi_system_error_new(errno,
				    "Cannot access bucket %s",
				    ibn->bucket_name);
			}
			goto out;
		}

		{
			// Lock bucket for deletion.  Exclusive lock ensures
			// new objects cannot be added while we are in the
			// process of deleting this bucket
			struct isi_file_lock SCOPED_EX_FILE_LOCK(
				lb, path, &error);
			if (error)
				goto out;

			// find the longest empty bucket path
			do {
				path_empty = iobj_ibucket_empty(path,
				    level, &error);
				// printf ("path %s is  %d\n", path, path_empty);
				if (error)
					goto out;
				if (!path_empty ||
				    (new_bucket_path_len <= ostore_path_len))
					break;

				path_end = strrchr(path, '/');
				new_bucket_path_len = path_end - path;
				{
					struct fmt_scan_blotter blot;

					fmt_scan_blot_init(&blot);
					fmt_scan_blot_ahead(&test_path, &blot,
					    new_bucket_path_len);
				}
				level++;
			} while (1);

			if (level == 0) {
				error = ostore_object_exists_error_new(
					"Bucket %s is not empty",
					ibn->bucket_name);
				goto out;
			}
			*path_end = '/';

			// printf("longest_empty_path is %s\n", path);

			// create a tmp path to move the longest empty path to
			create_tmp_dir_path(OBJECT_STORE_TMP,
			    &suffix, &tmp_path,
			    OSTORE_BUCKET_DEFAULT_MODE, NULL, OSTORE_ACL_NONE,
			    OSTORE_ACL_NONE, NULL, &error);
			if (error)
				goto out;
			tmp_removal_needed = true;

			// move the longest empty path to tmp path
			ret = rename(path, fmt_string(&tmp_path));
			if (ret) {
				error  = isi_system_error_new(errno,
					     "rename (%s to tmp)  failed",
					     path);
				goto out;
			}

			// create the complete tmp path to remove
			fmt_print(&tmp_path, "%s", fmt_string(&bucket_path) +
			    strlen(fmt_string(&test_path)));
		}
	}

out:

	if (tmp_removal_needed) {
		int ret;

		char * tmp_path_str = fmt_detach(&tmp_path);
		ret = remove_path_until(tmp_path_str, OBJECT_STORE_TMP,
		    &error);
		if (ret) {
			ilog(IL_NOTICE, "Failed to remove path %s errno=%d",
			    tmp_path_str, ret);
		}

		free(tmp_path_str);
	}
	return error;
}

/*
 * Delete an existing bucket. Allow for the caller to determine the policy as
 * to whether a bucket can be deleted if there are any objects/containers that
 * exist in that bucket.  If the bucket has containers, does the policy need
 * to be expanded beyond a boolean such as handling the case of containers
 * existing without objects differently than containers and objects?
 */
struct isi_error *
ob_bucket_delete(struct ibucket_name *ibn, bool require_empty)
{
	struct isi_error *error = NULL;

	IOBJ_CHECK_BUCKET_NAME(ibn, error, out);

	/* attempt to remove the bucket */
	error = iobj_ibucket_remove(ibn);
	if (error)
		goto out;

 out:
	return error;
}


#define BUCKET_DIR_LEVEL  2
/**
 * Recursively descend bucket directories and return bucket names
 *
 * Note: Since this operation can potentially take a long time, we do it
 * without any kind of locking.  The consequence
 * of doing that is that file system entries (directories and files) comprising
 * the bucket heirarchy could have been removed (errno=ENOENT) while this
 * operation is in progress.  Hence it has been made capable of handling such
 * scenarios.
 */
static size_t
list_bucket(int dir_fd, const char *rel_path, const char *abs_path, int level,
    bucket_list_cb caller_cb, void *caller_context, bool *list_continue_out,
    struct isi_error **error_out)
{
	DIR *dp = NULL;
	struct dirent curr_ent;
	struct dirent *ep = 0;
	size_t count = 0;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt_curr_path);
	const char *curr_path = NULL;
	bool list_continue = true;

	int ret, fd;
	bool self_found = false, parent_found = false;
	bool gc_found = false;
	bool gclk_found = false;

	if (level)
		fmt_print(&fmt_curr_path, "%s/%s", abs_path, rel_path);
	else
		fmt_print(&fmt_curr_path, abs_path);
	curr_path = fmt_string(&fmt_curr_path);

	// Open dir
	fd = enc_openat(dir_fd, rel_path, ENC_DEFAULT, O_RDONLY);
	if (fd < 0) {
		if (errno == ENOTDIR)
			/*
			 * log the presence of an unexpected entry in a
			 * directory.
			 */
			ilog(IL_NOTICE,
			    "Found unexpected entry %s during bucket list, "
			    "expected directory",
			    curr_path);
		else if (errno != ENOENT && errno != EACCES)
			error = isi_system_error_new(errno,
			    "enc_openat() error at %s",  curr_path);
		goto out;
	}
	dp = fdopendir(fd);
	if (dp == NULL) {
		if (errno != ENOENT && errno != EACCES)
			error = isi_system_error_new(errno,
			    "fdopendir() error at %s",  curr_path);
		goto out;
	}

	// Process entries
	while ((ret = readdir_r(dp, &curr_ent, &ep)) == 0) {

		if (ep == NULL || !list_continue) {
			// no more entries or need to list
			goto out;
		}

		if (ep->d_type != DT_DIR) {
			if (!gc_found && strcmp(ep->d_name,
				OSTORE_OSTORE_CONFIG_NAME) == 0) {
				gc_found = true;
			} else if (!gclk_found && strcmp(ep->d_name,
				OSTORE_OSTORE_CONFIG_LK_NAME) == 0) {
				gclk_found = true;
			} else {
				ilog(IL_NOTICE,
				    "Found unexpected entry %s/%s during "
				    "bucket list, expected a directory",
				    curr_path, ep->d_name);
				continue;
			}
		} else if (!self_found && strcmp(ep->d_name, ".") == 0) {
			self_found = true;
		} else if (!parent_found && strcmp(ep->d_name, "..") == 0) {
			parent_found = true;
		} else if (level < BUCKET_DIR_LEVEL) {
			// descend
			count += list_bucket(fd, ep->d_name, curr_path,
			    level + 1, caller_cb, caller_context,
			    &list_continue, &error);
			if (error)
				goto out;
		} else if (level == BUCKET_DIR_LEVEL) {
			count++;
			// return the true bucket name
			list_continue = (*caller_cb)(caller_context,
			    ep->d_name);
		} else {
			ASSERT(0, "list bucket reached level %d", level);
		}
	}
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "readdir_r() error at %s", curr_path);
		goto out;
	}

out:
	// Close dir
	if (dp) {
		if (closedir(dp)) {
			error = isi_system_error_new(errno,
			    "closedir() error at %s", curr_path);
		}
	} else if (fd > -1) {
		close(fd);
	}

	if (error)
		isi_error_handle(error, error_out);

	*list_continue_out = list_continue;

	return count;
}


size_t
iobj_ibucket_list(struct ostore_handle *ios,
		 bucket_list_cb caller_cb,
		 void *caller_context,
		 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t count = 0;
	bool list_continue = true;

	IOBJ_CHECK_STORE_HANDLE(ios, error, out);
	// Arg check
	ASSERT(error_out);
	if (!ios || !(ios->path) || !caller_cb) {
		error = ostore_invalid_argument_error_new(
		    "invalid args:iobj_object_list");
		goto out;
	}

	count = list_bucket(AT_FDCWD, ios->path, ios->path, 0,
	    caller_cb, caller_context, &list_continue, &error);
 out:
	if (error)
		isi_error_handle(error, error_out);
	return count;
}

#if 0
/**
 * Get the specified properties.  If property names are specified they will be
 * returned in the order specified.  If no properties are specified, retrieve
 * them all.
 */
struct isi_error *
iobj_ibucket_prop_get(struct ibucket_handle *ibh,
    struct ios_keys *keys,
    struct ios_kvps **attrs);
#endif /* end of the TBD */

/**
 * Dump the internal cache of open buckets
 */
void
iobj_ibucket_dump(void)
{
	printf("Bucket dump not supported.\n");
}
/** @} */
