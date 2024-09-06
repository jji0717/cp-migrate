/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ns_bucket.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "ns_common.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <string.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include <isi_util/ifs_mounted.h>
#include <isi_acl/isi_acl_util.h>

#include "bucket_common.h"
#include "ostore_internal.h"
#include "genobj_acl.h"
#include "genobj_common.h"

/**
 * Utility to resolve a bucket to file system path
 */
static struct isi_error *
ns_resolve_bucket_path(struct ibucket_name *ibn, struct fmt *bucket_path)
{
	char *bname = ibn->bucket_name;

	if (!bname)
		fmt_print(bucket_path, "%s", ibn->ostore->path);
	else {
		if (is_prohibited_dir(ibn->ostore->path, bname)) {
			return ostore_pathname_prohibited_error_new(
			    "%s", bname);
		}
		fmt_print(bucket_path, "%s/%s", ibn->ostore->path, 
		    bname[0] == '/' ? bname + 1 : bname);
	}
	return NULL;
}

#define OPEN_RPATH(fd_, rel_path_, outfd_, abs_path_) 			\
	do { 								\
		int oldfd_ = fd_; 					\
		outfd_ = enc_openat(oldfd_, rel_path_, 			\
		    ENC_DEFAULT, O_RDONLY|O_NONBLOCK);			\
		if (oldfd_ != AT_FDCWD)					\
			close(oldfd_);					\
		if (outfd_ == -1) {					\
			error = isi_system_error_new(errno,		\
			    "Cannot open relative path %s", rel_path_);	\
			goto out;					\
		}							\
	} while (false);


/*
 * This routine updates the attributes and acl/mode if input by the user,
 * during put directory. This routine is called when the directory 
 * already exists.
 */

static bool
update_attrs_acl_for_existing_bucket(struct fmt *bpath, mode_t c_mode,
    const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode,
    bool overwrite, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *sd = NULL;
	int fd = -1;
	int ret = 0;
	enum ifs_security_info secinfo;
	bool bucket_processed = true;

	enum ifs_ace_rights ace_rights = 
	    IFS_RTS_DIR_WRITE_EA | IFS_RTS_STD_WRITE_DAC | 
	    IFS_RTS_DIR_READ_EA;

	/*
	 * Check if the Directory already exists.
	 * Open the directory to replace the extended attributes/acls.
	 * The open can fail due to access denied error for setting 
	 * attributes/acls.
	 */
	errno = 0;
	fd = ifs_createfile(-1, fmt_string(bpath), 
	    ace_rights, O_DIRECTORY, c_mode, NULL);
	
	// If the Directory exists and overwrite is not allowed, 
	// flag error.
	if (!overwrite && (fd >= 0 || (fd < 0 && 
	    (errno == EACCES || errno == ENOTDIR || 
	    errno == EEXIST || errno == ENOTEMPTY)))) {
		error = ostore_bucket_exists_error_new(
			"Unable to create Directory. %s already exists", 
			fmt_string(bpath));
		goto out;
	}
	else if (fd < 0) {
		switch (errno) {
		case ENOENT:
			/* 
			 * The component doesn't exists. So no processing
			 * needs to be done. This is not an error. 
			 * Exit the routine. 
			 */
			bucket_processed = false;
			goto out;
		case EACCES:
			error = ostore_access_denied_error_new(
			    "Access denied to set attributes/acls on %s", 
			    fmt_string(bpath));
			goto out;
		case ENOTDIR:
			error = isi_system_error_new(errno, 
			    "Access denied. Entity %s is not a directory", 
			    fmt_string(bpath));
			goto out;
		case ENAMETOOLONG:
			error = isi_system_error_new(errno, 
			    "Access denied, Path/File name %s too long", 
			    fmt_string(bpath));
			goto out;
		case EINVAL:
			error = ifs_createfile_invalid_arg_error();
			break;
		default:
			error = isi_system_error_new(errno, 
			    "Error while trying to open directory %s", 
			    fmt_string(bpath));
			goto out;
		}
	}

	// Proceed to set the attributes and acls.
	ASSERT(fd >= 0);

	if ((error = ensure_object_on_onefs(fd)))
		goto out;
	
	/* 
	 * Delete the old attributes and update the 
	 * user input attributes.
	 */
	genobj_attribute_erase(fd, &error);
	if (error) {
		error = isi_system_error_new(errno, 
		    "Unable to delete existing attributes of Directory %s", 
		    fmt_string(bpath));
		goto out;
	}
	
	if (attrs) {
		genobj_attributes_set(fd, attrs, &error);
		if (error)
			goto out;
	}
	
	if (prot) {
		ostore_ifs_checked_set_protection_fd(fd, prot, &error);
		if (error)
			goto out;
	}

	/*
	 * Update the acl information. Delete old acl info and 
	 * update with new acl info if specified. If acl is not 
	 * input, use the default mode.
	 * If the acl_mode is OSTORE_ACL_NONE, then the below steps
	 * will set the security descriptor to NULL.
	 */
	ostore_create_default_acl(&sd, acl_mode, NULL, &error);
	if (error) {
		goto out;
	}
	
	/* 
	 * Set the new acl info. 
	 * If secinfo is zero, the existing sd if any will be 
	 * removed.
	 */
	if (sd == NULL && acl_mode <= OSTORE_ACL_NONE) {
		secinfo = 0;
	}
	else {
		secinfo = IFS_SEC_INFO_DACL;
	}

	/* 
	 * This function call will set the mode if the 
	 * secinfo is zero. If the secinfo is specified 
	 * then the security descriptor is updated.
	 */
	ret = ifs_set_mode_and_sd(fd, SMAS_FLAGS_FORCE_SD, c_mode,
	    -1, -1, secinfo, sd);
	
	if (ret) {
		error = isi_system_error_new(errno, 
		    "Error while trying to set ACL/mode on Directory %s.",
		    fmt_string(bpath));
		goto out;
	}
	
out:
	/*
	 * Close the open fd.
	 */
	if (fd >= 0) {
		close(fd);
	}

	if (error) {
		isi_error_handle(error, error_out);		
	}
	return bucket_processed;
}

/*
 * Create a bucket given the bucket identifier and properties that should be
 * assigned to the bucket.
 */
struct ibucket_handle *
ns_bucket_create(struct ibucket_name *ibn, mode_t c_mode,
    const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode,
    bool recursive, bool overwrite, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ib_handle = NULL;
	struct fmt FMT_INIT_CLEAN(spath);
	struct fmt FMT_INIT_CLEAN(bpath);

	//Generate the pathname for the bucket
	error = ns_resolve_bucket_path(ibn, &bpath);
	if (error)
		goto out;

	// Update the attributes/acl if the bucket/directory,
	// already exists.
	bool processed = update_attrs_acl_for_existing_bucket(&bpath,
	    c_mode, attrs, prot, acl_mode, overwrite, &error);
	if (error || processed)
		goto out;

	// If acl is specified as header then use that acl
	// during bucket/directory creation.

	if (!recursive) {
		const char *ndir = strrchr(ibn->bucket_name, '/');
		if (ndir) {
			fmt_print(&spath, "%s/%.*s", ibn->ostore->path,
			    (int)(ndir - ibn->bucket_name), ibn->bucket_name);
			++ndir;
		} else {
			fmt_print(&spath, "%s", ibn->ostore->path);
			ndir = ibn->bucket_name;
		}

		// If acl is specified during bucket creation, 
		// set the acl as required. 
		mkdir_dash_p(&spath, ndir,
		    c_mode, attrs, prot, acl_mode, acl_mode,
		    NULL, NULL, &error);

	} else {
		// We think preserve ACL inheriting feature
		// is more important to user, thus we create dirs in place
		// without using the temp dir trick to get atomic behavior.
		fmt_print(&spath, "%s", ibn->ostore->path);
		mkdir_dash_p(&spath, ibn->bucket_name, c_mode,
		    attrs, prot, OSTORE_ACL_NONE, acl_mode, NULL, NULL,
		    &error);
	}

	// For recursive dir creation, we may fail in the middle
	// but we prefer not to clean up what have been created because
	// it is not trivial to determine what were created by this
	// thread without introducing lock and complexity.
	if (error)
		goto out;

	// on success add to cache
	ib_handle = bucket_handle_create(ibn->ostore,
	    ibn->bucket_name, fmt_string(&bpath), -1, attrs,
	    &error);
out:
	isi_error_handle(error, error_out);
	return ib_handle;
}

/*
 * Open an existing bucket.  If the bucket doesn't exist, return an error
 * distinctive from any type of access error.
 */
struct ibucket_handle *
ns_bucket_open(struct ibucket_name *ibn,
    enum ifs_ace_rights ns_bucket_ace_rights_open_flg, 
    int flags, struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ib_handle = NULL;
	struct fmt FMT_INIT_CLEAN(bucket_path);

	/* 
	 * Generate the pathname for the bucket
	 */	
	error = ns_resolve_bucket_path(ibn, &bucket_path);
	if (!error) {
		ib_handle = bucket_open(ibn, fmt_string(&bucket_path),
		    ns_bucket_ace_rights_open_flg, flags, cf_flags, &error);
	}

	if (error)
		isi_error_handle(error, error_out);	

	return ib_handle;
}

/*
 * Close a previously opened bucket.
 */
bool
ns_bucket_close(struct ibucket_handle *ib, struct isi_error **error_out)
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

/*
 * Delete an existing bucket.
 *
 * Namespace container deletion is implemented in genobj_delete
 * which allows recursive deletion.
 */
struct isi_error *
ns_bucket_delete(struct ibucket_name *ibn, bool require_empty)
{	
	struct isi_error *error = ostore_error_new(
	    "Operation supported via genobj_delete!");
	return error;
}

/*
 * List the buckets in the object store.  The caller provides a callback that is
 * called for each bucket.  The execution happens in the callers context.
 *
 *  @param ios handle for a currently opened object store
 *  @param caller_cb a function that is called with the name of each bucket
 *  @param caller_context an argument that the caller passes and
 *      which is returned through the callback
 *  @param error isi_error structure, NULL on success, set on failure.
 * 
 *  @return number of buckets listed
 * 
 */
size_t
ns_bucket_list(struct ostore_handle *ios, bucket_list_cb caller_cb,
    void *caller_context, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
	return 0;	
}

/*
 * Get the specified properties.  If property names are specified they will be
 * returned in the order specified.  If no properties are specified, retrieve
 * them all.
 */
struct isi_error *
obj_bucket_prop_get(struct ibucket_handle *ibh, void *attrs)
{
	struct isi_error *error = ostore_error_new("Operation not supported");
	return error;	
}

void ns_bucket_dump(void)
{
	
}

/*
 * Generate ostore bucket name from shim input to allow for abstraction of on
 * disk naming from shim input.  Naming function only, does not look at disk.
 */
struct ibucket_name *
ns_bucket_name_create(
	struct ostore_handle *ios,
	const char *shim_account,
	const char *shim_bucketname,
	struct isi_error **error_out)
{
	struct ibucket_name *ibn = NULL;
	struct isi_error *error  = NULL;
	
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);
	
	ibn = calloc(1, sizeof(struct ibucket_name));
	ASSERT(ibn);

	if (shim_bucketname) {
		char *tail;
		ibn->bucket_name = strdup(shim_bucketname);
		ibn->shim_bucketname = ibn->bucket_name;
		tail = ibn->bucket_name + strlen(ibn->bucket_name) - 1;
		while (*tail == '/')
			*tail-- = '\0';
		ASSERT(ibn->bucket_name);
	}

	ibn->ostore = ios;

	IOBJ_SET_BUCKET_NAME_MAGIC(ibn);
 out:
	if (error)
		isi_error_handle(error, error_out);

	return ibn;
}
