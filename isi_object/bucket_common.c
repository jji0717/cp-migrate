/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	bucket_common.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "bucket_common.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include <sys/extattr.h>
#include <isi_ufp/isi_ufp.h>

#include <ifs/ifs_userattr.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_file_lock.h>

#include "ostore_internal.h"

void
bucket_handle_destroy(struct ibucket_handle *handle)
{
	if (handle) {
		if (handle->base.fd != -1) {
			close(handle->base.fd);
			handle->base.fd = -1;
		}

		if (handle->base.name)
			free(handle->base.name);
		if (handle->base.path)
			free(handle->base.path);
		if (handle->attrs)
			free(handle->attrs);
		IOBJ_CLEAR_BUCKET_HANDLE_MAGIC(handle);
		free(handle);
	}
}

struct ibucket_handle *
bucket_handle_create(struct ostore_handle *ostore,
    const char *bucket_name, const char *bucket_path,
    int fd, const struct iobj_object_attrs *attrs,
    struct isi_error **error_out)
{
	struct ibucket_handle *handle = NULL;

	handle = calloc(1, sizeof(struct ibucket_handle));
	ASSERT(handle);

	handle->base.object_type = OOT_CONTAINER;
	handle->base.ostore = ostore;
	handle->base.fd = fd;

	if (bucket_path) {
		handle->base.path = strdup(bucket_path);
		ASSERT(handle->base.path);
	}

	if (bucket_name) {
		handle->base.name = strdup(bucket_name);
		ASSERT(handle->base.name);
	}

	IOBJ_SET_BUCKET_HANDLE_MAGIC(handle);

	return handle;
}

/*
 * Check to see if the bucket exists and that we can read, write
 * and traverse through it.
 */
struct ibucket_handle *
bucket_open(struct ibucket_name *ibn,
    const char *bucket_path, enum ifs_ace_rights bucket_ace_rights_open_flg, 
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error_out)
{
	struct ibucket_handle *ib_handle = NULL;
	struct isi_error *error = NULL;
	int fd = -1;
    struct ifs_createfile_ex cex = {};
	
	/* 
	 * Generate the pathname for the bucket
	 */

    cex.cf_flags = cf_flags;
	fd = ifs_createfile(-1, bucket_path, bucket_ace_rights_open_flg,
	    O_DIRECTORY, 0, &cex);

	if (fd < 0) {
		/* handle errors from access, check */
		switch (errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
			    "Bucket %s does not exist",
			    ibn->shim_bucketname);
			break;
		case EACCES:
			error = ostore_access_denied_error_new(
				"Access denied to Bucket %s",
				ibn->bucket_name);
			break;
		case EINVAL:
			error = ifs_createfile_invalid_arg_error();
			break;
		default:
			error = isi_system_error_new(errno,
			    "Cannot access bucket %s",
			    ibn->shim_bucketname);
		}
		goto out;
	}
	if ((error = ensure_object_on_onefs(fd))) {
		close(fd);
		goto out;
	}
	ib_handle = bucket_handle_create(ibn->ostore, ibn->shim_bucketname,
	    bucket_path, fd, NULL, &error);

out:
	if (error)
		isi_error_handle(error, error_out);

	return ib_handle;
}

const char *
bucket_get_name(struct ibucket_handle *handle)
{
	return handle->base.name;
}
