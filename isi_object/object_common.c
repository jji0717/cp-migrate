/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	object_common.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "object_common.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include <sys/extattr.h>

#include <ifs/ifs_userattr.h>
#include <ifs/ifs_syscalls.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_file_lock.h>

#include "iobj_licensing.h"
#include "ostore_internal.h"

void
object_handle_destroy(struct iobject_handle *iobj, struct isi_error **error_out)
{
	struct isi_error *error = NULL; // capture first error
	int ret;

	ASSERT(error_out);

	if (!iobj) {
		error = ostore_invalid_handle_error_new(
			"close of %p while not open", iobj);
		goto out;
	}

	if (iobj->base.name)
		free(iobj->base.name);
	if (iobj->base.path)
		free(iobj->base.path);
	if (iobj->base.fd != -1) {
		ret = close(iobj->base.fd);
		iobj->base.fd = -1;
 		if (ret)
			error = isi_system_error_new(errno, "close object "
			    "error");
	}

	if(iobj->dir_fd != -1) {
		ret = close(iobj->dir_fd);
		if (ret && !error)
			error = isi_system_error_new(errno,
			    "close object directory error");
		iobj->dir_fd = -1;
	}

	if (iobj->temp_obj_name)
		free(iobj->temp_obj_name);

	if (iobj->target_obj_name)
		free(iobj->target_obj_name);

	/*
	 * zap the handle here to force some misuse errors
	 */
	IOBJ_CLEAR_OBJECT_HANDLE_MAGIC(iobj);
	free(iobj);

 out:
	if (error)
		isi_error_handle(error, error_out);
}

/*
 * Allocate a new ostore entry
 */
struct iobject_handle *
object_handle_create(struct ostore_handle *ostore,
    struct ibucket_handle *ibh, const char *object_name,
    const char *object_path, int fd, int dir_fd, int flags,
    const char *target_name, char *temp_name)
{
	struct iobject_handle *iobj = NULL;
	/*
	 * create the empty object handle
	 */
	iobj = calloc(1, sizeof(struct iobject_handle));
	ASSERT(iobj);

	iobj->bucket = ibh;
	iobj->base.fd = fd;
	iobj->base.ostore = ostore;
	iobj->base.object_type = OOT_OBJECT;
	iobj->dir_fd = dir_fd;
	iobj->base.flags = flags;
	iobj->base.path = strdup(object_path);
	ASSERT(iobj->base.path);
	iobj->base.name = strdup(object_name);
	ASSERT(iobj->base.name);
	if (target_name) {
		iobj->target_obj_name = strdup(target_name);
		ASSERT(iobj->target_obj_name);
	}
	if (temp_name)
		iobj->temp_obj_name = temp_name; // already a copy

	iobj->c_mode = (mode_t) -1;
	iobj->acl_mode = OSTORE_ACL_NONE;

	IOBJ_SET_OBJECT_HANDLE_MAGIC(iobj);

	return iobj;
}

ssize_t
object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error_out)
{
	char *bufptr = NULL;
	off_t new_pos = -1;
	ssize_t total_read = -1;
	ssize_t bytes_read = 0;
	ssize_t bytes_left = size;/* initialize how much data we need to read */

	struct isi_error *error = NULL;

	ASSERT(error_out);
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	if (!buf) {
		error = ostore_invalid_argument_error_new(
		    "No input buffer was specified for reading from "
		    "object %s in path %s",
		    iobj->base.name, iobj->base.path);
		goto out;
	}

	if (size < 0) {
		error = ostore_invalid_argument_error_new(
		    "Amount of data to read was invalid (negative amount?) "
		    "for object %s in path %s",
		    iobj->base.name, iobj->base.path);
		goto out;
	}
	/*
	 * check to see that the offset seems valid
	 */
	if (offset < 0) {
		error = ostore_invalid_argument_error_new(
		    "Offset specified is before the beginning of the "
		    "object %s in path %s",
		    iobj->base.name, iobj->base.path);
		goto out;
	}

	/*
	 * position the offset specified in the arg list relative to the
	 * beginning of the object.
	 */
	if ((new_pos = lseek(iobj->base.fd, offset, SEEK_SET)) == (off_t)-1) {
		error = isi_system_error_new(errno, 
		    "Failed to seek to offset %lld for reading from "
		    "object %s in path %s",
		    offset, iobj->base.name, iobj->base.path);
		goto out;
	}

	/*
	 * read the data given taking into account that we may have to chunk
	 * the data in.
	 */
	bufptr = buf;
	bytes_left = size;
	while (bytes_left > 0) {
		bytes_read = read(iobj->base.fd, bufptr, bytes_left);
		if (bytes_read == -1) {
			error = isi_system_error_new(errno, 
			    "Failed to read %zu bytes starting at offset %lld "
			    "for object %s in path %s",
			    bytes_left, new_pos, iobj->base.name,
			    iobj->base.path);
			goto out;
		}
		if (bytes_read == 0) {
			/*
			 * hit end of file for object 
			 */
			break;
		}
		/* keep track of where we are and how much we have left to do */
		new_pos += bytes_read; /* just for our info */
		bytes_left -= bytes_read;
		bufptr += bytes_read;
	}
	total_read = size - bytes_left;

 out:
	if (error)
		isi_error_handle(error, error_out);

	return total_read;
}

ssize_t
object_write(struct iobject_handle *iobj, off_t offset, void * buf,
    ssize_t size, struct isi_error **error_out)
{
	char *bufptr = NULL;
	off_t new_pos = -1;
	ssize_t total_written = -1;
	ssize_t bytes_written = 0;
	ssize_t bytes_left = size; /* init how much data we need to write */

	struct isi_error *error = NULL;

	ASSERT(error_out);

	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	/*
	 * check to see that the offset seems valid
	 */
	if (offset < 0) {
		error = ostore_invalid_argument_error_new(
			"Offset specified is before the beginning of the "
			"object %s in bucket %s",
			iobj->base.name, iobj->bucket->base.name);
		goto out;
	}

	if (!buf) {
		error = ostore_invalid_argument_error_new(
		    "No input buffer was specified for write to "
		    "object %s in bucket %s",
		    iobj->base.name, iobj->bucket->base.name);
		goto out;
	}

	if (size < 0) {
		error = ostore_invalid_argument_error_new(
		    "Amount of data to write was invalid (negative amount?) "
		    "for object %s in bucket %s",
		    iobj->base.name, iobj->bucket->base.name);
		goto out;
	}

	/*
	 * position the offset specified in the arg list relative to the
	 * beginning of the object.
	 */
	if ((new_pos = lseek(iobj->base.fd, offset, SEEK_SET)) == (off_t)-1) {
		error = isi_system_error_new(errno,
		    "Failed to seek to offset %lld for writing to "
		    "object %s in bucket %s",
		    offset, iobj->base.name, iobj->bucket->base.name);
		goto out;
	}

	/*
	 * write the data given taking into account that we may have to chunk
	 * the data out.
	 */
	bufptr = buf;
	bytes_left = size;
	while (bytes_left > 0) {
		if ((bytes_written = write(iobj->base.fd, bufptr, bytes_left))
		    == -1) {
			error = isi_system_error_new(errno,
			    "Failed to write %zu bytes starting at offset %lld "
			    "for object %s in bucket %s",
			    bytes_left, new_pos, iobj->base.name,
			    iobj->bucket->base.name);
			goto out;
		}
		/* keep track of where we are and how much we have left to do */
		new_pos += bytes_written; /* just for our info */
		bytes_left -= bytes_written;
		bufptr += bytes_written;
	}
	total_written = size - bytes_left;

 out:
	if (error)
		isi_error_handle(error, error_out);

	return total_written;
}

