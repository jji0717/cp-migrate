/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	bucket_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __BUCKET_COMMON_H__
#define __BUCKET_COMMON_H__

#include "ostore.h"
#include <isi_acl/isi_sd.h>

/**
 * Private module containing bucket interfaces shared by namespace access and
 * two tier object store.
 */

/**
 * Common routine to create an in-memory bucket handle
 */
struct ibucket_handle *
bucket_handle_create(struct ostore_handle *ostore,
    const char *bucket_name, const char *bucket_path,
    int fd, const struct iobj_object_attrs *attrs,
    struct isi_error **error_out);

/**
 * Common routine to destroy an in-memory bucket handle
 */
void bucket_handle_destroy(struct ibucket_handle *handle);

/**
 * Common routine to get the bucket name
 */

const char *bucket_get_name(struct ibucket_handle *handle);

struct ibucket_handle *bucket_open(struct ibucket_name *ibn,
    const char *bucket_path, enum ifs_ace_rights bucket_ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error_out);


#endif
