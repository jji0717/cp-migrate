/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	object_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __OBJECT_COMMON_H__
#define __OBJECT_COMMON_H__

#include "ostore.h"

/**
 * Private module containing object interfaces shared by namespace access and
 * two tier object store.
 */

/**
 * Common routine to create an in-memory object handle
 */
struct iobject_handle * object_handle_create(
    struct ostore_handle *ostore,
    struct ibucket_handle *ibh,
    const char *object_name,
    const char *object_path, int fd, int dir_fd, int flags,
    const char *target_name, char *temp_name);

/**
 * Common routine to destroy an in-memory object handle
 */
void object_handle_destroy(struct iobject_handle *iobj,
    struct isi_error **error_out);

/**
 * Common routine to get system attributes of an object
 */
void object_system_attributes_get(struct iobject_handle *iobj,
    struct stat *stat, struct isi_error **error_out);

void object_attribute_get(struct iobject_handle *iobj, const char *key,
    void **value, size_t *value_size, struct isi_error **error_out);
    

    
void object_attribute_list_release(struct iobj_object_attrs *list,
    struct isi_error **error_out);
    
int object_attribute_list(struct iobject_handle *iobj,
    struct iobj_object_attrs **attr_list,
    struct isi_error **error_out);

ssize_t
object_read(struct iobject_handle *iobj, off_t offset, void * buf,
    ssize_t size, struct isi_error **error_out);

ssize_t
object_write(struct iobject_handle *iobj, off_t offset, void * buf,
    ssize_t size, struct isi_error **error_out);

#endif
