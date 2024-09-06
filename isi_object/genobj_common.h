/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	genobj_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __GENOBJ_COMMON_H__
#define __GENOBJ_COMMON_H__

#include "store_provider.h"

int genobj_list_ext_attrs(int fd,
    struct iobj_object_attrs **attr_list,
    struct isi_error **error_out);

/**
 * Set attributes (user namespace only) for object
 * @param[IN] fd - object file descriptor
 * @param[IN] attr_list - list of attributes
 * @param[OUT] error_out - error if copy fails
 */
void genobj_attributes_set(int fd, const struct iobj_object_attrs *attr_list,
     struct isi_error **error);

void genobj_attribute_set(struct igenobj_handle *iobj, const char *key,
    void *value, size_t value_length, struct isi_error **error);

void genobj_attribute_get(struct igenobj_handle *iobj, const char *key,
    void **value, size_t *value_length, struct isi_error **error);

void genobj_attribute_delete(struct igenobj_handle *iobj, const char *key,
    struct isi_error **error);

void genobj_attribute_erase(int fd, struct isi_error **error_out);

void genobj_attribute_release(void *data);

/**
 * List attributes (and values) for an object. Use
 * iobj_object_attribute_list_release() to free the space allocated for the
 * value by this routine to avoid memory leak. The macros
 * IOBJ_OBJECT_ATTR_KEY() and IOBJ_OBJECT_ATTR_VALUE() can be used to help
 * extract the name and value from the attribute list.
 *
 * @param iobj handle for previously opened object
 * @param list pointer to iobj_object_attrs listing of the attributes.
 * @param error_out isi_error structure, NULL if no error, set if error found
 * @returns count of number of attributes returned in list.
 */
int genobj_attribute_list(struct igenobj_handle *iobj,
    struct iobj_object_attrs **list, struct isi_error **error);


/**
 * Return the system attributes (stats) for an object.  The data returned is
 * the stat structure as returned from fstat on the object file.
 * @param iobj handle as returned from object create or object open calls
 * @param stat pointer to a stat structure to be filled in by this call
 * @param error pointer to an isi_error structure, NULL on success, set on fail
 */
void genobj_system_attributes_get(struct igenobj_handle *iobj,
    struct stat *stat, struct isi_error **error);

/**
 * Close a generic generic object
 */
void genobj_close(struct igenobj_handle *igobj, struct isi_error **error);

/**
 * Given a generic object, get the name
 */
const char *genobj_get_name(struct igenobj_handle *igobj,
    struct isi_error **error);

#endif /* __GENOBJ_COMMON_H__ */
