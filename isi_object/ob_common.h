/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ob_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __OB_COMMON_H__
#define __OB_COMMON_H__

/**
 * Private headers common to Two-tier object store implementation
 */

#include "ostore.h"
#include "common.h"

/**
 * List object stores
 *
 * @param acct_name  - account name
 * @param param - iobj_list_obj_param structure controls list behavior
 * @param error	- error status returned on failure
 * @return number of objects enumerated
 */
size_t ob_store_list(const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error);

/**
 * Create a new object store.  This will perform all operations needed to
 * initialize any initial persistent information and then perform the
 * iobj_init function to initialize in memory requirements as well.
 * @param store_type    The store type
 * @param storename	Name to be assigned to the object store to be created
 * @param ns_path The namespace file system path.
 * @param error	Error status returned on failure
 */
struct ostore_handle *ob_store_create(
    const char *storename,
    const char *ns_path,
    struct isi_error **error);

/**
 * Open an existing object store for use.  This initializes all in
 * memory information needed for access the store (open).  This is an
 * idempotent operation.
 * @param storename Name of the object store
 * @param snapshot  Snapshot identifier (use HEAD_SNAPID if you want
 *                  the HEAD version)
 * @param error	    Error status returned on failure
 */
struct ostore_handle *ob_store_open(
    const char *storename, uint64_t snapshot, 
    struct isi_error **error);

/**
 * Close the object store
 * @param ios	Handle for the open object store
 * @param error Error status returned on failure
 */
void ob_store_close(struct ostore_handle *ios, struct isi_error **error_out);

/**
 * Get the store related attributes that effect all buckets in the store.
 * If any attribute is to be changed for the store, then this get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then the companion set function is called to change the
 * attributes.  This allows us to conserve the default attributes if they are
 * not changed.
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
void ob_store_attributes_get(struct ostore_handle *ios, 
    struct iobj_ostore_attrs **attrs, struct isi_error **error);

/**
 * Set the store related attributes that effect all buckets in the store.
 * If any attribute is to be changed for the store, then the get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then then this is called to change the attributes.  This
 * allows us to conserve the default attributes if they are not changed. 
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
void ob_store_attributes_set(struct ostore_handle *ios, 
    struct iobj_ostore_attrs *attrs, struct isi_error **error);

/**
 * Release the attributes structure returned by iobj_ostore_attributes_get
 * once it is no longer needed. 
 */
void ob_store_attributes_release(struct iobj_ostore_attrs *attrs);

void ob_store_parameters_release(struct ostore_parameters *params);

void ob_store_parameters_set(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out);

void ob_store_parameters_get(struct ostore_handle *ios,
    struct ostore_parameters **params, struct isi_error **error_out);
/**
 * Destroy the object store.  All information in the store will be unavailable
 * @param store_name name of the object store to be destroyed
 * @return isi_error on failure
 */
struct isi_error *ob_store_destroy(const char *store_name);

void ob_store_dump(void);


struct ibucket_name *ob_bucket_name_create(struct ostore_handle *ios,
    const char *shim_account, const char *shim_bucketname,
    struct isi_error **error);

/**
 * Check to see if a bucket already exists.
 */
bool ob_bucket_exists(struct ibucket_name *ibn); 

/**
 * Create a bucket given the bucket identifier and properties that should be
 * assigned to the bucket.
 */
struct ibucket_handle *ob_bucket_create(struct ibucket_name *ibn,
    mode_t c_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode, bool recursive, bool overwrite, 
    struct isi_error **error);

/**
 * Open an existing bucket.  If the bucket doesn't exist, return an error
 * distinctive from any type of access error.
 */
struct ibucket_handle *ob_bucket_open(struct ibucket_name *ibn,
    enum ifs_ace_rights bucket_ace_rights_open_flg, 
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error);

/**
 * Close a previously opened bucket.
 */
bool ob_bucket_close(struct ibucket_handle *ibh, struct isi_error **error);

/**
 * Delete an existing bucket. Allow for the caller to determine the policy as
 * to whether a bucket can be deleted if there are any objects/containers that
 * exist in that bucket.  If the bucket has containers, does the policy need
 * to be expanded beyond a boolean such as handling the case of containers
 * existing without objects differently than containers and objects?
 */
struct isi_error *ob_bucket_delete(struct ibucket_name *ibn,
    bool require_empty);

/**
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
size_t ob_bucket_list(struct ostore_handle *ios, bucket_list_cb caller_cb,
    void *caller_context, struct isi_error **error_out);

/**
 * Get the specified properties.  If property names are specified they will be
 * returned in the order specified.  If no properties are specified, retrieve
 * them all.
 */
struct isi_error *
obj_bucket_prop_get(struct ibucket_handle *ibh, void *attrs);

/**
 * Get and set acls on a bucket.  ACLs might be considered just another
 * property by a protocol, but we will handle them separately in the ostore
 * layer.
 */
struct isi_error *
ob_bucket_acl_set(struct ibucket_handle *ibh, struct ifs_security_acl *acl);

struct isi_error *
ob_bucket_acl_get(struct ibucket_handle *ibh, struct ifs_security_acl **acl);

void ob_bucket_dump(void);

/**
 * Create an object in the bucket corresponding to the bucket handle given,
 * using @a c_mode to specify the read/write characteristics and the given
 * attributes. On success the object handle is returned otherwise a NULL is
 * returned and error is set appropriately.
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name name for the object to be opened
 * @param flags specifying whether or not detecting collision of internal
 *              object hashed name
 * @param c_mode permission for the object
 * @param attributes list of key/value pairs that make up the attributes for
 *              the object
 * @param acl ACL to be set on the object (just a string for now)
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *ob_object_create(
    struct ibucket_handle *ibh,
    const char *iobj_name,
    int flags, mode_t c_mode,
    struct iobj_object_attrs *attributes,
    enum ostore_acl_mode acl_mode,
    bool overwrite, struct isi_error **error);

/**
 * Open an object in the bucket corresponding to the bucket handle give, using
 * the flags to specify the read/write characteristics.  On success the object
 * handle is returned otherwise a NULL is returned and error is set
 * appropriately. 
 * @param ibh bucket handle for a currently opened bucket
 * @param name name for the object to be opened
 * @param flags flags specifying read or write along with characteristics of
 * those operations
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *ob_object_open(struct ibucket_handle *ibh, 
    const char *iobj_name,  enum ifs_ace_rights ace_rights_open_flg, 
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error);

bool ob_object_close(struct iobject_handle *iobj, struct isi_error **error);

void ob_object_commit(struct iobject_handle *iobj, struct isi_error **error);

ssize_t ob_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error); 

ssize_t ob_object_write(struct iobject_handle *iobj, off_t offset, 
    void *buf, ssize_t size, struct isi_error **error);

/**
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param param iobj_list_obj_param structure control the behavior of the list
 *  @param error isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed 
 */
size_t ob_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error);
 
struct isi_error *ob_object_delete(struct ibucket_handle *ibh,
    const char *iobj_name);

/**
 * Open an object via a generic interface.
 * @param[IN] ios -The store handle
 * @param[IN] path - The container/object path
 * @param[IN] flags - The open flags
 * @param[IN] ace_rights_open_flg -
 * @param[IN] cf_flags - The ifs_createfile flags
 * @return handle to the opened object
 */
struct igenobj_handle *ob_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error);

void ob_object_dump(void);

/**
 * Copy an object via a generic interface.
 *
 * The errors during copy are reported via user provided callback.
 * A single isi_error object may be generated as output @a err_out.
 *
 * @param[IN] cp_param - structure holds all parameters needed to execute copy
 * @param[OUT] err_out - isi_error object
 *
 * @return 0 if successful; otherwise non-0
 */
int ob_genobj_copy(struct ostore_copy_param *cp_param,
    struct isi_error **err_out);

/**
 * Move an source object to new destination
 *
 * @param src_ios The source store handle
 * @param src The source object path
 * @param dest_ios The target store handle
 * @param dest The target object path
 * @param[OUT] err_out - isi_error object if failed to move
 *
 * @return 0 if successful; 1 if found source not exists or destination
 *         exists before starting to move; or -1 otherwise
 */
int ob_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **error);


/**
 * Delete a generic generic object
 */
void ob_genobj_delete(struct ostore_handle *ios, const char * path, 
    struct ostore_del_flags flags, struct isi_error **error);

#endif
