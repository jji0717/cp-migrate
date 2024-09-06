/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ns_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __NS_COMMON_H__
#define __NS_COMMON_H__

/**
 * Private headers common to namespace store implementation
 */

#include "ostore.h"
#include "common.h"
#include "genobj_acl.h"

#define DEFAULT_NS       "ifs"
#define DEFAULT_NS_PATH  "/ifs"

struct ns_list_obj_param {
	/** parameters given by the client */
	struct iobj_list_obj_param *user_param;
	/** internal state */
	int current_depth; // the current depth in the traverse.
	int fd; // the parent folder fd
	const struct iobj_list_obj_info *container;
	bool list_continue;
	bool skip_system_dir;
	uint64_t resume_cookie;

};

/*
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param list_param - ns_list_obj_param structure control list behavior
 *  @param error_out - isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed
 */
size_t nsp_object_list(struct ns_list_obj_param *list_param,
    struct isi_error **error_out);

/**
 * List namespace stores
 *
 * @param acct_name  - account name
 * @param param - iobj_list_obj_param structure controls list behavior
 * @param error	- error status returned on failure
 * @return number of objects enumerated
 */
size_t ns_store_list(const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error);

/**
 * Create a new namespace store. This will perform all operations needed to
 * initialize any initial persistent information and then perform the
 * iobj_init function to initialize in memory requirements as well.
 * @param store_name	Name to be assigned to the object store to be created
 * @param ns_path       The namespace file system path.
 * @param error	Error status returned on failure
 */
struct ostore_handle *ns_store_create(
    const char *store_name,
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
struct ostore_handle *ns_store_open(
    const char *storename, uint64_t snapshot, 
    struct isi_error **error);

/**
 * Close the object store
 * @param ios	Handle for the open object store
 * @param error Error status returned on failure
 */
void ns_store_close(struct ostore_handle *ios, struct isi_error **error_out);

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
void ns_store_attributes_get(struct ostore_handle *ios, 
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
void ns_store_attributes_set(struct ostore_handle *ios, 
    struct iobj_ostore_attrs *attrs, struct isi_error **error);

/**
 * Release the attributes structure returned by iobj_ostore_attributes_get
 * once it is no longer needed. 
 */
void ns_store_attributes_release(struct iobj_ostore_attrs *attrs);

void ns_store_parameters_release(struct ostore_parameters *params);

void ns_store_parameters_set(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out);

void ns_store_parameters_get(struct ostore_handle *ios,
    struct ostore_parameters **params, struct isi_error **error_out);
/**
 * Destroy the object store.  All information in the store will be unavailable
 * @param store_name name of the object store to be destroyed
 * @return isi_error on failure
 */
struct isi_error *ns_store_destroy(const char *store_name);

void ns_store_dump(void);

/**
 * Given a store name, resolve it to the file system path
 */
struct isi_error *ns_resolve_store_path(const char *store_name,
    struct fmt *store_path);

struct ibucket_name *ns_bucket_name_create(struct ostore_handle *ios,
    const char *shim_account, const char *shim_bucketname,
    struct isi_error **error);

/**
 * Check to see if a bucket already exists.
 */
bool ns_bucket_exists(struct ibucket_name *ibn); 

/**
 * Create a bucket given the bucket identifier and properties that should be
 * assigned to the bucket.
 */
struct ibucket_handle *ns_bucket_create(struct ibucket_name *ibn,
    mode_t c_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode, bool recursive, bool overwrite, 
    struct isi_error **error);

/**
 * Open an existing bucket.  If the bucket doesn't exist, return an error
 * distinctive from any type of access error.
 */
struct ibucket_handle *ns_bucket_open(struct ibucket_name *ibn,
    enum ifs_ace_rights ns_bucket_ace_rights_open_flg, int flags, 
    struct ifs_createfile_flags cf_flags, struct isi_error **error);

/**
 * Close a previously opened bucket.
 */
bool ns_bucket_close(struct ibucket_handle *ibh, struct isi_error **error);

/**
 * Delete an existing bucket. Allow for the caller to determine the policy as
 * to whether a bucket can be deleted if there are any objects/containers that
 * exist in that bucket.  If the bucket has containers, does the policy need
 * to be expanded beyond a boolean such as handling the case of containers
 * existing without objects differently than containers and objects?
 */
struct isi_error *ns_bucket_delete(struct ibucket_name *ibn,
    bool require_empty);

/**
 * List the buckets in the object store.  The caller provides a callback that is
 * called for each bucket.  The execution happens in the callers context.
 *
 *  @param ios handle for a currently opened object store
 *  @param caller_cb a function that is called with the name of each bucket
 *  @param caller_context an argument that the caller passes and
 *      which is returned through the callback
 *  @param error isi_error stucture, NULL on success, set on failure.
 * 
 *  @return number of buckets listed
 * 
 */
size_t ns_bucket_list(struct ostore_handle *ios, bucket_list_cb caller_cb,
    void *caller_context, struct isi_error **error_out);

/**
 * Get the specified properties.  If property names are specified they will be
 * returned in the order specified.  If no properties are specified, retrieve
 * them all.
 */
struct isi_error *
obj_bucket_prop_get(struct ibucket_handle *ibh, void *attrs);

void ns_bucket_dump(void);

/**
 * Create an object in the bucket corresponding to the bucket handle given,
 * using @a c_mode to specify the read/write characteristics and the given
 * attributes.  On success the object handle is returned otherwise a NULL is
 * returned and error is set appropriately.  
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name name for the object to be opened
 * @param flags  not used
 * @param c_mode permission for the object
 * @param attributes list of key/value pairs that make up the attributes for
 * the object
 * @param acl ACL to be set on the object (just a string for now)
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *ns_object_create(
	struct ibucket_handle *ibh, 
	const char *iobj_name, 
	int flags, mode_t c_mode,
	struct iobj_object_attrs *attributes, 
	enum ostore_acl_mode acl_mode, 
	bool overwrite,
	struct isi_error **error);

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
struct iobject_handle *ns_object_open(struct ibucket_handle *ibh, 
    const char *iobj_name, enum ifs_ace_rights ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error);

bool ns_object_close(struct iobject_handle *iobj, struct isi_error **error);

void ns_object_commit(struct iobject_handle *iobj, struct isi_error **error);

ssize_t ns_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error); 

ssize_t ns_object_write(struct iobject_handle *iobj, off_t offset, 
    void *buf, ssize_t size, struct isi_error **error);

/**
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param param iobj_list_obj_param structure control the behavior of the list
 *  @param error isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed 
 */
size_t ns_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error);
 
/*
 * Delete an existing namespace object.
 */
struct isi_error *ns_object_delete(struct ibucket_handle *ibh,
    const char *iobj_name);

void ns_object_dump(void);

/**
 * Open an object via a generic interface.
 * @param ios The store handle
 * @param path The container/object path
 * @param flags The open flags
 * @param ace_rights_open_flg Ace rights to open the general object.
 * @param cf_flags The ifs_createfile flags
 */
struct igenobj_handle *ns_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error);

/**
 * Copy an object via a generic interface.
 *
 * The errors during copy are reported via user provided callback.
 * A single isi_error object may be generated as output @a err_out.
 *
 * @param[IN] cp_param - structure holds all parameters needed to execute copy
 * @param[OUT] err_out - isi_error object

 * @return 0 if successful; otherwise non-0
 */
int ns_genobj_copy(struct ostore_copy_param *cp_param,
    struct isi_error **err_out);

/**
 * Move source object to new destination
 *
 * @param src_ios The source store handle
 * @param src The source object path
 * @param dest_ios The target store handle
 * @param dest The target object path
 * @param[OUT] err_out - isi_error object if failed to move
 *
 * @return 0 if successful; -1 if error detected prior to move;
 *         -2 otherwise
 */
int ns_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **err_out);

/**
 * Delete a generic object.
 *
 * @param ios The store handle
 * @param path The object path
 * @param flags delete flags
 * @param[OUT] err_out - isi_error object if failed to delete.
 */
void ns_genobj_delete(struct ostore_handle *ios, const char *path, 
    struct ostore_del_flags flags, struct isi_error **error);

/**
 * Checks if the given path is system internally used and prohibited from
 * access by namespace store user.
 *
 * @param[IN] base_path - store path
 * @param[IN] dir_path - directory pathname relative to store
 * @return true if the path is prohibited from access; false otherwise
 */
bool is_prohibited_dir(const char *base_path, const char *dir_path);

/**
 * Gets WORM attributes of the namespace object
 * @param[IN] igobj_hdl - namespace general object handle.
 * @PARAM[OUT] worm_attr - WORM attribute structure to be filled
 * @PARAM[OUT] worm_dom -  WORM domain to be filled
 * @PARAM[OUT] worm_ancs - WORM ancestors to be allocated and filled (requires
 *                         free, see existing examples).
 * @PARAM[OUT] num_ancs -  Number of WORM ancestors in worm_ancs.
 * @param[OUT] error_out - the isi_error to be returned
 *                         in case of failure
 */
void ns_genobj_get_worm(struct igenobj_handle *igobj_hdl,
    struct object_worm_attr *worm_attr, struct object_worm_domain *worm_dom,
    struct object_worm_domain **worm_ancs, size_t *num_ancs,
    struct isi_error **error_out);

/**
 * Sets WORM attributes to the namespace object
 * @param[IN] iobj - namespace object handle.
 * @PARAM[IN] set_arg - contains WORM attribute(s) to set
 * @param[OUT] error_out - the isi_error to be returned
 *                         in case of failure
 */
void ns_object_set_worm(struct iobject_handle *iobj,
    const struct object_worm_set_arg *set_arg,
    struct isi_error **error_out);


void ns_genobj_get_protection(struct igenobj_handle *igobj_hdl,
    struct pctl2_get_expattr_args *ge, struct isi_error **error_out);

void ns_genobj_set_protection(struct igenobj_handle *igobj_hdl, 
    struct pctl2_set_expattr_args *se, struct isi_error **error_out);


#endif
