/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	object_common.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#ifndef __STORE_PROVIDER_H__
#define __STORE_PROVIDER_H__

/**
 * This contains the interfaces for store providers.
 */	
#include "ostore.h"

/**
 * Interface to list stores
 *
 * @param acct_name  - account name
 * @param param - iobj_list_obj_param structure controls list behavior
 * @param error	- error status returned on failure
 * @return number of objects enumerated
 */
typedef size_t (*i_list_store)(
    const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error);

/**
 * Interface for creating a new object store.
 * iobj_init function to initialize in memory requirements as well.
 * @param store_type    The store type
 * @param storename	Name to be assigned to the object store to be created
 * @param ns_path The namespace store file system path. Must be null for
 * OST_OBJECT.
 * @param error	Error status returned on failure
 */
typedef struct ostore_handle *(*i_create_store)(
    const char *storename,
    const char *ns_path,
    struct isi_error **error);

/**
 * Interface for deleting a object store.
 * iobj_init function to initialize in memory requirements as well.
 * @param storename	Name to be assigned to the object store to be created
 * @return isi_error on failure
 */
typedef struct isi_error *(*i_destroy_store)(
    const char *storename);

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
typedef void (*i_get_store_attributes)(struct ostore_handle *ios, 
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
typedef void (*i_set_store_attributes)(struct ostore_handle *ios, 
    struct iobj_ostore_attrs *attrs, struct isi_error **error);

/**
 * Release the attributes structure returned by iobj_ostore_attributes_get
 * once it is no longer needed. 
 */
typedef void (*i_release_store_attributes)(struct iobj_ostore_attrs *attrs);

/**
 * Interface for opening a store.
 * @param[IN] store - the store name
 * @param[OUT] error - the error
 */
typedef struct ostore_handle *(*i_open_store)(
    const char *store,
	uint64_t snapshot,
    struct isi_error **error);

/**
 * Interface for closing a store.
 * @param[IN] store - the store to close
 * @param[OUT] error_out - the error
 */
typedef void (*i_close_store) (struct ostore_handle *store,
    struct isi_error **error);

/**
 * Interface for dumping the store
 */
typedef void (*i_dump_store)(void);

/**
 * Release the attributes structure returned by iobj_ostore_privattrs_get
 * once it is no longer needed.
 */
typedef void (*i_release_store_parameters)(struct ostore_parameters *params);

/**
 * Get the private (non-consumer) store related attributes that effect all
 * buckets in the store.  These attributes are currently meant for testing
 * purposes only.
 * If any attribute is to be changed for the store, then this get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then the companion set function is called to change the
 * attributes.  This allows us to conserve the default attributes if they are
 * not changed.
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
typedef void (*i_set_store_parameters)(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out);

/**
 * Get the private (non-consumer) store related attributes that effect all
 * buckets in the store.  These attributes are currently meant for testing
 * purposes only.
 * If any attribute is to be changed for the store, then the get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then then this is called to change the attributes.  This
 * allows us to conserve the default attributes if they are not changed.
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
typedef void (*i_get_store_parameters)(struct ostore_handle *ios,
    struct ostore_parameters **params, struct isi_error **error_out);


/**
 * Generate ostore bucket name from shim input to allow for abstraction of on
 * disk naming from shim input.  Naming function only, does not look at disk.
 */
typedef struct ibucket_name *(*i_create_bucket_name)(struct ostore_handle *ios,
    const char *shim_account, const char *shim_bucketname,
    struct isi_error **error);

/**
 * Interface for creating a bucket
 * @param[IN] ibn - bucket_name
 * @param[IN] c_mode - the mode used to create the bucket
 * @param[IN] attrs - attributes
 * @param[IN] prot - structure containing the protection property to set
 * @param[IN] acl - the security acl to set
 * @param[OUT] error_out - the error
 */
typedef struct ibucket_handle *(*i_create_bucket)(struct ibucket_name *ibn,
    mode_t c_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode, bool recursive, bool overwrite, 
    struct isi_error **error);

/**
 * Interface for opening a bucket
 * @param[IN] ibn - bucket_name
 * @param[OUT] error - the error
 */
typedef struct ibucket_handle *(*i_open_bucket)(struct ibucket_name *ibn,
    enum ifs_ace_rights bucket_ace_rights_open_flg, int flags, 
    struct ifs_createfile_flags cf_flags, struct isi_error **error);

/**
 * Interface for closing a bucket
 * @param[IN] ibh - bucket handle
 * @param[OUT] error - the error
 */
typedef bool (*i_close_bucket)(struct ibucket_handle *ibh,
    struct isi_error **error);

/**
 * Interface for deleting a bucket
 * @param[IN] ibh - bucket handle
 * @returns - the error
 */
typedef struct isi_error *
(*i_delete_bucket)(struct ibucket_name *ibn, bool require_empty);

/**
 * Interface for creating an object.
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name name for the object to be opened
 * @param flags specifying whether or not detecting collision of internal
 *              object hashed name
 * @param c_mode permission for the object
 * @param attributes list of key/value pairs that make up the attributes for
 * the object
 * @param acl ACL to be set on the object (just a string for now)
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef struct iobject_handle *(*i_create_object)(
	struct ibucket_handle *ibh,
	const char *iobj_name,
	int flags, mode_t c_mode,
	struct iobj_object_attrs *attributes,
	enum ostore_acl_mode acl_mode, bool overwrite,
	struct isi_error **error);

/**
 * Interface for opening an object.
 * @param ibh bucket handle for a currently opened bucket
 * @param name name for the object to be opened
 * @param flags flags specifying read or write along with characteristics of
 * those operations
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef struct iobject_handle *(*i_open_object)(struct ibucket_handle *ibh,
    const char *name,  enum ifs_ace_rights ace_rights_open_flg, int flags, 
    struct ifs_createfile_flags cf_flags, struct isi_error **error);

/**
 * Interface for closing an object
 * @param iobj - the object to close
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef bool (*i_close_object)(struct iobject_handle *iobj,
    struct isi_error **error);

typedef void (*i_commit_object)(struct iobject_handle *iobj,
    struct isi_error **error);

/**
 * Interface for reading object data
 * @param iobj - the object handle
 * @param offset - the offset
 * @param buf - the buffer to receive the data
 * @param size - the size of the output buffer
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef ssize_t (*i_read_object)(struct iobject_handle *iobj, off_t offset,
    void * buf, ssize_t size, struct isi_error **error);

/**
 * Interface for writing object data
 * @param iobj - the object handle
 * @param offset - the offset
 * @param buf - the buffer to receive the data
 * @param size - the size of the output buffer
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef ssize_t (*i_write_object)(struct iobject_handle *iobj, off_t offset,
    void *buf, ssize_t size, struct isi_error **error);

/**
 * Interface for deleting an object
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name the name of the object to delete
 * @param error isi_error structure, NULL on success, set on failure.
 */
typedef struct isi_error *(*i_delete_object)(struct ibucket_handle *ibh,
    const char *iobj_name);

/**
 * Interface for listing object.
 * called for each object.  The execution happens in the callers context.
 *  @param param iobj_list_obj_param structure control the behavior of the list
 *  @param error isi_error stucture, NULL on success, set on failure
 *
 *  @return number of objects listed
 */
typedef size_t (*i_list_object)(struct iobj_list_obj_param *param,
    struct isi_error **error);

typedef void (*i_set_genobj_attribute)(struct igenobj_handle *iobj,
    const char *key, void *value, size_t value_length,
    struct isi_error **error);

typedef void (*i_get_genobj_attribute)(struct igenobj_handle *iobj,
    const char *key, void **value, size_t *value_length,
    struct isi_error **error);

typedef void (*i_delete_genobj_attribute)(struct igenobj_handle *iobj,
    const char *key, struct isi_error **error);

typedef void (*i_release_genobj_attribute)(void *data);

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
typedef int (*i_list_genobj_attribute)(struct igenobj_handle *iobj,
    struct iobj_object_attrs **list, struct isi_error **error);

/**
 * Free memory allocated for attribute as returned by
 * iobj_object_attribute_list().
 *
 * @params data attribute to be freed
 * @params error isi_error struct, NULL on success, set on failure
 */
typedef void (*i_release_genobj_attribute_list)(
    struct iobj_object_attrs *attr_list,
	struct isi_error **error);

/**
 * Return the system attributes (stats) for an object.  The data returned is
 * the stat structure as returned from fstat on the object file.
 * @param iobj handle as returned from object create or object open calls
 * @param stat pointer to a stat structure to be filled in by this call
 * @param error pointer to an isi_error structure, NULL on success, set on fail
 */
typedef void (*i_get_genobj_system_attributes)(struct igenobj_handle *iobj,
    struct stat *stat, struct isi_error **error);

/**
 * Interface for opening a generic object based on the path
 * The store implementation should open the concrete object.
 */
typedef struct igenobj_handle *(*i_open_genobj)(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error);

/**
 * Interface for copying a file or dir
 * The store implementation implements the copy.
 */
typedef int (*i_copy_genobj)(struct ostore_copy_param *cp_param,
    struct isi_error **error);

/**
 * Interface for moving a file or dir
 * The store implementation implements the move operation.
 */
typedef int (*i_move_genobj)(struct ostore_handle *src_ios,
    const char *src, struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **error);

/**
 * Interface for closing a generic object based on the path
 * The store implementation should close the concrete object.
 */
typedef void (*i_close_genobj)(struct igenobj_handle *igobj,
    struct isi_error **error);

/**
 * Interface for deleting a generic object based on the path
 * The store implementation should close the concrete object.
 */
typedef void (*i_delete_genobj)(struct ostore_handle *ios, 
    const char * path, struct ostore_del_flags flags, 
    struct isi_error **error);

/**
 * Interface for getting the generic object name
 */
typedef const char *(*i_get_genobj_name)(struct igenobj_handle *igobj,
    struct isi_error **error_out);

typedef void (*i_get_genobj_worm_attr)(struct igenobj_handle *iobj,
    struct object_worm_attr *worm_attr, struct object_worm_domain *worm_dom,
    struct object_worm_domain **worm_ancs, size_t *num_ancs,
    struct isi_error **error_out);


typedef void (*i_get_genobj_protection_attr)(struct igenobj_handle *igobj_hdl, 
    struct pctl2_get_expattr_args *ge, struct isi_error **error);

typedef void (*i_set_object_protection_attr)(struct igenobj_handle *igobj_hdl, 
    struct pctl2_set_expattr_args *se, struct isi_error **error);


typedef void (*i_set_object_worm_attr)(struct iobject_handle *iobj,
    const struct object_worm_set_arg *set_arg,
    struct isi_error **error_out);

/**
 * abstract store interfaces.
 */
struct i_store_interface {
	i_list_store list_store;
	i_create_store create_store;
	i_destroy_store destroy_store;
	i_open_store open_store;
	i_close_store close_store;
	i_dump_store dump_store;
	i_get_store_attributes get_store_attributes;
	i_set_store_attributes set_store_attributes;
	i_release_store_attributes release_store_attributes;
	i_get_store_parameters get_store_parameters;
	i_set_store_parameters set_store_parameters;
	i_release_store_parameters release_store_parameters;

};

/**
 * Generic object interfaces
 */
struct i_genobj_interface {
	i_open_genobj open_genobj;
	i_close_genobj close_genobj;
	i_get_genobj_name get_genobj_name;
	i_get_genobj_attribute get_genobj_attribute;
	i_set_genobj_attribute set_genobj_attribute;
	i_delete_genobj_attribute delete_genobj_attribute;
	i_release_genobj_attribute release_genobj_attribute;
	i_list_genobj_attribute list_genobj_attribute;
	i_release_genobj_attribute_list release_genobj_attribute_list;
	i_get_genobj_system_attributes get_genobj_system_attributes;
	i_copy_genobj copy_genobj;
	i_move_genobj move_genobj;
	i_delete_genobj delete_genobj;
	i_get_genobj_worm_attr get_worm_attr;
	i_get_genobj_protection_attr get_protection_attr;
	i_set_object_protection_attr set_protection_attr;
};

/**
 * abstract bucket interfaces.
 */
struct i_bucket_interface {
	i_create_bucket_name create_bucket_name;
	i_create_bucket create_bucket;
	i_open_bucket open_bucket;
	i_close_bucket close_bucket;
	i_delete_bucket delete_bucket;
};

/**
 * abstract object interfaces.
 */
struct i_object_interface {
	i_create_object create_object;
	i_open_object open_object;
	i_delete_object delete_object;
	i_commit_object commit_object;
	i_close_object close_object;
	i_read_object read_object;
	i_write_object write_object;
	i_list_object list_object;
	i_set_object_worm_attr set_worm_attr;
};

/**
 * master store provider interface.
 */
struct i_store_provider {
	struct i_store_interface store;
	struct i_genobj_interface genobj;
	struct i_bucket_interface bucket;
	struct i_object_interface object;
};

#endif

