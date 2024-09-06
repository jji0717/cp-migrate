#ifndef __OSTORE__H__
#define __OSTORE__H__
/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ostore.h __ 2011/08/31 12:03:35 PDT __ ecande
 */
/** @addtogroup ostore
 * @{ */
/** @file
 * Header file for the external object store API
 */

#include <ifs/bam/bam_pctl.h>
#include <sys/stat.h>

#include <stdbool.h>
#include <fcntl.h>
#include <unistd.h>
#include <fts.h>
#include <unistd.h>

#include <isi_util/isi_error.h>
#include <sys/isi_acl.h> 

#include <isi_object/ostore_gcfg.h>
#include <isi_acl/isi_sd.h>
#include <isi_object/fs_op_utils.h>

#ifdef __cplusplus
extern "C" {
#endif

/* COMMON DEFINIIONS */

/* GENERAL OBJECT STORE INTERFACES */

enum OSTORE_TYPE {
	OST_INVALID = -1,
	OST_OBJECT = 0,
	OST_NAMESPACE = 1,
	OST_MAX = 1
};

enum OSTORE_OBJECT_TYPE {
	OOT_INVALID = -1,
	OOT_CONTAINER = 0, // container object, like a bucket
	OOT_OBJECT = 1, // data object
	OOT_FIFO = 2,
	OOT_CHR = 3,
	OOT_BLK = 4,
	OOT_LNK = 5,
	OOT_SOCK = 6,
	OOT_WHT = 7,
	OOT_STUB = 8,
	OOT_ALL = 9 // all object types, this is not a separate object type
};

enum OSTORE_SYMBLNK_TYPE {
	OLT_NONE = 0,
	OLT_FILE = 1,   // symbolic link to a file
	OLT_DIR  = 2    // symbolic link to a dir
};

enum ostore_acl_mode {
	OSTORE_ACL_NONE               = 0,
	OSTORE_ACL_STORE              = 1,
	OSTORE_ACL_BUCKET_HASH        = 2,
	OSTORE_ACL_BUCKET             = 3,
	OSTORE_ACL_OBJECT_HASH        = 4,
	OSTORE_ACL_OBJECT             = 5,
	OSTORE_ACL_OBJECT_FILE        = 6,
	OSTORE_ACL_TMP_OBJECT_FILE    = 7,
	OSTORE_OWNERREAD              = 8,
	OSTORE_PRIVATE                = 9,
	OSTORE_PUBLICREAD             = 10,
	OSTORE_PUBLICREADWRITE        = 11,
	NS_PRIVATE_READ               = 12,
	NS_PRIVATE                    = 13,
	NS_PUBLIC_READ                = 14,
	NS_PUBLIC_READ_WRITE          = 15,
	NS_PUBLIC                     = 16
};


/**
 * handle by which the object store can be referenced.
 */
struct ostore_handle;

/**
 * Opaque Bucket name structure
 */
struct ibucket_name;

/**
 * Generic object handle. This is the base handle of ibucket_handle and
 * iobject_handle. It can be cast from a bucket and an object handle.
 */
struct igenobj_handle;

/**
 * Bucket handle
 */
struct ibucket_handle;

/**
 * Object handle
 */
struct iobject_handle;

/**
 * For protection setting, modified from python lib
 *
 */


//function to get protection attributes
void iobj_genobj_get_protection_attr(struct igenobj_handle *igobj_hdl,
    struct pctl2_get_expattr_args *ge, struct isi_error **error_out);

//function to set protection attributes
void iobj_genobj_set_protection_attr(struct igenobj_handle *igobj_hdl,
    struct pctl2_set_expattr_args *se, struct isi_error **error_out);


struct iobj_set_protection_args *genobj_set_protection_args_create_and_init(void);

void genobj_set_protection_args_release(struct iobj_set_protection_args *);


#include <ifs/ifs_types.h>

// use OneFS model for worm obj attrs
struct object_worm_attr {
	struct worm_state 	w_attr;
	bool 			w_is_valid; // whether it is a worm object
};

// use OneFS model for worm container attrs
struct object_worm_domain {
	struct domain_entry dom_entry;
        ifs_domainid_t domain_id;
        char * domain_root;
};

//
struct object_worm_set_arg {
	uint64_t 	w_retention_date;
	bool 		w_commit; // whether to commit the file
};

struct iobj_obj_attr_entry {
	char *key;
	int value_size;
	void *value;
};

struct iobj_object_attrs {
	int magic;
	int allocated_entries;
	int num_entries;
	struct iobj_obj_attr_entry attrs[];
};

struct iobj_object_attrs *genobj_attr_list_create(int count);

void genobj_attr_list_append(struct iobj_object_attrs *list,
    const char *key, const void *value, int value_size,
    struct isi_error **error_out);

/**
 * Free memory allocated for attribute as returned by
 * iobj_object_attribute_list().
 *
 * @params data attribute to be freed
 * @params error isi_error struct, NULL on success, set on failure
 */
void genobj_attribute_list_release(struct iobj_object_attrs *attr_list,
    struct isi_error **error);

/**
 * structure modeling information returned in the callback
 */
struct iobj_list_obj_info {
	const char *name;
	enum OSTORE_OBJECT_TYPE object_type;
	enum OSTORE_SYMBLNK_TYPE symblnk_type;
	uint64_t id;
	const struct stat *stats;
	const struct iobj_object_attrs *attr_list;
	uint64_t cookie;
	int cur_depth; // the depth relative to the root of the query
	const struct iobj_list_obj_info *container; // link to the container
	const char *path; // this field used for store access point obj
			  // for regular obj, it is not used yet
};

/**
 * Callback for listing objects in a bucket.  The caller provides a callback
 * that is called for each object. The execution happens in the callers context.
 *
 *  @param caller_context an argument that the caller passes and
 *  which is returned through the callback
 *  @param iobj_name name of the object returned by the callback
 *  @param stats pointer to stats structure for object, NULL if not available
 *  @param attr pointer to user defined attributes list, NULL if not available
 *
 *  @return The callback is expected to return true when it is desired to
 *  continue with the listing; false otherwise
 *
 */
typedef bool (*iobj_list_obj_cb)(void *caller_context,
    const struct iobj_list_obj_info *iobj_param);

/**
 * parameter to control list object behavior
 */
struct iobj_list_obj_param {
	/*
	 *  The bucket handle, this need to be set in case of single bucket list,
	 *  or the ios must be specified.
	 */
	struct ibucket_handle *ibh;

	/*
	 *  The callback for each entry enumerated.
	 */
	iobj_list_obj_cb caller_cb;

	/*
	 * opaque caller allocated context
	 */
	void *caller_context;

	/*
	 * indicate if stats are requested
	 */
	bool stats_requested;

	/*
	 * indicate if user-defined attribute are requested
	 */
	bool uda_requested;

	int cookie_count;
	/*
	 * The resume tokens
	 */
	uint64_t *resume_cookie;

	/*
	 * indicate the depth of the traverse in search mode
	 */
	int max_depth;
};


/**
 * List stores of given type
 *
 * @param store_type - the store type
 * @param acct_name  - account name
 * @param param - iobj_list_obj_param structure controls list behavior
 * @param error	- error status returned on failure
 * @return number of objects enumerated
 */
size_t iobj_ostore_list(enum OSTORE_TYPE store_type,
    const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error);


/**
 * Open an existing object store for use.  This initializes all in
 * memory information needed for access the store (open).  This is an
 * idempotent operation.
 * @param store_type The store type
 * @param storename Name of the object store
 * @param snapshot  Snapshot identifier (use HEAD_SNAPID if you want
 *                  the HEAD version)
 * @param error	    Error status returned on failure
 */
struct ostore_handle *iobj_ostore_open(enum OSTORE_TYPE store_type, 
    const char *storename, uint64_t snapshot, 
    struct isi_error **error);

/**
 * Close the object store
 * @param ios	Handle for the open object store
 * @param error Error status returned on failure
 */
void iobj_ostore_close(struct ostore_handle *ios, struct isi_error **error_out);

/**
 * Create a new object store.  This will perform all operations needed to
 * initialize any initial persistent information and then perform the
 * iobj_init function to initialize in memory requirements as well.
 * @param store_type    The store type
 * @param storename	Name to be assigned to the object store to be created
 * @param error	Error status returned on failure
 */
struct ostore_handle *iobj_ostore_create(enum OSTORE_TYPE store_type,
    const char *storename,
    const char *ns_path,
    struct isi_error **error);

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
void iobj_ostore_attributes_get(struct ostore_handle *ios, 
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
void iobj_ostore_attributes_set(struct ostore_handle *ios, 
    struct iobj_ostore_attrs *attrs, struct isi_error **error);

/**
 * Release the attributes structure returned by iobj_ostore_attributes_get
 * once it is no longer needed. 
 */
void iobj_ostore_attributes_release(struct ostore_handle *ios,
    struct iobj_ostore_attrs *attrs);

/**
 * Destroy the object store.  All information in the store will be unavailable
 * @param store_type    the store type
 * @param store_name	Name of the object store to be destroyed
 * @param error		Error status returned on failure
 */
struct isi_error *iobj_ostore_destroy(enum OSTORE_TYPE store_type,
    const char *store_name);

void iobj_ostore_dump(enum OSTORE_TYPE store_type);

/* BUCKET OPERATIONS */

/**
 * Generate ostore bucket name from shim input to allow for abstraction of on
 * disk naming from shim input.  Naming function only, does not look at disk.
 */
struct ibucket_name *iobj_ibucket_from_acct_bucket(struct ostore_handle *ios,
    const char *shim_account, const char *shim_bucketname,
    struct isi_error **error);

/**
 * Free the bucket name mapping pointer after use.
 * @param ibn Bucket name mapping pointer.
 */
void iobj_ibucket_name_free(struct ibucket_name *ibn);

/**
 * Check to see if a bucket already exists.
 */
bool iobj_ibucket_exists(struct ibucket_name *ibn); 

/**
 * Create a bucket given the bucket identifier and properties that should be
 * assigned to the bucket.
 */
struct ibucket_handle *iobj_ibucket_create(struct ibucket_name *ibn,
    mode_t c_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode, bool recursive, bool overwrite,
    struct isi_error **error);

/**
 * Open an existing bucket.  If the bucket doesn't exist, return an error
 * distinctive from any type of access error.
 */
struct ibucket_handle *iobj_ibucket_open(struct ibucket_name *ibn,
    enum ifs_ace_rights ace_mode, int flags, 
    struct ifs_createfile_flags cf_flags, 
    struct isi_error **error);

/**
 * Close a previously opened bucket.
 */
bool
iobj_ibucket_close(struct ibucket_handle *ibh, struct isi_error **error);

/**
 * Delete an existing bucket. Allow for the caller to determine the policy as
 * to whether a bucket can be deleted if there are any objects/containers that
 * exist in that bucket.  If the bucket has containers, does the policy need
 * to be expanded beyond a boolean such as handling the case of containers
 * existing without objects differently than containers and objects?
 */
struct isi_error *iobj_ibucket_delete(struct ibucket_name *ibn,
    bool require_empty);

/**
 * Callback for listing buckets.  The caller provides a callback
 * that is called for each bucket. The execution happens in the callers context.
 *
 *  @param caller_context an argument that the caller passes and
 *  which is returned through the callback
 *  @param ibucket_name name of the bucket returned by the callback
 * 
 *  @return The callback is expected to return true when it is desired to
 *  continue with the listing; false otherwise
 * 
 */
typedef bool (*bucket_list_cb)(void *caller_context, const char *ibucket_name);

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
size_t iobj_ibucket_list(struct ostore_handle *ios, bucket_list_cb caller_cb,
    void *caller_context, struct isi_error **error_out);

/**
 * Get the specified properties.  If property names are specified they will be
 * returned in the order specified.  If no properties are specified, retrieve
 * them all.
 */
struct isi_error *
iobj_ibucket_prop_get(struct ibucket_handle *ibh, void *attrs);

void
iobj_ibucket_dump(void);

void iobj_ibucket_attribute_set(struct ibucket_handle *iobj, const char *key,
    void *value, size_t value_length, struct isi_error **error);

void iobj_ibucket_attribute_get(struct ibucket_handle *iobj, const char *key,
    void **value, size_t *value_length, struct isi_error **error);

void iobj_ibucket_attribute_delete(struct ibucket_handle *iobj, const char *key,
    struct isi_error **error);

void iobj_ibucket_attribute_release(void *data);

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
int iobj_ibucket_attribute_list(struct ibucket_handle *iobj,
    struct iobj_object_attrs **list, struct isi_error **error);

/**
 * Free memory allocated for attribute as returned by
 * iobj_object_attribute_list().
 *
 * @params data attribute to be freed
 * @params error isi_error struct, NULL on success, set on failure
 */
void iobj_ibucket_attribute_list_release(struct ibucket_handle *iobj,
    struct iobj_object_attrs *attr_list,
	struct isi_error **error);

/**
 * Return the system attributes (stats) for an object.  The data returned is
 * the stat structure as returned from fstat on the object file.
 * @param iobj handle as returned from object create or object open calls
 * @param stat pointer to a stat structure to be filled in by this call
 * @param error pointer to an isi_error structure, NULL on success, set on fail
 */
void iobj_ibucket_system_attributes_get(struct ibucket_handle *iobj,
    struct stat *stat, struct isi_error **error);


/* objects interfaces */

/**
 * Create an object in the bucket corresponding to the bucket handle give, using
 * the flags to specify the read/write characteristics and the given
 * attributes.  On success the object handle is returned otherwise a NULL is
 * returned and error is set appropriately.  
 * @param ibh bucket handle for a currently opened bucket
 * @param name name for the object to be opened
 * @param flags specifying whether or not enforcing single object of same name
 * @param attributes list of key/value pairs that make up the attributes for
 * the object
 * @param acl ACL to be set on the object (just a string for now)
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *iobj_object_create(
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
struct iobject_handle *iobj_object_open(struct ibucket_handle *ibh, 
    const char *iobj_name,  enum ifs_ace_rights object_ace_rights_open_flg, int flags, 
    struct ifs_createfile_flags cf_flags, struct isi_error **error);

bool iobj_object_close(struct iobject_handle *iobj, struct isi_error **error);

void iobj_object_commit(struct iobject_handle *iobj, struct isi_error **error);

ssize_t iobj_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error); 

ssize_t iobj_object_write(struct iobject_handle *iobj, off_t offset, 
    void *buf, ssize_t size, struct isi_error **error);

/**
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param param iobj_list_obj_param structure control the behavior of the list
 *  @param error isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed 
 */
size_t iobj_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error);
 
struct isi_error *iobj_object_delete(struct ibucket_handle *ibh,
    const char *iobj_name);


void iobj_object_attribute_set(struct iobject_handle *iobj, const char *key,
    void *value, size_t value_length, struct isi_error **error);

void iobj_object_attribute_get(struct iobject_handle *iobj, const char *key, 
    void **value, size_t *value_length, struct isi_error **error);

void iobj_object_attribute_delete(struct iobject_handle *iobj, const char *key,
    struct isi_error **error);

void iobj_object_attribute_release(void *data);

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
int iobj_object_attribute_list(struct iobject_handle *iobj, 
    struct iobj_object_attrs **list, struct isi_error **error);

/**
 * Free memory allocated for attribute as returned by
 * iobj_object_attribute_list().
 *
 * @params data attribute to be freed
 * @params error isi_error struct, NULL on success, set on failure
 */
void iobj_object_attribute_list_release(struct iobject_handle *iobj,
    struct iobj_object_attrs *attr_list,
	struct isi_error **error);

/**
 * Return the system attributes (stats) for an object.  The data returned is
 * the stat structure as returned from fstat on the object file.
 * @param iobj handle as returned from object create or object open calls
 * @param stat pointer to a stat structure to be filled in by this call
 * @param error pointer to an isi_error structure, NULL on success, set on fail
 */
void iobj_object_system_attributes_get(struct iobject_handle *iobj,
    struct stat *stat, struct isi_error **error);

void iobj_object_dump(void);

/**
 * Open an object via a generic interface.
 * @param ios The store handle
 * @param path The container/object path
 * @param cf_flags The ifs_createfile flags
 * @param flags The open flags
 */
struct igenobj_handle *iobj_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error);

/**
 * Copy an object via a generic interface.
 * @param[IN] cp_param structure holds all parameters needed to execute copy
 * @param[OUT] err_out isi_error object
 * @return 0 if successful; otherwise non-0
 */
int iobj_genobj_copy(struct ostore_copy_param *cp_param,
    struct isi_error **err_out);

/**
 * Move file or directory
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
int iobj_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **err_out);

/**
 * Given a generic object, get the type
 */
enum OSTORE_OBJECT_TYPE iobj_genobj_get_type(struct igenobj_handle *igobj);

/**
 * Close a generic generic object
 */
void iobj_genobj_close(struct igenobj_handle *igobj, struct isi_error **error);


struct ostore_del_flags {
	int recur:1; // recursive
	int cont_on_err:1; // continue to delete the rest even if failed to
			   // delete one object
	// below TBD
	int force:1; // delete without confirmation regardless of
	             // the file's permissions
	int worm:1; // perform a worm delete
};

/**
 * Delete a generic generic object
 */
void iobj_genobj_delete(struct ostore_handle *ios, const char * path,
    struct ostore_del_flags flags, struct isi_error **error);

/**
 * Given a generic object, get the name
 */
const char *iobj_genobj_get_name(struct igenobj_handle *igobj,
    struct isi_error **error);

/**
 * Given a generic object, get filerev
 */
u_int64_t iobj_genobj_get_filerev(struct igenobj_handle *igobj,
    struct isi_error **error_out);


/* This routine is namespace specific, ns_store.c */

/**
 * Set ostore system directories to which regular operations should be
 * prohibited.
 *
 * This routine must be called by client prior to other operations if
 * client needs to specify the system directories.
 *
 * @param[IN] paths - system directories
 * @return number of paths extracted from the given string
 */
int ostore_set_system_dirs(const char *paths);

/*
 * This routine retrieves the acl of the Bucket/Object.
 * @param[IN] igobj_hdl - General object handle.
 * @param[IN] sec_info - security information. 
 * @param[OUT] sd_out the security descriptor after modification based on 
 *                 user input.
 * @param[OUT] error_out error output.
 */
void iobj_genobj_acl_get(struct igenobj_handle *igobj_hdl, 
    enum ifs_security_info sec_info, 
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out);


/**
 * This routine modifies the security descriptor based on the 
 * modifications input by the user.
 *
 * @param[IN] igobj_hdl - General object handle.
 * @param[IN] mods - modifications to the acl input by the user.
 * @param[IN] num_mods - number of ace's (to be added/modified/deleted).
 * @param[IN] acl_mod_type indicates if ace's are to be replaced or modified.
 * @param[IN] sec_info - security information. 
 * @param[OUT] sd_out the security descriptor after modification based on 
 *                 user input.
 * @param[OUT] error_out error output.
 */

void iobj_genobj_modify_sd(struct igenobj_handle *igobj_hdl, 
    struct ifs_ace_mod *mods, int num_mods, 
    enum ifs_acl_mod_type acl_mod_type,
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out);

/*
 * This routine stores the acl of the Bucket/Object.
 *
 * @param[IN] igobj_hdl - General object handle.
 * @param[IN] sd - security descriptor.
 * @param[IN] sec_info - security information.
 * @param[OUT] error_out error output.
 */
void iobj_genobj_acl_set(struct igenobj_handle *igobj_hdl, 
    struct ifs_security_descriptor *sd, enum ifs_security_info secinfo, 
    struct isi_error **error_out);

/**
 * Set canned acl and unix permission access mode and ownership
 * for the object.
 *
 * The mode is used only when @acl_mode is OSTORE_ACL_NONE
 *
 * If mode or uid or gid has value of -1, then corresponding
 * unix permission mode or owner user id or group id will not
 * be changed.
 * @param[IN] igobj_hdl - General object handle.
 * @param[IN] acl_mode - predefined ACL mode
 * @param[IN] mode - access mode
 * @param[IN] uid - user id
 * @param[IN] gid - group id
 * @param[OUT] error - the error on failure
 */
void iobj_genobj_access_control_set(struct igenobj_handle *igobj_hdl,
    enum ostore_acl_mode acl_mode, int mode, uid_t uid, gid_t gid,
    struct isi_error **error_out);

/**
 * Gets WORM attributes of the object
 * @param[IN] igobj_hdl -  General object handle.
 * @PARAM[OUT] worm_attr - WORM attribute structure to be filled
 * @PARAM[OUT] worm_dom -  WORM domain to be filled
 * @param[OUT] error_out - the isi_error to be returned
 *                         in case of failure
 */
void iobj_genobj_get_worm_attr(struct igenobj_handle *igobj_hdl,
    struct object_worm_attr *worm_attr, struct object_worm_domain *worm_dom,
    struct object_worm_domain **worm_ancs, size_t *num_ancs,
    struct isi_error **error_out);

/**
 * Sets WORM attributes to the object
 * @param[IN] iobj - object handle.
 * @PARAM[IN] set_arg - contains WORM attribute(s) to set
 * @param[OUT] error_out - the isi_error to be returned
 *                         in case of failure
 */
void iobj_object_set_worm_attr(struct iobject_handle *iobj,
    const struct object_worm_set_arg *set_arg,
    struct isi_error **error_out);

/* ostore_error.c */
ISI_ERROR_CLASS_DECLARE(OSTORE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_INVALID_ARGUMENT_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_ATTRIBUTE_NOT_FOUND_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_STORE_NOT_MOUNTED_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_INVALID_HANDLE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_ACCESS_DENIED_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_BUCKET_EXISTS_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_OBJECT_EXISTS_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_OUT_OF_RANGE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_EXCEEDS_LIMIT_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_INVALID_LICENSE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_UNSUPPORTED_OBJECT_TYPE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_PATHNAME_NOT_EXIST_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_PATHNAME_NOT_DIR_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_PATHNAME_NOT_ONEFS_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(OSTORE_COPY_TO_SUB_DIR_ERROR_CLASS);

#ifdef __cplusplus
}
#endif

/** @} */
#endif /* __OSTORE__H__ */
