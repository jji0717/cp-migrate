/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	common.h __ 2011/08/31 12:03:24 PDT __ ecande
 */
/** @addtogroup ostore_private
 * @{ */
/** @file
 * @ingroup ostore_private
 */

#ifndef __LIB__ISI_OBJECT__COMMON__H_
#define __LIB__ISI_OBJECT__COMMON__H_

#include "store_provider.h"
#include <isi_acl/isi_sd.h>
#include "genobj_acl.h"

enum {
	OSTORE_ROOT_DEFAULT_MODE         = (S_IRWXU|S_IXGRP|S_IXOTH),
	OSTORE_STORE_DEFAULT_MODE        = (S_IRWXU|S_IRWXG|S_IRWXO),
	OSTORE_BUCKET_HASH_MODE          = (S_IRWXU|S_IRWXG|S_IRWXO),
	OSTORE_BUCKET_DEFAULT_MODE       = (S_IRWXU),
	OSTIRE_OBJECT_HASH_MODE          = (S_IRWXU|S_IRWXG|S_IRWXO),
	OSTORE_OBJECT_DIR_DEFAULT_MODE   = (S_IRWXU),
	OSTORE_OBJECT_DEFAULT_MODE       = (S_IRUSR|S_IWUSR),

	OSTORE_STORE_CONFIG_ACCESS_CHECK = (R_OK),
	OSTORE_STORE_ACCESS_CHECK        = (R_OK|W_OK|X_OK),
	OSTORE_BUCKET_ACCESS_CHECK       = (R_OK|W_OK|X_OK),
	OSTORE_OBJECT_FIND_ACCESS        = (R_OK),

	OSTORE_OBJECT_CREATE_FLAGS = (O_WRONLY|O_CREAT|O_EXCL), /* create w/
								 * fail on
								 * existing */
	OSTORE_OBJECT_OPEN_READ_FLAGS = (O_RDONLY),
	OSTORE_OBJECT_DIR_OPEN_FLAGS = (O_RDONLY) // open() will work only or RD
};

enum ostore_object_create_flags {
	OSTORE_ENFORCE_SINGLE_OBJECT = 0x0001,
};

/**
 * This contains common internal implementation definitions.
 */

#define ONE_FS_NAME  				"OneFS"
#define OSTORE_OBJECT_CREATE_EXTENSION		".tmp"
#define OSTORE_OBJECT_KEY_ATTR_NAMESPACE	EXTATTR_NAMESPACE_IFS
#define OSTORE_OBJECT_NAME_KEY			"ifsus_resource_name"
#define OSTORE_OBJECT_KEY_MAX_LENGTH		1024

/**
 * handle by which the object store can be referenced.
 */
struct ostore_handle {
	char *name; /**< Name of the object store */
	int magic; /**< magic number used for sanity check */
	enum OSTORE_TYPE store_type;
	uint64_t snapshot; /**< Id for the particular snapshot being accessed */
	uint version; /**< some means to determine the current version of the
		      * store */
	char *path; /**< full path name to the object store */
	struct iobj_ostore_attrs *attrs; /**< attrs for all buckets in the
					    store */
	struct ostore_parameters *params; /**< configuration parameters */
};

/**
 * This structure models a generic object. This can be a container object
 * or a data object. ibucket_handle and iobject_handle are derived form this.
 * This object is abstract: do not instantiate it directly.
 */
struct igenobj_handle {
	int magic; /**< magic number used for sanity check */
	enum OSTORE_OBJECT_TYPE object_type;
	struct ostore_handle *ostore; /**< ostore reference */
	char *name; /**< name of the object */
	char *path; /**< full path to the object */
	int flags; /**< flags associated with IO operations to the file */
	int fd; /**< file descriptor for reading/writing. */
	/*
	 * items below this line are not currently used, but for possible future
	 * use
	 */
	uint64_t	     ref;     /**< reference count on the bucket */
};

/**
 * Structure to represent the bucket for the data.
 */
struct ibucket_name {
	int magic; /**< magic number used for sanity check */
	struct ostore_handle *ostore; /**< object store the bucket is found in */
	char                 *bucket_name; /**< name specified for this bucket */
	const char	     *shim_bucketname;
	char		     *shim_account;
};

/**
 * Bucket handle to be used by most all bucket and object related operations.
 */
struct ibucket_handle {
	struct igenobj_handle base;
	void *attrs;
};

struct iobject_handle {
	struct igenobj_handle base;
	struct ibucket_handle *bucket; /**< bucket that contains the object */
	int dir_fd; /**< file descriptor for the object's directory */
	char *temp_obj_name; /**< name of tmpfile created relative to obj dir */
	char *target_obj_name; /**< target name of object relative to obj dir */
	bool committed; /**< flag showing whether object has been committed */
	// mode to set on object, but delayed until commit time
	enum ostore_acl_mode acl_mode; /** the pre-defined ACL mode */
	mode_t c_mode; /** file creation mode */
};

enum {
	OSTORE_IOS_MAGIC           = 0xdeadbeef,
	OSTORE_IBN_MAGIC           = 0xbeeffeed,
	OSTORE_IBH_MAGIC           = 0xfeedbeef,
	OSTORE_IOBJ_MAGIC          = 0xbeefdead,
	OSTORE_OBJ_ATTR_LIST_MAGIC = 0xdeafbeef,
	OSTORE_INVALID_MAGIC       = 0xffffffff
};

#define IOBJ_SET_STORE_HANDLE_MAGIC(h) (h)->magic = OSTORE_IOS_MAGIC;
#define IOBJ_CLEAR_STORE_HANDLE_MAGIC(h) (h)->magic = OSTORE_INVALID_MAGIC;
#define IOBJ_CHECK_STORE_HANDLE(handle, error, label)			\
	if (!(handle) || (handle)->magic != OSTORE_IOS_MAGIC ||		\
	    !(handle)->path) {						\
		(error) = ostore_invalid_handle_error_new(              \
			"Invalid store handle");			\
	goto label;                                                     \
}

#define IOBJ_SET_BUCKET_HANDLE_MAGIC(h) (h)->base.magic = OSTORE_IBH_MAGIC;
#define IOBJ_CLEAR_BUCKET_HANDLE_MAGIC(h) (h)->base.magic = OSTORE_INVALID_MAGIC;
#define IOBJ_CHECK_BUCKET_HANDLE(handle, error, label)			\
	if (!(handle) || (handle)->base.magic != OSTORE_IBH_MAGIC || 	\
	    !(handle)->base.ostore || !(handle)->base.path ) {	\
		(error) = ostore_invalid_handle_error_new(              \
			"Invalid bucket handle");			\
	goto label;                                                     \
}

#define IOBJ_SET_BUCKET_NAME_MAGIC(h) (h)->magic = OSTORE_IBN_MAGIC;
#define IOBJ_CLEAR_BUCKET_NAME_MAGIC(h) (h)->magic = OSTORE_INVALID_MAGIC;
#define IOBJ_CHECK_BUCKET_NAME(handle, error, label)			\
	if (!(handle) || (handle)->magic != OSTORE_IBN_MAGIC ||		\
	    !(handle)->ostore || (!(handle)->ostore->path && 		\
		!(handle)->bucket_name)) {				\
		(error) = ostore_invalid_handle_error_new(              \
			"Invalid bucket name");				\
	goto label;                                                     \
}

#define IOBJ_SET_OBJECT_HANDLE_MAGIC(h) (h)->base.magic = OSTORE_IOBJ_MAGIC;
#define IOBJ_CLEAR_OBJECT_HANDLE_MAGIC(h) (h)->base.magic = OSTORE_INVALID_MAGIC;
#define IOBJ_CHECK_OBJECT_HANDLE(handle, error, label)			\
	if (!(handle) || (handle)->base.magic != OSTORE_IOBJ_MAGIC ||	\
	    !(handle)->base.name ||					\
	    !(handle)->base.path || (handle)->base.fd < 0) {		\
		(error) = 						\
		    ostore_invalid_handle_error_new(			\
			    "Invalid object handle");			\
	goto label;                                                     \
}

#define IOBJ_SET_OBJECT_ATTR_LIST_MAGIC(h) 				\
	(h)->magic = OSTORE_OBJ_ATTR_LIST_MAGIC;
#define IOBJ_CLEAR_OBJECT_ATTR_LIST_MAGIC(h) 				\
	(h)->magic = OSTORE_INVALID_MAGIC;
#define IOBJ_CHECK_OBJECT_ATTR_LIST(handle, error, label)		\
	if (!(handle) || (handle)->magic != OSTORE_OBJ_ATTR_LIST_MAGIC){\
		(error) = 						\
		    ostore_invalid_handle_error_new(			\
			    "Invalid object attr list handle");		\
	goto label;                                                     \
}

#define IOBJ_CHECK_GENOBJ_HANDLE(handle, error, label)			\
	if (!(handle) || ((handle)->object_type != OOT_CONTAINER	\
	    && (handle)->object_type != OOT_OBJECT)) {			\
		(error) = 						\
		    ostore_invalid_handle_error_new(			\
			    "Invalid object handle");			\
		goto label;                                             \
	} else if ((handle)->object_type == OOT_CONTAINER) {		\
		IOBJ_CHECK_BUCKET_HANDLE(((struct ibucket_handle*)handle), \
		    error, label);					\
	} else								\
		IOBJ_CHECK_OBJECT_HANDLE(((struct iobject_handle*)handle), \
		    error, label);

#define IOBJ_OBJECT_ATTR_KEY(list, a)        list->attrs[(a)].key
#define IOBJ_OBJECT_ATTR_VALUE(list, a)      list->attrs[(a)].value
#define IOBJ_OBJECT_ATTR_VALUE_SIZE(list, a) list->attrs[(a)].value_size


/**
 * Generate the components for a directory path given an initial base path (in
 * @a tpath) and new path elements (in @a new_path).  Upon success, @a tpath
 * will contain the entire (concatenated) path to the object.
 * The directory corresponding to base_path is expected to already exist.
 */
void mkdir_dash_p(struct fmt *tpath, const char *new_path,
    int create_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode base_acl_mode,
    enum ostore_acl_mode final_acl_mode,
    int *fd, struct persona *owner, struct isi_error **error_out);

char *create_dir_elements(char *base_path, const char *new_path, 
    int name_ext_sz, struct isi_error **error_out);

/**
 * Create a path in 'result' which is as follows
 * 	 prefix/<tempName>[/]suffix
 *
 * 'tempName' is generated internally and such that result is unique
 */
void create_tmp_dir_path(const char *prefix, const struct fmt *suffix,
    struct fmt *result, int create_mode, const struct iobj_object_attrs *attrs,
    enum ostore_acl_mode base_acl_mode, enum ostore_acl_mode final_acl_mode,
    struct persona *owner, struct isi_error **error_out);

/**
* Walk 'rm_path_str' in reverse removing each component in the path until
* it  becomes equal or shorter than becomes_path_str.
* Returns 0 on success, -1 on error.  The error
* location is indicated by the content remaining in 'rm_path_str'
*/
int remove_path_until(const char *rm_path_str, const char *becomes_path_str, 
    struct isi_error **error_out);

/**
 * Remove all files in a given directory that have the same prefix as the
 * template specified.  This was created to remove all object files from an
 * object directory
 */
void remove_all_object_entries(const char *object_path, const char *prefix, 
    struct isi_error **error_out);

void ostore_set_default_acl(const char *path, int fd, 
    enum ostore_acl_mode mode, struct persona *owner, 
    struct isi_error **error_out);

void ostore_create_default_acl(struct ifs_security_descriptor **sd,  
    enum ostore_acl_mode mode, struct persona *owner, 
    struct isi_error **error_out);

void ostore_release_acl(struct ifs_security_descriptor *sd);

/**
 * Ensure the file descriptor refers to object on OneFS
 * @param[IN] fd - file descriptor
 * @return NULL if the file descriptor refers to object on OneFS,
 * 	or isi_error otherwise.
 */
struct isi_error *ensure_object_on_onefs(int fd);

/**
 * Construct special isi_error corresponding to invalid argument error
 * from ifs_createfile.
 *
 * The reason behind this is that ifs_createfile returns EINVAL error
 * if the file/directory path to be opened is not OneFS object assuming no
 * programming error.
 *
 * @return ostore_unsupported_object_type_error
 */
struct isi_error *ifs_createfile_invalid_arg_error(void);

extern struct i_store_provider ns_provider;
extern struct i_store_provider ob_provider;

/** @} */
#endif /* __LIB__ISI_OBJECT__COMMON__H_ */
