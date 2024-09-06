/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ob_store.c __ 2011/08/31 12:01:48 PDT __ ecande
 */
/** @file
 * @ingroup ostore
 */
/**
 * @defgroup ostore Isilon Object Store
 *
 * The Isilon Object Store is divided into two main pieces,
 * 1) the object store itself with it's own library which provides the
 *    interface and mechanisms to create, store, read, destroy, etc. objects
 *    and their containers, and
 * 2) shims built on top of that object store library which provide the
 *    defined interfaces for specific object protocol implementations.
 */
/**
 * @defgroup ostore_private Private Isilon Object Store interface functions
 * This is the protocol independent interfaces for providing the mechanisms for
 * performing object related operations
 * @ingroup ostore
 */
/**
 * @addtogroup ostore
 * @{
 */

#include "ob_common.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <string.h>

#include <ifs/ifs_types.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include "isi_util/ifs_mounted.h"

#include "bucket_common.h"
#include "genobj_common.h"
#include "object_common.h"
#include "ostore_internal.h"

// Implementing the two tier object store interfaces:
struct i_store_provider ob_provider =
{
	// store interfaces
	{
		ob_store_list,
		ob_store_create,
		ob_store_destroy,
		ob_store_open,
		ob_store_close,
		ob_store_dump,
		ob_store_attributes_get,
		ob_store_attributes_set,
		ob_store_attributes_release,
		ob_store_parameters_get,
		ob_store_parameters_set,
		ob_store_parameters_release

	},

	// generic object interface
	{
		ob_genobj_open,
		genobj_close,
		genobj_get_name,
		genobj_attribute_get,
		genobj_attribute_set,
		genobj_attribute_delete,
		genobj_attribute_release,
		genobj_attribute_list,
		genobj_attribute_list_release,
		genobj_system_attributes_get,
		ob_genobj_copy,
		ob_genobj_move,
		ob_genobj_delete,
		NULL,
		NULL,
		NULL
	},

	// bucket interfaces:
	{
		ob_bucket_name_create,
		ob_bucket_create,
		ob_bucket_open,
		ob_bucket_close,
		ob_bucket_delete
	},

	// object interfaces:
	{
		ob_object_create,
		ob_object_open,
		ob_object_delete,
		ob_object_commit,
		ob_object_close,
		ob_object_read,
		ob_object_write,
		ob_object_list,
		NULL,
	}
};

static void
ob_store_attributes_get_set(bool get, struct ostore_handle *ios,
    struct iobj_ostore_attrs **attrs, struct ostore_parameters **params,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct gci_tree *tree = NULL;
	struct gci_base *gb = NULL;
	struct gci_ctx *a_ctx = NULL;
	struct gci_ctx *p_ctx = NULL;

	struct fmt FMT_INIT_CLEAN(config_file_name);

	fmt_print(&config_file_name, "%s/%s", ios->path,
	    OSTORE_OSTORE_CONFIG_NAME);

	tree = gci_tree_new(fmt_string(&config_file_name), NULL, NULL, NULL, 0);

	tree->root = &gci_ivar_ostore_config;

	gb = gcfg_open(tree, 0, &error);
	if (error) {
		goto out;
	}

	if (attrs) {
		a_ctx = gci_ctx_new(gb, 0, &error);
		if (error)
			goto out;
	}

	if (params) {
		p_ctx = gci_ctx_new(gb, 0, &error);
		if (error)
			goto out;
	}
	if (get) {
		if (attrs) {
			gci_ctx_read_path(a_ctx, "ostore_attrs", attrs,
			    &error);
			if (error)
				goto out;
		}

		if (params) {
			gci_ctx_read_path(p_ctx, "ostore_params", params,
			    &error);
			if (error)
				goto out;
		}
	} else {
		if (attrs) {
			ASSERT(*attrs);
			gci_ctx_write_path(a_ctx, "ostore_attrs", *attrs, 
			    &error);
			if (error)
				goto out;

			gci_ctx_commit(a_ctx, &error);
			if (error)
				goto out;
		}

		if (params) {
			ASSERT(*params);
			gci_ctx_write_path(p_ctx, "ostore_params", *params, 
			    &error);
			if (error)
				goto out;

			gci_ctx_commit(p_ctx, &error);
			if (error)
				goto out;
		}
	}
 out:
	if (error)
		isi_error_handle(error, error_out);

	/*
	 * clean up gconfig related pieces
	 */
	if (a_ctx)
		gci_ctx_free(a_ctx);

	if (p_ctx)
		gci_ctx_free(p_ctx);

	if (gb)
		gcfg_close(gb);

	if (tree)
		gci_tree_free(tree);

}

void
ob_store_parameters_release(struct ostore_parameters *params)
{
	if (params) {
		if (params->hash_name_split_insert)
			free(params->hash_name_split_insert);
		if (params->hash_name_translate)
			free(params->hash_name_translate);
		if (params->ostore_default_sid)
			free(params->ostore_default_sid);
		free(params);
	}
}

void
ob_store_attributes_release(struct iobj_ostore_attrs *attrs)
{
	if (attrs)
		free(attrs);
}

struct ostore_list_entry {
	SLIST_ENTRY(ostore_list_entry) entries;
	struct ostore_handle *handle;
	int ref;
	/* Add a real locking mechanism here */
	pthread_mutex_t lock;
};

	
SLIST_HEAD(ostore_list_head, ostore_list_entry) ostore_head = 
    SLIST_HEAD_INITIALIZER(ostore_head);

/* replace these with real locking when we decide what that is */
static pthread_mutex_t ostore_list_lock;

static void __attribute__ ((constructor)) ob_init(void);
static void __attribute__ ((destructor)) ob_finish(void);

void
ob_init(void)
{
	pthread_mutex_init(&ostore_list_lock, NULL);
}

void
ob_finish(void)
{
	pthread_mutex_destroy(&ostore_list_lock);
}

static void
ostore_lock(pthread_mutex_t *lock)
{
	pthread_mutex_lock(&ostore_list_lock);
}

static void
ostore_unlock(pthread_mutex_t *lock)
{
	pthread_mutex_unlock(&ostore_list_lock);
}

static void
ob_store_cache_remove(struct ostore_list_entry *entry)
{
	/* 
	 * when an entry is created and each part allocated, immediately after
	 * allocating the part, the next layer is allocated.  This meant that
	 * the upper level did not need contained pointers initialized as the
	 * allocation of that contained item would either be valid on the
	 * allocation or set to null on failure.  This avoids the problem of
	 * freeing non-existent entries here.
	 */
	if (entry) {
		if (entry->handle) {
			if (entry->handle->name)
				free(entry->handle->name);
			if (entry->handle->path)
				free(entry->handle->path);
			if (entry->handle->attrs)
				ob_store_attributes_release(entry->handle->attrs);
			if (entry->handle->params)
				ob_store_parameters_release(entry->handle->params);
			IOBJ_CLEAR_STORE_HANDLE_MAGIC(entry->handle);
			pthread_mutex_destroy(&entry->lock);
			free(entry->handle);
		}
		free(entry);
	}
}

/**
 * Allocate a new ostore entry and add to the list.
 */
static struct ostore_handle *
ob_store_cache_add(const char *store_name, uint64_t snapshot, int version,
    const char *path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *new_store = NULL;

	struct ostore_list_entry *entry = NULL;

	ostore_lock(&ostore_list_lock);
	SLIST_FOREACH(entry, &ostore_head, entries) {
		if (!strcmp(store_name, entry->handle->name))
			break;
	}

	if (entry != NULL){
		ASSERT(!strcmp(entry->handle->path,path));
		ostore_lock(&(entry->lock));
		entry->ref++;
		ostore_unlock(&(entry->lock));
		new_store = entry->handle;
	}
	
	if (!entry) {
		/*
		 * No current entry in the list so create a new one and add it
		 */

		entry = calloc(1, sizeof(struct ostore_list_entry));
		ASSERT(entry);
	  
		entry->handle = calloc(1, sizeof(struct ostore_handle));
		ASSERT(entry->handle);

		IOBJ_SET_STORE_HANDLE_MAGIC(entry->handle);

		/*
		 * fill out the handle to be returned
		 */
		entry->handle->name = strdup(store_name);
		ASSERT(entry->handle->name);
		entry->handle->snapshot = snapshot;
		entry->handle->version = version;
		entry->handle->path = strdup(path);
		entry->handle->store_type = OST_OBJECT;
		pthread_mutex_init(&entry->lock, NULL);
		ASSERT(entry->handle->path);

		ob_store_attributes_get_set(true, entry->handle,
		    &entry->handle->attrs, &entry->handle->params, &error);
		if (error)
			goto out;

		/*
		 * now add the entry to the list
		 */
		entry->ref = 1;
		SLIST_INSERT_HEAD(&ostore_head, entry, entries);
		new_store = entry->handle;
	}

 out:
	ostore_unlock(&ostore_list_lock);

	if (error){
		if (entry && entry->ref == 0)
			ob_store_cache_remove(entry);
		isi_error_handle(error, error_out);
	}

	return new_store;
}

/**
 * The primary purpose of this function is to fill in the root directory used
 * by all stores and return that in the struct fmt.  It will do so, even when
 * returning an error.
 *
 * In addition, this function will make sure that store exists, and create it
 * if it doesn't.  An error will be returned if we fail to create the store.
 *
 * @a fmt  -- will be filled with the name of the root store.
 * @a error_out -- set if we fail to create the store.
 */
static void
ob_store_root_open(struct fmt *fmt, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(tpath);

	/* first fill in the store-uber-root */
	fmt_print(fmt, "%s", OBJECT_STORE_ROOTNAME);

	/* Now see if we have access to it.  If so, we're done. */
	if (!eaccess(fmt_string(fmt), OSTORE_STORE_ACCESS_CHECK))
		goto out;

	/* If we can't access it because it doesn't exist, try to fix that.
	   For any other reason, return the error */
	if (errno != ENOENT) {
		error = isi_system_error_new(errno, 
		    "ob_store_root_open() failed access(%s)",
		    fmt_string(fmt));
		goto out;
	}

	/* only allow creation if ifs is mounted */
	bool mounted = ifs_is_mounted(&error);
	if (error)
		goto out;
	if (!mounted) {
		error = ostore_store_not_mounted_error_new("/ifs not mounted");
		goto out;
	}

	/* now create the dirl */
	mkdir_dash_p(&tpath, fmt_string(fmt), OSTORE_ROOT_DEFAULT_MODE, NULL,
	    NULL, OSTORE_ACL_NONE, OSTORE_ACL_NONE, NULL, NULL, &error);
	if (error)
		goto out;

out:
	if (error)
		isi_error_handle(error, error_out);
}

size_t
ob_store_list(const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
	return 0;
}

/**
 * Create a new object store.
 */
struct ostore_handle *
ob_store_create(const char *store_name,
    const char *ns_path,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ostore_return = NULL;
	struct fmt FMT_INIT_CLEAN(store_fmt);

	/*
	 * At least until we put the acls in, this will allow for the store to
	 * be created without the mode bits specified being masked.
	 */
	umask(0);

	/* 
	 * The initial structure for the datastore layout must exist ahead for
	 * time before the store is created.
	 */
	ob_store_root_open(&store_fmt, &error);
	if (error)
		goto out;

	fmt_print(&store_fmt, "/%s", store_name);

	/* mkdir() but allow it to already exist. */
	if (mkdir(fmt_string(&store_fmt), OSTORE_STORE_DEFAULT_MODE) && 
	    (errno != EEXIST)) {
		/*
		 * store may already exist or it could have been
		 * a true failure.  return as such
		 */
		error = isi_system_error_new(errno, 
		    "ob_store_create() failed store_name mkdir(%s)",
		    fmt_string(&store_fmt));
		goto out;
	}

	/*
	 * set the default ACL for the store.  Currently this is an inherited
	 * acl which will be applied to the hash directories.
	 */
	ostore_set_default_acl(fmt_string(&store_fmt), -1, OSTORE_ACL_STORE,
	    NULL, &error);
	if (error)
		goto out;

	/* create a tmp store directory */
	if (mkdir(OBJECT_STORE_TMP, OSTORE_STORE_DEFAULT_MODE) && 
	    (errno != EEXIST)) {
		error = isi_system_error_new(errno, 
		    "ob_store_create() failed store_name mkdir(%s)",
		    OBJECT_STORE_TMP);
		goto out;
	}

	/*
	 * set the default ACL for the temp directory.
	 */
	ostore_set_default_acl(OBJECT_STORE_TMP, -1, OSTORE_ACL_STORE,
	    NULL, &error);
	if (error)
		goto out;

	/*
	 * we have created a new store.  Call the open routine to
	 * handle the rest.  Specify the primary snap as we have just
	 * created it.
	 */
	ostore_return = ob_store_cache_add(store_name, HEAD_SNAPID,
	    OBJECT_STORE_VERSION, fmt_string(&store_fmt), &error);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return ostore_return;
}

/* 
 * Open the object store
 */
struct ostore_handle *
ob_store_open(const char *store_name,
    uint64_t snapshot, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ostore_return = NULL;
	struct fmt FMT_INIT_CLEAN(store_path);
	
	/* 
	 * Generate the pathname for the object store
	 */
	fmt_print(&store_path, "%s/%s", OBJECT_STORE_ROOTNAME, store_name);
	    
	/*
	 * Check to see if the directory exists and that we can read, write
	 * and traverse through it.
	 */
	if (eaccess(fmt_string(&store_path), OSTORE_STORE_ACCESS_CHECK)) {
		/* handle errors from access, check */
		switch (errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
				"Cannot open store %s", store_name);
			break;
		case EACCES:
			error = ostore_access_denied_error_new(
				"Cannot open store %s", store_name);
			break;
		default:
			error = isi_system_error_new(errno,
			    "Cannot access object store %s",
			    store_name);
			break;
		}
		goto out;
	}

	ostore_return = ob_store_cache_add(store_name, snapshot,
	    OBJECT_STORE_VERSION, fmt_string(&store_path), &error);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return ostore_return;
}

/*
 * Close the object store
 */
void
ob_store_close(struct ostore_handle *ios, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_list_entry *entry = NULL;

	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	/*
	 * Find the store handle and check to see if the reference count is 
	 * 1.  If it is then decrement the count and remove the entry.
	 */
	ostore_lock(&ostore_list_lock);

	SLIST_FOREACH(entry, &ostore_head, entries) {
		if (ios == entry->handle)
			break;
	}

	/* If we didn't find the entry, someone closed without open! */
	if (!entry) {
		error = ostore_invalid_handle_error_new(
			"close of %p attempted while not open", 
			ios);
		goto out;
	}

	/* now decrement the refcount and remove */
	ostore_lock(&(entry->lock));
	if (--entry->ref == 0) {
		SLIST_REMOVE(&ostore_head, entry, ostore_list_entry, entries);
		ostore_unlock(&(entry->lock));
		ob_store_cache_remove(entry);
	} else
		ostore_unlock(&(entry->lock));

out:
	ostore_unlock(&ostore_list_lock);

	if (error)
		isi_error_handle(error, error_out);
}

void
ob_store_attributes_set(struct ostore_handle *ios,
    struct iobj_ostore_attrs *attrs, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	struct iobj_ostore_attrs *save_attrs = NULL;

	ob_store_attributes_get_set(false, ios, &attrs, NULL, &error);
	if (error)
		goto out;
	
        save_attrs = ios->attrs;
	ob_store_attributes_get_set(true, ios, &ios->attrs, NULL, &error);
	if (error)
		goto out;
 out:
	/*
	 * Since we replaced the attributes, we need to release the old ones
	 */
	if (save_attrs && save_attrs != ios->attrs)
		ob_store_attributes_release(save_attrs);

	if (error)
		isi_error_handle(error, error_out);

}

void
ob_store_attributes_get(struct ostore_handle *ios,
    struct iobj_ostore_attrs **pattrs, struct isi_error **error)
{
	struct iobj_ostore_attrs *attrs = NULL;

	attrs = (struct iobj_ostore_attrs*)calloc(1, 
	    sizeof(struct iobj_ostore_attrs));
	ASSERT(attrs);

	/* simple equality at this point */
	*attrs = *ios->attrs;
	
	*pattrs = attrs;
}

/**
 * Private function to set the testing parameters for the object store
 */
void
ob_store_parameters_set(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	struct ostore_parameters *save_params = NULL;

	ob_store_attributes_get_set(false, ios, NULL, &params, &error);
	if (error)
		goto out;

	save_params = ios->params;
	ob_store_attributes_get_set(true, ios, NULL, &ios->params, &error);
	if (error)
		goto out;
 out:
	/*
	 * Since we replaced the params, we need to release the old ones
	 */
	if (save_params && ios->params != save_params)
		ob_store_parameters_release(save_params);

	if (error)
		isi_error_handle(error, error_out);

}

void
ob_store_parameters_get(struct ostore_handle *ios,
    struct ostore_parameters **pparams, struct isi_error **error)
{
	struct ostore_parameters *params = NULL;

	params = (struct ostore_parameters *)calloc(1, 
	    sizeof(struct ostore_parameters));
	ASSERT(params);

	/* copy the attributes out */
	*params = *ios->params;
	params->hash_name_split_length = ios->params->hash_name_split_length;
	params->hash_name_split_left = ios->params->hash_name_split_left;
	params->hash_name_split_right = ios->params->hash_name_split_right;
	params->hash_name_translate = strdup(ios->params->hash_name_translate);
	ASSERT(params->hash_name_translate);
	params->hash_name_split_insert = 
	    strdup(ios->params->hash_name_split_insert);
	ASSERT(params->hash_name_split_insert);
	params->hash_name_hashto = ios->params->hash_name_hashto;
	params->hash_name_max_collisions =
	    ios->params->hash_name_max_collisions;
	params->max_buckets = ios->params->max_buckets;
	params->recreate_bucket = ios->params->recreate_bucket;
	params->ostore_default_sid = 
	    strdup(ios->params->ostore_default_sid);
	ASSERT(params->ostore_default_sid);

	*pparams = params;
}

/*
 * Destroy the object store.
 */
struct isi_error *
ob_store_destroy(const char *store_name)
{
	struct isi_error *error = ostore_error_new("Operation not supported");
	return error;
}

void 
ob_store_dump(void)
{
	struct ostore_list_entry *entry = NULL;
	struct fmt FMT_INIT_CLEAN(fmt);

	if (SLIST_EMPTY(&ostore_head))
		printf("No object stores are currently open\n");
	else {
		SLIST_FOREACH(entry, &ostore_head, entries) {
			fmt_print(&fmt, "Store name: %s; Path: %s; "
			    "Refcount: %d, accounting: %{}, ordering: %{}, "
			    "versioning: %{}\n",
			    entry->handle->name, entry->handle->path, 
			    entry->ref,
			    enum_accounting_options_fmt(entry->handle->attrs->accounting),
			    enum_ordering_options_fmt(entry->handle->attrs->ordering),
			    enum_version_options_fmt(entry->handle->attrs->versioning));
			printf("%s", fmt_string(&fmt));
			fmt_truncate(&fmt);
		}
	}
}

struct igenobj_handle *
ob_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
	return NULL;
}


int
ob_genobj_copy(struct ostore_copy_param *cp_param, struct isi_error **err_out)
{
	ASSERT(err_out && *err_out == NULL);
	*err_out = ostore_error_new("Operation not supported");
	return -1;
}

int
ob_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **err_out)
{
	ASSERT(err_out && *err_out == NULL);
	*err_out = ostore_error_new("Operation not supported");
	return -1;
}

void
ob_genobj_delete(struct ostore_handle *ios, const char *path, 
    struct ostore_del_flags flags, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
}

/** @} */
