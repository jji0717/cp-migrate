/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	genobj_common.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "genobj_common.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include <sys/extattr.h>
#include <sys/stat.h>

#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_restripe.h>
#include <ifs/ifs_userattr.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_file_lock.h>
#include <isi_ufp/isi_ufp.h>

#include "bucket_common.h"
#include "fs_op_utils.h"
#include "object_common.h"
#include "ostore_internal.h"
#include "iobj_hash.h"
#include "iobj_licensing.h"
#include "ostore_internal.h"

struct iobj_set_protection_args *
genobj_set_protection_args_create_and_init(void)
{
	struct iobj_set_protection_args *se =
	    malloc(sizeof (struct iobj_set_protection_args));
	ASSERT(se);

	se->node_pool_id = -1;

	return se;
}

void
genobj_set_protection_args_release(struct iobj_set_protection_args *se)
{
	if (se)
		free(se);
}

/**
 * Simple single attribute get. Use iobj_object_attribute_release() to free the
 * space allocated for the value by this routine to avoid memory leak.
 * @param iobj handle for previously opened object
 * @param key  ascii string for key name for attribute
 * @param value pointer to value to be returned for attribute.
 * @param value_size pointer to size of value to be returned.
 * @param error_out isi_error structure, NULL if no error, set if error found
 */
void
genobj_attribute_get(struct igenobj_handle *iobj, const char *key,
    void **value, size_t *value_size, struct isi_error **error_out)
{
	int size = 0;
	int size1 = 0;
	void *data = NULL;
	struct isi_error *error = NULL;

	ASSERT(error_out);

	if (!key) {
		error = ostore_invalid_argument_error_new(
		    "No key specified for attribute");
		goto out;
	}

	if (!value || !value_size) {
		error = ostore_invalid_argument_error_new(
		    "No value or value_size specified");
		goto out;
	}

	size = ifs_userattr_get(iobj->fd, key, NULL, 0);
	if (size == -1) {
		if (errno == ENOATTR)
			error = ostore_attribute_not_found_error_new(
			    "Attribute %s not found for object %s",
			    key, iobj->name);
		else
			error = isi_system_error_new(errno);
		goto out;
	}

	/*
	 * Extra space allocated for '\0' termination. Defensive fix
	 * for callers that assumes null terminated values.
	 */
	data = malloc(size + 1);
	ASSERT(data);

	size1 = ifs_userattr_get(iobj->fd, key, data, size);

	if (size1 == -1) {
		if (errno == ENOATTR)
			error = ostore_attribute_not_found_error_new(
			    "Attribute %s no longer found for object %s",
			    key, iobj->name);
		else
			error = isi_system_error_new(errno);
		goto out;
	}

	((char *) data)[size1] = '\0';
	*value = data;
	*value_size = size1;

 out:
	if (error)
		isi_error_handle(error, error_out);
}

static void
genobj_attr_list_entry_release(struct iobj_obj_attr_entry *entry)
{
	if (entry) {
		if (entry->key)
			free(entry->key);
		if (entry->value)
			free(entry->value);
		entry->key = entry->value = NULL;
	}
}

struct iobj_object_attrs *
genobj_attr_list_create(int count)
{
	ASSERT(count > 0);
	int size = (count * sizeof(struct iobj_obj_attr_entry)) +
	    sizeof (struct iobj_object_attrs);
	struct iobj_object_attrs *list = calloc(1, size);
	ASSERT(list);
	IOBJ_SET_OBJECT_ATTR_LIST_MAGIC(list);
	list->allocated_entries = count;
	return list;
}

void
genobj_attr_list_append(struct iobj_object_attrs *list,
    const char *key, const void *value, int value_size,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobj_obj_attr_entry *entry = &list->attrs[list->num_entries];

	ASSERT(error_out);

	IOBJ_CHECK_OBJECT_ATTR_LIST(list, error, out);

	if (!key || (value_size && !value)) {
		error = ostore_invalid_argument_error_new(
		    "Key not specified, or value incorrect");
		goto out;
	}

	/*
	 * Verify that we will not exceed the allocated space for this attr
	 * list.
	 */
	if (list->num_entries >= list->allocated_entries) {
		error = ostore_out_of_range_error_new(
		    "Append of attribute would exceeded "
		    "the allocated number of attributes");
		goto out;
	}

	/*
	 * allocate and set the key
	 */
	entry->key = strdup(key);
	ASSERT(entry->key);

	/*
	 * allocate and set the value
	 */
	if (value_size) {
                /*
		 * Extra space allocated for '\0' termination. Defensive fix
		 * for callers that assumes null terminated values.
		 */
		entry->value = malloc(value_size + 1 );
		ASSERT(entry->value);
		memcpy(entry->value, value, value_size);
		((char *) entry->value)[value_size] = '\0';
	}
	else
		entry->value = NULL;

	entry->value_size = value_size;

	/*
	 * bump the current number of entries
	 */
	list->num_entries++;
 out:
	if (error)
		isi_error_handle(error, error_out);

}

/**
 * internal implementation to list attributes given object
 * file descriptor.
 */
int
genobj_list_ext_attrs(int fd,
    struct iobj_object_attrs **attr_list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobj_object_attrs *list = NULL;
	char attr_data[IFS_USERATTR_VAL_MAXSIZE + 1];
	int attr_data_size = sizeof(attr_data);
	int returned_data_size = 0;
	int attrs_added = 0;
	int count = 0;
	int i;

	/*
	 * Need something big enough to hold all the keys we can get back from
	 * ifs_userattr_list.
	 */
	struct ifs_userattr_key ifs_attr_keys[IFS_USERATTR_MAXKEYS];

	ASSERT(error_out);
	ASSERT(attr_list);

	/*
	 * grab the list of attributes for the object from the kernel
	 */
	count = ifs_userattr_list(fd, ifs_attr_keys);

	/*
	 * check for error
	 */
	if (count < 0) {
		if (errno == EOPNOTSUPP) {
			error = ostore_unsupported_object_type_error_new(
			    "getting attribute list");
		} else {
			error = isi_system_error_new(errno,
			    "getting attribute list");
		}
		goto out;
	}

	/*
	 * check for no attributes
	 */
	if (count == 0)
		goto out;

	/*
	 * Create enough room for the attribute list itself.  The space for
	 * the key and value will be added on as they are appended on
	 */
	list = genobj_attr_list_create(count);
	ASSERT(list);

	for (i = 0; i < count; i++) {
		/*
		 * get the value corresponding to the attribute
		 */
		if ((returned_data_size = ifs_userattr_get(fd,
		    ifs_attr_keys[i].key,
		    attr_data, attr_data_size)) == -1) {
			if (errno == ENOATTR)
				continue;
			else {
				error = isi_system_error_new(errno,
				    "getting value for attr %s in list",
				    ifs_attr_keys[i].key);
				goto out;
			}
		}

		/*
		 * Add the key/value pair to the next slot on the list.
		 */
		genobj_attr_list_append(list, ifs_attr_keys[i].key,
		    attr_data, returned_data_size, &error);
		if (error)
			goto out;

		/*
		 * get an accurate count of the attributes we added.  This
		 * will allow us to compensate for attrs that got removed
		 * while we were listing them
		 */
		attrs_added++;
	}

	*attr_list = list;

 out:
	if (error)
		isi_error_handle(error, error_out);

	return attrs_added;
}

void
genobj_attribute_list_release(struct iobj_object_attrs *list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int i;

	ASSERT(error_out);

	IOBJ_CHECK_OBJECT_ATTR_LIST(list, error, out);

	if (list) {
		for (i = 0; i < list->allocated_entries; i++)
			genobj_attr_list_entry_release(&list->attrs[i]);

		IOBJ_CLEAR_OBJECT_ATTR_LIST_MAGIC(list);
		free(list);
	}
 out:
	if (error)
		isi_error_handle(error, error_out);
}

/**
 * List attributes (and values) for an object. Use
 * iobj_genobj_attribute_list_release() to free the space allocated for the
 * value by this routine to avoid memory leak. The macros
 * IOBJ_OBJECT_ATTR_KEY() and IOBJ_OBJECT_ATTR_VALUE() can be used to help
 * extract the name and value from the attribute list.
 *
 * @param iobj handle for previously opened object
 * @param list pointer to iobj_object_attrs listing of the attributes.
 * @param error_out isi_error structure, NULL if no error, set if error found
 * @returns count of number of attributes returned in list.
 *
 * Note:  This function executes without a lock and so it is possible for
 * attrs to be removed (or new attrs to be added) while it is executing.
 * The check against ENOATTR is to allow for attributes to be removed (we
 * can't do anything about the ones that get added)
 */
int
genobj_attribute_list(struct igenobj_handle *iobj,
    struct iobj_object_attrs **attr_list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int count = 0;

	ASSERT(error_out);

	/*
	 * validate the object handle
	 */

	if (attr_list == NULL) {
	        error = ostore_invalid_argument_error_new(
		    "Invalid parameter, list must be specified");
		goto out;
	}

	count = genobj_list_ext_attrs(iobj->fd, attr_list, &error);
out:
	if (error)
		isi_error_handle(error, error_out);

	return count;
}

/**
 * Simple single attribute set. If either value or value_size is set to 0, key
 * will be deleted.
 * @param iobj handle for previously opened object
 * @param key  utf-8 string for key name for attribute; should be <=
 * 	IFS_USERATTR_KEY_MAXSIZE and any utf-8 char except '\0'
 * @param value value to be set for attribute, should be <=
 *     IFS_USERATTR_VAL_MAXSIZE and any utf-8 char except '\0'
 * @param value_size size of value to be set
 * @param error_out isi_error structure, NULL if no error, set if error found
 */
static void
single_attribute_set(int fd, const char *key,
    void *value, size_t value_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out && !(*error_out));

	if (!key) {
		error = ostore_invalid_argument_error_new(
		    "Key not specified for attribute");
		goto out;
	}

	if (value_size == 0) {
		value = "";
	}

	if (ifs_userattr_set(fd, key, value, value_size) == -1) {
		switch (errno) {
		case ENAMETOOLONG:
			error = ostore_exceeds_limit_error_new(
			    "Attribute key exceeds limit: %d bytes",
			    IFS_USERATTR_KEY_MAXSIZE);
			break;
		case EOVERFLOW:
			error = ostore_exceeds_limit_error_new(
			    "Attribute value exceeds limit: %d bytes",
			    IFS_USERATTR_VAL_MAXSIZE);
			break;
		case E2BIG:
			error = ostore_exceeds_limit_error_new(
			    "Attribute count or size exceeds limits");
			break;
		default:
			error = isi_system_error_new(errno);
			break;
		}
		goto out;
	}
 out:
	if (error)
		isi_error_handle(error, error_out);

}

void
genobj_attributes_set(int fd,
    const struct iobj_object_attrs *attr_list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out && !(*error_out) && attr_list);

	int count = attr_list->num_entries, i = 0;
	for (i = 0; error == NULL && i < count; i++) {
		struct iobj_obj_attr_entry entry = attr_list->attrs[i];
		/* Set the key/value pair to the object.*/
		single_attribute_set(fd, entry.key,
		    entry.value, entry.value_size, &error);
	}

	isi_error_handle(error, error_out);
}

void
genobj_attribute_set(struct igenobj_handle *iobj, const char *key,
    void *value, size_t value_size, struct isi_error **error_out)
{
	ASSERT(error_out && !(*error_out) && iobj);
	single_attribute_set(iobj->fd, key, value, value_size, error_out);
}
/*
 * Free memory allocated for attribute as returned by
 * iobj_genobj_attribute_get().
 *
 * @param data attribute to be freed
 */
void
genobj_attribute_release(void *data)
{
	if (data)
		free(data);
}

/*
 * Remove all user attributes associated with this fd
 *
 * Note: The current implementation does a attribute listing and then
 * individually deletes listed attributes.  Since this is not an atomic
 * operation, (i.e. attrs can be deleted elsewhere after the listing but
 * before they could be deleted here), any non-existent attrs are ignored.
 *
 * A better implementation would rely on a single system call that deletes all
 * attrs.
 */
void
genobj_attribute_erase(int fd, struct isi_error **error_out)
{
	int count;
	struct isi_error *error = NULL;
	struct ifs_userattr_key attr_key[IFS_USERATTR_MAXKEYS];

	memset(attr_key, 0,
	    IFS_USERATTR_MAXKEYS*sizeof(struct ifs_userattr_key));

	// list
	count = ifs_userattr_list(fd, attr_key);
	if (count < 0) {
		error = isi_system_error_new(errno);
		goto out;
	}
	// delete
	for (int i=0; i < count; ++i) {
		if (ifs_userattr_delete(fd, attr_key[i].key) != 0) {
			if (errno == ENOATTR)
				continue;
			error = isi_system_error_new(errno);
			goto out;
		}
	}
 out:
	if (error)
		isi_error_handle(error, error_out);
}


void
genobj_attribute_delete(struct igenobj_handle *iobj, const char *key,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out);


	if (key) {
		if (ifs_userattr_delete(iobj->fd, key) == -1) {
			if (errno == ENOATTR)
				error = ostore_attribute_not_found_error_new(
				    "Attribute %s not found for object %s",
				    key, iobj->name);
			else
				error = isi_system_error_new(errno);

			goto out;
		}
	} else {
		// delete all
		genobj_attribute_erase(iobj->fd, &error);
	}

 out:
	if (error)
		isi_error_handle(error, error_out);
}



void
genobj_system_attributes_get(struct igenobj_handle *iobj,
    struct stat *stat, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out);

	if (stat == NULL) {
	        error = ostore_invalid_argument_error_new(
		    "the stat parameter must be specified");
		goto out;
	}

	if (fstat(iobj->fd, stat) != 0) {
		error = isi_system_error_new(errno,
		    "Could not get system attributes for object %s",
		    iobj->name);
		goto out;
	}

 out:
	if (error)
		isi_error_handle(error, error_out);
}

void
genobj_close(struct igenobj_handle *igobj, struct isi_error **error_out)
{
	if (igobj->object_type == OOT_CONTAINER)
		bucket_handle_destroy((struct ibucket_handle*)igobj);
	else if (igobj->object_type == OOT_OBJECT)
		object_handle_destroy((struct iobject_handle*)igobj, error_out);
	else
		ASSERT(0, "Wrong object type %d\n", igobj->object_type);
}


const char *
genobj_get_name(struct igenobj_handle *igobj,
    struct isi_error **error)
{
	return igobj->name;
}
