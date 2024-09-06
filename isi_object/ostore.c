/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	dispatch.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "ostore.h"
#include "common.h"
#include "ostore_internal.h"
#include "genobj_acl.h"
#include "genobj_common.h"
#include <stdio.h>

/**
 * This module contains the stubs for the generic interface functions
 * for stores. It dispatches the calls to the registered store implementations.
 */
 
static struct i_store_provider* g_intf[OST_MAX+1] = {
		&ob_provider,
		&ns_provider
};

static enum OSTORE_TYPE __inline
store_type_from_object(struct iobject_handle *iobj)
{
	return iobj->base.ostore->store_type;
}

static enum OSTORE_TYPE __inline
store_type_from_bucket(struct ibucket_handle *ibh)
{
	return ibh->base.ostore->store_type;
}


struct ostore_handle *
iobj_ostore_create(enum OSTORE_TYPE store_type, const char *store_name,
    const char *ns_path,
    struct isi_error **error_out)
{
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.create_store);
	return si->store.create_store(store_name, ns_path, error_out);
}

struct isi_error *
iobj_ostore_destroy(enum OSTORE_TYPE store_type, const char *store_name)
{
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.destroy_store);
	return si->store.destroy_store(store_name);
}

void iobj_ostore_attributes_get(struct ostore_handle *ios,
    struct iobj_ostore_attrs **attrs, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.get_store_attributes);
	si->store.get_store_attributes(ios, attrs, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ostore_attributes_set(struct ostore_handle *ios,
    struct iobj_ostore_attrs *attrs, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.set_store_attributes);
	si->store.set_store_attributes(ios, attrs, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ostore_attributes_release(struct ostore_handle *ios,
    struct iobj_ostore_attrs *attrs)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.release_store_attributes);
	si->store.release_store_attributes(attrs);

out:
	if (error)
		ASSERT(false, "Invalid object store handle.");
}

size_t iobj_ostore_list(enum OSTORE_TYPE store_type,
    const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error)
{
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.list_store);
	return si->store.list_store(acct_name, param, error);
}

struct ostore_handle *
iobj_ostore_open(enum OSTORE_TYPE store_type, const char *store_name, 
    uint64_t snapshot, 
    struct isi_error **error_out)
{
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.open_store);
	return si->store.open_store(store_name, snapshot, error_out);
}

void
iobj_ostore_parameters_release(struct ostore_handle *ios,
    struct ostore_parameters *params)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.release_store_parameters);
	si->store.release_store_parameters(params);

out:
	if (error)
		ASSERT(false, "Invalid object store handle.");
}

void
iobj_ostore_parameters_set(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.set_store_parameters);
	si->store.set_store_parameters(ios, params, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void
iobj_ostore_parameters_get(struct ostore_handle *ios,
    struct ostore_parameters **params, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.get_store_parameters);
	si->store.get_store_parameters(ios, params, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void
iobj_ostore_close(struct ostore_handle *ios, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->store.close_store);
	si->store.close_store(ios, error_out);
out:
	if (error)
		isi_error_handle(error, error_out);
}

void
iobj_ostore_dump(enum OSTORE_TYPE store_type)
{
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	if(si->store.dump_store)
		si->store.dump_store();
}

/*
 * Free the bucket/account to bucket mapping structure
 */
void
iobj_ibucket_name_free(struct ibucket_name *ibn)
{
	if (ibn) {
		if (ibn->bucket_name)
			free(ibn->bucket_name);
		if (ibn->shim_account)
			free(ibn->shim_account);
		free(ibn);
	}
}

struct ibucket_name *
iobj_ibucket_from_acct_bucket(
	struct ostore_handle *ios,
	const char *shim_account,
	const char *shim_bucketname,
	struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ibucket_name *ibn = NULL;

	IOBJ_CHECK_STORE_HANDLE(ios, error, out);
	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->bucket.create_bucket_name);
	ibn = si->bucket.create_bucket_name(ios, shim_account, shim_bucketname,
	    &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return ibn;
}


struct ibucket_handle *
iobj_ibucket_create(struct ibucket_name *ibn, mode_t c_mode,
    const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode acl_mode,
    bool recursive, bool overwrite, struct isi_error **error_out)
{
	struct ibucket_handle *ibh = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_NAME(ibn, error, out);

	enum OSTORE_TYPE store_type = ibn->ostore->store_type;

	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->bucket.create_bucket);
	ibh = si->bucket.create_bucket(ibn, c_mode,
	    attrs, prot, acl_mode, recursive, overwrite, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return ibh;
}

struct ibucket_handle *
iobj_ibucket_open(struct ibucket_name *ibn, enum ifs_ace_rights ace_mode,
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error_out)
{
	struct ibucket_handle *ibh = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_NAME(ibn, error, out);

	enum OSTORE_TYPE store_type = ibn->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->bucket.open_bucket);
	
	ibh = si->bucket.open_bucket(ibn, ace_mode, flags, cf_flags, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return ibh;
}

bool
iobj_ibucket_close(struct ibucket_handle *ibh, struct isi_error **error_out)
{
	bool success = false;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(ibh);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->bucket.close_bucket);
	success = si->bucket.close_bucket(ibh, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return success;
}

struct isi_error *
iobj_ibucket_delete(struct ibucket_name *ibn, bool require_empty)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_NAME(ibn, error, out);

	enum OSTORE_TYPE store_type = ibn->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->bucket.delete_bucket);
	error = si->bucket.delete_bucket(ibn, require_empty);
out:
	return error;
}

void iobj_ibucket_attribute_set(struct ibucket_handle *iobj, const char *key,
    void *value, size_t value_length, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.set_genobj_attribute);
	si->genobj.set_genobj_attribute((struct igenobj_handle *)iobj, key,
	    value, value_length, &error);
out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ibucket_attribute_get(struct ibucket_handle *iobj, const char *key,
    void **value, size_t *value_length, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.get_genobj_attribute);
	si->genobj.get_genobj_attribute((struct igenobj_handle *)iobj, key,
	    value, value_length, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ibucket_attribute_delete(struct ibucket_handle *iobj, const char *key,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.delete_genobj_attribute);
	si->genobj.delete_genobj_attribute((struct igenobj_handle *)iobj,
	    key, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ibucket_attribute_release(void *data)
{
	if (data)
		free(data);
}

int iobj_ibucket_attribute_list(struct ibucket_handle *iobj,
    struct iobj_object_attrs **list, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int count = 0;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.list_genobj_attribute);
	count = si->genobj.list_genobj_attribute((struct igenobj_handle *)iobj,
	    list, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return count;
}

void iobj_ibucket_attribute_list_release(struct ibucket_handle *iobj,
    struct iobj_object_attrs *attr_list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.release_genobj_attribute_list);
	si->genobj.release_genobj_attribute_list(attr_list, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_ibucket_system_attributes_get(struct ibucket_handle *iobj,
    struct stat *stat, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.get_genobj_system_attributes);
	si->genobj.get_genobj_system_attributes((struct igenobj_handle *)iobj,
	    stat, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

struct iobject_handle *
iobj_object_create(
	struct ibucket_handle *ibh, 
	const char *iobj_name, 
	int flags, mode_t c_mode,
	struct iobj_object_attrs *attributes, 
	enum ostore_acl_mode acl_mode,
	bool overwrite,
	struct isi_error **error_out)
{
	struct iobject_handle *iobj = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(ibh);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.create_object);
	iobj = si->object.create_object(ibh, iobj_name, flags, c_mode,
	    attributes, acl_mode, overwrite, &error);
out:
	if (error)
		isi_error_handle(error, error_out);
	return iobj;
}

struct iobject_handle *
iobj_object_open(struct ibucket_handle *ibh, 
    const char *iobj_name,  
    enum ifs_ace_rights object_ace_rights_open_flg, 
    int flags, struct ifs_createfile_flags cf_flags, 
    struct isi_error **error_out)
{
	struct iobject_handle *iobj = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(ibh);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.open_object);
	iobj = si->object.open_object(ibh, iobj_name, 
	    object_ace_rights_open_flg, flags, cf_flags, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return iobj;
}    

bool
iobj_object_close(struct iobject_handle *iobj, struct isi_error **error_out)
{
	bool success = false;
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.close_object);
	success = si->object.close_object(iobj, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return success;
}

struct isi_error *
iobj_object_delete(struct ibucket_handle *ibh, const char *iobj_name)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	enum OSTORE_TYPE store_type = store_type_from_bucket(ibh);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.delete_object);
	error = si->object.delete_object(ibh, iobj_name);

out:
	return error;
}

void
iobj_object_commit(struct iobject_handle *iobj, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.commit_object);
	si->object.commit_object(iobj, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

ssize_t
iobj_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error_out)
{
	ssize_t sz = -1;
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.read_object);
	sz = si->object.read_object(iobj, offset, buf, size, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return sz;
}

ssize_t
iobj_object_write(struct iobject_handle *iobj, off_t offset, 
    void *buf, ssize_t size, struct isi_error **error_out)
{
	ssize_t sz = -1;
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.write_object);
	sz = si->object.write_object(iobj, offset, buf, size, &error);
out:
	if (error)
		isi_error_handle(error, error_out);
	return sz;
}

size_t
iobj_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error_out)
{
	size_t cnt = 0;
	struct isi_error *error = NULL;
	IOBJ_CHECK_BUCKET_HANDLE(param->ibh, error, out);

	enum OSTORE_TYPE store_type = param->ibh->base.ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->object.list_object);
	cnt = si->object.list_object(param, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return cnt;
}

void iobj_object_attribute_set(struct iobject_handle *iobj, const char *key,
    void *value, size_t value_length, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.set_genobj_attribute);
	si->genobj.set_genobj_attribute((struct igenobj_handle *)iobj, key,
	    value, value_length, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_object_attribute_get(struct iobject_handle *iobj, const char *key,
    void **value, size_t *value_length, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.get_genobj_attribute);
	si->genobj.get_genobj_attribute((struct igenobj_handle *)iobj, key,
	    value, value_length, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_object_attribute_delete(struct iobject_handle *iobj, const char *key,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.delete_genobj_attribute);
	si->genobj.delete_genobj_attribute((struct igenobj_handle *)iobj,
	    key, &error);

out:
	if (error)
		isi_error_handle(error, error_out);

}

void iobj_object_attribute_release(void *data)
{
	if (data)
		free(data);
}

int iobj_object_attribute_list(struct iobject_handle *iobj,
    struct iobj_object_attrs **list, struct isi_error **error_out)
{	
	struct isi_error *error = NULL;
	int count = 0;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.list_genobj_attribute);
	count = si->genobj.list_genobj_attribute((struct igenobj_handle *)iobj,
	    list, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return count;
}

void iobj_object_attribute_list_release(struct iobject_handle *iobj,
    struct iobj_object_attrs *attr_list,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.release_genobj_attribute_list);
	si->genobj.release_genobj_attribute_list(attr_list, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_object_system_attributes_get(struct iobject_handle *iobj,
    struct stat *stat, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.get_genobj_system_attributes);
	si->genobj.get_genobj_system_attributes((struct igenobj_handle *)iobj,
	    stat, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void
iobj_object_dump(void)
{

	printf("Dumping all objects is not supported.\n");
}

struct igenobj_handle *
iobj_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags, 
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct igenobj_handle *iobj = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	enum OSTORE_TYPE store_type = ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.open_genobj);
	iobj = si->genobj.open_genobj(ios, path, flags, ace_rights_open_flg,
	    cf_flags, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return iobj;
}

int
iobj_genobj_copy(struct ostore_copy_param *cp_param,
    struct isi_error **error_out)
{
	ASSERT(cp_param);
	int rval = 0;
	struct ostore_handle *src_ios = cp_param->src_ios;
	struct ostore_handle *dest_ios = cp_param->dest_ios;

	struct isi_error *error = NULL;
	IOBJ_CHECK_STORE_HANDLE(src_ios, error, out);
	IOBJ_CHECK_STORE_HANDLE(dest_ios, error, out);

	enum OSTORE_TYPE store_type = src_ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	enum OSTORE_TYPE dest_store_type = dest_ios->store_type;
	ASSERT(store_type == dest_store_type);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.copy_genobj);
	rval = si->genobj.copy_genobj(cp_param, error_out);

out:
	if (error)
		isi_error_handle(error, error_out);
	return rval;

}

int
iobj_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **err_out)
{
	struct isi_error *error = NULL;
	int rval = 0;
	IOBJ_CHECK_STORE_HANDLE(src_ios, error, out);
	IOBJ_CHECK_STORE_HANDLE(dest_ios, error, out);

	enum OSTORE_TYPE store_type = src_ios->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	enum OSTORE_TYPE dest_store_type = dest_ios->store_type;
	ASSERT(store_type == dest_store_type);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.move_genobj);
	rval = si->genobj.move_genobj(src_ios, src,
	    dest_ios, dest, mv_flags, err_out);

out:
	if (error)
		isi_error_handle(error, err_out);
	return rval;

}
enum OSTORE_OBJECT_TYPE
iobj_genobj_get_type(struct igenobj_handle *igobj)
{
	enum OSTORE_OBJECT_TYPE otype = OOT_INVALID;
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj, error, out);

	otype = igobj->object_type;
out:
	if (error)
		ASSERT(false, "Invalid object handle.");

	return otype;
}

const char *
iobj_genobj_get_name(struct igenobj_handle *igobj,
    struct isi_error **error_out)
{
	const char *name = NULL;
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj, error, out);

	enum OSTORE_TYPE store_type = igobj->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.get_genobj_name);
	name = si->genobj.get_genobj_name(igobj, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
	return name;
}

uint64_t
iobj_genobj_get_filerev(struct igenobj_handle *igobj,
    struct isi_error **error_out)
{
	uint64_t filerev = 0;
	struct isi_error *error = NULL;

	IOBJ_CHECK_GENOBJ_HANDLE(igobj, error, out);

	if (getfilerev(igobj->fd, &filerev) != 0)
		error = isi_system_error_new(errno,
		    "Unable to get filerev");

out:
	if (error)
		isi_error_handle(error, error_out);

	return filerev;
}

void
iobj_genobj_close(struct igenobj_handle *igobj, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj, error, out);

	enum OSTORE_TYPE store_type = igobj->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.close_genobj);
	si->genobj.close_genobj(igobj, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}

void iobj_genobj_delete(struct ostore_handle *ios, const char * path,
    struct ostore_del_flags flags, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum OSTORE_TYPE store_type = ios->store_type;

	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	ASSERT(si->genobj.delete_genobj);
	si->genobj.delete_genobj(ios, path, flags, &error);

	if (error)
		isi_error_handle(error, error_out);
}

/*
 * This routine retrieves the acl of the object.
 *
 */
void 
iobj_genobj_acl_get(struct igenobj_handle *igobj_hdl, 
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out, 
    struct isi_error **error_out) 
{

	struct isi_error *error = NULL;
	get_security_descriptor(igobj_hdl, sec_info, 
	    sd_out, &error);
	if (error) {
		goto out;
	}

 out:
	if (error) {
		isi_error_handle(error, error_out);
	}
}


/*
 * This routine modifies the security descriptor with the 
 * modifications input by the user.
 */

void 
iobj_genobj_modify_sd(struct igenobj_handle *igobj_hdl, 
    struct ifs_ace_mod *mods, int num_mods, 
    enum ifs_acl_mod_type acl_mod_type,
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out) 
{
	
	struct isi_error *error = NULL;

	modify_security_descriptor(igobj_hdl, mods, num_mods, 
	    acl_mod_type, sec_info, sd_out, &error);

	if (error) {
		goto out;
	}

 out:
	if (error) {
		isi_error_handle(error, error_out);
	}
}


/*
 * This routine stores the acl of the Bucket/Object.
 *
 */

void 
iobj_genobj_acl_set(struct igenobj_handle *igobj_hdl, 
    struct ifs_security_descriptor *sd,  enum ifs_security_info secinfo, 
    struct isi_error **error_out) 
{

	struct isi_error *error = NULL;

	set_security_descriptor(igobj_hdl, sd, secinfo, &error);

	if (error) {
		goto out;
	}

 out:
	if (error) {
		isi_error_handle(error, error_out);
	}
}

void
iobj_genobj_access_control_set(struct igenobj_handle *igobj_hdl,
    enum ostore_acl_mode acl_mode, int mode, uid_t uid, gid_t gid,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	set_access_control(igobj_hdl, acl_mode, mode, uid, gid, &error);

	if (error)
		goto out;

 out:
	if (error) {
		isi_error_handle(error, error_out);
	}
}

void
iobj_genobj_get_worm_attr(struct igenobj_handle *igobj,
    struct object_worm_attr *worm_attr, struct object_worm_domain *worm_dom,
    struct object_worm_domain **worm_ancs, size_t *num_ancs,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj, error, out);

	enum OSTORE_TYPE store_type = igobj->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	if (!si->genobj.get_worm_attr)
		error = ostore_error_new("Operation not supported");
	else
		si->genobj.get_worm_attr(igobj, worm_attr, worm_dom, worm_ancs,
		    num_ancs, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}


void iobj_genobj_get_protection_attr(struct igenobj_handle *igobj_hdl,
    struct pctl2_get_expattr_args *ge, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj_hdl, error, out);

	enum OSTORE_TYPE store_type = igobj_hdl->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];
	if(!si->genobj.get_protection_attr)
		error = ostore_error_new("Operation not supported");
	else
		si->genobj.get_protection_attr(igobj_hdl, ge, &error);

out:
	if(error)
		isi_error_handle(error, error_out);
}


void iobj_genobj_set_protection_attr(struct igenobj_handle *igobj_hdl,
    struct pctl2_set_expattr_args *se, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_GENOBJ_HANDLE(igobj_hdl, error, out);

	enum OSTORE_TYPE store_type = igobj_hdl->ostore->store_type;
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	if (!si->genobj.set_protection_attr)
		error = ostore_error_new("Operation not supported");
	else
		si->genobj.set_protection_attr(igobj_hdl, se, &error);


out:
	if(error)
		isi_error_handle(error, error_out);

}


void
iobj_object_set_worm_attr(struct iobject_handle *iobj,
    const struct object_worm_set_arg *set_arg, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	enum OSTORE_TYPE store_type = store_type_from_object(iobj);
	ASSERT(store_type <= OST_MAX && store_type >= OST_OBJECT);
	struct i_store_provider *si = g_intf[store_type];

	if (!si->object.set_worm_attr)
		error = ostore_error_new("Operation not supported");
	else
		si->object.set_worm_attr(iobj, set_arg, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}
