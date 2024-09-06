#include "oapi_bucket.h"

#include "oapi_store.h"
#include "isi_object_api/common_obj_handler.h"
#include "isi_object_api/ns_object_protection.h"
#include <isi_util_cpp/isi_exception.h>

namespace {
// private class visible only in this module
class oapi_bucket_name {
public:
	oapi_bucket_name(const oapi_store &store, const char *acct_name,
	    const char *bucket_name);
	~oapi_bucket_name();
	ibucket_name *get_handle() {return bucket_name_;}
private:
	ibucket_name * bucket_name_;
};


oapi_bucket_name::oapi_bucket_name(const oapi_store &store,
    const char *acct_name,
    const char *bucket_name) : bucket_name_(NULL)
{
	
	bucket_name_ = iobj_ibucket_from_acct_bucket(store.get_handle(),
	    (char*)acct_name, (char*)bucket_name, isi_error_suppress());
}
	
oapi_bucket_name::~oapi_bucket_name()
{
	if(bucket_name_)
		iobj_ibucket_name_free(bucket_name_);
}

}

void
oapi_bucket::create(const char *acct_name, const char *bucket_name,
    mode_t create_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot, bool recursive,
    enum ostore_acl_mode acl_mode, bool overwrite,
    isi_error *&error)
{
	oapi_bucket_name bn(get_store(), acct_name, bucket_name);

	ASSERT(handle_ == NULL);
	handle_ = (igenobj_handle*)iobj_ibucket_create(bn.get_handle(), 
	    create_mode, attrs, prot, acl_mode, recursive, overwrite, &error);
	if (error)
		isi_error_add_context(error);
	else
		set_path(bucket_name);
}

void
oapi_bucket::open(const char *acct_name,
    const char *bucket_name, enum ifs_ace_rights ace_mode, int flags, 
    struct ifs_createfile_flags cf_flags, isi_error *&error)
{
	oapi_bucket_name bn(get_store(), acct_name, bucket_name);

	ASSERT(handle_ == NULL);
	handle_ = (igenobj_handle*)iobj_ibucket_open(bn.get_handle(),
	    ace_mode, flags, cf_flags, &error);
	if (error)
		isi_error_add_context(error);
	else
		set_path(bucket_name);
}

void
oapi_bucket::get_sys_attr(object_sys_attr &sys_attr, isi_error *&error)

{
	ASSERT(handle_ != 0);

	iobj_ibucket_system_attributes_get(get_handle(), &sys_attr.attr_, &error);

	if (error) {
		isi_error_add_context(error);
		return;
	}

	sys_attr.objrev = iobj_genobj_get_filerev(oapi_genobj::get_handle(),
	    &error);
	if (error) {
		isi_error_add_context(error);
		return;
	}
}

void
oapi_bucket::set_attr(const char *key, const void *value, size_t value_len,
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_ibucket_attribute_set(get_handle(), key,
	    (void*)value, value_len, &error);

	if (error) {
		isi_error_add_context(error);
	}
}

bool
oapi_bucket::get_attr(const char *key, oapi_attrval &attr_val,
    isi_error *&error)
{
	bool found = true;
	ASSERT(handle_ != 0);

	void *val = NULL;
	size_t val_len = 0;
	iobj_ibucket_attribute_get(get_handle(), key,
	    &val, &val_len, &error);

	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_ATTRIBUTE_NOT_FOUND_ERROR_CLASS)) {
			// ignore the error
			isi_error_free(error);
			error = NULL;
		}
		else
			isi_error_add_context(error);
		found = false;
	}

	if (!error && val)
		attr_val.set_value(val, val_len);

	return found;
}

void
oapi_bucket::delete_attr(const char *key, isi_error *& error)
{
	ASSERT(handle_ != 0);

	iobj_ibucket_attribute_delete(get_handle(), key,
	    &error);

	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_ATTRIBUTE_NOT_FOUND_ERROR_CLASS)) {
			// ignore the error
			isi_error_free(error);
			error = NULL;
		}
		else
			isi_error_add_context(error);
	}
}

void
oapi_bucket::list_attrs(std::map<std::string, std::string> &attrs,
    isi_error *&error)
{
	iobj_object_attrs *attr_list = NULL;

	do {
		iobj_ibucket_attribute_list(get_handle(), &attr_list, &error);

		if (error) {
			isi_error_add_context(error);
			break;
		}

		if (attr_list == NULL || attr_list->num_entries == 0)
			break;

		std::string val;
		for (int i = 0; i < attr_list->num_entries; ++i) {
			iobj_obj_attr_entry &entry =
			    attr_list->attrs[i];

			ilog(IL_DEBUG, " %s attr: %p %d",
			    entry.key,
			    entry.value, entry.value_size);

			val.assign((const char*)entry.value, entry.value_size);
			attrs[entry.key] = val;
		}
	} while (false);

	if (attr_list) {
		iobj_ibucket_attribute_list_release(get_handle(),
		    attr_list, &error);
		if (error) {
			isi_error_add_context(error);
		}
	}
}


void
oapi_bucket::remove(const char *acct_name,
    const char *bucket_name, isi_error *&error )
{
	oapi_bucket_name bn(get_store(), acct_name, bucket_name);

	ASSERT(handle_ == NULL);
	error = iobj_ibucket_delete(bn.get_handle(), true);
	if (error)
		isi_error_add_context(error);
}

void
oapi_bucket::close()
{
	if (handle_) {
		iobj_ibucket_close(get_handle(), isi_error_suppress());
		handle_ = NULL;
	}	
}


oapi_bucket::~oapi_bucket()
{
	close();
}
