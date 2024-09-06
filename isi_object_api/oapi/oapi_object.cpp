#include "oapi_object.h"

#include <errno.h>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util/isi_printf.h>
#include <isi_object/iobj_ostore_protocol.h>

#include "oapi_store.h"

void
oapi_object::create(const char *acct_name,
    const char *bucket_name,
    enum ifs_ace_rights bucket_open_ace_mode,
    const char *object_name,
    mode_t c_mode, enum ostore_acl_mode acl_mode, 
    bool overwrite, isi_error *&error)
{
	ASSERT(handle_ == 0);
	bucket_.open(acct_name, bucket_name, bucket_open_ace_mode, 
	    O_DIRECTORY, CF_FLAGS_NONE, error);
	if (error) {
		isi_error_add_context(error);
		return;
	}
	create(bucket_, object_name, c_mode, acl_mode, overwrite, error);
}

void
oapi_object::create(const oapi_bucket &bucket,
    const char *object_name, mode_t c_mode, 
    enum ostore_acl_mode acl_mode, bool overwrite, 
    isi_error *&error)
{
	ASSERT(handle_ == 0);

	handle_ = (igenobj_handle *)iobj_object_create(bucket.get_handle(),
	    const_cast<char*>(object_name),
	    0, c_mode, NULL, acl_mode, overwrite, &error);

	if (error) {
		isi_error_add_context(error);
	}
	else {
		name_ = object_name;
		created_ = true;
		std::string path(bucket.get_path());
		path += name_;
		set_path(path);
	}
}


void
oapi_object::open(const char *acct_name,
    const char *bucket_name,
    enum ifs_ace_rights bucket_open_ace_mode,
    const char *object_name, enum ifs_ace_rights obj_open_ace_mode,
    int flags, struct ifs_createfile_flags cf_flags, isi_error *&error)
{
	ASSERT(handle_ == 0);

	bucket_.open(acct_name, bucket_name, bucket_open_ace_mode, 
	    O_DIRECTORY, CF_FLAGS_NONE, error);
	if (error) {
		isi_error_add_context(error);
		return;
	}

	handle_ = (igenobj_handle *)iobj_object_open(bucket_.get_handle(),
	    object_name, obj_open_ace_mode, flags, cf_flags, &error);
	    
	if (error) {
		isi_error_add_context(error);
		return;
	}
	else {
		name_ = object_name;
		std::string path(bucket_.get_path());
		path += name_;
		set_path(path);
	}
}

void
oapi_object::remove(const char *acct_name,
    const char *bucket_name,
    const char *object_name, isi_error *&error)
{
	ASSERT(handle_ == 0);

	bucket_.open(acct_name, bucket_name, 
	    (enum ifs_ace_rights) BUCKET_WRITE, 
	    O_DIRECTORY, CF_FLAGS_NONE, error);
	if (error) {
		isi_error_add_context(error);
		return;
	}

	error = iobj_object_delete(bucket_.get_handle(),
	    object_name);
	    
	if (error) {
		isi_error_add_context(error);
		return;
	}
}

void
oapi_object::write(const void *data, off_t offset, size_t len,
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	size_t writeLen = iobj_object_write(get_handle(), offset,
	    (char*)data,
	    len, &error);

	ASSERT(error || writeLen == len);

	if (error) {
		isi_error_add_context(error);
	}
}

void
oapi_object::read(void *data, off_t offset, size_t buff_len,
    size_t &read_len, isi_error *&error)
{
	ASSERT(handle_ != 0);

	read_len = iobj_object_read(get_handle(), offset,
	    data, buff_len, &error);

	ASSERT(error || read_len >= 0);

	if (error || read_len < 0) {
		isi_error_add_context(error);
	}
}

void
oapi_object::get_sys_attr(object_sys_attr &sys_attr, isi_error *&error)

{
	ASSERT(handle_ != 0);

	iobj_object_system_attributes_get(get_handle(), &sys_attr.attr_,
	    &error);

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
oapi_object::set_attr(const char *key, const void *value, size_t value_len,
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_object_attribute_set(get_handle(), key,
	    (void*)value, value_len, &error);
	    
	if (error) {
		isi_error_add_context(error);
	}
}

bool
oapi_object::get_attr(const char *key, oapi_attrval &attr_val,
    isi_error *&error)
{
	bool found = true;
	ASSERT(handle_ != 0);

	void *val = NULL;
	size_t val_len = 0;
	iobj_object_attribute_get(get_handle(), key,
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
oapi_object::delete_attr(const char *key, isi_error *& error)
{
	ASSERT(handle_ != 0);

	iobj_object_attribute_delete(get_handle(), key,
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
oapi_object::list_attrs(std::map<std::string, std::string> &attrs,
    isi_error *&error)
{
	iobj_object_attrs *attr_list = NULL;

	do {
		iobj_object_attribute_list(get_handle(), &attr_list, &error);

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
		iobj_object_attribute_list_release(get_handle(),
		    attr_list, &error);
		if (error) {
			isi_error_add_context(error);
		}
	}
}			

void
oapi_object::commit(isi_error *&error)
{
	ASSERT(created_);
	if (handle_) {
		iobj_object_commit(get_handle(), &error);
		if (error) {
			isi_error_add_context(error);
		}
	}
}

void
oapi_object::close()
{
	if (handle_) {
		iobj_object_close(get_handle(),
		    isi_error_suppress());
		handle_ = NULL;
	}
	bucket_.close();
}

oapi_object::~oapi_object()
{
	try {
		if (handle_) {
			close();
		}

	}
	catch (...) {
		// swallow the exceptions if any
	}
}

void
oapi_object::set_worm_attr(const object_worm_set_arg &set_arg,
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_object_set_worm_attr(get_handle(), &set_arg,
	    &error);
}


