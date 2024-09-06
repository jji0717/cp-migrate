#include <errno.h>

#include "oapi_store.h"
#include <ifs/ifs_types.h>
#include <isi_object/ostore_internal.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_util_cpp/isi_exception.h>

#include "isi_ilog/ilog.h"

oapi_store::~oapi_store()
{
	close(true);
}
		
void
oapi_store::open_store(OSTORE_TYPE store_type,
    const std::string &store,
    isi_error *&error)
{
	ASSERT(store_handle_ == NULL);
	store_handle_ = iobj_ostore_open(
	    store_type,
	    store.c_str(),
	    HEAD_SNAPID,
	    &error);

	if (error == NULL) {
		store_type_ = store_type;
		store_name_ = store;
	}

}

void
oapi_store::create_store(OSTORE_TYPE store_type,
    const std::string &store,
    const char *ns_path,
    isi_error *&error)
{
	ASSERT(store_handle_ == NULL);

	store_handle_ = iobj_ostore_create(
	    store_type,
	    store.c_str(),
	    ns_path,
	    &error);

	if (error == NULL) {
		store_type_ = store_type;
		store_name_ = store;
	}
}


void
oapi_store::destroy_store(isi_error *&error)
{
	ASSERT( store_handle_ != NULL);

	error = iobj_ostore_destroy(store_type_,
	    store_handle_->name);
	if (!error)
		close(true);
}

isi_error *
oapi_store::close(bool suppress)
{
	isi_error *error = 0;
	if (store_handle_) {
		iobj_ostore_close((ostore_handle *)store_handle_, &error);
		if (!error) {
			store_handle_ = NULL;
			store_type_ = OST_INVALID;
		}
		if (error && suppress) {
			ilog(IL_ERR, "Unable to close store; error=%#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = 0;
		}
	}
	return error;
}
