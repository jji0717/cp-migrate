#include "oapi_acct.h"

#include <vector>
#include <errno.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

#include "oapi_store.h"

oapi_acct::~oapi_acct()
{

}

void
oapi_acct::list_bucket(oapi_store &store, LIST_BUCKET_CALLBACK callback, void *context,
    isi_error *&error)
{
	iobj_ibucket_list(store.get_handle(), callback, context, &error);
}

