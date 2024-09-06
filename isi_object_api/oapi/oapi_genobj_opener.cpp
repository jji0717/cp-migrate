#include "oapi_genobj_opener.h"

#include "oapi_bucket.h"
#include "oapi_object.h"
#include <isi_rest_server/api_error.h>

oapi_genobj *
oapi_genobj_opener::open(oapi_store &store, const char *path, int flags, 
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags, isi_error *&error)
{
	oapi_genobj *obj = NULL;
	igenobj_handle *oh = iobj_genobj_open(store.get_handle(),
	    path, flags, ace_rights_open_flg, cf_flags, &error);

	if (error) {
		isi_error_add_context(error);
		return NULL;
	}
	
	OSTORE_OBJECT_TYPE otype = iobj_genobj_get_type(oh);
	
	if (otype == OOT_CONTAINER) {
		// create a container
		obj = new oapi_bucket(store, oh, path);
	}
	else if (otype == OOT_OBJECT) {
		// create an object
		obj = new oapi_object(store, oh, path);
	}
	else {
		iobj_genobj_close(oh, isi_error_suppress());
		
		throw api_exception(AEC_FORBIDDEN, "Cannot open the object %s."
		    " The object type is not supported.",
		    path);
	}
	
	return obj;
}

