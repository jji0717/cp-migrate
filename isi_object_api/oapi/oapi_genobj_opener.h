#ifndef __OAPI_GENOBJ_OPENER_H__
#define __OAPI_GENOBJ_OPENER_H__

#include "oapi_genobj.h"

//A factory class to open a generic object.
class oapi_genobj_opener {
public:
	/**
	 * Open an object with the given name.
	 * @param[IN] store - the store
	 * @param[IN] path - object path
	 * @param[IN] flags - specifying read or write characteristics
	 *                    (same as flags in libc open).
	 * @param[IN] ace_rights_open_flg - Specify the access rights 
	 *                    with which, the directory/file should be 
	 *                    opened.
	 * @param[IN] cf_flags - specify the ifs_createfile flags
	 * @param[OUT] error - the error on failure
	 */
	oapi_genobj *open(oapi_store &store, const char *path, 
	    int flags, enum ifs_ace_rights ace_rights_open_flg,
	    struct ifs_createfile_flags cf_flags,
	    isi_error *&error);

};

#endif
