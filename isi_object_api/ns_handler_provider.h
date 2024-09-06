/*
 * Copyright (c) 2013
 *	Isilon Systems, LLC.  All rights reserved.
 */

#ifndef __NS_HANDLER_PROVIDER_H_
#define __NS_HANDLER_PROVIDER_H_

#include "store_provider.h"

class common_obj_handler;
class oapi_genobj;
class response;
class request;


/**
 * The handler provider responsible for building pertinent handlers
 * based on input for namespace for an assumed-existing resource.
 */
class ns_handler_provider {
public:
	ns_handler_provider(const request &input, response &output,
	    enum ifs_ace_rights ace_rights_open_flg);

	ns_handler_provider(const request &input, response &output);

	~ns_handler_provider();

	/**
	 * Get the generic object handler
	 */
	common_obj_handler &get_handler();

	/**
	 * Get opened object.
	 */
	oapi_genobj &get_obj() { return *genobj_; }

	/** Get the store object. */
	oapi_store &get_store() { return store_; }

public:
	/**
	 * Routine to open a generic object by given target uri
	 * @param[IN]  store   the oapi_store
	 * @param[IN]  target  the uri of target object
	 * @param[IN]  cf_flags  ifs_createfile flags
	 * @param[IN]  follow_symlinks  (boolean) if true, symlink target file
	 *             is returned.  if false, symlink itself is returned.
	 * @return the oapi_genobj on success.
	 * @throws exception when open failed.
	 *
	 * @note when target is Null, it refers to object for the store access
	 * point. when target is '/' or empty, it refers to object for the 
	 * store target path
	 */
	static oapi_genobj *
	open_genobj(oapi_store &store, const char *target,
	    enum ifs_ace_rights ace_rights_open_flg,
	    struct ifs_createfile_flags cf_flags, bool follow_symlinks=true);

private:
	const request &input_;
	response &output_;
	oapi_genobj *genobj_;
	oapi_store store_;

	ns_handler_provider(const ns_handler_provider &other);
	ns_handler_provider &operator =(ns_handler_provider &other);
};


#endif  //__NS_HANDLER_PROVIDER_H_
