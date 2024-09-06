/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	oapi_acl.cpp __ lchampakesan. 
 */

#include "oapi_acl.h"

#include <isi_util_cpp/isi_exception.h>

void 
oapi_acl::allocate_ace_mods(int cnt)
{
	// Free allocated memory if any.
	free_acl_memory();

	if (cnt == 0) {
		ace_mods_ = 0;
		ace_mods_cnt_ = 0;
		return;
	}

	int err = ifs_allocate_ace_mod(&ace_mods_, cnt);
	if (err) {
		throw isi_exception(isi_system_error_new(err, 
			"Error while allocating memory. Internal error. "));
	}
	
	ace_mods_cnt_ = cnt;
}

void
oapi_acl::free_acl_memory() 
{
	if (ace_mods_) {
		ifs_free_ace_mod(ace_mods_, ace_mods_cnt_);
	}

	// Initialize the values.
	ace_mods_cnt_ = 0;
	ace_mods_ = 0;
}

oapi_acl::~oapi_acl()
{
	free_acl_memory();
}
