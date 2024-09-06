/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	oapi_acl.h __ lchampakesan. 
 */

#ifndef __OAPI_ACL_H__
#define __OAPI_ACL_H__

#include <isi_acl/isi_sd.h>

class oapi_acl {

public:
        oapi_acl() : ace_mods_cnt_(0), ace_mods_(0) {}
        ~oapi_acl();

	struct ifs_ace_mod *get_ace_mods() {return ace_mods_;}
	int get_ace_mods_cnt() {return ace_mods_cnt_;}

	/* Allocate the ace_mods. */
	void allocate_ace_mods(int cnt);


private:
	int ace_mods_cnt_;
	struct ifs_ace_mod *ace_mods_;

	void free_acl_memory();

	// not implemented
	oapi_acl(const oapi_acl &other);
	oapi_acl &operator = (const oapi_acl &other);
};

#endif
