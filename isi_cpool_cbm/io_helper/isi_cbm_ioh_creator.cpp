
/**
 * @file
 *
 * Cloud IO helper factory for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "isi_cbm_ioh_creator.h"

namespace {

typedef std::map<cl_provider_type, ioh_create_fun> helper_creator_map;

/** Returns global io helper creation function map */
helper_creator_map &
get_io_creator_map()
{
	static helper_creator_map c_map_;
	return c_map_;
}

}

isi_cbm_ioh_base *
isi_cbm_ioh_creator::get_io_helper(enum cl_provider_type type,
    const isi_cbm_account &acct)
{
	helper_creator_map &c_map = get_io_creator_map();
	helper_creator_map::iterator itr = c_map.find(type);
	if (itr != c_map.end())
		return (itr->second)(acct);
	else
		return NULL;
}


isi_cbm_ioh_register::isi_cbm_ioh_register(enum cl_provider_type type,
    ioh_create_fun creator)
{
	helper_creator_map &c_map = get_io_creator_map();
	// yli-xxx: we need to protect the map if we truly support
	// pluggable io helper
	c_map[type] = creator;
}

