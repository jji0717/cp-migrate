/*
 * ns_attr_mgr.cpp
 *
 *  Created on: Mar 15, 2012
 *      Author: lwang2
 */

#include "ns_attr_mgr.h"
#include "handler_common.h"

// avoid unit test memory leak error
static const attribute_manager &g_ns_attr_mgr = ns_attr_mgr::get_attr_mgr();

const attribute_manager &
ns_attr_mgr::get_attr_mgr()
{
	static ns_attr_mgr mgr;
	return mgr;
}

ns_attr_mgr::ns_attr_mgr()
{
	attr_map_[ISI_NAME] = attr_def(ISI_NAME, OAT_STRING);
	attr_map_[ISI_SIZE] = attr_def(ISI_SIZE, OAT_INT);
	attr_map_[ISI_BLK_SIZE] = attr_def(ISI_BLK_SIZE, OAT_INT);
	attr_map_[ISI_BLOCKS] = attr_def(ISI_BLOCKS, OAT_INT);

	attr_map_[ISI_MTIME_VAL] = attr_def(ISI_MTIME_VAL, OAT_INT);
	attr_map_[ISI_CTIME_VAL] = attr_def(ISI_CTIME_VAL, OAT_INT);
	attr_map_[ISI_ATIME_VAL] = attr_def(ISI_ATIME_VAL, OAT_INT);
	attr_map_[ISI_BTIME_VAL] = attr_def(ISI_BTIME_VAL, OAT_INT);

	attr_map_[ISI_MTIME] = attr_def(ISI_MTIME, OAT_STRING,
	    ISI_MTIME_VAL, date_str_from_val);
	attr_map_[ISI_CTIME] = attr_def(ISI_CTIME, OAT_STRING,
	    ISI_CTIME_VAL, date_str_from_val);
	attr_map_[ISI_ATIME] = attr_def(ISI_ATIME, OAT_STRING,
	    ISI_ATIME_VAL, date_str_from_val);
	attr_map_[ISI_BTIME] = attr_def(ISI_BTIME, OAT_STRING,
	    ISI_BTIME_VAL, date_str_from_val);

	attr_map_[ISI_ID] = attr_def(ISI_ID, OAT_INT);
	attr_map_[ISI_NLINK] = attr_def(ISI_NLINK, OAT_INT);
	attr_map_[ISI_HIDDEN] = attr_def(ISI_HIDDEN, OAT_BOOL);
	attr_map_[ISI_ACC_MODE] = attr_def(ISI_ACC_MODE, OAT_INT);
	attr_map_[ISI_OWNER] = attr_def(ISI_OWNER, OAT_STRING,
	    ISI_UID, user_name_from_id);
	attr_map_[ISI_GROUP] = attr_def(ISI_OWNER, OAT_STRING,
	    ISI_GID, group_name_from_id);
	attr_map_[ISI_GID] = attr_def(ISI_GID, OAT_INT);
	attr_map_[ISI_UID] = attr_def(ISI_UID, OAT_INT);
	attr_map_[ISI_CONTENT_TYPE] = attr_def(ISI_CONTENT_TYPE, OAT_STRING);
	attr_map_[ISI_OBJECT_TYPE] = attr_def(ISI_OBJECT_TYPE, OAT_STRING);
	attr_map_[ISI_STUB] = attr_def(ISI_STUB, OAT_BOOL);
	attr_map_[ISI_SYMBLNK_TYPE] = attr_def(ISI_SYMBLNK_TYPE, OAT_STRING);
	attr_map_[ISI_URL] = attr_def(ISI_UID, OAT_STRING);
	attr_map_[ISI_BUCKET] = attr_def(ISI_BUCKET, OAT_STRING);
	attr_map_[ISI_CONTAINER_PATH] = attr_def(ISI_CONTAINER_PATH, OAT_STRING);
}


void
ns_attr_mgr::get_default(std::list<string> &default_attrs) const
{
	// similar to what you will get from "ls -l"
	default_attrs.push_back(ISI_NAME);
	default_attrs.push_back(ISI_SIZE);
	default_attrs.push_back(ISI_OWNER);
	default_attrs.push_back(ISI_GROUP);
	default_attrs.push_back(ISI_ACC_MODE);
	default_attrs.push_back(ISI_OBJECT_TYPE);
	default_attrs.push_back(ISI_STUB);
	default_attrs.push_back(ISI_SYMBLNK_TYPE);
	default_attrs.push_back(ISI_MTIME);
}
