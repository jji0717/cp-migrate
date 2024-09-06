/*
 * attribute_manager.cpp
 *
 *  Created on: Mar 15, 2012
 *      Author: lwang2
 */

#include "attribute_manager.h"

void
attribute_manager::get_all_sys_attr(std::list<string> &all_attrs) const
{
	attr_map::const_iterator it;
	for (it = attr_map_.begin(); it != attr_map_.end(); ++it) {
		all_attrs.push_back(it->first);
	}
}

/**
 * Get the definition of an attribute
 */
const attr_def *
attribute_manager::get_attr_def(const std::string &attr) const
{
	attr_map::const_iterator it;
	it = attr_map_.find(attr);

	if (it != attr_map_.end())
		return &it->second;

	return NULL;
}

/**
 * Check if the attribute is valid
 */
bool
attribute_manager::is_valid_attr(const std::string &attr) const
{
	attr_map::const_iterator it;
	it = attr_map_.find(attr);

	return it != attr_map_.end();
}
