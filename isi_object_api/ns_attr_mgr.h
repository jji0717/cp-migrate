/*
 * ns_attr_mgr.h
 *
 *  Created on: Mar 15, 2012
 *      Author: lwang2
 */

#ifndef __NS_ATTR_MGR_H__
#define __NS_ATTR_MGR_H__

#include "attribute_manager.h"


class ns_attr_mgr : public attribute_manager {
public:
	/**
	 * Get default list of attributes
	 */
	void get_default(std::list<string> &default_attrs) const;

	static const attribute_manager &get_attr_mgr();

private:
	ns_attr_mgr();
};

#endif /* __NS_ATTR_MGR_H__ */
