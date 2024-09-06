/*
 * ob_attr_mgr.h
 *
 *  Created on: Mar 15, 2012
 *      Author: lwang2
 */

#ifndef __OB_ATTR_MGR_H__
#define __OB_ATTR_MGR_H__

#include "attribute_manager.h"

class ob_attr_mgr : public attribute_manager {
public:
	/**
	 * Get default list of attributes
	 */
	void get_default(std::list<string> &default_attrs) const;

	static const attribute_manager &get_attr_mgr();

private:
	ob_attr_mgr();
};

#endif /* __OB_ATTR_MGR_H__ */
