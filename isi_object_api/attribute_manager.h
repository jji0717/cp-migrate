/*
 * attribute_manager.h
 *
 *  Created on: Mar 15, 2012
 *      Author: lwang2
 */

#ifndef __ATTRIBUTE_MANAGER_H__
#define __ATTRIBUTE_MANAGER_H__

#include <string>
#include <list>
#include <map>

namespace Json {
	class Value;
}


using std::string;

enum oapi_attr_type {
	OAT_INVALID = -1,
	OAT_STRING = 0,
	OAT_DATE = 1,
	OAT_INT = 2,
	OAT_BOOL = 3
};

typedef void (*attr_converter)(const Json::Value &src, Json::Value &dest);

/**
 * Function to convert from GID to group name
 */

void group_name_from_id(const Json::Value &src, Json::Value &dest);

/**
 * Function to convert from UID to user name
 */
void user_name_from_id(const Json::Value &src, Json::Value &dest);


/**
 * Function to convert from time_t to HTTP date string
 */
void date_str_from_val(const Json::Value &src, Json::Value &dest);

// definition of an attribute
struct attr_def {

	/**
	 * Define an attribute given the name and types
	 */
	attr_def(const std::string &name,
	    oapi_attr_type val_type)
	    : name_(name), disp_name_(name), val_type_(val_type)
	    , converter_(NULL) { }

	/**
	 * Define an attribute whose value derives from another attribute
	 */
	attr_def(const std::string &name,
	    oapi_attr_type val_type,
	    const std::string &src_attr,
	    attr_converter func
	    )
	    : name_(name), disp_name_(name), val_type_(val_type)
	    , src_attr_(src_attr), converter_(func) { }

	/**
	 * Define an attribute given both name and the display name
	 */
	attr_def(const std::string &name, const std::string &disp_name,
	    oapi_attr_type val_type)
	    : name_(name), disp_name_(disp_name), val_type_(val_type)
	    , converter_(NULL) { }

	attr_def(oapi_attr_type val_type) : val_type_(val_type) { }

	attr_def() { }

	std::string name_;
	std::string disp_name_;
	oapi_attr_type val_type_;
	std::string src_attr_;
	attr_converter converter_;
};

class attribute_manager {
	typedef std::map<const std::string, attr_def> attr_map;

public:
	/**
	 * Get default list of attributes
	 */
	virtual void get_default(std::list<string> &default_attrs) const = 0;

	/**
	 * Get all system attributes
	 */
	void get_all_sys_attr(std::list<string> &all_attrs) const;

	/**
	 * Get the definition of an attribute
	 */
	const attr_def *get_attr_def(const std::string &attr) const;

	/**
	 * Check if the attribute is valid
	 */
	bool is_valid_attr(const std::string &attr) const;

protected:
	attr_map attr_map_;
};

#endif /* __ATTRIBUTE_MANAGER_H__ */
