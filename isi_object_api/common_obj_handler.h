/*
 * common_obj_handler.h
 *
 *  Created on: Mar 10, 2012
 *      Author: lijun
 */

#ifndef __COMMON_OBJ_HANDLER_H__
#define __COMMON_OBJ_HANDLER_H__

#include "isi_rest_server/api_headers.h"
#include "oapi/oapi_genobj.h"

#define STANDARD_BITS   (S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO)

/** error handling for setting access */
class set_access_error_handler {
public:
	/**
	 * Maps isi_error to api_exception
	 * @param error  pointer to an isi_error object
	 * @throw api_exception appropriately mapped from the error
	 */
	static void throw_on_error(isi_error *error);
};

class request;
class response;

/**
 * Common object handler responsible for transferring properties common to all
 * objects: containers and objects.
 */
class common_obj_handler {
public:
	/**
	 * Get x-isi-ifs-attr- attributes from the request headers
	 * @param[OUT] xattrs - the key-value map
	 */
	static void get_isi_attributes_to_map(
	    const api_header_map &hdr_map,
	    std::map<std::string, std::string> &xattrs);

	/**
	 * Get the attribute list based on headers from request
	 * @param[IN] input  a request object
	 * @return attribute list
	 */
	static struct iobj_object_attrs *get_obj_attr_list(
	    const request &input);

	/**
	 * Get pre-defined acl or the octal mode number from input header
	 *
	 * @param[IN] store  store object in which the pre-defined acl
	 *                   is defined
	 * @param[IN] input  a request object
	 * @param[OUT] acl_mode pre-defined acl mode if specified in input
	 * @param[OUT] mode the access mode number if specified in input
	 * @param[IN] throw_if_not_found reporting as error if not found
	 *                   by throw api_exception
	 * @throw api_exception if mode number is invalid
	 */
	static void get_access_control(const request &input,
	    const oapi_store &store, enum ostore_acl_mode &acl_mode,
	    mode_t &mode, bool throw_if_not_found = true);

	/**
	 * Get the octal mode number from input access control string
	 * @param[IN] acc_ctrl access control string
	 * @return the actual mode number converted
	 * @throw api_exception if mode number is invalid
	 */
	static mode_t get_create_mode(const std::string &acc_ctrl);

         /*
          * Get the pre-defined acl from the input.
	  */
	static enum ostore_acl_mode get_predefined_acl(const request &input, 
		const oapi_store &store);

	virtual void set_attribute_headers(oapi_genobj &object,
	    response &output);

	virtual void get_head_info(oapi_genobj &object,
	    const request &input,
	    response &output);

	virtual void set_content_encoding_header(oapi_genobj &object,
	    const request &input, response &output) = 0;

	virtual void set_common_headers(oapi_genobj &object,
	    const request &input,
	    response &output,
	    oapi_genobj::object_sys_attr &sys_attr, 
	    bool head_method);

	virtual void set_content_type_header(oapi_genobj &object,
	    response &output) = 0;

	virtual void set_target_type_header(oapi_genobj &object,
	    response &output);

	virtual void set_basic_system_attributes(oapi_genobj &object, 
	    const request &input, response &output, 
	    oapi_genobj::object_sys_attr &sys_attr, 
	    bool head_method);

	bool handle_conditional_get(
	    const std::string & obj_name,
	    const oapi_genobj::object_sys_attr &attr,
	    const request &input, response &output);

	/**
	 * Get the metadata from the object opened.
	 */
	virtual void get_attributes(oapi_genobj &object, const request &input,
	    response &output);

	/**
	 * Put the metadata for the object opened.
	 */
	virtual void put_attributes(oapi_genobj &object, const request &input,
	    response &output);

	/**
	 * Get the content from the object opened.
	 * In the case of data objects, the content
	 * In the case of containers, the directory contents.
	 */
	virtual void get_content(oapi_genobj &object, const request &input,
	    response &output) = 0;

	/**
	 * Handle POST operation on an object
	 */
	virtual void handle_post(oapi_genobj &object, const request &input,
	    response &output) = 0;

	/**
	 * Get the acl of the object entity opened.
	 */
	virtual void get_acl(oapi_genobj &genobject, 
	    enum ifs_security_info sec_info,
	    ifs_security_descriptor **sd_out);

	/**
	 * Set the acl of the object entity opened.
	 */
	virtual void set_acl(const request &input, 
	    oapi_genobj &genobject);

protected:
	common_obj_handler() {};
	virtual ~common_obj_handler() {};

	/**
	 * Check content type characters for invalid characters in security
	 */
	bool check_content_type_invalid_chars(const std::string& content_type_value);
};

std::string get_protection_level_str(struct protection_policy &level);

std::string get_protection_policy_str(struct protection_policy &policy);

std::string get_ssd_strategy_str(enum ifs_ssd_strategy strategy);

#endif /* __COMMON_OBJ_HANDLER_H__ */
