#ifndef __OBJECT_HANDLER_H__
#define __OBJECT_HANDLER_H__

#include <isi_rest_server/multipart_processor.h>
#include "isi_rest_server/uri_handler.h"

#include "common_obj_handler.h"
#include "oapi/oapi_object.h"

class object_handler : public uri_handler, public common_obj_handler {
public:
	object_handler() : uri_handler()
	{
		supported_methods_[request::GET] = true;
		supported_methods_[request::PUT] = true;
		supported_methods_[request::DELETE] = true;
		supported_methods_[request::HEAD] = true;
	}

	virtual void http_get(const request &input, response &output);
	virtual void http_put(const request &input, response &output);
	virtual void http_delete(const request &input, response &output);
	virtual void http_head(const request &input, response &output);
	virtual bool validate_input(const request &input, response &output);

	// The following should be refactored into store type specific
	// validators
	static void validate_object_name(const std::string &bucket_name,
	    const std::string &object_name);

	static void validate_object_name(const std::string &object_name);

	/**
	 * Get the data from the object opened.
	 */
	virtual void get_content(oapi_genobj &object, const request &input,
	    response &output);

	// implementing interface for genobj_handler:
	virtual void set_content_encoding_header(oapi_genobj &object,
	    const request &input, response &output);

	virtual void set_content_type_header(oapi_genobj &object,
	    response &output);

	virtual void handle_post(oapi_genobj &object, const request &input,
	    response &output);
	/**
	 * Function responsible for putting object
	 * Refactored out from http_put.
	 * Can throw std::exception
	 */
	void put_object(const request &input,  response &output);


private:
	/**
	 * private methods for put attributes
	 */
	void put_attribute(const request &input, response &output);

	/**
	 * private methods for get attributes
	 */
	void get_attribute(const request &input, response &output);

};

/**
 * Class implementing ipart_handler interface.
 * Which can handle multipart/form-data and regular content-type data
 */
class object_part_handler : public ipart_handler {
public:
	object_part_handler(oapi_bucket &bucket, const request &input,
	    response &output);

	virtual bool get_header(const std::string &header, std::string &value)
	    const;

	/**
	 * Notified by the processor when headers are captured.
	 */
	virtual bool on_headers_ready();

	/**
	 * Notified by the processor of the data.
	 * @param buff - the data buffer
	 * @param len - the length of the data
	 * @return true if the handler wishes to continue to receive data
	 * false otherwise
	 */
	virtual bool on_new_data(const char *buff, size_t len);

	/**
	 * Notified by the process of the end of data
	 * @return true if the handler wishes to continue to receive data
	 * false otherwise
	 */
	virtual void on_data_end();

	/**
	 * clear the handler
	 */
	virtual void clear();

	/**
	 * Get user defined attributes via HTTP headers
	 */
	void get_input_attributes(std::map<std::string, std::string> &xattrs);

	/**
	 * Get input parameters;
	 * This function is to get the object name.
	 */
	virtual void get_input_parameters(std::string &obj_name);

	void set_attrs();
	/**
	 * Accessors:
	 */
	const request &input() {return input_;}

	response &output() {return output_;}
private:
	/**
	 * Maps common error to api_exception
	 * @param error  pointer to an isi_error object
	 * @param do_what a string context for the error
	 * @param which second context string for the error
	 * @throw api_exception appropriately mapped from the error
	 */
	void throw_on_error(isi_error *error, const char *do_what,
	    const char *which);

private:
	const request &input_;
	response &output_;
	oapi_object object_;
	off_t offset_;
	oapi_bucket &bucket_;
};

#endif

