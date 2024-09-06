#ifndef __BUCKET_HANDLER_H__
#define __BUCKET_HANDLER_H__

#include <isi_rest_server/uri_handler.h>
#include "common_obj_handler.h"

class bucket_handler : public uri_handler, public common_obj_handler {
public:
	bucket_handler() : uri_handler()
	{
		supported_methods_[request::GET] = true;
		supported_methods_[request::PUT] = true;
		supported_methods_[request::POST] = true;
		supported_methods_[request::DELETE] = true;
	}

	virtual void http_get(const request &input, response &output);
	virtual void http_put(const request &input, response &output);
	virtual void http_post(const request &input, response &output);
	virtual void http_delete(const request &input, response &output);

	/**
	 * Validate the bucket name and set the output response accordingly
	 * @throws api_exception in case bucket name is invalid
	 */
	static void validate_bucket_name(const std::string &bucket_name);

	virtual bool validate_input(const request &input, response &output);

	virtual bool validate_output(const request &input, response &output);

	// implementing interface for genobj_handler:
	virtual void set_content_encoding_header(oapi_genobj &object,
	    const request &input, response &output);

	virtual void set_content_type_header(oapi_genobj &object,
	    response &output);

	virtual void get_content(oapi_genobj &object, const request &input,
	    response &output);

	virtual void handle_post(oapi_genobj &object, const request &input,
	    response &output);
};

#endif

