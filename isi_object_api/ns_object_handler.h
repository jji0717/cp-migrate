#ifndef __NS_OBJECT_HANDLER_H__
#define __NS_OBJECT_HANDLER_H__

#include <isi_rest_server/multipart_processor.h>
#include "isi_rest_server/uri_handler.h"
#include "oapi/oapi_object.h"

/**
 * Generic object handler which can handle both containers and data objects
 */
class ns_object_handler : public uri_handler {
public:
	ns_object_handler();

	virtual void http_get(const request &input, response &output);
	virtual void http_put(const request &input, response &output);
	virtual void http_post(const request &input, response &output);
	virtual void http_delete(const request &input, response &output);
	virtual void http_head(const request &input, response &output);
	
	virtual bool validate_input(const request &input, response &output);

public:
	/*
	 * Routine to retrieve the target type input by the user.
	 * @param[IN] target type string.
	 * @param[OUT] target type input by the user.
	 */
	static void get_target_type(const request &input,
	    std::string &target_type);

	/**
	 * Get the namespace store name and path from request
	 */
	static void get_input_params(const request &input,
	    std::string &ns,
	    std::string &path);

	/**
	 * Get the namespace store name, path and bucket from request
	 */
	static void get_input_params(const request &input,
	    std::string &ns,
	    std::string &path,
	    std::string &bucket);

	/**
	 * Function responsible for creating containers in the bucket
	 * Refactored out from handle_create
	 * Can throw std::exception
	 */
	static void create_bucket_containers(
	    const request 	&input,
	    oapi_bucket 	&bucket,
	    const std::string	&bucket_name,
	    oapi_store    	&store,
	    bool 		recursive,
	    bool 		overwrite,
	    int			levels,
	    struct isi_error	**error_out);
};

#endif

