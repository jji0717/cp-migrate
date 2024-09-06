#ifndef __QUERY_HANDLER_H__
#define __QUERY_HANDLER_H__

#include <isi_object/ostore.h>
#include <isi_rest_server/uri_handler.h>

class oapi_bucket;

class object_query;

/**
 * Common object query handler
 */
class query_handler {
public:
	/**
	 * search objects in the given bucket or in all buckets
	 */
	void search_object (const std::string &bucket, const request &input,
	    response &output);

	/**
	 * search objects in the given bucket or in all buckets
	 * This overload requires the bucket has already been opened.
	 */
	void search_object (oapi_bucket &bucket, const request &input,
	    response &output);

	/**
	 * search objects in all account all buckets
	 * This overload requires the bucket has already been opened.
	 */
	void search_object (const request &input,
	    response &output);
	/**
	 * Lists stores of given store type
	 */
	void list_store(enum OSTORE_TYPE store_type, const request &input,
	    response &output);
};

#endif
