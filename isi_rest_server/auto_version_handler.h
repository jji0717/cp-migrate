#ifndef __AUTO_VERSION_HANDLER_H__
#define __AUTO_VERSION_HANDLER_H__

#include <isi_rest_server/uri_handler.h>

class auto_version_handler : public uri_handler {
public:
	/*
	 * Call the constructor with a pointer to the same object being used in
	 * the newest version of the uri.  This class doesn't own the pointer
	 * and doesn't free it.  It needs to be the same object so that its
	 * schemas will be initialized.
	 *
	 * Call the constructor with a privilege ISI_PRIV_XXX is equivalent to
	 * calling with priv_filter(ISI_PRIV_XXX).set_run_as(false).
	 */
	auto_version_handler(
	    uri_handler *current_version_handler, int priv);
	auto_version_handler(
	    uri_handler *current_version_handler, const priv_filter &pf);

	virtual void http_get(const request &input, response &output);
	virtual void http_put(const request &input, response &output);
	virtual void http_post(const request &input, response &output);
	virtual void http_delete(const request &input, response &output);
	virtual void http_head(const request &input, response &output);

	virtual void downgrade_get_output(response &output);
	virtual void downgrade_post_output(response &output);

	virtual void remove_undefined_query_args(request &input);

	static void remove_undefined_fields(Json::Value &data,
	    const Json::Value &schema);
	static void remove_undefined_fields_object(Json::Value &data,
	    const Json::Value &schema);
	static void remove_undefined_fields_array(Json::Value &data,
	    const Json::Value &schema);
	static void remove_undefined_fields_type_array(Json::Value &data,
	    const Json::Value &schema);

private:
	uri_handler *current_version_handler_;
};

#endif

