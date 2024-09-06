#include "acct_handler.h"

#include <uuid.h>

#include "handler_common.h"
#include "oapi/oapi_acct.h"
#include "query_handler.h"
#include "store_provider.h"

#define JS_ATTR_BUCKET "bucket"
namespace {

struct list_bucket_context {
	const request &input_;
	response &output_;
	list_bucket_context(const request &input, response &output);

};

list_bucket_context::list_bucket_context(
    const request &input, response &output
    ) : input_(input), output_(output)
{

}

static bool list_bucket_callback(void *context, const char *name)
{
	ASSERT(context && name);

	list_bucket_context *ctx = (list_bucket_context*) context;
	
	Json::Value bucketVal(Json::objectValue);
	bucketVal[JS_ATTR_BUCKET] = Json::Value(name);

	Json::Value &j_output = ctx->output_.get_json();
	j_output.append(bucketVal);

	return true;
}

} // anonymous namespace

void
acct_handler::http_get(const request &input, response &output)
{
	output.set_status(ASC_OK);

	if (is_version_request(input)) {
		oapi_set_versions_to_response(input.get_namespace(), output);
		return;
	}

	std::string user;
	input.get_header(HDR_REMOTE_USER, user);
	oapi_acct acct(user);
  
	list_bucket_context context(input, output);
	Json::Value empty_val(Json::arrayValue);

	Json::Value &j_output = output.get_json();
	j_output = empty_val;
	isi_error *error = 0;
	oapi_store store;
	store_provider::open_store(store, input);

	acct.list_bucket(store, list_bucket_callback, &context, error);
	if (error) {
		j_output = empty_val;		// reset output
		throw api_exception(error, AEC_EXCEPTION,
		    "Unable to list bucket");
	}
		
	return;
}

void
acct_handler::http_post(const request &input, response &output)
{
	if (is_object_query(input)) {
		output.set_status(ASC_OK);
		query_handler q_handler;

		q_handler.search_object(input, output);
	}
	else {
		throw api_exception(AEC_METHOD_NOT_ALLOWED,
		    "POST operation on accounts is not allowed,"
		    " please refer to the API"
		    " documentation.");
	}
}

bool
acct_handler::validate_input(const request &input, response &output)
{
	// no json input validation
	return true;
}

bool
acct_handler::validate_output(const request &input, response &output)
{
	// no json output validation
	return true;
}
