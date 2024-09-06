#include "bucket_handler.h"

#include <uuid.h>
#include <vector>

#include <isi_object/iobj_ostore_protocol.h>
#include <isi_util_cpp/http_util.h>
#include <isi_rest_server/api_utils.h>

#include "handler_common.h"
#include "oapi/oapi_bucket.h"
#include "oapi/oapi_store.h"
#include "object_handler.h"
#include "query_handler.h"
#include "store_provider.h"

#define RESERVED_BUCKET_PREFIX	"isi_"
#define MIN_BNAME_LEN	1
#define MAX_BNAME_LEN	100

#define PARAM_FILE_NAME	"filename"

const size_t BUFF_SIZE = 8192;

namespace {

/**
 * Derived class handling post an object in POST.
 */
class bucket_part_handler : public object_part_handler
{
public:
	virtual void get_input_parameters(std::string &obj_name);

	bucket_part_handler(oapi_bucket &bucket, const request &input,
	    response &output)
	    : object_part_handler(bucket, input, output) { }
};

/**
 * Internal class implementing imultipart_handler interface
 * for posting objects in buckets
 */
class bucket_post_handler : public imultipart_handler
{
public:
	bucket_post_handler(oapi_bucket &bucket, const request &input,
	    response &output) : part_handler_(bucket, input, output) { }

	/**
	 * Notified by the processor of a newly found part
	 * @return the multipart_part, if it is NULL, the processor
	 *  will stop. The part_handler should be instantiated by
	 *  the multipart_handler.
	 */
	virtual ipart_handler *on_new_part();

	/**
	 * Notified by the processor of end of an part
	 * @return true if to continue, false otherwise
	 */
	virtual bool on_part_end(ipart_handler *part_handler);

	/**
	 * Notified by the processor on the finishing of all parts
	 * or on end. The handler can destroy itself only when
	 * this is received.
	 */
	virtual void on_end();

private:
	bucket_part_handler part_handler_;
};

/**
 * Get the input parameters
 * @throws exception in case of invalid input parameter.
 */
void
get_input_params(const request &input,
    std::string &bucket_name)
{
	bucket_name = input.get_token("BUCKET");

	bucket_handler::validate_bucket_name(bucket_name);
}

void
list_bucket(oapi_bucket &bucket, const request &input, response &output)
{
	output.set_status(ASC_OK);
	query_handler q_handler;

	q_handler.search_object(bucket, input, output);
}

void
bucket_part_handler::get_input_parameters(
    std::string &obj_name)
{
	// We get the object name from the content-disposition field
	// in this part
	std::string disp;
	if (!get_header(HDR_CONTENT_DISPOSITION, disp)) {
		throw api_exception(AEC_BAD_REQUEST,
		    "There must be a content_disposition in "
		    "multipart/form-data.");
	}

	std::vector<std::string> fields = request::tokenize(disp, ";", false);

	for (size_t i = 0; i < fields.size(); ++i) {
		const std::string &str = fields[i];
		std::vector<std::string> pair =
		    request::tokenize(str, "=", false);

		if (pair.size() != 2)
			continue;

		std::string field;

		request::trim(pair[0], field);

		if (field == PARAM_FILE_NAME) {
			std::string val;
			request::trim(pair[1], val);

			// remove the quotes:
			if (val.size() > 1 && val[0] == '\"' &&
			    val[val.size() - 1] == '\"')
				obj_name = val.substr(1, val.size() - 2);
			else
				obj_name = val;
			break;
		}

	}

	object_handler::validate_object_name(obj_name);
}

ipart_handler *
bucket_post_handler::on_new_part()
{
	return &part_handler_;
}

bool
bucket_post_handler::on_part_end(ipart_handler *part_handler)
{
	part_handler->clear();
	return false;
}

void bucket_post_handler::on_end()
{

}

void
post_objects(oapi_bucket &bucket, const request &input, response &output)
{

	bucket_post_handler mpart_handler(bucket, input, output);
	multipart_procecssor mpart_processor(input, output, mpart_handler);

	mpart_processor.process();
}

}

/*
 * Validate the bucket name and set the output response accordingly
 * @throws exception if valid bucket name
 */
void
bucket_handler::validate_bucket_name(const std::string &bucket_name)
{
	size_t bname_len = bucket_name.size();
	if (bname_len < MIN_BNAME_LEN || bname_len > MAX_BNAME_LEN) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Bucket name length: %d is out of limit.",
		    (int)bname_len);
	}

	if (bucket_name == "." || bucket_name == ".." ) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid bucket name: %s ",
		    bucket_name.c_str());
	}

	if (!strncasecmp(bucket_name.c_str(), RESERVED_BUCKET_PREFIX,
	    sizeof(RESERVED_BUCKET_PREFIX)-1)) {
		throw api_exception(AEC_FORBIDDEN,
		    "Bucket name prefixed with 'isi_' is "
		    "reserved. Violated name=%s.", bucket_name.c_str());
	}
}

void
bucket_handler::http_get(const request &input, response &output)
{
	oapi_store store;
	store_provider::open_store(store, input);

	oapi_bucket bucket(store);
	std::string bucket_name;
	isi_error *error = NULL;

	get_input_params(input, bucket_name);

	bucket.open(SH_ACCT, bucket_name.c_str(), 
	    (enum ifs_ace_rights) BUCKET_READ, 0,
	    CF_FLAGS_NONE, error);
	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "open a bucket");
	}

	get_content(bucket, input, output);
}

void
bucket_handler::http_put(const request &input, response &output)
{
	output.set_status(ASC_OK);
	
	std::string acct, bucket_name;
	get_input_params(input, bucket_name);

	ilog(IL_DEBUG, "Creating a bucket %s for uri=%s ...",
	    bucket_name.c_str(), input.get_uri().c_str());

	isi_error  *error = 0;
	oapi_store store;
	store_provider::open_store(store, input);
	oapi_bucket bucket(store);
	bool overwrite = true;
	find_args(overwrite, arg_to_bool(), input.get_arg_map(), 
	    PARAM_OVERWRITE);

	// create-mode is not exposed to user in object store
	bucket.create(SH_ACCT, bucket_name.c_str(), 0, NULL, NULL, false,
	    OSTORE_PRIVATE, overwrite, error);

	oapi_error_handler().throw_on_error(error, "create bucket");
}

void
bucket_handler::handle_post(oapi_genobj &object, const request &input,
    response &output)
{
	oapi_bucket &bucket = dynamic_cast<oapi_bucket &>(object);
	if (is_object_query(input))
		list_bucket(bucket, input, output);
	else
		post_objects(bucket, input, output);
}

/**
 * Query on the bucket will be done via POST as it is not a standard practice
 * to include message body in GET, and it is impractical to encode the JSON
 * query in the GET URI.
 */
void
bucket_handler::http_post(const request &input, response &output)
{
	oapi_store store;
	store_provider::open_store(store, input);

	oapi_bucket bucket(store);
	std::string bucket_name;
	isi_error *error = NULL;

	get_input_params(input, bucket_name);

	if (is_object_query(input)) {
		bucket.open(SH_ACCT, bucket_name.c_str(), 
		    (enum ifs_ace_rights) BUCKET_READ, 0, 
		    CF_FLAGS_NONE, error);
	}
	else {
		bucket.open(SH_ACCT, bucket_name.c_str(), 
		    (enum ifs_ace_rights) BUCKET_WRITE, 0, 
		    CF_FLAGS_NONE, error);
	}
	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "open a bucket");
	}

	handle_post(bucket, input, output);
}

void
bucket_handler::http_delete(const request &input, response &output)
{
	output.set_status(ASC_NO_CONTENT);

	std::string acct, bucket_name;
	get_input_params(input, bucket_name);

	ilog(IL_DEBUG, "Got request to create a bucket %s...",
	    bucket_name.c_str());

	isi_error *error = 0;
	oapi_store store;
	store_provider::open_store(store, input);
	oapi_bucket bucket(store);
	bucket.remove(SH_ACCT, bucket_name.c_str(), error);

	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
			ilog(IL_DEBUG, "Bucket does not exist error=%#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			return;
		}

		else if (isi_error_is_a(error,
		    OSTORE_OBJECT_EXISTS_ERROR_CLASS)) {
			throw api_exception(error, AEC_CONFLICT,
			    "Bucket is not empty.");
		}

		oapi_error_handler().throw_on_error(error, "delete bucket");
	}
}

/**
 * SET CONTENT-TYPE header in the output
 */
void
bucket_handler::set_content_type_header(oapi_genobj &object, response &output)
{
	output.set_content_type(api_content::APPLICATION_JSON);
}

/**
 * SET CONTENT-ENCODING header in the output
 */
void
bucket_handler::set_content_encoding_header(oapi_genobj &object,
    const request &input, response &output)
{
}

bool
bucket_handler::validate_input(const request &input, response &output)
{
	// no json input validation
	return true;
}

bool
bucket_handler::validate_output(const request &input, response &output)
{
	// no json output validation
	return true;
}

void
bucket_handler::get_content(oapi_genobj &object, const request &input,
     response &output)
{
	output.set_status(ASC_OK);
	oapi_object::object_sys_attr attr = {{0}};

	set_common_headers(object, input, output, attr, false);

	handle_conditional_get(object.get_name(), attr, input, output);

	query_handler q_handler;

	q_handler.search_object(dynamic_cast<oapi_bucket &>(object),
	    input, output);
}
