#include "object_handler.h"

#include "isi_ilog/ilog.h"
#include <algorithm>
#include <sstream>
#include <errno.h>

#include <isi_json/json_helpers.h>
#include <isi_persona/persona_util.h>
#include <isi_util_cpp/http_util.h>
#include <isi_rest_server/api_headers.h>
#include <isi_rest_server/api_utils.h>

#include "handler_common.h"
#include "bucket_handler.h"
#include "store_provider.h"
#include "ns_object_handler.h"
#include <isi_object/iobj_ostore_protocol.h>
#include <isi_object/common.h>

#define MIN_ONAME_LEN	1
#define MAX_ONAME_LEN	500

namespace {

const size_t BUFF_SIZE = 8192;

struct BYTE_RANGE {

	/**
	 * minimum: 0, maximum: the content length
	 */
	off_t first_;

	/**
	 * -1 indicates till the end
	 */
	off_t last_;

	BYTE_RANGE(off_t first = 0, off_t last = -1)
	    : first_(first), last_(last) { }
};

/**
 * Opens an oapi_object by given bucket and object names
 * @param store the store
 * @param object  an instance of oapi_object
 * @param bucket_name  bucket name
 * @throw api_exception if it fails to open the object
 */
void
open_object(oapi_object &object,
    const std::string &bucket_name,
    enum ifs_ace_rights bucket_ace_rights_open_flg, 
    const std::string &obj_name, 
    enum ifs_ace_rights object_ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags)
{
	isi_error  *error = 0;
	object.open(SH_ACCT, bucket_name.c_str(), 
	    bucket_ace_rights_open_flg, obj_name.c_str(), 
	    object_ace_rights_open_flg, flags, cf_flags, error);
	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
			throw api_exception(error, AEC_NOT_FOUND,
			    "Object %s does not exist.", obj_name.c_str());
		}

		oapi_error_handler().throw_on_error(error, "open object");
	}
}
/**
 * Internal class implementing imultipart_handler interface
 */
class object_mpart_handler : public imultipart_handler
{
public:
	object_mpart_handler(oapi_bucket &bucket, const request &input,
	    response &output);

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
	object_part_handler part_handler_;
};

object_mpart_handler::object_mpart_handler(oapi_bucket &bucket,
    const request &input,
    response &output) : part_handler_(bucket, input, output)
{
}

/**
 * Get the account, bucket and object names from input request
 * @param input[IN] - the input request
 * @param bucket_name[OUT] - the bucket name
 * @param obj_name[OUT] - the object name
 */
void
get_input_params(const request &input,
    std::string &bucket_name, std::string &obj_name)
{
	if (input.get_namespace() == NS_OBJECT) {
		bucket_name = input.get_token("BUCKET");
		obj_name = input.get_token("OBJECT");
		object_handler::validate_object_name(bucket_name, obj_name);
	}
	else {
		const std::string &uri = input.get_token("OBJECT");
		size_t pos = uri.find_first_of('/', 0);
		size_t l_pos = uri.find_last_of('/');

		if (pos != std::string::npos) {
			obj_name = uri.substr(l_pos + 1);

			if (pos != l_pos)
				bucket_name = uri.substr(pos + 1,
				    l_pos - pos - 1);
		}
	}
}

/**
 * Check if the given attribute header is an isi extended attribute
 * @param key - the user defined attribute name
 * @return - true if it the attribute begins with X_ISI_META_
 *         - false otherwise
 */
bool
is_isi_attribute_hdr(const std::string &key)
{
	return key.size() >= sizeof(X_ISI_META_) &&
	    !key.compare(0, sizeof(X_ISI_META_)-1, X_ISI_META_);
}

/**
 * Get the range header from the input
 * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
 * This functions ignore invalid range requests.
 */
bool
get_range_header(const request &input, BYTE_RANGE &range)
{
	bool range_set = false;
	std::string range_str;

	do {
		if (!input.get_header(HDR_RANGE, range_str))
			break;

		if (range_str.find(RANGE_UNIT) != 0)
			break;

		if (range_str.size() <= strlen(RANGE_UNIT))
			break;

		range_str = range_str.substr(strlen(RANGE_UNIT));

		size_t dash_pos = range_str.find('-');

		if (dash_pos == range_str.npos)
			break;

		off_t first = 0, last = -1;

		// no suffix byte range support yet:
		if (dash_pos == 0)
			break;

		if (dash_pos != 0) {
			// first byte set:
			std::string first_str = range_str.substr(0, dash_pos);
			first = strtoll(first_str.c_str(), NULL, 10);
			if (first == 0 && errno == EINVAL)
				break;
		}

		if (range_str.size() > dash_pos + 1) {
			std::string last_str = range_str.substr(dash_pos + 1);
			last = strtoll(last_str.c_str(), NULL, 10);
			if (last == 0 && errno == EINVAL)
				break;
		}
		else
			last = -1;

		if ( last != -1 && last < first)
			break;

		// get a valid range request:
		range.first_ = first;
		range.last_ = last;
		range_set = true;
	} while (false);

	return range_set;
}

/**
 * Function responsible for transferring contents from object
 * to http stream
 * Refactored out from http_get.
 */
void get_content(
   oapi_object &object,
   const oapi_object::object_sys_attr &attr,
   const request &input, response &output)
{
	BYTE_RANGE range;
	bool range_set = get_range_header(input, range);

	if (range.first_ != 0 && range.first_ >= attr.attr_.st_size) {
		throw api_exception(AEC_INVALID_REQUEST_RANGE,
		    "Request range not satisfiable: "
		    "%llu-%llu valid range: 0-%llu",
		    range.first_, range.last_,
		    attr.attr_.st_size-1);
	}

	if (range_set)
		output.set_status(ASC_PARTIAL_CONTENT);

	off_t end_pos = (range.last_ == -1 ||
	    range.last_ > attr.attr_.st_size - 1) ?
	    attr.attr_.st_size -1 : range.last_;

	off_t len_to_read = end_pos - range.first_ + 1;

	if (len_to_read <= 0)
		return;

	if (range_set) {
		std::stringstream tstrm;
		tstrm << range.first_;
		tstrm << "-";
		tstrm << (range.first_ + len_to_read - 1);
		tstrm << "/";
		tstrm << attr.attr_.st_size;
		output.get_header_map()[api_header::CONTENT_RANGE] =
		    tstrm.str();
	}

	std::vector<unsigned char> buff_vec(BUFF_SIZE, 0);

	unsigned char *buff = &buff_vec.at(0);
	ASSERT(buff);

	off_t offset = range.first_;

	while (len_to_read > 0) {
		size_t readLen = 0;
		size_t to_read =
		    (len_to_read > (off_t)BUFF_SIZE)? BUFF_SIZE : len_to_read;

		isi_error *error = 0;
		object.read(buff, offset, to_read, readLen, error);

		if (error)
			oapi_error_handler().throw_on_error(error, "read object");

		if (readLen > 0) {
			output.write_data((const char *)buff,
			    readLen);
		}

		offset += readLen;
		len_to_read -= readLen;

		if (readLen >= 0 && readLen < to_read) {
			throw api_exception(AEC_EXCEPTION,
			    "Failed to read all data "
				"expected:%zu, returned:%zu",
			    to_read, readLen);
		}
	}
}

ipart_handler *
object_mpart_handler::on_new_part()
{
	return &part_handler_;
}

bool
object_mpart_handler::on_part_end(ipart_handler *part_handler)
{
	// we only support 1 and only 1 part
	return false;
}

void object_mpart_handler::on_end()
{

}

} // anonymous namespace

object_part_handler::object_part_handler(oapi_bucket &bucket,
    const request &input,
    response &output)
    : input_(input), output_(output), object_(bucket.get_store())
    , offset_(0), bucket_(bucket)
{
}

void
object_part_handler::throw_on_error(isi_error *error,
    const char *do_what, const char *which)
{
	if (!error)
		return;

	if (isi_error_is_a(error,
	    OSTORE_EXCEEDS_LIMIT_ERROR_CLASS)) {
		throw api_exception(error, AEC_LIMIT_EXCEEDED,
		    "Unable to %s %s -- limit exceeded.", do_what, which);
	} else if (isi_system_error_is_a(error, EISDIR)) {
		throw api_exception(error, AEC_FORBIDDEN,
		    "The target exists as a directory.");
	} else {	
		std::string what = do_what;
		if (which && strlen(which)) {
			what += " ";
			what += which;
		}
		oapi_error_handler().throw_on_error(error, what.c_str());
	}
}
/**
 * set the x-isi-ifs-attr- attributes plus content-encoding/content-type
 * in put object requests
 */
void
object_part_handler::set_attrs()
{
	isi_error *error = 0;
	oapi_object &object = object_;

	std::map<std::string, std::string> xattrs;

	get_input_attributes(xattrs);
	std::map<std::string, std::string>::iterator it;
	for (it = xattrs.begin(); it != xattrs.end(); ++it) {
		const std::string &key = it->first;

		const std::string &val = it->second;
		object.set_attr(key.c_str(), (void*)val.data(),
		    val.size(), error);

		ilog(IL_DEBUG, "set attribute: %s=%s", key.c_str(),
		    val.c_str());
		throw_on_error(error, "set attribute", key.c_str());
	}

	std::string content_type, content_encoding;
	if (get_header(api_header::CONTENT_TYPE, content_type)) {
		ilog(IL_DEBUG, "To set attribute: %s",
		     api_header::CONTENT_TYPE.c_str());

		object.set_attr(ATTR_CONTENT_TYPE,
		    (void*)content_type.data(),
		    content_type.size(), error);

		throw_on_error(error, "set attribute", ATTR_CONTENT_TYPE);
	}

	if (get_header(HDR_CONTENT_ENCODING, content_encoding)) {
		ilog(IL_DEBUG, "To set attribute: %s",
		    api_header::CONTENT_TYPE.c_str());

		object.set_attr(ATTR_CONTENT_ENCODING,
		    (void*)content_encoding.data(),
		    content_encoding.size(), error);

		throw_on_error(error, "set attribute", ATTR_CONTENT_ENCODING);
	}
}

/**
 * Get x-isi-ifs-attr- attributes headers from the request headers
 * @param xattrs[OUT] - the key-value map
 */
void
object_part_handler::get_input_attributes(
    std::map<std::string, std::string> &xattrs)
{
	// Get headers in the whole request
	common_obj_handler::get_isi_attributes_to_map(
	    input_.get_header_map(), xattrs);

	// Get the headers per part
	common_obj_handler::get_isi_attributes_to_map(header_map_, xattrs);

}

/**
 * First find in the perpart headers, if not found check the whole request
 */
bool
object_part_handler::get_header(const std::string &header,
    std::string &value) const
{
	if (ipart_handler::get_header(header, value))
		return true;

	return input_.get_header(header.c_str(), value);
}

void
object_part_handler::get_input_parameters(
    std::string &obj_name)
{
	std::string bucket_name;
	::get_input_params(input_, bucket_name, obj_name);
}

bool
object_part_handler::on_headers_ready()
{
	enum ostore_acl_mode acl_mode = OSTORE_ACL_NONE;
	mode_t c_mode = DEFAULT_MODE_OBJECT;
	const oapi_store &store = bucket_.get_store();

	output_.set_status(ASC_OK);

	std::string obj_name;
	get_input_parameters(obj_name);

	isi_error *error = 0;

	common_obj_handler::get_access_control(input_, store,
	    acl_mode, c_mode, false);

	bool overwrite = true;
	find_args(overwrite, arg_to_bool(), input_.get_arg_map(), 
	    PARAM_OVERWRITE);
	
	object_.create(bucket_, obj_name.c_str(), c_mode, acl_mode,
	    overwrite, error);
	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
			//?? we need appropriate msg here, what not found?
			const char *msg = "Failed to create object";
			throw api_exception(error, AEC_NOT_FOUND, msg);
		}
		oapi_error_handler().throw_on_error(error, "create object");
	}

	set_attrs();

	return true;
}

bool
object_part_handler::on_new_data(const char *buff, size_t len)
{
	if (len <= 0)
		return false;

	isi_error *error = 0;
	object_.write(buff, offset_, len, error);
	throw_on_error(error, "write object", "");

	offset_ += len;
	return true;
}

void
object_part_handler::on_data_end()
{
	isi_error *error = 0;
	object_.commit(error);
	
	if (error) {
		if (isi_error_is_a(error, OSTORE_ACCESS_DENIED_ERROR_CLASS)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "Access denied to put file. Need write and "
			    "related permissions to put file and delete/write "
			    "related permissions to re-put file. "
			    "Please refer to the documentation.");
		} else if (isi_system_error_is_a(error, ENOENT)) {
			// src is internal tmp file
			throw api_exception(error, AEC_FORBIDDEN,
			    "Unable to put file -- a directory component in "
			    "target path does not exist.");
		} else
			throw_on_error(error, "commit object", "");
	}
}

void
object_part_handler::clear()
{
	object_.close();
	ipart_handler::clear();
}

/**
 * Get the data from the object opened.
 */
void
object_handler::get_content(oapi_genobj &object, const request &input,
   response &output)
{
	oapi_object::object_sys_attr attr = {{0}};
	bool head_method = false;

	set_common_headers(object, input, output, attr, head_method);

	handle_conditional_get(object.get_name(), attr, input, output);

	::get_content(dynamic_cast<oapi_object &>(object), attr,
	    input, output);
}

void
object_handler::http_get(const request &input, response &output)
{
	/*
	 * Bug # : 83958: Return attributes if ?attribute OR
	 * ?attribute&blah.... is used. Any other input arguments/
	 * parameters like ?attributeabx returns the file contents.
	 * This solution is to fix the bug that causes the file
	 * contents to be returned if ?attribute&<xyz> was used.
	 * Checks if the input argument is OHQ_ATTRIBUTE.
	 */

	if (is_get_put_attr(input)) {
		get_attribute(input, output);
		return;
	}

	std::string bucket_name, obj_name;

	output.set_status(ASC_OK);
	get_input_params(input, bucket_name, obj_name);

	oapi_store store;
	store_provider::open_store(store, input);

	oapi_object object(store);

	// The ace_rights passed, opens the parent directory of the 
	// object in the specified mode.
	open_object(object, bucket_name, 
	    (enum ifs_ace_rights) IFS_RTS_DIR_TRAVERSE, 
	    obj_name, (enum ifs_ace_rights) OBJECT_READ, 
	    0, CF_FLAGS_NONE);

	get_content(object, input, output);
}

/*
 * Function responsible for putting object
 * Refactored out from http_put.
 * Can throw std::exception
 */
void
object_handler::put_object(
    const request &input,
    response &output)
{
	int create_container 	= 0;
	bool overwrite 		= false;
	oapi_store store;
	store_provider::open_store(store, input);

	oapi_bucket bucket(store);
	std::string bucket_name, obj_name;
	isi_error *error = NULL;

	find_args(create_container, arg_to_generic<int>(), input.get_arg_map(),
	    PARAM_CREATE_CONTAINER);

	find_args(overwrite, arg_to_bool(), input.get_arg_map(),
	    PARAM_OVERWRITE);

	get_input_params(input,
	    bucket_name, obj_name);

	bucket.open(SH_ACCT, bucket_name.c_str(), 
	    (enum ifs_ace_rights) BUCKET_GEN_WRITE, 0,
	    CF_FLAGS_NONE, error);

	if (error &&
	    (create_container != 0) &&
	    isi_error_is_a(error, OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
		isi_error_free(error);
		error = NULL;
		ns_object_handler::create_bucket_containers(
		    input, bucket,
		    bucket_name, store,
		    false, overwrite,
		    create_container, &error);
	}

	if (error) {
		if (isi_system_error_is_a(error, ENOTDIR)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "A component of path '%s' is not a "
			    "directory.", bucket_name.c_str());
		} else if (isi_system_error_is_a(error, ENOENT)) {
			throw api_exception(error, AEC_FORBIDDEN,
			    "A component of path '%s' does not exist.",
			    bucket_name.c_str());
		}
		oapi_error_handler().throw_on_error(error,
		    "open the container");
	}

	object_mpart_handler mpart_handler(bucket, input, output);
	multipart_procecssor mpart_processor(input, output, mpart_handler);

	mpart_processor.process();
}

void
object_handler::http_put(const request &input, response &output)
{
	/* 
	 * Bug #: 83958 addresses the bug fix for http_get. 
	 * Fixed http_put too as it addresses a similar bug.
	 */
	if (is_get_put_attr(input)) {
		put_attribute(input, output);
		return;
	}

	put_object(input, output);
}

void
object_handler::handle_post(oapi_genobj &object, const request &input,
    response &output)
{
	throw api_exception(AEC_METHOD_NOT_ALLOWED,
	    "POST operation on object is not allowed,"
	    " please refer to the API"
	    " documentation");
}

void
object_handler::http_delete(const request &input, response &output)
{
	std::string bucket_name, obj_name;

	output.set_status(ASC_NO_CONTENT);
	get_input_params(input, bucket_name, obj_name);

	isi_error *error = 0;
	oapi_store store;
	store_provider::open_store(store, input);
	oapi_object object(store);
	object.remove(SH_ACCT, bucket_name.c_str(),
	    obj_name.c_str(), error);
	if (error) {
		if (isi_error_is_a(error,
		    OSTORE_OBJECT_NOT_FOUND_ERROR_CLASS)) {
			ilog(IL_DEBUG, "Failed to remove object "
			    "error=%#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			return;
		}
		oapi_error_handler().throw_on_error(error, "remove object");
	}
}

void
object_handler::http_head(const request &input, response &output)
{
	std::string bucket_name, obj_name;

	output.set_status(ASC_OK);
	get_input_params(input, bucket_name, obj_name);

	oapi_store store;
	store_provider::open_store(store, input);
	oapi_object object(store);

	open_object(object, bucket_name, 
	    (enum ifs_ace_rights) BUCKET_READ, 
	    obj_name, (enum ifs_ace_rights) OBJECT_READ, 
	    0, CF_FLAGS_NONE);
	
	get_head_info(object, input, output);
}

/*
 * Put attributes via JSON message body.
 */
void
object_handler::put_attribute(const request &input, response &output)
{
	output.set_status(ASC_OK);
	
	std::string bucket_name, obj_name;
	get_input_params(input, bucket_name, obj_name);

	oapi_store store;
	store_provider::open_store(store, input);
	oapi_object object(store);

	open_object(object, bucket_name, 
	    (enum ifs_ace_rights) BUCKET_WRITE, 
	    obj_name, 
	    (enum ifs_ace_rights) (IFS_RTS_FILE_WRITE_ATTRIBUTES | 
		IFS_RTS_FILE_WRITE_EA), 0, CF_FLAGS_NONE);

	put_attributes(object, input, output);

}

/*
 * Get attribute via JSON message body
 */
void
object_handler::get_attribute(const request &input, response &output)
{
	std::string bucket_name, obj_name;

	output.set_status(ASC_OK);
	get_input_params(input, bucket_name, obj_name);

	oapi_store store;
	store_provider::open_store(store, input);

	oapi_object object(store);

	open_object(object, bucket_name, 
	    (enum ifs_ace_rights) IFS_RTS_DIR_TRAVERSE,
	    obj_name, (enum ifs_ace_rights) OBJECT_READ, 
	    0, CF_FLAGS_NONE);

	get_attributes(object, input, output);
}


/**
 * SET CONTENT-TYPE header in the output
 */
void
object_handler::set_content_type_header(oapi_genobj &object, response &output)
{
	oapi_attrval content_type;
	isi_error *error = 0;
	object.get_attr(ATTR_CONTENT_TYPE, content_type, error);

	oapi_error_handler().throw_on_error(error, "get object attribute");

	if (content_type.length()) {
		if(check_content_type_invalid_chars(
		    string((const char*)content_type.value(),
			    content_type.length())))
			throw api_exception(AEC_NOT_ACCEPTABLE,
			    "invalid content type");

		output.set_content_type(
		    (const char *)content_type.value());
	}
}

/**
 * SET CONTENT-ENCODING header in the output
 */
void
object_handler::set_content_encoding_header(oapi_genobj &object,
    const request &input, response &output)
{
	isi_error *error = 0;
	oapi_attrval content_encoding;

	object.get_attr(ATTR_CONTENT_ENCODING, content_encoding, error);
	oapi_error_handler().throw_on_error(error, "get object attribute");

	std::vector<accept_encoding> a_encs;
	bool valid = input.get_accept_encodings(a_encs);

	if (!valid) {
		throw api_exception(AEC_NOT_ACCEPTABLE, "invalid encoding");
	}

	if (content_encoding.length()) {

		const char *s_enc = (const char *)content_encoding.value();
		if (!strcmp(s_enc, "identity"))
			return;

		bool acceptable = a_encs.size() == 0 ? true : false;
		std::vector<accept_encoding>::iterator it;
		for (it = a_encs.begin(); it != a_encs.end(); ++it) {
			const accept_encoding &enc = *it;
			if (enc.coding_ == "*" &&
			    enc.qvalue_ != 0) {
				acceptable = true;
				break;
			}

			if (enc.coding_ == s_enc &&
			    enc.qvalue_ != 0) {
				acceptable = true;
				break;
			}
		}

		if (acceptable) {
			output.get_header_map()["Content-Encoding"] =
			    (const char *)content_encoding.value();
		}
		else {
			ilog(IL_DEBUG, "Stored content-encoding not "
			    "acceptable: %s",
			    (const char *)content_encoding.value());
			throw api_exception(AEC_NOT_ACCEPTABLE,
			    "Stored content-encoding not acceptable.");
		}
	}
}

bool
object_handler::validate_input(const request &input, response &output)
{
	// no json input
	return true;
}

/**
 * Validate the object name and set the output response accordingly
 * @throws api_exception in case the names are invalid.
 */
void
object_handler::validate_object_name(const std::string &bucket_name,
    const std::string &object_name)
{
	bucket_handler::validate_bucket_name(bucket_name);
	validate_object_name(object_name);
}

/**
 * Validate the object name and set the output response accordingly
 * @throws api_exception in case the names are invalid.
 */
void
object_handler::validate_object_name(
    const std::string &object_name)
{
	size_t oname_len = object_name.size();
	if (oname_len < MIN_ONAME_LEN || oname_len > MAX_ONAME_LEN) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Object name length: %d is out of limit.", (int)oname_len);

	}

	if (object_name == "." || object_name == ".." ) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid object name: %s ",
		    object_name.c_str());
	}
}
