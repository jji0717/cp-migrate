#include <errno.h>

#include <vector>
#include <algorithm>

#include <sys/ioctl.h>

#include <isi_ilog/ilog.h>
#include <isi_json/json_helpers.h>
#include <isi_util/isi_assert.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_util_cpp/uri_encode.h>
#include <isi_util_cpp/uri_normalize.h>

#include "resume_token.h"
#include "api_error.h"
#include "api_utils.h"
#include "request.h"

unsigned request::next_request_id = 0;

accept_encoding::accept_encoding(const std::string &coding, const float &qvalue)
	: qvalue_(qvalue)
{
	size_t end = coding.find_last_not_of(" \t");
	size_t beg = coding.find_first_not_of(" \t");

	if (end >= beg && beg != std::string::npos && end != std::string::npos)
		coding_ = coding.substr(beg, end - beg +1 );
}

const char *request::ignore_if_blank_init[] = {"limit"};

std::set<std::string>
request::ignore_if_blank(ignore_if_blank_init, std::end(ignore_if_blank_init));

unsigned request::get_next_request_id()
{
	scoped_mutex mtx;
	return next_request_id++;
}

unsigned request::get_request_id() const
{
	return request_id_;
}

bool request::get_internal() const
{
	return internal_;
}

request::request(const std::string &base_url, const std::string &ns,
    const std::string &uri, const std::string &http_method,
    const std::string &args, const api_header_map &hdrs,
    size_t content_len, int user_token_fd, const std::string &remote_addr,
    idata_stream &strm, bool internal)
	: base_url_(base_url), method_str_(http_method)
	, method_(method_from_string(method_str_)), arg_str_(args)
	, header_map_(hdrs), content_length_(content_len)
	, user_token_fd_(user_token_fd), remote_addr_(remote_addr)
	, namespace_(ns), request_id_(get_next_request_id()), uri_(uri)
	, state_(NONE), strm_(strm), logged_(false), internal_(internal)
{
	normalize_uri(uri_);
	// Trim trailing '/' if present
	if (!uri_.empty() && uri_.at(uri_.size() - 1) == '/')
		uri_.erase(uri_.size() - 1, 1);

	std::vector<std::string> arg_tokens = tokenize(args, "&=");

	std::string name_str = "";
	std::string value_str = "";
	bool name = true;
	bool first = true; // indicates if it is the first argument

	for (std::vector<std::string>::iterator iter =
	    arg_tokens.begin(); iter != arg_tokens.end(); iter++) {

		if (*iter == "&") {
			// get keys with no value ie /foo/?describe
			if (!name_str.empty()) {
				uri_query_comp_decode(value_str);

				if (first) {
					main_arg_ = name_str;
					first = false;
				}

				// Add query arg to map, unless its name is
				// in the ignore list and its value is blank.
				if (!(find(ignore_if_blank.begin(),
				    ignore_if_blank.end(), name_str) !=
				    ignore_if_blank.end() &&
				    value_str.length() == 0)) {
					arg_map_.insert(std::make_pair(
						name_str, value_str));
				}
			}

			// next token should be a name
			name = true;
			name_str.clear();
			value_str.clear();
		} else if (*iter == "=") {
			// next token should be a value
			name = false;
			value_str.clear();
		} else {
			if (name) {
				name_str = *iter;
			} else {
				value_str = *iter;
			}
		}
	}

	// get the last key value pair
	if (!name_str.empty()) {
		uri_query_comp_decode(value_str);

		if (first) {
			main_arg_ = name_str;
			first = false;
		}

		// Add query arg to map, unless its name is
		// in the ignore list and its value is blank.
		if (!(find(ignore_if_blank.begin(),
		    ignore_if_blank.end(), name_str) !=
		    ignore_if_blank.end() &&
		    value_str.length() == 0)) {
			arg_map_.insert(std::make_pair(
				name_str, value_str));
		}
	}

	// No timer unless initialized specially with set_timer().
	timer_ = nullptr;

	// Log the request
	log_request();
}

std::vector<std::string>
request::tokenize(const std::string &data, const std::string &delims,
    bool include_tokens)
{
	std::vector<std::string> ret;

	if (data.empty())
		return ret;

	bool delim_set[256] = { };
	std::string::const_iterator iter;

	for (iter = delims.begin(); iter < delims.end(); iter++) {
		delim_set[unsigned(*iter)] = true;
	}

	size_t begin = 0;
	size_t end = 0;

	while (begin < data.size()) {
	
		if (delim_set[unsigned(data[begin])]) {
			if (include_tokens) {
				ret.push_back(std::string(1, data.at(begin)));
			}
			begin++;
			end++;
		} else {
			end = begin + 1;
			while (end < data.size()) {
				if (delim_set[unsigned(data[end])]) {
					break;
				}
				end++;
			}

			ret.push_back(data.substr(begin, end - begin));
			begin = end;
		}
	}
	return ret;
}

/*
 * Trim the source str of the white spaces
 * return the result in the destination string
 */
void
request::trim(const std::string &src, std::string &dst)
{
	if (src.size() == 0)
		return;

	const char *data = src.c_str();
	const char *end = src.c_str() + src.size() - 1;

	for (; data <= end; ++data) {
		if (*data != ' ')
			break;
	}

	const char *start = data;

	data = end;

	for (; data >= start; --data) {
		if (*data != ' ')
			break;
	}

	if (data > start)
		dst.assign(start, data - start + 1);
	else
		dst.clear();
}

void
request::read_str_input() const
{
	ASSERT(method_ == PUT || method_ == POST || method_ == DELETE,
	    "Cannot get input body from method type %d", method_);
	ASSERT(state_ == NONE);

	body_.resize(content_length_);

	if ((strm_.read((char *)body_.data(), content_length_)) 
	    != content_length_)
		throw ISI_ERRNO_EXCEPTION("Short read");

	/* XXX: we could scrubs password here if we knew that we were
	   going to log this. */
	ilog(IL_REST_BODY, "body: %.1023s", body_.c_str());

	state_ = BODY_LOADED;
}

void
request::read_json_input() const
{
	// XXX check content type? Probably will make testing and
	// dev work using curl a little harder

	Json::Reader reader;

	const std::string &body = get_body();

	// An empty body is equivalent to an empty json object value
	if (body.size() == 0) {
		json_obj_ = Json::objectValue;

	// Non-empty bodies need to be parsed
	} else if (!reader.parse(body, json_obj_)) {
		const std::string &msg = reader.getFormattedErrorMessages();
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid JSON input, parse errors: %s", msg.c_str());
	}

	state_ = JSON_LOADED;
}

size_t 
request::read_body(void *buff, size_t len) const
{
	ASSERT(state_ == NONE || state_ == STREAMING);
	state_ = STREAMING;
	return strm_.read(buff, len);
}

void
request::get_error(std::string &error_info) const
{
	return strm_.get_error(error_info);
}

request::method
request::method_from_string(const std::string &method_name)
{
	if (method_name == "GET") {
		return GET;
	} else if (method_name == "PUT") {
		return PUT;
	} else if (method_name == "POST") {
		return POST;
	} else if (method_name == "DELETE") {
		return DELETE;
	} else if (method_name == "HEAD") {
		return HEAD;
	}

	return UNSUPPORTED;
}

const char *
request::string_from_method(unsigned method)
{
	const char *ret;

	switch (method) {
	case GET: 
		ret = "GET";
		break;
	case PUT:
		ret = "PUT";
		break;
	case POST:
		ret = "POST";
		break;
	case DELETE:
		ret = "DELETE";
		break;
	case HEAD:
		ret = "HEAD";
		break;
	default:
		ret = "UNSUPPORTED";
		break;
	}

	return ret;
}

const std::string &
request::get_token(const std::string &name) const
{
	uri_tokens::const_iterator it = tokens_.find(name);

	/* should never happen */
	if (it == tokens_.end())
		throw isi_exception("TOKEN NOT FOUND %s", name.c_str());

	return it->second;
}

bool
request::get_header(const std::string &header, std::string &val) const
{
	bool found = false;
	api_header_map::const_iterator it;

	it = header_map_.find(header);
	if (it != header_map_.end()) {
		val = it->second;
		found = true;
	}

	return found;
}

bool
request::get_accept_encodings(std::vector<accept_encoding> &encodings) const
{
	bool valid = true;

	std::string accept_encodings;
	if (get_header(api_header::CONTENT_DISPOSITION, accept_encodings)) {
		ilog(IL_DEBUG, "Accept-Encoding %s ",
		    accept_encodings.c_str());
	}

	size_t start = 0;
	size_t pos = 0;
	size_t len = accept_encodings.length();
	for (; len &&
	    pos != std::string::npos &&
	    pos != len - 1;
	    start = pos + 1) {

		pos = accept_encodings.find_first_of(',', start);

		if (pos == start)
			break;

		std::string enc = accept_encodings.substr(start,
		    pos == std::string::npos ?
		    std::string::npos : pos - start);
		size_t pos2 = enc.find_first_of(';');
		if (pos2 == std::string::npos) {
			encodings.push_back(accept_encoding(enc, 1));
			continue;
		}
		std::string cd = enc.substr(0, pos2 - 1);

		if (pos2 == enc.length() - 1) {
			encodings.push_back(accept_encoding(cd, 1));
			continue;
		}

		std::string qv = enc.substr(pos2 + 1);

		if (qv.length() > 2 && !qv.compare(0, 2, "q=")) {

			qv = qv.substr(2);
			char *endptr = NULL;
			float q = strtof(qv.c_str(), &endptr);

			if (endptr != qv.c_str() + qv.length()) {
				valid = false;
				break;
			}

			if (q == ERANGE || q > 1 || q < 0) {
				valid = false;
				break;
			}

			// check the quality value
			encodings.push_back(accept_encoding(cd, q));
			continue;
		}
		else {
			valid = false;
			break;
		}
	}

	if (!valid) {
		ilog(IL_DEBUG, "Encoding %s not supported",
		    accept_encodings.c_str());
		encodings.clear();
	}

	return valid;
}

void
request::remove_query_arg(const std::string &arg)
{
	arg_map_.erase(arg);
}

bool
request::get_arg(const char *param, std::string &val) const
{
	bool found = false;
	arg_map::const_iterator it = arg_map_.find(param);

	if (it != arg_map_.end()) {
		val = it->second;
		found = true;
	}
	return found;
}

Json::Value
request::get_args_as_json(const Json::Value &schema) const
{
	Json::Value ret(Json::objectValue);
	
	// Get the properties object 
	Json::Value empty_obj(Json::objectValue);
	Json::Value properties(Json::objectValue);

	if (schema.isObject() && !schema.isNull()) {
		properties = schema.get("properties", empty_obj);
		if (!properties.isObject() || properties.isNull()) {
			properties = empty_obj;
		}
	}

	// Convert the query args into a json object
	Json::Value string_schema(Json::objectValue);
	string_schema["type"] = "string";
	
	for (arg_iter it = arg_map_.begin(); it != arg_map_.end(); ++it) {

		// Convert the query arg string into a json value
		const std::string &arg_str = it->first;
		const std::string &val_str = it->second;

		// By default, the json value will be a string
		Json::Value value = val_str;

		// Lookup the type of arg_str in properties
		Json::Value arg_schema = properties.get(arg_str, string_schema);

		if (!arg_schema.isObject() || arg_schema.isNull()) {
			arg_schema = string_schema;
		} else if (!arg_schema.isMember("type") ||
		    !arg_schema["type"].isString()) {
			arg_schema["type"] = "string";
		}

		std::string type_str = arg_schema["type"].asString();
		bool is_array = (type_str == "array");
		if (is_array) {
			// Parse out the inner item type for the array
			if (arg_schema.isMember("items") &&
			    arg_schema["items"].isObject()) {
				Json::Value items = arg_schema["items"];
				if (items.isMember("type") &&
				    items["type"].isString()) {
					type_str = items["type"].asString();
				}
			}
		}

		try {
			if (type_str == "integer") {
				value = lexical_cast<Json::Int64>(val_str);
			} else if (type_str == "number") {
				value = lexical_cast<double>(val_str);
			} else if (type_str == "boolean") {
				// Fix case of incoming query arg bool value.
				std::string val_str_lower = val_str;
				std::transform(val_str_lower.begin(),
				    val_str_lower.end(), val_str_lower.begin(),
				    ::tolower);
				if (val_str_lower == "true") {
					value = true;
				} else if (val_str_lower == "false") {
					value = false;
				} else if (val_str_lower == "any") {
					// For backward compatibility, secretly
					// ignore booleans set to "any".  In one
					// of the older handlers (quota/quotas)
					// "any" is interpreted as equivalent to
					// leaving out the query arg.
					continue;
				} else {
					value = 
					    lexical_cast<bool>(val_str_lower);
				}
			} // Otherwise leave the type as a string
		} catch (const api_exception &e) {
			/**
			 * The string could not be converted to the correct
			 * type.  Leave it as a string and let the schema
			 * validator deal with it.
			 */
		}

		// If this key appears more than once, or if the schema
		// indicates that this is an array type, use a json array
		if (arg_map_.count(it->first) > 1 || is_array) {
			Json::Value empty_array(Json::arrayValue);
			Json::Value array = ret.get(arg_str, empty_array);
			array.append(value);
			ret[arg_str] = array;
		} else {
			ret[arg_str] = value;
		}
	}

	return ret;
}

Json::Value
request::get_args_and_resume_as_json(const Json::Value &schema) const
{
	Json::Value args_json = get_args_as_json(schema);
	std::string resume_str;

	if (json_to_c(resume_str, args_json, RESUME_QUERY_ARG)) {

		std::string args_str;
		std::string item_str;

		resume_input_token rit(resume_str,
		    VALIDATED_RESUME_TOKEN_VERSION);
		rit >> args_str >> item_str;

		// Use the args from the resume token
		Json::Reader reader;
		args_json.clear();
		if (!reader.parse(args_str, args_json)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Could not read args as json.");
		}

		// Put the resume argument into the object to return
		args_json[RESUME_QUERY_ARG] = resume_str;

	}

	return args_json;
}


zid_t
request::zid() const
{
	zid_t zid = NO_ZONE;
	ioctl(get_user_token(), CIOZID, &zid);
	return zid;
}

typedef std::set<std::string> no_log_headers;

no_log_headers no_log_rest;

/* Initialize lists of headers we shouldn't log */
struct no_log_list_initializer {
	no_log_list_initializer()
	{
		/* The request cookie is password equivalent */
		no_log_rest.insert("COOKIE");
	}
} s_no_log_list_initializer;

/*
 * The following scrubbing routines are meant to eliminate passwords
 * from the audit log.  They require more knowledge of how the JSON
 * looks than I would like to include, but we only have the URI and
 * body string at this level.
 */

static bool
is_json_space(char c)
{
	bool is_space = false;

	/*
	 * From the JSON RFC 7159
	 * https://tools.ietf.org/html/rfc7159#section-2
	 * The following characters are JSON whitespace.
	 */
	switch (c) {
	case ' ':
	case '\t':
	case '\r':
	case '\n':
		is_space = true;
		break;
	default:
		is_space = false;
	}

	return is_space;
}

/*
 * Finds fields in the body that look like field: "value" for JSON k:v strings
 * and replaces the value with "***" in new_body.
 *
 * @retval true found the field and new_body contains the scrubbed body.
 * @retval false the field wasn't found and new_body is unmodified.
 */
static bool
scrub_password(const std::string &body, const std::string &field, std::string &new_body)
{
	size_t offset = 0;
	size_t new_offset = 0;
	size_t last_offset = 0;
	bool is_scrubbed = false;

	while (offset < body.size()) {
		new_offset = body.find(field, offset);
		if (new_offset == std::string::npos) {
			break;
		}

		/* skip over the key */
		offset = new_offset + field.length();

		/* skip over the \s*:\s*" */
		while (is_json_space(body[offset])) {
			offset += 1;
		}

		if (body[offset] != ':') {
			continue;
		}
		offset += 1;

		while (is_json_space(body[offset])) {
			offset += 1;
		}

		/* only scrub strings */
		if (body[offset] != '"') {
			continue;
		}
		offset += 1;

		if (!is_scrubbed) {
			is_scrubbed = true;
			new_body.reserve(body.size() + 128);
		}

		/* add all of the body from the last add to the beginning of the
		   password then add the scrubbed password. */
		new_body.append(body, last_offset, offset - last_offset);
		new_body.append("***\"");

		/* skip copying the password body */
		for (; body[offset] != '\0' && body[offset] != '"'; offset += 1) {
			if (body[offset] == '\\') {
				offset += 1;
				if (body[offset] == '\0') {
					break;
				}
			}
		}

		/* added earlier */
		if (body[offset] == '"') {
			offset += 1;
		}

		last_offset = offset;
	}

	if (is_scrubbed) {
		new_body.append(body, last_offset, body.size() - last_offset);
	}

	return is_scrubbed;
}

/*
 * Finds password strings in the body and replaces them with "***" in new_body.
 *
 * @retval true passwords found in body and new_body contains the scrubbed body.
 * @retval false no passwords found in body and new_body is unmodified.
 */
static bool
scrub_passwords(const std::string &uri, const std::string &body, std::string &new_body)
{
	if ((uri.find("/auth/") == std::string::npos)&&(uri.find("/ndmp/users") == std::string::npos)) {
		return false;
	}

	if (scrub_password(body, "\"password\"", new_body)) {
		std::string tmp_new_body;
		if (scrub_password(new_body, "\"bind_password\"", tmp_new_body)) {
			new_body = tmp_new_body;
		}

		return true;
	}

	return scrub_password(body, "\"bind_password\"", new_body);
}

char *
request::audit_request() const
{
	struct fmt fmt = FMT_INITIALIZER;

	fmt_print(&fmt, "{");
	fmt_print(&fmt, "\"user\":{%s}", get_ntoken_str(user_token_fd_).c_str());
	fmt_print(&fmt, ",\"uri\":\"%s\"", uri_.c_str());
	fmt_print(&fmt, ",\"method\":\"%s\"", method_str_.c_str());
	fmt_print(&fmt, ",\"args\":\"%s\"", arg_str_.c_str());
	/* XXX: scrub passwords from body */
	if (method_has_input(method_)) {
		std::string new_body;
		if (scrub_passwords(uri_, get_body(), new_body)) {
			fmt_print(&fmt, ",\"body\":%s", new_body.c_str());
		}
		else {
			fmt_print(&fmt, ",\"body\":%s", get_body().c_str());
		}
	} else {
		fmt_print(&fmt, ",\"body\":{}");
	}
	fmt_print(&fmt, "}");
	char *s = fmt_detach(&fmt);
	fmt_clean(&fmt);
	return s;
}

void
request::set_timer(method_timer *new_timer)
{
	timer_ = new_timer;
}

void
request::log_timing_checkpoint(const char *label, int level) const
{
	if (timer_ != NULL) {
		timer_->log_timing_checkpoint(label, level);
	}
}

void
request::log_request(bool error)
{
	bool printed = false;
	if (logged_)
		return;

	/*
	 * IL_REST_USER, IL_REST_REQ, and IL_REST_HDR control whether user,
	 * request, or header data are printed.
	 *
	 * IL_REST_READS controls whether logs are printed for GET and HEAD
	 * requests.  The content that is printed is determined by the other
	 * flags.
	 */
	ILOG_START(IL_REST_USER | IL_REST_REQ | IL_REST_HDR | IL_REST_READS) {

		// Don't log read requests by default unless called after error
		bool read_check = error || (ILOG_MATCH_LEVEL & IL_REST_READS) ||
		    (method_ != request::GET && method_ != request::HEAD);

		if ((ILOG_MATCH_LEVEL & IL_REST_REQ) && read_check) {
			fmt_print(ilogfmt, "base: %s", base_url_.c_str());
			fmt_print(ilogfmt, "; uri: %s", uri_.c_str());
			fmt_print(ilogfmt, "; method: %s",
			    method_str_.c_str());
			fmt_print(ilogfmt, "; args: %s", arg_str_.c_str());
			fmt_print(ilogfmt, "; len: %ld", content_length_);
			printed = true;
		}

		if ((ILOG_MATCH_LEVEL & IL_REST_HDR) && read_check) {
			if (printed)
				fmt_print(ilogfmt, "; ");

			fmt_print(ilogfmt, "headers: ");
			for (api_header_map::const_iterator iter =
			    header_map_.begin(); iter != header_map_.end();
			    iter++) {
				if (no_log_rest.find(iter->first) ==
				    no_log_rest.end()) {
					fmt_print(ilogfmt, "%s='%s' ",
					    iter->first.c_str(),
					    iter->second.c_str());
				}
			}
			printed = true;
		}

		if ((ILOG_MATCH_LEVEL & IL_REST_USER) && read_check) {
			if (printed)
				fmt_print(ilogfmt, "; ");

			fmt_print(ilogfmt, "remote_addr: %s",
			    remote_addr_.c_str());
			fmt_print(ilogfmt, "; user: %s",
			    get_ntoken_str(user_token_fd_).c_str());
			printed = true;
		}
	} ILOG_END;

	logged_ = printed;
}
