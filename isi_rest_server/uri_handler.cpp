#include <assert.h>

#include <sstream>
#include <list>
#include <set>
#include <iostream>
#include <limits>

#include <isi_ilog/ilog.h>
#include <isi_json/json_helpers.h>
#include <isi_json/json_schema_validator.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util_cpp/scoped_lock.h>

#include "api_content.h"
#include "api_error.h"
#include "api_utils.h"
#include "json_sorter.h"
#include "resume_token.h"
#include "scoped_settcred.h"
#include "uri_handler.h"


namespace {

unsigned int MAX_METHOD_RESTARTS = 20;

}

const std::string uri_handler::describe_option = "describe";
const std::string uri_handler::list_option     = "list";
const std::string uri_handler::internal_option = "internal";
const std::string uri_handler::json_option     = "json";

serializer_none uri_handler::default_serializer;

void
uri_handler::execute_http_method(request &input, response &output, bool validate_schema)
{
	/*
	 * Since any implementation can throw, set any headers
	 * we always want before calling the implementation.
	 */
	output.get_header_map()[api_header::ALLOW] = supported_methods_str_;

	/* Bail on out unsupported methods (e.g. CONNECT) before priv checks */
	request::method method = input.get_method();
	if (method == request::NUM_METHODS || method == request::UNSUPPORTED) {
		http_method_not_allowed(input, output); // throws
		return;
	}

	const request::arg_map &args = input.get_arg_map();

	/* Handle describe before priv check, this is open information */
	if (method == request::GET && args.count(describe_option)) {
		if (args.count(json_option))
			get_describe_json(input, output);
		else
			get_describe(input, output);
		return;
	}

	std::auto_ptr<scoped_settcred> apsc;
	if (!input.get_internal()) {
		/* Check privs */
		priv_filter_.throw_unless_privileged(input.get_user_token(),
		    method);
		/* Change user if required for this method */
		bool as_user_req = priv_filter_.get_run_as(method);
		apsc.reset(new scoped_settcred(input.get_user_token(),
			as_user_req));
	}

	scoped_serializer_lock method_serialization_lock(serializer_, method);

	bool fail = false;
	UFAIL_POINT_CODE(uri_handler_execute, { fail = RETURN_VALUE; });
	if (fail) {
		const char *msg = "Failing for failpoint 'uri_handler_execute'";
		ilog(IL_INFO, "%s", msg);
		throw isi_exception(msg);
	}

	// Check environmental criteria is met.
	environmental_criteria_(input);

	// Prevent undefined query args from reaching the handler
	remove_undefined_query_args(input);

	// Validate input
	if (!validate_input(input, output, validate_schema))
		throw api_exception(AEC_BAD_REQUEST,
		    "Input validation failed.");

	input.log_timing_checkpoint("input validation complete");

	if (!initialized_) {
		throw api_exception(AEC_EXCEPTION,
		    "URI handler initialization failed.");
	}

	for (unsigned int i = 0; ; ++i) {
		try {
			switch (method) {
			case request::GET:
				http_get(input, output);
				break;
			case request::PUT:
				/* default put success code */
				output.set_status(ASC_NO_CONTENT);
				http_put(input, output);
				break;
			case request::POST:
				/* default post success code */
				output.set_status(ASC_CREATED);
				http_post(input, output);
				break;
			case request::DELETE:
				/* default delete success code */
				output.set_status(ASC_NO_CONTENT);
				http_delete(input, output);
				break;
			case request::HEAD:
				http_head(input, output);
				break;
			default:
				assert(0); // caught above
				break;
			}

			break;
		} catch (api_exception &e) {
			if (e.error_code() != AEC_RESTART)
				throw;
			if (i == MAX_METHOD_RESTARTS) {
				ilog(IL_INFO, "Reached maximum %d restarts when "
				    "attempting to write config during %s %s",
				    i, request::string_from_method(method),
				    input.get_uri().c_str());
				e.error_code() = AEC_CONFLICT;
				throw;
			}
			ilog(IL_DEBUG, "Restarting %s %s, try %d",
			    request::string_from_method(method),
			    input.get_uri().c_str(), i + 1);
		} catch (isi_exception &e) {
			if (!isi_error_is_restartable(e.get()))
				throw;
			if (i == MAX_METHOD_RESTARTS) {
				ilog(IL_INFO, "Reached maximum %d restarts when "
				    "attempting to write config during %s %s",
				    i, request::string_from_method(method),
				    input.get_uri().c_str());
				throw api_exception(e.detach(), AEC_CONFLICT);
			}
			ilog(IL_DEBUG, "Restarting %s %s, try %d",
			    request::string_from_method(method),
			    input.get_uri().c_str(), i + 1);
		}
	}

	// Commenting out the below output validation logic since we are using
	// external validators to check output JSON correctness for now.

	//if (method_has_output(method))
	//	validate_output(input, output);

	//input.log_timing_checkpoint("output validation complete");
}

/*
 * Bug 187663
 * Too many undefined query arguments are still being removed for the Freight
 * Trains release, so this function is being disabled until more schema
 * hardening can be done in Pipeline.
 * This functionality has been moved into the auto_version_handler class
 * as an overloaded virtual function as that class depends on not encountering
 * undefined query arguments.
 */
void
uri_handler::remove_undefined_query_args(request &input)
{ }

void
uri_handler::get_describe_json(const request &input, response &output)
{
	Json::Value &output_json = output.get_json();

	for (unsigned m = 0; m < request::NUM_METHODS; ++m) {
		if (!supported_methods_[m])
			continue;

		std::string method_str = request::string_from_method(m);
		std::string input_key = method_str + "_input_schema";
		std::string output_key = method_str + "_output_schema";
		std::string args_key = method_str + "_args";

		const Json::Value &input_schema = doc_.get_input_schema(m);
		if (input_schema.isObject() && !input_schema.isNull()) {
			output_json[input_key] = input_schema;
		}

		const Json::Value &output_schema = doc_.get_output_schema(m);
		if (output_schema.isObject() && !output_schema.isNull()) {
			output_json[output_key] = output_schema;
		}
		
		const Json::Value &arg_schema = doc_.get_arg_schema(m);
		if (arg_schema.isObject() && !arg_schema.isNull()) {
			output_json[args_key] = arg_schema;
		}
	}
}

void
uri_handler::get_describe(const request &input, response &output)
{
	std::stringstream body;

	body <<
	    replace_version_placeholders(doc_.get_overview(), input.get_uri());

	output.set_content_type(api_content::TEXT_PLAIN);
	output.get_body() = body.str();
}

std::string
uri_handler::describe_method(unsigned method)
{
	std::stringstream ss;
	const Json::Value *ref;
	const char *const method_name = request::string_from_method(method);

	ref = &doc_.get_input_schema(method);
	if (ref->isObject() && !ref->isNull()) {
		ss << "\n\n" << method_name << " Request Body Schema:";
		ss << *ref;
	}

	ref = &doc_.get_output_schema(method);
	if (ref->isObject() && !ref->isNull()) {
		ss << "\n\n" << method_name << " Response Body Schema:";
		ss << *ref;
	}

	return ss.str();
}

bool
compare_size(const std::vector<std::string> &a, const
    std::vector<std::string> &b)
{
	return a.size() < b.size();
}

bool
uri_handler::validate_input(const request &input, response &output, bool validate_schema)
{
	bool retval = true;

	// Validate input json
	const Json::Value &schema = (input.get_method() == request::HEAD) ?
			doc_.get_input_schema(request::GET) : doc_.get_input_schema(input.get_method());

	if (validate_schema || force_schema_validation) {
		if (!schema.isNull()) {
			std::vector<json_schema_validator::error> errors;
			json_schema_validator validator(schema);

			retval &= validator.validate(input.get_json(), errors);

			for (std::vector<json_schema_validator::error>::iterator it =
				errors.begin(); it != errors.end(); ++it) {
				std::string error_msg = it->get_error_message();
				ilog(IL_INFO, "%s", error_msg.c_str());

				// Add error to the response
				output.add_error(AEC_BAD_REQUEST, error_msg,
					it->path_);
			}
		}
	}

	// Validate query args
	const Json::Value &arg_schema = (input.get_method() == request::HEAD) ?
			doc_.get_arg_schema(request::GET) : doc_.get_arg_schema(input.get_method());
	if (validate_schema || force_schema_validation) {
		if (!arg_schema.isNull()) {
			std::vector<json_schema_validator::error> errors;
			json_schema_validator validator(arg_schema);

                        //third boolean arg is allow_unknown_values, true==ignore unknown query args
			retval &= validator.validate(
				input.get_args_as_json(arg_schema), errors, true);

			for (std::vector<json_schema_validator::error>::iterator it =
				errors.begin(); it != errors.end(); ++it) {
				std::string error_msg = it->get_error_message();
				ilog(IL_INFO, "%s", error_msg.c_str());

				// Add error to the response
				output.add_error(AEC_BAD_REQUEST, error_msg,
					it->path_);
			}
		}
	}

	/*
	 * If input and query args are valid, attempt to validate resume token
	 * for GET requests.
	 */
	if (retval && input.get_method() == request::GET) {
		const request::arg_map &args = input.get_arg_map();
		std::string resume_str;

		if (find_args(resume_str, args, RESUME_QUERY_ARG)) {
			std::string args_str;
			std::string item_str;

			try {
				resume_input_token rit(resume_str,
				    VALIDATED_RESUME_TOKEN_VERSION);
				rit >> args_str >> item_str;
			} catch (api_exception &e) {
				/*
				 * This means the resume token is not using 
				 * VALIDATED_RESUME_TOKEN_VERSION so we can't
				 * validate.
				 */
				return retval;
			}

			const Json::Value &get_output_schema =
			    doc_.get_output_schema(request::GET);

			if (arg_schema.isNull() || get_output_schema.isNull()) {
				/*
				 * VALIDATED_RESUME_TOKEN_VERSION requires
				 * schemas.
				 */
				ilog(IL_INFO, "schema not found in validate.");
				throw api_exception(AEC_EXCEPTION,
				    "Resume token schema not found.");
			}

			Json::Reader reader;
			std::vector<json_schema_validator::error> errors;

			// Validate args from resume token
			Json::Value args_json(Json::objectValue);
			if (!reader.parse(args_str, args_json)) {
				throw api_exception(AEC_BAD_REQUEST,
				    "The resume token args are not json.");
			}

			json_schema_validator args_val(arg_schema);
			if (!args_val.validate(args_json, errors, true)) {
				throw api_exception(AEC_BAD_REQUEST,
				    "The resume token args are not valid.");
			}

			// Validate item from resume token
			Json::Value item_json(Json::objectValue);
			if (!reader.parse(item_str, item_json)) {
				throw api_exception(AEC_BAD_REQUEST,
				    "The resume token item is not json.");
			}

			Json::Value item_schema =
			    get_item_schema(get_output_schema);

			json_schema_validator item_val(item_schema);
			if (!item_val.validate(item_json, errors, false,
			    true)) {
				throw api_exception(AEC_BAD_REQUEST,
				    "The resume token item is not valid.");
			}
		}
	}

	return retval;
}

bool
uri_handler::validate_output(const request &input, response &output)
{
	// Don't validate output in release builds.
	if (g_release)
		return true;

	// If there is already an error the output will be error objects, so
	// skip validation
	if (output.has_error())
		return true;

	// Don't validate other content types, could be error when we have a
	// schema, though.
	if (output.get_content_type() != api_content::APPLICATION_JSON)
		return true;

	// Don't validate streaming output.
	if (output.is_streaming()) {
		return true;
	}

	const Json::Value &schema = doc_.get_output_schema(input.get_method());

	// No schema defined
	if (schema.isNull())
		return true;

	std::vector<json_schema_validator::error> errors;
	json_schema_validator validator(schema);

	bool retval = validator.validate(output.get_json(), errors);

	std::vector<json_schema_validator::error>::iterator it = errors.begin();
	if (it != errors.end()) {
		std::string schema_string;
		std::string output_string;
		Json::Value output_json = output.get_json();
		Json::FastWriter writer;
		schema_string = writer.write(schema);
		output_string = writer.write(output_json);
		ilog(IL_DEBUG, "schema error with the following schema");
		int i = 0;
		const char *s = schema_string.c_str();
		int len = strlen(s);
		while (i < len) {
			char buf[201];
			strncpy(buf, &s[i], 200);
			buf[200] = 0;
			i += 200;
			ilog(IL_DEBUG, "%s", buf);
		}
		ilog (IL_DEBUG, "output: %s", output_string.c_str());
	}

	for (; it != errors.end(); ++it) {
		std::string error_msg = "Json output doesn't validate against "
		    "schema.  ";
		error_msg.append(it->path_).append(" has error: ").
		    append(it->message_);
		ilog(IL_DEBUG, "%s", error_msg.c_str());
	}

	return retval;
}

void
uri_handler::http_method_not_allowed(const request &input, response &output)
{
	std::string &str = output.get_body();

	str = "Only methods ";
	str += supported_methods_str_;
	str += " are allowed for this resource.";

	throw api_exception(AEC_METHOD_NOT_ALLOWED, str);
}

void
uri_handler::http_get(const request &input, response &output)
{
	http_method_not_allowed(input, output);
}

void
uri_handler::http_put(const request &input, response &output)
{
	http_method_not_allowed(input, output);
}

void
uri_handler::http_post(const request &input, response &output)
{
	http_method_not_allowed(input, output);
}

void
uri_handler::http_delete(const request &input, response &output)
{
	http_method_not_allowed(input, output);
}

void
uri_handler::http_head(const request &input, response &output)
{
	/*
	 * N.B. default head is to use get method if it exists.
	 * This could be a performance problem for some handlers, which
	 * should override this method. It also will not work for handlers
	 * that using streaming get.
	 */
	if (supported_methods_[request::GET]) {
		http_get(input, output);
		output.set_content_length(output.get_body_size());
		output.get_body() = "";
	} else {
		http_method_not_allowed(input, output);
	}
}

void
uri_handler::populate_supported_methods()
{
	std::string method_str;
	bool first = true;

	for (std::map<int, bool>::iterator it = supported_methods_.begin();
	    it != supported_methods_.end(); ++it) {

		if (it->second) {
			if (first) {
				first = false;
			} else {
				method_str += ", ";
			}

			method_str += request::string_from_method(it->first);
		}
	}

	supported_methods_str_ = method_str;
}

bool
uri_handler::is_method_supported(int method) const
{
	std::map<int, bool>::const_iterator it = supported_methods_.find(
	    method);

	if (it != supported_methods_.end()) {
		return it->second;
	}

	return false;
}

void
uri_handler::update_docs(const std::string &root, const std::string &dir)
{
	doc_.update(root, dir);

	// Update the supported methods vector based on schemas
	std::map<int, bool> overview_supported_methods =
	    doc_.get_overview_supported_methods();

	for (std::map<int, bool>::const_iterator it =
	    overview_supported_methods.begin(); it !=
	    overview_supported_methods.end(); ++it) {
		if (it->second) {
			supported_methods_[it->first] = it->second;
		}
	}

	// Update supported methods str based on the new vector
	populate_supported_methods();
}

Json::Value
uri_handler::get_args_and_resume_as_json(const request &input)
{
	return input.get_args_and_resume_as_json(
	    doc_.get_arg_schema(input.get_method()));
}

/*
Filters the given list of Json objects, removing any that do not match
all of the key: value combinations in the filters object.
Returns the filtered list.
*/
std::vector<Json::Value>
uri_handler::filter_json_list_match_all(const std::vector<Json::Value> &list,
    const Json::Value &filters, int filter_comparison) {

	std::vector<Json::Value> filtered_list;

	std::vector<Json::Value>::const_iterator obj = list.begin();
	for(; obj != list.end(); ++obj) {
		// Check this object against all filters.
		Json::ValueIterator filter = filters.begin();
		bool passed = true;
		for(; filter != filters.end() ; filter++) {
			Json::Value fieldValue = (*obj)[filter.memberName()];
			Json::Value filterValue = *filter;
			switch (filter_comparison) {
			case FC_NOT_EQUAL:
				passed &= (fieldValue != filterValue);
				break;
			case FC_GREATER_THAN:
				passed &= (fieldValue > filterValue);
				break;
			case FC_LESS_THAN:
				passed &= (fieldValue < filterValue);
				break;
			case FC_EQUAL:
			default:
				passed &= (fieldValue == filterValue);
				break;
			}
			if (!passed) {
				// This object failed a filter.
				// Move on to the next object.
				break;
			}
		}
		if (passed) {
			filtered_list.push_back(*obj);
		}
	}

	return filtered_list;
}

/*
Filters the given list of Json objects, removing any that do not match
at least one of the key:value pairs in the filters object.
If a value in a pair is a vector, the filter will check the actual value
against each item in the vector for a possible match before rejecting.
Returns the filtered list.
*/
std::vector<Json::Value>
uri_handler::filter_json_list_match_any(const std::vector<Json::Value> &list,
    const Json::Value &filters, int filter_comparison) {

	std::vector<Json::Value> filtered_list;

	std::vector<Json::Value>::const_iterator obj = list.begin();
	for(; obj != list.end(); ++obj) {
		// Check this object against each filter.
		Json::ValueIterator filter = filters.begin();
		for(; filter != filters.end() ; filter++) {
			bool passed = false;
			Json::Value fieldValue = (*obj)[filter.memberName()];
			Json::Value splitFilterValues(Json::arrayValue);
			Json::Value rawFilterValue = *filter;
			if (rawFilterValue.type() == Json::arrayValue) {
				// Multiple allowed values were specified for
				// this field type.  Check for all of them.
				splitFilterValues = rawFilterValue;
			} else {
				// Just one possible allowed was sent for this
				// field type.  Create a 1-element array with
				// just that value to check.
				splitFilterValues.append(rawFilterValue);
			}
			Json::ValueIterator filterValue =
			    splitFilterValues.begin();
			for (; filterValue != splitFilterValues.end();
			    filterValue++) {
				switch (filter_comparison) {
				case FC_NOT_EQUAL:
					passed |= (fieldValue != *filterValue);
					break;
				case FC_GREATER_THAN:
					passed |= (fieldValue > *filterValue);
					break;
				case FC_LESS_THAN:
					passed |= (fieldValue < *filterValue);
					break;
				case FC_EQUAL:
				default:
					passed |= (fieldValue == *filterValue);
					break;
				}
				if (passed) {
					// This object passed the filters.
					// Move on to the next object.
					filtered_list.push_back(*obj);
					break;
				}
			}
			if (passed) {
				break;
			}
		}
	}

	return filtered_list;
}

void
uri_handler::format_list_response(const request &input,
    const std::vector<Json::Value> &list,
    const std::string &top_level_name,
    const std::string &primary_field,
    const std::vector<std::string> &field_sort_order,
    unsigned default_size_limit, response &output)
{
	std::map<std::string, int> field_sort_dirs;
	return format_list_response(input, list, top_level_name, primary_field,
	    field_sort_order, field_sort_dirs, default_size_limit, output);
}

void
uri_handler::format_list_response(const request &input,
    const std::vector<Json::Value> &list,
    const std::string &top_level_name,
    const std::string &primary_field,
    const std::vector<std::string> &field_sort_order,
    const std::map<std::string, int> &field_sort_dirs,
    unsigned default_size_limit, response &output, int default_sort_dir)
{
	ilog(IL_DEBUG, "In '%s' - primary_field: %s",
	    __FUNCTION__, primary_field.c_str());
	if (top_level_name.empty()) {
		throw api_exception(AEC_EXCEPTION,
		    "No top level name specified.");
	}

	if (field_sort_order.empty()) {
		throw api_exception(AEC_EXCEPTION,
		    "No field sort order specified.");
	}

	if (default_size_limit < 1) {
		throw api_exception(AEC_EXCEPTION,
		    "list size limit must be > 0.");
	}

	bool found_resume_arg = false;
	std::string resume_str;
	std::string args_str;
	std::string item_str;
	Json::Value item_json(Json::objectValue);

	/*
	 * Get the combined query args as json
	 */
	unsigned int method_tmp = input.get_method();
	// use GET schema for HEAD requests, since HEAD schema may not exist
	if (method_tmp == request::HEAD) {
		method_tmp = request::GET;
	}
	const Json::Value &arg_schema = doc_.get_arg_schema(method_tmp);
	if (arg_schema.isNull()) {
		// VALIDATED_RESUME_TOKEN_VERSION requires schemas.
		throw api_exception(AEC_EXCEPTION,
		    "Resume token schema not found.");
	}
	Json::Value args_json = input.get_args_and_resume_as_json(arg_schema);

	/*
	 * Find the resume token in the args if present, to get the last item.
	 */
	if (json_to_c(resume_str, args_json, RESUME_QUERY_ARG)) {
		found_resume_arg = true;

		resume_input_token rit(resume_str,
		    VALIDATED_RESUME_TOKEN_VERSION);
		rit >> args_str >> item_str;

		Json::Reader reader;
		if (!reader.parse(item_str, item_json)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The resume token could not be parsed.");
		}
	}

	// Get sort arg
	std::string arg_sort = primary_field;
	json_to_c(arg_sort, args_json, SORT_QUERY_ARG);

	if (std::find(field_sort_order.begin(), field_sort_order.end(),
	    arg_sort) == field_sort_order.end()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Cannot sort on field: %s", arg_sort.c_str());
	}

	// Get dir arg
	int arg_dir = default_sort_dir;
	std::string dir_str;
	if (json_to_c(dir_str, args_json, DIR_QUERY_ARG)) {
		arg_to_sort_dir conv;
		conv(arg_dir, dir_str);
	}

	if (arg_dir != SORT_DIR_ASC && arg_dir != SORT_DIR_DESC) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Sort direction query arg is invalid.");
	}

	// Get limit arg
	unsigned arg_limit = default_size_limit;
	json_to_c(arg_limit, args_json, LIMIT_QUERY_ARG);
	if (arg_limit == 0) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Limit argument must be greater than zero.");
	}
	arg_limit = std::min(arg_limit, default_size_limit);

	/*
	 * Make a copy of the input list and sort it
	 */
	std::vector<Json::Value> sorted_list(list.begin(), list.end());
	json_sorter list_sorter(arg_sort, field_sort_order, field_sort_dirs,
	    arg_dir);
	std::sort(sorted_list.begin(), sorted_list.end(), list_sorter);

	/*
	 * Iterate over the collection, starting at the beginning, or with the
	 * sentinel value if we are resuming a previous list operation.
	 * Continue until we reach the end, or we reach the limit of items to
	 * return.
	 */
	std::vector<Json::Value>::iterator it = sorted_list.begin();
	if (found_resume_arg) {
		json_sorter resume_sorter(arg_sort, field_sort_order, arg_dir,
		    true);

		it = std::upper_bound(sorted_list.begin(), sorted_list.end(),
		    item_json, resume_sorter);
	}

	Json::Value output_list(Json::arrayValue);
	for (unsigned i = 0; it != sorted_list.end() && i < arg_limit; ++it) {
		output_list.append(*it);
		i++;
	}

	/*
	 * If we didn't finish the list, we need a resume token.
	 */
	Json::Value &output_json = output.get_json();
	if (it != sorted_list.end()) {
		--it;

		/*
		 * The value of the field we sorted on is included in the
		 * output resume token as well as the primary key (ID)
		 * provided (if they differ).  We will store these value as
		 * json in the output resume token, which will be obfuscated.
		 */
		item_json.clear();
		item_json[arg_sort] = (*it)[arg_sort];

		if (primary_field != arg_sort) {
			item_json[primary_field] = (*it)[primary_field];
		}

		Json::FastWriter writer;
		item_str = writer.write(item_json);

		/*
		 * If there was no resume argument found, we store the query
		 * args as a string in the resume token.
		 */
		if (!found_resume_arg) {
			args_str = writer.write(args_json);
		} else {
			// Remove "resume" from args
			args_json.removeMember(RESUME_QUERY_ARG);
		}

		/*
		 * Put the original query args and the last item into the resume
		 * token.
		 */
		resume_output_token rot(VALIDATED_RESUME_TOKEN_VERSION);
		rot << args_str << item_str;

		output_json["resume"] = rot.str();
	} else {
		output_json["resume"] = Json::nullValue;
	}

	/*
	 * Make sure the output fields match the names used in the URI
	 * specification.
	 */
	output_json[top_level_name] = output_list;
	output_json["total"] = list.size();
}

void
uri_handler::format_list_response_new(const request &input,
    const std::vector<Json::Value> &list,
    const std::string &top_level_name,
    const std::vector<std::string> &unique_fields,
    const std::vector<std::string> &field_sort_order,
    const std::map<std::string, int> &field_sort_dirs,
    unsigned default_size_limit, response &output)
{
	if (top_level_name.empty()) {
		throw api_exception(AEC_EXCEPTION,
		    "No top level name specified.");
	}

	if (unique_fields.empty()) {
		throw api_exception(AEC_EXCEPTION,
		    "No unique fields specified.");
	}

	if (field_sort_order.empty()) {
		throw api_exception(AEC_EXCEPTION,
		    "No field sort order specified.");
	}

	// All fields at this index and before will be in the resume token
	size_t resume_field_sort_order_index = 0;

	// Verify that field_sort_order contains unique_fields
	for (std::vector<std::string>::const_iterator uit =
	    unique_fields.begin(); uit != unique_fields.end(); ++uit) {
		bool found = false;
		size_t i = 0;

		for (std::vector<std::string>::const_iterator sit =
		    field_sort_order.begin(); sit != field_sort_order.end();
		    ++sit) {
			if (*uit == *sit) {
				found = true;

				if (i > resume_field_sort_order_index) {
					resume_field_sort_order_index = i;
				}

				break;
			}
			i++;
		}

		if (!found) {
			 throw api_exception(AEC_EXCEPTION,
			     "unique_fields must appear in field_sort_order");
		}
	}

	if (default_size_limit < 1) {
		throw api_exception(AEC_EXCEPTION,
		    "list size limit must be > 0.");
	}

	bool found_resume_arg = false;
	std::string resume_str;
	std::string args_str;
	std::string item_str;
	Json::Value item_json(Json::objectValue);

	/*
	 * Get the combined query args as json
	 */
	unsigned int method_tmp = input.get_method();
	// use GET schema for HEAD requests, since HEAD schema may not exist
	if (method_tmp == request::HEAD) {
		method_tmp = request::GET;
	}
	const Json::Value &arg_schema = doc_.get_arg_schema(method_tmp);
	if (arg_schema.isNull()) {
		// VALIDATED_RESUME_TOKEN_VERSION requires schemas.
		throw api_exception(AEC_EXCEPTION,
		    "Resume token schema not found.");
	}
	Json::Value args_json = input.get_args_and_resume_as_json(arg_schema);

	/*
	 * Find the resume token in the args if present, to get the last item.
	 */
	if (json_to_c(resume_str, args_json, RESUME_QUERY_ARG)) {
		found_resume_arg = true;

		resume_input_token rit(resume_str,
		    VALIDATED_RESUME_TOKEN_VERSION);
		rit >> args_str >> item_str;

		Json::Reader reader;
		if (!reader.parse(item_str, item_json)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The resume token could not be parsed.");
		}
	}

	// Get sort arg
	std::string arg_sort = field_sort_order[0];
	json_to_c(arg_sort, args_json, SORT_QUERY_ARG);

	if (std::find(field_sort_order.begin(), field_sort_order.end(),
	    arg_sort) == field_sort_order.end()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Cannot sort on field: %s", arg_sort.c_str());
	}

	// Get dir arg
	int arg_dir = SORT_DIR_ASC;
	if (field_sort_dirs.count(arg_sort) > 0) {
		arg_dir = field_sort_dirs.find(arg_sort)->second;
	}

	std::string dir_str;
	if (json_to_c(dir_str, args_json, DIR_QUERY_ARG)) {
		arg_to_sort_dir conv;
		conv(arg_dir, dir_str);
	}

	if (arg_dir != SORT_DIR_ASC && arg_dir != SORT_DIR_DESC) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Sort direction query arg is invalid.");
	}

	// Get limit arg
	unsigned arg_limit = default_size_limit;
	json_to_c(arg_limit, args_json, LIMIT_QUERY_ARG);
	if (arg_limit == 0) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Limit argument must be greater than zero.");
	}
	arg_limit = std::min(arg_limit, default_size_limit);

	/*
	 * Make a copy of the input list and sort it
	 */
	std::vector<Json::Value> sorted_list(list.begin(), list.end());
	json_sorter list_sorter(arg_sort, field_sort_order, field_sort_dirs,
	    arg_dir);
	std::sort(sorted_list.begin(), sorted_list.end(), list_sorter);

	/*
	 * Iterate over the collection, starting at the beginning, or with the
	 * sentinel value if we are resuming a previous list operation.
	 * Continue until we reach the end, or we reach the limit of items to
	 * return.
	 */
	std::vector<Json::Value>::iterator it = sorted_list.begin();
	if (found_resume_arg) {
		json_sorter resume_sorter(arg_sort, field_sort_order,
		    field_sort_dirs, arg_dir, true);

		it = std::upper_bound(sorted_list.begin(), sorted_list.end(),
		    item_json, resume_sorter);
	}

	Json::Value output_list(Json::arrayValue);
	for (unsigned i = 0; it != sorted_list.end() && i < arg_limit; ++it) {
		output_list.append(*it);
		i++;
	}

	/*
	 * If we didn't finish the list, we need a resume token.
	 */
	Json::Value &output_json = output.get_json();
	if (it != sorted_list.end()) {
		--it;

		/*
		 * The sort arg, the unique fields, as well as any fields that
		 * appear before a unique field are stored in the resume token.
		 */
		item_json.clear();
		item_json[arg_sort] = (*it)[arg_sort];

		size_t i = 0;
		for (std::vector<std::string>::const_iterator sit =
		    field_sort_order.begin(); sit != field_sort_order.end() &&
		    i <= resume_field_sort_order_index; ++sit) {
			item_json[*sit] = (*it)[*sit];
			i++;
		}

		Json::FastWriter writer;
		item_str = writer.write(item_json);

		/*
		 * If there was no resume argument found, we store the query
		 * args as a string in the resume token.
		 */
		if (!found_resume_arg) {
			args_str = writer.write(args_json);
		} else {
			// Remove "resume" from args
			args_json.removeMember(RESUME_QUERY_ARG);
		}

		/*
		 * Put the original query args and the last item into the resume
		 * token.
		 */
		resume_output_token rot(VALIDATED_RESUME_TOKEN_VERSION);
		rot << args_str << item_str;

		output_json["resume"] = rot.str();
	} else {
		output_json["resume"] = Json::nullValue;
	}

	/*
	 * Make sure the output fields match the names used in the URI
	 * specification.
	 */
	output_json[top_level_name] = output_list;
	output_json["total"] = list.size();
}




/*
*  Helper function to replace all instances of <<version=x>> in
*  text with the PAPI version found in handler URI 'uri'.
*/
std::string
uri_handler::replace_version_placeholders(
    const std::string &text, const std::string &uri)
{
	size_t start = 0;
	size_t end = 0;
	std::string outstr = "";

	// Parse actual version number out of "uri" string.
	start = uri.find('/', 0);
	if (start == std::string::npos) {
		return text;
	}
	end = uri.find('/', start+1);
	if (end == std::string::npos) {
		return text;
	}
	std::string actual_version = uri.substr(start+1, (end-(start+1)));

	// Replace each instance of <<version=x>>
	start = 0;
	while (start < text.size()) {

		end = text.find("<<version=", start);
		if (end == std::string::npos) {
			// Came to the end of the file.
			// Append the rest of the file and return.
			outstr.append(text.substr(start));
			break;
		}

		// If there is a close tag,
		// replace <<version=x>> with actual_version.
		if (text.find(">>", end) != std::string::npos) {

			outstr.append(text.substr(start, end - start));
			outstr.append(actual_version);

		} else {
			// There was an open << with no close >>.
			// Just append the rest of the file and return.
			outstr.append(text.substr(start));
			break;
		}

		start = text.find(">>", end);
		start += 2;
	}
	return outstr;
}
