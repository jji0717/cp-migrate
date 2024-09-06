#include "auto_version_handler.h"

#include <isi_ilog/ilog.h>

auto_version_handler::auto_version_handler(
    uri_handler *current_version_handler, int priv) :
    uri_handler(priv_filter(priv).set_run_as(false))
{
	current_version_handler_ = current_version_handler;
}

auto_version_handler::auto_version_handler(
    uri_handler *current_version_handler, const priv_filter &pf) :
    uri_handler(pf)
{
	current_version_handler_ = current_version_handler;
}

void
auto_version_handler::http_get(const request &input, response &output)
{
	if (is_method_supported(request::GET)) {
		current_version_handler_->http_get(input, output);
		downgrade_get_output(output);
	} else {
		 http_method_not_allowed(input, output);
	}
}

void
auto_version_handler::http_put(const request &input, response &output)
{
	if (is_method_supported(request::PUT)) {
		current_version_handler_->http_put(input, output);
	} else {
		 http_method_not_allowed(input, output);
	}
}

void
auto_version_handler::http_post(const request &input, response &output)
{
	if (is_method_supported(request::POST)) {
		current_version_handler_->http_post(input, output);
		downgrade_post_output(output);
	} else {
		 http_method_not_allowed(input, output);
	}
}

void
auto_version_handler::http_delete(const request &input, response &output)
{
	if (is_method_supported(request::DELETE)) {
		current_version_handler_->http_delete(input, output);
	} else {
		 http_method_not_allowed(input, output);
	}
}

void
auto_version_handler::http_head(const request &input, response &output)
{
	if (is_method_supported(request::HEAD)) {
		current_version_handler_->http_head(input, output);
	} else {
		 http_method_not_allowed(input, output);
	}
}

void
auto_version_handler::downgrade_get_output(response &output)
{
	const Json::Value &schema = doc_.get_output_schema(request::GET);

	if (!schema.isNull()) {
		remove_undefined_fields(output.get_json(), schema);
	}
}

void
auto_version_handler::downgrade_post_output(response &output)
{
	const Json::Value &schema = doc_.get_output_schema(request::POST);

	if (!schema.isNull()) {
		remove_undefined_fields(output.get_json(), schema);
	}
}

/*
 * Removes any query arguments from the input if the query arg schema is
 * present and the arg is not in the schema.
 */
void
auto_version_handler::remove_undefined_query_args(request &input)
{
	const Json::Value &arg_schema = (input.get_method() == request::HEAD) ?
	    doc_.get_arg_schema(request::GET) :
	    doc_.get_arg_schema(input.get_method());

	if (!arg_schema.isNull() && arg_schema.isObject()) {
		// If the schema explicitly allows addional properties,
		// don't remove any fields.
		if (arg_schema.isMember("additionalProperties")) {
			const Json::Value &addProp =
			    arg_schema["additionalProperties"];
			if (addProp.isBool() && addProp.asBool()) {
				return;
			}
		}

		std::vector<std::string> args_present;

		for (request::arg_iter it = input.get_arg_map().begin();
		    it != input.get_arg_map().end(); ++it) {
			args_present.push_back(it->first);
		}

		for (std::vector<std::string>::iterator it =
		    args_present.begin(); it != args_present.end(); ++it) {
			bool arg_in_schema = false;

			if (arg_schema.isMember("properties") &&
			    arg_schema["properties"].isObject()) {
				if (arg_schema["properties"].isMember(*it)) {
					arg_in_schema = true;
				}
			}

			if (!arg_in_schema) {
				if (*it != "_dc" && *it != "_cb") {
					/* WebUI uses spurious _dc and _cb arg as
					   a cache-busting technique */
					ilog(IL_INFO,
					    "%s: removing undefined query arg: %s",
					    input.get_uri().c_str(),
					    it->c_str());
				}
				input.remove_query_arg(*it);
			}
		}
	}
}

void
auto_version_handler::remove_undefined_fields(Json::Value &data,
    const Json::Value &schema)
{
	if (!data.isObject() && !data.isArray()) {
		return;
	}

	if (schema.isObject() && schema.isMember("type")) {
		/*
		 * Since the introduction of the error schema, the original schema is
		 * put as it is in the type array after error schema. Auto version
		 * handler is not concerned with the error schema so we extract the
		 * original output schema.
		 */
		bool error_schema_present = schema["type"].isArray() &&
			schema["type"].size() == 2 &&
			schema["type"][0].isObject() &&
			schema["type"][0].isMember("properties") &&
			schema["type"][0]["properties"].isMember("errors");
		/*
		 * Extract the original output schema only if error schema is
		 * present at index 0 of the array; otherwise this might be a
		 * recursive call.
		 */
		const Json::Value &type_val = (error_schema_present &&
			schema["type"][1].isMember("type")) ? schema["type"][1]["type"] :
			schema["type"];

		Json::Value actual_schema = error_schema_present ?
			schema["type"][1] : schema;

		if (type_val.isString()) {
			// Normal Types

			if (type_val.asString() == "object" &&
			    data.isObject() && !data.isNull()) {
				// Type and data are object
				remove_undefined_fields_object(data, actual_schema);

			} else if (type_val.asString() == "array" &&
			    data.isArray()) {
				// Type and data are array
				remove_undefined_fields_array(data, actual_schema);
			}

		} else if (type_val.isArray()) {
			// Type is array of schemas
			remove_undefined_fields_type_array(data, type_val);
		}
	}
}

void
auto_version_handler::remove_undefined_fields_object(Json::Value &data,
    const Json::Value &schema)
{
	if (!data.isObject() || !schema.isObject()) {
		return;
	}

	if (schema.isMember("additionalProperties")) {
		const Json::Value &addProp = schema["additionalProperties"];
		if (addProp.isBool() && addProp.asBool()) {
			// If the schema explicitly allows addional properties,
			// don't remove any fields.
			return;
		}
	}

	// Find undefined fields
	std::vector<std::string> undefined_fields;
	if (schema.isMember("properties")) {
		const Json::Value &properties_obj = schema["properties"];

		// Add each data field not in schema to undefined_fields
		for (Json::Value::iterator it = data.begin(); it != data.end();
                    ++it) {
			if (!properties_obj.isMember(it.memberName())) {
				undefined_fields.push_back(it.memberName());
			}
		}

		// Remove undefined fields from data
		for (std::vector<std::string>::iterator it =
		    undefined_fields.begin(); it != undefined_fields.end();
		    ++it) {
			data.removeMember(*it);
		}

		// Recursive call for remaining fields
		for (Json::Value::iterator it = data.begin(); it != data.end();
                    ++it) {
			remove_undefined_fields(data[it.memberName()],
			    properties_obj[it.memberName()]);
		}
	}
}

void
auto_version_handler::remove_undefined_fields_array(Json::Value &data,
    const Json::Value &schema)
{
	if (!data.isArray() || !schema.isObject()) {
		return;
	}

	if (schema.isMember("items")) {
		const Json::Value &items_schema = schema["items"];

		// Recursive call for each item in array
		for (int i = 0; i < data.size(); ++i) {
			remove_undefined_fields(data[i], items_schema);
		}
	}
}

/*
 * This function attempts to remove undefined fields in an object, when the
 * schema type is an array of possible schemas.  This cannot be done correctly
 * in all cases.  For exampe if the type array contains two object schemas with
 * different properties, and the data object contains some properties from both
 * schemas, we can't tell which fields to remove.
 * In that case this function assumes the schema with the most properties in
 * common with the data is the correct schema.
 */
void
auto_version_handler::remove_undefined_fields_type_array(Json::Value &data,
    const Json::Value &schema_array)
{
	if (!schema_array.isArray() || schema_array.empty()) {
		return;
	}

	// Determine whether we are looking for an object or array schema
	bool desire_object = false;
	bool desire_array = false;

	if (data.isObject() && !data.isNull()) {
		desire_object = true;
	} else if (data.isArray()) {
		desire_array = true;
	} else {
		return;
	}

	int schema_index = 0;
	bool found_schema = false;
	int max_common_properties = -1;

	// Iterate over possible schemas
	for (int i = 0; i < schema_array.size(); ++i) {
		const Json::Value &schema = schema_array[i];

		if (!schema.isObject() || !schema.isMember("type")) {
			continue;
		}

		const Json::Value &type_val = schema["type"];

		if (!type_val.isString()) {
			continue;
		}

		if (desire_array && type_val.asString() == "array") {
			if (!found_schema) {
				found_schema = true;
				schema_index = i;
			} else {
				ilog(IL_ERR,
				    "Array auto versioning failed.");
				return;
			}
		} else if (desire_object && type_val.asString() == "object") {
			// Find the object schema most similar to data object

			if (!schema.isMember("properties")) {
				continue;
			}

			int common_properties = 0;
			const Json::Value &properties_obj =
			    schema["properties"];

			if (!properties_obj.isObject()) {
				continue;
			}

			for (Json::Value::iterator it = data.begin();
			    it != data.end(); ++it) {
				if (properties_obj.isMember(it.memberName())) {
					common_properties++;
				}
			}

			if (common_properties > max_common_properties) {
				found_schema = true;
				schema_index = i;
				max_common_properties = common_properties;
			}
		}
	}

	// Recursive call for the schema we found
	if (found_schema) {
		remove_undefined_fields(data, schema_array[schema_index]);
	}
}

