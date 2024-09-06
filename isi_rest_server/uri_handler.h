#ifndef __URI_HANDLER_H__
#define __URI_HANDLER_H__

#include <isi_util_cpp/scoped_lock.h>
#include <isi_util_cpp/user_text.h>
#include <iostream>

#include "api_utils.h"
#include "handler_doc.h"
#include "method_serializer.h"
#include "method_stats.h"
#include "priv_filter.h"
#include "request.h"
#include "response.h"
#include <functional>

/**
 * Default criteria is no specific criteria
 * But developers may want to gate handlers from
 * executing by mapping in some criteria BEFORE
 * we execute the particular method in the request.
 * The one below is a passthrough and is the default.
 * Ones that decide not to allow it should throw api_exception
 * instances.
 */
typedef void (*environmental_criteria)(const request &);

inline void
no_environmental_criteria(const request &unused) { return; }

class uri_handler {
private:
	const priv_filter priv_filter_;
	method_serializer serializer_;

	static serializer_none default_serializer;

	environmental_criteria environmental_criteria_;

public:
	enum filter_comparisons {
		FC_EQUAL = 0,
		FC_NOT_EQUAL,
		FC_GREATER_THAN,
		FC_LESS_THAN
	};
	bool force_schema_validation = true;

	/** URI handler base class.
	 *
	 * @param pf filter to check uri privs
	 */
	uri_handler(const priv_filter &pf = priv_filter::empty_filter)
	    : priv_filter_(pf), serializer_(uri_handler::default_serializer),
	    environmental_criteria_(no_environmental_criteria),
	    supported_methods_(), initialized_(true)
	{
		supported_methods_[request::HEAD] = true;
	}

	uri_handler(const priv_filter &pf, method_serializer serializer)
	    : priv_filter_(pf), serializer_(serializer),
	    environmental_criteria_(no_environmental_criteria),
	    supported_methods_(),
	    initialized_(true)
	{
		supported_methods_[request::HEAD] = true;
	}

	uri_handler(const priv_filter &pf, bool force_schema_val)
	    : priv_filter_(pf), serializer_(uri_handler::default_serializer),
	    environmental_criteria_(no_environmental_criteria),
	    supported_methods_(), initialized_(true)
	{
		supported_methods_[request::HEAD] = true;
		force_schema_validation = force_schema_val;
	}

	uri_handler(const priv_filter &pf, method_serializer serializer,
	        bool force_schema_val)
	    : priv_filter_(pf), serializer_(serializer),
	    environmental_criteria_(no_environmental_criteria),
	    supported_methods_(),
	    initialized_(true)
	{
		supported_methods_[request::HEAD] = true;
		force_schema_validation = force_schema_val;
	}

	uri_handler(const priv_filter &pf, environmental_criteria ec)
	    : priv_filter_(pf),
	      serializer_(uri_handler::default_serializer),
	      environmental_criteria_(ec),
	      supported_methods_(), initialized_(true)
	{
		supported_methods_[request::HEAD] = true;
	}

	virtual ~uri_handler()
	{ }

	void execute_http_method(request &input, response &output, bool validate_schema = true);
	virtual void remove_undefined_query_args(request &input);
	void http_method_not_allowed(const request &input, response &output);
	virtual bool validate_input(const request &input, response &output, bool validate_schema = true);
	virtual bool validate_output(const request &input, response &output);

	virtual void get_describe_json(const request &input, response &output);
	virtual void get_describe(const request &input, response &output);
	virtual std::string describe_method(unsigned method);

	virtual void http_get(const request &input, response &output);
	virtual void http_put(const request &input, response &output);
	virtual void http_post(const request &input, response &output);
	virtual void http_delete(const request &input, response &output);
	virtual void http_head(const request &input, response &output);

	/** Populate the supported methods string based on the vector */
	void populate_supported_methods();

	bool is_method_supported(int method) const;

	/** update docs and supported methods vector from root/dir */
	void update_docs(const std::string &root, const std::string &dir);

	static const std::string describe_option;
	static const std::string list_option;
	static const std::string internal_option;
	static const std::string json_option;

	/** Per method call stats */
	method_stats stats_[request::NUM_METHODS + 1];

	/**
	 * Filters the given list of Json objects, removing any that do not
	 * exactly match (contain) all of the key: value combinations in the
	 * filters object.  Returns the filtered list.
	 * @ list The list of Json objects to filter.
	 * @ filters A Json object containing field: value pairs to match in
	 * each object.
	*/
	static std::vector<Json::Value>
	filter_json_list_match_all(const std::vector<Json::Value> &list,
	    const Json::Value &filters, int filter_comparison = FC_EQUAL);

	/**
	 * Filters the given list of Json objects, removing any that do not
	 * match at least one of the key: value combinations in the filters
	 * object.  Returns the filtered list.
	 * @ list The list of Json objects to filter.
	 * @ filters A Json object containing field:value pairs to match in
	 * each object.  If a value in a pair is a vector, the filter will
	 * check the actual value against each item in the vector for a
	 * possible match before rejecting.
	*/
	static std::vector<Json::Value>
	filter_json_list_match_any(const std::vector<Json::Value> &list,
	    const Json::Value &filters, int filter_comparison = FC_EQUAL);

	/**
	 * These two functions require the primary_field to be unique.  For a
	 * version without that requirement, use format_list_response_new.
	 *
	 * Format a list of JSON object, including sorting the list and
	 * inserting a resume token (if necessary).
	 *
	 * @a input the request structure
	 * @a top_level_name the name to use for the JSON list that is created
	 * @a primary_field the field name for the primary key (ID) for the
	 * list data. This must be a unique field.
	 * @a field_sort_order order in which list should be sorted
	 * (this does not have to include all fields of the object)
	 * @a field_sort_dirs sort direction for specific fields
	 * (this does not have to include all fields of the object)
	 * @a default_size_limit the maximum number of list elements that
	 * will be returned before a resume token is created
	 * @a output the output structure
	 */
	void format_list_response(const request &input,
	    const std::vector<Json::Value> &list,
	    const std::string &top_level_name,
	    const std::string &primary_field,
	    const std::vector<std::string> &field_sort_order,
	    unsigned default_size_limit, response &output);

	void format_list_response(const request &input,
	    const std::vector<Json::Value> &list,
	    const std::string &top_level_name,
	    const std::string &primary_field,
	    const std::vector<std::string> &field_sort_order,
	    const std::map<std::string, int> &field_sort_dirs,
	    unsigned default_size_limit, response &output,
	    int default_sort_dir = SORT_DIR_ASC);

	/**
	 * Format a list of JSON objects, including sorting the list and
	 * inserting a resume token (if necessary).
	 *
	 * @a input The request structure.
	 *
	 * @a top_level_name The name to use for the JSON list that is created.
	 *
	 * @a unique_fields A list of one or more fields that combine to
	 * uniquely identify the object.  Most collections will only need "id"
	 * in this list.  These fields must appear in the field_sort_order.
	 * The resume token will include all the unique_fields and all fields
	 * in the field_sort_order that appear before any of the unique_fields.
	 * Therefore, unique_fields should appear as early possible in the
	 * field_sort_order to reduce the size of the resume token.
	 *
	 * @a field_sort_order The priority order of fields for comparing
	 * objects.  Only fields in this list are valid inputs for the sort
	 * argument.
	 *
	 * @a field_sort_dirs The sort direction for specific fields.  If a
	 * field is not specified, sorting will be done in ascending order.
	 *
	 * @a default_size_limit The maximum number of list elements that
	 * will be returned before a resume token is created.
	 *
	 * @a output The output structure.
	 */
	void format_list_response_new(const request &input,
	    const std::vector<Json::Value> &list,
	    const std::string &top_level_name,
	    const std::vector<std::string> &unique_fields,
	    const std::vector<std::string> &field_sort_order,
	    const std::map<std::string, int> &field_sort_dirs,
	    unsigned default_size_limit, response &output);

	Json::Value get_args_and_resume_as_json(const request &input);

private:
	/*
	*  Helper function to replace all instances of <<version=x>> in
	*  text with the PAPI version found in handler URI 'uri'.
	*/
	std::string
	replace_version_placeholders(const std::string &, const std::string &);

	std::string supported_methods_str_;

protected:
	handler_doc doc_;
	std::map<int, bool> supported_methods_;
	bool initialized_;

};

#endif

