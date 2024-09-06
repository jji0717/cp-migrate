#ifndef __HANDLER_DOC_H__
#define __HANDLER_DOC_H__

#include <jsoncpp/json.h>

#include <isi_json/json_schema_validator.h>

#include "request.h"
#include "response.h"

class handler_doc {
public:

	static const std::string OVERVIEW_DOC;
	static const std::string OVERVIEW_SCHEMA;
	static const std::string INPUT_SCHEMA;
	static const std::string OUTPUT_SCHEMA;

	/** Construct empty handler doc specification */
	handler_doc();

	/** Construct from found paths and methods.
	 *
	 * @param r root path for documents in filesystem
	 * @param d URI path for handler
	 */
	handler_doc(const std::string &r, const std::string &d);

	/** Clears an existing handler doc object and reinitializes it */
	void update(const std::string &r, const std::string &d);

	/** Return supported methods found in the overview doc */
	const std::map<int, bool> &get_overview_supported_methods() const;

	/** Return resource overview documentation */
	const std::string &get_overview() const;

	/** Return input schema for @a method or json null if not found */
	const Json::Value &get_input_schema(int method) const;

	/** Return output schema for @a method or json null if not found */
	const Json::Value &get_output_schema(int method) const;

	/** Return query arg schema for @a method or json null if not found */
	const Json::Value &get_arg_schema(int method) const;

private:
	std::string doc_dir_;

	std::string overview_;

	std::map<int, Json::Value> input_schemas_;
	std::map<int, Json::Value> output_schemas_;
	std::map<int, Json::Value> arg_schemas_;

	std::map<int, bool> supported_methods_;

	void read_schema(const std::string &path, Json::Value &schema);

	Json::Value empty_schema_;
};

#endif
