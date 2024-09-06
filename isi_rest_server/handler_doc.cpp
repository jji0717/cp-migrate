#include <sys/stat.h>

#include <assert.h>

#include <iostream>
#include <fstream>
#include <sstream>

#include <isi_ilog/ilog.h>
#include <isi_util/isi_assert.h>

#include "handler_doc.h"

const std::string handler_doc::OVERVIEW_DOC    = "overview.txt";
const std::string handler_doc::OVERVIEW_SCHEMA = "overview.json";
const std::string handler_doc::INPUT_SCHEMA    = "input_schema.json";
const std::string handler_doc::OUTPUT_SCHEMA   = "output_schema.json";

handler_doc::handler_doc()
{ }

handler_doc::handler_doc(const std::string &root, const std::string &dir) :
    input_schemas_(), output_schemas_(), arg_schemas_(),
    supported_methods_(), empty_schema_(Json::nullValue)
{
	update(root, dir);
}

void
handler_doc::update(const std::string &root, const std::string &dir)
{
	// Clear data structures
	doc_dir_.erase();
	overview_.erase();
	input_schemas_.clear();
	output_schemas_.clear();
	arg_schemas_.clear();
	supported_methods_.clear();

	ASSERT(!root.empty(), "must provide a document directory!");

	doc_dir_ = root + dir + '/';

	/* Read overview document */
	std::string overview_doc_path = doc_dir_;
	overview_doc_path += OVERVIEW_DOC;
	std::ifstream overview_doc(overview_doc_path.c_str());

	if (overview_doc.is_open()) {
		std::stringstream buffer;
		buffer << overview_doc.rdbuf();
		overview_doc.close();
		overview_ = buffer.str();
		ilog(IL_TRACE, "Loaded file: %s", overview_doc_path.c_str());
	} else {
		ilog(IL_DEBUG, "Overview doc '%s' does not exist!",
		    overview_doc_path.c_str());
	}

	/* Read the overview schema */
	std::string overview_schema_path = doc_dir_;
	overview_schema_path += OVERVIEW_SCHEMA;
	std::ifstream overview_schema(overview_schema_path.c_str());
	Json::Value overview_schema_json;

	if (overview_schema.is_open()) {
		Json::Reader reader;
		
		if (!reader.parse(overview_schema, overview_schema_json)) {
			std::string err =
			    reader.getFormattedErrorMessages();
			ilog(IL_ERR,
			    "Error loading overview schema %s: %s",
			    overview_schema_path.c_str(), err.c_str());
		} else {
			ilog(IL_TRACE, "Loaded json file: %s",
			    overview_schema_path.c_str());
		}
	} else {
		ilog(IL_DEBUG, "Overview schema '%s' does not exist!",
		    overview_schema_path.c_str());
	}

	/* Populate supported methods based on overview doc */
	for (int i = request::GET; i < request::NUM_METHODS; ++i) {
		std::string method_str = request::string_from_method(i);
		std::string key_name = method_str + "_args";

		if (overview_schema_json.isMember(key_name)) {
			supported_methods_[i] = true;
		}
	}

	/* Read error schema */
	Json::Value error_schema;
	std::string error_path = root + "/error_schema.json";
	read_schema(error_path, error_schema);

	/* Read json schemas */
	for (int i = request::GET; i < request::NUM_METHODS; ++i) {

		/* Supported method? Also, skip HEAD for now */
		if (!supported_methods_[i] || i == request::HEAD) {
			continue;
		}

		// Store the query arg schemas
		std::string method_str = request::string_from_method(i);
		std::string key_name = method_str + "_args";
		arg_schemas_[i] = overview_schema_json[key_name];

		std::string file_prefix = doc_dir_ + method_str + "_";

		// Read the input schemas
		if (method_has_input(i)) {
			std::string path = file_prefix + INPUT_SCHEMA;
			read_schema(path, input_schemas_[i]);
		}

		// Read the output schemas
		if (method_has_output(i)) {
			Json::Value schema = Json::objectValue;
			schema["type"] = Json::arrayValue;
			schema["type"].append(error_schema);
			schema["type"].append(Json::nullValue);
			output_schemas_[i] = schema;

			std::string path = file_prefix + OUTPUT_SCHEMA;
			read_schema(path, output_schemas_[i]["type"][1]);
		}
	}
}

void
handler_doc::read_schema(const std::string &path, Json::Value &schema)
{
	Json::Reader reader;
	std::ifstream ifs(path.c_str());
	struct stat sb;

	if (!ifs.is_open() ||
	    stat(path.c_str(), &sb) != 0) {
		ilog(IL_DEBUG, "Json '%s' does not exist", path.c_str());
		return;
	}

	/* allow empty files to disable json without warning */
	if (sb.st_size == 0) {
		return;
	}

	if (!reader.parse(ifs, schema)) {
		std::string err = reader.getFormattedErrorMessages();
		ilog(IL_ERR, "Error loading json file %s: %s", path.c_str(),
		    err.c_str());
		return;
	}

	ilog(IL_TRACE, "Loaded json file: %s", path.c_str());
}

const std::map<int, bool> &
handler_doc::get_overview_supported_methods() const
{
	return supported_methods_;
}

const std::string &
handler_doc::get_overview() const
{
	return overview_;
}

const Json::Value &
handler_doc::get_input_schema(int method) const
{
	if (input_schemas_.count(method) == 0) {
		return empty_schema_;
	} else {
		return input_schemas_.find(method)->second;
	}
}

const Json::Value &
handler_doc::get_output_schema(int method) const
{
	if (output_schemas_.count(method) == 0) {
		return empty_schema_;
	} else {
		return output_schemas_.find(method)->second;
	}
}

const Json::Value &
handler_doc::get_arg_schema(int method) const
{
	if (arg_schemas_.count(method) == 0) {
		return empty_schema_;
	} else {
		return arg_schemas_.find(method)->second;
	}
}

