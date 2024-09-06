#include <assert.h>
#include <errno.h>

#include <vector>

#include <sys/isi_ntoken.h>

#include <isi_config/array.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_printf.h>
#include <isi_util_cpp/safe_strerror.h>
#include <isi_version/isi_version.h>

#include "json_sorter.h"
#include "resume_token.h"
#include "api_utils.h"

const bool g_debug = (get_kern_osflavor() & KERN_OSFLAVOR_DEBUG);
const bool g_release = !g_debug;

const char *SORT_QUERY_ARG   = "sort";
const char *DIR_QUERY_ARG    = "dir";
const char *LIMIT_QUERY_ARG  = "limit";
const char *RESUME_QUERY_ARG = "resume";
const char *SCOPE_QUERY_ARG  = "scope";

const char *VAL_AT_DEFAULT = "@DEFAULT";

const unsigned VALIDATED_RESUME_TOKEN_VERSION = 1001;

const std::string ARG_SCOPE = SCOPE_QUERY_ARG;
const std::string ARG_RESUME = RESUME_QUERY_ARG;

/*
 * Validate that a given field is in the list of sortable fields.
 *
 * @a field field name to search for in list
 * @a list the list to search in
 */
void validate_field_in_sort_list(const std::string& field,
    const std::vector<std::string>& list) {

	if (std::find(list.begin(), list.end(), field) == list.end()) {
		std::string str = "Field '";
		str += field;
		str += "' is not a valid field to sort on.";
		throw api_exception(AEC_BAD_REQUEST, str);
	}
}

std::string
quote_shell_args(const std::string &str)
{
	size_t len;

	if ((len = shquote(str.c_str(), NULL, 0)) == (size_t)-1)
		throw isi_exception("Error shell quoting input '%s'",
		    str.c_str());

	len++; // shquote wants buffer size but returns length

	std::vector<char> buf(len);

	len = shquote(str.c_str(), &buf[0], len);
	assert(len != (size_t)-1);

	return std::string(&buf[0]);
}

request::arg_iter_pair
find_args_helper(const request::arg_map &args, const std::string &name,
    bool req, bool mult)
{
	request::arg_iter_pair range = args.equal_range(name);

	if (range.first == range.second) {
		if (req)
			throw api_exception(AEC_BAD_REQUEST,
			    "Missing argument '%s'", name.c_str());
		else
			return range;
	}

	if (++range.first != range.second && !mult)
		throw api_exception(AEC_BAD_REQUEST,
		    "Duplicate argument %s", name.c_str());

	--range.first;

	return range;
}

bool
find_resume_arg(std::string &resume_str, const request::arg_map &args,
    const std::string *exclusive_args[])
{
	if (!find_args(resume_str, args, RESUME_QUERY_ARG))
		return false;

	std::string str;

	bool ok  = true;
	int  cnt = 0;

	while (true) {
		if (exclusive_args[cnt] == NULL)
			break;

		if (find_args(str, args, *exclusive_args[cnt++]))
			ok = false;
	}

	if (ok)
		return true;

	/* build exception message */
	int i = 0;
	str = *exclusive_args[i];
	for (++i; i < cnt - 1; ++i) {
		str += ", ";
		str += *exclusive_args[i];
	}
	if (i < cnt) {
		str += " and ";
		str += *exclusive_args[i];
	}

	throw api_exception(AEC_BAD_REQUEST, "Args %s not permitted with %s",
	    str.c_str(), RESUME_QUERY_ARG);
}

void
arg_to_time::operator()(time_t &val, const std::string &str)
{
	char *endptr;
	long long temp_val;
	
	if (str.empty())
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid time specification: '%s'", "");

	errno = 0;
	temp_val = strtoll(str.c_str(), &endptr, 10);

	if (*endptr != '\0'  || errno || (no_neg_ && (temp_val < 0)))
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid time specification: '%s'", str.c_str());

	if (temp_val < 0) {
		temp_val +=  time(0);
		if (temp_val < 0)
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid time specification: '%s'", str.c_str());
	}
	val = temp_val;
}

void
arg_to_field::operator()(int &val, const std::string &str) const
{
	for (unsigned i = 0; i < cnt_; ++i)
		if (str == fields_[i].name) {
			val = fields_[i].val;
			return;
		}

	throw api_exception(AEC_BAD_REQUEST,
	    "Invalid field specification: '%s'", str.c_str());
}

std::string
arg_to_field::operator()(int val) const
{
	for (unsigned int i = 0; i != cnt_; ++i)
		if (fields_[i].val == val)
			return fields_[i].name;
	assert(0);
	return "";
}

void
arg_validate_field::operator()(const std::string &str) const
{
	for (unsigned i = 0; i < cnt_; ++i)
		if (str == fields_[i].name)
			return;

	throw api_exception(AEC_BAD_REQUEST,
	    "Invalid field specification: '%s'", str.c_str());
}

void
arg_validate_field::operator()(int val) const
{
	for (unsigned int i = 0; i != cnt_; ++i)
		if (fields_[i].val == val)
			return;

	throw api_exception(AEC_BAD_REQUEST,
	    "Invalid field specification: '%d'", val);
}

arg_to_devs::arg_to_devs(bool allow_zero, bool check_valid):
	all_(false), allow_zero_(allow_zero), check_valid_(check_valid)
{
}

void
arg_to_devs::operator()(uint16_set &devs, const std::string &str)
{
	uint16_t devid;
	struct arr_device	*dev = NULL;
	void			**dppv;

	if (str == "all") {
		all_ = true;
		uint16_set_clean(&devs);
	} else {
		std::istringstream iss(str);

		devid = lexical_cast<uint16_t>(str);

		if (!allow_zero_ && (devid == 0))
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid devid specification: '%s'",
			     str.c_str());

		if (check_valid_ && (devid != 0)) {
			struct arr_config *ac = NULL;
			bool valid = false;

			scoped_ptr_clean<arr_config> arr_config_clean(ac,
			    arr_config_free);
			ac = arr_config_load(isi_error_throw());
			ARR_DEVICE_FOREACH(dppv, dev,
			    arr_config_get_devices(ac)) {
				if (arr_device_get_devid(dev) == devid) {
					valid = true;
					break;
				}
			}
			if (!valid)
			    throw api_exception(AEC_BAD_REQUEST,
				"Invalid devid specification: '%s'",
				    str.c_str());
		}

		if (!all_)
			uint16_set_add(&devs, devid);
	}
}

void
sort_dir_to_arg::operator()(std::string& str, int val)
{
	switch (val) {
	case SORT_DIR_ASC:	       
		str = "ASC";
		break;
	case SORT_DIR_DESC:
		str = "DESC";
		break;
	default:
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid sort dir enum '%d'", val);
	}
}

void
arg_to_sort_dir::operator()(int &val, const std::string &str)
{
	if (str == "ASC")
		val = SORT_DIR_ASC;
	else if (str == "DESC")
		val = SORT_DIR_DESC;
	else
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid field order '%s'", str.c_str());
}

void
arg_validate_sort_dir(int dir)
{
	if (dir == SORT_DIR_ASC || dir == SORT_DIR_DESC)
		return;
	else
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid field order '%d'", dir);
}

void
arg_to_scope::operator()(int &val, const std::string &str)
{
	if (str == "user")
		val = SCOPE_USER;
	else if (str == "default")
		val = SCOPE_DEFAULT;
	else if (str == "effective")
		val = SCOPE_EFFECTIVE;
	else
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid scope '%s'", str.c_str());
}

bool
str_to_bool(const std::string &str)
{
	if (strcasecmp(str.c_str(), "true") == 0 ||
	    strcasecmp(str.c_str(), "1")    == 0 ||
	    strcasecmp(str.c_str(), "yes")  == 0)
		return true;

	if (strcasecmp(str.c_str(), "false") == 0 ||
	    strcasecmp(str.c_str(), "0")     == 0 ||
	    strcasecmp(str.c_str(), "no")   == 0)
		return false;

	throw api_exception(AEC_BAD_REQUEST,
	    "Invalid flag value: %s", str.c_str());
}

tribool
str_to_tribool(const std::string &str)
{
	if (strcasecmp(str.c_str(), "any") == 0 ||
	    strcasecmp(str.c_str(), "all") == 0)
		return tribool::TB_ANY;
	else
		return str_to_bool(str) ? tribool::TB_TRUE : tribool::TB_FALSE;
}

void
arg_validate_begin_end(time_t begin, time_t end)
{
	if (begin > end)
		throw api_exception(AEC_BAD_REQUEST, "begin is after end");
}

std::string
get_ntoken_str(int cred_fd)
{
	struct fmt FMT_INIT_CLEAN(fmt_str);
	native_token *nt = fd_to_ntoken(cred_fd);
	// native_token_fmt handles nt=NULL well
	fmt_print(&fmt_str, "%{}",
	native_token_json_fmt(nt));

	if (nt != NULL)
		ntoken_free(nt);

	return std::string(fmt_string(&fmt_str));
}

namespace {

const std::string OK_CHARS_FOR_EXTERNAL_CALL = 
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-.@/'\"";

} // namespace


Json::Value
make_external_json_call(const std::string cmd)
{
	/*
	 * We only allow certain characters in the command and
	 * we forbid certain character sequences.
	 */
	if ((cmd.find_first_not_of(OK_CHARS_FOR_EXTERNAL_CALL) !=
		std::string::npos) ||
	    (cmd.find("..") != std::string::npos)) {
		ilog(IL_ERR, "Invalid external command: %s", cmd.c_str());
		throw api_exception(AEC_EXCEPTION,
		    "External command contains invalid characters");
	}

	/*
	 * Run the desired command.
	 */
	std::FILE* out = ::popen(cmd.c_str(), "r");

	if (0 == out) {
		safe_strerror error_msg(errno);
		ilog(IL_ERR, "Could not popen command '%s': %s",
		    cmd.c_str(), error_msg.c_str());
		throw api_exception(AEC_EXCEPTION,
		    "Failed to call external command");
	}

	scoped_ptr_clean<FILE> clean(out, ::pclose);

	/*
	 * Read the JSON result from the command.
	 */
	std::string result;
	int c;
	while ((c = std::fgetc(out)) != EOF) {
		result += c;
	}

	if (std::ferror(out)) {
		safe_strerror error_msg(errno);
		ilog(IL_ERR, "Failed to read from command '%s': %s",
		    cmd.c_str(), error_msg.c_str());
		throw api_exception(AEC_EXCEPTION,
		    "Failed while reading from external command");
	}


	int pclose_result = ::pclose(out);
	clean.release();
	if (0 != pclose_result) {
		safe_strerror error_msg(errno);
		ilog(IL_ERR, "Could not close pipe to command '%s': %s",
		    cmd.c_str(), error_msg.c_str());
	}

	/*
	 * Parse the JSON result.
	 */ 
	Json::Value value;
	Json::Reader reader;

	if (!reader.parse(result, value)) {
		ilog(IL_ERR,
		    "Could not parse JSON from command '%s': %s",
		    cmd.c_str(), result.c_str());
		throw api_exception(AEC_EXCEPTION,
		    "External command returned invalid JSON");
	}

	return value;
}

std::vector<std::string>
get_field_names(const field_set fields[], unsigned fields_cnt)
{
	std::vector<std::string> ret;

	for (unsigned i = 0; i < fields_cnt; i++) {
		ret.push_back(fields[i].name);
	}

	return ret;
}

Json::Value
get_item_schema(const Json::Value &collection_schema)
{
	Json::Value response_types = collection_schema;
	if (collection_schema.isMember("type") &&
	    collection_schema["type"].isArray()) {
		response_types = collection_schema["type"];
	}

	// Look through all (both) the sub-objects, one of them is the errors
	// schema the other has the schema for the collection list we want.
	for (Json::ValueIterator it = response_types.begin();
	    it != response_types.end(); it++) {

		Json::Value &sub_object = *it;

		// Find correct "properties" sub-object.
		if (sub_object.isMember("properties")) {
			Json::Value &properties = sub_object["properties"];

			for (Json::ValueIterator it2 = properties.begin();
			    it2 != properties.end(); it2++) {

				std::string key_name = it2.memberName();

				if (key_name != "total" &&
				    key_name != "resume" &&
				    key_name != "errors") {

					// Return the actual collection object
					// list schema.
					Json::Value &list = *it2;
					if (list.isMember("items")) {
						return list["items"];
					}
				}
			}
		}
	}

	throw api_exception(AEC_EXCEPTION, "Item schema not found");
}
