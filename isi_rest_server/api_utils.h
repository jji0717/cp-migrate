#ifndef INC_API_UTILS_H_
#define INC_API_UTILS_H_

#include <sstream>
#include <string>

#include <ifs/ifs_syscalls.h>

#include <jsoncpp/json.h>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/scoped_clean.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_util_cpp/user_text.h>
#include <isi_upgrade_api/isi_upgrade_api.h>

#include "api_error.h"
#include "request.h"

/** True if this is a release build */
extern const bool g_release;

/** False if this is a release build */
extern const bool g_debug;

/* common query args used by many handlers */
extern const char *SORT_QUERY_ARG;
extern const char *DIR_QUERY_ARG;
extern const char *LIMIT_QUERY_ARG;
extern const char *RESUME_QUERY_ARG;
extern const char *SCOPE_QUERY_ARG;

extern const std::string ARG_SCOPE;
extern const std::string ARG_RESUME;

/* string "@DEFAULT" used to indicate value should be returned to default
 * tracking */
extern const char *VAL_AT_DEFAULT;

/*
 * This token version is used by the standard resume token format which only
 * contains a query arg json object and a last item json object.  If this
 * version is found in a token, the token will be automatically validated.
 */
extern const unsigned VALIDATED_RESUME_TOKEN_VERSION;

/* MACRO for JSON Validation checks */
#define DEF_VALUE_CHECK(_func, _method, _user_text)		\
inline void							\
_func(const Json::Value &j_input, const char *key,			\
	const char *user_text=_user_text)			\
{								\
	if (!j_input[key]._method())				\
	{							\
		throw api_exception(AEC_BAD_REQUEST,		\
			get_user_text(_user_text), key);	\
	}							\
}

/* MACRO for JSON required keys and their validation */
#define DEF_REQ_VALUE_CHECK(_func, _check_func, _user_text)		 \
inline void								 \
_func(const Json::Value &j_input, const char *key,				 \
	const char *user_text=_user_text)				 \
{									 \
	/*								 \
	 * TODO: print the path				 \
	 */								 \
	mandatory_value_check(j_input, key, user_text);			 \
	_check_func(j_input, key, user_text);				 \
}

inline Json::Value
non_empty_or_null(const std::string &s)
{
	if (s.empty())
		return Json::nullValue;
	else
		return Json::Value(s);
}

template<typename T>
inline Json::Value
non_zero_or_null(const T &t)
{
	if (t == 0)
		return Json::nullValue;
	else
		return Json::Value(t);
}

struct field_set {
	int         val;
	const char *name;
};

enum sort_dir {
	SORT_DIR_ASC,
	SORT_DIR_DESC
};

enum settings_scope {
	SCOPE_USER,
	SCOPE_DEFAULT,
	SCOPE_EFFECTIVE
};

struct tribool {
	enum value {
		TB_FALSE = 0,
		TB_TRUE  = 1,
		TB_ANY
	};

	tribool(value v = TB_FALSE) : value_(v) { }

	bool operator==(value v) const { return v == value_; }

	bool operator!=(value v) const { return !operator==(v); }

	value value_;
};

inline std::ostream &operator<<(std::ostream &o, const tribool &t)
{
	return o << (unsigned)t.value_;
}

inline std::istream &operator>>(std::istream &i, tribool &t)
{
	return i >> (unsigned&)t.value_;
}


template<typename T>
inline std::string
to_str(const T &val)
{
	std::ostringstream oss;
	oss << val;
	return oss.str();
}

template<typename T>
T
lexical_cast(const std::string &str)
{
	T ret;
	std::istringstream iss(str);
	iss >> ret;
	if (!iss)
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid String: \"%s\"", str.c_str());
	if (iss.peek() != EOF)
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid String: \"%s\"", str.c_str());

	return ret;
}

inline std::string
lexical_cast(const std::string &str)
{
	return str;
}

/** quote @a str for use with shell invocation (wraps shquote(1)) */
std::string quote_shell_args(const std::string &str);

/** Convert string to bool, @throws on non-convertible string */
bool str_to_bool(const std::string &str);

/** Convert string to tribool, @throws on non-convertible string */
tribool str_to_tribool(const std::string &str);

request::arg_iter_pair
find_args_helper(const request::arg_map &, const std::string &, bool, bool);

/** Find and process arguments.
 *
 * @param data destination argument data
 * @param func on-argument data setting function
 * @param args source argument map
 * @param name argument name to find
 * @param req  true if required
 * @param mult true if multiple allowed
 *
 * @return true if arguments found.
 * @throw api_exception if arguments not found and required or if more than one
 * found and multiple are not allowed.
 */
template<class Data, class Handler>
bool
find_args(Data &data, Handler func,
    const request::arg_map &args, const std::string &name,
    bool req = false, bool mult = false)
{
	request::arg_iter_pair range = find_args_helper(args, name, req, mult);

	for (request::arg_iter it = range.first; it != range.second; ++it)
		func(data, it->second);

	return range.first != range.second;
}

template<class Data>
struct arg_to_generic;

template<class Data>
bool
find_args(Data &data,
    const request::arg_map &args, const std::string &name,
    bool req = false, bool mult = false)
{
	arg_to_generic<Data> handler;
	return find_args(data, handler, args, name, req, mult);
}

bool
find_resume_arg(std::string &str, const request::arg_map &args,
    const std::string *exclusive_args[]);

template<class Data>
struct arg_to_generic {

	void operator()(Data &val, const std::string &str)
	{
		val = lexical_cast<Data>(str);
	}
};

template<>
struct arg_to_generic<std::string> {
	void operator()(std::string &val, const std::string &str)
	{
		val = str;
	}
};

template<>
struct arg_to_generic< std::vector<int> > {
	void operator()(std::vector<int> &val, const std::string &str)
	{
		int temp = atoi(str.c_str());
		val.push_back(temp);
	}
};

template<>
struct arg_to_generic<bool> {
	void operator()(bool &val, const std::string &str)
	{
		val = str_to_bool(str);
	}
};

template<>
struct arg_to_generic<tribool> {
	void operator()(tribool &val, const std::string &str)
	{
		val = str_to_tribool(str);
	}
};

template<class Data>
struct arg_to_bounded_generic{

	// requires a defined operator<() for 'Data' 

	arg_to_bounded_generic(const Data &begin, const Data &end)
	    : begin_(begin), end_(end)
	{ }

	void operator()(Data &val, const std::string &str)
	{
		val = lexical_cast<Data>(str);

		if (val < begin_ ||  end_ < val)
			throw api_exception(AEC_BAD_REQUEST,
			    "invalid parameter value: %s", str.c_str());
	}

	Data begin_;
	Data end_;
};

struct arg_to_time {
	arg_to_time(bool no_neg = false)
	    : no_neg_(no_neg)
	{ }
	
	void operator()(time_t &val, const std::string &str);

	bool no_neg_;
};

struct arg_to_field {
	arg_to_field(const field_set *fields, unsigned cnt)
	    : fields_(fields), cnt_(cnt)
	{ }

	const field_set *fields_;
	const unsigned   cnt_;

	void operator()(int &val, const std::string &str) const;

	void operator()(unsigned &val, const std::string &str) const
	{
		operator()((int &)val, str);
	}

	unsigned operator()(const std::string &str) const
	{
		unsigned ret;
		operator()(ret, str);
		return ret;
	}

	std::string operator()(int val) const;
};

struct arg_validate_field {
	arg_validate_field(const field_set *fields, unsigned cnt)
	    : fields_(fields), cnt_(cnt)
	{ }

	const field_set *fields_;
	const unsigned   cnt_;

	void operator()(const std::string &str) const;

	void operator()(int val) const;
};

struct uint16_set;

struct arg_to_devs {
	arg_to_devs(bool allow_zero = true, bool check_valid = true);
	void operator()(uint16_set &devs, const std::string &str);

	bool all_;
	bool allow_zero_;
	bool check_valid_;
};

struct arg_to_flag {
	arg_to_flag(int flag)
	    : flag_(flag)
	{ }

	int flag_;

	/* requires default copy, assign */

	void operator()(int &flags, const std::string &str)
	{
		if (str_to_bool(str))
			flags |= flag_;
		else
			flags &= ~flag_;
	}
};

struct arg_to_bool {
	void operator()(bool &val, const std::string &str)
	{
		val = str_to_bool(str);
	}
};

struct arg_to_sort_dir {
	void operator()(int &val, const std::string &str);
};

struct sort_dir_to_arg {
	void operator()(std::string& str, int val);
};

struct arg_to_scope {
	void operator()(int &val, const std::string &str);
};


// validate begin <= end
void arg_validate_begin_end(time_t begin, time_t end);

void arg_validate_sort_dir(int dir);

/**
 * return native token string for given user's credential descriptor
 * @a cred_fd
 */
std::string
get_ntoken_str(int cred_fd);

/**
 * Call to an external program that writes JSON to its standard
 * output.
 *
 * @a cmd full command to run
 */
Json::Value
make_external_json_call(const std::string cmd);

std::vector<std::string>
get_field_names(const field_set fields[], unsigned fields_cnt);

Json::Value get_item_schema(const Json::Value &collection_schema);

/* JSON Validation Checks */

/* 
 * TODO: better if these checks are possible inside jsoncpp's asBool, asInt and
 * other as* methods.
 */

DEF_VALUE_CHECK(array_value_check, isArray, JSON_INPUT_x_ARRAY_VALUE)
DEF_VALUE_CHECK(object_value_check, isObject, JSON_INPUT_x_OBJECT_VALUE)
DEF_VALUE_CHECK(string_value_check, isString, JSON_INPUT_x_STRING_VALUE)
DEF_VALUE_CHECK(bool_value_check, isBool, JSON_INPUT_x_BOOL_VALUE)
DEF_VALUE_CHECK(int_value_check, isInt, JSON_INPUT_x_INT_VALUE)

inline void
non_empty_array_value_check(const Json::Value &j_input, const char *key,
	const char *user_text=JSON_INPUT_x_NON_EMPTY_ARRAY_VALUE)
{
	array_value_check(j_input, key, user_text);
	if (j_input[key].empty())
	{
		throw api_exception(AEC_BAD_REQUEST, get_user_text(user_text),
			key);
	}
}

inline void
mandatory_value_check(const Json::Value &j_input, const char *key,
	const char *user_text=JSON_INPUT_x_REQUIRED)
{
	/*
	 * TODO: print the path
	 */
	ilog(IL_DEBUG, "checking if '%s' exists in JSON input", key);
	/* 
	 * TODO: even if the API input was not a valid json, the daemon reaches
	 * here and crashes due to an assertion in jsoncpp. This should not
	 * happen. Maybe enclose the code below in a try-catch block or
	 * validate JSON even before reaching here.
	 */
	if (!j_input.isMember(key))
	{
		throw api_exception(AEC_ARG_REQUIRED, get_user_text(user_text),
			key);
	}
}

/*
 * Rolling upgrade helper macro
 */
#define ROLLING_UPGRADE_REQUIRE_FEATURE(FEATURE) \
    do { \
        if (!(ISI_UPGRADE_API_DISK_ENABLED(FEATURE))) \
	    throw api_exception(AEC_CONFLICT, \
	        get_user_text(ROLLING_UPGRADE_CONFLICT)); \
    } while (0)

/* NOTE: there can be DEF_NON_EMPTY_CHECK as well if required */
DEF_REQ_VALUE_CHECK(mandatory_bool_check, bool_value_check,
	JSON_INPUT_x_REQUIRED_BOOL_VALUE)
DEF_REQ_VALUE_CHECK(mandatory_string_check, string_value_check,
	JSON_INPUT_x_REQUIRED_STRING_VALUE)
DEF_REQ_VALUE_CHECK(mandatory_int_check, int_value_check,
	JSON_INPUT_x_REQUIRED_INT_VALUE)
DEF_REQ_VALUE_CHECK(mandatory_object_check, object_value_check,
	JSON_INPUT_x_REQUIRED_OBJECT_VALUE)
DEF_REQ_VALUE_CHECK(mandatory_array_check, array_value_check,
	JSON_INPUT_x_REQUIRED_ARRAY_VALUE)
DEF_REQ_VALUE_CHECK(mandatory_non_empty_array_check,
	non_empty_array_value_check, JSON_INPUT_x_REQUIRED_NON_EMPTY_ARRAY)

#endif

