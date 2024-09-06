#ifndef __REQUEST_H__
#define __REQUEST_H__

#include <string>
#include <map>
#include <vector>
#include <set>

#include <jsoncpp/json.h>

#include <sys/types.h>

#include <isi_rest_server/api_headers.h>
#include <isi_rest_server/uri_tokens.h>
#include <isi_rest_server/method_stats.h>

// The following munged HTTP headers are needed until
// we unmunge it in fcgi_helpers.cpp
#define HDR_CONTENT_DISPOSITION	"CONTENT_DISPOSITION"
#define HDR_CONTENT_ENCODING	"CONTENT_ENCODING"
#define HDR_REMOTE_USER		"REMOTE_USER"
#define HDR_ACCEPT_ENCODING	"ACCEPT_ENCODING"

#define HDR_IF_MODIFIED_SINCE	"IF_MODIFIED_SINCE"
#define HDR_IF_UNMODIFIED_SINCE	"IF_UNMODIFIED_SINCE"
#define HDR_IF_MATCH		"IF_MATCH"
#define HDR_IF_NONE_MATCH	"IF_NONE_MATCH"
#define HDR_RANGE		"RANGE"
#define RANGE_UNIT		"bytes="

/** Abstract class supporting reading body over stream */
class idata_stream {
public:
	idata_stream() {}

	virtual ~idata_stream() {}

	/**
	 * Read len byte of data from the stream into buff
	 * @param buff[OUT] - the buffer to receive the data
	 * @param len[IN] - the length of data in bytes to read
	 *                - it is callers responsibility to
	 *                - make sure the buffer has enough space.
	 * @return the length of data read, -1 indicates error
	 */
	virtual size_t read(void *buff, size_t len) = 0;

	// XXX add read_to_fd()

	/**
	 * Get error information in case of error
	 */
	virtual void get_error(std::string &error_info) = 0;

private:
	// not implemented:
	idata_stream(const idata_stream & other);
	idata_stream &operator= (const idata_stream &other);
};

/**
 * Modelling accept encoding
 */
struct accept_encoding {
	std::string coding_;
	float qvalue_;

	/**
	 * Construct accept_encoding object.
	 * @pre coding should not be empty.
	 * @pre qvalue should between 0~1.0
	 */
	accept_encoding(const std::string &coding, const float &qvalue);
};

/** REST request object.
 *
 * Uses http concepts, but we're not limited to http as a transport
 * mechanism. Includes 5 parts:
 *
 *   uri       - specification of resource path
 *   method    - action to be taken on resource
 *   arguments - further refinement of read action
 *   body      - input for creation and modification actions
 *   headers   - further detail about request from client
 */
class request {
public:
	typedef std::multimap<std::string, std::string> arg_map;
	typedef arg_map::const_iterator                 arg_iter;
	typedef std::pair<arg_iter, arg_iter>           arg_iter_pair;

	/** Methods corresponding to http methods we care about */
	enum method {
		UNSUPPORTED = -1,   // unsupported
		GET,                // read
		PUT,                // modify
		POST,               // create
		DELETE,             // delete
		HEAD,               // get meta data only
		NUM_METHODS         // sentinel
	};

	/** Return string form of @a method */
	static const char *string_from_method(unsigned method);

	/** Return method type from string @a name */
	static method method_from_string(const std::string &name);

	/** Tokenize string.
	 *
	 * @param s input string
	 * @param d delimiters to break tokens on
	 * @param i include tokens
	 */
	static std::vector<std::string>
	tokenize(const std::string &s, const std::string &d, bool i = true);

	/** Construct request.
	 *
	 * @param base_url Base url string (e.g. "https://host:port/platform")
	 * @param ns Namespace. (e.g. platform, object, namespace)
	 * @param uri URI path (e.g. /1/foo/bar")
	 * @param http_method Method name
	 * @param args URI query arguments string
	 * @param hdrs Input headers
	 * @param content_len Input body length (content-length)
	 * @param user_token_fd User token fd
	 * @param remote_addr Remote address
	 * @param strm Body input stream handler
	 * @param internal true if the request was created by an internal
	 * daemon like the PAPI itself to send to another node. Must only be
	 * set by the papi_d
	 */
	request(const std::string &base_url, const::std::string &ns,
	    const std::string &uri, const std::string &http_method,
	    const std::string &args, const api_header_map &hdrs,
	    size_t content_len, int user_token_fd,
	    const std::string &remote_addr, idata_stream &strm,
	    bool internal=false);

	/** Get user token fd */
	inline int get_user_token() const;

	/** Get base URL string */
	inline const std::string &get_base_url() const;

	/**
	 * Trim the source str of the white spaces
	 * return the result in the destination string
	 */
	static void trim(const std::string &src, std::string &dst);

	/** Get Namespace */
	inline const std::string &get_namespace() const;

	/** Get request URI - this is component-wise percent encoded  */
	inline const std::string &get_uri() const;

        /** set uri_ */
        inline void set_uri (const std::string& newUri);

	/** Return positional URI request token for @a name.
	 *
	 * @throw isi_exception if not found (mapping, handler disagreement).
	 */
	const std::string &get_token(const std::string &name) const;

	/** Return method type */
	inline method get_method() const;

	/** Return query arguments map */
	inline const arg_map &get_arg_map() const;

        /** Return query arguments map, including arguments from any any
	 * resume token found.
	 */
	arg_map get_arg_map_including_resume() const;

	/** Return the main argument: the first argument */
	inline const std::string &get_main_arg() const;

	/** Remove a query argument from the arg map */
        void remove_query_arg(const std::string &arg);

	/** Return header map */
	inline const api_header_map &get_header_map() const;

	/** Get the value from the input for the given header.
	 *
	 * @param header the name of the HTTP header
	 * @param val the value of the header, if the header is found.
	 * @return - true if the header is found, false if not.
	 */
	bool get_header(const std::string &header, std::string &val) const;

	/** Return input content/body length */
	inline const uint64_t content_length() const;

	/** Return the accept encodings from the input.
	 *
	 * @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.3
	 * The encodings are sorted by preference.
	 * @return true if the accept encoding passes validation.
	 */
	bool get_accept_encodings(std::vector<accept_encoding> &out) const;

	/** @{ Function for dealing with request body */

	/** Return input body as string.
	 *
	 * @pre get_method() == POST or PUT
	 * @pre stream reading has not started
	 *
	 * @throw api_exception AEC_BAD_REQUEST on;
	 *   - failure reading from source or short read
	 */
	inline const std::string &get_body() const;

	/** Return input body as a parsed Json object.
	 *
	 * @pre get_method() == POST or PUT
	 * @pre stream reading has not started
	 *
	 * @throw api_exception AEC_BAD_REQUEST on:
	 *   - failure reading from source or short read
	 *   - invalid json
	 *   - invalid content type
	 */
	inline const Json::Value &get_json() const;

	/** Read body directly.
	 *
	 * Sets body state to streaming after which the get_body() and
	 * get_json() methods cannot be used.
	 *
	 * @return amount of data read
	 */
	size_t read_body(void *buff, size_t len) const;

	/** Return any error information that occurred during reading */
	void get_error(std::string &error_info) const;

	/** @} */

	/** Return positional URI tokens map for modification.
	 *
	 * Semi-private - used by uri_manager
	 */
	inline uri_tokens &tokens();

	/** Return constant map of URI tokens.
	 *
	 * @return constant map of URI tokens
	 */
	inline const uri_tokens &tokens() const;

	/**
	 * Get the value from the input for the given argument.
	 * The query parameter is written as ?query
	 * where query is [<parameter>=<value>][&<parameter>=<value>]...
	 * @param param[IN] - the name of the query parameter
	 * @param val[OUT] - the value of the parameter, if the parameter is found.
	 * @return - true if the parameter is found, false if not.
	 */
	bool get_arg(const char *param, std::string &val) const;

	/**
	 * Get a representation of the query args as a json map.
	 * @param schema - The schema to vaidate with.  This will determine
	 * what types to cast query args to.
	 */
	Json::Value get_args_as_json(const Json::Value &schema) const;

	/**
	 * This function returns a json representation of the query args,
	 * including, args that have been stored in a resume token from a
	 * previous call.  This only works for resume tokens using
	 * VALIDATED_RESUME_TOKEN_VERSION.
	 */
	Json::Value get_args_and_resume_as_json(const Json::Value &schema)
	    const;

	/**
	 * This function returns the zone id of the request.
	 */
	zid_t zid() const;

	/**
	 * Logs information about this request through ilog.
	 */
	void log_request(bool error = false);

	/**
	 * Logs audit information about this request through syslog.
	 */
	void log_request_for_audit();

	/**
	 * Retrieve request id
	 */
	unsigned get_request_id() const;

	/**
	 * Is this an internal request
	 */
	bool get_internal() const;

	/**
	 * Returns information about request for auditing purposes.
	 */
	char *audit_request() const;

	/**
	 * Assign a new method_timer object to use with get_time_passed().
	 * Creation/deletion of the method_timer should be handled by the
	 * caller.
	 */
	void set_timer(method_timer *new_timer);

	/**
	 * Passthrough to the method_timer.log_timing_checkpoint() method to
	 * do request time logging.  Does nothing if no timer was set.
	 */
	void
	log_timing_checkpoint(const char *label, int level = IL_DEBUG) const;

private:
	void read_str_input() const;

	void read_json_input() const;

	enum state {
		NONE,
		BODY_LOADED,
		JSON_LOADED,
		STREAMING,
	};

	/**
	 * Set of special case query arguments that we want to ignore (not load
	 * into the request object) if they are blank and have no actual value.
	 */
	const static char *ignore_if_blank_init[];
	static std::set<std::string> ignore_if_blank;

	const std::string    base_url_;
	const std::string    method_str_;
	const method         method_;
	const std::string    arg_str_;
	const api_header_map header_map_;
	const size_t         content_length_;
	const int            user_token_fd_;
	const std::string    remote_addr_;
	const std::string    namespace_;
	const unsigned       request_id_;

	std::string          uri_;
	mutable std::string  body_;
	mutable Json::Value  json_obj_;
	mutable state        state_;

	std::string          main_arg_;
	arg_map              arg_map_;
	uri_tokens           tokens_;

	idata_stream        &strm_;
	bool                 logged_;

	bool                 internal_;
	static unsigned next_request_id;
	static unsigned get_next_request_id();

	method_timer         *timer_;

	/* uncopyable, unassignable */
	request(const request &);
	request &operator=(const request &);
};

/** Return true if @a method requires input */
inline bool method_has_input(int method);

/** Return true if we expect output for @a method */
inline bool method_has_output(int method);

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

int
request::get_user_token() const
{
	return user_token_fd_;
}

const std::string &
request::get_base_url() const
{
	return base_url_;
}

uri_tokens &
request::tokens()
{
	return tokens_;
}

const uri_tokens &
request::tokens() const
{
	return tokens_;
}

request::method
request::get_method() const
{
	return method_;
}

const request::arg_map &
request::get_arg_map() const
{
	return arg_map_;
}

const api_header_map &
request::get_header_map() const
{
	return header_map_;
}

const std::string &
request::get_main_arg() const
{
	return main_arg_;
}

const std::string &
request::get_body() const
{
	if (state_ != BODY_LOADED && state_ != JSON_LOADED)
		read_str_input();
	return body_;
}

const Json::Value &
request::get_json() const
{
	if (state_ != JSON_LOADED)
		read_json_input();
	return json_obj_;
}

const std::string &
request::get_uri() const
{
	return uri_;
}

void request::set_uri(const std::string& newUri)
{
    uri_ = newUri;
}

const std::string &
request::get_namespace() const
{
	return namespace_;
}

const uint64_t
request::content_length() const
{
	return content_length_;
}

bool
method_has_input(int method)
{
	switch (method) {
	case request::PUT:
	case request::POST:
		return true;

	case request::GET:
	case request::HEAD:
	case request::DELETE:
	default:
		return false;
	}
}

bool
method_has_output(int method)
{
	switch (method) {
	case request::POST:
	case request::GET:
		return true;

	default:
	case request::PUT:
	case request::DELETE:
	case request::HEAD:
		return false;
	}
}

#endif
