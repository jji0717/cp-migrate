#ifndef __RESPONSE_H__
#define __RESPONSE_H__

#include <string>
#include <map>

#include <jsoncpp/json.h>

#include <isi_rest_server/api_content.h>
#include <isi_rest_server/api_error.h>
#include <isi_rest_server/api_headers.h>
#include <isi_rest_server/request.h>

class response;

/**
 * Abstract class for writing REST responses
 */
class odata_stream {
public:
	odata_stream() {}

	virtual ~odata_stream() {}

	/**
	 * Write len byte of data to the stream
	 * @param buff[IN] - the buffer to receive the data
	 * @param len[IN] - the length of data in bytes to read
	 *                - it is callers responsibility to
	 *                - make sure the buffer has enough space.
	 */
	virtual void write_data(const void *buff, size_t len) = 0;

	/**
	 * Write output from @a fd.
	 *
	 * @param f fd to read from
	 * @param n number of bytes to write or 0 to end of file
	 */
	virtual void write_from_fd(int f, size_t n) = 0;

	/**
	 * Write headers in rsp to the stream
	 */
	virtual void write_header(const response &rsp) = 0;

private:
	/* uncopyable, unassignable */
	odata_stream(const odata_stream &);
	odata_stream &operator=(const odata_stream &);
};

class request;

/** REST output response class.
 *
 * The class handles 4 types of output:
 *  - Output to a string managed by the object. This is appropriate for
 *    smallish amounts of data. Body will be sent at the end of the operation.
 *    See get_body() for access to this string.
 *  - Output to a JSON document. Body will be sent at the end of the operation.
 *    See get_json() for access to this document.
 *  - Streaming output - handler should call write_header() and write_data()
 *    and or write_from_fd().
 *  - Errors set via set_error() - this converts the output to a JSON document
 *    containing our standard error schema. Any string body is lost. If
 *    streaming has already begun, the result is undefined.
 */
class response {
public:
	/** Construct with @a out_strm as data sink */
	inline response(odata_stream &out_strm);

	/** Add error to response.
	 *
	 * @param e error code
	 * @param m error message
	 * @param f field name, or empty if non-applicable.
	 */
	void add_error(api_error_code e, const std::string &m,
	    const std::string &f = "");

	/** Get output json object.
	 *
	 * Converts content type and discards any override body.
	 */
	inline Json::Value &get_json();

	/** Get output body string.
	 *
	 * Be sure to change set content type as well.
	 */
	inline std::string &get_body();

	/** Write @a len bytes data from @abuf.
	 *
	 * Switches response state to STREAMING.
	 */
	void write_data(const void *buf, size_t len);

	/** Write data output from file.
	 *
	 * Switches response state to STREAMING.
	 *
	 * @param f fd to read from
	 * @param n number of bytes to write or 0 to end of file
	 */
	void write_from_fd(int f, size_t n);

	/** Return true if this already has an error */
	inline bool has_error();

	/** Get current status code */
	inline api_status_code get_status() const;

	/** Set current status code */
	inline void set_status(api_status_code c);

	/** Get current content type */
	inline const std::string &get_content_type() const;

	/** Set content type to @a t */
	inline void set_content_type(const std::string &t);

	/** Get current output headers */
	inline api_header_map &get_header_map();
	inline const api_header_map &get_header_map() const;

	/** Set content length header, useful for HEAD */
	void set_content_length(unsigned long len);

	/** Return body size for inline outout.
	 *
	 * @pre streaming output has not been started
	 */
	unsigned long get_body_size() const;

	/** Set post location header given @a input path and created @a id */
	void set_post_location(const request &input, const std::string &id);

	/** Send response if not yet sent */
	void send(request::method method=request::UNSUPPORTED);

	/* Returns information about response for auditing purposes */
	char *audit_response();

	/** Determine if this is a streaming response. **/
	bool is_streaming() { return state_ == STREAMING; }

private:
	void set_json_output();

	void set_str_output();

	enum state {
		JSON,
		STRING,
		STREAMING,
		SENT,
	};

	api_status_code	     status_;
	std::string          content_type_;
	state                state_;
	bool                 has_error_;
	std::string          body_str_;
	Json::Value          json_obj_;
	odata_stream        &out_strm_;
	api_header_map	     header_map_;
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

response::response(odata_stream &out_strm)
	: status_(ASC_OK), content_type_(api_content::TEXT_PLAIN)
	, state_(STRING), has_error_(false), out_strm_(out_strm)
{ }

Json::Value &
response::get_json()
{
	if (state_ != JSON)
		set_json_output();
	return json_obj_;
}

std::string &
response::get_body()
{
	if (state_ != STRING)
		set_str_output();
	return body_str_;
}

bool
response::has_error()
{
	return has_error_;
}

api_status_code
response::get_status() const
{
	return status_;
}

void
response::set_status(api_status_code c)
{
	status_ = c;
}

const std::string &
response::get_content_type() const
{
	return content_type_;
}

void
response::set_content_type(const std::string &t)
{
	content_type_ = t;
}

api_header_map &
response::get_header_map()
{
	return header_map_;
}

const api_header_map &
response::get_header_map() const
{
	return header_map_;
}

#endif
