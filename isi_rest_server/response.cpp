#include <isi_ilog/ilog.h>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/stream.hpp>

#include "request.h"
#include "response.h"

const std::string ERROR_CODE_STR    = "code";
const std::string ERROR_MESSAGE_STR = "message";
const std::string ERROR_ARRAY_STR   = "errors";
const std::string ERROR_FIELD_STR   = "field";

namespace {

/**
 * This is a wrapper around the odata_stream to allow it to be
 * used as a std::ostream (needed by the streaming JSON writer).
 */
class json_sink : public boost::iostreams::sink {
public:
	json_sink(odata_stream& out) : out_(out), total_chars_(0) {}

	std::streamsize write(const char* s, std::streamsize n)
	{
		out_.write_data(s, n);
		total_chars_ += n;
		return n;
	}

	unsigned long get_total_chars() { return total_chars_; }

private:
	odata_stream& out_;
	unsigned long total_chars_;
};

} // namespace

void
response::add_error(api_error_code ec, const std::string &msg,
    const std::string &field)
{
	// If it is in a streaming state, the status is already
	// written, it is too late to write status and set json
	// output.
	if (state_ == STREAMING || state_ == SENT)
		return;

	const api_error_table::tab_entry &err_ent =
	    api_error_table::get_entry(ec);

	status_ = std::max(status_, err_ent.sc);

	Json::Value &obj = get_json();

	if (!has_error_) {
		has_error_ = true;
		obj = Json::Value(Json::objectValue);
		obj[ERROR_ARRAY_STR] = Json::Value(Json::arrayValue);
	}

	Json::Value &err = obj[ERROR_ARRAY_STR].append(
	    Json::Value(Json::objectValue));

	if (!field.empty())
		err[ERROR_FIELD_STR] = field;
	err[ERROR_MESSAGE_STR] = msg;
	err[ERROR_CODE_STR] = err_ent.symbol;
}

void
response::set_content_length(unsigned long len)
{
	char buf[64];
	sprintf(buf, "%lu", len);
	header_map_[api_header::CONTENT_LENGTH] = buf;
}

void
response::set_post_location(const request &input, const std::string &id)
{
	std::string loc_path;

	loc_path.reserve(input.get_uri().size() + 1 + id.size());

	loc_path += input.get_uri();
	loc_path += '/';
	loc_path += id;

	header_map_[api_header::LOCATION] = loc_path;
}

unsigned long
response::get_body_size() const
{
	ASSERT(state_ != STREAMING);

	/* XXX +2 for trailing \r\n ? */

	if (state_ == STRING)
		return body_str_.size();
	else {
		/*
		 * XXX json has a stream output interface
		 * which could be a little better here
		 */
		Json::FastWriter writer;
		const std::string body = writer.write(json_obj_);
		return body.size();
	}
}

void
response::set_json_output()
{
	ASSERT(state_ != STREAMING && state_ != SENT);

	content_type_ = api_content::APPLICATION_JSON;
	body_str_.clear();
	state_ = JSON;
}

void
response::set_str_output()
{
	ASSERT(state_ != STREAMING && state_ != SENT);

	// XXX here, everywhere - error state?

	state_ = STRING;
}

void
response::write_data(const void *buf, size_t len)
{
	ASSERT(state_ != SENT);

	if (state_ == JSON || state_ == STRING)
		out_strm_.write_header(*this);

	state_ = STREAMING;

	out_strm_.write_data(buf, len);
}

void
response::write_from_fd(int fd, size_t len)
{
	ASSERT(state_ != SENT);

	if (state_ == JSON || state_ == STRING)
		out_strm_.write_header(*this);

	state_ = STREAMING;

	out_strm_.write_from_fd(fd, len);
}

void
response::send(request::method method)
{
	ASSERT(state_ != SENT);
	std::string log_body;

	// Figure out if we will log the response
	bool will_log_body = false;
	ILOG_START(IL_REST_BODY | IL_REST_READS) {
		if (ILOG_MATCH_LEVEL & IL_REST_BODY) {
			if ((method == request::POST) ||
			    ((ILOG_MATCH_LEVEL & IL_REST_READS) &&
			    (method == request::GET))) {
				will_log_body = true;
			}
		}
	} ILOG_END;

	if (state_ == STREAMING)
		return;

	out_strm_.write_header(*this);

	if (state_ == STRING) {
		if (will_log_body)
			log_body = body_str_;
		out_strm_.write_data(body_str_.data(), body_str_.size());
		if (body_str_.size() > 0)
			out_strm_.write_data("\r\n", 2);
	} else {
		/*
		 * XXX json has a stream output interface
		 * which could be a little better here
		 */
		Json::FastWriter dbfw;
		if (will_log_body)
			log_body = dbfw.write(json_obj_);

		Json::StyledStreamWriter writer("");
		json_sink sink(out_strm_);
		boost::iostreams::stream<json_sink> out(sink);

		writer.write(out, json_obj_);

		if (sink.get_total_chars() != 0) {
			out_strm_.write_data("\r\n", 2);
		}
	}

	if (will_log_body) {
		// Log the response if needed
		ILOG_START(IL_REST_BODY | IL_REST_READS) {
			fmt_print(ilogfmt, "response: %s", log_body.c_str());
		} ILOG_END;
	}

	state_ = SENT;
}

char *
response::audit_response()
{
	struct fmt fmt = FMT_INITIALIZER;

	fmt_print(&fmt, "{");
	fmt_print(&fmt, "\"status\":%d,\"statusmsg\":\"%s\"", status_, api_status_code_str(status_));
	if (state_ == STREAMING) {
		/* skip this */
	} else if (state_ == STRING) {
		const std::string &body = body_str_;
		const char *cbody = body.c_str();
		fmt_print(&fmt, ",\"body\":%s", (!cbody || !(*cbody)) ? "{}" : cbody);
	} else {
		Json::FastWriter writer;
		const std::string &body = writer.write(json_obj_);
		const char *cbody = body.c_str();
		fmt_print(&fmt, ",\"body\":%s", (!cbody || !(*cbody)) ? "{}" : cbody);
	}
	fmt_print(&fmt, "}");
	char *s = fmt_detach(&fmt);
	fmt_clean(&fmt);
	return s;
}
