#include "fastcgi_soa_handler.h"

#include "api_content.h"
#include "api_headers.h"

#include <boost/algorithm/string.hpp>

#include <isi_rest_client/rest_client.h>
#include <isi_ilog/ilog.h>

// FastCGI SOA Class

fastcgi_soa_handler::fastcgi_soa_handler(int priv,
    const std::string &socket_path) : soa_handler(priv),
    socket_path_(socket_path)
{ }

fastcgi_soa_handler::fastcgi_soa_handler(const priv_filter &pf,
    const std::string &socket_path) : soa_handler(pf),
    socket_path_(socket_path)
{ }

static rest_method_t
convert_method(request::method in)
{
	rest_method_t out = UNSUPPORTED;

	switch(in) {
		case request::GET:
			out = GET;
			break;
		case request::PUT:
			out = PUT;
			break;
		case request::POST:
			out = POST;
			break;
		case request::DELETE:
			out = DELETE;
			break;
		case request::HEAD:
			out = HEAD;
			break;
		default:
			break;
	}

	return out;
}

void
fastcgi_soa_handler::call_service(const request &input, response &output)
{
	const struct timeval tv = { DEFAULT_REST_CLIENT_TIMEOUT, 0 };
	rest_request_t *request = NULL;
	rest_response_t *response = NULL;

	const char *in_body = "";
	request::method method = input.get_method();

	if (method == request::PUT ||
	    method == request::POST ||
	    method == request::DELETE) {
		in_body = input.get_body().c_str();
	}

	request = rest_request_new(socket_path_.c_str(),
	    convert_method(input.get_method()), in_body);

	// URI Tokens
	ilog(IL_DEBUG, "Request uri: %s", input.get_uri().c_str());
	std::vector<std::string> tokens;
	boost::split(tokens, input.get_uri(), boost::is_any_of("/"));

	for (std::vector<std::string>::iterator it = tokens.begin();
	    it != tokens.end(); ++it) {
		ilog(IL_DEBUG, "Request uri token: %s", it->c_str());
		rest_request_add_uri_token(request, it->c_str());
	}

	// Query Args
	const request::arg_map &args = input.get_arg_map();

	for (request::arg_iter it = args.begin(); it != args.end(); ++it) {
		ilog(IL_DEBUG, "Request query arg: %s=%s", it->first.c_str(),
		    it->second.c_str());
		rest_request_add_query_arg(request, it->first.c_str(),
		    it->second.c_str());
	}

	// Headers
	const api_header_map &in_headers = input.get_header_map();

	for (api_header_map::const_iterator it = in_headers.begin();
	    it != in_headers.end(); ++it) {
		ilog(IL_DEBUG, "Request header: %s=%s", it->first.c_str(),
		    it->second.c_str());
		rest_request_add_http_header(request, it->first.c_str(),
		    it->second.c_str(), api_error_throw());
	}

	// Remote Call
	ilog(IL_DEBUG, "Calling remote service");
	response = rest_request_execute(request, &tv, api_error_throw());

	// Response Headers
	api_header_map &out_headers = output.get_header_map();

	std::string content_type = api_content::TEXT_PLAIN;

	const struct isi_kventry* entry;
	ISI_HASH_FOREACH(entry, &(response->http_headers->hash), hash_entry) {
		if (entry->data && entry->value) {
			if (api_header::CONTENT_TYPE == entry->data) {
				content_type = entry->value;
			} else if (api_header::STATUS != entry->data) {
				ilog(IL_DEBUG, "Response header: %s=%s",
				    entry->data, entry->value);
				out_headers[entry->data] = entry->value;
			}
		}
	}

	// Content Type
	ilog(IL_DEBUG, "Response content type: %s", content_type.c_str());
	output.set_content_type(content_type);

	// Status Code
	ilog(IL_DEBUG, "Response code: %d", (int) response->status);
	output.set_status((api_status_code) response->status);

	// Response Body
	ilog(IL_DEBUG, "Response body: %s", response->body);
	output.get_body() = response->body;

	// Free
	rest_request_free(&request);
	rest_response_free(&response);
}

