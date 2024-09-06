#include "handler_common.h"
#include "oapi_mapper.h"
#include "test_request_helper.h"

#include <check.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_rest_server/request_thread.h>

#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

const static std::string MTHD_PUT("PUT");
const static std::string MTHD_POST("POST");
const static std::string namespace_base("https://localhost:8080/namespace/");
const static std::string object_base("https://localhost:8080/object/");

void
test_request_helper::test_send_request_ext(const std::string &method,
    const std::string &uri, const std::string &store_type,
    const std::string &args, const api_header_map &headers,
    size_t content_len, int user_fd, const std::string &remote_addr,
    idata_stream &istrm, response &output)
{
	std::string base; 

	/* 
	 * Determine of this id for NS or Object
	 * Add the base and the header of PUT or POST
	 */
	if (store_type == NS_OBJECT) {
		base = object_base;
	} else {
		base = namespace_base;
	}

	/* 
	 * Create the request and response
	 */
	request input(base, store_type, uri, method, args, headers,
	    content_len, user_fd, remote_addr, istrm);

	/* 
	 * Execute the request
	 */
	uri_manager mgr(oapi_mapper);
	mgr.execute_request(input, output);

}

void
test_request_helper::test_send_request(const std::string &method,
    const std::string &uri, const std::string &store_type,
    const std::string &args, const api_header_map &headers,
    response &output)
{
	/*
	 * parameters needed for request construction
	 */
	test_istream istrm;
	int user_fd = 0;

	/*
	 * Get credentials
	 */
	user_fd = gettcred(NULL, 0);
	fail_if(user_fd <= 0, "Failed to get the credential.");

	test_send_request_ext(method, uri, store_type, args, headers,
	    0, user_fd, "127.0.0.1", istrm, output);

	/* 
	 * Close the fd object
	 */
	close(user_fd);
}

std::string
test_request_helper::get_remote_ip( )
{
	char cmd[64] = "isi_nodes \%{ipv6_address}";
	char buffer[128] = {0};
	std::string remote_ip = "127.0.0.1";
	FILE *pipe = NULL;
	struct in6_addr addr6 = {0};
	pipe = popen(cmd, "r");

	/*
	 * Check if process is opened
	 */
	fail_if(NULL == pipe, "Failed to execute popen.");

	while(fgets(buffer, sizeof(buffer), pipe)) {
		buffer[strlen(buffer)-1] = '\0';
		if (1 == inet_pton(AF_INET6, buffer, &addr6)) {
			pclose(pipe);
			remote_ip = "::1";
			return remote_ip;
		}
	}

	/*
	 * Close the pipe
	 */
	pclose(pipe);

	return remote_ip;
}
