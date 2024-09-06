#include <fcntl.h>
#include "ns_object_handler.h"
#include <check.h>
#include <stdio.h>
#include <isi_rest_server/api_error.h>
#include <isi_rest_server/uri_manager.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_util_cpp/check_leak_primer.hpp>

#include "handler_common.h"
#include "oapi_mapper.h"
#include "test_request_helper.h"
#include "ns_object_protection.h"

TEST_FIXTURE(suite_setup);

SUITE_DEFINE_FOR_FILE(check_protection_get_put, .mem_check = CK_MEM_LEAKS, .dmalloc_opts = 0, .suite_setup = suite_setup);

static test_request_helper test_helper;



TEST_FIXTURE(suite_setup)
{
	test_ostream ostrm;
	response resp(ostrm);
	api_header_map headers;

	test_helper.test_send_request("GET", "/",
	    "/namespace", "", headers, resp);
}

TEST(check_protection_put, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	std::string method = "PUT";
	std::string file = "/ifs/testFile1.txt";
	std::string uri(file);

	fail_if(open(file.c_str(), O_RDWR | O_CREAT) < 0,
	    "failed to create test file");

	test_json_ostream ostrm;
	response trial_output(ostrm);
	api_header_map headers;

	std::string prot_string = "{\"coalesced\":true,";
	prot_string.append("\"data_disk_pool_id\":1,");
	prot_string.append("\"disk_pool_policy_id\":5,");
	prot_string.append("\"endurant\":false,");
	prot_string.append("\"is_enforced\":true,");
	prot_string.append("\"level\":\"PROTECTION_TYPE_MIRRORING\",");
	prot_string.append("\"manually_manage_access\":true,");
	prot_string.append("\"manually_manage_protection\":true,");
	prot_string.append("\"metadata_disk_pool_id\":1,");
	prot_string.append("\"performance\":0,");
	prot_string.append("\"policy\":\"6x\",");
	prot_string.append("\"ssd_strategy\":\"metadata\"}");

	test_json_istream trial_input(prot_string);

	int user_fd = gettcred(NULL, 0);

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	test_helper.test_send_request_ext(method, uri, NS_NAMESPACE, "protection",
	    headers, trial_input.get_size(), user_fd,
	    test_helper.get_remote_ip(), trial_input, trial_output);

	/*
	 * Check the http return code
	 */
	fail_if(trial_output.get_status() !=  ASC_OK,
	    "Failed to set Protection Attributes %d\n", trial_output.get_status());


	api_header_map header;

	method = "GET";

	uri = "/ifs/testFile1.txt";

	std::string store_type = "/namespace";

	std::string args = "protection";

	test_ostream ostrm_get;
	response trial_output_get(ostrm_get);

	test_helper.test_send_request(method, uri, store_type, args, header, 
		trial_output_get);

	fail_if(trial_output_get.get_status() !=  ASC_OK,
		"GET file failed: %d\n",
		trial_output_get.get_status());

	const Json::Value &attrs = trial_output_get.get_json();

	fail_if(attrs[ISI_PROTECTION_SSD_STRATEGY].asString() != "metadata", 
	    "GET and PUT values do not match");
	fail_if(attrs[ISI_PROTECTION_MANUALLY_MANAGE_ACCESS].asBool() != true, 
	    "GET and PUT values do not match");
	fail_if(attrs[ISI_PROTECTION_COALESCED].asBool() != true, 
	    "GET and PUT values do not match");
	fail_if(attrs[ISI_PROTECTION_PERFORMANCE].asInt() != 0, 
	    "GET and PUT values do not match");

	fail_if(attrs[ISI_PROTECTION_MANUALLY_MANAGE_PROTECTION].asBool() != true, 
	    "GET and PUT values do not match");

	fail_if(attrs[ISI_PROTECTION_DISK_POOL_POLICY_ID].asInt() != 5, 
	    "GET and PUT values do not match");

}
