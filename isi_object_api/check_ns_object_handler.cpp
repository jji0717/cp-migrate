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
#include "check_create_dummy_file.hpp"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(check_ns_object_handler,
    CK_MEM_LEAKS,
    NULL,
    suite_setup,
    suite_teardown,
    NULL,
    NULL,
    NULL,
    120);

static test_request_helper rh;

const static std::string MTHD_GET("GET");
const static std::string MTHD_PUT("PUT");
const static std::string MTHD_HEAD("HEAD");
const static std::string MTHD_POST("POST");
const static std::string MTHD_DELETE("DELETE");
const static std::string base("https://localhost:8080/namespace/");

#define BUF_READ_SIZE	8192

/* Utility routines. */
int copy_file(const char *src, const char *dest)
{
	int srcfile, destfile, rd, wr = 0;
	char *buf;

	// Allocate memory in the heap.
	buf = (char *) calloc(BUF_READ_SIZE, sizeof(char));

	// Open the source and destination files.
	srcfile = open(src, O_RDONLY);
	fail_if ((srcfile < 0),
	    "Failed to open source %s for copy: (%d)%s",
	    src, errno, strerror(errno));

	destfile = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	fail_if ((destfile < 0),
	    "Failed to open destination %s for copy: (%d)%s",
	    dest, errno, strerror(errno));

	// Copy contents from source file to destination file.
	while ((rd = read(srcfile, buf, BUF_READ_SIZE)) > 0) {
		wr = write(destfile, buf, rd);
		fail_if(wr < 0, "Failed to write file %s: errno (%d)%s",
		    dest, errno, strerror(errno));
		// The read and write should be the same so we know
		// the number of bytes read and written are the same.
		fail_if(rd != wr,
		    "Bytes read : %d and bytes written : %d are not the same",
		    rd, wr);
		memset(buf, '\0', BUF_READ_SIZE);
		rd = 0, wr = 0;
	}

	fail_if(rd < 0, "Failed to read file %s: errno (%d)%s",
	    src, errno, strerror(errno));

	fchmod(destfile, (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH));


	// Close the source and destination files.
	close(srcfile);
	close(destfile);

	// Free the allocated memory.
	free(buf);
	return 0;
}



/*
 * Some of the tests in this file has .mem_hint hard coded, which may change
 * when test env. and build optimization level are changed.
 *
 * The issue is believed to be caused by local static object whose life cycle
 * begins after unit test starts. The idea here is to implement some code in
 * suite_setup to help reduce the bogus memory leak warning.
 *
 * Thus it is still desirable if we can find a reliable way to do it.
 */

TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_ns_object_handler",	// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_ns_object_handler",	// Syslog program
		IL_TRACE_PLUS|IL_DETAILS|IL_CP_EXT|
		IL_REST_USER|IL_REST_REQ|IL_REST_HDR,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_ns_object_handler.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif

	// to help reduce the bogus memory leak.
	test_ostream ostrm;
	response resp(ostrm);
	api_header_map headers;
	std::string str = "/ifs/__directory_with_file_unit_test/1.txt";

	rh.test_send_request(MTHD_GET, "/",
	    NS_NAMESPACE, "", headers, resp);
	rh.test_send_request(MTHD_GET, "/ifs",
	    NS_NAMESPACE, "", headers, resp);
	rh.test_send_request(MTHD_GET, "/ifs/__not_exist_",
	    NS_NAMESPACE, "", headers, resp);
	rh.test_send_request(MTHD_PUT, "/ifs/.ifsvar",
	    NS_NAMESPACE, "", headers, resp);

	// Fixing the leak that is in test_case_sensitivity.
	std::string dpath = "/ifs/mYfOlDeR";
	std::string fpath = "/ifs/mYfOlDeR/MyFiLe.tXt";

	headers[HDR_TARGET_TYPE] = TT_CONTAINER;
	rh.test_send_request(MTHD_PUT, dpath, NS_NAMESPACE, "", headers, resp);

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request(MTHD_PUT, fpath, NS_NAMESPACE, "", headers, resp);

	rh.test_send_request(MTHD_GET, fpath, NS_NAMESPACE, "case-sensitive=1", headers, resp);
	create_dummy_test_file();

}
TEST_FIXTURE(suite_teardown)
{
	remove_dummy_test_file();
}

/*
 * Get the IFS root
 */
TEST(test_list_ifs_directory, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	/*
	 * parameters needed for request construction
	 */
	std::string uri("/ifs");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Send the GET request
	 */
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE, "",
	    headers, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "listing /IFS directory failed: %d\n",
	    reqresp.get_status());
}

/*
 * Get a non existant directory
 */
TEST(test_list_nonxist_directory, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	/*
	 * parameters needed for request construction
	 */
	std::string uri("/ifs/__does_not_exist_unit_test");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Send the GET request
	 */
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NOT_FOUND,
	    "listing directory failed: %d\n",
	    reqresp.get_status());
}

/*
 * Find if a matched element exists in json array
 *
 * The element to be matched is, but not necessarily has only two fields
 *  {
 *    tag1: value1,
 *    tag2: value2
 *  }
 */
static bool
has_match_in_json_array(const Json::Value &array,
    const char *tag1, const char *value1,
    const char *tag2, const char *value2)
{
	ASSERT(!array.isNull() && array.isArray());
	ASSERT(tag1 && tag2 && value1 && value2);
	for (Json::Value::const_iterator itr = array.begin();
	    itr != array.end(); ++itr) {
		std::string val1, val2;
		if (json_to_c(val1, *itr, tag1, false) &&
		    json_to_c(val2, *itr, tag2, false)) {
			// matching tag/value pair
			if (!val1.compare(value1))
				return !val2.compare(value2);
		}
	}
	return false;
}

/*
 * Get the store names
 */
TEST(test_add_delete_store, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	/*
	 * parameters needed for request construction
	 */
	const char *test_sn= "__test_store";
	std::string store_uri("/");
	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	test_json_istream istrm(std::string("{\"path\": \"/ifs/\"}"));
	int user_fd = gettcred(NULL, 0);

	// to add store
	store_uri += test_sn;
	rh.test_send_request_ext(MTHD_PUT, store_uri, NS_NAMESPACE, "",
	    headers, istrm.get_size(), user_fd, rh.get_remote_ip(),
	    istrm, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "add store failed: %d\n", reqresp.get_status());

	/*
	 * To verify store exists
	 */
	rh.test_send_request(MTHD_GET, "/", NS_NAMESPACE, "", headers,
	    reqresp);

	Json::Reader reader;
	Json::Value resp_val;
	reader.parse(ostrm.get_content(), resp_val);
	const Json::Value &children = resp_val["namespaces"];
	fail_if((children.isNull() || !children.isArray()),
	    "wrong result: %s", ostrm.get_content().c_str());
	fail_if(!has_match_in_json_array(children,
	    "name", test_sn,
	    "path", "/ifs/"),
	    "wrong result: %s", ostrm.get_content().c_str());

	// ********* to delete store
	rh.test_send_request(MTHD_DELETE, store_uri, NS_NAMESPACE, "",
	    headers, reqresp);
	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "delete store failed: %d\n", reqresp.get_status());

	close(user_fd);
}

/*
 * Get the store names
 */
TEST(test_add_imaginary_store, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	/*
	 * parameters needed for request construction
	 */
	const char *IMAGINARY= "StoreDoesNotExist";
	std::string store_uri("/");
	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	test_json_istream istrm(std::string("{\"path\": \"/ifs/\"}"));
	int user_fd = gettcred(NULL, 0);

	// to add store
	store_uri += IMAGINARY;

	rh.test_send_request_ext(MTHD_PUT, store_uri, NS_NAMESPACE, "",
	    headers, istrm.get_size(), user_fd, rh.get_remote_ip(),
	    istrm, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "add store failed: %d\n", reqresp.get_status());

	/*
	 * To verify store exists
	 */
	rh.test_send_request(MTHD_GET, "/", NS_NAMESPACE, "", headers,
	    reqresp);

	Json::Reader reader;
	Json::Value resp_val;
	reader.parse(ostrm.get_content(), resp_val);
	const Json::Value &children = resp_val["namespaces"];
	fail_if((children.isNull() || !children.isArray()),
	    "wrong result: %s", ostrm.get_content().c_str());
	fail_if(!has_match_in_json_array(children,
	    "name", IMAGINARY,
	    "path", "/ifs/"),
	    "wrong result: %s", ostrm.get_content().c_str());

	// ********* to delete store
	rh.test_send_request(MTHD_DELETE, store_uri, NS_NAMESPACE, "",
	    headers, reqresp);
	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "delete store failed: %d\n", reqresp.get_status());

	close(user_fd);
}

TEST(test_add_invalid_store, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	test_json_istream istrm(std::string("{\"path\": \"/etc\"}"));
	int user_fd = gettcred(NULL, 0);

	rh.test_send_request_ext(MTHD_PUT, "/__test_store", NS_NAMESPACE, "",
	    headers, istrm.get_size(), user_fd, rh.get_remote_ip(),
	    istrm, reqresp);

	fail_if(reqresp.get_status() ==  ASC_OK,
	    "adding non ifs store succeeded: %d\n", reqresp.get_status());

	close(user_fd);
}

/*
 * Get the store names
 */
TEST(test_list_stores,  .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	/*
	 * parameters needed for request construction
	 */
	std::string uri("/");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Send the GET request
	 */
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "listing root directory failed: %d\n", reqresp.get_status());
}

/*
 * Test creating and deleting a folder with special characters
 */
TEST(test_create_special_directory, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	/* 
	 *parameters needed for request construction
	 */
	std::string uri("/ifs/~!@#$%^&*()_+t {}[]<>\\?;:'\"");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Set the headers
	 */
	headers[HDR_TARGET_TYPE] = TT_CONTAINER;

	/* 
	 * Send the PUT request
	 */
	rh.test_send_request(MTHD_PUT, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/* 
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "Creating directory failed: %d\n",
	    reqresp.get_status());		

	/* 
	 * Send the DELETE request
	 */
	rh.test_send_request(MTHD_DELETE, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/* 
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "Deleting directory failed: %d\n", reqresp.get_status());
}

/*
 * Test Putting a folder and deleting
 */
TEST(test_create_delete_directory, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	/*
	 *parameters needed for request construction
	 */
	std::string uri("/ifs/__create_delete_directory_unit_test");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Set the headers
	 */
	headers[HDR_TARGET_TYPE] = TT_CONTAINER;

	/*
	 * Send the PUT request
	 */
	rh.test_send_request(MTHD_PUT, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "Creating directory failed: %d\n",
	    reqresp.get_status());

	/*
	 * Send the DELETE request
	 */
	rh.test_send_request(MTHD_DELETE, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "Deleting directory failed: %d\n", reqresp.get_status());
}

/*
 * Test Deleting a folder that does not exist
 */
TEST(test_delete_nonexist_directory, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	/*
	 *parameters needed for request construction
	 */
	std::string uri("/ifs/__does_not_exist_unit_test");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Send the DELETE request
	 */
	rh.test_send_request(MTHD_DELETE, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 * Returns 200 for now (ASC_OK), will be changed in the future to be
	 * 204 (ASC_NO_CONTENT)
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "Delete non existing directory failed: %d\n",
	    reqresp.get_status());
}

/*
 * Add detail param = yes
 */
TEST(test_list_directory_detail, .mem_check = CK_MEM_DISABLE
    /* XXX */)
{
	/*
	 * parameters needed for request construction
	 */
	std::string uri("/ifs");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;


	/*
	 * Send the GET request
	 */
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE,
	    "detail=yes", headers, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "listing /IFS?detail=yes failed: %d\n",
	    reqresp.get_status());
}

/*
 * Test Putting and Getting a file and deleting
 */
TEST(test_put_get_delete_file, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0) /* mem_hint may need adjust if code changed */
{
	
	for (int i = 0; i < 2; ++i) {
		if (i == 1) 
			restart_leak_detection(false);
		/*
		 *parameters needed for request construction
		 */
		std::string uri("/ifs/__directory_with_file_unit_test");
		std::string urifile("/ifs/__directory_with_file_unit_test/1.txt");
		test_ostream ostrm;
		response reqresp(ostrm);
		api_header_map headers;

		/*
		 * Send the PUT request
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_PUT, uri, NS_NAMESPACE, "", headers,
		    reqresp);

		/*
		 * Check the http return code
		 */
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating directory failed: %d\n",
		    reqresp.get_status());

		/*
		 * Send the PUT File request
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE, "", headers,
		    reqresp);

		/*
		 * Check the http return code
		 */
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating file failed: %d\n",
		    reqresp.get_status());

		/*
		 * Send the GET File request
		 */
		rh.test_send_request(MTHD_GET, urifile, NS_NAMESPACE, "", headers,
		    reqresp);

		/*
		 * Check the http return code
		 */
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file failed: %d\n",
		    reqresp.get_status());

		/*
		 * Send the DELETE file request
		 */
		rh.test_send_request(MTHD_DELETE, urifile, NS_NAMESPACE, "", headers,
		    reqresp);

		/*
		 * Check the http return code
		 */
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file failed: %d\n", reqresp.get_status());

		/*
		 * Send the DELETE directory request
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri, NS_NAMESPACE, "", headers,
		    reqresp);

		/*
		 * Check the http return code
		 */
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory failed: %d\n", reqresp.get_status());
	}
}

/*
 * Test Putting and Getting a file with the recursive option and deleting
 */
TEST(test_put_get_delete_create_container, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0) /* mem_hint may need adjust if code changed */
{

	for (int i = 0; i < 2; ++i) {
		if (i == 1)
			restart_leak_detection(false);
		/*
		 *parameters needed for request construction
		 */
		std::string uri1("/ifs/__create_container_unit_test");
		std::string uri2 = uri1 + "/hash_1";
		std::string uri3 = uri2 + "/hash_2";
		std::string uri4 = uri3 + "/hash_3";
		std::string uri5 = uri4 + "/hash_4";
		std::string uri6 = uri5 + "/hash_5";
		std::string uri7 = uri6 + "/hash_6";
		std::string uri8 = uri7 + "/hash_7";
		std::string uri9 = uri8 + "/hash_8";
		std::string uri10 = uri9 + "/hash_9";
		std::string urifile = uri4 + "/xyzzy.txt";
		std::string urifile1 = uri7 + "/xyzzy1.txt";
		std::string urifile2 = uri9 + "/xyzzy2.txt";
		test_ostream ostrm;
		response reqresp(ostrm);
		api_header_map headers;

		/*
		 * Make sure we start with a clean slate
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri10, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri9, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri8, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri7, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri6, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri5, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri4, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri3, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri2, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri1, NS_NAMESPACE,
		    "", headers, reqresp);

		/*
		 * Make sure you cannot create
		 * 2 levels of directory without "recursive=true"
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_PUT, uri2, NS_NAMESPACE,
		    "", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_BAD_REQUEST,
		    "Creating directory %s should have failed with %d but got: %d\n",
		    uri2.c_str(), ASC_BAD_REQUEST, reqresp.get_status());

		/*
		 * Send the PUT request to
		 * create the first 2 levels of directory
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_PUT, uri2, NS_NAMESPACE,
		    "recursive=true", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating directory %s failed: %d\n",
		    uri2.c_str(), reqresp.get_status());

		/*
		 * Make sure you cannot create
		 * the last 2 directories and file in the same
		 * call without  "create_container=2"
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    "create_container=1", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_FORBIDDEN,
		    "Creating file with create_container=1"
		    " %s should have failed with %d but got: %d\n",
		    urifile.c_str(), ASC_FORBIDDEN, reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    "", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_NOT_FOUND,
		    "Creating file (No param)"
		    " %s should have failed with %d but got: %d\n",
		    urifile.c_str(), ASC_NOT_FOUND, reqresp.get_status());

		/*
		 * Send the PUT File request to create the
		 * third and forth level directories and the file
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    "create_container=2", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating file %s failed: %d\n",
		    urifile.c_str(), reqresp.get_status());

		/*
		 * Make sure you can create
		 * the next 3 directories and file in the same
		 * call with  "create_container=10"
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile1, NS_NAMESPACE,
		    "create_container=10", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating file %s failed: %d\n",
		    urifile1.c_str(), reqresp.get_status());

		/*
		 * Send the PUT File request to create
		 * ALL missing levels of directories and the file
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_PUT, urifile2, NS_NAMESPACE,
		    "create_container=-1", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating file %s failed: %d\n",
		    urifile2.c_str(), reqresp.get_status());

		/*
		 * Send the GET File request
		 */
		rh.test_send_request(MTHD_GET, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_GET, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile1.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_GET, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile2.c_str(), reqresp.get_status());

		/*
		 * Make sure we cannot delete a directory unless it is empty
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri4, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_FORBIDDEN,
		    "Deleting NONempty directory %s should have failed"
		    " with %d but got: %d\n",
		    uri4.c_str(), ASC_FORBIDDEN, reqresp.get_status());

		/*
		 * Send the DELETE file request
		 */
		rh.test_send_request(MTHD_DELETE, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_DELETE, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile1.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_DELETE, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile2.c_str(), reqresp.get_status());

		/*
		 * Make sure we cannot delete a directory
		 * that contains another directory
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_FORBIDDEN,
		    "Deleting directory tree %s should have failed"
		    " with %d but got: %d\n",
		    uri1.c_str(), ASC_FORBIDDEN, reqresp.get_status());

		/*
		 * Send the DELETE directory requests
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri10, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 10 %s failed: %d\n",
		    uri10.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri9, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 9 %s failed: %d\n",
		    uri9.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri8, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 8 %s failed: %d\n",
		    uri8.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri7, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 7 %s failed: %d\n",
		    uri7.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri6, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 6 %s failed: %d\n",
		    uri6.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri5, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 5 %s failed: %d\n",
		    uri5.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri4, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 4 %s failed: %d\n",
		    uri4.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri3, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 3 %s failed: %d\n",
		    uri3.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 2 %s failed: %d\n",
		    uri2.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 1 %s failed: %d\n",
		    uri1.c_str(), reqresp.get_status());

	}
}

/*
 * Test Deleting a file that does not exist
 */
TEST(test_delete_nonexisting_file, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	/*
	 *parameters needed for request construction
	 */
	std::string uri("/ifs/__nonexisting_file_unit_test/2.txt");
	test_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Send the DELETE file request
	 */
	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request(MTHD_DELETE, uri, NS_NAMESPACE, "", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
	    "Deleting file failed: %d\n", reqresp.get_status());
}

/*
 * Get a directory acl
 */
TEST(test_get_directory_acl, .mem_check = CK_MEM_DISABLE)
{
	/*
	 * parameters needed for request construction
	 */
	std::string uri("/ifs/");
	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	/*
	 * Get the acl for /ifs
	 */
	headers[HDR_TARGET_TYPE] = TT_CONTAINER;
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE, "acl", headers,
	    reqresp);

	//Json::Value &resp_val = reqresp.get_json();
	//printf("%s\n", resp_val.toStyledString().c_str());

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "failed to get acl: %d\n", reqresp.get_status());
}

/*
 * Get a file acl
 */
TEST(test_get_file_acl, .mem_check = CK_MEM_DISABLE)
{
	/*
	 * parameters needed for request construction
	 */
	std::string file = "/ifs/__unit_test_file__.txt";
	std::string uri(file);

	fail_if(open(file.c_str(), O_RDWR | O_CREAT) < 0,
	    "failed to create test file");

	mode_t o_mode = 0644;
	int ret;
	ret = chmod(file.c_str(), o_mode);
	fail_if(ret < 0, "failed to change mode");

	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request(MTHD_GET, uri, NS_NAMESPACE, "acl", headers,
	    reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "failed to get acl: %d\n", reqresp.get_status());

	Json::Value &resp_val = reqresp.get_json();
	//printf("%s\n", resp_val.toStyledString().c_str());
	fail_if(resp_val["authoritative"] != "mode", "failed to get acl mode");
	fail_if(resp_val["mode"] != "0644", "failed to get acl mode");

	fail_if(unlink(file.c_str()) < 0, "failed to remove test file");
}

/*
 * Test ownership update
 */
TEST(test_set_ownership_acl, .mem_check = CK_MEM_DISABLE)
{
	/*
	 * parameters needed for request construction
	 */
	std::string file = "/ifs/__unit_test_file__.txt";
	std::string uri(file);

	fail_if(open(file.c_str(), O_RDWR | O_CREAT) < 0,
	    "failed to create test file");

	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	std::string acl = "{\"action\":\"update\",";
	// admin uid/gid is 10/10; either is ok
	//acl.append("\"group\":{\"id\":\"GID:10\",\"type\":\"group\"},");
	//acl.append("\"owner\":{\"id\":\"UID:10\",\"type\":\"user\"},");
	acl.append("\"group\":{\"name\":\"admin\",\"type\":\"group\"},");
	acl.append("\"owner\":{\"name\":\"admin\",\"type\":\"user\"},");
	acl.append("\"authoritative\":\"acl\",");
	acl.append("\"acl\":[{\"accessrights\":[\"dir_gen_read\",");
	acl.append("\"dir_gen_write\",\"dir_gen_execute\",\"delete_child\"],");
	acl.append("\"accesstype\":\"allow\",\"inherit_flags\":[],");
	acl.append("\"trustee\":{\"id\":\"GID:0\",\"name\":\"wheel\",");
	acl.append("\"type\":\"group\"}}]}");
	test_json_istream istrm(acl);

	int user_fd = gettcred(NULL, 0);

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request_ext(MTHD_PUT, uri, NS_NAMESPACE, "acl",
	    headers, istrm.get_size(), user_fd, rh.get_remote_ip(),
	    istrm, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "failed to set_ownership: %d\n", reqresp.get_status());

	struct stat sb;
	int ret = stat(file.c_str(), &sb);
	fail_if(ret < 0, "failed to stat the test file");
	fail_if(sb.st_uid != 10 || sb.st_gid != 10,
	    "failed to set ownership");

	fail_if(unlink(file.c_str()) < 0, "failed to remove test file");
}

/*
 * Test permission update
 */
TEST(test_set_unix_permission, .mem_check = CK_MEM_DISABLE)
{
	/*
	 * parameters needed for request construction
	 */
	std::string file = "/ifs/__unit_test_file__.txt";
	std::string uri(file);

	fail_if(open(file.c_str(), O_RDWR | O_CREAT) < 0,
	    "failed to create test file");

	mode_t o_mode = 0644;
	int ret;
	ret = chmod(file.c_str(), o_mode);
	fail_if(ret < 0, "failed to change mode");

	test_json_ostream ostrm;
	response reqresp(ostrm);
	api_header_map headers;

	std::string unix_p = "{";
	unix_p.append("\"group\":{\"name\":\"admin\",\"type\":\"group\"},");
	unix_p.append("\"owner\":{\"name\":\"admin\",\"type\":\"user\"},");
	unix_p.append("\"authoritative\":\"mode\",");
	unix_p.append("\"mode\":\"0666\"}");
	test_json_istream istrm(unix_p);

	int user_fd = gettcred(NULL, 0);

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request_ext(MTHD_PUT, uri, NS_NAMESPACE,
	    "acl", headers, istrm.get_size(), user_fd, rh.get_remote_ip(),
	    istrm, reqresp);

	/*
	 * Check the http return code
	 */
	fail_if(reqresp.get_status() !=  ASC_OK,
	    "failed to set unix_permission: %d\n", reqresp.get_status());

	struct stat sb;
	ret = stat(file.c_str(), &sb);
	fail_if(ret < 0, "failed to stat the test file");
	bool set_failed = false;
	if (sb.st_uid != 10 || sb.st_gid != 10 || (sb.st_mode & 0777) != 0666)
		set_failed = true;
	fail_if(set_failed, "failed to set unix_permission");

	fail_if(unlink(file.c_str()) < 0, "failed to remove test file");
}

static void
list_or_query_bucket(const std::string method, const char *dir,
    const char *query_str, test_json_istream &istrm,
    std::map<std::string, std::string> &expect_res)
{
	test_json_ostream ostrm;
	response resp(ostrm);
	api_header_map headers;
	Json::Reader reader;
	Json::Value val;
	int user_fd = gettcred(NULL, 0);

	rh.test_send_request_ext(method, dir, NS_NAMESPACE,
	    query_str, headers, istrm.get_size(), user_fd,
	    rh.get_remote_ip(), istrm, resp);

	fail_if(resp.get_status() != ASC_OK,
	    "unexpected status: %d", resp.get_status());
	reader.parse(ostrm.get_content(), val);

	const Json::Value &children = val["children"];
	fail_if(children.isNull() || !children.isArray(), "");

	for (Json::Value::const_iterator itr = children.begin();
	    itr != children.end(); ++itr) {
		const Json::Value &itm = *itr;
		std::string fname;
		std::string symblnk_type;

		// printf("\t%s\n", itm.toStyledString().c_str());

		fail_if(!itm.isObject() || itm["name"].isNull() ||
		    itm["symbolic_link_type"].isNull(), "");

		fname = itm["name"].asString();
		symblnk_type = itm["symbolic_link_type"].asString();

		fail_if(expect_res[fname] != symblnk_type,
		    "%s expect %s, actual %s", fname.c_str(),
		    expect_res[fname].c_str(), symblnk_type.c_str());
	}

	fail_if(children.size() != expect_res.size(),
	    "expect %ld objects, actual %ld objects",
	    expect_res.size(), children.size());
}

TEST(test_symbolic_link, .mem_check = CK_MEM_DISABLE)
{
#define TEST_BUCKET  "symbolic_test_dir"
#define TEST_DIR     "/ifs/" TEST_BUCKET
	system("rm -rf " TEST_DIR);
	fail_if(system("mkdir " TEST_DIR) != 0, "");

	fail_if(system("ln -s "DUMMY_TEST_FILE" " TEST_DIR "/r.txt") != 0, "");
	fail_if(system("ln -s " TEST_DIR "/r.txt " TEST_DIR "/a.txt") != 0, "");
	fail_if(system("ln -s /ifs/data " TEST_DIR "/d") != 0, "");
	fail_if(system("ln -s " TEST_DIR "/d " TEST_DIR "/d1") != 0, "");

	// test get operation
	{
		std::map<std::string, std::string> expect_res;
		test_json_istream istrm("");

		expect_res["r.txt"] = "object";
		expect_res["a.txt"] = "object";
		expect_res["d"]     = "container";
		expect_res["d1"]    = "container";

		list_or_query_bucket(MTHD_GET, TEST_DIR, "detail=default", istrm,
		    expect_res);
	}

	// query operation
	{
		std::map<std::string, std::string> expect_res;
		const char *query_str = "{"
		    "\"result\": [\"name\", \"type\", \"symbolic_link_type\"],"
		    "\"scope\": {"
		        "\"operator\": \"=\","
			"\"attr\": \"symbolic_link_type\","
			"\"value\": \"object\""
		    "}"
		"}";
		test_json_istream istrm(query_str);

		expect_res["r.txt"] = "object";
		expect_res["a.txt"] = "object";

		list_or_query_bucket(MTHD_POST, TEST_DIR, "query&detail=default",
		    istrm, expect_res);
	}
	{
		std::map<std::string, std::string> expect_res;
		const char *query_str = "{"
		    "\"result\": [\"name\", \"type\", \"symbolic_link_type\"],"
		    "\"scope\": {"
		        "\"operator\": \"=\","
			"\"attr\": \"symbolic_link_type\","
			"\"value\": \"container\""
		    "}"
		"}";
		test_json_istream istrm(query_str);

		expect_res["d"]  = "container";
		expect_res["d1"] = "container";

		list_or_query_bucket(MTHD_POST, TEST_DIR, "query&detail=default",
		    istrm, expect_res);
	}
	{
		std::map<std::string, std::string> expect_res;
		const char *query_str = "{"
		    "\"result\": [\"name\", \"type\", \"symbolic_link_type\"]"
		"}";
		test_json_istream istrm(query_str);

		expect_res["r.txt"] = "object";
		expect_res["a.txt"] = "object";
		expect_res["d"]  = "container";
		expect_res["d1"] = "container";

		list_or_query_bucket(MTHD_POST, TEST_DIR, "query&detail=default",
		    istrm, expect_res);
	}


	fail_if(system("rm -rf " TEST_DIR) != 0, "");
}

TEST(test_case_sensitivity, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	std::string dpath = "/ifs/mYfOlDeR";
	std::string fpath = "/ifs/mYfOlDeR/MyFiLe.tXt";

	struct {
		std::string fpath;
		bool sensitive_match;
	} fpath_case_sensitivity[] = {
		{"/ifs/mYfOlDeR/MyFiLe.tXt", true},
		{"/ifs/mYfOlDeR/myfile.txt", false},
		{"/ifs/myfolder/MyFiLe.txt", false},
		{"/ifs/myfolder/myfile.txt", false},
		{"/ifs/MYFOLDER/MYFILE.TXT", false}
	};
	test_ostream ostrm;
	response resp(ostrm);
	api_header_map headers;

	headers[HDR_TARGET_TYPE] = TT_CONTAINER;
	rh.test_send_request(MTHD_PUT, dpath, NS_NAMESPACE, "", headers, resp);
	fail_if(resp.get_status() != ASC_OK, "create directory failed: %d",
	    resp.get_status());

	headers[HDR_TARGET_TYPE] = TT_OBJECT;
	rh.test_send_request(MTHD_PUT, fpath, NS_NAMESPACE, "", headers, resp);
	fail_if(resp.get_status() != ASC_OK, "create file failed: %d",
	    resp.get_status());

#define CNT(arr) (sizeof(arr) / sizeof(arr[0]))

#define OP_(op, fpath, req, expect_res) {\
	test_ostream ostrm; \
	response resp(ostrm); \
	rh.test_send_request(op, fpath, NS_NAMESPACE, req, headers, resp); \
	fail_if(resp.get_status() != expect_res, \
	    "%s %s expect %d, actual %d", op.c_str(), fpath.c_str(), \
	    expect_res, resp.get_status()); \
	}

	// get & head op
	for (size_t i = 0; i < CNT(fpath_case_sensitivity); ++i) {
		std::string &test_fpath = fpath_case_sensitivity[i].fpath;
		bool sensitive_match = fpath_case_sensitivity[i].sensitive_match;

		OP_(MTHD_GET, test_fpath, "case-sensitive=1",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_GET, test_fpath, "case-sensitive=true",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_GET, test_fpath, "",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_GET, test_fpath, "case-sensitive=0", ASC_OK);
		OP_(MTHD_GET, test_fpath, "case-sensitive=false", ASC_OK);
		OP_(MTHD_GET, test_fpath, "case-sensitive=not_set", ASC_BAD_REQUEST);

		OP_(MTHD_HEAD, test_fpath, "case-sensitive=1",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_HEAD, test_fpath, "case-sensitive=true",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_HEAD, test_fpath, "",
		    (sensitive_match ? ASC_OK : ASC_NOT_FOUND));
		OP_(MTHD_HEAD, test_fpath, "case-sensitive=0", ASC_OK);
		OP_(MTHD_HEAD, test_fpath, "case-sensitive=false", ASC_OK);
		OP_(MTHD_HEAD, test_fpath, "case-sensitive=not_set", ASC_BAD_REQUEST);
	}

#define CHECK_DEL(path, exists) { \
	struct stat st; \
	fail_if(exists && stat(path, &st) != 0, "%s not exists", path); \
	fail_if(!exists && stat(path, &st) == 0, "%s exists", path); \
}
	// delete, no matter succeed or not, DELETE always returns ASC_NO_CONTENT
	for (size_t i = 0; i < CNT(fpath_case_sensitivity); ++i) {
		std::string &test_fpath = fpath_case_sensitivity[i].fpath;

		if (fpath_case_sensitivity[i].sensitive_match)
			continue;

		OP_(MTHD_DELETE, test_fpath, "case-sensitive=1", ASC_NO_CONTENT);
		OP_(MTHD_DELETE, test_fpath, "case-sensitive=true", ASC_NO_CONTENT);
		OP_(MTHD_DELETE, test_fpath, "", ASC_NO_CONTENT);

		CHECK_DEL(fpath.c_str(), true);
		CHECK_DEL(dpath.c_str(), true);
	}
	OP_(MTHD_DELETE, fpath, "", ASC_NO_CONTENT);
	CHECK_DEL(fpath.c_str(), false);
	CHECK_DEL(dpath.c_str(), true);

	headers[HDR_TARGET_TYPE] = TT_CONTAINER;
	OP_(MTHD_DELETE, dpath, "", ASC_NO_CONTENT);
	CHECK_DEL(fpath.c_str(), false);
	CHECK_DEL(dpath.c_str(), false);
}

/*
 * Test Cloning and Getting a file with the recursive option and deleting
 */
TEST(test_clone_get_delete_create_container, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0) /* mem_hint may need adjust if code changed */
{
	/*
	 *parameters needed for request construction
	 */
	std::string uri1("/ifs/__clone_unit_test");
	std::string uri2 = uri1 + "/clone_1";
	std::string uri3 = uri2 + "/clone_2";
	std::string uri4 = uri3 + "/clone_3";
	std::string uri5 = uri4 + "/clone_4";
	std::string uri6 = uri5 + "/clone_5";
	std::string uri7 = uri6 + "/clone_6";
	std::string uri8 = uri7 + "/clone_7";
	std::string uri9 = uri8 + "/clone_8";
	std::string uri10 = uri9 + "/clone_9";
	std::string urifile = uri4 + "/xyzzy.txt";
	std::string urifile1 = uri7 + "/xyzzy1.txt";
	std::string urifile2 = uri9 + "/xyzzy2.txt";

	std::string clonesourcefile =
		uri2 + "/clonesource.txt";
	std::string clonesourceheader =
		"/namespace" + clonesourcefile;
	std::string creat_cont_param_neg =
		"create_container=-1" ;
	std::string creat_cont_param_1 =
		"create_container=1";
	std::string creat_cont_param_2 =
		"create_container=2";
	std::string creat_cont_param_10 =
		"create_container=10";

	for (int i = 0; i < 2; ++i) {
		if (i == 1)
			restart_leak_detection(false);

		test_ostream ostrm;
		response reqresp(ostrm);
		api_header_map headers;
		api_header_map cpheaders;

		/*
		 * Make sure we start with a clean slate
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, clonesourcefile,
		    NS_NAMESPACE, "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri10, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri9, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri8, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri7, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri6, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri5, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri4, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri3, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri2, NS_NAMESPACE,
		    "", headers, reqresp);
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri1, NS_NAMESPACE,
		    "", headers, reqresp);


		/*
		 * Send the PUT request to
		 * create the first 2 levels of directory
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_PUT, uri2, NS_NAMESPACE,
		    "recursive=true", headers,
		    reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Creating directory %s failed: %d\n",
		    uri2.c_str(), reqresp.get_status());

		/*
		 * Create the clone source file
		 */
		copy_file("/etc/services", clonesourcefile.c_str());

		/*
		 * Make sure you cannot create
		 * the last 2 directories and file in the same
		 * call without  "create_container=2"
		 */
		cpheaders[HDR_TARGET_TYPE] = TT_OBJECT;
		cpheaders[HDR_COPY_SOURCE] = clonesourceheader;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    creat_cont_param_1, cpheaders, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NOT_FOUND,
		    "Cloning %s to %s with create_container=1"
		    " should have failed with %d but got: %d\n",
		    clonesourcefile.c_str(), urifile.c_str(),
		    ASC_NOT_FOUND, reqresp.get_status());

		cpheaders[HDR_TARGET_TYPE] = TT_OBJECT;
		cpheaders[HDR_COPY_SOURCE] = clonesourceheader;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    "", cpheaders, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NOT_FOUND,
		    "Cloning (No param)"
		    " %s to %s should have failed with %d but got: %d\n",
		    clonesourcefile.c_str(), urifile.c_str(),
		    ASC_NOT_FOUND, reqresp.get_status());

		/*
		 * Send the PUT File request to create the
		 * third and forth level directories and the file
		 */
		cpheaders[HDR_TARGET_TYPE] = TT_OBJECT;
		cpheaders[HDR_COPY_SOURCE] = clonesourceheader;
		rh.test_send_request(MTHD_PUT, urifile, NS_NAMESPACE,
		    creat_cont_param_2, cpheaders, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Cloning %s to %s failed: %d\n",
		    clonesourcefile.c_str(), urifile.c_str(),
		    reqresp.get_status());

		/*
		 * Make sure you can create
		 * the next 3 directories and file in the same
		 * call with  "create_container=10"
		 */
		cpheaders[HDR_TARGET_TYPE] = TT_OBJECT;
		cpheaders[HDR_COPY_SOURCE] = clonesourceheader;
		rh.test_send_request(MTHD_PUT, urifile1, NS_NAMESPACE,
		    creat_cont_param_10, cpheaders, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Cloning %s to %s failed: %d\n",
		    clonesourcefile.c_str(), urifile1.c_str(),
		    reqresp.get_status());

		/*
		 * Send the PUT File request to create
		 * ALL missing levels of directories and the file
		 */
		cpheaders[HDR_TARGET_TYPE] = TT_OBJECT;
		cpheaders[HDR_COPY_SOURCE] = clonesourceheader;
		rh.test_send_request(MTHD_PUT, urifile2, NS_NAMESPACE,
		    creat_cont_param_neg, cpheaders, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "Cloning %s to %s failed: %d\n",
		    clonesourcefile.c_str(), urifile2.c_str(),
		    reqresp.get_status());

		/*
		 * Send the GET File request
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_GET, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_GET, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile1.c_str(), reqresp.get_status());

		rh.test_send_request(MTHD_GET, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_OK,
		    "GET file %s failed: %d\n",
		    urifile2.c_str(), reqresp.get_status());

		/*
		 * Send the DELETE file request
		 */
		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile1.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, urifile2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    urifile2.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_OBJECT;
		rh.test_send_request(MTHD_DELETE, clonesourcefile, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting file %s failed: %d\n",
		    clonesourcefile.c_str(), reqresp.get_status());


		/*
		 * Send the DELETE directory requests
		 */
		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri10, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 10 %s failed: %d\n",
		    uri10.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri9, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 9 %s failed: %d\n",
		    uri9.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri8, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 8 %s failed: %d\n",
		    uri8.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri7, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 7 %s failed: %d\n",
		    uri7.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri6, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 6 %s failed: %d\n",
		    uri6.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri5, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 5 %s failed: %d\n",
		    uri5.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri4, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 4 %s failed: %d\n",
		    uri4.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri3, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 3 %s failed: %d\n",
		    uri3.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri2, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 2 %s failed: %d\n",
		    uri2.c_str(), reqresp.get_status());

		headers[HDR_TARGET_TYPE] = TT_CONTAINER;
		rh.test_send_request(MTHD_DELETE, uri1, NS_NAMESPACE,
		    "", headers, reqresp);
		fail_if(reqresp.get_status() !=  ASC_NO_CONTENT,
		    "Deleting directory level 1 %s failed: %d\n",
		    uri1.c_str(), reqresp.get_status());
	}
}


static inline uint64_t
get_filerev(const char *fname)
{
	uint64_t filerev = 0;
	int fd = open(fname, O_RDONLY);

	fail_if(fd < 0, "failed to open file: %s", fname);
	fail_if(getfilerev(fd, &filerev) != 0, "failed to get filerev");
	close(fd);
	return filerev;
}

enum etag_type { ETAG_NO, ETAG_TAG, ETAG_MASK, ETAG_ERR,
    ETAG_WITH_TAG, ETAG_NO_TAG };

static inline void
etag_verify(const std::string &op, const std::string &file,
    const std::string &query,
    const std::string &extra_hdr_k, const std::string &extra_hdr_v,
    bool if_match, bool if_none_match, enum etag_type type,
    int expect_status, bool expect_etag)
{
	test_ostream ostrm;
	response resp(ostrm);
	api_header_map headers;
	api_header_map::const_iterator itr;
	api_header_map::const_iterator end = resp.get_header_map().end();
	struct fmt FMT_INIT_CLEAN(etag);
	struct fmt FMT_INIT_CLEAN(etag_req);
	struct stat st;

	fail_if(stat(file.c_str(), &st) != 0,
	    "unexpected error: %s", strerror(errno));
	fail_if(if_match && if_none_match,
	    "don't set If-Match If-None-Match together");

	switch (type) {
	case ETAG_NO:
		break;
	case ETAG_TAG:
		fmt_print(&etag_req, "\"%lu-%lu-%lu\"",
		    st.st_ino, st.st_snapid, get_filerev(file.c_str()));
		break;
	case ETAG_MASK:
		fmt_print(&etag_req, "*");
		break;
	case ETAG_ERR:
		fmt_print(&etag_req, "no-such-etag");
		break;
	case ETAG_WITH_TAG:
		fmt_print(&etag_req, "\"%lu-%lu-%lu\", \"no-such-etag\"",
		    st.st_ino, st.st_snapid, get_filerev(file.c_str()));
		break;
	case ETAG_NO_TAG:
		fmt_print(&etag_req, "\"%d-%d-%d\", \"no-such-etag\"", 0, 0, 0);
		break;
	default:
		fail("invalid parameter");
	}

	if (if_match)
		headers[HDR_IF_MATCH] = fmt_string(&etag_req);

	if (if_none_match)
		headers[HDR_IF_NONE_MATCH] = fmt_string(&etag_req);

	if (extra_hdr_k.size() > 0)
		headers[extra_hdr_k] = extra_hdr_v;

	rh.test_send_request(op, file, NS_NAMESPACE, query, headers, resp);

	fail_if(resp.get_status() != expect_status,
	    "expect status code: %d, actual %d",
	    expect_status, resp.get_status());

	itr = resp.get_header_map().find("Etag");

	if (expect_etag) {
		//fmt_print(&etag, "%lu-%lu-%lu",
		//    st.st_ino, st.st_snapid, get_filerev(file.c_str()));
		fail_if(itr == end, "expect header: Etag");
		//fail_if(itr->second != fmt_string(&etag),
		//    "%s - expect Etag: %s, actual: %s",
		//    op.c_str(),  fmt_string(&etag), itr->second.c_str());
		}
	else
		fail_if(itr != end, "unexpect header: Etag");
}

TEST(test_etag, .mem_check = CK_MEM_DISABLE)
{
#define TEST_FILE "/ifs/test_etag.txt"
#define TEST_MV_FILE "/ifs/test_mv_etag.txt"
#define TEST_FOLDER "/ifs/etag_folder"

#define F_F false, false
#define T_F true, false
#define F_T false, true
#define ASC_COND_F ASC_PRE_CONDITION_FAILED

	const std::string d_default = "?detail=default";
	const std::string meta = "?metadata";

	struct test_setup{
		std::string method;
		std::string file;
		std::string param;
		bool if_match;
		bool if_none_match;
		enum etag_type type;
		int expect_status;
		bool expect_etag;
	} test_matrix[] = {
		// HEAD:
		{MTHD_HEAD, TEST_FILE, "", F_F, ETAG_NO, ASC_OK, true},

		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, "", T_F, ETAG_NO_TAG, ASC_COND_F, true},

		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_NO, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, "", F_T, ETAG_NO_TAG, ASC_OK, true},

		{MTHD_HEAD, TEST_FILE, d_default, F_F, ETAG_NO, ASC_OK, true},

		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, d_default, T_F, ETAG_NO_TAG, ASC_COND_F, true},

		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_NO, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_HEAD, TEST_FILE, d_default, F_T, ETAG_NO_TAG, ASC_OK, true},

		// GET
		{MTHD_GET, TEST_FILE, "", F_F, ETAG_NO, ASC_OK, true},

		{MTHD_GET, TEST_FILE, "", T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, "", T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, "", T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_GET, TEST_FILE, "", T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, "", T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, "", T_F, ETAG_NO_TAG, ASC_COND_F, true},

		{MTHD_GET, TEST_FILE, "", F_T, ETAG_NO, ASC_OK, true},
		{MTHD_GET, TEST_FILE, "", F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, "", F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, "", F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_GET, TEST_FILE, "", F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, "", F_T, ETAG_NO_TAG, ASC_OK, true},

		{MTHD_GET, TEST_FILE, d_default, F_F, ETAG_NO, ASC_OK, true},

		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, d_default, T_F, ETAG_NO_TAG, ASC_COND_F, true},

		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_NO, ASC_OK, true},
		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, d_default, F_T, ETAG_NO_TAG, ASC_OK, true},

		// metedata
		{MTHD_GET, TEST_FILE, meta, F_F, ETAG_NO, ASC_OK, true},

		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FILE, meta, T_F, ETAG_NO_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_NO, ASC_OK, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FILE, meta, F_T, ETAG_NO_TAG, ASC_OK, true},

		// query
		{MTHD_GET, TEST_FOLDER, "?query", F_F, ETAG_NO, ASC_OK, true},

		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_NO, ASC_COND_F, true},
		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_MASK, ASC_OK, true},
		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_ERR, ASC_COND_F, true},
		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_WITH_TAG, ASC_OK, true},
		{MTHD_GET, TEST_FOLDER, "?query", T_F, ETAG_NO_TAG, ASC_COND_F, true},

		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_NO, ASC_OK, true},
		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_MASK, ASC_COND_F, true},
		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_ERR, ASC_OK, true},
		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_WITH_TAG, ASC_COND_F, true},
		{MTHD_GET, TEST_FOLDER, "?query", F_T, ETAG_NO_TAG, ASC_OK, true},

		// delete
		{MTHD_DELETE, TEST_FILE,   "", F_F, ETAG_NO, ASC_NO_CONTENT, false},
		{MTHD_DELETE, TEST_FOLDER, "", F_F, ETAG_NO, ASC_NO_CONTENT, false},
	};

	system("rm "     TEST_FILE   " 2>&1 | cat /dev/null");
	system("rm -rf " TEST_FOLDER " 2>&1 | cat /dev/null");

	fail_if(system("cp "DUMMY_TEST_FILE" " TEST_FILE) != 0,
	    "failed to setup file");
	fail_if(system("mkdir " TEST_FOLDER) != 0, "failed to create folder");

	for (size_t i = 0; i < sizeof(test_matrix) / sizeof(test_matrix[0]); ++i) {
		struct test_setup &ts = test_matrix[i];

		etag_verify(ts.method, ts.file, ts.param, "", "",
		    ts.if_match, ts.if_none_match, ts.type,
		    ts.expect_status, ts.expect_etag);
	}
}
