#include "object_handler.h"
#include <check.h>
#include <isi_rest_server/api_error.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_util_cpp/check_leak_primer.hpp>
#include "handler_common.h"

TEST_FIXTURE(suite_setup);

SUITE_DEFINE_FOR_FILE(check_object_handler,
    .mem_check = CK_MEM_DEFAULT,
    .dmalloc_opts =  CHECK_DMALLOC_DEFAULT,
    .suite_setup = suite_setup);

TEST_FIXTURE(suite_setup)
{
	// to help reduce the bogus memory leak.
	std::string astr = "hello";
}

/**
 * Check a valid object name
 */
TEST(test_validate_object_name_1, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	std::string bucket_name = "bucket1";
	std::string obj_name = "object1";
	object_handler::validate_object_name(bucket_name, obj_name);
}

/**
 * invalid object name with a .
 */

TEST(test_validate_object_name_2,.mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	std::string bucket_name = "bucket1";
	std::string obj_name = ".";

	// If the test throws an exception pass the test
	fail_unless_throws_any(object_handler::validate_object_name(bucket_name,
	    obj_name));
}

/**
 * invalid object name with a less than 1 character
 */
TEST(test_validate_object_name_3, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	std::string bucket_name = "bucket1";
	std::string obj_name = "";

	// If the test throws an exception pass the test
	fail_unless_throws_any(object_handler::validate_object_name(bucket_name,
	    obj_name));
}

