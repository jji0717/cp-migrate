/* XXX:FAIL this has to come before anything that #defines fail */
#include <isi_util_cpp/check_leak_primer.hpp>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "api_error.h"

SUITE_DEFINE_FOR_FILE(api_error);


TEST(api_error_basic)
{
	api_exception e(AEC_BAD_REQUEST, "msg");

	fail_unless(e.error_code() == AEC_BAD_REQUEST);

	fail_unless(e.what() != NULL);

	fail_unless(strcmp(api_status_code_str(ASC_OK), "Ok") == 0);

	fail_unless(strcmp(api_status_code_str((api_status_code)-1), "") == 0);
}

TEST(api_error_both_error_and_msg)
{
	api_exception e((isi_error *)0, AEC_BAD_REQUEST, "err %02d", 2);

	fail_unless(e.error_code() == AEC_BAD_REQUEST);

	fail_unless(strcmp(e.what(), "err 02") == 0);
}

TEST(api_error_throw_on_err)
{
	try {
		api_exception::throw_on_error(0);
	}
	catch (...) {
		fail(0);
	}
	// ?? this leaks ?
	try {
		api_exception::throw_on_error(
		    isi_system_error_new(7, "system"));
		fail(0);
	}
	catch (...) {
	}

	try {
		api_exception::throw_on_error(
		    isi_system_error_new(7, "system"));
		fail(0);
	}
	catch (...) {
	}

	try {
		throw api_exception(
		    isi_system_error_new(7, "system"),
		    AEC_FORBIDDEN,
		    "some work");
		fail(0);
	}
	catch (api_exception &e) {
		fail_unless(strcmp(e.what(), "some work") == 0);
	}

}
