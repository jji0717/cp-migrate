#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "report_writer.h"

#define TEST_REPORT_DIR "/tmp/nonexistingpath/"
#define TEST_REPORT_FILE \
    TEST_REPORT_DIR "check_report_writer.xml"

TEST_FIXTURE(setup_test)
{
	unlink(TEST_REPORT_FILE);
	rmdir(TEST_REPORT_DIR);

	// Note TEST_REPORT_DIR is explicitly removed above.  It should NOT
	// exist prior to test execution.  It should be created at runtime by
	// report_writer.  If the test fails because of a missing directory,
	// there is a real product issue, not a test setup issue.
}

TEST_FIXTURE(teardown_test)
{
	//unlink(TEST_REPORT_FILE);
	//rmdir(TEST_REPORT_DIR);
}

SUITE_DEFINE_FOR_FILE(check_report_writer,
	.mem_check = CK_MEM_LEAKS,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST(smoke_report_writer)
{
	struct isi_error *error = NULL;

	report_writer *writer = report_writer::create_report_writer(
	    TEST_REPORT_FILE, &error);
	fail_if(error != NULL, "Failed to create report writer: %#{}",
	    isi_error_fmt(error));
	fail_if(writer == NULL, "NULL writer returned without error");

	writer->write_member("test_key", "test_value", &error);
	fail_if(error != NULL, "Failed to write value: %#{}",
	    isi_error_fmt(error));

	writer->start_member("test_container", &error);
	fail_if(error != NULL, "Failed to start member: %#{}",
	    isi_error_fmt(error));

	writer->start_member("test_sub_container", &error);
	fail_if(error != NULL, "Failed to start member: %#{}",
	    isi_error_fmt(error));

	writer->write_member("test_key", "test_value", &error);
	fail_if(error != NULL, "Failed to write value: %#{}",
	    isi_error_fmt(error));

	writer->end_member(&error);
	fail_if(error != NULL, "Failed to end member: %#{}",
	    isi_error_fmt(error));

	writer->end_member(&error);
	fail_if(error != NULL, "Failed to end member: %#{}",
	    isi_error_fmt(error));

	writer->complete_report(&error);
	fail_if(error != NULL, "Failed to complete report: %#{}",
	    isi_error_fmt(error));

	delete writer;
}

TEST(encode_string)
{
	char original_string [] = "this\nis\na\ntest";
	const char expected_string [] = "this\\nis\\na\\ntest";
	char string_buffer [64];

	report_writer::encode_string(original_string, string_buffer);
	fail_unless(strcmp(string_buffer, expected_string) == 0,
	    "Failed to match encoded_string: %s != %s", string_buffer,
	    expected_string);
}