#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_object_api/check_create_dummy_file.hpp>

#include "daemon_job_record.h"
#include "daemon_job_request.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(check_daemon_job_record,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .timeout = 0,
    .fixture_timeout = 1200);


TEST_FIXTURE(suite_setup)
{
	create_dummy_test_file();
}

TEST_FIXTURE(suite_teardown)
{
	remove_dummy_test_file();
}

TEST(test_serialize_deserialize_no_desc)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id = 9;
	const cpool_operation_type *op_type = OT_RECALL;
	djob_op_job_task_state state = DJOB_OJT_RUNNING;

	daemon_job_record djob_rec_from, djob_rec_to;
	djob_rec_from.set_daemon_job_id(djob_id);
	djob_rec_from.set_operation_type(op_type);
	djob_rec_from.set_state(state);

	const void *serialized;
	size_t serialized_size;
	djob_rec_from.get_serialization(serialized, serialized_size);

	djob_rec_to.set_serialization(serialized, serialized_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_rec_to.get_daemon_job_id() != djob_id,
	    "(exp: %d act: %d)", djob_id, djob_rec_to.get_daemon_job_id());
	fail_if (djob_rec_to.get_operation_type() != op_type,
	    "(exp: %{} act: %{})", task_type_fmt(op_type->get_type()),
	    task_type_fmt(djob_rec_to.get_operation_type()->get_type()));
	fail_if (djob_rec_to.get_state() != state,
	    "(exp: %p act: %p)", state, djob_rec_to.get_state());
}

TEST(test_serialize_deserialize_desc)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id = 9;
	const cpool_operation_type *op_type = OT_WRITEBACK;
	djob_op_job_task_state state = DJOB_OJT_RUNNING;
	const char *description = "this is a writeback job";

	daemon_job_record djob_rec_from, djob_rec_to;
	djob_rec_from.set_daemon_job_id(djob_id);
	djob_rec_from.set_operation_type(op_type);
	djob_rec_from.set_state(state);
	djob_rec_from.set_description(description);

	const void *serialized;
	size_t serialized_size;
	djob_rec_from.get_serialization(serialized, serialized_size);

	djob_rec_to.set_serialization(serialized, serialized_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_rec_to.get_daemon_job_id() != djob_id,
	    "(exp: %d act: %d)", djob_id, djob_rec_to.get_daemon_job_id());
	fail_if (djob_rec_to.get_operation_type() != op_type,
	    "(exp: %{} act: %{})", task_type_fmt(op_type->get_type()),
	    task_type_fmt(djob_rec_to.get_operation_type()->get_type()));
	fail_if (djob_rec_to.get_state() != state,
	    "(exp: %p act: %p)", state, djob_rec_to.get_state());
	fail_if (strcmp(djob_rec_to.get_description(), description) != 0,
	    "(exp: <%s> act: <%s>)", description,
	    djob_rec_to.get_description());
}

TEST(test_manual_archive_recall_interface)
{
	struct isi_error *error = NULL;
	daemon_job_request req;

	req.set_type(OT_ARCHIVE);

	// Verify add a "real" file
	const char test_file_1 [] =  DUMMY_TEST_FILE;
	req.add_file(test_file_1, &error);
	fail_if(error, "Error adding file \"%s\": %{}", test_file_1,
	    isi_error_fmt(error));

	// Negative - add directory
	const char test_file_2 [] = "/ifs/.ifsvar";
	req.add_file(test_file_2, &error);
	fail_unless(error != NULL, "Should not be able to directory, \"%s\"",
	    test_file_2);
	isi_error_free(error);
	error = NULL;

	// Negative - add missing file
	const char test_file_3 [] =
	    "/ifs/some_bogus_filename_that_does_not_exist";
	req.add_file(test_file_3, &error);
	fail_unless(error != NULL,
	    "Should not be able to add bogus file, \"%s\"", test_file_3);
	isi_error_free(error);
	error = NULL;

	// Verify add a "real" directory
	const char test_dir_1 [] = "/ifs/.ifsvar";
	req.add_directory(test_dir_1, &error);
	fail_if(error, "Error adding dir \"%s\": %{}", test_dir_1,
	    isi_error_fmt(error));

	// Negative - add file
	const char test_dir_2 [] =  DUMMY_TEST_FILE;
	req.add_directory(test_dir_2, &error);
	fail_unless(error != NULL,
	    "Should not be able to file, \"%s\"", test_dir_2);
	isi_error_free(error);
	error = NULL;

	// Negative - add missing dir
	const char test_dir_3 [] =
	    "/ifs/some_bogus_directory_that_does_not_exist";
	req.add_directory(test_dir_3, &error);
	fail_unless(error != NULL,
	    "Should not be able to add bogus directory, \"%s\"", test_dir_3);
	isi_error_free(error);
	error = NULL;
}
