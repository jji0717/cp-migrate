#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "daemon_job_configuration.h"
#include "daemon_job_manager.h"
#include "daemon_job_member.h"
#include "daemon_job_record.h"
#include "task_key.h"

#include "scoped_object_list.h"

using namespace isi_cloud;

SUITE_DEFINE_FOR_FILE(check_scoped_object_list,
	.mem_check = CK_MEM_LEAKS,
	.timeout = 0,
	.fixture_timeout = 1200);

/*
 * Allocate/lock objects and open SBTs, add them to a scoped_object_list, and
 * see that this test does not leak memory.
 */
TEST(test_scoped_object_list, CK_MEM_LEAKS, 0)
{
	scoped_object_list sol;

	struct isi_error *error = NULL;
	const void *serialization;
	size_t serialization_size;
	const cpool_operation_type *op_type = OT_CLOUD_GC;
	djob_id_t djob_id = op_type->get_single_job_id();

	daemon_job_record *djob_record = new daemon_job_record;
	djob_record->get_serialization(serialization, serialization_size);
	sol.add_daemon_job_record(djob_record);

	daemon_job_configuration *djob_config = new daemon_job_configuration;
	djob_config->get_serialization(serialization, serialization_size);
	sol.add_daemon_job_config(djob_config);

	file_task_key file_key(OT_ARCHIVE, 12345, 54321);
	daemon_job_member *djob_member = new daemon_job_member(&file_key);
	djob_member->get_serialization(serialization, serialization_size);
	sol.add_daemon_job_member(djob_member);

	daemon_job_manager::lock_daemon_job_record(djob_id, true, true,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	sol.add_locked_daemon_job_record(djob_id);

	int member_sbt_fd = daemon_job_manager::open_member_sbt(djob_id, true,
	    &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	sol.add_daemon_job_member_sbt(member_sbt_fd);

	daemon_job_manager::lock_daemon_job_config(true, true, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	sol.add_locked_daemon_job_configuration();
}
