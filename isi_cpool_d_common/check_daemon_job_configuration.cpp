#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "daemon_job_configuration.h"

SUITE_DEFINE_FOR_FILE(check_daemon_job_configuration,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 0,
    .fixture_timeout = 1200);

class daemon_job_configuration_ut : public daemon_job_configuration
{
public:
	void set_num_active_daemon_jobs(int num_jobs)
	{
		num_active_daemon_jobs_ = num_jobs;
		serialization_up_to_date_ = false;
	}
};

TEST(test_serialize_deserialize)
{
	struct isi_error *error = NULL;
	daemon_job_configuration_ut from_config, to_config;

	int num_active_daemon_jobs = 42;
	djob_id_t last_used_djob_id = 14;

	from_config.set_num_active_daemon_jobs(num_active_daemon_jobs);
	from_config.set_last_used_daemon_job_id(last_used_djob_id);
	from_config.set_operation_state(OT_RECALL, DJOB_OJT_PAUSED);
	from_config.set_operation_state(OT_WRITEBACK, DJOB_OJT_PAUSED);

	const void *serialized;
	size_t serialized_size;
	from_config.get_serialization(serialized, serialized_size);

	to_config.set_serialization(serialized, serialized_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (to_config.get_num_active_jobs() != num_active_daemon_jobs,
	    "(exp: %d act: %d)", num_active_daemon_jobs,
	    to_config.get_num_active_jobs());
	fail_if (to_config.get_last_used_daemon_job_id() != last_used_djob_id,
	    "(exp: %d act: %d)", last_used_djob_id,
	    to_config.get_last_used_daemon_job_id());

	djob_op_job_task_state op_state;

	op_state = to_config.get_operation_state(OT_ARCHIVE);
	fail_if (op_state != DJOB_OJT_RUNNING, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(op_state));

	op_state = to_config.get_operation_state(OT_RECALL);
	fail_if (op_state != DJOB_OJT_PAUSED, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(op_state));

	op_state = to_config.get_operation_state(OT_WRITEBACK);
	fail_if (op_state != DJOB_OJT_PAUSED, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_PAUSED),
	    djob_op_job_task_state_fmt(op_state));

	op_state = to_config.get_operation_state(OT_CACHE_INVALIDATION);
	fail_if (op_state != DJOB_OJT_RUNNING, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(op_state));

	op_state = to_config.get_operation_state(OT_LOCAL_GC);
	fail_if (op_state != DJOB_OJT_RUNNING, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(op_state));

	op_state = to_config.get_operation_state(OT_CLOUD_GC);
	fail_if (op_state != DJOB_OJT_RUNNING, "(exp: %{} act: %{})",
	    djob_op_job_task_state_fmt(DJOB_OJT_RUNNING),
	    djob_op_job_task_state_fmt(op_state));
}
