#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>

#include "operation_type.h"
#include "task_key.h"

#include "daemon_job_member.h"

SUITE_DEFINE_FOR_FILE(check_daemon_job_member,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 0,
    .fixture_timeout = 1200);

TEST(test_serialize_deserialize_no_name)
{
	struct isi_error *error = NULL;
	file_task_key key(OT_RECALL, 456, 234);
	daemon_job_member from(&key), to(&key);

	const void *serialized;
	size_t serialized_size;
	from.get_serialization(serialized, serialized_size);

	to.set_serialization(serialized, serialized_size, &error);

	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
}

TEST(test_serialize_deserialize_desc_name)
{
	struct isi_error *error = NULL;
	file_task_key key(OT_RECALL, 456, 234);
	daemon_job_member from(&key), to(&key);
	from.set_name("hello!");

	const void *serialized;
	size_t serialized_size;
	from.get_serialization(serialized, serialized_size);

	to.set_serialization(serialized, serialized_size, &error);

	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (strcmp(to.get_name(), from.get_name()) != 0,
	    "name mismatch (exp: %s act: %s)", from.get_name(), to.get_name());
}
