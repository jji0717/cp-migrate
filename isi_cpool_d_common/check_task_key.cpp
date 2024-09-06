#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "operation_type.h"

#include "task_key.h"

SUITE_DEFINE_FOR_FILE(check_task_key,
	.mem_check = CK_MEM_LEAKS,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST(test_equal_file_task_keys)
{
	file_task_key key_1(OT_ARCHIVE, 123, 456);
	file_task_key key_2(OT_ARCHIVE, 123, 456);

	fail_if (!key_1.equals(&key_2), "key_1 (%{}) key_2 (%{})",
	    cpool_task_key_fmt(&key_1), cpool_task_key_fmt(&key_2));
}

TEST(test_unequal_file_task_keys__op_type)
{
	file_task_key key_1(OT_ARCHIVE, 123, 456);
	file_task_key key_2(OT_RECALL, 123, 456);

	fail_if (key_1.equals(&key_2), "key_1 (%{}) key_2 (%{})",
	    cpool_task_key_fmt(&key_1), cpool_task_key_fmt(&key_2));
}

TEST(test_unequal_file_task_keys__lin)
{
	file_task_key key_1(OT_ARCHIVE, 123, 456);
	file_task_key key_2(OT_ARCHIVE, 321, 456);

	fail_if (key_1.equals(&key_2), "key_1 (%{}) key_2 (%{})",
	    cpool_task_key_fmt(&key_1), cpool_task_key_fmt(&key_2));
}

TEST(test_unequal_file_task_keys__snapid)
{
	file_task_key key_1(OT_ARCHIVE, 123, 456);
	file_task_key key_2(OT_ARCHIVE, 123, 654);

	fail_if (key_1.equals(&key_2), "key_1 (%{}) key_2 (%{})",
	    cpool_task_key_fmt(&key_1), cpool_task_key_fmt(&key_2));
}
