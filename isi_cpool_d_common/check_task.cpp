#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_cloud_common/isi_cpool_version.h>

#include "operation_type.h"

#include "cpool_d_common.h"
#include "task.h"

/*
 * Define a TEST_WITH_SIGNAL macro.  This was copied from src/ports/devel/
 * libcheck/work/check-0.9.2/src/check.h and then modified to take a signal
 * parameter.
 *
 * (Note: this is no longer used by this unit test suite, but is being kept for
 * easy future lookup.)
 */
#define TEST_WITH_SIGNAL(__f, __signal, __init...)		\
	static void __f(void *context);				\
	static void __f ## _wrapped(void *context)		\
	{							\
	        tcase_fn_start(# __f, __FILE__, __LINE__);	\
	        __f(context);					\
	}							\
	static struct check_test_def test_func_ ## __f = {	\
	        magic  : CHECK_TEST_DEF_MAGIC,			\
	        name   : # __f,					\
	        func   : __f ## _wrapped,			\
	        signal : __signal,				\
	        suite_def_for_file : &SUITE_FILE,		\
	        __init						\
	};							\
	TEST_SET_ADD(test_func_ ## __f);			\
	static void __f(void *context __unused)

TEST_FIXTURE(setup_suite);

SUITE_DEFINE_FOR_FILE(check_task,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	/* to avoid memory leak messages resulting from static allocations */
	isi_cloud_object_id object_id;
}

TEST(test_local_gc_task_serialize_deserialize)
{
	struct isi_error *error = NULL;
	isi_cloud::local_gc_task from, to;

	isi_cloud_object_id object_id(246, 357);
	object_id.set_snapid(258);

	const void *serialized;
	size_t serialized_size;

	from.set_lin(12345);
	from.set_snapid(87654);
	from.set_object_id(object_id);
	struct timeval date_of_death_tv = { 4, 100 };
	from.set_date_of_death(date_of_death_tv);
	from.get_serialized_task(serialized, serialized_size);

	to.set_serialized_task(serialized, serialized_size, &error);
	fail_if(error != NULL, "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(to.get_lin() != 12345,
	    "lin mismatch (exp: 12345 act: %{})", lin_fmt(to.get_lin()));
	fail_if(to.get_snapid() != 87654,
	    "lin mismatch (exp: 87654 act: %{})", snapid_fmt(to.get_snapid()));
	fail_if(to.get_object_id() != object_id,
	    "object_id mismatch (exp: %s act: %s)", object_id.to_c_string(),
	    to.get_object_id().to_c_string());
	fail_if(to.get_date_of_death().tv_sec != 4 ||
			to.get_date_of_death().tv_usec != 100,
	    "date of death mismatch (exp: sec 4 usec 100 act: sec %d usec %d)",
		to.get_date_of_death().tv_sec, to.get_date_of_death().tv_usec);
}

class archive_task_ut : public isi_cloud::archive_task {
public:
	archive_task_ut() { }
	~archive_task_ut() { }
	void set_version(int version) {
		version_ = version;
	}
};

static void
test_archive_task_serialize_deserialize__helper__create_tasks(
    archive_task_ut *&src_task,
    archive_task_ut *&dst_task,
    int task_version)
{
	fail_if (src_task != NULL);
	fail_if (dst_task != NULL);

	src_task = new archive_task_ut();
	fail_if (src_task == NULL);

	src_task->set_version(task_version);
	src_task->set_lin(12345);
	src_task->set_snapid(54321);

	if (src_task->has_policyid()) {
		src_task->set_policy_id(11111);
	} else {
		src_task->set_policy_name("aPolicy");
	}

	dst_task = new archive_task_ut();
	fail_if (dst_task == NULL);

}

static void
test_task_serialize_deserialize__archive(int expected_task_version)
{
	struct isi_error *error = NULL;

	archive_task_ut *src_task = NULL, *dst_task = NULL;
	const void *serialized_task;
	size_t serialized_task_size;

	test_archive_task_serialize_deserialize__helper__create_tasks(
	    src_task, dst_task, expected_task_version);

	src_task->get_serialized_task(serialized_task, serialized_task_size);

	fail_if (src_task->get_version() != expected_task_version,
	    "task version mismatch (exp: %d act: %d)",
	    expected_task_version, src_task->get_version());

	/* Deserialize - verify destination task version. */
	dst_task->set_serialized_task(serialized_task, serialized_task_size,
	    &error);
	fail_unless (error == NULL,
	    "unexpected error: %#{}", isi_error_fmt(error));
	fail_if (dst_task->get_version() != expected_task_version,
	    "task version mismatch (exp: %d act: %d)",
	    expected_task_version, dst_task->get_version());

	if (dst_task->has_policyid()) {
		fail_if (dst_task->get_policy_id() != src_task->get_policy_id(),
		    "policy id mismatch (src: %d dst: %d)",
		    src_task->get_policy_id(), dst_task->get_policy_id());
	} else {
		fail_if (strcmp (dst_task->get_policy_name(),src_task->get_policy_name()) != 0,
		    "policy name mismatch (src: %s dst: %s)",
		    src_task->get_policy_name(), dst_task->get_policy_name());
	}

	delete src_task;
	delete dst_task;
}

TEST(test_task_serialize_deserialize__archive_v1)
{
	test_task_serialize_deserialize__archive(CPOOL_ARCHIVE_TASK_V1);
}

TEST(test_task_serialize_deserialize__archive_v2)
{
	test_task_serialize_deserialize__archive(CPOOL_ARCHIVE_TASK_V2);
}

