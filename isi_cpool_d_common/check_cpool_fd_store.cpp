#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_fd_store.h"

/*
 * Define a TEST_WITH_SIGNAL macro.  This was copied from src/ports/devel/
 * libcheck/work/check-0.9.2/src/check.h and then modified to take a signal
 * parameter.
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
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_cpool_fd_store,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	/*
	 * Prevent false positive memory leak messages from statically
	 * allocated variables by creating them before the memory bookkeeping
	 * starts.
	 */
	cpool_fd_store::getInstance();
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
}

#define DEL_PRIORITY_MAX 50
TEST_FIXTURE(setup_test)
{
	/*
	 * Delete SBTs for all task types up to priority DEL_PRIORITY_MAX.
	 */
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = NULL;
	char *name_buf = NULL;

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < DEL_PRIORITY_MAX; ++pri) {
			name_buf = get_pending_sbt_name(op_type->get_type(),
			    pri);
			delete_sbt(name_buf, &error);
			fail_if (error != NULL,
			    "failed to delete SBT (%s): %{}",
			    name_buf, isi_error_fmt(error));
			free(name_buf);
		}

		name_buf = get_inprogress_sbt_name(op_type->get_type());
		delete_sbt(name_buf, &error);
		fail_if (error != NULL,
		    "failed to delete SBT (%s): %{}",
		    name_buf, isi_error_fmt(error));
		free(name_buf);
	}
}

/*
 * Attempt to retrieve a non-existent pending SBT file descriptor without the
 * ability to create it.
 *
 * The 4 leaked bytes are a result of allocating space for the priority 0 file
 * descriptor and using a static instance that isn't deallocated.
 */
TEST(test__get_pending__non_existent__no_create, CK_MEM_LEAKS, 4)
{
	struct isi_error *error = NULL;
	int fd = -3;

	fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(CPOOL_TASK_TYPE_ARCHIVE, 0, false, &error);
	fail_if (error == NULL);
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if (fd != -1, "fd: %d", fd);
	isi_error_free(error);
}

/*
 * Attempt to incrementally retrieve non-existent pending SBT file descriptors.
 */
TEST(test__get_pending__increment, CK_MEM_LEAKS, 40)
{
	struct isi_error *error = NULL;
	int fd;

	for (int i = 0; i < 10; ++i) {
		fd = cpool_fd_store::getInstance().\
		    get_pending_sbt_fd(CPOOL_TASK_TYPE_CLOUD_GC, i, false,
		    &error);
		fail_if (error == NULL, "expected ENOENT");
		fail_if (!isi_system_error_is_a(error, ENOENT),
		    "expected ENOENT: %#{}", isi_error_fmt(error));
		fail_if (fd != -1, "fd: %d", fd);
		isi_error_free(error);
		error = NULL;
	}
}

/*
 * Retrieve a pending SBT files descriptor, then retrieve it again.
 */
TEST(test__get_pending__non_existing__create__repeat, CK_MEM_LEAKS, 4)
{
	struct isi_error *error = NULL;
	int fd;

	fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(CPOOL_TASK_TYPE_RECALL, 0, true, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (fd == -1);

	fd = cpool_fd_store::getInstance().\
	    get_pending_sbt_fd(CPOOL_TASK_TYPE_RECALL, 0, false, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));
	fail_if (fd == -1);
}
