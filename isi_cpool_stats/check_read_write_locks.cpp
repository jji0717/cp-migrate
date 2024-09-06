#include <sys/stat.h>
#include <pthread.h>
#include <thread>

#include <isi_util_cpp/check_leak_primer.hpp>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat_read_write_lock.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_read_write_locks,
    //.mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .suite_teardown = teardown_suite,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 90,
    .fixture_timeout = 1200);

#define test_read_write_lock_name "test_cpool_read_write_lock"

TEST_FIXTURE(setup_suite)
{
	//check_leak_primer l;
}

TEST_FIXTURE(teardown_suite)
{
}

TEST_FIXTURE(setup_test)
{
}

TEST_FIXTURE(teardown_test)
{
}

TEST(test_read_write_read_lock)
{
	struct isi_error *error = NULL;
	cpool_stat_read_write_lock lock(test_read_write_lock_name);
	//cpool_stat_read_write_lock writelck(test_read_write_lock_name);
	lock.lock_for_read(&error);
	fail_if (error != NULL, "Failed to take read lock: %#{}",
	    isi_error_fmt(error));
	//writelck.lock_for_read(&error);

	lock.unlock(&error);
	fail_if(error != NULL, "Failed to release lock: %#{}",
	    isi_error_fmt(error));
}

TEST(test_read_write_write_lock)
{
	struct isi_error *error = NULL;
	cpool_stat_read_write_lock lock(test_read_write_lock_name);

	lock.lock_for_write(&error);
	fail_if (error != NULL, "Failed to take write lock: %#{}",
	    isi_error_fmt(error));

	lock.unlock(&error);
	fail_if(error != NULL, "Failed to release lock: %#{}",
	    isi_error_fmt(error));
}

static void
take_many_read_and_write_locks_(int count)
{
	ASSERT(count > 0);

	struct isi_error *error = NULL;

	for (int i = 0; i < count; i++) {
		cpool_stat_read_write_lock read_lock(test_read_write_lock_name);
		read_lock.lock_for_read(&error);
		fail_if(error != NULL, "Failed to take read lock: %#{}",
		    isi_error_fmt(error));

		read_lock.unlock(&error);
		fail_if(error != NULL, "Failed to release read lock: %#{}",
		    isi_error_fmt(error));

		cpool_stat_read_write_lock write_lock(
		    test_read_write_lock_name);
		read_lock.lock_for_write(&error);
		fail_if(error != NULL, "Failed to take write lock: %#{}",
		    isi_error_fmt(error));
		// Release the write lock by letting it fall off the stack
	}
}
// static void take_read_and_write_locks(int cnt)
// {
// 	struct isi_error *error = NULL;
// 	cpool_stat_read_write_lock write_lock(test_read_write_lock_name);
// }
static void *take_lck_p1(void *cnt)
{
	//::sleep(1);
	struct isi_error *error = NULL;
	cpool_stat_read_write_lock write_lock(test_read_write_lock_name);
	//::sleep(2);
	printf("%s called try to lock for read\n", __func__);
	write_lock.lock_for_read(&error);
	printf("%s called tid:%p\n", __func__, pthread_self());
	write_lock.unlock(&error);
	return NULL;
}
static void *take_lck_p2(void *cnt)
{
	struct isi_error *error = NULL;
	cpool_stat_read_write_lock write_lock(test_read_write_lock_name);
	::sleep(1);
	printf("%s called try to lock for write\n", __func__);
	write_lock.lock_for_write(&error);
	fail_if(error != NULL, "Failed to lock for write: %{}",isi_error_fmt(error));
	printf("%s called tid:%p\n", __func__, pthread_self());
	write_lock.unlock(&error);
	return NULL;
}
static void *take_lck_p3(void *cnt)
{
	struct isi_error *error = NULL;
	cpool_stat_read_write_lock write_lock(test_read_write_lock_name);
	write_lock.lock_for_write(&error);
	printf("%s called tid:%p\n", __func__, pthread_self());
	return NULL;
}
static void*
execute_pthread_for_multithread_test_(void *args)
{
	take_many_read_and_write_locks_(10);
	::pthread_exit(0);
}
TEST(test_write_lock_multiple_threads_jji)
{
	pthread_t thread_id[3];
	int err = ::pthread_create(&thread_id[0], NULL,take_lck_p1,NULL);
	//::sleep(1);
	err = ::pthread_create(&thread_id[1],NULL,take_lck_p2,NULL);
	//err = ::pthread_create(&thread_id[2],NULL,take_lck_p3,NULL);
	// for (int i = 0;i<2;i++)
	// 	::pthread_detach(thread_id[i]);
	// for (int i = 0;i<2;i++)
	// 	::pthread_join(thread_id[i], NULL);
 	pthread_join(thread_id[0], NULL);
 	// pthread_join(thread_id[1],NULL);
	//::sleep(10);
}
/*
 * Note* Disabling memcheck here because of false positives from pthread init
 */
TEST(test_read_write_lock_multiple_threads, mem_check : CK_MEM_DISABLE)
{
	const int num_threads_per_test = 10;

	pthread_t thread_id[num_threads_per_test];

	for (int i = 0; i < num_threads_per_test; i++) {
		int err = ::pthread_create(&thread_id[i], NULL,
		    execute_pthread_for_multithread_test_, NULL);
		fail_if(err != 0, "Error in creating test thread: %d", err);
	}
	for (int i = 0; i < num_threads_per_test; i++) {
		int err = ::pthread_join(thread_id[i], NULL);
		fail_if(err != 0, "Error in joining test thread: %d", err);
	}
}

TEST(test_read_write_lock_multiple_processes)
{
	// Create a child process
	pid_t child_pid = fork();
	fail_if(child_pid < 0);

	// In each process, take a series of read locks and write locks
	take_many_read_and_write_locks_(10);

	if (child_pid == 0)
		exit(0);
}

TEST(test_negative_take_read_write_lock_multiple_times)
{
	struct isi_error *error = NULL;

	/* Test taking a read lock (should succeed) */
	cpool_stat_read_write_lock read_lock(test_read_write_lock_name);
	read_lock.lock_for_read(&error);
	fail_if(error != NULL, "Failed to take read lock: %#{}",
	    isi_error_fmt(error));

	/* Try to take read lock again (should fail) */
	read_lock.lock_for_read(&error);
	fail_unless(error != NULL, "Should have error taking lock twice");

	isi_error_free(error);
	error = NULL;

	read_lock.unlock(&error);
	fail_if(error != NULL, "Failed to release read lock: %#{}",
	    isi_error_fmt(error));
}
