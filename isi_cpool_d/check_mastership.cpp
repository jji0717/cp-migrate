#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "thread_mastership.h"
#include "thread_pool.h"

#define MASTERSHIP_LOCKFILE "cpool_d_unit_test_lock_file"
#define MASTERSHIP_MASTERFILE "cpool_d_unit_test_master_file"

class ut_thread_pool : public thread_pool {
public:
	void test_wait_for_mastership(const char *lockfile_name,
	    const char *masterfile_name, int sleep_interval)
	{
		wait_for_mastership(lockfile_name, masterfile_name,
		    sleep_interval);
	};

	void test_cleanup_mastership()
	{
		cleanup_mastership();
	};

	bool has_mastership()
	{
		return mastership_lock_fd_ != -1;
	};
};

TEST_FIXTURE(setup_suite)
{
	/*
	 * Pre-create a ut_thread_pool to avoid false positive memory leaks
	 * around some of the pthread stuff
	 */
	ut_thread_pool uttp;
}

SUITE_DEFINE_FOR_FILE(check_mastership,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST(test_acquire_mastership)
{
	/* Should be no reason for a testpoint to fail acquisition */
	int master_fd = -1;

	bool acquired = acquire_mastership(&master_fd,
	    CPOOL_LOCKS_DIRECTORY MASTERSHIP_LOCKFILE,
	    CPOOL_LOCKS_DIRECTORY MASTERSHIP_MASTERFILE);
	fail_if(!acquired || master_fd < 0, "Failed to acquire mastership");

	close(master_fd);
}

TEST(test_mastership_thread_pool)
{
	const int wait_time = 1;
	ut_thread_pool *tp = new ut_thread_pool();

	/* Test get mastership */
	tp->test_wait_for_mastership(MASTERSHIP_LOCKFILE, MASTERSHIP_MASTERFILE,
	    wait_time);
	fail_unless(tp->has_mastership(), "tp should have mastership");

	tp->test_cleanup_mastership();
	fail_if(tp->has_mastership(), "tp should no longer have mastership");

	/* Test get mastership again to make sure destructor will release it */
	tp->test_wait_for_mastership(MASTERSHIP_LOCKFILE, MASTERSHIP_MASTERFILE,
	    wait_time);
	fail_unless(tp->has_mastership(), "tp should have mastership");

	delete tp;

	/* Verify that a new instance of tp can get mastership */
	tp = new ut_thread_pool();
	tp->test_wait_for_mastership(MASTERSHIP_LOCKFILE, MASTERSHIP_MASTERFILE,
	    wait_time);
	fail_unless(tp->has_mastership(), "tp should have mastership");

	delete tp;
}
