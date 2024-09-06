#include <sys/stat.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat.h"
#include "cpool_stat_key.h"

TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_cpool_stat,
    .mem_check = CK_MEM_LEAKS,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 0,
    .fixture_timeout = 1200);

TEST_FIXTURE(setup_test)
{
	// intentionally empty
}

TEST_FIXTURE(teardown_test)
{
	// intentionally empty
}

TEST(test_account_stat_key)
{
	cpool_stat_account_key low_acct_key(1);
	cpool_stat_account_key high_acct_key(2);

	fail_unless(low_acct_key.compare(high_acct_key) < 0,
	    "Low key should be less than high key");
	fail_unless(high_acct_key.compare(low_acct_key) > 0,
	    "High key should be greater than low key");
	fail_unless(low_acct_key.compare(low_acct_key) == 0,
	    "Low key should be equal to itself");

	cpool_stat_account_query_result_key low_qrt(1, 1);
	cpool_stat_account_query_result_key high_qrt(1, 2);

	fail_unless(low_qrt.compare(high_qrt) < 0,
	    "Low key should be less than high key");
	fail_unless(high_qrt.compare(low_qrt) > 0,
	    "High key should be greater than low key");
	fail_unless(low_qrt.compare(low_qrt) == 0,
	    "Low key should be equal to itself");
}
