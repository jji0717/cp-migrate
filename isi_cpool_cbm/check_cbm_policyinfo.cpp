#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>

#include <check.h>

#include "isi_cbm_test_util.h"
#include "isi_cbm_policyinfo.h"

#include "isi_cbm_test_util.h"

TEST_FIXTURE(suite_setup)
{
	ASSERT (cbm_test_cpool_ufp_init(),
	    "failed to initialize user failpoints");
}

SUITE_DEFINE_FOR_FILE(check_cbm_policyinfo,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = NULL,
    .test_setup = NULL,
    .test_teardown = NULL,
    .timeout = 0,
    .fixture_timeout = 1200);

static void
check_open_helper(struct isi_cfm_policy_provider *ppi,
    const char *failpoint_name)
{
	ASSERT(ppi != NULL);
	// failpoint_name can be NULL

	struct isi_error *error = NULL;
	struct ufp *ufp = NULL;

	/* If a failpoint was supplied, turn it on before opening. */
	if (failpoint_name != NULL) {
		ufp_globals_rd_lock();
		ufp = ufail_point_lookup(failpoint_name);
		ufp_globals_rd_unlock();
		fail_if (ufp == NULL);

		ufp_globals_wr_lock();
		int ret = ufail_point_set(ufp, "return(1)");
		ufp_globals_wr_unlock();
		fail_if (ret != 0, "%s", strerror(ret));
	}

	isi_cfm_open_policy_provider_info(ppi, &error);

	if (failpoint_name != NULL) {
		fail_if (error == NULL);
		fail_unless (isi_system_error_is_a(error, ENOTSOCK),
		    "non-injected error opening PPI: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;

		fail_unless (ppi->proxy_context == NULL);
		fail_unless (ppi->cp_context == NULL);
		fail_unless (ppi->sp_context == NULL);
	}
	else {
		fail_if (error != NULL,
		    "non-injected error opening PPI: %#{}",
		    isi_error_fmt(error));

		fail_if (ppi->proxy_context == NULL);
		fail_if (ppi->cp_context == NULL);
		fail_if (ppi->sp_context == NULL);
	}

	if (ufp != NULL) {
		ufp_globals_wr_lock();
		ufail_point_destroy(ufp);
		ufp_globals_wr_unlock();
	}
}

/*
 * Test isi_cfm_open_policy_provider_info without injecting any failures.
 */
TEST(check_open_ppi__succeed)
{
	struct isi_error *error = NULL;
	struct isi_cfm_policy_provider ppi;

	check_open_helper(&ppi, NULL);

	/*
	 * Since the open succeeded, close the PPI manually.  (An unsuccessful
	 * open triggers a close automatically.)
	 */
	isi_cfm_close_policy_provider_info(&ppi, &error);
	fail_unless (error == NULL,
	    "failed to close successfully opened PPI: %#{}",
	    isi_error_fmt(error));

	fail_unless (ppi.sp_context == NULL);
	fail_unless (ppi.cp_context == NULL);
	fail_unless (ppi.proxy_context == NULL);
}

/*
 * Test isi_cfm_open_policy_provider_info and inject a failure in opening the
 * smartpools context.
 */
TEST(check_open_ppi__sp_open_fail)
{
	struct isi_cfm_policy_provider ppi;

	check_open_helper(&ppi, "isi_cfm_open_ppinfo__sp_open_failure");
}

/*
 * Test isi_cfm_open_policy_provider_info and inject a failure in opening the
 * cloudpools context.
 */
TEST(check_open_ppi__cp_open_fail)
{
	struct isi_cfm_policy_provider ppi;

	check_open_helper(&ppi, "isi_cfm_open_ppinfo__cp_open_failure");
}

/*
 * Test isi_cfm_open_policy_provider_info and inject a failure in opening the
 * proxy context.
 */
TEST(check_open_ppi__proxy_open_fail)
{
	struct isi_cfm_policy_provider ppi;

	check_open_helper(&ppi, "isi_cfm_open_ppinfo__proxy_open_failure");
}
