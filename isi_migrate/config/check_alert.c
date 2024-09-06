#include <unistd.h>

#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "siq_alert.h"

#define FAKE_ID "fedcba0987654321fedcba0987654321"
#define FAKE_NAME "test_pol_1234567890abcdef"
#define FAKE_TARGET "/ifs/faketgt1234567890abcdef"

TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);

SUITE_DEFINE_FOR_FILE(siq_alert, .suite_teardown = suite_teardown,
    .test_setup = test_setup);

TEST_FIXTURE(test_setup)
{
	siq_gc_active_alert_cleanup(FAKE_ID);
}

TEST_FIXTURE(suite_teardown)
{
	siq_gc_active_alert_cleanup(FAKE_ID);
}

TEST(simple_siq_alert)
{
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	/*
	 * Add alert.
	 */
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_START, 0);
	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	/*
	 * Remove alert.
	 */
	siq_gc_active_alert_remove(active_alerts, SW_SIQ_POLICY_START);
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	/*
	 * Save alerts.
	 */
	siq_gc_active_alert_save(active_alerts, &error);
	fail_if_error(error);

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;
}

TEST(multiple_siq_alert, .attrs="overnight")
{
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));

	/*
	 * Add alerts.
	 */
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_START, 0);
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_START, 0);
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_FAIL, 0);

	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));
	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));

	/*
	 * Remove alert.
	 */
	siq_gc_active_alert_remove(active_alerts, SW_SIQ_POLICY_START);
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	/*
	 * Save alerts.
	 */
	siq_gc_active_alert_save(active_alerts, &error);
	fail_if_error(error);

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	/*
	 * Verify proper alerts exist.
	 */
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;

}

TEST(cancel_all_siq_alert, .attrs="overnight")
{
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));

	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_CONFIG));

	/*
	 * Add alerts.
	 */
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_START, 0);
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_FAIL, 0);
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_FAIL, 0);
	siq_gc_active_alert_add(active_alerts, SW_SIQ_POLICY_CONFIG, 0);

	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));
	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));
	fail_unless(siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_CONFIG));

	/*
	 * Save alerts.
	 */
	siq_gc_active_alert_save(active_alerts, &error);
	fail_if_error(error);

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;

	/*
	 * Cancel all outstanding alerts.
	 * This will send cancel events to CELOG which will be ignored.
	 */
	siq_cancel_all_alerts_for_policy(FAKE_NAME, FAKE_ID);

	/*
	 * Load alerts.
	 */
	active_alerts = siq_gc_active_alert_load(FAKE_ID, &error);
	fail_if_error(error);

	/*
	 * Verify alerts don't exist.
	 */
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_START));
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_FAIL));
	fail_unless(!siq_gc_active_alert_exists(active_alerts,
	    SW_SIQ_POLICY_CONFIG));

	siq_gc_active_alert_free(active_alerts);
	active_alerts = NULL;
}
