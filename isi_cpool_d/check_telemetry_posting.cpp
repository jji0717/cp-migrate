#include <isi_config/array.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "telemetry_posting_thread_pool.h"

#define TEST_REPORT_FILE "/tmp/check_telemetry_posting_file"

class test_telemetry_posting_thread_pool : public telemetry_posting_thread_pool
{
public:
	/* Thin wrapper just exposes protected functionality */
	static void test_write_stats_data_to_report(
	    std::vector<cpool_stat_time_record*> &stats,
	    const char *report_filename,
	    const struct cpool_config_context *context,
	    struct isi_error **error_out) {

		write_stats_data_to_report(stats, report_filename,
		    context, error_out);
	};
};

TEST_FIXTURE(setup_suite)
{
	//  isi_config's arr_config_load does some static initialization that
	//	makes the memchecker go crazy.  Do initialization here to avoid
	//	spurious leak messages
	struct isi_error *error = NULL;
	struct arr_config *ac = arr_config_load(&error);
	ASSERT(error == NULL,
	    "failed to load arr_config (from isi_config.array.c: %{}",
	    isi_error_fmt(error));
	arr_config_free(ac);
}

TEST_FIXTURE(setup_test)
{
}

TEST_FIXTURE(teardown_test)
{
}

SUITE_DEFINE_FOR_FILE(check_telemetry_posting,
	.mem_check = CK_MEM_DISABLE,
	.suite_setup = setup_suite,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST(smoke_write_report)
{
	struct isi_error *error = NULL;

	struct cpool_config_context *ctx = NULL;

	ctx = cpool_context_open(&error);
	fail_if(error != NULL, "Failed to open context: %#{}",
	    isi_error_fmt(error));

	std::vector<cpool_stat_time_record*> stats;

	cpool_stat_manager *mgr = cpool_stat_manager::get_new_instance(&error);
	fail_if(error != NULL, "failed to create stat manager instance: %#{}",
	    isi_error_fmt(error));

	ASSERT(mgr != NULL);

	/* Just use existing stats data for this report */
	mgr->get_all_data_for_accounts(stats, 60*60, NULL, &error);
	fail_if(error != NULL, "failed to get stats data: %#{}",
	    isi_error_fmt(error));

	test_telemetry_posting_thread_pool::test_write_stats_data_to_report(
	    stats, TEST_REPORT_FILE, ctx, &error);
	fail_if(error != NULL, "Failed to write report: %#{}",
	    isi_error_fmt(error));

	cpool_context_close(ctx);

	delete mgr;
}
