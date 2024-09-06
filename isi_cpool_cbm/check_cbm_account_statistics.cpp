
#include <isi_cpool_cbm/check_cbm_common.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cbm_id.h>
#include <isi_cpool_cbm/isi_cbm_account_util.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/check_cbm_common.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_cbm/isi_cbm_account_statistics.h>
#include <isi_cpool_cbm/isi_cbm_error.h>

#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h> ///jji

#include <isi_util/isi_error.h>
#include <check.h>

using namespace isi_cloud;

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_cleanup);

SUITE_DEFINE_FOR_FILE(check_cbm_account_statistics,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = suite_setup,
    .suite_teardown = suite_cleanup,
    .test_setup = NULL,
    .test_teardown = NULL,
    .runner_data = NULL,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	// Create Azure and S3 providers/accounts.
	cbm_test_common_create_az_provider_account();
	cbm_test_common_create_aws_s3_provider_account();

	isi_cbm_reload_config(&error);
	fail_if(error, "failed to reload config %#{}", isi_error_fmt(error));
}


TEST_FIXTURE(suite_cleanup)
{
	// Delete the azure and aws s3 providers/accounts.
	cbm_test_common_delete_az_provider_account();
	cbm_test_common_delete_aws_s3_provider_account();

	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
}


/**
 * Routine to access cloud account related functionalities.
 */

TEST(test_get_account_capacity_azure, .mem_check = CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	struct cpool_account *acct;
	isi_cbm_id acct_id;
	isi_cbm_account account;

	// Get the context.
	scoped_ppi_reader ppi_rdr;
	const isi_cfm_policy_provider *ppi = ppi_rdr.get_ppi(&error);
	fail_if(error, "failed to get the context %#{}", isi_error_fmt(error));

	// Get the cpool_account version of the account using the account name.
	acct= cpool_account_get_by_name(ppi->cp_context, CBM_TEST_COMMON_ACCT_AZ,
	    &error);

	// Set the isi_cbm_id version of the account.
	acct_id.set_from_account(*acct);

	// Get the isi_cbm_account.
	isi_cfm_get_account((isi_cfm_policy_provider *) ppi, acct_id,
	    account, &error);

	fail_if(error, "failed to get isi_cbm_account structure. %#{}",
	    isi_error_fmt(error));

	// Will use the current date to query for the capacity.
	// Based on when the query happens, the storage capacity might
	// be returned as 0 or NULL too. The query is based on GMT timestamp,
	// so the result will be based on when the query is sent to Azure
	// with respect to the timezone during which the tests are run.
	struct tm timestamp = {0, 0, 0, 0, 0, 0, 0, 0, 0};

	// Use the isi_cbm_account structure to query for the account capacity.
	isi_cloud::cl_account_statistics_param acct_stats_param = {
		ACCOUNT_CAPACITY,
		timestamp,
		STATS_UNIT_NONE,
		"",
		"",
		"",
		""
	};

	struct cl_account_statistics acct_stats;

	isi_cbm_get_account_statistics(account, acct_stats_param, acct_stats, &error);

	if (error) {
		// We will not fail the test if the Azure returns a valid
		// response but hasnt yet updated the capacity info.
		if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND)) {
			fprintf(stderr, "\n Usage not found in Azure response. "
			    "CBM_CLAPI_OBJECT_NOT_FOUND err \n");
			acct_stats.used_capacity = 0;
		}
		// Any other exceptions we handle.
		else {
			fail(error, "Azure Get Account capacity failed. %#{}",
			    isi_error_fmt(error));
		}
	}

	fprintf(stderr, "The max capacity- %lf \n", acct_stats.max_capacity);
	fprintf(stderr, "The used capacity- %lf \n", acct_stats.used_capacity);
	fprintf(stderr, "The free capacity- %lf \n",
	    acct_stats.max_capacity - acct_stats.used_capacity);

	// This failure occurs if there is no error reported and
	// no account usage value is returned. Typically this scenario should
	// never occur.
	// Assert to validate if the account's used capacity is
	// greater than zero.
	fail_if(acct_stats.used_capacity < 0, "Azure Used capacity is invalid. %lf \n",
	    acct_stats.used_capacity);
}


/**
 * Routine to access cloud account related functionalities.
 */

TEST(test_get_account_capacity_s3, .mem_check = CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	struct cpool_account *acct;
	isi_cbm_id acct_id;
	isi_cbm_account account;

	// Get the context.
	scoped_ppi_reader ppi_rdr;
	const isi_cfm_policy_provider *ppi = ppi_rdr.get_ppi(&error);
	fail_if(error, "failed to get the context %#{}", isi_error_fmt(error));

        // Get the cpool_account version of the account using the account name.
	acct= cpool_account_get_by_name(ppi->cp_context, CBM_TEST_COMMON_ACCT_AWS_S3,
	    &error);

	// Set the isi_cbm_id version of the account.
	acct_id.set_from_account(*acct);

	// Get the isi_cbm_account.
	isi_cfm_get_account((isi_cfm_policy_provider *) ppi, acct_id,
	    account, &error);

	fail_if(error, "failed to get isi_cbm_account structure. %#{}",
	    isi_error_fmt(error));

	struct tm timestamp = {0, 0, 0, 0, -1, -1, 0, 0, 0};

	// Use the isi_cbm_account structure to query for the account capacity.
	struct cl_account_statistics_param acct_stats_param;
	acct_stats_param.op = ACCOUNT_CAPACITY;
	acct_stats_param.timestamp = timestamp;
	acct_stats_param.stats_unit = GIGA_BYTES;
	acct_stats_param.capacity_response_format = "";
	acct_stats_param.capacity_response_path = "";
	acct_stats_param.capacity_response_value = "";
	acct_stats_param.billing_bucket = "isilonusagedir";

	struct cl_account_statistics acct_stats;

	isi_cbm_get_account_statistics(account, acct_stats_param, acct_stats,
	    &error);

	/* TODO:: Temporary fix for bug 155042 - the report queried from S3 is
	 * not available (not yet generated) at the start of any given month
	 * so this test could fail with a CBM_CLAPI_OBJECT_NOT_FOUND error.
	 * If we hit this particular error, bail out - nothing else useful can
	 * be done with the test.
	 * N.b., The better solution is to replace the report reading logic with
	 * something more robust, but that's out of scope for Riptide.
	 * Incidentally, failures of this type cause no direct customer impact
	 * in the field
	 */
	if (error && isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND)) {
		fprintf(stderr, "\n Usage not found in S3 response. "
		    "CBM_CLAPI_OBJECT_NOT_FOUND err \n");
		isi_error_free(error);
		return;
	}
	/* End temporary fix bug 155042 */

	fail_if(error, "AWS S3 Get Account capacity failed. %#{}",
	    isi_error_fmt(error));

	// Assert to validate if the account's used capacity is
	// greater than zero.
	fail_if(acct_stats.used_capacity <= 0,
	    "AWS S3 Used capacity is invalid. %lf \n",
	    acct_stats.used_capacity);
}
TEST(test_remove_account) ///jjz
{
	//cbm_test_remove_account("ran_acct");
	cpool_config_context *ctx = cpool_context_open(&error);
	printf("\n****************\n");
	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	struct cpool_account *acct = cpool_account_get_by_name(ctx,
	    "ran_acct", &error);

	

	if (acct) {
		uint32_t acct_id = cpool_account_get_id(acct);
		const char *cluster = cpool_account_get_cluster(acct);
		isi_cbm_id account_id(acct_id, cluster);
		cl_provider_type cl_type = cpool_account_get_type(acct);

		if (cl_type == CPT_AWS || cl_type == CPT_AZURE) {
			isi_cbm_ioh_base_sptr io_helper =
			    isi_cpool_get_io_helper(account_id, &error);

			fail_if(error, "Failed to get IO helper for "
			    "account: %#{}.\n", isi_error_fmt(error));
			cbm_test_object_cleaner cleaner;
			cleaner.acct = acct;
			cleaner.io_helper = io_helper;
			cleaner.cmo = true;

			cl_list_object_param param;
			param.caller_cb = cbm_test_list_object_cb;
			param.caller_context = &cleaner;
			param.attribute_requested = true;
			param.include_snapshot = true;
			param.depth = 0;

			try {
				io_helper.get()->list_object(
				    acct->metadata_bucket, param);

				cleaner.cmo = false;
				io_helper.get()->list_object(acct->bucket, param);

				io_helper.get()->delete_object(
				    acct->metadata_bucket, "", 0);

				io_helper.get()->delete_object(acct->bucket,
				    "", 0);
			}
			catch (const std::exception &ex) {
				// we do not care about the errors here
				printf("\nExceptions while cleaning cloud "
				    "objects, %s\n", ex.what());
			}
		}
		printf("%s called: acct_id:%u, cluster:%s\n", __func__, acct_id, cluster);
		cpool_account_remove(ctx, acct, &error);
		fail_if(error, "Failed to remove the account: %#{}.\n",
		    isi_error_fmt(error));

		cpool_context_commit(ctx, &error);
		fail_if(error, "Failed to save the GC change: %#{}.\n",
		    isi_error_fmt(error));

		cpool_account_free(acct);
	}

	cpool_context_close(ctx);
}