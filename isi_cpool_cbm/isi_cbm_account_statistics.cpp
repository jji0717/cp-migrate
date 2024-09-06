#include <sstream>
#include <string>

#include "isi_cbm_account_statistics.h"
#include "isi_cbm_account_util.h"
#include "isi_cbm_error_util.h"

#include <isi_ilog/ilog.h>
#include <isi_cloud_api/cl_provider.h>

#include "isi_cbm_error.h"
#include "isi_cbm_policyinfo.h"
#include "io_helper/isi_cbm_ioh_base.h"
#include "io_helper/isi_cbm_ioh_creator.h"
#include "isi_cbm_scoped_ppi.h"

using namespace isi_cloud;
using namespace std;

/**
 * Get the account capacity usage from cloud.
 *
 * account [in]: Has the account information for which statistics is requested.
 * acct_stats_param [in]: Parameters to parse the response from cloud.
 * acct_stats [out]: The struct that has the statistics information..
 * error_out [out]: isi_error if failed.
 */
void
isi_cbm_get_account_statistics(const isi_cbm_account &account,
    isi_cloud::cl_account_statistics_param &acct_stats_param,
    isi_cloud::cl_account_statistics &acct_stats,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	isi_cbm_ioh_base_sptr io_helper;
	cpool_events_attrs *cev_attrs = NULL;

	io_helper = isi_cpool_get_io_helper(account, &error);

	if (error)
		goto out;

	try {
		// Get the Cloud Service Provider info.
		cloud_service_provider *csp = NULL;
		scoped_ppi_reader ppi_rdr;
		const isi_cfm_policy_provider *ctx = ppi_rdr.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		csp = cloud_service_provider_get_by_type(ctx->cp_context,
		    account.type, &error);
		ON_ISI_ERROR_GOTO(out, error);

		// As of now update max capacity with the max capacity in the
		// account structure.
		// Once max capacity API is available from the cloud provider,
		// this can be removed.
		// Update relevant param info from gconfig.
		acct_stats.max_capacity = account.max_capacity;
		ASSERT(csp->capacity_response_format != NULL);
		acct_stats_param.capacity_response_format =
		    csp->capacity_response_format;

		if (csp->capacity_response_path)
			acct_stats_param.capacity_response_path =
			    csp->capacity_response_path;

		if (csp->capacity_response_value)
			acct_stats_param.capacity_response_value =
			    csp->capacity_response_value;

		io_helper->get_account_statistics(acct_stats_param, acct_stats);

		// Uncomment for debug purposes.
		// fprintf(stderr, "acct_stats, max-cap: %lu, used-cap: %lu \n",
		//    acct_stats.max_capacity, acct_stats.used_capacity);
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)

	ON_ISI_ERROR_GOTO(out, error, "failed to get cloud capacity");

 out:
	isi_error_handle(error, error_out);
}

