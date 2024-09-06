#include <isi_cpool_cbm/isi_cbm_wbi.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_d_api/cpool_api.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>

#include "wbi_scanning_thread_pool.h"

using namespace isi_cloud;

wbi_scanning_thread_pool::wbi_scanning_thread_pool()
{
}

wbi_scanning_thread_pool::~wbi_scanning_thread_pool()
{
}

std::shared_ptr<wbi>
wbi_scanning_thread_pool::get_index(struct isi_error **error_out) const
{
	return isi_cbm_get_wbi(error_out);
}

const struct cpool_d_index_scan_thread_intervals *
wbi_scanning_thread_pool::get_sleep_intervals(
    const cpool_d_config_settings *config_settings) const
{
	ASSERT(config_settings != NULL);

	return config_settings->get_wbi_intervals();
}

bool
wbi_scanning_thread_pool::are_access_list_guids_being_removed(
    isi_error **error_out) const
{
	struct isi_error *error = NULL;
	bool ret_val = false;

	/* Grab a reference to the current gconfig contexts. */
	scoped_ppi_reader ppi_rdr;

	isi_cfm_policy_provider *ppi =
	    (isi_cfm_policy_provider *) ppi_rdr.get_ppi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	for (struct permit_guid_list_element *iter =
	    smartpools_get_permit_list_next(ppi->sp_context, NULL);
	    iter != NULL;
	    iter = smartpools_get_permit_list_next(ppi->sp_context, iter)) {

		cm_attr_state state = smartpools_get_guid_element_state(iter);

		if (state == CM_A_ST_REMOVING) {
			ret_val = true;
			break;
		}
	}

 out:
	isi_error_handle(error, error_out);

	return ret_val;
}

bool
wbi_scanning_thread_pool::can_process_entries(struct isi_error **error_out)
    const
{
	struct isi_error *error = NULL;
	bool ret_val = false;

	ret_val = fs_is_fully_accessible() &&
		!are_access_list_guids_being_removed(&error);

	if (!ret_val)
		ilog(IL_DEBUG, "wbi_scanning_thread_pool disallowing entry "
		    "processing: guids are being removed from access list");

	isi_error_handle(error, error_out);

	return ret_val;
}

void
wbi_scanning_thread_pool::process_item(const isi_cloud::lin_snapid &lin_snapid,
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;

	writeback_file(lin_snapid.get_lin(), &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{}", lin_fmt(lin_snapid.get_lin()));

 out:
	isi_error_handle(error, error_out);
}
