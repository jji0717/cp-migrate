#include <memory>

#include <isi_cpool_cbm/isi_cbm_csi.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_d_api/cpool_api.h>

#include "csi_scanning_thread_pool.h"

using namespace isi_cloud;

csi_scanning_thread_pool::csi_scanning_thread_pool()
{
}

csi_scanning_thread_pool::~csi_scanning_thread_pool()
{
}

std::shared_ptr<csi>
csi_scanning_thread_pool::get_index(struct isi_error **error_out) const
{
	return isi_cbm_get_csi(error_out);
}

const struct cpool_d_index_scan_thread_intervals *
csi_scanning_thread_pool::get_sleep_intervals(
    const cpool_d_config_settings *config_settings) const
{
	ASSERT(config_settings != NULL);

	return config_settings->get_csi_intervals();
}

void
csi_scanning_thread_pool::process_item(const isi_cloud::lin_snapid &lin_snapid,
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;

	invalidate_stubbed_file_cache(lin_snapid.get_lin(),
	    lin_snapid.get_snapid(), &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{} snapid: %{}", lin_fmt(lin_snapid.get_lin()),
	    snapid_fmt(lin_snapid.get_snapid()));

 out:
	isi_error_handle(error, error_out);
}
