#include <isi_cpool_cbm/isi_cbm_ooi.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/cpool_gcfg.h>
#include <isi_cpool_d_api/cpool_api.h>

#include "ooi_scanning_thread_pool.h"

using namespace isi_cloud;

ooi_scanning_thread_pool::ooi_scanning_thread_pool()
{
}

ooi_scanning_thread_pool::~ooi_scanning_thread_pool()
{
}

std::shared_ptr<ooi>
ooi_scanning_thread_pool::get_index(struct isi_error **error_out) const
{
	return isi_cbm_get_ooi(error_out);
}

const struct cpool_d_index_scan_thread_intervals *
ooi_scanning_thread_pool::get_sleep_intervals(
	const cpool_d_config_settings *config_settings) const
{
	ASSERT(config_settings != NULL);

	return config_settings->get_ooi_intervals();
}

void ooi_scanning_thread_pool::process_item(const isi_cloud_object_id &object_id,
											struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	/////调用cpool_api.cpp的tm.add_task(OT_CLOUD_GC,...)
	delete_cloud_object(object_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "cloud object ID: %{}", isi_cloud_object_id_fmt(&object_id));

out:
	isi_error_handle(error, error_out);
}

// void ooi_scanning_thread_pool::process_entry(isi_cloud::ooi_entry,isi_cloud::ooi,
// 	    struct timeval failed_item_retry_time)
// {
// 	printf("***");
// }