#ifndef __ISI_CPOOL_D_OOI_SCANNING_THREAD_POOL_H__
#define __ISI_CPOOL_D_OOI_SCANNING_THREAD_POOL_H__

#include <memory>

#include "time_based_scanning_thread_pool.h"

struct cpool_d_index_scan_thread_intervals;
class cpool_d_config_settings;
class isi_cloud_object_id;

namespace isi_cloud {
	class ooi;
	class ooi_entry;
};

class ooi_scanning_thread_pool :
    public tbi_scanning_thread_pool<
        isi_cloud::ooi, isi_cloud::ooi_entry, isi_cloud_object_id>
{
protected:
	std::shared_ptr<isi_cloud::ooi> get_index(
	    struct isi_error **error_out) const;
	const struct cpool_d_index_scan_thread_intervals *
	    get_sleep_intervals(
	    const cpool_d_config_settings *config_settings) const;
		
	///////模板函数,继承自tbi_scannning_thread_pool的不同类的process_item的参数类型不同
	void process_item(const isi_cloud_object_id &item,
	    struct isi_error **error_out) const;

	/////继承自time_based_scanning_thread_pool.h的process_entry  jjz
	// void process_entry(/*entry_type *entry*/isi_cloud::ooi_entry, /*index_type &index*/isi_cloud::ooi,
	//     struct timeval failed_item_retry_time);

	/* Copy and assignment are not allowed. */
	ooi_scanning_thread_pool(const ooi_scanning_thread_pool &);
	ooi_scanning_thread_pool &operator=(const ooi_scanning_thread_pool &);

public:
	ooi_scanning_thread_pool();
	~ooi_scanning_thread_pool();
};

#endif // __ISI_CPOOL_D_OOI_SCANNING_THREAD_POOL_H__
