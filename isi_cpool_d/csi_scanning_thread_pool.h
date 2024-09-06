#ifndef __ISI_CPOOL_D_CSI_SCANNING_THREAD_POOL_H__
#define __ISI_CPOOL_D_CSI_SCANNING_THREAD_POOL_H__

#include <memory>

#include "time_based_scanning_thread_pool.h"

struct cpool_d_index_scan_thread_intervals;
class cpool_d_config_settings;

namespace isi_cloud {
	class csi;
	class csi_entry;
	class lin_snapid;
};

class csi_scanning_thread_pool :
    public tbi_scanning_thread_pool<
        isi_cloud::csi, isi_cloud::csi_entry, isi_cloud::lin_snapid>
{
protected:
	std::shared_ptr<isi_cloud::csi> get_index(
	    struct isi_error **error_out) const;
	const struct cpool_d_index_scan_thread_intervals *
	    get_sleep_intervals(
	    const cpool_d_config_settings *config_settings) const;
		
	///////模板函数,继承自tbi_scannning_thread_pool的不同类的process_item的参数类型不同
	void process_item(const isi_cloud::lin_snapid &item,
	    struct isi_error **error_out) const;

	/* Copy and assignment are not allowed. */
	csi_scanning_thread_pool(const csi_scanning_thread_pool &);
	csi_scanning_thread_pool &operator=(const csi_scanning_thread_pool &);

public:
	csi_scanning_thread_pool();
	~csi_scanning_thread_pool();
};

#endif // __ISI_CPOOL_D_CSI_SCANNING_THREAD_POOL_H__
