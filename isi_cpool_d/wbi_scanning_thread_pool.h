#ifndef __ISI_CPOOL_D_WBI_SCANNING_THREAD_POOL_H__
#define __ISI_CPOOL_D_WBI_SCANNING_THREAD_POOL_H__

#include <memory>

#include "time_based_scanning_thread_pool.h"

struct cpool_d_index_scan_thread_intervals;
class cpool_d_config_settings;

namespace isi_cloud {
	class wbi;
	class wbi_entry;
	class lin_snapid;
};

class wbi_scanning_thread_pool :
    public tbi_scanning_thread_pool<
        isi_cloud::wbi, isi_cloud::wbi_entry, isi_cloud::lin_snapid>
{
private:
	bool are_access_list_guids_being_removed(isi_error **error_out) const;

protected:
	std::shared_ptr<isi_cloud::wbi> get_index(
	    struct isi_error **error_out) const;
	const struct cpool_d_index_scan_thread_intervals *
	    get_sleep_intervals(
	    const cpool_d_config_settings *config_settings) const;
	void process_item(const isi_cloud::lin_snapid &item,
	    struct isi_error **error_out) const;
	virtual bool can_process_entries(struct isi_error **error_out) const;

	/* Copy and assignment are not allowed. */
	wbi_scanning_thread_pool(const wbi_scanning_thread_pool &);
	wbi_scanning_thread_pool &operator=(const wbi_scanning_thread_pool &);

public:
	wbi_scanning_thread_pool();
	~wbi_scanning_thread_pool();
};

#endif // __ISI_CPOOL_D_WBI_SCANNING_THREAD_POOL_H__
