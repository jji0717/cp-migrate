#ifndef __ISI_CPOOL_D_TELEMETRY_COLLECTION_THREADPOOL_H__
#define __ISI_CPOOL_D_TELEMETRY_COLLECTION_THREADPOOL_H__

#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include "thread_pool.h"

class telemetry_collection_thread_pool : public thread_pool
{
protected:
	static void *start_thread(void *arg);

	/**
	 * @return the time at which the next event should occur
	 */
	time_t get_next_event_time(struct isi_error **error_out) const;

	/**
	 */
	void process_event(time_t event_time, struct isi_error **error_out);

	/* Default/copy construction and assignment are prohibited. */
	telemetry_collection_thread_pool();
	telemetry_collection_thread_pool(
	    const telemetry_collection_thread_pool &);
	telemetry_collection_thread_pool &operator=(
	    const telemetry_collection_thread_pool &);

	/**
	 * Queries cloud for usage data
	 */
	void retrieve_cloud_usage(const struct cpool_account *acct,
	    struct cl_account_statistics &account_stats,
	    const isi_cbm_account &cbm_account,
	    struct isi_error **error_out);

public:
	telemetry_collection_thread_pool(int num_threads);
	~telemetry_collection_thread_pool();
};

#endif // __ISI_CPOOL_D_TELEMETRY_COLLECTION_THREADPOOL_H__
