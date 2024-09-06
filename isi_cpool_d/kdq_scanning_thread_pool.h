#ifndef __ISI_CPOOL_D_KDQ_THREAD_POOL_H__
#define __ISI_CPOOL_D_KDQ_THREAD_POOL_H__

#include "thread_pool.h"

class kdq_scanning_thread_pool : public thread_pool
{
protected:
	static void *scan_kdq(void *arg);

	/**
	 * Read up to the desired number of entries from the queue.
	 *
	 * @param[in]  q_fd		an open file descriptor of the queue
	 * @param[in]  num_desired	the desired number of entries to read
	 * from the queue
	 * @param[out] num_read		the number of entries actually read
	 * from the queue
	 * @param[out] entries		the entries read from the queue; must
	 * be non-NULL
	 * @param[out] data_buf		a buffer containing the data of the
	 * entries read from the queue; opaque to the caller; must be non-NULL
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	virtual void read_entries_from_queue(int q_fd, uint32_t num_desired,
	    uint32_t &num_read, struct pq_bulk_query_entry *entries,
	    char *data_buf, struct isi_error **error_out) const;

	/**
	 * Process an entry read from the queue.  If successfully processed,
	 * the entry will be removed.
	 *
	 * @param[in]  entry		the entry read from the queue
	 * @param[in]  q_fd		an open file descriptor of the queue
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	virtual void process_queue_entry(
	    const struct pq_bulk_query_entry *entry, int q_fd,
	    struct isi_error **error_out) const;

	/**
	 * Retrieve the sleep interval in seconds.  The short interval is used
	 * for throttling purposes; the long interval is used for the frequency
	 * of thread activity.
	 */
	float get_sleep_interval(bool get_short_interval) const;

public:
	kdq_scanning_thread_pool();
	~kdq_scanning_thread_pool();

	void start_threads(int num_threads);
};

#endif // __ISI_CPOOL_D_KDQ_THREAD_POOL_H__
