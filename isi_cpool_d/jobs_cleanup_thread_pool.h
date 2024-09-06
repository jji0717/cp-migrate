#ifndef __ISI_CPOOL_D_JOBS_CLEANUP_THREAD_POOL_H__
#define __ISI_CPOOL_D_JOBS_CLEANUP_THREAD_POOL_H__

#include "thread_pool.h"

class jobs_cleanup_thread_pool : public thread_pool
{
protected:
	static void *cleanup_daemon_jobs(void *arg);

	void read_cleanup_settings(
	    struct cpool_d_cleanup_settings &cleanup_settings);

	/* Copy construction and assignment aren't allowed. */
	jobs_cleanup_thread_pool(const jobs_cleanup_thread_pool&);
	jobs_cleanup_thread_pool operator=(const jobs_cleanup_thread_pool&);

public:
	jobs_cleanup_thread_pool();
	~jobs_cleanup_thread_pool();

	void start_threads(int num_threads);
};

#endif // __ISI_CPOOL_D_JOBS_CLEANUP_THREAD_POOL_H__
