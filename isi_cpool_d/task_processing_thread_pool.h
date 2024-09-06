#ifndef __ISI_CPOOL_D_TASK_PROCESSING_THREAD_POOL_H__
#define __ISI_CPOOL_D_TASK_PROCESSING_THREAD_POOL_H__

class server_task_map;

#include "thread_pool.h"

class task_processing_thread_pool : public thread_pool
{
protected:
	typedef struct {
		task_processing_thread_pool	*tp;
		bool		recovery;
	} thread_args;

	thread_args		*normal_args_;
	thread_args		*recovery_args_;

	int			num_normal_threads_;
	int			num_recovery_threads_;

	server_task_map		*task_map_;

	/**
	 * Create the worker threads (normal and recovery) for this
	 * thread_pool.  Never throws.
	 */
	void start_threads(void);

	/**
	 * Get the maximum number of tasks to be read when attempting to start
	 * a task.
	 */
	uint32_t get_max_task_reads(void) const;

	/**
	 * Return the interval (in seconds) for a thread to wait before
	 * attempting to process another task.
	 *
	 * @param[in]  type		the type of task this thread processes
	 * @param[in]  recovery		true if thread is a recovery thread,
	 *				false if thread is a normal thread
	 * @param[in]  tasks_remain	true if, at the time of starting a
	 *				Task, additional Tasks remained to be
	 *				processed, else false
	 *
	 * @return the amount of time this thread should wait, in seconds
	 */
	float get_sleep_interval(task_type type, bool recovery,
	    bool tasks_remain);

	/* Default construction, copy, and assignment aren't allowed. */
	task_processing_thread_pool();
	task_processing_thread_pool(const thread_pool&);
	task_processing_thread_pool operator=(const thread_pool&);

public:
	task_processing_thread_pool(server_task_map *task_map,
	    int num_normal_threads, int num_recovery_threads);
	~task_processing_thread_pool();

	void shutdown(void);

	/**
	 * Worker thread entry function.  Never throws.
	 */
	static void *start_thread(void *);
};

#endif // __ISI_CPOOL_D_TASK_PROCESSING_THREAD_POOL_H__
