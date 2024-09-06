#ifndef INC_REQUEST_THREAD_H_
#define INC_REQUEST_THREAD_H_

#include <pthread.h>

#include <deque>
#include <vector>

#include <isi_rest_server/fcgi_helpers.h>
#include <isi_rest_server/response.h>
#include <isi_rest_server/uri_manager.h>
#include <isi_thread/thread.h>
#include <isi_qpl/qpl.h>
#include <isi_audit/audit_util.h>

class fcgi_request;
class request_thread_pool;

class request_thread : public isi_thread::thread {
public:
	explicit request_thread(request_thread_pool &pool);

	~request_thread();

private:
	friend class request_thread_pool;

	void *on_run();

	void process(fcgi_request *f_req);

	request_thread_pool &pool_;
	uri_manager         manager_;
};

class request_thread_pool {
public:
	/** Create thread pool.
	 *
	 * @param umap uri mapper instance
	 * @param sync synchronous operation, other params ignored
	 * @param maxw max waiting (queued) requests
	 * @param maxt max worker threads
	 */
	request_thread_pool(const uri_mapper &umap, bool sync,
	    unsigned maxw, unsigned maxt);

	~request_thread_pool();

	/** Return true if request could be processed.
	 *
	 * @post if successful, this owns request
	 */
	bool put_request(fcgi_request *request);

	/** Wait for a request to be available.
	 *
	 * Returned request is popped off the pool queue and is owned by
	 * caller.
	 *
	 * @return request or NULL if pool is shutting down.
	 */
	fcgi_request *get_request();

	/** Return URI mapper */
	inline const uri_mapper &mapper() const;

	/** Send shutdown, join all threads and clean queue */
	void shutdown();

	/** Dump leaks since last call (requires dmalloc linking and setup) */
	void dump_leaks();

	/** Set auto dmalloc leak dumping to val (valid only with sync) */
	void set_autodmalloc(bool val);

	/** Enable/disable auditing of PAPI config */
	void set_audit(bool val);
	bool get_audit(void) const;
	int  log_audit_event(const char *id, const char *s);
private:
	typedef std::deque<fcgi_request *>  work_queue;   /////工作队列:生产者消费者
	typedef std::vector<request_thread *>  thread_list;

	pthread_mutex_t   mtx_;
	pthread_cond_t    cnd_;
	work_queue        waiting_;
	const uri_mapper &mapper_;
	unsigned          max_waiting_;
	unsigned          max_threads_;
	thread_list       threads_;
	request_thread   *sync_rt_;
	unsigned long     dmark_;
	bool              shutdown_;
	bool              autodmalloc_;
	// some performance counters:
	unsigned          busy_sz_;
	unsigned          peak_queue_sz_;
	unsigned          peak_busy_sz_;
	// audit info
	bool		  audit_;
	HandleContext     audithandle_;
	pthread_mutex_t   auditmtx_;
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

const uri_mapper &
request_thread_pool::mapper() const
{
	return mapper_;
}

#endif
