#include <uuid.h>

#include <isi_ilog/ilog.h>
#include <isi_qpl/qpl.h>
#include <isi_rest_server/request.h>
#include <isi_rest_server/request_thread.h>
#include <isi_rest_server/uri_mapper.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_dmalloc.h>
#include <isi_util_cpp/scoped_clean.h>
#include <isi_util_cpp/scoped_lock.h>

#include <lw/threadpool.h> // for QPL promises

request_thread::request_thread(request_thread_pool &pool)
	: pool_(pool), manager_(pool.mapper())
{ }

request_thread::~request_thread()
{ }

void *
request_thread::on_run()
{
	while (true) {
		if (fcgi_request *f_req = pool_.get_request()) ////如果工作队列中没有fcgi request,那么线程阻塞
			process(f_req);
		else {/////////////程序退出
			ilog(IL_INFO, "no request from pool, terminating");
			break;
		}
	}

	return NULL;
}

/* Returns a new string formatted uuid */
static char *
gen_uuid_str()
{
        uint32_t uuid_result = -1;
        uuid_t uuid;
        char *id = NULL;

        uuid_create(&uuid, &uuid_result);
        if (uuid_result == uuid_s_ok) {
                uuid_to_string(&uuid, &id, &uuid_result);
                if (uuid_result != uuid_s_ok) {
                        id = strdup("???");
                }
        } else {
                id = strdup("???");
        }
        return id;
}

void
request_thread::process(fcgi_request *f_req)
try
{
	std::auto_ptr<fcgi_request> f(f_req);

	response output(f_req->get_output_stream());

	request::method method = request::UNSUPPORTED;

	if (request *a_req = f_req->api_request()) {
		std::auto_ptr<request> a(a_req);
		bool audit = pool_.get_audit();
		int status = 0;
		char *id = NULL;
		scoped_ptr_clean<char> id_clean(id, free);

		method = a_req->get_method();

		if (audit) {
			if (method != request::PUT &&
			  method != request::POST &&
			  method != request::DELETE)
				audit = false;
			else {
				// event ID
                                id = gen_uuid_str();

				char *s = a_req->audit_request();
				status = pool_.log_audit_event(id, s);
				free(s);
			}
		}
		manager_.execute_request(*a_req , output);
		if (audit && (status == 0)) {
			char *s = output.audit_response();
			pool_.log_audit_event(id, s);
			free(s);
		}
	} else {
		ilog(IL_INFO, "%s: error reading api request", __func__);
		output.add_error(AEC_BAD_REQUEST, "Error parsing request");
	}

	output.send(method);
	f_req->finish();
}
catch (const isi_exception &e)
{
	/*
	 * This block should only deal with memory errors or errors
	 * arising from the final send().
	 */
	ilog(IL_INFO, "Error processing request: %s", e.what());
}


request_thread_pool::request_thread_pool(const uri_mapper &um,
    bool sync, unsigned maxw, unsigned maxt)
	: mapper_(um), max_waiting_(maxw), max_threads_(maxt), sync_rt_(NULL)
	, shutdown_(false), autodmalloc_(false), busy_sz_(maxt)
	, peak_queue_sz_(0), peak_busy_sz_(0), audit_(false), audithandle_(NULL)
{
	pthread_mutex_init(&mtx_, NULL);
	pthread_mutex_init(&auditmtx_, NULL);
	pthread_cond_init(&cnd_, NULL);

	(void) max_threads_; /* to eliminate not-used warning */

	if (sync)
		sync_rt_ = new request_thread(*this);
	else {
		for (unsigned int i = 0; i < maxt; ++i)
			threads_.push_back(new request_thread(*this));

		for (unsigned int i = 0; i < maxt; ++i)
			threads_[i]->run();
	}

	/* Wait for all threads to be fully started up so stats are valid */
	if (!sync) {
		while (true) {
			scoped_lock lock(mtx_);
			if (busy_sz_ == 0)
				break;
		}
	}

	dmark_ = dmalloc_mark();
}

request_thread_pool::~request_thread_pool()
{
	if (sync_rt_)
		delete sync_rt_;
	else if (!threads_.empty())
		ilog(IL_ERR, "pool destroyed without shutdown!");

	pthread_cond_destroy(&cnd_);
	pthread_mutex_destroy(&mtx_);
	pthread_mutex_destroy(&auditmtx_);
}

bool
request_thread_pool::put_request(fcgi_request *f_req)
{
	if (sync_rt_) {
		sync_rt_->process(f_req);
		if (autodmalloc_)
			dump_leaks();
		return true;
	}

	scoped_lock lock(mtx_);

	UFAIL_POINT_CODE(request_pool_full, { return false; });

	if (waiting_.size() >= max_waiting_)  /////队列中已有的fcgi_request太多,就不加入队列了
		return false;

	waiting_.push_back(f_req);
	if (waiting_.size() > peak_queue_sz_)////这个参数用不到,忽略它
		peak_queue_sz_ = waiting_.size();

	if (waiting_.size() > 0) {
		pthread_cond_signal(&cnd_);
	}

	return true;
}

fcgi_request *
request_thread_pool::get_request()
{
	ilog(IL_REST_THR, "%p get_request lock wait", this);
	scoped_lock lock(mtx_);

	/*
	 * At start up, busy is the same as thread count. As each
	 * enters the wait, the count drains to 0, and it raises
	 * again as threads get work from the queue.
	 */
	--busy_sz_;

	while (!shutdown_ && waiting_.empty()) {
		ilog(IL_REST_THR, "%p get_request cond wait", this);
		pthread_cond_wait(&cnd_, &mtx_);
	}

	if (shutdown_) {
		ilog(IL_REST_THR, "%p get_request shutdown", this);
		return NULL;
	}

	fcgi_request *ret = waiting_.front();
	waiting_.pop_front();
	++busy_sz_;

	if (busy_sz_ > peak_busy_sz_)
		peak_busy_sz_ = busy_sz_;

	ilog(IL_REST_THR, "%p qsize=%lu, get_request return %p",
	    this, waiting_.size(), ret);

	return ret;
}

void
request_thread_pool::shutdown()
{
	if (sync_rt_)
		return;

	/* Time to wait for each thread to join before moving on */
	timespec JOIN_WAIT = {
		.tv_sec = 0,
		.tv_nsec = 10 * 1000 * 1000
	};

	/*
	 * N.B. Without the sleep, pthread doesn't seem to be
	 * fair to the pool threads trying to grab the lock.
	 * Sleep in the for statement causes it to happen
	 * without the lock held by design.
	 */
	const long UNLOCKED_SLEEP = 100 * 1000;

	for ( ; !threads_.empty(); usleep(UNLOCKED_SLEEP)) {
		ilog(IL_REST_THR, "shutdown lock wait");
		scoped_lock lock(mtx_);
		pthread_cond_broadcast(&cnd_);
		shutdown_ = true; // only needed once

		for (thread_list::iterator it = threads_.begin();
		    it != threads_.end(); ) {
			if ((*it)->join(JOIN_WAIT)) {
				delete *it;
				it = threads_.erase(it);
			} else
				++it;
		}
	}

	while (!waiting_.empty()) {
		ilog(IL_REST_THR, "shutdown deleting: %p", waiting_.front());
		delete waiting_.front();
		waiting_.pop_front();
	}
}

void
request_thread_pool::dump_leaks()
{
	unsigned long mark = dmark_;

	unsigned long bytes = dmalloc_count_changed(mark, 1, 0);

	if (bytes == 0)
		return;

	fprintf(stderr, "LEAKED %lu BYTES\n", bytes);

	/*
	 * This juggling results in printing the message as well as using the
	 * dmalloc log file, if any.
	 */
	unsigned int new_dm_debug = dmalloc_token_to_value("print-messages");
	unsigned int old_dm_debug = dmalloc_debug_current();
	dmalloc_debug(new_dm_debug | old_dm_debug);
	fprintf(stderr, "\n");
	dmalloc_log_changed(mark, 1, 0, 0);
	dmalloc_debug(old_dm_debug);

	dmark_ = dmalloc_mark();
}

void
request_thread_pool::set_autodmalloc(bool val)
{
	autodmalloc_ = val;
}


/**
 * Pool Auditing Support
 **/

void
request_thread_pool::set_audit(bool val)
{
        if (!val && audit_) {
                /* Disabling audit */
                if (audithandle_) {
                        P_CloseLog(&audithandle_);
                        audithandle_ = NULL;
                }
        }
	audit_ = val;
}

bool
request_thread_pool::get_audit(void) const
{
	return audit_;
}

int
request_thread_pool::log_audit_event(const char *id, const char *s)
{
	const int max_retries = 5;
	int current_try = 0;
	int status = 0;
	int nsecs = 0;

        for (current_try = 0; current_try < max_retries; ++current_try, sleep(nsecs)) {
		scoped_lock lock(auditmtx_);
		if (audithandle_ == NULL) {
			if ((status=P_OpenLog(&audithandle_, "config")) != QPL_SUCCESS) {
				ilog(IL_NOTICE, "Error connecting to audit daemon - try #%d\n", current_try);
				nsecs += 1;
				continue;
			}

                        /* Log startup message */
                        char *startmsg_id = NULL;
                        scoped_ptr_clean<char> id_clean(startmsg_id, free);
                        startmsg_id = gen_uuid_str();
                        const char *start_msg = "\"PAPI config logging started.\"";
                        status = P_LogConfigEvent(audithandle_, startmsg_id, start_msg, strlen(start_msg));
                        if (status != QPL_SUCCESS) {
                                ilog(IL_ERR, "Audit startup event I/O error");
                                P_CloseLog(&audithandle_);
                                audithandle_ = NULL;
                                continue;
                        }
		}
		status = P_LogConfigEvent(audithandle_, id, s, strlen(s));
		if (status != QPL_SUCCESS) {
			if (status == QPL_TOO_BIG) {
				ilog(IL_ERR, "Cannot deliver audit event to "
				"audit daemon. Event payload size (%lu) + "
				"format length is "
				"greater than allowed size (%d)",
				strlen(s), AUDIT_MAX_PAYLOAD_SIZE);
				return status;
			}
			ilog(IL_ERR,  "Audit event I/O error");
			P_CloseLog(&audithandle_);
			audithandle_ = NULL;
			continue;
		}
		break;
	}
	return status;
}
