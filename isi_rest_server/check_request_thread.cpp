#include <unistd.h>

/* mess with private stuff for testing */
#define private public
#include "request_thread.h"
#undef private

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_util_cpp/scoped_lock.h>

SUITE_DEFINE_FOR_FILE(request_thread, .mem_check = CK_MEM_DISABLE,
    .dmalloc_opts = NULL, .suite_setup = NULL, .suite_teardown = NULL,
    .test_setup = 0, .test_teardown = NULL, .runner_data = NULL,
    .timeout = 10);

uri_mapper mapper(1);

/* override request thead for verification */
class fake_request_thread : public request_thread
{
public:
	static unsigned long all_sum;

	fake_request_thread(request_thread_pool &pool)
	    : request_thread(pool), sum_(0)
	{ }

	~fake_request_thread()
	{
		all_sum += sum_;
	}

	void *on_run()
	{
		while (true)
			if (fcgi_request *f_req = pool_.get_request())
				sum_ += (unsigned long)f_req;
			else
				break;

		return NULL;
	}

	unsigned long sum_;
};

unsigned long fake_request_thread::all_sum = 0;

TEST(basic_shutdown)
{
	request_thread_pool pool(mapper, false, 5, 5);

	fail_unless(&pool.mapper() == &mapper);

	pool.shutdown();
}

TEST(sync_shutdown)
{
	request_thread_pool pool(mapper, true, 5, 5);

	pool.shutdown();
}

TEST(queue_full)
{
	const unsigned queue_size = 2;
	request_thread_pool pool(mapper, false, queue_size, 0);

	for (unsigned i = 0; i < queue_size; ++i)
		fail_unless(pool.put_request((fcgi_request *)((uint64_t)i + 1)));

	fail_if(pool.put_request((fcgi_request *)666));

	/* clean out bogus requests so we don't crash */
	for (unsigned i = 0; i < queue_size; ++i)
		pool.get_request();

	pool.shutdown();
}

TEST(processing)
{
	const unsigned ITERS   = 1000;
	const unsigned THREADS = 5;
	const unsigned DEPTH   = 20;

	request_thread_pool pool(mapper, false, DEPTH, 0);

	for (unsigned i = 0; i < THREADS; ++i) {
		request_thread *t = new fake_request_thread(pool); 
		pool.threads_.push_back(t);
		t->run();
	}

	unsigned long sum = 0;

	for (unsigned long i = 1; i < ITERS; ++i) {
		if (pool.put_request((fcgi_request *)i))
			sum += i;
		if ((i % (DEPTH / 2)) == 0)
			usleep(10);
	}

	/* wait for queue to empty */
	for (; true; usleep(100)) {
		scoped_lock lock(pool.mtx_);
		if (pool.waiting_.empty())
			break;
	}

	pool.shutdown();

	/* see if all requests were processed */
	fail_unless(fake_request_thread::all_sum == sum);
}
