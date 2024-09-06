#include <unistd.h>

#include <isi_thread/thread.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

static void setup(void *);

SUITE_DEFINE_FOR_FILE(thread, CK_MEM_DEFAULT, 0, setup);

void
setup(void *x)
{
}

class basic_thread : public isi_thread::thread {
public:
	basic_thread()
	    : ran_(false)
	{ }

	void *on_run()
	{
		ran_ = true;
		return 0;
	}

	bool ran_;
};

class timed_thread : public isi_thread::thread {
public:
	timed_thread(long usecs)
	    : usecs_(usecs)
	{ }

	void *on_run()
	{
		usleep(usecs_);
		return 0;
	}

	long usecs_;
};

timespec
make_timespec(time_t secs, long usecs)
{
	timespec ret;
	ret.tv_sec  = secs;
	ret.tv_nsec = usecs * 1000;

	return ret;
}

#define TENTH_SECONDS_US (100 * 1000)
#define SECONDS_US       (1000 * 1000)

TEST(no_run)
{
	isi_thread::thread *t = new basic_thread();
	delete t;
}

TEST(basic_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	basic_thread *t = new basic_thread();
	t->run();
	t->join();
	fail_unless(t->ran_);
	delete t;
}

TEST(basic_timed_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	basic_thread *t = new basic_thread();
	t->run();
	bool res = t->join(make_timespec(0, 1 * SECONDS_US));
	fail_unless(res);
	fail_unless(t->ran_);
	delete t;
}

TEST(multi_basic_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	isi_thread::thread *threads[10];
	const int count = sizeof threads / sizeof *threads;

	for (int i = 0; i < count; ++i)
		threads[i] = new basic_thread();

	for (int i = 0; i < count; ++i)
		threads[i]->run();

	for (int i = 0; i < count; ++i)
		threads[i]->join();

	for (int i = 0; i < count; ++i)
		delete threads[i];
}

TEST(delayed_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	isi_thread::thread *t = new timed_thread(1 * TENTH_SECONDS_US);
	t->run();
	t->join();
	delete t;
}

TEST(delayed_timed_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	isi_thread::thread *t = new timed_thread(5 * TENTH_SECONDS_US);
	t->run();
	bool res = t->join(make_timespec(0, 1 * TENTH_SECONDS_US));
	fail_unless(res == false);
	t->join();
	delete t;
}

TEST(multi_delayed_join, .mem_check = CK_MEM_DISABLE /* XXX */)
{
	isi_thread::thread *threads[5];
	bool res;

	const int count = sizeof threads / sizeof *threads;

	for (int i = 0; i < count; ++i)
		threads[i] = new timed_thread(2 * SECONDS_US);

	for (int i = 0; i < count; ++i)
		threads[i]->run();

	for (int i = 0; i < count; ++i) {
		res = threads[i]->join(make_timespec(0, 1 * TENTH_SECONDS_US));
		fail_unless(res == false);
	}

	for (int i = 0; i < count; ++i)
		threads[i]->join();

	for (int i = 0; i < count; ++i)
		delete threads[i];
}
