#include <pthread.h>
#include <time.h>

#include <isi_util_cpp/scoped_lock.h>

#include "method_stats.h"

/*                 _   _               _         _        _
 *  _ __ ___   ___| |_| |__   ___   __| |    ___| |_ __ _| |_ ___
 * | '_ ` _ \ / _ \ __| '_ \ / _ \ / _` |   / __| __/ _` | __/ __|
 * | | | | | |  __/ |_| | | | (_) | (_| |   \__ \ || (_| | |_\__ \
 * |_| |_| |_|\___|\__|_| |_|\___/ \__,_|___|___/\__\__,_|\__|___/
 *                                     |_____|
 */

method_stats::method_stats()
	: calls_(0), errors_(0), time_(0)
{
	pthread_mutex_init(&mtx_, NULL);
}

method_stats::~method_stats()
{
	pthread_mutex_destroy(&mtx_);
	mtx_ = 0;
}

void
method_stats::clear()
{
	scoped_lock lock(mtx_);
	calls_  = 0;
	errors_ = 0;
	time_   = 0;
}

void
method_stats::update(bool success, uint64_t elapsed)
{
	scoped_lock lock(mtx_);
	calls_  += 1;
	errors_ += !success;
	time_   += elapsed;
}

/*                 _   _               _    _   _
 *  _ __ ___   ___| |_| |__   ___   __| |  | |_(_)_ __ ___   ___ _ __
 * | '_ ` _ \ / _ \ __| '_ \ / _ \ / _` |  | __| | '_ ` _ \ / _ \ '__|
 * | | | | | |  __/ |_| | | | (_) | (_| |  | |_| | | | | | |  __/ |
 * |_| |_| |_|\___|\__|_| |_|\___/ \__,_|___\__|_|_| |_| |_|\___|_|
 *                                     |_____|
 */

method_timer::method_timer(method_stats *stats)
	: stats_(stats), success_(false), start_(current()), last_check_(start_)
{ }

method_timer::~method_timer()
{
	if (stats_)
		stats_->update(success_, current() - start_);
}

uint64_t
method_timer::check_time()
{
	uint64_t last_last_check = last_check_;
	last_check_ = current();
	return (last_check_ - last_last_check) / 1000;
}

uint64_t
method_timer::total_passed()
{
	return (current() - start_) / 1000;
}

void
method_timer::log_timing_checkpoint(const char *label, int level)
{
	ilog(level, "Timing checkpoint: %s: %ldms passed (%ldms total)",
	    label, check_time(), total_passed());
}

uint64_t
method_timer::current()
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC_FAST, &ts);
	return 1000000UL * ts.tv_sec + (ts.tv_nsec + 500) / 1000;
}
