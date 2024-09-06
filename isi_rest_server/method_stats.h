#ifndef __METHOD_STATS_H__
#define __METHOD_STATS_H__

#include <stdint.h>

#include <isi_ilog/ilog.h>

struct pthread_mutex;

struct method_stats {
	method_stats();

	~method_stats();

	/** Clear all stats */
	void clear();

	/** Update with data for a call */
	void update(bool success, uint64_t elapsed);

	uint32_t calls_;  ///< total number of calls
	uint32_t errors_; ///< of those calls, number returning error
	uint64_t time_;   ///< total time in microseconds spent processing

	/** lock for writes and for reading if accuracy is desired */
	pthread_mutex *mtx_;

private:

	/* uncopyable, unassignable */
	method_stats(const method_stats &);
	method_stats &operator=(const method_stats &);
};

struct method_timer {
	/** Starts the timer, default result is error */
	method_timer(method_stats *stats);

	/** Records elapsed time on destruction */
	~method_timer();

	/** Change stats to put result in */
	inline void set_stats(method_stats *stats);

	/** Change recorded result to true */
	inline void set_success();

	/**
	 * Get time passed in milliseconds since either the timer started or
	 * check_time() was called.
	 */
	uint64_t check_time();

	/** Get total time passed in milliseconds since timer start.*/
	uint64_t total_passed();

	/**
	 * Use ilog() to log a message of the form:
	 * "Timing checkpoint: <label>: <x>ms passed (<y>ms total)"
	 * (Where passed is the time since last checked and total is the time
	 * since the beginning of the request.)
	 */
	void log_timing_checkpoint(const char *label, int level = IL_DEBUG);

private:
	inline uint64_t current();

	method_stats *stats_;
	bool          success_;
	uint64_t      start_;
	uint64_t      last_check_;

	/* uncopyable, unassignable */
	method_timer(const method_timer &);
	method_timer &operator=(const method_timer &);
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

void
method_timer::set_stats(method_stats *stats)
{
	stats_ = stats;
}

void
method_timer::set_success()
{
	success_ = true;
}

#endif
