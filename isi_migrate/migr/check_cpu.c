#include <unistd.h>
#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "cpu_throttle.c"

/* 2^64 */
#define MAX_PERIOD 0xFFFFFFFFFFFFFFFF

SUITE_DEFINE_FOR_FILE(siq_cpu);

uint64_t period_elapsed;
uint64_t period_cpu_use;
uint64_t sleep_time;
uint32_t ration;


TEST(check_sleep_max_cpu_use, .attrs="overnight")
{
	period_elapsed = MAX_PERIOD;
	period_cpu_use = MAX_PERIOD;
	ration = 1;

	/* 100% cpu usage, .1% ration. Make sure we don't over-compensate
	 * and sleep for more than MAX_SLEEP_TIME. Also makes sure we act
	 * sanely with max values for both the period values. */

	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == MAX_SLEEP_TIME);
}

TEST(check_sleep_min_cpu_use, .attrs="overnight")
{
	period_elapsed = THROTTLE_INTERVAL;
	period_cpu_use = 0;
	ration = 100;

	/* 0% cpu usage, 10% ration. Since we didn't use any cpu, we shouldn't
	 * sleep at all. Also makes sure we act sanely with 0 cpu use value. */

	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == 0);
}

TEST(check_sleep_min_ration, .attrs="overnight")
{
	period_elapsed = THROTTLE_INTERVAL / 2;
	period_cpu_use = .002 * period_elapsed;
	ration = 1;

	/* .2% cpu usage, .1% ration (minimum) ==> should sleep amount equal
	 * to period_elapsed (need to cut use in half). Also makes sure we
	 * act sanely with minimum ration. */
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == period_elapsed);
}

TEST(check_sleep_max_ration, .attrs="overnight")
{
	period_elapsed = THROTTLE_INTERVAL;
	period_cpu_use = .1 * THROTTLE_INTERVAL;
	ration = 1000;

	/* 10% cpu usage, 100% ration (maximum) ==> should not sleep.
	 * Also makes sure we act sanely with maximum ration. */
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == 0);
}

TEST(check_sleep_max_period, .attrs="overnight")
{
	period_elapsed = MAX_PERIOD;
	period_cpu_use = 0;
	ration = 1;

	/* No cpu usage, .1% ration (minimum), max time has elapsed.
	 * Since no cpu was used, we should not sleep at all */
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == 0);
}

TEST(check_sleep_min_period, .attrs="overnight")
{
	period_elapsed = 0;
	period_cpu_use = MAX_PERIOD;
	ration = 1;

	/* Max cpu usage, .1% ration (minimum), 0 time has elapsed. This is
	 * not really possible but lets make sure we act sanely.
	 * The math will say we should sleep for a very long time, but a sane
	 * response is to sleep for a max of one second */
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == MAX_SLEEP_TIME);
}

TEST(check_sleep_smallest_period, .attrs="overnight")
{
	period_elapsed = 1;
	period_cpu_use = MAX_PERIOD;
	ration = 1;

	/* Max cpu usage, .1% ration (minimum), 1ms time has elapsed. This is
	 * not really possible but lets make sure we act sanely.
	 * The math will say we should sleep for a very long time, but a sane
	 * response is to sleep for a max of one second */
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    ration);
	fail_unless(sleep_time == MAX_SLEEP_TIME);
}
