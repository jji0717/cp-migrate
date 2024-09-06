#include <stdarg.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <isi_cpool_d_common/cpool_d_debug.h>

#include "scheduler.h"

SUITE_DEFINE_FOR_FILE(check_scheduler,
	.mem_check = CK_MEM_LEAKS,
	.timeout = 0,
	.fixture_timeout = 1200);

static void
scheduler_test_runner(const char *schedule_info, time_t starting_from,
    int num_times, time_t *expected_times)
{
	ASSERT(schedule_info != NULL);
	ASSERT(num_times > 0, "num_times: %d", num_times);
	ASSERT(expected_times != NULL);

	scheduler s;

	for (int i = 0; i < num_times; ++i) {
		struct isi_error *error = NULL;
		time_t next_time = s.get_next_time(schedule_info,
		    starting_from, &error);
		fail_if (error != NULL, "unexpected error (i: %d): %#{}",
		    i, isi_error_fmt(error));
		fail_if (next_time != expected_times[i],
		    "next time mismatch (i: %d exp: %d act: %d)",
		    i, expected_times[i], next_time);

		starting_from = next_time + 1;
	}
}

/*
 * Every time is valid, so scheduler should return consecutive values.
 * Starting from 100 (Thu 01 Jan 1970 00:01:40 GMT), the next 10 times that
 * match this schedule are:
 *	100	Thu 01 Jan 1970 00:01:40 GMT
 *	101	Thu 01 Jan 1970 00:01:41 GMT
 *	102	Thu 01 Jan 1970 00:01:42 GMT
 *	103	Thu 01 Jan 1970 00:01:43 GMT
 *	104	Thu 01 Jan 1970 00:01:44 GMT
 *	105	Thu 01 Jan 1970 00:01:45 GMT
 *	106	Thu 01 Jan 1970 00:01:46 GMT
 *	107	Thu 01 Jan 1970 00:01:47 GMT
 *	108	Thu 01 Jan 1970 00:01:48 GMT
 *	109	Thu 01 Jan 1970 00:01:49 GMT
 */
TEST(test__ALL_ALL_ALL_ALL_ALL_ALL)
{
	/* starting time is 01-Jan-1970 00:16:40 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 40;
	start_localtime.tm_min = 16;
	start_localtime.tm_hour = 0;
	start_localtime.tm_mday = 1;
	start_localtime.tm_mon = 0;
	start_localtime.tm_year = 70;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	time_t expected_times[10];
	for (int i = 0; i < 10; ++i)
		expected_times[i] = starting_time + i;

	scheduler_test_runner("* * * * * *", starting_time,
	    10, expected_times);
}

/*
 * Any time occurring at 10 seconds past the minute is valid.
 */
TEST(test__10_ALL_ALL_ALL_ALL_ALL)
{
	/* starting time is 01-Jan-1970 00:16:40 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 40;
	start_localtime.tm_min = 16;
	start_localtime.tm_hour = 0;
	start_localtime.tm_mday = 1;
	start_localtime.tm_mon = 0;
	start_localtime.tm_year = 70;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	/*
	 * The earliest expected time is 30 seconds later, then a minute and 30
	 * seconds later, two minutes and 30 seconds later, etc.
	 */
	time_t expected_times[10];
	for (int i = 0; i < 10; ++i)
		expected_times[i] = starting_time + 60 * i + 30;

	scheduler_test_runner("10 * * * * *", starting_time,
	    10, expected_times);
}

/*
 * Any time occurring in the 14th minute of any hour is valid.
 */
TEST(test__ALL_14_ALL_ALL_ALL_ALL)
{
	/* starting time is 01-Jan-1970 03:14:57 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 57;
	start_localtime.tm_min = 14;
	start_localtime.tm_hour = 3;
	start_localtime.tm_mday = 1;
	start_localtime.tm_mon = 0;
	start_localtime.tm_year = 70;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);
	
	time_t expected_times[] = {
		starting_time,
		starting_time + 1,
		starting_time + 2,		/* Thu 01 Jan 1970 03:14:59 */
		starting_time + 59*60 + 3,	/* Thu 01 Jan 1970 04:14:00 */
		starting_time + 59*60 + 4,
		starting_time + 59*60 + 5,
		starting_time + 59*60 + 6,
		starting_time + 59*60 + 7,
		starting_time + 59*60 + 8,
		starting_time + 59*60 + 9
	};

	scheduler_test_runner("* 14 * * * *", starting_time,
	    10, expected_times);
}

/*
 * Any time occurring in the 11th hour of any day is valid.
 */
TEST(test__ALL_ALL_11_ALL_ALL_ALL)
{
	/* starting time is 03-Feb-1971 10:59:34 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 34;
	start_localtime.tm_min = 59;
	start_localtime.tm_hour = 10;
	start_localtime.tm_mday = 3;
	start_localtime.tm_mon = 1;
	start_localtime.tm_year = 71;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	time_t expected_times[] = {
		starting_time + 26,
		starting_time + 27,
		starting_time + 28,
		starting_time + 29,
		starting_time + 30,
		starting_time + 31,
		starting_time + 32,
		starting_time + 33,
		starting_time + 34,
		starting_time + 35
	};

	scheduler_test_runner("* * 11 * * *", starting_time,
	    10, expected_times);
}

/*
 * Any time occurring on the 4th day of any month is valid.
 */
TEST(test__ALL_ALL_ALL_4_ALL_ALL)
{
	/* starting time is 03-Feb-1971 23:00:00 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 0;
	start_localtime.tm_min = 0;
	start_localtime.tm_hour = 23;
	start_localtime.tm_mday = 3;
	start_localtime.tm_mon = 1;
	start_localtime.tm_year = 71;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	/*
	 * The first expected time is an hour after start, then each second
	 * after that.
	 */
	time_t expected_times[10];
	for (int i = 0; i < 10; ++i)
		expected_times[i] = starting_time + 3600 + i;

	scheduler_test_runner("* * * 4 * *", starting_time,
	    10, expected_times);
}

/*
 * Any time occurring in April is valid.
 */
TEST(test__ALL_ALL_ALL_ALL_3_ALL)
{
	/* starting time is 31-Mar-1971 23:00:00 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 0;
	start_localtime.tm_min = 0;
	start_localtime.tm_hour = 23;
	start_localtime.tm_mday = 31;
	start_localtime.tm_mon = 2;
	start_localtime.tm_year = 71;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	/*
	 * The first expected time is an hour after start, then each second
	 * after that.
	 */
	time_t expected_times[10];
	for (int i = 0; i < 10; ++i)
		expected_times[i] = starting_time + 3600 + i;

	scheduler_test_runner("* * * * 3 *", starting_time,
	    10, expected_times);
}

/*
 * Each midnight occurring on a Sunday is valid.
 */
TEST(test__0_0_0_ALL_ALL_0)
{
	/* starting time is (Sat) 6-Feb-1971 22:00:00 local time */
	struct tm start_localtime;
	start_localtime.tm_sec = 0;
	start_localtime.tm_min = 0;
	start_localtime.tm_hour = 22;
	start_localtime.tm_mday = 6;
	start_localtime.tm_mon = 1;
	start_localtime.tm_year = 71;
	start_localtime.tm_isdst = -1;

	time_t starting_time = mktime(&start_localtime);

	/*
	 * The first expected time is two hours after start, then a week
	 * seperates each successive expected time.
	 */
	time_t expected_times[10];
	for (int i = 0; i < 10; ++i)
		expected_times[i] = starting_time + 7200 + i * 7*24*60*60;

	scheduler_test_runner("0 0 0 * * 0", starting_time,
	    10, expected_times);
}
