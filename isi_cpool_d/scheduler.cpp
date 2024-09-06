#include <ctype.h>
#include <errno.h>

#include <isi_cpool_d_common/cpool_d_debug.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>

#define SCHED_PRIVATE
#include "scheduler_private.h"
#undef SCHED_PRIVATE

#include "scheduler.h"

schedule_item::schedule_item(int min, int max)
	: min_(min), max_(max)
{
	ASSERT(min >= 0, "non-negative minimum required (%d)", min);
	ASSERT(max >= min, "minimum (%d) cannot exceed maximum (%d)",
	    min, max);
}

schedule_item::~schedule_item()
{
}

void
schedule_item::parse(const char *token_string, struct isi_error **error_out)
{
	ASSERT(token_string != NULL);

	struct isi_error *error = NULL;

	if (strcmp(token_string, "*") == 0) {
		for (int i = min_; i <= max_; ++i)
			valid_values_.insert(i);
	}
	else {
		char *token_string_dup = strdup(token_string);
		ASSERT(token_string_dup != NULL);

		char *save_ptr;
		for (char *token = strtok_r(token_string_dup, ",", &save_ptr);
		    token != NULL; token = strtok_r(NULL, ",", &save_ptr))
			// should check for valid ints here
			valid_values_.insert(atoi(token));

		free(token_string_dup);
	}

	/* Validate the parsed values. */
	std::set<int>::const_iterator iter = valid_values_.begin();
	for (; iter != valid_values_.end(); ++iter) {
		if (*iter < min_ || *iter > max_) {
			error = isi_system_error_new(EINVAL,
			    "invalid value: %d", *iter);
			goto out;
		}
	}

 out:
	isi_error_handle(error, error_out);
}

void
schedule_item::advance(int &start, bool &advanced, bool &overflowed)
{
	advanced = false;
	overflowed = false;
	std::set<int>::const_iterator iter;

	if (valid_values_.find(start) == valid_values_.end()) {
		/*
		 * start isn't a valid value, so we need to advance it (which
		 * may cause overflow) - first try to advance it to the lowest
		 * valid value greater than start.
		 */
		advanced = true;
		iter = valid_values_.upper_bound(start);
		if (iter != valid_values_.end()) {
			/*
			 * We found a valid value greater than start (no
			 * overflow needed).
			 */
			start = *iter;
		}
		else {
			/*
			 * start is greater than any valid value which means
			 * overflow.
			 */
			iter = valid_values_.begin();
			start = *iter;
			overflowed = true;
		}
	}
}

bool
schedule_item::is_valid(int value) const
{
	return (valid_values_.find(value) != valid_values_.end());
}

int
schedule_item::reset(void) const
{
	ASSERT(valid_values_.size() >= 1, "no valid values specified");
	return *(valid_values_.begin());
}

scheduler::scheduler()
{
}

scheduler::~scheduler()
{
}

void
scheduler::validate_string(const char *string, struct isi_error **error_out)
{
	ASSERT(string != NULL);

	struct isi_error *error = NULL;
	const unsigned int len = strlen(string);
	for (unsigned int i = 0; i < len; ++i) {
		switch (string[i]) {
			case '*':
			case ',':
			case ' ':
				break;
			default:
				if (isdigit(string[i]))
					continue;

				error = isi_system_error_new(EINVAL,
				    "invalid characters found in schedule info."
				    "  Only digits, whitespace, asterisks "
				    "and commas allowed: \"%s\"", string);
				goto out;
				break;
		}
	}
 out:
	isi_error_handle(error, error_out);
}

void
scheduler::parse(const char *schedule_info, struct isi_error **error_out)
{
	ASSERT (schedule_info != NULL);

	struct isi_error *error = NULL;

	/* Parse the incoming schedule information. */
	char *schedule_info_dup = strdup(schedule_info);
	ASSERT(schedule_info_dup != NULL);

	char *save_ptr;
	char *token = strtok_r(schedule_info_dup, " ", &save_ptr);

	validate_string(schedule_info_dup, &error);
	ON_ISI_ERROR_GOTO(out, error);

	for (int i = 0; i < 6; ++i) {
		if (token == NULL) {
			error = isi_system_error_new(EINVAL,
			    "invalid schedule info (missing token for i = %d)",
			    i);
			goto out;
		}

		switch (i) {
		case 0:
			second_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		case 1:
			minute_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		case 2:
			hour_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		case 3:
			day_of_month_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		case 4:
			month_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		case 5:
			day_of_week_.parse(token, &error);
			ON_ISI_ERROR_GOTO(out, error);
			break;
		}

		token = strtok_r(NULL, " ", &save_ptr);
	}

 out:
	free(schedule_info_dup);

	isi_error_handle(error, error_out);
}

time_t
scheduler::get_next_time(const char *schedule_info, time_t starting_from,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	time_t local_starting_from = starting_from, next_time = 0;
	bool ok = false, advanced, overflowed;

	parse(schedule_info, &error);
	ON_ISI_ERROR_GOTO(out, error);

	do {
		struct tm next_tm;
		localtime_r(&local_starting_from, &next_tm);

		/*
		 * For each granularity (second, minute, hour, day of month,
		 * month) advance to the next value greater than or equal to
		 * the current value of that granularity.  If such an advance
		 * is made, reset all finer granularities to their lowest
		 * values.  If such an advance causes an overflow (e.g. 58
		 * seconds advancing to 10 seconds), increment the value one
		 * level of granularity up.
		 */

		/* seconds */
		second_.advance(next_tm.tm_sec, advanced, overflowed);
		if (advanced) {
			/*
			 * Nothing to reset as seconds is the finest
			 * granularity.
			 */
			if (overflowed)
				++next_tm.tm_min;
		}

		/* minutes */
		minute_.advance(next_tm.tm_min, advanced, overflowed);
		if (advanced) {
			next_tm.tm_sec = second_.reset();

			if (overflowed)
				++next_tm.tm_hour;
		}

		/* hours */
		hour_.advance(next_tm.tm_hour, advanced, overflowed);
		if (advanced) {
			next_tm.tm_min = minute_.reset();
			next_tm.tm_sec = second_.reset();

			if (overflowed)
				++next_tm.tm_mday;
		}

		/* day of month */
		day_of_month_.advance(next_tm.tm_mday, advanced, overflowed);
		if (advanced) {
			next_tm.tm_hour = hour_.reset();
			next_tm.tm_min = minute_.reset();
			next_tm.tm_sec = second_.reset();

			if (overflowed)
				++next_tm.tm_mon;
		}

		/* month */
		month_.advance(next_tm.tm_mon, advanced, overflowed);
		if (advanced) {
			next_tm.tm_mday = day_of_month_.reset();
			next_tm.tm_hour = hour_.reset();
			next_tm.tm_min = minute_.reset();
			next_tm.tm_sec = second_.reset();

			if (overflowed)
				++next_tm.tm_year;
		}

		/*
		 * Convert next_tm to a time_t.  We'll need to tell mktime that
		 * it should figure out whether or not DST is in effect.
		 */
		next_tm.tm_isdst = -1;
		next_time = mktime(&next_tm);

		/*
		 * We've created a potential next time from the schedule
		 * information, but it might not match the schedule information
		 * for two possible reasons: (1) mktime doesn't care about
		 * invalid dates and will resolve them on a relative basis
		 * (e.g. April 31st will be treated the same as May 1st) and
		 * (2) the day_of_week is calculated from the other fields of
		 * next_tm and thus must be verified after mktime is called.
		 * Therefore, we dissect our next_time candidate back into its
		 * parts and compare.
		 */
		struct tm normalized_tm;
		localtime_r(&next_time, &normalized_tm);

		if (second_.is_valid(normalized_tm.tm_sec) &&
		    minute_.is_valid(normalized_tm.tm_min) &&
		    hour_.is_valid(normalized_tm.tm_hour) &&
		    day_of_month_.is_valid(normalized_tm.tm_mday) &&
		    month_.is_valid(normalized_tm.tm_mon) &&
		    day_of_week_.is_valid(normalized_tm.tm_wday))
			ok = true;

		/*
		 * In case we had an invalid time, prepare for the next
		 * attempt.  (In the case where we had a valid time, this is
		 * innocuous.)
		 */
		local_starting_from = next_time + 1;
	} while(!ok);

 out:
	isi_error_handle(error, error_out);

	return next_time;
}
