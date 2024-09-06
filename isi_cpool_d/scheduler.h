#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

#include <time.h>

#include <set>

#define SCHED_PRIVATE
#include "scheduler_private.h"
#undef SCHED_PRIVATE

class scheduler
{
protected:
	schedule_item__second			second_;
	schedule_item__minute			minute_;
	schedule_item__hour			hour_;
	schedule_item__day_of_month		day_of_month_;
	schedule_item__month			month_;
	schedule_item__day_of_week		day_of_week_;

	void parse(const char *schedule_info, struct isi_error **error_out);

	/* Copy construction and assignment are not allowed. */
	scheduler(const scheduler &);
	scheduler &operator=(const scheduler &);

	void validate_string(const char *string, struct isi_error **error_out);
public:
	scheduler();
	~scheduler();

	time_t get_next_time(const char *schedule_info, time_t starting_from,
	    struct isi_error **error_out);
};

#endif // __SCHEDULER_H__
