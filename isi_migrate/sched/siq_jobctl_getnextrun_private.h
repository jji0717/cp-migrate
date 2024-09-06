#ifndef _SIQ_JOBCTL_GETNEXTRUN_PRIVATE_H_
#define _SIQ_JOBCTL_GETNEXTRUN_PRIVATE_H_

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <limits.h>

#include "isi_date/date.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_policy.h"
#include "siq_jobctl.h"


static int
sched_check_for_work(const struct siq_policy *policy, time_t prev_start, 
    time_t prev_end, time_t since, time_t *pnext_start, int once);
static int
get_schedule_info(char *schedule, char **recur_str, time_t *base_time);

#endif /* _SIQ_JOBCTL_GETNEXTRUN_PRIVATE_H_ */
