#ifndef SIQ_GLOB_PARAM_LOAD_H
#define SIQ_GLOB_PARAM_LOAD_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <ctype.h>
#include <sys/param.h>

#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "siq_jobctl.h"
#include "isi_migrate/config/siq_job.h"

enum siq_state sched_get_siq_state(void);
time_t sched_get_quorum_poll_interval(void);
time_t sched_get_policies_poll_interval(void);
const char *sched_get_siq_dir(void);
const char *sched_get_log_dir(void);
const char *sched_get_log_name(void);
const char *sched_get_del_files(void);
const char *sched_get_sched_dir(void);
const char *sched_get_reports_dir(void);
const char *sched_get_edit_dir(void);
const char *sched_get_jobs_dir(void);
const char *sched_get_run_dir(void);
const char *sched_get_trash_dir(void);
const char *sched_get_lock_dir(void);
const char *sched_get_snap_dir(void);
const char *sched_get_pidfile(void);
const char *sched_get_version(void);
const char *sched_get_full_log_name(void);
int sched_get_log_maxsize(void);
int get_max_concurrent_jobs(struct siq_gc_conf *conf);
int sched_get_max_concurrent_jobs(void);
int sched_get_min_workers_per_policy(void);
int sched_get_log_level(void);
void set_fake_dirs(const char *fake_base_dir);

#if defined(__cplusplus)
}
#endif


#endif /* SIQ_GLOB_PARAM_LOAD_H */

