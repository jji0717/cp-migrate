#ifndef SIQ_JOBCTL_PRIVATE_H_
#define SIQ_JOBCTL_PRIVATE_H_

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/param.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>

#include "isi_date/date.h"
#include "isi_snapshot/snapshot.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_policy.h"
#include "siq_jobctl.h"
#include "siq_log.h"
#include "siq_glob_param_load.h"

#include "siq.h"

#include "sched_main.h"

typedef enum {
  SIQ_JC_PAUSE = 0,
  SIQ_JC_RESUME,
  SIQ_JC_CANCEL,
  SIQ_JC_TOTAL
} siq_job_control_t;

#define SEARCH_FILE_COUNT	3

struct search_file {
	const char	*file_name;
	enum siq_job_state	job_state;
};

static int job_file_add_rm(char *j_id, siq_job_control_t jc);
static enum sched_ret job_dir_search(const char *j_id, char *job_dir);
static enum sched_ret job_control(const char *job_dir, siq_job_control_t jc);
static enum sched_ret is_file_exist(const char *path, const char *file_name);
static void remove_all_locks(SNAP *snap);

#endif /*SIQ_JOBCTL_PRIVATE_H_*/
