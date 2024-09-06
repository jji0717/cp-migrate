#ifndef SIQ_LOG_H
#define SIQ_LOG_H

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "siq_jobctl.h"
#include "isi_migrate/config/siq_job.h"
#include "siq_glob_param_load.h"

#define MIGRATE_LOG_NAME "isi_migrate"
#define PRINT_BOOL(x) x ? "true" : "false"
#define PRINT_NULL(x) x ? x : "NULL"

#define log(sev, fmt, ...) ilog(siq_to_islog_type_map[sev], fmt, ##__VA_ARGS__)

int cur_loglevel(void);
void log_syslog(int level, const char *msg, ...);

#ifdef USE_BURST_LOGGING
#include "isi_burst_sdk/include/burst.h"
#include "isi_burst_sdk/include/hostinterface.h"

struct HostInterfaceImpl
{
	HostInterface		_p;
	LoggingHostInterface*	_log;
	void*			_sysContext;
	void			(*setSystemContext)
				    (struct HostInterfaceImpl* _this, 
				    void* context);
};

rde_status_t getHostInterfaceImpl(struct HostInterfaceImpl* * res);
rde_status_t releaseHostInterfaceImpl(struct HostInterfaceImpl* res);
#endif /* USE_BURST_LOGGING */

#endif /* SIQ_LOG_H */
