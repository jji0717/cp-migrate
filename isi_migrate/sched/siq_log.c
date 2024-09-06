#include <stdbool.h>
#include <string.h>
#include <syslog.h>

#include <isi_util/isi_assert.h>

#define USE_BURST_LOGGING
#include "siq_log.h"
#include "isi_burst_sdk/include/hostinterface.h"

static enum severity severity = INFO;

int
cur_loglevel(void)
{
	return severity;
}

//Force a message to use syslog and skip isi_ilog.
//Used for Python/PAPI-only interfaces
void
log_syslog(int level, const char *msg, ...)
{
	va_list ap;
	char *message = NULL;
	int ret;
	
	va_start(ap, msg);
	ret = vasprintf(&message, msg, ap);
	ASSERT(ret != -1);
	va_end(ap);

	openlog(MIGRATE_LOG_NAME, LOG_CONS | LOG_PID, LOG_USER);
	syslog(level, message);
	closelog();

	free(message);
	message = NULL;
}

//Burst Logging integration

struct LoggingHostInterfaceImpl
{
	LoggingHostInterface _p;
};

static void vlog(struct _LoggingHostInterface* _this, ewoc_LogSeverity level,
    const char* module, const char* format, va_list ap)
{
	char *logString = NULL;

	if (!ilog_will_log_level(siq_to_islog_type_map[DEBUG]))
		return;
	
	vasprintf(&logString, format, ap);
	ASSERT(logString);

	//Log to ilog
	log(DEBUG, "[%s]: %s", module, logString);
	free(logString);
}

static LoggingHostInterface* getLoggingInterface(HostInterface* that)
{
	struct HostInterfaceImpl* this = (struct HostInterfaceImpl*)that;
	return this->_log;
}

static void* getSystemContext(HostInterface* that)
{
	struct HostInterfaceImpl* this = (struct HostInterfaceImpl*)that;
	return this->_sysContext;
}

static void setSystemContext(struct HostInterfaceImpl* this, void* context)
{
	this->_sysContext = context;
}

rde_status_t getHostInterfaceImpl(struct HostInterfaceImpl* *res)
{
	*res = calloc(1, sizeof(struct HostInterfaceImpl));
	ASSERT(res);

	(*res)->_p.getLoggingInterface = getLoggingInterface;
	(*res)->_p.getSystemContext = getSystemContext;
	(*res)->setSystemContext = setSystemContext;
	(*res)->_log = 0;
	(*res)->_sysContext = 0;

	(*res)->_log = calloc(1, sizeof(struct LoggingHostInterfaceImpl));
	ASSERT((*res)->_log);
	(*res)->_log->vlog = vlog;

	return RDE_STATUS_OK;
}

rde_status_t releaseHostInterfaceImpl(struct HostInterfaceImpl* res)
{
	if (res != NULL) {
		free(res->_log);
	}
	free(res);
	res = NULL;
	return RDE_STATUS_OK;
}
