#ifndef __SIQ_UTIL_P_H__
#define __SIQ_UTIL_P_H__

#include <string.h>
#include <syslog.h>
#include <stdlib.h>
#include <sys/param.h>
#include <stdio.h>

#include "siq_util_local.h"

const char *siq_log_level_strs_gp[SEVERITY_TOTAL+1] = {
	"fatal",
	"error",
	"notice",
	"info",
	"copy",
	"debug",
	"trace",
	NULL
};

static const siq_enum_string_map_t siq_log_level_str_map_gp = {
	TRACE,
	"trace",
	SIZEM1(siq_log_level_strs_gp)/sizeof(char *),
	siq_log_level_strs_gp
};

#endif /* __SIQ_UTIL_P_H__ */
