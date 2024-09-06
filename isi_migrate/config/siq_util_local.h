#ifndef __SIQ_UTIL_LOCAL_H__
#define __SIQ_UTIL_LOCAL_H__

#include <sys/param.h>
#include "siq_util.h"

/* A helper structure for mapping enum values to and from strings */
typedef struct siq_enum_string_map {
	int def_val;
	const char *def_str;
	int nvals;
	const char * const *strs;
} siq_enum_string_map_t;

const char *siq_enum_to_str(const siq_enum_string_map_t map, int val);
int siq_str_to_enum(const siq_enum_string_map_t map, const char *str);

/* Universal buffer size for operations with MXD.  We must be sure that a user
 * can enter a long description (or even name) */
static const int bufsz = MAXPATHLEN+1;

#endif /* __SIQ_UTIL_LOCAL_H__ */
