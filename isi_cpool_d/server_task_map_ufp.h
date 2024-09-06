#ifndef __ISI_CPOOL_D_SERVER_TASK_MAP_UFP_H__
#define __ISI_CPOOL_D_SERVER_TASK_MAP_UFP_H__

#include <isi_ufp/isi_ufp.h>

/*
 * Failpoints for server_task_map use ENOTSOCK to identify injected failures as
 * it is very unlikely to occur organically.
 */

#define UFP_READ_TASK_ERROR \
	UFAIL_POINT_CODE(cpool_server_task_map__read_task_failure, {	\
		if (error == NULL) {					\
			delete task;					\
			task = NULL;					\
									\
			error = isi_system_error_new(ENOTSOCK,		\
			    "user failpoint hit for "			\
			    "start_task_helper__read_task");		\
		}							\
	});

#define UFP_QUALIFY_TASK_ERROR \
	UFAIL_POINT_CODE(cpool_server_task_map__qualify_task_failure, {	\
		if (error == NULL) {					\
			error = isi_system_error_new(ENOTSOCK,		\
			    "user failpoint hit for "			\
			    "start_task_helper__qualify_task");		\
		}							\
	});

#define UFP_FINALIZE_ERROR \
	UFAIL_POINT_CODE(cpool_server_task_map__finalize_failure, {	\
		if (error == NULL) {					\
			error = isi_system_error_new(ENOTSOCK,		\
			    "user failpoint hit for "			\
			    "start_task_helper__qualify_task");		\
		}							\
	});

#endif // __ISI_CPOOL_D_SERVER_TASK_MAP_UFP_H__
