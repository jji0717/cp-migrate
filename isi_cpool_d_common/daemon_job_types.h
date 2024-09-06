#ifndef __CPOOL_DAEMON_JOB_TYPES_H__
#define __CPOOL_DAEMON_JOB_TYPES_H__

#include <stdint.h>

typedef uint32_t djob_id_t;

/*
 * Values added to explicitly show that the ordering of these values matters,
 * even though that's kind of an abuse of enums; see
 * daemon_job_manager::get_state(task, error).
 */
enum djob_op_job_task_state_value {
	DJOB_OJT_NONE		= 0,
	DJOB_OJT_RUNNING	= 1,
	DJOB_OJT_PAUSED		= 2,
	DJOB_OJT_CANCELLED	= 3,
	DJOB_OJT_ERROR		= 4,
	DJOB_OJT_COMPLETED	= 5,

	/*
	 * This state is used solely for reporting and its value can float
	 * (unlike the others).
	 */
	DJOB_OJT_PENDING,

	/*
	 * This state is used to stop tasks on shutdown and its value can
	 * float.
	 */
	DJOB_OJT_STOPPED
};

/*
 * Ensure the ordering of these values since
 * daemon_job_manager::get_state(task, error) is dependent on it.
 */
CTASSERT(DJOB_OJT_NONE < DJOB_OJT_RUNNING);
CTASSERT(DJOB_OJT_RUNNING < DJOB_OJT_PAUSED);
CTASSERT(DJOB_OJT_PAUSED < DJOB_OJT_CANCELLED);
CTASSERT(DJOB_OJT_CANCELLED < DJOB_OJT_ERROR);
CTASSERT(DJOB_OJT_ERROR < DJOB_OJT_COMPLETED);

typedef enum djob_op_job_task_state_value djob_op_job_task_state;

struct fmt_conv_ctx djob_op_job_task_state_fmt(
    enum djob_op_job_task_state_value);

/*
 * Do not change the values of this enum as they are used as array indices and
 * for serialization/deserialization of objects.
 */
enum task_type_value {
	CPOOL_TASK_TYPE_NONE			= -1, // needed?
	CPOOL_TASK_TYPE_ARCHIVE			=  0,
	CPOOL_TASK_TYPE_RECALL			=  1,
	CPOOL_TASK_TYPE_WRITEBACK		=  2,
	CPOOL_TASK_TYPE_CACHE_INVALIDATION	=  3,
	CPOOL_TASK_TYPE_LOCAL_GC		=  4,
	CPOOL_TASK_TYPE_CLOUD_GC		=  5,
	CPOOL_TASK_TYPE_RESTORE_COI		=  6,
	CPOOL_TASK_TYPE_MAX_TASK_TYPE  // must be last
};

typedef enum task_type_value task_type;

struct fmt_conv_ctx task_type_fmt(enum task_type_value);

enum task_process_completion_reason_value {
	CPOOL_TASK_SUCCESS,
	CPOOL_TASK_PAUSED,
	CPOOL_TASK_CANCELLED,
	CPOOL_TASK_STOPPED,
	CPOOL_TASK_NON_RETRYABLE_ERROR,
	CPOOL_TASK_RETRYABLE_ERROR
};

typedef enum task_process_completion_reason_value
    task_process_completion_reason;

struct fmt_conv_ctx
    task_process_completion_reason_fmt(
    enum task_process_completion_reason_value);

enum task_location_value {
	TASK_LOC_NONE,
	TASK_LOC_PENDING,
	TASK_LOC_INPROGRESS
};

typedef enum task_location_value task_location;

struct fmt_conv_ctx task_location_fmt(enum task_location_value);

#endif // __CPOOL_DAEMON_JOB_TYPES_H__
