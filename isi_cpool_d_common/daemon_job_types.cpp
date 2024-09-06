#include <isi_util/isi_format.h>

#include "daemon_job_types.h"

MAKE_ENUM_FMT(djob_op_job_task_state, enum djob_op_job_task_state_value,
	ENUM_VAL(DJOB_OJT_NONE, "none"),
	ENUM_VAL(DJOB_OJT_RUNNING, "running"),
	ENUM_VAL(DJOB_OJT_PAUSED, "paused"),
	ENUM_VAL(DJOB_OJT_CANCELLED, "cancelled"),
	ENUM_VAL(DJOB_OJT_ERROR, "error"),
	ENUM_VAL(DJOB_OJT_COMPLETED, "completed"),
	ENUM_VAL(DJOB_OJT_PENDING, "pending"),
);

MAKE_ENUM_FMT(task_type, enum task_type_value,
	ENUM_VAL(CPOOL_TASK_TYPE_ARCHIVE, "archive"),
	ENUM_VAL(CPOOL_TASK_TYPE_RECALL, "recall"),
	ENUM_VAL(CPOOL_TASK_TYPE_WRITEBACK, "writeback"),
	ENUM_VAL(CPOOL_TASK_TYPE_CACHE_INVALIDATION, "cache-invalidation"),
	ENUM_VAL(CPOOL_TASK_TYPE_LOCAL_GC, "local-gc"),
	ENUM_VAL(CPOOL_TASK_TYPE_CLOUD_GC, "cloud-gc"),
	ENUM_VAL(CPOOL_TASK_TYPE_RESTORE_COI, "restore-coi"),
);

MAKE_ENUM_FMT(task_process_completion_reason,
    enum task_process_completion_reason_value,
        ENUM_VAL(CPOOL_TASK_SUCCESS, "success"),
        ENUM_VAL(CPOOL_TASK_PAUSED, "paused"),
        ENUM_VAL(CPOOL_TASK_CANCELLED, "cancelled"),
        ENUM_VAL(CPOOL_TASK_STOPPED, "stopped"),
        ENUM_VAL(CPOOL_TASK_NON_RETRYABLE_ERROR, "non-retryable error"),
        ENUM_VAL(CPOOL_TASK_RETRYABLE_ERROR, "retryable error")
);

MAKE_ENUM_FMT(task_location, enum task_location_value,
	ENUM_VAL(TASK_LOC_NONE, "none"),
	ENUM_VAL(TASK_LOC_PENDING, "pending"),
	ENUM_VAL(TASK_LOC_INPROGRESS, "in-progress")
);
