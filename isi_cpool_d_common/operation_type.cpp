#include <isi_util/isi_assert.h>
#include <isi_util/isi_format.h>

#include <string.h>

#include "cpool_d_common.h"

#include "operation_type.h"

const cpool_operation_type cpool_operation_type::CP_OT_NONE =
    cpool_operation_type(CPOOL_TASK_TYPE_NONE, false, false,
    DJOB_RID_INVALID);

/*
 * Writeback: regular SBTs / single-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_WRITEBACK =
    cpool_operation_type(CPOOL_TASK_TYPE_WRITEBACK, false, false,
    DJOB_RID_WRITEBACK);

/*
 * Cache invalidation: regular SBTs / single-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_CACHE_INVALIDATION =
    cpool_operation_type(CPOOL_TASK_TYPE_CACHE_INVALIDATION, false, false,
    DJOB_RID_CACHE_INVALIDATION);

/*
 * Local GC: regular SBTs / single-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_LOCAL_GC =
    cpool_operation_type(CPOOL_TASK_TYPE_LOCAL_GC, false, false,
    DJOB_RID_LOCAL_GC);

/*
 * Cloud GC: hashed SBTs / single-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_CLOUD_GC =
    cpool_operation_type(CPOOL_TASK_TYPE_CLOUD_GC, true, false,
    DJOB_RID_CLOUD_GC);

/*
 * Restore COI: hashed SBTs / multi-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_RESTORE_COI =
    cpool_operation_type(CPOOL_TASK_TYPE_RESTORE_COI, true, true,
    DJOB_RID_RESTORE_COI);

/*
 * Archive: regular SBTs / multi-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_ARCHIVE =
    cpool_operation_type(CPOOL_TASK_TYPE_ARCHIVE, false, true);

/*
 * Recall: regular SBTs / multi-job
 */
const cpool_operation_type cpool_operation_type::CP_OT_RECALL =
    cpool_operation_type(CPOOL_TASK_TYPE_RECALL, false, true);

const cpool_operation_type *cpool_operation_type::op_types[] = {
	OT_ARCHIVE, OT_RECALL, OT_WRITEBACK,
	OT_CACHE_INVALIDATION, OT_LOCAL_GC, OT_CLOUD_GC, OT_RESTORE_COI, NULL
};

static void
pack(void **dst, const void *src, int size)
{
	memcpy(*dst, src, size);
	*(char **)dst += size;
}

static void
unpack(void **dst, void *src, int size)
{
	memcpy(src, *dst, size);
	*(char **)dst += size;
}

bool
cpool_operation_type::uses_hsbt(void) const
{
	return uses_hsbt_;
}

bool
cpool_operation_type::is_multi_job(void) const
{
	return multi_job_;
}

djob_id_t
cpool_operation_type::get_single_job_id(void) const
{
	ASSERT(single_job_id_ != DJOB_RID_INVALID,
	    "single-job ID requested for multi-job operation: %{}",
	    task_type_fmt(type_));
	return single_job_id_;
}

task_type
cpool_operation_type::get_type(void) const
{
	return type_;
}

size_t
cpool_operation_type::get_packed_size(void)
{
	return sizeof(task_type);	// type_
}

void
cpool_operation_type::pack(void **stream) const
{
	ASSERT(stream != NULL);

	::pack(stream, &type_, sizeof(type_));
}

const cpool_operation_type *
cpool_operation_type::unpack(void **stream)
{
	ASSERT(stream != NULL);

	task_type temp;
	::unpack(stream, &temp, sizeof temp);

	return get_type(temp);
}

const cpool_operation_type *
cpool_operation_type::get_type(task_type type)
{
	const cpool_operation_type *ret = OT_NONE;

	switch (type) {
	case CPOOL_TASK_TYPE_NONE:
		ret = OT_NONE;
		break;
	case CPOOL_TASK_TYPE_ARCHIVE:
		ret = OT_ARCHIVE;
		break;
	case CPOOL_TASK_TYPE_RECALL:
		ret = OT_RECALL;
		break;
	case CPOOL_TASK_TYPE_WRITEBACK:
		ret = OT_WRITEBACK;
		break;
	case CPOOL_TASK_TYPE_CACHE_INVALIDATION:
		ret = OT_CACHE_INVALIDATION;
		break;
	case CPOOL_TASK_TYPE_LOCAL_GC:
		ret = OT_LOCAL_GC;
		break;
	case CPOOL_TASK_TYPE_CLOUD_GC:
		ret = OT_CLOUD_GC;
		break;
	case CPOOL_TASK_TYPE_RESTORE_COI:
		ret = OT_RESTORE_COI;
		break;
	default:
		ASSERT(false, "unhandled task type: %{}",
		    task_type_fmt(type));
	}

	return ret;
}
