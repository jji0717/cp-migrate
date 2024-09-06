#ifndef __CPOOL_OPERATION_TYPE_H__
#define __CPOOL_OPERATION_TYPE_H__

#include <stddef.h>
#include <stdlib.h>

#include "daemon_job_types.h"

#define OT_NONE			&cpool_operation_type::CP_OT_NONE
#define OT_ARCHIVE		&cpool_operation_type::CP_OT_ARCHIVE
#define OT_RECALL		&cpool_operation_type::CP_OT_RECALL
#define OT_WRITEBACK		&cpool_operation_type::CP_OT_WRITEBACK
#define OT_CACHE_INVALIDATION	&cpool_operation_type::CP_OT_CACHE_INVALIDATION
#define OT_LOCAL_GC		&cpool_operation_type::CP_OT_LOCAL_GC
#define OT_CLOUD_GC		&cpool_operation_type::CP_OT_CLOUD_GC
#define OT_RESTORE_COI		&cpool_operation_type::CP_OT_RESTORE_COI

#define OT_FOREACH(var) \
	(var) = cpool_operation_type::op_types[0]; \
	for (int i = 0; (var) != NULL; \
	    (var) = cpool_operation_type::op_types[++i])

/*
 * Wanted to use something like (sizeof *cpool_operation_type::op_types /
 * sizeof cpool_operation_type::op_types[0]) here, but wasn't able to figure
 * it out.
 */
#define OT_NUM_OPS 7

class cpool_operation_type
{
public:
	static const cpool_operation_type CP_OT_NONE;
	static const cpool_operation_type CP_OT_ARCHIVE;
	static const cpool_operation_type CP_OT_RECALL;
	static const cpool_operation_type CP_OT_WRITEBACK;
	static const cpool_operation_type CP_OT_CACHE_INVALIDATION;
	static const cpool_operation_type CP_OT_LOCAL_GC;
	static const cpool_operation_type CP_OT_CLOUD_GC;
	static const cpool_operation_type CP_OT_RESTORE_COI;

	static const cpool_operation_type *op_types[];

	bool uses_hsbt(void) const;
	bool is_multi_job(void) const;
	djob_id_t get_single_job_id(void) const;
	task_type get_type(void) const;

	static size_t get_packed_size(void);
	void pack(void **stream) const;
	static const cpool_operation_type *unpack(void **stream);
	static const cpool_operation_type *get_type(task_type type);

protected:
	task_type type_;
	bool uses_hsbt_;
	bool multi_job_;
	djob_id_t single_job_id_;

private:
	cpool_operation_type(task_type type, bool uses_hsbt,
	    bool is_multi_job)
		: type_(type), uses_hsbt_(uses_hsbt), multi_job_(is_multi_job)
		, single_job_id_(-1)
	{ }

	cpool_operation_type(task_type type, bool uses_hsbt,
	    bool is_multi_job, int single_job_id)
		: type_(type), uses_hsbt_(uses_hsbt), multi_job_(is_multi_job)
		, single_job_id_(single_job_id)
	{ }
};

#endif // __CPOOL_OPERATION_TYPE_H__
