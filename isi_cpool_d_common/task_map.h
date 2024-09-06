#ifndef __CPOOL_TASK_MAP_H__
#define __CPOOL_TASK_MAP_H__

#include "daemon_job_types.h"
#include "operation_type.h"

namespace isi_cloud {
class task;
}

class task_key;

class task_map
{
protected:
	const cpool_operation_type	*op_type_;
	bool				uses_hsbt_;

	/**
	 * Claim a task as being actively worked on.
	 *
	 * @param[in]  task		the task to claim
	 * @param[out] error_out	any error encountered during the
	 * operation
	 *
	 * @return whether or not the task was claimed
	 */
	bool claim_task(const isi_cloud::task *task,
	    struct isi_error **error_out) const;

	/**
	 * Relinquish an active-work claim on a task.
	 *
	 * @param[in]  task		the task for which the claim is to be
	 * relinquished
	 * @param[out] error_out	any error encountered during the
	 * operation
	 *
	 * @return whether or not the task was relinquished
	 */
	bool relinquish_task(const isi_cloud::task *task,
	    struct isi_error **error_out) const;

	/* find_task helper functions */
	isi_cloud::task *find_task_pending(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const;

	isi_cloud::task *find_task_inprogress(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const;

	void read_entry_from_sbt(int sbt_fd, const task_key *key,
	    void *&entry_buf, size_t &entry_size,
	    struct isi_error **error_out) const;

	/* Default construction is not allowed. */
	task_map();

	/* Copy construction and assignment are not currently implemented. */
	task_map(const task_map &);
	task_map &operator=(const task_map &);

public:
	task_map(const cpool_operation_type *type);
	virtual ~task_map();

	const cpool_operation_type *get_op_type(void) const;
	void set_op_type(const cpool_operation_type *op_type);

	/**
	 * Lock a task.  If editing/moving the task, the exclusive flag should
	 * be set.
	 *
	 * @param[in]  key		identifies the task to lock
	 * @param[in]  block		whether or not to block waiting for the
	 * lock
	 * @param[in]  exclusive	whether or not to take an exclusive or
	 * shared lock
	 * @param[out] error		any error encountered during the
	 * operation
	 */
	void lock_task(const task_key *key, bool block, bool exclusive,
	    struct isi_error **error_out) const;

	/**
	 * Unlock a previously-locked task.
	 *
	 * @param[in]  key		identifies the task to unlock
	 * @param[out] error		any error encountered during the
	 * operation
	 */
	void unlock_task(const task_key *key,
	    struct isi_error **error_out) const;

	// return value must be deleted
	virtual isi_cloud::task *find_task(const task_key *key,
	    bool ignore_ENOENT, struct isi_error **error_out) const;
};

#endif // __CPOOL_TASK_MAP_H__
