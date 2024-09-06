#ifndef __ISI_CPOOL_D_SERVER_TASK_MAP_H__
#define __ISI_CPOOL_D_SERVER_TASK_MAP_H__

#include <vector>

#include <isi_cpool_d_common/scoped_object_list.h>
#include <isi_cpool_d_common/task_map.h>
#include <isi_cpool_d_common/ckpt.h>
#include <isi_util_cpp/scoped_rwlock.h>

class server_task_map : public task_map
{
public:
	class read_task_ctx {
	public:
		read_task_ctx(const server_task_map *);
		~read_task_ctx();

	protected:
		int				priority_;
		btree_key_t			iter_key_;
		struct get_sbt_entry_ctx	*sbt_ctx_;

		friend class server_task_map;
	};

protected:
	int					num_priorities_;
	bool					initialized_;
	bool					shutdown_pending_;
	mutable pthread_rwlock_obj		map_lock_;

	/* start_task helper functions */
	isi_cloud::task *start_task_helper__read_task(bool recovery,
	    read_task_ctx *ctx, struct isi_error **error_out) const;
	isi_cloud::task *start_task_helper__read_task_recovery(
	    read_task_ctx *ctx, struct isi_error **error_out) const;
	isi_cloud::task *start_task_helper__read_task_normal(
	    read_task_ctx *ctx, struct isi_error **error_out) const;

	/*
	 * Helper functions to convert an SBT entry to a task.  Upon success,
	 * task will be claimed and locked, otherwise neither.
	 */
	isi_cloud::task *start_task_helper__sbt_ent_to_claimed_locked_task(
	    int sbt_fd, struct sbt_entry *sbt_ent,
	    struct isi_error **error_out) const;
	isi_cloud::task *
	    start_task_helper__sbt_ent_to_claimed_locked_task__sbt(int sbt_fd,
	    struct sbt_entry *sbt_ent, struct isi_error **error_out) const;
	isi_cloud::task *
	    start_task_helper__sbt_ent_to_claimed_locked_task__hsbt(int sbt_fd,
	    struct sbt_entry *sbt_ent, struct isi_error **error_out) const;

	bool start_task_helper__qualify_task(isi_cloud::task *task,
	    bool recovery, djob_op_job_task_state &task_state,
	    struct isi_error **error_out) const;
	void start_task_helper__finalize(isi_cloud::task *task, bool recovery,
	    struct isi_error **error_out) const;
	void start_task_helper__finalize_recovery(isi_cloud::task *task,
	    std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;
	void start_task_helper__finalize_normal(isi_cloud::task *task,
	    std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;

	void get_new_wait_until_time(isi_cloud::task *task) const;

	/* Default construction is not allowed. */
	server_task_map();

	/* Copy construction and assignment are currently not implemented. */
	server_task_map(const server_task_map &);
	server_task_map &operator=(const server_task_map &);

public:
	server_task_map(const cpool_operation_type *op_type,
	    int num_priorities);
	virtual ~server_task_map();

	/**
	 * Initialize the server_task_map, which must be done before use.
	 *
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void initialize(struct isi_error **error_out);

	void shutdown(bool down);
	bool is_shutdown_pending(void) const;

	int get_num_priorities(void) const;

	/**
	 * Start a task, the first step in processing it.  Caller is
	 * responsible for deleting the returned task, if applicable.  On
	 * success the task will be claimed; on error it will not.  The
	 * claim held on this task will be released by finish_task, and should
	 * not be released by the caller.
	 *
	 * @param[io]  ctx		a context variable; caller should pass
	 * NULL for the initial call and then pass it to subsequent calls
	 * unchanged; caller is not responsible for deallocation unless most
	 * recent call to start_task returned EAGAIN
	 * @param[in]  max_task_reads	the maximum number of tasks to read
	 * before returning with EAGAIN
	 * @param[in]  recovery		whether or not to start a task in a
	 * recovery scenario
	 * @param[out] task_state	the state of the returned task
	 * (DJOB_OJT_NONE is returned task is NULL); note that this is an
	 * optimization as the state could be retrieved from the returned task
	 * (but this saves some SBT reads)
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	isi_cloud::task *start_task(read_task_ctx *&ctx_out,
	    int max_task_reads, bool recovery,
	    djob_op_job_task_state &task_state,
	    struct isi_error **error_out) const;

	/**
	 * Finish a task, the last step in processing it.  This will release
	 * the claim on this task taken in start_task, regardless of error.
	 *
	 * @param[in]  key		the key of the task to finish
	 * @param[in]  reason		the reason the task processing is
	 * complete
	 * @param[out] error		any error encountered during the
	 * operation
	 */
	void finish_task(const task_key *key,
	    task_process_completion_reason reason,
	    struct isi_error **error_out) const;

	/**
	 * Retrieve any checkpoint data currently attached to the Task.
	 * Checkpoint data is used by the consumer function to store context,
	 * state, etc. as needed and is opaque to the Task and the task_map.
	 * Never throws.
	 *
	 * @param[in]  key		the key of the task for which
	 * checkpoint data is requested
	 * @param[out] checkpoint_data	the checkpoint data; as input must
	 * be NULL; must be freed by the caller
	 * @param[out] checkpoint_data_size	the size of the retrieved
	 * cehckpoint data
	 * @param[out] task_state	the state of the task
	 * @param[in] offset		Offset into checkpoint to read from
	 * (only affects file checkpoints)
	 * @param[in] ckpt		The file_ckpt to read from
	 * (must be set iff the task uses file checkpoints)
	 *
	 * @return whether or not the operation was successful
	 */
	bool get_task_checkpoint(const task_key *key,
	    void *&checkpoint_data, size_t &checkpoint_data_size,
	    djob_op_job_task_state &task_state, off_t offset,
	    file_ckpt *ckpt) const;

	/**
	 * Set the checkpoint data for the specified Task.  Any existing
	 * checkpoint data will be overwritten.  Never throws.
	 *
	 * @param[in]  key		the key of the task on which checkpoint
	 * data is to be set
	 * @param[in]  checkpoint_data	the new checkpoint_data
	 * @param[in]  checkpoint_data_size	the size of the new checkpoint
	 * data
	 * @param[out] task_state	the state of the task
	 * @param[in] offset		Offset into checkpoint to write to
	 * (only affects file checkpoints)
	 * @param[in] ckpt		The file_ckpt to write to
	 * (must be set iff the task uses file checkpoints)
	 *
	 * @return whether or not the operation was successful
	 */
	bool set_task_checkpoint(const task_key *key,
	    void *checkpoint_data, size_t checkpoint_data_size,
	    djob_op_job_task_state &task_state, off_t offset,
	    file_ckpt *ckpt) const;
};

#endif // __ISI_CPOOL_D_SERVER_TASK_MAP_H__
