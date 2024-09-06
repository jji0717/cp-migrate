#ifndef __ISI_CPOOL_D_CLIENT_TASKMAP_H__
#define __ISI_CPOOL_D_CLIENT_TASKMAP_H__

#include <vector>

#include <isi_cpool_d_common/task_map.h>
#include <isi_cpool_d_common/resume_key.h>

class scoped_object_list;

struct job_member
{
	char			*key_;
	char			*name_;
	djob_op_job_task_state	state_;
};

class client_task_map : public task_map
{
protected:
	/* Default constructor not allowed */
	client_task_map();

	/* Copy construction and assignment are not currently implemented. */
	client_task_map(const client_task_map &);
	client_task_map &operator=(const client_task_map &);

	/* add_task helper functions */
	virtual void add_task_helper__existing(const isi_cloud::task *new_task,
	    isi_cloud::task *existing_task, djob_id_t daemon_job_id,
	    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;
	virtual void add_task_helper__nonexisting(isi_cloud::task *new_task,
	    djob_id_t daemon_job_id, const char *filename,
	    std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;

	/*
	 * These _add_task_helper__* functions are intended to be final (to
	 * borrow from Java), as they are responsible solely for adding
	 * operations to manipulate entries in SBTs based on the input task(s).
	 * The idea (when this class had a derived subclass) was that these
	 * functions could/should be universally used as they had only one
	 * responsibility which is so closely coupled with the overall
	 * CloudPools daemon design, and that the subclass could override the
	 * non-leading underscore versions of these functions as needed to
	 * enforce their specific business logic.  Although this class no
	 * longer has subclasses, it seemed somewhere between non-harmful and
	 * wise to maintain this separation.
	 */

	/**
	 * Assumes existing_task has already saved its serialization.
	 */
	void _add_task_helper__existing(const isi_cloud::task *new_task,
	    isi_cloud::task *existing_task, djob_id_t daemon_job_id,
	    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;
	void _add_task_helper__nonexisting(isi_cloud::task *new_task,
	    djob_id_t daemon_job_id, const char *filename,
	    std::vector<struct hsbt_bulk_entry> &sbt_ops,
	    scoped_object_list &objects_to_delete,
	    struct isi_error **error_out) const;

#ifdef PRC_TASK
	void prc_task(const task_key *key, djob_op_job_task_state new_state,
	    struct isi_error **error_out) const;
#endif

	unsigned int get_sbt_task_count(int sbt_fd,
	    bool only_count_tasks_marked_done, struct isi_error **error_out);

public:
	client_task_map(const cpool_operation_type *op_type);
	virtual ~client_task_map();

	/**
	 * Add a task to be processed as a member of the specified daemon job
	 * ID.
	 *
	 * @param[io]  task		the task to process
	 * @param[in]  daemon_job_id	the daemon job to which task belongs
	 * @param[in]  filename		the name of the file identified by
	 * task, if applicable; can be NULL
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	virtual void add_task(isi_cloud::task *task, djob_id_t daemon_job_id,
	    const char *filename, struct isi_error **error_out) const;

#ifdef PRC_TASK
	/**
	 * Pause a task.  The task will not be processed until it is either
	 * resumed or cancelled.
	 */
	void pause_task(const task_key *key,
	    struct isi_error **error_out) const { ASSERT(false, "nyi"); }
#endif

	void query_job_for_members(djob_id_t daemon_job_id, unsigned int start,
	    resume_key &start_key,
	    unsigned int max, std::vector<struct job_member *> &members,
	    struct isi_error **error_out);

	/**
	 * Returns the number of tasks which have not been completed for this
	 * task map.  This is the sum of all tasks in all pending queues plus
	 * the number of tasks in the in-progress queue that haven't been marked
	 * complete yet.  Note, the true value could change while this function
	 * is executing
	 */
	unsigned int get_incomplete_task_count(struct isi_error **error_out);
};

#endif // __ISI_CPOOL_D_CLIENT_TASKMAP_H__
