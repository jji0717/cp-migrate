#ifndef __ISI_CPOOL_D_TASK_H__
#define __ISI_CPOOL_D_TASK_H__

#include <map>
#include <set>

#include <isi_config/ifsconfig.h>
#include <isi_job/job.h>

#include "isi_cloud_object_id.h"
#include "operation_type.h"
#include "task_key.h"

#define ISI_CFM_CHECKPOINT_ADS_NAME "$CPOOL_CHECKPOINT"
#define MAX_DAEMON_JOBS_PER_TASK		90

namespace isi_cloud {

/**
 * A Task defines a unit of work to be handled by a consumer function, such as
 * the Cloud Block Manager (CBM).
 *
 * Task objects are not thread-safe as they are not required to be in the
 * current design.
 */
class task
{
public:
	/*
	 * Errors that can't be summarized by an integer value fall into the
	 * "OTHER" category.  If a new error type is returned from a consumer
	 * function (and subsequently stored in with the task) that has an
	 * integer value, create a new type in this enum.
	 */
	typedef enum {
		PENDING_FIRST_ATTEMPT,
		SUCCESS,
		CBM,
		SYSTEM,
		OTHER
	} status_type;

protected:
	/* These fields will be serialized. */
	int			version_;
	uint32_t		handling_node_id_;
	int			priority_;
	status_type		most_recent_status_type_;
	int			most_recent_status_value_;
	int			num_attempts_;
	struct timeval		latest_state_change_time_;
	struct timeval		latest_location_change_time_;
	struct timeval		wait_until_time_;
	std::set<djob_id_t>	daemon_job_ids_;
	djob_op_job_task_state	state_;
	task_location		location_;
	mutable size_t		serialized_data_size_;
	mutable void		*serialized_data_;
	size_t			checkpoint_data_size_;
	mutable void		*checkpoint_data_;

	/* These fields are not serialized. */
	mutable bool		serialization_up_to_date_;
	mutable size_t		serialized_task_size_;
	mutable void		*serialized_task_;

	mutable void		*saved_serialization_;
	mutable size_t		saved_serialization_size_;

	const cpool_operation_type *op_type_;

	virtual void serialize(void) const;
	virtual void deserialize(struct isi_error **error_out);

	/*
	 * Setting / getting serialized data should only be done via derived
	 * classes.
	 */
	/**
	 * Retrieve the data associated with this Task in a serialized format.
	 * Caller should neither edit nor deallocate contents.	Never throws.
	 *
	 * @param[out] out_data		the data in serialized format
	 * @param[out] out_size		the size of the serialized data
	 */
	void get_serialized_data(const void *&data, size_t &size) const;

	/**
	 * Assign new serialized data to this Task.  Never throws.
	 *
	 * @param[in]  data		the new serialized data
	 * @param[in]  size		the size of the new serialized data
	 */
	void set_serialized_data(const void *data, size_t size);

	/* Default construction is not allowed. */
	task();

	/* Copy and assignment are currently not implemented. */
	task(const task&);
	task& operator=(const task&);

public:
	task(const cpool_operation_type *op_type);
	virtual ~task();

	/**
	 * Retrieve the type of this task.  Never throws.
	 *
	 * @return the task's type
	 */
	const cpool_operation_type *get_op_type(void) const;

	/**
	 * Retrieve the version of this Task.  Never throws.
	 *
	 * @return the Task's version
	 */
	int get_version(void) const;
	// intentionally no set_version, see ctor

	/**
	 * Retrieve the ID of the node to most recently process this Task.
	 * Never throws.
	 *
	 * @return the aforementioned ID
	 */
	uint32_t get_handling_node_id(void) const;

	/**
	 * Sets the ID of the node to most recently process this Task.	Never
	 * throws.
	 *
	 * @param[in]  node_id		the aforementioned ID
	 */
	void set_handling_node_id(uint32_t node_id);

	/**
	 * Retrieves the priority assigned to this Task.  Never throws.
	 *
	 * @return this Task's priority
	 */
	int get_priority(void) const;

	/**
	 * Set this Task's priority.  Never throws.
	 *
	 * @param[in]  pri		the new priority
	 */
	void set_priority(int pri);

	/**
	 * Returns the status of the most recent processing attempt.
	 * Never throws.
	 *
	 * @param[out] type		the status type (see enum status_type)
	 * @param[out] value		the status value
	 */
	void get_most_recent_status(status_type &type, int &value) const;

	/**
	 * Sets the status (type and value) of the most recent processing
	 * attempt.  Never throws.
	 *
	 * @param[in]  type		the type of error returned from
	 * processing this task
	 * @param[in]  value		the value of error type returned from
	 * processing this task
	 */
	void set_most_recent_status(status_type type, int value);

	/**
	 * Returns the number of times this Task has been processed,
	 * successfully or otherwise.  Never throws.
	 *
	 * @return the number of attempts
	 */
	int get_num_attempts(void) const;

	/**
	 * Increment the number of times this Task has been processed,
	 * successfully or otherwise.  Never throws.
	 */
	void inc_num_attempts(void);

	/**
	 * Get the absolute time of the most recent state change for this Task.
	 * Never throws.
	 *
	 * @return the aforementioned time
	 */
	struct timeval get_latest_state_change_time(void) const;
	// no set - see set_state

	/**
	 * Get the absolute time of the most recent location change for this
	 * task.
	 *
	 * @return the aforementioned time
	 */
	struct timeval get_latest_location_change_time(void) const;
	// no set - see set_location

	/**
	 * Get the absolute time before which this Task should not be
	 * processed.  Never throws.
	 *
	 * @return the aforementioned time
	 */
	struct timeval get_wait_until_time(void) const;

	/**
	 * Set the new absolute time before which this Task should not be
	 * processed.  Never throws.
	 *
	 * @param[in]  new_time		the aforementioned time
	 */
	void set_wait_until_time(struct timeval new_time);

	/**
	 * Retrieve the IDs of the daemon jobs of which this task is a member
	 *
	 * @param[out] ids		array of daemon job ids
	 * @param[out] states		array of job states
	 * @param[out] num_jobs		number of daemon jobs
	 */
	const std::set<djob_id_t> &get_daemon_jobs(void) const;

	/**
	 * Determine if this task is a member of a daemon job.
	 *
	 * @param[in]  djob_id		the daemon job to check
	 *
	 * @return			true if this task is a member of the
	 * specified daemon job, else false
	 */
	bool is_member_of(djob_id_t job_id) const;

	/**
	 * Add a daemon job to which this task belongs.  A task can belong to a
	 * maximum of MAX_DAEMON_JOBS_PER_TASK daemon jobs, and an error will
	 * be returned if adding a daemon job would exceed this value.  Also,
	 * an error will be returned if the daemon job ID to add has previously
	 * been added to the task.
	 *
	 * @param[in]  id		the daemon job ID to add
	 * @param[out] error		any error encountered during the
	 * operation
	 */
	void add_daemon_job(djob_id_t id, struct isi_error **error_out);

	/**
	 * Remove a daemon job ID from the task.
	 *
	 * @param[in]  id		the id of daemon job to remove
	 *
	 * @return			true if daemon job was removed, false
	 * if task was not a member of the specified daemon job
	 */
	bool remove_daemon_job(djob_id_t job_id);

	/**
	 * Retrieve the state of this task.
	 */
	djob_op_job_task_state get_state(void) const;

	/**
	 * Set the state of this task.
	 *
	 * @param[in]  state		the state of the task
	 */
	void set_state(djob_op_job_task_state state);

	/**
	 * Retrieve the location of this task.
	 */
	task_location get_location(void) const;

	/**
	 * Set the location of this task.
	 */
	void set_location(task_location location);

	/**
	 * Retrieve the checkpoint data associated with this Task in a
	 * serialized format.  Caller should neither edit nor deallocate
	 * contents.  Never throws.
	 *
	 * @param[out] out_data		the data in serialized format
	 * @param[out] out_size		the size of the serialized data
	 */
	void get_checkpoint_data(const void *&out_data,
	    size_t &out_size) const;

	/**
	 * Assign new checkpoint data to this Task.  Never throws.
	 *
	 * @param[in]  data		the new (serialized) checkpoint data
	 * @param[in]  size		the size of the new checkpoint data
	 */
	void set_checkpoint_data(const void *data, size_t size);

	/**
	 * Retrieve this Task in a serialized format.  Caller should neither
	 * edit nor deallocate contents.  Never throws.
	 *
	 * @param[out] out_task		this Task in serialized format
	 * @param[out] out_size		the size of the serialized Task
	 */
	void get_serialized_task(const void *&out_task, size_t &out_size)
	    const;

	/**
	 * Reinitialize this Task using a serialized version of a Task.  Never
	 * throws.
	 *
	 * @param[in]  data		the serialized Task
	 * @param[in]  size		the size of the serialized Task
	 * @param[out] error		any error encountered during the
	 * operation
	 */
	void set_serialized_task(const void *task, size_t size,
	    struct isi_error **error_out);

	/**
	 * Save the current serialization such that it can be retrieved later.
	 * Only one serialization can be saved (although it can be overwritten
	 * by a later save).  Never throws.
	 */
	void save_serialization(void) const;

	/**
	 * Retrieve the saved serialization.  Never throws.
	 *
	 * @param[out] serialization	the saved serialization
	 * @param[out] size		the saved serialization's size
	 */
	void get_saved_serialization(const void *&serialization,
	    size_t &size) const;

	/**
	 * Called when checkpoint information is about to be read or written.
	 */
	bool has_ckpt_within_task() const
	{
		return op_type_->get_type() != CPOOL_TASK_TYPE_WRITEBACK;
	}

	/**
	 * Factory method for creating Tasks for various operations.  Caller is
	 * responsible for deallocating the returned Task using delete.  Never
	 * throws.
	 * @param[in]  type		the type of Task to create
	 */
	static task *create_task(const cpool_operation_type *op_type);

	/**
	 * Print the task in a formatted way.  Never throws.
	 */
	virtual void fmt_conv(struct fmt *fmt,
	    const struct fmt_conv_args *args) const = 0;

	/**
	 * Get the key that identifies this task.  Caller should not deallocate
	 * the key as this will be done when the task is destroyed.
	 *
	 * @param[out] key		a pointer to the key
	 * @param[out] key_len		the length of the pointed-to key
	 */
	virtual const task_key *get_key(void) const = 0;

	/**
	 * Get a 64-bit identifier for this task.  This value is currently used
	 * for locking; different tasks with the same identifier will adversely
	 * affect performance but should not affect correctness.
	 *
	 * @return the identifier
	 */
	virtual uint64_t get_identifier(void) const = 0;

	/**
	 * Returns whether a task's state is truly completed (even though it
	 * may still exist in an SBT somewhere)
	 */
	bool is_done_processing(void) const;
};

class archive_task : public task
{
protected:
	file_task_key key_;
	/* Policy name is only for task version CPOOL_ARCHIVE_TASK_V1 only for compatibility */
	char          *policy_name_;
	/* Policy id is for task version CPOOL_ARCHIVE_TASK_V2 */
	uint32_t      policy_id_;

	/* Copy construction and assignment are not allowed. */
	archive_task(const archive_task&);
	archive_task& operator=(const archive_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	archive_task();
	archive_task(ifs_lin_t lin, ifs_snapid_t snapid);
	~archive_task();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);
	/* This is only used by task version 1.
	   New task should NOT rely on policy name
	*/
	void set_policy_name(const char *policy_name);
	void set_policy_id(uint32_t policy_id);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;
	/* This is only used by task version 1.
	   New task should NOT rely on policy name
	*/
	const char *get_policy_name(void) const;
	uint32_t get_policy_id(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;

	bool has_policyid(void) const;
};

class recall_task : public task
{
protected:
	file_task_key			key_;
	bool				retain_stub_;

	/* Copy and assignment are not allowed. */
	recall_task(const recall_task&);
	recall_task& operator=(const recall_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	recall_task();
	recall_task(ifs_lin_t lin, ifs_snapid_t snapid);
	~recall_task();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);
	void set_retain_stub(bool retain_stub);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;
	bool get_retain_stub() const ;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

class local_gc_task : public task
{
protected:
	file_task_key			key_;
	isi_cloud_object_id		object_id_;
	struct timeval			date_of_death_;

	/* Copy and assignment are not allowed. */
	local_gc_task(const local_gc_task&);
	local_gc_task& operator=(const local_gc_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	local_gc_task();
	local_gc_task(ifs_lin_t lin, ifs_snapid_t snapid);
	~local_gc_task();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);
	void set_object_id(const isi_cloud_object_id &object_id);
	void set_date_of_death(struct timeval date_of_death);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;
	const isi_cloud_object_id &get_object_id(void) const;
	struct timeval get_date_of_death(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

class cloud_gc_task : public task
{
protected:
	cloud_object_id_task_key	key_;

	/* Copy and assignment are not allowed. */
	cloud_gc_task(const cloud_gc_task&);
	cloud_gc_task& operator=(const cloud_gc_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	cloud_gc_task();
	cloud_gc_task(const isi_cloud_object_id &object_id);
	~cloud_gc_task();

	void set_cloud_object_id(const isi_cloud_object_id &object_id);
	const isi_cloud_object_id &get_cloud_object_id(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

class writeback_task : public task
{
protected:
	file_task_key			key_;

	/* Copy and assignment are not allowed. */
	writeback_task(const writeback_task&);
	writeback_task& operator=(const writeback_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	writeback_task();
	writeback_task(ifs_lin_t lin, ifs_snapid_t snapid);
	~writeback_task();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

class cache_invalidation_task : public task
{
protected:
	file_task_key			key_;

	/* Copy and assignment are not allowed. */
	cache_invalidation_task(const cache_invalidation_task&);
	cache_invalidation_task& operator=(const cache_invalidation_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	cache_invalidation_task();
	cache_invalidation_task(ifs_lin_t lin, ifs_snapid_t snapid);
	~cache_invalidation_task();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

class restore_coi_task : public task
{
protected:
	account_id_task_key	key_;
	struct timeval		dod_;
	mutable std::string	string_form_;

	/* Copy and assignment are not allowed. */
	restore_coi_task(const restore_coi_task&);
	restore_coi_task& operator=(const restore_coi_task&);

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

public:
	restore_coi_task();
	restore_coi_task(const task_account_id &account_id);
	~restore_coi_task();

	void set_account_id(const task_account_id &account_id);
	const task_account_id &get_account_id(void) const;

	void set_dod(struct timeval dod);
	struct timeval get_dod(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	const task_key *get_key(void) const;
	uint64_t get_identifier(void) const;
};

} // end namespace isi_cloud

struct fmt_conv_ctx cpool_task_fmt(const isi_cloud::task *t);

#endif // __ISI_CPOOL_D_TASK_H__
