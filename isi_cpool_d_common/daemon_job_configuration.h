#ifndef __CPOOL_DAEMON_JOB_CONFIGURATION_H__
#define __CPOOL_DAEMON_JOB_CONFIGURATION_H__

#include <ifs/ifs_types.h>

#include <stdlib.h>
#include <stdint.h>

#include "daemon_job_types.h"
#include "operation_type.h"

class daemon_job_configuration
{
protected:
	int				version_;
	int				num_active_daemon_jobs_;
	djob_id_t			last_djob_id_;
	djob_op_job_task_state		operation_states_[OT_NUM_OPS];

	mutable bool			serialization_up_to_date_;
	mutable void			*serialization_;
	mutable size_t			serialization_size_;

	mutable void			*saved_serialization_;
	mutable size_t			saved_serialization_size_;

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

	/* Copy construction and assignment are not allowed. */
	daemon_job_configuration(const daemon_job_configuration &);
	daemon_job_configuration &operator=(const daemon_job_configuration &);

public:
	daemon_job_configuration();
	~daemon_job_configuration();

	/**
	 * The daemon job configuration is, in some ways, treated as a
	 * "special" daemon job record.  Retrieve the daemon job ID used for
	 * the daemon job configuration.
	 */
	static djob_id_t get_daemon_job_id(void);

	/**
	 * Retrieve the lowest daemon job ID used for multi-jobs.  All
	 * non-multi-jobs will have IDs lower than this value.
	 */
	static djob_id_t get_multi_job_id_base(void);

	/**
	 * Retrieve the version of this configuration.
	 */
	int get_version(void) const;
	// intentionally no set_version, see ctor

	/**
	 * Retrieve the maximum allowed number of concurrent daemon jobs.
	 */
	static int get_max_concurrent_daemon_jobs(
	    struct isi_error **error_out);

	static int UNLIMITED_DAEMON_JOBS;

	/**
	 * Retrieve the number of currently active jobs.  Includes jobs for
	 * both single-job and multi-job operations.
	 */
	int get_num_active_jobs(void) const;

	/**
	 * Increment the number of currently active jobs.
	 */
	void increment_num_active_jobs(void);

	/**
	 * Decrement the number of currently active jobs.
	 */
	void decrement_num_active_jobs(void);

	/**
	 * Retrieve the last used daemon job ID.
	 */
	djob_id_t get_last_used_daemon_job_id(void) const;

	/**
	 * Update the last used daemon job ID.
	 */
	void set_last_used_daemon_job_id(djob_id_t last_used_id);

	/**
	 * Retrieve an operation's state.
	 */
	djob_op_job_task_state get_operation_state(
	    const cpool_operation_type *op_type) const;

	/**
	 * Set an operation's state.
	 */
	void set_operation_state(const cpool_operation_type *op_type,
	    djob_op_job_task_state new_state);

	/**
	 * Retrieve the serialized version of this configuration.
	 */
	void get_serialization(const void *&serialized,
	    size_t &serialized_size) const;

	/**
	 * Reset this configuration using a serialized version.
	 */
	void set_serialization(const void *serialized,
	    size_t serialized_size, struct isi_error **error_out);

	/**
	 * Save the current serialization such that it can be retrieved later.
	 * Only one serialization can be saved (although it can be overwritten
	 * by a later save).
	 */
	void save_serialization(void) const;

	/**
	 * Retrieve the saved serialization.
	 */
	void get_saved_serialization(const void *&serialization,
	    size_t &serialization_size) const;
};

#endif // __CPOOL_DAEMON_JOB_CONFIGURATION_H__
