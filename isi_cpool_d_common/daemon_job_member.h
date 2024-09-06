#ifndef __DAEMON_JOB_MEMBER__
#define __DAEMON_JOB_MEMBER__

class cpool_operation_type;
class task_key;

class daemon_job_member
{
protected:
	int				version_;
	const task_key			*key_;
	djob_op_job_task_state		final_state_;
	char				*name_;

	size_t				max_name_size_;

	mutable void			*serialization_;
	mutable size_t			serialization_size_;
	mutable bool			serialization_up_to_date_;

	mutable void			*saved_serialization_;
	mutable size_t			saved_serialization_size_;

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

	/* Copy construction and assignment are currently not implemented. */
	daemon_job_member(const daemon_job_member &);
	daemon_job_member& operator=(const daemon_job_member &);

public:
	/*
	 * Default construction is only intended to be used by
	 * isi_cpool_d_util.
	 */
	daemon_job_member();
	daemon_job_member(const task_key *key);
	~daemon_job_member();

	uint32_t get_version(void) const;
	void set_version(uint32_t version);

	const task_key *get_key(void) const;
	/* intentionally no set_key, see ctor */

	void set_name(const char *new_name);
	const char *get_name(void) const;

	void set_final_state(djob_op_job_task_state new_state);
	djob_op_job_task_state get_final_state(void) const;

	void get_serialization(const void *&serialization,
	    size_t &serialization_size) const;
	void set_serialization(const void *serialization,
	    size_t serialization_size, struct isi_error **error_out);

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

#endif // __DAEMON_JOB_MEMBER__
