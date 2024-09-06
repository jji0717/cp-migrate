#ifndef __CPOOL_DAEMON_JOB_RECORD_H__
#define __CPOOL_DAEMON_JOB_RECORD_H__

#include <sys/isi_assert.h>

#include <time.h>

#include <isi_job/job.h>
#include <isi_job/jobstatus_gcfg.h>

#include "daemon_job_stats.h"
#include "daemon_job_types.h"
#include "operation_type.h"

class daemon_job_record
{
protected:
	int				version_;
	djob_id_t			daemon_job_id_;
	const cpool_operation_type	*operation_type_;
	time_t				creation_time_;
	time_t				latest_modification_time_;
	time_t				completion_time_;
	djob_op_job_task_state		state_;
	jd_jid_t			job_engine_job_id_;
	jobstat_state			job_engine_job_state_;
	daemon_job_stats		stats_;
	char				*description_;

	mutable bool			serialization_up_to_date_;
	mutable void			*serialization_;
	mutable size_t			serialization_size_;

	mutable void			*saved_serialization_;
	mutable size_t			saved_serialization_size_;

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

	void set_latest_modification_time(time_t new_time);
	void set_completion_time(time_t new_time);

	/* Copy construction and assignment are currently not implemented. */
	daemon_job_record(const daemon_job_record &);
	daemon_job_record &operator=(const daemon_job_record &);

public:
	daemon_job_record();
	virtual ~daemon_job_record();

	int get_version(void) const;
	// intentionally no set_version, see ctor

	djob_id_t get_daemon_job_id(void) const;
	void set_daemon_job_id(djob_id_t id);

	const cpool_operation_type *get_operation_type(void) const;
	void set_operation_type(const cpool_operation_type *operation_type);

	time_t get_creation_time(void) const;
	void set_creation_time(time_t creation_time);

	time_t get_latest_modification_time(void) const;
	// intentionally no public set function, see record-modifying functions

	time_t get_completion_time(void) const;
	// intentionally no public set function, see set_state

	djob_op_job_task_state get_state(void) const;
	void set_state(djob_op_job_task_state state);

	jd_jid_t get_job_engine_job_id(void) const;
	void set_job_engine_job_id(jd_jid_t je_id);

	jobstat_state get_job_engine_state(void) const;
	void set_job_engine_state(jobstat_state state);

	const daemon_job_stats *get_stats(void) const;

	void increment_num_members(void);
	void decrement_num_members(void);
	void increment_num_succeeded(void);
	void increment_num_failed(void);
	void increment_num_cancelled(void);
	void increment_num_pending(void);
	void decrement_num_pending(void);
	void increment_num_inprogress(void);
	void decrement_num_inprogress(void);

	const char *get_description(void) const;
	void set_description(const char *description);

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
	 * Retrieve the saved serialization.  The caller should NOT deallocate
	 * the returned pointer.  If save_serialization() has not previously
	 * been called, then NULL and 0 will be stored in the outgoing
	 * parameters.
	 */
	void get_saved_serialization(const void *&serialization,
	    size_t &serialization_size) const;

	static btree_key_t get_key(djob_id_t id);
	btree_key_t get_key(void) const;

	/**
	 * A daemon job is considered finished if-and-only-if the associated
	 * JobEngine job is finished (if one exists) and all tasks have been
	 * accounted for.
	 */
	bool is_finished(void) const;

	/**
	 * For convenience - returns true if there is a non-NULL_JID job engine
	 * id and the job engine is not in a terminal state.
	 * if a non-NULL_JID job engine id and the job engine state is
	 * STATE_NULL, check state in job engine
	 */
	bool job_engine_job_is_running(struct isi_error **error_out) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;
};

struct fmt_conv_ctx cpool_djob_rec_fmt(const daemon_job_record *);

#endif // __CPOOL_DAEMON_JOB_RECORD_H__
