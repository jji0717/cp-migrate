#ifndef __CPOOL_DAEMON_JOB_STATS_H__
#define __CPOOL_DAEMON_JOB_STATS_H__

class daemon_job_stats
{
protected:
	int				version_;
	int				num_members_;
	int				num_succeeded_;
	int				num_failed_;
	int				num_cancelled_;
	int				num_pending_;
	int				num_inprogress_;

	mutable bool			serialization_up_to_date_;
	mutable void			*serialization_;
	mutable size_t			serialization_size_;

	void serialize(void) const;
	void deserialize(struct isi_error **error_out);

	/* Copy construction is not allowed. */
	daemon_job_stats(const daemon_job_stats &);

public:
	daemon_job_stats();
	~daemon_job_stats();

	daemon_job_stats &operator=(const daemon_job_stats &);
	bool operator==(const daemon_job_stats &) const;
	bool operator!=(const daemon_job_stats &) const;

	/**
	 * Retrieve the version of these stats.
	 */
	int get_version(void) const;
	// intentionally no set_version, see ctor

	/**
	 * Retrieve the number of members.
	 */
	int get_num_members(void) const;

	/**
	 * Increment the number of members.
	 */
	void increment_num_members(void);

	/**
	 * Decrement the number of members.
	 */
	void decrement_num_members(void);

	/**
	 * Retrieve the number of succeeded members.
	 */
	int get_num_succeeded(void) const;

	/**
	 * Increment the number of members.
	 */
	void increment_num_succeeded(void);

	/**
	 * Retrieve the number of failed members.
	 */
	int get_num_failed(void) const;

	/**
	 * Increment the number of failed members.
	 */
	void increment_num_failed(void);

	/**
	 * Retrieve the number of cancelled members.
	 */
	int get_num_cancelled(void) const;

	/**
	 * Increment the number of cancelled members.
	 */
	void increment_num_cancelled(void);

	/**
	 * Retrieve the number of pending members.
	 */
	int get_num_pending(void) const;

	/**
	 * Increment the number of pending members.
	 */
	void increment_num_pending(void);

	/**
	 * Decrement the number of pending members.
	 */
	void decrement_num_pending(void);

	/**
	 * Retrieve the number of in-progress members.
	 */
	int get_num_inprogress(void) const;

	/**
	 * Increment the number of in-progress members.
	 */
	void increment_num_inprogress(void);

	/**
	 * Decrement the number of in-progress members.
	 */
	void decrement_num_inprogress(void);

	size_t get_packed_size(void) const;
	void pack(void **stream) const;
	void unpack(void **stream, isi_error **error_out);

	/**
	 * Retrieve the serialized version.
	 */
	void get_serialization(const void *&serialized,
	    size_t &serialized_size) const;

	/**
	 * Reset these stats using a serialized version.
	 */
	void set_serialization(const void *serialized,
	    size_t serialized_size, struct isi_error **error_out);
};

struct fmt_conv_ctx cpool_djob_stats_fmt(const daemon_job_stats *);

#endif // __CPOOL_DAEMON_JOB_STATS_H__
