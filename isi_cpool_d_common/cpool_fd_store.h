#ifndef __ISI_CPOOL_FD_STORE_H__
#define __ISI_CPOOL_FD_STORE_H__

#include <pthread.h>

#include "cpool_d_common.h"
#include "operation_type.h"

struct isi_error;
///////这些fd主要用来: ifs_sbt_get_entry_at(fd,...)的第一个参数
class cpool_fd_store
{
private:
	int max_pending_priority_[OT_NUM_OPS];
	int *pending_sbt_fds_[OT_NUM_OPS];
	int inprogress_sbt_fd_[OT_NUM_OPS];
	int lock_file_fd_[OT_NUM_OPS];
	int move_lock_fd_[OT_NUM_OPS];

	int daemon_jobs_sbt_fd_;
	int djob_record_lock_fd_; /////访问daemon_job_record的sbt fd
	int file_ckpt_dir_fd_;

	pthread_rwlock_t fd_store_lock_;

	cpool_fd_store();

	cpool_fd_store(cpool_fd_store const &);
	void operator=(cpool_fd_store const &);

	void init_sbt_fds();

	static inline bool fd_is_valid(int fd);

	inline void read_lock();
	inline void write_lock();
	inline void unlock();

public:
	~cpool_fd_store();

	static cpool_fd_store &getInstance()
	{
		static cpool_fd_store instance;

		return instance;
	}

	void create_pending_sbt_fds(task_type type, const int num_priorities,
								struct isi_error **error_out);

	int get_max_pending_priority(task_type type) const;

	int get_pending_sbt_fd(task_type type, int priority, bool create,
						   struct isi_error **error_out);

	int get_inprogress_sbt_fd(task_type type, bool create,
							  struct isi_error **error_out);

	int get_djobs_sbt_fd(bool create, struct isi_error **error_out);

	int get_lock_fd(task_type type, struct isi_error **error_out);

	int get_move_lock_fd(task_type type, struct isi_error **error_out);

	int get_djob_record_lock_fd(struct isi_error **error_out);

	int get_file_ckpt_dir_fd(struct isi_error **error_out);

	void close_sbt_fds();
};
#endif // __ISI_CPOOL_FD_STORE_H__
