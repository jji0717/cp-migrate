#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include <isi_util/isi_error.h>

#include "cpool_d_debug.h"
#include "cpool_fd_store.h"

static const char file_ckpt_dir_path[] = "/ifs/.ifsvar/modules/cloud/ckpt";


cpool_fd_store::cpool_fd_store()
{

	init_sbt_fds();

	int ret = pthread_rwlock_init(&fd_store_lock_, NULL);
	ASSERT(ret == 0);
}

cpool_fd_store::~cpool_fd_store()
{
	close_sbt_fds();
	int ret = pthread_rwlock_destroy(&fd_store_lock_);
	ASSERT(ret == 0);
}

inline void
cpool_fd_store::read_lock() {
	int ret = pthread_rwlock_rdlock(&fd_store_lock_);
	ASSERT(ret == 0);
}

inline void
cpool_fd_store::write_lock() {
	int ret = pthread_rwlock_wrlock(&fd_store_lock_);
	ASSERT(ret == 0);
}

inline void
cpool_fd_store::unlock() {
	int ret = pthread_rwlock_unlock(&fd_store_lock_);
	ASSERT(ret == 0);
}

void
cpool_fd_store::init_sbt_fds()
{
	for (int i = 0; i < CPOOL_TASK_TYPE_MAX_TASK_TYPE; ++i) {
		max_pending_priority_[i] = -1;
		pending_sbt_fds_[i] = NULL;
		inprogress_sbt_fd_[i] = -1;
		lock_file_fd_[i] = -1;
		move_lock_fd_[i] = -1;
	}
	daemon_jobs_sbt_fd_ = -1;
	djob_record_lock_fd_ = -1;
	file_ckpt_dir_fd_ = -1;
}

void
cpool_fd_store::close_sbt_fds()
{
	int i, j;

	//close_sbt actually works on any fd

	for (i = 0; i < CPOOL_TASK_TYPE_MAX_TASK_TYPE; ++i) {
		if (pending_sbt_fds_[i] != NULL) {
			for (j = 0; j < max_pending_priority_[i]; ++j) {
				if (pending_sbt_fds_[i][j] >= 0) {
					close_sbt(pending_sbt_fds_[i][j]);
				}
			}

			delete[] pending_sbt_fds_[i];
			pending_sbt_fds_[i] = NULL;
		}

		if (inprogress_sbt_fd_[i] >= 0)
			close_sbt(inprogress_sbt_fd_[i]);

		if (lock_file_fd_[i] >= 0)
			close_sbt(lock_file_fd_[i]);

		if (move_lock_fd_[i] >= 0)
			close_sbt(move_lock_fd_[i]);
	}

	if (daemon_jobs_sbt_fd_ >= 0)
		close_sbt(daemon_jobs_sbt_fd_);

	if (djob_record_lock_fd_ >= 0)
		close_sbt(djob_record_lock_fd_);

	if (file_ckpt_dir_fd_ >= 0)
		close_sbt(file_ckpt_dir_fd_);

	init_sbt_fds();
}


bool
cpool_fd_store::fd_is_valid(int fd)
{
	/* fd has a valid value and isn't stale */
	return fd >= 0 && fcntl(fd, F_GETFD) != -1;
}

void
cpool_fd_store::create_pending_sbt_fds(task_type type, const int num_priorities,
    struct isi_error **error_out)
{
	ASSERT(num_priorities > 0);

	struct isi_error *error = NULL;
	char *name_buf = NULL;

	read_lock();
	if (pending_sbt_fds_[type] != NULL)
// ^ should check that there are enough pending SBT fds based on priority and
// expand as needed
		goto out;

	unlock();

	write_lock();
	if (pending_sbt_fds_[type] != NULL)
		goto out;

	max_pending_priority_[type] = num_priorities - 1;
	pending_sbt_fds_[type] = new int[num_priorities];

	for (int i = 0; i < num_priorities; ++i) {
		name_buf = get_pending_sbt_name(type, i);
		// create the sbt if it doesn't exist
		pending_sbt_fds_[type][i] = open_sbt(name_buf, true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		free(name_buf);
		name_buf = NULL;
	}

 out:
	unlock();
	if (name_buf)
		free(name_buf);

	if (error) {
		if (pending_sbt_fds_[type] != NULL) {
			free(pending_sbt_fds_[type]);
			pending_sbt_fds_[type] = NULL;
                        max_pending_priority_[type] = 0;
		}
	}
	isi_error_handle(error, error_out);
}

int
cpool_fd_store::get_max_pending_priority(task_type type) const
{
	return max_pending_priority_[type];
}

int
cpool_fd_store::get_pending_sbt_fd(task_type type, int priority,
     bool create, struct isi_error **error_out)
{
	ASSERT(priority >= 0);

	int fd = -1, max_pri, i;
	int *old_fds = NULL, *new_fds = NULL;
	char *pending_sbt_name = NULL;
	struct isi_error *error = NULL;

	/*
	 * Determine if the store has the space to store the
	 * desired file descriptor.  Having the space doesn't
	 * guarantee that we've already cached the fd, but not
	 * having the space guarantees not only that the fd is
	 * not cached, but that we also need to allocate space
	 * in order to cache it.
	 */
	read_lock();
	max_pri = max_pending_priority_[type];

	if (priority <= max_pri) {
		/*
		 * Lookup a fd, which may or may not have already been
		 * cached.
		 */
		fd = pending_sbt_fds_[type][priority];

		if (fd_is_valid(fd))
			goto out;

		unlock();
		write_lock();
		/*
		 * Read the fd again to check if other threads have
		 * opened the SBT. Not necessary to check if
		 * the buffer needs to be expanded here as the
		 * it never gets reduced.
		 */
		fd = pending_sbt_fds_[type][priority];

		if (fd_is_valid(fd))
			goto out;
	} else {
		/*
		 * Expand the buffer.
		 */
		unlock();
		write_lock();
		max_pri = max_pending_priority_[type];

		// Check again if the buffer needs to be expanded
		if (priority >= max_pri) {
			old_fds = pending_sbt_fds_[type];
			new_fds = new int[priority + 1];
			ASSERT(new_fds != NULL);

			for (i = 0; i < max_pri; ++i)
				new_fds[i] = old_fds[i];
			for (; i <= priority; ++i)
				new_fds[i] = -1;

			delete [] old_fds;
			old_fds = NULL;
			pending_sbt_fds_[type] = new_fds;
			max_pending_priority_[type] = priority;

			new_fds = NULL;	// No longer used in this function
		}
	}

	pending_sbt_name = get_pending_sbt_name(type, priority);

	fd = open_sbt(pending_sbt_name, create, &error);
	if (error == NULL)
		pending_sbt_fds_[type][priority] = fd;
	else
		isi_error_add_context(error, "invalid priority");

out:
	unlock();

	if (pending_sbt_name != NULL)
		free(pending_sbt_name);

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_inprogress_sbt_fd(task_type type, bool create,
    struct isi_error **error_out)
{
	int fd = -1;
	char *inprogress_sbt_name = NULL;
	struct isi_error *error = NULL;

	/*
	 * Lookup a fd, which may or may not have already been
	 * cached.
	 */
	read_lock();
	fd = inprogress_sbt_fd_[type];

	if (fd_is_valid(fd))
		goto out;

	unlock();
	inprogress_sbt_name = get_inprogress_sbt_name(type);

	write_lock();

	fd = inprogress_sbt_fd_[type];

	if (fd_is_valid(fd))
		goto out;
	/*
	 * Open a file descriptor, and cache the result.
	 * Open the SBT, but don't create it if it doesn't
	 * exist; this determines whether or not the
	 * type/priority combination is valid, since
	 * isi_cpool_d creates (as needed) all pending SBTs
	 * when it starts.
	 */
	fd = open_sbt(inprogress_sbt_name, create, &error);
	if (error == NULL)
		inprogress_sbt_fd_[type] = fd;

out:
	unlock();

	if (inprogress_sbt_name != NULL)
		free(inprogress_sbt_name);

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_djobs_sbt_fd(bool create, struct isi_error **error_out)
{
	int fd = -1;
	char *djobs_sbt_name = NULL;
	struct isi_error *error = NULL;

	read_lock();
	fd = daemon_jobs_sbt_fd_;

	if (fd_is_valid(fd))
		goto out;

	unlock();
	djobs_sbt_name = get_daemon_jobs_sbt_name();

	write_lock();

	fd = daemon_jobs_sbt_fd_;
	if (fd_is_valid(fd))
		goto out;

	// Open a file descriptor, and cache the result.
	fd = open_sbt(djobs_sbt_name, create, &error);
	if (error == NULL && fd != -1)
		daemon_jobs_sbt_fd_ = fd;

out:
	unlock();

	if (djobs_sbt_name != NULL)
		free(djobs_sbt_name);

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_lock_fd(task_type type, struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(lock_filename);

	/*
	 * Lookup a fd, which may or may not have already been cached.
	 */
	read_lock();
	fd = lock_file_fd_[type];

	if (fd_is_valid(fd))
		goto out;

	unlock();

	fmt_print(&lock_filename, "%sisi_cpool_d.%{}.lock",
	    CPOOL_LOCKS_DIRECTORY, task_type_fmt(type));
	errno = 0;

	write_lock();

	fd = lock_file_fd_[type];

	if (fd_is_valid(fd))
		goto out;

	/*
	 * Open a file descriptor, and cache the result.
	 */
	fd = open(fmt_string(&lock_filename), O_RDONLY | O_CREAT, 0755);
	if (fd < 0)
		error = isi_system_error_new(errno,
		    "failed to open lock file (%s)",
		    fmt_string(&lock_filename));
	else
		lock_file_fd_[type] = fd;

out:
	unlock();

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_move_lock_fd(task_type type, struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(move_lock_filename);

	read_lock();
	fd = move_lock_fd_[type];

	if (fd_is_valid(fd))
		goto out;

	unlock();

	// Open a file descriptor, and cache the result.
	fmt_print(&move_lock_filename, "%sisi_cpool_d.%{}.move_lock",
	    CPOOL_LOCKS_DIRECTORY, task_type_fmt(type));
	errno = 0;

	write_lock();

	fd = move_lock_fd_[type];
	if (fd_is_valid(fd))
		goto out;

	fd = open(fmt_string(&move_lock_filename), O_RDONLY | O_CREAT, 0755);
	if (fd < 0)
		error = isi_system_error_new(errno,
		    "failed to open move file lock file (%s)",
		    fmt_string(&move_lock_filename));

	else
		move_lock_fd_[type] = fd;

out:
	unlock();

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_djob_record_lock_fd(struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;
	char *lock_file_buf = NULL;

	read_lock();
	fd = djob_record_lock_fd_;

	if (fd_is_valid(fd))
		goto out;

	unlock();

	// Open a file descriptor, and cache the result.
	lock_file_buf = get_djob_record_lock_file_name();
	errno = 0;

	write_lock();

	fd = djob_record_lock_fd_;

	if (fd_is_valid(fd))
		goto out;

	fd = open(lock_file_buf, O_RDONLY | O_CREAT, 0755);
	if (fd < 0)
		error = isi_system_error_new(errno,
		    "failed to open daemon jobs lock file (%s)",
		    lock_file_buf);
	else
		djob_record_lock_fd_ = fd;

out:
	unlock();

	if (lock_file_buf != NULL)
		free(lock_file_buf);

	isi_error_handle(error, error_out);

	return fd;
}

int
cpool_fd_store::get_file_ckpt_dir_fd(struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;

	read_lock();
	fd = file_ckpt_dir_fd_;

	if (fd_is_valid(fd))
		goto out;

	unlock();

	write_lock();

	fd = file_ckpt_dir_fd_;

	if (fd_is_valid(fd))
		goto out;

	fd = open(file_ckpt_dir_path, O_DIRECTORY);
	if (fd < 0 && errno == ENOENT) {
		mkdir(file_ckpt_dir_path, 0600);
		fd = open(file_ckpt_dir_path, O_DIRECTORY);
	}

	if (fd < 0) {
		ON_SYSERR_GOTO(out, errno, error,
		    "failed to open %s", file_ckpt_dir_path);
	}

	file_ckpt_dir_fd_ = fd;

out:
	unlock();

	isi_error_handle(error, error_out);

	return fd;
}
