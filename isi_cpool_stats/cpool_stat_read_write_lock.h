#ifndef __CPOOL_STAT_READ_WRITE_LOCK_H__
#define __CPOOL_STAT_READ_WRITE_LOCK_H__

#include <stdio.h>
#include <errno.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <isi_util/isi_assert.h>
#include <isi_util/util.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_ilog/ilog.h>

#import "cpool_stat_list_element.h"

#define MAX_CP_STAT_NAME_LENGTH 4096
#define CP_STAT_KEY_PATH "/tmp/cpool_stat_"
#define CP_STAT_FTOK_PROJ_ID 8675309

/**
 * Converts a given name to key by creating a file (if needed, then using ftok
 * on the file to create a key sufficient for shared memory mounting or
 * semaphore creating
 *
 * @param name
 * @param error_out - standard Isilon error parameter
 * @return
 */
//将字符串转为key,作为shared memory的id
key_t cpool_stat_name_to_key(const char *name, struct isi_error **error_out);

/**
 * A (semaphore based) scoped lock class.
 *
 * Since there is no concept (at least in FreeBSD) of shared pthread mutexes,
 * we introduce the ability to create shared (cross-process) read/write locks.
 * The locks are created using two semaphores (one read and one write).  The
 * semaphores (call them s_r and s_w) work together using standard semop calls
 * as follows to manage the locks:
 *  read lock operation:
 *                  wait for s_w == 0, s_r == <any value>
 *                  increment s_r
 *  read unlock operation:
 *                  decrement s_r
 *  write_lock operation:
 *                  wait_for s_w == 0, s_r == 0
 *                  increment s_r, increment s_w
 *  write_unlock operation:
 *                  decrement s_r, decrement s_w
 *
 * Each lock has a name which can be shared across threads and processes.  Lock
 * is taken when consumer calls either lock_for_read or lock_for_write.  Lock
 * will be released either when consumer calls unlock or when destructor is
 * called.  Is is an error to take lock more than once before unlocking.
 */
class cpool_stat_read_write_lock
{
public:

	/**
	 * Constructor - just initialization, does NOT take a lock
	 */
	cpool_stat_read_write_lock(const char *lock_name) : sem_id_(-1),
	    lock_taken_(false)
	{
		ASSERT(lock_name != NULL && strlen(lock_name) > 0);
		sem_name_ = strdup(lock_name);
	};

	/**
	 * Destructor - releases the lock taken by this object
	 */
	virtual ~cpool_stat_read_write_lock();

	/**
	 * Take a read lock
	 *
	 * @param error_out - standard Isilon error parameter
	 */
	void lock_for_read(struct isi_error **error_out);

	/**
	 * Take a write lock
	 *
	 * @param error_out - standard Isilon error parameter
	 */
	void lock_for_write(struct isi_error **error_out);

	/**
	 * Release a previously taken lock.  If no lock has been taken, does
	 * nothing
	 *
	 * @param error_out - standard Isilon error parameter
	 */
	void unlock(struct isi_error **error_out);

	/**
	 * Returns true if a lock has been taken and false otherwise
	 */
	inline bool lock_is_taken() const
	{
		return lock_taken_;
	};

private:

	/**
	 * Creates 2 semaphores for performing read/write locks
	 */
	void initialize_semaphores(struct isi_error **error_out);

	/**
	 * The heart of the locking logic.  Modifies the semaphores to perform
	 * locking actions indicated
	 */
	void do_lock(bool wait_for_read, bool wait_for_write,
	    short read_delta, short write_delta, struct isi_error **error_out)
	    const;
	/**
	 * Specialized functions for unlocking different types of locks
	 */
	void unlock_for_read(struct isi_error **error_out);
	void unlock_for_write(struct isi_error **error_out);

	int sem_id_;
	char *sem_name_;
	bool lock_taken_;
	bool is_read_lock_;
};

#endif // __CPOOL_STAT_READ_WRITE_LOCK_H__
