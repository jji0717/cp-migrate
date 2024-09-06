#ifndef __IFS_CPOOL_FLOCK__H__
#define __IFS_CPOOL_FLOCK__H__

#include <pthread.h>
#include <uuid.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <isi_cloud_common/isi_cloud_app.h>
#include <isi_ilog/ilog.h>

#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_ufp/isi_ufp.h>
#include "isi_cpool_revoked_fd.h"

enum ifs_cpool_flock_locker_t
{
	CFLT_THREAD,
	CFLT_PROCESS
};

#define DEFAULT_CPOOL_FLOCK_RETRY_COUNT -1
/////byte range lock(范围锁)在linux中就是文件锁(记录锁):锁定文件中的一个区域(也可能是整个文件) #include <fcntl.h>
/**
 * Perform a cpool flock (byte range lock) operation on an open
 * file.
 *
 * @param[in] fd                File descriptor.
 * @param[in] domain            The lock domain one of
 *       LOCK_DOMAIN_CPOOL_CACHE, LOCK_DOMAIN_CPOOL_STUB,
 *       LOCK_DOMAIN_CPOOL_SYNCH.
 * @param[in] a_op              One of the locking commands
 *       values: F_SETLK, F_UNLCK, F_GETLK, F_CONVERTLK.
 * @param[in] a_fl              flock structure pointer.
 *      			Used to set the range of the
 *      			lock.
 *      			The l_type of the a_fl will
 *      			contain one of the possible lock
 *      			values that are in:
 *      			cpool_lock_types: CPOOL_LK_SR
 *      			CPOOL_LK_SW CPOOL_LK_X CPOOL_LK_UNLCK
 * @param[in] a_flags           One of F_SETLKW or
 *       			F_SETLK_REMOTE
 * @param[in] retries		retry count in case of
 *       			recoverable errors. The default is to
 *				retry infinitely on EINTR
 * @param[in] cflt		whether the lock is to be taken on
 *				behalf of the calling thread or process.
 *
 * @return 0 on success, -1 on failure. Caller should check errno...
 * @return EWOULDBLOCK if lock cannot be granted and async_ok is false.
 * @return EINPROGRESS if lock will be granted asynchronously.
 */
extern "C" int thr_self(long *id);
inline int ifs_cpool_flock(int fd, enum cpool_lock_domain domain,
						   int a_op, struct flock *a_fl, int a_flags,
						   int retries = DEFAULT_CPOOL_FLOCK_RETRY_COUNT,
						   ifs_cpool_flock_locker_t cflt = CFLT_THREAD)
{
	/*
	 * retries is type char so that overloading does not confuse
	 * it with the pid, even though the system call has both pid and tid.
	 * char is certainly large enough for a retry count.
	 */
	int flock_retry_count = 0;
	long tid = 0;
	int error = 0;

	// Use thread id if this is to be a thread level locking operation
	if (cflt == CFLT_THREAD)
	{
		error = thr_self(&tid);
		if (error != 0)
		{
			goto out;
		}
	}

	while (retries == DEFAULT_CPOOL_FLOCK_RETRY_COUNT ||
		   flock_retry_count < retries)
	{
		flock_retry_count++;
		error = ifs_cpool_flock(fd, domain, a_op, a_fl, a_flags,
								false, 0,
								(uint32_t)getpid(), (uint32_t)((uint64_t)tid & 0xFFFFFFFF));
		if ((error == 0) || (errno != EINTR) ||
			isi_cloud_common_is_shutdown_marked())
		{
			// success or a real error, exit the loop
			break;
		}
		ilog(IL_DEBUG, "Interrupt returned from ifs_cpool_flock "
					   "on retry %d of %d",
			 flock_retry_count, retries);
	}
	if (error != 0 && errno != EINTR && (a_op == F_UNLCK || a_op == F_CONVERTLK))
	{
		if (!isi_cpool_revoked_fd(fd))
		{
			// Unlocking should always succeed
			// Usually when it fails it points to
			// holding a file descriptor for a file that
			// was already closed so a core will help us identify that.

			// Converting a lock should always succeed.
			// We always do a downgrade convert from Exclusive to Shared.
			// Usually when it fails it points to holding a file descriptor
			// for a file that was already closed so a core will help us
			// identify that.
			// This is true also because we register to SIGQUORUM and under
			// that (RIPTIDE and forward) no merge-lock CAVIATs can occur.
			// Also all CPOOL domains are not supporting merge-locks.
			ASSERT(0, "errno is %d, op is %d", errno, a_op);
		}
		else
		{
			ilog(IL_INFO, "ifs_cpool_flock no unlocking/converting"
						  "fd is revoked %d of %d",
				 fd, a_op);
		}
	}

out:
	return error;
}
#endif
