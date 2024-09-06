#pragma once

#include <isi_cpool_d_common/ifs_cpool_flock.h>

/**
 * class encapsulating the locking of the cpool domain lock.
 * This object automatically releases the lock when destructed.
 * The caller can also explicitly unlock the lock
 * The fd must be valid through the lock and unlock period.
 *
 * At construction the user(locker_type) of the lock can be designated as the
 * creating process.  Such designation allows the process's thread that
 * acquired the lock to be different from the one that releases the lock.
 * Otherwise, the lock must only be released by the thread that acquired the
 * lock.
 *
 * MT-unsafe
 */
class isi_cbm_scoped_flock {
public:
	isi_cbm_scoped_flock(enum cpool_lock_domain domain,
	    ifs_cpool_flock_locker_t locker_type);

	~isi_cbm_scoped_flock();

	/**
	 * Acquire the domain lock exclusively on the file.
	 * @param fd[in] the file descriptor.
	 * @param wait[in] if to wait for the lock if already locked
	 */
	void lock_ex(int fd, bool wait, struct isi_error **error_out);

	/**
	 * Acquire the SHARED-READ domain lock on the file.
	 * @param fd[in] the file descriptor.
	 * @param wait[in] if to wait for the lock if already locked
	 */
	void lock_sr(int fd, bool wait, struct isi_error **error_out);

	// unlock the lock
	void unlock();
private:
	enum cpool_lock_domain domain_;
	bool locked_;
	int fd_;
	struct flock flock_;
	ifs_cpool_flock_locker_t locker_type_;
};


class scoped_sync_lock : public isi_cbm_scoped_flock {
public:
	scoped_sync_lock(ifs_cpool_flock_locker_t locker_type = CFLT_THREAD);

};

class scoped_stub_lock : public isi_cbm_scoped_flock {
public:
	scoped_stub_lock(ifs_cpool_flock_locker_t locker_type = CFLT_THREAD);

};
