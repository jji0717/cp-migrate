#include "isi_cbm_scoped_flock.h"

#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/stat.h>
#include <sys/proc.h>
#include <unistd.h>

#include <isi_cpool_cbm/isi_cbm_error.h>

isi_cbm_scoped_flock::isi_cbm_scoped_flock(enum cpool_lock_domain domain,
    ifs_cpool_flock_locker_t locker_type) : domain_(domain), locked_(false)
	, fd_(-1), locker_type_(locker_type)
{
	flock_.l_start = 0;
	flock_.l_len = 0;
	flock_.l_pid = 0;
	flock_.l_type = CPOOL_LK_SR;
	flock_.l_whence = SEEK_SET;
	flock_.l_sysid = 0;
}

isi_cbm_scoped_flock::~isi_cbm_scoped_flock()
{
	unlock();
}

void
isi_cbm_scoped_flock::lock_ex(int fd, bool wait, struct isi_error **error_out)
{
	ASSERT(!locked_ && error_out);
	struct isi_error *error = NULL;
	int retries = DEFAULT_CPOOL_FLOCK_RETRY_COUNT;

	fd_ = fd;
	flock_.l_start = 0;
	flock_.l_len = 0;
	flock_.l_pid = getpid();
	flock_.l_type = CPOOL_LK_X;
	flock_.l_whence = SEEK_SET;
	flock_.l_sysid = 0;
	errno = 0;
	if (ifs_cpool_flock(fd, domain_,
	    F_SETLK, &flock_, wait ? F_SETLKW : 0, retries, locker_type_)) {
		if (wait || errno != EWOULDBLOCK) {
			error = isi_cbm_error_new(CBM_DOMAIN_LOCK_FAILURE,
			    "Failed to lock(domain=%d) file: fd(%d),errno(%d)",
			    domain_, fd, errno);
		}
		if (errno == EWOULDBLOCK) {
			error = isi_cbm_error_new(CBM_DOMAIN_LOCK_CONTENTION,
			    "Contention to lock(domain=%d) file: fd(%d),errno(%d)",
			    domain_, fd, errno);

		}
	} else 
		locked_ = true;

	isi_error_handle(error, error_out);
}


void
isi_cbm_scoped_flock::lock_sr(int fd, bool wait, struct isi_error **error_out)
{
	ASSERT(!locked_ && error_out);
	struct isi_error *error = NULL;
	int retries = DEFAULT_CPOOL_FLOCK_RETRY_COUNT;

	fd_ = fd;
	flock_.l_start = 0;
	flock_.l_len = 0;
	flock_.l_pid = getpid();
	flock_.l_type = CPOOL_LK_SR;
	flock_.l_whence = SEEK_SET;
	flock_.l_sysid = 0;

	errno = 0;
	if (ifs_cpool_flock(fd, domain_,
	    F_SETLK, &flock_, wait ? F_SETLKW : 0, retries, locker_type_)) {
		if (wait || errno != EWOULDBLOCK) {
			error = isi_cbm_error_new(CBM_DOMAIN_LOCK_FAILURE,
			    "Failed to lock(domain=%d) file: fd(%d),errno(%d)",
			    domain_, fd, errno);
		}
		if (errno == EWOULDBLOCK) {
			error = isi_cbm_error_new(CBM_DOMAIN_LOCK_CONTENTION,
			    "Contention to lock(domain=%d) file: fd(%d),errno(%d)",
			    domain_, fd, errno); 
		}


	} else
		locked_ = true;

	isi_error_handle(error, error_out);
}

void
isi_cbm_scoped_flock::unlock()
{
	int retries = DEFAULT_CPOOL_FLOCK_RETRY_COUNT;

	if (locked_) {
		int rc = ifs_cpool_flock(fd_, domain_,
		    F_UNLCK, &flock_, 0, retries, locker_type_);

		// this should always succeed, otherwise a bad bug
		ASSERT(rc == 0 || strerror(errno));
		locked_ = false;
	}
}

scoped_sync_lock::scoped_sync_lock(ifs_cpool_flock_locker_t locker_type)
	: isi_cbm_scoped_flock(LOCK_DOMAIN_CPOOL_SYNCH, locker_type)
{}

scoped_stub_lock::scoped_stub_lock(ifs_cpool_flock_locker_t locker_type)
	: isi_cbm_scoped_flock(LOCK_DOMAIN_CPOOL_STUB, locker_type)
{}


