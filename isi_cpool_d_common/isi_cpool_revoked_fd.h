#ifndef __ISI_CPOOL_REVOKED_FD__H__
#define __ISI_CPOOL_REVOKED_FD__H__


/**
 *
 *
 * @author ssasson (2/12/2016)
 *
 * @param fd
 *
 * @return bool
 * This function purpose is to identify a stituation
 * where we hold a file descriptor of a revoked file.
 * This can occure while we are trying to unlock a file
 * Please see bug 166328
 */
bool isi_cpool_revoked_fd(int fd);

#endif
