#include <isi_ilog/ilog.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_config/array.h>
#include <isi_gmp/isi_gmp.h>
#include <fcntl.h>
#include <sys/file.h>
#include <stdio.h>
#include <unistd.h>
#include "thread_mastership.h"



bool acquire_mastership(int *lock_fd_in, const char *lockfile,
    const char *masterfile)
{
	int master_fd = -1;
	struct isi_error *error = NULL;
	FILE *dest = NULL;
	int32_t nodeid = -1;
	int num_of_bytes_written = -1;
	int lock_fd = -1;
	bool mastership_obtained = false;
	ASSERT(lockfile != NULL &&
	    masterfile != NULL &&
	    lock_fd_in != NULL &&
	    strlen(lockfile) != 0 &&
	    strlen(masterfile) != 0);

	lock_fd = *lock_fd_in;

	// Test if there is a quorum
	bool has_quorum = gmp_has_quorum(&error);

	if (error != NULL) {
		ilog(IL_ERR,
		    "thread is retrying due to failure"
		    "on querying for quorum: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		goto out;

	}

	if (!has_quorum) {
		ilog(IL_DEBUG,
		    "thread is retrying due to no "
		    "quorum");
		goto out;

	}

	if (lock_fd == -1) {
		lock_fd = open(lockfile, O_CREAT | O_RDONLY, 0400);

		if (lock_fd == -1) {
			ilog(IL_TRACE,
			    "thread is retrying due to failure "
			    "opening a lock file: %s error: %s",
			    lockfile, strerror(errno));
			goto out;
		}
	}

	// Try to take an exclusive lock on the file
	// This is done with non-blocking mode
	///在非阻塞且拿不到锁的情况下,返回0:表示成功,返回-1: errno=EWOULDBLOCK
	////没拿到mastership的goto out ????
	if (flock(lock_fd, LOCK_EX | LOCK_NB)) {
		ilog(IL_TRACE,
		    "thread is retrying due to failure "
		    "locking the lock file: %s error: %s",
		    lockfile, strerror(errno));
		goto out;
	}

	// Open the master file for writing the master devid
	// Retrying in case the node is read only waiting for it
	// to be RWR
	master_fd = open(masterfile, O_CREAT | O_RDWR, 0600);
	if (master_fd == -1) {
		ilog(IL_ERR,
		    "thread is retrying due to failiure "
		    "openning a master file: %s error:%s",
		    masterfile, strerror(errno));
		goto out;

	}

	// If we reached here we are the master.
	// Open a file for writing the node id
	// of the master to it.
	dest = fdopen(master_fd, "w+");
	if (dest == NULL) {
		ilog(IL_ERR,
		    "thread is retrying due to failiure "
		    "fdopen a master file: %s error: %s",
		    masterfile, strerror(errno));
		goto out;

	}

	// Try the get the node id of the master
	nodeid = arr_get_cluster_nodeid(&error);
	if (error != NULL) {
		ilog(IL_ERR,
		    "thread is retrying due to failiure"
		    "to get a node id: %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		goto out;
	}

	// Write the node id to the file
	num_of_bytes_written = fprintf(dest, "%d\n", nodeid);
	if (num_of_bytes_written < 0) {
		ilog(IL_ERR,
		    "thread is retrying due to failiure"
		    "writing node id file:%s error:%s",
		    masterfile, strerror(errno));
		goto out;

	}
	fflush(dest);

	mastership_obtained = true;

 out:
	if (master_fd != -1 && dest == NULL)
		close(master_fd);
	if (dest != NULL)
		fclose(dest);

	if (!mastership_obtained && lock_fd != -1) {
		flock(lock_fd, F_UNLCK);
	}

	*lock_fd_in = lock_fd;

	return mastership_obtained;

}

