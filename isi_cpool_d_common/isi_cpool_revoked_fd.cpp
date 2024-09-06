#include <sys/param.h>
#include <sys/sysctl.h>
#include <sys/user.h>
#include <sys/isi_assert.h>

#include <err.h>
#include <libprocstat.h>
#include <stdio.h>
#include <stdlib.h>
#include <sysexits.h>
#include <unistd.h>

#include "isi_cpool_revoked_fd.h"

/**
 * procstat_is_fd_revoked
 *
 * The function process the procstat and tries to find the file
 * descriptor fd. It then tries to determine if the vnode for
 * this fd is revoked.
 */

bool
procstat_is_fd_revoked(struct procstat *procstat, struct kinfo_proc *kipp, int fd)
{
	struct filestat_list *head = NULL;
	struct filestat *fst = NULL;
	struct vnstat vn;
	int error = 0;
	bool revoked = false;
	bool free_files = false;
	bool free_mount_string = false;

	vn.vn_mntdir = NULL;

	head = procstat_getfiles(procstat, kipp, 0);


	if (head == NULL) {
		goto out;
	}
	free_files = true;

	STAILQ_FOREACH(fst, head, next) {
		if (fst->fs_fd != fd) {
			continue;
		}

		if (fst != NULL && fst->fs_type == PS_FST_TYPE_VNODE) {

			error = procstat_get_vnode_info(procstat, fst,
				    &vn, NULL);

			if (vn.vn_mntdir != NULL) {
				free_mount_string = true;
			}
			if (error != 0) {
				goto out;
			}

			if (vn.vn_type == PS_FST_VTYPE_VBAD) {
				revoked = true;
				goto out;
			}

		}
	}
 out:
	if (free_files) {
		procstat_freefiles(procstat, head);
	}

	if (free_mount_string) {
		free(vn.vn_mntdir);
	}
	return revoked;
}

/**
 *
 *
 * @author ssasson (2/12/2016)
 *
 * @param fd
 *
 * @return bool
 * This function purpose is to identify a situation where we
 * hold a file descriptor of a revoked file. This can occure
 * while we are trying to unlock a file Please see bug 166328
 */
bool isi_cpool_revoked_fd(int fd) {
	unsigned int cnt = 0;
	struct kinfo_proc *p = NULL;
	struct procstat *prstat = NULL;
	bool revoked = false;
	bool freeprocs = false;
	bool freeprocstat = false;

	prstat = procstat_open_sysctl();

	ASSERT(prstat != NULL);

	freeprocstat = true;

	p = procstat_getprocs(prstat, KERN_PROC_PID, (uint32_t)getpid(), &cnt);
	freeprocs = true;

	ASSERT(p != NULL && cnt == 1);

	revoked = procstat_is_fd_revoked(prstat, p, fd);

	if (freeprocs) {
		procstat_freeprocs(prstat, p);
	}
	if (freeprocstat) {
		procstat_close(prstat);
	}
	return revoked;

}

