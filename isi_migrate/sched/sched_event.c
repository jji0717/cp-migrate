#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <fcntl.h>
#include "sched_includes.h"
#include "sched_event.h"


#define SIQ_SOURCE_RECORD_DIR SIQ_IFS_CONFIG_DIR "source_records"
#define SIQ_POLICIES_FILE SIQ_IFS_CONFIG_DIR "siq-policies.gc"
#define SIQ_CONF_FILE SIQ_IFS_CONFIG_DIR "siq-conf.gc"
#define SCHED_MAX_EVENTS 64


static bool 
set_file_watch(int kq_fd, const char *path, int *fd_in_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct kevent ke = {};
	int fd;
	int res;
	bool exists = true;

	ASSERT(path && *path);
	ASSERT(fd_in_out);
	ASSERT(*fd_in_out == -1);

	fd = open(path, O_RDONLY);
	if (fd == -1) {
		if (errno == ENOENT)
			exists = false;
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error opening file %s for watch: %s",
		    path, strerror(errno));
		goto out;
	}

	EV_SET(&ke, fd, EVFILT_VNODE, EV_ADD | EV_ENABLE | EV_CLEAR,
	    NOTE_DELETE | NOTE_EXTEND | NOTE_WRITE | NOTE_ATTRIB, 0, NULL);
	res = kevent(kq_fd, &ke, 1, NULL, 0, NULL); 
	if (res == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error setting kevent watch on file %s: %s",
		    path, strerror(errno));
		goto out;
	}
	log(DEBUG, "Watching file %s (fd %d)", path, fd);

	*fd_in_out = fd;

out:
	if (error && fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
	return exists;
}


static bool
set_dir_watch(int kq_fd, const char *path, int *fd_in_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct kevent ke = {};
	int fd;
	int res;
	bool exists = true;

	ASSERT(path && *path);
	ASSERT(fd_in_out);
	ASSERT(*fd_in_out == -1);

	fd = open(path, O_RDONLY);
	if (fd == -1) {
		if (errno == ENOENT)
			exists = false;
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error opening directory %s for watch: %s",
		    path, strerror(errno));
		goto out;
	}

	EV_SET(&ke, fd, EVFILT_VNODE, EV_ADD | EV_ENABLE | EV_CLEAR,
	    NOTE_WRITE, 0, NULL);
	res = kevent(kq_fd, &ke, 1, NULL, 0, NULL); 
	if (res == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error setting kevent watch on directory %s: %s",
		    path, strerror(errno));
		goto out;
	}
	log(DEBUG, "Watching directory %s (fd %d)", path, fd);

	*fd_in_out = fd;

out:
	if (error && fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
	return exists;
}


static void
set_group_watch(int kq_fd, int *fd_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct kevent ke = {};
	int fd;
	int res;

	fd = gmp_change_fd_get(&error);
	if (error) {
		isi_error_free(error);
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error getting group file descriptor for watch");
		goto out;
	}

	EV_SET(&ke, fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR,
	    0, 0, NULL);
	res = kevent(kq_fd, &ke, 1, NULL, 0, NULL);
	if (res == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error setting kevent watch on group file descriptor: %s",
		    strerror(errno));
		goto out;
	}
	log(DEBUG, "Watching group change socket (fd %d)", fd);

	*fd_out = fd;

out:
	isi_error_handle(error, error_out);
}


bool
set_proc_watch(struct sched_ctx *sctx, int pid, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct kevent ke = {};
	int res;
	bool exists = true;

	log(TRACE, "%s", __func__);

	ASSERT(pid > 0);

	EV_SET(&ke, pid, EVFILT_PROC,
	    EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_EXIT, 0, NULL);
	res = kevent(sctx->kq_fd, &ke, 1, NULL, 0, NULL);	
	if (res == -1) {
		if (errno == ESRCH) {
			log(DEBUG, "Attempted to set watch on process that "
			    "does not exist (pid %d", pid);
			exists = false;
		} else {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "Error setting watch on process: %s",
			    strerror(errno));
		}
		goto out;
	}

	log(DEBUG, "Watching procid %d", pid);

out:
	isi_error_handle(error, error_out);
	return exists;
}


static void
set_or_reset_watches(struct sched_ctx *sctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool exists;

	/* monitor config directory */
	if (sctx->kq_config_dir_fd == -1) {
		set_dir_watch(sctx->kq_fd, SIQ_IFS_CONFIG_DIR,
		    &sctx->kq_config_dir_fd, &error);
		if (error)
			goto out;
	}

	/* monitor siq-conf.gc */
	if (sctx->kq_siq_conf_fd == -1) {
		exists = set_file_watch(sctx->kq_fd, SIQ_CONF_FILE,
		    &sctx->kq_siq_conf_fd, &error);
		if (error) {
			/* SIQ conf file may not exist yet... */
			if (exists == false) {
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}
	
	/* monitor policies file */
	if (sctx->kq_pols_fd == -1) {
		exists = set_file_watch(sctx->kq_fd, SIQ_POLICIES_FILE,
		    &sctx->kq_pols_fd, &error);
		if (error) {
			/* Policies file may not exist for a while on a newly
			 * configured cluster. We monitor the config directory
			 * so we'll see if/when the file is created */
			if (exists == false) {
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}

	/* monitor event file - used to wake the scheduler up for testing and
	 * to indicate a job has been resumed */
	if (sctx->kq_sched_event_fd == -1) {
		exists = set_file_watch(sctx->kq_fd, SIQ_SCHED_EVENT_FILE,
		    &sctx->kq_sched_event_fd, &error);
		if (error) {
			/* Sched_event file may not exist for a while on a newly
			 * configured cluster. We monitor the config directory
			 * so we'll see if/when the file is created */
			if (exists == false) {
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}

	/* monitor total_rationed_workers file */
	if (sctx->kq_workers_fd == -1) {
		exists = set_file_watch(sctx->kq_fd, SIQ_WORKER_POOL_TOTAL,
		    &sctx->kq_workers_fd, &error);
		if (error) {
			/* total workers file may not exist until the
			 * bandwidth daemon writes it. We'll check again on
			 * the next sched loop. */
			if (exists == false) {
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}

	/* monitor source record directory*/
	if (sctx->kq_src_rec_fd == -1) {
		exists = set_dir_watch(sctx->kq_fd, SIQ_SOURCE_RECORDS_DIR,
		    &sctx->kq_src_rec_fd, &error);
		if (error) {
			/* Source record dir may not exist for a while after
			 * initial cluster setup. We monitor the config
			 * directory so we'll see when the file is created */
			if (exists == false) {
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}

	/* monitor group changes */
	if (sctx->kq_group_fd == -1) {
		set_group_watch(sctx->kq_fd, &sctx->kq_group_fd, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}


static void
set_existing_coord_watches(struct sched_ctx *sctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	DIR *rundir = NULL;
	struct dirent *dp = NULL;
	int count;
	int nodeid;
	int procid;
	bool exists;

	rundir = opendir(sched_get_run_dir());
	if (rundir == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error opening run directory: %s", strerror(errno));
		goto out;
	}

	opendir_skip_dots(rundir);
	while ((dp = readdir(rundir))) {
		count = sscanf(dp->d_name, "%*x_%d_%d", &nodeid, &procid);
		if (count != 2)
			continue;
		if (nodeid != sctx->node_num)
			continue;

		/* attempt to watch coord process. it may not actually be
		 * running at this point */
		exists = set_proc_watch(sctx, procid, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
	if (rundir)
		closedir(rundir);
}


void
init_event_queue(struct sched_ctx *sctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	sctx->kq_src_rec_fd = -1;
	sctx->kq_pols_fd = -1;
	sctx->kq_siq_conf_fd = -1;
	sctx->kq_config_dir_fd = -1;
	sctx->kq_group_fd = -1;
	sctx->kq_sched_event_fd = -1;
	sctx->kq_workers_fd = -1;

	sctx->kq_fd = kqueue();
	if (sctx->kq_fd == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Error creating kqueue: %s", strerror(errno));
		goto out;
	}

	/* watch policies file, source record, config dir, trash dir, group
	 * socket, etc. */
	set_or_reset_watches(sctx, &error);
	if (error)
		goto out;

	/* watch local coordinator processes that were started by a previous
	 * scheduler */
	set_existing_coord_watches(sctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}


static void
handle_event(struct sched_ctx *sctx, struct kevent *ke,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int new_log_level;
	int newfd = -1;
	bool log_fds = true;

	switch (ke->filter) {
	case EVFILT_VNODE:	
		if (ke->ident == sctx->kq_pols_fd) {
			log(DEBUG, "Detected policies file change");
			if (ke->fflags & NOTE_DELETE) {
				log(DEBUG, "Policies file deleted");
				close(sctx->kq_pols_fd);
				sctx->kq_pols_fd = -1;
			}
		} else if (ke->ident == sctx->kq_sched_event_fd) {
			log(DEBUG, "Detected sched_event file change");
			if (ke->fflags & NOTE_DELETE) {
				log(DEBUG, "Sched_event file deleted");
				close(sctx->kq_sched_event_fd);
				sctx->kq_sched_event_fd = -1;
			}
		} else if (ke->ident == sctx->kq_workers_fd) {
			log(DEBUG, "Detected total workers file change");
			if (ke->fflags & NOTE_DELETE) {
				log(DEBUG, "total workers file deleted");
				close(sctx->kq_workers_fd);
				sctx->kq_workers_fd = -1;
			}
		} else if (ke->ident == sctx->kq_src_rec_fd) {
			log(DEBUG, "Detected source record dir change");
			if (ke->fflags & NOTE_DELETE) {
				log(DEBUG, "Source record file deleted");
				close(sctx->kq_src_rec_fd);
				sctx->kq_src_rec_fd = -1;
			}
		} else if (ke->ident == sctx->kq_siq_conf_fd) {
			log(DEBUG, "Detected siq conf file change");
			if (ke->fflags & NOTE_DELETE) {
				log(DEBUG, "SIQ conf file deleted");
				close(sctx->kq_siq_conf_fd);
				sctx->kq_siq_conf_fd = -1;
			}

			/* see if log level changed in siq-conf.gc */
			new_log_level = sched_get_log_level();
			if (new_log_level < 0) {
				log(ERROR, "Failed to read log level");
			} else if (new_log_level != sctx->log_level) {
				log(ERROR, "Log level: %d", new_log_level);
				set_siq_log_level(new_log_level);
				sctx->log_level = new_log_level;
			}
		} else if (ke->ident == sctx->kq_config_dir_fd)
			log(DEBUG, "Detected change in config directory");
		else {
			log(ERROR, "Unexpected vnode event on fd %d",
			    (int)ke->ident);
			goto out;
		}
		break;

	case EVFILT_READ:
		if (ke->ident == sctx->kq_group_fd) {
			log(DEBUG, "Detected group change activity");
			newfd = gmp_change_fd_get(&error);
			if (error)
				goto out;
			if (newfd != sctx->kq_group_fd)
				sctx->kq_group_fd = -1;
		} else {
			log(ERROR, "Unexpected read event on fd %d",
			    (int)ke->ident);
			goto out;
		}
		break;

	case EVFILT_PROC:
		log(DEBUG, "Process id %d exited", (int)ke->ident);
		break;

	default:
		log(ERROR, "Unexpected event type %d fd %d", ke->filter,
		    (int)ke->ident);
		goto out;
	}

	log_fds = false;

out:
	if (log_fds) {
		/* output the kq related fds in case of an error or
		 * unexpected event */
		log(ERROR, "kq_fd: %d kq_src_rec_fd: %d kq_pols_fd: %d "
		    "kq_config_dir_fd: %d kq_siq_conf_fd: %d "
		    "kq_group_fd: %d "
		    "kq_sched_event_fd: %d kq_workers_fd: %d",
		    sctx->kq_fd, sctx->kq_src_rec_fd, sctx->kq_pols_fd,
		    sctx->kq_config_dir_fd, sctx->kq_siq_conf_fd,
		    sctx->kq_group_fd,
		    sctx->kq_sched_event_fd, sctx->kq_workers_fd);
	}
	isi_error_handle(error, error_out);
}


/* wait for an event, or until the next scheduled job is due to run,
 * whichever comes first */
void
wait_for_event(struct sched_ctx *sctx, int timeout,
    struct isi_error **error_out)
{
	int nev1;
	int nev2;
	int i;
	struct kevent ke_arr[SCHED_MAX_EVENTS];
	struct isi_error *error = NULL;
	struct timespec ts1 = {timeout, 0};
	struct timespec ts2 = {0, 0};

	/* call kevent with a predetermined timeout, sleep briefly, then call
	 * kevent again with a timeout of zero. the coalesces bursts of events
	 * due to policy changes, source record activity, etc., that would
	 * otherwise cause multiple mainloop iterations per event */
	nev1 = kevent(sctx->kq_fd, NULL, 0, ke_arr, SCHED_MAX_EVENTS, &ts1);

	if (nev1 == -1) {
		if (errno != EINTR) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "kevent failed: %s", strerror(errno));
		}
		goto out;
	}
	
	if (nev1 == 0) {
		log(DEBUG, "kevent timed out (no events)");
		goto out;
	}

	siq_nanosleep(0, 500000);
	nev2 = kevent(sctx->kq_fd, NULL, 0, ke_arr + nev1,
	    SCHED_MAX_EVENTS - nev1, &ts2);

	if (nev2 == -1) {
		if (errno != EINTR) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "kevent failed: %s", strerror(errno));
		}
		goto out;
	}
	
	/* see if any of the events require action */
	for (i = 0; i < nev1 + nev2; i++) {
		handle_event(sctx, &ke_arr[i], &error);
		if (error)
			goto out;
	}

	/* try to set/reset watches for invalid descriptors, i.e. when a file
	 * is deleted, or hasn't been created yet */
	set_or_reset_watches(sctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);	
}
