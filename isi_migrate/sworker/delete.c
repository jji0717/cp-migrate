#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <signal.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/dfm/dfm_on_disk.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include "isi_domain/dom.h"

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/treewalk.h"
#include "sworker.h"

struct held_entry {
	unsigned hash;
	unsigned enc;
	char name[0];
};

/*  _   _ _   _ _     
 * | | | | |_(_) |___ 
 * | | | | __| | / __|
 * | |_| | |_| | \__ \
 *  \___/ \__|_|_|___/
 */

/**
 * Check connection with primary 
 */
void
check_connection_with_primary(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
 {
	int sock = sw_ctx->global.primary;
	struct isi_error *error = NULL;
 
 	log(TRACE, "check_connection_with_primary");
 
	check_connection_with_peer(sock, false, 1, &error);
	if (error)
		goto out;
 	verify_run_id(sw_ctx, &error);
out:
	isi_error_handle(error, error_out);
}

bool
entry_in_list(const char *name, int enc, const char *list, int len,
    struct migr_sworker_ctx *sw_ctx) {
	struct migr_sworker_delete_ctx *del = &sw_ctx->del;
	int off, start;
	bool found = false;
	bool wrapped = false;
	char *lname;
	int lenc;
	struct isi_error *error = NULL;


	/* entry can't be in an empty list */
	if (len == 0)
		goto out;

	/* start looking where we left off last time, since entries will
	 * usually appear in the same order in the list and the local dir. if
	 * this isn't the case, we still search the whole list */
	off = start = del->bookmark;
	while (!found) {
		/* periodically check pworker connection */
		if ((del->op_count++ % 1000) == 0) {
			check_connection_with_primary(sw_ctx, &error);
			if (error)
				exit(EXIT_SUCCESS);
		}

		lname = (char *)(list + off + sizeof(u_int16_t));
		lenc = *(u_int16_t *)(list + off);

		/* name and encoding must match */
		if (lenc == enc && (strcmp(lname, name) == 0))
			found = true;

		off += sizeof(u_int16_t) + strlen(lname) + 1;
		if (off >= len) {
			/* sanity check to make sure we aren't stuck in an
			 * infinite loop over a malformed list (should wrap
			 * only once) */
			ASSERT(!wrapped);
			wrapped = true;
			off -= len;
		}

		/* if we've seen the whole list, give up */
		if (off == start)
			break;
	}

	/* save place for next time */
	del->bookmark = off;

out:
	return found;
}

/* hold an entry in the next hold queue to be checked against the next list */
static void
hold_entry(char *name, int enc, unsigned hash,
    struct migr_sworker_ctx *sw_ctx) {
	struct held_entry *entry = NULL;

	log(TRACE, "hold_entry");
	log(DEBUG, "hold name %s enc %d hash %u", name, enc, hash);

	/* buffer holds file encoding, hash value and name */
	entry = malloc(sizeof(struct held_entry) + strlen(name) + 1);
	entry->hash = hash;
	entry->enc = enc;
	memcpy(entry->name, name, strlen(name) + 1);

	pvec_insert(sw_ctx->del.next_hold_queue, 0, (void *)entry);
}

/* get an entry from the current hold queue. caller provides a buffer to hold
 * the entry name */
static int
get_held_entry(char *name_out, int *enc_out, unsigned *hash_out,
    struct migr_sworker_ctx *sw_ctx) {
	struct held_entry *entry = NULL;

	log(TRACE, "get_held_entry");

	if (!pvec_pop(sw_ctx->del.hold_queue, (void **)&entry)) {
		log(DEBUG, "hold_queue empty");
		return -1;
	}

	*enc_out = entry->enc;
	*hash_out = entry->hash;
	memcpy(name_out, entry->name, strlen(entry->name) + 1);
	free(entry);

	log(DEBUG, "get name %s enc %d hash %u",
	    name_out, *enc_out, *hash_out);

	return 0;
}

/* clean out the queue in case of errors */
static void
cleanup_hold_queues(struct migr_sworker_ctx *sw_ctx) {
	struct entry *entry = NULL;

	while (pvec_pop(sw_ctx->del.hold_queue, (void **)&entry))
		free(entry);

	while (pvec_pop(sw_ctx->del.next_hold_queue, (void **)&entry))
		free(entry);
}

/* determine whether to preserve entry, delete it, or hold it for comparison
 * with the next list */
static void
process_entry(const char *list, int len, unsigned range_begin,
    unsigned range_end, bool continued, char *name, int enc, unsigned hash,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	/* if it's in the list, skip it (don't delete) */
	if (entry_in_list(name, enc, list, len, sw_ctx))
		goto out;

	if (hash < range_end || (hash == range_end && !continued)) {
		ASSERT(hash >= range_begin);
		/* wasn't found in the list and the range has been
		 * covered, so delete it */
		delete(name, enc, sw_ctx, &error);
		if (error)
			goto out;
	} else {
		/* wasn't found, but could be in the next list */
		hold_entry(name, enc, hash, sw_ctx);
	}

out:
	isi_error_handle(error, error_out);
}

static void
verify_tw_sync(struct migr_sworker_ctx *sw_ctx, const char *list, int len,
    struct isi_error **error_out)
{
	/* bug 96259 indicates that under longevity/group change conditions,
	 * we may fail to sync a dir during initial sync, but not notice it
	 * is missing until we try to sync a file to the missing dir during
	 * an incremental sync. to catch the failure earlier, we want to verify
	 * that every dir/file in a delete (keep) list exists on the target */
	log(DEBUG, "%s", __func__);
	
	int off = 0;
	char path[MAXPATHLEN + 1];
	char *name = NULL;
	struct isi_error *error = NULL;
	struct stat st;

	while (off < len) {
		name = (char *)(list + off + sizeof(u_int16_t));
		sprintf(path, "%s/%s", sw_ctx->worker.cur_path.path, name);

		if (lstat(path, &st) != 0 && errno == ENOENT) {
			/* entry in delete list but not target dir. create an
			 * ioerror to stop the job */
			log(ERROR, "bug 96259 entry %s not found", path);
			error = isi_system_error_new(errno,
			    "bug 96259 entry %s not found", path);
			goto out;
		}

		log(TRACE, "%s %s exists", __func__, path);
		off += sizeof(uint16_t) + strlen(name) + 1;
	}

out:
	isi_error_handle(error, error_out);
}

/*  _____      _                  
 * | ____|_  _| |_ ___ _ __ _ __  
 * |  _| \ \/ / __/ _ \ '__| '_ \ 
 * | |___ >  <| ||  __/ |  | | | |
 * |_____/_/\_\\__\___|_|  |_| |_|
 */

/* keep or delete files based upon list messages from the source */
void
process_list(const char *list, int len, unsigned range_begin,
    unsigned range_end, bool continued, bool last,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct migr_sworker_delete_ctx *del = &sw_ctx->del;
	struct migr_dirent *mde;
	struct isi_error *error = NULL;
	unsigned hash;
	char namebuf[MAXNAMELEN + 1];
	char *name;
	int enc;
	int dup_fd;
	struct ptr_vec *pvtmp;

	log(TRACE, "process_list");
	log(DEBUG, "processing list for range %x-%x, continued: %d, last: %d",
	    range_begin, range_end, continued, last);

	if (sw_ctx->global.verify_tw_sync) {
		verify_tw_sync(sw_ctx, list, len, &error);
		if (error)
			goto out;
	}

	/* starting delete phase on new directory? */
	if (!del->in_progress) {
		ASSERT(del->mdir == NULL);
		ASSERT(pvec_empty(del->hold_queue));
		ASSERT(range_begin == 0);

		del->error = false;
		del->in_progress = true;

		dup_fd = dup(sw_ctx->worker.cwd_fd);
		if (dup_fd < 0) {
			/* give up on processing this directory */
			error = isi_system_error_new(errno, "dup failed");
			log(ERROR, "dup failed (%d)", errno);
			del->error = true;
			goto out;
		}
		del->mdir = migr_dir_new(dup_fd, NULL, true, &error);
		if (error) {
			/* give up on processing this directory */
			log(ERROR, "migr_dir_new failed");
			close(dup_fd);
			del->error = true;
			goto out;
		}
	}

	/* for pre-Chopu sources, if we encounter an error in list processing
	 * for a directory, we want to skip the whole directory (may be more
	 * than one list msg). for Chopu and later, an error in this code path
	 * fails the job */
	if (del->error)
		goto out;

	/* last list message for directory must cover up to end */
	ASSERT(!last || range_end == 0x7FFFF);

	/* new list so reset bookmark */
	del->bookmark = 0;

	/* adjacent list messages may overlap on a hash bucket due to
	 * collisions. lists that will be followed with an overlapping list
	 * have the "continued" flag set.
	 *
	 * when this happens, entries that have hashes in the overlap that are
	 * not in the current list need to be held for checking against
	 * subsequent lists until either the entry is found (kept), or the list
	 * hash range moves beyond the held entry's hash value */

	/* for each local entry that was held due to an overlap, process it
	 * with the new list */
	while (get_held_entry(namebuf, &enc, &hash, sw_ctx) == 0) {
		/* no held entry should have a hash value outside the range
		 * of the current list message */
		ASSERT(hash <= range_end);

		process_entry(list, len, range_begin, range_end, continued,
		    namebuf, enc, hash, sw_ctx, &error);
		if (error) {
			del->error = true;
			goto out;
		}
	}

	/* for each local entry that lies within the hash range of the message,
	 * keep it if it is identified in the list message, and delete it
	 * otherwise */
	while ((mde = migr_dir_read(del->mdir, &error))) {
		if (error) {
			/* give up on processing this directory */
			log(ERROR, "migr_dir_read failed");
			del->error = true;
			goto out;
		}

		/* periodically check pworker connection */
		if ((del->op_count++ % 1000) == 0) {
			check_connection_with_primary(sw_ctx, &error);
			if (error)
				exit(EXIT_SUCCESS);
		}

		/* skip unsupported types */
		if (stat_mode_to_file_type(mde->stat->st_mode)
		    == SIQ_FT_UNKNOWN) {
			migr_dir_unref(del->mdir, mde);
			continue;
		}

		hash = dfm_key2hash(mde->cookie);
		name = mde->dirent->d_name;
		enc = mde->dirent->d_encoding;

		/* only consider entries with hash values that fall within the
		 * list range. if out of range, wait for a list message with
		 * the correct range */
		if (hash > range_end) {
			migr_dir_unread(del->mdir);
			break;
		}

		process_entry(list, len, range_begin, range_end, continued,
		    name, enc, hash, sw_ctx, &error);
		migr_dir_unref(del->mdir, mde);
		if (error) {
			del->error = true;
			goto out;
		}
	}

	/* sanity check. the current hold queue should be empty */
	ASSERT(pvec_empty(del->hold_queue));

	if (last) {
		/* the next hold queue must be empty if no more messages are
		 * expected! */
		ASSERT(pvec_empty(del->next_hold_queue));
		del->in_progress = false;
		migr_dir_free(del->mdir);
		del->mdir = NULL;
	}

	/* swap the hold_queue and next_hold_queue */
	pvtmp = del->hold_queue;
	del->hold_queue = del->next_hold_queue;
	del->next_hold_queue = pvtmp;

out:
	/* if an error occurred, give up on this directory and prepare for the
	 * next one */
	if (del->error && last) {
		/* cleanup and reset in_progress to false so we can
		 * start fresh on the next directory */
		cleanup_hold_queues(sw_ctx);
		migr_dir_free(del->mdir);
		del->mdir = NULL;
		del->in_progress = false;
		del->error = false;
	}

	isi_error_handle(error, error_out);
}

/*  ____       _      _       
 * |  _ \  ___| | ___| |_ ___ 
 * | | | |/ _ \ |/ _ \ __/ _ \
 * | |_| |  __/ |  __/ ||  __/
 * |____/ \___|_|\___|\__\___|
 */

static volatile int safe_rmrf_is_in_progress;
#define KEEP_ALIVE_INTERVAL	5

static void
safe_rmrf_sigchld(int signo)
{
	safe_rmrf_is_in_progress = 0;
}

/**
 * This is a wrapper around "rm -rf" designed to be safe for the 
 * full range of valid filenames (ie, no problems with spaces, shell
 * escape characters, etc).  name must be a single component relative
 * name with no slashes.  It may not be "." or "..".
 *
 * Returns -1 on error, 0 otherwise.
 */
void
safe_rmrf(int dirfd, const char *name, enc_t enc, int sock_to_monitor, 
    ifs_domainid_t domain_id, uint32_t domain_generation,
    struct isi_error **error_out)
{
	int len;
	pid_t child;
	struct isi_error *error = NULL;
	struct sigaction ign, intact, quitact, chldact, our_chldact;
	sigset_t newsigblock, oldsigblock;

	/**
	 * Fail for invalid filenames. We should never see these,
	 * but be paranoid just in case.
	 */
	for (len = 0; 0 != name[len]; len++) {
		if (name[len] == '/') {
			error = isi_system_error_new(EINVAL, "Recursive "
			    "delete name argument contains slashes: %s", name);
			goto out;
		}
	}

	if ((len == 0) || (len == 1 && name[0] == '.') ||
	    (len == 2 && name[0] == '.' && name[1] == '.')) {
		error = isi_system_error_new(EINVAL, "Recursive delete called "
		    "with invalid name argument: %s", name);
		goto out;
	}

	/**
	 * Use the same signal handler logic as system(). Only the
	 * SIGCHLD handling is truly necessary here to let us wait for
	 * the child we spawn.
	 */
	ign.sa_handler = SIG_IGN;
	sigemptyset(&ign.sa_mask);
	ign.sa_flags = 0;
	sigaction(SIGINT, &ign, &intact);
	sigaction(SIGQUIT, &ign, &quitact);
	if (sock_to_monitor == -1) { /* there is no socket to monitor */
		/* save old child action...*/
		sigaction(SIGCHLD, NULL, &chldact);
		/* ... and block SIGCHLD */
		sigemptyset(&newsigblock);
		sigaddset(&newsigblock, SIGCHLD);
		sigprocmask(SIG_BLOCK, &newsigblock, &oldsigblock);
	} else {
		/* set up new SIGCHLD handler saving the old one... */
		our_chldact.sa_handler = safe_rmrf_sigchld;
		sigfillset(&our_chldact.sa_mask);
		our_chldact.sa_flags = 0;
		sigaction(SIGCHLD, &our_chldact, &chldact);
		/* ... and unblock SIGCHLD signal saving old proc mask */
		sigemptyset(&newsigblock);
		sigaddset(&newsigblock, SIGCHLD);
		sigprocmask(SIG_UNBLOCK, &newsigblock, &oldsigblock);

		safe_rmrf_is_in_progress = 1;
	}

	child = fork();

	/* Fork and remove */
	if (child != 0) {
		/* Parent process */
		pid_t ret_child;
		int status;
		if (child == -1) {
			error = isi_system_error_new(errno,
			    "Error spawning recursive delete child process");
			goto out;
		}

		if (sock_to_monitor != -1) {
			/* 
			 * While waiting for child process to complete,
			 * monitor connection with primary by sending
			 * out-of-band byte periodically. At any time
			 * we can receive SIGCHLD signal from child.
			 * Firstly it should interrupt 'long' system
			 * calls like sleep and send, secondly signal
			 * handler sets safe_rmrf_is_in_progress to
			 * false that leads to going out of the loop.
			 */
			while (safe_rmrf_is_in_progress) {
				log(TRACE, "safe_rmrf in progress");
				check_connection_with_peer(
				    sock_to_monitor, false, 1, &error);
				if (error) {
					/* Connection is lost */
					log(INFO, "Disconnected from primary "
					    "worker (fd = %d)",
					    sock_to_monitor);
					if (kill(child, SIGKILL) == -1) {
						log(FATAL, "Failed to kill "
						    "safe_rmrf child process");
					}
					exit(EXIT_SUCCESS);
				}
				if (!safe_rmrf_is_in_progress)
					break;
				siq_nanosleep(KEEP_ALIVE_INTERVAL, 0);
			}
		}

		do {
			ret_child = waitpid(child, &status, 0);
		} while (ret_child == -1 && errno == EINTR);

		if (ret_child != child || !WIFEXITED(status) ||
		    WEXITSTATUS(status)) {
			error = isi_system_error_new(EIO,
			    "Recursive delete on directory %s failed: %s", name,
			    strerror(errno));
			goto out;
		}
	} else {
		/* Child process */
		char *argv[5];
		int ret;

		sigaction(SIGINT, &intact, NULL);
		sigaction(SIGQUIT, &quitact, NULL);
		sigaction(SIGCHLD, &chldact, NULL);
		sigprocmask(SIG_SETMASK, &oldsigblock, NULL);

		argv[0] = "rm";
		argv[1] = "-rf";
		argv[2] = "--";
		asprintf(&argv[3], "./%s", name);
		argv[4] = NULL;

		isi_cbm_init(&error);
		if (error)
			goto out;


		/* Move child proc's working dir to match context cwd */
		ret = fchdir(dirfd);
		
		/* Bypass domain restricted write */
		if (domain_id != 0 && ret == 0) {
			ret = ifs_domain_allowwrite(domain_id, 
			    domain_generation);
			if (ret != 0)
				_exit(-1);
		}

		/* Set per-process encoding to enc and remove. */
		if ((0 == ret) && (0 == enc_set_proc(enc)))
			execv("/bin/rm", argv);

		/* Only get here on error, avoid flushing streams */
		_exit(-1);
	}

out:
	sigaction(SIGINT, &intact, NULL);
	sigaction(SIGQUIT, &quitact, NULL);
	sigaction(SIGCHLD, &chldact, NULL);
	sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
	isi_error_handle(error, error_out);
}

void
delete(char *name, enc_t enc, struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct isi_error *error = NULL, *q_err = NULL;
	struct stat st;
	int selected;
	int ret = -1;
	int fd = -1;
	bool is_comp_store = false;
	bool dom_gen_changed = false;

	log(COPY, "Deleting %s/%s", worker->cur_path.path, name);

	if (enc_fstatat(worker->cwd_fd, name, enc, &st,
	    AT_SYMLINK_NOFOLLOW) == -1) {
		if (errno == ENOENT) {
			/* If the file/dir is already gone, nothing to do */
			log(DEBUG, "Failed to delete %s/%s, does not exist",
			    worker->cur_path.path, name);
		} else {
			error = isi_system_error_new(errno,
			    "Failed to stat %s/%s",
			    worker->cur_path.path, name);
		}
		goto out;
	}

	/* Skip files under global exclude rules */
	selected = siq_select(NULL, name, enc, &st, time(NULL), NULL,
	    stat_mode_to_file_type(st.st_mode), &error);
	if (error)
		goto out;

	if (!selected) {
		log(COPY, "Not deleting file %s/%s skipped by global "
		    "exclude rules", worker->cur_path.path, name);
		goto out;
	}

	/*
	 * If the current dirent is compliance store, then
	 * dont try to delete it.
	 */

	if (global->compliance_v2 || global->compliance) {
		/*
		 * We skip all compliance store entries.
		 */
		fd = ifs_lin_open(st.st_ino, HEAD_SNAPID, O_RDONLY);
		if (fd == -1 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %{}", lin_fmt(st.st_ino));
			goto out;
		}
		if (fd == -1 && errno == ENOENT)
			goto out;

		is_comp_store = is_compliance_store(fd, &error);
		if (error || is_comp_store)
			goto out;

		close(fd);
		fd = -1;
	}

	if (S_ISDIR(st.st_mode)) {
		/* for now we can only handle the case where the root of the
		 * recursive delete has immutable flags set. a better long
		 * term solution would be to either:
		 *    a) replace the system rm -rf call with a programmatic
		 *       recursive delete that can clear the flags
		 * or
		 *    b) handle the failed rm -rf by moving the directory to
		 *       a temp location and creating an alert telling the
		 *       user to manually unset flags and delete it */
		if (st.st_flags & IMMUT_APP_FLAG_MASK) {
			clear_flags(&st, IMMUT_APP_FLAG_MASK,
			    name, enc, &error);
			if (error)
				goto out;
		}

		safe_rmrf(sw_ctx->worker.cwd_fd, name, enc, global->primary,
		    global->domain_id, global->domain_generation, &error);
		if (error) {
			ret = enc_fstatat(worker->cwd_fd, name, enc, &st,
			    AT_SYMLINK_NOFOLLOW);
			if (ret == -1 && errno == ENOENT) {
				/* If safe_rmrf failed but the directory is
				 * gone, we'll let it slide */
				isi_error_free(error);
				error = NULL;
			} else {
				if (errno == EPERM) {
					check_quota_delete(st.st_ino, &q_err);
					if (q_err != NULL) {
						isi_error_free(error);
						error = q_err;
					}
				}
				goto out;
			}
		}
		if (global->stf_sync) {
			sw_ctx->stats.stf_cur_stats->dirs->deleted->dst++;
		} else {
			sw_ctx->stats.tw_cur_stats->dirs->dst->deleted++;
		}
	} else {
		if (st.st_flags & IMMUT_APP_FLAG_MASK) {
			clear_flags(&st, IMMUT_APP_FLAG_MASK,
			    name, enc, &error);
			if (error)
				goto out;
		}

		int rc = enc_unlinkat(worker->cwd_fd, name, enc, 0);
		if (rc && errno == EROFS) {
			/*
			 * Bug 205081
			 * If the domain generation changed, exit and don't
			 * send an error message.
			 */
			dom_gen_changed = check_dom_gen(sw_ctx, &error);
			if (error) {
				/*
				 * log an error with check_dom_gen, but
				 * alert based on the enc_unlinkat error
				 */
				log(ERROR, "check_dom_gen error: %{}",
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			} else if (dom_gen_changed)
				exit(EXIT_SUCCESS);
			else if (!S_ISDIR(st.st_mode))
				rc = try_force_delete_entry(worker->cwd_fd,
				    name, enc);
		}

		if (rc && errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Failed to unlink %s/%s (%llx)",
			    worker->cur_path.path, name, st.st_ino);
			goto out;
		}
		if (global->stf_sync) {
			sw_ctx->stats.stf_cur_stats->files->deleted->dst++;
		} else {
			sw_ctx->stats.tw_cur_stats->files->deleted->dst++;
		}
	}

	/* Don't add ADS entries to delete list */
	if (global->logdeleted && !inprog->ads) {
		log(TRACE, "Log deleted file %s", name);
		listadd(&inprog->deleted_files, name, enc, 0, LIST_TO_SLOG, 
		    global->primary, worker->cur_path.utf8path);
	}


out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);
}
