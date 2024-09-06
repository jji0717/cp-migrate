#include <stdio.h>
#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <ifs/ifs_syscalls.h>
#include <isi_ufp/isi_ufp.h>

#include "isirep.h"
#include "alg.h"
#include "cpu_throttle.h"

UFAIL_POINT_LIB(isi_migrate)

extern bool _g_expected_dataloss;
static enum origin g_parent;
static int g_snap_fd = -1;

struct siq_hash_req_entry {
	struct full_hash_file_req_msg file;
	TAILQ_ENTRY(siq_hash_req_entry) ENTRIES;
};
/* Defines struct siq_hash_reqs for hash list */
TAILQ_HEAD(siq_hash_reqs, siq_hash_req_entry);

struct hasher_ctx {
	int s;
	int dir_fd;
	enum hash_type hash_type;
	uint64_t min_piece_len;
	int pieces_per_file;
	struct path dir;
	enum origin origin;
	struct siq_hash_reqs queue;
	int queue_size;
	uint64_t src_dlin;
	bool stop_hashing;
	bool file_hash_in_progress;

	/* hashing state for current file */
	int cur_fd;
	char *buf;
	int total_read;
	struct hash_ctx *hash;
	uint64_t piece_len;
	uint64_t piece_so_far;
	uint64_t offset;

	/* cpu throttling */
	struct migr_cpu_throttle_ctx cpu_throttle;
};

static int process_queue(void *ctx);
static int start_file_hash(void *ctx);
static int continue_file_hash(void *ctx);


/*  _   _      _                     
 * | | | | ___| |_ __   ___ _ __ ___ 
 * | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * |  _  |  __/ | |_) |  __/ |  \__ \
 * |_| |_|\___|_| .__/ \___|_|  |___/
 *              |_|                  
 */

static int
queue_empty(struct hasher_ctx *ctx)
{
	return TAILQ_EMPTY(&ctx->queue);
}

static void
queue_remove_file(struct hasher_ctx *ctx, struct siq_hash_req_entry *entry)
{
	log(TRACE, "%s", __func__);
	TAILQ_REMOVE(&ctx->queue, entry, ENTRIES);
	free(entry->file.file);
	free(entry);
	ctx->queue_size--;
	ASSERT(ctx->queue_size >= 0);
}

static void
queue_remove_files_from(struct hasher_ctx *ctx,
    struct siq_hash_req_entry *starting_entry)
{
	struct siq_hash_req_entry *entry, *tmp_file;
	bool removing = false;

	log(TRACE, "%s", __func__);
	TAILQ_FOREACH_SAFE(entry, &ctx->queue, ENTRIES, tmp_file) {
		if (entry == starting_entry)
			removing = true;
		if (removing)
			queue_remove_file(ctx, entry);
	}
}

static void
queue_cleanup(struct hasher_ctx *ctx)
{
	struct siq_hash_req_entry *entry, *tmp_entry;
	log(TRACE, "%s", __func__);

	TAILQ_FOREACH_SAFE(entry, &ctx->queue, ENTRIES, tmp_entry) {
		queue_remove_file(ctx, entry);
	}
}

static struct siq_hash_req_entry *
queue_find_file_by_src_dkey(struct hasher_ctx *ctx, uint64_t src_dkey,
    bool exact)
{
	struct siq_hash_req_entry *entry, *tmp_entry;

	log(TRACE, "%s", __func__);
	TAILQ_FOREACH_SAFE(entry, &ctx->queue, ENTRIES, tmp_entry) {
		/* Files in queue always in src_dkey order */
		if (entry->file.src_dkey > src_dkey) {
			if (exact)
				break;
			return entry;
		}
		if (entry->file.src_dkey == src_dkey)
			return entry;
	}
	return NULL;
}

static int 
change_dir(struct hasher_ctx *ctx, struct path *path)
{
	int ret;

	log(TRACE, "%s", __func__);
	if (ctx->dir_fd != -1) {
		close(ctx->dir_fd);
		ctx->dir_fd = -1;
	}

	/* We're working with a snapshot if this is on the pworker side */
	if (g_snap_fd != -1) {
		ctx->dir_fd = fd_rel_to_ifs(g_snap_fd, path);
		if (ctx->dir_fd == -1) {
			log(ERROR, "Can't chdir to %s: %s", path->path,
			    strerror(errno));
			    return -1;
		}
	}
	else {
		ret = schdirfd(path, -1, &ctx->dir_fd);
		if (ret == -1) {
			ctx->dir_fd = -1;
			return -1;
		}
	}
	path_clean(&ctx->dir);
	path_copy(&ctx->dir, path);
	return 0;
}

static void
send_hash(struct hasher_ctx *ctx, struct full_hash_file_req_msg *file,
    uint64_t offset, uint64_t piece_len, enum hash_status status,
    char *hash_str)
{
	struct generic_msg msg = {};
	struct full_hash_file_resp_msg *hmsg =
	    &msg.body.full_hash_file_resp;

	log(DEBUG, "Sending hash of %s: %s %s\n",
	    file->file, hash_str, hash_status_str(status));

	msg.head.type = FULL_HASH_FILE_RESP_MSG;
	hmsg->file = file->file;
	hmsg->enc = file->enc;
	hmsg->src_dkey = file->src_dkey;
	hmsg->offset = offset;
	hmsg->len = piece_len;
	hmsg->hash_str = hash_str;
	hmsg->hash_type = ctx->hash_type;
	hmsg->status = status;
	hmsg->origin = ctx->origin;
	hmsg->src_dlin = ctx->src_dlin;
	msg_send(ctx->s, &msg);
}

/* Send back a full hash reply indicating that the file should be synced
 * regardless of any hashing calculations */
static void
send_do_sync(struct hasher_ctx *ctx, struct full_hash_file_req_msg *file)
{
	send_hash(ctx, file, 0, 0, HASH_SYNC, NULL);
}

static void
send_error_msg(struct hasher_ctx *ctx, enum hash_error error)
{
	struct generic_msg msg = {};
	struct hash_error_msg *hemsg =
	    &msg.body.hash_error;
	int rc;

	log(DEBUG, "Sending error message %s", hash_error_str(error));

	msg.head.type = HASH_ERROR_MSG;
	hemsg->hash_error = error;
	rc = msg_send(ctx->s, &msg);
}

static void
schedule(void *func, struct hasher_ctx *ctx)
{
	struct timeval timeout = {0, 0};
	migr_register_timeout(&timeout, func, ctx);
}


/*   ____      _ _ _                _        
 *  / ___|__ _| | | |__   __ _  ___| | _____ 
 * | |   / _` | | | '_ \ / _` |/ __| |/ / __|
 * | |__| (_| | | | |_) | (_| | (__|   <\__ \
 *  \____\__,_|_|_|_.__/ \__,_|\___|_|\_\___/
 */

static int
full_hash_dir_req_cb(struct generic_msg *m, void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct path path;
	struct enc_vec ENC_VEC_INIT_CLEAN(evec);
	struct full_hash_dir_req_msg *dir_msg;

	log(TRACE, "%s", __func__);

	/* A dir message is sent only after the previous dir
	 * is finished, which means the queue should be empty
	 * when a new dir message is received. */
	ASSERT(queue_empty(hasher));

	dir_msg = &m->body.full_hash_dir_req;
	log(TRACE, "Request to cd to %s", dir_msg->dir);
	enc_vec_copy_from_array(&evec, dir_msg->evec,
	    dir_msg->eveclen / sizeof(enc_t));
	path_init(&path, dir_msg->dir, &evec);
	hasher->src_dlin = dir_msg->src_dlin;
	if (change_dir(hasher, &path) == -1) {
		log(ERROR, "Missing path %s", path.path);
		/* pworker will assert on receipt */
		send_error_msg(hasher, HASH_ERR_MISSING_DIR);
		ASSERT(0);
	}
	path_clean(&path);

	hasher->stop_hashing = true;
	return 0;
}

static int
full_hash_file_req_cb(struct generic_msg *m, void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct full_hash_file_req_msg *file_msg;
	struct siq_hash_req_entry *ent;

	log(TRACE, "%s", __func__);

	hasher->stop_hashing = false;

	file_msg = &m->body.full_hash_file_req;
	/* We work at one directory at a time so the current directory
	 * lin should match the file's directory lin. If not, just
	 * mark the file to need syncing  */
	if (file_msg->src_dlin != hasher->src_dlin) {
		log(ERROR, "Unexpected dir lin mismatch. "
		    "Current dir %s lin=%llx. File dir_lin=%llx",
		    hasher->dir.path, hasher->src_dlin, file_msg->src_dlin);
		send_error_msg(hasher, HASH_ERR_UNEXPECTED_MSG);
		ASSERT(0);
	}
	log(DEBUG, "Request to hash %s (%s%s)", file_msg->file,
	    hasher->queue_size > 0 ?
	    "during hashing of " : "queue previously empty",
	    hasher->queue_size > 0 ?
	    TAILQ_FIRST(&hasher->queue)->file.file : "");

	ent = calloc(1, sizeof(struct siq_hash_req_entry));
	ASSERT(ent);
	ent->file.file = strdup(file_msg->file);
	ASSERT(ent->file.file);

	ent->file.enc = file_msg->enc;
	ent->file.size = file_msg->size;
	ent->file.src_dkey = file_msg->src_dkey;

	TAILQ_INSERT_TAIL(&hasher->queue, ent, ENTRIES);
	hasher->queue_size++;

	schedule(process_queue, hasher);
	return 0;
}

static int
hash_stop_cb(struct generic_msg *m, void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct siq_hash_req_entry *cur_ent = NULL;
	struct hash_stop_msg *hsmsg;
	struct siq_hash_req_entry *to_remove_entry;

	log(TRACE, "%s", __func__);

	if (hasher->queue_size > 0)
		cur_ent = TAILQ_FIRST(&hasher->queue);
	
	hasher->stop_hashing = false;

	hsmsg = &m->body.hash_stop;
	if (hasher->queue_size == 0) {
		log(DEBUG, "Received hash stop request %s for file %s "
		    "while not processing a queue (Not an error)",
		    hash_stop_str(hsmsg->stop_type), hsmsg->file);
		goto out;
	}

	if (hsmsg->src_dlin != hasher->src_dlin) {
		log(DEBUG,
		    "%s: received a hash stop for dir (%llx) "
		    "and not for current dir %s (%llx) "
		    "for file %s (not an error)",
		    __func__, hsmsg->src_dlin, hasher->dir.path,
		    hasher->src_dlin, hsmsg->file);
		goto out;
	}

	if (hsmsg->stop_type == HASH_STOP_FILE) {
		log(DEBUG, "Request to stop hashing of file %s/%s",
		    hasher->dir.path, hsmsg->file);

		ASSERT(cur_ent != NULL);
		if (hsmsg->src_dkey == cur_ent->file.src_dkey) {
			queue_remove_file(hasher, cur_ent);
			log(DEBUG, "==> file is currently being hashed");
			hasher->stop_hashing = true;
		}
		to_remove_entry = queue_find_file_by_src_dkey(hasher,
		    hsmsg->src_dkey, true);
		if (!to_remove_entry)
			log(DEBUG, "==> File not/no longer in queue");
		else
			queue_remove_file(hasher, to_remove_entry);
		goto out;
	} else if (hsmsg->stop_type == HASH_STOP_FROM) {
		log(DEBUG, "Request to stop hashing of files starting "
		    "at %s/%s",
		    hasher->dir.path, hsmsg->file);

		ASSERT(cur_ent != NULL);
		if (hsmsg->src_dkey == cur_ent->file.src_dkey)
			log(DEBUG, "==> file is currently being hashed");

		to_remove_entry = queue_find_file_by_src_dkey(hasher,
		    hsmsg->src_dkey, false);

		/* This would indicate that either the queue is empty,
		 * or the src_dkey is for a file beyond the queue (and
		 * thus not in the queue) */
		if (!to_remove_entry) {
			if (queue_empty(hasher)) {
				log(DEBUG, "==> Queue empty");
				hasher->stop_hashing = true;
				goto out;
			}
			/* The file is beyond this queue, so continue
			 * with the hashing of the current file */
			log(DEBUG, "==> Is beyond queue");
			goto out;
		}
		queue_remove_files_from(hasher, to_remove_entry);
		/* The stop request included the current file */
		if (hsmsg->src_dkey <= cur_ent->file.src_dkey) {
			hasher->stop_hashing = true;
			goto out;
		}

		/* The stop request was for beyond the current
		 * file. Keep on hashing current file. */
		goto out;
	} else if (hsmsg->stop_type == HASH_STOP_ALL) {
		log(DEBUG, "Request to stop hashing of all files in "
		    "directory %s",
		    hasher->dir.path);
		queue_cleanup(hasher);
		hasher->stop_hashing = true;
		goto out;
	} else {
		log(ERROR, "Invalid HASH_STOP type of %d",
		    hsmsg->stop_type);
		send_error_msg(hasher, HASH_ERR_UNEXPECTED_MSG);
		ASSERT(0);
	}

out:
	return 0;
}

static int
hash_quit_cb(struct generic_msg *m, void *ctx)
{
	log(INFO, "Received quit request. Exiting");
	exit(0);
}

static int
noop_cb(struct generic_msg *m, void *ctx)
{
	return 0;
}

static int
disconnect_cb(struct generic_msg *m, void *ctx)
{
	log(DEBUG, "Received disconnect callback. Exiting");
	exit(0);
}

static int
timer_cb(void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct timeval timeout = {10, 0};

	log(TRACE, "%s", __func__);

	if (hasher->cpu_throttle.host == -1)
		connect_cpu_throttle_host(&hasher->cpu_throttle,
		    (g_parent == PWORKER ? SIQ_PHASHER : SIQ_SHASHER));

	send_cpu_stat_update(&hasher->cpu_throttle);

	migr_register_timeout(&timeout, timer_cb, hasher);

	return 0;
}


/*  _   _           _     _             
 * | | | | __ _ ___| |__ (_)_ __   __ _ 
 * | |_| |/ _` / __| '_ \| | '_ \ / _` |
 * |  _  | (_| \__ \ | | | | | | | (_| |
 * |_| |_|\__,_|___/_| |_|_|_| |_|\__, |
 *                                |___/ 
 */

static int
start_file_hash(void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct siq_hash_req_entry *cur_ent = TAILQ_FIRST(&hasher->queue);
	struct full_hash_file_req_msg *file = &cur_ent->file;
	struct stat st;

	log(TRACE, "%s", __func__);

	ASSERT(hasher->hash == NULL);
	ASSERT(!hasher->file_hash_in_progress);

	hasher->file_hash_in_progress = true;
	hasher->cur_fd = -1;
	hasher->stop_hashing = false;
	hasher->total_read = 0;
	hasher->piece_so_far = 0;
	hasher->offset = 0;

	hasher->cur_fd = enc_openat(hasher->dir_fd, file->file, file->enc,
	    O_RDONLY | O_NOFOLLOW);
	if (hasher->cur_fd == -1) {
		if (errno == ENOENT) {
			log(ERROR, "File %s/%s is unexpectedly missing",
			    hasher->dir.path, file->file);
			    /* The file should never be missing.  Just tell
			     * pworker to try to sync it. */
		} else {
			log(ERROR, "Bad open of file %s/%s: %s",
			    hasher->dir.path, file->file,
			    strerror(errno));
		}
		/* Something went wrong, tell pworker just to sync the file. */
		/* XXX Maybe if this is the p_hasher, this is a major error
		 * and the file shouldn't be synced. */
		send_do_sync(hasher, file);
		goto cleanup;
	}

	if (fstat(hasher->cur_fd, &st) == -1) {
		log(ERROR, "Can't get stat of %s/%s: %s",
		    hasher->dir.path, file->file, strerror(errno));
		send_do_sync(hasher, file);
		goto cleanup;
	}

	if (!S_ISREG(st.st_mode)) {
		log(DEBUG, "%s/%s is not a regular file (is %d). Skipping",
		    hasher->dir.path, file->file, st.st_mode);
		send_hash(hasher, file, 0, 0, HASH_NO_SYNC, NULL);
		goto cleanup;
	}

	if (st.st_size != file->size) {
		log(ERROR, "File %s/%s has size of %lld, expected %lld",
		    hasher->dir.path, file->file, st.st_size, file->size);
		send_do_sync(hasher, file);
		goto cleanup;
	}

	if (hasher->min_piece_len == 0) {
		log(TRACE, "%s: hashing entire file at once", __func__);
		hasher->piece_len = 0;
	} else {
		/* Make sure piece size is a multiple of
		 * OPTIMAL_FILE_READ_SIZE */
		hasher->piece_len = ((st.st_size / hasher->pieces_per_file) /
		    OPTIMAL_FILE_READ_SIZE) * OPTIMAL_FILE_READ_SIZE;
		if (hasher->piece_len < hasher->min_piece_len) {
			log(TRACE, "%s: Divided piece len of %llu too small. "
			    "Piece len=%llu",
			    __func__, hasher->piece_len, hasher->min_piece_len);
			hasher->piece_len = hasher->min_piece_len;
		} else {
			log(TRACE, "%s: Divided piece size=%llu",
			    __func__, hasher->piece_len);
		}
	}

	hasher->hash = hash_alloc(hasher->hash_type);
	hash_init(hasher->hash);

	schedule(continue_file_hash, hasher);
	return 0;

cleanup:
	if (hasher->cur_fd != -1) {
		close(hasher->cur_fd);
		hasher->cur_fd = -1;
	}

	queue_remove_file(hasher, cur_ent);
	hasher->stop_hashing = false;
	hasher->file_hash_in_progress = false;

	schedule(process_queue, hasher);
	return 0;
}

static int
continue_file_hash(void *ctx)
{
	struct hasher_ctx *hasher = ctx;
	struct siq_hash_req_entry *cur_ent = TAILQ_FIRST(&hasher->queue);
	struct full_hash_file_req_msg *file = &cur_ent->file;
	int n_read;
	char *hash_str;
	bool eof = false, just_sent_piece = false;

	ASSERT(hasher->cur_fd > 0);
	ASSERT(hasher->file_hash_in_progress);

	if (hasher->stop_hashing)
		goto cleanup;

	n_read = read(hasher->cur_fd, hasher->buf, OPTIMAL_FILE_READ_SIZE);
	UFAIL_POINT_CODE(diff_hasher_read,
		n_read = -1;
		errno = RETURN_VALUE;);

	if (n_read == -1) {
		log(ERROR, "Failed to read from %s/%s: %s",
		    hasher->dir.path, file->file, strerror(errno));

		if (hasher->origin == SWORKER) {
			/* On target side, inform pworker to sync the file */
			send_do_sync(hasher, file);
		} else {
			/* On source side, this is an error */
			if (_g_expected_dataloss) {
				/* send NO_SYNC message to p_hasher */
				send_hash(hasher, file, 0, 0, HASH_NO_SYNC, NULL);
			}
			else {
				send_error_msg(hasher, HASH_ERR_FILE_SYSTEM);
			}
		}
		goto cleanup;
	}
	hasher->total_read += n_read;

	if (n_read == 0 || n_read < OPTIMAL_FILE_READ_SIZE)
		eof = true;

	hash_update(hasher->hash, hasher->buf, n_read);

	if (hasher->piece_len > 0) {
		hasher->piece_so_far += n_read;
		if (hasher->piece_so_far == hasher->piece_len) {
			hash_str = hash_end(hasher->hash);

			send_hash(hasher, file, hasher->offset,
			    hasher->piece_len,
			    eof ? HASH_EOF : HASH_PIECE, hash_str);
			/* Start over with hash for next piece */
			free(hash_str);
			hash_init(hasher->hash);
			just_sent_piece = true;
			hasher->offset = hasher->total_read;
			hasher->piece_so_far = 0;
		} else
			just_sent_piece = false;
	}

	if (eof) {
		/* Hashing only ends on EOF. If we didn't just send a piece
		 * (which will always be the case with piece_len == 0 == entire
		 * file), then send the final piece */
		if (!just_sent_piece) {
			hash_str = hash_end(hasher->hash);
			send_hash(hasher, file, hasher->offset,
			    hasher->piece_len == 0 ?
			        hasher->total_read : hasher->piece_so_far,
			    HASH_EOF, hash_str);
			free(hash_str);
		}
		goto cleanup;
	}

	/* Hash the next piece */
	schedule(continue_file_hash, hasher);
	return 0;

 cleanup:
 	ASSERT(hasher->hash);
	hash_free(hasher->hash);
	hasher->hash = NULL;

	if (hasher->cur_fd != -1) {
		close(hasher->cur_fd);
		hasher->cur_fd = -1;
	}

	/* Remove file from queue if we haven't already */
	if (!hasher->stop_hashing)
		queue_remove_file(hasher, cur_ent);
	hasher->stop_hashing = false;
	hasher->file_hash_in_progress = false;

	schedule(process_queue, hasher);
	return 0;
}

static int
process_queue(void *ctx)
{
	struct hasher_ctx *hasher = ctx;

	/* full_hash_file_req_cb can call process_queue while we're in the
	 * middle of hashing a file. If this happens, we don't want to start
	 * work on another item from the queue. That would be very bad... */
	if (hasher->file_hash_in_progress)
		goto out;

	if (!queue_empty(hasher)) {
		start_file_hash(hasher);
		goto out;
	}

out:
	return 0;
}

/* Return socket fd to hasher connection */
int
fork_hasher(enum origin origin, void *ctx, int snap_fd,
    enum hash_type hash_type, uint64_t min_hash_piece_len, int pieces_per_file,
    int log_level, uint16_t cpu_port)
{
	int sockfd[2];
	pid_t hasher_pid;
	char proctitle[] = "X_hasher";
	char *ilog_hasher_app_name = NULL;
	int hash_size_remainder; //XXX:EML get around a stupid assert bug
				 //because ASSERT can't take a '%' within it.

	/* Piece size must always been evenly divisible by the optimal read
	 * size. */
	hash_size_remainder = min_hash_piece_len % OPTIMAL_FILE_READ_SIZE;
	ASSERT(hash_size_remainder == 0);

	if (socketpair(AF_LOCAL, SOCK_STREAM, 0, sockfd) == -1) {
		log(ERROR, "Can't create socketpair for hasher (%s). "
		    "No hashing will occur", strerror(errno));
		return -1;
	}

	g_parent = origin;
	if (origin == PWORKER) {
		proctitle[0] = 'p';
		ilog_hasher_app_name = "siq_pworker_hasher";
		ASSERT(snap_fd != -1);
		g_snap_fd = snap_fd;
	}
	else if (origin == SWORKER) {
		proctitle[0] = 's';
		ilog_hasher_app_name = "siq_sworker_hasher";
		ASSERT(snap_fd == -1);
	}
	
	ASSERT(origin == PWORKER || origin == SWORKER);

	if ((hasher_pid = fork()) == 0) {
		/* Hasher */
		struct hasher_ctx hasher = {};

		close(sockfd[0]);
		setproctitle(proctitle);
		ASSERT(ilog_running_app_choose(ilog_hasher_app_name) != 1);

		hasher.s = sockfd[1];
		hasher.origin = origin;
		hasher.hash_type = hash_type;
		hasher.min_piece_len = min_hash_piece_len;
		hasher.pieces_per_file = pieces_per_file;
		hasher.dir_fd = -1;
		hasher.cur_fd = -1;
		hasher.hash = NULL;
		hasher.buf = malloc(OPTIMAL_FILE_READ_SIZE);
		hasher.file_hash_in_progress = false;

		log(TRACE, "%s: type=%d  min_piece_len=%llu  piece_per_file=%d",
		    __func__, hash_type, min_hash_piece_len,
		    pieces_per_file);

		/* Parent may have had timeouts scheduled */
		migr_cancel_all_timeouts();
		
		migr_rm_and_close_all_fds();

		migr_add_fd(hasher.s, &hasher, "worker");
		migr_register_callback(hasher.s, FULL_HASH_DIR_REQ_MSG,
		    full_hash_dir_req_cb);
		migr_register_callback(hasher.s, FULL_HASH_FILE_REQ_MSG,
		    full_hash_file_req_cb);
		migr_register_callback(hasher.s, HASH_STOP_MSG, hash_stop_cb);
		migr_register_callback(hasher.s, HASH_QUIT_MSG, hash_quit_cb);
		migr_register_callback(hasher.s, NOOP_MSG, noop_cb);
		migr_register_callback(hasher.s, DISCON_MSG, disconnect_cb);

		TAILQ_INIT(&hasher.queue);

		cpu_throttle_ctx_init(&hasher.cpu_throttle,
		    (origin == PWORKER) ? SIQ_WT_P_HASHER : SIQ_WT_S_HASHER,
		    cpu_port);
		timer_cb(&hasher);
		cpu_throttle_start(&hasher.cpu_throttle);

		migr_process();
		exit(0);
	}
	ASSERT(hasher_pid != -1);

	/* Parent */
	log(DEBUG, "forked off %s %d", proctitle, hasher_pid);
	close(sockfd[1]);
	migr_add_fd(sockfd[0], ctx, proctitle);
	return sockfd[0];
}

char *
hash_status_str(enum hash_status status)
{
	static char buf[30];

	switch (status) {
	case HASH_PIECE:
		return "HASH_PIECE";
	case HASH_EOF:
		return "HASH_EOF";
	case HASH_SYNC:
		return "HASH_SYNC";
	case HASH_NO_SYNC:
		return "HASH_NO_SYNC";
	default:
		snprintf(buf, sizeof(buf), "Unknown status %d", status);
		return buf;
	}
}

char *
hash_error_str(enum hash_error err)
{
	static char buf[30];
	switch (err) {
	case HASH_ERR_FILE_SYSTEM:
		return "HASH_ERR_FILE_SYSTEM";
	case HASH_ERR_MISSING_DIR:
		return "HASH_ERR_MISSING_DIR";
	case HASH_ERR_UNEXPECTED_MSG:
		return "HASH_ERR_UNEXPECTED_MSG";
	default:
		snprintf(buf, sizeof(buf), "Unknown error %d", err);
		return buf;
	}
}

char *
origin_str(int origin)
{
	static char buf[30];
	
	switch (origin) {
	case PWORKER:
		return "pworker";
	case SWORKER:
		return "sworker";
	case PWORKER_AND_SWORKER:
		return "pworker & sworker";
	default:
		snprintf(buf, sizeof(buf), "Unknown origin %d", origin);
		return buf;
	}
}

char *
sync_state_str(enum sync_state state)
{
	static char buf[30];

	switch (state) {
	case UNDECIDED:
		return "UNDECIDED";
	case NEEDS_SYNC:
		return "NEEDS_SYNC";
	case NO_SYNC:
		return "NO_SYNC";
	default:
		snprintf(buf, sizeof(buf), "Unknown sync state %d", state);
		return buf;
	}
}

char *hash_stop_str(enum hash_stop stop)
{
	static char buf[30];

	switch (stop) {
	case HASH_STOP_ALL:
		return "HASH_STOP_ALL";
	case HASH_STOP_FILE:
		return "HASH_STOP_FILE";
	case HASH_STOP_FROM:
		return "HASH_STOP_FROM";
	default:
		snprintf(buf, sizeof(buf), "Unknown hash stop %d", stop);
		return buf;
	}
}
