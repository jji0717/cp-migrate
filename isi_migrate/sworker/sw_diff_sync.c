#include <stdio.h>
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/alg.h"
#include "sworker.h"

/*
 * Messages from pworker to forward to hasher: For HASH_STOP_MSG and
 * HASH_QUIT_MSG */
int
forward_to_hasher_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;

	log(DEBUG, "%s: %s", __func__, msg_type_str(m->head.type));
	/* Just forward to hasher */
	msg_send(sw_ctx->global.hasher, m);
	return 0;
}

/*
 * Messages from hasher to forward to pworker: FULL_HASH_FILE_RESP_MSG and,
 * HASH_ERROR */
int
forward_to_pworker_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;

	log(DEBUG, "%s: %s", __func__, msg_type_str(m->head.type));

	/* Just forward to primary */
	msg_send(sw_ctx->global.primary, m);
	return 0;
}

void
send_short_hash_resp(
	enum sync_state sync_state,
	uint64_t src_dkey,
	struct migr_sworker_ctx *sw_ctx)
{
	struct generic_msg msg = {};

	/* Send the short hash response to the pworker (which will tell the
	 * p_hasher to start doing a full hash for files that are
	 * UNDECIDED)  */
	msg.head.type = SHORT_HASH_RESP_MSG;
	msg.body.short_hash_resp.sync_state = sync_state;
	msg.body.short_hash_resp.src_dkey = src_dkey;
	msg_send(sw_ctx->global.primary, &msg);
}

static struct file_state *
find_fstate(
    char *name,
    enc_t enc,
    uint64_t dirlin,
    uint64_t filelin,
    struct migr_sworker_ctx *sw_ctx)
{
	struct file_state *fstate;

	TAILQ_FOREACH(fstate, sw_ctx->inprog.fstates, ENTRIES) {
		if (fstate->src_dirlin == dirlin &&
		    fstate->src_filelin == filelin &&
		    strcmp(fstate->name, name) == 0 &&
		    fstate->enc == enc)
			return fstate;
	}
	return NULL;
}

struct file_state *
ds_file_done(struct file_done_msg *fdone, struct migr_sworker_ctx *sw_ctx)
{
	struct file_state *fstate;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;

	fstate = find_fstate(fdone->fname, fdone->fenc,
	    fdone->src_dlin, fdone->src_lin, sw_ctx);
	ASSERT(fstate);

	ASSERT(fdone->sync_state == NEEDS_SYNC ||
	    fdone->sync_state == NO_SYNC);

	/* If the FILE_DONE_MSG says a sync needs to be done, but the file is
	 * currently in the UNDECIDED state, that means that there was no
	 * FILE_DATA_MSG sent to trigger creating the new file for the sync
	 * and this must be an empty file. This should never happen as an
	 * empty file should have been marked NEEDS_SYNC in the
	 * FILE_BEGIN_MSG, but it's here just in case ...*/
	if (fdone->sync_state == NEEDS_SYNC &&
	    fstate->sync_state == UNDECIDED) {
		ASSERT(fstate->stat_state.size == 0);
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY,
		    "NEEDS_SYNC: %s/%s (empty file)",
		    worker->cur_path.path, fstate->name);
	}
	else if (fdone->sync_state == NO_SYNC) {
		/* If the FILE_DONE_MSG says a sync doesn't need to be
		 * done, then the file state must have been
		 * previously UNDECIDED */
		ASSERT(fstate->sync_state == UNDECIDED);
		set_sync_state(fstate, NO_SYNC);
		log(COPY,
		    "NO_SYNC: %s/%s (pworker request)",
		    worker->cur_path.path, fstate->name);
		/* Even though the file is NO_SYNC, we still will take care of
		 * making sure the file attributes are synced.
		 */
	}
	return fstate;
}

/* If doing diff_sync, the pworker needs to know if the directory
 * already exists, or if it's a new directory (in which case there's
 * no need to doing any file hash matching) */
void
ds_dir4(struct dir_msg4 *msg4, bool is_new_dir, struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct generic_msg resp = {};
	struct ack_dir_msg *ack = &resp.body.ack_dir;
	struct full_hash_dir_req_msg *hash_dir =
	    &resp.body.full_hash_dir_req;

	if (msg4->need_ack_dir) {
		/* Let the pworker know if the directory exists on the target
		 * or not. */
		resp.head.type = ACK_DIR_MSG;
		ack->dir = msg4->dir;
		ack->eveclen = msg4->eveclen;
		ack->evec = msg4->evec;
		ack->is_new_dir = is_new_dir;
		msg_send(global->primary, &resp);
	}

	/* Tell the s_hasher to change directories, unless it's a new
	 * dir (where there's no hashing to do)  */
	if (!is_new_dir) {
		bzero(&resp, sizeof(struct generic_msg));
		resp.head.type = FULL_HASH_DIR_REQ_MSG;
		hash_dir->eveclen = sizeof(enc_t) *
		    enc_vec_size(&worker->cur_path.evec);
		hash_dir->evec = worker->cur_path.evec.encs;
		hash_dir->dir = worker->cur_path.path;
		hash_dir->src_dlin = msg4->dir_lin;
		msg_send(global->hasher, &resp);
	}
}

/**
 * In all cases, this function is required to return an fstate.
 * If an error occurs, the fstate is returned in the NEEDS_SYNC
 * case to let us use the normal file error paths.  This isn't
 * optimal.
 */
struct file_state *
ds_file_begin(struct file_begin_msg *begin, bool file_exists, int nlinks,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct file_state *fstate;
	struct isi_error *error = NULL;
	struct stat st = {};

	log(TRACE, "%s", __func__);

	ASSERT(!file_exists || error_out);
	/* For each FILE_BEGIN_MSG we will add an entry to the fstates
	 * list */
	fstate = calloc(1, sizeof(struct file_state));
	ASSERT(fstate);

	/* Init value of fd */
	fstate->fd = -1;

	fstate->name = strdup(begin->fname);
	ASSERT(fstate->name);

	if (file_exists) {
		if (enc_fstatat(worker->cwd_fd, begin->fname, begin->fenc, &st,
		    AT_SYMLINK_NOFOLLOW) != 0) {
			error = isi_system_error_new(errno,
			    "Failed to stat file %s/%s", worker->cur_path.path,
			    begin->fname);
			goto out;
		}
	}

	/* pworker says it needs to be synced (likely because the file
	 * size is smaller than JUST_SEND_THE_FILE_SIZE. There are no
	 * short hashes and the pworker is not expecting a
	 * SHORT_HASH_RESP_MSG in this case */
	if (begin->sync_state == NEEDS_SYNC) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (pworker request)",
		    worker->cur_path.path, begin->fname);
	}
	/* File doesn't exist here, obviously it needs to be synced */
	else if (!file_exists) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (not on target)",
		    worker->cur_path.path, begin->fname);
		/* Immediately tell pworker that the file is NEEDS_SYNC */
		send_short_hash_resp(fstate->sync_state,
		    begin->src_dkey, sw_ctx);
	}
	/* Also need to force sync if there are multiple hardlinks */
	else if (nlinks > 1) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (nlinks > 1)",
		    worker->cur_path.path, begin->fname);
		/* Immediately tell pworker that the file is NEEDS_SYNC */
		send_short_hash_resp(fstate->sync_state,
		    begin->src_dkey, sw_ctx);
	}
	/* File type on target doesn't match type in file begin message */
	else if (begin->file_type != stat_mode_to_file_type(st.st_mode)) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (file type mismatch)",
		    worker->cur_path.path, begin->fname);
		send_short_hash_resp(fstate->sync_state,
		    begin->src_dkey, sw_ctx);
	}
	else if (st.st_flags & SF_FILE_STUBBED) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (stub on target)",
		    worker->cur_path.path, begin->fname);
		send_short_hash_resp(fstate->sync_state,
		    begin->src_dkey, sw_ctx);
	}
	/* Compare short hashes and initiate full hash if needed (will open
	 * fd) */
	else {
		ASSERT(begin->sync_state == UNDECIDED);
		diff_sync_short_hash(begin, fstate, sw_ctx, &error);
		if (error) {
			log(COPY, "NEEDS_SYNC: %s/%s (due to error)",
			    worker->cur_path.path, begin->fname);
			set_sync_state(fstate, NEEDS_SYNC);
			send_short_hash_resp(fstate->sync_state,
			    begin->src_dkey, sw_ctx);
		}
	}

out:
	isi_error_handle(error, error_out);
	return fstate;
}

struct file_state *
ds_file_data(
    struct file_data_msg *fdata,
    struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct file_state *fstate;
	struct isi_error *error = NULL;

	fstate = find_fstate(fdata->fname, fdata->fenc,
	    fdata->src_dlin, fdata->src_lin, sw_ctx);
	ASSERT(fstate);

	log(TRACE, "%s: %s = %s", __func__, fstate->name,
	    sync_state_str(fstate->sync_state));

	/* While doing diff sync, Receiving a FILE_DATA_MSG on a file that's
	 * UNDECIDED means either that full hashing has determined that it
	 * needs to be synced, or it can mean that the file size is less than
	 * JUST_SEND_THE_FILE_SIZE and no hashing has been done at all */
	if (fstate->sync_state == UNDECIDED) {
		log(COPY,
		    "NEEDS_SYNC: %s/%s (full hash mismatch or "
		    "file len < min size for hashing)",
		    worker->cur_path.path, fstate->name);

		set_sync_state(fstate, NEEDS_SYNC);
		
		/* We absolutely know that this is a regular file as in
		 * diff_sync_short_hash(), any non-regular file is immediately
		 * marked as NEEDS_SYNC */
		ASSERT(fstate->orig_is_reg_file);

		/* If a file was UNDECIDED, it must have been opened during
		 * the short hash comparison for the FILE_BEGIN_MSG */
		ASSERT(fstate->fd != -1);

		/* Just truncate the old file, so new data can be put into it.
		 * If this file is an ADS container, it has already received
		 * all its ADS files as they are sent immediately after the
		 * FILE_BEGIN_MSG. */
		if (ftruncate(fstate->fd, 0) == -1) {
			error = isi_system_error_new(errno,
			    "Failed to truncate file %s/%s",
			    worker->cur_path.path, fstate->name);
			goto out;
		}
	}
 out:
	isi_error_handle(error, error_out);
	return fstate;
}

/**
 * If error is returned, begin->sync_state will not be modified.
 */
void
diff_sync_short_hash(
	struct file_begin_msg *begin,
	struct file_state *fstate,
	struct migr_sworker_ctx *sw_ctx,
	struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct generic_msg msg = {};
	struct full_hash_file_req_msg *fhreq = &msg.body.full_hash_file_req;
	struct short_hash_req_info *shash_info;
	int i, len, hash_buf_size = 0, n_read, rc;
	char *hash_buf = NULL, *hash_str = NULL;
	struct hash_ctx *hash_ctx = NULL;
	struct stat st;
	struct isi_error *error = NULL;
	int hash_remainder;//XXX fix for a stupid assert compilation issue
	
	ASSERT(fstate->fd == -1);
	ASSERT(begin->sync_state != NEEDS_SYNC);
	
	fstate->fd = enc_openat(worker->cwd_fd,
	    begin->fname, begin->fenc, O_RDWR | O_NOFOLLOW);
	ASSERT(fstate->fd != 0);
	if (fstate->fd == -1) {
		if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Failed to open file %s/%s",
			    worker->cur_path.path, begin->fname);
			goto cleanup;
		}
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (not on target)",
		    worker->cur_path.path, begin->fname);
		goto send_resp;
	}

	/*
	 * Paranoia test to make sure we opened a normal file
	 * rather than a device someone raced into place.
	 */
	rc = fstat(fstate->fd, &st);
	if (rc == -1 || !S_ISREG(st.st_mode)) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (not regular file [%d])",
		    worker->cur_path.path, begin->fname, st.st_mode);
		goto send_resp;
	}
	
	/* No need to calculate short hashes if file size mismatch */
	if (begin->size != st.st_size) {
		set_sync_state(fstate, NEEDS_SYNC);
		log(COPY, "NEEDS_SYNC: %s/%s (file size mismatch)",
		    worker->cur_path.path, begin->fname);
		goto send_resp;
	}

	/* Start short hash match tests */

	/* The begin->short_hashes_len must be a multiple of sizeof(struct
	 * short_hash_req_info) */
	hash_remainder = begin->short_hashes_len %
	    sizeof(struct short_hash_req_info);
	ASSERT(hash_remainder == 0);
	
	len = begin->short_hashes_len / sizeof(struct short_hash_req_info);
	for (i = 0; i < len;  i++) {
		shash_info = (struct short_hash_req_info *)
		    (begin->short_hashes +
			i * sizeof(struct short_hash_req_info));

		if (shash_info->hash_len > hash_buf_size) {
			hash_buf_size = shash_info->hash_len;
			free(hash_buf);
			hash_buf = calloc(hash_buf_size, sizeof(char));
			ASSERT(hash_buf);
		}

		log(DEBUG, "%s: hash_buf_size=%d",
		    __func__, hash_buf_size);

		rc = lseek(fstate->fd, shash_info->hash_offset, SEEK_SET);
		if (rc == -1) {
			error = isi_system_error_new(errno,
			    "Error lseeking into %s/%s",
			    worker->cur_path.path, begin->fname);
			goto cleanup;
		}
		n_read = read(fstate->fd, hash_buf, shash_info->hash_len);
		if (n_read != shash_info->hash_len) {
			log(COPY,
			    "NEEDS_SYNC: %s/%s (hash read size mismatch "
			    "Read=%d. Expected=%llu)",
			    worker->cur_path.path, begin->fname,
			    n_read, shash_info->hash_len);
			set_sync_state(fstate, NEEDS_SYNC);
			break;
		}
		hash_ctx = hash_alloc(global->hash_type);
		hash_init(hash_ctx);
		hash_update(hash_ctx, hash_buf, n_read);
		hash_str = hash_end(hash_ctx);

		log(DEBUG, "%s: received hash:  %s",
		    __func__, shash_info->hash_str);
		log(DEBUG, "%s: local hash:     %s",
		    __func__, hash_str);

		if (strcmp(shash_info->hash_str, hash_str) != 0) {
			log(COPY,
			    "NEEDS_SYNC: %s/%s (short hashes mismatch)",
			    worker->cur_path.path, begin->fname);
			set_sync_state(fstate, NEEDS_SYNC);
		}
		else
			log(DEBUG, "%s: hash match", __func__);

		free(hash_str);
		hash_free(hash_ctx);
	}

 send_resp:
	send_short_hash_resp(fstate->sync_state, begin->src_dkey, sw_ctx);

	/* If sync_state == UNDECIDED, tell the s_hasher to start the full
	 * hash */
	if (fstate->sync_state == UNDECIDED) {
		bzero(&msg, sizeof(struct generic_msg));
		msg.head.type = FULL_HASH_FILE_REQ_MSG;
		fhreq->src_dlin = begin->src_dlin;
		fhreq->src_dkey = begin->src_dkey;
		fhreq->size = begin->size;
		fhreq->file = begin->fname;
		fhreq->enc = begin->fenc;
		msg_send(global->hasher, &msg);
	}

 cleanup:	
	free(hash_buf);
	isi_error_handle(error, error_out);
}
