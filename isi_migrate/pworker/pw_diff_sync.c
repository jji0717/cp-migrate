#include <stdio.h>
#include "isi_migrate/migr/treewalk.h"
#include "pworker.h"
#define PW_DS_CONFIG
#include "pw_diff_sync.h"

static enum origin
other_worker(enum origin origin)
{
	if (origin == PWORKER)
		return SWORKER;
	else if (origin == SWORKER)
		return PWORKER;
	ASSERT(0);
}

struct cache_entry *
cache_find_by_dkey(struct migr_pworker_ctx *pw_ctx, uint64_t dkey)
{
	struct cache_entry *ent;
	ent = TAILQ_FIRST(&pw_ctx->cache);
	while (ent) {
		if (ent->md->cookie == dkey)
			return ent;
		ent = TAILQ_NEXT(ent, ENTRIES);
	}
	return NULL;
}
	
bool
cache_has_msgs_pending(struct migr_pworker_ctx *pw_ctx, bool wait_for_ack)
{
	struct cache_entry *entry;
	int ar = 0, rts = 0, wfa = 0;

	/* If the only cache states left in the cache are READY_TO_SEND or
	 * ACK_RECEIVED there are no messages pending from the sworker to the
	 * pworker; the sworker is waiting for pworker to give it a file to
	 * work on. */
	TAILQ_FOREACH(entry, &pw_ctx->cache, ENTRIES) {
		if (entry->cache_state == READY_TO_SEND)
			rts++;
		else if (entry->cache_state == ACK_RECEIVED)
			ar++;
		else if (!wait_for_ack && entry->cache_state == WAITING_FOR_ACK)
			wfa++;
		else
			return true;
	}
	/* If the cache has only ACK_RECEIVED, all files should have been
	 * checkpointed by now and removed from the cache */
	ASSERT(!(rts == 0 && wfa == 0 && ar != 0));
	return false;
}

void
make_sure_cache_is_processed(struct migr_pworker_ctx *pw_ctx)
{
	/* If cache is empty, or we're doing ADS processing, no need to force
	 * processing of cache */
	if (TAILQ_EMPTY(&pw_ctx->cache) || pw_ctx->ads_oldpwd != -1)
	    return;

	/* If no cache entries are expecting messages from the sworker,
	 * processing would stall if migr_continue_reg_dir() isn't the next
	 * call which will step through the cache and find any entries that
	 * need to be sent.
	 * Only call migr_continue_reg_dir if delete_dir hasn't already
	 * been set in a prior migr_continue_reg_dir call for this
	 * directory. */
	if (!cache_has_msgs_pending(pw_ctx, false) &&
	    pw_ctx->delete_dir == NULL) {
		log(TRACE,
		    "%s: Files left in cache need to be forced to process",
		    __func__);
		migr_call_next(pw_ctx, migr_continue_reg_dir);
		migr_reschedule_work(pw_ctx);
	}
}


void
hasher_stop(struct migr_pworker_ctx *pw_ctx,
    int to_stop,
    enum hash_stop type,
    struct full_hash_file_resp_msg *resp)
{
	struct generic_msg msg = {};
	
	log(TRACE, "%s", __func__);
	
	msg.head.type = HASH_STOP_MSG;
	msg.body.hash_stop.stop_type = type;
	msg.body.hash_stop.file = resp->file;	
	msg.body.hash_stop.enc = resp->enc;
	msg.body.hash_stop.src_dkey = resp->src_dkey;
	msg.body.hash_stop.src_dlin = pw_ctx->dir_lin;
	
	log(DEBUG, "%s: Request for %s hasher stop for file %s/%s",
	    __func__,  origin_str(to_stop), pw_ctx->cwd.path, resp->file);

	if (to_stop == PWORKER ||
	    to_stop == PWORKER_AND_SWORKER)
		msg_send(pw_ctx->hasher, &msg);
	if (to_stop == SWORKER ||
	    to_stop == PWORKER_AND_SWORKER)
		msg_send(pw_ctx->sworker, &msg);
}

static void
hash_pieces_cleanup(struct hash_pieces *pieces)
{
	struct hash_piece *piece, *junk;

	TAILQ_FOREACH_SAFE(piece, pieces, ENTRIES, junk) {
		free(piece->hash_str);
		free(piece);
	}
}

int
full_hash_resp_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct full_hash_file_resp_msg *resp = &m->body.full_hash_file_resp;
	struct hash_piece *this_piece, *other_piece;
	struct cache_entry *cache_ent;
	bool this_eof = false, other_eof = false;
	struct hash_pieces *this_hash_head, *other_hash_head;
	enum origin origin = resp->origin;
	int *this_hash_size_p, *other_hash_size_p;

	log(TRACE, "%s: from %s for %s", __func__, origin_str(origin), 
	    resp->file);
	ASSERT(resp->hash_type == pw_ctx->hash_type);

	/* This may happen on occasion. Not an error as it's likely that the
	 * hasher was just slow in getting back the message and we've moved on
	 * from the original directory */
	if (resp->src_dlin != pw_ctx->dir_lin) {
		log(DEBUG, "%s: received a hash file response dir (%llx) "
		    "for not current dir (%llx)",
		    __func__, resp->src_dlin, pw_ctx->dir_lin);
		return 0;
	}

	cache_ent = cache_find_by_dkey(pw_ctx, resp->src_dkey);
	if (!cache_ent) {
		/* This can happen if the hashing has been canceled */
		log(DEBUG, "Received full hash for uncached file %s/%s (%llu)",
		    pw_ctx->cwd.path, resp->file, resp->src_dkey);
		/* XXXX DNR Is this really necessary? Shouldn't both have
		 * already been sent a stop? */
		hasher_stop(pw_ctx, PWORKER_AND_SWORKER, HASH_STOP_FILE, resp);
		return 0;
	}
	
	if (cache_ent->cache_state != WAITING_FOR_FULL_HASH) {
		/* This is OK as one cluster can be slower than the other in
		 * sending hash pieces and we might have received enough hash
		 * pieces to determine that this is a NEEDS_SYNC */

		/* XXX DNR: ASSERT here if not in NEEDS_SYNC state?*/
		log(TRACE, "%s: received full hash piece for entry %s "
		    "no longer needing it. Current cache state = %s",
		    __func__, FNAME(cache_ent->md),
		    cache_state_str(cache_ent->cache_state));
		return 0;
	}

	if (resp->status == HASH_SYNC || resp->status == HASH_NO_SYNC) {
		cache_ent->sync_state = (resp->status == HASH_SYNC) ?
		    NEEDS_SYNC : NO_SYNC;

		log(COPY, "%s: %s/%s (hasher file issue)",
		    sync_state_str(cache_ent->sync_state), pw_ctx->cwd.path,
		    resp->file);
		/* This hasher has already stopped hashing the file. Tell the
		 * other one. */
		hasher_stop(pw_ctx, other_worker(origin), HASH_STOP_FILE, resp);
		if (pw_ctx->expected_dataloss)
			log_corrupt_file_entry (pw_ctx, resp->file);

		goto out;
	}

	this_piece = calloc(1, sizeof(struct hash_piece));
	this_piece->offset = resp->offset;
	this_piece->len = resp->len;
	this_piece->hash_str = strdup(resp->hash_str);
	if (resp->status == HASH_EOF)
		this_piece->eof = true;

	if (origin == PWORKER) {
		this_hash_head = cache_ent->pw_hash_head;
		other_hash_head = cache_ent->sw_hash_head;
		this_hash_size_p = &cache_ent->pw_hash_size;
		other_hash_size_p = &cache_ent->sw_hash_size;
		other_eof = cache_ent->sw_eof;
		if (resp->status == HASH_EOF)
			this_eof = cache_ent->pw_eof = true;
	}
	else if (origin == SWORKER) {
		this_hash_head = cache_ent->sw_hash_head;
		other_hash_head = cache_ent->pw_hash_head;
		this_hash_size_p = &cache_ent->sw_hash_size;
		other_hash_size_p = &cache_ent->pw_hash_size;
		other_eof = cache_ent->pw_eof;
		if (resp->status == HASH_EOF)
			this_eof = cache_ent->sw_eof = true;
	}
	else
		ASSERT(0);

	log(TRACE, "%s: hash pieces: pworker=%d, sworker=%d",
	    __func__, cache_ent->pw_hash_size, cache_ent->sw_hash_size);

	/* Since matching hash pieces in the list are removed, there must
	 * always be at least one hash list that is empty */
	ASSERT(TAILQ_EMPTY(this_hash_head) || TAILQ_EMPTY(other_hash_head));

	/* We can't do any hash string compares if this hash isn't empty
	 * (meaning the other is empty) or if both hash lists are empty
	 * (meaning either it's the first time an entry was made into the
	 * lists, or all the hash pieces have matched so far). In this case,
	 * just add the new piece to this list. */
	if (!TAILQ_EMPTY(this_hash_head) ||
	    (TAILQ_EMPTY(this_hash_head) && TAILQ_EMPTY(other_hash_head))) {
		TAILQ_INSERT_TAIL(this_hash_head, this_piece, ENTRIES);
		(*this_hash_size_p)++;
		log(DEBUG, "%s: Added %s hash piece #%d of %s/%s (%s)",
			    __func__, origin_str(origin), *this_hash_size_p,
		    pw_ctx->cwd.path, resp->file,
		    hash_status_str(resp->status));
	}

	/* This list is empty and the other hash list isn't empty. Compare our
	 * new piece to the first piece in the other hash list */
	else {
		other_piece = TAILQ_FIRST(other_hash_head);

		/* Offset should always match! Length may not if for
		 * some reason the file got truncated on the target  */
		ASSERT(other_piece->offset == this_piece->offset);

		log(DEBUG, "%s:  this offset=%llu  len=%llu  eof=%d  hash=%s",
		    __func__, this_piece->offset, this_piece->len,
		    this_piece->eof, this_piece->hash_str);

		log(DEBUG, "%s: other offset=%llu  len=%llu  eof=%d  hash=%s",
		    __func__, other_piece->offset, other_piece->len,
		    other_piece->eof, other_piece->hash_str);

		/* We need to sync the file if either the hash strings
		 * mismatch or if one hash piece is at EOF at the other
		 * isn't */
		if (this_piece->eof != other_piece->eof ||
		    strcmp(this_piece->hash_str, other_piece->hash_str) != 0) {
			log(DEBUG, "%s: %s hash mismatch for %s/%s (%s)",
			    __func__, origin_str(origin),
			    pw_ctx->cwd.path, resp->file,
			    hash_status_str(resp->status));
			log(COPY, "NEEDS_SYNC: %s/%s (hash mismatch on %s)",
			    pw_ctx->cwd.path, resp->file, origin_str(origin));
			cache_ent->sync_state = NEEDS_SYNC;
			/* XXX We could check to see if p_hasher or s_hasher
			 * hasn't already sent the EOF piece, and send the
			 * stop message only to that one */
			hasher_stop(pw_ctx, PWORKER_AND_SWORKER,
			    HASH_STOP_FILE, resp);
		}

		/* The hash strings match. No need to add this new piece to
		 * the hash list and no need to keep the first entry in the
		 * other hash list. */
		else {
			log(DEBUG, "%s: %s hash match for %s/%s (%s)",__func__,
			    origin_str(origin), pw_ctx->cwd.path, resp->file,
			    hash_status_str(resp->status));
			/* If both p_hasher and s_hasher have hit EOF, then
			 * the files match and no sync is needed */
			if (this_piece->eof && other_piece->eof) {
				cache_ent->sync_state = NO_SYNC;
				log(COPY, "NO_SYNC: %s/%s (files match)",
				    pw_ctx->cwd.path, resp->file);
			}
			/* Since this piece was never added to this_hash_head,
			 * only remove the piece on the other hash */
			TAILQ_REMOVE(other_hash_head, other_piece, ENTRIES);
			(*other_hash_size_p)--;
			ASSERT(*other_hash_size_p >= 0);
			free(other_piece->hash_str);
			free(other_piece);
		}
		free(this_piece->hash_str);
		free(this_piece);
	}

out:
	/* Full hash has resulted in NEEDS_SYNC or NO_SYNC */
	if (cache_ent->sync_state != UNDECIDED) {
		pw_ctx->outstanding_full_hashes--;
		ASSERT(pw_ctx->outstanding_full_hashes >= 0);
		set_cache_state(cache_ent, READY_TO_SEND);
		log(TRACE, "%s: outstanding full hashes = %d", __func__,
		    pw_ctx->outstanding_full_hashes);
		log_cache_state(pw_ctx);
		make_sure_cache_is_processed(pw_ctx);
	}

	return 0;
}

void
hasher_chdir(struct migr_pworker_ctx *pw_ctx)
{
	struct generic_msg msg = {};
	struct full_hash_dir_req_msg *hash_dir = &msg.body.full_hash_dir_req;

	log(TRACE, "%s", __func__);
	msg.head.type = FULL_HASH_DIR_REQ_MSG;
	hash_dir->eveclen = sizeof(enc_t) *
	    enc_vec_size(&pw_ctx->cwd.evec);
	hash_dir->evec = pw_ctx->cwd.evec.encs;
	hash_dir->dir = pw_ctx->cwd.path;
	hash_dir->src_dlin = pw_ctx->dir_lin;
	msg_send(pw_ctx->hasher, &msg);
}

int
ack_dir_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct ack_dir_msg *ack = &m->body.ack_dir;

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->doing_diff_sync);
	ASSERT(migr_is_waiting_for(pw_ctx, MIGR_WAIT_ACK_DIR));

	pw_ctx->cwd_is_new_on_sworker = ack->is_new_dir;
	log(DEBUG, "%s: new dir = %d", __func__, ack->is_new_dir);
	pw_ctx->dir_state.dir->is_new_dir = ack->is_new_dir;

	/* No hashing necessary if it's a new directory on the target; all
	 * files in the directory will be NEEDS_SYNC. If it isn't a new
	 * directory on the target, then the p_hasher may have files to hash
	 * in the directory */
	if (!ack->is_new_dir)
		hasher_chdir(pw_ctx);

	/* Now start processing the directory */
	migr_call_next(pw_ctx, migr_start_ads_or_reg_dir);
	migr_reschedule_work(pw_ctx);
	return 0;
}

static void
send_full_hash_file_req(
	struct migr_pworker_ctx *pw_ctx,
	struct cache_entry *cache_ent)
{
	struct generic_msg msg = {};
	struct full_hash_file_req_msg *fhreq = &msg.body.full_hash_file_req;

	log(TRACE, "%s", __func__);
	msg.head.type = FULL_HASH_FILE_REQ_MSG;


	fhreq->size = cache_ent->fstate.st.st_size;
	fhreq->file = FNAME(cache_ent->md);
	fhreq->enc = cache_ent->md->dirent->d_encoding;
	fhreq->src_dkey = cache_ent->md->cookie;
	fhreq->src_dlin = pw_ctx->dir_lin;

	msg_send(pw_ctx->hasher, &msg);
}

int
short_hash_resp_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct short_hash_resp_msg *shresp = &m->body.short_hash_resp;
	struct cache_entry *cache_ent;
	
	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->waiting_for_short_hash_resp);

	pw_ctx->waiting_for_short_hash_resp = false;

	/* The short hash request (in the FILE_BEGIN_MSG) is sent immediately
	 * after adding the entry to the cache */
	cache_ent = pw_ctx->latest_cache_ent;

	ASSERT(cache_ent->cache_state == WAITING_FOR_SHORT_HASH);
	ASSERT(cache_ent->md->cookie == shresp->src_dkey);

	/* Any file in the short hash response must currently be in
	 * the UNDECIDED state in the cache */
	ASSERT(cache_ent->sync_state == UNDECIDED);

	cache_ent->sync_state = shresp->sync_state;
	log(TRACE, "%s: %s/%s marked as %s", __func__,
	    pw_ctx->cwd.path, FNAME(cache_ent->md), 
	    sync_state_str(shresp->sync_state));
	
	/* Any file where the short hashes matched is still in the UNDECIDED
	 * state, and needs to get a full hash */
	if (shresp->sync_state == UNDECIDED) {
		set_cache_state(cache_ent, WAITING_FOR_FULL_HASH);
		pw_ctx->outstanding_full_hashes++;
		send_full_hash_file_req(pw_ctx, cache_ent);
	}
	else {
		log(COPY, "NEEDS_SYNC: %s/%s (sworker request)",
		    pw_ctx->cwd.path, FNAME(cache_ent->md));
		set_cache_state(cache_ent, READY_TO_SEND);
		log_cache_state(pw_ctx);
		make_sure_cache_is_processed(pw_ctx);
	}
	return 0;
}

static void
short_hash(struct migr_pworker_ctx *pw_ctx, struct cache_entry *cache_ent,
    struct short_hash_req_info *shash, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct hash_ctx *hash_ctx = NULL;
	char short_hash_buf[pw_ctx->short_hash_len], *hash_str = NULL;
	int n_read, ret;
	
	hash_ctx = hash_alloc(pw_ctx->hash_type);
	hash_init(hash_ctx);
	ret = lseek(cache_ent->fd, shash->hash_offset, SEEK_SET);
	if (ret == -1) {
		error = isi_system_error_new(errno, 
			"Failed to lseek file %s for hashing",
			FNAME(cache_ent->md));
		goto out;
	}
	n_read = read(cache_ent->fd, short_hash_buf, sizeof(short_hash_buf));
	if (n_read == -1) {
		error = isi_system_error_new(errno,
		    "Failed to read file %s for hashing ", 
		    FNAME(cache_ent->md));
		goto out;
	}
	shash->hash_len = n_read;
	hash_update(hash_ctx, short_hash_buf, n_read);
	hash_str = hash_end(hash_ctx);
	log(DEBUG, "Short hash='%s'", hash_str);
	strncpy(shash->hash_str, hash_str, sizeof(shash->hash_str) - 1);
	shash->hash_str[sizeof(shash->hash_str) - 1] = 0;
	free(hash_str);

out:
	hash_free(hash_ctx);
	isi_error_handle(error, error_out);

}

static const char *
get_needs_sync_reason(struct migr_pworker_ctx *pw_ctx, struct stat *st)
{
	if (pw_ctx->cwd_is_new_on_sworker)
		return "new dir on target";
	if (st->st_flags & SF_FILE_STUBBED)
		return "stub file";
	if (S_ISLNK(st->st_mode))
		return "symlink";
	if (st->st_size < pw_ctx->just_send_the_file_len)
		return "file size";

	return "unknown reason";
}

void
add_file_for_diff_sync(struct migr_pworker_ctx *pw_ctx, struct migr_dirent *md,
    struct stat *st, int fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cache_entry *cache_ent;

	cache_ent = calloc(1, sizeof(struct cache_entry));
	ASSERT(cache_ent);
	TAILQ_INSERT_TAIL(&pw_ctx->cache, cache_ent, ENTRIES);
	pw_ctx->latest_cache_ent = cache_ent;
	pw_ctx->cache_size++;
	cache_ent->pw_hash_head = calloc(1, sizeof(struct hash_pieces));
	ASSERT(cache_ent->pw_hash_head);
	cache_ent->sw_hash_head = calloc(1, sizeof(struct hash_pieces));
	ASSERT(cache_ent->sw_hash_head);
	TAILQ_INIT(cache_ent->pw_hash_head);
	TAILQ_INIT(cache_ent->sw_hash_head);
	cache_ent->md = md;
	cache_ent->fd = dup(fd);
	ASSERT(cache_ent->fd > 0);
	log(DEBUG, "%s: put in cache %s. Count = %d", __func__,
	    FNAME(md), pw_ctx->cache_size);

	if (pw_ctx->cwd_is_new_on_sworker ||
	    st->st_size < pw_ctx->just_send_the_file_len ||
	    S_ISLNK(st->st_mode) || st->st_nlink > 1 ||
	    st->st_flags & SF_FILE_STUBBED) {
		log(COPY, "NEEDS_SYNC:  %s/%s (%s)",
		    pw_ctx->cwd.path, FNAME(md),
		    get_needs_sync_reason(pw_ctx, st));
		cache_ent->sync_state = NEEDS_SYNC;
		set_cache_state(cache_ent, READY_TO_SEND);
		cache_ent->n_shashes = 0;
		return;
	}
		    
	log(DEBUG, "%s: Calculating short hash for %s",
	    __func__, FNAME(md));

	/* if file size <= short hash length, we only do one hash and lower
	 * the size to match the file length. if file size < 2x short hash
	 * length but > 1x short hash length then start and end hashes will
	 * overlap */
	cache_ent->n_shashes = st->st_size > pw_ctx->short_hash_len ? 2 : 1;
	cache_ent->shashes =
	    calloc(cache_ent->n_shashes, sizeof(struct short_hash_req_info));
	
	/* first hash */	
	cache_ent->shashes[0].hash_offset = 0;
	cache_ent->shashes[0].hash_len =
	    st->st_size > pw_ctx->short_hash_len ?
	    pw_ctx->short_hash_len : st->st_size;
	short_hash(pw_ctx, cache_ent, &cache_ent->shashes[0], &error);
	
	/* second hash (if file length > short hash length) */	
	if (st->st_size > pw_ctx->short_hash_len && !error) {
		cache_ent->shashes[1].hash_offset =
		    st->st_size - pw_ctx->short_hash_len;
		cache_ent->shashes[1].hash_len = pw_ctx->short_hash_len;
		short_hash(pw_ctx, cache_ent, &cache_ent->shashes[1], &error);
	}
	if (error) {
		/* bail out but make sure to remove cache entry */
		cache_entry_cleanup (pw_ctx, pw_ctx->dir_state.dir, cache_ent);
		goto out;
	}

	/* Back to start of file */
	lseek(cache_ent->fd, 0, SEEK_SET); // xxx rc

	cache_ent->sync_state = UNDECIDED;
	set_cache_state(cache_ent, WAITING_FOR_SHORT_HASH);

out:
	isi_error_handle(error, error_out);
	return;
}

void
cache_entry_cleanup(struct migr_pworker_ctx *pw_ctx,
    struct migr_dir *dir,
    struct cache_entry *cache_ent)
{
	log(DEBUG, "%s: cache removal of %s. %d left in cache",
	    __func__, FNAME(cache_ent->md), pw_ctx->cache_size - 1);

	if (cache_ent->fd != -1) {
		log(DEBUG, "%s: closing fd of %s (%d)", __func__,
		    FNAME(cache_ent->md), cache_ent->fd);
		close(cache_ent->fd);
	}

	/* Anything else that needs to be cleaned up */
	TAILQ_REMOVE(&pw_ctx->cache, cache_ent, ENTRIES);
	migr_dir_unref(dir, cache_ent->md);
	hash_pieces_cleanup(cache_ent->pw_hash_head);
	free(cache_ent->pw_hash_head);
	hash_pieces_cleanup(cache_ent->sw_hash_head);
	free(cache_ent->sw_hash_head);
	free(cache_ent->shashes);
	free(cache_ent->fstate.name);
	free(cache_ent);
	pw_ctx->cache_size--;
	ASSERT(pw_ctx->cache_size >= 0);
}

void
ds_start_file(
	struct migr_pworker_ctx *pw_ctx,
	int fd,
	struct migr_dirent *md,
	struct migr_file_state *file,
	struct generic_msg *msg)
{
	struct cache_entry *cache_ent;

	/* This function is called from migr_continue_dir()
	 * immediately after an entry is added to the cache */
	cache_ent = pw_ctx->latest_cache_ent;
	msg->body.file_begin_msg.sync_state = cache_ent->sync_state;
	log(TRACE, "%s: %s %s",
	    __func__, msg->body.file_begin_msg.fname,
	    sync_state_str(cache_ent->sync_state));

	/* No hashing or HASH_RESP_MSG from sworker if we
	 * already know that the file needs to be synced. If it's
	 * UNDECIDED the sworker will have to do a short hash
	 * comparison and send back a HASH_RESP_MSG */
	if (cache_ent->sync_state == UNDECIDED) {
		ASSERT(cache_ent->cache_state ==
		    WAITING_FOR_SHORT_HASH);
		msg->body.file_begin_msg.short_hashes_len =
		    cache_ent->n_shashes *
		    sizeof(struct short_hash_req_info);
		msg->body.file_begin_msg.short_hashes =
		    (char *)cache_ent->shashes;
		/* After sending a FILE_BEGIN, we wait for the
		 * SHORT_HASH_RESP comes back before proceeding with
		 * the file */
		log(TRACE,
		    "%s: Setting waiting_for_short_hash_resp = true",
		    __func__);
		pw_ctx->waiting_for_short_hash_resp = true;
	}
	else {
		log(TRACE,
		    "%s: NOT setting waiting_for_short_hash_resp",
		    __func__);
	}
	cache_ent->fstate.fd = fd;
	cache_ent->fstate.st = *md->stat;
	ASSERT_IMPLIES(!is_fsplit_work_item(pw_ctx->work),
	    pw_ctx->work->f_low == 0);
	cache_ent->fstate.cur_offset = pw_ctx->work->f_low;
	cache_ent->fstate.type = file->type;
	cache_ent->fstate.name = strdup(md->dirent->d_name);
	cache_ent->fstate.enc = md->dirent->d_encoding;
	/* Bug 114951
	 * Copy skip_data to cache */
	cache_ent->fstate.skip_data = file->skip_data;
	cache_ent->fstate.hard_link = file->hard_link;
}

static bool
diff_sync_waiting_for_msgs(struct migr_pworker_ctx *pw_ctx, bool dir_end)
{
	struct generic_msg msg = {};

	/* If file is UNDECIDED, after the FILE_BEGIN nothing happens until
	 * the SHORT_HASH_RESP_MSG comes back */
	if (pw_ctx->waiting_for_short_hash_resp) {
		log(TRACE, "%s: Still waiting for short hash response for %s",
		    __func__, FNAME(pw_ctx->latest_cache_ent->md));
		return true;
	}

	/**
	 * Bug 172631
	 * do not finish a directory if the cache has pending messages
	 */
	if (dir_end && cache_has_msgs_pending(pw_ctx, true)) {
		log(TRACE, "%s: At end of dir and waiting for pending cache"
		    " messages.", __func__);
		pw_ctx->wait_for_acks = true;
		return true;
	}

	/* If the cache is full, nothing happens until a file finishes its
	 * processing and is removed from the cache ... unless no messages
	 * from the sworker are pending, which implies _all_ the entries in
	 * the cache are READY_TO_SEND or ACK_RECEIVED (but not yet
	 * checkpointed since there are files in the cache before that have
	 * not received an ACK), which means we need to continue on so the
	 * remaining READY_TO_SEND files can be processed */
	if (pw_ctx->cache_size >= pw_ctx->max_cache_size) {
		log(TRACE, "%s: Full hash cache is full (%d)",
		    __func__, pw_ctx->cache_size);
		if (cache_has_msgs_pending(pw_ctx, true)) {
			if (pw_ctx->lin_map_blk) {
				log(TRACE, "%s, Cache full - sending LIN_MAP_BLK_MSG", __func__);
				msg.head.type = LIN_MAP_BLK_MSG;
				msg_send(pw_ctx->sworker, &msg);
			}
			return true;
		}
		log(TRACE, "%s:     but all entries are READY_TO_SEND",
		    __func__);
		return false;
	}

	/* Stalled if at the end of a slice and there are messages pending
	 * That is, can't move on to next slice until all files in the current
	 * slice are finished processing */
	if (pw_ctx->wait_for_acks &&
	    cache_has_msgs_pending(pw_ctx, true)) {
		log(TRACE,
		    "%s: Still waiting for final acks of slice. "
		    "Acks left = %d", __func__, pw_ctx->outstanding_acks);
		return true;
	}
	log(TRACE, "%s: not waiting for msgs", __func__);
	return false;
}

bool
ds_continue_dir(struct migr_pworker_ctx *pw_ctx, enum migr_dir_result *result,
    bool dir_end)
{
	struct cache_entry *entry, *dummy;
	bool do_continue = true;
	struct siq_stat *stats = pw_ctx->tw_cur_stats;

	/* result is only set if returning false */

	if (diff_sync_waiting_for_msgs(pw_ctx, dir_end)) {
		*result = MIGR_CONTINUE_DIR;
		do_continue = false;
		migr_wait_for_response(pw_ctx, MIGR_WAIT_MSG);
		goto out;
	}

	log(TRACE, "%s: cache size=%d. States:",
	    __func__, pw_ctx->cache_size);
	TAILQ_FOREACH_SAFE(entry, &pw_ctx->cache, ENTRIES, dummy) {
		log(TRACE, "%s:        %s = %s", __func__,
		    FNAME(entry->md),
		    cache_state_str(entry->cache_state));
		/* If any file has been determined to be NEEDS_SYNC or
		 * NO_SYNC, it's time start processing the file. For
		 * NEEDS_SYNC that means sending the file data
		 * For NO_SYNC it means only sending the FILE_DONE_MSG */
		if (entry->cache_state == READY_TO_SEND) {
			ASSERT(entry->sync_state == NEEDS_SYNC ||
			    entry->sync_state == NO_SYNC);
			/* Copy this entry's file state into the context's
			 * state. The name is always set with a strdup, so be
			 * sure to free any old contents */
			if (entry->sync_state == NEEDS_SYNC)
				stats->files->replicated->new_files++;
			else
				stats->files->replicated->updated_files++;

			if (pw_ctx->file_state.name)
				free(pw_ctx->file_state.name);
			pw_ctx->file_state = entry->fstate;
			pw_ctx->file_state.name = strdup(entry->fstate.name);
			set_cache_state(entry, SENDING);
			pw_ctx->working_cache_ent = entry;

			/* Indicates start sending file */
			*result = MIGR_CONTINUE_FILE;
			do_continue = false;
			goto out;
		}
	}
out:
	if (do_continue)
		log(TRACE, "%s: Continuing reading dir %s",
		    __func__, pw_ctx->cwd.path);
	else
		log(TRACE, "%s: Not yet continuing reading dir %s (%s)",
		    __func__, pw_ctx->cwd.path,
		    migr_dir_result_str(*result));
	return do_continue;
}

static void
hash_error_handle(struct migr_pworker_ctx *pw_ctx,
    struct hash_error_msg *errmsg,
    enum origin worker)
{
	errno = 0;
	ioerror(pw_ctx->coord, pw_ctx->cwd.utf8path, NULL, 0, 0,
	    "Hashing failed at the %s: %s (%s)", origin_str(worker),
	    pw_ctx->cwd.utf8path, hash_error_str(errmsg->hash_error));
}

int
pw_hash_error_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct hash_error_msg *errmsg = &m->body.hash_error;

	hash_error_handle(pw_ctx, errmsg, PWORKER);
	return 0;
}

int
sw_hash_error_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct hash_error_msg *errmsg = &m->body.hash_error;

	hash_error_handle(pw_ctx, errmsg, SWORKER);
	return 0;
}

static void
log_diff_sync_variables(bool config_file_exists)
{
	int i;

	log(config_file_exists ? INFO : DEBUG,
	    "Pworker Diff Sync Values:");
	for (i = 0; i < sizeof(ds_config) / sizeof(struct ds_config); i ++)
		log(config_file_exists ? INFO : DEBUG,
		    "    %s: %d", ds_config[i].name, *ds_config[i].value_p);
}

void
diff_sync_init(struct migr_pworker_ctx *pw_ctx)
{
	parse_diff_sync_variables();
	pw_ctx->just_send_the_file_len = JUST_SEND_THE_FILE_LEN;
	pw_ctx->short_hash_len = SHORT_HASH_LEN;
	pw_ctx->min_full_hash_piece_len = MAX(MIN_FULL_HASH_PIECE_LEN,
	    OPTIMAL_FILE_READ_SIZE);
	pw_ctx->hash_pieces_per_file = HASH_PIECES_PER_FILE;
	pw_ctx->max_cache_size = MAX_CACHE_SIZE;
	pw_ctx->hash_type = HASH_TYPE;
	TAILQ_INIT(&pw_ctx->cache);
}

void
parse_diff_sync_variables(void)
{
	FILE *fp;
	char *s, buf[1000];
	int i, len, line = 0;
	bool found;

	if (access(DIFF_SYNC_CONFIG_FILE, R_OK) != 0) {
		log(TRACE, "No pw_diff_sync config file at %s",
		    DIFF_SYNC_CONFIG_FILE);
		log_diff_sync_variables(false);
		return;
	}
	fp = fopen(DIFF_SYNC_CONFIG_FILE, "r");
	if (!fp) {
		log(ERROR, "Can't open pw_diff_sync config file at %s: %s",
		    DIFF_SYNC_CONFIG_FILE, strerror(errno));
		return;
	}

	while (fgets(buf, sizeof(buf), fp)) {
		line++;
		/* Catch line bigger than buffer, but make sure it's not just
		 * last line of file with no ending new line */
		if (strlen(buf) == sizeof(buf) - 1 &&
		    buf[strlen(buf) - 1] != '\n') {
			log(ERROR, "In %s:%d, too long of line",
			    DIFF_SYNC_CONFIG_FILE, line);
			break;
		}
		s = buf;
		while (*s && isspace(*s))
			s++;
		if (!*s || *s == '#')
			continue;
		found = false;
		for (i = 0;
		     i < sizeof(ds_config) / sizeof(struct ds_config);
		     i ++) {
			len = strlen(ds_config[i].name);
			if (strncmp(ds_config[i].name, s, len) == 0) {
				found = true;
				s += len;
				while (*s && !isdigit(*s))
					s++;
				if (!*s) {
					log(ERROR, "In %s:%d, no value for %s",
					    DIFF_SYNC_CONFIG_FILE, line,
					    ds_config[i].name);
					break;
				}
				*ds_config[i].value_p = atoi(s);
				log(TRACE, "Read %s = %d",
				    ds_config[i].name,
				    *ds_config[i].value_p);
			}
		}
		if (!found) {
			log(ERROR, "In %s:%d, invalid entry.",
			    DIFF_SYNC_CONFIG_FILE, line);
		}
	}
	fclose(fp);
	log_diff_sync_variables(true);
}

char *
cache_state_str(enum cache_state state)
{
	static char buf[30];

	switch (state) {
	case DS_INIT:
		return "DS_INIT";
	case WAITING_FOR_SHORT_HASH:
		return "WAITING_FOR_SHORT_HASH";
	case WAITING_FOR_FULL_HASH:
		return "WAITING_FOR_FULL_HASH";
	case READY_TO_SEND:
		return "READY_TO_SEND";
	case SENDING:
		return "SENDING";
	case WAITING_FOR_ACK:
		return "WAITING_FOR_ACK";
	case ACK_RECEIVED:
		return "ACK_RECEIVED";
	case DS_ERROR:
		return "DS_ERROR";
	case CHECKPOINTED:
		return "CHECKPOINTED";
	default:
		snprintf(buf, sizeof(buf), "Unknown cache state %d", state);
		return buf;
	}
}

void
log_cache_state(struct migr_pworker_ctx *pw_ctx)
{
	struct cache_entry *entry;

	log(TRACE, "<<<<<<<< Cache state, %d entries  >>>>>>>>>>>>",
	    pw_ctx->cache_size);
	TAILQ_FOREACH(entry, &pw_ctx->cache, ENTRIES) {
		log(TRACE, "%s:        %s = %s", __func__,
		    FNAME(entry->md), cache_state_str(entry->cache_state));
	}
	log(TRACE, ">>>>>>>> ----------------------- <<<<<<<<<<<<");
}		
