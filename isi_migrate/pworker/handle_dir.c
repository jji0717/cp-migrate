#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/acl.h>

#include <sys/isi_enc.h>
#include <ifs/ifs_userattr.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>
#include <isi_acl/sd_acl.h>
#include <ifs/ifs_lin_open.h>
#include <isi_domain/dom.h>
#include <isi_domain/dom_util.h>
#include <isi_cpool_cbm/isi_cbm_snap_diff.h>
#include <isi_cpool_bkup/isi_cpool_bkup.h>
#include <isi_cpool_bkup/isi_file_exclude.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/alg.h"
#include "pworker.h"

static bool migr_start_dir(struct migr_pworker_ctx *, struct migr_dir_state *,
    struct isi_error **);


static void __noinline
isi_ioerror_only_paths(struct migr_pworker_ctx *pw_ctx,
    struct isi_error *error, char *name, enc_t enc,
    ifs_lin_t lin, char *op, char *utf8path, char *dir_path)
{
	char *tpath =NULL;
	char *tutf8path = NULL;
	bool mem = false;
	if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
		errno = isi_system_error_get_error(
		    (const struct isi_system_error *)error);
	else
		errno = EIO;
	if (is_tw_work_item(pw_ctx) || !lin) {
		tpath = dir_path;
		tutf8path = utf8path;
	} else {
		tpath = get_valid_rep_path(lin, pw_ctx->cur_snap);
		tutf8path = tpath;
		mem = true;
	}

	/*
	 * if expected_dataloss flag set, ignore sending error to coord
	 */
	if (!pw_ctx->expected_dataloss) {
		ioerror(pw_ctx->coord, tutf8path, name, enc, lin,
			"Failed to %s: %s/%s (lin %llx)", op,
			tpath, name ? name : "<unknown>", lin);
	}
	pw_ctx->tw_cur_stats->files->skipped->error_io++;

	if (mem && tpath) {
		free(tpath);
	}
}

static void __noinline
isi_ioerror_only(struct migr_pworker_ctx *pw_ctx,
    struct isi_error *error, char *name, enc_t enc,
    ifs_lin_t lin, char *op)
{
	isi_ioerror_only_paths(pw_ctx, error, name,
	    enc, lin, op, pw_ctx->cwd.utf8path,
	    pw_ctx->cwd.utf8path);
}

static struct isi_error * __noinline
isi_ioerror_and_system_error(struct migr_pworker_ctx *pw_ctx,
    int kept_errno, char *name, enc_t enc, ifs_lin_t lin, char *op,
    char *err_op)
{
	struct isi_error *error;

	error = isi_system_error_new(kept_errno,
	    "%s: (%s) (%s) (%s)", err_op, pw_ctx->cwd.utf8path,
	    pw_ctx->cwd.path, name ? name : "<unknown>");
	isi_ioerror_only(pw_ctx, error, name, enc, lin, op);
	return error;
}

static char pworker_zero_buff[IDEAL_READ_SIZE];

/*  _   _ _   _ _     
 * | | | | |_(_) |___ 
 * | | | | __| | / __|
 * | |_| | |_| | \__ \
 *  \___/ \__|_|_|___/
 */

static uint32_t
pworker_checksum_compute(char *data, int size)
{
	checksum_context checksum_ctx;
	uint32_t result;
	ASSERT(size <= IDEAL_READ_SIZE);
	ASSERT(data);

	checksumInit(&checksum_ctx, 0);
	checksumUpdate(&checksum_ctx, data, size, 0);

	result = *(uint32_t *)checksum_ctx.result;
	UFAIL_POINT_CODE(send_bad_checksum, result++);

	return result;
}

/*
 * Checks if file is unchanged since prev snapshot.
 *
 * @return true on success and file unchanged
 *	   false on success and file changed
 *	   undefined on error, with error_out set
 */
static bool
file_unchanged(struct migr_pworker_ctx *pw_ctx, int dirfd, char *fname,
    enc_t fenc, struct stat *new_stat, ifs_snapid_t *prev_snap_out,
    bool *size_changed_out, struct isi_error **error_out)
{
	struct stat old_stat;
	bool unchanged = false;
	struct isi_error *error = NULL;
	int ret;

	if ((dirfd == -1) || (fname == NULL) || (new_stat == NULL))
		goto out;

	/* Stat the old version (if it exists). */
	ret = enc_fstatat(dirfd, fname, fenc, &old_stat, AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		switch (errno) {
		case ENOENT:
			/* New file */
			break;
		default:
			/* Error */
			error = isi_ioerror_and_system_error(pw_ctx, errno,
			    fname, fenc, old_stat.st_ino,
			    "old_file_unch_stat", "Error stat old file");
			break;
		}
		goto out;
	}

	/* If the inode number changed, it must have changed. */
	if (new_stat->st_ino != old_stat.st_ino)
		goto out;

	/*
	 * check predicate on old stat
	 * If file wasn't selected before, it should be transferred
	 */
	ret = siq_select(pw_ctx->plan, fname, fenc, &old_stat,
	    pw_ctx->old_time, pw_ctx->cwd.utf8path, 
	    stat_mode_to_file_type(old_stat.st_mode), &error);
	if (error) {
		isi_ioerror_only(pw_ctx, error, fname, fenc,
		    old_stat.st_ino, "old_file_unch_siq_select");
		goto out;
	}

	if (ret == 0) {
		log(TRACE, "File %s(%s) had passed selection first "
		    "time, it should be sent", fname,
		    enc_type2name(fenc));
		goto out;
	}

	ret = ifs_snap_file_modified(new_stat->st_ino, old_stat.st_snapid,
	    new_stat->st_snapid);

	switch(ret) {
	case 0:
		unchanged = true;
		break;
	case 1:
		unchanged = false;
		(*prev_snap_out) = old_stat.st_snapid;
		(*size_changed_out) = old_stat.st_size != new_stat->st_size;
		break;
	default:
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    fname, fenc, new_stat->st_ino,
		    "old_file_unch_fmod", "Error comparing versions");
		break;
	}

out:
	isi_error_handle(error, error_out);
	return unchanged;
}

/*  ____                 _   _   _ _   _ _     
 * / ___|  ___ _ __   __| | | | | | |_(_) |___ 
 * \___ \ / _ \ '_ \ / _` | | | | | __| | / __|
 *  ___) |  __/ | | | (_| | | |_| | |_| | \__ \
 * |____/ \___|_| |_|\__,_|  \___/ \__|_|_|___/
 */

static int
migr_send_dir(struct migr_pworker_ctx *pw_ctx, bool need_ack_dir, char *acl)
{
	struct generic_msg msg = {};
	int ret, fd;

	ilog(IL_INFO, "migr_send_dir: path %s  ads=%d  ads_old=%d",
	    pw_ctx->cwd.path, pw_ctx->doing_ads, pw_ctx->ads_oldpwd);
	pw_ctx->checkpoints++;

	msg.head.type = DIR_MSG4;
	msg.body.dir4.di_flags = (unsigned)pw_ctx->dir_state.st.st_flags;

	fd = migr_dir_get_fd(pw_ctx->dir_state.dir);

	if (pw_ctx->dir_state.st.st_flags & (SF_HASNTFSACL | SF_HASNTFSOG))
		msg.body.dir4.acl = acl;

	msg.body.dir4.eveclen = sizeof(enc_t) *
	    enc_vec_size(&pw_ctx->cwd.evec);
	msg.body.dir4.evec = pw_ctx->cwd.evec.encs;
	msg.body.dir4.dir = pw_ctx->cwd.path;
	msg.body.dir4.dir_lin = pw_ctx->dir_lin;
	msg.body.dir4.mode = pw_ctx->dir_state.st.st_mode;
	msg.body.dir4.atime = pw_ctx->dir_state.st.st_atimespec.tv_sec;
	msg.body.dir4.mtime = pw_ctx->dir_state.st.st_mtimespec.tv_sec;
	msg.body.dir4.reap = 0;
	msg.body.dir4.uid = (unsigned)pw_ctx->dir_state.st.st_uid;
	msg.body.dir4.gid = (unsigned)pw_ctx->dir_state.st.st_gid;
	msg.body.dir4.need_ack_dir = need_ack_dir;
	msg.body.dir4.acl = acl;

	ret = msg_send(pw_ctx->sworker, &msg);

	/* we don't get acks that let us know that a directory was synced
	 * unless it contains files that are synced/acked. under certain
	 * conditions, we could end up checkpointing (timer_callback) past
	 * the empty dir even though the sworker never created it (group 
	 * change/core). to get around this without changing the
	 * messaging/protocol, we suspend checkpointing until after the next 
	 * file ack is received, at which point we know the directory was 
	 * created. see bug 96259 */
	pw_ctx->skip_checkpoint = true;

	return ret;
}

static int
send_ads(struct migr_pworker_ctx *pw_ctx, bool start)
{
	struct generic_msg msg = {};

	log(TRACE, "send_ads: start = %d", start);
	msg.head.type = ADS_MSG;
	msg.body.ads.start = start;

	msg_send(pw_ctx->sworker, &msg);
	return 0;
}

bool
migr_start_ads(struct migr_pworker_ctx *pw_ctx, int fd, struct stat *st,
    char *debug_fname, enc_t debug_enc, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool has_ads = true;
	bool skip_ads;
	int dfd;

	log(TRACE, "%s: has ADS", __func__);

	if (!(st->st_flags & UF_HASADS))
		return false;

	/* If we are working on part of a file, and we aren't the first worker
	 * to have worked on this file, then don't work on an ADS stream for
	 * this file. The worker with the earliest region of the file will
	 * send the ADS entries. */
	if (is_fsplit_work_item(pw_ctx->work) && pw_ctx->work->f_low != 0) {
		return false;
	}

	if (pw_ctx->compliance_v2 && pw_ctx->restore) {
		skip_ads = comp_skip_ads(pw_ctx->cur_lin, pw_ctx->cur_snap,
		    pw_ctx->prev_snap, &error);
		if (error)
			goto out;

		if (skip_ads)
			return false;
	}

	ASSERT(!pw_ctx->doing_ads);

	/* Delay if a files per second limit is active */
	throttle_delay(pw_ctx);

	/* Cue us up to the right spot in the dir to begin. */
	dfd = enc_openat(fd, ".", ENC_DEFAULT, O_RDONLY | O_XATTR);
	if (dfd == -1) {
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    debug_fname, debug_enc, st->st_ino, "ads_opendir",
		    "Failed to open ADS dir");
		goto out;
	}

	pw_ctx->ads_oldpwd = open(".", O_RDONLY);

	UFAIL_POINT_CODE(ads_chdir,
		if (pw_ctx->ads_oldpwd != -1) {
			close(pw_ctx->ads_oldpwd);
			pw_ctx->ads_oldpwd = -1;
			errno = RETURN_VALUE;
		}
	);

	if (pw_ctx->ads_oldpwd == -1) {
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    debug_fname, debug_enc, st->st_ino, "open cwd ads",
		    "Failed to open ADS cwd");
		goto out;
	}

	if (fchdir(dfd)) {
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    debug_fname, debug_enc, st->st_ino, "chdir ads",
		    "Failed to fchdir ADS dir");
		goto out;
	}

	if (fstat(dfd, &pw_ctx->ads_dir_state.st)) {
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    debug_fname, debug_enc, st->st_ino, "stat ads",
		    "Failed to stat ADS dir");
		goto out;
	}

	pw_ctx->ads_dir_state.dir = migr_dir_new(dfd, NULL, true, &error);
	if (error) {
		isi_ioerror_only(pw_ctx, error, debug_fname, debug_enc,
		    st->st_ino, "dir new ads");
		goto out;
	}

	pw_ctx->doing_ads = true;

	/* 
	 * XXXJDH: Why is this here?
	 *
	 * If this ADS directory has been modified, prepare for deletes.
	 */

	send_ads(pw_ctx, 1);
	pw_ctx->ads_delete = (pw_ctx->action == SIQ_ACT_SYNC);

	has_ads = migr_start_dir(pw_ctx, &pw_ctx->ads_dir_state, &error);
	if (error) {
		isi_ioerror_only(pw_ctx, error, debug_fname, debug_enc,
		    st->st_ino, "dir start ads");
		goto out;
	}
	ASSERT(!has_ads);

out:
	isi_error_handle(error, error_out);
	return true;
}

static void
migr_send_dir_range_update(struct migr_pworker_ctx *pw_ctx, 
    uint64_t cookie_end)
{
	struct generic_msg m = {};
	
	if (pw_ctx->stf_sync == false)
		goto out;
	
	log(TRACE, "SEND DIR_UPGRADE_MSG to sworker");
	m.head.type = DIR_UPGRADE_MSG;
	m.body.dir_upgrade.slice_begin = 
	    pw_ctx->dir_state.dir->prev_sent_cookie;
	m.body.dir_upgrade.slice_max = cookie_end;
	msg_send(pw_ctx->sworker, &m);

out:
	return;
}

static void
migr_send_done_msg(struct migr_pworker_ctx *pw_ctx)
{
	struct generic_msg m = {};

	ASSERT(!pw_ctx->pending_done_msg);
	pw_ctx->pending_done_msg = true;
	
	ilog(IL_INFO, "%s called, Send DONE_MSG to sworker", __func__);
	m.head.type = DONE_MSG;
	m.body.done.count = 0;
	msg_send(pw_ctx->sworker, &m);
	flush_burst_buffer(pw_ctx);
}

static void
migr_send_file_done_msg(
    struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file,
    bool success, char *hash)
{
	struct generic_msg msg = {};
	int ret;

#ifdef DEBUG_PERFORMANCE_STATS
	file_data_end();
#endif

	log(TRACE, "%s", __func__);
	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		log(TRACE, "%s: %s cache_state=%s", __func__,
		    FNAME(pw_ctx->working_cache_ent->md),
		    cache_state_str(pw_ctx->working_cache_ent->cache_state));


	msg.head.type = FILE_DONE_MSG;
	msg.body.file_done_msg.success = success;
	msg.body.file_done_msg.hash_str = hash;
	msg.body.file_done_msg.size_changed = file->size_changed;

	msg.body.file_done_msg.src_dlin = pw_ctx->dir_lin;
	msg.body.file_done_msg.src_lin = file->st.st_ino;
	msg.body.file_done_msg.fname = file->name;
	msg.body.file_done_msg.fenc = file->enc;

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
		ASSERT(pw_ctx->working_cache_ent->cache_state == SENDING);
		set_cache_state(pw_ctx->working_cache_ent, WAITING_FOR_ACK);
		msg.body.file_done_msg.sync_state =
		    pw_ctx->working_cache_ent->sync_state;
	}

	//XXX:EML do something with the error
	ret = msg_send(pw_ctx->sworker, &msg);
	if (!pw_ctx->doing_ads) {
		pw_ctx->tw_sending_file = false;
	}
}

/**
 * Hash the specified file range. If end of file is reached, an error is 
 * returned because on the pworker side this should never happen (we are 
 * hashing ranges prior to a block we are about to send).
 */
static off_t
pw_migr_hash_range_fd(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, off_t offset,
    uint64_t len, struct isi_error **error_out)
{
	char *buf;
	int cur_len;
	struct isi_error *error = NULL;
	buf = malloc(MIN(pw_ctx->read_size, len));
	ASSERT(buf);

	while (len > 0) {
		cur_len = MIN(len, pw_ctx->read_size);
		if (file->cpstate.sync_type == BST_DEEP_COPY) {
			//XXXDPL Diff sync?
			ASSERT(file->cpstate.bsess != NULL);
			cur_len = isi_file_bsession_read_file(
			    file->cpstate.bsess, buf, offset, cur_len, &error);
			if (error)
				goto out;
		} else
			cur_len = pread(file->fd, buf, cur_len, offset);
		if (cur_len == -1) {
			error = isi_ioerror_and_system_error(pw_ctx, errno,
			    file->name, file->enc, file->st.st_ino,
			    "hash_range_read", "Failed to read hash block");

			goto out;
		}
		ASSERT(cur_len != 0);
		len -= cur_len;

		hash_update(file->bb_hash_ctx, buf, cur_len);
		offset += cur_len;
	}
out:
	free(buf);
	isi_error_handle(error, error_out);
	return offset;
}

/**
 * Add a zero'd range to the hash.
 */
static void
migr_hash_zero_range(struct hash_ctx *bb_hash_ctx, uint64_t len)
{
	int cur_len;

	while (len > 0) {
		cur_len = MIN(len, IDEAL_READ_SIZE);
		len -= cur_len;

		hash_update(bb_hash_ctx, pworker_zero_buff, cur_len);
	}
}

static void
fsync_snap_version(ifs_lin_t lin, ifs_snapid_t snap,
    struct isi_error **error_out)
{
	int ret;
	int fd = -1;
	struct isi_error *error = NULL;

	fd = ifs_lin_open(lin, snap, O_RDONLY);
	if (fd == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "ifs_lin_open failed on %llx snap %lld",
		    lin, snap);
		goto out;
	}

	if (fd == -1)
		goto out;

	ret = fsync(fd);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "fstat failed on lin %llx snap %lld",
		    lin, snap);
		goto out;
	}

out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);

}

static bool
migr_continue_stub_sync(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, struct isi_error **error_out)
{
	bool read_success = false;
	bool done = false;

	struct generic_msg GEN_MSG_INIT_CLEAN(m);
	struct siq_ript_msg *ript = NULL;

	struct isi_error *error = NULL;

	struct migr_cpools_file_state *cpst = NULL;

	size_t *blobs_to_send, *blobs_sent;
	size_t *blob_size;

	size_t read_size;
	uint8_t *data = NULL;
	const char *sct_str = NULL;

	ASSERT(pw_ctx != NULL && file != NULL);

	log(TRACE, "%s", __func__);

	cpst = &file->cpstate;
	ASSERT(cpst->sync_type == BST_STUB_SYNC);
	ASSERT(pw_ctx->job_ver.common >= MSG_VERSION_RIPTIDE &&
	    (pw_ctx->curr_ver & FLAG_VER_CLOUDPOOLS));

	sct_str = (cpst->curr_sct == SCT_DATA) ? "data" : "metadata";

	if (cpst->curr_sct == SCT_METADATA) {
		blobs_to_send = &cpst->nmblobs;
		blobs_sent = &cpst->mblobs_sent;
		blob_size = &cpst->mbytes_pb;
	} else {
		ASSERT(cpst->curr_sct == SCT_DATA);
		blobs_to_send = &cpst->ndblobs;
		blobs_sent = &cpst->dblobs_sent;
		blob_size = &cpst->dbytes_pb;
	}

	if (*blobs_sent == *blobs_to_send)
		goto out;

	log(TRACE, "%s: sct: %d %lu/%lu sent size %lu",
	    __func__, cpst->curr_sct, *blobs_sent, *blobs_to_send, *blob_size);

	m.head.type = build_ript_msg_type(CLOUDPOOLS_STUB_DATA_MSG);
	ript = &m.body.ript;
	ript_msg_init(ript);
	ript_msg_set_field_uint32(ript, RMF_TYPE, 1,
	    (uint32_t)cpst->curr_sct);

	read_success = isi_file_bsession_read_blob(cpst->bsess, cpst->curr_sct,
	    (void**)&data, &read_size, &error);
	if (error)
		goto out;

	if (read_success) {
		log(TRACE, "%s: %llx read %lu byte %s blob.",
		    __func__, file->st.st_ino, read_size, sct_str);

		ript_msg_set_field_bytestream(ript, RMF_DATA, 1, data,
		    (uint32_t)read_size);

		msg_send(pw_ctx->sworker, &m);

		(*blobs_sent)++;

		ASSERT(*blobs_sent <= *blobs_to_send);

		isi_file_bsession_blob_free(data);
		data = NULL;
	}

out:
	if (error == NULL &&
	    (!read_success || *blobs_sent == *blobs_to_send)) {
		switch (cpst->curr_sct) {
		case SCT_METADATA:
			log(TRACE, "%s: %llx done sending metadata.", __func__,
			    file->st.st_ino);
			cpst->curr_sct = SCT_DATA;
			break;

		case SCT_DATA:
			log(TRACE, "%s: %llx done sending data.", __func__,
			    file->st.st_ino);
			done = true;
			break;

		default:
			ASSERT(0, "%s: %llx invalid value for stub content "
			    "type.", __func__, file->st.st_ino);
			break;
		}
	}

	isi_error_handle(error, error_out);
	return done;
}

static bool
migr_find_next_region(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, off_t *region_begin_out,
    unsigned *region_len_out, int *region_type_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_cbm_snap_diff_iterator *diff_iter = NULL;
	struct snap_diff_region region;
	ifs_snapid_t old_snapid, new_snapid;
	ifs_lin_t lin;
	int ret;
	bool found_region = false;

	log(TRACE, "%s", __func__);

	old_snapid = file->prev_snap;
	new_snapid = file->st.st_snapid;
	lin = file->st.st_ino;
	region.byte_count = 0;
	region.region_type = RT_SPARSE;
	region.start_offset = 0;

	/*
	 * If its restore and file is modified then always use
	 * the HEAD as new snapid.
	 */
	if (pw_ctx->restore && old_snapid != INVALID_SNAPID) {
		old_snapid = file->st.st_snapid;
		new_snapid = HEAD_SNAPID;
	}

	/*
	 * XXX  Temporary hack to flush all the coalesced data before
	 * calling snap diff iterator. This needs to be removed once
	 * the bug 85519. XXX
	 */

	if (file->cur_offset == 0) {
		fsync_snap_version(lin, new_snapid, &error);
		if (error)
			goto out;
	}

	/*
	 *
	 * PSCALE-23403
	 * For Cloudpools deep copy sync, in previous implementation
	 * isi_cbm_snap_diff_next() calcuate a range for two snapshots with
	 * identical cache mask. It calls cbm_cache_iterator.next() to calculate
	 * an isi_cbm_cache_record_info which indicates an offset/length cache
	 * range. Inside this function, it calls cpool_cache.get_status_range()
	 * to loop the whole range of the LIN to find out a sequential region
	 * with the same cache mask type with the previous region.
	 *
	 * But this implementation is inefficient in processing a large file,
	 * as each round processing, SIQ just pick up 128KB data to do the data
	 * content diff via ifs_snap_diff_next() and advance the cursor.
	 * In the next round, it run the whole time-consuming process again
	 * from the new offset (just advanced 128KB offset) till EOF.
	 *
	 * PSCALE-23403 introduces a cache (file->sd_region) in SIQ pworker
	 * context fstate to save the snap diff region for each file snap
	 * diff iteration after invoking isi_cbm_snap_diff_next(). Actually
	 * inside isi_cbm_snap_diff_next(), it not only handle Cloudpools
	 * stub snap diff comparation, but also handle normal file snap diff
	 * comparation. The key to this solution is "not to do what once
	 * been done before". As we have already obtained a snap diff
	 * region in the previous round then reuse it in the new round,
	 * instead throwing it away. The reuse will be terminated until the
	 * previous region exhausted and starts to process a new snap-diff
	 * range type or hits EOF.
	 *
	 * The principle of the solution is not only applicable to Cloudpools
	 * stub file sync, but also valid for the other types of file
	 * data sync such as REGULAR and SPARSE. But there are some gaps in
	 * handling file split case. Given the customer issue for CITYGROUP
	 * https://bugs.west.isilon.com/show_bug.cgi?id=274998
	 * is urgent, we shrink the scope of this fix within Cloudpools stub
	 * sync and create PSCALE-24802 to track the normal file sync and
	 * address file split case in versions beyond Empire.
	 *
	 */
	log(TRACE, "%s lin:%llx, file->cur_offset=%llu, file->sd_region.start_offset=%llu, file->sd_region.byte_count=%llu, file->sd_region.region_type=%d",\
	    __func__, lin, file->cur_offset, file->sd_region.start_offset, file->sd_region.byte_count, file->sd_region.region_type);
	if ((file->st.st_flags & SF_FILE_STUBBED) && !is_fsplit_work_item(pw_ctx->work) && file->cur_offset != 0 && (file->cur_offset >= file->sd_region.start_offset) && (file->cur_offset < file->sd_region.start_offset + file->sd_region.byte_count)) {
		/* Reuse fstate snap diff region and set local viable region here */
		region.start_offset = file->cur_offset;
		region.byte_count = file->sd_region.byte_count - (file->cur_offset - file->sd_region.start_offset);
		region.region_type = file->sd_region.region_type;
		log(TRACE, "%s reuse region: lin:%llx, file->cur_offset=%llu, file.sd_region.start_offset=%llu, region.byte_count=%llu, region.region_type=%d",\
		    __func__, lin, file->cur_offset, file->sd_region.start_offset, region.byte_count, region.region_type);
	} else {

		diff_iter = isi_cbm_snap_diff_create(lin, file->cur_offset, old_snapid,
		new_snapid, (file->st.st_flags & SF_FILE_STUBBED) == 0);
		if (!diff_iter) {
			error = isi_siq_error_new(E_SIQ_CLOUDPOOLS, "Could not "
			"initialize snap diff iterator");
			goto out;
		}

		log(TRACE, "%s lin:%llx, file->cur_offset=%llu", __func__, lin, file->cur_offset);

		do {
			ret = isi_cbm_snap_diff_next(diff_iter, &region);
			UFAIL_POINT_CODE(snap_block_diff_error,
				/* Force block error but exclude for extra file */
				if (!pw_ctx->doing_extra_file) {
					ret = -1;
					errno = RETURN_VALUE;
				}
			);
			if (ret) {
				error = isi_ioerror_and_system_error(pw_ctx, errno,
				file->name, file->enc, file->st.st_ino,
				"diff_range", "Failed to diff file");
				goto out;
			}
			if (region.region_type == RT_UNCHANGED) {
				pw_ctx->tw_cur_stats->bytes->data->unchanged +=
				region.byte_count;
				pw_ctx->stf_cur_stats->bytes->data->unchanged +=
				region.byte_count;
			}

			ASSERT(file->prev_snap != INVALID_SNAPID ||
			region.region_type != RT_UNCHANGED);
		} while (region.byte_count && region.region_type == RT_UNCHANGED);

		if (region.byte_count) {
			file->sd_region = region;
			log(TRACE, "%s lin:%llx, file->sd_region.start_offset=%llu, file->sd_region.byte_count=%llu, file->sd_region.region_type=%u",\
			    __func__, lin, file->sd_region.start_offset, file->sd_region.byte_count, file->sd_region.region_type);
		}
	}

	if (region.byte_count) {
		ASSERT(region.byte_count > 0);

		*region_type_out = region.region_type;
		/*
		 * For files that are modified , we would treat HEAD
		 * sparse regions as data regions.  We could optimize
		 * in future to have one more check for the file in
		 * in snapshot version to make sure it has sparse region.
		 */
		if (pw_ctx->restore && old_snapid != INVALID_SNAPID &&
		    *region_type_out == RT_SPARSE)
			*region_type_out = RT_DATA;

		if (*region_type_out == RT_SPARSE)
			*region_len_out =
			    MIN(region.byte_count, IDEAL_READ_SIZE);
		else
			*region_len_out = 
			    MIN(region.byte_count, pw_ctx->read_size);

		*region_begin_out = region.start_offset;
		found_region = true;

		log(TRACE, "%s postprocess lin:%llx, region_begin_out=%llu, region_len_out=%u, region.region_type=%d",\
		    __func__, lin, *region_begin_out, *region_len_out, region.region_type);
	}

out:
	if (diff_iter)
		isi_cbm_snap_diff_destroy(diff_iter);

	isi_error_handle(error, error_out);
	return found_region;
}

static void
migr_send_data_msg(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, off_t reg_offset, unsigned reg_len, int reg_type,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_snapid_t new_snapid;
	ifs_lin_t lin;
	new_snapid = file->st.st_snapid;
	lin = file->st.st_ino;
	char *buf = NULL;
	struct generic_msg msg = {};
	int ret;

	log(TRACE, "%s", __func__);
	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		ASSERT(pw_ctx->working_cache_ent->cache_state == SENDING);
	ASSERT(reg_len > 0);

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.logical_size = reg_len;
	msg.body.file_data.offset = reg_offset;
	msg.body.file_data.src_dlin = pw_ctx->dir_lin;
	msg.body.file_data.src_lin = file->st.st_ino;
	msg.body.file_data.fname = file->name;
	msg.body.file_data.fenc = file->enc;

	switch(reg_type) {
	case RT_SPARSE:
		msg.body.file_data.data_type = MSG_DTYPE_SPARSE;
		msg.body.file_data.data_size = 0;
		if (file->bb_hash_ctx)
			migr_hash_zero_range(file->bb_hash_ctx, reg_len);
		pw_ctx->tw_cur_stats->bytes->data->sparse += reg_len;
		pw_ctx->stf_cur_stats->bytes->data->sparse += reg_len;
		break;

	case RT_DATA:
		msg.body.file_data.data_type = MSG_DTYPE_DATA;
		msg.body.file_data.data_size = reg_len;
		buf = malloc(reg_len);
		ASSERT(buf);
		if (file->cpstate.sync_type == BST_DEEP_COPY &&
		    file->st.st_flags & SF_FILE_STUBBED) {
			ASSERT(file->cpstate.bsess != NULL);

			ret = isi_file_bsession_read_file(file->cpstate.bsess,
			    buf, reg_offset, reg_len, &error);

			if (error)
				goto out;
		} else
			ret = pread(file->fd, buf, reg_len, reg_offset);
		UFAIL_POINT_CODE(send_file_read,
			/* Force read error but exclude for extra file */
			if (!pw_ctx->doing_extra_file) {
				ret = -1;
				errno = RETURN_VALUE;
			}
		);
		if (ret < 0) {
			error = isi_ioerror_and_system_error(pw_ctx, errno,
			    file->name, file->enc, lin,
			    "send_file_read", "Failed to read block");
			goto out;
		}
		ASSERT(ret == reg_len);
		if (file->bb_hash_ctx)
			hash_update(file->bb_hash_ctx, buf, reg_len);
		pw_ctx->tw_cur_stats->bytes->data->file += reg_len;
		pw_ctx->stf_cur_stats->bytes->data->file += reg_len;
		break;

	case RT_UNCHANGED:
		msg.body.file_data.data_type = MSG_DTYPE_UNCHANGED;
		msg.body.file_data.data_size = 0;
		break;

	default:
		log(ERROR, "Encountered bad region type %d, "
		  "lin %016llx %016llx", reg_type, lin, new_snapid);
		ASSERT(false);
		break;
	}
	ASSERT(!error);

	msg.body.file_data.data = buf;
	if (pw_ctx->checksum_size > 0 && reg_type == RT_DATA) 
		msg.body.file_data.checksum =
		    pworker_checksum_compute(buf, reg_len);

#ifdef DEBUG_PERFORMANCE_STATS
	if (reg_offset == 0)
		file_data_start();
#endif

	//XXX:EML do something with the error
	ret = msg_send(pw_ctx->sworker, &msg);
	ASSERT(msg.head.len > msg.body.file_data.data_size);

	if (reg_type != RT_UNCHANGED) {
		pw_ctx->tw_cur_stats->bytes->data->total += reg_len;
		pw_ctx->stf_cur_stats->bytes->data->total += reg_len;
	}

	/* skip_checkpoint is true if we're working on the first file after
	 * sending a directory. If we overflow the pworker send buffer and the
	 * sworker receive buffer with file data messages, we know it has
	 * processed the directory, so allow checkpointing again.
	 * See bug 109968. */
	if (pw_ctx->skip_checkpoint)
		pw_ctx->cur_file_data_sent += msg.body.file_data.data_size;

	if (pw_ctx->cur_file_data_sent > (SOCK_BUFF_SIZE * 2)) {
		pw_ctx->skip_checkpoint = false;
		if (pw_ctx->split_req_pending) {
			log(DEBUG, "Attempting split for pending request");
			try_work_split(pw_ctx);
		}
	}

	

out:
	free(buf);
	buf = NULL;
	isi_error_handle(error, error_out);
}

/**
 * Enable bb hashing if it's currently disabled, we aren't doing a full 
 * transfer and and we either received a block update, or the file size has 
 * changed.
 *
 * Perform any block based catch-up hashing (of skipped regions) is needed. 
 * If the catch up size is greater than an the imposed limit, the function will
 * return before finishing the whole region to allow us to check for RPC 
 * messages periodically.  If the catchup is completed, the function will 
 * return true.  If not, false will be returned.  In either case, the 
 * cur_offset field in file will be updated to point to the byte immediately 
 * after the currently hashed region.
 */
static bool
migr_check_bb_catchup(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, bool have_region,
    off_t reg_offset, struct isi_error **error_out)
{
	off_t new_offset;
	struct isi_error *error = NULL;
	bool finished = true;
	uint64_t catchup_len = 0, send_len = 0;
	off_t max_offset;
	struct work_restart_state *work = pw_ctx->work;

	/* 
	 * Need to do bb hashing if and only if we're
	 * sending any blocks or the file size is changing.
	 */
	if (!pw_ctx->skip_bb_hash && !file->skip_bb_hash &&
	    !file->bb_hash_ctx && file->prev_snap != INVALID_SNAPID &&
	    (have_region || file->size_changed)) {
		file->bb_hash_ctx = hash_alloc(HASH_MD5);
		ASSERT(file->bb_hash_ctx);
		hash_init(file->bb_hash_ctx);
	}

	if (file->bb_hash_ctx) {
		/* We have to hash up to the next region or up to
		 * the end of file if there is no region */
		if (have_region)
			max_offset = reg_offset;
		else if (is_fsplit_work_item(work))
			max_offset = work->f_high;
		else
			max_offset = file->st.st_size;

		if (file->cur_offset != max_offset) {
			/* Send a message to the other side to also get them
			 * started hashing this region.  This should be sent
			 * prior to doing the hash on our side to avoid additive
			 * time penalty. If catchup_len is larger than our
			 * arbitrary size limit, we split the region into chunks
			 * to be processed by each side.
			 */
			catchup_len = max_offset - file->cur_offset;
			send_len = MIN(catchup_len, 5 * IDEAL_READ_SIZE);

			/*
			 * Throttle bb catchup to at most RETURN_VALUE bytes
			 * per second.
			 */
			UFAIL_POINT_CODE(slow_bb_catchup,
			    send_len = MIN(RETURN_VALUE, send_len);
			    siq_nanosleep(1, 0);
			);

			migr_send_data_msg(pw_ctx, file, file->cur_offset,
			    send_len, RT_UNCHANGED, &error);

			/* migr_send_data_msg sends ioerror internally */
			if (error) {
				finished = false;
				goto out;
			}
			new_offset = pw_migr_hash_range_fd(pw_ctx, file,
			    file->cur_offset, send_len, &error);
			/* pw_migr_hash_range_fd sends ioerror internally */
			if (error) {
				finished = false;
				goto out;
			}
			ASSERT(new_offset == file->cur_offset + send_len);

			file->cur_offset = new_offset;
			if (new_offset != max_offset)
				finished = false;
		}
	}
out:
	isi_error_handle(error, error_out);
	return finished;
}

static void
advance_target_offset(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, uint64_t offset,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	uint64_t cur_offset = 0, rem_offset = offset;

	/*
	 * The file could be large enough that the starting offset of a file
	 * split work item is larger than the max size of logical_size in the
	 * FILE_DATA_MSG.
	 */
	while(rem_offset > MAX_DATA_REGION_SIZE) {
		migr_send_data_msg(pw_ctx, file, cur_offset,
		    MAX_DATA_REGION_SIZE, RT_UNCHANGED, &error);

		if (error)
			goto out;

		cur_offset += MAX_DATA_REGION_SIZE;
		rem_offset -= MAX_DATA_REGION_SIZE;
	}

	migr_send_data_msg(pw_ctx, file, cur_offset,
	    rem_offset, RT_UNCHANGED, &error);

out:
	isi_error_handle(error, error_out);
}

bool
migr_continue_file(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct work_restart_state *work = pw_ctx->work;
	off_t reg_offset = 0;
	unsigned reg_len = 0;
	int reg_type = 0;
	bool have_region = false;
	char *hash_val = NULL;
	bool done = false;
	bool hash_finished;
	int bw_limit = 0;
	uint64_t max_reg_len = 0;

	ilog(IL_INFO, "migr_continue_file called");
	ASSERT_IMPLIES(file->cur_offset == 0, file->bb_hash_ctx == NULL);

	/* Sleep measured in milliseconds. */
	UFAIL_POINT_CODE(dont_work_on_file,
	    if (RETURN_VALUE > 0) {
		    siq_nanosleep(0, RETURN_VALUE * 1000000);
	    }
	    return false;
	);

	/*
	 * Skip ADS file based on index specified in the fail point
	 */
	UFAIL_POINT_CODE(skip_adsfile,
		if (pw_ctx->doing_ads &&
		    pw_ctx->ads_dir_state.dir->_buffers->_index == RETURN_VALUE) {
			errno = EIO;
			error = isi_system_error_new(errno,
			    "Failed to read ADS file");
			goto out;
		}
	);

	// Force the worker to exit if failpoint is set
	UFAIL_POINT_CODE(worker_exit_continue_file,
		process_gensig_exit(pw_ctx);
		return false;
	);

	if (file->cur_offset == 0)
		pw_ctx->cur_file_data_sent = 0;

	if (file->st.st_flags & SF_FILE_STUBBED) {
		// Confirm no split on stub file: bug 145266
		ASSERT(!is_fsplit_work_item(pw_ctx->work));
	}

	if (is_fsplit_work_item(pw_ctx->work) && !pw_ctx->doing_ads) {
		// We better be working on the large file
		ASSERT(file->st.st_ino == pw_ctx->work->fsplit_lin);
	}

	if (file->st.st_flags & SF_FILE_STUBBED &&
	    file->cpstate.sync_type == BST_STUB_SYNC && !file->hard_link) {
		if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx) &&
		    !pw_ctx->working_cache_ent) {
			log(TRACE,
			    "%s: Diffsync: No current working entry. Not yet "
			    "ready to send stub data for %llx", __func__,
			    file->st.st_ino);
			return true;
		}
		have_region = !migr_continue_stub_sync(pw_ctx, file, &error);
		goto out;
	}

	/* this is a hard linked file and the data is being sent elsewhere.
	 * just send the file done message */
	if (file->skip_data) {
		log(DEBUG, "skipping data send for hard linked file");
		goto out;
	}

	/* For diff sync, catch a file that's been marked NO_SYNC and finish
	 * the sending */

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
		ASSERT(pw_ctx->cache_size > 0);
		/* A NULL working_cache_ent indicates that we just finished
		 * sending the ADS files of an ADS container. We have to get
		 * back to migr_continue_dir() to start sending the file
		 * data */
		if (!pw_ctx->working_cache_ent) {
			log(TRACE,
			    "%s: No current working entry. Not yet ready to "
			    "send file data", __func__);
			return true;
		}
		log(TRACE, "%s: Have current working entry for %s",
		    __func__, FNAME(pw_ctx->working_cache_ent->md));
		if (pw_ctx->working_cache_ent->sync_state == NO_SYNC) {
			log(TRACE, "%s:   is NO_SYNC. Send DONE_MSG", __func__);
			ASSERT(pw_ctx->working_cache_ent->cache_state ==
			    SENDING);
			done = true;
			goto out;
		}
	}

	switch (file->type) {
	case SIQ_FT_REG:
		/* Find the next sparse or data region */
		have_region = migr_find_next_region(pw_ctx, file,
		    &reg_offset, &reg_len, &reg_type, &error);

		log(TRACE, "%s SIG_FT_REG in:%llx, reg_offset=%ld, reg_len=%u, reg_type=%d",\
			__func__, file->st.st_ino, reg_offset, reg_len, reg_type);

		/* migr_find_next_region sends ioerror internally */
		if (error)
			goto out;

		/* For filesplit work items, only send data in the file
		 * region <= work->f_high
		 */
		if (is_fsplit_work_item(work) && have_region &&
		    !pw_ctx->doing_ads) {
			if (reg_offset >= work->f_high) {
				/* The next available region starts past the
				 * file range */
				have_region = false;
			}
			else if ((reg_offset + reg_len) > work->f_high) {
				/* The next available region extends past the
				 * file range, limit the amount sent
				 * (work->f_high is inclusive) */
				reg_len = work->f_high - reg_offset;
			}
		}

		/*
		 * Enable bb hashing if needed and do catch up hashing for
		 * skipped regions (if any)
		 */
		hash_finished = migr_check_bb_catchup(pw_ctx, file, have_region,
		    reg_offset, &error);
		/* migr_check_bb_catchup sends ioerror internally */
		if (error)
			goto out;

		/*
		 * Partial catchup hash.  Skip out to avoid blocking
		 * excessively without checking for RPC's.
		 */
		if (!hash_finished)
			goto early_out;
		break;

	case SIQ_FT_SYM:
	case SIQ_FT_BLOCK:
	case SIQ_FT_CHAR:
	case SIQ_FT_SOCK:
	case SIQ_FT_FIFO:
		have_region = false;
		break;

	default:
		ASSERT(FALSE);
	}

	/* Create and send a FILE_DATA_MSG with the appropriate data */
	if (!error && have_region) {
		/* First data message for a file split work item? */
		if (is_fsplit_work_item(work) && work->f_low > 0 &&
		    file->cur_offset == work->f_low && !pw_ctx->doing_ads) {
			/* 
			 * Before sending any data, send unchanged region
			 * messages that cause the sworker to move its
			 * file->cur_offset to the correct region.
			 */
			advance_target_offset(pw_ctx, file, work->f_low,
			    &error);
			if (error)
				goto out;
		}

		/* Adjust amount to send based on bandwidth rule. We only
		 * want this message to take MAX_DATA_MSG_SEND_TIME seconds 
		 * to finish sending so we can respond to timeouts/messages
		 * in a reasonable amount of time */
		bw_limit = get_bandwidth();
		if (bw_limit > 0 && reg_type == RT_DATA) {
			/* max bytes to send = (kb/s * max send time * 1000) / 1 byte
			 * Note: bw is measured in kb/s == kiloBITS/s */
			max_reg_len = (bw_limit * MAX_DATA_MSG_SEND_TIME) * 125;
			if (reg_len > max_reg_len)
				reg_len = max_reg_len;
		}

		migr_send_data_msg(pw_ctx, file, reg_offset, reg_len,
		    reg_type, &error);
	}
	/* migr_send_data_msg sends ioerror internally */

out:
	if (error) {
		/* Saw a failure, report it */
		log(TRACE, "%s: error",  __func__);
		if ((is_tw_work_item(pw_ctx) || pw_ctx->doing_ads) &&
		    !pw_ctx->doing_extra_file) {
			migr_send_file_done_msg(pw_ctx, file, false, NULL);
		} else {
			pw_ctx->file_state.hash_str = NULL;
		}
		done = true;
	} else if (!have_region) {
		/* No more regions, no errors, send success */
		if (file->bb_hash_ctx) {
			hash_val = hash_end(file->bb_hash_ctx);
			ASSERT(hash_val);
		}
		log(TRACE, "%s: no more regions", __func__);

		if ((is_tw_work_item(pw_ctx) || pw_ctx->doing_ads) &&
		    !pw_ctx->doing_extra_file) {
			migr_send_file_done_msg(pw_ctx, file, true, hash_val);
		} else {
			if (hash_val)
				pw_ctx->file_state.hash_str = strdup(hash_val);
		}
		if (hash_val)
			free(hash_val);
		done = true;
		siq_track_op_end(SIQ_OP_FILE_DATA);
	}
	if (done) {
		close(file->fd);
		file->fd = -1;
		hash_free(file->bb_hash_ctx);
		file->bb_hash_ctx = NULL;

		/*
		 * The resetting cache information here is safe for now as this
		 * cache is only used by stub file in which file split will not
		 * be possible.
		 */
		file->sd_region.region_type = RT_SPARSE;
		file->sd_region.start_offset = 0;
		file->sd_region.byte_count = 0;

		if (file->cpstate.bsess != NULL) {
			struct isi_error *error2 = NULL;
			isi_file_bsession_destroy(file->cpstate.bsess,
			    (error == NULL),
			    &error2);
			if (error && error2) {
				isi_error_add_context(error, "Also error "
				    "destroying bsession: %s",
				    isi_error_get_detailed_message(error2));
				isi_error_free(error2);
				error2 = NULL;
			} else if (error2) {
				error = error2;
			}
			memset(&file->cpstate, 0, sizeof(file->cpstate));
		}

		if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
			/* No longer have a working cache entry */
			pw_ctx->working_cache_ent = NULL;
		}
	}
	if (have_region)
		file->cur_offset = reg_offset + reg_len;

 early_out:
	isi_error_handle(error, error_out);
	return (done && !error);
}

void
send_user_attrs(struct migr_pworker_ctx *pw_ctx, int fd, bool is_dir,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct generic_msg attr_msg = {};
	struct ifs_userattr_key keys[IFS_USERATTR_MAXKEYS];
	char value[IFS_USERATTR_VAL_MAXSIZE + 1];
	ssize_t len;
	int num_attrs, i, tmp_errno;
	struct stat st;
	char *path = NULL;

	log(TRACE, "send_user_attrs");

	UFAIL_POINT_CODE(skip_user_attrs,
		if (!is_dir || RETURN_VALUE) {
			ON_SYSERR_GOTO(out, EIO, error,
				"Failed to read extended user attributes");
		}
	);

	num_attrs = ifs_userattr_list(fd, keys);
	if (num_attrs == -1) {
		tmp_errno = errno;
		if (fstat(fd, &st) == 0)
			path = get_valid_rep_path(st.st_ino, pw_ctx->cur_snap);
		error = isi_system_error_new(tmp_errno,
		    "Failed to get attribute list%s%s",
		    path ? " for file " : "", path ? path : "");
		free(path);
		goto out;
	}

	if (num_attrs == 0)
		goto out;

	attr_msg.head.type = USER_ATTR_MSG;
	attr_msg.body.user_attr.namespc = NS_USER;
	attr_msg.body.user_attr.encoding = ENC_DEFAULT;
	attr_msg.body.user_attr.is_dir = is_dir;

	for (i = 0; i < num_attrs; i++) {
		len = ifs_userattr_get(fd, keys[i].key, &value, sizeof(value));
		if (len == -1) {
			tmp_errno = errno;
			if (fstat(fd, &st) == 0)
				path = get_valid_rep_path(st.st_ino,
				    pw_ctx->cur_snap);
			error = isi_system_error_new(tmp_errno,
			    "Failed to get attribute value for key %s%s%s",
			    keys[i].key, path ? " of file " : "",
			    path ? path : "");
			free(path);
			goto out;
		}

		attr_msg.body.user_attr.keylen = strlen(keys[i].key) + 1;
		attr_msg.body.user_attr.key = keys[i].key;
		attr_msg.body.user_attr.vallen = len + 1;
		value[len] = 0;
		attr_msg.body.user_attr.val = value;

		msg_send(pw_ctx->sworker, &attr_msg);
	}

out:
	isi_error_handle(error, error_out);
}

// This worm state overrides the worm state sent by FILE_BEGIN_MSG/
// LIN_UPDATE_MSG, which has only a 32-bit retention date.
void
send_worm_state(struct migr_pworker_ctx *pw_ctx, struct worm_state *worm)
{
	struct generic_msg msg = {};

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->curr_ver & FLAG_VER_3_5);

	msg.head.type = WORM_STATE_MSG;
	msg.body.worm_state.committed = worm->w_committed;
	msg.body.worm_state.retention_date = worm->w_retention_date;

	msg_send(pw_ctx->sworker, &msg);
}

static void
send_cloudpools_setup(struct migr_pworker_ctx *pw_ctx,
    enum isi_cbm_backup_sync_type sync_type)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);

	if (!(pw_ctx->curr_ver & FLAG_VER_CLOUDPOOLS) ||
	    pw_ctx->job_ver.common < MSG_VERSION_RIPTIDE)
		return;

	msg.head.type = build_ript_msg_type(CLOUDPOOLS_SETUP_MSG);
	ript_msg_init(&msg.body.ript);

	ript_msg_set_field_uint8(&msg.body.ript, RMF_CPOOLS_SYNC_TYPE, 1,
	    (uint8_t)sync_type);

	msg_send(pw_ctx->sworker, &msg);
}

/*
 * If the file is a regular file, get its sync_type and store it in
 * file->cpstate.sync_type. If it is BST_DEEP_COPY, remove SF_FILE_STUB_FLAGS
 * from the flags parameter as legacy code does not expect to process these.
 * See code comments for additional details.
 */
void
set_cloudpools_sync_type(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_file_state *file, enum file_type type, struct stat *st,
    unsigned *flags, struct isi_error **error_out)
{
	bool cpse_found = false;
	struct cpss_entry old_cpse = {};

	struct migr_cpools_file_state *cpf = &file->cpstate;
	struct isi_error *error = NULL;

	ASSERT(pw_ctx != NULL && file != NULL && st != NULL);

	if (type != SIQ_FT_REG)
		return;

	UFAIL_POINT_CODE(fake_stub_file, st->st_flags |= SF_FILE_STUBBED);

	if (!(pw_ctx->curr_ver & FLAG_VER_CLOUDPOOLS) &&
	    (st->st_flags & SF_FILE_STUBBED)) {
		error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
		    "The target does not support Cloudpools");
		goto out;
	}

	if (!is_treewalk(pw_ctx)) {
		cpse_found = get_cpss_entry(st->st_ino, &old_cpse,
		    pw_ctx->chg_ctx->cpss1, &error);
		if (error)
			goto out;
	}

	ASSERT(cpse_found || old_cpse.sync_type == BST_NONE);

	cpf->sync_type = isi_cbm_backup_sync_type_get(fd, old_cpse.sync_type,
	    pw_ctx->common_cbm_file_vers, pw_ctx->deep_copy, &error);
	if (error)
		goto out;

	log(TRACE, "%llx cpools sync type: %d", st->st_ino, cpf->sync_type);

	if (cpf->sync_type == BST_NONE) {
		error = isi_siq_error_new(E_SIQ_CLOUDPOOLS,
		    "Could not sync stub file %llx", st->st_ino);
		goto out;
	} else if (cpf->sync_type == BST_DEEP_COPY) {
		/* Remove the SF_FILE_STUBBED and SF_CACHED_STUB flags since
		 * they are defined in UF_IFSFLAGS as flags that are not set
		 * via bam_setattr() as of Riptide and legacy code does not
		 * expect these for deep copy. */
		*flags &= ~SF_FILE_STUB_FLAGS;
	}

out:
	isi_error_handle(error, error_out);
}

void
do_cloudpools_setup(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_file_state *file, enum file_type type, struct stat *st,
    struct isi_error **error_out)
{
	struct cpss_entry cpse = {};
	struct isi_file_bsession_opt bso = {};

	struct migr_cpools_file_state *cpf = &file->cpstate;
	struct isi_error *error = NULL;

	ASSERT(pw_ctx != NULL && file != NULL && st != NULL);

	if (type != SIQ_FT_REG)
		return;

	ASSERT(cpf->sync_type != BST_NONE);
	cpse.sync_type = cpf->sync_type;

	isi_file_bsession_opt_init(&bso);

	if (st->st_flags & SF_FILE_STUBBED) {
		cpss_log_add_entry(pw_ctx, pw_ctx->cur_cpss_ctx, st->st_ino,
		    &cpse, NULL, true, &error);
		if (error)
			goto out;

		bso.sbo = cpf->sync_type == BST_DEEP_COPY ?
		    SBO_DATA_ONLY : SBO_STUB_ONLY;
		if (cpf->sync_type == BST_STUB_SYNC)
			bso.region_types = ISI_CPOOL_CACHE_MASK_DIRTY |
			    ISI_CPOOL_CACHE_MASK_CACHED;

		bso.bk_type = BT_SHORT_TERM;

		cpf->mbytes_pb = cpf->dbytes_pb = pw_ctx->read_size;

		cpf->bsess = isi_file_bsession_create(fd, NULL, &bso,
		    &cpf->mbytes_pb, &cpf->nmblobs,
		    &cpf->dbytes_pb, &cpf->ndblobs,
			    &error);
		if (error)
			goto out;

		cpf->mblobs_sent = cpf->dblobs_sent = 0;
	}

	// Send cloudpools setup all the time. The target uses the sync type
	// from this message to determine whether or not to call
	// isi_cbm_purge().
	if (!file->hard_link)
		send_cloudpools_setup(pw_ctx, cpf->sync_type);

	if (st->st_flags & SF_FILE_STUBBED) {
		if (cpf->sync_type == BST_STUB_SYNC) {
			//XXXDPL Do we even really need to do this, since
			// migr_continue_file() will bail early for stub sync?
			if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
				pw_ctx->latest_cache_ent->fstate.skip_data =
				    true;
			else
				file->skip_data = true;
		}

		if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
			//XXXDPL Can pw_ctx->latest_cache_ent change between
			// ds_start_file() and here?
			pw_ctx->latest_cache_ent->fstate.cpstate = *cpf;
		}
	}
out:
	isi_error_handle(error, error_out);
}

bool
migr_start_file(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_dirent *md, struct migr_file_state *file,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	bool send_ads = false;
	bool is_ads = false;
	char sym_target[MAXPATHLEN + 1];
	enc_t sym_enc = ENC_DEFAULT;
	int ret;
	struct dirent *d;
	struct stat *st;
	struct worm_state worm = {};
	struct siq_stat *tw_stats = pw_ctx->tw_cur_stats;
	enum ifs_security_info secinfo = 0;
	bool have_worm = false;

	ASSERT(file->type != SIQ_FT_UNKNOWN);

	siq_track_op_beg(SIQ_OP_FILE_DATA);
	d = md->dirent;
	st = md->stat;
	log(TRACE, "%s: %s %s fd %d", __func__, d->d_name,
	    file_type_to_name(file->type), fd);

	is_ads = st->st_flags & UF_ADS;

	msg.head.type = FILE_BEGIN_MSG;
	msg.body.file_begin_msg.file_type = file->type;

	switch (file->type) {
	case SIQ_FT_REG:
		//Overload the use of symlink to help with bulk lin updates
		if (st->st_nlink == 1 && pw_ctx->lin_map_blk) {
			log(DEBUG, "%s: Setting NHL flag on %s", 
			    __func__, file->name);
			msg.body.file_begin_msg.symlink = NHL_FLAG;
		} else {
			msg.body.file_begin_msg.symlink = "";
		}
		msg.body.file_begin_msg.senc = 0;

		if (file->prev_snap != INVALID_SNAPID) {
			/* Should never be partial transfer for diff sync */
			ASSERT(!pw_ctx->doing_diff_sync);
			log(TRACE, "%s: Not a full transfer", __func__);
			msg.body.file_begin_msg.full_transfer =
			    pw_ctx->full_transfer;
			if (!is_ads)
				tw_stats->files->replicated->updated_files++;
		} else {
			if (!file->skip_data)
				log(TRACE, "%s: full transfer", __func__);
			msg.body.file_begin_msg.full_transfer = 1;
			if (!is_ads && !DIFF_SYNC_BUT_NOT_ADS(pw_ctx) &&
			    !is_fsplit_work_item(pw_ctx->work))
				tw_stats->files->replicated->new_files++;
		}
		break;

	case SIQ_FT_SYM:
		ret = enc_readlinkat(AT_FDCWD, d->d_name,
		    d->d_encoding, sym_target, MAXPATHLEN, &sym_enc);
		if (ret == -1) {
			error = isi_ioerror_and_system_error(pw_ctx, errno,
			    d->d_name, d->d_encoding, st->st_ino,
			    "symlink read", "Failed to read symlink");
			goto out;
		}
		sym_target[ret] = 0;
		msg.body.file_begin_msg.symlink = sym_target;
		msg.body.file_begin_msg.senc = sym_enc;
		msg.body.file_begin_msg.full_transfer = 1;
		if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
			tw_stats->files->replicated->new_files++;
		tw_stats->files->replicated->symlinks++;
		break;

	case SIQ_FT_BLOCK:
	case SIQ_FT_CHAR:
		sprintf(sym_target, "%u %u",
		    major(st->st_rdev), minor(st->st_rdev));
		msg.body.file_begin_msg.symlink = sym_target;
		msg.body.file_begin_msg.full_transfer = 1;
		if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
			tw_stats->files->replicated->new_files++;
		if (file->type == SIQ_FT_CHAR)
			tw_stats->files->replicated->char_specs++;
		else
			tw_stats->files->replicated->block_specs++;
		break;

	case SIQ_FT_SOCK:
		msg.body.file_begin_msg.symlink = "";
		msg.body.file_begin_msg.full_transfer = 1;
		tw_stats->files->replicated->sockets++;
		if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
			tw_stats->files->replicated->new_files++;
		break;

	case SIQ_FT_FIFO:
		msg.body.file_begin_msg.symlink = "";
		msg.body.file_begin_msg.full_transfer = 1;
		tw_stats->files->replicated->fifos++;
		if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
			tw_stats->files->replicated->new_files++;
		break;

	default:
		log(ERROR, "unsupported type");
		goto out;
	}

	msg.body.file_begin_msg.src_lin = pw_ctx->filelin;
	msg.body.file_begin_msg.di_flags = (unsigned)st->st_flags;

	msg.body.file_begin_msg.src_dlin = pw_ctx->dir_lin;
	msg.body.file_begin_msg.src_dkey = md->cookie;

	/*
	 * Bug 152146
	 * check for an ACL in all cases
	 */
	if (st->st_flags & (SF_HASNTFSACL | SF_HASNTFSOG)) {
		secinfo = IFS_SEC_INFO_OWNER | IFS_SEC_INFO_GROUP;
		if (st->st_flags & SF_HASNTFSACL)
			secinfo |=
			    IFS_SEC_INFO_DACL | IFS_SEC_INFO_SACL;

		msg.body.file_begin_msg.acl =
		    get_sd_text_secinfo(fd, secinfo);
		if (msg.body.file_begin_msg.acl  == NULL) {
			error = isi_ioerror_and_system_error(pw_ctx,
			    errno, d->d_name, d->d_encoding,
			    st->st_ino, "acl read",
			    "Failed to read acl");
			goto out;
		}
	} else
		msg.body.file_begin_msg.acl = get_file_acl(fd);

	msg.body.file_begin_msg.fenc = d->d_encoding;

	msg.body.file_begin_msg.mode = st->st_mode;
	msg.body.file_begin_msg.size = st->st_size;
	msg.body.file_begin_msg.atime_sec = st->st_atimespec.tv_sec;
	msg.body.file_begin_msg.atime_nsec = st->st_atimespec.tv_nsec;
	msg.body.file_begin_msg.mtime_sec = st->st_mtimespec.tv_sec;
	msg.body.file_begin_msg.mtime_nsec = st->st_mtimespec.tv_nsec;
	msg.body.file_begin_msg.fname = (char *)d->d_name;
	msg.body.file_begin_msg.uid = (unsigned)st->st_uid;
	msg.body.file_begin_msg.gid = (unsigned)st->st_gid;
	if (!is_ads) {
		msg.body.file_begin_msg.prev_cookie =
		    pw_ctx->dir_state.dir->prev_sent_cookie;
	}

	if (!(pw_ctx->curr_ver & FLAG_VER_CLOUDPOOLS) &&
	    (st->st_flags & SF_FILE_STUBBED)) {
		error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
		    "The target does not support Cloudpools");
		goto out;
	}

	/* This can modify the msg flags on deep copy. */
	set_cloudpools_sync_type(pw_ctx, fd, file, file->type, st,
	    &msg.body.file_begin_msg.di_flags, &error);
	if (error)
		goto out;

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
		ds_start_file(pw_ctx, fd, md, file, &msg);
	} else {
		file->fd = fd;
		file->st = *st;
		ASSERT_IMPLIES(!is_fsplit_work_item(pw_ctx->work),
		    pw_ctx->work->f_low == 0);
		file->cur_offset = pw_ctx->doing_ads ? 0 : pw_ctx->work->f_low;
		log(TRACE, "%s: file->name=%s enc=%d",
		    __func__, FNAME(md), md->dirent->d_encoding);
		if (file->name)
			free(file->name);
		file->name = strdup(md->dirent->d_name);
		file->enc = md->dirent->d_encoding;
		file->cookie = md->cookie;

		/* Sworker must be told to skip bb hash for hard links */
		if (file->skip_data) {
			//XXXDPL for stubs skip_data also depends on whether
			// stub sync or not. Need to figure this out!
			file->skip_bb_hash = true;
			msg.body.file_begin_msg.skip_bb_hash = true;
		} else
			file->skip_bb_hash = pw_ctx->skip_bb_hash;
	}

	have_worm = get_worm_state(st->st_ino, pw_ctx->cur_snap, &worm, NULL,
	    &error);
	if (error)
		goto out;

	if (have_worm && !(pw_ctx->curr_ver & FLAG_VER_3_5)) {
		msg.body.file_begin_msg.worm_committed =
		    (unsigned)worm.w_committed;

		// This results in a loss of precision for 64-bit retention
		// dates. WORM_STATE_MSG corrects this for targets that support
		// Mavericks and later. For earlier targets, if the date is
		// later than MAX_INT32, set it to MAX_INT32-- the latest we
		// can safely set it to.
		msg.body.file_begin_msg.worm_retention_date =
		    worm.w_retention_date <= 0x7FFFFFFF ?
		    (unsigned)worm.w_retention_date : 0x7FFFFFFF;
	}

	/* User attrs (if any) must immediately follow the file begin message
	 * for diff sync to work properly */
	ret = msg_send(pw_ctx->sworker, &msg);
	send_user_attrs(pw_ctx, fd, false, &error);
	if (error) {
		/*
		 * Bug 252620
		 *
		 * If we fail to read extended attrs due to a corrupted extension
		 * block, log corruption and continue so we have a chance to send
		 * the rest of the file if only the extended attributes are
		 * unreadable. No USER_ATTR_MSG is sent if there are no
		 * attributes, so continuing here is equivalent to handling a
		 * file with no attributes.
		 */
		if (pw_ctx->expected_dataloss) {
			char filename[MAXPATHLEN];
			isi_error_free(error);
			error = NULL;
			snprintf(filename, sizeof(filename), "/%s", d->d_name);
			log_corrupt_file_entry(pw_ctx, filename);
		}
		else {
			goto out;
		}
	}

	do_cloudpools_setup(pw_ctx, fd, file, file->type, st, &error);
	if (error)
		goto out;

	if (have_worm && (pw_ctx->curr_ver & FLAG_VER_3_5)) {
		if (!worm.w_committed || st->st_nlink == 1)
			send_worm_state(pw_ctx, &worm);
		else {
			set_hash_exception(pw_ctx, st->st_ino, &error);
			if (error)
				goto out;
		}
	}

	/* Update previous cookie for upgrade */
	if (!is_ads) {
		log(TRACE, "migr_start_file: sent %llx-%llx", 
		    pw_ctx->dir_state.dir->prev_sent_cookie, md->cookie);
		migr_dir_set_prev_sent_cookie(pw_ctx->dir_state.dir);
	}

	/* XXXJDH: This check has to match the conditions of sending an ACK */
	if (!(st->st_flags & UF_ADS)) {
		pw_ctx->outstanding_acks++;
		pw_ctx->tw_sending_file = true;
	}

	if (msg.body.file_begin_msg.acl) {
		free(msg.body.file_begin_msg.acl);
		msg.body.file_begin_msg.acl = NULL;
	}

	if (file->type == SIQ_FT_REG && !file->skip_data) {
		send_ads = migr_start_ads(pw_ctx, fd, st, d->d_name,
		    d->d_encoding, &error);
		/* migr_start_ads sends ioerror internally */
		if (error) {
			/*
			 * If fail to fstat or chdir ADS directory,
			 * Set send_ads as false so that we can continue
			 * with the parent file.
			 * Setting send_ads = false will allow caller
			 * to set next function as MIGR_CONTINUE_FILE.
			 * Log ADS directory path.
			 */
			if (pw_ctx->expected_dataloss) {
				char filename[MAXNAMELEN];
				isi_error_free(error);
				error = NULL;
				file->fd = -1;
				send_ads = false;
				sprintf(filename, "/%s:", d->d_name);
				log_corrupt_file_entry (pw_ctx, filename);
			}
			goto out;
		}
		if (send_ads)
			tw_stats->files->replicated->files_with_ads++;
	} else if (file->hard_link)
		tw_stats->files->replicated->hard_links++;

out:
	if (error)
		file->fd = -1;
	isi_error_handle(error, error_out);
	return send_ads;
}

static bool
skip_cpools_ads(struct migr_pworker_ctx *pw_ctx, struct dirent *de,
    struct stat *st, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool res = false;

	int adsparent_fd = -1;
	ifs_lin_t adsparent_lin = 0;
	bool found = false;
	struct stat ds_adsparent_st = {};
	struct stat *adsparent_st = NULL;

	if (!((st->st_flags & UF_ADS) &&
	    isi_file_eelem_exists(IFT_CLOUD, IFET_ADS, de->d_name)))
		goto out;

	//XXXDPL Is there an easier way to get the ADS parent in diff sync?
	if (pw_ctx->doing_diff_sync) {
		found = get_dir_parent(pw_ctx->ads_dir_state.st.st_ino,
		    pw_ctx->ads_dir_state.st.st_snapid, &adsparent_lin,
		    false, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(ENOENT,
			    "%s Unable to find ADS parent for %llx!",
			    __func__, pw_ctx->ads_dir_state.st.st_ino);
			goto out;
		}

		adsparent_fd = ifs_lin_open(adsparent_lin,
		    pw_ctx->ads_dir_state.st.st_snapid, O_RDONLY);
		if (adsparent_fd < 0) {
			error = isi_system_error_new(errno,
			    "%s could not open ADS parent for %llx",
			    __func__, st->st_ino);
			goto out;
		}

		if (fstat(adsparent_fd, &ds_adsparent_st) != 0) {
			error = isi_system_error_new(errno,
			    "%s could not stat ADS parent for %llx",
			    __func__, st->st_ino);
			goto out;
		}

		adsparent_st = &ds_adsparent_st;
	} else {
		ASSERT(pw_ctx->file_state.fd > 0);
		adsparent_st = &(pw_ctx->file_state.st);
	}

	ASSERT(adsparent_st != NULL);

	if (adsparent_st->st_flags & SF_FILE_STUBBED)
		res = true;
out:
	if (adsparent_fd > 0)
		close(adsparent_fd);

	isi_error_handle(error, error_out);
	return res;
}


static bool
should_file_sync(struct migr_pworker_ctx *pw_ctx, struct dirent *de,
    struct stat *st, int fd, bool bit, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool res = false;
	bool skip_cpools = false;

	/* If the target does not support FLAG_VER_CLOUDPOOLS, then fail the
	 * sync if we encounter a stub file */
	if (!(pw_ctx->curr_ver & FLAG_VER_CLOUDPOOLS) &&
	    (pw_ctx->file_state.st.st_flags & SF_FILE_STUBBED)) {
		error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
		    "The target does not support Cloudpools");
		goto out;
	}

	/* diff sync does it's own determining of whether a file should be
	 * synced or not through asynchronous callbacks */
	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
		res = true;
		goto out;
	}

	skip_cpools = skip_cpools_ads(pw_ctx, de, st, &error);
	if (error)
	       goto out;
	if (skip_cpools) {
		log(DEBUG, "Skipping ADS file %s per cloudpools policy.",
		    de->d_name);
		goto out;
	}

	if (bit) {
		/* We need to sync all special files during stf upgrade */
		if (pw_ctx->stf_upgrade_sync && supported_special_file(st))
			res = true;
		else
			res = (pw_ctx->initial_sync && !pw_ctx->snapop);
		goto out;
	}
	res = true;

out:
	isi_error_handle(error, error_out);
	return res;
}

enum migr_dir_result
migr_handle_generic_dir(struct migr_pworker_ctx *pw_ctx, 
    struct migr_dir_state *dir, struct migr_dirent *md, 
    migr_pworker_func_t start_work_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool success;
	bool need_unref = true;
	bool is_new_dir;
	struct generic_msg lin_map_blk_msg = {};

	is_new_dir = dir->dir->is_new_dir;

	/*
	 * If we hit a dir while we haven't finished processing this
	 * contiguous run of files (known by having outstanding ACKS),
	 * we need to unread the dir (so any directory splitting can
	 * still occur with this dir), then go back in the event loop
	 * so the files that haven't finished can be completed.
	 */
	if (pw_ctx->outstanding_acks != 0) {
		migr_dir_unread(dir->dir);
		need_unref = false;

		pw_ctx->wait_for_acks = true;

		/* 
		 * If not doing diff sync (which uses its own
		 * wait mechanism), waiting is needed until all the
		 * ACKs come back.  The ack_callback will restart this
		 * continue_dir path to eventually get to the dir we
		 * just pushed back to the treewalker.
		 */
		if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
			migr_wait_for_response(pw_ctx,
			    MIGR_WAIT_ACK_DOWN);
			siq_track_op_beg(SIQ_OP_FILE_ACKS);
		}


		/* Send lin map checkpoint message to target */
		if (pw_ctx->lin_map_blk) {
			lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
			msg_send(pw_ctx->sworker, &lin_map_blk_msg);
		}
		
		flush_burst_buffer(pw_ctx);

		log(TRACE, "%s: Reached sub directory. %d outstanding "
		    "ACKS", __func__, pw_ctx->outstanding_acks);
		goto out;
	}

	/* 
	 * If push fails, this subdir can be skipped since it is
	 * excluded and doesn't have any included subdirectories
	 */
	success = migr_tw_push(&pw_ctx->treewalk, md, &error);

	if (error) {
		isi_ioerror_only(pw_ctx, error, NULL, 0, 0,
		    "migr_dir_push");
		log(ERROR, "%s: Error pushing %s. cwd=%s", __func__,
		    FNAME(md), pw_ctx->cwd.path);
		goto out;
	}

	if (success) {
		struct migr_dir *new_dir;
		new_dir = migr_tw_get_dir(&pw_ctx->treewalk, &error);
		if (error) {
			isi_ioerror_only(pw_ctx, error, NULL, 0, 0,
			    "migr_tw_get_dir");
			/* XXX: we want this?? */
			goto out;
		}
		new_dir->parent_is_new_dir = is_new_dir;
		new_dir->is_new_dir = is_new_dir;

		siq_track_op_end_nocount(SIQ_OP_DIR_FINISH);
		/* 
		 * To keep restart state consistent we need to set the
		 * checkpoint beyond the dir we just pushed, otherwise
		 * a restart would pick up in the middle of the
		 * child directory we just pushed, eventually finish
		 * and re-examining this directory and be restarting
		 * before this directory. 
		 *
		 * It's also possible to set the checkpoint here and
		 * not skip anything since we've already asserted a
		 * synchronous state before doing the push.
		 */
		migr_dir_set_checkpoint(dir->dir, md->cookie);
		pw_ctx->last_chkpt_time = time(NULL);
		/*
		 * Set prev sent cookie for pushed directory since
		 * we go through it , so the parent does not need to
		 * look for abandoned files in pushed directory.
		 */
		migr_send_dir_range_update(pw_ctx, md->cookie - 1);
		migr_dir_set_prev_sent_cookie(dir->dir);

		migr_dir_unref(dir->dir, md);
		dir->dir = NULL;
		md = NULL;
		need_unref = false; /* we had to do it here instead */
		migr_call_next(pw_ctx, start_work_func);
	}

out:
	if (need_unref && md && dir->dir)
		migr_dir_unref(dir->dir, md);

	isi_error_handle(error, error_out);
	return MIGR_CONTINUE_DIR;
}

static enum migr_dir_result
migr_handle_dir(struct migr_pworker_ctx *pw_ctx, struct migr_dir_state *dir,
    struct migr_dirent *md, struct isi_error **error_out)
{
	return migr_handle_generic_dir(pw_ctx, dir, md, migr_start_work,
	    error_out);
}

static void
hard_linked_file(struct migr_pworker_ctx *pw_ctx, struct stat *st,
    uint64_t dir_lin, uint64_t dir_offset, struct migr_file_state *fstate,
    struct isi_error **error_out)
{
	struct rep_entry entry = {}, old_entry;
	struct isi_error *error = NULL;
	struct rep_ctx *rep = pw_ctx->cur_rep_ctx;
	struct work_restart_state *work = pw_ctx->work;
	bool found;
	bool success;
	char *tw_data = NULL;
	size_t tw_size = 0;
	ifs_lin_t saved_lin;
	uint64_t saved_cookie;
	uint64_t lin = st->st_ino;
	int ret = 0;
	struct worm_state worm = {};

	log(TRACE, "hard_linked_file");

	/*
	 * We need to flush repstate log since the set_entry calls
	 * in this function are dependent on the immediate results
	 * to go any further.
	 */
	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	/* try to claim sending responsibility*/
	entry.lcount = 1;
	entry.hl_dir_lin = dir_lin;
	entry.hl_dir_offset = dir_offset;

	/* Check for WORM committed file. If WORM committed don't send file
	   data as part of treewalk phase. Instead we (later) set the exception
	   bit and send the data and WORM state as part of the lin hash
	   exception phase. Bug 96326. */
	ret = dom_get_info_by_lin(lin, st->st_snapid, NULL, NULL, &worm);
	if (ret) {
		error = isi_system_error_new(errno,
		    "Could not get WORM state for hard linked LIN %llx", lin);
		goto out;
	}


	success = set_entry_cond(lin, &entry, rep, true, NULL, 0,
	    NULL, NULL, 0, &error);
	if (error)
		goto out;

	if (success) {
		/* if the entry was added successfully, responsibility for
		 * sending the file data is bound to this directory lin and
		 * offset (cookie). if a worker dies before the file is sent,
		 * another worker will pick up the item and resend the data
		 * when it reaches dir/offset */
		fstate->hard_link = worm.w_committed;
		log(DEBUG, "hard linked file (%s) at dir_lin %llx "
		    "dir_offset %llu", fstate->hard_link ? "full send" :
		    "skip - WORM committed", dir_lin, dir_offset);
		goto out;
	}

	/* if the conditional set failed, an entry has already been added for
	 * this lin. see if the current dir lin and offset is responsible for
	 * sending the file (we recovered a work item from a worker that had
	 * previously claimed responsibility at this location) */
	do {
		found = get_entry(lin, &entry, rep, &error);
		if (error)
			goto out;
		ASSERT(found == true);
		if (entry.hl_dir_lin == dir_lin &&
		    entry.hl_dir_offset == dir_offset) {
			/* we are going resend the file */
			fstate->hard_link = false;
			log(DEBUG, "hard linked file (full resend) at dir_lin "
			    "%llx dir_offset %llu", dir_lin, dir_offset);
			goto out;
		}

		/* we are not responsible for sending the file data. update
		 * the link count and only send file info */
		fstate->hard_link = true;
		old_entry = entry;
		entry.lcount++;
	
		/*
		 * If there is already checkpoint for this directory, then
		 * make sure we dont add link counts for dirents already
		 * covered.
		 */	
		if ((work->lin == dir_lin) &&
		    (work->dircookie1 >= dir_offset)) {
			log(TRACE, "Skip repstate link increment due to"
			    "restart, dir lin %llx dirent %llu",
			    dir_lin, dir_offset);
			goto out;
		}


		saved_lin = work->lin;
		saved_cookie = work->dircookie1;
		work->lin = dir_lin;
		work->dircookie1 = dir_offset;
		tw_data = migr_tw_serialize(&pw_ctx->treewalk, &tw_size);
		success = set_entry_cond(lin, &entry, rep, false, &old_entry,
		    pw_ctx->wi_lin, work, tw_data, tw_size, &error);
		if (error)
			goto out;
		if (!success) {
			/* Reset all the values */
			if (tw_data)
				free(tw_data);
			tw_data = NULL;
			work->lin = saved_lin;
			work->dircookie1 = saved_cookie;
		}
	} while (!success);

	log(DEBUG, "hard linked file (skipping data) at dir_lin %llx "
	    "dir_offset %llu", dir_lin, dir_offset);	    

out:
	fstate->skip_data = fstate->hard_link;
	if (tw_data)
		free(tw_data);

	isi_error_handle(error, error_out);
}

static void
send_lin(struct migr_pworker_ctx *pw_ctx, char *fname, enc_t enc,
    uint64_t slin, uint64_t cur_cookie, uint64_t prev_cookie)
{
	struct generic_msg m = {};

	log(DEBUG, "sending lin map message for file %s enc %d slin %llx",
	    fname, enc, slin);

	m.head.type = LIN_MAP_MSG;
	m.body.lin_map.name = fname;
	m.body.lin_map.enc = enc;
	m.body.lin_map.src_lin = slin;
	m.body.lin_map.cur_cookie = cur_cookie;
	m.body.lin_map.prev_cookie = prev_cookie;

	if (pw_ctx->lin_map_blk)
		pw_ctx->outstanding_acks++;

	msg_send(pw_ctx->sworker, &m);
}

static enum migr_dir_result
migr_handle_file(struct migr_pworker_ctx *pw_ctx, struct migr_dir_state *dir,
    struct migr_dirent *md, enum file_type type, struct migr_file_state *file,
    siq_plan_t *plan, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	struct rep_entry entry = {};
	struct dirent *de;
	bool need_unref = true;
	bool is_ads = false;
	bool send_ads = false;
	bool bit = false;
	bool size_changed = false;
	bool multiply_linked = false;
	bool sync_file = false;
	bool chkpt_time;
	int fd = -1;
	int sel;
	bool found, success;
	char *tw_data = NULL;
	size_t tw_size = 0;
	enum migr_dir_result result = MIGR_CONTINUE_DIR;
	ifs_snapid_t prev_snap = INVALID_SNAPID;
	struct rep_entry old_entry, *oldp;
	struct rep_entry new_entry = {};
	struct generic_msg lin_map_blk_msg = {};
	struct work_restart_state *work = pw_ctx->work;

	log(DEBUG, "%s: %llx (%s)", __func__, md->cookie, md->dirent->d_name);

	if (!dir->dir->selected)
		goto out;

	/*
	 * If we have outstanding acks and the checkpoint interval has been
	 * reached, we need to unread the file and force the target to dump
	 * its acks so we can get to 0 outstanding acks. Once that happens,
	 * we will go back into the event loop, re-read this file, and
	 * set the checkpoint to the last successfully sent file.
	 */
	chkpt_time = (bool)(time(NULL) - pw_ctx->last_chkpt_time >
	    pw_ctx->chkpt_interval);
	if (pw_ctx->outstanding_acks != 0 && chkpt_time &&
	    !pw_ctx->doing_ads && !pw_ctx->doing_extra_file) {

		/* Push this entry back into the queue of entries for the
		 * current dir. It will be read again once we're done
		 * waiting for acks. */
		migr_dir_unread(dir->dir);
		need_unref = false;
		pw_ctx->wait_for_acks = true;

		/*
		 * If not doing diff sync (which uses its own
		 * wait mechanism), waiting is needed until all the
		 * ACKs come back.  The ack_callback will restart this
		 * continue_dir path to eventually get to the entry we
		 * just pushed back to the treewalker.
		 */
		if (!pw_ctx->doing_diff_sync) {
			migr_wait_for_response(pw_ctx,
			    MIGR_WAIT_ACK_DOWN);
			siq_track_op_beg(SIQ_OP_FILE_ACKS);
		}


		/* Send lin map checkpoint message to target */
		if (pw_ctx->lin_map_blk) {
			lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
			msg_send(pw_ctx->sworker, &lin_map_blk_msg);
		}

		flush_burst_buffer(pw_ctx);

		log(TRACE, "%s: Checkpointing with %d outstanding "
		    "ACKS", __func__, pw_ctx->outstanding_acks);
		goto out;
	}

	/* Either we forced the sworker to dump ACKs, or we got lucky with a
	   random bulk lin ack. Either way, time to checkpoint. */
	if (pw_ctx->outstanding_acks == 0 && pw_ctx->lin_map_blk &&
	    dir->dir->prev_sent_cookie > dir->dir->_slice.begin &&
	    dir->dir->prev_sent_cookie > dir->dir->_checkpoint) {
		migr_dir_set_checkpoint(dir->dir, dir->dir->prev_sent_cookie);
		pw_ctx->last_chkpt_time = time(NULL);
	}

	de = md->dirent;
	file->type = type;

	/* Delay if a files per second limit is active */
	throttle_delay(pw_ctx);

	/* Open and fstat the file. Use ifs_lin_open for devices/sockets */
	switch (type) {
	case SIQ_FT_REG:
	case SIQ_FT_SYM:
		fd = enc_openat(AT_FDCWD, de->d_name, de->d_encoding,
		    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
		break;

	case SIQ_FT_BLOCK:
	case SIQ_FT_CHAR:
	case SIQ_FT_SOCK:
	case SIQ_FT_FIFO:
		fd = ifs_lin_open(de->d_fileno, de->d_snapid, O_RDONLY);
		break;

	default:
		goto out;
	}

	if (fd == -1) {
		error = isi_ioerror_and_system_error(pw_ctx, errno,
		    de->d_name, de->d_encoding, de->d_ino,
		    "open", "File is missing or unreadable");
		goto out;
	}

	/*
	 * Sanity check.  If this is zero, it indicates a bad fd operation
	 * sometime in the past since we never expect 0 to be closed.
	 */
	ASSERT(fd != 0);

	/* Hack to keep from leaking too many FDs.  See bug 38163. */
	ASSERT(fd < 1000);

	if (fstat(fd, &st) < 0) {
		error = isi_system_error_new(errno, "Failed to stat file %s ",
		    de->d_name);
		goto out;
	}

	/* check file for selection based on default and predicate criteria */
	sel = siq_select(plan, de->d_name, de->d_encoding, &st,
	    pw_ctx->new_time, pw_ctx->cwd.utf8path, type, &error);
	if (error) {
		isi_ioerror_only(pw_ctx, error, de->d_name, de->d_encoding,
		    de->d_ino, "siq_select");
		goto out;
	} else if (sel == 0) {
		log(DEBUG, "Skipping file for selection: %s/%s",
		    pw_ctx->cwd.path, de->d_name);

		/*
		 * We can skip setting rep entries if its not stf sync
		 * or the file is ADS entry
		 */		
		if ((!pw_ctx->stf_sync && !pw_ctx->stf_upgrade_sync) || is_ads)
			goto out;

		if (st.st_nlink == 1) {
			new_entry.lcount = 1;
			new_entry.excl_lcount = 1;
			stf_rep_log_add_entry(pw_ctx,
			    pw_ctx->cur_rep_ctx, st.st_ino, &new_entry, NULL,
			    true, true, &error);
			goto out;
		}
		/*
		 * If there is already checkpoint for this directory, 
		 * make sure we dont add link counts for dirents
		 * already covered.
		 */	
		if ((work->lin == dir->dir->_stat.st_ino) &&
		    (work->dircookie1 >= md->cookie)) {
			log(TRACE, "Skip repstate link increment due to"
			    "restart, dir lin %llx dirent %llu",
			    st.st_ino, md->cookie);
			goto out;
		}
		/*
		 * We need to bump excl_count in rep entry
		 * for this lin.
		 */
		do {
			found = get_entry(st.st_ino, &old_entry,
			    pw_ctx->cur_rep_ctx, &error);
			if (error)
				goto out;
			if (found) {
				new_entry = old_entry;
				oldp = &old_entry;
				new_entry.excl_lcount++;
				new_entry.lcount++;
			} else {
				new_entry.lcount = 1;
				new_entry.excl_lcount = 1;
				oldp = NULL;
			}

			if (tw_data)
				free(tw_data);

			work->lin = dir->dir->_stat.st_ino;
			work->dircookie1 = md->cookie;
			tw_data = migr_tw_serialize(&pw_ctx->treewalk,
			    &tw_size);
			/* flush log before checkpoint */
			flush_stf_logs(pw_ctx, &error);
			if (error)
				goto out;
			success = set_entry_cond(st.st_ino, &new_entry,
			    pw_ctx->cur_rep_ctx, !found, oldp, pw_ctx->wi_lin,
			    work, tw_data, tw_size, &error);
			if (error)
				goto out;
		} while (!success);

		goto out;
	}

	ASSERT(type != SIQ_FT_UNKNOWN);

	is_ads = st.st_flags & UF_ADS;

	/* If stf initial sync or stf upgrade sync and not ads, add file to
	 * the repstate */
	if (pw_ctx->stf_sync && !is_ads &&
	    (pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync)) {
		log(DEBUG, "Adding repstate entry for file %s", de->d_name);

		if (st.st_nlink == 1) {
			file->skip_data = false;
			file->hard_link = false;
		} else {
			/* multiply linked file */
			multiply_linked = true;
			hard_linked_file(pw_ctx, &st, dir->dir->_stat.st_ino,
			    md->cookie, file, &error);
			if (error)
				goto out;
		}
	}

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx)) {
		add_file_for_diff_sync(pw_ctx, md, &st, fd, &error);
		if (error) {
			/* Diff sync keeps the fd and md in the cache, 
		 	 * cleanup is done in add_file_for_diff_sync () 
		 	 * So just set need_unref as false
		 	 */
			isi_ioerror_only(pw_ctx, error,
				NULL, 0, 0, "migr_handle_file");
			need_unref = false;
			goto out;
		}
	}

	/*
	* Only files we want to replicate are left.
	* Don't double count file split work items.
	*/
	if (!is_ads && !is_fsplit_work_item(work))
		pw_ctx->tw_cur_stats->files->total++;

	/* Diff sync keeps the fd and md in the cache, so cleanup is delayed */
	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		need_unref = false;

	/* Initialize assuming a full send */
	file->prev_snap = INVALID_SNAPID;
	file->size_changed = true;

	if (pw_ctx->snapop) {
		/* If not an ADS file, remember lin of sending file */
		if (!is_ads) {
			pw_ctx->filelin = st.st_ino;

			if (pw_ctx->prev_snap_cwd_fd > 0) {
				bit = file_unchanged(pw_ctx,
				    pw_ctx->prev_snap_cwd_fd, de->d_name,
				    de->d_encoding, &st, &prev_snap,
				    &size_changed, &error);

				/* file_unchanged sends ioerror internally */
				if (error)
					goto out;
				log(TRACE, "%s: bit=%d", __func__, bit);
				/*
				 * If we had a previous version and the
				 * file changed, record the snapid to cause a
				 * block based delta to occur.
				 */
				if (!bit && !pw_ctx->full_transfer) {
					file->prev_snap = prev_snap;
					file->size_changed = size_changed;
					UFAIL_POINT_CODE(force_full_transfer,
					    file->prev_snap = INVALID_SNAPID;
					    file->size_changed = true;
					);
				}
			}
			if (pw_ctx->initial_sync && pw_ctx->siq_lastrun &&
			    (st.st_ctime <= pw_ctx->siq_lastrun))
				bit = true;
		}
	} else if (!is_ads) {
		/*
		 * XXX: Unclear how much of this is live code... Should
		 * ONLY be for read-only mode syncs.
		 */
		pw_ctx->filelin = st.st_ino;
		if (pw_ctx->initial_sync && pw_ctx->siq_lastrun &&
		    (st.st_ctime <= pw_ctx->siq_lastrun))
			bit = true;
		else
			bit = false;
	}

	/* We will always do full sync for hard links */
	if (multiply_linked) {
		file->prev_snap = INVALID_SNAPID;
	}

	if (pw_ctx->assess) {
		assess_update_stat(pw_ctx, bit, &st, file);
		goto out;
	}

	sync_file = should_file_sync(pw_ctx, de, &st, fd, bit, &error);
	if (error)
		goto out;
	if (!sync_file) {
		pw_ctx->tw_cur_stats->files->skipped->up_to_date++;
		log(DEBUG, "Skipping file for up-to-date: %s/%s",
		    pw_ctx->cwd.path, de->d_name);
	}
	
	if (pw_ctx->stf_sync && !multiply_linked && !is_ads) {
		ASSERT(pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync);
		entry.lcount = 1;
		/*
		 * if this is an stf upgrade sync, need to send linmap data to
		 * the target for files that are up to date. files that are
		 * synced send linmap data via the file begin message. if this 
		 * is an stf upgrade sync and the file is multiply linked, we
		 * resend all of its instances (only one with file data) so
		 * that the target can set up the hard links
		 * Set rep_entry value non_hashed to true since we are not
		 * computing hash value for this lin
		 */

		if (pw_ctx->stf_upgrade_sync) {
		
			if (!sync_file) {
				send_lin(pw_ctx, de->d_name, de->d_encoding,
				    st.st_ino, md->cookie,
				    dir->dir->prev_sent_cookie);
				log(TRACE, "migr_handle_file: sent %llx-%llx",
				    pw_ctx->dir_state.dir->prev_sent_cookie, 
				    md->cookie);
				/* Update previous cookie for upgrade */
				migr_dir_set_prev_sent_cookie(dir->dir);
				/*
				 * Set non_hashed flag since we are skipping 
				 * hash calculation. This needs to be computed
				 * when we do first stf repeated sync for this
				 * file.
				 */

				entry.non_hashed = true;
			} else {
				entry.changed = true;
			}
		}
		stf_rep_log_add_entry(pw_ctx, pw_ctx->cur_rep_ctx,
		    st.st_ino, &entry, NULL, true, true, &error);
		if (error)
			goto out;
	} 

	if (!multiply_linked && !sync_file)
		goto out;

	/* Files we need to transfer. Only count non-ads files */
	if (!is_ads)
		pw_ctx->tw_cur_stats->files->replicated->selected++;
	else {
		pw_ctx->tw_cur_stats->files->replicated->ads_streams++;
		pw_ctx->stf_cur_stats->files->replicated->ads_streams++;
	}

	if (!DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		log(COPY, "Sending %s/%s", pw_ctx->cwd.path, de->d_name);

	send_ads = migr_start_file(pw_ctx, fd, md, file, &error);

	/* migr_start_file sends ioerror internally */
	if (error)
		goto out;

	fd = -1;

	if (send_ads)
		log(TRACE, "%s: sending ADS %s/%s", __func__, pw_ctx->cwd.path,
		    de->d_name);

	/* For diff sync, the MIGR_CONTINUE_FILE is only returned through
	 * ds_continue_dir() when looking through the cache. If this is a hard
	 * linked file and we're skipping the data, we only need to send file
	 * begin/end messages and don't care about contents matching the
	 * target */
	if (send_ads)
		result = MIGR_CONTINUE_ADS;
	else if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		result = MIGR_CONTINUE_DIR;
	else
		result = MIGR_CONTINUE_FILE;

out:
	if (need_unref && md && dir->dir)
		migr_dir_unref(dir->dir, md);

	if (fd != -1)
		close(fd);

	if (tw_data)
		free(tw_data);
	isi_error_handle(error, error_out);
	return result;
}

static enum migr_dir_result
migr_continue_dir(struct migr_pworker_ctx *pw_ctx, struct migr_dir_state *dir,
    struct migr_file_state *file, siq_plan_t *plan,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dirent *md = NULL;
	uint64_t cookie;
	enum migr_dir_result result = MIGR_CONTINUE_DIR;
	enum migr_dir_result ds_result;
	enum file_type type;

	log(TRACE, "%s", __func__);

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx) &&
	    !ds_continue_dir(pw_ctx, &ds_result, migr_dir_is_end(dir->dir))) {
		result = ds_result;
		goto out;
	}
	cookie = dir->dir->_resume;
	md = migr_dir_read(dir->dir, &error);
	/*
	 * Simulate readdirplus failure scenario.
	 * Skip entire ADS directory if we are not able
	 * to read ADS directory contents.
	 */
	UFAIL_POINT_CODE(skip_adsdir,
		if (pw_ctx->doing_ads &&
		    pw_ctx->ads_dir_state.dir->_buffers &&
		    pw_ctx->ads_dir_state.dir->_buffers->_index == 1) {
			free(pw_ctx->ads_dir_state.dir->_buffers);
			pw_ctx->ads_dir_state.dir->_buffers = NULL;
			result = MIGR_CONTINUE_DONE;
			_g_directory_corrupted = true;
			goto out;
		}
	);

	if (error) {
		isi_ioerror_only(pw_ctx, error, NULL, 0, 0, "migr_dir_read");
		goto out;
	} else if (md == NULL) {
		/*
		 * Bug 225582
		 * Check if we processed an empty directory in an initial sync
		 * and count the number of sequential empty directories.
		 */
		if (pw_ctx->initial_sync && pw_ctx->lin_map_blk
		    && cookie == DIR_SLICE_MIN
		    && dir->dir->_resume == DIR_SLICE_MAX)
			pw_ctx->initial_sync_empty_sub_dirs++;
		result = MIGR_CONTINUE_DONE;
		goto out;
	}

	type = dirent_type_to_file_type(md->dirent->d_type);

	log(TRACE, "%s read %s %s", __func__, file_type_to_name(type),
	    FNAME(md));

	if (type == SIQ_FT_DIR)
		result = migr_handle_dir(pw_ctx, dir, md, &error);
	else {
		/* Bug 225582 - reset empty dir count when we find a file */
		if (pw_ctx->initial_sync_empty_sub_dirs != 0) {
			ASSERT_DEBUG(pw_ctx->initial_sync
			    && pw_ctx->lin_map_blk);
			pw_ctx->initial_sync_empty_sub_dirs = 0;
		}
		result = migr_handle_file(pw_ctx, dir, md, type, file, plan,
		    &error);
	}

out:
	isi_error_handle(error, error_out);
	return result;
}

static bool
migr_start_dir(struct migr_pworker_ctx *pw_ctx, struct migr_dir_state *dir,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool has_ads = false;

	log(TRACE, "migr_start_dir");

	/* Delay if a files per second limit is active */
	throttle_delay(pw_ctx);

	if (!pw_ctx->doing_ads) {
		/* Only want to send ADS files the first time the directory is
		 * visited */
		if (dir->dir->visits == 1)
			has_ads = migr_start_ads(pw_ctx,
			    migr_dir_get_fd(dir->dir), &dir->st, "", 0,
			    &error);
	}
	
	/* migr_start_ads sends ioerror internally */
	isi_error_handle(error, error_out);
	return has_ads;
}

void
migr_continue_generic_file(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, migr_pworker_func_t return_func,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "migr_continue_generic_file");

	if (migr_continue_file(pw_ctx, file, &error)) {
		migr_call_next(pw_ctx, return_func);
	}

	/* migr_continue_file sends ioerror internally */
	isi_error_handle(error, error_out);
}

void
migr_continue_ads_file_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	migr_continue_generic_file(pw_ctx, &pw_ctx->ads_file_state,
	    migr_continue_ads_dir_for_file, error_out);
}

void
migr_continue_ads_file_for_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	migr_continue_generic_file(pw_ctx, &pw_ctx->ads_file_state,
	    migr_continue_ads_dir_for_dir, error_out);
}

void
migr_continue_reg_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "migr_continue_reg_file");

	migr_continue_generic_file(pw_ctx, &pw_ctx->file_state,
	    migr_continue_reg_dir, error_out);
}

static bool
migr_continue_generic_dir(struct migr_pworker_ctx *pw_ctx,
    struct migr_dir_state *dir, struct migr_file_state *file,
    siq_plan_t *plan, migr_pworker_func_t dir_func,
    migr_pworker_func_t file_func, migr_pworker_func_t ads_func,
    migr_pworker_func_t delete_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum migr_dir_result result;
	bool finished = false;
	int dup_fd;

	log(TRACE, "migr_continue_generic_dir");

	result = migr_continue_dir(pw_ctx, dir, file, plan, &error);
	/* migr_continue_dir sends ioerror internally */
	if (error)
		goto out;

	log(TRACE, "%s: migr_continue_dir() returns %s",
	    __func__, migr_dir_result_str(result));

	switch (result) {
	case MIGR_CONTINUE_DIR:
		/*
		 * Either the same function is just rescheduled or a recursion
		 * caused us to restart in a new directory.
		 */
		break;

	case MIGR_CONTINUE_FILE:
		/* Continue sending file contents. */
		migr_call_next(pw_ctx, file_func);
		break;

	case MIGR_CONTINUE_ADS:
		/* 
		 * Walk the ADS dir.  For ADS variants, the function will
		 * be NULL and should never be called.
		 */
		ASSERT(ads_func != NULL);

		migr_call_next(pw_ctx, ads_func);
		break;

	case MIGR_CONTINUE_DONE:
		if (migr_dir_is_end(dir->dir) &&
		    !(dir->dir->_stat.st_flags & UF_ADS))
			pw_ctx->tw_cur_stats->dirs->src->visited++;

		/* XXXJDH: here is where you'd conditionally use LIST_MSG */
		if (migr_dir_is_end(dir->dir) && delete_func) {
			siq_track_op_bump_count(SIQ_OP_DIR_FINISH);
			log(TRACE, "%s: At end of dir %s", __func__,
			    pw_ctx->cwd.path);
			/* Can't start deleting until we're done with every
			 * file in directory */
			if (pw_ctx->outstanding_acks != 0 &&
			    pw_ctx->wait_for_acks) {
				log(TRACE, "%s:     and still have %d acks",
				    __func__, pw_ctx->outstanding_acks);
				finished = false;
				goto out;
			}
			UFAIL_POINT_CODE(free_delete_dir,
			    if (pw_ctx->delete_dir != NULL) {
				    log(INFO, "%s: pw_ctx->delete_dir already "
					"initialized. Freeing "
					"pw_ctx->delete_dir", __func__);
				    migr_dir_free(pw_ctx->delete_dir);
				    pw_ctx->delete_dir = NULL;
			    });
			ASSERT(pw_ctx->delete_dir == NULL);
			dup_fd = dup(migr_dir_get_fd(dir->dir));
			ASSERT(dup_fd > 0);
			pw_ctx->delete_dir = migr_dir_new(dup_fd, NULL, true,
			    &error);
			if (error) {
				isi_ioerror_only(pw_ctx, error,
		    		    NULL, 0, 0, "migr_dir_new_generic");
				goto out;
			}
			listreset(&pw_ctx->delete_filelist);

			siq_track_op_beg(SIQ_OP_DIR_DEL);
			migr_call_next(pw_ctx, delete_func);
		} else
			finished = true;

		if (_g_expected_dataloss && _g_directory_corrupted &&
			(!error))
		{
			/*
			 * Looks like directory is corrupted
			 * e.g. readdirplus failed for the current dir
			 * log and bail out.
			 * this function is generic and is called for both
			 * ADS and regular directories. So we are good
			 */

			/*
			 * For ADS directory, ':' should be present
			 * e.g. /ifs/data/testdir:
			 * testdir has ADS directory but failed to read
			 */
			if (pw_ctx->doing_ads) {
				struct migr_dirent *last_md = NULL;
				struct migr_dirent *last_md_ads = NULL;
				char filename[MAXNAMELEN];
				last_md = pw_ctx->dir_state.dir->_last_read_md;
				last_md_ads = pw_ctx->ads_dir_state.dir->_last_read_md;
				if (last_md) {
					/* Only when parent is a file */
					sprintf(filename, "/%s:", last_md->dirent->d_name);
					log_corrupt_file_entry (pw_ctx, filename);
				}
				else
					/* Parent is a directory */
					log_corrupt_file_entry (pw_ctx, ":");
			}
			else
				log_corrupt_file_entry (pw_ctx, "");
			_g_directory_corrupted = false;
		}
		break;
	}

out:
	isi_error_handle(error, error_out);
	return finished;
}

bool
migr_continue_delete(struct migr_pworker_ctx *pw_ctx, siq_plan_t *plan,
    bool send_error, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dirent *md;
	struct dirent *de;
	bool finished = false;
	int ret;
	bool skip = false;

	log(TRACE, "%s", __func__);

	md = migr_dir_read(pw_ctx->delete_dir, &error);
	if (error) {
		if (send_error) {
			isi_ioerror_only(pw_ctx, error,
			    NULL, 0, 0, "migr_dir_read_delete");
		}
		goto out;
	}

	if (md == NULL) {
		finished = true;
		listflush(&pw_ctx->delete_filelist, LIST_TO_SYNC,
		    pw_ctx->sworker);
		migr_dir_free(pw_ctx->delete_dir);
		pw_ctx->delete_dir = NULL;
		if (pw_ctx->expected_dataloss && _g_directory_corrupted) {
			/* it might be possible that directory read 
			 * failed and _g_directory_corrupted is set.
			 * reset this flag to false, since we have already
			 * logged directory name
			 */
			_g_directory_corrupted = false;
		}
		goto out;
	}

	/*
	 * Check whether the dirent needs to be skipped due to 
	 * predicates. If so do not include that in the delete list.
	 */
	de = md->dirent;
	ret = siq_select(plan, de->d_name, de->d_encoding, md->stat,
	    pw_ctx->new_time, pw_ctx->cwd.utf8path, 
	    dirent_type_to_file_type(de->d_type), &error);
	if (error) {
		if (send_error) {
			isi_ioerror_only(pw_ctx, error, de->d_name, 
			    de->d_encoding, de->d_ino, "delete siq_select");
		} 
		goto out;
	}
	if (!error && ret == 0) {
		log(DEBUG, "Skipping file delete: %s/%s", pw_ctx->cwd.path,
		    de->d_name);
		skip = true;
	}

	if (!skip) {
		listadd(&pw_ctx->delete_filelist, FNAME(md),
		    md->dirent->d_encoding, md->cookie,
		    LIST_TO_SYNC, pw_ctx->sworker, NULL);
	}

out:
	if (md)
		migr_dir_unref(pw_ctx->delete_dir, md);
	isi_error_handle(error, error_out);
	return finished;
}

static void
migr_finish_ads_dir(struct migr_pworker_ctx *pw_ctx,
    migr_pworker_func_t next_func)
{
	log(TRACE, "%s", __func__);

	send_ads(pw_ctx, 0);

	UFAIL_POINT_CODE(ads_chdir_oldpwd,
	    log(FATAL, "Error changing to old pwd"));
	/* A bit harsh, but there isn't much of any EXPECTED
	 * error that can cause an fchdir to fail */
	if (fchdir(pw_ctx->ads_oldpwd)) {
		log(FATAL, "Error changing to old pwd");
	}

	close(pw_ctx->ads_oldpwd);
	pw_ctx->ads_oldpwd = -1;
	pw_ctx->doing_ads = false;

	migr_dir_free(pw_ctx->ads_dir_state.dir);
	pw_ctx->ads_dir_state.dir = NULL;

	if (is_tw_work_item(pw_ctx)) {
		migr_call_next(pw_ctx, next_func);
	} else {
		if (pw_ctx->rep_entry.is_dir)
			migr_call_next(pw_ctx, stf_continue_lin_commit);
		else
			migr_call_next(pw_ctx, stf_continue_lin_data);
	} 
}

static void
migr_continue_ads_delete(struct migr_pworker_ctx *pw_ctx,
    migr_pworker_func_t next_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool finished;

	log(TRACE, "%s", __func__);
	finished = migr_continue_delete(pw_ctx, NULL, true, &error);
	/* migr_continue_delete sends ioerror internally */
	if (error)
		goto out;

	if (finished)
		migr_finish_ads_dir(pw_ctx, next_func);

out:
	isi_error_handle(error, error_out);

}

static void
migr_continue_ads_delete_for_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	migr_continue_ads_delete(pw_ctx, migr_continue_reg_dir, error_out);
}

void
migr_continue_ads_dir_for_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	if (migr_continue_generic_dir(pw_ctx, &pw_ctx->ads_dir_state,
	    &pw_ctx->ads_file_state, NULL, migr_continue_ads_dir_for_dir,
	    migr_continue_ads_file_for_dir, NULL,
	    migr_continue_ads_delete_for_dir, error_out))
		migr_finish_ads_dir(pw_ctx, migr_continue_reg_dir);
}

void
migr_continue_ads_delete_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	migr_continue_ads_delete(pw_ctx, migr_continue_reg_file, error_out);
}

void
migr_continue_ads_dir_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	log(TRACE, "%s", __func__);
	if (migr_continue_generic_dir(pw_ctx, &pw_ctx->ads_dir_state,
	    &pw_ctx->ads_file_state, NULL, migr_continue_ads_dir_for_file,
	    migr_continue_ads_file_for_file, NULL,
	    migr_continue_ads_delete_for_file, error_out))
		migr_finish_ads_dir(pw_ctx, migr_continue_reg_file);
}

void
migr_finish_generic_reg_dir(struct migr_pworker_ctx *pw_ctx,
    migr_pworker_func_t start_work_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct generic_msg lin_map_blk_msg = {};

	log(TRACE, "%s", __func__);
	if (pw_ctx->outstanding_acks != 0
	    || pw_ctx->initial_sync_empty_sub_dirs >= MAX_EMPTY_SUB_DIRS) {
		/* Send lin map checkpoint message to target */
		if (pw_ctx->lin_map_blk) {
			lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
			msg_send(pw_ctx->sworker, &lin_map_blk_msg);
		}
		flush_burst_buffer(pw_ctx);
		migr_wait_for_response(pw_ctx, MIGR_WAIT_ACK_UP);
		siq_track_op_beg(SIQ_OP_FILE_ACKS);
		goto out;
	}

	if (DIFF_SYNC_BUT_NOT_ADS(pw_ctx))
		ASSERT(pw_ctx->cache_size == 0);

	migr_send_dir_range_update(pw_ctx, pw_ctx->dir_state.dir->_slice.end);
	
	if (migr_tw_pop(&pw_ctx->treewalk, pw_ctx->dir_state.dir)) {
		pw_ctx->dir_state.dir = NULL;
		migr_call_next(pw_ctx, start_work_func);
		siq_track_op_end_nocount(SIQ_OP_DIR_FINISH);
		goto out;
	}

	siq_track_op_end_nocount(SIQ_OP_DIR_FINISH);
	migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
	pw_ctx->done = 1;
	if (pw_ctx->checkpoints && !pw_ctx->is_local_operation &&
	    pw_ctx->work->rt != WORK_DOMAIN_MARK) {
		migr_send_done_msg(pw_ctx);
	} else {
		log(TRACE, "local operation or there are no checkpoints");
		tw_request_new_work(pw_ctx);
	}

out:
	isi_error_handle(error, error_out);
}

void
migr_finish_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	migr_finish_generic_reg_dir(pw_ctx, migr_start_work, error_out);
}

static void
migr_continue_reg_delete(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool finished;

	finished = migr_continue_delete(pw_ctx, pw_ctx->plan, true, &error);
	/* migr_continue_delete sends ioerror internally */
	if (error)
		goto out;

	if (finished) {
		siq_track_op_end(SIQ_OP_DIR_DEL);
		migr_finish_reg_dir(pw_ctx, &error);
	}
	/* migr_continue_delete would send ioerror internally */

out:
	isi_error_handle(error, error_out);
}

void
migr_continue_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool finished = false;

	log(TRACE, "migr_continue_reg_dir");

	finished = migr_continue_generic_dir(pw_ctx, &pw_ctx->dir_state,
	    &pw_ctx->file_state, pw_ctx->plan, migr_continue_reg_dir,
	    migr_continue_reg_file, migr_continue_ads_dir_for_file,
	    pw_ctx->action == SIQ_ACT_SYNC ? migr_continue_reg_delete : NULL,
	    &error);
	/* migr_continue_generic_dir sends ioerror internally */
	if (error || !finished)
		goto out;

	migr_finish_reg_dir(pw_ctx, &error);
out:
	isi_error_handle(error, error_out);
}


void
migr_start_ads_or_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry entry = {};

	log(TRACE, "%s", __func__);

	if (pw_ctx->stf_sync &&
	    (pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync)) {
		log(DEBUG, "Adding repstate entry for dir %s",
		    pw_ctx->dir_state.dir->name);
		entry.lcount = 1;
		entry.is_dir = true;
		stf_rep_log_add_entry(pw_ctx,
		    pw_ctx->cur_rep_ctx, pw_ctx->dir_state.st.st_ino,
		    &entry, NULL, true, true, &error);
		if (error)
			goto out;
	}

	/* migr_start_dir() returns true if directory has ADS entries */
	/* migr_start_dir sends ioerror internally */
	if (migr_start_dir(pw_ctx, &pw_ctx->dir_state, &error)) {
		if (!error)
			migr_call_next(pw_ctx, migr_continue_ads_dir_for_dir);
	}
	else if (!error) {
		migr_call_next(pw_ctx, migr_continue_reg_dir);
	}
out:
	isi_error_handle(error, error_out);
}

void
migr_start_work(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;
	bool need_ack_dir = false;
	int ret;
	enum ifs_security_info secinfo;
	char *acl = NULL;

	ilog(IL_INFO, "migr_start_work called"); ////TRACE -> INFO

	ASSERT(!pw_ctx->dir_state.dir);

	siq_track_op_beg(SIQ_OP_DIR_FINISH);

	dir = migr_tw_get_dir(&pw_ctx->treewalk, &error);
	if (error) {
		isi_ioerror_only(pw_ctx, error, NULL, 0, 0,
		    "migr_start_work");
		goto out;
	}

	dir->visits++;
	log(TRACE, "%s: Dir %s visits=%d", __func__, dir->name, dir->visits);
	pw_ctx->dir_state.dir = dir;
	if (fchdir(migr_dir_get_fd(dir)) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to chdir in start_work: %s",
		    pw_ctx->treewalk._path.path);
		isi_ioerror_only_paths(pw_ctx, error, "", 0, dir->_stat.st_ino,
		    "start_work_fchdir",
		    pw_ctx->treewalk._path.utf8path,
		    pw_ctx->treewalk._path.path);
		goto out;

	}

	ret = 0;
	UFAIL_POINT_CODE(start_work_fstat,
	    ret = -1; errno = RETURN_VALUE);
	if (!ret)
		ret = fstat(migr_dir_get_fd(dir), &pw_ctx->dir_state.st);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to fstat in start_work: %s",
		    pw_ctx->treewalk._path.path);
		isi_ioerror_only_paths(pw_ctx, error, "", 0, dir->_stat.st_ino,
		    "start_work_fstat",
		    pw_ctx->treewalk._path.utf8path,
		    pw_ctx->treewalk._path.path);
		goto out;
	}

	if (pw_ctx->dir_state.st.st_flags & (SF_HASNTFSACL | SF_HASNTFSOG)) {
		secinfo = IFS_SEC_INFO_OWNER | IFS_SEC_INFO_GROUP;
		if (pw_ctx->dir_state.st.st_flags & SF_HASNTFSACL)
			secinfo |= IFS_SEC_INFO_DACL | IFS_SEC_INFO_SACL;

		acl = get_sd_text_secinfo(migr_dir_get_fd(dir), secinfo);
		if (acl == NULL) {
			error = isi_system_error_new(errno,
			    "Failed to get acl in start_work: %s",
			    pw_ctx->treewalk._path.path);
			isi_ioerror_only_paths(pw_ctx, error, "", 0, dir->_stat.st_ino,
			    "start_work_acl",
			    pw_ctx->treewalk._path.utf8path,
			    pw_ctx->treewalk._path.path);
			goto out;
		}
	}



	/* update the cwd and fd in prev snap (if it exists) */
	path_copy(&pw_ctx->cwd, &pw_ctx->treewalk._path);
	if (pw_ctx->prev_snap_root_fd > 0) {
		if (pw_ctx->prev_snap_cwd_fd > 0) {
			close(pw_ctx->prev_snap_cwd_fd);
			pw_ctx->prev_snap_cwd_fd = -1;
		}

		pw_ctx->prev_snap_cwd_fd =
		    fd_rel_to_ifs(pw_ctx->prev_snap_root_fd, &pw_ctx->cwd);
		if (pw_ctx->prev_snap_cwd_fd == -1) {
			if (errno == ENOENT || errno == ENOTDIR) {
				log(DEBUG, "Path %s does not exist in prev "
				    "snap", pw_ctx->cwd.path);
			} else {
				error = isi_system_error_new(errno,
				    "Failed to get fd for %s in prev snap",
				    pw_ctx->cwd.path);
				goto out;
			}
		}
	}

	init_list_deleted(&pw_ctx->deleted_files, pw_ctx->cwd.utf8path);

	/* Initialize variable for parent dir lin */
	pw_ctx->dir_lin = pw_ctx->dir_state.st.st_ino;
	if (pw_ctx->doing_diff_sync) {
		log(TRACE,
		    "%s: %s  visits=%d  parent_is_new_dir=%d  is_new_dir=%d",
		    __func__, pw_ctx->dir_state.dir->name,
		    pw_ctx->dir_state.dir->visits,
		    pw_ctx->dir_state.dir->parent_is_new_dir,
		    pw_ctx->dir_state.dir->is_new_dir);
		/* If this is the first time the current directory has been
		 * visited, and the parent dir isn't new, then we need an
		 * ACK_DIR from the sworker to determine if the directory is
		 * new or not on the target. The ack_dir_callback() will do
		 * the hasher_chdir, if it's not a new directory on the
		 * target  */
		if (pw_ctx->dir_state.dir->visits == 1 &&
		    !pw_ctx->dir_state.dir->parent_is_new_dir) {
			need_ack_dir = true;
			log(TRACE, "%s: needs an ACK_DIR", __func__);
		}
		/* If this isn't the first time the directory has been
		 * visited, and we know that this isn't a new dir on the
		 * target, then we don't need an ACK_DIR, but we need the
		 * p_hasher to change directory for possible hashing of files
		 * in the directory */
		else if (pw_ctx->dir_state.dir->visits > 1 &&
		    !pw_ctx->dir_state.dir->is_new_dir) {
			hasher_chdir(pw_ctx);
		}
		/* Otherwise, we know this directory is new, so no ACK_DIR is
		 * or chdir for the p_hasher is needed */
	}

	check_for_compliance(pw_ctx, pw_ctx->dir_lin, &error);
	if (error) {
		goto out;
	}

	migr_send_dir(pw_ctx, need_ack_dir, acl);

	send_user_attrs(pw_ctx, migr_dir_get_fd(dir), true, &error);
	if (error) {
		/*
		 * Bug 256098
		 *
		 * If we fail to read extended attrs due to a corrupted extension
		 * block, log corruption and continue so we have a chance to send
		 * the contents of the directory if only the extended attributes
		 * are unreadable. No USER_ATTR_MSG is sent if there are no
		 * attributes, so continuing here is equivalent to handling a
		 * directory with no attributes.
		 */
		if (pw_ctx->expected_dataloss) {
			char filename[MAXPATHLEN];
			isi_error_free(error);
			error = NULL;
			snprintf(filename, sizeof(filename), "/%s", dir->name);
			log_corrupt_file_entry(pw_ctx, filename);
		}
		else {
			goto out;
		}
	}

	if (need_ack_dir)
		migr_wait_for_response(pw_ctx, MIGR_WAIT_ACK_DIR);
	else
		migr_start_ads_or_reg_dir(pw_ctx, &error);
	/* migr_start_ads_or_reg_dir sends ioerror internally */

out:
	if (acl)
		free(acl);
	isi_error_handle(error, error_out);
}

char *
migr_dir_result_str(enum migr_dir_result res)
{
	static char buf[30];

	switch (res) {
	case MIGR_CONTINUE_DIR:
		return "MIGR_CONTINUE_DIR";
	case MIGR_CONTINUE_FILE:
		return "MIGR_CONTINUE_FILE";
	case MIGR_CONTINUE_ADS:
		return "MIGR_CONTINUE_ADS";
	case MIGR_CONTINUE_DONE:
		return "MIGR_CONTINUE_DONE";
	default:
		snprintf(buf, sizeof(buf), "Unknown dir result %d", res);
		return buf;
	}
}
