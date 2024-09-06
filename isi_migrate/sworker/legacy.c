#include <sys/acl.h>
#include <sys/isi_enc.h>
#include <sys/module.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <md5.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <ifs/ifs_syscalls.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_licensing/licensing.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/util.h>

#define context kernel_context
#include "isi_snapshot/snapshot.h"
#undef context

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/migr/siq_workers_utils.h"

#include "sworker.h"


static void
copy_ads(int from_fd, int to_fd, char *from, enc_t enc, 
    struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	int size, from_dfd, to_dfd, src_fd, dest_fd;
	char buffer[BUFFER_SIZE];
	struct stat st;
	struct dirent *d;
	DIR *dir;

	log(TRACE, "copy_ads");
	from_dfd = enc_openat(from_fd, ".", ENC_DEFAULT, O_RDONLY | O_XATTR);
	if (from_dfd == -1) {
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path, from, enc,
		    inprog->file_obj ? inprog->sstate.src_filelin : 
		    inprog->dstate.src_dirlin,
		    "Failed to open ADS directory for %s/%s", 
		    worker->cur_path.path, from);
		sw_ctx->stats.io_errors++;
		return;
	}

	to_dfd = enc_openat(to_fd, ".", ENC_DEFAULT, O_RDONLY | O_XATTR);
	if (to_dfd == -1) {
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path, from, enc,
		    inprog->file_obj ? inprog->sstate.src_filelin : 
		    inprog->dstate.src_dirlin,
		    "Failed to open new ADS directory for %s/%s",
		    worker->cur_path.path, from);
		close(from_dfd);
		sw_ctx->stats.io_errors++;
		return;
	}

	dir = fdopendir_enc(from_dfd);
	if (dir == NULL) {
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path, from, enc,
		    inprog->file_obj ? inprog->sstate.src_filelin :
		    inprog->dstate.src_dirlin,
		    "Failed to open ADS directory for %s/%s",
		    worker->cur_path.path, from);
		close(from_dfd);
		close(to_dfd);
		sw_ctx->stats.io_errors++;
		return;
	}
	readdir(dir); /* Skip . and .. */
	readdir(dir);

	while ((d = readdir(dir))) {
		struct timeval tvs[2];
		acl_t acl;

		src_fd = enc_openat(from_dfd, d->d_name, d->d_encoding,
		    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
		if (src_fd == -1) {
			inprog->result = ACK_RES_ERROR;
			ioerror(global->primary,
			    worker->cur_path.utf8path, from, enc, 
			    inprog->file_obj ? inprog->sstate.src_filelin :
			    inprog->dstate.src_dirlin,
			    "Failed to open ADS %s in %s/%s", d->d_name,
			    worker->cur_path.path, from);
			sw_ctx->stats.io_errors++;
			continue;
		}

		if (fstat(src_fd, &st) == -1) {
			inprog->result = ACK_RES_ERROR;
			ioerror(global->primary, 
			    worker->cur_path.utf8path, from, enc,
			    inprog->file_obj ? inprog->sstate.src_filelin : 
			    inprog->dstate.src_dirlin,
			    "Failed to stat ADS %s in %s/%s",
			    d->d_name, worker->cur_path.path, from);
			close(src_fd);
			sw_ctx->stats.io_errors++;
			continue;
		}

		dest_fd = enc_openat(to_dfd, d->d_name, d->d_encoding,
		    O_WRONLY | O_CREAT | O_TRUNC | O_FSYNC, st.st_mode);
		if (dest_fd == -1) {
			sw_ctx->inprog.result = ACK_RES_ERROR;
			ioerror(global->primary, 
			    worker->cur_path.utf8path, from, enc,
			    inprog->file_obj ? inprog->sstate.src_filelin :
			    inprog->dstate.src_dirlin,
			    "Failed to open new ADS %s for %s/%s", d->d_name,
			    worker->cur_path.path, from);
			close(src_fd);
			sw_ctx->stats.io_errors++;
			continue;
		}

		for (;;) {
			size = read(src_fd, buffer, sizeof(buffer));
			if (size <= 0)
				break;

			size = write(dest_fd, buffer, size);
			if (size < 0)
				break;
		}
		if (size < 0) {
			inprog->result = ACK_RES_ERROR;
			ioerror(global->primary, 
			    worker->cur_path.utf8path, from, enc,
			    inprog->file_obj ? inprog->sstate.src_filelin :
			    inprog->dstate.src_dirlin,
			    "Failed to copy ADS %s to new %s/%s", d->d_name,
			    worker->cur_path.path, from);
			sw_ctx->stats.io_errors++;
		}

		fchown(dest_fd, st.st_uid, st.st_gid);
		fchmod(dest_fd, st.st_mode);

		acl = acl_get_fd(src_fd);
		if (acl != NULL) {
			acl_set_fd(dest_fd, acl);
			acl_free(acl);
		}

		tvs[0].tv_sec = st.st_atime;
		tvs[0].tv_usec = 0;
		tvs[1].tv_sec = st.st_mtime;
		tvs[1].tv_usec = 0;
		futimes(dest_fd, tvs);

		close(src_fd);
		close(dest_fd);
	}

	closedir(dir); // also closes from_dfd
	close(to_dfd);
	return;
}

static inline void
legacy_apply_security_to_fd(int fd, fflags_t src_flags, uid_t uid, gid_t gid,
    mode_t mode, char *acl, char *error_fname)
{
	struct isi_error *error = NULL;
	apply_security_to_fd(fd, src_flags, NULL, uid, gid, mode, acl, &error);
	if (error) {
		log(ERROR,"%s on %s", isi_error_get_message(error),
		    error_fname);
		isi_error_free(error);
	}
}

void
old_finish_file(struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct isi_error *error = NULL;

	/*
	 * if ads_start = 0 then it is end of regular file,
	 *  not end of ads of file or directory
	 */
	struct timeval tvs[2];
	int ret;
	struct generic_msg msg;

	ASSERT(global->job_ver.common < MSG_VERSION_SBONNET);

	log(TRACE, "old_finish_file");
	if (inprog->fstate.fd == -1)
		return;

	legacy_apply_security_to_fd(inprog->fstate.fd,
	    inprog->fstate.stat_state.flags,
	    inprog->fstate.stat_state.owner,
	    inprog->fstate.stat_state.group,
	    inprog->fstate.stat_state.mode,
	    inprog->fstate.stat_state.acl, inprog->fstate.name);
	/*XXX: EML is this really where we should be doing this? */
	if (inprog->fstate.stat_state.acl) {
		free(inprog->fstate.stat_state.acl);
		inprog->fstate.stat_state.acl = NULL;
	}

	if (inprog->fstate.stat_state.flags_set &&
	    fchflags(inprog->fstate.fd, inprog->fstate.stat_state.flags) == -1)
		log(ERROR, "fchflags on %s failed", inprog->fstate.name);


	ret = enc_renameat(worker->cwd_fd, inprog->fstate.tname, ENC_DEFAULT, 
	    worker->cwd_fd, inprog->fstate.name, inprog->fstate.enc);
	if (ret == -1 && (errno == EISDIR || errno == ENOTDIR)) {
		delete(inprog->fstate.name, inprog->fstate.enc, sw_ctx,
		    &error);
		ret = enc_renameat(worker->cwd_fd, inprog->fstate.tname, 
		    ENC_DEFAULT, worker->cwd_fd, inprog->fstate.name, 
		    inprog->fstate.enc);
	}

	if (ret == -1) {
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path, 
		    inprog->fstate.name, inprog->fstate.enc, 
		    inprog->file_obj ?
		    (inprog->ads ? inprog->sstate.src_filelin : 
		    inprog->fstate.src_filelin) : (inprog->dstate.src_dirlin),
		    "Failed to rename temporary file %s to %s/%s (in %s)",
		    inprog->fstate.tname, worker->cur_path.path, 
		    inprog->fstate.name,
		    inprog->fstate.enc > ENC_MAX ?  "unsupported encoding" :
		    enc_type2name(inprog->fstate.enc));
		sw_ctx->stats.io_errors++;

		/* Unlink temporary file on error */
		enc_unlinkat(worker->cwd_fd, inprog->fstate.tname, 
		    ENC_DEFAULT, 0);
	}

	tvs[0].tv_sec = inprog->fstate.stat_state.atime.tv_sec;
	tvs[0].tv_usec = 0;
	tvs[1].tv_sec = inprog->fstate.stat_state.mtime.tv_sec;
	tvs[1].tv_usec = 0;
	futimes(inprog->fstate.fd, tvs);

	if (close(inprog->fstate.fd) == -1){
		inprog->result = ACK_RES_ERROR;
		sw_ctx->stats.io_errors++;
	}

	inprog->fstate.fd = -1;

	/* If finished not ads stream, send ACK_MSG with operation result */
	if (!inprog->ads) {
		log(TRACE, "Send ack_msg for file %s, rev =%lld",
		    inprog->fstate.name, inprog->fstate.rev);
		msg.head.type = OLD_ACK_MSG2;
		msg.body.old_ack2.file = inprog->fstate.name;
		msg.body.old_ack2.enc = inprog->fstate.enc;
		msg.body.old_ack2.rev = inprog->fstate.rev;
		msg.body.old_ack2.dir_lin = inprog->fstate.src_dirlin;
		msg.body.old_ack2.filesize = sw_ctx->stats.curfile_size;
		msg.body.old_ack2.result = inprog->result;
		msg.body.old_ack2.io_errors = sw_ctx->stats.io_errors;
		msg.body.old_ack2.checksum_errors =
		    sw_ctx->stats.checksum_errors;
		msg.body.old_ack2.transfered = sw_ctx->stats.ads_of_curfile;
		msg.body.old_ack2.filelin = inprog->fstate.src_filelin;
		sw_ctx->stats.curfile_size = 0;
		sw_ctx->stats.ads_of_curfile = 0;
		sw_ctx->stats.io_errors = 0;
		sw_ctx->stats.checksum_errors = 0;
		inprog->result = ACK_RES_OK;

		msg_send(global->primary, &msg);

		if (msg.body.old_ack2.checksum_errors) {
			errno = EIO;
			ioerror(global->primary, worker->cur_path.utf8path, 
			    inprog->fstate.name, inprog->fstate.enc, 
			    inprog->file_obj ? 
			    (inprog->ads ? inprog->sstate.src_filelin :
			    inprog->fstate.src_filelin) :
			    (inprog->dstate.src_dirlin),
			    "Checksum failure while transferring %s/%s (%s)",
			    worker->cur_path.path, inprog->fstate.name, 
			    inprog->fstate.enc > ENC_MAX ? 
			    "unsupported encoding" :
			    enc_type2name(inprog->fstate.enc));
		}
	}

	free(inprog->fstate.tname);
	free(inprog->fstate.name);
	isi_error_free(error);
}

/*
 *  To allow the sworker be backward compatible with older versions of the
 *  pworker, we have to be able accept the old work_init. Just copy its
 *  contents into the new work_init, and pass if off to the work_init_callback
 */
static int
old_work_init_callback(struct generic_msg *m_in, void *ctx)
{
	struct generic_msg m_out = {};
	struct old_work_init_msg *in = &m_in->body.old_work_init;
	struct old_work_init_msg6 *out = &m_out.body.old_work_init6;

	log(TRACE, "%s", __func__);
	out->target = in->target;
	out->peer = in->peer;
	out->target_base = in->path;
	out->source_base = in->src;
	out->bwhost = in->bwhost;
	out->predicate = in->predicate;
	out->lastrun_time = in->lastrun_time;
	out->flags = in->flags;
	out->pass = in->pass;
	out->job_name = in->job_name;
	out->loglevel = in->loglevel;
	out->action = in->action;
	out->hash_type = HASH_NONE;
	out->run_id = 0;
	work_init_callback(&m_out, ctx);
	return 0;
}

static int
old_work_init4_callback(struct generic_msg *m_in, void *ctx)
{
	struct generic_msg m_out = {};
	struct old_work_init_msg4 *in = &m_in->body.old_work_init4;
	struct old_work_init_msg6 *out = &m_out.body.old_work_init6;

	log(TRACE, "%s", __func__);
	out->target = in->target;
	out->peer = in->peer;
	out->target_base = in->path;
	out->source_base = in->src;
	out->bwhost = in->bwhost;
	out->predicate = in->predicate;
	out->run_id = 0;
	out->lastrun_time = in->lastrun_time;
	out->flags = in->flags;
	out->pass = in->pass;
	out->job_name = in->job_name;
	out->loglevel = in->loglevel;
	out->action = in->action;
	out->hash_type = in->hash_type;
	out->min_hash_piece_len = in->min_hash_piece_len;
	out->hash_pieces_per_file = in->hash_pieces_per_file;
	out->worker_id = in->worker_id;
	out->skip_bb_hash = in->skip_bb_hash;
	work_init_callback(&m_out, ctx);
	return 0;
}

/*
 *  To allow the sworker be backward compatible with older versions of the
 *  pworker, we have to be able accept the old dir_msg3. Just copy its
 *  contents into the new dir_msg4, and pass if off to the dir4_callback() The
 *  only difference between old_dir_msg3 and dir_msg4 is the addition of
 *  dir_lin in dir_msg4.
 */
static int
old_dir3_callback(struct generic_msg *m_in, void *ctx)
{
	struct generic_msg m_out = {};
	struct old_dir_msg3 *in = &m_in->body.old_dir3;
	struct dir_msg4 *out = &m_out.body.dir4;

	log(TRACE, "%s", __func__);

	out->dir = in->dir;
	out->mode = in->mode;
	out->uid = in->uid;
	out->gid = in->gid;
	out->atime = in->atime;
	out->mtime = in->mtime;
	out->reap = in->reap;
	out->acl = in->acl;
	out->eveclen = in->eveclen;
	out->evec = in->evec;
	out->di_flags = in->di_flags;
	dir4_callback(&m_out, ctx);
	return 0;
}

static int
old_file_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;

	struct old_file_msg4 *msg4 = &m->body.old_file4;
	enc_t enc = ENC_DEFAULT; /* XXX */
	char tmp_buffer[] = "." PROGNAME ".XXXXXX";

	ASSERT(global->job_ver.common < MSG_VERSION_SBONNET);
	log(TRACE, "file_callback");
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");

	old_finish_file(sw_ctx);
	ASSERT(worker->cwd_fd != -1);
	ASSERT(inprog->dstate.dir_valid);

	log(TRACE, "Get msg %d", m->head.type);
	enc = msg4->enc;
	log(TRACE, "Get file %s(%d)", msg4->file, enc);

	if (!inprog->ads)
		inprog->file_obj = true;

	/* mkstemp creates the tmp file wherever the proc's cwd is, which is
	 * lame. since we are tracking cwd state in our context, we need to 
	 * fchdir to that location before calling mkstemp. */
	fchdir(worker->cwd_fd);
	inprog->fstate.fd = mkstemp(tmp_buffer);
	if (inprog->fstate.fd == -1) {
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path, msg4->file,
		    enc, msg4->filelin, "Failed to open temporary file %s/%s "
		    "for write", worker->cur_path.path, tmp_buffer);
		sw_ctx->stats.io_errors++;
	} else {
		struct stat st;
		int fd;

		log(COPY, "Writing %s/%s", worker->cur_path.path, msg4->file);
		fd = enc_openat(worker->cwd_fd, msg4->file, enc, O_RDONLY |
		    O_NOFOLLOW | O_OPENLINK);
		if (fd != -1) {
			if (fstat(fd, &st) == 0 && (st.st_flags & UF_HASADS))
				copy_ads(fd, inprog->fstate.fd, msg4->file, 
				    enc, sw_ctx);
			close(fd);
		}
	}

	inprog->fstate.tname = strdup(tmp_buffer);
	inprog->fstate.name  = strdup(msg4->file);
	inprog->fstate.enc   = enc;
	inprog->fstate.stat_state.owner = (uid_t)msg4->uid;
	inprog->fstate.stat_state.group = (gid_t)msg4->gid;
	inprog->fstate.stat_state.atime.tv_sec = msg4->atime;
	inprog->fstate.stat_state.atime.tv_nsec = 0;
	inprog->fstate.stat_state.mtime.tv_sec = msg4->mtime;
	inprog->fstate.stat_state.mtime.tv_nsec = 0;
	inprog->fstate.stat_state.mode  = (mode_t)msg4->mode;
	inprog->fstate.stat_state.size  = (uint64_t)msg4->size;
	free(inprog->fstate.stat_state.acl);
	inprog->fstate.stat_state.acl = NULL;
	inprog->fstate.stat_state.flags = 0;
	inprog->fstate.stat_state.flags_set = 0;
	if (msg4->acl != NULL)
		inprog->fstate.stat_state.acl = strdup(msg4->acl);
	inprog->fstate.stat_state.flags_set = 1;
	inprog->fstate.stat_state.flags = (fflags_t)msg4->di_flags;
	inprog->fstate.rev = (u_int64_t)msg4->rev;
	inprog->fstate.src_dirlin = (u_int64_t)msg4->dir_lin;
	inprog->fstate.src_filelin = (u_int64_t)msg4->filelin;
	log(TRACE, "Remember source file's lin %lld",
	    (u_int64_t)msg4->filelin);

	sw_ctx->stats.ads_of_curfile++;
	return 0;
}

/*
 * Previous releases of SyncIQ (2.0 and 2.5) responded to the OLD_CLUSTER_MSG
 * and OLD_CLUSTER_MSG2 with a OLD_CLUSTER_MSG with a list of target node IPS.
 */
static void
legacy_send_lnn_ip_pairs(struct migr_sworker_ctx *sw_ctx,
    struct nodex *nodes, int num_nodes)
{
	struct generic_msg resp = {};
	struct node_lnn_ip_pairs *pairs = NULL;
	int i;

	resp.head.type = OLD_CLUSTER_MSG;
	resp.body.old_cluster.name =  sw_ctx->global.cluster_name;
	resp.body.old_cluster.devslen = num_nodes *
	    sizeof(struct node_lnn_ip_pairs);
	resp.body.old_cluster.devs = calloc(num_nodes,
	    sizeof(struct node_lnn_ip_pairs));

	pairs = (struct node_lnn_ip_pairs *)resp.body.old_cluster.devs;
 	for (i = 0; i < num_nodes; i++) {
		pairs[i].lnn = nodes[i].lnn;
		pairs[i].ip = nodes[i].addr.inx_addr4;
		log(TRACE, "Dev %d node.lnn %d, node.ip %s", i, nodes[i].lnn,
		    inx_addr_to_str(&nodes[i].addr));
	}

	msg_send(sw_ctx->global.primary, &resp);
}

/*
 * The following callbacks, old_cluster_callback() and old_cluster2_callback
 * are used for the two different legacy message types sent from source cluster
 * to target cluster to get a list of IPs on nodes in the target cluster.
 *
 * Message types received from different versions of SyncIQ:
 *
 *     - OLD_CLUSTER_MSG:
 *         = From source cluster running OneFS 5.0 / SIQ 2.0.
 *         = Respond with the list of IPs within another OLD_CLUSTER_MSG reply
 *           to the source.
 *         = No ERR_MSG reply can be sent.
 *         = No target restricting.
 *
 *     - OLD_CLUSTER_MSG2:
 *         = From source cluster running OneFS 5.5 / SIQ 2.5.
 *         = Respond with the list of IPs within an OLD_CLUSTER_MSG reply to
 *           the source.
 *         = ERR_MSG reply can be sent.
 *         = Source can request target_restrict.
 *         = Unique run_id for each job
 */

static int
old_cluster_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct nodex *nodes = NULL;
	struct isi_error *error = NULL;
	char *msg = NULL;
	int n;

	/* restrict results to IPv4 */
	n = get_local_cluster_info(&nodes, NULL, NULL, NULL, &g_dst_addr, NULL,
	    false, true, &msg, &error);

	if (msg) {
		log(NOTICE, "%s", msg);
		free(msg);
		msg = NULL;
	}

	legacy_send_lnn_ip_pairs(sw_ctx, nodes, n);
	exit(EXIT_SUCCESS);
}

static int
old_cluster2_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct nodex *nodes = NULL;
	struct isi_error *error = NULL;
	int n = 0;
	char *restrict_name = NULL;
	char *policy_id = NULL;
	char *msg = NULL;
	uint64_t run_id = 0;

	log(DEBUG, "%s: Source cluster is running OneFS 5.5",
	    __func__);
	restrict_name = m->body.old_cluster2.name;
	policy_id = m->body.old_cluster2.sync_id;
	run_id = m->body.old_cluster2.run_id;

	/* v2.5 used a a OLD_CLUSTER_MSG2 with a zero run_id to indicate that
	 * the sync run file needed to be cleaned up (and nothing else
	 * expected, so the sworker exits). (v2.6 uses the separate
	 * CLEANUP_SYNC_RUN_MSG to do so.) */
	if (run_id == 0) {
		remove_syncrun_file(policy_id);
		exit(EXIT_SUCCESS);
	};

	create_syncrun_file(policy_id, run_id, &error);

	UFAIL_POINT_CODE(legacy_fail_syncrun_create,
	    error = isi_system_error_new(EIO,"sync run fail"));

	if (error)
		send_error_and_exit(sw_ctx->global.primary, error);

	/* If restrict_name is non-NULL and refers to a zone name on this
	 * cluster, passing it to get_local_cluster_info() will result in only
	 * getting info on nodes in the zone. */
	if (restrict_name && *restrict_name)
		log(TRACE, "%s: restricted target name=%s", __func__,
		    restrict_name);

	/* restrict results to IPv4 */
	n = get_local_cluster_info(&nodes, NULL, NULL, NULL, &g_dst_addr,
	    restrict_name, false, true, &msg, &error);

	if (msg) {
		log(NOTICE, "%s", msg);
		free(msg);
		msg = NULL;
	}

	if (n == 0)
		send_error_and_exit(sw_ctx->global.primary, error);

	legacy_send_lnn_ip_pairs(sw_ctx, nodes, n);
	exit(EXIT_SUCCESS);
}


/* This is the pre-5.1 version of the data callback and is still used
 * when talking to 5.0 and earlier pworkers. */
static int
old_data_callback(struct generic_msg *fd, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;

	int got;
	unsigned msg_checksum, checksum_result;

	ASSERT(global->job_ver.common < MSG_VERSION_SBONNET);
	/* SyncIQ never purposely uses fd 0 for file data, so any use
	 * of it is unintentional and a sign of a bug. */
	ASSERT(inprog->fstate.fd != 0);

	log(TRACE, "data_callback");
	if (!fd->body.old_data.size) {
		old_finish_file(sw_ctx);
		return 0;
	}

	if (inprog->fstate.fd == -1) {
	 /* This DATA_MSG is file data, but error has occurred before */
		return 0;
	}

	if (global->checksum_size) {
		ASSERT(global->checksum_size == 4);
		msg_checksum = *(u_int32_t *)fd->body.old_data.data;
		checksum_result = sworker_checksum_compute(
		    fd->body.old_data.data + 4, fd->body.old_data.size - 4);

		if (msg_checksum != checksum_result) {
			log(ERROR, "Checksum error on file %s occurred "
			    "(0x%x/0x%x)", inprog->fstate.name,
			    checksum_result, msg_checksum);
			/* XXX: Intentional that we don't send an error? */
			close(inprog->fstate.fd);
			inprog->fstate.fd = -1;
			inprog->result = ACK_RES_ERROR;
			sw_ctx->stats.checksum_errors++;
			goto out;
		}
	}

	got = write(inprog->fstate.fd, 
	    fd->body.old_data.data + global->checksum_size,
	    fd->body.old_data.size - global->checksum_size);
	if (got != (int)(fd->body.old_data.size - global->checksum_size)) {
		errno = got == -1 ? errno : EIO;
		inprog->result = ACK_RES_ERROR;
		ioerror(global->primary, worker->cur_path.utf8path,
		    inprog->file_obj ? (inprog->ads ? inprog->sstate.name :
		    inprog->fstate.name) : NULL, 
		    inprog->file_obj ? (inprog->ads ? inprog->sstate.enc :
		    inprog->fstate.enc) : 0, inprog->file_obj ? (inprog->ads ? 
		    inprog->sstate.src_filelin : inprog->fstate.src_filelin) : 
		    inprog->dstate.src_dirlin,
		    "Failed to write data to a file in %s", 
		    worker->cur_path.path);
		close(inprog->fstate.fd);
		inprog->fstate.fd = -1;
		sw_ctx->stats.io_errors++;
	}
	if (got != -1)
		sw_ctx->stats.curfile_size += got;

out:
	return 0;
}

static int
old_symlink_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct old_symlink_msg4 *msg4 = &m->body.old_symlink4;
	struct isi_error *error = NULL;
	struct generic_msg amsg;
	struct timeval tvs[2];
	int fd;
	enc_t nenc = ENC_DEFAULT, tenc = ENC_DEFAULT; /* XXX */
	ASSERT(global->job_ver.common < MSG_VERSION_SBONNET);

	log(TRACE, "symlink_callback");
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");

	if (global->job_ver.common < MSG_VERSION_SBONNET)
		old_finish_file(sw_ctx);
	else
		ASSERT(inprog->fstate.fd == -1);
	ASSERT(worker->cwd_fd != -1);
	ASSERT(inprog->dstate.dir_valid);

	amsg.head.type = OLD_ACK_MSG2;
	amsg.body.old_ack2.filelin = msg4->filelin;
	log(TRACE, "ack2 filelin: %lld", amsg.body.old_ack2.filelin);

	amsg.body.old_ack2.result = ACK_RES_OK;
	amsg.body.old_ack2.file = msg4->name;
	amsg.body.old_ack2.checksum_errors = sw_ctx->stats.checksum_errors;
	amsg.body.old_ack2.io_errors = sw_ctx->stats.io_errors;
	amsg.body.old_ack2.transfered = sw_ctx->stats.ads_of_curfile +1;
	amsg.body.old_ack2.filesize = sw_ctx->stats.curfile_size;

	sw_ctx->stats.checksum_errors = 0;
	sw_ctx->stats.io_errors = 0;
	sw_ctx->stats.ads_of_curfile = 0;
	sw_ctx->stats.curfile_size = 0;

	nenc = msg4->nenc;
	tenc = msg4->tenc;

	amsg.body.old_ack2.enc = msg4->nenc;
	amsg.body.old_ack2.rev = msg4->rev;
	log(TRACE, "Get FILE_MSG with filerev = %lld", msg4->rev);
	amsg.body.old_ack2.dir_lin = msg4->dir_lin;

	if (enc_symlinkat(worker->cwd_fd, msg4->target, tenc, msg4->name,
	    nenc) != 0) {
		if (errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "Failed to create symlink %s/%s",
			    worker->cur_path.path, msg4->name);
			goto out;
		}

		delete(msg4->name, nenc, sw_ctx, &error);
		if (error) {
			isi_error_free(error);
			error = isi_system_error_new(errno,
			    "Failed to delete existing file %s/%s",
			    worker->cur_path.path, msg4->name);
			goto out;
		}

		if (enc_symlinkat(worker->cwd_fd, msg4->target, tenc,
		    msg4->name, nenc) != 0) {
		    	error = isi_system_error_new(errno,
			    "Failed to create symlink %s/%s",
			    worker->cur_path.path, msg4->name);
			goto out;
		}
	}

	log(COPY, "Writing symlink %s/%s", worker->cur_path.path, msg4->name);
	fd = enc_openat(worker->cwd_fd, msg4->name, nenc, 
	    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open symlink %s/%s", worker->cur_path.path,
		    msg4->name);
		goto out;
	}

	fchown(fd, (uid_t)msg4->uid, (gid_t)msg4->gid);
	fchmod(fd, (mode_t)msg4->mode);
	fchflags(fd, (fflags_t)msg4->di_flags);
	tvs[0].tv_sec = msg4->atime;
	tvs[0].tv_usec = 0;
	tvs[1].tv_sec = msg4->mtime;
	tvs[1].tv_usec = 0;
	futimes(fd, tvs);
	close(fd);

	log(TRACE, "Send ACK_MSG2 for symlink"); 

	msg_send(global->primary, &amsg);
	return 0;

out:
	if (error) {
		amsg.body.old_ack2.result = ACK_RES_ERROR;
		log(TRACE, "%s", isi_error_get_message(error));
		ioerror(global->primary, worker->cur_path.utf8path, msg4->name,
		    nenc, msg4->filelin, "%s", isi_error_get_message(error));
		amsg.body.old_ack2.io_errors++;
		msg_send(global->primary, &amsg);
		isi_error_free(error);
	}
	return -1;
}

/**
 * Appends new list "list" to tlp
 */
static int
listjoin(struct filelist *tlp, const char *list, unsigned used)
{
	log(TRACE, "listjoin");

	if (!tlp || !list || !used)
		goto error;

	if (tlp->used + used > tlp->size) {
		void *nlp = realloc(tlp->list, tlp->size = tlp->used + used);
		if (NULL == nlp) {
			if (tlp->list) {
				free(tlp->list);
				tlp->list = NULL;
			}
			tlp->used = tlp->size = 0;
			goto error;
		}
		tlp->list = (char *)nlp;
	}

	if (used > 1) {
		memcpy(tlp->list + tlp->used, list, used);
		tlp->used += used;
	} 

	return 0;

error:
	return -1;
}

static void
old_process_list(const char *list, int len, struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_delete_ctx *del = &sw_ctx->del;
	struct migr_dir *mdir;
	struct migr_dirent *mde;
	struct isi_error *error = NULL;
	enum file_type type;
	int ops = 0;
	int dup_fd;
	bool found;

	log(TRACE, "old_delete");

	dup_fd = dup(sw_ctx->worker.cwd_fd);
	if (dup_fd < 0) {
		error = isi_system_error_new(errno, "dup failed");
		log(ERROR, "dup failed (%d)", errno);
		goto out;
	}
	mdir = migr_dir_new(dup_fd, NULL, true, &error);
	if (error) {
		log(ERROR, "error creating migr dir");
		close(dup_fd);
		goto out;
	}

	/* find_in_list that lets it start each search where the previous
	 * search ended */
	del->bookmark = 0;

	/* look at each local entry, skipping unsupported types. delete the
	 * entry if it isn't in the (keep) list */
	while ((mde = migr_dir_read(mdir, &error))) {
		/* periodically check pworker connection */
		if ((ops++ % 1000) == 0) {
			check_connection_with_primary(sw_ctx, &error);
			if (error)
				exit(EXIT_SUCCESS);
		}

		/* skip unsupported types */
		type = stat_mode_to_file_type(mde->stat->st_mode);
		if (type != SIQ_FT_DIR &&
		    type != SIQ_FT_SYM &&
		    type != SIQ_FT_REG) {
			migr_dir_unref(mdir, mde);
			continue;
		}

		found = entry_in_list(mde->dirent->d_name,
		    mde->dirent->d_encoding, list, len, sw_ctx);

		if (!found) {
			/* entry is not in the list so delete it */
			delete(mde->dirent->d_name, mde->dirent->d_encoding,
			    sw_ctx, &error);
			isi_error_free(error);
			error = NULL;
		}

		migr_dir_unref(mdir, mde);
	}

	migr_dir_free(mdir);

out:
	isi_error_free(error);
	return;
}

static int
old_list_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
        struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
        struct filelist *tlp = &inprog->files[inprog->ads];
	bool is_ads = inprog->ads;
	struct isi_error *error = NULL;

        log(TRACE, "old_list_callback");
        if (!inprog->incmp[is_ads] && m->body.list.used > 0 &&
            listjoin(tlp, m->body.list.list, m->body.list.used)) {
		log(INFO, "failed to join %s lists, used %u, arrived %u",
		    is_ads ? "stream" : "file", tlp->used,
		    m->body.list.used);
		/* Disable deletes */
		inprog->incmp[is_ads] = 1;
	}

        if (m->body.list.last) {
		log(DEBUG, "%s list complete, size %u",
		    is_ads ? "stream" : "file", tlp->used);

		if (!inprog->incmp[is_ads]) {
			old_process_list(tlp->list, tlp->used, sw_ctx);
			listreset(tlp);
		}

		if (!is_ads)
			finish_dir(sw_ctx, &error);

		inprog->incmp[is_ads] = 0;
	}

	sw_process_fs_error(sw_ctx, SW_ERR_DIR, inprog->dstate.name, 
	    inprog->dstate.enc, inprog->dstate.src_dirlin, error);
	isi_error_free(error);
	return 0;
}

static int
old_work_init5_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	FILE *f;
	struct generic_msg auth_msg = {};
	struct old_work_init_msg6 *init = &m->body.old_work_init6;
	char _pass[1024 + sizeof(int)] = {0}, *pass = _pass + sizeof(int);
	char proctitle[100];
	struct isi_error *isierror = NULL;
	char *job_name = NULL;

	global->loglevel = init->loglevel;
	set_siq_log_level(global->loglevel);

	/* Set ILOG job name to "policy name":"run id" */
	asprintf(&job_name, "%s:%llu", init->job_name, init->run_id);
	ilog_running_app_job_set(job_name);

	/* Change process title to include the source cluster and policy
	 * names */
	snprintf(global->src_cluster_name, sizeof(global->src_cluster_name),
	    "%s", init->peer);
	cluster_name_from_node_name(global->src_cluster_name);
	snprintf(proctitle, sizeof(proctitle), "%s.%s",
	    global->src_cluster_name, init->job_name);
	setproctitle(proctitle);

	log(TRACE, "%s", __func__);
	log(NOTICE, "Starting worker %d on '%s' from %s",
	    init->worker_id, init->job_name,
	    inx_addr_to_str(&g_src_addr));

	/* Reload config, and verify we're licensed and up */
	if (global_config)
		siq_gc_conf_free(global_config);

	global_config = siq_gc_conf_load(&isierror);
	if (global_config == NULL) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "Failed to load global configuration %s %s",
		    (isierror ? ":" : ""),
		    (isierror ? isi_error_get_message(isierror) : ""));
	}
	siq_gc_conf_close(global_config);

	if (global_config->root->application->max_sworkers_per_node <= 0)
		global_config->root->application->max_sworkers_per_node = 100;

	log(TRACE, "Action %d", init->action);
	if (!SIQ_LIC_OK(init->action)) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "Unable to process policy '%s' from '%s': "
		    "insufficient licensing",
		    init->job_name,
	    	    inx_addr_to_str(&g_src_addr));
	}
	global->action = init->action;

	if (get_child_count() > 
	    global_config->root->application->max_sworkers_per_node) {
		log(FATAL, "sworkers per node limit (%d) exceeded",
		    global_config->root->application->max_sworkers_per_node);
	}

	if (global_config->root->state != SIQ_ST_ON) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "%s on local host is %s",
		    SIQ_APP_NAME,
		    siq_state_to_text(global_config->root->state));
	}

	/* Set up some fields based on the set parameters. */
	path_init(&global->source_base_path, init->source_base, NULL);
	global->source_base_path_len = strlen(init->source_base) + 1;
	path_init(&global->target_base_path, init->target_base, NULL);

	global->skip_bb_hash = init->skip_bb_hash;

	global->checksum_size = (init->flags & FLAG_NO_INTEGRITY)
	    ? 0 : CHECKSUM_SIZE;

	global->logdeleted = (bool)(init->flags & FLAG_LOGDELETES);

	global->stf_sync = (bool)(init->flags & FLAG_STF_SYNC);
	global->initial_sync = (bool)(init->flags & FLAG_CLEAR);

	/* sanity check: stf upgrade sync cannot be an initial sync, and must
	 * be a stf sync */
	global->stf_upgrade_sync = (bool)(init->flags & FLAG_STF_UPGRADE_SYNC);
	if (global->stf_upgrade_sync) {
		log(DEBUG, "STF upgrade sync");
		ASSERT(global->stf_sync);
		ASSERT(!global->initial_sync);
	}

	if (global->logdeleted)
		log(TRACE, "Log deleted files on destination cluster");

	init_statistics(&sw_ctx->stats.tw_cur_stats);
	init_statistics(&sw_ctx->stats.tw_last_stats);

	/* Attempt to authenticate. */
	auth_msg.body.auth.salt = (int)time(0);
	f = fopen(global_config->root->coordinator->passwd_file, "r");
	if (!f || fscanf(f, "%s", pass) != 1) {
		log(DEBUG, "%s missing or bad, no authentication",
		    global_config->root->coordinator->passwd_file);
		strcpy(pass, "");
	}
	if (f)
		fclose(f);

	auth_msg.head.type = AUTH_MSG;
	auth_msg.body.auth.pass = 0;
	msg_send(global->primary, &auth_msg);

	/* Get us started in the correct directory. */
	path_init(&worker->cur_path, init->target_base, 0);

	memcpy(_pass, &auth_msg.body.auth.salt, sizeof(int));
	global->secret = MD5Data(_pass, sizeof(int) + 
	    strlen(_pass + sizeof(int)), 0);
	log(DEBUG, "Generated secret of %s", global->secret);

	inprog->result = ACK_RES_OK;
	sw_ctx->stats.io_errors = 0;
	sw_ctx->stats.checksum_errors = 0;

	/* Only OneFS 5.5+ will have sync_id set */
	if (init->sync_id)
		global->sync_id = strdup(init->sync_id);
	else
		global->sync_id = NULL;

	global->run_id = init->run_id;

	/* Create/open linmap for STF based syncs */
	if (global->stf_sync) {
		/* The linmap is created on the initial sync. The workers race
		 * to create the linmap, so a failed create_linmap call isn't
		 * a disaster -- a failed open is */
		if (global->initial_sync || global->stf_upgrade_sync) {
			/* XXX name needs to be made collision safe. Use tdb
			 * domain ID when available? */
			create_linmap(init->sync_id, init->sync_id, true,
			    &isierror);
			if (isierror)
				log(FATAL, "Failed to create linmap %s: %s",
				    init->sync_id, strerror(errno));
			else
				log(DEBUG, "Created linmap %s", init->sync_id);
		}
		open_linmap(init->sync_id, init->sync_id, &global->lmap_ctx,
		    &isierror);
		if (isierror)
			log(FATAL, "Error opening linmap %s : %s",
			    init->sync_id, strerror(errno));
	}

	if (init->hash_type != HASH_NONE) {
		global->doing_diff_sync = true;
		log(TRACE, "%s: Running diff sync. hash type=%d",
		    __func__, init->hash_type);
		global->hash_type = init->hash_type;
		if (global->hasher == 0) {
			log(DEBUG, "%s: Starting hasher", __func__);
			global->hasher = fork_hasher(SWORKER, sw_ctx, -1,
			    init->hash_type, init->min_hash_piece_len,
			    init->hash_pieces_per_file, global->loglevel, 
			    global_config->root->bwt->cpu_port);

			migr_register_callback(global->primary,
			    HASH_STOP_MSG, forward_to_hasher_callback);

			migr_register_callback(global->primary,
			    HASH_QUIT_MSG, forward_to_hasher_callback);

			migr_register_callback(global->hasher,
			    FULL_HASH_FILE_RESP_MSG,
			    forward_to_pworker_callback);

			migr_register_callback(global->hasher,
			    HASH_ERROR_MSG, forward_to_pworker_callback);

			migr_register_callback(global->hasher,
			    DISCON_MSG, disconnect_callback);
		}
	}
	else
		global->doing_diff_sync = false;

	/* If source is older version without target awareness, enter policy
	 * data in target database. With target awareness, this is done by
	 * the tmonitor. */
	if (!(init->flags & FLAG_HAS_TARGET_AWARE))
		handle_non_target_aware_source(init, global);
	
	ifs_setnoatime(true); /* Ignore errors, if any */
	isi_error_free(isierror);
	if (job_name)
		free(job_name);
	return 0;
}

static int
old_file_begin_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct old_file_begin_msg *old_begin = &m->body.old_file_begin_msg;
	struct generic_msg m_out = {};
	struct file_begin_msg *begin = &m_out.body.file_begin_msg;
	struct file_state *fstate = NULL;
	struct stat st;
	int ret;
	bool file_exists;
	bool success;
	int nlinks;
	struct isi_error *error = NULL;
	struct isi_error *dummy_error = NULL;
	enum file_type type = old_begin->file_type;
	unsigned int dev_major = 0, dev_minor = 0;
	uint64_t lin_out = 0;

	/* translate old message to current version */
	begin->src_lin = old_begin->src_lin;
	begin->src_dlin = old_begin->src_dlin;
	begin->src_dkey = old_begin->src_dkey;
	begin->size = old_begin->size;
	begin->file_type = old_begin->file_type;
	begin->di_flags = old_begin->di_flags;
	begin->mode = old_begin->mode;
	begin->uid = old_begin->uid;
	begin->gid = old_begin->gid;
	begin->atime_sec = old_begin->atime;
	begin->atime_nsec = 0;
	begin->mtime_sec = old_begin->mtime;
	begin->mtime_nsec = 0;
	begin->acl = old_begin->acl;
	begin->full_transfer = old_begin->full_transfer;
	begin->fname = old_begin->fname;
	begin->fenc = old_begin->fenc;
	begin->symlink = old_begin->symlink;
	begin->senc = old_begin->senc;
	begin->sync_state = old_begin->sync_state;
	begin->short_hashes_len = old_begin->short_hashes_len;
	begin->short_hashes = old_begin->short_hashes;
	begin->skip_bb_hash = false;

	ASSERT(global->job_ver.common >= MSG_VERSION_SBONNET);

	/* pworker should not be sending unknown or unsupported types */
	ASSERT(type != SIQ_FT_UNKNOWN);

	log(TRACE, "%s", __func__);
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");

	ASSERT(worker->cwd_fd > 0);
	ASSERT(inprog->dstate.dir_valid || global->stf_sync);

	log(TRACE, "Get file %s(%d)", begin->fname, begin->fenc);

	if (!inprog->ads)
		inprog->file_obj = true;

	UFAIL_POINT_CODE(old_begin_fstatat_fail,
	    error = isi_system_error_new(RETURN_VALUE,
	    "Failed to stat %s/%s", worker->cur_path.path, begin->fname);
	    goto out);

	/*
	 * Stat the name to see whether we need to delete an existing
	 * file.
	 */
	ret = enc_fstatat(worker->cwd_fd, begin->fname, begin->fenc,
	    &st, AT_SYMLINK_NOFOLLOW);

	if (ret == 0) {
		file_exists = true;
		nlinks = st.st_nlink;
	} else if (errno == ENOENT) {
		file_exists = false;
		nlinks = 0;
	} else {
		error = isi_system_error_new(errno, "Failed to stat %s/%s",
		    worker->cur_path.path, begin->fname);
		goto out;
	}

	/*
	 * Clear immutable/append only flags if set and if the
	 * file is a regular file.
	 */
	if (file_exists && type == SIQ_FT_REG && S_ISREG(st.st_mode) &&
	    (st.st_flags & IMMUT_APP_FLAG_MASK)) {
		clear_flags(&st, IMMUT_APP_FLAG_MASK,
		    begin->fname, begin->fenc, &error);
		if (error)
			goto out;
	}

	/*
	 * Return a pointer to sw_ctx->inprog.fstate for non-diff sync
	 * or a dynamic fstate for diff sync.
	 */
	fstate = choose_or_create_fstate(sw_ctx, begin,
	    file_exists, nlinks, &error);
	if (error)
		goto out;

	fstate->type = type;
	fstate->orig_is_reg_file = S_ISREG(st.st_mode);

	/*
	 * If regular sync, we delete the old file and create and open the new
	 * file here. For diff sync, the file is only opened/created if this
	 * is a known NEEDS_SYNC.  Otherwise for diff sync, the file is
	 * UNDECIDED and we hold off opening/creating the file until later.
	 */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx) &&
	    !(fstate->sync_state == NEEDS_SYNC))
		goto out;

	/* In the case of diff sync and NEEDS_SYNC, if the file
	 * exists, it has already been opened. It has to be closed so
	 * we can delete it and create the new one.
	 */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx) && fstate->fd != -1) {
		close(fstate->fd);
		fstate->fd = -1;
	}

	/*
	 * Delete the existing file if something is already in
	 * this location and either the new file we're creating
	 * is a symlink, or the existing file isn't a
	 * normal file, or the existing file has multiple links.
	 */
	if (file_exists && (type == SIQ_FT_SYM || type == SIQ_FT_CHAR ||
	    type == SIQ_FT_BLOCK || type == SIQ_FT_SOCK ||
	    !fstate->orig_is_reg_file || nlinks > 1)) {
		delete(begin->fname, begin->fenc, sw_ctx, &error);
		if (error)
			goto out;
	}

	UFAIL_POINT_CODE(old_begin_open_or_create_fail,
	    error = isi_system_error_new(RETURN_VALUE,
	    "Failed to open %s/%s", worker->cur_path.path,
	    begin->fname); goto out);

	/* Hard link handling for stf initial/upgrade syncs */
	if (global->stf_sync && (global->initial_sync ||
	    global->stf_upgrade_sync) && !sw_ctx->inprog.ads) {
		/* If the slin has already been mapped, this is a multiply
		 * linked file on the source, so create a hard link to the dlin
		 * instead of a new file. */
		
		success = get_mapping(begin->src_lin, &lin_out,
		    global->lmap_ctx, &error);
		if (error)
			goto out;

		if (success) {
			fstate->fd = make_hard_link(begin->fname, begin->fenc,
			    lin_out, worker->cwd_fd, sw_ctx, &error);
			goto out;
		}
	}

	switch (type) {
	case SIQ_FT_REG:
		fstate->fd = open_or_create_reg(sw_ctx, worker->cwd_fd,
		    begin->fname, begin->fenc, &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_SYM:
		fstate->fd = open_or_create_sym(sw_ctx, worker->cwd_fd,
		    begin->symlink, begin->senc, begin->fname,
		    begin->fenc, &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_CHAR:
	case SIQ_FT_BLOCK:
		ASSERT(sscanf(begin->symlink, "%u %u",
		    &dev_major, &dev_minor) == 2);
		fstate->fd = open_or_create_special(sw_ctx, worker->cwd_fd,
		    begin->fname, begin->fenc, type, dev_major,
		    dev_minor, &lin_out, &error);
		if (error)
			goto out;
		break;

	case SIQ_FT_SOCK:
		fstate->fd = open_or_create_socket(sw_ctx, worker->cwd_fd,
		    begin->fname, begin->fenc, &lin_out, &error);
		if (error)
			goto out;
		break;

	default:
		ASSERT(false);
	}

	/* Add source and target LINs to the lin map. If this is an initial
	 * sync and the mapping fails, another worker beat us to it. Delete the
	 * newly created file and instead create a link to the dlin */
	if (global->stf_sync && !sw_ctx->inprog.ads) {
		log(DEBUG, "Adding lin mapping %llx->%llx for file %s",
		    begin->src_lin, lin_out, begin->fname);

		ASSERT(lin_out);
		if (global->initial_sync || global->stf_upgrade_sync) {
			success = set_mapping_cond(begin->src_lin, lin_out,
			    global->lmap_ctx, true, 0, &error);
			if (error)
				goto out;

			if (!success) {
				/* Another worker has created and mapped the
				 * file. Delete the one just created and
				 * instead link to the mapped dlin */
				delete(begin->fname, begin->fenc, sw_ctx,
				    &error);
				close(fstate->fd);
				fstate->fd = -1;
				if (error)
					goto out;

				get_mapping(begin->src_lin, &lin_out,
				    global->lmap_ctx, &error);
				if (error)
					goto out;

				fstate->fd = make_hard_link(begin->fname,
				    begin->fenc, lin_out, worker->cwd_fd,
				    sw_ctx, &error);
				if (error)
					goto out;
			}
		} else {
			set_mapping(begin->src_lin, lin_out,
			    global->lmap_ctx, &error);
			if (error)
				goto out;
		}
	}
	/* Save this file hash so that we can use for upgrades */
	inprog->dstate.last_file_hash = begin->src_dkey;

out:
	ASSERT(error || fstate);

	if (error) {
		/*
		 * If we hit an error early on, we may not have an fstate and
		 * need to create one now for use by file_data_msg and
		 * file_done_msg.  We want to force a sync in this case to
		 * go through the normal error path.
		 */
		if (!fstate) {
			fstate = choose_or_create_fstate(sw_ctx, begin,
			    false, 0, &dummy_error);
			isi_error_free(dummy_error);
			dummy_error = NULL;
		}
		if (fstate->fd != -1) {
			close(fstate->fd);
			fstate->fd = -1;
		}
		inprog->result = ACK_RES_ERROR;
		if (error && isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
			errno = isi_system_error_get_error(
			    (const struct isi_system_error *)error);
		else
			errno = EIO;
		ioerror(global->primary, worker->cur_path.utf8path,
		    begin->fname, begin->fenc, begin->src_lin,
		    "Failed to open %s %s/%s for write",
		    file_type_to_name(type), worker->cur_path.path,
		    begin->fname);
		sw_ctx->stats.io_errors++;
		fstate->error_closed_fd = true;
	}

	/* Initialize fstate with the data from begin */
	initialize_fstate_fields(sw_ctx, fstate, begin);
 
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
		TAILQ_INSERT_TAIL(inprog->fstates, fstate, ENTRIES);
		inprog->fstates_size++;
	}

	log(TRACE, "Remember source file's lin %lld",
	    (u_int64_t)begin->src_lin);

	sw_ctx->stats.ads_of_curfile++;
	isi_error_free(error);
	return 0;
}

void
migr_sworker_register_legacy(struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	migr_register_callback(global->primary, OLD_DATA_MSG,
	    old_data_callback);
	migr_register_callback(global->primary, OLD_WORK_INIT_MSG,
	    old_work_init_callback);
	migr_register_callback(global->primary, OLD_WORK_INIT_MSG4,
	    old_work_init4_callback);
	migr_register_callback(global->primary, OLD_DIR_MSG3,
	    old_dir3_callback);
	migr_register_callback(global->primary, OLD_FILE_MSG4,
	    old_file_callback);
	migr_register_callback(global->primary, OLD_SYMLINK_MSG4,
	    old_symlink_callback);
 	migr_register_callback(global->primary, OLD_CLUSTER_MSG,
 	    old_cluster_callback);
 	migr_register_callback(global->primary, OLD_CLUSTER_MSG2,
 	    old_cluster2_callback);
	migr_register_callback(global->primary, LIST_MSG,
	    old_list_callback);
	migr_register_callback(global->primary, OLD_WORK_INIT_MSG5,
	    old_work_init5_callback);
	migr_register_callback(global->primary, OLD_FILE_BEGIN_MSG,
	    old_file_begin_callback);
}

char *
make_5_0_policy_id(char *src_cluster_name, char *policy_name,
    char *target_dir)
{
	char *policy_id_5_0;

	/* The best we can do is for the policy_id from a 5.0 source
	 * is cluster_name.policy_name */

	// A 5.0 policy ID in the TDB is src_cluster_name.policy_name
	asprintf(&policy_id_5_0, "%s.%s.%s",
	    src_cluster_name, policy_name, target_dir);
	ASSERT(policy_id_5_0);
	return policy_id_5_0;
}

struct target_record *
check_for_5_0_policy(struct target_records *tdb, char *src_cluster_name,
    char *policy_name, char *target_dir)
{
	char *policy_id_5_0;
	struct target_record *trec;

	policy_id_5_0 = make_5_0_policy_id(src_cluster_name, policy_name,
	    target_dir);
	trec = tdb->record;
	while (trec) {
		/* If this is a matching 5.0 policy, then this policy
		 * is OK. */
		if (match_5_0_policy(policy_id_5_0, trec)) {
			log(DEBUG, "Policy %s was last used from a "
			    "5.0 cluster ", policy_id_5_0);
			break;
		}
		trec = trec->next;
	}
	
	free(policy_id_5_0);
	return trec;
}

bool
remove_duplicate_5_0_policy(char *policy_id, struct isi_error **error_out)
{
	int lock_fd = -1, lock_fd_5_0 = -1;
	bool ret = false;
	char *policy_id_5_0 = NULL;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	/* Lock and check existence of 6.5+ version target record */
	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (error)
		goto out;

	if (!siq_target_record_exists(policy_id))
		goto out;

	trec = siq_target_record_read(policy_id, &error);
	if (error)
		goto out;

	/* Lock and check existence of 5.0 version target record */
	policy_id_5_0 = make_5_0_policy_id(trec->src_cluster_name,
	    trec->policy_name, trec->target_dir);

	lock_fd_5_0 = siq_target_acquire_lock(policy_id_5_0, O_EXLOCK, &error);
	if (error)
		goto out;

	if (!siq_target_record_exists(policy_id_5_0))
		goto out;

	/* If here then both records exist, better delete the 5.0 version */
	siq_target_delete_record(policy_id_5_0, true, true, &error);
	if (error)
		goto out;

	ret = true;

out:

	if (lock_fd_5_0 != -1)
		close(lock_fd_5_0);

	if (lock_fd != -1)
		close(lock_fd);

	if (policy_id_5_0)
		free(policy_id_5_0);

	siq_target_record_free(trec);

	isi_error_handle(error, error_out);
	return ret;
}

enum legacy_version {SIQ_UNKNOWN, SIQ_5_0, SIQ_5_5};

static struct target_record *
get_prev_legacy_policy_id(char *policy_id, char *policy_id_5_0,
    enum legacy_version *prev_legacy_type, struct isi_error **error_out)
{
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	*prev_legacy_type = SIQ_UNKNOWN;

	/* First look for regular policy ID */
	if (policy_id && siq_target_record_exists(policy_id)) {
		trec = siq_target_record_read(policy_id, &error);
		if (error)
			goto out;

		*prev_legacy_type = SIQ_5_5;
		goto out;
	}

	/* Then look by 5.0 pseudo policy ID */
	if (policy_id_5_0 && *prev_legacy_type != SIQ_5_5 &&
	    siq_target_record_exists(policy_id_5_0)) {
		trec = siq_target_record_read(policy_id_5_0, &error);
		if (error)
			goto out;

		*prev_legacy_type = SIQ_5_0;
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return trec;
}

void
handle_non_target_aware_source(const struct old_work_init_msg6 *init,
    struct migr_sworker_global_ctx *global)
{
	int lock_fd = -1, lock_fd_5_0 = -1, i;
	struct target_records *tdb = NULL;
	struct target_record *trec = NULL;
	struct isi_error *isierror = NULL;
	char *policy_id = NULL, *policy_id_5_0 = NULL, *errmsg = NULL;
	char *add_record_policy_id = NULL;
	enum legacy_version prev_legacy_type = SIQ_UNKNOWN,
	    current_legacy_type = SIQ_UNKNOWN;
	bool add_record, upgrade_policy_id, update_timestamp;
	const int JOB_STATUS_TIME_DIFF_ALLOWED = 120;
	int lock_type[2] = {O_SHLOCK, O_EXLOCK};

	/* Don't put any samba backup jobs in the TDB */
	if (starts_with(init->target_base, SIQ_IFS_IDMAP_DIR)) {
		log(DEBUG, "%s: Not adding legacy samba job %s to TDB",
		    __func__, init->target_base);
		return;
	}

	/* This differentiates between 5.0 and 5.5 source clusters. 5.5 sends
	 * the policy name as sync_id in the WORK_INIT_MSG4. The 5.0
	 * OLD_WORK_INIT_MSG only includes the policy name, not its ID. But
	 * the policy ID is needed to uniquely match the same policy on a
	 * source cluster after it has been upgraded from 5.0 to
	 * 6.0. Otherwise, after the upgrade the policy would be considered
	 * new and an unneeded full sync will occur.
	 *
	 * Thus, for 5.0 source clusters, a pseudo-ID is made and stored in
	 * the TDB. The pseudo-ID is src_cluster_name.policy_name.target_dir */

	policy_id_5_0 = make_5_0_policy_id(global->src_cluster_name,
	    init->job_name, init->target_base);

	/* Only 5.5+ has sync_id (i.e. policy ID) */
	if (init->sync_id) {
		current_legacy_type = SIQ_5_5;
		policy_id = strdup(init->sync_id);
		log(DEBUG,
		    "Pworker for policy %s is old 5.5 version that is not "
		    "target aware",
		    init->job_name);

		/* Remove duplicate 5.0 target record if it exists. This can
		 * happen if the sworker crashed last time in between saving
		 * the new 5.5+ target record and deleting the old 5.0
		 * record. */
		remove_duplicate_5_0_policy(policy_id, &isierror);
		if (isierror) {
			errmsg = "Faled to remove duplicate 5.0 target record";
			goto done;
		}
	}
	/* Must be 5.0 */
	else {
		current_legacy_type = SIQ_5_0;
		log(DEBUG,
		    "Pworker for policy %s is old 5.0 version that is not "
		    "target aware", init->job_name);
	}

	/* Get a list of target records for target_path_confirm() below */
	siq_target_records_read(&tdb, &isierror);
	if (isierror) {
		errmsg = "Failed to read list of target records";
		goto done;
	}

	/* We need to make sure this policy is in the TDB. If it's not, add
	 * it. If it's already there, its job status timestamp needs to be
	 * updated.
	 *
	 * To do this, in the first iteration of the below loop, obtain a read
	 * lock on the TDB (so that the multiple sworkers for the policy can
	 * get the lock simultaneously). There are four ways to proceed after
	 * looking for the legacy policy record in the TDB:
	 *
	 * 1) If the policy is in the TDB, and its job status timestamp is
	 * recent, no need to update the TDB record.
	 *
	 * 2) If the policy is in the TDB, and its job status timestamp is
	 * _not_ recent, the TDB record needs to have the job status timestamp
	 * updated.
	 *
	 * 3) If the policy is in the TDB, but with a pseudo-ID, and the
	 * current source cluster is now 5.5 and using real policy IDs, the
	 * pseudo ID needs to be replaced with the real ID, and the job status
	 * timestamp needs to be updated.
	 *
	 * 4) If the policy is not in the TDB, it needs to be added.
	 *
	 * In the case that the TDB record needs to be updated with a current
	 * timestamp or policy ID, or there is no TDB record for the policy in
	 * the TDB, the second iteration of the below loop will get a write
	 * lock on the TDB to change the timestamp, or add a new record as
	 * needed.
	 *
	 * But since there will always be a race between workers to be the
	 * first one to update the TDB, after obtaining the write lock, check
	 * to see if another sworker hasn't beaten this sworker to make the
	 * required changes.
	 */

	for (i = 0; i < 2; i++) {

		add_record = false;
		update_timestamp = false;
		upgrade_policy_id = false;

		/* First iteration use read lock. Second iteration use write
		 * lock */
		if (policy_id) {
			lock_fd = siq_target_acquire_lock(policy_id,
			    lock_type[i], &isierror);
			if (lock_fd == -1) {
				errmsg = "Can't get lock on 5.5 policy.";
				goto done;
			}
		}

		lock_fd_5_0 = siq_target_acquire_lock(policy_id_5_0,
		    lock_type[i], &isierror);
		if (lock_fd_5_0 == -1) {
			errmsg = "Can't get lock on 5.0 policy.";
			goto done;
		}

		trec = get_prev_legacy_policy_id(policy_id, policy_id_5_0,
		    &prev_legacy_type, &isierror);
		if (isierror) {
			errmsg = "Failed to read target record.";
			goto done;
		}

		/* There is already record for this policy in the TDB */
		if (trec) {
			log(DEBUG, "%s: Policy %s from %s already in TDB (%d)",
			    __func__, init->job_name, init->peer, i);
			ASSERT(trec->cant_cancel, "The record for legacy "
			    "policy %s from %s has cant_cancel unset",
			    init->job_name, init->peer);

			/* If the source's legacy version has not changed, and
			 * the timestamp has yet to be updated, then note the
			 * need for a timestamp update */
			if (current_legacy_type == prev_legacy_type) {
				if (time(0) - trec->job_status_timestamp >
				    JOB_STATUS_TIME_DIFF_ALLOWED)
					update_timestamp = true;
			}
			else if (prev_legacy_type == SIQ_5_0 &&
			    current_legacy_type == SIQ_5_5) {
				log(DEBUG, "Source cluster has upgraded from "
				    "5.0 to 5.5");
				upgrade_policy_id = true;
				update_timestamp = true;
			}
			/* The only other possibility is that current is 5.0
			 * and previous one was 5.5. There's no way that could
			 * happen as if the previous was 5.5 it would be
			 * stored in the TDB with a proper policy_id, so no
			 * match could be made with a pseudo-ID */
			else
				ASSERT(0,
				    "Absolutely impossible that the previous"
				    "SIQ version was 5.5 and this one is 5.0");
		}
		else
			add_record = true;

		/* If no changes needed, then done */
		if (!update_timestamp && !upgrade_policy_id && !add_record)
			goto done;

		/* Change to the policy record is needed */

		/* If we're on the first iteration (with the read lock), we
		 * have to get a write lock and start again to do the updates
		 * (first freeing the TDB and releasing the read lock) */
		if (lock_type[i] == O_SHLOCK) {
			siq_target_record_free(trec);
			trec = NULL;
			if (lock_fd_5_0 != -1) {
				close(lock_fd_5_0);
				lock_fd_5_0 = -1;
			}
			if (lock_fd != -1) {
				close(lock_fd);
				lock_fd = -1;
			}
		}

		/* On second iteration with the write lock, and updates to the
		 * TDB needed: the loop exits, and take care of updates
		 * below */

	}

	if (add_record) {
		if (current_legacy_type == SIQ_5_5) {
			add_record_policy_id = policy_id;
		} else {
			add_record_policy_id = policy_id_5_0;
		}
		target_path_confirm(tdb, add_record_policy_id, init->job_name, 
		    init->target_base, init->target_base, init->flags, 
		    &isierror);
		if (isierror) {
			errmsg = "Failed target negotiation";
			goto done;
		}

		log(DEBUG, "%s: Put new pre-Hab policy %s from %s in TDB",
		    __func__, init->job_name, init->peer);

		trec = calloc(1, sizeof(struct target_record));
		ASSERT(trec);

		/* Yep, without target awareness in old versions, no target
		 * cancel */
		trec->cant_cancel = true;

		/* Because without target awareness, the target doesn't know
		 * these states */
		trec->job_status = SIQ_JS_NONE;
		trec->cancel_state = CANCEL_INVALID;

		trec->id = strdup(add_record_policy_id);
		trec->target_dir = strdup(init->target_base);
		trec->policy_name = strdup(init->job_name);
		trec->src_cluster_name = strdup(init->peer);
		cluster_name_from_node_name(trec->src_cluster_name);
		trec->last_src_coord_ip = strdup(inx_addr_to_str(&g_src_addr));
		trec->job_status_timestamp = time(0);
		trec->monitor_node_id = get_local_node_id();
	}
	else {
		if (upgrade_policy_id) {
			log(DEBUG, "%s: upgrading 5.0 policy ID to 5.5 in "
			    "existing TDB record from %s to %s for policy %s",
			    __func__, policy_id_5_0, policy_id,
			    init->job_name);
			free(trec->id);
			trec->id = strdup(policy_id);
		}
		log(DEBUG,
		    "%s: update timestamp and src_coord_ip in existing TDB "
		    "record for policy %s",  __func__, init->job_name);

		trec->job_status_timestamp = time(0);
		free(trec->last_src_coord_ip);
		trec->last_src_coord_ip = strdup(inx_addr_to_str(&g_src_addr));
	}

	siq_target_record_write(trec, &isierror);
	if (isierror) {
		errmsg = "Unable to write policy from old version "
		    "of SIQ to target database";
		goto done;
	}

	/* We created a new target record with 5.5+ policy id, remove the
	 * 5.0 target record */
	if (upgrade_policy_id && siq_target_record_exists(policy_id_5_0)) {
		siq_target_delete_record(policy_id_5_0, true, true,
		    &isierror);
	}

done:
	if (isierror) {
		struct fmt FMT_INIT_CLEAN(fmt);
		fmt_print(&fmt, "Target initialization error: %s: %s",
		    errmsg, isi_error_get_message(isierror));
		send_error_msg_and_exit(global->primary, ENOTDIR,
		    fmt_string(&fmt));
	}

	if (lock_fd != -1)
		close(lock_fd);
	if (lock_fd_5_0 != -1)
		close(lock_fd_5_0);
	siq_target_record_free(trec);
	siq_target_records_free(tdb);
	free(policy_id);
	free(policy_id_5_0);
}
