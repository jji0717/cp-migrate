#include <sys/acl.h>
#include <sys/isi_enc.h>
#include <sys/module.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>

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

#include <ifs/ifs_types.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/ifs_procoptions.h>
#include <ifs/ifs_userattr.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_licensing/licensing.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_acl/sd_acl.h>
#include <isi_util/isi_sysctl.h>
#include <isi_util/isi_extattr.h>

#define context kernel_context
#include "isi_snapshot/snapshot.h"
#undef context

#include <isi_domain/worm.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cbm_file_versions.h>
#include <isi_cpool_cbm/isi_cbm_snap_diff.h>
#include <isi_cpool_bkup/isi_cpool_bkup.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>

#define USE_BURST_LOGGING
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/migr/alg.h"
#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/config/siq_btree_public.h"
#include "isi_migrate/migr/repstate.h"

#include "sworker.h"
#include "isi_domain/cstore_links.h"

extern rde_status_t ewoc_init(HostInterface* host);
extern void ewoc_setTraceLevel(ewoc_LogSeverity);

struct siq_gc_conf* global_config;
struct migr_sworker_ctx *g_ctx;
struct int_set prev_up_storage_nodes;

static unsigned char sworker_zero_buff[IDEAL_READ_SIZE];
static void
wait_for_burst_connect(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

void send_cached_file_ack_msgs(struct migr_sworker_ctx *sw_ctx);
void sync_lmap_updates(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin,
    ifs_lin_t dlin, bool sync_only, struct isi_error **error_out);

const char *siq_sw_fs_error_type_to_str[SW_ERR_MAX+1] = {
	"file",
	"dir",
	"ads",
	"lin",
	NULL
};

static int timer_callback(void *ctx);

/*
 * Given a fd, find the corresponding lin and add domain_id to it.
 */
static void
add_to_domain(int fd, uint64_t domain_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	int ret;
	
	ret = fstat(fd, &st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Stat operation failed");
		goto out;
	}
	ret = ifs_domain_add_bylin(st.st_ino, domain_id);
	if (ret != 0) {
		if (errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "ifs_domain_add_bylin failed for lin %llx",
			    st.st_ino);
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
}

/*  _   _ _   _ _     
 * | | | | |_(_) |___ 
 * | | | | __| | / __|
 * | |_| | |_| | \__ \
 *  \___/ \__|_|_|___/
 */

static void
print_common_stat_state(FILE *f, struct common_stat_state *css)
{
	fprintf(f, "    mode: %d\n", css->mode);
	fprintf(f, "    owner: %d\n", css->owner);
	fprintf(f, "    group: %d\n", css->group);
	fprintf(f, "    atime: %.24s (%lld %lld)\n", ctime(&css->atime.tv_sec), 
	    css->atime.tv_sec, css->atime.tv_nsec);
	fprintf(f, "    mtime: %.24s (%lld %lld)\n", ctime(&css->mtime.tv_sec), 
	    css->mtime.tv_sec, css->mtime.tv_nsec);
	fprintf(f, "    acl: %s\n", PRINT_NULL(css->acl));
	fprintf(f, "    reap: %d\n", css->reap);
	fprintf(f, "    size: %lld\n", css->size);
	fprintf(f, "    flags_set: %d\n", css->flags_set);
}
 
static void
print_file_state(FILE *f, struct file_state *fs)
{
	fprintf(f, "  fd: %d\n", fs->fd);
	fprintf(f, "  name: %s\n", PRINT_NULL(fs->name));
	fprintf(f, "  type: %d\n", fs->type);
	fprintf(f, "  src_lin: %llx\n", fs->src_filelin);
	fprintf(f, "  stat_state:\n");
	print_common_stat_state(f, &fs->stat_state);
	fprintf(f, "  cur_off: %lld\n", fs->cur_off);
	fprintf(f, "  bytes_changed: %lld\n", fs->bytes_changed);
	fprintf(f, "  sync_state: %d\n", fs->sync_state);
}
 
static int
dump_status(void *ctx)
{
	char *output_file = NULL;
	FILE *f;
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_delete_ctx *del = &sw_ctx->del;
	struct migr_sworker_comp_ctx *comp = &sw_ctx->comp;
	int i = 0;
	int ret = 1;
	const uint64_t *key_var = NULL, *value_var = NULL;
	struct lmap_log_entry *llog = NULL;
	
	asprintf(&output_file, "/var/tmp/sworker_status_%d.txt", getpid());
	
	f = fopen(output_file, "w");
	if (f == NULL) {
		log(ERROR,"Status generation failed: %s", strerror(errno));
		goto out;
	}
	
	fprintf(f, "SyncIQ job '%s' (%s)\n", global->job_name, global->sync_id);
	
	fprintf(f, "\n****Global Context****\n");
	fprintf(f, "Worker ID: %d\n", global->worker_id);
	fprintf(f, "run_id: %lld\n", global->run_id);
	fprintf(f, "source_base_path: %s\n", 
	    PRINT_NULL(global->source_base_path.path));
	fprintf(f, "target_base_path: %s\n", 
	    PRINT_NULL(global->target_base_path.path));
	fprintf(f, "src_cluster_name: %s\n", global->src_cluster_name);
	fprintf(f, "domain_id: %lld\n", global->domain_id);
	fprintf(f, "domain_generation: %d\n", global->domain_generation);
	fprintf(f, "lmap_log:\n");
	fprintf(f, "  index: %d\n", global->lmap_log.index);
	fprintf(f, "  entries:\n");
	for (i = 0; i < MAX_BLK_OPS; i++) {
		llog = &global->lmap_log.entries[i];
		if (llog->slin != 0 && llog->dlin != 0) {
			fprintf(f, "    [%d] \n", i);
			fprintf(f, "      slin: %llx\n", llog->slin);
			fprintf(f, "      dlin: %llx\n", llog->dlin);
			fprintf(f, "      oper: %d\n", llog->oper);
		} else {
			fprintf(f, "    [%d] {EMPTY} \n", i);
		}
	}
	fprintf(f, "last_ack_time: %.24s (%lld)\n", ctime(&global->last_ack_time), 
	    global->last_ack_time);
	fprintf(f, "root_tmp_lin: %llx\n", global->root_tmp_lin);
	if (global->tmp_working_map) {
		UINT64_UINT64_MAP_FOREACH(key_var, value_var,
		    global->tmp_working_map){
			if (key_var != NULL)
			    fprintf(f, "    tmp_working_map %llx:", *key_var);
			else
			    fprintf(f, "    tmp_working_map {EMPTY}:");

			if (value_var != NULL)
			    fprintf(f, "%llx\n", *value_var);
			else
			    fprintf(f, "{EMPTY}\n");
		}
	} else {
		fprintf(f, "tmp_working_map: NULL\n");
	}
	fprintf(f, "stf_operations: %d\n", global->stf_operations);
	fprintf(f, "last_stf_slin: %llx\n", global->last_stf_slin);
	fprintf(f, "stf_sync: %s\n", PRINT_BOOL(global->stf_sync));
	fprintf(f, "doing_diff_sync: %s\n", PRINT_BOOL(global->doing_diff_sync));
	fprintf(f, "compliance: %s\n", PRINT_BOOL(global->compliance));
	fprintf(f, "compliance_v2: %s\n", PRINT_BOOL(global->compliance_v2));
	fprintf(f, "lin_map_blk: %s\n", PRINT_BOOL(global->lin_map_blk));
	fprintf(f, "lmap_pending_acks: %d\n", global->lmap_pending_acks);
	fprintf(f, "do_bulk_tw_lmap: %s\n", PRINT_BOOL(global->do_bulk_tw_lmap));
	fprintf(f, "bulk_tw_acks_count: %d\n", global->bulk_tw_acks_count);
	
	fprintf(f, "fstate:\n");
	print_file_state(f, &inprog->fstate);
	fprintf(f, "sstate:\n");
	print_file_state(f, &inprog->sstate);
	fprintf(f, "dstate:\n");
	fprintf(f, "  name: %s\n", PRINT_NULL(inprog->dstate.name));
	fprintf(f, "  stat_state:\n");
	print_common_stat_state(f, &inprog->dstate.stat_state);
	fprintf(f, "  src_dirlin: %lld\n", inprog->dstate.src_dirlin);
	fprintf(f, "  dir_valid: %s\n", PRINT_BOOL(inprog->dstate.dir_valid));
	fprintf(f, "  last_file_hash: %llx\n", inprog->dstate.last_file_hash);
	fprintf(f, "ads: %d\n", inprog->ads);
	fprintf(f, "cur_path: %s\n", PRINT_NULL(worker->cur_path.path));
	
	fprintf(f, "delete state:\n");
	fprintf(f, "  mdir:\n");
	print_migr_dir(f, del->mdir, "    ");
	fprintf(f, "  in_progress: %s\n", PRINT_BOOL(del->in_progress));
	fprintf(f, "  hold_queue size: %d\n", 
	    pvec_size(del->hold_queue));
	fprintf(f, "  next_hold_queue size: %d\n", 
	    pvec_size(del->next_hold_queue));
	fprintf(f, "  op_count: %d\n", del->op_count);
	fprintf(f, "  bookmark: %d\n", del->bookmark);
	fprintf(f, "  error: %s\n", PRINT_BOOL(del->error));
	
	if (global->compliance_v2) {
		fprintf(f, "comp_ctx:\n");
		fprintf(f, "  worm_commit_ctx: %p\n", comp->worm_commit_ctx);
		if (comp->worm_commit_ctx) {
			fprintf(f, "    fd: %d\n", 
			    comp->worm_commit_ctx->lin_list_fd);
		}
		fprintf(f, "  worm_commit_log:\n");
		fprintf(f, "    index: %d\n", comp->worm_commit_log.index);
		fprintf(f, "  worm_commit_iter: %p\n", comp->worm_commit_iter);
		fprintf(f, "  comp_map_ctx: %p\n", comp->comp_map_ctx);
		if (comp->comp_map_ctx) {
			fprintf(f, "    fd: %d\n",
			    comp->comp_map_ctx->map_fd);
		}
		fprintf(f, "  comp_map_log:\n");
		fprintf(f, "    index: %d\n", comp->comp_map_log.index);
		fprintf(f, "  comp_map_iter: %p\n", comp->comp_map_iter);
		fprintf(f, "  comp_target_min_lin: %llx\n",
		    comp->comp_target_min_lin);
		fprintf(f, "  comp_target_current_lin: %llx\n",
		    comp->comp_target_current_lin);
		fprintf(f, "  comp_target_max_lin: %llx\n",
		    comp->comp_target_max_lin);
		fprintf(f, "  comp_map_chkpt_lin: %llx\n",
		    comp->comp_map_chkpt_lin);
		fprintf(f, "  process_work: %s\n", 
		    PRINT_BOOL(comp->process_work));
		fprintf(f, "  done: %s\n",
		    PRINT_BOOL(comp->done));
		fprintf(f, "  comp_map_split_req: %s\n",
		    PRINT_BOOL(comp->comp_map_split_req));
		fprintf(f, "  comp_map_ufp: %s\n",
		    PRINT_BOOL(comp->comp_map_ufp));
		fprintf(f, "  work_restart_type: %s\n",
		    work_type_str(comp->rt));
	}

	print_net_state(f);

	print_cpu_throttle_state(f, &sw_ctx->cpu_throttle);
	
	log(NOTICE, "Status information dumped to %s", output_file);
	ret = 0;
out:
	if (f) {
		fclose(f);
		f = NULL;
	}
	free(output_file);
	return ret;
}

static void
sigusr2(int signo)
{
	struct timeval tv = {};

	migr_register_timeout(&tv, dump_status, g_ctx);
}

static struct file_state *
get_inprog_fstate(struct migr_sworker_ctx *sw_ctx)
{
	return DIFF_SYNC_BUT_NOT_ADS(sw_ctx) ? sw_ctx->inprog.ds_tmp_fstate :
	    &sw_ctx->inprog.fstate;
}

time_t last_time = 0;
#define VERIFY_INTERVAL 10

void
verify_run_id(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	time_t cur_time;
	int fd = -1;
	uint64_t file_run_id; 
	ssize_t ret;
	struct isi_error *error = NULL;
	int run_dir_fd = -1;
	char sync_file_name[MAXNAMELEN + 1];

	if (global->sync_id) {
		cur_time = time(0);
		if ((cur_time < last_time)
		    || ((cur_time - last_time) > VERIFY_INTERVAL)) {

			sprintf(sync_file_name, "%s", global->sync_id);
			run_dir_fd = open(SYNCRUN_FILE_DIR, O_RDONLY);
			ASSERT(run_dir_fd != 0);
			if (run_dir_fd < 0) {
				error = isi_siq_error_new(E_SIQ_WRK_DEATH,
				    "Unable to open run dir '%s': %s",
				    SYNCRUN_FILE_DIR, strerror(errno));
				goto out;
			}

			fd = enc_openat(run_dir_fd, sync_file_name,
			    ENC_DEFAULT, O_RDWR|O_NOFOLLOW);
			ASSERT(fd != 0);
			if (fd < 0) {
				error = isi_siq_error_new(E_SIQ_WRK_DEATH,
				    "Unable to open sync run file %s: %s",
				    sync_file_name, strerror(errno));
				goto out;
			}
			ret = read(fd, &file_run_id, sizeof(file_run_id));
			if (ret < sizeof(file_run_id)) {
				error = isi_siq_error_new(E_SIQ_WRK_DEATH,
				    "Unable to read run file %s: %s",
				    sync_file_name, strerror(errno));
				goto out;
			}
			last_time = cur_time;
			UFAIL_POINT_CODE(run_id_mismatch, 
			    file_run_id = global->run_id + 1);
			if (file_run_id != global->run_id) {
				error = isi_siq_error_new(E_SIQ_WRK_DEATH,
				    "Multiple sync jobs for same sync run %s",
				    global->sync_id);
				goto out;
			}
		}
	}

out:
	if (error) {
		siq_send_error(global->primary, (struct isi_siq_error *) error,
		    EL_SIQ_DEST);
	}
	if (fd != -1)
		close(fd);
	if (run_dir_fd != -1)
		close(run_dir_fd);

	isi_error_handle(error, error_out);
}

bool
check_dom_gen(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct domain_entry de = {};
	bool changed = false;
	struct isi_error *error = NULL;

	if (!global->stf_sync || !global->domain_id)
		goto out;

	if (dom_get_entry(global->domain_id, &de) != 0) {
		error = isi_system_error_new(errno,
		    "failed to get domain entry for domain %llu",
		    global->domain_id);
		goto out;
	}

	changed = de.d_generation != global->domain_generation;
out:
	isi_error_handle(error, error_out);
	return changed;
}

void
sw_process_fs_error(struct migr_sworker_ctx *sw_ctx,
    enum siq_sw_fs_error_type type, char *name, enc_t enc, uint64_t lin,
    struct isi_error *error)
{
	struct isi_siq_fs_error *siqfse = (struct isi_siq_fs_error *)error;
	struct isi_system_error *se = (struct isi_system_error *)error;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	int cur_errno;
	char *error_msg = NULL;
	char *extra_msg = "";
	const char *type_msg = NULL;
	char *final_error_msg = NULL;
	bool use_ioerror = true;
	bool force_fatal = false;
	bool dom_gen_changed;
	struct int_set INT_SET_INIT_CLEAN(up_storage_nodes);
	struct int_set INT_SET_INIT_CLEAN(up_storage_diff);
	struct generic_msg msg;
	struct isi_error *new_error = NULL;
	char *lin_path = NULL;
	bool dump_exitcode = false;

	if (!error) {
		return;
	}

	error_msg = isi_error_get_detailed_message(error);

	if (isi_error_is_a(error, ISI_SIQ_FS_ERROR_CLASS)) {
		cur_errno = isi_siq_fs_error_get_unix_error(siqfse);
		use_ioerror = false;
		dump_exitcode = true;
	} else if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS)) {
		cur_errno = isi_system_error_get_error(se);
		dump_exitcode = true;
	} else {
		cur_errno = EIO;
	}

	if (cur_errno == EROFS) {
		// Check for Domain issues
		dom_gen_changed = check_dom_gen(sw_ctx, &new_error);
		if (new_error) {
			// Don't make it any worse, just skip this part
			isi_error_free(new_error);
			new_error = NULL;
		} else if (dom_gen_changed) {
			// Domain generation changed
			extra_msg = " (Domain Generation changed)";
			// Bug 227181 - don't log dexit if domain_gen++
			dump_exitcode = false;
		} else {
			// This must be a FS problem
		}
		force_fatal = true;
	} else if (cur_errno == EIO) {
		// Check for Group changes
		get_not_dead_storage_nodes(&up_storage_nodes);
		UFAIL_POINT_CODE(clear_up_storage_nodes,
		    int_set_truncate(&up_storage_nodes));
		int_set_symmetric_difference(&up_storage_diff,
		    &up_storage_nodes, &prev_up_storage_nodes);
		if (int_set_size(&up_storage_diff) != 0) {
			// Possible cause is a group change
			extra_msg =
			    " (Group change detected)";
			// Bug 227181 - don't log dexit if group change
			dump_exitcode = false;
		}
		force_fatal = true;
	} else if (cur_errno == ENOSPC || cur_errno == ENXIO ||
	    cur_errno == ENODEV || cur_errno == EBUSY) {
		force_fatal = true;
	}

	// Bug 227181 - try to dexit log sparingly
	if (dump_exitcode)
		dump_exitcode_ring(cur_errno);

	if (type < SW_ERR_MAX) {
		type_msg = siq_sw_fs_error_type_to_str[type];
	} else {
		type_msg = "unknown";
	}

	if (type == SW_ERR_LIN || type == SW_ERR_ADS) {
		force_fatal = true;
	}

	if (global->initial_sync) {
		asprintf(&final_error_msg, "Error while transferring %s %s/%s "
		    "(%s encoding): %s%s", type_msg, worker->cur_path.path,
		    type == SW_ERR_DIR ? "" : name,
		    enc > ENC_MAX ? "unsupported" : enc_type2name(enc),
		    error_msg ? error_msg : "Error", extra_msg);
	} else {
		lin_path = get_valid_rep_path(lin, HEAD_SNAPID);
		asprintf(&final_error_msg, "Error while transferring %s %s:"
		    " %s%s", type_msg, lin_path,
		    error_msg ? error_msg : "Error", extra_msg);

		free(lin_path);

	}


	if (use_ioerror) {
		msg.head.type = POSITION_MSG;
		msg.body.position.reason = POSITION_ERROR;
		msg.body.position.comment = final_error_msg;
		msg.body.position.file = name;
		msg.body.position.enc = enc;
		msg.body.position.filelin = lin;
		msg.body.position.dir = worker->cur_path.utf8path;
		msg.body.position.eveclen = 0;
		msg.body.position.evec = NULL;
		msg.body.position.dirlin = 0;
		msg_send(global->primary, &msg);
		
		msg.body.error.error = cur_errno;
		msg.head.type = ERROR_MSG;
		msg.body.error.io = 1;
		msg.body.error.str = final_error_msg;
		log(TRACE, "Error with errno %d", msg.body.error.error);	
		msg_send(global->primary, &msg);
	} else {
		siq_send_error(global->primary, (struct isi_siq_error *)error, 
		    EL_SIQ_DEST);
	}

	log(FATAL, "%s", final_error_msg);

	free(final_error_msg);
	isi_error_free(new_error);
	free(error_msg);
	return;
}

void
send_stats(struct migr_sworker_ctx *sw_ctx)
{
	if (sw_ctx->global.stf_sync && !(sw_ctx->global.initial_sync ||
	    sw_ctx->global.stf_upgrade_sync)) {
		ASSERT(sw_ctx->global.job_ver.common >= MSG_VERSION_CHOPU);
		send_sworker_stf_stat(sw_ctx->global.primary,
		    sw_ctx->stats.stf_cur_stats,
		    sw_ctx->stats.stf_last_stats,
		    sw_ctx->global.job_ver.common);
	} else {
		send_sworker_tw_stat(sw_ctx->global.primary,
		    sw_ctx->stats.tw_cur_stats, sw_ctx->stats.tw_last_stats,
		    sw_ctx->global.job_ver.common);
	}
}

/**
 * Duplicated in pworker and sworker to allow failpoint
 * use.
 */
static bool
sw_two_down_nodes(void)
{
	int max_down = 1;

	UFAIL_POINT_CODE(set_max_down_nodes_value, max_down = RETURN_VALUE);

	return max_down_nodes(max_down);
}

static void
log_fstates(struct migr_sworker_ctx *sw_ctx)
{
	struct file_state *fstate;

	log(TRACE, "%d files in fstates", sw_ctx->inprog.fstates_size);
	TAILQ_FOREACH(fstate, sw_ctx->inprog.fstates, ENTRIES) {
		log(TRACE, "    %s = %s", fstate->name, 
		    sync_state_str(fstate->sync_state));
	}
}

#define sw_siq_fs_error(ctx, err_cl, err_scl, args...)		\
    _sw_siq_fs_error(ctx, err_cl, err_scl, __FUNCTION__,	\
	__FILE__, __LINE__, "" args)

static struct isi_error*
_sw_siq_fs_error(struct migr_sworker_ctx* sw_ctx, int siq_err, int fs_err,
    const char* func, const char* file, int line, const char* format, ...)
{
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	va_list ap;
	struct isi_error* error;
	char* dir;
	ifs_lin_t dirlin;
	unsigned eveclen;
	enc_t* evec;
	char* filename;
	enc_t enc;
	ifs_lin_t filelin;
	char *errstr = NULL;
	int ret;

	dir = worker->cur_path.path;
	ASSERT(dir);
	dirlin = inprog->fstate.src_dirlin;
	ASSERT(dirlin);
	eveclen = enc_vec_size(&worker->cur_path.evec) * sizeof(enc_t);
	ASSERT(eveclen);
	evec = worker->cur_path.evec.encs;
	ASSERT(evec);
	filename = inprog->fstate.name;
	ASSERT(filename);
	enc = inprog->fstate.enc;
	filelin = inprog->fstate.src_filelin;
	ASSERT(filelin);

	va_start(ap, format);
	ret = vasprintf(&errstr, format, ap);
	ASSERT(ret != -1);
	error = _isi_siq_fs_error_new(siq_err, fs_err, dir, dirlin, eveclen,
	    evec, filename, enc, filelin, func, file, line, "%s", errstr);
	va_end(ap);

	if (errstr)
		free(errstr);
	return error;
}

void
send_err_msg(int err, struct migr_sworker_ctx *sw_ctx)
{
	struct generic_msg m = {};

	m.head.type = ERROR_MSG;
	m.body.error.error = err;
	msg_send(sw_ctx->global.primary, &m);
	return;
}

/**
 * Add a zero'd range to the hash.
 */
static void
sw_migr_hash_zero_range(struct hash_ctx *hash_ctx, uint64_t len)
{
	int cur_len;

	while (len > 0) {
		cur_len = MIN(len, IDEAL_READ_SIZE);
		len -= cur_len;

		hash_update(hash_ctx, sworker_zero_buff, cur_len);
	}
}

/**
 * Hash the specified file range.  If end of file is reached,
 * bytes past the end of file are considered to be zero (writes
 * truncate forward).
 */
static void
sw_migr_hash_range_fd(int fd, struct hash_ctx *hash_ctx, off_t offset,
    uint64_t len, char *fname, struct isi_error **error_out)
{
	unsigned char *buf;
	int cur_len;
	struct isi_error *error = NULL;
	buf = malloc(MIN(IDEAL_READ_SIZE, len));
	ASSERT(buf);

	while (len > 0) {
		cur_len = MIN(len, IDEAL_READ_SIZE);
		cur_len = pread(fd, buf, cur_len, offset);
		if (cur_len == -1) {
			error = isi_system_error_new(errno,
			    "Failed to read data in file %s", fname);
			goto out;
		}
		/* If we hit end of file, treat this as a zero'd region. */
		if (cur_len == 0) {
			sw_migr_hash_zero_range(hash_ctx, len);
			len = 0;
		} else {
			hash_update(hash_ctx, buf, cur_len);
			len -= cur_len;
		}
		offset += cur_len;
	}
out:
	free(buf);
	isi_error_handle(error, error_out);
}

/**
 * Update the uid, gid, mode, and acl of a file.  If old_st is passed
 * in, it should be a stat of fd prior to this call and will be used
 * to decide whether we can skip the actual update.  If null, we'll
 * always do the update.
 *
 * Note that this function is called from treewalk, lin update, and
 * legacy code, so be careful with assumptions.
 */
void
apply_security_to_fd(int fd, fflags_t src_flags, struct stat *old_st,
    uid_t uid, gid_t gid, mode_t mode, char *acl,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *sd = NULL;
	enum ifs_security_info secinfo = 0;
	char *old_acl = NULL;
	ifs_lin_t debug_lin = 0;
	char *path = NULL;
	int tmp_errno;
	int ret;

	if (old_st) {
		debug_lin = old_st->st_ino;

		if (old_st->st_uid == uid && old_st->st_gid == gid &&
		    old_st->st_mode == mode) {
			if (!acl && 0 == (old_st->st_flags &
			    (SF_HASNTFSACL | SF_HASNTFSOG)))
				goto out;
			if (acl) {
				old_acl = get_sd_text(fd);
				if (acl && old_acl &&
				    0 == strcmp(old_acl, acl))
					goto out;
			}
		}
	}
	if (acl) {
		sd = sd_from_text(acl, &secinfo);
		if (sd == NULL) {
			log(ERROR, "Bad ACL text dlin %llx, (%s)",
			    debug_lin, acl);
		}
	}

	ret = ifs_set_mode_and_sd(fd, SMAS_FLAGS_FORCE_SD, mode, uid, gid,
	    secinfo, sd);
	if (ret) {
		tmp_errno = errno;
		if (debug_lin) {
			path = get_valid_rep_path(debug_lin, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "set_mode_and_sd error for %s", path);
			free(path);
		} else {
			error = isi_system_error_new(tmp_errno,
			    "set_mode_and_sd error");
		}
		goto out;
	}

 out:
	if (old_acl)
		free(old_acl);
	if (sd) {
		cleanup_sd(sd);
		free(sd);
	}
	isi_error_handle(error, error_out);
}

/**
 * Update the attrs of a file.  If old_st is passed
 * in, it should be a stat of fd prior to this call and will be used
 * to decide whether we can skip some of the actual update.  If null, we'll
 * always do the update.
 *
 * Note that this function is called from treewalk, lin update, and
 * legacy code, so be careful with assumptions.
 */

void
apply_attrs_to_fd(int fd, struct stat *old_st, struct ifs_syncattr_args *args,
    char *acl, struct ifs_security_descriptor *sd_in,
    enum ifs_security_info *si_in, struct worm_state *old_worm, bool compliance,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *sd = NULL;
	enum ifs_security_info secinfo = 0;
	ifs_lin_t debug_lin = 0;
	char *path = NULL;
	int tmp_errno;
	int ret;

	log(TRACE, "%s", __func__);

	if (old_st) {
		debug_lin = old_st->st_ino;
	}

	if (acl) {
		if (sd_in && si_in) {
			sd = sd_in;
			secinfo = *si_in;
		} else {
			sd = sd_from_text(acl, &secinfo);
			if (sd == NULL) {
				error = isi_system_error_new(EINVAL,
				    "Bad ACL text dlin %llx, (%s)",
				    debug_lin, acl);
				goto out;
			}
		}
		/*
		 * Bug 155147
		 * if the file is worm committed and governed by a
		 * compliance domain, don't try to change the ACL
		 */
		if (old_worm && old_worm->w_committed && compliance)
			secinfo = IFS_SEC_INFO_NOOP;
	}

	args->secinfo = secinfo;
	args->sd = sd;

	ret = ifs_syncattr(args);
	if (ret) {
		tmp_errno = errno;
		if (debug_lin) {
			path = get_valid_rep_path(debug_lin, HEAD_SNAPID);
			error = isi_system_error_new(tmp_errno,
			    "syncattr error for %s", path);
			free(path);
		} else {
			error = isi_system_error_new(tmp_errno,
			    "syncattr error");
		}
		goto out;
	}

 out:
	if (sd) {
		if (sd != sd_in) {
			cleanup_sd(sd);
			free(sd);
		}
		sd = NULL;
	}

	isi_error_handle(error, error_out);
}

void
apply_attributes(struct migr_sworker_ctx *sw_ctx, int fd,
    struct common_stat_state *stat_state, bool data_changed, bool is_stf,
    struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;
	struct stat st;
	char *path = NULL;
	struct ifs_syncattr_args syncargs = IFS_SYNCATTR_ARGS_INITIALIZER;
	struct worm_state worm = {};
	struct timespec now = {};
	int clock_id = CLOCK_MONOTONIC;

	struct parent_entry pe = {};
	unsigned int num_entries = 0;

	ifs_lin_t lin;
	bool is_worm = false, compliance = false, do_sd_chown = false;

	struct ifs_security_descriptor *sd_src = NULL, *sd_tgt = NULL;
	enum ifs_security_info si_src = 0, si_tgt = 0;

	log(TRACE, "%s", __func__);
	ret = fstat(fd, &st);
	if (ret == -1) {
		error = isi_system_error_new(errno, "fstat error");
		goto out;
	}

	// STF transfers enforced regular files here but not treewalk.
	// Is there a reason why?
	if ((!is_stf || (stat_state->type == SIQ_FT_REG)) && 
	    (data_changed && st.st_size != stat_state->size)) {
		ret = ftruncate(fd, stat_state->size);
		if (ret) {
			path = get_valid_rep_path(st.st_ino, HEAD_SNAPID);
			error = isi_system_error_new(errno,
			    "ftruncate error for %s", path);
			goto out;
		}
	}

	/* For ADS file, get the WORM state of its container since ADS files
	 * themselves do not store WORM state.
	 */
	lin = st.st_ino;
	if (sw_ctx->inprog.ads) {
		ret = pctl2_lin_get_parents(lin, st.st_snapid, &pe, 1,
		    &num_entries);
		if (ret || num_entries < 1) {
			path = get_valid_rep_path(lin, st.st_snapid);
			error = isi_system_error_new(errno,
			    "error getting ADS container for %s", path);
			goto out;
		}
		lin = pe.lin;
	}

	is_worm = get_worm_state(lin, st.st_snapid, &worm, &compliance, &error);
	if (error)
		goto out;

	UFAIL_POINT_CODE(apply_attributes_clock_id, clock_id = RETURN_VALUE;);

	if (is_worm && worm.w_committed) {
		if (compliance) {
			if (ifs_cdate_get(&now)) {
				error = isi_system_error_new(errno,
				    "error reading compliance clock");
				goto out;
			}
		/* XXX vfs_timestamp doesn't seem exposed to user space */
		} else if (clock_gettime(clock_id, &now)) {
			error = isi_system_error_new(errno,
			    "error reading system clock");
			goto out;
		}
	}

	/* XXX need a call similar to linux's futimens to set timestamps with
	 * nanosecond resolution */

	syncargs.fd = fd;

	//If this file is not committed, we can safely apply time and 
	//mode changes
	if (!is_worm || !worm.w_committed) {
		if (stat_state->atime.tv_sec != st.st_atimespec.tv_sec ||
		    stat_state->atime.tv_nsec != st.st_atimespec.tv_nsec) {
			syncargs.atime_sec = stat_state->atime.tv_sec;
			syncargs.atime_nsec = stat_state->atime.tv_nsec;
		}
		if (stat_state->mtime.tv_sec != st.st_mtimespec.tv_sec ||
		    stat_state->mtime.tv_nsec != st.st_mtimespec.tv_nsec) {
			syncargs.mtime_sec = stat_state->mtime.tv_sec;
			syncargs.mtime_nsec = stat_state->mtime.tv_nsec;
		}

		if (stat_state->mode != st.st_mode)
			syncargs.mode = stat_state->mode;
	}
	//Apply flags if not committed (or is expired) and not read only
	if (stat_state->flags != st.st_flags && (!is_worm ||
	    !worm.w_committed ||
	    (worm.w_retention_date != WORM_RETAIN_FOREVER &&
	    worm.w_retention_date <= now.tv_sec &&
	    (stat_state->flags & ~UF_DOS_RO) == (st.st_flags & ~UF_DOS_RO) &&
	    (stat_state->flags & UF_DOS_RO) == 0)))
		syncargs.flags = stat_state->flags;

	/*
	 * Bug 166584
	 * Skip chown/chgrp check when worm committed and compliance domain
	 */
	if (is_worm && worm.w_committed && compliance) {
		log(DEBUG, "%s: Skipping chown/chgrp check for worm "
		    "committed/compliance lin %llx",
		    __func__, st.st_ino);
	/*
	 * Bug 166584
	 * if SF_HASNTFSOG is set, compare personas
	 */
	} else if ((stat_state->flags & SF_HASNTFSOG) == SF_HASNTFSOG) {
		sd_src = sd_from_text(stat_state->acl, &si_src);
		if (sd_src == NULL) {
			error = isi_system_error_new(EINVAL,
			    "Bad ACL text dlin %llx, (%s)",
			    lin, stat_state->acl);
			goto out;
		}
		ASSERT_DEBUG((si_src & (IFS_SEC_INFO_OWNER |
		    IFS_SEC_INFO_GROUP)) != 0);
		if ((st.st_flags & SF_HASNTFSOG) == SF_HASNTFSOG) {
			/*
			 * Bug 166584
			 * If both the source and target files
			 * are SF_HASNTFSOG, then compare owner
			 * and group personas.
			 */
			si_tgt = IFS_SEC_INFO_OWNER |
			    IFS_SEC_INFO_GROUP;
			sd_tgt = get_sd(fd, si_tgt);
			if (sd_tgt == NULL) {
				error = isi_system_error_new(
				    EINVAL,
				    "Failure in get_sd() lin "
				    "%llx",
				    st.st_ino);
				goto out;
			}
			if (!persona_equal(sd_src->owner,
			    sd_tgt->owner))
				do_sd_chown = true;

			if (!persona_equal(sd_src->group,
			    sd_tgt->group))
				do_sd_chown = true;
		} else
			do_sd_chown = true;
	} else if ((st.st_flags & SF_HASNTFSOG) == SF_HASNTFSOG) {
		/*
		 * Bug 166584
		 * If only the target has SF_HASNTFSOG, update the
		 * uid and gid.
		 */
		syncargs.uid = stat_state->owner;
		syncargs.gid = stat_state->group;
	} else {
		/*
		 * Bug 166584
		 * If neither has SF_HASNTFSOG, compare uid and gid.
		 */
		if (stat_state->owner != st.st_uid)
			syncargs.uid = stat_state->owner;
		if (stat_state->group != st.st_gid)
			syncargs.gid = stat_state->group;
	}
	/*
	 * Bug 169659
	 * Pass the mode if SF_HASNTFSOG but not SF_HASNTFSACL
	 */
	if (do_sd_chown && (stat_state->flags & SF_HASNTFSACL) == 0)
		syncargs.mode = stat_state->mode;

	//Apply worm commit and retention date
	if (is_worm && !sw_ctx->inprog.ads) {
		if (sw_ctx->global.compliance_v2 && 
		    stat_state->worm_committed && !syncargs.worm.w_committed) {
			//Does this not work for new files?
			//Need to do late stage commit
			add_worm_commit(sw_ctx, lin, &error);
			if (error) {
				goto out;
			}
		} else {
			syncargs.worm.w_committed = stat_state->worm_committed;
		}
		if (!compliance ||
		    (stat_state->worm_retention_date > worm.w_retention_date &&
		    stat_state->worm_retention_date != WORM_RETAIN_FOREVER))
			syncargs.worm.w_retention_date =
			    stat_state->worm_retention_date;
	}

	//Actually apply the changes
	if (!worm.w_committed || !compliance || syncargs.flags != VNOVAL ||
	    syncargs.uid != VNOVAL || syncargs.gid != VNOVAL ||
	    syncargs.worm.w_retention_date != VNOVAL || do_sd_chown)
		apply_attrs_to_fd(fd, &st, &syncargs, stat_state->acl, sd_src,
		    (sd_src ? &si_src : NULL),
		    (is_worm ? &worm : NULL), compliance, &error);

	if (error)
		goto out;

	/* chflags() is the last attribute set as if the immutable flag is
	 * set, no other attributes can be changed. chflags() only updates a
	 * file's ctime, so it doesn't affect the atime and mtime set by
	 * futimes() */
	// STF transfers only require that the file be a regular file.
	if ((is_stf && stat_state->type == SIQ_FT_REG) ||
	   (!is_stf && stat_state->flags & IMMUT_APP_FLAG_MASK)) {
		ret = fsync(fd);
		if (ret) {
			path = get_valid_rep_path(st.st_ino, HEAD_SNAPID);
			error = isi_system_error_new(errno,
			    "flush error for %s", path);
			goto out;
		}
	}


out:
	if (path)
		free(path);

	if (sd_src) {
		cleanup_sd(sd_src);
		free(sd_src);
		sd_src = NULL;
	}

	if (sd_tgt) {
		/* don't call cleanup_sd() since sd_tgt was allocated as one
		 * block of memory in get_sd(). */
		free(sd_tgt);
		sd_tgt = NULL;
	}

	isi_error_handle(error, error_out);
}

/**
 * Finish up computing a block based hash (if it was used) and compare
 * it with the hash returned by the other side.  An error is returned
 * through error_out if there is a mismatch.
 * 
 * Returns true if a hash was used in the transfer (even if the hashes didn't 
 * match) and false otherwise
 */
bool
finish_bb_hash(struct migr_sworker_ctx *sw_ctx,
    struct file_done_msg *done_msg, bool *hash_match,
    struct isi_error **error_out)
{
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct isi_error *error = NULL;
	char *hash_val = NULL;
	bool hash_used = false;
	bool hash_matches = false;

	log(TRACE, "%s", __func__);
	/*
	 * We use the bb hash if it's not a full transfer and there is at
	 * least one updated block or a changed size.  This case deals
	 * with the changed size.
	 */
	if (!inprog->fstate.full_transfer && !sw_ctx->global.skip_bb_hash &&
	    !inprog->fstate.skip_bb_hash && done_msg->size_changed &&
	    !inprog->fstate.bb_hash_ctx) {
		inprog->fstate.bb_hash_ctx = hash_alloc(HASH_MD5);
		ASSERT(inprog->fstate.bb_hash_ctx);
		hash_init(inprog->fstate.bb_hash_ctx);
	}

	/* Both sides either should or shouldn't have computed the hash */
	ASSERT_IFF(inprog->fstate.bb_hash_ctx == NULL,
	    done_msg->hash_str == NULL);

	if (inprog->fstate.bb_hash_ctx) {
		hash_used = true;
		/* Add skipped final region if any */
		if (inprog->fstate.stat_state.size > inprog->fstate.cur_off)
			sw_migr_hash_range_fd(inprog->fstate.fd,
			    inprog->fstate.bb_hash_ctx,
			    inprog->fstate.cur_off,
			    inprog->fstate.stat_state.size -
			    inprog->fstate.cur_off,
			    inprog->fstate.name, &error);
		if (error)
			goto out;

		/* Compute the hash */
		hash_val = hash_end(inprog->fstate.bb_hash_ctx);
		ASSERT(hash_val);

		/* Check to see if it matches the pworker hash */
		hash_matches = (strcmp(hash_val, done_msg->hash_str) == 0);
		
		UFAIL_POINT_CODE(bbd_hash_mismatch, hash_matches = false);
		
		if (hash_match)
			*hash_match = hash_matches;
		if (!hash_matches) {
			log(ERROR, " Differing source/dest hashes for %s/%s, %s"
			    " %s", sw_ctx->worker.cur_path.path,
			    inprog->fstate.name,
			    hash_val, done_msg->hash_str);
		}
	}
out:
	log(DEBUG, "finish_bb_hash:%s/%s %s %s", sw_ctx->worker.cur_path.path,
	    inprog->fstate.name, hash_val, done_msg->hash_str);

	free(hash_val);
	hash_val = NULL;
	isi_error_handle(error, error_out);
	return hash_used;
}

/*
 * Function to add peer lin into resync list during
 * initial/incremental sync. The caller has to ensure
 * function is called only for compliance syncs.
 * NOTE: We are doing in place resync list updates
 * as we dont have stong directory ack mechansism
 * and also directory/file count ratios are typically
 * very low, hence performance impact is minimal.
 */
void
add_peer_lin_to_resync(int dir_fd, struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	int res = -1;
	struct isi_error *error = NULL;
	struct worm_dir_info wdi = {};
	struct lin_list_entry ent = {};

	log(TRACE, "%s", __func__);

	UFAIL_POINT_CODE(skip_add_peer_lin,
	    return;
	);

	res = extattr_get_fd(dir_fd, EXTATTR_NAMESPACE_IFS,
	    "worm_dir_info", &wdi, sizeof wdi);
	if (res != sizeof wdi) {
		error = isi_system_error_new(errno,
		    "Failed to get worm dir info ");
		goto out;
	}

	/*
	 * We only add entries for primary namespace.
	*/
	if (wdi.wdi_is_cstore)
		goto out;

	ent.is_dir = true;
	set_lin_list_entry(wdi.wdi_peer_lin, ent, ctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static int
file_done_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct file_done_msg *fdone = &m->body.file_done_msg;
	struct file_ack_entry *fae = NULL;
	struct stat st;
	int ret = 0;
	struct generic_msg msg = {};
	struct file_state *fstate = NULL;
	struct isi_error *error = NULL;
	bool hash_match = false;
	bool hash_used = false;
	bool data_changed;
	int tmp_errno;
	uint64_t dlin;
	uint64_t file_lin = 0;
	bool found;

	/*
	 * if ads_start = 0 then it is end of regular file,
	 *  not end of ads of file or directory
	 */

	log(TRACE, "%s", __func__);

	ASSERT(global->job_ver.common >= MSG_VERSION_SBONNET);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	bzero(&st, sizeof(st));

	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
		fstate = ds_file_done(fdone, sw_ctx);
		inprog->ds_tmp_fstate = NULL;
	} else
		fstate = &inprog->fstate;

	ASSERT(fstate->fd != -1 || fstate->error_closed_fd);

	/* SyncIQ never purposely uses fd 0 for file data, so any use
	 * of it is unintentional and a sign of a bug. */
	ASSERT(fstate->fd != 0);

	/* XXX: is this the correct thing for bad done message? */
	/* If there was an earlier error, just do the cleanup */
	if (!fdone->success || fstate->error_closed_fd)
		goto cleanup;

	hash_used = finish_bb_hash(sw_ctx, fdone, &hash_match, &error);
	if (error)
		goto cleanup;
	if (hash_used && !hash_match) {
		if (global->job_ver.common < MSG_VERSION_CHOPU) {
			error = sw_siq_fs_error(sw_ctx,
			    E_SIQ_BBD_CHKSUM, EIO, 
			    "Differing source/dest hashes for %s/%s,",
			    sw_ctx->worker.cur_path.path,
			    inprog->fstate.name);
			siq_send_error(global->primary,
			    (struct isi_siq_error *)error, 
			    EL_SIQ_DEST);
			isi_error_free(error);
			error = NULL;
		} else {
			ret = fstat(fstate->fd, &st);
			if (ret != 0) {
				tmp_errno = errno;
				log(ERROR, "Unable to stat target lin, "
				    "its corresponding src lin is %llx",
				    fstate->src_filelin);
				error = isi_system_error_new(tmp_errno);
				goto cleanup;
			}

			/*
		 	 * Add linmap entry so that resync can find
		 	 * dlin during non stf syncs
		 	 */
			found = get_mapping(fstate->src_filelin, &dlin,
				   global->lmap_ctx, &error);
			if (error)
				goto cleanup;
			if (!found) {
				set_mapping(fstate->src_filelin,
				   st.st_ino, global->lmap_ctx, &error);
				if (error)
					goto cleanup;
			}
			 /* Send a negative ack about hash mismatch */
			msg.head.type = LIN_ACK_MSG;
			msg.body.lin_ack.src_lin = fstate->src_filelin;
			msg.body.lin_ack.dst_lin = st.st_ino;
			msg.body.lin_ack.result = 1;
			msg.body.lin_ack.siq_err = E_SIQ_BBD_CHKSUM;
			msg_send(global->primary, &msg);
		}
		goto cleanup;
	}

	/* Indicate whether any data changed */
	if (fstate->full_transfer || fstate->bytes_changed
	    || fdone->size_changed)
		data_changed = true;
	else
		data_changed = false;

	verify_run_id(sw_ctx, NULL);

	/* Apply inode attributes. */
	apply_attributes(sw_ctx, fstate->fd, &(fstate->stat_state),
	    data_changed, false, &error);
	if (error) {
		log(ERROR, "%s: %s has attr error %s",
		    __func__, fstate->name, isi_error_get_message(error));
		goto cleanup;
	}

	UFAIL_POINT_CODE(fail_after_worm_file_done,
	    if (fstate->stat_state.worm_committed) {
		    ifs_lin_t worm_lin = 0;
		    if (fstat(fstate->fd, &st) == 0)
			    worm_lin = st.st_ino;
		    log(FATAL, "%s: fail_after_worm_file_done: %llx", __func__,
			worm_lin);});

	/* If we are doing diff sync, save this info so we can add to linmap below */
	if (!inprog->ads && global->doing_diff_sync && global->do_bulk_tw_lmap) {
		log(TRACE, "%s saving sync_lmap_updates", __func__);
		ret = fstat(fstate->fd, &st);
		if (ret != 0) {
			tmp_errno = errno;
			log(ERROR, "%s: Unable to fstat target lin %s for bulk linmap", __func__, fstate->name);
			error = isi_system_error_new(tmp_errno);
			goto cleanup;
		}
	}

cleanup:
	/* sync and close the file */
	if (fstate->fd != -1) {
		ret = fsync(fstate->fd);
		if (!error && ret == -1)
			error = isi_system_error_new(errno, "fsync error");

		if (fstate->dsess) {
			struct isi_error *error2 = NULL;
			bool valid =
			    isi_file_dsession_destroy(fstate->dsess, true,
			    &error2);
			if (error && error2) {
				char *error2_msg =
				    isi_error_get_detailed_message(error2);
				isi_error_add_context(error,
				    "Error destroying dsession %s",
				    error2_msg);
				free(error2_msg);
				isi_error_free(error2);
				error2 = NULL;
			} else if (error2) {
				error = error2;
				error2 = NULL;
			} else if (!valid) {
				error = isi_system_error_new(EIO,
				    "Cloudpools cleanup error");
			}
			fstate->dsess = NULL;
		}

		if (close(fstate->fd) == -1) {
			/* We've already called fsync'd... */
			if (!error && ret == -1) {
				error = isi_system_error_new(errno,
				    "close error");
			} else {
				log(ERROR, "error closing file %s",
				    fstate->name);
			}
		}
		fstate->fd = -1;
	}

	if (error) {
		inprog->result = ACK_RES_ERROR;
		sw_ctx->stats.io_errors++;
		file_lin = inprog->file_obj ?
                   (inprog->ads ? inprog->sstate.src_filelin :
                   fstate->src_filelin) : (inprog->dstate.src_dirlin);
		goto out;
	}

	/* If finished non-ads stream, send ACK_MSG with operation result */
	if (!inprog->ads && !global->do_bulk_tw_lmap) {
		/*
		 * If its upgrade sync then we could have some in memory
		 * lin mapping updates which need to be flushed before
		 * sending ack to the source.
		 */
		flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
		if (error)
			goto out;
	
		log(TRACE, "ACK for %s (%016llx/%016llx)",
		    fstate->name, fstate->src_dirlin,
		    fstate->src_dir_key);
		msg.head.type = ACK_MSG3;
		msg.body.ack3.src_lin = fstate->src_filelin;
		msg.body.ack3.src_dlin = fstate->src_dirlin;
		msg.body.ack3.src_dkey = fstate->src_dir_key;
		msg.body.ack3.file = fstate->name;
		msg.body.ack3.enc = fstate->enc;

		msg_send(global->primary, &msg);
	} else if (!inprog->ads && global->doing_diff_sync && 
	    global->do_bulk_tw_lmap) {
		sync_lmap_updates(sw_ctx, fstate->src_filelin, st.st_ino, false, &error);
		if (error) {
			goto out;
		}
		log(DEBUG, "%s: Adding %s to bulk ack queue", __func__, 
		    fstate->name);
		fae = calloc(1, sizeof(struct file_ack_entry));
		ASSERT(fae);
		fae->msg = calloc(1, sizeof(struct ack_msg3));
		ASSERT(fae->msg);
		fae->msg->src_lin = fstate->src_filelin;
		fae->msg->src_dlin = fstate->src_dirlin;
		fae->msg->src_dkey = fstate->src_dir_key;
		fae->msg->file = strdup(fstate->name);
		ASSERT(fae->msg->file);
		fae->msg->enc = fstate->enc;
		TAILQ_INSERT_TAIL(global->bulk_tw_acks, fae, ENTRIES);
		global->bulk_tw_acks_count++;
		log(TRACE, "%s: bulk_tw_acks_count = %d", __func__, global->bulk_tw_acks_count);
		ASSERT(global->bulk_tw_acks_count == global->lmap_pending_acks);
	}

	if (fstate->stat_state.acl) {
		free(fstate->stat_state.acl);
		fstate->stat_state.acl = NULL;
	}

	/* To log the name, it has to be done before the free */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx))
		log(TRACE, "%s: Removing %s from cache.",
		    __func__, fstate->name);
	hash_free(fstate->bb_hash_ctx);
	fstate->bb_hash_ctx = NULL;
	free(fstate->name);
	fstate->name = NULL;
	fstate->error_closed_fd = false;

	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
		TAILQ_REMOVE(inprog->fstates, fstate, ENTRIES);
		inprog->fstates_size--;
		ASSERT(inprog->fstates_size >= 0);
		log_fstates(sw_ctx);
		free(fstate);
		fstate = NULL;
	} else {
		fstate->cur_off = 0;
		fstate->full_transfer = 0;
		fstate->bytes_changed = 0;
	}

out:
	sw_process_fs_error(sw_ctx, SW_ERR_FILE,
	    fstate != NULL ? fstate->name : "",
	    fstate != NULL ? fstate->enc : 0 , file_lin, error);
	isi_error_free(error);
	return 0;
}

void
finish_dir(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct timespec ts[3] = {};
	int flags = 0;
	struct isi_error *error = NULL;
	struct stat st;
	int ret;

	log(TRACE, "finish_dir");
	/* Ugly, but this is called multiple times */
	if (!inprog->dstate.dir_valid)
		return;
	inprog->dstate.dir_valid = false;

	verify_run_id(sw_ctx, NULL);

	UFAIL_POINT_CODE(check_connection_with_peer_for_dir,
	check_connection_with_peer(global->primary, true, 1, &error);
	    if (error) 
	    	goto errout;
	);

	if (global->logdeleted) {
		log(TRACE, "Send list of deleted files to primary");
		listflush(&inprog->deleted_files, LIST_TO_SLOG,
		    global->primary);
	}

	sw_ctx->stats.dirs++;
	if (inprog->dstate.stat_state.reap)
		return;

	verify_run_id(sw_ctx, NULL);

	ret = fstat(worker->cwd_fd, &st);
	if (ret == -1) {
		error = isi_system_error_new(errno, "fstat failed");
		goto errout;
	}

	UFAIL_POINT_CODE(set_dir_attributes_fail,
	    error = isi_system_error_new(RETURN_VALUE,
	    "Failed to fchown %s/%s", worker->cur_path.path,
	    inprog->dstate.name);
	    goto errout);

	apply_security_to_fd(worker->cwd_fd, inprog->dstate.stat_state.flags,
	    &st, inprog->dstate.stat_state.owner,
	    inprog->dstate.stat_state.group, inprog->dstate.stat_state.mode,
	    inprog->dstate.stat_state.acl, &error);
	if (error)
		goto errout;

	if (st.st_atimespec.tv_sec != inprog->dstate.stat_state.atime.tv_sec ||
	    st.st_atimespec.tv_nsec != inprog->dstate.stat_state.atime.tv_nsec
	    ) {
		    flags |= VT_ATIME;
		    ts[0].tv_sec = inprog->dstate.stat_state.atime.tv_sec;
		    ts[0].tv_nsec = inprog->dstate.stat_state.atime.tv_nsec;
	}

	if (st.st_mtimespec.tv_sec != inprog->dstate.stat_state.mtime.tv_sec ||
	    st.st_mtimespec.tv_nsec != inprog->dstate.stat_state.mtime.tv_nsec
	    ) {
		    flags |= VT_MTIME;
		    ts[1].tv_sec = inprog->dstate.stat_state.mtime.tv_sec;
		    ts[1].tv_nsec = inprog->dstate.stat_state.mtime.tv_nsec;
	}

	if (flags && fvtimes(worker->cwd_fd, ts, flags) == -1) {
		error = isi_system_error_new(errno, "fvtimes failed");
		goto errout;
	}

	if (inprog->dstate.stat_state.flags_set &&
	    st.st_flags != inprog->dstate.stat_state.flags &&
	    (fchflags(worker->cwd_fd, inprog->dstate.stat_state.flags) == -1)) {
		error = isi_system_error_new(errno, "fchflags failed");
		goto errout;
	}

errout:
	/*XXX: EML is this REALLY the right place to do this?*/
	if (inprog->dstate.stat_state.acl) {
		free(inprog->dstate.stat_state.acl);
		inprog->dstate.stat_state.acl = NULL;
	}

	isi_error_handle(error, error_out);
}

/*   ____      _ _ _                _        
 *  / ___|__ _| | | |__   __ _  ___| | _____ 
 * | |   / _` | | | '_ \ / _` |/ __| |/ / __|
 * | |__| (_| | | | |_) | (_| | (__|   <\__ \
 *  \____\__,_|_|_|_.__/ \__,_|\___|_|\_\___/
 */

int
disconnect_callback(struct generic_msg *m, void *ctx)
{
	log(INFO, "Disconnect from %s (%d)", m->body.discon.fd_name,
	    m->body.discon.fd);
	exit(EXIT_SUCCESS);
}

int
dir4_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct enc_vec evec = ENC_VEC_INITIALIZER;
	struct stat st;
	struct isi_error *error = NULL;
	struct path *new_source_path = NULL, *new_target_path = NULL;
	struct path *relative_target_path = NULL;
	struct dir_msg4 *msg4 = &m->body.dir4;
	ifs_lin_t lin_out;
	bool is_new_dir = false;
	int kept_errno = 0;
	int ret = 0;
	int tmp_fd = -1;

	log(TRACE, "%s", __func__);
	
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");

	if (global->job_ver.common < MSG_VERSION_SBONNET)
		old_finish_file(sw_ctx);

	finish_dir(sw_ctx, &error);
	if (error) {
		goto fail;
	}
	inprog->file_obj = false;
	inprog->dstate.stat_state.mode  = (mode_t)msg4->mode;
	inprog->dstate.stat_state.owner = (uid_t)msg4->uid;
	inprog->dstate.stat_state.group = (gid_t)msg4->gid;
	/* XXX next rev of the dir message should contain second *and*
	 * nanosecond times */
	inprog->dstate.stat_state.atime.tv_sec = msg4->atime;
	inprog->dstate.stat_state.atime.tv_nsec = 0;
	inprog->dstate.stat_state.mtime.tv_sec = msg4->mtime;
	inprog->dstate.stat_state.mtime.tv_nsec = 0;
	inprog->dstate.stat_state.reap  = msg4->reap;
	free(inprog->dstate.name);
	inprog->dstate.name = strdup(msg4->dir + global->source_base_path_len);
	ASSERT(inprog->dstate.name);
	free(inprog->dstate.stat_state.acl);
	inprog->dstate.stat_state.acl = NULL;

	if (msg4->acl) {
		inprog->dstate.stat_state.acl = strdup(msg4->acl);
		ASSERT(inprog->dstate.stat_state.acl);
	}

	log(TRACE, "Copy evecs, evec num = %zu", msg4->eveclen / sizeof(enc_t));
	enc_vec_copy_from_array(&evec, msg4->evec, msg4->eveclen / sizeof(enc_t));

	inprog->dstate.src_dirlin = msg4->dir_lin;

	inprog->dstate.stat_state.flags_set = 1;
	inprog->dstate.stat_state.flags = (fflags_t)msg4->di_flags;

	new_source_path = malloc(sizeof(struct path));
	new_target_path = malloc(sizeof(struct path));

	path_init(new_source_path, msg4->dir, &evec);
	path_init(new_target_path, NULL, NULL);
	path_copy(new_target_path, path_changebeg(new_source_path,
	    &global->source_base_path, &global->target_base_path));

	if (worker->cwd_fd > 0) {
		close(worker->cwd_fd);
		worker->cwd_fd = -1;
	}

	path_copy(&worker->cur_path, new_target_path);
	log(TRACE, "new target path: %s", worker->cur_path.path);

	/* Try cd'ing into an existing directory */
	UFAIL_POINT_CODE(dir4_schdirfd_fail, ret = -1;
	    kept_errno = RETURN_VALUE; error = isi_system_error_new(kept_errno, 
	    "Failed to smkchdirfd to target directory"); goto fail);

	if (global->stf_upgrade_sync) {
		/* Change dir to base path first */
		log(TRACE, "dir4_callback: base path %s", 
		    global->target_base_path.path);
		ret = smkchdirfd(&global->target_base_path, AT_FDCWD, 
		    &tmp_fd);
		if (ret == -1) {
			error = isi_system_error_new(errno, 
			    "Failed to smkchdirfd to target base path");
			goto fail;
		}
		/* Now, construct the relative path */
		
		relative_target_path = relpath(new_source_path, 
		    &global->source_base_path);
		/* Skip this step for the base path */
		if (relative_target_path != NULL && 
		    relative_target_path->used != 0) {
			log(TRACE, "dir4_callback: using smkchdirfd_upgrade %s",
			    relative_target_path->path);
			/* Use smkchdirfd_upgrade to get there */
			ret = smkchdirfd_upgrade(relative_target_path, 
			    tmp_fd, global->domain_id, &worker->cwd_fd);
			if (ret == -1) {
				error = isi_system_error_new(errno, 
				    "Failed to smkchdirfd_upgrade");
				goto fail;
			}
			if (tmp_fd > 0) {
				close(tmp_fd);
				tmp_fd = -1;
			}
		} else {
			worker->cwd_fd = tmp_fd;
			tmp_fd = -1;
		}
	} else {
		ret = schdirfd(new_target_path, AT_FDCWD, &worker->cwd_fd);
		if (ret == -1 && (errno == ENOENT || errno == ENOTDIR)) {
			ret = smkchdirfd(new_target_path, AT_FDCWD,
			    &worker->cwd_fd);
			if (ret == -1) {
				error = isi_system_error_new(errno, 
				    "Failed to smkchdirfd to target directory"
				    " %s", new_target_path->path);
				goto fail;
			}
			is_new_dir = true;
		}
		if (ret == -1) {
			error = isi_system_error_new(errno, 
			    "Failed to schdirfd to target directory"
			    " %s", new_target_path->path);
			goto fail;
		}
		if (global->stf_sync) {
			add_to_domain(worker->cwd_fd, global->domain_id,
			    &error);
			if (error)
				goto fail;
		}
	}

	ASSERT(worker->cwd_fd > 0);
	if (global->compliance_v2) {
		/*
		 * Add the peer lin directory into resync list.
		 */
		add_peer_lin_to_resync(worker->cwd_fd, global->resync_ctx,
		    &error);
		if (error)
			goto fail;
	}

	clear_user_attrs(worker->cwd_fd, NULL, &error);
	if (error)
		goto fail;

	/* Remove immutable/read-only flags */
	ret = fstat(worker->cwd_fd, &st);
	if (ret == -1) {
		error = isi_system_error_new(errno,
			    "Failed to stat dir %s",
			    new_target_path->path);
		goto fail;
	}
	if (st.st_flags & IMMUT_APP_FLAG_MASK) {
		log(TRACE, "%s: clear_flags_fd on %s", __func__,
		    new_target_path->path);
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK, worker->cwd_fd,
		    NULL, &error);
		if (error) {
			log(ERROR, "%s: failed to clear_flags_fd on %s", 
			    __func__, new_target_path->path);
		}
	}
	
	/* Add source and target dir LINs to the lin map */
	if (global->stf_sync && !sw_ctx->inprog.ads) {
		log(DEBUG, "Adding lin mapping %llx->%llx for dir %s",
		    msg4->dir_lin, st.st_ino, msg4->dir);
		/*
		 * try to add mapping conditionally. if mapping fails,
		 * worker restarted or there could race with another worker.
		 */
		ret = set_mapping_cond(msg4->dir_lin, st.st_ino,
		    global->lmap_ctx, true, 0, &error);
		if (error)
		    goto fail;

		if (ret == 0) {
			log(DEBUG, " Dir %s/%s already found in map under "
			    "slin %llx", sw_ctx->worker.cur_path.path,
			    m->body.lin_map.name, msg4->dir_lin);
			ret = get_mapping(msg4->dir_lin, &lin_out,
			    global->lmap_ctx, &error);
			if (error)
				goto fail;

			ASSERT(ret && (lin_out == st.st_ino));
		}
	}

	log(TRACE, "%s: need_ack_dir=%d", __func__, msg4->need_ack_dir);

	/* If doing diff_sync, the pworker needs to know if the directory
	 * already exists, or if it's a new directory (in which case there's
	 * no need to doing any file hash matching) */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx))
		ds_dir4(msg4, is_new_dir, sw_ctx);

	if (global->logdeleted) {
		init_list_deleted_dst(&inprog->deleted_files,
		    worker->cur_path.utf8path, global->cluster_name);
	}

	inprog->dstate.dir_valid = true;

fail:
	if (new_target_path) {
		log(DEBUG, "%s: dir %s: new_dir=%d: ret=%d",
		    __func__, new_target_path->path, is_new_dir, ret);
	}

	if (error) {
		inprog->result = ACK_RES_ERROR;
	}
	if (tmp_fd > 0) {
		close(tmp_fd);
		tmp_fd = -1;
	}	
	enc_vec_clean(&evec);

	if (new_source_path) {
		path_clean(new_source_path);
		free(new_source_path);
	}

	if (new_target_path) {
		path_clean(new_target_path);
		free(new_target_path);
	}
	
	sw_process_fs_error(sw_ctx, SW_ERR_DIR, inprog->dstate.name, 
	    inprog->dstate.enc, inprog->dstate.src_dirlin, error);
	isi_error_free(error);
	
	return 0;
}

int
open_or_create_reg(struct migr_sworker_ctx *sw_ctx, int dir_fd,
    char *name, enc_t enc, uint64_t *lin_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	int fd;
	int ret;

	fd = enc_openat(dir_fd, name, enc, O_RDWR|O_CREAT|O_NOFOLLOW, 0600);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "%s: Failed to open file %s/%s for write", __func__,
		    sw_ctx->worker.cur_path.path, name);
		goto out;
	}

	/* SyncIQ never purposely uses fd 0 for file data, so any use
	 * of it is unintentional and a sign of a bug. */
	ASSERT(fd != 0);

	/*
	 * Paranoia test to make sure we opened a normal file
	 * rather than a device someone raced into place.
	 */
	ret = fstat(fd, &st);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "%s: Failed to stat file %s/%s", __func__,
		    sw_ctx->worker.cur_path.path, name);
		goto out;
	}

	if (!S_ISREG(st.st_mode)) {
		error = isi_system_error_new(EEXIST, "%s: Expected a regular "
		    "file, but found non-regular file at %s/%s : type: %s "
		    "lin: %llx nlinks: %d", __func__,
		    sw_ctx->worker.cur_path.path, name,
		    file_type_to_name(stat_mode_to_file_type(st.st_mode)),
		    st.st_ino, st.st_nlink);
		goto out;
	}

	if (lin_out)
		*lin_out = st.st_ino;
out:
	if (error && fd != -1) {
		close(fd);
		fd = -1;
	}
	isi_error_handle(error, error_out);
	return fd;
}

int
open_or_create_sym(struct migr_sworker_ctx *sw_ctx, int dir_fd,
    char *link_name, enc_t link_enc, char *name, enc_t enc, uint64_t *lin_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	int fd = -1;
	int ret;

	ret = enc_symlinkat(dir_fd, link_name, link_enc, name, enc);
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Failed to make symlink %s/%s",
		    sw_ctx->worker.cur_path.path, name);
		goto out;
	}

	fd = enc_openat(dir_fd, name, enc,
	    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open symlink %s/%s",
		    sw_ctx->worker.cur_path.path, name);
		goto out;
	}

	/* SyncIQ never purposely uses fd 0 for file data, so any use
	 * of it is unintentional and a sign of a bug. */
	ASSERT(fd != 0);

	/*
	 * Paranoia test to make sure we opened a normal file
	 * rather than a device someone raced into place.
	 */
	ret = fstat(fd, &st);
	if (ret == -1 || !S_ISLNK(st.st_mode)) {
		error = isi_system_error_new(errno,
		    "Failed to stat symlink %s/%s",
		    sw_ctx->worker.cur_path.path, name);
		close(fd);
		fd = -1;
		goto out;
	}
	if (lin_out)
		*lin_out = st.st_ino;

out:
	isi_error_handle(error, error_out);
	return fd;
}

/* create a block/char special device file */
int
open_or_create_special(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, enum file_type type, unsigned int major, unsigned int minor,
    uint64_t *lin_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	int ret;
	mode_t mode;
	struct stat st;

	if (type == SIQ_FT_CHAR)
		mode = S_IFCHR;
	else if (type == SIQ_FT_BLOCK)
		mode = S_IFBLK;
	else
		ASSERT(false);

	log(TRACE, "Creating %s %s", file_type_to_name(type), name);

	fchdir(dir_fd);

	/* XXX this is a hack that needs to be replaced with an enc_mknodat
	 * syscall that supports sockets, fifos, devices, etc. */
	/* change process encoding prior to node creation */
	if (enc_set_proc(enc) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to set process encoding");
		goto out;
	}
	/* end enc_set_proc hack XXX */
	ret = mknod(name, mode, makedev(major, minor));
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to mknod %s", name);
		goto out;
	}

	/* revert process encoding */
	if (enc_set_proc(ENC_DEFAULT) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to revert process encoding");
		goto out;
	}

	ret = enc_fstatat(dir_fd, name, enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Failed to stat new node %s", name);
		goto out;
	}
	if (type == SIQ_FT_CHAR)
		ASSERT(S_ISCHR(st.st_mode) == true);
	else
		ASSERT(S_ISBLK(st.st_mode) == true);

	if (lin_out)
		*lin_out = st.st_ino;

	fd = ifs_lin_open(st.st_ino, st.st_snapid, O_RDONLY);
	if (fd <= 0) {
		error = isi_system_error_new(errno, "Failed to open lin %llx",
		    st.st_ino);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return fd;
}

/* create a unix socket file */
int
open_or_create_socket(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, uint64_t *lin_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	struct sockaddr_un addr;
	int sock = -1;
	int ret;
	struct stat st;

	log(TRACE, "Creating socket %s", name);
	fchdir(dir_fd);

	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		error = isi_system_error_new(errno, "Failed to create socket");
		goto out;
	}

	bzero(&addr, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, name, sizeof(addr.sun_path) - 1);

	/* XXX this is a hack that needs to be replaced with an enc_mknodat
	 * syscall that supports sockets, fifos, devices, etc. */
	/* change process encoding prior to node creation */
	if (enc_set_proc(enc) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to set process encoding");
		goto out;
	}
	/* end enc_set_proc hack XXX */
	ret = bind(sock, (struct sockaddr *)&addr, SUN_LEN(&addr));
	if (ret == -1 && errno != EADDRINUSE) {
		error = isi_system_error_new(errno, "Failed to bind socket");
		goto out;
	}

	/* revert process encoding */
	if (enc_set_proc(ENC_DEFAULT) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to revert process encoding");
		goto out;
	}

	close(sock);
	sock = -1;

	ret = enc_fstatat(dir_fd, name, enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Failed to stat new node %s", name);
		goto out;
	}
	ASSERT(S_ISSOCK(st.st_mode) == true);
	if (lin_out)
		*lin_out = st.st_ino;

	fd = ifs_lin_open(st.st_ino, st.st_snapid, O_RDONLY);
	if (fd <= 0) {
		error = isi_system_error_new(errno, "Failed to open lin %llx",
		    st.st_ino);
		goto out;
	}

out:
	if (sock != -1)
		close(sock);

	isi_error_handle(error, error_out);
	return fd;
}

/* create a fifo */
int
open_or_create_fifo(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, uint64_t *lin_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	struct stat st;
	int ret;

	log(TRACE, "Creating FIFO %s", name);
	fchdir(dir_fd);

	/* XXX this is a hack that needs to be replaced with an enc_mknodat
	 * syscall that supports sockets, fifos, devices, etc. */
	/* change process encoding prior to node creation */
	if (enc_set_proc(enc) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to set process encoding");
		goto out;
	}
	/* end enc_set_proc hack XXX */

	ret = mkfifo(name, O_RDONLY);
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to create fifo");
		goto out;
	}

	/* revert process encoding */
	if (enc_set_proc(ENC_DEFAULT) == -1) {
		error = isi_system_error_new(errno,
		    "Failed to revert process encoding");
		goto out;
	}

	ret = enc_fstatat(dir_fd, name, enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Failed to stat new node %s", name);
		goto out;
	}
	ASSERT(S_ISFIFO(st.st_mode) == true);
	if (lin_out)
		*lin_out = st.st_ino;

	fd = ifs_lin_open(st.st_ino, st.st_snapid, O_RDONLY);
	if (fd <= 0) {
		error = isi_system_error_new(errno, "Failed to open lin %llx",
		    st.st_ino);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return fd;
}

/*
 * Derive the mode of an ADS child stream from the attributes of its parent
 * file or directory.  Since the parent can be a directory or a file, only
 * propagate bits with the same meaning across both inode types.
 */
static mode_t
adsmodestream(const mode_t mode)
{
	return mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP 
	    | S_IROTH | S_IWOTH);
}

/*
 * This is used to clear out the immutable/append only flags.
 */
void
clear_flags_fd(struct stat *st, unsigned long flags_to_clear,
    int fd, char *debug_name, struct isi_error **error_out)
{
	int ret, tmp_errno;
	struct isi_error *error = NULL;
	char *debug_path = NULL;
	struct worm_state worm = {};
	bool is_worm = false;
	if (debug_name == NULL)
		debug_name = "";

	ASSERT(fd > 0);

	/* Bug 152648: skip fchflags() on worm committed files */
	is_worm = get_worm_state(st->st_ino, HEAD_SNAPID, &worm, NULL,
	    &error);
	if (error || (is_worm && worm.w_committed))
		goto out;

	ret = fchflags(fd, st->st_flags & ~(flags_to_clear));
	if (ret) {
		tmp_errno = errno;
		debug_path = get_valid_rep_path(st->st_ino, HEAD_SNAPID);
		error = isi_system_error_new(tmp_errno,
		    "Failed to fchflag file target lin %llx (%s,%s)",
		    st->st_ino, debug_path, debug_name);

		goto out;
	}

	ret = fsync(fd);
	if (ret) {
		debug_path = get_valid_rep_path(st->st_ino, HEAD_SNAPID);
		error = isi_system_error_new(errno,
		    "Failed to fsync target lin %llx (%s,%s)",
		    st->st_ino, debug_path, debug_name);
		goto out;
	}
out:
	if (debug_path)
		free(debug_path);
	isi_error_handle(error, error_out);
}


/*
 * This is used to clear out the immutable/append only flags.
 */
void
clear_flags(struct stat *st, unsigned long flags_to_clear,
    char *name, int enc, struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;
	/*
	 * Bug 196399
	 * We have the inode already, so we should skip the overhead of a name
	 * lookup.
	 */
	fd = ifs_lin_open(st->st_ino, HEAD_SNAPID, O_RDONLY);
	if (fd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open file %s (%s) (%llx) for write",
		    name, enc_get_name(enc), st->st_ino);
		goto out;
	}
	clear_flags_fd(st, flags_to_clear, fd, name, &error);
	if (error)
		goto out;

out:
	if (fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
}

/*
 * Helper for file_begin_callback.
 * This copies the relevant fields from begin into fstate.
 */
void
initialize_fstate_fields(struct migr_sworker_ctx* sw_ctx,
    struct file_state *fstate, struct file_begin_msg *begin) 
{
	struct common_stat_state *pstate = NULL;
	struct file_state *tmp_fstate = NULL;
	struct common_stat_state *new_stat_state;

	/* Ugly, but fstate->name was already set */
	ASSERT(fstate->name);
	fstate->enc = begin->fenc;
	new_stat_state = &fstate->stat_state;
	/*
	 * If current context is ADS, use parent metadata entries for ADS
	 * streams. Have to determine if parent is a file or a directory.
	 */
	if (sw_ctx->inprog.ads) {
		if (sw_ctx->global.doing_diff_sync) {
			/* Empty TAILQ implies directory */
			if (TAILQ_EMPTY(sw_ctx->inprog.fstates))
				pstate = &sw_ctx->inprog.dstate.stat_state;
			else {
				tmp_fstate = TAILQ_LAST(sw_ctx->inprog.fstates,
				    file_state_list);
				pstate = &fstate->stat_state;
			}
		}
		else {
			if (sw_ctx->inprog.sstate.fd == -1)
				pstate = &sw_ctx->inprog.dstate.stat_state;
			else
				pstate = &sw_ctx->inprog.sstate.stat_state;
		}
		new_stat_state->owner = pstate->owner;
		new_stat_state->group = pstate->group;
		new_stat_state->mode  = adsmodestream(pstate->mode);
		new_stat_state->atime = pstate->atime;
		new_stat_state->mtime = pstate->mtime;
		new_stat_state->worm_committed = pstate->worm_committed;
		new_stat_state->worm_retention_date =
		    pstate->worm_retention_date;
	} else {
		new_stat_state->owner = (uid_t)begin->uid;
		new_stat_state->group = (gid_t)begin->gid;
		new_stat_state->mode  = (mode_t)begin->mode;
		new_stat_state->atime.tv_sec = begin->atime_sec;
		new_stat_state->atime.tv_nsec = begin->atime_nsec;
		new_stat_state->mtime.tv_sec = begin->mtime_sec;
		new_stat_state->mtime.tv_nsec = begin->mtime_nsec;

		new_stat_state->worm_committed = (bool)begin->worm_committed;
		new_stat_state->worm_retention_date =
		    begin->worm_retention_date;
	}

	new_stat_state->size  = begin->size;
	free(new_stat_state->acl);
	new_stat_state->acl = NULL;
	new_stat_state->flags = 0;
	new_stat_state->flags_set = 0;
	if (begin->acl != NULL) {
		new_stat_state->acl = strdup(begin->acl);
		ASSERT(new_stat_state->acl);
	}
	new_stat_state->flags_set = 1;
	new_stat_state->flags = (fflags_t)begin->di_flags;
	fstate->src_filelin = (u_int64_t)begin->src_lin;
	fstate->src_dirlin = (u_int64_t)begin->src_dlin;
	fstate->src_dir_key = (u_int64_t)begin->src_dkey;
	fstate->full_transfer = begin->full_transfer;
	if (sw_ctx->global.doing_diff_sync)
		ASSERT(fstate->full_transfer == 1);
}

/**
 * Helper for file_begin.  This selects whether to use the fstate
 * internal to the sw_ctx or whether to allocate a dynamic fstate for
 * diff sync.  This is guaranteed to return an fstate with the fstate->name
 * field filled in even if an error is returned.
 */
struct file_state *
choose_or_create_fstate(struct migr_sworker_ctx *sw_ctx,
    struct file_begin_msg *begin, bool file_exists, int nlinks,
    struct isi_error **error_out)
{
	struct file_state *fstate = NULL;
	struct isi_error *error = NULL;

	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
		/* This will set fstate->fd, if the file exists */
		sw_ctx->inprog.ds_tmp_fstate = fstate =
		    ds_file_begin(begin, file_exists, nlinks, sw_ctx, &error);
		ASSERT(fstate);
	} else {
		fstate = &sw_ctx->inprog.fstate;
		if (fstate->fd != -1) {
			log(ERROR, "Bad state for file %s(%d) %d",
			    begin->fname, begin->fenc, fstate->fd);
		}
		ASSERT(fstate->fd == -1);
		ASSERT(fstate->name == NULL);
		fstate->name = strdup(begin->fname);
		ASSERT(fstate->name);
		fstate->skip_bb_hash = sw_ctx->global.skip_bb_hash;
	}
	isi_error_handle(error, error_out);
	return fstate;
}

int
make_hard_link(char *name, enc_t enc, uint64_t lin_target, int at_fd,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out)
{
	int res;
	int fd = -1;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	struct stat st;

	/* see if there's an invalid file or dir that needs to be removed to
	 * create the hard link, if the correct hard link already exists,
	 * do nothing */
	res = enc_fstatat(at_fd, name, enc, &st, AT_SYMLINK_NOFOLLOW);
	if (res == -1 && errno != ENOENT) {
		tmp_errno = errno;
		error = isi_system_error_new(errno, "fstat operation failed"
		    " for entry %s", name);
		goto out;
	}
	else if (res == 0) {
		if (S_ISDIR(st.st_mode) || st.st_ino != lin_target) {
			delete(name, enc, sw_ctx, &error);
			if (error)
				goto out;
		} else {
			/* correct hard link already exists */
			fd = ifs_lin_open(lin_target, HEAD_SNAPID, O_RDWR);
			if (fd == -1) {
				tmp_errno = errno;
				error = isi_system_error_new(errno,
				    "ifs_lin_open failed for lin %llx",
				    lin_target);
			}
			goto out;
		}
	}

	fd = ifs_lin_open(lin_target, HEAD_SNAPID, O_RDWR);
	if (fd == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(errno,
		    "ifs_lin_open failed for lin %llx", lin_target);
		goto out;
	}

	if (fstat(fd, &st) == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(errno,
		    "fstat failed for lin %llx", lin_target);
		goto out;
	}

	if (st.st_flags & IMMUT_APP_FLAG_MASK) {
		clear_flags_fd(&st, IMMUT_APP_FLAG_MASK,
		    fd, NULL, &error);
		if (error)
			goto out;
	}

	res = enc_lin_linkat(lin_target, at_fd, name, enc);
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(errno,
		    "Unable to link lin %llx with new dirent %s",
		    lin_target, name);
		goto out;
	}
	log(DEBUG, "%s: %s->%llx", __func__, name, lin_target);

out:
	if (error) {
		log(ERROR, "Error making hard link %s->%llx: %s",
		    name, lin_target, strerror(tmp_errno));
		if (fd != -1) {
			close(fd);
			fd = -1;
		}
		enc_unlinkat(at_fd, name, enc, 0);
	}

	isi_error_handle(error, error_out);
	return fd;
}

static bool
stf_upgrade_dir_worker(struct migr_sworker_ctx *sw_ctx, 
    struct migr_tw *tw, struct migr_dir *current_dir, 
    struct isi_error **error_out)
{
	struct migr_dirent *md = NULL;
	struct isi_error *error = NULL;
	enum file_type type;
	int dirfd = -1;
	int ret = -1;
	bool dir_completed = false;

	log(TRACE, "stf_upgrade_dir_worker: migr_dir_read");

	/* Bug 190016 - check connection prior to migr_dir_read() */
	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	md = migr_dir_read(current_dir, &error);
	if (error) {
		log(ERROR, "stf_upgrade_dir_worker: error in migr_dir_read");
		goto out;
	}
	log(TRACE, "stf_upgrade_dir_worker: begin loop");
	while (md != NULL) {
		log(TRACE, "stf_upgrade_dir_worker: begin %s", 
		    md->dirent->d_name);
		/* Skip /ifs/.ifsvar */
		if (strcmp(md->dirent->d_name, ".ifsvar") == 0 && 
		    strcmp(tw->_path.path, "/ifs") == 0) {
			log(TRACE, "stf_upgrade_dir_worker: skipping .ifsvar");
			migr_dir_unref(current_dir, md);
			md = migr_dir_read(current_dir, &error);
			if (error) {
				log(ERROR, "stf_upgrade_dir_worker: "
				    "error in migr_dir_read loop (.ifsvar)");
				goto out;
			}
			continue;
		}
		/* Add to domain */
		log(TRACE, "stf_upgrade_dir_worker: tagging %s", 
			md->dirent->d_name);
		dirfd = migr_dir_get_fd(current_dir);
		ret = ifs_domain_add(dirfd, md->dirent->d_name, 
		    md->dirent->d_encoding, sw_ctx->global.domain_id);
		if (ret != 0) {
			if (errno == EEXIST) {
				log(DEBUG, "stf_upgrade_dir_worker: "
				    "ifs_domain_add returned (%d):"
				    " ignoring", errno);
			} else if (errno == ENOENT && 
			    sw_ctx->global.action == SIQ_ACT_SYNC) {
				/*
				 * For sync jobs, delete operation could
				 * be going in parallel, so ignore it.
				 */
				log(ERROR, "stf_upgrade_dir_worker: "
				    "ifs_domain_add returned (%d):"
				    " ignoring", errno);
			} else {
				migr_dir_unref(current_dir, md);
				log(ERROR, "stf_upgrade_dir_worker: "
				    "ifs_domain_add error (%d)", errno);
				error = isi_system_error_new(errno, 
				    "Failed to add %s to domain %lld", 
				    md->dirent->d_name, 
				    sw_ctx->global.domain_id);
				goto out;
			}
		}
		type = dirent_type_to_file_type(md->dirent->d_type);
		if (type == SIQ_FT_DIR) {
			/* Bug 141676 - Don't migr_tw_push after ENOENT */
			if (ret == 0 || errno == EEXIST) {
				/* Push onto stack and return (depth first
				 * traversal) */
				log(TRACE, "stf_upgrade_dir_worker: pushing subdir");
				migr_tw_push(tw, md, &error);
			}
			migr_dir_unref(current_dir, md);
			goto out;
		}
		/* Load next migr_dirent */
		migr_dir_unref(current_dir, md);

		/* Bug 190016 - check connection prior to migr_dir_read() */
		check_connection_with_primary(sw_ctx, &error);
		if (error)
			goto out;

		md = migr_dir_read(current_dir, &error);
		/*
		 * Bug 190016
		 * Ignore ENOENT here because it might not be on the next
		 * dirent, and we'll catch it in a later loop iteration.
		 */
		if (error) {
			if(isi_system_error_is_a(error, ENOENT)) {
				isi_error_free(error);
				error = NULL;
			} else {
				log(ERROR, "%s: "
				    "error in migr_dir_read loop", __func__);
				goto out;
			}
		}
	}
	dir_completed = true;
out:

	log(TRACE, "stf_upgrade_dir_worker: end");
	isi_error_handle(error, error_out);
	return dir_completed;
}

static void
stf_upgrade_abandoned_files(struct migr_sworker_ctx *sw_ctx, 
    uint64_t prev_cookie, uint64_t cur_cookie, struct path *path,
    struct isi_error **error_out)
{
	int fd = -1;	
	struct migr_tw tw = {};
	struct dir_slice slice = { .begin = prev_cookie, .end = cur_cookie };
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct domain_entry de = {};
	bool continue_flag = true;
	struct isi_error *error = NULL;

	log(TRACE, "%s: %llx-%llx", __func__, slice.begin, slice.end);

	/* short-circuit if domain already marked ready */
	if (dom_get_entry(global->domain_id, &de) != 0) {
		ON_SYSERR_GOTO(out, errno, error,
		    "Failed dom_get_entry for %llu",
		    global->domain_id);
	}

	if ((de.d_flags & DOM_READY) != 0)
		goto out;

	/* Instantiate treewalker */
	log(TRACE, "%s: migr_tw_init path: %s", __func__, path->path);

	/*
	 * Lets dup fd and give it treewalk. This will be 
	 * closed in migr_tw_clean
	 */
	fd = dup(sw_ctx->worker.cwd_fd);	
	migr_tw_init(&tw, fd , path, &slice, NULL, true, false, &error);
	if (error) {
		log(ERROR, "%s: migr_tw_init error", __func__);
		goto out;
	}
	fd = -1;
	/* Iterate */
	log(TRACE, "%s: loop begin", __func__);
	while (continue_flag) {
		bool dir_completed = false;
		struct migr_dir *current_dir = NULL;

		log(TRACE, "%s: looping", __func__);

		current_dir = migr_tw_get_dir(&tw, &error);
		log(TRACE, "%s: current_dir = %s", __func__,
		    current_dir->name);
		if (error) {
			log(ERROR, "%s: migr_tw_get_dir error", __func__);
			goto out;
		}
		log(TRACE, "%s: exec worker", __func__);
		dir_completed = stf_upgrade_dir_worker(sw_ctx, &tw, current_dir,
		    &error);
		if (error) {
			log(ERROR, "%s: stf_upgrade_dir_worker error",
			    __func__);
			goto out;
		}
		log(TRACE, "%s: post worker", __func__);
		if (dir_completed) {
			log(TRACE, "%s: popping dir %s", __func__,
			    current_dir->name);
			if (strcmp(current_dir->name, "BASE") == 0) {
				log(TRACE, "%s: last dir", __func__);
				continue_flag = false;
			} else {
				continue_flag = migr_tw_pop(&tw, current_dir);
			}
		}
	}

out:
	if (error && fd > 0) {
		close(fd);
	}

	log(TRACE, "%s: end", __func__);
	migr_tw_clean(&tw);
	isi_error_handle(error, error_out);
}

static void
set_domain_upgrade(struct migr_sworker_ctx *sw_ctx, char *filename, 
    enc_t enc, uint64_t prev_cookie, uint64_t cur_cookie, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *fullpath = NULL;
	
	asprintf(&fullpath, "%s/%s", sw_ctx->worker.cur_path.path, filename);
	ASSERT(fullpath != NULL);
	log(TRACE, "set_domain_upgrade: %s", fullpath);
	
	stf_upgrade_abandoned_files(sw_ctx, prev_cookie, cur_cookie, 
	    &sw_ctx->worker.cur_path, &error);

	log(TRACE, "set_domain_upgrade: exit");
	if (fullpath)
		free(fullpath);
	isi_error_handle(error, error_out);
}

void
clear_user_attrs(int fd, struct stat *st, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_userattr_key keys[IFS_USERATTR_MAXKEYS];
	int num_attrs, res, i;
	struct worm_state worm = {};
	bool is_worm = false;

	if (fd <= 0)
		goto out;

	/*
	 * Bug 151741
	 * Check the worm state before calling ifs_userattr_delete()
	 */
	if (st) {
		is_worm = get_worm_state(st->st_ino, HEAD_SNAPID, &worm, NULL,
		    &error);
		if (error || (is_worm && worm.w_committed))
			goto out;
	}

	num_attrs = ifs_userattr_list(fd, keys);
	if (num_attrs == -1) {
		error = isi_system_error_new(errno,
		    "Failed to get attribute list");
		goto out;
	}

	for (i = 0; i < num_attrs; i++) {
		res = ifs_userattr_delete(fd, keys[i].key);
		/* Bug 101357
		 * Job split on wide directory
		 * may result in multiple workers trying to
		 * clear attributes on the the same directory.
		 * Ignore ENOATTR errors since the source
		 * attributes will be copied later
		 */
		if (res == -1 && errno != ENOATTR) {
			error = isi_system_error_new(errno,
			    "Failed to delete user attribute %s", keys[i].key);
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
}

static void
check_or_add_lin_entry(struct file_begin_msg *begin,
    struct migr_sworker_ctx *sw_ctx, struct file_state *fstate,
    struct isi_error **error_out)
{
	int ret;
	bool success = false;
	struct stat st;
	ifs_lin_t tmp_lin = 0;
	ifs_lin_t slin = begin->src_lin;
	struct isi_error *error = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;

	/* Update linmap entry if not already done */
	if (global->stf_sync && (global->initial_sync ||
	    global->stf_upgrade_sync)) {
		ASSERT(fstate->fd > 0);
		
		set_domain_upgrade(sw_ctx, begin->fname, begin->fenc, 
		    begin->prev_cookie, begin->src_dkey, &error);
		if (error)
			goto out;
		ret = fstat(fstate->fd, &st);
		if (ret != 0) {
			error = isi_system_error_new(errno,
			    "Failed to stat %s/%s", worker->cur_path.path,
			    fstate->name);
			goto out;
		}
		if (!global->do_bulk_tw_lmap) {
			success = get_mapping(slin, &tmp_lin,
			    global->lmap_ctx, &error);
			if (error) {
				goto out;
			}
			if (success) {
				ASSERT(tmp_lin == st.st_ino);
			} else {
				success = set_mapping_cond(slin, st.st_ino,
				    global->lmap_ctx, true, 0, &error);
				if (error) {
					goto out;
				}
				/* Its singly linked file, so we should succeed */
				ASSERT(success == true);
			}
		}
	}
out:
	isi_error_handle(error, error_out);
}

static int
file_begin_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct file_begin_msg *begin = &m->body.file_begin_msg;
	struct file_state *fstate = NULL;
	struct stat st, tmp_st = {0};
	int ret;
	bool file_exists;
	bool success;
	int nlinks;
	struct isi_error *error = NULL;
	struct isi_error *dummy_error = NULL;
	enum file_type type = begin->file_type;
	unsigned int dev_major = 0, dev_minor = 0;
	uint64_t lin_out = 0;
	uint64_t file_lin = 0;
	bool valid_file = false;
	ifs_lin_t tmp_lin = 0;
	int tmp_fd = -1, err_no;

	ASSERT(global->job_ver.common >= MSG_VERSION_SBONNET);

	/* pworker should not be sending unknown or unsupported types */
	ASSERT(type != SIQ_FT_UNKNOWN);

	log(TRACE, "%s", __func__);
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");

	ASSERT(worker->cwd_fd > 0);
	ASSERT(inprog->dstate.dir_valid || global->stf_sync);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	log(TRACE, "Get file %s(%d)", begin->fname, begin->fenc);

	if (!inprog->ads)
		inprog->file_obj = true;

	UFAIL_POINT_CODE(begin_fstatat_fail,
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

	/* Skip bb hash if syncing a hard link */
	if (begin->skip_bb_hash) {
		log(DEBUG, "skipping bb hash for hard link %s", begin->fname);
		fstate->skip_bb_hash = true;
	}

	/*
	 * We dont want to delete file if we have already done all the 
	 * work and worker restarted.
	 */
	if (global->stf_sync && (global->initial_sync ||
	    global->stf_upgrade_sync) && !sw_ctx->inprog.ads &&
	    file_exists) {
		
		success = get_mapping(begin->src_lin, &tmp_lin,
		    global->lmap_ctx, &error);
		if (error)
			goto out;

		if (success) {
			if (st.st_ino == tmp_lin)
				valid_file = true;
		}
	}
	
	/* Determine if we can do a bulk linmap update for initial treewalk
	 * syncs before we exit due to diff sync
	 */
	if (global->stf_sync && !sw_ctx->inprog.ads && 
	    (global->initial_sync || global->stf_upgrade_sync) && 
	    (type == SIQ_FT_REG && begin->symlink && 
	    strcmp(begin->symlink, NHL_FLAG) == 0)) {
		log(DEBUG, "%s: do_bulk_tw_lmap = true", __func__);
		global->do_bulk_tw_lmap = true;
		ASSERT(global->lin_map_blk);
	} else if (sw_ctx->inprog.ads) {
		//Do nothing!
	} else {
		/* Force a flush because we can not cache updates for 
		 * hardlinks.  Since regular files can be interspersed, 
		 * they should be flushed to disk first. */
		log(DEBUG, "%s: do_bulk_tw_lmap = false", __func__);
		sync_lmap_updates(sw_ctx, 0, 0, true, &error);
		if (error)
			goto out;
		global->do_bulk_tw_lmap = false;
	}
	
	/*
	 * If regular sync, we delete the old file and create and open the new
	 * file here. For diff sync, the file is only opened/created if this
	 * is a known NEEDS_SYNC.  Otherwise for diff sync, the file is
	 * UNDECIDED and we hold off opening/creating the file until later.
	 */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx) &&
	    fstate->sync_state != NEEDS_SYNC) {
		goto out;
	}

	/* In the case of diff sync and NEEDS_SYNC, if the file
	 * exists, it has already been opened. It has to be closed so
	 * we can delete it and create the new one.
	 */
	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx) && fstate->fd != -1) {
		close(fstate->fd);
		fstate->fd = -1;
	}

	if (file_exists && (st.st_flags & SF_FILE_STUBBED)) {
retry_purge:
		ASSERT(error == NULL);
		tmp_fd = enc_openat(worker->cwd_fd, begin->fname, begin->fenc,
		    O_RDWR | O_NOFOLLOW, 0600);
		if (tmp_fd < 0) {
			error = isi_system_error_new(errno,
			    "Failed to open file %s/%s for isi_cbm_purge",
			    worker->cur_path.path, begin->fname);
			goto out;
		}

		/*
		 * Bug 249767
		 * Double check that lin still exists and requires purge. This
		 * is needed in case of hard-links where other sworkers race to
		 * purge the lin with current sworker.
		 */
		ret = enc_fstatat(worker->cwd_fd, begin->fname, begin->fenc,
		    &tmp_st, AT_SYMLINK_NOFOLLOW);
		if (ret == 0) {
			if (tmp_st.st_flags & SF_FILE_STUBBED)
				isi_cbm_purge(tmp_fd, &error);
		}

		close(tmp_fd);
		tmp_fd = -1;
		if (error) { /* isi_cbm_purge failed */
			err_no = isi_system_error_get_error(error);
			log(DEBUG, "%s: isi_cbm_purge failed:%s(%d)",
			    __func__, strerror(err_no), err_no);
			if (err_no == EINPROGRESS) {
				siq_nanosleep(10, 0);
				isi_error_free(error);
				error = NULL;
				goto retry_purge;
			} else if (err_no == EINVAL || err_no == ENOENT) {
				log(TRACE, "%s: Received %s from isi_cbm_purge,"
				    " skipping", __func__, strerror(err_no));
				isi_error_free(error);
				error = NULL;
			} else
				goto out;
		}
	}

	/*
	 * Delete the existing file if something is already in
	 * this location and either the new file we're creating
	 * is a symlink, or the existing file isn't a
	 * normal file, or the existing file has multiple links.
	 */
	if (file_exists && (type == SIQ_FT_SYM || type == SIQ_FT_CHAR ||
	    type == SIQ_FT_BLOCK || type == SIQ_FT_SOCK ||
	    type == SIQ_FT_FIFO || !fstate->orig_is_reg_file ||
	    nlinks > 1) && !valid_file) {
		delete(begin->fname, begin->fenc, sw_ctx, &error);
		if (error)
			goto out;
	} else if (file_exists && fstate->fd > 0) {
		/*
		 * Clear any existing user attributes since new ones (if any)
		 * will be sent following this message
		 */
		clear_user_attrs(fstate->fd, &st, &error);
		if (error)
			goto out;

	}

	UFAIL_POINT_CODE(begin_open_or_create_fail,
	    error = isi_system_error_new(RETURN_VALUE,
	    "Failed to open %s/%s", worker->cur_path.path,
	    begin->fname); goto out);

	/* Hard link handling for stf initial/upgrade syncs */
	if (global->stf_sync && (global->initial_sync ||
	    global->stf_upgrade_sync) && !sw_ctx->inprog.ads &&
	    !valid_file) {
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

	case SIQ_FT_FIFO:
		fstate->fd = open_or_create_fifo(sw_ctx, worker->cwd_fd,
		    begin->fname, begin->fenc, &lin_out, &error);
		if (error)
			goto out;
		break;

	default:
		ASSERT(false);
	}

	fstate->tgt_lin = lin_out;

	/* Add source and target LINs to the lin map. If this is an initial
	 * sync and the mapping fails, another worker beat us to it. Delete the
	 * newly created file and instead create a link to the dlin */
	if (global->stf_sync && !sw_ctx->inprog.ads) {
		log(DEBUG, "Adding lin mapping %llx->%llx for file %s",
		    begin->src_lin, lin_out, begin->fname);

		ASSERT(lin_out);
		if (global->initial_sync || global->stf_upgrade_sync) {
			set_domain_upgrade(sw_ctx, begin->fname, begin->fenc, 
			    begin->prev_cookie, begin->src_dkey, &error);
			if (error)
				goto out;
			if (type == SIQ_FT_REG && begin->symlink &&
			    strcmp(begin->symlink, NHL_FLAG) == 0 &&
			    !DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
				//Diff sync is handled later
			    	log(DEBUG, "%s: Adding %s to bulk queue", 
			    	    __func__, begin->fname);
				sync_lmap_updates(sw_ctx, begin->src_lin, 
				    lin_out, false, &error);
				if (error)
					goto out;
				else
					goto done;
			}
			//This was flushed earlier
			success = set_mapping_cond(begin->src_lin, lin_out,
			    global->lmap_ctx, true, 0, &error);
			if (error)
				goto out;

			if (!success) {
				success = get_mapping(begin->src_lin, &tmp_lin,
				    global->lmap_ctx, &error);
				if (error)
					goto out;
				ASSERT(success == true);
				/*
				 * We already have set correct mapping 
				 */
				if (tmp_lin == lin_out) {
					goto done;
				}
					
				/* Another worker has created and mapped the
				 * file. Delete the one just created and
				 * instead link to the mapped dlin */
				delete(begin->fname, begin->fenc, sw_ctx,
				    &error);
				if (error)
					goto out;
				close(fstate->fd);
				fstate->fd = -1;

				fstate->fd = make_hard_link(begin->fname,
				    begin->fenc, tmp_lin, worker->cwd_fd,
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

done:
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
		file_lin = inprog->file_obj ?
                   (inprog->ads ? inprog->sstate.src_filelin :
                   fstate->src_filelin) : (inprog->dstate.src_dirlin);
		
		sw_ctx->stats.io_errors++;
		fstate->error_closed_fd = true;
	} else {
		/* Initialize fstate with the data from begin */
		initialize_fstate_fields(sw_ctx, fstate, begin);
	 
		if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
			TAILQ_INSERT_TAIL(inprog->fstates, fstate, ENTRIES);
			inprog->fstates_size++;
			
			check_or_add_lin_entry(begin, sw_ctx, fstate, &error);
			if (error)
				goto fail;
		}

		log(TRACE, "Remember source file's lin %lld",
		    (u_int64_t)begin->src_lin);

		sw_ctx->stats.ads_of_curfile++;
	}
fail:	
	sw_process_fs_error(sw_ctx, SW_ERR_FILE, fstate->name, 
	    fstate->enc, file_lin, error);
	isi_error_free(error);
	return 0;
}

static int
old_file_begin2_callback(struct generic_msg *m, void *ctx)
{
	struct file_begin_msg *begin = &m->body.file_begin_msg;
	struct old_file_begin_msg2 *oldbegin = &m->body.old_file_begin2;

	//XXXDPL HACK - Ugly. Copy contents of old_file_begin_msg2 into 
	// file_begin_msg, which is the same struct plus 2 extra fields.
	*((struct old_file_begin_msg2*)begin) = *oldbegin;
	begin->worm_committed = 0;
	begin->worm_retention_date = 0;

	return file_begin_callback(m, ctx);
}

static int
worm_state_callback(struct generic_msg *m, void *ctx)
{
	int num_links_created;
	struct worm_state old_worm_state = {};
	struct isi_error *error = NULL;
	ifs_lin_t dlin = 0;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	struct file_state *fstate = NULL;

	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx))
		fstate = sw_ctx->inprog.ds_tmp_fstate;
	else
		fstate = &sw_ctx->inprog.fstate;
	log(TRACE, "%s", __func__);

	ASSERT(m->head.type == WORM_STATE_MSG);
	ASSERT(fstate != NULL);

	fstate->stat_state.worm_retention_date =
	    m->body.worm_state.retention_date;
	fstate->stat_state.worm_committed =
	    m->body.worm_state.committed;

	/*
	 * If the new worm state is committed and the old worm state is not,
	 * then we create the canonical links in the compliance store For
	 * the dlin under the source_guid directory in the compliance store
	 * using source_lin for the file name
	 *
	 * Create canonical links in the compliance store only if it is a
	 * compliance V2 domain.
	 */
	if (global->compliance_dom_ver == 2 && m->body.worm_state.committed) {
		dlin = fstate->tgt_lin;

		get_worm_state(dlin, HEAD_SNAPID, &old_worm_state,
		    NULL, &error);
		if (error)
			goto out;

		if (!old_worm_state.w_committed) {
			num_links_created = create_cstore_links_target(
			    dlin, global->comp_cluster_guid,
			    global->target_base_path.path, true,
			    global->is_src_guid ? fstate->src_filelin : dlin,
			    &error);
			if (error)
				goto out;

			log(DEBUG, "%s: num_links_created: %d",  __func__,
			    num_links_created);
		}
	}

out:
	if (error) {
		sw_process_fs_error(sw_ctx, SW_ERR_FILE, fstate->name,
		    fstate->enc, dlin, error);
		isi_error_free(error);
	}
	return 0;
}

static void
send_error_and_die(struct migr_sworker_ctx *sw_ctx, struct isi_error **error)
{
	struct isi_error *siq_err = NULL;

	if (!isi_error_is_a(*error, ISI_SIQ_ERROR_CLASS)) {
		char *msg = isi_error_get_detailed_message(*error);
		siq_err = isi_siq_error_new(E_SIQ_FILE_TRAN,
		    "Error sending cloudpools data: %s", msg);
		free(msg);
		isi_error_free(*error);
		*error = siq_err;
	}

	siq_send_error(sw_ctx->global.primary,
	    (struct isi_siq_error *)*error, EL_SIQ_DEST);

	log(FATAL, "%s", isi_error_get_detailed_message(*error));
}
static bool
should_create_dsession(struct migr_sworker_ctx *sw_ctx,
    struct file_state *fstate, struct stat *st,
    enum isi_cbm_backup_sync_type sync_type)
{
	if (sync_type == BST_STUB_SYNC &&
	    (fstate->stat_state.flags & SF_FILE_STUBBED))
		return true;

	if (!sw_ctx->global.initial_sync &&
	    !sw_ctx->global.stf_upgrade_sync &&
	    sw_ctx->global.stf_sync &&
	    sync_type == BST_DEEP_COPY &&
	    (st->st_flags & SF_FILE_STUBBED))
		return true;

	return false;
}


static int
cloudpools_setup_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct stat st = {};
	struct isi_file_dsession_opt dso = {};

	struct file_state *fstate = NULL;
	struct isi_error *error = NULL;
	uint8_t sync_type;

	log(TRACE, "%s", __func__);

	RIPT_MSG_GET_FIELD(uint8, &m->body.ript, RMF_CPOOLS_SYNC_TYPE, 1,
	    &sync_type, &error, out)

	fstate = get_inprog_fstate(sw_ctx);
	ASSERT(fstate != NULL && fstate->dsess == NULL);

	ASSERT(sync_type != BST_NONE);
	fstate->cp_sync_type = sync_type;

	if (fstat(fstate->fd, &st) != 0) {
		error = isi_system_error_new(errno, "%s could not stat file "
                    "with fd %d", __func__, fstate->fd);
		goto out;
	}

	// For initial sync, isi_cbm_purge() happens on FILE_BEGIN_MSG.
	ASSERT(!((st.st_flags & SF_FILE_STUBBED) && !sw_ctx->global.stf_sync));

	if ((st.st_flags & SF_FILE_STUBBED) &&
	    sw_ctx->global.stf_sync &&
	    !sw_ctx->global.initial_sync &&
	    !sw_ctx->global.stf_upgrade_sync &&
	    fstate->cp_sync_type != BST_DEEP_COPY) {
		log(DEBUG, "%s doing CBM purge (sync type = %d)",
		    __func__, fstate->cp_sync_type);
		isi_cbm_purge(fstate->fd, &error);
		if (error)
			goto out;
	}

	isi_file_dsession_opt_init(&dso);
	//ignore retention expiry during restore for siq
	dso.override_retention_expiration = true;
	dso.data_only = (fstate->cp_sync_type == BST_DEEP_COPY);

	/*
	 * If the target is also Riptide or later, this should be the only
	 * place where we create the dsession. In pre-Riptide target clusters
	 * we create the dsession in write_file_data() and write_file_sparse()
	 * since they do not know about the cloudpools_setup msg.
	 */
	if (should_create_dsession(sw_ctx, fstate, &st, sync_type)) {
		fstate->dsess = isi_file_dsession_create(fstate->fd, &dso,
		    &error);
		if (error)
			goto out;

		ASSERT(fstate->dsess);
	}

out:
	if (error)
		send_error_and_die(sw_ctx, &error);
	return 0;
}

static int
old_cloudpools_msg_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	// Riptide pworkers don't send CLOUDPOOLS_BEGIN_MSG or
	// CLOUDPOOLS_DONE_MSG. Pre-Riptide stub sync is not supported.
	ASSERT(sw_ctx->global.job_ver.common < MSG_VERSION_RIPTIDE);
	error = isi_siq_error_new(E_SIQ_FILE_DATA,
	    "Unsupported version for Cloudpools data");
	send_error_and_die(sw_ctx, &error); // No need to free error.

	return 0;
}

static int
cloudpools_stub_data_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct file_state *fstate = NULL;
	struct siq_ript_msg *ript = &m->body.ript;

	struct isi_error *error = NULL;
	uint8_t *data = NULL;
	size_t size = -1;  // Use MAX_SIZE_T as default case.
	uint32_t type;

	log(TRACE, "%s", __func__);

	fstate = get_inprog_fstate(sw_ctx);
	ASSERT(fstate->dsess != NULL);
	ASSERT(fstate->cp_sync_type == BST_STUB_SYNC);

	ript_msg_get_field_bytestream(ript, RMF_DATA, 1, &data,
	    (uint32_t *)&size, &error);
	if (error != NULL)
		goto out;

	RIPT_MSG_GET_FIELD(uint32, ript, RMF_TYPE, 1, &type, &error, out);

	log(DEBUG, "%s: got %lu byte %s blob", __func__, size,
	    type == SCT_DATA ? "data" : "metadata");

	isi_file_dsession_write_blob(fstate->dsess,
	    (enum isi_cbm_stub_content_type)type, (void *)data, size, &error);
	if (error)
		goto out;

out:
	if (error)
		send_error_and_die(sw_ctx, &error); // No need to free error.

	return 0;
}

static int
user_attr_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct isi_error *error = NULL;
	struct file_state *fstate = &(sw_ctx->inprog.fstate);
	uint64_t lin = 0;
	int attr_fd = -1;
	int res;
	struct stat st = {};
	struct worm_state worm = {};
	bool is_worm = false;

	log(TRACE, "%s", __func__);

	if (m->body.user_attr.is_dir)
		attr_fd = sw_ctx->worker.cwd_fd;
	else {
		if (sw_ctx->global.doing_diff_sync)
			fstate = sw_ctx->inprog.ds_tmp_fstate;
		else
			fstate = &sw_ctx->inprog.fstate;
		attr_fd = fstate->fd;
		/*
		 * Bug 151741
		 * Check the worm state before calling ifs_userattr_set()
		 */
		res = fstat(attr_fd, &st);
		if (res != 0) {
			error = isi_system_error_new(errno,
			    "Unable to stat %s", fstate->name);
			goto out;
		}

		is_worm = get_worm_state(st.st_ino, HEAD_SNAPID, &worm, NULL,
		    &error);
		if (error || (is_worm && worm.w_committed))
			goto out;
	}

	res = ifs_userattr_set(attr_fd, m->body.user_attr.key,
	    m->body.user_attr.val, strlen(m->body.user_attr.val));
	if (res == -1) {
		error = isi_system_error_new(errno, "Failed to set attribute");
		goto out;
	}

	log(DEBUG, "Set attribute key %s value %s",
	    m->body.user_attr.key, m->body.user_attr.val);
	
out:
	if (error) {
		if (fstate && fstate->fd != -1) {
			close(fstate->fd);
			fstate->fd = -1;
			fstate->error_closed_fd = true;
		}
		sw_ctx->inprog.result = ACK_RES_ERROR;
		lin = sw_ctx->inprog.file_obj ?
		    (sw_ctx->inprog.ads ? sw_ctx->inprog.sstate.src_filelin :
		    sw_ctx->inprog.fstate.src_filelin) :
		    sw_ctx->inprog.dstate.src_dirlin;
		sw_process_fs_error(sw_ctx, SW_ERR_FILE, fstate->name,
		    fstate->enc, lin, error);
		sw_ctx->stats.io_errors++;
	}

	isi_error_free(error);
	return 0;
}

static int
ads_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct file_state *fstate = NULL;
	int fd, container_fd;
	bool ads_container_is_dir;
	char *name;
	char *alloc_name = NULL;
	uint64_t dlin = 0;
	struct stat st;
	struct isi_error *error = NULL;

	log(TRACE, "%s: start=%d", __func__, m->body.ads.start);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;
	ASSERT(inprog->dstate.dir_valid || global->stf_sync);
	if (global->secret)
		log(FATAL, "Attempt to circumvent authentication encountered");
	if (m->body.ads.start) {
		if (worker->saved_cwd != -1) {
			error = isi_system_error_new(ENOTDIR, 
			    "Error accessing dest ADS directory of file %s: "
			    "Unexpected state in ads start", 
			    worker->cur_path.path);
			goto out;
		}

		if (global->doing_diff_sync) {
			/* Last entry in TAILQ is always latest file being
			 * processed, and ADS files are only sent for the
			 * latest file */
			if (!TAILQ_EMPTY(inprog->fstates)) {
				fstate = TAILQ_LAST(inprog->fstates,
				    file_state_list);
				/* If the file wasn't transferable, error and
				 * exit */
				if (fstate->error_closed_fd) {
					error = isi_system_error_new(ENOTDIR,
					    "Error accessing dst ADS directory "
					    " of file %s: file not "
					    "transferable", 
					    worker->cur_path.path);
					goto out;
				}
				ASSERT(fstate->fd != -1);
				ads_container_is_dir = false;
				container_fd = fstate->fd;
				name = fstate->name;
			}
			/* If TAILQ is empty, this ADS_MSG must have been for
			 * a directory */
			else {
				ads_container_is_dir = true;
				container_fd = worker->cwd_fd;
				name = worker->cur_path.path;
			}
		} else {
			if (inprog->fstate.error_closed_fd) {
				/* If the file wasn't transferable, error and
				 * exit */
				error = isi_system_error_new(ENOTDIR,
				    "Error accessing dst ADS directory "
				    " of file %s: file not transferable",
				    worker->cur_path.path);
				goto out;
			} else if (inprog->fstate.fd == -1) {
				ads_container_is_dir = true;
				container_fd = worker->cwd_fd;
				name = worker->cur_path.path;
			} else {
				ads_container_is_dir = false;
				container_fd = inprog->fstate.fd;
				name = inprog->fstate.name;

				/* If the name is still null, store dlin
				 * for path lookup below. */
				if (name == NULL)
					get_mapping(inprog->fstate.src_filelin,
					    &dlin, global->lmap_ctx, NULL);
			}
		}
		if (ads_container_is_dir || name == NULL) {
			if (!dlin) {
				if (fstat(container_fd, &st) == -1) {
					error = isi_system_error_new(errno,
					    "fstat error on ADS directory under"
					    " %s", worker->cur_path.path);
					goto out;
				}
				dlin = st.st_ino;
			}
			dir_get_utf8_str_path("/ifs/", ROOT_LIN, dlin,
			    HEAD_SNAPID, 0, &alloc_name, NULL);
			name = alloc_name;
		}
		log(TRACE, "%s: ADS Container %s is a %s", __func__,
		    name, ads_container_is_dir ? "dir" : "file");

		fd = enc_openat(container_fd, ".", 
		    ENC_DEFAULT, O_RDONLY | O_XATTR);
		UFAIL_POINT_CODE(ads_chdir,
		    if (fd != -1) {close(fd); fd = -1;});

		if (fd == -1) {
			if (alloc_name != NULL)
			    error = isi_system_error_new(errno,
				"Error opening ads dir of %s %s",
				ads_container_is_dir ? "dir" : "file",
				alloc_name);
			else
			    error = isi_system_error_new(errno,
				"Error opening ads dir of %s %s/%s",
				ads_container_is_dir ? "dir" : "file",
				worker->cur_path.path, name);

			goto out;
		}

		/* Save cwd, and set it to the fd we just opened. It will be
		 * restored in the ads stop call. */
		worker->saved_cwd = worker->cwd_fd;
		worker->cwd_fd = fd;

		/* Non-diff sync case save current fstate. Diff sync case,
		 * we'll be using sw_ctx->inprog.fstate for the ADS files
		 * (since it's not used for anything else)*/
		if (!global->doing_diff_sync) {
			inprog->sstate = inprog->fstate;
			memset(&inprog->fstate, 0, sizeof inprog->fstate);
			inprog->fstate.fd = -1;
		}
	} else {
		if (worker->saved_cwd == -1 && !global->stf_sync) {
			error = isi_system_error_new(ENOTDIR,
			    "Unexpected state in ads end");
			goto out;
		}

		if (global->job_ver.common < MSG_VERSION_SBONNET)
			old_finish_file(sw_ctx);
		else
			ASSERT(inprog->fstate.fd == -1);

		close(worker->cwd_fd);

		/* Restore the cwd that was saved in the ads start call. */
		worker->cwd_fd = worker->saved_cwd;
		worker->saved_cwd = -1;

		/* Restore fstate for regular sync */
		if (!global->doing_diff_sync)
			inprog->fstate = inprog->sstate;

		// XXX DNR This isn't needed for v2.5. Is it needed for legacy?
		if (inprog->fstate.fd == -1) {
			/* 
			 * There was the directory ads (symlinks too, but 
			 * at now, no reason to avoid it ultimately)
			 */
			sw_ctx->stats.curfile_size = 0;
			sw_ctx->stats.ads_of_curfile = 0;
		}
	}
	inprog->ads = m->body.ads.start;
	
out:
	sw_process_fs_error(sw_ctx, SW_ERR_ADS, NULL, 0, dlin, error);
	isi_error_free(error);
	if (alloc_name)
	    free(alloc_name);
	return 0;
}

static int
error_callback(struct generic_msg *msg, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	
	send_stats(sw_ctx);
	sw_ctx->inprog.result = ACK_RES_ERROR;
	return 0;
}

static void
write_file_data(struct file_state *fstate, char *data, off_t offset, int size,
    unsigned source_msg_version, ifs_lin_t *wlin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	off_t ret;
	ssize_t got;
	struct isi_file_dsession_opt dso = {};
	struct stat st = {};

	log(TRACE, "%s", __func__);

	if (fstat(fstate->fd, &st) != 0) {
		error = isi_system_error_new(errno,
		    "%s: Could not fstat file.", __func__);
		goto out;
	}

	/**
	 * Bug 183663
	 * Return st_ino to the caller in case we haven't updated the linmap
	 * yet
	 */
	if (wlin)
		*wlin = st.st_ino;

	// If writing file data to a stub, it must be deep copy.
	if (st.st_flags & SF_FILE_STUBBED) {
		/*
		 * If the dsession does not exist, then the source must be
		 * a pre-Riptide cluster as dsessions should be created in
		 * cloudpools_setup_callback(). In the event of a pre-Riptide
		 * source, the cloudpools_setup msg is not sent so we revert
		 * back to creating the dsession in write_file_data().
		 *
		 * cloudpools_setup_callback() also sets the sync_type.
		 * Since we know that we got file data and the file on
		 * the target is a stub, it must be deep copy so on the
		 * first pass through write_file_data(), set the sync type
		 * accordingly.
		 */
		if (!fstate->dsess) {
			ASSERT(source_msg_version < MSG_VERSION_RIPTIDE);
			isi_file_dsession_opt_init(&dso);
			//ignore retention expiry during restore for siq
			dso.override_retention_expiration = true;
			dso.data_only = true;
			fstate->dsess = isi_file_dsession_create(fstate->fd,
			    &dso, &error);
			if (error)
				goto out;

			ASSERT(fstate->dsess != NULL);

			fstate->cp_sync_type = BST_DEEP_COPY;
		}
		ASSERT(fstate->cp_sync_type == BST_DEEP_COPY);
		got = isi_file_dsession_write_file(fstate->dsess, data, offset,
		    size, &error);
		if (error)
			goto out;

		if (got == -1) {
			ON_SYSERR_DEXIT_GOTO(out, errno, error,
			    "write error writing file data at offset %llu, "
			    "length %u", offset, size);
		} else if (got != size) {
			error = isi_system_error_new(EIO,
			    "partial write error writing file data at "
			    "offset %llu, full length %u, partial "
			    "length %zd", offset, size, got);
			goto out;
		}

		goto out;
	}

	ret = lseek(fstate->fd, offset, SEEK_SET);
	UFAIL_POINT_CODE(normal_write_failure, ret = -1; errno = EIO);
	if (ret == -1) {
		ON_SYSERR_DEXIT_GOTO(out, errno, error,
		    "lseek error writing file data at offset %llu", offset);
	}
	got = write(fstate->fd, data, size);
	if (got == -1) {
		ON_SYSERR_DEXIT_GOTO(out, errno, error,
		    "write error writing file data at offset %llu, length %u",
		    offset, size);
	} else if (got != size) {
		error = isi_system_error_new(EIO,
		    "partial write error writing file data at offset %llu, "
		    "full length %u, partial length %zd", offset, size, got);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

void
write_file_sparse(struct file_state *fstate, off_t off, int size,
    unsigned source_msg_version, ifs_lin_t *wlin, struct isi_error **error_out)
{
	struct stat stat_info, stat_check;
	struct isi_error *error = NULL;
	off_t lseek_ret;
	ssize_t got;
	off_t curr_off = 0, end_off;
	int this_write_size, write_region;
	struct isi_cbm_snap_diff_iterator *diff_iter = NULL;
	struct isi_file_dsession_opt dso = {};

	struct snap_diff_region region = {};
	bool doing_retry = false;
	int ret;

	log(TRACE, "%s", __func__);

	got = fsync(fstate->fd);
	if (got == -1) {
		error = isi_system_error_new(errno,
		    "fssync error in sparse write");
		goto out;
	}

	got = fstat(fstate->fd, &stat_info);
	if (!got)
		UFAIL_POINT_CODE(sparse_write_failure, got = -1; errno = EIO);
	if (got == -1) {
		error = isi_system_error_new(errno,
		    "fstat error in sparse write %llu %u", off, size);
		goto out;
	}

	/**
	 * Bug 183663
	 * Return st_ino to the caller in case we haven't updated the linmap
	 * yet
	 */
	if (wlin)
		*wlin = stat_info.st_ino;

	curr_off = off;
	end_off = MIN(stat_info.st_size, (off + size));

	/*
	 * If the target already has sparse file, then we can skip
	 * some of regions. offcourse  we may still write 0's 
	 * in some of the sparse regions. But that should be 
	 * minimal.
	 */
	diff_iter = isi_cbm_snap_diff_create(stat_info.st_ino, curr_off,
	    INVALID_SNAPID, HEAD_SNAPID,
	    (stat_info.st_flags & SF_FILE_STUBBED) == 0);

	if (!diff_iter) {
		error = isi_system_error_new(errno, "Could not "
		    "initialize snap diff iterator for lin %016llx snap "
		    "0x%16llx", stat_info.st_ino, HEAD_SNAPID);
		goto out;
	}

	write_region = 0;
	while (curr_off < end_off) {
		if (write_region == 0) {
			/*
			 * Get next region to make progress
			 */
			ret = isi_cbm_snap_diff_next(diff_iter, &region);
			if (ret) {
				error = isi_system_error_new(errno,
			    	    "Failed to diff lin %016llx "
				    "snap 0x%016llx",
			      	    stat_info.st_ino, HEAD_SNAPID);
				    goto out;
			}

			if (region.byte_count > 0) { 
				if (region.region_type == RT_SPARSE) {
					curr_off = region.start_offset +
					    region.byte_count;
				} else {
					curr_off = region.start_offset;
					write_region = MIN(region.byte_count,
					    end_off - curr_off);
				}
				doing_retry = false;
				continue;
			}
			

			got = fstat(fstate->fd, &stat_check);
			if (got == -1) {
				error = isi_system_error_new(errno,
				    "fssync error in sparse write");
				goto out;
			}

			/* File is being truncated in parallel, this
			 * should never happen. */
			ASSERT(stat_check.st_size >= stat_info.st_size);

			/* We have flushed all coalesced data and the file
			 * system still returns that nothing is there, this
			 * should never happen. */
			ASSERT(doing_retry == false);

			/*
			 * Bug 122199 - We can get here if the only non-sparse
			 * data past curr_off is in the coalescer and has not
			 * been dumped to disk yet. fstat() gets the file
			 * size including data in the coalescer, and
			 * ifs_snap_diff_next() gets the file size only from
			 * data that has been committed to disk. This can
			 * cause our end_off to be larger than the size of
			 * the file actually committed to disk. Flush the
			 * coalescer and retry.
			 */
			got = fsync(fstate->fd);
			if (got == -1) {
				error = isi_system_error_new(errno,
				    "fsync error in sparse write");
				goto out;
			}
			doing_retry = true;
			continue;
		}

		/* Limit our write to the static buffer size.  Do
		 * in multiple writes if necessary.*/
		this_write_size =
		    MIN(IDEAL_READ_SIZE, write_region);

		if (stat_info.st_flags & SF_FILE_STUBBED) {
			/*
			 * If the dsession does not exist, then the source must
			 * be a pre-Riptide cluster as dsessions should be
			 * created in cloudpools_setup_callback(). In the event
			 * of a pre-Riptide source, the cloudpools_setup msg is
			 * not sent so we revert back to creating the dsession
			 * in write_file_sparse().
			 *
			 * cloudpools_setup_callback() also sets the sync_type.
			 * Since we know that we got file data and the file on
			 * the target is a stub, it must be deep copy so on the
			 * first pass through write_file_sparse(), set the sync
			 * type accordingly.
			 */
			if (!fstate->dsess) {
				ASSERT(source_msg_version <
				    MSG_VERSION_RIPTIDE);
				isi_file_dsession_opt_init(&dso);
				//ignore retention expiry during restore for siq
				dso.override_retention_expiration = true;
				dso.data_only = true;
				fstate->dsess = isi_file_dsession_create(
				    fstate->fd, &dso, &error);
				if (error)
					goto out;

				ASSERT(fstate->dsess != NULL);

				fstate->cp_sync_type = BST_DEEP_COPY;
			}
			ASSERT(fstate->cp_sync_type == BST_DEEP_COPY);
			got = isi_file_dsession_write_file(fstate->dsess,
			    sworker_zero_buff, curr_off, this_write_size,
			    &error);
			if (error)
				goto out;

			if (got == -1) {
				error = isi_system_error_new(errno,
				    "Target LIN %llx had a write error "
				    "writing file data at offset %llu, "
				    "length %u", stat_info.st_ino,
				    curr_off, this_write_size);
				goto out;
			} else if (got != this_write_size) {
				error = isi_system_error_new(EIO,
				    "Target LIN %llx had a partial write "
				    "error writing file data at offset %llu, "
				    "full length %u, partial length %zd",
				    stat_info.st_ino, curr_off,
				    this_write_size, got);
				goto out;
			}
		} else {
			/* Write zeros if we're in the middle of the file */
			lseek_ret = lseek(fstate->fd, curr_off, SEEK_SET);
			if (lseek_ret == -1) {
				error = isi_system_error_new(errno,
				    "lseek error writing file sparse at %llu",
				    curr_off);
				goto out;
			}

			got = write(fstate->fd, sworker_zero_buff,
			    this_write_size);
			if (got == -1) {
				error = isi_system_error_new(errno,
				    "write zero error %llu %u",
				    curr_off, this_write_size);
				goto out;
			}
		}
		/* Disk writes aren't supposed to return partial
		 * writes. */
		ASSERT(got == this_write_size);

		curr_off += got;
		write_region -= got;
	}

out:
	if (diff_iter)
		isi_cbm_snap_diff_destroy(diff_iter);

	isi_error_handle(error, error_out);
}

uint32_t
sworker_checksum_compute(char *data, int size)
{
	checksum_context checksum_ctx;
	uint32_t result;

	checksumInit(&checksum_ctx, 0);
	checksumUpdate(&checksum_ctx, data, size, 0);

	result = *(uint32_t *)checksum_ctx.result;
	UFAIL_POINT_CODE(checksum_failure, result++);

	return result;
}

static void
file_data_update_bb_hash(struct migr_sworker_ctx *sw_ctx,
    struct generic_msg *msg, struct isi_error **error_out)
{
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (!inprog->fstate.bb_hash_ctx) {
		inprog->fstate.bb_hash_ctx = hash_alloc(HASH_MD5);
		ASSERT(inprog->fstate.bb_hash_ctx);
		hash_init(inprog->fstate.bb_hash_ctx);
	}

	/* Hash skipped region (if any) */
	if (inprog->fstate.cur_off < msg->body.file_data.offset) {
		sw_migr_hash_range_fd(inprog->fstate.fd,
		    inprog->fstate.bb_hash_ctx,
		    inprog->fstate.cur_off,
		    msg->body.file_data.offset - inprog->fstate.cur_off,
		    inprog->fstate.name, &error);
		/* Treat this just like any other IO error.*/
		if (error)
			goto out;
	}

	/* Hash new data */
	switch (msg->body.file_data.data_type) {
		case MSG_DTYPE_DATA: 
			hash_update(inprog->fstate.bb_hash_ctx,
			    msg->body.file_data.data,
			    msg->body.file_data.logical_size);
			break;
		case MSG_DTYPE_SPARSE:
			sw_migr_hash_zero_range(inprog->fstate.bb_hash_ctx,
			    msg->body.file_data.logical_size);
			break;
		case MSG_DTYPE_UNCHANGED:
			sw_migr_hash_range_fd(inprog->fstate.fd,
			    inprog->fstate.bb_hash_ctx,
			    msg->body.file_data.offset,
			    msg->body.file_data.logical_size,
			    inprog->fstate.name, &error);
			break;
		default:
			ASSERT(0, "bad data type %d",
			    msg->body.file_data.data_type);
	}

out:
	isi_error_handle(error, error_out);
}

static void
check_checksum(struct migr_sworker_ctx *sw_ctx, struct file_state *fstate,
    struct generic_msg *msg, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	uint32_t checksum_result;

	ASSERT(sw_ctx->global.checksum_size == 4);
	checksum_result = sworker_checksum_compute(
	    msg->body.file_data.data,
	    msg->body.file_data.logical_size);

	if (msg->body.file_data.checksum != checksum_result) {
		log(ERROR, "Checksum error on file %s occurred "
		    "(0x%x/0x%x)", fstate->name,
		    checksum_result,
		    msg->body.file_data.checksum);
		send_stats(sw_ctx);
		sw_ctx->stats.tw_cur_stats->files->skipped->error_checksum++;
		error = isi_system_error_new(EIO, "bad checksum");
	}
	isi_error_handle(error, error_out);
}

static int
file_data_callback(struct generic_msg *msg, void *ctx)
{
	struct isi_error *error = NULL;
	struct isi_error *error2 = NULL;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct file_state *fstate = NULL;
	char *error_string;
	uint64_t file_lin = 0;
	uint64_t slin = 0, dlin = 0;
	bool committed = false;

	UFAIL_POINT_CODE(file_data_close_conn, close(global->primary));
	UFAIL_POINT_CODE(file_data_sleep, siq_nanosleep(RETURN_VALUE, 0));
	/* Default error string */
	error_string = "Failed to write data";

	ASSERT(global->job_ver.common >= MSG_VERSION_SBONNET);
	ilog(IL_INFO, "%s: off %llu %u", __func__,
	    msg->body.file_data.offset, msg->body.file_data.data_size);

	check_connection_with_primary(sw_ctx, &error);
	if (error)
		goto out;

	if (DIFF_SYNC_BUT_NOT_ADS(sw_ctx)) {
		fstate = ds_file_data(&msg->body.file_data, sw_ctx, &error);
		if (error)
			goto write_error;
	} else
		fstate = &inprog->fstate;

	/* SyncIQ never purposely uses fd 0 for file data, so any use
	 * of it is unintentional and a sign of a bug. */
	ASSERT(fstate->fd != 0);

	/* Check size/type of data are consistent */
	ASSERT(msg->body.file_data.data_type == MSG_DTYPE_DATA ||
	    msg->body.file_data.data_type == MSG_DTYPE_SPARSE ||
	    msg->body.file_data.data_type == MSG_DTYPE_UNCHANGED); 
	ASSERT_IMPLIES(msg->body.file_data.data_type == MSG_DTYPE_DATA,
	    msg->body.file_data.logical_size == msg->body.file_data.data_size);
	ASSERT_IMPLIES(msg->body.file_data.data_type == MSG_DTYPE_SPARSE,
	    msg->body.file_data.data_size == 0);
	ASSERT_IMPLIES(msg->body.file_data.data_type == MSG_DTYPE_UNCHANGED,
	    msg->body.file_data.data_size == 0);
	ASSERT(msg->body.file_data.logical_size > 0);
	ASSERT(msg->head.len > msg->body.file_data.data_size);

	/* This FILE_DATA_MSG is file data, but error has occurred before */
	if (fstate->fd == -1)
		goto out;

	if (msg->body.file_data.data_type == MSG_DTYPE_DATA &&
	    global->checksum_size) {
		check_checksum(sw_ctx, fstate, msg, &error);
		if (error) {
			error_string = "Bad checksum";
			goto write_error;
		}
	}

	/*
	 * We use the bb hash if it's not a full transfer and there is at
	 * least one updated block or a changed size.  This case deals
	 * with the update block.
	 */
	if (!fstate->full_transfer) {
		/* Partial transfer, so may be gaps, but guaranteed in order */
		ASSERT(msg->body.file_data.offset >= fstate->cur_off);
		ASSERT(!global->doing_diff_sync);
		if (!global->skip_bb_hash && !fstate->skip_bb_hash)
			file_data_update_bb_hash(sw_ctx, msg, &error);
	} else {
		/* Full transfer, so no gaps in data messages */
		ASSERT(msg->body.file_data.offset == fstate->cur_off);
	}

	if (error)
		goto write_error;

	verify_run_id(sw_ctx, NULL);

	switch (msg->body.file_data.data_type) {
		case MSG_DTYPE_DATA:
			write_file_data(fstate,
			    msg->body.file_data.data,
			    msg->body.file_data.offset,
			    msg->body.file_data.logical_size,
			    global->job_ver.common, &dlin, &error);
			break;
		case MSG_DTYPE_SPARSE:
			if (!global_config->root->application->
			    force_sparse_writes) {
				write_file_sparse(fstate,
				    msg->body.file_data.offset,
				    msg->body.file_data.logical_size,
				    global->job_ver.common, &dlin, &error);
			} else {
				write_file_data(fstate,
				    (char *)sworker_zero_buff,
				    msg->body.file_data.offset,
				    msg->body.file_data.logical_size,
				    global->job_ver.common, &dlin, &error);
			}
			break;
		case MSG_DTYPE_UNCHANGED:
			break;
		default:
			ASSERT(0, "bad data type %d",
			    msg->body.file_data.data_type);
	}

	/* If write failed because the file is committed, ignore it */
	if (error && isi_system_error_is_a(error, EROFS)) {
		UFAIL_POINT_CODE(file_data_reset_dlin, dlin = 0;);
		if (dlin == 0) {
			slin = inprog->ads ?
			    inprog->sstate.src_filelin : fstate->src_filelin;
			get_mapping(slin, &dlin, sw_ctx->global.lmap_ctx,
			    &error2);
			if (error2)
				goto out;
		}

		committed = is_worm_committed(dlin, HEAD_SNAPID, &error2);
		if (error2)
			goto out;

		if (committed) {
			ilog(IL_INFO,
			    "skipping write to committed file (lin %llx)",
			    __func__, dlin);
			isi_error_free(error);
			error = NULL;
		}
	}

	fstate->bytes_changed += msg->body.file_data.logical_size;
	fstate->cur_off = msg->body.file_data.offset +
	    msg->body.file_data.logical_size;

write_error:
	if (error) {
		inprog->result = ACK_RES_ERROR;
		file_lin = inprog->file_obj ?
                   (inprog->ads ? inprog->sstate.src_filelin :
                   fstate->src_filelin) : (inprog->dstate.src_dirlin);
		close(fstate->fd);
		fstate->fd = -1;
		sw_ctx->stats.io_errors++;
		fstate->error_closed_fd = true;
	}
	sw_ctx->stats.curfile_size = msg->body.file_data.offset +
	    msg->body.file_data.logical_size;

out:
	sw_process_fs_error(sw_ctx, SW_ERR_FILE,
	    fstate != NULL ? fstate->name : "",
	    fstate != NULL ? fstate->enc : 0 , file_lin, error);
	isi_error_free(error);
	isi_error_free(error2);
	return 0;
}

static int
list2_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct isi_error *error = NULL;

	ilog(IL_INFO, "list2 callback called");

	process_list(m->body.list2.list, m->body.list2.used,
	    m->body.list2.range_begin, m->body.list2.range_end,
	    m->body.list2.continued, m->body.list2.last, sw_ctx, &error);

	if (error && sw_ctx->global.job_ver.common >= MSG_VERSION_CHOPU) {
		/* for CHOPU and later sources, we fail the job if there are
		 * any deletion errors. for earlier sources, we continue */
		sw_process_fs_error(sw_ctx, SW_ERR_DIR, inprog->dstate.name,
		    inprog->dstate.enc, inprog->dstate.src_dirlin, error);
	}
		
	isi_error_free(error);
	return 0;
}

static int
done_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	struct generic_msg msg = {};
	struct isi_error *error = NULL;

	ilog(IL_INFO, "%s called", __func__); //TRACE -> INFO
	if (global->job_ver.common < MSG_VERSION_SBONNET)
		old_finish_file(sw_ctx);
	else {
		ASSERT(inprog->fstate.fd == -1);
		if (global->doing_diff_sync)
			ASSERT(TAILQ_EMPTY(inprog->fstates));
	}

	if (global->logdeleted) {
		listflush(&inprog->deleted_files, LIST_TO_SLOG,
		    global->primary);
	}

	m->head.type = DONE_MSG;

	if (global->stf_sync) {
		if (global->stf_operations > 0 && global->last_stf_slin != 0) {
			ilog(IL_INFO, "%s called, stf_operations: %d", __func__,
			    global->stf_operations);
			msg.head.type = LIN_ACK_MSG;
			msg.body.lin_ack.src_lin = global->last_stf_slin;
			msg.body.lin_ack.dst_lin = 0;
			msg.body.lin_ack.result = 0;
			msg.body.lin_ack.siq_err = 0;
			msg.body.lin_ack.oper_count = global->stf_operations;
			msg_send(global->primary, &msg);
		}
		global->stf_operations = 0;
		/*
		 * Before flush , make sure we check connection
		 * with the source worker. Otherwise it could
		 * lead to extremely odd race conditions.
		 */
		check_connection_with_peer(global->primary, true,
		    1, &error);
		if (error) 
			goto out;
		flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
		if (error)
			goto out;

		// Make sure we flush late stage commit list.
		if (comp_ctx->worm_commit_ctx) {
			flush_lin_list_log(&comp_ctx->worm_commit_log,
			    comp_ctx->worm_commit_ctx, &error);
			if (error) {
				goto out;
			}
		}
		//...and comp dir dels
		if (comp_ctx->worm_dir_dels_ctx) {
			flush_lin_list_log(&comp_ctx->worm_dir_dels_log,
			    comp_ctx->worm_dir_dels_ctx, &error);
			if (error) {
				goto out;
			}
		}

		if (global->doing_diff_sync && global->do_bulk_tw_lmap) {
			send_cached_file_ack_msgs(sw_ctx);
		}
		send_lmap_acks(sw_ctx, 0, 0, true);
		if (global->rep_mirror_ctx) {
			close_repstate(global->rep_mirror_ctx);
			global->rep_mirror_ctx = NULL;
			global->mirror_check = false;
		}
	}

	if (global->stf_sync &&
	    !(global->initial_sync || global->stf_upgrade_sync)) {
		m->body.done.count = 0;
	} else {
		finish_dir(sw_ctx, &error);
		if (error) {
			goto out;
		}
		send_stats(sw_ctx);
		m->body.done.count = sw_ctx->stats.dirs;
	}
	ilog(IL_INFO, "%s called, Send DONE_MSG to pworker", __func__);
	send_stats(sw_ctx);
	msg_send(global->primary, m); //sworker -> pworker  发送msg_done
	sw_ctx->stats.dirs = 0;

out:
	sw_process_fs_error(sw_ctx, SW_ERR_DIR, inprog->dstate.name, 
	    inprog->dstate.enc, inprog->dstate.src_dirlin, error);
	isi_error_free(error);
	return 0;
}

static void
get_job_version(struct migr_sworker_ctx *sw_ctx)
{
	int lock_fd = -1;
	char *policy_id = sw_ctx->global.sync_id;
	struct isi_error *error = NULL;
	struct target_record *trec = NULL;
	struct siq_job_version *job_ver, *prev_job_ver;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	/*
	 * XXX If the policy_id is NULL then the pworker is < OneFS 5.5
	 * and is almost certainly unsupported as of Riptide.
	 */
	if (policy_id == NULL) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "%s: Unsupported peer version", __func__);
	}

	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (error != NULL)
		log(FATAL, "siq_target_acquire_lock(%s): %s", policy_id,
		    isi_error_get_message(error));

	ASSERT(lock_fd != -1);

	trec = siq_target_record_read(policy_id, &error);
	if (error != NULL)
		log(FATAL, "siq_target_record_read(%s): %s", policy_id,
		    isi_error_get_message(error));

	close(lock_fd);

	/*
	 * If restoring and the most recent non-restore common job version is
	 * 0 (i.e. the last non-restore job succeeded) then use the previous
	 * non-restore job version for consistency; otherwise, use the the
	 * most recent non-restore job version.
	 */
	if (global->restore && trec->composite_job_ver.common == 0) {
		job_ver = &trec->prev_composite_job_ver;
	} else {
		job_ver = &trec->composite_job_ver;
	}

	/*
	 * The common version resulting from the pworker/sworker handshake
	 * ought to match that of the coordinator/tmonitor handshake.
	 * If they don't match then either:
	 *   1) sworker stale: the job terminated successfully (job_ver={0,0}),
	 *      or
	 *   2) sworker stale: the job terminated successfully, an upgrade was
	 *      committed on either or both of the source or target, and a new
	 *      job was started (job_ver={>0,>0}, non-matching run_id), or
	 *   3) a fatal versioning error occured.
	 *
	 * XXX Regarding 2), the target_monitor() routine presently updates
	 * the target record prior to creating/updating the syncrun file,
	 * so a narrow race condition does exist.
	 */
	if (global->job_ver.common != job_ver->common) {
		if (job_ver->common != 0) {
			verify_run_id(sw_ctx, &error);
		}

		if (job_ver->common == 0 || error != NULL) {
			exit(EXIT_SUCCESS);
		}

		ASSERT(global->job_ver.common == job_ver->common);
	}
	prev_job_ver = &trec->prev_composite_job_ver;

	/* Retrieve the other info written by the tmonitor. */
	global->job_ver.local = job_ver->local;
	global->local_job_ver_update =
	    (prev_job_ver->local < job_ver->local && prev_job_ver->local > 0);
	global->prev_local_job_ver = prev_job_ver->local;

	log_job_version_ufp(&global->job_ver, SIQ_SWORKER, __func__);

	siq_target_record_free(trec);
}

/*
 * Sets global->failover to true iff this is a failover job
 * (excludes failover revert).
 */
static void
check_failover(struct migr_sworker_global_ctx *global,
    struct isi_error **error_out)
{
	char *policy_id = global->sync_id;
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;

	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (lock_fd == -1)
		goto out;

	trec = siq_target_record_read(policy_id, &error);
	if (error)
		goto out;

	global->failover =
	    (trec->fofb_state == FOFB_FAILOVER_STARTED);

out:
	siq_target_record_free(trec);

	if (lock_fd != -1)
		close(lock_fd);

	isi_error_handle(error, error_out);

}

static void
work_init_msg_unpack(struct generic_msg *m, unsigned source_msg_version,
    struct work_init_msg *ret, struct isi_error **error_out)
{
	struct old_work_init_msg6 *wim = &m->body.old_work_init6;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (source_msg_version < MSG_VERSION_RIPTIDE) {
		ret->target = wim->target;
		ret->peer = wim->peer;
		ret->source_base = wim->source_base;
		ret->target_base = wim->target_base;
		ret->bwhost = wim->bwhost;
		ret->predicate = wim->predicate;
		ret->sync_id = wim->sync_id;
		ret->run_id = wim->run_id;
		ret->lastrun_time = wim->lastrun_time;
		ret->flags = wim->flags;
		ret->pass = wim->pass;
		ret->job_name = wim->job_name;
		ret->loglevel = wim->loglevel;
		ret->action = wim->action;
		ret->old_time = wim->old_time;
		ret->new_time = wim->new_time;
		ret->restrict_by = wim->restrict_by;
		ret->hash_type = wim->hash_type;
		ret->min_hash_piece_len = wim->min_hash_piece_len;
		ret->hash_pieces_per_file = wim->hash_pieces_per_file;
		ret->recovery_dir = wim->recovery_dir;
		ret->skip_bb_hash = wim->skip_bb_hash;
		ret->worker_id = wim->worker_id;
		ret->prev_snapid = wim->prev_snapid;
		ret->cur_snapid = wim->cur_snapid;
		ret->domain_id = wim->domain_id;
		ret->domain_generation = wim->domain_generation;
	} else {
		RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET, 1, &ret->target,
		    &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_PEER, 1, &ret->peer, &error,
		    out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_SOURCE_BASE, 1,
		    &ret->source_base, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET_BASE, 1,
		    &ret->target_base, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_BWHOST, 1, &ret->bwhost,
		    &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_PREDICATE, 1,
		    &ret->predicate, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_SYNC_ID, 1, &ret->sync_id,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_RUN_ID, 1, &ret->run_id,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_LASTRUN_TIME, 1,
		    &ret->lastrun_time, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FLAGS, 1, &ret->flags,
		    &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_PASS, 1, &ret->pass, &error,
		    out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_JOB_NAME, 1, &ret->job_name,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_LOGLEVEL, 1,
		    &ret->loglevel, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_ACTION, 1, &ret->action,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_OLD_TIME, 1,
		    &ret->old_time, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_NEW_TIME, 1,
		    &ret->new_time, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_RESTRICT_BY, 1,
		    &ret->restrict_by, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_HASH_TYPE, 1,
		    &ret->hash_type, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_MIN_HASH_PIECE_LEN,
		    1, &ret->min_hash_piece_len, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint32, ript,
		    RMF_HASH_PIECES_PER_FILE, 1, &ret->hash_pieces_per_file,
		    &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_RECOVERY_DIR, 1,
		    &ret->recovery_dir, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_SKIP_BB_HASH, 1,
		    &ret->skip_bb_hash, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_WORKER_ID, 1,
		    &ret->worker_id, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_PREV_SNAPID, 1,
		    &ret->prev_snapid, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_CUR_SNAPID, 1,
		    &ret->cur_snapid, &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_DOMAIN_ID, 1,
		    &ret->domain_id, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DOMAIN_GENERATION, 1,
		    &ret->domain_generation, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_DEEP_COPY, 1,
		    &ret->deep_copy, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_PEER_GUID, 1,
		    &ret->peer_guid, &error, out);
		ript_msg_get_field_bytestream(ript, RMF_CBM_FILE_VERSIONS,
		    1, &ret->common_cbm_vers_buf,
		    &ret->common_cbm_vers_buf_len, &error);
		if (error != NULL)
			goto out;

		RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_RUN_NUMBER, 1,
		    &ret->run_number, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_WI_LOCK_MOD, 1,
		    &ret->wi_lock_mod, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

int
work_init_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	FILE *f = NULL;
	struct generic_msg lin_map_blk_msg = {};
	struct generic_msg auth_msg = {};
	struct work_init_msg wim = {};
	char _pass[1024 + sizeof(int)] = {0}, *pass = _pass + sizeof(int);
	char proctitle[100];
	struct isi_error *isierror = NULL;
	int res;
	struct procoptions po = PROCOPTIONS_INIT;
	char lmap_name[MAXNAMELEN];
	char comp_map_name[MAXNAMELEN];
	char resync_name[MAXNAMELEN + 1];
	char *job_name = NULL;
	bool found = false;

	work_init_msg_unpack(m, global->job_ver.common, &wim, &isierror);
	if (isierror != NULL) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "%s", isi_error_get_message(isierror));
	}

	global->loglevel = wim.loglevel;
	set_siq_log_level(global->loglevel);

	/*
	 * Bug 270757 and PSCALE-13515
	 * target should not be /ifs/.ifsvar or /ifs/.snapshot
	 * and all their sub-dirs
	 * also cannot be purely /ifs (even /ifs/)
	 */
	if (strncmp(wim.target_base, "/ifs/.ifsvar", 12) == 0 ||
	    strncmp(wim.target_base, "/ifs/.snapshot", 14) == 0 ||
	    !passed_further_check(wim.target_base)) {
	        // Stop retries and fail this job.
	        send_err_msg(EINVAL, sw_ctx);
	        log(FATAL, "target_base illegal: %s.", wim.target_base);
	}

	/* Set ILOG job name to "policy name":"run id" */
	asprintf(&job_name, "%s:%llu", wim.job_name, wim.run_id);
	ilog_running_app_job_set(job_name);

	/* Change process title to include the source cluster and policy
	 * names */
	snprintf(global->src_cluster_name, sizeof(global->src_cluster_name),
	    "%s", wim.peer);

	cluster_name_from_node_name(global->src_cluster_name);
	snprintf(proctitle, sizeof(proctitle), "%s.%s",
	    global->src_cluster_name, wim.job_name);
	setproctitle(proctitle);
	po.po_isi_flag_on = PI_OBEY_VHS_LIMIT;
	/* Bug 162782 - ignore PI_AV_COMPLY */
	po.po_isi_flag_off |= PI_AV_COMPLY;
	if (setprocoptions(&po))
		log(ERROR, "Error setting proc flags. po_isi_flag_on: %x "
		    "po_isi_flag_off: %x", po.po_isi_flag_on,
		    po.po_isi_flag_off);

	ilog(IL_INFO, "%s called, Starting worker %d on '%s' from %s", __func__,
	    wim.worker_id, wim.job_name,
	    inx_addr_to_str(&g_src_addr));
	global->worker_id = wim.worker_id;
	if (global->job_name) {
		free(global->job_name);
	}
	global->job_name = strdup(wim.job_name);

	/* Reload config, and verify we're licensed and up */
	if (global_config)
		siq_gc_conf_free(global_config);

	global_config = siq_gc_conf_load(&isierror);
	if (isierror) {
		send_err_msg(ENOTSUP, sw_ctx);
		log(FATAL, "Failed to load global configuration %s %s",
		    (isierror?":":""),
		    (isierror?isi_error_get_message(isierror):""));
	}
	siq_gc_conf_close(global_config);

	if (global_config->root->application->max_sworkers_per_node <= 0)
		global_config->root->application->max_sworkers_per_node = 100;

	/* Bug 116114 - read in flip_lin_verify config */
	global->flip_lin_verify =
		global_config->root->application->flip_lin_verify;

	ilog(IL_INFO, "%s called, Action %d", __func__, wim.action);

	if (!SIQ_LIC_OK(wim.action)) {
		/* if target is localhost, ignore license check. local
		 * jobs like DomainMark and SnapRevert must be able to run
		 * regardless of SyncIQ license status */
		if (strcmp(wim.target, "localhost") == 0) {
			log(DEBUG,
			    "ignoring sworker license check for local job");
		} else {
			send_err_msg(ENOTSUP, sw_ctx);
			log(FATAL, "Unable to process policy '%s' from '%s': "
			    "insufficient licensing", wim.job_name,
		    	    inx_addr_to_str(&g_src_addr));
		}
	}
	global->action = wim.action;

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

	/* Only OneFS 5.5+ will have sync_id set */
	if (wim.sync_id) {
		global->sync_id = strdup(wim.sync_id);

		if (wim.flags & FLAG_RESTORE) {
			check_failover(global, &isierror);
			if (isierror) {
				log(FATAL,
				    "Failed to get the failover state: %s",
				    isi_error_get_message(isierror));
			}
		}
	} else
		global->sync_id = NULL;

	global->run_id = wim.run_id;

	get_job_version(sw_ctx);

	/* Set up some fields based on the set parameters. */
	path_init(&global->source_base_path, wim.source_base, NULL);
	global->source_base_path_len = strlen(wim.source_base) + 1;
	path_init(&global->target_base_path, wim.target_base, NULL);

	global->skip_bb_hash = wim.skip_bb_hash;

	global->checksum_size = (wim.flags & FLAG_NO_INTEGRITY)
	    ? 0 : CHECKSUM_SIZE;

	global->logdeleted = (bool)(wim.flags & FLAG_LOGDELETES);

	global->stf_sync = (bool)(wim.flags & FLAG_STF_SYNC);
	global->initial_sync = (bool)(wim.flags & FLAG_CLEAR);
	global->restore = (bool)(wim.flags & FLAG_RESTORE);
	global->curr_ver = (wim.flags & FLAG_VER_3_5);
	global->curr_ver |= (wim.flags & FLAG_VER_CLOUDPOOLS);
	global->compliance = (bool)(wim.flags & FLAG_COMPLIANCE);

	/* We do not need to check for the secondary also being v2 since
	 * the sync would have already failed we were syncing from v2 -> <v2.*/
	global->compliance_v2 = (bool)(wim.flags & FLAG_COMPLIANCE_V2);

	global->is_fake_compliance = false;

	/* Starting with Riptide, use job_ver, not curr_ver. */
	ASSERT((global->curr_ver & ~(FLAG_VER_3_5 | FLAG_VER_CLOUDPOOLS)) == 0);

	/* sanity check: stf upgrade sync cannot be an initial sync, and must
	 * be a stf sync */
	global->stf_upgrade_sync = (bool)(wim.flags & FLAG_STF_UPGRADE_SYNC);
	if (global->stf_upgrade_sync) {
		log(DEBUG, "STF upgrade sync");
		ASSERT(global->stf_sync);
		ASSERT(!global->initial_sync);
	}

	if (global->logdeleted)
		log(TRACE, "Log deleted files on destination cluster");

	/*
	 * All pre-halfpipe clusters will not have peer_guid in the
	 * work_init_msg sent by the pworker. In that case, we use the
	 * current cluster guid as the peer guid
	 */
	if (wim.peer_guid && global->compliance_v2) {
		snprintf(global->comp_cluster_guid,
		    sizeof(global->comp_cluster_guid), "%s", wim.peer_guid);
		global->is_src_guid = true;
	} else {
		snprintf(global->comp_cluster_guid,
		    sizeof(global->comp_cluster_guid), "%s",
		    get_cluster_guid());
		global->is_src_guid = false;
	}

	init_statistics(&sw_ctx->stats.tw_cur_stats);
	init_statistics(&sw_ctx->stats.tw_last_stats);
	init_statistics(&sw_ctx->stats.stf_cur_stats);
	init_statistics(&sw_ctx->stats.stf_last_stats);

	/* Get LNN and run number. Initialize compliance conflict logging if
	 * needed. */
	global->run_number = wim.run_number;
	global->lnn = get_local_lnn(&isierror);
	if (isierror)
		log(FATAL, "Failed to get local lnn: %s",
		    isi_error_get_message(isierror));

	if (wim.flags & FLAG_LOG_CONFLICTS)
		init_comp_conflict_log(global->sync_id, global->run_number,
		    global->lnn);

	/* Check if linmap updates are requested by source */
	if (wim.flags & FLAG_LIN_MAP_BLK) {
		global->lin_map_blk = !!(wim.flags & FLAG_LIN_MAP_BLK);
		lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
		msg_send(global->primary, &lin_map_blk_msg);
	}

	/* If burst socket usage requested , check if its possible */
	if (wim.flags & FLAG_BURST_SOCK) {
		wait_for_burst_connect(sw_ctx, &isierror);
		if (isierror) {
			log(FATAL, "burst socket connection error: %s",
			    isi_error_get_message(isierror));
		}
	}
	/* Attempt to authenticate. */
	auth_msg.body.auth.salt = (int)time(0);
	/* Bug 178393 - ignore passwd file during recovery or domain mark */
	if ((wim.flags & (FLAG_FAILBACK_PREP | FLAG_RESTORE |
	    FLAG_DOMAIN_MARK)) == 0)
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
	msg_send(global->primary, &auth_msg); /////sworker -> pworker  发送auth_msg

	/* Get us started in the correct directory. */
	path_init(&worker->cur_path, wim.target_base, 0);

	memcpy(_pass, &auth_msg.body.auth.salt, sizeof(int));
	global->secret = MD5Data(_pass, sizeof(int) + 
	    strlen(_pass + sizeof(int)), 0);
	ilog(IL_INFO, "%s called, Generated secret of %s", global->secret, __func__);

	inprog->result = ACK_RES_OK;
	sw_ctx->stats.io_errors = 0;
	sw_ctx->stats.checksum_errors = 0;

	global->lmap_ctx = NULL;
	global->comp_map_ctx = NULL;
	global->resync_ctx = NULL;
	/* Bug 128906 - open linmap for both stf and non stf sync's.
	 * non stf_sync's uses linmap for STF_PHASE_CT_LIN_HASH_EXCPT phase
	 * Bug 133753 - do not open linmap for failback prep jobs UNLESS
	 * FLAG_STF_SYNC is set.
	 */
	if (global->stf_sync || !(bool)(wim.flags & FLAG_FAILBACK_PREP)) {
		get_lmap_name(lmap_name, wim.sync_id, global->restore);
		open_linmap(wim.sync_id, lmap_name, &global->lmap_ctx,
		    &isierror);
		if (isierror)
			log(FATAL, "Error opening linmap %s: %s",
			    lmap_name, isi_error_get_message(isierror));

		if (global->compliance_v2) {
			/* Open compliance map. */
			get_compliance_map_name(comp_map_name,
			    sizeof(comp_map_name), wim.sync_id, false);
			found = compliance_map_exists(wim.sync_id,
			    comp_map_name, &isierror);
			if (isierror)
				log(FATAL, "Error while checking the"
				    "existence of compliance map %s:%s",
				    comp_map_name,
				    isi_error_get_message(isierror));
			if (found) {
				open_compliance_map(wim.sync_id, comp_map_name,
				    &global->comp_map_ctx, &isierror);
				if (isierror)
					log(FATAL, "Error while opening the"
					    "compliance map %s:%s",
					    comp_map_name,
					    isi_error_get_message(isierror));
				set_comp_map_lookup(global->lmap_ctx,
				    global->comp_map_ctx);
			}
		}

		if (global->restore) {
			set_dynamic_lmap_lookup(global->lmap_ctx,
			    wim.domain_id);
			/* XXX Also need to use different lmap name here */
		} else if (global->compliance_v2) {
			get_resync_name(resync_name, sizeof(resync_name),
			    wim.sync_id, false);
			open_lin_list(wim.sync_id, resync_name,
			    &global->resync_ctx, &isierror);
			if (isierror)
				log(FATAL, "Error while opening the"
				    "resync list %s:%s",
				    resync_name,
				    isi_error_get_message(isierror));
		}
	}

	if (wim.hash_type != HASH_NONE) {
		global->doing_diff_sync = true;
		log(TRACE, "%s: Running diff sync. hash type=%d",
		    __func__, wim.hash_type);
		global->hash_type = wim.hash_type;
		if (global->hasher == 0) {
			log(DEBUG, "%s: Starting hasher", __func__);
			global->hasher = fork_hasher(SWORKER, sw_ctx, -1,
			    wim.hash_type, wim.min_hash_piece_len,
			    wim.hash_pieces_per_file, global->loglevel,
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
	if (!(wim.flags & FLAG_HAS_TARGET_AWARE)) {
		/* Since the source is older, it is sending a pre-Riptide msg
		 * so we can read that directly into the handling function. */
		handle_non_target_aware_source(&m->body.old_work_init6,
		    global);
	}
	
	/* Set domain_id and domain_generation */
	global->domain_id = wim.domain_id;
	global->domain_generation = wim.domain_generation;
	ilog(IL_INFO, "%s: Using domain %lld %d", __func__,
	    wim.domain_id, wim.domain_generation);
	if (global->domain_id != 0) {
		ilog(IL_INFO, "%s: Setting ifs_domain_allowwrite %lld %d",
		     __func__, wim.domain_id, wim.domain_generation);
		res = ifs_domain_allowwrite(global->domain_id, 
		    global->domain_generation);
		if (res != 0) {
			/* TODO Once this is working, we should probably fail */
			log(NOTICE, "Failed to set domain allow write (%d)", 
			    errno);
		}
	}

	/* Get the compliance domain version for this sworker. */
	global->compliance_dom_ver = get_compliance_dom_ver(
	    wim.target_base, &isierror);
	if (isierror) {
		if (!isi_system_error_is_a(isierror, ENOENT)) {
			log(FATAL, "Failed to get compliance domain version of "
			    "target_dir: %s", isi_error_get_message(isierror));
		}

		isi_error_free(isierror);
		isierror = NULL;
		global->compliance_dom_ver = 0;
	}

	//If we are doing compliance v2 sync, register the callbacks
	if (global->compliance_v2) {
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_COMMIT_BEGIN_MSG),
		    comp_commit_begin_callback);
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG),
		    comp_target_work_range_callback);
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG),
		    comp_target_checkpoint_callback);
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_MAP_BEGIN_MSG),
		    comp_map_begin_callback);
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_MAP_MSG),
		    comp_map_callback);
		migr_register_callback(global->primary,
		    build_ript_msg_type(COMP_DIR_DELS_BEGIN_MSG),
		    comp_dir_dels_begin_callback);
		sw_ctx->comp.comp_target_min_lin = INVALID_LIN;
		sw_ctx->comp.comp_target_max_lin = INVALID_LIN;
		sw_ctx->comp.comp_target_current_lin = INVALID_LIN;
		sw_ctx->comp.comp_map_chkpt_lin = INVALID_LIN;
	}

	global->wi_lock_mod = wim.wi_lock_mod;
	global->wi_lock_timeout =
	    global_config->root->application->work_item_lock_timeout;

	timer_callback(sw_ctx);
	ifs_setnoatime(true); /* Ignore errors, if any */
	cpu_throttle_start(&sw_ctx->cpu_throttle);
	isi_error_free(isierror);
	if (job_name)
		free(job_name);
	return 0;
}

static int
auth_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx->worker;
	struct generic_msg err;
	struct isi_error *error = NULL;

	ilog(IL_INFO, "auth_callback called");
	err.head.type = ERROR_MSG;
	err.body.error.io = 0;
	err.body.error.error = EAUTH;
	err.body.error.str = 0;
	if (!global->secret) {
		msg_send(global->primary, &err);
		log(FATAL, "Out of order authentication attempt received");
		return -1; /* Placate static analysis. */
	}
	if (strncmp(global->secret, m->body.auth.pass,
	    strlen(global->secret))) {
		msg_send(global->primary, &err);
		log(FATAL, "Primary authentication failed");
	}
	free(global->secret);
	global->secret = 0;

	m->body.auth.salt = 0;
	m->body.auth.pass = 0;
	msg_send(global->primary, m); ////sworker -> pworker 发送auth_msg

	ASSERT(worker->cwd_fd == -1);

	if (smkchdirfd(&worker->cur_path, AT_FDCWD, &worker->cwd_fd)) {
		dump_exitcode_ring(errno);
		ioerror(global->primary, worker->cur_path.path, NULL, 0, 0,
		    "Failed to establish target dir %s", worker->cur_path.path);
		/* Tell coordinator that the job is doomed, then die. */
		memset(m, 0, sizeof *m);
		m->head.type = ERROR_MSG;
		m->body.error.error = ENOTDIR;
		msg_send(global->primary, m);
		log(FATAL, "Failure to mkdir is unrecoverable");
	}

	ASSERT(worker->cwd_fd >= 0);
	
	if (sw_ctx->global.stf_sync) {
		add_to_domain(worker->cwd_fd, global->domain_id, &error);
		if (error) {
			ioerror(global->primary, worker->cur_path.path, NULL, 
			    0, 0, "Failed to add target dir %s to domain %llu",
			    worker->cur_path.path, global->domain_id);
			/* Tell coordinator that the job is doomed, then die. */
			memset(m, 0, sizeof *m);
			m->head.type = ERROR_MSG;
			m->body.error.error = EDOM;
			msg_send(global->primary, m);
			log(FATAL, "Failure to mkdir is unrecoverable");
		}
	}

	if (sw_ctx->global.stf_sync && !sw_ctx->global.initial_sync) {
		initialize_tmp_map(worker->cur_path.path,
		    sw_ctx->global.sync_id, global,
		    global->restore, false, &error);
		if (error) {
			ioerror(global->primary, worker->cur_path.path, NULL,
			    0, 0, "Failed to setup tmp directory map. %s",
			    isi_error_get_message(error));
			memset(m, 0, sizeof *m);
			m->head.type = ERROR_MSG;
			m->body.error.error = ENOTDIR;
			msg_send(global->primary, m);
			log(FATAL, "Failed to setup tmp directory map. %s",
			    isi_error_get_message(error));

			goto out;
		}

		/*
		 * We dont require to keep cwd_fd since stf repeated sync 
		 * sets this as part of every lin_update_msg.
		 */
		close(worker->cwd_fd);
		worker->cwd_fd = -1;
	}

out:


	return 0;
}

void
target_path_confirm(struct target_records *tdb, const char *policy_id, 
    const char *policy, const char *src_cluster, const char *target_dir, 
    unsigned int flags, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct target_record *trec;

	/* Now, check for target path overlap with any other policy */
	for (trec = tdb->record; trec; trec = trec->next) {
		UFAIL_POINT_CODE(target_path_overlap,
		    target_dir = trec->target_dir);
		if (strcmp(trec->id, policy_id) == 0) {
			/* Skip checking our own record */
			continue;
		}
		if (strcmp(trec->target_dir, "") == 0) {
			/* Skip checking empty directories */
			continue;
		}
		if (path_overlap(trec->target_dir, target_dir) != NO_OVERLAP) {
			/* Overlapping directories has occurred */
			if (trec->allow_target_dir_overlap && 
			    !(flags & FLAG_STF_SYNC) && 
			    !(flags & FLAG_STF_UPGRADE_SYNC)) {
			        /* Allow target dir overlap flag only works with 
			         * treewalk based syncs */
			        log(DEBUG, "%s: Policy %s from %s overlaps with "
			            "%s from %s but has "
			            "allow_target_dir_overlap set", __func__, 
			            trec->policy_name, trec->src_cluster_name,
			            policy, src_cluster);
			        continue;
			} else {
				error = isi_siq_error_new(E_SIQ_TGT_PATH_OLAP,
				    "Target path overlap. Previous policy %s from %s "
				    "has path %s. New policy %s from %s has path %s",
				    trec->policy_name, trec->src_cluster_name,
				    trec->target_dir, policy, src_cluster, target_dir);
				break;
			}
		}
	}

	isi_error_handle(error, error_out);
}

static struct isi_error *
error_guid_mismatch(const char *target, const char *policy_name)
{
	return isi_siq_error_new(E_SIQ_TGT_ASSOC_BRK,
		"For target cluster %s, it appears that the policy %s has "
		"been run before, but target association has been broken. "
		"To run the policy again (requiring a full sync), the reset "
		"command must be run on policy %s on the source cluster",
		target, policy_name, policy_name);
}

void
target_confirm(struct migr_sworker_ctx *sw_ctx,
    struct target_init_msg *tim, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct target_records *tdb = NULL;
	struct target_record *trec = NULL;
	char *policy_id_5_0 = NULL;
	char UFAIL_TEST_BAD_ID[] = "UFAIL_TEST_BAD_ID";

	siq_target_records_read(&tdb, &error);
	if (error)
		goto out;

	trec = siq_target_record_find_by_id(tdb, tim->policy_id);

	if (trec) {
		ilog(IL_INFO, "%s: Policy ID has been seen before", __func__);
		UFAIL_POINT_CODE(source_guid_mismatch,
		    tim->src_cluster_id = UFAIL_TEST_BAD_ID);
		UFAIL_POINT_CODE(target_guid_mismatch,
		    tim->last_target_cluster_id = UFAIL_TEST_BAD_ID);
		if (trec->cant_cancel) {
			log(DEBUG, "%s: Previous run of policy %s / %s from "
			    "source cluster %s with no target awareness. "
			    "Source cluster now has target awareness",
			    __func__, tim->policy_name, tim->policy_id,
			    tim->src_cluster_name);
		} else if (strcmp(trec->src_cluster_id,
		    tim->src_cluster_id) != 0) {
			error = isi_siq_error_new(E_SIQ_TGT_MISMATCH,
			    "Mismatched ID detected for source cluster %s - "
			    "expected ID %s but got %s",
			    tim->src_cluster_name, tim->src_cluster_id,
			    trec->src_cluster_id);
			goto out;
		} else if (strcmp(tim->last_target_cluster_id, "") == 0) {
			/* An empty target guid from the source suggests that 
			 * a full sync is incoming.  Just reuse this record. */
		} else if (strcmp(get_cluster_guid(),
		    tim->last_target_cluster_id) != 0) {
			log(ERROR, "For target cluster %s, mismatched ID. "
			    "Expected ID %s but got %s",
			    sw_ctx->global.cluster_name, get_cluster_guid(),
			    tim->last_target_cluster_id);
			error = error_guid_mismatch(sw_ctx->global.cluster_name,
			    tim->policy_name);
			goto out;
		}
	}

	/* First time this policy has been initiated, or the policy has run
	 * previously when the source cluster was 5.0 and has since been
	 * upgraded (5.0 TDB entries do not have the policy_id as a 5.0 source
	 * doesn't send it to the target) */
	else {
		/* First check for 5.0 policy */
		policy_id_5_0 = make_5_0_policy_id(tim->src_cluster_name,
		    tim->policy_name, tim->target_dir);
		if (siq_target_record_exists(policy_id_5_0)) {
			log(DEBUG, "%s: Previous run of policy %s / %s "
			    "from 5.0 source cluster %s with no target "
			    "awareness. Source cluster now has target "
			    "awareness",
			    __func__, tim->policy_name, tim->policy_id,
			    tim->src_cluster_name);

			goto out;
		}

		if (strcmp(tim->last_target_cluster_id, "") != 0) {
			log(ERROR,
			    "For target cluster %s, mismatched ID. "
			    "Expected empty GUID but got %s",
			    sw_ctx->global.cluster_name,
			    tim->last_target_cluster_id);
			error = error_guid_mismatch(sw_ctx->global.cluster_name,
			    tim->policy_name);
			goto out;
		}
	}

	/* Always check for target path overlap and disallow */
	target_path_confirm(tdb, tim->policy_id, tim->policy_name,
	    tim->src_cluster_name, tim->target_dir, tim->flags, &error);
	if (error)
		goto out;

	/*
	 * If this is a self-sync, the source root path and target root path
	 * can't overlap.
	 */
	if (!(tim->flags & FLAG_RESTORE) && 
	    strcmp(tim->src_cluster_id, get_cluster_guid()) == 0) {
		log(TRACE, "%s: This is a self-sync", __func__);
		if (path_overlap(tim->src_root_dir, tim->target_dir)) {
			error = isi_siq_error_new(E_SIQ_SELF_PATH_OLAP,
			    "Policy %s syncs to local cluster and target path "
			    "%s overlaps source base path %s",
			    tim->policy_name, tim->target_dir,
			    tim->src_root_dir);
			goto out;
		}
	}

out:
	siq_target_records_free(tdb);
	free(policy_id_5_0);
	isi_error_handle(error, error_out);
}

void
send_error_msg_and_exit(int fd, int error_number, const char *err_msg)
{
	struct generic_msg reply_msg = {};

	log(ERROR, "%s. Exiting.", err_msg);
	reply_msg.head.type = ERROR_MSG;
	/* strdup() because of const */
	reply_msg.body.error.str = strdup(err_msg);
	reply_msg.body.error.error = error_number;
	msg_send(fd, &reply_msg);
	exit(EXIT_SUCCESS);
}


/* const for err_msg as needed for passed in isi_error_get_text() */
void
send_error_and_exit(int fd, struct isi_error *error)
{
	send_error_msg_and_exit(fd, 0, isi_error_get_message(error));
}

static ifs_lin_t
get_target_compliance_store_lin(char *target_dir, struct isi_error **error_out)
{
	char *path = NULL;
	int res;
	ifs_lin_t ret = INVALID_LIN;
	struct stat st;
	struct isi_error *error = NULL;

	/* Get the path to the target guid directory in the compliance store. */
	asprintf(&path, "%s/%s/%s", target_dir,
	    COMPLIANCE_STORE_ROOT_NAME, get_cluster_guid());
	ASSERT(path);

	res = lstat(path, &st);
	if (res) {
		error = isi_system_error_new(errno, "Failed to stat "
		    "the target compliance store: %s", path);
		goto out;
	}

	ret = st.st_ino;
	log(NOTICE, "Found compliance store: %s", path);

out:
	if (path)
		free(path);

	isi_error_handle(error, error_out);
	return ret;
}

static void
add_target_compliance_store_for_resync(char *target_dir, char *policy_id,
    struct isi_error **error_out)
{
	char resync_name[MAXNAMLEN];
	ifs_lin_t guid_lin;
	struct lin_list_ctx *ctx = NULL;
	struct lin_list_info_entry llie = {};
	struct lin_list_entry ent = {};
	struct isi_error *error = NULL;

	UFAIL_POINT_CODE(skip_compliance_target_guid,
		goto out;
	);

	get_resync_name(resync_name, sizeof(resync_name), policy_id, false);
	create_lin_list(policy_id, resync_name, true, &error);
	if (error)
		goto out;

	open_lin_list(policy_id, resync_name, &ctx, &error);
	if (error)
		goto out;

	llie.type = RESYNC;
	set_lin_list_info(&llie, ctx, false, &error);
	if (error)
		goto out;

	guid_lin = get_target_compliance_store_lin(target_dir, &error);
	if (error)
		goto out;

	/* Add the resync entry.
	 * This shouldn't matter but we add the src guid dir when creating a
	 * mirror policy. */
	log(NOTICE, "Adding %llx to the target resync list", guid_lin);
	ent.is_dir = true;
	set_lin_list_entry(guid_lin, ent, ctx, &error);
	if (error)
		goto out;
out:
	if (ctx)
		close_lin_list(ctx);

	isi_error_handle(error, error_out);
}

static void
set_resp_error(struct generic_msg *resp, struct isi_error *error,
    uint32_t ver)
{
	struct siq_ript_msg *ript_resp = &resp->body.ript;
	int siq_error = -1;

	siq_error = isi_siq_error_get_siqerr((struct isi_siq_error *)error);

	/* sending a target response with an error causes the coord
	 * to make the job unrunnable */
	if (ver < MSG_VERSION_CHOPU) {
		resp->body.old_target_resp.error =
		    (char *)isi_error_get_message(error);
	} else if (ver < MSG_VERSION_RIPTIDE) {
		resp->body.old_target_resp2.error =
		    (char *)isi_error_get_message(error);
		/* for bug 75403. necessary hack to communicate the
		 * error type to the source without causing existing
		 * coord builds to break. we can't send a siq_error_msg
		 * since coord's don't expect them at this point. the
		 * best we can do is stuff the siqerr value into a
		 * field of the target_resp_msg and let newer coords
		 * interpret it when error handling */
		resp->body.old_target_resp2.domain_generation =
		    (siq_error != -1) ? siq_error : 0;
	} else {
		ript_msg_set_field_str(ript_resp, RMF_ERROR, 1,
		    (char *)isi_error_get_message(error));
		ript_msg_set_field_uint32(ript_resp, RMF_DOMAIN_GENERATION, 1,
		    (siq_error != -1) ? siq_error : 0);
	}

	log(ERROR, "%s", isi_error_get_message(error));
}

static void
target_init_msg_unpack(struct generic_msg *m, unsigned source_msg_version,
    struct target_init_msg *ret, struct isi_error **error_out)
{
	struct old_target_init_msg *tim = &m->body.old_target_init;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (source_msg_version < MSG_VERSION_RIPTIDE) {
		if (m->head.type != OLD_TARGET_INIT_MSG) {
			log(FATAL,
			    "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->restricted_target_name = tim->restricted_target_name;
		ret->policy_id = tim->policy_id;
		ret->policy_name = tim->policy_name;
		ret->target_dir = tim->target_dir;
		ret->src_root_dir = tim->src_root_dir;
		ret->src_cluster_name = tim->src_cluster_name;
		ret->src_cluster_id = tim->src_cluster_id;
		ret->last_target_cluster_id = tim->last_target_cluster_id;
		ret->option = tim->option;
		ret->loglevel = tim->loglevel;
		ret->flags = tim->flags;
		ret->run_id = tim->run_id;
	} else {
		if (m->head.type !=
		    build_ript_msg_type(TARGET_INIT_MSG)) {
			log(FATAL,
			    "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		RIPT_MSG_GET_FIELD(str, ript, RMF_RESTRICTED_TARGET_NAME, 1,
		    &ret->restricted_target_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_POLICY_ID, 1,
		    &ret->policy_id, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_POLICY_NAME, 1,
		    &ret->policy_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET_DIR, 1,
		    &ret->target_dir, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_SRC_ROOT_DIR, 1,
		    &ret->src_root_dir, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_SRC_CLUSTER_NAME, 1,
		    &ret->src_cluster_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_SRC_CLUSTER_ID, 1,
		    &ret->src_cluster_id, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_LAST_TARGET_CLUSTER_ID, 1,
		    &ret->last_target_cluster_id, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_OPTION, 1, &ret->option,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_LOGLEVEL, 1,
		    &ret->loglevel, &error, out);
		RIPT_MSG_GET_FIELD(uint32, ript, RMF_FLAGS, 1, &ret->flags,
		    &error, out);
		RIPT_MSG_GET_FIELD(uint64, ript, RMF_RUN_ID, 1, &ret->run_id,
		    &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_WI_LOCK_MOD, 1,
		    &ret->wi_lock_mod, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint8, ript,
		    RMF_COMP_LATE_STAGE_UNLINK_PATCH, 1,
		    &ret->comp_late_stage_unlink_patch, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * - TARGET_INIT_MSG:
 *     = From source cluster running OneFS 6.0 / SIQ 2.6.
 *     = Respond with the list of IPs within TARGET_RESP_MSG reply to the
 *       source.
 *     = ERR_MSG reply can be sent.
 *     = Source can request target_restrict.
 *     = Confirms that any previously unseen policy doesn't conflict with a
 *       previous policy run on the target cluster
 *     = Option field in message determines if only IPs are returned, or if
 *       the target_monitor is started as well.
 */

static int
target_init_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_tmonitor_ctx *tctx = &sw_ctx->tmonitor;
	struct generic_msg GEN_MSG_INIT_CLEAN(resp);
	struct generic_msg GEN_MSG_INIT_CLEAN(ver_msg);
	struct target_init_msg tinit = {};
	struct siq_ript_msg *ript_resp = NULL;
	struct nodex *nodes = NULL;
	struct isi_cbm_file_versions *tgt_cbm_vers = NULL;
	struct isi_error *error = NULL;
	int siqerr = -1;
	char *msg = NULL;
	enum target_init_option option = OPTION_INVALID;
	bool make_unrunnable = false;
	bool is_compliance;
	unsigned ipslen = 0;
	char *lnn_ipx_list = NULL;
	char *job_name = NULL;
	uint8_t *tgt_cbm_vers_buf = NULL;
	size_t tgt_cbm_vers_buf_len = 64;

	ASSERT(ilog_running_app_choose("siq_sworker_tmonitor") != -1);

	target_init_msg_unpack(m, global->job_ver.common, &tinit, &error);
	if (error != NULL)
		goto out;

	set_siq_log_level(tinit.loglevel);

	/* Set ILOG job name to "policy name":"run id" */
	asprintf(&job_name, "%s:%llu", tinit.policy_name, tinit.run_id);
	ilog_running_app_job_set(job_name);

	log(DEBUG, "%s: Source cluster is running OneFS 6.0 or later",
	    __func__);
	option = tinit.option;

	/* 
	 * OPTION_INIT indicates that a job is about to be run, but it first
	 * needs confirmation that the policy is specified correctly and
	 * doesn't conflict with any previous runs of this, or any other
	 * policy.
	 * XXX
	 * Restore jobs are always local, hence for now we dont need target
	 * confirmation. But we need to check in future.
	 */

	if (option == OPTION_INIT && !(tinit.flags & FLAG_RESTORE) &&
	    !(tinit.flags & FLAG_FAILBACK_PREP)) {
		log(TRACE, "%s: OPTION_INIT", __func__);
		target_confirm(sw_ctx, &tinit, &error);
		if (error)
			goto out;
	} else
		log(TRACE, "%s: OPTION_IPS_ONLY", __func__);

	/* 
	 * If restricted_target_name is non-NULL and refers to a zone name on
	 * this cluster, passing it to get_local_cluster_info() will result in
	 * only getting info on nodes in the zone.
	 */
	if (tinit.restricted_target_name && *tinit.restricted_target_name)
		log(TRACE, "%s: restricted target name=%s", __func__,
		    tinit.restricted_target_name);

	/* Use internal IPs for localhost to localhost jobs */
	if (strcmp(tinit.src_cluster_id, get_cluster_guid()) == 0) {
		tctx->num_nodes = get_local_cluster_info(&nodes, &tctx->ips,
		    NULL, NULL, &g_dst_addr, tinit.restricted_target_name,
		    true, true, &msg, &error);
	} else {
		tctx->num_nodes = get_local_cluster_info(&nodes, &tctx->ips,
		    NULL, NULL, &g_dst_addr, tinit.restricted_target_name,
		    false, false, &msg, &error);
	}

	if (msg) {
		log(NOTICE, "%s", msg);
		free(msg);
		msg = NULL;
	}

out:
	if (job_name)
	    free(job_name);
	if (error && isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS))
		send_error_and_exit(sw_ctx->global.primary, error);
			
	if (error && isi_error_is_a(error, ISI_SIQ_ERROR_CLASS)) {
		siqerr = isi_siq_error_get_siqerr(
		    (struct isi_siq_error *)error);
		
		if (siq_err_has_attr(siqerr, EA_MAKE_UNRUNNABLE))
			make_unrunnable = true;
		else
			send_error_and_exit(sw_ctx->global.primary, error);
	}

	if (global->job_ver.common < MSG_VERSION_CHOPU) {
		resp.head.type = OLD_TARGET_RESP_MSG;
	} else if (global->job_ver.common < MSG_VERSION_RIPTIDE) {
		resp.head.type = OLD_TARGET_RESP_MSG2;
	} else {
		resp.head.type = build_ript_msg_type(TARGET_RESP_MSG);
		ript_resp = &resp.body.ript;
		ript_msg_init(ript_resp);
	}

	if (make_unrunnable) {
		set_resp_error(&resp, error, global->job_ver.common);
		goto cleanup;
	}

	/*
	 * if the source is sending new features i.e post 3.0 , then
	 * send a synciq version which is common to source
	 * and target.
	 */
	if ((tinit.flags & FLAG_VER_3_5) ||
	    (tinit.flags & FLAG_VER_CLOUDPOOLS)) {
		ver_msg.head.type = COMMON_VER_MSG;
		/*
		 * Send version which is common to both source and target.
		 */
		if (tinit.flags & FLAG_VER_3_5) {
			ver_msg.body.common_ver.version |= FLAG_VER_3_5;
			global->curr_ver = FLAG_VER_3_5;
		}

		if (tinit.flags & FLAG_VER_CLOUDPOOLS) {
			ver_msg.body.common_ver.version |= FLAG_VER_CLOUDPOOLS;
			global->curr_ver |= FLAG_VER_CLOUDPOOLS;
		}

		/* Starting with Riptide, use job_ver, not curr_ver. */
		ASSERT((global->curr_ver &
		    ~(FLAG_VER_3_5 | FLAG_VER_CLOUDPOOLS)) == 0);

		msg_send(sw_ctx->global.primary, &ver_msg);
	}

	/*
	 * Check compliance domain version compatibility.
	 */
	if (tinit.flags & FLAG_COMPLIANCE) {
		/* v1 -> v1 ok.
		 * v1 -> v2 initial syncs ok.
		 * v1 -> v2 incremental syncs ok only if source has patch.
		 * v2 -> v1 checked by coordinator after TARGET_RESP_MSG.
		 * v2 -> v2 ok.
		 */

		/*
		 * Check if the SyncIQ policy is rooted at a compliance domain root.
		 * If not, error out.
		 */
		check_compliance_domain_root(tinit.target_dir, &error);
		if (error) {
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		}

		global->compliance_dom_ver = get_compliance_dom_ver(
		    tinit.target_dir, &error);
		if (error) {
			error = isi_siq_error_new(E_SIQ_COMP_TGTDIR_ENOENT,
			    "%s: Failed to get compliance domain version of "
			    "the target_dir: %s", __func__,
			    isi_error_get_message(error));
			goto out;
		}

		/* We will assume that setting fake_compliance without a return
		 * value will mean we want a v1 domain. */
		UFAIL_POINT_CODE(fake_compliance,
			global->compliance_dom_ver = (RETURN_VALUE >= 2) ?
			    2 : 1;
			global->is_fake_compliance = true;
		);

		/* Pretend that the source has the patch which allows v1 -> v2
		 * compliance syncs. */
		UFAIL_POINT_CODE(fake_v1_v2_patch,
			tinit.flags |= FLAG_COMP_V1_V2_PATCH;
		);

		global->compliance_v2 = (tinit.flags & FLAG_COMPLIANCE_V2) &&
		    (global->compliance_dom_ver == 2);

		if (global->compliance_dom_ver == 0) {
			/* v1/v2 -> non-compliance. */
			error = isi_system_error_new(E_SIQ_CONF_INVAL,
			    "%s is not a SmartLock Compliance directory. "
			    "Please set up a SmartLock Compliance directory "
			    "here before running the policy.",
			    tinit.target_dir);
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		} else if (global->compliance_dom_ver == 2 &&
		    !(tinit.flags & FLAG_COMPLIANCE_V2) &&
		    !(tinit.flags & FLAG_CLEAR) &&
		    !(tinit.flags & FLAG_COMP_V1_V2_PATCH)) {
			/* v1 -> v2: Incremental syncs aren't allowed unless
			 * the source is patched or on halfpipe. */
			error = isi_siq_error_new(E_SIQ_CONF_INVAL,
			    "Incremental syncs from a version 1 "
			    "to a version 2 compliance domain are not "
			    "supported. Please contact Isilon support to "
			    "enable this functionality.");
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		} else if (global->compliance_dom_ver == 2 &&
		    !(tinit.flags & FLAG_COMPLIANCE_V2) &&
		    !(tinit.flags & FLAG_CLEAR) &&
		    (tinit.flags & FLAG_COMP_V1_V2_PATCH)) {
			/* v1+patch -> v2: Inform the source that this cluster
			 * can handle the patch. */
			general_signal_send(sw_ctx->global.primary,
			    SIQ_COMP_V1_V2_PATCH);
		} else if ((tinit.flags & FLAG_COMPLIANCE_V2) &&
		    (tinit.flags & FLAG_CLEAR) &&
		    global->compliance_dom_ver == 2) {
			/* v2 -> v2: Initial sync.
			* Add the target guid directory to the resync list. */
			add_target_compliance_store_for_resync(tinit.target_dir,
			    tinit.policy_id, &error);
			if (error)
				goto out;
		}

		/* Checks if source and target are compatible to support
		 * compliance mode late stage dir unlink.
		 */
		if (access(SIQ_COMP_LATE_STAGE_UNLINK_PATCH_FILE, F_OK) == 0) {
			if (tinit.comp_late_stage_unlink_patch != 1) {
				error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
				    "Target supports compliance mode late stage"
				    " dir unlink while source does not. Please "
				    "upgrade source cluster.");
				set_resp_error(&resp, error,
				    global->job_ver.common);
				goto cleanup;
			}

			ript_msg_set_field_uint8(ript_resp,
			    RMF_COMP_LATE_STAGE_UNLINK_PATCH, 1, 1);
			log(DEBUG, "%s: Target has comp late stage unlink patch",
			    __func__);
		} else if (errno == ENOENT) {
			error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
			    "Could not access Compliance mode late stage"
			    " unlink patch file. Has the target been "
			    "upgraded and committed with patch?");
			set_resp_error(&resp, error,
			    global->job_ver.common);
			goto cleanup;
		} else {
			error = isi_system_error_new(errno,
			    "Failed to check support for compliance late stage "
			    "dir unlink.");
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		}
	} else if (strcmp(tinit.target_dir, "/ifs") != 0
		    && strcmp(tinit.target_dir, "/ifs/") != 0) {
		/*
		 * If source dir doesn't have a Compliance domain set and
		 * the target dir path is '/ifs' or '/ifs/', we let the sync
		 * go ahead and do not check if it the target_dir has a
		 * Compliance domain set since you can't set a Compliance
		 * domain on '/ifs'.
		 */
		is_compliance =
		    is_or_has_compliance_dir(tinit.target_dir, &error);
		/* This is needed to check worm ancestors. */
		if (error) {
			/*
			 * If the source is with a domain on it, and the
			 * target dir doesn't exist, succeed.
			 */
			if (!isi_system_error_is_a(error, ENOENT))
				goto out;

			isi_error_free(error);
			error = NULL;
		} else if (is_compliance) {
			/* non-compliance -> v1/v2 fail. */
			error = isi_siq_error_new(E_SIQ_CONF_INVAL,
			    "Cannot sync from a non-SmartLock Compliance "
			    "directory to a SmartLock Compliance directory.");
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		}
	}

	construct_inx_addr_list(nodes, tctx->num_nodes, global->curr_ver,
	    &lnn_ipx_list, &ipslen);	

	if (global->job_ver.common < MSG_VERSION_CHOPU) {
		resp.body.old_target_resp.target_cluster_name =
		    sw_ctx->global.cluster_name;
		resp.body.old_target_resp.target_cluster_id =
		    get_cluster_guid();

		resp.body.old_target_resp.ipslen = ipslen;
		resp.body.old_target_resp.ips = lnn_ipx_list;

	} else if (global->job_ver.common < MSG_VERSION_RIPTIDE) {
		resp.body.old_target_resp2.target_cluster_name =
		    sw_ctx->global.cluster_name;
		resp.body.old_target_resp2.target_cluster_id =
		    get_cluster_guid();

		resp.body.old_target_resp2.ipslen = ipslen;
		resp.body.old_target_resp2.ips = lnn_ipx_list;
	} else {
		ript_msg_set_field_str(ript_resp, RMF_TARGET_CLUSTER_NAME, 1,
		    sw_ctx->global.cluster_name);
		ript_msg_set_field_str(ript_resp, RMF_TARGET_CLUSTER_ID, 1,
		    get_cluster_guid());
		ript_msg_set_field_bytestream(ript_resp, RMF_IPS, 1,
		    (uint8_t *)lnn_ipx_list, (uint32_t)ipslen);

		ript_msg_set_field_uint8(ript_resp, RMF_COMPLIANCE_DOM_VER, 1,
		    global->compliance_dom_ver);
		// Cloudpools file versions
		tgt_cbm_vers = isi_cbm_file_versions_get_supported();
		if (tgt_cbm_vers == NULL) {
			error = isi_siq_error_new(E_SIQ_CLOUDPOOLS,
			    "%s: Failed to create Cloudpools file "
			    "versions: %s", __func__, strerror(errno));
			goto cleanup;
		}
		tgt_cbm_vers_buf = malloc(tgt_cbm_vers_buf_len);
		tgt_cbm_vers_buf_len = isi_cbm_file_versions_serialize(
		    tgt_cbm_vers, tgt_cbm_vers_buf, tgt_cbm_vers_buf_len,
		    &error);
		if (error != NULL) {
			if (isi_cbm_error_is_a(error, CBM_LIMIT_EXCEEDED)) {
				isi_error_free(error);
				error = NULL;

				tgt_cbm_vers_buf = realloc(tgt_cbm_vers_buf,
				    tgt_cbm_vers_buf_len);
				tgt_cbm_vers_buf_len =
				    isi_cbm_file_versions_serialize(
				    tgt_cbm_vers, tgt_cbm_vers_buf,
				    tgt_cbm_vers_buf_len, &error);
				if (error != NULL) {
					set_resp_error(&resp, error,
					    global->job_ver.common);
					goto cleanup;
				}
			} else {
				set_resp_error(&resp, error,
				    global->job_ver.common);
				goto cleanup;
			}
		}
		ript_msg_set_field_bytestream(ript_resp, RMF_CBM_FILE_VERSIONS,
		    1, tgt_cbm_vers_buf, tgt_cbm_vers_buf_len);

		if (access(SIQ_TARGET_WORK_LOCKING_PATCH_FILE, F_OK) == 0) {
			ript_msg_set_field_uint8(ript_resp,
			    RMF_TARGET_WORK_LOCKING_PATCH, 1, 1);
			log(DEBUG, "%s: Detected target work locking patch.",
			    __func__);
		} else if (errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Failed to check for support of target work "
			    "locking.");
			set_resp_error(&resp, error, global->job_ver.common);
			goto cleanup;
		}

	}

cleanup:
	isi_cbm_file_versions_destroy(tgt_cbm_vers);

	/*
	 * If the peer only needs IPs, send the response now. Otherwise, wait
	 * until we know things are OK in the tmonitor before sending the
	 * response.
	 */
	if (option == OPTION_IPS_ONLY || error) {
		msg_send(sw_ctx->global.primary, &resp);
		free(tgt_cbm_vers_buf);
		exit(EXIT_SUCCESS);
	}

	/* Convert this sworker to a tmonitor on OPTION_INIT. */
	target_monitor(sw_ctx->global.primary, sw_ctx, &tinit, &resp);
	free(tgt_cbm_vers_buf);

	return 0;
}

static int
group_callback(void *ctx)
{
	uint32_t tmp_lnn;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct isi_error *error = NULL;

	log(TRACE, "group_callback");
	if (sw_two_down_nodes())
		log(FATAL, "Filesystem readiness lost, unable to continue");

	tmp_lnn = global->lnn;
	global->lnn = get_local_lnn(&error);
	if (error)
		log(FATAL, "Failed to get local lnn: %s",
		    isi_error_get_message(error));

	/* If our LNN changed, and we're logging compliance conflicts, then
	 * we need to log to the file for our new LNN. */
	if (tmp_lnn != global->lnn && has_comp_conflict_log())
		init_comp_conflict_log(global->sync_id, global->run_number,
		    global->lnn);

	return 0;
}

// If this policy is a failback policy, or an original policy referenced by
// another failback policy, remove any associations since this policy is going
// away.

static void 
target_policy_clear_assocs(char *policy_id) 
{
	struct siq_policies *policies = NULL;
	struct siq_policy *pol = NULL;

	struct isi_error *error = NULL;

	policies = siq_policies_load(&error);
	if (error)
		goto out;

	log(TRACE, "%s", __func__);

	while ((pol = siq_policy_get_by_mirror_pid(policies, policy_id))) {
		FREE_AND_NULL(pol->common->mirror_pid);
		pol->common->mirror_state = SIQ_MIRROR_OFF;
	}

	siq_policies_save(policies, &error);

out:
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		isi_error_free(error);
	}

	siq_policies_free(policies);	
}

static void 
target_delete_failover_snap(char *policy_id) 
{

	int lock_fd = -1;
	struct isi_error *error = NULL;

	struct siq_policies *policies = NULL;
	struct siq_policy *mirror_pol = NULL;

	struct siq_source_record *srec = NULL;
	ifs_snapid_t mirror_latest = INVALID_SNAPID;

	struct target_record *trec = NULL;

	bool delete_snap = false;

	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (lock_fd == -1)
		goto out;

	trec = siq_target_record_read(policy_id, &error);
	if (error || trec == NULL)
		goto out;

	close(lock_fd);
	lock_fd = -1;

	policies = siq_policies_load(&error);
	if (error)
		goto out;

	mirror_pol = siq_policy_get_by_mirror_pid(policies, policy_id);
	if (!mirror_pol) {
		delete_snap = true;
	} else {
		log(DEBUG, "%s mirror pol: %s", __func__,
		    mirror_pol->common->pid);

		if (!siq_source_has_record(mirror_pol->common->pid)) {
			delete_snap = true;
		} else {
			srec = siq_source_record_load(mirror_pol->common->pid,
			    &error);
			if (error)
				goto out;

			siq_source_get_latest(srec, &mirror_latest);

			log(DEBUG, "latest: %u mirror latest: %lld",
			    trec->latest_snap, mirror_latest);

			/* Only remove this snapshot if it's not in use by the
			   mirror policy. */
			if (mirror_latest != trec->latest_snap &&
			    mirror_latest != INVALID_SNAPID)
				delete_snap = true;
		}
	}

	/* Delete the latest snap found in target record */
	if (delete_snap && trec->latest_snap != INVALID_SNAPID) {
		delete_snapshot_helper(trec->latest_snap);
	}

out:
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		isi_error_free(error);
	}

	siq_policies_free(policies);

	siq_source_record_free(srec);

	siq_target_record_free(trec);

	if (lock_fd != -1)
		close(lock_fd);
}

static int
target_policy_delete_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct generic_msg reply_msg = {};
	struct target_record *trec = NULL;
	char fn[MAXPATHLEN + 1];
	int ret;
	struct isi_error *error = NULL;

	log(TRACE, "target_policy_delete_callback: begin");

	ASSERT(m->head.type == TARGET_POLICY_DELETE_MSG);
	/* Prep the reply */
	reply_msg.head.type = GENERIC_ACK_MSG;
	reply_msg.body.generic_ack.id =
	    strdup(m->body.target_policy_delete.policy_id);

	/* Clean up the target's work locking directories */
	delete_wi_locking_dir(m->body.target_policy_delete.policy_id,
	    &error);
	if (error) {
		log(INFO, "%s", isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	/*
	 * Remove the failover snapshot associated with policy
	 */
	target_delete_failover_snap(
	    m->body.target_policy_delete.policy_id);

	/* Bug 186554 - Remove idmap file. */
	trec = siq_target_record_read(m->body.target_policy_delete.policy_id,
	    &error);
	if (error) {
		log(INFO, "%s: Failed to read target record: %s", __func__,
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	} else {
		snprintf(fn, sizeof(fn), "%s/%.100s_%s.gz",
		    SIQ_IFS_IDMAP_DIR, trec->policy_name,
		    m->body.target_policy_delete.policy_id);
		ret = unlink(fn);
		if (ret == -1 && errno != ENOENT)
			log(INFO, "%s: failed to delete idmap file %s: %s",
			    __func__, fn, strerror(errno));
		siq_target_record_free(trec);
	}

	siq_target_delete_record(m->body.target_policy_delete.policy_id,
	    false, false, &error);
	if (!error) {
		siq_btree_remove_target(m->body.target_policy_delete.policy_id,
		    &error);
		isi_error_free(error);
		error = NULL;
	}

	target_policy_clear_assocs(m->body.target_policy_delete.policy_id);

	/* Send ack */
	if (error) {
		struct fmt FMT_INIT_CLEAN(fmt);
		fmt_print(&fmt, "%s", isi_error_get_message(error));
		reply_msg.body.generic_ack.msg = fmt_detach(&fmt);
		
		if (isi_error_is_a(error, ISI_SIQ_ERROR_CLASS)) {
			int siqerr = isi_siq_error_get_siqerr(
			    (struct isi_siq_error*)error);
			reply_msg.body.generic_ack.code =
			    (siqerr == E_SIQ_TDB_WARN) ? ACK_WARN : ACK_ERR;
		} else {
			reply_msg.body.generic_ack.code = ACK_ERR;
		}
	} else {
		reply_msg.body.generic_ack.code = ACK_OK;
		log(INFO, "Policy %s has been deleted",
		    m->body.target_policy_delete.policy_id);
	}

	remove_syncrun_file(m->body.target_policy_delete.policy_id);

	msg_send(sw_ctx->global.primary, &reply_msg);

	/* Cleanup */
	isi_error_free(error);
	free(reply_msg.body.generic_ack.id);
	free(reply_msg.body.generic_ack.msg);

	log(TRACE, "target_policy_delete_callback: end");

	exit(EXIT_SUCCESS);
}

static int
target_pol_stf_downgrade_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct generic_msg ack_msg = { .body.generic_ack.code = ACK_OK };
	struct isi_error *error = NULL;
	struct target_record *trec = NULL;
	char tmp_working_map_name[MAXNAMELEN + 1];
	char *policy_id = NULL;
	char cpss_mirror_name[MAXNAMLEN];
	int lock_fd = -1;
	char rep_mirror_name[MAXNAMLEN];

	log(TRACE, "%s", __func__);

	ASSERT(m->head.type == TARGET_POL_STF_DOWNGRADE_MSG);
	policy_id = m->body.target_pol_stf_downgrade.policy_id;

	ack_msg.head.type = GENERIC_ACK_MSG;
	ack_msg.body.generic_ack.id = strdup(policy_id);

	lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
	if (error)
		goto out;

	trec = siq_target_record_read(policy_id, &error);
	if (error)
		goto out;

	if (trec->job_status == SIQ_JS_RUNNING ||
	    trec->job_status == SIQ_JS_PAUSED) {
		error = isi_system_error_new(errno,
		    "Can't downgrade policy in current state (%d)",
		    trec->job_status);
		goto out;
	}

	/* delete the linmap if it exists */
	remove_linmap(policy_id, policy_id, true, &error);
	if (error)
		goto out;

	/* delete tmp working linmap if it exists */
	get_tmp_working_map_name(tmp_working_map_name, policy_id);
	remove_linmap(policy_id, tmp_working_map_name, true, &error);
	if (error)
		goto out;

	/*
	 * Bug 175709
	 * Remove the mirrored repstate.
	 */
	get_mirrored_rep_name(rep_mirror_name, policy_id);
	remove_repstate(policy_id, rep_mirror_name, true, &error);
	if (error)
		goto out;

	/*
	 * Bug 216899
	 *
	 * Remove the CPSS (Cloud Pools Sync State) associated with the policy.
	 */
	get_mirrored_cpss_name(cpss_mirror_name, policy_id);
	remove_cpss(policy_id, cpss_mirror_name, true, &error);
	if (error)
		goto out;

	log(DEBUG, "STF downgrade of policy %s completed", policy_id);

out:
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		struct fmt FMT_INIT_CLEAN(fmt);
		fmt_print(&fmt, "%s", isi_error_get_message(error));
		ack_msg.body.generic_ack.msg = fmt_detach(&fmt);
		ack_msg.body.generic_ack.code = ACK_ERR;
	}

	msg_send(sw_ctx->global.primary, &ack_msg);

	siq_target_record_free(trec);
	free(ack_msg.body.generic_ack.id);
	free(ack_msg.body.generic_ack.msg);
	exit(EXIT_SUCCESS);
}

void
send_cached_file_ack_msgs(struct migr_sworker_ctx *sw_ctx)
{
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct generic_msg send_msg = {};
	struct file_ack_entry *current = NULL;
	int count = 0;
	
	log(TRACE, "%s begin", __func__);
	while (!TAILQ_EMPTY(global->bulk_tw_acks)) {
		current = TAILQ_FIRST(global->bulk_tw_acks);
		send_msg.head.type = ACK_MSG3;
		memcpy(&send_msg.body.ack3, current->msg, 
		    sizeof(struct ack_msg3));
		msg_send(sw_ctx->global.primary, &send_msg);
		
		TAILQ_REMOVE(global->bulk_tw_acks, current, ENTRIES);
		free(current->msg->file);
		free(current->msg);
		free(current);
		count++;
	}
	log(DEBUG, "%s: finished count: %d", __func__, count);
	ASSERT(count == global->bulk_tw_acks_count);
	global->bulk_tw_acks_count = 0;
	log(TRACE, "%s: bulk_tw_acks_count = 0", __func__);
}

void
sync_lmap_updates(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin,
    ifs_lin_t dlin, bool sync_only, struct isi_error **error_out)
{
	struct generic_msg lin_map_blk_msg = {};
	struct isi_error *error = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_comp_ctx *comp_ctx = &sw_ctx->comp;
	unsigned lmap_pending_updates = global->lmap_pending_acks;

	log(TRACE, "%s: lmap_pending_acks %d bulk_tw_acks_count %d", 
	    __func__, global->lmap_pending_acks, 
	    global->bulk_tw_acks_count);

	/* Ensure that these counts are sane.  Note that 
	 * lmap_pending_acks counts at file begin and 
	 * bulk_tw_acks_count counts at file done so 
	 * they may not match */
	if (global->doing_diff_sync) {
		ASSERT(global->lmap_pending_acks == 
		    global->bulk_tw_acks_count);
	}
	
	/*
	 * If we have reached the pending update limit , then 
	 * flush the entries and send the ack count back to source
	 */
	if (lmap_pending_updates >= LMAP_PENDING_LIMIT || sync_only || 
	    (global->doing_diff_sync && 
	    (lmap_pending_updates >= (DS_MAX_CACHE_SIZE - 2)))) {
		flush_lmap_log(&global->lmap_log, global->lmap_ctx, 
		    &error);
		if (error)
			goto out;

		if (global->doing_diff_sync && global->do_bulk_tw_lmap) {
			send_cached_file_ack_msgs(sw_ctx);
		} else {
			lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
			lin_map_blk_msg.body.lin_map_blk.lmap_updates = 
			    lmap_pending_updates;
			msg_send(sw_ctx->global.primary, &lin_map_blk_msg);
		}
		log(DEBUG, "%s: clearing lmap_pending_acks", __func__);
		sw_ctx->global.lmap_pending_acks = 0;
	}

	//Also sync late stage commit
	if (comp_ctx->worm_commit_ctx) {
		flush_lin_list_log(&comp_ctx->worm_commit_log,
		    comp_ctx->worm_commit_ctx, &error);
		if (error) {
			goto out;
		}
	}
	//...and comp dir dels
	if (comp_ctx->worm_dir_dels_ctx) {
		flush_lin_list_log(&comp_ctx->worm_dir_dels_log,
		    comp_ctx->worm_dir_dels_ctx, &error);
		if (error) {
			goto out;
		}
	}

	if (sync_only)
		goto out;

	/* Add new entry into lmap log */
	lmap_log_add_entry(global->lmap_ctx, &global->lmap_log, slin, dlin,
	    LMAP_SET, 0, &error);
	if (error)
		goto out;
	sw_ctx->global.lmap_pending_acks++;
	log(TRACE, "%s lmap_pending_acks = %d", __func__, sw_ctx->global.lmap_pending_acks);

out:
	isi_error_handle(error, error_out);
}

static int
lin_map_blk_callback(struct generic_msg *m, void *ctx)
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;

	log(TRACE, "%s", __func__);

	/* We need to sync lmap updates and send ack back to source */
	sync_lmap_updates(sw_ctx, 0, 0, true, &error);
	if (error)
		goto out;

	if (global->doing_diff_sync && global->do_bulk_tw_lmap) {
		global->do_bulk_tw_lmap = false;
	}
	UFAIL_POINT_CODE(lbc_sleep_exit,
	    if (m->body.lin_map_blk.lmap_updates == -1) {
		    siq_nanosleep(RETURN_VALUE, 0);
		    log(FATAL, "%s exiting", __func__);});
out:
	sw_process_fs_error(sw_ctx, SW_ERR_LIN, "linmap", 
	    ENC_DEFAULT, 0, error);
	isi_error_free(error);
	return 0;
}

static int
lin_map_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct stat st;
	struct isi_error *error = NULL;
	uint64_t lin_out = 0;
	struct generic_msg msg = {};
	int ret;
	int fd = -1;

	log(TRACE, "lin_map_callback");

	ASSERT(global->stf_upgrade_sync);
	
	ret = enc_fstatat(sw_ctx->worker.cwd_fd, m->body.lin_map.name,
	    m->body.lin_map.enc, &st, AT_SYMLINK_NOFOLLOW);
	if (ret != 0) {
		if (errno == ENOENT) {
			fd = open_or_create_reg(sw_ctx, sw_ctx->worker.cwd_fd,
			    m->body.lin_map.name, m->body.lin_map.enc, NULL,
			    &error);
			if (error)
				goto out;
			ASSERT(fd >= 0);
		
			/* Lets stat the new file */
			ret = fstat(fd, &st);
		}
	}
	if (ret != 0) {
		error = isi_system_error_new(errno, "Failed to stat %s/%s "
		    "which is believed to exist on the target",
		    sw_ctx->worker.cur_path.path, m->body.lin_map.name);
		goto out;
	}

	if (st.st_size == 0) {
		/*
		 * Any file with zero size will be considered
		 * as exception category. This might add few extra
		 * hops but it is better to be safe than sorry
		 */
		 /* Send a negative ack about hash mismatch */
		msg.head.type = LIN_ACK_MSG;
		msg.body.lin_ack.src_lin = m->body.lin_map.src_lin;
		msg.body.lin_ack.dst_lin = st.st_ino;
		msg.body.lin_ack.result = 1;
		msg.body.lin_ack.siq_err = E_SIQ_BBD_CHKSUM;
		msg_send(global->primary, &msg);
	}

	/* Set read only domain attribute */
	set_domain_upgrade(sw_ctx, m->body.lin_map.name, m->body.lin_map.enc, 
	    m->body.lin_map.prev_cookie, m->body.lin_map.cur_cookie, &error);
	if (error) {
		goto out;
	}

	if (global->lin_map_blk) {
		sync_lmap_updates(sw_ctx, m->body.lin_map.src_lin, st.st_ino,
		    false, &error);
		if (error)
			goto out;
	} else {
		/* try to add mapping conditionally. if mapping fails, we may 
		 * be woking on a recovery item. */
		ret = set_mapping_cond(m->body.lin_map.src_lin, st.st_ino,
		    global->lmap_ctx, true, 0, &error);
		if (error)
		    goto out;

		if (ret == 0) {
			log(DEBUG, "File %s/%s already found in map under slin"
			    "%llx", sw_ctx->worker.cur_path.path, 
			    m->body.lin_map.name, m->body.lin_map.src_lin);
			ret = get_mapping(m->body.lin_map.src_lin, &lin_out,
			    global->lmap_ctx, &error);
			if (error)
				goto out;

			/* if mapping already exists, it must be consistent 
			 * with out current information */
			ASSERT(ret && (lin_out == st.st_ino));
		}
	}
	
	/* Save this file hash so that we can use for upgrades */
	sw_ctx->inprog.dstate.last_file_hash = m->body.lin_map.cur_cookie;

out:
	if (fd > 0)
		close(fd);
	sw_process_fs_error(sw_ctx, SW_ERR_LIN, m->body.lin_map.name, 
	    m->body.lin_map.enc, m->body.lin_map.src_lin, error);
	isi_error_free(error);
	return 0;
}

static int
dir_upgrade_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx->inprog;
	struct isi_error *error = NULL;
	
	log(TRACE, "%s begin: %s", __func__, sw_ctx->worker.cur_path.path);
	
	ASSERT(global->stf_sync && 
	    (global->initial_sync || global->stf_upgrade_sync));
	
	stf_upgrade_abandoned_files(sw_ctx, 
	    m->body.dir_upgrade.slice_begin, 
	    m->body.dir_upgrade.slice_max, 
	    &sw_ctx->worker.cur_path, &error);

	sw_process_fs_error(sw_ctx, SW_ERR_DIR, inprog->dstate.name, 
	    inprog->dstate.enc, inprog->dstate.src_dirlin, error);
	isi_error_free(error);
	
	log(TRACE, "%s end", __func__);
	
	return 0;
}

int
timer_callback(void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = ctx;
	struct timeval timeout = {10, 0};

	struct isi_error *error = NULL;

	log(TRACE, "timer_callback");

	send_stats(sw_ctx);

	if (sw_ctx->global.primary > 0) {
		check_connection_with_primary(sw_ctx, &error);
		if (error) {
			log(ERROR, "%s", isi_error_get_message(error));
			exit(EXIT_FAILURE);
		}
	}

	if (sw_ctx->cpu_throttle.host == -1)
		connect_cpu_throttle_host(&sw_ctx->cpu_throttle, SIQ_SWORKER);

	send_cpu_stat_update(&sw_ctx->cpu_throttle);

	if (sw_ctx->comp.process_work) {
		comp_commit_send_periodic_checkpoint(sw_ctx, &error);
		if (error) {
			send_comp_target_error(sw_ctx->global.primary,
			    "Failure sending timed checkpoint "
			    "during compliance commit",
			    E_SIQ_COMP_COMMIT, error);
			exit(EXIT_FAILURE);
		}

		comp_map_send_periodic_checkpoint(sw_ctx, &error);
		if (error) {
			send_comp_target_error(sw_ctx->global.primary,
			    "Failure sending timed checkpoint "
			    "during compliance map processing",
			    E_SIQ_COMP_MAP_PROCESS, error);
			exit(EXIT_FAILURE);
		}
	}

	migr_register_timeout(&timeout, timer_callback, sw_ctx);
	
	isi_error_free(error);

	return 0;
}

/* Dump all acks back to pworker. Should only be used during
 * STF_PHASE_CT_LIN_UPDTS phase. */
static int
stf_chkpt_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct generic_msg msg = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
	if (error) {
		log(FATAL, "%s: failed to flush_lmap_log: %s", __func__,
		    isi_error_get_message(error));
	}

	msg.head.type = LIN_ACK_MSG;
	msg.body.lin_ack.src_lin = global->last_stf_slin;
	msg.body.lin_ack.dst_lin = 0;
	msg.body.lin_ack.result = 0;
	msg.body.lin_ack.siq_err = 0;
	msg.body.lin_ack.oper_count = global->stf_operations;
	msg_send(global->primary, &msg);
	global->stf_operations = 0;
	return 0;
}


static int
lin_prefetch_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct lin_prefetch_msg *linp = &m->body.lin_prefetch;
	uint64_t *lins = (uint64_t *)m->body.lin_prefetch.lins;
	struct ifs_lin_snapid_portal linsnaps[TOTAL_STF_LINS];
	struct isi_error *error = NULL;
	ifs_lin_t dlin;
	int i, count, lincount;
	bool found;
	int ret;

	log(TRACE, "%s", __func__);

	memset(linsnaps, 0,
	    sizeof(struct ifs_lin_snapid_portal) * TOTAL_STF_LINS);
	lincount = linp->linslen / sizeof(uint64_t);
	ASSERT(lincount <= TOTAL_STF_LINS);
	ASSERT(lincount > 0);
	for (i = 0, count = 0; i < lincount; i++) {
		found = get_mapping(lins[i], &dlin, global->lmap_ctx, &error);
		if (error)
			goto out;
		if (!found)
			continue;
		linsnaps[count].lin = dlin;
		linsnaps[count].snapid = HEAD_SNAPID;
		linsnaps[count].portal_depth = 0;
		count++;
	}
		
	if (count <= 0)
		goto out;
	
	ret = ifs_prefetch_lin(linsnaps, count, 0);
	if (ret) {
		error = isi_system_error_new(errno,
		    "Unable to prefetch lin range %llx - %llx",
		    linsnaps[0].lin, linsnaps[count - 1].lin);
		goto out;
	}

out:
	handle_stf_error_fatal(global->primary, error, E_SIQ_DEL_LIN,
	    EL_SIQ_DEST, linsnaps[0].lin, linsnaps[0].lin);
	return 0;
}

void
send_lmap_acks(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin, ifs_lin_t dlin,
    bool send_only)
{
	
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct pending_lmap_acks *lmap_acks = &global->lmap_acks;
	struct generic_msg lmap_updt_msg = {};

	/*
	 * If we have reached the pending update limit , then 
	 * flush the entries and send the ack count back to source
	 */
	if (lmap_acks->count >= SBT_BULK_MAX_OPS || (send_only &&
	    lmap_acks->count > 0)) {
		/*
		 * Lets send the linmap list to source
		 */
		lmap_updt_msg.head.type = DATABASE_UPDT_MSG;
		lmap_updt_msg.body.database_updt.flag = LMAP_UPDT;
		lmap_updt_msg.body.database_updt.updtlen = 
		    lmap_acks->count * sizeof(struct lmap_update);
		lmap_updt_msg.body.database_updt.updt = 
		    (char *)&lmap_acks->lmap_updates;
		msg_send(global->primary, &lmap_updt_msg);

		lmap_acks->count = 0;
	}
	
	if (send_only)
		goto out;

	lmap_acks->lmap_updates[lmap_acks->count].slin = slin;
	lmap_acks->lmap_updates[lmap_acks->count].dlin = dlin;
	lmap_acks->count++;
out:
	return;
}

static void
add_target_cluster_guid_lin(struct migr_sworker_global_ctx *global,
    struct isi_error **error_out)
{
	bool found;
	ifs_lin_t guid_lin;
	struct rep_entry entry = {};
	struct isi_error *error = NULL;

	ASSERT(global->compliance_v2 && global->is_src_guid);
	log(DEBUG, "%s: Processing compliance store target guid directory",
	    __func__);

	/* Don't add cluster guid for localhost syncs. */
	if (strcmp(global->comp_cluster_guid, get_cluster_guid()) == 0) {
		log(DEBUG, "%s: Skipping compliance store target guid "
		    "directory for local sync",  __func__);
		goto out;
	}

	guid_lin = get_target_compliance_store_lin(
	    global->target_base_path.path, &error);
	if (error)
		goto out;

	found = entry_exists(guid_lin, global->rep_mirror_ctx, &error);
	if (error)
		goto out;

	if (!found) {
		entry.lcount = 1;
		entry.is_dir = true;
		set_entry(guid_lin, &entry, global->rep_mirror_ctx, 0, NULL,
		    &error);
		if (error) {
			/*
			 * We could just be racing another
			 * worker. Check to see if the entry
			 * exists now.
			 */
			struct isi_error *error2 = NULL;
			found = entry_exists(guid_lin, global->rep_mirror_ctx,
			    &error2);
			if (error2) {
				isi_error_add_context(error,
				   "Failed entry_exists: %s",
				    isi_error_get_message(error2));
				isi_error_free(error2);
				error2 = NULL;
				goto out;
			}

			if (found) {
				isi_error_free(error);
				error = NULL;
			}
		}
	}

out:
	isi_error_handle(error, error_out);
}

static int
database_updt_callback(struct generic_msg *m, void *ctx)
{
	struct migr_sworker_ctx *sw_ctx = (struct migr_sworker_ctx *)ctx;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct database_updt_msg *dupdt = &m->body.database_updt;
	struct rep_update *rep_entries =
	    (struct rep_update *)m->body.database_updt.updt;
	int i, rep_count;
	ifs_lin_t dlin;
	char rep_mirror_name[MAXNAMLEN];
	char cpss_mirror_name[MAXNAMLEN];
	struct isi_error *error = NULL;
	struct rep_entry entries[SBT_BULK_MAX_OPS];
	struct cpss_entry cpss_entries[SBT_BULK_MAX_OPS];
	ifs_lin_t linlist[SBT_BULK_MAX_OPS];
	int lincount = 0;
	unsigned flag = dupdt->flag;
	bool found = false;
	bool mirrored = false;
	int ack_index;
	ifs_lin_t ack_lin;
	int fd = -1;
	bool lmap_removed;

	log(TRACE, "%s", __func__);

	ASSERT(global->curr_ver >= FLAG_VER_3_5);

	rep_count = dupdt->updtlen / sizeof(struct rep_update);
	ASSERT(rep_count <= SBT_BULK_MAX_OPS);

	/*
	 * If we need to mirror entries , make sure we have
	 * a repstate context. If its RESYNC then we are
	 * sure that we need to create repstate otherwise
	 * there has to be valid repstate already available.
	 */
	if (!global->mirror_check) {
		if (!global->rep_mirror_ctx) {
			ASSERT(global->sync_id != NULL);
			get_mirrored_rep_name(rep_mirror_name, global->sync_id);
			if (flag & RESYNC_ENTRIES) {
				create_repstate(global->sync_id, rep_mirror_name,
				    true, &error);
				if (error)
					goto out;
				open_repstate(global->sync_id, rep_mirror_name,
				    &global->rep_mirror_ctx, &error);
				if (error)
					goto out;
			} else {
				found = repstate_exists(global->sync_id,
				    rep_mirror_name, &error);
				if (error)
					goto out;
				if (found) {
					open_repstate(global->sync_id,
					    rep_mirror_name,
					    &global->rep_mirror_ctx, &error);
					if (error)
						goto out;
				}
			}
		}

		if (!global->cpss_mirror_ctx) {
			ASSERT(global->sync_id != NULL);
			get_mirrored_cpss_name(cpss_mirror_name, global->sync_id);
			if (flag & RESYNC_ENTRIES) {
				create_cpss(global->sync_id, cpss_mirror_name,
				    true, &error);
				if (error)
					goto out;
				open_cpss(global->sync_id, cpss_mirror_name,
				    &global->cpss_mirror_ctx, &error);
				if (error)
					goto out;
			} else {
				found = cpss_exists(global->sync_id,
				    cpss_mirror_name, &error);
				if (error)
					goto out;

				if (found) {
					open_cpss(global->sync_id,
					    cpss_mirror_name,
					    &global->cpss_mirror_ctx, &error);
					if (error)
						goto out;
				}
			}
		}

		global->mirror_check = found;
	}

	ASSERT(!!(global->rep_mirror_ctx) == !!(global->cpss_mirror_ctx));
	if (global->rep_mirror_ctx && global->cpss_mirror_ctx) {
		mirrored = true;
		memset(linlist, 0, sizeof(ifs_lin_t) * SBT_BULK_MAX_OPS);
		memset(entries, 0, sizeof(struct rep_entry) * SBT_BULK_MAX_OPS);
	}
	memset(cpss_entries, 0, sizeof(struct cpss_entry) * SBT_BULK_MAX_OPS);

	for (i = 0; i < rep_count; i++) {
		/*
		 * Check for INVALID_LIN to signify adding the compliance
		 * cluster guid directory to the mirror repstate.
		 */
		if (rep_entries[i].slin == INVALID_LIN) {
			add_target_cluster_guid_lin(global, &error);
			if (error)
				goto out;

			/*
			 * cpss entry will be ignored since
			 * sync_type == BST_NONE. Also, source doesn't expect a
			 * lmap ack for this lin so ignore.
			 */
			continue;
		}

		/*
		 * First lookup linmap and get dlin
		 */
		get_mapping(rep_entries[i].slin, &dlin,
		    sw_ctx->global.lmap_ctx, &error);
		if (error)
			goto out;

		/* Bump total number of operations for ACK */
		global->stf_operations++;

		if (dlin <= 0)
			continue;

		lmap_removed = false;

		if (flag & FLIP_REGULAR) {
			if (rep_entries[i].lcount == 0) {
				/*
				 * Remove deleted lins from linmap database
				 */
				log(TRACE, " Removing entry %llx from linmap",
				    rep_entries[i].slin);
				lmap_log_add_entry(global->lmap_ctx,
				    &global->lmap_log, rep_entries[i].slin,
				    0, LMAP_REM, 0, &error);
				if (error)
					goto out;
				lmap_removed = true;
			}
		} else if (flag & REVERT_ENTRIES) {
			if (rep_entries[i].flag & REP_NOT_ON_TARGET) {
				/*
				 * Remove deleted lins from linmap database
				 */
				log(TRACE, " Removing entry %llx from linmap",
				    rep_entries[i].slin);
				lmap_log_add_entry(global->lmap_ctx,
				    &global->lmap_log, rep_entries[i].slin,
				    0, LMAP_REM, 0, &error);
				if (error)
					goto out;
				lmap_removed = true;
			}

		} else if (flag & RESYNC_ENTRIES) {
			ASSERT(rep_entries[i].lcount > 0);
		}
		/*
		 * Bug 116114
		 * Confirm that LINs added to the linmap exist on HEAD.
		 */
		if (global->flip_lin_verify && !lmap_removed) {
			log(TRACE, "%s: flip_lin_verify testing dlin %{} "
                            "(slin %{})", __func__, lin_fmt(dlin),
                            lin_fmt(rep_entries[i].slin));
			fd = ifs_lin_open(dlin, HEAD_SNAPID, O_RDONLY);
			if (fd == -1) {
				error = isi_system_error_new(errno,
				    "Unable to open dlin %{} "
				    "(slin %{})", lin_fmt(dlin),
				    lin_fmt(rep_entries[i].slin));
				goto out;
			}
			close(fd);
			fd = -1;
		}
		/*
		 * Now fill the rep entries and linmap list
		 */
		entries[lincount].lcount = rep_entries[i].lcount;
		if (rep_entries[i].flag & REP_IS_DIR)
			entries[lincount].is_dir = true;
		if (rep_entries[i].flag & REP_NON_HASHED)
			entries[lincount].non_hashed = true;
		linlist[lincount] = dlin;

		cpss_entries[i].del_entry = (rep_entries[i].lcount == 0);
		if (!(rep_entries[i].flag & REP_IS_DIR)) {
			cpss_entries[i].sync_type =
			    (rep_entries[i].flag >> 3) & 0x03;
		}
		ASSERT(cpss_entries[i].sync_type < BST_MAX);

		lincount++;
		send_lmap_acks(sw_ctx, rep_entries[i].slin, dlin, false);
	}

	if (lincount) {
		if (mirrored) {
			/*
			 * Copy the entries to mirror repstate.
			 */
			copy_rep_entries(global->rep_mirror_ctx, linlist,
			    entries, lincount, &error);
			if (error)
				goto out;

			copy_cpss_entries(global->cpss_mirror_ctx, linlist,
			    cpss_entries, rep_count, &error);
			if (error)
				goto out;
			send_lmap_acks(sw_ctx, 0, 0, true);
		}
	}

	/*
	 * Flush local lmap updates.
	 */
	flush_lmap_log(&global->lmap_log, global->lmap_ctx, &error);
	if (error)
		goto out;

	/*
	 * Decrement the total number of operations for ACK by one since
	 * the handle_lin_ack already does increment it for last lin
	 */
	global->stf_operations--;
	ack_index = rep_count - 1;
	if (rep_entries[ack_index].slin == INVALID_LIN) {
		ack_lin = (ack_index >= 1) ? rep_entries[ack_index - 1].slin :
		    global->last_stf_slin;
	} else
		ack_lin = rep_entries[ack_index].slin;
	handle_lin_ack(sw_ctx, ack_lin, &error);
	if(error)
		goto out;

out:
	if (fd != -1)
		close(fd);
	handle_stf_error_fatal(global->primary, error, E_SIQ_DATABASE_ENTRIES,
	    EL_SIQ_DEST, rep_entries[0].slin, rep_entries[0].slin);
	return 0;
}

static void
wait_for_burst_connect(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out)
{
	int ret = -1;
	int burst_fd = -1;
	uint16_t burst_port_min = 
	    global_config->root->coordinator->ports->burst_recv_port_min;
	uint16_t burst_port_max = 
	    global_config->root->coordinator->ports->burst_recv_port_max;
	int mem_constraint = 
	    global_config->root->application->burst_mem_constraint;
	int burst_sock_buf_size = 
	    global_config->root->application->burst_sock_buf_size;
	uint16_t port = 0;
	struct sockaddr_in server_addr;
	burst_fd_t r, server;
	burst_pollfd_t pollfd;
	struct isi_error *error = NULL;
	struct migr_sworker_global_ctx *global = &sw_ctx->global;
	struct generic_msg m = {};
	int sleep_interval = 1000;
	int max_retries = 10;
	socklen_t len;
	rde_status_t rv;
	
	len = sizeof(server_addr);
	ret = getsockname(global->primary, (struct sockaddr *)&server_addr,
	    &len);
	if (ret == -1) {
		error = isi_system_error_new(errno, "getsockname failed");
		goto out;
	}
#if 0
	/* We dont support burst for IPV6 */
	if (server_addr.sin_family == AF_INET6) {
		error = isi_system_error_new(ENOTSUP,
		    "BURST socket mode not supported for IPV6 address");
		goto out;
	}
#endif
	
	rv = getHostInterfaceImpl(&global->burst_host);
	if (rv != RDE_STATUS_OK) {
		error = isi_siq_error_new(E_SIQ_GEN_NET, 
		    "burst_socket logging init failed");
		goto out;
	}
	ewoc_init(&global->burst_host->_p);
	ewoc_setTraceLevel(ewoc_Debug);
	burst_startup();

	server = burst_socket(mem_constraint);
	if (server < 0) {
		error = isi_system_error_new(errno, "burst_socket failed");
		goto out;
	}


	/* Try to find unused burst port, If not fail the job */
	port = burst_port_min;
	while(port <= burst_port_max) {	
		server_addr.sin_port = htons(port);
		ret = burst_bind(server, (burst_sockaddr_t*)&server_addr,
		    sizeof(server_addr));
		if (ret == -1 && errno != EADDRINUSE) {
			error = isi_system_error_new(errno,
			    "burst_bind failed");
			goto out;
		} else if (ret == 0) {
			break;
		}
		port++;
	}

	if (port > burst_port_max) {
		error = isi_system_error_new(errno, 
		    "burst_bind failed: Could not find unused port");
		goto out;
	}

	/* Send the UDP port back to the source */
	m.head.type = BURST_SOCK_MSG;
	m.body.burst_sock.port = port;
	m.body.burst_sock.max_buf_size = burst_sock_buf_size;
	msg_send(global->primary, &m);

	ret = burst_listen(server);
	if (ret == -1) {
		error = isi_system_error_new(errno, "burst_listen failed");
		goto out;
	}

	/* Lets try to wait for some time */	
	while(max_retries > 0) {
		pollfd._fd = server;
		pollfd._events = BURST_POLLIN;
		pollfd._revents = 0;
		burst_poll(&pollfd, 1, sleep_interval);
		if (pollfd._revents & BURST_POLLIN) {
			break;
		} else {
			max_retries--;
			check_connection_with_peer(global->primary, true, 1,
			    &error);
			    if (error) 
				goto out;
			}
		}

	if (max_retries == 0) {
		error = isi_system_error_new(ETIMEDOUT,
		    "Unable to receive burst_connect from primary");
		goto out;
	}

	ASSERT(pollfd._revents & BURST_POLLIN);

	r = burst_accept(server);
	if (r == 0) {
		error = isi_system_error_new(errno, "burst_accept failed");
		goto out;
	}
	burst_close(server);

	burst_fd = burst_get_udp_fd(r);
	ASSERT(burst_fd > 0);
	migr_add_fd(burst_fd, sw_ctx, "pworker_burst");
	migr_fd_set_burst_sock(burst_fd, r, 0);
	migr_fd_set_burst_recv_buf(burst_fd, burst_sock_buf_size);

	migr_register_callback(burst_fd, DISCON_MSG,
	    disconnect_callback);
	migr_register_callback(burst_fd, AUTH_MSG, auth_callback);
	migr_register_callback(burst_fd, OLD_TARGET_INIT_MSG,
	    target_init_callback);
	migr_register_callback(burst_fd, build_ript_msg_type(TARGET_INIT_MSG),
	    target_init_callback);
	migr_register_callback(burst_fd, DIR_MSG4, dir4_callback);
	migr_register_callback(burst_fd, ADS_MSG, ads_callback);
	migr_register_callback(burst_fd, FILE_BEGIN_MSG,
	    file_begin_callback);
	migr_register_callback(burst_fd, OLD_FILE_BEGIN_MSG2,
	    old_file_begin2_callback);
	migr_register_callback(burst_fd, FILE_DONE_MSG,
	    file_done_callback);
	migr_register_callback(burst_fd, ERROR_MSG, error_callback);
	migr_register_callback(burst_fd, FILE_DATA_MSG,
	    file_data_callback);
	migr_register_callback(burst_fd, DONE_MSG, done_callback);
	migr_register_callback(burst_fd, OLD_WORK_INIT_MSG6,
	    work_init_callback);
	migr_register_callback(burst_fd, build_ript_msg_type(WORK_INIT_MSG),
	    work_init_callback);
	migr_register_callback(burst_fd, LIST_MSG2, list2_callback);
	migr_register_callback(burst_fd, SNAP_MSG, snap_callback);
	migr_register_callback(burst_fd, TARGET_POLICY_DELETE_MSG, 
	    target_policy_delete_callback);
	migr_register_callback(burst_fd, DELETE_LIN_MSG,
	    delete_lin_callback);
	migr_register_callback(burst_fd, LINK_MSG, link_callback);
	migr_register_callback(burst_fd, UNLINK_MSG, unlink_callback);
	migr_register_callback(burst_fd, LIN_UPDATE_MSG,
	    lin_update_callback);
	migr_register_callback(burst_fd, OLD_LIN_UPDATE_MSG,
	    old_lin_update_callback);
	migr_register_callback(burst_fd, LIN_COMMIT_MSG,
	    lin_commit_callback);
	migr_register_callback(burst_fd, LIN_MAP_MSG, lin_map_callback);
	migr_register_callback(burst_fd, DIR_UPGRADE_MSG, 
	    dir_upgrade_callback);
	migr_register_callback(burst_fd, TARGET_POL_STF_DOWNGRADE_MSG,
	    target_pol_stf_downgrade_callback);
	migr_register_callback(burst_fd, USER_ATTR_MSG,
	    user_attr_callback);
	migr_register_callback(burst_fd, STALE_DIR_MSG,
	    stale_dir_callback);
	migr_register_callback(burst_fd, STF_CHKPT_MSG,
	    stf_chkpt_callback);
	migr_register_callback(burst_fd, LIN_PREFETCH_MSG,
	    lin_prefetch_callback);
	migr_register_callback(burst_fd, EXTRA_FILE_MSG,
	    extra_file_callback);
	migr_register_callback(burst_fd, EXTRA_FILE_COMMIT_MSG,
	    extra_file_commit_callback);
	migr_register_callback(burst_fd, LIN_MAP_BLK_MSG,
	    lin_map_blk_callback);
	migr_register_callback(burst_fd, DATABASE_UPDT_MSG,
	    database_updt_callback);
	log(DEBUG, "Burst connection successful");

out:
	isi_error_handle(error, error_out);
}


/*  __  __       _       
 * |  \/  | __ _(_)_ __  
 * | |\/| |/ _` | | '_ \ 
 * | |  | | (_| | | | | |
 * |_|  |_|\__,_|_|_| |_|
 */

static void
usage(void)
{
	printf("usage: %s [-dv] [-l loglevel]\n", PROGNAME);
	exit(EXIT_FAILURE);
}

int
main(int argc, char *argv[])
{
	struct migr_sworker_ctx sw_ctx = {};
	struct migr_sworker_global_ctx *global = &sw_ctx.global;
	struct migr_sworker_worker_ctx *worker = &sw_ctx.worker;
	struct migr_sworker_inprogress_ctx *inprog = &sw_ctx.inprog;
	struct migr_tmonitor_ctx *tmonitor = &sw_ctx.tmonitor;
	struct migr_sworker_delete_ctx *del = &sw_ctx.del;

	int debug = 0;
	int buff_size;
	char c;
	struct isi_error *ierr = (struct isi_error *)NULL;

	struct ptr_vec a, b;
	g_ctx = &sw_ctx;
	
	struct ilog_app_init init = {
		.full_app_name = "siq_sworker",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file= "",
		.syslog_threshold = IL_INFO_PLUS,  /////IL_TRACE_PLUS  -> IL_INFO_PLUS
		.component = "sworker_secondary",
		.job = "",
	};
	
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);

	global->primary = -1;
	global->root_tmp_lin = -1;
	global->tmp_working_map = NULL;
	global->tmp_working_map_ctx = NULL;
	global->tmpdir_hash = NULL;
	global->enable_hash_tmpdir = false;
	global->quotas_present = false;
	worker->cwd_fd = -1;
	worker->saved_cwd = -1;
	global->loglevel = INFO;
	global->flip_lin_verify = false;
	gethostname(global->cluster_name, MAXHOSTNAMELEN);
	cluster_name_from_node_name(global->cluster_name);
	global->wi_lock_fd = -1;

	/* assume legacy behavior until we get a target init */
	tmonitor->legacy = true;
	tmonitor->coord_fd = -1;
	tmonitor->policy_id = NULL;
	tmonitor->policy_name = NULL;

	del->mdir = NULL;
	del->in_progress = false;
	del->hold_queue = &a;
	del->next_hold_queue = &b;
	pvec_init(del->hold_queue);
	pvec_init(del->next_hold_queue);
	del->error = false;

	global_config = siq_gc_conf_load(&ierr);
	if (ierr) {
		log(ERROR, "Failed to load global configuration: %s",
		    isi_error_get_message(ierr));
		exit(EXIT_FAILURE);
	}

	/* Initialize the work item locking variables. They will be
	 * reassigned in the work init callback, but it's probably
	 * a good idea to assign them a base value. */
	global->wi_lock_timeout =
	    global_config->root->application->work_item_lock_timeout;
	global->wi_lock_mod = 10;

	/* Bug 116114 - read in flip_lin_verify config */
	global->flip_lin_verify =
	    global_config->root->application->flip_lin_verify;

	while ((c = getopt(argc, argv, "dl:v")) != -1) {
		switch (c) {
		case 'd':
			debug = 1;
			break;
		case 'l':
			if (isdigit(optarg[0])) {
				global->loglevel = atoi(optarg);
				if (global->loglevel > TRACE)
					global->loglevel = TRACE;
				set_siq_log_level(global->loglevel);
			}
			else {
				warnx("Invalid log level of '%s'", optarg);
				usage();
			}
			break;
		case 'v':
			siq_print_version(PROGNAME);
			return 0;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc) {
		warnx("unexpected argument -- %s", *argv);
		usage();
	}

	if (!debug)
		daemon(0, 0);

	check_for_dont_run(true);

	if (UFAIL_POINT_INIT("isi_migr_sworker", isi_migr_sworker_ufp) != 0)
		log(FATAL, "Failed to init ufp");

	log(INFO, "starting build <%s>", BUILD);

	/*
	 * Here is where we'd set the flag saying we wait for the message from
	 * the primary before sending our version number.  This is shitty!
	 */
	global->primary =////////创建secondary worker和primary通信的socket,默认配置的sworker的port是5667
	    fork_accept_from(global_config->root->coordinator->ports->sworker,
	    &global->job_ver.common, debug, POL_ALLOW_FALLBACK,
	    global_config->root->application->max_sworkers_per_node, 0);
	signal(SIGUSR2, sigusr2);
	gmp_kill_on_quorum_change_register(NULL, &ierr);
	if (ierr)
		log(FATAL, "main: gmp_kill_on_quorum_change_register error");

	/* Do this before adding group callback in case node is unmounted. */
	if (sw_two_down_nodes()) {
		log(DEBUG, "Filesystem unmounted or more than one node down");
		exit(EXIT_FAILURE);
	}
	
	int_set_init(&prev_up_storage_nodes);
	get_not_dead_storage_nodes(&prev_up_storage_nodes);

#ifndef NOUSEDOTSNAP
	snapshot_set_dotsnap(1, 0, NULL);
#endif /* NOUSEDOTSNAP */

	migr_register_group_callback(group_callback, &sw_ctx);

	inprog->fstate.fd = -1;
	inprog->ds_tmp_fstate = NULL;
	migr_add_fd(global->primary, &sw_ctx, "pworker");

	inprog->fstates = calloc(1, sizeof(struct file_state_list));
	ASSERT(inprog->fstates);
	TAILQ_INIT(inprog->fstates);
	
	global->bulk_tw_acks = calloc(1, sizeof(struct file_ack_list));
	ASSERT(global->bulk_tw_acks);
	TAILQ_INIT(global->bulk_tw_acks);
	global->bulk_tw_acks_count = 0;

	migr_register_callback(global->primary, DISCON_MSG,
	    disconnect_callback);
	migr_register_callback(global->primary, AUTH_MSG, auth_callback);
	migr_register_callback(global->primary, OLD_TARGET_INIT_MSG,
	    target_init_callback);
	migr_register_callback(global->primary,
	    build_ript_msg_type(TARGET_INIT_MSG), target_init_callback);
	migr_register_callback(global->primary, DIR_MSG4, dir4_callback);
	migr_register_callback(global->primary, ADS_MSG, ads_callback);
	migr_register_callback(global->primary, FILE_BEGIN_MSG,
	    file_begin_callback);
	migr_register_callback(global->primary, OLD_FILE_BEGIN_MSG2,
	    old_file_begin2_callback);
	migr_register_callback(global->primary, FILE_DONE_MSG,
	    file_done_callback);
	migr_register_callback(global->primary, ERROR_MSG, error_callback);
	migr_register_callback(global->primary, FILE_DATA_MSG,
	    file_data_callback);
	migr_register_callback(global->primary, DONE_MSG, done_callback);
	migr_register_callback(global->primary, OLD_WORK_INIT_MSG6,
	    work_init_callback);
	migr_register_callback(global->primary,
	    build_ript_msg_type(WORK_INIT_MSG), work_init_callback);
	migr_register_callback(global->primary, LIST_MSG2, list2_callback);
	migr_register_callback(global->primary, SNAP_MSG, snap_callback);
	migr_register_callback(global->primary, TARGET_POLICY_DELETE_MSG, 
	    target_policy_delete_callback);
	migr_register_callback(global->primary, DELETE_LIN_MSG,
	    delete_lin_callback);
	migr_register_callback(global->primary, LINK_MSG, link_callback);
	migr_register_callback(global->primary, UNLINK_MSG, unlink_callback);
	migr_register_callback(global->primary, LIN_UPDATE_MSG,
	    lin_update_callback);
	migr_register_callback(global->primary, OLD_LIN_UPDATE_MSG,
	    old_lin_update_callback);
	migr_register_callback(global->primary, LIN_COMMIT_MSG,
	    lin_commit_callback);
	migr_register_callback(global->primary, LIN_MAP_MSG, lin_map_callback);
	migr_register_callback(global->primary, DIR_UPGRADE_MSG, 
	    dir_upgrade_callback);
	migr_register_callback(global->primary, TARGET_POL_STF_DOWNGRADE_MSG,
	    target_pol_stf_downgrade_callback);
	migr_register_callback(global->primary, USER_ATTR_MSG,
	    user_attr_callback);
	migr_register_callback(global->primary, WORM_STATE_MSG,
	    worm_state_callback);
	migr_register_callback(global->primary, STALE_DIR_MSG,
	    stale_dir_callback);
	migr_register_callback(global->primary, STF_CHKPT_MSG,
	    stf_chkpt_callback);
	migr_register_callback(global->primary, LIN_PREFETCH_MSG,
	    lin_prefetch_callback);
	migr_register_callback(global->primary, EXTRA_FILE_MSG,
	    extra_file_callback);
	migr_register_callback(global->primary, EXTRA_FILE_COMMIT_MSG,
	    extra_file_commit_callback);
	migr_register_callback(global->primary, LIN_MAP_BLK_MSG,
	    lin_map_blk_callback);
	migr_register_callback(global->primary, DATABASE_UPDT_MSG,
	    database_updt_callback);
	migr_register_callback(global->primary, OLD_CLOUDPOOLS_BEGIN_MSG,
	    old_cloudpools_msg_callback);
	migr_register_callback(global->primary, OLD_CLOUDPOOLS_STUB_DATA_MSG,
	    old_cloudpools_msg_callback);
	migr_register_callback(global->primary,
	    build_ript_msg_type(CLOUDPOOLS_STUB_DATA_MSG),
	    cloudpools_stub_data_callback);
	migr_register_callback(global->primary, OLD_CLOUDPOOLS_DONE_MSG,
	    old_cloudpools_msg_callback);
	migr_register_callback(global->primary,
	    build_ript_msg_type(CLOUDPOOLS_SETUP_MSG),
	    cloudpools_setup_callback);
	migr_register_callback(global->primary,
	    build_ript_msg_type(TARGET_WORK_LOCK_MSG),
	    target_work_lock_callback);

	cpu_throttle_ctx_init(&sw_ctx.cpu_throttle, SIQ_WT_SWORKER,
	    global_config->root->bwt->cpu_port);

	/* Set maximum available TCP buff size for recv */
	log(TRACE, "Set receive buffer size to maximum available "
	    "value");
	buff_size = SOCK_BUFF_SIZE;
	while (setsockopt(global->primary, SOL_SOCKET, SO_RCVBUF,
	    (void*)&buff_size, sizeof(buff_size)) < 0) {
		buff_size /= 2;
		if (errno != ENOBUFS) {
			log(COPY, "setsockopt returns unexpected "
			"error. Errno = %d", errno);
		}
		if (buff_size < MIN_SOCK_BUFF_SIZE) {
			log(FATAL, "Can't set TCP recv buffer size"
				"to big enough value");
		}
		log(TRACE, "Next try with %d KB", (buff_size / 1024));
	}
	log(DEBUG, "TCP buffer for rcv was set to %d", buff_size);

	/* Register callbacks for legacy pworker interop */
	migr_sworker_register_legacy(&sw_ctx);

	global->verify_tw_sync = false;
	if (global->initial_sync) {
		UFAIL_POINT_CODE(verify_tw_sync,
		    global->verify_tw_sync = true);
	}

	global->do_bulk_tw_lmap = false;

	isi_cbm_init(&ierr);
	if (ierr)
		log(FATAL, "Unable to initialize cloudpools services.");

	umask(0);
	migr_process();

	return 0;
}
