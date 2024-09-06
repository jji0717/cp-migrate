#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <regex.h>
#include <md5.h>
#include <errno.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/sysctl.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/ifs_procoptions.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_snapshot/snapshot.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_domain/dom_util.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>

#define USE_BURST_LOGGING
#include "pworker.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/selectors.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_cpool_cbm/isi_cbm_file_versions.h"

extern rde_status_t ewoc_init(HostInterface* host);
extern void ewoc_setTraceLevel(ewoc_LogSeverity);

static struct siq_gc_conf *g_siq_conf;
static struct migr_pworker_ctx *g_ctx;
static bool g_lost_quorum;
static char LOST_QUORUM_MSG[] = "Filesystem readiness lost, unable to continue";

static uint64_t set_fsplit_tw_chkpt(struct migr_pworker_ctx *pw_ctx);
static void save_recovery_state(struct migr_pworker_ctx *pw_ctx);
static int timer_callback(void *ctx);

static int
sigquorum_timer(void *ctx)
{
	if (g_lost_quorum)
		log(FATAL, "%s", LOST_QUORUM_MSG);
	return 0;
}

static void
sigquorum(int signo)
{
	struct timeval tv = {};

	if (group_has_quorum() < 1) {
		g_lost_quorum = true;
		migr_register_timeout(&tv, sigquorum_timer, TIMER_SIGNAL);
	}
}

static void
print_rep_entry(FILE *f, struct rep_entry *entry)
{
	if (!entry) {
		fprintf(f, "        NULL\n");
		return;
	}
	fprintf(f, "        lcount: %d\n", entry->lcount);
	fprintf(f, "        exception: %s\n", PRINT_BOOL(entry->exception));
	fprintf(f, "        incl_for_child: %s\n", 
	    PRINT_BOOL(entry->incl_for_child));
	fprintf(f, "        hash_computed: %s\n", 
	    PRINT_BOOL(entry->hash_computed));
	fprintf(f, "        non_hashed: %s\n", PRINT_BOOL(entry->non_hashed));
	fprintf(f, "        is_dir: %s\n", PRINT_BOOL(entry->is_dir));
	fprintf(f, "        new_dir_parent: %s\n", 
	    PRINT_BOOL(entry->new_dir_parent));
	fprintf(f, "        not_on_target: %s\n", 
	    PRINT_BOOL(entry->not_on_target));
	fprintf(f, "        changed: %s\n", PRINT_BOOL(entry->changed));
	fprintf(f, "        flipped: %s\n", PRINT_BOOL(entry->flipped));
	fprintf(f, "        lcount_set: %s\n", PRINT_BOOL(entry->lcount_set));
	fprintf(f, "        lcount_reset: %s\n", 
	    PRINT_BOOL(entry->lcount_reset));
	fprintf(f, "        for_path: %s\n", PRINT_BOOL(entry->for_path));
	fprintf(f, "        hl_dir_lin: %llx\n", entry->hl_dir_lin);
	fprintf(f, "        offset: %llx\n", entry->hl_dir_offset);
	fprintf(f, "        excl_lcount: %d\n", entry->excl_lcount);
}

static void
print_migr_file_state(FILE *f, struct migr_file_state *fs)
{
	if (!fs) {
		fprintf(f, "  NULL\n");
		return;
	}
	fprintf(f, "  name: %s\n", fs->name);
	fprintf(f, "  fd: %d\n", fs->fd);
	fprintf(f, "  type: %d\n", fs->type);
	fprintf(f, "  lin: %llx\n", fs->st.st_ino);
	fprintf(f, "  mode: %d\n", fs->st.st_mode);
	fprintf(f, "  nlink: %d\n", fs->st.st_nlink);
	fprintf(f, "  owner: %d\n", fs->st.st_uid);
	fprintf(f, "  group: %d\n", fs->st.st_gid);
	fprintf(f, "  size: %lld\n", fs->st.st_size);
	fprintf(f, "  atime: %.24s (%lld)\n", ctime(&fs->st.st_atime), 
	    fs->st.st_atime);
	fprintf(f, "  mtime: %.24s (%lld)\n", ctime(&fs->st.st_mtime), 
	    fs->st.st_mtime);
	fprintf(f, "  cur_offset: %lld\n", fs->cur_offset);
}

static int
dump_status(void *ctx)
{
	char *output_file = NULL;
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct work_restart_state *work = NULL;
	struct rep_log_entry *rlog = NULL;
	void **pvec_item = NULL;
	struct migr_dir *mdir = NULL;
	int i = 0;
	int ret = 1;
	FILE *f = NULL;
	int tw_size = 0;

	asprintf(&output_file, "/var/tmp/pworker_status_%d.txt", getpid());
	
	f = fopen(output_file, "w");
	if (f == NULL) {
		log(ERROR,"Status generation failed: %s", strerror(errno));
		goto out;
	}

	fprintf(f, "SyncIQ job '%s' (%s)\n", pw_ctx->job_name,
	    pw_ctx->policy_id);
	
	fprintf(f, "\n****Global Context****\n");
	fprintf(f, "Worker ID: %d\n", pw_ctx->id);
	fprintf(f, "next_func: %p\n", pw_ctx->next_func);
	fprintf(f, "wait_state: %d\n", pw_ctx->wait_state);
	fprintf(f, "dir_fd: %d\n", pw_ctx->dir_fd);
	fprintf(f, "sent_dir_lin: %llx\n", pw_ctx->sent_dir_lin);
	fprintf(f, "sent_filelin: %llx\n", pw_ctx->sent_filelin);
	fprintf(f, "cwd: %s\n", pw_ctx->cwd.path);
	fprintf(f, "done: %d\n", pw_ctx->done);
	fprintf(f, "dir_lin: %lld\n", pw_ctx->dir_lin);
	fprintf(f, "filelin: %lld\n", pw_ctx->filelin);
	fprintf(f, "pending_done_msg: %s\n", 
	    PRINT_BOOL(pw_ctx->pending_done_msg));
	fprintf(f, "pending_work_req: %s\n", 
	    PRINT_BOOL(pw_ctx->pending_work_req));
	fprintf(f, "doing_ads: %s\n", PRINT_BOOL(pw_ctx->doing_ads));
	fprintf(f, "lastnet: %lld\n", pw_ctx->lastnet);
	fprintf(f, "wait_for_acks: %s\n", PRINT_BOOL(pw_ctx->wait_for_acks));
	fprintf(f, "doing_diff_sync: %s\n", 
	    PRINT_BOOL(pw_ctx->doing_diff_sync));
	fprintf(f, "cwd_is_new_on_sworker: %s\n", 
	    PRINT_BOOL(pw_ctx->cwd_is_new_on_sworker));
	fprintf(f, "outstanding_full_hashes: %d\n", 
	    pw_ctx->outstanding_full_hashes);
	fprintf(f, "outstanding_acks: %d\n", pw_ctx->outstanding_acks);
	fprintf(f, "tw_sending_file: %s\n",
	    PRINT_BOOL(pw_ctx->tw_sending_file));
	fprintf(f, "full_transfer: %s\n", PRINT_BOOL(pw_ctx->full_transfer));
	fprintf(f, "stf_sync: %s\n", PRINT_BOOL(pw_ctx->stf_sync));
	fprintf(f, "stf_upgrade_sync: %s\n", 
	    PRINT_BOOL(pw_ctx->stf_upgrade_sync));
	fprintf(f, "lin_map_blk: %s\n", PRINT_BOOL(pw_ctx->lin_map_blk));
	fprintf(f, "work: %p\n", pw_ctx->work);
	if (pw_ctx->work) {
		work = pw_ctx->work;
		fprintf(f, "  rt: %d\n", work->rt);
		fprintf(f, "  lin: %llx\n", work->lin);
		fprintf(f, "  dircookie1: %llx\n", work->dircookie1);
		fprintf(f, "  dircookie2: %llx\n", work->dircookie2);
		fprintf(f, "  num1: %u\n", work->num1);
		fprintf(f, "  den1: %u\n", work->den1);
		fprintf(f, "  num2: %u\n", work->num2);
		fprintf(f, "  den2: %u\n", work->den2);
		fprintf(f, "  min_lin: %llx\n", work->min_lin);
		fprintf(f, "  max_lin: %llx\n", work->max_lin);
		fprintf(f, "  wl_cur_level: %u\n", work->wl_cur_level);
		fprintf(f, "  tw_blk_cnt: %u\n", work->tw_blk_cnt);
	}
	fprintf(f, "wl_log:\n");
	fprintf(f, "  index: %d\n", pw_ctx->wl_log.index);
	fprintf(f, "  lin:\n");
	for (i = 0; i < MAX_BLK_OPS; i++) {
		fprintf(f, "    %d: %llx\n", i, pw_ctx->wl_log.entries[i].lin);
		fprintf(f, "    %d: %d\n", i,
		    pw_ctx->wl_log.entries[i].operation);
	}
	fprintf(f, "rep_log:\n");
	fprintf(f, "  index: %d\n", pw_ctx->rep_log.index);
	fprintf(f, "  entries:\n");
	for (i = 0; i < MAX_BLK_OPS; i++) {
		rlog = &pw_ctx->rep_log.entries[i];
		if (rlog->lin != 0) {
			fprintf(f, "    [%d] \n", i);
			fprintf(f, "      lin: %llx\n", rlog->lin);
			fprintf(f, "      is_new: %d\n", rlog->oper);
			fprintf(f, "      set_value: %s\n",
			    PRINT_BOOL(rlog->set_value));
			fprintf(f, "      old_entry:\n");
			print_rep_entry(f, &rlog->old_entry);
			fprintf(f, "      new_entry:\n");
			print_rep_entry(f, &rlog->new_entry);
		} else {
			fprintf(f, "    [%d] {EMPTY} \n", i);
		}
	}
	fprintf(f, "cur_snap_root_lin: %llu\n", pw_ctx->cur_snap_root_lin);
	fprintf(f, "domain_id: %lld\n", pw_ctx->domain_id);
	fprintf(f, "domain_generation: %d\n", pw_ctx->domain_generation);
	fprintf(f, "treewalk: \n");
	fprintf(f, "  _path: %s\n", PRINT_NULL(pw_ctx->treewalk._path.path));
	fprintf(f, "  _thawed_dirs: %d\n", pw_ctx->treewalk._thawed_dirs);
	tw_size = pvec_size(&pw_ctx->treewalk._dirs);
	fprintf(f, "  _dirs size: %d\n", tw_size);
	if (tw_size > 0) {
		fprintf(f, "  _dirs:\n");
		fprintf(f, "    ---\n");
		PVEC_FOREACH(pvec_item, &pw_ctx->treewalk._dirs) {
			mdir = (struct migr_dir *)*pvec_item;
			fprintf(f, "    ---\n");
			print_migr_dir(f, mdir, "    ");
		}
	}
	
	fprintf(f, "dir_state:\n");
	print_migr_dir(f, pw_ctx->dir_state.dir, "  ");
	fprintf(f, "file_state:\n");
	print_migr_file_state(f, &pw_ctx->file_state);
	fprintf(f, "ads_dir_state:\n");
	print_migr_dir(f, pw_ctx->ads_dir_state.dir, "  ");
	fprintf(f, "ads_file_state:\n");
	print_migr_file_state(f, &pw_ctx->ads_file_state);
	
	print_net_state(f);

	print_cpu_throttle_state(f, &pw_ctx->cpu_throttle);
	
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


void
send_stats(struct migr_pworker_ctx *pw_ctx)
{
	if (pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync ||
	    !pw_ctx->stf_sync) {
		ilog(IL_INFO, "%s called, send_stats: sending treewalk stats",__func__); ///TRACE -> INFO
		send_pworker_tw_stat(pw_ctx->coord, pw_ctx->tw_cur_stats,
		    pw_ctx->tw_last_stats, pw_ctx->sent_dir_lin,
		    pw_ctx->sent_filelin, pw_ctx->job_ver.common);
	} else {
		ilog(IL_INFO, "%s called, send_stats: sending STF stats", __func__); ///TRACE -> INFO
		send_pworker_stf_stat(pw_ctx->coord, pw_ctx->stf_cur_stats,
		    pw_ctx->stf_last_stats, pw_ctx->job_ver.common);
	}
}

void
send_work_request(struct migr_pworker_ctx *pw_ctx, bool initial)
{
	struct generic_msg msg;
	uint64_t total_netstat;

        if (pw_ctx->split_req_pending) {
                pw_ctx->split_req_pending = false;
                pw_ctx->split_wi_lin = 0;
        }

	ASSERT(!pw_ctx->pending_work_req);
	pw_ctx->pending_work_req = true;
	total_netstat = migr_get_send_netstat() + migr_get_recv_netstat();

	ilog(IL_INFO, "%s called, send_work_request", __func__); ///TRACE -> INFO

	msg.head.type = WORK_REQ_MSG;
	msg.body.work_req.errors = pw_ctx->errors;
	msg.body.work_req.dirs = pw_ctx->dirs;
	msg.body.work_req.files = pw_ctx->files_count;
	msg.body.work_req.transfered = pw_ctx->transfered;
	msg.body.work_req.net = (total_netstat - pw_ctx->lastnet);
	if (!pw_ctx->assess) {
		pw_ctx->tw_cur_stats->bytes->transferred = total_netstat;
		pw_ctx->tw_cur_stats->bytes->network->total = 
		    pw_ctx->stf_cur_stats->bytes->network->total =
		    total_netstat;
		pw_ctx->tw_cur_stats->bytes->network->to_target =
		    pw_ctx->stf_cur_stats->bytes->network->to_target =
		    migr_get_send_netstat();
		pw_ctx->tw_cur_stats->bytes->network->to_source =
		    pw_ctx->stf_cur_stats->bytes->network->to_source =
		    migr_get_recv_netstat();
	}
	msg.body.work_req.deleted = 0;
	msg.body.work_req.purged = 0;
	msg.body.work_req.initial = initial;

	if (!initial)
		send_stats(pw_ctx);
	else {
		/* Init statistics */
		init_statistics(&pw_ctx->tw_cur_stats);
		init_statistics(&pw_ctx->tw_last_stats);
		init_statistics(&pw_ctx->stf_cur_stats);
		init_statistics(&pw_ctx->stf_last_stats);
		if (!pw_ctx->assess) {
			pw_ctx->tw_cur_stats->bytes->transferred =
			    total_netstat;
			pw_ctx->tw_last_stats->bytes->transferred =
			    total_netstat;
			pw_ctx->tw_cur_stats->bytes->network->total =
			    pw_ctx->stf_cur_stats->bytes->network->total =
			    total_netstat;
			pw_ctx->tw_last_stats->bytes->network->total =
			    pw_ctx->stf_last_stats->bytes->network->total =
			    total_netstat;
			pw_ctx->tw_cur_stats->bytes->network->to_target = 
			    pw_ctx->stf_cur_stats->bytes->network->to_target = 
			    migr_get_send_netstat();
		    	pw_ctx->tw_last_stats->bytes->network->to_target =
		    	    pw_ctx->stf_last_stats->bytes->network->to_target =
			    migr_get_send_netstat();
			pw_ctx->tw_cur_stats->bytes->network->to_source = 
			    pw_ctx->stf_cur_stats->bytes->network->to_source = 
			    migr_get_recv_netstat();
			pw_ctx->tw_last_stats->bytes->network->to_source =
			    pw_ctx->stf_last_stats->bytes->network->to_source =
			    migr_get_recv_netstat();
		}
	}
	msg_send(pw_ctx->coord, &msg); ////pworker ->wokr_req_msg   coord

	pw_ctx->sent_dir_lin = 0;
	pw_ctx->sent_filelin = 0;
	pw_ctx->errors = pw_ctx->dirs = pw_ctx->files_count =
	    pw_ctx->transfered = 0;
	pw_ctx->lastnet = total_netstat;
}

/* 
 * Update
 * tw_cur_stats.files.replicated.selected
 * tw_cur_stats.files.replicated.transferred
 * tw_cur_stats.files.deleted.src
 * tw_cur_stats.files.skipped.up_to_date
 * tw_cur_stats.bytes.transferred
 * tw_cur_stats.bytes.recoverable
 * tw_cur_stats.bytes.recovered.src
 * fields of stat, when assess flag is set
 */
void
assess_update_stat(struct migr_pworker_ctx *pw_ctx, bool not_sent,
    struct stat* st, struct migr_file_state *file)
{
	bool is_ads = st->st_flags & UF_ADS;
	uint64_t filesize = st->st_size;

	log(TRACE, "assess_update_stat");

	if (not_sent) {
		pw_ctx->tw_cur_stats->files->skipped->up_to_date++;
		goto out;
	}

	if (pw_ctx->action != SIQ_ACT_SYNC &&
	    pw_ctx->action != SIQ_ACT_REPLICATE)
		goto out;

	pw_ctx->tw_cur_stats->files->replicated->selected++;
	pw_ctx->tw_cur_stats->bytes->transferred += filesize;
	pw_ctx->tw_cur_stats->bytes->data->total += filesize;
	pw_ctx->tw_cur_stats->bytes->data->file += filesize;

	if (file->type == SIQ_FT_SYM) {
		pw_ctx->tw_cur_stats->files->replicated->symlinks++;
		pw_ctx->tw_cur_stats->files->replicated->new_files++;
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
	} else if (file->type == SIQ_FT_SOCK) {
		pw_ctx->tw_cur_stats->files->replicated->sockets++;
		pw_ctx->tw_cur_stats->files->replicated->new_files++;
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
	} else if (file->type == SIQ_FT_BLOCK) {
		pw_ctx->tw_cur_stats->files->replicated->block_specs++;
		pw_ctx->tw_cur_stats->files->replicated->new_files++;
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
	} else if (file->type == SIQ_FT_CHAR) {
		pw_ctx->tw_cur_stats->files->replicated->char_specs++;
		pw_ctx->tw_cur_stats->files->replicated->new_files++;
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
	} else if (file->prev_snap != INVALID_SNAPID && !is_ads) {
		pw_ctx->tw_cur_stats->files->replicated->updated_files++;
	} else if (!is_ads) {
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
		pw_ctx->tw_cur_stats->files->replicated->new_files++;
	}

out:
	return;
}

void
tw_request_new_work(struct migr_pworker_ctx *pw_ctx)
{
	struct isi_error *error = NULL;

	log(COPY, "Get next job chunk");

	flush_burst_buffer(pw_ctx);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	flush_cpss_log(&pw_ctx->cpss_log, pw_ctx->cur_cpss_ctx, &error);
	if (error)
		goto out;

	free_work_item(pw_ctx, pw_ctx->cur_rep_ctx, &error);
	if (error)
		goto out;
	
	/*
	 * Clear out the treewalk state to prevent any further recovery
	 * state being written while waiting for a new work item.
	 */
	pw_ctx->dir_state.dir = NULL;
	migr_tw_clean(&pw_ctx->treewalk);

	if (pw_ctx->dir_fd != -1) {
		close(pw_ctx->dir_fd);
		pw_ctx->dir_fd = -1;
	}

	migr_wait_for_response(pw_ctx, MIGR_WAIT_DONE);
	send_work_request(pw_ctx, 0);
out:
	if (error) {
		handle_stf_error_fatal(pw_ctx->coord, error,
		    E_SIQ_WORK_CLEANUP, EL_SIQ_SOURCE, pw_ctx->wi_lin,
		    pw_ctx->wi_lin);
		isi_error_free(error);
	}
}

int
migr_continue_work(void *ctx)
{
	static int op_count = 0;
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;
	int siq_error;
	struct migr_dirent *last_md = NULL;
	struct migr_dirent *last_md_ads = NULL;
	char filename[MAXNAMELEN];

	if (pw_ctx->state == SIQ_JS_PAUSED || get_bandwidth() == 0 ||
	    pw_ctx->udelay == 0)
		return 0;

	UFAIL_POINT_CODE(continue_work_pause,
		siq_nanosleep(RETURN_VALUE, 0);
	);

	UFAIL_POINT_CODE(exit_after_ops,
		if (op_count >= RETURN_VALUE)
			log(FATAL, "Exiting after %d ops", op_count);
	);

	UFAIL_POINT_CODE(force_worker_exit,
		process_gensig_exit(ctx);
	);

	op_count++;

	if (pw_ctx->next_func)
		(*pw_ctx->next_func)(pw_ctx, &error);

	UFAIL_POINT_CODE(post_work_error_noioerror,
		error = isi_system_error_new(errno, "Artificial error "
		    "generated by post_work_error failpoint");
	);
	UFAIL_POINT_CODE(post_work_error_ioerror,
		error = isi_system_error_new(errno, "Artificial error "
		    "generated by post_work_error failpoint");
		ioerror(pw_ctx->coord, "",
			NULL, 0, 0, "Dummy failure");
	);

	/*
	 * XXXJDH: This is the central point of failure.  Need to have cool
	 * error handling here to send the actual IO errors and detect
	 * whether to continue or bail now.
	 */
	if (error) {
		/*
		 * Don't dump exit codes when errors are expected
		 */
		if (!pw_ctx->expected_dataloss)
			dump_exitcode_ring(isi_error_to_int(error));

		if (!is_treewalk(pw_ctx) && pw_ctx->work != NULL) {
			siq_error = map_work_to_error(pw_ctx->work->rt);
			handle_stf_error_fatal(pw_ctx->coord, error, siq_error,
			    EL_SIQ_SOURCE, pw_ctx->wi_lin, pw_ctx->work->lin);
		}
		else if (pw_ctx->expected_dataloss) {
			/*
			 * bail out
			 */
			isi_error_free(error);
			error = NULL;
			if (!pw_ctx->doing_ads && (pw_ctx->dir_state.dir->_buffers == NULL)) {
				/*
				 * We failed while doing fchdir or fstat for the directory.
				 * We push a new directory in the tree and we call migr_start_work ()
				 * to process the directory. In the migr_start_work () we do
				 * fchdir and fstat before even we call readdirplus(), we might fail
				 * during fchdir or fstat. Handle this corrupted directory here.
				 * Log directory path in the log file and remove
				 * directory from the tree.
				 */
				log_corrupt_file_entry (pw_ctx, "");
				migr_call_next(pw_ctx, migr_finish_reg_dir);
			}
			else if (!pw_ctx->doing_ads && (pw_ctx->dir_state.dir->_buffers->_count)) {
				/*
				 * it means we have some dirents and we are here
				 * because one of dirent processing is failed
				 * log current dirent and skip it, go to next
				 * dirent if one present.
				 * At this point we don`t know if ads or reg dir
				 * so just invoke common function.
				 * with below call, we are back to our cycle
				 * we read next dirent and continue
				 */
				last_md = pw_ctx->dir_state.dir->_last_read_md;
				if (last_md) {
					sprintf(filename, "/%s", last_md->dirent->d_name);
					log_corrupt_file_entry (pw_ctx, filename);
				}
				migr_call_next(pw_ctx, migr_start_ads_or_reg_dir);
			}
			else if(pw_ctx->doing_ads && pw_ctx->ads_dir_state.dir->_buffers->_count) {
				/*
				 * it means we have ADS stream files and we are here
				 * because one of dirent processing is failed.
				 * log current dirent and skip it, go to next
				 * dirent if one present.
				 */

				last_md = pw_ctx->dir_state.dir->_last_read_md;
                                last_md_ads = pw_ctx->ads_dir_state.dir->_last_read_md;

				/*
				 * Skip ADS file. But where to go?
				 * We can have 2 scenarios,
				 */

				/*
				 * Scenario 1
				 * Regular file has an ADS directory
				 * We process Regular file first, send file begin message
				 * check if it has ADS directory, if yes than
				 * start processing for ADS directory by calling
				 * migr_continue_ads_dir_for_file ()
				 * last_md = Regular file
				 * last_md_ads = Corrupted ADS file inside ADS directory.
				 * Simply calling migr_continue_ads_dir_for_file ()
				 * will move to next ADS file and if no next dirent,
				 * it will call ADS finish dir and back to parent file.
				 */

				/*
				 * Scenario 2
				 * Directory has an ADS directory
				 * Get directory from tree, check if it has
				 * ADS directory, if yes than
				 * start processing for ADS directory by calling
				 * migr_continue_ads_dir_for_dir ()
				 * last_md = NULL since we have not started processing
				 * parent directory.
				 * last_md_ads = Corrupted ADS file inside ADS directory.
				 * Simply calling migr_continue_ads_dir_for_dir ()
				 * will move to next ADS file and if no next dirent,
				 * it will call ADS finish dir and bacl to parent directory.
				 */

				if (last_md) {
					/* Scenario 1 */
					migr_call_next(pw_ctx, migr_continue_ads_dir_for_file);
					sprintf(filename, "/%s:%s", last_md->dirent->d_name,
						last_md_ads->dirent->d_name);
				}
				else {
					/* Scenario 2 */
					migr_call_next(pw_ctx, migr_continue_ads_dir_for_dir);
					sprintf(filename, ":%s", last_md_ads->dirent->d_name);

					/*
					 * We should always process ADS directory first for
					 * a directory i.e when dir->visits=1
					 * We are here since we are processing ADS directory, so
					 * better increment visits so that we accidentally
					 * never call ADS processing again for the same
					 * directory.
					 */
					pw_ctx->dir_state.dir->visits++;
				}
				log_corrupt_file_entry (pw_ctx, filename);
			} else {
				/* we should not be here */
				ASSERT(0);
			}
			migr_reschedule_work(pw_ctx);
		} else {
			/* Just die for now. */
			errno = EIO;
			ioerror(pw_ctx->coord, "", NULL, 0, 0, "%s",
			    isi_error_get_message(error));
			log(FATAL, "Error message: %s",
			    isi_error_get_message(error));
			isi_error_free(error);
		}
	} else if (pw_ctx->next_func)
		migr_reschedule_work(pw_ctx);

	return 0;
}

void
migr_reschedule_work(struct migr_pworker_ctx *pw_ctx)
{
	struct timeval timeout = { 0, 0 };
	migr_register_timeout(&timeout, migr_continue_work, pw_ctx);
}

void
add_split_work_entry(struct migr_pworker_ctx *pw_ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, struct isi_error **error_out) {

	struct isi_error *error = NULL;

	/* 
	 * First check the connection with coordinator before
	 * adding any new workitems. There could be race where
	 * worker is about to add work entry but coordinator 
	 * reset the worker due to target group change.
	 * In those case its possible for coordinator to
	 * not pickup the split entry.
	 */
	check_connection_with_peer(pw_ctx->coord, true, 1, &error);
	if (error) {
		log(FATAL, "%s", isi_error_get_message(error));
	}

	/* Add new work item and update current work item. This will fail if
	 * our coordinator has died and a new coordinator has incremented our
	 * on-disk work item's num_coord_restarts field. */
	update_work_entries_cond(pw_ctx->cur_rep_ctx, p_lin, parent,
	    p_tw_data, p_tw_size, c_lin, child, c_tw_data, c_tw_size,
	    true, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);

}

static void
save_split_recovery_state(struct migr_pworker_ctx *pw_ctx,
    ifs_lin_t split_wi_lin, struct migr_work_desc *initial,
    struct path *path, char *selectors, bool full_transfer,
    struct migr_work_desc *split, struct work_restart_state *child,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_tw split_tw = {};
	char *p_tw_data = NULL, *c_tw_data = NULL;
	size_t p_tw_size = 0, c_tw_size = 0;

	migr_tw_init(&split_tw, pw_ctx->cur_snap_root_fd, path, &split->slice,
	    selectors, full_transfer, true, &error);
	if (error)
		goto out;

	p_tw_data = migr_tw_serialize(&pw_ctx->treewalk, &p_tw_size);
	c_tw_data = migr_tw_serialize(&split_tw, &c_tw_size);
	ASSERT(pw_ctx->wi_lin > 0 && split_wi_lin > 0);

	add_split_work_entry(pw_ctx, pw_ctx->wi_lin,
	    pw_ctx->work, p_tw_data, p_tw_size, split_wi_lin,
	    child, c_tw_data, c_tw_size, &error);

	if (error)
		goto out;

out:
	migr_tw_clean(&split_tw);

	if (p_tw_data)
		free(p_tw_data);
	if (c_tw_data)
		free(c_tw_data);
	isi_error_handle(error, error_out);
}

/*
 * Update the current dirs checkpoint to one cookie before the current file.
 * If the current file is only file in the directory, do nothing, as the
 * checkpoint is already set to the beginning of the slice.
 */
uint64_t
set_fsplit_tw_chkpt(struct migr_pworker_ctx *pw_ctx)
{
	uint64_t chkpt_cookie = -1;
	struct migr_dir *curr_dir = pw_ctx->dir_state.dir;
	struct migr_file_state *curr_file = &pw_ctx->file_state;

	ASSERT(pw_ctx->outstanding_acks == 0);

	if (!is_treewalk(pw_ctx))
		return chkpt_cookie;

	if (curr_dir == NULL)
		return chkpt_cookie;

	/* Get the cookie before the current file. */
	chkpt_cookie = migr_dir_get_prev_cookie(curr_dir, curr_file->cookie);
	if (chkpt_cookie != -1 &&
	    chkpt_cookie != curr_dir->_slice.begin &&
	    chkpt_cookie != curr_dir->_checkpoint) {
		log(DEBUG, "%s: %llx", __func__, chkpt_cookie);
		migr_dir_set_checkpoint(curr_dir, chkpt_cookie);
		pw_ctx->last_chkpt_time = time(NULL);
	}

	return chkpt_cookie;
}

/*
 * This function determines if the given file is eligible to be split.
 * If the file can be split, the new work item and current work item are
 * initialized/updated with an appropriate range of the file to transfer.
 */
bool
init_file_split_work(struct migr_pworker_ctx *pw_ctx,
    struct work_restart_state *new_work, struct migr_file_state *file)
{
	struct work_restart_state *work = pw_ctx->work;
	uint64_t mid = 0, low = 0, high = 0, prog = 0;
	bool success = false;


	log(TRACE, "%s", __func__);

	if (file->fd < 0 || file->type != SIQ_FT_REG)
		goto out;

	low = work->f_low;
	prog = file->cur_offset;
	high = is_fsplit_work_item(work) ? work->f_high : file->st.st_size;

	log(TRACE, "Trying file split on lin=%llx "
	    " low=%llu, prog=%llu, high=%llu %d",
	    file->st.st_ino, low, prog, high, is_fsplit_work_item(work));

	ASSERT(prog >= low);
	ASSERT(high >= prog);

	log(TRACE, "Trying file split on lin=%llx"
	    " low=%llu, prog=%llu, high=%llu",
	    file->st.st_ino, low, prog, high);

	if ((high - prog) >= MIN_FILE_SPLIT_SIZE) {
		mid = prog + (high - prog) / 2;	
	} else {
		goto out;
	}

	ASSERT(mid > prog);
	ASSERT(mid < high);

	bzero(new_work, sizeof(struct work_restart_state));

	/* Update file split fields */
	work->f_high = mid;
	new_work->f_low = mid;
	new_work->f_high = high;
	work->fsplit_lin = new_work->fsplit_lin = file->st.st_ino;

	/* 
	 * Copy the work item, the new work item should behave exactly like
	 * the donating work item with the exception of file ranges
	 */
	new_work->rt = work->rt;
	new_work->dircookie1 = work->dircookie1;
	new_work->dircookie2 = work->dircookie2;
	new_work->num1 = work->num1;
	new_work->den1 = work->den1;
	new_work->num2 = work->num2;
	new_work->den2 = work->den2;
	new_work->wl_cur_level = work->wl_cur_level;
	new_work->tw_blk_cnt = work->tw_blk_cnt;

	/* Bug 138002 - Adjust the lin range to ensure that the filesplit
	 * work item begins work on the large file lin. */
	new_work->lin = new_work->chkpt_lin = new_work->min_lin = work->lin;
	new_work->max_lin = work->max_lin;

	/* Bug 162293 - Update our checkpoint to this file. We know we have
	 * sent and acked every other file if we're here. */
	work->chkpt_lin = work->lin;

	success = true;

out:
	return success;
}

/*
 * This function attempts to  split the current file in a work item. If the
 * split is successful, the current work item is modified, a new work item is
 * created, and both are saved.
 */
bool
try_file_split(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct work_restart_state *work = pw_ctx->work;
	struct migr_file_state *file = &pw_ctx->file_state;
	struct work_restart_state new_work = {};
	struct worm_state worm = {};
	struct generic_msg ack_msg;
	uint64_t chkpt_cookie = -1;
	char *tw_data = NULL;
	size_t tw_size = 0;
	bool success = false, have_worm = false;

	log(TRACE, "%s", __func__);

	UFAIL_POINT_CODE(disable_file_splitting, goto out;);

	if (file->fd < 0 || file->type != SIQ_FT_REG)
		goto out;

	/* Don't split worm committed files */
	have_worm = get_worm_state(file->st.st_ino, pw_ctx->cur_snap, &worm,
	    NULL, &error);
	if (error)
		goto out;

	if (have_worm && worm.w_committed)
		goto out;

	/* Don't split cloudpools stub files */
	/* XXXDPL It may be possible to file split for deep copy. Would we
	   want to? */
	if (file->st.st_flags & SF_FILE_STUBBED) {
		log(DEBUG, "Wont try file split on stub lin=%llx", 
		    file->st.st_ino);
		goto out;
	}

	log(DEBUG, "Trying file split. Outstanding acks: %d",
	    pw_ctx->outstanding_acks);

	/* Don't split if we have outstanding acks, we want to make sure that
	 * every file up to this large file has been successfully sent. If we
	 * have outstanding acks, ask the sworker to dump all of its acks. */
	if (pw_ctx->outstanding_acks) {
		if (pw_ctx->lin_map_blk) {
			log(DEBUG, "Sending lin_map_blk_msg");
			bzero(&ack_msg, sizeof(ack_msg));
			ack_msg.head.type = LIN_MAP_BLK_MSG;
			msg_send(pw_ctx->sworker, &ack_msg);
		} else if (!is_treewalk(pw_ctx) && pw_ctx->prev_lin) {
			log(DEBUG, "Sending stf_chkpt_msg");
			bzero(&ack_msg, sizeof(ack_msg));
			ack_msg.head.type = STF_CHKPT_MSG;
			ack_msg.body.stf_chkpt.slin = pw_ctx->prev_lin;
			ack_msg.body.stf_chkpt.oper_count =
			    pw_ctx->outstanding_acks;
			msg_send(pw_ctx->sworker, &ack_msg);
		}
		pw_ctx->fsplit_acks_pending = true;
		goto out;
	}

	success = init_file_split_work(pw_ctx, &new_work, file);

	/* Add new work item and update current work item */
	if (success) {
		/*
		 * For treewalk, add the new work entry with an identical
		 * treewalk struct as the donating pworker.
		 * Otherwise, NULL tw_data is fine, STF doesn't need it.
		 * Checkpoint at the last successfully sent file, we know it
		 * was received by the sworker because of our outstanding acks
		 * check.
		 */
		if (is_treewalk(pw_ctx)) {
			chkpt_cookie = set_fsplit_tw_chkpt(pw_ctx);
			if (chkpt_cookie == -1) {
				log(DEBUG, "Failed to find a valid file "
				    "split checkpoint. Denying file split.");
				success = false;
				goto out;
			}
			tw_data = migr_tw_serialize(&pw_ctx->treewalk,
			    &tw_size);
		}

		add_split_work_entry(pw_ctx, pw_ctx->wi_lin,
		    work, tw_data, tw_size, pw_ctx->split_wi_lin,
		    &new_work, tw_data, tw_size, &error);

		if (error)
			goto out;

		log(NOTICE, "Successful file split[%s], lin=%llx, two ranges "
		    "are current work(%llu) f_low=%llu prog=%llu f_high=%llu, "
		    "new work(%llu) f_low=%llu f_high=%llu",
		    work_type_str(work->rt), file->st.st_ino, pw_ctx->wi_lin,
		    work->f_low, file->cur_offset, work->f_high,
		    pw_ctx->split_wi_lin, new_work.f_low, new_work.f_high);
	}

out:
	if (tw_data)
		free(tw_data);

	isi_error_handle(error, error_out);
	return success;
}

bool
try_tw_split(struct migr_pworker_ctx *pw_ctx, bool *last_file,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct path path = {};
	struct migr_dir *split_dir = NULL;
	struct migr_work_desc initial, split;
	enum migr_split_result result;
	struct work_restart_state *work = pw_ctx->work, new_work;
	struct migr_tw *tw = &pw_ctx->treewalk;
	char *selectbuf = NULL;
	int selectlen = 0;
	bool succeeded = false;

	log(TRACE, "%s", __func__);

	migr_tw_get_work_desc(&pw_ctx->treewalk, &initial);

	result = migr_tw_split(&pw_ctx->treewalk, &path, &split, &split_dir,
	    &error);

	if (error) {
		/*
		 * An error performing a split isn't serious enough to justify
		 * killing the job. Send a split failed/unavailable response.
		 */
		log(ERROR, "migr_tw_split failed with error: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		error = NULL;
	}

	succeeded = (result == MIGR_SPLIT_SUCCESS);

	if (succeeded) {
		struct migr_work_desc after;

		ASSERT(split_dir);

		if (split_dir->_select_tree)
			selectbuf = sel_export(split_dir->_select_tree,
			    &selectlen);

		/* Bug 189993 - Initialize the split work item. */
		bzero(&new_work, sizeof(struct work_restart_state));
		new_work.rt = pw_ctx->work->rt;
		new_work.lin = pw_ctx->work->lin;
		new_work.dircookie1 = pw_ctx->work->dircookie1;

		save_split_recovery_state(pw_ctx, pw_ctx->split_wi_lin,
		    &initial, &path, selectbuf, pw_ctx->full_transfer, &split,
		    &new_work, &error);

		if (error) {
			/* PSCALE-11307
			 * When tw splitting in DomainMark job, the job may fail
			 * as the directory state changes on Head (snapshot) in code path
			 * try_work_split -> try_tw_split -> save_split_recovery_state
			 * -> migr_tw_init, with error msg
			 * "Unable to split work item <> to new work item <>, Local error:
			 * Failed to get fd for dir <> under ifs fd 9 (migr_tw_init):
			 * No such file or directory".
			 *
			 * In this case, the splitted wi is not flushed to disk yet.
			 * We cannot just ignore the error and continue in tw splitting,
			 * but we can restart the wi instead of having the job fail.
			 */
			if (work->rt == WORK_DOMAIN_MARK && pw_ctx->cur_snap == HEAD_SNAPID &&
				isi_system_error_is_a(error, ENOENT)) {
					log(FATAL, "Failure during split from "
					"current work(%{}) to new work item(%{})",
					lin_fmt(pw_ctx->wi_lin), lin_fmt(pw_ctx->split_wi_lin));
			}
			goto out;
		}

		migr_tw_get_work_desc(&pw_ctx->treewalk, &after);

		log(NOTICE, "Successful split[%s], two ranges are "
		    " current work(%llu) range=%{}/%{}, "
		    "new work item(%llu) split_range=%{}/%{}",
		    work_type_str(work->rt), pw_ctx->wi_lin,
		    lin_fmt(split_dir->_stat.st_ino),
		    dir_slice_fmt(&split_dir->_slice),
		    pw_ctx->split_wi_lin, lin_fmt(split.lin),
		    dir_slice_fmt(&split.slice));

		ASSERT(split.slice.begin < split.slice.end);
		ASSERT(after.slice.begin < after.slice.end);

		ASSERT(after.lin != split.lin ||
		    after.slice.begin >= split.slice.end ||
		    after.slice.end <= split.slice.begin);
	} else if (result == MIGR_SPLIT_UNAVAILABLE &&
	    		work->rt == WORK_SNAP_TW &&
			!migr_tw_has_unread_dirents(tw, &error)) {
		*last_file = true;
	}

out:
	if(selectbuf)
		free(selectbuf);
	isi_error_handle(error, error_out);
	return succeeded;
}

void
try_work_split(struct migr_pworker_ctx *pw_ctx)
{
	struct work_restart_state *work = pw_ctx->work;
	struct generic_msg m = {};
	struct isi_error *error = NULL;
	bool found_split = false, last_file = false;

	ASSERT(work != NULL || pw_ctx->wi_lin == 0);

	UFAIL_POINT_CODE(dont_split, goto out);

	if (pw_ctx->wi_lin == 0) {
		/* No work left, so just return */
		goto out;
	}

	/* flush repstate log before splits */
	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;


	switch (work->rt) {
	case WORK_SUMM_TREE:
		found_split = try_snapset_split(pw_ctx, &error);
		break;

	case WORK_CC_DIR_DEL:
	case WORK_CC_DIR_CHGS:
	case WORK_CT_LIN_UPDTS:
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
	case WORK_FLIP_LINS:
	case WORK_DATABASE_CORRECT:
	case WORK_DATABASE_RESYNC:
	case WORK_CT_STALE_DIRS:
	case WORK_CT_LIN_HASH_EXCPT:
	case WORK_CC_COMP_RESYNC:
	case WORK_COMP_CONFLICT_CLEANUP:
	case WORK_COMP_CONFLICT_LIST:
	case WORK_CT_COMP_DIR_LINKS:
		found_split = try_btree_split(pw_ctx, &last_file, &error);
		break;

	case WORK_CC_DIR_DEL_WL:
	case WORK_CC_DIR_CHGS_WL:
	case WORK_CC_PPRED_WL:
		found_split = try_worklist_split(pw_ctx, &error);
		break;

	case WORK_IDMAP_SEND:
		break;

	case WORK_SNAP_TW:
	case WORK_DOMAIN_MARK:
		found_split = try_tw_split(pw_ctx, &last_file, &error);
		break;
	
	case WORK_COMP_COMMIT:
	case WORK_COMP_MAP_PROCESS:
	case WORK_CT_COMP_DIR_DELS:
		found_split = try_comp_target_split(pw_ctx, &error);
		break;

	default:
		ASSERT(0);
	}

	if (error)
		goto out;


	if (last_file && pw_ctx->do_file_split &&
	    !pw_ctx->fsplit_acks_pending) {
		log(DEBUG, "Last file detected");
		found_split = try_file_split(pw_ctx, &error);
		goto out;
	}

out:
	if (error) {
		handle_stf_error_fatal(pw_ctx->coord, error, E_SIQ_WORK_SPLIT,
		    EL_SIQ_SOURCE, pw_ctx->wi_lin, pw_ctx->split_wi_lin);
		return;
	} else if (work && (work->rt == WORK_COMP_COMMIT ||
	    work->rt == WORK_COMP_MAP_PROCESS ||
	    work->rt == WORK_CT_COMP_DIR_DELS)) {
		//WORK_COMP_COMMIT, WORK_COMP_MAP_PROCESS
		//and WORK_CT_COMP_DIR_DELS does things
		//differently - split message will be sent after 
		//we hear back from the target side
		return;
	}

	pw_ctx->split_req_pending = false;
	pw_ctx->split_wi_lin = 0;
	m.head.type = SPLIT_RESP_MSG;
	m.body.split_resp.succeeded = found_split;
	msg_send(pw_ctx->coord, &m);
}

int
split_req_callback(struct generic_msg *srm, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct generic_msg lin_map_blk_msg = {};

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->split_req_pending == false);
	ASSERT(srm->body.split_req.split_wi_lin > 0);

	pw_ctx->split_wi_lin = srm->body.split_req.split_wi_lin;

	/* We don't have to wait for a dir ack to start sending a file split
	 * work item */
	if (pw_ctx->work && is_fsplit_work_item(pw_ctx->work))
		pw_ctx->skip_checkpoint = false;

	if (pw_ctx->skip_checkpoint) {
		/* related to bug 96259. dirs are not acked by the sworker,
		 * so we don't want to checkpoint (side effect of split)
		 * while we're uncertain whether one or more dirs have been
		 * created on the target. postpone the split until we're
		 * safe to checkpoint */
		pw_ctx->split_req_pending = true;
		log(DEBUG, "Postponing split request");

		/* flush the sworkers lin map, causing it to send an ack. This
		 * way we don't have to wait until it fills up to do a split.
		 */
		if (pw_ctx->lin_map_blk && pw_ctx->outstanding_acks != 0 ) {
			lin_map_blk_msg.head.type = LIN_MAP_BLK_MSG;
			UFAIL_POINT_CODE(split_req_lmap_updates,
			    lin_map_blk_msg.body.lin_map_blk.lmap_updates = -1;
			);
			msg_send(pw_ctx->sworker, &lin_map_blk_msg);
		}
	} else
		try_work_split(pw_ctx);

	return 0;
}

static int
auth_callback(struct generic_msg *msg, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	char *secret;

	ilog(IL_INFO, "auth_callback called"); //TRACE -> INFO
	if (!msg->body.auth.salt) {
		log(NOTICE, "Starting worker %d on '%s' with %s",
		    pw_ctx->id, pw_ctx->job_name, pw_ctx->sworker_host);
		send_work_request(pw_ctx, 1);
		return 0;
	}

	memcpy(pw_ctx->passwd, &msg->body.auth.salt, sizeof(int));
	secret = MD5Data(pw_ctx->passwd, sizeof(int) + strlen(pw_ctx->passwd +
	    sizeof(int)), 0);

	free(pw_ctx->passwd); pw_ctx->passwd = NULL;
	log(DEBUG, "Generated secret of %s", secret);

	msg->body.auth.salt = 0;
	msg->body.auth.pass = secret;
	msg_send(pw_ctx->sworker, msg); ////pworker -> sworker 发送auth_msg
	flush_burst_buffer(pw_ctx);

	return 0;
}

static int
error_callback(struct generic_msg *msg, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;

	if (pw_ctx->checkpoints)
		send_stats(pw_ctx);
	msg_send(pw_ctx->coord, msg);
	return 0;
}

static int
position_callback(struct generic_msg *msg, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;

	log(TRACE, "position_callback");

	msg->body.position.filelin = pw_ctx->filelin;
	msg->body.position.dir = pw_ctx->cwd.utf8path;
	msg->body.position.dirlin = pw_ctx->dir_lin;
	msg_send(pw_ctx->coord, msg);

	return 0;
}

static int
bandwidth_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	int ration = (int)m->body.bandwidth.ration;

	log(DEBUG, "bandwidth_callback: ration updated from %d to %d kbps",
	    get_bandwidth(), ration);
	if (get_bandwidth() == 0 && ration != 0)
		migr_reschedule_work(pw_ctx);
	set_bandwidth(ration);

	return 0;
}

static int
throttle_callback(struct generic_msg *m, void *ctx)
{
	const int MAX_SENSIBLE_GRANT = USECS_PER_SEC;
	struct migr_pworker_ctx *pw_ctx = ctx;
	int ration = (int)m->body.throttle.ration;
	bool need_reschedule = false;

	log(DEBUG, "throttle_callback: ration updated from %d to %d fps",
	    pw_ctx->files_per_sec, ration);

	need_reschedule = (pw_ctx->files_per_sec == 0 && ration != 0);
	pw_ctx->files_per_sec = ration;

	if (ration < 0 || ration > MAX_SENSIBLE_GRANT) {
		/* Treat as infinite */
		pw_ctx->udelay = -1;
	} else if (ration == 0) {
		/* No ration */
		pw_ctx->udelay = 0;
	} else {
		/* Convert ration file/sec to usecs/file */
		pw_ctx->udelay = USECS_PER_SEC / ration;
	}

	if (need_reschedule)
		migr_reschedule_work(pw_ctx);

	return 0;
}

static void
set_tw_chkpt_exiting(struct migr_pworker_ctx *pw_ctx)
{
	uint64_t chkpt_cookie = -1;
	struct migr_dir *curr_dir = pw_ctx->dir_state.dir;
	struct migr_file_state *curr_file = &pw_ctx->file_state;

	if (curr_dir == NULL)
		return;

	/* Get the cookie before the current file. */
	chkpt_cookie = migr_dir_get_prev_cookie(curr_dir, curr_file->cookie);
	if (chkpt_cookie != -1 &&
	    chkpt_cookie != curr_dir->_slice.begin &&
	    chkpt_cookie > curr_dir->_checkpoint) {
		log(DEBUG, "%s: %llx", __func__, chkpt_cookie);
		migr_dir_set_checkpoint(curr_dir, chkpt_cookie);
		pw_ctx->last_chkpt_time = time(NULL);
	}

	return;
}

static void
exit_if_pending(void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter", __func__);

	if (migr_is_waiting_for(pw_ctx, MIGR_WAIT_PENDING_EXIT) &&
	    ((pw_ctx->outstanding_acks == 0) ||
	    (pw_ctx->outstanding_acks == 1 && pw_ctx->tw_sending_file))) {
		//This seems to set the correct checkpoint for all tw syncs
		if (is_treewalk(pw_ctx)) {
			log(TRACE, "%s: updating tw checkpoint", __func__);
			set_tw_chkpt_exiting(pw_ctx);
		}
		if (is_fsplit_work_item(pw_ctx->work)) {
			log(TRACE, "%s: updating f_low", __func__);
			pw_ctx->work->f_low = pw_ctx->file_state.cur_offset;
		}
		check_connection_with_peer(pw_ctx->coord, true, 1, &error);
		if (error) {
			//Skip timer callback
			log(INFO, "%s: failed check_connection_with_peer",
			    __func__);
			isi_error_free(error);
		} else {
			timer_callback(ctx);
		}
		log(INFO, "Exiting via signal from coordinator");
		exit(EXIT_SUCCESS);
	}
}

/*
 * Acknowledgement message checkpointing progress in a directory and confirming
 * transmission.
 */
static int
ack_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct migr_dir *dir;
	struct cache_entry *entry, *dummy;

	log(TRACE, "%s: filename %s with lin %016llx/%016llx", __func__,
	    m->body.ack3.file, m->body.ack3.src_dlin, m->body.ack3.src_dkey);

	ASSERT(pw_ctx->outstanding_acks > 0);
	pw_ctx->outstanding_acks--;

	log(TRACE, "%s: acks now outstanding = %d", __func__, 
	    pw_ctx->outstanding_acks);

	dir = migr_tw_find_dir(&pw_ctx->treewalk, m->body.ack3.src_dlin);
	if (dir == NULL) {
		log(NOTICE, "Unable to find directory LIN %016llx for "
		    "ACK of %s", m->body.ack3.src_dlin, m->body.ack3.file);
		goto out;
	}

	pw_ctx->skip_checkpoint = false;

	/* handle pending split request now that we are able to checkpoint */
	if (!migr_is_waiting_for(pw_ctx, MIGR_WAIT_PENDING_EXIT) &&
	    pw_ctx->split_req_pending) {
		log(DEBUG, "Attempting split for pending request");
		try_work_split(pw_ctx);
	}

	if (!pw_ctx->doing_diff_sync) {
		migr_dir_set_checkpoint(dir, m->body.ack3.src_dkey);
		pw_ctx->last_chkpt_time = time(NULL);
		pw_ctx->tw_cur_stats->files->replicated->transferred++;
	} else {
		entry = cache_find_by_dkey(pw_ctx, m->body.ack3.src_dkey);
		ASSERT(entry->cache_state == WAITING_FOR_ACK);
		set_cache_state(entry, ACK_RECEIVED);
		if (entry->sync_state == NEEDS_SYNC)
			pw_ctx->tw_cur_stats->files->replicated->transferred++;

		/* Go through the cache (which is in order of files read
		 * from directory) and checkpoint any that needs 
		 * checkpointing as long as all files before it need 
		 * checkpointing. This prevents gaps in setting the 
		 * checkpoint */
		log(TRACE, "%s: Checking cache states:", __func__);
		TAILQ_FOREACH_SAFE(entry, &pw_ctx->cache, ENTRIES, 
		    dummy) {
			log(TRACE, "%s:        %s = %s", __func__,
			    FNAME(entry->md),
			    cache_state_str(entry->cache_state));
			if (entry->cache_state != ACK_RECEIVED)
				break;
			migr_dir_set_checkpoint(dir, entry->md->cookie);
			pw_ctx->last_chkpt_time = time(NULL);
			set_cache_state(entry, CHECKPOINTED);
			cache_entry_cleanup(pw_ctx, dir, entry);
		}
	}

	if (pw_ctx->outstanding_acks == 0) {
		if (migr_is_waiting_for(pw_ctx, MIGR_WAIT_PENDING_EXIT)) {
			goto out;
		}
		if (migr_is_waiting_for(pw_ctx, MIGR_WAIT_ACK_UP)) {
			migr_call_next(pw_ctx, migr_finish_reg_dir);
			migr_reschedule_work(pw_ctx);
			siq_track_op_end(SIQ_OP_FILE_ACKS);
		} else if (pw_ctx->wait_for_acks) {
			pw_ctx->wait_for_acks = false;

			/* 
			 * We were waiting to recurse down into a new directory
			 * but needed to be synchronous before doing so.  No
			 * outstanding acks means we've reached that point.
			 * The new directory is still sitting in the treewalk
			 * object so we need to continue walking the current
			 * dir to pull it off and start working.
			 */
			migr_call_next(pw_ctx, migr_continue_reg_dir);
			migr_reschedule_work(pw_ctx);
			siq_track_op_end(SIQ_OP_FILE_ACKS);
		}
	} else if (pw_ctx->doing_diff_sync)
		make_sure_cache_is_processed(pw_ctx);
out:
	exit_if_pending(ctx);
	return 0;
}

static int
list_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct generic_msg mclone;
	int size;

	log(TRACE, "list_callback");
	mclone.head.type = LIST_MSG;
	mclone.body.list.last = m->body.list.last;
	mclone.body.list.used = m->body.list.used;
	mclone.body.list.list = (char *)get_data_offset(pw_ctx->sworker,
	    &size);
	mclone.body.list.list += sizeof(mclone.body.list.last);

	if (size < mclone.body.list.used) {
		log(ERROR, "Not enough room to pack list (%d > %d)",
		    mclone.body.list.used, size);
	}

	memcpy(mclone.body.list.list, m->body.list.list, m->body.list.used);
	msg_send(pw_ctx->coord, &mclone);
	return 0;
}

/* Forward stat msg (tw or stf) from sworker to coord */
static int
sworker_stat_callback(struct generic_msg *m, void *ctx)
{
	log(TRACE, "sworker_stat_callback");
	msg_send(((struct migr_pworker_ctx *)ctx)->coord, m);
	return 0;
}

static int
jobctl_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	enum siq_job_state state;

	log(TRACE, "jobctl_callback");
	state = m->body.job_ctl.state;

	if (!(state == SIQ_JS_PAUSED || state == SIQ_JS_RUNNING) ||
	    !(pw_ctx->state == SIQ_JS_PAUSED || 
	    pw_ctx->state == SIQ_JS_RUNNING)) {
		log(ERROR, "Invalid jobctl msg %d", state);
		goto out;
	}

	if (state != pw_ctx->state) {
		pw_ctx->state = state;
		log(NOTICE, "Job %s %s", pw_ctx->job_name,
		    state == SIQ_JS_PAUSED ? "paused" : "resumed");

		if (pw_ctx->state == SIQ_JS_RUNNING)
			migr_reschedule_work(pw_ctx);
	}

out:
	return 0;
}

void
set_hash_exception(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin,
    struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	bool success = false;
	struct rep_ctx *rep;
	struct rep_entry old_entry, new_entry, *oldp = NULL;
	bool found = false;
	
	rep = pw_ctx->cur_rep_ctx;

	/*
	 * Set the job phase entry to note that we need to 
	 * execute hash exception phase
	 */
	if (!pw_ctx->hash_exception) {
		do {
			success = stf_job_phase_cond_enable(
			    STF_JOB_PHASE_LIN,
			    STF_PHASE_CT_LIN_HASH_EXCPT, rep,
			    &error);
			if (error)
				goto out;
		} while (!success);
		pw_ctx->hash_exception = true;
	}

	do {
		found = get_entry(lin, &old_entry, rep, &error);
		if (error)
			goto out;
		ASSERT(found == true || !pw_ctx->stf_sync);
		
		if (found) {
			new_entry = old_entry;
			oldp = &old_entry;
		} else {
			new_entry.lcount = 1;
			new_entry.changed = true;
			oldp = NULL;
		}		
		new_entry.exception = true;
		success = set_entry_cond(lin, &new_entry, rep, !found,
		    oldp, 0, NULL, NULL, 0, &error);
		if (error)
			goto out;
	} while (!success);
	
	pw_ctx->stf_cur_stats->change_transfer->hash_exceptions_found++;

out:
	isi_error_handle(error, error_out);
}

static int
lin_ack_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct work_restart_state *work = pw_ctx->work;
	struct isi_error *error = NULL;
	struct rep_ctx *rep;
	struct rep_entry old_entry, *oldp;
	struct rep_entry new_entry = {};
	bool found, success = false;
	
	log(TRACE, "lin_ack_callback");

	pw_ctx->skip_checkpoint = false;
	pw_ctx->fsplit_acks_pending = false;

	/* handle pending split request now that we are able to checkpoint */
	if (!migr_is_waiting_for(pw_ctx, MIGR_WAIT_PENDING_EXIT) &&
	    pw_ctx->split_req_pending) {
		log(DEBUG, "Attempting split for pending request");
		try_work_split(pw_ctx);
	}

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;
	
	rep = pw_ctx->cur_rep_ctx;
	/* Check for negative ack for hash mismatch error */
	if (m->body.lin_ack.result &&
	    m->body.lin_ack.siq_err == E_SIQ_BBD_CHKSUM) {
		set_hash_exception(pw_ctx, m->body.lin_ack.src_lin, &error);	
		goto out;
	}

	if (m->body.lin_ack.result &&
	    m->body.lin_ack.siq_err == E_SIQ_LINK_EXCPT) {
		
		if (!pw_ctx->link_exception) {
			do {
				success = stf_job_phase_cond_enable(
				    STF_JOB_PHASE_LIN,
				    STF_PHASE_CT_DIR_LINKS_EXCPT, rep,
				    &error);
				if (error)
					goto out;
			} while (!success);
			pw_ctx->link_exception = true;
		}

		do {
			found = get_entry(m->body.lin_ack.src_lin, &old_entry,
			    rep, &error);
			if (error)
				goto out;
			ASSERT(found == true);
			new_entry = old_entry;
			oldp = &old_entry;
			new_entry.exception = true;
			success = set_entry_cond(m->body.lin_ack.src_lin,
			    &new_entry, rep, !found, oldp, 0, NULL, NULL,
			    0, &error);
			if (error)
				goto out;
		} while (!success);

		goto out;
	}
	
	/* A positive ack */
	if (!m->body.lin_ack.result) {
		work->chkpt_lin = m->body.lin_ack.src_lin;
		ASSERT(work->chkpt_lin >= work->min_lin,
		  "chkpt_lin: %{}, min_lin: %{}", lin_fmt(work->chkpt_lin),
		    lin_fmt(work->min_lin));
		ASSERT(work->chkpt_lin < work->max_lin,
		    "chkpt_lin: %{}, max_lin: %{}", lin_fmt(work->chkpt_lin),
		    lin_fmt(work->max_lin));
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		   rep, &error);
		if (error)
			goto out;
		ASSERT(pw_ctx->outstanding_acks >= m->body.lin_ack.oper_count);
		pw_ctx->outstanding_acks -= m->body.lin_ack.oper_count;
		exit_if_pending(ctx);
		if (pw_ctx->outstanding_acks < STF_ACK_INTERVAL &&
		    (pw_ctx->wait_state == MIGR_WAIT_STF_LIN_ACK)) {
			if (work->rt == WORK_FLIP_LINS ||
			    work->rt == WORK_DATABASE_CORRECT ||
			    work->rt == WORK_DATABASE_RESYNC) {
				migr_call_next(pw_ctx,
				    migr_continue_flip_lins);
			} else if (work->rt == WORK_CT_COMP_DIR_LINKS) {
				/*
				 * Bug 246376
				 * Sworker may send lin ack while pworker hasn't
				 * finished the comp dir link phase. This is
				 * possible in following situation:
				 * - pworker has reached it's lin ack
				 * interval and is waiting for ack from sworker,
				 * - sworker hasn't reached it's threashold when
				 * it sends ack message.
				 */
				migr_call_next(pw_ctx,
				    migr_continue_regular_comp_lin_links);
			} else {
				migr_call_next(pw_ctx,
				    stf_continue_change_transfer);
			}
			migr_reschedule_work(pw_ctx);
		}
	}

out:
	if (error) {
		/* XXX TBD XXX */
		/* Need to identify retry and non retry errors */
		log(FATAL, "Error during ack processing for lin %llu",
		    m->body.lin_ack.src_lin);
	}
	return 0;
}

static int
done_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;


	ASSERT(pw_ctx->pending_done_msg);
	pw_ctx->pending_done_msg = false;

	log(TRACE, "done_callback");
	pw_ctx->checkpoints -= m->body.done.count;
	if (pw_ctx->done && !pw_ctx->checkpoints) {
		log(TRACE, "done_callback: get next job chunk");

		if (pw_ctx->lmap_mirror_ctx) {
			close_linmap(pw_ctx->lmap_mirror_ctx);
			pw_ctx->lmap_mirror_ctx = NULL;
			pw_ctx->mirror_check  = false;
		}

		if (pw_ctx->stf_sync &&
		    !(pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync)) {
			free_work_item(pw_ctx, pw_ctx->chg_ctx->rep2, &error);
			if (error) {
				/* Do we need to send error back to coord */
				log(FATAL, "Error while finishing work : %s",
				    isi_error_get_message(error));
				isi_error_free(error);
				error = NULL;
			}
			pw_ctx->outstanding_acks = 0;
			send_work_request(pw_ctx, false);
		} else {
			if (pw_ctx->logdeletes)
				listflush(&pw_ctx->deleted_files, LIST_TO_LOG,
				    pw_ctx->coord);

			tw_request_new_work(pw_ctx);
		}
	} else if (!pw_ctx->done) {
		log(TRACE, "done_callback: not all data was sent");
	} else {
		ASSERT(!pw_ctx->pending_done_msg);
		pw_ctx->pending_done_msg = true;

		log(TRACE, "done_callback: repeat DONE_MSG");
		m->head.type = DONE_MSG;
		msg_send(pw_ctx->sworker, m);
		flush_burst_buffer(pw_ctx);
	}
	send_stats(pw_ctx);
	return 0;
}

static int
disconnect_callback(struct generic_msg *m, void *ctx)
{
	log(INFO, "Disconnect from %s", m->body.discon.fd_name);
	struct migr_pworker_ctx *pw_ctx = ctx;
	if(pw_ctx && pw_ctx->corrupt_file) {
		log(INFO, "Closing worker`s corrupt file descriptor");
		fclose(pw_ctx->corrupt_file);
		pw_ctx->corrupt_file = NULL;
	}
	log_siq_ops();
	exit(EXIT_SUCCESS);
}

int
process_gensig_exit(void *ctx)
{
	struct generic_msg ack_msg = {};
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter", __func__);

	// Early sanity check
	if (!pw_ctx) {
		goto out;
	}

	// If we are doing worklists, save our current location
	if (pw_ctx->chg_ctx && pw_ctx->chg_ctx->wl_iter) {
		flush_stf_logs(pw_ctx, &error);
		if (error) {
			log(ERROR, "%s: failed to flush_stf_logs: %s",
			    __func__, isi_error_get_message(error));
			isi_error_free(error);
			goto out;
		}
		wl_checkpoint(&pw_ctx->wl_log,
		   pw_ctx->chg_ctx->wl_iter,
		   pw_ctx->work->dircookie1,
		   pw_ctx->work->dircookie2, &error);
		if (error) {
			log(ERROR, "%s: failed to wl_checkpoint: %s",
			    __func__, isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			goto out;
		}
	}

	/*
	 * Just exit if we are doing diff sync or if we are doing file splitting
	 * AND we are not doing bulk linmap updates
	 *
	 * Also just exit if we are doing WORK_DATABASE_RESYNC(Bug 259348)
	 * or WORK_FLIP_LINS(PSCALE-13853)
	 */
	if (!pw_ctx->work || pw_ctx->doing_diff_sync ||
	    (!pw_ctx->lin_map_blk && pw_ctx->do_file_split) ||
	    pw_ctx->work->rt == WORK_DATABASE_RESYNC ||
	    pw_ctx->work->rt == WORK_FLIP_LINS) {
	    goto out;
	}

	if (pw_ctx->outstanding_acks || pw_ctx->checkpoints) {
		log(DEBUG, "%s: outstanding acks", __func__);
		if (pw_ctx->lin_map_blk && !(pw_ctx->work->rt == WORK_CT_LIN_HASH_EXCPT)) {
			log(DEBUG, "%s: sending lin_map_blk_msg", __func__);
			ack_msg.head.type = LIN_MAP_BLK_MSG;
			msg_send(pw_ctx->sworker, &ack_msg);
		} else if ((!is_treewalk(pw_ctx) || pw_ctx->work->rt == WORK_CT_LIN_HASH_EXCPT)
			    && (pw_ctx->prev_lin != 0 || pw_ctx->cur_lin != 0)) {
			log(DEBUG, "%s: sending stf_chkpt_msg", __func__);
			ack_msg.head.type = STF_CHKPT_MSG;
			ack_msg.body.stf_chkpt.slin = pw_ctx->prev_lin != 0 ? pw_ctx->prev_lin : pw_ctx->cur_lin;
			ack_msg.body.stf_chkpt.oper_count =
			        pw_ctx->outstanding_acks;
			msg_send(pw_ctx->sworker, &ack_msg);
		} else {
			log(DEBUG, "%s: waiting for acks before exit", __func__);
		}
		migr_wait_for_response(pw_ctx, MIGR_WAIT_PENDING_EXIT);
		return 0;
	} else {
		log(DEBUG, "%s: no outstanding acks, exiting", __func__);
	}
	timer_callback(pw_ctx);

out:
	log(DEBUG, "%s: exiting", __func__);
	exit(EXIT_SUCCESS);
}

static int
general_signal_callback(struct generic_msg *m, void *ctx)
{
	enum general_signal sig = m->body.general_signal.signal;
	int ret = 0;

	switch (sig) {
	case SIQ_GENSIG_EXIT:
		log(INFO, "Received exit signal from coordinator");
		ret = process_gensig_exit(ctx);
		return ret;

	default:
		log(DEBUG, "Unexpected general signal %d", sig);
	}
	return 0;
}

static int
lin_map_blk_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct isi_error *error = NULL;
	log(TRACE, "%s", __func__);

	if (!pw_ctx->lin_map_blk && !pw_ctx->doing_diff_sync) {
		pw_ctx->lin_map_blk = true;
	} else {
		/* Seems like periodic lin map blk msg */
		pw_ctx->outstanding_acks -= m->body.lin_map_blk.lmap_updates;
		ASSERT(pw_ctx->outstanding_acks >= 0);

		/* It is safe to checkpoint, allow splits */
		pw_ctx->skip_checkpoint = false;
		pw_ctx->fsplit_acks_pending = false;

		exit_if_pending(ctx);

		/* handle pending split requests */
		if (pw_ctx->split_req_pending) {
			log(DEBUG, "Attempting split for pending request");
			try_work_split(pw_ctx);
		} else if (pw_ctx->initial_sync_empty_sub_dirs >=
		    MAX_EMPTY_SUB_DIRS) {
			flush_stf_logs(pw_ctx, &error);
			if (error != NULL)
				goto out;
			if (migr_tw_has_slices(&pw_ctx->treewalk))
				save_recovery_state(pw_ctx);
		}

		/* Bug 225582 - reset empty dir count */
		pw_ctx->initial_sync_empty_sub_dirs = 0;

		if (pw_ctx->outstanding_acks == 0) {
			if (migr_is_waiting_for(pw_ctx, MIGR_WAIT_ACK_UP)) {
				migr_call_next(pw_ctx, migr_finish_reg_dir);
				migr_reschedule_work(pw_ctx);
				siq_track_op_end(SIQ_OP_FILE_ACKS);
			} else if (pw_ctx->wait_for_acks) {
				pw_ctx->wait_for_acks = false;
				migr_call_next(pw_ctx, migr_continue_reg_dir);
				migr_reschedule_work(pw_ctx);
				siq_track_op_end(SIQ_OP_FILE_ACKS);
			}
		}
	}
out:
	if (error != NULL) {
		log(FATAL, "Error during linmap callback: %s",
		    isi_error_get_message(error));
	}
	return 0;
}

/*
 * Function to check if we need to consider a particular lin mapping
 * as part of linmap mirror. Input is repstate entry for a particular
 * lin mapping sent by the target. Based on the rep entry and current
 * work type, we decide whether lin mapping should be considered for
 * mirrored linmap or not.
 */
static bool
filter_lmap_updates(enum work_restart_type rt, struct rep_entry *entry,
    enum lmap_oper *oper)
{
	bool filtered = false;

	switch(rt) {

		case WORK_CT_FILE_DELS:
		case WORK_CT_DIR_DELS:
			ASSERT(entry->lcount == 0);
			*oper = LMAP_REM;
			break;
		case WORK_FLIP_LINS:
			if (entry->not_on_target) {
				ASSERT(entry->lcount > 0);
				*oper = LMAP_SET;
			} else {
				filtered = true;
			}
			break;
		case WORK_DATABASE_RESYNC:
			ASSERT(entry->lcount > 0);
			*oper = LMAP_SET;
			break;
		case WORK_DATABASE_CORRECT:
			if (entry->lcount == 0) {
				*oper = LMAP_SET;
			} else {
				filtered = true;
			}
			break;
		default:
			ASSERT(0);
	}
	return filtered;
}

static int
database_update_callback(struct generic_msg *m, void *ctx)
{
	struct database_updt_msg *dupdt = &m->body.database_updt;
	struct lmap_update *lmap_entries = 
	    (struct lmap_update *)m->body.database_updt.updt;
	struct migr_pworker_ctx *pw_ctx = ctx;
	int i, lmap_count;
	struct rep_entry entry = {};
	struct changeset_ctx *cc_ctx = pw_ctx->chg_ctx;
	struct work_restart_state *work = pw_ctx->work;
	enum lmap_oper lmap_oper;
	char lmap_name[MAXNAMELEN];
	char *policy_id = pw_ctx->policy_id;
	struct isi_error *error = NULL;
	bool found = false;
	struct rep_ctx *rep_ctx = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->curr_ver >= FLAG_VER_3_5);
	lmap_count = dupdt->updtlen / sizeof(struct lmap_update);
	ASSERT(lmap_count <= SBT_BULK_MAX_OPS);

	/*
	 * First check if the mirror lmap exists, if not
	 * then no need to make progress unless its resync
	 */
	get_mirrored_lmap_name(lmap_name, policy_id);
	if (work->rt == WORK_DATABASE_RESYNC && !pw_ctx->lmap_mirror_ctx) {
		create_linmap(policy_id, lmap_name, true, &error);
		if (error)
			goto out;
	}

	if (!pw_ctx->lmap_mirror_ctx && !pw_ctx->mirror_check) {
		found = linmap_exists(policy_id, lmap_name, &error);
		if (error)
			goto out;

		if (!found)
			goto out;

		open_linmap(policy_id, lmap_name, &pw_ctx->lmap_mirror_ctx,
		    &error);
		if (error)
			goto out;

		pw_ctx->mirror_check = true;
	}

	if (!pw_ctx->lmap_mirror_ctx)
		goto out;

	if (work->rt == WORK_DATABASE_RESYNC && !is_treewalk(pw_ctx)) {
		ASSERT(cc_ctx != NULL);
		rep_ctx = cc_ctx->rep1;
	} else {
		rep_ctx = pw_ctx->cur_rep_ctx;
	}

	for (i = 0; i < lmap_count; i++) {
		found = get_entry(lmap_entries[i].slin, &entry, rep_ctx,
		    &error);
		if (error)
			goto out;
		if (found != true) {
			error = isi_system_error_new(ENOENT,
			    "Unable to find rep entry for lin %llx",
			    lmap_entries[i].slin);
			goto out;
		}

		if (!filter_lmap_updates(work->rt, &entry, &lmap_oper)) {
			lmap_log_add_entry(pw_ctx->lmap_mirror_ctx,
			    &pw_ctx->lmap_mirror_log, lmap_entries[i].dlin,
			    lmap_entries[i].slin, lmap_oper, 0, &error);
			if (error)
				goto out;
		}
	}
	flush_lmap_log(&pw_ctx->lmap_mirror_log, pw_ctx->lmap_mirror_ctx,
	    &error);
	if (error)
		goto out;
out:	
	if (error) {
		log(FATAL, "Error during linmap mirror update of entries "
		    "%llx : %s", lmap_entries[0].slin,
		    isi_error_get_message(error));
	}
	return 0;
}

static void
init_diff_sync(struct generic_msg *m, struct migr_pworker_ctx *pw_ctx,
    uint32_t loglevel)
{
	log(DEBUG, "%s: Doing diff sync", __func__);
	pw_ctx->doing_diff_sync = true;
	diff_sync_init(pw_ctx);

	if (pw_ctx->job_ver.common < MSG_VERSION_RIPTIDE) {
		m->body.old_work_init6.min_hash_piece_len =
		    pw_ctx->min_full_hash_piece_len;
		m->body.old_work_init6.hash_pieces_per_file =
		    pw_ctx->hash_pieces_per_file;
	} else {
		ript_msg_set_field_uint64(&m->body.ript,
		    RMF_MIN_HASH_PIECE_LEN, 1,
		    pw_ctx->min_full_hash_piece_len);
		ript_msg_set_field_uint32(&m->body.ript,
		    RMF_HASH_PIECES_PER_FILE, 1,
		    pw_ctx->hash_pieces_per_file);
	}

	pw_ctx->hasher = fork_hasher(PWORKER, pw_ctx, pw_ctx->cur_snap_root_fd,
	    pw_ctx->hash_type, pw_ctx->min_full_hash_piece_len,
	    pw_ctx->hash_pieces_per_file, loglevel,
	    g_siq_conf->root->bwt->cpu_port);

	migr_register_callback(pw_ctx->sworker,
	    SHORT_HASH_RESP_MSG, short_hash_resp_callback);

	migr_register_callback(pw_ctx->hasher,
	    FULL_HASH_FILE_RESP_MSG, full_hash_resp_callback);

	migr_register_callback(pw_ctx->sworker,
	    FULL_HASH_FILE_RESP_MSG, full_hash_resp_callback);

	migr_register_callback(pw_ctx->hasher,
	    HASH_ERROR_MSG, pw_hash_error_callback);

	migr_register_callback(pw_ctx->sworker,
	    HASH_ERROR_MSG, sw_hash_error_callback);

	migr_register_callback(pw_ctx->sworker,
	    ACK_DIR_MSG, ack_dir_callback);

	migr_register_callback(pw_ctx->hasher,
	    DISCON_MSG, disconnect_callback);
}

static int
burst_sock_callback(struct generic_msg *m, void *ctx)
{
	int ret;
	socklen_t len;
	int burst_fd;
	struct isi_error *error = NULL;
	struct migr_pworker_ctx *pw_ctx = ctx;
	burst_fd_t sender;
	struct sockaddr_in peer_sockaddr;
	struct sockaddr_in local_sockaddr;
	uint16_t burst_port_min = 
	    g_siq_conf->root->coordinator->ports->burst_send_port_min;
	uint16_t burst_port_max = 
	    g_siq_conf->root->coordinator->ports->burst_send_port_max;
	uint16_t port = 0;
	int mem_constraint = 
	    g_siq_conf->root->application->burst_mem_constraint;
	rde_status_t rv;

	log(TRACE, "burst_sock_callback");
	ASSERT(m->body.burst_sock.port > 0);

	len = sizeof(local_sockaddr);
	ret = getsockname(pw_ctx->sworker, (struct sockaddr *)&local_sockaddr,
	    &len);
	if (ret == -1) {
		error = isi_system_error_new(errno, "getsockname failed");
		goto out;
	}
#if 0
	/* We dont support burst for IPV6 */
	if (local_sockaddr.sin_family == AF_INET6) {
		error = isi_system_error_new(ENOTSUP,
		    "BURST socket mode not supported for IPV6 address");
		goto out;
	}
#endif
	/* Get the peer IP address from control channel */
	len = sizeof(peer_sockaddr);
	ret = getpeername(pw_ctx->sworker, (struct sockaddr *)&peer_sockaddr,
	    &len);
	if (ret == -1) {
		error = isi_system_error_new(errno, "getpeername failed");
		goto out;
	}
	
	rv = getHostInterfaceImpl(&pw_ctx->burst_host);
	if (rv != RDE_STATUS_OK) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "burst_socket logging init failed");
		goto out;
	}
	ewoc_init(&pw_ctx->burst_host->_p);
	ewoc_setTraceLevel(ewoc_Debug);
	burst_startup();

	sender = burst_socket(mem_constraint);
	ASSERT(sender >= 0);

	port = burst_port_min;
	/* If we need to bound to one of the local ports, try it */
	while(port && (port <= burst_port_max)) {
		local_sockaddr.sin_port = htons(port);
		ret = burst_bind(sender, (burst_sockaddr_t*)&local_sockaddr,
		    sizeof(local_sockaddr));
		if (ret == -1 && errno != EADDRINUSE) {
			error = isi_system_error_new(errno,
			    "burst_bind failed");
			goto out;
		} else if (ret == 0) {
			break;
		}
		port++;
	}

	if (port && (port > burst_port_max)) {
		error = isi_system_error_new(errno, 
		    "burst_bind failed: Could not find unused port");
		goto out;
	}

	peer_sockaddr.sin_port = htons(m->body.burst_sock.port);
	ret = burst_connect(sender, (burst_sockaddr_t*) &peer_sockaddr,
	    sizeof(peer_sockaddr));
	if (ret == -1) {
		log (FATAL, "Unable to connect using burst : %s",
		    strerror(errno));
	}

	burst_fd = burst_get_udp_fd(sender);
	ASSERT(burst_fd > 0);	
	migr_add_fd(burst_fd, pw_ctx, "sworker_burst");
	migr_fd_set_burst_sock(burst_fd, sender,
	    m->body.burst_sock.max_buf_size);

	/*Switch the socket fd so that all future calls will see burst */
	pw_ctx->tcp_sworker = pw_ctx->sworker;
	pw_ctx->sworker = burst_fd;
	
	log(DEBUG, "Burst connection successful");

out:
	if (error) {
		log(FATAL, "Error while negotiating burst mode : "
		    "%s", isi_error_get_message(error));
	}

	return 0;
}

/**
 * Connect to the sworker and register the appropriate callbacks.
 */
static void
init_remote_connection(struct migr_pworker_ctx *pw_ctx)
{
	int buff_size = SOCK_BUFF_SIZE;
	int tmp_size;
	uint32_t ver = 0;
	struct isi_error *error = NULL;

	pw_ctx->sworker = connect_to(pw_ctx->sworker_host,
	    g_siq_conf->root->coordinator->ports->sworker, POL_DENY_FALLBACK,
	    pw_ctx->job_ver.common, &ver, pw_ctx->forced_addr, true, &error);

	if (pw_ctx->sworker == -1) {
		/*
		 * Bug 166691
		 * don't log msg_handshake socket errors
		 */
		if (ver != SHAKE_FAIL)
			log(ERROR, "Failed to connect to target worker "
			    "%d (%s): %s",
			    pw_ctx->id, pw_ctx->sworker_host, error ?
			    isi_error_get_message(error) : "<unknown>");
		exit(EXIT_FAILURE);
	}

	ASSERT(ver == pw_ctx->job_ver.common);

	migr_add_fd(pw_ctx->sworker, pw_ctx, "sworker");

	migr_register_callback(pw_ctx->sworker, AUTH_MSG,
	    auth_callback);
	migr_register_callback(pw_ctx->sworker, ERROR_MSG,
	    error_callback);
	migr_register_callback(pw_ctx->sworker, SIQ_ERROR_MSG,
	    error_callback);
	migr_register_callback(pw_ctx->sworker, POSITION_MSG,
	    position_callback);
	migr_register_callback(pw_ctx->sworker, ACK_MSG3,
	    ack_callback);
	migr_register_callback(pw_ctx->sworker, OLD_SWORKER_TW_STAT_MSG,
	    sworker_stat_callback);
	migr_register_callback(pw_ctx->sworker,
	    build_ript_msg_type(SWORKER_TW_STAT_MSG), sworker_stat_callback);
	migr_register_callback(pw_ctx->sworker, OLD_SWORKER_STF_STAT_MSG,
	    sworker_stat_callback);
	migr_register_callback(pw_ctx->sworker,
	    build_ript_msg_type(SWORKER_STF_STAT_MSG), sworker_stat_callback);
	migr_register_callback(pw_ctx->sworker, LIST_MSG,
	    list_callback);
	migr_register_callback(pw_ctx->sworker, DISCON_MSG,
	    disconnect_callback);
	migr_register_callback(pw_ctx->sworker, DONE_MSG,
	    done_callback);
	migr_register_callback(pw_ctx->sworker, LIN_ACK_MSG,
	    lin_ack_callback);
	migr_register_callback(pw_ctx->sworker, EXTRA_FILE_COMMIT_MSG,
	    extra_file_commit_callback);
	migr_register_callback(pw_ctx->sworker, LIN_MAP_BLK_MSG,
	    lin_map_blk_callback);
	migr_register_callback(pw_ctx->sworker, DATABASE_UPDT_MSG,
	    database_update_callback);
	migr_register_callback(pw_ctx->sworker, BURST_SOCK_MSG,
	    burst_sock_callback);
	if (pw_ctx->compliance_v2) {
		log(TRACE, "%s: adding compliance target callbacks", __func__);
		migr_register_callback(pw_ctx->sworker,
		    build_ript_msg_type(COMP_TARGET_WORK_RANGE_MSG),
		    comp_target_work_range_callback);
		migr_register_callback(pw_ctx->sworker,
		    build_ript_msg_type(COMP_TARGET_CHECKPOINT_MSG),
		    comp_target_checkpoint_callback);
		migr_register_callback(pw_ctx->sworker,
		    build_ript_msg_type(COMP_MAP_MSG),
		    comp_map_callback);
	}

	if (g_siq_conf->root->coordinator->wsock_buf_size > 0) {
		buff_size = MAX(MIN_SOCK_BUFF_SIZE,
		    g_siq_conf->root->coordinator->wsock_buf_size);
		buff_size = MIN(SOCK_BUFF_SIZE, buff_size);
	}

	tmp_size = buff_size;
	/*Set maximum available TCP buff size for recv*/
	log(TRACE, "Set receive buffer size to maximum available "
	    "value");
	while (setsockopt(pw_ctx->sworker, SOL_SOCKET, SO_RCVBUF,
	    (void*)&tmp_size, sizeof(tmp_size)) < 0) {
		tmp_size /= 2;
		if (errno != ENOBUFS) {
			log(COPY, "setsockopt returns unexpected "
			"error. Errno = %d", errno);
		}
		if (tmp_size < MIN_SOCK_BUFF_SIZE) {
			log(FATAL, "Can't set TCP recv buffer size"
				"to big enough value");
		}
		log(TRACE, "Next try with %d KB", (tmp_size / 1024));
	}
	log(DEBUG, "TCP buffer for rcv was set to %d", tmp_size);

	tmp_size = buff_size;
	/*Set maximum available TCP buff size for recv*/
	log(TRACE, "Set send buffer size to maximum available "
	    "value");
	while (setsockopt(pw_ctx->sworker, SOL_SOCKET, SO_SNDBUF,
	    (void*)&tmp_size, sizeof(tmp_size)) < 0) {
		tmp_size /= 2;
		if (errno != ENOBUFS) {
			log(COPY, "setsockopt returns unexpected "
			"error. Errno = %d", errno);
		}
		if (tmp_size < MIN_SOCK_BUFF_SIZE) {
			log(FATAL, "Can't set TCP send buffer size"
				"to big enough value");
		}
		log(TRACE, "Next try with %d KB", (tmp_size / 1024));
	}
	log(DEBUG, "TCP buffer for snd was set to %d", tmp_size);
	if (g_siq_conf->root->coordinator->wread_buf_size > 0) {
		pw_ctx->read_size = MAX(pw_ctx->read_size,
		    g_siq_conf->root->coordinator->wread_buf_size);
		pw_ctx->read_size = MIN(IDEAL_READ_SIZE, pw_ctx->read_size);
	}
	log(DEBUG, "Read size set to %d ", pw_ctx->read_size); 
		    
	ASSERT(error == NULL);
}

void
send_err_msg(const struct migr_pworker_ctx *pw_ctx, const int err, 
    const char *errstr)
{
	struct generic_msg m = {};

	m.head.type = ERROR_MSG;
	m.body.error.error = err;
	m.body.error.io = 0;
	m.body.error.str = (char *)errstr; //XXXDPL HACK discards const.
	msg_send(pw_ctx->coord, &m);
}

static bool
lin_in_compliance_domain(const struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin, 
    struct isi_error **error_out)
{
	int ret = -1;
	bool res = false;

	struct domain_set doms;	
	ifs_domainid_t dummy = 0;

	struct isi_error *error = NULL;

	domain_set_init(&doms);

	ret = dom_get_info_by_lin(lin, pw_ctx->cur_snap, &doms, NULL, NULL);
	if (ret) {
		if (errno == ENOENT) {
			goto out;
		}

		error = isi_system_error_new(errno, 
		    "Could not get domain info for %llx", lin);
		goto out;
	}

	ret = dom_get_matching_domains(&doms, DOM_COMPLIANCE, 1, &dummy, NULL);
	if (ret < 0) {
		error = isi_system_error_new(errno, 
		    "Could not get domain info for %llx", lin);
		goto out;
	}

	if (ret > 0) {
		res = true;
	}
	
out:
	domain_set_clean(&doms);
	isi_error_handle(error, error_out);
	return res;
}

/** 
 * Check LIN to see if it is in a compliance domain. If so, fail policy if
 * compliance domain isn't rooted at the source root.
 */
void
check_for_compliance(const struct migr_pworker_ctx *pw_ctx, 
    const ifs_lin_t lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool lin_is_compliance = false;

	if (!pw_ctx->fail_on_compliance) {
		goto out;
	}

	/* /ifs is not allowed to have any domains */
	if (lin == ROOT_LIN) {
		goto out;
	}

	lin_is_compliance = lin_in_compliance_domain(pw_ctx, lin, 
	    &error);
	if (error) {
		goto out;
	}

	if (lin_is_compliance) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF, 
		    "SmartLock Compliance root directory (%llx) found "
		    "below root path. Exclude this directory from "
		    "the policy or create a policy with root directory "
		    "at the SmartLock Compliance root.", lin);			
		goto out;
	}
out:
	isi_error_handle(error, error_out);
}


/**
 * Check if source root is a WORM ancestor of any compliance domains. 
 * If so, fail if policy tries to sync any files in the compliance domain.
 */
static void
check_for_compliance_subdirs(struct work_init_msg *wim,
    struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	int ret = -1;
	struct domain_set subdoms = {};
	struct isi_error *error = NULL;
	struct path p = {};
	int root_fd = -1;
	int rv = 0;
	struct stat st;

	ifs_domainid_t dummy;

	domain_set_init(&subdoms);

	path_init(&p, wim->source_base, NULL);
	root_fd = fd_rel_to_ifs(pw_ctx->cur_snap_root_fd, &p);
	if (root_fd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open policy root directory : %s",
		    wim->source_base);
		goto out;
	}

	rv = fstat(root_fd, &st);
	if (rv == -1) {
		error = isi_system_error_new(errno,
		"Unable to stat root path %s", wim->source_base);
		goto out;
	}

	/*
	 * Store the root lin for future compliance related phases.
	 */
	pw_ctx->cur_snap_root_lin = st.st_ino;

	/* /ifs cannot have any domains set */
	if (st.st_ino == ROOT_LIN) {
		goto out;
	}

	ret = dom_get_info_by_path(wim->source_base, NULL,
	    &subdoms, NULL);
	if (ret) {
		error = isi_system_error_new(errno, 
		    "Could not get SmartLock ancestors for %s",
		    wim->source_base);
		goto out;
	}

	ret = dom_get_matching_domains(&subdoms, DOM_COMPLIANCE, 1, &dummy, 
	    NULL);	
	if (ret < 0) {
		error = isi_system_error_new(errno, 
		    "Could not get SmartLock settings for paths under %s",
		    wim->source_base);
		goto out;
	}

	if (ret > 0) {
		log(INFO, "Warning: SmartLock Compliance directories found "
		    "within the policy root path. SyncIQ cannot sync these "
		    "files. This policy will fail unless the SmartLock "
		    "Compliance directories are excluded.");
		pw_ctx->fail_on_compliance = true;
	}

out:
	if (root_fd != -1) {
		close(root_fd);
	}

	path_clean(&p);
	domain_set_clean(&subdoms);
	isi_error_handle(error, error_out);
}

/**
 * Setup interface restrictions, if any.
 */
static void
init_interface_restrictions(char *restrict_by, 
    struct migr_pworker_ctx *pw_ctx)
{
	struct isi_error *error = NULL;

	/* restrict_by is only set in work_init_msg4 if we're doing
	 * force_interface */
	if (restrict_by && *restrict_by) {
		pw_ctx->forced_addr = get_local_node_pool_addr(restrict_by,
		    &error);
		if (error) {
			/* We really would like to send this message to the
			 * coordinator. */
			log(FATAL,
			    "Pworker can't run in pool %s : %s", restrict_by,
			    isi_error_get_message(error));
		}
	}
}

static void
path_ctx_init_from_msg(struct work_init_msg *wim,
    struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	pw_ctx->cur_snap = wim->cur_snapid;
	/* fd of the artificial /ifs for the current snapshot */
	pw_ctx->cur_snap_root_fd =
	    ifs_lin_open(ROOT_LIN, pw_ctx->cur_snap, O_RDONLY);
	if (pw_ctx->cur_snap_root_fd < 0) {
		error = isi_system_error_new(errno, "Failed ifs_lin_open on "
		    "root lin at snapid %llu", pw_ctx->cur_snap);
		goto out;
	}

	pw_ctx->prev_snap = wim->prev_snapid;
	if (pw_ctx->prev_snap != INVALID_SNAPID) {
		log(DEBUG, "working with two snapshots");
		/* fd of the artificial /ifs for the previous snapshot */
		pw_ctx->prev_snap_root_fd =
		    ifs_lin_open(ROOT_LIN, pw_ctx->prev_snap, O_RDONLY);
		if (pw_ctx->prev_snap_root_fd < 0) {
			error = isi_system_error_new(errno,
			    "Failed ifs_lin_open on root lin at snapid %llu",
			    pw_ctx->prev_snap);
			goto out;
		}
	} else {
		log(DEBUG, "working with one snapshot");
		pw_ctx->prev_snap_root_fd = -1;
	}

	if (pw_ctx->restore) {
		log(NOTICE, "restore: from snapid: %llu to snapid: %llu",
		    pw_ctx->cur_snap, pw_ctx->prev_snap);
	} else {
		log(NOTICE, "prev snapid: %llu cur snapid: %llu",
		    pw_ctx->prev_snap, pw_ctx->cur_snap);
	}

out:
	isi_error_handle(error, error_out);
}

static void
basic_ctx_init_from_msg(struct work_init_msg *wim,
    struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_job_version job_ver, prev_job_ver;
        struct siq_source_record *srec = NULL;
	enum siq_job_action pending_job = SIQ_JOB_NONE;

	if (pw_ctx->action == SIQ_ACT_REMOVE)
		log(FATAL, "REMOVE job seen");
	if (pw_ctx->action == SIQ_ACT_MIGRATE)
		log(FATAL, "MIGRATE job seen");

	pw_ctx->flags = wim->flags;
	pw_ctx->action = wim->action;
	pw_ctx->assess = (bool)(wim->flags & FLAG_ASSESS);
	pw_ctx->initial_sync = (bool)(wim->flags & FLAG_CLEAR);
	pw_ctx->snapop = (bool)(wim->flags & FLAG_SNAPSHOT_PRI);
	pw_ctx->old_time = (time_t)(wim->old_time);
	pw_ctx->new_time = (time_t)(wim->new_time);
	pw_ctx->checksum_size =
	    (wim->flags & FLAG_NO_INTEGRITY) ? 0 : CHECKSUM_SIZE;
	pw_ctx->skip_bb_hash = wim->skip_bb_hash;
	pw_ctx->id = wim->worker_id;
	pw_ctx->mklinks = (bool)(wim->flags & FLAG_MKLINKS);
	pw_ctx->logdeletes = (bool)(wim->flags & FLAG_LOGDELETES);
	pw_ctx->system = (bool)(wim->flags & FLAG_SYSTEM);
	pw_ctx->is_local_operation = pw_ctx->assess;
	pw_ctx->do_file_split =
	    (bool)(wim->flags & FLAG_FILE_SPLIT);

	pw_ctx->expected_dataloss = 
	    (bool)(wim->flags & FLAG_EXPECTED_DATALOSS);
	pw_ctx->stf_sync = (bool)(wim->flags & FLAG_STF_SYNC);
	pw_ctx->restore = (bool)(wim->flags & FLAG_RESTORE);
	pw_ctx->compliance = (bool)(wim->flags & FLAG_COMPLIANCE);
	pw_ctx->compliance_v2 = (bool)(wim->flags & FLAG_COMPLIANCE_V2);
	pw_ctx->curr_ver = (wim->flags & (FLAG_VER_3_5 |
		FLAG_VER_CLOUDPOOLS));

	/* Starting with Riptide, use job_ver, not curr_ver. */
	ASSERT((pw_ctx->curr_ver & ~(FLAG_VER_3_5 | FLAG_VER_CLOUDPOOLS)) == 0);

	/* if this is an STF upgrade sync, we do some things to prepare for
	 * subsequent STF based syncs. send source/target LIN pairs for all
	 * files in the dataset whether or not the files need to be sent. */
	pw_ctx->stf_upgrade_sync =
	    (bool)(wim->flags & FLAG_STF_UPGRADE_SYNC);
	if (pw_ctx->stf_upgrade_sync) {
		ASSERT(pw_ctx->stf_sync);
		ASSERT(!pw_ctx->initial_sync);
	}

	pw_ctx->comp_v1_v2_patch = (bool)(wim->flags & FLAG_COMP_V1_V2_PATCH);

	/* Bug 178393 - ignore passwd file during recovery or domain mark */
	bool need_pass = wim->pass &&
	    ((wim->flags &
	    (FLAG_FAILBACK_PREP | FLAG_RESTORE | FLAG_DOMAIN_MARK)) == 0);
	pw_ctx->passwd = calloc(1, (need_pass ? strlen(wim->pass) + 1 : 1)
	    + sizeof(int));
	ASSERT(pw_ctx->passwd);
	strcpy(pw_ctx->passwd + sizeof(int), need_pass ? wim->pass : "");

	pw_ctx->siq_lastrun = wim->lastrun_time;

	/* Connect to and initialize the secondary, bandwidth, and throttle */
	if (wim->job_name)
		pw_ctx->job_name = strdup(wim->job_name);
	else
		pw_ctx->job_name = strdup("Default");

	setproctitle(pw_ctx->job_name);

	if (!wim->peer) {
		log(FATAL, "No peer for this job");
		return; /* Placate static analysis. */
	}
	if (!wim->target) {
		log(FATAL, "No target for this job");
		return; /* Placate static analysis. */
	}

	if (pw_ctx->restore) {
		pw_ctx->sworker_host = strdup("localhost");
	} else {
		pw_ctx->sworker_host = strdup(wim->peer);
	}
	pw_ctx->target = strdup(wim->target);

	pw_ctx->policy_id = strdup(wim->sync_id);
	ASSERT(pw_ctx->policy_id);

	/* Get the job version info from the source record. */
	srec = siq_source_record_load(pw_ctx->policy_id, &error);
	if (error) {
		goto out;
	}

	siq_source_get_pending_job(srec, &pending_job);
	pw_ctx->src = (pending_job != SIQ_JOB_FAILOVER) &&
	    (pending_job != SIQ_JOB_FAILOVER_REVERT);

	if (pw_ctx->restore) {
		siq_source_get_restore_composite_job_ver(srec, &job_ver);
		siq_source_get_restore_prev_composite_job_ver(srec,
		    &prev_job_ver);
	} else {
		siq_source_get_composite_job_ver(srec, &job_ver);
		siq_source_get_prev_composite_job_ver(srec, &prev_job_ver);
	}

	if (job_ver.local != pw_ctx->job_ver.local) {
		log(ERROR, "job_ver.local(0x%x) != pw_ctx->job_ver.local(0x%x)",
		    job_ver.local, pw_ctx->job_ver.local);

		/* Forced policy deletion can result in a zeroed
		 * job version in the source record. */
		if (job_ver.local == 0) {
			log(FATAL, "Source record's job version is 0, "
			    "policy may have been deleted");
		}
		ASSERT(job_ver.local == pw_ctx->job_ver.local);
	}

	pw_ctx->job_ver.common = job_ver.common;
	pw_ctx->local_job_ver_update =
	    (prev_job_ver.local < job_ver.local && prev_job_ver.local > 0);
	pw_ctx->prev_local_job_ver = prev_job_ver.local;

	log_job_version_ufp(&pw_ctx->job_ver, SIQ_PWORKER, __func__);

	log(TRACE, "Times for predicate: old=%ld; new=%ld", pw_ctx->old_time,
	    pw_ctx->new_time);
	
	pw_ctx->domain_id = wim->domain_id;
	pw_ctx->domain_generation = wim->domain_generation;
	log(TRACE, "Received domain %lld %d", pw_ctx->domain_id, 
	    pw_ctx->domain_generation);

	if (pw_ctx->expected_dataloss) {
		/*
		 * Let read_buffer () know that we have 
		 * corruption expected
		 */
		_g_expected_dataloss = true;
		pw_ctx->corrupt_file = NULL;
	}

	switch (wim->deep_copy) {
		case SIQ_DEEP_COPY_DENY:
			pw_ctx->deep_copy = DCO_DENY;
			break;
		case SIQ_DEEP_COPY_ALLOW:
			pw_ctx->deep_copy = DCO_ALLOW;
			break;
		case SIQ_DEEP_COPY_FORCE:
			pw_ctx->deep_copy = DCO_FORCE;
			break;
		default:
			ASSERT(0);
	}

	pw_ctx->target_work_locking_patch = wim->target_work_locking_patch;

	log(NOTICE, "Starting %s %s sync",
	    pw_ctx->stf_sync ? "stf" : "normal",
	    pw_ctx->stf_upgrade_sync ? "upgrade" : 
	    (pw_ctx->initial_sync ? "initial" : "update"));

out:
	siq_source_record_free(srec);
	isi_error_handle(error, error_out);
}

static void
work_init_msg_unpack(struct generic_msg *m, struct work_init_msg *ret,
    struct isi_error **error_out)
{
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	ASSERT(m->head.type == build_ript_msg_type(WORK_INIT_MSG));

	RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET, 1, &ret->target, &error,
	    out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_PEER, 1, &ret->peer, &error, out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_SOURCE_BASE, 1, &ret->source_base,
	    &error, out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET_BASE, 1, &ret->target_base,
	    &error, out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_BWHOST, 1, &ret->bwhost, &error,
	    out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_PREDICATE, 1, &ret->predicate,
	    &error, out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_SYNC_ID, 1, &ret->sync_id, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_RUN_ID, 1, &ret->run_id, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_LASTRUN_TIME, 1,
	    &ret->lastrun_time, &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_FLAGS, 1, &ret->flags, &error,
	    out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_PASS, 1, &ret->pass, &error, out);
	RIPT_MSG_GET_FIELD(str, ript, RMF_JOB_NAME, 1, &ret->job_name, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_LOGLEVEL, 1, &ret->loglevel,
	    &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_ACTION, 1, &ret->action, &error,
	    out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_OLD_TIME, 1, &ret->old_time,
	    &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_NEW_TIME, 1, &ret->new_time,
	    &error, out);
	RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_RESTRICT_BY, 1,
	    &ret->restrict_by, &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_HASH_TYPE, 1, &ret->hash_type,
	    &error, out);
	RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_MIN_HASH_PIECE_LEN, 1,
	    &ret->min_hash_piece_len, &error, out);
	RIPT_MSG_GET_FIELD_ENOENT(uint32, ript, RMF_HASH_PIECES_PER_FILE, 1,
	    &ret->hash_pieces_per_file, &error, out);
	RIPT_MSG_GET_FIELD_ENOENT(str, ript, RMF_RECOVERY_DIR, 1,
	    &ret->recovery_dir, &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_SKIP_BB_HASH, 1,
	    &ret->skip_bb_hash, &error, out)
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_WORKER_ID, 1, &ret->worker_id,
	    &error, out)
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_PREV_SNAPID, 1, &ret->prev_snapid,
	    &error, out)
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_CUR_SNAPID, 1, &ret->cur_snapid,
	    &error, out)
	RIPT_MSG_GET_FIELD(uint64, ript, RMF_DOMAIN_ID, 1, &ret->domain_id,
	    &error, out)
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_DOMAIN_GENERATION, 1,
	    &ret->domain_generation, &error, out)
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_DEEP_COPY, 1, &ret->deep_copy,
	    &error, out)

	ript_msg_get_field_bytestream(ript, RMF_CBM_FILE_VERSIONS,
	    1, &ret->common_cbm_vers_buf,
	    &ret->common_cbm_vers_buf_len, &error);
	if (error != NULL)
		goto out;

	RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_RUN_NUMBER, 1,
	    &ret->run_number, &error, out);
	RIPT_MSG_GET_FIELD_ENOENT(uint8, ript, RMF_TARGET_WORK_LOCKING_PATCH,
	    1, &ret->target_work_locking_patch, &error, out);

out:
	isi_error_handle(error, error_out);
}

static void
prep_old_work_init(struct migr_pworker_ctx *pw_ctx, struct generic_msg *m,
    const struct work_init_msg* wim, char* host)
{
	m->head.type = OLD_WORK_INIT_MSG6;

	m->body.old_work_init6.target = wim->target;
	m->body.old_work_init6.peer = host;
	m->body.old_work_init6.source_base = wim->source_base;
	m->body.old_work_init6.target_base = wim->target_base;
	m->body.old_work_init6.bwhost = wim->bwhost;
	m->body.old_work_init6.predicate = wim->predicate;
	m->body.old_work_init6.sync_id = wim->sync_id;
	m->body.old_work_init6.run_id = wim->run_id;
	m->body.old_work_init6.lastrun_time = wim->lastrun_time;
	m->body.old_work_init6.flags = wim->flags | FLAG_HAS_TARGET_AWARE;
	m->body.old_work_init6.pass = NULL;
	m->body.old_work_init6.job_name = wim->job_name;
	m->body.old_work_init6.loglevel = wim->loglevel;
	m->body.old_work_init6.action = wim->action;
	m->body.old_work_init6.old_time = wim->old_time;
	m->body.old_work_init6.new_time = wim->new_time;
	m->body.old_work_init6.restrict_by = wim->restrict_by;
	m->body.old_work_init6.hash_type = wim->hash_type;
	m->body.old_work_init6.min_hash_piece_len = wim->min_hash_piece_len;
	m->body.old_work_init6.hash_pieces_per_file = wim->hash_pieces_per_file;
	m->body.old_work_init6.recovery_dir = wim->recovery_dir;
	m->body.old_work_init6.skip_bb_hash = wim->skip_bb_hash;
	m->body.old_work_init6.worker_id = wim->worker_id;
	m->body.old_work_init6.prev_snapid = wim->prev_snapid;
	m->body.old_work_init6.cur_snapid = wim->cur_snapid;
	m->body.old_work_init6.domain_id = pw_ctx->domain_id;
	m->body.old_work_init6.domain_generation = pw_ctx->domain_generation;
}

static int
work_init_callback(struct generic_msg *m, void *ctx)
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct work_init_msg wim = {};
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;
	char host[MAXHOSTNAMELEN];
	char repname[MAXNAMELEN];
	char cpssname[MAXNAMELEN];
	struct generic_msg process_id_msg = {};
	char *job_name = NULL;

	work_init_msg_unpack(m, &wim, &error);
	if (error != NULL)
		log(FATAL, "%s", isi_error_get_message(error));

	/* Set up extern variables; do this now to catch errors early. */
	set_siq_log_level(wim.loglevel);

	/* Set ILOG job name to "policy name":"run id" */
	asprintf(&job_name, "%s:%llu", wim.job_name,
	    wim.run_id);
	ilog_running_app_job_set(job_name);
	ilog(IL_INFO, "%s called", __func__); /////TRACE -> INFO

	/* Do the simple message -> pw_ctx copies */
	basic_ctx_init_from_msg(&wim, pw_ctx, &error);  /////wim就是coord发送过来的work_init_msg
	if (error) {
		send_err_msg(pw_ctx, EIO, isi_error_get_message(error));
		log(FATAL, "%s", isi_error_get_message(error));
	}

	/* Initialize path based fields in pw_ctx from the message */
	path_ctx_init_from_msg(&wim, pw_ctx, &error);
	if (error) {
		send_err_msg(pw_ctx, EIO, isi_error_get_message(error));
		log(FATAL, "%s", isi_error_get_message(error));
	}

	/* Deserialize Cloudpools common file versions */
	pw_ctx->common_cbm_file_vers =
	    isi_cbm_file_versions_deserialize(wim.common_cbm_vers_buf,
	    wim.common_cbm_vers_buf_len, &error);
	if (error != NULL) {
		send_err_msg(pw_ctx, errno, isi_error_get_message(error));
		log(FATAL, "%s", isi_error_get_message(error));
	}

	migr_register_callback(pw_ctx->coord, WORK_RESP_MSG3,  ////coord -> pworker
	    work_resp_stf_callback);
	migr_register_callback(pw_ctx->coord, SPLIT_REQ_MSG,
	    split_req_callback);

	/* Setup any interface restrictions */
	init_interface_restrictions(wim.restrict_by, pw_ctx);

	/* Setup predicate */
	if (wim.predicate && strlen(wim.predicate) > 0) {
		pw_ctx->plan = siq_parser_parse(wim.predicate, time(NULL));
		if (!pw_ctx->plan)
			log(FATAL, "Failed to parse predicate %s",
			    wim.predicate);
	}

	check_for_compliance_subdirs(&wim, pw_ctx, &error);
	if (error) {
		send_err_msg(pw_ctx, EPERM, isi_error_get_message(error));
		log(FATAL, isi_error_get_message(error));
	}

	pw_ctx->chkpt_interval =
	    g_siq_conf->root->application->tw_chkpt_interval;

	/* Connect to secondary (if needed) and kick off work */
	if (!pw_ctx->is_local_operation) {
		/* Connect to secondary */
		init_remote_connection(pw_ctx);

		/* Send work init to secondary worker. Just use sent message
		 * of WORK_INIT_MSG6 with minor update */
		gethostname(host, MAXHOSTNAMELEN);
		ilog(IL_INFO, "%s called Action: %d", __func__, wim.action);

		if (pw_ctx->job_ver.common < MSG_VERSION_RIPTIDE) {
			prep_old_work_init(pw_ctx, m, &wim, host);
		} else {
			ript_msg_set_field_str(ript, RMF_PEER, 1, host);
			ript_msg_set_field_str(ript, RMF_PASS, 1, 0);
			ript_msg_set_field_uint32(ript, RMF_FLAGS, 1,
			    wim.flags | FLAG_HAS_TARGET_AWARE);
			ript_msg_set_field_uint64(ript, RMF_DOMAIN_ID, 1,
			    pw_ctx->domain_id);
			ript_msg_set_field_uint32(ript, RMF_DOMAIN_GENERATION,
			    1, pw_ctx->domain_generation);
			ript_msg_set_field_str(ript, RMF_PEER_GUID, 1, get_cluster_guid());

			/*
			 * Bug 270757 and PSCALE-13515
			 * Create a fake target_base as a MITM attack.
			 */
			UFAIL_POINT_CODE(fake_target_ifsvar,
			    ript_msg_set_field_str(ript, RMF_TARGET_BASE, 1,
				    "/ifs/.ifsvar/modules/tsm/backup");
			    log(ERROR, "%s in pworker.c with new target_base from %s.\n",
				    __func__, wim.target_base);
			);
			UFAIL_POINT_CODE(fake_target_ifsvar_moreslash,
			    ript_msg_set_field_str(ript, RMF_TARGET_BASE, 1,
				    "/ifs///.ifsvar/modules/tsm/backup");
			    log(ERROR, "%s in pworker.c with new target_base from %s.\n",
				    __func__, wim.target_base);
			);
			UFAIL_POINT_CODE(fake_target_snapshot,
			    ript_msg_set_field_str(ript, RMF_TARGET_BASE, 1,
				    "/ifs/.snapshot/");
			    log(ERROR, "%s in pworker.c with new target_base from %s.\n",
				    __func__, wim.target_base);
			);
			UFAIL_POINT_CODE(fake_target_ifs,
			    ript_msg_set_field_str(ript, RMF_TARGET_BASE, 1,
				    "/ifs/");
			    log(ERROR, "%s in pworker.c with new target_base from %s.\n",
				    __func__, wim.target_base);
			);
		}

		/* Setup diff sync (if needed) */
		if (wim.hash_type != HASH_NONE)
			init_diff_sync(m, pw_ctx, wim.loglevel);

		msg_send(pw_ctx->sworker, m);  /////pworker -> sworker
	} else {
		pw_ctx->sworker = SOCK_LOCAL_OPERATION;
		ilog(IL_NOTICE, "%s called, Starting worker %d on '%s' with %s",__func__,
		    pw_ctx->id, pw_ctx->job_name, pw_ctx->sworker_host);
		send_work_request(pw_ctx, 1); ////pworker -> coord
	}

	/* if a bandwidth host is specified initialize limit to zero and wait
	 * for a ration */
	if (wim.bwhost)
		set_bandwidth(0);

	if (is_treewalk(pw_ctx)) {
		/* Make sure we open the repstate */
		get_base_rep_name(repname, pw_ctx->policy_id, pw_ctx->restore);
		get_base_cpss_name(cpssname, pw_ctx->policy_id,
		    pw_ctx->restore);
	} else {
		get_repeated_rep_name(repname, pw_ctx->policy_id,
		    pw_ctx->cur_snap, pw_ctx->restore);
		get_repeated_cpss_name(cpssname, pw_ctx->policy_id,
		    pw_ctx->cur_snap, pw_ctx->restore);
	}

	ASSERT(pw_ctx->cur_rep_ctx == NULL);
	open_repstate(pw_ctx->policy_id, repname, &pw_ctx->cur_rep_ctx, &error);
	/*XXX: this really shouldn't be a log(FATAL) */
	if (error)
		log(FATAL, "Error opening repstate %s", repname);

	ASSERT(pw_ctx->cur_cpss_ctx == NULL);
	open_cpss(pw_ctx->policy_id, cpssname, &pw_ctx->cur_cpss_ctx, &error);
	if (error)
		log(FATAL, "Error opening cpools sync state %s", cpssname);

	cpu_throttle_start(&pw_ctx->cpu_throttle);
	pw_ctx->state = SIQ_JS_RUNNING;
	
	//Send Process ID for tracking
	process_id_msg.head.type = WORKER_PID_MSG;
	process_id_msg.body.worker_pid.worker_id = pw_ctx->id;
	process_id_msg.body.worker_pid.process_id = getpid();
	msg_send(pw_ctx->coord, &process_id_msg);
	if (job_name)
		free(job_name);

	return 0;
}

/**
 * Duplicated in pworker and sworker to allow failpoint
 * use.
 */
static bool
pw_two_down_nodes(void)
{
	int max_down = 1;

	UFAIL_POINT_CODE(set_max_down_nodes_value, max_down = RETURN_VALUE);

	return max_down_nodes(max_down);
}

static int
group_callback(void *ctx)
{
	log(TRACE, "group_callback");
	if (pw_two_down_nodes())
		log(FATAL, "Filesystem readiness lost, unable to continue");
	return 0;
}

static void
save_recovery_state(struct migr_pworker_ctx *pw_ctx)
{
	struct isi_error *error = NULL;
	char *data = NULL;
	size_t size = 0;
	
	if (g_lost_quorum)
		log(FATAL, "%s", LOST_QUORUM_MSG);

	if (!pw_ctx->work)
		goto out;

	ASSERT(migr_tw_has_slices(&pw_ctx->treewalk));
	data = migr_tw_serialize(&pw_ctx->treewalk, &size);

	set_work_entry(pw_ctx->wi_lin, pw_ctx->work, data, size,
	    pw_ctx->cur_rep_ctx, &error);
out:
	free(data);

	if (error) {
		handle_stf_error_fatal(pw_ctx->coord, error,
		    E_SIQ_WORK_SAVE, EL_SIQ_SOURCE, pw_ctx->wi_lin,
		    pw_ctx->wi_lin);
		isi_error_free(error);
	}
}

/*
 * if we are using the burst socket then flush all msgs
 */
void
flush_burst_buffer(struct migr_pworker_ctx *pw_ctx)
{
	if ((pw_ctx->sworker > 0) &&
	    sock_type_from_fd_info(pw_ctx->sworker) == SOCK_BURST) {
		flush_burst_send_buf(pw_ctx->sworker);
	}
}

int
timer_callback(void *ctx)  /////10s 调用一次
{
	struct migr_pworker_ctx *pw_ctx = ctx;
	struct timeval timeout = {10, 0};
	uint64_t total_netstat;
	struct isi_error *error = NULL;

	UFAIL_POINT_CODE(fast_timeout, timeout.tv_sec = 0);

	log(TRACE, "timer_callback");

	/* add bandwidth update */
	total_netstat = migr_get_recv_netstat() + migr_get_send_netstat();
	if (!pw_ctx->assess) {
		pw_ctx->tw_cur_stats->bytes->transferred = total_netstat;
		pw_ctx->tw_cur_stats->bytes->network->total =
		    pw_ctx->stf_cur_stats->bytes->network->total =
		    total_netstat;
		pw_ctx->tw_cur_stats->bytes->network->to_target =
		    pw_ctx->stf_cur_stats->bytes->network->to_target =
		    migr_get_send_netstat();
		pw_ctx->tw_cur_stats->bytes->network->to_source =
		    pw_ctx->stf_cur_stats->bytes->network->to_source =
		    migr_get_recv_netstat();
	}
	pw_ctx->last = total_netstat;
	pw_ctx->lastfile = pw_ctx->filestat;

	if (pw_ctx->coord != -1 && (pw_ctx->checkpoints || pw_ctx->stf_sync ||
	    pw_ctx->domain_mark))
		send_stats(pw_ctx);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	/* Periodically flush burst send buffer */
	flush_burst_buffer(pw_ctx);

	if (pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync ||
	    !pw_ctx->stf_sync) {
		/*
		 * Don't serialize the remnant that's left after
		 * we finish the walk.
		 */
		if (migr_tw_has_slices(&pw_ctx->treewalk) &&
		    pw_ctx->skip_checkpoint == false)
			save_recovery_state(pw_ctx);
	} else if (pw_ctx->work) {
		set_work_entry(pw_ctx->wi_lin, pw_ctx->work, NULL, 0,
		    pw_ctx->cur_rep_ctx, &error);
		if (error)
			goto out;
		//If this is a phase with worklists, checkpoint that too
		if (pw_ctx->chg_ctx && pw_ctx->chg_ctx->wl_iter != NULL) {
			wl_checkpoint(&pw_ctx->wl_log,
			   pw_ctx->chg_ctx->wl_iter,
			   pw_ctx->work->dircookie1,
			   pw_ctx->work->dircookie2, &error);
			if (error)
				goto out;
		}
	}

	if (pw_ctx->cpu_throttle.host == -1)
		connect_cpu_throttle_host(&pw_ctx->cpu_throttle, SIQ_PWORKER);

	send_cpu_stat_update(&pw_ctx->cpu_throttle);/////调用cpu_throttle.c中的send_cpu_stat_update

	migr_register_timeout(&timeout, timer_callback, pw_ctx);

	gettimeofday(&pw_ctx->lasttv, 0);

out:
	if (error) {
		handle_stf_error_fatal(pw_ctx->coord, error,
		    E_SIQ_WORK_SAVE, EL_SIQ_SOURCE, pw_ctx->wi_lin,
		    pw_ctx->wi_lin);
		isi_error_free(error);
	}
	return 0;
}

static void
usage(void)
{
	printf("usage: %s [-dv] [-l loglevel]\n", PROGNAME);
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	struct isi_error *error = NULL;
	struct migr_pworker_ctx pw_ctx = {};
	int debug = 0;
	char c;
	struct ilog_app_init init = {
		.full_app_name = "siq_pworker",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file = "",
		.syslog_threshold = IL_INFO_PLUS,  ///////IL_TRACE_PLUS -> IL_INFO_PLUS
		.component = "pworker_primary",
		.job = "",
	};
	struct procoptions po = PROCOPTIONS_INIT;
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);
	g_ctx = &pw_ctx;

	/* Bug 162782 - ignore PI_AV_COMPLY */
	po.po_isi_flag_off |= PI_AV_COMPLY;
	if (setprocoptions(&po))
		log(ERROR, "Error setting proc flags. po_isi_flag_on: %x "
		    "po_isi_flag_off: %x", po.po_isi_flag_on,
		    po.po_isi_flag_off);

	g_siq_conf = siq_gc_conf_load(&error);

	if (error)
		log(FATAL, "Failed to load global config: %s",
		    isi_error_get_message(error));

	while ((c = getopt(argc, argv, "dl:v")) != -1) {
		switch (c) {
		case 'd':
			debug = 1;
			break;
		case 'l':
			set_siq_log_level(atoi(optarg));
			break;
		case 'v':
			siq_print_version(PROGNAME);
			return 0;
		default:
			usage();
		}
	}

	if (!debug)
		daemon(0, 0);

	check_for_dont_run(true);

	/* XXXJDH: needs to have updated ufp with no return code. */
	if (UFAIL_POINT_INIT("isi_migr_pworker", isi_migr_pworker_ufp) != 0)
		log(FATAL, "Failed to init ufp");

	log(INFO, "starting build <%s>", BUILD);

	pw_ctx.state = SIQ_JS_NONE;
	pw_ctx.wi_lock_fd = -1;
	pw_ctx.dir_fd = -1;
	pw_ctx.state_ads.fd = -1;
	pw_ctx.state_file.fd = -1;
	pw_ctx.ads_oldpwd = -1;
	pw_ctx.files_per_sec = -1;
	pw_ctx.udelay = -1;
	pw_ctx.file_state.fd = -1;
	pw_ctx.file_state.sd_region.region_type = RT_SPARSE;
	pw_ctx.file_state.sd_region.start_offset = 0;
	pw_ctx.file_state.sd_region.byte_count = 0;
	path_init(&pw_ctx.cwd, NULL, NULL);
	pw_ctx.read_size = 128 * 1024;
	pw_ctx.prev_snap_cwd_fd = -1;
	pw_ctx.delete_filelist.list = NULL;
	pw_ctx.delete_filelist.size = 0;
	lin_set_init(&pw_ctx.domain_mark_lins);
	pw_ctx.send_work_item_lock = false;

	pw_ctx.lnn = get_local_lnn(&error);
	if (error) {
		log(FATAL, "failed to get_local_lnn: %s",
		    isi_error_get_message(error));
	}

	if (g_siq_conf->root->coordinator->skip_work_host != 0) {
		log(NOTICE, "worker pools is disabled");
		pw_ctx.lnn = 0;	
	}
	pw_ctx.coord = fork_accept_from(   //////返回一个primary worker和coordinator通信的socket
	    g_siq_conf->root->coordinator->ports->pworker, /////默认配置的pworker的port是2098
	    &pw_ctx.job_ver.local, debug, POL_DENY_FALLBACK, 0, pw_ctx.lnn);

	/* Forked pworkers should reload the global conf, it may have chnaged
	 * since the original pworker loaded it. */
	siq_gc_conf_close(g_siq_conf);
	g_siq_conf = siq_gc_conf_load(&error);

	signal(SIGQUORUM, sigquorum);
	signal(SIGUSR2, sigusr2);
	gmp_kill_on_quorum_change_register(NULL, &error);
	if (error)
		log(FATAL, "gmp_kill_on_quorum_change_register error: %s",
		    isi_error_get_message(error));

	/* Do this before adding group callback in case node is unmounted. */
	if (pw_two_down_nodes())
		log(FATAL, "/ifs is unmounted or more than one node is down");

	snapshot_set_dotsnap(1, 0, NULL);

	init_statistics(&pw_ctx.tw_cur_stats);
	init_statistics(&pw_ctx.tw_last_stats);
	init_statistics(&pw_ctx.stf_cur_stats);
	init_statistics(&pw_ctx.stf_last_stats);

	migr_register_group_callback(group_callback, NULL);

	migr_add_fd(pw_ctx.coord, &pw_ctx, "coord");

	cpu_throttle_ctx_init(&pw_ctx.cpu_throttle, SIQ_WT_PWORKER,
	    g_siq_conf->root->bwt->cpu_port/*bandwidth监听的cpu throttle端口*/);

	timer_callback(&pw_ctx);

	migr_register_callback(pw_ctx.coord, OLD_WORK_INIT_MSG6,  ////这些callback都是给coord的？？？？
	    work_init_callback);
	migr_register_callback(pw_ctx.coord,
	    build_ript_msg_type(WORK_INIT_MSG), work_init_callback);
	migr_register_callback(pw_ctx.coord, JOBCTL_MSG,
	    jobctl_callback);
	migr_register_callback(pw_ctx.coord, DISCON_MSG,
	    disconnect_callback);
	migr_register_callback(pw_ctx.coord, BANDWIDTH_MSG,
	    bandwidth_callback);
	migr_register_callback(pw_ctx.coord, THROTTLE_MSG,
	    throttle_callback);
	migr_register_callback(pw_ctx.coord, GENERAL_SIGNAL_MSG,
	    general_signal_callback);

	isi_cbm_init(&error);
	if (error)
		log(FATAL, "Unable to initialize cloudpools services.");

	/* Ignore errors, if any */
	ifs_setnoatime(true);

	migr_process();

	return 0;
}

/**
 * A helper function for logging corrupted files/directories
 */

void
log_corrupt_file_entry(struct migr_pworker_ctx *pw_ctx, const char *filename)
{

	/* check for the file failures_workerid_workerpid.log, 
	 * if exist than write entry
	 * if file doesn`t exist, create a new file as 
	 * failures_workerid_workerpid.log under policy dir under reports 
	 * /ifs/.ifsvar/modules/tsm/sched/reports/policy_id/failures_1_1234.log
	 */

	char logfile[MAXPATHLEN+1];
	sprintf(logfile, "%s/%s/failures_%d_%d.log", sched_get_reports_dir(), 
		pw_ctx->policy_id, pw_ctx->id, getpid());
	if(pw_ctx->corrupt_file == NULL) {
		/* 
		 * Make sure we don`t overwrite any 
		 * existing file. It is very rare scenario but it might be 
		 * possible that during initial sync we create a file
		 * say failures_workerid_pid.log (failures_0_1234.log) and 
		 * during incremental sync we might overwrite the same file. (
		 * unfortunately if incremental job get assigned to same the 
		 * worker id with same pid) 
		 * To be safer side, open file in append mode. So that
		 * file contents will be appended during incremental sync 
		 * if file already exist.
		 */
		/* first time, create a file for append */
		pw_ctx->corrupt_file = fopen(logfile, "a");
		if (pw_ctx->corrupt_file == NULL) {
			/* not able to create file, log into log file */
			log(ERROR,"%s%s\n",
				pw_ctx->treewalk._path.path, filename);
		}
		else {
			fprintf(pw_ctx->corrupt_file, "%s%s\n",
				pw_ctx->treewalk._path.path, filename);
		}
	}
	else {
		fprintf(pw_ctx->corrupt_file, "%s%s\n",
			pw_ctx->treewalk._path.path, filename);
	}
}

/*
 * Add cpss_entry to in-memory log. If the log is full, then we
 * would flush the log to disk before adding new entry.
 */
void
cpss_log_add_entry(struct migr_pworker_ctx *pw_ctx, struct cpss_ctx *ctx,
    ifs_lin_t lin, struct cpss_entry *new, struct cpss_entry *old, bool is_new,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpss_entry null_entry = {};
	struct cpss_log *logp = &pw_ctx->cpss_log;

	log(TRACE, "cpss_log_add_entry(%llx, %d)", lin, is_new);
	ASSERT(new != NULL);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		/*
		 * Log is full, so we need flush it
		 */
		flush_cpss_log(&pw_ctx->cpss_log, pw_ctx->cur_cpss_ctx, &error);
		if (error)
			goto out;
	}
	ASSERT(logp->index < MAX_BLK_OPS);

	logp->entries[logp->index].is_new = is_new;
	ASSERT(is_new || old);
	if (old) {
		logp->entries[logp->index].old_entry = *old;
	} else {
		logp->entries[logp->index].old_entry = null_entry;
	}

	logp->entries[logp->index].lin = lin;
	logp->entries[logp->index].new_entry = *new;

	logp->index++;

out:
	isi_error_handle(error, error_out);
}
