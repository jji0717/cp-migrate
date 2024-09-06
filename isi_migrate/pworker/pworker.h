#ifndef PWORKER_H
#define PWORKER_H

#include <stdbool.h>

#include <sys/queue.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include "isi_domain/dom.h"

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/migr/alg.h"
#include "isi_migrate/migr/work_item.h"

#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/cpools_sync_state.h"
#include "isi_migrate/migr/worklist.h"
#include "isi_migrate/migr/lin_list.h"
#include "isi_migrate/migr/cpu_throttle.h"
#include "isi_burst_sdk/include/burst.h"
#include "isi_cpool_bkup/isi_cpool_bkup.h"
#include "isi_cpool_bkup/isi_cpool_bkup_sync.h"
#include "change_compute.h"

#define FILES_GRANT 100
#define PROGNAME "isi_migr_pworker"
#define MAX_WORK_STACK_SIZE 32

#define MAX_REP_STATE_SCAN_COUNT 10000

/* logical_size in FILE_DATA_MSG is type unsigned */
#define MAX_DATA_REGION_SIZE 0xffffffff

/* max number of seconds to spend sending a file data msg */
#define MAX_DATA_MSG_SEND_TIME 10

/*
 * Bug 225582
 * Max number of sequential empty sub dirs to walk prior to checkpoint
 */
#define MAX_EMPTY_SUB_DIRS 100

/*
 * While handling the ADS directory of a file during diff sync and idmap
 * transfer, files are transferred using the regular sync methods. */
#define DIFF_SYNC_BUT_NOT_ADS(ctx)	((ctx)->doing_diff_sync && \
					!(ctx)->doing_ads && \
					!(ctx)->doing_extra_file)

struct migr_pworker_ctx;

typedef void (*migr_pworker_func_t)(struct migr_pworker_ctx *,
    struct isi_error **);

struct migr_cpools_file_state {
	struct isi_file_bsession *bsess;
	enum isi_cbm_backup_sync_type	sync_type;
	size_t		mbytes_pb;  // Metadata bytes per blob for bsession.
	size_t		dbytes_pb;  // Data bytes per blob for bsession.
	size_t		nmblobs;    // # of metadata blobs for bsession.
	size_t		ndblobs;    // # of data blobs for bsession.

	size_t		mblobs_sent; // # of metadata blobs sent.
	size_t		dblobs_sent; // # of data blobs sent.
	enum isi_cbm_stub_content_type curr_sct; // Current blob type to send.
};

struct migr_file_state {
	int		fd;
	struct stat	st;
	off_t		cur_offset;
	struct snap_diff_region	sd_region;
	enum file_type	type;
	char		*name;
	enc_t		enc;
	bool		size_changed;
	ifs_snapid_t	prev_snap;
	bool		skip_bb_hash;
	struct hash_ctx *bb_hash_ctx;
	char 		*hash_str;
	struct migr_cpools_file_state cpstate;
	uint64_t 	cookie;

	bool		skip_data;

	/* hard link state */
	bool		hard_link;
};

struct migr_dir_state {
	struct migr_dir	*dir;
	struct stat	 st;
};

struct resync_dir_state {
	int			dir_fd;
	ifs_lin_t		dir_lin;
	unsigned int		expected_links; // Number of dirent_lin children.
	unsigned int		index;
	uint64_t		resume_cookie;
};

struct resync_parents_state {
	ifs_lin_t		dirent_lin;
	struct parent_entry	*parents;
	unsigned int		num_parents;
	unsigned int		num_links;
	unsigned int		index;
	struct resync_dir_state	cur_parent;
	bool			is_dir;
	bool			skip_comp_store;
};

struct hash_piece {
	uint64_t offset;
	uint64_t len;
	char *hash_str;
	bool eof;
	TAILQ_ENTRY(hash_piece) ENTRIES;
};
TAILQ_HEAD(hash_pieces, hash_piece);

/* State of entries in the diff sync cache */
enum cache_state {DS_INIT,
		  WAITING_FOR_SHORT_HASH,
		  WAITING_FOR_FULL_HASH,
		  READY_TO_SEND,
		  SENDING,
		  WAITING_FOR_ACK,
		  ACK_RECEIVED,
		  CHECKPOINTED,
		  DS_ERROR}; /* No error state at the moment */

struct cache_entry {
	int fd;
	struct migr_dirent *md;
	enum sync_state sync_state;
	int n_shashes;
	struct short_hash_req_info *shashes;
	struct hash_pieces *pw_hash_head;
	int pw_hash_size;
	struct hash_pieces *sw_hash_head;
	int sw_hash_size;
	bool pw_eof;
	bool sw_eof;
	struct migr_file_state fstate;
	enum cache_state cache_state;
	TAILQ_ENTRY(cache_entry) ENTRIES;
};
TAILQ_HEAD(cache, cache_entry);

enum migr_pworker_wait {
	MIGR_NO_WAIT,

	/** Waiting for FILE_ACK while ascending directory tree. */
	MIGR_WAIT_ACK_UP,

	/** Waiting for FILE_ACK before descending the directory tree. */
	MIGR_WAIT_ACK_DOWN,

	/** Diff sync is waiting for hash messages. */
	MIGR_WAIT_MSG,
	MIGR_WAIT_ACK_DIR,
	MIGR_WAIT_DONE,
	MIGR_WAIT_STF_LIN_ACK,
	MIGR_WAIT_PENDING_EXIT,
	
	//Compliance target phase waiting
	MIGR_WAIT_COMP_TARGET_BEGIN,
	MIGR_WAIT_COMP_TARGET_CHKPT,
};

enum migr_dir_result {
	MIGR_CONTINUE_DIR,
	MIGR_CONTINUE_FILE,
	MIGR_CONTINUE_ADS,
	MIGR_CONTINUE_DONE,
};

char *migr_dir_result_str(enum migr_dir_result res);

#define MAX_ITER_ENTRIES 10 /* Max number of entries in stf rep buffer */
/* Entry in stf rep buffer */
struct lin_rep_entry {
	ifs_lin_t lin;
	struct rep_entry entry;
};

/*
 * stf rep iter buffer. This stores the atmost MAX_ITER_ENTRIES number 
 * of lin and rep entries which are read from repstate
 */
struct stf_rep_iter {
	struct rep_iter *iter;
	struct lin_rep_entry lin_rep[MAX_ITER_ENTRIES];
	int index;
	int count;
};

struct lin_lin_list_entry {
	ifs_lin_t lin;
	struct lin_list_entry entry;
};

struct resync_iter {
	struct lin_list_iter *iter;
	struct lin_lin_list_entry lin_lin[MAX_ITER_ENTRIES];
	int index;
	int count;
};

/*
 * Structure used to order LINK messages such that
 * all the newly created parents are first sent
 * to the target and then the current dirent.
 */
struct comp_rplookup {
	ifs_lin_t last_parent_lin;
	ifs_lin_t dir_lin;
	struct dirent entry;
	int lcount;
	bool resync_phase;
	char *buf;
	size_t buf_len;
	size_t buf_cnt;
	struct dirent *entryp;
};

struct migr_pworker_ctx {
	/** Socket to the coordinator. */
	int			 coord;

	/** TCP Socket to the secondary. */
	int			 tcp_sworker;
	
	/* Socket used for SyncIQ Data */
	int			sworker;

	/** Secondary host. */
	char			*sworker_host;

	/** Socket to the p_hasher. */
	int			 hasher;

	/** 
	 * Worker ID used only for correlating coord, pworker and sworker log
	 * messages.
	 */
	int			 id;

	/** 
	 * Current job state.  Mostly an indicator of the failure of a job.
	 *
	 * XXXJDH: This should probably be deprecated and a failing job should
	 * be identified in one location instead of letting anywhere set it.
	 */
	enum siq_job_state state;

	/**
	 * A mish-mash of various options.  Most are are translated into
	 * specific flags like access, initial_sync, and snapop.
	 *
	 * XXXJDH: This should be removed after making sure all flags are
	 * represented in individual fields.
	 */
	unsigned flags;

	/** 
	 * Copy, Sync, Migrate, Remove, etc...
	 */
	enum siq_action_type action;

	/**
	 * True iff action != SIQ_ACT_FAILOVER &&
	 * action != SIQ_ACT_FAILOVER_REVERT.
	 */
	bool src;

	/**
	 * Also known as local operation like the actual "Access" feature or
	 * for recover capacity.
	 */
	bool assess;

	/**
	 * True if this is a full/initial sync.
	 */
	bool initial_sync;

	/**
	 * True if doing a snapshot-based job.
	 */
	bool snapop;

	/**
	 * This is the ctime of the old snapshot.  Used for predicates.
	 */
	time_t old_time;

	/**
	 * This is the ctime of the new snapshot.  Used for predicates.
	 */
	time_t new_time;

	/**
	 * CHECKSUM_SIZE if integrity check is enabled. 0 otherwise.
	 */
	int checksum_size;

	/* 
	 * Work item info.
	 */

	/**
	 * Work item directory selectors.
	 */
	char *work_selectors;

	/**
	 * Treewalker
	 */
	struct migr_tw treewalk;

	/**
	 * Next function in state machine.
	 */
	migr_pworker_func_t next_func;

	/**
	 * Reason for no next state.
	 */
	enum migr_pworker_wait wait_state;

	struct migr_dir_state dir_state;
	struct migr_file_state file_state;

	struct migr_dir_state ads_dir_state;
	struct migr_file_state ads_file_state;

	struct migr_dir *delete_dir;
	struct filelist delete_filelist;

	struct resync_parents_state resync_parents_state;

	int outstanding_acks;

/* From 
extern struct pworker g_pw;
*/
	char *job_name;
	char *policy_id;
	siq_plan_t *plan;
	char *passwd;
	int errors;
	int dirs;
	int files_count;
	int transfered;
	int yield_files;
	char *target;			/* Secondary */
	bool mklinks;
	bool logdeletes;
	bool system;		/* true if the policy type == SIQ_PT_SYSTEM */

	time_t siq_lastrun;

	struct filelist  deleted_files;

	/*
	 * Acknowledgment context information about already sent items
	 * used in recovery process.
	 *
	 * The dir_lin and filelin are sent from the coordinator on restarts.
	 */
	int dir_fd;
	ifs_lin_t sent_dir_lin;
	ifs_lin_t sent_filelin;

	/*
	 *
	 *  From pw_context
extern struct pw_context g_ctx;
	 *
	 */

	struct path	 cwd;
	int		 prev_snap_cwd_fd;
	off_t		 cur_offset;
	int		 checkpoints;
	int		 done;

	/* LIN of parent dir for current entity. */
	ifs_lin_t	 dir_lin;
	/* LIN of current processing file. */
	ifs_lin_t	 filelin;

/*
extern struct worker_info g_wi;
*/

/* Globals */
	struct siq_stat *tw_cur_stats;
	struct siq_stat *tw_last_stats;
	struct siq_stat *stf_cur_stats;
	struct siq_stat *stf_last_stats;

	bool pending_done_msg;
	bool pending_work_req;
	bool is_local_operation;

	/* CPU throttling */
	struct migr_cpu_throttle_ctx cpu_throttle;  //////struct migr_pworker_ctx的成员变量
	
	/* Files per second throttling */
	int files_per_sec;
	int udelay;			/* Throttle in usecs/file */
	struct timeval tv;
	struct timeval lasttv;
	struct timeval statdelay_tv;
	struct timeval statdelay_lasttv;
	int statdelay_udelay;

	uint64_t last;
	uint64_t lastfile;
	uint64_t filestat;

	/* dofile */
	struct migr_file_state state_ads;
	struct migr_file_state state_file;
	int read_size;

	/* doads */
	int ads_delete, ads_oldpwd;
	bool doing_ads;
	DIR *ads_dir;

	/* send_work_request */
	uint64_t lastnet;

	const struct inx_addr *forced_addr;

	bool wait_for_acks;
	time_t last_chkpt_time;
	time_t chkpt_interval;

	/* Below for diff sync */
	enum hash_type hash_type;
	bool doing_diff_sync;
	bool doing_extra_file;
	bool cwd_is_new_on_sworker;
	bool waiting_for_short_hash_resp;
	int outstanding_full_hashes;
	struct cache cache; // a TAILQ
	int cache_size;

	/* The newest entry in the cache */
	struct cache_entry *latest_cache_ent;

	/* The entry in the cache that is currently being sent to the
	 * sworker */
	struct cache_entry *working_cache_ent;

	int just_send_the_file_len;
	int short_hash_len;
	int min_full_hash_piece_len;
	int hash_pieces_per_file;
	int max_cache_size;
	int max_hash_pieces;

	/* Below for BBD */
	bool full_transfer;
	bool skip_bb_hash;
	
	/* STF related data structures */
	bool stf_sync;
	bool stf_upgrade_sync;
	bool restore;
	bool snap_switched;
	struct changeset_ctx *chg_ctx;
	struct rep_ctx *cur_rep_ctx;
	struct cpss_ctx *cur_cpss_ctx;
	/* rep iter for change transfer */
	struct stf_rep_iter stf_rep_iter;
	/* Current/previous lin during change transfer */
	uint64_t prev_lin;
	uint64_t cur_lin;
	/* Current rep entry during change transfer*/
	struct rep_entry rep_entry;
	struct work_restart_state *work;
	uint64_t wi_lin;
	int wi_lock_fd;
	struct wl_log wl_log;
	struct rep_log rep_log;
	struct cpss_log cpss_log;
	bool has_excludes;
	bool hash_exception;
	bool link_exception;

	/* Compliance resync iterator */
	struct resync_iter resync_iter;
	/* Current resync entry during change transfer*/
	struct lin_list_entry resync_entry;

	ifs_snapid_t prev_snap;
	ifs_snapid_t cur_snap;
	int prev_snap_root_fd;
	int cur_snap_root_fd;
	ifs_lin_t cur_snap_root_lin;
	
	ifs_domainid_t domain_id;
	uint32_t domain_generation;

	bool lin_map_blk;
	unsigned curr_ver;
	unsigned flip_flags;
	bool	mirror_check;
	struct map_ctx *lmap_mirror_ctx;
	struct lmap_log lmap_mirror_log;
	struct lin_set domain_mark_lins;

	// WORM Compliance sync.
	bool compliance;

	// Fail sync if file encountered in a WORM Compliance domain.
	// Only if domain isn't rooted at the source.
	bool fail_on_compliance; 

	bool domain_mark;

	/* related to bug 96259 */
	bool skip_checkpoint;

	/* bug 155281. True if we need to receive acks before file splitting */
	bool fsplit_acks_pending;
	
	bool split_req_pending;
	bool do_file_split;
	bool comp_target_split_pending;
	ifs_lin_t split_wi_lin;

	uint64_t cur_file_data_sent;

	//Burst logging
	struct HostInterfaceImpl *burst_host;

	/* expected dataloss flag */
	bool expected_dataloss;

	/* per worker log file */
	FILE *corrupt_file;

	/* Version of the current job. */
	struct siq_job_version job_ver;

	/* Status of the local version of the current job compared with that
	 * of the latest successful job, if any. */
	bool local_job_ver_update;
	uint32_t prev_local_job_ver;

	/* lnn of this node */
	int lnn;

	/* Cloudpools deep copy information */
	enum isi_cbm_backup_deep_copy_opt deep_copy;
	struct isi_cbm_file_versions *common_cbm_file_vers;

	/* Compliance V2 related information */
	bool compliance_v2;
	bool comp_v1_v2_patch;
	struct map_ctx *lmap_restore_ctx;
	struct lin_list_ctx *resync_ctx;
	struct compliance_map_ctx *comp_map_ctx;
	struct comp_rplookup comp_rplookup;

	bool tw_sending_file;

	/* Send a work item lock to the sworker */
	bool send_work_item_lock;
	bool target_work_locking_patch;

	/* Bug 225582 - sequential empty sub-directories count */
	int initial_sync_empty_sub_dirs;
};

static inline void
migr_call_next(struct migr_pworker_ctx *pw_ctx, migr_pworker_func_t func)
{
	ASSERT(func != NULL);
	pw_ctx->next_func = func;
	pw_ctx->wait_state = MIGR_NO_WAIT;
}

static inline void
migr_wait_for_response(struct migr_pworker_ctx *pw_ctx,
    enum migr_pworker_wait wait)
{
	ASSERT(wait != MIGR_NO_WAIT);
	pw_ctx->next_func = NULL;
	pw_ctx->wait_state = wait;
}

static inline void
snap_switch(ifs_snapid_t *snap1, ifs_snapid_t *snap2)
{
	ifs_snapid_t temp;
	temp = *snap1;
	*snap1 = *snap2;
	*snap2 = temp;
}

static inline bool
migr_is_waiting_for(struct migr_pworker_ctx *pw_ctx,
    enum migr_pworker_wait wait)
{
	return pw_ctx->next_func == NULL && pw_ctx->wait_state == wait;
}

/*
 * True for non stf initial/upgrade/repeated or stf initial sync.
 */
static inline bool
is_treewalk(struct migr_pworker_ctx *pw_ctx)
{
	if (pw_ctx->initial_sync || pw_ctx->stf_upgrade_sync ||
	    !pw_ctx->stf_sync) {
		return true;
	} else {
		return false;
	}
}

/*
 * True if current work item is treewalk
 */
static inline bool
is_tw_work_item(struct migr_pworker_ctx *pw_ctx)
{
	ASSERT(pw_ctx->work != NULL);
	return (pw_ctx->work->rt == WORK_SNAP_TW);
}

void send_err_msg(const struct migr_pworker_ctx *ctx, const int err, 
    const char *errstr);

/* Fail if LIN in compliance domain. */

void check_for_compliance(const struct migr_pworker_ctx *ctx,
    const ifs_lin_t lin, struct isi_error **error_out);

void send_stats(struct migr_pworker_ctx *);
int do_remove_operation(struct migr_pworker_ctx *, char *file, enc_t enc,
    uint64_t filesize);

void send_ioerror(int fd, int err);
void assess_update_stat(struct migr_pworker_ctx *pw_ctx, bool not_sent, 
    struct stat* st, struct migr_file_state *file);
char *get_src_path(struct migr_pworker_ctx *, char *path);

void tw_request_new_work(struct migr_pworker_ctx *pw_ctx);
int dowork(struct migr_pworker_ctx *pw_ctx, const struct path *, int, char *);
int restart(void *);
void engine(struct migr_pworker_ctx *pw_ctx);

int migr_continue_work(void *ctx);
void migr_start_work(struct migr_pworker_ctx *, struct isi_error **);
void migr_init_work(struct migr_pworker_ctx *, struct isi_error **);  //////没有定义,没有用到
void migr_reschedule_work(struct migr_pworker_ctx *);
void migr_start_ads_or_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void send_user_attrs(struct migr_pworker_ctx *pw_ctx, int fd, bool is_dir,
    struct isi_error **error_out);
void send_worm_state(struct migr_pworker_ctx *pw_ctx, struct worm_state *worm);
void set_cloudpools_sync_type(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_file_state *file, enum file_type type, struct stat *st,
    unsigned *flags, struct isi_error **error_out);
void do_cloudpools_setup(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_file_state *file, enum file_type type, struct stat *st,
    struct isi_error **error_out);
bool migr_start_file(struct migr_pworker_ctx *pw_ctx, int fd,
    struct migr_dirent *md, struct migr_file_state *file,
    struct isi_error **error_out);

void migr_continue_reg_dir(struct migr_pworker_ctx *,
    struct isi_error **);
enum migr_dir_result migr_handle_generic_dir(struct migr_pworker_ctx *,
    struct migr_dir_state *, struct migr_dirent *, migr_pworker_func_t,
    struct isi_error **);
void migr_finish_generic_reg_dir(struct migr_pworker_ctx *, 
    migr_pworker_func_t start_work_func, struct isi_error **error_out);
void migr_finish_reg_dir(struct migr_pworker_ctx *, struct isi_error **);
int short_hash_resp_callback(struct generic_msg *m, void *ctx);
int full_hash_resp_callback(struct generic_msg *m, void *ctx);
void hasher_chdir(struct migr_pworker_ctx *pw_ctx);
int ack_dir_callback(struct generic_msg *m, void *ctx);
int short_hash_resp_callback(struct generic_msg *m, void *ctx);
struct cache_entry *cache_find_by_cookie(struct migr_pworker_ctx *pw_ctx,
	   uint64_t cookie);
void hasher_stop(struct migr_pworker_ctx *pw_ctx,
    int to_stop,
    enum hash_stop type,
    struct full_hash_file_resp_msg *resp);
enum migr_dir_result migr_continue_dir_diff_sync(
	struct migr_pworker_ctx *pw_ctx,
	struct migr_dir_state *dir,
	struct migr_file_state *file, siq_plan_t *plan,
	struct isi_error **error_out);
int pw_hash_error_callback(struct generic_msg *m, void *ctx);
int sw_hash_error_callback(struct generic_msg *m, void *ctx);
void parse_diff_sync_variables(void);
void add_file_for_diff_sync(
	struct migr_pworker_ctx *pw_ctx,
	struct migr_dirent *md,
	struct stat *st,
	int fd,
	struct isi_error **error_out);
void cache_entry_cleanup(
	struct migr_pworker_ctx *pw_ctx,
	struct migr_dir *dir,
	struct cache_entry *cache_ent);
char *cache_state_str(enum cache_state state);
struct cache_entry *cache_find_by_dkey(struct migr_pworker_ctx *pw_ctx,
    uint64_t dkey);
void diff_sync_init(struct migr_pworker_ctx *pw_ctx);
void log_cache_state(struct migr_pworker_ctx *pw_ctx);
void make_sure_cache_is_processed(struct migr_pworker_ctx *pw_ctx);
bool cache_has_msgs_pending(struct migr_pworker_ctx *pw_ctx, bool wait_for_ack);
void ds_start_file(
	struct migr_pworker_ctx *pw_ctx,
	int fd,
	struct migr_dirent *md,
	struct migr_file_state *file,
	struct generic_msg *msg);
bool ds_continue_dir(struct migr_pworker_ctx *pw_ctx,
	enum migr_dir_result *result,
	bool dir_end);

#define FNAME(md) ((md)->dirent->d_name)

static void inline
set_cache_state(struct cache_entry *entry, enum cache_state state)
{
	ASSERT(entry);
	log(TRACE, "Changing cache state of %s: %s ==> %s",
	    FNAME(entry->md),
	    cache_state_str(entry->cache_state),
	    cache_state_str(state));
	entry->cache_state = state;
}

static inline void
throttle_delay(struct migr_pworker_ctx *pw_ctx)
{
        /* The 0.1s threshold prevents too many short and inaccurate sleeps */
        const int THRESH = 100000;
        int diff = 0;

        pw_ctx->filestat++;
        if (pw_ctx->udelay <= 0 ||
            (pw_ctx->statdelay_udelay += pw_ctx->udelay) < THRESH)
                goto out;

        gettimeofday(&pw_ctx->statdelay_tv, 0);
        if (pw_ctx->statdelay_lasttv.tv_sec != 0)
                diff = tv_usec_diff(&pw_ctx->statdelay_tv,
                    &pw_ctx->statdelay_lasttv);

        pw_ctx->statdelay_udelay -= diff;

        pw_ctx->statdelay_lasttv = pw_ctx->statdelay_tv;

        if (pw_ctx->statdelay_udelay < THRESH) {
        	/*
		 * statdelay_udelay is zero when files are being sent at the
		 * correct rate, positive when sending too fast (requiring
		 * sleep), and negative when sending too slow, i.e. while
		 * waiting for a new work item. to prevent sending bursts we
		 * don't allow statdelay_udelay to go negative.
		 */
                if (pw_ctx->statdelay_udelay < 0)
                        pw_ctx->statdelay_udelay = 0;
                goto out;
        }

        /* Don't allow a sleep of more than a second with the minimal rate of
         * 1 file/sec  */
        if (pw_ctx->statdelay_udelay > USECS_PER_SEC)
                pw_ctx->statdelay_udelay = USECS_PER_SEC;
        siq_nanosleep(0, pw_ctx->statdelay_udelay * 1000);

        /* We're caught up to our rate */
        pw_ctx->statdelay_udelay = 0;
        gettimeofday(&pw_ctx->statdelay_lasttv, 0);

 out:
	return;
}

int work_resp_stf_callback(struct generic_msg *m, void *ctx);
int split_req_callback(struct generic_msg *srm, void *ctx);
void send_work_request(struct migr_pworker_ctx *pw_ctx, bool initial);
void send_wi_lock_msg(int sworker, uint64_t wi_lin, bool lock);

void
migr_continue_generic_file(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, migr_pworker_func_t return_func,
    struct isi_error **error_out);

bool
migr_start_ads(struct migr_pworker_ctx *pw_ctx, int fd, struct stat *st,
    char *debug_fname, enc_t debug_enc, struct isi_error **error_out);

void
migr_continue_ads_dir_for_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_ads_dir_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

bool
migr_continue_file(struct migr_pworker_ctx *pw_ctx,
    struct migr_file_state *file, struct isi_error **error_out);

void
migr_continue_reg_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_ads_file_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_ads_file_for_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_ads_delete_for_file(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_regular_comp_lin_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_comp_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_comp_map_process(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
stf_continue_comp_conflict_cleanup(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
migr_continue_comp_dir_dels(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);

void
send_link_message(struct migr_pworker_ctx *pw_ctx, ifs_lin_t parent_lin,
    struct dirent *d, bool new_singly_linked, bool conflict_res,
    bool skip_chkpt, bool chkpt_child_lin, struct isi_error **error_out);

void
is_special_file_link(struct migr_pworker_ctx *pw_ctx, struct link_msg *lp,
    struct isi_error **error_out);

/* STF based functions */
void
stf_continue_lin_commit(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_data(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_transfer(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_change_transfer(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_ads(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_deletes(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_unlink(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_lin_link(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_request_new_work(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
free_work_item(struct migr_pworker_ctx *pw_ctx, struct rep_ctx *rep,
    struct isi_error **error_out);
void
read_work_item(struct migr_pworker_ctx *pw_ctx,
    struct generic_msg *m, struct rep_ctx *rep, 
    struct isi_error **error_out);
void
handle_work(struct migr_pworker_ctx *pw_ctx);
void
flush_stf_logs(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out);

bool
migr_continue_delete(struct migr_pworker_ctx *pw_ctx, siq_plan_t *plan,
    bool send_error, struct isi_error **error_out);
void
migr_send_idmap(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
int
extra_file_commit_callback(struct generic_msg *m, void *ctx);
void
clear_checkpoint(struct work_restart_state *work);
bool
stat_lin_snap(ifs_lin_t lin, ifs_snapid_t snap, struct stat *stat,
    bool missing_ok, struct isi_error **error_out);
void
set_hash_exception(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin,
    struct isi_error **error_out);
int
process_gensig_exit(void *ctx);
void
migr_continue_flip_lins(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
migr_continue_regular_comp_resync(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
stf_continue_conflict_list_begin(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
int
comp_target_work_range_callback(struct generic_msg *m, void *ctx);
int
comp_target_checkpoint_callback(struct generic_msg *m, void *ctx);
int
comp_map_callback(struct generic_msg *m, void *ctx);
bool
try_tw_split(struct migr_pworker_ctx *pw_ctx, bool *last_file,
		struct isi_error **error_out);
bool
try_btree_split(struct migr_pworker_ctx *pw_ctx, bool *last_file,
		struct isi_error **error_out);
bool
try_comp_target_split(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);
bool
try_snapset_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
bool
try_lin_division_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
bool
try_file_split(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out);
bool
try_worklist_split(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
try_work_split(struct migr_pworker_ctx *pw_ctx);

/* File split helper functions */
bool
init_file_split_work(struct migr_pworker_ctx *pw_ctx,
    struct work_restart_state *new_work, struct migr_file_state *file);

/* Domain treewalk functions */

void
dmk_start_work(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void 
dmk_continue_reg_dir(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);
void flush_burst_buffer(struct migr_pworker_ctx *pw_ctx);
void
add_split_work_entry(struct migr_pworker_ctx *pw_ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, struct isi_error **error_out);

/* helper function for corruption files logging */
void
log_corrupt_file_entry(struct migr_pworker_ctx *pw_ctx, const char *filename);
void
stf_comp_rplookup_links(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
void
revert_entry(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin,
    struct rep_entry *entryp, struct isi_error **error_out);
void
stf_comp_rplookup_lin(struct migr_pworker_ctx *pw_ctx, ifs_lin_t cur_lin,
    struct dirent *de, int lcount, bool resync_phase);

#endif /* PWORKER_H */
