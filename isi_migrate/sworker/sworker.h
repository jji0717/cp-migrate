#ifndef __SWORKER_H__
#define __SWORKER_H__

#include <sys/queue.h>
#include "isi_domain/dom.h"
#include "isi_migrate/migr/alg.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_migrate/migr/comp_conflict_log.h"
#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_migrate/siq_error.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/compliance_map.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/lin_list.h"
#include "isi_migrate/migr/cpools_sync_state.h"
#include "isi_migrate/migr/cpu_throttle.h"
#include "isi_burst_sdk/include/burst.h"
#include <isi_quota/isi_quota.h>
#include <isi_util/isi_error.h>
#define ISI_ERROR_PROTECTED
#include <isi_util/isi_error_protected.h>

#define BUFFER_SIZE (1024 * 128)

#define DEF_CLU_NAME "target"

#define LINMAP_NAME "linmap"

#define COPY_COLLISION_PREFIX "copy_collision"
#define TW_MV_TOPTMPNAME ".tw-tmp-"
#define TW_MV_TMPNAME ".dlin-"
#define TW_MV_SAVEDDIRNAMEFILE ".tw-tmp-name"

#define IMMUT_APP_FLAG_MASK (UF_DOS_RO|UF_IMMUTABLE|UF_APPEND|UF_NOUNLINK|SF_IMMUTABLE|SF_APPEND|SF_NOUNLINK)

/* While handling the ADS directory of a file during diff sync, files are
 * transferred using the regular sync methods */
#define DIFF_SYNC_BUT_NOT_ADS(sw_ctx)	((sw_ctx)->global.doing_diff_sync && \
	!(sw_ctx)->inprog.ads && !(sw_ctx)->global.doing_extra_file)

#define LIN_ACK_INTERVAL 10  /* In Secs */
extern struct siq_gc_conf* global_config;
extern struct migr_sworker_ctx *g_ctx;
struct isi_file_dsession;

/* LMAP_PENDING_LIMIT has to be less than MAX_BLK_OPS */
#define LMAP_PENDING_LIMIT	(MAX_BLK_OPS - 10)

struct pending_lmap_acks {
	int count;
	struct lmap_update lmap_updates[SBT_BULK_MAX_OPS];
};

/**
 * This holds inode state used to set the metadata
 * for a file or directory.
 */
struct common_stat_state
{
	mode_t		mode;
	uid_t		owner;
	gid_t		group;
	struct timespec atime;
	struct timespec mtime;
	char		*acl;
	int		reap;
	int		flags_set;
	uint64_t	size;
	fflags_t	flags;
	enum file_type 	type;
	bool		worm_committed;
	uint64_t	worm_retention_date;
};

/**
 * This holds the state relevant to a file currently
 * being synced.  Explicit metadata for the inode (rather
 * than for the sync operation of the file) should be
 * placed in common_stat_state.  Note that filenames are
 * included in this structure because they relate to the
 * link to the inode, rather than the inode itself.
 */
struct file_state {
	struct		common_stat_state stat_state;
	int		fd;
	char		*tname;
	char		*name;
	enc_t		enc;
	bool		full_transfer;
	enum file_type	type;

	/* Block based delta hashing state */
	off_t		cur_off;
	bool		skip_bb_hash;
	struct		hash_ctx *bb_hash_ctx;
	uint64_t	bytes_changed;

	/* For archive bit support */
	u_int64_t	rev;
	ifs_lin_t	src_dirlin;
	ifs_lin_t	src_filelin;
	uint64_t	src_dir_key;

	/* Error state for Scotch Bonnet+ */
	bool error_closed_fd;

	/* For use during a diff sync*/
	enum sync_state sync_state;
	bool		orig_is_reg_file;

	/* For cloudpools */
	struct isi_file_dsession *dsess;
	enum isi_cbm_backup_sync_type cp_sync_type;

	/* LIN on this cluster (secondary) */
	ifs_lin_t	tgt_lin;

	TAILQ_ENTRY(file_state) ENTRIES;
};

struct dir_state {
	struct		common_stat_state stat_state;
	char		*name;
	enc_t		enc;

	ifs_lin_t	src_dirlin;
	bool		dir_valid;
	uint64_t	last_file_hash;
};

TAILQ_HEAD(file_state_list, file_state);

struct file_ack_entry {
	struct ack_msg3 *msg;
	TAILQ_ENTRY(file_ack_entry) ENTRIES;
};

TAILQ_HEAD(file_ack_list, file_ack_entry);

/**
 * These are updated only during daemon startup or in
 * the work init message.
 */
struct migr_sworker_global_ctx {
	char	 	*secret;
	char		cluster_name[MAXHOSTNAMELEN];
	int		loglevel;
	char            *sync_id; /* Policy id. */
	uint64_t        run_id; /* Timestamp when job was started. */
	uint64_t        run_number; /* Number of times this job has run. */
	int		worker_id;
	char		*job_name;
	bool		doing_diff_sync;
	bool		doing_extra_file;
	bool 		logdeleted;
	enum siq_action_type action;	
	bool		initial_sync;
	bool		stf_sync;
	bool		stf_upgrade_sync;
	bool		restore;
	bool		failover;
	bool		compliance;
	bool		compliance_v2;
	bool		is_fake_compliance;
	int		compliance_dom_ver;
	int		primary; 	/* fd of primary */
	int		checksum_size; 
	struct path	source_base_path;
	int		source_base_path_len;
	struct path	target_base_path;
	int		hasher; 	/* hasher fd */
	enum hash_type	hash_type;
	bool		skip_bb_hash;
	char		src_cluster_name[MAXHOSTNAMELEN];
	char		comp_cluster_guid[MAXNAMELEN];
	bool		is_src_guid;
	ifs_domainid_t	domain_id;
	uint32_t	domain_generation;
	struct map_ctx *lmap_ctx;
	struct compliance_map_ctx *comp_map_ctx;
	struct lin_list_ctx *resync_ctx;
	struct map_ctx *tmp_working_map_ctx;
	struct uint64_uint64_map *tmp_working_map;
	ifs_lin_t	root_tmp_lin;
	bool		quotas_present;
	time_t		last_ack_time;
	uint32_t	stf_operations;
	ifs_lin_t	last_stf_slin;
	struct lmap_log	lmap_log;
	bool		lin_map_blk;
	unsigned	lmap_pending_acks;
	unsigned	curr_ver;
	struct rep_ctx	*rep_mirror_ctx;
	struct rep_log	rep_mirror_log;
	struct cpss_ctx	*cpss_mirror_ctx;
	struct	pending_lmap_acks lmap_acks;
	bool		mirror_check;
	bool		verify_tw_sync;
	struct HostInterfaceImpl *burst_host; //Burst logging
	bool		do_bulk_tw_lmap;
	struct file_ack_list *bulk_tw_acks;
	unsigned	bulk_tw_acks_count;
	uint32_t	lnn;

	/* Version of the current job. */
	struct siq_job_version job_ver;

	/* Status of the local version of the current job compared with that
	 * of the latest successful job, if any. */
	bool local_job_ver_update;
	uint32_t prev_local_job_ver;
	struct isi_hash	*tmpdir_hash;
	bool		enable_hash_tmpdir;

	int		wi_lock_timeout;
	int		wi_lock_mod;
	int		wi_lock_fd;
	/* verify added LINs during flip lins */
	bool flip_lin_verify;
};

/**
 * These are statistics related, although they may have
 * an actual functional purpose.
 */
struct migr_sworker_stats_ctx {
	struct siq_stat	*tw_cur_stats;
	struct siq_stat	*tw_last_stats;
	struct siq_stat	*stf_cur_stats;
	struct siq_stat	*stf_last_stats;
	unsigned 	io_errors;
	unsigned 	checksum_errors; /* Only for pre-2.5 pworkers */
	/*
	 * Size of current file (ads included)
	 * (needed for count recoverable bytes on src)
	 */
	u_int64_t 	curfile_size; 
	unsigned 	ads_of_curfile; /* Num ads that current file has */
	int		dirs;
};

/**
 * These are related to the current state of the directory/
 * file currently being processed.
 */
struct migr_sworker_inprogress_ctx {
	struct file_state 	fstate;
	struct file_state 	sstate;
	struct dir_state 	dstate;
	struct filelist	deleted_files;
	bool		file_obj; /* True if object is file or file's ads */
	unsigned 	result; /* new for TSM, result of receiving file */
	int		ads;

	/* For diff sync */
	struct file_state_list *fstates;
	int		fstates_size;
	/* Holds fstate from file begin until uattr msg for diff sync */
	struct file_state	*ds_tmp_fstate;

	/* For legacy list behavior */
	struct filelist	files[2]; /* Primary files & ads streams */
	int		incmp[2]; /* Don't delete if set */
};

/**
 * These are related to the current state of the worker.
 */
struct migr_sworker_worker_ctx {
	int		cwd_fd;
	int		saved_cwd;
	struct path	cur_path;
};

/**
 * This is the context for the target monitor.
 */
struct migr_tmonitor_ctx {
	int coord_fd;
	char *policy_id;
	char *policy_name;
	enum siq_target_cancel_state cancel_state;
	enum siq_job_state job_state;
	bool cleaned_up;
	bool legacy;
	bool restore;
	bool failback_prep;
	int num_nodes;
	char **ips;
	char *restrict_name;
	bool stf_sync;
	bool discon_enable_mirror;
	bool doing_comp_commit_finish;
};

/**
 * Delete state
 */
struct migr_sworker_delete_ctx {
	struct migr_dir	*mdir;
	bool in_progress;
	struct ptr_vec *hold_queue;
	struct ptr_vec *next_hold_queue;
	int op_count;
	int bookmark;
	bool error;
};

/**
 * Compliance FOFB State
 */
struct migr_sworker_comp_ctx {
	struct lin_list_ctx *worm_commit_ctx;
	struct lin_list_log worm_commit_log;
	struct lin_list_iter *worm_commit_iter;
	struct compliance_map_ctx *comp_map_ctx;
	struct compliance_map_log comp_map_log;
	struct compliance_map_iter *comp_map_iter;
	struct lin_list_ctx *worm_dir_dels_ctx;
	struct lin_list_log worm_dir_dels_log;
	struct lin_list_iter *worm_dir_dels_iter;
	ifs_lin_t comp_target_min_lin;
	ifs_lin_t comp_target_current_lin;
	ifs_lin_t comp_target_max_lin;
	ifs_lin_t comp_map_chkpt_lin;
	bool process_work;
	bool done;
	bool comp_map_split_req;
	bool comp_map_ufp;
	enum work_restart_type rt;
	bool curr_dir_delayed_delete;
};

/**
 * Root sworker context structure
 */
struct migr_sworker_ctx {
	struct migr_sworker_global_ctx global;
	struct migr_sworker_stats_ctx stats;
	struct migr_sworker_inprogress_ctx inprog;
	struct migr_sworker_worker_ctx worker;
	struct migr_sworker_delete_ctx del;
	struct migr_tmonitor_ctx tmonitor;
	struct migr_cpu_throttle_ctx cpu_throttle;
	struct migr_sworker_comp_ctx comp;
};

/*
 * Bug 215632
 *
 * TMP HASH settings
 *
 * A single monolithic temporary working directory (TMP_WORKING_DIR) can lead
 * to multiple sworkers competing for locks to perform namespace operations in
 * that directory. In the worst case, this can lead to individual namespace
 * operations like rename and unlink taking time measured in whole wall clock
 * seconds.
 * This change adds sub-directories to temporary working directories,
 * including quota temporary directories, to reduce the likelihood that a
 * large number of sworkers could be simultaneously attempting namespace
 * operations on the same temporary working directory. The new hash directories
 * take the form TMP_WORKING_DIR/TMPDIR_HASH_FMT.
 */
struct tmpdir_hash_key {
	ifs_lin_t root_lin;
	ifs_lin_t mask;
};

struct tmpdir_hash_entry {
	struct isi_hash_entry entry;
	struct tmpdir_hash_key key;
	ifs_lin_t lin;
};

/*
 * Number of LSBs to use for directory hashing.
 * (Number of hash directories created) = 2**TMPDIR_BITS
 */
#define TMPDIR_BITS (6)
/*
 * Max number of TMPDIR_BITS allowed.
 * Must match to format string in TMPDIR_HASH_FMT
 */
#define TMPDIR_MAX_BITS (16)
/* Enforce TMPDIR_MAX_BITS limit */
#define TMPDIR_MASK ((1<<TMPDIR_BITS)-1) & ((1<<TMPDIR_MAX_BITS)-1)
#define TMPDIR_HASH_PREFIX ".tmp_hash-"
/* Zero-pad should match TMPDIR_MAX_BITS */
#define TMPDIR_HASH_FMT "%s%04llx"

void send_stats(struct migr_sworker_ctx *);
void check_connection(struct migr_sworker_ctx *);
bool entry_in_list(const char *, int, const char *, int,
    struct migr_sworker_ctx *);
void process_list(const char *, int, unsigned, unsigned, bool, bool,
    struct migr_sworker_ctx *, struct isi_error **);
void delete(char *, enc_t, struct migr_sworker_ctx *, struct isi_error **);

int forward_to_hasher_callback(struct generic_msg *m, void *ctx);
int forward_to_pworker_callback(struct generic_msg *m, void *ctx);
int short_hash_req_callback(struct generic_msg *m, void *ctx);
int work_init_callback(struct generic_msg *m, void *ctx);
int dir4_callback(struct generic_msg *m, void *ctx);
void write_file_sparse(struct file_state *fstate, off_t off, int size,
    unsigned source_msg_version, ifs_lin_t *wlin, struct isi_error **error_out);
int snap_callback(struct generic_msg *m, void *ctx);
struct file_state *ds_file_done(
    struct file_done_msg *fdone, struct migr_sworker_ctx *sw_ctx);

void ds_dir4(struct dir_msg4 *msg4, bool is_new_dir,
    struct migr_sworker_ctx *sw_ctx);

struct file_state *ds_file_begin(
    struct file_begin_msg *begin,
    bool file_exists,
    int nlinks,
    struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

struct file_state *ds_file_data(
    struct file_data_msg *fdata,
    struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

void send_error_and_exit(int fd, struct isi_error *error);
void send_error_msg_and_exit(int fd, int error_number, const char *err_msg);
void migr_sworker_register_legacy(struct migr_sworker_ctx *sw_ctx);
void old_finish_file(struct migr_sworker_ctx *sw_ctx);
struct target_record *check_for_5_0_policy(struct target_records *tdb,
    char *src_cluster_name, char *policy_name, char *target_dir);
bool remove_duplicate_5_0_policy(char *policy_id,
    struct isi_error **error_out);
void handle_non_target_aware_source(const struct old_work_init_msg6 *,
    struct migr_sworker_global_ctx *global);

void finish_dir(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out);

uint32_t sworker_checksum_compute(char *data, int size);
void
send_short_hash_resp(
	enum sync_state sync_state,
	uint64_t src_dkey,
	struct migr_sworker_ctx *sw_ctx);
void diff_sync_short_hash(
	struct file_begin_msg *begin,
	struct file_state *fstate,
	struct migr_sworker_ctx *sw_ctx,
	struct isi_error **error_out);

bool
move_or_remove_dirent(struct migr_sworker_ctx *sw_ctx, int dirfd, int tmpfd,
    struct stat *st, char *d_name, unsigned d_enc,
    struct isi_error **error_out);

void
move_dirents(struct migr_sworker_ctx *, int dirfd,
    int *dir_unlinks, int *file_unlinks, struct isi_error **error_out);

void
delete_lin(struct migr_sworker_ctx *, uint64_t dlin,
    struct isi_error **error_out);

void
link_lin(struct migr_sworker_ctx *, uint64_t dlin, uint64_t direntlin,
    uint64_t direntslin, char *d_name, unsigned d_enc, unsigned d_type,
    struct isi_error **error_out);

void
unlink_lin(struct migr_sworker_ctx *, uint64_t dlin, uint64_t direntlin,
    char *d_name, unsigned d_enc, unsigned d_type,
    struct isi_error **error_out);

int
lin_update_callback(struct generic_msg *m, void *ctx);
int
old_lin_update_callback(struct generic_msg *m, void *ctx);
int
lin_commit_callback(struct generic_msg *m, void *ctx);
int
extra_file_callback(struct generic_msg *m, void *ctx);
int
extra_file_commit_callback(struct generic_msg *m, void *ctx);
int
delete_lin_callback(struct generic_msg *m, void *ctx);
int
link_callback(struct generic_msg *m, void *ctx);
int
unlink_callback(struct generic_msg *m, void *ctx);
int
disconnect_callback(struct generic_msg *m, void *ctx);
int
stale_dir_callback(struct generic_msg *m, void *ctx);

void
target_work_lock_msg_unpack(struct generic_msg *m,
    struct target_work_lock_msg *ret, struct isi_error **error_out);
int
target_work_lock_callback(struct generic_msg *m, void *ctx);

bool
finish_bb_hash(struct migr_sworker_ctx *sw_ctx,
    struct file_done_msg *done_msg, bool *hash_matches,
    struct isi_error **error_out);


static void inline set_sync_state(
	struct file_state *fstate,
	enum sync_state state)
{
	ASSERT(fstate);
	log(TRACE, "Changing sync state of %s: %s ==> %s",
	    fstate->name,
	    sync_state_str(fstate->sync_state),
	    sync_state_str(state));
	fstate->sync_state = state;
}

inline static bool
match_5_0_policy(char *policy_id_5_0, struct target_record *trec)
{
	return strcmp(policy_id_5_0, trec->id) == 0;
}

char *make_5_0_policy_id(char *src_cluster_name, char *policy_name,
    char *target_dir);

void
verify_run_id(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out);

void
target_monitor(int coord_fd, struct migr_sworker_ctx *sw_ctx,
    struct target_init_msg *tim, struct generic_msg *resp);

void
target_path_confirm(struct target_records *tdb, const char *policy_id, 
    const char *policy, const char *src_cluster, const char *target_dir, 
    unsigned int flags, struct isi_error **error_out);

void
safe_rmrf(int dirfd, const char *name, enc_t enc, int sock_to_monitor, 
    ifs_domainid_t domain_id, uint32_t domain_generation,
    struct isi_error **error_out);

void
check_connection_with_primary(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

void
target_confirm(struct migr_sworker_ctx *sw_ctx,
    struct target_init_msg *tim, struct isi_error **error_out);

void
send_err_msg(int err, struct migr_sworker_ctx *sw_ctx);

void
clear_flags(struct stat *st, unsigned long flags_to_clear,
    char *name, int enc, struct isi_error **error_out);

void
clear_user_attrs(int fd, struct stat *st, struct isi_error **error_out);

struct file_state *
choose_or_create_fstate(struct migr_sworker_ctx *sw_ctx,
    struct file_begin_msg *begin, bool file_exists, int nlinks,
    struct isi_error **error_out);

int
make_hard_link(char *name, enc_t enc, uint64_t lin_target, int at_fd,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out);
    
int
open_or_create_reg(struct migr_sworker_ctx *sw_ctx, int dir_fd,
    char *name, enc_t enc, uint64_t *lin_out, struct isi_error **error_out);

int
open_or_create_sym(struct migr_sworker_ctx *sw_ctx, int dir_fd,
    char *link_name, enc_t link_enc, char *name, enc_t enc, uint64_t *lin_out,
    struct isi_error **error_out);

int
open_or_create_special(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, enum file_type type, unsigned int major, unsigned int minor,
    uint64_t *lin_out, struct isi_error **error_out);

int
open_or_create_socket(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, uint64_t *lin_out, struct isi_error **error_out);

void
initialize_fstate_fields(struct migr_sworker_ctx* sw_ctx,
    struct file_state *fstate, struct file_begin_msg *begin);

int
open_or_create_fifo(struct migr_sworker_ctx *sw_ctx, int dir_fd, char *name,
    enc_t enc, uint64_t *lin_out, struct isi_error **error_out);

void
clear_flags_fd(struct stat *st, unsigned long flags_to_clear, 
    int fd, char *debug_name, struct isi_error **error_out);

/* Error handling */
enum siq_sw_fs_error_type {
	SW_ERR_FILE = 0,
	SW_ERR_DIR,
	SW_ERR_ADS,
	SW_ERR_LIN,
	SW_ERR_MAX
};
extern const char *siq_sw_fs_error_type_to_str[SW_ERR_MAX+1];

bool
check_dom_gen(struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out);

void
sw_process_fs_error(struct migr_sworker_ctx *sw_ctx,
    enum siq_sw_fs_error_type type, char *name, enc_t enc, uint64_t lin,
    struct isi_error *error);

int
check_lin_is_worm(const ifs_lin_t lin, bool *is_worm, struct worm_state *worm);

void
apply_attrs_to_fd(int fd, struct stat *old_st, struct ifs_syncattr_args *args,
    char *acl, struct ifs_security_descriptor *sd_in,
    enum ifs_security_info *si_in, struct worm_state *old_worm, bool compliance,
    struct isi_error **error_out);

void
apply_attributes(struct migr_sworker_ctx *sw_ctx, int fd,
    struct common_stat_state *stat_state, bool data_changed, bool is_stf,
    struct isi_error **error_out);

void
apply_security_to_fd(int fd, fflags_t src_flags,
    struct stat *old_st,
    uid_t uid, gid_t gid, mode_t mode, char *acl,
    struct isi_error **error_out);
void
handle_lin_ack(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin,
     struct isi_error **error_out);
void
send_lmap_acks(struct migr_sworker_ctx *sw_ctx, ifs_lin_t slin, ifs_lin_t dlin,
    bool send_only);
ifs_lin_t
get_hash_dirname(ifs_lin_t lin, char **dirname_out);
void
reuse_old_lin(struct migr_sworker_ctx *sw_ctx, struct link_msg *lm,
    struct isi_error **error_out);

/********************TEMP DIRECTORY FUNCTIONS************************/
int
get_tmp_fd(ifs_lin_t file_lin, struct migr_sworker_global_ctx *global,
    struct isi_error **error_out);
ifs_lin_t
get_tmp_lin(ifs_lin_t file_lin, struct migr_sworker_global_ctx *global,
    ifs_lin_t *tmp_lin_out, struct isi_error **error_out);
bool
is_tmp_lin(ifs_lin_t dir_lin, struct migr_sworker_global_ctx *global,
    struct isi_error **error_out);
void
tree_merge(ifs_lin_t from_root_lin, ifs_lin_t to_root_lin,
    struct isi_error **error_out);
int
initialize_tmp_map(const char *root_path, const char *policy_id,
    struct migr_sworker_global_ctx *global, bool restore,
    bool create_dirs, struct isi_error **error_out);
ifs_lin_t *
tmp_map_find(struct migr_sworker_global_ctx *global, ifs_lin_t qlin,
    struct isi_error **error_out);
void
cleanup_tmp_dirs(const char *root_path, bool restore,
    struct migr_sworker_ctx *sw_ctx, struct isi_error **error_out);
void
merge_copy_collision_entries(int rootfd, char *policy_id,
    struct isi_error **error_out);
void
siq_enc_renameat(int from_dfd, const char *from_name, enc_t from_enc,
    int to_dfd, const char *to_name, enc_t to_enc,
    struct isi_error **error_out);
void
write_treemv_orig_name(const char *dir_name, enc_t dir_enc, int dlin_fd,
    ifs_lin_t flin, struct isi_error **error_out);
void
read_treemv_orig_name(int to_fd, const char *twtmp_name, char **dir_name,
    enc_t *dir_enc, struct isi_error **error_out);
/********************QUOTA SUPPORT FUNCTIONS*************************/
struct qdi_wrapper {
	struct quota_domain_id qdi;
	STAILQ_ENTRY(qdi_wrapper) next;
};

STAILQ_HEAD(quota_queue_head, qdi_wrapper);
void
check_quota_delete(ifs_lin_t lin, struct isi_error **error_out);
bool
crossed_quota(ifs_lin_t a_lin, ifs_lin_t b_lin, struct isi_error **error_out);
bool
check_quota_ready(const struct quota_domain *domain,
    struct isi_error **error_out);
struct quota_queue_head *
get_quotas_for_lin(ifs_lin_t lin, enum quota_lin_query,
    struct isi_error **error_out);
void
treemv_restart(struct migr_sworker_ctx* ctx, ifs_lin_t plin, ifs_lin_t flin,
    ifs_lin_t tlin, int fenc, char *fname, char *curfname,
    bool save_name, struct isi_error **error_out);
void
treemv(struct migr_sworker_ctx* ctx, ifs_lin_t parent_lin, int parent_fd,
    ifs_lin_t flin, int to_fd, int new_enc, char *new_name, char *cur_name,
    struct dirent* de, bool save_name, struct isi_error** error_out);

/*******************COMPLIANCE FOFB FUNCTIONS************************/
void
edit_target_record_comp_commit(char *policy_id, bool new_value,
    struct isi_error **error_out);
void
comp_commit_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);
int
comp_commit_begin_callback(struct generic_msg *m, void *ctx);
int
comp_target_work_range_callback(struct generic_msg *m, void *ctx);
ifs_lin_t
comp_find_split(struct migr_sworker_comp_ctx *comp_ctx,
    ifs_lin_t cur_lin, ifs_lin_t max_lin, struct isi_error **error_out);
int
comp_target_checkpoint_callback(struct generic_msg *m, void *ctx);
int
process_worm_commits(void *ctx);
void
add_worm_commit(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out);
int
comp_map_begin_callback(struct generic_msg *m, void *ctx);
void
add_peer_lin_to_resync(int dir_fd, struct lin_list_ctx *ctx,
    struct isi_error **error_out);
int
comp_map_callback(struct generic_msg *m, void *ctx);
int
process_compliance_map(void *ctx);
void
comp_map_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);

void
comp_dir_dels_send_periodic_checkpoint(struct migr_sworker_ctx *sw_ctx,
    struct isi_error **error_out);
int
comp_dir_dels_begin_callback(struct generic_msg *m, void *ctx);
int
process_worm_dir_dels(void *ctx);
void
add_worm_dir_del(struct migr_sworker_ctx *sw_ctx, ifs_lin_t lin,
    struct isi_error **error_out);

#endif // __SWORKER_H__
