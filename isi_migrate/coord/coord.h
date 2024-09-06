#ifndef COORD_H
#define COORD_H

#include "isi_migrate/config/bwt_conf.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_alert.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/cpools_sync_state.h"
#include "isi_migrate/migr/linmap.h"
#include "isi_migrate/migr/worklist.h"
#include "isi_migrate/migr/summ_stf.h"
#include "isi_snapshot/snapshot.h"
#include "isi_domain/dom.h"

extern struct coord_ctx g_ctx;
extern struct siq_gc_conf *global_config;
#define JOB_SPEC gc_job_spec->spec
extern struct siq_gc_spec *gc_job_spec;
#define JOB_SUMM gc_job_summ->root->summary
extern struct siq_gc_job_summary *gc_job_summ;

#define save_report() save_report_func(__func__)
#define SIQ_JS(s) (JOB_SUMM->total->state == SIQ_JS_##s)
#define SIQ_JNAME(rc) (((rc) && (rc)->job_name) ? (rc)->job_name : "noname")
#define SIQ_APPNAME(rc) ((rc) ? siq_action_type2app_name[(rc)->action] : \
        "noname")

#define SNAPDIR ".snapshot"

/* snap_(mode == 0x0) => (mode == "without snapshots") */
#define SIQ_SNAP_MODE_NONE	0x0
#define SIQ_SNAP_MODE_PRESET	0x1
#define SIQ_SNAP_MODE_MAKE_PRI	0x2
#define SIQ_SNAP_MODE_MAKE_SEC	0x4
#define MAX_TGT_TRIES 5

enum tgt_send_result {
	TGT_SEND_OK,
	TGT_SEND_FAIL,
	TGT_SEND_MIGR_PROCESS_ERR,
	TGT_SEND_MAX,
};

static uint32_t TIMER_DELAY = 5;

static const int CONNECT_WAIT[] = {15, 30, 60, 120, 240, 300};
static const int NUM_WAIT = sizeof(CONNECT_WAIT) / sizeof(int);

static char default_node_mask[SIQ_MAX_NODES + 1];

extern struct ptr_vec splittable_queue;
extern struct ptr_vec idle_queue;
extern struct ptr_vec split_pending_queue;
extern struct ptr_vec offline_queue;
extern struct ptr_vec dying_queue;


struct siq_snap {
	ifs_snapid_t	 id;
	SNAP		*snap;
	ifs_lockid_t	 lock;
	int		 root_fd;
};

struct conf_paths {
	char **paths;
	int npaths;
	int size;
};

struct migrconf {
	char	*job_dir;
	char	*job_id;
	char	*job_name;
	char	*path;
	struct conf_paths paths;
	struct conf_paths tmp_paths;
	char	*predicate;
	char	*target;
	char	*targetpath;
	char	*passwd;
	int	 loglevel;
	enum siq_action_type action;
	int	 flags;
	int	 depth;
	int	 wperhost;
	bool	 force_wpn;
	int	 max_wperpolicy;
	char	*bwhost;
	bool	 disable_bw;
	bool	 disable_th;
	bool	 disable_work;
	int	 noop;

	char	*log_removed_files_name;
	int	 fd_deleted; /* file desc for deleted files */
	char	*link_target;

	int	 max_errors;
	time_t	 pause_threshold;
	bool	 doing_diff_sync;
	bool	 skip_bb_hash;
	int	 conf_load_error;

	enum siq_snapmode source_snapmode;

	/* Target snapshot information */
	bool	 target_snapshot;
	unsigned expiration;
	char	*snap_sec_name;
	char	*snapshot_alias;

	/* A special job that deletes policies */
	bool	 is_delete_job;
	
	/* Target domain information */
	ifs_domainid_t domain_id;
	uint32_t domain_generation;

	char	*rename_snap;
	bool	 disable_fofb;
	bool database_mirrored;

	int skip_lookup;
	bool disable_bulk_lin_map;
	bool	 limit_work_item_retry;

	enum siq_deep_copy deep_copy;
	enum siq_job_action job_type;
	bool compliance_v2;

	int target_wi_lock_mod;
	bool target_work_locking_patch;
};

struct tmonitor_ctx {
	int sock;
	char *target_name;
	int sworker_port;
	bool target_restrict;
	const struct inx_addr *forced_addr;
	char *policy_id;
	char *policy_name;
	char *src_root_dir;
	char *target_dir;
	uint64_t run_id;
	char last_target_cluster_id[SIQ_MAX_TARGET_GUID_LEN];
	int loglevel;
	uint32_t flags;
};

struct work_item {
	/* work item lin */
	ifs_lin_t wi_lin;
	/* split work item lin if split_req sent */
	ifs_lin_t split_wi_lin;
	struct fmt history;
	uint32_t retries;
	bool count_retry;
};

struct work_item_node {
	struct work_item *work;
	TAILQ_ENTRY(work_item_node) ENTRIES;
};
TAILQ_HEAD(work_item_queue, work_item_node);

struct worker {
	bool		 configured;
	bool		 dying;
	bool		 reset;
	int		 id;
	struct node	*s_node;  ////这个worker放在s_node上
	struct node	*t_node;
	int		 process_id;
	time_t		 last_work;
	int		 socket;          ////和 pworker daemon创建出的pworker通信的socket
	int		 notified;
	time_t		 last_split;
	time_t		 last_connect;
	int		 connect_failures;
	struct work_item *work;
	int		 bw_ration;
	int		 bw_last_sent;
	int		 th_ration;
	int		 th_last_sent;
	bool		 shuffle;
};

struct workers {
	int count;
	struct worker **workers;
	int connected;
};

struct node {
	int		lnn;
	char		*host;
	int		 n_workers;
	bool		 down;

	/* only used by source nodes */
	int		 bytes_sent;
	int		 bytes_sent_last;
	int		 files_sent;
	int		 files_sent_last;
};

struct nodes {
	int count;
	int node_worker_limit;
	struct node **nodes;
	char **ips;
};

struct coord_ctx {
	struct migrconf   rc;

	/* node / worker lists */
	struct workers	 *workers;
	struct nodes 	 *src_nodes;
	struct nodes 	 *tgt_nodes;

	/* temporary lists for worker (re)configuration */
	struct nodes 	 *new_src_nodes;
	struct nodes 	 *new_tgt_nodes;
	
	int		  items;
	bool		  done;
	bool		  timer;
	struct work_req_msg stats;
	time_t		  start;	/* job start time. */
	time_t		  last_time;	/* time for pruning */
	time_t		  conn_alert;	/* State for no-conn alert. */
	int		  reapruns;
	FILE		 *frepeat;
	bool		  job_had_error;
	void *		  snaptimer;
	time_t		  old_time;
	time_t		  new_time;
	char *		  restrict_by;
	const struct inx_addr *forced_addr;
	bool		  started;
	struct path	  source_base;
	struct path	  target_base;
	uint64_t	  run_id;

	int		  bw_host;	/* socket to bandwidth host (if any) */
	int		  th_host;	/* socket to throttle host */
	int		  work_host;	/* socket to worker host */
	int		  bw_last;	/* to calc delta for bw stat update */
	int		  th_last;	/* to calc delta for th stat update */
	int		  bw_ration;	/* current job bandwidth ration */
	int		  th_ration;	/* current job throttle ration */
	uint32_t	  wk_ration;	/* current job worker ration */
	int		  total_wpn;	/* total workers if wpn is used */
	char *		  init_node_mask;
	char *		  work_node_mask;
	char *		  disconnected_node_mask;
	time_t		  bw_last_conn_attempt;
	time_t		  bw_last_conn;
	int		  bw_conn_failures;
	time_t		  th_last_conn_attempt;
	time_t		  th_last_conn;
	int		  th_conn_failures;
	time_t		  work_last_conn_attempt;
	time_t		  work_last_conn;
	int		  work_conn_failures;
	int		  work_fast_start_count; // Worker pool/worker startup
	int		  work_fast_kill_count;
	int *		  work_lnn_list;
	size_t		  work_lnn_list_len;
	time_t		  work_last_worker_inc;

	bool		  initial_sync;

	struct siq_source_record *record;

	struct siq_snap prev_snap;
	struct siq_snap curr_snap;

	/* STF related fields */
	struct rep_ctx *rep; /* Current rep context */
	struct cpss_ctx *cpss;
	struct stf_job_state job_phase; /* Current phase, all next states */
	ifs_lin_t cur_work_lin; /* Current lin used for new work item */
	bool stf_sync;
	bool stf_upgrade_sync;	/* Existing policy upgrading to STF */

	bool stf_downgrade_op;
	bool stf_target_downgrade_ok;
	bool waiting_for_tgt_msg;
	bool restore; /* Set if its snapshot restore operation */
	bool use_restore_job_vers; /* Set if its snapshot restore operation */
	bool failover; /* Set if its failover operation */
	bool failover_revert; /* Set for failover revert operation. */
	int failback_action;
	struct tmonitor_ctx tctx;
	unsigned curr_ver;

	bool snap_revert_op;
	ifs_snapid_t snap_revert_snapid;
	bool tgt_tmp_removed;
	bool workers_configured;

	/* Retain snapshots and incremental repstate for changelist job. */
	bool changelist_op;

	int active_workers;

	char *comp_conflict_log_dir;

	/* Committed platform version at process init. */
	struct version_status ver_stat;

	/* Version of the current job. */
	struct siq_job_version job_ver;

	/* Status of the local version of the current job compared with that
	 * of the latest successful job, if any. */
	bool local_job_ver_update;
	uint32_t prev_local_job_ver;

	struct isi_cbm_file_versions *common_cbm_file_vers;
};

static inline bool
is_treewalk(struct coord_ctx *ctx)
{
	if (ctx->initial_sync || ctx->stf_upgrade_sync ||
	    !ctx->stf_sync) {
		return true;
	} else {
		return false;
	}
}

void save_report_func(const char *func);

/* dynamic_worker_allocation */
void configure_workers(bool initial);
void cleanup_node_list(struct nodes *);
void shuffle_offline_workers(void);

/* coord target monitor helpers */
void connect_to_tmonitor(struct isi_error **);
void send_status_update_to_tmonitor(void);
void disconnect_from_tmonitor(void);

void connect_bandwidth_host(void);
void connect_throttle_host(void);
void connect_worker_host(void);

/* callbacks */
int jobctl_callback(__unused void *ctx);
int siq_error_callback(struct generic_msg *m, void *ctx);
int task_complete_ack_callback(struct generic_msg *m, void *ctx);
int group_callback(void *ctx);

int get_cluster_ips(char *, int, uint32_t, char ***, struct isi_error **);

void set_job_state(enum siq_job_state state);

void clear_composite_job_ver(struct siq_source_record *srec, bool restore);
void get_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver);
void get_prev_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver);
void set_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver);
void set_prev_composite_job_ver(struct siq_source_record *srec, bool restore,
    struct siq_job_version *job_ver);

void process_paths(struct siq_conf *gc, struct migrconf *rc,
    ifs_snapid_t snapid);
int migr_readconf(int argc, char **argv, struct siq_conf *gc,
    struct migrconf *rc, struct siq_source_record **record);
char *expand_snapshot_patterns(char **entry,
    const char *policy_name, const char *src_cluster_name);

void fatal_error(bool retry, enum siq_alert_type type, const char *fmt, ...);
void fatal_error_from_isi_error(bool retry, struct isi_error *error_in);

void get_error_str(struct generic_msg *m, struct coord_ctx *ctx,
    char **return_str);

int dump_status(void *ctx);

int redistribute_work_callback(void *ctx);
int connect_worker(struct worker *);

void initialize_queues(void);
void initialize_workers(void); ///没有定义，没有用到过

bool load_recovery_items(struct isi_error **error_out);

void register_redistribute_work(void);
void register_timer_callback(int delay);
void register_new_lock_renewal(void);
void register_latest_lock_renewal(void);

void update_bandwidth(struct worker *w, bool force);
void update_throttle(struct worker *w, bool force);
int bwt_update_timer_callback(void *ctx);
void reallocate_worker(void);
int redistribute_workers(void *ctx);

void check_and_create_snap(void);
void remove_snapshot_locks(void);
void load_and_lock_snap_or_error(struct siq_snap *, ifs_snapid_t, bool,
    const char *);

void finish_deleting_snapshot(bool);
void delete_old_snapshot(void);
void delete_new_snapshot(void);
void checkpoint_next_phase(struct coord_ctx *ctx,
    bool update, char *tw_data, size_t tw_size,
    bool job_init, struct isi_error **error_out);
void add_first_work_item(struct coord_ctx *ctx, struct isi_error **error_out);
bool load_or_create_recovery_state(struct coord_ctx *ctx, struct isi_error **);
void path_stat(int ifs_fd, struct path *path, struct stat *st,
    struct isi_error **error_out);

void setup_restrictions(char *restrict_by, bool force_interface);
void get_source_ip_list(void);
unsigned test_connection(void);
void post_primary_success_work(void);
void do_target_snapshot(char *snapshot_name);
void target_cleanup_complete(void);
void mirrored_repstate_cleanup(void);

bool allow_failback_snap_rename(struct coord_ctx *ctx);
void on_failover_restore_done(struct isi_error **);
void on_failover_revert_done(struct isi_error **);
void on_failback_prep_restore_done(struct isi_error **);
bool check_or_repair_for_failback_prep(struct coord_ctx *ctx, bool found,
    struct isi_error **);
void check_create_prep_restore_trec(struct isi_error **);
void failback_prep_restore_rep_rename(bool pre_restore, struct isi_error **);
void do_failback_prep_finalize(void);
void verify_failover_done(void);
void do_domain_treewalk(void);
void remove_all_work_items(struct rep_ctx *rep, struct isi_error **error_out);
void record_phase_start(enum stf_job_phase phase);
void post_success_prune_btrees(ifs_snapid_t kept_prev_snap,
    ifs_snapid_t kept_cur_snap);
int queue_size(struct ptr_vec *q);

enum tgt_send_result send_tgt_msg_with_retry(struct generic_msg *m,
    uint32_t resp_type, migr_callback_t resp_cb);

void move_snapshot_alias_from_head(const char *mirror_id,
    const char *policy_id, struct isi_error **error_out);
void
move_snapshot_alias_helper(ifs_snapid_t target_sid, ifs_snapid_t alias_sid,
    bool *found_target, bool *found_alias, struct isi_error **error_out);

void
pworker_tw_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct pworker_tw_stat_msg *ret, struct isi_error **error_out);
void
sworker_tw_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct sworker_tw_stat_msg *ret, struct isi_error **error_out);
void
pworker_stf_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct pworker_stf_stat_msg *ret, struct isi_error **error_out);
void
sworker_stf_stat_msg_unpack(struct generic_msg *m, uint32_t ver,
    struct sworker_stf_stat_msg *ret, struct isi_error **error_out);

#endif /* COORD_H */
