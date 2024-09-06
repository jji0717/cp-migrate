#ifndef SCHED_H
#define SCHED_H

/*
 *  pol_ctx - The context for change-based policies.
 */

struct pol_ctx {
	SLIST_ENTRY(pol_ctx) next;

	//  The snapshot identifier currently associated with the policy.
	ifs_snapid_t sid;

	//  The maximimum known snapshot id at the time of the last sid
	//  discovery attempt.
	ifs_snapid_t prev_max_sid;

	//  The set of known sids which are either:
	//      1) >= sid with no STF root path, or
	//      2) >= sid with an STF root path overlapping
	//         that of the current job, or
	//      3) >= prev_max_sid with no existing STF.
	struct snapid_set interesting_sids;

	//  Whether a sync/repl-worthy change has been detected.
	bool change_detected;

	//  The time that the last check occured.
	time_t last_check;

	//  The time that the last detection update occured.
	time_t last_update;

	// The context keeping track of delays that are imposed when
	// job fails.
	struct siq_policy_delay delay_ctx;

	//  The policy identifier.
	char pid[POLICY_ID_LEN + 1];
};

SLIST_HEAD(pol_ctx_list, pol_ctx);

struct pol_snap_ctx {
	SLIST_ENTRY(pol_snap_ctx) next;

	/* The last time we checked for a new snapshot */
	time_t last_check;

	/* The context keeping track of delays that are imposed when
	 * the job fails */
	struct siq_policy_delay delay_ctx;

	/* The policy identifier */
	char pid[POLICY_ID_LEN + 1];
};

SLIST_HEAD(pol_snap_ctx_list, pol_snap_ctx);

struct sched_node
{
	int devid;
	int lnn;
	int max_workers;
};

/*  ____       _              _    ____            _            _
 * / ___|  ___| |__   ___  __| |  / ___|___  _ __ | |_ _____  _| |_
 * \___ \ / __| '_ \ / _ \/ _` | | |   / _ \| '_ \| __/ _ \ \/ / __|
 *  ___) | (__| | | |  __/ (_| | | |__| (_) | | | | ||  __/>  <| |_
 * |____/ \___|_| |_|\___|\__,_|  \____\___/|_| |_|\__\___/_/\_\\__|
 */

struct sched_ctx
{
	char edit_dir[MAXPATHLEN + 1];
	char edit_jobs_dir[MAXPATHLEN + 1];
	char edit_run_dir[MAXPATHLEN + 1];
	char test_file[MAXPATHLEN + 1];

	int node_num;
	struct siq_policies *policy_list;
	enum siq_state siq_state;
	int log_level;
	bool max_jobs_running;
	bool rm_pidfile_on_exit;

	/* event based scheduler */
	int kq_fd;
	int kq_src_rec_fd;
	int kq_pols_fd;
	int kq_sched_event_fd;
	int kq_config_dir_fd;
	int kq_siq_conf_fd;
	int kq_group_fd;
	int kq_workers_fd;

	/* policy list change detection */
	struct timespec plist_mtime;
	time_t plist_check_time;

	/* change-based policy context list */
	struct pol_ctx_list pctx_list;

	/* snapshot-triggered sync policy context list */
	struct pol_snap_ctx_list snap_ctx_list;

	uint32_t siq_ver;

	/* A map mapping devid keys to struct sched_node values */
	struct int_map node_info;

	/* True iff SIQ license is enabled */
	bool siq_license_enabled;
};


#endif
