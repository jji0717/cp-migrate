#ifndef __SIQ_SOURCE_TYPES_H__
#define __SIQ_SOURCE_TYPES_H__

#include "siq_policy.h"

#define SIQ_MAX_TARGET_GUID_LEN 64
#define SIQ_SOURCE_RECORD_DELETE_SNAP_SIZE 2
#define SIQ_MAX_NODES 512

/*  ____       _ _                 _        _   _             
 * |  _ \ ___ | (_) ___ _   _     / \   ___| |_(_) ___  _ __  
 * | |_) / _ \| | |/ __| | | |   / _ \ / __| __| |/ _ \| '_ \ 
 * |  __/ (_) | | | (__| |_| |  / ___ \ (__| |_| | (_) | | | |
 * |_|   \___/|_|_|\___|\__, | /_/   \_\___|\__|_|\___/|_| |_|
 *                      |___/                                 
 */

enum siq_policy_action {
	SIQ_POLICY_FULL_SYNC = 0,
	SIQ_POLICY_INCREMENTAL_SYNC,
	SIQ_NUM_POLICY_ACTION,
};

#define SIQ_POLICY_ACTION_FOREACH(var) \
	for ((var) = 0; (var) < SIQ_NUM_POLICY_ACTION; (var)++)

struct fmt_conv_ctx siq_policy_action_fmt(enum siq_policy_action);
bool siq_policy_action_parse(const char *, enum siq_policy_action *);

/*      _       _          _        _   _             
 *     | | ___ | |__      / \   ___| |_(_) ___  _ __  
 *  _  | |/ _ \| '_ \    / _ \ / __| __| |/ _ \| '_ \ 
 * | |_| | (_) | |_) |  / ___ \ (__| |_| | (_) | | | |
 *  \___/ \___/|_.__/  /_/   \_\___|\__|_|\___/|_| |_|
 
 */
enum siq_job_action {
	SIQ_JOB_NONE = 0,
	SIQ_JOB_RUN,
	SIQ_JOB_ASSESS,
	SIQ_JOB_FAILOVER,
	SIQ_JOB_FAILOVER_REVERT,
	SIQ_JOB_FAILBACK_PREP,
	SIQ_JOB_FAILBACK_PREP_DOMAIN_MARK,
	SIQ_JOB_FAILBACK_PREP_RESTORE,
	SIQ_JOB_FAILBACK_PREP_FINALIZE,
	SIQ_JOB_FAILBACK_COMMIT,
	SIQ_NUM_JOB_ACTION,
};

#define SIQ_JOB_ACTION_FOREACH(var) \
	for ((var) = 0; (var) < SIQ_NUM_JOB_ACTION; (var)++)

struct fmt_conv_ctx siq_job_action_fmt(enum siq_job_action);
bool siq_job_action_parse(const char *, enum siq_job_action *);

/*  _____       _ _ _                 _       _____   _           _
 * |  ___| ___ |_| | |__   ___   ____| | __  /  ___|_| |_  ___  _| |_  ___
 * | |_   / _ \ _| | '_ \ / _ \ / ___| |/ /  \___  \_   _|/ _ \|_   _|/ _ \
 * |  _| | (_| | | | |_)_| (_| | (___|   (    ___) | | |_| ( | | | |_|  __/
 * |_|    \__._|_|_| .__/ \__._|\____|_|\_\  |_____/ \___|\__._| \___|\___|
 */

enum siq_failback_state {
	SIQ_FBS_NONE = 0,
	SIQ_FBS_PREP_INIT,
	SIQ_FBS_PREP_DOM_TREEWALK,
	SIQ_FBS_PREP_RESTORE,
	SIQ_FBS_PREP_FINALIZE,
	SIQ_FBS_SYNC_READY,
	SIQ_NUM_FAILBACK_STATES
};

#define SIQ_FAILBACK_STATE_FOREACH(var) \
    for ((var) = 0; (var) < SIQ_NUM_FAILBACK_STATES; (var)++)

struct fmt_conv_ctx siq_failback_state_fmt(enum siq_failback_state);
bool siq_failback_state_parse(const char *, enum siq_failback_state *);

/*      _       _        ___                           
 *     | | ___ | |__    / _ \__      ___ __   ___ _ __ 
 *  _  | |/ _ \| '_ \  | | | \ \ /\ / / '_ \ / _ \ '__|
 * | |_| | (_) | |_) | | |_| |\ V  V /| | | |  __/ |   
 *  \___/ \___/|_.__/   \___/  \_/\_/ |_| |_|\___|_|   
 */

enum siq_job_owner {
	SIQ_OWNER_NONE = 0,
	SIQ_OWNER_COORD,
	SIQ_NUM_JOB_OWNER,
};

#define SIQ_JOB_OWNER_FOREACH(var) \
	for ((var) = 0; (var) < SIQ_NUM_JOB_OWNER; (var)++)

struct fmt_conv_ctx siq_job_owner_fmt(enum siq_job_owner);
bool siq_job_owner_parse(const char *, enum siq_job_owner *);

/*  ____                             ____                        _ 
 * / ___|  ___  _   _ _ __ ___ ___  |  _ \ ___  ___ ___  _ __ __| |
 * \___ \ / _ \| | | | '__/ __/ _ \ | |_) / _ \/ __/ _ \| '__/ _` |
 *  ___) | (_) | |_| | | | (_|  __/ |  _ <  __/ (_| (_) | | | (_| |
 * |____/ \___/ \__,_|_|  \___\___| |_| \_\___|\___\___/|_|  \__,_|
 */

struct siq_source_pid {
	char pid[POLICY_ID_LEN + 1];
};

struct siq_source_record_fields {
	char			pid[POLICY_ID_LEN + 1];
	uint64_t		revision;
	
	bool			assess;

	bool			unrunnable;
	bool			policy_delete;
	bool			pol_stf_ready;
	bool			manual_failback_prep;

	enum siq_job_action	pending_job;

	enum siq_job_owner	current_owner;

	enum siq_failback_state failback_state;

	time_t			start_schedule;
	time_t			end_schedule;

	ifs_snapid_t		new_snap;
	ifs_snapid_t		latest_snap;
	bool			retain_latest_snap;
	ifs_snapid_t		manual_snap;

	ifs_snapid_t		restore_new_snap;
	ifs_snapid_t		restore_latest_snap;

	ifs_snapid_t		delete_snap[SIQ_SOURCE_RECORD_DELETE_SNAP_SIZE];

	bool			reset_pending;

	bool			database_mirrored;

	char			target_guid[SIQ_MAX_TARGET_GUID_LEN];
	char			conn_fail_nodes[SIQ_MAX_NODES + 1];

	time_t			job_delay_start;
	time_t			last_known_good_time;

	struct siq_job_version	composite_job_ver;
	struct siq_job_version	prev_composite_job_ver;

	struct siq_job_version	restore_composite_job_ver;
	struct siq_job_version	restore_prev_composite_job_ver;
};

struct siq_source_record {
	struct siq_source_record_fields _fields;	

	int _lock_fd;
	int _lock_state;
};

struct fmt_conv_ctx siq_source_fmt(const struct siq_source_record *);

#endif /* __SIQ_SOURCE_TYPES_H__ */

