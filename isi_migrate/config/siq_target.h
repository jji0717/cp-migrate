#ifndef __SIQ_TARGET_H__
#define __SIQ_TARGET_H__

#include "siq_job.h"
#include <time.h>
#include "isi_migrate/siq_error.h"
#include "isi_domain/dom.h"

#ifdef __cplusplus
extern "C" {
#endif

enum siq_target_cancel_state {
	CANCEL_INVALID = -1,
	CANCEL_WAITING_FOR_REQUEST,
	CANCEL_REQUESTED,
	CANCEL_PENDING,
	CANCEL_ACKED,
	CANCEL_ERROR,
	CANCEL_DONE,
};

enum siq_target_cancel_resp {
	CLIENT_CANCEL_SUCCESS,
	CLIENT_CANCEL_ERROR,
	CLIENT_CANCEL_NOT_RUNNING,
	CLIENT_CANCEL_ALREADY_IN_PROGRESS,
};

enum siq_target_fofb_state {
	FOFB_NONE,
	FOFB_FAILOVER_STARTED,
	FOFB_FAILOVER_DONE,
	FOFB_FAILOVER_REVERT_STARTED,
	FOFB_FAILBACK_PREP_STARTED,
	FOFB_FAILBACK_PREP_DONE,
	FOFB_MAX
};
	

// The function siq_target_states_set() can set both job and cancel state
// simultaneously (so a change can be made while holding a single lock on the
// TDB). Use a bitmask to choose which, or both types of state are to be
// changed.
#define CHANGE_JOB_STATE		0x01
#define CHANGE_CANCEL_STATE		0x02

struct target_record {
	char *id;
	char *target_dir;
	char *policy_name;
	char *src_cluster_name;
	char *src_cluster_id;
	char *last_src_coord_ip;
	enum siq_job_state job_status;
	time_t job_status_timestamp;
	bool read_only;
	int monitor_node_id;
	int monitor_pid;
	enum siq_target_cancel_state cancel_state;
	bool cant_cancel;
	bool allow_target_dir_overlap;
	bool linmap_created;
	ifs_domainid_t domain_id;
	uint32_t domain_generation;
	uint32_t latest_snap;
	enum siq_target_fofb_state fofb_state;
	bool enable_hash_tmpdir;

	/* If configured, a snapshot alias will be taken in a sync
	 * and used in a failover to point to HEAD. */
	uint32_t latest_archive_snap_alias;
	uint32_t latest_archive_snap;

	struct siq_job_version composite_job_ver;
	struct siq_job_version prev_composite_job_ver;
	
	bool do_comp_commit_work;

	struct target_record* next;
};

struct target_records {
	struct target_record* record;
};

int siq_target_acquire_lock(const char *pid, int lock_type,
    struct isi_error **error_out);
struct target_record * siq_target_record_read(const char *id,
    struct isi_error **error_out);
void siq_target_record_write(struct target_record *record,
    struct isi_error **error_out);
void siq_target_record_free(struct target_record *record);

void siq_target_records_read(struct target_records **records,
    struct isi_error **error_out);
void siq_target_records_free(struct target_records *records);


struct target_record *
siq_target_record_find_by_id(struct target_records *records, const char *id);
bool siq_target_record_exists(const char *pid);
struct target_record *
siq_target_record_copy(struct target_record *to_copy);
int siq_target_states_set(const char *pid, int chosen_states,
    enum siq_job_state new_jstate, enum siq_target_cancel_state new_cstate,
    bool have_ex_lock, bool restore, struct isi_error **error_out);
enum siq_target_cancel_state
siq_target_cancel_state_get(const char *id, bool have_sh_lock,
    struct isi_error **error_out);
enum siq_target_cancel_resp
siq_target_cancel_request(const char *id, struct isi_error **error_out);
void siq_target_delete_record(const char *policy_id, bool record_only,
    bool have_lock, struct isi_error **error_out);
void siq_target_get_current_generation(char *policy_id, uint32_t *gen_out, 
    struct isi_error **error_out);

const char * siq_target_cancel_state_to_text(
    enum siq_target_cancel_state cstate);

enum siq_target_cancel_state text_to_siq_target_cancel_state(const char *ctext);

const char *siq_target_fofb_state_to_text(enum siq_target_fofb_state state);

enum siq_target_fofb_state text_to_siq_target_fofb_state(const char *text);

#ifdef __cplusplus
}
#endif

#endif /* __SIQ_TARGET_H__ */

