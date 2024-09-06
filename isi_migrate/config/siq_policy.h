#ifndef __SIQ_POLICY_H__
#define __SIQ_POLICY_H__

#include <stdbool.h>
#include <stddef.h>

#include "isi_migrate/config/siq_gconfig_gcfg.h"
#include "isi_migrate/sched/siq.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_source_types.h"
#include "isi_migrate/siq_error.h"

#include "isi_util/isi_hash.h"

#define SPEC_FILENAME "spec.gc"

// From python TsmPolicyChange::get_def_snap_*(). If they change, these 
// have to change.
#define DEF_TARGET_SNAP_PATTERN \
    "SIQ-%{SrcCluster}-%{PolicyName}-%Y-%m-%d_%H-%M-%S"
#define DEF_TARGET_SNAP_ALIAS "SIQ-%{SrcCluster}-%{PolicyName}-latest"

/* Min and max policy priorities. These are used by PAPI to validate priority
 * settings. Update these as needed to add more priority levels */
#define MIN_POL_PRIORITY 0
#define MAX_POL_PRIORITY 1

struct siq_policies {
	struct siq_policies_root *root;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;

	int lock_fd;
	bool internal_lock;
	bool upgrading;
};

struct siq_gc_spec {
	struct siq_spec *spec;
	struct gci_tree tree;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;
};
extern struct gci_tree siq_gc_policies_gci_root;

struct target_record;
enum siq_job_action;

__BEGIN_DECLS

/* Create an empty list of policies. */
struct siq_policies* siq_policies_create(void);

/* Load policies from global policies list.
 * Returns the list or NULL in case of error */
struct siq_policies* siq_policies_load(struct isi_error **error_out);

/* Load policies from global policies list without a lock.   
 * Returns the list or NULL in case of error */
struct siq_policies* siq_policies_load_nolock(struct isi_error **error_out);

/* Load policies from global policies list.  
 * Returns the list or NULL in case of error.
 * Policies that are returned are read only. */
struct siq_policies* siq_policies_load_readonly(struct isi_error **error_out);
	
/* Sets upgrade mode on the policies list */
void siq_policies_set_upgrade(struct siq_policies* pl);

/* A list of policies ids */
struct siq_pids_list {
	int size;
	char **ids;
};

/* List policies ids within the policies list Returns a list of ids, otherwise
 * NULL */
struct siq_pids_list *siq_get_pids(const struct siq_policies* pl);

/* Free policies ids list */
void siq_free_pids(struct siq_pids_list *pidsl);

/* Datapath Hash Computation */
struct siq_policies_save_hash_entry {
	struct isi_hash_entry hash_entry;
	char hash_key[0];  //Variable length key for isi_hash.  Must be last!
};

/* Overrides siq_policies */
void siq_policy_force_update_datapath_hash(struct siq_policies *policies, 
    struct siq_policy *policy);

/* Save policies list to the global persistent storage
 * Returns 0, or < 0 in case of an error */
int siq_policies_save(struct siq_policies* policies, 
    struct isi_error **error_out);

/* Free policies list.  A destructor can't fail */
void siq_policies_free(struct siq_policies* pl);

/* Policy methods */

/* Create an empty policy, returns NULL in case of an error */
struct siq_policy* siq_policy_create(void);

struct siq_policy* siq_failover_policy_create_from_target_record(
    const struct target_record *record, const bool revert);

/* Get a pointer to a policy */
struct siq_policy* siq_policy_get_by_pid(struct siq_policies *policies, 
    const char *pid);
struct siq_policy* siq_policy_get_by_mirror_pid(struct siq_policies *policies, 
    const char *pid);
struct siq_policy* siq_policy_get_by_name(struct siq_policies *policies, 
    const char *name);
struct siq_policy *siq_policy_get(struct siq_policies *pl, const char *input, 
    struct isi_error **error_out);

/* Schedules a policy for deletion.  In case of an error returns < 0.  
 * If a policy doesn't exist in the given list an error is returned */
int siq_policy_remove(struct siq_policies *policies, 
    struct siq_policy *policy, struct isi_error **error_out);
int siq_policy_remove_by_pid(struct siq_policies *policies, char *pid, 
    struct isi_error **error_out);
int siq_policy_remove_by_name(struct siq_policies *policies, char *name, 
    struct isi_error **error_out);

/* Actually deletes a policy.  This should only be called by the coordinator
 * during a delete job.  In case of an error returns < 0.  If a policy 
 * doesn't exist in the given list an error is returned */	
int siq_policy_remove_local(struct siq_policies *pl, 
    struct siq_policy *policy, char *job_dir, struct isi_error **error_out);

/* Add a policy to a policies list */
bool siq_policy_add(struct siq_policies *pl, struct siq_policy *policy,
    struct isi_error **error_out);

/* Frees all policy data structures.  Since it's a destructor it can't fail */
void siq_policy_free(struct siq_policy *policy);

/* Copy policy */
struct siq_policy* siq_policy_copy(struct siq_policy *src_policy);

/* Frees other policy data structures */
void siq_policy_common_free(struct siq_policy_common *common);
void siq_policy_scheduler_free(struct siq_policy_scheduler *scheduler);
void siq_policy_coordinator_free(struct siq_policy_coordinator *coordinator);
void siq_policy_paths_free(struct siq_policy_path_head *paths);
void siq_policy_datapath_free(struct siq_policy_datapath *datapath);
void siq_policy_target_free(struct siq_policy_target *target);

/* Runs a policy immediately.  Returns 0 if success, -1 if error */
int siq_policy_run(struct siq_policies* pl, char *pid, char *snap_override,
    enum siq_job_action job_type, struct isi_error** error_out);

/* Marks the policy runnable again. */
int siq_policy_make_runnable(const struct siq_policies* pl, char *pid,
    struct isi_error **error_out);

/* Marks policy failback prep jobs as manual run. */
int siq_policy_set_manual_failback_prep(const struct siq_policies* pl, 
    char *pid, struct isi_error **error_out);

/* Forces the policy to do a full replication */
int siq_policy_force_full_sync(const struct siq_policies* pl, char *pid,
    struct isi_error **error_out);
/* Below is just a bundle of useful functions */

char* siq_root_path_calc(const struct siq_policy *policy);
char* siq_calc_policy_hash(const struct siq_policy *policy);
char* siq_create_policy_id(bool timestamp);

bool siq_policy_can_add(struct siq_policies *policies, struct
    siq_policy *policy, struct isi_error **error_out);
bool siq_policy_is_valid(const struct siq_policy *policy,
	struct isi_error **error_out);

struct siq_spec* siq_spec_create(void);
struct siq_spec* siq_spec_create_from_policy(struct siq_policy *policy);
struct siq_spec* siq_spec_copy_from_policy(struct siq_policy *policy);
struct siq_spec* siq_spec_copy(struct siq_spec *src);

struct siq_policy* siq_policy_create_from_spec(struct siq_spec *spec);

void siq_spec_free(struct siq_spec* spec);
void siq_report_spec_free(struct siq_spec *spec);
void siq_spec_save(struct siq_policy *policy, char *path, 
    struct isi_error **error_out);
struct siq_gc_spec *siq_gc_spec_load(char *path, struct isi_error **error_out);
void siq_gc_spec_free(struct siq_gc_spec *spec);

void siq_policy_paths_insert(struct siq_policy_path_head *head, const char *path,
    enum siq_src_pathtype type);
void delete_prep_restore_trec(const char *policy_id,
    struct isi_error **error_out);
/**
 * Reporting API
 */
struct siq_job_summary; /* Avoid inclusion of siq_job.h */
struct siq_composite_report;

/**
 * Loads all existing policy reports.
 * @param pid		valid policy id
 * @param report	Returns an array of composite reports
 * @param reports	Returns the number of reports returned
 * @returns
 * 0 on success, and non-zero error code on failure. Error codes TBD.
 */
int
siq_policy_report_load(char *pid, struct siq_composite_report ***report, 
    int *reports);

/**
 * Loads all policy reports within specified date range (inclusive)
 * using policy end time
 * @param pid		valid policy id
 * @param start		start time of range
 * @param end		end time of range
 * @param report	pointer to array of composite reports of length *reports
 * @param reports	On successful return is set to the number of
 *			reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure. Error codes TBD.
 */
int
siq_policy_report_load_date_range(char *pid, time_t start, time_t end,
    struct siq_composite_report ***report, int *reports);

/**
 * Loads the last n policy reports
 * @param pid		valid policy id
 * @param n		number of reports
 * @param report	pointer to array of composite reports of length *reports
 * @param reports	On successful return is set to the number of
 *			reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure. Error codes TBD.
 */
int
siq_policy_report_load_recent(char *pid, int n, 
    struct siq_composite_report ***report, int *reports);

/**
 * Returns the total number of reports for a given policy
 * @param pid		valid policy id
 * @param reports	On successful return is set to the number of
 *			reports found.
 * @returns
 * 0 on success, and non-zero error code on failure. Error codes TBD.
 */
int
siq_policy_report_get_count(char *pid, int *reports);

/**
 * Loads all policy reports for a given job_id
 * using policy end time
 * @input: 
 * pid		valid policy id
 * job_id	valid job_id
 * @output:
 * report	pointer to array of length *reports of siq_job_summary_t 
 *		structures containing requested reports.
 * reports	On successful return is set to the number of
 *		reports loaded.
 * @returns
 * 0 on success, and non-zero error code on failure.
 */
int
siq_policy_report_load_job_id(char *pid, char *job_id,
    struct siq_composite_report ***report, int *reports);

/**
 * Loads the last n reports across all policies
 * @input: 
 * n		Number of reports to load
 * @output:
 * report	Pointer to array of length *reports of siq_job_summary_t 
 *		structures containing requested reports.
 * @returns
 * Number of reports loaded
 */
int
siq_policy_report_load_all_recent(int n, struct siq_composite_report ***report, 
    struct isi_error **error_out);

/**
 * Generate a failback policy name, given the original policy name. The value
 * returned by this fn must be freed. 
 *
 * Given 'foo', the result would be 'foo_failback'. If 'foo_failback' exists,
 * tries 'foo_failback_1', 'foo_failback_2', etc. until an available name
 * found.
 * 
 * @input: 
 * root_name	original policy name
 * policies	policies list
 * @output:
 * @returns
 * generated name if success, else NULL
 */

char *
siq_generate_failback_policy_name(char *root_name, 
    struct siq_policies *policies);

__END_DECLS

#define SIQ_POLICY_REPORT_NAMEFMT "report-%ld.gc"
#define SIQ_POLICY_REPORT_NAMEREX "report-[0-9]*\\.gc"

#endif /* __SIQ_POLICY_H__ */
