/**
 * @file siq_job.h
 * TSM Job Statistics/Progress public API
 * (c) 2006 Isilon Systems
 */
#ifndef __SIQ_JOB_H__
#define __SIQ_JOB_H__

#include <isi_domain/dom.h>

#include "isi_migrate/config/siq_gconfig_gcfg.h"
#include "isi_migrate/config/siq_policy.h"

#define REPORT_FILENAME "report.gc"

#ifdef __cplusplus
extern "C" {
#endif

enum domain_type {
	DT_NONE = 0,
	DT_SYNCIQ = DOM_SYNCIQ,
	DT_SNAP_REVERT = DOM_SNAPREVERT,
	DT_WORM = DOM_WORM,
};

struct siq_gc_job_summary {
	struct siq_job_summary_root *root;
	char *path;
	struct gci_tree gci_tree;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;
};

struct siq_composite_report {
	struct siq_job_summary *composite;
	int num_reports;
	uint32_t duration;
	struct siq_job_summary **elements;
};

struct siq_stat* siq_stat_populate(struct siq_stat *s);
struct siq_stat* siq_stat_create(void);
struct siq_job_summary*
    siq_job_summary_populate(struct siq_job_summary *job_summ);
struct siq_job_summary* siq_job_summary_create(void);
void siq_stat_copy(struct siq_stat *dst, struct siq_stat *src);
void siq_stat_free(struct siq_stat *s);
void siq_job_total_free(struct siq_job_total *t);
void siq_string_list_free(struct siq_string_list_head *list);
void siq_policy_path_free(struct siq_policy_path_head *list);
void siq_phase_history_free(struct siq_phase_history_head *phases);
void siq_workers_free(struct siq_worker_head *workers);
void siq_job_summary_free(struct siq_job_summary *js);
enum siq_action_type get_spec_action(const char *run_dir,
    const char *job_dirname);

struct siq_gc_job_summary*
    siq_gc_job_summary_load(char *path, struct isi_error **error_out);
struct siq_job_summary*
    siq_job_summary_load_readonly(char *path, struct isi_error **error_out);
void siq_gc_job_summary_save(struct siq_gc_job_summary *job_summ, 
    struct isi_error **error_out);
struct siq_gc_job_summary*
    siq_job_summary_save_new(struct siq_job_summary *root, char *path, 
    struct isi_error **error_out);
void siq_gc_job_summary_free(struct siq_gc_job_summary *job_summ);
void siq_job_summary_free_nospec(struct siq_job_summary *js);
void siq_failover_snap_rename_new_to_latest(const char *policy_id, 
    const char *policy_name, struct isi_error **error_out);
void siq_start_failover_revert_job(const char *policy_id, int log_level, 
    int workers_per_node, bool grab_local, struct isi_error **error_out);
void siq_start_failover_job(const char *policy_id, int log_level, 
    int workers_per_node, bool grab_local, struct isi_error **error_out);

void siq_domain_mark_policy_id(char *root, char *pol_id_out,
    struct isi_error **error_out);
void siq_start_domain_mark_job(char *root, enum domain_type type, int log_level,
    int workers_per_node, bool grab_local, struct isi_error **error_out);
void siq_cleanup_domain_mark_job(char *root, struct isi_error **error_out);
void siq_remove_domain(char *root, enum domain_type type,
    struct isi_error **error_out);

void siq_snap_revert_policy_id(ifs_snapid_t snapid, char *pol_id_out,
    struct isi_error **error_out);
void siq_start_snap_revert_job(ifs_snapid_t snapid, int log_level,
    int workers_per_node, bool grab_local, struct isi_error **error_out);
void siq_cleanup_snap_revert_job(ifs_snapid_t snapid,
    struct isi_error **error_out);

/**
 * Frees the members of an array of siq_job_summary_t
 * Does not free the array itself.
 */
void
siq_job_summary_free_array(struct siq_job_summary **js, int count);

/**
 * Appends an error message
 */
void
siq_job_error_msg_append(struct siq_job_summary *js, char* msg);

/**
 * Appends warning message
 */
void
siq_job_warning_msg_append(struct siq_job_summary *js, char* msg);

void
siq_job_retransmitted_file_append(struct siq_job_summary *js, char* msg);

void
siq_snapshots_target_append(struct siq_job_summary *js, char* msg);

char *
domain_type_to_text(enum domain_type);

#ifdef __cplusplus
}
#endif

#endif /* __SIQ_JOB_H__ */
