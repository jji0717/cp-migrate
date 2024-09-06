#ifndef __SIQ_UTIL_H__
#define __SIQ_UTIL_H__

#include <syslog.h>

#include "isi_flexnet/isi_flexnet.h"
#include "isi_ilog/ilog.h"
#include "isi_migrate/config/siq_gconfig_gcfg.h"

#define SIZEM1(s) (sizeof(s) - 1)
#define STANDARD_NORMALIZE 0
#define ADD_SLASH_TO_PATH 1

#define CALLOC_AND_ASSERT(x, y) x = calloc(1, sizeof(y)); ASSERT(x);
#define FREE_AND_NULL(x) free(x); x = NULL;
#define CREATE_IF_NULL(field, type) if (!field) CALLOC_AND_ASSERT(field, type)

static const char *DEFAULT_SNAP_PATTERN =
    "SIQ-%{SrcCluster}-%{PolicyName}-%Y-%m-%d_%H-%M-%S";
static const char *DEFAULT_SNAP_ALIAS =
    "SIQ-%{SrcCluster}-%{PolicyName}-latest";
static const char *DEFAULT_SNAPSHOT_SYNC_PATTERN = "*";

/* Log levels */
enum severity {
	FATAL = 0,
	ERROR,
	NOTICE,
	INFO,
	COPY,
	DEBUG,
	TRACE,
	SEVERITY_TOTAL
};
typedef enum severity siq_log_level_t;

//SIQ Log Levels -> ilog levels
static const int siq_to_islog_severity_map[] = {
	IL_FATAL,
	IL_ERR_PLUS,
	IL_NOTICE_PLUS,
	IL_INFO_PLUS,
	IL_INFO_PLUS | IL_COPY,
	IL_DEBUG_PLUS,
	IL_TRACE_PLUS
};

static const int siq_to_islog_type_map[] = {
	IL_FATAL,
	IL_ERR,
	IL_NOTICE,
	IL_INFO,
	IL_COPY,
	IL_DEBUG,
	IL_TRACE
};

extern const char *siq_log_level_strs_gp[SEVERITY_TOTAL + 1];

__BEGIN_DECLS

const char *siq_log_level_to_str(int log_level);
int siq_str_to_log_level(const char *log_level_str);

int extract_subnet_and_pool_name(const char *subnet_and_pool_name,
    char **p_subnet_name, char **p_pool_name);

struct flx_pool *get_pool(struct flx_config *config,
    const char *subnet_and_pool_name, struct isi_error **);

bool text2bool(const char *text);
bool text2bool_err(const char *text, bool *err);
char *bool2text(bool value);
int acquire_lock(const char *lock_file, int lock_type,
    struct isi_error **error_out);
int get_local_node_id(void);
char *job_state_to_text(enum siq_job_state state, bool friendly);
enum siq_job_state text_to_job_state(const char *txt);

char *get_cluster_guid(void);
bool delete_snapshot_helper(ifs_snapid_t);

enum path_overlap_result {
	NO_OVERLAP = 0,
	PATH_A_PARENT,
	PATH_B_PARENT,
	PATHS_EQUAL,
	PATH_OVERLAP_RESULT_TOTAL
};

void normalize_path(char *path, int add_slash);
enum path_overlap_result path_overlap(const char *a, const char *b);
int set_siq_log_level(int siq_level);
int set_siq_log_level_str(char *val);
void siq_nanosleep(int seconds, long nanoseconds);
char *job_phase_enum_str(enum stf_job_phase phase);
char *job_phase_enum_friendly_str(enum stf_job_phase phase);

bool siq_job_exists(const char *pol_id);

struct isi_error *safe_snprintf(char *s, size_t n, const char *format, ...);

void siq_wake_sched(void);

void free_and_clean_pvec(struct ptr_vec *vec);

int get_compliance_dom_ver(const char *path, struct isi_error **error_out);

void
get_tmp_working_dir_name(char *buf, bool restore);

unsigned
check_compliance_domain_root(char *target_dir, struct isi_error **error_out);

void
check_failback_allowed(struct siq_policy *policy, struct isi_error **error_out);

bool passed_further_check(const char *target_base);

__END_DECLS

#endif /* __SIQ_UTIL_H__ */
