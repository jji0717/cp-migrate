#ifndef SIQ_ALERT_H
#define SIQ_ALERT_H


#include <isi_celog/isi_celog.h>
#include <isi_util/isi_error.h>

#include "isi_migrate/config/siq_gconfig_gcfg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SIQ_CONFIG_DIR "/ifs/.ifsvar/modules/tsm/config"
#define SIQ_ACTIVE_ALERTS_DIR SIQ_CONFIG_DIR "/alerts"
#define SIQ_ACTIVE_ALERTS_LOCK_DIR SIQ_CONFIG_DIR "/alert_locks"
#define GLOBAL_SIQ_ALERT "GLOBAL_SIQ_ALERT"

struct siq_gc_active_alert {
	struct siq_active_alert_root *root;
	struct gci_tree gci_tree;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;

	char *path;
	int lock_fd;
};

enum siq_alert_type {
	SIQ_ALERT_NONE = 0,
	SIQ_ALERT_POLICY_FAIL,
	SIQ_ALERT_POLICY_START,
	SIQ_ALERT_POLICY_CONFIG,
	SIQ_ALERT_TARGET_PATH_OVERLAP,
	SIQ_ALERT_SELF_PATH_OVERLAP,
	SIQ_ALERT_VERSION,
	SIQ_ALERT_SIQ_CONFIG,
	SIQ_ALERT_LICENSE,
	SIQ_ALERT_TARGET_CONNECTIVITY,
	SIQ_ALERT_LOCAL_CONNECTIVITY,
	SIQ_ALERT_DAEMON_CONNECTIVITY,
	SIQ_ALERT_SOURCE_SNAPSHOT,
	SIQ_ALERT_TARGET_SNAPSHOT,
	SIQ_ALERT_BBD_CHECKSUM,
	SIQ_ALERT_FS,
	SIQ_ALERT_SOURCE_FS,
	SIQ_ALERT_TARGET_FS,
	SIQ_ALERT_POLICY_UPGRADE,
	SIQ_ALERT_TARGET_ASSOC,
	SIQ_ALERT_RPO_EXCEEDED,
	SIQ_ALERT_COMP_CONFLICTS,
	SIQ_ALERT_MAX
};

bool
siq_create_alert(enum siq_alert_type type, const char *policy_name,
    const char *policy_id, const char *target, char **msg_out,
    const char *msg);

bool
siq_create_rpo_alert(const char *policy_name, const char *policy_id,
    const char *target, time_t last_success, char **msg_out,
    struct siq_gc_active_alert *alerts, const char *msg);

bool
siq_create_config_alert(char **msg_out, const char *fmt, ...);

bool
siq_create_alert_from_error(struct isi_error *error, const char *policy_name,
    const char *policy_id, const char *target, char **msg_out);

bool
siq_cancel_alert_internal(enum siq_alert_type type, const char *policy_name,
    const char *policy_id, struct siq_gc_active_alert *alerts);

bool
siq_cancel_alert(enum siq_alert_type type, const char *policy_name,
    const char *policy_id);

void
siq_cancel_alerts_for_policy(const char *policy_name, const char *policy_id,
    enum siq_alert_type exclude[], size_t exclude_size);

void
siq_cancel_all_alerts_for_policy(const char *policy_name,
    const char *policy_id);

void
siq_gc_active_alert_free(struct siq_gc_active_alert *alerts);

struct siq_gc_active_alert *
siq_gc_active_alert_load(const char *policy_id, struct isi_error **error_out);

void
siq_gc_active_alert_save(struct siq_gc_active_alert *alerts,
    struct isi_error **error_out);

bool
siq_gc_active_alert_exists(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid);

void
siq_gc_active_alert_add(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid, time_t last_success);

void
siq_gc_active_alert_remove(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid);

void
siq_gc_active_alert_cleanup(const char *policy_id);

#ifdef __cplusplus
}
#endif

#endif /* SIQ_ALERT_H */
