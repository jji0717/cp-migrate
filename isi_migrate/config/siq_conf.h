#ifndef __SIQ_CONF_H__
#define __SIQ_CONF_H__

#include <sys/types.h>
#include <stdbool.h>
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_gconfig_gcfg.h"

#define SIQ_APP_NAME "SyncIQ"
#define SIQ_APP_VER_MAJOR 1
#define SIQ_APP_VER_MIDDLE 0
#define SIQ_APP_VER_MINOR 1
#define SIQ_APP_WORKING_DIR "/ifs/.ifsvar/modules/tsm"
#define SIQ_APP_LOG_DIR "/var/log"
#define SIQ_APP_LOG_FILE "isi_migrate.log"

#define SIQ_BWT_WORKING_DIR "/ifs/.ifsvar/modules/tsm/bwt"
#define SIQ_BWT_BW_LOG_FILE "bandwidth.log"
#define SIQ_BWT_TH_LOG_FILE "throttle.log"
#define SIQ_BWT_CPU_LOG_FILE "cpu.log"
#define SIQ_BWT_WORKER_LOG_FILE "worker.log"

#define SIQ_SCHED_WORKING_DIR "/ifs/.ifsvar/modules/tsm/sched"
#define SIQ_SCHED_REPORTS_DIR "/ifs/.ifsvar/modules/tsm/sched/reports"
//TODO: Do we even use deleted-files anymore?
#define SIQ_SCHED_DELETED_FILES_LOG_FILE "deleted-files"
#define SIQ_SCHED_REPORT_FILE_PREFIX "report"

#define SCHED_NAME_EDIT_DIR	"sched_edit"
#define SCHED_NAME_JOBS_DIR	"jobs"
#define SCHED_NAME_RUN_DIR	"run"
#define SCHED_NAME_TRASH_DIR	"trash"
#define SCHED_NAME_LOCK_DIR	"lock"
#define SCHED_NAME_SNAP_DIR	"snapshot_management"

#define SIQ_REPORTS_REPORT_DB_PATH \
    "/ifs/.ifsvar/modules/tsm/sched/reports/reports.db"

struct siq_gc_conf {
	struct siq_conf *root;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;
};
extern struct gci_tree siq_gc_conf_gci_root;

__BEGIN_DECLS

struct siq_conf* siq_conf_create_or_populate(struct siq_conf *conf);
struct siq_gc_conf* siq_gc_conf_load(struct isi_error **error_out);
int siq_gc_conf_save(struct siq_gc_conf *conf, struct isi_error **error_out);

void siq_conf_free(struct siq_conf *conf);
void siq_gc_conf_close(struct siq_gc_conf *conf);
void siq_gc_conf_free(struct siq_gc_conf *conf);

int get_deny_fallback_min_ver(void);
void override_connect_timeout(int timeout);
int get_connect_timeout(void);

void siq_print_version(const char *name);
char* siq_state_to_text(enum siq_state state);

__END_DECLS

#endif /* __SIQ_CONF_H__ */
