#ifndef __SCHED_SNAP_LOCK_H
#define __SCHED_SNAP_LOCK_H

#include "isi_migrate/config/siq_gconfig_gcfg.h"

#define SCHED_DIR "/ifs/.ifsvar/modules/tsm/sched/"
#define CONFIG_PATH SCHED_DIR "snapshot_management/sched-locked-snaps.gc"
#define SCHED_SNAP_GC_LOCK SCHED_DIR "lock/.sched-locked-snaps.gc.lock"
#define MAN_SNAP_LOCK_NAME "SIQ-sched-man-lock"


struct sched_locked_snaps {
	struct sched_locked_snaps_root *root;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;

	int lock_fd;
};

struct sched_locked_snaps *
sched_locked_snaps_load(struct isi_error **error_out);

struct sched_locked_snaps *
sched_locked_snaps_load_readonly(struct isi_error **error_out);

void
sched_locked_snaps_save(struct sched_locked_snaps *snaps,
    struct isi_error **error_out);

void
sched_locked_snaps_add(struct sched_locked_snaps *snaps, ifs_snapid_t snap_id);

void
sched_locked_snaps_free(struct sched_locked_snaps *snaps);

void
sched_snapshot_lock_cleanup(void);

bool
sched_lock_manual_snapshot(ifs_snapid_t man_sid,
    struct isi_error **error_out);

#endif
