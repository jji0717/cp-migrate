#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/sched/sched_snap_locks.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/sched/siq_log.h"

#include <isi_snapshot/snapshot.h>

struct gci_tree sched_locked_snaps_gci_root = {
	.root = &gci_ivar_sched_locked_snaps_root,
	.primary_config = CONFIG_PATH,
	.fallback_config_ro = NULL,
	.change_log_file = NULL
};

static struct sched_locked_snaps *
sched_locked_snaps_load_internal(int lock_type, struct isi_error **error_out)
{
	struct sched_locked_snaps *snaps = NULL;
	struct isi_error *error = NULL;

	ASSERT(lock_type == O_EXLOCK || lock_type == O_SHLOCK);

	CALLOC_AND_ASSERT(snaps, struct sched_locked_snaps);

	snaps->lock_fd = acquire_lock(SCHED_SNAP_GC_LOCK, lock_type, &error);
	if (error)
		goto out;

	snaps->gci_base = gcfg_open(&sched_locked_snaps_gci_root, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	snaps->gci_ctx = gci_ctx_new(snaps->gci_base, true, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(snaps->gci_ctx, "", &snaps->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}

out:
	if (error) {
		sched_locked_snaps_free(snaps);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return snaps;
	}
}

struct sched_locked_snaps *
sched_locked_snaps_load(struct isi_error **error_out)
{
	return sched_locked_snaps_load_internal(O_EXLOCK, error_out);
}

struct sched_locked_snaps *
sched_locked_snaps_load_readonly(struct isi_error **error_out)
{
	struct sched_locked_snaps *snaps = NULL;
	struct isi_error *error = NULL;

	snaps = sched_locked_snaps_load_internal(O_SHLOCK, &error);
	if (error) {
		isi_error_handle(error, error_out);
		return NULL;
	}

	if (snaps->lock_fd >= 0) {
		close(snaps->lock_fd);
		snaps->lock_fd = -1;
	}

	return snaps;
}

void
sched_locked_snaps_save(struct sched_locked_snaps *snaps,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(snaps);
	ASSERT(snaps->lock_fd >= 0);

	gci_ctx_write_path(snaps->gci_ctx, "", snaps->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_commit(snaps->gci_ctx, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_free(snaps->gci_ctx);
	snaps->gci_ctx = gci_ctx_new(snaps->gci_base, true, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(snaps->gci_ctx, "", &snaps->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

void
sched_locked_snaps_add(struct sched_locked_snaps *snaps, ifs_snapid_t snap_id)
{
	struct sched_locked_snap *locked_snap = NULL;

	ASSERT(snaps != NULL);

	CALLOC_AND_ASSERT(locked_snap, struct sched_locked_snap);
	locked_snap->snap_id = snap_id;

	SLIST_INSERT_HEAD(&snaps->root->locked_snap, locked_snap, next);
}

void
sched_locked_snaps_free(struct sched_locked_snaps *snaps)
{
	if (!snaps)
		return;

	if (snaps->gci_ctx)
		gci_ctx_free(snaps->gci_ctx);

	if (snaps->gci_base)
		gcfg_close(snaps->gci_base);

	if (snaps->lock_fd >= 0) {
		close(snaps->lock_fd);
		snaps->lock_fd = -1;
	}

	free(snaps);
}

static void
remove_snap_lock(ifs_snapid_t sid, char *lock_name,
    struct isi_error **error_out)
{
	struct snap_lock_info *locks = NULL;
	SNAP *snap = NULL;
	int i = 0;
	struct isi_error *error = NULL;

	snap = snapshot_open_by_sid(sid, &error);
	if (error) {
		/* non-existence is okay */
		if (isi_error_is_a(error, SNAP_NOSTFLIN_ERROR_CLASS) ||
		    isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS)) {
			isi_error_free(error);
			error = NULL;
		}
		goto out;

	}
	
	locks = snapshot_get_locks(snap);
	for (i = 0; locks[i].lockid != INVALID_LOCKID; i++) {
		if (!strcmp(locks[i].comment, lock_name) &&
		    locks[i].lock_count > 0) {
			break;
		}
	}

	snapshot_rmlock(snap, &locks[i].lockid, &error);

out:
	if (snap)
		snapshot_close(snap);

	if (locks)
		free(locks);

	isi_error_handle(error, error_out);
}

void
sched_snapshot_lock_cleanup(void)
{
	struct sched_locked_snaps *snaps = NULL;
	struct sched_locked_snap *cur = NULL, *tmp = NULL;
	struct snapid_set used_sids = SNAPID_SET_INITIALIZER;
	struct isi_error *error = NULL;
	bool changed = false;

	siq_source_get_used_snapids(&used_sids, &error);
	if (error) {
		log (ERROR, "Failed to load set of used snapshots: %s",
		    isi_error_get_message(error));
		goto out;
	}
	
	snaps = sched_locked_snaps_load(&error);
	if (error) {
		log (ERROR, "Failed to load list of locked snapshots: %s",
		    isi_error_get_message(error));
		goto out;
	}

	SLIST_FOREACH_SAFE(cur, &snaps->root->locked_snap, next, tmp) {

		if (snapid_set_contains(&used_sids, cur->snap_id))
			continue;

		/* Remove lock */
		remove_snap_lock(cur->snap_id, MAN_SNAP_LOCK_NAME, &error);
		if (error) {
			log(ERROR, "Failed to remove snapshot lock on "
			    "snapid %lu: %s", cur->snap_id,
			    isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		/* Remove snap from list */
		SLIST_REMOVE(&snaps->root->locked_snap, cur, sched_locked_snap,
		    next);
		changed = true;
	}

	if (changed) {
		sched_locked_snaps_save(snaps, &error);
		if (error) {
			log(ERROR, "Failed to save locked snapshot list: %s",
			    isi_error_get_message(error));
		}
	}

out:
	snapid_set_clean(&used_sids);
	sched_locked_snaps_free(snaps);
	isi_error_free(error);
}

bool
sched_lock_manual_snapshot(ifs_snapid_t man_sid,
		struct isi_error **error_out)
{
	struct sched_locked_snaps *locked_snaps = NULL;
	struct snap_lock_info *locks = NULL;
	struct isi_error *error = NULL;
	ifs_lockid_t lockid = 0;
	SNAP *snap = NULL;
	time_t t = 0;
	bool snap_exists = true;
	int i = 0;

	snap = snapshot_open_by_sid(man_sid, &error);
	if (error) {
		if (isi_error_is_a(error, SNAP_NOSTFLIN_ERROR_CLASS) ||
		    isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS)) {
			snap_exists = false;
		}
		goto out;
	}


	/* Don't lock again if we already have a lock */
	locks = snapshot_get_locks(snap);
	for (i = 0; locks[i].lockid != INVALID_LOCKID; i++) {
		if (!strcmp(locks[i].comment, MAN_SNAP_LOCK_NAME) &&
		    locks[i].lock_count > 0) {
			goto out;
		}
	}

	/* Record on-disk that we are locking this snapshot. It's important
	 * that we record the lock before actually locking. In the event of a
	 * crash during this function, we'd rather have a recorded lock that
	 * doesn't exist than lock that exists but isn't recorded.*/
	locked_snaps = sched_locked_snaps_load(&error);
	if (error) {
		log (ERROR, "Failed to load list of locked snapshots: %s",
		    isi_error_get_message(error));
		goto out;
	}

	sched_locked_snaps_add(locked_snaps, man_sid);
	sched_locked_snaps_save(locked_snaps, &error);
	if (error) {
		log (ERROR, "Failed to save list of locked snapshots: %s",
		    isi_error_get_message(error));
		goto out;

	}

	/* Lock the snapshot indefinitely. The scheduler will periodically
	 * check if the snapshot is still in use, and will unlock it when it
	 * is no longer needed. */
	snapshot_lock(snap, &t, MAN_SNAP_LOCK_NAME, &lockid, &error);

out:
	if (snap)
		snapshot_close(snap);

	if (locks)
		free(locks);

	sched_locked_snaps_free(locked_snaps);
	isi_error_handle(error, error_out);
	return snap_exists;
}
