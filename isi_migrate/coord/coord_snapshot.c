#include <ifs/ifs_lin_open.h>
#include <isi_ufp/isi_ufp.h>

#include "coord.h"
#include "isi_migrate/config/siq_target.h"

#define SNAPSHOT_LEASE_RENEW 3
#define SNAPSHOT_LEASE_LENGTH 15
#define MIN_POLICIES_POL_INTERVAL 30

#define MAX_SNAP_LOCK_ERRORS 3

/* Bug 155195- default is never expire */
#define PRISNAPSHOTEXPIRATION 0

#define SNAPSHOTSDIR "/ifs/.snapshot"

static bool
load_and_lock_snap(struct siq_snap *snap, ifs_snapid_t id, bool skip_lock,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct snap_lock_info *locks = NULL;
	bool snap_exists = false, lock_exists = false;
	time_t t;
	int i = 0;
	char *lock_name = NULL;

	snap->id = id;
	snap->snap = snapshot_open_by_sid(id, &error);
	if (error) {
		if (isi_error_is_a(error, SNAP_NOSTFLIN_ERROR_CLASS) ||
		    isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS)) {
			isi_error_free(error);
			error = NULL;
		}
		goto out;
	}

	snap_exists = true;

	snap->root_fd = ifs_lin_open(ROOT_LIN, id, O_RDONLY);
	if (snap->root_fd < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open snapshot root of %llu: %s", id,
		    strerror(errno));
		goto out;
	}

	if (skip_lock)
		goto out;

	/* Skip snapshot locking failpoint */
	UFAIL_POINT_CODE(skip_snap_lock,
		goto out;
	);

	time(&t);
	t += MIN_POLICIES_POL_INTERVAL * SNAPSHOT_LEASE_LENGTH;

	/*
	 * Try to find an existing lock that this job created
	 * on this same snapshot - did this job crash last time?
	 * Bug 121773 - Also look for locks named "SyncIQ Treewalk" which
	 * could have been created prior to a jaws upgrade.
	 */
	asprintf(&lock_name, "SIQ-%s", g_ctx.rc.job_id);
	locks = snapshot_get_locks(snap->snap);
	for (i = 0; locks[i].lockid != INVALID_LOCKID; i++) {
		if ((!strcmp(locks[i].comment, lock_name) ||
		     !strcmp(locks[i].comment, "SyncIQ treewalk")) &&
		    locks[i].lock_count > 0) {
			lock_exists = true;
			break;
		}
	}

	/* Take the existing lock or create a new one */
	if (lock_exists) {
		snap->lock = locks[i].lockid;
		snapshot_set_lock_expir(snap->snap, &snap->lock, &t, &error);
	} else {
		snapshot_lock(snap->snap, &t, lock_name, &snap->lock,
		    &error);
	}

	if (error)
		goto out;

out:
	if (error) {
		if (snap->snap) {
			snapshot_close(snap->snap);
			snap->snap = NULL;
		}

		if (snap->root_fd >= 0) {
			close(snap->root_fd);
			snap->root_fd = -1;
		}

		snap->id = INVALID_SNAPID;
	}

	if (lock_name)
		free(lock_name);

	if (locks) {
		free(locks);
	}

	isi_error_handle(error, error_out);
	return snap_exists;
}

void
load_and_lock_snap_or_error(struct siq_snap *snap, ifs_snapid_t id,
    bool skip_lock, const char *snap_name)
{
	struct isi_error *error = NULL;
	bool existing_snap;

	existing_snap = load_and_lock_snap(snap, id, skip_lock, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Failed to open and lock %s snapid %d: %s",
		    snap_name, id, isi_error_get_message(error));
	else if (!existing_snap)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Can't find %s snapid %d", snap_name, id);

	if (!skip_lock) {
		if (!strcmp(snap_name, "new"))
			register_new_lock_renewal();
		else if (!strcmp(snap_name, "latest"))
			register_latest_lock_renewal();
	}
}

/**
 * Syncs with "preset" snapshots are specified with the source path being a
 * path into a snapshot in the .snapshot directory. What we need are non-
 * snapshot paths (including selectors), and an fd for the root lin of the
 * appropriate snapshot.
 */
static void
handle_preset_snap(void)
{
	struct stat st;
	int i;
	struct path path;
	char *tmp;
	char plus_minus;

	log(TRACE, "handle_preset_snap");

	if (stat(g_ctx.source_base.path, &st))
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Failed to stat %s: %s",
		    g_ctx.source_base.path, strerror(errno));

	snap_path_to_normal_path(&g_ctx.source_base);
	load_and_lock_snap_or_error(&g_ctx.curr_snap, st.st_snapid, false,
	    "preset");

	log(DEBUG, "preset snap with snapid: %llu and adjusted path %s",
	    g_ctx.curr_snap.id, g_ctx.source_base.path);

	/* convert the selector paths */
	for (i = 0; g_ctx.rc.paths.paths[i]; i++) {
		/* store the include/exclude (+/-) symbol */
		plus_minus = g_ctx.rc.paths.paths[i][0];
		path_init(&path, g_ctx.rc.paths.paths[i] + 1, NULL);
		free(g_ctx.rc.paths.paths[i]);

		snap_path_to_normal_path(&path);

		tmp = malloc(strlen(path.path) + 1);
		tmp[0] = plus_minus;
		strcpy(tmp + 1, path.path);
		g_ctx.rc.paths.paths[i] = tmp;
		path_clean(&path);
	}
	g_ctx.old_time = 0;
	g_ctx.new_time = time(0);
}

static ifs_snapid_t
get_snapid_from_name(char *name)
{
	SNAP *snap = NULL;
	struct isi_str snap_name;
	ifs_snapid_t sid = INVALID_SNAPID;
	
	isi_str_init(&snap_name, name, strlen(name) + 1, ENC_DEFAULT, 
	    ISI_STR_NO_MALLOC);
	snap = snapshot_open_by_name(&snap_name, NULL);
	
	snapshot_get_snapid(snap, &sid);
	
	snapshot_close(snap);
	return sid;
}

static void
get_latest_snap(struct siq_source_record *srec, bool restore,
    ifs_snapid_t *snapidp)
{
	if (restore) {
		siq_source_get_restore_latest(srec, snapidp);
	} else {
		siq_source_get_latest(srec, snapidp);
	}
}

static void
get_new_snap(struct siq_source_record *srec, bool restore,
    ifs_snapid_t *snapidp)
{
	if (restore) {
		siq_source_get_restore_new(srec, snapidp);
	} else {
		siq_source_get_new(srec, snapidp);
	}
}

static void
set_new_snap(struct siq_source_record *srec, bool restore,
    ifs_snapid_t snapid)
{
	if (restore) {
		siq_source_add_restore_new_snap(srec, snapid);	
	} else {
		siq_source_set_new_snap(srec, snapid);
	}
}

/**
 * Check to see whether we are a snapshot-based sync and either
 * create snapshots if necessary or use pre-existing snapshots.
 * Return true if an existing snapshot is being used, false otherwise.
 */
void
check_and_create_snap(void)
{
	struct isi_error *error = NULL;
	ifs_snapid_t latest_snapid = INVALID_SNAPID;
	ifs_snapid_t new_snapid = INVALID_SNAPID;
	ifs_snapid_t manual_snapid = INVALID_SNAPID;
	struct fmt FMT_INIT_CLEAN(fmt);
	SNAP *snap = NULL;
	struct isi_str *tmp_snap_name = NULL;
	bool skip_lock = false;

	log(TRACE, "check_and_create_snap");

	/* in case of sync of a source inside /ifs/.snapshot (SIQ_SNAP_PRESET),
	 * change the path and selectors to a non-snap path and store 
	 * snapshot's snapid */
	if (g_ctx.rc.source_snapmode == SIQ_SNAP_PRESET) {
		log(DEBUG, "snap mode SIQ_SNAP_PRESET");
		handle_preset_snap();
		goto out;
	}

	if (g_ctx.rc.source_snapmode != SIQ_SNAP_MAKE)
		goto out;
	else
		log(DEBUG, "snap mode SIQ_SNAP_MAKE");

	/* Cleanup previous snapshots */
	finish_deleting_snapshot(false);

	g_ctx.old_time = 0;
	g_ctx.new_time = 0;

	get_latest_snap(g_ctx.record, g_ctx.restore, &latest_snapid);
	if (latest_snapid != INVALID_SNAPID) {
		/* HACK: Don't take snapshot on assess for non-initial sync. */	
		if (g_ctx.rc.flags & FLAG_ASSESS) {
			goto out;
		}

		/* Don't lock the latest snapshot if it was specified as a
		 * manual snapshot on the previous run, the scheduler will
		 * manage that lock */
		if (siq_source_retain_latest(g_ctx.record))
			skip_lock = true;

		load_and_lock_snap_or_error(&g_ctx.prev_snap, latest_snapid,
		    skip_lock, "latest");

		g_ctx.initial_sync = false;
		g_ctx.old_time = get_snap_ctime(latest_snapid);
	} else {
		log(DEBUG, "Full sync requested");
		/* No old snapshot on source, counted as first policy run */
		ASSERT(!g_ctx.snap_revert_op);
		g_ctx.prev_snap.root_fd = -1;
		g_ctx.prev_snap.id = INVALID_SNAPID;
		g_ctx.initial_sync = true;
		g_ctx.rc.flags |= FLAG_CLEAR;
		if (g_ctx.rc.flags & FLAG_DIFF_SYNC) {
			log(COPY, "First run is a diff sync");
			g_ctx.rc.doing_diff_sync = true;
		} else
			log(DEBUG, "First run is not a diff sync");
	}

	/* restarting failed, canceled, etc. job */
	get_new_snap(g_ctx.record, g_ctx.restore, &new_snapid);
	if (new_snapid != INVALID_SNAPID) {
		load_and_lock_snap_or_error(&g_ctx.curr_snap, new_snapid,
		    false, "new");
		g_ctx.new_time = get_snap_ctime(new_snapid);
		goto out;
	}

	/* using manually specified snapshot */
	siq_source_get_manual(g_ctx.record, &manual_snapid);
	if (manual_snapid != INVALID_SNAPID) {
		load_and_lock_snap_or_error(&g_ctx.curr_snap, manual_snapid,
		    true, "manual");
		g_ctx.new_time = get_snap_ctime(manual_snapid);
		goto out;
	}

	if (g_ctx.snap_revert_op) {
		/* name the new snapshot 'xyz.pre_revert.timestamp' where xyz
		 * is the name of the snapshot to be reverted. this will make 
		 * it easy to locate the pre-revert snapshot for head data
		 * recovery or un-restoring a failed revert, etc. */
		snap = snapshot_open_by_sid(latest_snapid, &error);
		if (error)
			goto out;

		tmp_snap_name = snapshot_get_name(snap);
		snapshot_close(snap);
		snap = NULL;
		fmt_print(&fmt, "%s.pre_revert.%llu",
		    ISI_STR_GET_STR(tmp_snap_name), time(NULL));
		isi_str_free(tmp_snap_name);
	} else if (g_ctx.restore) {
		fmt_print(&fmt, "SIQ-%s-restore-new", g_ctx.rc.job_id);
	} else {
		fmt_print(&fmt, "SIQ-%s-new", g_ctx.rc.job_id);
	}

	/* Take snapshot */
	new_snapid = takesnap(fmt_string(&fmt), g_ctx.rc.path,
	    PRISNAPSHOTEXPIRATION, NULL, &error);
	if (error) {
		log(ERROR, "Can't create new snapshot (%s): %{}",
		    fmt_string(&fmt), isi_error_fmt(error));
		if (isi_error_is_a(error, SNAP_EXIST_ERROR_CLASS)) {
			/* The new snapshot already exists, 
			 * sanity check and use it */
			char *snap_name = (char *)fmt_string(&fmt);
			new_snapid = get_snapid_from_name(snap_name);
			if (new_snapid <= g_ctx.prev_snap.id) {
				fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
				    "Snapshots initialization error - "
				    "existing new snapshot is older "
				    "than latest snapshot");
			}
			log(NOTICE, "Using existing new snapshot: %lld",
			    new_snapid);
			isi_error_free(error);
			error = NULL;
		} else if (isi_system_error_is_a(error, EPERM)) {
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
				"Snapshots initialization error - "
				"Snapshot creation may be disabled");
		} else {
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
			    "Snapshots initialization error");
		}
	}

	set_new_snap(g_ctx.record, g_ctx.restore, new_snapid);

	siq_source_record_save(g_ctx.record, &error);
	if (error) {
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error recording new snapshot %s",
		    isi_error_get_message(error));
	}

	load_and_lock_snap_or_error(&g_ctx.curr_snap, new_snapid, false,
	    "new");
	g_ctx.new_time = get_snap_ctime(new_snapid);

out:
	/* ctime() uses a static buffer, so two log() statements needed */
	log(DEBUG, "snap times:  old='%s'", ctime(&g_ctx.old_time));
	log(DEBUG, "snap times:  new='%s'", ctime(&g_ctx.new_time));

	if (g_ctx.prev_snap.id == INVALID_SNAPID) {
		log(NOTICE, "Initial sync. new snap id: %llu",
		    g_ctx.curr_snap.id);
	} else {
		log(NOTICE, "Incremental sync. new snap id: %llu "
		    "old snap id: %llu", g_ctx.curr_snap.id,
		    g_ctx.prev_snap.id);
	}
}

static void
renew_lock(struct siq_snap *snap, int *error_count, const char *snap_name)
{
	struct isi_error *ie = NULL;
	time_t t;

	t = time(0) + MIN_POLICIES_POL_INTERVAL * SNAPSHOT_LEASE_LENGTH;

	if (snap->snap == NULL)
		return;

	snapshot_set_lock_expir(snap->snap, &snap->lock, &t, &ie);
	if (ie) {
		if (*error_count < MAX_SNAP_LOCK_ERRORS) {
			(*error_count)++;
			if (*error_count == MAX_SNAP_LOCK_ERRORS) {
				log(ERROR, "Multiple attempts to renew"
				    " %s source snapshot lock failed: "
				    "%{}", snap_name, isi_error_fmt(ie));
			} else {
				log(ERROR, "Failed to renew %s source "
				    "snapshot lock: %{}", snap_name,
				    isi_error_fmt(ie));
			}
		} else {
			log(COPY, "Failed to renew %s source snapshot "
			    "lock: %{}", snap_name, isi_error_fmt(ie));
		}

		isi_error_free(ie);
	} else {
		log(TRACE, "Lock was set for old snapshot");
		*error_count = 0;
	}
}


static int
renew_new_callback(void *ctx)
{
	static int new_errors = 0;

	log(DEBUG, "Renewing \"new\" snapshot lock");
	if (g_ctx.done) {
		log(DEBUG, "Job %s completing", SIQ_JNAME(&g_ctx.rc));
		return 0;
	}

	renew_lock(&g_ctx.curr_snap, &new_errors, "new");

	register_new_lock_renewal();
	return 0;
}

void
register_new_lock_renewal(void)
{
	struct timeval tv = { MIN_POLICIES_POL_INTERVAL *
	    SNAPSHOT_LEASE_RENEW, 0 };
	g_ctx.snaptimer = migr_register_timeout(&tv, renew_new_callback, 0);
}

static int
renew_latest_callback(void *ctx)
{
	static int latest_errors = 0;

	log(DEBUG, "Renewing  \"latest\" snapshot lock");
	if (g_ctx.done) {
		log(DEBUG, "Job %s completing", SIQ_JNAME(&g_ctx.rc));
		return 0;
	}

	renew_lock(&g_ctx.prev_snap, &latest_errors, "latest");

	register_latest_lock_renewal();
	return 0;
}

void
register_latest_lock_renewal(void)
{
	struct timeval tv = { MIN_POLICIES_POL_INTERVAL *
	    SNAPSHOT_LEASE_RENEW, 0 };
	g_ctx.snaptimer = migr_register_timeout(&tv, renew_latest_callback, 0);
}

static void
remove_snap_lock_and_close(struct siq_snap *snap)
{
	if (snap->snap == NULL)
		return;

	/* snap->lock may be NULL if the scheduler is maintaining the lock.
	 * This is done for manually specified snapshots */
	if (snap->lock)
		snapshot_rmlock(snap->snap, &snap->lock, NULL);

	snapshot_close(snap->snap);
	snap->snap = NULL;

	close(snap->root_fd);
	snap->root_fd = -1;

	snap->id = INVALID_SNAPID;
}

void
remove_snapshot_locks(void)
{
	remove_snap_lock_and_close(&g_ctx.curr_snap);
	remove_snap_lock_and_close(&g_ctx.prev_snap);
}

void
finish_deleting_snapshot(bool delete_can_fail)
{
	struct isi_error *error = NULL;
	ifs_snapid_t *delete_id;
	ifs_snapid_t latest_id;
	struct isi_str *name;
	SNAP *snap;
	int i = 0;

	siq_source_get_delete(g_ctx.record, &delete_id);
	for (i = 0; i < SIQ_SOURCE_RECORD_DELETE_SNAP_SIZE; i++) {
		if (!delete_snapshot_helper(delete_id[i]) && !delete_can_fail)
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
			    "Unable to delete snapshot from "
			    "previous run: %lld", delete_id[i]);
	}

	if (siq_source_retain_latest(g_ctx.record))
		/* don't rename the snapshot if it wasn't created by SyncIQ */
		goto out;

	/* XXX
	 * Right now we dont delete the snapshots. This needs to be 
	 * handled after discussion on whether to delete or to keep it.
	 * XXX
	 */
	if (g_ctx.restore)
		goto out;

	get_latest_snap(g_ctx.record, g_ctx.restore, &latest_id);
	snap = snapshot_open_by_sid(latest_id, NULL);
	if (snap != NULL) {
		struct fmt FMT_INIT_CLEAN(fmt);
		struct isi_str expect_str;

		if (g_ctx.restore) {
			fmt_print(&fmt, "SIQ-%s-restore-latest",
			    g_ctx.rc.job_id);
		} else { 
			fmt_print(&fmt, "SIQ-%s-latest", g_ctx.rc.job_id);
		}
		isi_str_init(&expect_str, __DECONST(char *, fmt_string(&fmt)),
		    fmt_length(&fmt) + 1, ENC_UTF8, ISI_STR_NO_MALLOC);

		name = snapshot_get_name(snap);
		snapshot_close(snap);
		snap = NULL;

		if (name == NULL)
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
			    "Latest snapshot has been deleted, policy must be "
			    "reset to continue");

		if (isi_str_compare(name, &expect_str) != 0) {
			snapshot_rename(name, &expect_str, &error);
			if (error) {
				log(ERROR, "Failed to rename latest "
				    "snapshot: %s: %{}",
				    ISI_STR_GET_STR(name),
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			} else {
				log(NOTICE, "Renamed snapshot '%s' to '%s'",
				    ISI_STR_GET_STR(name),
				    ISI_STR_GET_STR(&expect_str));
			}
		}
	}
out:
	siq_source_clear_delete_snaps(g_ctx.record);
	siq_source_record_save(g_ctx.record, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT, 
		    "Error writing deleted snapshot state: %s",
		    isi_error_get_message(error));
}

void
delete_old_snapshot()
{
	SNAP *old_snap = NULL;
	struct isi_str *old_name = NULL;
	struct isi_error *error = NULL;
	ifs_snapid_t latest_id;
	char new_name[MAXPATHLEN];
	bool changelist_op = false, del_latest = false;
	time_t create_time, expir_time;
	struct tm *create_tm;
	struct isi_str *i_new_name = NULL;

	log(DEBUG, "%s: expire=%d, pattern=%s", __func__,
	    JOB_SPEC->coordinator->rename_expiration, 
	    g_ctx.rc.rename_snap);

	if (g_ctx.changelist_op && !g_ctx.job_had_error)
		changelist_op = true;

	if (*g_ctx.rc.rename_snap == 0 && !changelist_op) {
		/* Rename is not configured and this is not a changelist op */
		siq_source_finish_job(g_ctx.record, &error);
		goto out;
	}

	siq_source_get_latest(g_ctx.record, &latest_id);

	old_snap = snapshot_open_by_sid(latest_id, NULL);
	if (old_snap == NULL) {
		/* Snapshot has gone away for some reason, just skip rename */
		siq_source_finish_job(g_ctx.record, &error);
		goto out;
	}

	old_name = snapshot_get_name(old_snap);
	if (old_name == NULL) {
		/* Snapshot is being deleted, skip rename */
		siq_source_finish_job(g_ctx.record, &error);
		goto out;
	}

	del_latest = siq_source_should_del_latest_snap(g_ctx.record, &error);
	if (error)
		goto out;

	if (!del_latest) {
		/* This snapshot is somehow part of a merge, or was not created
		 * by SyncIQ, so leave it alone */
		log(NOTICE, "Snapshot was not renamed either because SyncIQ "
		    "did not create it, or it is part of a merge");
		siq_source_finish_job(g_ctx.record, &error);
		goto out;
	}

	snapshot_get_ctime(old_snap, &create_time);

	if (JOB_SPEC->coordinator->rename_expiration && !changelist_op) {
		expir_time = create_time +
		    JOB_SPEC->coordinator->rename_expiration;
		snapshot_set_expiration(old_snap, &expir_time);
		snapshot_commit(old_snap, &error);
		if (error)
			goto out;
	}

	if (*g_ctx.rc.rename_snap != 0) {
		create_tm = localtime(&create_time);
		strftime(new_name, sizeof(new_name), g_ctx.rc.rename_snap,
		    create_tm);
	} else {
		get_changelist_snap_name(g_ctx.rc.job_name, new_name);
	}

	i_new_name = get_unique_snap_name(new_name, &error);
	if (error)
		goto out;

	snapshot_rename(old_name, i_new_name, &error);
	if (error)
		goto out;

	siq_source_detach_latest_snap(g_ctx.record);
	siq_source_finish_job(g_ctx.record, &error);

out:
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error renaming latest snapshot: %s",
		    isi_error_get_message(error));

	siq_source_record_save(g_ctx.record, &error);
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error recording latest snapshot: %s",
		    isi_error_get_message(error));

	finish_deleting_snapshot(true);

	if (i_new_name)
		isi_str_free(i_new_name);
	if (old_snap)
		snapshot_close(old_snap);
	if (old_name)
		isi_str_free(old_name);
}

void
delete_new_snapshot(void)
{
	struct isi_error *error = NULL;

	siq_source_finish_assess(g_ctx.record, &error);
	if (error)
		goto out;

	/* Save the new record before performing any more work */
	siq_source_record_save(g_ctx.record, &error);
	if (error)
		goto out;
	
	finish_deleting_snapshot(true);

out:
	if (error)
		fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
		    "Error recording access snapshot: %s",
		    isi_error_get_message(error));

}

/*
 * Move the alias_sid to an indicated snapshot_sid.
 * Returns error as EINVAL if:
 *      alias_sid is not an alias
 *      target_sid is an alias, INVALID_SID, or DNE
 */
void
move_snapshot_alias_helper(ifs_snapid_t target_sid, ifs_snapid_t alias_sid,
    bool *found_target, bool *found_alias, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_str *s_alias = NULL;
	struct isi_str *s_aliases[2] = { NULL, NULL };
	SNAP *target_snap = NULL;
	SNAP *alias_snap = NULL;
	ifs_snapid_t old_alias_target = INVALID_SNAPID;

	if (found_target != NULL)
		*found_target = false;
	if (found_alias != NULL)
		*found_alias = false;

	/* preliminary validation */
	if (target_sid == INVALID_SNAPID ||
	    alias_sid == INVALID_SNAPID ||
	    alias_sid == HEAD_SNAPID ||
	    alias_sid == target_sid)
		goto err;

	/* retrieve and validate the alias name */
	alias_snap = snapshot_open_by_sid(alias_sid, &error);
	if (error)
		goto out;
	if (found_alias != NULL)
		*found_alias = true;
	if (alias_snap == NULL || !snapshot_is_alias(alias_snap))
		goto err;
	snapshot_get_alias_target(alias_snap, &old_alias_target);
	if (old_alias_target == target_sid)
		goto out; /* already done */

	s_alias = snapshot_get_name(alias_snap);
	if (s_alias == NULL)
		goto err;

	s_aliases[0] = s_alias;

	/* retrieve and validate the target snap */
	if (target_sid != HEAD_SNAPID) {
		target_snap = snapshot_open_by_sid(target_sid, &error);
		if (error)
			goto out;
		if (found_target != NULL)
			*found_target = true;
		if (target_snap == NULL || snapshot_is_alias(target_snap))
			goto err;
	}

	/* now move it */
	if (target_sid == HEAD_SNAPID)
		snapshot_alias_create(NULL, s_aliases, true, &error);
	else if (target_snap)
		snapshot_alias_create(target_snap, s_aliases, false, &error);

	goto out;

err:
	error = isi_system_error_new(EINVAL,
		"Failed to move target of alias %ld to snapshot %ld.",
		alias_sid, target_sid);
	goto out;

out:
	if (s_alias)
		isi_str_free(s_alias);
	if (target_snap)
		snapshot_close(target_snap);
	if (alias_snap)
		snapshot_close(alias_snap);
	isi_error_handle(error, error_out);
	return;
}

/*
 * On failback, move the snapshot_alias refered to in target_record
 * from HEAD to the mirror policy's latest snap in the source record.
 * sets error_out to EINVAL if:
 *      no target record exists for policy_id
 *      no source record exists for mirror_id
 */
void
move_snapshot_alias_from_head(const char *mirror_id,
    const char *policy_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int lock_fd = -1;
	struct target_record *trec = NULL;
	ifs_snapid_t target_sid = INVALID_SNAPID;

	if (!mirror_id || !policy_id)
		goto err;

	/* determine the target snap alias, if it doesn't exist then quit */
	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (error)
		goto out;
	if (-1 == lock_fd)
		goto err;

	trec = siq_target_record_read(policy_id, &error);
	if (error)
		goto out;
	ASSERT(trec != NULL);
	if (trec->latest_archive_snap_alias == INVALID_SNAPID)
		goto out;

	/* determine the mirror policy's source snap */
	if (g_ctx.record == NULL) {
		g_ctx.record = siq_source_record_load(mirror_id, &error);
		if (error || g_ctx.record == NULL) {
			goto out;
		}
	}
	siq_source_get_latest(g_ctx.record, &target_sid);
	if (target_sid == INVALID_SNAPID)
		goto err;

	/* now do the move */
	move_snapshot_alias_helper(target_sid, trec->latest_archive_snap_alias,
		NULL, NULL, &error);
	goto out;

err:
	error = isi_system_error_new(EINVAL,
		"Failed to determine target alias for policy %s to move from "
		"head to snapshot in mirror policy %s.",
		policy_id, mirror_id);
	goto out;

out:
	if (-1 != lock_fd)
		close(lock_fd);
	siq_target_record_free(trec);
	isi_error_handle(error, error_out);
}

