#include <stdio.h>

#include "isi_migrate/siq_error.h"
#include "../sched/siq_log.h"
#include "siq_alert.h"
#include "siq_source.h"


struct siq_alert {
	enum siq_alert_type type;

	/* prevents coalescing of 'sub-alerts' since we can have multiple
	 * siq alerts/strings mapping to one celog event type */
	char *tag;

	/* alert has a corresponding celog event id and behavior */
	int celog_eid;

	/* alert text */
	char *description;
};


/* siq_alert table indices do *not* match the enumeration in siq_alert.h.
 * siq_alert_get fetches the siq_alert struct that corresponds to an
 * siq_alert_type */
static struct siq_alert siq_alert_table[] = 
{	
	/* This alert just handles existing places where we alerted but don't
	 * have anything more specific to say. This alert should *NOT* be used
	 * going forward -- we should create new alert/event types as needed
	 * when suitable ones don't already exist */
	{SIQ_ALERT_POLICY_FAIL, "policy_fail",
	 SW_SIQ_POLICY_FAIL,
	 "SyncIQ policy failed."},

	{SIQ_ALERT_POLICY_START, "policy_start",
	 SW_SIQ_POLICY_START,
	 "SyncIQ scheduler failed to start policy."},

	{SIQ_ALERT_POLICY_CONFIG, "policy_config",
	 SW_SIQ_POLICY_CONFIG,
	 "SyncIQ detected a problem with policy configuration."},

	{SIQ_ALERT_TARGET_PATH_OVERLAP, "target_path_overlap",
	 SW_SIQ_POLICY_CONFIG,
	 "SyncIQ policy target path overlaps target path of another policy."},

	{SIQ_ALERT_SELF_PATH_OVERLAP, "self_path_overlap",
	 SW_SIQ_POLICY_CONFIG,
	 "SyncIQ policy target path overlaps source path on same cluster."},

	{SIQ_ALERT_VERSION, "version",
	 SW_SIQ_VERSION,
	 "SyncIQ attempting to sync to incompatible (older) target version. "
	 "Target may need to be upgraded to proceed."},

	{SIQ_ALERT_SIQ_CONFIG, "siq_config",
	 SW_SIQ_SIQ_CONFIG,
	 "SyncIQ configuration error."},

	{SIQ_ALERT_TARGET_CONNECTIVITY, "target_connectivity",
	 SW_SIQ_CONNECTIVITY,
	 "SyncIQ is unable to connect to a resource on the target cluster. "
	 "Use ping to determine if target is reachable. Verify that SyncIQ is "
	 "licensed and enabled on the target cluster, and that the SyncIQ "
	 "daemons are running."},

	{SIQ_ALERT_LOCAL_CONNECTIVITY, "local_connectivity",
	 SW_SIQ_CONNECTIVITY,
	 "SyncIQ error connecting to a local resource."},

	{SIQ_ALERT_DAEMON_CONNECTIVITY, "daemon_connectivity",
	 SW_SIQ_CONNECTIVITY,
	 "SyncIQ error connecting to daemon (bandwidth, throttle, pworker). "
	 "Please verify all SyncIQ daemons are running."},

	{SIQ_ALERT_SOURCE_SNAPSHOT, "source_snapshot",
	 SW_SIQ_SNAPSHOT,
	 "SyncIQ failed to take a snapshot on source cluster."},

	{SIQ_ALERT_TARGET_SNAPSHOT, "target_snapshot",
	 SW_SIQ_SNAPSHOT,
	 "SyncIQ failed to take snapshot on target cluster. Verify that "
	 "snapshots are licensed on the target, or disable target snapshots "
	 "for the policy."},

	{SIQ_ALERT_BBD_CHECKSUM, "bbd_checksum",
	 SW_SIQ_BBD_CHECKSUM,
	 "SyncIQ detected target file mismatch (checksum). File(s) may have "
	 "been modified on target cluster."},

	{SIQ_ALERT_FS, "fs",
	 SW_SIQ_FS,
	 "SyncIQ encountered a filesystem error."},

	{SIQ_ALERT_SOURCE_FS, "source_fs",
	 SW_SIQ_FS,
	 "SyncIQ encountered a filesystem error on source cluster."},

	{SIQ_ALERT_TARGET_FS, "target_fs",
	 SW_SIQ_FS,
	 "SyncIQ encountered a filesystem error on target cluster."},

	{SIQ_ALERT_POLICY_UPGRADE, "policy_upgrade",
	 SW_SIQ_POLICY_UPGRADE,
	 "SyncIQ failed to upgrade policy."},

	{SIQ_ALERT_TARGET_ASSOC, "target_assoc",
	 SW_SIQ_TARGET_ASSOC,
	 "SyncIQ policy target association error."},

	{SIQ_ALERT_RPO_EXCEEDED, "rpo_exceeded",
	 SW_SIQ_RPO_EXCEEDED,
	 "SyncIQ RPO exceeded."},

	{SIQ_ALERT_COMP_CONFLICTS, "comp_conflicts",
	 SW_SIQ_COMP_CONFLICTS,
	 "SyncIQ resolved compliance mode WORM committed file conflicts."},
};


static struct siq_alert *
siq_get_alert(enum siq_alert_type type)
{
	int i;
	struct siq_alert *alert = NULL;

	for (i = 0;
	    i < (sizeof(siq_alert_table) / sizeof(struct siq_alert));
	    i++) {
		if (siq_alert_table[i].type == type)
			alert = &siq_alert_table[i];
	}

	return alert;
}


/* Caller chooses specific siq alert for a given situation.
 *
 * Returns true iff the alert has been sent to CELOG and updated in our
 * gc file. */
static bool
siq_create_alert_internal(enum siq_alert_type type, const char *policy_name,
    const char *policy_id, const char *target, time_t last_success,
    char **msg_out, struct siq_gc_active_alert *alerts, const char *fmt, ...)
{
	struct siq_alert *alert = NULL;
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;
	va_list ap;
	bool res = false;
	bool rpo = false;
	char *caller_msg = NULL;
	char *full_msg = NULL;
	const char *pname = NULL;
	const char *tname = NULL;

	if (type == SIQ_ALERT_NONE)
		goto out;

	ASSERT(type < SIQ_ALERT_MAX);
	rpo = (type == SIQ_ALERT_RPO_EXCEEDED);

	alert = siq_get_alert(type);
	ASSERT(alert);
	
	pname = policy_name ? policy_name : "Unknown";
	tname = target ? target : "Unknown";

	va_start(ap, fmt);
	vasprintf(&caller_msg, fmt, ap);
	va_end(ap);

	/* full message consists of the alert description followed by the
	 * caller's passed in formatted string */
	asprintf(&full_msg, "(policy name: %s target: %s) %s %s",
	    pname, tname, alert->description, caller_msg);
	free(caller_msg);

	if (alerts == NULL) {
		active_alerts = siq_gc_active_alert_load(policy_id, &error);
		if (error) {
			log(ERROR, "%s: Failed to load active alerts for "
			    "policy %s (%s): %s", __func__, policy_name,
			    policy_id, isi_error_get_message(error));
			goto out;
		}
	} else {
		active_alerts = alerts;
	}

	/* skip creating an alert if the source record is missing */
	if (!siq_source_has_record(policy_id))
		goto out;

	log(ERROR, "%s: type: %d %s", __func__, type, full_msg);

	/* The last_success specifier is currently only used for rpo alerts. */
	if (rpo) {
		celog_event_create(NULL, CCF_NONE, alert->celog_eid, CS_WARN,
		    0, full_msg, "policy %s target %s tag %s last_success %ld",
		    pname, tname, alert->tag, last_success);
	} else {
		celog_event_create(NULL, CCF_NONE, alert->celog_eid, CS_WARN,
		    0, full_msg, "policy %s target %s tag %s", pname, tname,
		    alert->tag);
	}

	siq_gc_active_alert_add(active_alerts, alert->celog_eid, last_success);
	siq_gc_active_alert_save(active_alerts, &error);
	if (error != NULL) {
		log(ERROR,
		    "%s: Failed to save active alerts for policy %s (%s): %s",
		    __func__, policy_name, policy_id,
		    isi_error_get_message(error));
		goto out;
	}

	res = true;

out:
	if (alerts == NULL)
		siq_gc_active_alert_free(active_alerts);
	if (msg_out != NULL)
		*msg_out = full_msg;
	else
		free(full_msg);

	isi_error_free(error);
	return res;
}

bool
siq_create_alert(enum siq_alert_type type, const char *policy_name,
    const char *policy_id, const char *target, char **msg_out,
    const char *msg)
{
	return siq_create_alert_internal(type, policy_name, policy_id, target,
	    0, msg_out, NULL, "%s", msg);
}

/* Checks to see if the SIQ_ALERT_RPO_EXCEEDED for the policy_id is currently
 * active. If it is not, we attempt to create an event for type.
 *
 * Returns true iff an alert has been sent to CELOG */
bool
siq_create_rpo_alert(const char *policy_name, const char *policy_id,
    const char *target, time_t last_success, char **msg_out,
    struct siq_gc_active_alert *alerts, const char *msg)
{
	return siq_create_alert_internal(SIQ_ALERT_RPO_EXCEEDED, policy_name,
	    policy_id, target, last_success, msg_out, alerts, "%s", msg);
}

/* Checks to see if the SIQ_ALERT_SIQ_CONFIG is currently
 * active. If it is not, we attempt to create an event for type.
 *
 * Returns true iff an alert has been sent to CELOG */
bool
siq_create_config_alert(char **msg_out, const char *fmt, ...)
{
	struct siq_alert *alert = NULL;
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;
	va_list ap;
	bool res = false;
	char *caller_msg = NULL;
	char *full_msg = NULL;
	char *name = GLOBAL_SIQ_ALERT;
	enum siq_alert_type type = SIQ_ALERT_SIQ_CONFIG;

	alert = siq_get_alert(type);
	ASSERT(alert != NULL);

	va_start(ap, fmt);
	vasprintf(&caller_msg, fmt, ap);
	ASSERT(caller_msg);
	va_end(ap);

	/* full message consists of the alert description followed by the
	 * caller's passed in formatted string */
	asprintf(&full_msg, "%s: %s", alert->description, caller_msg);
	ASSERT(full_msg);
	free(caller_msg);

	active_alerts = siq_gc_active_alert_load(name, &error);
	if (error) {
		log(ERROR, "%s: Failed to load active alerts for %s: %s",
		    __func__, name, isi_error_get_message(error));
		goto out;
	}

	log(ERROR, "%s: type: %d %s", __func__, type, full_msg);

	if (!siq_gc_active_alert_exists(active_alerts, alert->celog_eid)) {
		celog_event_create(NULL, CCF_NONE, alert->celog_eid, CS_WARN,
		    0, full_msg, "policy %s tag %s", name, alert->tag);
		siq_gc_active_alert_add(active_alerts, alert->celog_eid, 0);
		siq_gc_active_alert_save(active_alerts, &error);
		if (error != NULL) {
			log(ERROR,
			    "%s: Failed to save active alerts for %s: %s",
			    __func__, name, isi_error_get_message(error));
			goto out;
		}
	}

	res = true;

out:
	siq_gc_active_alert_free(active_alerts);
	if (msg_out != NULL)
		*msg_out = full_msg;
	else
		free(full_msg);

	isi_error_free(error);
	return res;
}

static bool
siq_create_alert_from_siq_error(struct isi_siq_error *siq_error_in,
    const char *policy_name, const char *policy_id, const char *target,
    char **msg_out)
{
	enum siq_alert_type type;
       
	type = siq_err_get_alert_type(isi_siq_error_get_siqerr(siq_error_in));

	return siq_create_alert(type, policy_name, policy_id, target,
	    msg_out, isi_error_get_message((struct isi_error *)siq_error_in));
}


static bool
siq_create_alert_from_siq_fs_error(struct isi_siq_fs_error *siq_fs_error_in,
    const char *policy_name, const char *policy_id, const char *target,
    char **msg_out)
{
	const char *err_msg = NULL;
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;
	bool res = false;

	err_msg = isi_error_get_message((struct isi_error *)siq_fs_error_in);

	active_alerts = siq_gc_active_alert_load(policy_id, &error);
	if (error != NULL) {
		log(ERROR,
		    "%s: Failed to load active alerts for policy %s (%s): %s",
		    __func__, policy_name, policy_id,
		    isi_error_get_message(error));
		goto out;
	}

	celog_event_create(NULL, CCF_NONE, SW_SIQ_FS, CS_WARN, 0, err_msg,
	    "policy %s target %s tag fs file %s filelin %llx "
	    "dir %s dirlin %llx", policy_name, target,
	    isi_siq_fs_error_get_file(siq_fs_error_in),
	    isi_siq_fs_error_get_filelin(siq_fs_error_in),
	    isi_siq_fs_error_get_dir(siq_fs_error_in),
	    isi_siq_fs_error_get_dirlin(siq_fs_error_in));

	siq_gc_active_alert_add(active_alerts, SW_SIQ_FS, 0);
	siq_gc_active_alert_save(active_alerts, &error);
	if (error != NULL) {
		log(ERROR,
		    "%s: Failed to save active alerts for policy %s (%s): %s",
		    __func__, policy_name, policy_id,
		    isi_error_get_message(error));
		goto out;
	}

	log(ERROR, "%s: pol_name: %s target: %s msg: %s",
	    __func__, policy_name, target, err_msg);

	res = true;

out:
	siq_gc_active_alert_free(active_alerts);
	isi_error_free(error);
	return res;
}


/* pick alert based on info from isi error and siqerr map. only siq alerts
 * that have an associated alert type in the siqerr map will result in an
 * alert being created */
bool
siq_create_alert_from_error(struct isi_error *error_in,
    const char *policy_name, const char *policy_id, const char *target,
    char **msg_out)
{
	bool res = false;
	if (isi_error_is_a(error_in, ISI_SIQ_ERROR_CLASS)) {
		res = siq_create_alert_from_siq_error(
		    (struct isi_siq_error *)error_in, policy_name, policy_id,
		    target, msg_out);

	} else if (isi_error_is_a(error_in, ISI_SIQ_FS_ERROR_CLASS)) {
		res = siq_create_alert_from_siq_fs_error(
		    (struct isi_siq_fs_error *)error_in, policy_name,
		    policy_id, target, msg_out);

	} else if (isi_error_is_a(error_in, ISI_SYSTEM_ERROR_CLASS)) {
		/* not enough info to know whether to create alert, or what
		 * type. when synciq is using isi siq errors exclusively, this
		 * won't be a problem. for now, any system error that should
		 * result in an alert needs to be converted to a siq error,
		 * and its info/alert mapping needs to be added to the
		 * siq_errmap */
		goto out;

	} else {
		/* what else is there? */
		goto out;
	}

out:
	return res;
}

/* If alerts is not NULL, then the caller will save and free the
 * siq_gc_active_alert */
bool
siq_cancel_alert_internal(enum siq_alert_type type, const char *policy_name,
    const char *policy_id, struct siq_gc_active_alert *alerts)
{
	struct siq_alert *alert = NULL;
	struct siq_gc_active_alert *active_alerts = NULL;
	struct siq_active_alert *current = NULL;
	struct siq_active_alert *temp = NULL;
	struct isi_error *error = NULL;
	bool alerts_modified = false;
	bool rpo = false;

	if (type == SIQ_ALERT_NONE)
		goto out;

	ASSERT(type < SIQ_ALERT_MAX);
	rpo = (type == SIQ_ALERT_RPO_EXCEEDED);

	alert = siq_get_alert(type);
	if (alert == NULL)
		goto out;

	if (alerts == NULL) {
		active_alerts = siq_gc_active_alert_load(policy_id, &error);
		if (error) {
			log(ERROR, "%s: Failed to load active alerts for "
			    "policy %s (%s): %s", __func__, policy_name,
			    policy_id, isi_error_get_message(error));
			goto out;
		}
	} else {
		active_alerts = alerts;
	}

	SLIST_FOREACH_SAFE(current, &active_alerts->root->alert, next, temp) {
		if (current->celog_eid == alert->celog_eid) {
			if (rpo) {
				celog_event_end(CEF_END | CEF_ANONYMOUS,
				    alert->celog_eid, "policy %s last_success "
				    "%ld", policy_name, current->last_success);
				log(NOTICE, "%s: Resolving event: %s. "
				    "Last success: %ld", __func__, alert->tag,
				    current->last_success);
			} else {
				celog_event_end(CEF_END | CEF_ANONYMOUS,
				    alert->celog_eid, "policy %s",
				    policy_name);
				log(NOTICE, "%s: Resolving event: %s",
				    __func__, alert->tag);
			}

			SLIST_REMOVE(&active_alerts->root->alert, current,
			    siq_active_alert, next);
			FREE_AND_NULL(current);
			alerts_modified = true;
		}
	}

	if (alerts_modified) {
		siq_gc_active_alert_save(active_alerts, &error);
		if (error != NULL) {
			log(ERROR,
			    "%s: Failed to save active alerts for policy %s"
			    " (%s): %s", __func__, policy_name, policy_id,
			    isi_error_get_message(error));
			alerts_modified = false;
			goto out;
		}
	}

out:
	if (alerts == NULL)
		siq_gc_active_alert_free(active_alerts);
	isi_error_free(error);
	return alerts_modified;
}

/* Returns true iff a cancel request was sent to CELOG and the alerts gconfig
 * was updated accordingly. */
bool
siq_cancel_alert(enum siq_alert_type type, const char *policy_name,
    const char *policy_id)
{
	return siq_cancel_alert_internal(type, policy_name, policy_id, NULL);
}

void
siq_cancel_alerts_for_policy(const char *policy_name, const char *policy_id,
    enum siq_alert_type exclude[], size_t exclude_size)
{
	struct siq_gc_active_alert *active_alerts = NULL;
	struct isi_error *error = NULL;
	bool alerts_modified = false;
	bool skip_alert = false;
	size_t i;
	enum siq_alert_type type;

	active_alerts = siq_gc_active_alert_load(policy_id, &error);
	if (error != NULL) {
		log(ERROR,
		    "%s: Failed to load active alerts for policy %s (%s): %s",
		    __func__, policy_name, policy_id,
		    isi_error_get_message(error));
		goto out;
	}

	for (type = SIQ_ALERT_NONE; type < SIQ_ALERT_MAX; type++) {
		for (i = 0; i < exclude_size; i++) {
			if (type == exclude[i]) {
				skip_alert = true;
				break;
			}
		}

		if (skip_alert) {
			skip_alert = false;
			continue;
		}

		alerts_modified |= siq_cancel_alert_internal(type,
		    policy_name, policy_id, active_alerts);
	}

	if (alerts_modified) {
		siq_gc_active_alert_save(active_alerts, &error);
		if (error != NULL) {
			log(ERROR,
			    "%s: Failed to save active alerts for policy %s"
			    " (%s): %s", __func__, policy_name, policy_id,
			    isi_error_get_message(error));
			goto out;
		}
	}

out:
	siq_gc_active_alert_free(active_alerts);
	isi_error_free(error);
	return;
}

void
siq_cancel_all_alerts_for_policy(const char *policy_name,
    const char *policy_id)
{
	siq_cancel_alerts_for_policy(policy_name, policy_id, NULL, 0);
}

void
siq_gc_active_alert_free(struct siq_gc_active_alert *alerts)
{
	if (!alerts) {
		return;
	}

	if (alerts->gci_ctx) {
		gci_ctx_free(alerts->gci_ctx);
	}

	if (alerts->gci_base) {
		gcfg_close(alerts->gci_base);
	}

	if (alerts->lock_fd != -1) {
		close(alerts->lock_fd);
		alerts->lock_fd = -1;
	}

	FREE_AND_NULL(alerts->path);
	free(alerts);
}

struct siq_gc_active_alert *
siq_gc_active_alert_load(const char *policy_id, struct isi_error **error_out)
{
	struct siq_gc_active_alert *alerts = NULL;
	struct isi_error *error = NULL;
	char path[MAXPATHLEN + 1];

	ASSERT(policy_id);
	ASSERT(strlen(policy_id) == POLICY_ID_LEN ||
	    strcmp(policy_id, GLOBAL_SIQ_ALERT) == 0, "%s (len = %zu)",
	    policy_id, strlen(policy_id));

	snprintf(path, sizeof(path), "%s/%s.gc", SIQ_ACTIVE_ALERTS_DIR,
	    policy_id);

	CALLOC_AND_ASSERT(alerts, struct siq_gc_active_alert);
	alerts->path = strdup(path);
	alerts->gci_tree.root = &gci_ivar_siq_active_alert_root;
	alerts->gci_tree.primary_config = alerts->path;
	alerts->gci_tree.fallback_config_ro = NULL;
	alerts->gci_tree.change_log_file = NULL;

	snprintf(path, sizeof(path), "%s/%s.lock", SIQ_ACTIVE_ALERTS_LOCK_DIR,
	    policy_id);
	alerts->lock_fd = acquire_lock(path, O_EXLOCK, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to acquire lock %s: %s", __func__,
		    path, isi_error_get_message(error));
		goto out;
	}

	alerts->gci_base = gcfg_open(&alerts->gci_tree, 0, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	alerts->gci_ctx = gci_ctx_new(alerts->gci_base, true, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	gci_ctx_read_path(alerts->gci_ctx, "", &alerts->root, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

out:
	if (error != NULL) {
		siq_gc_active_alert_free(alerts);
		alerts = NULL;
	}

	isi_error_handle(error, error_out);
	return alerts;
}

void
siq_gc_active_alert_save(struct siq_gc_active_alert *alerts,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(alerts != NULL);
	ASSERT(alerts->lock_fd >= 0);

	gci_ctx_write_path(alerts->gci_ctx, "", alerts->root, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	gci_ctx_commit(alerts->gci_ctx, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	gci_ctx_free(alerts->gci_ctx);
	alerts->gci_ctx = gci_ctx_new(alerts->gci_base, true, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	gci_ctx_read_path(alerts->gci_ctx, "", &alerts->root, &error);
	if (error != NULL) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/* NOTE: It is possible that an alert exists in the gconfig file and is
 * not currently active in the CELOG database. This can happen if a user
 * ends/quiets an event via the CLI or WebUI. At all times, the CELOG
 * list of events is a subset of the SIQ list so at worst we may send
 * a few extra cancel requests upon calls to siq_cancel_event(). */
bool
siq_gc_active_alert_exists(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid)
{
	bool found = false;
	struct siq_active_alert *current = NULL;

	ASSERT(alerts != NULL);

	SLIST_FOREACH(current, &alerts->root->alert, next) {
		if (current->celog_eid == celog_eid) {
			found = true;
			break;
		}
	}

	return found;
}

void
siq_gc_active_alert_add(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid, time_t last_success)
{
	struct siq_active_alert *new_alert = NULL;

	ASSERT(alerts != NULL);

	CALLOC_AND_ASSERT(new_alert, struct siq_active_alert);
	new_alert->celog_eid = celog_eid;
	new_alert->last_success = last_success;

	SLIST_INSERT_HEAD(&alerts->root->alert, new_alert, next);
}

/* Removes all alerts for a given celog_eid from the gconfig file */
void
siq_gc_active_alert_remove(struct siq_gc_active_alert *alerts,
    uint32_t celog_eid)
{
	struct siq_active_alert *current = NULL;
	struct siq_active_alert *temp = NULL;

	ASSERT(alerts != NULL);

	SLIST_FOREACH_SAFE(current, &alerts->root->alert, next, temp) {
		if (current->celog_eid == celog_eid) {
			SLIST_REMOVE(&alerts->root->alert, current,
			    siq_active_alert, next);
			FREE_AND_NULL(current);
		}
	}
}

void
siq_gc_active_alert_cleanup(const char *policy_id)
{
	char path[MAXPATHLEN + 1];

	ASSERT(policy_id);
	ASSERT(strlen(policy_id) == POLICY_ID_LEN ||
	    strcmp(policy_id, GLOBAL_SIQ_ALERT) == 0);

	log(TRACE, "%s", __func__);

	snprintf(path, sizeof(path), "%s/%s.gc", SIQ_ACTIVE_ALERTS_DIR,
	    policy_id);
	if (unlink(path) != 0) {
		log(errno == ENOENT ? DEBUG : ERROR,
		    "Failed to delete alert gc file for policy %s: %s",
		     policy_id, strerror(errno));
	}

	snprintf(path, sizeof(path), "%s/%s.gc.lck", SIQ_ACTIVE_ALERTS_DIR,
	    policy_id);
	if (unlink(path) != 0) {
		log(errno == ENOENT ? DEBUG : ERROR,
		    "Failed to delete alert gc lck file for policy %s: %s",
		     policy_id, strerror(errno));
	}

	snprintf(path, sizeof(path), "%s/%s.lock", SIQ_ACTIVE_ALERTS_LOCK_DIR,
	    policy_id);
	if (unlink(path) != 0) {
		log(errno == ENOENT ? DEBUG : ERROR,
		    "Failed to delete alert lock file for policy %s: %s",
		     policy_id, strerror(errno));
	}
}
