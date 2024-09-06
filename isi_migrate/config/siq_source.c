#include <unistd.h>

#define ISI_ERROR_PROTECTED
#include <isi_snapshot/snapshot.h>
#include <isi_util/isi_error_protected.h>
#include <isi_xml/xmlutil.h>
#include <isi_migrate/migr/isirep.h>

#include "siq_source.h"

#define SIQ_SOURCE		"siq-source"
#define SIQ_RECORDS		"policy-records"
#define SIQ_RECORD		"policy-record"
#define SIQ_ID			"policy-id"
#define SIQ_REVISION		"revision"
#define SIQ_STATE		"state"
#define SIQ_NEW_SNAP		"new-snap-id"
#define SIQ_RETAIN_NEW_SNAP	"retain-new-snap"
#define SIQ_LATEST_SNAP		"latest-snap-id"
#define SIQ_RETAIN_LATEST_SNAP	"retain-latest-snap"
#define SIQ_MANUAL_SNAP		"manual-snap-id"
#define SIQ_RESTORE_NEW_SNAP	"restore-new-snap-id"
#define SIQ_RESTORE_LATEST_SNAP	"restore-latest-snap-id"
#define SIQ_DELETE_SNAP		"delete-snap-id"
#define SIQ_DELETE_SNAP2	"delete-snap-id2"
#define SIQ_UNRUNNABLE		"unrunnable"
#define SIQ_POLICY_DELETE	"policy-delete"
#define SIQ_PENDING_JOB		"pending-job"
#define SIQ_CURRENT_OWNER	"current-owner"
#define SIQ_TARGET_GUID		"target-guid"
#define SIQ_START_SCHED		"start-sched"
#define SIQ_END_SCHED		"end-sched"
#define SIQ_POL_STF_READY	"pol-stf-ready"
#define SIQ_CONN_FAIL_NODES	"conn-fail-nodes"
#define SIQ_FAILBACK_STATE	"failback-state"
#define SIQ_RESET_PENDING	"reset-pending"
#define SIQ_MANUAL_FAILBACK_PREP	"manual-failback-prep"
#define SIQ_DATABASE_MIRRORED	"database-mirrored"
#define SIQ_JOB_DELAY_START	"job-delay-start"
#define SIQ_LAST_KNOWN_GOOD_TIME	"last-known-good-time"
#define SIQ_COMPOSITE_JOB_VER_LOCAL	"composite-job-ver-local"
#define SIQ_COMPOSITE_JOB_VER_COMMON	"composite-job-ver-common"
#define SIQ_PREV_COMPOSITE_JOB_VER_LOCAL	"prev-composite-job-ver-local"
#define SIQ_PREV_COMPOSITE_JOB_VER_COMMON	"prev-composite-job-ver-common"
#define SIQ_RESTORE_COMPOSITE_JOB_VER_LOCAL \
				"restore-composite-job-ver-local"
#define SIQ_RESTORE_COMPOSITE_JOB_VER_COMMON \
				"restore-composite-job-ver-common"
#define SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_LOCAL \
				"restore-prev-composite-job-ver-local"
#define SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_COMMON \
				"restore-prev-composite-job-ver-common"

/* Define lengths for snprintf, take into account '\0' */
/* <lock_dir>/.<polid>.lock */
#define LOCK_PATH_LEN strlen(SIQ_SOURCE_RECORDS_LOCK_DIR) + POLICY_ID_LEN + 8

/* <srec_dir>/<polid>.xml */
#define SREC_PATH_LEN strlen(SIQ_SOURCE_RECORDS_DIR) + POLICY_ID_LEN + 6

struct siq_source_pid_error {
	struct isi_error	_super;
	char 			_pid[POLICY_ID_LEN + 1];
};

ISI_ERROR_CLASS_DEFINE_EMPTY(siq_source_pid_error,
    SIQ_SOURCE_PID_ERROR_CLASS, ISI_ERROR_CLASS);

#define SIQ_SOURCE_PID_ERROR_CLASS_DEFINE(lower, upper, errorstr)	\
static void lower##_free_impl(struct isi_error *e) {isi_error_free_impl(e);} \
static void lower##_format_impl(struct isi_error *e,			\
    const char *format,	va_list ap) {					\
	const struct siq_source_pid_error *pe = (void *)e;		\
	fmt_print(&e->fmt, "%s: %s", errorstr, pe->_pid);		\
}									\
ISI_ERROR_CLASS_DEFINE(lower, upper##_CLASS,				\
    SIQ_SOURCE_PID_ERROR_CLASS);					\
struct isi_error *_##lower##_new(const char *func,			\
    const char *file, int line, const char *pid) {	\
	return _siq_source_pid_error_new(func, file, line,		\
	upper##_CLASS, pid); }

static struct isi_error *
_siq_source_pid_error_new(const char *function, const char *file,
    int line, const struct isi_error_class *ec, 
    const char *pid)
{
	union {
		va_list ap;
	} empty = {};
	struct siq_source_pid_error *e = calloc(1, sizeof(*e));
	ASSERT(e);

	strncpy(e->_pid, pid, POLICY_ID_LEN);
	e->_pid[POLICY_ID_LEN] = '\0';
	isi_error_init(&e->_super, ec, function, file, line, "", empty.ap);

	return &e->_super;
}

SIQ_SOURCE_PID_ERROR_CLASS_DEFINE(siq_source_stale_error,
    SIQ_SOURCE_STALE_ERROR,
    "Policy modification conflicted with a prior change");

SIQ_SOURCE_PID_ERROR_CLASS_DEFINE(siq_source_duplicate_error,
    SIQ_SOURCE_DUPLICATE_ERROR, "Policy already exists");

/*  ____       _ _                 _        _   _             
 * |  _ \ ___ | (_) ___ _   _     / \   ___| |_(_) ___  _ __  
 * | |_) / _ \| | |/ __| | | |   / _ \ / __| __| |/ _ \| '_ \ 
 * |  __/ (_) | | | (__| |_| |  / ___ \ (__| |_| | (_) | | | |
 * |_|   \___/|_|_|\___|\__, | /_/   \_\___|\__|_|\___/|_| |_|
 *                      |___/                                 
 */

MAKE_ENUM_PARSE(siq_policy_action_parse, siq_policy_action,
    SIQ_POLICY_ACTION_FOREACH);
MAKE_ENUM_FMT(siq_policy_action, enum siq_policy_action,
	{ SIQ_POLICY_FULL_SYNC, "full-sync", "Full Sync" },
	{ SIQ_POLICY_INCREMENTAL_SYNC, "incremental-sync",
	    "Incremental Sync" },
);

/*      _       _          _        _   _             
 *     | | ___ | |__      / \   ___| |_(_) ___  _ __  
 *  _  | |/ _ \| '_ \    / _ \ / __| __| |/ _ \| '_ \ 
 * | |_| | (_) | |_) |  / ___ \ (__| |_| | (_) | | | |
 *  \___/ \___/|_.__/  /_/   \_\___|\__|_|\___/|_| |_|
 */

MAKE_ENUM_PARSE(siq_job_action_parse, siq_job_action,
    SIQ_JOB_ACTION_FOREACH);
MAKE_ENUM_FMT(siq_job_action, enum siq_job_action,
	{ SIQ_JOB_NONE, "no-job", "No Job Action" },
	{ SIQ_JOB_RUN, "run-job", "Run Job" },
	{ SIQ_JOB_ASSESS, "assess-job", "Assess Job" },
	{ SIQ_JOB_FAILOVER, "failover-job", "Failover Job" },
	{ SIQ_JOB_FAILOVER_REVERT, "failover-revert-job", 
	    "Failover Revert Job" },
	{ SIQ_JOB_FAILBACK_PREP, "failback-prep-job", 
	    "Failback Prep Job - Init" },
	{ SIQ_JOB_FAILBACK_PREP_DOMAIN_MARK, "failback-prep-domain-mark-job", 
	    "Failback Prep Job - Domain Mark" },
	{ SIQ_JOB_FAILBACK_PREP_RESTORE, "failback-prep-restore-job", 
	    "Failback Prep Job - Restore" },
	{ SIQ_JOB_FAILBACK_PREP_FINALIZE, "failback-prep-finalize-job", 
	    "Failback Prep Job - Finalize" },
	{ SIQ_JOB_FAILBACK_COMMIT, "failback-commit-job", 
	    "Failback Commit Job" },
);


/*      _       _        ___                           
 *     | | ___ | |__    / _ \__      ___ __   ___ _ __ 
 *  _  | |/ _ \| '_ \  | | | \ \ /\ / / '_ \ / _ \ '__|
 * | |_| | (_) | |_) | | |_| |\ V  V /| | | |  __/ |   
 *  \___/ \___/|_.__/   \___/  \_/\_/ |_| |_|\___|_|   
 */

MAKE_ENUM_PARSE(siq_job_owner_parse, siq_job_owner,
    SIQ_JOB_OWNER_FOREACH);
MAKE_ENUM_FMT(siq_job_owner, enum siq_job_owner,
	{ SIQ_OWNER_NONE, "no-owner", "No Job Owner" },
	{ SIQ_OWNER_COORD, "coordinator", "Coordinator" },
);

/*  _____       _ _ _                 _       _____   _           _
 * |  ___| ___ |_| | |__   ___   ____| | __  /  ___|_| |_  ___  _| |_  ___
 * | |_   / _ \ _| | '_ \ / _ \ / ___| |/ /  \___  \_   _|/ _ \|_   _|/ _ \
 * |  _| | (_| | | | |_)_| (_| | (___|   (    ___) | | |_| ( | | | |_|  __/
 * |_|    \__._|_|_| .__/ \__._|\____|_|\_\  |_____/ \___|\__._| \___|\___|
 */

MAKE_ENUM_PARSE(siq_failback_state_parse, siq_failback_state,
    SIQ_FAILBACK_STATE_FOREACH);
MAKE_ENUM_FMT(siq_failback_state, enum siq_failback_state,
	{ SIQ_FBS_NONE, "no-fbs", "None" },
	{ SIQ_FBS_PREP_INIT, "init", "Prep: Init" },
	{ SIQ_FBS_PREP_DOM_TREEWALK, "dom-treewalk", "Prep: Domain Treewalk" },
	{ SIQ_FBS_PREP_RESTORE, "restore", "Prep: Snapshot Restore" },
	{ SIQ_FBS_PREP_FINALIZE, "finalize", "Prep: Finalize" },
	{ SIQ_FBS_SYNC_READY, "sync-ready", "Ready to Failback" },
);


/* add function to convert failback state from enum to text
 * for sra usage
 */
const char *
siq_source_failback_state_to_text(enum siq_failback_state state) {
        switch(state) {
        case SIQ_FBS_NONE:
                return "NONE";
        case SIQ_FBS_PREP_INIT:
                return "PREP_INIT";
        case SIQ_FBS_PREP_DOM_TREEWALK:
                return "PREP_DOM_TREEWALK";
        case SIQ_FBS_PREP_RESTORE:
                return "PREP_RESTORE";
        case SIQ_FBS_PREP_FINALIZE:
                return "PREP_FINALIZE";
        case SIQ_FBS_SYNC_READY:
                return "SYNC_READY";
        default:
                return "UNKNOWN";
        }
}


/*  ____                             ____                        _ 
 * / ___|  ___  _   _ _ __ ___ ___  |  _ \ ___  ___ ___  _ __ __| |
 * \___ \ / _ \| | | | '__/ __/ _ \ | |_) / _ \/ __/ _ \| '__/ _` |
 *  ___) | (_) | |_| | | | (_|  __/ |  _ <  __/ (_| (_) | | | (_| |
 * |____/ \___/ \__,_|_|  \___\___| |_| \_\___|\___\___/|_|  \__,_|
 */

static void
siq_source_record_clean(struct siq_source_record *record)
{
	memset(record, 0, sizeof(*record));
}

static void
siq_source_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const struct siq_source_record *record = (const void *)arg.ptr;

	fmt_print(fmt, "%s (%lld): delete=%d snaps=(%lld, %lld)",
	    record->_fields.pid, record->_fields.revision,
	    record->_fields.policy_delete, record->_fields.latest_snap,
	    record->_fields.new_snap);
}

MAKE_FMT_CONV(siq_source, const struct siq_source_record *);

static void
siq_source_record_lock(struct siq_source_record *rec, const char *pid,
    int type, struct isi_error **error_out)
{
	char lock_filename[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	ASSERT(type == O_SHLOCK || type == O_EXLOCK);

	ASSERT(rec->_lock_state == 0);
	ASSERT(rec->_lock_fd == -1);

	snprintf(lock_filename, LOCK_PATH_LEN, "%s/.%s.lock",
	    SIQ_SOURCE_RECORDS_LOCK_DIR, pid);

	rec->_lock_fd = acquire_lock(lock_filename, type, &error);
	if (error) {
		ASSERT(rec->_lock_fd == -1);
		goto out;
	}

	rec->_lock_state = type;

out:
	isi_error_handle(error, error_out);
}

void siq_source_record_free(struct siq_source_record *rec)
{
	if (rec == NULL)
		return;

	/* Shouldn't be freeing without unlocking */
	ASSERT(rec->_lock_fd == -1);
	ASSERT(rec->_lock_state == 0);

	free(rec);
}

void
siq_source_record_unlock(struct siq_source_record *rec)
{
	if (rec->_lock_fd >= 0)
		close(rec->_lock_fd);
	rec->_lock_fd = -1;
	rec->_lock_state = 0;
}

static void
siq_source_copy_guid(char *dst, const char *src)
{
	strncpy(dst, src, SIQ_MAX_TARGET_GUID_LEN);
	dst[SIQ_MAX_TARGET_GUID_LEN - 1] = 0;
}

static void
siq_source_read(xmlNodePtr node, struct siq_source_record *record,
    XmlResult *res)
{
	char *pid = NULL, *action = NULL, *owner = NULL, *target_guid;
	char *failback_state = NULL;
	char *conn_fail_nodes = NULL;
	int64_t temp;
	uint32_t i;

	CONFIG_GET_PROP(pid, node, SIQ_ID, res, out);

	if (strlen(pid) != POLICY_ID_LEN) {
		res->result = CONF_BAD_PARAM;
		res->param = SIQ_ID;
		goto out;
	} else
		strcpy(record->_fields.pid, pid);
	xmlFree(pid);

	CONFIG_EXTRACT(UInt64, node, SIQ_REVISION, &record->_fields.revision,
	    res, out);
	
	CONFIG_EXTRACT(UInt64, node, SIQ_NEW_SNAP, &record->_fields.new_snap,
	    res, out);

	CONFIG_EXTRACT(UInt64, node, SIQ_LATEST_SNAP, &record->_fields.latest_snap,
	    res, out);

	record->_fields.retain_latest_snap = !!xmlFindChild(node,
	    SIQ_RETAIN_LATEST_SNAP);

	CONFIG_EXTRACT_EXIST(UInt64, node, SIQ_MANUAL_SNAP,
	    &record->_fields.manual_snap, res, out);

	CONFIG_EXTRACT_EXIST(UInt64, node, SIQ_RESTORE_NEW_SNAP,
	    &record->_fields.restore_new_snap, res, out);

	CONFIG_EXTRACT_EXIST(UInt64, node, SIQ_RESTORE_LATEST_SNAP,
	    &record->_fields.restore_latest_snap, res, out);

	CONFIG_EXTRACT(UInt64, node, SIQ_DELETE_SNAP, &record->_fields.delete_snap[0],
	    res, out);

	CONFIG_EXTRACT(UInt64, node, SIQ_DELETE_SNAP2, &record->_fields.delete_snap[1],
	    res, out);

	CONFIG_EXTRACT(Int64, node, SIQ_START_SCHED, &temp, res, out);
	record->_fields.start_schedule = (time_t) temp;
	ASSERT(record->_fields.start_schedule >= 0);

	CONFIG_EXTRACT(Int64, node, SIQ_END_SCHED, &temp, res, out);
	record->_fields.end_schedule = (time_t) temp;
	ASSERT(record->_fields.end_schedule >= 0);

	record->_fields.unrunnable = !!xmlFindChild(node, SIQ_UNRUNNABLE);
	record->_fields.policy_delete = !!xmlFindChild(node, SIQ_POLICY_DELETE);
	record->_fields.pol_stf_ready = !!xmlFindChild(node, SIQ_POL_STF_READY);
	record->_fields.reset_pending = !!xmlFindChild(node, SIQ_RESET_PENDING);
	record->_fields.manual_failback_prep = !!xmlFindChild(node,
	    SIQ_MANUAL_FAILBACK_PREP);
	record->_fields.database_mirrored =
	    !!xmlFindChild(node, SIQ_DATABASE_MIRRORED);

	CONFIG_EXTRACT(String, node, SIQ_PENDING_JOB, &action, res, out);
	if (!siq_job_action_parse(action, &record->_fields.pending_job)) {
		res->result = CONF_BAD_PARAM;
		res->param = SIQ_PENDING_JOB;
	}
	xmlFree(action);

	CONFIG_EXTRACT(String, node, SIQ_CURRENT_OWNER, &owner, res, out);
	if (!siq_job_owner_parse(owner, &record->_fields.current_owner)) {
		res->result = CONF_BAD_PARAM;
		res->param = SIQ_CURRENT_OWNER;
	}
	xmlFree(owner);

	CONFIG_EXTRACT_EXIST(String, node, SIQ_FAILBACK_STATE, &failback_state,
	    res, out);
	record->_fields.failback_state = SIQ_FBS_NONE;
	if (failback_state && !siq_failback_state_parse(failback_state,
	    &record->_fields.failback_state)) {
		res->result = CONF_BAD_PARAM;
		res->param = SIQ_FAILBACK_STATE;
	}

	CONFIG_EXTRACT_EXIST(String, node, SIQ_TARGET_GUID, &target_guid,
	    res, out);
	siq_source_copy_guid(record->_fields.target_guid,
	    target_guid ? target_guid : "");
	xmlFree(target_guid);

	CONFIG_EXTRACT_EXIST(String, node, SIQ_CONN_FAIL_NODES,
	    &conn_fail_nodes, res, out);
	memset(record->_fields.conn_fail_nodes, '.', SIQ_MAX_NODES);
	if (conn_fail_nodes) {
		memcpy(record->_fields.conn_fail_nodes, conn_fail_nodes,
		    SIQ_MAX_NODES);
	}
	xmlFree(conn_fail_nodes);

	CONFIG_EXTRACT(Int64, node, SIQ_JOB_DELAY_START, &temp, res, out);
	record->_fields.job_delay_start = (time_t) temp;
	ASSERT(record->_fields.job_delay_start >= 0);

	CONFIG_EXTRACT(Int64, node, SIQ_LAST_KNOWN_GOOD_TIME, &temp, res, out);
	record->_fields.last_known_good_time = (time_t) temp;
	ASSERT(record->_fields.last_known_good_time >= 0);

	CONFIG_EXTRACT(UInt32, node, SIQ_COMPOSITE_JOB_VER_LOCAL, &i, res,
	    out);
	record->_fields.composite_job_ver.local = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_COMPOSITE_JOB_VER_COMMON, &i, res,
	    out);
	record->_fields.composite_job_ver.common = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_PREV_COMPOSITE_JOB_VER_LOCAL, &i,
	    res, out);
	record->_fields.prev_composite_job_ver.local = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_PREV_COMPOSITE_JOB_VER_COMMON, &i,
	    res, out);
	record->_fields.prev_composite_job_ver.common = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_RESTORE_COMPOSITE_JOB_VER_LOCAL,
	    &i, res, out);
	record->_fields.restore_composite_job_ver.local = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_RESTORE_COMPOSITE_JOB_VER_COMMON,
	    &i, res, out);
	record->_fields.restore_composite_job_ver.common = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_LOCAL,
	    &i, res, out);
	record->_fields.restore_prev_composite_job_ver.local = i;

	CONFIG_EXTRACT(UInt32, node, SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_COMMON,
	    &i, res, out);
	record->_fields.restore_prev_composite_job_ver.common = i;

out:
	if (configErrCode(res) != CONF_OK) {
		siq_source_record_clean(record);
	}
	if (failback_state)
		free(failback_state);
}

static void
siq_source_record_read(const char *pid, struct siq_source_record *rec,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	XmlTree tree = {};
	XmlResult *res = NULL;
	xmlNodePtr child, cur;
	char record_filename[MAXPATHLEN + 1];

	snprintf(record_filename, SREC_PATH_LEN, "%s/%s.xml",
	    SIQ_SOURCE_RECORDS_DIR, pid);

	res = configRead(&tree, record_filename);
	if (configErrCode(res) != CONF_OK)
		goto out;

	CONFIG_XML_FINDCHILD(child, tree.root, SIQ_RECORDS, res, out);

	cur = NULL;
	cur = xmlFindNextChild(child, SIQ_RECORD, cur);
	ASSERT(cur != NULL);
	siq_source_read(cur, rec, res);

out:
	if (configErrCode(res) != CONF_OK) {
		char *errmsg = configError(res);

		if (configErrCode(res) == CONF_NO_SUCH_FILE) {
			error = isi_system_error_new(ENOENT, "%s", errmsg);
		} else {
			error = isi_system_error_new(EINVAL, "%s", errmsg);
		}

		free(errmsg);
	}

	configFinish(&tree);
	configDestroyResult(res);
	isi_error_handle(error, error_out);
}

static struct siq_source_record *
siq_source_record_new(void)
{
	struct siq_source_record *rec = NULL;

	rec = calloc(1, sizeof(*rec));
	ASSERT(rec);
	rec->_lock_fd = -1;
	rec->_lock_state = 0;

	return rec;
}

/* This function:
 * 1. Grabs a shared lock on the source record.
 * 2. Loads the source record.
 * 3. Releases the shared lock.
 * 4. Returns a pointer to the requested source record.

 * If the caller modifies the source record, they should call
 * siq_source_record_save() to write their changes to disk.
 */
struct siq_source_record *
siq_source_record_load(const char *pid, struct isi_error **error_out)
{
	struct siq_source_record *rec = NULL;
	struct isi_error *error = NULL;

	if (strlen(pid) != POLICY_ID_LEN) {
		error = isi_system_error_new(EINVAL, "Failed to load source "
		    "record for invalid pid: %s", pid);
		goto out;
	}

	rec = siq_source_record_new();

	siq_source_record_lock(rec, pid, O_SHLOCK, &error);
	if (error)
		goto out;

	siq_source_record_read(pid, rec, &error);

out:
	if (rec)
		siq_source_record_unlock(rec);

	if (error && rec) {
		siq_source_record_free(rec);
		rec = NULL;
	}

	isi_error_handle(error, error_out);
	return rec;
}

/* This function:
 * 1. Grabs an exclusive lock on the source record.
 * 2. Writes the source record to disk.
 * 3. Releases the exclusive lock.
 *
 * The caller is expected to have loaded the source record they are
 * now saving with siq_source_record_load().
 */
void
siq_source_record_save(struct siq_source_record *rec,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_source_record_lock(rec, rec->_fields.pid, O_EXLOCK, &error);
	if (error) {
		log(ERROR, "%s: siq_source_acquire_lock failed %#{}",
		    __FUNCTION__, isi_error_fmt(error));
		goto out;
	}

	siq_source_record_write(rec, &error);
out:
	siq_source_record_unlock(rec);

	isi_error_handle(error, error_out);
}



static void
set_child_fmt(xmlNodePtr parent, const char *name, const char *format, ...)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	va_list args;

	va_start(args, format);
	fmt_vprint(&fmt, format, args);
	va_end(args);

	xmlNewChild(parent, NULL, name, fmt_string(&fmt));
}

static void
_siq_source_record_write_raw(const struct siq_source_record *record,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	XmlTree ftree = {};
	XmlResult *res = NULL;
	xmlNodePtr child, cur;
	struct fmt fmt = FMT_INITIALIZER;
	char record_filename[MAXPATHLEN + 1];
	char conn_fail_nodes[SIQ_MAX_NODES + 1];


	snprintf(record_filename, SREC_PATH_LEN, "%s/%s.xml",
	    SIQ_SOURCE_RECORDS_DIR, record->_fields.pid);

	ftree.doc = xmlNewDoc("1.0");
	ASSERT(ftree.doc);

	ftree.doc->children = xmlNewDocNode(ftree.doc, NULL, SIQ_SOURCE, NULL);
	ASSERT(ftree.doc->children);

	child = xmlNewChild(ftree.doc->children, NULL, SIQ_RECORDS, NULL);
	cur = xmlNewChild(child, NULL, SIQ_RECORD, NULL);
	xmlNewProp(cur, SIQ_ID, record->_fields.pid);
	xmlNewChildFmt(cur, SIQ_REVISION, "%llu", record->_fields.revision);
	xmlNewChildFmt(cur, SIQ_NEW_SNAP, "%llu", record->_fields.new_snap);
	xmlNewChildFmt(cur, SIQ_LATEST_SNAP, "%llu",
	    record->_fields.latest_snap);
	if (record->_fields.retain_latest_snap)
		xmlNewChild(cur, NULL, SIQ_RETAIN_LATEST_SNAP, NULL);
	xmlNewChildFmt(cur, SIQ_MANUAL_SNAP, "%llu",
	    record->_fields.manual_snap);
	xmlNewChildFmt(cur, SIQ_RESTORE_NEW_SNAP, "%llu",
	    record->_fields.restore_new_snap);
	xmlNewChildFmt(cur, SIQ_RESTORE_LATEST_SNAP, "%llu",
	    record->_fields.restore_latest_snap);
	xmlNewChildFmt(cur, SIQ_DELETE_SNAP, "%llu",
	    record->_fields.delete_snap[0]);
	xmlNewChildFmt(cur, SIQ_DELETE_SNAP2, "%llu",
	    record->_fields.delete_snap[1]);

	ASSERT(record->_fields.start_schedule >= 0);
	xmlNewChildFmt(cur, SIQ_START_SCHED, "%lld",
	    record->_fields.start_schedule);

	ASSERT(record->_fields.end_schedule >= 0);
	xmlNewChildFmt(cur, SIQ_END_SCHED, "%lld",
	    record->_fields.end_schedule);

	if (record->_fields.unrunnable)
		xmlNewChild(cur, NULL, SIQ_UNRUNNABLE, NULL);

	if (record->_fields.policy_delete)
		xmlNewChild(cur, NULL, SIQ_POLICY_DELETE, NULL);

	if (record->_fields.pol_stf_ready)
		xmlNewChild(cur, NULL, SIQ_POL_STF_READY, NULL);

	if (record->_fields.manual_failback_prep)
		xmlNewChild(cur, NULL, SIQ_MANUAL_FAILBACK_PREP, NULL);

	if (record->_fields.reset_pending)
		xmlNewChild(cur, NULL, SIQ_RESET_PENDING, NULL);

	if (record->_fields.database_mirrored)
		xmlNewChild(cur, NULL, SIQ_DATABASE_MIRRORED, NULL);

	set_child_fmt(cur, SIQ_PENDING_JOB, "%{}",
	    siq_job_action_fmt(record->_fields.pending_job));

	set_child_fmt(cur, SIQ_CURRENT_OWNER, "%{}",
	    siq_job_owner_fmt(record->_fields.current_owner));

	if (record->_fields.failback_state) {
		set_child_fmt(cur, SIQ_FAILBACK_STATE, "%{}",
		    siq_failback_state_fmt(record->_fields.failback_state));
	}

	if (*record->_fields.target_guid)
		xmlNewChild(cur, NULL, SIQ_TARGET_GUID,
		    record->_fields.target_guid);

	if (*record->_fields.conn_fail_nodes == 0)
		memset(conn_fail_nodes, '.', SIQ_MAX_NODES);
	else
		memcpy(conn_fail_nodes, record->_fields.conn_fail_nodes,
		    SIQ_MAX_NODES);
	conn_fail_nodes[SIQ_MAX_NODES] = '\0';
	xmlNewChild(cur, NULL, SIQ_CONN_FAIL_NODES, conn_fail_nodes);

	ASSERT(record->_fields.job_delay_start >= 0);
	xmlNewChildFmt(cur, SIQ_JOB_DELAY_START, "%lld",
	    record->_fields.job_delay_start);

	ASSERT(record->_fields.last_known_good_time >= 0);
	xmlNewChildFmt(cur, SIQ_LAST_KNOWN_GOOD_TIME, "%lld",
	    record->_fields.last_known_good_time);

	xmlNewChildFmt(cur, SIQ_COMPOSITE_JOB_VER_LOCAL, "%u",
	    record->_fields.composite_job_ver.local);

	xmlNewChildFmt(cur, SIQ_COMPOSITE_JOB_VER_COMMON, "%u",
	    record->_fields.composite_job_ver.common);

	xmlNewChildFmt(cur, SIQ_PREV_COMPOSITE_JOB_VER_LOCAL, "%u",
	    record->_fields.prev_composite_job_ver.local);

	xmlNewChildFmt(cur, SIQ_PREV_COMPOSITE_JOB_VER_COMMON, "%u",
	    record->_fields.prev_composite_job_ver.common);

	xmlNewChildFmt(cur, SIQ_RESTORE_COMPOSITE_JOB_VER_LOCAL, "%u",
	    record->_fields.restore_composite_job_ver.local);

	xmlNewChildFmt(cur, SIQ_RESTORE_COMPOSITE_JOB_VER_COMMON, "%u",
	    record->_fields.restore_composite_job_ver.common);

	xmlNewChildFmt(cur, SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_LOCAL, "%u",
	    record->_fields.restore_prev_composite_job_ver.local);

	xmlNewChildFmt(cur, SIQ_RESTORE_PREV_COMPOSITE_JOB_VER_COMMON, "%u",
	    record->_fields.restore_prev_composite_job_ver.common);

	res = configWrite(&ftree, record_filename, 0640);
	if (configErrCode(res) != CONF_OK) {
		char *errmsg = configError(res);
		error = isi_system_error_new(errno, "Error writing %s: %s",
		    record_filename, errmsg);
		free(errmsg);
	}
	configFinish(&ftree);
	configDestroyResult(res);

	fmt_clean(&fmt);
	isi_error_handle(error, error_out);
}

/* This function:
 * 1. Grabs an exclusive lock on the source record.
 * 2. Loads the source record.
 * 3. Returns a pointer to the requested source record.
 * 
 * NOTE: The exclusive lock is not released. The caller must explicitly
 * release the lock via siq_source_record_unlock().
 *
 * If the caller modifies the source record, they should call
 * siq_source_record_write() to write their changes to disk.
 */

struct siq_source_record *
siq_source_record_open(const char *pid, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_source_record *rec = NULL;

	if (strlen(pid) != POLICY_ID_LEN) {
		error = isi_system_error_new(EINVAL, "Failed to load source "
		    "record for invalid pid: %s", pid);
		goto out;
	}

	rec = siq_source_record_new();

	siq_source_record_lock(rec, pid, O_EXLOCK, &error);
	if (error)
		goto out;

	siq_source_record_read(pid, rec, &error);

out:
	if (error && rec) {
		siq_source_record_unlock(rec);
		siq_source_record_free(rec);
		rec = NULL;
	}

	isi_error_handle(error, error_out);
	return rec;
}

/* This function:
 * 1. Writes the source record to disk.
 *
 * NOTE: The caller is expected to hold an exclusive lock on the source
 * record they are writing. They should have this lock by loading the
 * source record via siq_source_record_open().
 * siq_source_record_unlock() should be called when finished with the source
 * record.
 */

void
siq_source_record_write(struct siq_source_record *rec,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_source_record ondisk_rec = {};

	if (siq_source_has_record(rec->_fields.pid)) {
		/* Record exists, make sure the revisions are the same */
		siq_source_record_read(rec->_fields.pid, &ondisk_rec, &error);
		if (error)
			goto out;

		if (ondisk_rec._fields.revision != rec->_fields.revision) {
			error = siq_source_stale_error_new(rec->_fields.pid);
			goto out;
		}
	} else {
		/* Record doesn't exist, make sure we are creating it and not
		 * re-saving a deleted version */
		if (rec->_fields.revision != 0) {
			error = siq_source_stale_error_new(rec->_fields.pid);
			goto out;
		}
	}	

	rec->_fields.revision++;

	_siq_source_record_write_raw(rec, &error);

out:
	if (error)
		log(ERROR, "%s failed: %#{}",
		    __FUNCTION__, isi_error_fmt(error));

	isi_error_handle(error, error_out);
}

void
siq_source_create(const char *pid, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct siq_source_record record = {
		._fields = {
			.revision = 0,
			.unrunnable = false,
			.policy_delete = false,
			.new_snap = INVALID_SNAPID,
			.latest_snap = INVALID_SNAPID,
			.retain_latest_snap = false,
			.manual_snap = INVALID_SNAPID,
			.restore_new_snap = INVALID_SNAPID,
			.restore_latest_snap = INVALID_SNAPID,
			.delete_snap[0] = INVALID_SNAPID,
			.delete_snap[1] = INVALID_SNAPID,
			.reset_pending = false,
			.database_mirrored = false,
			.job_delay_start = 0,
			.last_known_good_time = 0
		},
		._lock_fd = -1,
		._lock_state = 0
	};
	/* Changes corresponding to adding target_guid to source_record.xml
	 * for resync prep srm workflows. In order to populate target_guid
	 * during resync policy creation, removed the forced initialization of
	 * target_guid to 0 in siq_source_force_full_sync routine and moved
	 * the initialization of target_guid here instead.
	 * Tracked with Bug 102116
	 */ 
	memset(record._fields.target_guid, 0,
	    sizeof(record._fields.target_guid));
	memset(&record._fields.composite_job_ver, 0,
	    sizeof(record._fields.composite_job_ver));
	memset(&record._fields.prev_composite_job_ver, 0,
	    sizeof(record._fields.prev_composite_job_ver));
	memset(&record._fields.restore_composite_job_ver, 0,
	    sizeof(record._fields.restore_composite_job_ver));
	memset(&record._fields.restore_prev_composite_job_ver, 0,
	    sizeof(record._fields.restore_prev_composite_job_ver));

	ASSERT(strlen(pid) == POLICY_ID_LEN);
	strcpy(record._fields.pid, pid);

	if (siq_source_has_record(record._fields.pid)) {
		error = siq_source_duplicate_error_new(pid);
		goto out;
	}

	siq_source_record_save(&record, &error);

out:
	isi_error_handle(error, error_out);
}

bool
siq_source_has_record(const char *pid)
{
	char filename[MAXPATHLEN + 1];
	bool result = false;

	snprintf(filename, SREC_PATH_LEN, "%s/%s.xml",
	    SIQ_SOURCE_RECORDS_DIR, pid);

	if (access(filename, F_OK) != -1)
		result = true;

	return result;
}

void
siq_source_delete(const char *pid, struct isi_error **error_out)
{
	int ret = 0;
	char path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;
	struct siq_source_record *rec = NULL;
	
	snprintf(path, SREC_PATH_LEN, "%s/%s.xml",
	    SIQ_SOURCE_RECORDS_DIR, pid);

	if (!siq_source_has_record(pid)) {
		error = isi_system_error_new(ENOENT, "Cannot delete source "
		    "record: %s: Source record does not exist.", path);
		goto out;
	}

	rec = siq_source_record_new();

	siq_source_record_lock(rec, pid, O_EXLOCK, &error);
	if (error)
		goto out;

	/* Remove source record */
	ret = unlink(path);
	if (ret == -1) {
		error = isi_system_error_new(errno, "Error deleting source "
		    "record: %s (%s)", path, strerror(errno));
		siq_source_record_unlock(rec);
		goto out;
	}
	siq_source_record_unlock(rec);

	/* Remove lock file */
	snprintf(path, LOCK_PATH_LEN, "%s/.%s.lock",
	    SIQ_SOURCE_RECORDS_LOCK_DIR, pid);
	ret = unlink(path);
	if (ret == -1)
		error = isi_system_error_new(errno, "Error deleting source "
		    "record lock: %s (%s)", path, strerror(errno));

	/* Remove the backup <polid>.xml~ file if it exists */
	snprintf(path, SREC_PATH_LEN + 1, "%s/%s.xml~",
	    SIQ_SOURCE_RECORDS_DIR, pid);
	ret = unlink(path);

out:
	if (rec) {
		siq_source_record_free(rec);
	}

	isi_error_handle(error, error_out);
}

// For straight-across value assignment. 
#define SIQ_SOURCE_SETVAL_DEFINE(name, field, type)			\
void name(struct siq_source_record *record, type value)			\
{									\
	record->_fields.field = value; 				\
}									

static bool
should_del_snapshot(char *caller_pid, ifs_snapid_t snapid,
		struct isi_error **error_out)
{
	struct siq_source_record *rec = NULL;
	DIR *dir = NULL;
	struct dirent *entry;
	char polid[MAXPATHLEN + 1];
	struct isi_error *error = NULL;
	bool result = true;

	/* Open the source records directory */
	dir = opendir(SIQ_SOURCE_RECORDS_DIR);
	if (dir == NULL) {
		error = isi_system_error_new(errno,
		    "Error opening source records dir: %s",
		    SIQ_SOURCE_RECORDS_DIR);
		goto out;
	}

	for (entry = readdir(dir); entry != NULL; entry = readdir(dir)) {
		/* Make sure it looks like a source record -
		 * <POLID>.xml */
		if (strlen(entry->d_name) != POLICY_ID_LEN + 4)
			continue;

		strncpy(polid, entry->d_name, POLICY_ID_LEN);
		polid[POLICY_ID_LEN] = '\0';

		/* skip the callers source record */
		if (!strncmp(polid, caller_pid, POLICY_ID_LEN))
			continue;

		if (rec) {
			siq_source_record_free(rec);
			rec = NULL;
		}

		rec = siq_source_record_load(polid, &error);
		if (error) {
			/* Ignore non-existence errors, records can be
			 * deleted as we iterate */
			isi_error_free(error);
			error = NULL;
			continue;
		}

		if (rec->_fields.latest_snap == snapid ||
		    rec->_fields.new_snap == snapid) {
			result = false;
			goto out;
		}
	}


out:
	if (rec) {
		siq_source_record_free(rec);
		rec = NULL;
	}

	if (dir != NULL)
		closedir(dir);

	isi_error_handle(error, error_out);
	return result;
}

bool
siq_source_should_del_latest_snap(struct siq_source_record *rec,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool result = false;

	if (rec->_fields.retain_latest_snap)
		return false;

	result = should_del_snapshot(rec->_fields.pid, rec->_fields.latest_snap,
			&error);

	isi_error_handle(error, error_out);
	return result;
}

bool
siq_source_should_del_new_snap(struct siq_source_record *rec,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool result = false;

	result = should_del_snapshot(rec->_fields.pid, rec->_fields.new_snap,
			&error);

	isi_error_handle(error, error_out);
	return result;
}

void siq_source_detach_latest_snap(struct siq_source_record *record)
{
	record->_fields.latest_snap = INVALID_SNAPID;
	record->_fields.retain_latest_snap = false;
}

void siq_source_finish_job(struct siq_source_record *record,
    struct isi_error **error_out)
{
	bool delete_latest = false;
	struct isi_error *error = NULL;

	ASSERT(record->_fields.delete_snap[0] == INVALID_SNAPID);
	/* Don't delete the snap if it is being referenced by another record,
	 * or if SyncIQ didn't create it */

	if (!record->_fields.retain_latest_snap &&
	    siq_source_should_del_latest_snap(record, &error))
		delete_latest = true;

	if (error)
		goto out;

	if (delete_latest)
		record->_fields.delete_snap[0] = record->_fields.latest_snap;
	if (record->_fields.manual_snap != INVALID_SNAPID) {
		record->_fields.latest_snap = record->_fields.manual_snap;
		record->_fields.retain_latest_snap = true;
	} else {
		record->_fields.latest_snap = record->_fields.new_snap;
		record->_fields.retain_latest_snap = false;
	}
	record->_fields.new_snap = INVALID_SNAPID;
	record->_fields.manual_snap = INVALID_SNAPID;

out:
	isi_error_handle(error, error_out);
}

void siq_source_finish_assess(struct siq_source_record *record,
    struct isi_error **error_out)
{
	bool delete_snap = false;
	struct isi_error *error = NULL;

	ASSERT(record->_fields.delete_snap[0] == INVALID_SNAPID);
	/* Don't delete the snap if it is being referenced by another record */

	delete_snap = siq_source_should_del_new_snap(record, &error);
	if (error)
		goto out;

	if (delete_snap)
		record->_fields.delete_snap[0] = record->_fields.new_snap;
	record->_fields.new_snap = INVALID_SNAPID;

out:
	isi_error_handle(error, error_out);
}

void siq_source_force_full_sync(struct siq_source_record *record,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool del_latest = false, del_new = false, res = false;
	char cpss_mirror_name[MAXNAMLEN];

	/* 
	 * If a _new_snap is actually set, then the coordinator would have
	 * to have to have cleaned out any _delete_snap snapshots before
	 * taking the _new_snap.
	 *
	 * The latest_snap is put into the _delete_snap[1] to avoid conflicting
	 * with an actual snap in _delete_snap[0] from the last job run that
	 * might not have been cleaned out.
	 *
	 * Snapshots that weren't taken by SyncIQ are flagged with
	 * _retain_xxx_snap and must not be deleted.
	 */

	if (record->_fields.latest_snap != INVALID_SNAPID &&
	    !record->_fields.retain_latest_snap) {
		ASSERT(record->_fields.delete_snap[1] == INVALID_SNAPID);
		res = siq_source_should_del_latest_snap(record, &error);
		if (error)
			goto out;
		if (res)
			del_latest = true;
	}

	if (record->_fields.new_snap != INVALID_SNAPID) {
		ASSERT(record->_fields.delete_snap[0] == INVALID_SNAPID);
		res = siq_source_should_del_new_snap(record, &error);
		if (error)
			goto out;
		if (res)
			del_new = true;
	}

	if (del_latest)
		record->_fields.delete_snap[1] = record->_fields.latest_snap;

	if (del_new)
		record->_fields.delete_snap[0] = record->_fields.new_snap;

	get_mirrored_cpss_name(cpss_mirror_name, record->_fields.pid);
	log(DEBUG, "Removing cpss %s on policy %s.", record->_fields.pid,
		cpss_mirror_name);
	remove_cpss(record->_fields.pid, cpss_mirror_name, true, &error);
	if (error)
		goto out;

	record->_fields.failback_state = SIQ_FBS_NONE;
	record->_fields.reset_pending = true;
	record->_fields.database_mirrored = false;
	record->_fields.unrunnable = false;
	record->_fields.latest_snap = INVALID_SNAPID;
	record->_fields.retain_latest_snap = false;
	record->_fields.new_snap = INVALID_SNAPID;
	record->_fields.manual_snap = INVALID_SNAPID;
	memset(record->_fields.target_guid, 0, sizeof(record->_fields.target_guid));
	memset(&record->_fields.composite_job_ver, 0,
	    sizeof(record->_fields.composite_job_ver));
	memset(&record->_fields.prev_composite_job_ver, 0,
	    sizeof(record->_fields.prev_composite_job_ver));

out:
	isi_error_handle(error, error_out);
}

void siq_source_clear_reset_pending(struct siq_source_record *record)
{
	record->_fields.reset_pending = false;
}

void siq_source_set_database_mirrored(struct siq_source_record *record)
{
	record->_fields.database_mirrored = true;
}

void siq_source_clear_database_mirrored(struct siq_source_record *record)
{
	record->_fields.database_mirrored = false;
}

void siq_source_set_policy_delete(struct siq_source_record *record)
{
	record->_fields.policy_delete = true;
}

void siq_source_set_unrunnable(struct siq_source_record *record)
{
	record->_fields.unrunnable = true;
}

void siq_source_set_runnable(struct siq_source_record *record)
{
	record->_fields.unrunnable = false;
}

void siq_source_set_pol_stf_ready(struct siq_source_record *record)
{
	record->_fields.pol_stf_ready = true;
}

void siq_source_clear_pol_stf_ready(struct siq_source_record *record)
{
	record->_fields.pol_stf_ready = false;
}

static void
check_and_invalidate_snapshot(ifs_snapid_t *snapid)
{
	SNAP *snap;

	/* Check to see if the snapshot still exists. */
	snap = snapshot_open_by_sid(*snapid, NULL);
	if (snap != NULL) {
		struct isi_str *name = snapshot_get_name(snap);

		snapshot_close(snap);

		/* An existing means it hasn't been deleted. */
		if (name) {
			isi_str_free(name);
			return;
		}
	}

	/* If it does not, then it's safe to forget about it. */
	*snapid = INVALID_SNAPID;
}

void siq_source_clear_delete_snaps(struct siq_source_record *record)
{
	check_and_invalidate_snapshot(&record->_fields.delete_snap[0]);
	check_and_invalidate_snapshot(&record->_fields.delete_snap[1]);
}

void
siq_source_set_manual_snap(struct siq_source_record *record,
    ifs_snapid_t man_snap)
{
	ASSERT(record->_fields.new_snap == INVALID_SNAPID);
	record->_fields.manual_snap = man_snap;
}

void
siq_source_add_restore_new_snap(struct siq_source_record *record,
    ifs_snapid_t new_snap)
{
	ASSERT(record->_fields.restore_new_snap == INVALID_SNAPID);
	record->_fields.restore_new_snap = new_snap;
}

void
siq_source_set_target_guid(struct siq_source_record *record,
    const char *target_guid)
{
	siq_source_copy_guid(record->_fields.target_guid, target_guid);
}

SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_pending_job, pending_job, 
    const enum siq_job_action);
SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_failback_state, failback_state,
    const enum siq_failback_state);
SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_latest, latest_snap,
    ifs_snapid_t);
SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_new_snap, new_snap,
    ifs_snapid_t);
SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_restore_latest, restore_latest_snap,
    ifs_snapid_t);
SIQ_SOURCE_SETVAL_DEFINE(siq_source_set_manual_failback_prep,
    manual_failback_prep, bool);

void
siq_source_set_sched_times(struct siq_source_record *record,
    time_t start, time_t end)
{
	record->_fields.start_schedule = start;
	record->_fields.end_schedule = end;
}

void siq_source_take_ownership(struct siq_source_record *record)
{
	ASSERT(record->_fields.current_owner == SIQ_OWNER_NONE);
	record->_fields.current_owner = SIQ_OWNER_COORD;
}

void siq_source_release_ownership(struct siq_source_record *record)
{
	ASSERT(record->_fields.current_owner == SIQ_OWNER_COORD);
	record->_fields.current_owner = SIQ_OWNER_NONE;
}

void siq_source_clear_restore_latest(struct siq_source_record *record)
{
	record->_fields.restore_latest_snap = INVALID_SNAPID;
}

void siq_source_clear_restore_new(struct siq_source_record *record)
{
	record->_fields.restore_new_snap = INVALID_SNAPID;
}

void
siq_source_set_job_delay_start(struct siq_source_record *record,
    time_t delay_start)
{
	record->_fields.job_delay_start = delay_start;
}

void
siq_source_clear_job_delay_start(struct siq_source_record *record)
{
	record->_fields.job_delay_start = 0;
}

void
siq_source_set_last_known_good_time(struct siq_source_record *record,
    time_t lkg)
{
	record->_fields.last_known_good_time = lkg;
}

#define SIQ_SOURCE_ACCESSOR_DEFINE_BASE(name, type)			\
static void name##_inner(struct siq_source_record *, type);		\
void name(struct siq_source_record *record, type value)			\
{									\
	name##_inner(record, value);					\
}									\
static void name##_inner(struct siq_source_record *record, type value)

SIQ_SOURCE_ACCESSOR_DEFINE_BASE(siq_source_get_target_guid, char *)
{
	strncpy(value, record->_fields.target_guid, SIQ_MAX_TARGET_GUID_LEN);
	value[SIQ_MAX_TARGET_GUID_LEN - 1] = 0;
}

SIQ_SOURCE_ACCESSOR_DEFINE_BASE(siq_source_get_next_action,
    enum siq_policy_action *)
{
	if (record->_fields.latest_snap != INVALID_SNAPID)
		*value = SIQ_POLICY_INCREMENTAL_SYNC;
	else
		*value = SIQ_POLICY_FULL_SYNC;
}

#define SIQ_SOURCE_ACCESSOR_DEFINE(name, type, element)			\
SIQ_SOURCE_ACCESSOR_DEFINE_BASE(name, type *)				\
{									\
	*value = record->_fields.element;				\
}

SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_new, ifs_snapid_t, new_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_latest, ifs_snapid_t, latest_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_manual, ifs_snapid_t, manual_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_restore_new, ifs_snapid_t,
    restore_new_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_restore_latest, ifs_snapid_t,
    restore_latest_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_delete, ifs_snapid_t*, delete_snap);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_pending_job, enum siq_job_action,
    pending_job);
SIQ_SOURCE_ACCESSOR_DEFINE(siq_source_get_failback_state, 
    enum siq_failback_state, failback_state);

bool siq_source_is_policy_deleted(struct siq_source_record *record)
{
	return record->_fields.policy_delete;
}

bool siq_source_has_associated(struct siq_source_record *record)
{
	return strcmp(record->_fields.target_guid, "") != 0;
}

bool siq_source_has_owner(struct siq_source_record *record)
{
	return record->_fields.current_owner != SIQ_OWNER_NONE;
}

bool siq_source_is_unrunnable(struct siq_source_record *record)
{
	return record->_fields.unrunnable;
}

bool siq_source_should_run(struct siq_source_record *record)
{
	return !record->_fields.unrunnable || record->_fields.policy_delete;
}

bool siq_source_pol_stf_ready(struct siq_source_record *record)
{
	return record->_fields.pol_stf_ready;
}

bool siq_source_get_reset_pending(struct siq_source_record *record)
{
	return record->_fields.reset_pending;
}

bool siq_source_get_database_mirrored(struct siq_source_record *record)
{
	return record->_fields.database_mirrored;
}

bool siq_source_retain_latest(struct siq_source_record *record)
{
	return record->_fields.retain_latest_snap;
}

bool siq_source_manual_failback_prep(struct siq_source_record *record)
{
	return record->_fields.manual_failback_prep;
}

void
siq_source_set_merge_snaps(struct siq_source_record *record1,  
    struct siq_source_record *record2, ifs_snapid_t new_snap)
{
	record1->_fields.new_snap = new_snap;
	record2->_fields.new_snap = new_snap;
}

void
siq_source_get_sched_times(struct siq_source_record *record,
    time_t *start, time_t *end)
{
	if (start != NULL)
		*start = record->_fields.start_schedule;
	if (end != NULL)
		*end = record->_fields.end_schedule;
}

void
siq_source_set_node_conn_failed(struct siq_source_record *record, int node_id)
{
	ASSERT(node_id > 0);
	ASSERT(node_id <= SIQ_MAX_NODES);

	record->_fields.conn_fail_nodes[node_id - 1] = 'F';
}

void
siq_source_clear_node_conn_failed(struct siq_source_record *record)
{
	memset(record->_fields.conn_fail_nodes, '.', SIQ_MAX_NODES);
}

bool
siq_source_get_node_conn_failed(struct siq_source_record *record, int node_id)
{
	ASSERT(node_id > 0);
	ASSERT(node_id <= SIQ_MAX_NODES);

	return (record->_fields.conn_fail_nodes[node_id - 1] == 'F');
}

void
siq_source_get_job_delay_start(struct siq_source_record *record,
    time_t *delay_start)
{
	*delay_start = record->_fields.job_delay_start;
}

void
siq_source_get_used_snapids(struct snapid_set *sid_set,
    struct isi_error **error_out)
{
	struct siq_source_record *rec = NULL;
	DIR *dir = NULL;
	struct dirent *entry;
	char polid[MAXPATHLEN + 1];
	int err = -1;
	struct isi_error *error = NULL;

	ASSERT(sid_set != NULL);

	/* Open the source records directory */
	dir = opendir(SIQ_SOURCE_RECORDS_DIR);
	if (dir == NULL) {
		error = isi_system_error_new(errno,
		    "Error opening source records dir: %s",
		    SIQ_SOURCE_RECORDS_DIR);
		goto out;
	}

	for (entry = readdir(dir); entry != NULL; entry = readdir(dir)) {
		/* Make sure it looks like a source record -
		 * <POLID>.xml */
		if (strlen(entry->d_name) != POLICY_ID_LEN + 4)
			continue;

		strncpy(polid, entry->d_name, POLICY_ID_LEN);
		polid[POLICY_ID_LEN] = '\0';

		if (rec) {
			siq_source_record_free(rec);
			rec = NULL;
		}

		rec = siq_source_record_load(polid, &error);
		if (error) {
			/* Ignore non-existence errors, records can be
			 * deleted as we iterate */
			err = isi_system_error_get_error(
			    (const struct isi_system_error *)error);
			if (err == ENOENT) {
				isi_error_free(error);
				error = NULL;
				continue;
			}
			goto out;
		}

		snapid_set_add(sid_set, rec->_fields.latest_snap);
		snapid_set_add(sid_set, rec->_fields.new_snap);
		snapid_set_add(sid_set, rec->_fields.manual_snap);
	}


out:
	if (rec) {
		siq_source_record_free(rec);
		rec = NULL;
	}

	if (dir != NULL)
		closedir(dir);

	isi_error_handle(error, error_out);

}

void
siq_source_get_last_known_good_time(struct siq_source_record *record,
    time_t *lkg)
{
	*lkg = record->_fields.last_known_good_time;
}

void
siq_source_clear_composite_job_ver(struct siq_source_record *record)
{
	memset(&record->_fields.composite_job_ver, 0,
	    sizeof(record->_fields.composite_job_ver));
}

void
siq_source_get_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(job_ver, &record->_fields.composite_job_ver, sizeof(*job_ver));
}

void
siq_source_set_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(&record->_fields.composite_job_ver, job_ver, sizeof(*job_ver));
}

void
siq_source_get_prev_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(job_ver, &record->_fields.prev_composite_job_ver,
	    sizeof(*job_ver));
}

void
siq_source_set_prev_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(&record->_fields.prev_composite_job_ver, job_ver,
	    sizeof(*job_ver));
}

void
siq_source_clear_restore_composite_job_ver(struct siq_source_record *record)
{
	memset(&record->_fields.restore_composite_job_ver, 0,
	    sizeof(record->_fields.restore_composite_job_ver));
}

void
siq_source_get_restore_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(job_ver, &record->_fields.restore_composite_job_ver,
	    sizeof(*job_ver));
}

void
siq_source_set_restore_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(&record->_fields.restore_composite_job_ver, job_ver,
	    sizeof(*job_ver));
}

void
siq_source_get_restore_prev_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(job_ver, &record->_fields.restore_prev_composite_job_ver,
	    sizeof(*job_ver));
}

void
siq_source_set_restore_prev_composite_job_ver(struct siq_source_record *record,
    struct siq_job_version *job_ver)
{
	memcpy(&record->_fields.restore_prev_composite_job_ver, job_ver,
	    sizeof(*job_ver));
}
