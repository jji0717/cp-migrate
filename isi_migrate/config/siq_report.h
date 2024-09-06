#ifndef __SIQ_REPORT_H__
#define __SIQ_REPORT_H__

#include <stdbool.h>
#include <sqlite3.h>

#include "../sched/siq_log.h"

__BEGIN_DECLS

#define SIQ_REPORT_FILE_NAME_MAX	64
#define SIQ_REPORT_UPGRADE_SCRIPT "/usr/libdata/patchmgr/siq-report-upgrade"

typedef struct _siq_report_file_t {
	char name[SIQ_REPORT_FILE_NAME_MAX + 1];
} siq_report_file_t;

#define SIQ_DEFAULT_MAX_REPORTS	2000

/* Report file names are "prefix-timestamp.xml". However, error reports from
 * 5.0 and delete files do not end with ".xml". */
__inline static time_t
get_report_time(const char *report)
{
	time_t t;
	int i;

	for (i = strlen(report) - 1; i >= 0; i--) {
		if (report[i] == '-')
			break;
	}
	ASSERT(i > 0, "Missing '-' in report name '%s'", report);

	t = strtoul(report + i + 1, NULL, 10);
	ASSERT(t != 0 && (unsigned long)t != ULONG_MAX,
	    "From '%s', strtoul('%s') failed: %s",
	    report, report + i + 1, strerror(errno));
	return t;
 }

__inline static int
siq_report_name_cmp(const siq_report_file_t *x, const siq_report_file_t *y)
{
	return get_report_time(x->name) - get_report_time(y->name);
}

/* Note that the "filename" column in the "reports" table now actually stores 
 * lins and full paths to .xml files.  This is because of the lack of sqlite 
 * support for renaming columns.  Also, filenames that are .xml are are pre-7.0 
 * reports that have not yet been upgraded. */
#define REPORTS_TABLE "reports"
#define REPORTS_CREATE_SQL \
"CREATE TABLE IF NOT EXISTS reports (\
    filename TEXT PRIMARY KEY,\
    policy_id TEXT,\
    job_id TEXT,\
    timestamp INTEGER,\
    dirty_bit INTEGER);"

#define REPORTS_CREATE_INDICIES_SQL \
"CREATE INDEX IF NOT EXISTS index1 ON reports(filename); \
 CREATE INDEX IF NOT EXISTS index2 ON reports(policy_id); \
 CREATE INDEX IF NOT EXISTS index3 ON reports(job_id); \
 CREATE INDEX IF NOT EXISTS index4 ON reports(timestamp); \
 CREATE INDEX IF NOT EXISTS index5 ON reports(dirty_bit); \
 CREATE INDEX IF NOT EXISTS index6 ON \
     reports(filename, policy_id, job_id, timestamp, dirty_bit); \
 CREATE INDEX IF NOT EXISTS index7 ON reports(policy_id, job_id); \
 CREATE INDEX IF NOT EXISTS index8 ON reports(policy_id, timestamp); \
 CREATE INDEX IF NOT EXISTS index9 ON reports(policy_id, job_id, timestamp);"

#define REPORTS_INSERT_SQL \
    "INSERT OR REPLACE INTO reports(filename, policy_id, job_id, timestamp, \
     dirty_bit) VALUES('%s', '%s', '%s', %d, 0);"

#define REPORTS_UPGRADE_LIN_SQL \
    "UPDATE reports SET filename = '%s' WHERE filename = '%s';"

#define REPORTS_DELETE_FILE_SQL \
    "DELETE FROM reports WHERE filename = '%s';"

#define REPORTS_DELETE_PID_SQL \
    "DELETE FROM reports WHERE policy_id = '%s';"

#define REPORTS_DELETE_DIRTY_BIT_SQL \
    "DELETE FROM reports WHERE dirty_bit = 1;"

#define REPORTS_SELECT_NUM_COLS 5

#define REPORTS_SELECT_PID_JID_NUM_COLS 2

#define REPORTS_SELECT_PID_SQL \
    "SELECT filename, policy_id, job_id, timestamp, dirty_bit \
     FROM reports WHERE policy_id = '%s' \
     ORDER BY timestamp ASC;"

#define REPORTS_SELECT_PID_JID_SQL \
    "SELECT filename, policy_id, job_id, timestamp, dirty_bit \
     FROM reports WHERE policy_id = '%s' AND job_id = '%s' \
     ORDER BY timestamp ASC;"

#define REPORTS_SELECT_PID_DIRTY_BIT_SQL \
    "SELECT filename, policy_id, job_id, timestamp, dirty_bit \
     FROM reports WHERE policy_id = '%s' AND dirty_bit = %d;"

#define REPORTS_SELECT_LATEST_PID_JID_LIST_SQL \
    "SELECT DISTINCT policy_id, job_id FROM reports \
     ORDER BY timestamp DESC LIMIT %d;"

#define REPORTS_SELECT_LATEST_PID_JID_LIST_BY_PID_SQL \
    "SELECT DISTINCT policy_id, job_id FROM reports WHERE policy_id = '%s'\
     ORDER BY timestamp DESC LIMIT %d;"

#define REPORTS_SELECT_ALL_PID_JID_LIST_BY_PID_SQL \
    "SELECT DISTINCT policy_id, job_id FROM reports WHERE policy_id = '%s' \
     ORDER BY timestamp DESC;"

#define REPORTS_SELECT_LATEST_ALL_PID_JID_LIST_SQL \
    "SELECT DISTINCT policy_id, job_id FROM reports \
    ORDER BY timestamp DESC LIMIT %d;"

#define REPORTS_SELECT_DATERANGE_PID_JID_LIST_SQL \
    "SELECT policy_id, job_id \
    FROM (SELECT policy_id, job_id, timestamp \
          FROM reports \
          GROUP BY policy_id, job_id HAVING MAX(timestamp)) AS reports \
    WHERE reports.timestamp >= %d AND reports.timestamp <= %d;"

#define REPORTS_SELECT_DATERANGE_PID_JID_LIST_BY_PID_SQL \
    "SELECT policy_id, job_id \
    FROM (SELECT policy_id, job_id, timestamp \
          FROM reports WHERE policy_id = '%s' \
          GROUP BY policy_id, job_id HAVING MAX(timestamp)) AS reports \
    WHERE reports.timestamp >= %d AND reports.timestamp <= %d;"

#define REPORTS_SELECT_COUNT_SQL \
    "SELECT count(*) FROM (SELECT DISTINCT policy_id, job_id FROM reports);"

#define REPORTS_SELECT_COUNT_BY_PID_SQL \
    "SELECT count(*) FROM (SELECT DISTINCT job_id FROM reports \
     WHERE policy_id = '%s');"

#define REPORTS_SELECT_COUNT_BY_LEGACY_JID_SQL \
    "SELECT count(*) FROM reports WHERE job_id LIKE '%%LEGACY-%%';"

#define REPORTS_SELECT_COUNT_BY_LIN_SQL \
    "SELECT count(*) FROM reports WHERE filename = '%s';"

#define REPORTS_SELECT_COUNT_BY_PID_JID_DIRTY_BIT_SQL \
    "SELECT count(*) FROM reports \
     WHERE policy_id = '%s' AND job_id = '%s' AND dirty_bit = %d;"

#define REPORTS_UPDATE_DIRTY_BIT_SQL \
    "UPDATE reports SET dirty_bit = %d;"

#define REPORTS_UPDATE_DIRTY_BIT_BY_LIN_SQL \
    "UPDATE reports SET dirty_bit = %d WHERE filename = '%s';"

#define REPORTS_UPDATE_DIRTY_BIT_BY_PID_AND_TIME_SQL \
    "UPDATE reports SET dirty_bit = %d WHERE policy_id = '%s' AND \
     timestamp <= %d;"

#define REPORTS_UPDATE_DIRTY_BIT_BY_PID_MAX_NUM_SQL \
    "UPDATE reports SET dirty_bit = %d WHERE policy_id = '%s' AND job_id NOT IN \
         (SELECT job_id FROM ( \
             SELECT policy_id, job_id, timestamp FROM reports \
             WHERE policy_id = '%s' GROUP BY policy_id, job_id \
             HAVING MAX(timestamp) ORDER BY timestamp DESC LIMIT %d));"

#define REPORTS_STATUS_TABLE "reports_status"

#define REPORTS_STATUS_SELECT_NUM_COLS 2

#define REPORTS_STATUS_CREATE_SQL \
    "CREATE TABLE IF NOT EXISTS reports_status ("\
    "policy_id TEXT PRIMARY KEY," \
    "job_id INTEGER, " \
    "inc_job_id INTEGER);"

#define REPORTS_STATUS_CREATE_INDICIES_SQL \
    "CREATE INDEX IF NOT EXISTS reports_status_index1 " \
    "ON reports_status(policy_id);"

#define REPORTS_STATUS_DELETE_PID_SQL \
    "DELETE FROM reports_status WHERE policy_id = '%s';"

#define REPORTS_STATUS_UPDATE_SQL \
    "INSERT OR REPLACE INTO reports_status(policy_id, job_id, inc_job_id) " \
    "VALUES ('%s', %d, %d);"

#define REPORTS_STATUS_SELECT_SQL \
    "SELECT job_id, inc_job_id FROM reports_status WHERE policy_id = '%s';"

struct reportdb_record {
	char *lin;
	char *policy_id;
	char *job_id;
	int timestamp;
	int dirty_bit;
};

struct reportdb_pid_jid_record {
	char *policy_id;
	char *job_id;	
};

enum report_type {
    FILE_REPORT, 
    FILE_DELETED, 
    FILE_ERROR, 
    FILE_NONE, 
    FILE_SKIP, 
    FILE_LEGACY
};

enum report_type get_report_type(char *file);

sqlite3 *siq_reportdb_open_config(char *path, struct siq_gc_conf *conf_in, 
    struct isi_error **error_out);
sqlite3* siq_reportdb_open(char *path, struct isi_error **error_out);
void siq_reportdb_close(sqlite3 *db);

void siq_reportdb_insert(sqlite3 *db, char *lin, char *policy_id, 
    char *job_id, int timestamp, struct isi_error **error_out);
    
void siq_reportdb_delete_by_lin(sqlite3 *db, char *lin, 
    struct isi_error **error_out);
void siq_reportdb_delete_by_policyid(sqlite3 *db, char *policy_id, 
    struct isi_error **error_out);
void siq_reportdb_delete_by_dirty_bit(sqlite3 *db, struct isi_error **error_out);
    
void siq_reportdb_pid_jid_record_free(struct reportdb_pid_jid_record *record);
void siq_reportdb_pid_jid_records_free(struct reportdb_pid_jid_record **records);

int siq_reportdb_get_latest_reports(sqlite3 *db, int max_num,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out);
int siq_reportdb_get_latest_reports_by_policy_id(sqlite3 *db, char *policy_id, 
    int max_num, struct reportdb_pid_jid_record ***records_out, 
    struct isi_error **error_out);
    
int siq_reportdb_get_all_reports_by_policy_id(sqlite3 *db, char *policy_id,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out);
int siq_reportdb_get_all_latest_reports(sqlite3 *db, int max_num, 
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out);
int siq_reportdb_get_daterange_reports(sqlite3 *db, int start, int end,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out);
int siq_reportdb_get_daterange_reports_by_policy_id(sqlite3 *db, 
    char *policy_id, int start, int end, 
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out);
int siq_reportdb_get_by_policyid(sqlite3 *db, 
    struct reportdb_record ***records_out, char *policy_id, 
    struct isi_error **error_out);
int siq_reportdb_get_by_policyid_jobid(sqlite3 *db, 
    struct reportdb_record ***records_out, char *policy_id, char *job_id, 
    struct isi_error **error_out);
int siq_reportdb_get_by_policyid_dirtybit(sqlite3 *db, 
    struct reportdb_record ***records_out, char *policy_id, int dirty_bit, 
    struct isi_error **error_out);

void siq_reportdb_update_dirty_bit(sqlite3 *db, int dirty_bit, 
    struct isi_error **error_out);
void siq_reportdb_update_dirty_bit_by_lin(sqlite3 *db, int dirty_bit, 
    char *lin, struct isi_error **error_out);
void siq_reportdb_update_dirty_bit_by_policy_id_and_time(sqlite3 *db, 
    int dirty_bit, char *policy_id, int oldest, struct isi_error **error_out);
void siq_reportdb_update_dirty_bit_by_policy_id_and_max_num(sqlite3 *db, 
    int dirty_bit, char *policy_id, int max, struct isi_error **error_out);
    
void siq_reportdb_record_free(struct reportdb_record *record);
void siq_reportdb_records_free(struct reportdb_record **records);

void siq_reportdb_rebuild(struct isi_error **error_out);
void siq_reportdb_rebuild_policy(char *policy_id, struct isi_error **error_out);

void siq_reportdb_get_count(sqlite3 *db, int *num_out, 
    struct isi_error **error_out);
void siq_reportdb_get_count_by_policy_id(sqlite3 *db, char *policy_id, 
    int *num_out, struct isi_error **error_out);
void siq_reportdb_get_count_by_legacy_job_id(sqlite3 *db, int *num_out, 
    struct isi_error **error_out);
void siq_reportdb_get_count_by_lin(sqlite3 *db, char *lin, 
    int *num_out, struct isi_error **error_out);
void siq_reportdb_get_count_by_pid_jid_dirty_bit(sqlite3 *db, 
    char *policy_id, char *job_id, int dirty_bit, int *num_out, 
    struct isi_error **error_out);
    
int siq_reportdb_get_reports_by_job_id(sqlite3 *db, char *policy_id, 
    char *job_id, struct siq_job_summary ***reports_out, 
    struct isi_error **error_out);

int siq_reportdb_load_recent(sqlite3 *db, char *pid, int n,
    struct siq_composite_report ***report, int *reports,
    struct isi_error **error_out);

void siq_composite_report_free(struct siq_composite_report *report);
void siq_composite_reports_free(struct siq_composite_report **reports);
void siq_reportdb_create_composite_report(struct siq_job_summary **reports, 
    int num_reports, struct siq_composite_report **composite_out, 
    struct isi_error **error_out);
int siq_reports_status_check_inc_job_id(const char *policy_id, 
    struct isi_error **error_out);
int siq_reports_status_check_and_set_inc_job_id(const char *policy_id,
    struct isi_error **error_out);
void siq_reports_status_set_inc_job_id(const char *policy_id, 
    struct isi_error **error_out);
void siq_reports_status_delete_by_policyid(sqlite3 *db, char *policy_id,
    struct isi_error **error_out);

__END_DECLS

#endif // __SIQ_REPORT_H__
