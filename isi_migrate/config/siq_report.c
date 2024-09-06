/*
 * @file @siq_report.c
 * SIQ Reporting API
 * (C) 2009 Isilon Systems
 */
#include <dirent.h>

#include <ifs/bam/bam_pctl.h>
#include <isi_util/util.h>

#include "siq_report.h"
#include "siq_policy.h"
#include "siq_job.h"
#include "siq_sqlite.h"

#include "isi_migrate/migr/isirep.h"

static void
siq_reportdb_create_table(sqlite3 *db, struct isi_error **error_out);

static void
siq_reportdb_create_reports_status_table(sqlite3 *db, 
    struct isi_error **error_out);

static void
siq_reportdb_set_reports_status(sqlite3 *db, const char *policy_id, 
    const int job_id, const bool inc_job_id, struct isi_error **error_out);


/*
 * Check whether given input file is part of report logs.
 */
enum report_type
get_report_type(char *file)
{
	if (strcmp(file, ".") == 0 || strcmp(file, "..") == 0)
		return FILE_SKIP;

	/* Any file name that is too long cannot be a valid report file */
	if (strlen(file) > SIQ_REPORT_FILE_NAME_MAX)
		return FILE_NONE;
	if (ends_with(file, ".xml"))
		return FILE_LEGACY;
	if (ends_with(file, ".lck"))
		return FILE_SKIP;
	if (starts_with(file, "report-"))
		return FILE_REPORT;
	if (starts_with(file, "deleted-files-"))
		return FILE_DELETED;
	if (starts_with(file, "error-file-"))
		return FILE_ERROR;

	return FILE_NONE;
}

sqlite3 *
siq_reportdb_open_config(char *path, struct siq_gc_conf *conf_in, 
    struct isi_error **error_out)
{
	sqlite3 *db = NULL;
	bool ret = false;
	char *db_path = NULL;
	struct siq_gc_conf *config = NULL;
	struct isi_error *error = NULL;
	
	if (!path) {
		db_path = SIQ_REPORTS_REPORT_DB_PATH;
	} else {
		db_path = path;
	}
	
	siq_sqlite_open(db_path, &db, &error);
	if (error) {
		goto out;
	}
	
	ret = siq_sqlite_verify_table_exists(db, REPORTS_TABLE, &error);
	if (error) {
		goto out;
	}
	
	if (!ret) {
		siq_reportdb_create_table(db, &error);
		if (error) {
			goto out;
		}
	}

	ret = siq_sqlite_verify_table_exists(db, REPORTS_STATUS_TABLE, &error);
	if (error) {
		goto out;
	}
	
	if (!conf_in) {
		config = siq_gc_conf_load(&error);
		if (error) {
			goto out;
		}
		siq_gc_conf_close(config);
	} else {
		config = conf_in;
	}
	siq_sqlite_set_cache_size(db, config->root->reports->sqlite_cache_size, 
	    &error);
	if (error) {
		goto out;
	}
	
	if (config->root->reports->sqlite_unsafe_mode) {
		siq_sqlite_set_unsafe_mode(db, &error);
	}
	if (error) {
		goto out;
	}
	
	if (!ret) {
		siq_reportdb_create_reports_status_table(db, &error);
		if (error) {
			goto out;
		}
	}

out:
	if (config && !conf_in) {
		siq_gc_conf_free(config);
	}
	isi_error_handle(error, error_out);
	return db;
}

sqlite3 *
siq_reportdb_open(char *path, struct isi_error **error_out)
{
	return siq_reportdb_open_config(path, NULL, error_out);
}

void
siq_reportdb_close(sqlite3 *db)
{
	siq_sqlite_close(db);
}

static void
siq_reportdb_create_indicies(sqlite3 *db, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	
	ASSERT(db);
	
	siq_sqlite_exec_retry(db, REPORTS_CREATE_INDICIES_SQL, NULL, NULL, 
	    &error);
	if (error) {
		goto out;
	}
	
out:
	isi_error_handle(error, error_out);
}

static void
siq_reportdb_create_table(sqlite3 *db, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool ret = false;
	
	ASSERT(db);
	
	siq_sqlite_exec_retry(db, REPORTS_CREATE_SQL, NULL, NULL, &error);
	if (error) {
		goto out;
	}
	
	ret = siq_sqlite_verify_table_exists(db, REPORTS_TABLE, &error);
	if (error) {
		goto out;
	}
	
	siq_reportdb_create_indicies(db, &error);
	if (error) {
		goto out;
	}
	
out:
	isi_error_handle(error, error_out);
}


static void
siq_reportdb_create_reports_status_indicies(sqlite3 *db,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(db);

	siq_sqlite_exec_retry(db, REPORTS_STATUS_CREATE_INDICIES_SQL,
	    NULL, NULL, &error);
	if (error) {
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
siq_reportdb_create_reports_status_table(sqlite3 *db, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool ret = false;
	
	ASSERT(db);
	
	siq_sqlite_exec_retry(db, REPORTS_STATUS_CREATE_SQL, NULL, NULL, 
	    &error);
	if (error) {
		goto out;
	}
	
	ret = siq_sqlite_verify_table_exists(db, REPORTS_STATUS_TABLE, &error);
	if (error) {
		goto out;
	}
	siq_reportdb_create_reports_status_indicies(db, &error);
	if (error) {
		goto out;
	}
out:
	isi_error_handle(error, error_out);
}

static void
siq_reportdb_generic_exec(sqlite3 *db, struct isi_error **error_out, 
    char *query_template, ...)
{
	char *query = NULL;
	va_list args;
	
	ASSERT(db);
	
	va_start(args, query_template);
	vasprintf(&query, query_template, args);
	va_end(args);
	
	ASSERT(query);
	
	siq_sqlite_exec_retry(db, query, NULL, NULL, error_out);
	
	free(query);
}

void
siq_reportdb_insert(sqlite3 *db, char *lin, char *policy_id, 
    char *job_id, int timestamp, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_INSERT_SQL, lin,
	    policy_id, job_id, timestamp);
}

void
siq_reportdb_delete_by_lin(sqlite3 *db, char *lin, 
    struct isi_error **error_out)
{
	int exists = 0;
	
	siq_reportdb_get_count_by_lin(db, lin, &exists, error_out);
	if (*error_out) {
		return;
	}
	
	if (exists) {
		siq_reportdb_generic_exec(db, error_out, 
		    REPORTS_DELETE_FILE_SQL, lin);
	}
}

void
siq_reportdb_delete_by_policyid(sqlite3 *db, char *policy_id,
    struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_DELETE_PID_SQL, 
	    policy_id);
}

void
siq_reportdb_delete_by_dirty_bit(sqlite3 *db, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_DELETE_DIRTY_BIT_SQL);
}

void
siq_reportdb_pid_jid_record_free(struct reportdb_pid_jid_record *record)
{
	if (record != NULL) {
		free(record->policy_id);
		record->policy_id = NULL;
		free(record->job_id);
		record->job_id = NULL;
		free(record);
	}
}

void
siq_reportdb_pid_jid_records_free(struct reportdb_pid_jid_record **records)
{
	struct reportdb_pid_jid_record *current;
	int pointer = 0;
	
	if (records == NULL) {
		return;
	}
	
	current = records[pointer];
	while (current != NULL) {
		siq_reportdb_pid_jid_record_free(current);
		records[pointer] = NULL;
		pointer++;
		current = records[pointer];
	}
	
	free(records);
}

static int
siq_reportdb_pid_jid_get(sqlite3 *db, 
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out, 
    char *query_template, ...)
{
	va_list args;
	char *query = NULL;
	char **result = NULL;
	int num_rows = 0;
	int num_cols = 0;
	int result_pointer = 0;
	int record_total = 0;
	int record_pointer = 0;
	int ret = 0;
	struct reportdb_pid_jid_record *current_record = NULL;
	struct reportdb_pid_jid_record **to_return = NULL;
	struct isi_error *error = NULL;
	
	ASSERT(db);
	
	va_start(args, query_template);
	vasprintf(&query, query_template, args);
	va_end(args);
	
	ASSERT(query);
	
	siq_sqlite_query(db, query, &result, &num_rows, &num_cols, &error);
	if (error) {
		goto out;
	} else if (num_rows <= 0) {
		//Return NULL
		goto out;
	}
	ASSERT(num_cols == REPORTS_SELECT_PID_JID_NUM_COLS);
	
	record_total = (num_rows + 1) * num_cols;
	//Allocate an extra array element as a sentinel
	to_return = calloc(num_rows + 1, 
	    sizeof(struct reportdb_pid_jid_record *));
	ASSERT(to_return);
	
	//Iterate, but skip past the column headers
	for (result_pointer = REPORTS_SELECT_PID_JID_NUM_COLS; 
	    result_pointer < record_total;) {
		current_record = calloc(1, 
		    sizeof(struct reportdb_pid_jid_record));
		ASSERT(current_record);
		
		//policy_id
		current_record->policy_id = strdup(result[result_pointer]);
		result_pointer++;
		//job_id
		current_record->job_id = strdup(result[result_pointer]);
		result_pointer++;
		
		to_return[record_pointer] = current_record;
		record_pointer++;
	}

out:
	siq_sqlite_free_query_result(result);
	free(query);
	
	if (error) {
		ret = -1;
		siq_reportdb_pid_jid_records_free(to_return);
	} else {
		ret = num_rows;
		*records_out = to_return;
	}
	
	isi_error_handle(error, error_out);
	return ret;
}

int
siq_reportdb_get_latest_reports(sqlite3 *db, int max_num,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out, 
	    REPORTS_SELECT_LATEST_PID_JID_LIST_SQL, max_num);
}

int
siq_reportdb_get_latest_reports_by_policy_id(sqlite3 *db, char *policy_id, 
    int max_num, struct reportdb_pid_jid_record ***records_out, 
    struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out, 
	    REPORTS_SELECT_LATEST_PID_JID_LIST_BY_PID_SQL, policy_id, max_num);
}

int
siq_reportdb_get_all_reports_by_policy_id(sqlite3 *db, char *policy_id,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out, 
	    REPORTS_SELECT_ALL_PID_JID_LIST_BY_PID_SQL, policy_id);
}

int
siq_reportdb_get_all_latest_reports(sqlite3 *db, int max_num, 
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out, 
	    REPORTS_SELECT_LATEST_ALL_PID_JID_LIST_SQL, max_num);
}

int
siq_reportdb_get_daterange_reports(sqlite3 *db, int start, int end,
    struct reportdb_pid_jid_record ***records_out, struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out,
	    REPORTS_SELECT_DATERANGE_PID_JID_LIST_SQL, start, end);
}

int
siq_reportdb_get_daterange_reports_by_policy_id(sqlite3 *db, char *policy_id,
    int start, int end, struct reportdb_pid_jid_record ***records_out, 
    struct isi_error **error_out)
{
	return siq_reportdb_pid_jid_get(db, records_out, error_out,
	    REPORTS_SELECT_DATERANGE_PID_JID_LIST_BY_PID_SQL, policy_id, 
	    start, end);
}

static int
exec_reports_upgrade(char *file)
{
	char *syscmd = NULL;
	int sys_ret = 0;
	
	//Need to upgrade this file to .gc
	asprintf(&syscmd, "%s %s", SIQ_REPORT_UPGRADE_SCRIPT, file);
	ASSERT(syscmd);
	sys_ret = system(syscmd);
	if (sys_ret != 0 && WEXITSTATUS(sys_ret) != 0) {
		log(ERROR, "%s: Failed to execute reports upgrade script", 
		    __func__);
		return -1;
	}
	FREE_AND_NULL(syscmd);
	
	unlink(file);
	
	return sys_ret;
}

static char *
siq_reportdb_upgrade_or_strdup_reports(sqlite3 *db, char *input, 
    struct isi_error **error_out)
{
	int ret = 0;
	char buffer[MAXPATHLEN + 1];
	struct stat rep_stat;
	char *to_return = NULL;
	struct isi_error *error = NULL;
	
	if (ends_with(input, ".xml")) {
		//This report needs to be upgraded on the fly before we continue
		//Disable SIGINT
		signal(SIGINT, SIG_IGN);
		//Run upgrade script
		ret = exec_reports_upgrade(input);
		if (ret != 0) {
			error = isi_siq_error_new(E_SIQ_SQL_ERR, 
			    "Failed to upgrade report");
			goto out;
		}
		//Stat the new gc file
		memset(buffer, 0, sizeof(buffer));
		strncpy(buffer, input, strlen(input) - 4);
		strcat(buffer, ".gc");
		ret = stat(buffer, &rep_stat);
		if (ret != 0) {
			error = isi_siq_error_new(E_SIQ_SQL_ERR, 
			    "Failed to stat upgraded report");
			goto out;
		}
		//Update db record with new lin
		asprintf(&to_return, "%lld", rep_stat.st_ino);
		ASSERT(to_return);
	
		siq_reportdb_generic_exec(db, &error, 
		    REPORTS_UPGRADE_LIN_SQL, to_return, input);
		if (error) {
			goto out;
		}
	} else {
		to_return = strdup(input);
	}
out:
	//Restore SIGINT
	signal(SIGINT, SIG_DFL);
	isi_error_handle(error, error_out);
	return to_return;
}

static int
siq_reportdb_generic_get(sqlite3 *db, struct reportdb_record ***records_out, 
    struct isi_error **error_out, char *query_template, ...)
{
	va_list args;
	char *query = NULL;
	char **result = NULL;
	int num_rows = 0;
	int num_cols = 0;
	int result_pointer = 0;
	int record_total = 0;
	int record_pointer = 0;
	int ret = 0;
	struct reportdb_record *current_record = NULL;
	struct reportdb_record **to_return = NULL;
	struct isi_error *error = NULL;
	
	ASSERT(db);
	
	va_start(args, query_template);
	vasprintf(&query, query_template, args);
	va_end(args);
	
	ASSERT(query);
	
	siq_sqlite_query(db, query, &result, &num_rows, &num_cols, &error);
	if (error) {
		goto out;
	} else if (num_rows <= 0) {
		//Return NULL
		goto out;
	}
	ASSERT(num_cols == REPORTS_SELECT_NUM_COLS);
	
	record_total = (num_rows + 1) * num_cols;
	//Allocate an extra array element as a sentinel
	to_return = calloc(num_rows + 1, sizeof(struct reportdb_record *));
	ASSERT(to_return);
	
	//Iterate, but skip past the column headers
	for (result_pointer = REPORTS_SELECT_NUM_COLS; 
	    result_pointer < record_total;) {
		current_record = calloc(1, sizeof(struct reportdb_record));
		ASSERT(current_record);
		
		//lin
		current_record->lin = 
		    siq_reportdb_upgrade_or_strdup_reports(
		    db, result[result_pointer], &error);
		if (error) {
			free(current_record);
			goto out;
		}
		result_pointer++;
		//policy_id
		current_record->policy_id = strdup(result[result_pointer]);
		result_pointer++;
		//job_id
		current_record->job_id = strdup(result[result_pointer]);
		result_pointer++;
		//timestamp
		current_record->timestamp = atoi(result[result_pointer]);
		result_pointer++;
		//dirty_bit
		current_record->dirty_bit = atoi(result[result_pointer]);
		result_pointer++;
		
		to_return[record_pointer] = current_record;
		record_pointer++;
	}

out:
	siq_sqlite_free_query_result(result);
	free(query);
	
	if (error) {
		ret = -1;
		siq_reportdb_records_free(to_return);
	} else {
		ret = num_rows;
		*records_out = to_return;
	}
	
	isi_error_handle(error, error_out);
	return ret;
}

int
siq_reportdb_get_by_policyid(sqlite3 *db, struct reportdb_record ***records_out, 
    char *policy_id, struct isi_error **error_out)
{
	return siq_reportdb_generic_get(db, records_out, error_out, 
	    REPORTS_SELECT_PID_SQL, policy_id);
}

int
siq_reportdb_get_by_policyid_jobid(sqlite3 *db, 
    struct reportdb_record ***records_out, char *policy_id, char *job_id, 
    struct isi_error **error_out)
{
	return siq_reportdb_generic_get(db, records_out, error_out, 
	    REPORTS_SELECT_PID_JID_SQL, policy_id, job_id);
}

int
siq_reportdb_get_by_policyid_dirtybit(sqlite3 *db, 
    struct reportdb_record ***records_out, char *policy_id, int dirty_bit, 
    struct isi_error **error_out)
{
	return siq_reportdb_generic_get(db, records_out, error_out, 
	    REPORTS_SELECT_PID_DIRTY_BIT_SQL, policy_id, dirty_bit);
}

void
siq_reportdb_update_dirty_bit(sqlite3 *db, int dirty_bit, 
    struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_UPDATE_DIRTY_BIT_SQL, 
	    dirty_bit);
}

void
siq_reportdb_update_dirty_bit_by_lin(sqlite3 *db, int dirty_bit, 
    char *lin, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, 
	    REPORTS_UPDATE_DIRTY_BIT_BY_LIN_SQL, dirty_bit, lin);
}

void
siq_reportdb_update_dirty_bit_by_policy_id_and_time(sqlite3 *db, int dirty_bit, 
    char *policy_id, int oldest, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, 
	    REPORTS_UPDATE_DIRTY_BIT_BY_PID_AND_TIME_SQL, dirty_bit, policy_id,
	    oldest);
}

void
siq_reportdb_update_dirty_bit_by_policy_id_and_max_num(sqlite3 *db, 
    int dirty_bit, char *policy_id, int max, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, 
	    REPORTS_UPDATE_DIRTY_BIT_BY_PID_MAX_NUM_SQL, dirty_bit, policy_id,
	    policy_id, max);
}

void
siq_reportdb_record_free(struct reportdb_record *record)
{
	if (record != NULL) {
		free(record->lin);
		record->lin = NULL;
		free(record->policy_id);
		record->policy_id = NULL;
		free(record->job_id);
		record->job_id = NULL;
		free(record);
	}
}

void
siq_reportdb_records_free(struct reportdb_record **records)
{
	struct reportdb_record *current;
	int pointer = 0;
	
	if (records == NULL) {
		return;
	}
	
	current = records[pointer];
	while (current != NULL) {
		siq_reportdb_record_free(current);
		records[pointer] = NULL;
		pointer++;
		current = records[pointer];
	}
	
	free(records);
}

static void
siq_reportdb_rebuild_policy_helper(sqlite3 *db, char *path, char *policy_id,
    struct isi_error **error_out)
{
	DIR *dirp = NULL;
	struct dirent *de = NULL;
	struct siq_job_summary *js = NULL;
	enum report_type rp;
	char filepath[MAXPATHLEN + 1];
	char buffer[MAXPATHLEN + 1];
	struct stat rep_stat;
	int ret = 0;
	char lin_buff[20];
	char job_id_buff[20];
	struct isi_error *error = NULL;
	int legacy_count = 1;
	int last_job_id = 1; // Last (highest) job id seen.
	bool last_job_succeeded = false;
	
	dirp = opendir(path);
	errno = 0;
	de = readdir(dirp);
	while (de != NULL) {
		if (de->d_type != DT_REG) {
			errno = 0;
			de = readdir(dirp);
			continue;
		}
		rp = get_report_type(de->d_name);
		if (rp == FILE_REPORT || rp == FILE_LEGACY) {
			memset(filepath, 0, sizeof(filepath));
			snprintf(filepath, sizeof(filepath), "%s/%s", path, 
			    de->d_name);
			if (rp == FILE_LEGACY) {
				//Need to upgrade this file to .gc
				ret = exec_reports_upgrade(filepath);
				if (ret != 0) {
					log(ERROR, 
					    "%s: Failed to upgrade report %s", 
					    __func__, filepath);
					goto out;
				}
				//Stat the new gc file
				memset(buffer, 0, sizeof(buffer));
				strncpy(buffer, filepath, strlen(filepath) - 4);
				strcat(buffer, ".gc");
				ret = stat(buffer, &rep_stat);
				if (ret != 0) {
					error = isi_siq_error_new(E_SIQ_SQL_ERR, 
					    "Failed to stat upgraded report");
					goto out;
				}
				snprintf(lin_buff, sizeof(lin_buff), "%lld", 
				    rep_stat.st_ino);
			} else {
				snprintf(lin_buff, sizeof(lin_buff), "%lld", 
				    de->d_fileno);
			}
			js = siq_job_summary_load_readonly(filepath, &error);
			if (error) {
				log(ERROR, "%s: Failed open report %s", 
				    __func__, filepath);
				goto out;
			}
			if (js->job_id == 0) {
				snprintf(job_id_buff, sizeof(job_id_buff), 
				    "LEGACY-%d", legacy_count);
				legacy_count++;
			} else {
				snprintf(job_id_buff, sizeof(job_id_buff), 
				    "%d", js->job_id);
			}

			if (js->job_id > last_job_id) {
				last_job_id = js->job_id;
				last_job_succeeded = false;
			}

			if (js->total->state == SIQ_JS_SUCCESS ||
			    js->total->state == SIQ_JS_SUCCESS_ATTN) {
				if (js->job_id == last_job_id)	
					last_job_succeeded = true;
			}

			siq_reportdb_insert(db, lin_buff, policy_id, 
			    job_id_buff, js->total->end, &error);
			siq_job_summary_free(js);
			if (error) {
				log(ERROR, "%s: "
				    "Failed to insert report %s into db", 
				    __func__, filepath);
				goto out;
			}
		}
		//Iterate
		errno = 0;
		de = readdir(dirp);
	}
	
	if (errno != 0) {
		log(ERROR, "%s: encountered error in readdir (%d)", 
		    __func__, errno);
	}

	siq_reportdb_set_reports_status(db, policy_id, last_job_id, 
	    last_job_succeeded, &error);
	if (error)
		goto out;

out:
	closedir(dirp);	
	isi_error_handle(error, error_out);
}

void
siq_reportdb_rebuild_policy(char *policy_id, struct isi_error **error_out)
{
	sqlite3 *db = NULL;
	struct isi_error *error = NULL;
	
	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error) {
		log(ERROR, "%s: Failed to open report db", __func__);
		goto out;
	}
	
	siq_reportdb_delete_by_policyid(db, policy_id, &error);
	if (error) {
		log(ERROR, "%s: Failed to clear reports from db", __func__);
		goto out;
	}
	
	siq_reportdb_rebuild_policy_helper(db, SIQ_SCHED_REPORTS_DIR, 
	    policy_id, &error);
	
out:
	siq_reportdb_close(db);
	isi_error_handle(error, error_out);
}

void
siq_reportdb_rebuild(struct isi_error **error_out)
{
	sqlite3 *db = NULL;
	DIR *dirp = NULL;
	struct dirent *de = NULL;
	int ret = 0;
	char path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;
	
	ret = unlink(SIQ_REPORTS_REPORT_DB_PATH);
	if (ret != 0 && errno != ENOENT) {
		log(ERROR, "%s: Failed to unlink report db", __func__);
		error = isi_siq_error_new(E_SIQ_SQL_ERR, 
		    "Failed to unlink report db");
		goto out;
	}
	
	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error) {
		log(ERROR, "%s: Failed to open report db", __func__);
		goto out;
	}
	
	dirp = opendir(SIQ_SCHED_REPORTS_DIR);
	if (!dirp) {
		log(ERROR, "%s: Failed to open report dir", __func__);
		goto out;
	}
	
	errno = 0;
	de = readdir(dirp);
	while (de != NULL) {
		if (de->d_type == DT_DIR) {
			//Skip . and ..
			if (!strcmp(de->d_name, ".") || 
			    !strcmp(de->d_name, "..") ||
			    !strcmp(de->d_name, ".snapshot")) {
			    	errno = 0;
				de = readdir(dirp);
				continue;
			}
			log(TRACE, "%s: Processing %s", __func__, de->d_name);
			sprintf(path, "%s/%s", SIQ_SCHED_REPORTS_DIR, 
			    de->d_name);
			siq_reportdb_rebuild_policy_helper(db, path, de->d_name, 
			    &error);
			if (error) {
				goto out;
			}
		} else {
			log(TRACE, "%s: Skipping %s", __func__, de->d_name);
		}
		//Iterate
		errno = 0;
		de = readdir(dirp);
	}
	if (errno != 0) {
		log(ERROR, "%s: encountered error in readdir (%d)", 
		    __func__, errno);
	}
	
out:
	siq_reportdb_close(db);
	if (dirp) {
		closedir(dirp);
	}
	isi_error_handle(error, error_out);
}

void
siq_reportdb_get_count(sqlite3 *db, int *num_out, struct isi_error **error_out)
{
	char *result = NULL;
	struct isi_error *error = NULL;
	      
	siq_sqlite_exec_scalar(db, REPORTS_SELECT_COUNT_SQL, &result, &error);
	if (error) {
		goto out;
	}
	
	*num_out = atoi(result);
out:
	free(result);
	isi_error_handle(error, error_out);
}

void
siq_reportdb_get_count_by_policy_id(sqlite3 *db, char *policy_id, int *num_out,
    struct isi_error **error_out)
{
	char *query = NULL;
	char *result = NULL;
	struct isi_error *error = NULL;
	
	asprintf(&query, REPORTS_SELECT_COUNT_BY_PID_SQL, policy_id);
	ASSERT(query);
	      
	siq_sqlite_exec_scalar(db, query, &result, &error);
	if (error) {
		goto out;
	}
	
	*num_out = atoi(result);
out:
	free(query);
	free(result);
	isi_error_handle(error, error_out);
}

void
siq_reportdb_get_count_by_legacy_job_id(sqlite3 *db, int *num_out, 
    struct isi_error **error_out)
{
	char *query = NULL;
	char *result = NULL;
	struct isi_error *error = NULL;
	
	asprintf(&query, REPORTS_SELECT_COUNT_BY_LEGACY_JID_SQL);
	ASSERT(query);
	      
	siq_sqlite_exec_scalar(db, query, &result, &error);
	if (error) {
		goto out;
	}
	
	*num_out = atoi(result);
out:
	free(query);
	free(result);
	isi_error_handle(error, error_out);
}

void
siq_reportdb_get_count_by_lin(sqlite3 *db, char *lin, int *num_out,
    struct isi_error **error_out)
{
	char *query = NULL;
	char *result = NULL;
	struct isi_error *error = NULL;
	
	asprintf(&query, REPORTS_SELECT_COUNT_BY_LIN_SQL, lin);
	ASSERT(query);
	      
	siq_sqlite_exec_scalar(db, query, &result, &error);
	if (error) {
		goto out;
	}
	
	*num_out = atoi(result);
out:
	free(query);
	free(result);
	isi_error_handle(error, error_out);
}

void
siq_reportdb_get_count_by_pid_jid_dirty_bit(sqlite3 *db, 
    char *policy_id, char *job_id, int dirty_bit, int *num_out, 
    struct isi_error **error_out)
{
	char *query = NULL;
	char *result = NULL;
	struct isi_error *error = NULL;
	
	asprintf(&query, REPORTS_SELECT_COUNT_BY_PID_JID_DIRTY_BIT_SQL, 
	    policy_id, job_id, dirty_bit);
	ASSERT(query);
	      
	siq_sqlite_exec_scalar(db, query, &result, &error);
	if (error) {
		goto out;
	}
	
	*num_out = atoi(result);
out:
	free(query);
	free(result);
	isi_error_handle(error, error_out);
}

int
siq_reportdb_get_reports_by_job_id(sqlite3 *db, char *policy_id, 
    char *job_id, struct siq_job_summary ***reports_out, 
    struct isi_error **error_out)
{
	struct reportdb_record **records = NULL;
	struct reportdb_record *current_record = NULL;
	struct siq_job_summary **to_return = NULL;
	struct siq_job_summary *new_report = NULL;
	int num_records = 0;
	int counter = 0;
	int checker = 0;
	char current_path[MAXPATHLEN + 1];
	char rel_path[MAXPATHLEN + 1];
	size_t path_len;
	ifs_lin_t lin;
	struct isi_error *error = NULL;
	int ret = 0;
	
	num_records = siq_reportdb_get_by_policyid_jobid(db, &records, 
	    policy_id, job_id, &error);
	if (error) {
		goto out;
	}
	
	if (num_records <= 0) {
		return 0;
	}
	to_return = calloc(num_records, sizeof(struct siq_job_summary *));
	ASSERT(to_return);
	
	for (counter = 0; counter < num_records; counter++) {
		current_record = records[counter];
		lin = atoll(current_record->lin);

		/* This loop was added to solve bug 159310. If the function
		 * siq_job_summary_load_readonly does not populate new_report
		 * then we have the wrong path to the lin and need to recall
		 * pctl2_lin_get_path_plus to get the correct path.
		*/
		for (checker = 0; checker < 2; checker++) {
			if (new_report != NULL) {
				siq_job_summary_free(new_report);
				new_report = NULL;
			}
			ret = pctl2_lin_get_path_plus(ROOT_LIN, lin,
			HEAD_SNAPID, 0, 0, ENC_DEFAULT, sizeof(rel_path),
			rel_path, &path_len, 0, NULL, NULL, 0, NULL, NULL,
			PCTL2_LIN_GET_PATH_NO_PERM_CHECK);  // bug 220379
			if (ret != 0) {
				error = isi_siq_error_new(E_SIQ_REPORT_ERR,
				    "Failed get path for lin %s",
				    current_record->lin);
				goto out;
			}
			snprintf(current_path, sizeof(current_path), "/ifs/%s",
		    	    rel_path);
		
			new_report = siq_job_summary_load_readonly(
			    current_path, &error);
			if (error) {
				siq_job_summary_free(new_report);
				goto out;
			}

			if (new_report->job_id != 0)
				break;
		}
		
		to_return[counter] = new_report;
		new_report = NULL;
	}
	
out:
	siq_reportdb_records_free(records);
	if (error) {
		ret = -1;
		if (to_return) {
			free(to_return);
		}
	} else {
		ret = num_records;
		*reports_out = to_return;
	}
	isi_error_handle(error, error_out);
	return ret;
}

void 
siq_composite_report_free(struct siq_composite_report *report)
{
	siq_job_summary_free(report->composite);
	report->composite = NULL;
	siq_job_summary_free_array(report->elements, report->num_reports);
	FREE_AND_NULL(report->elements);
	free(report);
}

void 
siq_composite_reports_free(struct siq_composite_report **reports)
{
	struct siq_composite_report *current = NULL;
	int i = 0;
	
	if (!reports) {
		return;
	}
	
	current = reports[i];
	while(current != NULL) {
		siq_composite_report_free(current);
		current = NULL;
		i++;
		current = reports[i];
	}
	free(reports);
}

#define FIELD_SUM(field) composite_rpt->field += current_report->field;

#define FIELD_SUM_SAFE(parents, field) 	\
if (current_report->parents) FIELD_SUM(parents->field);

#define FIELD_SUM_SAFE_SRCDST(field)			\
if (current_report->field) {				\
	FIELD_SUM(field->src);		\
	FIELD_SUM(field->dst);		\
}

static void
sum_dirs_helper(struct siq_job_summary *composite_rpt, 
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->dirs) {
		FIELD_SUM_SAFE(total->stat->dirs->src, visited);
		FIELD_SUM_SAFE(total->stat->dirs->dst, deleted);
		FIELD_SUM_SAFE_SRCDST(total->stat->dirs->created);
		FIELD_SUM_SAFE_SRCDST(total->stat->dirs->deleted);
		FIELD_SUM_SAFE_SRCDST(total->stat->dirs->linked);
		FIELD_SUM_SAFE_SRCDST(total->stat->dirs->unlinked);
		FIELD_SUM(total->stat->dirs->replicated);
	}
}

static void
sum_files_helper(struct siq_job_summary *composite_rpt, 
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->files) {
		FIELD_SUM(total->stat->files->total);
		if (current_report->total->stat->files->replicated) {
			FIELD_SUM(total->stat->files->replicated->selected);
			FIELD_SUM(total->stat->files->replicated->transferred);
			FIELD_SUM(total->stat->files->replicated->new_files);
			FIELD_SUM(total->stat->files->replicated->updated_files);
			FIELD_SUM(total->stat->files->replicated->files_with_ads);
			FIELD_SUM(total->stat->files->replicated->ads_streams);
			FIELD_SUM(total->stat->files->replicated->symlinks);
			FIELD_SUM(total->stat->files->replicated->block_specs);
			FIELD_SUM(total->stat->files->replicated->char_specs);
			FIELD_SUM(total->stat->files->replicated->sockets);
			FIELD_SUM(total->stat->files->replicated->fifos);
			FIELD_SUM(total->stat->files->replicated->hard_links);
		}

		FIELD_SUM_SAFE_SRCDST(total->stat->files->deleted);
		FIELD_SUM_SAFE_SRCDST(total->stat->files->linked);
		FIELD_SUM_SAFE_SRCDST(total->stat->files->unlinked);
	
		if (current_report->total->stat->files->skipped) {
			FIELD_SUM(total->stat->files->skipped->up_to_date);
			FIELD_SUM(total->stat->files->skipped->user_conflict);
			FIELD_SUM(total->stat->files->skipped->error_io);
			FIELD_SUM(total->stat->files->skipped->error_net);
			FIELD_SUM(total->stat->files->skipped->error_checksum);
		}
	}
}

static void
sum_bytes_helper(struct siq_job_summary *composite_rpt, 
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->bytes) {
		FIELD_SUM(total->stat->bytes->transferred);
		FIELD_SUM(total->stat->bytes->recoverable);
		FIELD_SUM_SAFE_SRCDST(total->stat->bytes->recovered);

		if (current_report->total->stat->bytes->data) {
			FIELD_SUM(total->stat->bytes->data->total);
			FIELD_SUM(total->stat->bytes->data->file);
			FIELD_SUM(total->stat->bytes->data->sparse);
			FIELD_SUM(total->stat->bytes->data->unchanged);
		}
		if (current_report->total->stat->bytes->network) {
			FIELD_SUM(total->stat->bytes->network->total);
			FIELD_SUM(total->stat->bytes->network->to_target);
			FIELD_SUM(total->stat->bytes->network->to_source);
		}
	}
}

static void
sum_change_compute_helper(struct siq_job_summary *composite_rpt,
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->change_compute) {
		FIELD_SUM(total->stat->change_compute->lins_total);
		FIELD_SUM(total->stat->change_compute->dirs_new);
		FIELD_SUM(total->stat->change_compute->dirs_deleted);
		FIELD_SUM(total->stat->change_compute->dirs_moved);
		FIELD_SUM(total->stat->change_compute->dirs_changed);
		FIELD_SUM(total->stat->change_compute->files_new);
		FIELD_SUM(total->stat->change_compute->files_linked);
		FIELD_SUM(total->stat->change_compute->files_unlinked);
		FIELD_SUM(total->stat->change_compute->files_changed);
	}
}

static void
sum_change_transfer_helper(struct siq_job_summary *composite_rpt,
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->change_transfer) {
		FIELD_SUM(total->stat->change_transfer->hash_exceptions_found);
		FIELD_SUM(total->stat->change_transfer->hash_exceptions_fixed);
		FIELD_SUM(total->stat->change_transfer->flipped_lins);
		FIELD_SUM(total->stat->change_transfer->corrected_lins);
		FIELD_SUM(total->stat->change_transfer->resynced_lins);
	}
}

static void
sum_compliance_helper(struct siq_job_summary *composite_rpt,
    struct siq_job_summary *current_report)
{
	if (current_report->total->stat->compliance) {
		FIELD_SUM(total->stat->compliance->resync_compliance_dirs_new);
		FIELD_SUM(
		    total->stat->compliance->resync_compliance_dirs_linked);
		FIELD_SUM(total->stat->compliance->resync_conflict_files_new);
		FIELD_SUM(
		    total->stat->compliance->resync_conflict_files_linked);
		FIELD_SUM(total->stat->compliance->conflicts);
		FIELD_SUM(total->stat->compliance->committed_files);
	}
}

void
siq_reportdb_create_composite_report(struct siq_job_summary **reports, 
    int num_reports, struct siq_composite_report **composite_out, 
    struct isi_error **error_out)
{
	struct siq_composite_report *new_report = NULL;
	struct siq_job_summary *current_report = NULL;
	struct siq_job_summary *composite_rpt = NULL;
	struct siq_string_list *current_str = NULL;
	struct siq_phase_history *current_phase = NULL;
	struct siq_phase_history *new_phase = NULL;
	struct siq_phase_history *last_phase = NULL;
	struct siq_phase_history *saved_phase = NULL;
	int count = 0;
	int success_count = 0;
	int fail_count = 0;
	
	if (num_reports < 1 || !reports) {
		return;
	}
	//Allocate
	new_report = calloc(1, sizeof(struct siq_composite_report));
	new_report->composite = siq_job_summary_create();
	composite_rpt = new_report->composite;
	new_report->elements = reports;
	new_report->num_reports = num_reports;
	
	//Populate the static stuff
	composite_rpt->action = reports[0]->action;
	if (reports[0]->snapshots) {
		SLIST_FOREACH(current_str, &reports[0]->snapshots->target, next) {
			siq_snapshots_target_append(composite_rpt, 
			    current_str->msg);
		}
	}
	composite_rpt->total->start = reports[0]->total->start;
	composite_rpt->total->end = reports[0]->total->end;
	composite_rpt->total->state = reports[0]->total->state;
	composite_rpt->job_id = reports[0]->job_id;
	siq_report_spec_free(composite_rpt->job_spec);
	composite_rpt->job_spec = siq_spec_copy(reports[0]->job_spec);
	
	//Iterate
	for (count = 0; count < num_reports; count++) {
		current_report = reports[count];
		//We want the start time of the oldest report
		if (current_report->total) {
			if (current_report->total->start < 
			    composite_rpt->total->start) {
				composite_rpt->total->start = 
				    current_report->total->start;
			}
			//We want the state/end time of the newest report
			if (composite_rpt->total->end < 
			    current_report->total->end) {
				composite_rpt->total->end = 
				    current_report->total->end;
				composite_rpt->total->state = 
				    current_report->total->state;
			}

			if (current_report->total->state == SIQ_JS_SUCCESS ||
			    current_report->total->state == SIQ_JS_SKIPPED)
				success_count++;
			else if (current_report->total->state == SIQ_JS_FAILED)
				fail_count++;
			
			
			new_report->duration += current_report->total->end - 
			    current_report->total->start;
		}
		//We want the highest total number of phases
		if (current_report->total_phases > composite_rpt->total_phases) {
			composite_rpt->total_phases = 
			    current_report->total_phases;
		}

		//siq_stat_dirs
		if (current_report->total && current_report->total->stat) {
			sum_dirs_helper(composite_rpt, current_report);
			sum_files_helper(composite_rpt, current_report);
			sum_bytes_helper(composite_rpt, current_report);
			sum_change_compute_helper(composite_rpt, current_report);
			sum_change_transfer_helper(composite_rpt, current_report);
			sum_compliance_helper(composite_rpt, current_report);
		}

		FIELD_SUM(num_retransmitted_files);
		
		//siq_job_chunks
		if (current_report->chunks) {
			FIELD_SUM(chunks->total);
			FIELD_SUM(chunks->running);
			FIELD_SUM(chunks->success);
			FIELD_SUM(chunks->failed);
		}

		composite_rpt->sync_type = current_report->sync_type;
		
		//Aggregate errors, warnings, retransmitted files, phases
		SLIST_FOREACH(current_str, &current_report->errors, next) {
			siq_job_error_msg_append(composite_rpt, 
			    current_str->msg);
		}
		
		SLIST_FOREACH(current_str, &current_report->warnings, next) {
			siq_job_warning_msg_append(composite_rpt, 
			    current_str->msg);
		}
		
		SLIST_FOREACH(current_str, &current_report->retransmitted_files, 
		    next) {
			siq_job_retransmitted_file_append(composite_rpt, 
			    current_str->msg);
		}
		SLIST_FOREACH(current_phase, &current_report->phases, next) {
			CALLOC_AND_ASSERT(new_phase, struct siq_phase_history);
			memcpy(new_phase, current_phase, 
			    sizeof(struct siq_phase_history));
			if (last_phase == NULL) {
				SLIST_INSERT_HEAD(&composite_rpt->phases, 
				    new_phase, next);
			} else {
				SLIST_INSERT_AFTER(last_phase, 
				    new_phase, next);
			}
			if (current_phase == 
			    SLIST_FIRST(&current_report->phases)) {
				saved_phase = new_phase;
			}
		}
		last_phase = saved_phase;
	}

	if (success_count > 0) {
		if (fail_count > 0) 
			composite_rpt->total->state = SIQ_JS_SUCCESS_ATTN;
		else if (composite_rpt->total->state != SIQ_JS_SKIPPED)
			composite_rpt->total->state = SIQ_JS_SUCCESS;
	}
	// If most recent report was canceled or preempted, report canceled
	// or preempted.
	else if (composite_rpt->total->state != SIQ_JS_CANCELLED &&
	    composite_rpt->total->state != SIQ_JS_PREEMPTED)
		composite_rpt->total->state = SIQ_JS_FAILED;
	
	*composite_out = new_report;
}

static bool
siq_reportdb_get_reports_status(sqlite3 *db, const char *policy_id, 
    int *job_id_out, bool *inc_job_id_out, struct isi_error **error_out)
{
	char *query = NULL;
	char **result = NULL;
	int num_rows = 0;
	int num_cols = 0;
	bool ret = false;
	struct isi_error *error = NULL;
	
	ASSERT(db && policy_id);

	asprintf(&query, REPORTS_STATUS_SELECT_SQL, policy_id);
	ASSERT(query);
	
	siq_sqlite_query(db, query, &result, &num_rows, &num_cols, &error);
	if (error) {
		goto out;
	} else if (num_rows <= 0) {
		//Return NULL
		goto out;
	}
	ASSERT(num_cols == REPORTS_STATUS_SELECT_NUM_COLS);
	
	if (job_id_out)
		*job_id_out = atoi(result[REPORTS_STATUS_SELECT_NUM_COLS]); 

	if (inc_job_id_out)
		*inc_job_id_out = 
		    !!atoi(result[REPORTS_STATUS_SELECT_NUM_COLS + 1]); 

	ret = true;

out:
	siq_sqlite_free_query_result(result);
	free(query);

	isi_error_handle(error, error_out);
	return ret;
}

static void
siq_reportdb_set_reports_status(sqlite3 *db, const char *policy_id, 
    const int job_id, const bool inc_job_id, struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_STATUS_UPDATE_SQL,
	    policy_id, job_id, inc_job_id, policy_id);
}

void
siq_reports_status_set_inc_job_id(const char *policy_id,
    struct isi_error **error_out)
{
	int job_id = 0;
	sqlite3 *db = NULL;
	bool found = false;
	struct isi_error *error = NULL;

	ASSERT(policy_id);

	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error)
		goto out;

	found = siq_reportdb_get_reports_status(db, policy_id, &job_id, NULL, 
	    &error);
	if (error)
		goto out;

	if (!found)
		job_id = 1;

	ASSERT(job_id > 0);

	siq_reportdb_set_reports_status(db, policy_id, job_id, true, &error);
	if (error)
		goto out;	
	
out:
	if (db)
		siq_reportdb_close(db);
	isi_error_handle(error, error_out);
}

static int
siq_reports_status_check_inc_job_id_internal(const char *policy_id,
    const bool set_job_id, struct isi_error **error_out)
{
	int ret = 0;

	int job_id = 0;
	bool inc_job_id = 0;
	sqlite3 *db = NULL;
	bool found = false;
	struct isi_error *error = NULL;

	ASSERT(policy_id);

	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error)
		goto out;

	found = siq_reportdb_get_reports_status(db, policy_id, &job_id, 
	    &inc_job_id, &error);
	if (error)
		goto out;

	if (!found)
		job_id = 1;

	ASSERT(job_id > 0);

	if (!inc_job_id)
		goto out;

	job_id++;
	siq_reportdb_set_reports_status(db, policy_id, job_id,
	    set_job_id, &error);
	if (error)
		goto out;	
	
out:
	if (!error)
		ret = job_id;

	if (db)
		siq_reportdb_close(db);

	isi_error_handle(error, error_out);
	return ret;
}

int
siq_reports_status_check_inc_job_id(const char *policy_id,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int res;

	res = siq_reports_status_check_inc_job_id_internal(policy_id,
	    false, &error);

	isi_error_handle(error, error_out);
	return res;
}

int
siq_reports_status_check_and_set_inc_job_id(const char *policy_id,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int res;

	res = siq_reports_status_check_inc_job_id_internal(policy_id,
	    true, &error);

	isi_error_handle(error, error_out);
	return res;
}

void
siq_reports_status_delete_by_policyid(sqlite3 *db, char *policy_id,
    struct isi_error **error_out)
{
	siq_reportdb_generic_exec(db, error_out, REPORTS_STATUS_DELETE_PID_SQL,
	    policy_id);
}
