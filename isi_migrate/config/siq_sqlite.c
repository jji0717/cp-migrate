#include "siq_sqlite.h"
#include <unistd.h>
#include <stdio.h>

#include "isi_migrate/sched/siq_log.h"

void
siq_sqlite_open(char *filename, sqlite3 **db, struct isi_error **error_out)
{
	int ret = 0;
	sqlite3 *to_return;
	struct isi_error *error = NULL;
	
	ASSERT(*db == NULL);
	
	if (!filename || strcmp(filename, "") == 0) {
		error = isi_siq_error_new(E_SIQ_SQL_ERR, 
		    "%s: File name cannot be blank", __func__);
	}
	
	ret = sqlite3_open(filename, &to_return);
	if (ret != SQLITE_OK) {
		error = isi_siq_error_new(E_SIQ_SQL_ERR, 
		    "%s: Failed to open sqlite db %s (%d)", __func__, 
		    filename, ret);
		sqlite3_close(to_return);
		goto out;
	}
	
	sqlite3_busy_timeout(to_return, SQL_BUSY_TIMEOUT);
	*db = to_return;
	
out:
	isi_error_handle(error, error_out);
}

static int
siq_sqlite_verify_table_exists_callback(void *table_exists, int columns, 
    char **results, char **col_names)
{
	if ((results != NULL) && (columns > 0)) {
		*(bool *)table_exists = true;
	}
	return 0;
}

bool
siq_sqlite_verify_table_exists(sqlite3 *db, char *table_name, 
    struct isi_error **error_out)
{
	char *query = NULL;
	bool table_exists = false;
	struct isi_error *error = NULL;
	
	//Ensure that the target_status table exists
	ASSERT(db != NULL);
	
	asprintf(&query, "SELECT * FROM sqlite_master WHERE "
	    "type = 'table' and name = '%s';", table_name);
	
	siq_sqlite_exec_retry(db, query, siq_sqlite_verify_table_exists_callback, 
	    &table_exists, &error);
	
	if (error) {
		goto out;
	}
	
out:
	free(query);
	isi_error_handle(error, error_out);
	return table_exists;
}

void
siq_sqlite_close(sqlite3 *db)
{
	if (!db)
		return;
	sqlite3_close(db);
}

static void
_siq_sqlite_exec_retry(sqlite3 *db, char *query, 
    int (*callback)(void*,int,char**,char**), void *arg, 
    bool use_trans, struct isi_error **error_out) 
{
	int ret = 0;
	int retry = 0;
	char *trans_query = NULL;
	char *err_msg = NULL;
	struct isi_error *error = NULL;
	
	if (use_trans) {
		asprintf(&trans_query, "BEGIN TRANSACTION; %s COMMIT;", query);
	} else {
		trans_query = strdup(query);
	}
	ASSERT(trans_query);
	log(TRACE, "Query: %s\n", trans_query);
	
	for (retry = 0; retry < SQL_RETRY_TIMES; retry++) {
		ret = sqlite3_exec(db, trans_query, callback, arg, &err_msg);
		if ((ret == SQLITE_BUSY) || (ret == SQLITE_LOCKED)) {
			//Busy retry loop...
			log(TRACE, "err_msg: %s\n", err_msg);
			if (err_msg != NULL) {
				sqlite3_free(err_msg);
				err_msg = NULL;
			}
			ret = 0;
			siq_nanosleep(random() & 0x3, 0);
			sqlite3_exec(db, "ROLLBACK;", NULL, 0, NULL);
		} else if (ret != SQLITE_OK) {
			//Query errored out
			log(TRACE, "err_msg: %s\n", err_msg);
			error = isi_siq_error_new(E_SIQ_SQL_ERR, 
			    "%s: Sqlite error encountered: %s (%d)", __func__, 
			    err_msg, ret);
			goto out;
		} else {
			break;
		}
	}
	
out:
	if (err_msg != NULL) {
		sqlite3_free(err_msg);
		err_msg = NULL;
	}
	free(trans_query);
	
	isi_error_handle(error, error_out);
	return;
}

void
siq_sqlite_exec_retry(sqlite3 *db, char *query, 
    int (*callback)(void*,int,char**,char**), void *arg, 
    struct isi_error **error_out)
{
	_siq_sqlite_exec_retry(db, query, callback, arg, true, error_out);
}

void
siq_sqlite_query(sqlite3 *db, char *query, char ***result, int *num_rows, 
    int *num_cols, struct isi_error **error_out)
{
	int ret = 0;
	char *trans_query = NULL;
	char *err_msg = NULL;
	struct isi_error *error = NULL;
	
	asprintf(&trans_query, "BEGIN TRANSACTION; %s COMMIT;", query);
	log(TRACE, "Query: %s\n", trans_query);
	
	ret = sqlite3_get_table(db, trans_query, result, num_rows, num_cols, 
	    &err_msg);
	if (ret != SQLITE_OK) {
		error = isi_siq_error_new(E_SIQ_SQL_ERR, 
		    "%s: Sqlite error encountered: %s (%d)", __func__, 
		    err_msg, ret);
		sqlite3_free(err_msg);
		sqlite3_exec(db, "ROLLBACK;", NULL, 0, NULL);
		err_msg = NULL;
		goto out;
	}
	
out:
	free(trans_query);
	
	isi_error_handle(error, error_out);
	return;
}

void
siq_sqlite_exec_scalar(sqlite3 *db, char *query, char **result_out, 
    struct isi_error **error_out)
{
	int ret = 0;
	char **result = NULL;
	char *trans_query = NULL;
	char *err_msg = NULL;
	int num_rows = 0;
	int num_cols = 0;
	struct isi_error *error = NULL;
	
	asprintf(&trans_query, "BEGIN TRANSACTION; %s COMMIT;", query);
	log(TRACE, "Query: %s\n", trans_query);
	
	ret = sqlite3_get_table(db, trans_query, &result, &num_rows, &num_cols, 
	    &err_msg);
	if (ret != SQLITE_OK) {
		error = isi_siq_error_new(E_SIQ_SQL_ERR, 
		    "%s: Sqlite error encountered: %s (%d)", __func__, 
		    err_msg, ret);
		sqlite3_free(err_msg);
		sqlite3_exec(db, "ROLLBACK;", NULL, 0, NULL);
		err_msg = NULL;
		goto out;
	}
	
	//If these asserts fail, you should probably fix your query.
	ASSERT(num_rows == 1);
	ASSERT(num_cols == 1);
	
	*result_out = strdup(result[1]);
	
out:
	siq_sqlite_free_query_result(result);
	
	free(trans_query);
	
	isi_error_handle(error, error_out);
	return;
}

void
siq_sqlite_free_query_result(char **result)
{
	sqlite3_free_table(result);
	result = NULL;
}

void
siq_sqlite_set_cache_size(sqlite3 *db, int size, 
    struct isi_error **error_out)
{
	char *query = NULL;
	struct isi_error *error = NULL;
	
	ASSERT(db != NULL);
	
	asprintf(&query, "PRAGMA cache_size = %d;", size);
	
	_siq_sqlite_exec_retry(db, query, NULL, NULL, false, &error);
	
	if (error) {
		goto out;
	}
	
out:
	free(query);
	isi_error_handle(error, error_out);
}

void
siq_sqlite_set_unsafe_mode(sqlite3 *db, struct isi_error **error_out)
{
	char *query = NULL;
	struct isi_error *error = NULL;
	
	ASSERT(db != NULL);
	
	asprintf(&query, "PRAGMA synchronous = OFF;");
	_siq_sqlite_exec_retry(db, query, NULL, NULL, false, &error);
	if (error) {
		goto out;
	}
	
	free(query);
	asprintf(&query, "PRAGMA journal_mode = MEMORY;");
	_siq_sqlite_exec_retry(db, query, NULL, NULL, false, &error);
	if (error) {
		goto out;
	}
	
out:
	free(query);
	isi_error_handle(error, error_out);
}

