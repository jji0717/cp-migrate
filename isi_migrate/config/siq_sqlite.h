#ifndef __SIQ_SQLITE_H__
#define __SIQ_SQLITE_H__

#include <sqlite3.h>
#include "isi_migrate/siq_error.h"

// Timeout 60 seconds
#define SQL_BUSY_TIMEOUT 60000
#define SQL_RETRY_TIMES 10

void siq_sqlite_open(char *filename, sqlite3 **db, 
    struct isi_error **error_out);

bool siq_sqlite_verify_table_exists(sqlite3 *db, char *table_name, 
    struct isi_error **error_out);

void siq_sqlite_close(sqlite3 *db);

void siq_sqlite_exec_retry(sqlite3 *db, char *query, 
    int (*callback)(void*,int,char**,char**), void *arg, 
    struct isi_error **error_out);

void siq_sqlite_query(sqlite3 *db, char *query, char ***result, int *num_rows, 
    int *num_cols, struct isi_error **error_out);

void siq_sqlite_exec_scalar(sqlite3 *db, char *query, char **result_out, 
    struct isi_error **error_out);

void siq_sqlite_free_query_result(char **result);

void siq_sqlite_set_cache_size(sqlite3 *db, int size, 
    struct isi_error **error_out);

void siq_sqlite_set_unsafe_mode(sqlite3 *db, struct isi_error **error_out);

#endif
