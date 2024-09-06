#include <sqlite3.h>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <isi_util/isi_error.h>
#include <isi_cloud_common/isi_cpool_version.h>
#include "cpool_stat_persistence_manager.h"

#define DATABASE_NAME "/ifs/.ifsvar/modules/cloud/cpool_stats.db"
#define TEST_DATABASE_NAME "/ifs/.ifsvar/modules/cloud/cpool_stats_test.db"
#define TOTALS_TABLE "cpool_stats_curr_totals"
#define HISTORY_TABLE "cpool_stats_history_totals"
#define HISTORY_TIME_INDEX "time_i" ///索引名字: 放在history_table中

/*
 * Ideally, we wouldn't use a timeout waiting for a lock - instead,
   just use a large value
 */
#define SQL_WAIT_FOR_LOCK_TIMEOUT_MS 1000*60*60 // 1 hour
/////begin transaction和commit中间的所有数据如果遇到执行错误会回滚.因为commit之前批量的数据都是保留在内存中。
////没有使用begin transaction,commit,每次操作数据都被自动提交写入磁盘
#define BEGIN_WRITE_TRANSACTION_QUERY "BEGIN IMMEDIATE TRANSACTION"
#define BEGIN_READ_TRANSACTION_QUERY "BEGIN TRANSACTION"
#define COMMIT_TRANSACTION_QUERY "COMMIT"
#define ROLLBACK_TRANSACTION_QUERY "ROLLBACK"

#define BEGIN_TRANSACTION(conn, query, error, out_label) {\
	ASSERT(conn != NULL); \
	int __begin_trans_ret__ = sqlite3_exec(conn, query, NULL, NULL, NULL); \
	CHECK_SQLITE_ERROR(__begin_trans_ret__, conn, error, \
	    ERROR_BEGIN_TRANSACTION, out_label); \
}

#define BEGIN_WRITE_TRANSACTION(conn, error, out_label) \
	BEGIN_TRANSACTION(conn, BEGIN_WRITE_TRANSACTION_QUERY, error, out_label)

#define BEGIN_READ_TRANSACTION(conn, error, out_label) \
	BEGIN_TRANSACTION(conn, BEGIN_READ_TRANSACTION_QUERY, error, out_label)

#define COMMIT_TRANSACTION(conn, error, out_label) { \
	ASSERT(conn != NULL); \
	int __end_trans_ret__ = sqlite3_exec(conn, COMMIT_TRANSACTION_QUERY, \
	    NULL, NULL, NULL); \
	CHECK_SQLITE_ERROR(__end_trans_ret__, conn, error, ERROR_COMMIT_SCHEMA, \
	    out); \
}

#define ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn, error) {\
	ASSERT(conn != NULL); \
	if (error != NULL) \
		sqlite3_exec(conn, ROLLBACK_TRANSACTION_QUERY, NULL, NULL, \
		    NULL); \
}

#define COMPLETE_RETRYABLE_TRANSACTION

#define GET_USER_VERSION "PRAGMA user_version"
#define SET_USER_VERSION "PRAGMA user_version = "

#define CREATE_TABLE(name) "create table if not exists '" name "' "

#define SCHEMA_TOTALS_TABLE "(" \
    "account integer not null," \
    "bytes_in unsigned big int default 0," \
    "bytes_out unsigned big int default 0," \
    "num_gets unsigned big int default 0," \
    "num_puts unsigned big int default 0," \
    "num_deletes unsigned big int default 0," \
    "storage_used unsigned big int default 0," \
    "storage_total unsigned big int default 0," \
    "primary key (account))"
#define SCHEMA_HISTORY_TABLE "(" \
    "account integer not null," \
    "time integer not null," \
    "bytes_in unsigned big int default 0," \
    "bytes_out unsigned big int default 0," \
    "num_gets unsigned big int default 0," \
    "num_puts unsigned big int default 0," \
    "num_deletes unsigned big int default 0," \
    "storage_used unsigned big int default 0," \
    "storage_total unsigned big int default 0," \
    "primary key (account, time))"

/* Updates history table with latest running totals and a timestamp */
///把total table中的数据拿出来+当前时间，插入到history table中
#define HISTORY_TABLE_UPDATE_QUERY "insert into " HISTORY_TABLE " " \
    "(time, account, bytes_in, bytes_out, num_gets, num_puts, num_deletes, " \
      "storage_used) " \
    "select strftime('%s','now'), " \
      "account, bytes_in, bytes_out, num_gets, num_puts, num_deletes, " \
      "storage_used from " TOTALS_TABLE

/* Creates a record for given account if none exists already */
#define CHECK_ACCOUNT_REC_EXISTS_QUERY \
    "insert or ignore into " TOTALS_TABLE " (account) values (?)"

/* Adds to running totals table */
#define ADD_TO_TOTALS_QUERY \
    "update " TOTALS_TABLE " " \
	"set bytes_in=bytes_in + ?, bytes_out=bytes_out + ?, " \
	"num_gets=num_gets + ?, num_puts=num_puts + ?, " \
	"num_deletes=num_deletes + ?, storage_used=storage_used + ?, " \
	"storage_total=0 where account=?"

/* Sets values in the running totals table */
#define SET_TOTALS_QUERY_PREFIX \
    "update " TOTALS_TABLE " " \
	"set "

/* List of column names to be leveraged by queries */
#define STAT_COL_NAMES " bytes_in, bytes_out, num_gets, num_puts, " \
    "num_deletes, storage_used, storage_total "

/* Retrieves current totals from database for a given account */
#define GET_TOTAL_STATS_QUERY \
    "select " STAT_COL_NAMES " from " HISTORY_TABLE " " \
	"where account = ? " \
	"order by time " \
	"desc limit 1"

/*
 * Retrieves last record before given start_time and last_record before
 * end_time
 */
#define GET_CHANGE_IN_STATS_QUERY \
    "select " STAT_COL_NAMES " from " \
    /* select last record between start_time and end_time */ \
	"(" \
	    "select " STAT_COL_NAMES " from " HISTORY_TABLE " " \
		"where account = ? and time <= ? " \
		"order by time " \
		"desc limit 1) " \
    "union all " \
    /* select last record before start_time */ \
    "select " STAT_COL_NAMES " from " \
	"(" \
	    "select " STAT_COL_NAMES " from " HISTORY_TABLE " " \
		"where account = ? and time <= ? " \
		"order by time " \
		"desc limit 1)"

#define GET_ALL_DATAPOINTS_QUERY_PREFIX \
    "select account, time, " STAT_COL_NAMES " from " HISTORY_TABLE " " \
	"where time >= strftime('%s', 'now') - ? "
#define GET_ALL_DATAPOINTS_QUERY_ACCOUNT_FILTER \
    " and account in "
#define GET_ALL_DATAPOINTS_QUERY_SUFFIX \
    " order by account"

#define GET_ALL_DATAPOINTS_QUERY_UNFILTERED \
    GET_ALL_DATAPOINTS_QUERY_PREFIX \
    GET_ALL_DATAPOINTS_QUERY_SUFFIX

#define DELETE_TOTALS_TABLE_QUERY "delete from " TOTALS_TABLE
#define DELETE_HISTORY_TABLE_QUERY "delete from " HISTORY_TABLE

#define PREPOPULATE_HISTORY_QUERY \
    "insert into " HISTORY_TABLE "(time, account, bytes_in, bytes_out, " \
	"num_gets, num_puts, num_deletes, storage_used) values "\
	"(?, ?, ?, ?, ?, ?, ?, ?)"

/*
 * Removes all but one record between given start and end dates
 */
#define CULL_HISTORY_TABLE_QUERY_BETWEEN(sqlite_date_start, sqlite_date_end) \
    "delete from cpool_stats_history_totals " \
    "where " \
	"time between strftime('%s', " sqlite_date_start ") and " \
			"strftime('%s', " sqlite_date_end ") and " \
	"not time in (" \
	    "select time from cpool_stats_history_totals " \
	    "where time between strftime('%s', " sqlite_date_start ") and " \
			"strftime('%s', " sqlite_date_end ") " \
	    "order by time desc limit 1)"

#define DO_CULL_HISTORY_TABLE_BETWEEN(sqlite_date_start, sqlite_date_end, out) \
    { \
	int sql_ret = sqlite3_exec(conn_, \
	    CULL_HISTORY_TABLE_QUERY_BETWEEN( \
		sqlite_date_start, sqlite_date_end), \
	    NULL, NULL, NULL); \
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_CULL_HISTORY, \
	    out); \
    }

#define CHECK_SQLITE_RESULT(sqlite_result, expected_result, connection, err_ptr, err_msg, out_lbl) \
    if (sqlite_result != expected_result) {\
	err_ptr = isi_exception_error_new(err_msg, sqlite3_errmsg(connection)); \
	goto out_lbl; \
    }
#define CHECK_SQLITE_ERROR(sqlite_result, connection, err_ptr, err_msg, goto_lbl) \
    CHECK_SQLITE_RESULT(sqlite_result, SQLITE_OK, connection, err_ptr, err_msg, \
	goto_lbl)

#define ERROR_OPEN_CONNECTION "Failed to open database connection: %s"
#define ERROR_SET_TIMEOUT "Failed to set timeout for database connection: %s"
#define ERROR_BEGIN_TRANSACTION "Failed to begin transaction: %s"
#define ERROR_CREATE_TOTALS "Failed to create " TOTALS_TABLE ": %s"
#define ERROR_CREATE_HISTORY "Failed to create " HISTORY_TABLE ": %s"
#define ERROR_COMMIT_SCHEMA "Failed to create database schema: %s"
#define ERROR_PREPARE_STATEMENT "Failed to prepare statement: %s"
#define ERROR_BIND_PARAMETER "Failed to bind parameter: %s"
#define ERROR_VERIFY_TOTALS_ACCOUNT_EXISTS \
    "Failed to verify that account key exists: %s"
#define ERROR_QUERY_STAT_CHANGE "Failed to query for stats changes: %s"
#define ERROR_UPDATE_TOTALS "Failed to update " TOTALS_TABLE ": %s"
#define ERROR_UPDATE_HISTORY "Failed to update " HISTORY_TABLE ": %s"
#define ERROR_CULL_HISTORY "Failed to cull records from " HISTORY_TABLE ": %s"
#define ERROR_FINALIZE_STATEMENT "Failed to finalize statement: %s"
#define ERROR_DELETE_TOTALS_TABLE "Failed to delete from totals table: %s"
#define ERROR_DELETE_HISTORY_TABLE "Failed to delete from totals table: %s"
#define ERROR_ALL_STAT_QUERY "Failed to evaluate query for all stats: %s"
#define ERROR_RESET_STATEMENT "Failed to reset prepared statement: %s"
#define ERROR_EXECUTE_PREPOPULATE "Failed to prepopulate " HISTORY_TABLE ": %s"
#define ERROR_SET_USER_VERSION "Failed to set Cpool Stats version: %s"
#define ERROR_GET_USER_VERSION "Failed to get Cpool Stats version: %s"

/**
 * Internal implementation - cpool_stat_persistence_manager will really
 * leverage a database_manager.  The subclassing allows us to keep a clear
 * separation between interface and implementation (especially necessary here
 * since it's possible we will switch to an SBT implementation in the future)
 */
class cpool_stat_database_manager : public cpool_stat_persistence_manager
{
public:
	cpool_stat_database_manager() : database_file_(NULL), conn_(NULL)
	{
	};

	virtual ~cpool_stat_database_manager()
	{
		close_connection();
		if (database_file_)
			free(database_file_);
	};

	void initialize(const char *db_name, struct isi_error **error_out);

	/* Update functions */
	void add_to_stats_totals(const std::vector<acct_stat*> &stats,
	    struct isi_error **error_out);

	void set_stats_totals(
	    const std::vector<acct_stat*> &stats, const unsigned int field_mask,
	    struct isi_error **error_out);

	void update_stats_history(struct isi_error **error_out);

	void cull_stats_history(struct isi_error **error_out);


	/* Query functions */
	void get_change_in_stats(const cpool_stat_account_key &acct_key,
	    time_t start_time, time_t end_time, cpool_stat &ret_buffer,
	    struct isi_error **error_out);

	void get_total_stats(const cpool_stat_account_key &acct_key,
	    cpool_stat &ret_buffer, struct isi_error **error_out);

	void get_all_stats_results(
	    const std::vector<const cpool_stat_account_query_result_key*> &queries,
	    std::vector<acct_time_stat*> &ret_buffer,
	    struct isi_error **error_out);

	void delete_all_records(struct isi_error **error_out);

	void cleanup_for_test(struct isi_error **error_out);

	void prepopulate_stats_history(const cpool_stat &stat_per_interval,
	    const std::vector<account_key_t> &keys, unsigned int num_entries,
	    time_t time_spacing, struct isi_error **error_out);

	void get_all_data_for_accounts(
	    std::vector<cpool_stat_time_record*> &stats_out,
	    const unsigned int seconds_to_go_back,
	    const std::vector<account_key_t> *account_keys,
	    struct isi_error **error_out);

	uint32_t get_user_version(struct isi_error **error_out);
	void set_user_version(uint32_t user_version, struct isi_error **error_out);

protected:
	char *database_file_;

private:
	void open_connection(const char *database_name, uint32_t version,
	    struct isi_error **error_out);

	void close_connection();

	void create_database_schema(struct isi_error **error_out);


	/* Utilities for updating stats totals */
	void check_record_exists_for_account_key(
	    const cpool_stat_account_key *key, struct isi_error **error_out);
	void add_to_account_stats_totals( const cpool_stat *value,
	    const cpool_stat_account_key *key, struct isi_error **error_out);
	void get_change_in_stats_impl(const account_key_t acct_key,
	    time_t start_time, time_t end_time, cpool_stat &ret_buffer,
	    sqlite3_stmt *statement, struct isi_error **error_out);
	void build_set_totals_query(const unsigned int field_mask,
	    std::string &query);
	void set_account_stats_totals(const cpool_stat *value,
	    const cpool_stat_account_key *key, const unsigned int field_mask,
	    std::string &query, struct isi_error **error_out);
	void append_keys_to_sql_query(
	    const std::vector<account_key_t> *account_keys,
	    std::string &query_string);

	void check_set_user_version(uint32_t user_version,
	    struct isi_error **error_out);

	sqlite3 *conn_; ////sqlite3 db handle
	static uint32_t user_version_found_;
};

uint32_t cpool_stat_database_manager::user_version_found_ = 0;

class cpool_stat_test_database_manager : public cpool_stat_database_manager
{
	virtual ~cpool_stat_test_database_manager()
	{
		if (database_file_ != NULL)
			unlink(database_file_);
	};
};

cpool_stat_persistence_manager*
cpool_stat_persistence_manager::get_new_instance(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_database_manager *db_mgr =
	    new cpool_stat_database_manager();
	db_mgr->initialize(DATABASE_NAME, &error);
	if (error != NULL) {
		delete (db_mgr);
		db_mgr = NULL;
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return db_mgr;
}

cpool_stat_persistence_manager*
cpool_stat_persistence_manager::get_new_test_instance(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_database_manager *db_mgr =
	    new cpool_stat_database_manager();
	db_mgr->initialize(TEST_DATABASE_NAME, &error);
	if (error != NULL) {
		delete (db_mgr);
		db_mgr = NULL;
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return db_mgr;
}

void
cpool_stat_persistence_manager::cleanup_for_test()
{
	unlink(TEST_DATABASE_NAME);
}

void
cpool_stat_database_manager::initialize(const char *db_name,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	open_connection(db_name, CPOOL_STATS_VERSION, &error);
	ON_ISI_ERROR_GOTO(out, error);

	database_file_ = strdup(db_name);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::check_record_exists_for_account_key(
    const cpool_stat_account_key *key, struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);
	ASSERT(key != NULL);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;
	sqlite3_stmt *statement = NULL;

	sql_ret = sqlite3_prepare_v2(conn_, CHECK_ACCOUNT_REC_EXISTS_QUERY,
	    strlen(CHECK_ACCOUNT_REC_EXISTS_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	sql_ret = sqlite3_bind_int(statement, 1, key->get_account_key());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	/* Note* Do not start a transaction here - one is started by caller */
	sql_ret = sqlite3_step(statement);

	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_VERIFY_TOTALS_ACCOUNT_EXISTS, out);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void /////对已有的account的数据,把新的cpool_stat的数据累加上去
cpool_stat_database_manager::add_to_account_stats_totals(
    const cpool_stat *value, const cpool_stat_account_key *key,
    struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);
	ASSERT(key != NULL);
	ASSERT(value != NULL);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;
	sqlite3_stmt *statement = NULL;

	sql_ret = sqlite3_prepare_v2(conn_, ADD_TO_TOTALS_QUERY,
	    strlen(ADD_TO_TOTALS_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	sql_ret = sqlite3_bind_int64(statement, 1, value->get_bytes_in()); /////绑定数据
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int64(statement, 2, value->get_bytes_out());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int64(statement, 3, value->get_num_gets());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int64(statement, 4, value->get_num_puts());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int64(statement, 5, value->get_num_deletes());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int64(statement, 6, value->get_total_usage());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int(statement, 7, key->get_account_key());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

//	sql_ret = sqlite3_bind_int64(statement, 7, value->get_storage_total());
//	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	/* Note* Do not start a transaction here - one is started by caller */
	sql_ret = sqlite3_step(statement);
	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_UPDATE_TOTALS, out);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void ////把stat_map_中的内存stats拷贝出来，再持久化到total_table中
cpool_stat_database_manager::add_to_stats_totals(
    const std::vector<acct_stat*> &stats, struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	std::vector<acct_stat*>::const_iterator it;

	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	for (it = stats.begin(); it != stats.end(); ++it) {
		acct_stat *cur_acct_stat = (*it);

		const cpool_stat *value = cur_acct_stat->get_read_only_value();
		const cpool_stat_account_key *key = cur_acct_stat->get_key();
		ASSERT(key != NULL);
		ASSERT(value != NULL);

		/* Make sure a record exists for this account */
		check_record_exists_for_account_key(key, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/* Do the update */
		add_to_account_stats_totals(value, key, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::build_set_totals_query(
    const unsigned int field_mask, std::string &query)
{
	char delimiter = ' ';

	query.assign(SET_TOTALS_QUERY_PREFIX);

	if (field_mask & CP_STAT_BYTES_IN) {
		query.append(1, delimiter);
		query.append("bytes_in=? ");
		delimiter = ',';
	}

	if (field_mask & CP_STAT_BYTES_OUT) {
		query.append(1, delimiter);
		query.append("bytes_out=? ");
		delimiter = ',';
	}

	if (field_mask & CP_STAT_NUM_GETS) {
		query.append(1, delimiter);
		query.append("num_gets=? ");
		delimiter = ',';
	}

	if (field_mask & CP_STAT_NUM_PUTS) {
		query.append(1, delimiter);
		query.append("num_puts=? ");
		delimiter = ',';
	}

	if (field_mask & CP_STAT_NUM_DELETES) {
		query.append(1, delimiter);
		query.append("num_deletes=? ");
		delimiter = ',';
	}

	if (field_mask & CP_STAT_TOTAL_USAGE) {
		query.append(1, delimiter);
		query.append("storage_used=? ");
		delimiter = ',';
	}

	query.append(" where account=?");
}

void
cpool_stat_database_manager::set_account_stats_totals(
    const cpool_stat *value, const cpool_stat_account_key *key,
    const unsigned int field_mask, std::string &query,
    struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);
	ASSERT(key != NULL);
	ASSERT(value != NULL);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;
	int bind_index = 1;
	sqlite3_stmt *statement = NULL;

	if (field_mask == 0) {
		error = isi_system_error_new(EINVAL,
		    "Invalid field mask (%d).  No changes would be made",
		    field_mask);
		goto out;
	}

	sql_ret = sqlite3_prepare_v2(conn_, query.c_str(),
	    query.size(), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	if (field_mask & CP_STAT_BYTES_IN) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_bytes_in());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	if (field_mask & CP_STAT_BYTES_OUT) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_bytes_out());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	if (field_mask & CP_STAT_NUM_GETS) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_num_gets());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	if (field_mask & CP_STAT_NUM_PUTS) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_num_puts());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	if (field_mask & CP_STAT_NUM_DELETES) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_num_deletes());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	if (field_mask & CP_STAT_TOTAL_USAGE) {
		sql_ret = sqlite3_bind_int64(statement, bind_index++,
		    value->get_total_usage());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);
	}

	sql_ret = sqlite3_bind_int(statement, bind_index++,
	    key->get_account_key());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	/* Note* Do not start a transaction here - one is started by caller */
	sql_ret = sqlite3_step(statement);
	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_UPDATE_TOTALS, out);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::set_stats_totals(
    const std::vector<acct_stat*> &stats, const unsigned int field_mask,
    struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	std::string query;
	build_set_totals_query(field_mask, query);

	std::vector<acct_stat*>::const_iterator it;

	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	for (it = stats.begin(); it != stats.end(); ++it) {
		acct_stat *cur_acct_stat = (*it);

		const cpool_stat *value = cur_acct_stat->get_read_only_value();
		const cpool_stat_account_key *key = cur_acct_stat->get_key();
		ASSERT(key != NULL);
		ASSERT(value != NULL);

		/* Make sure a record exists for this account */
		check_record_exists_for_account_key(key, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/* Do the update */
		set_account_stats_totals(value, key, field_mask, query, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);

}

void
cpool_stat_database_manager::update_stats_history(struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;

	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	sql_ret = sqlite3_exec(conn_, HISTORY_TABLE_UPDATE_QUERY, NULL,
	    NULL, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_UPDATE_HISTORY,
	    out);

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::cull_stats_history(struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);

	struct isi_error *error = NULL;
	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	/*
	 * Do several deletes to make sure any records that might have been
	 * missed (e.g., due to nodes down or whatever) are caught - not
	 * foolproof, but effective
	 */

	/* Thin to one record for one month ago */
	DO_CULL_HISTORY_TABLE_BETWEEN(
	    "date('now', '-1 month')", "date('now', '-1 month', '1 day')",
	    out);

	/* Thin to one record for a month and a day ago */
	DO_CULL_HISTORY_TABLE_BETWEEN(
	    "date('now', '-1 month', '-1 day')", "date('now', '-1 month')",
	    out);

	/* Thin to one record for a month and two days ago */
	DO_CULL_HISTORY_TABLE_BETWEEN(
	    "date('now', '-1 month', '-2 day')",
		"date('now', '-1 month', '-1 day')",
	    out);

	/* Thin to one record for a month and three days ago */
	DO_CULL_HISTORY_TABLE_BETWEEN(
	    "date('now', '-1 month', '-3 day')",
		"date('now', '-1 month', '-2 day')",
	    out);

	COMMIT_TRANSACTION(conn_, error, out)
out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);
}


/**
 * This routine gets the CPOOL STATS VERSION of the DB.
 * This version is used to determine if the current cloudpools version
 * can access the DB or not.
 */
uint32_t
cpool_stat_database_manager::get_user_version(isi_error **error_out)
{
	isi_error *error = NULL;
	ASSERT(conn_ != NULL);
	uint32_t user_version_found = 0;

	// Get the Cpool Stats Version number.
	int sql_ret = SQLITE_OK;
	sqlite3_stmt *statement = NULL;

	sql_ret = sqlite3_prepare_v2(conn_, GET_USER_VERSION,
	    strlen(GET_USER_VERSION), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	// Check the version returned.
	/* Note* Do not start a transaction here - one is started by caller */
	sql_ret = sqlite3_step(statement);
	if (sql_ret == SQLITE_ROW) {
		user_version_found = sqlite3_column_int(statement, 0);
		sql_ret = sqlite3_step(statement);
	}

	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_GET_USER_VERSION, out);

out:
	sqlite3_finalize(statement);
        isi_error_handle(error, error_out);
	return user_version_found;
}


/**
 * This routine sets the CPOOL_STATS_VERSION as the database metadata user_version.
 * This indicates which version of cloudpools DB table structures are used.
 */

void
cpool_stat_database_manager::set_user_version(uint32_t user_version,
    isi_error **error_out)
{
	isi_error *error = NULL;
	ASSERT(conn_ != NULL);

	// Get the Cpool Stats Version number.
	// If the version is greater than zero, check for version compatibility.
	// else set the version number as the database is created newly.
	int sql_ret = SQLITE_OK;
	sqlite3_stmt *statement = NULL;

	std::stringstream db_usr_ver_query;
	db_usr_ver_query << SET_USER_VERSION;
	db_usr_ver_query << user_version;

	std::string vers_query = db_usr_ver_query.str();

	sql_ret = sqlite3_prepare_v2(conn_, vers_query.c_str(),
	    vers_query.size(), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	/* Note* Do not start a transactIon here - one is started by caller */
	sql_ret = sqlite3_step(statement);
	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_UPDATE_TOTALS, out);

out:
	sqlite3_finalize(statement);
        isi_error_handle(error, error_out);
}


/**
 * This routine is used to check and set the CPOOL_STATS_VERSION of the DB
 * tables and its structures.
 */
void
cpool_stat_database_manager::check_set_user_version(uint32_t user_version,
    isi_error **error_out)
{
	isi_error *error = NULL;
	ASSERT(conn_ != NULL);

	// Get the version if it is not queried yet.
	if (user_version_found_ <= 0) {
		user_version_found_ = get_user_version(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

        // The DB exists if the DB version is already set (greater than zero),
	// If the version is not greater than zero, then the DB was just
	// created and the version needs to be set.
        if (user_version_found_ > 0) {
		if (user_version != user_version_found_) {
			error = isi_system_error_new(EPROGMISMATCH,
			    "Cpool stats version mismatch. expected version = %d, "
			    "found version = %d", CPOOL_STATS_VERSION,
			    user_version_found_);
		}
	}
        else {
		set_user_version(user_version, &error);
	}

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::open_connection(const char *database_name,
    uint32_t user_version, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(conn_ == NULL);

	/* Open for reading & writing and create if it doesn't exist */
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;///////////要么打开,要么创建新的
	int ret_val = sqlite3_open_v2(database_name, &conn_, flags,  /////打开一个数据库
	    NULL);

	/*
	 * Can only get a NULL conn_ back if out of memory occurs.  Even
	 * failure to open will yield a non-NULL connection
	 */
	ASSERT(conn_ != NULL);

	CHECK_SQLITE_ERROR(ret_val, conn_, error, ERROR_OPEN_CONNECTION, out);

	/* Set a max time to wait for locks before SQLITE_BUSY is returned */
	ret_val = sqlite3_busy_timeout(conn_, SQL_WAIT_FOR_LOCK_TIMEOUT_MS);
	CHECK_SQLITE_ERROR(ret_val, conn_, error, ERROR_SET_TIMEOUT, out);

	// Get the user_version to check if this is a newly created database or
	// already existing one.
	// If this is already existing db, then check for version compatibility.
	check_set_user_version(user_version, &error);
	ON_ISI_ERROR_GOTO(out, error);

	create_database_schema(&error);

out:
	if (error != NULL)
		close_connection();

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::close_connection()
{
	if (conn_ != NULL) {
		sqlite3_close(conn_);
		conn_ = NULL;
	}
}

//int sqlite3_exec(sqlite3 *db, const char *sql, int(*callback)(void *arg, int, char *, char **),void *arg,char **errmsg);
void
cpool_stat_database_manager::create_database_schema(
    struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;

	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	// Create 'totals' table if not exists
	//account(主键),bytes_in,bytes_out,num_gets,num_puts,num_deletes,storage_used,storage_total
	sql_ret = sqlite3_exec(conn_,
	    CREATE_TABLE(TOTALS_TABLE) SCHEMA_TOTALS_TABLE,
	    NULL, NULL, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_CREATE_TOTALS, out);

	// Create 'history' table if not exists
	//account,time,bytes_in,bytes_out,num_gets,num_puts,num_deletes,storage_used,stoarge_total
	//(account,time)主键
	sql_ret = sqlite3_exec(conn_,
	    CREATE_TABLE(HISTORY_TABLE) SCHEMA_HISTORY_TABLE,
	    NULL, NULL, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_CREATE_HISTORY, out);

	// We need an index on the time column to avoid a full table scan for some queries
	//////对history_table创建一个历史时间的索引,索引名字:HISTORY_TIME_INDEX,索引选定的column:time
	sql_ret = sqlite3_exec(conn_,"create index if not exists "
	    HISTORY_TIME_INDEX " on " HISTORY_TABLE " (time);",NULL,NULL,NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error,
	    "Failed to create " HISTORY_TIME_INDEX ": %s", out);

	COMMIT_TRANSACTION(conn_, error, out);

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::get_change_in_stats_impl(
    const account_key_t acct_key, time_t start_time, time_t end_time,
    cpool_stat &ret_buffer, sqlite3_stmt *statement, struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);
	ASSERT(statement != NULL);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;

	cpool_stat begin_value, end_value;

	sql_ret = sqlite3_bind_int(statement, 1, acct_key);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int(statement, 2, end_time);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int(statement, 3, acct_key);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_bind_int(statement, 4, start_time);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_step(statement);
	if (sql_ret == SQLITE_ROW) {
		begin_value.set_bytes_in(sqlite3_column_int64(statement, 0));
		begin_value.set_bytes_out(sqlite3_column_int64(statement, 1));
		begin_value.set_num_gets(sqlite3_column_int64(statement, 2));
		begin_value.set_num_puts(sqlite3_column_int64(statement, 3));
		begin_value.set_num_deletes(sqlite3_column_int64(statement, 4));
		begin_value.set_total_usage(sqlite3_column_int64(statement, 5));
//		begin_value.set_storage_total(sqlite3_column_int64(statement, 6));

		sql_ret = sqlite3_step(statement);

		if (sql_ret == SQLITE_ROW) {
			end_value.set_bytes_in(
			    sqlite3_column_int64(statement, 0));
			end_value.set_bytes_out(
			    sqlite3_column_int64(statement, 1));
			end_value.set_num_gets(
			    sqlite3_column_int64(statement, 2));
			end_value.set_num_puts(
			    sqlite3_column_int64(statement, 3));
			end_value.set_num_deletes(
			    sqlite3_column_int64(statement, 4));
			end_value.set_total_usage(
			    sqlite3_column_int64(statement, 5));
//			end_value.set_storage_total(
//			    sqlite3_column_int64(statement, 6));

			sql_ret = sqlite3_step(statement);
		}
	}

	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_QUERY_STAT_CHANGE, out);

	/*
	 * Note - make sure to use 'abs' function.  Depending on which of two
	 * records came back, we could have a negative number in here
	 */
	ret_buffer.set_bytes_in(abs(
	    end_value.get_bytes_in() - begin_value.get_bytes_in()));

	ret_buffer.set_bytes_out(abs(
	    end_value.get_bytes_out() - begin_value.get_bytes_out()));

	ret_buffer.set_num_gets(abs(
	    end_value.get_num_gets() - begin_value.get_num_gets()));

	ret_buffer.set_num_puts(abs(
	    end_value.get_num_puts() - begin_value.get_num_puts()));

	ret_buffer.set_num_deletes(abs(
	    end_value.get_num_deletes() - begin_value.get_num_deletes()));

	ret_buffer.set_total_usage(abs(
	    end_value.get_total_usage() - begin_value.get_total_usage()));
//
//	ret_buffer.set_storage_total(abs(
//	    end_value.get_storage_total() - begin_value.get_storage_total()));

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::get_change_in_stats(
    const cpool_stat_account_key &acct_key, time_t start_time, time_t end_time,
    cpool_stat &ret_buffer, struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);

	struct isi_error *error = NULL;
	sqlite3_stmt *statement = NULL;
	int sql_ret = SQLITE_OK;

	sql_ret = sqlite3_prepare_v2(conn_, GET_CHANGE_IN_STATS_QUERY,
	    strlen(GET_CHANGE_IN_STATS_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	get_change_in_stats_impl(acct_key.get_account_key(), start_time,
	    end_time, ret_buffer, statement, &error);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::get_all_stats_results(
    const std::vector<const cpool_stat_account_query_result_key*> &queries,
    std::vector<acct_time_stat*> &ret_buffer, struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);
	ASSERT(ret_buffer.empty());

	struct isi_error *error = NULL;
	sqlite3_stmt *statement = NULL;
	int sql_ret = SQLITE_OK;
	time_t now(time(NULL));
	std::vector<const cpool_stat_account_query_result_key*>::const_iterator
	    it;

	BEGIN_READ_TRANSACTION(conn_, error, out)

	sql_ret = sqlite3_prepare_v2(conn_, GET_CHANGE_IN_STATS_QUERY,
	    strlen(GET_CHANGE_IN_STATS_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	for (it = queries.begin(); it != queries.end(); ++it) {

		cpool_stat value;
		const cpool_stat_account_query_result_key *query = (*it);

		ASSERT(query != NULL);

		sql_ret = sqlite3_reset(statement);
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_RESET_STATEMENT,
		    out);

		get_change_in_stats_impl(query->get_account_key(),
		    now - query->get_interval(), now, value,
		    statement, &error);
		ON_ISI_ERROR_GOTO(out, error);

		ret_buffer.push_back(new acct_time_stat(*query, value));
	}

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	sqlite3_finalize(statement);

	if (error != NULL) {
		std::vector<acct_time_stat*>::iterator it;
		for (it = ret_buffer.begin(); it != ret_buffer.end(); ++it) {
			delete (*it);
		}
		ret_buffer.clear();
	}

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::get_total_stats(
    const cpool_stat_account_key &acct_key, cpool_stat &ret_buffer,
    struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);

	struct isi_error *error = NULL;
	sqlite3_stmt *statement = NULL;
	cpool_stat begin_value, end_value;

	int sql_ret = SQLITE_OK;

	sql_ret = sqlite3_prepare_v2(conn_, GET_TOTAL_STATS_QUERY,
	    strlen(GET_TOTAL_STATS_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	sql_ret = sqlite3_bind_int(statement, 1, acct_key.get_account_key());
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

	sql_ret = sqlite3_step(statement);
	if (sql_ret == SQLITE_ROW) {
		ret_buffer.set_bytes_in(sqlite3_column_int64(statement, 0));
		ret_buffer.set_bytes_out(sqlite3_column_int64(statement, 1));
		ret_buffer.set_num_gets(sqlite3_column_int64(statement, 2));
		ret_buffer.set_num_puts(sqlite3_column_int64(statement, 3));
		ret_buffer.set_num_deletes(sqlite3_column_int64(statement, 4));
		ret_buffer.set_total_usage(sqlite3_column_int64(statement, 5));
//		ret_buffer.set_storage_total(sqlite3_column_int64(statement, 6));

		sql_ret = sqlite3_step(statement);
	}

	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_QUERY_STAT_CHANGE, out);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::delete_all_records(struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;

	BEGIN_WRITE_TRANSACTION(conn_, error, out)

	sql_ret = sqlite3_exec(conn_, DELETE_TOTALS_TABLE_QUERY, NULL,
	    NULL, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_DELETE_TOTALS_TABLE,
	    out);

	sql_ret = sqlite3_exec(conn_, DELETE_HISTORY_TABLE_QUERY, NULL,
	    NULL, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_DELETE_HISTORY_TABLE,
	    out);

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::prepopulate_stats_history(
    const cpool_stat &stat_per_interval, const std::vector<account_key_t> &keys,
    unsigned int num_entries, time_t time_spacing, struct isi_error **error_out)
{
	ASSERT(conn_);

	struct isi_error *error = NULL;
	int sql_ret = SQLITE_OK;
	time_t timestamp = time(NULL) - (time_spacing * num_entries);
	sqlite3_stmt *statement = NULL;
	cpool_stat total_stat;

	BEGIN_READ_TRANSACTION(conn_, error, out)

	sql_ret = sqlite3_prepare_v2(conn_, PREPOPULATE_HISTORY_QUERY,
	    strlen(PREPOPULATE_HISTORY_QUERY), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);
	for (unsigned int ent = 0; ent < num_entries; ent++) {

		total_stat.add(stat_per_interval);

		/* Bind non-account parameters outside of inner loop */
		sql_ret = sqlite3_bind_int(statement, 1, timestamp);
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER,
		    out);

		sql_ret = sqlite3_bind_int64(statement, 3,
		    total_stat.get_bytes_in());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		sql_ret = sqlite3_bind_int64(statement, 4,
		    total_stat.get_bytes_out());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		sql_ret = sqlite3_bind_int64(statement, 5,
		    total_stat.get_num_gets());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		sql_ret = sqlite3_bind_int64(statement, 6,
		    total_stat.get_num_puts());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		sql_ret = sqlite3_bind_int64(statement, 7,
		    total_stat.get_num_deletes());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		sql_ret = sqlite3_bind_int64(statement, 8,
		    total_stat.get_total_usage());
		CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);

		std::vector<account_key_t>::const_iterator key_it;
		for (key_it = keys.begin(); key_it != keys.end(); ++key_it) {

			sql_ret = sqlite3_bind_int(statement, 2, (*key_it));
			CHECK_SQLITE_ERROR(sql_ret, conn_, error,
			    ERROR_BIND_PARAMETER, out);

			sql_ret = sqlite3_step(statement);
			CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
			    ERROR_EXECUTE_PREPOPULATE, out);

			sql_ret = sqlite3_reset(statement);
			CHECK_SQLITE_ERROR(sql_ret, conn_, error,
			    ERROR_RESET_STATEMENT, out);
		}
		timestamp += time_spacing;
	}

	COMMIT_TRANSACTION(conn_, error, out)

out:
	ON_ISI_ERROR_ROLLBACK_TRANSACTION(conn_, error)

	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}

void
cpool_stat_database_manager::append_keys_to_sql_query(
    const std::vector<account_key_t> *account_keys, std::string &query_string)
{
	ASSERT(account_keys != NULL);

	std::vector<account_key_t>::const_iterator iter;
	char key_str_buffer[16];

	/*
	 * Start with a bogus account key to guard against empty account_key
	 * vector (Note, in the empty vector case, we should be returning a
	 * query that will yield an empty result set
	 */
	query_string.append(" (-9999");

	for (iter = account_keys->begin(); iter != account_keys->end(); ++iter)
	{
		sprintf(key_str_buffer, ", %d", *iter);
		query_string.append(key_str_buffer);
	}
	query_string.append(") ");
}

void////////telemetry_posting_thread_pool.cpp使用:从history table中读取数据,往/ifs/.ifsvar下面的report中写
cpool_stat_database_manager::get_all_data_for_accounts(
    std::vector<cpool_stat_time_record*> &stats_out,
    const unsigned int seconds_to_go_back,
    const std::vector<account_key_t> *account_keys,
    struct isi_error **error_out)
{
	ASSERT(conn_ != NULL);

	struct isi_error *error = NULL;
	sqlite3_stmt *statement = NULL;

	int sql_ret = SQLITE_OK;
	std::string query_string;

	/* No keys to filter on, return non-filtered query */
	if (account_keys == NULL) {
		query_string = GET_ALL_DATAPOINTS_QUERY_UNFILTERED;

	/*
	 * Otherwise, add the account keys to the query string in the form of
	 * a list (Note* there is no way to do bind a list with sqlite, so we
	 * need to manually build up the query string in this manner
	 */
	} else {
		// select account, time,  bytes_in, bytes_out, num_gets, num_puts, num_deletes, storage_used, storage_total
       // from cpool_stats_history_totals where time >= strftime('%s', 'now') - ?  and account in  (-9999, 1, 2, 3)  order by account
		query_string = GET_ALL_DATAPOINTS_QUERY_PREFIX;
		query_string.append(GET_ALL_DATAPOINTS_QUERY_ACCOUNT_FILTER);
		append_keys_to_sql_query(account_keys, query_string);
		query_string.append(GET_ALL_DATAPOINTS_QUERY_SUFFIX);
	}

	sql_ret = sqlite3_prepare_v2(conn_, query_string.c_str(),
	    query_string.length(), &statement, NULL);
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_PREPARE_STATEMENT,
	    out);

	///select account, time, bytes_in, bytes_out,.... from history_table where time >= strftime('%s', 'now') - ? 
	////? 为传入的参数:seconds_to_go_back
	sql_ret = sqlite3_bind_int64(statement, 1, seconds_to_go_back); ///// where time >= now - seconds_to_go_back
	CHECK_SQLITE_ERROR(sql_ret, conn_, error, ERROR_BIND_PARAMETER, out);
	///sqlite_row:这个比较常用,当sql语句是读命令,比如"select*from...",返回的数据很多行.重点:每次只返回一行,并且
	///函数的返回值为SQLITE_ROW,所以需要重复调用sqlite3_step函数,当所有数据返回后,sqlite3_step返回 SQLITE_DONE
	for (sql_ret = sqlite3_step(statement); sql_ret == SQLITE_ROW; //每个循环中做sqlite3_step
	    sql_ret = sqlite3_step(statement)) {

		account_key_t acct_key;
		cpool_stat stat;
		time_t timestamp;

		acct_key = sqlite3_column_int(statement, 0); ////从statement中获得第0列： acct_key
		timestamp = sqlite3_column_int(statement, 1);///从statement中获得第1列: timestamp
		stat.set_bytes_in(sqlite3_column_int64(statement, 2)); ///////从statement中获得第2列的值,设置bytes_in
		stat.set_bytes_out(sqlite3_column_int64(statement, 3));
		stat.set_num_gets(sqlite3_column_int64(statement, 4));
		stat.set_num_puts(sqlite3_column_int64(statement, 5));
		stat.set_num_deletes(sqlite3_column_int64(statement, 6));
		stat.set_total_usage(sqlite3_column_int64(statement, 7));

		stats_out.push_back( /////因为每条过滤的数据需要push到stats_out,所以采用sqlite3_step,而不是作为一个整体去执行
		    new cpool_stat_time_record(stat, acct_key, timestamp));
	}
	////sqlite_done:意味着sql语句执行完成,并且成功.一旦执行成功,sqlite3_step()就不应该被再次调用执行.除非我们使用sqlite3_reset()重置sqlite3_stmt数据
	CHECK_SQLITE_RESULT(sql_ret, SQLITE_DONE, conn_, error,
	    ERROR_QUERY_STAT_CHANGE, out);

out:
	sqlite3_finalize(statement);

	isi_error_handle(error, error_out);
}
