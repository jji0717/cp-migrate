#ifndef __ISI_CPOOL_STAT_MANAGER_H__
#define __ISI_CPOOL_STAT_MANAGER_H__

#include "cpool_stat.h"
#include "cpool_stat_key.h"
#include "cpool_stat_shared_map.h"
#include "cpool_stat_persistence_manager.h"

/**
 * This class is the "glue" between all disparate pieces of cpool_stats.  It
 * combines the shared memory and persistence layers via a single interface
 */
/////cpool_stat_manager里面包含了一个共享内存(stat_shared_map_),用来存放libcurl上传下载删除后回调函数返回的stat
////stats_delta_thread.cpp调用add_to_stats_totals把stat_map_中的stat持久化到total_table
///stats_history_thread.cpp调用update_stats_history:把total_table的stat添加time,持久化到history_table
///telemetry_posting_thread_pool.cpp调用write_stats_data_to_report把history_table中的stat写进/ifs/data/Isilon_Support/pkg/report
class cpool_stat_manager {
public:
	virtual ~cpool_stat_manager();

	/**
	 * Returns a new instance of cpool_stat_manager
	 * Consumer is responsible for deallocating when done
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 * @return  A new instance of cpool_stat_manager
	 */
	static cpool_stat_manager *get_new_instance(
	    struct isi_error **error_out);

	/**
	 * Returns a new instance of cpool_stat_manager suitable for testing
	 * (e.g., modifies only test copies of memory/serialization structures
	 * rather than the main structures used in live code)
	 * Consumer is responsible for deallocating when done
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 * @return  A new instance of cpool_stat_manager
	 */
	static cpool_stat_manager *get_new_test_instance(
	    struct isi_error **error_out);

	/**
	 * Add the values of delta stats to existing in memory stats for the
	 * given key.
	 *
	 * @param acct_key  A unique key identifying which stats should be
	 *                  modified in memory
	 * @param delta  The change in stats to be added (e.g., summed) to the
	 *               existing in-memory values
	 * @param error_out  Standard Isilon error_out parameter
	 */
	////libcurl上传下载删除完成后调用的回调函数 stats_delta_thread.cpp调用
	void add_to_stats(account_key_t acct_key,
	    const cpool_stat &delta, struct isi_error **error_out);

	/**
	 * Sets (overwrites) the values of stats in persistent storage.  Only
	 * modifies stats indicated by field_mask.
	 * Note * Writes directly to persistent memory and is consequently much
	 * less performant than other stats write operations.
	 *
	 * @param stats  A vector of account/stat pairs to be overwritten in
	 *              persistent storage
	 * @param field_mask  A bit mask indicating which fields should be
	 *                    written to persistent storage.  Other fields are
	 *                    ignored.  E.g.,
	 *                        (CP_STAT_NUM_PUTS | CP_STAT_NUM_GETS)
	 * @param error_out  Standard Isilon error_out parameter
	 */
	////telemetry_collecting使用，RAN可以忽略不计
	void set_current_stats(const std::vector<acct_stat*> &stats,
	    const unsigned int field_mask, struct isi_error **error_out);

	/**
	 * Reads current stats from in-memory structures and saves them to
	 * persistent storage
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	////把stat_map_中的内存stats持久化到total_table. stats_delta_thread.cpp
	void save_current_stats(struct isi_error **error_out);

	/**
	 * Copies most recently serialized stats along with a timestamp to a
	 * historical table of stats
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	///将total table中的数据全部读出，加上current time，一起插入到history table. 
	///在isi_cpool_d/stats_history_thread.cpp中使用
	void update_stats_history(struct isi_error **error_out);

	/**
	 * "Thins out" old records, leaving only one record per day for history
	 * more than a month old
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	////stats_history_thread.cpp中将旧的stats从db中删除
	void cull_old_stats(struct isi_error **error_out);

	/**
	 * Gets the change in stats for the given account between the given
	 * times
	 *
	 * @param acct_key  A unique key referring to which stats should be
	 *                  returned
	 * @param start_time  The start time of the change in stats
	 * @param end_time  The end time of the change in stats
	 * @param ret_buffer [out]  An instance of cpool_stat to receive the
	 *                          persisted values
	 * @param error_out  Standard Isilon error_out parameter
	 */
	///////用不到//////
	void get_change_in_stats(account_key_t acct_key, time_t start_time,
	    time_t end_time, cpool_stat &ret_buffer,
	    struct isi_error **error_out);

	/**
	 * Gets the total stats values for the given account key
	 *
	 * @param acct_key  A unique key referring to which stats should be
	 *                  returned
	 * @param ret_buffer [out]  An instance of cpool_stat to receive the
	 *                          persisted values
	 * @param error_out  Standard Isilon error_out parameter
	 */
	//////用不到，测试验证使用//////
	void get_total_stats(account_key_t acct_key, cpool_stat &ret_buffer,
	    struct isi_error **error_out);

	/**
	 * Registers a given database query to be held in cache and
	 * periodically updated
	 *
	 * @param acct_key  A unique key referring to the account which should
	 *                  be queried
	 * @param time_delta  Number seconds in history to query.  (E.g., query
	 *                    change in stats between current time and 120
	 *                    seconds earlier)
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void register_cached_query(const account_key_t acct_key,
	    unsigned int time_delta, struct isi_error **error_out);

	/**
	 * Removes a registered query (see register_cached_query)
	 *
	 * @param acct_key  A unique key referring to the account being queried
	 * @param time_delta  Number seconds in history being queried.
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void deregister_cached_query(const account_key_t acct_key,
	    unsigned int time_delta, struct isi_error **error_out);

	/**
	 * Removes all registered queries
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void clear_cached_queries(struct isi_error **error_out);

	/**
	 * Executes all cached queries and holds their results in memory
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void execute_cached_queries(struct isi_error **error_out);

	/**
	 * Returns the most recently cached query result for the given account
	 * key and time_delta.
	 *
	 * @param acct_key  A unique key referring to the account being queried
	 * @param  time_delta  Number of seconds in history that should have
	 *                     been queried  (E.g., query change in stats
	 *                     between current time and 120 seconds earlier)
	 * @param ret_buffer  A cpool_stat instance to hold the result
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void get_cached_query_result(const account_key_t acct_key,
	    unsigned int time_delta, cpool_stat &ret_buffer,
	    struct isi_error **error_out);

	/**
	 * Should *only* be used for testing.  Erases in-memory structures
	 * leveraged for this test instance
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	void cleanup_test_instance(struct isi_error **error_out);

	/**
	 * Should *only* be used for testing.  A static cleanup function for
	 * removing persistent storage created by all test instances.
	 */
	static void cleanup_for_test();

	/**
	 * Destroys all stored stats.  Ideally, should only be used in test
	 * scenarios
	 *
	 * @param error_out  Standard Isilon error_out parameter
	 */
	/////用在测试环境
	void clear_all_stats(struct isi_error **error_out);

	/**
	 * Prepopulates historical stats num_entries times with given
	 * information.  Ideally only called in test scenarios
	 *
	 * @param stat_per_interval  The value of the stat to appear in each
	 *                           history record
	 * @param keys  A vector of account keys.  Each will have a record
	 *              created for each time step
	 * @param num_entries  The number of entries to be added to history
	 * @param time_spacing  The time (in seconds) between subsequent records
	 * @param error_out  Standard Isilon error_out parameter
	 */
	///测试环境
	void prepopulate_stats_history(const cpool_stat &stat_per_interval,
	    const std::vector<account_key_t> &keys, unsigned int num_entries,
	    time_t time_spacing, struct isi_error **error_out);

	/**
	 * Returns all data points in the last seconds_to_go_back related to
	 * all accounts
	 *
	 * @param stats_out[out]  A vector of cpool_stat_time_record instances
	 *                        to hold the returned datapoints
	 * @param seconds_to_go_back  The number of seconds in history to report
	 *                            on (e.g., returns data collected in the
	 *                            last N seconds where
	 *                            N === seconds_to_go_back)
	 * @param account_keys  A pointer to a vector of account keys which
	 *                      should be included in stats_out.  A NULL value
	 *                      means to include all account keys.
	 * @param error_out  Standard Isilon error_out parameter
	 */
	///获取31天(默认)前,所有accounts的数据(cpool_stat)
	void get_all_data_for_accounts(
	    std::vector<cpool_stat_time_record*> &stats_out,
	    const unsigned int seconds_to_go_back,
	    const std::vector<account_key_t> *account_keys,
	    struct isi_error **error_out);

	/**
	 * For testing purposes
	 * Preallocates storage space for the given number of accounts to avoid
	 * false positive memory leaks in shared memory
	 *
	 * @param num_potential_accounts  The number of accounts which should
	 *                                have available stats space
	 */
	void preallocate_space_for_tests(int num_potential_accounts,
	    struct isi_error **error_out);

private:
	/**
	 * Private constructor for factory implementation
	 */
	cpool_stat_manager();

	/**
	 * A shared implementation of the get_new_instance and
	 * get_new_test_instance functions
	 */
	static cpool_stat_manager *get_new_instance_impl(bool for_test,
	    struct isi_error **error_out);

	cpool_stat_persistence_manager* get_persistence_manager(
	    struct isi_error **error_out);

	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *stat_map_;/////重载<的类或结构体才能作为map的key
	cpool_stat_shared_map<cpool_stat_account_query_result_key, cpool_stat>
	    *stat_query_map_;///////用不到
	cpool_stat_persistence_manager *persistence_mgr_;
	bool is_test_instance_;
};

#endif // __ISI_CPOOL_STAT_MANAGER_H__
