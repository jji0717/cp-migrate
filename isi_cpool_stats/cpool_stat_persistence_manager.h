#ifndef __ISI_CPOOL_STAT_PERSISTENCE_MANAGER_H__
#define __ISI_CPOOL_STAT_PERSISTENCE_MANAGER_H__

#include <vector>

#include <isi_cloud_common/isi_cpool_version.h>

#include "cpool_stat.h"
#include "cpool_stat_key.h"
#include "cpool_stat_shared_map.h"

typedef cpool_stat_list_element<cpool_stat_account_key, cpool_stat> acct_stat;
typedef cpool_stat_list_element<cpool_stat_account_query_result_key, cpool_stat>
    acct_time_stat;

class cpool_stat_time_record {  ///////只用在telemetry_posting中.从history表中拿到account,timestamp, bytes_in, .....
public:
	cpool_stat_time_record(const cpool_stat &stat,
	    const cpool_stat_account_key &account_key, const time_t timestamp) :
	    stat_(stat), acct_key_(account_key), timestamp_(timestamp) {};

	const cpool_stat &get_stats() const { return stat_; };
	const cpool_stat_account_key &get_key() const { return acct_key_; };
	const time_t get_timestamp() const { return timestamp_; };

private:
	cpool_stat stat_; /////每个account中的stat作为value
	cpool_stat_account_key acct_key_;////account作为key
	time_t timestamp_;
};

/**
 * NOTE* One instance CANNOT be shared by multiple threads.  DB Connections
 * are not inherently thread safe
 */
class cpool_stat_persistence_manager {
public:
	static cpool_stat_persistence_manager *get_new_instance(
	    struct isi_error **error_out);

	virtual ~cpool_stat_persistence_manager() {};

	/* Update functions */
	virtual void add_to_stats_totals(
	    const std::vector<acct_stat*> &stats,
	    struct isi_error **error_out) = 0;

	virtual void set_stats_totals(/////用不到
	    const std::vector<acct_stat*> &stats, const unsigned int field_mask,
	    struct isi_error **error_out) = 0;

	/////lib/isi_cpool_d/stats_history_thread.cpp调用.一个cluster中只有一个node中的一个线程执行
	virtual void update_stats_history(struct isi_error **error_out) = 0;
	////stats_history_thread.cpp中调用.把history表中旧的数据删掉
	virtual void cull_stats_history(struct isi_error **error_out) = 0;

	/* Query functions */ ///用不到
	virtual void get_change_in_stats(const cpool_stat_account_key &acct_key,
	    time_t start_time, time_t end_time, cpool_stat &ret_buffer,
	    struct isi_error **error_out) = 0;
	inline void get_change_in_stats(const cpool_stat_account_key &acct_key,
	    time_t start_time, cpool_stat &ret_buffer,
	    struct isi_error **error_out)
	{
		get_change_in_stats(acct_key, start_time, time(NULL),
		    ret_buffer, error_out);
	};
	virtual void get_total_stats(const cpool_stat_account_key &acct_key,
	    cpool_stat &ret_buffer, struct isi_error **error_out) = 0;
	virtual void get_all_stats_results(
	    const std::vector<const cpool_stat_account_query_result_key*> &queries,
	    std::vector<acct_time_stat*> &ret_buffer,
	    struct isi_error **error_out) = 0;

	virtual void delete_all_records(struct isi_error **error_out) = 0;

	/* Test utilities */
	static cpool_stat_persistence_manager *get_new_test_instance(
	    struct isi_error **error_out);
	static void cleanup_for_test();

	virtual void prepopulate_stats_history(
	    const cpool_stat &stat_per_interval,
	    const std::vector<account_key_t> &keys, unsigned int num_entries,
	    time_t time_spacing, struct isi_error **error_out) = 0;

	virtual void get_all_data_for_accounts(
	    std::vector<cpool_stat_time_record*> &stats_out,
	    const unsigned int seconds_to_go_back,
	    const std::vector<account_key_t> *account_keys,
	    struct isi_error **error_out) = 0;

	virtual uint32_t get_user_version(struct isi_error **error_out) = 0;

	virtual void set_user_version(uint32_t user_version,
	    struct isi_error **error_out) = 0;

protected:
	// Protected constructor to hide implementation.  Use create_manager to
	//    get a new instance
	cpool_stat_persistence_manager() {};
};

#endif // __ISI_CPOOL_STAT_PERSISTENCE_MANAGER_H__
