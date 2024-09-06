#include <vector>
#include "cpool_stat_manager.h"
#include "cpool_stat_persistence_manager.h"

#define SHARED_MAP_NAME "cpool_stat_map_"
#define TEST_SHARED_MAP_NAME "test_cpool_stat_map_"

#define SHARED_QUERY_MAP_NAME "cpool_query_stat_map_"
#define TEST_SHARED_QUERY_MAP_NAME "test_cpool_query_stat_map_"

#define ASSERT_PROPERLY_INITIALIZED(mgr) \
	ASSERT((mgr) != NULL, "Manager is NULL"); \
	ASSERT((mgr)->stat_map_ != NULL, "Stat map is NULL"); \
	ASSERT((mgr)->stat_query_map_ != NULL, "Stat query map is NULL");

cpool_stat_manager *
cpool_stat_manager::get_new_instance(struct isi_error **error_out)
{
	return get_new_instance_impl(false, error_out);
}

cpool_stat_manager *
cpool_stat_manager::get_new_test_instance(struct isi_error **error_out)
{
	return get_new_instance_impl(true, error_out);
}

cpool_stat_manager *
cpool_stat_manager::get_new_instance_impl(bool for_test,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_manager *mgr = new cpool_stat_manager();
	ASSERT(mgr);

	const char *map_name =  ////"cpool_stat_map_"
	    (for_test ? TEST_SHARED_MAP_NAME : SHARED_MAP_NAME);
	const char *query_map_name =
	    (for_test ? TEST_SHARED_QUERY_MAP_NAME : SHARED_QUERY_MAP_NAME);

	mgr->stat_map_ =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(map_name, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mgr->stat_query_map_ =
	    cpool_stat_shared_map<cpool_stat_account_query_result_key, cpool_stat>::
		get_instance(query_map_name, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mgr->is_test_instance_ = for_test;

	/*
	 * In test scenarios, pre-create database manager to avoid false
	 * positive memory leaks
	 */
	if (for_test) {
		mgr->get_persistence_manager(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	ASSERT_PROPERLY_INITIALIZED(mgr);
out:
	if (error != NULL) {
		delete mgr;
		mgr = NULL;
	}

	isi_error_handle(error, error_out);

	return mgr;
}

cpool_stat_manager::cpool_stat_manager() : stat_map_(NULL),
    stat_query_map_(NULL), persistence_mgr_(NULL)
{
}

cpool_stat_manager::~cpool_stat_manager()
{
	if (persistence_mgr_)
		delete persistence_mgr_;
}

void  ////libcurl上传下载完成后调用的回调函数(isi_cpool_stats/isi_cpool_stats_api_cb.cpp中的void update_stats_operation(void *in_cb_ctx,const cloud_api_data &data))
cpool_stat_manager::add_to_stats(account_key_t acct_key,
    const cpool_stat &delta, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat *stat_ptr = NULL;
	const cpool_stat_account_key map_key(acct_key);

	stat_map_->lock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	////先遍历一下整个stat_map中的acct_key是否存在,不存在就insert
	stat_ptr = stat_map_->at(map_key, &error);////这个key就是account_key

	/* The entry didn't exist, add it now */
	if (error && isi_system_error_is_a(error, ENOENT)) {
		isi_error_free(error);
		error = NULL;

		stat_map_->insert(map_key, delta, &error);

	/* Successfully found existing stat - add to it */
	////如果存在这个account_key,就找到那个slot,修改这个slot的cpool_stat
	} else if (error == NULL) {
		ASSERT(stat_ptr != NULL);

		stat_ptr->add(delta);////这个stat_ptr指向了stat_map_中的那个cpool_stat,所以可以直接修改
	}

	if (error == NULL)
		stat_map_->unlock_map(&error);
	else
		stat_map_->unlock_map(isi_error_suppress(IL_NOTICE));

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::set_current_stats(const std::vector<acct_stat*> &stats,/////用不到
    const unsigned int field_mask, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->set_stats_totals(stats, field_mask, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void////将cpool_stat_map_中的内存数据持久化到total_table中
cpool_stat_manager::save_current_stats(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	bool map_locked = false;
	struct isi_error *error = NULL;
	std::vector<acct_stat*> stats, stats_copy;///acct_stat就是cpool_stat_list_element
	std::vector<acct_stat*>::iterator it;

	stat_map_->lock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked = true;

	stat_map_->as_vector(stats, &error);
	ON_ISI_ERROR_GOTO(out, error);


	/*
	 * For each element in vector - copy value and reset to 0.  Note, we do
	 * a copy here so we can safely release the shared memory lock before
	 * calling to persistence manager - the alternative is to keep shared
	 * memory lock while persistence manager is serializing (potentially a
	 * long time)
	 */
	///把stat_map_中的stats拷贝出来，这样可以先释放锁，再做持久化。
	//不把stats拷贝出来，那么需要在持久化后，再释放锁，时间可能会长。
	for (it = stats.begin(); it != stats.end(); ++it) {


		const cpool_stat_account_key *curr_key = (*it)->get_key();
		cpool_stat *curr_stat = (*it)->get_value();
		ASSERT(curr_key != NULL);
		ASSERT(curr_stat != NULL);

		/* Don't bother to record stats with no changes */
		if (curr_stat->is_zero())
			continue;

		stats_copy.push_back(new acct_stat(*curr_key, *curr_stat));
		curr_stat->clear();////stats拷贝出来后, 将这个stat的bytes_in bytes_out,...重新设为0
	}

	stat_map_->unlock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked = false;

	if (stats_copy.size() > 0) {
		cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
		////持久化到total_table中
		p_mgr->add_to_stats_totals(stats_copy, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	if (map_locked) {
		stat_map_->unlock_map(
		    (error == NULL) ? &error : isi_error_suppress(IL_NOTICE));
	}

	/* Cleanup stats_copy */
	for (it = stats_copy.begin(); it != stats_copy.end(); ++it)
		delete (*it);
	stats_copy.clear();

	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::update_stats_history(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->update_stats_history(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::get_total_stats(account_key_t acct_key, /////用不到,测试使用
    cpool_stat &ret_buffer, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	const cpool_stat_account_key map_key(acct_key);

	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->get_total_stats(map_key, ret_buffer, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::get_change_in_stats(account_key_t acct_key,
    time_t start_time, time_t end_time, cpool_stat &ret_buffer,
    struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;

	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->get_change_in_stats(acct_key, start_time, end_time,
	    ret_buffer, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:

	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::cleanup_test_instance(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;

	stat_map_->reset_after_test(&error);

	stat_query_map_->reset_after_test(&error);

	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::cleanup_for_test()
{
	cpool_stat_persistence_manager::cleanup_for_test();
}

void
cpool_stat_manager::clear_all_stats(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = NULL;

	stat_map_->clear(&error);
	ON_ISI_ERROR_GOTO(out, error);

	stat_query_map_->clear(&error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->delete_all_records(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::register_cached_query(const account_key_t acct_key,
    unsigned int time_delta, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	const cpool_stat_account_query_result_key key(acct_key, time_delta);

	const cpool_stat *existing_value = stat_query_map_->find(key, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* If key didn't previously exist, add it now */
	if (existing_value == NULL) {
		/* Get first cached value */
		cpool_stat first_value;
		cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		p_mgr->get_change_in_stats(
		    cpool_stat_account_key(acct_key), time(NULL) - time_delta,
		    first_value, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/* Add to the map */
		stat_query_map_->insert(key, first_value, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::deregister_cached_query(const account_key_t acct_key,
    unsigned int time_delta, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	const cpool_stat_account_query_result_key key(acct_key, time_delta);

	stat_query_map_->erase(key, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::get_cached_query_result(const account_key_t acct_key,
    unsigned int time_delta, cpool_stat &ret_buffer,
    struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	const cpool_stat *existing_value = NULL;
	const cpool_stat_account_query_result_key key(acct_key, time_delta);
	bool map_locked = false;

	stat_query_map_->lock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked = true;

	existing_value = stat_query_map_->at(key, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * Existing_value should not be NULL, if key didn't exist, 'at' should
	 * have populated 'error' and we wouldn't have gotten here
	 */
	ASSERT(existing_value != NULL);
	ret_buffer.copy(*existing_value);

out:
	if (map_locked) {
		if (error == NULL)
			stat_query_map_->unlock_map(&error);
		else
			stat_query_map_->unlock_map(
			    isi_error_suppress(IL_NOTICE));
	}

	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::clear_cached_queries(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;

	stat_query_map_->clear(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::execute_cached_queries(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	std::vector<const cpool_stat_account_query_result_key*> queries;
	std::vector<acct_time_stat*> result_buffer;
	std::vector<acct_time_stat*>::iterator it;
	cpool_stat_persistence_manager *p_mgr = NULL;
	bool map_locked = false;

	stat_query_map_->lock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked = true;

	/* Get all "registered" queries (i.e., keys) from stat_query_map_ */
	stat_query_map_->keys_as_vector(queries, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Execute all returned queries against the database */
	p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->get_all_stats_results(queries, result_buffer, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Cache each query result in stat_query_map_ */
	for (it = result_buffer.begin(); it != result_buffer.end(); ++it) {
		acct_time_stat *cur_result = *it;

		ASSERT(cur_result->get_key() != NULL);

		/*
		 * There really shouldn't be any failures here, but if there
		 * are, we don't want them to disrupt populating remaining stats
		 */
		cpool_stat *mapped_stat =
		    stat_query_map_->at(*cur_result->get_key(), &error);
		if (error != NULL) {
			ilog(IL_ERR, "Failed to find key in map: %#{}",
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}

		if (mapped_stat != NULL) {
			ASSERT(cur_result->get_value() != NULL);
			mapped_stat->copy(*cur_result->get_value());
		}
	}
out:
	if (map_locked)
		stat_query_map_->unlock_map(
		    error == NULL ? &error : isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::cull_old_stats(struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->cull_stats_history(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::prepopulate_stats_history(
    const cpool_stat &stat_per_interval, const std::vector<account_key_t> &keys,
    unsigned int num_entries, time_t time_spacing, struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->prepopulate_stats_history(stat_per_interval, keys,
	    num_entries, time_spacing, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

cpool_stat_persistence_manager*
cpool_stat_manager::get_persistence_manager(struct isi_error **error_out)
{
	if (persistence_mgr_ == NULL) {

		struct isi_error *error = NULL;

		if (!is_test_instance_)
			persistence_mgr_ =
			    cpool_stat_persistence_manager::get_new_instance(
				&error);
		else
			persistence_mgr_ = cpool_stat_persistence_manager::
			    get_new_test_instance(&error);

		if (error != NULL) {
			ASSERT(persistence_mgr_ == NULL);
			isi_error_handle(error, error_out);
		}
	}

	return persistence_mgr_;
}

void  /////从history_table中获取cpool_stat telemetry_post
cpool_stat_manager::get_all_data_for_accounts(
    std::vector<cpool_stat_time_record*> &stats_out,
    const unsigned int seconds_to_go_back,
    const std::vector<account_key_t> *account_keys,
    struct isi_error **error_out)
{
	ASSERT_PROPERLY_INITIALIZED(this);

	struct isi_error *error = NULL;
	cpool_stat_persistence_manager *p_mgr = get_persistence_manager(
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	p_mgr->get_all_data_for_accounts(stats_out, seconds_to_go_back,
	    account_keys, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_manager::preallocate_space_for_tests(int num_potential_accounts,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	stat_map_->preallocate_space_for_tests(num_potential_accounts, &error);
	ON_ISI_ERROR_GOTO(out, error);

	stat_query_map_->preallocate_space_for_tests(num_potential_accounts,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}
