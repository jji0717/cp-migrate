#include <sys/stat.h>

#include <isi_util_cpp/check_leak_primer.hpp>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat_persistence_manager.h"

// N.b., In the build system, the false positives should be 0, locally we get 80
#define EXPECTED_FALSE_POSITIVE_MEM_LEAKS 0

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_persistence,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 90,
    .fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;

	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	delete mgr;

	check_leak_primer();
}

TEST_FIXTURE(setup_test)
{
	cpool_stat_persistence_manager::cleanup_for_test();
	//printf("%s called\n", __func__);
}

TEST_FIXTURE(teardown_test)
{
}

TEST(test_persistence_smoke_create,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	delete mgr;
}

///////往db中插入一行<account_key, cpool_stat>
TEST(test_persistence_smoke_add_to_stats_totals,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Create and push a list of stats */
	std::vector<acct_stat*> stat_list;  /////acct_stat是一个pair类型<account,cpool_stat>
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	stat_list.push_back(new acct_stat(key, stat));
	cpool_stat_account_key key1(1);
	cpool_stat stat1(3,4,5,6,7,8);

	stat_list.push_back(new acct_stat(key1, stat1));
	mgr->add_to_stats_totals(stat_list, &error);/////往db中插入一行<account,cpool_stat>
	fail_if(error != NULL, "Error inserting into persistence: %{}",
	    isi_error_fmt(error));

	/* Clean up */
	delete mgr;
	std::vector<acct_stat*>::iterator it;
	for (it = stat_list.begin(); it != stat_list.end(); it++)
		delete(*it);
	stat_list.clear();
}
// TEST(test_persistence_update_stats_history)
// {
// 	struct isi_error *error = NULL;
// 	/* Create a persistence manager */
// 	cpool_stat_persistence_manager *mgr =
// 	    cpool_stat_persistence_manager::get_new_test_instance(&error);
// 	cpool_stat_account_key k1(7),k2(77);
// 	cpool_stat stat1(1,2,3,4,5,6),stat2(11,12,13,14,15,16);
// 	std::vector<acct_stat*> stat_list;
// 	stat_list.push_back(new acct_stat(k1,stat1));
// 	stat_list.push_back(new acct_stat(k2,stat2));
// 	mgr->add_to_stats_totals(stat_list,&error);
// 	//////update time: copy data from total table to history table
// 	mgr->update_stats_history(&error);
// 	delete mgr;
// }
TEST(test_persistence_smoke_update_stats_history,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Test stats history update */
	mgr->update_stats_history(&error);  /////选出total table中的所有数据，并且为每条加上当前插入的时间，插入history table
	fail_if(error != NULL, "Error updating stats history table: %{}",
	    isi_error_fmt(error));

	/* Clean up */
	delete mgr;
}

TEST(test_get_total_stats,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;
	const int num_test_updates = 10;
	cpool_stat query_buffer;

	/* Create a list of stats for pushing into persistence */
	std::vector<acct_stat*> stat_list;
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	cpool_stat expected_results(
		1 * num_test_updates,
		2 * num_test_updates,
		3 * num_test_updates,
		4 * num_test_updates,
		5 * num_test_updates,
		6 * num_test_updates
	);

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Verify current totals start at 0 (and don't fail with no records) */
	mgr->get_total_stats(key, query_buffer, &error); ////给定指定account，从history table中按照时间降序选出一条记录(即最新的一条记录)
	fail_if(error != NULL, "Failed to query for totals: %#{}",
	    isi_error_fmt(error));
	fail_if(query_buffer.get_bytes_in() != 0,
	    "Incorrect value.  Expected 0, got %d",
	    query_buffer.get_bytes_in());

	/* Populate persistence manager */
	stat_list.push_back(new acct_stat(key, stat));

	for (int i = 0; i < num_test_updates; i++) {
		/* Add a short sleep to ensure timestamps are unique */
		sleep(1);

		mgr->add_to_stats_totals(stat_list, &error);////针对同一个account,把对应的cpool_stat累加上去
		fail_if(error != NULL,
		    "Error inserting into persistence (%d): %{}", i,
		    isi_error_fmt(error));

		// mgr->update_stats_history(&error);
		// fail_if(error != NULL,
		//     "Error updating history table persistence (%d): %{}", i,
		//     isi_error_fmt(error));
	}
	mgr->update_stats_history(&error);
	return;
	/* Done with stat list */
	std::vector<acct_stat*>::iterator it;
	for (it = stat_list.begin(); it != stat_list.end(); it++)
		delete(*it);
	stat_list.clear();

	/* Grab current totals */
	mgr->get_total_stats(key, query_buffer, &error);
	fail_if(error != NULL, "Failed to query for totals: %#{}",
	    isi_error_fmt(error));

	/* Verify expected return values */
	fail_if(query_buffer.get_bytes_in() != expected_results.get_bytes_in(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_bytes_in(), query_buffer.get_bytes_in());

	fail_if(query_buffer.get_bytes_out() != expected_results.get_bytes_out(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_bytes_out(), query_buffer.get_bytes_out());

	fail_if(query_buffer.get_num_puts() != expected_results.get_num_puts(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_num_puts(), query_buffer.get_num_puts());

	fail_if(query_buffer.get_num_gets() != expected_results.get_num_gets(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_num_gets(), query_buffer.get_num_gets());

	fail_if(query_buffer.get_num_deletes() != expected_results.get_num_deletes(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_num_deletes(), query_buffer.get_num_deletes());

	fail_if(
	    query_buffer.get_total_usage() != expected_results.get_total_usage(),
	    "Incorrect value.  Expected %d, got %d",
	    expected_results.get_total_usage(), query_buffer.get_total_usage());

	/* Clean up */
	delete mgr;
}

TEST(test_get_change_in_stats,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;
	const int num_test_updates = 10;
	cpool_stat query_buffer;
	time_t start_time, end_time, midpt_time;

	/* Create a list of stats for pushing into persistence */
	std::vector<acct_stat*> stat_list;
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	start_time = time(NULL);

	/* Verify stats changes start at 0 (and don't fail with no records) */
	mgr->get_change_in_stats(key, start_time, query_buffer, &error);
	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	fail_if(query_buffer.get_bytes_in() != 0,
	    "Incorrect value.  Expected 0, got %d",
	    query_buffer.get_bytes_in());

	/* Populate persistence manager */
	stat_list.push_back(new acct_stat(key, stat));

	for (int i = 0; i < num_test_updates; i++) {
		/* Add a short sleep to ensure timestamps are unique */
		sleep(1);

		mgr->add_to_stats_totals(stat_list, &error);
		fail_if(error != NULL, "Error inserting into persistence: %{}",
		    isi_error_fmt(error));

		mgr->update_stats_history(&error);
		fail_if(error != NULL,
		    "Error updating history table persistence: %{}",
		    isi_error_fmt(error));

		if (i == (num_test_updates/2) - 1)
			midpt_time = time(NULL);
	}
	sleep(1);

	end_time = time(NULL);

	/* Done with stat list */
	std::vector<acct_stat*>::iterator it;
	for (it = stat_list.begin(); it != stat_list.end(); it++)
		delete(*it);
	stat_list.clear();

	/* Check changes since beginning of test */
	mgr->get_change_in_stats(key, start_time, query_buffer, &error);

	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	uint64_t expected_value = (stat.get_bytes_in() * num_test_updates);
	fail_if(query_buffer.get_bytes_in() != expected_value,
	    "Incorrect value.  Expected %d, got %d", expected_value,
	    query_buffer.get_bytes_in());

	/* Check changes since beginning of test and end of test */
	mgr->get_change_in_stats(key, start_time, end_time, query_buffer,
	    &error);

	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	fail_if(query_buffer.get_bytes_in() != expected_value,
	    "Incorrect value.  Expected %d, got %d", expected_value,
	    query_buffer.get_bytes_in());

	/* Make sure no changes between end of test and now */
	mgr->get_change_in_stats(key, end_time, query_buffer, &error);

	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	fail_if( query_buffer.get_bytes_in() != 0,
	    "Incorrect value.  Expected 0, got %d",
	    query_buffer.get_bytes_in());

	/* Check changes since test midpoint */
	mgr->get_change_in_stats(key, midpt_time, query_buffer, &error);

	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	expected_value = (stat.get_bytes_in() * num_test_updates/2);
	fail_if(query_buffer.get_bytes_in() != expected_value,
	    "Incorrect value.  Expected %d, got %d", expected_value,
	    query_buffer.get_bytes_in());

	/* Clean up */
	delete mgr;
}

TEST(test_get_all_stats_results,
    mem_check: CK_MEM_DEFAULT, mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;
	const account_key_t acct_key = 7;
	const cpool_stat_account_key key(acct_key);
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	acct_time_stat *ret_ptr = NULL;
	std::vector<acct_time_stat*> ret_buffer;

	/* Create a list of queries to be executed later*/
	cpool_stat_account_query_result_key long_query(acct_key, 999999);
	cpool_stat_account_query_result_key now_query(acct_key, 0);
	cpool_stat_account_query_result_key short_query(acct_key, 3);
	std::vector<const cpool_stat_account_query_result_key*> queries;
	queries.push_back(&long_query);
	queries.push_back(&now_query);
	queries.push_back(&short_query);

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Verify stats results start at 0 (and don't fail with no records) */
	mgr->get_all_stats_results(queries, ret_buffer, &error);
	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	fail_if(ret_buffer.size() != queries.size(),
	    "Incorrect value.  Expected %d, got %d", queries.size(),
	    ret_buffer.size());
	ret_ptr = *(ret_buffer.begin());
	fail_if(ret_ptr->get_value()->get_bytes_in() != 0,
	    "Incorrect number of bytes_in.  Expected 0, got %d",
	    ret_ptr->get_value()->get_bytes_in());

	/* Clean up ret_buffer for reuse later */
	std::vector<acct_time_stat*>::iterator ret_it;
	for (ret_it = ret_buffer.begin(); ret_it != ret_buffer.end(); ++ret_it)
		delete (*ret_it);
	ret_buffer.clear();

	/* Create a list of stats for pushing into persistence */
	std::vector<acct_stat*> stat_list;
	stat_list.push_back(new acct_stat(key, stat));

	/* Push first record into database */
	mgr->add_to_stats_totals(stat_list, &error);
	fail_if(error != NULL, "Error inserting into persistence: %{}",
	    isi_error_fmt(error));
	mgr->update_stats_history(&error);
	fail_if(error != NULL, "Error updating history table persistence: %{}",
	    isi_error_fmt(error));

	/* Add a short sleep to ensure timestamps are unique */
	sleep(3);

	/* Push a second record into database */
	mgr->add_to_stats_totals(stat_list, &error);
	fail_if(error != NULL, "Error inserting into persistence: %{}",
	    isi_error_fmt(error));
	mgr->update_stats_history(&error);
	fail_if(error != NULL, "Error updating history table persistence: %{}",
	    isi_error_fmt(error));

	/* Verify stats results start at 0 (and don't fail with no records) */
	mgr->get_all_stats_results(queries, ret_buffer, &error);
	fail_if(error != NULL, "Failed to query for changes: %#{}",
	    isi_error_fmt(error));
	fail_if(ret_buffer.size() != queries.size(),
	    "Incorrect value.  Expected %d, got %d", queries.size(),
	    ret_buffer.size());

	for (ret_it = ret_buffer.begin(); ret_it != ret_buffer.end(); ++ret_it)
	{
		ret_ptr = (*ret_it);

		/*
		 * Got the long query to beginning of time - expect total of
		 * all changes between beginning and now
		 */
		if (ret_ptr->get_key()->compare(long_query) == 0) {
			uint64_t expected = stat.get_bytes_in() * 2;
			fail_if(
			    ret_ptr->get_value()->get_bytes_in() != expected,
			    "Incorrect number of bytes_in.  Expected %d, got %d",
			    expected, ret_ptr->get_value()->get_bytes_in());

		/*
		 * Got the query to 'now' - expect no changes between now and
		 * now
		 */
		} else if (ret_ptr->get_key()->compare(now_query) == 0) {
			uint64_t expected = 0;
			fail_if(
			    ret_ptr->get_value()->get_bytes_in() != expected,
			    "Incorrect number of bytes_in.  Expected %d, got %d",
			    expected, ret_ptr->get_value()->get_bytes_in());

		/*
		 * Got the query to a second ago - expect only second change
		 * between to appear between 1 second ago and now
		 */
		} else if (ret_ptr->get_key()->compare(short_query) == 0) {
			uint64_t expected = stat.get_bytes_in();
			fail_if(
			    ret_ptr->get_value()->get_bytes_in() != expected,
			    "Incorrect number of bytes_in.  Expected %d, got %d",
			    expected, ret_ptr->get_value()->get_bytes_in());

		} else {
			fail_if(true, "Unknown key found in ret_buffer");
		}

		/* Deallocate ret_buffer as we go - why not. */
		delete (*ret_it);
	}

	/* Done with stat list */
	std::vector<acct_stat*>::iterator it;
	for (it = stat_list.begin(); it != stat_list.end(); it++)
		delete(*it);
	stat_list.clear();

	/* Clean up */
	ret_buffer.clear();
	delete mgr;
}

TEST(test_prepopulate, mem_check: CK_MEM_DEFAULT,
    mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;
	const account_key_t test_key = 7;

	const unsigned int num_entries = 10;
	const cpool_stat test_stat(1, 2, 3, 4, 5, 6);
	cpool_stat query_result;
	time_t spacing = 300;
	std::vector<account_key_t> keys;
	keys.push_back(7);

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Prepopulate the database */
	mgr->prepopulate_stats_history(test_stat, keys, num_entries, spacing,
	    &error);
	fail_if(error != NULL, "Error prepopulating stats: %#{}",
	    isi_error_fmt(error));

	/* Verify expected result */
	mgr->get_change_in_stats(test_key, (time_t)0, query_result, &error);
	fail_if(error != NULL, "Error querying stats: %#{}",
	    isi_error_fmt(error));
	fail_if(
	    query_result.get_bytes_in() != test_stat.get_bytes_in()*num_entries,
	    "Incorrect bytes_in found.  Expected %llu, got %llu",
	    test_stat.get_bytes_in()*num_entries, query_result.get_bytes_in());

	delete mgr;
}

TEST(check_set_total_stats, mem_check: CK_MEM_DEFAULT,
    mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;

	cpool_stat query_buffer;

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Create and push a list of stats */
	std::vector<acct_stat*> stat_list;
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	stat_list.push_back(new acct_stat(key, stat));

	mgr->set_stats_totals(stat_list, CP_STAT_TOTAL_USAGE, &error);
	fail_if(error != NULL, "Error inserting into persistence: %{}",
	    isi_error_fmt(error));

	mgr->update_stats_history(&error);
	fail_if(error != NULL,
	    "Error updating history table persistence: %{}",
	     isi_error_fmt(error));

	mgr->get_total_stats(key, query_buffer, &error);
	fail_if(error != NULL, "Failed to query for totals: %#{}",
	    isi_error_fmt(error));

	/*
	 * Expect all values except total_usage to be zero (e.g., because of
	 * the CP_STAT_TOTAL_USAGE mask in line 503
	 */
	fail_if(query_buffer.get_bytes_in() != 0 ||
		query_buffer.get_bytes_out() != 0 ||
		query_buffer.get_num_gets() != 0 ||
		query_buffer.get_num_puts() != 0 ||
		query_buffer.get_num_deletes() != 0,
		"Incorrect stat value returned.  Expected zeroes, got: "
		"%d, %d, %d, %d, %d",
		query_buffer.get_bytes_in(), query_buffer.get_bytes_out(),
		query_buffer.get_num_gets(), query_buffer.get_num_puts(),
		query_buffer.get_num_deletes());
	fail_if(query_buffer.get_total_usage() != 6,
		"Incorrect usage value returned.  Expected %d, got: %d",
		stat.get_total_usage(), query_buffer.get_total_usage());

	/* Clean up */
	delete mgr;
	std::vector<acct_stat*>::iterator it;
	for (it = stat_list.begin(); it != stat_list.end(); it++)
		delete(*it);
	stat_list.clear();
}

TEST(check_get_all_data_points, mem_check: CK_MEM_DEFAULT,
     mem_hint: EXPECTED_FALSE_POSITIVE_MEM_LEAKS)
{
	struct isi_error *error = NULL;
	cpool_stat stat_in(1, 2, 3, 4, 5, 6);
	account_key_t key_1 = 1, key_2 = 2, key_3 = 3;
	std::vector<account_key_t> keys;
	std::vector<cpool_stat_time_record*> stats_out;
	const unsigned int num_entries_per_acct = 5;
	const time_t seconds_apart = 300;
	time_t start_time = time(NULL) - seconds_apart*num_entries_per_acct;

	/* Create a persistence manager */
	cpool_stat_persistence_manager *mgr =
	    cpool_stat_persistence_manager::get_new_test_instance(&error);
	fail_if(error != NULL, "Error creating manager instance: %{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Failed to create instance");

	/* Prepopulate data */
	keys.push_back(key_1);
	keys.push_back(key_2);
	keys.push_back(key_3);
	mgr->prepopulate_stats_history(stat_in, keys, num_entries_per_acct,
	    seconds_apart, &error);
	fail_if(error != NULL, "Error prepopulating stats: %#{}",
	    isi_error_fmt(error));

	/* Grab all data points */
	mgr->get_all_data_for_accounts(stats_out,
	    seconds_apart * num_entries_per_acct + 1, &keys, &error);
	fail_if(error != NULL, "Error retrieving all stats: %#{}",
	    isi_error_fmt(error));
	fail_if(stats_out.size() != num_entries_per_acct * keys.size(),
	    "Wrong number of data points returned.  Expected %d, got %d",
	    num_entries_per_acct * keys.size(), stats_out.size());

	/* Verify we got back what was expected */
	std::vector<cpool_stat_time_record*>::iterator it;
	for (it = stats_out.begin(); it != stats_out.end(); ++it) {

		cpool_stat_time_record *cur_record = (*it);

		const cpool_stat &cur_stat =
		    cur_record->get_stats();
		const cpool_stat_account_key &key = cur_record->get_key();
		const time_t timestamp = cur_record->get_timestamp();

		fail_if(
		    timestamp < start_time,
		    "Incorrect timestamp (%d), should be greater than "
		    "start (%d)", timestamp, start_time);

		fail_if(key.compare(key_1) != 0 &&
		    key.compare(key_2) != 0 &&
		    key.compare(key_3) != 0,
		    "Unexpected key returned: %d", key.get_account_key());

		fail_if(cur_stat.get_bytes_in()%stat_in.get_bytes_in() != 0,
		    "Wrong bytes in.  Expected multiple of %llu, got %llu",
		    stat_in.get_bytes_in(), cur_stat.get_bytes_in());
		fail_if(cur_stat.get_bytes_out()%stat_in.get_bytes_out() != 0,
		    "Wrong bytes out.  Expected multiple of %llu, got %llu",
		    stat_in.get_bytes_out(), cur_stat.get_bytes_out());
		fail_if(cur_stat.get_num_gets()%stat_in.get_num_gets() != 0,
		    "Wrong num gets.  Expected multiple of %llu, got %llu",
		    stat_in.get_num_gets(), cur_stat.get_num_gets());
		fail_if(cur_stat.get_num_puts()%stat_in.get_num_puts() != 0,
		    "Wrong num puts.  Expected multiple of %llu, got %llu",
		    stat_in.get_num_puts(), cur_stat.get_num_puts());
		fail_if(
		    cur_stat.get_num_deletes()%stat_in.get_num_deletes() != 0,
		    "Wrong num deletes.  Expected multiple of %llu, got %llu",
		    stat_in.get_num_deletes(), cur_stat.get_num_deletes());
		fail_if(
		    cur_stat.get_total_usage()%stat_in.get_total_usage() != 0,
		    "Wrong usage.  Expected multiple of %llu, got %llu",
		    stat_in.get_total_usage(), cur_stat.get_total_usage());
	}

	/* Cleanup */
	for (it = stats_out.begin(); it != stats_out.end(); ++it)
		delete (*it);
	stats_out.clear();
	delete mgr;
}
