#include <sys/stat.h>

#include <isi_util_cpp/check_leak_primer.hpp>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat.h"
#include "cpool_stat_key.h"
#include "cpool_stat_manager.h"
#include "cpool_stat_persistence_manager.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_stat_manager,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .suite_teardown = teardown_suite,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 90,
    .fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;

	cpool_stat_manager *mgr = cpool_stat_manager::get_new_test_instance(
	    &error);
	fail_if(error != NULL, "Error getting test instance: %#{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Received NULL manager instance");

	delete mgr;

	check_leak_primer();
}

TEST_FIXTURE(teardown_suite)
{
}

TEST_FIXTURE(setup_test)
{
	cpool_stat_manager::cleanup_for_test();
}

TEST_FIXTURE(teardown_test)
{
}

////////cpool_stats先写入stat_map_,再把内存中的stats持久化到total_table中，再把total_table中的stats持久化到history_table中
TEST(test_stat_collection,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const account_key_t key(7);
	const cpool_stat delta(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	const int num_deltas = 10;

	// Test getting a new instance
	cpool_stat_manager *mgr = cpool_stat_manager::get_new_test_instance(
	    &error);
	fail_if(error != NULL, "Error getting test instance: %#{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Received NULL manager instance");

	// Do a pre-test cleanup to make sure any lingering shared_memory from
	//  previous tests is gone
	mgr->cleanup_test_instance(&error);
	fail_if(error != NULL, "Error cleaning up: %#{}", isi_error_fmt(error));

	for (int i = 0; i < num_deltas; i++) {
		// Test adding deltas to in-memory stats
		mgr->add_to_stats(key, delta, &error);///////加到内存中(stat_map_)
		fail_if(error != NULL, "Error while adding to stats: %#{}",
		    isi_error_fmt(error));

		// Test adding deltas a second time to ensure proper summation
		mgr->add_to_stats(key, delta, &error);
		fail_if(error != NULL, "Error while adding to stats: %#{}",
		    isi_error_fmt(error));

		// Test serializing in-memory stats
		mgr->save_current_stats(&error); ////内存中的数据持久化到total_table中
		fail_if(error != NULL, "Error while saving current stats: %#{}",
		    isi_error_fmt(error));

		// Test adding records to the history tables
		mgr->update_stats_history(&error); /////把total_table中的数据逐条加上time，持久化到history_table
		fail_if(error != NULL, "Error updating history table: %#{}",
		    isi_error_fmt(error));

		// Need to do a short sleep to ensure non-conflicting
		//   timestamps in history table
		sleep(1);
	}

// TODO:: Verify existing records when implemented - there should be num_deltas records in history

	// Verify the totals in the history table
	cpool_stat stats_out;
	const int num_recordings = num_deltas*2;
	cpool_stat expected_stats_out(delta.get_bytes_in()*num_recordings,
					delta.get_bytes_out()*num_recordings,
					delta.get_num_gets()*num_recordings,
					delta.get_num_puts()*num_recordings,
					delta.get_num_deletes()*num_recordings,
					delta.get_total_usage()*num_recordings);
	mgr->get_total_stats(key, stats_out, &error);
	fail_if(error != NULL, "Error retrieving totals stats: %#{}",
	    isi_error_fmt(error));

	fail_if(stats_out.get_bytes_in() != expected_stats_out.get_bytes_in(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_bytes_in(), stats_out.get_bytes_in());

	fail_if(stats_out.get_bytes_out() != expected_stats_out.get_bytes_out(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_bytes_out(), stats_out.get_bytes_out());

	fail_if(stats_out.get_num_gets() != expected_stats_out.get_num_gets(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_num_gets(), stats_out.get_num_gets());

	fail_if(stats_out.get_num_puts() != expected_stats_out.get_num_puts(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_num_puts(), stats_out.get_num_puts());

	fail_if(stats_out.get_num_deletes() != expected_stats_out.get_num_deletes(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_num_deletes(), stats_out.get_num_deletes());

	fail_if(stats_out.get_total_usage() != expected_stats_out.get_total_usage(),
	    "Incorrect bytes_in returned.  Expected %d, got %d",
	    expected_stats_out.get_total_usage(), stats_out.get_total_usage());

	// Cleanup
	mgr->cleanup_test_instance(&error);
	fail_if(error != NULL, "Error cleaning up: %#{}", isi_error_fmt(error));

	delete mgr;
}

TEST(test_query_registration,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	const account_key_t acct_key = 7;
	const unsigned int query_time_delta = 60*60; // 1 hour
	cpool_stat stat_delta(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;

	// Test getting a new instance
	cpool_stat_manager *mgr = cpool_stat_manager::get_new_test_instance(
	    &error);
	fail_if(error != NULL, "Error getting test instance: %#{}",
	    isi_error_fmt(error));
	fail_if(mgr == NULL, "Received NULL manager instance");

	// Do a pre-test cleanup to make sure any lingering shared_memory from
	//  previous tests is gone
	mgr->cleanup_test_instance(&error);
	fail_if(error != NULL, "Error cleaning up: %#{}", isi_error_fmt(error));

	// Negative test retrieve unregistered cached query
	mgr->get_cached_query_result(acct_key, query_time_delta, stat_delta,
	    &error);
	fail_if(error == NULL, "Expected error getting unregistered query");
	fail_unless(isi_system_error_is_a(error, ENOENT),
	    "Unexpected error type.  Expected ENOENT, got %#{}",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	// Test register above query
	mgr->register_cached_query(acct_key, query_time_delta, &error);
	fail_if(error != NULL, "Error registering query: %#{}",
	    isi_error_fmt(error));

	// Test register above query again - should not populate error
	mgr->register_cached_query(acct_key, query_time_delta, &error);
	fail_if(error != NULL, "Error double registering query: %#{}",
	    isi_error_fmt(error));

	// (Positive) Test retrieve registered cached query
	mgr->get_cached_query_result(acct_key, query_time_delta, stat_delta,
	    &error);
	fail_if(error != NULL, "Error retriving registered query: %#{}",
	    isi_error_fmt(error));
	fail_unless(stat_delta.get_bytes_in() == 0,
	    "Incorrect delta bytes in.  Expected 0, got %d",
	    stat_delta.get_bytes_in());

	// Test deregister above query
	mgr->deregister_cached_query(acct_key, query_time_delta, &error);
	fail_if(error != NULL, "Error deregistering query: %#{}",
	    isi_error_fmt(error));

	// Negative test retrieve (once again) unregistered cached query
	mgr->get_cached_query_result(acct_key, query_time_delta, stat_delta,
	    &error);
	fail_if(error == NULL, "Expected error getting deregistered query");
	fail_unless(isi_system_error_is_a(error, ENOENT),
	    "Unexpected error type.  Expected ENOENT, got %#{}",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	// Test deregister same query again - should not populate error
	mgr->deregister_cached_query(acct_key, query_time_delta, &error);
	fail_if(error != NULL,
	    "Should not see error deregistering non-existing query: %#{}",
	    isi_error_fmt(error));

	// Cleanup
	mgr->cleanup_test_instance(&error);
	fail_if(error != NULL, "Error cleaning up: %#{}", isi_error_fmt(error));

	delete mgr;
}
