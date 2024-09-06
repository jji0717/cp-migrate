#include <sys/stat.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat.h"
#include "cpool_stat_key.h"
#include "cpool_stat_list_element.h"
#include "cpool_stat_shared_map.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_shared_memory,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .suite_teardown = teardown_suite,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 90,
    .fixture_timeout = 1200);

#define test_shared_map_name "test_cpool_shared_map___1"

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;
	// Make sure we start with a clean map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	ASSERT(error == NULL, "Failed to create initial instance of map: %#{}",
	    isi_error_fmt(error));
	map->reset_after_test(&error);
	ASSERT(error == NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST_FIXTURE(teardown_suite)
{
}

TEST_FIXTURE(setup_test)
{
}

TEST_FIXTURE(teardown_test)
{
}

static bool
key_exists(cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map,
    cpool_stat_account_key &key, struct isi_error **error_out)
{
	return (map->find(key, error_out) != NULL);
}

TEST(test_list_element)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	// Test creating a list element
	cpool_stat_list_element<cpool_stat_account_key, cpool_stat> test_el;
	fail_if(!test_el.is_empty(), "List element should default to empty");

	// Test setting a value
	test_el.set(key, stat);
	fail_if(test_el.is_empty(),
	    "List element should not be empty after 'set'");
	fail_unless(test_el.get_key()->compare(key) == 0,
	    "Incorrect key for list element");
	fail_unless(test_el.get_value()->get_bytes_in() == stat.get_bytes_in(),
	    "Incorrect value for list element");

	// Test clearing the value
	test_el.clear();
	fail_if(!test_el.is_empty(),
	    "List element should be empty after 'clear'");
}

TEST(test_shared_map_smoke_create)
{
	struct isi_error *error = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_insert)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	struct isi_error *error = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// Test insert a key and validate operations on value
	map->insert(key, stat, &error);
	fail_if(error != NULL, "Error while inserting: %{}",
	    isi_error_fmt(error));

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_size)
{
	struct isi_error *error = NULL;
	size_t map_sz = 0;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz > 0, "Count should be 0 at creation time: %d", map_sz);

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_at)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	cpool_stat *stat_ptr = NULL;

	struct isi_error *error = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	stat_ptr = map->at(key, &error);
	fail_if(error == NULL, "Should get an error calling 'at' with bad key");
	fail_unless(isi_system_error_is_a(error, ENOENT),
	    "Expected ENOENT error from 'at' call.  Instead got: %#{}",
	    isi_error_fmt(error));
	fail_if(stat_ptr != NULL,
	    "Should not be able to fetch a non-existing key");
	isi_error_free(error);
	error = NULL;

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_find)
{
	cpool_stat_account_key key(7);

	struct isi_error *error = NULL;
	const cpool_stat *stat_ptr = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	stat_ptr = map->find(key, &error);
	fail_if(error != NULL, "Failed to find key: %{}", isi_error_fmt(error));
	fail_if(stat_ptr != NULL, "Should not find a non-existing key");

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_erase)
{
	cpool_stat_account_key key(7);

	struct isi_error *error = NULL;
	bool success = false;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	success = map->erase(key, &error);
	fail_if(error != NULL, "Failed to erase: %{}", isi_error_fmt(error));
	fail_if(success, "Should not be able to erase a non-existing key");

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_smoke_as_vector)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	struct isi_error *error = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// Test insert a key and validate operations on value
	map->insert(key, stat, &error);
	fail_if(error != NULL, "Error while inserting: %{}",
	    isi_error_fmt(error));

	map->lock_map(&error);
	fail_if(error != NULL, "Failed to lock map: %#{}",
	    isi_error_fmt(error));

	std::vector<
	    cpool_stat_list_element<cpool_stat_account_key, cpool_stat>*> stats;
	map->as_vector(stats, &error);
	fail_if(error != NULL, "Error while converting to vector: %{}",
	    isi_error_fmt(error));
	fail_unless(stats.size() == 1,
	    "Incorrect vector size.  Expected 1, got: %d", stats.size());
	cpool_stat_list_element<cpool_stat_account_key, cpool_stat>* v_ptr =
	    (*(stats.begin()));

	uint64_t bytes_in = v_ptr->get_value()->get_bytes_in();
	fail_unless(bytes_in == stat.get_bytes_in(),
	    "Unexpected value for bytes_in.  Expected %d, got %d",
	    stat.get_bytes_in(), bytes_in);

	map->unlock_map(&error);
	fail_if(error != NULL, "Failed to unlock map: %#{}",
	    isi_error_fmt(error));

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	struct isi_error *error = NULL;
	bool success = false;
	const cpool_stat *stat_ptr = NULL;
	cpool_stat *writable_stat_ptr = NULL;
	size_t map_sz = 0;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz > 0, "Count should be 0 at creation time: %d", map_sz);

	// Negative tests finding/modifying an existing value
	success = key_exists(map, key, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_if(success, "Non-existing key should not exist");

	stat_ptr = map->at(key, &error);
	fail_if(error == NULL, "Should get an error calling 'at' with bad key");
	fail_unless(isi_system_error_is_a(error, ENOENT),
	    "Expected ENOENT error from 'at' call.  Instead got: %#{}",
	    isi_error_fmt(error));
	fail_if(stat_ptr != NULL,
	    "Should not be able to fetch a non-existing key");

	isi_error_free(error);
	error = NULL;

	stat_ptr = map->find(key, &error);
	fail_if(error != NULL, "Failed to find key: %{}", isi_error_fmt(error));
	fail_if(stat_ptr != NULL, "Should not find a non-existing key");

	success = map->erase(key, &error);
	fail_if(error != NULL, "Failed to erase: %{}", isi_error_fmt(error));
	fail_if(success, "Should not be able to erase a non-existing key");

	// Test insert a key and validate operations on value
	map->insert(key, stat, &error);
	fail_if(error != NULL, "Error while inserting: %{}",
	    isi_error_fmt(error));

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz != 1, "Wrong size for map.  Expected 1, got: %d",
	    map_sz);

	success = key_exists(map, key, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_unless(success, "Could not verify key's existence");

	map->lock_map(&error);
	fail_if(error != NULL, "Failed to lock map: %#{}",
	    isi_error_fmt(error));

	writable_stat_ptr = map->at(key, &error);
	fail_if(error != NULL, "Failed calling 'at': %#{}",
	    isi_error_fmt(error));
	fail_if(writable_stat_ptr == NULL,
	    "Did not fetch existing key.  Error should have been populated");

	writable_stat_ptr->set_bytes_in(1234);

	map->unlock_map(&error);
	fail_if(error != NULL, "Failed to unlock map: %#{}",
	    isi_error_fmt(error));

	stat_ptr = map->find(key, &error);
	fail_if(error != NULL, "Failed to find key: %{}", isi_error_fmt(error));
	fail_if(stat_ptr == NULL, "Could not find existing key");

	// Test inserting duplicate key
	map->insert(key, stat, &error);
	fail_if(error == NULL,
	    "Should have gotten an EEXIST error from double insert");
	fail_unless(isi_system_error_is_a(error, EEXIST),
	    "Wrong error type while inserting.  Expected EEXIST, got: %{}",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	// Test erasing existing key
	success = map->erase(key, &error);
	fail_if(error != NULL, "Failed to erase: %{}", isi_error_fmt(error));
	fail_unless(success, "Could not erase existing key");

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz > 0, "Wrong size for map.  Expected 0, got: %d",
	    map_sz);

	success = key_exists(map, key, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_if(success, "Key should no longer exist");

	// Test putting key back in and using clear to get rid of it
	map->insert(key, stat, &error);
	fail_if(error != NULL, "Error while inserting: %{}",
	    isi_error_fmt(error));

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz != 1, "Wrong size for map.  Expected 1, got: %d",
	    map_sz);

	map->clear(&error);
	fail_if(error != NULL, "Failed to clear: %{}", isi_error_fmt(error));

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz > 0, "Wrong size for map.  Expected 0, got: %d",
	    map_sz);

	success = key_exists(map, key, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_if(success, "Key should no longer exist");

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_overflow)
{
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	bool success = false;
	const cpool_stat *stat_ptr = NULL;
	size_t map_sz = 0;
	const int num_test_keys = CP_STAT_SHARED_MAP_PAGE_SIZE*3 + 1;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// Test insert a bunch of keys
	for (int i = 0; i < num_test_keys; i++) {
		cpool_stat_account_key key(i);
		map->insert(key, stat, &error);
		fail_if(error != NULL, "Error while inserting key %d: %{}",
		    i, isi_error_fmt(error));

		map_sz = map->size(&error);
		fail_if(error != NULL, "Failed to get size: %{}",
		    isi_error_fmt(error));
		fail_if((int)map_sz != i + 1,
		    "Incorrect size.  Expected %d, got %d", i + 1, map_sz);
	}

	// Check retrieval of all keys
	for (int i = 0; i < num_test_keys; i++) {
		cpool_stat_account_key key(i);

		success = key_exists(map, key, &error);
		fail_if(error != NULL, "Failed to check key exists: %{}",
		    isi_error_fmt(error));
		fail_unless(success, "Could not verify key's existence %d", i);

		stat_ptr = map->find(key, &error);
		fail_if(error != NULL, "Failed to find key: %{}",
		    isi_error_fmt(error));
		fail_if(stat_ptr == NULL, "Could not find existing key: %d", i);
	}

	map->clear(&error);
	fail_if(error != NULL, "Failed to clear: %{}", isi_error_fmt(error));

	map_sz = map->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if (map_sz > 0, "Wrong size for map.  Expected 0, got: %d",
	    map_sz);

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_multiple_instances)
{
	cpool_stat_account_key key_1(1), key_2(2);
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	bool success = false;
	size_t map_sz = 0;

	// Test creating 2 maps
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map_1 =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map 1: %{}",
	    isi_error_fmt(error));
	fail_if (map_1 == NULL, "Failed to get instance of shared map 1");

	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map_2 =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map 2: %{}",
	    isi_error_fmt(error));
	fail_if (map_2 == NULL, "Failed to get instance of shared map 2");

	// Test insert a key into each map and verify it can be found in the
	//  other
	map_1->insert(key_1, stat, &error);
	fail_if(error != NULL, "Error while inserting key_1: %{}",
	    isi_error_fmt(error));

	map_2->insert(key_2, stat, &error);
	fail_if(error != NULL, "Error while inserting key_2: %{}",
	    isi_error_fmt(error));

	map_sz = map_1->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if((int)map_sz != 2, "Incorrect size.  Expected 2, got %d",
	    map_sz);

	success = key_exists(map_1, key_2, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_unless(success, "Could not verify key_2 in map_1");

	success = key_exists(map_2, key_1, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_unless(success, "Could not verify key_1 in map_2");

	success = map_2->erase(key_1, &error);
	fail_if(error != NULL, "Failed to erase: %{}", isi_error_fmt(error));
	fail_unless(success, "Should be able to delete key_2 from map_1");

	// Test adding a third map (created after others are populated)
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map_3 =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map 3: %{}",
	    isi_error_fmt(error));
	fail_if (map_3 == NULL, "Failed to get instance of shared map 3");

	map_sz = map_3->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if((int)map_sz != 1, "Incorrect size.  Expected 1, got %d",
	    map_sz);

	success = key_exists(map_3, key_2, &error);
	fail_if(error != NULL, "Failed to check key exists: %{}",
	    isi_error_fmt(error));
	fail_unless(success, "Could not verify key_2 in map_3");

	map_1->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));

	map_2->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));

	map_3->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_shared_map_multiple_processes)
{
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	bool success = false;
	const cpool_stat *stat_ptr = NULL;
	size_t map_sz = 0;
	const int num_test_keys = CP_STAT_SHARED_MAP_PAGE_SIZE*3 + 1;

	// Create a child process
	pid_t child_pid = fork();
	fail_if(child_pid < 0);

	// Test create a shared map (each process)
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// In the child process only, test inserting a bunch of keys
	//    (enough to overflow the page size)
	if (child_pid == 0) {
		for (int i = 0; i < num_test_keys; i++) {
			cpool_stat_account_key key(i);
			map->insert(key, stat, &error);
			fail_if(error != NULL,
			    "(child) Error while inserting key %d: %{}",
			    i, isi_error_fmt(error));

			map_sz = map->size(&error);
			fail_if(error != NULL, "(child) Failed to get size: %{}",
			    isi_error_fmt(error));
			fail_if((int)map_sz != i + 1,
			    "(child) Incorrect size.  Expected %d, got %d",
			    i + 1, map_sz);

			success = key_exists(map, key, &error);
			fail_if(error != NULL,
			    "(child) Failed to check key exists: %{}",
			    isi_error_fmt(error));
			fail_unless(success,
			    "(child) Could not verify key's existence %d", i);

		}

		_exit(0);
	} else {
		// parent process waits for child to do inserts - should not be
		//  necessary once locking is in place
		sleep(5);

		// Test retrieval of all keys (parent process)
		for (int i = 0; i < num_test_keys; i++) {
			cpool_stat_account_key key(i);

			success = key_exists(map, key, &error);
			fail_if(error != NULL,
			    "(parent) Failed to check key exists: %{}",
			    isi_error_fmt(error));
			fail_unless(success,
			    "(parent) Could not verify key's existence %d", i);

			stat_ptr = map->find(key, &error);
			fail_if(error != NULL,
			    "(parent) Failed to find key: %{}",
			    isi_error_fmt(error));
			fail_if(stat_ptr == NULL,
			    "(parent) Could not find existing key: %d", i);
		}

		map->reset_after_test(&error);
		fail_if(error != NULL,
		    "(parent) Failed to reset after test: %{}",
		    isi_error_fmt(error));
	}
}

TEST(test_shared_map_multiple_process_simultaneous_inserts)
{
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	size_t map_sz = 0;
	const int num_test_keys = CP_STAT_SHARED_MAP_PAGE_SIZE + 1;

	// Create a child process
	pid_t child_pid = fork();
	fail_if(child_pid < 0);

	bool is_child_process = (child_pid == 0);

	// Test create a shared map (each process)
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// Test inserting a bunch of unique keys from each process
	for (int i = 0; i < num_test_keys; i++) {
		int key_id = i + (is_child_process ? num_test_keys : 0);

		cpool_stat_account_key key(key_id);
		map->insert(key, stat, &error);
		fail_if(error != NULL,
		    "Error while inserting key %d: %{}",
		    key_id, isi_error_fmt(error));
	}

	if (is_child_process) {

		exit(0);
	} else {
		// Give time to make sure other process finished
		sleep(5);

		map_sz = map->size(&error);
		fail_if(error != NULL, "Failed to get size: %{}",
		    isi_error_fmt(error));
		fail_if((int)map_sz != num_test_keys*2,
		    "Incorrect size.  Expected %d, got %d", num_test_keys*2,
		    map_sz);

		map->reset_after_test(&error);
		fail_if(error != NULL, "Failed to reset after test: %{}",
		    isi_error_fmt(error));
	}
}

TEST(test_shared_map_sequential_instances)
{
	cpool_stat stat(1, 2, 3, 4, 5, 6);
	struct isi_error *error = NULL;
	bool success = false;
	size_t map_sz = 0;
	const int num_test_keys = CP_STAT_SHARED_MAP_PAGE_SIZE*3 + 1;

	// Test creating map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map_1 =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map 1: %{}",
	    isi_error_fmt(error));
	fail_if (map_1 == NULL, "Failed to get instance of shared map 1");

	// Test insert a lot of keys into map to verify later
	for (int i = 0; i < num_test_keys; i++) {
		cpool_stat_account_key key(i);

		map_1->insert(key, stat, &error);
		fail_if(error != NULL, "Error while inserting key: %{}",
		    isi_error_fmt(error));
	}
	map_1 = NULL;

	// Test getting a second reference to the map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map_2 =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map 2: %{}",
	    isi_error_fmt(error));
	fail_if (map_2 == NULL, "Failed to get instance of shared map 2");

	map_sz = map_2->size(&error);
	fail_if(error != NULL, "Failed to get size: %{}", isi_error_fmt(error));
	fail_if((int)map_sz != num_test_keys,
	    "Incorrect size.  Expected %d, got %d", num_test_keys, map_sz);

	for (int i = 0; i < num_test_keys; i++) {
		cpool_stat_account_key key(i);

		success = key_exists(map_2, key, &error);
		fail_if(error != NULL, "Failed to check key exists: %{}",
		    isi_error_fmt(error));
		fail_unless(success, "Key should exist: %d", i);
	}

	map_2->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

TEST(test_lock_entire_map)
{
	cpool_stat_account_key key(7);
	cpool_stat stat(1, 2, 3, 4, 5, 6);

	struct isi_error *error = NULL;
	bool success = false;
	const cpool_stat *stat_ptr = NULL;

	// Test creating a map
	cpool_stat_shared_map<cpool_stat_account_key, cpool_stat> *map =
	    cpool_stat_shared_map<cpool_stat_account_key, cpool_stat>::
		get_instance(test_shared_map_name, &error);

	fail_if (error != NULL, "Failed to create shared map: %{}",
	    isi_error_fmt(error));
	fail_if (map == NULL, "Failed to get instance of shared map");

	// Test locking the map and making changes
	map->lock_map(&error);
	fail_if(error != NULL, "Failed to lock_map: %{}", isi_error_fmt(error));

	map->insert(key, stat, &error);
	fail_if(error != NULL, "Error while inserting: %{}",
	    isi_error_fmt(error));

	stat_ptr = map->find(key, &error);
	fail_if(error != NULL, "Failed to find key: %{}", isi_error_fmt(error));
	fail_if(stat_ptr == NULL, "Could not find existing key");

	success = map->erase(key, &error);
	fail_if(error != NULL, "Failed to erase: %{}", isi_error_fmt(error));
	fail_unless(success, "Could not erase existing key");

	// Test double-locking map (should fail)
	map->lock_map(&error);
	fail_unless(error != NULL, "Should not be able to doubly lock map",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	// Test unlocking the map
	map->unlock_map(&error);
	fail_if(error != NULL, "Failed to unlock_map: %{}", isi_error_fmt(error));

	// Test double unlocking the map (should fail)
	map->unlock_map(&error);
	fail_unless(error != NULL, "Should not be able to double unlock map");
	isi_error_free(error);
	error = NULL;

	map->reset_after_test(&error);
	fail_if(error != NULL, "Failed to reset after test: %{}",
	    isi_error_fmt(error));
}

