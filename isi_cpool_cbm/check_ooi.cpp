#include <ifs/ifs_syscalls.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_sbtree/sbtree.h>

#include "isi_cbm_coi_sbt.h"
#include "unit_test_helpers.h"
#include "isi_cbm_ooi.h"

using namespace isi_cloud;

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_ooi,
					  .mem_check = CK_MEM_LEAKS,
					  .suite_setup = setup_suite,
					  .test_setup = setup_test,
					  .timeout = 30,
					  .fixture_timeout = 1200);

static uint32_t g_devid;

TEST_FIXTURE(setup_suite)
{
	/*
	 * Create an isi_cloud_object_id object here to allocate static heap
	 * variables before check starts its memory accounting.
	 */
	isi_cloud_object_id unused_object_id;

	struct isi_error *error = NULL;

	g_devid = get_devid(&error);
	fail_if(error != NULL, "error getting devid: %#{}",
			isi_error_fmt(error));
}

TEST_FIXTURE(setup_test)
{
	const char *ooi_lock_filename = get_ooi_lock_filename();
	unlink(ooi_lock_filename);
}

/*
 * Test the addition of a new cloud object ID to an entry in the OOI which does
 * not yet exist.
 */
TEST(test_prepare_for_cloud_object_addition__new_entry)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	isi_cloud_object_id object_id;
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = NULL;

	/* Prepare the bulk entry for addition. */
	/////ooi entry的key：process_time devid
	entry = ooi._prepare_for_cloud_object_addition(date_of_death, g_devid, ///////从ooi中拿出ooi entry,往ooi entry中insert object_id
												   object_id, coi_sbt_op, &error);
	fail_if(error != NULL,
			"failure in preparing for cloud object addition: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	/* Execute the bulk operation to verify coi_sbt_op. */
	coi_sbt_bulk_op(1, &coi_sbt_op, &error);
	fail_if(error != NULL, "failed to execute bulk op: %#{}",
			isi_error_fmt(error));

	delete entry;

	/*
	 * Retrieve the just-added entry, which should have one cloud object
	 * ID.
	 */
	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error != NULL, "failed to retrieve just-added entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);
	fail_if(entry->get_cloud_objects().size() != 1,
			"cloud object ID set size mismatch (exp: 1 act: %zu)",
			entry->get_cloud_objects().size());

	delete entry;
}

/*
 * Test the addition of a new cloud object ID to an entry in the OOI which
 * already exists and therefore needs to be modified.
 */
//////先往ooi中创建一个ooi entry,并且有一个object_id. 根据process_time,devid,拿出这个ooi entry,再往里面添加另外一个object_id
TEST(test_prepare_for_cloud_object_addition__modified_entry)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	/* Create an OOI entry that will exist prior to adding the new one. */
	uint64_t oid_high = 1, oid_low = 2, oid_snapid = 3;
	isi_cloud_object_id add_object_id(oid_high, oid_low);
	add_object_id.set_snapid(oid_snapid);

	ooi_entry add_entry;
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);
	add_entry.add_cloud_object(add_object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	btree_key_t key = add_entry.to_btree_key();

	const void *serialization;
	size_t serialization_size;
	add_entry.get_serialized_entry(serialization, serialization_size);

	fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key, serialization, /////往ooi中添加一个ooi entry(包含一个object_id)
							  serialization_size, NULL) != 0,
			"failed to add to OOI: [%d] %s",
			errno, strerror(errno));

	/*
	 * Having added an entry, prepare the bulk entry for addition (which
	 * should modify the entry we just created).
	 */
	isi_cloud_object_id object_id;
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = NULL;

	/* Prepare the bulk entry for addition. */
	entry = ooi._prepare_for_cloud_object_addition(date_of_death, g_devid, ////往同一个ooi entry中添加另外一个object_id
												   object_id, coi_sbt_op, &error);
	fail_if(error != NULL,
			"failure in preparing for cloud object addition: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	/* Execute the bulk operation to verify coi_sbt_op. */
	coi_sbt_bulk_op(1, &coi_sbt_op, &error);
	fail_if(error != NULL, "failed to execute bulk op: %#{}",
			isi_error_fmt(error));

	delete entry;

	/*
	 * Retrieve the just-added entry, which should have two cloud object
	 * IDs.
	 */
	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error != NULL, "failed to retrieve just-added entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);
	fail_if(entry->get_cloud_objects().size() != 2,
			"cloud object ID set size mismatch (exp: 2 act: %zu)",
			entry->get_cloud_objects().size());

	delete entry;
}

/*
 * Test the addition of a new cloud object ID to an entry in the OOI which
 * already exists but is full; a new entry will need to be created.
 */
TEST(test_prepare_for_cloud_object_addition__full_entry)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	/*
	 * Saturate an OOI entry with cloud object IDs to persist a full entry.
	 */
	ooi_entry add_entry;
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);

	uint64_t oid_high = 1, oid_low = 2, oid_snapid = 0;
	isi_cloud_object_id add_object_id(oid_high, oid_low);
	add_object_id.set_snapid(oid_snapid);

	while (true)
	{
		add_entry.add_cloud_object(add_object_id, &error); //////往同一个ooi entry中不断加入不同的object_id
		if (error != NULL && isi_system_error_is_a(error, ENOSPC))
		{
			isi_error_free(error);
			error = NULL;

			break;
		}

		fail_if(error != NULL, "failed to add object ID: %#{}",
				isi_error_fmt(error));

		add_object_id.set_snapid(++oid_snapid); ////确保加入的object_id是不同的
	}

	btree_key_t key = add_entry.to_btree_key();

	const void *serialization;
	size_t serialization_size;
	add_entry.get_serialized_entry(serialization, serialization_size);

	fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key, serialization,
							  serialization_size, NULL) != 0,
			"failed to add to OOI: [%d] %s",
			errno, strerror(errno));

	/*
	 * Having added the saturated entry, prepare the bulk entry for
	 * addition (which should create a new one).
	 */
	isi_cloud_object_id object_id;
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = NULL;

	/* Prepare the bulk entry for addition. */
	entry = ooi._prepare_for_cloud_object_addition(date_of_death, g_devid,
												   object_id, coi_sbt_op, &error);
	fail_if(error != NULL,
			"failure in preparing for cloud object addition: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	/* Execute the bulk operation to verify coi_sbt_op. */
	coi_sbt_bulk_op(1, &coi_sbt_op, &error);
	fail_if(error != NULL, "failed to execute bulk op: %#{}",
			isi_error_fmt(error));

	delete entry;

	/*
	 * Retrieve the saturated entry and verify the number of cloud object
	 * IDs.
	 */
	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error != NULL, "failed to retrieve saturated entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);
	fail_if(entry->get_cloud_objects().size() != oid_snapid,
			"cloud object ID set size mismatch (exp: %lu act: %zu)",
			oid_snapid, entry->get_cloud_objects().size());

	delete entry;

	/* Retrieve the new entry to verify that it was created. */
	entry = ooi.get_entry(date_of_death, g_devid, 2, &error);
	fail_if(error != NULL, "failed to retrieve just-added entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);
	fail_if(entry->get_cloud_objects().size() != 1,
			"cloud object ID set size mismatch (exp: 1 act: %zu)",
			entry->get_cloud_objects().size());

	delete entry;
}

/*
 * Test the removal of a cloud object ID from an existing entry which also
 * contains other cloud object IDs.
 */
TEST(test_prepare_for_cloud_object_removal__existing_object_and_others)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	uint64_t oid_high = 1, oid_low = 2, oid_snapid = 3;
	isi_cloud_object_id object_id(oid_high, oid_low);
	object_id.set_snapid(oid_snapid);

	ooi_entry add_entry;
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);
	add_entry.add_cloud_object(object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	/* Add a second cloud object to the entry before persisting. */
	object_id.set_snapid(oid_snapid + 1);
	add_entry.add_cloud_object(object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	btree_key_t key = add_entry.to_btree_key();

	const void *serialization;
	size_t serialization_size;
	add_entry.get_serialized_entry(serialization, serialization_size);

	fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key, serialization,  /////把ooi entry(有2个object_id)加入到ooi中
							  serialization_size, NULL) != 0,
			"failed to add to OOI: [%d] %s",
			errno, strerror(errno));

	/*
	 * With setup complete, prepare a bulk operation entry to remove the
	 * cloud object ID we just added.
	 */
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = ooi.prepare_for_cloud_object_removal(object_id,
															add_entry.to_btree_key(), coi_sbt_op, &error);
	fail_if(error != NULL,
			"failure in preparing for cloud object removal: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	/*
	 * Execute the bulk operation to verify coi_sbt_op.  Since there are
	 * other cloud object IDs in the entry, the entry should still exist.
	 */
	coi_sbt_bulk_op(1, &coi_sbt_op, &error);
	fail_if(error != NULL, "failed to execute bulk op: %#{}",
			isi_error_fmt(error));

	delete entry;

	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error != NULL, "failed to retrieve just-added entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);
	fail_if(entry->get_cloud_objects().size() != 1,
			"cloud object ID set size mismatch (exp: 1 act: %zu)",
			entry->get_cloud_objects().size());

	delete entry;
}

/*
 * Test the removal of a cloud object ID from an entry which contains no other
 * cloud object IDs.
 */
TEST(test_prepare_for_cloud_object_removal__existing_object_no_others)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	uint64_t oid_high = 1, oid_low = 2, oid_snapid = 3;
	isi_cloud_object_id object_id(oid_high, oid_low);
	object_id.set_snapid(oid_snapid);

	ooi_entry add_entry;
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);
	add_entry.add_cloud_object(object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	btree_key_t key = add_entry.to_btree_key();

	const void *serialization;
	size_t serialization_size;
	add_entry.get_serialized_entry(serialization, serialization_size);

	fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key, serialization,
							  serialization_size, NULL) != 0,
			"failed to add to OOI: [%d] %s",
			errno, strerror(errno));

	/*
	 * With setup complete, prepare a bulk operation entry to remove the
	 * cloud object ID we just added.
	 */
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = ooi.prepare_for_cloud_object_removal(object_id,
															add_entry.to_btree_key(), coi_sbt_op, &error);
	fail_if(error != NULL,
			"failure in preparing for cloud object removal: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	/*
	 * Execute the bulk operation to verify coi_sbt_op.  Since there are no
	 * other cloud object IDs in the entry, the entry should be removed.
	 */
	coi_sbt_bulk_op(1, &coi_sbt_op, &error);
	fail_if(error != NULL, "failed to execute bulk op: %#{}",
			isi_error_fmt(error));

	delete entry;

	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error == NULL, "expected ENOENT");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if(entry != NULL);

	isi_error_free(error);
}

/*
 * Test the removal of a non-existent cloud object ID from an existing entry.
 */
TEST(test_prepare_for_cloud_object_removal__existing_entry)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	uint64_t oid_high = 1, oid_low = 2, oid_snapid = 3;
	isi_cloud_object_id object_id(oid_high, oid_low);
	object_id.set_snapid(oid_snapid);

	ooi_entry add_entry;
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);
	add_entry.add_cloud_object(object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	btree_key_t key = add_entry.to_btree_key();

	const void *serialization;
	size_t serialization_size;
	add_entry.get_serialized_entry(serialization, serialization_size);

	fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key, serialization, /////把ooi entry(with object_id)加入到ooi中
							  serialization_size, NULL) != 0,
			"failed to add to OOI: [%d] %s",
			errno, strerror(errno));

	/*
	 * With setup complete, prepare a bulk operation entry to remove a
	 * non-existent cloud object ID from the entry we just added.
	 */
	object_id.set_snapid(++oid_snapid);
	struct coi_sbt_bulk_entry coi_sbt_op;
	ooi_entry *entry = ooi.prepare_for_cloud_object_removal(object_id,
															add_entry.to_btree_key(), coi_sbt_op, &error);
	fail_if(error == NULL, "expected ENOENT");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if(entry != NULL);

	isi_error_free(error);
	error = NULL;

	/*
	 * There's no bulk operation to execute, but we'll retrieve the entry,
	 * which should still contain the original cloud object ID.
	 */
	entry = ooi.get_entry(date_of_death, g_devid, 1, &error);
	fail_if(error != NULL, "failed to retrieve entry: %#{}",
			isi_error_fmt(error));
	fail_if(entry == NULL);

	delete entry;
}

/*
 * Test the removal of a non-existent cloud object ID from a non-existent
 * entry.
 */
TEST(test_prepare_for_cloud_object_removal__non_existing)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	isi_cloud_object_id object_id;
	btree_key_t bogus_key = {{123, 456}};
	struct coi_sbt_bulk_entry coi_sbt_op;

	ooi_entry *entry = ooi.prepare_for_cloud_object_removal(object_id,
															bogus_key, coi_sbt_op, &error);
	fail_if(error == NULL, "expected ENOENT");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if(entry != NULL);

	isi_error_free(error);
}

/*
 * Test the validation of an OOI entry
 */
TEST(test_entry_validation)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	const int num_objects = 2;
	isi_cloud_object_id object_id[num_objects];
	btree_key_t key[num_objects];
	ooi_entry add_entry[num_objects];
	const void *serialization[num_objects];
	size_t serialization_size[num_objects];

	for (int i = 0; i < num_objects; i++)
	{
		uint64_t oid_high = i + 1, oid_low = i, snapid = 3;
		object_id[i].set(oid_high, oid_low);
		object_id[i].set_snapid(snapid);

		add_entry[i].set_date_of_death(date_of_death);
		add_entry[i].set_devid(g_devid);
		add_entry[i].set_index(i + 1);
		add_entry[i].add_cloud_object(object_id[i], &error);
		fail_if(error != NULL, "failed to add object ID: %#{}",
				isi_error_fmt(error));

		key[i] = add_entry[i].to_btree_key();

		add_entry[i].get_serialized_entry(serialization[i],
										  serialization_size[i]);

		fail_if(ifs_sbt_add_entry(ooi.get_sbt_fd(), &key[i],
								  serialization[i], serialization_size[i], NULL) != 0,
				"failed to add to OOI: [%d] %s", errno, strerror(errno));
	}

	/*
	 * Validate against correct and mismatched entry
	 */
	ooi.validate_entry(key[0], serialization[0], serialization_size[0],
					   true, &error);
	fail_if(error != NULL, "failed to validate ooi entry: %#{}",
			isi_error_fmt(error));
	ooi.validate_entry(key[0], serialization[1], serialization_size[1],
					   true, &error);
	fail_if(error == NULL, "Expected error, none ocurred");
	fail_if(!isi_system_error_is_a(error, ERANGE),
			"expected ERANGE: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	ooi.validate_entry(key[0], serialization[0], serialization_size[0],
					   false, &error);
	fail_if(!isi_system_error_is_a(error, EDOOFUS),
			"expected EDOOFUS: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;
}

/*
 * Test the validation of an non-existence OOI entry
 */
TEST(test_non_existence_entry_validation)
{
	struct isi_error *error = NULL;

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	struct timeval date_of_death;
	gettimeofday(&date_of_death, NULL);

	isi_cloud_object_id object_id;
	btree_key_t key;
	ooi_entry add_entry;
	const void *serialization;
	size_t serialization_size;

	uint64_t oid_high = 1, oid_low = 0, snapid = 3;
	object_id.set(oid_high, oid_low);
	object_id.set_snapid(snapid);
	add_entry.set_date_of_death(date_of_death);
	add_entry.set_devid(g_devid);
	add_entry.set_index(1);
	add_entry.add_cloud_object(object_id, &error);
	fail_if(error != NULL, "failed to add object ID: %#{}",
			isi_error_fmt(error));

	key = add_entry.to_btree_key();
	add_entry.get_serialized_entry(serialization,
								   serialization_size);
	// DO NOT add this entry to SBT

	// Validate non-existence entry;
	ooi.validate_entry(key, NULL, 0, false, &error);
	fail_if(error != NULL, "failed to validate an non-existence OOI entry: %#{}",
			isi_error_fmt(error));

	ooi.validate_entry(key, NULL, 0, true, &error);
	fail_if(error == NULL, "expected ENOENT");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"expected ENOENT: %#{}", isi_error_fmt(error));

	isi_error_free(error);
}
