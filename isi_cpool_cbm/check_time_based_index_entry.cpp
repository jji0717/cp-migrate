#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "isi_cbm_time_based_index_entry.h"

#define TBI_ENTRY_TEST_VERSION 		1

/*
 * Since the time based index entry is a template class, create a test class
 * which derives from it.  The template parameter must be a class (no
 * primitives) since we have to define some functions on it.
 */
class integer
{
public:
	int value_;

	integer() : value_(0) { }
	integer(int value) : value_(value) { }
	~integer() { }

	static size_t get_packed_size(void)
	{
		return sizeof(int);
	}

	bool operator<(const integer &rhs) const
	{
		return value_ < rhs.value_;
	}

	void pack(void **stream) const
	{
		memcpy(*stream, &value_, sizeof value_);
		*(char **)stream += sizeof value_;
	}

	void unpack(void **stream)
	{
		memcpy(&value_, *stream, sizeof value_);
		*(char **)stream += sizeof value_;
	}
};

class tbi_entry_test_class : public time_based_index_entry<integer>
{
public:
	tbi_entry_test_class()
		: time_based_index_entry<integer>(0) { }
	tbi_entry_test_class(uint32_t item_version)
		: time_based_index_entry<integer>(item_version) { }
	~tbi_entry_test_class() { }
		
};

SUITE_DEFINE_FOR_FILE(check_time_based_index_entry,
	.mem_check = CK_MEM_LEAKS,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST(test_serialize_deserialize)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class from_object(TBI_ENTRY_TEST_VERSION), to_object;
	struct timeval process_time = { 54, 0 };
	ifs_devid_t devid = 23;
	uint32_t index = 17;
	int items[5] = { 2, 3, 4, 5, 6 };
	std::set<integer> item_set;
	btree_key_t key;
	const void *serialization;
	size_t serialization_size;
	struct timeval to_obj_process_time;

	from_object.set_process_time(process_time);
	from_object.set_devid(devid);
	from_object.set_index(index);

	for (int i = 0; i < 5; ++i) {
		integer an_int(items[i]);
		from_object.add_item(an_int, &error);
		fail_if (error != NULL, "failed to add item (%d): %#{}",
		    items[i], isi_error_fmt(error));

		item_set.insert(items[i]);
	}

	key = from_object.to_btree_key();
	from_object.get_serialized_entry(serialization, serialization_size);

	to_object.set_serialized_entry(TBI_ENTRY_TEST_VERSION,
		key, serialization, serialization_size, &error);
	fail_if(error != NULL,
		"failed to set serialized entry %#{}, (%s), (%lu), %#{}",
		key, serialization, serialization_size, isi_error_fmt(error));

	to_obj_process_time = to_object.get_process_time();
	fail_if (timercmp(&to_obj_process_time, &process_time, !=),
	    "process time mismatch (exp: sec %ld usec %ld act: sec %ld usec %ld)",
	    process_time.tv_sec, process_time.tv_usec,
		to_object.get_process_time().tv_sec, to_object.get_process_time().tv_usec);
	fail_if (to_object.get_devid() != devid,
	    "devid mismatch (exp: %u act: %u)",
	    devid, to_object.get_devid());
	fail_if (to_object.get_index() != index,
	    "index mismatch (exp: %u act: %u)",
	    index, to_object.get_index());
	fail_if (item_set.size() != to_object.get_items().size(),
	    "item set size mismatch (exp: %zu act: %zu)",
	    item_set.size(), to_object.get_items().size());
	std::set<integer>::const_iterator iter = to_object.get_items().begin();
	for (int i = 0; iter != to_object.get_items().end(); ++iter, ++i)
		fail_if (iter->value_ != items[i],
		    "item set element mismatch (exp: %d act: %d)",
		    items[i], iter->value_);
}

TEST(test_to_btree_key)
{
	/*
	 * Testing the member function is sufficient as it calls the static
	 * function.
	 */
	tbi_entry_test_class test_object;

	struct timeval process_time = {0,100};
	test_object.set_process_time(process_time);	// 0x64
	test_object.set_devid(1748);		// 0x6d4
	test_object.set_index(63829);		// 0xf955

	btree_key_t key = test_object.to_btree_key();

	fail_if (key.keys[0] != 100,
	    "keys[0] mismatch (exp: 0x%016x act: 0x%016x)", 100, key.keys[0]);
	fail_if (key.keys[1] != 0x6d40000f955,
	    "keys[1] mismatch (exp: 0x%016x act: 0x%016x)",
	    0x6d40000f955, key.keys[1]);
}

TEST(test_from_btree_key)
{
	/*
	 * Testing the member function is sufficient as it calls the static
	 * function.
	 */
	tbi_entry_test_class test_object;
	btree_key_t key = {{ 0x64, 0x6d40000f955 }};

	test_object.from_btree_key(key);

	fail_if (test_object.get_process_time().tv_usec != 100,
	    "process time mismatch (exp: 100 act: %ld)",
	    test_object.get_process_time().tv_usec);
	fail_if (test_object.get_devid() != 1748,
	    "devid mismatch (exp: 1748 act: %u)",
	    test_object.get_devid());
	fail_if (test_object.get_index() != 63829,
	    "index mismatch (exp: 63829 act: %ld)",
	    test_object.get_index());
}

TEST(test_add_item)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class test_object;
	integer an_int(7);

	test_object.add_item(an_int, &error);
	fail_if (error != NULL,
	    "failed to add item: %#{}", isi_error_fmt(error));
}

TEST(test_add_item_existing)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class test_object;
	integer an_int(7);

	test_object.add_item(an_int, &error);
	fail_if (error != NULL,
	    "failed to add item: %#{}", isi_error_fmt(error));

	test_object.add_item(an_int, &error);
	fail_if (error == NULL, "expected EEXIST");
	fail_if (!isi_system_error_is_a(error, EEXIST),
	    "expected EEXIST: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_add_item_full)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class test_object;

	/*
	 * An SBT entry can hold about 8100 bytes, so attempt to add 2500 items
	 * which should be more than enough.
	 */
	for (int i = 0; i < 2500 && error == NULL; ++i) {
		integer an_int(i);
		test_object.add_item(an_int, &error);
	}

	fail_if (error == NULL, "expected ENOSPC");
	fail_if (!isi_system_error_is_a(error, ENOSPC),
	    "expected ENOSPC: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_remove_item)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class test_object;
	integer an_int(7);

	test_object.add_item(an_int, &error);
	fail_if (error != NULL,
	    "failed to add item: %#{}", isi_error_fmt(error));

	test_object.remove_item(an_int, &error);
	fail_if (error != NULL,
	    "failed to remove item: %#{}", isi_error_fmt(error));
}

TEST(test_remove_item_non_existent)
{
	struct isi_error *error = NULL;
	tbi_entry_test_class test_object;
	integer an_int(7);

	test_object.remove_item(an_int, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}
