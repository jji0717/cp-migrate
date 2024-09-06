#include <ifs/ifs_syscalls.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "unit_test_helpers.h"
#include "isi_cloud_object_id.h"

TEST_FIXTURE(setup_suite)
{
	/*
	 * To prevent bogus memory leak messages stemming from statically
	 * allocated memory.
	 */
	isi_cloud_object_id object_id;
}

SUITE_DEFINE_FOR_FILE(check_isi_cloud_object_id,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.timeout = 0,
	.fixture_timeout = 1200);

class isi_cloud_object_id_ut : public isi_cloud_object_id
{
public:
	isi_cloud_object_id_ut()
		: isi_cloud_object_id() { }
	isi_cloud_object_id_ut(uint64_t high, uint64_t low)
		: isi_cloud_object_id(high, low) { }
	isi_cloud_object_id_ut(const isi_cloud_object_id &id)
		: isi_cloud_object_id(id) { }

	uint64_t get_high(void) const { return high_; }
	uint64_t get_low(void) const { return low_; }
	const std::string &get_string_form(void) const { return string_form_; }
};

TEST(test_default_ctor)
{
	isi_cloud_object_id_ut object_id;

	fail_if (object_id.get_high() != 0,
	    "high value mismatch (exp: 0 act: %lu",
	    object_id.get_high());
	fail_if (object_id.get_low() != 0,
	    "low value mismatch (exp: 0 act: %lu",
	    object_id.get_low());
}

TEST(test_high_low_ctor)
{
	isi_cloud_object_id_ut object_id(1, 2);

	fail_if (object_id.get_high() != 1,
	    "high value mismatch (exp: 1 act: %lu",
	    object_id.get_high());
	fail_if (object_id.get_low() != 2,
	    "low value mismatch (exp: 2 act: %lu",
	    object_id.get_low());
}

TEST(test_copy_ctor)
{
	isi_cloud_object_id_ut object_id_1(10, 20);
	isi_cloud_object_id_ut object_id_2(object_id_1);

	fail_if (object_id_2.get_high() != 10,
	    "high value mismatch (exp: 10 act: %lu",
	    object_id_2.get_high());
	fail_if (object_id_2.get_low() != 20,
	    "low value mismatch (exp: 20 act: %lu",
	    object_id_2.get_low());
}

TEST(test_set)
{
	isi_cloud_object_id_ut object_id(100, 200);
	object_id.set(2, 4);

	fail_if (object_id.get_high() != 2,
	    "high value mismatch (exp: 2 act: %lu",
	    object_id.get_high());
	fail_if (object_id.get_low() != 4,
	    "low value mismatch (exp: 4 act: %lu",
	    object_id.get_low());
}

TEST(test_generate)
{
	isi_cloud_object_id_ut object_id;
	for (int i = 0;i<5;i++)
	{
		object_id.generate();
		std::string str = object_id.to_string();
		printf("\nisi-guid:%s, len:%lu\n", str.c_str(), str.size());
	}
	// can't verify the high/low values since a GUID is being generated
	// but this test ensures that the command doesn't seg fault, leak
	// memory, etc.
}

TEST(test_assignment_operator)
{
	isi_cloud_object_id_ut object_id_1, object_id_2(3, 6);
	object_id_1 = object_id_2;

	fail_if (object_id_1.get_high() != 3,
	    "high value mismatch (exp: 3 act: %lu",
	    object_id_1.get_high());
	fail_if (object_id_1.get_low() != 6,
	    "low value mismatch (exp: 6 act: %lu",
	    object_id_1.get_low());
}

TEST(test_comparator_operators)
{
	isi_cloud_object_id_ut object_id_1, object_id_2(3, 6), object_id_3;

	fail_if (object_id_1 == object_id_2,
	    "object_id_1 shouldn't be equivalent to object_id_2");
	fail_if (object_id_1 != object_id_3,
	    "object_id_1 should be equivalent to object_id_3");
	fail_if (object_id_2 == object_id_3,
	    "object_id_2 shouldn't be equivalent to object_id_3");
}

TEST(test_pack_unpack)
{
	isi_cloud_object_id_ut before, after;
	void *packed = NULL, *temp = NULL;

	before.set(23, 42);

	packed = malloc(before.get_packed_size());
	fail_if (packed == NULL, "failed to allocate packed buffer");
	temp = packed;

	before.pack(&packed);
	packed = temp;

	after.unpack(&packed);
	packed = temp;

	fail_if (before != after,
	    "object IDs before and after serialization/deserialization "
	    "should be equivalent (before.high: %lu before.low: %lu "
	    "after.high: %lu after.low: %lu)",
	    before.get_high(), before.get_low(), after.get_high(),
	    after.get_low());

	free(packed);
}
