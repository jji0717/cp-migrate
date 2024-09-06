#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "isi_cbm_time_based_index.h"

#define TBI_ENTRY_TEST_VERSION				1


static void
integer_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	fmt_print(fmt, "%d", arg.ints[0]);
}

/*
 * Since the time based index is a template class, create a test class
 * which derives from it.  The template parameters must be classes (no
 * primitives) since we have to define some functions on it and we need two of
 * them: one for the entry type and one for the item type.
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

	struct fmt_conv_ctx get_print_fmt(void) const
	{
		struct fmt_conv_ctx ctx;

		ctx.func = integer_fmt_conv;
		ctx.arg.ints[0] = value_;

		return ctx;
	}
};

class tbi_entry_test_class : public time_based_index_entry<integer>
{
public:
	tbi_entry_test_class()
		: time_based_index_entry<integer>(TBI_ENTRY_TEST_VERSION) { }
	tbi_entry_test_class(uint32_t item_version)
		: time_based_index_entry<integer>(item_version) { }
	tbi_entry_test_class(btree_key_t key, const void *serialized,
	    size_t serialized_size, isi_error **error_out)
		: time_based_index_entry<integer>(TBI_ENTRY_TEST_VERSION, key,
		    serialized, serialized_size, error_out) { }
	~tbi_entry_test_class() { }

	void clear_items(void)
	{
		items_.clear();
		serialization_up_to_date_ = false;
	}
};

class tbi_test_class : public time_based_index<tbi_entry_test_class, integer>
{
public:
	tbi_test_class() : time_based_index<tbi_entry_test_class, integer>(0)
	{};
	int get_sbt_fd(void) const
	{
		return sbt_fd_;
	}

	uint64_t get_lock_identifier(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index) const
	{
		return time_based_index<tbi_entry_test_class, integer>::
		    get_lock_identifier(process_time, devid, index);
	}

	void _lock_entry(const struct timeval &process_time, ifs_devid_t devid,
	    uint32_t index, bool block, bool exclusive,
	    struct isi_error **error_out) const
	{
		time_based_index<tbi_entry_test_class, integer>::_lock_entry(
		    process_time, devid, index, block, exclusive, error_out);
	}

	void _unlock_entry(const struct timeval &process_time, ifs_devid_t devid,
	    uint32_t index, struct isi_error **error_out) const
	{
		time_based_index<tbi_entry_test_class, integer>::_unlock_entry(
		    process_time, devid, index, error_out);
	}

	void _lock_entry_group(const struct timeval &process_time, ifs_devid_t devid,
	    bool block, bool exclusive, struct isi_error **error_out) const
	{
		time_based_index<tbi_entry_test_class, integer>::
		    _lock_entry_group(process_time, devid, block, exclusive,
		    error_out);
	}

	void _unlock_entry_group(const struct timeval &process_time, ifs_devid_t devid,
	    struct isi_error **error_out) const
	{
		time_based_index<tbi_entry_test_class, integer>::
		    _unlock_entry_group(process_time, devid, error_out);
	}

	tbi_entry_test_class *get_entry(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index, struct isi_error **error_out)
	{
		return time_based_index<tbi_entry_test_class, integer>::
		    get_entry(process_time, devid, index, error_out);
	}

	void remove_entry(tbi_entry_test_class *ent,
	    struct isi_error **error_out)
	{
		time_based_index<tbi_entry_test_class, integer>::remove_entry(
		    ent, error_out);
	}

	const char *get_sbt_name(void) const
	{
		return "isi_cpool_d.tbi_test_class";
	}

	const char *get_lock_filename(void) const
	{
		return
		    "/ifs/.ifsvar/modules/cloud/"
		    "isi_cpool_d.tbi_test_class.lock";
	}
};

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_time_based_index,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 1200,
	.fixture_timeout = 2400);

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;
	get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));
}

TEST_FIXTURE(teardown_suite)
{
}

TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	tbi_test_class test_index;

	delete_sbt(test_index.get_sbt_name(), &error);
	fail_if (error != NULL, "failed to delete SBT (%s): %#{}",
	    test_index.get_sbt_name(), isi_error_fmt(error));
}

TEST_FIXTURE(teardown_test)
{
}

TEST(test_get_lock_identifier)
{
	tbi_test_class test_index;

	struct timeval process_time = {1500,0};	/* 0x5dc */
	ifs_devid_t devid = 13849;	/* 0x3619 */
	uint32_t index = 4562954;	/* 0x45a00a */

	/*
	 * 32 bits of process time + 16 bits of devid + 16 bits of index
	 * (all least significant bits)
	 */
	uint64_t exp_lock_identifier = 0x000005dc3619a00a;
	uint64_t act_lock_identifier =
	    test_index.get_lock_identifier(process_time, devid, index);

	fail_if (act_lock_identifier != exp_lock_identifier,
	    "lock identifier mismatch (exp: 0x%016x act: 0x%016x)",
	    exp_lock_identifier, act_lock_identifier);
}

TEST(test_get_existing_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	int an_int = 57;
	integer item(an_int);
	struct timeval process_time= { 2100, 100 };
	struct timeval entry_process_time = {0};

	test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	/*
	 * Adding the previous item should have created an entry with an index
	 * value of 1.
	 */
	tbi_entry_test_class *existing_entry =
	    test_index.get_entry(process_time, devid, 1, &error);
	fail_if (error != NULL, "failed to get entry: %#{}",
	    isi_error_fmt(error));
	fail_if (existing_entry == NULL);
	entry_process_time = existing_entry->get_process_time();
	fail_if (timercmp(&entry_process_time, &process_time, !=),
	    "process_time mismatch (exp: sec %ld usec %ld act: sec %ld usec %ld)",
	    process_time.tv_sec, process_time.tv_usec,
		entry_process_time.tv_sec, entry_process_time.tv_usec);
	fail_if (existing_entry->get_devid() != devid,
	    "devid mismatch (exp: %u act: %u)",
	    devid, existing_entry->get_devid());
	fail_if (existing_entry->get_index() != 1,
	    "index mismatch (exp: 1 act: %u)", existing_entry->get_index());
	delete existing_entry;
}

TEST(test_get_nonexistent_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	int an_int = 57;
	integer item(an_int);
	struct timeval process_time = { 2100, 100 };

	/* Verify no entry is retrieved (since none was added). */
	tbi_entry_test_class *nonexistent_entry =
	    test_index.get_entry(process_time, devid, 1, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if (nonexistent_entry != NULL);
	isi_error_free(error);
}

TEST(test_remove_existing_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	int an_int = 57;
	integer item(an_int);
	struct timeval process_time = { 34567, 100 };

	test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	tbi_entry_test_class test_entry;
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(1);

	test_index.remove_entry(&test_entry, &error);
	fail_if (error != NULL, "failed to remove entry: %#{}",
	    isi_error_fmt(error));
}

TEST(test_remove_nonexistent_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	tbi_entry_test_class test_entry;
	struct timeval process_time = { 365, 100 };
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(10);

	test_index.remove_entry(&test_entry, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

typedef enum {
	UNKNOWN, UNLOCKED, SHARED, EXCLUSIVE
} lock_type;

static struct {
	tbi_test_class *index_;
	tbi_entry_test_class *entry_;
	lock_type entry_lock_type_;
	lock_type entry_group_lock_type_;
	struct isi_error **error_;
} lock_check_arg;

static lock_type
entry_lock_tester(tbi_test_class *test_index, tbi_entry_test_class *test_entry,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	lock_type type = UNKNOWN;
	bool got_shared, got_exclusive;

	/*
	 * We can determine the type of lock held (if any) on an entry by
	 * attempting to take a shared and (seperately) an exclusive lock and
	 * examining the results:
	 *
	 * ACTUAL	SHARED		EXCL
	 * UN		YES		YES
	 * SH		YES		NO
	 * EX		NO		NO
	 *
	 * For example, we can determine that the entry is unlocked if we are
	 * successful in taking both a shared and exclusive lock on it.
	 */

	/* false: don't block / false: shared */
	test_index->_lock_entry(test_entry->get_process_time(),
	    test_entry->get_devid(), test_entry->get_index(), false, false,
	    &error);
	if (error == NULL) {
		got_shared = true;

		test_index->_unlock_entry(test_entry->get_process_time(),
		    test_entry->get_devid(), test_entry->get_index(), &error);
	}
	else if (isi_system_error_is_a(error, EWOULDBLOCK)) {
		got_shared = false;

		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

	/* false: don't block / true: exclusive */
	test_index->_lock_entry(test_entry->get_process_time(),
	    test_entry->get_devid(), test_entry->get_index(), false, true,
	    &error);
	if (error == NULL) {
		got_exclusive = true;

		test_index->_unlock_entry(test_entry->get_process_time(),
		    test_entry->get_devid(), test_entry->get_index(), &error);
	}
	else if (isi_system_error_is_a(error, EWOULDBLOCK)) {
		got_exclusive = false;

		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

	if (got_shared && got_exclusive)
		type = UNLOCKED;
	else if (got_shared && !got_exclusive)
		type = SHARED;
	else if (!got_shared && !got_exclusive)
		type = EXCLUSIVE;
	else
		error = isi_system_error_new(EINVAL,
		    "impossible lock combination "
		    "(got_shared: NO got_exclusive: YES)");

 out:
	isi_error_handle(error, error_out);

	return type;
}

static lock_type
entry_group_lock_tester(tbi_test_class *test_index,
    tbi_entry_test_class *test_entry, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	lock_type type = UNKNOWN;
	bool got_shared, got_exclusive;

	/*
	 * We can determine the type of lock held (if any) on an entry by
	 * attempting to take a shared and (seperately) an exclusive lock and
	 * examining the results:
	 *
	 * ACTUAL	SHARED		EXCL
	 * UN		YES		YES
	 * SH		YES		NO
	 * EX		NO		NO
	 *
	 * For example, we can determine that the entry is unlocked if we are
	 * successful in taking both a shared and exclusive lock on it.
	 */

	/* false: don't block / false: shared */
	test_index->_lock_entry_group(test_entry->get_process_time(),
	    test_entry->get_devid(), false, false, &error);
	if (error == NULL) {
		got_shared = true;

		test_index->_unlock_entry_group(test_entry->get_process_time(),
		    test_entry->get_devid(), &error);
	}
	else if (isi_system_error_is_a(error, EWOULDBLOCK)) {
		got_shared = false;

		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

	/* false: don't block / true: exclusive */
	test_index->_lock_entry_group(test_entry->get_process_time(),
	    test_entry->get_devid(), false, true, &error);
	if (error == NULL) {
		got_exclusive = true;

		test_index->_unlock_entry_group(test_entry->get_process_time(),
		    test_entry->get_devid(), &error);
	}
	else if (isi_system_error_is_a(error, EWOULDBLOCK)) {
		got_exclusive = false;

		isi_error_free(error);
		error = NULL;
	}
	ON_ISI_ERROR_GOTO(out, error);

	if (got_shared && got_exclusive)
		type = UNLOCKED;
	else if (got_shared && !got_exclusive)
		type = SHARED;
	else if (!got_shared && !got_exclusive)
		type = EXCLUSIVE;
	else
		error = isi_system_error_new(EINVAL,
		    "impossible lock combination "
		    "(got_shared: NO got_exclusive: YES)");

 out:
	isi_error_handle(error, error_out);

	return type;
}

static void *
lock_type_checker(void *arg)
{
	ASSERT(arg == NULL);

	struct isi_error *error = NULL;

	lock_check_arg.entry_lock_type_ =
	    entry_lock_tester(lock_check_arg.index_, lock_check_arg.entry_,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_check_arg.entry_group_lock_type_ =
	    entry_group_lock_tester(lock_check_arg.index_,
	    lock_check_arg.entry_, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, lock_check_arg.error_);

	pthread_exit(NULL);
}

static void
check_lock_types(tbi_test_class *test_index, tbi_entry_test_class *test_entry,
    lock_type &entry_lock_type, lock_type &entry_group_lock_type,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	entry_lock_type = UNKNOWN;
	entry_group_lock_type = UNKNOWN;

	lock_check_arg.index_ = test_index;
	lock_check_arg.entry_ = test_entry;
	lock_check_arg.entry_lock_type_ = UNKNOWN;
	lock_check_arg.entry_group_lock_type_ = UNKNOWN;
	lock_check_arg.error_ = &error;

	pthread_t thread_id;
	pthread_attr_t thread_attr;

	if (pthread_attr_init(&thread_attr) != 0) {
		error = isi_system_error_new(errno,
		    "failed to initialize thread attributes");
		    goto out;
	}

	if (pthread_create(&thread_id, NULL, lock_type_checker, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to create thread to check lock types");
		goto out;
	}

	if (pthread_join(thread_id, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to join lock type checking thread");
		goto out;
	}

	if (pthread_attr_destroy(&thread_attr) != 0) {
		error = isi_system_error_new(errno,
		    "failed to destroy thread attributes");
		goto out;
	}

	entry_lock_type = lock_check_arg.entry_lock_type_;
	entry_group_lock_type = lock_check_arg.entry_group_lock_type_;

 out:
	isi_error_handle(error, error_out);
}

/*
 * Memory leak checking disabled because value differs when running on a local
 * node versus in the build system.  All of the "leaked" memory instances were
 * pthread-related.
 */
TEST(test_lock_unlock_entry_group, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to det devid: %#{}",
	    isi_error_fmt(error));

	tbi_entry_test_class test_entry;
	struct timeval process_time = { 10, 100 };
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(30);

	lock_type entry_lock_type;
	lock_type entry_group_lock_type;

	/* Initially, both an entry and its group should be unlocked. */
	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_group_lock_type);

	/*
	 * Locking an entry group should take an exclusive lock on the entry
	 * group but leave the entries of that group unlocked.
	 */
	test_index.lock_entry_group(test_entry.get_process_time(), &error);
	fail_if (error != NULL, "failed to lock entry group: %#{}",
	    isi_error_fmt(error));

	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != EXCLUSIVE,
	    "unexpected lock type: %d", entry_group_lock_type);

	/* Unlocking an entry should unlock both the entry and its group. */
	test_index.unlock_entry_group(test_entry.get_process_time(), &error);
	fail_if (error != NULL, "failed to unlock entry group: %#{}",
	    isi_error_fmt(error));

	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_group_lock_type);
}

/*
 * Memory leak checking disabled because value differs when running on a local
 * node versus in the build system.  All of the "leaked" memory instances were
 * pthread-related.
 */
TEST(test_lock_unlock_entry, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize index: %#{}",
	    isi_error_fmt(error));

	tbi_entry_test_class test_entry;
	struct timeval process_time = { 10, 100 };
	test_entry.set_process_time(process_time);
	test_entry.set_devid(20);
	test_entry.set_index(30);

	lock_type entry_lock_type;
	lock_type entry_group_lock_type;

	/* Initially, both an entry and its group should be unlocked. */
	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_group_lock_type);

	/*
	 * Locking an entry should take an exclusive lock on the entry and a
	 * shared lock on its group.
	 */
	test_index.lock_entry(&test_entry, &error);
	fail_if (error != NULL, "failed to lock entry: %#{}",
	    isi_error_fmt(error));

	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != EXCLUSIVE,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != SHARED,
	    "unexpected lock type: %d", entry_group_lock_type);

	/* Unlocking an entry should unlock both the entry and its group. */
	test_index.unlock_entry(&test_entry, &error);
	fail_if (error != NULL, "failed to unlock entry: %#{}",
	    isi_error_fmt(error));

	check_lock_types(&test_index, &test_entry, entry_lock_type,
	    entry_group_lock_type, &error);
	fail_if (error != NULL,
	    "lock type checker failed: %#{}", isi_error_fmt(error));
	fail_if (entry_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_lock_type);
	fail_if (entry_group_lock_type != UNLOCKED,
	    "unexpected lock type: %d", entry_group_lock_type);
}

TEST(test_add_item_empty)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	int an_int = 5;
	integer item(an_int);
	struct timeval process_time = { 14, 100 };

	test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	/* Retrieve the item we (should have) just added. */
	btree_key_t key;
	char entry_buf[8192];
	size_t entry_size_out;

	/*
	 * An index value of 0 is reserved (as it represents the entry group
	 * when calculating the lock identifier), so our entry should have been
	 * added at index 1.
	 */
	key = tbi_entry_test_class::to_btree_key(process_time, devid, 1);

	fail_if (ifs_sbt_get_entry_at(test_index.get_sbt_fd(), &key, entry_buf,
	    sizeof entry_buf, NULL, &entry_size_out) != 0,
	    "failed to read entry from SBT");

	/* The retrieved entry should have exactly 1 item. */
	tbi_entry_test_class test_entry(TBI_ENTRY_TEST_VERSION);
	test_entry.set_serialized_entry(TBI_ENTRY_TEST_VERSION, key, entry_buf,
		entry_size_out, &error);
	fail_if(error != NULL, "failed to set serialized entry for "
		"tbi_entry_test_class. %#{}", isi_error_fmt(error));

	std::set<integer> items = test_entry.get_items();
	fail_if (items.size() != 1,
	    "item set size mismatch (exp: 1 act: %zu)", items.size());
	fail_if ((items.begin())->value_ != an_int,
	    "item set item mismatch (exp: 5 act: %d)",
	    (items.begin())->value_);
}

TEST(test_add_item_existing_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	int an_int = 5, another_int = 6;
	integer an_item(an_int), another_item(another_int);
	struct timeval process_time = { 14, 100 };

	/* Add both items to the index. */
	test_index.add_item(TBI_ENTRY_TEST_VERSION, an_item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	test_index.add_item(TBI_ENTRY_TEST_VERSION, another_item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	/* Retrieve the items we (should have) just added. */
	btree_key_t key;
	char entry_buf[8192];
	size_t entry_size_out;

	/*
	 * An index value of 0 is reserved (as it represents the entry group
	 * when calculating the lock identifier), so our entry should have been
	 * added at index 1.
	 */
	key = tbi_entry_test_class::to_btree_key(process_time, devid, 1);

	fail_if (ifs_sbt_get_entry_at(test_index.get_sbt_fd(), &key, entry_buf,
	    sizeof entry_buf, NULL, &entry_size_out) != 0,
	    "failed to read entry from SBT");

	/* The retrieved entry should have exactly 2 item. */
	tbi_entry_test_class test_entry(TBI_ENTRY_TEST_VERSION);
	test_entry.set_serialized_entry(TBI_ENTRY_TEST_VERSION, key, entry_buf,
		entry_size_out, &error);
	fail_if(error != NULL, "Failed to set serialized entry for "
		"tbi_entry_test_class %#{}", isi_error_fmt(error));

	std::set<integer> items = test_entry.get_items();
	fail_if (items.size() != 2,
	    "item set size mismatch (exp: 2 act: %zu)", items.size());
}

TEST(test_add_item_overflow_entry)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	/*
	 * 8149:	max SBT entry size
	 * int:		version
	 * size_t:	number of items
	 */
	size_t max_entry_bytes_available = 8149 - sizeof(int) - sizeof(size_t);
	size_t max_num_objects_per_entry =
	    max_entry_bytes_available / integer::get_packed_size();

	struct timeval process_time = { time(NULL), 100 };

	/*
	 * Add enough items to cause an entry to fill up and spill over into
	 * another entry.
	 */
	for (size_t i = 0; i < max_num_objects_per_entry + 10; ++i) {
		integer item(i);
		test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
		fail_if (error != NULL, "failed to add an item (%d): %#{}",
		    i, isi_error_fmt(error));
	}

	/* We should have two entries: one full and one containing 1 item. */
	btree_key_t key;
	char entry_buf[8149];
	size_t entry_size_out;
	tbi_entry_test_class test_entry;
	std::set<integer> items;

	/* (first entry) */
	key = tbi_entry_test_class::to_btree_key(process_time, devid, 1);

	fail_if (ifs_sbt_get_entry_at(test_index.get_sbt_fd(), &key, entry_buf,
	    sizeof entry_buf, NULL, &entry_size_out) != 0,
	    "failed to read entry from SBT");

	test_entry.set_serialized_entry(TBI_ENTRY_TEST_VERSION, key,
		entry_buf, entry_size_out, &error);
	fail_if(error != NULL, "Failed to set serialized entry for "
		"tbi_entry_test_class %#{}", isi_error_fmt(error));
	
	items = test_entry.get_items();

	fail_if (items.size() != max_num_objects_per_entry,
	    "item set size mismatch (exp: %zu act: %zu)",
	    max_num_objects_per_entry, items.size());

	/* (second entry) */
	key = tbi_entry_test_class::to_btree_key(process_time, devid, 2);

	fail_if (ifs_sbt_get_entry_at(test_index.get_sbt_fd(), &key, entry_buf,
	    sizeof entry_buf, NULL, &entry_size_out) != 0,
	    "failed to read entry from SBT");

	test_entry.set_serialized_entry(TBI_ENTRY_TEST_VERSION, key,
		entry_buf, entry_size_out, &error);
	fail_if(error != NULL, "Failed to set serialized entry for "
		"tbi_entry_test_class %#{}", isi_error_fmt(error));
	
	items = test_entry.get_items();

	fail_if (items.size() != 10,
	    "item set size mismatch (exp: 10 act: %zu)", items.size());
}

TEST(test_remove_existing_item)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	integer item(4);
	struct timeval process_time = { time(NULL), 100 };

	test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	test_index.remove_item(item, process_time, &error);
	fail_if (error != NULL, "failed to remove item: %#{}",
	    isi_error_fmt(error));
}

TEST(test_remove_nonexistent_item)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	integer item(4);
	struct timeval process_time = { time(NULL), 100 };

	test_index.remove_item(item, process_time, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_remove_existing_item_from_nonexistent_group)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	integer item(4);
	time_t seconds = time(NULL);
	struct timeval process_time = { seconds, 100 };

	test_index.add_item(TBI_ENTRY_TEST_VERSION, item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	process_time.tv_sec = seconds + 1;
	test_index.remove_item(item, process_time, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	process_time.tv_sec = seconds - 1;
	test_index.remove_item(item, process_time, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_remove_nonexistent_item_from_existing_group)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	integer existing_item(4), nonexistent_item(5);
	struct timeval process_time = { time(NULL), 100 };

	test_index.add_item(TBI_ENTRY_TEST_VERSION, existing_item, process_time, &error);
	fail_if (error != NULL, "failed to add item: %#{}",
	    isi_error_fmt(error));

	test_index.remove_item(nonexistent_item, process_time, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

TEST(test_remove_item_from_existing_multi_index_group)
{
	struct isi_error *error = NULL;

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	/*
	 * Fill up multiple entries in the same group, then try to remove both
	 * an existing item and a nonexistent item.
	 */

	/*
	 * 8149:	max SBT entry size
	 * int:		version
	 * size_t:	number of items
	 */
	size_t max_entry_bytes_available = 8149 - sizeof(int) - sizeof(size_t);
	size_t max_num_objects_per_entry =
	    max_entry_bytes_available / integer::get_packed_size();

	struct timeval process_time = { time(NULL), 100 };
	uint32_t index = 1;

	tbi_entry_test_class test_entry(TBI_ENTRY_TEST_VERSION);
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);

	for (size_t i = 1; i <= 2 * max_num_objects_per_entry + 10; ++i) {
		integer item(i);
		test_entry.add_item(item, &error);
		fail_if (error != NULL, "failed to add an item (%d): %#{}",
		    i, isi_error_fmt(error));

		if (i % max_num_objects_per_entry == 0 ||
		    i == 2 * max_num_objects_per_entry + 10) {
			test_entry.set_index(index++);

			btree_key_t key = test_entry.to_btree_key();

			const void *serialization;
			size_t serialization_size;
			test_entry.get_serialized_entry(serialization,
			    serialization_size);

			fail_if (ifs_sbt_add_entry(test_index.get_sbt_fd(),
			    &key, serialization, serialization_size,
			    NULL) != 0, "failed to add entry to index: %s",
			    strerror(errno));

			test_entry.clear_items();
		}
	}

	/*
	 * We now have 3 entries for this group.  First attempt to remove an
	 * existing item, then a nonexistent item.
	 */
	integer existing_item(4), nonexistent_item(50000);

	test_index.remove_item(existing_item, process_time, &error);
	fail_if (error != NULL, "failed to remove item: %#{}",
	    isi_error_fmt(error));

	test_index.remove_item(nonexistent_item, process_time, &error);
	fail_if (error == NULL, "expected ENOENT");
	fail_if (!isi_system_error_is_a(error, ENOENT),
	    "expected ENOENT: %#{}", isi_error_fmt(error));
	isi_error_free(error);
}

static void *
get_entry_before_or_at_lock_helper(void *arg)
{
	ASSERT(arg != NULL);

	tbi_test_class *test_index = static_cast<tbi_test_class *>(arg);
	ASSERT(test_index != NULL);

	struct isi_error *error = NULL;
	struct timeval process_time = {0};

	ifs_devid_t devid = get_devid(&error);
	ASSERT(error == NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	tbi_entry_test_class test_entry(TBI_ENTRY_TEST_VERSION);

	/* Lock entries [20, 2], [30, 1], and [50, 1].  Lock entry group 40. */
	process_time.tv_sec = 20;
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(2);
	test_index->lock_entry(&test_entry, &error);
	ASSERT(error == NULL, "failed to lock entry: %#{}",
	    isi_error_fmt(error));

	process_time.tv_sec = 30;
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(1);
	test_index->lock_entry(&test_entry, &error);
	ASSERT(error == NULL, "failed to lock entry: %#{}",
	    isi_error_fmt(error));

	process_time.tv_sec = 50;
	test_entry.set_process_time(process_time);
	test_entry.set_devid(devid);
	test_entry.set_index(1);
	test_index->lock_entry(&test_entry, &error);
	ASSERT(error == NULL, "failed to lock entry: %#{}",
	    isi_error_fmt(error));

	struct timeval lock_entry_group_time = { 40, 0 };
	test_index->lock_entry_group(lock_entry_group_time, &error);
	ASSERT(error == NULL, "failed to lock entry group: %#{}",
	    isi_error_fmt(error));

	pthread_exit(NULL);
}

static void
verify_and_process_entry(tbi_test_class *test_index,
    tbi_entry_test_class *test_entry, bool entry_should_be_null,
    bool more_eligible_entries, bool exp_more_eligible_entries_value,
    struct timeval exp_process_time, ifs_devid_t exp_devid, uint32_t exp_index,
    int linenum)
{
	struct timeval entry_process_time = {0};
	if (entry_should_be_null) {
		fail_if (test_entry != NULL,
		    "should not have retrieved an entry (line: %d)", linenum);
		fail_if (more_eligible_entries !=
		    exp_more_eligible_entries_value,
		    "more_eligible_entries mismatch "
		    "(exp: %s act: %s) (line: %d)",
		    exp_more_eligible_entries_value ? "true" : "false",
		    more_eligible_entries ? "true" : "false", linenum);
		return;
	}

	fail_if (test_entry == NULL,
	    "should have retrieved an entry (line: %d)", linenum);

	fail_if (more_eligible_entries !=
	    exp_more_eligible_entries_value,
	    "more_eligible_entries mismatch (exp: %s act: %s) (line: %d)",
	    exp_more_eligible_entries_value ? "true" : "false",
	    more_eligible_entries ? "true" : "false", linenum);

	entry_process_time = test_entry->get_process_time();
	fail_if (timercmp(&entry_process_time, &exp_process_time, !=),
	    "process_time mismatch (exp: sec %ld usec %ld act: sec %ld usec %ld) (line: %d)",
	    exp_process_time.tv_sec, exp_process_time.tv_usec,
		entry_process_time.tv_sec, entry_process_time.tv_usec, linenum);

	fail_if (test_entry->get_devid() != exp_devid,
	    "devid mismatch (exp: %u act: %u) (line: %d)",
	    exp_devid, test_entry->get_devid(), linenum);

	fail_if (test_entry->get_index() != exp_index,
	    "index mismatch (exp: %u act: %u) (line: %d)",
	    exp_index, test_entry->get_index(), linenum);

	/* "Process" the entry. */
	btree_key_t key = test_entry->to_btree_key();
	fail_if (ifs_sbt_remove_entry(test_index->get_sbt_fd(), &key) != 0,
	    "failed to process entry: %s (line %d)", strerror(errno), linenum);

	/* Since get_entry_before_or_at locked the entry, unlock it now. */
	struct isi_error *error = NULL;
	test_index->unlock_entry(test_entry, &error);
	fail_if (error != NULL, "failed to unlock entry (line: %d): %#{}",
	    linenum, isi_error_fmt(error));
}

/*
 * Memory leak checking disabled because value differs when running on a local
 * node versus in the build system.  All of the "leaked" memory instances were
 * pthread-related.
 */
TEST(test_get_entry_before_or_at, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	struct timeval process_time = {0};

	tbi_test_class test_index;
	test_index.initialize(&error);
	fail_if (error != NULL, "failed to initialize test index: %#{}",
	    isi_error_fmt(error));

	ifs_devid_t devid = get_devid(&error);
	fail_if (error != NULL, "failed to get devid: %#{}",
	    isi_error_fmt(error));

	/*
	 * Add some entries.  Since we aren't actually processing them, we
	 * don't need to stuff items into them.
	 *
	 * These for-loops will create some entry groups with multiple entries,
	 * specifically:
	 *
	 * entry(10,100): idx: 1, 2
	 * entry(20,100): idx: 1, 2, 3
	 * entry(30,100): idx: 1
	 * entry(40,100): idx: 1, 2
	 * entry(50,100): idx: 1, 2, 3
	 */
	tbi_entry_test_class test_entry(TBI_ENTRY_TEST_VERSION);
	for (int i = 1; i <= 5; ++i) {
		for (int j = 0; j <= i % 3; ++j) {
			process_time.tv_sec = i * 10;
			process_time.tv_usec = 100;
			test_entry.set_process_time(process_time);
			test_entry.set_devid(devid);
			test_entry.set_index(j + 1);

			btree_key_t key = test_entry.to_btree_key();

			const void *serialization;
			size_t serialization_size;
			test_entry.get_serialized_entry(serialization,
			    serialization_size);
			fail_if (ifs_sbt_add_entry(test_index.get_sbt_fd(),
			    &key, serialization, serialization_size,
			    NULL) != 0,
			    "failed to add entry: %s", strerror(errno));
		}
	}

	/* Lock some of the entries and some of the entry groups. */
	/* Lock entries [20, 2], [30, 1], and [50, 1].  Lock entry group 40. */
	////另起一个线程锁住entries:entry(20),idx:2 entry(30),idx:1 entry(50),idx:1, 把整个entry group是40的全锁住
	pthread_t thread_id;
	fail_if (pthread_create(&thread_id, NULL,
	    get_entry_before_or_at_lock_helper, &test_index) != 0,
	    "failed to create locker thread: %s", strerror(errno));
	fail_if (pthread_join(thread_id, NULL) != 0,
	    "failed to join locker thread: %s", strerror(errno));

	/*
	 * Retrieve entries from the index with varying upper limits on process
	 * time.  First, no entry should be returned if the upper limit is 0
	 * (or anything less than 10).
	 */
	bool more_eligible_entries;
	tbi_entry_test_class *retrieved_entry = NULL;

	retrieved_entry =
	    test_index.get_entry_before_or_at(0, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	verify_and_process_entry(&test_index, retrieved_entry, true,
	    more_eligible_entries, false, process_time, devid, 10, __LINE__);

	// if (retrieved_entry == NULL)
	// {
	// 	printf("\nretrieved null, more_eligible_entries:%d\n",more_eligible_entries);
	// }
	// return;
	
	/*
	 * There are 5 entries with a process time less than 25, but 1 of them
	 * ([20, 2]) is locked.  Verify we retrieve the other 4.
	 */
	retrieved_entry =
	    test_index.get_entry_before_or_at(25, more_eligible_entries, 
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	////期望retrieved_entry的process_time是:(10,100) idx:1
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true/*能够读到更多的entry*/, process_time, devid, 1/*idx: 1*/, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	retrieved_entry =
	    test_index.get_entry_before_or_at(25, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	//期望读到的retrtieved_entry的process_time:(10,100) idx:2
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true, process_time, devid, 2, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	retrieved_entry =
	    test_index.get_entry_before_or_at(25, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 20;
	process_time.tv_usec = 100;
	//期望读到的retrieved_entry是process_time:(20,100), idx:1
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true, process_time, devid, 1, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	retrieved_entry =
	    test_index.get_entry_before_or_at(25, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 20;
	process_time.tv_usec = 100;
	///期望读到的retrieved_entry:process_time: (20,100) idx: 3 （20,100),idx:2被锁住了
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true, process_time, devid, 3, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	/*
	 * No entry should be retrieved for a process time upper limit of 25
	 * since we've now consumed them all (except for [20, 2] which is still
	 * locked).
	 */
	retrieved_entry =
	    test_index.get_entry_before_or_at(25, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	////已经读不到entry了,process_time比25小的已经读完了
	verify_and_process_entry(&test_index, retrieved_entry, true/*读不到entry了*/,
	    more_eligible_entries, false, process_time, devid, 10, __LINE__);

	/*
	 * No entry should be retrieved for a process time upper limit of 35
	 * either; even though an entry exists with process time 30, it is
	 * locked.
	 */
	retrieved_entry =
	    test_index.get_entry_before_or_at(30, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	////读不到entry了,(30,100) idx:1被锁住了
	verify_and_process_entry(&test_index, retrieved_entry, true,
	    more_eligible_entries, false, process_time, devid, 10, __LINE__);

	/*
	 * In order to retrieve our next entry, we need a process time upper
	 * limit no less than 50 since the 40 entry group is locked
	 * (exclusively).  Also, we should retrieve [50, 2] since [50, 1] is
	 * locked.
	 */
	retrieved_entry =
	    test_index.get_entry_before_or_at(55, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 50;
	process_time.tv_usec = 100;
	////entry(50,100)idx:1 和整个entry(40)都被锁住了,所以只能读到(50,100)idx:2
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true, process_time, devid, 2, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	/*
	 * Finally, retrieve [50, 3], then verify that we can't retrieve any
	 * more entries.
	 */
	retrieved_entry =
	    test_index.get_entry_before_or_at(55, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 50;
	process_time.tv_usec = 100;
	///读到entry(50,100) idx:3
	verify_and_process_entry(&test_index, retrieved_entry, false,
	    more_eligible_entries, true, process_time, devid, 3, __LINE__);
	delete retrieved_entry;
	retrieved_entry = NULL;

	retrieved_entry =
	    test_index.get_entry_before_or_at(55, more_eligible_entries,
	    &error);
	fail_if (error != NULL, "get_entry_before_or_at failed: %#{}",
	    isi_error_fmt(error));
	process_time.tv_sec = 10;
	process_time.tv_usec = 100;
	////全部读完了,读不到了
	verify_and_process_entry(&test_index, retrieved_entry, true,
	    more_eligible_entries, false, process_time, devid, 10, __LINE__);
}
