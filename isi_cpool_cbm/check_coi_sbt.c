#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/btree/btree.h>
#include <ifs/sbt/sbt.h>

#include <isi_util/check_helper.h>
#include <isi_sbtree/sbtree.h>

#include "isi_cbm_coi_sbt.h"

TEST_FIXTURE(suite_init);
TEST_FIXTURE(suite_uninit);
TEST_FIXTURE(test_init);
TEST_FIXTURE(test_uninit);

SUITE_DEFINE_FOR_FILE(check_coi_sbt,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_init,
    .suite_teardown = suite_uninit,
    .test_setup = test_init,
    .test_teardown = test_uninit,
    .timeout = 60,
    .fixture_timeout = 1200);


static const char *btree_name = "coi_sbt_test";
static int root_fd, sbt128b_fd;
static const enc_t enc = ENC_DEFAULT;

TEST_FIXTURE(suite_init)
{
	int error;

	root_fd = sbt_open_root_dir(O_WRONLY);
	fail_if(root_fd < 0, "Failed to open SBT root dir, error: %s.\n",
	    strerror(errno));

	error = enc_unlinkat(root_fd, btree_name, enc, 0);

	error = ifs_sbt_create(root_fd, btree_name, enc, SBT_128BIT);
	fail_if(error < 0, "Failed to create SBT: %s, error: %s.\n",
	    btree_name, strerror(errno));
}

TEST_FIXTURE(test_init)
{
	sbt128b_fd = enc_openat(root_fd, btree_name, enc, O_RDWR);
	fail_if(sbt128b_fd < 0, "Failed to open SBT tree %s, error: %s.\n",
	    btree_name, strerror(errno));
}

TEST_FIXTURE(test_uninit) {
	int error;

	error = close(sbt128b_fd);
	fail_if(error < 0, "Failed to close SBT tree %s, error: %s.\n",
	    btree_name, strerror(errno));
}

TEST_FIXTURE(suite_uninit) {
	int error;

	error = enc_unlinkat(root_fd, btree_name, enc, 0);
	fail_if(error < 0, "Failed to unlink SBT tree %s, error: %s.\n",
	    btree_name, strerror(errno));

	error = close(root_fd);
	fail_if(error < 0, "Failed to close SBT root dir, error: %s.\n",
	    strerror(errno));
}

/**
 * add two same keys, the second add should fail with EINVAL
 */
TEST(test_add_two_same_keys)
{
	struct isi_error *error = NULL;



	struct coi_sbt_key key = {99192913919313, 23444555, 12313131313131313};

	btree_key_t hk = {};
	uint64_t value = 987654321;
	coi_sbt_add_entry(sbt128b_fd, &key, &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));


	value = 1234567;
	coi_sbt_add_entry(sbt128b_fd, &key, &value,
	    sizeof(value), &hk, &error);

	fail_if(!error || !isi_system_error_is_a(error, EINVAL),
	    "Failed to add key to the HSBT: %#{}.\n", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * add two different keys, all should succeed
 */
TEST(test_add_two_different_keys)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key = {99192913919313, 23444555, 12313131313131313};

	btree_key_t hk = {};
	uint64_t value = 987654321;
	coi_sbt_add_entry(sbt128b_fd, &key, &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct coi_sbt_key key2 = {99192913919313, 23444556, 12313131313131313};

	value = 1234567;
	coi_sbt_add_entry(sbt128b_fd, &key2, &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key2, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * This test adding the key and find it. It check if the returned hash
 * keys are matched or the value are matched.
 */
TEST(test_add_and_find_key)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key = {123456, 23444555, 0};
	const char *value = "abcdefg";
	btree_key_t hk = {};
	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	bool found = coi_sbt_get_entry(sbt128b_fd, &key,
	    NULL, NULL, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	char val[256] = {0};
	size_t val_size = sizeof(val);

	found = coi_sbt_get_entry(sbt128b_fd, &key, val,
	   &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(value) || strcmp(value, val),
	    "the values are not equal");

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_add_remove_and_find_key)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key = {123456, 23444555, 0};
	const char *value = "abcdefg";
	btree_key_t hk = {};
	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	bool found = coi_sbt_get_entry(sbt128b_fd, &key, NULL,
	    NULL, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	found = coi_sbt_get_entry(sbt128b_fd, &key, NULL,
	    NULL, &hk, &error);

	fail_if(error, "Error when finding deleted entry from b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(found, "Deleted entry is found in the B-tree.\n");
}


TEST(test_cond_mod_entry)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key = {123456, 23444555, 0};
	const char *value = "abcdefg";
	btree_key_t hk = {};
	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// test the case where we have the correct value
	const char *new_value = "super secret";

	coi_sbt_cond_mod_entry(sbt128b_fd, &key, new_value,
	    strlen(new_value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(error, "Failed to do conditional mod to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	char val[256] = {0};
	size_t val_size = sizeof(val);
	bool found = coi_sbt_get_entry(sbt128b_fd, &key,
	    val, &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(new_value) || strcmp(new_value, val),
	    "the values are not equal");

	// now test the changed situation
	coi_sbt_cond_mod_entry(sbt128b_fd, &key, value,
	    strlen(value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(!error || !isi_system_error_is_a(error, ERANGE),
	    "Failed to do conditional modification for HSBT: %#{}.\n",
	     isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_cond_set_to_null)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key = {123456, 23444555, 0};
	const char *value = "abcdefg";
	btree_key_t hk = {};
	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// test the case where we have the correct value
	const char *new_value = "super secret";

	coi_sbt_cond_mod_entry(sbt128b_fd, &key, new_value,
	    strlen(new_value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(error, "Failed to do conditional mod to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	char val[256] = {0};
	size_t val_size = sizeof(val);
	bool found = coi_sbt_get_entry(sbt128b_fd, &key,
	    val, &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(new_value) || strcmp(new_value, val),
	    "the values are not equal");

	// now test the changed situation
	coi_sbt_cond_mod_entry(sbt128b_fd, &key, NULL,
	    0, NULL, BT_CM_BUF, new_value, strlen(new_value), NULL,
	    &error);


	memset(val, 0, sizeof(val));
	found = coi_sbt_get_entry(sbt128b_fd, &key,
	    val, &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != 0,
	    "the values are not equal");

	fail_if(error, "Failed to do conditional mod to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * Non-conditional modification of bulk-add
 */
TEST(test_bulk_add_succeed)
{
	struct isi_error *error = NULL;

	struct coi_sbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

	struct coi_sbt_key key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	struct coi_sbt_key key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	coi_sbt_bulk_op(2, bulk_ops, &error);

	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key2, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * Non-conditional modification of bulk-add
 */
TEST(test_bulk_mod_succeed)
{
	struct isi_error *error = NULL;

	struct coi_sbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

	struct coi_sbt_key key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	struct coi_sbt_key key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	coi_sbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// now switch the values
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_MOD;
	bulk_ops[0].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_MOD;
	bulk_ops[1].bulk_entry.entry_buf = &value;

	coi_sbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key2, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * Non-conditional modification of bulk-add-delete
 */
TEST(test_bulk_add_delete)
{
	struct isi_error *error = NULL;

	struct coi_sbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

	struct coi_sbt_key key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	coi_sbt_bulk_op(1, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct coi_sbt_key key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	// now switch the values
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_DEL;

	coi_sbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key2, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * conditional bulk-add-delete
 */
TEST(test_bulk_cond_add_delete)
{
	struct isi_error *error = NULL;

	struct coi_sbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

	struct coi_sbt_key key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	coi_sbt_bulk_op(1, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct coi_sbt_key key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	// now fake the value change, it should fail
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_DEL;
	bulk_ops[0].bulk_entry.cm = BT_CM_BUF;
	bulk_ops[0].bulk_entry.old_entry_buf = &value2; // this should fail
	bulk_ops[0].bulk_entry.old_entry_size = sizeof(value2);

	coi_sbt_bulk_op(2, bulk_ops, &error);
	fail_if(!error || !isi_system_error_is_a(error, ERANGE),
	    "Failed to do conditional modification for HSBT: %#{}.\n",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	bulk_ops[0].bulk_entry.old_entry_buf = &value; // this should succeed

	coi_sbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add and delete key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	coi_sbt_remove_entry(sbt128b_fd, &key2, &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

static void
test_version_common(int sbt128b_fd, enum SBT_SEARCH_DIR sdir,
    struct coi_sbt_key *key, bool expect_found, struct coi_sbt_key *expect)
{
	struct isi_error *error = NULL;
	btree_key_t hk = {};
	struct coi_sbt_key out_key = {0};
	bool found = false;

	switch(sdir) {
	case SD_NEXT:
		found = coi_sbt_get_next_version(sbt128b_fd, key,
		    NULL, NULL, &hk, &out_key, &error);
		break;
	case SD_PREV:
		found = coi_sbt_get_prev_version(sbt128b_fd, key,
		    NULL, NULL, &hk, &out_key, &error);
		break;
	case SD_LAST:
		found = coi_sbt_get_last_version(sbt128b_fd, key,
		    NULL, NULL, &hk, &out_key, &error);
		break;
	default:
		fail("Do not support the operation %d", sdir);
	}

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(expect_found != found, "Do not meet expectation, "
	    "expect found=%d actual found=%d, dir=%d, "
	    "key: %lx %lx %lx", expect_found, found, sdir,
	    key->high_, key->low_, key->snapid_);

	if (expect_found && expect) {
		fail_if(out_key.high_ != expect->high_ ||
		    out_key.low_ != expect->low_ ||
		    out_key.snapid_ != expect->snapid_,
		    "Found version does not match expectation, "
		    "actual: %lx, %lx, %lx, expected: %lx %lx %lx.\n",
		    out_key.high_, out_key.low_, out_key.snapid_,
		    expect->high_, expect->low_, expect->snapid_);
	}
}

/**
 * Test various versioning navigations.
 */
TEST(test_versioning)
{
	struct isi_error *error = NULL;

	struct coi_sbt_key key1 = {0x11e3c1b641c3e3deU, 0xb25a0843070045b1U, 0};
	struct coi_sbt_key key2 = {0x11e3c1cff0326dd9U, 0xb25a0843070045b1U, 0};
	struct coi_sbt_key key = key1;
	struct coi_sbt_key expect = {0};

	const char *value = "abcdefg";
	btree_key_t hk = {};
	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	key.snapid_ = 1;

	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	key.snapid_ = 1000;

	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// insert another object, this is known to collide with the key1
	key = key2;

	coi_sbt_add_entry(sbt128b_fd, &key, value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// find the next version, expect to found version 1
	key = key1;
	expect = key;
	expect.snapid_ = 1;

	test_version_common(sbt128b_fd, SD_NEXT, &key, true, &expect);

	// to find version 1's next version, expect to find version 1000
	key.snapid_ = 1;
	expect = key;
	expect.snapid_ = 1000;

	test_version_common(sbt128b_fd, SD_NEXT, &key, true, &expect);

	// to find version 500's next version, expect to find version 1000
	key.snapid_ = 500;
	expect = key;
	expect.snapid_ = 1000;

	test_version_common(sbt128b_fd, SD_NEXT, &key, true, &expect);

	// to find version 1000's next version, nothing expected
	key.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_NEXT, &key, false, NULL);


	// to find version 1000's previous version, version 1 expected
	key.snapid_ = 1000;
	expect = key;
	expect.snapid_ = 1;
	test_version_common(sbt128b_fd, SD_PREV, &key, true, &expect);

	// to find non-existent version 500's previous version,
	// version 1 expected
	key.snapid_ = 500;
	expect = key;
	expect.snapid_ = 1;
	test_version_common(sbt128b_fd, SD_PREV, &key, true, &expect);

	// to find version 1's previous version, version 0 expected
	key.snapid_ = 1;
	expect = key;
	expect.snapid_ = 0;
	test_version_common(sbt128b_fd, SD_PREV, &key, true, &expect);

	// to find version 0's previous version, nothing expected
	key.snapid_ = 0;
	test_version_common(sbt128b_fd, SD_PREV, &key, false, NULL);

	// to find version 0's last version, expect to find version 1000
	key.snapid_ = 0;
	expect = key;
	expect.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// to find version 1's last version, expect to find version 1000
	key.snapid_ = 1;
	expect = key;
	expect.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// to find version 1000's last version, expect to find version 1000
	key.snapid_ = 1000;
	expect = key;
	expect.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// to find non-existent version out-of-the-range 1001's last version,
	// expect to find version 1000
	key.snapid_ = 1001;
	expect = key;
	expect.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// to find non-existent version in-the-range 500's last version,
	// expect to find version 1000
	key.snapid_ = 500;
	expect = key;
	expect.snapid_ = 1000;
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// test the single version object
	key = key2;
	// find version 0's next, nothing expected:
	test_version_common(sbt128b_fd, SD_NEXT, &key, false, NULL);

	// find version 0's previous, nothing expected:
	test_version_common(sbt128b_fd, SD_PREV, &key, false, NULL);

	expect = key;
	// find version 0's last, itself is expected:
	test_version_common(sbt128b_fd, SD_LAST, &key, true, &expect);

	// test the non-existent object
	key.high_ = 123456;
	key.low_ = 23444554;
	key.snapid_ = 0;
	// find version 0's next, nothing expected:
	test_version_common(sbt128b_fd, SD_NEXT, &key, false, NULL);

	// find version 0's previous, nothing expected:
	test_version_common(sbt128b_fd, SD_PREV, &key, false, NULL);

	// find version 0's last, nothing expected:
	test_version_common(sbt128b_fd, SD_LAST, &key, false, NULL);
}

