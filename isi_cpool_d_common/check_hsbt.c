#include "hsbt.h"

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

TEST_FIXTURE(suite_init);
TEST_FIXTURE(suite_uninit);
TEST_FIXTURE(test_init);
TEST_FIXTURE(test_uninit);

SUITE_DEFINE_FOR_FILE(check_hsbt,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_init,
    .suite_teardown = suite_uninit,
    .test_setup = test_init,
    .test_teardown = test_uninit,
    .timeout = 60,
    .fixture_timeout = 1200);


static const char *btree_name = "hsbt_test";
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
 * test add a long key.
 */
TEST(test_add_key)
{
	struct isi_error *error = NULL;

	const char *long_key = "0123456789012345678901234567890123456789"
	    "012345678901234567890123456789012345678901234567890123456789"
	    "0123456789";
	const char *value = "abcdefg";
	btree_key_t hk = {};
	hsbt_add_entry(sbt128b_fd, long_key, strlen(long_key), value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, long_key, strlen(long_key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * test find in an emtpty b-tree.
 */
TEST(test_find_empty_key)
{
	struct isi_error *error = NULL;

	const char *long_key = "0123456789012345678901234567890123456789"
	    "012345678901234567890123456789012345678901234567890123456789"
	    "0123456789";
	btree_key_t hk = {};
	bool found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key),
	     NULL, NULL, &hk, &error);

	fail_if(error, "Error when finding empty b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(found, "Entry found in an emtpry B-tree.\n");
}

struct int_192b {
	uint64_t p1;
	uint64_t p2;
	uint64_t p3;
};

/**
 * add two same keys, the second add should fail with EINVAL
 */
TEST(test_add_two_same_keys)
{
	struct isi_error *error = NULL;



	struct int_192b key = {99192913919313, 23444555, 12313131313131313};

	btree_key_t hk = {};
	uint64_t value = 987654321;
	hsbt_add_entry(sbt128b_fd, &key, sizeof(key), &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));


	value = 1234567;
	hsbt_add_entry(sbt128b_fd, &key, sizeof(key), &value,
	    sizeof(value), &hk, &error);

	fail_if(!error || !isi_system_error_is_a(error, EINVAL),
	    "Failed to add key to the HSBT: %#{}.\n", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * add two different keys, all should succeed
 */
TEST(test_add_two_different_keys)
{
	struct isi_error *error = NULL;

	struct int_192b key = {99192913919313, 23444555, 12313131313131313};

	btree_key_t hk = {};
	uint64_t value = 987654321;
	hsbt_add_entry(sbt128b_fd, &key, sizeof(key), &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct int_192b key2 = {99192913919313, 23444556, 12313131313131313};

	value = 1234567;
	hsbt_add_entry(sbt128b_fd, &key2, sizeof(key2), &value,
	    sizeof(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key2, sizeof(key2), &error);

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

	const char *long_key = "0123456789012345678901234567890123456789"
	    "012345678901234567890123456789012345678901234567890123456789"
	    "0123456789";
	const char *value = "abcdefg";
	btree_key_t hk = {};
	hsbt_add_entry(sbt128b_fd, long_key, strlen(long_key), value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	bool found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key),
	    NULL, NULL, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	char val[256] = {0};
	size_t val_size = sizeof(val);

	found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key), val,
	   &val_size, &hk2, &error);


	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(value) || strcmp(value, val),
	    "the values are not equal");

	hsbt_remove_entry(sbt128b_fd, long_key, strlen(long_key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_add_remove_and_find_key)
{
	struct isi_error *error = NULL;

	char key = 'a';
	const char *value = "abcdefg";
	btree_key_t hk = {};
	hsbt_add_entry(sbt128b_fd, &key, sizeof(key), value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	bool found = hsbt_get_entry(sbt128b_fd, &key, sizeof(key), NULL,
	    NULL, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	found = hsbt_get_entry(sbt128b_fd, &key, sizeof(key), NULL,
	    NULL, &hk, &error);

	fail_if(error, "Error when finding deleted entry from b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(found, "Deleted entry is found in the B-tree.\n");
}


TEST(test_cond_mod_entry)
{
	struct isi_error *error = NULL;

	const char *long_key = "0123456789012345678901234567890123456789"
	    "012345678901234567890123456789012345678901234567890123456789"
	    "0123456789";
	const char *value = "abcdefg";
	btree_key_t hk = {};
	hsbt_add_entry(sbt128b_fd, long_key, strlen(long_key), value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// test the case where we have the correct value
	const char *new_value = "super secret";

	hsbt_cond_mod_entry(sbt128b_fd, long_key, strlen(long_key), new_value,
	    strlen(new_value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(error, "Failed to do conditional mod to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	char val[256] = {0};
	size_t val_size = sizeof(val);
	bool found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key),
	    val, &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(new_value) || strcmp(new_value, val),
	    "the values are not equal");

	// now test the changed situation
	hsbt_cond_mod_entry(sbt128b_fd, long_key, strlen(long_key), value,
	    strlen(value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(!error || !isi_system_error_is_a(error, ERANGE),
	    "Failed to do conditional modification for HSBT: %#{}.\n",
	     isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	hsbt_remove_entry(sbt128b_fd, long_key, strlen(long_key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_cond_set_to_null)
{
	struct isi_error *error = NULL;

	const char *long_key = "0123456789012345678901234567890123456789"
	    "012345678901234567890123456789012345678901234567890123456789"
	    "0123456789";
	const char *value = "abcdefg";
	btree_key_t hk = {};
	hsbt_add_entry(sbt128b_fd, long_key, strlen(long_key), value,
	    strlen(value), &hk, &error);

	fail_if(error, "Failed to add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// test the case where we have the correct value
	const char *new_value = "super secret";

	hsbt_cond_mod_entry(sbt128b_fd, long_key, strlen(long_key), new_value,
	    strlen(new_value), NULL, BT_CM_BUF, value, strlen(value), NULL,
	    &error);

	fail_if(error, "Failed to do conditional mod to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	btree_key_t hk2 = {};
	char val[256] = {0};
	size_t val_size = sizeof(val);
	bool found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key),
	    val, &val_size, &hk2, &error);

	fail_if(error, "Error when finding in b-tree: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(!found, "Entry just added is not found.\n");

	fail_if(hk2.keys[0] != hk.keys[0] || hk2.keys[1] != hk.keys[1],
	    "the hash keys are not equal");

	fail_if(val_size != strlen(new_value) || strcmp(new_value, val),
	    "the values are not equal");

	// now test the changed situation
	hsbt_cond_mod_entry(sbt128b_fd, long_key, strlen(long_key), NULL,
	    0, NULL, BT_CM_BUF, new_value, strlen(new_value), NULL,
	    &error);


	memset(val, 0, sizeof(val));
	found = hsbt_get_entry(sbt128b_fd, long_key, strlen(long_key),
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

	hsbt_remove_entry(sbt128b_fd, long_key, strlen(long_key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

/**
 * Non-conditional modification of bulk-add
 */
TEST(test_bulk_add_succeed)
{
	struct isi_error *error = NULL;

	struct hsbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct hsbt_bulk_entry));

	struct int_192b key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	struct int_192b key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	hsbt_bulk_op(2, bulk_ops, &error);

	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key2, sizeof(key2), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * Non-conditional modification of bulk-add
 */
TEST(test_bulk_mod_succeed)
{
	struct isi_error *error = NULL;

	struct hsbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct hsbt_bulk_entry));

	struct int_192b key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	struct int_192b key2 = {0, 0, 0};

	uint64_t value2 = 12345678;

	bulk_ops[1].is_hash_btree = true;
	bulk_ops[1].key = &key2;
	bulk_ops[1].key_len = sizeof(key2);

	bulk_ops[1].bulk_entry.fd = sbt128b_fd;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[1].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.entry_size = sizeof(value2);
	bulk_ops[1].bulk_entry.entry_flags = flags;

	hsbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	// now switch the values
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_MOD;
	bulk_ops[0].bulk_entry.entry_buf = &value2;
	bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_MOD;
	bulk_ops[1].bulk_entry.entry_buf = &value;

	hsbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key2, sizeof(key2), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * Non-conditional modification of bulk-add-delete
 */
TEST(test_bulk_add_delete)
{
	struct isi_error *error = NULL;

	struct hsbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct hsbt_bulk_entry));

	struct int_192b key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	hsbt_bulk_op(1, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct int_192b key2 = {0, 0, 0};

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

	hsbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key2, sizeof(key2), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}


/**
 * conditional bulk-add-delete
 */
TEST(test_bulk_cond_add_delete)
{
	struct isi_error *error = NULL;

	struct hsbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};


	memset(bulk_ops, 0, 2 * sizeof(struct hsbt_bulk_entry));

	struct int_192b key = {0xAFDDCDBAC0988, 23444555, 12313131313131313};

	uint64_t value = 987654321;

	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &key;
	bulk_ops[0].key_len = sizeof(key);

	bulk_ops[0].bulk_entry.fd = sbt128b_fd;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = &value;
	bulk_ops[0].bulk_entry.entry_size = sizeof(value);
	bulk_ops[0].bulk_entry.entry_flags = flags;

	hsbt_bulk_op(1, bulk_ops, &error);
	fail_if(error, "Failed to bulk add key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	struct int_192b key2 = {0, 0, 0};

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

	hsbt_bulk_op(2, bulk_ops, &error);
	fail_if(!error || !isi_system_error_is_a(error, ERANGE),
	    "Failed to do conditional modification for HSBT: %#{}.\n",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	bulk_ops[0].bulk_entry.old_entry_buf = &value; // this should succeed

	hsbt_bulk_op(2, bulk_ops, &error);
	fail_if(error, "Failed to bulk add and delete key to the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key, sizeof(key), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));

	hsbt_remove_entry(sbt128b_fd, &key2, sizeof(key2), &error);

	fail_if(error, "Failed to remove from the HSBT: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_sbt_entry_to_hsbt_entry)
{
	struct isi_error *error = NULL;

	/*
	 * Add an entry using the hashing interface.
	 */
	const char *key = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ)!@#$%^&*("
	    "abcdefghijklmnopqrstuvwxyz";
	const char *value = "3.1415926535897932384626433832795028841971693993";
	btree_key_t hashed_key;
	hsbt_add_entry(sbt128b_fd, key, strlen(key), value, strlen(value),
	    &hashed_key, &error);
	fail_unless(error == NULL,
	    "failed to add entry in preparation for test");

	/*
	 * Read the entry using the non-hashed interface.
	 */
	btree_key_t start_key = {{ 0, 0 }}, next_key;
	char buf[SBT_MAX_ENTRY_SIZE_128];
	size_t num_entries_out = 0;;
	int result = ifs_sbt_get_entries(sbt128b_fd, &start_key, &next_key,
	    sizeof buf, buf, 1, &num_entries_out);
	fail_unless(result == 0 && num_entries_out == 1,
	    "failed to get entry "
	    "(num_entries_out: %zu result: %d errno: %d %s)",
	    num_entries_out, result, errno, strerror(errno));

	/*
	 * Convert to an hsbt_entry.
	 */
	const struct sbt_entry *sbt_ent = (const struct sbt_entry *)buf;
	struct hsbt_entry hsbt_ent;
	sbt_entry_to_hsbt_entry(sbt_ent, &hsbt_ent);

	fail_unless(hsbt_ent.valid_, "hsbt entry should be valid");
	fail_unless(hsbt_ent.key_length_ == strlen(key),
	    "key length mismatch (exp: %zu act: %zu)",
	    strlen(key), hsbt_ent.key_length_);
	fail_unless(hsbt_ent.value_length_ == strlen(value),
	    "value length mismatch (exp: %zu act: %zu)",
	    strlen(value), hsbt_ent.value_length_);
	fail_unless(hsbt_ent.key_ != NULL &&
	    memcmp(hsbt_ent.key_, key, strlen(key)) == 0, "key mismatch");
	fail_unless(hsbt_ent.value_ != NULL &&
	    memcmp(hsbt_ent.value_, value, strlen(value)) == 0,
	    "value mismatch");

	hsbt_entry_destroy(&hsbt_ent);
}
