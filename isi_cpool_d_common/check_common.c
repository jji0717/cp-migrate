#include <sys/stat.h>

#include <check.h>

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>

#include "cpool_d_common.h"

static int g_sbt_fd = -1;
static const char *g_sbt_name = "isi_cpool_d_common.unit_test.sbt";

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_common,
					  .suite_setup = setup_suite,
					  .suite_teardown = teardown_suite,
					  .test_setup = setup_test,
					  .test_teardown = teardown_test,
					  .timeout = 12,
					  .fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	// intentionally empty
}

TEST_FIXTURE(teardown_suite)
{
	// intentionally empty
}

TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;

	delete_sbt(g_sbt_name, &error);
	fail_unless(error == NULL,
				"error deleting SBT (%s) prior to test: %{}",
				g_sbt_name, isi_error_fmt(error));

	g_sbt_fd = open_sbt(g_sbt_name, true, &error);
	fail_unless(error == NULL,
				"error creating/opening SBT (%s) prior to test: %{}",
				g_sbt_name, isi_error_fmt(error));
}

TEST_FIXTURE(teardown_test)
{
	struct isi_error *error = NULL;

	delete_sbt(g_sbt_name, &error);
	fail_unless(error == NULL,
				"error deleting SBT (%s) prior to test: %{}",
				g_sbt_name, isi_error_fmt(error));
}

/*
 * Utility functions
 */
static void add_sbt_entry(int sbt_fd, btree_key_t key, const void *buf,
						  size_t buf_size)
{
	int result;
	struct btree_flags flags = {};

	result = ifs_sbt_add_entry(sbt_fd, &key, buf, buf_size, &flags);
	fail_if(result != 0, "error adding entry in preparation for test: %s",
			strerror(errno));
}

/**
 * Test get_sbt_entry on an empty SBT.
 */
TEST(test_get_sbt_entry_empty)
{
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t iter_key;
	struct get_sbt_entry_ctx *ctx = NULL;

	/*
	 * Initializing the iteration key to non-zero values will test more
	 * than initializing to zero values.
	 */
	iter_key.keys[0] = 10;
	iter_key.keys[1] = 20;

	struct sbt_entry *ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf,
										  &ctx, &error);
	printf("\nsizeof of sbt_entry:%lu\n", sizeof(*ent));
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent != NULL, "no entry should be returned from an empty SBT");
	fail_if(entry_buf != NULL,
			"entry buf should be NULL when no entry is returned");

	get_sbt_entry_ctx_destroy(&ctx);
}

/**
 * Test get_sbt_entry on an SBT with one element whose key is less than where we
 * start looking.
 */
TEST(test_add_get_sbt_entry)
{
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t add_key, iter_key;
	struct get_sbt_entry_ctx *ctx = NULL;
	const char *add_buf = "hello";

	add_key.keys[0] = 25;
	add_key.keys[1] = 26;
	add_sbt_entry(g_sbt_fd, add_key, add_buf, strlen(add_buf));

	iter_key.keys[0] = 1;
	iter_key.keys[1] = 17;

	struct sbt_entry *ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	if (entry_buf)
	{
		printf("%s called entry_buf:%s\n", __func__, ent->buf);
		printf("k1:%ld k2:%ld\n", iter_key.keys[0], iter_key.keys[1]); /////因为sbt中只add了一个entry{25,26},所以指向下一个entry的iter_key为空{-1,-1}
	}
	free(entry_buf);
	get_sbt_entry_ctx_destroy(&ctx);
}
TEST(test_get_sbt_entry_one_element)
{
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t add_key, iter_key;
	struct get_sbt_entry_ctx *ctx = NULL;
	const char *add_buf = "hello";

	add_key.keys[0] = 25;
	add_key.keys[1] = 26;
	add_sbt_entry(g_sbt_fd, add_key, add_buf, strlen(add_buf));

	/*
	 * Initializing the "start" key to a value greater than the element we
	 * added will test more code branches.
	 */
	iter_key.keys[0] = 50;
	iter_key.keys[1] = 51;

	struct sbt_entry *ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf,
										  &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL,
			"an entry should be returned from a non-empty SBT");
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != add_key.keys[0] ||
				ent->key.keys[1] != add_key.keys[1],
			"unexpected key (%d/%d) from returned entry (exp: %d/%d)",
			ent->key.keys[0], ent->key.keys[1], add_key.keys[0],
			add_key.keys[1]);
	fail_if(ent->size_out != strlen(add_buf),
			"returned entry size (%zu) does not match original (%zu)",
			ent->size_out, strlen(add_buf));
	fail_if(strncmp(ent->buf, add_buf, strlen(add_buf)) != 0,
			"entry content mismatch (exp: [%s] act: [%s])",
			add_buf, ent->buf);

	free(entry_buf);
	get_sbt_entry_ctx_destroy(&ctx);
}

/**
 * Test get_entry on an SBT with multiple elements where the first element is
 * removed after it is seen and new elements are added, one less than the first
 * element and one greater.
 */
TEST(test_get_entry_multiple_elements_with_removal_and_close_addition)
{
	/*
	 * Start with two entries, B and D.  Get B, which should point iter_key
	 * to D, then remove B and add A and C.  Get the next entry (should be
	 * D) and the next (should be A) and the next (should be NULL, since
	 * C > B).  ?????
	 */
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t iter_key = {{990, 10000}};
	struct get_sbt_entry_ctx *ctx = NULL;
	struct sbt_entry *ent = NULL;

	/*
	 * Create the keys - note that B[0] == C[0] (this is the "close
	 * addition" part).
	 */
	btree_key_t entry_A_key = {{1087, 114}};
	btree_key_t entry_B_key = {{6400, 8956}};
	btree_key_t entry_C_key = {{6400, 9000}};
	btree_key_t entry_D_key = {{902104, 17}};

	/* Add B and D ... */
	add_sbt_entry(g_sbt_fd, entry_B_key, NULL, 0);
	add_sbt_entry(g_sbt_fd, entry_D_key, NULL, 0);

	/* ... get B ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	printf("\nafter get B ent->key{%ld %ld}\n", ent->key.keys[0], ent->key.keys[1]);
	printf("\nafter get B entry_buf->key{{%ld %ld}}\n", entry_B_key.keys[0], entry_B_key.keys[1]);
	printf("\nafter get B iter_key{{%ld %ld}}\n", iter_key.keys[0], iter_key.keys[1]);
	fail_if(ent->key.keys[0] != entry_B_key.keys[0] ||
				ent->key.keys[1] != entry_B_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_B_key.keys[0], entry_B_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... verify that iter_key is equal to D ... */
	fail_if(iter_key.keys[0] != entry_D_key.keys[0] ||
				iter_key.keys[1] != entry_D_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_D_key.keys[0], entry_D_key.keys[1],
			iter_key.keys[0], iter_key.keys[1]);

	// printf("\nafter get B, iter_key {%ld %ld}\n", iter_key.keys[0], iter_key.keys[1]);
	// ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	// printf("\nent->key{%ld %ld}\n", ent->key.keys[0], ent->key.keys[1]);
	// return;
	/* ... remove B and add A and C ... */
	fail_if(ifs_sbt_cond_remove_entry(g_sbt_fd, &entry_B_key, BT_CM_NONE,
									  NULL, 0, NULL) != 0,
			"error removing element: %s", strerror(errno));
	add_sbt_entry(g_sbt_fd, entry_A_key, NULL, 0);
	add_sbt_entry(g_sbt_fd, entry_C_key, NULL, 0);
	printf("\nafter remove B and add A and C, iter_key {%ld %ld}\n", iter_key.keys[0], iter_key.keys[1]);

	/* ... get D ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	printf("\nafter get D, ent->key{{%ld %ld}}\n", ent->key.keys[0], ent->key.keys[1]);
	printf("\nafter get D, entry_D_key{{%ld %ld}}\n", entry_D_key.keys[0], entry_D_key.keys[1]);
	printf("\nafter get D, iter_key {%ld %ld}\n", iter_key.keys[0], iter_key.keys[1]);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != entry_D_key.keys[0] ||
				ent->key.keys[1] != entry_D_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_D_key.keys[0], entry_D_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... get A ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	printf("\nafter get A, ent->key{{%ld %ld}}\n", ent->key.keys[0], ent->key.keys[1]);
	printf("\nafter get A entry_A_Key{{%ld %ld}}\n", entry_A_key.keys[0], entry_A_key.keys[1]);
	printf("\nafter get A, iter_key {%ld %ld}\n", iter_key.keys[0], iter_key.keys[1]);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != entry_A_key.keys[0] ||
				ent->key.keys[1] != entry_A_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_A_key.keys[0], entry_A_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... and finally get nothing instead of C. */
	// iter_key.keys[0] = 6399;
	// iter_key.keys[1] = 9000;
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent != NULL);
	fail_if(entry_buf != NULL);
	printf("\nafter finally, iter_key {%ld %ld}\n", iter_key.keys[0], iter_key.keys[1]);
	get_sbt_entry_ctx_destroy(&ctx);
}

/**
 * Same as test_get_entry_multiple_elements_with_removal_and_close_addition,
 * but we add entry C further away from entry B to test a different code path.
 */
TEST(test_get_entry_multiple_elements_with_removal_and_far_addition)
{
	/*
	 * Start with two entries, B and D.  Get B, which should point iter_key
	 * to D, then remove B and add A and C.  Get the next entry (should be
	 * D) and the next (should be A) and the next (should be NULL, since
	 * C > B).
	 */
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t iter_key;
	struct get_sbt_entry_ctx *ctx = NULL;
	struct sbt_entry *ent = NULL;

	/*
	 * Create the keys - note that B[0] < C[0] (this is the "far addition"
	 * part).
	 */
	btree_key_t entry_A_key = {{1087, 114}};
	btree_key_t entry_B_key = {{6400, 8956}};
	btree_key_t entry_C_key = {{6401, 9000}};
	btree_key_t entry_D_key = {{902104, 17}};

	/* Add B and D ... */
	add_sbt_entry(g_sbt_fd, entry_B_key, NULL, 0);
	add_sbt_entry(g_sbt_fd, entry_D_key, NULL, 0);

	/* ... get B ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != entry_B_key.keys[0] ||
				ent->key.keys[1] != entry_B_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_B_key.keys[0], entry_B_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... verify that iter_key is equal to D ... */
	fail_if(iter_key.keys[0] != entry_D_key.keys[0] ||
				iter_key.keys[1] != entry_D_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_D_key.keys[0], entry_D_key.keys[1],
			iter_key.keys[0], iter_key.keys[1]);

	/* ... remove B and add A and C ... */
	fail_if(ifs_sbt_cond_remove_entry(g_sbt_fd, &entry_B_key, BT_CM_NONE,
									  NULL, 0, NULL) != 0,
			"error removing element: %s", strerror(errno));
	add_sbt_entry(g_sbt_fd, entry_A_key, NULL, 0);
	add_sbt_entry(g_sbt_fd, entry_C_key, NULL, 0);

	/* ... get D ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != entry_D_key.keys[0] ||
				ent->key.keys[1] != entry_D_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_D_key.keys[0], entry_D_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... get A ... */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL);
	fail_if(entry_buf == NULL);
	fail_if(ent->key.keys[0] != entry_A_key.keys[0] ||
				ent->key.keys[1] != entry_A_key.keys[1],
			"key mismatch (exp: %zu/%zu act: %zu/%zu)",
			entry_A_key.keys[0], entry_A_key.keys[1],
			ent->key.keys[0], ent->key.keys[1]);
	free(entry_buf);
	entry_buf = NULL;

	/* ... and finally get nothing instead of C. */
	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent != NULL);
	fail_if(entry_buf != NULL);

	get_sbt_entry_ctx_destroy(&ctx);
}

/**
 * Test get_entry on an SBT with multiple elements whose keys are less than
 * where we start looking. For X entries, attempt to get (X+1) entries and see
 * that we return NULL instead of the first one again; this may seem
 * counterintuitive but it prevents infinite loops in callers of get_entry.
 */
TEST(test_get_entry_multiple_elements_repeat_get)
{
	struct isi_error *error = NULL;
	char *entry_buf = NULL;
	btree_key_t add_key1, add_key2, iter_key;
	struct get_sbt_entry_ctx *ctx = NULL;
	const char *add_buf1 = "hello", *add_buf2 = "goodbye";
	struct sbt_entry *ent = NULL;

	add_key1.keys[0] = 25;
	add_key1.keys[1] = 26;
	add_sbt_entry(g_sbt_fd, add_key1, add_buf1, strlen(add_buf1) + 1);
	add_key2.keys[0] = 30;
	add_key2.keys[1] = 31;
	add_sbt_entry(g_sbt_fd, add_key2, add_buf2, strlen(add_buf2) + 1);

	/*
	 * Initializing the iteration key to a value greater than the elements
	 * we added will test more code branches.
	 */
	iter_key.keys[0] = 50;
	iter_key.keys[1] = 51;

	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL,
			"an entry should be returned from a non-empty SBT");
	fail_if(ent->key.keys[0] != add_key1.keys[0] ||
				ent->key.keys[1] != add_key1.keys[1],
			"unexpected key (%d/%d) from returned entry (exp: %d/%d)",
			ent->key.keys[0], ent->key.keys[1], add_key1.keys[0],
			add_key1.keys[1]);
	fail_if(ent->size_out != strlen(add_buf1) + 1,
			"returned entry size (%zu) does not match original (%zu)",
			ent->size_out, strlen(add_buf1) + 1);
	fail_if(strncmp(ent->buf, add_buf1, ent->size_out) != 0,
			"entry content mismatch (exp: [%s] act: [%s])",
			add_buf1, ent->buf);

	free(entry_buf);
	entry_buf = NULL;

	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent == NULL,
			"an entry should be returned from a non-empty SBT");
	fail_if(ent->key.keys[0] != add_key2.keys[0] ||
				ent->key.keys[1] != add_key2.keys[1],
			"unexpected key (%d/%d) from returned entry (exp: %d/%d)",
			ent->key.keys[0], ent->key.keys[1], add_key2.keys[0],
			add_key2.keys[1]);
	fail_if(ent->size_out != strlen(add_buf2) + 1,
			"returned entry size (%zu) does not match original (%zu)",
			ent->size_out, strlen(add_buf2) + 1);
	fail_if(strncmp(ent->buf, add_buf2, ent->size_out) != 0,
			"entry content mismatch (exp: [%s] act: [%s])",
			add_buf2, ent->buf);

	free(entry_buf);
	entry_buf = NULL;

	ent = get_sbt_entry(g_sbt_fd, &iter_key, &entry_buf, &ctx, &error);
	fail_if(error != NULL, "unexpected error: %#{}",
			isi_error_fmt(error));
	fail_if(ent != NULL, "unexpected entry returned");
	fail_if(entry_buf != NULL);

	get_sbt_entry_ctx_destroy(&ctx);
}

// Checks that each character is legal for its place in a utf8 char
static void
check_utf8_char(char *str, char *label)
{
	size_t len = strlen(str);

	fail_unless(IS_START_OF_UTF8_CHAR(str[0]), "Bad leading char: %s",
				label);
	for (unsigned int i = 1; i < len; i++)
		fail_if(IS_START_OF_UTF8_CHAR(str[i]),
				"Leading char in non leading position: %s", label);
}

TEST(check_abbreviate_path_overflow)
{
	const char *in = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	char out[100];
	memset(out, '#', sizeof(out));
	abbreviate_path(in, out, 10);
	fail_unless(out[11] == '#');
	fail_unless(out[10] == '#'); // otherwise we overwrote our buffer.

	memset(out, '#', sizeof(out));
	abbreviate_path(in, out, 40);
	fail_unless(out[41] == '#');
	fail_unless(out[40] == '#');
}

TEST(check_abbreviate_path)
{
	check_utf8_char("a", "English 'a'");
	check_utf8_char("\xc3\xb8", "LATIN SMALL LETTER O WITH STROKE");
	check_utf8_char("\xd0\xa9", "CYRILLIC CAPITAL LETTER SHCHA");
	check_utf8_char("\xd7\x90", "HEBREW LETTER ALEF");
	check_utf8_char("\xe1\xa5\x91", "TAI LE LETTER XA");
	check_utf8_char("\xe2\x86\xbb", "CLOCKWISE OPEN CIRCLE ARROW");

	const char original_1[] = "/abc/def/ghi";
	const size_t out_size_1 = 6;
	char modified_1[out_size_1];

	abbreviate_path(original_1, modified_1, out_size_1);
	fail_unless(strcmp(modified_1, "...hi") == 0,
				"Did not abbreviate as expected: %s", modified_1);

	const char original_2[] = "/abc/def/ghi/jkl/mno/pqr/tuv/wxy/z12";
	const size_t out_size_2 = 31;
	char modified_2[out_size_2];

	abbreviate_path(original_2, modified_2, out_size_2);
	fail_unless(
		strcmp(modified_2, "/abc/def/ghi/jk.../tuv/wxy/z12") == 0,
		"Did not abbreviate as expected: %s", modified_2);

	const char original_3[] = "/abc/def";
	const size_t out_size_3 = 31;
	char modified_3[out_size_3];

	abbreviate_path(original_3, modified_3, out_size_3);
	fail_unless(
		strcmp(modified_3, "/abc/def") == 0,
		"Should not have abbreviated this path: %s", modified_3);

	const char original_4[] = "\xd0\xaf\xd0\xaf\xd0\xaf\xd0\xaf\xd0\xaf\xd0\xaf\xd0\xaf";
	const size_t out_size_4 = 8;
	char modified_4[out_size_4];

	abbreviate_path(original_4, modified_4, out_size_4);
	fail_unless(
		strncmp(modified_4, "...\xd0\xaf", 5) == 0,
		"Did not break on leading utf8 character: %s", modified_4);
}

/*
 * Utility function for cleaning up job files SBTs (keeps the job-file-sbt
 * logic separated from the testpoints)
 */
static void
delete_job_files_sbt(djob_id_t job_id, bool fail_on_error)
{
	struct isi_error *error = NULL;

	char *sbt_name = get_djob_file_list_sbt_name(job_id);

	ASSERT(sbt_name);

	delete_sbt(sbt_name, &error);

	free(sbt_name);

	if (fail_on_error)
		fail_if(error != NULL,
				"Failed to delete job files SBT for job %d: %{}",
				job_id, isi_error_fmt(error));
	else if (error != NULL)
		isi_error_free(error);
}

TEST(check_open_job_files_sbt)
{
	struct isi_error *error = NULL;
	djob_id_t job_id = 123456;
	int fd = -1;

	/*
	 * Make sure no previous running test leaked an SBT
	 * Don't care if this succeeds or not
	 */
	delete_job_files_sbt(job_id, false);

	/* Open uncreated - should fail */
	fd = open_djob_file_list_sbt(job_id, false, &error);
	fail_if(error == NULL, "Expected error trying to open missing SBT");
	fail_if(fd > 0, "Valid fd not expected: %d", fd);

	isi_error_free(error);
	error = NULL;

	/* Create SBT - should succeed */
	fd = open_djob_file_list_sbt(job_id, true, &error);
	fail_if(error != NULL, "Unexpected error creating SBT: %{}", error);
	fail_if(fd <= 0, "Invalid fd returned: %d", fd);

	close_sbt(fd);
	fd = -1;

	/* Open recently created SBT without create flag - should succeed */
	fd = open_djob_file_list_sbt(job_id, false, &error);
	fail_if(error != NULL, "Unexpected error opening SBT: %{}", error);
	fail_if(fd <= 0, "Invalid fd returned: %d", fd);

	/* Cleanup */
	close_sbt(fd);
	delete_job_files_sbt(job_id, false);
}
