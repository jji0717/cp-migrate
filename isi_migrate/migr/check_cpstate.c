#include <check.h>
#include <isi_util/isi_error.h>
#include <isi_sbtree/sbtree.h>
#include <ifs/btree/btree.h>

#include "cpools_sync_state.h"
#include "check_helper.h"

#define fail_if_ie(stmt) \
do { \
	stmt; \
	fail_unless(!error, "%s", error ? isi_error_get_message(error) : ""); \
} while (0)

#define rand_bool rand() % 2;

static struct cpss_ctx *ctx = NULL;
static struct cpss_entry a;
static struct cpss_entry b;
static char NAME[32];
static char POL[32];

static void
reset_entry(struct cpss_entry *cpse)
{
	memset(cpse, 0, sizeof(struct cpss_entry));
	cpse->sync_type = BST_STUB_SYNC;
}

static void
randomize_entry(struct cpss_entry *cpse)
{
	cpse->sync_type = (rand() % 2) + 1;
}

static int
compare_entries(struct cpss_entry *a, struct cpss_entry *b)
{
	return memcmp(a, b, sizeof(struct cpss_entry));
}

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_cpss,
    .mem_check = CK_MEM_DISABLE,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

TEST_FIXTURE(suite_setup)
{
	static int sub = 0;
	snprintf(POL, 32, "nn%d.%d", getpid(), sub++);
	snprintf(NAME, 32, "nn%d.%d", getpid(), sub++);
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;
	char pat[255];

	get_repeated_cpss_pattern(pat, POL, false);
	fail_if_ie(prune_old_cpss(POL, pat, NULL, 0, &error));

	fail_if_ie(create_cpss(POL, NAME, false, &error));

	fail_if_ie(open_cpss(POL, NAME, &ctx, &error));
	fail_if(ctx == NULL);

	reset_entry(&a);
	reset_entry(&b);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	close_cpss(ctx);

	fail_if_ie(remove_cpss(POL, NAME, false, &error));
}

TEST(open_non_existing_cpss, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct cpss_ctx *ctx = NULL;

	open_cpss(POL, "bar", &ctx, &error);

	fail_if(error == NULL);

	fail_if(ctx != NULL);
	isi_error_free(error);
}

TEST(get_invalid_entry, .attrs="overnight")
{
	bool found;
	struct isi_error *error = NULL;
	fail_if_ie(found = get_cpss_entry(999, &a, ctx, &error));
	fail_if(found);
}

TEST(add_get_new_entry, .attrs="overnight")
{
	int res;
	bool found;
	struct isi_error *error = NULL;

	fail_if_ie(set_cpss_entry(1000, &a, ctx, &error));
	fail_if_ie(found = get_cpss_entry(1000, &b, ctx, &error));
	fail_unless(found);
	res = compare_entries(&a, &b);
	fail_unless(res == 0);
}

TEST(get_removed_entry, .attrs="overnight")
{
	bool found;
	struct isi_error *error = NULL;

	fail_if_ie(set_cpss_entry(1000, &a, ctx, &error));
	fail_if_ie(remove_cpss_entry(1000, ctx, &error));
	fail_if_ie(found = get_cpss_entry(1000, &b, ctx, &error));
	fail_if(found);
}

TEST(remove_nonexistent_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;
	bool exists;

	fail_if_ie(exists = remove_cpss_entry(1000, ctx, &error));
	fail_unless(exists == false);
}

TEST(change_existing_entry, .attrs="overnight")
{
	bool found;
	struct isi_error *error = NULL;

	fail_if_ie(set_cpss_entry(1000, &a, ctx, &error));
	randomize_entry(&a);
	fail_if_ie(set_cpss_entry(1000, &a, ctx, &error));
	fail_if_ie(found = get_cpss_entry(1000, &b, ctx, &error));
	fail_if(!found);
	fail_unless(compare_entries(&a, &b) == 0);
}

TEST(iterate_empty_sbt, .attrs="overnight")
{
	struct cpss_iter *iter = NULL;
	u_int64_t lin_out = 0;
	struct cpss_entry out;
	bool got_one;
	struct isi_error *error = NULL;

	iter = new_cpss_iter(ctx);
	fail_if(iter == NULL);

	fail_if_ie(got_one = cpss_iter_next(iter, &lin_out, &out, &error));
	close_cpss_iter(iter);

	// Passing in NULL should have no effect.
	close_cpss_iter(NULL);
}

#define ITER_TEST_SIZE 100
TEST(iterate_populated_sbt, .attrs="overnight")
{
	int i = 0;
	u_int64_t lin_out = 0;
	struct cpss_iter *iter = NULL;
	bool got_one;
	struct cpss_entry in[ITER_TEST_SIZE];
	struct cpss_entry out;
	struct isi_error *error = NULL;

	/* prevent padding bytes from causing false compare mismatches */
	memset(in, 0, sizeof(struct cpss_entry) * ITER_TEST_SIZE);

	for (i = 0; i < ITER_TEST_SIZE; i++) {
		randomize_entry(&in[i]);
		fail_if_ie(set_cpss_entry(i + 2, &in[i], ctx, &error));
	}

	iter = new_cpss_iter(ctx);
	fail_if(iter == NULL);

	i = 0;
	while (true) {
		fail_if_ie(got_one = cpss_iter_next(iter, &lin_out, &out,
		    &error));
		if (!got_one)
			break;
		i++;
		fail_unless(compare_entries(&out, &in[lin_out - 2]) == 0);
	}

	fail_unless(i == ITER_TEST_SIZE, "Entry count mismatch %d %d", i,
	    ITER_TEST_SIZE);
}

TEST(prune_keep_single, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptid;
	struct isi_error *error = NULL;

	snprintf(name1, sizeof(name1), "%s_snap_%u", POL, 23);
	fail_if_ie(create_cpss(POL, name1, false, &error));
	snprintf(name2, sizeof(name2), "%s_snap_%u", POL, 29);
	fail_if_ie(create_cpss(POL, name2, false, &error));
	snprintf(name3, sizeof(name3), "%s_snap_%u", POL, 2000);
	fail_if_ie(create_cpss(POL, name3, false, &error));
	snprintf(name4, sizeof(name4), "%s_snap_%u", POL, 1900);
	fail_if_ie(create_cpss(POL, name4, false, &error));

	fail_if_ie(fail_unless(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name4, &error)));

	keptid = 29;
	get_repeated_cpss_pattern(pat, POL, false);
	fail_if_ie(prune_old_cpss(POL, pat, &keptid, 1, &error));
	fail_if_ie(fail_if(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name4, &error)));
}

TEST(prune_keep_none, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptid;
	struct isi_error *error = NULL;

	snprintf(name1, sizeof(name1), "%s_snap_%u", POL, 23);
	fail_if_ie(create_cpss(POL, name1, false, &error));
	snprintf(name2, sizeof(name2), "%s_snap_%u", POL, 29);
	fail_if_ie(create_cpss(POL, name2, false, &error));
	snprintf(name3, sizeof(name3), "%s_snap_%u", POL, 2000);
	fail_if_ie(create_cpss(POL, name3, false, &error));
	snprintf(name4, sizeof(name4), "%s_snap_%u", POL, 1900);
	fail_if_ie(create_cpss(POL, name4, false, &error));

	fail_if_ie(fail_unless(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name4, &error)));

	keptid = 29;
	get_repeated_cpss_pattern(pat, POL, false);
	fail_if_ie(prune_old_cpss(POL, pat, &keptid, 0, &error));
	fail_if_ie(fail_if(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name4, &error)));
}

TEST(prune_keep_base, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char nameb[255];
	char pat[255];

	struct isi_error *error = NULL;

	snprintf(nameb, sizeof(nameb), "%s_snap_rep_base", POL);
	fail_if_ie(create_cpss(POL, nameb, false, &error));
	snprintf(name1, sizeof(name1), "%s_snap_%u", POL, 23);
	fail_if_ie(create_cpss(POL, name1, false, &error));
	snprintf(name2, sizeof(name2), "%s_snap_%u", POL, 29);
	fail_if_ie(create_cpss(POL, name2, false, &error));
	snprintf(name3, sizeof(name3), "%s_snap_%u", POL, 2000);
	fail_if_ie(create_cpss(POL, name3, false, &error));
	snprintf(name4, sizeof(name4), "%s_snap_%u", POL, 1900);
	fail_if_ie(create_cpss(POL, name4, false, &error));

	fail_if_ie(fail_unless(cpss_exists(POL, nameb, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name4, &error)));

	get_repeated_cpss_pattern(pat, POL, false);
	fail_if_ie(prune_old_cpss(POL, pat, NULL, 0, &error));
	fail_if_ie(fail_unless(cpss_exists(POL, nameb, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name4, &error)));
}

TEST(prune_keep_two, .attrs="overnight")
{
	char name1[255];
	char name2[255];
	char name3[255];
	char name4[255];
	char pat[255];
	ifs_snapid_t keptids[2];
	struct isi_error *error = NULL;

	snprintf(name1, sizeof(name1), "%s_snap_%u", POL, 23);
	fail_if_ie(create_cpss(POL, name1, false, &error));
	snprintf(name2, sizeof(name2), "%s_snap_%u", POL, 29);
	fail_if_ie(create_cpss(POL, name2, false, &error));
	snprintf(name3, sizeof(name3), "%s_snap_%u", POL, 2000);
	fail_if_ie(create_cpss(POL, name3, false, &error));
	snprintf(name4, sizeof(name4), "%s_snap_%u", POL, 1900);
	fail_if_ie(create_cpss(POL, name4, false, &error));

	fail_if_ie(fail_unless(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name4, &error)));

	keptids[0] = 29;
	keptids[1] = 2000;
	get_repeated_cpss_pattern(pat, POL, false);
	fail_if_ie(prune_old_cpss(POL, pat, keptids, 2, &error));
	fail_if_ie(fail_if(cpss_exists(POL, name1, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name2, &error)));
	fail_if_ie(fail_unless(cpss_exists(POL, name3, &error)));
	fail_if_ie(fail_if(cpss_exists(POL, name4, &error)));
}

TEST(get_names)
{
	char buf[255];
	char cmp[255];

	get_base_cpss_name(buf, POL, false);
	snprintf(cmp, sizeof(cmp), "%s_cpools_state_base", POL);
	fail_unless(!strcmp(buf, cmp));

	get_base_cpss_name(buf, "", false);
	fail_unless(!strcmp(buf, "_cpools_state_base"));

	get_base_cpss_name(buf, POL, true);
	snprintf(cmp, sizeof(cmp), "%s_cpools_state_base_restore", POL);
	fail_unless(!strcmp(buf, cmp));

	get_base_cpss_name(buf, "", true);
	fail_unless(!strcmp(buf, "_cpools_state_base_restore"));

	get_mirrored_cpss_name(buf, POL);
	snprintf(cmp, sizeof(cmp), "%s_cpools_state_base_mirrored", POL);
	fail_unless(!strcmp(buf, cmp));

	get_mirrored_cpss_name(buf, "");
	fail_unless(!strcmp(buf, "_cpools_state_base_mirrored"));

	get_repeated_cpss_name(buf, POL, 123, false);
	snprintf(cmp, sizeof(cmp), "%s_snap_%d", POL, 123);
	fail_unless(!strcmp(buf, cmp));

	get_repeated_cpss_name(buf, POL, 123, true);
	snprintf(cmp, sizeof(cmp), "%s_snap_restore_%d", POL, 123);
	fail_unless(!strcmp(buf, cmp));
}

TEST(get_cpsskey)
{
	uint64_t lin;
	struct isi_error *error = NULL;

	struct cpss_entry ent = {};

	reset_entry(&ent);
	fail_if_ie(set_cpss_entry(1, &ent, ctx, &error));
	fail_if_ie(set_cpss_entry(5, &ent, ctx, &error));
	fail_if_ie(set_cpss_entry(10, &ent, ctx, &error));
	fail_if_ie(set_cpss_entry(1000, &ent, ctx, &error));

	fail_if_ie(lin = get_cpss_max_lin(ctx, &error));
	fail_unless(lin == 1000);

	fail_if_ie(get_cpsskey_at_loc(0, 1, ctx, &lin, &error));
	fail_unless(lin == 1);

	fail_if_ie(get_cpsskey_at_loc(1, 4, ctx, &lin, &error));
	fail_unless(lin == 5);

	fail_if_ie(get_cpsskey_at_loc(1, 2, ctx, &lin, &error));
	fail_unless(lin == 10);

	fail_if_ie(get_cpsskey_at_loc(1, 1, ctx, &lin, &error));
	fail_unless(lin == 1000);
}

TEST(sync_entry)
{
	bool success;
	bool found;

	struct isi_error *error = NULL;
	struct cpss_entry ent1 = {};
	struct cpss_entry ent2 = {};
	struct cpss_entry ent_get = {};


	reset_entry(&ent1);
	reset_entry(&ent2);

	ent2.sync_type = BST_DEEP_COPY;

	fail_if_ie(success = sync_cpss_entry(ctx, 1000, &ent1, &error));
	fail_unless(success);

	fail_if_ie(found = get_cpss_entry(1000, &ent_get, ctx, &error));
	fail_unless(found);
	fail_unless(compare_entries(&ent1, &ent_get) == 0);

	fail_if_ie(success = sync_cpss_entry(ctx, 1000, &ent2, &error));
	fail_unless(success);

	fail_if_ie(found = get_cpss_entry(1000, &ent_get, ctx, &error));
	fail_unless(found);
	fail_unless(compare_entries(&ent2, &ent_get) == 0);
}

TEST(logflush)
{
	bool found = false;

	struct cpss_log log = {};

	struct cpss_entry ent1 = {};
	struct cpss_entry ent2 = {};
	struct cpss_entry ent_get = {};

	uint64_t lin = 0;

	struct isi_error *error = NULL;

	reset_entry(&ent1);
	reset_entry(&ent2);

	ent2.sync_type = BST_DEEP_COPY;

	while (log.index < MAX_BLK_OPS) {
		log.entries[log.index].lin = log.index + 1000;
		log.entries[log.index].is_new = true;
		log.entries[log.index].old_entry = ent1;
		log.entries[log.index].new_entry = ent1;
		log.index++;
	}

	fail_if_ie(flush_cpss_log(&log, ctx, &error));

	for (lin = 1000; lin < 1000 + MAX_BLK_OPS; lin++) {
		fail_if_ie(found = get_cpss_entry(lin, &ent_get, ctx, &error));
		fail_unless(found);
		fail_unless(compare_entries(&ent1, &ent_get) == 0);
	}

	log.index = 0;

	while (log.index < MAX_BLK_OPS) {
		log.entries[log.index].lin = log.index + 1000;
		log.entries[log.index].is_new = false;
		log.entries[log.index].old_entry = ent1;
		log.entries[log.index].new_entry = ent2;
		log.index++;
	}

	fail_if_ie(flush_cpss_log(&log, ctx, &error));

	for (lin = 1000; lin < 1000 + MAX_BLK_OPS; lin++) {
		fail_if_ie(found = get_cpss_entry(lin, &ent_get, ctx, &error));
		fail_unless(found);
		fail_unless(compare_entries(&ent2, &ent_get) == 0);
	}

	log.index = 0;
	while (log.index < 10) {
		log.entries[log.index].lin = log.index + 1000;
		log.entries[log.index].is_new = false;
		log.entries[log.index].old_entry = ent2;
		log.entries[log.index].new_entry = ent1;
		log.index++;
	}

	log.entries[log.index].lin = 1001;
	log.entries[log.index].is_new = false;
	log.entries[log.index].old_entry = ent1;
	log.entries[log.index].new_entry = ent2;
	log.index++;

	fail_if_ie(flush_cpss_log(&log, ctx, &error));

	for (lin = 1000; lin < 1010; lin++) {
		fail_if_ie(found = get_cpss_entry(lin, &ent_get, ctx, &error));
		fail_unless(found);
		fail_unless(compare_entries(lin == 1001 ? &ent2 : &ent1,
		    &ent_get) == 0);
	}
}

TEST(set_entry_cond)
{
	bool success = false;
	bool found = false;

	struct cpss_entry ent1 = {};
	struct cpss_entry ent2 = {};
	struct cpss_entry ent_get = {};

	struct isi_error *error = NULL;

	reset_entry(&ent1);
	reset_entry(&ent2);

	ent2.sync_type = BST_DEEP_COPY;

	fail_if_ie(success = set_cpss_entry_cond(1000, &ent1, ctx, false,
		&ent1, &error));
	fail_if(success);

	fail_if_ie(success = set_cpss_entry_cond(1000, &ent1, ctx, true,
		&ent1, &error));
	fail_unless(success);

	fail_if_ie(found = get_cpss_entry(1000, &ent_get, ctx, &error));
	fail_unless(found);
	fail_unless(compare_entries(&ent1, &ent_get) == 0);

	fail_if_ie(success = set_cpss_entry_cond(1000, &ent2, ctx, false,
		&ent1, &error));
	fail_unless(success);

	fail_if_ie(found = get_cpss_entry(1000, &ent_get, ctx, &error));
	fail_unless(found);
	fail_unless(compare_entries(&ent2, &ent_get) == 0);
}

TEST(copy_entries)
{
	bool found = false;

	struct cpss_entry ents[10] = {{0}};
	struct cpss_entry ent_get = {};
	uint64_t lins[10];

	struct isi_error *error = NULL;

	size_t i;

	for (i = 0; i < 10; i++) {
		randomize_entry(&ents[i]);
		lins[i] = 1000 + i;
	}

	fail_if_ie(copy_cpss_entries(ctx, lins, ents, 10, &error));

	for (i = 0; i < 10; i++) {
		fail_if_ie(found = get_cpss_entry(lins[i], &ent_get, ctx,
		    &error));
		fail_unless(found);
		fail_unless(compare_entries(&ents[i], &ent_get) == 0);
	}
}
