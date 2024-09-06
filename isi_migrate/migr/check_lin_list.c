#include <check.h>
#include <isi_sbtree/sbtree.h>

#include "isirep.h"
#include "isi_migrate/config/siq_btree_public.h"
#include "work_item.h"
#include "lin_list.h"
#include "check_helper.h"

#define fail_if_isi_error(err) \
do {\
        fail_unless(!err, "%s", error ?isi_error_get_message(error): "");\
} while(0)

#define rand_bool rand() % 2;

static int res;
static bool found;
static struct lin_list_ctx *ctx = NULL;
static struct lin_list_entry ent_a;
static struct lin_list_entry ent_b;
static struct lin_list_info_entry llie;
static char NAME[32];
static char POL[32];

static btree_key_t *
lin_to_key(uint64_t lin, btree_key_t *key)
{
	ASSERT(key);
	key->keys[0] = lin;
	key->keys[1] = 0;
	return key;
}

static void
reset_entry(struct lin_list_entry *ent)
{
	ent->is_dir = false;
	ent->commit = false;
}

static void
randomize_entry(struct lin_list_entry *ent)
{
	ent->is_dir = rand_bool;
	ent->commit = rand_bool;
}

static int
compare_entries(struct lin_list_entry *a, struct lin_list_entry *b)
{
	return memcmp(a, b, sizeof(*a));
}

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_lin_list,
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
	struct isi_error *error = NULL;

	siq_btree_remove_source(POL, &error);
	fail_if_isi_error(error);

	siq_btree_remove_target(POL, &error);
	fail_if_isi_error(error);
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;

	if (ctx != NULL)
		close_lin_list(ctx);

	remove_lin_list(POL, NAME, true, &error);
	fail_if_isi_error(error);

	create_lin_list(POL, NAME, false, &error);
	fail_if_isi_error(error);

	open_lin_list(POL, NAME, &ctx, &error);
	fail_if_isi_error(error);

	reset_entry(&ent_a);
	reset_entry(&ent_b);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	close_lin_list(ctx);
	ctx = NULL;

	remove_lin_list(POL, NAME, false, &error);
	fail_if_isi_error(error);
}

TEST(create_open_close_remove, .attrs="overnight")
{
	// just do set_up and tear_down
}

TEST(open_non_existing_lin_list, .attrs="overnight")
{
	struct lin_list_ctx *ctx = NULL;
	struct isi_error *error = NULL;

	open_lin_list(POL, "bar", &ctx, &error);
	fail_if(error == NULL || ctx != NULL);
	isi_error_free(error);
}

TEST(get_invalid_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	found = get_lin_list_entry(999, &ent_a, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(add_get_new_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_lin_list_entry(1000, ent_a, ctx, &error);
	fail_if_isi_error(error);
	found = get_lin_list_entry(1000, &ent_b, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	res = compare_entries(&ent_a, &ent_b);
	fail_unless(res == 0);
}

TEST(get_removed_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_lin_list_entry(1000, ent_a, ctx, &error);
	fail_if_isi_error(error);
	remove_lin_list_entry(1000, ctx, &error);
	fail_if_isi_error(error);
	found = get_lin_list_entry(1000, &ent_b, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(change_existing_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	set_lin_list_entry(1000, ent_a, ctx, &error);
	fail_if_isi_error(error);
	randomize_entry(&ent_a);
	set_lin_list_entry(1000, ent_a, ctx, &error);
	fail_if_isi_error(error);
	found = get_lin_list_entry(1000, &ent_b, ctx, &error);
	fail_if_isi_error(error);
	fail_if(!found);
	res = compare_entries(&ent_a, &ent_b);
	fail_unless(res == 0);
}

TEST(iterate_empty_sbt, .attrs="overnight")
{
	struct lin_list_iter *iter = NULL;
	struct isi_error *error = NULL;

	u_int64_t lin_out = 0;
	struct lin_list_entry ent_out;
	bool got_one;

	iter = new_lin_list_iter(ctx);
	fail_if(iter == NULL);

	got_one = lin_list_iter_next(iter, &lin_out, &ent_out, &error);
	fail_if_isi_error(error);
	fail_if(got_one);
}

TEST(set_get_lin_list_info, .attrs="overnight")
{
	struct isi_error *error = NULL;

	llie.type = COMMIT;
	set_lin_list_info(&llie, ctx, false, &error);
	fail_if_isi_error(error);

	llie.type = MAX_LIN_LIST;
	get_lin_list_info(&llie, ctx, &error);
	fail_unless(llie.type == COMMIT);

	llie.type = MAX_LIN_LIST;
	set_lin_list_info(&llie, ctx, true, &error);
	fail_unless(error != NULL);
}

TEST(doubleset_get_list_list_info, .attrs="overnight")
{
	struct isi_error *error = NULL;

	llie.type = COMMIT;
	set_lin_list_info(&llie, ctx, false, &error);
	fail_if_isi_error(error);
	llie.type = RESYNC;
	set_lin_list_info(&llie, ctx, true, &error);
	fail_if_isi_error(error);

	llie.type = MAX_LIN_LIST;
	get_lin_list_info(&llie, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(llie.type == RESYNC);
}

#define ITER_TEST_SIZE 120
TEST(iterate_populated_sbt, .attrs="overnight")
{
	int i = 0;
	u_int64_t lin_out = 0;
	struct lin_list_iter *iter = NULL;
	struct lin_list_entry ent_out;
	bool got_one;
	struct lin_list_entry ent_in[ITER_TEST_SIZE];
	struct isi_error *error = NULL;

	/* prevent padding bytes from causing false compare mismatches */
	memset(ent_in, 0, sizeof(*ent_in) * ITER_TEST_SIZE);

	/* create a info entry */
	llie.type = COMMIT;
	set_lin_list_info(&llie, ctx, false, &error);
	fail_if_isi_error(error);

	for (i = 0; i < ITER_TEST_SIZE; i++) {
		randomize_entry(&ent_in[i]);
		set_lin_list_entry(i + 2, ent_in[i], ctx, &error);
		fail_if_isi_error(error);
	}

	iter = new_lin_list_iter(ctx);
	fail_if(iter == NULL);

	i = 0;
	while (true) {
		got_one = lin_list_iter_next(iter, &lin_out, &ent_out, &error);
		fail_if_isi_error(error);
		if (!got_one)
			break;
		i++;
		fail_unless(compare_entries(&ent_out, &ent_in[lin_out - 2])
		    == 0);
	}

	fail_unless(i == ITER_TEST_SIZE, "Entry count mismatch %d %d", i,
	    ITER_TEST_SIZE);
}

TEST(add_log_then_flush, .attrs="overnight")
{
	int i;
	u_int64_t lin_out;
	bool got_one;
	struct lin_list_iter *iter = NULL;
	struct lin_list_entry ent_out, tmp_ent = {};
	struct lin_list_entry ent_in[ITER_TEST_SIZE];
	struct lin_list_log logp = {};
	struct isi_error *error = NULL;

	/* prevent padding bytes from causing false compare mismatches */
	memset(ent_in, 0, sizeof(*ent_in) * ITER_TEST_SIZE);

	/* create info entry */
	llie.type = RESYNC;
	set_lin_list_info(&llie, ctx, false, &error);
	fail_if_isi_error(error);

	for (i = 0; i < ITER_TEST_SIZE; i++) {
		randomize_entry(&ent_in[i]);
		lin_list_log_add_entry(ctx, &logp, i + 2, true, &ent_in[i],
		    NULL, &error);
		fail_if_isi_error(error);
	}

	/* flush the log to disk */
	flush_lin_list_log(&logp, ctx, &error);
	fail_if_isi_error(error);

	iter = new_lin_list_iter(ctx);
	fail_if(iter == NULL);

	for (i = 0; i < 20; i++) {
		randomize_entry(&tmp_ent);
		lin_list_log_add_entry(ctx, &logp, i + 2, false, &tmp_ent,
		    &ent_in[i], &error);
		fail_if_isi_error(error);
		ent_in[i] = tmp_ent;
	}

	/* flush the log to disk */
	flush_lin_list_log(&logp, ctx, &error);
	fail_if_isi_error(error);

	i = 0;
	while (true) {
		got_one = lin_list_iter_next(iter, &lin_out, &ent_out, &error);
		fail_if_isi_error(error);
		if (!got_one)
			break;
		i++;
		fail_unless(compare_entries(&ent_out, &ent_in[lin_out - 2])
		    == 0, "%d\n", i);
	}

	fail_unless(i == ITER_TEST_SIZE, "Entry count mismatch %d %d", i,
	    ITER_TEST_SIZE);
	close_lin_list_iter(iter);
}
