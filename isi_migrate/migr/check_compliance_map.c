#include <check.h>
#include <isi_sbtree/sbtree.h>

#include "isirep.h"
#include "isi_migrate/config/siq_btree_public.h"
#include "work_item.h"
#include "compliance_map.h"
#include "check_helper.h"

#define fail_if_isi_error(err) \
do {\
        fail_unless(!err, "%s", error ?isi_error_get_message(error): "");\
} while(0)

#define rand_bool rand() % 2;

static bool found;
static bool success;
static uint64_t new_lin;
static uint64_t old_lin;
static struct compliance_map_ctx *ctx = NULL;
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
reset_entry(struct compliance_map_log_entry *comp)
{
	memset(comp, 0, sizeof(struct compliance_map_log_entry));
}

static void
default_entry(struct compliance_map_log_entry *comp)
{
	comp->old_lin = 1;
	comp->new_lin = 1;
	comp->oper = CMAP_SET;
}

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_compliance_map,
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
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;

	create_compliance_map(POL, NAME, false, &error);
	fail_if_isi_error(error);

	open_compliance_map(POL, NAME, &ctx, &error);
	fail_if_isi_error(error);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	close_compliance_map(ctx);

	remove_compliance_map(POL, NAME, false, &error);
	fail_if_isi_error(error);
}

TEST(create_open_close_remove, .attrs="overnight")
{
	// just do set_up and tear_down
}

TEST(open_non_existing_compliance_map, .attrs="overnight")
{
	struct compliance_map_ctx *ctx = NULL;
	struct isi_error *error = NULL;

	open_compliance_map(POL, "bar", &ctx, &error);
	fail_if(error == NULL || ctx != NULL);
	isi_error_free(error);
}

TEST(get_invalid_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	found = get_compliance_mapping(999, &new_lin, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(add_get_new_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	success = set_compliance_mapping(1000, 1001, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(success);
	found = get_compliance_mapping(1000, &new_lin, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	fail_unless(1001 == new_lin);
}

TEST(get_removed_entry, .attrs="overnight")
{
	struct isi_error *error = NULL;

	success = set_compliance_mapping(1000, 1001, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(success);
	found = remove_compliance_mapping(1000, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	found = get_compliance_mapping(1000, &new_lin, ctx, &error);
	fail_if_isi_error(error);
	fail_if(found);
}

TEST(change_existing_entry, .attrs="overnight")
{
	uint64_t tmp_new_lin;
	struct isi_error *error = NULL;

	success = set_compliance_mapping(1000, 1001, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(success);
	new_lin = rand();
	success = set_compliance_mapping(1000, new_lin, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(success);
	found = get_compliance_mapping(1000, &tmp_new_lin, ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found);
	fail_unless(new_lin == tmp_new_lin);
}

TEST(iterate_empty_sbt, .attrs="overnight")
{
	bool got_one;
	struct compliance_map_iter *iter = NULL;
	struct isi_error *error = NULL;

	iter = new_compliance_map_iter(ctx);
	fail_if(iter == NULL);

	got_one = compliance_map_iter_next(iter, &old_lin, &new_lin, &error);
	fail_if_isi_error(error);
	fail_if(got_one);
}

#define ITER_TEST_SIZE 100
TEST(iterate_populated_sbt, .attrs="overnight")
{
	bool got_one;
	int i = 0;
	uint64_t new_lin_in[ITER_TEST_SIZE];
	struct compliance_map_iter *iter = NULL;
	struct isi_error *error = NULL;

	for (i = 0; i < ITER_TEST_SIZE; i++) {
		new_lin_in[i] = rand();
		success = set_compliance_mapping(i + 2, new_lin_in[i], ctx,
		    &error);
		fail_if_isi_error(error);
		fail_unless(success);
	}

	iter = new_compliance_map_iter(ctx);
	fail_if(iter == NULL);

	i = 0;
	while (true) {
		got_one = compliance_map_iter_next(iter, &old_lin, &new_lin,
		    &error);
		fail_if_isi_error(error);
		if (!got_one)
			break;
		fail_unless(new_lin_in[i] == new_lin, "%lld != %lld",
		    new_lin_in[i], new_lin);
		i++;
	}

	fail_unless(i == ITER_TEST_SIZE, "Entry count mismatch %d %d", i,
	    ITER_TEST_SIZE);
}
