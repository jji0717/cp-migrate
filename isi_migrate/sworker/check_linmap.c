#include <check.h>
#include <isi_migrate/migr/isirep.h>
#include "isi_migrate/migr/linmap.h"

int res;
struct map_ctx *ctx = NULL;
static char NAME[32];
struct isi_error *error = NULL;
bool found;

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_linmap,
    .mem_check = CK_MEM_DISABLE,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

TEST_FIXTURE(suite_setup)
{
	snprintf(NAME, 32, "%d", getpid());
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	create_linmap(NAME, NAME, false, &error);
	fail_if(error);

	open_linmap(NAME, NAME, &ctx, &error);
	fail_if(error);
}

TEST_FIXTURE(test_teardown)
{
	close_linmap(ctx);

	remove_linmap(NAME, NAME, true, &error);
	fail_if(error);
}

TEST(create_open_close_remove, .attrs="overnight")
{
	// just do set_up and tear_down
}

TEST(open_non_existing_linmap, .attrs="overnight")
{
	struct map_ctx *ctx_local = NULL;
	struct isi_error *error_local = NULL;
       	open_linmap("bar", "bar", &ctx_local, &error_local);
	fail_if(!error_local || ctx_local != NULL);
	isi_error_free(error_local);
}

TEST(get_invalid_lin, .attrs="overnight")
{
	u_int64_t slin = 999;
	u_int64_t dlin;

	found = get_mapping(slin, &dlin, ctx, &error);
	fail_if(error);
	fail_if(found == true);
}

TEST(add_get_new_mapping, .attrs="overnight")
{
	u_int64_t slin = 1000;
	u_int64_t dlin = 1234;
	u_int64_t dlin_out = 0;

	set_mapping(slin, dlin, ctx, &error);
	fail_if(error);
	found = get_mapping(slin, &dlin_out, ctx, &error);
	fail_if(error);
	fail_unless(dlin_out == dlin);
}

TEST(get_removed_mapping, .attrs="overnight")
{
	u_int64_t slin = 1000;
	u_int64_t dlin = 2000;
	u_int64_t dlin_out = 0;

	set_mapping(slin, dlin, ctx, &error);
	fail_if(error);
	found = remove_mapping(slin, ctx, &error);
	fail_unless(found);
	fail_if(error);
	found = get_mapping(slin, &dlin_out, ctx, &error);
	fail_if(error);
	fail_if(found);
}

TEST(change_existing_mapping, .attrs="overnight")
{
	u_int64_t slin = 1000;
	u_int64_t dlin_a = 1234;
	u_int64_t dlin_b = 9876;
	u_int64_t dlin_out = 0;

	set_mapping(slin, dlin_a, ctx, &error);
	fail_if(error);
	set_mapping(slin, dlin_b, ctx, &error);
	fail_if(error);
	found = get_mapping(1000, &dlin_out, ctx, &error);
	fail_if(error);
	fail_unless(found);
	fail_unless(dlin_out == dlin_b);
}

TEST(iterate_empty_sbt, .attrs="overnight")
{
	struct map_iter *iter = NULL;
	u_int64_t slin_out = 0;
	u_int64_t dlin_out = 0;
	bool got_one;

	iter = new_map_iter(ctx);
	fail_if(iter == NULL);

	got_one = map_iter_next(iter, &slin_out, &dlin_out, &error);
	fail_if(error);
	fail_if(got_one);
}

TEST(iterate_populated_sbt, .attrs="overnight")
{
	int i = 0;
	u_int64_t slin_out;
	u_int64_t dlin_out;
	struct map_iter *iter = NULL;
	bool got_one;

	for (i = 0; i < 20; i++) {
		set_mapping(i, 100 - i, ctx, &error);
		fail_if(error);
	}

	iter = new_map_iter(ctx);
	fail_if(iter == NULL);

	i = 0;
	while (true) {
		got_one = map_iter_next(iter, &slin_out, &dlin_out, &error);
		fail_if(error);
		if (!got_one)
			break;
		fail_unless(slin_out + dlin_out == 100);
		i++;
	}

	fail_unless(i == 20);
}
