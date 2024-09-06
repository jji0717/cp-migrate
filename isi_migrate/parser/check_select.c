#include <check.h>
#include <isi_migrate/migr/isirep.h>
#include <isi_util/isi_error.h>

TEST_FIXTURE(set_up);
TEST_FIXTURE(tear_down);

siq_plan_t *plan;
struct isi_error *error = NULL;
int result;

SUITE_DEFINE_FOR_FILE(migr_select,
    .mem_check = CK_MEM_DISABLE,
    .test_setup = set_up,
    .test_teardown = tear_down);

TEST_FIXTURE(set_up)
{
        plan = NULL;
}

TEST_FIXTURE(tear_down)
{
        siq_plan_free(plan);
}

/* a dir named .ifsvar should be rejected */
TEST(dir_ifsvar, .attrs="overnight")
{
	struct stat st;
	st.st_ino = ROOT_LIN;
	st.st_snapid = HEAD_SNAPID;
	result = siq_select(NULL, ".ifsvar", ENC_UTF8, &st, time(NULL), NULL,
	    SIQ_FT_DIR, &error);

	fail_unless(result == 0);
}

/* item that is not a dir or file or link is not selected */
TEST(unsupported_file, .attrs="overnight")
{
	result = siq_select(NULL, "foo", ENC_UTF8, NULL, time(NULL), NULL,
	    SIQ_FT_UNKNOWN, &error);

	fail_unless(result == 0);
}

/* test name predicate */
TEST(name_pred, .attrs="overnight")
{
	plan = siq_parser_parse("-name 'foo*'", time(NULL));

	result = siq_select(plan, "foob", ENC_UTF8, NULL, time(NULL), NULL,
	    SIQ_FT_REG, &error);

	fail_unless(result == 1);

	result = siq_select(plan, "barf", ENC_UTF8, NULL, time(NULL), NULL,
	    SIQ_FT_REG, &error);

	fail_unless(result == 0);
}

/* dirs should not be affected by name predicates */
TEST(dir_with_name_pred, .attrs="overnight")
{
	struct stat st;
	st.st_ino = ROOT_LIN;
	st.st_snapid = HEAD_SNAPID;
	plan = siq_parser_parse("-name 'foo*'", time(NULL));

	result = siq_select(plan, "barf", ENC_UTF8, &st, time(NULL), NULL,
	    SIQ_FT_DIR, &error);

	fail_unless(result == 1);
}

/* test path predicate */
TEST(path_pred, .attrs="overnight")
{
	plan = siq_parser_parse("-path '*/foo/*'", time(NULL));

	result = siq_select(plan, NULL, ENC_UTF8, NULL, time(NULL),
	    "/ifs/foo/bar", SIQ_FT_REG, &error);

	fail_unless(result == 1);

	result = siq_select(plan, NULL, ENC_UTF8, NULL, time(NULL),
	    "/ifs/foob/ar", SIQ_FT_REG, &error);

	fail_unless(result == 0);
}
