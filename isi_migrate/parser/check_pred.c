#include <check.h>
#include <isi_migrate/migr/isirep.h>

TEST_FIXTURE(set_up);
TEST_FIXTURE(tear_down);

static siq_plan_t *p;

SUITE_DEFINE_FOR_FILE(migr_pred,
    .mem_check = CK_MEM_DISABLE,
    .test_setup = set_up,
    .test_teardown = tear_down);

TEST_FIXTURE(set_up)
{
	p = NULL;
}

TEST_FIXTURE(tear_down)
{
	siq_plan_free(p);
}

TEST(null_empty_pred, .attrs="overnight")
{
	p = siq_parser_parse(NULL, time(NULL));
	fail_unless(p == NULL);

	p = siq_parser_parse("", time(NULL));
	fail_unless(p == NULL);
}

TEST(fname_pred, .attrs="overnight")
{
	p = siq_parser_parse("-name 'foo'", time(NULL));
	fail_unless(p->need_fname && p->need_dname && p->need_type);
        fail_unless(!p->need_size && !p->need_date && !p->need_path);
}

TEST(path_pred, .attrs="overnight")
{
	p = siq_parser_parse("-path '*foo*'", time(NULL));
	fail_unless(p->need_path);
        fail_unless(!p->need_fname && !p->need_size && !p->need_date);
}

TEST(size_pred, .attrs="overnight")
{
	p = siq_parser_parse("-size '1KB'", time(NULL));
	fail_unless(p->need_size);
        fail_unless(!p->need_fname && !p->need_path && !p->need_date);
}
