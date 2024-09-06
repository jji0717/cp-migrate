#include <check.h>

#include "pworker.h"
#include "isi_migrate/migr/worklist.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_cc_worklists,
    .mem_check = CK_MEM_DISABLE,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

#define TESTPOLICY "TEST_WL_PW"
#define TESTWLNAME "TEST_WL"
struct migr_pworker_ctx *pw_ctx = NULL;
struct isi_error *error = NULL;

TEST_FIXTURE(suite_setup)
{
	remove_worklist(TESTPOLICY, TESTWLNAME, true, &error);
	remove_repstate(TESTPOLICY, TESTWLNAME, true, &error);
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	pw_ctx = calloc(1, sizeof(struct migr_pworker_ctx));
	ASSERT(pw_ctx);
	pw_ctx->chg_ctx = calloc(1, sizeof(struct changeset_ctx));
	ASSERT(pw_ctx->chg_ctx);
	pw_ctx->chg_ctx->summ_stf = calloc(1, sizeof(struct summ_stf_ctx));
	ASSERT(pw_ctx->chg_ctx->summ_stf);
	pw_ctx->work = calloc(1, sizeof(struct work_restart_state));
	ASSERT(pw_ctx->work);

	create_repstate(TESTPOLICY, TESTWLNAME, true, &error);
	fail_if(error);
	open_repstate(TESTPOLICY, TESTWLNAME, &pw_ctx->cur_rep_ctx, &error);
	fail_if(error);
	pw_ctx->chg_ctx->rep2 = pw_ctx->cur_rep_ctx;

	create_worklist(TESTPOLICY, TESTWLNAME, true, &error);
	fail_if(error);
	open_worklist(TESTPOLICY, TESTWLNAME, &pw_ctx->chg_ctx->wl, &error);
	fail_if(error);
}

TEST_FIXTURE(test_teardown)
{
	pw_ctx->chg_ctx->rep2 = NULL;
	close_worklist(pw_ctx->chg_ctx->wl);
	close_repstate(pw_ctx->cur_rep_ctx);
	if (pw_ctx->work) {
		free(pw_ctx->work);
		pw_ctx->work = NULL;
	}
	if (pw_ctx->chg_ctx->summ_stf) {
		free(pw_ctx->chg_ctx->summ_stf);
		pw_ctx->chg_ctx->summ_stf = NULL;
	}
	if (pw_ctx->chg_ctx) {
		free(pw_ctx->chg_ctx);
		pw_ctx->chg_ctx = NULL;
	}
	if (pw_ctx) {
		free(pw_ctx);
		pw_ctx = NULL;
	}
	remove_worklist(TESTPOLICY, TESTWLNAME, true, &error);
	remove_repstate(TESTPOLICY, TESTWLNAME, true, &error);
}

TEST(wl_basic, .attrs="overnight")
{
	bool res = false;
	uint64_t wilin = 0;
	struct rep_entry re = {};
	struct wl_entry *wle = NULL;

	//Basic setup
	pw_ctx->wi_lin = 12345;
	pw_ctx->work->wl_cur_level = 0;
	//This lets us bypass check_connection_with_peer
	pw_ctx->coord = SOCK_LOCAL_OPERATION;
	rep_log_add_entry(pw_ctx->cur_rep_ctx, &pw_ctx->rep_log, pw_ctx->wi_lin, &re,
	    NULL, SBT_SYS_OP_ADD, false, &error);
	fail_if(error);
	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Adding to level 1
	wl_log_add_entry(pw_ctx, 2000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 3000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 4000, &error);
	fail_if(error);

	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 2000);

	//Adding to level 2
	wl_log_add_entry(pw_ctx, 5000, &error);
	fail_if(error);
	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Verify the rest of level 1 and then into level 2
	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 3000);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 4000);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 2);
	fail_unless(wilin == 5000);
}

TEST(wl_basic_checkpoint, .attrs="overnight")
{
	bool res = false;
	uint64_t wilin = 0;
	struct rep_entry re = {};
	struct wl_entry *wle = NULL;
	int ret = 0;

	//Basic setup
	pw_ctx->wi_lin = 12345;
	pw_ctx->work->wl_cur_level = 0;
	//This lets us bypass check_connection_with_peer
	pw_ctx->coord = SOCK_LOCAL_OPERATION;
	rep_log_add_entry(pw_ctx->cur_rep_ctx, &pw_ctx->rep_log, pw_ctx->wi_lin, &re,
	    NULL, SBT_SYS_OP_ADD, false, &error);
	fail_if(error);
	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Adding to level 1
	wl_log_add_entry(pw_ctx, 2000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 3000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 4000, &error);
	fail_if(error);

	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Read to the 2nd entry, then checkpoint
	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 2000);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 3000);

	ret = wl_checkpoint(&pw_ctx->wl_log, pw_ctx->chg_ctx->wl_iter, 11111,
	    22222, &error);
	fail_if(error);
	fail_unless(ret == 0);

	//Reform the iterator and verify the checkpoint and last worklist item
	close_wl_iter(pw_ctx->chg_ctx->wl_iter);
	pw_ctx->chg_ctx->wl_iter = NULL;

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 3000);
	fail_if(!wle);
	fail_unless(wle->dircookie1 == 11111);
	fail_unless(wle->dircookie2 == 22222);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 4000);
}

TEST(wl_restore_iter, .attrs="overnight")
{
	bool res = false;
	uint64_t wilin = 0;
	struct rep_entry re = {};
	struct wl_entry *wle = NULL;
	int ret = 0;

	//Basic setup
	pw_ctx->wi_lin = 12345;
	pw_ctx->work->wl_cur_level = 0;
	//This lets us bypass check_connection_with_peer
	pw_ctx->coord = SOCK_LOCAL_OPERATION;
	rep_log_add_entry(pw_ctx->cur_rep_ctx, &pw_ctx->rep_log, pw_ctx->wi_lin, &re,
	    NULL, SBT_SYS_OP_ADD, false, &error);
	fail_if(error);
	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Adding to level 1
	wl_log_add_entry(pw_ctx, 2000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 3000, &error);
	fail_if(error);
	wl_log_add_entry(pw_ctx, 4000, &error);

	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Read to the 2nd entry, then checkpoint
	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 2000);

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 3000);

	ret = wl_checkpoint(&pw_ctx->wl_log, pw_ctx->chg_ctx->wl_iter, 11111,
	     22222, &error);
	fail_if(error);
	fail_unless(ret == 0);

	//Now also write a different dircookie1 and 2 to work item
	pw_ctx->work->lin = wilin;
	pw_ctx->work->chkpt_lin = wilin;
	pw_ctx->work->dircookie1 = 33333;
	pw_ctx->work->dircookie2 = 44444;
	pw_ctx->work->min_lin = 1000;
	pw_ctx->work->max_lin = 10000;

	flush_stf_logs(pw_ctx, &error);
	fail_if(error);

	//Restore from checkpoint
	close_wl_iter(pw_ctx->chg_ctx->wl_iter);
	pw_ctx->chg_ctx->wl_iter = NULL;
	free(wle);
	wle = NULL;
	wilin = 0;

	res = wl_get_entry(pw_ctx, &wilin, &wle, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(pw_ctx->work->wl_cur_level == 1);
	fail_unless(wilin == 3000);
	fail_unless(wle->dircookie1 == 11111);
	fail_unless(wle->dircookie2 == 22222);
}
