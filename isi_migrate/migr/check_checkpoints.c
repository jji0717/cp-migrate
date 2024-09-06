#include <check.h>

#include "isirep.h"
#include "worklist.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(migr_checkpoints,
    .mem_check = CK_MEM_DISABLE,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

#define TESTPOLICY "TEST_WL_CHECKPOINT"
#define TESTWLNAME "TEST_WL"
struct wl_ctx *wlc = NULL;
struct isi_error *error = NULL;

TEST_FIXTURE(suite_setup)
{
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	create_worklist(TESTPOLICY, TESTWLNAME, true, &error);
	fail_if(error);
	wlc = calloc(1, sizeof(struct wl_ctx));
	open_worklist(TESTPOLICY, TESTWLNAME, &wlc, &error);
	fail_if(error);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	if (wlc) {
		close_worklist(wlc);
	}
	remove_worklist(TESTPOLICY, TESTWLNAME, true, &error);
	isi_error_free(error);
}

TEST(wl_basic, .attrs="overnight")
{
	struct wl_log wlog = {};
	struct wl_iter *witer = NULL;
	btree_key_t wkey = {};
	bool res = false;
	uint64_t lin;

	//Recall, level + 1
	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500000, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500001, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500002, &error);
	fail_if(error);
	flush_wl_log(&wlog, wlc, 10000, 0, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 1, 500003, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 1, 500004, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 1, 500005, &error);
	fail_if(error);
	flush_wl_log(&wlog, wlc, 10000, 1, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 2, 500006, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 2, 500007, &error);
	fail_if(error);
	wl_log_add_entry_1(&wlog, wlc, 10000, 2, 500008, &error);
	fail_if(error);
	flush_wl_log(&wlog, wlc, 10000, 2, &error);
	fail_if(error);

	set_wl_key(&wkey, 10000, 1, 0);
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500000);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500001);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500002);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_if(res);
	close_wl_iter(witer);

	set_wl_key(&wkey, 10000, 2, 0);
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500003);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500004);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500005);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_if(res);
	close_wl_iter(witer);

	set_wl_key(&wkey, 10000, 3, 0);
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500006);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500007);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(res);
	fail_unless(lin == 500008);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_if(res);
	close_wl_iter(witer);
}

TEST(wl_checkpoint_basic, .attrs="overnight")
{
	struct wl_log wlog = {};
	struct wl_iter *witer = NULL;
	int level = 0;
	btree_key_t wkey = {};
	bool res = false;
	struct wl_entry *entry = NULL;
	uint64_t lin;
	int ret = 0;

	//Recall, level + 1
	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500000, &error);
	fail_if(error);
	flush_wl_log(&wlog, wlc, 10000, 0, &error);
	fail_if(error);

	set_wl_key(&wkey, 10000, 1, 0);
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(lin == 500000);

	//Write a checkpoint
	ret = wl_checkpoint(&wlog, witer, 500, 600, &error);
	fail_if(error);

	//Clear iter and reread it to see if checkpoint happened
	close_wl_iter(witer);
	witer = NULL;
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, &level, &entry, &error);
	close_wl_iter(witer);
	fail_if(error);
	fail_if(!entry);

	//Verify correctness
	fail_unless(lin == 500000);
	fail_unless(entry->dircookie1 == 500);
	fail_unless(entry->dircookie2 == 600);
}

TEST(wl_checkpoint_mid, .attrs="overnight")
{
	struct wl_log wlog = {};
	struct wl_iter *witer = NULL;
	int level = 0;
	btree_key_t wkey = {};
	bool res = false;
	struct wl_entry *entry = NULL;
	uint64_t lin;
	int ret = 0;

	//Recall, level + 1
	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500000, &error);
	fail_if(error);

	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500001, &error);
	fail_if(error);

	wl_log_add_entry_1(&wlog, wlc, 10000, 0, 500002, &error);
	fail_if(error);
	flush_wl_log(&wlog, wlc, 10000, 0, &error);
	fail_if(error);

	set_wl_key(&wkey, 10000, 1, 0);
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(lin == 500000);
	res = wl_iter_next(&wlog, witer, &lin, NULL, NULL, &error);
	fail_if(error);
	fail_unless(lin == 500001);

	//Write a checkpoint
	ret = wl_checkpoint(&wlog, witer, 500, 600, &error);
	fail_if(error);

	//Clear iter and reread it to see if checkpoint happened
	close_wl_iter(witer);
	witer = NULL;
	witer = new_wl_iter(wlc, &wkey);
	res = wl_iter_next(&wlog, witer, &lin, &level, &entry, &error);
	close_wl_iter(witer);
	fail_if(error);
	fail_if(!entry);

	//Verify correctness
	fail_unless(lin == 500001);
	fail_unless(entry->dircookie1 == 500);
	fail_unless(entry->dircookie2 == 600);

	//Cleanup
	if (entry) {
		free(entry);
		entry = NULL;
	}
}
