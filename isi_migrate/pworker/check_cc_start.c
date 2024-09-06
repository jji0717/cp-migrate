#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <isi_util/isi_error.h>
#include <dirent.h>
#include <stdint.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/isi_macro.h>
#include <isi_migrate/migr/isirep.h>
#include <isi_migrate/migr/summ_stf.h>
#include <isi_migrate/migr/check_helper.h>
#include <isi_util/isi_error.h>
#include <check.h>
#include <sys/isi_enc.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_str.h>
#include "pworker.h"
#include "test_helper.h"
#include <isi_snapshot/snapshot.h>
#include <isi_util/util_adt.h>
#include <ifs/ifs_lin_open.h>
#include <isi_testutil/snapshot.h>


#include <isi_util/check_helper.h>
TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

#define CC_TEST_TIMEOUT 240

SUITE_DEFINE_FOR_FILE(cc_start, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout = CC_TEST_TIMEOUT);


int MAX_SNAPS=90;
char pre_repstate_name_root[] = "unittest_repstate_check_cc";
int MAX_REPSTATE=10;
char pre_unit_test_root[] = "/ifs/unittest/check_cc.XXXXXX";
char pre_unit_test_outside_root[] = "/ifs/unittest/check_cc.moves.XXXXXX";

char *unit_test_root;
char *unit_test_outside_root;
char *repstate_name_root;
struct migr_pworker_ctx g_pw_ctx;
struct work_restart_state g_work;
static struct uint64_uint64_map expected;

#define CHECK_BIT_DIR (1ull<<32)
#define CHECK_BIT_NEW (1ull<<33)
#define CHECK_BIT_CHANGED (1ull<<34)

static void
helper_add_expected(ifs_lin_t lin, uint32_t lcount, bool is_dir, bool not_on_target, bool changed)
{
	uint64_t value;
	value = lcount;
	if (is_dir)
		value |= CHECK_BIT_DIR;
	if (not_on_target)
		value |= CHECK_BIT_NEW;
	if (changed)
		value |= CHECK_BIT_CHANGED;
	uint64_uint64_map_add(&expected, lin, &value);
}
static void
helper_add_expected_excl(ifs_lin_t lin, uint32_t lcount, uint32_t excl_lcount, bool is_dir,
    bool not_on_target, bool changed)
{
	uint64_t value;
	value=(excl_lcount << 16);
	value |= lcount;
	if (is_dir)
		value |= CHECK_BIT_DIR;
	if (not_on_target)
		value |= CHECK_BIT_NEW;
	if (changed)
		value |= CHECK_BIT_CHANGED;
	uint64_uint64_map_add(&expected, lin, &value);
}

static void
helper_clear_expected(void)
{
	uint64_uint64_map_clean(&expected);
}

static void
helper_execute_check(struct rep_ctx *rctx, int unknown_lins, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry rep;
	struct rep_iter  *riter = NULL;
	int cnt;
	uint64_t value;
	uint64_t lcount;
	uint64_t excl_lcount;
	ifs_lin_t lin;
	int kept_errno;
	bool got_one;
	

	riter = new_rep_range_iter(rctx, WORK_RES_RANGE_MAX+1);
	cnt = 0;

	errno = 0;
	while (true) {
		got_one = rep_iter_next(riter, &lin, &rep, &error);
		if (error)
			goto out;
		if (!got_one)
			break;
		cnt++;
		if (uint64_uint64_map_contains(&expected, lin)) {
			value = *uint64_uint64_map_find(&expected, lin);
		} else if (unknown_lins == 0){
			error = isi_system_error_new(ENOENT,
			    "Unexpected lin seen: %{}", lin_fmt(lin));
			goto out;
		} else {
			continue;
		}

		if ((!(value & CHECK_BIT_DIR)) != !rep.is_dir) {
			error = isi_system_error_new(EINVAL,
			    "dir bit error: lin %{}, saw %d",
			    lin_fmt(lin), rep.is_dir);
			goto out;
		}
		if ((!(value & CHECK_BIT_NEW)) != !rep.not_on_target) {
			error = isi_system_error_new(EINVAL,
			    "new bit error: lin %{}, saw %d",
			    lin_fmt(lin), rep.not_on_target);
			goto out;
		}
		if ((!(value & CHECK_BIT_CHANGED)) != !rep.changed) {
			error = isi_system_error_new(EINVAL,
			    "changed bit error: lin %{}, saw %d",
			    lin_fmt(lin), rep.changed);
			goto out;
		}
		lcount = value & 0xFFFF;
		if (lcount != rep.lcount) {
			error = isi_system_error_new(EINVAL,
			    "link count error: lin %{}, saw %u, expected %u",
			    lin_fmt(lin), (unsigned)rep.lcount,
			    (unsigned)lcount);
			goto out;
		}
		excl_lcount = ((value & 0xFFFFFFFF) >> 16);

		if (excl_lcount != rep.excl_lcount) {
			error = isi_system_error_new(EINVAL,
			    "excl link count error: lin %{}, saw %u, expected %u",
			    lin_fmt(lin), (unsigned)rep.excl_lcount,
			    (unsigned)excl_lcount);
			goto out;
		}
		errno = 0;
	}
	if (errno) {
		kept_errno = errno;
		error = isi_system_error_new(kept_errno, "Iterator error");
		goto out;
	}

	if (cnt < uint64_uint64_map_size(&expected)) {
		error = isi_system_error_new(EINVAL,
		    "Missing expected entries: map size %d, saw %d",
		    uint64_uint64_map_size(&expected), cnt);
		goto out;
	} else if (unknown_lins >= 0 &&
	    (cnt != unknown_lins + uint64_uint64_map_size(&expected))) {
		error = isi_system_error_new(EINVAL,
		    "Mismatched expected size: expected size %d, saw %d",
		    unknown_lins + uint64_uint64_map_size(&expected), cnt);
		goto out;
	}

 out:
	if (riter)
		close_rep_iter(riter);
	isi_error_handle(error, error_out);
}


static void
fully_remove_single(int cnt)
{
	int error;
	char cmd_str[80];
	sprintf(cmd_str, "isi_stf_test remove %s_%d", repstate_name_root, cnt);

	error = system(cmd_str);
	fail_if(error, "Failed to remove snapshot %s_%d", repstate_name_root, cnt);
}


static ifs_snapid_t
take_snapshot(char *path, int cnt)
{
	struct isi_str snap_name, *paths[2];
	char name[255];
	ifs_snapid_t snapid = 0;
	struct isi_error *error = NULL;

	sprintf(name, "%s_%d", repstate_name_root, cnt);

	isi_str_init(&snap_name, name, strlen(name) + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	paths[0] = isi_str_alloc(path, strlen(path) + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	paths[1] = NULL;

	snapid = snapshot_create(&snap_name, 0, paths, NULL, &error);
	fail_if_isi_error(error);

	isi_str_free(paths[0]);
	return snapid;
}

static ifs_lin_t
get_lin_path(const char *path)
{
	struct stat st;
	int error;

	error = stat(path, &st);
	fail_if(error, "Failed to stat file %s, errno %d", path, errno);
	return st.st_ino;
}

static ifs_lin_t
get_lin_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	return get_lin_path(path);
}

static void
touch_path(const char *path)
{
	char command[MAXPATHLEN + 20];
	sprintf(command, "touch %s", path);
	system(command);
}

static void
touch_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	touch_path(path);
}

static void
link_rel(const char *rel_src, const char *rel_dst)
{
	char path1[MAXPATHLEN], path2[MAXPATHLEN];
	sprintf(path1, "%s/%s", unit_test_root, rel_src);
	sprintf(path2, "%s/%s", unit_test_root, rel_dst);
	link(path1, path2);
}

static void
unlink_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	unlink(path);
}

static void
rename_rel(const char *orig_rel, const char *new_rel)
{
	char orig_path[MAXPATHLEN];
	char new_path[MAXPATHLEN];
	sprintf(orig_path, "%s/%s", unit_test_root, orig_rel);
	sprintf(new_path, "%s/%s", unit_test_root, new_rel);
	rename(orig_path, new_path);
}

static void
rmdir_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	rmdir(path);
}

static void
mkdir_path(const char *path)
{
	char command[MAXPATHLEN + 20];
	sprintf(command, "mkdir -p %s", path);
	system(command);
}

static void
mkdir_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	mkdir_path(path);
}

static void
mvdir_outside(const char *rel)
{
	char path[MAXPATHLEN];
	char command[MAXPATHLEN + 20];
	sprintf(path, "%s/%s", unit_test_root, rel);
	sprintf(command, "mv %s %s", path, unit_test_outside_root);
	system(command);
}

static void
mvdir_rel(const char *rel1, const char *rel2)
{
	char path1[MAXPATHLEN], path2[MAXPATHLEN];
	char command[MAXPATHLEN + 20];
	sprintf(path1, "%s/%s", unit_test_root, rel1);
	sprintf(path2, "%s/%s", unit_test_root, rel2);
	sprintf(command, "mv %s %s", path1, path2);
	system(command);
}

static void
do_full_change_compute(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	int rv;
	struct isi_error *error = NULL;

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	do {
		rv = process_ppath_worklist(pw_ctx, &error);
		if (error)
			goto out;
	} while (rv);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	do {
		rv = process_removed_dirs(pw_ctx, &error);
		if (error)
			goto out;
	} while (rv);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;
	
	do {
		rv = process_removed_dirs_worklist(pw_ctx, &error);
		if (error)
			goto out;
	} while (rv);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	do {
		rv = process_dirent_and_file_mods(pw_ctx, &error);
		if (error)
			goto out;
	} while (rv);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;

	do {
		rv = process_added_dirs_worklist(pw_ctx, &error);
		if (error)
			goto out;
	} while (rv);

	flush_stf_logs(pw_ctx, &error);
	if (error)
		goto out;
 out:
	isi_error_handle(error, error_out);
}

static void
flip_lins(struct changeset_ctx  *chg_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry trep1_entry;
	struct rep_entry trep2_entry;
	struct rep_iter  *rep_iter ;
	uint64_t lin;
	bool got_one;
	bool found;
	
	rep_iter = new_rep_range_iter(chg_ctx->rep2, WORK_RES_RANGE_MAX +1);
	ASSERT(rep_iter != NULL);
	while(1) {
		got_one = rep_iter_next(rep_iter, &lin, &trep2_entry, &error);
		if (error)
			goto out;

		if (!got_one) {
			log(TRACE, 
			    "unable to get next lin entry,"
			    "looks like end of repstate");
			close_rep_iter(rep_iter);
			goto out;
		}
		/* Look for entry in rep1*/
		found = get_entry(lin, &trep1_entry, chg_ctx->rep1, &error);
		if (error)
			goto out;
		if (!found)
			ASSERT(trep2_entry.not_on_target == true);

		/* Check for deleted lins, Delete them from rep1 */
		if (trep2_entry.lcount == 0) {
			remove_entry(lin, chg_ctx->rep1, &error);
			if (error)
				goto out;
		} else {
			trep2_entry.exception = false;
			trep2_entry.hash_computed = false;
			trep2_entry.changed = false;
			trep2_entry.lcount_set = false;
			trep2_entry.lcount_reset = false;
			set_entry(lin, &trep2_entry, chg_ctx->rep1, 0,
			    NULL, &error);
			if (error)
				goto out;
		}
		trep1_entry.flipped = true;
		set_entry(lin, &trep1_entry, chg_ctx->rep2, 0, NULL, &error);
		if (error)
			goto out;
	}
	
out:
	isi_error_handle(error, error_out);
}

static void
init_work(struct work_restart_state *work)
{
	work->min_lin = WORK_RES_RANGE_MAX +1;
	work->max_lin = -1;
	work->dircookie1 = work->dircookie2 = DIR_SLICE_MIN;
	work->lin = 0;
	work->wl_cur_level = 0;
}

TEST_FIXTURE(suite_setup)
{
        struct ifs_dotsnap_options dso = {};

        dso.per_proc = 1;
        dso.root_accessible = 1;
        dso.sub_accessible = 1;
        dso.root_visible = 1;

	ifs_set_dotsnap_options(&dso);
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	char command[255];

	memset(&g_pw_ctx, 0, sizeof(struct migr_pworker_ctx));
	init_pw_ctx(&g_pw_ctx);
	g_pw_ctx.work = &g_work;
	g_pw_ctx.work->min_lin = WORK_RES_RANGE_MAX +1;
	g_pw_ctx.work->max_lin = -1;
	g_pw_ctx.work->dircookie1 = g_pw_ctx.work->dircookie2 = DIR_SLICE_MIN;
	g_pw_ctx.work->lin = 0;
	g_pw_ctx.work->wl_cur_level = 0;
	init_statistics(&g_pw_ctx.stf_cur_stats);
	system("mkdir -p /ifs/unittest");
	unit_test_root = mktemp(pre_unit_test_root);
	unit_test_outside_root = mktemp(pre_unit_test_outside_root);
	fail_unless(unit_test_root != NULL);
	repstate_name_root = malloc(255);
	sprintf(repstate_name_root, "%s%s", pre_repstate_name_root,
	    strchr(unit_test_root, '.') + 1);

	sprintf(command, "mkdir -p %s", unit_test_root);
	system(command);
	sprintf(command, "mkdir -p %s", unit_test_outside_root);
	system(command);
	/*XXX: UGLY */
	sprintf(command, "rm -rf /ifs/.ifsvar/modules/tsm/%s",repstate_name_root);
	system(command);
	uint64_uint64_map_init(&expected);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_str snap_name;
	char name[255] = {};
	int i;
	struct isi_error *error = NULL;

	char command[255];

	for (i = 0; i < MAX_SNAPS; i++) {
		sprintf(name, "%s_%d", repstate_name_root, i);
		isi_str_init(&snap_name, name, strlen(name) + 1, ENC_DEFAULT,
		    ISI_STR_NO_MALLOC);
		snapshot_destroy(&snap_name, &error);
		if (error) {
			isi_error_free(error);
			error = NULL;
		}
	}

	/*XXX: UGLY */
	sprintf(command, "rm -rf /ifs/.ifsvar/modules/tsm/%s_snap_*", repstate_name_root);
	system(command);
	sprintf(command, "rm -rf %s", unit_test_root);
	system(command);
	sprintf(command, "rm -rf %s", unit_test_outside_root);
	system(command);
	if (g_pw_ctx.plan)
		siq_plan_free(g_pw_ctx.plan);
	if (g_pw_ctx.stf_cur_stats) {
		siq_stat_free(g_pw_ctx.stf_cur_stats);
		g_pw_ctx.stf_cur_stats = NULL;
	}
	free(repstate_name_root);
	helper_clear_expected();
}

#if 0
TEST(dummy_create, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid;
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
}

TEST(repeat_create, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, true, &error));
	fail_if_isi_error(error);
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(repeat_create_nosnap, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	fully_remove_single(0);
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	process_stf_lins(&g_pw_ctx, true, &error);
	fail_unless(error != NULL);
	isi_error_free(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(getlins_unchanged, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t lin1, lin2, lin3;
	struct changeset_ctx *cc_ctx;
	int found;
	int i;
	ifs_lin_t lins[10];


	touch_rel("file1");
	touch_rel("file2");
	touch_rel("file3");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lin3 = get_lin_rel("file3");
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, true, &error));
	fail_if_isi_error(error);
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	cc_ctx = g_pw_ctx.chg_ctx;
	cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf, 0);
	do {
		found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins, 10, &error);
		fail_if_isi_error(error);
		for (i=0; i < found; i++) {
			fail_unless(lins[i] != lin1);
			fail_unless(lins[i] != lin2);
			fail_unless(lins[i] != lin3);
		}
	} while (found != 0);

	close_summ_stf_iter(cc_ctx->summ_iter);
	free_changeset(cc_ctx);
	
}



TEST(getlins_changed, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t lin1, lin2, lin3;
	struct changeset_ctx *cc_ctx;
	int found;
	int i;
	ifs_lin_t lins[10];
	int found1 = 0;
	int found2 = 0;


	touch_rel("file1");
	touch_rel("file2");
	touch_rel("file3");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lin3 = get_lin_rel("file3");
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	touch_rel("file1");
	touch_rel("file2");
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, true, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	cc_ctx = g_pw_ctx.chg_ctx;
	cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf, 0);
	do {
		found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins, 10, &error);
		fail_if_isi_error(error);
		fail_unless(found <= 10);
		for (i=0; i < found; i++) {
			if (lins[i] == lin1)
				found1++;
			if (lins[i] == lin2)
				found2++;
			fail_unless(lins[i] != lin3);
		}
	} while (found != 0);

	fail_unless(found1 == 1);
	fail_unless(found2 == 1);
	close_summ_stf_iter(cc_ctx->summ_iter);
	free_changeset(cc_ctx);
}

TEST(getlins_changed_multi_snaps, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2, snapid3;
	struct changeset_ctx *cc_ctx;
	ifs_lin_t lin1, lin2, lin3;
	int found;
	int i;
	ifs_lin_t lins[10];
	int found1 = 0;
	int found2 = 0;


	touch_rel("file1");
	touch_rel("file2");
	touch_rel("file3");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lin3 = get_lin_rel("file3");
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	touch_rel("file1");

	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	touch_rel("file2");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid3 = take_snapshot("/ifs", 2);
	fail_unless(snapid3 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid3, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, true, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	cc_ctx = g_pw_ctx.chg_ctx;
	cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf, 0);
	do {
		found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins, 10, &error);
		fail_if_isi_error(error);
		fail_unless(found <= 10);
		for (i=0; i < found; i++) {
			if (lins[i] == lin1)
				found1++;
			if (lins[i] == lin2)
				found2++;
			fail_unless(lins[i] != lin3);
		}
	} while (found != 0);

	fail_unless(found1 == 1);
	fail_unless(found2 == 1);
	close_summ_stf_iter(cc_ctx->summ_iter);
	free_changeset(cc_ctx);
}

TEST(getlins_changed_multi_query, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	struct changeset_ctx *cc_ctx;
	ifs_lin_t lin1, lin2, lin3;
	int found;
	int i;
	ifs_lin_t lins[10];
	int found1 = 0;
	int found2 = 0;


	touch_rel("file1");
	touch_rel("file2");
	touch_rel("file3");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lin3 = get_lin_rel("file3");
	snapid = take_snapshot("/ifs", 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	touch_rel("file1");
	touch_rel("file2");
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, 2, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot("/ifs", 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, true, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	cc_ctx = g_pw_ctx.chg_ctx;
	cc_ctx->summ_iter = new_summ_stf_iter(cc_ctx->summ_stf, 0);
	do {
		found = get_summ_stf_next_lins(cc_ctx->summ_iter, lins, 1, &error);
		fail_if_isi_error(error);
		fail_unless(found <= 1);
		for (i=0; i < found; i++) {
			if (lins[i] == lin1)
				found1++;
			if (lins[i] == lin2)
				found2++;
			fail_unless(lins[i] != lin3);
		}
	} while (found != 0);

	fail_unless(found1 == 1);
	fail_unless(found2 == 1);
	close_summ_stf_iter(cc_ctx->summ_iter);
	free_changeset(cc_ctx);
}

TEST(process_mod_single_lin_empty, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid,
	    snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(linr, 1, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_two_lin_dir_new, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("file1");
	mkdir_rel("file2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false,
	   &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, true, true, false);
	helper_add_expected(lin2, 1, true, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_two_lin_dir_new_multi_snaps, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2, snapid3, snapid4;
	ifs_lin_t linr, lin1, lin2;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("file1");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	mkdir_rel("file2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	snapid3 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid3 > 0, "Failed to create snapshot");
	snapid4 = take_snapshot(unit_test_root, 3);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid4,
	   false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, true, true, false);
	helper_add_expected(lin2, 1, true, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_single_lin_two_new, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2;


	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid,
	    snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_single_lin_two_old, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr;

	linr = get_lin_path(unit_test_root);
	touch_rel("file1");
	touch_rel("file2");
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(linr, 1, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_single_lin_two_new_two_old, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin3;


	linr = get_lin_path(unit_test_root);
	touch_rel("file2");
	touch_rel("file4");
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file3");
	lin1 = get_lin_rel("file1");
	lin3 = get_lin_rel("file3");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin3, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_single_lin_link_adds, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1;


	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	lin1 = get_lin_rel("file1");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	link_rel("file1", "link1");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(lin1, 2, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);

}

TEST(process_mod_all_lin_new_trees, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	touch_rel("dir1/file1a");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	while (process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_all_lin_tree_add_link, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	touch_rel("dir1/file1a");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while(process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	link_rel("dir1/file1a", "dir2/link1");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while(process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir2, 1, true, false, true);
	helper_add_expected(lin1a, 2, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_all_lin_tree_add_link_multi_snaps, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2, snapid3, snapid4;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	touch_rel("dir1/file1a");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	snapid3 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid3 > 0, "Failed to create snapshot");
	dir2 = get_lin_rel("dir2");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid4 = take_snapshot(unit_test_root, 3);
	fail_unless(snapid4 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid4, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while(process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	link_rel("dir1/file1a", "dir2/link1");
	snapid = snapid4;
	snapid4 = take_snapshot(unit_test_root, 4);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid4, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while(process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir2, 1, true, false, true);
	helper_add_expected(lin1a, 2, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_mod_all_lin_tree_remove_file, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	touch_rel("dir1/file1a");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);


	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	unlink_rel("dir1/file1a");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_unless(!error, "%s", error ?isi_error_get_message(error): "");
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir1, 1, true, false, true);
	helper_add_expected(lin1a, 0, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	/* Ugly readdir interface */

	free_changeset(g_pw_ctx.chg_ctx);
}


TEST(process_mod_all_lin_tree_remove_tree, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;


	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	touch_rel("dir1/file1a");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	unlink_rel("dir1/file1a");
	rmdir_rel("dir1");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir1, 0, true, false, true);
	helper_add_expected(lin1a, 0, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(process_recurse_all_lin_tree_remove_tree, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, dir1, dir2, dir3, dir4, dir5;


	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("dir1");
	mkdir_rel("dir1/dir3");
	mkdir_rel("dir1/dir3/dir4");
	mkdir_rel("dir1/dir3/dir4/dir5");
	mkdir_rel("dir2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	dir3 = get_lin_rel("dir1/dir3");
	dir4 = get_lin_rel("dir1/dir3/dir4");
	dir5 = get_lin_rel("dir1/dir3/dir4/dir5");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(dir3, 1, true, true, false);
	helper_add_expected(dir4, 1, true, true, false);
	helper_add_expected(dir5, 1, true, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	mvdir_outside("dir1/dir3");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir3, 0, true, false, true);
	helper_add_expected(dir4, 0, true, false, true);
	helper_add_expected(dir5, 0, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test repstate link count/lin contents against a series of
 * snapshots made durring an fsstress run.  Note that fsstress is a
 * bit limited for this use since it will never move entire directories
 * into or out of the replication set.
 *
 * as of now mknod is disabled.
 */
#define NUM_SNAP_FSSTRESS_CHANGE_TEST 5
TEST(fsstress_change_test, .attrs="overnight") {
	pid_t fsstress_pid;
	ifs_snapid_t allsnap[NUM_SNAP_FSSTRESS_CHANGE_TEST];
	ifs_lin_t rootlin;
	int i;
	char repstate_name[255];
	char snap_root[1024];
	u_int64_t entry_count;
	struct isi_error *error = NULL;
	int status;
	time_t start_time;

	start_time = time(NULL);
	rootlin = get_lin_path(unit_test_root);
	/* Create an initial empty tree and dummy repstate */
	allsnap[0] = take_snapshot(unit_test_root, 0);
	fail_unless(allsnap[0] > 0, "Failed to create snapshot");
	fprintf(stderr, "\nTaking initial snapshot: %lld\n", allsnap[0]);
	create_dummy_begin(repstate_name_root, allsnap[0], rootlin, &error);
	fail_if_isi_error(error);
	/* Start up fsstress in parallel building a tree */
	fsstress_pid = fork();
	if (!fsstress_pid) {
		fprintf(stderr, "\nfsstress starting\n");
		execl("/usr/bin/fsstress", "/usr/bin/fsstress", "-d", unit_test_root,
		    "-n 30000", "-f", "mknod=0", (char *)NULL);
		fprintf(stderr, "fsstress ended\n");
		exit(0);
	}
	fail_unless(fsstress_pid > 0);
	/*
	 * Wait for fsstress to get started and then take a snapshot
	 * every 3 seconds for NUM_SNAP_FSSTRESS_CHANGE_TEST
	 * snapshots.
	 */
	sleep(4);
	fprintf(stderr, "\nTaking snapshot: ");
	for (i = 1; i < NUM_SNAP_FSSTRESS_CHANGE_TEST; i++) {
		allsnap[i] = take_snapshot(unit_test_root, i);
		if (allsnap[i] <= 0) {
			kill(fsstress_pid, SIGKILL);
			waitpid(fsstress_pid, &status, 0);
			fail_unless(allsnap[i] > 0,
			    "Failed to create snapshot");
		}
		fprintf(stderr, "%lld ", allsnap[i]);
		if (i < (NUM_SNAP_FSSTRESS_CHANGE_TEST + 1))
			sleep(3);
	}
	fprintf(stderr,"\n");
	/*
	 * Kill fssterss so asserts below won't leave it running
	 */
	kill(fsstress_pid, SIGKILL);
	waitpid(fsstress_pid, &status, 0);

	/**
	 * Build and test sequential repstates for each snapshot or until we're
	 * in danger of timing out.  Be paranoid about timing out since false
	 * failures are BAD.
	 */
	for (i = 0; (i < (NUM_SNAP_FSSTRESS_CHANGE_TEST - 1)) &&
	    ((time(NULL) - start_time) < CC_TEST_TIMEOUT / 4) ; i++) {
		g_pw_ctx.chg_ctx = create_changeset(repstate_name_root,
		    allsnap[i], allsnap[i + 1], false, &error);
		fail_if_isi_error(error);
		g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
		fail_unless(g_pw_ctx.chg_ctx != NULL);
		init_work(g_pw_ctx.work);
		set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
		fail_if_isi_error(error);
		while(!process_stf_lins(&g_pw_ctx, false, &error));
		fail_if_isi_error(error);
		do_full_change_compute(&g_pw_ctx, &error);
		fail_if_isi_error(error);
		sprintf(repstate_name, "%s_snap_rep_base", repstate_name_root);
		sprintf(snap_root, "%s/.snapshot/%s_%d", unit_test_root,
		    repstate_name_root, i + 1);
		flip_lins(g_pw_ctx.chg_ctx, &error);
		fail_if_isi_error(error);
		free_changeset(g_pw_ctx.chg_ctx);
		entry_count = repstate_tester(snap_root, repstate_name_root,
		    repstate_name, &error);
		fail_if_isi_error(error);
		fprintf(stderr, "Snap %lld, %lld entries %s %s\n",
		    allsnap[i + 1], entry_count, snap_root, repstate_name);
	}
	
}

#define NUM_SNAPS_FSSTRESS (STF_COUNT + 2)
TEST(fsstress_stf_parallel_walk, .attrs="overnight") {
	pid_t fsstress_pid;
	ifs_snapid_t allsnap[NUM_SNAPS_FSSTRESS];
	ifs_lin_t rootlin;
	int i;
	char repstate_name[255];
	char snap_root[1024];
	u_int64_t entry_count;
	struct isi_error *error = NULL;
	int status;
	time_t start_time;
	int time_diff;

	start_time = time(NULL);
	rootlin = get_lin_path(unit_test_root);
	/* Create an initial empty tree and dummy repstate */
	allsnap[0] = take_snapshot(unit_test_root, 0);
	fail_unless(allsnap[0] > 0, "Failed to create snapshot");
	fprintf(stderr, "\nTaking initial snapshot: %lld\n", allsnap[0]);
	create_dummy_begin(repstate_name_root, allsnap[0], rootlin, &error);
	fail_if_isi_error(error);
	/* Start up fsstress in parallel building a tree */
	fsstress_pid = fork();
	if (!fsstress_pid) {
		fprintf(stderr, "\nfsstress starting\n");
		execl("/usr/bin/fsstress", "/usr/bin/fsstress", "-d", unit_test_root,
		    "-n 10000", "-f", "mknod=0", (char *)NULL);
		fprintf(stderr, "fsstress ended\n");
		exit(0);
	}
	fail_unless(fsstress_pid > 0);
	/*
	 * Wait for fsstress to get started and then take a snapshot
	 * every 3 seconds for NUM_SNAP_FSSTRESS_CHANGE_TEST
	 * snapshots.
	 */
	sleep(4);
	fprintf(stderr, "\nTaking snapshot: ");
	for (i = 1; i < NUM_SNAPS_FSSTRESS; i++) {
		allsnap[i] = take_snapshot(unit_test_root, i);
		if (allsnap[i] <= 0) {
			kill(fsstress_pid, SIGKILL);
			waitpid(fsstress_pid, &status, 0);
			fail_unless(allsnap[i] > 0,
			    "Failed to create snapshot");
		}
		fprintf(stderr, "%lld ", allsnap[i]);
		if (i < (NUM_SNAPS_FSSTRESS + 1))
			sleep(3);
	}
	fprintf(stderr,"\n");
	/*
	 * Kill fssterss so asserts below won't leave it running
	 */
	kill(fsstress_pid, SIGKILL);
	waitpid(fsstress_pid, &status, 0);

	/*
	 * Make sure we have enough time to finish test 
	 */
	time_diff = (CC_TEST_TIMEOUT / 2) - (time(NULL) - start_time);

	if (time_diff < 0) {
		fprintf(stderr, "Skipping changeset computation since snapshot"
		    " operations cross %d secs\n", CC_TEST_TIMEOUT / 2);
	} else {
		g_pw_ctx.chg_ctx = create_changeset(repstate_name_root,
		    allsnap[0], allsnap[NUM_SNAPS_FSSTRESS - 1], false, &error);
		fail_if_isi_error(error);
		g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
		fail_unless(g_pw_ctx.chg_ctx != NULL);
		init_work(g_pw_ctx.work);
		set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0,
		    g_pw_ctx.chg_ctx->rep2, &error);
		fail_if_isi_error(error);
		while(!process_stf_lins(&g_pw_ctx, false, &error));
		fail_if_isi_error(error);
		do_full_change_compute(&g_pw_ctx, &error);
		fail_if_isi_error(error);
		sprintf(repstate_name, "%s_snap_rep_base", repstate_name_root);
		sprintf(snap_root, "%s/.snapshot/%s_%d", unit_test_root,
		    repstate_name_root, NUM_SNAPS_FSSTRESS - 1);
		flip_lins(g_pw_ctx.chg_ctx, &error);
		fail_if_isi_error(error);
		free_changeset(g_pw_ctx.chg_ctx);
		entry_count = repstate_tester(snap_root, repstate_name_root,
		    repstate_name, &error);
		fail_if_isi_error(error);
		fprintf(stderr, "Between Snap %lld - %lld, %lld entries %s %s\n",
		    allsnap[0], allsnap[NUM_SNAPS_FSSTRESS -  1], entry_count,
		    snap_root, repstate_name);
	}

}

/**
 * Test that a newly created directory dirent that doesn't match a filename
 * predicate is still transferred.
 */
TEST(process_fname_skipped_dir_send, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	fail_unless(!error, "%s", error ?isi_error_get_message(error): "");
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("skip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("skip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lins, 1, true, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}


/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_fname_skipped_send, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("skip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("skip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_fname_skipped_rename_unskipped, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("skip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("skip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	rename_rel("skip", "filenonskip");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_clear_expected();
	helper_add_expected_excl(lins, 1, 0, false, true, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);



	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_fname_unskipped_rename_skipped, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("filenonskip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("filenonskip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 0, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	rename_rel("filenonskip", "skip");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_clear_expected();
	helper_add_expected_excl(lins, 1, 1, false, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);


	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Touch it and make sure we now detect it as untransferred */
	touch_rel("skip");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 3);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_clear_expected();
	helper_add_expected_excl(lins, 1, 1, false, true, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);



	free_changeset(g_pw_ctx.chg_ctx);
}


/**
 * Test that removing a directory dirent whose filename doesn't match
 * a predicate still transfers
 */
TEST(process_fname_skipped_dir_remove, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("skip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("skip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_unless(!error, "%s", error ?isi_error_get_message(error): "");
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lins, 1, true, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Start another pass unlinking file3 and file 1 */
	rmdir_rel("skip");
	unlink_rel("file1");

	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(lin1, 0, false, false, true);
	helper_add_expected(lins, 0, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}


/**
 * Test that removing a dirent whose filename caused it not to be transferred
 * originally works.
 */
TEST(process_fname_skipped_remove, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("skip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("skip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Start another pass unlinking file3 and file 1 */
	unlink_rel("skip");
	unlink_rel("file1");

	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_unless(!error, "%s", error ?isi_error_get_message(error): "");
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(lin1, 0, false, false, true);
	helper_add_expected_excl(lins, 0, 0, false, true, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that removing a dirent whose filename caused it not to be transferred
 * originally works while adding a new dirent with the same cookie that isn't
 * skipped.
 */
TEST(process_fname_skipped_remove_unskipped_add, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, linS, linskipped;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("FILEskip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	linskipped = get_lin_rel("FILEskip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected_excl(linskipped, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Start another pass unlinking skip and file 1 */
	unlink_rel("FILEskip");
	unlink_rel("file1");
	touch_rel("fileSKIP");
	linS = get_lin_rel("fileSKIP");

	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(lin1, 0, false, false, true);
	helper_add_expected(linS, 1, false, true, false);
	helper_add_expected_excl(linskipped, 0, 0, false, true, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}
/**
 * Test that removing a dirent whose filename caused it to be transferred
 * originally works while adding a new dirent with the same cookie that is
 * skipped.
 */
TEST(process_fname_unskipped_remove_skipped_add, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lins, linskipped;

	g_pw_ctx.plan = siq_parser_parse("-name 'file*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	touch_rel("fileskip");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	lins = get_lin_rel("fileskip");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_unless(!error, "%s", error ?isi_error_get_message(error): "");

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lins, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Start another pass unlinking skip and file 1 */
	unlink_rel("fileskip");
	unlink_rel("file1");
	touch_rel("FILEskip");
	linskipped = get_lin_rel("FILEskip");

	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(lin1, 0, false, false, true);
	helper_add_expected(lins, 0, false, false, true);
	helper_add_expected_excl(linskipped, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(try_worklist_split_with_no_lins, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lin;
	bool found_split;
	bool exists;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, 
	    snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);

	process_single_lin_modify(&g_pw_ctx, linr, false, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	exists = wl_get_entry(&g_pw_ctx, &lin, &error);
	fail_if_isi_error(error);
	fail_unless(exists == false);
	g_pw_ctx.wi_lin = WORK_RES_RANGE_MIN + 1;
	found_split = try_worklist_split(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found_split == false);
	
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(try_worklist_split_with_n_added_dirs, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;
	ifs_lin_t dir3, dir4, dir5;
	bool found_split;
	struct work_restart_state work = {};

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	mkdir_rel("dir3");
	mkdir_rel("dir4");
	mkdir_rel("dir5");
	touch_rel("dir1/file1a");
	touch_rel("dir2/file1b");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	dir3 = get_lin_rel("dir3");
	dir4 = get_lin_rel("dir4");
	dir5 = get_lin_rel("dir5");
	lin1a = get_lin_rel("dir1/file1a");
	lin1b = get_lin_rel("dir2/file1b");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	while(process_dirent_and_file_mods(&g_pw_ctx, &error));
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	process_added_dirs_worklist(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	/* Try to split worklist */
	g_pw_ctx.wi_lin = WORK_RES_RANGE_MIN;
	found_split = try_worklist_split(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	fail_unless(found_split == true);

	/* Process my portion of work lins */	
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);

	/* Check contents of new wid */
	close_wl_iter(g_pw_ctx.chg_ctx->wl_iter);
	g_pw_ctx.chg_ctx->wl_iter = NULL;	
	init_work(&work);
	g_pw_ctx.work = &work;

	/* Process my portion of work lins */	
	while (process_added_dirs_worklist(&g_pw_ctx, &error));
	fail_if_isi_error(error);

	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(dir3, 1, true, true, false);
	helper_add_expected(dir4, 1, true, true, false);
	helper_add_expected(dir5, 1, true, true, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, false, true, false);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);
		
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(try_dirent_iteration_with_checkpoints, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid1, snapid2;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, lin1a, lin1b;
	struct migr_dir *md_out = NULL;
	struct migr_dirent *de = NULL;
	enum dirent_mod_type modtype;
	struct work_restart_state *work;
	struct rep_entry rep_entry = {};
	bool exists;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	snapid1 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;

	/* Add new entries to repstate and check that we covered all of them */
	while((de = find_modified_dirent(&g_pw_ctx, linr, false, &work->dircookie2,
	    false, &work->dircookie1, &modtype, &md_out, true, &error))) {
		fail_if_isi_error(error);
		fail_unless(modtype == DENT_ADDED);
		exists = entry_exists(de->stat->st_ino, g_pw_ctx.chg_ctx->rep2,
		    &error);
		fail_if_isi_error(error);
		fail_unless(exists == false);
		memset(&rep_entry, 0, sizeof(rep_entry));
		rep_entry.not_on_target = true;
		rep_entry.is_dir = (de->dirent->d_type == DT_DIR);
		rep_entry.lcount = 1;
		set_entry(de->stat->st_ino, &rep_entry, g_pw_ctx.chg_ctx->rep2,
		    0, NULL, &error);
		fail_if_isi_error(error);
		/* Reset the dir_lin that would force us to use resume_cookie */
		g_pw_ctx.chg_ctx->dir_lin = 0;

		migr_dir_unref(md_out, de);
		de = NULL;
	}


	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	
	unlink_rel("file1");
	touch_rel("file1a");
	mkdir_rel("dir1b");
	lin1a = get_lin_rel("file1a");
	lin1b = get_lin_rel("dir1b");
	
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid1, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;
	md_out = NULL;
	
	/* Add new entries to repstate and check that we covered all of them */
	while((de = find_modified_dirent(&g_pw_ctx, linr, false, &work->dircookie2,
	    false, &work->dircookie1, &modtype, &md_out, true, &error))) {
		fail_if_isi_error(error);
		exists = entry_exists(de->stat->st_ino, g_pw_ctx.chg_ctx->rep2,
		    &error);
		fail_if_isi_error(error);
		fail_unless(exists == false);

		memset(&rep_entry, 0, sizeof(rep_entry));
		rep_entry.not_on_target = (modtype == DENT_ADDED);
		rep_entry.is_dir = (de->dirent->d_type == DT_DIR);
		rep_entry.lcount = modtype == DENT_ADDED;
		set_entry(de->stat->st_ino, &rep_entry, g_pw_ctx.chg_ctx->rep2,
		    0, NULL, &error);
		fail_if_isi_error(error);
		/* Reset the dir_lin that would force us to use resume_cookie */
		g_pw_ctx.chg_ctx->dir_lin = 0;

		migr_dir_unref(md_out, de);
		de = NULL;
	}


	helper_clear_expected();
	helper_add_expected(lin1, 0, false, false, false);
	helper_add_expected(lin1a, 1, false, true, false);
	helper_add_expected(lin1b, 1, true, true, false);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

enum sel_type {
	EXCLUDE = 1,
	INCLUDE,
	FOR_CHILD
};

static void
add_sel_entry(ifs_lin_t mylin, ifs_lin_t rel_lin, char *name, 
    enum sel_type sel, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct sel_entry sel_entry = {};

	sel_entry.plin = rel_lin;
	strcpy(sel_entry.name, name);
	sel_entry.enc = ENC_DEFAULT;
	switch(sel) {
		case EXCLUDE: 
			sel_entry.excludes = 1;
			break;
		case INCLUDE:
			sel_entry.includes = 1;
			break;
		case FOR_CHILD:
			sel_entry.for_child = 1;
			break;
		default:
			ASSERT(0);
	}
	set_sel_entry(mylin, &sel_entry, g_pw_ctx.chg_ctx->sel2, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}
TEST(try_simple_exclusion_rules_routines, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid1;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, dirx;
	struct migr_dir *md_out = NULL;
	struct migr_dirent *de = NULL;
	enum dirent_mod_type modtype;
	struct work_restart_state *work;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	mkdir_rel("dir1/dirx");
	mkdir_rel("dir1/dirx/test1");
	mkdir_rel("dir1/dirx/test2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	dirx = get_lin_rel("dir1/dirx");
	snapid1 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;

	g_pw_ctx.has_excludes = 1;

	/* Set all excludes */	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(lin1, linr, "file1", EXCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dirx, dir1, "dirx", EXCLUDE, &error);
	fail_if_isi_error(error);

	/* check that we dont hit file exclude */
	while((de = find_modified_dirent(&g_pw_ctx, linr, false, &work->dircookie2,
	    false, &work->dircookie1, &modtype, &md_out, true, &error))) {
		fail_if(de->stat->st_ino == lin1);
		migr_dir_unref(md_out, de);
		de = NULL;
	}

	init_work(g_pw_ctx.work);
	/* check that we dont hit directory exclude */
	while((de = find_modified_dirent(&g_pw_ctx, dir1, true, &work->dircookie2,
	    false, &work->dircookie1, &modtype, &md_out, true, &error))) {
		fail_if(de->stat->st_ino == dirx);
		migr_dir_unref(md_out, de);
		de = NULL;
	}

	free_changeset(g_pw_ctx.chg_ctx);
}
	
TEST(try_simple_exclusion_rules, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid1;
	ifs_lin_t linr, lin1, lin2, dir1, dir2, dir3, dirx, old_dirx;
	ifs_lin_t lint1, lint2;
	struct work_restart_state *work;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	touch_rel("file1");
	touch_rel("file2");
	mkdir_rel("dir1");
	mkdir_rel("dir2");
	mkdir_rel("dir3");
	mkdir_rel("dir1/dirx");
	mkdir_rel("dir1/dirx/test1");
	mkdir_rel("dir1/dirx/test2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	dir1 = get_lin_rel("dir1");
	dir2 = get_lin_rel("dir2");
	dir3 = get_lin_rel("dir3");
	dirx = get_lin_rel("dir1/dirx");
	lint1 = get_lin_rel("dir1/dirx/test1");
	lint2 = get_lin_rel("dir1/dirx/test2");
	snapid1 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;

	g_pw_ctx.has_excludes = 1;
	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(lin1, linr, "file1", EXCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dirx, dir1, "dirx", EXCLUDE, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(dir3, 1, true, true, false);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);

	/* Now lets move around exclude directory to rep dir */
	mvdir_rel("dir1/dirx", "dir2");
	mkdir_rel("dir1/dirx");
	mvdir_rel("dir3", "dir1/dirx");
	
	old_dirx = dirx;
	dirx = get_lin_rel("dir1/dirx");
	snapid = snapid1;
	snapid1 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	g_pw_ctx.has_excludes = 1;

	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(lin1, linr, "file1", EXCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dirx, dir1, "dirx", EXCLUDE, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir1, 1, true, false, true);
	helper_add_expected(dir2, 1, true, false, true);
	helper_add_expected(old_dirx, 1, true, true, false);
	helper_add_expected(lint1, 1, true, true, false);
	helper_add_expected(lint2, 1, true, true, false);
	helper_add_expected(dir3, 0, true, false, true);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);

	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(try_simple_exclusion_inclusion_rules_routines, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid1;
	ifs_lin_t linr;
	ifs_lin_t file1_i, dir1_i, file1_x, dir_x, for_child1, dir_i = 0, dir1, file1, dir1_x;
	ifs_lin_t new_for_child1, new_dir_i, new_dir1_i;
	int count;
	struct migr_dir *md_out = NULL;
	struct migr_dirent *de = NULL;
	enum dirent_mod_type modtype;
	struct work_restart_state *work;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	mkdir_rel("dir1");
	touch_rel("dir1/file1");
	mkdir_rel("dir1/dir2");	
	touch_rel("dir1/dir2/file2");	
	touch_rel("dir1/dir2/file3");	
	mkdir_rel("dir1/dir2/dir-i");	
	mkdir_rel("dir1/dir2/dir-i/test2");	
	mkdir_rel("dir1/dir-x");
	mkdir_rel("dir1/dir-x/test1");
	mkdir_rel("dir1/dir-x/test2");
	mkdir_rel("dir1/dir-x/for-child");
	mkdir_rel("dir1/dir-x/for-child/dir1-x");
	mkdir_rel("dir1/dir-x/for-child/dir-i");
	mkdir_rel("dir1/dir-x/for-child/dir-i/test1");
	touch_rel("dir1/dir-x/for-child/dir-i/file1");
	touch_rel("dir1/dir-x/for-child/dir-i/file-x");

	dir1 = get_lin_rel("dir1");
	file1 = get_lin_rel("dir1/file1");
	dir_x = get_lin_rel("dir1/dir-x");
	for_child1 = get_lin_rel("dir1/dir-x/for-child");
	dir_i = get_lin_rel("dir1/dir-x/for-child/dir-i");
	dir1_x = get_lin_rel("dir1/dir-x/for-child/dir1-x");
	dir1_i = get_lin_rel("dir1/dir-x/for-child/dir-i/test1");
	file1_i = get_lin_rel("dir1/dir-x/for-child/dir-i/file1");
	file1_x = get_lin_rel("dir1/dir-x/for-child/dir-i/file-x");
	snapid1 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;

	g_pw_ctx.has_excludes = 1;

	/* Set all excludes */	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_x, dir1, "dir-x", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(for_child1, dir_x, "for-child", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_i, for_child1, "dir-i", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(file1_x, dir_i, "file-x", EXCLUDE, &error);
	fail_if_isi_error(error);

	/* check that for-child only gives one dirent */
	while((de = find_modified_dirent(&g_pw_ctx, dir_x, true, &work->dircookie2,
	    false, &work->dircookie1, &modtype, &md_out, true, &error))) {
		fail_unless(de->stat->st_ino == for_child1);
		migr_dir_unref(md_out, de);
		de = NULL;
	}

	init_work(g_pw_ctx.work);

	/* check that for-child only gives one dirent */
	while((de = find_modified_dirent(&g_pw_ctx, for_child1, true, 
	    &work->dircookie2, false, &work->dircookie1, 
	    &modtype, &md_out, true, &error))) {
		fail_unless(de->stat->st_ino == dir_i);
		migr_dir_unref(md_out, de);
		de = NULL;
	}

	init_work(g_pw_ctx.work);
	/* check that included directory doesnt give excluded file */
	count = 0;
	while((de = find_modified_dirent(&g_pw_ctx, dir_i, true,
	    &work->dircookie2, false, &work->dircookie1, 
	    &modtype, &md_out, true, &error))) {
		fail_if(de->stat->st_ino == file1_x);
		migr_dir_unref(md_out, de);
		de = NULL;
		count++;
	}
	fail_unless( count == 2);
	free_changeset(g_pw_ctx.chg_ctx);


	/* let us move around folders */
	mvdir_rel("dir1/dir-x/for-child", "dir1");
	mvdir_rel("dir1/dir2", "dir1/dir-x/for-child");
	new_for_child1 = get_lin_rel("dir1/dir-x/for-child");
	new_dir_i = get_lin_rel("dir1/dir-x/for-child/dir-i");
	new_dir1_i = get_lin_rel("dir1/dir-x/for-child/dir-i/test2");

	snapid = snapid1;
	snapid1 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	g_pw_ctx.has_excludes = 1;

	/* Set all excludes */	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_x, dir1, "dir-x", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(new_for_child1, dir_x, "for-child", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(new_dir_i, new_for_child1, "dir-i", INCLUDE, &error);
	fail_if_isi_error(error);


	/* check that we correctly manage for-child directory */
	while((de = find_modified_dirent(&g_pw_ctx, dir_x, false,
	    &work->dircookie2, false, &work->dircookie1, 
	    &modtype, &md_out, true, &error))) {
		if (modtype == DENT_REMOVED) {
			fail_unless(de->stat->st_ino == for_child1);
		} else if (modtype == DENT_ADDED) {
			fail_unless(de->stat->st_ino == new_for_child1);
		}
		migr_dir_unref(md_out, de);
		de = NULL;
	}

	init_work(g_pw_ctx.work);
	count = 0;
	/* check that unnecessary dirents are removed from new for-child */
	while((de = find_modified_dirent(&g_pw_ctx, new_for_child1, false,
	    &work->dircookie2, false, &work->dircookie1, &modtype,
	    &md_out, true, &error))) {
		if (modtype == DENT_REMOVED) {
			fail_if(de->stat->st_ino == dir_i);
			count++;
		}
		fail_if(modtype == DENT_ADDED);
		migr_dir_unref(md_out, de);
		de = NULL;
	}
	fail_unless(count == 2);

	init_work(g_pw_ctx.work);
	/* check that we include all dirents for old for-child */
	count = 0;
	while((de = find_modified_dirent(&g_pw_ctx, for_child1, false,
	    &work->dircookie2, false, &work->dircookie1, &modtype,
	    &md_out, true, &error))) {
		fail_if(de->stat->st_ino == dir_i);
		migr_dir_unref(md_out, de);
		de = NULL;
		count++;
	}
	fail_unless( count == 1);
	free_changeset(g_pw_ctx.chg_ctx);
}


TEST(try_simple_exclusion_inclusion_rules_complex, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid1;
	ifs_lin_t linr;
	ifs_lin_t file1_i, file1_x, dir_x, for_child1, dir_i = 0, dir1, file1, dir1_x;
	ifs_lin_t new_for_child1, new_dir_i, new_dir1_i, dir2, file2, test2, test1, test3;
	struct work_restart_state *work;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);

	mkdir_rel("dir1");
	touch_rel("dir1/file1");
	mkdir_rel("dir1/dir2");	
	touch_rel("dir1/dir2/file2");	
	mkdir_rel("dir1/dir2/dir-i");	
	mkdir_rel("dir1/dir2/dir-i/test2");	
	mkdir_rel("dir1/dir-x");
	mkdir_rel("dir1/dir-x/test1");
	mkdir_rel("dir1/dir-x/for-child");
	mkdir_rel("dir1/dir-x/for-child/dir1-x");
	mkdir_rel("dir1/dir-x/for-child/dir-i");
	mkdir_rel("dir1/dir-x/for-child/dir-i/test3");
	touch_rel("dir1/dir-x/for-child/dir-i/file1");
	touch_rel("dir1/dir-x/for-child/dir-i/file-x");

	dir1 = get_lin_rel("dir1");
	file1 = get_lin_rel("dir1/file1");
	dir2 = get_lin_rel("dir1/dir2");
	file2 = get_lin_rel("dir1/dir2/file2");
	new_dir_i = get_lin_rel("dir1/dir2/dir-i");
	test2 = get_lin_rel("dir1/dir2/dir-i/test2");
	dir_x = get_lin_rel("dir1/dir-x");
	test1 = get_lin_rel("dir1/dir-x/test1");
	for_child1 = get_lin_rel("dir1/dir-x/for-child");
	dir_i = get_lin_rel("dir1/dir-x/for-child/dir-i");
	dir1_x = get_lin_rel("dir1/dir-x/for-child/dir1-x");
	test3 = get_lin_rel("dir1/dir-x/for-child/dir-i/test3");
	file1_i = get_lin_rel("dir1/dir-x/for-child/dir-i/file1");
	file1_x = get_lin_rel("dir1/dir-x/for-child/dir-i/file-x");

	snapid1 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	work = g_pw_ctx.work;

	g_pw_ctx.has_excludes = 1;

	/* Set all excludes */	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_x, dir1, "dir-x", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(for_child1, dir_x, "for-child", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_i, for_child1, "dir-i", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(file1_x, dir_i, "file-x", EXCLUDE, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(dir1, 1, true, true, false);
	helper_add_expected(dir2, 1, true, true, false);
	helper_add_expected(file1, 1, false, true, false);
	helper_add_expected(file2, 1, false, true, false);
	helper_add_expected(new_dir_i, 1, true, true, false);
	helper_add_expected(test2, 1, true, true, false);
	helper_add_expected(dir_x, 1, true, true, false);
	helper_add_expected(for_child1, 1, true, true, false);
	helper_add_expected(dir_i, 1, true, true, false);
	helper_add_expected(test3, 1, true, true, false);
	helper_add_expected(file1_i, 1, false, true, false);

	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);

	free_changeset(g_pw_ctx.chg_ctx);


	/* let us move around folders */
	mvdir_rel("dir1/dir-x/for-child", "dir1");
	mvdir_rel("dir1/dir2", "dir1/dir-x/for-child");
	new_for_child1 = get_lin_rel("dir1/dir-x/for-child");
	new_dir_i = get_lin_rel("dir1/dir-x/for-child/dir-i");
	new_dir1_i = get_lin_rel("dir1/dir-x/for-child/dir-i/test2");

	snapid = snapid1;
	snapid1 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid1 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid1, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	init_work(g_pw_ctx.work);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	g_pw_ctx.has_excludes = 1;

	/* Set all excludes */	
	add_sel_entry(linr, 0, "", INCLUDE, &error);
	fail_if_isi_error(error);
	add_sel_entry(dir_x, dir1, "dir-x", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(new_for_child1, dir_x, "for-child", FOR_CHILD, &error);
	fail_if_isi_error(error);
	add_sel_entry(new_dir_i, new_for_child1, "dir-i", INCLUDE, &error);
	fail_if_isi_error(error);

	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	check_select_entries(g_pw_ctx.chg_ctx, g_pw_ctx.chg_ctx->sel1, true, &error);
	fail_if_isi_error(error);
	check_select_entries(g_pw_ctx.chg_ctx, g_pw_ctx.chg_ctx->sel2, true, &error);
	fail_if_isi_error(error);

	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_clear_expected();
	helper_add_expected(dir1, 1, true, false, true);
	helper_add_expected(dir2, 1, true, false, true);
	helper_add_expected(file2, 0, false, false, false);
	helper_add_expected(dir_x, 1, true, false, true);
	helper_add_expected(for_child1, 1, true, false, true);
	helper_add_expected(dir1_x, 1, true, true, false);
	helper_add_expected(file1_x, 1, false, true, false);
	helper_add_expected(dir_i, 1, true, false, true);
	
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 0, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_path_skipped_send, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lin3, lins, lins2, dirlin;

	g_pw_ctx.plan = siq_parser_parse("-path '*mdir*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("mdir");
	touch_rel("mdir/file1");
	touch_rel("mdir/file2");
	touch_rel("mdir/file3");
	touch_rel("skip");
	touch_rel("skip2");
	dirlin = get_lin_rel("mdir");
	lin1 = get_lin_rel("mdir/file1");
	lin2 = get_lin_rel("mdir/file2");
	lin3 = get_lin_rel("mdir/file3");
	lins = get_lin_rel("skip");
	lins2 = get_lin_rel("skip2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(dirlin, 1, true, true, false);
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lin3, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_add_expected_excl(lins2, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_path_skipped_send_nonskipped, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lin3, lins, lins2, dirlin, dirlin2, dirlin3, dirlin4, dirlin5;

	g_pw_ctx.plan = siq_parser_parse("-path '*mdir*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("top");
	mkdir_rel("top/dir");
	mkdir_rel("top/dir/mdir");
	touch_rel("top/dir/mdir/file1");
	touch_rel("top/dir/mdir/file2");
	touch_rel("top/dir/mdir/file3");
	mkdir_rel("stop");
	mkdir_rel("stop/sdir");
	touch_rel("stop/sdir/skip");
	touch_rel("stop/sdir/skip2");
	dirlin = get_lin_rel("top/dir/mdir");
	dirlin2 = get_lin_rel("top/dir");
	dirlin3 = get_lin_rel("top");
	dirlin4 = get_lin_rel("stop/sdir");
	dirlin5 = get_lin_rel("stop");
	lin1 = get_lin_rel("top/dir/mdir/file1");
	lin2 = get_lin_rel("top/dir/mdir/file2");
	lin3 = get_lin_rel("top/dir/mdir/file3");
	lins = get_lin_rel("stop/sdir/skip");
	lins2 = get_lin_rel("stop/sdir/skip2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(dirlin, 1, true, true, false);
	helper_add_expected(dirlin2, 1, true, true, false);
	helper_add_expected(dirlin3, 1, true, true, false);
	helper_add_expected(dirlin4, 1, true, true, false);
	helper_add_expected(dirlin5, 1, true, true, false);
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lin3, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_add_expected_excl(lins2, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
	rename_rel("stop", "top/dir/mdir/stop2");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_clear_expected();
	helper_add_expected(lins, 1, false, true, false);
	helper_add_expected(lins2, 1, false, true, false);
	helper_add_expected(dirlin, 1, true, false, true);
	helper_add_expected(dirlin5, 1, true, false, true);
	helper_add_expected(dirlin4, 1, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test that a newly created dirent that doesn't match a filename predicate isn't
 * transferred.
 */
TEST(process_path_skipped_send_skipped, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lin3, lins, lins2, dirlin, dirlin2, dirlin3, dirlin4, dirlin5;
	g_pw_ctx.plan = siq_parser_parse("-path '*mdir*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("top");
	mkdir_rel("top/dir");
	mkdir_rel("top/dir/mdir");
	touch_rel("top/dir/mdir/file1");
	touch_rel("top/dir/mdir/file2");
	touch_rel("top/dir/mdir/file3");
	mkdir_rel("stop");
	mkdir_rel("stop/sdir");
	touch_rel("stop/sdir/skip");
	touch_rel("stop/sdir/skip2");
	dirlin = get_lin_rel("top/dir/mdir");
	dirlin2 = get_lin_rel("top/dir");
	dirlin3 = get_lin_rel("top");
	dirlin4 = get_lin_rel("stop/sdir");
	dirlin5 = get_lin_rel("stop");
	lin1 = get_lin_rel("top/dir/mdir/file1");
	lin2 = get_lin_rel("top/dir/mdir/file2");
	lin3 = get_lin_rel("top/dir/mdir/file3");
	lins = get_lin_rel("stop/sdir/skip");
	lins2 = get_lin_rel("stop/sdir/skip2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);

	helper_add_expected(dirlin, 1, true, true, false);
	helper_add_expected(dirlin2, 1, true, true, false);
	helper_add_expected(dirlin3, 1, true, true, false);
	helper_add_expected(dirlin4, 1, true, true, false);
	helper_add_expected(dirlin5, 1, true, true, false);
	helper_add_expected(lin1, 1, false, true, false);
	helper_add_expected(lin2, 1, false, true, false);
	helper_add_expected(lin3, 1, false, true, false);
	helper_add_expected_excl(lins, 1, 1, false, true, false);
	helper_add_expected_excl(lins2, 1, 1, false, true, false);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
	rename_rel("stop", "top/dir/stop2");
	snapid = snapid2;
	snapid2 = take_snapshot(unit_test_root, 2);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);
	while(!process_stf_lins(&g_pw_ctx, false, &error));
	fail_if_isi_error(error);
	do_full_change_compute(&g_pw_ctx, &error);
	fail_if_isi_error(error);
	helper_clear_expected();
	helper_add_expected(dirlin2, 1, true, false, true);
	helper_add_expected(dirlin5, 1, true, false, true);
	helper_add_expected(dirlin4, 1, true, false, true);
	helper_execute_check(g_pw_ctx.chg_ctx->rep2, 1, &error);
	fail_if_isi_error(error);

	flip_lins(g_pw_ctx.chg_ctx, &error);
	fail_if_isi_error(error);
	free_changeset(g_pw_ctx.chg_ctx);
}

TEST(worklist_basic_operations, .attrs="overnight")
{

	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2;
	ifs_lin_t lin = 0;

	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	touch_rel("file1");
	touch_rel("file2");
	lin1 = get_lin_rel("file1");
	lin2 = get_lin_rel("file2");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");
	g_pw_ctx.chg_ctx = create_changeset(repstate_name_root, 
	    snapid, snapid2, false, &error);
	fail_if_isi_error(error);
	g_pw_ctx.cur_rep_ctx = g_pw_ctx.chg_ctx->rep2;
	fail_unless(NULL != g_pw_ctx.chg_ctx);
	set_work_entry(g_pw_ctx.wi_lin, g_pw_ctx.work, NULL, 0, g_pw_ctx.chg_ctx->rep2, &error);
	fail_if_isi_error(error);

	wl_log_add_entry(&g_pw_ctx, 1, &error);
	fail_if_isi_error(error);

	flush_stf_logs(&g_pw_ctx, &error);

	wl_get_entry(&g_pw_ctx, &lin, &error);
	fail_if_isi_error(error);
	
	fail_unless(lin == 1);
	free_changeset(g_pw_ctx.chg_ctx);
}

/**
 * Test to recreate new inode with old lin number
 */
TEST(recreate_lins, .attrs="overnight")
{
	int dfd = -1;
	int ret = -1;
	struct isi_error *error = NULL;
	ifs_snapid_t snapid, snapid2;
	ifs_lin_t linr, lin1, lin2, lin3, dirlin, dirlin2, dirlin3, dirlin4;
	char path[MAXPATHLEN];

	g_pw_ctx.plan = siq_parser_parse("-path '*mdir*'", time(NULL));
	linr = get_lin_path(unit_test_root);
	snapid = take_snapshot(unit_test_root, 0);
	fail_unless(snapid > 0, "Failed to create snapshot");
	create_dummy_begin(repstate_name_root, snapid, linr, &error);
	fail_if_isi_error(error);
	mkdir_rel("top");
	mkdir_rel("top/dir");
	mkdir_rel("top/dir1");
	mkdir_rel("top/dir/mdir");
	touch_rel("top/dir/mdir/file1");
	touch_rel("top/dir/mdir/file2");
	touch_rel("top/dir/mdir/file3");
	dirlin = get_lin_rel("top/dir/mdir");
	dirlin2 = get_lin_rel("top/dir1");
	dirlin3 = get_lin_rel("top");
	lin1 = get_lin_rel("top/dir/mdir/file1");
	lin2 = get_lin_rel("top/dir/mdir/file2");
	lin3 = get_lin_rel("top/dir/mdir/file3");
	snapid2 = take_snapshot(unit_test_root, 1);
	fail_unless(snapid2 > 0, "Failed to create snapshot");

	/* Remove one file and directory */
	unlink_rel("top/dir/mdir/file1");
	rmdir_rel("top/dir1");

	ret = enc_set_proc(ENC_DEFAULT);
	fail_unless(ret == 0);

	/* Now re-create the files and directory */
	sprintf(path, "%s/%s", unit_test_root, "top");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);
	ret = enc_lin_create_linkat(dirlin2, dfd, "dir1",
	    ENC_DEFAULT, S_IFDIR|S_IRWXU|S_IRWXG, 0, NULL, 0);
	fail_unless(ret == 0);
	close(dfd);

	/* Now re-create the files and directory */
	sprintf(path, "%s/%s", unit_test_root, "top/dir/mdir");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);
	ret = enc_lin_create_linkat(lin1, dfd, "file1",
	    ENC_DEFAULT, S_IFREG|S_IRWXU|S_IRWXG, 0, NULL, 0);
	fail_unless(ret == 0);
	close(dfd);

	/* Now make sure that lins before and after creation match */
	dirlin4 = get_lin_rel("top/dir1");
	linr = get_lin_rel("top/dir/mdir/file1");
	fail_unless(dirlin2 == dirlin4);
	fail_unless(lin1 == linr);

	touch_rel("top/dir/mdir/file1");

	/* Again try to create, it should fail */

	sprintf(path, "%s/%s", unit_test_root, "top/dir/mdir");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);
	ret = enc_lin_create_linkat(lin1, dfd, "file1",
	    ENC_DEFAULT, S_IFREG|S_IRWXU, 0, NULL, 0);
	fail_unless(ret == -1 && errno == EEXIST);
	close(dfd);
}
#endif

/**
 * Tests both get_dir_parent() and stat_lin_snap().
 */
TEST(test_stat_lin_snap, .attrs="overnight")
{
	char *pdirname = NULL;
	char dirname[] = "dir";
	char subdirname[] = "subdir";
	char lastdirname[] = "last";
	char path[MAXPATHLEN + 1];
	struct stat pdst = {}, dst = {}, sdst = {}, lst = {}, sls_st = {};
	ifs_snapid_t snapid1, snapid2, snapid3;
	ifs_lin_t plin;
	int pfd = -1, dfd = -1, sdfd = -1;
	bool found;
	struct isi_error *error = NULL;

	pdirname = create_new_dir();
	fail_if(stat(pdirname, &pdst));

	pfd = ifs_lin_open(pdst.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_unless(pfd > 0);

	fail_if(enc_mkdirat(pfd, dirname, ENC_DEFAULT, 0777));
	dfd = enc_openat(pfd, dirname, ENC_DEFAULT, O_RDONLY);
	fail_unless(dfd > 0);
	fail_if(fstat(dfd, &dst));
	fail_unless(snprintf(path, MAXPATHLEN + 1, "%s/%s", pdirname, dirname));

	fail_if(enc_mkdirat(dfd, subdirname, ENC_DEFAULT, 0777));
	sdfd = enc_openat(dfd, subdirname, ENC_DEFAULT, O_RDONLY);
	fail_unless(sdfd > 0);
	fail_if(fstat(sdfd, &sdst));

	fail_if(enc_mkdirat(sdfd, lastdirname, ENC_DEFAULT, 0777));
	fail_if(enc_fstatat(sdfd, lastdirname, ENC_DEFAULT, &lst,
	    AT_SYMLINK_NOFOLLOW));

	snapid1 = ut_take_snapshot(path, &error);
	fail_if(error != NULL,"ut_take_snapshot(): %{}", isi_error_fmt(error));

	/* test when lin exists in snap */
	found = get_dir_parent(lst.st_ino, snapid1, &plin, false, &error);
	fail_unless(found && error == NULL && plin == sdst.st_ino);

	found = stat_lin_snap(lst.st_ino, snapid1, &sls_st, false, &error);
	fail_unless(found && error == NULL && lst.st_ino == sls_st.st_ino);

	fail_if(futimes(sdfd, NULL));
	snapid2 = ut_take_snapshot(path, &error);
	fail_if(error != NULL,"ut_take_snapshot(): %{}", isi_error_fmt(error));

	found = stat_lin_snap(lst.st_ino, snapid2, &sls_st, false, &error);
	fail_unless(found && error == NULL && lst.st_ino == sls_st.st_ino);

	fail_if(enc_renameat(dfd, subdirname, ENC_DEFAULT, pfd, subdirname,
	    ENC_DEFAULT));
	fail_if(close(dfd));
	snapid3 = ut_take_snapshot(path, &error);
	fail_if(error != NULL,"ut_take_snapshot(): %{}", isi_error_fmt(error));

	/* test when list doesn't exist in snapshot */
	found = get_dir_parent(lst.st_ino, snapid3, &plin, true, &error);
	fail_if(found || error != NULL);

	/* test missing_ok = true */
	found = stat_lin_snap(lst.st_ino, snapid3, &sls_st, true, &error);
	fail_if(found || error != NULL);

	/* test missing_ok = false */
	found = stat_lin_snap(lst.st_ino, snapid3, &sls_st, false, &error);
	fail_if(found || error == NULL);
	isi_error_free(error);
	error = NULL;

	fail_if(enc_unlinkat(pfd, dirname, ENC_DEFAULT, AT_REMOVEDIR));
	fail_if(enc_unlinkat(sdfd, lastdirname, ENC_DEFAULT, AT_REMOVEDIR));
	fail_if(close(sdfd));
	fail_if(enc_unlinkat(pfd, subdirname, ENC_DEFAULT, AT_REMOVEDIR));
	fail_if(close(pfd));
	fail_if(rmdir(pdirname));
	ut_rm_snapshot(snapid1, &error);
	fail_if(error != NULL,"ut_rm_snapshot(): %{}", isi_error_fmt(error));
	ut_rm_snapshot(snapid2, &error);
	fail_if(error != NULL,"ut_rm_snapshot(): %{}", isi_error_fmt(error));
	ut_rm_snapshot(snapid3, &error);
	fail_if(error != NULL,"ut_rm_snapshot(): %{}", isi_error_fmt(error));

	clean_dir(pdirname);
	pdirname = NULL;
}
