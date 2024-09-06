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
#include <isi_util/isi_error.h>
#include <check.h>
#include <sys/isi_enc.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_str.h>
#include "pworker.h"
#include "test_helper.h"
#include <isi_snapshot/snapshot.h>
#include <isi_util/util_adt.h>


#include <isi_util/check_helper.h>
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

#define CC_TEST_TIMEOUT 240

SUITE_DEFINE_FOR_FILE(cc_restore, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout = CC_TEST_TIMEOUT);


int MAX_SNAPSHOTS=90;
static char pre_repstate_name_root[] = "unittest_repstate_check_cc";
static char pre_unit_test_root[] = "/ifs/unittest/check_cc.XXXXXX";

char *unit_test_root;
char *repstate_name_root;
struct migr_pworker_ctx g_pw_ctx;
struct work_restart_state g_work;
static struct uint64_uint64_map expected;

#define CHECK_BIT_DIR (1ull<<32)
#define CHECK_BIT_NEW (1ull<<33)
#define CHECK_BIT_CHANGED (1ull<<34)

#define POLICYID "TEST_restore_chkpt_less_than_min"
#define REPNAME "TEST"


static void
helper_clear_expected(void)
{
	uint64_uint64_map_clean(&expected);
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
create_large_file(const char *rel, int num_megabytes)
{
	char command[MAXPATHLEN + 100];

	sprintf(command, "dd if=/dev/zero of=%s/%s bs=1M count=%d",
	    unit_test_root, rel, num_megabytes);
	system(command);
}

static void
unlink_rel(const char *rel)
{
	char path[MAXPATHLEN];
	sprintf(path, "%s/%s", unit_test_root, rel);
	unlink(path);
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
	fail_unless(unit_test_root != NULL);
	repstate_name_root = malloc(255);
	sprintf(repstate_name_root, "%s%s", pre_repstate_name_root,
	    strchr(unit_test_root, '.') + 1);

	sprintf(command, "mkdir -p %s", unit_test_root);
	system(command);
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

	for (i = 0; i < MAX_SNAPSHOTS; i++) {
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
	system(command);
	if (g_pw_ctx.plan)
		siq_plan_free(g_pw_ctx.plan);
	if (g_pw_ctx.stf_cur_stats) {
		siq_stat_free(g_pw_ctx.stf_cur_stats);
		g_pw_ctx.stf_cur_stats = NULL;
	}
	free(repstate_name_root);
	helper_clear_expected();

	remove_repstate(POLICYID, REPNAME, true, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
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
	ifs_lin_t lin4;
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
	create_large_file("top/dir/mdir/file1", 1024);
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

	/* Try to re-create a file which has no snapshots */
	touch_rel("top/dir/mdir/file4");
	lin4 = get_lin_rel("top/dir/mdir/file4");
	unlink_rel("top/dir/mdir/file4");
	sprintf(path, "%s/%s", unit_test_root, "top/dir/mdir");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);

	ret = enc_lin_create_linkat(lin4, dfd, "file4",
	    ENC_DEFAULT, S_IFREG|S_IRWXU|S_IRWXG, 0, NULL, 0);
	fail_unless(ret == -1 && errno == ENOENT);
	close(dfd);

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

	sprintf(path, "%s/%s", unit_test_root, "top");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);
	ret = enc_lin_create_linkat(lin1, dfd, "dir1",
	    ENC_DEFAULT, S_IFDIR|S_IRWXU|S_IRWXG, 0, NULL, 0);
	fail_unless(ret == -1 && errno == EEXIST);
	close(dfd);

	sprintf(path, "%s/%s", unit_test_root, "top/dir/mdir");
	dfd = open(path, O_RDONLY);
	fail_unless(dfd > 0);
	ret = enc_lin_create_linkat(lin1, dfd, "file1",
	    ENC_DEFAULT, S_IFREG|S_IRWXU, 0, NULL, 0);
	fail_unless(ret == -1 && errno == EEXIST);
	close(dfd);
}

TEST(restore_chkpt_less_than_min, .attrs="overnight")
{
	struct migr_pworker_ctx pw_ctx = {};
	struct work_restart_state work_item = {};
	struct generic_msg msg = {};
	struct rep_ctx *ctx = NULL;
	struct isi_error *error = NULL;

	//Create a dummy repstate
	create_repstate(POLICYID, REPNAME, true, &error);
	fail_if_isi_error(error);
	open_repstate(POLICYID, REPNAME, &ctx, &error);
	fail_if_isi_error(error);

	//Put a known weird work item into repstate
	work_item.rt = WORK_CT_DIR_LINKS;
	work_item.dircookie1 = 0x3e8;
	work_item.dircookie2 = 0x3e8;
	work_item.min_lin = 0x1002f64c2;
	work_item.max_lin = 0x1002f64f5;
	set_work_entry(9000, &work_item, NULL, 0, ctx, &error);
	fail_if_isi_error(error);

	//Now try to read it
	pw_ctx.policy_id = strdup(POLICYID);
	msg.body.work_resp3.wi_lin = 9000;
	read_work_item(&pw_ctx, &msg, ctx, &error);
	fail_if_isi_error(error);

	//Verify the work item
	fail_unless(pw_ctx.work->chkpt_lin == 0x1002f64c2);

	//Clean up
	close_repstate(ctx);
	free(pw_ctx.policy_id);
	free(pw_ctx.work);
}
