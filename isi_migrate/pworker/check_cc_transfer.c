#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <isi_util/isi_error.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/isi_macro.h>
#include <isi_migrate/migr/isirep.h>
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
SUITE_DEFINE_FOR_FILE(cc_transfer, .test_setup = test_setup, 
    .test_teardown = test_teardown, .timeout = 60);

char test_root[] = "/ifs/test";
char snap_root[] = "testsnap";
char symlink_path[] = "/ifs/junk/path";
//char symlink_path1[] = "/ifs/junk/path1";
char repname[] = "myrep";
ifs_snapid_t chg_snapid, chg_snapid2;
struct work_restart_state g_work;
struct migr_pworker_ctx pw_ctx = {};

static void
cleanup(void) 
{
	char command[MAXPATHLEN + 50];
	sprintf(command, "%s %s", "rm -rf", test_root);
	system(command);
	
	sprintf(command, "%s %s", "mkdir", test_root);
	system(command);
}

static void
remove_snapshots(void)
{
	struct isi_str snap_name;
	struct isi_error *error = NULL;
	char name[255] = {};
	int i;

	for (i = 0; i < 1024; i++) {
		sprintf(name, "%s_%d", snap_root, i);
		isi_str_init(&snap_name, name, strlen(name) + 1, ENC_DEFAULT,
		    ISI_STR_NO_MALLOC);
		snapshot_destroy(&snap_name, &error);
		if (error) {
			isi_error_free(error);
			error = NULL;
		}
	}
}

static void
test_remove_repstate(void)
{
	char name[256];
	struct isi_error *error = NULL;

	sprintf(name, "%s_snap_rep_base", repname);
	remove_repstate(repname, name, true, &error);
	if (error)
		isi_error_free(error);
	error = NULL;
	
	sprintf(name, "%s_snap_%lld", repname, chg_snapid);
	remove_repstate(repname, name, true, &error);
	if (error)
		isi_error_free(error);
	error = NULL;

	sprintf(name, "%s_snap_%lld", repname, chg_snapid2);
	remove_repstate(repname, name, true, &error);
	if (error)
		isi_error_free(error);
	error = NULL;
}

static void
free_tmp_structures(struct migr_pworker_ctx *pw_ctx)
{
	char *hash_val = NULL;

	if (pw_ctx->stf_rep_iter.iter) {
		free(pw_ctx->stf_rep_iter.iter);
		pw_ctx->stf_rep_iter.iter = NULL;
	}
	if (pw_ctx->chg_ctx)
		free_changeset(pw_ctx->chg_ctx);

	if (pw_ctx->file_state.bb_hash_ctx) {
		hash_val = hash_end(pw_ctx->file_state.bb_hash_ctx);
		free(hash_val);
	}
	if (pw_ctx->file_state.hash_str)
		free(pw_ctx->file_state.hash_str);

	if (pw_ctx->ads_dir_state.dir) {
		migr_dir_free(pw_ctx->ads_dir_state.dir);
	}
	if (pw_ctx->dir_state.dir) {
		migr_dir_free(pw_ctx->dir_state.dir);
	}
	if (pw_ctx->stf_cur_stats)
		siq_stat_free(pw_ctx->stf_cur_stats);
	
	if (pw_ctx->tw_cur_stats)
		siq_stat_free(pw_ctx->tw_cur_stats);
}

TEST_FIXTURE(test_setup)
{
	cleanup();
	remove_snapshots();
	test_remove_repstate();
}

TEST_FIXTURE(test_teardown)
{
	cleanup();
	remove_snapshots();
	test_remove_repstate();
}

static ifs_snapid_t
take_snapshot(char *path, int cnt)
{
	struct isi_str snap_name, *paths[2];
	char name[255];
	ifs_snapid_t snapid = 0;
	struct isi_error *error = NULL;

	sprintf(name, "%s_%d", snap_root, cnt);

	isi_str_init(&snap_name, name, strlen(name) + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	paths[0] = isi_str_alloc(path, strlen(path) + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	paths[1] = NULL;

	snapid = snapshot_create(&snap_name, 0, paths, NULL, &error);
	fail_if_isi_error(error);
	fail_unless(snapid > 0, "Failed to create snapshot %s dir %s", name, path);

	isi_str_free(paths[0]);
	return snapid;
}

static void
touch_entry(char *entry){
	
	char command[MAXPATHLEN + 50];
	sprintf(command, "touch %s/%s",test_root, entry);
	system(command);
}

static void
touch_file(char *entry, struct stat *st)
{
	int ret;
	char command[MAXPATHLEN + 50];
	char path[MAXPATHLEN + 1];


	sprintf(path, "%s/%s",test_root, entry);
	ret = stat(path, st);
	fail_unless(ret == -1);

	sprintf(command, "touch %s/%s",test_root, entry);
	system(command);

	ret = stat(path, st);
	fail_unless(ret == 0);
}

static void
create_dir(char *entry, struct stat *st)
{
	int ret;
	char command[MAXPATHLEN + 50];
	char path[MAXPATHLEN + 1];


	sprintf(path, "%s/%s",test_root, entry);
	ret = stat(path, st);
	fail_unless(ret == -1);

	sprintf(command, "mkdir %s/%s",test_root, entry);
	system(command);

	ret = stat(path, st);
	fail_unless(ret == 0);
}

static void
create_sym(char *entry, struct stat *st)
{
	int ret;
	char command[MAXPATHLEN + 50];
	char path[MAXPATHLEN + 1];

	sprintf(path, "%s/%s",test_root, entry);
	ret = stat(path, st);
	fail_unless(ret == -1);

	sprintf(command, "ln -s  %s %s/%s",symlink_path, test_root, entry);
	system(command);
	
	ret = lstat(path, st);
	fail_unless(ret == 0);
}

static void
create_ads(char *entry)
{
	char path[MAXPATHLEN + 1];
	int parentfd, fd, adsfd;
	sprintf(path, "%s/%s",test_root, entry);
	parentfd = open(path, O_RDONLY);
	fail_unless(parentfd > 0);

	adsfd = enc_openat(parentfd, ".", ENC_DEFAULT, O_RDONLY | O_XATTR);
	fail_unless(adsfd > 0);
	
	fd = enc_openat(adsfd, "test", ENC_DEFAULT, O_CREAT|O_RDWR|O_NOFOLLOW,
		0777);
	if (fd == -1) {
		printf("error is %s\n", strerror(errno));
	}
	fail_unless(fd > 0);

	close(fd);
	close(adsfd);
	close(parentfd);
}

static void
add_data(char *entry)
{

	int size = IDEAL_READ_SIZE  + 1;
	int ret;
	char path[MAXPATHLEN + 1];

	sprintf(path, "%s/%s",test_root, entry);
	ret = truncate(path, size);
	fail_unless(ret == 0);
}

static void 
setup_lin(struct migr_pworker_ctx *pw_ctx,  char *entry,
    unsigned mode, bool has_ads, bool has_data)
{
	struct stat st;
	struct rep_entry rep_entry = {};
	struct isi_error *error = NULL;

	rep_entry.lcount = 1;
	rep_entry.not_on_target = true;
	rep_entry.changed = true;

	switch (mode) {
	case SIQ_FT_REG:
	    touch_file(entry, &st);
	    break;
	    
	case SIQ_FT_DIR:
	    create_dir(entry, &st);
	    rep_entry.is_dir = true;
	    break;
	    
	case SIQ_FT_SYM:
	    create_sym(entry, &st);
	    break;
	    
	default:
	    ASSERT(0);
	}

	chg_snapid = take_snapshot("/ifs", 0);
	if (mode != SIQ_FT_SYM)
		touch_entry(entry);

	if (has_ads) {
		create_ads(entry);
	}

	if (has_data && (mode == SIQ_FT_REG)) {
		add_data(entry);
	}
		
	fail_unless(chg_snapid > 0, "Failed to create snapshot");
	chg_snapid2 = take_snapshot("/ifs", 1);

	memset(pw_ctx, 0, sizeof(struct migr_pworker_ctx));		
	init_pw_ctx(pw_ctx);
	pw_ctx->initial_sync = false;
	pw_ctx->stf_upgrade_sync = false;
	pw_ctx->stf_sync = true;
	pw_ctx->work = &g_work;
	pw_ctx->work->min_lin = WORK_RES_RANGE_MAX + 1;
	pw_ctx->work->max_lin = -1;
	init_statistics(&pw_ctx->stf_cur_stats);
	init_statistics(&pw_ctx->tw_cur_stats);
	
	create_dummy_begin(repname, chg_snapid, 0, &error);
	fail_unless(!error);
	pw_ctx->chg_ctx = create_changeset(repname, chg_snapid,
	    chg_snapid2, false, &error);
	fail_unless(!error);
	pw_ctx->cur_rep_ctx = pw_ctx->chg_ctx->rep2;
	pw_ctx->cur_snap = chg_snapid2;
	pw_ctx->prev_snap = chg_snapid;
	pw_ctx->work->rt = WORK_CT_LIN_UPDTS;
	
	set_entry(st.st_ino, &rep_entry, pw_ctx->chg_ctx->rep2, 0,
	    NULL, &error);
	fail_if(error);
}

TEST(simple_file_transfer, .attrs="overnight") {

	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "file1", SIQ_FT_REG, false, false);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_data);	

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_lin_commit);

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_change_transfer);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}
	
	free_tmp_structures(&pw_ctx);
}

TEST(simple_dir_transfer, .attrs="overnight")
{
	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "dir1", SIQ_FT_DIR, false, false);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_commit);
	
	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_change_transfer);

	free_tmp_structures(&pw_ctx);
}

TEST(simple_symlink_transfer, .attrs="overnight")
{
	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "symlink1", SIQ_FT_SYM, false, false);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_data);	

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_lin_commit);

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_change_transfer);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}
	
	free_tmp_structures(&pw_ctx);

}

TEST(file_transfer_with_ads, .attrs="overnight") {

	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "file1", SIQ_FT_REG, true, false);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_ads);	

	free_tmp_structures(&pw_ctx);
}

TEST(dir_transfer_with_ads, .attrs="overnight") {

	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "file1", SIQ_FT_DIR, true, false);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_ads);	

	free_tmp_structures(&pw_ctx);
}

TEST(file_with_large_data, .attrs="overnight") {


	struct isi_error *error = NULL;

	setup_lin(&pw_ctx, "file1", SIQ_FT_REG, false, true);

	stf_continue_change_transfer(&pw_ctx, &error);

	if (error) {
		printf("error is %s\n", isi_error_get_message(error));
	}

	fail_unless(pw_ctx.next_func == stf_continue_lin_data);	

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_lin_data);	

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_lin_data);

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_lin_commit);	

	(*pw_ctx.next_func)(&pw_ctx, &error);
	fail_unless(pw_ctx.next_func == stf_continue_change_transfer);

	free_tmp_structures(&pw_ctx);
}

