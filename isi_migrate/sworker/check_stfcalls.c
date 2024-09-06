
#include <isi_migrate/migr/check_helper.h>
#include <isi_migrate/migr/selectors.h>
#include <ifs/ifs_lin_open.h>
#include <isi_snapshot/snapshot.h>
#include "sworker.h"

#include <libgen.h>
#include <unistd.h>

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
SUITE_DEFINE_FOR_FILE(migr_stfcalls, .timeout = 60,
    .suite_setup = suite_setup, .suite_teardown = suite_teardown);

TEST_FIXTURE(suite_setup)
{
}

TEST_FIXTURE(suite_teardown)
{
}

#define TMPDIR "tmpdir"
#define SUBDIR "subdir"
TEST(test_move_or_remove_dirent, .attrs="overnight")
{
	struct migr_sworker_ctx sw_ctx = {};
	struct stat st;
	struct isi_error *error = NULL;
	char *rootdir = NULL, *tmppath = NULL;
	int rootfd, tmpfd, dirfd;

	bzero(&sw_ctx, sizeof(struct migr_sworker_ctx));
	/* run the test with tmpdir hashing enabled */
	sw_ctx.global.enable_hash_tmpdir = true;
	rootdir = setup_simple_dir(10);
	fail_if(rootdir == NULL);
	rootfd = open(rootdir, O_RDONLY);
	fail_if(rootfd <= 0);

	fail_if(enc_mkdirat(rootfd, TMPDIR, ENC_DEFAULT, 0700), "%s",
	    strerror(errno));
	fail_if(enc_mkdirat(rootfd, SUBDIR, ENC_DEFAULT, 0700), "%s",
	    strerror(errno));
	printf("rootdir: %s\n", rootdir);
	tmpfd = enc_openat(rootfd, TMPDIR, ENC_DEFAULT , O_RDONLY);
	fail_if(tmpfd <= 0, "%s", strerror(errno));
	dirfd = enc_openat(rootfd, SUBDIR, ENC_DEFAULT , O_RDONLY);
	fail_if(dirfd <= 0, "%s", strerror(errno));

	fail_if(fstat(dirfd, &st));
	fail_if(fchflags(dirfd,
	    (st.st_flags | IMMUT_APP_FLAG_MASK) & SF_SETTABLE));
	fail_if(fstat(dirfd, &st));
	close(dirfd);
	/* call move_or_remove_dirent() with immutable directory */
	fail_unless(move_or_remove_dirent(&sw_ctx, rootfd, tmpfd, &st, SUBDIR,
	    ENC_DEFAULT, &error), "%s", strerror(isi_error_to_int(error)));
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
	close(tmpfd);
	/* delete tmpdir */
	fail_unless(asprintf(&tmppath, "%s/%s", rootdir, TMPDIR));
	/* clean_dir frees the pointer */
	clean_dir(tmppath);
	tmppath = NULL;

	/* test that ENOENT from clear flags is caught */
	fail_if(move_or_remove_dirent(&sw_ctx, -1, rootfd, &st, SUBDIR,
	    ENC_DEFAULT, &error), "%s", strerror(isi_error_to_int(error)));
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
	close(rootfd);
	clean_dir(rootdir);
	rootdir = NULL;
}

/**
 * Bug 205081
 * Check that delete() exits if EROFS results from a domain generation
 * change
 */
TEST(test_delete_lin_dom_change, .attrs="overnight")
{
	struct migr_sworker_ctx sw_ctx = {};
	struct isi_error *error = NULL;
	struct stat dst = {}, st = {};
	ifs_domainid_t domid = 0;
	uint32_t genid = 0, new_genid = 0;
	char *dirname = NULL, *fname = "dom_change";
	int ret, dfd, fd, status;
	pid_t pid = 0;

	dirname = create_new_dir();
	fail_if_err(stat(dirname, &dst));

	dfd = ifs_lin_open(dst.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(dfd == -1);

	fail_if_err(fstat(dfd, &dst));
	fail_if_err(dom_make_domain(dst.st_ino, &domid, &genid));
	fail_if_err(ifs_domain_add_bylin(dst.st_ino, domid));
	fail_if_err(dom_set_siq(domid));
	fail_if_err(dom_set_readonly(domid, true));
	fail_if_err(dom_mark_ready(domid));
	fail_if_err(ifs_domain_allowwrite(domid, genid));

	memset(&sw_ctx, 0, sizeof(struct migr_sworker_ctx));
	init_statistics(&sw_ctx.stats.stf_cur_stats);
	sw_ctx.global.stf_sync = true;
	sw_ctx.global.domain_id = domid;
	sw_ctx.global.domain_generation = genid;
	sw_ctx.worker.cwd_fd = dfd;
	path_init(&sw_ctx.worker.cur_path, dirname, NULL);

	fd = enc_openat(dfd, fname, ENC_DEFAULT, O_CREAT | O_RDWR, 0600);
	fail_if(fd == -1);
	fail_if_err(fstat(fd, &st));
	close(fd);
	fd = -1;

	/* increment domain id */
	fail_if_err(dom_inc_generation(domid, &new_genid));

	/* using fork to check for exit */
	pid = fork();
	fail_if(pid < 0);
	if (pid == 0) {
		delete(fname, ENC_DEFAULT, &sw_ctx, &error);
		exit(EXIT_FAILURE);
	}
	ret = wait(&status);
	fail_unless(ret == pid);
	fail_unless(WIFEXITED(status));
	fail_unless(WEXITSTATUS(status) == 0);

	/* check delete after ifs_domain_allowwrite */
	fail_if_err(ifs_domain_allowwrite(domid, new_genid));
	delete(fname, ENC_DEFAULT, &sw_ctx, &error);

	fail_if_error(error);
	close(dfd);
	dfd = -1;
	fail_if_err(dom_set_readonly(domid, false));
	clean_dir(dirname);
	siq_stat_free(sw_ctx.stats.stf_cur_stats);
	path_clean(&sw_ctx.worker.cur_path);
}

TEST(test_move_to_tmpdir_error, .attrs="overnight")
{
	struct migr_sworker_ctx sw_ctx = {};
	struct stat st;
	struct isi_error *error = NULL;
	char *rootdir = NULL, *tmppath = NULL;
	int rootfd, tmpfd, dirfd;
	struct fmt FMT_INIT_CLEAN(fmt);
	char *dir = NULL;

	bzero(&sw_ctx, sizeof(struct migr_sworker_ctx));
	/* run the test with tmpdir hashing enabled */
	sw_ctx.global.enable_hash_tmpdir = true;
	rootdir = setup_simple_dir(10);
	fail_if(rootdir == NULL);
	rootfd = open(rootdir, O_RDONLY);
	fail_if(rootfd <= 0);

	fail_if(enc_mkdirat(rootfd, TMPDIR, ENC_DEFAULT, 0700), "%s",
	    strerror(errno));
	fail_if(enc_mkdirat(rootfd, SUBDIR, ENC_DEFAULT, 0700), "%s",
	    strerror(errno));

	tmpfd = enc_openat(rootfd, TMPDIR, ENC_DEFAULT , O_RDONLY);
	fail_if(tmpfd <= 0, "%s", strerror(errno));
	dirfd = enc_openat(rootfd, SUBDIR, ENC_DEFAULT , O_RDONLY);
	fail_if(dirfd <= 0, "%s", strerror(errno));

	fail_if(fstat(dirfd, &st));
	close(dirfd);

	/* to get ENOTEMPTY from move_to_tmpdir */
	fmt_print(&fmt, "%s/%s/%lx", rootdir, TMPDIR, st.st_ino);
	fail_if(mkdir(fmt_string(&fmt), 0777));
	dir = strdup(fmt_string(&fmt));
	fmt_clean(&fmt);
	create_file(dir, "file.txt");

	fail_if(move_or_remove_dirent(&sw_ctx, rootfd, tmpfd, &st, SUBDIR,
	    ENC_DEFAULT, &error));
	fail_unless(errno == ENOTEMPTY);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	clean_dir(dir);
	dir = NULL;
	close(tmpfd);
	/* delete tmpdir */
	fail_unless(asprintf(&tmppath, "%s/%s", rootdir, TMPDIR));
	/* clean_dir frees the pointer */
	clean_dir(tmppath);
	tmppath = NULL;

	close(rootfd);
	clean_dir(rootdir);
	rootdir = NULL;
}

TEST(test_reuse_old_lin_collision, .attrs="overnight")
{
	char *dir_path = NULL;
	bool restore = true;
	struct migr_sworker_ctx sw_ctx = {};
	struct link_msg tlink_msg = {};
	struct isi_error *error = NULL;
	ifs_domainid_t domid = 0;
	ifs_snapid_t snapid;
	struct isi_str *snap_name = NULL, *paths[2] = {NULL, NULL};
	char *name_str = "reuse_old_lin_test";
	uint32_t genid;
	char lmap_name[MAXNAMELEN + 1] = {};
	char *from = "dir";
	char to[MAXNAMELEN + 1] = {};
	int dir_fd = -1;
	int tmp_fd = -1;
	struct stat dir_st = {};
	struct stat st = {};
	char *policy_id = NULL;
	ifs_lin_t tmp_lin = INVALID_LIN;

	policy_id = siq_create_policy_id(false);
	fail_if(policy_id == NULL);

	init_statistics(&sw_ctx.stats.stf_cur_stats);
	dir_path = create_new_dir();
	fail_if(dir_path == NULL);
	fail_if_err(stat(dir_path, &dir_st));

	snap_name = isi_str_alloc(name_str, (strlen(name_str) + 1),
	    ENC_UTF8, ISI_STR_NO_MALLOC);
	fail_if(snap_name == NULL, "Cannot alloc string %s: %s",
	    name_str, strerror(errno));

	paths[0] = isi_str_alloc(dir_path, sizeof(dir_path), ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	fail_if(paths[0] == NULL, "Cannot alloc string for path: %s",
	    strerror(errno));

	fail_if_err(dom_make_domain(dir_st.st_ino, &domid, &genid));
	fail_if_err(ifs_domain_add_bylin(dir_st.st_ino, domid));
	fail_if_err(dom_set_siq(domid));
	fail_if_err(dom_mark_ready(domid));

	sw_ctx.global.domain_id = domid;
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.restore = restore;

	get_lmap_name(lmap_name, policy_id, restore);
	create_linmap(policy_id, lmap_name, true, &error);
	fail_if_error(error);
	open_linmap(policy_id, lmap_name, &sw_ctx.global.lmap_ctx, &error);
	fail_if_error(error);
	set_dynamic_lmap_lookup(sw_ctx.global.lmap_ctx, domid);

	dir_fd = ifs_lin_open(dir_st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(dir_fd == -1);
	fail_if_err(enc_mkdirat(dir_fd, from, ENC_DEFAULT, 0755));

	fail_if_err(enc_fstatat(dir_fd, from, ENC_DEFAULT, &st, 0));

	snprintf(to, sizeof(to), ".slin-%llx", tlink_msg.d_lin);
	tlink_msg.d_name = to;

	snapshot_destroy(snap_name, NULL);
	snapid = snapshot_create(snap_name, (time_t)0, paths, NULL, &error);
	fail_if_error(error);

	fail_if_err(enc_unlinkat(dir_fd, from, ENC_DEFAULT, AT_REMOVEDIR));

	ifs_sync_blocking();

	sw_ctx.global.enable_hash_tmpdir = true;
	tmp_fd = initialize_tmp_map(dir_path, policy_id,
	    &sw_ctx.global, restore, true, &error);
	fail_if_error(error);
	fail_if(tmp_fd == -1);
	fail_if_err(close(tmp_fd));
	tmp_fd = -1;

	tlink_msg.d_type = DT_DIR;
	tlink_msg.d_enc = ENC_DEFAULT;
	tmp_lin = get_tmp_lin(ROOT_LIN, &sw_ctx.global, NULL, &error);
	fail_if_error(error);

	tlink_msg.dirlin = tmp_lin;
	tlink_msg.d_lin = st.st_ino;

	reuse_old_lin(&sw_ctx, &tlink_msg, &error);
	fail_if_error(error);

	/* Simulate two separate create in tmp requests. */
	reuse_old_lin(&sw_ctx, &tlink_msg, &error);
	fail_if_error(error);

	/*
	 * Simulate create tmp requests winning race against correct parent.
	 * Confirm that lin exists in correct parent after operation.
	 */
	tlink_msg.dirlin = dir_st.st_ino;
	tlink_msg.d_name = from;
	reuse_old_lin(&sw_ctx, &tlink_msg, &error);
	fail_if_error(error);

	fail_if_err(enc_fstatat(dir_fd, from, ENC_DEFAULT, &st, 0));
	fail_unless(tlink_msg.d_lin == st.st_ino);

	/*
	 * Simulate create in correct parent wins race againt tmp.
	 * Confirm that lin exists in correct parent after operation.
	 */

	tlink_msg.dirlin = tmp_lin;
	tlink_msg.d_name = to;
	reuse_old_lin(&sw_ctx, &tlink_msg, &error);
	fail_if_error(error);

	fail_if_err(enc_fstatat(dir_fd, from, ENC_DEFAULT, &st, 0));
	fail_unless(tlink_msg.d_lin == st.st_ino);

	snapshot_destroy(snap_name, NULL);
	isi_str_free(paths[0]);
	isi_str_free(snap_name);

	fail_if_err(close(dir_fd));
	dir_fd = -1;
	close_linmap(sw_ctx.global.lmap_ctx);
	remove_linmap(policy_id, lmap_name, true, &error);
	fail_if_error(error);
	siq_stat_free(sw_ctx.stats.stf_cur_stats);
	sw_ctx.tmonitor.coord_fd = SOCK_LOCAL_OPERATION;
	cleanup_tmp_dirs(dir_path, restore, &sw_ctx, &error);
	fail_if_error(error);
	clean_dir(dir_path);
	free(policy_id);
}
