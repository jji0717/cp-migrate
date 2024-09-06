#include <check.h>
#include <isi_migrate/migr/check_helper.h>
#include <sys/extattr.h>
#include <sys/isi_enc.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>
#include "sworker.h"

struct migr_sworker_ctx sw_ctx;

static char policy_id[POLICY_ID_LEN + 1];

#define fail_if_isi_error(err) \
do {\
        if (err) {\
                struct fmt FMT_INIT_CLEAN(fmt);\
                fmt_print(&fmt, "isi_error: %{}", isi_error_fmt(err));\
                fail_unless(!err, "%s", fmt_string(&fmt));\
        }\
} while(0)


TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(utils, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout = 60);

TEST_FIXTURE(test_setup)
{
	char *pid = NULL;
	// Empty for now
	memset(&sw_ctx, 0, sizeof(struct migr_sworker_ctx));
	init_statistics(&sw_ctx.stats.stf_cur_stats);
	pid = siq_create_policy_id(false);
	fail_if(pid == NULL);
	strlcpy(policy_id, pid, sizeof(policy_id));
	free(pid);
	pid = NULL;
	sw_ctx.tmonitor.policy_id = policy_id;
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.tmonitor.coord_fd = -1;
}

TEST_FIXTURE(test_teardown)
{
	// Empty for now
	if (sw_ctx.stats.stf_cur_stats) {
		siq_stat_free(sw_ctx.stats.stf_cur_stats);
		sw_ctx.stats.stf_cur_stats = NULL;
	}
}

TEST(test_move_dirents, .attrs="overnight")
{

	char *dirname1;
	char *dirname2;
	char *tmpdir;
	char *file1 = "testfile1";
	char *file2 = "testfile2";
	int ret;
	struct stat st;
	uint64_t file1_lin;
	struct isi_error *error = NULL;
	char path[MAXPATHLEN + 1];
	char linkpath[MAXPATHLEN + 1];
	int dirfd, dirfd2, tmpfd;
	int dir_unlinks, file_unlinks;

	dirname1 = setup_layered_dir(5,5);
	fail_if (dirname1 == NULL);
	chmod(dirname1, 0700);

	dirname2 = setup_simple_dir(10);
	fail_if (dirname2 == NULL);
	chmod(dirname2, 0700);

	tmpdir = setup_simple_dir(10);
	fail_if (tmpdir == NULL);
	chmod(tmpdir, 0700);

	create_file(dirname1, file1);
	snprintf(path, sizeof(path), "%s/%s", dirname1, file1);
	//printf("\n dir  is %s \n",dirname1);
	//printf("\n dir  is %s \n",dirname2);
	//printf("\n dir  is %s \n",tmpdir);
	//mkdir(path, 0700);
	stat(path, &st);
	file1_lin = st.st_ino;

	dirfd = open(dirname1, O_RDONLY);
	fail_unless(dirfd > 0);

	dirfd2 = open(dirname2, O_RDONLY);
	fail_unless(dirfd2 > 0);

	sw_ctx.global.enable_hash_tmpdir = true;
	tmpfd = initialize_tmp_map(tmpdir, sw_ctx.tmonitor.policy_id,
	    &sw_ctx.global, false, true, &error);
	fail_if_error(error, "initialize_tmp_map");
	close(tmpfd);
	tmpfd = -1;

	/* Simple move all dirents */
	move_dirents(&sw_ctx, dirfd, &dir_unlinks, &file_unlinks, &error);
	if (error) {
		printf("Error is %s\n", isi_error_get_message(error));
	}
	fail_if_error(error, "move_dirents");

	tmpfd = get_tmp_fd(file1_lin,  &sw_ctx.global, &error);
	fail_if_error(error, "get_tmp_fd");

	snprintf(path, sizeof(path), "%llx", file1_lin);
	ret = enc_fstatat(tmpfd, path, ENC_DEFAULT, &st, AT_SYMLINK_NOFOLLOW);
	fail_unless(ret == 0);
	close(tmpfd);
	tmpfd = -1;

	create_file(dirname1, file2);
	snprintf(path, sizeof(path), "%s/%s", dirname1, file2);
	snprintf(linkpath, sizeof(linkpath), "%s/%s", dirname2, file2);

	link(path, linkpath);

	stat(path, &st);
	fail_unless(st.st_nlink == 2);

	/* the file2 should not be present in tmpdir */
	move_dirents(&sw_ctx, dirfd2, &dir_unlinks, &file_unlinks, &error);
	fail_if_error(error, "move_dirents");

	tmpfd = get_tmp_fd(st.st_ino,  &sw_ctx.global, &error);
	fail_if_error(error, "get_tmp_fd");

	snprintf(path, sizeof(path), "%llx", st.st_ino);
	ret = enc_fstatat(tmpfd, path, ENC_DEFAULT, &st, AT_SYMLINK_NOFOLLOW);
	fail_unless(ret == -1);
	close(tmpfd);
	tmpfd = -1;
	cleanup_tmp_dirs(tmpdir, false, &sw_ctx, &error);
	fail_if_error(error, "cleanup_tmp_dirs");
	close(dirfd);
	close(dirfd2);
	clean_dir(dirname1);
	clean_dir(dirname2);
	clean_dir(tmpdir);

	if (error)
		isi_error_free(error);

}

TEST(test_hash_tmpdir_children, .attrs="overnight")
{
	struct isi_error *error = NULL;
	ifs_lin_t tmp_lin, ptmp_lin, i, mask = (TMPDIR_MASK) + 1;
	char *tmpdir = NULL;
	int tmpfd = -1;

	tmpdir = setup_simple_dir(10);
	fail_if(tmpdir == NULL);

	sw_ctx.global.enable_hash_tmpdir = true;
	tmpfd = initialize_tmp_map(tmpdir, sw_ctx.tmonitor.policy_id,
	    &sw_ctx.global, false, true, &error);
	fail_if_error(error, "initialize_tmp_map");

	for (i = 0; i < mask; i++) {
		tmp_lin = get_tmp_lin(i, &sw_ctx.global, &ptmp_lin, &error);
		fail_if_error(error, "get_tmp_lin");
		fail_if(tmp_lin == INVALID_LIN);
		fail_if(tmp_lin == ptmp_lin);
		fail_if(ptmp_lin == INVALID_LIN);
	}
	close(tmpfd);
	tmpfd = -1;

	cleanup_tmp_dirs(tmpdir, false, &sw_ctx, &error);
	fail_if_error(error, "cleanup_tmp_dirs");

	clean_dir(tmpdir);
}
