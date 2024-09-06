
#include <isi_migrate/migr/check_helper.h>
#include <isi_quota/check_quota_helper.h>
#include <ifs/ifs_lin_open.h>
#include <isi_ufp/isi_ufp.h>
#include "sworker.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(migr_tree_mv,
    .test_setup = test_setup, .test_teardown = test_teardown,
    .suite_setup = suite_setup);

/* Test Globals */
static char *testdir = NULL;
static char srcdir_path[MAXPATHLEN + 1];
static char srcdir_name[MAXNAMLEN + 1];
static char tgtdir_path[MAXPATHLEN + 1];
static char tgtdir_name[MAXNAMLEN + 1];
static char *file1 = "testfile1";
static char *file2 = "testfile2";
static char *dir1 = "testdir_1";
static char *dir2 = "testdir_2";
static char *dir3 = "testdir_3";
static char *dir4 = "testdir_4";
static int testfd = -1;
static int srcfd = -1;
static int tgtfd = -1;
static struct stat testst;
static struct stat srcst;
static struct stat tgtst;
static char path[MAXPATHLEN + 1];
static char path2[MAXPATHLEN + 1];
static ifs_lin_t bad_lin = 0x99999999;

/*
 * fd is the parent fd for the new file
 * enc is the desired encoding
 */
inline static char *
create_new_dir_with_enc(int fd, int enc)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int i, ret;

	for (i = 0;; i++) {
		fmt_print(&fmt, "test.siq.enc.%04d", i);
		ret = enc_mkdirat(fd, fmt_string(&fmt), enc, 0777);
		if (ret == 0)
		    break;
		else if (errno != EEXIST)
			fail_unless(errno == EEXIST, "errno = %d", errno);
		fmt_clean(&fmt);
	}

	return strdup(fmt_string(&fmt));
}

/* checks for any temporary directories made by a treemv.
 * Removes them if found, and also deletes them when found */
inline static bool
clean_mvtmp_in_dir(char *testdir_path)
{
	char *tmp_path = NULL;
	bool found_tmp = false;
	DIR *testdir = NULL;
	struct dirent *d = NULL;
	testdir = opendir(testdir_path);
	do {
		d = readdir(testdir);
		if (d && 0 == strncmp(d->d_name, TW_MV_TMPNAME, 6)) {
			sprintf(path, "%s/%s", testdir_path, d->d_name);
			found_tmp = true;
			tmp_path = strdup(path);
			clean_dir(tmp_path); /* frees tmp_path */
		}
	} while (d != NULL);

	if (testdir)
		closedir(testdir);

	return found_tmp;
}

/* creates an ads file */
inline static void
create_ads(const char *container, const char* stream)
{
	int fd, ads_fd, flags;
	struct stat st;

	fail_if(stat(container, &st) < 0, "stat %s:%s: %s", container, stream,
	    strerror(errno));
	if (S_ISDIR(st.st_mode))
		flags = O_RDONLY;
	else
		flags = O_RDWR;

	fd = open(container, flags);
	fail_if(fd < 0, "open %s: %s", container, strerror(errno));
	ads_fd = enc_openat(fd, stream, ENC_DEFAULT,
	    O_CREAT | O_RDWR | O_XATTR, 0775);
	fail_if(ads_fd < 0, "enc_openat %s: %s", stream, strerror(errno));
	close(ads_fd);
	close(fd);
}

inline static void
create_and_setup_quota(struct quota_domain *domain, struct quota_domain_id *qdi)
{
	SETUP_DOMAIN(domain, qdi);
	GET_DOMAIN(domain, qdi);
}

TEST_FIXTURE(suite_setup)
{
    /* BEJ: XXX: ufp's are memleaking 40 bytes, disable them to stop leaks. */
    UFAIL_POINT_DISABLE();
}

TEST_FIXTURE(test_setup)
{
	int tmp_srcfd = -1, tmp_tgtfd = -1, tmp_testfd = -1;
	int ret = -1;

	/* check if we are starting from a good spot */
	fail_unless(srcfd == -1, "Bad Initial Setup");
	fail_unless(tgtfd == -1, "Bad Initial Setup");
	fail_unless(testdir == NULL, "Bad Inital Setup");
	fail_unless(testfd == -1, "Bad Initial Setup");

	/* create main test dir, the parent of src and tgt dirs */
	testdir = create_new_dir();
	fail_unless(testdir != NULL);
	tmp_testfd = open(testdir, O_RDONLY);
	fail_unless(tmp_testfd > 0, "tmp_testfd open fail, errno: %d", errno);
	ret = fstat(tmp_testfd, &testst);
	fail_unless(ret == 0);
	testfd = ifs_lin_open(testst.st_ino, HEAD_SNAPID, O_RDWR);
	fail_if(testfd == -1);
	close(tmp_testfd);

	/* setup src dir */
	sprintf(srcdir_name, "source_directory");
	sprintf(srcdir_path, "%s/%s", testdir, srcdir_name);
	mkdir(srcdir_path, 0755);
	tmp_srcfd = open(srcdir_path, O_RDONLY);
	fail_unless(tmp_srcfd > 0, "tmp_srcfd open fail, errno: %d", errno);
	ret = fstat(tmp_srcfd, &srcst);
	fail_unless(ret == 0);
	srcfd = ifs_lin_open(srcst.st_ino, HEAD_SNAPID, O_RDWR);
	fail_if(srcfd == -1);
	close(tmp_srcfd);

	/* setup tgt dir */
	sprintf(tgtdir_name, "target_directory");
	sprintf(tgtdir_path, "%s/%s", testdir, tgtdir_name);
	mkdir(tgtdir_path, 0755);
	tmp_tgtfd = open(tgtdir_path, O_RDONLY);
	fail_unless(tmp_tgtfd > 0, "tmp_tgtfd open fail, errno: %d", errno);
	ret = fstat(tmp_tgtfd, &tgtst);
	fail_unless(ret == 0);
	tgtfd = ifs_lin_open(tgtst.st_ino, HEAD_SNAPID, O_RDWR);
	fail_if(tgtfd == -1);
	close(tmp_tgtfd);

}

TEST_FIXTURE(test_teardown)
{
	/* Cleanup */
	if (srcfd > 0) {
		close(srcfd);
		srcfd = -1;
	}
	if (tgtfd > 0) {
		close(tgtfd);
		tgtfd = -1;
	}
	if (testfd > 0) {
		close(testfd);
		testfd = -1;
	}

	/* extra sure to nuke spare test dirs */
	clean_mvtmp_in_dir(testdir);

	/* clean up the test dir */
	clean_dir(testdir);
	testdir = NULL;
}

/****************************TREE MOVE TESTS******************************/

TEST(test_mv_f, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 */
	create_file(srcdir_path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File1 */
	sprintf(path, "%s/%s", srcdir_path, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s", tgtdir_path, srcdir_name, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_f_ads, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 */
	create_file(srcdir_path, file1);

	/* Create ADS file in File1 */
	sprintf(path, "%s/%s", srcdir_path, file1);
	create_ads(path, "ads_stream");

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File1 */
	sprintf(path, "%s/%s", srcdir_path, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s", tgtdir_path, srcdir_name, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_d, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s", tgtdir_path, srcdir_name, dir1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_df, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File2 in Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);
	create_file(path, file2);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_dfs, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Create File1 in Dir1 */
	create_file(path, file1);

	/* Create File2 in Dir1 */
	create_file(path, file2);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File1 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	/* Validate File2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_f_and_df, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 */
	create_file(srcdir_path, file1);

	/* Create File2 in Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);
	create_file(path, file2);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File1 */
	sprintf(path, "%s/%s", srcdir_path, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s", tgtdir_path, srcdir_name, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	/* Validate File2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, file2);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_dddd, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Create Dir2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, dir2);
	mkdir(path, 0755);

	/* Create Dir3 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, dir3);
	mkdir(path, 0755);

	/* Create Dir4 */
	sprintf(path, "%s/%s/%s/%s/%s", srcdir_path, dir1, dir2, dir3, dir4);
	mkdir(path, 0755);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s/%s", srcdir_path, dir1, dir2, dir3, dir4);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s/%s", tgtdir_path, srcdir_name,
		dir1,dir2,dir3,dir4);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_ddf, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Create Dir2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, dir2);
	mkdir(path, 0755);

	/* Create File1 */
	create_file(path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_f_collide, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create collision dir/File1 on tgt */
	sprintf(path, "%s/%s", tgtdir_path, srcdir_name);
	mkdir(path, 0777);
	create_file(path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_no_src, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     bad_lin,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_no_tgt, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     bad_lin,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	clean_mvtmp_in_dir(testdir);
}

TEST(test_mv_no_parent, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv(&sw_ctx, bad_lin, bad_lin,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_bad_name, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     "",
		     "",
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name + 500,
		     srcdir_name + 500,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_bad_enc, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_ISO_8859_15,	/* aka Latin-9 */
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_f_diff_enc, .attrs="overnight")
{
	int tmpfd = -1;
	int tgtencfd = -1;
	char *tgtencdir = NULL;
	struct isi_error *error = NULL;
	struct stat tgtencst;
	char *testtmpdir = NULL;
	int ret = -1;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* setup tgt dir with different encoding */
	testtmpdir = create_new_dir();
	fail_unless(testtmpdir != NULL);
	tgtencdir = create_new_dir_with_enc(testfd, ENC_ISO_8859_15);
	fail_unless(tgtencdir != NULL);
	sprintf(path, "%s/%s", testdir, tgtencdir);
	ret = stat(path, &tgtencst);
	fail_unless(ret != -1, "failed to stat %s", path);
	tgtencfd = ifs_lin_open(tgtencst.st_ino, HEAD_SNAPID, O_RDWR);
	fail_unless(tgtencfd > 0, "tgtencfd open fail, errno: %d", errno);

	/* Create File1 */
	create_file(srcdir_path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtencfd,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate File1 */
	sprintf(path, "%s/%s", srcdir_path, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s", testdir, tgtencdir, srcdir_name , file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	/* cleanup */
	if (tgtencfd != -1)
	    close(tgtencfd);
	clean_dir(tgtencdir);
	tgtencdir = NULL;
	clean_dir(testtmpdir);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* now rename a dir while moving it */
TEST(test_mv_ddf_rename, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Create Dir2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, dir2);
	mkdir(path, 0755);

	/* Create File1 */
	create_file(path, file1);

	/* Test */
	treemv(&sw_ctx,
		     testst.st_ino,
		     testfd,
		     srcst.st_ino,
		     tgtfd,
		     ENC_DEFAULT,
		     srcdir_name + 1, //XXX: hacky way to change the name
		     srcdir_name,
		     NULL,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name + 1,
		dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test - earliest successful restart after first dir is renamed */
TEST(test_mv_ddf_restart_1stfull, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	fail_if(-1 == mkdir(path, 0755));
	/* Create Dir2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	/* Create File1 */
	create_file(path, file1);
	/* Create tmp dir */
	sprintf(path, "%s/%s%llx", testdir, TW_MV_TMPNAME, srcst.st_ino);
	mkdir(path, 0775);
	/* Rename first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(path, "%s/%s%llx", testdir, TW_MV_TOPTMPNAME, tmpst.st_ino);
	fail_if(-1 == rename(srcdir_path, path));
	sprintf(path, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);

	/* Test */
	treemv_restart(&sw_ctx, testst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     path,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test - just after moving the first dir's children into the first tmp */
TEST(test_mv_ddf_restart_1stempty, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	mkdir(path, 0775);
	/* Rename first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create Filedata in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));
	sprintf(path, "%s/%s/%s/%s", testdir, tmpdirname, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     testst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Test for nested treemv_restart */
TEST(test_mv_restart_tmpdir, .attrs="overnight")
{
        int tmpfd = -1;
        struct isi_error *error = NULL;
        struct stat tmpst = {};
        char tmpsrcname[MAXPATHLEN], tmptgtname[MAXPATHLEN];
        char tmpdirname[MAXPATHLEN], tmptgt[MAXPATHLEN];
        struct migr_sworker_ctx sw_ctx = {};
        char *new_name = "new_tgtname";
        struct quota_domain_id qdi;
        struct quota_domain domain;

        /* Setup the sw_ctx with what we need */
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.global.sync_id = 0;

        /* Create tmp dir */
        sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
        sprintf(path, "%s/%s", testdir, tmpdirname);
        fail_if(-1 == mkdir(path, 0775));
        /* Move first dir */
        fail_if(-1 == stat(path, &tmpst));
        sprintf(tmptgtname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
        sprintf(path, "%s/%s", tgtdir_path, tmptgtname);
        fail_if(-1 == rename(srcdir_path, path));


        /* Create Filedata in the tmp dir */
        sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
        fail_if(-1 == mkdir(path, 0755));
        fail_if(-1 == stat(path, &tmpst));
        qdi_init(&qdi, tmpst.st_ino, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
        create_and_setup_quota(&domain, &qdi);
        create_file(path, file1);

        /* Create a dir which will be renamed later */
        sprintf(tmpsrcname, "tmpdir");
        sprintf(tmptgt, "%s/%s", testdir, tmpsrcname);
        fail_if(-1 == mkdir(tmptgt, 0755));
        fail_if(-1 == stat(tmptgt, &tmpst));

        /* Create tmp dir for above dir */
        sprintf(tmpsrcname, "%s%llx", TW_MV_TMPNAME, tmpst.st_ino);
       sprintf(path, "%s/%s/%s/%s", testdir, tmpdirname, dir1, tmpsrcname);
        fail_if(-1 == mkdir(path, 0755));
        fail_if(-1 == stat(path, &tmpst));
        create_file(path, file1);

        /* Create TW_MV_SAVEDDIRNAMEFILE under tmpdir*/
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "Unable to open tmpsrc file");
        write_treemv_orig_name(new_name,
                        ENC_DEFAULT, tmpfd, tmpst.st_ino, &error);
        close(tmpfd);
        fail_if(error);

        /* Rename the tmp dir */
        sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
        sprintf(path, "%s/%s", testdir, tmpsrcname);
        fail_if(-1 == rename(tmptgt, path));

        /* Test */
        treemv_restart(&sw_ctx,
                     tgtst.st_ino,
                     srcst.st_ino,
                     tgtst.st_ino,
                     ENC_DEFAULT,
                     srcdir_name,
                     tmptgtname,
                     false,
                     &error);
        fail_if_error(error);

        /* Validate Dir move */
        sprintf(path, "%s/%s", testdir, tmpdirname);
        tmpfd = open(path, O_RDONLY);
        fail_unless(tmpfd == -1 && errno == ENOENT);
        if (tmpfd != -1)
                close(tmpfd); /* make cpcheck shutup */
        sprintf(path, "%s/%s/%s", testdir, new_name, file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);
        sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);

        quota_domain_delete(&domain, &error);
        fail_if_error(error, "quota_domain_delete");
        if (clean_mvtmp_in_dir(testdir))
                fail("Extra Temporary Dir Found");
}

/* Restart test - just after moving the first dir to the desired location */
TEST(test_mv_ddf_restart_1stmvd, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	mkdir(path, 0775);
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create Filedata in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));
	sprintf(path, "%s/%s/%s/%s", testdir, tmpdirname, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate Dir4 */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test - after the first dir is handled,
 * try restart with a tmp for 2nd dir */
TEST(test_mv_ddf_restart_2ndfull, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;


	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create Filedata in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));
	sprintf(path, "%s/%s/%s/%s", testdir, tmpdirname, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Create a tmpdir for the 2nd dir to be moved */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == stat(path, &tmpst));
	sprintf(path, "%s/%s/%s%llx", testdir, tmpdirname,
	    TW_MV_TMPNAME, tmpst.st_ino);
	fail_if(-1 == mkdir(path, 0775));

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test - after the first dir is handled,
 * try restart with a tmp for 2nd dir that has all of dir2's children*/
TEST(test_mv_ddf_restart_2ndempty, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	char tmppath[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create dir1 in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));

	/* Create a tmpdir for the 2nd dir to be moved */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == stat(path, &tmpst));
	sprintf(path, "%s/%s/%s%llx", testdir, tmpdirname,
	    TW_MV_TMPNAME, tmpst.st_ino);
	fail_if(-1 == mkdir(path, 0775));

	/* put the rest of the test data (dir2/file1) into the tmpdir1 */
	strcpy(tmppath, path);
	sprintf(path, "%s/%s", tmppath, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test - after the first dir is handled,
 * try restart with 2nd dir already moved. */
TEST(test_mv_ddf_restart_2ndmvd, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	char tmppath[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create dir1 in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));

	/* Create a tmpdir for the dir1 to be moved */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == stat(path, &tmpst));
	sprintf(path, "%s/%s/%s%llx", testdir, tmpdirname,
	    TW_MV_TMPNAME, tmpst.st_ino);
	fail_if(-1 == mkdir(path, 0775));

	/* put the rest of the test data into the tmpdir1 */
	strcpy(tmppath, path);
	sprintf(path, "%s/%s", tmppath, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Move dir1 to dst */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	sprintf(path2, "%s/%s/%s", tgtdir_path, tmpsrcname, dir1);
	fail_if(-1 == rename(path, path2));

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test -
 * try restart with a tmp for 2nd dir that has some of dir2's children*/
TEST(test_mv_ddf_restart_2ndpartialempty, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	char tmppath[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create dir1 in the tmp dir */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == mkdir(path, 0755));

	/* Create a tmpdir for the 2nd dir to be moved */
	sprintf(path, "%s/%s/%s", testdir, tmpdirname, dir1);
	fail_if(-1 == stat(path, &tmpst));
	create_file(path, file2);
	sprintf(path, "%s/%s/%s%llx", testdir, tmpdirname,
	    TW_MV_TMPNAME, tmpst.st_ino);
	fail_if(-1 == mkdir(path, 0775));

	/* put the rest of the test data (dir2/file1) into the tmpdir1 */
	strcpy(tmppath, path);
	sprintf(path, "%s/%s", tmppath, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test -
 * try restart with all dirs moved except, tmp still exists, and no final rename */
TEST(test_mv_ddf_restart_tmpallempty, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Create dir1, dir2, file1 in the first dir */
	sprintf(path, "%s/%s/%s", tgtdir_path, tmpsrcname, dir1);
	fail_if(-1 == mkdir(path, 0755));
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, tmpsrcname, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* Restart test -
 * try restart with work done except the final rename */
TEST(test_mv_ddf_restart_missingfinalrename, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	sprintf(tmpdirname, "%s%llx", TW_MV_TMPNAME, srcst.st_ino);
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	sprintf(tmpsrcname, "%s%llx", TW_MV_TOPTMPNAME, tmpst.st_ino);
	sprintf(path, "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));

	/* Get rid of tmp dir */
	sprintf(path, "%s/%s", testdir, tmpdirname);
	fail_if(-1 == rmdir(path));

	/* Create dir1, dir2, file1 in the first dir */
	sprintf(path, "%s/%s/%s", tgtdir_path, tmpsrcname, dir1);
	fail_if(-1 == mkdir(path, 0755));
	sprintf(path, "%s/%s/%s/%s", tgtdir_path, tmpsrcname, dir1, dir2);
	fail_if(-1 == mkdir(path, 0755));
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx, tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	sprintf(path, "%s/%s/%s/%s", srcdir_path, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	sprintf(path, "%s/%s/%s/%s/%s", tgtdir_path, srcdir_name, dir1, dir2, file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* try the restart function with a bad source dir, should fail */
TEST(test_mv_restart_no_src, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv_restart(&sw_ctx,
		     ROOT_LIN,
		     bad_lin,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* try the restart function with a bad target dir, should fail */
TEST(test_mv_restart_no_tgt, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     ROOT_LIN,
		     srcst.st_ino,
		     bad_lin,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_restart_no_parent, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);


	/* Test */
	treemv_restart(&sw_ctx,
		     bad_lin,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_restart_bad_name, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);

	/* Test */
	treemv_restart(&sw_ctx, ROOT_LIN,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     "",
		     "",
		     false,
		     &error);
	fail_unless_error(error);

	treemv_restart(&sw_ctx, ROOT_LIN,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name + 500,
		     srcdir_name + 500,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_restart_bad_enc, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create File1 on src */
	create_file(srcdir_path, file1);

	/* Create File1 on tgt */
	create_file(tgtdir_path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     ROOT_LIN,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_ISO_8859_15,	/* aka Latin-9 */
		     srcdir_name,
		     srcdir_name,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

/* test restart when no work has been done yet */
TEST(test_mv_ddf_restart_cold, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create Dir1 */
	sprintf(path, "%s/%s", srcdir_path, dir1);
	mkdir(path, 0755);

	/* Create Dir2 */
	sprintf(path, "%s/%s/%s", srcdir_path, dir1, dir2);
	mkdir(path, 0755);

	/* Create File1 */
	create_file(path, file1);

	/* Test */
	treemv_restart(&sw_ctx,
		     ROOT_LIN,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     srcdir_name,
		     false,
		     &error);
	fail_unless_error(error);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

TEST(test_mv_restart_hardlink, .attrs="overnight")
{
	int tmpfd = -1;
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char tmpsrcname[MAXPATHLEN];
	char tmpdirname[MAXPATHLEN];
	struct migr_sworker_ctx sw_ctx = {};

	/* Setup the sw_ctx with what we need */
	sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
	sw_ctx.global.sync_id = 0;

	/* Create tmp dir */
	snprintf(tmpdirname, sizeof(tmpdirname), "%s%llx", TW_MV_TMPNAME,
	    srcst.st_ino);
	snprintf(path, sizeof(tmpdirname), "%s/%s", testdir, tmpdirname);
	fail_if(-1 == mkdir(path, 0775));
	/* Create File1 in tmp dir */
	create_file(path, file1);

	/* Move first dir */
	fail_if(-1 == stat(path, &tmpst));
	snprintf(tmpsrcname, sizeof(tmpsrcname), "%s%llx", TW_MV_TOPTMPNAME,
	    tmpst.st_ino);
	snprintf(path, sizeof(path), "%s/%s", tgtdir_path, tmpsrcname);
	fail_if(-1 == rename(srcdir_path, path));
	/* Hardlink File1 from src to tgt */
	snprintf(path, sizeof(path), "%s/%s/%s", testdir, tmpdirname, file1);
	snprintf(path2, sizeof(path2), "%s/%s/%s", tgtdir_path, tmpsrcname,
	    file1);
	fail_if(-1 == link(path, path2));

	/* Test */
	treemv_restart(&sw_ctx,
		     tgtst.st_ino,
		     srcst.st_ino,
		     tgtst.st_ino,
		     ENC_DEFAULT,
		     srcdir_name,
		     tmpsrcname,
		     false,
		     &error);
	fail_if_error(error);

	/* Validate move completed */
	snprintf(path, sizeof(path), "%s/%s", srcdir_path, file1);
	tmpfd = open(path, O_RDONLY);
	fail_unless(tmpfd == -1 && errno == ENOENT);
	if (tmpfd != -1)
		close(tmpfd); /* make cpcheck shutup */
	snprintf(path, sizeof(path), "%s/%s/%s", tgtdir_path, srcdir_name,
	    file1);
	tmpfd = open(path, O_RDONLY);
	fail_if(tmpfd < 0, "File move validation failed.");
	close (tmpfd);

	if (clean_mvtmp_in_dir(testdir))
		fail("Extra Temporary Dir Found");
}

static void
test_mv_restart_1stdircomplete_target_tmpdir(bool save_name)
{
        int tmpfd = -1;
        struct isi_error *error = NULL;
        struct stat tmpst = {}, tmptgtst = {};
        char tmpsrcname[MAXPATHLEN];
        char tmpdirname[MAXPATHLEN];
        char tmptgtdirname[MAXPATHLEN];
        struct migr_sworker_ctx sw_ctx = {};

        /* Setup the sw_ctx with what we need */
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.global.sync_id = 0;

        /* Create tmp dir */
        snprintf(tmpdirname, sizeof(tmpdirname), "%s%llx", TW_MV_TMPNAME,
            srcst.st_ino);
        snprintf(path, sizeof(path), "%s/%s", testdir, tmpdirname);
        fail_if(-1 == mkdir(path, 0775));
        /* Create File1 in tmp dir */
        create_file(path, file1);

        /* Move first dir */
        fail_if(-1 == stat(path, &tmpst));
        snprintf(tmpsrcname, sizeof(tmpsrcname), "%s%llx", TW_MV_TOPTMPNAME,
            tmpst.st_ino);
        snprintf(path, sizeof(path), "%s/%s", tgtdir_path, tmpsrcname);
        fail_if(-1 == rename(srcdir_path, path));

        if (save_name) {
                tmpfd = open(path, O_RDONLY);
                fail_if(tmpfd < 0, "Unable to open tmpsrc file");
                write_treemv_orig_name(srcdir_name,
                                ENC_DEFAULT, tmpfd, srcst.st_ino, &error);
                close(tmpfd);
                fail_if(error);
        }

        /* Create tmp dir for target */
        snprintf(tmptgtdirname, sizeof(tmptgtdirname), "%s%llx", TW_MV_TMPNAME,
            tgtst.st_ino);
        snprintf(path, sizeof(tmptgtdirname), "%s/%s", testdir, tmptgtdirname);
        fail_if(-1 == mkdir(path, 0775));
        fail_if(-1 == stat(path, &tmptgtst));


        /* Test */
        treemv_restart(&sw_ctx,
                     tgtst.st_ino,
                     srcst.st_ino,
                     tmptgtst.st_ino,
                     ENC_DEFAULT,
                     srcdir_name,
                     tmpsrcname,
                     save_name,
                     &error);
        fail_if_error(error);

        /* Validate move completed */
        snprintf(path, sizeof(path), "%s/%s/%s", testdir, tmpdirname, file1);
        tmpfd = open(path, O_RDONLY);
        fail_unless(tmpfd == -1 && errno == ENOENT);
        if (tmpfd != -1)
                close(tmpfd); /* make cpcheck shutup */
        snprintf(path, sizeof(path), "%s/%s/%s/%s", testdir, tmptgtdirname, srcdir_name,
            file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);
}

TEST(test_mv_restart_1stdircomplete_target_tmpdir_without_savename, .attrs="overnight")
{
        test_mv_restart_1stdircomplete_target_tmpdir(false);
}

TEST(test_mv_restart_1stdircomplete_target_tmpdir_with_savename, .attrs="overnight")
{
        test_mv_restart_1stdircomplete_target_tmpdir(true);
}

static void
test_mv_restart_1stdir_not_complete_target_tmpdir(bool save_name)
{
        int tmpfd = -1;
        struct isi_error *error = NULL;
        struct stat tmpst = {}, tmptgtst = {};
        char tmpsrcname[MAXPATHLEN];
        char tmpdirname[MAXPATHLEN];
        char tmptgtdirname[MAXPATHLEN];
        struct migr_sworker_ctx sw_ctx = {};

        /* Setup the sw_ctx with what we need */
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.global.sync_id = 0;

        /* Create tmp dir */
        snprintf(tmpdirname, sizeof(tmpdirname), "%s%llx", TW_MV_TMPNAME,
            srcst.st_ino);
        snprintf(path, sizeof(path), "%s/%s", tgtdir_path, tmpdirname);
        fail_if(-1 == mkdir(path, 0775));
        /* Create File1 in tmp dir */
        create_file(path, file1);

        if (save_name) {
                tmpfd = open(path, O_RDONLY);
                fail_if(tmpfd < 0, "Unable to open tmpsrc file");
                write_treemv_orig_name(srcdir_name,
                                ENC_DEFAULT, tmpfd, srcst.st_ino, &error);
                close(tmpfd);
                fail_if(error);
        }

        /* Move first dir */
        fail_if(-1 == stat(path, &tmpst));
        snprintf(tmpsrcname, sizeof(tmpsrcname), "%s%llx", TW_MV_TOPTMPNAME,
            tmpst.st_ino);
        snprintf(path, sizeof(path), "%s/%s", tgtdir_path, tmpsrcname);
        fail_if(-1 == rename(srcdir_path, path));
        /* Create File2 in tmp dir */
        create_file(path, file2);

        /* Create tmp dir for target */
        snprintf(tmptgtdirname, sizeof(tmptgtdirname), "%s%llx", TW_MV_TMPNAME,
            tgtst.st_ino);
        snprintf(path, sizeof(tmptgtdirname), "%s/%s", testdir, tmptgtdirname);
        fail_if(-1 == mkdir(path, 0775));
        fail_if(-1 == stat(path, &tmptgtst));

        /* Test */
        treemv_restart(&sw_ctx,
                     tgtst.st_ino,
                     srcst.st_ino,
                     tmptgtst.st_ino,
                     ENC_DEFAULT,
                     srcdir_name,
                     tmpsrcname,
                     save_name,
                     &error);
        fail_if_error(error);

        /* Validate move completed */
        snprintf(path, sizeof(path), "%s/%s/%s", testdir, tmpdirname, file1);
        tmpfd = open(path, O_RDONLY);
        fail_unless(tmpfd == -1 && errno == ENOENT);
        if (tmpfd != -1)
                close(tmpfd); /* make cpcheck shutup */
        snprintf(path, sizeof(path), "%s/%s/%s/%s", testdir, tmptgtdirname, srcdir_name,
            file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);
        snprintf(path, sizeof(path), "%s/%s/%s/%s", testdir, tmptgtdirname, srcdir_name,
            file2);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);
}

TEST(test_mv_restart_1stdir_not_complete_target_tmpdir_without_savename, .attrs="overnight")
{
        test_mv_restart_1stdir_not_complete_target_tmpdir(false);
}

TEST(test_mv_restart_1stdir_not_complete_target_tmpdir_with_savename, .attrs="overnight")
{
        test_mv_restart_1stdir_not_complete_target_tmpdir(true);
}

TEST(test_mv_restart_complete_except_rename_savefile)
{
        int tmpfd = -1;
        struct isi_error *error = NULL;
        struct stat tmptgtst = {};
        char tmpsrcname[MAXPATHLEN];
        char tmptgtdirname[MAXPATHLEN];
        struct migr_sworker_ctx sw_ctx = {};
        struct quota_domain_id qdi;
        struct quota_domain domain;

        /* Setup the sw_ctx with what we need */
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.global.sync_id = 0;

        /* Move first dir with a dummy lin (which should be renamed)*/
        snprintf(tmpsrcname, sizeof(tmpsrcname), "%s1234", TW_MV_TOPTMPNAME);
        snprintf(path, sizeof(path), "%s/%s", tgtdir_path, tmpsrcname);
        fail_if(-1 == rename(srcdir_path, path));
        create_file(path, file1);

        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "Unable to open tmpsrc file");
        write_treemv_orig_name(srcdir_name,
                        ENC_DEFAULT, tmpfd, srcst.st_ino, &error);
        close(tmpfd);
        fail_if(error);

        /* Create quota on target directory */
        qdi_init(&qdi, tgtst.st_ino, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
        create_and_setup_quota(&domain, &qdi);

        /* Create tmp dir for target */
        snprintf(tmptgtdirname, sizeof(tmptgtdirname), "%s%llx", TW_MV_TMPNAME,
            tgtst.st_ino);
        snprintf(path, sizeof(tmptgtdirname), "%s/%s", testdir, tmptgtdirname);
        fail_if(-1 == mkdir(path, 0775));
        fail_if(-1 == stat(path, &tmptgtst));

        /* Test */
        treemv_restart(&sw_ctx,
                     tgtst.st_ino,
                     srcst.st_ino,
                     tmptgtst.st_ino,
                     ENC_DEFAULT,
                     srcdir_name,
                     tmpsrcname,
                     true,
                     &error);
        fail_if_error(error);

        /* Validate move completed */
        snprintf(path, sizeof(path), "%s/%s/%s", testdir, tgtdir_path, tmpsrcname);
        tmpfd = open(path, O_RDONLY);
        fail_unless(tmpfd == -1 && errno == ENOENT);
        if (tmpfd != -1)
                close(tmpfd); /* make cpcheck shutup */
        snprintf(path, sizeof(path), "%s/%s/%s/%s", testdir, tmptgtdirname, srcdir_name,
            file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);

        /* Cleanup */
        quota_domain_delete(&domain, &error);
        fail_if_error(error, "quota_domain_delete");

        clean_mvtmp_in_dir(testdir);
}

/* Creates a tmpdir whose move has completed except rename and is under treemv
 * source dir. A treemv restart is invoked to rename the dir and move it to
 * target.
 */
TEST(test_treemv_restart_from_treemv, .attrs="overnight")
{
        int tmpfd = -1;
        struct isi_error *error = NULL;
        struct migr_sworker_ctx sw_ctx = {};
        const char *new_name = "new_name";
        struct stat tmpst = {};
        char tmpsrcname[MAXPATHLEN + 1];
        struct quota_domain_id qdi;
        struct quota_domain domain;

        /* Setup the sw_ctx with what we need */
        sw_ctx.global.primary = SOCK_LOCAL_OPERATION;
        sw_ctx.global.sync_id = 0;

        /* Create tmp dir under source whose move has finished but name has not
         * been changed
         */
        snprintf(tmpsrcname, sizeof(tmpsrcname), "%s1234", TW_MV_TOPTMPNAME);
        snprintf(path, sizeof(path), "%s/%s", srcdir_path, tmpsrcname);
        (void) mkdir(path, 0755);
        create_file(path, file1);

        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "Unable to open path: %s", path);
        fail_if(-1 == stat(path, &tmpst));
        write_treemv_orig_name(new_name,
                        ENC_DEFAULT, tmpfd, tmpst.st_ino, &error);
        close(tmpfd);
        fail_if(error);

        /* Create quota on target directory */
        qdi_init(&qdi, tgtst.st_ino, QUOTA_DOMAIN_ALL, false, ANY_INSTANCE);
        create_and_setup_quota(&domain, &qdi);

        /* Test */
        treemv(&sw_ctx,
                     testst.st_ino,
                     testfd,
                     srcst.st_ino,
                     tgtfd,
                     ENC_DEFAULT,
                     srcdir_name,
                     srcdir_name,
                     NULL,
                     false,
                     &error);
        fail_if_error(error);

        /* Validate Dir1 */
        sprintf(path, "%s/%s", srcdir_path, new_name);
        tmpfd = open(path, O_RDONLY);
        fail_unless(tmpfd == -1 && errno == ENOENT);
        if (tmpfd != -1)
                close(tmpfd); /* make cpcheck shutup */
        sprintf(path, "%s/%s/%s/%s", tgtdir_path, srcdir_name, new_name, file1);
        tmpfd = open(path, O_RDONLY);
        fail_if(tmpfd < 0, "File move validation failed.");
        close (tmpfd);

        /* Cleanup */
        quota_domain_delete(&domain, &error);
        fail_if_error(error, "quota_domain_delete");

        if (clean_mvtmp_in_dir(testdir))
                fail("Extra Temporary Dir Found");
}

static void
save_treemv_orig_name_test(char *dir, enc_t encoding)
{
	struct isi_error *error = NULL;
	struct stat tmpst = {};
	char dirname[MAXPATHLEN];
	char savedfile[MAXPATHLEN];
	int parent_fd = -1;
	char *outputname = NULL;
	enc_t outputenc = -1;

	/* Create directory to save */
	snprintf(dirname, sizeof(dirname), "%s/%s", testdir, dir);
	fail_if(-1 == mkdir(dirname, 0775));
	parent_fd = open(dirname, O_RDONLY);
	fail_if(-1 == fstat(parent_fd, &tmpst));

	/* Save directory to file */
	write_treemv_orig_name(dir, encoding, parent_fd, tmpst.st_ino,
	    &error);
	fail_if(error, "%s", error ? isi_error_get_message(error) : "");

	/* Verify file exists */
	fail_if(-1 == stat(dirname, &tmpst));
	snprintf(savedfile, sizeof(savedfile), "%s/%s", dirname,
	    TW_MV_SAVEDDIRNAMEFILE);
	fail_if(-1 == stat(savedfile, &tmpst));

	/* Read directory from file */
	read_treemv_orig_name(testfd, dir, &outputname,
	    &outputenc, &error);
	fail_if(error, "%s", error ? isi_error_get_message(error): "");

	/* Verify */
	fail_unless(strcmp(dir, outputname) == 0);
	fail_unless(outputenc == encoding);
	free(outputname);

}

TEST(test_save_treemv_orig_name, .attrs="overnight")
{
	save_treemv_orig_name_test("1", ENC_UTF8);
	save_treemv_orig_name_test("22", ENC_EUC_JP_MS);
	save_treemv_orig_name_test("333", ENC_CP1252);
	save_treemv_orig_name_test(dir1, ENC_ISO_8859_1);
}

/*
 * BEJ: TODO: additional cases:
 *      Hardlinks accross domains
 *      Hardlinks
 *      Softlinks
 */
