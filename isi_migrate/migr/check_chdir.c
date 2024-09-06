#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "isirep.h"

#include <check.h>


SUITE_DEFINE_FOR_FILE(migr_chdir, .timeout = 60);

inline static char *
create_top_dir(void)
{
	char *fname;
	fname = malloc(80);
	strcpy(fname, "/ifs/test.siq.chdir/chdir.XXXXXX");
	fail_unless(NULL != mktemp(fname));
	mkdir("/ifs/test.siq.chdir",0777);
	fail_unless(0 == mkdir(fname, 0777));
	return fname;
}

static int
same_file(int fd1, int fd2)
{
	struct stat st1;
	struct stat st2;

	fail_unless(0 == fstat(fd1, &st1));
	fail_unless(0 == fstat(fd2, &st2));
	return (st1.st_ino == st2.st_ino &&
	    st1.st_snapid == st2.st_snapid);
}
static int
same_file_path(int fd1, char *path)
{
	int test_fd;
	bool result;
	test_fd = open(path, O_RDONLY);
	fail_unless(test_fd > 0);
	result = same_file(test_fd, fd1);
	close(test_fd);
	return result;
}

TEST(abs_mk_existing, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirc");
	fail_unless(0 == mkdir(final_path, 0777));

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(out_fd > 0);
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_nonexistent_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	strcat(final_path,"/dirc");

	path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_file_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	int test_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	test_fd = open(final_path, O_CREAT, 0777);
	fail_unless(test_fd >= 0);
	close(test_fd);
	test_fd = -1;

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_file_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	int test_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	test_fd = open(final_path, O_CREAT, 0777);
	fail_unless(test_fd >= 0);
	close(test_fd);
	test_fd = -1;

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_dev_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mknod(final_path, S_IFBLK, 45));

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_dev_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mknod(final_path, S_IFBLK, 45));

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_valid_sym_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == symlink("/ifs/home", final_path));

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_mk_invalid_sym_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	char sym_dest[1024];
	strcpy(sym_dest, "/ifs/bad_name.XXXXXX");
	fail_unless(NULL != mktemp(sym_dest));

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == symlink(sym_dest, final_path));

	_path_init(&p, final_path, NULL);
	fail_unless(!smkchdirfd(&p, -1, &out_fd));
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}
/*=======================*/

TEST(abs_open_existing, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirc");
	fail_unless(0 == mkdir(final_path, 0777));

	_path_init(&p, final_path, NULL);
	fail_unless(!schdirfd(&p, -1, &out_fd));
	fail_unless(out_fd > 0);
	fail_unless(same_file_path(out_fd, final_path));
	close(out_fd);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_nonexistent_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	strcat(final_path,"/dirc");

	path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOENT);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_file_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	int test_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	test_fd = open(final_path, O_CREAT, 0777);
	fail_unless(test_fd >= 0);
	close(test_fd);
	test_fd = -1;

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_file_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	int test_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	test_fd = open(final_path, O_CREAT, 0777);
	fail_unless(test_fd >= 0);
	close(test_fd);
	test_fd = -1;

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_dev_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mknod(final_path, S_IFBLK, 45));

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_dev_middle, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == mknod(final_path, S_IFBLK, 45));

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_valid_sym_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == symlink("/ifs/home", final_path));

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

TEST(abs_open_invalid_sym_final, .attrs="overnight")
{
	char final_path[1024];
	char *topdir;
	struct path p;
	int out_fd = -1;
	char sym_dest[1024];
	strcpy(sym_dest, "/ifs/bad_name.XXXXXX");
	fail_unless(NULL != mktemp(sym_dest));

	topdir = create_top_dir();
	strcpy(final_path, topdir);
	strcat(final_path,"/dira");
	fail_unless(0 == mkdir(final_path, 0777));
	strcat(final_path,"/dirb");
	fail_unless(0 == symlink(sym_dest, final_path));

	_path_init(&p, final_path, NULL);
	fail_unless(schdirfd(&p, -1, &out_fd));
	fail_unless(errno == ENOTDIR);
	free(topdir);
	path_clean(&p);
}

