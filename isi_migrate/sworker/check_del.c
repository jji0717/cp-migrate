#include "./sw_check_helper.h"


#include <isi_migrate/migr/selectors.h>
#include "sworker.h"
SUITE_DEFINE_FOR_FILE(migr_tw, .timeout = 60);

/*
 * Delete a directory without children.
 */
TEST(delete_empty_dir, .attrs="overnight")
{
	char *top_dir;
	char *child_dir;
	int fd;
	int ret;
	int err;
	struct stat st;
	struct isi_error *error = NULL;
	
	top_dir = create_new_dir();
	fail_unless(top_dir != NULL);
	fail_unless(0 == strncmp(top_dir, "/ifs/test", 9));

	child_dir = malloc(strlen(top_dir)+1+50);
	fail_unless(child_dir != NULL);
	sprintf(child_dir, "%s/to_del_empty_dir", top_dir);
	ret = mkdir(child_dir, 0777);
	fail_unless(ret == 0);
	fd = open(top_dir, O_RDONLY);
	fail_unless(fd >= 0);

	safe_rmrf(fd, "to_del_empty_dir", ENC_UTF8, -1, 0, 0, &error);
	fail_unless(error == NULL);
	ret = stat(child_dir, &st);
	err = errno;
	fail_unless(ret != 0);
	fail_unless(err == ENOENT);
	free(child_dir);
	free(top_dir);
}

/*
 * Delete a directory with children.
 */
TEST(test_dir_with_files, .attrs="overnight")
{
	char *top_dir;
	char *child_dir;
	char *scratch_path;
	int fd;
	int scratch_fd;
	int ret;
	int err;
	struct stat st;
	struct isi_error *error = NULL;
	
	top_dir = create_new_dir();
	fail_unless(top_dir != NULL);
	fail_unless(0 == strncmp(top_dir, "/ifs/test", 9));

	child_dir = malloc(strlen(top_dir)+1+2*MAXNAMLEN);
	scratch_path = malloc(strlen(top_dir)+1+2*MAXNAMLEN);

	fail_unless(child_dir != NULL);
	sprintf(child_dir, "%s/to_del_full_dir", top_dir);
	ret = mkdir(child_dir, 0777);
	fail_unless(ret == 0);

	fd = open(top_dir, O_RDONLY);
	fail_unless(fd >= 0);

	sprintf(scratch_path, "%s/file1", child_dir);
	scratch_fd = open(top_dir, O_CREAT, 0666);
	fail_unless(scratch_fd >= 0);
	close(scratch_fd);
	sprintf(scratch_path, "%s/file2", child_dir);
	scratch_fd = open(top_dir, O_CREAT, 0666);
	fail_unless(scratch_fd >= 0);
	close(scratch_fd);
	sprintf(scratch_path, "%s/dir1", child_dir);
	ret = mkdir(scratch_path, 0777);
	fail_unless(ret == 0);

	safe_rmrf(fd, "to_del_full_dir", ENC_UTF8, -1, 0, 0, &error);
	fail_unless(error == NULL);
	ret = stat(child_dir, &st);
	err = errno;
	fail_unless(ret != 0);
	fail_unless(err == ENOENT);
	free(child_dir);
	free(top_dir);
	free(scratch_path);
}

/*
 * Delete a filename that doesn't exist.
 * Somewhat odd test because the expected behavior is no error.
 */
TEST(delete_bad_name, .attrs="overnight")
{
	char *top_dir;
	char *child_dir;
	int fd;
	int ret;
	int err;
	struct stat st;
	struct isi_error *error = NULL;
	
	top_dir = create_new_dir();
	fail_unless(top_dir != NULL);
	fail_unless(0 == strncmp(top_dir, "/ifs/test", 9));

	child_dir = malloc(strlen(top_dir)+1+50);
	fail_unless(child_dir != NULL);
	sprintf(child_dir, "%s/to_del_empty_bad_dir", top_dir);
	ret = mkdir(child_dir, 0777);
	fail_unless(ret == 0);
	fd = open(top_dir, O_RDONLY);
	fail_unless(fd >= 0);

	safe_rmrf(fd, "to_del_empty_dir", ENC_UTF8, -1, 0, 0, &error);
	fail_unless(error == NULL);
	ret = stat(child_dir, &st);
	err = errno;
	fail_unless(ret == 0);
	free(child_dir);
	free(top_dir);
}

