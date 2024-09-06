#include "./sw_check_helper.h"


#include <isi_migrate/migr/selectors.h>
#include "sworker.h"

SUITE_DEFINE_FOR_FILE(migr_sparse, .timeout = 60);

/*
 * Check that a simple sparse file 
 * Here the target sparse file is larger than the sparse
 * window we are trying to create
 */
TEST(simple_sparse_test, .attrs="overnight")
{
	char *top_dir;
	char *file = "sparse";
	int fd;
	int ret;
	struct stat stat1, stat2;
	struct file_state fstate = {};

	top_dir = create_new_dir();
	fstate->fd = create_filled_region(top_dir, file, 8 * 1000 * 1024,
	    4 * 1024);
	fail_unless(fstate->fd > 0);

	fsync(fstate->fd);
	ret = fstat(fstate->fd, &stat1);
	fail_unless(ret == 0);

	write_file_sparse(fstate->fd, 0, 2000 * 1024, 0, NULL, NULL);
	fsync(fstate->fd);

	ret = fstat(fstate->fd, &stat2);
	fail_unless(ret == 0);

	fail_unless(stat1.st_blocks == stat2.st_blocks);
	unlink(top_dir);
	free(top_dir);
}

/*
 * Here there is data region in the sparse window 
 * Hence only data region should have 0's with no extra
 * blocks allocated 
 */
TEST(sparse_test_with_data_region, .attrs="overnight")
{
	char *top_dir;
	char *file = "sparse";
	int fd;
	int ret;
	struct stat stat1, stat2;

	top_dir = create_new_dir();
	fstate->fd = create_filled_region(top_dir, file, 8 * 100 * 1024,
	    4 * 1024);
	fail_unless(fstate->fd > 0);
	fsync(fstate->fd);
	ret = fstat(fstate->fd, &stat1);
	fail_unless(ret == 0);

	write_file_sparse(fstate->fd, 0, 2000 * 1024, 0, NULL, NULL);
	fsync(fstate->fd);

	ret = fstat(fstate->fd, &stat2);
	fail_unless(ret == 0);

	fail_unless(stat1.st_blocks == stat2.st_blocks);
	fsync(fstate->fd);
	fail_unless(is_zero_region(fd, 8 * 100 * 1024, 4 * 1024));

	unlink(top_dir);
	free(top_dir);
}

TEST(sparse_test_exact, .attrs="overnight")
{
	char *top_dir;
	int fd, ret;
	struct stat stat1, stat2;

	top_dir = create_new_dir();
	fd = create_filled_region(top_dir, "sparse_exact", 8 * 100 * 1024,
	    8 * 2 * 1024);

	fail_unless(fd > 0);

	ret = ftruncate(fd, 8 * 200 * 1024);

	fsync(fd);
	ret = fstat(fd, &stat1);
	fail_unless(ret == 0);

	write_file_sparse(fd, 8 * 100 * 1024, 8 * 1024, 0, NULL, NULL);
	fsync(fd);

	ret = fstat(fd, &stat2);
	fail_unless(ret == 0);

	fail_unless(stat1.st_blocks == stat2.st_blocks);
	fsync(fd);
	fail_unless(is_zero_region(fd, 8 * 100 * 1024, 8 * 1024));
	fail_unless(!is_zero_region(fd, 8 * 101 * 1024, 8 * 1024));

	free(top_dir);
}
