#include "check_helper.h"
#include "isi_snapshot/snapshot.h"

SUITE_DEFINE_FOR_FILE(migr_dir, .timeout = 60);

#define MIGR_DIR_TEST(__test_name, __source, __begin, __end)		\
static void __test_name##_inner(struct migr_dir *dir);			\
TEST(__test_name, .attrs="overnight")							\
{									\
	struct isi_error *error = NULL;					\
	struct migr_dir *dir;						\
	int fd;								\
	char *dir_name = (__source);					\
	struct dir_slice slice = { (__begin), (__end) };		\
	fd = open(dir_name, O_RDONLY);					\
	fail_unless(fd != -1);						\
	dir = migr_dir_new(fd, &slice, true, &error);				\
	fail_if_error(error);						\
	__test_name##_inner(dir);					\
	migr_dir_free(dir);						\
	clean_dir(dir_name);						\
}									\
static void __test_name##_inner(struct migr_dir *dir)

#define MIGR_DIR_SLICE_TEST(__test_name, __expected, __src, __begin, __end) \
MIGR_DIR_TEST(__test_name, __src, __begin, __end)			\
{									\
	int count = count_dirents(dir);					\
	fail_unless(count == __expected, "count = %d", count);		\
}

#define MIGR_DIR_COUNT_TEST(__test_name, __expected, __src)		\
MIGR_DIR_SLICE_TEST(__test_name, __expected, __src,			\
    DIR_SLICE_MIN, DIR_SLICE_MAX)

MIGR_DIR_COUNT_TEST(simple_walk, 100, setup_simple_dir(100));
MIGR_DIR_COUNT_TEST(off_boundary, 252, setup_simple_dir(252));
MIGR_DIR_COUNT_TEST(conflict_walk, 200, setup_conflict_dir(4, 50));
MIGR_DIR_COUNT_TEST(conflict_off_boundary, 140, setup_conflict_dir(20, 7));

MIGR_DIR_SLICE_TEST(first_half_simple, 44, setup_simple_dir(100),
    DIR_SLICE_MIN, DIR_SLICE_MAX / 2);
MIGR_DIR_SLICE_TEST(second_half_simple, 56, setup_simple_dir(100),
    DIR_SLICE_MAX / 2, DIR_SLICE_MAX);
MIGR_DIR_SLICE_TEST(split_boundary_upper, 26, setup_simple_dir(100),
    DIR_SLICE_MIN, 0x2590e00000000001ull);
MIGR_DIR_SLICE_TEST(split_boundary_lower, 74, setup_simple_dir(100),
    0x2590e00000000001ull, DIR_SLICE_MAX);

MIGR_DIR_COUNT_TEST(short_simple, 25, setup_simple_dir(25));
MIGR_DIR_COUNT_TEST(short_conflict_walk, 30, setup_conflict_dir(10, 3));


#define MIGR_SPLIT_DIR_TEST(__test_name, __src, __initial, __before, __after) \
MIGR_DIR_TEST(__test_name, __src, DIR_SLICE_MIN, DIR_SLICE_MAX)		\
{									\
	struct isi_error *error = NULL;					\
	struct migr_dir *other_dir;					\
	struct dir_slice slice;						\
	enum migr_split_result result;					\
	int count;							\
	read_n_dirents(dir, __initial);					\
	result = migr_dir_split(dir, &slice, &error);			\
	fail_if_error(error);						\
	count = count_dirents(dir);					\
	fail_unless(count == __before, "before_count=%d", count);	\
	if (__after == 0)						\
		fail_unless(result != MIGR_SPLIT_SUCCESS);		\
	else {								\
		fail_unless(result == MIGR_SPLIT_SUCCESS);		\
		other_dir = migr_dir_new(dup(migr_dir_get_fd(dir)),	\
		    &slice, true, &error);				\
		fail_if_error(error);					\
		count = count_dirents(other_dir);			\
		fail_unless(count == __after, "after_count=%d", count);	\
		migr_dir_free(other_dir);				\
	}								\
}

static char *
setup_missing_upper(int count, uint64_t limit)
{
	struct isi_error *error = NULL;
	struct migr_dir *mdir;
	struct migr_dirent *de;
	struct dir_slice slice = { limit, DIR_SLICE_MAX };
	DIR *dir;
	char *dir_name;
	int i, actual, ret;

	dir_name = create_new_dir();

	for (actual = 0, i = 0; actual < count; ) {
		/* Create enough to fill the remaining. */
		for (; actual < count; actual++)
			create_file(dir_name, "file_%04d.txt", i++);

		dir = opendir(dir_name);
		fail_unless(dir != NULL);

		mdir = migr_dir_new(dup(dirfd(dir)), &slice, true, &error);
		fail_if_error(error);
		for (;;) {
			de = migr_dir_read(mdir, &error);
			fail_if_error(error);
			if (de == NULL)
				break;

			ret = enc_unlinkat(dirfd(dir), de->dirent->d_name,
			    ENC_DEFAULT, 0);
			fail_unless(ret != -1);
			migr_dir_unref(mdir, de);
			actual--;
		}
		migr_dir_free(mdir);
		closedir(dir);

	}

	return dir_name;
}

MIGR_SPLIT_DIR_TEST(simple_split, setup_simple_dir(50), 10, 17, 23);
MIGR_SPLIT_DIR_TEST(split_at_the_end, setup_simple_dir(1), 0, 1, 0);
MIGR_SPLIT_DIR_TEST(empty_above, setup_missing_upper(50, DIR_SLICE_MAX / 2),
    48, 0, 2);

MIGR_SPLIT_DIR_TEST(conflicts_do_split, setup_conflict_dir(2, 25),
    26, 0, 24);

MIGR_DIR_TEST(double_split, setup_simple_dir(4), DIR_SLICE_MIN, DIR_SLICE_MAX)
{
	struct isi_error *error = NULL;
	struct dir_slice slice;
	enum migr_split_result result;

	read_n_dirents(dir, 3);
	result = migr_dir_split(dir, &slice, &error);
	fail_if_error(error);
	fail_unless(result == MIGR_SPLIT_SUCCESS);

	result = migr_dir_split(dir, &slice, &error);
	fail_if_error(error);
	fail_unless(result == MIGR_SPLIT_UNAVAILABLE,
	    "second split is %{} and %{}\n", dir_slice_fmt(&dir->_slice),
	    dir_slice_fmt(&slice));
}

TEST(file_changed, .attrs="overnight")
{
	struct isi_error *ie = NULL;
	char *dir;
	char *file1="filechanged";
	struct stat file1_stat;
	struct stat file2_stat;
	char *file2="filenotchanged";
	char *snap1 = "snap1";
	char *snap2 = "snap2";
	char *snap3 = "snap3";
	struct isi_str snap_str1, snap_str2, snap_str3, *paths[2];
	ifs_snapid_t snapid1, snapid2,snapid3;
	struct fmt FMT_INIT_CLEAN(fmt);
	struct fmt FMT_INIT_CLEAN(fmt1);
	struct fmt FMT_INIT_CLEAN(fmt2);
	int fd, ret;

	dir = create_new_dir();

	create_file(dir, file1);
	create_file(dir, file2);


	paths[0] = isi_str_alloc(dir, strlen(dir)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	paths[1] = NULL;

	// create first snapshot
	isi_str_init(&snap_str1, snap1, strlen(snap1)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	snapid1 = snapshot_create(&snap_str1, (time_t)0, paths, NULL, &ie);
	fail_if_error(ie);

	/* Touch the file */
	fmt_print(&fmt, "touch %s/%s", dir,file1);
	system(fmt_string(&fmt));

	// creat second snapshot
	isi_str_init(&snap_str2, snap2, strlen(snap2)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	snapid2 = snapshot_create(&snap_str2, (time_t)0, paths, NULL, &ie);
	fail_if_error(ie);

	fmt_print(&fmt1, "%s/%s", dir,file1);
	// Get lin number for two files
	fd = open(fmt_string(&fmt1), O_RDONLY);
	fail_unless(fd != -1, "%s: %s", fmt_string(&fmt1),
	    strerror(errno));
	fail_unless(fstat(fd, &file1_stat) != -1, "%s : %s",file1,
		strerror(errno));
	close(fd);	

	fmt_print(&fmt2, "%s/%s", dir,file2);
	fd = open(fmt_string(&fmt2), O_RDONLY);
	fail_unless(fd != -1, "%s: %s", fmt_string(&fmt2),
	    strerror(errno));
	fail_unless(fstat(fd, &file2_stat) != -1, "%s : %s",file2,
		strerror(errno));
	close(fd);	

	ret = ifs_snap_file_modified(file1_stat.st_ino,
		 snapid1, snapid2);
	fail_unless( ret == 1,"%s : %d",snap1,ret);

	ret = ifs_snap_file_modified(file2_stat.st_ino,
		snapid1, snapid2);
	fail_unless( ret == 0, "%s : %d",snap1,ret);

	// creat third snapshot
	isi_str_init(&snap_str3, snap3, strlen(snap3)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	snapid3 = snapshot_create(&snap_str3, (time_t)0, paths, NULL, &ie);
	fail_if_error(ie);

	fail_unless(ifs_snap_file_modified(file1_stat.st_ino,
		 snapid1, snapid3) == 1,"%s : %s",snap3,strerror(errno));

	fail_unless(ifs_snap_file_modified(file2_stat.st_ino,
		snapid1, snapid3) == 0, "%s : %s",snap3,strerror(errno));

	clean_dir(dir);
	snapshot_destroy(&snap_str1, &ie);
	fail_if_error(ie);
	snapshot_destroy(&snap_str2, &ie);
	fail_if_error(ie);
	snapshot_destroy(&snap_str3, &ie);
	fail_if_error(ie);
	isi_str_free(paths[0]);
}

