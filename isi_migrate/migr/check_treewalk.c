#include "check_helper.h"

#include <ifs/ifs_lin_open.h>

#include <isi_migrate/migr/selectors.h>
#include <isi_testutil/snapshot.h>

static int ifs_fd = -1;

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(migr_tw,
    .mem_check = CK_MEM_DISABLE,
    .timeout = 120,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown);

TEST_FIXTURE(suite_setup)
{
	ifs_fd = open("/ifs", O_RDONLY);
	fail_unless(ifs_fd > 0);
}

TEST_FIXTURE(suite_teardown)
{
	close(ifs_fd);
	ifs_fd = -1;
}
	

inline static void
push_next_dirent(struct migr_tw *tw, struct migr_dir *dir)
{
	struct isi_error *error = NULL;
	struct migr_dirent *de;

	de = read_a_dirent(dir);
	migr_tw_push(tw, de, &error);
	fail_if_error(error);
	migr_dir_unref(dir, de);
}

inline static void
fail_unless_path_of(const char *original_path, const char *new_path,
   const char *expected_extra)
{
	const char *new_extra;
	int base_length;

	fail_unless(original_path != NULL);
	fail_unless(new_path != NULL);
	fail_unless(expected_extra != NULL);

	base_length = strlen(original_path);
	fail_unless(base_length <= strlen(new_path), "%d <= %d",
	    base_length, strlen(new_path));

	new_extra = new_path + base_length;

	fail_unless(strncmp(original_path, new_path, base_length) == 0,
	    "'%s' != base of '%s'", original_path, new_path);
	fail_unless(strcmp(new_extra, expected_extra) == 0,
	    "'%s' != '%s'", new_extra, expected_extra);
}

TEST(single_level_split, .attrs="overnight")
{
	char *dir_name;
	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct migr_dir *dir, *new_dir;
	struct path path = {}, new_path = {};
	struct migr_work_desc new_desc;
	enum migr_split_result result;

	dir_name = setup_layered_dir(50, 2);

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, NULL, false, true, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	read_n_dirents(dir, 20);

	push_next_dirent(&tw, dir);

	result = migr_tw_split(&tw, &new_path, &new_desc, &new_dir, &error);
	fail_if_error(error);

	fail_unless(result == MIGR_SPLIT_SUCCESS);
	fail_unless_path_of(dir_name, new_path.path, "");

	path_clean(&new_path);

	result = migr_tw_split(&tw, &new_path, &new_desc, &new_dir, &error);
	fail_if_error(error);

	fail_unless(result == MIGR_SPLIT_SUCCESS);
	fail_unless_path_of(dir_name, new_path.path, "");

	path_clean(&new_path);

	migr_tw_clean(&tw);
	path_clean(&path);
	clean_dir(dir_name);
}

#if 0
static void
hexdump(void *data, size_t size)
{
	uint8_t *cdata = data;
	int i, j;

	for (i = 0; i < size; i += 16) {
		printf("0x%04x: ", i);
		for (j = i; j < i + 16; j++) {
			if (j >= size)
				printf("   ");
			else
				printf("%02x ", cdata[j]);
		}
		printf("   ");

		for (j = i; j < i + 16; j++) {
			if (j >= size)
				printf(" ");
			else
				printf("%c",
				    isprint(cdata[j]) ? cdata[j] : '.');
		}
		printf("\n");
	}
}
#endif

TEST(serialization, .attrs="overnight")
{
	char *dir_name;
	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct migr_dir *dir;
	struct migr_dirent *de;
	struct path path = {};
	void *data;
	size_t size;
	int count;

	dir_name = setup_layered_dir(30, 5);
	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, NULL, false, true, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	read_n_dirents(dir, 20);

	push_next_dirent(&tw, dir);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);
	de = migr_dir_read(dir, &error);
	fail_if_error(error);

	migr_dir_set_checkpoint(dir, de->cookie);
	migr_dir_unref(dir, de);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);
	read_n_dirents(dir, 2);

	data = migr_tw_serialize(&tw, &size);

	count = count_dirents(dir);
	fail_unless(count == 2);

	migr_tw_clean(&tw);
	path_clean(&path);

	migr_tw_init_from_data(&tw, data, size, &error);
	fail_if_error(error);
	free(data);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);
	fail_unless(dir != NULL);

	count = count_dirents(dir);
	fail_unless(count == 4, "count = %d", count);

	migr_tw_clean(&tw);
	clean_dir(dir_name);
}

static void
sel_print(struct select *s, int indent)
{
	char fmt[256];

	while (s) {
		sprintf(fmt, "%%%ds %%d %%.%ds (%%p)", indent, s->len);
		printf(fmt, "\n", s->select, s->path, s);
		if (s->child)
			sel_print(s->child, indent + 2);
		s = s->next;
	}
}

static char *
build_selector_from_list(const char *path, const char *entries[], int count)
{
	struct select *select_tree = NULL;
	char **paths, *selectbuf;
	int i;

	paths = calloc(sizeof(*paths), count + 1);

	for (i = 0; i < count; i++) {
		fail_unless(*entries[i] == '+' || *entries[i] == '-');
		asprintf(&paths[i], "%c%s/%s", *entries[i], path,
		    entries[i] + 1);
	}

	select_tree = sel_build(path, (const char **)paths);
	selectbuf = sel_export(select_tree, &i);
	sel_free(select_tree);

	for (i = 0; i < count; i++)
		free(paths[i]);
	free(paths);

	return selectbuf;
}

static int
count_tree_files(struct migr_tw *tw, int limit, int checkpoint)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;
	struct migr_dirent *de;
	int count = 0;

	for (;;) {
		dir = migr_tw_get_dir(tw, &error);
		fail_if_error(error);

		de = migr_dir_read(dir, &error);
		fail_if_error(error);

		if (de == NULL) {
			//printf("popping %s\n", tw->_path.path);
			if (!migr_tw_pop(tw, dir))
				break;
			continue;
		}

		if (de->dirent->d_type == DT_DIR) {
			/*printf("pushing (through %llx/%llx) %s\n",
			    dir->_slice.begin, dir->_slice.end,
			    de->dirent->d_name);*/
			migr_tw_push(tw, de, &error);
			fail_if_error(error);
		} else if (dir->selected) {

			/*printf("(%016llx) %s/%s\n", de->cookie,
			    tw->_path.path, de->dirent->d_name);*/
			++count;
		}

		if (checkpoint == -1 || count <= checkpoint) {
			/*printf("CHECKPOINT %s/%s\n", tw->_path.path,
			    de->dirent->d_type != DT_DIR ?
			    de->dirent->d_name : "");*/
			migr_dir_set_checkpoint(dir, de->cookie);
		}

		migr_dir_unref(dir, de);
		if (limit != -1 && count >= limit)
			break;
	}

	return count;
}

TEST(selector_serialization, .attrs="overnight")
{
	const char *files[] = {
		"dir5/meow/oops/somefile1",		/* 1 */
		"dir5/meow/oops/somefile2",		/* 2 */
		"dir5/meow/oops/somedir/oow",
		"newdir/one/files2",			/* 3 */
		"newdir/one/files1",			/* 4 */
		"newdir/one/two/three/four/somefile",	/* 5 */
		"newdir/one/two/three/myfile1",
		"newdir/one/two/three/myfile2",
		"newdir/one/two/three/myfile3",
		"newdir/one/files4",			/* 6 */
		"newdir/one/files3",			/* 7 */
		"somedir/otherdir/nofile2",		/* 8 */
		"somedir/otherdir/nofile4",		/* 9 */
		"somedir/otherdir/nofile3",		/* 10 */
		"somedir/otherdir/nofile5",		/* 11 */
		"somedir/otherdir/nofile1",		/* 12 */
		NULL,
	};

	const char *select[] = {
		"-dir5/meow/oops/somedir",
		"-newdir/one/two/three",
		"+newdir/one/two/three/four",
		"+somedir",
	};

	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path path = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	char *dir_name, *selectors;
	void *data;
	size_t size;
	int count;

	dir_name = setup_specific_dir(files);

	selectors = build_selector_from_list(dir_name, select,
	    sizeof(select) / sizeof(*select));

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, selectors, true, true, &error);
	fail_if_error(error);

	count = count_tree_files(&tw, 5, 5);
	fail_unless(count == 5, "pre_count = %d", count);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);

	count = count_tree_files(&tw, -1, -1);
	fail_unless(count == 1, "post_count = %d", count);

	migr_tw_clean(&tw);
	free(data);

	free(selectors);
	path_clean(&path);
	clean_dir(dir_name);
}

TEST(selector_double_serialization, .attrs="overnight")
{
	const char *files[] = {
		"dir5/meow/oops/somefile1",		/* 1 */
		"dir5/meow/oops/somefile2",		/* 2 */
		"dir5/meow/oops/somedir/oow",
		"newdir/one/files2",			/* 3 */
		"newdir/one/files1",			/* 4 */
		"newdir/one/two/three/four/somefile",	/* 5 */
		"newdir/one/two/three/myfile1",
		"newdir/one/two/three/myfile2",
		"newdir/one/two/three/myfile3",
		"newdir/one/files4",			/* 6 */
		"newdir/one/files3",			/* 7 */
		"somedir/otherdir/nofile2",		/* 8 */
		"somedir/otherdir/nofile4",		/* 9 */
		"somedir/otherdir/nofile3",		/* 10 */
		"somedir/otherdir/nofile5",		/* 11 */
		"somedir/otherdir/nofile1",		/* 12 */
		NULL,
	};

	const char *select[] = {
		"-dir5/meow/oops/somedir",
		"-newdir/one/two/three",
		"+newdir/one/two/three/four",
		"+somedir",
	};

	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path path = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	char *dir_name, *selectors;
	void *data;
	size_t size;
	int count;

	dir_name = setup_specific_dir(files);

	selectors = build_selector_from_list(dir_name, select,
	    sizeof(select) / sizeof(*select));

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, selectors, true, true, &error);
	fail_if_error(error);

	count = count_tree_files(&tw, 5, 5);
	fail_unless(count == 5, "pre_count = %d", count);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);
	free(data);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);
	free(data);

	count = count_tree_files(&tw, -1, -1);
	fail_unless(count == 1, "post_count = %d", count);

	migr_tw_clean(&tw);

	free(selectors);
	path_clean(&path);
	clean_dir(dir_name);
}

TEST(selector_serialization_sanity, .attrs="overnight")
{
	const char *files[] = {
		"dir/foo",
		NULL,
	};

	const char *select[] = {
		"+dir",
	};

	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path path = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	char *dir_name, *selectors;
	void *data;
	size_t size;

	dir_name = setup_specific_dir(files);

	selectors = build_selector_from_list(dir_name, select,
	    sizeof(select) / sizeof(*select));
	
	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, selectors, true, true, &error);
	fail_if_error(error);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);
	fail_if_error(error);

	migr_tw_clean(&tw);
	free(data);

	free(selectors);
	path_clean(&path);
	clean_dir(dir_name);
}

TEST(serialization_plus_splitting, .attrs="overnight")
{
	const char *files[] = {
		"dir5/meow/oops/somefile1",		/* 1 */
		"dir5/meow/oops/somefile2",		/* 2 */
		"dir5/meow/oops/somedir/oow",		/* 3 */
		"newdir/one/files2",			/* 4 */
		"newdir/one/files1",			/* 5 */
		"newdir/one/two/three/four/somefile",	/* 6 */
		"newdir/one/two/three/myfile1",		/* 7 */
		"newdir/one/two/three/myfile2",		/* 8 */
		"newdir/one/two/three/myfile3",		/* 9 */
		"newdir/one/files4",			/* 10 */
		"newdir/one/files3",			/* 11 */
		"somedir/otherdir/nofile2",		/* 12 */
		"somedir/otherdir/nofile4",		/* 13 */
		"somedir/otherdir/nofile3",		/* 14 */
		"somedir/otherdir/nofile5",		/* 15 */
		"somedir/otherdir/nofile1",		/* 16 */
		NULL,
	};

	struct isi_error *error = NULL;
	struct migr_tw tw = {}, tw_new = {};
	struct migr_dir *new_dir = NULL;
	struct migr_work_desc new_desc;
	enum migr_split_result result;
	struct path path = {}, new_path = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	char *dir_name;
	void *data;
	size_t size;
	int count;

	dir_name = setup_specific_dir(files);

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, NULL, false, true, &error);
	fail_if_error(error);

	count = count_tree_files(&tw, 5, 5);
	fail_unless(count == 5, "pre_count = %d", count);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);

	count = count_tree_files(&tw, 1, 1);
	fail_unless(count == 1, "small_count = %d", count);

	result = migr_tw_split(&tw, &new_path, &new_desc, &new_dir, &error);
	fail_if_error(error);

	fail_unless(result == MIGR_SPLIT_SUCCESS);
	fail_unless_path_of(dir_name, new_path.path, "");

	count = count_tree_files(&tw, -1, -1);
	fail_unless(count == 5, "after_count = %d", count);

	migr_tw_clean(&tw);
	free(data);

	migr_tw_init(&tw_new, ifs_fd, &new_path, &new_desc.slice, NULL, false,
	    true, &error);
	fail_if_error(error);

	count = count_tree_files(&tw_new, -1, -1);
	fail_unless(count == 5, "after_count = %d", count);

	migr_tw_clean(&tw_new);

	path_clean(&path);
	path_clean(&new_path);
	clean_dir(dir_name);
}

TEST(serialize_at_new_directory, .attrs="overnight")
{
	const char *files[] = {
		"notpossible",				/* 1 */
		"somedir/otherdir/nofile2",		/* 2 */
		"somedir/otherdir/nofile4",		/* 3 */
		"somedir/otherdir/nofile3",		/* 4 */
		"somedir/otherdir/nofile5",		/* 5 */
		"somedir/otherdir/nofile1",		/* 6 */
		NULL,
	};

	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct migr_dir *dir, *new_dir = NULL;
	struct migr_work_desc new_desc;
	struct migr_dirent *de;
	enum migr_split_result result;
	struct path path = {}, new_path = {};
	struct fmt FMT_INIT_CLEAN(fmt);
	bool successful;
	char *dir_name;
	void *data;
	size_t size;
	int count;

	dir_name = setup_specific_dir(files);

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, NULL, false, true, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	de = migr_dir_read(dir, &error);
	fail_if_error(error);
	migr_dir_unref(dir, de);

	de = migr_dir_read(dir, &error);
	fail_if_error(error);

	successful = migr_tw_push(&tw, de, &error);
	fail_unless(successful);

	migr_dir_unref(dir, de);
	migr_dir_set_checkpoint(dir, de->cookie);

	data = migr_tw_serialize(&tw, &size);

	migr_tw_clean(&tw);

	migr_tw_init_from_data(&tw, data, size, &error);
	fail_if_error(error);

	result = migr_tw_split(&tw, &new_path, &new_desc, &new_dir, &error);
	fail_if_error(error);

	fail_unless(result == MIGR_SPLIT_UNKNOWN);

	count = count_tree_files(&tw, -1, -1);
	fail_unless(count == 5, "after_count = %d", count);

	migr_tw_clean(&tw);
	free(data);

	path_clean(&path);
	clean_dir(dir_name);
}

TEST(deep_dir_with_less_fds, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path path = {};
	struct migr_dir *dir;
	struct migr_dirent *de;
	char *dir_name;
	int fd, new_fd, i;

	dir_name = create_new_dir();

	fd = open(dir_name, O_RDONLY);
	fail_unless(fd != -1);

	for (i = 0; i < 300; i++) {
		new_fd = enc_mkdirat(fd, "a", ENC_DEFAULT, 0755);
		fail_unless(new_fd != -1);

		new_fd = enc_openat(fd, "a", ENC_DEFAULT, O_RDONLY);
		fail_unless(new_fd != -1);

		close(fd);
		fd = new_fd;
	}

	path_init(&path, dir_name, NULL);

	migr_tw_init(&tw, ifs_fd, &path, NULL, NULL, false, true, &error);
	fail_if_error(error);

	for (;;) {
		dir = migr_tw_get_dir(&tw, &error);
		fail_if_error(error);

		fail_unless(dir->_fd < 50);

		de = migr_dir_read(dir, &error);
		fail_if_error(error);

		if (de == NULL)
			break;

		migr_tw_push(&tw, de, &error);
		fail_if_error(error);
		migr_dir_unref(dir, de);
	}

	for (;;) {
		dir = migr_tw_get_dir(&tw, &error);
		fail_if_error(error);

		fail_unless(dir->_fd < 50);

		if (!migr_tw_pop(&tw, dir))
			break;
	}

	migr_tw_clean(&tw);
	path_clean(&path);
	clean_dir(dir_name);
}

/*
 * 1. Make sure resuming tw on a deleted cookie succeeds.
 * 2. Make sure resuming tw on a deleted cookie doesn't skip the next dirent.
 * 3. Make sure resuming tw on a deleted cookie in an empty directory
 *    succeeds.
 */
#define LC_FILE "a"
#define UC_FILE "A"
TEST(treewalk_init_with_deleted_cookies, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path path = {};
	struct migr_dir *dir;
	struct migr_dirent *de;
	struct dir_slice slice = DIR_SLICE_INITIALIZER;
	struct isi_extattr_blob eb = { .allocated = 0 };
	struct ifs_dinode *dip;
	struct stat st;
	char *dir_name;
	void *tw_data = NULL;
	uint64_t cookie;
	size_t tw_size;
	int dir_fd, fd, ret;

	dir_name = create_new_dir();

	path_init(&path, dir_name, NULL);

	dir_fd = open(dir_name, O_RDONLY);
	fail_unless(dir_fd != -1);

	/* use hash collisions to make the cookies predictable */
	fd = enc_openat(dir_fd, LC_FILE, ENC_DEFAULT, O_CREAT|O_RDWR, 0770);
	fail_if(fd == -1);
	ret = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS, "dinode", &eb);
	fail_if(ret);
	close(fd);

	dip = eb.data;
	cookie = rdp_cookie63(dip->di_parent_hash, 1);
	free(eb.data);
	dip = NULL;

	fd = enc_openat(dir_fd, UC_FILE, ENC_DEFAULT, O_CREAT|O_RDWR, 0770);
	fail_if(fd == -1);

	ret = fstat(fd, &st);
	fail_if(ret);

	close(fd);

	/* initialize tw */
	migr_tw_init(&tw, ifs_fd, &path, &slice, NULL, false, true, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	migr_dir_set_checkpoint(dir, cookie);

	/* serialize */
	tw_data = migr_tw_serialize(&tw, &tw_size);
	fail_if(tw_data == NULL);

	/* delete cookie dirent */
	ret = enc_unlinkat(dir_fd, LC_FILE, ENC_DEFAULT, 0);
	fail_if(ret);

	/* deserialize */
	migr_tw_clean(&tw);
	migr_tw_init_from_data(&tw, tw_data, tw_size, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	/* check that we don't skip the next dirent */
	de = migr_dir_read(dir, &error);
	fail_if_error(error);
	fail_if(de == NULL);
	fail_unless(st.st_ino == de->stat->st_ino);
	migr_dir_unref(dir, de);

	/* delete all dirents */
	ret = enc_unlinkat(dir_fd, UC_FILE, ENC_DEFAULT, 0);
	fail_if(ret);

	/* deserialize */
	migr_tw_clean(&tw);
	migr_tw_init_from_data(&tw, tw_data, tw_size, &error);
	fail_if_error(error);

	/* delete dir */
	migr_tw_clean(&tw);
	path_clean(&path);
	clean_dir(dir_name);
	/* deserialize */
	migr_tw_init_from_data(&tw, tw_data, tw_size, &error);
	fail_if_error(error);

	migr_tw_clean(&tw);
	path_clean(&path);
}

TEST(treewalk_bug203702, .attrs="overnight")
{
	struct isi_error *error = NULL;
	struct migr_tw tw = {};
	struct path tmp_path = {}, dir_path = {};
	struct migr_dir *dir;
	struct migr_dirent *de;
	struct dir_slice slice = DIR_SLICE_INITIALIZER;
	struct stat st;
	char *dir_name, *tmp_name, template[MAXPATHLEN];
	ifs_snapid_t snapid = 0;
	int dir_fd, tmp_fd, fd, ret, i, file_count = 1000,
	    iterations = 1000;

	dir_name = create_new_dir();
	fail_if(dir_name == NULL);
	tmp_name = create_new_dir();
	fail_if(tmp_name == NULL);

	path_init(&tmp_path, tmp_name, NULL);
	path_init(&dir_path, dir_name, NULL);

	dir_fd = open(dir_name, O_RDONLY);
	fail_unless(dir_fd != -1);
	tmp_fd = open(tmp_name, O_RDONLY);
	fail_unless(tmp_fd != -1);

	ret = fstat(dir_fd, &st);
	fail_if(ret);

	for (i = 0;i < file_count; i++) {
		ret = snprintf(template, MAXPATHLEN, "%x", i);
		fd = enc_openat(tmp_fd, template, ENC_DEFAULT, O_CREAT|O_RDWR,
		    0770);
		fail_if(fd == -1);
		close(fd);
	}

	/* initialize tw */
	migr_tw_init(&tw, ifs_fd, &tmp_path, &slice, NULL, false, true, &error);
	fail_if_error(error);

	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);

	while (true) {
		de = migr_dir_read(dir, &error);
		fail_if_error(error);
		if (de == NULL)
			break;
		ret = enc_renameat(tmp_fd, de->dirent->d_name,
		    de->dirent->d_encoding, dir_fd, de->dirent->d_name,
		    de->dirent->d_encoding);
		fail_if(ret < 0);
		if (snapid == 0) {
		    snapid = ut_take_snapshot(dir_name, &error);
		    fail_if_error(error);
		}
	}
	close(tmp_fd);
	path_clean(&tmp_path);
	/* clean_dir frees tmp_name */
	clean_dir(tmp_name);
	close(dir_fd);

	dir_fd = ifs_lin_open(st.st_ino, snapid, O_RDONLY);
	fail_if(dir_fd == -1);
	migr_tw_init(&tw, dir_fd, &dir_path, &slice, NULL, false, false, &error);
	fail_if_error(error);
	dir = migr_tw_get_dir(&tw, &error);
	fail_if_error(error);
	for (i = 0; i < iterations; i++) {
		de = migr_dir_read(dir, &error);
		fail_if_error(error);
	}

	close(dir_fd);
	path_clean(&dir_path);
	/* clean_dir frees dir_name */
	clean_dir(dir_name);
	ut_rm_snapshot(snapid, &error);
	fail_if_error(error);
}
