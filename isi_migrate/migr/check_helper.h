#ifndef __CHECK_HELPER_H__
#define __CHECK_HELPER_H__

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <check.h>

#include <ifs/ifs_types.h>
#include <ifs/ifm/ifm_dinode.h>

#include <isi_util/check_helper.h>
#include <isi_util/isi_dir.h>
#include <isi_util/isi_extattr.h>

#include "treewalk.h"

#define fail_if_err(op) do { \
	fail_if(op, "Error: %s", strerror(errno));  			\
} while(0);

#define fail_unless_errno(code, expected) do {				\
	char errbuf[255];						\
	int _ret = strerror_r(expected, errbuf, 254);			\
	fail_unless(_ret == 0, "Could not lookup error string for %s", 	\
	    #expected);							\
	_ret = ((code));						\
	fail_unless(_ret == -1, "return = %d, expect -1(%s)", _ret,  	\
	    errbuf);							\
	fail_unless(errno == expected, "errno = '%s', expected '%s'",	\
	    strerror(errno), errbuf);					\
} while (0)

inline static char *
create_new_dir(void)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int i, ret;

	for (i = 0;; i++) {
		fmt_print(&fmt, "/ifs/test.siq.%04d", i);
		ret = mkdir(fmt_string(&fmt), 0777);
		if (ret == 0)
			break;
		else if (ret == -1)
			fail_unless(errno == EEXIST, "errno = %d", errno);
		fmt_clean(&fmt);
	}

	return strdup(fmt_string(&fmt));
}

inline static void
create_file(char *dir, const char *format, ...)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	va_list va;
	int fd;

	fmt_print(&fmt, "%s/", dir);

	va_start(va, format);
	fmt_vprint(&fmt, format, va);
	va_end(va);

	fd = open(fmt_string(&fmt), O_WRONLY | O_CREAT, 0600);
	fail_unless(fd != -1, "%s: %s", fmt_string(&fmt),
	    strerror(errno));

	close(fd);
}

inline static char *
setup_layered_dir(int dcount, int fcount)
{
	char *dir;
	int i, j;

	dir = create_new_dir();

	for (i = 0; i < dcount; i++) {
		struct fmt FMT_INIT_CLEAN(fmt);
		fmt_print(&fmt, "%s/dir_%04d", dir, i);
		mkdir(fmt_string(&fmt), 0755);
		fmt_clean(&fmt);
		fmt_print(&fmt, "dir_%04d", i);
		for (j = 0; j < fcount; j++)
			create_file(dir, "%s/file_%04d.txt", fmt_string(&fmt), j);
	}

	return dir;
}

inline static char *
setup_simple_dir(int count)
{
	char *dir;
	int i;

	dir = create_new_dir();

	for (i = 0; i < count; i++)
		create_file(dir, "file_%04d.txt", i);

	return dir;
}

inline static char *
setup_conflict_dir(int count, int conflicts)
{
	char *dir, *tmp;
	int i, j;

	fail_unless(count < 26);

	tmp = calloc(1, conflicts + 1);
	fail_unless(tmp != NULL);

	dir = create_new_dir();

	for (i = 0; i < count; i++) {
		memset(tmp, 'a' + i, conflicts);
		for (j = 0; j < conflicts; j++) {
			tmp[j] = 'A' + i;

			create_file(dir, "conflict_%s.txt", tmp);
		}
	}

	free(tmp);

	return dir;
}

inline static char *
setup_specific_dir(const char *entries[])
{
	const char **curr;
	char *dir;
	int ret;

	dir = create_new_dir();

	for (curr = entries; *curr; curr++) {
		struct fmt FMT_INIT_CLEAN(fmt);
		char *tmp_path, *end;

		tmp_path = strdup(*curr);
		for (end = tmp_path; *end; ) {
			end = strchr(end, '/');
			if (end == NULL)
				break;

			*end = 0;
			fmt_print(&fmt, "%s/%s", dir, tmp_path);
			ret = mkdir(fmt_string(&fmt), 0755);
			fail_unless(ret == 0 || errno == EEXIST,
			    "errno = %d, fmt = %s", errno, fmt_string(&fmt));

			fmt_clean(&fmt);

			*end++ = '/';
		}

		fmt_print(&fmt, "%s/%s", dir, tmp_path);
		ret = open(fmt_string(&fmt), O_WRONLY | O_CREAT, 0600);
		fail_unless(ret != -1, "errno = %d, fmt = %s",
		    errno, fmt_string(&fmt));
		close(ret);
		free(tmp_path);
	}

	return dir;
}

inline static void
clean_dir(char *dir)
{
	struct fmt FMT_INIT_CLEAN(fmt);

	fmt_print(&fmt, "rm -rf -- %s", dir);
	system(fmt_string(&fmt));
	free(dir);
}

#if 0
inline static void
print_dir(char *dir_name)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;
	struct migr_dirent *de;
	int fd, i;

	fd = open(dir_name, O_RDONLY);
	fail_unless(fd != -1);

	dir = migr_dir_new(fd, NULL);

	for (i = 0; i < 120; i++) {
		de = migr_dir_read(dir, &error);
		fail_if_error(error);

		if (de == NULL)
			break;

		printf("%016llx %s\n", de->cookie, de->dirent->d_name);
	}

	migr_dir_free(dir);

	clean_dir(dir_name);
}
#endif

inline static int
count_dirents(struct migr_dir *dir)
{
	struct isi_error *error = NULL;
	struct migr_dirent *de;
	int i = 0;

	for (;;) {
		de = migr_dir_read(dir, &error);
		fail_if_error(error);

		if (de == NULL)
			break;

		i++;
		//printf("%016llx %s %d\n", de->cookie, de->dirent->d_name, i);
		migr_dir_unref(dir, de);
	}

	return i;
}

inline static struct migr_dirent *
read_a_dirent(struct migr_dir *dir)
{
	struct isi_error *error = NULL;
	struct migr_dirent *de;

	de = migr_dir_read(dir, &error);
	fail_if_error(error);
	fail_unless(de != NULL, "premature end of directory %p", dir);

	return de;
}

inline static void
read_n_dirents(struct migr_dir *dir, int n)
{
	struct migr_dirent *de;
	int i;

	for (i = 0; i < n; i++) {
		de = read_a_dirent(dir);
		migr_dir_unref(dir, de);
	}
}

#endif /* __CHECK_HELPER_H__ */
