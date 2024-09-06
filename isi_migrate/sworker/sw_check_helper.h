#ifndef __SW_CHECK_HELPER_H__
#define __SW_CHECK_HELPER_H__

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

#include <isi_util/check_helper.h>


inline static char *
create_new_dir(void)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int i, ret;

	for (i = 0;; i++) {
		fmt_print(&fmt, "/ifs/test.siq.sworker.%04d", i);
		ret = mkdir(fmt_string(&fmt), 0777);
		if (ret == 0)
			break;
		else if (ret == -1)
			fail_unless(errno == EEXIST, "errno = %d", errno);
		fmt_clean(&fmt);
	}

	return strdup(fmt_string(&fmt));
}

inline static int
create_filled_region(char *dir, char *file, off_t offset, size_t size)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	int fd,ret;
	char buff[1024];
	int i;

	for (i=0; i < 1024 ; i++)
		buff[i] = 'a';

	fmt_print(&fmt, "%s/%s",dir ,file);
	fd = open(fmt_string(&fmt), O_CREAT|O_RDWR, 0777);
	if (fd == -1 )
		return -1;
	ret = lseek(fd, offset, SEEK_SET);
	if (ret == -1) {
		close(fd);
		return -1;
	}
	while (size) {
		ret = write(fd, buff, 1024);
		fail_unless( ret == 1024);
		size -= ret;
	}
	return fd;
}

inline static bool
is_zero_region(int fd, off_t offset, size_t size)
{
	char buff[1024], readbuff[1024];
	int i;
	int ret;

	for (i=0; i < 1024 ; i++)
		buff[i] = 0;

	ret = lseek(fd, offset, SEEK_SET);
	fail_unless (ret == offset);
	while(size > 0) {
		ret = read(fd, readbuff, 1024);
		fail_unless(ret == 1024);
		size -= ret;
		ret = memcmp(readbuff, buff, 1024);
		if (ret != 0)
			return false;
	}

	return true;
}

#endif /* __SW_CHECK_HELPER_H__ */
