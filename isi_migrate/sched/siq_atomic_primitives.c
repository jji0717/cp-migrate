#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <stdio.h>

#include "siq.h"
#include "siq_log.h"

#include "siq_atomic_primitives.h"

int
siq_dir_move(const char *from, const char *to)
{
	int	ret;
	struct	stat sb = {0};

	log(TRACE, "siq_dir_move: enter %s -> %s", from, to);
	ret = stat(from, &sb);
	if (ret != 0) {
		log(ERROR, "siq_dir_move: stat(\"%s\") error %s",
		     from, strerror(errno));
		exit(1);
		return FAIL;
	}

	if (S_ISDIR((sb.st_mode)) == 0) {
		log(ERROR, "siq_dir_move: %s no S_ISDIR", from);
		return FAIL;
	}
	ret = rename(from, to);
	if ((ret != 0) && (errno != ENOTEMPTY)) {
		log(ERROR, "siq_dir_move: rename(%s, %s) error %s",
		     from, to, strerror(errno));
		return FAIL;
	}
	return SUCCESS;
} /* siq_dir_move */

int
siq_file_move(const char *from, const char *to) 
{
	int	ret;
	struct	stat sb;

	log(TRACE, "siq_file_move: enter");
	ret = stat(from, &sb);
	if (ret != 0) {
		log(ERROR, "siq_file_move: stat(%s) error %s",
		     from, strerror(errno));
		return ret;
	}
	if (!S_ISREG(sb.st_mode)) {
		log(ERROR, "siq_file_move: %s no S_IFREG", from);
		return -1;
	}
	errno = 0;
	ret = rename(from, to);
	if ((ret != 0) && (errno != ENOENT)) {
		log(ERROR, "siq_file_move: rename %s->%s error %s",
		     from, to, strerror(errno));
		return ret;
	}
	return 0;
} /* siq_file_move */

