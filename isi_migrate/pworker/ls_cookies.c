#include <stdio.h>

#include "isi_migrate/migr/treewalk.h"

int
main(int argc, char *argv[])
{
	struct isi_error *error = NULL;
	struct migr_dir *mdir;
	struct migr_dirent *de;
	DIR *dir;

	if (argc > 2) {
		fprintf(stderr, "usage: %s [<path>]\n", getprogname());
		exit(-1);
	}

	dir = opendir(argc == 1 ? "." : argv[1]);
	if (dir == NULL) {
		perror("opendir");
		exit(-1);
	}

	mdir = migr_dir_new(dirfd(dir), NULL, true, &error);
	if (error) {
		fprintf(stderr, "Failed to migr_dir_new: %s",
		    isi_error_get_message(error));
		exit(-1);
	}

	for (;;) {
		de = migr_dir_read(mdir, &error);
		if (error) {
			fprintf(stderr, "Failed to read: %s",
			    isi_error_get_message(error));
			exit(-1);
		}
		if (de == NULL)
			break;

		printf("%016llx %s\n", de->cookie, de->dirent->d_name);
	}

	return 0;
}
