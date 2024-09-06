#include <stdio.h>

#include "isi_migrate/migr/treewalk.h"

int
main(int argc, char *argv[])
{
	struct isi_error *error = NULL;
	struct migr_dir *mdir;
	struct migr_dirent *de;
	struct dir_slice initial, split;//, after;
	enum migr_split_result result;
	uint64_t start;
	int walk;
	DIR *dir;

	if (argc < 3 || argc > 4) {
		fprintf(stderr, "usage: %s <start> <split> [<path>]\n",
		    getprogname());
		exit(-1);
	}

	start = strtoull(argv[1], NULL, 16);
	if (start == 0) {
		fprintf(stderr, "%s: bad start\n", getprogname());
		exit(-1);
	}

	walk = strtoul(argv[2], NULL, 0);

	dir = opendir(argc == 3 ? "." : argv[3]);
	if (dir == NULL) {
		perror("opendir");
		exit(-1);
	}

	initial.begin = start;
	initial.end = DIR_SLICE_MAX;

	mdir = migr_dir_new(dirfd(dir), &initial, true, &error);
	if (error) {
		fprintf(stderr, "Failed to migr_dir_new: %s\n",
		    isi_error_get_message(error));
		exit(-1);
	}

	printf("Initializing to %016llx-%016llx\n", mdir->_slice.begin,
	    mdir->_slice.end);

	while (walk--) {
		de = migr_dir_read(mdir, &error);
		if (error) {
			fprintf(stderr, "Failed to migr_dir_read: %s\n",
			    isi_error_get_message(error));
			exit(-1);
		}

		printf("Read %016llx %s\n", de->cookie, de->dirent->d_name);

		migr_dir_unref(mdir, de);
	}

	result = migr_dir_split(mdir, &split, &error);
	if (error) {
		fprintf(stderr, "Failed to migr_dir_split: %s",
		    isi_error_get_message(error));
		exit(-1);
	}

	switch (result) {
	case MIGR_SPLIT_SUCCESS:
		printf("Split returned %016llx-%016llx\n", split.begin,
		    split.end);
		break;
	case MIGR_SPLIT_UNAVAILABLE:
		printf("Split unavailable\n");
		break;
	case MIGR_SPLIT_UNKNOWN:
		printf("Split unknown\n");
		break;
	}

	printf("Post-split to %016llx-%016llx\n", mdir->_slice.begin,
	    mdir->_slice.end);

	return 0;
}
