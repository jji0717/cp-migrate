#include "isi_migrate/config/siq_gconfig_gcfg.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/sched/start_policy.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>

static void
usage(void)
{
	printf("usage: domain_mark path type [-d] [-v]\n"
	    "    where type is 'synciq', 'worm', or 'snaprestore'. -d deletes "
	    "an\n"
	    "    existing domain. -v sets log level to TRACE "
	    "(isi_migrate.log)\n");
	exit(0);
}

int
main(int argc, char **argv)
{
	struct isi_error *error = NULL;
	enum domain_type type = DT_NONE;
	char *path = NULL;
	bool delete_op = false;
	bool verbose = false;
	int i;

	if (argc < 3)
		usage();

	path = argv[1];

	if (strcmp(argv[2], "synciq") == 0) {
		type = DT_SYNCIQ;
	} else if (strcmp(argv[2], "snaprestore") == 0) {
		type = DT_SNAP_REVERT;
	} else if (strcmp(argv[2], "worm") == 0) {
		type = DT_WORM;
	} else {
		usage();
	}

	for (i = 3; i < argc; i++) {
		if (strcmp(argv[i], "-d") == 0) {
			delete_op = true;
			continue;
		} else if (strcmp(argv[i], "-v") == 0) {
			verbose = true;
			continue;
		} else {
			usage();
		}
	}

	if (delete_op) {
		siq_remove_domain(path, type, &error);
		if (error)
			printf("%s\n", isi_error_get_message(error));
		goto out;
	}

	start_domain_mark(path, type, verbose, NULL);

out:
	return 0;
}
