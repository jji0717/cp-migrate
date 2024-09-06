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
	printf("usage: snap_restore snapshot_id [-v]\n");
	exit(0);
}

int
main(int argc, char **argv)
{
	ifs_snapid_t snapid = INVALID_SNAPID;
	bool verbose = false;

	if (argc < 2)
		usage();

	snapid = atol(argv[1]);

	if (argc > 2) {
		if (strcmp(argv[2], "-v") == 0) {
			verbose = true;
		} else {
			usage();
		}
	}

	start_snap_revert(snapid, verbose, NULL);

	return 0;
}
