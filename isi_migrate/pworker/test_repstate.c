#include <stdio.h>
#include <errno.h>
#include <fcntl.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <isi_util/isi_error.h>

#include "pworker.h"
#include "test_helper.h"



int main(int argc, char **argv)
{
	char *job;
	char *root;
	ifs_snapid_t snapid;
	char tmp_name[80];
	uint64_t count;
	struct isi_error *error = NULL;

	if (argc == 4) {
		root = argv[1];
		job = argv[2];
		snapid = strtoll(argv[3], NULL, 10);
	} else {
		fprintf(stderr, "\nUsage: check_repstate PATH [JOBNAME SNAPID]\n");
		exit(1);
	}
	sprintf(tmp_name, "%s_snap_%llu", job, snapid);
	count = repstate_tester(root, tmp_name, &error);
	fprintf(stderr,"\n%llu entries processed\n", count);
	if (error) {
		fprintf(stderr,"\nError processing repstate\n");
		exit(-1);
	} else {
		exit(0);
	}
}
