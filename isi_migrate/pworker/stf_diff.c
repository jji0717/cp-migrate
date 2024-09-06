#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/acl.h>

#include <sys/isi_enc.h>
#include <isi_ufp/isi_ufp.h>

#include "pworker.h"
#include "change_compute.h"

/**
 * A helper program to exercise the synciq changeset computation code
 * outside of the pworker.
 */

int main(int argc, char **argv)
{
	struct isi_error *error = NULL;
	struct ilog_app_init init = {
		.full_app_name = "siq_stf_diff",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file= "",
		.syslog_threshold = IL_TRACE_PLUS,
		.component = "stf_diff",
		.job = "",
	};
	
	ilog_init(&init, false);
	set_siq_log_level(INFO);

	if (argc == 4) {
		if (0 != strcmp(argv[1], "i"))
			exit(1);
		create_dummy_begin(argv[2], (uint64_t)atoi(argv[3]), &error);
		if (error)
			goto out;
	} else if (argc == 5) {
		if (0 != strcmp(argv[1], "r"))
			exit(1);
		create_changeset(argv[2], (uint64_t)atoi(argv[3]),
		    (uint64_t)atoi(argv[4]), &error);
		if (error)
			goto out;
	}

 out:
	return 0;
}


