#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <isi_util/isi_error.h>
#include <isi_ilog/ilog.h>

#include <check.h>
#include <isi_util/check_helper.h>

#include "isi_migrate/config/siq_util.h"
#include "linmap.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(linmap, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .timeout = 60);

TEST_FIXTURE(suite_setup)
{
	struct ilog_app_init init = {
		.full_app_name = "siq_coord",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.syslog_threshold = IL_TRACE_PLUS,
		.component = "secondary",
		.job = "",
	};
	ilog_init(&init, false, true);
	set_siq_log_level(DEBUG);
}

TEST_FIXTURE(suite_teardown)
{
}

static inline void is_logged(const char *msg)
{
	struct stat st = {0};
	const char *buffer_file = "/var/tmp/syncIQ_linmap.log";
	char cmd[256] = {0};
	sprintf(cmd, "grep '%s' /var/log/isi_migrate.log > %s", msg, buffer_file);
	system(cmd);

	fail_if(stat(buffer_file, &st));
	fail_if(st.st_size <= 0);
	fail_if(unlink(buffer_file));
}

TEST(linmap_logging)
{
	// create a linmap & open it
	const char *pol = "check-linmap-pol";
	char *mapname = "test-linmap";
	struct isi_error *err = NULL;
	struct map_ctx *mcp = NULL;

	create_linmap(pol, mapname, true, &err);
	fail_if_error(err);
	open_linmap(pol, mapname, &mcp, &err);
	fail_if_error(err);

	// create a new mapping, which should succeed and
	// Mapping set 1->2 should be logged
	set_mapping(1, 2, mcp, &err);
	fail_if_error(err);
	// verify log entry
	is_logged("Mapping set 1->2");

	// re-create above mapping again, should also succeed
	set_mapping(1, 2, mcp, &err);
	fail_if_error(err);

	// create mapping with different dlin, it should overwrite the
	// existing dlin, replacing linmap entry and mapping set 1->3
	// should be logged
	set_mapping(1, 3, mcp, &err);
	fail_if_error(err);
	is_logged("replacing linmap entry for slin: 1 old dlin: 2 new dlin: 3");
	is_logged("Mapping set 1->3");

	// destroy linmap
	close_linmap(mcp);
	remove_linmap(pol, mapname, true, &err);
	fail_if_error(err);
}

