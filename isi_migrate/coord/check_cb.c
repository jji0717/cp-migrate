#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ifs/ifs_lin_open.h>
#include <isi_util/isi_error.h>

#include <check.h>
#include <isi_util/check_helper.h>

#include "coord.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(callback, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .timeout = 60);

extern struct coord_ctx g_ctx;

TEST_FIXTURE(suite_setup)
{
	bzero(&g_ctx, sizeof(struct coord_ctx));

	struct ilog_app_init init = {
		.full_app_name = "siq_coord",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.syslog_threshold = IL_TRACE_PLUS,
		.component = "coord",
		.job = "",
	};
	ilog_init(&init, false, true);
	set_siq_log_level(NOTICE);
}

TEST_FIXTURE(suite_teardown)
{
}

static bool
_get_error_str(struct generic_msg *m, int siqerr)
{
	char *err_str = NULL;
	bool ret = false;

	m->body.siq_err.siqerr = siqerr;
	get_error_str(m, &g_ctx, &err_str);
	if (err_str != NULL) {
		log(NOTICE, "%s", err_str);
		ret = true;
		free(err_str);
	}
	return ret;
}

#define ERR_STR_TEMPLATE "/ifs/siq_check_cb_err_str_XXXXXX"
TEST(error_string, .attrs="overnight")
{
	struct generic_msg m = {};
	struct stat dir_st, file_st;
	char job_name[] = "error_string",
	    dir_name[] = ERR_STR_TEMPLATE,
	    *tmp_name = NULL, file_name[] = "file";
	int dfd = -1, fd = -1;

	tmp_name = mkdtemp(dir_name);
	fail_if(tmp_name == NULL, "failed to create temp file with template "
	    "%s: %s", dir_name, strerror(errno));
	fail_if(stat(tmp_name, &dir_st));
	dfd = ifs_lin_open(dir_st.st_ino, HEAD_SNAPID, O_RDONLY);
	fail_if(dfd == -1, "%s", strerror(errno));
	fd = enc_openat(dfd, file_name, ENC_DEFAULT, O_CREAT|O_RDWR, 0660);
	fail_if(fd == -1, "%s", strerror(errno));

	fail_if(0 != fstat(fd, &file_st), "%s", strerror(errno));
	close(fd);

	m.head.type = ERROR_MSG;
	m.body.error.error = 0;
	m.body.error.io = 0;
	m.body.error.str = NULL;
	m.body.siq_err.nodename = job_name;
	m.body.siq_err.errstr = NULL;
	m.body.siq_err.error = EPERM;
	m.body.siq_err.dirlin = dir_st.st_ino;
	m.body.siq_err.filelin = file_st.st_ino;
	m.body.siq_err.errloc = EL_SIQ_SOURCE;

	g_ctx.curr_snap.id = HEAD_SNAPID;
	g_ctx.prev_snap.id = HEAD_SNAPID;

	fail_unless(_get_error_str(&m, E_SIQ_STALE_DIRS));
	fail_unless(_get_error_str(&m, E_SIQ_LIN_UPDT));
	fail_unless(_get_error_str(&m, E_SIQ_LIN_COMMIT));
	fail_unless(_get_error_str(&m, E_SIQ_DEL_LIN));
	fail_unless(_get_error_str(&m, E_SIQ_UNLINK_ENT));
	fail_unless(_get_error_str(&m, E_SIQ_LINK_ENT));
	fail_unless(_get_error_str(&m, E_SIQ_PPRED_REC));
	fail_unless(_get_error_str(&m, E_SIQ_CC_DEL_LIN));
	fail_unless(_get_error_str(&m, E_SIQ_CC_DIR_CHG));

	fail_unless(0 == enc_unlinkat(dfd, file_name, ENC_DEFAULT, 0));
	close(dfd);
	fail_if(-1 == rmdir(tmp_name));

	/* Bug 227181 - check dexitcode ring filter */
	fail_unless(stat(tmp_name, &dir_st) == -1);
	fail_unless(errno == ENOENT);
	dump_exitcode_ring(ENOENT);
}

