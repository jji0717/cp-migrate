#include <fcntl.h>
#include <stdio.h>
#include <isi_domain/dom.h>
#include <isi_domain/worm.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_ilog/ilog.h>

#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"

#include <check.h>

using namespace isi_cloud;

#define DOM_DIR "/ifs/test.domain.cbm"
static ifs_domainid_t domid = 0;

#define fail_if_err(op) do {\
	int ret = op; \
	fail_if(ret != 0, "ret: %d, error: %s", ret, strerror(errno)); \
} while(0);

static void
set_worm_atime_ahead(int fd, int secs)
{
	struct timeval tv, times[2];
	struct timezone tz;

	fail_if_err(gettimeofday(&tv, &tz));

	times[0] = tv;
	times[0].tv_sec += secs;
	times[1] = tv;

	fail_if_err(futimes(fd, times));
}

static void
remove_write_priv(int fd)
{
	fail_if_err(fchmod(fd, S_IRUSR | S_IRGRP | S_IROTH));
}

static struct isi_cbm_file *
prepare_stubbed_worm_file(const char *fname, long retention_sec,
    bool commit_before_archive, isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file_obj = NULL;
	int fd = -1;
	struct stat st;
	char buf[128] = {0};

	// create a ifs file
	fd = open(fname, O_WRONLY | O_APPEND | O_CREAT, 0666);

	fail_if(fd < 0, "unable to create file");

	fail_if(write(fd, buf, sizeof(buf)) < 0,
	    "unable to write file");

	close(fd);

	if (commit_before_archive) {
		fd = open(fname, O_RDWR);
		set_worm_atime_ahead(fd, retention_sec);
		remove_write_priv(fd);
		close(fd);

		if (retention_sec == 0)
			sleep(2);
	}

	// stub file
	fail_if_err(stat(fname, &st));

	error = cbm_test_archive_file_with_pol(st.st_ino,
	    CBM_TEST_COMMON_POLICY_RAN, true);

	if (error)
		goto out;

	//
	if (!commit_before_archive) {
		fd = open(fname, O_RDWR);
		set_worm_atime_ahead(fd, retention_sec);
		remove_write_priv(fd);
		close(fd);
	}

	// open stubbed file
	file_obj = isi_cbm_file_open(st.st_ino, HEAD_SNAPID, 0, &error);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return file_obj;
}


TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_worm",			// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_worm",			// syslog program
		IL_TRACE_PLUS|IL_CP_EXT,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_worm.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif
	cbm_test_common_cleanup(true, true);
	system(CBM_ACCESS_ON "rm -rf " DOM_DIR CBM_ACCESS_OFF);
	fail_if_err(mkdir(DOM_DIR, 0777));
	fail_if_err(worm_create(DOM_DIR, &domid));
	fail_if_err(dom_mark_ready(domid));
	fail_if_err(dom_toggle_privdel(domid, true));

	cbm_test_common_setup(true, true);
}

TEST_FIXTURE(suite_teardown)
{
	fail_if_err(dom_remove_domain(domid));
	system(CBM_ACCESS_ON "rm -rf " DOM_DIR CBM_ACCESS_OFF);
	cbm_test_common_cleanup(true, true);
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_worm,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST(test_domain_stub)
{
	struct isi_error *error = NULL;

	// stub the committed and expired file should fail
	struct isi_cbm_file *file = prepare_stubbed_worm_file(
	    DOM_DIR "/file_to_stub.txt", 0, true, &error);

	fail_if(!error, "not expect to stub the worm committed file");

	isi_error_free(error);
	error = NULL;

	// stub the committed file should fail
	file = prepare_stubbed_worm_file(
	    DOM_DIR "/file_to_stub1.txt", 3600, true, &error);

	fail_if(!error, "not expect to stub the worm committed file");

	isi_error_free(error);
	error = NULL;

	// stub the un-committed file should succeed
	file = prepare_stubbed_worm_file(
	    DOM_DIR "/file_to_stub2.txt", 3600, false, &error);

	fail_if(error, "expect to stub the none worm committed file: %{}",
	    isi_error_fmt(error));

	isi_cbm_file_close(file, &error);

	fail_if(error, "unable to close file: %{}", isi_error_fmt(error));

	char cmd[MAXPATHLEN];
	memset(cmd, '\0', MAXPATHLEN);
	sprintf(cmd, "%s rm -f %s/file_to_stub.txt %s",
	    CBM_ACCESS_ON, DOM_DIR, CBM_ACCESS_OFF);
	system(cmd);
}

TEST(test_worm_modify, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	char buf[128];

	// wrtie a committed file should fail
	struct isi_cbm_file *file_obj = prepare_stubbed_worm_file(
	    DOM_DIR "/file_to_modify.txt", 3600*24, false, &error);

	fail_if(error, "unable to prepare the file: %{}",
	    isi_error_fmt(error));

	isi_cbm_file_write(file_obj, buf, 0, sizeof(buf), &error);

	fail_if(!error || !isi_error_is_a(error, CBM_ERROR_CLASS) ||
	    isi_cbm_error_get_type(error) != CBM_PERM_ERROR,
	    "not supposed to able to modify file with WORM");

	// free so not to leak mem
	if (error)
		isi_error_free(error);

	isi_cbm_file_close(file_obj, &error);
}

TEST(test_worm_read, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	char buf[128];

	struct isi_cbm_file *file_obj = prepare_stubbed_worm_file(
	    DOM_DIR "/file_to_read.txt", 3600*24, false, &error);

	fail_if(error, "unable to prepare the file: %{}",
	    isi_error_fmt(error));

	// cache read fail
	isi_cbm_file_read(file_obj, buf, 0, sizeof(buf), CO_CACHE, &error);

	fail_if(error, "failed to read the worm protected file.");

	isi_cbm_file_close(file_obj, &error);
}

