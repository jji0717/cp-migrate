#include <sys/types.h>
#include <sys/vnode.h>
#include <sys/stat.h>
#include <sys/isi_acl.h>
#include <ifs/ifs_types.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <ifs/btree/btree_on_disk.h>
#include <ifs/sbt/sbt.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_cbm/isi_cbm_coi.h>

#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_d_common/hsbt.h>
#include <isi_cpool_d_common/task.h>
#include <check.h>
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"

static bool g_privilege_on = false;
static const char *ctest_files_src_name = "/ifs/data/_check_access_test_.dat";
static cbm_test_file_helper *ctest_files_src = NULL;

static void set_test_proc_priv(bool state)
{
	if (state && !g_privilege_on) {
		cbm_test_enable_stub_access();
		g_privilege_on = true;
	}

	if (!state && g_privilege_on) {
		cbm_test_disable_stub_access();
		g_privilege_on = false;
	}
}

TEST_FIXTURE(suite_setup)
{
	cbm_test_common_setup(true, true);
	ctest_files_src = new cbm_test_file_helper(
		ctest_files_src_name, 100 * 1000,
		"test data for check access ....................\n");

	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(true, true);
	delete ctest_files_src;
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	/* Be sure the priv is off */
	set_test_proc_priv(false);
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_access,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .runner_data = NULL,
    .timeout = 120,
    .fixture_timeout = 1200);

#define BUF_READ_SIZE    8192

/* Utility routines. */
static int
copy_file(const char *src, const char *dest)
{
	int srcfile, destfile, rd, wr = 0;
	char *buf;

	// Allocate memory in the heap.
	buf = (char *) calloc(BUF_READ_SIZE, sizeof(char));

	// Open the source and destination files.
	fail_if ((srcfile = open(src, O_RDONLY)) < 0, "%s", strerror(errno));
	fail_if ((destfile = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0644))
	    < 0, "%s", strerror(errno));

	// Copy contents from source file to destination file.
	while ((rd = read(srcfile, buf, BUF_READ_SIZE)) > 0) {
		fail_if((wr = write(destfile, buf, rd)) < 0);
		// The read and write should be the same so we know
		// the number of bytes read and written are the same.
		fail_if(rd != wr,
		    "Bytes read : %d and bytes written : %d are not the same",
		    rd, wr);
		memset(buf, '\0', BUF_READ_SIZE);
		rd = 0, wr = 0;
	}

	fail_if(rd < 0);

	// Close the source and destination files.
	close(srcfile);
	close(destfile);

	// Free the allocated memory.
	free(buf);
	return 0;
}

struct ctest_files {
	const char *input_file;
	const char *output_file;
	const char *cloned_file;
	bool read_file;		// or copy_to_output
	bool write_file;	// or copy_to_clone
	bool prevalidate_file;	// or stub_output
	bool use_ads_file;	// or stub_clone
	bool success;
};

#define copy_to_output	read_file
#define copy_to_clone	write_file
#define stub_output	prevalidate_file
#define stub_clone	use_ads_file

/*
 * common routine to archive file
 */
static void
clone_stub_file(const char *pol_name, struct ctest_files &tfile)
{
	ifs_lin_t lin;
	int status = -1;
	struct stat statbuf;

	struct isi_error *error = NULL;

	status = unlink(tfile.output_file);
	status = unlink(tfile.cloned_file);

	if (tfile.copy_to_output) {
		status = copy_file(tfile.input_file, tfile.output_file);
		fail_if(status, "Could not perform copy of file %s",
		    tfile.output_file);

		if (tfile.stub_output){
			status = -1;
			status = stat(tfile.output_file, &statbuf);
			fail_if((status == -1),
			    "Stat of file %s failed with error: %s (%d)",
			    tfile.output_file, strerror(errno), errno);
			lin = statbuf.st_ino;

			error = cbm_test_archive_file_with_pol(lin, pol_name,
			    true);
			fail_if(error,
			    "Cannot archive %s, lin %#{}, error: %#{}",
			    tfile.output_file, lin_fmt(lin),
			    isi_error_fmt(error));
		}
	}

	if (tfile.copy_to_clone) {
		status = copy_file(tfile.input_file, tfile.cloned_file);
		fail_if(status, "Could not perform copy of file %s",
		    tfile.cloned_file);

		if (tfile.stub_clone){
			status = -1;
			status = stat(tfile.cloned_file, &statbuf);
			fail_if((status == -1),
			    "Stat of file %s failed with error: %s (%d)",
			    tfile.cloned_file, strerror(errno), errno);
			lin = statbuf.st_ino;

			error = cbm_test_archive_file_with_pol(lin, pol_name,
			    true);
			fail_if(error,
			    "Cannot archive %s, lin %#{}, error: %#{}",
			    tfile.cloned_file, lin_fmt(lin),
			    isi_error_fmt(error));
		}
	}

	struct fmt FMT_INIT_CLEAN(str);
	fmt_print(&str, "cp -c %s %s 2> /dev/null",
	    tfile.output_file, tfile.cloned_file);

	status = system(fmt_string(&str));
	if (tfile.success)
		fail_if(status, "Expected clone to succeed, but did not");
	else
		fail_if(!status, "Expected clone fail but succeeded");


	status = unlink(tfile.output_file);
	fail_if(status, "unable to cleanup test file %s",
	    tfile.output_file);

	if (tfile.copy_to_clone || tfile.success) {
		status = unlink(tfile.cloned_file);
		fail_if(status, "unable to cleanup test file %s",
		    tfile.cloned_file);
	}

}

#define TEST_MODE	0x01
#define TEST_TIME	0x02
#define TEST_SIZE	0x04

#define TEST_MODE_VALUE 0511

static void
test_attr_change(const char *pol_name, struct ctest_files &tfile, int attr)
{
	ifs_lin_t lin;
	int status = -1;
	struct stat statbuf;

	struct isi_error *error = NULL;

	status = unlink(tfile.output_file);
	fail_if((status && (errno != ENOENT)),
	    "Could not clean up old file %s before test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);

	if (tfile.copy_to_output) {

		status = copy_file(tfile.input_file, tfile.output_file);
		fail_if(status, "Could not perform copy of file %s",
		    tfile.output_file);

		if (tfile.stub_output){
			status = -1;
			status = stat(tfile.output_file, &statbuf);
			fail_if((status == -1),
			    "Stat of file %s failed with error: %s (%d)",
			    tfile.output_file, strerror(errno), errno);
			lin = statbuf.st_ino;

			error = cbm_test_archive_file_with_pol(lin, pol_name,
			    true);
			fail_if(error,
			    "Cannot archive %s, lin %#{}, error: %#{}",
			    tfile.output_file, lin_fmt(lin),
			    isi_error_fmt(error));
		}
	}

	status = stat(tfile.output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    tfile.output_file, strerror(errno), errno);

	switch(attr) {
	case TEST_MODE:
		status = chmod(tfile.output_file, TEST_MODE_VALUE);
		fail_if(status, "Failed to change file mode for %s",
		    tfile.output_file);
		break;
	case TEST_TIME:
		status = utimes(tfile.output_file, NULL);
		fail_if(status, "Failend to change file time for %s",
		    tfile.output_file);
		break;
	case TEST_SIZE:
		status = truncate(tfile.output_file, 0);
		fail_if(!status, "Did not fail truncate of %s as expected",
		    tfile.output_file);
		break;
	default:
		fail("%s: Unrecognized option for attr type %d for file %s",
		    __func__, attr, tfile.output_file);
	}

	status = unlink(tfile.output_file);
	fail_if((status),
	    "Could not clean up file %s after test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);
}


static void
test_flag_changes(const char *pol_name, struct ctest_files &tfile)
{
	ifs_lin_t lin;
	int status = -1;
	int fd = -1;
	u_int32_t new_flags;
	u_int32_t test_val;
	u_int32_t ret_val;
	struct stat statbuf;
	struct stat statbuf2;
	struct ifs_syncattr_args args;

	memset(&args, -1, sizeof(struct ifs_syncattr_args));

	args.worm.w_committed = false;
	args.secinfo = IFS_SEC_INFO_NONE;
	args.sd = NULL;

	struct isi_error *error = NULL;

	status = unlink(tfile.output_file);
	fail_if((status && (errno != ENOENT)),
	    "Could not clean up old file %s before test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);

	if (tfile.copy_to_output) {
		status = copy_file(tfile.input_file, tfile.output_file);
		fail_if(status, "Could not perform copy of file %s",
		    tfile.output_file);

		if (tfile.stub_output){
			status = -1;
			status = stat(tfile.output_file, &statbuf);
			fail_if((status == -1),
			    "Stat of file %s failed with error: %s (%d)",
			    tfile.output_file, strerror(errno), errno);
			lin = statbuf.st_ino;

			error = cbm_test_archive_file_with_pol(lin, pol_name,
			    true);
			fail_if(error,
			    "Cannot archive %s, lin %#{}, error: %#{}",
			    tfile.output_file, lin_fmt(lin),
			    isi_error_fmt(error));
		}
	}

	status = stat(tfile.output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    tfile.output_file, strerror(errno), errno);

	if (tfile.stub_output)
		new_flags = statbuf.st_flags & ~SF_FILE_STUBBED;
	else
		new_flags = statbuf.st_flags | SF_FILE_STUBBED;

	test_val = statbuf.st_flags & SF_FILE_STUB_FLAGS;

	fd = open(tfile.output_file, O_RDWR);
	fail_unless(fd > 0);

	args.fd = fd;
	args.flags = new_flags;

	status = ifs_syncattr(&args);
	fail_if(status, "syncattr failed");

	status = stat(tfile.output_file, &statbuf2);
	fail_if(status, "stat failed");

	ret_val = (statbuf2.st_flags & SF_FILE_STUB_FLAGS);

	CHECK_P("\n%s before 0x%0x after 0x%0x, test_val 0x%0x ret_val 0x%0x\n",
	    "syncattr",
	    statbuf.st_flags, statbuf2.st_flags, test_val, ret_val);

	fail_if((test_val != ret_val), "ifs_syncattr changed flags field");

	status = fchflags(fd, new_flags);
	fail_if(status, "fchflags failed");

	status = stat(tfile.output_file, &statbuf2);
	fail_if(status, "stat failed");

	ret_val = (statbuf2.st_flags & SF_FILE_STUB_FLAGS);

	CHECK_P("\n%s before 0x%0x after 0x%0x, test_val 0x%0x ret_val 0x%0x\n",
	    "fchflags",
	    statbuf.st_flags, statbuf2.st_flags, test_val, ret_val);

	fail_if((test_val != ret_val), "fchflags changed flags field");

	if (fd != -1)
		close(fd);

	status = unlink(tfile.output_file);
	fail_if((status),
	    "Could not clean up file %s after test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);
}

/*
 * common routine to archive file
 */
static void
test_io(const char *pol_name, struct ctest_files &tfile)
{
	ifs_lin_t lin;
	int file_fd = -1;
	int adsd_fd = -1;
	int attr_fd = -1;
	int fd = -1;
	int status = -1;
	char buf[1];
	const char *io_file_name = tfile.output_file;

	struct fmt FMT_INIT_CLEAN(io_file_str);

	struct stat statbuf;

	struct isi_error *error = NULL;

	status = unlink(tfile.output_file);

	status = copy_file(tfile.input_file, tfile.output_file);
	fail_if(status, "Could not perform copy of file %s", io_file_name);

	status = stat(tfile.output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    io_file_name, strerror(errno), errno);
	lin = statbuf.st_ino;

	error = cbm_test_archive_file_with_pol(lin, pol_name, true);
	fail_if(error, "Cannot archive %s, lin %#{}, error: %#{}",
	    io_file_name, lin_fmt(lin),
	    isi_error_fmt(error));

	file_fd = open(tfile.output_file, O_RDWR);
	fail_if(file_fd < 0, "cannot open file %s, %s (%d)", io_file_name,
		    strerror(errno), errno);

	if (tfile.use_ads_file) {
		/*
		 * first read the file so that the cache can be created
		 */

		struct isi_cbm_file *file_obj = NULL;
		ssize_t r;

		// Turn on priv to be able to load the cache
		set_test_proc_priv(true);

		file_obj = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
		fail_if(!file_obj, "cannot open file %s, %#{}",
		    tfile.output_file, isi_error_fmt(error));

		r = isi_cbm_file_read(file_obj, buf, 0, 1, CO_DEFAULT, &error);
		fail_if(r < 0, "failed to read CBM file %s",
		    tfile.output_file);
		fail_if(error, "cannot read file %s, %#{}",
		    tfile.output_file, isi_error_fmt(error));

		isi_cbm_file_close(file_obj, &error);
		fail_if(error, "failed to close test file on cbm read %{}",
		    isi_error_fmt(error));

		// Reset priv after load of the cache
		set_test_proc_priv(false);

		adsd_fd = enc_openat(file_fd, "." , ENC_DEFAULT,
		    O_RDONLY|O_XATTR);
		fail_if (adsd_fd < 0,
		    "cannot open ADS dir for header corruption: %s (%d)",
		    strerror(errno), errno);

		fmt_print(&io_file_str, "%s::%s", io_file_name,
		    ISI_CBM_CACHE_CACHEINFO_NAME);

		io_file_name = fmt_string(&io_file_str);

		status = enc_unlinkat(adsd_fd, ISI_CBM_CACHE_CACHEINFO_NAME,
		    ENC_DEFAULT, 0);
		fail_if((status != -1),
		    "Unlink of ADS was not blocked as expected for %s",
		    io_file_name);

		attr_fd = enc_openat(adsd_fd, ISI_CBM_CACHE_CACHEINFO_NAME,
		    ENC_DEFAULT, O_RDWR|O_SYNC);

		fail_if(attr_fd < 0,
		    "Cannot open cacheinfo ADS for header corruption: %s (%d)",
		    strerror(errno), errno);

		fd = attr_fd;

	} else {
		fd = file_fd;
	}

	if (tfile.prevalidate_file) {
		status = fstat(fd, &statbuf);
		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    io_file_name, strerror(errno), errno);
	} else if (tfile.write_file) {
		/* force write to try and invalidate inode??? */
		pwrite(fd, "a", 1, statbuf.st_size);
	}

	if (tfile.read_file) {
		status = read(fd, buf, 1);
		fail_if((status != -1),
		    "Could read file %s when we should not",
		    io_file_name);
	}

	if (tfile.write_file) {
		status = write(fd, "a", 1);
		fail_if((status != -1),
		    "Could write file %s when we should not",
		    io_file_name);
	}

	if (file_fd != -1)
		close(file_fd);

	if (adsd_fd != -1)
		close(adsd_fd);

	if (attr_fd != -1)
		close(attr_fd);

	status = unlink(tfile.output_file);
	fail_if(status, "unable to cleanup test file %s",
	    tfile.output_file);
}

static void
test_createfile(const char *pol_name, struct ctest_files &tfile)
{
	ifs_lin_t lin;
	int fd = -1;
	int status = -1;
	struct stat statbuf;

	struct isi_error *error = NULL;

	status = unlink(tfile.output_file);
	fail_if((status && (errno != ENOENT)),
	    "Could not clean up old file %s before test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);

	status = copy_file(tfile.input_file, tfile.output_file);
	fail_if(status, "Could not perform copy of file %s", tfile.output_file);

	fd = ifs_createfile(-1, tfile.output_file,
	    IFS_RTS_FILE_READ_DATA, O_TRUNC,
	    0, NULL);
	fail_if(fd == -1, "Expect no error on O_TRUNC to reg file, "
	    "found error %s (%d)",
	    strerror(errno), errno);
	if (fd != -1)
		close(fd);

	status = stat(tfile.output_file, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    tfile.output_file, strerror(errno), errno);
	lin = statbuf.st_ino;

	error = cbm_test_archive_file_with_pol(lin, pol_name, true);
	fail_if(error,
	    "Cannot archive %s, lin %#{}, error: %#{}",
	    tfile.output_file, lin_fmt(lin),
	    isi_error_fmt(error));

	fd = ifs_createfile(-1, tfile.output_file,
	    IFS_RTS_FILE_READ_DATA, O_TRUNC,
	    0, NULL);
	if (fd == -1)
		status = errno;

	fail_if(fd != -1, "Expected error on O_TRUNC to stub file");
	status = errno;
	fail_if(status != ESTUBFILE, "Expected ESTUBFILE (%d) on O_TRUNC, "
	    "but %d was found (%s)",
	    ESTUBFILE, status,  strerror(status));

	set_test_proc_priv(true);

	fd = ifs_createfile(-1, tfile.output_file,
	    IFS_RTS_FILE_READ_DATA, O_TRUNC,
	    0, NULL);
	fail_if(fd == -1, "Expect no error on O_TRUNC to stub file with priv, "
	    "found error %s (%d)",
	    strerror(errno), errno);
	if (fd != -1)
		close(fd);

	set_test_proc_priv(false);

	status = unlink(tfile.output_file);
	fail_if((status && (errno != ENOENT)),
	    "Could not clean up test file %s after test: %s (%d)",
	    tfile.output_file, strerror(errno), errno);
}

static
struct ctest_files t[] = {
	{			// 0
		ctest_files_src_name,
		"/ifs/data/_clone_file_in", "/ifs/data/_clone_file_out0",
		true, false,
		true, false,
		false,
	},
	{			// 1
		ctest_files_src_name,
		"/ifs/data/_clone_file_in", "/ifs/data/_clone_file_out1",
		true, true,
		false, true,
		false,
	},
	{			// 2
		ctest_files_src_name,
		"/ifs/data/_clone_file_in", "/ifs/data/_clone_file_out2",
		true, true,
		false, false,
		true,
	},
	{			// 3
		ctest_files_src_name,
		"/ifs/data/_clone_file_in", "/ifs/data/_clone_file_out3",
		true, false,
		true, false,
		false,
	},
	{			// 4
		ctest_files_src_name,
		"/ifs/data/_clone_file_in", "/ifs/data/_clone_file_out4",
		true, false,
		false, false,
		true,
	},
	{			// 5
		ctest_files_src_name,
		"/ifs/data/_access_mode",  "",
		true, false,
		true, false, false,
	},
	{			// 6
		ctest_files_src_name,
	        "/ifs/data/_access_time", "",
		true, false,
		true, false, false,
	},
	{			// 7
		ctest_files_src_name,
		"/ifs/data/_access_size", "",
		true, false,
		true, false, false,
	},
	{			// 8
		ctest_files_src_name,
		"/ifs/data/_read_access_test",
		NULL,
		true, false,
		false, false,
		false,
	},
	{			// 9
		ctest_files_src_name,
		"/ifs/data/_write_access_test",
		NULL,
		false, true,
		false, false,
		false,
	},
	{			// 10
		ctest_files_src_name,
		"/ifs/data/_read_preval_access_test",
		NULL,
		true, false,
		true, false,
		false,
	},
	{			// 11
		ctest_files_src_name,
		"/ifs/data/_write_preval_access_test",
		NULL,
		false, true,
		true, false,
		false,
	},
	{			// 12
		ctest_files_src_name,
		"/ifs/data/_read_ads_access_test",
		NULL,
		true, false,
		false, true,
		false,
	},
	{			// 13
		ctest_files_src_name,
		"/ifs/data/_write_ads_access_test",
		NULL,
		false, true,
		false, true,
		false,
	},
	{			// 14
		ctest_files_src_name,
		"/ifs/data/_ifs_createfile_access_test",
		NULL,
		false, false,
		false, false,
		false,
	},
};


TEST(test_create_clone_0, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	clone_stub_file(CBM_TEST_COMMON_POLICY_RAN_C, t[0]);
}

TEST(test_create_clone_1, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	clone_stub_file(CBM_TEST_COMMON_POLICY_RAN_C, t[1]);
}

TEST(test_create_clone_2, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	clone_stub_file(CBM_TEST_COMMON_POLICY_RAN_C, t[2]);
}

TEST(test_flags_change_1, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_flag_changes(CBM_TEST_COMMON_POLICY_RAN_C, t[3]);
}

TEST(test_flags_change_2, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_flag_changes(CBM_TEST_COMMON_POLICY_RAN_C, t[4]);
}

TEST(test_access_mode, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_attr_change(CBM_TEST_COMMON_POLICY_RAN_C, t[5], TEST_MODE);
}

TEST(test_access_time, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_attr_change(CBM_TEST_COMMON_POLICY_RAN_C, t[6], TEST_TIME);
}

TEST(test_access_size, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_attr_change(CBM_TEST_COMMON_POLICY_RAN_C, t[7], TEST_SIZE);
}

TEST(test_io_read, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[8]);
}

TEST(test_io_write, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[9]);
}

TEST(test_io_preval_read, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[10]);
}

TEST(test_io_preval_write, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[11]);
}

TEST(test_ads_read, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[12]);
}

TEST(test_ads_write, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_io(CBM_TEST_COMMON_POLICY_RAN_C, t[13]);
}

TEST(test_ifs_createfile, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_createfile(CBM_TEST_COMMON_POLICY_RAN_C, t[14]);
}

