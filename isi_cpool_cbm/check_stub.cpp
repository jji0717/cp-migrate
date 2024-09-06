#include <ifs/ifs_types.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <ifs/btree/btree_on_disk.h>
#include <ifs/sbt/sbt.h>
#include <sys/extattr.h>
#include <sys/stat.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_cbm/isi_cbm_coi.h>
#include <isi_cpool_cbm/isi_cbm_file.h>

#include <isi_cpool_cbm/isi_cbm_coi_sbt.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>

#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_d_common/task.h>
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_mapper.h"

#include <check.h>

#define TEST_LOOP_LIMIT "/ifs/data/check_stub_limit"
#define BUF_READ_SIZE    		8192
#define ARCHIVE_RETRY_ATTEMPTS		2

static struct test_file_info test_files[] = {
        // normal single chunk
	{"/etc/services", "/ifs/data/foobar1", REPLACE_FILE},

	// already stubbed (error condition)
	{"/etc/services", "/ifs/data/foobar1", LEAVE_FILE},

	// normal, same file new name
	{"/etc/services", "/ifs/data/foobar2", REPLACE_FILE},

	// normal, multiple chunks
	{"/usr/lib/libz.so", "/ifs/data/foobar3", REPLACE_FILE},

	// overwrite previous file and restub
	{"/usr/lib/libz.so", "/ifs/data/foobar3", REPLACE_FILE},

	// no file present (error condition)
	{"/etc/system_update_time", "/ifs/data/foobar4", REMOVE_FILE},

	// normal, zero length file (no chunks)
	{"/etc/system_update_time", "/ifs/data/foobar4", REPLACE_FILE},
};

static int files_to_stub = sizeof (test_files)/sizeof (struct test_file_info);

#define MEMORY_CHECK_SBT_SIZE 8192

class StubMemCKPT
{
public:
	StubMemCKPT ()
	{
		reset();
	}
	static bool get_ckpt(const void *opaque,
	    void **out_stream, size_t *size_ckpt,
	    djob_op_job_task_state *state, off_t offset)
	{
		num_of_reads_++;
		*state = DJOB_OJT_NONE;

		if (num_stored_ == 0) {
			*out_stream = NULL;
			*size_ckpt = 0;
			return true;
		}
		*out_stream = malloc(num_stored_);
		memcpy((void *) *out_stream, buffer_, num_stored_);
		*size_ckpt = num_stored_;
		return true;
	}
	static bool set_ckpt(void *opaque, void *in_stream,
	    size_t stream_size, djob_op_job_task_state *state, off_t offset)
	{
		num_of_writes_ ++;
		*state = DJOB_OJT_NONE;

		if (stream_size > 0 && stream_size < MEMORY_CHECK_SBT_SIZE) {
			memcpy(buffer_, in_stream, stream_size);
			num_stored_ = stream_size;
			return true;
		}
		num_stored_ = 0;
		memset(buffer_,0, MEMORY_CHECK_SBT_SIZE);
		return false;

	}
	static void reset()
	{
		num_stored_ = 0;
		num_of_writes_ = 0;
		num_of_reads_ = 0;
		memset(buffer_,0, MEMORY_CHECK_SBT_SIZE);
	}
private:
	static size_t num_stored_;
	static int num_of_writes_;
	static int num_of_reads_;
	static char buffer_[MEMORY_CHECK_SBT_SIZE];

};
char  StubMemCKPT::buffer_[MEMORY_CHECK_SBT_SIZE];
size_t StubMemCKPT::num_stored_ = 0;
int StubMemCKPT::num_of_writes_ = 0;
int StubMemCKPT::num_of_reads_ = 0;

#define FP_VALUE_ON  "return(1)"
#define FP_VALUE_OFF "off"

#define SYSCTL_CMD(a,v)	(a) = (void *)(v); a##_sz = strlen(v);

/*
 * failpoints to test.  Note that FP_NO_EXTEND has its own test set
 * so that cleanup of partial invalidate can be validated.
 */
#define FP_NO_TRUNC		"efs.fail_point.cpool_stub_no_truncate"
#define FP_NO_TRUNC_ATTR	"efs.fail_point.cpool_stub_no_truncate_attr_set"
#define FP_NO_EXTEND		"efs.fail_point.cpool_stub_no_extend"
#define FP_STUB_TRUNC_RETRY	"efs.fail_point.cpool_stub_truncate_attr_retry"
#define FP_POST_EXTEND		"efs.fail_point.cpool_stub_postextend"
#define FP_SKIP_TRUNC		"efs.fail_point.cpool_stub_skip_invalidate"
#define FP_FAIL_FLUSH		"efs.fail_point.cpool_stub_fail_cache_flush"

static int
test_failpoint_onoff(const char *failpoint, bool on)
{
	void *sysctl_cmd = NULL;
	size_t sysctl_cmd_sz;
	const char *v = (on ? FP_VALUE_ON : FP_VALUE_OFF);

	SYSCTL_CMD(sysctl_cmd, v);
	return sysctlbyname(failpoint, NULL, NULL, sysctl_cmd, sysctl_cmd_sz);
}

void
clear_failpoints()
{
	test_failpoint_onoff(FP_NO_TRUNC, false);
	test_failpoint_onoff(FP_NO_TRUNC_ATTR, false);
	test_failpoint_onoff(FP_NO_EXTEND, false);
	test_failpoint_onoff(FP_STUB_TRUNC_RETRY, false);
	test_failpoint_onoff(FP_POST_EXTEND, false);
	test_failpoint_onoff(FP_SKIP_TRUNC, false);
	test_failpoint_onoff(FP_FAIL_FLUSH, false);
}

TEST_FIXTURE(suite_setup)
{
#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_stub",			// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_stub",			// Syslog program
		IL_TRACE_PLUS|IL_DETAILS|IL_CP_EXT,	// default level
		false,				// use syslog
		false,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_stub.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL) {
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif

	cbm_test_common_setup(true, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(true, true);
	clear_failpoints();
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	/*
	 * Remove the old files. This is done as a seperate loop as the loop
	 * below uses the files stubbed in one iteration in the next iteration
	 * for testing.
	 */
	for (int i = 0; i < files_to_stub; i++) {
		// Delete the file after the test.
		unlink(test_files[i].output_file);
	}

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_stub,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF
#define RM_STRING	CBM_ACCESS_ON "rm -f %s" CBM_ACCESS_OFF


/* Utility routines. */
int copy_file(const char *src, const char *dest)
{
	int srcfile, destfile, rd, wr = 0;
	char *buf;

	// Allocate memory in the heap.
	buf = (char *) calloc(BUF_READ_SIZE, sizeof(char));

	// Open the source and destination files.
	fail_if ((srcfile = open(src, O_RDONLY)) < 0,
	    "failed to open %s: %s (%d)", src, strerror(errno), errno);
	fail_if ((destfile = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0644))
	    < 0, "failed to open %s: %s (%d)", dest, strerror(errno), errno);

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
	fsync(destfile);
	close(destfile);

	// Free the allocated memory.
	free(buf);
	return 0;
}

/*
 * Check the extended attributes stored with a stub against expected results
 * NB: check_ctime option will print a warning rather than fail the check.
 */
static void
validate_stub_ea(const char *filename, struct stat *buf, int expected_flags,
    bool check_ctime)
{
	off_t       user_size;
	int         cp_flags;
	int         status;
	struct stat buf2;

#if 0 /* Not currently using shadow mtime */
	timespec user_mtime;

	/* Check mtime */
	fail_if(extattr_get_file(filename, ISI_CFM_MAP_ATTR_NAMESPACE,
		ISI_CFM_USER_ATTR_MTIME, &user_mtime, sizeof(user_mtime)) < 0,
	    "Could not get cpool mtime for %s from EA, %s (%d)",
	    filename, strerror(errno), errno);

	fail_if(((user_mtime.tv_sec != buf->st_mtimespec.tv_sec) ||
		(user_mtime.tv_nsec != buf->st_mtimespec.tv_nsec)),
	    "system and EA time do not compare for %s"
	    ", utime (%ld, %ld), cptime (%ld, %ld)",
	    filename,
	    user_mtime.tv_sec, user_mtime.tv_nsec,
	    buf->st_mtimespec.tv_sec, buf->st_mtimespec.tv_nsec);

	CHECK_P("\n%s: filename %s, utime (%ld,%ld), cptime (%ld, %ld)",
	    __func__, filename, user_mtime.tv_sec, user_mtime.tv_nsec,
	    buf->st_mtimespec.tv_sec, buf->st_mtimespec.tv_nsec);
#endif
	/* Check times and size in stat */
	status = stat(filename, &buf2);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	fail_if(((buf2.st_mtimespec.tv_sec != buf->st_mtimespec.tv_sec) ||
		(buf2.st_mtimespec.tv_nsec != buf->st_mtimespec.tv_nsec)),
	    "mtime does not compare for %s, old (%ld,%ld), new (%ld,%ld)",
	    filename, buf->st_mtimespec.tv_sec, buf->st_mtimespec.tv_nsec,
	    buf2.st_mtimespec.tv_sec, buf2.st_mtimespec.tv_nsec);

	if (check_ctime) {
		fail_if(((buf2.st_ctimespec.tv_sec != buf->st_ctimespec.tv_sec) ||
			(buf2.st_ctimespec.tv_nsec != buf->st_ctimespec.tv_nsec)),
		    "ctime does not compare for %s,"
		    " old (%ld,%ld), new (%ld,%ld)",
		    filename,
		    buf->st_ctimespec.tv_sec, buf->st_ctimespec.tv_nsec,
		    buf2.st_ctimespec.tv_sec, buf2.st_ctimespec.tv_nsec);
	} else if((buf2.st_ctimespec.tv_sec != buf->st_ctimespec.tv_sec) ||
	    (buf2.st_ctimespec.tv_nsec != buf->st_ctimespec.tv_nsec)) {
		printf("Info: "
		    "ctime does not compare for %s,"
		    " old (%ld,%ld), new (%ld,%ld)\n",
		    filename,
		    buf->st_ctimespec.tv_sec, buf->st_ctimespec.tv_nsec,
		    buf2.st_ctimespec.tv_sec, buf2.st_ctimespec.tv_nsec);
	}

	fail_if((buf2.st_size != buf->st_size),
	    "size does not compare for %s, old (%ld), new (%ld)",
	    filename, buf->st_size, buf2.st_size);

	/* Check size */
	fail_if(extattr_get_file(filename, ISI_CFM_MAP_ATTR_NAMESPACE,
	    ISI_CFM_USER_ATTR_SIZE, &user_size, sizeof(user_size)) < 0,
	    "Could not get cpool size for %s from EA, %s (%d)",
	    filename, strerror(errno), errno);

	fail_if((user_size != buf->st_size),
	    "system and EA size do not compare for %s, %s (%d)", filename);

	CHECK_P("\n%s: filename %s, usize %ld, cpsize %ld",
	    __func__, filename, user_size, buf->st_size);

	/* Check flags */
	fail_if(extattr_get_file(filename, ISI_CFM_MAP_ATTR_NAMESPACE,
	    ISI_CFM_USER_ATTR_FLAGS, &cp_flags, sizeof(cp_flags)) < 0,
	    "Could not get cpool flags for %s from EA, %s (%d)",
	    filename, strerror(errno), errno);

	fail_if((cp_flags != expected_flags),
	    "cpools flags (%d) do not compare to expected value (%d) for %s",
	    cp_flags, expected_flags, filename);

	CHECK_P("\n%s: filename %s, expected flags 0x%08x, cpflags 0x%08x\n",
	    __func__, filename, cp_flags, expected_flags);
}

/*
 * common routine to archive file
 */
static void
archive_test_files(const char *pol_name)
{
	ifs_lin_t lin;
	int status = -1;
	int limit = -1;
	struct stat statbuf;

	struct isi_error *error = NULL;
	int i;

	bool check_eas = false;
	int tries = ARCHIVE_RETRY_ATTEMPTS;
	bool first_try = true;


	int tfd = open(TEST_LOOP_LIMIT, O_RDONLY);
	if (tfd != -1) {
		char tbuf[16];
		size_t res = read(tfd, tbuf, 8);
		if (res != 1) {
			limit = atoi(tbuf);
		}
		close(tfd);
	}


	for (i = 0; i < files_to_stub; i++) {
		if (limit >= 0 && i > limit) {
			fprintf(stderr,
			    "\n\n***Stopping test as request at %d***\n\n",
			    limit);
			break;
		}

		switch(test_files[i].copy_file) {
		case REPLACE_FILE:
		case REMOVE_FILE:
			status = unlink(test_files[i].output_file);
			status = copy_file(test_files[i].input_file,
			    test_files[i].output_file);
			break;
		case LEAVE_FILE:
			CHECK_P("\n leaving file %s\n", test_files[i].output_file);
			status = 0;
			break;
		default:
			fail("Unexpected copy file status");
			break;
		}

		fail_if(status, "Could not perform copy of file %s",
		    test_files[i].output_file);

		status = -1;

		status = stat(test_files[i].output_file, &statbuf);
		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    test_files[i].output_file, strerror(errno), errno);
		lin = statbuf.st_ino;

		if (test_files[i].copy_file == REMOVE_FILE) {
			status = unlink(test_files[i].output_file);
		}

		check_eas = true;
		tries = ARCHIVE_RETRY_ATTEMPTS;
		first_try = true;

		while (tries--) {
			/*
			 * if this is the first time we already have the stats
			 * we need.  If this a retry, then we need to get them
			 * again....
			 */
			if (first_try) {
				first_try = false;
			} else if (test_files[i].copy_file != REMOVE_FILE) {
				status = stat(test_files[i].output_file,
				    &statbuf);
				fail_if((status == -1),
				    "Stat of file %s failed with error: "
				    "%s (%d)",
				    test_files[i].output_file,
				    strerror(errno), errno);
			}

			/*
			 * do not do auto retries here as we want to get the
			 * stat for the time compares only when here is no
			 * coalescer flush that happens (when ther is no
			 * CBM_STATE_STUB_ERROR returned).
			 */
			error = cbm_test_archive_file_with_pol(lin, pol_name,
			    false);

			if (error && tries &&
			    isi_cbm_error_is_a(error, CBM_STALE_STUB_ERROR)) {
				isi_error_free(error);
				error = NULL;
			} else {
				tries = 0;
			}
		}

#ifdef CHECK_CBM_ENABLE_TRACE
		if (error) {
			struct fmt FMT_INIT_CLEAN(fmttmp);
			fmt_print(&fmttmp, "\nfile %s error %{}\n",
			    test_files[i].output_file, isi_error_fmt(error));
			CHECK_P(fmt_string(&fmttmp));
		} else {
			CHECK_P("\nfile %s, success\n",
			    test_files[i].output_file);
		}
#endif

		switch (test_files[i].copy_file) {
		case REPLACE_FILE:
			fail_if(error,
			    "Cannot archive %s, lin %#{}, error: %#{}",
			    test_files[i].output_file, lin_fmt(lin),
			    isi_error_fmt(error));
			break;
		case REMOVE_FILE:
			fail_if(!error,
			    "Expected error on file not found %s, lin %#{}",
			    test_files[i].output_file, lin_fmt(lin));

			fail_if(!isi_system_error_is_a(error, ENOENT),
			    "Expected ENOENT for already stubbed %s, "
			    "lin %#{}, got %#{}",
			    test_files[i].output_file, lin_fmt(lin),
			    isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;
			check_eas = false;
			break;
		case LEAVE_FILE:
			fail_if(!error,
			    "Expected error on file already stubbed %s, "
			    "lin %#{}",
			    test_files[i].output_file, lin_fmt(lin));

			fail_if(!isi_cbm_error_is_a(error, CBM_ALREADY_STUBBED),
			    "Expected CBM_ALREADY_STUBBED "
			    "for already stubbed %s, "
			    "lin %#{} got %#{}",
			    test_files[i].output_file, lin_fmt(lin),
			    isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;
			break;
			// default case already handled as error above
		}
		if (check_eas)
			validate_stub_ea(test_files[i].output_file,
			    &statbuf, IFS_CPOOL_FLAGS_STUBBED, true);
	}
}

TEST(test_put_file,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	archive_test_files(CBM_TEST_COMMON_POLICY_RAN);
}

TEST(test_put_file_with_compression,  mem_check : CK_MEM_DEFAULT,
    mem_hint : 0)
{
	archive_test_files(CBM_TEST_COMMON_POLICY_RAN_C);
}

TEST(test_archive_fail_on_policy_delete, mem_check : CK_MEM_DEFAULT)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_archive_fail_on_policy_delete.txt";

	cbm_test_file_helper test_fo(path, 0, "a few words");
	ifs_lin_t lin = test_fo.get_lin();

	const char * deleted_policy = "A_POLICY_DOESNOT_EXIST";
	error = cbm_test_archive_file_with_pol(lin,
	    deleted_policy, false);

	fail_if(!isi_cbm_error_is_a(error, CBM_FILE_CANCELLED),
	    "failed to cancel the stub when policy name %s was not found, %#{}",
	    deleted_policy,
	    isi_error_fmt(error));
	isi_error_free(error);
}

struct stub_lock_test_context {
	const char *path; // file to be locked by child thread
	int fd; // set by child thread, to be freed by parent
	scoped_stub_lock stub_lock; // holding lock on fd
};


/* disabled memcheck due to 392 bytes leak from fake_auth_task_map */
TEST(test_archive_fail_on_stub_lock, mem_check : CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_archive_fail_on_stub_lock.txt";
	long tid;

	cbm_test_file_helper test_fo(path, 0, "a few words");
	ifs_lin_t lin = test_fo.get_lin();

	/*
	 * Emulate a stub lock being taken on the file from a different
	 * thread.
	 */
	int fd = open(path, O_RDONLY);
	struct flock oflock;
	oflock.l_start = 0;
	oflock.l_len = 0;
	oflock.l_pid = getpid();
	oflock.l_type = CPOOL_LK_SR;
	oflock.l_whence = SEEK_SET;
	oflock.l_sysid = 0;
	int ierror = thr_self(&tid);

	if (ierror)
		fail("Failed to retrieve thread id.");

	ierror = ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_STUB,
	    F_SETLK, &oflock, F_SETLKW, false, 0,
	    (uint32_t) getpid(), (uint32_t) ((uint64_t) (tid + 1) & 0xFFFFFFFF));

	if (ierror)
		fail("Failed taking stub lock errno =%d", errno);

	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, false);

	if (!error || !isi_cbm_error_is_a(error, CBM_DOMAIN_LOCK_CONTENTION))
		fail("expect domain lock error to archive the file.");

	if (error)
		isi_error_free(error);

	close(fd);
}


static void test_checkpoint(const char *failpoint, bool retry,
    bool expect_first_trunc, u_int32_t expected_flags, const char *filename)
{
	ifs_lin_t lin;
	int status = -1;
	int result = 0;
	bool found = false;
	int count = 0;
	struct stat statbuf, statafter;

	struct isi_error *error = NULL;
	struct coi_sbt_entry hsbt_ent;
	const char *coi_name = NULL;
	int sbt_fd = -1;
	btree_key_t key = {{ 0, 0 }}, next_key;
	struct sbt_entry *sbt_ent = NULL;

	char entry_buf[SBT_MAX_ENTRY_SIZE_128];
	size_t num_ent_out;
	std::string cmo;

	isi_cloud::coi_entry ce;
	std::set<ifs_lin_t> referencing_lins;
	std::set<ifs_lin_t>::const_iterator iter;

	StubMemCKPT memckpt;
	memckpt.reset();

	static struct test_file_info this_test_file =  {

		"/boot/kernel.amd64/kernel.gz",
		"/ifs/data/_check_stub_trinv_1",
		REPLACE_FILE
	};

	status = unlink(filename);
	status = copy_file(this_test_file.input_file,
	    filename);
	fail_if(status, "Could not perform copy of file %s",
	    filename);

	status = -1;

	status = stat(filename, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);
	lin = statbuf.st_ino;

	status = test_failpoint_onoff(failpoint, true);
	fail_if(status == -1, "Cannot set failpoint for %s: %s (%d)",
	    failpoint, strerror(errno), errno);

	if (retry) {
		error = cbm_test_archive_file_with_pol_and_ckpt(lin,
		    CBM_TEST_COMMON_POLICY_RAN,
		    memckpt.get_ckpt, memckpt.set_ckpt, true);
		fail_if((!error || !isi_cbm_error_is_a(error,
			    CBM_STUB_INVALIDATE_ERROR)),
		    "Expected CBM_STUB_INVALIDATE_ERROR from failpoint, "
		    "got %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;

		status = test_failpoint_onoff(failpoint, false);
		fail_if(status == -1, "Cannot clear failpoint for %s: %s (%d)",
		    failpoint, strerror(errno), errno);

		status = stat(filename, &statafter);
		fail_if((status == -1),
		    "Stat of file %s after 1st FP failed with error: %s (%d)",
		    filename, strerror(errno), errno);

		if (expect_first_trunc) {
			fail_if((statafter.st_blocks >= statbuf.st_blocks),
			    "First invalidate failed, "
			    "blocks before: %ld, blocks after %ld",
			    statbuf.st_blocks, statafter.st_blocks);
		} else {
			fail_if((statafter.st_blocks < statbuf.st_blocks),
			    "First invalidate should not have succeeded, "
			    "blocks before: %ld, blocks after %ld",
			    statbuf.st_blocks, statafter.st_blocks);
		}

		fail_if((statafter.st_size != statbuf.st_size),
		    "Size should not change after failed first truncate/extend, "
		    "size before: %ld, size after %ld",
		    statbuf.st_size, statafter.st_size);

		error = cbm_test_archive_file_with_pol_and_ckpt(lin,
		    CBM_TEST_COMMON_POLICY_RAN,
		    memckpt.get_ckpt, memckpt.set_ckpt, false);

		if (error && isi_system_error_is_a(error, EINVAL)) {
			isi_error_free(error);
			error = NULL;

			error = cbm_test_archive_file_with_pol_and_ckpt(lin,
			    CBM_TEST_COMMON_POLICY_RAN,
			    memckpt.get_ckpt, memckpt.set_ckpt, false);
		}

		fail_if(error,
		    "Restart of archive after without truncate failed %#{}",
		    isi_error_fmt(error));

		status = stat(filename, &statafter);
		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    filename, strerror(errno), errno);

		fail_if((statafter.st_blocks >= statbuf.st_blocks),
		    "Invalidate did not succeed, "
		    "blocks before: %ld, blocks after %ld",
		    statbuf.st_blocks, statafter.st_blocks);

		fail_if((statafter.st_size != statbuf.st_size),
		    "Size should not change after successful truncate/extend, "
		    "size before: %ld, size after %ld",
		    statbuf.st_size, statafter.st_size);

	} else {
		error = cbm_test_archive_file_with_pol_and_ckpt(lin,
		    CBM_TEST_COMMON_POLICY_RAN,
		    memckpt.get_ckpt, memckpt.set_ckpt, true);
		fail_if(error,
		    "Unexpected error archiving %s, %#{}",
		    isi_error_fmt(error));

		status = test_failpoint_onoff(failpoint, false);
		fail_if(status == -1, "Cannot clear failpoint for %s: %s (%d)",
		    failpoint, strerror(errno), errno);

		status = stat(filename, &statafter);
		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    filename, strerror(errno), errno);

		if (expect_first_trunc) {
			fail_if((statafter.st_blocks >= statbuf.st_blocks),
			    "Invalidate did not succeed, "
			    "blocks before: %ld, blocks after %ld",
			    statbuf.st_blocks, statafter.st_blocks);
		} else {
			/* # blocks will increase due to add of EA for stub map */
			fail_if((statafter.st_blocks < statbuf.st_blocks),
			    "Invalidate should not have succeeded, "
			    "blocks before: %ld, blocks after %ld",
			    statbuf.st_blocks, statafter.st_blocks);
		}

		fail_if((statafter.st_size != statbuf.st_size),
		    "Size should not change after truncate/extend, "
		    "size before: %ld, size after %ld",
		    statbuf.st_size, statafter.st_size);
	}

	/* Now look at COI queue */

	coi_name = get_coi_sbt_name();
	sbt_fd = open_sbt(coi_name, false, &error);
	fail_if(error, "Could not open COI sbt");
	while (result == 0 && !found) {
		result = ifs_sbt_get_entries(sbt_fd, &key, &next_key,
		    sizeof entry_buf, entry_buf, 1, &num_ent_out);

		fail_if((num_ent_out == 0), "No COI entries found");
		fail_if((result == -1), "Error retrieving element: %s\n",
		    strerror(errno));

		sbt_ent = (struct sbt_entry *)entry_buf;

		sbt_entry_to_coi_sbt_entry(sbt_ent, &hsbt_ent);

		fail_if(!hsbt_ent.valid_, "Found invalid hstb entry");

		ce.set_serialized_entry(CPOOL_COI_VERSION, hsbt_ent.value_,
		    hsbt_ent.value_length_, &error);
		fail_if(error != NULL, "Error setting serialized entry %{}\n",
		    isi_error_fmt(error));

		referencing_lins = ce.get_referencing_lins();

		iter = referencing_lins.begin();

		if (referencing_lins.size() != 0) {
			for (count = 0; iter != referencing_lins.end();
			     ++iter) {
				count++;
				if (*iter == lin) {
					found = true;
				}
			}
		}

		if (found && count == 1)  { //clean up if only lin in entry
			result = ifs_sbt_remove_entry(sbt_fd, &key);
			fail_if((result != 0 && errno != ENOENT),
			    "Error deleting element: %s\n",
			    strerror(errno));
		}
		key = next_key;

		coi_sbt_entry_destroy(&hsbt_ent);

	}

	fail_if(!found, "No COI entry found.");

	/*
	 * XXXegc: Note that check_ctime (4th param) is set to false for now.
	 * This is because the checkpoint operation is causing the
	 * invalidation which can cause the ADS directory to be created even
	 * if there is no cache.  This can cause ctime to advance on the inode
	 * currently.
	 */
	validate_stub_ea(filename, &statbuf, expected_flags, false);

#ifndef CHECK_CBM_ENABLE_TRACE
	status = unlink(filename);
	fail_if(status, "Could not perform cleanup of file %s",
		    filename);
#endif
}

static void
test_writecache_handling(const char *failpoint, bool writecached,
    int expected_flags, const char *filename)
{
	ifs_lin_t lin;
	int status = -1;
	struct stat statbefore, statafter;

	struct isi_error *error = NULL;
	bool fail = true;

	StubMemCKPT memckpt;
	memckpt.reset();

	static struct test_file_info this_test_file =  {

		"/boot/kernel.amd64/kernel.gz",
		"/ifs/data/_check_stub_trinv_1",
		REPLACE_FILE
	};

	status = unlink(filename);
	status = copy_file(this_test_file.input_file,
	    filename);
	fail_if(status, "Could not perform copy of file %s",
	    filename);

	status = -1;

	status = stat(filename, &statbefore);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);
	lin = statbefore.st_ino;

	CHECK_P("\nBefore stats %x mtime (%ld,%ld) ctime (%ld,%ld) for %s\n",
	    statbefore.st_flags,
	    statbefore.st_mtimespec.tv_sec, statbefore.st_mtimespec.tv_nsec,
	    statbefore.st_ctimespec.tv_sec, statbefore.st_ctimespec.tv_nsec,
	    filename);

	if (!writecached) {
		struct fmt FMT_INIT_CLEAN(test_cmd);

		fmt_print(&test_cmd, "isi set -c off -L %{}",
		    lin_fmt(statbefore.st_ino));

		CHECK_P("\nIssuing WRITECACHE reset command of %s\n",
		    fmt_string(&test_cmd));

		status = system(fmt_string(&test_cmd));
		fail_if(status != 0, "Failed to reset WRITECACHE %s: (%d)",
		    filename, status);

		status = stat(filename, &statafter);
		fail_if((status == -1),
		    "Stat of file %s failed with error: %s (%d)",
		    filename, strerror(errno), errno);

		CHECK_P("changed flags from (%x) to (%x) "
		    "mtime (%ld,%ld) ctime (%ld,%ld) for %s\n",
		    statbefore.st_flags, statafter.st_flags,
		    statafter.st_mtimespec.tv_sec,
		    statafter.st_mtimespec.tv_nsec,
		    statafter.st_ctimespec.tv_sec,
		    statafter.st_ctimespec.tv_nsec,
		    filename);

		statbefore = statafter;

		CHECK_P("flags before (%x)",  statbefore.st_flags);
	}

	status = test_failpoint_onoff(failpoint, true);
	fail_if(status == -1, "Cannot set failpoint for %s: %s (%d)",
	    failpoint, strerror(errno), errno);

	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt,
	    false);

	fail_if((!error || !isi_cbm_error_is_a(error, CBM_READ_ONLY_ERROR)),
	    "Expected CBM_READ_ONLY_ERROR from failpoint");
	isi_error_free(error);
	error = NULL;

	status = stat(filename, &statafter);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	CHECK_P("Pass 1 stats %x mtime (%ld,%ld) ctime (%ld,%ld) for %s\n",
	    statafter.st_flags,
	    statafter.st_mtimespec.tv_sec, statafter.st_mtimespec.tv_nsec,
	    statafter.st_ctimespec.tv_sec, statafter.st_ctimespec.tv_nsec,
	    filename);

	if (writecached)
		fail = ((statafter.st_flags & UF_WRITECACHE) ||
		    (statafter.st_flags & SF_FILE_STUBBED));
	else
		fail = ((statafter.st_flags & UF_WRITECACHE) ||
		    (statafter.st_flags & SF_FILE_STUBBED));

	fail_if(fail, "Stat flags not set as expected for first pass, "
	    "before (%x) and after (%x)",
	    statbefore.st_flags, statafter.st_flags);

	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt,
	    false);
	fail_if((!error || !isi_cbm_error_is_a(error, CBM_READ_ONLY_ERROR)),
	    "Expected CBM_READ_ONLY_ERROR from failpoint");
	isi_error_free(error);
	error = NULL;

	status = stat(filename, &statafter);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	CHECK_P("Pass 2 stats %x mtime (%ld,%ld) ctime (%ld,%ld) for %s\n",
	    statafter.st_flags,
	    statafter.st_mtimespec.tv_sec, statafter.st_mtimespec.tv_nsec,
	    statafter.st_ctimespec.tv_sec, statafter.st_ctimespec.tv_nsec,
	    filename);

	if (writecached)
		fail = ((statafter.st_flags & UF_WRITECACHE) ||
		    (statafter.st_flags & SF_FILE_STUBBED));
	else
		fail = ((statafter.st_flags & UF_WRITECACHE) ||
		    (statafter.st_flags & SF_FILE_STUBBED));

	fail_if(fail, "Stat flags not set as expected for second pass, "
	    "before (%x) and after (%x)",
	    statbefore.st_flags, statafter.st_flags);

	status = test_failpoint_onoff(failpoint, false);
	fail_if(status == -1, "Cannot clear failpoint for %s: %s (%d)",
	    failpoint, strerror(errno), errno);

	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, false);
	fail_if(error,
	    "Restart of archive after without truncate failed %#{}",
	    isi_error_fmt(error));

	status = stat(filename, &statafter);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	CHECK_P("Final stats %x mtime (%ld,%ld) ctime (%ld,%ld) for %s\n",
	    statafter.st_flags,
	    statafter.st_mtimespec.tv_sec, statafter.st_mtimespec.tv_nsec,
	    statafter.st_ctimespec.tv_sec, statafter.st_ctimespec.tv_nsec,
	    filename);

	if (writecached)
		fail = (!(statafter.st_flags & UF_WRITECACHE) ||
		    !(statafter.st_flags & SF_FILE_STUBBED));
	else
		fail = ((statafter.st_flags & UF_WRITECACHE) ||
		    !(statafter.st_flags & SF_FILE_STUBBED));

	fail_if(fail, "Stat flags not set as expected after completion, "
	    "before (%x) and after (%x)",
	    statbefore.st_flags, statafter.st_flags);

	fail_if((statafter.st_blocks >= statbefore.st_blocks),
	    "Invalidate did not succeed, blocks before: "
	    "%ld, blocks after %ld",
	    statbefore.st_blocks, statafter.st_blocks);

	validate_stub_ea(filename, &statbefore,  expected_flags, true);

#ifndef CHECK_CBM_ENABLE_TRACE
	status = unlink(filename);
	fail_if(status, "Could not perform cleanup of file %s",
		    filename);
#endif
}

TEST(test_trinv_ckpt_notrunc, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	test_checkpoint(FP_NO_TRUNC, true, false,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_no_trunc");
}

TEST(test_trinv_ckpt_notruncattr, mem_check : CK_MEM_DEFAULT)
{
	test_checkpoint(FP_NO_TRUNC_ATTR, true, false,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_no_trunc_attr");

	/*
	 * Turn on both failpoints, first to cause a retry, and the
	 * second to cause a non-retryable error on the retry
	 */
	test_failpoint_onoff(FP_STUB_TRUNC_RETRY, true);
	test_checkpoint(FP_NO_TRUNC_ATTR, true, false,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_no_trunc_attr_noretry");
	test_failpoint_onoff(FP_STUB_TRUNC_RETRY, false);
}

TEST(test_trinv_ckpt_truncattr_retry)
{
	/*
	 * Force a retryable error on the truncate after stubbing
	 * The retry should succeed.
	 */
	test_checkpoint(FP_STUB_TRUNC_RETRY, false, true,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_no_trunc_attr_retry");
}

TEST(test_trinv_ckpt_postextend, mem_check : CK_MEM_DEFAULT)
{
	test_checkpoint(FP_POST_EXTEND, true, true,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_post_extend");
}

TEST(test_trinv_ckpt_skiptrunc, mem_check : CK_MEM_DEFAULT)
{
	test_checkpoint(FP_SKIP_TRUNC, false, false,
	    IFS_CPOOL_FLAGS_STUB_MARKED, "/ifs/data/fp_skip_trunc");
}

TEST(test_writecache_failure, mem_check : CK_MEM_DEFAULT)
{
	test_writecache_handling(FP_FAIL_FLUSH, true,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_wc_test");
}

TEST(test_no_writecache_failure, mem_check : CK_MEM_DEFAULT)
{
	test_writecache_handling(FP_FAIL_FLUSH, false,
	    IFS_CPOOL_FLAGS_STUBBED, "/ifs/data/fp_nowc_test");
}

static void
test_inval_retry_common(int iteration)
{
	static struct test_file_info this_test_file =  {

		"/boot/kernel.amd64/kernel.gz",
		"/ifs/data/inval_retry",
		REPLACE_FILE
	};
	struct stat statbuf;
	struct stat statafter;
	struct isi_error *error = NULL;
	int status = -1;
	ifs_lin_t lin;
	const char *filename = this_test_file.output_file;
	const char *failpoint = FP_NO_EXTEND;
	int fd = -1;

	int max_iterations = 2;

	fail_if ((1 > iteration > max_iterations),
	    "test iteration of %d out of valid range: 1,%d",
	    iteration, max_iterations);

	//   1) create file
	status = unlink(filename);
	status = copy_file(this_test_file.input_file, filename);
	fail_if(status, "Could not perform copy of file %s, error: %{} (%d)",
	    filename, strerror(errno), errno);

	status = -1;

	fd = open(filename, O_RDWR);
	fail_if((fd == -1), "Cannot open test file %s, error: %s (%d)",
	    filename, strerror(errno), errno);

	status = fstat(fd, &statbuf);
	fail_if((status == -1), "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);
	lin = statbuf.st_ino;

	//   2) set failpoint for partial invalidate
	status = test_failpoint_onoff(failpoint, true);
	fail_if(status == -1, "Cannot set failpoint for %s: %s (%d)",
	    failpoint, strerror(errno), errno);

	//   3) archive
	error = cbm_test_archive_file_with_pol(lin, CBM_TEST_COMMON_POLICY_RAN,
	    true);
	fail_if((!error || !isi_cbm_error_is_a(error, CBM_STUB_INVALIDATE_ERROR)),
	    "Expected CBM_STUB_INVALIDATE_ERROR from failpoint received %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	status = test_failpoint_onoff(failpoint, false);
	fail_if(status == -1, "Cannot clear failpoint for %s: %s (%d)",
	    failpoint, strerror(errno), errno);

	//   4) check stats - including size
	status = fstat(fd, &statafter);
	fail_if((status == -1), "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	fail_if((statafter.st_size != 0),
	    "Invalidate did not succeed, but size %ld expected zero",
	    statbuf.st_size);

	//   5) complete invalidate
	if (iteration == 1) {
		// a) invalidate request
		status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_INVALIDATE_DATA,
		    0, NULL, NULL, NULL);
		fail_if(status != 0, "Cannot invalidate %s, error: %s (%d)",
		    filename, strerror(errno), errno);
	} else {
		// b) access cache
		isi_cbm_file *cbm_file = NULL;
		cbm_file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
		fail_if(error != NULL, "Stub file open of %s failed: %#{}",
		    filename, isi_error_fmt(error));

		cbm_file->cacheinfo->cacheheader_lock(false, &error);
		fail_if(error != NULL,
		    "Lock on cacheheader for %s failed: %#{}",
		    filename, isi_error_fmt(error));

		isi_cbm_file_close(cbm_file, &error);
		fail_if(error != NULL,
		    "Close of stub file %s failed: %#{}",
		    filename, isi_error_fmt(error));
	}

	//   6) check stats - especially size
	status = fstat(fd, &statafter);
	fail_if((status == -1), "Stat of file %s failed with error: %s (%d)",
	    filename, strerror(errno), errno);

	fail_if((statafter.st_size == 0),
	    "Invalidate completion did not succeed, iteration %d,"
	    "original size was %ld, current size %ld",
	    iteration, statbuf.st_size, statafter.st_size);

	//   7) clean up test_file
	if (fd != -1) {
		status = close(fd);
		fail_if((status != 0),
		    "Close of %s failed, error: %s (%d))",
		    filename, strerror(errno), errno);
	}
#ifndef CHECK_CBM_ENABLE_TRACE
	status = unlink(filename);
	fail_if(status, "Could not perform cleanup of file %s",
	    filename);
#endif
}

TEST(test_inval_retry_invalidate, mem_check : CK_MEM_DEFAULT)
{
	test_inval_retry_common(1);
}

TEST(test_inval_retry_cache, mem_check : CK_MEM_DEFAULT)
{
	test_inval_retry_common(2);
}
