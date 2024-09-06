#include <ifs/ifs_types.h>
#include <ifs/bam/bam_cpool.h>
#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/pq/pq.h>
#include <sys/stat.h>
#include <sys/proc.h>
#include <sys/module.h>
#include <sys/isi_lwext.h>

#include <check.h>

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>
#include <isi_sstore/sstore.h>
#include <isi_util/isi_str.h>

#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/task_key.h>

#include "cpool_api.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_dedupe_interop,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.timeout = 60,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	// to prevent memory leak messages resulting from static
	// heap variables.
	isi_cloud_object_id object_id;
	cpool_fd_store::getInstance();
}

TEST_FIXTURE(teardown_suite)
{
	// restart isi_cpool_d
	system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
}

/**
 * Deletes SBTs for all task types up to priority DEL_PRIORITY_MAX.
 */
#define DEL_PRIORITY_MAX 50
#define DEL_JOBS_MAX 50
TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	const cpool_operation_type *op_type = NULL;
	char *name_buf = NULL;

	OT_FOREACH(op_type) {
		for (int pri = 0; pri < DEL_PRIORITY_MAX; ++pri) {
			name_buf = get_pending_sbt_name(op_type->get_type(),
			    pri);
			delete_sbt(name_buf, &error);
			fail_if (error != NULL, "failed to delete SBT: %#{}",
			    isi_error_fmt(error));
			free(name_buf);
		}

		name_buf = get_inprogress_sbt_name(op_type->get_type());
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete SBT: %#{}",
		    isi_error_fmt(error));
		free(name_buf);
	}

	for (djob_id_t job_id = 0; job_id < DEL_JOBS_MAX; ++job_id) {
		name_buf = get_djob_file_list_sbt_name(job_id);
		delete_sbt(name_buf, &error);
		fail_if (error != NULL, "failed to delete SBT: %#{}",
		    isi_error_fmt(error));
		free(name_buf);
	}

	name_buf = get_daemon_jobs_sbt_name();
	delete_sbt(name_buf, &error);
	fail_if (error != NULL, "failed to delete SBT: %#{}",
	    isi_error_fmt(error));
	free(name_buf);

	/*
	 * Enable stub access for this test.
	 */
	struct procoptions po = PROCOPTIONS_INIT;

	if (getprocoptions(&po)) {
		fail("Could not get current process options from kernel "
		    "%d-%s", errno, strerror(errno));
	}

	po.po_isi_flag_on |= PI_CPOOL_ALLOW_ACCESS_MASK;
	if (setprocoptions(&po) < 0) {
		fail("Failed to enable access to stubbed files - %d-%s",
		    errno, strerror(errno));
	}
}

#define remove_file(file, test_end) _remove_file(__LINE__, file, test_end)

static void
_remove_file(int linenum, const char *file, bool test_end)
{
	int ret = 0;
	ret = unlink(file);

	if (test_end && ret != 0) {
		fail("Deletion of the file failed at line %d. %d-%s",
		    linenum, errno, strerror(errno));
	}
}

struct pseudo_stub {
	ifs_cpool_stubmap_info header;
	int payload;
};

/*
 * Toggle stub file access for this test.
 *
 * @param state supplies whether the stub access privilege is on.
 */
static void
toggle_privilege(bool state)
{
	struct procoptions po = PROCOPTIONS_INIT;

	if (getprocoptions(&po)) {
		fail("Could not get current process options from kernel "
		    "%d-%s", errno, strerror(errno));
	}

	if (state)
		po.po_isi_flag_on |= PI_CPOOL_ALLOW_STUB_OPS;
	else
		po.po_isi_flag_off |= PI_CPOOL_ALLOW_STUB_OPS;

	if (setprocoptions(&po) < 0) {
		fail("Failed to enable access to stubbed files - %d-%s",
		    errno, strerror(errno));
	}

	if (getprocoptions(&po)) {
		fail("Could not get current process options from kernel "
		    "%d-%s", errno, strerror(errno));
	}
}

/*
 * Create a pseudo stub file for use in the test.
 */
static int create_pseudo_stub(const char *filename)
{
	size_t fake_map_info_size;
	int fd;
	uint64_t filerev;
	pseudo_stub stub;
	struct stat stat_buf;
	int val;

	fd = open(filename, O_RDWR | O_CREAT, 0755);
	fail_if (fd == -1, "open failed: %d %s", errno, strerror(errno));
	fail_if (getfilerev(fd, &filerev) == -1, "getfilerev failed: %d %s",
	    errno, strerror(errno));

	fail_if (fstat(fd, &stat_buf) == -1, "fstat failed: %d %s",
	    errno, strerror(errno));

	fake_map_info_size = sizeof(stub);
	stub.header.magic = IFS_CPOOL_CFM_MAP_MAGIC;
	stub.header.version = CPOOL_CFM_MAP_VERSION;
	stub.header.header_size = sizeof(stub);
	stub.payload = 0xbaadf00d;
	val = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_FULL, filerev,
	    &fake_map_info_size, &stub, &fake_map_info_size);

	fail_if(val == -1,
	    "ifs_cpool_stub_ops failed: %d %s", errno, strerror(errno));

	return fd;
}

static void
recall_pseudo_stub(const char *filename)
{
	int ret, fd;
	uint64_t filerev;

	fd = open(filename, O_RDWR, 0755);
	fail_if (fd == -1,
	    "open failed for %s: %d %s",
	    filename, errno, strerror(errno));
	fail_if (getfilerev(fd, &filerev) == -1,
	    "getfilerev failed for %s: %d %s",
	    filename, errno, strerror(errno));
	ret = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_RECALL, filerev,
	    NULL, NULL, NULL);
	fail_if (ret == -1,
	    "ifs_cpool_stub_ops failed for %s: %d %s",
	    filename, errno, strerror(errno));

	close(fd);
}

struct isi_str *
str_from_c_str(const char *_s)
{
	struct isi_str *str;
	char *s;

	/*
	 * This use of isi_str_alloc does not modify the source, but the
	 * isi_str(3) API in general isn't const-friendly.
	 */
	s = __DECONST(char *, _s);
	str = isi_str_alloc(s, strlen(s) + 1, ENC_DEFAULT, 0);
	fail_if(str == NULL);
	return (str);
}

/**
 * Test dedupe interop i.e. its blocked.
 */

#define MAX_DEDUPE_EXTENT 512

TEST(test_cpool_dedupe_interop)
{
	char buffer[0x1000];
	int fd;
	struct isi_error *ie;
	int index;
	int rv;
	struct isi_str *src, *dst;

	printf("\n");
	remove_file("/ifs/test_cp_dedupe1", false);
	remove_file("/ifs/test_cp_dedupe2", false);
	remove_file("/ifs/test_cp_dedupe3", false);

	/*
	 * Create two stub files and a third normal one.
	 */

	toggle_privilege(true);
	fd = create_pseudo_stub("/ifs/test_cp_dedupe1");
	for (index = 0; index < 512; index++) {
		rv = pwrite(fd, buffer, 0x1000, index * 0x1000);
		fail_if(rv == -1,
		    "pwrite failed: %d %d %s",
		    rv, errno, strerror(errno));
	}

	ifs_set_dedupe_was_sampled(fd, true, &ie);
	close(fd);

	fd = create_pseudo_stub("/ifs/test_cp_dedupe2");
	for (index = 0; index < 512; index++) {
		rv = pwrite(fd, buffer, 0x1000, index * 0x1000);
		fail_if(rv == -1,
		    "pwrite fd2 failed: %d %d %s",
		    rv, errno, strerror(errno));
	}

	ifs_set_dedupe_was_sampled(fd, true, &ie);
	close(fd);

	fd = open("/ifs/test_cp_dedupe3", O_RDWR | O_CREAT, 0755);
	fail_if (fd == -1, "open failed: %d %s", errno, strerror(errno));
	for (index = 0; index < 512; index++) {
		rv = pwrite(fd, buffer, 0x1000, index * 0x1000);
		fail_if(rv == -1,
		    "pwrite fd3 failed: %d %d %s",
		    rv, errno, strerror(errno));
	}

	ifs_set_dedupe_was_sampled(fd, true, &ie);
	close(fd);

	/* Attempt to dedupe them. 2 cases both are stubs, 1 is a stub */

	src = str_from_c_str("/ifs/test_cp_dedupe1");
	dst = str_from_c_str("/ifs/test_cp_dedupe2");
	ie = NULL;

	ss_dedupe_file(src, dst, &ie);
	fail_if(isi_error_to_int(ie) != ESTUBFILE,
	    "Expected to fail the dedupe operation. ret: %d.",
	    isi_error_to_int(ie));

	isi_error_free(ie);

	ie = NULL;
	isi_str_free(dst);
	dst = str_from_c_str("/ifs/test_cp_dedupe3");

	ss_dedupe_file(src, dst, &ie);
	fail_if(isi_error_to_int(ie) != ESTUBFILE,
	    "Expected to fail the dedupe operation. ret: %d.",
	    isi_error_to_int(ie));

	isi_error_free(ie);

	/*
	 * "Recall" the pseudo stub files created by the test before deleting
	 * as to not trigger GC (since they weren't really stubbed to begin
	 * with).
	 */
	recall_pseudo_stub("/ifs/test_cp_dedupe1");
	recall_pseudo_stub("/ifs/test_cp_dedupe2");

	remove_file("/ifs/test_cp_dedupe1", true);
	remove_file("/ifs/test_cp_dedupe2", true);
	remove_file("/ifs/test_cp_dedupe3", true);
	isi_str_free(src);
	isi_str_free(dst);
}


