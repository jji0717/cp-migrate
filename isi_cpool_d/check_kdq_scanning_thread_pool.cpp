#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <ifs/pq/pq.h>

#include <isi_cloud_api/cl_provider_common.h>
#include <isi_cpool_cbm/check_cbm_common.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_d_api/client_task_map.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/task.h>
#include <isi_cpool_d_common/unit_test_helpers.h>
#include <isi_cpool_stats/cpool_stats_unit_test_helper.h>
#include <isi_sbtree/sbtree.h>
#include <isi_ufp/isi_ufp.h>

#include "kdq_scanning_thread_pool.h"

static const char *g_pq_name = "kdq_stp_ut.pq";
static isi_cbm_test_sizes small_size(8 * 1024, 8 * 1024, 16 * 1024);
static cbm_test_env env;

#define SYSCTL_CMD(a,v)	(a) = (void *)(v); a##_sz = strlen(v);
#define ENABLE_KD_DEBUG "efs.bam.cpool.enable_kd_debug"

static int
set_bool_sysctl(const char *name, bool on)
{
	void *sysctl_cmd = NULL;
	size_t sysctl_cmd_sz = 0;
	const char *value = (on ? "1" : "0");

	SYSCTL_CMD(sysctl_cmd, value);
	return sysctlbyname(name, NULL, NULL, sysctl_cmd, sysctl_cmd_sz);
}

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_kdq_scanning_thread_pool,
	.mem_check = CK_MEM_LEAKS,
	.suite_setup = setup_suite,
	.suite_teardown = teardown_suite,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	cbm_test_policy_spec specs[] = {
		cbm_test_policy_spec(CPT_RAN)
	};
	env.setup_env(specs, sizeof specs / sizeof specs[0]);

	isi_cbm_reload_config(&error);
	fail_if(error != NULL, "failed to load config: %#{}",
	    isi_error_fmt(error));

	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");

	cpool_fd_store::getInstance();

	isi_cbm_get_coi(&error);
	fail_if (error != NULL, "failed to get COI: %#{}",
	    isi_error_fmt(error));

	cpool_stats_unit_test_helper::on_suite_setup();
}

TEST_FIXTURE(teardown_suite)
{
	env.cleanup();
	isi_cbm_unload_conifg();
	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
	cpool_fd_store::getInstance().close_sbt_fds();
}

TEST_FIXTURE(setup_test)
{
	fail_if (ifs_pq_delete_queue(g_pq_name, 0) != 0 && errno != ENOENT,
	    "failed to delete PQ (%s) in preparation for test: %d %s",
	    g_pq_name, errno, strerror(errno));

	set_bool_sysctl(ENABLE_KD_DEBUG, true);

	isi_cloud::cl_provider_module::init();
}

TEST_FIXTURE(teardown_test)
{
	set_bool_sysctl(ENABLE_KD_DEBUG, false);

	isi_cloud::cl_provider_module::destroy();
}

class kdq_scanning_thread_pool_ut : public kdq_scanning_thread_pool
{
public:
	kdq_scanning_thread_pool_ut()
		: kdq_scanning_thread_pool() { }

	void read_entries_from_queue(int q_fd, uint32_t num_desired,
	    uint32_t &num_read, struct pq_bulk_query_entry *entries,
	    char *data_buf, struct isi_error **error_out) const
	{
		kdq_scanning_thread_pool::read_entries_from_queue(q_fd,
		    num_desired, num_read, entries, data_buf, error_out);
	}

	void process_queue_entry(const struct pq_bulk_query_entry *entry,
	    int q_fd, struct isi_error **error_out) const
	{
		kdq_scanning_thread_pool::process_queue_entry(entry, q_fd,
		    error_out);
	}
};

/*
 * Test reading entries from an empty PQ.
 */
TEST(test_read_entries_from_queue__empty)
{
	struct isi_error *error = NULL;

	kdq_scanning_thread_pool_ut tp;
	int pq_fd;
	uint32_t num_entries_desired, num_entries_read;
	struct pq_bulk_query_entry entries[10];
	char data_buf[10 * 512];

	/* Create/open the test PQ. */
	fail_if (ifs_pq_open(g_pq_name, &pq_fd, PQ_OPEN_OPTIONS_CREATE) != 0,
	    "failed to open PQ: %s", strerror(errno));

	/* Read the entries from the PQ; there shouldn't be any. */
	num_entries_desired = 10;
	tp.read_entries_from_queue(pq_fd, num_entries_desired,
	    num_entries_read, entries, data_buf, &error);
	fail_if (error != NULL,
	    "read_entries_from_queue failed: %#{}", isi_error_fmt(error));
	fail_if (num_entries_read != 0,
	    "num_entries_read mismatch (exp: 0 act: %u)", num_entries_read);
}

/*
 * Test reading entries from a non-empty PQ.
 */
TEST(test_read_entries_from_queue__non_empty)
{
	struct isi_error *error = NULL;

	kdq_scanning_thread_pool_ut tp;
	int pq_fd;
	uint32_t num_entries_desired, num_entries_read;
	struct pq_bulk_query_entry entries[10];
	char data_buf[10 * 512];

	/* Create/open the test PQ. */
	fail_if (ifs_pq_open(g_pq_name, &pq_fd, PQ_OPEN_OPTIONS_CREATE) != 0,
	    "failed to open PQ: %s", strerror(errno));

	/* Add 5 items to the test PQ. */
	for (int i = 0; i < 5; ++i) {
		char *item_buf = new char[32];
		fail_if (item_buf == NULL, "i: %d", i);
		snprintf(item_buf, 32, "item_%02d", i);

		fail_if (ifs_pq_insert(pq_fd, NULL, NULL, 0,
		    static_cast<void *>(item_buf), strlen(item_buf),
		    PQ_INSERT_OPTIONS_APPEND) != 0,
		    "failed to insert item (i: %d): %s\n",
		    i, errno, strerror(errno));

		delete [] item_buf;
	}

	/* Read entries from the PQ, requesting varying numbers of entries. */
	num_entries_desired = 1;
	tp.read_entries_from_queue(pq_fd, num_entries_desired,
	    num_entries_read, entries, data_buf, &error);
	fail_if (error != NULL,
	    "read_entries_from_queue failed: %#{}", isi_error_fmt(error));
	fail_if (num_entries_read != 1,
	    "num_entries_read mismatch (exp: 1 act: %u)", num_entries_read);

	num_entries_desired = 3;
	tp.read_entries_from_queue(pq_fd, num_entries_desired,
	    num_entries_read, entries, data_buf, &error);
	fail_if (error != NULL,
	    "read_entries_from_queue failed: %#{}", isi_error_fmt(error));
	fail_if (num_entries_read != 3,
	    "num_entries_read mismatch (exp: 3 act: %u)", num_entries_read);

	num_entries_desired = 7;
	tp.read_entries_from_queue(pq_fd, num_entries_desired,
	    num_entries_read, entries, data_buf, &error);
	fail_if (error != NULL,
	    "read_entries_from_queue failed: %#{}", isi_error_fmt(error));
	fail_if (num_entries_read != 5,
	    "num_entries_read mismatch (exp: 5 act: %u)", num_entries_read);
}

/*
 * Test a simulated crash between adding a local GC task to isi_cpool_d and
 * removing the KDQ entry, then test the reprocessing of the KDQ entry.
 *
 * Memory checking disabled on this test as the amount of memory leaked from
 * cpool_fd_store is not constant (see Bug147515).  Once cpool_fd_store is
 * refactored (post-Riptide tech debt) the memory checking on this test will be
 * re-examined.
 *
 * At various times, either 4 or 8 bytes are "leaked" from cpool_fd_store
 * functions.  A false positive leak of an additional 80 bytes may occur on
 * local builds on virtual nodes.
 */
TEST(test_process_queue_entry__failpoint, CK_MEM_DISABLE)
{
	struct isi_error *error = NULL;

	/* Create and archive a file. */
	const char *path = "/ifs/test_process_queue_entry__failpoint.txt";

	fail_if (remove(path) != 0 && errno != ENOENT,
	    "failed to remove file (%s): %d %s", path, errno, strerror(errno));

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	fail_if (fd == -1, "failed to open file (%s): %d %s",
	    path, errno, strerror(errno));

	const char *buffer = "arbitrary text for file";
	size_t buffer_size = strlen(buffer) + 1;
	size_t bytes_written = write(fd, buffer, buffer_size);
	fail_if (bytes_written != buffer_size,
	    "failed to write file (%s), %zu bytes written: %d %s",
	    path, bytes_written, errno, strerror(errno));

	struct stat stat_buf;
	fail_if (fstat(fd, &stat_buf) != 0, "failed to fstat: %d %s",
	    errno, strerror(errno));
	ifs_lin_t lin = stat_buf.st_ino;

	fail_if (fsync(fd) != 0, "failed to fsync: %d %s",
	    errno, strerror(errno));
	fail_if (close(fd) != 0, "failed to close: %d %s",
	    errno, strerror(errno));

	cbm_test_policy_spec spec(CPT_RAN);
	error = cbm_test_archive_file_with_pol(lin, spec.policy_name_.c_str(),
	    false);
	fail_if (error != NULL, "failed to archive file (%s): %#{}",
	    path, isi_error_fmt(error));

	/* Create a KDQ entry for the archived file. */
	fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	fail_if (fd == -1, "failed to open file (%s): %d %s",
	    path, errno, strerror(errno));

	char stubmap_buf[ISI_CFM_MAP_EXTATTR_THRESHOLD];
	size_t stub_map_size = 8192;
	size_t stub_data_size = ISI_CFM_MAP_EXTATTR_THRESHOLD;
	fail_if (ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER, 0,
	    &stub_map_size, stubmap_buf, &stub_data_size) != 0,
	    "failed to retrieve stubmap header: %d %s",
	    errno, strerror(errno));

	fail_if (close(fd) != 0, "failed to close: %d %s",
            errno, strerror(errno));

	size_t pq_entry_size = sizeof(struct ifs_kernel_delete_pq_info) +
	    stub_data_size;
	struct ifs_kernel_delete_pq_info *pq_entry =
	    (struct ifs_kernel_delete_pq_info *)malloc(pq_entry_size);
	fail_if (pq_entry == NULL);

	pq_entry->lin = lin;
	pq_entry->snap_id = HEAD_SNAPID;
	memcpy(&(pq_entry->stubmap), stubmap_buf, stub_data_size);

	pq_h pq;
	fail_if (pq_open(g_pq_name, &pq, PQ_OPEN_OPTIONS_CREATE),
	    "failed to open KDQ (%s): %d %s", g_pq_name,
	    errno, strerror(errno));

	btree_key_t key;
	fail_if (pq_insert(pq, &key, NULL, 0, pq_entry, pq_entry_size,
	    PQ_INSERT_OPTIONS_APPEND) != 0,
	    "failed to insert into KDQ (%s): %d %s", g_pq_name,
	    errno, strerror(errno));

	free(pq_entry);
	pq_entry = NULL;

	/*
	 * Configure the failpoint to "crash" between the local GC task
	 * addition and the KDQ entry removal.
	 */
	ufp_globals_rd_lock();
	struct ufp *ufp =
	    ufail_point_lookup("kdq_stp__process__skip_kdq_delete");
	ufp_globals_rd_unlock();
	fail_if (ufp == NULL);

	ufp_globals_wr_lock();
	int ret = ufail_point_set(ufp, "return(1)");
	ufp_globals_wr_unlock();
	fail_if (ret != 0, "failed to set ufp: %d %s", ret, strerror(ret));

	/*
	 * Process the entry, then verify that a local GC task exists and that
	 * the entry still exists in the KDQ.
	 */
	kdq_scanning_thread_pool_ut tp;
	uint32_t num_entries_read;
	struct pq_bulk_query_entry entry;
	char entry_data_buf[512];
	tp.read_entries_from_queue(pq, 1, num_entries_read, &entry,
	    entry_data_buf, &error);
	fail_if (error != NULL, "failed to read entry: %#{}",
	    isi_error_fmt(error));
	fail_if (num_entries_read != 1,
	    "num_entries_read value mismatch (exp: 1 act: %u)",
	    num_entries_read);

	tp.process_queue_entry(&entry, pq, &error);
	fail_if (error != NULL, "failed to process entry: %#{}",
	    isi_error_fmt(error));

	isi_cloud::local_gc_task lgc_t;
	lgc_t.set_lin(lin);
	lgc_t.set_snapid(HEAD_SNAPID);

	client_task_map ctm(OT_LOCAL_GC);

	/* false: don't ignore ENOENT (return it as error if appropriate) */
	isi_cloud::task *retrieved_task = ctm.find_task(lgc_t.get_key(), false,
	    &error);
	fail_if (error != NULL, "failed to find local GC task: %#{}",
	    isi_error_fmt(error));
	fail_if (retrieved_task == NULL);

	delete retrieved_task;
	retrieved_task = NULL;

	tp.read_entries_from_queue(pq, 1, num_entries_read, &entry,
	    entry_data_buf, &error);
	fail_if (error != NULL, "failed to read entry: %#{}",
	    isi_error_fmt(error));
	fail_if (num_entries_read != 1,
	    "num_entries_read value mismatch (exp: 1 act: %u)",
	    num_entries_read);

	/*
	 * Turn off the failpoint and reprocess the entry.  The local GC task
	 * should still exist, but we should no longer have a KDQ entry.
	 */
	ufp_globals_wr_lock();
	ret = ufail_point_set(ufp, "off");
	ufp_globals_wr_unlock();
	fail_if (ret != 0, "failed to set ufp: %d %s", ret, strerror(ret));

	tp.process_queue_entry(&entry, pq, &error);
	fail_if (error != NULL, "failed to process entry: %#{}",
	    isi_error_fmt(error));

	/* false: don't ignore ENOENT (return it as error if appropriate) */
	retrieved_task = ctm.find_task(lgc_t.get_key(), false, &error);
	fail_if (error != NULL, "failed to find local GC task: %#{}",
	    isi_error_fmt(error));
	fail_if (retrieved_task == NULL);

	delete retrieved_task;
	retrieved_task = NULL;

	tp.read_entries_from_queue(pq, 1, num_entries_read, &entry,
	    entry_data_buf, &error);
	fail_if (error != NULL, "failed to read entry: %#{}",
	    isi_error_fmt(error));
	fail_if (num_entries_read != 0,
	    "num_entries_read value mismatch (exp: 0 act: %u)",
	    num_entries_read);

	/* cleanup */
	ufp_globals_wr_lock();
	ufail_point_destroy(ufp);
	ufp_globals_wr_unlock();

	remove(path);
}
