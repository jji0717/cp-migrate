#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/extattr.h>

#include <isi_ilog/ilog.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_snapshot/snapshot.h>
#include <isi_cpool_security/cpool_protect.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_gc.h"
#include "isi_cbm_invalidate.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_test_util.h"

#include <check.h>

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF

using namespace isi_cloud;

// from using static instance of cpool_fd_store
#define KNOWN_LEAK 16

static bool ran_only_g = true;

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

#ifdef ENABLE_ILOG_USE
	struct ilog_app_init init =  {
		"check_cbm_snap",		// full app name
		"cpool",			// component
		"",				// job (opt)
		"check_cbm_snap",		// syslog program
		IL_TRACE_PLUS|IL_CP_EXT,	// default level
		false,				// use syslog
		true,				// use stderr
		false,				// log_thread_id
		LOG_DAEMON,			// syslog facility
		"/ifs/logs/check_cbm_snap.log",	// log file
		IL_ERR_PLUS,			// syslog_threshold
		NULL,				//tags
	};

	char *ilogenv = getenv("CHECK_USE_ILOG");
	if (ilogenv != NULL){
		ilog_init(&init, false, true);
		ilog(IL_NOTICE, "ilog initialized\n");
	}
#endif
	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(ran_only_g, true);
	cbm_test_common_setup(ran_only_g, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");

	cpool_fd_store::getInstance();

}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(ran_only_g, true);
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

SUITE_DEFINE_FOR_FILE(check_cbm_snap,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 60,
    .fixture_timeout = 1200);

static uint64_t 
create_stub_file(const char *path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat sb = {0};
	int fd, status;
	uint64_t lin;

	ASSERT(path);
	ASSERT(error_out);

	// create regular file
	fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	fail_if((fd == -1),
	    "open of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	for (int i = 0; i < 128; ++i) {
		write(fd, buffer, buffer_size);
	}
	status = fstat(fd, &sb);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	lin = sb.st_ino;
	fsync(fd);
	close(fd);

	// stub it
	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	return lin;
}

static void
delete_head_file(const char *path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat sb = {0};
	int status;
	uint64_t lin;
	bool deleted = false;

	status = stat(path, &sb);
	if (status == -1) {
		error = isi_system_error_new(errno, 
		    "Stat of file %s failed", path);
		goto out;
	}
	lin = sb.st_ino;

	if (unlink(path) == 0)
		deleted = true;

 out:
	isi_error_handle(error, error_out);
}

TEST(test_cbm_snap_del)
{
	struct isi_error *error = NULL;
	uint64_t lin;
	const char *path = "/ifs/test_cbm_snap_del.txt";
	ifs_snapid_t snapid;
	SNAP *snap = NULL;

	// create a stub file
	lin = create_stub_file(path, &error);

	// take a snap
	std::string snap_name1 = "test_cbm_snap_del";
	remove_snapshot(snap_name1);
	snapid = take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}",
	    isi_error_fmt(error));
	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed", snapid);

	// delete head version of stub
	delete_head_file(path, &error);
	fail_if(error, "deleting head version of %s %#{}", path,
	    isi_error_fmt(error));

	// begin snap deletion
	remove_snapshot(snap_name1);

	// finish snap deletion
	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to finish destroying snapshot: %#{}",
	    isi_error_fmt(error));

	// cleanup
	snapshot_close(snap);
}

/**
 * Verify stub after archive, snap then delete the snap
 */
TEST(test_archive_snap_del_snap)
{
	struct isi_error *error = NULL;

	const char *path = "/ifs/test_archive_snap_del_snap.txt";

	remove(path);

	int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	const int buffer_size = 8192;
	char buffer[buffer_size];
	memset(buffer, 'A', sizeof(buffer));
	write(fd, buffer, buffer_size);

	struct stat sb = {0};
	int status = fstat(fd, &sb);

	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    path, strerror(errno), errno);
	uint64_t lin = sb.st_ino;

	fsync(fd);
	close(fd);
	error = cbm_test_archive_file_with_pol(lin,
	    CBM_TEST_COMMON_POLICY_RAN, false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	ifs_snapid_t snapid;
	SNAP *snap = NULL;

	isi_cbm_coi_sptr coi;
	coi = isi_cbm_get_coi(&error);
	fail_if(error, "Failed to open the COI: %#{}.\n",
	    isi_error_fmt(error));

	std::string snap_name1 = "test_archive_snap_del_snap";
	remove_snapshot(snap_name1);
	snapid = take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}.\n",
	    isi_error_fmt(error));

	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	std::set<ifs_lin_t> lins;
	isi_cloud_object_id object_id = file->mapinfo->get_object_id();
	coi->get_references(object_id, lins, &error);
	fail_if(error, "Failed to get_references from COI: %#{}.\n",
	    isi_error_fmt(error));
	fail_if(lins.find(file->lin) == lins.end(),
	    "error, the LIN %{} is not found in the COI entry for %s, %d",
	    lin_fmt(file->lin), object_id.to_c_string(),
	    lins.size()	);

	bool dirty = false;
	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "Failed to invalidate the file: %#{}.\n",
	    isi_error_fmt(error));
	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed", snapid);

	remove_snapshot(snap_name1);

	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to snapshot_destroy_finish snapid: %d: %#{}.\n",
	    snapid, isi_error_fmt(error));

	// cleanup
	snapshot_close(snap);

	struct timeval date_of_death_tv;
	gettimeofday(&date_of_death_tv, NULL);
	isi_cbm_local_gc(lin, snapid, object_id,
	    date_of_death_tv, NULL, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to isi_cbm_local_gc snapid: %d: %#{}.\n",
	    snapid, isi_error_fmt(error));

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	object_id = file->mapinfo->get_object_id();
	lins.clear();
	coi->get_references(object_id, lins, &error);
	fail_if(error, "Failed to get_references from COI: %#{}.\n",
	    isi_error_fmt(error));

	fail_if(lins.find(file->lin) == lins.end(),
	    "error, the LIN %{} is not found in the COI entry for %s, %d",
	    lin_fmt(file->lin), object_id.to_c_string(),
	    lins.size());

	char out_buf[buffer_size];
	memset(out_buf, 0, sizeof(out_buf));

	size_t outlen = isi_cbm_file_read(file, out_buf,
	    0,
	    sizeof(out_buf), CO_CACHE, &error);
	fail_if(error, "cannot read file %s, %#{}", path,
	    isi_error_fmt(error));

	fail_if(outlen != sizeof(out_buf),
	    "The read length is not matched %d %d.",
	    sizeof(out_buf), outlen);

	fail_if(strncmp(buffer, out_buf, buffer_size),
	    "The data is not matched in 1st chunk after stubbing exp:%10s "
	    "actual:%10s.",
	    buffer, out_buf);

	isi_cbm_file_close(file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	remove(path);
}

TEST(test_cbm_snap_estale)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_cbm_snap_estale.txt";
	ifs_snapid_t snapid;
	SNAP *snap = NULL;
	uint64_t lin;

	// Remove the file if it already exists
	remove(path);

	// Create a stub file
	lin = create_stub_file(path, &error);

	// Take snapshot of /ifs
	std::string stub_snap = "test_cbm_snap_estale";
	remove_snapshot(stub_snap);
	snapid = take_snapshot("/ifs", stub_snap, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}",
	    isi_error_fmt(error));
	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed", snapid);

	// Get the fd associated with the stub
	struct isi_cbm_file *snap_file = isi_cbm_file_open(lin, snapid, 0, &error);
	fail_if(error != NULL, "Failed to open stub file");
	isi_cfm_mapinfo map;
	int fd = snap_file->fd;

	// Delete the snapshot
	delete_head_file(path, &error);
	fail_if(error, "deleting head version of %s %#{}", path,
	    isi_error_fmt(error));
	remove_snapshot(stub_snap);
	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to finish destroying snapshot: %#{}",
	    isi_error_fmt(error));

	// Try reading the stub map by calling isi_cph_get_stubmap on FD
	isi_cph_get_stubmap(fd, map, &error);

	// Look for ESTALE in errno
	fail_unless(errno == ESTALE, "Got an non-ESTALE system error");
	fail_if(error == NULL, "Should have encountered an error");
	isi_error_free(error);
	error = NULL;

	// Do cleanup - remove paths, files and free memory
	isi_cbm_file_close(snap_file, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	snapshot_close(snap);
	remove(path);
}

static void
test_broken_stub(const char *attr_name)
{

	struct isi_error *error = NULL;
	uint64_t lin;
	const char *path = "/ifs/test_cbm_snap_del2.txt";
	ifs_snapid_t snapid;
	SNAP *snap = NULL;
	struct isi_cbm_file *stub = NULL;
	int ret;

	// create a stub file
	lin = create_stub_file(path, &error);

	// save its object id
	stub = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Failed to open the CBM file: %#{}.\n",
	    isi_error_fmt(error));
	isi_cloud_object_id object_id(stub->mapinfo->get_object_id());
	isi_cbm_file_close(stub, &error);
	fail_if(error, "Failed to close the CBM file: %#{}.\n",
	    isi_error_fmt(error));

	// break it by removing a map attr
	ret = extattr_delete_file(path, ISI_CFM_MAP_ATTR_NAMESPACE,
	    attr_name);
	fail_if(ret != 0, "failed to delete map size errno = %d", errno);

	// take a snap
	std::string snap_name1 = "test_cbm_snap_del_broken_stub";
	remove_snapshot(snap_name1);
	snapid = take_snapshot("/ifs", snap_name1, false, &error);
	fail_if (error, "Failed to snapshot the file: %#{}",
	    isi_error_fmt(error));
	snap = snapshot_open_by_sid(snapid, &error);
	fail_if(error, "snapshot_open_by_sid(%lld) failed: %#{}", snapid,
	    isi_error_fmt(error));

	// delete head version of stub
	ret = unlink(path);
	fail_if(ret != 0, "deleting head version of %s: errno = %d", path,
	    errno);

	// begin snap deletion
	remove_snapshot(snap_name1);

	// finish snap deletion
	snapshot_destroy_finish(snap, &error);
	fail_if(error, "Failed to finish destroying snapshot: %#{}",
	    isi_error_fmt(error));
	snapshot_close(snap);

	// cleanup the coi and cloud object for this stub
	isi_cbm_coi_sptr coi;
	coi = isi_cbm_get_coi(&error);
	fail_if(error, "Failed to open the COI: %#{}.\n",
	    isi_error_fmt(error));
	struct timeval effective_dod = {0};
	struct timeval date_of_death_tv = { 123, 0 };
	coi->remove_ref(object_id, lin, HEAD_SNAPID, date_of_death_tv,
		&effective_dod, &error);
	fail_if(error, "Failed to remove ref: %#{}",
	    isi_error_fmt(error));
	isi_cbm_cloud_gc(object_id, NULL, NULL, NULL, NULL, &error);
	fail_if(error, "Failed to cloud gc: %#{}",
	    isi_error_fmt(error));
}

TEST(test_cbm_snap_del_broken_stub_mapsize,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	test_broken_stub(ISI_CFM_MAP_ATTR_SIZE);

}

TEST(test_cbm_snap_del_broken_stub_map,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	test_broken_stub(ISI_CFM_MAP_ATTR_NAME);

}
