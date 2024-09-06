#include <fcntl.h>
#include <stdio.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>

#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <ifs/btree/btree_on_disk.h>
#include <ifs/sbt/sbt.h>

#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_d_common/task.h>

#include "check_cbm_common.h"
#include "isi_cpool_cbm.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_cache.h"
#include "isi_cbm_test_util.h"

#include <check.h>

#define CHECK_TEMP_FILE 	"/tmp/test_cpool_write.tmp.XXXXXX"

using namespace isi_cloud;


TEST_FIXTURE(suite_setup)
{
	cbm_test_common_cleanup(false, true);
	cbm_test_common_setup(false, true);
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(false, true);
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();

	checksum_cache_cleanup();
}

TEST_FIXTURE(teardown_test)
{
	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_cbm_invalidate,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 60,
    .fixture_timeout = 1200);

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF
#define RM_STRING	CBM_ACCESS_ON "rm -f %s" CBM_ACCESS_OFF
			
TEST(test_cbm_add_csi,
    mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{

	ifs_lin_t lin;
	int status = -1;
	struct stat statbuf;
	const char *pol_name =  CBM_TEST_COMMON_POLICY_RAN_INVALIDATE;
	const char *filename = NULL;

	size_t buf_sz = 8192;
	void *buf[8192];
	uint64_t offset = 0;

	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);

	struct isi_cbm_file *file_obj;

	bool cached = false;
	bool invalidate = false;
	int result = 0;
	bool found = false;
	int count = 0;

	const char *csi_name = NULL;
	int sbt_fd = -1;
	btree_key_t key = {{ 0, 0 }}, next_key;
	struct sbt_entry *ent = NULL;

	char entry_buf[SBT_MAX_ENTRY_SIZE_128];
	size_t num_ent_out;

	isi_cloud::csi_entry ce;
	std::set<isi_cloud::lin_snapid> cached_files;
	std::set<isi_cloud::lin_snapid>::const_iterator iter;

	struct test_file_info test_file =
	    {"/etc/services", "/ifs/data/ran_csi_unit_test", REPLACE_FILE};

	fmt_print(&str, CP_STRING, test_file.input_file,
	    test_file.output_file);

	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);

	status = -1;

	filename = test_file.output_file;

	status = stat(filename, &statbuf);
	fail_if((status == -1),"Stat of file %s failed with error: %s (%d)", 
	    filename, strerror(errno), errno);

	lin = statbuf.st_ino;

	/*
	 * archive and invalidate the file
	 */
	error = cbm_test_archive_file_with_pol(lin, pol_name, true);
	fail_if(error, "Failed to archive file %s, error: %#{}",
	    filename, isi_error_fmt(error));

	file_obj = isi_cbm_file_open(statbuf.st_ino, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", filename, 
	    isi_error_fmt(error));

	ssize_t rd_sz = isi_cbm_file_read(file_obj, buf, offset, buf_sz,
	    CO_DEFAULT, &error);
	fail_if(error, "Failed to read %s via cache, error: %#{}",
	    filename, isi_error_fmt(error));
	fail_if(rd_sz < 0, "failed to read CBM file");

	/*
	 * Close and reopen the file
	 */
	isi_cbm_file_close(file_obj, isi_error_suppress());
	file_obj = isi_cbm_file_open(statbuf.st_ino, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file %s, %#{}", filename, 
	    isi_error_fmt(error));

	cpool_cache &cacheinfo = *file_obj->cacheinfo;

	if (!cacheinfo.is_cache_open()) {
		cacheinfo.cache_open(file_obj->fd,
		    false, false, &error);
		fail_if(error, "Cannot open cache, %#{}", isi_error_fmt(error));
	}

	cacheinfo.cacheheader_lock(true, &error);
	fail_if(error, "Cannot lock cacheheader, %#{}", isi_error_fmt(error));

	cacheinfo.get_invalidate_state(cached, invalidate, &error);
	fail_if(error, "Cannot get cache state, %#{}", isi_error_fmt(error));

	fail_if(!cached, "Expected CACHED state to be set, failed");

	/* Now look at csi queue */

	csi_name = get_csi_sbt_name();
	sbt_fd = open_sbt(csi_name, false, &error);
	fail_if(error, "Could not open CSI sbt");
	while (result == 0 && !found) {
		result = ifs_sbt_get_entries(sbt_fd, &key, &next_key,
		    sizeof entry_buf, entry_buf, 1, &num_ent_out);

		fail_if((num_ent_out == 0), "No CSI entries found");
		fail_if((result == -1), "Error retrieving element: %s\n",
		    strerror(errno));

		ent = (struct sbt_entry *)entry_buf;

		ce.set_serialized_entry(CPOOL_CSI_VERSION, key, ent->buf,
			ent->size_out, &error);
		fail_if(error != NULL, "CSI - Error setting serialized entry %#{}\n",
		    isi_error_fmt(error));

		cached_files = ce.get_cached_files();
		iter = cached_files.begin();

		if (cached_files.size() != 0) {
			for (count = 0; iter != cached_files.end(); 
			     ++iter, count++) {
				if (iter->get_lin() == lin &&
				    iter->get_snapid() == HEAD_SNAPID)
					found = true;
			}
		}

		if (found && count == 1)  { //clean up if only lin in entry
			result = ifs_sbt_remove_entry(
				sbt_fd, &(ent->key));
			fail_if((result != 0), "Error deleting element: %s\n",
			    strerror(errno));
		}
		key = next_key;
	}

	isi_cbm_file_close(file_obj, isi_error_suppress());

	fail_if(!found, "No CSI entry found.");

	unlink(filename);
}
