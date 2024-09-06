#include <check.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_ufp/isi_ufp.h>
#include "isi_cpool_cbm/isi_cbm_gc.h"
#include "isi_cpool_cbm/isi_cbm_archive_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_coi.h"
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"


static cbm_test_env env;

static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

static void delete_all_cfg()
{
	env.cleanup();
}


#define SPEC cbm_test_policy_spec
#define MEMORY_CHECK_SBT_SIZE 8192

static void activate_pause_fail_point(int num, task_state *state)
{
	UFAIL_POINT_CODE(azure_pause_operation, {
		if (num > 5) {
			*state = TASK_PAUSED;
		}
	});

}


static void activate_cancel_fail_point(int num, task_state *state)
{
	UFAIL_POINT_CODE(azure_cancel_operation, {
		if (num > 5) {
			*state = TASK_CANCELLED;
		}
	});

}

class MemoryAzureCheckPoint
{
public:
	MemoryAzureCheckPoint ()
	{
		reset();
	}
	static bool get_ckpt(const void *opaque,
	    const void **out_stream, size_t *size_ckpt,
	    task_state *state, off_t offset)
	{
		num_of_reads_++;
		*state = TASK_NONE;
		activate_cancel_fail_point(num_of_reads_, state);
		activate_pause_fail_point(num_of_reads_, state);

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
	static bool set_ckpt(const void *opaque, const void *in_stream,
	    size_t stream_size, task_state *state, off_t offset)
	{
		num_of_writes_ ++;
		*state = TASK_NONE;
		activate_cancel_fail_point(num_of_writes_, state);
		activate_pause_fail_point(num_of_writes_, state);

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
char  MemoryAzureCheckPoint::buffer_[MEMORY_CHECK_SBT_SIZE];
size_t MemoryAzureCheckPoint::num_stored_ = 0;
int MemoryAzureCheckPoint::num_of_writes_ = 0;
int MemoryAzureCheckPoint::num_of_reads_ = 0;

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	cbm_test_policy_spec specs[] = {
		SPEC(CPT_RAN),
		SPEC(CPT_RAN, true),
		SPEC(CPT_RAN, false, true),
		SPEC(CPT_RAN, true, true),
		SPEC(CPT_RAN, false, false, small_size),
		SPEC(CPT_RAN, false, false, small_size,
		     FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL, 0,
		     3600),
		SPEC(CPT_AZURE),
		SPEC(CPT_AZURE, true),
		SPEC(CPT_AZURE, true, false),
		SPEC(CPT_AZURE, false, true),
		SPEC(CPT_AZURE, true, true),
		SPEC(CPT_AZURE, false, false, small_size),
		SPEC(CPT_AZURE, true, false, small_size),
		SPEC(CPT_AZURE, false, true, small_size),
		SPEC(CPT_AZURE, true, true, small_size)
	};

	env.setup_env(specs, sizeof(specs)/sizeof(cbm_test_policy_spec));
	// now load the config
	isi_cbm_reload_config(& error);

	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));
	
	cpool_regenerate_mek(& error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(& error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(false, true);
	cbm_test_common_setup(false, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Failed to init failpoints");
	cbm_test_leak_check_primer(true);
	restart_leak_detection(0);
}

TEST_FIXTURE(suite_teardown)
{
	delete_all_cfg();
	cbm_test_common_cleanup(false, true);

	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
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

SUITE_DEFINE_FOR_FILE(check_azure_checkpointing,
    .mem_check = CK_MEM_LEAKS,
    .dmalloc_opts = NULL,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .runner_data = NULL,
    .timeout = 120,
    .fixture_timeout = 1200);

#define CREATE_STRING	\
    "dd if=/dev/random of=%s bs=$(( 1024 * 1024 )) count=10"
#define TEST_FILE 	"/ifs/data/check_azure_checkpointing.test"

TEST(test_ran_delete_cloud_objects)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryAzureCheckPoint memckpt;
	isi_cloud::coi coi;

	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status && errno != ENOENT,
	    "Failed to delete file %d",
	    errno);

	// Create a 10MB file
	fmt_print(&str, CREATE_STRING, TEST_FILE);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);


	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;
	cbm_test_fail_point ufp("azure_pause_operation");
	ufp.set("return(1)");

	// Archive it and a PAUSE will happnen in the midway.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, false);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,
	    "Expecting CBM_FILE_PAUSED error  %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status,
	    "Failed to delete file %d",
	    errno);

	// Now that the file is deleted
	// Call archive again, all the cloud objects should
	// be deleted by archive.
	// Resume using the checkpoint.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);

	fail_if(error != NULL,
	    "Error occured while resuming  %#{}",
	    isi_error_fmt(error));
	const void *out_stream = NULL;
	size_t size;
	task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!  %#{}",
	    isi_error_fmt(error));
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo = ckpt.get_mapinfo();
	free((void *)out_stream);
	memckpt.reset();
	isi_cloud_object_id object_id = ckpt_mapinfo.get_object_id();
	ufp.set("off");
	sync();
	// Make sure there is no COI for that file.
	coi.initialize(&error);
	fail_if(error != NULL, "Failed to initialize coi  %#{}",
	    isi_error_fmt(error));
	coi.get_entry(object_id, &error);
	fail_if((error == NULL || isi_system_error_is_a(error, ENOENT) == false),
		 "Found COI or Error is not ENOENT  %#{}",
		 isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;
}


TEST(test_azure_gc)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryAzureCheckPoint memckpt;

	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status && errno != ENOENT,
	    "Failed to delete file %d",
	    errno);

	// Create a 10MB file
	fmt_print(&str, CREATE_STRING, TEST_FILE);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);


	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;

	// Archive the file so we cann apply GC on it
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_AZ,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
	fail_if(error != NULL,
	    "Archive should succeed!  %#{}",
	    isi_error_fmt(error));
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status,
	    "Failed to delete file %d",
	    errno);
	// Get the mapinfo for this file from the checkpoint.
	const void *out_stream = NULL;
	size_t size;
	task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!  %#{}",
	    isi_error_fmt(error)); 
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo = ckpt.get_mapinfo();
	free((void *)out_stream);
	memckpt.reset();
	isi_cloud_object_id object_id = ckpt_mapinfo.get_object_id();

	sync();
	struct timeval date_of_death_tv = { 0, 0 };
	isi_cbm_local_gc(lin, HEAD_SNAPID,
	    object_id, date_of_death_tv,
	    NULL, NULL,
	    NULL,
	    NULL, &error);

	fail_if(error != NULL, "local GC failed error:  %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;
	isi_cbm_cloud_gc(object_id,
	    NULL, NULL,
	    memckpt.get_ckpt,
	    memckpt.set_ckpt, &error);

	fail_if(error != NULL,
	    "Error occured while cloud gc : %#{}",
	    isi_error_fmt(error));

}

TEST(test_azure_checkpoint_gc)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryAzureCheckPoint memckpt;
       	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status && errno != ENOENT,
	    "Failed to delete file %d",
	    errno);

	// Create a 10MB file
	fmt_print(&str, CREATE_STRING, TEST_FILE);
	CHECK_P("\n%s\n", fmt_string(&str));
	status = system(fmt_string(&str));
	fail_if(status, "Could not perform %s", fmt_string(&str));

	fmt_truncate(&str);

	status = -1;

	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;
	cbm_test_fail_point ufp("azure_pause_operation");
	ufp.set("return(1)");

	// Archive it and a PAUSE will happnen in the midway.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_AZ,
	    memckpt.get_ckpt, memckpt.set_ckpt, false);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,
	    "Expecting CBM_FILE_PAUSED error  %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Resume using the checkpoint.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_AZ,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);

	fail_if(error != NULL, 
	    "Error occured while resuming  %#{}",
	    isi_error_fmt(error));

	const void *out_stream = NULL;
	size_t size;
	task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	isi_error_free(error);
	error = NULL;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!");
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo = ckpt.get_mapinfo();
	free((void *)out_stream);
	memckpt.reset();
	isi_cloud_object_id object_id = ckpt_mapinfo.get_object_id();
	ufp.set("off");
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s %d",
	    TEST_FILE, errno);
	sync();
	struct timeval date_of_death_tv = { 0, 0 };
	isi_cbm_local_gc(lin, HEAD_SNAPID,
	    object_id, date_of_death_tv,
	    NULL, NULL,
	    NULL,
	    NULL, &error);

	fail_if(error != NULL, "local GC failed error:  %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;
	ufp.set("off");
	isi_cbm_cloud_gc(object_id,
	    NULL, NULL,
	    memckpt.get_ckpt,
	    memckpt.set_ckpt, &error);

	fail_if(error != NULL,
	    "Error occured while cloud gc : %#{}",
	    isi_error_fmt(error));


}

