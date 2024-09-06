#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_ufp/isi_ufp.h>
#include "isi_cpool_cbm/isi_cbm_gc.h"
#include "isi_cpool_cbm/isi_cbm_archive_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_coi.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_scoped_ppi.h"
#include "isi_cpool_cbm/isi_cbm_account_util.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"

#include <check.h>

#define MEMORY_CHECK_SBT_SIZE 8192

static void activate_pause_fail_point(int num, djob_op_job_task_state *state)
{
	UFAIL_POINT_CODE(pause_operation, {
		if (num > 5) {
			*state = DJOB_OJT_PAUSED;
		}
	});

}


static void activate_cancel_fail_point(int num, djob_op_job_task_state *state)
{
	UFAIL_POINT_CODE(cancel_operation, {
		if (num > 5) {
			*state = DJOB_OJT_CANCELLED;
		}
	});

}

class MemoryCheckPoint
{
public:
	MemoryCheckPoint ()
	{
		reset();
	}
	static bool get_ckpt(const void *opaque,
	    void **out_stream, size_t *size_ckpt,
	    djob_op_job_task_state *state, off_t offset)
	{
		num_of_reads_++;
		*state = DJOB_OJT_NONE;
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
	static bool set_ckpt(void *opaque, void *in_stream,
	    size_t stream_size, djob_op_job_task_state *state, off_t offset)
	{
		num_of_writes_ ++;
		*state = DJOB_OJT_NONE;
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
char  MemoryCheckPoint::buffer_[MEMORY_CHECK_SBT_SIZE];
size_t MemoryCheckPoint::num_stored_ = 0;
int MemoryCheckPoint::num_of_writes_ = 0;
int MemoryCheckPoint::num_of_reads_ = 0;

TEST_FIXTURE(suite_setup)
{
	cbm_test_common_setup(true, true);
	fail_if(!cbm_test_cpool_ufp_init(), "Failed to init failpoints");
}

TEST_FIXTURE(suite_teardown)
{
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

SUITE_DEFINE_FOR_FILE(check_pause_resume,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF
#define RM_STRING	CBM_ACCESS_ON "rm -f %s" CBM_ACCESS_OFF
#define CREATE_STRING	CBM_ACCESS_ON \
    "dd if=/dev/random of=%s bs=$(( 1024 * 1024 )) count=10" CBM_ACCESS_OFF
#define TEST_FILE 	"/ifs/data/check_pause_resume.test"


TEST(test_pause_archive)
{
	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryCheckPoint memckpt;
	memckpt.reset();

	// Delete the file
	unlink(TEST_FILE);

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
	cbm_test_fail_point ufp("pause_operation");
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
	// Resume using the checkpoint.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);

	fail_if(error != NULL, 
	    "Error occured while resuming  %#{}",
	    isi_error_fmt(error));
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s", TEST_FILE);
}


TEST(test_cancel_archive)
{
	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryCheckPoint memckpt;
	memckpt.reset();

	// Delete the file
	unlink(TEST_FILE);

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
	cbm_test_fail_point ufp("cancel_operation");
	ufp.set("return(1)");

	// Archive it and a CANCEL will happnen in the midway.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, false);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_CANCELLED) == false,
	    "Expecting CBM_FILE_CANCELLED error: %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s", TEST_FILE);
}

TEST(test_pause_gc)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryCheckPoint memckpt;
	memckpt.reset();

	// Delete the file
	unlink(TEST_FILE);

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

	// Archive the file so we cann apply GC on it
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
		// Make sure we are sending Pause to the caller
	fail_if(error != NULL,
	    "Archive should succeed!");
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s", TEST_FILE);

	// Get the mapinfo for this file from the checkpoint.
	void *out_stream = NULL;
	size_t size;
	djob_op_job_task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!");
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo.copy(ckpt.get_mapinfo(), &error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	free((void *)out_stream);
	memckpt.reset();
	isi_cloud_object_id object_id = ckpt_mapinfo.get_object_id();

	sync();
	scoped_ppi_reader ppi_reader;
	struct isi_cfm_policy_provider *ppi = NULL;

	ppi = (struct isi_cfm_policy_provider *)
	    ppi_reader.get_ppi(&error);
	fail_if(error, "Failed to get ppi: %{}", isi_error_fmt(error));

	struct timeval date_of_death_tv = { 0, 0 };
	isi_cbm_local_gc(lin, HEAD_SNAPID,
	    object_id, date_of_death_tv,
	    ppi, NULL,
	    NULL,
	    NULL, &error);

	fail_if(error != NULL, "local GC failed error:  %#{}",
	    isi_error_fmt(error));

	cbm_test_fail_point ufp("pause_operation");
	ufp.set("return(1)");
	isi_cbm_cloud_gc(object_id,
	    NULL, NULL,
	    memckpt.get_ckpt,
	    memckpt.set_ckpt, &error);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,
	    "Expecting CBM_FILE_PAUSED error");
	isi_error_free(error);
	error = NULL;
	ufp.set("off");
	isi_error_free(error);
	error = NULL;

	// Resume GC from checkpoint.
	isi_cbm_cloud_gc(object_id,
	    NULL, NULL,
	    memckpt.get_ckpt,
	    memckpt.set_ckpt, &error);

	fail_if(error != NULL,
	    "Error occured while resuming : %#{}",
	    isi_error_fmt(error));
}

TEST(test_pause_recall)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryCheckPoint memckpt;
	memckpt.reset();

	// Delete the file
	unlink(TEST_FILE);

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


	// Archive it first, no pause on archive.
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
	fail_if(error != NULL, "Archive failed.error: %#{}",
	    isi_error_fmt(error));

	memckpt.reset();
	cbm_test_fail_point ufp("pause_operation");
	ufp.set("return(1)");
	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL,
	    memckpt.get_ckpt, memckpt.set_ckpt,
	    &error); 


	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,
	    "Expecting CBM_FILE_PAUSED error: %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Resume recalling
	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL,
	    memckpt.get_ckpt, memckpt.set_ckpt,
	    &error); 

	fail_if(error != NULL, 
	    "Error occured while resuming error: %#{}",
	    isi_error_fmt(error));
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s", TEST_FILE);
}

TEST(test_cancel_recall)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryCheckPoint memckpt;
	memckpt.reset();

	// Delete the file
	unlink(TEST_FILE);

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


	// Archive it with no pause on archive
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
	fail_if(error != NULL, "Archive failed.error: %#{}",
	    isi_error_fmt(error));

	memckpt.reset();
	cbm_test_fail_point ufp("cancel_operation");
	ufp.set("return(1)");
	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL,
	    memckpt.get_ckpt, memckpt.set_ckpt,
	    &error); 


	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_CANCELLED) == false,
	    "Expecting CBM_FILE_CANCELLED error: %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status, "Could not unlink test file %s", TEST_FILE);
}

