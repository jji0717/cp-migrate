#include <check.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_cpool_config/cpool_config.h>
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


static cbm_test_env env;

static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

static void delete_all_cfg()
{
	env.cleanup();
}


#define SPEC cbm_test_policy_spec
#define MEMORY_CHECK_SBT_SIZE 8192

/////在test_ran_delete_cloud_objects_checkpointing中使用
//在做clear_cloud_gc的时候,收到pause信号
static void activate_delete_pause_fail_point(int num,
    djob_op_job_task_state *state)
{
	printf("%s called num:%d\n", __func__, num);
	UFAIL_POINT_CODE(gc_delete_pause_operation, {
		if (num > 2) {
			*state = DJOB_OJT_PAUSED;
		}
	});

}


static void activate_pause_fail_point(int num, djob_op_job_task_state *state)
{
	printf("%s called num:%d\n", __func__, num);
	UFAIL_POINT_CODE(gc_pause_operation, {
		///////因为这个文件是大文件,需要分多次cdo才能全部upload到云端.每一个cdo upload时,都会write_checkpoint
		/////此时num_of_writes_会++,当num_of_writes_>5时,状态就成为DJOB_OJT_PAUSED
		if (num > 5) { 
			*state = DJOB_OJT_PAUSED;
		}
	});

}


static void activate_cancel_fail_point(int num, djob_op_job_task_state *state)
{
	UFAIL_POINT_CODE(gc_cancel_operation, {
		if (num > 5) {
			*state = DJOB_OJT_CANCELLED;
		}
	}); 

}
static void test_my_failpoint(int *cnt)  /////jjz test failpoint
{
	//int cnt = 0;
	UFAIL_POINT_CODE(gc_my_failpoint,{
		*cnt = RETURN_VALUE;		
	});
	printf("%s called num:%d\n",__func__, *cnt);
}
class MemoryGCCheckPoint
{
public:
	MemoryGCCheckPoint ()
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
		activate_delete_pause_fail_point(num_of_writes_, state);

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
		activate_delete_pause_fail_point(num_of_writes_, state);

		if (stream_size > 0 && stream_size < MEMORY_CHECK_SBT_SIZE) {
			memcpy(buffer_, in_stream, stream_size);
			num_stored_ = stream_size;
			return true;
		}
		num_stored_ = 0;
		memset(buffer_,0, MEMORY_CHECK_SBT_SIZE);
		return false;

	}
	static void reset_counters()
	{
		num_of_writes_ = 0;
		num_of_reads_ = 0;
	}
	static int get_num_of_writes()
	{
		return num_of_writes_;
	}
	static int get_num_of_reads()
	{
		return num_of_reads_;
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
char  MemoryGCCheckPoint::buffer_[MEMORY_CHECK_SBT_SIZE];
size_t MemoryGCCheckPoint::num_stored_ = 0;
int MemoryGCCheckPoint::num_of_writes_ = 0;
int MemoryGCCheckPoint::num_of_reads_ = 0;

void get_object_name_and_ioh(const isi_cloud_object_id &object_id,
    const isi_cfm_mapinfo& stub_map,
    struct cl_object_name &cloud_object_name,
    isi_cbm_ioh_base **cmo_io_helper,
    struct isi_error **error_out)
{
	ASSERT(cmo_io_helper != NULL);

	struct isi_error *error = NULL;
	isi_cbm_id cmo_account_id;
	struct isi_cbm_account cmo_account;
	// cl_cmo_ostream out_stream;
	// isi_cfm_mapinfo stub_map;


	cmo_account_id = stub_map.get_account();

	// Access config under read lock
	{
		scoped_ppi_reader ppi_reader;

		struct isi_cfm_policy_provider *ppi;

		ppi = (struct isi_cfm_policy_provider *)
		    ppi_reader.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");


		isi_cfm_get_account((isi_cfm_policy_provider *)ppi,
		    cmo_account_id, cmo_account, &error);
		if (error != NULL) {
			isi_error_add_context(error,
			    "failed to get account information "
			    "(ID: %u cluster: %s)",
			    cmo_account_id.get_id(),
			    cmo_account_id.get_cluster_id());
			goto out;
		}
	}

	*cmo_io_helper =
	    isi_cbm_ioh_creator::get_io_helper(cmo_account.type, cmo_account);
	if (*cmo_io_helper == NULL) {
		error = isi_cbm_error_new(CBM_NO_IO_HELPER,
		    "failed to get I/O helper for account "
		    "(ID: %u cluster: %s)", cmo_account_id.get_id(),
		    cmo_account_id.get_cluster_id());
		goto out;
	}

	cloud_object_name.container_cmo = stub_map.get_container();
	cloud_object_name.container_cdo = "unused";
	cloud_object_name.obj_base_name = object_id;


 out:


	if (error) {
		ilog(IL_ERR, "Failed getting io helper (%s, %ld), error: %#{}",
		    object_id.to_c_string(), object_id.get_snapid(),
		    isi_error_fmt(error));
		isi_error_handle(error, error_out);
	}
}


// This function is used to verify that
// indeed the cloud objects have been removed
// return true if cloud objects been removed
// return false otherwise.
////验证cloud object是否被删除。如果已被删除了,返回true.如果没有,返回false
///通过对目录做head object，返回error,且error是not found，那么说明已被删除
bool cloud_objects_have_been_removed(const isi_cloud_object_id& object_id,
    isi_cfm_mapinfo& stub_map,
    isi_cbm_ioh_base *cmo_io_helper,
    struct cl_object_name& cloud_object_name,
    struct isi_error **error_out) {
	struct isi_error *error = NULL;
	off_t offset = 0;
	isi_cbm_ioh_base_sptr cdo_io_helper;
	isi_cfm_mapinfo::iterator iter;
	uint64_t cloud_snapid;
	struct cl_object_name cmo_cloud_object_name = cloud_object_name;
	bool result = true;

	// This function intentionally creates network failures - disable
	//  cloud_api network logging
	override_clapi_verbose_logging_enabled(false);

	stub_map.get_containing_map_iterator_for_offset(iter, offset, &error);

	ON_ISI_ERROR_GOTO(out, error);

	for (; iter != stub_map.end(); iter.next(&error)) {
		const isi_cfm_mapentry& map_entry = iter->second;

		/*
		 * We may need a different IO helper object based on the
		 * account ID for this map entry.  Also, update the cloud
		 * object name using the information stored in this map entry.
		 */
		update_io_helper(map_entry, cdo_io_helper, &error);
		if (error != NULL) {
			result = false;
			ON_ISI_ERROR_GOTO(out, error, "offset: %zu length: %zu",
			    map_entry.get_offset(), map_entry.get_length());
		}

		cloud_object_name.container_cmo = "unused";
		cloud_object_name.container_cdo = map_entry.get_container();
		cloud_object_name.obj_base_name = map_entry.get_object_id();
		cloud_snapid = cloud_object_name.obj_base_name.get_snapid();

		{
			size_t bytes_to_scan = map_entry.get_length();
			size_t chunksize = stub_map.get_chunksize();
			/* CMO index is 0, CDOs are indexed from 1. */
			int index = 1;
			// If we are resuming from a checkpoint
			// calculate bytes_to_scan and the index
			// that are appropriate for the offset (basically
			// continue the operation where we stopped).
			// Otherwise offset is updated so we will
			// save the right value to the checkpoint.
			// This will account for in continuous mapping.
			if (offset > map_entry.get_offset() &&
			    (size_t)offset < map_entry.get_offset() +
			    map_entry.get_length()) {
				bytes_to_scan =
				    map_entry.get_length() +
				    map_entry.get_offset() - offset;
				index = 1 +
				    (offset - map_entry.get_offset()) /
				    chunksize;
			} else
				offset =  map_entry.get_offset();
			while (bytes_to_scan > 0) {
				try {
					isi_cloud::str_map_t attr_map;
					cdo_io_helper->get_cdo_attrs(
					    cloud_object_name, index,
					    attr_map);
				}
				CATCH_CLAPI_EXCEPTION(NULL, error, false)

				if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND)) {
					isi_error_free(error);
					error = NULL;
				} else {
					ilog(IL_ERR, "Tried to delete RAN CDO %s snap %lld"
					    "didn't get CBM_CLAPI_OBJECT_NOT_FOUND"
					    " got error %#{}\n",
					    cloud_object_name.container_cdo.c_str(),
					    cloud_object_name.obj_base_name.get_snapid(),
					    isi_error_fmt(error));
					result = false;
					goto out;


				}


				if (bytes_to_scan < chunksize)
					bytes_to_scan = 0;
				else
					bytes_to_scan -= chunksize;
				offset += chunksize;
				++index;

			}
		}
	}
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * With all the CDOs deleted, we can delete the CMO.
	 */
	try {
		isi_cloud::str_map_t attr_map;
		cmo_io_helper->get_cmo_attrs(cmo_cloud_object_name, /////使用http head
		    attr_map);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false)

	if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND)) {
		isi_error_free(error);
		error = NULL;
	} else {
		ilog(IL_ERR, "Deleting cloud object"
		    "error is not CBM_CLAPI_OBJECT_NOT_FOUND"
		    "cmo %s snap %lld error %#{}\n",
		    cloud_object_name.container_cmo.c_str(),
		    cloud_object_name.obj_base_name.get_snapid(),
		    isi_error_fmt(error));
		result = false;
		goto out;

	}


out:
	revert_clapi_verbose_logging();

	isi_error_handle(error, error_out);
	return result;

}
static void toss_your_cookies(void)
{
	throw cl_exception(CL_PARTIAL_FILE, "PreLeak memory from throw");
}

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

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
	try {
		// Throw an exception before running any tests so that
		// the mutexes used by throw will be preallocated and
		// in the memory snapshot used to look for leaks.  In the
		// normal case the memory is cleaned up by image rundown
		// but we do not run down between tests here.
		toss_your_cookies();
	}
	catch (...) {
		CHECK_P("threw and caught an exception to preAllocate memory\n");
	}

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

SUITE_DEFINE_FOR_FILE(check_gc_checkpointing,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 120,
    .fixture_timeout = 1200);

#define CREATE_STRING	\
    "dd if=/dev/random of=%s bs=$(( 1024 * 1024 )) count=10"
#define TEST_FILE 	"/ifs/data/check_azure_checkpointing.test"
TEST(test_failpoint) //jjz  failpoint
{
	cbm_test_fail_point ufp("gc_my_failpoint");
	ufp.set("return(55)");
	int var = 0;
	test_my_failpoint(&var);
	printf("%s called var:%d\n", __func__, var);

}
//1.大文件(10MB)在archive的中途被pause(使用failpoint) 2.大文件被删除或者被修改.
//3再一次archive先前的这个文件，由于主文件被删除或修改，且有checkpoint，那么archive会调用cloud gc
TEST(test_ran_delete_cloud_objects)
{
	printf("\n%s called\n", __func__);
	ifs_lin_t lin;  
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryGCCheckPoint memckpt;
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
	cbm_test_fail_point ufp("gc_pause_operation");
	ufp.set("return(1)");  /////设置failpoint的值: 这里return(1)没意义，return(2)也可以

	// Archive it and a PAUSE will happnen in the midway.
	////这个文件完整的archive到云端需要分为10个chunk依次上传.
	////当archive每一个chunk时，write_checkpoint中会调用set_ckpt_data_func，从而调用activate_pause_fail_point->gc_pause_operation
	///当上传到第5个chunk时，memckpt自身保存的变量num_write_ > 5,使得state_ 成为 PAUSED
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,CBM_TEST_COMMON_POLICY_RAN,memckpt.get_ckpt,memckpt.set_ckpt,false);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,
	    "Expecting CBM_FILE_PAUSED error  %#{}",
	    isi_error_fmt(error));
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
	// Delete the file
	status = unlink(TEST_FILE);   //////上传到一半，然后这个主文件被删掉了
	fail_if(status,
	    "Failed to delete file %d",
	    errno);

	//return;
	//printf("%s called can not be here\n", __func__);
	
	// Now that the file is deleted
	// Call archive again, all the cloud objects should
	// be deleted by archive.
	// Resume using the checkpoint.
	////文件删除后,调用archive后在isi_cbm_archive_by_id这里检查文件是否被删除。
	//若删除,就cloud_gc,不往下做archive。
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);

	//return;
	//printf("%s called can not be here\n", __func__);

	fail_if(error != NULL,
	    "Error occured while resuming  %#{}",
	    isi_error_fmt(error));
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
	    "Unpacking failed!  %#{}",
	    isi_error_fmt(error));
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo.copy(ckpt.get_mapinfo(), &error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

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
	struct cl_object_name cloud_object_name;
	isi_cbm_ioh_base *cmo_io_helper = NULL;
	get_object_name_and_ioh(object_id, ckpt_mapinfo,
	    cloud_object_name, &cmo_io_helper, &error);
	fail_if(error != NULL,
	    "Getting object_name and ioh failed!  %#{}",
	    isi_error_fmt(error));
	////验证是否已经被删除,已被删除,返回true。没有,返回false
	bool result = cloud_objects_have_been_removed(object_id,
	    ckpt_mapinfo, cmo_io_helper,
	    cloud_object_name, &error);
	fail_if(!result,
	    "Removing all cloud object failed!  %#{}",
	    isi_error_fmt(error));
	if (cmo_io_helper != NULL)
		delete cmo_io_helper;
}

//1.大文件(10MB)在archive的中途被pause(使用failpoint) 2.删除这个被archive的主文件.
//3再次archive,发现主文件被删除,那么进入clear cloud_gc,然后再次中途pause(即在做cloud_gc的中途被pause),保存当时的offset
//4.再次对大文件archive,同样的类似于step3,进入cloud_gc,从之前ckpt保存的offset开始,继续删除到完成,然后退出
TEST(test_ran_delete_cloud_objects_checkpointing)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryGCCheckPoint memckpt;
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

	status = -1;

	status = stat(TEST_FILE, &statbuf);
	fail_if((status == -1),
	    "Stat of file %s failed with error: %s (%d)",
	    TEST_FILE, strerror(errno), errno);
	lin = statbuf.st_ino;
	cbm_test_fail_point ufp("gc_pause_operation");
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
	// This time we pause during the delete of the cloud objects
	///由于主文件被删除了,再次调用archive,导致调用clear_cloud_objects(里面会调用cloud gc)
	////在cloud gc的时候,再次pause
	cbm_test_fail_point ufp_delete("gc_delete_pause_operation");
	ufp_delete.set("return(1)");
	// reset the counters so now we count
	// the number of saved checkpoints from zero
	memckpt.reset_counters();

	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
	// Make sure we are sending Pause to the caller
	fail_if(isi_cbm_error_is_a(error, CBM_FILE_PAUSED) == false,  ///////执行clear_cloud_objects执行到一半,被pause
	    "Expecting CBM_FILE_PAUSED error  %#{}",
	    isi_error_fmt(error));
	ufp_delete.set("off");
	isi_error_free(error);
	error = NULL;
	// reset the counters so now we count
	// the number of saved checkpoints from zero
	memckpt.reset_counters();
	// Resume the delete of the objects from a 
	// checkpoint.
	//

	// return;
	// printf("%s called can not be here\n", __func__);
	printf("%s called the last cbm_test_archive_file\n", __func__);
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);

	fail_if(error != NULL,
	    "Error occured while resuming  %#{}",
	    isi_error_fmt(error));
	fail_if(memckpt.get_num_of_writes() != 3,
	    "Didn't restart correctly from a checkpoint. %d",
	    memckpt.get_num_of_writes());
	void *out_stream = NULL;
	size_t size;
	djob_op_job_task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	isi_error_free(error);
	error = NULL;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!  %#{}",
	    isi_error_fmt(error));
	isi_cfm_mapinfo ckpt_mapinfo;
	ckpt_mapinfo.copy(ckpt.get_mapinfo(), &error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	free((void *)out_stream);
	memckpt.reset();
	isi_cloud_object_id object_id = ckpt_mapinfo.get_object_id();
	ufp.set("off");
	isi_error_free(error);
	error = NULL;
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
	struct cl_object_name cloud_object_name;
	isi_cbm_ioh_base *cmo_io_helper = NULL;
	get_object_name_and_ioh(object_id, ckpt_mapinfo,
	    cloud_object_name, &cmo_io_helper, &error);
	fail_if(error != NULL,
	    "Getting object_name and ioh failed!  %#{}",
	    isi_error_fmt(error));
	bool result = cloud_objects_have_been_removed(object_id,
	    ckpt_mapinfo, cmo_io_helper,
	    cloud_object_name, &error);
	fail_if(!result,
	    "Removing all cloud object failed!  %#{}",
	    isi_error_fmt(error));
	if (cmo_io_helper != NULL)
		delete cmo_io_helper;


}

///这个case当gc到中途时，收到pause，保存当前offset到checkpoint. 再次resume gc时，读取checkpoint,解析offset
///从这个offset开始继续gc
TEST(test_ran_gc_checkpointing)
{

	ifs_lin_t lin;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(str);
	int status = -1;
	struct stat statbuf;
	MemoryGCCheckPoint memckpt;
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

	// Archive the file so we cann apply GC on it
	error = cbm_test_archive_file_with_pol_and_ckpt(lin,
	    CBM_TEST_COMMON_POLICY_RAN,
	    memckpt.get_ckpt, memckpt.set_ckpt, true);
		// Make sure we are sending Pause to the caller
	fail_if(error != NULL,
	    "Archive should succeed!");
	// Delete the file
	status = unlink(TEST_FILE);
	fail_if(status,
	    "Failed to delete file %d",
	    errno);
	
	return;
	printf("%s called can not be here\n", __func__);
	// Get the mapinfo for this file from the checkpoint.这里看来只是为了拿：object_id
	void *out_stream = NULL;
	size_t size;
	djob_op_job_task_state state;
	off_t offset;
	void *opaque;
	fail_if(!memckpt.get_ckpt(opaque, &out_stream, &size, &state, offset),  /////opaque, offset用不到
	    "No checkpoint was found after archive");
	isi_cbm_archive_ckpt ckpt;
	ckpt.unpack(out_stream ,size, &error);
	fail_if(error != NULL,
	    "Unpacking failed!  %#{}",
	    isi_error_fmt(error));
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

	struct timeval date_of_death = { 0 };
	isi_cbm_local_gc(lin, HEAD_SNAPID, object_id, date_of_death, ppi, NULL,
	    NULL, NULL, &error);

	fail_if(error != NULL, "local GC failed error:  %#{}",
	    isi_error_fmt(error));
	printf("%s called already unlink test_file\n", __func__);
	/*
	 * Cloud GC requires the cluster have permission to delete a specific
	 * object from the cloud.
	 */
	char *current_guid;
	cpool_get_cluster_guid(&current_guid, &error);
	fail_if(error, "Failed to cluster GUID: %#{}", isi_error_fmt(error));
	smartpools_set_guid_state(ppi->sp_context, CM_A_ST_ENABLED,
	    current_guid);

	cbm_test_fail_point ufp("gc_pause_operation");
	ufp.set("return(1)");
	/////////先做一次cloud gc(需要删除多个chunk),中途被pause, 保存当前的offset到checkpoint,退出
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
	memckpt.reset_counters(); 
	// Resume GC from checkpoint.
	//由于前一次gc保存了checkpoint,解析checkpoint,读出offset,从offset位置开始继续gc
	isi_cbm_cloud_gc(object_id,
	    NULL, NULL,
	    memckpt.get_ckpt,
	    memckpt.set_ckpt, &error);

	// Free the resources to prevent a leak
	smartpools_set_guid_state(ppi->sp_context, CM_A_ST_DISABLED,
	    current_guid);
	free(current_guid);

	fail_if(error != NULL,
	    "Error occured while resuming : %#{}",
	    isi_error_fmt(error));
	fail_if(memckpt.get_num_of_writes() > 5,
	    "Didn't restart correctly from a checkpoint. %d",
	    memckpt.get_num_of_writes());
	struct cl_object_name cloud_object_name;
	isi_cbm_ioh_base *cmo_io_helper = NULL;
	get_object_name_and_ioh(object_id, ckpt_mapinfo,
	    cloud_object_name, &cmo_io_helper, &error);
	fail_if(error != NULL,
	    "Getting object_name and ioh failed!  %#{}",
	    isi_error_fmt(error));
	bool result = cloud_objects_have_been_removed(object_id,
	    ckpt_mapinfo, cmo_io_helper,
	    cloud_object_name, &error);
	fail_if(!result,
	    "Removing all cloud object failed!  %#{}",
	    isi_error_fmt(error));
	if (cmo_io_helper != NULL)
		delete cmo_io_helper;

}
