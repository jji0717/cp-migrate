#include <check.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <pthread.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h>

#include "unit_test_helpers.h"
#include "isi_cbm_file.h"
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_security/cpool_security.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_cpool_stats/cpool_stats_unit_test_helper.h>
#include "isi_cpool_cbm/isi_cbm_archive_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_coi.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_scoped_ppi.h"
#include "isi_cpool_cbm/isi_cbm_account_util.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "check_cbm_common.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_error.h"
#include "isi_cbm_mapper.h"

#include "isi_cbm_sync.h"
#include "isi_cbm_coi.h"
#include "isi_cbm_gc.h"

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_gc,
					  .mem_check = CK_MEM_LEAKS,
					  .suite_setup = suite_setup,
					  .suite_teardown = suite_teardown,
					  .test_setup = setup_test,
					  .test_teardown = teardown_test,
					  .timeout = 300,
					  .fixture_timeout = 1200);

static cbm_test_env env;
#define SPEC cbm_test_policy_spec
static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

TEST_FIXTURE(suite_setup)
{
	pthread_mutex_t dummymutex;
	pthread_mutex_init(&(dummymutex), NULL);
	pthread_mutex_destroy(&(dummymutex));
	struct isi_error *error = NULL;

	cbm_test_common_setup_gconfig();
	///////先只创建一个account,在云端只有一个大的bucket(利用account_id拼接组合而成.d00*****,m00****)
	cbm_test_policy_spec specs[] = {
		SPEC(CPT_RAN)};
	//	SPEC(CPT_RAN, false, false, small_size)};
	//	SPEC(CPT_AWS, false, false, small_size),
	//	SPEC(CPT_AZURE, false, false, small_size)};

	env.setup_env(specs, sizeof(specs) / sizeof(cbm_test_policy_spec)); 
	isi_cbm_reload_config(&error);
	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));

	fail_if(!cbm_test_cpool_ufp_init());

	//cbm_test_leak_check_primer(true); //jjz  这个函数感觉没什么用
	cpool_stats_unit_test_helper::on_suite_setup();
	restart_leak_detection(0);
}

TEST_FIXTURE(suite_teardown)
{
	//printf("\n%s called\n", __func__);
	env.cleanup();
	isi_cbm_unload_conifg();

	cbm_test_common_cleanup_gconfig();
}

TEST_FIXTURE(setup_test)
{
	cl_provider_module::init();
	cbm_test_enable_stub_access();
}

TEST_FIXTURE(teardown_test)
{
	//printf("\n%s called\n", __func__);
	cl_provider_module::destroy();
	checksum_cache_cleanup();
}
TEST(test_smoke)
{
	printf("\n%s called\n", __func__);
}

// #ifdef WIP
TEST(test_cloud_gc_single_chunk_ran, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *stub;

	const char *path = "/ifs/test_gc_single_chunk_ran.test_file";
	cbm_test_file_helper file_helper(path, 100, "");
	ifs_lin_t lin = file_helper.get_lin();
	cbm_test_policy_spec spec(CPT_RAN);
	printf("\n%s called begin cbm_test_archive_file_with_pol\n", __func__);
	error = cbm_test_archive_file_with_pol(lin, spec.policy_name_.c_str(),
										   false);
	printf("%s called finish cbm_test_archive_file_with_pol\n", __func__);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %{}",
			isi_error_fmt(error));

	stub = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "Error - Unable to open stub");

	isi_cloud_object_id oid(stub->mapinfo->get_object_id());
	ifs_lin_t t_lin = stub->lin;

	isi_cbm_file_close(stub, &error);
	fail_if(error, "Error closing stub");

	fail_if(lin != t_lin, "Error: unequal lins");
	//printf("%s called lin:%ld\n", __func__, lin);
	////isi_run -c:   Allow for direct access to CloudPools stubbed files.
	system("isi_run -c rm -rf /ifs/test_gc_single_chunk_ran.test_file");   ///////删掉这个文件的目的是为了产生ooi entry(减少对文件的引用，只有引用为0,才能cloud gc)

	isi_cbm_cloud_gc_ex(oid, NULL, NULL, NULL, NULL, &error);/////给定一个object_id就能完成cloud gc
	fail_if(error, "%{}", isi_error_fmt(error));

	//remove(path);
}
TEST(test_cloud_gc)
{
	struct isi_error *error = NULL;
	ifs_lin_t lin = 4296614629;////////ifs/test_gc_single_chunk_ran.test_file这个文件的lin值
	struct isi_cbm_file *stub;
	stub = isi_cbm_file_open(lin,HEAD_SNAPID,0,&error);
	fail_if(error, "Error - Unable to open stub");

	isi_cloud_object_id oid(stub->mapinfo->get_object_id()); //////object_id: 00703403240f69ca6846e1e1199f39f5
	isi_cbm_file_close(stub, &error);
	//printf("\n%s called lin:%ld oid:%s\n", __func__, stub->lin, oid.to_string().c_str());
	//isi_cbm_cloud_gc(oid, NULL, NULL, NULL, NULL, &error);
	isi_cbm_cloud_gc_ex(oid, NULL, NULL, NULL, NULL, &error);

}
TEST(test_archive_file) /////这个case为了产生gc的前置的stub文件
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/test_gc_single_chunk_ran.test_file";
	cbm_test_file_helper file_helper(path, 100, "");
	ifs_lin_t lin = file_helper.get_lin();
	cbm_test_policy_spec spec(CPT_RAN);
	printf("\n%s called begin cbm_test_archive_file_with_pol, lin:%ld\n", __func__, lin);
	error = cbm_test_archive_file_with_pol(lin, spec.policy_name_.c_str(),
										   false);
	struct isi_cbm_file *stub;
	stub = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	isi_cloud_object_id oid(stub->mapinfo->get_object_id());
	uint64_t high = oid.get_high();
	uint64_t low = oid.get_low();
	isi_cbm_file_close(stub, &error);
	printf("%s called high:%ld low:%ld\n", __func__, high, low);
}
// #endif

static void
modify_stub_file(ifs_lin_t lin, uint64_t offset)
{
	struct isi_error *error = NULL;
	const char txt[] = "hello, world!";

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open file: %#{}", isi_error_fmt(error));

	isi_cbm_file_write(file, txt, offset, strlen(txt), &error);
	fail_if(error, "failed to write stub file: %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close stub file: %#{}", isi_error_fmt(error));
}

static isi_cloud_object_id
get_object_id(ifs_lin_t lin)
{
	struct isi_error *error = NULL;
	isi_cloud_object_id oid;

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open stub file: %#{}", isi_error_fmt(error));

	oid = file->mapinfo->get_object_id();

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close stub file: %#{}", isi_error_fmt(error));

	return oid;
}

static isi_cbm_account
get_stub_account(ifs_lin_t lin)
{
	struct isi_error *error = NULL;
	struct isi_cbm_account acct;

	struct isi_cbm_file *file =
		isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open stub file: %#{}", isi_error_fmt(error));

	isi_cbm_id aid = file->mapinfo->get_account();

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close file: %#{}", isi_error_fmt(error));

	{
		scoped_ppi_reader ppi_reader;
		struct isi_cfm_policy_provider *ppi =
			(struct isi_cfm_policy_provider *)ppi_reader.get_ppi(&error);

		fail_if(error, "failed to get ppi: %#{}", isi_error_fmt(error));

		isi_cfm_get_account(
			(isi_cfm_policy_provider *)ppi, aid, acct, &error);
		fail_if(error, "failed to get account: %#{}",
				isi_error_fmt(error));
	}

	return acct;
}

static bool
check_cloud_object_exists(isi_cbm_ioh_base *ioh,
						  isi_cloud_object_id &oid, int index, bool disable_clapi_logging)
{
	struct isi_error *error = NULL;
	struct cl_object_name object_name;
	isi_cloud::str_map_t attr_map;

	if (disable_clapi_logging)
	{
		override_clapi_verbose_logging_enabled(false);
	}

	object_name.container_cmo = "unused";
	object_name.container_cdo = ioh->get_account().container_cdo;
	object_name.obj_base_name = oid;

	try
	{
		ioh->get_cdo_attrs(object_name, index, attr_map);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false);

	if (error)
		isi_error_free(error);

	if (disable_clapi_logging)
	{
		revert_clapi_verbose_logging();
	}

	return error == NULL;
}

static bool
check_cmo_exists(isi_cbm_ioh_base *ioh, isi_cloud_object_id &oid)
{
	struct isi_error *error = NULL;
	struct cl_object_name object_name;
	isi_cloud::str_map_t attr_map;

	object_name.container_cmo = ioh->get_account().container_cmo;
	object_name.container_cdo = "unused";
	object_name.obj_base_name = oid;

	try
	{
		ioh->get_cmo_attrs(object_name, attr_map);
	}
	CATCH_CLAPI_EXCEPTION(NULL, error, false);

	if (error)
		isi_error_free(error);

	return error == NULL;
}


static void
cbm_test_cmo_gone_with_policy_spec(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	const char *path = "/ifs/data/cbm_test_cmo_gone_with_policy_spec.txt";
	isi_cbm_sync_option opt = {blocking : true, skip_settle : false};
	isi_cbm_coi_sptr coi;

	cbm_test_file_helper file_helper(path, small_size.chunksize_ * 2, "");
	ifs_lin_t lin = file_helper.get_lin();
	isi_cloud_object_id obj_id[6];

	error = cbm_test_archive_file_with_pol(
		lin, spec.policy_name_.c_str(), false);
	fail_if(error, "failed to archive file: %{}", isi_error_fmt(error));

	isi_cbm_account acct = get_stub_account(lin);
	isi_cbm_ioh_base *ioh =
		isi_cbm_ioh_creator::get_io_helper(acct.type, acct);

	obj_id[0] = get_object_id(lin);

	// 6 COI entries created, 5 of them are used for cloud_gc, 5th created
	// such that other coi entries has no LIN reference
	for (int i = 1; i < 6; ++i)
	{
		modify_stub_file(lin, small_size.chunksize_ * 0);
		isi_cbm_sync_opt(lin, HEAD_SNAPID, NULL, NULL, NULL, opt, &error);
		fail_if(error, "failed to sync stub: %{}", isi_error_fmt(error));
		obj_id[i] = get_object_id(lin);
	}

	for (int i = 0; i < 5; ++i)
		fail_if(!check_cmo_exists(ioh, obj_id[i]));

	// gc version 1 & 3, keep the coi entries
	{
		cbm_test_fail_point ufp("fail_cloud_gc_remove_coi");
		ufp.set("return(1)");

		isi_cbm_cloud_gc(obj_id[1], NULL, NULL, NULL, NULL, &error);
		fail_if(error, "unexpected error: %{}", isi_error_fmt(error));

		isi_cbm_cloud_gc(obj_id[3], NULL, NULL, NULL, NULL, &error);
		fail_if(error, "unexpected error: %{}", isi_error_fmt(error));

		ufp.set("off");
	}

	coi = isi_cbm_get_coi(&error);
	fail_if(error, "failed to get coi: %{}", isi_error_fmt(error));

	// check coi entries all exists
	for (int i = 0; i < 5; ++i)
	{
		coi_entry *coi_entry = coi->get_entry(obj_id[i], &error);
		fail_if(error, "failed to get coi_entry: #{}",
				isi_error_fmt(error));

		delete coi_entry;

		if (i == 1 || i == 3)
			fail_if(check_cmo_exists(ioh, obj_id[i]));
		else
			fail_if(!check_cmo_exists(ioh, obj_id[i]));
	}

	isi_cbm_cloud_gc(obj_id[2], NULL, NULL, NULL, NULL, &error);
	// check coi entries all exists
	for (int i = 0; i < 5; ++i)
	{
		coi_entry *coi_entry = coi->get_entry(obj_id[i], &error);

		if (i == 2)
		{
			fail_if(!error, "expect the coi entry not exists");
			isi_error_free(error);
			error = NULL;
		}
		else
		{
			fail_if(error, "failed to get coi_entry: #{}",
					isi_error_fmt(error));

			delete coi_entry;
		}

		if (i == 1 || i == 2 || i == 3)
			fail_if(check_cmo_exists(ioh, obj_id[i]));
		else
			fail_if(!check_cmo_exists(ioh, obj_id[i]));
	}

	// cleanup all the version
	for (int i = 0; i < 5; ++i)
	{
		isi_cbm_cloud_gc(obj_id[i], NULL, NULL, NULL, NULL, &error);

		if (i == 2)
		{
			fail_if(!error, "expect error");
			isi_error_free(error);
			error = NULL;
		}
		else
		{
			fail_if(error, "unexpected error: %{}", isi_error_fmt(error));
		}
	}

	for (int i = 0; i < 5; ++i)
	{
		coi_entry *coi_entry = coi->get_entry(obj_id[i], &error);

		fail_if(!error || coi_entry, "unexpect coi entry");
		fail_if(check_cmo_exists(ioh, obj_id[i]));

		isi_error_free(error);
		error = NULL;
	}

	delete ioh;
}

TEST(test_cloud_gc_with_cmo_gone)
{
	cbm_test_policy_spec spec(CPT_RAN, false, false, small_size);

	override_clapi_verbose_logging_enabled(false);

	cbm_test_cmo_gone_with_policy_spec(spec);

	revert_clapi_verbose_logging();
}

TEST(test_remove_account) //jji
{
	//cbm_test_remove_account("ran_acct");
	printf("this is test_remove_account\n");
	// cpool_config_context *ctx = cpool_context_open(&error);
	// printf("\n****************\n");
	// fail_if(error, "Failed to open the cpool config: %#{}.\n",
	//     isi_error_fmt(error));

	// struct cpool_account *acct = cpool_account_get_by_name(ctx,
	//     "ran_acct", &error);

	

	// if (acct) {
	// 	uint32_t acct_id = cpool_account_get_id(acct);
	// 	const char *cluster = cpool_account_get_cluster(acct);
	// 	isi_cbm_id account_id(acct_id, cluster);
	// 	cl_provider_type cl_type = cpool_account_get_type(acct);

		
	// 	printf("%s called: acct_id:%u, cluster:%s\n", __func__, acct_id, cluster);
	// 	cpool_account_remove(ctx, acct, &error);
	// 	fail_if(error, "Failed to remove the account: %#{}.\n",
	// 	    isi_error_fmt(error));

	// 	cpool_context_commit(ctx, &error);
	// 	fail_if(error, "Failed to save the GC change: %#{}.\n",
	// 	    isi_error_fmt(error));

	// 	cpool_account_free(acct);
	// }else{printf("\naccount not found\n");}

	// cpool_context_close(ctx);
}
