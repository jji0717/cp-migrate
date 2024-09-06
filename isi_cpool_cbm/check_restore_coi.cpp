#include <check.h>

#include "check_cbm_common.h"
#include "io_helper/isi_cbm_ioh_base.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_restore_coi.h"
#include "isi_cbm_test_util.h"

using namespace isi_cloud;

static cbm_test_env env;

static isi_cbm_test_sizes small_size(4 * 8192, 8192, 1024 * 1024);

static void delete_all_cfg()
{
	env.cleanup();
}

#define SPEC cbm_test_policy_spec

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	cbm_test_policy_spec specs[] = {
		SPEC(CPT_AWS, false, false, small_size),
		SPEC(CPT_AZURE, false, false, small_size),
		SPEC(CPT_RAN, false, false, small_size),
	};

	env.setup_env(specs, sizeof(specs)/sizeof(cbm_test_policy_spec));
	// now load the config
	isi_cbm_reload_config(&error);

	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));

	cbm_test_leak_check_primer(true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
	restart_leak_detection(0);
	isi_cbm_get_coi(isi_error_suppress());
	isi_cbm_get_ooi(isi_error_suppress());
}

TEST_FIXTURE(suite_teardown)
{
	delete_all_cfg();
	isi_cbm_unload_conifg();

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

SUITE_DEFINE_FOR_FILE(check_restore_coi,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 300,
    .fixture_timeout = 1200);

static void
test_restore_coi_x(cl_provider_type type)
{
	struct isi_error *error = NULL;

	// archive two files
	const char *path1 = "/ifs/test_restore_coi_1.txt";
	cbm_test_policy_spec spec(type, false, false, small_size);

	cbm_test_file_helper test_fo1(path1, 17, "AAAAA");
	ifs_lin_t lin1 = test_fo1.get_lin();

	error = cbm_test_archive_file_with_pol(lin1,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	const char *path2 = "/ifs/test_restore_coi_2.txt";
	cbm_test_file_helper test_fo2(path2, 17, "BBBBB");
	ifs_lin_t lin2 = test_fo2.get_lin();

	error = cbm_test_archive_file_with_pol(lin2,
	    spec.policy_name_.c_str(), false);
	fail_if(error, "Failed to archive the file: %#{}.\n",
	    isi_error_fmt(error));

	// simulate the DR case by removing the two COI entries
	struct isi_cbm_file *file1 =
		isi_cbm_file_open(lin1, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file 1 %s, %#{}", path1,
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo1;
	isi_cph_get_stubmap(file1->fd, mapinfo1, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_id acct_id1 = mapinfo1.get_account();
	isi_cloud_object_id id1 = mapinfo1.get_object_id();

	isi_cbm_file_close(file1, &error);
	fail_if(error, "cannot close file 1 %s, %#{}", path1,
	    isi_error_fmt(error));

	isi_cbm_coi_sptr coi = isi_cbm_get_coi(&error);
	fail_if(error, "Failed to get the coi: %#{}.\n",
	    isi_error_fmt(error));


	struct isi_cbm_file *file2 =
		isi_cbm_file_open(lin2, HEAD_SNAPID, 0, &error);
	fail_if(error, "cannot open file 1 %s, %#{}", path2,
	    isi_error_fmt(error));

	isi_cfm_mapinfo mapinfo2;
	isi_cph_get_stubmap(file2->fd, mapinfo2, &error);
	fail_if(error, "Failed to get the mapinfo: %#{}.\n",
	    isi_error_fmt(error));

	isi_cbm_id acct_id2 = mapinfo2.get_account();
	isi_cloud_object_id id2 = mapinfo2.get_object_id();
	isi_cbm_file_close(file2, &error);
	fail_if(error, "cannot close file 1 %s, %#{}", path2,
	    isi_error_fmt(error));

	// remove id1:
	coi->remove_object(id1, true, &error);
	fail_if(error, "Failed to remove the object the coi: %#{}.\n",
	    isi_error_fmt(error));

	// remove id2:
	coi->remove_object(id2, true, &error);
	fail_if(error, "Failed to remove the object the coi: %#{}.\n",
	    isi_error_fmt(error));

	std::auto_ptr<coi_entry> entry1(coi->get_entry(id1,  &error));
	fail_if(entry1.get() != NULL, "The id1 is still found in the COI\n");

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	std::auto_ptr<coi_entry> entry2(coi->get_entry(id2,  &error));
	fail_if(entry1.get() != NULL, "The id2 is still found in the COI\n");

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	// now restore the COIs
	struct timeval now;
	gettimeofday(&now, NULL);

	struct timeval dod;
	dod.tv_sec = now.tv_sec + THREE_MONTHS;
	dod.tv_usec = now.tv_usec;

	isi_cbm_restore_coi_for_acct(acct_id1.get_cluster_id(),
	    acct_id1.get_id(), dod, NULL, NULL, NULL, &error);

	fail_if(error, "Failed to restore the COI entries: %#{}.\n",
	    isi_error_fmt(error));

	entry1.reset(coi->get_entry(id1,  &error));
	fail_if(error, "Failed to find the id1 in the COI entries "
	    "after restore: %#{}.\n",
	    isi_error_fmt(error));

	entry2.reset(coi->get_entry(id2,  &error));
	fail_if(error, "Failed to find the id2 in the COI entries "
	    "after restore: %#{}.\n",
	    isi_error_fmt(error));
}

TEST(test_restore_coi_on_aws)
{
	test_restore_coi_x(CPT_AWS);
}

TEST(test_restore_coi_on_azure)
{
	test_restore_coi_x(CPT_AZURE);
}

TEST(test_restore_coi_on_ran)
{
	test_restore_coi_x(CPT_RAN);
}
