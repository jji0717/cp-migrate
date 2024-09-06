#include <ifs/ifs_syscalls.h>

#include <isi_cloud_api/cl_exception.h>
#include <isi_cloud_api/cl_provider_common.h>
#include <isi_config/array.h>
#include <isi_config/ifsconfig.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_stats/cpool_stats_unit_test_helper.h>

#include "check_cbm_common.h"
#include "isi_cbm_coi_sbt.h"
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include "isi_cbm_test_util.h"
#include "unit_test_helpers.h"

#include "isi_cbm_coi.h"

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(teardown_suite);
TEST_FIXTURE(setup_test);
TEST_FIXTURE(teardown_test);

SUITE_DEFINE_FOR_FILE(check_coi,
					  .mem_check = CK_MEM_LEAKS,
					  .suite_setup = setup_suite,
					  .suite_teardown = teardown_suite,
					  .test_setup = setup_test,
					  .test_teardown = teardown_test,
					  .timeout = 20,
					  .fixture_timeout = 1200);

uint8_t g_guid[IFSCONFIG_GUID_SIZE];
char g_guid_str[IFSCONFIG_GUID_STR_SIZE];
static uint32_t g_devid;

static cbm_test_env env;

TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	cbm_test_leak_check_primer(true);
	isi_cloud::cl_exception unused(isi_cloud::CL_OK, "unused");

	IfsConfig ifsc;
	arr_config_load_as_ifsconf(&ifsc, &error);
	fail_if(error != NULL, "failure to get cluster guid: %#{}",
			isi_error_fmt(error));
	memcpy(g_guid, ifsc.guid, IFSCONFIG_GUID_SIZE);
	ifsConfFree(&ifsc);

	for (int i = 0; i < IFSCONFIG_GUID_SIZE; i++)
		sprintf(g_guid_str + i * 2, "%02x", g_guid[i]);

	cbm_test_policy_spec specs[] = {
		cbm_test_policy_spec(CPT_RAN),
	};
	env.setup_env(specs, sizeof(specs) / sizeof(cbm_test_policy_spec));
	isi_cbm_reload_config(&error);
	fail_if(error != NULL,
			"failed to load config in preparation for tests: %#{}",
			isi_error_fmt(error));

	g_devid = get_devid(&error);
	fail_if(error != NULL, "error getting devid: %#{}",
			isi_error_fmt(error));

	cpool_stats_unit_test_helper::on_suite_setup();

	fail_if(!cbm_test_cpool_ufp_init(), "failed to initialize UFP");
}

TEST_FIXTURE(teardown_suite)
{
	env.cleanup();
	isi_cloud::cl_provider_module::init();
	isi_cbm_unload_conifg();

	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
}

TEST_FIXTURE(setup_test)
{
	struct isi_error *error = NULL;
	isi_cloud::cl_provider_module::init();
	cbm_test_enable_stub_access();

	isi_cbm_coi_sptr coi;

	coi = isi_cbm_get_coi(&error);
	fail_if(error != NULL, "failure to get COI: %#{}",
			isi_error_fmt(error));

	// remove objects used
	isi_cloud_object_id ids[] = {
		isi_cloud_object_id(1, 2),
		isi_cloud_object_id(10, 20),
		isi_cloud_object_id(20, 30),
		isi_cloud_object_id(100, 200),
		isi_cloud_object_id(101, 200),
		isi_cloud_object_id(300, 400),
	};

	for (size_t i = 0; i < sizeof(ids) / sizeof(isi_cloud_object_id); ++i)
	{
		//////从coi sbt中删除object id对应的coi entry cmo entry
		coi->remove_object(ids[i], true, isi_error_suppress());
	}
}

TEST_FIXTURE(teardown_test)
{
	isi_cloud::cl_provider_module::destroy();

	checksum_cache_cleanup();
}

/* Utility functions. */
static void add_sbt_entry(int sbt_fd, const coi_sbt_key &key,
						  const void *buf, size_t buf_size)
{
	isi_error *error = NULL;
	isi_cbm_coi_sptr coi;

	coi = isi_cbm_get_coi(&error);
	fail_if(error != NULL, "failure to get COI: %#{}",
			isi_error_fmt(error));

	isi_cloud::coi_entry entry;
	entry.set_serialized_entry(CPOOL_COI_VERSION, buf, buf_size, &error);
	fail_if(error, "Failed to deserialize the entry: %#{}",
			isi_error_fmt(error));

	const std::set<ifs_lin_t> &lins = entry.get_referencing_lins();

	std::set<ifs_lin_t>::const_iterator it = lins.begin();
	fail_if(it == lins.end(), "No LINs in the set");

	ifs_lin_t lin = *it;
	struct timeval dod = entry.get_co_date_of_death();
	bool backed = entry.is_backed();
	coi->add_object(entry.get_object_id(), entry.get_archiving_cluster_id(),
					entry.get_archive_account_id(), entry.get_cmo_container(),
					entry.is_compressed(), entry.has_checksum(), lin, &error,
					&dod, &backed);

	fail_if(error, "error adding entry in preparation for test: %#{}",
			isi_error_fmt(error));
}

///////coi_entry_ut coi_ut ooi_entry ooi_ut的定义都在unit_test_helpers.h
TEST(test_entry_serialize_deserialize, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	isi_error *error = NULL;
	coi_entry_ut ce, ce2;

	isi_cloud_object_id object_id(10, 20);
	isi_cbm_id archive_account_id(100, g_guid_str);
	const char *cmo_container = "bananaman";
	struct timeval co_date_of_death = {246135, 0};
	ifs_lin_t referencing_lins[3] = {21, 23, 42};

	const void *serialized = NULL;
	size_t serialized_size;

	std::set<ifs_lin_t>::const_iterator iter;
	std::vector<std::string> what_didnt_match;

	ce.set_object_id(object_id);
	ce.set_archiving_cluster_id(g_guid);
	ce.set_archive_account_id(archive_account_id);
	ce.set_cmo_container(cmo_container);
	ce.update_co_date_of_death(co_date_of_death, false);

	for (int i = 0; i < 3; ++i)
	{
		ce.add_referencing_lin(referencing_lins[i], &error);
		fail_if(error != NULL,
				"failed to add (fake) referencing lin (%llu): %#{}",
				referencing_lins[i], isi_error_fmt(error));
	}

	ce.get_serialized_entry(serialized, serialized_size);

	isi_cbm_id temp_acct_id(100, g_guid_str);
	ce2.set_archive_account_id(temp_acct_id);
	ce2.set_serialized_entry(CPOOL_COI_VERSION, serialized,
							 serialized_size, &error);
	fail_if(error != NULL, "Failed to set serialized entry %#{}",
			isi_error_fmt(error));

	if (ce.verbose_does_not_equal(ce2, what_didnt_match))
	{
		std::string fail_buf;
		fail_buf.append("serialized-then-deserialized "
						"did not match original:");
		std::vector<std::string>::const_iterator iter =
			what_didnt_match.begin();
		for (; iter != what_didnt_match.end(); ++iter)
		{
			fail_buf.append("\n\t");
			fail_buf.append(*iter);
		}

		fail(fail_buf.c_str());
	}
}

TEST(test_jji_smoke)
{
	const char *dir_name = "/ifs/test_remove_referencing_lin_with_snaps";
	const char *file_name = "test_file";
	std::string dir_file_name;
	dir_file_name.assign(dir_name);
	dir_file_name.append("/");
	dir_file_name.append(file_name);
	mkdir(dir_name, 0600);
	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file(dir_file_name.c_str(), 0, "");
	ifs_lin_t test_file_lin = test_file.get_lin();
	printf("\npolicy_name:%s lin:%lld\n", spec.policy_name_.c_str(), test_file_lin); /////policy_name: pol_ran_nc_ne_0_0_0_local_partial_0_0_0
}
TEST(test_remove_referencing_lin_without_snaps)
{
	struct isi_error *error = NULL;
	coi_entry_ut ce;
	int ref_count;

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file(
		"/ifs/test_remove_referencing_lin_without_snaps.test_file", 0, "");
	ifs_lin_t test_file_lin = test_file.get_lin();

	error = cbm_test_archive_file_with_pol(test_file_lin,
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 0,
				"reference_count mismatch (exp: 0 act: %d)", ref_count);

	/*
	 * Add a referencing LIN that has no snapshots.
	 */
	ce.add_referencing_lin(test_file_lin, &error);
	fail_if(error != NULL,
			"unexpected error adding referencing LIN: %#{}",
			isi_error_fmt(error));

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 1,
				"reference_count mismatch (exp: 1 act: %d)", ref_count);

	/*
	 * Verify that after removing the LIN the reference count is back to 0.
	 */
	ce.remove_referencing_lin(test_file_lin, HEAD_SNAPID, &error);
	fail_if(error != NULL, "error removing referenced LIN: %#{}",
			isi_error_fmt(error));

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 0,
				"reference_count mismatch (exp: 0 act: %d)", ref_count);
}

#define RM_COMMAND CBM_ACCESS_ON "rm -rf %s" CBM_ACCESS_OFF

TEST(test_remove_referencing_lin_with_snaps)
{
	struct isi_error *error = NULL;

	const char *dir_name = "/ifs/test_remove_referencing_lin_with_snaps";

	struct fmt FMT_INIT_CLEAN(cleanup_cmd);

	fmt_print(&cleanup_cmd, RM_COMMAND, dir_name);
	fail_if(system(fmt_string(&cleanup_cmd)),
			"failed to clean up directory: %s", dir_name);

	fail_if(mkdir(dir_name, 0600) != 0, "failed to make directory: %s",
			strerror(errno));
	const char *file_name = "test_file";

	std::string dir_file_name;
	dir_file_name.assign(dir_name);
	dir_file_name.append("/");
	dir_file_name.append(file_name);

	time_t now;
	time(&now);
	char snap_name[64];
	snprintf(snap_name, sizeof snap_name, "a_snap_%lld", now);

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file(dir_file_name.c_str(), 0, "");
	ifs_lin_t test_file_lin = test_file.get_lin();

	error = cbm_test_archive_file_with_pol(test_file_lin,
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	coi_entry_ut ce;
	int ref_count;

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 0,
				"reference_count mismatch (exp: 0 act: %d)", ref_count);

	/*
	 * Take a snapshot of the LIN, then add it as a referencing LIN.
	 */
	take_snapshot(dir_name, snap_name, false, &error);
	fail_if(error != NULL, "failed to take snapshot: %#{}",
			isi_error_fmt(error));

	ce.add_referencing_lin(test_file_lin, &error);
	fail_if(error != NULL,
			"unexpected error adding referencing LIN: %#{}",
			isi_error_fmt(error));

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 1,
				"reference_count mismatch (exp: 1 act: %d)", ref_count);

	/*
	 * Verify that after removing the LIN the reference count is back to 0.
	 */
	ce.remove_referencing_lin(test_file_lin, HEAD_SNAPID, &error);
	fail_if(error != NULL, "error removing referenced LIN: %#{}",
			isi_error_fmt(error));

	ref_count = ce.get_reference_count();
	fail_unless(ref_count == 0,
				"reference_count mismatch (exp: 0 act: %d)", ref_count);

	remove_snapshot(snap_name);
	unlink(dir_file_name.c_str());
	rmdir(dir_name);
}

TEST(test_remove_referencing_lin_with_deleted_file)
{
	struct isi_error *error = NULL;

	/*
	 * Create the test file.
	 */
	const char *test_file =
		"/ifs/test_remove_referencing_lin_with_removed_file.test_file";
	int fd = open(test_file, O_WRONLY | O_EXCL | O_CREAT, 0644);
	fail_if(fd == -1, "failed to open test file (errno: %d %s)",
			errno, strerror(errno));

	/*
	 * Add its LIN to the COI entry.
	 */
	coi_entry_ut ce;
	int ref_count = ce.get_reference_count();
	fail_if(ref_count != 0, "reference count mismatch (exp: 0 act: %d)",
			ref_count);

	struct stat stat_buf;
	fail_if(fstat(fd, &stat_buf) == -1,
			"failed to stat test file (errno: %d %s)", errno, strerror(errno));

	ifs_lin_t deleted_lin = stat_buf.st_ino;
	ce.add_referencing_lin(deleted_lin, &error);
	fail_if(error != NULL,
			"unexpected error adding referencing LIN: %#{}",
			isi_error_fmt(error));
	ref_count = ce.get_reference_count();
	fail_if(ref_count != 1, "reference count mismatch (exp: 1 act: %d)",
			ref_count);

	fail_if(close(fd) == -1,
			"failed to close the test file (errno: %d %s)", errno,
			strerror(errno));

	/*
	 * Delete the test file.
	 */
	fail_if(unlink(test_file) == -1,
			"failed to remove test file (errno %d %s)", errno,
			strerror(errno));

	/*
	 * Remove the deleted LIN from the set of referencing LINs.
	 */
	ce.remove_referencing_lin(deleted_lin, HEAD_SNAPID, &error);
	fail_if(error != NULL,
			"unexpected error removing referencing LIN: %#{}",
			isi_error_fmt(error));
}

TEST(test_update_dod, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	coi_entry_ut ce;

	struct timeval co_date_of_death = {246135, 0};
	struct timeval co_date_of_death_later = {346135, 0};
	struct timeval cur_date_of_death = {0};

	ce.update_co_date_of_death(co_date_of_death, false);

	// update DoD to a later time
	ce.update_co_date_of_death(co_date_of_death_later, false);

	cur_date_of_death = ce.get_co_date_of_death();

	fail_if(cur_date_of_death.tv_sec != co_date_of_death_later.tv_sec,
			"fail to update DoD");

	// update DoD to a earlier time without setting force to true
	ce.update_co_date_of_death(co_date_of_death, false);
	cur_date_of_death = ce.get_co_date_of_death();
	fail_if(cur_date_of_death.tv_sec == co_date_of_death.tv_sec,
			"expect DoD no change, but it is updated to %lu",
			cur_date_of_death.tv_sec);

	// force to update DoD
	ce.update_co_date_of_death(co_date_of_death, true);
	cur_date_of_death = ce.get_co_date_of_death();
	fail_if(cur_date_of_death.tv_sec != co_date_of_death.tv_sec,
			"fail to update DoD forcedly");
}

TEST(test_get_entry_matching_entry, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	coi_ut c;
	struct isi_error *error = NULL;
	int coi_sbt_fd;
	coi_entry_ut ce;
	isi_cloud_object_id object_id(10, 20);
	isi_cbm_id archive_account_id(100, g_guid_str);
	const char *cmo_container = "bananaman";
	struct timeval co_date_of_death = {246135, 0};
	ifs_lin_t referencing_lins[3] = {21, 23, 42};
	const void *serialized = NULL;
	size_t serialized_size;
	coi_sbt_key hsbt_key = {0};
	isi_cloud::coi_entry *coi_entry = NULL;

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	ce.set_object_id(object_id);
	ce.set_archiving_cluster_id(g_guid);
	ce.set_archive_account_id(archive_account_id);
	ce.set_cmo_container(cmo_container);
	ce.update_co_date_of_death(co_date_of_death, false);

	for (int i = 0; i < 3; ++i)
	{
		ce.add_referencing_lin(referencing_lins[i], &error);
		fail_if(error != NULL,
				"failed to add (fake) referencing lin (%llu): %#{}",
				referencing_lins[i], isi_error_fmt(error));
	}

	ce.get_serialized_entry(serialized, serialized_size);

	object_id_to_hsbt_key(object_id, hsbt_key);

	add_sbt_entry(coi_sbt_fd, hsbt_key, serialized,
				  serialized_size);

	coi_entry = c.get_entry(object_id, &error);
	fail_if(error != NULL, "unexpected error encountered: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL, "entry expected for existing object id");

	delete coi_entry;
}

TEST(test_get_entry_matching_large_entry)
{
	coi_ut c;
	struct isi_error *error = NULL;
	int coi_sbt_fd;
	coi_entry_ut ce;
	isi_cloud_object_id object_id(10, 20);
	isi_cbm_id archive_account_id(100, g_guid_str);
	const char *cmo_container = "bananaman";
	struct timeval co_date_of_death = {246135, 0};
	const void *serialized = NULL;
	size_t serialized_size;
	coi_sbt_key hsbt_key = {0};
	isi_cloud::coi_entry *coi_entry = NULL;

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	ce.set_object_id(object_id);
	ce.set_archiving_cluster_id(g_guid);
	ce.set_archive_account_id(archive_account_id);
	ce.set_cmo_container(cmo_container);
	ce.update_co_date_of_death(co_date_of_death, false);

	/*
	 * In coi::get_entry, 256 is hardcoded as the initial entry size, so
	 * creating an entry with 40 LINs will be larger than 256 bytes (since
	 * 40 * 8 bytes/LIN = 320 bytes).
	 */
	for (int i = 0; i < 40; ++i)
	{
		ce.add_referencing_lin(i, &error);
		fail_if(error != NULL,
				"failed to add (fake) referencing lin (%llu): %#{}",
				i, isi_error_fmt(error));
	}

	ce.get_serialized_entry(serialized, serialized_size);

	object_id_to_hsbt_key(object_id, hsbt_key);

	add_sbt_entry(coi_sbt_fd, hsbt_key, serialized,
				  serialized_size);

	coi_entry = c.get_entry(object_id, &error);
	fail_if(error != NULL, "unexpected error encountered: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL, "entry expected for existing object id");

	delete coi_entry;
}

TEST(test_get_entry_no_matching_entry, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	coi_ut c;
	struct isi_error *error = NULL;
	isi_cloud::coi_entry *coi_entry = NULL;
	isi_cloud_object_id object_id;

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	coi_entry = c.get_entry(object_id, &error);
	fail_if(error == NULL || !isi_system_error_is_a(error, ENOENT),
			"unexpected error encountered: %#{}", isi_error_fmt(error));
	fail_if(coi_entry != NULL,
			"no entry should be returned for a non-existent object id");

	isi_error_free(error);
}

TEST(test_add_object, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id(1234, g_guid_str);
	const char *cmo_container = "12";
	bool is_compressed = false;
	bool has_checksum = false;
	ifs_lin_t referencing_lin = 12345;
	int coi_sbt_fd;
	size_t retrieved_entry_buf_size = 256;
	void *retrieved_entry_buf = malloc(retrieved_entry_buf_size);
	fail_if(retrieved_entry_buf == NULL);
	coi_sbt_key hsbt_key = {0};

	object_id_to_hsbt_key(object_id, hsbt_key);

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();
	/////实际中在isi_cbm_archive_common中会调用coi.add_object(object_id);
	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lin,
				 &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	coi_sbt_get_entry(coi_sbt_fd, &hsbt_key,
					  retrieved_entry_buf, &retrieved_entry_buf_size, NULL, &error);
	fail_if(error != 0,
			"unexpected error retrieving entry from COI: %#{}",
			isi_error_fmt(error));

	coi_entry_ut ce;
	ce.set_serialized_entry(CPOOL_COI_VERSION, retrieved_entry_buf,
							retrieved_entry_buf_size, &error);
	fail_if(error != NULL, "Failed while setting serialized entry %#{}",
			isi_error_fmt(error));

	isi_cloud_object_id retrieved_object_id = ce.get_object_id();
	fail_if(retrieved_object_id != object_id,
			"object id mismatch (exp: (%s  %ld) act: (%s %ld))",
			object_id.to_c_string(), object_id.get_snapid(),
			retrieved_object_id.to_c_string(),
			retrieved_object_id.get_snapid());

	int retrieved_version = ce.get_version();
	fail_if(retrieved_version != CPOOL_COI_VERSION,
			"version mismatch (exp: %d act: %d)", CPOOL_COI_VERSION,
			retrieved_version);

	const uint8_t *retrieved_archiving_cluster_id =
		ce.get_archiving_cluster_id();
	if (memcmp(retrieved_archiving_cluster_id, g_guid,
			   IFSCONFIG_GUID_SIZE) != 0)
	{
		char error_message[256];
		snprintf(error_message, sizeof(error_message),
				 "archiving_cluster_id_mismatch (exp: ");
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; ++i)
			sprintf(&error_message[strlen(error_message)],
					"%02x", g_guid[i]);
		strcat(error_message, " act: ");
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; ++i)
			sprintf(&error_message[strlen(error_message)],
					"%02x", retrieved_archiving_cluster_id[i]);
		strcat(error_message, ")");
		fail(error_message);
	}

	isi_cbm_id retrieved_archive_account_id = ce.get_archive_account_id();
	fail_if(!retrieved_archive_account_id.equals(archive_account_id),
			"archive_account_id mismatch (exp: %u/%s act: %u/%s)",
			archive_account_id.get_id(), archive_account_id.get_cluster_id(),
			retrieved_archive_account_id.get_id(),
			retrieved_archive_account_id.get_cluster_id());

	const char *retrieved_cmo_container = ce.get_cmo_container();
	fail_if(strcmp(retrieved_cmo_container, cmo_container) != 0,
			"cmo_container mismatch (exp: %d act: %d)",
			cmo_container, retrieved_cmo_container);

	bool retrieved_is_compressed = ce.is_compressed();
	fail_if(retrieved_is_compressed != is_compressed,
			"compression value mismatch (exp: %s act: %s)",
			is_compressed ? "TRUE" : "FALSE",
			retrieved_is_compressed ? "TRUE" : "FALSE");

	int retrieved_ref_count = ce.get_reference_count();
	fail_if(retrieved_ref_count != 1,
			"reference count mismatch (exp: 1 act: %d)", retrieved_ref_count);

	std::set<ifs_lin_t> referencing_lins = ce.get_referencing_lins();
	fail_if(referencing_lins.size() != 1,
			"reference count mismatch (exp: 1 act: %d)",
			referencing_lins.size());
	fail_if(referencing_lins.find(referencing_lin) ==
				referencing_lins.end(),
			"expected referencing LIN not found");

	btree_key_t ooi_entry_key = ce.get_ooi_entry_key();
	fail_if(ooi_entry_key.keys[0] != coi_entry::NULL_BTREE_KEY.keys[0] ||
				ooi_entry_key.keys[1] != coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			ooi_entry_key.keys[0], ooi_entry_key.keys[1]);

	free(retrieved_entry_buf);
}

TEST(test_add_object_with_existing_object, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = true;
	bool has_checksum = true;
	ifs_lin_t referencing_lin = 12345;

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lin, &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lin, &error);
	fail_if(error == NULL,
			"expected but did not receive error from adding existing object");
	isi_error_free(error);
	error = NULL;
}

////////调用coi.remove_object(object_id)之前先要调用coi.removing_object(object_id)目的是标记这个object_id,
////阻止后续添加新的lin引用到这个coi_entry
TEST(test_remove_object_not_ignoring_ref_count, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c; /////unit_test_helpers.h
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = true;

	bool has_checksum = true;

	int coi_sbt_fd;
	coi_entry_ut ce;

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file(
		"/ifs/test_remove_object_not_ignoring_ref_count.test_file", 0, "");
	ifs_lin_t referencing_lin = test_file.get_lin();

	error = cbm_test_archive_file_with_pol(referencing_lin,
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lin, &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	/*
	 * First try to mark impending removal for the object with a ref count
	 * greater than 0.
	 */
	c.removing_object(object_id, &error);
	fail_if(error == NULL,
			"expected but did not get error from marking removal of COI entry"
			"with reference count greater than 0");
	fail_if(!isi_system_error_is_a(error, EINVAL),
			"Expected EINVAL but got: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	/*
	 * First try to remove the object with a ref count greater than 0.
	 */
	c.remove_object(object_id, false, &error);
	fail_if(error == NULL,
			"expected but did not get error from removing entry from COI "
			"with non-ignored reference count greater than 0");
	isi_error_free(error);
	error = NULL;

	struct timeval date_of_death_tv;
	gettimeofday(&date_of_death_tv, NULL);
	c.remove_ref(object_id, referencing_lin, HEAD_SNAPID, date_of_death_tv,
				 NULL, &error);
	fail_if(error != NULL,
			"unexpected error removing reference from COI entry: %#{}",
			isi_error_fmt(error));

	/*
	 * Now that the reference has been removed, try to mark the
	 * coi entry for removal again
	 */
	c.removing_object(object_id, &error);
	fail_if(error != NULL,
			"unexpected error marking removal of entry from COI "
			"with reference count of 0: %#{}",
			isi_error_fmt(error));

	/*
	 * Confirm that we can't add any references to the coi entry after
	 * it has been marked for removal
	 */
	c.add_ref(object_id, referencing_lin, &error);
	fail_if(error == NULL,
			"Expected but did not get error adding reference: %#{}",
			isi_error_fmt(error));
	fail_if(!isi_cbm_error_is_a(error, CBM_INTEGRITY_FAILURE),
			"Expected CBM_INTEGRITY_FAILURE but got: %#{}",
			isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	/*
	 * And, remove the object.
	 */
	c.remove_object(object_id, false, &error);
	fail_if(error != NULL,
			"unexpected error removing entry from COI "
			"with reference count of 0: %#{}",
			isi_error_fmt(error));
}

TEST(test_remove_object_ignoring_ref_count, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = true;
	bool has_checksum = true;
	int coi_sbt_fd;
	coi_entry_ut ce;

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file(
		"/ifs/test_remove_object_ignoring_ref_count.test_file", 0, "");
	ifs_lin_t referencing_lin = test_file.get_lin();

	error = cbm_test_archive_file_with_pol(referencing_lin,
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lin, &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	c.removing_object(object_id, &error);
	fail_if(error == NULL,
			"expected but did not get error from marking removal of COI entry"
			"with reference count greater than 0");
	fail_if(!isi_system_error_is_a(error, EINVAL),
			"Expected EINVAL but got: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	c.remove_object(object_id, true, &error);
	fail_if(error != NULL,
			"unexpected error removing entry from COI "
			"(ignoring ref count): %#{}",
			isi_error_fmt(error));
}

TEST(test_add_ref, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = false;
	bool has_checksum = false;
	ifs_lin_t referencing_lins[] = {12345, 98765};

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	coi_sbt_key hsbt_key = {0};
	object_id_to_hsbt_key(object_id, hsbt_key);

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lins[0], &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error != NULL,
			"unexpected error from adding reference: %#{}",
			isi_error_fmt(error));

	int coi_sbt_fd = c.get_coi_sbt_fd();

	size_t retrieved_entry_buf_size = 256;
	void *retrieved_entry_buf = malloc(retrieved_entry_buf_size);
	fail_if(retrieved_entry_buf == NULL);

	coi_sbt_get_entry(coi_sbt_fd, &hsbt_key,
					  retrieved_entry_buf, &retrieved_entry_buf_size, NULL, &error);
	fail_if(error != 0,
			"unexpected error retrieving entry from COI: %#{}",
			isi_error_fmt(error));

	coi_entry_ut ce;
	ce.set_serialized_entry(CPOOL_COI_VERSION, retrieved_entry_buf, retrieved_entry_buf_size, &error);
	free(retrieved_entry_buf);
	fail_if(error != NULL, "Failed while setting serialized entry %#{}",
			isi_error_fmt(error));

	int retrieved_ref_count = ce.get_reference_count();
	fail_if(retrieved_ref_count != 2,
			"reference count mismatch (exp: 2 act: %d)", retrieved_ref_count);

	btree_key_t ooi_entry_key = ce.get_ooi_entry_key();
	fail_if(ooi_entry_key.keys[0] != coi_entry::NULL_BTREE_KEY.keys[0] ||
				ooi_entry_key.keys[1] != coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			ooi_entry_key.keys[0], ooi_entry_key.keys[1]);
}
TEST(test_add_ref_jji)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = false;
	bool has_checksum = false;
	ifs_lin_t referencing_lins[] = {12345, 98765};

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI");
	coi_sbt_key hsbt_key = {0};
	object_id_to_hsbt_key(object_id, hsbt_key);

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lins[0], &error);
	fail_if(error != NULL, "unexpected error from adding object");

	int coi_sbt_fd = c.get_coi_sbt_fd(); ////测试api, unit_test_helpers.h
	size_t retrieved_entry_buf_size = 256;
	void *retrieved_entry_buf = malloc(retrieved_entry_buf_size);
	fail_if(retrieved_entry_buf == NULL);

	coi_sbt_get_entry(coi_sbt_fd, &hsbt_key,
					  retrieved_entry_buf, &retrieved_entry_buf_size, NULL, &error);

	coi_entry_ut ce;
	ce.set_serialized_entry(CPOOL_COI_VERSION, retrieved_entry_buf,
							retrieved_entry_buf_size, &error);
	free(retrieved_entry_buf);
	int retrieved_ref_cnt = ce.get_reference_count();
	printf("\n ref_cnt:%d\n", retrieved_ref_cnt);

	btree_key_t ooi_entry_key = ce.get_ooi_entry_key();
	printf("ooi entry key: 0x%016lx  0x%016lx\n", ooi_entry_key.keys[0],
		   ooi_entry_key.keys[1]);
}
TEST(test_add_ref_existing, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = false;
	bool has_checksum = false;
	ifs_lin_t referencing_lins[] = {12345, 98765};

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lins[0],
				 &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error != NULL,
			"unexpected error from adding reference: %#{}",
			isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[0], &error);
	fail_if(error == NULL, "expected EALREADY");
	fail_if(!isi_system_error_is_a(error, EALREADY),
			"expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	c.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error == NULL, "expected EALREADY");
	fail_if(!isi_system_error_is_a(error, EALREADY),
			"expected EALREADY: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;
}

TEST(test_remove_ref, mem_check
	 : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	coi_ut c;
	isi_cloud_object_id object_id(100, 200);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "12";
	bool is_compressed = true;

	int retrieved_ref_count;
	bool has_checksum = true;

	int coi_sbt_fd;
	size_t retrieved_entry_buf_size = 256;
	void *retrieved_entry_buf = malloc(retrieved_entry_buf_size);
	fail_if(retrieved_entry_buf == NULL);
	coi_entry_ut ce;

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file_1(
		"/ifs/test_remove_ref.test_file.1", 0, "");
	cbm_test_file_helper test_file_2(
		"/ifs/test_remove_ref.test_file.2", 0, "");

	ifs_lin_t referencing_lins[] = {
		test_file_1.get_lin(),
		test_file_2.get_lin()};

	error = cbm_test_archive_file_with_pol(test_file_1.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	error = cbm_test_archive_file_with_pol(test_file_2.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	c.add_object(object_id, g_guid, archive_account_id,
				 cmo_container, is_compressed, has_checksum, referencing_lins[0],
				 &error);
	fail_if(error != NULL,
			"unexpected error from adding object: %#{}", isi_error_fmt(error));

	struct timeval date_of_death_tv;
	gettimeofday(&date_of_death_tv, NULL);
	c.remove_ref(object_id, referencing_lins[1], HEAD_SNAPID, date_of_death_tv,
				 NULL, &error);
	fail_if(error == NULL, "expected CBM_LIN_DOES_NOT_EXIST");
	fail_if(!isi_cbm_error_is_a(error, CBM_LIN_DOES_NOT_EXIST),
			"expected CBM_LIN_DOES_NOT_EXIST: %#{}", isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	c.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error != NULL,
			"unexpected error from adding reference: %#{}",
			isi_error_fmt(error));

	coi_sbt_key hsbt_key = {0};
	object_id_to_hsbt_key(object_id, hsbt_key);
	coi_sbt_get_entry(coi_sbt_fd, &hsbt_key,
					  retrieved_entry_buf, &retrieved_entry_buf_size, NULL, &error);
	fail_if(error != 0,
			"unexpected error retrieving entry from COI: %#{}",
			isi_error_fmt(error));

	ce.set_serialized_entry(CPOOL_COI_VERSION, retrieved_entry_buf,
							retrieved_entry_buf_size, &error);
	free(retrieved_entry_buf);
	fail_if(error != NULL, "Failed while setting serialized entry %#{}",
			isi_error_fmt(error));

	retrieved_ref_count = ce.get_reference_count();
	fail_if(retrieved_ref_count != 2,
			"reference count mismatch (exp: 2 act: %d)", retrieved_ref_count);

	c.remove_ref(object_id, referencing_lins[1], HEAD_SNAPID, date_of_death_tv,
				 NULL, &error);
	fail_if(error != NULL,
			"unexpected error received from removing referenced LIN: %#{}",
			isi_error_fmt(error));
}

/*
 * Remove references for an object ID from the COI and verify the value of the
 * effective date of death.
 */
TEST(test_remove_ref_effective_dod)
{
	struct isi_error *error = NULL;

	coi_ut c;
	int coi_sbt_fd = -1;
	isi_cloud_object_id object_id(20, 30);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "";
	bool is_compressed = false;
	bool has_checksum = false;
	struct timeval date_of_death_tv = {0};
	struct timeval effective_dod = {0};

	cbm_test_policy_spec spec(CPT_RAN);
	cbm_test_file_helper test_file_1(
		"/ifs/test_remove_ref.test_file.1", 0, "");
	cbm_test_file_helper test_file_2(
		"/ifs/test_remove_ref.test_file.2", 0, "");
	cbm_test_file_helper test_file_3(
		"/ifs/test_remove_ref.test_file.3", 0, "");
	cbm_test_file_helper test_file_4(
		"/ifs/test_remove_ref.test_file.4", 0, "");

	ifs_lin_t referencing_lins[] = {
		test_file_1.get_lin(),
		test_file_2.get_lin(),
		test_file_3.get_lin(),
		test_file_4.get_lin()};

	error = cbm_test_archive_file_with_pol(test_file_1.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	error = cbm_test_archive_file_with_pol(test_file_2.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	error = cbm_test_archive_file_with_pol(test_file_3.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	error = cbm_test_archive_file_with_pol(test_file_4.get_lin(),
										   spec.policy_name_.c_str(), false);
	fail_if(error != NULL,
			"failed to archive file in preparation for test: %#{}",
			isi_error_fmt(error));

	c.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));
	coi_sbt_fd = c.get_coi_sbt_fd();

	/*
	 * Add an object and several additional references to the COI.
	 */
	c.add_object(object_id, g_guid, archive_account_id, cmo_container,
				 is_compressed, has_checksum, referencing_lins[0], &error);
	fail_if(error != NULL, "error adding object to COI: %#{}",
			isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error != NULL, "error adding reference to COI: %#{}",
			isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[2], &error);
	fail_if(error != NULL, "error adding reference to COI: %#{}",
			isi_error_fmt(error));

	c.add_ref(object_id, referencing_lins[3], &error);
	fail_if(error != NULL, "error adding reference to COI: %#{}",
			isi_error_fmt(error));

	/*
	 * Remove one of the references, ignoring effective date of death.
	 */
	date_of_death_tv.tv_sec = 12345;
	c.remove_ref(object_id, referencing_lins[1], HEAD_SNAPID, date_of_death_tv,
				 NULL, &error);
	fail_if(error != NULL, "error removing reference from COI: %#{}",
			isi_error_fmt(error));

	/*
	 * Remove one of the references and verify that the date of death is
	 * the correct value.
	 */
	date_of_death_tv.tv_sec = 123;
	c.remove_ref(object_id, referencing_lins[3], HEAD_SNAPID, date_of_death_tv,
				 &effective_dod, &error);
	fail_if(error != NULL, "error removing reference from COI: %#{}",
			isi_error_fmt(error));
	fail_unless(effective_dod.tv_sec == 12345,
				"effective date of death mismatch (exp: 12345 act: %d)",
				effective_dod);

	/*
	 * Remove one of the references using a smaller date of death and
	 * verify the effective date of death is still the previous value.
	 */
	date_of_death_tv.tv_sec = 1234;
	c.remove_ref(object_id, referencing_lins[2], HEAD_SNAPID, date_of_death_tv,
				 &effective_dod, &error);
	fail_if(error != NULL, "error removing reference from COI: %#{}",
			isi_error_fmt(error));
	fail_unless(effective_dod.tv_sec == 12345,
				"effective date of death mismatch (exp: 12345 act: %d)",
				effective_dod.tv_sec);

	/*
	 * Remove one of the references using a larger date of death and
	 * verify the effective date of death is now the new value.
	 */
	date_of_death_tv.tv_sec = 123456;
	c.remove_ref(object_id, referencing_lins[0], HEAD_SNAPID, date_of_death_tv,
				 &effective_dod, &error);
	fail_if(error != NULL, "error removing reference from COI: %#{}",
			isi_error_fmt(error));
	fail_unless(effective_dod.tv_sec == 123456,
				"effective date of death mismatch (exp: 123456 act: %d)",
				effective_dod.tv_sec);
}

/*
 * Test the addition and removal of referencing LINs to verify the
 * ooi entry key.
 */
TEST(test_add_remove_ref__ooi_entry_key_check)
{
	struct isi_error *error = NULL;

	coi_ut coi;
	coi.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	ooi_ut ooi;
	ooi.initialize(&error);
	fail_if(error != NULL, "failed to initialize OOI: %#{}",
			isi_error_fmt(error));

	isi_cloud_object_id object_id(100, 200);
	object_id.set_snapid(0);

	isi_cbm_id archive_account_id;
	ifs_lin_t referencing_lins[3] = {12345, 23456, 34567};

	/* Add a cloud object ID to the COI. */
	coi.add_object(object_id, g_guid, archive_account_id,
				   "cmoContainerName", false, false, referencing_lins[0], &error);
	fail_if(error != NULL, "failed to add cloud object ID to COI: %#{}",
			isi_error_fmt(error));

	/*
	 * Retrieve the COI entry; the OOI entry key should be NULL_BTREE_KEY.
	 */
	coi_entry *coi_entry = coi.get_entry(object_id, &error);
	fail_if(error != NULL,
			"failed to retrieve cloud object ID from COI: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL);

	fail_if(coi_entry->get_ooi_entry_key().keys[0] !=
					coi_entry::NULL_BTREE_KEY.keys[0] ||
				coi_entry->get_ooi_entry_key().keys[1] !=
					coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			coi_entry->get_ooi_entry_key().keys[0],
			coi_entry->get_ooi_entry_key().keys[1]);

	delete coi_entry;

	/* Add a second reference to the cloud object ID. */
	coi.add_ref(object_id, referencing_lins[1], &error);
	fail_if(error != NULL, "failed to add referencing LIN: %#{}",
			isi_error_fmt(error));

	/*
	 * Retrieve the COI entry again; the OOI entry key should still be
	 * NULL_BTREE_KEY.
	 */
	coi_entry = coi.get_entry(object_id, &error);
	fail_if(error != NULL,
			"failed to retrieve cloud object ID from COI: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL);

	fail_if(coi_entry->get_ooi_entry_key().keys[0] !=
					coi_entry::NULL_BTREE_KEY.keys[0] ||
				coi_entry->get_ooi_entry_key().keys[1] !=
					coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			coi_entry->get_ooi_entry_key().keys[0],
			coi_entry->get_ooi_entry_key().keys[1]);

	delete coi_entry;

	/*
	 * Remove one of the references from the COI entry.  With a reference
	 * still remaining the OOI entry key should still be NULL_BTREE_KEY.
	 * Here we'll change the date of death to the value that will be used
	 * later when verifying the OOI entry key.
	 */
	struct timeval date_of_death_tv = {500, 0};
	struct timeval upd_date_of_death_tv = date_of_death_tv;
	coi.remove_ref(object_id, referencing_lins[0], HEAD_SNAPID,
				   date_of_death_tv, NULL, &error);
	fail_if(error != NULL, "failed to remove referencing LIN: %#{}",
			isi_error_fmt(error));

	coi_entry = coi.get_entry(object_id, &error);
	fail_if(error != NULL,
			"failed to retrieve cloud object ID from COI: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL);

	fail_if(coi_entry->get_ooi_entry_key().keys[0] !=
					coi_entry::NULL_BTREE_KEY.keys[0] ||
				coi_entry->get_ooi_entry_key().keys[1] !=
					coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			coi_entry->get_ooi_entry_key().keys[0],
			coi_entry->get_ooi_entry_key().keys[1]);

	delete coi_entry;
	printf("\n before remove 2 ref lins,ooi entry key:0x%016lx,  0x%016lx\n",
		   coi_entry->get_ooi_entry_key().keys[0], coi_entry->get_ooi_entry_key().keys[1]);
	/*
	 * Remove the last reference from the COI entry.  This should add an
	 * entry to the OOI and update the OOI entry key.
	 */
	//////最开始coi_entry中有2个ref lin,现在2个ref lin都被删除了,没有对这个object id的引用了,因此产生了ooi entry
	upd_date_of_death_tv.tv_sec -= 10;
	coi.remove_ref(object_id, referencing_lins[1], HEAD_SNAPID,
				   upd_date_of_death_tv, NULL, &error);
	fail_if(error != NULL, "failed to remove referencing LIN: %#{}",
			isi_error_fmt(error));

	ooi_entry *ooi_entry = ooi.get_entry(date_of_death_tv, g_devid, 1,
										 &error);
	fail_if(error != NULL, "failed to retrieve entry from OOI: %#{}",
			isi_error_fmt(error));
	fail_if(ooi_entry == NULL);

	fail_if(ooi_entry->get_cloud_objects().find(object_id) ==
				ooi_entry->get_cloud_objects().end(),
			"failed to find object ID in OOI entry: %#{}",
			isi_error_fmt(error));

	btree_key_t expected_ooi_entry_key = ooi_entry->to_btree_key();

	coi_entry = coi.get_entry(object_id, &error);
	fail_if(error != NULL,
			"failed to retrieve cloud object ID from COI: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL);

	// printf("\n after remove 2 ref lins,ooi entry key:0x%016lx,  0x%016lx\n",
	// 	   expected_ooi_entry_key.keys[0], expected_ooi_entry_key.keys[1]);
	// printf("\n after remove 2 ref lins,ooi entry key:0x%016lx,  0x%016lx\n",
	// 	   coi_entry->get_ooi_entry_key().keys[0], coi_entry->get_ooi_entry_key().keys[1]);
	// return;

	fail_if(coi_entry->get_ooi_entry_key().keys[0] !=
					expected_ooi_entry_key.keys[0] ||
				coi_entry->get_ooi_entry_key().keys[1] !=
					expected_ooi_entry_key.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			expected_ooi_entry_key.keys[0],
			expected_ooi_entry_key.keys[1],
			coi_entry->get_ooi_entry_key().keys[0],
			coi_entry->get_ooi_entry_key().keys[1]);

	delete ooi_entry;
	delete coi_entry;

	/*
	 * Add a reference to the COI entry.  This should remove the cloud
	 * object ID from the OOI (the entire entry actually, since it was the
	 * only object ID being stored in the entry) and reset the OOI entry
	 * key to NULL_BTREE_KEY.
	 */
	coi.add_ref(object_id, referencing_lins[2], &error);
	fail_if(error != NULL, "failed to add referencing LIN: %#{}",
			isi_error_fmt(error));

	ooi_entry = ooi.get_entry(date_of_death_tv, g_devid, 1,
							  &error);
	fail_if(error == NULL, "expected ENOENT");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"expected ENOENT: %#{}", isi_error_fmt(error));
	fail_if(ooi_entry != NULL);

	isi_error_free(error);
	error = NULL;

	coi_entry = coi.get_entry(object_id, &error);
	fail_if(error != NULL,
			"failed to retrieve cloud object ID from COI: %#{}",
			isi_error_fmt(error));
	fail_if(coi_entry == NULL);

	fail_if(coi_entry->get_ooi_entry_key().keys[0] !=
					coi_entry::NULL_BTREE_KEY.keys[0] ||
				coi_entry->get_ooi_entry_key().keys[1] !=
					coi_entry::NULL_BTREE_KEY.keys[1],
			"ooi entry key mismatch "
			"(exp: 0x%016lx/0x%016lx act: 0x%016lx/0x%016lx)",
			coi_entry::NULL_BTREE_KEY.keys[0],
			coi_entry::NULL_BTREE_KEY.keys[1],
			coi_entry->get_ooi_entry_key().keys[0],
			coi_entry->get_ooi_entry_key().keys[1]);

	delete coi_entry;
}

/*
 * Test the validation of a coi entry
 */
TEST(test_entry_validation)
{
	struct isi_error *error = NULL;

	coi_ut coi;
	coi.initialize(&error);
	fail_if(error != NULL, "failed to initialize COI: %#{}",
			isi_error_fmt(error));

	const int num_objects = 2;
	isi_cloud_object_id object_id[num_objects];
	isi_cbm_id archive_account_id;
	ifs_lin_t referencing_lins[num_objects] = {12345, 6789};
	coi_entry *coi_entry[num_objects];
	const void *serialization[num_objects];
	size_t serialization_size[num_objects];

	for (int i = 0; i < num_objects; i++)
	{
		object_id[i].set(i + 100, 200);
		object_id[i].set_snapid(0);

		/* Add above cloud objects to the COI. */
		coi.add_object(object_id[i], g_guid, archive_account_id,
					   "cmoContainerName1", false, false, referencing_lins[i],
					   &error);
		fail_if(error != NULL,
				"failed to add cloud object ID[%d]  to COI: %#{}",
				i, isi_error_fmt(error));
		/*
		 * Retrieve the COI entry and the serialized content thereof for
		 * the objects
		 */
		coi_entry[i] = coi.get_entry(object_id[i], &error);
		coi_entry[i]->save_serialization();
		coi_entry[i]->get_serialized_entry(serialization[i],
										   serialization_size[i]);
	}

	/*
	 * Validate against correct and mismatched entry
	 */
	coi.validate_entry(object_id[0], serialization[0],
					   serialization_size[0], &error);
	fail_if(error != NULL, "failed to validate coi entry: %#{}",
			isi_error_fmt(error));
	coi.validate_entry(object_id[0], serialization[1],
					   serialization_size[1], &error);
	fail_if(error == NULL, "Expected error, none ocurred");
	isi_error_free(error);
	error = NULL;

	for (int i = 0; i < num_objects; i++)
	{
		delete coi_entry[i];
	}
}

TEST(test_multi_version, mem_check
	 : CK_MEM_DEFAULT,
	   mem_hint : 0)
{
	struct isi_error *error = NULL;

	isi_cbm_coi_sptr coi = isi_cbm_get_coi(&error);
	fail_if(error != NULL, "failed to get COI: %#{}",
			isi_error_fmt(error));

	isi_cloud_object_id id1(1, 2);
	isi_cloud_object_id last_obj;

	cmoi_entry cmo_entry;
	id1.set_snapid(0);
	uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE];

	memcpy(archiving_cluster_id, "0123456789ABCDEFGH", IFSCONFIG_GUID_SIZE);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "test_container";
	ifs_lin_t referencing_lin = 1234567;

	coi->add_object(id1, archiving_cluster_id, archive_account_id,
					cmo_container, true, true,
					referencing_lin, &error, NULL);
	fail_if(error != NULL, "Failed to add_object: %#{}",
			isi_error_fmt(error));

	coi->get_last_version(id1, last_obj, &error);
	fail_if(error != NULL, "Failed to get_last_version: %#{}",
			isi_error_fmt(error));

	std::auto_ptr<coi_entry> last_entry(coi->get_entry(last_obj, &error));
	fail_if(error != NULL, "Failed to get_last_version: %#{}",
			isi_error_fmt(error));

	coi->get_cmo(last_obj, cmo_entry, &error);
	fail_if(error != NULL, "Failed to get_cmo: %#{}",
			isi_error_fmt(error));

	fail_if(cmo_entry.get_highest_ver() != 0,
			"The highest version is wrong %d", cmo_entry.get_highest_ver());

	isi_cloud_object_id id2(1, 2);
	id2.set_snapid(1);
	coi->add_object(id2, archiving_cluster_id, archive_account_id,
					cmo_container, true, true,
					referencing_lin, &error, NULL);

	coi->get_last_version(id1, last_obj, &error);
	fail_if(last_obj.get_snapid() != 1, "The version does not match",
			last_obj.get_snapid());

	fail_if(error != NULL, "Failed to get_last_version: %#{}",
			isi_error_fmt(error));

	last_entry.reset(coi->get_entry(last_obj, &error));
	fail_if(error != NULL, "Failed to get_entry: %#{}",
			isi_error_fmt(error));

	coi->get_cmo(last_obj, cmo_entry, &error);
	fail_if(error != NULL, "Failed to get_cmo: %#{}",
			isi_error_fmt(error));

	fail_if(cmo_entry.get_highest_ver() != 1,
			"The highest version is wrong %d", cmo_entry.get_highest_ver());

	coi->remove_object(last_obj, true, &error);
	fail_if(error != NULL, "Failed to remove_object: %#{}",
			isi_error_fmt(error));

	coi->get_last_version(id1, last_obj, &error);
	fail_if(error != NULL, "Failed to get_last_version: %#{}",
			isi_error_fmt(error));

	fail_if(last_obj.get_snapid() != 0, "The version does not match",
			last_obj.get_snapid());

	last_entry.reset(coi->get_entry(last_obj, &error));
	fail_if(error != NULL, "Failed to get_last_version: %#{}",
			isi_error_fmt(error));

	coi->get_cmo(last_obj, cmo_entry, &error);
	fail_if(error != NULL, "Failed to get_cmo: %#{}",
			isi_error_fmt(error));

	fail_if(cmo_entry.get_highest_ver() != 1,
			"The highest version is wrong %d", cmo_entry.get_highest_ver());

	coi->remove_object(last_obj, true, &error);
	fail_if(error != NULL, "Failed to remove_object: %#{}",
			isi_error_fmt(error));

	coi->get_cmo(last_obj, cmo_entry, &error);
	fail_if(error == NULL, "Do not expect the CMOI entry");
	fail_if(!isi_system_error_is_a(error, ENOENT),
			"Expecting ENOENT error, but got: %#{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;
}

/**
 * The following tests syncer id, next syncer id and syncing status.
 */
TEST(test_sync_status)
{

	struct isi_error *error = NULL;
	isi_cbm_coi_sptr coi = isi_cbm_get_coi(&error);
	fail_if(error != NULL, "failed to get COI: %#{}",
			isi_error_fmt(error));

	isi_cloud_object_id id1(1, 2);

	cmoi_entry cmo_entry;
	id1.set_snapid(0);
	uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE];

	memcpy(archiving_cluster_id, "0123456789ABCDEFGH", IFSCONFIG_GUID_SIZE);
	isi_cbm_id archive_account_id;
	const char *cmo_container = "test_container";
	ifs_lin_t referencing_lin = 1234567;

	coi->add_object(id1, archiving_cluster_id, archive_account_id,
					cmo_container, true, true,
					referencing_lin, &error, NULL);
	fail_if(error != NULL, "Failed to add_object: %#{}",
			isi_error_fmt(error));

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 0, "syncer id should be 0");
	fail_if(cmo_entry.get_next_syncer_id() != 0,
			"next syncer id should be 0");
	fail_if(cmo_entry.get_syncing() != false,
			"syncing should be false");

	uint64_t syncer_id1 = 0;
	coi->generate_sync_id(id1, syncer_id1, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(syncer_id1 != 1, "syncer 1 should have id 1");

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 0, "syncer id should be 0");
	fail_if(cmo_entry.get_next_syncer_id() != 1,
			"next syncer id should be 1");
	fail_if(cmo_entry.get_syncing() != false,
			"syncing should be false");

	uint64_t syncer_id2 = 0;
	coi->generate_sync_id(id1, syncer_id2, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(syncer_id2 != 2, "syncer 2 should have id 2");

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 0, "syncer id should be 0");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != false,
			"syncing should be false");

	coi->update_sync_status(id1, syncer_id1, true, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 1, "syncer id should be 1");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != true,
			"syncing should be true");

	coi->update_sync_status(id1, syncer_id2, true, &error);

	fail_if(!error ||
				!isi_cbm_error_is_a(error, CBM_SYNC_ALREADY_IN_PROGRESS),
			"Expected error CBM_SYNC_ALREADY_IN_PROGRESS not found: %#{}.\n",
			isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	// should be unchaged:
	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 1, "syncer id should be 1");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != true,
			"syncing should be true");

	coi->update_sync_status(id1, syncer_id1, false, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 1, "syncer id should be 1");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != false,
			"syncing should be false");

	coi->update_sync_status(id1, syncer_id2, true, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 2, "syncer id should be 2");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != true,
			"syncing should be true");

	coi->update_sync_status(id1, syncer_id1, false, &error);

	// this is disallowed
	fail_if(!error ||
				!isi_cbm_error_is_a(error, CBM_SYNC_ALREADY_IN_PROGRESS),
			"Expected error CBM_SYNC_ALREADY_IN_PROGRESS not found: %#{}.\n",
			isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	coi->update_sync_status(id1, syncer_id2, false, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	coi->get_cmo(id1, cmo_entry, &error);
	fail_if(error, "Error :%#{}", isi_error_fmt(error));

	fail_if(cmo_entry.get_syncer_id() != 2, "syncer id should be 2");
	fail_if(cmo_entry.get_next_syncer_id() != 2,
			"next syncer id should be 2");
	fail_if(cmo_entry.get_syncing() != false,
			"syncing should be false");

	coi->remove_object(id1, true, &error);
	fail_if(error != NULL, "Failed to remove_object: %#{}",
			isi_error_fmt(error));
}
