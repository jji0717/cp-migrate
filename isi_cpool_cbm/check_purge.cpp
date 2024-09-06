#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_coi.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include "check_cbm_common.h"

#include <check.h>

using namespace isi_cloud;

TEST_FIXTURE(setup_suite)
{
	cbm_test_common_cleanup(true, true);
	cbm_test_common_setup(true, true);
}

TEST_FIXTURE(cleanup_suite)
{
	cbm_test_common_cleanup(true, true);
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;
	isi_cbm_init(&error);
	fail_if (error != NULL, "isi_cbm_init failed: %#{}",
	    isi_error_fmt(error));

	cl_provider_module::init();
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;
	isi_cbm_destroy(&error);
	fail_if (error != NULL, "isi_cbm_destroy failed: %#{}",
	    isi_error_fmt(error));

	cl_provider_module::destroy();

	checksum_cache_cleanup();
}

SUITE_DEFINE_FOR_FILE(check_stub_file_purge,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .suite_teardown = cleanup_suite,
    .test_setup = test_setup,
    .test_teardown = test_teardown,
    .timeout = 60,
    .fixture_timeout = 1200);

struct isi_cbm_file *
stub_and_open_a_test_file(const char *fname)
{
	struct fmt FMT_INIT_CLEAN(str);
	struct isi_error *error = NULL;
	struct stat st;
	char *buf = (char *)malloc(1024);

	// cp file
	unlink(fname);

	fmt_print(&str, "isi_run -c cp -f %s %s", "/etc/services", fname);

	fail_if(system(fmt_string(&str)) != 0, "can't cp file");

	// stub file
	fail_if(stat(fname, &st) != 0, "can't stat file");

	error = cbm_test_archive_file_with_pol(
	    st.st_ino, CBM_TEST_COMMON_POLICY_RAN, true);

	fail_if(error, "unable to stub file");

	// read file to open cache
	struct isi_cbm_file *file =
	    isi_cbm_file_open(st.st_ino, HEAD_SNAPID, 0, &error);

	fail_if(error, "unable to open stub file");

	isi_cbm_file_read(file, buf, 0, 1024, CO_CACHE, &error);

	fail_if(error, "unable to write stub file");

	free(buf);

	return file;
}

TEST(purge_stub_file, mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	isi_cfm_mapinfo mapinfo;

	struct isi_cbm_file *file =
	    stub_and_open_a_test_file("/ifs/data/file_to_purge.txt");

	fail_if(!file, "unable prepare a stub file");

	isi_cbm_file_stub_purge(file, true, false, true, &error);

	fail_if(error, "unable to purge stub file %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);

	fail_if(error, "unable to close cbm file %#{}", isi_error_fmt(error));
	//
	// 1. mapinfo
	int fd = open("/ifs/data/file_to_purge.txt", O_RDWR);

	fail_if(fd < 0, "unable to open file");

	isi_cph_get_stubmap(fd, mapinfo, &error);

	fail_if(!error, "purged file supposed to have no mapinfo");

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

/**
XXXpd: commented out because cacheinfo interface changed.  Can now
be created only with a file handle.   Hualiang to look into
how we can test for absence of cacheinfo under those circumstanaces
	// 2. cache
	cpool_cache cacheinfo;

	cacheinfo.cache_open(fd, &error);

	fail_if(!error, "not supposed to open cache %#{}", isi_error_fmt(error));

	if (error) {
		isi_error_free(error);
		error = NULL;
	}
*/
	// not supposed to fail on purge again
	file = isi_cbm_file_get(fd, &error);

	fail_if(!error, "not supposed to cbm open purged file %#{}",
	    isi_error_fmt(error));

	isi_error_free(error);
	error = NULL;

	unlink("/ifs/data/file_to_purge.txt");
	close(fd);
}

static void
remove_cloud_obj_ref(isi_cbm_file *file, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cloud::coi coi;
	const isi_cfm_mapinfo *mapinfo = file->mapinfo;

	coi.initialize(&error);
	if (error != NULL) {
		isi_error_add_context(error);
		goto out;
	}

	struct timeval date_of_death_tv;
	gettimeofday(&date_of_death_tv, NULL);
	coi.remove_ref(mapinfo->get_object_id(), file->lin, file->snap_id,
	    date_of_death_tv, NULL, &error);

	if (error != NULL) {
		isi_error_add_context(error,
		    "Failed to coi::remove_ref "
		    "for %{}",
		    isi_cbm_file_fmt(file));
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}
/*
// commented out -- the logic need be revisisted.
TEST(purge_stub_file_with_no_force, mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	isi_cfm_mapinfo mapinfo;

	struct isi_cbm_file *file =
	    stub_and_open_a_test_file("/ifs/data/file_to_purge_no_force.txt");

	fail_if(!file, "unable prepare a stub file");

	//
	remove_cloud_obj_ref(file, &error);
	fail_if(error, "unable to remove coi %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "unable to close cbm file %#{}", isi_error_fmt(error));

	//
	int fd = open("/ifs/data/file_to_purge_no_force.txt", O_RDWR);
	file = isi_cbm_file_get(fd, &error);

	fail_if(error, "can't cmb open file, %#{}", isi_error_fmt(error));

	isi_cbm_file_stub_purge(file, false, &error);

	fail_if(!error, "none force purge here should return error");

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	isi_cbm_file_close(file, &error);

	fail_if(error, "unable to close cbm file %#{}", isi_error_fmt(error));

	unlink("/ifs/data/file_to_purge_no_force.txt");
}
*/

TEST(purge_stub_file_with_force, mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	isi_cfm_mapinfo mapinfo;

	struct isi_cbm_file *file =
	    stub_and_open_a_test_file("/ifs/data/file_to_purge_force.txt");

	fail_if(!file, "unable prepare a stub file");

	//
	remove_cloud_obj_ref(file, &error);
	fail_if(error, "unable to remove coi %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "unable to close cbm file %#{}", isi_error_fmt(error));

	//
	int fd = open("/ifs/data/file_to_purge_force.txt", O_RDWR);
	file = isi_cbm_file_get(fd, &error);
	fail_if(error, "unable cbm file open %#{}", isi_error_fmt(error));

	isi_cbm_file_stub_purge(file, true, false, true, &error);
	fail_if(error, "unable to purge stub file %#{}", isi_error_fmt(error));

	isi_cbm_file_close(file, &error);
	fail_if(error, "unable to close cbm file %#{}", isi_error_fmt(error));

	unlink("/ifs/data/file_to_purge_force.txt");
}
