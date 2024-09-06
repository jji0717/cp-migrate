#include <check.h>
#include <vector>

#include <isi_cpool_security/cpool_protect.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>
#include <isi_cpool_cbm/io_helper/compressed_istream.h>
#include "isi_cbm_mapper.h"
#include "isi_cbm_file.h"
#include "isi_cbm_invalidate.h"
#include "isi_cbm_sync.h"
#include "isi_cbm_test_util.h"
#include "check_cbm_common.h"

#define SPEC cbm_test_policy_spec

static cbm_test_env env;

static isi_cbm_test_sizes small_size(8 * 1024, 1024, 2 * 1024 * 1024);

static cbm_test_policy_spec specs[] = {
     SPEC(CPT_AWS, false, false, small_size),
     SPEC(CPT_AWS, true, false, small_size),
     SPEC(CPT_AWS, false, true, small_size),
     SPEC(CPT_AWS, true, true, small_size),
};

class cl_provider_failpoint {
 public:
	bool setup_failpoint();

	void teardown_failpoint();

 private:
	std::vector<struct ufp *> ufp_vec_;
};

bool
cl_provider_failpoint::setup_failpoint()
{
	const char *ufp_name_lst[] = {
	    "put_container_post_api_fail",
	    "get_container_location_post_api_fail",
	    "put_object_post_api_fail",
	    "update_object_post_api_fail",
	    "get_object_post_api_fail",
	    "head_container_post_api_fail",
	    "head_object_post_api_fail",
	    "snapshot_object_post_api_fail",
	    "clone_object_post_api_fail",
	    "move_object_post_api_fail",
	    "get_account_cap_post_api_fail",
	    "list_object_post_api_fail",
	    "get_account_statistics_post_api_fail"};

	int n = sizeof(ufp_name_lst) / sizeof(ufp_name_lst[0]);
	bool ret = true;

	ufp_vec_.resize(n);
	ufp_globals_wr_lock();

	for (int i = 0; i < n; ++i) {
		ufp_vec_[i] = ufail_point_lookup(ufp_name_lst[i]);

		if (!ufp_vec_[i] || ufail_point_set(ufp_vec_[i], "return(1)") != 0)
			ret = false;
	}

	if (!ret) {
		for (std::vector<struct ufp *>::iterator it = ufp_vec_.begin();
		    it != ufp_vec_.end(); ++it) {
			if (*it)
				ufail_point_destroy(*it);
		}
		ufp_vec_.clear();
	}

	ufp_globals_wr_unlock();

	return ret;
}

void
cl_provider_failpoint::teardown_failpoint()
{
	for (std::vector<struct ufp *>::iterator it = ufp_vec_.begin();
	    it != ufp_vec_.end(); ++it) {
		if (*it) {
			ufail_point_set(*it, "off");
			ufail_point_destroy(*it);
		}
	}

	ufp_vec_.clear();
}


TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	cbm_test_common_setup_gconfig();

	env.setup_env(specs, sizeof(specs) / sizeof(specs[0]));
	// now load the config
	isi_cbm_reload_config(&error);

	/*
	 * suite setup invokes PAPI code to initialize provider account,
	 * in which the num_retires_ parameter in cl_provider_wrap is
	 * set to zero. Zero num_retires_ will not work for this UT as
	 * this UT will test cloud API with failpoint triggered network
	 * failure, for which an exception will be thrown without being
	 * caught and handled (149611). To get around failpoint triggered
	 * network failure, the num_retries_ must be set to >= 1
	 */
	isi_cloud::set_number_network_attempts(1);

	cpool_regenerate_mek(&error);
	fail_if(error, "failed to generate mek: %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);
	fail_if(error, "failed to generate dek: %#{}", isi_error_fmt(error));

	// prevent fake memory leak warning
	cbm_test_leak_check_primer(true);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");
	restart_leak_detection(0);
}

TEST_FIXTURE(suite_teardown)
{
	env.cleanup();

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

SUITE_DEFINE_FOR_FILE(check_streaming,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 600,
    .fixture_timeout = 1200);

static void
modify_every_other_chunk(ifs_lin_t lin, size_t chunksz, size_t filesz)
{
	struct isi_error *error = NULL;

	const char txt[] = "hello, world!";
	struct isi_cbm_file *file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);

	fail_if(error, "failed to open file: %#{}", isi_error_fmt(error));

	for (size_t i = 0; i < filesz / chunksz; i += 2) {
		isi_cbm_file_write(file, txt, i * chunksz, strlen(txt), &error);

		fail_if(error, "failed to write stub file: %#{}",
		    isi_error_fmt(error));
	}

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close stub file: %#{}", isi_error_fmt(error));
}

static bool
is_overflow(ifs_lin_t lin)
{
	struct isi_error *error = NULL;

	isi_cfm_mapinfo mapinfo;
	struct isi_cbm_file *file = NULL;
	bool overflow = false;

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open file: %#{}", isi_error_fmt(error));

	isi_cph_get_stubmap(file->fd, mapinfo, &error);
	fail_if(error, "failed to get stubmap: %#{}", isi_error_fmt(error));

	overflow = mapinfo.is_overflow();

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close file: %#{}", isi_error_fmt(error));

	return overflow;
}

static void
read_through_file(ifs_lin_t lin, size_t chunksz, size_t filesz)
{
	struct isi_error *error = NULL;
	struct isi_cbm_file *file = NULL;
	bool dirty = false;
	std::vector<char> buf(chunksz);
	const int n = (filesz + chunksz - 1) / chunksz;

	file = isi_cbm_file_open(lin, HEAD_SNAPID, 0, &error);
	fail_if(error, "failed to open file: %#{}", isi_error_fmt(error));

	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(dirty, "unexpected dirty stub");

	for (int i = 0; i < n; ++i) {
		isi_cbm_file_read(file, buf.data(),
		    i * chunksz, chunksz, CO_CACHE, &error);

		fail_if(error, "failed to read stub file: %#{}",
		    isi_error_fmt(error));
	}

	isi_cbm_invalidate_cache(file->lin, file->snap_id, &dirty, &error);
	fail_if(error, "unexpected error: %#{}", isi_error_fmt(error));
	fail_if(dirty, "unexpected dirty stub");

	isi_cbm_file_close(file, &error);
	fail_if(error, "failed to close file: %#{}", isi_error_fmt(error));
}

static void
test_s3_streaming_with_spec(cbm_test_policy_spec &spec)
{
	struct isi_error *error = NULL;
	isi_cbm_sync_option opt = { blocking: true, skip_settle: false };
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECT);
	const int n_cdos = mapinfo.get_overflow_threshold() + 2;
	const size_t file_sz = small_size.chunksize_ * n_cdos;

	std::string fname("/ifs/data/");
	fname += spec.policy_name_;

	// threshould + 2 CDOs
	cbm_test_file_helper test_file(fname.c_str(), file_sz, "");
	ifs_lin_t lin = test_file.get_lin();

	error = cbm_test_archive_file_with_pol(lin,
	    spec.policy_name_.c_str(), false);

	fail_if(error, "failed to archive file: %#{}", isi_error_fmt(error));
	fail_if(is_overflow(lin), "stub file is not expected to overflow");

	modify_every_other_chunk(lin, small_size.chunksize_, file_sz);

	isi_cbm_sync_opt(lin, HEAD_SNAPID, NULL, NULL, NULL, opt, &error);
	fail_if(error, "failed to sync back stub data: %#{}",
	    isi_error_fmt(error));

	fail_if(!is_overflow(lin), "stub file is expected to overflow");

	read_through_file(lin, small_size.chunksize_, file_sz);

	isi_cbm_recall(lin, HEAD_SNAPID, false, NULL, NULL, NULL, NULL, &error);
	fail_if(error, "failed recall the stub file: %#{}",
	    isi_error_fmt(error));

	system((std::string("isi_run -c rm ") + fname).c_str());
}

TEST(test_s3_streaming)
{
	cl_provider_failpoint fp;
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECT);

	fail_if(!fp.setup_failpoint());

	override_clapi_verbose_logging_enabled(false);

	for (size_t i = 0; i < sizeof(specs) / sizeof(specs[0]); ++i) {
		printf("\nstreaming: %s\n", specs[i].policy_name_.c_str());

		// FIXME: encryption supports in-memory input only,
		// small threhold forces both CDO/CMO compression to tmp file
		// which breaks the CDO encryption
		if (specs[i].compress_ && specs[i].encrypt_)
			compressed_istream::reset_threshold();
		else {
			int sz = mapinfo.get_overflow_threshold() *
			    mapinfo.get_mapentry_pack_size(false);

			compressed_istream::set_threshold(sz);
		}

		test_s3_streaming_with_spec(specs[i]);
	}
	revert_clapi_verbose_logging();

	fp.teardown_failpoint();
}

