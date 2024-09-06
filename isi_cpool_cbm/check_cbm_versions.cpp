#include <sstream>
#include <string>

#include <fcntl.h>
#include <stdio.h>
#include <check.h>
#include <sys/types.h>
#include <sys/extattr.h>



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
#include "isi_cbm_file_versions.h"
#include "isi_cbm_error.h"
#include "isi_licensing/licensing.h"

#define CP_STRING	CBM_ACCESS_ON "cp -f %s %s" CBM_ACCESS_OFF

using namespace isi_cloud;

static bool ran_only_g = true;
static bool policies = false;  // create or clean
static bool has_license = false;

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));

	cbm_test_common_cleanup(ran_only_g, policies);
	cbm_test_common_setup(ran_only_g, policies);
	fail_if(!cbm_test_cpool_ufp_init(), "Fail points failed to initiate");

	cpool_fd_store::getInstance();

	has_license = isi_licensing_module_status(ISI_LICENSING_CLOUDPOOLS) == ISI_LICENSING_LICENSED;
}

TEST_FIXTURE(suite_teardown)
{
	cbm_test_common_cleanup(ran_only_g, policies);
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

SUITE_DEFINE_FOR_FILE(check_cbm_versions,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = setup_test,
    .test_teardown = teardown_test,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST(test_cbm_versions_set_supported,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_cbm_file_version ver[] = {{1, 0xFF}, {2, 0xFE}};
	size_t num = sizeof(ver)/sizeof(ver[0]);
	supported_versions_set(ver, num);
	supported_versions_clear();
}


TEST(test_cbm_versions_serialize_deserialize,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	isi_cbm_file_versions *vs, *vs1, *vs2;
	size_t nbytes, buf_nbytes = 100;
	char buf[buf_nbytes];

	struct isi_cbm_file_version ver[] = {{1, 0xFF}, {2, 0xFE}};
	size_t num = sizeof(ver)/sizeof(ver[0]);
	supported_versions_set(ver, num);

	vs = isi_cbm_file_versions_get_supported();

	// negative test: smaller buf
	nbytes = isi_cbm_file_versions_serialize(vs, buf, 1, &error);
	fail_if(!error, "Expected error");
	fail_if(!isi_cbm_error_is_a(error, CBM_LIMIT_EXCEEDED),
	    "Expected error type %#{}", cbm_error_type_fmt(CBM_LIMIT_EXCEEDED));
	isi_error_free(error);
	error = NULL;

	// positive test
	nbytes = isi_cbm_file_versions_serialize(vs, buf, buf_nbytes, &error);
	fail_if(error, "Failure: %#{}.\n", isi_error_fmt(error));
	fail_if(nbytes > buf_nbytes, "nbytes = %lu; buf_nbytes = %lu",
	    nbytes, buf_nbytes);

	// negative test: smaller buf
	// XXX no failure on smaller buf (e.g. nbytes/), check 

	// negative test: larger buf
	vs1 = isi_cbm_file_versions_deserialize(buf,buf_nbytes, &error);
	fail_if(!error, "Expected error");
	fail_if(vs1, "Expected NULL");
	isi_error_free(error);
	error = NULL;

	// positive test
	vs1 = isi_cbm_file_versions_deserialize(buf, nbytes, &error);
	fail_if(error, "Failure: %#{}.\n", isi_error_fmt(error));

	// empty test
	vs2 = isi_cbm_file_versions_deserialize(NULL, 0, &error);
	fail_if(error, "Failure: %#{}.\n", isi_error_fmt(error));

	isi_cbm_file_versions_destroy(vs2);
	isi_cbm_file_versions_destroy(vs1);
	isi_cbm_file_versions_destroy(vs);
	supported_versions_clear();
}

std::string
isi_cbm_file_version_string(const struct isi_cbm_file_version & ver)
{
	std::stringstream ret;
	ret << "{" << ver._num << ", " << ver._flags << "}";
	return ret.str();
}

bool
isi_cbm_file_version_same(const struct isi_cbm_file_version & ver1,
    const struct isi_cbm_file_version & ver2)
{
	return (ver1._num == ver2._num) && (ver1._flags == ver2._flags);
}

TEST(test_cbm_version_serialize_deserialize,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	isi_cbm_file_version ver1 = {1, 0xFF};
	isi_cbm_file_version ver2 = {0, 0x11};
	size_t nbytes, buf_nbytes = 100;
	char buf[buf_nbytes];

	// negative test: smaller buf
	nbytes = isi_cbm_file_version_serialize(&ver1, buf, 1, &error);
	fail_if(!error, "Expected error");
	fail_if(!isi_cbm_error_is_a(error, CBM_LIMIT_EXCEEDED),
	    "Expected error type %#{}", cbm_error_type_fmt(CBM_LIMIT_EXCEEDED));
	isi_error_free(error);
	error = NULL;

	// positive test
	nbytes = isi_cbm_file_version_serialize(&ver1, buf, buf_nbytes, &error);
	fail_if(error, "Failure: %#{}.\n", isi_error_fmt(error));
	fail_if(nbytes > buf_nbytes, "nbytes = %lu; buf_nbytes = %lu",
	    nbytes, buf_nbytes);

	// negative test: larger buf
	isi_cbm_file_version_deserialize(&ver2, buf, buf_nbytes, &error);
	fail_if(!error, "Expected error");
	isi_error_free(error);
	error = NULL;

	// positive test
	isi_cbm_file_version_deserialize(&ver2, buf, nbytes, &error);
	fail_if(error, "Failure: %#{}.\n", isi_error_fmt(error));
	fail_if(!isi_cbm_file_version_same(ver1, ver2),
	    "ver1: %s != ver2: %s", isi_cbm_file_version_string(ver1).c_str(),
	    isi_cbm_file_version_string(ver2).c_str());
}



TEST(test_cbm_versions_version_presence,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	isi_cbm_file_versions *vs;
	bool found;

	struct isi_cbm_file_version ver[] = {{1, 0xFF}, {2, 0xFE}};
	size_t num = sizeof(ver)/sizeof(ver[0]);
	supported_versions_set(ver, num);

	vs = isi_cbm_file_versions_get_supported();

	struct isi_cbm_file_version ver1 = {2, 0x30};
	struct isi_cbm_file_version ver_np1 = {3, 0xFF};
	struct isi_cbm_file_version ver_np2 = {2, 0x01};

	found = isi_cbm_file_version_exists(&ver1, vs);
	fail_if(!found, "Failure: Could not find version %s",
	    isi_cbm_file_version_string(ver1).c_str());
	found = isi_cbm_file_version_exists(&ver_np1, vs);
	fail_if(found, "Failure: Should not have found version %s",
	    isi_cbm_file_version_string(ver_np1).c_str());
	found = isi_cbm_file_version_exists(&ver_np1, vs);
	fail_if(found, "Failure: Should not have found version %s",
	    isi_cbm_file_version_string(ver_np2).c_str());

	isi_cbm_file_versions_destroy(vs);

	supported_versions_clear();
}

TEST(test_cbm_versions_version_common,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	isi_cbm_file_versions *vs1, *vs2, *vs_common;
	bool found;

	struct isi_cbm_file_version ver1[] = {{4, 0xAAA}, {1, 0xFF}, {2, 0xFE}};
	size_t num = sizeof(ver1)/sizeof(ver1[0]);
	supported_versions_set(ver1, num);
	vs1 = isi_cbm_file_versions_get_supported();
	fail_if(!vs1);

	struct isi_cbm_file_version ver_common = {2, 0xF0};

	struct isi_cbm_file_version ver2[] = {{5, 0xAAA}, ver_common, {3, 0xFE},
		{6, 0xFF}};
	num = sizeof(ver2)/sizeof(ver2[0]);
	supported_versions_set(ver2, num);
	vs2 = isi_cbm_file_versions_get_supported();
	fail_if(!vs2);

	vs_common = isi_cbm_file_versions_common(vs1, vs2);
	fail_if(!vs_common);
	found = isi_cbm_file_version_exists(&ver_common, vs_common);
	fail_if(!found, "Should have found version %s",
	    isi_cbm_file_version_string(ver_common).c_str());

	isi_cbm_file_versions_destroy(vs_common);
	isi_cbm_file_versions_destroy(vs2);
	isi_cbm_file_versions_destroy(vs1);

	supported_versions_clear();
}

TEST(test_cbm_versions_unlicensed,
    mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_cbm_file_version ver[] = {{1, 0xFF}, {2, 0xFE}};
	size_t num = sizeof(ver)/sizeof(ver[0]);
	isi_cbm_file_versions *vs;

	supported_versions_set(ver, num);
	isi_cbm_file_versions_license(false);

	// Confirm that no versions are in the supported set
	vs = isi_cbm_file_versions_get_supported();
	fail_if(!vs);
	for (size_t i = 0; i < num; i++) {
		bool has_version = isi_cbm_file_version_exists(ver + i, vs);
		fail_if(has_version, "found version %s ; expected empty",
		    isi_cbm_file_version_string(ver[i]).c_str());
	}
	isi_cbm_file_versions_destroy(vs);

	isi_cbm_file_versions_license(true);
	supported_versions_clear();
}
