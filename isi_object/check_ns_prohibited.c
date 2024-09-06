#include <check.h>

#include <ifs/ifs_types.h>

#include <isi_util/isi_error.h>
#include "isi_object/iobj_licensing.h"
#include <isi_object/ostore_internal.h>

#include <isi_object/check_utils.h>
#include <isi_object/iobj_ostore_protocol.h>

#include <stdio.h>

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(check_ns_prohibited, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout=20);


#define DEFAULT_NSA		"ifs"
#define DEFAULT_NSA_PATH	"/ifs"

#define DEFAULT_SYS_PATHS	"/ifs/.ifsvar, /ifs/.object"

static struct ostore_handle *ios_g = NULL;

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = 0;
	ios_g = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);
	fail_unless(ios_g != NULL, "Expected ifs namespace by default");
	ostore_set_system_dirs(DEFAULT_SYS_PATHS);
}

TEST_FIXTURE(suite_teardown)
{
	struct isi_error *error = 0;
	iobj_ostore_close(ios_g, &error);
	fail_if(error, "Closing a namespace store failed.");
}

TEST_FIXTURE(test_setup)
{
}

TEST_FIXTURE(test_teardown)
{
}


/**
 * test create store to system used dir
 */
TEST(test_proh_create_store)
{
	struct isi_error *error = NULL;
	const char *ap = "system_dir_ap";
	struct ostore_handle *ios = 0;
	bool c_failed = false;
	// creae store to /ifs/.ifsvar
	ios = iobj_ostore_create(OST_NAMESPACE, ap,
	    DEFAULT_NSA_PATH "/.ifsvar", &error);
	c_failed = (ios == NULL) && isi_error_is_a(error,
	    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
	fail_unless(c_failed,
	    "Expected failure creating access point to system used dir");
	isi_error_free(error);
	error = 0;
}

/**
 * test creating a new object in system used dir
 */
TEST(test_proh_open_bucket)
{
	struct isi_error *error = NULL;
	bool failed = false;
	struct ibucket_handle *ibh = 0;

	struct ibucket_name *ibn = NULL;
	const char *dirs[] = {".ifsvar", ".ifsvar/modules"};
	for (int i = 0; i < 2; ++i) {
		ibn = iobj_ibucket_from_acct_bucket(ios_g, "TEST_ACCT",
		    dirs[i], &error);
		fail_if(error,
		    "received error %#{} formatting ibn from acct an bucket",
		    isi_error_fmt(error));
		ibh = iobj_ibucket_open(ibn, 
		    (enum ifs_ace_rights) CONTAINER_RWRITE, 
		    O_DIRECTORY, CF_FLAGS_NONE, &error);
		failed = (ibh == NULL) && isi_error_is_a(error,
		    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
		fail_unless(failed, "Expected failure creating access point "
		    "to system used dir");
		iobj_ibucket_name_free(ibn);
		isi_error_free(error);
		error = 0;
	}
}


struct list_object_tester {
	size_t count;
	char ** obj_names;
	size_t num_names;
};

static bool
test_list_cb_all(void *ctx, const struct iobj_list_obj_info *info)
{
	struct list_object_tester * mt = ((struct list_object_tester *)ctx);
	bool failed = !strcmp(info->name, ".ifsvar");
	failed |= !strcmp(info->name, ".object");
	fail_if(failed, "Don't expect system used directory being returned.");
	mt->count += 1;
	return true;
}

/**
 * test listing objects
 */
TEST(test_proh_list_object)
{
	struct isi_error *error = NULL;
	struct iobj_list_obj_param param = {0};
	// open bucket:
	struct ibucket_handle *ibh = 0;

	// open the root container
	ibh = test_util_open_bucket(false, OSTORE_ACL_NONE,
	    (enum ifs_ace_rights) CONTAINER_READ,
	    ios_g, "TEST_ACCT", "/");

	struct list_object_tester ctx = {0};

	param.ibh = ibh;
	param.caller_cb = test_list_cb_all;
	param.caller_context = &ctx;

	iobj_object_list(&param, &error);
	fail_if(error != NULL, "received error %#{} when listing in %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	test_util_close_bucket(ibh);
}

/* test copy object to system used dir*/
TEST(test_proh_copy)
{
	int rval = 0;
	struct isi_error *error = 0;
	bool failed = false;
	const char *src = "/ifs/__test_proh_copy";
	const char *psrc = src + strlen(DEFAULT_NSA_PATH) + 1;
	const char *pdest = "/.ifsvar/home_unit_test_copied";

	create_test_folder(src);

	struct fsu_copy_flags cp_flags;
	cp_flags.cont = 0;
	cp_flags.noovw = 1;
	cp_flags.merge = 1;
	struct ostore_copy_param o_cp_param = {
	    {psrc, pdest, cp_flags, NULL, NULL}, ios_g, ios_g
	};

	rval = iobj_genobj_copy(&o_cp_param, &error);
	failed = rval && isi_error_is_a(error,
	    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
	fail_unless(failed, "Expected failure copying object "
	    "to system used dir");
	isi_error_free(error);
	delete_test_folder(src);
}


/* test move object to system used dir*/
TEST(test_proh_move)
{
	int rval = 0;
	struct isi_error *error = 0;
	bool failed = false;
	const char *src = "/ifs/__test_proh_move";
	const char *psrc = src + strlen(DEFAULT_NSA_PATH) + 1;
	const char *pdest = "/.ifsvar/home_unit_test_copied";

	create_test_folder(src);

        struct fsu_move_flags mv_flags;
        mv_flags.ovw = 0;
	rval = iobj_genobj_move(ios_g, psrc, ios_g, pdest, &mv_flags, &error);

	failed = rval && isi_error_is_a(error,
	    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
	fail_unless(failed, "Expected failure moving object "
	    "to system used dir");
	isi_error_free(error);
	delete_test_folder(src);
}


/* test delete object to system used dir*/
TEST(test_proh_delete)
{
	struct isi_error *error = 0;
	bool failed = false;
	const char *src = "/ifs/.ifsvar/__test_proh_move";
	const char *pdest = "/.ifsvar/home_unit_test_copied";
	struct ostore_del_flags flags = {.recur = true};

	create_test_folder(src);

	iobj_genobj_delete(ios_g, pdest, flags, &error);

	failed = error && isi_error_is_a(error,
	    OSTORE_PATHNAME_PROHIBITED_ERROR_CLASS);
	fail_unless(failed, "Expected failure moving object "
	    "to system used dir");
	isi_error_free(error);
	delete_test_folder(src);
}
