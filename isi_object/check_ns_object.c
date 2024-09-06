#include <ifs/ifs_types.h>

#include <check.h>
#include <stdio.h>

#include <isi_util/isi_error.h>
#include <isi_util/check_helper.h>
#include "isi_object/iobj_licensing.h"
#include <isi_object/ostore_internal.h>

#include <isi_object/check_utils.h>
#include <isi_object/iobj_ostore_protocol.h>
#include <isi_object/ns_common.h>


TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(check_ns_object, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .timeout = 20);

#define DEFAULT_NSA	"ifs"

/* start with the store empty */
TEST_FIXTURE(suite_setup)
{
	struct isi_error *ie = NULL;

	if (isi_licensing_module_status(ISI_LICENSING_WORM) ==
	    ISI_LICENSING_UNLICENSED) {
		isi_licensing_set_eval_status(ISI_LICENSING_WORM, &ie);
		fail_if_error(ie, "Failed to set an evaluation license");
	}
}

/* at the  end, let's leave the store empty */
TEST_FIXTURE(suite_teardown)
{
}

/**
 * test open an invalid object
 */
TEST(test_open_invalid_object)
{
	struct isi_error *error = NULL;
	// open bucket:
	struct ibucket_handle *ibh = 0;

	struct ostore_handle *ios = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);

	fail_if(error != NULL, "received error %#{} when opening store %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	// open the root container
	ibh = test_util_open_bucket(false, OSTORE_ACL_NONE,
	    (enum ifs_ace_rights) (IFS_RTS_DIR_LIST | IFS_RTS_DIR_TRAVERSE),
	    ios, "TEST_ACCT", "/");

	iobj_object_open(ibh, "test-non-existing-object", 
	    (enum ifs_ace_rights) IFS_RTS_FILE_READ_DATA, 0,
	    CF_FLAGS_NONE, &error);

	fail_unless(error != NULL, "Expecting error when opening a"
	    " non-existing object");

	isi_error_free(error);
	error = NULL;

	test_util_close_bucket(ibh);

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} when closing store",
	    isi_error_fmt(error), DEFAULT_NSA);
}

/**
 * test creating a new object
 */
TEST(test_object_create)
{
	struct isi_error *error = NULL;
	// open bucket:
	struct ibucket_handle *ibh = 0;

	struct ostore_handle *ios = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);

	fail_if(error != NULL, "received error %#{} when opening store %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	// open the root container
	ibh = test_util_open_bucket(false, OSTORE_ACL_NONE,
	    (enum ifs_ace_rights) CONTAINER_RWRITE,
	    ios, "TEST_ACCT", "/");

	/*
	 * test object creation with mode
	 */
	mode_t oldmode = umask(022);
	mode_t cmode = 0777; // assuming process mask = 022
	struct iobject_handle *oh = iobj_object_create(ibh,
	    "__test-create-object", 0, cmode, NULL, OSTORE_ACL_NONE, true, 
	    &error);
	fail_if(error, "received error %#{} creating object",
		isi_error_fmt(error));

	umask(oldmode);

	iobj_object_write(oh, 0, "hello\n", sizeof("hello"), &error);
	fail_if(error, "write object failed %#{}", isi_error_fmt(error));

	iobj_object_commit(oh, &error);
	fail_if(error, "commit object failed %#{}", isi_error_fmt(error));

	struct stat st = {0};
	fail_if(fstat(oh->base.fd, &st), "failed to get stat of %s %s",
	    oh->base.path);
	fail_if((st.st_mode & 0777) != cmode, "expect file permission mode %o"
	    " instead of %o", cmode, st.st_mode & 0777);

	// clean up the obj
	test_util_genobj_delete(&(oh->base));
	iobj_object_close(oh, &error);
	oh = NULL;

	/*
	 * test object creation with long name -- 255 chars
	 */
	const char *long_name = "1111111111222222222233333333334444444444"
	    "555555555566666666667777777777888888888899999999990000000000"
	    "111111111122222222223333333333444444444455555555556666666666"
	    "777777777788888888889999999999000000000011111111112222222222"
	    "33333333334444444444555555555566666";
	oh = iobj_object_create(ibh,
	    long_name, 0, cmode, NULL, OSTORE_ACL_NONE, true, &error);
	fail_if(error, "failed to creating object with long name: %#{} ",
		isi_error_fmt(error));
	iobj_object_commit(oh, &error);
	fail_if(error, "commit object failed %#{}", isi_error_fmt(error));
	// clean up the obj
	test_util_genobj_delete(&(oh->base));
	iobj_object_close(oh, &error);
	oh = NULL;

	// close bucket & store
	test_util_close_bucket(ibh);
	iobj_ostore_close(ios, &error);

	fail_if(error, "received error %#{} close object",
		isi_error_fmt(error));

}

struct test_list_context {
	size_t count;
	char ** obj_names;
	size_t num_names;
};

static bool
test_list_cb_all(void *ctx, const struct iobj_list_obj_info *info)
{
	struct test_list_context * mt = ((struct test_list_context *)ctx);

	mt->count += 1;

	return true;
}

/**
 * test listing objects
 */
TEST(test_list_object)
{
	struct isi_error *error = NULL;
	struct iobj_list_obj_param param = {0};
	// open bucket:
	struct ibucket_handle *ibh = 0;

	struct ostore_handle *ios = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);

	fail_if(error != NULL, "received error %#{} when opening store %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	// open the root container
	ibh = test_util_open_bucket(false, OSTORE_ACL_NONE,
	    (enum ifs_ace_rights) CONTAINER_READ,
	    ios, "TEST_ACCT", "/");

	struct test_list_context ctx = {0};

	param.ibh = ibh;
	param.caller_cb = test_list_cb_all;
	param.caller_context = &ctx;
	param.stats_requested = true;

	iobj_object_list(&param, &error);
	fail_if(error != NULL, "received error %#{} when listing in %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	test_util_close_bucket(ibh);

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} when closing store",
	    isi_error_fmt(error));
}

/**
 * test get worm attr on regular file or not-committed file
 */
TEST(test_ns_object_get_worm1)
{
	struct isi_error *error = NULL;
	bool failed = false;
	struct iobject_handle iobj;
	struct object_worm_attr worm_attr = {};
	struct object_worm_domain worm_dom = {};

	//  test regular file
	const char *reg_tfile = "/ifs/__unit_test_non_worm__";
	memset((void *) &worm_attr, 0, sizeof worm_attr);
	iobj.base.fd = open(reg_tfile, O_RDWR | O_CREAT, 0600);
	ns_genobj_get_worm(&iobj.base, &worm_attr, &worm_dom, NULL, NULL,
	    &error);
	fail_if(worm_attr.w_is_valid, "mismatched worm attr");
	delete_test_worm_file(reg_tfile, NULL); //clean up

	// Free domain root string allocated by ns_genobj_get_worm.
	free(worm_dom.domain_root);
	worm_dom.domain_root = NULL;

	///  test worm file not committed
	const char *worm_root = "/ifs/__unit_test_worm__/";
	const char *worm_f = "worm_f";

	create_test_worm_file(worm_root, worm_f, NULL, NULL);

	struct fmt FMT_INIT_CLEAN(file_pn);
	fmt_print(&file_pn, "%s%s", worm_root, worm_f);

	iobj.base.fd = open(fmt_string(&file_pn), O_RDONLY);
	fail_if(iobj.base.fd < 0, "failed to open test file %s",
	    fmt_string(&file_pn));

	ns_genobj_get_worm(&iobj.base, &worm_attr, &worm_dom, NULL, NULL,
	    &error);
	fail_if(error, "failed to get object worm:%#{}",
	    isi_error_fmt(error));
	failed = !worm_attr.w_is_valid || worm_attr.w_attr.w_committed ||
	    worm_attr.w_attr.w_retention_date != 0;
	fail_if(failed, "mismatched worm attr");
	delete_test_worm_file(worm_root, NULL); //clean up

	// Free domain root string allocated by ns_genobj_get_worm.
	free(worm_dom.domain_root);
	worm_dom.domain_root = NULL;

}

/*
 * test get committed worm file
 */
TEST(test_ns_object_get_worm2)
{
	struct isi_error *error = NULL;
	bool failed = false;
	struct iobject_handle iobj;
	struct object_worm_attr worm_attr = {};
	struct object_worm_domain worm_dom = {};

	memset((void *) &worm_attr, 0, sizeof worm_attr);
	memset((void *) &worm_dom, 0, sizeof worm_dom);
	error = NULL;

	const char *worm_root = "/ifs/__unit_test_worm__/";
	const char *worm_f = "worm_f";
	create_test_worm_file(worm_root, worm_f, "13071200", true);

	struct fmt FMT_INIT_CLEAN(file_pn);
	fmt_print(&file_pn, "%s%s", worm_root, worm_f);

	iobj.base.fd = open(fmt_string(&file_pn), O_RDONLY);
	fail_if(iobj.base.fd < 0, "failed to open test file %s",
	    fmt_string(&file_pn));

	ns_genobj_get_worm(&iobj.base, &worm_attr, &worm_dom, NULL, NULL,
	    &error);
	fail_if(error, "failed to get object worm:%#{}",
	    isi_error_fmt(error));
	failed = !worm_attr.w_is_valid || !worm_attr.w_attr.w_committed;
	// TODO: make date non-hard-coded
	// failed = failed || worm_attr.w_attr.w_retention_date != 1389114000;
	fail_if(failed, "mismatched worm attr");

	delete_test_worm_file(worm_root, worm_f); //clean up

	// Free domain root string allocated by ns_genobj_get_worm.
	free(worm_dom.domain_root);
	worm_dom.domain_root = NULL;

}

/**
 * test set worm on object
 */
TEST(test_ns_object_set_worm)
{
	struct isi_error *error = NULL;
	//bool failed = false;
	struct iobject_handle iobj;
	struct object_worm_set_arg set_arg = {};

	set_arg.w_retention_date = time(0) + 3600;
	set_arg.w_commit = 1;

	//  test regular file
	const char *reg_tfile = "/ifs/__unit_test_non_worm__";
	iobj.base.fd = open(reg_tfile, O_RDWR | O_CREAT, 0600);

	ns_object_set_worm(&iobj, &set_arg,  &error);
	fail_if(!error, "should fail on regular file");
	isi_error_free(error);
	delete_test_worm_file(reg_tfile, NULL); //clean up

	///  test worm file not committed
	error = NULL;
	const char *worm_root = "/ifs/__unit_test_worm__/";
	const char *worm_f = "worm_f";
	create_test_worm_file(worm_root, worm_f, NULL, NULL);

	struct fmt FMT_INIT_CLEAN(file_pn);
	fmt_print(&file_pn, "%s%s", worm_root, worm_f);
	iobj.base.fd = open(fmt_string(&file_pn), O_RDONLY);

	ns_object_set_worm(&iobj, &set_arg, &error);
	fail_if(error, "failed to set worm %#{}", isi_error_fmt(error));

	delete_test_worm_file(worm_root, worm_f); //clean up
}
