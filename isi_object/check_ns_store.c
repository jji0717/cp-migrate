#include <check.h>
#include <stdio.h>
#include <ifs/ifs_types.h>

#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>
#include <isi_object/ostore.h>
#include <isi_object/check_utils.h>

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(check_ns_store, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout=20);

#define DEFAULT_NSA	 	"ifs"
#define DEFAULT_NSA_PATH	"/ifs"
#define TMP_PATH		"/ifs/__unit_test_tmp"
#define ACCESS_PT               "__unit_test_tmp"

static struct ostore_handle *ios_g = NULL;

TEST_FIXTURE(suite_setup)
{
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = 0;
	ios_g = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);
	fail_unless(ios_g != NULL, "Expected ifs namespace by default");
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = 0;
	iobj_ostore_close(ios_g, &error);
	fail_if(error, "Closing a namespace store failed.");
}

/* test that we fail if we open a non-existent store */
TEST(test_invalid_open)
{
	struct isi_error *error = 0;
	iobj_ostore_open(OST_NAMESPACE, "__unittest-non-existent",
		HEAD_SNAPID, &error);
	fail_unless(error != 0, "expected error opening non-existent store");
	isi_error_free(error);
}

/* test open the default namespace access point "ifs" */
TEST(test_open_ifs)
{
	struct isi_error *error = 0;
	struct ostore_handle *ios= iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);
	fail_unless(ios != NULL, "Expected ifs namespace by default");
	
	iobj_ostore_close(ios, &error);
	fail_if(error, "Closing a namespace store failed.");
}

/* test create / destroy store access point */
TEST(test_create_destroy_store)
{
	struct isi_error *error = 0;

	// create a store, minimize colliding with existing ones
	const char *ap = "access_point_unit_test";
	struct ostore_handle *ios= iobj_ostore_create(OST_NAMESPACE, ap,
	    DEFAULT_NSA_PATH, &error);
	fail_if(ios == NULL, "Creating store access point failed.");

	// close and destroy
	iobj_ostore_close(ios, &error);
	fail_if(error, "Closing namespace store failed.");
	error = iobj_ostore_destroy(OST_NAMESPACE, ap);
	fail_if(error, "Destroying store access point failed.");
}

/* test create / destroy store access point */
TEST(test_create_store_error_cases)
{
	struct isi_error *error = 0;
	const char *ap = "access_point_unit_test";
	struct ostore_handle *ios = 0;

	// creae store to non-exist path
	ios = iobj_ostore_create(OST_NAMESPACE, ap,
	    DEFAULT_NSA_PATH "/__unittest-non-existent_path", &error);
	fail_unless(ios == NULL,
	    "Expected failure creating access point to non-exist dir");
	fail_unless(isi_error_is_a(error,
	    OSTORE_PATHNAME_NOT_EXIST_ERROR_CLASS),
	    "Expected failure creating access point to non-exist dir");
	isi_error_free(error);
	error = 0;

	// creae store to exist but not OneFS path
	ios = iobj_ostore_create(OST_NAMESPACE, ap,
	    "/root", &error);
	fail_unless(ios == NULL,
	    "Expected failure creating access point to non-onefs path");
	fail_unless(isi_error_is_a(error,
	    OSTORE_PATHNAME_NOT_ONEFS_ERROR_CLASS),
	    "Expected failure creating access point to non-onefs path");
	isi_error_free(error);
	error = 0;
}

static void
create_test_src_file(const char *src)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	fmt_print(&cmdstr, "echo \"hello\" > %s ", src);
	fmt_print(&cmdstr, ";isi_runat %s touch ads2 ", src);
	ret = system(fmt_string(&cmdstr));
	fail_if(ret, "%s failed", fmt_string(&cmdstr));
}

static int
file_compare(const char *src, const char *dest)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	fmt_print(&cmdstr, "/usr/bin/diff -r %s %s", src, dest);
	ret = system(fmt_string(&cmdstr));
	return ret;
}

static int
check_ads(const char *dest)
{
	int ret = 0;
	char cmdstr[1024];
	sprintf(cmdstr, "isi_runat %s ls ads2 > /dev/null", dest);
	ret = system(cmdstr);
	return ret;
}

/* test copy file*/
TEST(test_copy_file)
{
	int rval = 0;
	struct isi_error *error = 0;
	const char *src = TMP_PATH "/_file__";
	const char *dest = TMP_PATH "/_file2__";
	// iobj_genobj_copy routine requires path without access point
	const char *psrc = src + strlen(DEFAULT_NSA_PATH) + 1;
	const char *pdest = dest + strlen(DEFAULT_NSA_PATH) + 1;

	create_test_folder(TMP_PATH);
	create_test_src_file(src);

	struct fsu_copy_flags cp_flags;
	cp_flags.cont = 0;
	cp_flags.noovw = 1;
	cp_flags.merge = 0;

	struct ostore_copy_param o_cp_param = {
	    {psrc, pdest, cp_flags, NULL, NULL}, ios_g, ios_g
	};

	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, dest), "%s and %s are not same",
	    src, dest);
	fail_if(check_ads(dest), "ads in file %s is missing", dest);

	// check when target exists
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_unless(rval, "expect error");
	isi_error_free(error);
	error = 0;
	rval = 0;
	// check again with overwrite allowed
	o_cp_param.base_param.cp_flags.noovw = 0;
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, dest), "%s and %s are not same",
	    src, dest);

	delete_test_folder(TMP_PATH);
}

/* test copy and delete folder*/
TEST(test_copy_delete_folder)
{
	int rval = 0;
	struct isi_error *error = 0;
	const char *src = TMP_PATH "/_fd__";
	const char *dest = TMP_PATH "/_fd2__";
	const char *psrc = src + strlen(DEFAULT_NSA_PATH) + 1;
	const char *pdest = dest + strlen(DEFAULT_NSA_PATH) + 1;

	create_test_folder(src);
	create_test_src_file(TMP_PATH "/_fd__/file1");
	create_test_folder(TMP_PATH "/_fd__/folder1");
	struct fsu_copy_flags cp_flags;
	cp_flags.cont = 0;
	cp_flags.noovw = 1;
	cp_flags.merge = 0;
	cp_flags.recur = 1;
	struct ostore_copy_param o_cp_param = {
	    {psrc, pdest, cp_flags, NULL, NULL}, ios_g, ios_g
	};
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, dest), "%s and %s are not same",
	    src, dest);
	fail_if(check_ads(dest), "ads in dir %s is missing", dest);
	fail_if(check_ads(TMP_PATH "/_fd2__/file1"),
	    "ads in file %s is missing", pdest);

	// check when target exists
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_unless(rval, "expect error");
	isi_error_free(error);
	error = 0;
	rval = 0;

	// check again with overwrite  and merge allowed
	o_cp_param.base_param.cp_flags.noovw = 0;
	o_cp_param.base_param.cp_flags.merge = 1;
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, dest), "%s and %s are not same",
	    src, dest);

	struct ostore_del_flags del_flags = {.recur =1, .cont_on_err = 1};
	iobj_genobj_delete(ios_g, ACCESS_PT/*"__unit_test_tmp"*/, del_flags, &error);
	fail_if(error, "received error %#{} ", isi_error_fmt(error));
	//delete_test_folder(TMP_PATH);
}

/* test move folder*/
TEST(test_move_folder)
{
	int rval = 0;
	struct isi_error *error = 0;
	const char *src = TMP_PATH "/_fd__";
	const char *psrc = src + strlen(DEFAULT_NSA_PATH) + 1;
	const char *dest = TMP_PATH "/_fd2__";
	const char *pdest = dest + strlen(DEFAULT_NSA_PATH) + 1;
	const char *dest_mv = TMP_PATH "/_fd_mv__";
	const char *pdest_mv = dest_mv + strlen(DEFAULT_NSA_PATH) + 1;
	const char *src_ovw = TMP_PATH "/_fd3__";
	const char *psrc_ovw = src_ovw + strlen(DEFAULT_NSA_PATH) + 1;
	const char *dest_ovw = TMP_PATH "/_fd_ovw__/_fd_mv__";
	const char *pdest_ovw = dest_ovw + strlen(DEFAULT_NSA_PATH) + 1;

	create_test_folder(src);
	create_test_src_file(TMP_PATH "/_fd__/file1");
	create_test_folder(TMP_PATH "/_fd__/folder1");
	create_test_folder(dest_ovw);

	// make a copy first
	struct fsu_copy_flags cp_flags;
	cp_flags.cont = 0;
	cp_flags.noovw = 1;
	cp_flags.merge = 0;
	cp_flags.recur = 1;
	cp_flags.cflag = 0;
	struct ostore_copy_param o_cp_param = {
                {psrc, pdest, cp_flags, NULL, NULL}, ios_g, ios_g
	};
	rval = iobj_genobj_copy(&o_cp_param, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, dest), "%s and %s are not same",
            src, dest);
	fail_if(check_ads(dest), "ads in dir %s is missing", dest);
	fail_if(check_ads(TMP_PATH "/_fd2__/file1"),
	    "ads in file %s is missing", pdest);

       // make another copy for overwrite
	struct ostore_copy_param o_cp_param2 = {
                {psrc, psrc_ovw, cp_flags, NULL, NULL}, ios_g, ios_g
	};
	rval = iobj_genobj_copy(&o_cp_param2, &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(src, src_ovw), "%s and %s are not same",
	    src, src_ovw);
	fail_if(check_ads(src_ovw), "ads in dir %s is missing", src_ovw);
	fail_if(check_ads(TMP_PATH "/_fd3__/file1"),
	    "ads in file %s is missing", psrc_ovw);

        // now do the move and check
        struct fsu_move_flags mv_flags;
        mv_flags.ovw = 0;
        rval = iobj_genobj_move(ios_g, psrc, ios_g, pdest_mv, &mv_flags,
            &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(dest_mv, dest), "%s and %s are not same",
            src, dest);
	fail_if(check_ads(dest_mv), "ads in dir %s is missing", dest_mv);
	fail_if(check_ads(TMP_PATH "/_fd_mv__/file1"),
	    "ads in file %s is missing", pdest_mv);

        // move to an already existing destination
        // check with overwrite option NOT set
        mv_flags.ovw = 0;
        rval = iobj_genobj_move(ios_g, psrc_ovw, ios_g, pdest_ovw, &mv_flags,
            &error);
        fail_unless(rval != 0, "expect error");
        fail_unless(isi_system_error_is_a(error, EEXIST), "Destination exists");
        isi_error_free(error);
        rval = 0;
        error = 0;

        // Now check with overwrite option set
        mv_flags.ovw = 1;
        rval = iobj_genobj_move(ios_g, psrc_ovw, ios_g, pdest_ovw, &mv_flags,
            &error);
	fail_if(error || rval, "received error %#{} ", isi_error_fmt(error));
	fail_if(file_compare(dest_ovw, dest), "%s and %s are not same",
            dest_ovw, dest);
	fail_if(check_ads(dest_ovw), "ads in dir %s is missing", dest_ovw);
	fail_if(check_ads(TMP_PATH "/_fd_ovw__/_fd_mv__/file1"),
	    "ads in file %s is missing", pdest_ovw);

	struct stat src_stat;
	fail_unless(stat(src, &src_stat) && errno == ENOENT,
	    "source should be moved"),

	delete_test_folder(TMP_PATH);
}
