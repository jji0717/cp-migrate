#include <check.h>

#include <ifs/ifs_types.h>

#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>

#include <isi_object/check_utils.h>

TEST_FIXTURE(store_teardown);

SUITE_DEFINE_FOR_FILE(check_store, .suite_teardown = store_teardown);

#define TEST_STORE "__unittest-store"
#define ACCESS_CHECK_DIR_NAME "__access_check__"
#define ACCESS_CHECK_UID 10
#define ACCESS_CHECK_ROOT_UID 0

int store_current_uid = ACCESS_CHECK_ROOT_UID;

/* at the end, let's leave the store empty */
TEST_FIXTURE(store_teardown)
{
	test_util_remove_store(TEST_STORE);
}


static void
reset_euid(int uid)
{
	int rv = -1;

	if (uid == store_current_uid)
		return;

	if (store_current_uid != ACCESS_CHECK_ROOT_UID) {
		rv = seteuid(ACCESS_CHECK_ROOT_UID);
		fail_if((rv < 0), "Could not set euid to %d", 
		    ACCESS_CHECK_ROOT_UID);
		rv = setegid(ACCESS_CHECK_ROOT_UID);
		fail_if((rv < 0), "Could not set egid to %d", 
		    ACCESS_CHECK_ROOT_UID);
	}

	rv = setegid(uid);
	fail_if((rv < 0), "Could not set egid to %d", uid);

	rv = seteuid(uid);
	fail_if((rv < 0), "Could not set euid to %d", uid);

	store_current_uid = uid;
}

/* test that we fail if we open a non-existent store */
TEST(test_invalid_open) {
	struct isi_error *error = 0;
	iobj_ostore_open(OST_OBJECT, "__unittest-non-existent",
	    HEAD_SNAPID, &error);
	fail_unless(error!=0, "expected error opening non-existent store");
	isi_error_free(error);
}

static bool list_bucket_callback(void *context, const char*bucket)
{
	return true;
}

/* test that we can create and open a new store */
TEST(test_first_create) {
	struct isi_error *error = 0;
	struct ostore_handle *ios = 0;
	char *store_name = TEST_STORE;

	/* make sure we start with nothing */
	test_util_remove_store(store_name);

	/* now create the store */
	ios = iobj_ostore_create(OST_OBJECT, store_name, NULL, &error);
	fail_if(error, "received error %#{} creating store", 
	    isi_error_fmt(error));

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));

	iobj_ibucket_list(ios, list_bucket_callback, NULL, &error);
	fail_if(!error, 
	    "expected error from listing buckets on closed store handle");

	fail_if(!isi_error_is_a(error, OSTORE_INVALID_HANDLE_ERROR_CLASS),
	    "expected invalid handle error");

	isi_error_free(error);
	error = NULL;

	/* run a second create to show it works */
	ios = iobj_ostore_create(OST_OBJECT, store_name, NULL, &error);
	fail_if(error, "received error %#{} creating store 2", 
	    isi_error_fmt(error));

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} during close 2", 
	    isi_error_fmt(error));
}

/**
 * prove that closing too many times is bad.
 */
TEST(test_double_close) {
	struct isi_error *error = 0;
	struct ostore_handle *ios = 0;

	char *store_name = TEST_STORE;

	ios = iobj_ostore_create(OST_OBJECT, store_name, NULL, &error);
	fail_if(error, "received error %#{} creating store", 
	    isi_error_fmt(error));

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));
	iobj_ostore_close(ios, &error);
	fail_if(!error, "expected error on second close");
	isi_error_free(error);
}



/* test that creates and opens return the same cached handles */
TEST(test_create_open_handles) {
	struct isi_error *error = 0;
	struct ostore_handle *ios1, *ios2, *ios3;

	char *store_name = TEST_STORE;

	ios1 = iobj_ostore_create(OST_OBJECT, store_name, NULL, &error);
	fail_if(error, "received error %#{} creating store", 
	    isi_error_fmt(error));

	ios2 = iobj_ostore_open(OST_OBJECT, store_name, HEAD_SNAPID, &error);
	fail_if(error, "received error %#{} opening store", 
	    isi_error_fmt(error));

	fail_if(ios1 != ios2, 
	    "got different handles %p/%p from create and open",
	    ios1, ios2);

	ios3 = iobj_ostore_open(OST_OBJECT, store_name, HEAD_SNAPID, &error);
	fail_if(error, "received error %#{} opening store", 
	    isi_error_fmt(error));

	fail_if(ios2 != ios3, 
	    "got different handles %p/%p from two opens",
	    ios2, ios3);

	iobj_ostore_close(ios3, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));
	iobj_ostore_close(ios2, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));
	iobj_ostore_close(ios1, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));
}

TEST(test_store_access)
{
	struct isi_error *error = 0;
	struct ostore_handle *ios;
	struct fmt FMT_INIT_CLEAN(path);

	char *dir_name = ACCESS_CHECK_DIR_NAME;

	char *store_name = TEST_STORE;

	int rv = -1;

	ios = iobj_ostore_create(OST_OBJECT, store_name, NULL, &error);
	fail_if(error, "received error %#{} creating store", 
	    isi_error_fmt(error));

	fmt_print(&path, "%s/%s", ios->path, dir_name);

	reset_euid(ACCESS_CHECK_UID);

	rv = mkdir(fmt_string(&path),0777);
	fail_if((rv < 0), "Could not create dir under store"); 

	reset_euid(ACCESS_CHECK_ROOT_UID);
	
	/*
	 * check_iobj_acl returns boolean true for compare success, false for
	 * failure 
	 */
	fail_if(!check_iobj_acl(fmt_string(&path), OSTORE_ACL_BUCKET_HASH),
	    "Unexpected acl set on child of store");

	reset_euid(ACCESS_CHECK_UID);

	rmdir(fmt_string(&path));

	reset_euid(ACCESS_CHECK_ROOT_UID);
	
	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} during close", 
	    isi_error_fmt(error));
}
