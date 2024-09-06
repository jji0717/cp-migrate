#include <check.h>

#include <ifs/ifs_types.h>

#include <stdio.h>
#include <time.h>

#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>
#include <isi_licensing/licensing.h>

#include "check_utils.h"
#include "iobj_licensing.h"
#include "iobj_ostore_protocol.h"

TEST_FIXTURE(store_setup);
TEST_FIXTURE(store_teardown);
TEST_FIXTURE(test_bucket_setup);
TEST_FIXTURE(test_bucket_teardown);

SUITE_DEFINE_FOR_FILE(check_bucket, 
    .suite_setup = store_setup,
    .suite_teardown = store_teardown,
    .test_setup = test_bucket_setup, 
    .test_teardown = test_bucket_teardown,
    .timeout = 12);

#define TEST_STORE "__unittest-bucket"
#define TEST_STORE_BUCKET_LS "__unittest-bucket-ls__"
#define BUCKET_ACCT "unittst"
#define BUCKET_NAME "bucket_bucket_name"
#define LIMIT_MAX_BUCKETS 5; /* limit to test max bucket check against */
#define ACCESS_CHECK_DIR_NAME "__access_check__"
#define ACCESS_CHECK_UID 1001
#define ACCESS_CHECK_ROOT_UID 0

static struct ostore_handle * g_STORE_HANDLE = 0;
char * g_test_shim_acct = "unitest_shim_acct";
char * g_test_bucket_with_acct = "unitest_bucket_with_acct";

/* at the beginning, start with an empty store */
TEST_FIXTURE(store_setup)
{
	test_util_remove_store(TEST_STORE);
	test_util_remove_store(TEST_STORE_BUCKET_LS);

	iobj_set_license_ID(IOBJ_LICENSING_UNLICENSED);
}

/* at the end, let's leave the store empty */
TEST_FIXTURE(store_teardown)
{
	test_util_remove_store(TEST_STORE);
	test_util_remove_store(TEST_STORE_BUCKET_LS);
}

/**
 * Common setup routine for testing bucket. 
 * Create the store and set it to g_STORE_HANDLE
 */

TEST_FIXTURE(test_bucket_setup)
{	
	struct isi_error *error = 0;

	g_STORE_HANDLE = iobj_ostore_create(OST_OBJECT, TEST_STORE, NULL, &error);

	fail_if(error, "received error %#{} creating store", 
		isi_error_fmt(error));		
}

/**
 * Common teardown routine for testing bucket. 
 * Close the store g_STORE_HANDLE.
 */

TEST_FIXTURE(test_bucket_teardown)
{	

	struct isi_error *error = 0;
	iobj_ostore_close(g_STORE_HANDLE, &error);

	fail_if(error, "received error %#{} close store", 
		isi_error_fmt(error));
}

/**
 * test creating a new bucket / close / open / delete
 */
TEST(test_bucket_create)
{
	// create bucket:		
	// The access right (BUCKET_READ) passed is not used during 
	// bucket creation. 
	struct ibucket_handle *ibh = 0;
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET, 
	    (enum ifs_ace_rights) BUCKET_READ, 
	    g_STORE_HANDLE, BUCKET_ACCT, BUCKET_NAME);

	test_util_close_bucket(ibh);
	
	// open bucket:
	ibh = test_util_open_bucket(false, OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ, 
	    g_STORE_HANDLE, BUCKET_ACCT, BUCKET_NAME);

	test_util_close_bucket(ibh);

	// delete bucket
	struct isi_error *error = NULL;
	struct ibucket_name *ibn = NULL;

	ibn = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, 
	    BUCKET_ACCT, BUCKET_NAME, &error);
	fail_if(error, "received error %#{} generating ibn for bucket delete",
	    isi_error_fmt(error));

	error = iobj_ibucket_delete(ibn, false);
	fail_if(error, "received error %#{} deleting bucket", 
		isi_error_fmt(error));	
	iobj_ibucket_name_free(ibn);
}

/**
 * test create bucket with account specified  and delete
 */
TEST(test_create_bucket_with_acct)
{
	struct isi_error *error = 0;
	//create bucket
	struct ibucket_name *ib = 0;
	struct ibucket_handle *ibh = 0;
	
	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, g_test_shim_acct, 
			g_test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket", 
		isi_error_fmt(error));
	
	ibh = iobj_ibucket_create(ib, 0666, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	test_util_close_bucket(ibh);

	// delete
	error = iobj_ibucket_delete(ib, false);
	fail_if(error, "received error %#{} deleting bucket", 
		isi_error_fmt(error));		
		
	iobj_ibucket_name_free(ib);
}

/**
 * test delete bucket with object in it
 */
TEST(test_delete_bucket_with_object)
{	
	struct isi_error *error = 0;
	struct ibucket_name *ib = 0;
	struct ibucket_handle *ibh = 0;
	char *obj_name = "unitest_object_name";
	char * new_test_bucket_with_acct = "unitest_new_bucket_with_acct";
	
	// create bucket
	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, g_test_shim_acct, 
	    new_test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));
	
	ibh = iobj_ibucket_create(ib, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	// create object
	struct iobject_handle *oh = iobj_object_create(ibh, obj_name,
	    0, 0600, NULL, OSTORE_ACL_OBJECT, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	// try to delete bucket; it should not succeed
	error = iobj_ibucket_delete(ib, false);
	fail_if(error==0);
	isi_error_free(error);
	error = 0;

	// clean up to catch memory leaks
	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", isi_error_fmt(error));
	test_util_close_bucket(ibh);
	iobj_ibucket_name_free(ib);
}

/** 
 * test where we create and delete the bucket and then close the handle
 */
TEST(test_delete_followed_by_close)
{
	struct isi_error *error = 0;
	struct ibucket_name *ib = 0;
	struct ibucket_handle *ibh = 0;

	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, 
		g_test_shim_acct, g_test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket name", 
		isi_error_fmt(error));
	
	// create bucket 
	ibh = iobj_ibucket_create(ib, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	// delete bucket
	error = iobj_ibucket_delete(ib, false);
	fail_if(error, "received error %#{} deleting bucket", 
		isi_error_fmt(error));	
	
	// close handle
	test_util_close_bucket(ibh);

	iobj_ibucket_name_free(ib);
}

#define BUCKET_LIST_COUNT_1      8
#define BUCKET_LIST_ABORT_COUNT  5
#define BUCKET_LIST_COUNT_2      2
#define BUCKET_LIST_COUNT_TOTAL  (BUCKET_LIST_COUNT_1 + BUCKET_LIST_COUNT_2)
#define BUCKET_LIST_ACCT         "__bucket_list_acct__"
#define BUCKET_LIST_NAME_FMT     "__bucket_list_%03d_bucket__"
/*
 * NB: We know that the buckets will be returned with names that have
 * account_bucket as their name, so we use the following to formulate that
 * name for comparison later when we first create the bucket. If we change the
 * interface to return these seperately at some point than this code will need
 * to be corrected as well.
 */
#define BUCKET_LIST_ACT_NAME_FMT BUCKET_LIST_ACCT "_" BUCKET_LIST_NAME_FMT

int delete_list[] = {4,1,2};
#define BUCKET_LIST_DELETE_COUNT (sizeof(delete_list)/sizeof(delete_list[0]))

#define BUCKET_LIST_MAX_PASS            4
#define BUCKET_LIST_MAX_PASS_ITERATIONS 6
int pass_list[BUCKET_LIST_MAX_PASS][BUCKET_LIST_MAX_PASS_ITERATIONS]= {
	{2,6,2,-1,}, // -1 signifies go to the end....
	{1,3,1,4,-1},
	{-1},
	{6,1,1,1,1,-1}
};

struct buckets_find {
	char  *name;
	struct ibucket_name *ibn;
	bool   found[BUCKET_LIST_MAX_PASS];
	bool   deleted;
};

/*
 * Helper function to create a series of buckets and stash some info away aout
 * them as they are created.
 */
static void
create_bucket_sequence(struct ostore_handle *ios, 
    struct buckets_find *bucket_list, int bl_start, int bl_end)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ibh = NULL;

	struct fmt FMT_INIT_CLEAN(bname);
	struct fmt FMT_INIT_CLEAN(AandBname);

	int i, p;

	/*
	 * Create a series of buckets and stash away the info for we can
	 * verify and clean up after ourselves.
	 */
	for (i = bl_start; i < bl_end; i++) {

		fmt_print(&bname, BUCKET_LIST_NAME_FMT, i);
		fmt_print(&AandBname, BUCKET_LIST_ACT_NAME_FMT, i);

		/*
		 * create the bucket information (hash, etc.) for use in
		 * creating (and deleting) the bucket itself.  Note that we
		 * stash away the ibn so we can use it for clean up later.
		 */
		if (!bucket_list[i].ibn) {
			bucket_list[i].ibn = iobj_ibucket_from_acct_bucket(ios, 
			    BUCKET_LIST_ACCT, (char *)fmt_string(&bname), 
			    &error);
			fail_if(error, 
			    "Received error %#{} generating name for "
			    "bucket list",
			    isi_error_fmt(error));
		}

		if (!bucket_list[i].name || bucket_list[i].deleted){
			/* 
			 * create the bucket
			 */
			ibh = iobj_ibucket_create(bucket_list[i].ibn, 
			    0600, NULL, OSTORE_ACL_BUCKET, false, &error);
			fail_if(error, 
			    "Received error %#{} creating bucket for list",
			    isi_error_fmt(error));

			/*
			 * stash away the name for the comparison.  As noted
			 * above, we actually name the bucket with the
			 * account_bucket so we stash away that generated name
			 * here. 
			 */
			if (bucket_list[i].name)
				free(bucket_list[i].name);
			bucket_list[i].name = strdup(fmt_string(&AandBname));
		
			/*
			 * We're not going to do anything with the bucket, so
			 * close it while we have the handle. 
			 */
			iobj_ibucket_close(ibh, &error);
			fail_if(error, "Received error %#{} closing bucket",
			    isi_error_fmt(error));

			for (p = 0; p < BUCKET_LIST_MAX_PASS; p++)
				bucket_list[i].found[p] = false;
			bucket_list[i].deleted = false;
		}

		/* 
		 * truncate the names so that we can reuse the fmt structs.
		 */
		fmt_truncate(&bname);
		fmt_truncate(&AandBname);
	}
}

struct bucket_list_arg {
	struct buckets_find *bucket_list;
	int pass;
	int bl_start;
	int bl_end;
	int counter;  // max entries to list; set negative to render ineffective
};

static bool
mark_buckets(void *context_arg, const char *bucket_name)
{
	struct bucket_list_arg *bla = (struct bucket_list_arg *) context_arg;
	bool found = false;
	int pass = bla->pass;
	int i;

	/* 
	 * verify that the bucket_name returned is a known bucket and and mark
	 * it found 
	 */

	for (i = bla->bl_start, found = false; i < bla->bl_end; i++) {
		if (bla->bucket_list[i].name &&
		    !strcmp(bucket_name, bla->bucket_list[i].name)) {
			bla->bucket_list[i].found[pass] = true;
			found = true;
			break;
		}
	}

	fail_if(!found, "Found unexpected bucket %s in list",
	    bucket_name);

	if (bla->counter > 0) { 
		bla->counter -= 1;
		if (bla->counter == 0)
			return false;
	}

	return true;
}

static void
check_for_not_found(struct ostore_handle *ios, int pass, 
    struct buckets_find *bucket_list, int bl_start, int bl_end)
{
	int i;

	/*
	 * Now go through the list of created buckets and see if there
	 * were any that we didn't find from the listing. If so, generate a
	 * failure.  
	 */
	for (i = bl_start; i < bl_end; i++) {
		/*
		 * Check to see if the bucket was never found and while we are
		 * there, clean up the memory we used to stash away the name.
		 */
		if (bucket_list[i].name && !bucket_list[i].deleted)
			fail_if(!bucket_list[i].found, 
			    "Bucket listing did not find entry %s",
			    bucket_list[i].name);
		/*
		 * reset the found flag for the next test
		 */
		bucket_list[i].found[pass] = false;
	}
}

TEST(test_bucket_list)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ios = NULL;
	struct fmt FMT_INIT_CLEAN(bname);
	struct fmt FMT_INIT_CLEAN(AandBname);
	int i, d;
	size_t count_bucket;

	struct buckets_find *bucket_list  = 
	    (struct buckets_find *) calloc(BUCKET_LIST_COUNT_TOTAL,
		sizeof (struct buckets_find));

	struct bucket_list_arg bla;
	bla.bucket_list = bucket_list;
	bla.pass = 0;
	bla.bl_start = 0;
	bla.bl_end = BUCKET_LIST_COUNT_TOTAL;
	bla.counter = -1;
	
	/*
	 * Open a clean store to be sure that we have a store where we have a
	 * clean slate for testing the listing. If any buckets were left
	 * around in the store we are using then this test would fail.
	 */
	ios = iobj_ostore_create(OST_OBJECT, TEST_STORE_BUCKET_LS, NULL, &error);
	fail_if(error, "Received error %#{} creating store for bucket list",
	    isi_error_fmt(error));

	/*
	 * Try listing an empty store
	 */
	count_bucket = iobj_ibucket_list(ios, mark_buckets, &bla, &error);
	fail_if(error, "Received error %#{} listing bucket",
	    isi_error_fmt(error));
	fail_if(count_bucket, "Found buckets in empty store");

	/*
	 * Generate the first set of buckets
	 */
	create_bucket_sequence(ios, bucket_list, 0, BUCKET_LIST_COUNT_1);

	/*
	 * Try listing a full store
	 */
	count_bucket = iobj_ibucket_list(ios, mark_buckets, &bla,
			   &error);
	fail_if(error, "Received error %#{} listing bucket",
	    isi_error_fmt(error));
	fail_if(count_bucket != BUCKET_LIST_COUNT_1, "Found %d buckets",
	    count_bucket);

	/*
	 * Now go through the list of created buckets and see if there
	 * were any that we didn't find from the listing. If so, generate a
	 * failure.  (NOTE THAT THIS RESETS the found flag for the pass).
	 */
	check_for_not_found(
		ios,                      // store
		0,                        // pass
		bucket_list,              // bucket list
		0,                        // bucket list start position
		BUCKET_LIST_COUNT_TOTAL); // bucket list end position


	/*
	 * Try an incomplete listing by specifying a max number of
	 * buckets to count
	 */
	bla.counter = BUCKET_LIST_ABORT_COUNT;
	count_bucket = iobj_ibucket_list(ios, mark_buckets, &bla,
			   &error);
	fail_if(error, "Received error %#{} listing bucket",
	    isi_error_fmt(error));
	fail_if(count_bucket != BUCKET_LIST_ABORT_COUNT, "Found %d buckets",
	    count_bucket);
	bla.counter = -1; // reset to do complete listing henceforth

	/*
	 * Now delete some of the buckets
	 */
	for (d = 0; d < BUCKET_LIST_DELETE_COUNT; d++) {
		i = delete_list[d];
		if (i >= BUCKET_LIST_COUNT_TOTAL)
			continue;

		if (bucket_list[i].ibn) {
			error = iobj_ibucket_delete(bucket_list[i].ibn, false);
			fail_if(error, 
			    "Received error %#{} deleting bucket after listing",
			    isi_error_fmt(error));
			bucket_list[i].deleted = true;
		}
	}

	/*
	 * Now create all the buckets
	 */
	create_bucket_sequence(ios, bucket_list, 0, BUCKET_LIST_COUNT_TOTAL); 

	/*
	 * Try listing the buckets again
	 */
	count_bucket = iobj_ibucket_list(ios, mark_buckets, &bla,
			   &error);
	fail_if(error, "Received error %#{} listing bucket",
	    isi_error_fmt(error));
	fail_if(count_bucket != BUCKET_LIST_COUNT_TOTAL,
	    "Found %d buckets", count_bucket);


	/*
	 * Now go through the list of created buckets and see if there
	 * were any that we didn't find from the listing. If so, generate a
	 * failure.  
	 */
	check_for_not_found(
		ios,                      // store
		0,                        // pass
		bucket_list,              // bucket list
		0,                        // bucket list start position
		BUCKET_LIST_COUNT_TOTAL); // bucket list end position


	/*
	 * Now go back through the list of created buckets and clean up.
	 */
	for (i = 0; i < BUCKET_LIST_COUNT_TOTAL; i++) {
		/*
		 * Check to see if the bucket was never found and while we are
		 * there, clean up the memory we used to stash away the name.
		 */
		if (bucket_list[i].name) {
			free(bucket_list[i].name);
			bucket_list[i].name = NULL;
		}		
		
		/*
		 * If we still have the ibn, use it to delete the bucket as we
		 * are done with it. After it is deleted, release the ibn.
		 */
		if (bucket_list[i].ibn) {
			if (!bucket_list[i].deleted) {
				error = iobj_ibucket_delete(bucket_list[i].ibn,
				    false);
				fail_if(error, 
				    "Received error %#{} deleting bucket "
				    "after listing",
				    isi_error_fmt(error));
			}

			iobj_ibucket_name_free(bucket_list[i].ibn);
			bucket_list[i].ibn = NULL;
		}
	}
	
	/*
	 * Guess we made it, delet the store we created for this test and then 
	 * free the struct we use to stash away the bucket information.
	 */
	iobj_ostore_close(ios, &error);
	fail_if(error, "Received error %#{} closing store for bucket list",
	    isi_error_fmt(error));

	free(bucket_list);
}

static int 
set_max_bucket_limit(struct ostore_handle *ios, int max_buckets)
{
	struct isi_error *error = NULL;
	struct ostore_parameters *params = NULL;
	int return_max_buckets = OSTORE_OBJECT_MAX_BUCKETS;

	iobj_ostore_parameters_get(ios, &params, &error);
	fail_if(error, "received error %#{} getting initial store parameters",
	    isi_error_fmt(error));

	return_max_buckets = params->max_buckets;

	if (max_buckets != -1)
		params->max_buckets = max_buckets;

	iobj_ostore_parameters_set(ios, params, &error);
	fail_if(error, "received error %#{} setting store parameters for "
	    "max buckets", isi_error_fmt(error));

	iobj_ostore_parameters_release(ios, params);

	return return_max_buckets;
}

TEST(test_bucket_limit)
{
	struct ibucket_handle *ibh[OSTORE_OBJECT_MAX_BUCKETS], *ibh1 = NULL;
	struct ibucket_name *ibn[OSTORE_OBJECT_MAX_BUCKETS], *ibn1 = NULL;
	char bucket_name[100];
	struct ostore_handle *ios = NULL;
	struct isi_error *error = NULL;
	size_t i;
	int limit_max_buckets = LIMIT_MAX_BUCKETS;
	int original_max_buckets = -1;

	ios = iobj_ostore_create(OST_OBJECT, TEST_STORE_BUCKET_LS, NULL, &error);
	fail_if(error, "Received error %#{} creating store for bucket list",
	    isi_error_fmt(error));

	original_max_buckets = set_max_bucket_limit(ios, limit_max_buckets);
		
	// init
	for (i = 0; i < limit_max_buckets; ++i)
		ibh[i] = 0;

	// create upto max buckets.
	// Access right (BUCKET_READ) passed is not used during bucket creation.
	for (i = 0; i < limit_max_buckets; ++i) {
		snprintf(bucket_name, sizeof(bucket_name) - 1, "bucket%zu", i);
		ibh[i] = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
		    (enum ifs_ace_rights) BUCKET_READ, ios, 
		    BUCKET_ACCT, bucket_name);
		ibn[i] = iobj_ibucket_from_acct_bucket(ios, BUCKET_ACCT, 
		    bucket_name, &error);
	}

	// try to create one more--this should fail
	snprintf(bucket_name, sizeof(bucket_name) - 1, "bucket%zu", i);
	ibn1 = iobj_ibucket_from_acct_bucket(ios, BUCKET_ACCT, "bucket", 
	    &error);
	fail_if(error, "received error %#{} formatting ibn from acct an bucket",
	    isi_error_fmt(error));
	ibh1 = iobj_ibucket_create(ibn1, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error == NULL, "expected max bucket limit failure");
	isi_error_free(error);
	error = NULL;

	// cleanup
	iobj_ibucket_name_free(ibn1);
	for (i = 0; i < limit_max_buckets; ++i) {
		test_util_close_bucket(ibh[i]);
		iobj_ibucket_delete(ibn[i], false);
		iobj_ibucket_name_free(ibn[i]);
	}

	/*
	 * Set max buckets back to original value
	 */
	if (original_max_buckets != -1)
		set_max_bucket_limit(ios, original_max_buckets);
		
	iobj_ostore_close(ios, &error);
	fail_if(error, "Received error %#{} closing store for bucket list",
	    isi_error_fmt(error));

}

TEST(test_bucket_access)
{
	struct ibucket_handle *ibh = NULL;
	struct ibucket_handle *ibh_fail = NULL;
	struct ibucket_name *ibn = NULL;
	struct isi_error *error = NULL;
	int flags = 0;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;

	struct fmt FMT_INIT_CLEAN(path);

	char *dir_name = ACCESS_CHECK_DIR_NAME;
	char *buck_name = "bucket_access_unittest";
	
	int level = 1;
	int rv = -1;

	// create bucket:		
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
	    BUCKET_READ, g_STORE_HANDLE, 
	    BUCKET_ACCT, buck_name);
	
	fmt_print(&path, "%s/%s_%d", ibh->base.path, dir_name, level++);

	test_util_close_bucket(ibh);

	rv = seteuid(ACCESS_CHECK_UID);
	fail_if((rv < 0), "Could not set euid to %d", ACCESS_CHECK_UID);

	ibn = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, BUCKET_ACCT,
	    buck_name, &error);
	fail_if(error, "received error %#{} formatting ibn from acct an bucket",
	    isi_error_fmt(error));

	ibh_fail = iobj_ibucket_open(ibn, BUCKET_READ, flags, cf_flags, &error);
		
	fail_if(!error, "Was expecting to be denied access to bucket "
	    "but succeeded");
	isi_error_free(error);
	error = NULL;

	/*
	 * Clean up so we don't leak any memory.
	 */
	iobj_ibucket_name_free(ibn);

	rv = mkdir(fmt_string(&path), 0777);
	fail_if((rv == 0), 
	    "Could create dir under store when it should be denied");

	rv = seteuid(ACCESS_CHECK_ROOT_UID);
	fail_if((rv < 0), "Could not set euid to %d", ACCESS_CHECK_ROOT_UID);
	
	rv = mkdir(fmt_string(&path), 0777);
	fail_if((rv < 0), "Could not create dir under store");

	/*
	 * Add a bit more to the path, this would be a second level
	 * hash directory which we should be able to do as the first level
	 * will allow all access (as would the second)
	 */
	fmt_print(&path, "/%s_%d", dir_name, ++level);

	rv = seteuid(ACCESS_CHECK_UID);
	fail_if((rv < 0), "Could not set euid to %d", ACCESS_CHECK_UID);

	rv = mkdir(fmt_string(&path), 0777);
	fail_if((rv < 0), "Could not create dir under store");

	rv = seteuid(ACCESS_CHECK_ROOT_UID);
	fail_if((rv < 0), "Could not set euid to %d", ACCESS_CHECK_ROOT_UID);	
}

TEST(test_check_acl_values)
{
	struct ibucket_handle *ibh = 0;
	struct fmt FMT_INIT_CLEAN(path);

	char *store_path = NULL;
	char *bucket_path = NULL;
	char *sep = NULL;
	char *fnd_str = NULL;
	int level = 0;
	bool rv = false;

	// create bucket:		
	// Access right (BUCKET_READ) passed is not used during bucket 
	// creation. 
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ, 
	    g_STORE_HANDLE, BUCKET_ACCT, BUCKET_NAME);
	
	store_path = g_STORE_HANDLE->path;
	bucket_path = ibh->base.path;

	fmt_print(&path, "%s", store_path);
	
	/* 
	 * get the pieces of the bucket path one by one, starting with the
	 * first hash directory name and ending with the bucket name
	 */
	sep = bucket_path + strlen(store_path) + 1;
	while (1) {
		int namelen;

		/* check to see if we are done */
		if (!sep) 
			break;

		/* we're at the next level */
		level++;

		/* split the path at the next '/' */
		fnd_str = sep;
		sep = strchr(fnd_str, '/');
		if (sep) {
			namelen = (sep - fnd_str);
			sep++;
		} else {
			/* suck to the end of the string */
			namelen = strlen(fnd_str);
		}

		/* Add the new component to the path, grabbing up to but not
		   including the slash. */
		fmt_print(&path, "/%.*s", namelen, fnd_str);

		/*
		 * check_iobj_acl returns boolean true for compare success,
		 * false for failure
		 */
		rv = check_iobj_acl(fmt_string(&path), 
		    ((level < 3) ? OSTORE_ACL_BUCKET_HASH : OSTORE_ACL_BUCKET));
		fail_if(!rv, "Unexpected acl set on %s",
		    ((level < 3) ? "bucket_hash" : "bucket"));
	}
	test_util_close_bucket(ibh);
}

// Retain for performance testing
#if 0

static bool
dummy_list_cb(void * this, const char * bucket_name)
{
	return true;
}

#define CALC_ELAPSED_TIME(ts2,ts1) do { ts2.tv_sec -= ts1.tv_sec; 		\
					ts2.tv_nsec -= ts1.tv_nsec; 		\
					if (ts2.tv_nsec < 0) { 			\
						ts2.tv_sec -= 1; 		\
						ts2.tv_nsec += 1000000000; 	\
					} 					\
} while (0)

#define MAX_TST_ITER 100
TEST(test_bucket_list_perf)
{
	struct ibucket_handle *ibh[OSTORE_OBJECT_MAX_BUCKETS];
	char bucket_name[100];
	struct ostore_handle *ios = NULL;
	struct isi_error *error = NULL;
	struct timespec ts1, ts2;
	int rc, i, count;

	ios = iobj_ostore_create(OST_OBJECT, TEST_STORE_BUCKET_LS, NULL, &error);
	fail_if(error, "Received error %#{} creating store for bucket list",
	    isi_error_fmt(error));

	// create upto max buckets.
	// The access right (BUCKET_READ) passed is not used during 
	// bucket creation. 
	for (i = 0; i < OSTORE_OBJECT_MAX_BUCKETS; ++i) {
		snprintf(bucket_name, sizeof(bucket_name) - 1, "bucket%d", i);
		ibh[i] = test_util_open_bucket(true, 
		    OSTORE_ACL_BUCKET,
		    (enum ifs_ace_rights) BUCKET_READ, 
		    ios, BUCKET_ACCT, bucket_name);
	}

	rc = clock_gettime(CLOCK_MONOTONIC, &ts1);
	for (int i = 0; i < MAX_TST_ITER; ++i)
		count = iobj_ibucket_list(ios, dummy_list_cb, 0, &error);
	rc = clock_gettime(CLOCK_MONOTONIC, &ts2);
	CALC_ELAPSED_TIME(ts2,ts1);
	printf("total time taken = %d sec %d nsec, count = %d\n", ts2.tv_sec, ts2.tv_nsec, count);
	fail_if(count != OSTORE_OBJECT_MAX_BUCKETS);
	
	// cleanup
	for (i = 0; i < OSTORE_OBJECT_MAX_BUCKETS; ++i) {
		if (ibh[i])
			test_util_close_bucket(ibh[i]);
	}
	iobj_ostore_close(ios, &error);
	fail_if(error, "Received error %#{} closing store for bucket list",
	    isi_error_fmt(error));
}

#endif
