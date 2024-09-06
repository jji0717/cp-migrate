
#include <check.h>
#include <pthread.h>

#include <ifs/ifs_types.h>

#include <stdio.h>

#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>
#include "check_utils.h"
#include "iobj_licensing.h"
#include "iobj_ostore_protocol.h"

TEST_FIXTURE(test_bucket_setup);
TEST_FIXTURE(test_bucket_teardown);

SUITE_DEFINE_FOR_FILE(check_bucket_threads,
    .test_setup = test_bucket_setup,
    .test_teardown = test_bucket_teardown,
    .mem_check = CK_MEM_DISABLE); // Issues with dmalloc and pthread usage

#define TEST_STORE "__unittest-bucket"
#define BUCKET_ACCT "unittst"
#define BUCKET_NAME "bucket_bucket_name"

static struct ostore_handle * g_STORE_HANDLE = 0;

TEST_FIXTURE(test_bucket_setup)
{	
	struct isi_error *error = 0;
	iobj_set_license_ID(IOBJ_LICENSING_UNLICENSED);
	
	test_util_remove_store(TEST_STORE);
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
	test_util_remove_store(TEST_STORE);
}

#define CHK_BUCKET_MAX_THREADS	10
struct thr_in
{
	char *bucket_name;
	struct ibucket_handle *ibh;
	struct isi_error *error;
};

static void *
thr_cc_routine(void * arg)
{
	struct thr_in *thp = (struct thr_in *) (arg);
	struct ibucket_name *ibn = 0;

	ibn = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, BUCKET_ACCT,
		  thp->bucket_name, &(thp->error));
	if (thp->error)
		goto out;
	thp->ibh = iobj_ibucket_create(ibn, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &(thp->error));

 out:
	if (ibn)
		iobj_ibucket_name_free(ibn);
	return 0;
}


/**
* Test bucket creation with multiple concurrent requests for same bucket
* All requests should succeed
*/
TEST(test_concurrent_create)
{
	pthread_t thr[CHK_BUCKET_MAX_THREADS];
	struct thr_in thr_arg[CHK_BUCKET_MAX_THREADS];
	int ret;
	struct isi_error *error = NULL;
	char * bucket_name = "cc_bucket";

	// create
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		thr_arg[i].bucket_name = bucket_name;
		thr_arg[i].ibh = NULL;
		thr_arg[i].error = NULL;

		ret = pthread_create(&thr[i], 0, thr_cc_routine, &thr_arg[i]);
		fail_if(ret != 0);
	}

	// wait for all to be done
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		ret = pthread_join(thr[i], 0);
		fail_if(ret != 0);
	}

	// check
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		fail_if(thr_arg[i].error,
		    "received error %#{} creating bucket %d", 
		    isi_error_fmt(error));
		test_util_close_bucket(thr_arg[i].ibh);
	}
}


static void *
thr_cd_routine(void * arg)
{
	struct thr_in *thp = (struct thr_in *) (arg);
	struct ibucket_name *ibn = 0;

	ibn = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, BUCKET_ACCT,
		  thp->bucket_name, &(thp->error));
	if (thp->error)
		goto out;
	thp->error = iobj_ibucket_delete(ibn, false);

 out:
	if (ibn)
		iobj_ibucket_name_free(ibn);
	return 0;
}

/**
* Test bucket deletion with multiple concurrent requests for same bucket
* Only one request should succeed
*/
TEST(test_concurrent_delete)
{
	pthread_t thr[CHK_BUCKET_MAX_THREADS];
	struct thr_in thr_arg[CHK_BUCKET_MAX_THREADS];
	int ret;
	char * bucket_name = "cd_bucket";
	struct ibucket_handle *ibh;
	int count_success = 0; 
	
	// create a bucket
	// The access right (BUCKET_READ) passed is not used 
	// during bucket creation.
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ,
	    g_STORE_HANDLE, BUCKET_ACCT, bucket_name);
	test_util_close_bucket(ibh);

	// delete
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		thr_arg[i].bucket_name = bucket_name;
		thr_arg[i].ibh = NULL;
		thr_arg[i].error = NULL;

		ret = pthread_create(&thr[i], 0, thr_cd_routine, &thr_arg[i]);
		fail_if(ret != 0);
	}

	// wait for all to be done
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		ret = pthread_join(thr[i], 0);
		fail_if(ret != 0);
	}

	// check
	for (int i= 0; i < CHK_BUCKET_MAX_THREADS; ++i) {
		if (thr_arg[i].error == NULL)
			count_success++;
		else
			isi_error_free(thr_arg[i].error);
	}

	fail_if(count_success != 1, "count_success is %d", count_success);
}
