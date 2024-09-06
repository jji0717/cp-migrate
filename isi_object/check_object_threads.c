#include <check.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>

#include <sys/extattr.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_userattr.h>
#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>
#include <isi_object/iobj_ostore_protocol.h>

#include "check_utils.h"
#include "iobj_licensing.h"

TEST_FIXTURE(test_object_setup);
TEST_FIXTURE(test_object_teardown);

SUITE_DEFINE_FOR_FILE(check_object_threads,
	.test_setup = test_object_setup,
	.test_teardown = test_object_teardown,
	.timeout = 20,
	.mem_check = CK_MEM_DISABLE);


#define TEST_STORE		"__unittest-object"
#define BUCKET_ACCT 		"unittest" 
#define BUCKET_NAME 		"object_bucket_name"

static struct ostore_handle *g_STORE_HANDLE = 0;
static struct ibucket_handle *g_BUCKET_HANDLE = 0;

/**
 * Common setup routine for testing object. 
 * Create the store and set it to g_STORE_HANDLE
 */

TEST_FIXTURE(test_object_setup)
{	
	struct isi_error *error = NULL;

	iobj_set_license_ID(IOBJ_LICENSING_UNLICENSED);

	test_util_remove_store(TEST_STORE);
	g_STORE_HANDLE = iobj_ostore_create(OST_OBJECT, TEST_STORE, NULL, &error);
	fail_if(error, "received error %#{} creating store", 
		isi_error_fmt(error));

	// The access rights (BUCKET_READ) flag is not used,
	// during bucket creation. 
	g_BUCKET_HANDLE = test_util_open_bucket(true, 
	    OSTORE_ACL_BUCKET, (enum ifs_ace_rights) BUCKET_READ, 
	    g_STORE_HANDLE, 
	    BUCKET_ACCT, BUCKET_NAME);
	fail_if(!g_BUCKET_HANDLE,
	    "Did not create the bucket using test_util_open_bucket");
}

TEST_FIXTURE(test_object_teardown)
{	
	struct isi_error *error = NULL;
	
	test_util_close_bucket(g_BUCKET_HANDLE);
	iobj_ostore_close(g_STORE_HANDLE, &error);
	fail_if(error, "received error %#{} close store", 
		isi_error_fmt(error));
	test_util_remove_store(TEST_STORE);
}


struct conc_list_delete_arg
{
	struct ibucket_handle *ibh;
	size_t max_objects;
	const char *obj_name_prefix;
	bool stop;
};

static bool
test_list_cb(void *this, const struct iobj_list_obj_info *info)
{
	return true;
}

static void *
thr_list_routine(void * arg)
{
	struct conc_list_delete_arg *cld_arg = (struct conc_list_delete_arg *)arg;
	struct isi_error *error = NULL;
	size_t count;

	struct iobj_list_obj_param param = {0};
	param.ibh = cld_arg->ibh;
	param.caller_cb = test_list_cb;
	param.stats_requested = false;
	param.caller_context = NULL;

	while (cld_arg->stop == false){
		count = iobj_object_list(&param,
		    &error);
		fail_if(error);
	}

	return 0;
}

static void *
thr_delete_routine(void * arg)
{
	struct conc_list_delete_arg *cld_arg = (struct conc_list_delete_arg *)arg;
	struct isi_error *error = NULL;
	char obj_name[100] = {'\0'};
	
	for (int i = 0; i < cld_arg->max_objects; ++i) {
		snprintf(obj_name, sizeof(obj_name)-1, "%s-%d",
		    cld_arg->obj_name_prefix, i);
		error = iobj_object_delete(cld_arg->ibh, obj_name);
		fail_if(error);
	}

	return 0;
}
// test concurrent listing and deletion
TEST(test_concurrent_list_delete)
{
	char obj_name[100] = {'\0'};
	size_t max_objects = 10;
	const char *obj_name_prefix = "obj";
	struct ibucket_handle *ibh;
	struct isi_error *error = NULL;
	struct conc_list_delete_arg cld_arg;
	int ret;
	pthread_t lister, deleter;
	size_t count;

	// create bucket
	// The access rights (BUCKET_READ) flag is not used 
	// during bucket creation. 
	ibh = test_util_open_bucket(true, 
	    OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ, 
	    g_STORE_HANDLE, 
	    BUCKET_ACCT,
	    (char *) __FUNCTION__ 	// bucket name
	    );
	fail_if(ibh == NULL);

	// create 'max_object' in the bucket
	for (size_t i = 0 ; i < max_objects; ++i) {
		snprintf(obj_name, sizeof(obj_name)-1, "%s-%zu", obj_name_prefix,
		    i);
		struct iobject_handle *oh = iobj_object_create(ibh, obj_name,
		    0, 0600, NULL, OSTORE_ACL_OBJECT_FILE, &error);
		fail_if(error);
		iobj_object_commit(oh, &error);
		fail_if(error);
		iobj_object_close(oh, &error);
		fail_if(error);
	}

	struct iobj_list_obj_param param = {0};
	param.ibh = ibh;
	param.caller_cb = test_list_cb;
	param.stats_requested = false;
	param.caller_context = NULL;

	// verify max objects created
	count = iobj_object_list(&param, &error);
	fail_if(error || (count != max_objects));

	// launch first lister and then delete threads
	cld_arg.ibh = ibh;
	cld_arg.max_objects = max_objects;
	cld_arg.obj_name_prefix = obj_name_prefix;
	cld_arg.stop = false;
	ret = pthread_create(&lister, 0, thr_list_routine, &cld_arg);
	fail_if(ret != 0);
	sleep(1); 
	ret = pthread_create(&deleter, 0, thr_delete_routine, &cld_arg);
	fail_if(ret != 0);

	// wait for deleter completion
	ret = pthread_join(deleter, 0);
	fail_if(ret != 0);

	// now signal lister to stop
	cld_arg.stop = true;
	ret = pthread_join(lister, 0);
	fail_if(ret != 0);
}

