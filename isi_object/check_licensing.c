#include <check.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <isi_object/ostore_internal.h>

#include "check_utils.h"
#include "iobj_licensing.h"
#include "iobj_ostore_protocol.h"

TEST_FIXTURE(licensing_setup);
TEST_FIXTURE(licensing_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

struct ostore_handle *g_store_handle = 0;

#define TEST_STORE "__unittest-bucket"
/*
 * A test license key for the ObjectIQ module, valid for 2 days until
 * Wed Feb 11 19:00:00 2015
*/
#define TEST_LICENSE_CUST_KEY "ISILO-NK4PA-FYSEO-UUTGO-OJNGL"
// An invalid license key
#define ISI_LICENSING_INVALID_ID "Not_ObjectIQ"


SUITE_DEFINE_FOR_FILE(check_licensing,
			.suite_setup = licensing_setup,
			.suite_teardown = licensing_teardown,
			.test_setup = test_setup,
			.test_teardown = test_teardown,
			.mem_check = CK_MEM_DISABLE); 	// Issues with dmalloc
							// pthread and
							// licensing usage);

TEST_FIXTURE(licensing_setup)
{
	// taken from licensing isi_licensing/check_licensing.c
	//
	// TODO: THIS CODE HAS RACE ISSUES, PLEASE UPDATE FROM
	// isi_licensing/check_licensing.c
	
	/* Keep MCP from interfering with our backup of licensing.xml */
	system("/usr/bin/killall isi_mcp");
	/* Make sure it's really dead */
	system("/usr/bin/killall isi_mcp");
	system("/usr/bin/killall isi_mcp");
	system("/usr/bin/killall isi_mcp");
	system("/usr/bin/killall isi_mcp");

	/* Save the license file if possible */
	rename("/etc/mcp/override/licensing.xml",
	    "/etc/mcp/override/licensing.xml.save");
}

TEST_FIXTURE(licensing_teardown)
{
	/* Move the saved license file back */
	rename("/etc/mcp/override/licensing.xml.save",
	    "/etc/mcp/override/licensing.xml");

	/* Restart MCP */
	system("/usr/sbin/isi_mcp start");
}

TEST_FIXTURE(test_setup)
{
	struct isi_error *error = NULL;

	test_util_remove_store(TEST_STORE);
	g_store_handle = iobj_ostore_create(OST_OBJECT, TEST_STORE, NULL, &error);
	fail_if(error);
}

TEST_FIXTURE(test_teardown)
{
	struct isi_error *error = NULL;

	iobj_ostore_close(g_store_handle, &error);
	fail_if(error);
	test_util_remove_store(TEST_STORE);
}

static int
reset_license_file(void)
{
	return unlink("/etc/mcp/override/licensing.xml");
}

TEST(license_valid_to_invalid)
{
	struct isi_error *error = NULL;
	struct ibucket_name *ib1 = NULL, *ib2 = NULL;
	struct ibucket_handle *ibh1 = NULL, *ibh2 = NULL;
	struct iobject_handle *oh1 = NULL, *oh2 = NULL;
	char obj_data[] = "Some Object data";
	ssize_t res;
	enum isi_licensing_error lic_result;
	
	reset_license_file();


	// set a valid license key
	lic_result = isi_licensing_add_key(TEST_LICENSE_CUST_KEY);
	fail_if(lic_result != ISI_LICENSING_ERR_SUCCESS,
	    "received error %d setting license key %s", lic_result,
	    TEST_LICENSE_CUST_KEY);    
	
	// set a license type
	iobj_set_license_ID(ISI_LICENSING_OBJECT);
	
	// verify that bucket and objects can be created
	//  and objects written to
	ib1 = iobj_ibucket_from_acct_bucket(g_store_handle, "bucket1Acct", 
	    "bucket1Name", &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));
	ibh1 = iobj_ibucket_create(ib1, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));
	oh1 = iobj_object_create(ibh1, "object1", 0, 0600, NULL, 
	    OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
	    isi_error_fmt(error));
	res = iobj_object_write(oh1, 0, obj_data, sizeof(obj_data), &error);
	fail_if(error, "received error %#{} writing object", 
	    isi_error_fmt(error));

	// set a invalid license type
	iobj_set_license_ID(ISI_LICENSING_INVALID_ID);

	// verify that new buckets and objects (against old buckets) cannot be
	// created and existing objects written to
	ib2 = iobj_ibucket_from_acct_bucket(g_store_handle, "bucket1Acct",
	    "bucket2Name", &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));
	ibh2 = iobj_ibucket_create(ib2, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error==NULL);
	isi_error_free(error);
	error = NULL;
	oh2 = iobj_object_create(ibh1, "object2", 0, 0600, NULL, 
	    OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error==NULL);
	isi_error_free(error);
	error = NULL;
	res = iobj_object_write(oh1, 0, obj_data, sizeof(obj_data), &error);
	fail_if(error==NULL);
	isi_error_free(error);
	error = NULL;

	// cleanup
	iobj_object_close(oh1, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
	iobj_ibucket_close(ibh1, &error);
	fail_if(error, "received error %#{} close bucket", 
		isi_error_fmt(error));
	iobj_ibucket_name_free(ib1);
	iobj_ibucket_name_free(ib2);

}

TEST(license_invalid_to_valid)
{
	struct isi_error *error = NULL;
	struct ibucket_name *ib1 = NULL, *ib2 = NULL;
	struct ibucket_handle *ibh1 = NULL, *ibh2 = NULL;
	struct iobject_handle *oh1 = NULL;
	char obj_data[] = "Some Object data";
	ssize_t res;
	enum isi_licensing_error lic_result;
	
	reset_license_file();

	// set a invalid license type
	iobj_set_license_ID(ISI_LICENSING_INVALID_ID);

	// verify that new buckets  cannot be created
	ib2 = iobj_ibucket_from_acct_bucket(g_store_handle, "bucket1Acct",
	    "bucket2Name", &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));
	ibh2 = iobj_ibucket_create(ib2, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error==NULL);
	isi_error_free(error);
	error = NULL;

	// set a valid license key
	lic_result = isi_licensing_add_key(TEST_LICENSE_CUST_KEY);
	fail_if(lic_result != ISI_LICENSING_ERR_SUCCESS,
	    "received error %d setting license key %s", lic_result,
	    TEST_LICENSE_CUST_KEY);

	// set a valid license type
	iobj_set_license_ID(ISI_LICENSING_OBJECT);

	// verify that bucket and objects can be created
	//  and objects written to
	ib1 = iobj_ibucket_from_acct_bucket(g_store_handle, "bucket1Acct", 
	    "bucket1Name", &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));
	ibh1 = iobj_ibucket_create(ib1, 0600, NULL, OSTORE_ACL_BUCKET, 
	    false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));
	oh1 = iobj_object_create(ibh1, "object1", 0, 0600, NULL, 
	    OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
	    isi_error_fmt(error));
	res = iobj_object_write(oh1, 0, obj_data, sizeof(obj_data), &error);
	fail_if(error, "received error %#{} writing object", 
	    isi_error_fmt(error));

	// cleanup
	iobj_object_close(oh1, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
	iobj_ibucket_close(ibh1, &error);
	fail_if(error, "received error %#{} close bucket", 
		isi_error_fmt(error));
	iobj_ibucket_name_free(ib1);
	iobj_ibucket_name_free(ib2);
}

