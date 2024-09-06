#include <check.h>

#include <ifs/ifs_types.h>

#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>

#include <isi_object/check_utils.h>
#include <isi_object/iobj_ostore_protocol.h>

#include <stdio.h>

SUITE_DEFINE_FOR_FILE(check_ns_bucket, .timeout = 40);

#define DEFAULT_NSA	"ifs"

/**
 * test open a bucket
 */
TEST(test_open_bucket)
{
	struct isi_error *error = NULL;
	// open bucket:
	struct ibucket_handle *ibh = 0;
	
	struct ostore_handle *ios = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);
	
	fail_if(error, "received error %#{} when opening store %s", 
	    isi_error_fmt(error), DEFAULT_NSA);	
	
	// open the root container
	ibh = test_util_open_bucket(false, OSTORE_ACL_NONE,
	    (enum ifs_ace_rights) IFS_RTS_DIR_GEN_READ,
	    ios, "TEST_ACCT", "/");

	test_util_close_bucket(ibh);

	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} when closing store",
	    isi_error_fmt(error), DEFAULT_NSA);
}

/*
 * test creating a bucket
 * a. long name
 * b. with attrs and mode
 */
TEST(test_create_bucket)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ibh = 0;
	/*
	 * open store
	 */
	struct ostore_handle *ios = iobj_ostore_open(OST_NAMESPACE,
	    DEFAULT_NSA, HEAD_SNAPID, &error);

	fail_if(error, "received error %#{} when opening store %s",
	    isi_error_fmt(error), DEFAULT_NSA);

	struct ibucket_name *ibn = NULL;
	/*
	 * test bucket creation with long name -- 255 chars
	 */
	const char *long_name = "1111111111222222222233333333334444444444"
	    "555555555566666666667777777777888888888899999999990000000000"
	    "111111111122222222223333333333444444444455555555556666666666"
	    "777777777788888888889999999999000000000011111111112222222222"
	    "33333333334444444444555555555566666";

	ibn = iobj_ibucket_from_acct_bucket(ios, "TEST_ACCT",
	    long_name, &error);
	fail_if(error,
	    "received error %#{} formatting ibn from acct an bucket",
	    isi_error_fmt(error));

	ibh = iobj_ibucket_create(ibn, 0, NULL, NULL, OSTORE_ACL_NONE,
	    false, true, &error);
	fail_if(error, "received error %#{} creating bucket",
		isi_error_fmt(error));

	iobj_ibucket_name_free(ibn);
	ibn = NULL;
	test_util_genobj_delete(&(ibh->base));
	test_util_close_bucket(ibh);
	ibh = NULL;

	/*
	 * test recursive for deep dir create, the path partially shared with
	 * next test purposely to ensure that some level of parent path exists.
	 * so don't alter the shared portion.
	 */
	const char *recu_path_shared = "__unit_test/a/b/c/d/e/f/g/h/i/j/k";
	char recu_name[2048];
//	int nloop = 40; // file recu_name with about 40* 40 = 1600 (chars)
	/* 
	 * ifs_createfile supports only 1024 characters for path length.
	 * So this test along with the next test where path /1/2/.... 
	 * appended, together form 1024 characters.
	 * So reduced the path length from 40 * 40 to 24 * 40.
	 * So when the path in the next test /1/2/.... is appended, the 
	 * tests still pass.
	 */
	int nloop = 24;
	error = NULL;
	strcpy(recu_name, recu_path_shared);
	while (nloop-- > 0) 
		strcat(recu_name, "/100/100/100/100/100/100/100/100/100/100");
	//printf("MAX_PATH %d", PATH_MAX); // 1024
	//apache2 build seems to have PATH_MAX=1024  
	//however, linux internal PATH_MAX is 4096
	//ASSERT(strlen(recu_name) > PATH_MAX);
	ibn = iobj_ibucket_from_acct_bucket(ios, "TEST_ACCT",
	    recu_name, &error);
	ibh = iobj_ibucket_create(ibn, 0, NULL, NULL, OSTORE_ACL_NONE,
	    true, true, &error);
	fail_if(error, "received error %#{} creating bucket",
	    isi_error_fmt(error));
	// test deletion of long path, sorry doesn't work for now
	// test_util_genobj_delete(&(ibh->base));
	iobj_ibucket_name_free(ibn);
	ibn = NULL;

	struct ibucket_handle *ibh2;
	error = NULL;
	//strcpy(recu_name, recu_path_shared);
	/*
	 * ifs syscall, ifs_createfile supports only 1024 characters
	 * for the path length.
	 */
	strcat(recu_name, "/1/2/3/4/5/6/7/8/9/10/111");
	ibn = iobj_ibucket_from_acct_bucket(ios, "TEST_ACCT",
	    recu_name, &error);
	ibh2 = iobj_ibucket_create(ibn, 0, NULL, NULL, OSTORE_ACL_NONE,
	    true, true, &error);
	fail_if(error, "received error %#{} creating bucket",
	    isi_error_fmt(error));
	iobj_ibucket_name_free(ibn);
	ibn = NULL;

	//test_util_genobj_delete(&(ibh->base));
	test_util_close_bucket(ibh);
	//test_util_genobj_delete(&(ibh2->base));
	test_util_close_bucket(ibh2);
	ibh = NULL;
	delete_test_folder("/ifs/__unit_test/");

	/*
	 * test bucket creation with attr and mode
	 */
	error = NULL;
	ibn = iobj_ibucket_from_acct_bucket(ios, "TEST_ACCT",
	    "__new_bucket", &error);
	fail_if(error,
	    "received error %#{} formatting ibn from acct an bucket",
	    isi_error_fmt(error));

	struct iobj_object_attrs *attrs = genobj_attr_list_create(2);
	genobj_attr_list_append(attrs, "key1", "val1", strlen("val1"), &error);
	genobj_attr_list_append(attrs, "key2", "val2", strlen("val2"), &error);

	mode_t oldmode = umask(022);
	mode_t cmode = 0740; // assuming process mask = 022
	ibh = iobj_ibucket_create(ibn, cmode, attrs, NULL, OSTORE_ACL_NONE,
	    false, true, &error);
	fail_if(error, "received error %#{} creating bucket",
		isi_error_fmt(error));

	struct stat st = {0};
	stat(ibh->base.path, &st);
	fail_if((st.st_mode & 0777) != cmode, "expect file permission mode %o"
	    " instead of %o", cmode, st.st_mode & 0777);

	umask(oldmode);
	/*
	 * Clean up so we don't leak any memory.
	 */

	genobj_attribute_list_release(attrs, &error);

	iobj_ibucket_name_free(ibn);

	test_util_genobj_delete(&(ibh->base));
	test_util_close_bucket(ibh);

	/*
	 * close store
	 */
	iobj_ostore_close(ios, &error);
	fail_if(error, "received error %#{} when closing store",
	    isi_error_fmt(error), DEFAULT_NSA);
}
