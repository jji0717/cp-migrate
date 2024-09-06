#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <check.h>

#include <ifs/ifs_types.h>

#include <isi_util/isi_error.h>

#include <isi_object/ostore_internal.h>
#include <isi_object/common.h>
#include <isi_object/check_utils.h>

TEST_FIXTURE(test_common_teardown);

SUITE_DEFINE_FOR_FILE(check_common, .test_teardown = test_common_teardown);

#define CREATE_MODE (S_IRWXU|S_IRGRP|S_IXGRP)

#define BASE_PATH   "/ifs/__unittest_obj"
#define ACL_BASE_PATH "/ifs/.object/__unittest_common"

/**
 * Common teardown routine for testing object. 
 * Close the store g_STORE_HANDLE.
 */

TEST_FIXTURE(test_common_teardown)
{	
	remove(BASE_PATH);
}


/*** TESTS for mkdir_dash_p() ***/
static void
reset_base(char *base_path, bool create)
{
	int rv;

	/* start fresh */
	test_util_rm_recursive(base_path);

	if (create) {
		/* if we're meant to, make base */
		rv = mkdir(base_path, 0777);
		fail_if(rv < 0, "mkdir got %d for %s", errno, base_path);
	}
}

/** count the number of distinct paths that finally exist under @a path */
static void
verify_path_count(char *path, int expect_elements, int expect_finals)
{
	int rv;

	/* ugly but simple */

/* These two commands are based on the following two types of output:

   -- total element count (we add the /tmp/__unittest_obj for them):
    # find /tmp/__unittest_obj
    /tmp/__unittest_obj
    /tmp/__unittest_obj/foo
    /tmp/__unittest_obj/foo/bar

   -- final elements only:
    # find /tmp/__unittest_obj -links 2
    /tmp/__unittest_obj/foo/bar
*/
	if (expect_elements >= 0) {
		struct fmt FMT_INIT_CLEAN(cmd);
		/* always expect base dir */
		expect_elements++;

		fmt_print(&cmd, "find %s | wc -l | xargs test %d -eq", 
		    path, expect_elements);
		rv = system(fmt_string(&cmd));

		fail_unless(WIFEXITED(rv) && (WEXITSTATUS(rv) == 0), 
		    "system(%s) expecting %d elements returned 0x%x", 
		    fmt_string(&cmd), expect_elements, rv);
	}

	if (expect_finals >= 0) {
		struct fmt FMT_INIT_CLEAN(cmd);
		fmt_print(&cmd, "find %s -links 2 | wc -l | xargs test %d -eq",
		    path, expect_finals);
		rv = system(fmt_string(&cmd));

		fail_unless(WIFEXITED(rv) && (WEXITSTATUS(rv) == 0), 
		    "system(%s) expecting %d finals returned 0x%x", 
		    fmt_string(&cmd), expect_finals, rv);
	}
}

static void
add_element(char *base_path, char *add_path, bool expect_success)
{
	struct fmt FMT_INIT_CLEAN(tpath);
	struct isi_error *error = 0;
	struct persona *owner = NULL;
	int fd = 0;

	fmt_print(&tpath, "%s", base_path);

	mkdir_dash_p(&tpath, add_path, CREATE_MODE, NULL,
	    OSTORE_ACL_NONE, OSTORE_ACL_NONE, &fd, owner, &error);

	if (expect_success)
		fail_if(error, "Add got error: %#{}", isi_error_fmt(error));
	else {
		fail_if(!error, "Expected error but got success");
		isi_error_free(error);
	}
}


/* A null base-path should work.  Try making an already-existing root dir. */
TEST(test_mkdir_root)
{
	char *base_path = "";
	char *add_path = "/tmp";
	add_element(base_path, add_path, true);
}

/* A null base-path should work.  Try making a new dir in root */
TEST(test_mkdir_root_plus)
{
	char *base_path = BASE_PATH;

	reset_base(base_path, false);
	add_element("", base_path, true); // unusual args
}

/* we should fail if the base path isn't there, adding something */
TEST(test_mkdir_n_p)
{
	char *base_path = BASE_PATH;
	char *add_path = "foo/bar";

	reset_base(base_path, false);
	add_element(base_path, add_path, false);
}

/* we should succeed if the base path isn't there, if we don't add anything */
TEST(test_mkdir_n_n)
{
	char *base_path = BASE_PATH;
	char *add_path = "";

	reset_base(base_path, false);
	add_element(base_path, add_path, true);
}

/* we should succeed if the base path is there, adding nothing */
TEST(test_mkdir_p_n)
{
	char *base_path = BASE_PATH;
	char *add_path = "";

	reset_base(base_path, true);
	add_element(base_path, add_path, true);
	verify_path_count(base_path, 0, -1);
}

/* we should succeed if the base path is there and the sub-path isn't  */
TEST(test_mkdir_p_p)
{
	char *base_path = BASE_PATH;
	char *add_path = "foo/bar";

	reset_base(base_path, true);
	add_element(base_path, add_path, true);
	verify_path_count(base_path, 2, 1);
}

/* make sure it works with final slash on the name */
TEST_ANON()
{
	char *base_path = BASE_PATH;
	char *add_path = "foo/bar/";

	reset_base(base_path, true);
	add_element(base_path, add_path, true);
	verify_path_count(base_path, 2, 1);
}


/** add two in a row, always expecting success */
static void
make_and_add2(char *base_path, char *add1_path, char *add2_path, 
    int expect_elements, int expect_finals)
{
	reset_base(base_path, true);
	add_element(base_path, add1_path, true);
	add_element(base_path, add2_path, true);
	verify_path_count(base_path, expect_elements, expect_finals);
}

/* we should succeed if the base path is there and the sub-path is all there */
TEST_ANON()
{
	make_and_add2(BASE_PATH, "foo/bar", "foo/bar", 2, 1);
}

/* we should succeed if the base path is there and sub-path is partly there */
TEST_ANON()
{
	make_and_add2(BASE_PATH, "foo", "foo/bar/baz", 3, 1);
}

/* make sure there's no surprise for similar names */
TEST_ANON()
{
	make_and_add2(BASE_PATH, "foo", "foo/", 1, 1);
}

TEST_ANON()
{
	make_and_add2(BASE_PATH, "foo", "foo2", 2, 2);
}

TEST_ANON()
{
	make_and_add2(BASE_PATH, "foo2", "foo", 2, 2);
}


TEST(test_set_acl_dir)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(dpath);
	char *base_path = ACL_BASE_PATH;
	const char *path = NULL;
	struct persona *owner = NULL;

	int rv = -1;
	
	reset_base(base_path, true);
	fmt_print(&dpath, "%s/%s", base_path, "acl_dir");
	path = fmt_string(&dpath);
	
	rv = mkdir(path, 0777);
	fail_if((rv < 0), "Couldn't create acl test dir, error %s", 
	    strerror(errno));

	ostore_set_default_acl(path, -1, OSTORE_ACL_STORE, owner, &error);
	fail_if(error, "Unexpected error %s adding acl", isi_error_fmt(error));
		
	fail_if(!check_iobj_acl(path, OSTORE_ACL_STORE), "SDs do not compare");

	rmdir(path);
	rmdir(base_path);
}
