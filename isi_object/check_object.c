#include <check.h>
#include <errno.h>
#include <stdio.h>
#include <sys/extattr.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_userattr.h>
#include <isi_util/isi_error.h>
#include <isi_object/ostore_internal.h>
#include <isi_object/iobj_ostore_protocol.h>

#include "check_utils.h"
#include "iobj_licensing.h"

TEST_FIXTURE(store_setup);
TEST_FIXTURE(store_teardown);
TEST_FIXTURE(test_object_setup);
TEST_FIXTURE(test_object_teardown);

SUITE_DEFINE_FOR_FILE(check_object, .suite_setup = store_setup,
    .suite_teardown = store_teardown,
    .test_setup = test_object_setup, .test_teardown = test_object_teardown);

#define DEFAULT_MODE		0600
#define TEST_STORE		"__unittest-object"
#define BUCKET_ACCT 		"unittest" 
#define BUCKET_NAME 		"object_bucket_name"

#define NAME_COLLISION_STORE	"_Name_Collide_Store_"

#define ACCESS_CHECK_DIR_NAME "__access_check__"

#define USING_EUID

#define ACCESS_CHECK_UID 10
#define ACCESS_CHECK_ROOT_UID 0
#ifdef USING_EUID
#define ACCESS_CHECK_BASE_UID 1001
#else
#define ACCESS_CHECK_BASE_UID ACCESS_CHECK_ROOT_UID
#endif

static struct ostore_handle *g_STORE_HANDLE = 0;
static struct ibucket_handle *g_BUCKET_HANDLE = 0;

/*
 * Use this to check subdirs by setting before a test and comparing at the end
 */
int start_tmp_subdirs = 0;
int object_current_uid = ACCESS_CHECK_ROOT_UID;

static void
reset_euid(int uid)
{
	int rv = -1;

	if (uid == object_current_uid)
		return;

	if (object_current_uid != ACCESS_CHECK_ROOT_UID) {
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

	object_current_uid = uid;
}

/* start with the store empty */
TEST_FIXTURE(store_setup)
{
	test_util_remove_store(TEST_STORE);
	test_util_remove_store(NAME_COLLISION_STORE);
	
	iobj_set_license_ID(IOBJ_LICENSING_UNLICENSED);

}

/* at the  end, let's leave the store empty */
TEST_FIXTURE(store_teardown)
{ 
	test_util_remove_store(TEST_STORE);
	test_util_remove_store(NAME_COLLISION_STORE);
}

/**
 * Common setup routine for testing object. 
 * Create the store and set it to g_STORE_HANDLE
 */

TEST_FIXTURE(test_object_setup)
{	
	struct isi_error *error = NULL;

	g_STORE_HANDLE = iobj_ostore_create(OST_OBJECT, TEST_STORE, NULL, &error);

	fail_if(error, "received error %#{} creating store", 
		isi_error_fmt(error));

	start_tmp_subdirs = get_number_dirents(OBJECT_STORE_TMP);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_BASE_UID);
#endif

	// The access rights (BUCKET_WRITE) is not used when
	// bucket creation is done. 
	g_BUCKET_HANDLE = test_util_open_bucket(true, 
	    OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_WRITE,
	    g_STORE_HANDLE, BUCKET_ACCT, BUCKET_NAME);	

	fail_if(!g_BUCKET_HANDLE, "Did not create the bucket using test_util_open_bucket");

}

/**
 * Common teardown routine for testing object. 
 * Close the store g_STORE_HANDLE.
 */

TEST_FIXTURE(test_object_teardown)
{	
	struct isi_error *error = NULL;
	int end_tmp_subdirs = 0;

	test_util_close_bucket(g_BUCKET_HANDLE);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_ROOT_UID);
#endif

	end_tmp_subdirs = get_number_dirents(OBJECT_STORE_TMP);

	fail_if(start_tmp_subdirs - end_tmp_subdirs, 
	    "Unexpected tmp subdirs found");
		

	iobj_ostore_close(g_STORE_HANDLE, &error);

	fail_if(error, "received error %#{} close store", 
		isi_error_fmt(error));
	
}

/*
 * Set the parameters for name collision so that name splitting will
 * happen at 8 characters, taking the first 2 and last 2 characters
 * for the filename (with the default string ".." inserted between),
 * and hashing everyhing to the hash directory 8/8
 */
static void 
disable_name_hashing(struct ostore_handle *ios, int threshold, 
    int left, int right, int hash_value, char *translate, char *insert, int max)
{
	struct isi_error *error = NULL;
	struct ostore_parameters *params = NULL;

	iobj_ostore_parameters_get(ios, &params, &error);
	fail_if(error, "received error %#{} getting initial store parameters",
	    isi_error_fmt(error));

	if (threshold != -1)
		params->hash_name_split_length = threshold;
	if (left != -1)
		params->hash_name_split_left = left;
	if (right != -1)
		params->hash_name_split_right = right;
	if (hash_value != -1)
		params->hash_name_hashto = hash_value;
	if (translate) {
		if(params->hash_name_translate)
			free(params->hash_name_translate);
		params->hash_name_translate = strdup(translate);
	}
	if (insert) {
		if (params->hash_name_split_insert)
			free(params->hash_name_split_insert);
		params->hash_name_split_insert = strdup(insert);

	}
	if (max != -1)
		params->hash_name_max_collisions = max;

	iobj_ostore_parameters_set(ios, params, &error);
	fail_if(error, "received error %#{} setting store parameters for "
	    "name collision", isi_error_fmt(error));

	iobj_ostore_parameters_release(ios, params);
}

/**
 * test creating a new object
 */
TEST(test_object_create)
{

	struct isi_error *error = NULL;
		
	char *obj_name = "unitest_object_name";

	fail_if(!g_BUCKET_HANDLE, "Bucket not opened");
	
	/*
	  TBD when ostore acl is implemented. The ostore_acl_mode
	   passed should be checked in all places (check_buckets.c too).
	   The parameter passed for ostore_acl_mode is dependent on 
	   the type of headers that are used to set the acl during the 
	   object creation. 
	*/
	struct iobject_handle *oh = iobj_object_create(g_BUCKET_HANDLE,
	    obj_name, 0, DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	// clean up to catch memory leaks:	
	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
		
}

struct t_rwfiles {
	char *spath;
	char *sname;
	char *fullname;
};

struct t_rwfiles rw_files[] = {
	{"/boot", "efs.ko", NULL},
	{"/etc", "fstab", NULL}
};

struct overwrite {
	int new;
	int old;
} overwrite_list = {2,0};

static int 
find_input_files(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(find);
	FILE *f = NULL;
	char str[MAXPATHLEN];
	char *ptr = NULL;
	int i;
	int inf_count = sizeof rw_files / sizeof rw_files[0];

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_ROOT_UID);
#endif

	for (i = 0; i < inf_count; i++) {
		fmt_print(&find, "find %s -name %s", 
		    rw_files[i].spath, rw_files[i].sname);

		f = popen(fmt_string(&find), "r");
		fail_if(!f, "Could not open pipe for %s command", 
		    fmt_string(&find));

		ptr = fgets(str, sizeof(str), f);
		fail_if(!ptr, "Could not find file for \"%s\" command",
		    fmt_string(&find));

		str[strlen(str)-1] = '\0';

		rw_files[i].fullname = strdup(str);
		
		if (f) 
			pclose(f);
		fmt_truncate(&find);
	}

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_BASE_UID);
#endif

	if (error)
		isi_error_handle(error, error_out);
	return inf_count;
}

static void
cleanup_found_files(int input_count)
{
	int i;

	// clean up the memory allocated when finding the file...
	for (i = 0; i < input_count; i++)
		if (rw_files[i].fullname)
			free(rw_files[i].fullname);
}

static void
do_object_write(struct iobject_handle *iobj, char *filename, size_t wsize,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	off_t offset = 0;
	ssize_t bytes_read = 0;
	ssize_t bytes_written = 0;
	ssize_t left_to_write = 0;
	int buf_off = 0;

	char *buffer = NULL;
       
	bool status = false;
	int fd = -1;

	if ((fd = open(filename, O_RDONLY, 0)) == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open input file %s for writing to object %s",
		    filename, iobj->base.name);
		goto out;
	}
		    
	buffer = calloc(1, wsize);
	if (!buffer) {
		error = isi_system_error_new(errno,
		    "Cannot allocate buffer for data transfer");
		goto out;
	}

	while(true) {
		bytes_read = read(fd, buffer, wsize);
		if (bytes_read  == -1) {
			error = isi_system_error_new(errno, 
			    "Cannot read input file %s for object %s", 
			    filename, iobj->base.name);
			goto out;
		}

		if (bytes_read == 0)
			break;

		left_to_write = bytes_read;
		buf_off = 0;
		while (left_to_write > 0) {
			bytes_written = iobj_object_write(iobj, /* object */
			    offset, /* where to start data in the object */
			    &buffer[buf_off], /* how we get the data there */
			    left_to_write, /* how much we are doing this time */
			    &error);
			if (error)
				goto out;
			if (bytes_written == -1) {
				error = isi_system_error_new(errno, 
				    "write failed to object %s in at offset %lld",
				    iobj->base.name, offset);
				goto out;
			}
			left_to_write -= bytes_written;
			buf_off += bytes_written;
			offset += bytes_written;
		}
	}
	status = true;

 out:
	/* clean up first */
	if (buffer) {
		free(buffer);
		buffer = NULL;
	}
	if (fd != -1) {
		close(fd);
		fd = -1;
	}
	/* report any errors */
	if (error) {
		isi_error_handle(error, error_out);
	}
}

static size_t
do_object_compare(struct iobject_handle *iobj, char *filename, size_t rd_size, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	off_t offset = 0;
	ssize_t bytes_read = 0;
	ssize_t bytes_object = 0;
	ssize_t left_to_read = 0;
	int buf_off = 0;
	off_t read_off = 0;

	struct stat f_sb;
	struct stat o_sb;

	char *o_buffer = NULL;
	char *f_buffer = NULL;
       
	size_t compare = -1;
	int fd = -1;

	if ((fd = open(filename, O_RDONLY, 0)) == -1) {
		error = isi_system_error_new(errno, 
		    "Could not open target file %s for reading "
		    "from object %s",
		    filename, iobj->base.name);
		goto out;
	}
		    
	/*
	 * compare the size of the files first.  When there is an object api to 
	 * get the size use that, for now cheat and access directly using fd
	 * in object handle.
	 */
	if (fstat(fd, &f_sb) == -1) {
		error = ostore_error_new(
			"Failed get stats on source file %s for object %s",
			filename, iobj->base.name);
		goto out;
	}
	
	if (fstat(iobj->base.fd, &o_sb) == -1) {
		 error = ostore_error_new(
			 "Failed get stats on object file %s for object %s",
			 filename, iobj->base.name);
		goto out;
	}

	if (f_sb.st_size != o_sb.st_size) {
		error = ostore_error_new(
			"Size of object %s, %lld, does not match size of "
			"source file %s, %lld",
			iobj->base.name, o_sb.st_size, filename, f_sb.st_size);
		compare = -1;
		goto out;
	}

	o_buffer = calloc(1, rd_size);
	if (!o_buffer) {
		error = isi_system_error_new(errno,
		    "Cannot allocate buffer for data transfer");
		goto out;
	}
	f_buffer = calloc(2, rd_size);
	if (!f_buffer) {
		error = isi_system_error_new(errno,
		    "Cannot allocate buffer for data transfer");
		goto out;
	}

	read_off = 0;

	while(true) {
		bytes_read = read(fd, f_buffer, rd_size);
		if (bytes_read  == -1) {
			error = isi_system_error_new(errno, 
			    "Cannot read input file %s for object %s", 
			    filename, iobj->base.name);
			goto out;
		}

		if (bytes_read == 0)
			break;

		left_to_read = bytes_read;
		buf_off = 0;
		while (left_to_read > 0) {
			bytes_object = iobj_object_read(iobj, read_off, 
			    &o_buffer[buf_off], left_to_read, &error);

			if (bytes_object == -1) {
				error = isi_system_error_new(errno, 
				    "object read failed for offset %lld, size %zu, "
				    "object %s",
				    read_off, left_to_read, iobj->base.name);
				goto out;
			}
			
			left_to_read -= bytes_object;
			buf_off += bytes_object;
			offset += bytes_object;
		}
		read_off += bytes_read;
		if ((compare = memcmp(f_buffer, o_buffer, bytes_read)) != 0)
			goto out;
	}
	
 out:
	/* clean up first */

	if (o_buffer) {
		free(o_buffer);
		o_buffer = NULL;
	}

	if (f_buffer) {
		free(f_buffer);
		f_buffer = NULL;
	}

	if (fd != -1)
		close(fd);
	
	if (error)
		isi_error_handle(error, error_out);

	return compare;
}

TEST(test_object_write_read_compare)
{
	struct isi_error *error = NULL;
	int input_count = 0;
	int i;
	int obj_i; /* this is the index for the object name for all cases 
		    * and the index for the filename when comparing object to
		    * file before the commit of the overwriting object */

	size_t r = 0;
	size_t wr_size = 32768;
	size_t rd_size = 16384;

	int flags = 0;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;

	struct iobject_handle *iobj, *iobj2;

	char **obj_names = NULL;
		
	struct fmt FMT_INIT_CLEAN(obj_name);

	input_count = find_input_files(&error);

	obj_names = calloc(input_count, sizeof (char *));

	for (i = 0; i < input_count; i++) {

		/*
		 * obj_i should always be used as the index for obj_names but
		 * should be used for the index for rwfiles only for the case
		 * of comparing the file prior to commit (when obj_i != i).
		 * All other times, i should be the index for rw_files.
		 */
		if (overwrite_list.new == i) {
			obj_i = overwrite_list.old;
			fail_if(!obj_names[obj_i],
			    "\n\nAttemped to reuse an object name that has yet"
			    "\nto be defined for write_read_compare, check to "
			    "\nsee that overwrite_list has new > old\n");
		} else {
			obj_i = i;
			fmt_print(&obj_name, 
			    "unit_test_object#read_write_compare_%s_%s", 
			    rw_files[i].spath, rw_files[i].sname);
			obj_names[obj_i] = fmt_detach(&obj_name);
		}

		iobj = iobj_object_create(g_BUCKET_HANDLE, obj_names[obj_i], 
		    0, DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
		fail_if(error, "Received error %{} creating object %s",
		    isi_error_fmt(error), obj_names[obj_i]);

		do_object_write(iobj, rw_files[i].fullname, wr_size, 
		    &error);
		fail_if(error, 
		    "Received error %#{} writing object %s from file %s",
		    isi_error_fmt(error), obj_names[obj_i], 
		    rw_files[i].fullname);

		/*
		 * If this is the chosen overwrite case, check to see that an
		 * open and read of the object returns the old object info
		 * up until the new copy of the object is committed.
		 */
		if (obj_i != i)	{

			iobj2 = iobj_object_open(g_BUCKET_HANDLE, 
			    obj_names[obj_i], 
			    (enum ifs_ace_rights) OBJECT_READ,
			    flags, cf_flags, &error);
			fail_if(error, "received error %#{} opening object", 
			    isi_error_fmt(error));

			r = do_object_compare(iobj2, rw_files[obj_i].fullname, 
			    rd_size, &error);

			fail_if(error, 
			    "Received error %#{} comparing object %s with "
			    "file %s for rewrite before commit case", 
			    isi_error_fmt(error), obj_names[obj_i], 
			    rw_files[obj_i].fullname);
			fail_if(r, "Failed comparison of object %s to file %s "
			    "for requested read size of %d for rewrite before "
			    "commit case", obj_names[obj_i], 
			    rw_files[obj_i].fullname, rd_size);

			iobj_object_close(iobj2, &error);
			fail_if(error, "Received error %#{} on object close",
			    isi_error_fmt(error));
		}

		/*
		 * commit object so that following open can find it
		 */
		iobj_object_commit(iobj, &error);
		fail_if(error,
		    "Received error %#{} committing object %s",
		    isi_error_fmt(error), obj_names[obj_i]);

		iobj_object_close(iobj, &error);
		fail_if(error, "Received error %#{} on object close",
		    isi_error_fmt(error));

		iobj = iobj_object_open(g_BUCKET_HANDLE, obj_names[obj_i], 
		    (enum ifs_ace_rights) OBJECT_READ, flags, cf_flags,
		    &error);
		fail_if(error, "received error %#{} opening object", 
		    isi_error_fmt(error));

		r = do_object_compare(iobj, rw_files[i].fullname, rd_size, 
		    &error);
		fail_if(error, 
		    "Received error %#{} comparing object %s with file %s",
		    isi_error_fmt(error), obj_names[obj_i], 
		    rw_files[i].fullname);
		fail_if(r, "Failed comparison of object %s to file %s "
		    "for requested read size of %d", 
		    obj_names[obj_i], rw_files[i].fullname, rd_size);

		iobj_object_close(iobj, &error);
		fail_if(error, "Received error %#{} on object close",
		    isi_error_fmt(error));

	}
	cleanup_found_files(input_count);

	for (i = 0; i < input_count; i++) {
		if (obj_names[i] ) {
			error = iobj_object_delete(g_BUCKET_HANDLE, 
			    obj_names[i]);
			fail_if(error, "Received error %#{} on object close",
			    isi_error_fmt(error));

			free(obj_names[i]);
		}
	}

	free(obj_names);
}

struct list_object_tester {
	size_t count;
	char ** obj_names;
	size_t num_names;
}; 

static bool
test_list_cb_all(void *this, const struct iobj_list_obj_info *info)
{
	struct list_object_tester * mt = ((struct list_object_tester *)this);
	bool match = false;
	
	mt->count += 1;
	// Check that the returned name matches what was set
	for (int i =0; i <  mt->num_names; ++i) {
		if (strcmp(mt->obj_names[i], info->name) == 0) {
			match = true;
			break;
		}
	}
	fail_if(match == false);

	return true;
}

static bool
test_list_cb_at_most_2(void *this, const struct iobj_list_obj_info *info)
{
	struct list_object_tester * mt = ((struct list_object_tester *)this);
	bool match = false;
	
	mt->count += 1;
	// Check that the returned name matches what was set
	for (int i =0; i <  mt->num_names; ++i) {
		if (strcmp(mt->obj_names[i], info->name) == 0) {
			match = true;
			break;
		}
	}
	fail_if(match == false);

	if ( mt->count == 2)
		return false;
	return true;
}

static bool
test_list_cb_stat(void *this, const struct iobj_list_obj_info *info)

{
	struct stat *stats = (struct stat *)this;

	if (!this)
		fail_if(info->stats, "Expected no stats returned");
	else {
		fail_if(!info->stats, "Expected stats to be returned");

		fail_if(info->stats->st_ino != stats->st_ino, "ino doesn't match");

		fail_if(info->stats->st_birthtime != stats->st_birthtime,
		    "birthtime doesn't match");
	}
	return true;
}

TEST(test_list_object_stat)
{
	struct isi_error *error = NULL;
	struct ibucket_handle *ibh = NULL;
	struct ibucket_name *ib = NULL;
	struct stat stats;
	int count = 0;
	char *obj_name = "unitest_object_name_list_stat";

	struct iobject_handle *iobj = NULL;
	struct fmt FMT_INIT_CLEAN(opath);

	// create a bucket
	char * test_shim_acct = "unitest_shim_acct";
	char * test_bucket_with_acct = "unitest_bucket_list_object_stat";
	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, test_shim_acct, 
	    test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));

	ibh = iobj_ibucket_create(ib, DEFAULT_MODE,
	    NULL, OSTORE_ACL_BUCKET, false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	iobj = iobj_object_create(ibh, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
	    isi_error_fmt(error));

	iobj_object_commit(iobj, &error);
	fail_if(error, "received error %#{} committing object", 
	    isi_error_fmt(error));

	fmt_print(&opath, "%s/%s", iobj->base.path, iobj->target_obj_name);
	stat(fmt_string(&opath), &stats);

	struct iobj_list_obj_param param = {0};
	param.ibh = ibh;
	param.caller_cb = test_list_cb_stat;
	param.stats_requested = true;
	param.caller_context = &stats;

	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing object with stats", 
		isi_error_fmt(error));
	fail_if(count != 1, "Expected only a single object with stats");
	

	param.caller_context = NULL;
	param.stats_requested = false;
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing object without stats", 
		isi_error_fmt(error));
	fail_if(count != 1, "Expected only a single object without stats");

	iobj_object_close(iobj, &error);
	fail_if(error, "received error %#{} close object", 
	    isi_error_fmt(error));
	
	error = iobj_object_delete(ibh, obj_name);
	fail_if(error, "received error %#{} delete object", 
	    isi_error_fmt(error));

	test_util_close_bucket(ibh);
	iobj_ibucket_name_free(ib);
}

#define BOGUS_DEPTH 2
#define BOGUS_FILE_NAME "BOGUS_FILE"
#define BOGUS_DIR_BASE "BOGUS_DIR"
#define BOGUS_DIRECTORY_DEPTH (BOGUS_DEPTH + 4)

TEST(test_list_object)
{
	struct isi_error *error = 0;
	struct ibucket_handle *ibh = 0;
	struct ibucket_name *ib = 0;
	struct list_object_tester test_list_context = {0};
	size_t count;
	size_t i;
	char *obj_name[] = {"unitest_object_name1",
				"unitest_object_name2",
				"unitest_object_name3",
				"unitest_object_name4"};
	size_t num_obj = sizeof(obj_name)/sizeof(obj_name[0]);
	struct iobject_handle *oh[sizeof(obj_name)/sizeof(obj_name[0])];
	struct fmt FMT_INIT_CLEAN(badpath);
	char *path = NULL;
	char *ptr = NULL;
	int ret = -1;
	
	// create a bucket
	char * test_shim_acct = "unitest_shim_acct";
	char * test_bucket_with_acct = "unitest_bucket_list_object";
	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, test_shim_acct, 
	    test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket name", 
	    isi_error_fmt(error));

	ibh = iobj_ibucket_create(ib, DEFAULT_MODE,
	    NULL, OSTORE_ACL_BUCKET, false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	struct iobj_list_obj_param param = {0};
	param.ibh = ibh;
	param.caller_cb = test_list_cb_all;
	param.stats_requested = false;
	param.caller_context = &test_list_context;

	// list objects in (empty bucket)
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing objects", 
		isi_error_fmt(error));
	fail_if((count != 0) || (count != test_list_context.count)); 

	// create objects
	test_list_context.obj_names = obj_name; 
	test_list_context.num_names = num_obj;
	test_list_context.count = 0;
	for (i = 0; i < num_obj; ++i) {
		oh[i] = iobj_object_create(ibh, obj_name[i], 0, DEFAULT_MODE,
		    NULL, OSTORE_ACL_OBJECT_FILE, &error);
		fail_if(error, "received error %#{} creating object", 
		    isi_error_fmt(error));
	}

	// list all objects before commit
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing objects", 
		isi_error_fmt(error));
	fail_if((count != 0), 
	    "Test failed, expected zero objects before commit, found %d",
	    count);

	for (i = 0; i < num_obj; ++i) {
		iobj_object_commit(oh[i], &error);
		fail_if(error, "received error %#{} committing object", 
		    isi_error_fmt(error));
	}

	// list all objects
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing objects", 
		isi_error_fmt(error));
	fail_if((count != num_obj) || (count != test_list_context.count));

	// list atmost 2 objects
	test_list_context.count = 0; // reset count
	param.caller_cb = test_list_cb_at_most_2;
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing objects", 
		isi_error_fmt(error));
	fail_if((count > 2) || (count != test_list_context.count));

	// Add some bogus files in the object directory and verify that we
	// the proper number of objects.
	path = strdup(oh[1]->base.path);
	fail_if(!path);
	for (i = 0; i < BOGUS_DEPTH; i++) {
		ptr = strrchr(path, '/');
		fail_if(!ptr);
		*ptr = '\0';
	}

	fmt_print(&badpath, "%s/%s", path, BOGUS_FILE_NAME);

	ret = creat(fmt_string(&badpath), 0777);
	fail_if(ret == -1, "Could not create bogus file %s", 
	    fmt_string(&badpath), strerror(errno));

	param.caller_cb = test_list_cb_all;
	// list all objects with that bogus file
	test_list_context.count = 0; // reset count
	count = iobj_object_list(&param, &error);
	fail_if(error, "received error %#{} listing objects", 
		isi_error_fmt(error));
	fail_if((count != num_obj) || (count != test_list_context.count),
	    "object count mismatch %d vs expected %d vs callback count %d "
	    "with bogus file",
	    count, num_obj, test_list_context.count);
	
	unlink(fmt_string(&badpath));

	// create a hierachy or bogus directories to check the list will 
	// continue
	fmt_truncate(&badpath);
	fmt_print(&badpath, "%s", path);
	for (i = 0; i < BOGUS_DIRECTORY_DEPTH; i++) {
		fmt_print(&badpath, "/%s%zu", BOGUS_DIR_BASE, i);
		ret = mkdir(fmt_string(&badpath), 777);
		fail_if(ret == -1, "Could not create bogus dir %s",
		    fmt_string(&badpath), strerror(errno));
	}

	// list all objects with that bogus directory depth
	test_list_context.count = 0; // reset count
	count = iobj_object_list(&param, &error);
	fail_if(error, 
	    "received error %#{} listing objects - bogus directories", 
	    isi_error_fmt(error));
	fail_if((count != num_obj) || (count != test_list_context.count), 
	    "object count mismatch %d vs expected %d vs callback count %d "
	    "with bogus directories",
	    count, num_obj, test_list_context.count);
	

	// clean up
	for (size_t i = 0; i < num_obj; ++i) {
		iobj_object_close(oh[i], &error);
		fail_if(error, "received error %#{} close object", 
		    isi_error_fmt(error));
		error = iobj_object_delete(ibh, obj_name[i]);
		fail_if(error, "received error %#{} delete object", 
		    isi_error_fmt(error));
	}

	free(path);

	test_util_close_bucket(ibh);
	iobj_ibucket_name_free(ib);
}

/*
 * Simple create/delete of an object in a bucket
 */
TEST(test_delete_object1)
{
	struct isi_error *error = 0;
		
	char *obj_name = "unitest_object_name_test_delete1";

	// do it twice to make sure nothing is left that precludes
	// creation
	for (int i = 0; i < 2; ++i) {
		// create object
		fail_if(!g_BUCKET_HANDLE, "Bucket not opened");
		
		/* 
		   TBD when ostore acl is implemented. 
		   The ostore_acl_mode passed is dependent on the 
		   headers allowed to be passed when objects are created.
		   The value passed need to be validated in all places
		   wherever it is called.
		*/
		struct iobject_handle *oh = iobj_object_create(
		    g_BUCKET_HANDLE, obj_name, 0,
		    DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
		fail_if(error, "received error %#{} creating object", 
			isi_error_fmt(error));
	
		// delete object
		error = iobj_object_delete(g_BUCKET_HANDLE, obj_name);
		fail_if(error, "received error %#{} deleting object", 
			isi_error_fmt(error));
	
		// clean up to catch memory leaks
		iobj_object_close(oh, &error);
		fail_if(error, "received error %#{} close object", 
			isi_error_fmt(error));
	}
}

/*
 * test_delete_object2 was deleted as it tested the reordering/renaming of
 * object directories for the collision case.  With the addition of the
 * collision directory to the hierarchy, this is functionality was removed so
 * the test case was as well. 
 */
/*
 * Test deletion where there is collision at the bottom hash for object name
 */
TEST(test_delete_object3)
{
	struct isi_error *error = 0;
	int i;
	char *obj_name = "unitest_object_name_test_delete3";
	
	// create object
	fail_if(!g_BUCKET_HANDLE, "Bucket not opened");
	struct iobject_handle *oh = iobj_object_create(g_BUCKET_HANDLE,
	    obj_name, 0, DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	// Create a different dummy botHash in the topHash dir for this object
	struct fmt FMT_INIT_CLEAN(dummy_opath);
	fmt_print(&dummy_opath, "%s", oh->base.path);
	char * dummy_obj = fmt_detach(&dummy_opath);
	for (i=0; i < 1; ++i)
	{
		char * marker = strrchr(dummy_obj, '/');
		fail_if(marker == 0);
		*marker = '\0';
	}
	fmt_truncate(&dummy_opath); 
	fmt_print(&dummy_opath, "%s_bh_collision", dummy_obj);
	int ret = mkdir(fmt_string(&dummy_opath),
	    OSTORE_OBJECT_DIR_DEFAULT_MODE);
	fail_if (ret, "errno is %d for path %s", dummy_obj);

	// delete object
	error = iobj_object_delete(g_BUCKET_HANDLE, obj_name);
	fail_if(error, "received error %#{} deleting object", 
		isi_error_fmt(error));

	// verify that the original bh dir does not exist but the new does
	struct stat sb;
	ret = stat(dummy_obj, &sb);
	fail_if(ret == 0);
	ret = stat(fmt_string(&dummy_opath), &sb);
	fail_if(ret != 0);

	// clean up to catch memory leaks
	free(dummy_obj);
	fmt_clean(&dummy_opath);
	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
}

/*
 * Test deletion where there is collision at the top hash for object name
 */
TEST(test_delete_object4)
{
	struct isi_error *error = 0;
	int i;
	char *obj_name = "unitest_object_name_test_delete4";
	
	// create object
	fail_if(!g_BUCKET_HANDLE, "Bucket not opened");
	struct iobject_handle *oh = iobj_object_create(g_BUCKET_HANDLE,
	    obj_name, 0, DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	// Create a different dummy topHash in the bucket dir for this object
	struct fmt FMT_INIT_CLEAN(dummy_opath);
	fmt_print(&dummy_opath, "%s", oh->base.path);
	char * dummy_obj = fmt_detach(&dummy_opath);
	for (i=0; i < 2; ++i)
	{
		char * marker = strrchr(dummy_obj, '/');
		fail_if(marker == 0);
		*marker = '\0';

	}
	fmt_truncate(&dummy_opath); 
	fmt_print(&dummy_opath, "%s_th_collision", dummy_obj);
	int ret = mkdir(fmt_string(&dummy_opath),
	    OSTORE_OBJECT_DIR_DEFAULT_MODE);
	fail_if (ret, "errno is %d for path %s", errno, dummy_obj);

	// delete object
	error = iobj_object_delete(g_BUCKET_HANDLE, obj_name);
	fail_if(error, "received error %#{} deleting object", 
		isi_error_fmt(error));

	// verify that the original topHash dir does not exist but the new does
	struct stat sb;
	ret = stat(dummy_obj, &sb);
	fail_if(ret == 0);
	ret = stat(fmt_string(&dummy_opath), &sb);
	fail_if(ret != 0);

	// clean up to catch memory leaks
	free(dummy_obj);
	fmt_clean(&dummy_opath);
	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
}

/**
 * test creation of object after handle is acquired but bucket is 
 * deleted.
 */  
TEST(test_object_create_in_deleted_bucket)
{

	struct isi_error *error = 0;
	struct ibucket_handle *ibh = 0;
	char *obj_name = "unitest_object_name_no_bucket";
	struct ibucket_name *ib = 0;
	struct iobject_handle *oh = 0;

	// create and delete a bucket but retain the handle
	char * test_shim_acct = "unitest_shim_acct";
	char * test_bucket_with_acct = "unitest_bucket_with_acct";
	ib = iobj_ibucket_from_acct_bucket(g_STORE_HANDLE, test_shim_acct, 
	    test_bucket_with_acct, &error);
	fail_if(error, "received error %#{} creating bucket name", 
		isi_error_fmt(error));

	ibh = iobj_ibucket_create(ib, DEFAULT_MODE,
	    NULL, OSTORE_ACL_BUCKET, false, &error);
	fail_if(error, "received eror %#{} creating bucket", 
	    isi_error_fmt(error));

	error = iobj_ibucket_delete(ib, false);
	fail_if(error, "received error %#{} deleting bucket", 
	    isi_error_fmt(error));
	
	// try to create an object against the retained handle; it should fail
	oh = iobj_object_create(ibh, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error == 0);
	isi_error_free(error);

	// close the handle from the bucket
	test_util_close_bucket(ibh);
	
	// clean up to catch memory leaks:
	iobj_ibucket_name_free(ib);
}

/*
 * Note that leading and trailing white space is removed by the underlying
 * library.  Be sure to take that into account 
 */
struct test_attrs {
	char *key;
	char *value;
} our_attrs[] = { 
	{"test_attribute",    "abcdefghijklmnopqrstuvwxyz0123456789_"},
	{"SecondAttribute",   "This is a test *** ."},
	{"TheThirdAttribute", "A longer attribute that contains some "
	                      "*&(#* special characters\" that might be there."}
};

TEST(test_object_attributes)
{
	// simple attribute test, set, get, release
	struct isi_error *error = NULL;
	struct iobj_object_attrs *list = NULL;
	void *attr_value = NULL;
	size_t attr_value_size = 0;
	int num_attrs = sizeof our_attrs/ sizeof our_attrs[0];
	bool found = false;
	int attr_count = 0;
	size_t value_size, our_attr_size;

	int flags = 0;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;

	char *key = NULL;
	char *value = NULL;

	int i, a;

	// now create the object	
	char *obj_name = "unitest_object_name_test_attr";

	struct iobject_handle *oh = iobj_object_create(g_BUCKET_HANDLE, 
	    obj_name, 0, DEFAULT_MODE, NULL, OSTORE_ACL_OBJECT_FILE, &error);
		
	fail_if(error, "received error %#{} creating object", 
	    isi_error_fmt(error));
	
	/* 
	 * check the list once before we add any attributes
	 */
	attr_count = iobj_object_attribute_list(oh, &list, &error);
	fail_if(error, "received error %#{} getting empty attribute list",
	    isi_error_fmt(error));

	fail_if(attr_count, "Initial number of attributes returned "
	    "expected to be 0");

	iobj_object_attribute_set(oh, our_attrs[0].key, our_attrs[0].value,
	    strlen(our_attrs[0].value), &error);
	fail_if(error, "received error %#{} on object attribute set",
	    isi_error_fmt(error));

	iobj_object_commit(oh, &error);
	fail_if(error, "received error %#{} commit object", 
	    isi_error_fmt(error));

	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", 
	    isi_error_fmt(error));
	
	/*
	 * reopen the object and check for a single attribute
	 */
	oh = iobj_object_open(g_BUCKET_HANDLE, obj_name, 
	    (enum ifs_ace_rights) OBJECT_READ, flags, cf_flags, 
	    &error);
	fail_if(error, "received error %#{} opening object", 
	    isi_error_fmt(error));

	iobj_object_attribute_get(oh, our_attrs[0].key,
	    &attr_value, &attr_value_size, &error);
	fail_if(error, "received error %#{} on object attribute get",
	    isi_error_fmt(error));

	fail_if((attr_value_size != strlen(our_attrs[0].value)),
	    "attribute returned was not the same size than attribute set");

	fail_if((attr_value == NULL), "NULL attribute returned");

	fail_if(memcmp(attr_value, our_attrs[0].value, attr_value_size), 
	    "attribute comparison failed");

	iobj_object_attribute_release(attr_value);

	// Set the single attribute to one with an empty value
	iobj_object_attribute_set(oh, our_attrs[0].key, NULL, 0, &error);
	fail_if(error , "received error %#{} on set empty attr value",
	    isi_error_fmt(error));

	iobj_object_attribute_get(oh, our_attrs[0].key, &attr_value,
	   &attr_value_size, &error);
	fail_if(error , "received error %#{} on get empty attr value",
	    isi_error_fmt(error));
	fail_if(attr_value_size != 0);
	iobj_object_attribute_release(attr_value);

	iobj_object_attribute_delete(oh, our_attrs[0].key,  &error);
	fail_if(error, "received error %#{} on object attribute delete",
	    isi_error_fmt(error));

	iobj_object_attribute_get(oh, our_attrs[0].key, &attr_value, 
	    &attr_value_size, &error);
	fail_if(!error, 
	    "didn't revieve expected on object attribute get after delete");
	isi_error_free(error);
	error = NULL;

	for (i = 0; i < num_attrs; i++) {
		our_attr_size = strlen(our_attrs[i].value);
		iobj_object_attribute_set(oh, our_attrs[i].key, 
		    our_attrs[i].value, our_attr_size, &error);
		fail_if(error, "received error %#{} on object attribute set",
		    isi_error_fmt(error));
	}

	attr_count = iobj_object_attribute_list(oh, &list, &error);
	fail_if(error, "received error %#{} getting attribute list",
	    isi_error_fmt(error));

	fail_if(attr_count != num_attrs, "Number of attributes returned "
	    "from attr list %d does not match number of attributes set %d",
	    attr_count, num_attrs);

	for (a = 0; a < attr_count; a++) {

		key = IOBJ_OBJECT_ATTR_KEY(list, a);

		value = IOBJ_OBJECT_ATTR_VALUE(list, a);
		value_size = IOBJ_OBJECT_ATTR_VALUE_SIZE(list, a);
		for (i = 0, found = false; i < num_attrs; i++) {
			if (!strcmp(key, our_attrs[i].key)) {
				found = true;
				our_attr_size = strlen(our_attrs[i].value);
				fail_if(value_size != our_attr_size,
				    "Size of attribute %d does not match "
				    "our_attrs %d, our_attr key is %s", 
				    value_size, our_attr_size, 
				    our_attrs[i].key);
				fail_if(strcmp(value, our_attrs[i].value),
				    "Value in returned list %s did "
				    "not match original %s in for key %s",
				    value, our_attrs[i].value, key);
				break;
			}
		}

		fail_if(!found, "Key %s in returned list did not match any "
		    "original key, iter %d", key, a);
	}

	/*
	 * Delete all attrs from list
	 */
	iobj_object_attribute_delete(oh, NULL, &error);
	fail_if(error, "received error %#{} deleting all attrs from list",
		    isi_error_fmt(error));
	
	iobj_object_attribute_list_release(oh, list, &error);
	fail_if(error, "returned error %#{} from list_release", 
	    isi_error_fmt(error));
	
	/*
	 * try a simple invalid handle....
	 */
	iobj_object_attribute_list(NULL, &list, &error);
	fail_if(!error, "Expected to receive error from specifying NULL "
	    "handle but did not");
	isi_error_free(error);
	error = NULL;

	attr_count = iobj_object_attribute_list(oh, &list, &error);
	fail_if(error, "received error %#{} getting empty attribute list",
	    isi_error_fmt(error));

	fail_if(attr_count, "Number of attributes returned expected to be 0");

	// clean up to catch memory leaks:	
	iobj_object_close(oh, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));		

	error = iobj_object_delete(g_BUCKET_HANDLE, obj_name);
	fail_if(error, "received error %#{} delete object", 
		isi_error_fmt(error));		
}

struct collision {
	char *oname;
	char *fname;
	bool found;
};

/*
 * parameters for name collision testing.  Note that a seperate object store
 * is used since we adjust parameters for this test
 */
#define HASH_THRES	8	// threshold to start hash name splitting
#define HASH_LEFT	3	// bytes at left of name to include in filename
#define HASH_RIGHT	3	// bytes at right of name to include in fname
#define HASH_VALUE	7	// hash directory value for all objects
#define HASH_INSERT	NULL	// characters to be inserted between R and L
#define HASH_DUP_POINT	1	// point to insert duplicate object
#define HASH_DUP_INDEX  0	// cdata index to use as duplicate
#define HASH_EXTRA	"aaa..bbb" // oname for too many objects to hash

struct collision cdata[] = {
{"aaa1111bbb",NULL,false},
{"aaa*/?%bbb",NULL,false},
{"aaa..bbb",NULL,false},
};

TEST(test_object_name_collision)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ios = NULL;
	struct ibucket_handle *ibh = NULL;
	struct iobject_handle **iobj;
	struct iobject_handle *iobjerr = NULL;
	struct fmt FMT_INIT_CLEAN(find);
	struct fmt FMT_INIT_CLEAN(fname);

	int object_count = sizeof cdata/sizeof cdata[0];

	char oname[MAXPATHLEN];
	char str[MAXPATHLEN];
	char *ptr = NULL;
	int oname_size;
	int i;
	int len;

	FILE *f = NULL;

	iobj = calloc(object_count, sizeof (struct iobject_handle *));

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_ROOT_UID);
#endif

	ios = iobj_ostore_create(OST_OBJECT, NAME_COLLISION_STORE, NULL, &error);
	fail_if(error, "Received error %#{} creating ostore for "
	    "name collision test", isi_error_fmt(error));

	disable_name_hashing(ios, HASH_THRES, HASH_LEFT, HASH_RIGHT, 
	    HASH_VALUE, NULL, HASH_INSERT, object_count);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_BASE_UID);
#endif

	// Access rights (BUCKET_READ) flag not used while bucket creation 
	// is done.
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ,
	    ios, "Obj", "NameCollide");
	fail_if(!ibh, "Did not create the bucket using test_util_open_bucket");

	for (i = 0; i < object_count; i++) {
		iobj[i] = iobj_object_create(ibh, cdata[i].oname, 
		    OSTORE_ENFORCE_SINGLE_OBJECT, DEFAULT_MODE,
		        NULL, OSTORE_ACL_OBJECT_FILE, &error);
		fail_if(error, 
		    "Received error %#{} creating object %d for collision test",
		    isi_error_fmt(error), i);

		
		ptr = strrchr(iobj[i]->base.path, '/');
		fail_if(!ptr, "Couldn't dig out object dir name");
		if (cdata[i].fname)
			free(cdata[i].fname);
		cdata[i].fname = strdup(++ptr);

		if (i == HASH_DUP_POINT){
			iobjerr = iobj_object_create(ibh, 
			    cdata[HASH_DUP_INDEX].oname, 
			    OSTORE_ENFORCE_SINGLE_OBJECT, DEFAULT_MODE,
			    NULL, OSTORE_ACL_OBJECT_FILE, &error);
			fail_if(!error, 
			    "Failed to received error creating colliding "
			    "object for collision test before commit");
			isi_error_free(error);
			error = NULL;
		}

		iobj_object_commit(iobj[i], &error);
		fail_if(error, 
		    "Received error %#{} commiting object %d for collision"
		    "test after commit", isi_error_fmt(error), i);

		if (i == HASH_DUP_POINT){
			iobjerr = iobj_object_create(ibh, 
			    cdata[HASH_DUP_INDEX].oname, 
			    OSTORE_ENFORCE_SINGLE_OBJECT, DEFAULT_MODE,
			    NULL, OSTORE_ACL_OBJECT_FILE, &error);
			fail_if(!error, 
			    "Failed to received error creating colliding "
			    "object for collision test");
			isi_error_free(error);
			error = NULL;
		}
	}

	iobjerr = iobj_object_create(ibh, HASH_EXTRA, 
	    OSTORE_ENFORCE_SINGLE_OBJECT, DEFAULT_MODE, NULL, 
	    OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(!error, 
	    "Expected to receive error creating 4th object for collision test, "
	    "but did not");
	isi_error_free(error);
	error = NULL;

	for (i = 0; i < object_count; i++) {
		iobj_object_close(iobj[i], &error);
		fail_if(error, 
		    "Received error %#{} closing object %d for collision test", 
		    isi_error_fmt(error), i);
	}

	// free up iobj array
	free(iobj);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_ROOT_UID);
#endif

	fmt_print(&find, "find %s/%d/%d -name obj", 
	    ibh->base.path, HASH_VALUE, HASH_VALUE);

	f = popen(fmt_string(&find), "r");

	fail_if(!f, "Could not open pipe for %s command", fmt_string(&find));

	while (fgets(str, sizeof(str), f)) {
		len = strlen(str);
		if (len > 0 && (str[--len] == '\n'))
			str[len] = '\0';
		fmt_print(&fname, str);
		ptr = strrchr(str, '/');
		*ptr = '\0';
		ptr = strrchr(str, '/');
		ptr++;

		for (i=0; i < object_count; i++) {
			if (!strcmp(cdata[i].fname, ptr)) {
				oname_size = 
				    extattr_get_file(str,
					OSTORE_OBJECT_KEY_ATTR_NAMESPACE,
					OSTORE_OBJECT_NAME_KEY,
					oname,
					sizeof oname);
				oname[oname_size] = '\0';
				if (!strcmp(cdata[i].oname, oname))
					cdata[i].found = true;
			}
		}

		fmt_truncate(&fname);

	}
	if (f != NULL)
		pclose(f);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_BASE_UID);
#endif

	for (i=0; i < object_count; i++) {
		fail_if(!cdata[i].found,
		    "Did not find expected object %s in file %s",
		    cdata[i].oname, cdata[i].fname);
		error = iobj_object_delete(ibh, cdata[i].oname);
		fail_if(error, 
		    "Received error %#{} deleting object %d for collision"
		    "test", isi_error_fmt(error), i);
		if (cdata[i].fname)
			free(cdata[i].fname);
		
	}

	iobj_ibucket_close(ibh, &error);
	fail_if(error, 
	    "Received error %#{} closing bucket for collision test", 
	    isi_error_fmt(error));

	iobj_ostore_close(ios, &error);
	fail_if(error, 
	    "Received error %#{} closing store for collision test", 
	    isi_error_fmt(error));

}

TEST(test_system_attrs)
{
	struct isi_error *error = NULL;
		
	char *obj_name = "unitest_object_sysattr_name";

	int input_count = 0;
	size_t wr_size = 32768;
	int ret = -1;

	struct stat file_stats, obj_stats;
	    

	fail_if(!g_BUCKET_HANDLE, "Bucket not opened");
	
	struct iobject_handle *iobj = 
		iobj_object_create(g_BUCKET_HANDLE, obj_name, 0, DEFAULT_MODE,
		    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	/*
	 * write some data to the object so we can check the length.
	 */

	input_count = find_input_files(&error);
	fail_if(error, "received error %#{} generating name of file to write",
	    isi_error_fmt(error));

	fail_if(input_count < 1, 
	    "Did not find any files to use as object right source");

	do_object_write(iobj, rw_files[0].fullname, wr_size, &error);
	fail_if(error, 
	    "Received error %#{} writing object %s from file %s",
	    isi_error_fmt(error), obj_name, 
	    rw_files[0].fullname);

	/*
	 * Get stats for original file
	 */
	ret = stat(rw_files[0].fullname, &file_stats);
	fail_if(ret, "stats on source file failed with error %s",
	    strerror(errno));

	/*
	 * Get object's system attributes 
	 */
	iobj_object_system_attributes_get(iobj, &obj_stats, &error);
	fail_if(error, "received error %#{} while getting system attributes"
	    "for the object", isi_error_fmt(error));

	/*
	 * compare relevant statistics
	 */
	fail_if((file_stats.st_size != obj_stats.st_size),
	    "Size of object %lld and size of file %lld do not match",
	    obj_stats.st_size, file_stats.st_size);
	
	// clean up to catch memory leaks:	
	iobj_object_close(iobj, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
	cleanup_found_files(input_count);
}

TEST(test_object_access)
{
	struct ibucket_handle *ibh = NULL;
	struct iobject_handle *iobj = NULL;
	struct iobject_handle *iobj2 = NULL;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(path);
	struct fmt FMT_INIT_CLEAN(opath);
	struct fmt FMT_INIT_CLEAN(tpath);

	char *dir_name = ACCESS_CHECK_DIR_NAME;
	char *obj_name = "__Object_Access_Check__" ;
	
	int rv = -1;
	int flags = 0;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;

	fail_if(!g_BUCKET_HANDLE, "Bucket not opened");

	ibh = g_BUCKET_HANDLE;
	
	reset_euid(ACCESS_CHECK_UID);

	/*
	 * try creating the object (should fail)
	 */
	iobj2 = iobj_object_create(ibh, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(!error, "expected a failure on the create, but succeeded");
	isi_error_free(error);
	error = NULL;

	reset_euid(ACCESS_CHECK_BASE_UID);

	/* now do create it */
	/* 
	   TBD. ostore_acl_mode passed need to be validated based on the 
	   headers that are passed during object creation. Need to be done
	   at all places it is called.
	*/
	iobj = iobj_object_create(g_BUCKET_HANDLE, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	fmt_print(&path, "%s/%s", iobj->base.path, dir_name);
	fmt_print(&opath, "%s/%s", iobj->base.path, iobj->target_obj_name);
	fmt_print(&tpath, "%s/%s", iobj->base.path, iobj->temp_obj_name);

	reset_euid(ACCESS_CHECK_UID);

	/* try opening the temp object file directly */
	rv = open(fmt_string(&tpath), O_RDONLY);
	fail_if((rv >= 0),
	    "Could open the temp object file when it should be denied");
	
	/*
	 * try creating the object with an existing one open (fail)
	 */
	iobj2 = iobj_object_create(g_BUCKET_HANDLE, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(!error, "expected a failure on the create, but succeeded");
	isi_error_free(error);
	error = NULL;

	reset_euid(ACCESS_CHECK_BASE_UID);

	/* Commit the successfully opened object */
	iobj_object_commit(iobj, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));

	reset_euid(ACCESS_CHECK_UID);

	/*
	 * try creating the object with an existing one open (fail)
	 */
	iobj2 = iobj_object_create(g_BUCKET_HANDLE, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(!error, "expected a failure on the create, but succeeded");
	isi_error_free(error);
	error = NULL;

	/*
	 * try opening the object with an existing one open (fail)
	 */
	iobj2 = iobj_object_open(g_BUCKET_HANDLE, obj_name, 
	    (enum ifs_ace_rights) OBJECT_READ, flags, cf_flags,
	    &error);
	fail_if(!error, "expected a failure on the open, but succeeded");
	isi_error_free(error);
	error = NULL;

	rv = open(fmt_string(&opath), O_RDONLY);
	fail_if((rv >= 0),
	    "Could open the object file when it should be denied");
	
	reset_euid(ACCESS_CHECK_BASE_UID);

	/* Close the successfully opened object */
	iobj_object_close(iobj, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));

	reset_euid(ACCESS_CHECK_UID);

	/*
	 * try creating the object with an existing one (fail)
	 */
	iobj = iobj_object_create(g_BUCKET_HANDLE, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(!error, "expected a failure on the create, but succeeded");
	isi_error_free(error);
	error = NULL;

	/*
	 * Try dropping a directory into the object dir (fail)
	 */
	rv = mkdir(fmt_string(&path), 0777);
	fail_if((rv == 0), 
	    "Could create dir under object when it should be denied");

	/* 
	 * try creating a file in the object dir (fail)
	 */
	rv = creat(fmt_string(&path), 0666);
	fail_if((rv >= 0),
	    "Could create file under object when it should be denied");

	/*
	 * try opening the object file when the object is not open (fail)
	 */
	rv = open(fmt_string(&opath), O_RDWR);
	fail_if((rv >= 0),
	    "Could open the object file when it should be denied");
	
       	reset_euid(ACCESS_CHECK_BASE_UID);

#if 0
	error = iobj_object_delete(ibh, obj_name);
	fail_if(error, "received error %#{} delete object", 
	    isi_error_fmt(error));
#endif
}

struct object_acl_check {
	enum ostore_acl_mode mode;
	char *name;
};

struct object_acl_check object_acl_info[] = {
	{OSTORE_ACL_OBJECT_HASH, "Object Hash"},
	{OSTORE_ACL_OBJECT_HASH, "Object Hash"},
	{OSTORE_ACL_OBJECT, "Object"},
	{OSTORE_ACL_OBJECT_FILE, "Object File"}
};

struct object_acl_check collision_acl_info[] = {
	{OSTORE_ACL_OBJECT_HASH, "Object Hash"},
	{OSTORE_ACL_OBJECT_HASH, "Object Hash"},
	{OSTORE_ACL_OBJECT_HASH, "Object Hash"},
	{OSTORE_ACL_OBJECT, "Object"},
	{OSTORE_ACL_OBJECT_FILE, "Object File"}
};

static void 
check_object_acl_values(struct ibucket_handle *ibh, 
    struct object_acl_check *acl_info, int depth)
{
	struct isi_error *error = NULL;
	struct iobject_handle *iobj = NULL;
	struct fmt FMT_INIT_CLEAN(path);
	struct fmt FMT_INIT_CLEAN(opath);

	const char *object_path = NULL;
	char *bucket_path = NULL;
	char *sep = NULL;
	char *fnd_str = NULL;
	int level = -1;
	bool rv = false;

	char *obj_name = "__Object_ACL_Value_Check__" ;

	/* create object */
	/* 
	   TBD. The ostore_acl_mode passed need to be validated 
	   based on the headers that are allowed to be passed during 
	   object creation. Validation need to be done for both 
	   check_bucket and check_object routines that use ostore_acl_mode.
	*/
	iobj = iobj_object_create(ibh, obj_name, 0, DEFAULT_MODE,
	    NULL, OSTORE_ACL_OBJECT_FILE, &error);
	fail_if(error, "received error %#{} creating object", 
		isi_error_fmt(error));

	/* Commit the successfully opened object */
	iobj_object_commit(iobj, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));

	bucket_path = ibh->base.path;
	fmt_print(&opath, "%s/%s", iobj->base.path, iobj->target_obj_name);
	object_path = fmt_string(&opath);
	fmt_print(&path, "%s", bucket_path);
	
	/* 
	 * get the pieces of the bucket path one by one, starting with the
	 * first hash directory name and ending with the bucket name
	 */
	sep = (char *)object_path + strlen(bucket_path) + 1;
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
		    acl_info[level].mode);
		fail_if(!rv, "Unexpected acl set on %s",
		    acl_info[level].name);
	}

	/* Close the successfully opened object */
	iobj_object_close(iobj, &error);
	fail_if(error, "received error %#{} close object", 
		isi_error_fmt(error));
#if 0
	error = iobj_object_delete(ibh, obj_name);
	fail_if(error, "received error %#{} delete object", 
	    isi_error_fmt(error));
#endif
}

TEST(test_check_object_acl_values)
{
	check_object_acl_values(g_BUCKET_HANDLE, object_acl_info, 4);
}

TEST(test_check_collision_acl_values)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ios = NULL;
	struct ibucket_handle *ibh = NULL;

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_ROOT_UID);
#endif

	ios = iobj_ostore_create(OST_OBJECT, NAME_COLLISION_STORE, NULL, &error);
	fail_if(error, "Received error %#{} creating ostore for "
	    "name collision test", isi_error_fmt(error));

	disable_name_hashing(ios, HASH_THRES, HASH_LEFT, HASH_RIGHT, 
	    HASH_VALUE, NULL, HASH_INSERT, 5);

#ifdef USING_EUID
	reset_euid(ACCESS_CHECK_BASE_UID);
#endif

	// Access rights (BUCKET_READ) flag is not used when bucket
	// creation is done. 
	ibh = test_util_open_bucket(true, OSTORE_ACL_BUCKET,
	    (enum ifs_ace_rights) BUCKET_READ,
	    ios, "Obj", "NameCollide");
	fail_if(!ibh, "Did not create the bucket using test_util_open_bucket");

	check_object_acl_values(ibh, collision_acl_info, 5);

	iobj_ibucket_close(ibh, &error);
	iobj_ostore_close(ios, &error);
}

