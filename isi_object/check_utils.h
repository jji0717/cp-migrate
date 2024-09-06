#ifndef _CHECK_UTILS_H_
#define _CHECK_UTILS_H_

#include <errno.h>

/**
 * Utility function to create a folder
 * @param[in] path - path name
 */
void create_test_folder(const char *path);

/*
 * Utility function to delete a file path
 * @param[in] path - path name
 */
void delete_test_folder(const char *path);

/**
 * Utility function to delete a file path recursively
 * @param[in] path - path name
 */
void test_util_rm_recursive(const char *path);

/**
 * Utility function to remove the object store
 * @param[in] store_name - object store name
 */
void test_util_remove_store(char *store_name);

/**
 * Utitlity function to delete generic object
 * @param[in] igobj - the object handle
 */
void
test_util_genobj_delete(struct igenobj_handle *igobj);

/**
 * Utility to open or create a bucket.
 * @param[in] create - indicates if to create a bucket
 * @param[in] ios - the object store handle
 * @param[in] bucket_name - the bucket name
 * @return - the bucket handle on success
 */
struct ibucket_handle * test_util_open_bucket(
	bool create, 
	enum ostore_acl_mode create_mode,
	enum ifs_ace_rights ace_rights_open_flg,
	struct ostore_handle* ios, 
	char * acct_name,
	char *bucket_name);

/**
 * Utitlity function to close a buecket
 * @param[in] ibh - the bucket handle
 */
void test_util_close_bucket(struct ibucket_handle *ibh);

/**
 *
 *  Utility to get the number of entries in a directory, returns -1 if not a
 *  directory or the number of entries (less 2 to acount for . and ..) in the
 *  directory. 
 *  @param path - path to the directory
 *  @return - number of entries in the directory (less . & ..) or -1 if not dir
 */
int get_number_dirents(char *path);

bool check_iobj_acl(const char *path, enum ostore_acl_mode acl_mode);


/**
 * Utility function to create a worm file (and root)
 */
void create_test_worm_file(const char *worm_root, const char *file_name,
    const char *retention, bool commit);

/*
 * Utility function to delete a worm file (and root)
 */
void delete_test_worm_file(const char *worm_root, const char *file_name);



#endif /* _CHECK_UTILS_H_ */
