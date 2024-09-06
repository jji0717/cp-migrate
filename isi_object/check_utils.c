#include <stdlib.h>

#include <isi_util/isi_format.h>
#include <check.h>

#include <isi_acl/isi_acl_util.h>
#include <isi_acl/isi_acl_test.h>

#include <isi_object/ostore_internal.h>
#include <isi_object/check_utils.h>
#include <stdio.h>

/*
 * Utility function to create a folder
 * @param[in] path - path name
 */
void
create_test_folder(const char *path)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	fmt_print(&cmdstr, "mkdir -p %s", path);
	fmt_print(&cmdstr, ";isi_runat %s touch ads2 ", path);
	ret = system(fmt_string(&cmdstr));
	fail_if(ret, "%s failed", fmt_string(&cmdstr));
}

/*
 * Utility function to delete a file path
 * @param[in] path - path name
 */
void
delete_test_folder(const char *path)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	fmt_print(&cmdstr, "rm -rf %s", path);
	ret = system(fmt_string(&cmdstr));
	fail_if(ret, "%s failed", fmt_string(&cmdstr));
}


/*
 * Utility function to delete a file path
 * @param[in] path - path name
 */
void
test_util_rm_recursive(const char *path)
{
	delete_test_folder(path);
}

/*
 * Utility function to remove the object store
 * @param[in] store_name - object store name
 */
void
test_util_remove_store(char *store_name)
{
	// this fn isn't implemented yet
	//iobj_ostore_destroy();

	// implement ourselves in a lame and unsafe manner
	struct fmt FMT_INIT_CLEAN(tpath);
	fmt_print(&tpath, "/ifs/.object/%s", store_name);
	test_util_rm_recursive(fmt_string(&tpath));
}

/*
 * Utility to open or create a bucket.
 * @param[in] create - indicates if to create a bucket
 * @param[in] ios - the object store handle
 * @param[in] acct_name - account name for the bucket
 * @param[in] bucket_name - the bucket name
 * @return - the bucket handle on success
 */
struct ibucket_handle *
test_util_open_bucket(bool create, enum ostore_acl_mode create_mode,
    enum ifs_ace_rights ace_rights_open_flg, struct ostore_handle* ios, 
    char *acct, char *bucket_name)
{
	struct ibucket_handle *ibh = NULL;
	struct ibucket_name *ibn = NULL;
	struct isi_error *error = NULL;

	int flags = 0;
	struct ifs_createfile_flags cf_flags = CF_FLAGS_NONE;
	
	ibn = iobj_ibucket_from_acct_bucket(ios, acct, bucket_name, &error);
	fail_if(error,
	    "received error %#{} formatting ibn from acct an bucket",
	    isi_error_fmt(error));

	if(create)	
		ibh = iobj_ibucket_create(ibn, 0600, NULL, NULL, create_mode,
		    false, true, &error);
	else 
		ibh = iobj_ibucket_open(ibn, ace_rights_open_flg, flags, 
		    cf_flags, &error);
		
	fail_if(error, "received error %#{} %s bucket", 
		isi_error_fmt(error), (create? "creating" : "opening"));

	/*
	 * Clean up so we don't leak any memory.
	 */
	iobj_ibucket_name_free(ibn);

	return ibh;
}

/*
 * Utitlity function to close a buecket
 * @param[in] ibh - the bucket handle
 */
void
test_util_close_bucket(struct ibucket_handle *ibh)
{
	struct isi_error *error = 0;
	iobj_ibucket_close(ibh, &error);	
	fail_if(error, "received error %#{} close bucket", 
		isi_error_fmt(error));
}

/*
 * Utitlity function to delete generic object
 * @param[in] igobj - the object handle
 */
void
test_util_genobj_delete(struct igenobj_handle *igobj)
{
	struct isi_error *error = 0;
	struct ostore_del_flags flags = {0};
	flags.recur = 1;
	int len = strlen(igobj->ostore->path) + 1;
	char *path = (char *) calloc (1, len);
	ASSERT(path);

	// The iobj_genobj_delete takes the ostore handle and the 
	// path (the path doesn't include the ostore path. So 
	// remove ostore path if it is present in the path already).
	strncpy(path, igobj->path, len - 1);
	char *opath = path;

	// If the path has the ostore path, extract only the path 
	// without the ostore path.
	if (strcmp(igobj->ostore->path, path) == 0) {
		opath = igobj->path + len;
	}
	
	iobj_genobj_delete(igobj->ostore, opath, flags, &error);
	fail_if(error, "received error %#{} delete object %s",
		isi_error_fmt(error), igobj->name);
	free(path);
}

/*
 *  Utility to get the number of entries in a directory, returns -1 if not a
 *  directory or the number of entries (less 2 to acount for . and ..) in the
 *  directory. 
 */
int
get_number_dirents(char *path)
{
	struct stat stats;
	int count = -1;
	int ret = -1;

	if ((ret = stat(path, &stats))) {
		fail_if(errno != ENOTDIR,
		    "Unexpected error from stat %s on %s",
		    strerror(errno), path);
		goto out;
	}

	if (!(stats.st_mode & S_IFDIR))
		goto out;

	/*
	 * Since we now know this is a directory, get the number of entries by
	 * taking the link count and correcting for . and ..
	 */
	count = stats.st_nlink - 2;
	
 out:
	return count;
}

bool
check_iobj_acl(const char *path, enum ostore_acl_mode acl_mode)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *sd = NULL;
	struct ifs_security_descriptor *file_sd = NULL;
	size_t size = 0;
	int rv = -1;
	struct persona * owner = NULL;

	ostore_create_default_acl(&sd, acl_mode, owner, &error);
	fail_if(error, "Unexpected error %s creating acl", 
	    isi_error_fmt(error));

	rv = aclt_get_secdesc((char *)path, -1, 
	    (IFS_SEC_INFO_DACL | IFS_SEC_INFO_OWNER), 
	    &size, &file_sd);
	fail_if((rv != 0), "Couldn't get file sd: %s", strerror(errno));
	fail_if(file_sd == NULL);
	
	/* 
	 * Because of bug 83885, we now set the inheritance flags on the
	 * control and aces;  we must clear it or the compare will fail
	 */
	file_sd->control &= ~(IFS_SD_CTRL_DACL_AUTO_INHERITED |
	    IFS_SD_CTRL_SACL_AUTO_INHERITED);
	if (file_sd->dacl != NULL) {
		for (int i = 0; i < file_sd->dacl->num_aces; i++)
			file_sd->dacl->aces[i].flags &=
			    ~IFS_ACE_FLAG_INHERITED_ACE;
	}

	
	// Compare the following and flag error if any of these mismatch.
	// 1. Security descriptor revision.
	// 2. Security descriptor control.
	// 3. ACL entries.
	rv = -1;
	if ((sd->revision == file_sd->revision) && 
	    (!aclt_compare_control(sd->control, file_sd->control)) &&
	    (!aclt_compare_security_acls(sd->dacl, file_sd->dacl)) && 
	    (!aclt_compare_security_acls(sd->sacl, file_sd->sacl)))
	    {
		    rv = 0;
	    }
	
	ostore_release_acl(sd);

	free(file_sd);

	return (rv == 0);
}

void create_test_worm_file(const char *worm_root, const char *file_name,
    const char *retention, bool commit)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	// only able to do it with privdel=on
	fmt_print(&cmdstr, "isi worm domains create --mkdir --privileged-delete=on %s --force >/dev/null;", worm_root);
	fmt_print(&cmdstr, "touch %s/%s; ", worm_root, file_name);
	if (retention)
		fmt_print(&cmdstr, "touch -at %s %s/%s;",
		    retention, worm_root, file_name);
	if (commit)
		fmt_print(&cmdstr, "chmod -w %s/%s;", worm_root, file_name);
	ret = system(fmt_string(&cmdstr));

	fail_if(ret, "failed to create test worm file: %s",
	    fmt_string(&cmdstr));
}

void delete_test_worm_file(const char *worm_root, const char *file_name)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	if (file_name) {
		int ret = 0;
		fmt_print(&cmdstr, "isi worm files delete %s/%s --force;",
		    worm_root, file_name);
		ret = system(fmt_string(&cmdstr));
		fail_if(ret, "failed to delete test worm file: %s",
		    fmt_string(&cmdstr));
	}
	delete_test_folder(worm_root);
}

