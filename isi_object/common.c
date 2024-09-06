/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	common.c __ 2011/08/31 12:03:18 PDT __ ecande
 */
/** @addtogroup ostore_private
 * @{ */
/** @file
 */
#include <sys/mount.h>
#include <sys/param.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <string.h>
#include <fts.h>
#include <dirent.h>

#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_acl/isi_acl_util.h>
#include <sys/isi_persona.h>
#include <sys/isi_ntoken.h>

#include "genobj_common.h"
#include "ostore_internal.h"
#include "genobj_acl.h"


/**
 * Generate the components for a directory path given an initial base path and
 * possible new components.  Upon success, this routine will return a newly
 * allocated string containing the entire path to the object.  The directory
 * corresponding to base_path must already exist.  If the fd pointer is
 * specified, the fd of the last directory in the path is returned with an
 * exclusive lock taken on it.  If the fd is not specified, the directory is
 * created but no lock is taken.
 */
void
mkdir_dash_p(struct fmt *tpath, const char *new_path,
    int create_mode, const struct iobj_object_attrs *attrs,
    const struct iobj_set_protection_args *prot,
    enum ostore_acl_mode base_acl_mode,
    enum ostore_acl_mode final_acl_mode,
    int *out_fd, struct persona *owner, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum ostore_acl_mode acl_mode = base_acl_mode;
	int cur_fd = -1;
	int parent_fd = -1;
	struct ifs_security_descriptor *sd = NULL;
	enum ifs_security_info secinfo = 0;
	int lock_flag = 0;
	struct ifs_createfile_ex cex = {};

	struct fmt FMT_INIT_CLEAN(bpath_fmt);
	char *bpath = NULL, *bpath_fwd, *bpath_end, *rel_bpath;

	ASSERT(new_path && tpath && *error_out == NULL);
	// remove front '/' if any
	while (*new_path == '/')
		++new_path;
	/* nothing to do if no new_path... just get out of here */
        if (!*new_path)
		goto out;

	fmt_print(&bpath_fmt, "%s/%s", fmt_string(tpath), new_path);
	bpath = fmt_detach(&bpath_fmt);
	rel_bpath = bpath;
	bpath_fwd = bpath + strlen(fmt_string(tpath)) + 1;

	/* create the pieces one by one */
	while (1) {
		struct stat sb = {0};
		int err_sav = 0;

		bpath_end = strchr(bpath_fwd, '/');
		if (bpath_end) {
			*bpath_end = '\0';
			/* when intermediate path exists advance to next path */
			if (faccessat(parent_fd, rel_bpath, F_OK, 0) == 0) {
				*bpath_end = '/';
				bpath_fwd = bpath_end + 1;
				continue;
			}
		} else {
			acl_mode = final_acl_mode;
			if (out_fd) 
				lock_flag = O_EXLOCK;
		}

		/* get an sd for the acl mode if there is one */
		ostore_create_default_acl(&sd, acl_mode, owner, &error);
		if (error)
			goto out;

		if (sd) {
			// For Bucket and object file, set the logged in 
			// user as the owner.
			if ((acl_mode == OSTORE_ACL_BUCKET) ||
			    (acl_mode == OSTORE_ACL_OBJECT_FILE) ||
			    (acl_mode == OSTORE_ACL_TMP_OBJECT_FILE)) {
				secinfo = IFS_SEC_INFO_DACL | 
				    IFS_SEC_INFO_OWNER;
			}
			else {
				secinfo = IFS_SEC_INFO_DACL;
			}
		}
		else {
			secinfo = 0;
		}

		/* Try and create the new component */
		cex.secinfo = secinfo;
		cex.sd = sd;
		
		cur_fd = ifs_createfile(parent_fd, rel_bpath,
		    IFS_RTS_DIR_GEN_WRITE, O_CREAT|O_DIRECTORY|lock_flag,
		    create_mode, &cex);

		if (cur_fd > 0 && !bpath_end)
			break; //done successfully

		// checking error
		if (cur_fd < 0) {
			err_sav = errno;
			// skip access error only if exists and not at the end
			if (err_sav == EACCES && bpath_end &&
			    *(bpath_end + 1) != '\0' ) {
				if (!stat(bpath, &sb))
					err_sav = 0;
			}
			if (err_sav) {
				error = isi_system_error_new(err_sav,
				    "Cannot create container %s",
				    bpath);
				goto out;
			}
		}

		if (sd) {
			ostore_release_acl(sd);
			sd = NULL;
		}

		// advance to next path
		if (bpath_end) {
			*bpath_end = '/';
			bpath_fwd = bpath_end + 1;
		}
		// reset relative path
		if (cur_fd >= 0) {
			rel_bpath = bpath_fwd;
			if (parent_fd >= 0)
				close(parent_fd);
			parent_fd = cur_fd;
		}
	}
	if (cur_fd != -1 && attrs) {
		genobj_attributes_set(cur_fd, attrs, &error);
		if (error)
			goto out;
	}
	if (cur_fd != -1 && prot) {
		ostore_ifs_checked_set_protection_fd(cur_fd,
		    prot, &error);
		if (error)
			goto out;
	}
 out:
	if (bpath)
		free(bpath);
	if (sd)
		ostore_release_acl(sd);
	if (parent_fd >= 0)
		close(parent_fd);
	if (error) {
		isi_error_handle(error, error_out);
		if (cur_fd != -1) {
			close(cur_fd);
			cur_fd = -1;
		}
	} else {
		// change tpath for return
		fmt_print(tpath, "/%s", new_path);

		if (out_fd)
			*out_fd = cur_fd;
		else {
			close(cur_fd);
			cur_fd = -1;
		}
	}
}

// result = prefix/<tempName>[/]suffix
void
create_tmp_dir_path(const char *prefix, const struct fmt *suffix,
    struct fmt *result, int create_mode, const struct iobj_object_attrs *attrs,
    enum ostore_acl_mode base_acl_mode, enum ostore_acl_mode final_acl_mode, 
    struct persona *owner, struct isi_error **error_out)
{
	struct fmt FMT_INIT_CLEAN(result_tmp);
	struct isi_error *error = NULL;

	// create a tmp path
	{
		struct fmt FMT_INIT_CLEAN(tmp_path);
		char *templ_path, *tempxx_path;
		
		fmt_print(&tmp_path, "%s/temp.XXXXXX", prefix);
		tempxx_path = fmt_detach(&tmp_path);
		templ_path = mkdtemp(tempxx_path);
		if (templ_path == NULL) {
			error = isi_system_error_new(errno,
			    "failed to create tmp path for prefix %s suffix %s",
			    prefix, fmt_string(suffix));
			free(tempxx_path);
			goto out;
		}
		fmt_print(&result_tmp, "%s", templ_path);
		free(tempxx_path);
	}

	// append suffix to it and create it
	if (strlen(fmt_string(suffix))) {
		mkdir_dash_p(&result_tmp, fmt_string(suffix), create_mode,
		    attrs, NULL, base_acl_mode, final_acl_mode, NULL, owner, &error);
		if (error)
			goto out;
	}

	fmt_truncate(result);
	fmt_print(result, "%s", fmt_string(&result_tmp));
 out:
	if (error)
		isi_error_handle(error, error_out);
}

/**
 * Remove deep sub-directory of a long path close to location of length PATH_MAX
 * It doesn't nothing if path length is less than PATH_MAX.
 *
 * @param[IN] long_path  a long path
 * @return isi_error object if error occurs
 */
static struct isi_error *
remove_tip_of_long_path(const char *long_path)
{
	struct fmt FMT_INIT_CLEAN(tmp_fmt);
	struct fmt FMT_INIT_CLEAN(temp_dest);
	struct fmt FMT_INIT_CLEAN(err_path);
	char *tempxx_path = NULL, *temp_dir;
	int err_no = 0;
	struct isi_error *error = NULL;

	size_t len_long_path = strlen(long_path);
	if (len_long_path <= PATH_MAX -1)
		return NULL; // no need to do anything

	// tmp file name length needs to be less NAME_MAX
	fmt_print(&tmp_fmt, "%s/tmp.XXXXXX", NS_STORE_TMP);
	tempxx_path = fmt_detach(&tmp_fmt);
	temp_dir = mkdtemp(tempxx_path);
	if (!temp_dir) {
		error = isi_system_error_new(errno,
		    "failed to create temp folder in %s", NS_STORE_TMP);
		goto out;
	}

	// move tip to temp and remove it
	char *tip_path = strchr(long_path + PATH_MAX - NAME_MAX - 1, '/');
	ASSERT(tip_path);
	*tip_path = '\0';
	fmt_print(&temp_dest, "%s/tmp1", temp_dir);

	if (rename(long_path, fmt_string(&temp_dest))) {
		error = isi_system_error_new(errno,
		    "Cannot move directory %s to %s",
		    long_path, fmt_string(&temp_dest));
		remove(fmt_string(&temp_dest));
		goto out;
	}
	if (ostore_ifs_remove(temp_dir, true, &err_no,
	    &err_path) != 0) {
		error = isi_system_error_new(err_no,
		    "Cannot delete directory %s",
		    fmt_string(&err_path));
	}
out:
	if (tempxx_path)
		free(tempxx_path);

	return error;
}

// Walk 'rm_path_str' in reverse removing each component in the path until
// it  becomes equal becomes_path_str.
// Returns 0 on success, -1 on error.  The error
// location is indicated by the content remaining in 'rm_path_str'
int
remove_path_until(const char *rm_path_str, const char *becomes_path_str,
    struct isi_error **error_out)
{
	const char *rm_path_end;
	size_t rm_path_len, becomes_path_len;
	int ret = -1;
	struct fmt FMT_INIT_CLEAN(path_str);
	struct fmt FMT_INIT_CLEAN(err_path);
	int err_no = 0;
	
	ASSERT (rm_path_str && becomes_path_str);

	rm_path_len = strlen(rm_path_str);
	becomes_path_len = strlen(becomes_path_str);

	// check that becomes_path_str is achievable
	if (strncmp(rm_path_str, becomes_path_str, becomes_path_len) != 0)
		goto out;

	ret = 0; // default to success ret now

	// no need to do anything
	if (rm_path_len == becomes_path_len || rm_path_len == 0)
		goto out;

	/*
	 * deletion of very long path (lenght > PATH_MAX will fail
	 */
	if (rm_path_len >= PATH_MAX / 2)
		goto ifs_remove;

	while(rm_path_len > becomes_path_len) {
		ret = remove(rm_path_str);
		/*
		 * handle race of someone else deleting this before we get
		 * here, as long as it is gone.
		 */
		if (ret) {
			if (errno == ENOENT)
				ret = 0;
			else if (errno == EACCES) {
				*error_out = ostore_access_denied_error_new
				    ("Deletion of %s denied", rm_path_str);
				goto out;
			}
			else
				goto out;
		}
		rm_path_end = strrchr(rm_path_str, '/');
		if (!rm_path_end)
			goto out;
		*(char *) rm_path_end = '\0';
		rm_path_len = rm_path_end - rm_path_str;
	}
	goto out;

ifs_remove:

	/*
	 * advance to first sub-dir because ostore_ifs_remove deletes
	 * the given dir-path, and we don't want becomes_path_str deleted.
	 */
	rm_path_end = rm_path_str + becomes_path_len;
	while (*rm_path_end == '/') {
		++rm_path_end;
		if (*rm_path_end == '\0')
			goto out; // done
	}

	rm_path_end = strchr(rm_path_end, '/');
	if (rm_path_end) {
		fmt_print(&path_str, "%.*s",
		    (int) (rm_path_end - rm_path_str), rm_path_str);
	} else
		fmt_print(&path_str, "%s", rm_path_str);

	if ((*error_out = remove_tip_of_long_path(rm_path_str))) {
		ret = -1;
		goto out;
	}

	if (ostore_ifs_remove(fmt_string(&path_str), true, &err_no,
	    &err_path) != 0) {
		*error_out = isi_system_error_new(err_no,
		    "Cannot delete directory %s",
		    fmt_string(&err_path));
		ret = -1;
	}

out:
	return ret;
}
/**
 * Remove all contents from a given directory.  If a prefix is specified, only
 * remove the regular files that match the prefix specified.  If no prefix is
 * specified, remove everything.  If there is a directory found, it will be
 * removed only if it is empty.  
 * @params object_path directory from which files are to be removed
 * @params prefix that is to be used as a template for selecting target files
 */
void
remove_all_object_entries(const char *object_path, const char *prefix, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct dirent dirent;
	struct dirent *result = NULL;

	bool do_delete = false;
	bool found_dot = false;
	bool found_dotdot = false;

	int ret = -1;
	int fd = -1;
	DIR *dirp = NULL;

	dirp = opendir(object_path);
	if (!dirp) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno, 
			    "Cannot open directory for object file removal");
		goto out;
	}

	/*
	 * extract the fd for the directory to use for the unlinkat function
	 */
	fd = dirfd(dirp);

	while (!(ret = readdir_r(dirp, &dirent, &result)) && result != NULL) {
		if (prefix && dirent.d_type == DT_REG && 
		    !strncmp(dirent.d_name, prefix, strlen(prefix)))
			do_delete = true;
		else if (!prefix) {
			if (dirent.d_type == DT_DIR) {
				if (!found_dot && 
				    !strcmp(dirent.d_name, ".")) {
					found_dot = true;
					continue;
				} else if (!found_dotdot && 
				    !strcmp(dirent.d_name, "..")) {
					found_dotdot = true;
					continue;
				}
			}
			do_delete = true;
		}

		if (do_delete) {
			/* 
			 * Use the unlinkat to avoid additional lookups.  Note
			 * that enc_unlinkat, unlike unlink, can remove
			 * directories as well.
			 */
			ret = enc_unlinkat(fd, dirent.d_name, ENC_DEFAULT, 0);
			if (ret == -1) {
				if (dirent.d_type == DT_DIR && 
				    errno == ENOTEMPTY) {
					ilog(IL_NOTICE, 
					    "Found unexpected populated "
					    "directory %s during object "
					    "deletion, tmp dir %s has not "
					    "been removed",
					    dirent.d_name, object_path);
				} else if (errno != ENOENT) { /* handle race on
								 delete */
					error = isi_system_error_new(errno, 
					    "Cannot unlink object file %s "
					    "from dir %s",
					    dirent.d_name, object_path);
					goto out;
				}
			}
			do_delete = false; /* reset for next pass */
		}
	}
	if (ret == -1) {
		error = isi_system_error_new(errno, 
		    "Cannot readdir for unlinking object files in dir %s",
		    object_path);
		goto out;
	}
	
 out:
	if (dirp)
		closedir(dirp);

	if (error)
		isi_error_handle(error, error_out);
}

/**
 * Return a persona for the current thread.  This routine will extract a
 * persona for a SID, if possible, or if not possible, a persona for a UID.
 * If neither of these can be determined an error is generated.  The generated
 * persona needs to be freed when no longer needed.
 * @param trustee pointer to contain the persona.
 * @param error_out error returned if persona cannot be generated
 */
static void
get_access_persona(struct persona **trustee, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct native_token *ntoken = NULL;
	struct persona *new_trustee = NULL;

	int fd = -1;
	fd = gettcred(NULL, 0);
	if (fd < 0) {
		error = isi_system_error_new(errno, "gettcred");
		goto out;
	}

	ntoken = fd_to_ntoken(fd);
	if (!ntoken) {
		error = isi_system_error_new(errno, "fd_to_ntoken");
		goto out;
	}

	if (ntoken->nt_fields & NTOKEN_SID) {
		new_trustee = persona_dup(ntoken->nt_sid);
	} else if(ntoken->nt_fields & NTOKEN_UID) {
		new_trustee = persona_alloc_uid(ntoken->nt_uid);
	} else {
		error = ostore_error_new("couldn't get uid from token");
		goto out;
	}

	ilog(IL_DEBUG,  "Creating bucket acl using sid %{}",
	    persona_fmt(new_trustee));

	*trustee = new_trustee;

 out:
	if (error)
		isi_error_handle(error, error_out);
	if (fd >= 0)
		close(fd);
	if (ntoken)
		ntoken_free(ntoken);
}


/**
 * Create a security descriptor for applying to a file system entity according
 * to the mode specified.
 * @param sd address to set the created security descriptor.
 * @param mode flag to specify the ACL to create
 * @param error_out pointer for returning any error (struct isi_error)
 */
void 
ostore_create_default_acl(struct ifs_security_descriptor **sd,  
    enum ostore_acl_mode mode, struct persona *owner, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *newsd = NULL;
	struct ifs_security_acl *dacl = NULL;
	struct ifs_ace **aces = NULL;
	struct persona *trustee = NULL;
	int ace_count = 0;
	int i;

	ASSERT(sd);

	/*
	 * If owner is NULL, get the persona we need to set the 
	 * user level ace otherwise use the owner info passed.
	 * 
	 */
	if (owner == NULL) {
		get_access_persona(&trustee, &error);
	}
	else {
		trustee = owner;
	}
	
	if (error)
		goto out;
	
	switch(mode) {
	case OSTORE_ACL_NONE:
		goto out;
		break;

	case OSTORE_ACL_STORE:
	case OSTORE_ACL_OBJECT_HASH:
	case OSTORE_ACL_BUCKET_HASH:
		aces = calloc(1, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Allow everyone to do everything and inherit for all
		 * children. 
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			IFS_RTS_DIR_ALL, 
			(IFS_ACE_FLAG_OBJECT_INHERIT | 
			    IFS_ACE_FLAG_CONTAINER_INHERIT), 
			0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize ace");
			goto out;
		}
		ace_count++;

		break;

	case OSTORE_ACL_BUCKET:
		aces = calloc(2, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Allow only the owner/creator to access the entity, but give
		 * them all access to the entity.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			IFS_RTS_DIR_ALL, 0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;

		/*
		 * Add an entry that is only for inheritance which allows all
		 * access to the inheriting entity and specifies that same
		 * access to be inherited for the entities children.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			IFS_RTS_DIR_TRAVERSE, 0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize traverse access ace \
                            for everyone.");
			goto out;
		}
		ace_count++;

		break;

	case OSTORE_ACL_OBJECT:
		aces = calloc(1, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Allow everyone traverse the object directory.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_DIR_TRAVERSE | IFS_RTS_DIR_LIST | 
			    IFS_RTS_DIR_READ_EA | IFS_RTS_DIR_READ_ATTRIBUTES),
			0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;

	case OSTORE_ACL_OBJECT_FILE:
		aces = calloc(2, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Allow only the owner/creator to access the entity, but give
		 * them all access to the entity.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			IFS_RTS_FILE_ALL, 0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;

		/* 
		 * This was added to allow everyone to be able to 
		 * view the object attributes when they use ?detail = yes
		 * during the display of all the objects in the Bucket.
		 */

		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			IFS_RTS_FILE_READ_ATTRIBUTES, 
			0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;

        case NS_PRIVATE_READ:
		aces = calloc(1, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Owner has read access/read acl/write acl/execute access.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_STD_READ_CONTROL | IFS_RTS_STD_WRITE_DAC |
			IFS_RTS_FILE_GEN_READ | IFS_RTS_FILE_GEN_EXECUTE),
			0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;	
	
        case NS_PRIVATE:
		aces = calloc(1, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Owner has full access.
		 * Read/write, execute, read/write:attributes, acl.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_ALL | IFS_RTS_STD_WRITE_DAC),
			0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;
		
	case NS_PUBLIC_READ:
		aces = calloc(2, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Owner has full access.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_ALL | IFS_RTS_STD_WRITE_DAC),
			0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_READ | IFS_RTS_FILE_GEN_EXECUTE),
			0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;	

	case NS_PUBLIC_READ_WRITE:
		aces = calloc(2, sizeof(struct ifs_ace));
		ASSERT(aces);

		/*
		 * Owner has full access.
		 */
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_ALL | IFS_RTS_STD_WRITE_DAC),
			0, 0, trustee)) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
	
		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_READ | IFS_RTS_FILE_GEN_WRITE |
			IFS_RTS_FILE_GEN_EXECUTE),
			0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;			

	case NS_PUBLIC:
		aces = calloc(1, sizeof(struct ifs_ace));
		ASSERT(aces);

		if (aclu_initialize_ace(&aces[ace_count], 
			IFS_ACE_TYPE_ACCESS_ALLOWED, 
			(IFS_RTS_FILE_GEN_ALL | IFS_RTS_STD_WRITE_DAC),
			0, 0, persona_everyone())) {
			error = isi_system_error_new(errno, 
			    "Cannot initialize access ace");
			goto out;
		}
		ace_count++;
		break;					

	default:
		error = ostore_error_new("Unexpected acl mode selected %d", 
		    mode);
		goto out;
		break;
	}

	if (ace_count < 1)
		goto out;

	/*
	 * Create the acl from the ACEs specified above.  The acl is allocated
	 * by the routine.
	 */
	if (aclu_initialize_acl(&dacl, aces, ace_count, 0)) {
		error = isi_system_error_new(errno, "Creating acl");
		goto out;
	}

	newsd = calloc(1, sizeof(struct ifs_security_descriptor));
	ASSERT(newsd);

	if (aclu_initialize_sd(newsd, IFS_SD_CTRL_DACL_PRESENT, trustee, NULL,
	    &dacl, NULL, false)) {
		error = isi_system_error_new(errno, "Creating sd");
		goto out;
	}
	dacl = NULL; /* owned by newsd now */
	
 out:
	if (aces) {
		for (i = 0; i < ace_count; i++)
			aclu_free_ace(aces[i]);
		free(aces);
	}

	if (trustee) {
		persona_free(trustee);
	}

	if (error) {
		if (newsd) {
			aclu_free_sd(newsd, true);
			/* 
			 * In case of error, clean this out just in case
			 * error is not checked for success/failure
			 */
			newsd = NULL;
		}
		if (dacl)
			aclu_free_acl(dacl);

		isi_error_handle(error, error_out);
	}
	*sd = newsd;
}

/**
 * free the security descriptor and acl that was generated
 * ostore_create_default_acl().
 * @params sd security descriptor to be released
 */
void
ostore_release_acl(struct ifs_security_descriptor *sd)
{
	aclu_free_sd(sd, true);
}
/**
 * Create and set an ACL, determined by the mode specified, on a file system
 * entity described by either a path or a file descriptor.  If the file
 * descriptor is specified (not -1) it is chosen over the a non-NULL path.
 * @param path if non-NULL, the path to the file system entity for the ACL to
 *        be applied
 * @param fd if not -1, an open file descriptor for the file system entity for
 *        the ACL to be applied
 * @param mode flag (defined by ostore_acl_mode) which determines what ACL is
 *        to be applied
 * @param error generated error if an ACL cannot be applied
 */
void
ostore_set_default_acl(const char *path, int fd, enum ostore_acl_mode mode, 
    struct persona *owner, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_security_descriptor *sd = NULL;
	int ret = -1;
	enum ifs_security_info secinfo = 0;

	ASSERT(path || fd != -1);

	ostore_create_default_acl(&sd, mode, owner, &error);
	if (error)
		goto out;

	/* 
	 * if the sd was not defined then the mode specified does not require
	 * an acl so we are done.
	 */
	if (!sd)
		goto out;

	// If the sd is set for Bucket or Object file logged in user will 
	// be the owner.
	if ((mode == OSTORE_ACL_BUCKET) || (mode == OSTORE_ACL_OBJECT_FILE)
	    || (mode == OSTORE_ACL_TMP_OBJECT_FILE)) {
		secinfo = IFS_SEC_INFO_DACL | IFS_SEC_INFO_OWNER;
	}
	else {
		secinfo = IFS_SEC_INFO_DACL;
	}

	/*
	 * Use the fd if valid otherwise use the path
	 */
	if (fd != -1)
		ret = aclu_set_fd_sd(fd, secinfo, sd, SMAS_FLAGS_FORCE_SD);
	else
		ret = aclu_set_sd(path, 0, secinfo, sd, SMAS_FLAGS_FORCE_SD);

	if (ret != 0 ) {
		error = isi_system_error_new(errno, "Cannot set ACL on %s", 
		    path);
		goto out;
	}
 out:
	if (error)
		isi_error_handle(error, error_out);

	if (sd)
		ostore_release_acl(sd);
}

/*
 * Ensure the file descriptor refers to object on OneFS
 * @param[IN] fd - file descriptor
 * @return NULL if the file descriptor refers to object on OneFS,
 * 	or isi_error otherwise.
 */
struct isi_error *ensure_object_on_onefs(int fd)
{
	struct statfs fs_stat;
	if (fstatfs(fd, &fs_stat)) {
		return isi_system_error_new(errno,
		    "fstatfs failed on object");
	}
	if (strcmp(fs_stat.f_mntfromname, ONE_FS_NAME)) {
		return ostore_unsupported_object_type_error_new(
		    "not a OneFS object");
	}
	return NULL;
}

struct isi_error *ifs_createfile_invalid_arg_error()
{
	return ostore_unsupported_object_type_error_new(
	    "not a OneFS object");
}

/** @} */
