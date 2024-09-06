/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	object.c __ 2011/08/31 12:00:44 PDT __ ecande
 */
/**
 * @addtogroup ostore
 * @{
 */
/** @file
 */

#include "ob_common.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include <sys/extattr.h>
#include <isi_ufp/isi_ufp.h>

#include <ifs/ifs_userattr.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_file_lock.h>

#include "genobj_common.h"
#include "object_common.h"
#include "ostore_internal.h"
#include "iobj_hash.h"
#include "iobj_licensing.h"
#include "genobj_acl.h"

UFAIL_POINT_LIB(ostore)

/*
 * this is temp routine until we start any type of versioning implementation.
 * the IOBJECT_OBJECT_NAME is the name of the stable file, while
 * IOBJECT_TEMP_NAME is the name of the copy of the file that is being
 * updated (open for write on existing file).
 */

#define IOBJECT_BASE_NAME                  "obj"
#define IOBJECT_OBJECT_NAME                IOBJECT_BASE_NAME
#define IOBJECT_TEMP_NAME                  IOBJECT_BASE_NAME ".tmp"
#define IOBJECT_TEMPLATE_NAME              IOBJECT_TEMP_NAME "_XXXXXX"
#define IOBJECT_COLLISION_OBJDIR_TEMPLATE  "XXXXXX"

/*
 * temporarily support only read mode on open.  write will be able to
 * happen only on creates for the short term
 */
static char *
open_mode_extension(int flags)
{
	return (IOBJECT_OBJECT_NAME);
}

static int
open_mode_flags(int flags)
{
	return (OSTORE_OBJECT_OPEN_READ_FLAGS);
}

static char *
fmt_iobj_object_open_flags(int flags)
{
	return ("Not Yet Implemented");
}

static const char *
created_object_name(int flags)
{
	return(IOBJECT_OBJECT_NAME);
}

/**
 * This function returns an both the allocated string based on the flags for
 * creating an object and the file descriptor for the opened file .  It is the
 * callers responsibility to free this string and close this descriptor when
 * they are no longer being utilized.  At the time of this code being generated,
 * on the successful creation of the object, these two pieces of information
 * will be placed in the object handle and therefore need to be freed/closed
 * when the handle is deleted.
 */
static int
temporary_object_create(
    enum ostore_object_create_flags flags, const char *iobj_name,
    struct ibucket_handle *ibh, const char *opath, int dir_fd, char **oname,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *tmp_obj_name = NULL;
	char *tmpfile_template = NULL;
	char *tmpname = NULL;
	int fd = -1;

	/*
	 * we should never be called unless the parent directory has been
	 * opened already.
	 */
	ASSERT(dir_fd >= 0);

	if (flags && OSTORE_ENFORCE_SINGLE_OBJECT) {
		/*
		 * for the exclusive open case, we have a well known name for
		 * the object temp file, just open it here after we dup a copy
		 * for the handle.
		 */
		tmp_obj_name = strdup(IOBJECT_TEMP_NAME);
		ASSERT(tmp_obj_name);
		fd = enc_openat(dir_fd, tmp_obj_name, ENC_DEFAULT,
		    OSTORE_OBJECT_CREATE_FLAGS,
		    OSTORE_OBJECT_DEFAULT_MODE);
	} else {
		struct fmt FMT_INIT_CLEAN(temp_path);
		fmt_print(&temp_path, "%s/%s", opath, IOBJECT_TEMPLATE_NAME);
		tmpfile_template = fmt_detach(&temp_path);
		fd = mkstemp(tmpfile_template);
		if (fd >= 0) {
			tmpname = strrchr(tmpfile_template, '/');
			ASSERT(tmpname);
			tmp_obj_name = strdup(++tmpname);
			ASSERT(tmp_obj_name);
		}
	}

	if (fd < 0) {
		if (errno == EEXIST &&
		    (flags & OSTORE_ENFORCE_SINGLE_OBJECT)) {
			error = ostore_object_exists_error_new(
				"Cannot create object %s in bucket %s, "
				"object already exists",
				iobj_name, ibh->base.name);
		} else {
			error = isi_system_error_new(errno,
			    "Cannot create object %s in bucket %s, path %s",
			    iobj_name, ibh->base.name,
			    opath);
		}
		goto out;
	}

	*oname = tmp_obj_name;

 out:
	if (error) {
		isi_error_handle(error, error_out);
		if (tmp_obj_name)
			free(tmp_obj_name);
	}

	if (tmpfile_template)
		free(tmpfile_template);

	return fd;
}

enum ostore_hash_options {
	OSTORE_HASH_DIR  = 0x1,
	OSTORE_HASH_NAME = 0x2,
	OSTORE_HASH_ALL  = 0x3
};

enum ostore_hash_shift {
	OSTORE_HASH_SHIFT = 16,
	OSTORE_HASH_MASK  = 0xffff
};

/**
 * Generate the hashed path name for the object specified.  The hash_mode
 * option allows for specifying if just the hash path to the object directory
 * is generated or if the entire name is generated.
 * @param fmt FMT structure to hold the requested formatted
 * @param obj_name name of the object to create
 * @param hash_mode mask as to what part of the hash name is being requested
 * @param params pointer to a structure which holds parameters to describe the
 *        hashing algorithm.
 * @returns boolean set to true if the filename was shortened and could
 *        therefore be susceptible to name collisions.
 */
static bool
hash_object_name(struct fmt *fmt, const char *iobj_name, int hash_mode,
    struct ostore_parameters *params)
{
	uint32_t hval = 0;
	struct fmt FMT_INIT_CLEAN(hname);
	struct fmt FMT_INIT_CLEAN(outname);
	const char *sep = NULL;
	const char *fnd_str = NULL;
	int name_right_offset = 0;
	int namelen = strlen(iobj_name);
	bool handle_name_collision = false;

	/*
	 * Since the object name is being consolidated down in
	 * size to be sure it fits in MAXNAMELEN, it is
	 * possible that there can be a collision on the
	 * filename for this object.  Set this flag so we
	 * handle the directory structure below.
	 */
	if (namelen >= params->hash_name_split_length)
		handle_name_collision = true;


	/*
	 * If the hash directory structure is being requested, generate the
	 * hashed part of that here.
	 */
	if (hash_mode & OSTORE_HASH_DIR) {
		hval = iobj_hash32(iobj_name, namelen, 0);

		/*
		 * Suppress real hashing
		 */
		if (params->hash_name_hashto !=
		    OSTORE_OBJECT_NAME_HASHTO_DEFAULT) {
			hval = (params->hash_name_hashto << OSTORE_HASH_SHIFT)
			    | params->hash_name_hashto;
		}

		UFAIL_POINT_CODE(ostore_hash_dir_to_one,
		    hval = (1 << OSTORE_HASH_SHIFT) | 1;
		);

		fmt_print(fmt, "%d/%d", (OSTORE_HASH_MASK & hval),
		    (hval >> OSTORE_HASH_SHIFT));

		/*
		 * add / for dir + name or when we will have the extra dir for
		 * name collision
		 */
		if (hash_mode & OSTORE_HASH_NAME || handle_name_collision)
			fmt_print(fmt, "/");
	}

	/*
	 * If we don't have to handle name collisions and we don't want the
	 * object name, then we are done.
	 */
	if (handle_name_collision || (hash_mode & OSTORE_HASH_NAME)) {

		if (!handle_name_collision)
			/*
			 * Without the name collision case we just use the
			 * object_name for the name of the object directory.
			 */
			fmt_print(&hname, "%s", iobj_name);
		else {
			/*
			 * Grab the first and last set of characters as
			 * specified by hash_name_split_right and
			 * hash_name_split_left, and insert the separation
			 * characters specified by hash_name_split_insert.
			 * The current default values for these are 32, 32 and
			 * ".."
			 */
			name_right_offset = namelen -
			    params->hash_name_split_right;

			fmt_print(&hname, "%.*s%s%s",
			    params->hash_name_split_left,
			    iobj_name,
			    params->hash_name_split_insert,
			    &iobj_name[name_right_offset]);
		}

		/*
		 * now "encode" it to handle some special characters
		 */
		sep = fmt_string(&hname);

		while (1) {
			if (!sep)
				break;

			/* split the path at the next restricted char */
			fnd_str = sep;
			sep = strpbrk(fnd_str, params->hash_name_translate);
			if (sep) {
				namelen = (sep - fnd_str);
				fmt_print(&outname, "%.*s%%%hhx", namelen,
				    fnd_str, *sep);
				sep++;
			} else
				/* suck to the end of the string */
				fmt_print(&outname, "%s", fnd_str);
		}

		fmt_print(fmt, "%s", fmt_string(&outname));

	}
	return handle_name_collision;
}

enum object_create_state {
	OBJECT_CREATE_STATE_NONE = 0,
	OBJECT_CREATE_STATE_DIR  = 1,
	OBJECT_CREATE_STATE_TMP  = 2,
	OBJECT_CREATE_STATE_ATTR = 3,
	OBJECT_CREATE_STATE_FINAL= 4
};

/**
 * Store the true name of the object as an extended attribute of the directory
 * that contains the object (and all of it's versions
 * @param opath object path (as a &fmt)
 * @param iobj_name object name
 * @param error pointer to isi_error, returned as NULL on success
 */
static void
set_object_name(struct fmt *opath, int fd, const char *obj_name,
    struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;

	ASSERT(error_out);

	/* either the path or the fd needs to be specified */
	ASSERT(opath || fd != -1);

	if (fd != -1)
		ret = extattr_set_fd(fd, OSTORE_OBJECT_KEY_ATTR_NAMESPACE,
		    OSTORE_OBJECT_NAME_KEY, obj_name, strlen(obj_name));
	else
		ret = extattr_set_file(fmt_string(opath),
		    OSTORE_OBJECT_KEY_ATTR_NAMESPACE,
		    OSTORE_OBJECT_NAME_KEY, obj_name, strlen(obj_name));

	if (ret == -1) {
		error = isi_system_error_new(errno);
		isi_error_handle(error, error_out);
	}
}

static bool
get_object_name(struct fmt *opath, int fd, struct fmt *obj_name,
    struct isi_error **error_out)
{
	char obj_key[OSTORE_OBJECT_KEY_MAX_LENGTH+1];
	int ret = -1;
	struct isi_error *error = NULL;
	bool found = false;

	ASSERT(error_out);

	/* either the path or the fd needs to be specified */
	ASSERT(opath || fd != -1);

	if (fd != -1)
		ret = extattr_get_fd(fd, OSTORE_OBJECT_KEY_ATTR_NAMESPACE,
		    OSTORE_OBJECT_NAME_KEY, obj_key, sizeof(obj_key));
	else
		ret = extattr_get_file(fmt_string(opath),
		    OSTORE_OBJECT_KEY_ATTR_NAMESPACE,
		    OSTORE_OBJECT_NAME_KEY, obj_key, sizeof(obj_key));

	if (ret == -1) {
		/*
		 * if there is no such directory to check (case when there is
		 * no fd specified) or if there is a directory but there is no
		 * object name attribute, then return without filling in
		 * object name.
		 */
		if (errno != ENOENT && errno != ENOATTR) {
			error = isi_system_error_new(errno);
			isi_error_handle(error, error_out);
		}
	} else {
		obj_key[ret] = '\0';
		fmt_print(obj_name, "%s", obj_key);
		found = true;
	}
	return found;
}

static bool
verify_object_name(struct fmt *opath, int fd, const char *inname,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool found = false;

	struct fmt FMT_INIT_CLEAN(obj_name);

	ASSERT(error_out);

	/* either the path or the fd needs to be specified */
	ASSERT(opath || fd != -1);

	/* get the stored object name for the path specified */
	found = get_object_name(opath, fd, &obj_name, &error);
	if (error)
		goto out;

	/*
	 * compare the object name passed in to the object name that was
	 * stored with the object path passed in.  If they are the same return
	 * true.
	 */
	if (found)
		found = !strcmp(inname,fmt_string(&obj_name));

 out:
	if (error)
		isi_error_handle(error, error_out);

	return found;
}

/*
 * Look for an object directory in a given directory.  If found, then we
 * return the open file descriptor for it.  We also can return a fmt struct
 * with the object name.
 */
static bool
find_object_name(const char *opath, const char *iobj_name,
    int *fd, struct fmt *object, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	DIR *dp = NULL;
	struct dirent curr_ent;
	struct dirent *ep = 0;
	int dir_fd = -1;
	int tfd = -1;
	int ret = -1;
	bool found = false;
	bool found_dot = false;
	bool found_dotdot = false;

	ASSERT(error_out);
	ASSERT(opath);
	ASSERT(iobj_name);

	dp = opendir(opath);
	if (dp == NULL) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno,
			    "Looking for object directory for object %s",
			    iobj_name);
		goto out;
	}

	dir_fd = dirfd(dp);
	if (dir_fd == -1) {
		error = isi_system_error_new(errno);
		goto out;
	}

	/*
	 * Now check all the directory entries that we have
	 */
	while((ret = readdir_r(dp, &curr_ent, &ep)) == 0) {
		if (ep == 0) { // end of dir
			break;
		}

		/*
		 * If not a directory, can't be an object dir, so ignore it
		 * (maybe we should log this when logging is added).
		 */
		if (ep->d_type != DT_DIR)
			continue;

		if (!found_dot && !strcmp(ep->d_name, ".")) {
			found_dot = true;
			continue;
		}

		if (!found_dotdot && !strcmp(ep->d_name, "..")) {
			found_dotdot = true;
			continue;
		}

		tfd = enc_openat(dir_fd, ep->d_name, ENC_DEFAULT,
		    OSTORE_OBJECT_OPEN_READ_FLAGS);

		if (tfd < 0) {
			if (errno == ENOENT)
				continue;
			else {
				error = isi_system_error_new(errno,
				    "Cannot open potential object dir %s/%s",
				    opath, ep->d_name);
				goto out;
			}
		}

		found = verify_object_name(NULL, tfd, iobj_name, &error);
		if (error)
			goto out;

		/*
		 * Found what we were looking for, if we want the results,
		 * fill them in here.
		 */
		if (found) {
			if (fd)
				*fd = tfd;
			else {
				close(tfd);
				tfd = -1;
			}
			if (object)
				fmt_print(object, "%s", ep->d_name);
			break;
		}

		/* clean up to try again */
		if (tfd != -1) {
			close(tfd);
			tfd = -1;
		}
	}

	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "readdir_r() error trying to find object %s in %s "
		    "during create", iobj_name, opath);
		goto out;
	}

 out:
	if (dp)
		closedir(dp);

	if (error) {
		isi_error_handle(error, error_out);
		if (tfd != -1)
			close(tfd);
	}

	return found;
}

/*
 * Create an object in the bucket corresponding to the bucket handle given,
 * using @a c_mode to specify the read/write characteristics and the given
 * attributes. On success the object handle is returned otherwise a NULL is
 * returned and error is set appropriately.
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name name for the object to be opened
 * @param flags specifying whether or not detecting collision of internal
 *              object hashed name
 * @param c_mode permission for the object
 * @param attributes list of key/value pairs that make up the attributes for
 *              the object
 * @param acl ACL to be set on the object (just a string for now)
 * @param error isi_error structure, NULL on success, set on failure.
 *
 * For object store, c_mode is not used as acl is present
 */
struct iobject_handle *
ob_object_create(struct ibucket_handle *ibh, const char *iobj_name,
    int flags, mode_t c_mode, struct iobj_object_attrs *attrs,
    enum ostore_acl_mode acl_mode, bool overwrite,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobject_handle *iobj = NULL;
	struct ostore_parameters *params = ibh->base.ostore->params;
	bool object_exists = false;
	bool collision_possible = false;
	bool dir_locked = false;
	char *tmp_obj_name = NULL;
	char *dir_template = NULL;
	char *object_dir = NULL;
	int col_fd = -1;
	int dir_fd = -1;
	int fd = -1;
	struct ifs_createfile_ex cex = {};

	enum ifs_security_info secinfo = 0;
	struct ifs_security_descriptor *sd = NULL;

	enum object_create_state create_state = OBJECT_CREATE_STATE_NONE;
	struct persona *owner = NULL;

	ASSERT(error_out);

	struct fmt FMT_INIT_CLEAN(opath);
	struct fmt FMT_INIT_CLEAN(hash_path);

	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	/* license check */
	if (iobj_is_license_valid() == false) {
		error = ostore_invalid_license_error_new("invalid license");
		goto out;
	}

	/*
	 * We will make the assumption that the collision namespace is not
	 * sparse, i.e. if a object that was one of the collision set was
	 * deleted, the highest existing collision object will be renamed to
	 * the deleted object's name.
	 */

	/*
	 * First let's see if the object path can be created directly.
	 */
	collision_possible = hash_object_name(&hash_path, iobj_name,
	    OSTORE_HASH_ALL, params);
	fmt_print(&opath, "%s/%s", ibh->base.path, fmt_string(&hash_path));

	// shared bucket lock
	{
		struct isi_file_lock SCOPED_SH_FILE_LOCK(lb, ibh->base.path,
							    &error);
		if (error)
			goto out;

		if (!collision_possible) {
			ostore_create_default_acl(&sd,  OSTORE_ACL_OBJECT,
			    owner, &error);
			if (error)
				goto out;
			secinfo = IFS_SEC_INFO_DACL;
		}

		cex.secinfo = secinfo;
		cex.sd = sd;
		dir_fd = ifs_createfile(-1, fmt_string(&opath),
		    IFS_RTS_DIR_GEN_WRITE, O_CREAT|O_DIRECTORY|O_EXCL|O_EXLOCK,
		    OSTORE_OBJECT_DIR_DEFAULT_MODE, &cex);

		if (dir_fd >= 0) {
			/*
			 * The only missing directory in opath was the last
			 * element (object dir for the non-collision case,
			 * collision dir for the collision case).  We have
			 * created it and taken out an exclusive lock on it,
			 * flag that we have it locked.
			 */
			dir_locked = true;
 		} else {
			/*
			 * couldn't create that last piece of the directory,
			 * check out whether it was because it already existed
			 * or parts of the path where missing and handle
			 * accordingly.
			 */
			switch (errno) {
			case EEXIST:
				/*
				 * There is a directory for this path already
				 * existing, but we need to verify it was for
				 * the object name specified.  If it was then
				 * we are done.  If not then we need a unique
				 * name.  Check the object name under an
				 * exclusive lock on the object directory
				 * to be sure we don't get here between the
				 * directory create and the name being applied
				 */
				dir_fd = open(fmt_string(&opath),
				    (OSTORE_OBJECT_DIR_OPEN_FLAGS|O_EXLOCK), 0);
				if (dir_fd == -1) {
					error = isi_system_error_new(errno);
					goto out;
				}
				dir_locked = true;

				/*
				 * If we can have collisions, then the collision
				 * directory may well exist. Don't check for
				 * object here, kick out for the collision
				 * case later where we will look for/create
				 * the object directory under this collision
				 * directory.  Note that we have collision
				 * directory opened with a exclusive lock at
				 * this point.
				 */
				if (collision_possible)
					break;

				/*
				 * not a collision so we have the object
				 * directory, need to veryify the object
				 * name.
				 */
				object_exists = verify_object_name(&opath,
				    dir_fd, iobj_name, 	&error);
				if (error)
					goto out;

				if (object_exists) {
					if (flags &
					    OSTORE_ENFORCE_SINGLE_OBJECT) {
						error = ostore_object_exists_error_new(
							"Object %s in bucket "
							"%s already exists, "
							"cannot create it",
							iobj_name,
							ibh->base.name);
						goto out;
					}
				} else {
					error = ostore_error_new(
						"Object name %s does not match "
						"expected name for the object "
						"directory %s for bucket %s",
						iobj_name, fmt_string(&opath),
						ibh->base.name);
					goto out;
				}
				break;

			case ENOENT:
				/*
				 * could not create the directory due to
				 * missing path components. Generate the
				 * directory. For non-collision case this
				 * is the object directory.  For the case
				 * where collisions can occur, this is the
				 * collision directory.  mkdir_dash_p will
				 * return with the fd (dir_fd) of the opened
				 * directory with an exclusive lock held on
				 * it.
				 */
				fmt_truncate(&opath);
				fmt_print(&opath, "%s", ibh->base.path);
				mkdir_dash_p(&opath, fmt_string(&hash_path),
				    OSTORE_OBJECT_DIR_DEFAULT_MODE, attrs, NULL,
				    OSTORE_ACL_OBJECT_HASH,
				    (collision_possible ?
					OSTORE_ACL_OBJECT_HASH :
					OSTORE_ACL_OBJECT),
				    &dir_fd, owner, &error);
				if (error)
					goto out;
				dir_locked = true;
				break;

			default:
				error = isi_system_error_new(errno,
				    "Cannot create object %s in bucket %s",
				    iobj_name, ibh->base.name);
				goto out;
				break;
			}
		}

		/*
		 * so now handle the case of a collision directory.  For this
		 * case we have the standard two levels of hashing and then
		 * the collision directory.  We still need to add the object
		 * directory.
		 */
		if (collision_possible) {
			/*
			 * to hold the name of any found object
			 */
			struct fmt FMT_INIT_CLEAN(object);

			/*
			 * The ifs_createfile above (or mkdir_dash_p)
			 * has returned us a file descriptor for the collision
			 * directory with that directory holding an exclusive
			 * lock.  This means that we can safely look for the
			 * proper object dir (if it exists).  Save the
			 * collision directory fd and reset the object
			 * directory fd.
			 */
			col_fd = dir_fd;
			dir_fd = -1;

			/*
			 * look for an existing object, returning the fd if
			 * one is found (dir_fd).
			 */
			object_exists = find_object_name(fmt_string(&opath),
			    iobj_name, &dir_fd, &object, &error);
			if (error)
				goto out;

			if (object_exists) {
				/*
				 * Did find an object with this name, so check
				 * to see if we can recreate the object.
				 */
				if (flags & OSTORE_ENFORCE_SINGLE_OBJECT) {
					error = ostore_object_exists_error_new(
							"Object %s in bucket "
							"%s already exists, "
							"cannot create it",
							iobj_name,
							ibh->base.name);
					close(dir_fd);
					dir_fd = -1;
					goto out;
				}

				/*
				 * Found an existing object that we will be
				 * replacing, so close the collision dir
				 * (which will drop the lock) and then set
				 * dir_fd to the fd of the found object and
				 * update opath to be the complete path to the
				 * object directory.
				 */
				close(col_fd);
				col_fd = -1;
				dir_locked = false;

				/*
				 * Update the objectpath to include the object
				 * directory found.
				 */
				fmt_print(&opath, "/%s", fmt_string(&object));
			} else {
				/*
				 * There is no existing object with this name
				 * so go ahead and create a new uniquely named
				 * object directory for the object.  Note that
				 * we will hold the collision directory lock
				 * until we get a name on it. At that point we
				 * will also close the collision direction and
				 * reset dir_fd to the object directory.
				 */
				fmt_print(&opath, "/%s",
				    IOBJECT_COLLISION_OBJDIR_TEMPLATE);
				dir_template = fmt_detach(&opath);
				object_dir = mkdtemp(dir_template);
				if (!object_dir) {
					error = isi_system_error_new(errno,
					    "Cannot create object directory "
					    "in %s for name collision on "
					    "object %s",
					    dir_template, iobj_name);
					goto out;
				}
				fmt_print(&opath, "%s", object_dir);
				dir_fd = open(object_dir,
				    OSTORE_OBJECT_DIR_OPEN_FLAGS, 0);
				if (dir_fd < 0) {
					error = isi_system_error_new(errno);
					goto out;
				}

				/*
				 * Since we have the object directory for the
				 * collision case set the object level acl
				 * on this directory.
				 */
				ostore_set_default_acl(NULL, dir_fd,
				    OSTORE_ACL_OBJECT, owner, &error);
				if (error)
					goto out;
			}
		}


		/*
		 * set the object name on the directory using the fd rather
		 * than the path since we already have it open.
		 */
		if (!object_exists){
			set_object_name(NULL, dir_fd, iobj_name, &error);
			if (error)
				goto out;
		}

		/*
		 * Time to release the lock we have held. For the case of
		 * possible file name collisions, we have the collision
		 * directory (col_fd) locked. For the case of where there
		 * would not be a collision on file names, we have the object
		 * directory (dir_fd) locked.  For the former case we just
		 * close the collision directory and therefore unlock. For the
		 * latter case we need the object directory to be open (dir_fd
		 * goes into object handle) so we just unlock the object
		 * directory.
		 */
		if (dir_locked) {
			if (collision_possible) {
				/*
				 * if we created a new object directory under
				 * the locked collision directory, unlock
				 * the collision dir by closing it.
				 */
				if (col_fd != -1) {
					close(col_fd);
					col_fd = -1;
					dir_locked = false;
				}
			} else {
				/*
				 * If we are still holding a lock on the
				 * object directory, just unlock it.
				 */
				if(flock(dir_fd, LOCK_UN) == -1) {
					error = isi_system_error_new(errno,
					    "Releasing object directory lock");
					goto out;
				}
				dir_locked = false;
			}
		}

		create_state = OBJECT_CREATE_STATE_DIR;

		/*
		 * Now that we have the object directory open, we need to
		 * create the object file itself.   We always create it with a
		 * temporary name and rename to it's real name with the
		 * commit of the object.
		 */
		fd = temporary_object_create(flags, iobj_name, ibh,
		    fmt_string(&opath), dir_fd, &tmp_obj_name, &error);
		if (error)
			goto out;

		create_state = OBJECT_CREATE_STATE_TMP;

#ifdef ADD_ATTRS
		/*
		 * Now add the attributes specified as attributes to the file.
		 */
		if (attrs) {
			ISI_HASH_FOREACH(entry, &attrs->hash, hash_entry) {
				status = ifs_userattr_set(fd, entry->key.data,
				    entry->value, strlen(entry->value) + 1);
				if (status) {
					error = isi_system_error_new(errno,
					    "Cannot set attributes for object "
					    "%s in bucket %s",
					    iobj_name, ibh->base.name);
					goto out;
				}
			}
		}
#endif
		create_state = OBJECT_CREATE_STATE_ATTR;
	}

	/*
	 * Now create the object handle
	 */
	iobj = object_handle_create(ibh->base.ostore, ibh, iobj_name,
	    fmt_string(&opath), fd, dir_fd, flags, created_object_name(flags),
	    tmp_obj_name);

 out:
	if (sd)
		ostore_release_acl(sd);

	/*
	 * Be sure that if the dir_fd is valid, we never leave here with it
	 * locked.
	 */
	if (dir_locked && dir_fd != -1)
		flock(dir_fd, LOCK_UN);

	/*
	 * Be sure collision dir is closed if we used it (and will guarantee it
	 * is unlocked).
	 */
	if (col_fd != -1)
		close(col_fd);

	if (error) {
		isi_error_handle(error, error_out);
		switch(create_state) {
		case OBJECT_CREATE_STATE_ATTR:
		case OBJECT_CREATE_STATE_TMP:
			if (fd != -1)
				close(fd);
			enc_unlinkat(dir_fd, tmp_obj_name, ENC_DEFAULT, 0);
		case OBJECT_CREATE_STATE_DIR:
			/*
			 * NB: Do we need to worry about a race condition here
			 * where another object_create for the same object is
			 * in progress while we fail this one?
			 */
			rmdir(fmt_string(&opath));
			if (tmp_obj_name)
				free(tmp_obj_name);
			break;
		default:
			break;
		}
		if (dir_fd != -1) {
			close(dir_fd);
			dir_fd = -1;
		}
	}

	if (dir_template)
		free(dir_template);

	/*
	 * return the object handle on success and a NULL on failure where
	 * error_out should contain the error details.
	 */
	return iobj;
}

static bool
get_object_path(struct ibucket_handle *ibh,
    const char *iobj_name,
    struct fmt *opath,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(hash_path);
	struct fmt FMT_INIT_CLEAN(oname);
	struct ostore_parameters *params = ibh->base.ostore->params;
	bool collision_possible = false;
	bool object_found = false;

	ASSERT(error_out);

	/*
	 * Get the hash path separately since we will reuse it
	 */
	collision_possible = hash_object_name(&hash_path, iobj_name,
	    OSTORE_HASH_ALL, params);

	fmt_print(opath, "%s/%s", ibh->base.path, fmt_string(&hash_path));

	/*
	 * First let's try the case where we have no object collisions
	 * expected and see if the object path can be accessed directly.
	 */
	if (!collision_possible) {
		object_found = verify_object_name(opath, -1, iobj_name, &error);
		if (error)
			goto out;
	} else {

		/*
		 *  we could have a collision so use the find_object_name
		 * to check for the object.
		 */
		object_found = find_object_name(fmt_string(opath), iobj_name,
		    NULL,  &oname, &error);
		if (error)
			goto out;

		fmt_print(opath, "/%s", fmt_string(&oname));
	}

 out:
	if (error)
		isi_error_handle(error, error_out);

	return object_found;
}


/**
 * Open an object in the bucket corresponding to the bucket handle give, using
 * the flags to specify the read/write characteristics.  On success the object
 * handle is returned otherwise a NULL is returned and error is set
 * appropriately.
 * @param ibh bucket handle for a currently opened bucket
 * @param name name for the object to be opened
 * @param flags flags specifying read or write along with characteristics of
 * those operations
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *ob_object_open(struct ibucket_handle *ibh,
    const char *iobj_name,  enum ifs_ace_rights ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobject_handle *iobj = NULL;
	struct fmt FMT_INIT_CLEAN(opath);
	bool found = false;
	int dir_fd = -1;
	int fd = -1;

	ASSERT(error_out);

	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	found = get_object_path(ibh, iobj_name, &opath, &error);
	if (error)
		goto out;

	if (!found) {
		error = ostore_object_not_found_error_new(
			"Cannot find path for object %s in bucket %s",
			iobj_name, ibh->base.name);
		goto out;
	}

	/*
	 * hold the directory open so that the directory cannot be removed
	 * while the object file is being accessed.
	 */
	dir_fd = open(fmt_string(&opath), OSTORE_OBJECT_DIR_OPEN_FLAGS, 0);
	if (dir_fd == -1) {
		switch(errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
				"Cannot open object %s in bucket %s, "
				"object does not exist",
				iobj_name, ibh->base.name);
			break;

		default:
			error = isi_system_error_new(errno,
			    "Cannot open object %s in bucket %s, "
			    "path %s for flags %s",
			    iobj_name, ibh->base.name, fmt_string(&opath),
			    fmt_iobj_object_open_flags(flags));
			break;
		}
		goto out;
	}

	fd = enc_openat(dir_fd, open_mode_extension(flags), ENC_DEFAULT,
	    open_mode_flags(flags), OSTORE_OBJECT_DEFAULT_MODE);

	if (fd == -1) {
		switch(errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
				"Cannot open object %s in bucket %s, "
				"object does not exist",
				iobj_name, ibh->base.name);
			break;

		default:
			error = isi_system_error_new(errno,
			    "Cannot open object %s in bucket %s, "
			    "path %s for flags %s",
			    iobj_name, ibh->base.name, fmt_string(&opath),
			    fmt_iobj_object_open_flags(flags));
			break;
		}
		goto out;
	}

	iobj = object_handle_create(ibh->base.ostore, ibh, iobj_name,
	    fmt_string(&opath), fd,
	    dir_fd, flags, created_object_name(flags), NULL);

 out:
	if (error) {
		if (fd != -1)
			close(fd);
		if (dir_fd != -1)
			close(dir_fd);

		isi_error_handle(error, error_out);
	}

	/*
	 * return the object handle on success and a NULL on failure where
	 * error_out should contain the error details.
	 */
	return iobj;
}

/**
 * return values for iobj_iobject_empty
 */
enum path_empty_vals {
	IOBJECT_PATHEMPTY_NO_ENTRY   = -1, /*< entry does not exist */
	IOBJECT_PATHEMPTY_NOT_EMPTY =  0,  /*< entry is not empty */
	IOBJECT_PATHEMPTY_EMPTY     =  1   /*< the entry is considered empty */
};

/** An object dir is non-empty if it has more than the following entries:
 * 		.
 * 		..
 * 		obj or another-dir
 *  Otherwise it is considered empty.
 *
 * returns enum values above (path_empty_vals)
 */
static enum path_empty_vals
iobj_iobject_empty(const char * object_dir_name, bool consider_empty,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	DIR *dp;
	struct dirent curr_ent;
	struct dirent *ep = 0;
	int count_entries = 0;
	enum path_empty_vals empty_object = IOBJECT_PATHEMPTY_NOT_EMPTY;
	int ret;

	ASSERT(error_out);

	dp = opendir(object_dir_name);
	if (!dp) {
		if (errno == ENOENT) {
			empty_object = IOBJECT_PATHEMPTY_NO_ENTRY;
			goto out;
		}

		error = isi_system_error_new(errno,
		        "opendir() error at %s",  object_dir_name);
		goto out;
	}

	// check for entries
	while((ret = readdir_r(dp, &curr_ent, &ep)) == 0) {
		if (ep == 0) { // end of dir
			empty_object = IOBJECT_PATHEMPTY_EMPTY;
			goto out;
		}
		/*
		 * if this is not a directory, then if we are planning on
		 * deleting all object files in the directory (delete of the
		 * object itself), we don't want to count any of the
		 * object files or tmp object files that might be there as
		 * going against the object being empty
		 */
		if (consider_empty && curr_ent.d_type == DT_REG)
			continue;
		if (++count_entries > 3) {
			empty_object = IOBJECT_PATHEMPTY_NOT_EMPTY;
			goto out;
		}
	}
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "readdir_r() error at %s", object_dir_name);
		goto out;
	}


 out:
	if (dp)
		closedir(dp);

	if (error)
		isi_error_handle(error, error_out);

	return empty_object;
}


/**
 * Remove an "empty" (see above) object
 *
 * This function  makes a structural change and needs to be transaction safe.
 * This is done by ensuring
 * 	a) identifying what path elements needs to be removed
 * 	b) actually removing them
 * is done with an exclusive lock on the bucket holding the object.
 * Moreover b) needs to be a single atomic operation.
 */
static struct isi_error *
iobj_iobject_remove(const char *objpath, struct ibucket_handle *ibh,
    const char *iobj_name, const char *target_name)
{
	struct isi_error *error = NULL;
	const char *path;
	char *path_end = 0;
	char *store_tmp_xxx = NULL;
	char *renamed_path;
	int level = 0, ret;
	size_t new_object_path_len = 0, store_tmp_len = 0;
	enum path_empty_vals path_empty;
	bool found = false;
	bool tmp_removal_needed = false;
	bool consider_level_empty = false;
	bool delete_all_obj_found = false;
	size_t bucket_path_len;

	struct fmt FMT_INIT_CLEAN(test_path);
	struct fmt FMT_INIT_CLEAN(remove_path);
	struct fmt FMT_INIT_CLEAN(store_tmp);
	struct fmt FMT_INIT_CLEAN(object_path);

	bucket_path_len = strlen(ibh->base.path);

	{
		/*
		 * exclusive bucket lock to block creates and commits.  We'll
		 * bracket this here to allow for unlock before the temp dir
		 * is removed.
		 */
		struct isi_file_lock SCOPED_EX_FILE_LOCK(lb, ibh->base.path,
		    &error);
		if (error)
			goto out;

		if (!objpath) {
			// Get the object directory
			found = get_object_path(ibh, iobj_name, &object_path,
			    &error);
			if (error)
				goto out;
			if (!found) {
				error = ostore_object_not_found_error_new(
					"Object %s not found in bucket %s "
					"during delete attempt",
					iobj_name, ibh->base.name);
				goto out;
			}
		} else {
			fmt_print(&object_path, "%s", objpath);
		}

		if (target_name)
			fmt_print(&object_path, "/%s", target_name);
		else {
			delete_all_obj_found = true;
			consider_level_empty = true;
		}

		fmt_print(&test_path, "%s", fmt_string(&object_path));
		path = fmt_string(&test_path);

		// printf("\ntest path is %s\n", path);

		// find longest empty path
		do {
			if (consider_level_empty) {
				new_object_path_len = strlen(path);
			} else {
				path_end = strrchr(path, '/');
				new_object_path_len = path_end - path;
				{
					struct fmt_scan_blotter blot;

					fmt_scan_blot_init(&blot);
					fmt_scan_blot_ahead(&test_path, &blot,
					    new_object_path_len);
				}
			}

			path_empty = iobj_iobject_empty(path,
			    consider_level_empty, &error);
			consider_level_empty = false;

			if (error)
				goto out;

			/*
			 * There is something there so we stop
			 */
			if (path_empty == IOBJECT_PATHEMPTY_NOT_EMPTY)
				break;

			/*
			 * If we find that part of the path is non-existent,
			 * then we stop as someone else has been here.
			 */
			if (path_empty == IOBJECT_PATHEMPTY_NO_ENTRY)
				goto out;

			// go up the object tree
			level++;
		} while (new_object_path_len > bucket_path_len);

		*path_end = '/';

		// check path level
		if (level == 0) {// obj dir is not empty
			if (target_name) {
				// remove just this temp file:
				ret = remove(path);
				if (ret) {
					error  = isi_system_error_new(errno,
					    "remove (%s) failed", path);
				}
			}
			else
				error = ostore_error_new(
				    "Object %s is not empty", iobj_name);
			goto out;
		}

		/*
		 * perform the removal
		 */

		fmt_print(&store_tmp, "%s/", OBJECT_STORE_TMP);
		store_tmp_len = strlen(fmt_string(&store_tmp));
		fmt_print(&store_tmp, "%s", "temp.XXXXXX");
		store_tmp_xxx = fmt_detach(&store_tmp);
		renamed_path = mkdtemp(store_tmp_xxx);
		ret = rename(path, renamed_path);
		if (ret) {
			error  = isi_system_error_new(errno,
			    "rename (%s to tmp)  failed", path);
			goto out;
		}

		// find the complete temp path to remove
		fmt_truncate(&remove_path);
		fmt_print(&remove_path, "%s%s", renamed_path,
		    fmt_string(&object_path) + strlen(fmt_string(&test_path)));

		tmp_removal_needed = true;
	}
 out:

	if (tmp_removal_needed) {
		int ret;

		renamed_path[store_tmp_len] = '\0';  // truncate at "%s/tmp/"
		char * remove_path_str = fmt_detach(&remove_path);
		if (delete_all_obj_found) {
			remove_all_object_entries(remove_path_str, NULL,
			    &error);
			if (error) {
				free(remove_path_str);
				goto done;
			}
		}

		ret = remove_path_until(remove_path_str, renamed_path,
		    &error);
		if (ret) {
			ilog(IL_NOTICE, "Failed to remove path %s errno=%d",
			    remove_path_str, ret);
		}
		free(remove_path_str);
	}
 done:
 	if (store_tmp_xxx)
		free(store_tmp_xxx);

	return error;
}

struct isi_error *
ob_object_delete(struct ibucket_handle *ibh, const char *iobj_name)
{
	struct isi_error *error = NULL;

	IOBJ_CHECK_BUCKET_HANDLE(ibh, error, out);

	// Arg check (handle checked above)
	if (!iobj_name) {
		error = ostore_invalid_argument_error_new(
			"iobj_object_delete: object name not specified");
		goto out;
	}

	/*
	 * Traverse up through the object dir to the highest level directory
	 * that is empty, and perform the removal. Since the last argument is
	 * not specified to iobj_iobject_remove, all object files and
	 * temp object files will be removed.
	 */
	error = iobj_iobject_remove(NULL, ibh, iobj_name, NULL);
	if (error)
		goto out;

 out:

	return error;
}

/*
 * close of an object will always return true unless an operation would mean
 * data was not in the object.  If clean up of the temp file fails, we will
 * report the error status in "error" but the the function will return true
 * (success) since the handle will be destroyed (and fd's closed).
 */
bool
ob_object_close(struct iobject_handle *iobj, struct isi_error **error_out)
{
	bool succeed = true;
	struct isi_error *error = NULL;
	struct isi_error *error2 = NULL;

	ASSERT(error_out);

	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	if (iobj->temp_obj_name && !iobj->committed) {
		error = iobj_iobject_remove(iobj->base.path,
		    iobj->bucket, iobj->base.name,
		    iobj->temp_obj_name);
		/*
		 * We don't go to out here because we still want to
		 * clean up the object handle. We'll handle the error below.
		 */
	} else if (fsync(iobj->base.fd)) {
		error = isi_system_error_new(errno,
		    "Cannot sync object to disk during commit for "
		    "object %s in bucket %s\n",
		    iobj->base.name,
		    iobj->bucket->base.name);
		/*
		 * We don't go to out here because we still want to
		 * clean up the object handle. We'll handle the error below.
		 */

	}

	/*
	 * clean up the object handle
	 */
	object_handle_destroy(iobj, &error2);
	/*
	 * Since the error from remove/fsync is more important than the error
	 * from destroy, we will use it rather than this one.
	 */
	if (error && error2)
		isi_error_free(error2);
	else if (!error)
		error = error2;

 out:
	if (error)
		isi_error_handle(error, error_out);

	return succeed;
}

/**
 * Commit the object, that is move the object that was created in a temporary
 * name to it's real name.   This will make the object available for access on
 * a following open access.  Until the commit occurs, the object does not
 * exist.  For the case where we are allowing multiple object creations (one
 * object replacing another of the same name, an previously committed object
 * will be opened until the commit happens to the newly created object.  If a
 * close happens on the newly created object before the commit happens, the
 * created object will not exist.  If multiple creates are happening before a
 * commit, the last commit will determine with object will ultimately exist.
 */
void
ob_object_commit(struct iobject_handle *iobj, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out);
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	/*
	 * move temporary file to the permanent file location.  If there is
	 * the case where more than one create is going on for the same object
	 * at the same time, only one will win and that will be the last one
	 * that does the commit.
	 */

	if (iobj->temp_obj_name && !iobj->committed) {

		struct isi_file_lock SCOPED_SH_FILE_LOCK(lb,
		    iobj->bucket->base.path,
		    &error);

		/*
		 * if we are about to make this object available, all I/O that
		 * has been performed should be forced to disk...
		 */
		if (fsync(iobj->base.fd)) {
			error = isi_system_error_new(errno,
			    "Cannot sync object to disk during commit for "
			    "object %s in bucket %s\n",
			    iobj->base.name,
			    iobj->bucket->base.name);
			goto out;
		}

		/*
		 * Do the rename to commit the object to the bucket
		 */
		if(!enc_renameat(iobj->dir_fd, iobj->temp_obj_name,
			ENC_DEFAULT, iobj->dir_fd, iobj->target_obj_name,
			ENC_DEFAULT))
			iobj->committed = true;
		else {
			/*
			 * if we get ENOENT back then the object must have
			 * deleted before we can do the commit, so nothing
			 * else to do.
			 */
			if (errno != ENOENT) {
				error = isi_system_error_new(errno,
				    "Cannot rename tempfile on commit for "
				    "object %s in bucket %s\n",
				    iobj->base.name,
				    iobj->bucket->base.name);
				goto out;
			}
		}
	}

 out:
	if (error)
		isi_error_handle(error, error_out);

}

ssize_t
ob_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error_out)
{
	return object_read(iobj, offset, buf, size, error_out);
}

ssize_t
ob_object_write(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error_out)
{
	/* license check */
	if (iobj_is_license_valid() == false) {
		*error_out = ostore_invalid_license_error_new(
		    "invalid license");
		return -1;
	}
	return object_write(iobj, offset, buf, size, error_out);
}

void
ob_object_dump(void)
{

	printf("Object dump not supported.\n");
}

#define OBJECT_DIR_LEVEL  2
#define OBJECT_DIR_LEVEL_COLLISION 3
/**
 * Recursively descend object directories and return object names
 *
 * Note: Since this operation can take a long time (presence of many
 * many objects in a bucket), we do it without any kind of locking.  The
 * consequence of doing that is that file system entries (directories and
 * files) comprising the object hierarchy could have been removed
 * (errno=ENOENT) while this operation is in progress.  Hence it needs to be
 * capable of handling such scenarios.
 */
static size_t
list_object(int dir_fd, const char *rel_path, const char *abs_path, int level,
    iobj_list_obj_cb caller_cb,
    void *caller_context, bool stats_requested,
    bool uda_requested,
    bool *list_continue_out, struct isi_error **error_out)
{
	DIR *dp = NULL;
	struct dirent curr_ent;
	struct dirent *ep = 0;
	size_t count = 0;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(fmt_curr_path);
	const char *curr_path = NULL;
	bool list_continue = true;
	bool found = false;
	struct iobj_list_obj_info info = {0};

	int ret = -1;
	int fd = -1;

	bool self_found = false, parent_found = false;

	ASSERT(error_out);

	if (level)
		fmt_print(&fmt_curr_path, "%s/%s", abs_path, rel_path);
	else
		fmt_print(&fmt_curr_path, "%s", abs_path);
	curr_path = fmt_string(&fmt_curr_path);

	// Open dir
	fd = enc_openat(dir_fd, rel_path, ENC_DEFAULT, O_RDONLY);
	if (fd < 0) {
		if (errno == ENOTDIR)
			/*
			 * log the presence of an unexpected entry in a
			 * directory.
			 */
			ilog(IL_NOTICE,
			    "Found unexpected entry %s during object list, "
			    "expected directory",
			    curr_path);
		else if (errno != ENOENT && errno != EACCES)
			error = isi_system_error_new(errno,
			    "enc_openat() error at %s",  curr_path);
		goto out;
	}

	dp = fdopendir(fd);
	if (dp == NULL) {
		error = isi_system_error_new(errno,
		    "fdopendir() error at %s",  curr_path);
		goto out;
	}

	// Process entries
	while ((ret = readdir_r(dp, &curr_ent, &ep)) == 0) {

		if (ep == NULL || !list_continue) {
			// no more entries or need to list
			goto out;
		}

		if (ep->d_type != DT_DIR) {
			/*
			 * If we found something other than a directory, we
			 * don't process it here.
			 */
			ilog(IL_NOTICE,
			    "Found unexpected entry %s/%s during object list, "
			    "expected directory",
			    curr_path, ep->d_name);
			continue;
		}

		if (!self_found && strcmp(ep->d_name, ".") == 0) {
			self_found = true;
		} else if (!parent_found && strcmp(ep->d_name, "..") == 0) {
			parent_found = true;
		} else if (level < OBJECT_DIR_LEVEL) {
			// descend
			count += list_object(fd, ep->d_name, curr_path,
			    level + 1, caller_cb, caller_context,
			    stats_requested, uda_requested,
			    &list_continue, &error);
			if (error)
				goto out;
		} else if (level >= OBJECT_DIR_LEVEL &&
		    level <= OBJECT_DIR_LEVEL_COLLISION) {
			struct fmt FMT_INIT_CLEAN(obj_name);
			struct stat stats;
			struct stat *pstats = &stats;
			struct iobj_object_attrs *attr_list = NULL;
			int pfd = -1;
			int tfd = -1;

			// get the true object name
			pfd = enc_openat(fd, ep->d_name,
			    ENC_DEFAULT, OSTORE_OBJECT_OPEN_READ_FLAGS);

			/*
			 * no object directory found, so continue
			 */
			if (pfd == -1)
				continue;

			found = get_object_name(NULL, pfd, &obj_name, &error);
			if (error) {
				close(pfd);
				pfd = -1;
				goto out;
			}

			if (found) {
				/*
				 * this looks like an object directory,
				 * i.e. we found an object name associated
				 * with it, so look for a committed object
				 * file.  If we don't find a committed file
				 * then we are done with this entry.
				 */
				tfd = enc_openat(pfd, IOBJECT_OBJECT_NAME,
				    ENC_DEFAULT, OSTORE_OBJECT_OPEN_READ_FLAGS);

				close(pfd);
				pfd = -1;

				/* No object file found, so go on to the next */
				if (tfd == -1)
					continue;

				if (stats_requested)
					fstat(tfd, pstats);
				else
					pstats = NULL;


				if (uda_requested) {
					genobj_list_ext_attrs(tfd, &attr_list, &error);
					if (error) {
						close(tfd);
						tfd = -1;
						goto out;
					}
				}

				close(tfd);
				tfd = -1;

				/* bump the count since we did find one) */
				count++;

				info.attr_list = attr_list;
				info.cookie = 0;
				info.name = fmt_string(&obj_name);
				info.stats = pstats;
				info.object_type = OOT_OBJECT;
				info.id = (pstats ? pstats->st_ino : 0);
				info.container = NULL;

				// return the true object name
				list_continue = (*caller_cb)(caller_context,
				    &info);

				if (attr_list) {
					// free the attributes:
					genobj_attribute_list_release(
					    attr_list, &error);
					if (error)
						goto out;
				}

			} else {
				/*
				 * if there is no object name associated with
				 * this directory, then this may be a collision
				 * directory so dive down into it (after
				 * cleaning up the old fd).
				 */
				close(pfd);
				pfd = -1;
				count += list_object(fd, ep->d_name, curr_path,
				    level + 1, caller_cb,
				    caller_context, stats_requested,
				    uda_requested, &list_continue, &error);
				if (error)
					goto out;
			}

		} else {
			/*
			 * have exceeded the maximum depth of the object tree
			 * so cut it off here.   Don't want to return an error
			 * as that will stop the listing...
			 */
			ilog(IL_NOTICE,
			    "Found unexpected entry %s/%s during object list, "
			    "directory depth exceeds object depth",
			    curr_path, ep->d_name);
			goto out;
		}
	}
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "readdir_r() error at %s", curr_path);
		goto out;
	}

out:
	// Close dir
	if (dp) {
		if (closedir(dp)) {
			error = isi_system_error_new(errno,
			    "closedir() error at %s", curr_path);
		}
	} else if (fd > -1) {
		close(fd);
	}

	if (error)
		isi_error_handle(error, error_out);

	*list_continue_out = list_continue;

	return count;
}

/**
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param param iobj_list_obj_param structure control the behavior of the list
 *  @param error isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed
 */
size_t
ob_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t count = 0;
	bool list_continue = true;
	// Arg check
	ASSERT(param);
	ASSERT(error_out);

	struct ibucket_handle *ibh = param->ibh;

	IOBJ_CHECK_BUCKET_HANDLE(param->ibh, error, out);

	if (!ibh || !(ibh->base.path) || !param->caller_cb) {
		error = ostore_invalid_argument_error_new(
			"invalid args:iobj_object_list");
		goto out;
	}
	/*
	 * Start the listing at the bucket by using the special fd meaning /
	 * and the path to the bucket.
	 */
	count = list_object(AT_FDCWD, ibh->base.path, ibh->base.path, 0,
	    param->caller_cb, param->caller_context,
	    param->stats_requested,
	    param->uda_requested,
	    &list_continue,
	    &error);
 out:
	if (error)
		isi_error_handle(error, error_out);
	return count;
}
/** @} */
