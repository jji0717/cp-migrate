/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ns_common.h __ 2012/03/26 10:47:52 EDST __ lwang2
 */

#include "ns_common.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/queue.h>
#include <sys/stat.h>
#include <string.h>


#include <isi_domain/dom.h>
#include <isi_domain/dom_util.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include <isi_util/ifs_mounted.h>
#include <isi_util/enc.h>
#include <isi_acl/isi_acl_util.h>
#include <isi_util/syscalls.h>

#include "genobj_common.h"
#include "object_common.h"
#include "ostore_internal.h"
#include "genobj_acl.h"

#ifndef min
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif

static inline bool
is_special_dot_dir(const char *p)
{
	size_t len = strlen(p);
	if (len < 1)
		return false;

	return p[0] == '.' && (p[1] == '\0' || (len == 2 &&
	    p[1] == '.' && p[2] == '\0'));
}

/**
 * Utility to resolve a bucket to file system path
 */
static struct isi_error *
ns_resolve_object_path(struct ibucket_handle *ibh, const char *iobj_name,
    struct fmt *object_path)
{
	if (is_prohibited_dir(ibh->base.path, iobj_name)) {
		return ostore_pathname_prohibited_error_new(
		    "%s", iobj_name);
	}
	fmt_print(object_path, "%s/%s", ibh->base.path, iobj_name);
	return NULL;
}

static enum OSTORE_OBJECT_TYPE
object_type_from_dirent(const struct dirent *ent)
{
	uint16_t otype = ent->d_type;
	switch (otype) {
	case DT_FIFO:
		return OOT_FIFO;
	case DT_CHR:
		return OOT_CHR;
	case DT_DIR:
		return OOT_CONTAINER;
	case DT_BLK:
		return OOT_BLK;
	case DT_REG:
		return OOT_OBJECT;
	case DT_LNK:
		return OOT_LNK;
	case DT_SOCK:
		return OOT_SOCK;
	case DT_WHT:
		return OOT_WHT;
	default:
		return OOT_INVALID;
	}
}

static enum OSTORE_SYMBLNK_TYPE
symblnk_type_from_dirent(int parent_fd, const struct dirent *ent)
{
	struct stat st;
	int fd = -1;
	enum OSTORE_SYMBLNK_TYPE type = OLT_NONE;

	if (ent->d_type != DT_LNK)
		goto out;

	// open so to follow the symbolic link chain
	fd = enc_openat(parent_fd, ent->d_name, ent->d_encoding, O_RDONLY);
	if (fd < 0 || fstat(fd, &st) == -1)
		goto out;

	if (S_ISDIR(st.st_mode))
		type = OLT_DIR;
	else if (S_ISREG(st.st_mode))
		type = OLT_FILE;

 out:
	if (fd >= 0)
		close(fd);

	return type;
}

/*
 * Create an object in the bucket corresponding to the bucket handle given,
 * using @a c_mode to specify the read/write characteristics and the given
 * attributes.  On success the object handle is returned otherwise a NULL is
 * returned and error is set appropriately.
 * @param ibh bucket handle for a currently opened bucket
 * @param iobj_name name for the object to be opened
 * @param flags  not used
 * @param c_mode permission for the object
 * @param attributes list of key/value pairs that make up the attributes for
 * the object
 * @param acl ACL to be set on the object.
 * @param error isi_error structure, NULL on success, set on failure.
 */
struct iobject_handle *
ns_object_create(
    struct ibucket_handle *ibh, 
    const char *iobj_name, 
    int flags, mode_t c_mode,
    struct iobj_object_attrs *attributes, 
    enum ostore_acl_mode acl_mode, 
    bool overwrite,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobject_handle *iobj = NULL;
	struct fmt FMT_INIT_CLEAN(opath);
	struct fmt FMT_INIT_CLEAN(temp_path);
	char *tmpfile_template = NULL;
	int fd = -1;

	if ((error = ns_resolve_object_path(ibh, iobj_name, &opath)))
		goto out;

	// Check if the file already exists and if the overwrite 
	// flag is set. If overwrite flag is set, then replace the 
	// file else return error.

	if (!overwrite) {
		fd = ifs_createfile(-1, fmt_string(&opath), 
		    0, 0, c_mode, NULL);
		
		// If overwrite flag is not specified, and the file
		// exists, return error.
		if (fd >= 0 || (fd < 0 && 
		    (errno == EACCES || errno == EISDIR || 
		    errno == EEXIST || errno == ENOTEMPTY))) {
			if (fd >= 0) {
				close(fd);
				fd = -1;
			}
			error = ostore_object_exists_error_new(
 				"Unable to put file. File %s already exists", 
 				fmt_string(&opath));
 			goto out;
		}

	}

	// tmp file name length needs to be less NAME_MAX
	fmt_print(&temp_path, "%s/%.*s.XXXXXX", ibh->base.path,
	    NAME_MAX - (int) strlen(".XXXXXX"), iobj_name);
	tmpfile_template = fmt_detach(&temp_path);
	fd = mkstemp(tmpfile_template); // 0600 is default mode here

	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Cannot create object %s in bucket %s, path %s",
		    iobj_name, ibh->base.name,
		    fmt_string(&opath));
		goto out;
	}

	iobj = object_handle_create(ibh->base.ostore, ibh, iobj_name,
	    fmt_string(&opath), fd, -1, flags, NULL,
	    tmpfile_template);

	iobj->c_mode = c_mode;
	iobj->acl_mode = acl_mode;
out:
	if (error) {
		if (fd >= 0) {
			unlink(tmpfile_template);
			close(fd);
		}
		if (tmpfile_template)
			free(tmpfile_template);

		isi_error_handle(error, error_out);
	}

	return iobj;
}

/*
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
struct iobject_handle *
ns_object_open(struct ibucket_handle *ibh, 
    const char *iobj_name, enum ifs_ace_rights ace_rights_open_flg,
    int flags, struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct iobject_handle *io_handle = NULL;
	struct fmt FMT_INIT_CLEAN(opath);
	int fd = -1;
	int dir_fd = -1;
	struct ifs_createfile_ex cex = {};

	if ((error = ns_resolve_object_path(ibh, iobj_name, &opath)))
		goto out;

	cex.cf_flags = cf_flags;
	fd = ifs_createfile(-1, fmt_string(&opath), ace_rights_open_flg, flags,
	    0, &cex);

	if (fd == -1) {
		switch (errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
			    "Cannot open object %s in bucket %s, "
			    "object does not exist",
			    iobj_name, ibh->base.path);
			break;

		case EACCES:
			error = ostore_access_denied_error_new(
				"Access denied. Cannot open object %s "
				"in bucket %s.", iobj_name, ibh->base.path);
			break;						
		case EINVAL:
			error = ifs_createfile_invalid_arg_error();
			break;
		default:
			error = isi_system_error_new(errno,
			    "Cannot open object %s  with "
			    "path %s for flags %d",
			    iobj_name, fmt_string(&opath),
			    flags);
			break;
		}
		goto out;
	}

	if ((error = ensure_object_on_onefs(fd)))
		goto out;

	io_handle = object_handle_create(ibh->base.ostore,
	    ibh, iobj_name, fmt_string(&opath), fd,
	    dir_fd, flags, NULL, NULL);

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

	return io_handle;
}

static inline bool
access_denied(struct isi_error *error)
{
	return error && (isi_system_error_is_a(error, EACCES) ||
	    isi_error_is_a(error, OSTORE_ACCESS_DENIED_ERROR_CLASS));
}

/*
 * close of an object will always return true unless an operation would mean
 * data was not in the object.  If clean up of the temp file fails, we will
 * report the error status in "error" but the the function will return true
 * (success) since the handle will be destroyed (and fd's closed).
 */
bool
ns_object_close(struct iobject_handle *iobj, struct isi_error **error_out)
{
	bool succeed = true;
	struct isi_error *error = NULL;
	struct isi_error *error2 = NULL;
	int sfd = -1;

	ASSERT(error_out);

do_remove:	
	IOBJ_CHECK_OBJECT_HANDLE(iobj, error, out);

	if (iobj->temp_obj_name && !iobj->committed) {
		remove_path_until(iobj->temp_obj_name,
		    iobj->bucket->base.path,
		    &error);
		if (sfd == -1 && access_denied(error)) {
			isi_error_free(error);
			error = NULL;
			sfd = gettcred(NULL, true);
			if (sfd >= 0 && reverttcred() == 0)
				goto do_remove;
		}
	} else if (fsync(iobj->base.fd)) {
		error = isi_system_error_new(errno,
		    "Cannot sync object to disk during commit for "
		    "object %s in path %s\n",
		    iobj->base.name,
		    iobj->base.path);
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
 
	if (sfd >= 0) {
		settcred(sfd, TCRED_FLAG_THREAD, NULL);
		close(sfd);
	}

	if (error)
		isi_error_handle(error, error_out);

	return succeed;
}

void
ns_object_commit(struct iobject_handle *iobj, struct isi_error **error_out)
{
	int sfd = -1;
	struct stat st = {0};
	struct isi_error *error = NULL;
	ASSERT(iobj);

	if (iobj->temp_obj_name && !iobj->committed) {
		// Set acl only if acl_mode is specified.
		if (iobj->acl_mode > OSTORE_ACL_NONE) {
			// Set the acl on the file.
			ostore_set_default_acl(iobj->temp_obj_name, -1,
			    iobj->acl_mode, NULL, &error);
		} else if (iobj->c_mode != (mode_t) -1 &&
		    chmod(iobj->temp_obj_name, iobj->c_mode)) {
			// this is internal error ?
			error = isi_system_error_new(errno,
			    "cannot set permission mode");
		}

		// if we can't set the permissions, skip renaming
		if (error) {
			goto out;
		}

do_rename:
		if (rename(iobj->temp_obj_name, iobj->base.path) == -1) {
			switch(errno) {
			case EACCES:
				if (sfd == -1 && stat(iobj->base.path, &st) != 0 
				    && errno == ENOENT) {
					// we failed because the user does not
					// have delete permission for the rename
					// su and try again
					sfd = gettcred(NULL, true);
					if (sfd >= 0 && reverttcred() == 0)
						goto do_rename;
				}

				error = ostore_access_denied_error_new
				    ("Access denied to put file. Need write "
					"and related permissions to put file "
					"and delete/write related permissions "
					"to re-put file. Please refer to the "
					"documentation.");
				goto out;
				break;

			default:
				error = isi_system_error_new(errno,
				    "Cannot rename from temporary file to the "
				    "destination. source : %s, destination : "
				    "%s. errno: %d", iobj->temp_obj_name, 
				    iobj->base.path, errno);
				goto out;
				break;
			}
		}

		iobj->committed = true;
	}

out:
	if (sfd >= 0) {
		settcred(sfd, TCRED_FLAG_THREAD, NULL);
		close(sfd);
	}

	if (error)
		isi_error_handle(error, error_out);
}

ssize_t
ns_object_read(struct iobject_handle *iobj, off_t offset, void *buf,
    ssize_t size, struct isi_error **error_out)
{
	return object_read(iobj, offset, buf, size, error_out);
}

ssize_t
ns_object_write(struct iobject_handle *iobj, off_t offset, 
    void *buf, ssize_t size, struct isi_error **error_out)
{
	return object_write(iobj, offset, buf, size, error_out);
}


/**
 * Figure out the next cookie to use.
 */
static void
get_next_cookie(struct ns_list_obj_param *list_param)
{
	uint64_t cookie = 0;
	struct iobj_list_obj_param *param = list_param->user_param;
	if (param->cookie_count > list_param->current_depth) {
		cookie =  param->resume_cookie[list_param->current_depth];
	}

	list_param->resume_cookie = cookie;
}

/*
 * List the objects in a bucket.  The caller provides a callback that is
 * called for each object.  The execution happens in the callers context.
 *  @param list_param - ns_list_obj_param structure control list behavior
 *  @param error_out - isi_error structure, NULL on success, set on failure
 *
 *  @return number of objects listed
 */
size_t
nsp_object_list(struct ns_list_obj_param *list_param,
    struct isi_error **error_out)
{
	size_t count = 0;
	struct iobj_list_obj_param *param = list_param->user_param;
	struct isi_error *error = NULL;
	uint64_t *cookies_out = NULL;
	char *direntries_out = NULL;
	char *utf8fn = NULL;

	/*
	 * The access bits set are common to both directory and files.
	 * So the same access rights can be used for files too.
	 */
	enum ifs_ace_rights ace_rights = (IFS_RTS_DIR_LIST |
	    IFS_RTS_DIR_READ_EA | IFS_RTS_DIR_TRAVERSE |
	    IFS_RTS_DIR_READ_ATTRIBUTES);

	struct iobj_list_obj_info info = {0};

	int fd = list_param->fd;

	ASSERT (fd > 0, "The bucket must be opened first.");

	/*
	 * optimize for unsorted operation - return data in
	 * natural order returned from readdir, don't build
	 * up all entries, cannot return a total.
	 */

	unsigned used = 0;  // used readdirplus entries

	int max_depth = param->max_depth;
	unsigned prefetch_size = 1024/min(32, (max_depth < 0 ? 32 : max_depth + 1));
	size_t size_to_read = prefetch_size * sizeof(struct dirent);
	unsigned int rdp_avail = prefetch_size;

	cookies_out = (uint64_t *) malloc(prefetch_size *
	    sizeof(uint64_t));
	direntries_out = (char *) malloc(size_to_read);
	ASSERT(cookies_out && direntries_out);

	uint64_t resume_cookie  = list_param->resume_cookie;
	struct stat *stats = NULL;
	bool cookie_used = false; // indicate the cookie is used in this level

	if (param->stats_requested) {
		stats = (struct stat *) malloc(prefetch_size *
		    sizeof(struct stat));
		ASSERT(stats);
	}

	/*
	 * Param cookie count is used to indicate how many levels to
	 * traverse to resume the listing operation.
	 * Once the levels are traversed and the operation is resumed from
	 * the position, we no longer need to maintain or check against the
	 * param's cookie_count or resume_cookies array.
	 * Once resumed from the desired location based on cookie count/
	 * resume cookies array, the listing traversal can continue to process
	 * further sub-directories at lower levels beyond cookie count or
	 * can process sub-directories at higher levels than indicated by
	 * the cookie count.
	 *
	 * Check if we have traversed till the last cookie to resume the
	 * listing operation. If so, the resume cookie info is no longer
	 * needed to be checked.
	 * Bug # 176349.
	 */

	if (param->cookie_count > 0) {
		if (((list_param->current_depth + 1 ) >= param->cookie_count)
				&& (resume_cookie ==
						param->resume_cookie[param->cookie_count - 1])) {
					// Set the cookie count to 0 after this point,
					// during the traversal.
					// The param's resume_cookies parameter is untouched.
					// The cookie count is reset to the original value
					// when the call returns back to ns_object_list and the
					// calling routine will free the resume cookies array
					// if cookie count is non-zero.
					param->cookie_count = 0;
		}
	}

	while (list_param->list_continue) {
		/*
		 *  Reset rdp_avail everytime we call readdirplus
		 *  as it could potentially be changed within that call
		 *  resulting in an incorrect use of the buffers. In
		 *  addition make sure to pass the buffer size for the
		 *  dirent output instead of the number of elements.
		 */
		rdp_avail = prefetch_size;

		if (readdirplus(fd, RDP_NOTRANS|RDP_BACKUP,
		    &resume_cookie, 0,
		    &direntries_out[0], size_to_read,
		    &rdp_avail, stats, &cookies_out[0]) < 0) {
			error = isi_system_error_new(errno,
			    "Cannot read directory contents "
			    "in bucket %s\n",
			    param->ibh ? param->ibh->base.path : "");
			// TBD: do we need ibh besides path info?
			// note this path is only correct when depth > 0
			goto out;
		}

		if (rdp_avail == 0)
			break;

		char *ptr = &direntries_out[0];
		struct dirent *de;

		// process the entries read:
		for (used = 0, de = (struct dirent *)ptr;
		    used < rdp_avail && list_param->list_continue;
		    used++, ptr += de->d_reclen,
		    de = (struct dirent *)ptr) {
			struct stat *pstats = NULL;
			struct iobj_object_attrs *attr_list = NULL;

			if (param->stats_requested)
				pstats = stats + used;

			int tfd = -1;

			if (is_special_dot_dir(de->d_name))
				continue;

			if (list_param->skip_system_dir
			    && is_prohibited_dir(param->ibh->base.path,
			    de->d_name))
				continue;

			bool resuming = !cookie_used && (used == 0) &&
			    (param->cookie_count >
			    list_param->current_depth + 1);

			if (resuming) {
				// this must be directory
				if (de->d_type != DT_DIR) {
					error = ostore_error_new(
					    "Resume token is no longer valid.");
					if (tfd != -1)
						close(tfd);
					goto out;
				}
			}

			if (param->uda_requested) {
				tfd = ifs_createfile(fd, de->d_name, ace_rights,
				    0, 0, NULL);

				/* No object file found, so go on to the
				 * next */
				if (tfd == -1)
					continue;
				genobj_list_ext_attrs(tfd, &attr_list, &error);
				if (error) {
					close(tfd);
					tfd = -1;
					goto out;
				}
			}

			/*
			 * Convert the non-UTF characters to UTF-8 
			 * encoding.
			 */
			if (utf8fn) {
				free(utf8fn);
				utf8fn = NULL;
			}

			utf8fn = enc_utf8ify(de->d_name, de->d_encoding);

			// Details of the parent of the current sub-directory
			// that is being processed.
			info.attr_list = attr_list;
			info.cookie = cookies_out[used];
			// if conversion failed, then use as-is
			info.name = utf8fn ? utf8fn : de->d_name;
			info.stats = pstats;
			info.id = de->d_fileno;
			info.object_type = object_type_from_dirent(de);
			info.symblnk_type =
			    symblnk_type_from_dirent(fd, de);
			info.container = list_param->container;
			info.cur_depth = list_param->current_depth;

			if (de->d_type == DT_DIR &&
			    (list_param->current_depth < max_depth || max_depth < 0)) {
				//we are at an entry that is a directory that will be traversed
				//In order to resume while traversing it, we need a cookie
				//that will list the entry we are at as the first result
				//These cookies are offsets into the btree of entries
				//in the directory, and are used to tell readdirplus to
				//list results from that point on.
				//readdirplus returns offsets that are immediatly AFTER each
				//entry, so we can get the offset TO the entry by subtracting 1
				info.cookie = cookies_out[used] - 1;
			}

			if (!resuming) {
				// call the caller specified callback:
				// if resuming, user has already retrieved it
				list_param->list_continue = param->caller_cb(
				    param->caller_context,
				    &info);
			}

			++count;

			if (attr_list) {
				// free the attributes:
				genobj_attribute_list_release(
				    attr_list, &error);
				ASSERT(error == NULL);
			}

			if (list_param->list_continue && de->d_type == DT_DIR &&
			    (list_param->current_depth < max_depth || max_depth < 0)) {
				if (tfd == -1) {
					tfd = ifs_createfile(fd, de->d_name, 
					    ace_rights, O_DIRECTORY, 0, NULL);

					/* No object file found, so go on to the
					 * next */
					if (tfd == -1)
						continue;
				}

				// Details of the current sub-directory that needs traversal.
				struct ns_list_obj_param new_param = {0};
				new_param.user_param = param;
				new_param.current_depth =
				    list_param->current_depth + 1;
				new_param.fd = tfd;
				new_param.list_continue = true;
				new_param.container = &info;
				new_param.skip_system_dir =
				    list_param->skip_system_dir;

				// If the object listing resumes from a particular position,
				// we need to traverse to the correct hierarchy location in the
				// file structure before the listing operation starts/continues.
				// get_next_cookie is used to fetch the resume position at
				// each hierarchial level. We use this to get to the
				// resume position.
				// The param->cookie_count will indicate
				// if we are still resuming or the traversal is beyond resume.
				// If the traversal is beyond the resume position, then
				// proceed normally.
				if ((used == 0) && (!cookie_used) && (param->cookie_count > 0)) {
					get_next_cookie(&new_param);
					cookie_used = true;
				}

				count += nsp_object_list(&new_param, error_out);
				list_param->list_continue =
				    new_param.list_continue;
				// restore the cookie:
				info.cookie = cookies_out[used];
			}

			if (utf8fn) {
				free(utf8fn);
				utf8fn = NULL;
			}

			if (tfd != -1)
				close(tfd);
		}

		if (used >= 1)
			resume_cookie = cookies_out[used - 1];

		// process the next chunk
	}

out:
	if (error)
		isi_error_handle(error, error_out);

	if (cookies_out)
		free(cookies_out);

	if (direntries_out)
		free(direntries_out);
	if (stats)
		free(stats);

	if(utf8fn)
		free(utf8fn);

	return count;
}

size_t
ns_object_list(struct iobj_list_obj_param *param,
    struct isi_error **error_out)
{
	size_t count = 0;
	// Store the cookie count. It might be reset in the calling nsp_object_list.
	int cookie_cnt = param->cookie_count;
	uint64_t r_cookie = (param->cookie_count > 0 ? param->resume_cookie[0] : 0);
	struct ns_list_obj_param list_param = {
	    param, 0, param->ibh->base.fd, NULL, true, true,
	    r_cookie};
	count = nsp_object_list(&list_param, error_out);
	// Restore the cookie count. It is checked for object destruction at
	// the query handler.
	param->cookie_count = cookie_cnt;
	return count;
}


struct isi_error *
ns_object_delete(struct ibucket_handle *ibh,
    const char *iobj_name)
{
	return ostore_error_new("Operation supported via genobj_delete!");
}

void
ns_object_dump(void)
{
	
}

void
ns_object_set_worm(struct iobject_handle *iobj,
    const struct object_worm_set_arg *set_arg,
    struct isi_error **error_out)
{
	ASSERT(iobj && set_arg);
	struct isi_error *error = NULL;

	struct stat st;
	int fd = iobj->base.fd;
	struct timespec ts[3] = {};

	if (fstat(fd, &st) < 0) {
		error = isi_system_error_new(errno, "cannot stat object");
		goto out;
	}

	struct object_worm_attr worm_attr = {};
	struct object_worm_domain worm_dom = {};
	ns_genobj_get_worm(&(iobj->base), &worm_attr, &worm_dom, NULL, NULL,
	    &error);

	// Free domain root string allocated by ns_genobj_get_worm.
	free(worm_dom.domain_root);
	worm_dom.domain_root = NULL;

	if (!error && !worm_attr.w_is_valid) {
		error = isi_system_error_new(EOPNOTSUPP, "the object has no "
		    "associated SmartLock Root Directories");
	}

	if (error)
		goto out;

	if (set_arg->w_retention_date != (uint64_t) -1) {
		/*
		 * Bug 194466
		 * Use fvtimes to only set atime to adjust WORM
		 * retention.
		 */
		ts[0].tv_sec = set_arg->w_retention_date;
		ts[0].tv_nsec = 0;

		if (fvtimes(fd, ts, VT_ATIME)) {
			error = isi_system_error_new(errno, "cannot set "
			    "WORM retention date for the object");
			goto out;
		}
	}
	if (!set_arg->w_commit || worm_attr.w_attr.w_committed)
		goto out; // we are done here

	/* Bug 126339
	 * If file has an ACL, worm commit by setting the UF_DOS_RO flag */
	if ((st.st_flags & SF_HASNTFSACL) == SF_HASNTFSACL) {
		/* unset UF_DOS_RO if it's already there */
		if ((st.st_flags & UF_DOS_RO) == UF_DOS_RO) {
			if (fchflags(fd, st.st_flags & ~UF_DOS_RO)) {
				error = isi_system_error_new(errno,
				    "cannot commit the object to WORM");
				goto out;
			}
		}
		if (fchflags(fd, st.st_flags | UF_DOS_RO)) {
			error = isi_system_error_new(errno,
			    "cannot commit the object to WORM");
		}
		goto out;
	}

	mode_t wr_mode = st.st_mode & (S_IWUSR | S_IWOTH | S_IWGRP);
	if (!wr_mode) { // read only, then change it writable
		wr_mode = S_IWUSR;
		if (fchmod(fd, st.st_mode | wr_mode)) {
			error = isi_system_error_new(errno,
			    "cannot commit the object to WORM");
			goto out;
		}
	}
	if (fchmod(fd, st.st_mode & ~wr_mode)) {
		error = isi_system_error_new(errno,
		    "cannot commit the object to WORM");
		goto out;
	}

out:
	if (error)
		isi_error_handle(error, error_out);

}

