
/*-
 * Copyright (c) 1991, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <sys/cdefs.h>
__FBSDID("$FreeBSD$");

#include <ifs/ifs_types.h>
#include <ifs/ifs_restripe.h>
#include <ifs/ifs_userattr.h>
#include <ifs/bam/bam_pctl.h>

#include <sys/acl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <stdlib.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include <isi_acl/isi_acl_util.h>
#include <isi_sstore/sstore.h>
#include "genobj_common.h"
#include "fs_op_utils.h"
#include "ostore_internal.h"

/* One megabyte = 2^20 bytes*/
#define BIGBUFSIZE    1048576

/** cache of current copy target parent */
struct cp_parent_cache {
	char	path[PATH_MAX];
	int     pfd; // file or dir descriptor
};

/**
 * A wrapper to close file descriptor
 */
static void
close_fd(int fd)
{
	ASSERT(fd >= 0);
	int ret = 0;
	ret = close(fd);
	ASSERT(!ret);
}

/**
 * Routine to invoke client's callback function using information provided
 *
 * @param[IN] cp_param - parameter for copy operation
 * @param[IN] s        - severity of the message
 * @param[IN] on_dest  - whether the message is on destination side
 * @param[IN] src      - source pathname being copied from
 * @param[IN] dest     - destination file pathname
 * @return code returned from user provided callback
 */
static int
report_callback(struct fsu_copy_param *cp_param, enum fsu_copy_rpt_severity s,
    bool on_dest, const char *src, const char *dest, const char *msg)
{
	ASSERT(cp_param && src && dest && msg);

	if (s == COPY_RPT_ERR_INTERNAL) {
		ilog(IL_ERR, "copy src=%s, on_dest=%d, dest=%s, msg=%s",
		    src, on_dest, dest ? dest : "", msg);
	} else {
		ilog(IL_DEBUG, "copy src=%s, on_dest=%d, dest=%s, msg=%s",
		    src, on_dest, dest ? dest : "", msg);
	}

	if (!cp_param->cp_callback)
		return 0;

	struct fsu_copy_rpt rpt_info = {s, on_dest, src, dest, msg};
	return cp_param->cp_callback(cp_param->caller_context, &rpt_info);
}

/*
 * A variant of report_callback, but log error and assert for EBADF error
 * for easy debug
 */
static int
report_callback1(struct fsu_copy_param *cp_param, enum fsu_copy_rpt_severity s,
    bool on_dest, const char *src, const char *dest, int err_no)
{
	if (err_no == EBADF) {
		ilog(IL_ERR, "copy src=%s, on_dest=%d, dest=%s, msg=%s",
		    src, on_dest, dest ? dest : "", strerror(err_no));
		ASSERT(false);
	}
	return report_callback(cp_param, s, on_dest, src, dest, strerror(err_no));
}

/** 
 * Checks if the FTSENT object is an ADS
 * @param[IN] entp pointer to a valid FTSENT object
 * @return true if current object is an ADS; false otherwise 
 */
static bool
is_ads(const FTSENT *entp)
{
	ASSERT(entp && entp->fts_statp);
	return entp->fts_statp->st_flags & UF_ADS;
}

/**
 * Routine to copy mode,ownership,timestamps,ACL
 *
 * @param[IN] cp_param - parameter for copy operation
 * @param[IN] src_fd   - file descriptor of source file
 * @param[IN] src_file - source file pathname
 * @param[IN] fs       - source file stat
 * @param[IN] dst_fd   - file descriptor of destination file
 * @param[IN] dest_path   - destination file pathname
 * @return 0 if successful; otherwise non-0
 */
static int
preserve_sys_attrs(struct fsu_copy_param *cp_param,
    int src_fd, const char *src_file,
    struct stat *fs, int dst_fd, const char *dest_path)
{
	static struct timeval tv[2];
	struct stat ts;
	int rval, gotstat, islink, fdval;
	enum ifs_security_info secinfo = 0;

	rval = 0;
	fdval = dst_fd != -1;
	islink = !fdval && S_ISLNK(fs->st_mode);
	fs->st_mode &= S_ISUID | S_ISGID | S_ISVTX |
	    S_IRWXU | S_IRWXG | S_IRWXO;

	TIMESPEC_TO_TIMEVAL(&tv[0], &fs->st_atimespec);
	TIMESPEC_TO_TIMEVAL(&tv[1], &fs->st_mtimespec);
	if (islink ? lutimes(dest_path, tv) : utimes(dest_path, tv)) {
		struct fmt FMT_INIT_CLEAN(msg);
		fmt_print(&msg, "%sutimes: %s, %s", islink ? "l" : "",
		    dest_path, strerror(errno));
		report_callback(cp_param, COPY_RPT_WARNING, true, src_file,
		    dest_path, fmt_string(&msg));
		rval = 1;
	}
	if (fdval ? fstat(dst_fd, &ts) :
	    (islink ? lstat(dest_path, &ts) : stat(dest_path, &ts)))
		gotstat = 0;
	else {
		gotstat = 1;
		ts.st_mode &= S_ISUID | S_ISGID | S_ISVTX |
		    S_IRWXU | S_IRWXG | S_IRWXO;
	}

	/*
	 * Changing the ownership probably won't succeed, unless we're root
	 * or POSIX_CHOWN_RESTRICTED is not set.  Set uid/gid before setting
	 * the mode; current BSD behavior is to remove all setuid bits on
	 * chown.  If chown fails, lose setuid/setgid bits.
	 */
	if ((fs->st_flags & SF_HASNTFSOG) == SF_HASNTFSOG) {
		secinfo |= IFS_SEC_INFO_OWNER | IFS_SEC_INFO_GROUP;
	} else if (!gotstat || fs->st_uid != ts.st_uid ||
	    fs->st_gid != ts.st_gid)
		if (fdval ? fchown(dst_fd, fs->st_uid, fs->st_gid) :
		    (islink ? lchown(dest_path, fs->st_uid, fs->st_gid) :
		    chown(dest_path, fs->st_uid, fs->st_gid))) {
			if (errno != EPERM) {
				struct fmt FMT_INIT_CLEAN(msg);
				fmt_print(&msg, "chown: %s", strerror(errno));
				report_callback(cp_param, COPY_RPT_WARNING,
				    true, src_file,
				    dest_path, fmt_string(&msg));
				rval = 1;
			}
			fs->st_mode &= ~(S_ISUID | S_ISGID);
		}

	if ((fs->st_flags & SF_HASNTFSACL) == SF_HASNTFSACL) {
		secinfo |= IFS_SEC_INFO_DACL;
		fs->st_mode &= ~ACCESSPERMS;
	}

	if (!gotstat || fs->st_mode != ts.st_mode)
		/*
		 * Call chmod only if the file doesn't have ACLs
		 * or have set[ug]id/sticky bits.
		 */
		if ((fs->st_flags & SF_HASNTFSACL) != SF_HASNTFSACL ||
		    (fs->st_mode & ~ACCESSPERMS))
			if (fdval ? fchmod(dst_fd, fs->st_mode) :
			    (islink ? lchmod(dest_path, fs->st_mode) :
			    chmod(dest_path, fs->st_mode))) {
				struct fmt FMT_INIT_CLEAN(msg);
				fmt_print(&msg, "chmod: %s", strerror(errno));
				report_callback(cp_param, COPY_RPT_WARNING,
				    true, src_file,
				    dest_path, fmt_string(&msg));
				rval = 1;
			}

	if (!gotstat || fs->st_flags != ts.st_flags)
		if (fdval ?
		    fchflags(dst_fd, fs->st_flags) :
		    (islink ? lchflags(dest_path, fs->st_flags) :
		    chflags(dest_path, fs->st_flags))) {
			struct fmt FMT_INIT_CLEAN(msg);
			fmt_print(&msg, "chflags: %s", strerror(errno));
			report_callback(cp_param, COPY_RPT_WARNING,
			    true, src_file, dest_path, fmt_string(&msg));
			rval = 1;
		}

	if (secinfo) {
		struct ifs_sec_desc_buf sdb = {0, 0};

		if (src_fd != -1 ? aclu_get_fd_sd(src_fd, secinfo, &sdb) :
			aclu_get_sd(src_file, O_NOFOLLOW, secinfo, &sdb)) {
			struct fmt FMT_INIT_CLEAN(msg);
			fmt_print(&msg, "failed to get acl entries: %s",
			    strerror(errno));
			report_callback(cp_param, COPY_RPT_WARNING,
			    false, src_file, dest_path, fmt_string(&msg));
			rval = 1;
		} else if (fdval ?
		    aclu_set_fd_sd(dst_fd, secinfo, sdb.sd,
			SMAS_FLAGS_FORCE_SD) :
		    aclu_set_sd(dest_path, O_NOFOLLOW, secinfo, sdb.sd,
			SMAS_FLAGS_FORCE_SD)) {
			struct fmt FMT_INIT_CLEAN(msg);
			fmt_print(&msg, "failed to set acl entries: %s",
			    strerror(errno));
			report_callback(cp_param, COPY_RPT_WARNING,
			    false, src_file, dest_path, fmt_string(&msg));
			rval = 1;
		}

		// Free sd got from aclu_get_fd
		if (sdb.sd)
			aclu_free_sd_buf(&sdb, false);
	}

	return (rval);
}

/**
 * Copy the file content  
 *
 * @param[IN] from_fd - file descriptor of the source 
 * @param[IN] fs      - stat of the source file
 * @param[IN] to_fd   - file descriptor of the target
 * @param[IN] bigbuf  - whether big buffer is used for copy operation
 * @param[OUT] errout - the output error number
 * @return 0 if copy succeeds, 1 if read fails, 2 if write fails, -1 if no memory
 */
static int
copy_file_content(int from_fd, int to_fd, bool bigbuf, int *errout)
{
	char *buf = NULL;
	ssize_t wcount;
	size_t wresid;
	off_t wtotal;
	int rcount, rval = 0;
	char *bufp;
	u_int32_t bufsize = 0;
	
	*errout = 0;

	if (bigbuf)              /* use Big buffer */
		bufsize = BIGBUFSIZE;
	else
		bufsize = MAXBSIZE;

	buf = malloc(bufsize);
	if (!buf) {
		*errout = errno;
		return -1;
	}

	wtotal = 0;
	while ((rcount = read(from_fd, buf, bufsize)) > 0) {
		for (bufp = buf, wresid = rcount; ;
		bufp += wcount, wresid -= wcount) {
			wcount = write(to_fd, bufp, wresid);
			if (wcount <= 0)
				break;
			wtotal += wcount;

			if (wcount >= (ssize_t)wresid)
				break;
		}
		if (wcount != (ssize_t)wresid) {
			//warn("%s", dest_path);
			*errout = errno;			
			rval = 2;
			break;
		}
	}
	if (rcount < 0) {
		//warn("%s", entp->fts_path);
		*errout = errno;
		rval = 1;		
	}

	free(buf);
	return rval;
}

/**
 * Get file descriptor of parent of an object specified by given @a path
 * 
 * It closes and reassigns the cached file descriptor if @a path is different
 * than the cached one.
 * @param[IN] cp   - current cached parent information
 * @param[IN] path - the full path of the object
 * @param[OUT] name_out - the name of the object 
 * @return file descriptor of parent of the object, or -1 if it fails to open
 *         the parent; or AT_FDCWD if no parent can be identified 
 */
static int
get_parent_fd(struct cp_parent_cache *cp, const char *path, char **name_out)
{
	int pfd = -1;
	ASSERT(cp);
	ASSERT(strlen(path) < PATH_MAX);
	/* Open the parent directory with default encoding. */
	char *split = NULL, *name = NULL;

	if ((split = strrchr(path, '/')) != NULL) {
		*split = '\0';
		name = split + 1;
	} else {
		name = (char *)path;
		/* In this case the base path is empty. */
		path = "";
	}
	*name_out = name;
	if (cp->pfd != -1 && strcmp(cp->path, path) == 0) {
		pfd = cp->pfd;
	}
	else {
		if( cp->pfd >= 0 && cp->pfd != AT_FDCWD)
			close_fd(cp->pfd);
		if (!split)
			pfd = AT_FDCWD;
		else {
			pfd = enc_openat(AT_FDCWD, path, ENC_DEFAULT,
			    O_RDONLY|O_NONBLOCK);
			strcpy(cp->path, path);
		}
		cp->pfd = pfd;
	}

	if (split)
		*split = '/';
	/* If we have a real file descriptor dup it. */
	if (pfd != -1 && pfd != AT_FDCWD)
		return dup(pfd);
	else
		return pfd;
}

// same as the one in genobj_common.c, but accept file descriptor
/**
 * Simple single attribute set. If either value or value_size is set to 0, key
 * will be deleted.
 * @param fd file descriptor
 * @param key  utf-8 string for key name for attribute; should be <=
 * 	IFS_USERATTR_KEY_MAXSIZE and any utf-8 char except '\0'
 * @param value value to be set for attribute, should be <=
 *     IFS_USERATTR_VAL_MAXSIZE and any utf-8 char except '\0'
 * @param value_size size of value to be set
 * @param error_out isi_error structure, NULL if no error, set if error found
 */
static void
fsu_set_attribute(int fd, const char *key,
    void *value, size_t value_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(error_out);

	if (!key) {
		error = ostore_invalid_argument_error_new(
		    "Key not specified for attribute");
		goto out;
	}

	if (value_size == 0) {
		value = "";
	}

	if (ifs_userattr_set(fd, key, value, value_size) == -1) {
		switch (errno) {
		case ENAMETOOLONG:
			error = ostore_exceeds_limit_error_new(
			    "Attribute key exceeds limit: %d bytes",
			    IFS_USERATTR_KEY_MAXSIZE);
			break;
		case EOVERFLOW:
			error = ostore_exceeds_limit_error_new(
			    "Attribute value exceeds limit: %d bytes",
			    IFS_USERATTR_VAL_MAXSIZE);
			break;
		case E2BIG:
			error = ostore_exceeds_limit_error_new(
			    "Attribute count or size exceeds limits");
			break;
		default:
			error = isi_system_error_new(errno);
			break;
		}
		goto out;
	}
 out:
	if (error)
		isi_error_handle(error, error_out);

}

/**
 * Copy extended attributes (user namespace only) for file or directory
 * @param[IN] from_fd - file descriptor of source
 * @param[IN] to_fd   - file descriptor of source
 * @param[OUT] error_out - error if copy fails
 */
static void
copy_ext_attributes(int from_fd, int to_fd, struct isi_error **error_out)
{
	ASSERT(error_out);
	struct isi_error *error = NULL, *error1 = NULL;
	int count = 0, i = 0;
	struct iobj_object_attrs *attr_list = 0;
	count = genobj_list_ext_attrs(from_fd, &attr_list, &error);
	for (i = 0; error == NULL && i < count; i++) {
		struct iobj_obj_attr_entry entry = attr_list->attrs[i];
		/* Set the key/value pair to the target object.*/
		fsu_set_attribute(to_fd, entry.key,
		    entry.value, entry.value_size, &error);
	}
	if (count > 0)
		genobj_attribute_list_release(attr_list, &error1);
	if (error1) // ASSERT here, should not happen
		isi_error_free(error1);

	isi_error_handle(error, error_out);
}

/**
 * Copy a file object or an ADS
 *
 * @param[IN] cp_param - parameter for copy operation
 * @param[IN] cur_p    - cache for parent information
 * @param[IN] dest_path - destination file pathname
 * @param[IN] entp     - current FTS entry of the file to be copied
 * @param[OUT] obj_created - if destination created, set to 1; otherwise 0
 * @return 0 if successful; otherwise non-0
 */
static int
copy_file(struct fsu_copy_param *cp_param, struct cp_parent_cache *cur_p,
    const char *dest_path, const FTSENT *entp)
{
	ASSERT(cp_param && entp && cur_p && dest_path);

	struct isi_error *error = NULL;
	struct stat *fs = entp->fts_statp;
	int rval = 0, err = 0;
	int to_pfd = -1, from_fd = -1, to_fd = -1;
	char *name = NULL;
	struct fsu_copy_flags cp_flags = cp_param->cp_flags;
        int sfd = -1;

	from_fd = enc_openat(entp->fts_dirfd, entp->fts_name,
	    ENC_DEFAULT, O_RDONLY, 0);

	if (from_fd >= 0)
		to_pfd = get_parent_fd(cur_p, dest_path, &name);

	if (from_fd < 0 || to_pfd < 0) {
		report_callback1(cp_param, COPY_RPT_ERROR,
		    from_fd >= 0, // error on target side if true
		    entp->fts_path, dest_path, errno);
		rval = 1;
		goto out;
	}

	/* Basic file open flags for extracting file. */
	int oflags = (O_WRONLY | O_CREAT);

	/* Creating an ADS entry. */
	if (is_ads(entp))
		oflags |= O_XATTR;
	else {
		/*
		 * If we're not supposed to overwrite pre-existing files,
		 * use O_EXCL.  Otherwise, use O_TRUNC.
		 */
		if (cp_flags.noovw)
			oflags |= O_EXCL;

		else
			oflags |= O_TRUNC;
	}

	to_fd = enc_openat(to_pfd, name, ENC_DEFAULT, oflags,
		fs->st_mode & ~(S_ISUID | S_ISGID) & ~(cp_flags.mask));

	if (to_fd == -1) {
		report_callback1(cp_param, COPY_RPT_ERROR, true,
		    entp->fts_path, dest_path, errno);
		rval = 1;
		goto out;
	}

        if (cp_flags.cflag) {
do_clone:
                ss_clone_onefd(from_fd, to_fd, &error);
                if (error) {
                        if (sfd == -1 && isi_system_error_is_a(error, EPERM)) {
			        isi_error_free(error);
			        error = NULL;
			        sfd = gettcred(NULL, true);
			        if (sfd >= 0 && reverttcred() == 0)
				        goto do_clone;
		        }
		        report_callback(cp_param, COPY_RPT_ERROR, true,
                            entp->fts_path, dest_path,
                            isi_error_get_message(error));
		        isi_error_free(error);
		        rval = 1;
		        goto out;
                }
        } else {
                rval = copy_file_content(from_fd, to_fd, cp_flags.bigbuf, &err);

	        if (rval) {
		        if (rval > 0) {
			        // read/write error
			        struct fmt FMT_INIT_CLEAN(msg);
			        fmt_print(&msg, "%s: %s", rval == 2 ?
                                    "write error" : "read error", strerror(err));
			        report_callback(cp_param, COPY_RPT_ERR_INTERNAL,
			        rval == 2 ? true : false,
			        entp->fts_path, dest_path, fmt_string(&msg));
		        } else {
			        report_callback1(cp_param, COPY_RPT_ERR_INTERNAL, false,
			            entp->fts_path, dest_path, err);
		        }
		        goto out;
	        }
        }

	copy_ext_attributes(from_fd, to_fd, &error);
	if (error) {
		report_callback(cp_param, COPY_RPT_ERROR, true,
		    entp->fts_path, dest_path, isi_error_get_message(error));
		isi_error_free(error);
		rval = 1;
		goto out;
	}

	/*
	 * Don't remove the target even after an error.  The target might
	 * not be a regular file, or its attributes might be important,
	 * or its contents might be irreplaceable.  It would only be safe
	 * to remove it if we created it and its length is 0.
	 */

	if (cp_flags.pflag && !is_ads(entp) && // no support for ads
	    preserve_sys_attrs(cp_param, from_fd, entp->fts_path,
	    fs, to_fd, dest_path) != 0)
		rval = 1;

out:
	if (from_fd >= 0)
		close_fd(from_fd);
	if (to_pfd >= 0)
		close_fd(to_pfd);
	if (to_fd >= 0)
		close_fd(to_fd);
	if (sfd >= 0) {
		settcred(sfd, TCRED_FLAG_THREAD, NULL);
		close(sfd);
	}

	return (rval);
}

/*
 * Copy link object
 */
static int
copy_link(struct fsu_copy_param *cp_param, const char *dest_path,
    const FTSENT *from, int exists)
{
	int len;
	char llink[PATH_MAX];
	struct fsu_copy_flags cp_flags = cp_param->cp_flags;

	if ((len = readlink(from->fts_path, llink, sizeof(llink) - 1)) == -1) {
		report_callback1(cp_param, COPY_RPT_ERROR, false, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	llink[len] = '\0';
	if (exists && unlink(dest_path)) {
		report_callback1(cp_param, COPY_RPT_WARNING, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	if (symlink(llink, dest_path)) {
		report_callback1(cp_param, COPY_RPT_ERROR, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	if (cp_flags.pflag)
		return preserve_sys_attrs(cp_param, -1, from->fts_path,
		    from->fts_statp, -1, dest_path);
	else {
		if (lchmod(dest_path,
		    from->fts_statp->st_mode & ~(cp_flags.mask))) {
			report_callback1(cp_param, COPY_RPT_ERROR, true,
			    from->fts_path, dest_path, errno);
			return (1);
		}
	}
	return 0;
}

/**
 * Copy FIFO object
 */
static int
copy_fifo(struct fsu_copy_param *cp_param, const char *dest_path,
    const FTSENT *from, int exists)
{
	struct fsu_copy_flags cp_flags = cp_param->cp_flags;
	if (exists && unlink(dest_path)) {
		report_callback1(cp_param, COPY_RPT_WARNING, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	if (mkfifo(dest_path, from->fts_statp->st_mode & ~(cp_flags.mask))) {
		report_callback1(cp_param, COPY_RPT_ERROR, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	return (cp_flags.pflag ? preserve_sys_attrs(cp_param, -1,
	    from->fts_path, from->fts_statp, -1, dest_path) : 0);
}

/**
 * Copy special device file
 */
static int
copy_special(struct fsu_copy_param *cp_param, const char *dest_path,
    const FTSENT *from, int exists)
{
	struct fsu_copy_flags cp_flags = cp_param->cp_flags;
	if (exists && unlink(dest_path)) {
		report_callback1(cp_param, COPY_RPT_WARNING, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	if (mknod(dest_path, from->fts_statp->st_mode & ~(cp_flags.mask),
	    from->fts_statp->st_rdev)) {
		report_callback1(cp_param, COPY_RPT_ERROR, true, from->fts_path,
		    dest_path, errno);
		return (1);
	}
	return (cp_flags.pflag ? preserve_sys_attrs(cp_param, -1,
	    from->fts_path, from->fts_statp, -1, dest_path) : 0);
}

/**
 * Create directory
 * Copies mode and (user namespace only) extended attributes 
 */
static int
copy_create_dir(struct fsu_copy_param *cp_param, FTSENT *entp,
    struct cp_parent_cache *dest_cp, const char *dest_path)
{
	struct isi_error *error = NULL;
	int to_dfd = -1, from_dfd = -1;
	int rval = 0;
	DIR *dp = NULL;
	struct fmt FMT_INIT_CLEAN(msg);

	dp = opendir(entp->fts_path);

	if (!dp || (from_dfd = dirfd(dp)) <= 0) {
		fmt_print(&msg, "failed to open dir: %s", strerror(errno));
		report_callback(cp_param, COPY_RPT_ERROR, true, entp->fts_path,
		    dest_path, fmt_string(&msg));
		rval = 1;
		goto out;
	}
	if ((to_dfd = ifs_createfile(-1, dest_path,
	    IFS_RTS_DIR_GEN_WRITE, O_CREAT|O_DIRECTORY|O_NONBLOCK,
	    (entp->fts_statp->st_mode & ~(cp_param->cp_flags.mask)) | S_IRWXU,
	    NULL)) < 0) {
		fmt_print(&msg, "failed to create dir: %s", strerror(errno));
		report_callback(cp_param, COPY_RPT_ERROR, true, entp->fts_path,
		    dest_path, fmt_string(&msg));
		rval = 1;
		goto out;
	}

	copy_ext_attributes(from_dfd, to_dfd, &error);
	if (error) {
		fmt_print(&msg, "failed to copy attributes: %s",
		    isi_error_get_message(error));
		report_callback(cp_param, COPY_RPT_ERROR, true, entp->fts_path,
		    dest_path, fmt_string(&msg));
		isi_error_free(error);
		rval = 1;
		goto out;
	}

out:
	// can't close from_dfd separately, which is closed by closedir.
	if (dp)
		closedir(dp);
	if (to_dfd >= 0)
		close_fd(to_dfd);
	return rval;
}

/**
 * Reset copy-to path
 * 
 * if path = '/ifs/src/b/c'
 *    base_len = strlen("/ifs/src/");
 *    ctype = FILE_TO_FILE
 *    to_path.path = '/ifs/target/t1/t2'
 *    to_path.target_end points to '/t1/t2' of to_path
 * then
 *    to_path.path will be set to '/ifs/target/b/c
 *    
 * @param[IN] path     - file path 
 * @param[IN] path_len - lenght of @a path
 * @param[IN] base_len - length of the portion of @a path not to be copied
 * @param[IN] ctype    - type of copy
 * @param[IN] to_path  - file path information tmp object is renamed to
 * @return -1 if result pathname exceeds PATH_MAX; 0 otherwise
 */
static int
reset_copy_to_path(const char *path, u_short path_len, int base_len,
    enum fsu_copy_type ctype, struct fsu_copy_path *to_path )
{
	const char *p = &path[base_len];
	int nlen = path_len - base_len;
	char *target_mid = to_path->target_end;
	if (ctype != FILE_TO_FILE && *p != '/' && target_mid[-1] != '/')
		*target_mid++ = '/';
	*target_mid = 0;
	if (target_mid - to_path->p_path + nlen >= PATH_MAX)
		return -1;

	(void)strncat(target_mid, p, nlen);
	to_path->p_end = target_mid + nlen;
	*to_path->p_end = 0;
	STRIP_TRAILING_SLASH(*to_path);
	return 0;
}

/**
 * Append source file name to target with consideration of temp path
 * and ADS object
 *
 * @param[IN] curr     - current FTS entry of the file to be copied
 * @param[IN] base_len - length of copy source path base
 * @param[IN] ctype    - type of copy
 * @param[IN] tmp_to_fpath - path info of temp target
 * @param[IN] cp_to_path   - path info of real copy target
 * @param[IN] to_path  - pointing to either @a tmp_to_fpath or @a cp_to_path
 *                       indicating whether temp or real path is in use
 * @return -1 if result pathname exceeds PATH_MAX; 0 otherwise
 */
static int
append_src_name_to_target(const FTSENT *curr,
    int base_len, enum fsu_copy_type ctype,
     const struct fsu_copy_path *tmp_to_fpath,
     struct fsu_copy_path *cp_to_path,
     struct fsu_copy_path *to_path)
{
	int rval = 0;
	// if current file is ads or tmp file is not in use, don't reset
	if (to_path != tmp_to_fpath || !is_ads(curr))
		rval = reset_copy_to_path(curr->fts_path,
		    curr->fts_pathlen, base_len, ctype, cp_to_path);
	// current file is ads and tmp file is in use
	if (to_path == tmp_to_fpath && !rval && is_ads(curr)) {
		// copy filename only without intermediate path
		*to_path->p_end = '/';
		if ( to_path->p_end - to_path->p_path + 1 +
		    curr->fts_namelen >= PATH_MAX)
			rval = -1;
		else {
			strncpy(to_path->p_end + 1, curr->fts_name,
			    curr->fts_namelen);
			to_path->p_end[curr->fts_namelen + 1] = '\0';
		}
	}
	return rval;
}

/**
 * Rename the temp object to true object
 * @param[IN] cp_param     - parameter for copy operation
 * @param[IN] ctype        - type of copy
 * @param[IN] tmp_to_fpath - file path information of temp object
 * @param[IN] src_path_s   - copy source file pathname
 * @param[IN] base_len     - length of the portion of @a path not to be copied
 * @param[IN] cp_to_path   - file path information temp object is renamed to
 */
static int
move_tmp_to_dest(struct fsu_copy_param *cp_param, enum fsu_copy_type ctype,
    struct fsu_copy_path *tmp_to_fpath, const char *src_path_s,
    int base_len, struct fsu_copy_path *cp_to_path)
{
	int rval = reset_copy_to_path(src_path_s, strlen(src_path_s),
	    base_len, ctype, cp_to_path);
	const char *dest_path = cp_to_path->p_path;
	if (rval) {
		report_callback1(cp_param, COPY_RPT_ERROR, true,
		    src_path_s, dest_path, ENAMETOOLONG);
		return -1;
	}

	if (rename(tmp_to_fpath->p_path, cp_to_path->p_path)) {
		report_callback1(cp_param, COPY_RPT_ERROR, true,
		    src_path_s, dest_path, errno);
		return -1;
	} else if (cp_param->cp_flags.vflag){
		struct fsu_copy_rpt rpt_info = {COPY_RPT_INFO, false,
		    src_path_s, dest_path, "copied"};
		cp_param->cp_callback(cp_param->caller_context, &rpt_info);
	}
	return 0;
}

/**
 * Remove the temp object
 * @param[IN] cp_param     - parameter for copy operation
 * @param[IN] src          - copy source file pathname for error reporting
 * @param[IN] tmp_to_fpath - file path information of temp object
 */
static int
tmp_copy_cleanup(struct fsu_copy_param *cp_param, const char *src,
    struct fsu_copy_path *tmp_to_fpath)
{
	// get rid of tmp file
	if (remove(tmp_to_fpath->p_path) && errno != ENOENT) {
		// ? internal error
		report_callback1(cp_param, COPY_RPT_ERR_INTERNAL, true,
		    src, tmp_to_fpath->p_path, errno);
		return -1;
	}
	return 0;
}

int
ostore_ifs_copy(struct fsu_copy_param *cp_param,
    struct fsu_copy_path *cp_to_path,
    enum fsu_copy_type ctype, int fts_options,
    struct fsu_copy_path *tmp_to_fpath,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat to_stat;
	FTS *ftsp;
	FTSENT *curr;
	int base_len = 0, rtbase_len = 0;
	int badcp = 0; // failure indicator for object copying including ads
	int rval = 0; // failure indicator for any operation
	char *p;
	mode_t mask, mode;
	int dne = 0; // do-not-exist flag
	struct fsu_copy_path *to_path = cp_to_path;
	char *dest_path = cp_to_path->p_path;
	char src_path_s[PATH_MAX];
	struct cp_parent_cache dest_cp;
	dest_cp.pfd = -1;

	struct fsu_copy_flags cp_flags = cp_param->cp_flags;
	char *argv[2] = {(char *) cp_param->src, NULL};

	ASSERT(argv[0] && cp_to_path);
	ASSERT(tmp_to_fpath != cp_to_path);
	ASSERT(error_out && *error_out == NULL);

	if ((ftsp = fts_open(argv, fts_options, NULL)) == NULL) {
		report_callback1(cp_param, COPY_RPT_ERROR, false,
		    cp_param->src, dest_path, errno);
		error = isi_system_error_new(errno, "%s:%s",
		    argv[0], strerror(errno));
		badcp = rval = 1;
		goto out;
	}
	for (badcp = rval = 0; (curr = fts_read(ftsp)) != NULL; ) {

		switch (curr->fts_info) {
		case FTS_NS:
		case FTS_DNR:
		case FTS_ERR:
			report_callback(cp_param, COPY_RPT_WARNING, false,
			    curr->fts_path, dest_path,
			    strerror(curr->fts_errno));
			badcp = rval = 1;
			if (!cp_flags.cont) {
				error = isi_system_error_new(curr->fts_errno,
				    "%s: %s",
				    curr->fts_path, strerror(curr->fts_errno));
				goto out;
			}
			continue;
		case FTS_DC:			/* Warn, continue. */
			report_callback(cp_param, COPY_RPT_WARNING, false,
			    curr->fts_path, dest_path,
			    "directory causes a cycle");
			badcp = rval = 1;
			if (!cp_flags.cont) {
				error = isi_system_error_new(ELOOP,
				    "%s", curr->fts_path);
				goto out;
			}
			continue;

		case FTS_ADSC:	/* File/Dir ADS container pre-order. */
			continue;
		case FTS_ADSCP:	/* File/Dir ADS container post-order. */
			// give a chance to rename tmp object
			break; //continue;

		case FTS_ADSS:	/* File/Dir ADS */
			if (badcp) // no need to do further for owner file
				continue;
			/* we copy the ADS stream. */
			break;
		default:
			;
		}

		/*
		 * Need to remember the roots of traversals to create
		 * correct pathnames.  If there's a directory being
		 * copied to a non-existent directory, e.g.
		 *	cp -R a/dir noexist
		 * the resulting path name should be noexist/foo, not
		 * noexist/dir/foo (where foo is a file in dir), which
		 * is the case where the target exists.
		 *
		 * Also, check for "..".  This is for correct path
		 * concatenation for paths ending in "..", e.g.
		 *	cp -R .. /tmp
		 * Paths ending in ".." are changed to ".".  This is
		 * tricky, but seems the easiest way to fix the
		 * problem.
		 *
		 * XXX
		 * Since the first level MUST be FTS_ROOTLEVEL, base
		 * is always initialized.
		 */
		if (curr->fts_level == FTS_ROOTLEVEL && !rtbase_len) {
			// src root base, always same for whole phase of copy
			rtbase_len = curr->fts_pathlen;
			// adjusted base (FILE_TO_DIR not used by namespace)
			if (ctype == FILE_TO_DIR) {
				p = strrchr(curr->fts_path, '/');
				base_len = (p == NULL) ? 0 :
				    (int)(p - curr->fts_path + 1);

				if (!strcmp(&curr->fts_path[base_len],
				    ".."))
					base_len += 1;
			} else
				base_len = curr->fts_pathlen;
		}

		// logic setting up tmp object path
		if (!is_ads(curr) && tmp_to_fpath) {
			// move tmp file to dest if it is good
			if (to_path == tmp_to_fpath) {
				// ads may have impacted the end, restore it
				*tmp_to_fpath->p_end = '\0';
				if (badcp || move_tmp_to_dest(cp_param,
				    ctype, tmp_to_fpath, src_path_s,
				    rtbase_len, cp_to_path)) {
					tmp_copy_cleanup(cp_param,
					    src_path_s, tmp_to_fpath);
					if (!cp_flags.cont)
						goto out;
				}
				// switch to real target path
				to_path = cp_to_path;
				dest_path = cp_to_path->p_path;
			}

			if (curr->fts_info != FTS_ADSCP &&
			    !S_ISDIR(curr->fts_statp->st_mode) &&
			    (curr->fts_statp->st_mode & S_IFMT) != S_IFSOCK) {
				//save a copy for later rename
				strcpy(src_path_s, curr->fts_path);
				// switch to tmp path
				to_path = tmp_to_fpath;
				dest_path = tmp_to_fpath->p_path;
			}
		}

		badcp = 0; // reset for new object

		if (curr->fts_info == FTS_ADSCP)
			continue;

		// We need to append the source name to the target name.
		if (append_src_name_to_target(curr, base_len, ctype,
		    tmp_to_fpath, cp_to_path, to_path) == -1) {
			report_callback1(cp_param, COPY_RPT_ERROR, true,
			    curr->fts_path, dest_path, ENAMETOOLONG);

			badcp = rval = 1;
			if (!cp_flags.cont)
				goto out;

			continue;
		}

		if (curr->fts_info == FTS_DP) {
			/*
			 * We are nearly finished with this directory.  If we
			 * didn't actually copy it, or otherwise don't need to
			 * change its attributes, then we are done.
			 */
			if (!curr->fts_number)
				continue;
			/*
			 * If -p is in effect, set all the attributes.
			 * Otherwise, set the correct permissions, limited
			 * by the umask.  Optimise by avoiding a chmod()
			 * if possible (which is usually the case if we
			 * made the directory).  Note that mkdir() does not
			 * honour setuid, setgid and sticky bits, but we
			 * normally want to preserve them on directories.
			 */
			if (cp_flags.pflag) {
				if (preserve_sys_attrs(cp_param,
				    -1, curr->fts_accpath, curr->fts_statp,
				    -1, dest_path) != 0)
					badcp = rval = 1;
			} else {
				mask = ~(cp_flags.mask);
				mode = curr->fts_statp->st_mode;
				if ((mode & (S_ISUID | S_ISGID | S_ISTXT)) ||
				    ((mode & mask) | S_IRWXU) != (mode & mask))
					if (chmod(to_path->p_path,
					    mode & mask) != 0) {
						struct fmt FMT_INIT_CLEAN(msg);
						fmt_print(&msg, "chmod: %s",
						    strerror(errno));
						report_callback(cp_param,
						    COPY_RPT_WARNING, true,
						    curr->fts_path,
						    to_path->p_path,
						    fmt_string(&msg));
						badcp = rval = 1;
					}
			}
			if (!cp_flags.cont && badcp)
				goto out;
			continue;
		}

		/* Not an error but need to remember it happened */
		if (is_ads(curr) || lstat(cp_to_path->p_path, &to_stat) == -1)
			dne = 1;
		else {
			if ((S_ISDIR(to_stat.st_mode) && !cp_flags.merge) ||
			    ((S_ISLNK(to_stat.st_mode) ||
			     !S_ISDIR(to_stat.st_mode)) && cp_flags.noovw)) {
				// this is not allowed
				report_callback(cp_param, COPY_RPT_ERROR,
				    true, curr->fts_path, to_path->p_path,
				    "target exists(not copied)");
				badcp = rval = 1;
			} else if (to_stat.st_dev == curr->fts_statp->st_dev &&
			    to_stat.st_ino == curr->fts_statp->st_ino &&
			    to_stat.st_snapid == curr->fts_statp->st_snapid) {

				badcp = rval = 1;
				report_callback(cp_param, COPY_RPT_ERROR,
				    true, curr->fts_path, to_path->p_path,
				    "source and target are identical"
				    "(not copied).");
				if (S_ISDIR(curr->fts_statp->st_mode))
					(void)fts_set(ftsp, curr, FTS_SKIP);

			} else if (!S_ISDIR(curr->fts_statp->st_mode) &&
			    S_ISDIR(to_stat.st_mode)) {
				report_callback(cp_param, COPY_RPT_ERROR,
				    true, curr->fts_path, to_path->p_path,
				    "cannot overwrite directory"
				    " with non-directory");
				badcp = rval = 1;

			} else if (S_ISDIR(curr->fts_statp->st_mode) &&
			    !S_ISDIR(to_stat.st_mode) ) {
				report_callback(cp_param, COPY_RPT_ERROR,
				    true, curr->fts_path, to_path->p_path,
				    "cannot overwrite non-directory"
				    " with directory");
				badcp = rval = 1;
			}
			if (badcp) {
				if (!cp_flags.cont)
					goto out;
				if (S_ISDIR(to_stat.st_mode))
				    (void)fts_set(ftsp, curr, FTS_SKIP);
				continue;
			}
		}

		switch (curr->fts_statp->st_mode & S_IFMT) {
		case S_IFLNK:
			/* Catch special case of a non-dangling symlink */
			if ((fts_options & FTS_LOGICAL) ||
			    ((fts_options & FTS_COMFOLLOW) &&
			    curr->fts_level == 0)) {
				if (copy_file(cp_param, &dest_cp, dest_path,
				    curr))
					badcp = rval = 1;
			} else {
				if (copy_link(cp_param, dest_path, curr,
				    (!dne && to_path != tmp_to_fpath)))
					badcp = rval = 1;
			}
			break;
		case S_IFDIR:
			if (!cp_flags.recur) {
				report_callback(cp_param, COPY_RPT_ERROR, false
				    , curr->fts_path, dest_path, "source is a "
				    "directory (not copied)");
				(void)fts_set(ftsp, curr, FTS_SKIP);
				badcp = rval = 1;
				break;
			}
			/*
			 * If the directory doesn't exist, create the new
			 * one with the from file mode plus owner RWX bits,
			 * modified by the umask.  Trade-off between being
			 * able to write the directory (if from directory is
			 * 555) and not causing a permissions race.  If the
			 * umask blocks owner writes, we fail..
			 */
			ASSERT(dne || (S_ISDIR(to_stat.st_mode) &&
			    cp_flags.merge));

			if (copy_create_dir(cp_param, curr, &dest_cp,
			    dest_path)) {
				badcp = rval = 1;
				(void)fts_set(ftsp, curr, FTS_SKIP);
			}
			/*
			 * Arrange to correct directory attributes later
			 * (in the post-order phase) if this is a new
			 * directory, or if the -p flag is in effect.
			 */
			curr->fts_number = cp_flags.pflag || dne;
			break;
		case S_IFBLK:
		case S_IFCHR:
			if (cp_flags.recur) {
				if (copy_special(cp_param, dest_path, curr,
				    !dne))
					badcp = rval = 1;
			} else {
				if (copy_file(cp_param, &dest_cp, dest_path,
				    curr))
					badcp = rval = 1;
			}
			break;
		case S_IFSOCK:
			ilog(IL_DEBUG, "%s is a socket (not copied).",
			    curr->fts_path);
			break;
		case S_IFIFO:
			if (cp_flags.recur) {
				if (copy_fifo(cp_param, dest_path, curr, !dne))
					badcp = rval = 1;
			} else {
				if (copy_file(cp_param, &dest_cp, dest_path,
				    curr))
					badcp = rval = 1;
			}
			break;
		default:
			if (copy_file(cp_param, &dest_cp, dest_path, curr))
				badcp = rval = 1;
			break;
		}

		if (badcp && !cp_flags.cont)
			goto out;

		if (cp_param->cp_flags.vflag && !tmp_to_fpath) {
			struct fsu_copy_rpt rpt_info = {COPY_RPT_INFO, false,
			    curr->fts_path, dest_path, "copied"};
			cp_param->cp_callback(cp_param->caller_context,
			    &rpt_info);
		}
	}

	if (to_path == tmp_to_fpath) {
		if (badcp || move_tmp_to_dest(cp_param, ctype,
		    tmp_to_fpath, src_path_s, rtbase_len,
		    cp_to_path)) {
			tmp_copy_cleanup(cp_param,
			    src_path_s, tmp_to_fpath);
				goto out;
		}
	} else if (errno) { //fts_read error
		report_callback(cp_param, COPY_RPT_ERROR, false,
		    curr->fts_path, dest_path,
		    "Failed to traverse copy source");
		goto out;
	}
out:
	if (ftsp)
		fts_close(ftsp);
	if (dest_cp.pfd >= 0)
		close_fd(dest_cp.pfd);
	isi_error_handle(error, error_out);
	return (rval);
}

/**
 * Get the impersonated user id of current thread; or -1 if failed.
 */
static u_int
gettuid(void)
{
	u_int uid;
	int user_fd = gettcred(NULL, 1);
	if (user_fd < 0)
		return -1;
	if (ioctl(user_fd, CIOEUID, &uid)) {
		close(user_fd);
		return -1;
	}
	close(user_fd);
	return uid;
}

/**
 * Recursively remove directories and files
 *
 * @err_no and @err_path must be valid.
 *
 * @param[IN] argv - path of files and directories to be removed
 * @param[IN] cont_on_err - whether to continue the rest upon error
 * @param[IN] err_no - error code
 * @param[IN] err_path - the path causes the error
 * @return 0 if success; -1 otherwise
 */
static int
ostore_ifs_remove_x(char *paths[], bool cont_on_err, int *err_no,
    struct fmt *err_path)
{
	FTS *fts;
	FTSENT *p;
	int fts_flags;
	uid_t uid;
	int rval = 0, eval = 0;
	ASSERT(err_path && err_no);

	fts_flags = FTS_PHYSICAL | FTS_NOSTAT | FTS_NOCHDIR;

	if (!(fts = fts_open(paths, fts_flags, NULL))) {
		if (errno == ENOENT)
			return 0;
		if (!cont_on_err) {
			*err_no = errno;
			fmt_print(err_path, "%s", (char *)*paths);
		}
		return -1;
	}

	uid = gettuid();
	while ((p = fts_read(fts)) != NULL) {
		int st_flags = p->fts_statp->st_flags;
		rval = 0;

		switch (p->fts_info) {

		case FTS_ERR:
			if (p->fts_errno == ENOENT )
				continue;
			goto err;

		case FTS_D:
			/* Pre-order
			 * we want root be able to delete the file/dir even
			 * its flag says not changeable or
			 * can only be appended */
			if (!uid &&
			    (st_flags & (UF_APPEND|UF_IMMUTABLE)) &&
			    !(st_flags & (SF_APPEND|SF_IMMUTABLE)) &&
			    (rval = lchflags(p->fts_accpath,
			        st_flags &= ~(UF_APPEND|UF_IMMUTABLE))) < 0)
				goto err;
			continue;

		default:
			break;
		}

		if (!uid &&
		    (st_flags & (UF_APPEND|UF_IMMUTABLE)) &&
		    !(st_flags & (SF_APPEND|SF_IMMUTABLE))) {
			rval = lchflags(p->fts_accpath,
			    st_flags &= ~(UF_APPEND|UF_IMMUTABLE));
		}
		if (rval == 0) {
			/*
			 * If we can't read or search the directory, may still
			 * be able to remove it.
			 */
			switch (p->fts_info) {
			case FTS_DP:
			case FTS_DNR:
				rval = rmdir(p->fts_accpath);
				if (rval == 0 || ( errno == ENOENT))
					continue;

				/*
				 * Check if the error is 'directory not empty'
				 * for the main directory for which deletion is 
				 * attempted. This scenario arises due to 
				 * race condition, i.e. when a thread creates 
				 * sub-directories inside a directory,   
				 * another thread attempts deletion 
				 * on the directory.
				 */
				if (errno == ENOTEMPTY && 
				    p->fts_level == FTS_ROOTLEVEL &&
				    (strcmp(p->fts_accpath, paths[0]) == 0)) {
					goto out;
				}
				break;

			default:
				rval = unlink(p->fts_accpath);
				if (rval == 0 || (errno == ENOENT))
					continue;
			}
		}
err:
		if (eval != -1) { // record first error only
			if (rval)
				*err_no = errno;
			else
				*err_no = p->fts_errno;
			fmt_print(err_path, "%s", p->fts_path);
			eval = -1;
		}
		if (!cont_on_err)
			goto out2;
	}

out:
	if (errno) {
		*err_no = errno;
		fmt_truncate(err_path); // just in case err_path not empty
		fmt_print(err_path, "%s", (char *)*paths);
		eval = -1;
	}
out2:
	if (fts)
		fts_close(fts);//may change errno
	return eval;
}

int
ostore_ifs_remove(const char *path, bool cont_on_err, int *err_no,
    struct fmt *err_path)
{
	char *argv[2] = {(char *) path, NULL};
	return ostore_ifs_remove_x(argv, cont_on_err, err_no, err_path);
}

/**
 * Sets protection property to file or directory.
 * @PARAM[IN] fd - a file descriptor
 * @PARAM[IN] s_expattr - structure containing the protection property
 * @param[OUT] error_out - the isi_error returned in case of failure
 */
static void
genobj_set_expattr_fd(int fd,
    const struct pctl2_set_expattr_args *s_expattr,
    struct isi_error **error_out)
{
	ASSERT(error_out && *error_out == NULL);
	ASSERT(s_expattr);
	struct isi_error *error = NULL;

	int ret = pctl2_set_expattr(fd, s_expattr);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "failed to set object extended property");
		goto out;
	}

	enum restripe_type type = RESTRIPE_REPROTECT;

	restripe_fd(fd, type, NULL, NULL, &error);
	// if we failed enforce it, do we consider it as an error to user?
	// we can treat it no error to user because OneFS job will enforce
	// it later, and thus can be viewed as separate matter.
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
out:
	if (error)
		isi_error_handle(error, error_out);
}

void
ostore_ifs_checked_set_protection_fd(int fd,
    const struct iobj_set_protection_args *iobj_sp,
    struct isi_error **error_out)
{
	ASSERT(error_out && *error_out == NULL);
	struct isi_error *error = NULL;

	struct pctl2_get_expattr_args ge = PCTL2_GET_EXPATTR_ARGS_INITIALIZER;
	struct pctl2_set_expattr_args s_expattr =
	    PCTL2_SET_EXPATTR_ARGS_INITIALIZER;

	int ret = pctl2_get_expattr(fd, &ge);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "failed to get object extended property");
		goto out;
	}

	if (iobj_sp->node_pool_id != ge.ge_disk_pool_policy_id) {
		s_expattr.se_disk_pool_policy_id = iobj_sp->node_pool_id;
		s_expattr.se_manually_manage_protection = true;
	}

	genobj_set_expattr_fd(fd, &s_expattr, &error);

out:
	if (error)
		isi_error_handle(error, error_out);
}
