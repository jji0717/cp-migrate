/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ns_store.c __ 2012/03/26 10:47:52 EDST __ lwang2
 */

/**
 * Namespace store implementation
 */

#include "ns_common.h"

#include <sys/isi_privilege.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/queue.h>
#include <sys/stat.h>

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <isi_domain/worm.h>
#include <isi_domain/dom.h>
#include <isi_domain/dom_util.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_format.h>
#include "isi_util/ifs_mounted.h"
#include <isi_util/syscalls.h>

#include "bucket_common.h"
#include "fs_op_utils.h"
#include "genobj_common.h"
#include "object_common.h"
#include "ostore_internal.h"
#include "iobj_ostore_protocol.h"




// Implementing the namespace access interfaces:
struct i_store_provider ns_provider =
{
	// store interfaces
	{
		ns_store_list,
		ns_store_create,
		ns_store_destroy,
		ns_store_open,
		ns_store_close,
		ns_store_dump,
		ns_store_attributes_get,
		ns_store_attributes_set,
		ns_store_attributes_release,
		ns_store_parameters_get,
		ns_store_parameters_set,
		ns_store_parameters_release
	},

	// generic object interface
	{
		ns_genobj_open,
		genobj_close,
		genobj_get_name,
		genobj_attribute_get,
		genobj_attribute_set,
		genobj_attribute_delete,
		genobj_attribute_release,
		genobj_attribute_list,
		genobj_attribute_list_release,
		genobj_system_attributes_get,
		ns_genobj_copy,
		ns_genobj_move,
		ns_genobj_delete,
		ns_genobj_get_worm,
		ns_genobj_get_protection,
		ns_genobj_set_protection
	},

	// bucket interfaces:
	{
		ns_bucket_name_create,
		ns_bucket_create,
		ns_bucket_open,
		ns_bucket_close,
		ns_bucket_delete
	},

	// object interfaces:
	{
		ns_object_create,
		ns_object_open,
		ns_object_delete,
		ns_object_commit,
		ns_object_close,
		ns_object_read,
		ns_object_write,
		ns_object_list,
		ns_object_set_worm
		//ns_object_get_worm
	}
};

static inline struct igenobj_handle *
iobj_object_to_genobj(struct iobject_handle *oh)
{
	if (oh == NULL)
		return NULL;
	return &oh->base;
}

static inline
struct igenobj_handle *
iobj_bucket_to_genobj(struct ibucket_handle *bh)
{
	if (bh == NULL)
		return NULL;
	return &bh->base;
}

/**
 * read the store target path given the file descriptor of access point
 *
 * @param[IN] store_name  -- name of the store
 * @param[INOUT] p_path -- buffer for target path
 * @param[IN] path_size -- target path buffer size
 */
static struct isi_error *
read_ns_store_target(const char *store_name, char p_path[], int path_size)
{
	struct isi_error *error = NULL;
	int fd = -1;
	struct fmt FMT_INIT_CLEAN(access_pt);

	ASSERT(store_name && p_path);
	fmt_print(&access_pt, "%s/%s", NS_STORE_AP_ROOT, store_name);

	fd = open(fmt_string(&access_pt), O_RDONLY);
	if (fd == -1) {
		switch (errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
			    "Cannot open store %s", store_name);
			break;
		default:
			error = isi_system_error_new(errno,
			    "Cannot access object store %s",
			    store_name);
			break;
		}
		goto out;
	}

	int rcount = 0, rtotal = 0;
	while ((rcount = read(fd, p_path + rtotal,
	    path_size - rtotal -1)) > 0) {
		rtotal += rcount;
		if (rtotal >= PATH_MAX - 1)
			break;
	}
	if (rcount == -1) {
		error = isi_system_error_new(errno, "Failed to "
		    "read namespace path.");
		goto out;
	}

	p_path[rtotal] = '\0';

out:
	if (fd >= 0)
		close(fd);
	return error;
}

/**
 * Given the store name, resolve to a file system path
 */
struct isi_error *
ns_resolve_store_path(const char *store_name, struct fmt *store_path)
{
	struct isi_error *error = NULL;
	char p_path[PATH_MAX];

	if (!strcmp(store_name, DEFAULT_NS)) {
		int cred_fd = gettcred(NULL, 0);
		bool privileged = !ipriv_check_cred(cred_fd, ISI_PRIV_NS_IFS_ACCESS,
		    ISI_PRIV_FLAG_NONE);
		close(cred_fd);

		if (privileged) {
			fmt_print(store_path, "%s", DEFAULT_NS_PATH);
			return NULL;
		}
	}

	error = read_ns_store_target(store_name, p_path, PATH_MAX);

	if (!error)
		fmt_print(store_path, "%s", p_path);

	return error;
}


/**
 * Utility to resolve a bucket to file system path
 */
static struct isi_error *
ns_resolve_gen_path(struct ostore_handle *ios, const char *path,
    struct fmt *object_path)
{
	if (is_prohibited_dir(ios->path, path)) {
		return ostore_pathname_prohibited_error_new(
		    "%s", path);
	}
	fmt_print(object_path, "%s/%s", ios->path,
	    path[0] == '/' ? path + 1 : path);
	return NULL;
}

static void
ns_store_cache_remove(struct ostore_handle *handle)
{
	if (handle) {
		if (handle->name)
			free(handle->name);
		if (handle->path)
			free(handle->path);
		IOBJ_CLEAR_STORE_HANDLE_MAGIC(handle);
		free(handle);
	}
}

/**
 * Get the object name from a path
 */
static const char *
get_genobj_name_from_path(const char *path, const char *store_path)
{
	ASSERT (path && store_path);

	size_t store_len = strlen(store_path);
	size_t len = strlen(path);
	const char *end = path + len - 1;
	for (const char *p = end; p >= path+store_len; --p) {
		if (*p == '/') {
			if (p == end)
				continue;

			return p + 1;
		}
	}

	return "/";
}

/**
 * Allocate a new ostore entry and add to the list.
 */
static struct ostore_handle *
ns_store_cache_add(const char *store_name, uint64_t snapshot, int version,
    const char *path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *handle = NULL;

	handle = calloc(1, sizeof(struct ostore_handle));
	ASSERT(handle);

	IOBJ_SET_STORE_HANDLE_MAGIC(handle);

	/*
	 * fill out the handle to be returned
	 */
	handle->name = strdup(store_name);
	ASSERT(handle->name);
	handle->snapshot = snapshot;
	handle->version = version;
	handle->path = strdup(path);
	handle->store_type = OST_NAMESPACE;
	ASSERT(handle->path);

	if (error)
		isi_error_handle(error, error_out);

	return handle;
}

/**
 * The primary purpose of this function is to fill in the root directory used
 * by all stores and return that in the struct fmt.  It will do so, even when
 * returning an error.
 *
 * In addition, this function will make sure that store exists, and create it
 * if it doesn't.  An error will be returned if we fail to create the store.
 *
 * @a fmt  -- will be filled with the name of the root store.
 * @a error_out -- set if we fail to create the store.
 */
static void
create_ns_store_root(struct fmt *fmt, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(tpath);

	/* first fill in the store-uber-root */
	fmt_print(fmt, "%s", NS_STORE_AP_ROOT);

	/* Now see if we have access to it.  If so, we're done. */
	if (!eaccess(fmt_string(fmt), OSTORE_STORE_ACCESS_CHECK))
		goto out;

	/* If we can't access it because it doesn't exist, try to fix that.
	   For any other reason, return the error */
	if (errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "create_ns_store_root() failed access(%s)",
		    fmt_string(fmt));
		goto out;
	}

	/* only allow creation if ifs is mounted */
	bool mounted = ifs_is_mounted(&error);
	if (error)
		goto out;
	if (!mounted) {
		error = ostore_store_not_mounted_error_new("/ifs not mounted");
		goto out;
	}

	/* now create the dirl */
	mkdir_dash_p(&tpath, fmt_string(fmt),
	    OSTORE_ROOT_DEFAULT_MODE | S_IROTH, NULL, NULL,
	    OSTORE_ACL_NONE, OSTORE_ACL_NONE, NULL, NULL, &error);
	if (error)
		goto out;

out:
	if (error)
		isi_error_handle(error, error_out);
}

/**
 * Validate the store path is valid OneFS directory and not system internally
 * used.
 * @param[IN] ns_path - pathname for the store
 * @return isi_error object if the given @ns_path is not valid
 */
static struct isi_error *
validate_store_path(const char *ns_path)
{
	struct statfs fs_stat;
	if (statfs(ns_path, &fs_stat)) {
		if (errno == ENOENT) {
			return ostore_pathname_not_exist_error_new(
			    "%s", ns_path);
		} else if (errno == ENOTDIR) {
			return ostore_pathname_not_dir_error_new(
			    "%s", ns_path);
		}
		return isi_system_error_new(errno,
		    "failed to stat %s", ns_path);
	}
	if (strcmp(fs_stat.f_mntfromname, ONE_FS_NAME)) {
		return ostore_pathname_not_onefs_error_new(
		    "%s", ns_path);
	}

	if (is_prohibited_dir(ns_path, "")) {
		return ostore_pathname_prohibited_error_new(
		    "%s", ns_path);
	}
	return NULL;
}

// structure holding system directories which are prohibited from user ops
struct system_dirs_t {
	int count;
	char *sys_paths[0];
};

static struct system_dirs_t *system_dirs = NULL;

static void
free_system_dirs(void)
{
	if (!system_dirs)
		return;
	for(int idx = 0; idx < system_dirs->count; ++idx)
		free(system_dirs->sys_paths[idx]);
	free(system_dirs);
	system_dirs = NULL;
}

/**
 * Set ostore system directories to which regular operation should be prohibited.
 *
 * @param[IN] paths - system directories
 * @return number of paths extracted from the given string
 */
int
ostore_set_system_dirs(const char *paths)
{
	int count = 0;
	const char *p_paths = paths;
	while (*p_paths) {
		while (isspace(*p_paths) || *p_paths == ',')
			++p_paths;
		if (*p_paths)
			++count;
		while (*p_paths && !isspace(*p_paths) && *p_paths != ',')
			++p_paths;
	}
	if (count <= 0)
		return 0;

	struct system_dirs_t *sys_dirs = (struct system_dirs_t *) malloc(
	    sizeof(struct system_dirs_t) + sizeof(const char *) * count);
	char **sys_paths = sys_dirs->sys_paths;
	int idx = 0;
	p_paths = paths;
	while (idx < count) {
		while (isspace(*p_paths) || *p_paths == ',')
			++p_paths;
		const char *s_b =  p_paths;
		while (*p_paths && !isspace(*p_paths) && *p_paths != ',')
			++p_paths;
		const char *s_e = p_paths;
		sys_paths[idx] = (char *) malloc(s_e - s_b + 1);
		strncpy(sys_paths[idx], s_b, s_e - s_b);
		sys_paths[idx][s_e - s_b] = '\0';
		++idx;
	}
	sys_dirs->count = count;
	free_system_dirs();
	system_dirs = sys_dirs;
	return count;
}

/*
 * Checks if the given path is system internally used and prohibited from
 * access by namespace store user.
 *
 * @param[IN] base_path - store path
 * @param[IN] dir_path - directory pathname relative to store
 * @return true if the path is prohibited from access; false otherwise
 */
bool
is_prohibited_dir(const char *base_path, const char *dir_path)
{
	if (!system_dirs)
		return false;

	ASSERT(base_path && dir_path);
	struct fmt FMT_INIT_CLEAN(tmp_path_f);
	fmt_print(&tmp_path_f, "%s/%s", base_path, dir_path);
	const char *tmp_path = fmt_string(&tmp_path_f);
	int idx = 0;
	for (; idx < system_dirs->count; ++idx) {
		const char *proh_name = system_dirs->sys_paths[idx];
		const char *tmp = tmp_path;

		while (*tmp == '/')
			++tmp;
		while (*proh_name == '/')
			++proh_name;
		while (*proh_name && *tmp && *tmp == *proh_name) {
			while (*tmp && *tmp == *proh_name) {
				++tmp;
				++proh_name;
			}
			while (*tmp == '/')
				++tmp;
			while (*proh_name == '/')
				++proh_name;
		}
		if (!*proh_name)
			return true;
	}
	return false;
}

struct ostore_handle *ns_store_create(
    const char *store_name,
    const char *ns_path,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ostore_return = NULL;
	struct fmt FMT_INIT_CLEAN(store_fmt);

	struct fmt FMT_INIT_CLEAN(tmp_fmt);
	char *tempxx_path = NULL;
	bool tmp_removal_needed = false;
	int fd = -1;

	/*
	 * allow for the store object to be created without the mode
	 * bits specified being masked.
	 */
	umask(0);

	/*
	 * The initial structure for the datastore layout must exist ahead for
	 * time before the store is created.
	 */
	create_ns_store_root(&store_fmt, &error);
	if (error)
		goto out;

	fmt_print(&store_fmt, "/%s", store_name);

	if (strcmp(store_name, DEFAULT_NS) == 0) {
		struct stat sb;
		// default ns store 'ifs' must point to DEFAULT_NS_PATH
		ASSERT(!strcmp(ns_path, DEFAULT_NS_PATH));
		ns_path = DEFAULT_NS_PATH;
		if (!stat(fmt_string(&store_fmt), &sb))
			goto created; // we don't create ifs twice
	}

	/*
	 * check that the namespace store path is valid
	 */
	if ((error = validate_store_path(ns_path)))
		goto out;

	/* create a tmp store directory */
	if (!mkdir(NS_STORE_TMP, OSTORE_STORE_DEFAULT_MODE)) {
		if (chmod(NS_STORE_TMP, OSTORE_STORE_DEFAULT_MODE)) {
			error = isi_system_error_new(errno,
			    "ns_store_create() failed to set proper mode for"
			    " temp directory (%s)",
			    OBJECT_STORE_TMP);
			ilog(IL_ERR, "%s", isi_error_get_message(error));
		}
	} else if (errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "ns_store_create() failed to create temp directory (%s)",
		    OBJECT_STORE_TMP);
		ilog(IL_ERR, "%s", isi_error_get_message(error));
		goto out;
	}


	/* to create access point for store */
	// tmp file name length needs to be less NAME_MAX
	fmt_print(&tmp_fmt, "%s/%.*s.XXXXXX", NS_STORE_TMP,
	    NAME_MAX - (int) strlen(".XXXXXX"), store_name);
	tempxx_path = fmt_detach(&tmp_fmt);
	fd = mkstemp(tempxx_path); // 0600 is default mode here
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "failed to create temp file in ns_store_create");
		goto out;
	}

	tmp_removal_needed = true;

	int rcount = strlen(ns_path);
	int wcount = 0, wtotal = 0, wrcount = rcount;
	while (wtotal < rcount) {
		wcount = write(fd, ns_path + wtotal, wrcount);
		if (wcount == -1) {
			error = isi_system_error_new(errno,
			    "failed to write temp file in ns_store_create");
			goto out;
		}
		wtotal += wcount;
		wrcount -= wcount;
	}


#if 0  //this would change target ACL, need review
	/*
	 * Set the default ACL for the store.
	 */
	ostore_set_default_acl(fmt_string(&store_fmt), -1, OSTORE_ACL_STORE,
	    NULL, &error);
	if (error)
		goto out;
#endif

	// this replaces old access point if it exists
	if (rename(tempxx_path, fmt_string(&store_fmt))) {
		error = isi_system_error_new(errno,
		    "failed to rename temp file in ns_store_create");
		goto out;
	}
	tmp_removal_needed = false;
created:
	/*
	 * we have created a new store.  Call the open routine to
	 * handle the rest.  Specify the primary snap as we have just
	 * created it.
	 */
	ostore_return = ns_store_cache_add(store_name, HEAD_SNAPID,
	    OBJECT_STORE_VERSION, fmt_string(&store_fmt), &error);

 out:
	if (error)
		isi_error_handle(error, error_out);
	if (tmp_removal_needed)
		remove(tempxx_path);
	if (tempxx_path)
		free(tempxx_path);
	if (fd > 0)
		close(fd);
	return ostore_return;
}

/**
 * Delete an existing object store.
 */
struct isi_error *
ns_store_destroy(const char *store_name)
{
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(store_fmt);

	// we need to get the store access point
	create_ns_store_root(&store_fmt, &error);
	if (error)
		goto out;
	fmt_print(&store_fmt, "/%s", store_name);

	//we don't remove default namespace
	if (strcmp(store_name, DEFAULT_NS) == 0)
		goto out;

	/* create symbol link for ns access point */
	if (remove(fmt_string(&store_fmt)) &&
	    (errno != ENOENT)) {
		// store exists, but cannot remove
		error = isi_system_error_new(errno,
		    "ns_store_destroy() failed to delete access point(%s)",
		    fmt_string(&store_fmt));
		goto out;
	}

 out:
	return error;
}

// for namespace store initial setup
static void __attribute__ ((constructor)) ns_init(void);

void
ns_init(void)
{
	struct isi_error *error = 0;
	struct ostore_handle *oh = ns_store_create(DEFAULT_NS,
	    DEFAULT_NS_PATH, &error);

	if (error) {
		ilog(IL_ERR, "Failed to create the default store '%s': %#{}",
		    DEFAULT_NS, isi_error_fmt(error));
	    isi_error_free(error);
	    exit(1);
	} else
		ns_store_close(oh, &error);
}

// for namespace store finish up
static void __attribute__ ((destructor))  ns_finish(void);

void
ns_finish(void)
{
	free_system_dirs();
}

/**
 * namespace list store callback function
 * @param[IN] context - the callback context pointer
 * @param[IN] info - the object info
 */
static bool
ns_list_store_callback(void *context, const struct iobj_list_obj_info *info)
{
	struct iobj_list_obj_param *param =
	    (struct iobj_list_obj_param *) context;
	struct isi_error *error = NULL;
	char p_path[PATH_MAX];
	const char *old_path = info->path;

	error = read_ns_store_target(info->name, p_path, PATH_MAX);
	if (error) {
		if (!isi_system_error_is_a(error, EACCES))
			ilog(IL_ERR, "%s", isi_error_get_message(error));
		isi_error_free(error);
		return true; // skip this store
	}

	// we can dup info, but for efficiency, change on original copy
	((struct iobj_list_obj_info *) info)->object_type = OOT_CONTAINER;
	((struct iobj_list_obj_info *) info)->path = p_path;

	bool b_ret = param->caller_cb(param->caller_context, info);

	((struct iobj_list_obj_info *) info)->path = old_path;
	return b_ret;
}

/*
 * The implementation currently takes advantage of existing object list
 * feature. It may change when we have more requirements on store list.
 */
size_t
ns_store_list(const char *acct_name, struct iobj_list_obj_param *param,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t numRet = -1;

	int fd = open(NS_STORE_AP_ROOT, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Cannot read directory %s", NS_STORE_AP_ROOT);
		goto out;
	}
	struct iobj_list_obj_param param_v;
	memcpy(&param_v, param, sizeof param_v);
	param_v.caller_cb = ns_list_store_callback;
	param_v.caller_context = param;
	param_v.ibh = 0;

	struct ns_list_obj_param ns_param_v = {
	    &param_v, 0, fd, NULL, true, false, 0};

	numRet = nsp_object_list(&ns_param_v, &error);
	if (error)
		goto out;

out:
	if (error) {
		ilog(IL_ERR, "%s", isi_error_get_message(error));
		isi_error_handle(error, error_out);
	}
	if (fd > 0)
		close(fd);

	return numRet;
}

/*
 * Open the object store
 */
struct ostore_handle *
ns_store_open(const char *store_name,
    uint64_t snapshot,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ostore_handle *ostore_return = NULL;
	struct fmt FMT_INIT_CLEAN(store_path);

	/*
	 * Generate the pathname for the object store
	 */
	if ((error = ns_resolve_store_path(store_name, &store_path)))
		goto out;

	ostore_return = ns_store_cache_add(store_name, snapshot,
	    NS_STORE_VERSION, fmt_string(&store_path), &error);

 out:
	if (error)
		isi_error_handle(error, error_out);

	return ostore_return;
}

/*
 * Close the object store
 */
void
ns_store_close(struct ostore_handle *ios, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	IOBJ_CHECK_STORE_HANDLE(ios, error, out);

	ns_store_cache_remove(ios);

out:

	if (error)
		isi_error_handle(error, error_out);
}

void
ns_store_attributes_get(struct ostore_handle *ios,
    struct iobj_ostore_attrs **attrs, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
}


void
ns_store_attributes_set(struct ostore_handle *ios,
    struct iobj_ostore_attrs *attrs, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
}


void
ns_store_attributes_release(struct iobj_ostore_attrs *attrs)
{

}

void
ns_store_parameters_release(struct ostore_parameters *params)
{
}

void
ns_store_parameters_set(struct ostore_handle *ios,
    struct ostore_parameters *params, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
}

void
ns_store_parameters_get(struct ostore_handle *ios,
    struct ostore_parameters **params, struct isi_error **error_out)
{
	*error_out = ostore_error_new("Operation not supported");
}


void ns_store_dump(void)
{

}

struct igenobj_handle *
ns_genobj_open(struct ostore_handle *ios,
    const char *path,  int flags,
    enum ifs_ace_rights ace_rights_open_flg,
    struct ifs_createfile_flags cf_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct igenobj_handle *io_handle = NULL;
	struct fmt FMT_INIT_CLEAN(opath);
	const char *obj_name;
	struct stat sb = {0};
	int fd = -1;
	struct ifs_createfile_ex cex = {};

	if (path == NULL)
		fmt_print(&opath, "%s/%s", NS_STORE_AP_ROOT, ios->name);
	else if ((error = ns_resolve_gen_path(ios, path, &opath)))
		goto out;

	if (ace_rights_open_flg == BUCKET_WRITE)
                flags = O_DIRECTORY;

        if (ace_rights_open_flg == BUCKET_WRITE)
                flags |= O_DIRECTORY;

	cex.cf_flags = cf_flags;
	fd = ifs_createfile(-1, fmt_string(&opath), ace_rights_open_flg, flags,
	    0, &cex);

	if (fd == -1) {
		switch (errno) {
		case ENOENT:
			error = ostore_object_not_found_error_new(
			    "Object not found in path %s in store %s",
			    path, ios->name);
			break;
		case EACCES:
			error = ostore_access_denied_error_new(
				"Access denied. path %s in store %s",
				path, ios->name);
			break;
		case EOPNOTSUPP:
			if (stat(fmt_string(&opath), &sb) == -1) {
				error = isi_system_error_new(errno,
				    "Cannot get status of %s ",
				    fmt_string(&opath));
				goto out;
			}
			error = ostore_unsupported_object_type_error_new(
				"Cannot open object with path %s in store %s. "
				"Unsupported type : %d",
				path, ios->name, sb.st_mode);
			break;
		case EINVAL:
			error = ifs_createfile_invalid_arg_error();
			break;
		default:
			error = isi_system_error_new(errno,
			    "Cannot open object with path %s in store %s",
			    path, ios->name);
			break;
		}
		goto out;
	}

	if ((error = ensure_object_on_onefs(fd)))
		goto out;

	if (fstat(fd, &sb) == -1) {
		error = isi_system_error_new(errno,
		    "Cannot get status of %s ", fmt_string(&opath));
		goto out;
	}

	obj_name = get_genobj_name_from_path(fmt_string(&opath), ios->path);
	if (sb.st_mode & S_IFDIR && (0 == (sb.st_mode & S_IFCHR))) {
		io_handle = iobj_bucket_to_genobj(
		    bucket_handle_create(ios, obj_name,
		    fmt_string(&opath), fd, NULL,
		    &error));
	}
	else if (sb.st_mode & S_IFREG) {
		io_handle = iobj_object_to_genobj(
		    object_handle_create(ios, NULL, obj_name,
		    fmt_string(&opath), fd,
		    -1, flags, NULL, NULL));
	}
	else {
		error = ostore_unsupported_object_type_error_new(
		    "Cannot open object with path %s in store %s. "
		    "Unsupported type : %d",
		    path, ios->name, sb.st_mode);
	}

 out:
	if (error) {
		if (fd != -1)
			close(fd);
		isi_error_handle(error, error_out);
	}

	/*
	 * return the object handle on success and a NULL on failure where
	 * error_out should contain the error details.
	 */

	return io_handle;
}

void
ns_genobj_delete(struct ostore_handle *ios, const char * path,
    struct ostore_del_flags flags, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat s;
	bool isdir = false;
	struct fmt FMT_INIT_CLEAN(opath);

	ASSERT(error_out);

	if ((error = ns_resolve_gen_path(ios, path, &opath))) {
		goto out;
	}

	// Perform a worm delete if the worm option is set
	if (flags.worm) {
		int rc = worm_override_delete_file(fmt_string(&opath));
		if (rc == -1) {
			error = isi_system_error_new(errno,
			    "Error performing worm delete %s",
			    fmt_string(&opath));
		}
		goto out;
	}

	// Do stat to find out if the path is a directory or file.
	if (stat(fmt_string(&opath), &s) == 0) {
    	    if (s.st_mode & S_IFDIR) {
		isdir = true;
	    }
	}
	else {
	    error = isi_system_error_new(errno,
		"Unable to find file %s", fmt_string(&opath));
	    goto out;
	}

	if (isdir && flags.recur) {
	    int err_no = 0;
	    bool cont_on_error = flags.cont_on_err;
	    struct fmt FMT_INIT_CLEAN(err_path);

	    if (ostore_ifs_remove(fmt_string(&opath), cont_on_error, /////递归删除目录下的文件或者子目录
	    	&err_no, &err_path) != 0) {
		error = isi_system_error_new(err_no,
		    "Cannot delete directory %s",
		    fmt_string(&err_path));
	    }

	} else if (remove(fmt_string(&opath))) { ///////remove只能删除指定的文件.无法删除其它文件,子目录
	    error = isi_system_error_new(errno,
		"Cannot delete File or directory %s", fmt_string(&opath));
	}

out:
	if (error)
	    isi_error_handle(error, error_out);
}


/** copy context of ns store  */
struct ns_copy_context {
	int src_base_len; /** length of src pathname */
	int dest_base_len;/** length of dest pathname */
	struct ostore_copy_param *cp_param;
};

static bool
ns_copy_cb(void *ctx, const struct fsu_copy_rpt *rpt_info)
{
	struct ns_copy_context *ns_ctx = (struct ns_copy_context *) ctx;
	struct ostore_copy_param *cp_param = ns_ctx->cp_param;
	struct fsu_copy_param *b_param = (struct fsu_copy_param *) cp_param;

	if (b_param->cp_callback && rpt_info->severity >= COPY_RPT_WARNING) {
		struct fmt FMT_INIT_CLEAN(dest);
		struct fmt FMT_INIT_CLEAN(src);
		// converting back to relative path of the namespace
		const char *src_r = rpt_info->src +
		    strlen(cp_param->src_ios->path);
		const char *dest_r = b_param->dest;
		fmt_print(&src, "/%s%s", cp_param->src_ios->name, src_r);
		fmt_print(&dest, "/%s/%s%s", cp_param->dest_ios->name,
		    dest_r,  rpt_info->src + ns_ctx->src_base_len);
		struct fsu_copy_rpt rpt = {rpt_info->severity,
		    rpt_info->on_dest, fmt_string(&src),
		    fmt_string(&dest), rpt_info->msg};

		return b_param->cp_callback(b_param->caller_context, &rpt);
	}
	return true;
}

/*
 * To ensure destination path for copy is not same or sub-dir of source
 *
 * @param[in] psrc - source path
 * @param[in] pdest - destination path
 * @return ostore_copy_to_sub_dir_error if destination is same or sub-dir
 * of the source
 */
static struct isi_error *
ensure_dest_not_in_src_dir(const char *psrc, const char *pdest)
{
	char res_src[PATH_MAX], res_dest[PATH_MAX];
	char *ps = NULL;

	ps = realpath(psrc, res_src);
	if (!ps) {
		//can't check, let it fail later so error msg be consistent
		return NULL;
	}
	// realpath returns NULL if pdest is not resovable, however, the resovable
	// parent path is still in res_dest
	realpath(pdest, res_dest);
	if (strncmp(res_src, res_dest, strlen(res_src)) == 0) {
		char *ts = res_dest + strlen(res_src);
		// dest is same dir or sub dir of src
		if (*ts == '\0' || *ts == '/' ||
		    (*ts != '/' && *(ts - 1) == '/'))
			return ostore_copy_to_sub_dir_error_new("");
	}
	return NULL;
}

int
ns_genobj_copy(struct ostore_copy_param *cp_param, struct isi_error **err_out)
{
	ASSERT(cp_param && err_out && *err_out == NULL);
	struct isi_error *error = 0;
	struct stat src_stat;
	struct fmt FMT_INIT_CLEAN(src_path);
	struct fmt FMT_INIT_CLEAN(dest_path);
	struct fsu_copy_path to_path;
	enum fsu_copy_type type = FILE_TO_FILE;
	int fts_options = FTS_NOCHDIR | FTS_PHYSICAL | FTS_ADS;
	int rval = -1;
	// see cp manual for details.
	int Hflag = 0; // not to follow symbol link if src_path is link
	int Lflag = 0; // not to follow link during tree traversal

	struct fsu_copy_param *b_param = (struct fsu_copy_param *) cp_param;
	struct fsu_copy_flags cp_flags = b_param->cp_flags;

	if ((error = ns_resolve_gen_path(cp_param->dest_ios, b_param->dest,
	    &dest_path)))
		goto out;
	if ((error = ns_resolve_gen_path(cp_param->src_ios, b_param->src,
	    &src_path)))
		goto out;

	if (cp_flags.recur && (Lflag || Hflag))
		stat(fmt_string(&src_path), &src_stat);
	else
		lstat(fmt_string(&src_path), &src_stat);

	cp_flags.recur = true; //always recursive for namespace folder
	if (S_ISDIR(src_stat.st_mode) && cp_flags.recur) {
		if ((error = ensure_dest_not_in_src_dir(
		    fmt_string(&src_path), fmt_string(&dest_path))))
			goto out;
		type = DIR_TO_DNE;
	} else
		cp_flags.cont = false; // no continue for file

	if (cp_flags.recur) {
		if (Hflag)
			fts_options |= FTS_COMFOLLOW;
		if (Lflag) {
			fts_options &= ~FTS_PHYSICAL;
			fts_options |= FTS_LOGICAL;
		}
	} else {
		fts_options &= ~FTS_PHYSICAL;
		fts_options |= FTS_LOGICAL | FTS_COMFOLLOW;
	}

        // target to path
	strncpy(to_path.p_path, fmt_string(&dest_path), PATH_MAX);
	to_path.p_path[PATH_MAX -1] = '\0';

	to_path.p_end = to_path.p_path + strlen(to_path.p_path);
        if (to_path.p_path == to_path.p_end) {
		*to_path.p_end++ = '.';
		*to_path.p_end = 0;
	}

	if (to_path.p_end[-1] == '/')
		STRIP_TRAILING_SLASH(to_path);
	to_path.target_end = to_path.p_end;

	struct ns_copy_context ns_ctx = {strlen(fmt_string(&src_path)),
	    to_path.target_end - to_path.p_path, cp_param };

	struct fsu_copy_param ns_cp_param;
	ns_cp_param.src = fmt_string(&src_path);
	ns_cp_param.dest = fmt_string(&dest_path);
	ns_cp_param.cp_flags = b_param->cp_flags;
	ns_cp_param.cp_callback = ns_copy_cb;
	ns_cp_param.caller_context = (void *) &ns_ctx;

	rval = ostore_ifs_copy(&ns_cp_param, &to_path, type, fts_options,
	    NULL, &error);

out:
	isi_error_handle(error, err_out);
	return rval;
}


/*
 * Move source object to new destination
 *
 * returns 0 if successful; -1 if error detected prior to move;
 *         -2 if move (rename) fails
 * err_out will contain the specific error in case of failure
 */
int
ns_genobj_move(struct ostore_handle *src_ios, const char *src,
    struct ostore_handle *dest_ios, const char *dest,
    struct fsu_move_flags *mv_flags, struct isi_error **err_out)
{
	ASSERT(src && dest && src_ios && dest_ios);
	ASSERT(err_out && *err_out == NULL);
	int rval = -1;
	struct isi_error *error = NULL;
	struct stat src_stat = {0}, dest_stat = {0};
	struct fmt FMT_INIT_CLEAN(src_path);
	struct fmt FMT_INIT_CLEAN(dest_path);

	if ((error = ns_resolve_gen_path(dest_ios, dest, &dest_path)))
		goto out;
	if ((error = ns_resolve_gen_path(src_ios, src, &src_path)))
		goto out;

	if (stat(fmt_string(&src_path), &src_stat) &&
	    (errno == ENOENT || errno == ENOTDIR)) {
		error = isi_system_error_new(errno, "old pathname");
		goto out;
	}

	if (!stat(fmt_string(&dest_path), &dest_stat) &&
            (!mv_flags->ovw)) { // check overwrite
		error = isi_system_error_new(EEXIST, "new pathname");
		goto out;
	}

	// TODO: we need to consider across quota domain
	if (rename(fmt_string(&src_path), fmt_string(&dest_path))) {
		error = isi_system_error_new(errno, "move");
		rval = -2;
		goto out;
	}
	rval = 0;
out:
	if (error)
		isi_error_handle(error, err_out);
	return rval;
}

/*
 * Get Worm attributes and domains for a general namespace object
 */
void
ns_genobj_get_worm(struct igenobj_handle *igobj_hdl,
    struct object_worm_attr *worm_attr,
    struct object_worm_domain *worm_dom,
    struct object_worm_domain **worm_ancs, size_t *num_ancs,
    struct isi_error **error_out)
{
	ASSERT(igobj_hdl && worm_attr && worm_dom);
	struct isi_error *error = NULL;
	int ret = 0;
	int fd = igobj_hdl->fd;
	struct stat st;
	struct domain_set doms;
	struct domain_set worm_ancestors;
	struct domain_entry entry = {};
	struct worm_state worm = {};
	ifs_domainid_t domid;
	size_t i = 0, j = 0;

	domain_set_init(&doms);
	domain_set_init(&worm_ancestors);

	ret = fstat(fd, &st);

	if (!ret) {
		ret = dom_get_info_by_lin(st.st_ino, st.st_snapid,
		    &doms, &worm_ancestors, &worm);
	}
	if (ret) {
		error = isi_system_error_new(errno, "cannot get object "
		    "WORM information");
		goto out;
	}

	ret = dom_get_matching_domains(&doms, DOM_WORM | DOM_READY,
	    1, &domid,  &entry);

	if (ret < 0) {
		error = isi_system_error_new(errno, "cannot get object "
		    "domain information");
		goto out;
	} else if (ret > 0) {
		worm_attr->w_attr = worm;
		worm_attr->w_is_valid = true;
		worm_dom->dom_entry = entry;
		worm_dom->domain_id = domid;
		ifs_lin_t lin = entry.d_root;
		ifs_lin_t root_lin = 0;
		ifs_snapid_t snapid = HEAD_SNAPID;
		lin_get_path(root_lin, lin, snapid, 0, 0, NULL,
		    &worm_dom->domain_root);

	} else
		worm_attr->w_is_valid = false;

	if (worm_ancs != NULL) {
		*worm_ancs = calloc(domain_set_size(&worm_ancestors),
		    sizeof(struct object_worm_domain));
		if (*worm_ancs == NULL) {
			error = isi_system_error_new(ENOMEM,
			    "could not allocate WORM ancestor list");
			goto out;
		}

		j = 0;
		for (i = 0; i < domain_set_size(&worm_ancestors); i++) {
			struct domain_entry de = {};
			struct object_worm_domain *anc = (*worm_ancs) + j;
			if (dom_get_entry(domain_set_atindex(&worm_ancestors,
			    i), &de) < 0)
				continue;
			anc->dom_entry = de;
			anc->domain_id =
			    domain_set_atindex(&worm_ancestors, i);
			lin_get_path(0, de.d_root, HEAD_SNAPID, 0, 0, NULL,
			    &anc->domain_root);
			j++;
		}
	}

out:
	if (error == NULL && worm_ancs != NULL && num_ancs != NULL)
		*num_ancs = j;
	domain_set_clean(&worm_ancestors);
	domain_set_clean(&doms);
	if (error)
	    isi_error_handle(error, error_out);
}



void ns_genobj_get_protection(struct igenobj_handle *igobj_hdl,
    struct pctl2_get_expattr_args *ge, struct isi_error **error_out)
{

	ASSERT(igobj_hdl && ge && !*error_out);

	int fd = igobj_hdl->fd;

	struct isi_error *error = NULL;

	int x = pctl2_get_expattr(fd, ge);

	if(x == -1)
		error = isi_system_error_new(errno,
		    "Cannot Retrieve Protection Attributes");

	if(error)
		isi_error_handle(error, error_out);

	return;
}


void ns_genobj_set_protection(struct igenobj_handle *igobj_hdl,
    struct pctl2_set_expattr_args *se, struct isi_error **error_out)
{
	ASSERT(igobj_hdl && se && !*error_out);

	int fd = igobj_hdl->fd;

	struct isi_error *error = NULL;

	int x = pctl2_set_expattr(fd, se);

	if(x == -1)
		error = isi_system_error_new(errno, "Cannot Set Protection Attributes");

	if(error)
		isi_error_handle(error, error_out);

	return;

}

