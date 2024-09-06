
#ifndef __FS_OP_UTILS__H__
#define __FS_OP_UTILS__H__

#ifdef __cplusplus
extern "C" {
#endif

#include <ifs/ifs_types.h>

#define	STRIP_TRAILING_SLASH(p) {					\
        while ((p).p_end > (p).p_path + 1 && (p).p_end[-1] == '/')	\
                *--(p).p_end = 0;					\
}

enum fsu_copy_type { FILE_TO_FILE, FILE_TO_DIR, DIR_TO_DNE };

enum fsu_copy_rpt_severity {
	COPY_RPT_MIN = 1,
	COPY_RPT_INFO = 1,
	COPY_RPT_WARNING = 2,
	COPY_RPT_ERROR = 3,
	COPY_RPT_ERR_INTERNAL = 4,
	COPY_RPT_MAX = COPY_RPT_ERR_INTERNAL
};

/**
 * Path structure used for copy
 */
struct fsu_copy_path {
	char	*p_end;			/* pointer to NULL at end of path */
	char	*target_end;		/* pointer to end of target base */
	char	p_path[PATH_MAX];	/* buffer of the path */
};

/**
 * copy flags
 */
struct fsu_copy_flags {
	int recur:1; // recursive
	int merge:1; // merge dir if copy destination dir exists
	int noovw:1; // no overwrite
	int cont:1;  // continue to copy the rest even if failed to
		     // copy an object
	int pflag:1; // preserve mode,ownership,timestamps
	int cflag:1; // clone
	mode_t mask; // used for target
	// below TBD
	int force:1; // for existing target, remove it and recreate without
	             // confirmation regardless of permissions
	int iflag:1; // interactive
	int lflag:1; // follow link
	int vflag:1; // verbose
	int bigbuf:1; //big buffer
};


/** callback context for copy message report */
struct fsu_copy_rpt {
	enum fsu_copy_rpt_severity severity;
	bool on_dest; // whether msg is on destination side, i.e. write;
	const char *src;
	const char *dest;
	const char *msg;
};


/**
 * callback prototype for copy
 *
 * @param[IN] context - the context passed down to copy api
 * @param[IN] rpt_info - the report generated from copy operation
 * @return boolean value ignored by copy api
 * (TODO: some implementation can leverage this to end copying)
 */
typedef bool (*fsu_copy_cb)(void *context,
    const struct fsu_copy_rpt *rpt_info);

struct fsu_copy_param {
	const char *src;
	const char *dest;
	struct fsu_copy_flags cp_flags;

	fsu_copy_cb cp_callback; /* callback for reporting error or info */
	void *caller_context; /* opaque caller allocated context */
};

struct ostore_handle;
/**  */
struct ostore_copy_param {
	struct fsu_copy_param base_param;
	struct ostore_handle *src_ios;
	struct ostore_handle *dest_ios;
};


struct isi_error;

/**
 * Copy of a file or directory
 *
 * @param[IN] cp_param - parameter for copy operation
 * @param[OUT] error   - error object if fails
 * @return 0 if no error occurs; otherwise non-0
 */
int fsu_copy(struct fsu_copy_param *cp_param, struct isi_error **error);


/**
 * Copy of a file or directory
 *
 * @param[IN] cp_param - parameter for copy operation
 * @param[IN] to_path  - the target path of copy
 * @param[IN] type     - type of copy
 * @param[IN] fts_options - options control file hierarchy travel behavior
 * @param[IN] cp_flags    - flags control copy behavior
 * @param[IN] to_tmp   - the temp pathname used for atomic copy of file object
 * @param[OUT] error   - error object if fails
 * @return 0 if no error occurs; otherwise non-0
 */
int ostore_ifs_copy(struct fsu_copy_param *cp_param,
    struct fsu_copy_path *to_path, enum fsu_copy_type type, int fts_options,
    struct fsu_copy_path *to_tmp, struct isi_error **error);


/**
 * Recursively remove directory
 *
 * @err_no and @err_path must be valid.
 *
 * @param[IN] path - pathname to be removed
 * @param[IN] cont_on_err - whether to continue the rest upon error
 * @param[IN] err_no - error code
 * @param[IN] err_path - the path causes the error
 * @return 0 if success; -1 otherwise
 */
int ostore_ifs_remove(const char *path, bool cont_on_err, int *err_no,
    struct fmt *err_path);

/*
 * protection setting related structure
 */

// we decide not to support protection level/policy, etc...
// As most of fields / code are removed, this structure looks strange since the
// only field is for node pool id. we'd like to keep it as is for possible
// additional extension in the future.
struct iobj_set_protection_args {
	ifs_disk_pool_policy_id_t node_pool_id;
};

/**
 * Sets protection property to file or directory.
 *
 * It checks existing protection on the target. If there is no change,
 * then simply returns.
 * @PARAM[IN] fd - a file descriptor
 * @PARAM[IN] iobj_se - structure containing the protection property
 * @param[OUT] error_out - the isi_error returned in case of failure
 */
void ostore_ifs_checked_set_protection_fd(int fd,
    const struct iobj_set_protection_args *iobj_se,
    struct isi_error **error_out);

/**
 * Move Flags
 */
struct fsu_move_flags {
	int ovw:1; // overwrite
};

#ifdef __cplusplus
}
#endif

#endif /* __FS_OP_UTILS__H__ */
