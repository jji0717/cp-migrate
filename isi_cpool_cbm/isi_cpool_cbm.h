#ifndef __ISI_CLOUD_CBM__H__
#define __ISI_CLOUD_CBM__H__

#include <pthread.h>
#include <uuid.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>

#include <isi_config/ifsconfig.h>
#include <isi_cpool_d_common/cpool_d_common.h>
/**
 * CBM public C interfaces.
 */

__BEGIN_DECLS

/**
 * isi_error is the error delivery mechanism used in this set of APIs.
 * The isi_error can either be isi_system_error or isi_cbm_error.
 */
struct isi_error;

/**
 * The opaque CBM file object
 */
struct isi_cbm_file;

/**
 * The CBM account id
 */
typedef char cluster_guid[IFSCONFIG_GUID_STR_SIZE];

struct isi_cbm_account_id {
	/**
	 * The original cluster ID for the stub referring the account.
	 * It is NULL terminated.
	 */
	const cluster_guid cluster_id;
	unsigned int account_id;
};

/**
 * The CBM policy id.
 */
struct isi_cbm_policy_id {
	/**
	 * The original cluster ID for the stub referring the policy.
	 * It is NULL terminated.
	 */
	const cluster_guid cluster_id;
	unsigned int policy_id;
};

/**
 * Opaque account information binary object
 */
struct isi_cbm_account_se;

/** Opaque policy information binary object
 */
struct isi_cbm_policy_se;

/**
 * Opaque encryption key record binary object
 */
struct isi_cbm_enc_key_rec;

/**
 * stub map iterator
 */
struct isi_cbm_map_iterator;

/**
 * Opaque map record blob
 */
struct isi_cbm_map_record;

/**
 * Opaque stub cache range map iterator
 */
struct isi_cbm_cache_iterator;

/**
 * Opaque stub cache entry record binary object
 */
struct isi_cbm_cache_record;

/**
 * stub file version
 */
struct isi_cbm_file_version;

/*
 * Cache region status information (per region). INPROGRESS is a dirty region
 * that was update while a sync back operation on the file was being
 * performed.
 */
enum isi_cbm_cache_status {
	ISI_CPOOL_CACHE_INVALID		= -1,
	ISI_CPOOL_CACHE_NOTCACHED	= 0,
	ISI_CPOOL_CACHE_CACHED		= 1,
	ISI_CPOOL_CACHE_DIRTY		= 2,
	ISI_CPOOL_CACHE_INPROGRESS	= 3
};

/*
 * Mask to allow for selecting the status value(s) of interest while looking
 * for ranges of a particular status value(s).  THis allows for looking to all
 * cached (clean or dirty) by specifying CACHED|DIRTY|INPROGRESS or all dirty
 * (DIRTY|INPROGRESS), or just those that have been changed during sync
 * (INPROGRESS).
 */
enum isi_cbm_cachemask {
	ISI_CPOOL_CACHE_MASK_NOTCACHED  = 0x01,
	ISI_CPOOL_CACHE_MASK_CACHED	= 0x02,
	ISI_CPOOL_CACHE_MASK_DIRTY	= 0x04,
	ISI_CPOOL_CACHE_MASK_INPROGRESS	= 0x08
};

/**
 * Define the backup type when a stub file is marked as "backed-up"
 */
enum isi_cbm_backup_type {
	BT_TERM_INVALID = -1,
	/**
	 * Short term backup
	 */
	BT_SHORT_TERM = 0,
	/**
	 * Long term backup
	 */
	BT_LONG_TERM = 1,
};

/**
 * Obsolete cache region type
 */
enum isi_cbm_cache_region_type {
	RT_INVALID	= -1, /* invalid region type*/
	RT_NOT_CACHED	= 1, /* not cached */
	RT_CACHED	= 2, /* cached */
	RT_MODIFIED	= 4 /* the region is modified */
};

/**
 * The public cache record structure
 */
struct isi_cbm_cache_record_info {
	/**
	 * The region type
	 */
	enum isi_cbm_cache_status region_type;
	/**
	 * The offset of the region in bytes, it starts with 0
	 */
	off_t offset;
	/**
	 * The length of the region in bytes
	 */
	off_t length;
};

/**
 * Defining desired caching option when issue IOs to CBM file
 */
enum isi_cbm_io_cache_option {
	/**
	 * Default cache option controlled by the policy
	 */
	CO_DEFAULT = 0,
	/**
	 * explicitly request cache after read, override the option in the
	 * policy
	 */
	CO_CACHE = 1,
	/**
	 * explicitly disable cache after read, override the option in
	 * the policy
	 */
	CO_NOCACHE = 2
};

/**
 * Initialize the CBM functionality
 *
 * This routine will perform initializations required to use CBM.
 * It needs to be called by any external (non-CBM) tools, daemon, process
 * (including descendants thereof) before any other CBM functions are called.
 *
 * @param error_out[OUT]: error if any
 */
void isi_cbm_init(struct isi_error **error_out);

/**
 * End using CBM functionality
 *
 * This routine is complementary to isi_cbm_init().
 * It is optional, and should only be used by callers
 * that intend to scope their usage of CBM (scope begins with init and ends
 * with destroy).
 *
 * @param error_out[OUT]: error if any
 */
void isi_cbm_destroy(struct isi_error **error_out);

/**
 * Reload cloud pool config
 *
 * This routine needs to be called if application wants external (to-the-app)
 * change of accounts or pools or policies to take effect.
 *
 * @param error_out[OUT]: error if any
 */
void isi_cbm_reload_config(struct isi_error **error_out);

/**
 * cleanup any resource -- used during shutdown or testing
 */
void isi_cbm_unload_conifg(void);

/**
 * Given the lin and snap_id, open an existing stub file. The caller must have
 * the privilege to open the file by the LIN number.
 * @param lin[IN]: The LIN number of the file
 * @param snap_id[IN]: the snapshot id of the file
 * @param s_flags[IN]: the sync flags for opening file
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the opaque isi_cbm_file, this object must be closed by calling
 *         isi_cbm_close.
 */
struct isi_cbm_file *isi_cbm_file_open(uint64_t lin, uint64_t snap_id,
    int s_flags, struct isi_error **error_out);

/**
 * Given the lin and snap_id, truncate an existing stub file.
 * The caller needs to have the write privilege to the file.
 *
 * @param lin[IN]: The LIN number of the file
 * @param snap_id[IN]: the snapshot id of the file (Head version only)
 * @param offset[IN]: the offset to truncate the file to
 * @param error_out[OUT]: error if any
 */
void isi_cbm_file_truncate(uint64_t lin, uint64_t snap_id, off_t offset,
    struct isi_error **error_out);

/**
 * A variant of file truncate
 *
 * @param file_obj[IN]: pointer to cbm file object
 * @param size[IN]: the offset to truncate the file to
 * @param error_out[OUT]: error if any
 */
void isi_cbm_file_ftruncate(struct isi_cbm_file *file_obj, off_t offset,
    struct isi_error **error_out);

/**
 * update file mtime (EA)
 *
 * @param file_obj[IN]: pointer to cbm file object
 * @param time_spec[IN]: the modification time set to
 * @param error_out[OUT]: error if any
 */
void isi_cbm_file_update_mtime(struct isi_cbm_file *file_obj,
    struct timespec *time_spec, struct isi_error **error_out);

/**
 * Close the given CBM file.
 * @param file[IN]: The CBM file
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_file_close(struct isi_cbm_file *file,
    struct isi_error **error_out);

/**
 * Read data from the CBM file.
 * @param file[IN]: the CBM file
 * @param buf[IN/OUT]: the buffer to receive the content of the file
 *                     Its size must be  >= length
 * @param offset[IN]: the offset of the file
 * @param length[IN]: the length of the read
 * @param cache_opt[IN]: the cache option see the enum documentation.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the number of bytes actually being read. 0 indicates end of file.
 */
ssize_t isi_cbm_file_read(struct isi_cbm_file *file, void *buf,
    uint64_t offset, size_t length,
    enum isi_cbm_io_cache_option cache_opt,
    struct isi_error **error_out);

/**
 * Write data to the CBM file (non for restore).
 * @param file[IN]: the CBM file to write
 * @param buf[IN]: the buffer containing the data for write
 * @param offset[IN]: the offset in the file. This is the logical offset of the
 *                    file.
 * @param length[IN]: the length of the buffer for write
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the  number of bytes actually being written. -1 indicates failure.
 */
ssize_t isi_cbm_file_write(struct isi_cbm_file *file, const void *buf,
    uint64_t offset, size_t length, struct isi_error **error_out);

/**
 * Flush file data in cache to durable storage.
 * @param file_obj[IN]: the CBM file to flush
 * @param offset[IN]: the offset in the file.
 * @param length[IN]: the length of the buffer to flush
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the  number of bytes actually being written. -1 indicates failure.
 */
int isi_cbm_file_flush(struct isi_cbm_file *file_obj, off_t offset,
    size_t length, struct isi_error **error_out);

/**
 * Create a stub file for backup/restore operation.
 * @param path[IN]: the file path
 * @param flags[IN]: the open flags, see man page on open
 * @param mode[IN]: the mode flags, see man page on open
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the opaque CBM file handle. Close it using isi_cbm_file_close.
 */
struct isi_cbm_file *isi_cbm_file_create(
    const char *path,
    int flags,
    int mode,
    struct isi_error **error_out);

/**
 * Write data to the CBM file during restore.
 * @param file[IN]: the CBM file to write
 * @param buf[IN]: the buffer containing the data
 * @param offset[IN]: the offset in the file. This is the logical offset of the
 *                    file.
 * @return the  number of bytes actually being written.
 */
ssize_t isi_cbm_file_restore_write(struct isi_cbm_file *file, const void *buf,
    off_t offset, size_t length,
    struct isi_error **error_out);

/**
 * Mark the file has been backed up.
 * Backup application shall call this after a successful backup of a stub.
 * @param file[IN]: the stub file being backed up
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_file_mark_backed_up(struct isi_cbm_file *file,
    enum isi_cbm_backup_type backup_type,
    struct isi_error **error_out);

/**
 * Validate if the stub file during restore. This should be called
 * after mapping information, encryption key, account information have been
 * saved. It should be called before the close call.
 * Backup application shall call this after a successful backup of a stub.
 * @param file[IN]: the stub file being backed up
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return true if the file is a valid stub, false otherwise. If there is error
 *  it will return false.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
bool isi_cbm_file_validate(struct isi_cbm_file *file,
    struct isi_error **error_out);

/**
 * Get the file descriptor of the stub file. This can be used after
 * isi_cbm_file_create so that the caller can do additional POSIX IOs on the
 * file in addition to the operations supported here without another open.
 * @param file[IN]: the stub file being backed up
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the fd of the stub file. -1 on error. The caller must not close the
 *  fd as it is used by the input file.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
int isi_cbm_file_get_fd(struct isi_cbm_file *file,
    struct isi_error **error_out);

/**
 * Get the stats information from the system perspective. CloudPools keeps two
 * sets for the size, and timestamps. The information from fstat is from
 * the user's perspective. While this API returns the information from
 * the system's perspective.
 * @param file: the CBM file
 * @param sb: the stat buffer. see man page on fstat
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_file_get_stat(struct isi_cbm_file *file,
    struct stat *sb,
    struct isi_error **error_out);

struct fmt_conv_ctx isi_cbm_file_fmt(const struct isi_cbm_file *) __nonnull();

struct isi_cfm_policy_provider;

typedef bool (*get_ckpt_data_func_t)(const void *, void **, size_t *,
	djob_op_job_task_state *, off_t);
typedef bool (*set_ckpt_data_func_t)(void *, void *, size_t,
	djob_op_job_task_state *, off_t);

/**
 * Archive the file to a storage defined by the policy.
 * @param lin[IN] the LIN id of the file to stub
 * @param pol_name[IN] the name of the cloud pool policy
 * @param ppi[IN]  the policy provider context
 * @param ckpt_ctx[IN] the checkpointing context
 * @param ckpt_get[IN] interface to get the checkpointing data
 * @param ckpt_set[IN] interface to set the checkpointing data
 * @param bool[IN] do_async_invalidate[IN] do asynchronous invalidation in
 * case of inline invalidation error.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_archive_by_name(ifs_lin_t lin, const char *pol_name,
    struct isi_cfm_policy_provider *ppi,
    void *ckpt_ctx, get_ckpt_data_func_t ckpt_get,
    set_ckpt_data_func_t ckpt_set,
    bool do_async_invalidate,
    struct isi_error **error_out);

/**
 * Archive the file to a storage defined by the policy.
 * @param lin[IN] the LIN id of the file to stub
 * @param pol_id[IN] the id of the cloud pool policy
 * @param ppi[IN]  the policy provider context
 * @param ckpt_ctx[IN] the checkpointing context
 * @param ckpt_get[IN] interface to get the checkpointing data
 * @param ckpt_set[IN] interface to set the checkpointing data
 * @param bool[IN] do_async_invalidate[IN] do asynchronous invalidation in
 * case of inline invalidation error.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_archive_by_id(ifs_lin_t lin, uint32_t pol_id,
    struct isi_cfm_policy_provider *ppi,
    void *ckpt_ctx, get_ckpt_data_func_t ckpt_get,
    set_ckpt_data_func_t ckpt_set,
    bool do_async_invalidate,
    struct isi_error **error_out);

/**
 * Recall the file from the remote storage to the local storage. The
 * specified file must be a stub.
 * @param lin[IN] the LIN id of the file to stub
 * @param snapid[IN] the local snapshot id. Only HEAD_SNAPID is supported.
 * @param retain_stub[IN] indicate if to retain the stub. When true, the
 *        remains as a stub file with data fully cached.
 * @param pol_name[IN] the name of the cloud pool policy
 * @param ppi[IN]  the policy provider context
 * @param ckpt_ctx[IN] the checkpointing context
 * @param ckpt_get[IN] interface to get the checkpointing data
 * @param ckpt_set[IN] interface to set the checkpointing data
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_recall(ifs_lin_t lin , ifs_snapid_t snapid,
    bool retain_stub,
    struct isi_cfm_policy_provider *ppi,
    void *ckpt_ctx, get_ckpt_data_func_t ckpt_get,
    set_ckpt_data_func_t ckpt_set, struct isi_error **error_out);

/**
 * Utility function to dump the CMO information to the stderr
 * @param lin[IN] the LIN id of the file to stub
 * @param snapid[IN] the local snapshot id. Only HEAD_SNAPID is supported.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_dump_cmo(ifs_lin_t lin, ifs_snapid_t snap_id,
    struct isi_error **error_out);

/**
 * Interface for synchronizing modified data to the cloud storage.
 * @param lin[IN]: the LIN id of the stub
 * @param snapid[IN]: the snapid of the stub
 * @param get_ckpt_cb[IN]: the callback function for get the checkpoint data
 * @param set_ckpt_cb[IN]: the callback function for set the checkpoint data
 * @param ctx[IN]: the context pointer to pass to the callbacks
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_sync(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, struct isi_error **error_out);

/**
 * Given the fd, open an existing stub file. The caller must have
 * the privilege to open the file by the LIN number.
 * @param fd[IN]: The file descriptor. The fd must have been opened.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the opaque isi_cbm_file, this object must be closed by calling
 *         isi_cbm_close.
 */
struct isi_cbm_file *isi_cbm_file_get(int fd,
    struct isi_error **error_out);

/**
 * purge the stub file
 * @param fd[IN]: file to be purged
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_purge(int fd, struct isi_error **error_out);

/**
 * get the cluster id that stubbed the file
 * @param file[IN} file
 * @return cluster id
 */
const char * isi_cbm_stub_cluster_id(struct isi_cbm_file *file);

/**
 * Get the version of a cbm file
 * @param file [in]  the cbm file
 * @param ver [out]  the file version
 * @param error_out[out] error if any
 */
void isi_cbm_file_version_get(struct isi_cbm_file *file,
    struct isi_cbm_file_version *ver,
    struct isi_error **error_out);

__END_DECLS

#endif //__ISI_CLOUD_CBM__H__

