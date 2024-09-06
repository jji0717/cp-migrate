#ifndef __ISI_CPOOL_D_COMMON_H__
#define __ISI_CPOOL_D_COMMON_H__

#include <ifs/ifs_types.h>

#include <stdint.h>
#include <stdlib.h>

#include <isi_job/job.h>

#include "daemon_job_types.h"

#define CPOOL_DAEMON_JOB_FILE_LIST "cpool_djob_file_list"
#define CPOOL_DAEMON_JOBS_FILE "cpool_daemon_jobs_file"
#define CPOOL_DJOB_RECORD_LOCK_FILE "cpool_djob_record_lock_file"
#define CPOOL_LOCKS_DIRECTORY "/ifs/.ifsvar/modules/cloud/"
#define CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE "/ifs/.ifsvar/modules/cloud/isi_cpool_d.archive.v2"
#define CPOOL_INVALID_POLICY_ID 0

struct isi_error;

// UTF8 leading characters start either with a 0-bit or two 1-bits
#define IS_START_OF_UTF8_CHAR(chr) ( (((chr) & 0x80) != 0x80) || (((chr) & 0xC0)) == 0xC0 )

struct get_sbt_entry_ctx {
	uint32_t	magic_;
	btree_key_t	*first_key_;
	bool		looped_;
};

/**
 * Daemon job reserved IDs
 */
#define DJOB_RID_INVALID		((uint32_t)-1)
#define DJOB_RID_CONFIG			0
#define DJOB_RID_WRITEBACK		1
#define DJOB_RID_CACHE_INVALIDATION	2
#define DJOB_RID_LOCAL_GC		3
#define DJOB_RID_CLOUD_GC		4
#define DJOB_RID_RESTORE_COI		5

#define DJOB_RID_LAST_RESERVED_ID	DJOB_RID_RESTORE_COI

/*
typedef enum {
	DJOB_RID_INVALID		= UINT32_MAX,
	DJOB_RID_CONFIG			= 0,
	DJOB_RID_WRITEBACK		= 1,
	DJOB_RID_CACHE_INVALIDATION	= 2,
	DJOB_RID_LOCAL_GC		= 3,
	DJOB_RID_CLOUD_GC		= 4,
	DJOB_RID_LAST_RESERVED_ID	= DJOB_RID_CLOUD_GC
} reserved_daemon_job_id;
*/

/*
 * This formatter was originally created for PL but is needed for backport of a
 * bug, so it's defined here rather than the kernel (where a formatter this
 * generic would otherwise normally go) so the RUP doesn't require a node
 * reboot.
 */
struct fmt_conv_ctx
array_byte_fmt(const uint8_t *value, size_t size);

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Destroy a get_sbt_entry_ctx object.
 * @param[in]  ctx	      the context to destroy
 */
void get_sbt_entry_ctx_destroy(struct get_sbt_entry_ctx **ctx);

/**
 * Retrieve the entry with the smallest key greater than or equal to start_key,
 * which will be updated to the key of the next entry in the SBT.
 * @param[in]  sbt_fd			the sbt from which to retrieve an entry
 * @param[in]  start_key		the starting point for retrieving an
 * entry
 * @param[in]  entry_buf		must point to NULL, will hold the entry
 * if one is found, else remains unchanged
 * @param[in]  ctx			an opaque context used to avoid
 * infinite loops; should initially point to NULL, passed in unchanged to
 * subsequent calls, and destroyed using get_entry_ctx_destroy() when no longer
 * needed
 * @param[out] error_out		any error encountered during the
 * operation
 */
struct sbt_entry *get_sbt_entry(int sbt_fd, btree_key_t *start_key,
    char **entry_buf, struct get_sbt_entry_ctx **ctx,
    struct isi_error **error_out);

/**
 * Randomly retrieve one of the first randomization_factor entries with keys
 * greater than or equal to start_key, which will be updated to the key of the
 * next entry in the SBT.
 * @param[in]  sbt_fd			the sbt from which to retrieve an entry
 * @param[in]  start_key		the starting point for retrieving an
 * entry
 * @param[in]  entry_buf		must point to NULL, will hold the entry
 * if one is found, else remains unchanged
 * @param[in]  randomization_factor	the number of keys from which to draw a
 * random entry.  (E.g., randomization_factor == 8 means "return one of the
 * first 8 entries with a key greater than start_key"
 * @param[in]  ctx			an opaque context used to avoid
 * infinite loops; should initially point to NULL, passed in unchanged to
 * subsequent calls, and destroyed using get_entry_ctx_destroy() when no longer
 * needed
 * @param[out] error_out		any error encountered during the
 * operation
 */
struct sbt_entry *
get_sbt_entry_randomize(int sbt_fd, btree_key_t *start_key, char **entry_buf,
    unsigned int randomization_factor, struct get_sbt_entry_ctx **ctx,
    struct isi_error **error_out);

/**
 * Get this node's id.
 *
 * @return A pointer to the start of the id that is of size
 * IFSCONFIG_GUID_SIZE.
 */
const uint8_t *get_handling_node_id(void);

/**
 * Get this node's id and store it in the passed-in buffer.  Buffer must be
 * pre-allocated and of size greater than or equal to IFSCONFIG_GUID_SIZE.
 */
void get_node_id_copy(uint8_t *node_id_copy);

/**
 * Get this node's devid.
 * @param[out] error_out	any error encountered during the operation
 *
 * @return this node's devid
 */
int get_devid(struct isi_error **error_out);

/**
 * Get this node's lnn.
 * @param[out] error_out	any error encountered during the operation
 *
 * @return this node's lnn
 */
int get_lnn(struct isi_error **error_out);

/**
 * Retrieve the name of the pending SBT used to store Task objects of the given
 * type and priority.
 * @param[in] type		the type of Task
 * @param[in] priority		the priority
 *
 * @return the name of the pending SBT, for which the caller is responsible for
 * deallocating by using free().
 */
char *get_pending_sbt_name(task_type type, int priority);

/**
 * Retrieve the name of the in-progress SBT used to store Task objects of the
 * given type.
 * @param[in] type		the type of Task
 *
 * @return the name of the in-progress SBT, for which the caller is responsible
 * for deallocating by using free().
 */
char *get_inprogress_sbt_name(task_type type);

/**
 * Retrieve the name of the file used when locking a Task object.
 * @param[in] type		the type of Task
 *
 * @return the name of the lock file, for which the caller is responsible for
 * deallocating by using free().
 */
char *get_lock_file_name(task_type type);

/**
 * Retrieve the name of the move lock file used for a Task object.
 * @param[in] type		the type of Task
 *
 * @return the name of the lock file, for which the caller is responsible for
 * deallocating by using free().
 */
char *get_move_lock_file_name(task_type type);

/**
 * Retrieve the name of the COI SBT.
 *
 * @return the name of the COI SBT; unlike get_pending_sbt_name or
 * get_inprogress_sbt_name, the caller should NOT deallocate returned pointer.
 */
const char *get_coi_sbt_name(void);

/**
 * This model the COI's parent, multiple versions of the CMO stored in
 * the COI can references to the CMO
 */
const char *get_cmoi_sbt_name(void);

/**
 * Retrieve the name of the OOI SBT.
 *
 * @return the name of the OOI SBT; unlike get_pending_sbt_name or
 * get_inprogress_sbt_name, the caller should NOT deallocate returned pointer.
 */
const char *get_ooi_sbt_name(void);

/**
 * Retrieve the name of the file used by the OOI to lock entry groups.	The
 * caller should NOT deallocate returned pointer.
 *
 * @return the OOI lock filename
 */
const char *get_ooi_lock_filename(void);

/**
 * Retrieve the name of the CSI SBT.
 *
 * @return the name of the CSI SBT; unlike get_pending_sbt_name or
 * get_inprogress_sbt_name, the caller should NOT deallocate returned pointer.
 */
const char *get_csi_sbt_name(void);

/**
 * Retrieve the name of the file used by the CSI to lock entry groups.  The
 * caller should NOT deallocate returned pointer.
 *
 * @return the CSI lock filename
 */
const char *get_csi_lock_filename(void);

/**
 * Retrieve the name of the WBI SBT.
 *
 * @return the name of the WBI SBT; unlike get_pending_sbt_name or
 * get_inprogress_sbt_name, the caller should NOT deallocate returned pointer.
 */
const char *get_wbi_sbt_name(void);

/**
 * Retrieve the name of the file used by the WBI to lock entry groups.  The
 * caller should NOT deallocate returned pointer.
 *
 * @return the WBI lock filename
 */
const char *get_wbi_lock_filename(void);

/**
 * Delete the SBT of the given name. Used for testing only.
 * @param[in] sbt_name		the name of the sbt, as retrieved from
 * get_pending_sbt_name() or get_inprogress_sbt_name()
 * @param[out] error_out	the error, if any
 */
void delete_sbt(const char *sbt_name, struct isi_error **error_out);

/**
 * Possibly create and open an SBT.  Each SBT will be stored in a directory
 * within the sbt root directory.
 * @param[in] name		the name of the SBT to open
 * @param[in] create		if the SBT does not exist, a true value will
 * cause it to be created, otherwise ENOENT will be returned.
 * @param[out] error_out	stores information about any encountered error
 *
 * @return a file descriptor to the newly-opened SBT.
 */
int open_sbt(const char *name, bool create, struct isi_error **error_out);

/**
 * Possibly create and open a job files SBT.
 * @param[in] job_id		ID of the job these files are associated with
 * @param[in] create		if the SBT does not exist, a true value will
 *				cause it to be created, otherwise ENOENT will
 *				be returned.
 * @param[out] error_out	stores information about any encountered error
 *
 * @return a file descriptor to the SBT.
 */
int open_djob_file_list_sbt(djob_id_t job_id, bool create,
    struct isi_error **error_out);

/**
 * Close an open SBT.  Errors are logged but otherwise ignored.
 * @param[in] sbt_fd		the file descriptor for the SBT
 */
void close_sbt(int sbt_fd);

/**
 * Retrieve the name of the daemon jobs SBT used to store all daemon job
 * record.
 *
 * @return the name of the daemon job records SBT, for which the caller is
 * responsible for deallocating by using free().
 */
char *get_daemon_jobs_sbt_name(void);

/**
 * Retrieve the name of the daemon job records lock file.
 *
 * @return the name of the daemon job records lock file, for which the caller
 * is responsible for deallocating by using free().
 */
char *get_djob_record_lock_file_name(void);

/**
 * Retrieve the files list SBT of a daemon job record.
 * param[in] daemon_job_id		the daemon job id
 *
 * @return the name of the daemon job files list SBT, for which the caller is
 * responsible for deallocating by using free().
 */

char *get_djob_file_list_sbt_name(djob_id_t daemon_job_id);

/**
 * Copies path_in to path_out.	If path_in is longer than out_size, the path
 * is abbreviated by replacing characters with ellipsis (...).	For example,
 * /ifs/data/path/path/path/path/file.txt might become
 * /ifs/data/.../path/file.txt
 *
 * @param[in] path_in	the original (full-length) path
 * @param[out] path_out	the buffer to receive the abbreviated path - must be
 *   preallocated to at least out_size bytes
 * @param[in] the maximum allowable length of path_out
 */
void abbreviate_path(const char *path_in, char *path_out, size_t out_size);

/**
 * Check if the Node has the network connectivity set up.
 *
 * @param[out] network_up	true if network is up, else false; value
 * remains unchanged on error
 * @param[out] error_out	any error encountered during the operation
 */
void check_network_connectivity(bool *network_up,
    struct isi_error **error_out);

/**
 * Get an exclusive cpool domain lock for a given key.
 * @param[in] lock_fd	the fd of lock file.
 * @param[in] key	the value that gets locked.
 * @return 0 if success.
 */
int cpool_dom_lock_ex(int lock_fd, off_t key);

/**
 * Release the cpool domain lock for a given key.
 * @param[in] lock_fd	the fd of lock file.
 * @param[in] key	the value that gets locked.
 * @return 0 if success.
 */
int cpool_dom_unlock(int lock_fd, off_t key);

/**
 * Get policy name by policy id
 *
 * @param[in] policy_id	 the id of the policy
 * @param[out] error_out any error encountered during the operation
 * @return policy name if success
 */
const char*
lookup_policy_name(uint32_t policy_id, struct isi_error **error_out);

/**
 * API to fill in the flock parameters
 * @param [in] key - Input the key that will be used to take the lock
 * @param [in/out] flock_ - Of type struct flock. Passed in empty and populated
 */
void
populate_flock_params(off_t key, struct flock *flock_);

/**
 * determine which archive task version should be used
 *
 * @return the archive task version that should be used
 */
int
determine_archive_task_version(void);

#ifdef __cplusplus
} // end extern "C"
#endif

#endif // __ISI_CPOOL_D_COMMON_H__
