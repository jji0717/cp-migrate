#ifndef __ISI_CPOOL_D_C_API_H__
#define __ISI_CPOOL_D_C_API_H__

#include <isi_job/jobstatus_gcfg.h>
#include <isi_cpool_d_common/cpool_d_common.h>

#ifdef __cplusplus
extern "C" {
#endif

// forward declarations
struct isi_error;
struct cpool_account;

/**
 * Priority definitions used as parameters to API functions.
 */
enum cpool_api_priority_value {
	ISI_CPOOL_D_PRI_LOW =	0,
	ISI_CPOOL_D_PRI_HIGH =	1,
};

typedef enum cpool_api_priority_value cpool_api_priority;

/**
 * Messaging definitions used as parameters to API functions.
 */
enum cpool_message_recipient {
	CPM_RECIPIENT_NONE = 0,
	CPM_RECIPIENT_RESTORE_COI = 1 << 0
};

/**
 * Archive a file.
 * @param[in/out] djob_id	the id of the daemon_job which will own this
 *  archive.  If djob_id is DJOB_RID_INVALID, a new job will be created and
 *  djob_id will be populated.
 * @param[in]  fd		a file descriptor to the file to be archived
 * @param[in(optional)] filename	the filename to be displayed to users
 *  during query operations.  May be NULL if not needed.
 * @param[in]  policy_id	the id of the policy that defines how the
 *  file should be archived
 * @param[in]  priority		the priority at which to archive the file; see
 *  note above
 * @param[in]  ignore_already_archived	if true, no error is returned if file
 *  to be archived has already been archived
 * @param[in] ignore_invalid_file if true, no error is returned if file to be
 *  archived is an invalid file type. Only regular file is supported.
 * @param[out] error_out	holds information on any encountered errors
 */
void archive_file(djob_id_t *djob_id, int fd, const char *filename,
    uint32_t policy_id, cpool_api_priority priority,
    bool ignore_already_archived, bool ignore_invalid_file,
    struct isi_error **error_out);

/**
 * Recall a file, which reads all of the file's data from the cloud and
 * reconsitutes the file in the local filesystem.
 * @param[in/out] djob_id	the id of the daemon_job which will own this
 *  recall.  If djob_id is DJOB_RID_INVALID, a new job will be created and
 *  djob_id will be populated.
 * @param[in]  lin		the lin of the stub file to be recalled
 * @param[in]  snapid		the snapid of the stub file to be recalled
 * @param[in(optional)] filename	the filename to be displayed to users
 *  during query operations.  May be NULL if not needed.
 * @param[in]  priority		the priority at which to recall the file; see
 * note above
 * @param [in] retain_stub	whether the recalled file should remain a cloud
 *   stub after recalling
 * @param[in]  ignore_not_archived	if true, no error is returned if file
 * to be recalled has not been archived
 * @param[out] error_out	holds information on any encountered errors
 */
void recall_file(djob_id_t *djob_id, ifs_lin_t lin, ifs_snapid_t snap_id,
    const char *filename, cpool_api_priority priority, bool retain_stub,
    bool ignore_not_archived, struct isi_error **error_out);

/**
 * Returns the ID of a running daemon_job started from within a smartpools
 * job engine job.  If no such daemon job exists, it is created here and its
 * id is returned.
 */
djob_id_t get_daemon_job_id_for_smartpools(jd_jid_t je_id,
    struct isi_error **error_out);

/**
 * Invalidate the cache of a stubbed file.
 * @param[in]  lin		the lin of the file whose cache is to be
 * invalidated
 * @param[in]  snapid		the snapid of the file whose cache is to be
 * invalidated
 * @param[out] error_out	holds information on any error encountered
 * during the operation
 */
void invalidate_stubbed_file_cache(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out);

/**
 * A callback to notify isi_cpool_d that a job engine job has completed.
 *
 * @param[in] je_id		the id of the job engine job
 * @param[in] state		the final state of the job engine job
 * @param[out] error_out	holds information on any error encountered
 * during the operation
 */
void on_job_engine_job_complete(jd_jid_t je_id, enum jobstat_state state,
    struct isi_error **error_out);

/**
 * For test purposes only.  Preallocates memory in cpool_d to avoid false
 * positive leaks
 */
void preallocate_cpool_d_for_test(struct isi_error **error_out);

/**
 * Restore the COI for a given account.
 * @param[in/out] djob_id	the id of the daemon_job which will own this
 * restore.  If djob_id is DJOB_RID_INVALID, a new job will be created and
 * djob_id will be populated.
 * @param[in] cluster		the the cluster of the account
 * @param[in]  account_id	the account id
 * @param[in]  account_name	the name of the account (can be NULL)
 * @param[in]  dod		the date of expiration
 * @param[out] error_out	holds information on any encountered errors
 */
void restore_coi_for_account(djob_id_t *djob_id, const char *cluster,
    uint32_t account_id, const char *account_name, struct timeval dod,
    struct isi_error **error_out);

/**
 * 'Tickles' a job and forces it to check whether any remaining work needs to
 * be done.  If not, the job is marked complete. In most scenarios, either the
 * completion of job engine or the completion of the last task will make this
 * happen implicitly.  This only needs to be run for jobs with no tasks and no
 * associated job engine job.  (Today, this only happens with restore-coi)
 *
 * @param[in] djob_id		the id of the daemon job to be tickled
 * @param[out] error_out	holds information on any encountered errors
 */
void check_for_job_completion(djob_id_t djob_id, struct isi_error **error_out);

/**
 * Looks at all GUIDs in the permit list and (if they are are in a
 * 'being_removed' state and are done processing) disables them in the permit
 * list
 *
 * @param[in] num_tries		the number of times to try this transaction to
 * resolve conflicts (i.e., transaction is retried in the event of a gconfig
 * conflict, but not for other errors)
 * @param[out] error_out	holds information on any encountered errors
 */
void finish_disabling_eligible_guids(int num_tries,
    struct isi_error **error_out);

/**
 * Looks at all GUIDs in the permit list and (if they are are in a
 * 'being_added' state and are done processing) enables them in the permit list
 *
 * @param[in] num_tries		the number of times to try this transaction to
 * resolve conflicts (i.e., transaction is retried in the event of a gconfig
 * conflict, but not for other errors)
 * @param[out] error_out	holds information on any encountered errors
 */
void finish_adding_eligible_pending_guids(int num_tries,
    struct isi_error **error_out);

/**
 * Returns the number of tasks remaining to be completed before guids can be
 * disabled
 */
unsigned int get_num_tasks_remaining_to_disable_guids(
    struct isi_error **error_out);

/**
 * Moves the given guid to a disabled state if it previously existed in a
 * 'being_added' state
 *
 * @param[in] guid		the cluster guid to be disabled
 * @param[in] num_tries		the number of times to try this transaction to
 * resolve conflicts (i.e., transaction is retried in the event of a gconfig
 * conflict, but not for other errors)
 * @param[out] error_out	holds information on any encountered errors
 */
void cancel_guid_addition(const char *guid, int num_tries,
    struct isi_error **error_out);

/**
 * Returns whether a given account is part of any restore_coi job
 *
 * @param[in] account		the account to check (must not be NULL)
 * @param[out] error_out	holds information on any encountered errors
 */
bool is_account_managed_by_restore_coi_job(const struct cpool_account *account,
    struct isi_error **error_out);

/**
 * Send a notification to given thread running in isi_cpool_d
 *
 * @param[in] to		indicates which thread pool should receive the
 * message
 * @param[out] error_out	holds information on any encountered errors
 */
void notify_daemon(enum cpool_message_recipient to,
    struct isi_error **error_out);

#ifdef __cplusplus
}
#endif

#endif // __ISI_CPOOL_D_C_API_H__
