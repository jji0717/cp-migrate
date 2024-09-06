#ifndef __SIQ_JOBCTL_H__
#define __SIQ_JOBCTL_H__

#include "isi_migrate/config/siq_job.h"

extern struct job_lock job_lock;

#ifdef __cplusplus
extern "C" {
#endif

/**
 ** TSM Job Control API
 */

struct siq_running_job {
	char *policy_id;
	struct siq_job_summary *job_summary;
	struct siq_running_job *next;
};

struct siq_running_jobs {
	struct siq_running_job *job;
};

/**
 * @returns the job_id. 
 * Note: use it after siq_job_grab only.
 */
char *
siq_get_job_id(void);

/**
 * @returns the name of a new snapshot for a given policy @pol_id. 
 */
char *
siq_get_new_snap_name(char *pol_id);

/**
 * @returns the name of an old snapshot for a given policy @pol_id.
 */
char *
siq_get_old_snap_name(char *pol_id);

/**
 * @returns the name of the temporary snapshot for a given policy @pol_id.
 */
char *
siq_get_tmp_snap_name(char *pol_id);

/**
 * Removes a snapshot. If remove_locks is true, all locks are associated with it 
 * will be removed also.
 * @returns -1 on error (use errno for more information about an error),
 * 0 - otherwise. 
 */
void
siq_remove_snapshot(char *name, int remove_locks,
    struct isi_error **error_out);

/**
 * Sets new expiration for a given snapshot.
 * @returns -1 on error (use errno for more information about an error),
 * 0 - otherwise. 
 */
void
siq_set_snapshot_expiration(char *name, time_t expiration,
    struct isi_error **error_out);

/**
 * Returns job @pid status.
 * If the job specified is not found, SIQ_JS_NONE is returned.
 */
enum siq_job_state siq_job_state(char *pid);


/* Sets the state of an UNGRABBED job in the run directory. If the
 * ungrabbed job is not found, or there is an error modifying its report,
 * an error is returned. */
void
siq_ungrabbed_job_set_state(char *pid, enum siq_job_state state,
    struct isi_error **error_out);

/**
 * Loads running job report, for progress tracking purposes.
 * @param pid		valid policy id
 * @param report	job summary 
 * 
 * @returns
 * 0        on success
 * EFAULT   on invalid inputs
 * ENOENT   if unable to find a running job with id @pid.
 */
int
siq_job_report_load(char *pid, struct siq_job_summary **report);

/**
 * Loads list of all running job reports, for progress tracking purposes.
 * @param jobs		List of running jobs. Each item in the list contains
 *			the policy_id and corresponding report.
 */
void
siq_job_report_load_running(struct siq_running_jobs **jobs,
    struct isi_error **error_out);

/**
 * Frees siq_running_jobs structure created by siq_job_report_load_running()
 */
void
siq_running_jobs_free(struct siq_running_jobs *jobs);

struct siq_gc_spec *
siq_job_spec_load(const char *policy_id, struct isi_error **error_out);

/**
 * Cancels a job for a policy @pid. 
 * @Return 0 on success or an attempt to cancel already canceled job,
 * or EINVAL if the specified job is not running.  
 * NOTE An attempt to cancel already canceled job is considered
 * acceptable and returns success.
 */
int
siq_job_cancel(char *pid);

/**
 * Pauses a job for a policy @pid. 
 * @Returns 0 on success, or EINVAL if the specified job is not
 * running.  
 * NOTE An attempt to pause already paused job is considered
 * acceptable and returns success.
 */
int
siq_job_pause(char *pid);

/**
 * Resumes a job for a policy @pid. 
 * @Return 0 on success, or EINVAL if the specified job has not been
 * paused.
 * NOTE An attempt to resume running job is considered acceptable and
 * returns success.
 */
int
siq_job_resume(char *pid);

/**
 * Returns next job run time.
 * @param task		- 
 * @param prev_start	-
 * @param prev_end	-
 * @param since		-
 * @param next_start	-
 * @Return 0 on success, or 1 if there are no runs, -1 on error.
 */ 
int 
siq_job_get_next_run(const struct siq_policy *policy, time_t prev_start,
    time_t prev_end, time_t since, time_t *next_start);
/**
 * Coordinator call this function to acquire job.
 * @param orig_job_dir  - original job run dir (w/o pid)
 * @param policy_id_p	- set to policy id (hash number)
 * @param new_job_dir_p	- set to run job directory
 */
void
siq_job_grab(const char *orig_job_dir,
    char **policy_id_p, char **new_job_dir_p);

/**
 * Coordinator calls this function to create job based on policy name.
 *
 * @param name		- policy name
 * @param policy_id_p	- set to policy id (hash number)
 * @param job_dir_p	- set to run job directory
 * @Returns 
 * 0 			on success 
 * non-zero error  	on failure (job canceled, already owned by
 * 			another coordinator etc).
 */
int
siq_job_create(char *name, char **policy_id_p, char **job_dir_p);

int
siq_job_ungrab(const char *job_dir, struct siq_policy* policy, char *err_msg);

int
siq_job_kill(char *this_job_id, struct siq_policy* policy, int node_num,
    char *err_msg);

/**
 * Enables coordinator and worker processes to query job state.
 * @param dir	- job working directory, as returned by job_grab call
 *
 * @Returns
 * Job state as defined by siq_job_state enum. If @dir is
 * inaccessible, SIQ_JS_NONE is returned.
 */
enum siq_job_state
siq_job_current_state(const char *dir);

/**
 * Populates last job @start_time / @end_time by a given @policy_id. 
 * @Returns: false - if there are no corresponding internal scheduler
 * directory, true - otherwise.  
 **/
void
siq_job_time(struct siq_source_record *source_rec, 
    time_t *start_time, time_t *end_time);

/**
 * Moves the job dir into the trash directory (atomic) and then
 * attempts to clean the trash directory.
 *
 * Returns 0 on success, non-zero on failure.
 **/
int
trash_job_dir(const char *job_dir, const char *policy_id);

/**
 * Writes job report, saves source list and moves the job dir into the trash 
 * directory (atomic) which the scheduler will empty.
 **/
void
siq_job_cleanup(struct siq_gc_job_summary *gcjs, const char *policy_id,
    const char *job_dir, struct isi_error **error_out);

bool siq_job_lock(const char *policy_id, struct isi_error **error_out);
bool siq_job_try_lock(const char *policy_id, struct isi_error **error_out);
void siq_job_unlock(void);
bool siq_job_locked(void);
void siq_job_rm_lock_file(const char *policy_id);

#ifdef __cplusplus
}
#endif

#endif /* __SIQ_JOBCTL_H__ */
