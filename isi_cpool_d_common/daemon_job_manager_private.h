#ifndef __CPOOL_DAEMON_JOB_MANAGER_PRIVATE_H__
#define __CPOOL_DAEMON_JOB_MANAGER_PRIVATE_H__

#ifndef CPOOL_DJM_PRIVATE
#error "Don't include daemon_job_manager_private.h unless you know what "\
    "you're doing, include daemon_job_manager.h instead."
#endif
#undef CPOOL_DJM_PRIVATE

#include <vector>

#include "daemon_job_types.h"

struct hsbt_bulk_entry;
struct isi_error;

class cpool_operation_type;
class daemon_job_configuration;
class daemon_job_record;
class daemon_job_request;
class scoped_object_list;

namespace isi_cloud {
class task;

namespace daemon_job_manager {

void create_daemon_job_config(struct isi_error **error_out);

/** Assumes configuration is already locked. */
daemon_job_configuration *get_daemon_job_config(struct isi_error **error_out);

/**
 * If successful, returns a daemon job ID of a locked daemon job record and
 * passed-in daemon job configuration pointer will point to an allocated and
 * locked configuration that reflects the new daemon job (with its original
 * serialization saved).  In either case (success or failure), populates
 * objects_to_delete with any taken locks or allocated objects so that they are
 * released when the scoped_object_list falls out of scope.
 */
djob_id_t get_next_available_job_id(int max_jobs,
    daemon_job_configuration *&djob_config,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);

/**
 * Used at startup to create non-multi-jobs and used on-demand when
 * creating multi-jobs.
 */
djob_id_t create_job(const cpool_operation_type *op_type,
    struct isi_error **error_out);
/**
 * Prepare the SBT bulk operation for (potentially) creating the daemon job
 * record and (potentially) updating the daemon job configuration in order to
 * create a new daemon job (either multi- or single-job).
 */
djob_id_t create_job_helper__multi_job(const cpool_operation_type *op_type,
    std::vector<struct sbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);
djob_id_t create_job_helper__single_job(const cpool_operation_type *op_type,
    std::vector<struct sbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);

void add_member(const isi_cloud::task *task, djob_id_t job_id,
    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);
void add_member__multi_job(const isi_cloud::task *task, djob_id_t job_id,
    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);
void add_member__single_job(const isi_cloud::task *task, djob_id_t job_id,
    const char *filename, std::vector<struct hsbt_bulk_entry> &sbt_ops,
    scoped_object_list &objects_to_delete, struct isi_error **error_out);

void pr_operation(const cpool_operation_type *op_type,
    djob_op_job_task_state new_state, struct isi_error **error_out);
void prc_job(djob_id_t daemon_job_id, djob_op_job_task_state new_state,
    struct isi_error **error_out);
#ifdef PRC_TASK
void prc_task(const task_key *key, djob_op_job_task_state new_state,
    struct isi_error **error_out);
#endif

/*
 * When cancelling a JE job that is assocaited with a daemon job, JE may return
 * ENOTCONN if it is not running.  This variable defines the maximum number of
 * JE job cancel attempts due to ENOTCONN.
 */
const int MAX_JE_JOB_CANCEL_ATTEMPTS = 5;

/*
 * When cancelling a JE job that is associated with a daemon job, JE returns
 * EINVAL if the job has already completed.  In this case the daemon job
 * manager re-reads the daemon job record to determine if the JE job has in
 * fact completed or if some other error has occurred.  This variable defines
 * the maximum number of daemon job re-reads after EINVAL.
 */
const int MAX_JE_JOB_REREAD_ATTEMPTS = 5;

jd_jid_t delegate_to_job_engine(const daemon_job_request *request,
    djob_id_t djob_id, struct isi_error **error_out);
jd_jid_t start_job_engine_job(struct user_job_attrs *uattrs,
    enum job_type_id type, struct isi_error **error_out);
uint32_t lookup_policy_id(const char *policy_name,
    struct isi_error **error_out);

djob_op_job_task_state djob_ojt_combine(djob_op_job_task_state lhs,
    djob_op_job_task_state rhs, bool is_combined_jobs_state);

} // end namespace daemon_job_manager
} // end namespace isi_cloud

#endif // __CPOOL_DAEMON_JOB_MANAGER_PRIVATE_H__
