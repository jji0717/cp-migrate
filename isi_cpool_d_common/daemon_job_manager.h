#ifndef __CPOOL_DAEMON_JOB_MANAGER_H__
#define __CPOOL_DAEMON_JOB_MANAGER_H__

#include <list>
#include <set>
#include <vector>

#include <isi_job/jobstatus_gcfg.h>

#include "resume_key.h"
#include "daemon_job_types.h"

class daemon_job_configuration;
class daemon_job_member;
class daemon_job_record;
class daemon_job_request;
class daemon_job_stats;
class cpool_operation_type;
class scoped_object_list;
class task_key;

struct hsbt_bulk_entry;
struct isi_error;

namespace isi_cloud
{

    class task;

    namespace daemon_job_manager
    {

        void at_startup(struct isi_error **error_out);

        void lock_daemon_job_config(bool block, bool exclusive,
                                    struct isi_error **error_out);
        void unlock_daemon_job_config(struct isi_error **error_out);

        void lock_daemon_job_record(djob_id_t id, bool block, bool exclusive,
                                    struct isi_error **error_out);
        void unlock_daemon_job_record(djob_id_t id, struct isi_error **error_out);

        /** Assumes record is already locked. */
        daemon_job_record *get_daemon_job_record(djob_id_t id,
                                                 struct isi_error **error_out);

        /* daemon job member functions */
        int open_member_sbt(djob_id_t id, bool create, struct isi_error **error_out);
        // ^ public or private?

        daemon_job_member *get_daemon_job_member(djob_id_t djob_id, const task *task,
                                                 struct isi_error **error_out);
        daemon_job_member *get_daemon_job_member(djob_id_t djob_id,
                                                 const task_key *key, struct isi_error **error_out);

        /** Assumes daemon job configuration is NOT locked. */
        djob_op_job_task_state get_state(const cpool_operation_type *op_type,
                                         struct isi_error **error_out);
        /** Assumes daemon job record is locked. */
        djob_op_job_task_state get_state(const daemon_job_record *djob_rec,
                                         struct isi_error **error_out);
        /** Assumes daemon job record identified by djob_id is NOT locked. */
        djob_op_job_task_state get_state(djob_id_t djob_id,
                                         struct isi_error **error_out);
        /** Assumes task is locked. */
        djob_op_job_task_state get_state(const isi_cloud::task *task,
                                         struct isi_error **error_out);

        bool is_operation_startable(const cpool_operation_type *op_type,
                                    struct isi_error **error_out);

        /**
         * Start a given job and return the newly assigned job ID.  Does not launch
         * job engine and evaluate files.  Archives and recalls must be explicitly
         * added.
         *
         * @param[in]  op_type		the type of job to be started
         * @param[out] error_out	any error encountered during the operation
         *
         * @return			id of the started job
         */
        djob_id_t start_job_no_job_engine(const cpool_operation_type *op_type,
                                          struct isi_error **error_out);

        /**
         * Check whether a job engine job is finished (not running/waiting)
         *
         * @param[in]  je_job_id	job engine job ID
         * @param[out] error_out	any error encountered during the operation
         *
         * @return			state of the job engine job
         */
        bool
        job_engine_job_is_finished(jd_jid_t je_job_id,
                                   struct isi_error **error_out);

        /**
         * Start a given job and return the newly assigned job ID.
         *
         * @param[in]  request		describes the job to be started
         * @param[out] error_out	any error encountered during the operation
         *
         * @return			id of the started job
         */
        djob_id_t start_job(const daemon_job_request *request,
                            struct isi_error **error_out);

        /**
         * Add to an SBT bulk operation for the purpose of adding a task to an existing
         * daemon job.  Assumes task is locked.
         *
         * @param[in]  task		the task to be added
         * @param[in]  name		the name of the file or work item represented
         * by the task, if applicable; can be NULL
         * @param[in]  job_id		identifies the job to which the task will be
         * added
         * @param[out] sbt_ops		the collection of SBT operations to which this
         * function will add
         * @param[out] objects_to_delete	objects allocated by this function must
         * survive long enough for the caller to complete the bulk operation; this
         * function adds such objects to this list to ensure the objects are
         * deallocated when no longer needed
         * @param[out] error_out	any error encountered during the operation
         */
        void add_task(const isi_cloud::task *task, const char *name,
                      djob_id_t job_id, std::vector<struct hsbt_bulk_entry> &sbt_ops,
                      scoped_object_list &objects_to_delete, struct isi_error **error_out);

        /**
         * Add to an SBT bulk operation for the purpose of starting a task.  Assumes
         * task is locked.
         */
        void start_task(const isi_cloud::task *task,
                        std::vector<struct hsbt_bulk_entry> &sbt_ops,
                        scoped_object_list &objects_to_delete, struct isi_error **error_out);

        /**
         * Add to an SBT bulk operation for the purpose of finishing a task.  Assumes
         * task is locked.
         */
        void finish_task(const isi_cloud::task *task,
                         task_process_completion_reason reason,
                         std::vector<struct hsbt_bulk_entry> &sbt_ops,
                         scoped_object_list &objects_to_delete, struct isi_error **error_out);

        /**
         * Pause an operation.  All current and future jobs of this operation
         * will be paused until the operation is resumed.  Error (EALREADY)
         * will be returned if operation is already paused.
         */
        void pause_operation(const cpool_operation_type *op_type,
                             struct isi_error **error_out);

        /**
         * Resume a paused operation.  Error (EALREADY) will be returned if
         * operation is not paused.
         */
        void resume_operation(const cpool_operation_type *op_type,
                              struct isi_error **error_out);

        /**
         * Pause a job.  All current and future tasks belonging to this job
         * will be paused until the job is resumed.  Error (EALREADY) will be
         * returned if the job is already paused.
         */
        void pause_job(djob_id_t daemon_job_id, struct isi_error **error_out);

        /**
         * Resume a paused job.  Error (EALREADY) will be returned if job is not
         * paused.
         */
        void resume_job(djob_id_t daemon_job_id, struct isi_error **error_out);

        /**
         * Cancel a job.  Error (EALREADY) will be returned if the job is already
         * cancelled.
         */
        void cancel_job(djob_id_t daemon_job_id, struct isi_error **error_out);

#ifdef PRC_TASK
        /**
         * Add to an SBT bulk operation for the purpose of pausing a task.  Assumes
         * task is locked.
         */
        void pause_task(const isi_cloud::task *task,
                        std::vector<struct hsbt_bulk_entry> &sbt_ops,
                        scoped_object_list &objects_to_delete, struct isi_error **error_out);

        /**
         * Add to an SBT bulk operation for the purpose of resuming a task.  Assumes
         * task is locked.
         */
        void resume_task(const isi_cloud::task *task,
                         std::vector<struct hsbt_bulk_entry> &sbt_ops,
                         scoped_object_list &objects_to_delete, struct isi_error **error_out);

        /**
         * Add to an SBT bulk operation for the purpose of cancelling a task.  Assumes
         * task is locked.
         */
        void cancel_task(const isi_cloud::task *task,
                         std::vector<struct hsbt_bulk_entry> &sbt_ops,
                         scoped_object_list &objects_to_delete, struct isi_error **error_out);
#endif // PRC_TASK

        /**
         * Remove a job.  Assumes daemon job record is already locked; daemon job
         * record will remain locked after this function.
         */
        void remove_job(const daemon_job_record *djob_record,
                        struct isi_error **error_out);

        /**
         * Cleanup expired completed jobs.
         */
        uint32_t cleanup_old_jobs(uint32_t max_num, time_t older_than,
                                  struct isi_error **error_out);

        void cleanup_jobs(uint32_t max_num, time_t older_than, isi_error **error_out);
        /**
         * Get the op_type for the given daemon_job_id
         */
        const cpool_operation_type *get_operation_type_for_job_id(
            djob_id_t daemon_job_id, struct isi_error **error_out);

        void query_job_for_members(djob_id_t daemon_job_id, unsigned int start,
                                   resume_key &start_key,
                                   unsigned int max, std::vector<daemon_job_member *> &members,
                                   const cpool_operation_type *op_type, struct isi_error **error_out);

        /**
         * Retrieve all daemon job records.  Caller is responsible for deleting each
         * item in the returned collection.
         */
        void get_all_jobs(std::list<daemon_job_record *> &jobs,
                          struct isi_error **error_out);

        void check_job_complete(daemon_job_configuration *&djob_config,
                                daemon_job_record *djob_record, scoped_object_list &objects_to_delete,
                                struct isi_error **error_out);

    } // end namespace daemon_job_manager
} // end namespace isi_cloud

#endif // __CPOOL_DAEMON_JOB_MANAGER_H__
