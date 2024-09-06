/*
 * Prevent name mangling which leads to an unresolved symbol error when
 * building a C executable (such as isi_job_d).
 */
extern "C"
{
	void user_job_attrs_init(struct user_job_attrs *);
}

#include <stdlib.h>

#include <map>
#include <string>

#include <isi_daemon/isi_daemon.h>
#include <isi_gconfig/gconfig_helpers.h>
#include <isi_job/job.h>
#include <isi_job/message_gen.h>
#include <isi_job/jobstatus_gcfg.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_sbtree/sbtree.h>
#include <isi_upgrade_api/isi_upgrade_api.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util_cpp/scoped_clean.h>

#include "cpool_d_common.h"
#include "cpool_d_debug.h"
#include "cpool_fd_store.h"
#include "daemon_job_configuration.h"
#include "daemon_job_member.h"
#include "daemon_job_record.h"
#include "daemon_job_request.h"
#include "daemon_job_stats.h"
#include "hsbt.h"
#include "ifs_cpool_flock.h"
#include "scoped_object_list.h"
#include "task.h"
#include "task_map.h"

#include <isi_cpool_d_common/daemon_job_reader.h>

#define CPOOL_DJM_PRIVATE
#include "daemon_job_manager_private.h"
#undef CPOOL_DJM_PRIVATE

#include "daemon_job_manager.h"

#define SBT_ENTRY_MAX_SIZE 8192
#define DJOB_RECORD_NOT_YET_COMPLETED 0

namespace isi_cloud
{
	namespace daemon_job_manager
	{

		void
		create_daemon_job_config(struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			int djobs_sbt_fd, result;
			daemon_job_configuration djob_config;
			const void *djob_config_serialization;
			size_t djob_config_serialization_size;
			btree_key_t djob_config_key =
				daemon_job_record::get_key(
					daemon_job_configuration::get_daemon_job_id());

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(true,
																		  &error);
			ON_ISI_ERROR_GOTO(out, error);

			djob_config.get_serialization(djob_config_serialization,
										  djob_config_serialization_size);

			result = ifs_sbt_add_entry(djobs_sbt_fd, &djob_config_key,
									   djob_config_serialization, djob_config_serialization_size, NULL);
			if (result != 0 && errno != EEXIST)
			{
				error = isi_system_error_new(errno,
											 "failed to create daemon job configuration");
				goto out;
			}

		out:
			isi_error_handle(error, error_out);
		}

		void
		lock_daemon_job_config(bool block, bool exclusive,
							   struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			lock_daemon_job_record(daemon_job_configuration::get_daemon_job_id(),
								   block, exclusive, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		unlock_daemon_job_config(struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			unlock_daemon_job_record(daemon_job_configuration::get_daemon_job_id(),
									 &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		daemon_job_configuration *
		get_daemon_job_config(struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			daemon_job_configuration *ret_config = NULL;
			int fd, result;
			btree_key_t djob_config_key =
				daemon_job_record::get_key(
					daemon_job_configuration::get_daemon_job_id());
			char ent_buf[SBT_ENTRY_MAX_SIZE];
			size_t ent_size_out;

			fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			result = ifs_sbt_get_entry_at(fd, &djob_config_key, ent_buf,
										  sizeof ent_buf, NULL, &ent_size_out);
			if (result != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to read daemon job configuration");
				goto out;
			}

			ret_config = new daemon_job_configuration;
			if (ret_config == NULL)
			{
				error = isi_system_error_new(ENOMEM,
											 "failed to allocate daemon_job_configuration object");
				goto out;
			}

			ret_config->set_serialization(ent_buf, ent_size_out, &error);
			if (error != NULL)
			{
				delete ret_config;
				ret_config = NULL;

				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			/*
			 * XOR here - either we're returning an error or a configuration, but
			 * not both.
			 */
			ASSERT((error == NULL) != (ret_config == NULL));

			isi_error_handle(error, error_out);

			return ret_config;
		}

		void
		lock_daemon_job_record(djob_id_t id, bool block, bool exclusive,
							   struct isi_error **error_out)
		{
			ASSERT(id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			int fd, result;
			struct flock params;

			populate_flock_params(id, &params);
			if (!exclusive)
				params.l_type = CPOOL_LK_SR;

			fd = cpool_fd_store::getInstance().get_djob_record_lock_fd(&error);
			ON_ISI_ERROR_GOTO(out, error);

			result = ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_JOBS, F_SETLK, &params,
									 block ? F_SETLKW : 0);
			if (result != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to lock daemon job record (id: %d)", id);
				goto out;
			}

		out:
			isi_error_handle(error, error_out);
		}

		void
		unlock_daemon_job_record(djob_id_t id, struct isi_error **error_out)
		{
			ASSERT(id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			int fd, result;
			struct flock params;

			populate_flock_params(id, &params);

			fd = cpool_fd_store::getInstance().get_djob_record_lock_fd(&error);
			ON_ISI_ERROR_GOTO(out, error);

			result = ifs_cpool_flock(fd, LOCK_DOMAIN_CPOOL_JOBS, F_UNLCK, &params,
									 0);
			if (result != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to unlock daemon job record (id: %d)", id);
				goto out;
			}

		out:
			isi_error_handle(error, error_out);
		}

		/////由djob_id转换为btree_key.使用这个key,从sbt中get_entry. new一个djob_record,反序列换entry_buf,从而生成这个djob_record实例
		daemon_job_record *
		get_daemon_job_record(djob_id_t id, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			daemon_job_record *ret_record = NULL;
			int fd, result;
			btree_key_t djob_record_key = daemon_job_record::get_key(id);
			char ent_buf[SBT_ENTRY_MAX_SIZE];
			size_t ent_size_out;

			fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			result = ifs_sbt_get_entry_at(fd, &djob_record_key, ent_buf,
										  sizeof ent_buf, NULL, &ent_size_out);
			if (result != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to read daemon job record (id: %d)", id);
				goto out;
			}

			ret_record = new daemon_job_record;
			ASSERT(ret_record != NULL);

			ret_record->set_serialization(ent_buf, ent_size_out, &error); //////通过反序列化,将获得的字节流转化为djob_record
			if (error != NULL)
			{
				delete ret_record;
				ret_record = NULL;

				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			/*
			 * XOR here - either we're returning an error or a record, but not
			 * both.
			 */
			ASSERT((error == NULL) != (ret_record == NULL));

			isi_error_handle(error, error_out);

			return ret_record;
		}

		daemon_job_member *
		get_daemon_job_member(djob_id_t djob_id, const task_key *key,
							  struct isi_error **error_out)
		{
			ASSERT(djob_id != DJOB_RID_INVALID);
			ASSERT(key != NULL);

			struct isi_error *error = NULL;
			int member_sbt_fd = -1, result;
			const void *serialized_key;
			size_t serialized_key_len;
			bool found = true;
			daemon_job_member *ret_member = NULL;
			char ent_buf[SBT_ENTRY_MAX_SIZE];
			size_t ent_size_out = sizeof ent_buf;

			member_sbt_fd = open_member_sbt(djob_id, false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			key->get_serialized_key(serialized_key, serialized_key_len);

			if (key->get_op_type()->uses_hsbt())
			{
				found = hsbt_get_entry(member_sbt_fd, serialized_key,
									   serialized_key_len, ent_buf, &ent_size_out, NULL, &error);
				if (!found && error == NULL)
				{
					error = isi_system_error_new(ENOENT,
												 "failed to retrieve daemon job member "
												 "for key %{} in daemon job %d",
												 cpool_task_key_fmt(key), djob_id);
				}
			}
			else
			{
				ASSERT(serialized_key_len == sizeof(btree_key_t),
					   "%zu != %zu", serialized_key_len, sizeof(btree_key_t));

				const btree_key_t *bt_key =
					static_cast<const btree_key_t *>(serialized_key);
				ASSERT(bt_key != NULL);

				result = ifs_sbt_get_entry_at(member_sbt_fd,
											  const_cast<btree_key_t *>(bt_key), ent_buf, sizeof ent_buf,
											  NULL, &ent_size_out);
				if (result != 0)
				{
					error = isi_system_error_new(errno,
												 "failed to retrieve daemon job member "
												 "for key %{} in daemon job %d",
												 cpool_task_key_fmt(key), djob_id);
				}
			}
			ON_ISI_ERROR_GOTO(out, error);

			ASSERT(found);

			ret_member = new daemon_job_member(key);
			ASSERT(ret_member != NULL);

			ret_member->set_serialization(&(ent_buf[0]), ent_size_out, &error);
			if (error != NULL)
			{
				delete ret_member;
				ret_member = NULL;

				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			if (member_sbt_fd != -1)
				close_sbt(member_sbt_fd);

			/*
			 * XOR here - either we're returning an error or a member, but not
			 * both.
			 */
			ASSERT((error == NULL) != (ret_member == NULL));

			isi_error_handle(error, error_out);

			return ret_member;
		}

		daemon_job_member *
		get_daemon_job_member(djob_id_t djob_id, const isi_cloud::task *task,
							  struct isi_error **error_out)
		{
			ASSERT(task != NULL);

			struct isi_error *error = NULL;

			daemon_job_member *ret_member =
				get_daemon_job_member(djob_id, task->get_key(), &error);

			isi_error_handle(error, error_out);

			return ret_member;
		}

		djob_id_t
		get_next_available_job_id(int max_jobs, daemon_job_configuration *&djob_config,
								  scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(max_jobs >= 0 ||
					   max_jobs == daemon_job_configuration::UNLIMITED_DAEMON_JOBS,
				   "max_jobs: %d", max_jobs);
			ASSERT(djob_config == NULL);

			struct isi_error *error = NULL;
			bool local_ro_config_locked = false;
			daemon_job_configuration *local_ro_config = NULL;
			djob_id_t ret_id = DJOB_RID_INVALID, temp_id;
			int djobs_sbt_fd;
			daemon_job_record *djob_record = NULL;

			/*
			 * Lock the daemon job configuration in order to read the value of the
			 * last used job ID.  We'll have to give up this lock after reading the
			 * value in order to not violate deadlock prevention rules, which
			 * technically means that the last used job ID value we read here could
			 * become out of date; this is OK as we'll use the value only as a
			 * starting point for finding the next available job ID.
			 */
			/* true: block, false: shared lock */
			lock_daemon_job_config(true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			local_ro_config_locked = true;

			local_ro_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);

			temp_id = local_ro_config->get_last_used_daemon_job_id() + 1;

			delete local_ro_config;
			local_ro_config = NULL;

			unlock_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);
			local_ro_config_locked = false;

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false,
																		  &error);
			ON_ISI_ERROR_GOTO(out, error);

			for (; ret_id == DJOB_RID_INVALID && temp_id != DJOB_RID_INVALID;
				 ++temp_id)
			{
				/*
				 * Take an exclusive lock even though we're just reading the
				 * daemon job record that may or may not exist for temp_id; if
				 * no record exists we'll need exclusive access, and we can't
				 * upgrade a shared lock to an exclusive lock.  Don't block
				 * while waiting for this lock; if another thread has a lock on
				 * this ID, either it will use it if no daemon job record
				 * exists or it will continue looking if one does exist; either
				 * way, this thread can't use this ID.
				 */
				/* false: don't block, true: exclusive lock */
				lock_daemon_job_record(temp_id, false, true, &error);
				if (error != NULL)
				{
					if (isi_system_error_is_a(error, EWOULDBLOCK))
					{
						isi_error_free(error);
						error = NULL;

						continue;
					}
					ON_ISI_ERROR_GOTO(out, error);
				}

				djob_record = get_daemon_job_record(temp_id, &error);
				if (error != NULL)
				{
					if (isi_system_error_is_a(error, ENOENT))
					{
						/*
						 * If we failed to get the daemon job record
						 * because none exists, we've found an
						 * available daemon job ID.  Don't release the
						 * lock for this ID now as it is kept by this
						 * thread until the daemon job record is
						 * persisted, but "schedule" the unlocking for
						 * later using the scoped_object_list.
						 */
						objects_to_delete.add_locked_daemon_job_record(temp_id);

						ret_id = temp_id;

						isi_error_free(error);
						error = NULL;
					}
					else
					{
						/*
						 * We've failed for an unanticipated reason, so
						 * bail out.
						 */
						unlock_daemon_job_record(temp_id,
												 isi_error_suppress(IL_NOTICE));
						ON_ISI_ERROR_GOTO(out, error);
					}
				}
				else
				{
					/*
					 * A daemon job record already exists for this ID, so
					 * obviously we can't use it.
					 */
					delete djob_record;
					djob_record = NULL;

					unlock_daemon_job_record(temp_id, &error);
					ON_ISI_ERROR_GOTO(out, error);
				}
			}

			/*
			 * If we've exhausted the number of daemon job IDs, there's nothing
			 * more we can do, so bail out.
			 */
			if (ret_id == DJOB_RID_INVALID)
			{
				error = isi_system_error_new(ENOSPC,
											 "no available daemon job IDs");
				goto out;
			}

			/*
			 * If we've found an ID, we can safely lock the daemon job
			 * configuration, which we'll need to do in order to verify that we can
			 * create a new daemon job with this ID.
			 */
			/* true: block, true: exclusive lock */
			lock_daemon_job_config(true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_locked_daemon_job_configuration();

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_config(djob_config);

			if (max_jobs == daemon_job_configuration::UNLIMITED_DAEMON_JOBS ||
				djob_config->get_num_active_jobs() < max_jobs)
			{
				djob_config->save_serialization();
				djob_config->increment_num_active_jobs();

				/*
				 * We can't blindly update the last used daemon job ID to
				 * ret_id here, because we have no guarantee that two or more
				 * threads that are getting job IDs will get the lock on the
				 * daemon job configuration in the same order as their job IDs.
				 * The solution is to update the last used daemon job ID only
				 * if it is greater than the current value.
				 */
				if (ret_id > djob_config->get_last_used_daemon_job_id())
					djob_config->set_last_used_daemon_job_id(ret_id);
			}
			else
			{
				error = isi_system_error_new(EBUSY,
											 "too many concurrent daemon jobs");

				/*
				 * Reset ret_id since we aren't returning a valid daemon job
				 * ID.  Yes, we're holding a lock on the record identified by
				 * ret_id, but that will be released once the passed-in
				 * scoped_object_list is destroyed.
				 */
				ret_id = DJOB_RID_INVALID;
				goto out;
			}

		out:
			if (local_ro_config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			isi_error_handle(error, error_out);

			return ret_id;
		}

		djob_id_t
		create_job_helper__multi_job(const cpool_operation_type *op_type,
									 std::vector<struct sbt_bulk_entry> &sbt_ops,
									 scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type->is_multi_job());

			struct isi_error *error = NULL;
			int djobs_sbt_fd, max_concurrent_jobs;
			djob_id_t djob_id = DJOB_RID_INVALID;
			daemon_job_record *djob_rec = NULL;
			const void *record_serialization;
			size_t record_serialization_size;
			daemon_job_configuration *djob_config = NULL;
			const void *old_config_serialization, *new_config_serialization;
			size_t old_config_serialization_size, new_config_serialization_size;
			struct sbt_bulk_entry sbt_op;

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false,
																		  &error);
			ON_ISI_ERROR_GOTO(out, error);

			max_concurrent_jobs =
				daemon_job_configuration::get_max_concurrent_daemon_jobs(&error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * Get the next available daemon job ID.  This function will add locks
			 * and/or objects to objects_to_delete that need to be released and/or
			 * deleted as necessary.
			 */
			djob_id = get_next_available_job_id(max_concurrent_jobs, djob_config,
												objects_to_delete, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * With an available (and locked) daemon job ID, create the new daemon
			 * job record.  All new jobs start in the running state, regardless of
			 * their parent operation's state.
			 */
			djob_rec = new daemon_job_record;
			ASSERT(djob_rec != NULL);
			objects_to_delete.add_daemon_job_record(djob_rec);

			djob_rec->set_operation_type(op_type);
			djob_rec->set_daemon_job_id(djob_id);
			djob_rec->set_state(DJOB_OJT_RUNNING);

			djob_rec->get_serialization(record_serialization,
										record_serialization_size);

			/*
			 * Add this new daemon job record's addition to the SBT bulk operation.
			 */
			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.fd = djobs_sbt_fd;
			sbt_op.op_type = SBT_SYS_OP_ADD;
			sbt_op.key = djob_rec->get_key();
			sbt_op.entry_buf = const_cast<void *>(record_serialization);
			sbt_op.entry_size = record_serialization_size;

			sbt_ops.push_back(sbt_op);

			/*
			 * Add the daemon job configuration update to the SBT bulk operation.
			 * (The daemon job configuration has already been changed by
			 * get_next_available_job_id).
			 */

			djob_config->get_saved_serialization(old_config_serialization,
												 old_config_serialization_size);
			djob_config->get_serialization(new_config_serialization,
										   new_config_serialization_size);

			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.fd = djobs_sbt_fd;
			sbt_op.op_type = SBT_SYS_OP_MOD;
			sbt_op.key =
				daemon_job_record::get_key(
					daemon_job_configuration::get_daemon_job_id());
			sbt_op.entry_buf = const_cast<void *>(new_config_serialization);
			sbt_op.entry_size = new_config_serialization_size;
			sbt_op.cm = BT_CM_BUF;
			sbt_op.old_entry_buf =
				const_cast<void *>(old_config_serialization);
			sbt_op.old_entry_size = old_config_serialization_size;

			sbt_ops.push_back(sbt_op);

		out:
			isi_error_handle(error, error_out);

			return djob_id;
		}

		djob_id_t
		create_job_helper__single_job(const cpool_operation_type *op_type,
									  std::vector<struct sbt_bulk_entry> &sbt_ops,
									  scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(!op_type->is_multi_job());

			struct isi_error *error = NULL;
			djob_id_t djob_id = op_type->get_single_job_id();
			int djobs_sbt_fd;
			daemon_job_record *existing_djob_rec = NULL, *new_djob_rec = NULL;
			const void *record_serialization;
			size_t record_serialization_size;
			daemon_job_configuration *djob_config = NULL;
			const void *old_config_serialization, *new_config_serialization;
			size_t old_config_serialization_size, new_config_serialization_size;
			struct sbt_bulk_entry sbt_op;

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false,
																		  &error);
			ON_ISI_ERROR_GOTO(out, error);

			/* true: block, true: exclusive lock */
			lock_daemon_job_record(djob_id, true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_locked_daemon_job_record(djob_id);

			existing_djob_rec = get_daemon_job_record(djob_id, &error);
			if (error == NULL)
			{
				/*
				 * The daemon job already exists for this single-job operation,
				 * so we can bail out.  We add existing_djob_rec to
				 * objects_to_delete here just for the sake of symmetry, but we
				 * could have instead deleted it here.
				 */
				objects_to_delete.add_daemon_job_record(existing_djob_rec);
				goto out;
			}
			if (!isi_system_error_is_a(error, ENOENT))
			{
				/* Unexpected error, bail out. */
				isi_error_add_context(error,
									  "could not find daemon job record for %{}",
									  task_type_fmt(op_type->get_type()));
				goto out;
			}

			isi_error_free(error);
			error = NULL;

			/*
			 * Create the daemon job record.  All new jobs start in the running
			 * state, regardless of their parent operation's state.
			 */
			new_djob_rec = new daemon_job_record;
			ASSERT(new_djob_rec != NULL);
			objects_to_delete.add_daemon_job_record(new_djob_rec);

			new_djob_rec->set_operation_type(op_type);
			new_djob_rec->set_daemon_job_id(djob_id);
			new_djob_rec->set_state(DJOB_OJT_RUNNING);

			new_djob_rec->get_serialization(record_serialization,
											record_serialization_size);

			/*
			 * Add this new daemon job record's addition to the SBT bulk operation.
			 */
			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.fd = djobs_sbt_fd;
			sbt_op.op_type = SBT_SYS_OP_ADD;
			sbt_op.key = new_djob_rec->get_key();
			sbt_op.entry_buf = const_cast<void *>(record_serialization);
			sbt_op.entry_size = record_serialization_size;

			sbt_ops.push_back(sbt_op);

			/*
			 * Since we're no longer going to lock any daemon job records, we can
			 * lock the daemon job configuration to update it without fear of
			 * deadlock.
			 */
			/* true: block, true: exclusive lock */
			lock_daemon_job_config(true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_locked_daemon_job_configuration();

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_config(djob_config);

			djob_config->save_serialization();
			djob_config->increment_num_active_jobs();

			djob_config->get_saved_serialization(old_config_serialization,
												 old_config_serialization_size);
			djob_config->get_serialization(new_config_serialization,
										   new_config_serialization_size);

			/*
			 * Add the daemon job configuration updated to the SBT bulk operation.
			 */
			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.fd = djobs_sbt_fd;
			sbt_op.op_type = SBT_SYS_OP_MOD;
			sbt_op.key =
				daemon_job_record::get_key(
					daemon_job_configuration::get_daemon_job_id());
			sbt_op.entry_buf = const_cast<void *>(new_config_serialization);
			sbt_op.entry_size = new_config_serialization_size;
			sbt_op.cm = BT_CM_BUF;
			sbt_op.old_entry_buf =
				const_cast<void *>(old_config_serialization);
			sbt_op.old_entry_size = old_config_serialization_size;

			sbt_ops.push_back(sbt_op);

		out:
			isi_error_handle(error, error_out);

			return djob_id;
		}

		djob_id_t
		create_job(const cpool_operation_type *op_type, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			struct isi_error *error = NULL;
			djob_id_t djob_id = DJOB_RID_INVALID;
			std::vector<struct sbt_bulk_entry> sbt_ops;
			scoped_object_list objects_to_delete;
			int djob_member_sbt_fd = -1;

			if (op_type->is_multi_job())
			{
				djob_id = create_job_helper__multi_job(op_type, sbt_ops,
													   objects_to_delete, &error);
			}
			else
			{
				djob_id = create_job_helper__single_job(op_type, sbt_ops,
														objects_to_delete, &error);
			}
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * There might not be anything to do, such as in the case where an
			 * existing single job is to be created.  Feeding zero operations to
			 * ifs_sbt_bulk_op yields an error, so in such a case, bail out now.
			 */
			if (sbt_ops.size() == 0)
				goto out;

			/*
			 * Before committing the (potential) daemon job record addition and
			 * (potential) daemon job configuration update, create the daemon job
			 * member SBT for this job.  It's OK that this is done outside of the
			 * bulk operation; for one thing we can't create an SBT as part of a
			 * bulk operation, but if the bulk operation were to fail after we've
			 * created this new member SBT, there wouldn't be any members in it
			 * that might affect the next daemon job that gets the same job ID.
			 * We want to avoid a scenario in which the SBT changes have been
			 * committed but the daemon job member SBT fails to be created, which
			 * is why we prepare the bulk operation, create the member SBT, and
			 * then commit the bulk operation.
			 */
			djob_member_sbt_fd = open_member_sbt(djob_id, true, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (ifs_sbt_bulk_op(sbt_ops.size(), sbt_ops.data()) != 0)
			{
				error = isi_system_error_new(errno, "ifs_sbt_bulk_op failed");
				goto out;
			}

		out:
			if (djob_member_sbt_fd != -1)
				close_sbt(djob_member_sbt_fd);

			isi_error_handle(error, error_out);

			/*
			 * Once objects_to_delete falls out of scope, it will unlock/delete
			 * daemon job records and configurations as needed.
			 */

			return djob_id;
		}

		void
		at_startup(struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			const cpool_operation_type *op_type = NULL;

			create_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);

			/* Start each of the non-"multi-jobs". */
			OT_FOREACH(op_type)
			{
				if (!op_type->is_multi_job())
				{
					create_job(op_type, &error);
					ON_ISI_ERROR_GOTO(out, error,
									  "failed to create %{} job at startup",
									  task_type_fmt(op_type->get_type()));
				}
			}

			ilog(IL_NOTICE, "daemon job manager successfully initialized");

		out:
			isi_error_handle(error, error_out);
		}

		djob_op_job_task_state
		djob_ojt_combine(djob_op_job_task_state lhs, djob_op_job_task_state rhs,
						 bool is_combined_jobs_state)
		{
			/*
			 * To combine states, note the ordering in daemon_job_types.h:
			 *	RUNNING
			 *	PAUSED
			 *	CANCELLED
			 *	ERROR
			 *	COMPLETED
			 *
			 * When combining any of the operation state, combined job state, and
			 * task state, the resultant state can only fall down the list.  When
			 * combining the states of jobs for which a task is a member, the
			 * resultant state can only climb up the list; this means that if a
			 * task is owned by multiple jobs, the task can be processed so long as
			 * a single owning job is running.
			 *
			 * With that in mind, combining states is just a numerical comparison,
			 * abusing the fact that enumerated types can be treated as integers.
			 */

			djob_op_job_task_state state;

			if (is_combined_jobs_state)
				state = (lhs < rhs ? lhs : rhs);
			else
				state = (lhs < rhs ? rhs : lhs);

			return state;
		}

		djob_op_job_task_state
		get_state(const cpool_operation_type *op_type, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			struct isi_error *error = NULL;
			djob_op_job_task_state state = DJOB_OJT_NONE;
			daemon_job_configuration *djob_config = NULL;
			bool config_locked = false;

			/* true: block, false: shared lock */
			lock_daemon_job_config(true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			config_locked = true;

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);
			state = djob_config->get_operation_state(op_type);

		out:
			if (config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			if (djob_config != NULL)
				delete djob_config;

			isi_error_handle(error, error_out);

			return state;
		}

		djob_op_job_task_state
		get_state(const daemon_job_record *djob_rec, struct isi_error **error_out)
		{
			ASSERT(djob_rec != NULL);

			struct isi_error *error = NULL;
			djob_op_job_task_state state = DJOB_OJT_NONE;
			bool config_locked = false;

			/*
			 * The state of a job is a function of the state of the operation to
			 * which the job belongs and the job itself.
			 */
			const daemon_job_configuration *djob_config = NULL;
			const cpool_operation_type *op_type = NULL;

			/* true: block, false: shared lock */
			lock_daemon_job_config(true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			config_locked = true;

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);

			op_type = djob_rec->get_operation_type();

			/* false: not combining job states (see djob_ojt_combine) */
			state = djob_ojt_combine(djob_config->get_operation_state(op_type),
									 djob_rec->get_state(), false);

		out:
			if (config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			if (djob_config != NULL)
				delete djob_config;

			isi_error_handle(error, error_out);

			return state;
		}

		djob_op_job_task_state
		get_state(djob_id_t djob_id, struct isi_error **error_out)
		{
			ASSERT(djob_id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			djob_op_job_task_state state = DJOB_OJT_NONE;
			const daemon_job_record *djob_record = NULL;
			bool record_locked = false;

			/* true: block, false: shared lock */
			lock_daemon_job_record(djob_id, true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = true;

			djob_record = get_daemon_job_record(djob_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			state = get_state(djob_record, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			if (record_locked)
				unlock_daemon_job_record(djob_id,
										 isi_error_suppress(IL_NOTICE));

			if (djob_record != NULL)
				delete djob_record;

			isi_error_handle(error, error_out);

			return state;
		}

		djob_op_job_task_state
		get_state(const isi_cloud::task *task, struct isi_error **error_out)
		{
			ASSERT(task != NULL);

			struct isi_error *error = NULL;
			djob_op_job_task_state state = DJOB_OJT_NONE, op_state;
			const std::set<djob_id_t> member_jobs = task->get_daemon_jobs();
			std::set<djob_id_t>::const_iterator member_job_iter;
			std::map<djob_id_t, djob_op_job_task_state> locked_records;
			daemon_job_record *djob_rec = NULL;
			scoped_object_list objects_to_delete;
			bool config_locked = false;
			const daemon_job_configuration *djob_config = NULL;
			std::map<djob_id_t, djob_op_job_task_state>::const_iterator lr_iter;
			// ^ use scoped_object_list here?

			/*
			 * The task is already locked, so use it to determine which job records
			 * to lock (using the set guarantees we lock the in the correct order),
			 * and then lock the daemon job configuration.  While we iterate over
			 * the job records, grab their state to prevent the need for a second
			 * iteration.
			 */
			member_job_iter = member_jobs.begin();
			for (; member_job_iter != member_jobs.end(); ++member_job_iter)
			{
				/* true: block, false: shared lock */
				lock_daemon_job_record(*member_job_iter, true, false, &error);
				ON_ISI_ERROR_GOTO(out, error,
								  "daemon job ID: %d", *member_job_iter);

				/*
				 * Add the daemon job ID to the locked job map now even
				 * though we don't have its state.  We do this so that if a
				 * failure occurs while looking up the state, we still unlock
				 * the daemon job.
				 */
				locked_records[*member_job_iter] = DJOB_OJT_NONE;

				djob_rec = get_daemon_job_record(*member_job_iter, &error);
				ON_ISI_ERROR_GOTO(out, error,
								  "daemon job ID: %d", *member_job_iter);
				objects_to_delete.add_daemon_job_record(djob_rec);

				/*
				 * Now that we can determine the state, update the locked
				 * records item we just added.
				 */
				locked_records[*member_job_iter] = djob_rec->get_state();
			}

			/* true: block, false: shared lock */
			lock_daemon_job_config(true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			config_locked = true;

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_config(djob_config);

			/*
			 * With all needed locks taken, determine the state of the task.
			 *
			 * First, combine the state of the owning jobs.
			 */
			state = DJOB_OJT_COMPLETED;
			lr_iter = locked_records.begin();
			for (; lr_iter != locked_records.end(); ++lr_iter)
			{
				ASSERT(lr_iter->second != DJOB_OJT_COMPLETED,
					   "%{} state for job %d of task %{}",
					   djob_op_job_task_state_fmt(DJOB_OJT_COMPLETED),
					   lr_iter->first, cpool_task_fmt(task));

				/*
				 * true: determining combined job state (see djob_ojt_combine)
				 */
				state = djob_ojt_combine(state, lr_iter->second, true);
			}

			/* Next combine the combined job state and the operation state. */
			op_state = djob_config->get_operation_state(task->get_op_type());
			/* false: not determining combined job state (see djob_ojt_combine) */
			state = djob_ojt_combine(state, op_state, false);

			/* Finally, combine this with the task state. */
			/* false: not determining combined job state (see djob_ojt_combine) */
			state = djob_ojt_combine(state, task->get_state(), false);

		out:
			if (config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			lr_iter = locked_records.begin();
			for (; lr_iter != locked_records.end(); ++lr_iter)
				unlock_daemon_job_record(lr_iter->first,
										 isi_error_suppress(IL_NOTICE));

			isi_error_handle(error, error_out);

			return state;
		}

		bool
		is_operation_startable(const cpool_operation_type *op_type,
							   struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			struct isi_error *error = NULL;
			bool startable = false;

			djob_op_job_task_state state = get_state(op_type, &error);
			ON_ISI_ERROR_GOTO(out, error);

			switch (state)
			{
			case DJOB_OJT_RUNNING:
				startable = true;
				break;
			case DJOB_OJT_PAUSED:
			case DJOB_OJT_CANCELLED:
				startable = false;
				break;
			case DJOB_OJT_COMPLETED:
			case DJOB_OJT_ERROR:
				startable = !op_type->is_multi_job();
				break;
			default:
				ASSERT(false, "unexpected operation state: %{}",
					   djob_op_job_task_state_fmt(state));
			}

		out:
			isi_error_handle(error, error_out);

			return startable;
		}

		int
		open_member_sbt(djob_id_t id, bool create, struct isi_error **error_out)
		{
			ASSERT(id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;

			int fd = -1;
			char *sbt_name = get_djob_file_list_sbt_name(id);
			ASSERT(sbt_name != NULL);

			fd = open_sbt(sbt_name, create, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			free(sbt_name);

			/*
			 * XOR here - we're returning a valid file descriptor or an error but
			 * not both.
			 */
			ASSERT((error == NULL) != (fd == -1), "fd: %d error: %#{}",
				   fd, isi_error_fmt(error));

			isi_error_handle(error, error_out);

			return fd;
		}

		djob_id_t
		start_job_no_job_engine(const cpool_operation_type *op_type,
								struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			struct isi_error *error = NULL;
			djob_id_t djob_id = DJOB_RID_INVALID;

			/*
			 * If starting a multi-job, create a new one, otherwise return the ID
			 * of the job already created at startup.
			 */
			if (op_type->is_multi_job())
			{
				djob_id = create_job(op_type, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
			else
				djob_id = op_type->get_single_job_id();

		out:
			isi_error_handle(error, error_out);

			return djob_id;
		}

		djob_id_t start_job(const daemon_job_request *request,
							struct isi_error **error_out)
		{
			ASSERT(request != NULL);

			struct isi_error *error = NULL;
			struct isi_error *disposable_error = NULL;

			bool record_locked = false;
			daemon_job_record *djob_record = NULL;
			jd_jid_t je_id = NULL_JID;

			const void *old_rec_serialization, *new_rec_serialization;
			size_t old_rec_serialization_size, new_rec_serialization_size;

			djob_id_t djob_id = start_job_no_job_engine(request->get_type(),
														&error);
			ON_ISI_ERROR_GOTO(out, error);

			if (request->get_type()->get_type() == CPOOL_TASK_TYPE_RESTORE_COI)
			{
				// Do not create job engine jobs for COI restore
				goto out;
			}

			je_id = delegate_to_job_engine(request, djob_id, &error);

			/*
			 * Successfully started job engine - add je_id to the daemon job record
			 */
			if (error == NULL && je_id != NULL_JID)
			{

				int daemon_job_record_sbt_fd =
					cpool_fd_store::getInstance().get_djobs_sbt_fd(false,
																   &error);
				ON_ISI_ERROR_GOTO(out, error);

				/* true: block, true: exclusive lock */
				lock_daemon_job_record(djob_id, true, true, &error);
				ON_ISI_ERROR_GOTO(out, error);
				record_locked = true;

				djob_record = get_daemon_job_record(djob_id, &error);
				ON_ISI_ERROR_GOTO(out, error);

				djob_record->save_serialization();

				djob_record->set_job_engine_job_id(je_id);

				djob_record->get_saved_serialization(old_rec_serialization,
													 old_rec_serialization_size);
				djob_record->get_serialization(new_rec_serialization,
											   new_rec_serialization_size);

				btree_key_t record_key = djob_record->get_key();

				if (ifs_sbt_cond_mod_entry(daemon_job_record_sbt_fd, &record_key,
										   new_rec_serialization, new_rec_serialization_size, NULL,
										   BT_CM_BUF,
										   old_rec_serialization, old_rec_serialization_size, NULL) != 0)
				{
					error = isi_system_error_new(errno,
												 "failed to modify daemon job record (id: %d)",
												 djob_id);
					goto out;
				}

				/*
				 * If we had trouble starting job engine, we need to put the job in a
				 * finalized state.  However, since 'error' is already populated, we'll
				 * use a temporary isi_error (e.g., 'disposable_error') to do this.
				 * Since disposable_error is, well, disposable - we'll just log it if
				 * populated instead of bubbling it up to the caller.
				 */
			}
			else if (error != NULL)
			{

				/* true: block, true: exclusive lock */
				lock_daemon_job_record(djob_id, true, true, &disposable_error);
				ON_ISI_ERROR_GOTO(out, disposable_error);
				record_locked = true;

				djob_record = get_daemon_job_record(djob_id, &disposable_error);
				ON_ISI_ERROR_GOTO(out, disposable_error);

				djob_record->save_serialization();

				djob_record->set_job_engine_state(STATE_ABORTED);

				int daemon_job_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &disposable_error);
				ON_ISI_ERROR_GOTO(out, disposable_error);

				btree_key_t record_key = djob_record->get_key();

				struct hsbt_bulk_entry sbt_op;
				std::vector<struct hsbt_bulk_entry> sbt_ops;
				scoped_object_list objects_to_delete;

				daemon_job_configuration *djob_config = NULL;
				check_job_complete(djob_config, djob_record, objects_to_delete,
								   &disposable_error);
				ON_ISI_ERROR_GOTO(out, disposable_error);

				djob_record->set_state(DJOB_OJT_ERROR);

				djob_record->get_saved_serialization(old_rec_serialization,
													 old_rec_serialization_size);
				djob_record->get_serialization(new_rec_serialization,
											   new_rec_serialization_size);

				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.is_hash_btree = false;
				sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
				sbt_op.bulk_entry.key = record_key;
				sbt_op.bulk_entry.cm = BT_CM_BUF;
				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_rec_serialization);
				sbt_op.bulk_entry.old_entry_size = old_rec_serialization_size;
				sbt_op.bulk_entry.entry_buf =
					const_cast<void *>(new_rec_serialization);
				sbt_op.bulk_entry.entry_size = new_rec_serialization_size;

				sbt_ops.push_back(sbt_op);

				if (djob_config != NULL)
				{
					const void *old_cfg_serialization;
					size_t old_cfg_serialization_size;
					djob_config->get_saved_serialization(
						old_cfg_serialization, old_cfg_serialization_size);

					const void *new_cfg_serialization;
					size_t new_cfg_serialization_size;
					djob_config->get_serialization(new_cfg_serialization,
												   new_cfg_serialization_size);

					memset(&sbt_op, 0, sizeof sbt_op);

					sbt_op.is_hash_btree = false;
					sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
					sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
					sbt_op.bulk_entry.key =
						daemon_job_record::get_key(
							daemon_job_configuration::get_daemon_job_id());
					sbt_op.bulk_entry.cm = BT_CM_BUF;
					sbt_op.bulk_entry.old_entry_buf =
						const_cast<void *>(old_cfg_serialization);
					sbt_op.bulk_entry.old_entry_size =
						old_cfg_serialization_size;
					sbt_op.bulk_entry.entry_buf =
						const_cast<void *>(new_cfg_serialization);
					sbt_op.bulk_entry.entry_size =
						new_cfg_serialization_size;

					sbt_ops.push_back(sbt_op);
				}

				hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &disposable_error);
				ON_ISI_ERROR_GOTO(out, disposable_error);
			}

		out:
			if (record_locked)
				unlock_daemon_job_record(djob_id,
										 isi_error_suppress(IL_NOTICE));

			if (djob_record != NULL)
				delete djob_record;

			if (disposable_error)
			{
				ilog(IL_NOTICE, "Error while putting job into a terminal "
								"state after job engine failure: %#{}",
					 isi_error_fmt(disposable_error));
				isi_error_free(disposable_error);
			}

			isi_error_handle(error, error_out);

			return djob_id;
		}

		/*
		 * The relationship between job engine and daemon jobs is subtle. We use job
		 * engine to do LIN scanning jobs and treewalking jobs. Here is the pecking
		 * order for job engine delegation:
		 *
		 * if ( job has_directories or job has_files )
		 * do job engine treewalk (match items in given directories)
		 *
		 * else if (job has_filter or job has_policy)
		 * do job engine LIN scan (match all LINs against filter/policy)
		 *
		 * else if (job is archive)
		 * do job engine LIN scan (find a matching policy for all LINs)
		 *
		 * else
		 * do nothing
		 */
		jd_jid_t
		delegate_to_job_engine(const daemon_job_request *request,
							   djob_id_t djob_id, struct isi_error **error_out)
		{
			ASSERT(request != NULL);

			struct isi_error *error = NULL;
			daemon_job_record *record = NULL;
			bool record_locked = false;

			/* Use a LIN scan unless directories are explicitly specified. */
			enum job_type_id je_type = CLOUDPOOL_LIN_JOB;
			jd_jid_t je_id = NULL_JID;

			struct user_job_attrs uattrs;
			user_job_attrs_init(&uattrs);

			const std::vector<char *> &file_list = request->get_files();
			std::vector<char *>::const_iterator it_file;

			const std::vector<char *> &dir_list = request->get_directories();
			std::vector<char *>::const_iterator it_dir;

			const void *old_rec_serialization, *new_rec_serialization;
			size_t old_rec_serialization_size, new_rec_serialization_size;
			int daemon_job_record_sbt_fd;
			btree_key_t record_key;

			/* true: block, true: exclusive lock */
			lock_daemon_job_record(djob_id, true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = true;

			record = get_daemon_job_record(djob_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			ASSERT(record != NULL);

			record_key = record->get_key();

			if (record->get_job_engine_job_id() != NULL_JID)
				goto out;

			record->save_serialization();
			daemon_job_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/* See note above about when JE jobs are started. */
			if (dir_list.empty() && file_list.empty() &&
				request->get_type() == OT_RECALL &&
				request->get_file_filter() == 0)
			{
				/* Job had nothing to do - mark it complete. */
				record->set_job_engine_state(STATE_FINISHED);

				struct hsbt_bulk_entry sbt_op;
				std::vector<struct hsbt_bulk_entry> sbt_ops;
				scoped_object_list objects_to_delete;

				daemon_job_configuration *djob_config = NULL;
				check_job_complete(djob_config, record, objects_to_delete,
								   &error);
				ON_ISI_ERROR_GOTO(out, error);

				record->get_saved_serialization(old_rec_serialization,
												old_rec_serialization_size);
				record->get_serialization(new_rec_serialization,
										  new_rec_serialization_size);

				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.is_hash_btree = false;
				sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
				sbt_op.bulk_entry.key = record_key;
				sbt_op.bulk_entry.cm = BT_CM_BUF;
				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_rec_serialization);
				sbt_op.bulk_entry.old_entry_size = old_rec_serialization_size;
				sbt_op.bulk_entry.entry_buf =
					const_cast<void *>(new_rec_serialization);
				sbt_op.bulk_entry.entry_size = new_rec_serialization_size;

				sbt_ops.push_back(sbt_op);

				if (djob_config != NULL)
				{
					const void *old_cfg_serialization;
					size_t old_cfg_serialization_size;
					djob_config->get_saved_serialization(
						old_cfg_serialization, old_cfg_serialization_size);

					const void *new_cfg_serialization;
					size_t new_cfg_serialization_size;
					djob_config->get_serialization(new_cfg_serialization,
												   new_cfg_serialization_size);

					memset(&sbt_op, 0, sizeof sbt_op);

					sbt_op.is_hash_btree = false;
					sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
					sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
					sbt_op.bulk_entry.key =
						daemon_job_record::get_key(
							daemon_job_configuration::get_daemon_job_id());
					sbt_op.bulk_entry.cm = BT_CM_BUF;
					sbt_op.bulk_entry.old_entry_buf =
						const_cast<void *>(old_cfg_serialization);
					sbt_op.bulk_entry.old_entry_size =
						old_cfg_serialization_size;
					sbt_op.bulk_entry.entry_buf =
						const_cast<void *>(new_cfg_serialization);
					sbt_op.bulk_entry.entry_size =
						new_cfg_serialization_size;

					sbt_ops.push_back(sbt_op);
				}

				hsbt_bulk_op(sbt_ops.size(), sbt_ops.data(), &error);
				ON_ISI_ERROR_GOTO(out, error);

				goto out;
			}

			uattrs.cloud_attr.daemon_job_id = record->get_daemon_job_id();
			uattrs.cloud_attr.type = (record->get_operation_type() == OT_ARCHIVE ? CJT_MANUAL_ARCHIVE : CJT_MANUAL_RECALL);
			uattrs.cloud_attr.archive_policy_id = 0;
			uattrs.cloud_attr.recall_filter_id = 0;

			/* Add files to treewalk. */
			it_file = file_list.begin();
			for (; it_file != file_list.end(); ++it_file)
			{
				/* If any file was specified, we need to do a treewalk. */
				je_type = CLOUDPOOL_TREEWALK_JOB;

				const char *filename = *it_file;

				struct stat stat_buf;
				if (stat(filename, &stat_buf) != 0)
				{
					error = isi_system_error_new(errno,
												 "failed to stat file (\"%s\")", filename);
					goto out;
				}

				if (!S_ISREG(stat_buf.st_mode))
				{
					error = isi_exception_error_new(
						"non-regular file specified for treewalk: \"%s\"",
						filename);
					goto out;
				}

				ifs_lin_t lin = stat_buf.st_ino;
				lin_set_add(&uattrs.tw_lins, lin);
			}

			/* Add directories to treewalk. */
			it_dir = dir_list.begin();
			for (; it_dir != dir_list.end(); ++it_dir)
			{
				/* If any directory was specified, we need to do a treewalk. */
				je_type = CLOUDPOOL_TREEWALK_JOB;

				const char *dir = *it_dir;

				struct stat stat_buf;
				if (stat(dir, &stat_buf) != 0)
				{
					error = isi_system_error_new(errno,
												 "failed to stat directory (\"%s\")", dir);
					goto out;
				}

				if (!S_ISDIR(stat_buf.st_mode))
				{
					error = isi_exception_error_new(
						"non-directory specified for treewalk: \"%s\"",
						dir);
					goto out;
				}

				ifs_lin_t lin = stat_buf.st_ino;
				lin_set_add(&uattrs.tw_lins, lin);

				/*
				 * If user specified "/ifs" as a directory, ignore dirs and do
				 * a LIN scan for efficiency.
				 */
				if (lin == ROOT_LIN)
				{
					lin_set_clean(&uattrs.tw_lins);
					je_type = CLOUDPOOL_LIN_JOB;
					break;
				}
			}

			if (record->get_operation_type() == OT_ARCHIVE)
			{
				uattrs.cloud_attr.archive_policy_id =
					lookup_policy_id(request->get_policy(), &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
			else
				uattrs.cloud_attr.recall_filter_id =
					request->get_file_filter();

			ASSERT(record_locked);

			unlock_daemon_job_record(djob_id, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = false;

			je_id = start_job_engine_job(&uattrs, je_type, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			if (record_locked)
				unlock_daemon_job_record(djob_id,
										 isi_error_suppress(IL_NOTICE));

			if (record != NULL)
				delete record;

			isi_error_handle(error, error_out);

			return je_id;
		}

		uint32_t
		lookup_policy_id(const char *policy_name, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			uint32_t policy_id = 0;
			struct smartpools_context *context = NULL;
			struct fp_policy *policy = NULL;

			/* Missing policy name is not an error condition. */
			if (policy_name == NULL || policy_name[0] == '\0')
				goto out;

			smartpools_open_context(&context, &error);
			ON_ISI_ERROR_GOTO(out, error);

			smartpools_get_policy_by_name(policy_name, &policy, context, &error);
			ON_ISI_ERROR_GOTO(out, error);

			ASSERT(policy != NULL);
			policy_id = policy->id;

		out:
			if (context != NULL)
				smartpools_close_context(&context,
										 isi_error_suppress(IL_NOTICE));

			isi_error_handle(error, error_out);

			return policy_id;
		}

		static void
		msg_free(struct jd_msg *msg)
		{
			user_msg_free(&msg, isi_error_suppress(IL_NOTICE));
		}

		static int
		open_job_engine_comm_socket(struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			const unsigned int timeout = 30;
			struct timeval tv;
			tv.tv_sec = timeout;
			tv.tv_usec = 0;

			int sock_fd = isi_daemon_conn_connect("isi_job_d", &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv) != 0)
				ilog(IL_INFO, "Error setting job engine timeout to %u: %s",
					 timeout, strerror(errno));
		out:
			isi_error_handle(error, error_out);

			return sock_fd;
		}

		static void
		receive_job_engine_message(int socket_fd, struct jd_msg **msg_out,
								   struct isi_error **error_out)
		{
			ASSERT(msg_out != NULL);
			ASSERT(*msg_out == NULL);
			ASSERT(socket_fd >= 0);

			struct isi_error *error = NULL;
			const unsigned int max_messaging_attempts = 5;

			/*
			 * job engine sometimes blocks messages with EAGAIN and/or EWOULDBLOCK
			 * (particularly when it's under stress) - this is ok.  In these
			 * circumstances, wait a sec, then try again (a few times)
			 */
			for (unsigned int num_attempts = 0;
				 num_attempts < max_messaging_attempts; ++num_attempts)
			{

				if (error != NULL)
				{
					if (isi_system_error_is_a(error, EAGAIN) ||
						isi_system_error_is_a(error, EWOULDBLOCK))
					{

						isi_error_free(error);
						error = NULL;

						sleep(1);
					}
					else
					{
						ON_ISI_ERROR_GOTO(out, error);
					}
				}

				(*msg_out) = user_recv_msg(socket_fd, &error);

				/* Success - get out */
				if (error == NULL)
					break;
			}
			ON_ISI_ERROR_GOTO(out, error);

			if ((*msg_out) == NULL)
			{
				error = isi_exception_error_new(
					"Failed to talk to job engine: job engine returned NULL");
				goto out;
			}

			if ((*msg_out)->header.type == COORD_TO_USER_ERROR)
			{
				coord_to_user_error_data *error_data =
					(coord_to_user_error_data *)(*msg_out)->data;

				// Transfer ownership of the error to the caller
				error = error_data->ie_msg;
				error_data->ie_msg = NULL;

				ON_ISI_ERROR_GOTO(out, error,
								  "error received from job engine");
			}

			if ((*msg_out)->header.type != COORD_TO_USER_SUCCESS)
			{
				error = isi_exception_error_new(
					"Unexpected header type while talking to job engine: %d",
					(*msg_out)->header.type);
				goto out;
			}

		out:

			isi_error_handle(error, error_out);
		}

		jd_jid_t
		start_job_engine_job(struct user_job_attrs *uattrs, enum job_type_id type,
							 struct isi_error **error_out)
		{
			ASSERT(uattrs != NULL);

			struct isi_error *error = NULL;
			jd_jid_t je_job_id = NULL_JID;
			struct jd_msg *msg = NULL;
			scoped_ptr_clean<struct jd_msg> msg_clean(msg, msg_free);

			uint64_t cluster_version = 0;

			int sock_fd = open_job_engine_comm_socket(&error);
			ON_ISI_ERROR_GOTO(out, error);

			if (ISI_UPGRADE_API_DISK_ENABLED(MOBY_SNAPDELETE))
			{
				cluster_version = je_get_min_node_version(&error);
				ON_ISI_ERROR_GOTO(out, error);

				user_to_coord_send_start_job_2(sock_fd, je_job_id, type,
											   cluster_version, uattrs, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
			else
			{
				user_to_coord_send_start_job(sock_fd, je_job_id, type, uattrs,
											 &error);
				ON_ISI_ERROR_GOTO(out, error);
			}

			receive_job_engine_message(sock_fd, &msg, &error);
			ON_ISI_ERROR_GOTO(out, error);

			ASSERT(msg != NULL);

			je_job_id = msg->header.jid;

		out:
			if (sock_fd >= 0)
				close(sock_fd);

			isi_error_handle(error, error_out);

			return je_job_id;
		}

		static enum job_type_id
		get_job_engine_type(jd_jid_t je_job_id, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			enum job_type_id type = NUM_JOB_TYPE_IDS;
			jobstat_job_head *jobs = NULL;

			gci_cfg jobs_closer(GCI_JOBENGINE_STATUS, "jobs");
			jobs = jobs_closer.root<jobstat_job_head>();

			if (je_job_id == NULL_JID)
			{
				error = isi_system_error_new(EINVAL,
											 "invalid job id -- NULL_JID");
				ON_ISI_ERROR_GOTO(out, error);
			}

			jobstat_job *job;
			SLIST_FOREACH(job, jobs, link)
			{
				if (je_job_id == job->id)
				{
					type = job->type;
					break;
				}
			}

			if (type == NUM_JOB_TYPE_IDS)
			{
				error = isi_system_error_new(ENOENT,
											 "no matching job engine job found -- job (%d)", je_job_id);
				ON_ISI_ERROR_GOTO(out, error);
			}
		out:
			isi_error_handle(error, error_out);

			return type;
		}

		bool
		job_engine_job_is_finished(jd_jid_t je_job_id,
								   struct isi_error **error_out)
		{
			ASSERT(je_job_id != NULL_JID);

			struct isi_error *error = NULL;
			bool finished = true;

			UFAIL_POINT_CODE(cpool__djob_cleanup__force_je_job_finished, {
	    finished = (bool)(RETURN_VALUE);
	    goto out; });

			// Check gconfig job-status.jobs
			try
			{
				jobstat_job_head *jobs = NULL;
				gci_cfg jobs_closer(GCI_JOBENGINE_STATUS, "jobs");
				jobs = jobs_closer.root<jobstat_job_head>();

				jobstat_job *job;
				SLIST_FOREACH(job, jobs, link)
				{
					if (je_job_id == job->id)
					{
						finished = false;
						break;
					}
				}
			}
			catch (const isi_exception &e)
			{
				// if ERROR from reading gconfig, set false to
				// avoid related daemon jobs being cleanuped
				finished = false;
				error = e.detach();
			}

		out:
			isi_error_handle(error, error_out);

			return finished;
		}

#define UFP_DJOB_CANCEL_FORCE_JE_ENOTCONN                                                 \
	UFAIL_POINT_CODE(cpool__djob_cancel__force_je_ENOTCONN, {                             \
		static int num_hits = 1;                                                          \
		if (num_hits++ <= RETURN_VALUE)                                                   \
		{                                                                                 \
			error = isi_system_error_new(ENOTCONN,                                        \
										 "simulated JobEngine job cancellation failure"); \
			goto out;                                                                     \
		}                                                                                 \
	});

#define UFP_DJOB_CANCEL_FORCE_JE_EINVAL                                               \
	UFAIL_POINT_CODE(cpool__djob_cancel__force_je_EINVAL, {                           \
		error = isi_system_error_new(EINVAL,                                          \
									 "simulated JobEngine job cancellation failure"); \
		goto out;                                                                     \
	});

		static void
		send_recv_job_engine_cancel_request(jd_jid_t je_job_id,
											struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			const bool system_invoked = false;
			job_type_id type;
			int sock_fd = -1;
			struct jd_msg *msg = NULL;
			scoped_ptr_clean<struct jd_msg> msg_clean(msg, msg_free);

			UFP_DJOB_CANCEL_FORCE_JE_ENOTCONN
			UFP_DJOB_CANCEL_FORCE_JE_EINVAL

			type = get_job_engine_type(je_job_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			sock_fd = open_job_engine_comm_socket(&error);
			ON_ISI_ERROR_GOTO(out, error);

			/* If we didn't get an error above, this better be non-negative */
			ASSERT(sock_fd >= 0);

			user_to_coord_send_cancel_job(sock_fd, je_job_id, type, system_invoked,
										  &error);
			ON_ISI_ERROR_GOTO(out, error);

			receive_job_engine_message(sock_fd, &msg, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			if (sock_fd >= 0)
				close(sock_fd);

			isi_error_handle(error, error_out);
		}

#define UFP_DJOB_CANCEL_RELOCK_DJOB_FAILURE                                \
	UFAIL_POINT_CODE(cpool__djob_cancel__djob_lock_failure, {              \
		if (error == NULL)                                                 \
		{                                                                  \
			djob_record_locked = true;                                     \
			unlock_daemon_job_record(djob_id, &error);                     \
			if (error == NULL)                                             \
			{                                                              \
				djob_record_locked = false;                                \
				error = isi_system_error_new(ENOTSOCK,                     \
											 "failpoint-generated error"); \
			}                                                              \
		}                                                                  \
	});

#define UFP_DJOB_CANCEL_REREAD_DJOB_FAILURE                            \
	UFAIL_POINT_CODE(cpool__djob_cancel__djob_read_failure, {          \
		if (error == NULL)                                             \
		{                                                              \
			error = isi_system_error_new(ENOTSOCK,                     \
										 "failpoint-generated error"); \
		}                                                              \
	});

#define UFP_DJOB_CANCEL_UNLOCK_DJOB_FAILURE                                \
	UFAIL_POINT_CODE(cpool__djob_cancel__djob_unlock_failure, {            \
		if (error == NULL)                                                 \
		{                                                                  \
			djob_record_locked = false;                                    \
			lock_daemon_job_record(djob_id, true, false, &error);          \
			if (error == NULL)                                             \
			{                                                              \
				djob_record_locked = true;                                 \
				error = isi_system_error_new(ENOTSOCK,                     \
											 "failpoint-generated error"); \
			}                                                              \
		}                                                                  \
	});

		/*
		 * Refresh the in-memory version of the daemon job record.  Note that this
		 * function unlocks the daemon job record before returning, so the record could
		 * be out of date before it is used by the caller.  (For the initial use of
		 * this function, this is not a problem.)
		 */
		static void
		reread_daemon_job_record(daemon_job_record *&djob_record,
								 bool &djob_record_locked, struct isi_error **error_out)
		{
			ASSERT(djob_record != NULL);
			ASSERT(!djob_record_locked);

			struct isi_error *error = NULL;
			djob_id_t djob_id = djob_record->get_daemon_job_id();

			/* true: block, false: shared lock */
			lock_daemon_job_record(djob_id, true, false, &error);
			UFP_DJOB_CANCEL_RELOCK_DJOB_FAILURE
			ON_ISI_ERROR_GOTO(out, error);
			djob_record_locked = true;

			delete djob_record;

			djob_record = get_daemon_job_record(djob_id, &error);
			UFP_DJOB_CANCEL_REREAD_DJOB_FAILURE
			ON_ISI_ERROR_GOTO(out, error);

			unlock_daemon_job_record(djob_id, &error);
			UFP_DJOB_CANCEL_UNLOCK_DJOB_FAILURE
			ON_ISI_ERROR_GOTO(out, error);
			djob_record_locked = false;

		out:
			isi_error_handle(error, error_out);
		}

/* Delay the job being finished until the RETURN-VALUE-th attempt. */
#define UFP_DJOB_CANCEL_JE_JOB_RUNNING                     \
	UFAIL_POINT_CODE(cpool__djob_cancel__je_job_running, { \
		static int num_hits = 1;                           \
		je_job_running = (num_hits++ < RETURN_VALUE);      \
	});

		static void
		cancel_job_engine_job_for_daemon_job_record(daemon_job_record *&djob_record,
													bool &djob_record_locked, struct isi_error **error_out)
		{
			ASSERT(djob_record != NULL);
			ASSERT(!djob_record_locked); // caller should have unlocked

			struct isi_error *error = NULL;
			jd_jid_t je_job_id = djob_record->get_job_engine_job_id();
			djob_id_t djob_id = djob_record->get_daemon_job_id();

			/*
			 * JobEngine might kill itself when overloaded which results in a
			 * return of ENOTCONN.  In this case, sleep to allow JE to restart,
			 * then retry the cancel request.
			 */
			for (int i = 1; i <= MAX_JE_JOB_CANCEL_ATTEMPTS; ++i)
			{
				send_recv_job_engine_cancel_request(je_job_id, &error);
				if (error == NULL || !isi_system_error_is_a(error, ENOTCONN) ||
					i == MAX_JE_JOB_CANCEL_ATTEMPTS)
					break;

				ilog(IL_DEBUG,
					 "failed to cancel job engine job %d "
					 "(referenced by cpool_job %d) "
					 "on attempt %d of %d: %#{}",
					 je_job_id, djob_id, i,
					 MAX_JE_JOB_CANCEL_ATTEMPTS,
					 isi_error_fmt(error));

				isi_error_free(error);
				error = NULL;

				sleep(1);
			}

			/*
			 * There is a window between the JE job finishing and updating the
			 * daemon job record to this effect, meaning we might be trying to
			 * cancel an already-finished job (which causes a return of EINVAL).
			 * In such a scenario, re-read the daemon job and check to see if the
			 * JE job hasn't finished before passing EINVAL to the caller.
			 *
			 * Note that since the mechanism that the JE job uses to update the
			 * daemon job record (i.e. the job_virtuals cleanup function) is called
			 * after the JE job is finished (i.e. removed from the run queue), it's
			 * possible that we could get EINVAL from the cancellation request and
			 * re-read the daemon job record before the JE job can update it, so
			 * re-read the daemon job record more than once before passing EINVAL
			 * to the caller.
			 */
			if (error != NULL && isi_system_error_is_a(error, EINVAL))
			{
				for (int i = 1; i <= MAX_JE_JOB_REREAD_ATTEMPTS; ++i)
				{
					struct isi_error *error2 = NULL;

					reread_daemon_job_record(djob_record,
											 djob_record_locked, &error2);
					if (error2 != NULL)
					{
						isi_error_add_context(error2,
											  "error re-reading daemon job record "
											  "to validate accompanying EINVAL "
											  "on attempt %d of %d",
											  i, MAX_JE_JOB_REREAD_ATTEMPTS);

						isi_multi_error_handle(error2, &error);
						error2 = NULL;

						break;
					}

					bool je_job_running =
						djob_record->job_engine_job_is_running(&error2);
					if (error2 != NULL)
					{
						isi_error_add_context(error2,
											  "error checking job engine job "
											  "running state on attempt %d of %d",
											  i, MAX_JE_JOB_REREAD_ATTEMPTS);

						isi_multi_error_handle(error2, &error);
						error2 = NULL;

						break;
					}

					UFP_DJOB_CANCEL_JE_JOB_RUNNING
					if (!je_job_running)
					{
						/*
						 * JE job is no longer running, so no need to
						 * pass the EINVAL up to the caller.
						 */
						isi_error_free(error);
						error = NULL;

						break;
					}

					sleep(1);
				}
			}
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		add_member(const isi_cloud::task *task, djob_id_t job_id, const char *name,
				   std::vector<struct hsbt_bulk_entry> &sbt_ops,
				   scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);

			struct isi_error *error = NULL;

			if (task->get_op_type()->is_multi_job())
				add_member__multi_job(task, job_id, name, sbt_ops,
									  objects_to_delete, &error);
			else
				add_member__single_job(task, job_id, name, sbt_ops,
									   objects_to_delete, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		add_member__multi_job(const isi_cloud::task *task, djob_id_t job_id,
							  const char *name, std::vector<struct hsbt_bulk_entry> &sbt_ops,
							  scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);
			ASSERT(task->get_op_type()->is_multi_job(),
				   "%{} is not a multi-job operation",
				   task_type_fmt(task->get_op_type()->get_type()));
			/* filename can be NULL */
			ASSERT(job_id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			const void *serialized_key;
			size_t serialized_key_len;
			daemon_job_member *djob_member = NULL;
			const void *member_serialization;
			size_t member_serialization_size;
			int member_sbt_fd = -1;
			struct hsbt_bulk_entry sbt_op;

			/*
			 * Check that the member we want to add doesn't exist -- which it
			 * shouldn't since this is a multi-job.
			 */
			djob_member = get_daemon_job_member(job_id, task->get_key(), &error);
			if (error == NULL)
			{
				error = isi_system_error_new(EEXIST,
											 "daemon job member already exists for task (%{})",
											 cpool_task_fmt(task));
				objects_to_delete.add_daemon_job_member(djob_member);
			}
			else if (isi_system_error_is_a(error, ENOENT))
			{
				isi_error_free(error);
				error = NULL;
			}
			ON_ISI_ERROR_GOTO(out, error,
							  "error attempting to retrieve daemon job member for task (%{})",
							  cpool_task_fmt(task));

			ASSERT(djob_member == NULL);
			djob_member = new daemon_job_member(task->get_key());
			ASSERT(djob_member != NULL);
			if (name != NULL)
				djob_member->set_name(name);
			objects_to_delete.add_daemon_job_member(djob_member);

			djob_member->get_serialization(member_serialization,
										   member_serialization_size);

			member_sbt_fd = open_member_sbt(job_id, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_member_sbt(member_sbt_fd);

			task->get_key()->get_serialized_key(serialized_key,
												serialized_key_len);

			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.is_hash_btree = task->get_op_type()->uses_hsbt();
			sbt_op.bulk_entry.fd = member_sbt_fd;
			sbt_op.bulk_entry.op_type = SBT_SYS_OP_ADD;
			sbt_op.bulk_entry.entry_buf =
				const_cast<void *>(member_serialization);
			sbt_op.bulk_entry.entry_size = member_serialization_size;

			if (task->get_op_type()->uses_hsbt())
			{
				sbt_op.key = serialized_key;
				sbt_op.key_len = serialized_key_len;
			}
			else
			{
				ASSERT(serialized_key_len == sizeof(btree_key_t),
					   "%zu != %zu", serialized_key_len, sizeof(btree_key_t));

				const btree_key_t *bt_key =
					static_cast<const btree_key_t *>(serialized_key);
				ASSERT(bt_key != NULL);

				sbt_op.bulk_entry.key = *bt_key;
			}

			sbt_ops.push_back(sbt_op);

		out:
			isi_error_handle(error, error_out);
		}

		void
		add_member__single_job(const isi_cloud::task *task, djob_id_t job_id,
							   const char *name, std::vector<struct hsbt_bulk_entry> &sbt_ops,
							   scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);
			ASSERT(!task->get_op_type()->is_multi_job(),
				   "%{} is not a single job operation",
				   task_type_fmt(task->get_op_type()->get_type()));
			/* filename can be NULL */
			ASSERT(job_id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			const void *serialized_key, *serialized_member, *old_serialized_member;
			size_t serialized_key_len, serialized_member_size,
				old_serialized_member_size;
			daemon_job_member *djob_member = NULL;
			int member_sbt_fd = -1;
			struct hsbt_bulk_entry sbt_op;
			memset(&sbt_op, 0, sizeof sbt_op);

			task->get_key()->get_serialized_key(serialized_key,
												serialized_key_len);

			/*
			 * Check if the member we want already exists -- if it does, we make
			 * sure it is moved to a non-terminal state before serializing
			 */
			djob_member = get_daemon_job_member(job_id, task, &error);
			if (error == NULL)
			{

				djob_member->save_serialization();

				djob_member->get_saved_serialization(old_serialized_member,
													 old_serialized_member_size);

				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_serialized_member);
				sbt_op.bulk_entry.old_entry_size = old_serialized_member_size;

				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
			}
			else
			{
				/* We didn't find an existing entry ... */
				if (!isi_system_error_is_a(error, ENOENT))
				{
					/* ... because something bad happened - bail out. */
					isi_error_add_context(error);
					goto out;
				}

				isi_error_free(error);
				error = NULL;

				/* ... because there isn't one, so create one now. */
				ASSERT(djob_member == NULL);
				djob_member = new daemon_job_member(task->get_key());
				ASSERT(djob_member != NULL);

				sbt_op.bulk_entry.op_type = SBT_SYS_OP_ADD;
			}

			objects_to_delete.add_daemon_job_member(djob_member);

			djob_member->set_final_state(DJOB_OJT_NONE);

			if (name != NULL)
				djob_member->set_name(name);

			djob_member->get_serialization(serialized_member,
										   serialized_member_size);

			member_sbt_fd = open_member_sbt(job_id, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_member_sbt(member_sbt_fd);

			sbt_op.is_hash_btree = task->get_op_type()->uses_hsbt();
			sbt_op.bulk_entry.fd = member_sbt_fd;
			sbt_op.bulk_entry.entry_buf = const_cast<void *>(serialized_member);
			sbt_op.bulk_entry.entry_size = serialized_member_size;

			if (task->get_op_type()->uses_hsbt())
			{
				sbt_op.key = serialized_key;
				sbt_op.key_len = serialized_key_len;
			}
			else
			{
				ASSERT(serialized_key_len == sizeof(btree_key_t),
					   "%zu != %zu", serialized_key_len, sizeof(btree_key_t));

				const btree_key_t *bt_key =
					static_cast<const btree_key_t *>(serialized_key);
				ASSERT(bt_key != NULL);

				sbt_op.bulk_entry.key = *bt_key;
			}

			sbt_ops.push_back(sbt_op);

		out:
			isi_error_handle(error, error_out);
		}

		void
		add_task(const isi_cloud::task *task, const char *name, djob_id_t job_id,
				 std::vector<struct hsbt_bulk_entry> &sbt_ops,
				 scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);
			/* filename can be NULL */
			ASSERT(job_id != DJOB_RID_INVALID);

			/*
			 * From the perspective of the daemon job manager, no more than three
			 * operations are required to add a task to a daemon job:
			 *  - modify the daemon job record for the specified job
			 *     - statistics
			 *     - last modification time
			 *     - state (if single-operation job)
			 *  - add/modify the daemon job member entry for this task (maybe)
			 *  - update daemon job config to indicate another running job (maybe)
			 */

			struct isi_error *error = NULL;
			daemon_job_record *djob_rec = NULL;
			int daemon_jobs_sbt_fd;
			const void *old_rec_serialization, *new_rec_serialization;
			size_t old_rec_serialization_size, new_rec_serialization_size;
			btree_key_t djob_record_key;
			struct hsbt_bulk_entry sbt_op;

			daemon_jobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/* true: block, true: exclusive lock */
			lock_daemon_job_record(job_id, true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_locked_daemon_job_record(job_id);

			djob_rec = get_daemon_job_record(job_id, &error);
			ON_ISI_ERROR_GOTO(out, error);
			objects_to_delete.add_daemon_job_record(djob_rec);

			/* Verify we can add the task to the job. */
			if (djob_rec->get_operation_type() != task->get_op_type())
			{
				error = isi_system_error_new(EINVAL,
											 "operation type mismatch -- job (%{}) task (%{})",
											 task_type_fmt(djob_rec->get_operation_type()->get_type()),
											 task_type_fmt(task->get_op_type()->get_type()));
				goto out;
			}

			if (djob_rec->get_state() == DJOB_OJT_CANCELLED)
			{
				error = isi_system_error_new(EINVAL,
											 "the task (%{}) cannot be added to daemon job %d "
											 "because the job is %{}",
											 cpool_task_fmt(task), djob_rec->get_daemon_job_id(),
											 djob_op_job_task_state_fmt(djob_rec->get_state()));
				goto out;
			}

			djob_rec->save_serialization();

			djob_rec->increment_num_members();
			if (task->get_location() == TASK_LOC_PENDING)
				djob_rec->increment_num_pending();
			else
				djob_rec->increment_num_inprogress();

			/*
			 * If the job is completed (with or without error), reset the state to
			 * running.  Why?  A single-job operation job may have finished its
			 * previous iteration, and needs to be "restarted".  A multi-job
			 * operation's job might not be driven by a JobEngine job (such as in
			 * the case of "isi filepool apply"), and with the right timing could
			 * be set to a completed state prematurely -- see Bug 129806.
			 */
			if (djob_rec->get_state() == DJOB_OJT_COMPLETED ||
				djob_rec->get_state() == DJOB_OJT_ERROR)
			{

				djob_rec->set_state(DJOB_OJT_RUNNING);

				/*
				 * Don't forget to re-increment num_active_jobs!  (Multi-jobs
				 * only, singleton jobs don't actively change this value)
				 */
				if (djob_rec->get_operation_type()->is_multi_job())
				{
					daemon_job_configuration *djob_config = NULL;
					const void *old_config_serialization;
					const void *new_config_serialization;
					size_t old_config_serialization_size;
					size_t new_config_serialization_size;
					struct hsbt_bulk_entry config_sbt_op;

					/* true: block, true: exclusive lock */
					lock_daemon_job_config(true, true, &error);
					ON_ISI_ERROR_GOTO(out, error);

					objects_to_delete.add_locked_daemon_job_configuration();

					djob_config = get_daemon_job_config(&error);
					ON_ISI_ERROR_GOTO(out, error);

					objects_to_delete.add_daemon_job_config(djob_config);

					djob_config->save_serialization();
					djob_config->increment_num_active_jobs();

					djob_config->get_saved_serialization(
						old_config_serialization,
						old_config_serialization_size);
					djob_config->get_serialization(new_config_serialization,
												   new_config_serialization_size);

					memset(&config_sbt_op, 0, sizeof config_sbt_op);

					config_sbt_op.is_hash_btree = false;
					config_sbt_op.bulk_entry.fd = daemon_jobs_sbt_fd;
					config_sbt_op.bulk_entry.key =
						daemon_job_record::get_key(
							daemon_job_configuration::get_daemon_job_id());
					config_sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
					config_sbt_op.bulk_entry.cm = BT_CM_BUF;
					config_sbt_op.bulk_entry.old_entry_buf =
						const_cast<void *>(old_config_serialization);
					config_sbt_op.bulk_entry.old_entry_size =
						old_config_serialization_size;
					config_sbt_op.bulk_entry.entry_buf =
						const_cast<void *>(new_config_serialization);
					config_sbt_op.bulk_entry.entry_size =
						new_config_serialization_size;

					sbt_ops.push_back(config_sbt_op);
				}
			}

			djob_rec->get_saved_serialization(old_rec_serialization,
											  old_rec_serialization_size);
			djob_rec->get_serialization(new_rec_serialization,
										new_rec_serialization_size);

			djob_record_key = djob_rec->get_key();

			/*
			 * Create the SBT operation to update the record and add it to the
			 * collection.
			 */
			memset(&sbt_op, 0, sizeof sbt_op);

			/* The daemon jobs SBT is not hashed. */
			sbt_op.is_hash_btree = false;
			sbt_op.bulk_entry.fd = daemon_jobs_sbt_fd;
			sbt_op.bulk_entry.key = djob_record_key;
			sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
			sbt_op.bulk_entry.cm = BT_CM_BUF;
			sbt_op.bulk_entry.old_entry_buf =
				const_cast<void *>(old_rec_serialization);
			sbt_op.bulk_entry.old_entry_size = old_rec_serialization_size;
			sbt_op.bulk_entry.entry_buf =
				const_cast<void *>(new_rec_serialization);
			sbt_op.bulk_entry.entry_size = new_rec_serialization_size;

			sbt_ops.push_back(sbt_op);

			/*
			 * Create the daemon job member for this daemon job ID / task
			 * combination.
			 */
			add_member(task, job_id, name, sbt_ops, objects_to_delete, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			/*
			 * Unlocking records, deallocating objects on the heap, and other
			 * cleanup steps cannot occur until the SBT bulk operation has occurred
			 * and will be handled by the scoped_object_list.
			 */

			isi_error_handle(error, error_out);
		}

		void
		start_task(const isi_cloud::task *task,
				   std::vector<struct hsbt_bulk_entry> &sbt_ops,
				   scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);

			struct isi_error *error = NULL;
			int daemon_job_record_sbt_fd;
			std::set<djob_id_t> owning_job_ids;
			std::set<djob_id_t>::const_iterator job_ids_iter;
			daemon_job_record *daemon_job_rec = NULL;
			const void *old_serialization, *new_serialization;
			size_t old_serialization_size, new_serialization_size;
			btree_key_t daemon_job_rec_key;
			struct hsbt_bulk_entry sbt_op;

			daemon_job_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * Update the stats for each daemon job record, locking each before
			 * editing.
			 */
			owning_job_ids = task->get_daemon_jobs();
			job_ids_iter = owning_job_ids.begin();
			for (; job_ids_iter != owning_job_ids.end(); ++job_ids_iter)
			{
				/* true: block, true: exclusive lock */
				lock_daemon_job_record(*job_ids_iter, true, true, &error);
				ON_ISI_ERROR_GOTO(out, error);
				objects_to_delete.add_locked_daemon_job_record(*job_ids_iter);

				daemon_job_rec = get_daemon_job_record(*job_ids_iter, &error);
				ON_ISI_ERROR_GOTO(out, error);
				objects_to_delete.add_daemon_job_record(daemon_job_rec);

				daemon_job_rec->save_serialization();

				daemon_job_rec->decrement_num_pending();
				daemon_job_rec->increment_num_inprogress();

				daemon_job_rec->get_saved_serialization(old_serialization,
														old_serialization_size);
				daemon_job_rec->get_serialization(new_serialization,
												  new_serialization_size);

				daemon_job_rec_key = daemon_job_rec->get_key();

				/* Create the SBT operation and add it to the collection. */
				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.is_hash_btree = false;
				sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
				sbt_op.bulk_entry.key = daemon_job_rec_key;
				sbt_op.bulk_entry.cm = BT_CM_BUF;
				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_serialization);
				sbt_op.bulk_entry.old_entry_size = old_serialization_size;
				sbt_op.bulk_entry.entry_buf =
					const_cast<void *>(new_serialization);
				sbt_op.bulk_entry.entry_size = new_serialization_size;

				sbt_ops.push_back(sbt_op);
			}

		out:
			/*
			 * Unlocking records, deallocating objects on the heap, and other
			 * cleanup steps cannot occur until the SBT bulk operation has occurred
			 * and will be handled by the scoped_object_list.
			 */

			isi_error_handle(error, error_out);
		}

		djob_op_job_task_state
		completion_reason_to_final_state(task_process_completion_reason reason)
		{
			switch (reason)
			{
			case CPOOL_TASK_SUCCESS:
				return DJOB_OJT_COMPLETED;
			case CPOOL_TASK_CANCELLED:
				return DJOB_OJT_CANCELLED;
			case CPOOL_TASK_NON_RETRYABLE_ERROR:
				return DJOB_OJT_ERROR;
			case CPOOL_TASK_PAUSED:
			case CPOOL_TASK_RETRYABLE_ERROR:
				/*
				 * Although there are other task states around (e.g.,
				 * DJOB_OJT_RUNNING), they are not 'final'.  That being the
				 * case, we should return DJOB_OJT_NONE meaning 'we are not in
				 * a final state'
				 */
				return DJOB_OJT_NONE;
			case CPOOL_TASK_STOPPED:
				/*
				 * If a task is stopped, this is no change to the daemon job
				 * bookkeeping, so this should never be reached.  This case is
				 * included to appease the compiler without using the default
				 * block.
				 */
				ASSERT(false);
			}
		}

		void
		set_member_final_state(const isi_cloud::task *task,
							   djob_op_job_task_state final_state, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;
			std::set<djob_id_t>::const_iterator jobs_iter;
			scoped_object_list objects_to_delete;
			const unsigned int num_bulk_entries = 4;
			unsigned int bulk_entry_index = 0;
			struct hsbt_bulk_entry bulk_op[num_bulk_entries];
			const void *serialized_key;
			size_t serialized_key_len;
			const std::set<djob_id_t> &owning_job_ids = task->get_daemon_jobs();

			memset(bulk_op, 0, num_bulk_entries * sizeof(hsbt_bulk_entry));

			task->get_key()->get_serialized_key(serialized_key, serialized_key_len);

			for (unsigned int i = 0; i < num_bulk_entries; i++)
			{
				/*
				 * All of the sbt operations should be very similar, prepopulate
				 * what we can up front
				 */
				bulk_op[i].is_hash_btree = task->get_op_type()->uses_hsbt();
				bulk_op[i].bulk_entry.op_type = SBT_SYS_OP_MOD;

				if (task->get_op_type()->uses_hsbt())
				{
					bulk_op[i].key = serialized_key;
					bulk_op[i].key_len = serialized_key_len;
				}
				else
				{
					ASSERT(serialized_key_len == sizeof(btree_key_t),
						   "%zu != %zu", serialized_key_len,
						   sizeof(btree_key_t));

					const btree_key_t *bt_key =
						static_cast<const btree_key_t *>(serialized_key);
					ASSERT(bt_key != NULL);

					bulk_op[i].bulk_entry.key = *bt_key;
				}
			}

			/*
			 * For each owning job, populate an entry in the bulk_op.  As the bulk
			 * ops are filled, modify the SBT and empty the bulk ops.
			 */
			for (jobs_iter = owning_job_ids.begin();
				 jobs_iter != owning_job_ids.end();
				 /* Note - jobs_iter is incremented below */)
			{

				const void *member_serial = NULL, *old_serial = NULL;
				size_t member_serial_size, old_serial_size;

				int sbt_fd = -1;
				daemon_job_member *member = get_daemon_job_member(*jobs_iter,
																  task->get_key(), &error);
				ON_ISI_ERROR_GOTO(out, error,
								  "error retrieving daemon job member for task (%{})",
								  cpool_task_fmt(task));

				objects_to_delete.add_daemon_job_member(member);

				/* No change made, don't bother updating */
				if (member->get_final_state() == final_state)
				{
					++jobs_iter;
					continue;
				}

				sbt_fd = open_member_sbt(*jobs_iter, false, &error);
				ON_ISI_ERROR_GOTO(out, error);

				objects_to_delete.add_daemon_job_member_sbt(sbt_fd);

				member->get_saved_serialization(old_serial, old_serial_size);

				member->set_final_state(final_state);

				member->get_serialization(member_serial, member_serial_size);

				bulk_op[bulk_entry_index].bulk_entry.old_entry_buf =
					const_cast<void *>(old_serial);
				bulk_op[bulk_entry_index].bulk_entry.old_entry_size =
					old_serial_size;

				bulk_op[bulk_entry_index].bulk_entry.fd = sbt_fd;
				bulk_op[bulk_entry_index].bulk_entry.entry_buf =
					const_cast<void *>(member_serial);
				bulk_op[bulk_entry_index].bulk_entry.entry_size =
					member_serial_size;

				++jobs_iter;
				++bulk_entry_index;

				if (bulk_entry_index == num_bulk_entries ||
					jobs_iter == owning_job_ids.end())
				{

					hsbt_bulk_op(bulk_entry_index, bulk_op, &error);
					ON_ISI_ERROR_GOTO(out, error);

					for (unsigned int i = 0; i < bulk_entry_index; i++)
					{
						bulk_op[i].bulk_entry.fd = -1;
						bulk_op[i].bulk_entry.entry_buf = NULL;
						bulk_op[i].bulk_entry.entry_size = -1;
						bulk_op[i].bulk_entry.old_entry_buf = NULL;
						bulk_op[i].bulk_entry.old_entry_size = -1;
					}

					bulk_entry_index = 0;
				}
			}
		out:
			isi_error_handle(error, error_out);
		}

		void
		finish_task(const isi_cloud::task *task,
					task_process_completion_reason reason,
					std::vector<struct hsbt_bulk_entry> &sbt_ops,
					scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(task != NULL);
			/*
			 * If a task is stopped, there is no change to the daemon job
			 * bookkeeping, so this function shouldn't be called.
			 */
			ASSERT(reason != CPOOL_TASK_STOPPED);

			struct isi_error *error = NULL;
			int daemon_job_record_sbt_fd;
			std::set<djob_id_t> owning_job_ids;
			std::set<djob_id_t>::const_iterator job_ids_iter;
			daemon_job_record *daemon_job_rec = NULL;
			const void *old_rec_serialization, *new_rec_serialization;
			size_t old_rec_serialization_size, new_rec_serialization_size;
			daemon_job_configuration *daemon_job_cfg = NULL;
			btree_key_t daemon_job_rec_key;
			struct hsbt_bulk_entry sbt_op;
			djob_op_job_task_state final_state = DJOB_OJT_NONE;

			/*
			 * Lock each daemon job record that owns the finishing task.  We
			 * potentially need to lock the daemon job configuration, and in order
			 * to prevent deadlocks, we have to lock all of the daemon job records
			 * first.
			 */
			owning_job_ids = task->get_daemon_jobs();
			job_ids_iter = owning_job_ids.begin();
			for (; job_ids_iter != owning_job_ids.end(); ++job_ids_iter)
			{
				/* true: block, true: exclusive lock */
				lock_daemon_job_record(*job_ids_iter, true, true, &error);
				ON_ISI_ERROR_GOTO(out, error);
				objects_to_delete.add_locked_daemon_job_record(*job_ids_iter);
			}

			/*
			 * Now update the stats for each daemon job record and the daemon job
			 * configuration if necessary.
			 */
			daemon_job_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			owning_job_ids = task->get_daemon_jobs();
			job_ids_iter = owning_job_ids.begin();
			for (; job_ids_iter != owning_job_ids.end(); ++job_ids_iter)
			{
				daemon_job_rec = get_daemon_job_record(*job_ids_iter, &error);
				ON_ISI_ERROR_GOTO(out, error);
				objects_to_delete.add_daemon_job_record(daemon_job_rec);

				daemon_job_rec->save_serialization();

				daemon_job_rec->decrement_num_inprogress();
				switch (reason)
				{
				case CPOOL_TASK_SUCCESS:
					daemon_job_rec->increment_num_succeeded();

					check_job_complete(daemon_job_cfg, daemon_job_rec,
									   objects_to_delete, &error);
					ON_ISI_ERROR_GOTO(out, error);

					break;
				case CPOOL_TASK_PAUSED:
				case CPOOL_TASK_RETRYABLE_ERROR:
					daemon_job_rec->increment_num_pending();

					break;
				case CPOOL_TASK_CANCELLED:
					daemon_job_rec->increment_num_cancelled();

					check_job_complete(daemon_job_cfg, daemon_job_rec,
									   objects_to_delete, &error);
					ON_ISI_ERROR_GOTO(out, error);

					break;
				case CPOOL_TASK_NON_RETRYABLE_ERROR:
					daemon_job_rec->increment_num_failed();

					check_job_complete(daemon_job_cfg, daemon_job_rec,
									   objects_to_delete, &error);
					ON_ISI_ERROR_GOTO(out, error);

					break;
				case CPOOL_TASK_STOPPED:
					/*
					 * You should never get here as the assertion at the
					 * top of the function should have tripped.  This case
					 * is here to appease the compiler without using the
					 * default case.
					 */
					ASSERT(false);
					break;
				}

				daemon_job_rec->get_saved_serialization(old_rec_serialization,
														old_rec_serialization_size);
				daemon_job_rec->get_serialization(new_rec_serialization,
												  new_rec_serialization_size);

				daemon_job_rec_key = daemon_job_rec->get_key();

				/* Create the SBT operation and add it to the collection. */
				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.is_hash_btree = false;
				sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
				sbt_op.bulk_entry.key = daemon_job_rec_key;
				sbt_op.bulk_entry.cm = BT_CM_BUF;
				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_rec_serialization);
				sbt_op.bulk_entry.old_entry_size = old_rec_serialization_size;
				sbt_op.bulk_entry.entry_buf =
					const_cast<void *>(new_rec_serialization);
				sbt_op.bulk_entry.entry_size = new_rec_serialization_size;

				sbt_ops.push_back(sbt_op);
			}

			final_state = completion_reason_to_final_state(reason);

			/*
			 * Update final state of members, but only if it's changing
			 */
			if (final_state != DJOB_OJT_NONE)
			{
				set_member_final_state(task, final_state, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}

			/*
			 * Add an operation to update the daemon job configuration if
			 * necessary.
			 */
			if (daemon_job_cfg != NULL)
			{
				const void *old_cfg_serialization, *new_cfg_serialization;
				size_t old_cfg_serialization_size, new_cfg_serialization_size;

				daemon_job_cfg->get_saved_serialization(
					old_cfg_serialization, old_cfg_serialization_size);
				daemon_job_cfg->get_serialization(new_cfg_serialization,
												  new_cfg_serialization_size);

				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.is_hash_btree = false;
				sbt_op.bulk_entry.fd = daemon_job_record_sbt_fd;
				sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
				sbt_op.bulk_entry.key =
					daemon_job_record::get_key(
						daemon_job_configuration::get_daemon_job_id());
				sbt_op.bulk_entry.cm = BT_CM_BUF;
				sbt_op.bulk_entry.old_entry_buf =
					const_cast<void *>(old_cfg_serialization);
				sbt_op.bulk_entry.old_entry_size = old_cfg_serialization_size;
				sbt_op.bulk_entry.entry_buf =
					const_cast<void *>(new_cfg_serialization);
				sbt_op.bulk_entry.entry_size = new_cfg_serialization_size;

				sbt_ops.push_back(sbt_op);
			}

		out:
			/*
			 * Unlocking records, deallocating objects on the heap, and other
			 * cleanup steps cannot occur until the SBT bulk operation has occurred
			 * and will be handled by the scoped_object_list.
			 */

			isi_error_handle(error, error_out);
		}

		void
		check_job_complete(daemon_job_configuration *&djob_config,
						   daemon_job_record *djob_record, scoped_object_list &objects_to_delete,
						   struct isi_error **error_out)
		{
			// djob_config can be NULL
			ASSERT(djob_record != NULL);

			struct isi_error *error = NULL;

			/*
			 * We mark a job as cancelled at the time it is cancelled by the user
			 * so tasks are most likely still in the pending SBT.  As they are
			 * "drained" (i.e. started, not processed, then finished), this
			 * function is called, which, for the last task, will set the state of
			 * the job to either completed or error (based on the number of failed
			 * tasks).  We don't want this -- a cancelled job shouldn't change
			 * state, and we've already decremented the number of active jobs when
			 * the job was cancelled -- which is why we check for this condition
			 * here.
			 * BTW - only multi-jobs should enter a final state.  Singleton jobs
			 * must keep on trucking
			 */
			if (djob_record->get_operation_type()->is_multi_job() &&
				djob_record->get_state() != DJOB_OJT_CANCELLED &&
				djob_record->is_finished())
			{

				if (djob_record->get_stats()->get_num_failed() == 0)
					djob_record->set_state(DJOB_OJT_COMPLETED);
				else
					djob_record->set_state(DJOB_OJT_ERROR);

				if (djob_config == NULL)
				{
					/* true: block, true: exclusive lock */
					lock_daemon_job_config(true, true, &error);
					ON_ISI_ERROR_GOTO(out, error);
					objects_to_delete.add_locked_daemon_job_configuration();

					djob_config = get_daemon_job_config(&error);
					ON_ISI_ERROR_GOTO(out, error);
					objects_to_delete.add_daemon_job_config(
						djob_config);

					djob_config->save_serialization();
				}

				djob_config->decrement_num_active_jobs();
			}

		out:
			isi_error_handle(error, error_out);
		}

		void
		pr_operation(const cpool_operation_type *op_type,
					 djob_op_job_task_state new_state, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);
			ASSERT(new_state == DJOB_OJT_PAUSED || new_state == DJOB_OJT_RUNNING,
				   "invalid state: %{}", djob_op_job_task_state_fmt(new_state));

			struct isi_error *error = NULL;
			bool config_locked = false;
			daemon_job_configuration *djob_config = NULL;
			const void *old_serialization, *new_serialization;
			size_t old_serialization_size, new_serialization_size;
			int djobs_sbt_fd;
			btree_key_t djob_config_key;

			/* true: block, true: exclusive lock */
			lock_daemon_job_config(true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			config_locked = true;

			djob_config = get_daemon_job_config(&error);
			ON_ISI_ERROR_GOTO(out, error);

			djob_config->save_serialization();

			if (djob_config->get_operation_state(op_type) == new_state)
			{
				// error = isi_system_error_new(EALREADY,
				//     "%{} is already %{}",
				//     task_type_fmt(op_type->get_type()),
				//     djob_op_job_task_state_fmt(new_state));
				goto out;
			}

			djob_config->set_operation_state(op_type, new_state);

			djob_config->get_saved_serialization(old_serialization,
												 old_serialization_size);
			djob_config->get_serialization(new_serialization,
										   new_serialization_size);

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			djob_config_key = daemon_job_record::get_key(
				daemon_job_configuration::get_daemon_job_id());

			if (ifs_sbt_cond_mod_entry(djobs_sbt_fd, &djob_config_key,
									   new_serialization, new_serialization_size, NULL, BT_CM_BUF,
									   old_serialization, old_serialization_size, NULL) != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to modify daemon job configuration");
				goto out;
			}

		out:
			if (config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			if (djob_config != NULL)
				delete djob_config;

			isi_error_handle(error, error_out);
		}

		void
		pause_operation(const cpool_operation_type *op_type,
						struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			pr_operation(op_type, DJOB_OJT_PAUSED, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		resume_operation(const cpool_operation_type *op_type,
						 struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			pr_operation(op_type, DJOB_OJT_RUNNING, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		prc_job(djob_id_t daemon_job_id, djob_op_job_task_state new_state,
				struct isi_error **error_out)
		{
			ASSERT(daemon_job_id != DJOB_RID_INVALID);
			ASSERT(new_state == DJOB_OJT_PAUSED || new_state == DJOB_OJT_RUNNING ||
					   new_state == DJOB_OJT_CANCELLED,
				   "invalid state: %{}",
				   djob_op_job_task_state_fmt(new_state));

			struct isi_error *error = NULL;
			bool record_locked = false, config_locked = false;
			daemon_job_record *djob_record = NULL;
			daemon_job_configuration *djob_config = NULL;
			const void *old_rec_serialization, *new_rec_serialization;
			size_t old_rec_serialization_size, new_rec_serialization_size;
			const void *old_cfg_serialization, *new_cfg_serialization;
			size_t old_cfg_serialization_size, new_cfg_serialization_size;
			int djobs_sbt_fd;
			struct sbt_bulk_entry sbt_op;
			std::vector<struct sbt_bulk_entry> sbt_ops;
			bool is_je_job_running = false;

			/* true: block, true: exclusive lock */
			lock_daemon_job_record(daemon_job_id, true, true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = true;

			djob_record = get_daemon_job_record(daemon_job_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/* Single jobs can't be cancelled. */
			if (new_state == DJOB_OJT_CANCELLED &&
				!djob_record->get_operation_type()->is_multi_job())
			{
				error = isi_system_error_new(EINVAL,
											 "cancelling the %{} job is not allowed",
											 task_type_fmt(
												 djob_record->get_operation_type()->get_type()));
				goto out;
			}

			if (djob_record->get_state() == new_state)
			{
				error = isi_system_error_new(EALREADY,
											 "daemon job %d is already %{}", daemon_job_id,
											 djob_op_job_task_state_fmt(new_state));
				goto out;
			}

			/* Cancelled or completed jobs can't be modified. */
			if (djob_record->get_state() == DJOB_OJT_CANCELLED ||
				djob_record->get_state() == DJOB_OJT_ERROR ||
				djob_record->get_state() == DJOB_OJT_COMPLETED)
			{
				error = isi_system_error_new(EINVAL,
											 "daemon job %d is %{} and cannot be modified",
											 daemon_job_id,
											 djob_op_job_task_state_fmt(djob_record->get_state()));
				goto out;
			}

			djob_record->save_serialization();
			djob_record->set_state(new_state);

			djob_record->get_saved_serialization(old_rec_serialization,
												 old_rec_serialization_size);
			djob_record->get_serialization(new_rec_serialization,
										   new_rec_serialization_size);

			djobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * If a job is being cancelled, we also need to modify the daemon job
			 * configuration, so create an SBT operation for modifying the daemon
			 * job record, and then optionally one for modifying the daemon job
			 * configuration.
			 */
			memset(&sbt_op, 0, sizeof sbt_op);

			sbt_op.fd = djobs_sbt_fd;
			sbt_op.op_type = SBT_SYS_OP_MOD;
			sbt_op.key = djob_record->get_key();
			sbt_op.entry_buf = const_cast<void *>(new_rec_serialization);
			sbt_op.entry_size = new_rec_serialization_size;
			sbt_op.cm = BT_CM_BUF;
			sbt_op.old_entry_buf = const_cast<void *>(old_rec_serialization);
			sbt_op.old_entry_size = old_rec_serialization_size;

			sbt_ops.push_back(sbt_op);

			if (new_state == DJOB_OJT_CANCELLED)
			{
				/* true: block, true: exclusive lock */
				lock_daemon_job_config(true, true, &error);
				ON_ISI_ERROR_GOTO(out, error);
				config_locked = true;

				djob_config = get_daemon_job_config(&error);
				ON_ISI_ERROR_GOTO(out, error);

				djob_config->save_serialization();

				djob_config->decrement_num_active_jobs();

				djob_config->get_saved_serialization(old_cfg_serialization,
													 old_cfg_serialization_size);
				djob_config->get_serialization(new_cfg_serialization,
											   new_cfg_serialization_size);

				memset(&sbt_op, 0, sizeof sbt_op);

				sbt_op.fd = djobs_sbt_fd;
				sbt_op.op_type = SBT_SYS_OP_MOD;
				sbt_op.key =
					daemon_job_record::get_key(
						daemon_job_configuration::get_daemon_job_id());
				sbt_op.entry_buf = const_cast<void *>(new_cfg_serialization);
				sbt_op.entry_size = new_cfg_serialization_size;
				sbt_op.cm = BT_CM_BUF;
				sbt_op.old_entry_buf = const_cast<void *>(old_cfg_serialization);
				sbt_op.old_entry_size = old_cfg_serialization_size;

				sbt_ops.push_back(sbt_op);
			}

			if (ifs_sbt_bulk_op(sbt_ops.size(), sbt_ops.data()) != 0)
			{
				error = isi_system_error_new(errno, "ifs_sbt_bulk_op failed");
				goto out;
			}

			is_je_job_running = djob_record->job_engine_job_is_running(&error);
			ON_ISI_ERROR_GOTO(out, error);

			/* If this is a cancellation, we need to cancel job engine */
			if (is_je_job_running && new_state == DJOB_OJT_CANCELLED)
			{

				ASSERT(record_locked, "Expected djob record to be locked");

				/*
				 * job engine cancellation may take locks in job_d process, so
				 * we need to make sure they're released from here before
				 * proceeding
				 */
				unlock_daemon_job_record(daemon_job_id, &error);
				ON_ISI_ERROR_GOTO(out, error);

				record_locked = false;

				if (config_locked)
				{
					unlock_daemon_job_config(&error);
					ON_ISI_ERROR_GOTO(out, error);
				}

				config_locked = false;

				cancel_job_engine_job_for_daemon_job_record(djob_record,
															record_locked, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			if (record_locked)
				unlock_daemon_job_record(daemon_job_id,
										 isi_error_suppress(IL_NOTICE));

			if (config_locked)
				unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));

			if (djob_record != NULL)
				delete djob_record;

			if (djob_config != NULL)
				delete djob_config;

			isi_error_handle(error, error_out);
		}

		void
		pause_job(djob_id_t daemon_job_id, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			prc_job(daemon_job_id, DJOB_OJT_PAUSED, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		resume_job(djob_id_t daemon_job_id, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			prc_job(daemon_job_id, DJOB_OJT_RUNNING, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

		void
		cancel_job(djob_id_t daemon_job_id, struct isi_error **error_out)
		{
			struct isi_error *error = NULL;

			prc_job(daemon_job_id, DJOB_OJT_CANCELLED, &error);
			ON_ISI_ERROR_GOTO(out, error);

		out:
			isi_error_handle(error, error_out);
		}

#ifdef PRC_TASK
		void
		pause_task(const isi_cloud::task *task,
				   std::vector<struct hsbt_bulk_entry> &sbt_ops,
				   scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(false, "not yet implemented");
		}

		void
		resume_task(const isi_cloud::task *task,
					std::vector<struct hsbt_bulk_entry> &sbt_ops,
					scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(false, "not yet implemented");
		}

		void cancel_task(const isi_cloud::task *task,
						 std::vector<struct hsbt_bulk_entry> &sbt_ops,
						 scoped_object_list &objects_to_delete, struct isi_error **error_out)
		{
			ASSERT(false, "not yet implemented");
		}
#endif // PRC_TASK

		void
		remove_job(const daemon_job_record *djob_record, struct isi_error **error_out)
		{
			ASSERT(djob_record != NULL);

			struct isi_error *error = NULL;

			const cpool_operation_type *op_type = NULL;
			btree_key_t djob_record_key;
			const void *rec_serialization;
			size_t rec_serialization_size;
			char *djob_member_sbt_name = NULL;
			int djob_record_sbt_fd;

			op_type = djob_record->get_operation_type();
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			if (!op_type->is_multi_job())
			{
				error = isi_system_error_new(EINVAL,
											 "cannot remove daemon job %d, it is the only job for %{}",
											 djob_record->get_daemon_job_id(),
											 task_type_fmt(op_type->get_type()));
				goto out;
			}

			if (!djob_record->is_finished())
			{
				error = isi_system_error_new(EINVAL,
											 "daemon job %d is not yet finished "
											 "(JE job state: %{} stats: %{})",
											 djob_record->get_daemon_job_id(),
											 enum_jobstat_state_fmt(
												 djob_record->get_job_engine_state()),
											 cpool_djob_stats_fmt(djob_record->get_stats()));
				goto out;
			}

			djob_record_key = djob_record->get_key();
			djob_record->get_serialization(rec_serialization,
										   rec_serialization_size);

			djob_member_sbt_name = get_djob_file_list_sbt_name(
				djob_record->get_daemon_job_id());
			delete_sbt(djob_member_sbt_name, &error); ////////delete_sbt的定义在cpool_d_common.cpp/h中
			ON_ISI_ERROR_GOTO(out, error);

			djob_record_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (ifs_sbt_cond_remove_entry(djob_record_sbt_fd, &djob_record_key,
										  BT_CM_BUF, rec_serialization, rec_serialization_size, NULL) != 0)
			{
				error = isi_system_error_new(errno,
											 "failed to remove record for daemon job %d",
											 djob_record->get_daemon_job_id());
				goto out;
			}

		out:
			free(djob_member_sbt_name);

			isi_error_handle(error, error_out);
		}
		////Convert a daemon job id to a key used to store the daemon job in the SBT
		void daemon_job_key_to_bt_key(uint32_t daemon_job_key, btree_key_t *bt_key)
		{
			ASSERT(bt_key != NULL);
			bt_key->keys[0] = daemon_job_key;
			bt_key->keys[1] = (uint64_t)-1;
		}
		///////jjz test   jobs_cleanup
		void remove_djobex(djob_id_t job_id, void *djob_rec, const size_t rec_size, struct isi_error **error_out)
		{
			ilog(IL_NOTICE, "%s called djob_id:%d", __func__, job_id);
			struct isi_error *error = NULL;
			int result;
			int daemon_jobs_sbt_fd = -1;

			btree_key_t bt_key;
			//////djob_id -> bt_key
			daemon_job_key_to_bt_key(job_id, &bt_key);
			struct btree_flags flags = {};

			char *files_list_name = NULL;
			files_list_name = get_djob_file_list_sbt_name(job_id);

			daemon_jobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			delete_sbt(files_list_name, &error);
			if (error)
			{
				if (isi_system_error_is_a(error, ENOENT))
				{
					ilog(IL_DEBUG, "Unable to delete Deamon"
								   " job(Id: %u) files SBT: %#{}",
						 job_id,
						 isi_error_fmt(error));

					isi_error_free(error);
					error = NULL;
				}
				else
					goto out;
			}

			result = ifs_sbt_cond_remove_entry(daemon_jobs_sbt_fd, &bt_key,
											   BT_CM_BUF, djob_rec, rec_size, &flags);

			if (result == 0)
			{
				ilog(IL_NOTICE, "Daemon job (id: %u) removed successfully",
					 job_id);
				goto out;
			}
			error = isi_system_error_new(errno, "Unable to remove Daemon"
												" job record from SBT(Id: %u)",
										 job_id);
		out:
			if (files_list_name)
				free(files_list_name);

			if (error)
				isi_error_handle(error, error_out);
		}

		static bool
		ignore_pre_RT_jobs(const struct sbt_entry *sbt_ent, void *ctx,
						   struct isi_error **error_out)
		{
			/*
			 * Since we're ignoring them, return true if this entry is NOT a
			 * pre-RT job.  See Bug 249170.
			 */
			return (sbt_ent->key.keys[1] != (uint64_t)(-1));
		}

		/**
		 * This function controls how the Job Manager will determine which records
		 * need to be removed. The function compares the provided job record against
		 * the criteria and returns true if the requirements are met.
		 *
		 * @param   const daemon_job_record*	Job candidate
		 * @param   const void*			Requirements
		 * @returns bool			True if requirements are met
		 */
		bool
		cleanup_filter(const daemon_job_record *candidate, const void *criteria)
		{
			/**
			 * The criteria for cleanup is that the job to have completed before
			 * a specified date/time. The criteria parameter is this date.
			 */
			time_t older_than = *((time_t *)criteria);
			// ilog(IL_NOTICE, "%s called completion_time:%ld, older_than:%ld", __func__, candidate->get_completion_time(), older_than);
			//  return (candidate->get_operation_type()->is_multi_job());
			return (
				candidate->get_operation_type()->is_multi_job() && candidate->is_finished() && candidate->get_completion_time() != DJOB_RECORD_NOT_YET_COMPLETED && candidate->get_completion_time() < older_than);
		}
		void cleanup_jobs(uint32_t max_num, time_t older_than, isi_error **error_out)
		{
			struct isi_error *error = NULL;
			int daemon_jobs_sbt_fd = -1;

			struct sbt_entry *ent = NULL;
			char *entry_buf = NULL;
			struct get_sbt_entry_ctx *ctx = NULL;
			djob_id_t job_id;
			daemon_job_record djob_rd;
			/////daemon job id 0 is reserved, so start from 1
			btree_key_t key = {{1, 0}};
			daemon_jobs_sbt_fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			while (1)
			{
				if (entry_buf)
				{
					free(entry_buf);
					entry_buf = NULL;
				}
				ent = get_sbt_entry(daemon_jobs_sbt_fd, &key, &entry_buf, &ctx, &error);
				if (ent == NULL)
				{
					ilog(IL_NOTICE, "%s called get_sbt_entry ent == NULL", __func__);
					break;
				}
				if (ent->key.keys[0] == 0 && ent->key.keys[1] == 0)
				{
					continue;
				}
				// djob_rd.set_serialized_job(ent->buf, ent->size_out); ///// 7.2.1 version
				djob_rd.set_serialization(ent->buf, ent->size_out, &error);
				job_id = djob_rd.get_daemon_job_id();
				ilog(IL_NOTICE, "%s called djob_id:%d finished:%d", __func__, job_id, djob_rd.is_finished());
				if (djob_rd.get_completion_time() < older_than && djob_rd.get_completion_time() != DJOB_RECORD_NOT_YET_COMPLETED)
				{
					remove_djobex(job_id, ent->buf, ent->size_out, &error);
					if (error)
					{
						ilog(IL_NOTICE, "Error cleaning up daemon"
										" job (Id: %u): %#{}",
							 job_id, isi_error_fmt(error));
						isi_error_free(error);
						error = NULL;
					}
				}
			out:
				if (entry_buf != NULL)
				{
					free(entry_buf);
				}
				get_sbt_entry_ctx_destroy(&ctx);
				isi_error_handle(error, error_out);
			}
		}
		uint32_t
		cleanup_old_jobs(
			uint32_t max_num, time_t older_than, struct isi_error **error_out)
		{
			/**
			 * This function cleans up old jobs using the daemon_job_reader as
			 * an iterator.
			 */
			ASSERT(max_num > 0, "max_num: %d", max_num);

			const int INVALID_FD = -1;

			struct isi_error *error = NULL;
			uint32_t num_removed = 0;
			int fd = INVALID_FD;

			/**
			 * Loading as many entries as possible into reader
			 */
			daemon_job_reader reader;
			daemon_job_record *pjob = NULL;

			/*
			 * It's possible that some entries exist in the jobs SBT from a
			 * pre-Riptide build; these are incompatible with Riptide-or-later,
			 * so ignore them when retrieving jobs to return.
			 *
			 * Furthermore, we only want to cleanup jobs that match certain
			 * criteria, so use a second filter once the entries have been
			 * deserialized into jobs.
			 */
			reader.set_pre_deserialization_filter(ignore_pre_RT_jobs, NULL);
			reader.set_post_deserialization_filter(cleanup_filter, &older_than);

			/**
			 * Point the reader at the jobs SBT
			 */
			fd = cpool_fd_store::getInstance().get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			reader.open(fd, true); // Exclusive job locks

			/**
			 * Obtain jobs for cleanup by calling the reader's get_first_match
			 * and get_next_match() methods. These methods return a pointer to
			 * a locked daemon_job_record. Unlock the record prior to getting
			 * the next one and when the search is complete.
			 */
			pjob = reader.get_first_match(&error); //////调用sbt_reader.get_entries(),获得一个vector的sbt entries
			// ilog(IL_NOTICE, "%s called entries size:%lu", __func__, reader.get_entry_num());  里面获得所有djob_record这个SBT树中的entries, 后面会过滤掉single_job
			ON_ISI_ERROR_GOTO(out, error);

			/**
			 * Do not remove more than requested
			 */
			while (pjob && (num_removed < max_num))
			{
				ilog(IL_NOTICE, "%s called djob_id:%d", __func__, pjob->get_daemon_job_id());
				remove_job(pjob, &error);
				ON_ISI_ERROR_GOTO(out, error);
				++num_removed;

				/**
				 * The job needs to be unlocked and deallocated before moving
				 * on to the next one.
				 */
				unlock_daemon_job_record(pjob->get_daemon_job_id(), &error);
				delete pjob;
				pjob = NULL;
				ON_ISI_ERROR_GOTO(out, error);

				// Now get the next one
				pjob = reader.get_next_match(&error); /////从vector中读取下一个sbt entry,并转为djob_record
				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			if (pjob)
			{
				unlock_daemon_job_record(pjob->get_daemon_job_id(),
										 isi_error_suppress());
				delete pjob;
				pjob = NULL;
			}

			isi_error_handle(error, error_out);

			return num_removed;
		}

		static daemon_job_member *
		query_job_for_members_helper(const cpool_operation_type *op_type,
									 struct sbt_entry *sbt_entry, struct isi_error **error_out)
		{
			ASSERT(op_type != NULL);
			ASSERT(op_type != OT_NONE);

			struct isi_error *error = NULL;
			task_key *task_key = NULL;
			daemon_job_member *member = NULL;
			char *name_buf_heap = NULL;

			if (op_type->uses_hsbt())
			{
				struct hsbt_entry hashed_entry;
				sbt_entry_to_hsbt_entry(sbt_entry, &hashed_entry);
				ASSERT(hashed_entry.valid_);

				task_key = task_key::create_key(op_type, hashed_entry.key_,
												hashed_entry.key_length_, &error);
				ON_ISI_ERROR_GOTO(out, error);
				ASSERT(task_key != NULL);

				member = new daemon_job_member(task_key);
				member->set_serialization(hashed_entry.value_,
										  hashed_entry.value_length_, &error);

				hsbt_entry_destroy(&hashed_entry);
			}
			else
			{
				btree_key_t sbt_key = sbt_entry->key;
				task_key = task_key::create_key(op_type, &sbt_key,
												sizeof sbt_key, &error);
				ON_ISI_ERROR_GOTO(out, error);

				member = new daemon_job_member(task_key);
				member->set_serialization(sbt_entry->buf,
										  sbt_entry->size_out, &error);
			}
			ON_ISI_ERROR_GOTO(out, error);

			/* If the member does not have a name, get it now. */
			if (member->get_name() == NULL)
			{
				char name_buf_stack[512];
				size_t name_size_out;

				const char *name_buf = name_buf_stack;
				task_key->get_name(name_buf_stack, sizeof name_buf_stack,
								   &name_size_out, &error);
				if (error != NULL && isi_system_error_is_a(error, ENOSPC))
				{
					isi_error_free(error);
					error = NULL;
					name_buf_heap = new char[name_size_out + 1];
					ASSERT(name_buf_heap != NULL);

					name_buf = name_buf_heap;
					task_key->get_name(name_buf_heap, name_size_out + 1,
									   NULL, &error);
				}

				/*
				 * Could get an ENOENT if the file was removed by user,
				 * or ESTALE if just the snapshot was - this is
				 * ok - we still want to show files that can be resolved.
				 */
				if (error != NULL && (isi_system_error_is_a(error, ENOENT) ||
									  isi_system_error_is_a(error, ESTALE)))
				{
					isi_error_free(error);
					error = NULL;

					name_buf = "";
				}

				ON_ISI_ERROR_GOTO(out, error);

				member->set_name(name_buf);
			}

		out:
			if (error != NULL && member != NULL)
			{
				delete member;
				member = NULL;
			}

			if (task_key != NULL)
				delete task_key;

			if (name_buf_heap != NULL)
				delete[] name_buf_heap;

			isi_error_handle(error, error_out);

			return member;
		}

		const cpool_operation_type *get_operation_type_for_job_id(
			djob_id_t daemon_job_id, struct isi_error **error_out)
		{
			ASSERT(daemon_job_id != DJOB_RID_INVALID);

			struct isi_error *error = NULL;
			bool record_locked = false;
			daemon_job_record *djob_rec = NULL;
			const cpool_operation_type *op_type = OT_NONE;

			/* true: block, false: shared lock */
			lock_daemon_job_record(daemon_job_id, true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = true;

			djob_rec = get_daemon_job_record(daemon_job_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			op_type = djob_rec->get_operation_type();

		out:
			if (record_locked)
				unlock_daemon_job_record(daemon_job_id,
										 isi_error_suppress(IL_NOTICE));

			if (djob_rec != NULL)
				delete djob_rec;

			isi_error_handle(error, error_out);

			return op_type;
		}

		void
		query_job_for_members(djob_id_t daemon_job_id, unsigned int start,
							  resume_key &start_key,
							  unsigned int max_desired, std::vector<daemon_job_member *> &members,
							  const cpool_operation_type *op_type, struct isi_error **error_out)
		{
			ASSERT(daemon_job_id != DJOB_RID_INVALID);
			ASSERT(members.empty(), "non-empty vector (size: %zu)",
				   members.size());
			ASSERT(op_type != NULL && op_type != OT_NONE,
				   "Need a non-empty operation type");

			struct isi_error *error = NULL;
			bool record_locked = false;
			int member_sbt_fd = -1;
			unsigned int num_read_entries = 0;
			btree_key_t iter_key = {{0, 0}};
			btree_key_t last_key = {{0, 0}};
			bool finished = false;

			if (!start_key.empty())
			{
				/* if resume from prior request */
				iter_key = start_key.decode();
				start = 0; /* frist entry is counted as 0 */
			}

			/* true: block, false: shared lock */
			lock_daemon_job_record(daemon_job_id, true, false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			record_locked = true;

			member_sbt_fd = open_member_sbt(daemon_job_id, false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			while (true)
			{
				const size_t num_entries_to_retrieve = 8;
				char entries_buf[num_entries_to_retrieve * SBT_ENTRY_MAX_SIZE];
				size_t num_entries_retrieved = 0;

				last_key = iter_key; /* remember next key */

				/*
				 * Read entries as if they are non-hashed; when we iterate over
				 * the returned entries we can translate them to hashed entries
				 * if necessary.
				 */
				int result = ifs_sbt_get_entries(member_sbt_fd, &iter_key,
												 &iter_key, sizeof entries_buf, entries_buf,
												 num_entries_to_retrieve, &num_entries_retrieved);

				if (result != 0)
				{
					/* ENOSPC should not occur due to sizing of buffer. */
					ASSERT(errno != ENOSPC);
					error = isi_system_error_new(errno,
												 "error retrieving members for daemon job %d",
												 daemon_job_id);
					goto out;
				}

				if (num_entries_retrieved == 0)
				{
					/* No more entry */
					finished = true;
					break;
				}

				/* if we have not rearched the desired starting point */
				if ((num_read_entries + num_entries_retrieved) < start)
				{
					num_read_entries += num_entries_retrieved;
					continue;
				}

				/* Read some entries, add them to the vector. */
				char *temp = entries_buf;
				for (size_t i = 0; i < num_entries_retrieved;
					 ++i, ++num_read_entries)
				{

					struct sbt_entry *current_entry =
						(struct sbt_entry *)temp;
					ASSERT(current_entry != NULL);

					/*
					 * Skip entries as needed to get to the desired
					 * starting point.
					 */
					if (num_read_entries >= start)
					{
						daemon_job_member *member =
							query_job_for_members_helper(
								op_type, current_entry, &error);
						ON_ISI_ERROR_GOTO(out, error);
						ASSERT(member != NULL);
						members.push_back(member);

						if (members.size() == max_desired)
						{
							/* get next key */
							if ((i + 1) < num_entries_retrieved)
							{
								temp += (sizeof(struct sbt_entry) +
										 current_entry->size_out);
								current_entry =
									(struct sbt_entry *)temp;
								last_key = current_entry->key;
							}
							else
								last_key = iter_key;
							goto done;
						}
					}

					temp += (sizeof(struct sbt_entry) +
							 current_entry->size_out);
				}
			}

		done:
			ASSERT(members.size() <= max_desired);

			/* recalculate start key */
			if (finished)
				start_key.clear();
			else
				start_key.encode(last_key);

		out:
			if (record_locked)
				unlock_daemon_job_record(daemon_job_id,
										 isi_error_suppress(IL_NOTICE));

			if (member_sbt_fd != -1)
				close_sbt(member_sbt_fd);

			if (error != NULL)
			{
				std::vector<daemon_job_member *>::iterator iter =
					members.begin();
				for (; iter != members.end(); ++iter)
					delete *iter;

				members.clear();
			}

			isi_error_handle(error, error_out);
		}

		void
		get_all_jobs(std::list<daemon_job_record *> &jobs,
					 struct isi_error **error_out)
		{
			ASSERT(jobs.empty());

			struct isi_error *error = NULL;
			bool record_locked = false;

			/**
			 * Instantiate a daemon_job_reader and retrieve all of the active
			 * jobs.
			 */
			daemon_job_reader reader;
			daemon_job_record *djob_record = NULL;
			int jobs_fd = -1;

			jobs_fd = cpool_fd_store::getInstance()
						  .get_djobs_sbt_fd(false, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * It's possible that some entries exist in the jobs SBT from a
			 * pre-Riptide build; these are incompatible with Riptide-or-later,
			 * so ignore them when retrieving jobs to return.
			 */
			reader.set_pre_deserialization_filter(ignore_pre_RT_jobs, NULL);

			reader.open(jobs_fd, false); // Jobs not locked exclusively

			/**
			 * Obtain jobs for cleanup by calling the reader's get_first_match
			 * and get_next_match() methods. These methods return a pointer to
			 * a locked daemon_job_record. Unlock the record prior to getting
			 * the next one and when the search is complete.
			 */
			djob_record = reader.get_first_match(&error);
			ON_ISI_ERROR_GOTO(out, error);

			while (djob_record)
			{
				record_locked = true;

				jobs.push_back(djob_record);

				unlock_daemon_job_record(djob_record->get_daemon_job_id(), &error);
				ON_ISI_ERROR_GOTO(out, error);
				record_locked = false;

				djob_record = reader.get_next_match(&error);
				ON_ISI_ERROR_GOTO(out, error);
			}

		out:
			if (record_locked && djob_record)
				unlock_daemon_job_record(djob_record->get_daemon_job_id(),
										 isi_error_suppress(IL_NOTICE));

			if (error != NULL)
			{
				std::list<daemon_job_record *>::iterator iter = jobs.begin();
				for (; iter != jobs.end(); ++iter)
					delete *iter;
				jobs.clear();
			}

			isi_error_handle(error, error_out);
		}

	} // end namespace daemon_job_manager
} // end namespace isi_cloud
