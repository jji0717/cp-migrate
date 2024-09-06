#include <sys/stat.h>

#include <ifs/ifs_lin_open.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_util/syscalls.h>
#include <isi_ilog/ilog.h>

#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_licensing/licensing.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_fd_store.h>
#include <isi_cpool_d_common/daemon_job_configuration.h>
#include <isi_cpool_d_common/daemon_job_manager.h>
#include <isi_cpool_d_common/daemon_job_record.h>
#include <isi_cpool_d_common/operation_type.h>
#include <isi_cpool_d_common/scoped_object_list.h>
#include <isi_cpool_d_common/task.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>

#include "client_task_map.h"

#include "cpool_api.h"
#include "cpool_message.h"

#define XOR(a, b) ((a) != (b))

using namespace isi_cloud;

static bool
is_file_stubbed_stat(const struct stat &stat_buf)
{
	return ((stat_buf.st_flags & SF_FILE_STUBBED) != 0);
}

static bool
is_file_stubbed_fd(int fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool is_stubbed = false;
	struct stat stat_buf;

	if (fstat(fd, &stat_buf) != 0) {
		error = isi_system_error_new(errno, "failed to fstat file");
		goto out;
	}

	is_stubbed = is_file_stubbed_stat(stat_buf);

 out:
	isi_error_handle(error, error_out);

	return is_stubbed;
}

static bool
is_file_stubbed_lin_snapid(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool is_stubbed = false;
	int fd = -1;

	if ((fd = ifs_lin_open(lin, snapid, O_RDONLY)) < 0) {
		error = isi_system_error_new(errno,
		    "failed to open stub file");
		goto out;
	}

	is_stubbed = is_file_stubbed_fd(fd, &error);
	if (error != NULL) {
		isi_error_add_context(error,
		    "failed to get stubbed/non-stubbed state "
		    "for LIN/snapID %{}", lin_snapid_fmt(lin, snapid));
		goto out;
	}

 out:
	if (fd != -1)
		close(fd);

	isi_error_handle(error, error_out);

	return is_stubbed;
}

void
archive_file(djob_id_t *djob_id, int fd, const char *filename,
    uint32_t policy_id, cpool_api_priority priority,
    bool ignore_already_archived, bool ignore_invalid_file,
    struct isi_error **error_out)
{
	ASSERT(djob_id != NULL);
	ASSERT(policy_id != CPOOL_INVALID_POLICY_ID);

	struct isi_error *error = NULL;
	bool is_stubbed;
	struct stat stat_buf;
	client_task_map tm(OT_ARCHIVE);
	isi_cloud::archive_task t;

	isi_licensing_status licstat = isi_licensing_module_status(ISI_LICENSING_CLOUDPOOLS);
	if (licstat != ISI_LICENSING_LICENSED) {
		error = isi_system_error_new(EPERM,
		    "CloudPools is not licensed");
		goto out;
	}

	if (policy_id == CPOOL_INVALID_POLICY_ID) {
		error = isi_system_error_new(EINVAL,
		    "invalid policy id %d provided",
		    policy_id);
		goto out;
	}

	if (fstat(fd, &stat_buf) != 0) {
		error = isi_system_error_new(errno, "failed to fstat file");
		goto out;
	}

	/*
	 * Don't archive stubs - return error only if caller doesn't want to
	 * ignore it.
	 */
	is_stubbed = is_file_stubbed_stat(stat_buf);
	if (is_stubbed) {
		if (!ignore_already_archived)
			error = isi_system_error_new(EINVAL,
			    "file has already been archived.  (%{}, %{})",
			    lin_fmt(stat_buf.st_ino), snapid_fmt(HEAD_SNAPID));
		goto out;
	}

	/*
	 * Don't archive non-regular files - return error only if caller
	 * doesn't want to ignore it.
	 */
	if (!S_ISREG(stat_buf.st_mode)) {
		if (!ignore_invalid_file) {
			error = isi_system_error_new(EINVAL,
			    "cannot archive non-regular file. (%{}, %{})"
			    " mode: %d",
			    lin_fmt(stat_buf.st_ino), snapid_fmt(HEAD_SNAPID),
			    stat_buf.st_mode);
		}
		goto out;
	}

	if (stat_buf.st_flags & UF_ADS) {
		if (!ignore_invalid_file) {
			error = isi_system_error_new(EINVAL,
			    "Archiving of ADS is not supported	(%{}, %{})",
			    lin_fmt(stat_buf.st_ino), snapid_fmt(HEAD_SNAPID));
		}
		goto out;
	}

	/* If a daemon job wasn't provided, create one now. */
	if (*djob_id == DJOB_RID_INVALID) {
		*djob_id =
		    daemon_job_manager::start_job_no_job_engine(OT_ARCHIVE,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	t.set_lin(stat_buf.st_ino);
	t.set_snapid(HEAD_SNAPID);
	t.set_priority(priority);

	if (t.has_policyid()) {
		t.set_policy_id(policy_id);
	} else {
		const char* policy_name = lookup_policy_name(policy_id, &error);
		ON_ISI_ERROR_GOTO(out, error);

		t.set_policy_name(policy_name);
	}

	tm.add_task(&t, *djob_id, filename, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
recall_file(djob_id_t *djob_id, ifs_lin_t lin, ifs_snapid_t snap_id,
    const char *filename, cpool_api_priority priority, bool retain_stub,
    bool ignore_not_archived, struct isi_error **error_out)
{
	ASSERT(djob_id != NULL);

	struct isi_error *error = NULL;
	client_task_map tm(OT_RECALL);
	isi_cloud::recall_task t;
	bool is_stubbed;

	is_stubbed = is_file_stubbed_lin_snapid(lin, snap_id, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!is_stubbed) {
		if (!ignore_not_archived)
			error = isi_system_error_new(EINVAL,
			    "file to recall has not been archived");

		goto out;
	}

	/* If a daemon job wasn't provided, create one now. */
	if (*djob_id == DJOB_RID_INVALID) {
		*djob_id =
		    daemon_job_manager::start_job_no_job_engine(OT_RECALL,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	t.set_lin(lin);
	t.set_snapid(snap_id);
	t.set_priority(priority);
	t.set_retain_stub(retain_stub);

	tm.add_task(&t, *djob_id, filename, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
writeback_file(ifs_lin_t lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool stubbed = false;
	client_task_map tm(OT_WRITEBACK);
	isi_cloud::writeback_task t;
	djob_id_t job_id;

	stubbed = is_file_stubbed_lin_snapid(lin, HEAD_SNAPID, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!stubbed) {
		error = isi_system_error_new(EINVAL,
		    "file to writeback is not stubbed");
		goto out;
	}

	job_id = daemon_job_manager::start_job_no_job_engine(OT_WRITEBACK,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	t.set_lin(lin);
	t.set_snapid(HEAD_SNAPID);
	t.set_priority(0);

	tm.add_task(&t, job_id, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

//TODO:: When/where is this version used?  Is it needed?
void
writeback_file(const char *file_name, struct isi_error **error_out)
{
	ASSERT(file_name != NULL);

	struct isi_error *error = NULL;
	struct stat stat_buf;

	if (stat(file_name, &stat_buf) != 0) {
		error = isi_system_error_new(errno,
		    "failed to fstat file");
		goto out;
	}

	writeback_file(stat_buf.st_ino, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
stub_file_delete(ifs_lin_t lin, ifs_snapid_t snap_id,
    const isi_cloud_object_id &object_id,
    const struct timeval &deleted_stub_file_dod, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	client_task_map tm(OT_LOCAL_GC);
	isi_cloud::local_gc_task t;
	djob_id_t djob_id;

	djob_id = daemon_job_manager::start_job_no_job_engine(OT_LOCAL_GC,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	t.set_lin(lin);
	t.set_snapid(snap_id);
	t.set_priority(0);
	t.set_object_id(object_id);
	t.set_date_of_death(deleted_stub_file_dod);

	tm.add_task(&t, djob_id, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to prepare for addition of task (%#{})",
	    cpool_task_fmt(&t));

 out:
	isi_error_handle(error, error_out);
}

void
delete_cloud_object(const isi_cloud_object_id &object_id,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	client_task_map tm(OT_CLOUD_GC);
	isi_cloud::cloud_gc_task t;
	djob_id_t djob_id;

	djob_id = daemon_job_manager::start_job_no_job_engine(OT_CLOUD_GC,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	t.set_cloud_object_id(object_id);
	t.set_priority(0);

	tm.add_task(&t, djob_id, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
invalidate_stubbed_file_cache(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool is_stubbed;
	client_task_map tm(OT_CACHE_INVALIDATION);
	isi_cloud::cache_invalidation_task t;
	djob_id_t djob_id;

	is_stubbed = is_file_stubbed_lin_snapid(lin, snapid, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "attempting to invalidate cache for LIN/snapid %{}",
	    lin_snapid_fmt(lin, snapid));

	if (!is_stubbed) {
		error = isi_system_error_new(EINVAL,
		    "cannot invalidate the cache of a non-archived file");
		goto out;
	}

	djob_id =
	    daemon_job_manager::start_job_no_job_engine(OT_CACHE_INVALIDATION,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	t.set_lin(lin);
	t.set_snapid(snapid);
	t.set_priority(0);

	tm.add_task(&t, djob_id, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
delete_djob_member(struct job_member *&member)
{
	ASSERT(member != NULL);

	free(member->key_);
	free(member->name_);

	delete member;
	member = NULL;
}

void
query_job_for_members(djob_id_t daemon_job_id, unsigned int start,
    resume_key &start_key,
    unsigned int max, std::vector<struct job_member *> &members,
    const cpool_operation_type *&op_type, struct isi_error **error_out)
{
	ASSERT(daemon_job_id != DJOB_RID_INVALID);

	struct isi_error *error = NULL;
	op_type = isi_cloud::daemon_job_manager::get_operation_type_for_job_id(
	    daemon_job_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "Failed to get operation type for job id: %d", daemon_job_id);

	{
		ASSERT(op_type != OT_NONE);
		client_task_map tm(op_type);

		tm.query_job_for_members(daemon_job_id, start, start_key,
		    max, members, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
 out:
	if (error != NULL) {
		std::vector<struct job_member *>::iterator iter =
		    members.begin();
		for (; iter != members.end(); ++iter)
			delete_djob_member(*iter);

		members.clear();
	}

	isi_error_handle(error, error_out);
}

static djob_id_t
get_daemon_job_id_for_je_job_id(jd_jid_t je_id,
    djob_op_job_task_state *djob_state, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id = DJOB_RID_INVALID;
	std::list<daemon_job_record *> all_jobs;
	std::list<daemon_job_record *>::const_iterator iter;

	isi_cloud::daemon_job_manager::get_all_jobs(all_jobs, &error);
	ON_ISI_ERROR_GOTO(out, error);

	for (iter = all_jobs.begin(); iter != all_jobs.end(); ++iter) {
		if ((*iter)->get_job_engine_job_id() == je_id) {
			djob_id = (*iter)->get_daemon_job_id();

			if (djob_state != NULL) {
				*djob_state =
				    isi_cloud::daemon_job_manager::get_state(
				    *iter, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}

			break;
		}
	}

	if (djob_id == DJOB_RID_INVALID) {
		error = isi_system_error_new(ENOENT,
		    "no daemon job found for JobEngine job %u", je_id);
		goto out;
	}

 out:
	iter = all_jobs.begin();
	for (; iter != all_jobs.end(); ++iter)
		delete *iter;
	all_jobs.clear();

	isi_error_handle(error, error_out);

	return djob_id;
}

djob_id_t
get_daemon_job_id_for_smartpools(jd_jid_t je_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id = DJOB_RID_INVALID;
	djob_op_job_task_state djob_state = DJOB_OJT_NONE;
	bool record_locked = false;
	daemon_job_record *djob_record = NULL;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	int djob_record_sbt_fd = -1;
	btree_key_t djob_record_key;

	/*
	 * Look for an existing daemon job paired with the incoming JE job ID.
	 */
	djob_id = get_daemon_job_id_for_je_job_id(je_id, &djob_state, &error);
	if (error != NULL && isi_system_error_is_a(error, ENOENT)) {
		isi_error_free(error);
		error = NULL;

		djob_id = DJOB_RID_INVALID;
	}
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * If we found an existing job that's not complete, we're done - return
	 * its ID.
	 */
	if (djob_id != DJOB_RID_INVALID &&
	    djob_state != DJOB_OJT_ERROR &&
	    djob_state != DJOB_OJT_COMPLETED &&
	    djob_state != DJOB_OJT_CANCELLED)
		goto out;

	/*
	 * We didn't find an existing job, or we did but it's no longer
	 * running, so create one now.  We'll have to update it with the
	 * SmartPools JobEngine job ID and state.
	 */
	djob_id =
	    isi_cloud::daemon_job_manager::start_job_no_job_engine(OT_ARCHIVE,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* true: block, true: exclusive lock */
	isi_cloud::daemon_job_manager::lock_daemon_job_record(djob_id, true,
	    true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	record_locked = true;

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record->save_serialization();
	djob_record->set_job_engine_job_id(je_id);
	djob_record->set_job_engine_state(STATE_RUNNING);

	djob_record->get_saved_serialization(old_serialization,
	    old_serialization_size);
	djob_record->get_serialization(new_serialization,
	    new_serialization_size);

	djob_record_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record_key = djob_record->get_key();

	if (ifs_sbt_cond_mod_entry(djob_record_sbt_fd, &djob_record_key,
	    new_serialization, new_serialization_size, NULL, BT_CM_BUF,
	    old_serialization, old_serialization_size, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to persist updated daemon job record");
		goto out;
	}

 out:
	delete djob_record;
	djob_record = NULL;

	if (record_locked) {
		ASSERT(djob_id != DJOB_RID_INVALID);
		isi_cloud::daemon_job_manager::unlock_daemon_job_record(
		    djob_id, isi_error_suppress(IL_NOTICE));
		record_locked = false;
	}

	isi_error_handle(error, error_out);

	return djob_id;
}

static void
_check_job_complete_internal(djob_id_t djob_id, jd_jid_t je_id,
    enum jobstat_state state, struct isi_error **error_out)
{
	/*
	 * Should have either a null djob_id or a null job engine id, but
	 * not both
	 */
	ASSERT(XOR(djob_id == DJOB_RID_INVALID, je_id == NULL_JID),
	    "Should have either a null job_id or a null job_engine_id, but "
	    "not both: djob_id=%d, je_id=%d", djob_id, je_id);

	struct isi_error *error = NULL;
	bool record_locked = false;
	daemon_job_record *djob_record = NULL;
	int daemon_job_record_sbt_fd;
	btree_key_t djob_record_key;

	/* The daemon job record SBT is not hashed. */
	struct sbt_bulk_entry sbt_op;
	std::vector<struct sbt_bulk_entry> sbt_ops;
	scoped_object_list objects_to_delete;

	daemon_job_configuration *djob_config = NULL;

	const void *old_rec_serialization, *new_rec_serialization;
	size_t old_rec_serialization_size, new_rec_serialization_size;

	/*
	 * Job engine case, find associated daemon job id - we don't care about
	 * its state
	 */
	if (djob_id == DJOB_RID_INVALID) {
		djob_id = get_daemon_job_id_for_je_job_id(je_id, NULL,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);

		ASSERT(djob_id != DJOB_RID_INVALID);
	}

	/* true: block, true: exclusive lock */
	isi_cloud::daemon_job_manager::lock_daemon_job_record(djob_id, true,
	    true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	record_locked = true;

	djob_record =
	    isi_cloud::daemon_job_manager::get_daemon_job_record(djob_id,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record->save_serialization();

	if (je_id != NULL_JID)
		djob_record->set_job_engine_state(state);

	daemon_job_record_sbt_fd = cpool_fd_store::getInstance().\
	    get_djobs_sbt_fd(false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record_key = djob_record->get_key();

	/*
	 * Determine whether or not the daemon job is now complete, which may
	 * require a change to the daemon job configuration.  If such a change
	 * is required, this function will save the current daemon job
	 * configuration serialization.
	 */
	isi_cloud::daemon_job_manager::check_job_complete(djob_config,
	    djob_record, objects_to_delete, &error);
	ON_ISI_ERROR_GOTO(out, error);

	djob_record->get_saved_serialization(old_rec_serialization,
	    old_rec_serialization_size);
	djob_record->get_serialization(new_rec_serialization,
	    new_rec_serialization_size);

	bzero(&sbt_op, sizeof sbt_op);

	sbt_op.fd = daemon_job_record_sbt_fd;
	sbt_op.op_type = SBT_SYS_OP_MOD;
	sbt_op.key = djob_record_key;
	sbt_op.cm = BT_CM_BUF;
	sbt_op.old_entry_buf = const_cast<void *>(old_rec_serialization);
	sbt_op.old_entry_size = old_rec_serialization_size;
	sbt_op.entry_buf = const_cast<void *>(new_rec_serialization);
	sbt_op.entry_size = new_rec_serialization_size;

	sbt_ops.push_back(sbt_op);

	/*
	 * A non-NULL daemon job configuration means it was changed by the call
	 * to check_job_complete, so add this change to the bulk op.
	 */
	if (djob_config != NULL) {
		const void *old_cfg_serialization;
		size_t old_cfg_serialization_size;
		djob_config->get_saved_serialization(
		    old_cfg_serialization, old_cfg_serialization_size);

		const void *new_cfg_serialization;
		size_t new_cfg_serialization_size;
		djob_config->get_serialization(new_cfg_serialization,
		    new_cfg_serialization_size);

		btree_key_t djob_cfg_key =
		    daemon_job_record::get_key(
		    daemon_job_configuration::get_daemon_job_id());

		bzero(&sbt_op, sizeof sbt_op);

		sbt_op.fd = daemon_job_record_sbt_fd;
		sbt_op.op_type = SBT_SYS_OP_MOD;
		sbt_op.key = djob_cfg_key;
		sbt_op.cm = BT_CM_BUF;
		sbt_op.old_entry_buf =
		    const_cast<void *>(old_cfg_serialization);
		sbt_op.old_entry_size = old_cfg_serialization_size;
		sbt_op.entry_buf = const_cast<void *>(new_cfg_serialization);
		sbt_op.entry_size = new_cfg_serialization_size;

		sbt_ops.push_back(sbt_op);
	}

	/* The daemon job record SBT is not hashed. */
	if (ifs_sbt_bulk_op(sbt_ops.size(), sbt_ops.data()) != 0) {
		error = isi_system_error_new(errno, "ifs_sbt_bulk_op failed");
		goto out;
	}

 out:
// need to delete djob_config here if non-NULL - does it need to be unlocked too?
	if (record_locked)
		isi_cloud::daemon_job_manager::unlock_daemon_job_record(
		    djob_id, isi_error_suppress(IL_NOTICE));

	if (djob_record != NULL)
		delete djob_record;

	isi_error_handle(error, error_out);
}

void
on_job_engine_job_complete(jd_jid_t je_id, enum jobstat_state state,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	_check_job_complete_internal(DJOB_RID_INVALID, je_id, state, &error);

	isi_error_handle(error, error_out);
}

void
preallocate_cpool_d_for_test(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	/* Force opening of static file descriptors */
	cpool_fd_store::getInstance();

	/* Force initialization of cached cpool_d gconfig settings */
	scoped_cfg_reader reader;
	reader.get_cfg(&error);

	isi_error_handle(error, error_out);
}

void restore_coi_for_account(djob_id_t *djob_id, const char *cluster,
    uint32_t account_id, const char *account_name, struct timeval dod,
    struct isi_error **error_out)
{
	ASSERT(djob_id != NULL);

	struct isi_error *error = NULL;
	client_task_map tm(OT_RESTORE_COI);
	isi_cloud::restore_coi_task t;

	task_account_id acct;
	acct.cluster_ = cluster;
	acct.account_id_ = account_id;

	/* If a daemon job wasn't provided, create one now. */
	if (*djob_id == DJOB_RID_INVALID) {
		*djob_id = daemon_job_manager::start_job_no_job_engine(
		    OT_RESTORE_COI, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	t.set_account_id(acct);
	t.set_dod(dod);
	t.set_priority(0);
	tm.add_task(&t, *djob_id, account_name, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);
}

void
check_for_job_completion(djob_id_t djob_id, struct isi_error **error_out)
{
	ASSERT(djob_id != DJOB_RID_INVALID);

	struct isi_error *error = NULL;

	_check_job_complete_internal(djob_id, NULL_JID, STATE_NULL, &error);

	isi_error_handle(error, error_out);
}

bool
is_account_managed_by_restore_coi_job(const struct cpool_account *account,
    struct isi_error **error_out)
{
	ASSERT(account != NULL);

	struct isi_error *error = NULL;
	bool ret_val = false;
	isi_cloud::task *returned_task = NULL;
	isi_cloud::restore_coi_task *rcoi_task = NULL;
	struct task_account_id aid;

	aid.account_id_ = account->account_id;
	aid.cluster_.assign(account->birth_cluster_id);

	restore_coi_task task(aid);
	client_task_map tm(OT_RESTORE_COI);
	/* true ==> ignore_ENOENT */
	returned_task = tm.find_task(task.get_key(), true, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * If we find a task which is not done processing, we know the given
	 * account is managed by a restore_coi job
	 */
	if (returned_task != NULL) {
		rcoi_task = dynamic_cast<isi_cloud::restore_coi_task*>(
		    returned_task);

		ASSERT(rcoi_task != NULL);
		ret_val = !rcoi_task->is_done_processing();
	}

 out:
	if (returned_task != NULL)
		delete returned_task;

	isi_error_handle(error, error_out);

	return ret_val;
}

static bool
restore_coi_job_is_running_for(const char *guid,
    struct cpool_config_context *context, struct isi_error **error_out)
{
	ASSERT(guid != NULL);

	bool ret_val = false;
	struct isi_error *error = NULL;
	struct cpool_account *acct = NULL;

	for (acct = cpool_account_get_iterator(context); acct != NULL;
	    acct = cpool_account_get_next(context, acct)) {

		/*
		 * If the current account came from given guid, check
		 * whether it is part of any restore_coi job
		 */
		if (strcmp(acct->birth_cluster_id, guid) == 0) {
			ret_val = is_account_managed_by_restore_coi_job(acct,
			    &error);

			ON_ISI_ERROR_GOTO(out, error);

			if (ret_val)
				goto out;
		}
	}

 out:
	isi_error_handle(error, error_out);

	return ret_val;
}

unsigned int
get_num_tasks_remaining_to_disable_guids(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	unsigned int total_tasks = 0;
	client_task_map wb_tm(OT_WRITEBACK);
	client_task_map gc_tm(OT_CLOUD_GC);

	total_tasks += wb_tm.get_incomplete_task_count(&error);
	ON_ISI_ERROR_GOTO(out, error);

	total_tasks += gc_tm.get_incomplete_task_count(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);

	return total_tasks;
}

static bool
incomplete_tasks_remain(const cpool_operation_type *op_type,
    struct isi_error **error_out)
{
	ASSERT(op_type != NULL);

	struct isi_error *error = NULL;
	unsigned int tasks_remain = 0;

	client_task_map tm(op_type);
	tasks_remain = tm.get_incomplete_task_count(&error);
	ON_ISI_ERROR_GOTO(out, error,
	    "Failed to determine whether incomplete tasks remain");

 out:
	isi_error_handle(error, error_out);

	return (tasks_remain > 0);
}

void
finish_disabling_eligible_guids(int num_tries,
    struct isi_error **error_out)
{
	ASSERT(num_tries > 0, "num_tries is %d, should be greater than 0",
	    num_tries);

	struct isi_error *error = NULL;
	struct smartpools_context *context = NULL;
	struct cpool_config_context *cpool_ctx = NULL;

	cpool_ctx = cpool_context_open(&error);
	ON_ISI_ERROR_GOTO(out, error);

	for (int i = 0; i < num_tries; ++i) {
		ASSERT(context == NULL, "Context should be NULL here");
		ASSERT(error == NULL, "Error should be NULL here: %#{}",
		    isi_error_fmt(error));

		bool change_made = false;
		struct permit_guid_list_element *curr = NULL;

		smartpools_open_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		for (
		    curr = smartpools_get_permit_list_next(context, NULL);
		    curr != NULL;
		    curr = smartpools_get_permit_list_next(context, curr)) {

			ASSERT(curr->guid != NULL);

			if (curr->state != CM_A_ST_REMOVING)
				continue;

			bool all_queues_drained =
			    !incomplete_tasks_remain(OT_WRITEBACK, &error);
			ON_ISI_ERROR_GOTO(out, error);

			all_queues_drained = all_queues_drained &&
			    !incomplete_tasks_remain(OT_CLOUD_GC, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (all_queues_drained) {

				smartpools_set_guid_element_state(
				    CM_A_ST_DISABLED, curr);

				change_made = true;
			}
		}

		if (change_made) {

			smartpools_save_permit_list(context, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * Try to commit - if we reach a gconfig update
			 * conflict, we can try again
			 */
			smartpools_commit_changes(context, &error);
			if (error != NULL &&
			    i < num_tries - 1 &&
			    isi_error_is_a(error,
				GCI_RECOVERABLE_COMMIT_ERROR_CLASS)) {

				ilog(IL_DEBUG, "Failed to remove disabling "
				    "guids on attempt %d: %#{}",
				    i + 1, isi_error_fmt(error));

				isi_error_free(error);
				error = NULL;
			}
			ON_ISI_ERROR_GOTO(out, error);
		}

		smartpools_close_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/* If we didn't make a change, don't try again */
		if (!change_made)
			break;
	}
 out:

	if (cpool_ctx != NULL)
		cpool_context_close(cpool_ctx);

	if (context != NULL)
		smartpools_close_context(&context,
		    isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}

void
finish_adding_eligible_pending_guids(int num_tries,
    struct isi_error **error_out)
{
	ASSERT(num_tries > 0, "num_tries is %d, should be greater than 0",
	    num_tries);

	struct isi_error *error = NULL;
	struct smartpools_context *context = NULL;
	struct cpool_config_context *cpool_ctx = NULL;

	cpool_ctx = cpool_context_open(&error);
	ON_ISI_ERROR_GOTO(out, error);

	for (int i = 0; i < num_tries; ++i) {
		ASSERT(context == NULL, "Context should be NULL here");
		ASSERT(error == NULL, "Error should be NULL here: %#{}",
		    isi_error_fmt(error));

		bool change_made = false;
		struct permit_guid_list_element *curr = NULL;

		smartpools_open_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		for (
		    curr = smartpools_get_permit_list_next(context, NULL);
		    curr != NULL;
		    curr = smartpools_get_permit_list_next(context, curr)) {

			ASSERT(curr != NULL && curr->guid != NULL);

			if (curr->state != CM_A_ST_ADDING)
				continue;

			bool restore_coi_job_running =
			    restore_coi_job_is_running_for(curr->guid, cpool_ctx,
			    &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (!restore_coi_job_running) {

				smartpools_set_guid_element_state(
				    CM_A_ST_ENABLED, curr);

				change_made = true;
			}
		}

		if (change_made) {

			smartpools_save_permit_list(context, &error);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * Try to commit - if we reach a gconfig update
			 * conflict, we can try again
			 */
			smartpools_commit_changes(context, &error);
			if (error != NULL &&
			    i < num_tries - 1 &&
			    isi_error_is_a(error,
				GCI_RECOVERABLE_COMMIT_ERROR_CLASS)) {

				ilog(IL_DEBUG, "Failed to promote pending "
				    "guids on attempt %d: %#{}",
				    i + 1, isi_error_fmt(error));

				isi_error_free(error);
				error = NULL;
			}
			ON_ISI_ERROR_GOTO(out, error);
		}

		smartpools_close_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/* If we didn't make a change, don't try again */
		if (!change_made)
			break;
	}
 out:

	if (cpool_ctx != NULL)
		cpool_context_close(cpool_ctx);

	if (context != NULL)
		smartpools_close_context(&context,
		    isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}

void
cancel_guid_addition(const char *guid, int num_tries,
    struct isi_error **error_out)
{
	ASSERT(num_tries > 0, "num_tries is %d, should be greater than 0",
	    num_tries);

	struct isi_error *error = NULL;
	struct smartpools_context *context = NULL;

	for (int i = 0; i < num_tries; ++i) {
		ASSERT(context == NULL, "Context should be NULL here");
		ASSERT(error == NULL, "Error should be NULL here: %#{}",
		    isi_error_fmt(error));

		smartpools_open_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		smartpools_set_guid_state(context, CM_A_ST_DISABLED, guid);

		smartpools_save_permit_list(context, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/*
		 * If we reached a gconfig update conflict, we can try
		 * again
		 */
		smartpools_commit_changes(context, &error);
		if (error != NULL &&
		    i < num_tries - 1 &&
		    isi_error_is_a(error,
			GCI_RECOVERABLE_COMMIT_ERROR_CLASS)) {

			ilog(IL_DEBUG, "Failed to demote pending "
			    "guid %s on attempt %d: %#{}",
			    guid, i + 1, isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

		smartpools_close_context(&context, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
 out:

	if (context != NULL)
		smartpools_close_context(&context,
		    isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}

void
notify_daemon(cpool_message_recipient to, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_envelope out_envelope;

	out_envelope.add_message(to, CPM_ACTION_NOTIFY);

	out_envelope.send(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:

	isi_error_handle(error, error_out);
}
