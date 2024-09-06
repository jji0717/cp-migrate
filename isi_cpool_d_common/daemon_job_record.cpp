#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_ilog/ilog.h>

#include "cpool_d_common.h"
#include "cpool_d_debug.h"

#include "daemon_job_record.h"
#include "daemon_job_manager.h"

#define MAX_DESCRIPTION_SIZE 256

static void
pack(void **dst, const void *src, int size)
{
	memcpy(*dst, src, size);
	*(char **)dst += size;
}

static void
unpack(void **dst, void *src, int size)
{
	memcpy(src, *dst, size);
	*(char **)dst += size;
}

static void
record_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
				union fmt_conv_arg arg)
{
	const daemon_job_record *rec =
		static_cast<const daemon_job_record *>(arg.ptr);
	ASSERT(rec != NULL);

	rec->fmt_conv(fmt, args);
}

struct fmt_conv_ctx
cpool_djob_rec_fmt(const daemon_job_record *rec)
{
	struct fmt_conv_ctx ctx = {record_fmt_conv, {ptr : rec}};
	return ctx;
}

daemon_job_record::daemon_job_record()
	: version_(CPOOL_DAEMON_JOB_RECORD_VERSION), daemon_job_id_(DJOB_RID_INVALID), operation_type_(OT_NONE), completion_time_(0), state_(DJOB_OJT_NONE), job_engine_job_id_(NULL_JID), job_engine_job_state_(STATE_NULL), description_(NULL), serialization_up_to_date_(false), serialization_(NULL), serialization_size_(0), saved_serialization_(NULL), saved_serialization_size_(0)
{
	creation_time_ = time(NULL);
	latest_modification_time_ = creation_time_;
}

daemon_job_record::~daemon_job_record()
{
	if (description_ != NULL)
		delete[] description_;

	free(serialization_);
	free(saved_serialization_);
}

int daemon_job_record::get_version(void) const
{
	return version_;
}

djob_id_t
daemon_job_record::get_daemon_job_id(void) const
{
	return daemon_job_id_;
}

void daemon_job_record::set_daemon_job_id(djob_id_t id)
{
	ASSERT(id != DJOB_RID_INVALID);
	daemon_job_id_ = id;
	serialization_up_to_date_ = false;
}

const cpool_operation_type *
daemon_job_record::get_operation_type(void) const
{
	return operation_type_;
}

void daemon_job_record::set_operation_type(
	const cpool_operation_type *operation_type)
{
	operation_type_ = operation_type;
	serialization_up_to_date_ = false;
}

time_t
daemon_job_record::get_creation_time(void) const
{
	return creation_time_;
}

void daemon_job_record::set_creation_time(time_t creation_time)
{
	// part of ctor?
	creation_time_ = creation_time;
	serialization_up_to_date_ = false;
}

time_t
daemon_job_record::get_latest_modification_time(void) const
{
	return latest_modification_time_;
}

void daemon_job_record::set_latest_modification_time(time_t new_time)
{
	latest_modification_time_ = new_time;
	serialization_up_to_date_ = false;
}

time_t
daemon_job_record::get_completion_time(void) const
{
	return completion_time_;
}

void daemon_job_record::set_completion_time(time_t new_time)
{
	completion_time_ = new_time;
	serialization_up_to_date_ = false;
}

djob_op_job_task_state
daemon_job_record::get_state(void) const
{
	return state_;
}

void daemon_job_record::set_state(djob_op_job_task_state state)
{
	ASSERT(state != DJOB_OJT_NONE);

	state_ = state;

	/*
	 * Even though completion is a modification, it serves no purpose to
	 * update the modification time to the completion time.
	 */
	if (state_ == DJOB_OJT_COMPLETED ||
		state_ == DJOB_OJT_ERROR ||
		state_ == DJOB_OJT_CANCELLED)
		set_completion_time(time(NULL));
	else
		set_latest_modification_time(time(NULL));

	serialization_up_to_date_ = false;
}

jd_jid_t
daemon_job_record::get_job_engine_job_id(void) const
{
	return job_engine_job_id_;
}

void daemon_job_record::set_job_engine_job_id(jd_jid_t je_id)
{
	ASSERT(je_id != NULL_JID);

	job_engine_job_id_ = je_id;
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

jobstat_state
daemon_job_record::get_job_engine_state(void) const
{
	return job_engine_job_state_;
}

void daemon_job_record::set_job_engine_state(jobstat_state state)
{
	ASSERT(state != STATE_NULL);

	job_engine_job_state_ = state;
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

const daemon_job_stats *
daemon_job_record::get_stats(void) const
{
	return &stats_;
}

void daemon_job_record::increment_num_members(void)
{
	stats_.increment_num_members();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::decrement_num_members(void)
{
	stats_.decrement_num_members();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::increment_num_succeeded(void)
{
	stats_.increment_num_succeeded();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::increment_num_failed(void)
{
	stats_.increment_num_failed();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::increment_num_cancelled(void)
{
	stats_.increment_num_cancelled();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::increment_num_pending(void)
{
	stats_.increment_num_pending();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::decrement_num_pending(void)
{
	stats_.decrement_num_pending();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::increment_num_inprogress(void)
{
	stats_.increment_num_inprogress();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

void daemon_job_record::decrement_num_inprogress(void)
{
	stats_.decrement_num_inprogress();
	set_latest_modification_time(time(NULL));
	serialization_up_to_date_ = false;
}

const char *
daemon_job_record::get_description(void) const
{
	return description_;
}

void daemon_job_record::set_description(const char *description)
{
	ASSERT(description != NULL);

	if (description_ != NULL)
		delete[] description_;

	size_t stored_description_size =
		strlen(description) + 1 < MAX_DESCRIPTION_SIZE ? strlen(description) + 1 : MAX_DESCRIPTION_SIZE;

	description_ = new char[stored_description_size];
	ASSERT(description_ != NULL);

	strncpy(description_, description, stored_description_size);
	description_[stored_description_size - 1] = '\0';

	serialization_up_to_date_ = false;
}

//////序列化:将djob_record实例作为字节流写入sbt_entry中
void daemon_job_record::get_serialization(const void *&serialization,
										  size_t &serialization_size) const
{
	if (!serialization_up_to_date_)
		serialize();

	serialization = serialization_;
	serialization_size = serialization_size_;
}
/////反序列化：将sbt_entry中的字节流buf填充到djob_record中
void daemon_job_record::set_serialization(const void *serialization,
										  size_t serialization_size, struct isi_error **error_out)
{
	ASSERT(serialization != NULL);

	struct isi_error *error = NULL;

	free(serialization_);

	serialization_size_ = serialization_size;
	serialization_ = malloc(serialization_size_);
	if (serialization_ == NULL)
	{
		error = isi_system_error_new(ENOMEM, "malloc failed");
		goto out;
	}

	memcpy(serialization_, serialization, serialization_size_);

	deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (error != NULL)
	{
		free(serialization_);
		serialization_ = NULL;

		serialization_size_ = 0;
	}

	isi_error_handle(error, error_out);
}

void daemon_job_record::serialize(void) const
{
	free(serialization_);

	size_t description_len = 0;
	if (description_ != NULL)
		description_len = strlen(description_);

	serialization_size_ =
		sizeof version_ +
		sizeof daemon_job_id_ +
		operation_type_->get_packed_size() +
		sizeof creation_time_ +
		sizeof latest_modification_time_ +
		sizeof completion_time_ +
		sizeof state_ +
		sizeof job_engine_job_id_ +
		sizeof job_engine_job_state_ +
		stats_.get_packed_size() +
		sizeof description_len +
		description_len;

	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	void *temp = serialization_;

	pack(&temp, &version_, sizeof version_);
	pack(&temp, &daemon_job_id_, sizeof daemon_job_id_);
	operation_type_->pack(&temp);
	pack(&temp, &creation_time_, sizeof creation_time_);
	pack(&temp, &latest_modification_time_,
		 sizeof latest_modification_time_);
	pack(&temp, &completion_time_, sizeof completion_time_);
	pack(&temp, &state_, sizeof state_);
	pack(&temp, &job_engine_job_id_, sizeof job_engine_job_id_);
	pack(&temp, &job_engine_job_state_, sizeof job_engine_job_state_);
	stats_.pack(&temp);
	pack(&temp, &description_len, sizeof description_len);
	pack(&temp, description_, description_len);

	serialization_up_to_date_ = true;
}

void daemon_job_record::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t temp_description_len, correct_serialized_size;
	void *temp = serialization_;

	if (description_ != NULL)
	{
		delete[] description_;
		description_ = NULL;
	}

	unpack(&temp, &version_, sizeof version_);
	if (CPOOL_DAEMON_JOB_RECORD_VERSION != get_version())
	{
		error = isi_system_error_new(EPROGMISMATCH,
									 "Daemon Job Record Version mismatch. "
									 "expected version %d found version %d",
									 CPOOL_DAEMON_JOB_RECORD_VERSION, get_version());
		goto out;
	}

	unpack(&temp, &daemon_job_id_, sizeof daemon_job_id_);
	operation_type_ = cpool_operation_type::unpack(&temp);
	unpack(&temp, &creation_time_, sizeof creation_time_);
	unpack(&temp, &latest_modification_time_,
		   sizeof latest_modification_time_);
	unpack(&temp, &completion_time_, sizeof completion_time_);
	unpack(&temp, &state_, sizeof state_);
	unpack(&temp, &job_engine_job_id_, sizeof job_engine_job_id_);
	unpack(&temp, &job_engine_job_state_, sizeof job_engine_job_state_);
	stats_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	unpack(&temp, &temp_description_len, sizeof temp_description_len);
	if (temp_description_len > 0)
	{
		description_ = new char[temp_description_len + 1];
		if (description_ == NULL)
		{
			error = isi_system_error_new(ENOMEM, "new failed");
			goto out;
		}

		unpack(&temp, description_, temp_description_len);
		description_[temp_description_len] = '\0';
	}

	correct_serialized_size =
		sizeof version_ +
		sizeof daemon_job_id_ +
		operation_type_->get_packed_size() +
		sizeof creation_time_ +
		sizeof latest_modification_time_ +
		sizeof completion_time_ +
		sizeof state_ +
		sizeof job_engine_job_id_ +
		sizeof job_engine_job_state_ +
		stats_.get_packed_size() +
		sizeof temp_description_len +
		temp_description_len;

	if (serialization_size_ != correct_serialized_size)
	{
		error = isi_system_error_new(EINVAL,
									 "received size (%zu) does not match expected size (%zu)",
									 serialization_size_, correct_serialized_size);
		goto out;
	}

	serialization_up_to_date_ = true;

out:
	isi_error_handle(error, error_out);
}

void daemon_job_record::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialization_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialization_, serialization_size_);
	saved_serialization_size_ = serialization_size_;
}

void daemon_job_record::get_saved_serialization(const void *&serialization,
												size_t &serialization_size) const
{
	serialization = saved_serialization_;
	serialization_size = saved_serialization_size_;
}

btree_key_t
daemon_job_record::get_key(djob_id_t id) //////将djob_id转化为btree_key_t类型， 可以用来做ifs_sbt操作
{
	btree_key_t key = {{id, 0}};
	return key;
}

btree_key_t
daemon_job_record::get_key(void) const
{
	return get_key(daemon_job_id_);
}

bool daemon_job_record::job_engine_job_is_running(struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	bool running =
		job_engine_job_id_ != NULL_JID &&
		job_engine_job_state_ != STATE_CANCELLED_USER &&
		job_engine_job_state_ != STATE_CANCELLED_SYSTEM &&
		job_engine_job_state_ != STATE_ABORTED &&
		job_engine_job_state_ != STATE_FINISHED;

	/* If job_engine_job_state_ == STATE_NULL,
	 * job engine job state may be not sycronized in daemon job record.
	 * Check job engine.
	 */
	if (job_engine_job_id_ != NULL_JID &&
		job_engine_job_state_ == STATE_NULL)
	{
		running =
			!(isi_cloud::daemon_job_manager::job_engine_job_is_finished(
				job_engine_job_id_, &error));
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);

	return running;
}

bool daemon_job_record::is_finished(void) const
{
	bool all_tasks_resolved = false, job_engine_job_finished = false;

	/* Have we resolved all identified tasks for this job? */
	all_tasks_resolved = (get_stats()->get_num_members() == (get_stats()->get_num_succeeded() +
															 get_stats()->get_num_failed() +
															 get_stats()->get_num_cancelled()));

	if (all_tasks_resolved)
	{
		/* If the JobEngine job exists, is it finished? */
		struct isi_error *error = NULL;
		job_engine_job_finished = !job_engine_job_is_running(&error);
		if (error != NULL)
		{
			job_engine_job_finished = false;
			ilog(IL_INFO, "job engine job %u (CloudPools job: %u) "
						  "seen as not finished, "
						  "job engine job state check failed: %#{}",
				 job_engine_job_id_, daemon_job_id_,
				 isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}
	}

	return job_engine_job_finished && all_tasks_resolved;
}

void daemon_job_record::fmt_conv(struct fmt *fmt,
								 const struct fmt_conv_args *args) const
{
	ASSERT(fmt != NULL);
	ASSERT(args != NULL);

	fmt_print(fmt, "{");
	fmt_print(fmt, "\"version_\" : %d, ", version_);
	fmt_print(fmt, "\"daemon_job_id_\" : %u, ", daemon_job_id_);
	fmt_print(fmt, "\"operation_type_\" : \"%{}\", ",
			  task_type_fmt(operation_type_->get_type()));
	fmt_print(fmt, "\"creation_time_\" : %lu, ", creation_time_);
	fmt_print(fmt, "\"latest_modification_time_\" : %lu, ",
			  latest_modification_time_);
	fmt_print(fmt, "\"completion_time_\" : %lu, ", completion_time_);
	fmt_print(fmt, "\"state_\" : \"%{}\", ",
			  djob_op_job_task_state_fmt(state_));
	fmt_print(fmt, "\"job_engine_job_id_\" : %u, ", job_engine_job_id_);
	fmt_print(fmt, "\"job_engine_job_state_\" : \"%#{}\", ",
			  enum_jobstat_state_fmt(job_engine_job_state_));
	fmt_print(fmt, "\"stats_\" : %{}, ", cpool_djob_stats_fmt(&stats_));
	fmt_print(fmt, "\"description_\" : \"%s\"",
			  description_ == NULL ? "<null>" : description_);
	fmt_print(fmt, "}");
}
