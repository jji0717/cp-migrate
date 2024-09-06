#include <errno.h>

#include <isi_util/isi_error.h>

#include <isi_cloud_common/isi_cpool_version.h>

#include "operation_type.h"

#include "daemon_job_member.h"
#include "cpool_d_common.h"
#include "task_key.h"

#define MAX_DAEMON_JOB_FILENAME_LENGTH	512

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

daemon_job_member::daemon_job_member()
	: version_(CPOOL_DAEMON_JOB_MEMBER_VERSION), key_(NULL)
	, final_state_(DJOB_OJT_NONE), name_(NULL)
	, max_name_size_(MAX_DAEMON_JOB_FILENAME_LENGTH)
	, serialization_(NULL), serialization_size_(0)
	, serialization_up_to_date_(false), saved_serialization_(NULL)
	, saved_serialization_size_(0)
{
}

daemon_job_member::daemon_job_member(const task_key *key)
	: version_(CPOOL_DAEMON_JOB_MEMBER_VERSION), final_state_(DJOB_OJT_NONE)
	, name_(NULL), max_name_size_(MAX_DAEMON_JOB_FILENAME_LENGTH)
	, serialization_(NULL), serialization_size_(0)
	, serialization_up_to_date_(false), saved_serialization_(NULL)
	, saved_serialization_size_(0)
{
	ASSERT(key != NULL);

	key_ = key->clone();
}

daemon_job_member::~daemon_job_member()
{
	if (key_ != NULL)
		delete key_;

	if (name_ != NULL)
		delete [] name_;

	free(serialization_);
	free(saved_serialization_);
}

void
daemon_job_member::serialize(void) const
{
	free(serialization_);

	size_t name_length = 0;
	if (name_ != NULL)
		name_length = strlen(name_);

	serialization_size_ =
	    sizeof version_ +
	    sizeof name_length +
	    name_length +
	    sizeof final_state_;

	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	/* pack advances the destination pointer, so use a copy. */
	void *temp = serialization_;

	pack(&temp, &version_, sizeof version_);
	pack(&temp, &name_length, sizeof name_length);
	pack(&temp, name_, name_length);
	pack(&temp, &final_state_, sizeof final_state_);

	serialization_up_to_date_ = true;
}

void
daemon_job_member::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (name_ != NULL) {
		delete [] name_;
		name_ = NULL;
	}

	size_t name_length, correct_serialized_size;

	/* unpack advances the destination pointer, so use a copy. */
	void *temp = serialization_;

	unpack(&temp, &version_, sizeof version_);
	if (CPOOL_DAEMON_JOB_MEMBER_VERSION != get_version()) {
		error = isi_system_error_new(EPROGMISMATCH,
		    "Daemon Job member Version mismatch. "
		    "expected version %d found version %d",
		    CPOOL_DAEMON_JOB_MEMBER_VERSION, get_version());
		goto out;
	}

	unpack(&temp, &name_length, sizeof name_length);
	if (name_length > 0) {
		name_ = new char[name_length + 1];
		if (name_ == NULL) {
			error = isi_system_error_new(ENOMEM, "new failed");
			goto out;
		}

		unpack(&temp, name_, name_length);
		name_[name_length] = '\0';
	}
	unpack(&temp, &final_state_, sizeof final_state_);

	correct_serialized_size =
	    sizeof version_ +
	    sizeof name_length +
	    name_length +
	    sizeof final_state_;

	if (correct_serialized_size != serialization_size_) {
		error = isi_system_error_new(EINVAL,
		    "corruption? received %zu, expected %zu",
		    serialization_size_, correct_serialized_size);
		goto out;
	}

	serialization_up_to_date_ = true;

 out:
	isi_error_handle(error, error_out);
}

uint32_t
daemon_job_member::get_version(void) const
{
	return version_;
}

void
daemon_job_member::set_version(uint32_t version)
{
	version_ = version;
}

const task_key *
daemon_job_member::get_key(void) const
{
	return key_;
}

void
daemon_job_member::set_name(const char *new_name)
{
	if (new_name != NULL) {
		if (name_ != NULL)
			delete [] name_;

		if (strlen(new_name) > max_name_size_) {
			name_ = new char[max_name_size_];
			ASSERT(name_ != NULL);
			abbreviate_path(new_name, name_, max_name_size_);

		}
		else {
			name_ = new char[strlen(new_name) + 1];
			ASSERT(name_ != NULL);

			strcpy(name_, new_name);
			name_[strlen(new_name)] = '\0';
		}

		serialization_up_to_date_ = false;
	}
}

const char *
daemon_job_member::get_name(void) const
{
	return name_;
}

void
daemon_job_member::set_final_state(djob_op_job_task_state new_state)
{
	if (final_state_ != new_state) {
		final_state_ = new_state;

		serialization_up_to_date_ = false;
	}
}

djob_op_job_task_state
daemon_job_member::get_final_state(void) const
{
	return final_state_;
}

void
daemon_job_member::get_serialization(const void *&serialization,
    size_t &serialization_size) const
{
	if (!serialization_up_to_date_)
		serialize();

	serialization = serialization_;
	serialization_size = serialization_size_;
}

void
daemon_job_member::set_serialization(const void *serialization,
    size_t serialization_size, struct isi_error **error_out)
{
	ASSERT(serialization != NULL);

	struct isi_error *error = NULL;

	serialization_size_ = serialization_size;

	free(serialization_);
	serialization_ = malloc(serialization_size_);
	if (serialization_ == NULL) {
		error = isi_system_error_new(ENOMEM, "malloc failed");
		goto out;
	}

	memcpy(serialization_, serialization, serialization_size_);

	deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (error != NULL) {
		free(serialization_);
		serialization_ = NULL;

		serialization_size_ = 0;
	}

	isi_error_handle(error, error_out);
}

void
daemon_job_member::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialization_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialization_, serialization_size_);
	saved_serialization_size_ = serialization_size_;
}

void
daemon_job_member::get_saved_serialization(const void *&serialization,
    size_t &serialization_size) const
{
	serialization = saved_serialization_;
	serialization_size = saved_serialization_size_;
}
