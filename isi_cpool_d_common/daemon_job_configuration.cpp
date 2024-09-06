#include <errno.h>
#include <string.h>

#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_config/cpool_d_config.h>
#include <isi_cpool_config/scoped_cfg.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>

#include "cpool_d_common.h"

#include "daemon_job_configuration.h"

#define DJOB_CFG_MULTI_JOB_ID_BASE		10
CTASSERT(DJOB_CFG_MULTI_JOB_ID_BASE > DJOB_RID_LAST_RESERVED_ID);

int daemon_job_configuration::UNLIMITED_DAEMON_JOBS =
    cpool_d_config_settings::UNLIMITED_DAEMON_JOBS;

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

daemon_job_configuration::daemon_job_configuration()
	: version_(CPOOL_DJOB_CONFIG_VERSION), num_active_daemon_jobs_(0)
	, last_djob_id_(DJOB_CFG_MULTI_JOB_ID_BASE - 1)
	, serialization_up_to_date_(false), serialization_(NULL)
	, serialization_size_(0), saved_serialization_(NULL)
	, saved_serialization_size_(0)
{
	const cpool_operation_type *op_type = NULL;
	OT_FOREACH(op_type)
		operation_states_[op_type->get_type()] = DJOB_OJT_RUNNING;
}

daemon_job_configuration::~daemon_job_configuration()
{
	free(serialization_);
	free(saved_serialization_);
}

djob_id_t
daemon_job_configuration::get_daemon_job_id(void)
{
	return DJOB_RID_CONFIG;
}

djob_id_t
daemon_job_configuration::get_multi_job_id_base(void)
{
	return DJOB_CFG_MULTI_JOB_ID_BASE;
}

int
daemon_job_configuration::get_version(void) const
{
	return version_;
}

int
daemon_job_configuration::get_max_concurrent_daemon_jobs(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int max = -2;

	scoped_cfg_reader reader;
	const cpool_d_config_settings *config_settings =
	    reader.get_cfg(&error);
	ON_ISI_ERROR_GOTO(out, error);

	max = config_settings->get_maximum_concurrent_daemon_jobs();

 out:
	isi_error_handle(error, error_out);

	return max;
}

int
daemon_job_configuration::get_num_active_jobs(void) const
{
	return num_active_daemon_jobs_;
}

void
daemon_job_configuration::increment_num_active_jobs(void)
{
	++num_active_daemon_jobs_;
	serialization_up_to_date_ = false;
}

void
daemon_job_configuration::decrement_num_active_jobs(void)
{
	ASSERT(num_active_daemon_jobs_ >= 1,
	    "decrementing num_active_daemon_jobs "
	    "would result in a negative value");
	--num_active_daemon_jobs_;
	serialization_up_to_date_ = false;
}

djob_id_t
daemon_job_configuration::get_last_used_daemon_job_id(void) const
{
	return last_djob_id_;
}

void
daemon_job_configuration::set_last_used_daemon_job_id(djob_id_t last_used_id)
{
	last_djob_id_ = last_used_id;
	serialization_up_to_date_ = false;
}

djob_op_job_task_state
daemon_job_configuration::get_operation_state(
    const cpool_operation_type *op_type) const
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);

	djob_op_job_task_state state = operation_states_[op_type->get_type()];
	ASSERT(state != DJOB_OJT_NONE);
	ASSERT(state != DJOB_OJT_ERROR);
	ASSERT(state != DJOB_OJT_COMPLETED);

	return state;
}

void
daemon_job_configuration::set_operation_state(
    const cpool_operation_type *op_type, djob_op_job_task_state new_state)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);
	ASSERT(new_state == DJOB_OJT_RUNNING || new_state == DJOB_OJT_PAUSED,
	    "illegal new state: %{}", djob_op_job_task_state_fmt(new_state));

	operation_states_[op_type->get_type()] = new_state;
	serialization_up_to_date_ = false;
}

void
daemon_job_configuration::get_serialization(const void *&serialization,
    size_t &serialization_size) const
{
	if (!serialization_up_to_date_)
		serialize();

	serialization = serialization_;
	serialization_size = serialization_size_;
}

void
daemon_job_configuration::set_serialization(const void *serialization,
    size_t serialization_size, struct isi_error **error_out)
{
	ASSERT(serialization != NULL);

	struct isi_error *error = NULL;

	free(serialization_);

	serialization_size_ = serialization_size;
	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

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
daemon_job_configuration::serialize(void) const
{
	size_t num_ops = OT_NUM_OPS;

	free(serialization_);

	serialization_size_ =
	    sizeof version_ +
	    sizeof num_active_daemon_jobs_ +
	    sizeof last_djob_id_ +
	    sizeof num_ops +
	    num_ops * (cpool_operation_type::get_packed_size() +
	        sizeof operation_states_[0]);

	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	void *temp = serialization_;

	pack(&temp, &version_, sizeof version_);
	pack(&temp, &num_active_daemon_jobs_, sizeof num_active_daemon_jobs_);
	pack(&temp, &last_djob_id_, sizeof last_djob_id_);
	pack(&temp, &num_ops, sizeof num_ops);

	for (size_t i = 0; i < num_ops; ++i) {
		task_type type = cpool_operation_type::op_types[i]->get_type();
		pack(&temp, &type, sizeof type);
		pack(&temp, &operation_states_[i],
		    sizeof operation_states_[i]);
	}

	serialization_up_to_date_ = true;
}

void
daemon_job_configuration::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t num_ops;
	void *temp = serialization_;
	size_t correct_serialized_size;

	unpack(&temp, &version_, sizeof version_);
	if (CPOOL_DJOB_CONFIG_VERSION != get_version()) {
		error = isi_system_error_new(EPROGMISMATCH,
		    "DJOB Config Version mismatch. expected version %d found version %d",
		    CPOOL_DJOB_CONFIG_VERSION, get_version());
		goto out;
	}

	unpack(&temp, &num_active_daemon_jobs_,
	    sizeof num_active_daemon_jobs_);
	unpack(&temp, &last_djob_id_, sizeof last_djob_id_);

	unpack(&temp, &num_ops, sizeof num_ops);
	if (num_ops != OT_NUM_OPS) {
		error = isi_system_error_new(EINVAL,
		    "corruption? unpacked %zu, expected %d",
		    num_ops, OT_NUM_OPS);
		goto out;
	}

	for (size_t i = 0; i < num_ops; ++i) {
		int id;
		unpack(&temp, &id, sizeof id);
		if (id >= OT_NUM_OPS) {
			error = isi_system_error_new(EINVAL,
			    "invalid operation id (%d)", id);
			goto out;
		}

		unpack(&temp, &operation_states_[id],
		    sizeof operation_states_[id]);
	}

	correct_serialized_size =
	    sizeof version_ +
	    sizeof num_active_daemon_jobs_ +
	    sizeof last_djob_id_ +
	    sizeof num_ops +
	    num_ops * (cpool_operation_type::get_packed_size() +
	        sizeof operation_states_[0]);

	if (serialization_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "exp: %zu act: %zu",
		    correct_serialized_size, serialization_size_);
		goto out;
	}

	serialization_up_to_date_ = true;

 out:
	isi_error_handle(error, error_out);
}

void
daemon_job_configuration::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialization_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialization_, serialization_size_);
	saved_serialization_size_ = serialization_size_;
}

void
daemon_job_configuration::get_saved_serialization(const void *&serialization,
    size_t &serialization_size) const
{
	serialization = saved_serialization_;
	serialization_size = saved_serialization_size_;
}
