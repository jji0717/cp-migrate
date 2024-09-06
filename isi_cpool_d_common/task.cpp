#include <string.h>
#include <stdio.h>
#include <stdexcept>

#include <ifs/ifs_lin_open.h>
#include <isi_ilog/ilog.h>
#include <isi_util/isi_format.h>

#include <isi_cloud_common/isi_cpool_version.h>

#include "cpool_d_common.h"
#include "isi_cloud_object_id.h"

#include "task.h"

using namespace isi_cloud;

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
task_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const isi_cloud::task *t =
	    static_cast<const isi_cloud::task *>(arg.ptr);
	ASSERT(t != NULL);

	t->fmt_conv(fmt, args);
}

struct fmt_conv_ctx
cpool_task_fmt(const isi_cloud::task *t)
{
	struct fmt_conv_ctx ctx = { task_fmt_conv, { ptr: t } };
	return ctx;
}

task::task(const cpool_operation_type *op_type)
	: version_(CPOOL_TASK_VERSION), handling_node_id_(-1), priority_(-1)
	, most_recent_status_type_(PENDING_FIRST_ATTEMPT)
	, most_recent_status_value_(0), num_attempts_(0), state_(DJOB_OJT_NONE)
	, location_(TASK_LOC_NONE), serialized_data_size_(0)
	, serialized_data_(NULL), checkpoint_data_size_(0)
	, checkpoint_data_(NULL), serialization_up_to_date_(false)
	, serialized_task_size_(0), serialized_task_(NULL)
	, saved_serialization_(NULL), saved_serialization_size_(0)
	, op_type_(op_type)
{
	ASSERT(op_type_ != NULL);
	ASSERT(op_type_ != OT_NONE);

	gettimeofday(&latest_state_change_time_, NULL);
	gettimeofday(&latest_location_change_time_, NULL);
	gettimeofday(&wait_until_time_, NULL);
}

task::~task()
{
	if (serialized_data_ != NULL) {
		free(serialized_data_);
		serialized_data_ = NULL;
		serialized_data_size_ = 0;
	}

	if (checkpoint_data_ != NULL) {
		free(checkpoint_data_);
		checkpoint_data_ = NULL;
		checkpoint_data_size_ = 0;
	}

	if (serialized_task_ != NULL) {
		free(serialized_task_);
		serialized_task_ = NULL;
		serialized_task_size_ = 0;
	}

	if (saved_serialization_ != NULL) {
		free(saved_serialization_);
		saved_serialization_ = NULL;
		saved_serialization_size_ = 0;
	}

	daemon_job_ids_.clear();
}

const cpool_operation_type *
task::get_op_type(void) const
{
	return op_type_;
}

int
task::get_version(void) const
{
	return version_;
}

uint32_t
task::get_handling_node_id(void) const
{
	return handling_node_id_;
}

void
task::set_handling_node_id(const uint32_t node_id)
{
	handling_node_id_ =  node_id;

	serialization_up_to_date_ = false;
}

int
task::get_priority(void) const
{
	return priority_;
}

void
task::set_priority(int pri)
{
	priority_ = pri;
	serialization_up_to_date_ = false;
}

void
task::get_most_recent_status(status_type &type, int &value) const
{
	type = most_recent_status_type_;
	value = most_recent_status_value_;
}

void
task::set_most_recent_status(status_type type, int value)
{
	most_recent_status_type_ = type;
	most_recent_status_value_ = value;
	serialization_up_to_date_ = false;
}

int
task::get_num_attempts(void) const
{
	return num_attempts_;
}

void
task::inc_num_attempts(void)
{
	++num_attempts_;
	serialization_up_to_date_ = false;
}

struct timeval
task::get_latest_state_change_time(void) const
{
	return latest_state_change_time_;
}

struct timeval
task::get_latest_location_change_time(void) const
{
	return latest_location_change_time_;
}

struct timeval
task::get_wait_until_time(void) const
{
	return wait_until_time_;
}

void
task::set_wait_until_time(struct timeval in)
{
	wait_until_time_ = in;
	serialization_up_to_date_ = false;
}

const std::set<djob_id_t> &
task::get_daemon_jobs(void) const
{
	return daemon_job_ids_;
}

bool
task::is_member_of(djob_id_t job_id) const
{
	std::set<djob_id_t>::const_iterator iter =
	    daemon_job_ids_.find(job_id);
	return (iter != daemon_job_ids_.end());
}

void
task::add_daemon_job(djob_id_t id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	std::pair<std::set<djob_id_t>::iterator, bool> ret;

	if (daemon_job_ids_.size() == MAX_DAEMON_JOBS_PER_TASK) {
		error = isi_system_error_new(ENOSPC,
		    "failed to add daemon job ID (%d) to task (key: %{}), "
		    "maximum limit of daemon jobs reached (%d)",
		    id, cpool_task_key_fmt(get_key()),
		    MAX_DAEMON_JOBS_PER_TASK);
		goto out;
	}

	ret = daemon_job_ids_.insert(id);
	if (!ret.second) {
		error = isi_system_error_new(EEXIST,
		    "failed to add daemon job ID (%d) to task (key: %{}), "
		    "daemon job ID already exists",
		    id, cpool_task_key_fmt(get_key()));
		goto out;
	}

	serialization_up_to_date_ = false;

 out:
	isi_error_handle(error, error_out);
}

bool
task::remove_daemon_job(djob_id_t id)
{
	return (daemon_job_ids_.erase(id) == 1);
}

djob_op_job_task_state
task::get_state(void) const
{
	return state_;
}

void
task::set_state(djob_op_job_task_state state)
{
	ASSERT(state != DJOB_OJT_NONE);

	state_ = state;
	gettimeofday(&latest_state_change_time_, NULL);
	serialization_up_to_date_ = false;
}

task_location
task::get_location(void) const
{
	return location_;
}

void
task::set_location(task_location location)
{
	ASSERT(location != TASK_LOC_NONE);

	location_ = location;
	gettimeofday(&latest_location_change_time_, NULL);
	serialization_up_to_date_ = false;
}

void
task::get_serialized_data(const void *&out_data, size_t &out_size) const
{
	out_data = serialized_data_;
	out_size = serialized_data_size_;
}

void
task::set_serialized_data(const void *data, size_t size)
{
	if (size > serialized_data_size_) {
		if (serialized_data_size_ != 0)
			free(serialized_data_);
		serialized_data_ = malloc(size);
		ASSERT(serialized_data_ != NULL);
	}

	serialized_data_size_ = size;
	memcpy(serialized_data_, data, serialized_data_size_);
	serialization_up_to_date_ = false;
}

void
task::get_checkpoint_data(const void *&out_data, size_t &out_size) const
{
	out_data = checkpoint_data_;
	out_size = checkpoint_data_size_;
}

void
task::set_checkpoint_data(const void *data, size_t size)
{
	if (size > checkpoint_data_size_) {
		if (checkpoint_data_size_ != 0)
			free(checkpoint_data_);
		checkpoint_data_ = malloc(size);
		ASSERT(checkpoint_data_ != NULL);
	}

	checkpoint_data_size_ = size;
	memcpy(checkpoint_data_, data, checkpoint_data_size_);
	serialization_up_to_date_ = false;
}

void
task::get_serialized_task(const void *&out_task, size_t &out_size) const
{
	if (!serialization_up_to_date_)
		serialize();

	out_task = serialized_task_;
	out_size = serialized_task_size_;
}

void
task::set_serialized_task(const void *task, size_t size,
    struct isi_error **error_out)
{
	ASSERT(task != NULL);

	struct isi_error *error = NULL;

	if (size > serialized_task_size_) {
		if (serialized_task_size_ != 0)
			free(serialized_task_);

		serialized_task_ = malloc(size);
		if (serialized_task_ == NULL) {
			error = isi_system_error_new(ENOMEM, "malloc failed");
			goto out;
		}
	}

	serialized_task_size_ = size;
	memcpy(serialized_task_, task, serialized_task_size_);

	deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	serialization_up_to_date_ = true;

 out:
	if (error != NULL) {
		free(serialized_task_);
		serialized_task_ = NULL;

		serialized_task_size_ = 0;
	}

	isi_error_handle(error, error_out);
}

void
task::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialized_task_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialized_task_, serialized_task_size_);

	saved_serialization_size_ = serialized_task_size_;
}

void
task::get_saved_serialization(const void *&serialization, size_t &size) const
{
	serialization = saved_serialization_;
	size = saved_serialization_size_;
}

void
task::serialize(void) const
{
	free(serialized_task_);

	int checkpoint_size = 0;
	if (has_ckpt_within_task()) {
		checkpoint_size = sizeof checkpoint_data_size_ + 
			checkpoint_data_size_;
	}

	size_t num_daemon_jobs = daemon_job_ids_.size();
	
	serialized_task_size_ =
	    sizeof version_ +
	    sizeof handling_node_id_ +
	    sizeof priority_ +
	    sizeof most_recent_status_type_ +
	    sizeof most_recent_status_value_ +
	    sizeof num_attempts_ +
	    sizeof latest_state_change_time_ +
	    sizeof latest_location_change_time_ +
	    sizeof wait_until_time_ +
	    sizeof num_daemon_jobs +
	    num_daemon_jobs * sizeof(djob_id_t) +
	    sizeof state_ +
	    sizeof location_ +
	    sizeof serialized_data_size_ +
	    serialized_data_size_ +
	    checkpoint_size;

	serialized_task_ = malloc(serialized_task_size_);
	ASSERT(serialized_task_ != NULL);

	// since pack advances the dst pointer, use a copy to maintain
	// the correct serialized_task pointer
	void *temp = serialized_task_;

	pack(&temp, &version_, sizeof version_);
	pack(&temp, &handling_node_id_, sizeof handling_node_id_);
	pack(&temp, &priority_, sizeof priority_);
	pack(&temp, &most_recent_status_type_,
	    sizeof most_recent_status_type_);
	pack(&temp, &most_recent_status_value_,
	    sizeof most_recent_status_value_);
	pack(&temp, &num_attempts_, sizeof num_attempts_);
	pack(&temp, &latest_state_change_time_,
	    sizeof latest_state_change_time_);
	pack(&temp, &latest_location_change_time_,
	    sizeof latest_location_change_time_);
	pack(&temp, &wait_until_time_, sizeof wait_until_time_);
	pack(&temp, &num_daemon_jobs, sizeof num_daemon_jobs);
	std::set<djob_id_t>::const_iterator iter = daemon_job_ids_.begin();
	for (; iter != daemon_job_ids_.end(); ++iter)
		pack(&temp, &(*iter), sizeof (*iter));
	pack(&temp, &state_, sizeof state_);
	pack(&temp, &location_, sizeof location_);
	pack(&temp, &serialized_data_size_, sizeof serialized_data_size_);
	pack(&temp, serialized_data_, serialized_data_size_);
	if (has_ckpt_within_task()) {
		pack(&temp, &checkpoint_data_size_, sizeof checkpoint_data_size_);
		pack(&temp, checkpoint_data_, checkpoint_data_size_);
	}

	serialization_up_to_date_ = true;
}

void
task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int checkpoint_size;
	size_t correct_serialized_size;

	size_t temp_num_daemon_jobs;
	djob_id_t temp_daemon_job_id;
	
	free(serialized_data_);
	serialized_data_ = NULL;

	daemon_job_ids_.clear();

	if (checkpoint_data_ != NULL) {
		free(checkpoint_data_);
		checkpoint_data_ = NULL;
	}

	// since unpack advances the dst pointer, use a copy to maintain
	// the correct serialized_task pointer
	void *temp = serialized_task_;

	unpack(&temp, &version_, sizeof version_);
	if (CPOOL_TASK_VERSION != get_version() &&
		CPOOL_ARCHIVE_TASK_VERSION != get_version()) {
		error = isi_system_error_new(EPROGMISMATCH,
		    "Undefined task version. version %d",
		    get_version());
		goto out;
	}

	unpack(&temp, &handling_node_id_, sizeof handling_node_id_);
	unpack(&temp, &priority_, sizeof priority_);
	unpack(&temp, &most_recent_status_type_,
	    sizeof most_recent_status_type_);
	unpack(&temp, &most_recent_status_value_,
	    sizeof most_recent_status_value_);
	unpack(&temp, &num_attempts_, sizeof num_attempts_);
	unpack(&temp, &latest_state_change_time_,
	    sizeof latest_state_change_time_);
	unpack(&temp, &latest_location_change_time_,
	    sizeof latest_location_change_time_);
	unpack(&temp, &wait_until_time_, sizeof wait_until_time_);
	unpack(&temp, &temp_num_daemon_jobs, sizeof temp_num_daemon_jobs);
	for (size_t i = 0; i < temp_num_daemon_jobs; ++i) {
		unpack(&temp, &temp_daemon_job_id, sizeof temp_daemon_job_id);
		add_daemon_job(temp_daemon_job_id, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	unpack(&temp, &state_, sizeof state_);
	unpack(&temp, &location_, sizeof location_);

	// we need to know the size of the serialized data before we
	// can deserialize, since we need to allocate
	unpack(&temp, &serialized_data_size_, sizeof serialized_data_size_);
	if (serialized_data_size_ > 0) {
		serialized_data_ = malloc(serialized_data_size_);
		if (serialized_data_ == NULL) {
			error = isi_system_error_new(ENOMEM, "malloc failed");
			goto out;
		}

		unpack(&temp, serialized_data_, serialized_data_size_);
	}

	// Read the checkpoint data from SBT if there is no ADS file
	// associated with it.
	if (has_ckpt_within_task()) {
		// same here
		unpack(&temp, &checkpoint_data_size_, sizeof checkpoint_data_size_);
		if (checkpoint_data_size_ > 0) {
			checkpoint_data_ = malloc(checkpoint_data_size_);
			if (checkpoint_data_ == NULL) {
				error = isi_system_error_new(ENOMEM,
				    "malloc failed");
				goto out;
			}

			unpack(&temp, checkpoint_data_, checkpoint_data_size_);
		}
	}
	checkpoint_size = 0;
	if (has_ckpt_within_task()) {
		checkpoint_size = sizeof checkpoint_data_size_ +
		    checkpoint_data_size_;
	}

	// verify that we have a coherent Task by checking the size of the
	// supplied buffer and the size of this Task (which we now know because
	// we have the sizes of the serialized data and checkpoint data)
	correct_serialized_size =
	    sizeof version_ +
	    sizeof handling_node_id_ +
	    sizeof priority_ +
	    sizeof most_recent_status_type_ +
	    sizeof most_recent_status_value_ +
	    sizeof num_attempts_ +
	    sizeof latest_state_change_time_ +
	    sizeof latest_location_change_time_ +
	    sizeof wait_until_time_ +
	    sizeof temp_num_daemon_jobs +
	    temp_num_daemon_jobs * sizeof(djob_id_t) +
	    sizeof state_ +
	    sizeof location_ +
	    sizeof serialized_data_size_ +
	    serialized_data_size_ +
	    checkpoint_size;

	if (serialized_task_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_task_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

task *
task::create_task(const cpool_operation_type *op_type)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);

	task *new_task = NULL;

	if (op_type == OT_ARCHIVE)
		new_task = new isi_cloud::archive_task;
	else if (op_type == OT_RECALL)
		new_task = new isi_cloud::recall_task;
	else if (op_type == OT_LOCAL_GC)
		new_task = new isi_cloud::local_gc_task;
	else if (op_type == OT_CLOUD_GC)
		new_task = new isi_cloud::cloud_gc_task;
	else if (op_type == OT_WRITEBACK)
		new_task = new isi_cloud::writeback_task;
	else if (op_type == OT_CACHE_INVALIDATION)
		new_task = new isi_cloud::cache_invalidation_task;
	else if (op_type == OT_RESTORE_COI)
		new_task = new isi_cloud::restore_coi_task;
	else
		ASSERT(false, "unhandled op_type: %{}",
		    task_type_fmt(op_type->get_type()));

	return new_task;
}

bool
task::is_done_processing(void) const
{
	return most_recent_status_type_ == SUCCESS;
}

archive_task::archive_task()
	: task(OT_ARCHIVE), key_(OT_ARCHIVE), policy_name_(NULL)
	, policy_id_(CPOOL_INVALID_POLICY_ID)
{
	version_ = determine_archive_task_version();
}

archive_task::archive_task(ifs_lin_t lin, ifs_snapid_t snapid)
	: task(OT_ARCHIVE), key_(OT_ARCHIVE, lin, snapid), policy_name_(NULL)
	, policy_id_(CPOOL_INVALID_POLICY_ID)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	version_ = determine_archive_task_version();
}

archive_task::~archive_task()
{
	if (policy_name_ != NULL) {
		free(policy_name_);
		policy_name_ = NULL;
	}
}

bool
archive_task::has_policyid(void) const
{
	if (version_ >= CPOOL_ARCHIVE_TASK_VERSION) {
		return true;
	} else {
		return false;
	}
}

void
archive_task::serialize(void) const
{
	free(serialized_data_);

	size_t policy_name_length = 0;

	if (has_policyid()) {
		ASSERT(policy_id_ != CPOOL_INVALID_POLICY_ID);
		serialized_data_size_ =
		    key_.get_packed_size() +
		    sizeof(uint32_t);
	} else {
		ASSERT(policy_name_ != NULL);
		policy_name_length = strlen(policy_name_);
		serialized_data_size_ =
		    key_.get_packed_size() +
		    sizeof policy_name_length +
		    policy_name_length;
	}

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);

	if (has_policyid()) {
		pack(&temp, &policy_id_, sizeof(uint32_t));
	} else {
		pack(&temp, &policy_name_length, sizeof policy_name_length);
		pack(&temp, policy_name_, policy_name_length);
	}

	task::serialize();
}

void
archive_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	free(policy_name_);
	policy_name_ = NULL;
	policy_id_ = CPOOL_INVALID_POLICY_ID;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;
	size_t policy_name_length;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (has_policyid()) {
		unpack(&temp, &policy_id_, sizeof(uint32_t));

		correct_serialized_size =
		    key_.get_packed_size() +
		    sizeof(uint32_t);
	} else {
		unpack(&temp, &policy_name_length, sizeof policy_name_length);
		if (policy_name_length > 0) {
			policy_name_ = (char *)malloc(policy_name_length + 1);
			if (policy_name_ == NULL) {
				error = isi_system_error_new(ENOMEM, "malloc failed");
				goto out;
			}

			unpack(&temp, policy_name_, policy_name_length);
			policy_name_[policy_name_length] = '\0';
		}

		correct_serialized_size =
		    key_.get_packed_size() +
		    sizeof policy_name_length +
		    policy_name_length;
	}

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void archive_task::set_policy_id(uint32_t policy_id)
{
	ASSERT(policy_id != CPOOL_INVALID_POLICY_ID);
	if (has_policyid()) {
		policy_id_ = policy_id;
		serialization_up_to_date_ = false;
	} else {
		ASSERT(false, "task version (%d) does not support policy id", version_);
	}
}

void
archive_task::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	key_.set_lin(lin);
	serialization_up_to_date_ = false;
}

void
archive_task::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	key_.set_snapid(snapid);
	serialization_up_to_date_ = false;
}

void
archive_task::set_policy_name(const char *policy_name)
{
	ASSERT(policy_name != NULL);

	/*
	it is not allowed to set policy name in new archive task
	we should rely on policy id
	*/
	if (has_policyid()) {
		ASSERT(false, "task version (%d) does not support policy names", version_);
		return;
	}

	if (policy_name_ != NULL) {
		free(policy_name_);
		policy_name_ = NULL;
	}

	policy_name_ = strdup(policy_name);
	ASSERT(policy_name_ != NULL);

	serialization_up_to_date_ = false;
}

ifs_lin_t
archive_task::get_lin(void) const
{
	return key_.get_lin();
}

ifs_snapid_t
archive_task::get_snapid(void) const
{
	return key_.get_snapid();
}

const char *
archive_task::get_policy_name(void) const
{
	if (has_policyid()) {
		ASSERT(false, "task version (%d) does not support policy names", version_);
		return NULL;
	}
	return policy_name_;
}

uint32_t
archive_task::get_policy_id(void) const
{
	if (has_policyid()) {
		return policy_id_;
	} else {
		ASSERT(false, "task version (%d) does not support policy id", version_);
		return 0;
	}
}

void
archive_task::fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const
{
	if (!has_policyid()) {
		fmt_print(fmt, "%d/[%{}]/%s", version_, cpool_task_key_fmt(get_key()),
			policy_name_);
	} else {
		fmt_print(fmt, "%d/[%{}]/%d", version_, cpool_task_key_fmt(get_key()),
			policy_id_);
	}
}

const task_key *
archive_task::get_key(void) const
{
	return &key_;
}

uint64_t
archive_task::get_identifier(void) const
{
	return key_.get_identifier();
}

recall_task::recall_task()
	: task(OT_RECALL), key_(OT_RECALL), retain_stub_(false)
{
}

recall_task::recall_task(ifs_lin_t lin, ifs_snapid_t snapid)
	: task(OT_RECALL), key_(OT_RECALL, lin, snapid), retain_stub_(false)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);
}

recall_task::~recall_task()
{
}

void
recall_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ =
	    key_.get_packed_size() +
	    sizeof retain_stub_;

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);
	pack(&temp, &retain_stub_, sizeof retain_stub_);

	task::serialize();
}

void
recall_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	unpack(&temp, &retain_stub_, sizeof retain_stub_);

	correct_serialized_size =
	    key_.get_packed_size() +
	    sizeof retain_stub_;

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
recall_task::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	key_.set_lin(lin);
	serialization_up_to_date_ = false;
}

void
recall_task::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	key_.set_snapid(snapid);
	serialization_up_to_date_ = false;
}

void
recall_task::set_retain_stub(bool retain_stub)
{
	retain_stub_ = retain_stub;

	serialization_up_to_date_ = false;
}

ifs_lin_t
recall_task::get_lin(void) const
{
	return key_.get_lin();
}

ifs_snapid_t
recall_task::get_snapid(void) const
{
	return key_.get_snapid();
}

bool
recall_task::get_retain_stub() const
{
	return retain_stub_;
}

void
recall_task::fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "[%{}]/%s", cpool_task_key_fmt(get_key()),
	    retain_stub_ ? "true" : "false");
}

const task_key *
recall_task::get_key(void) const
{
	return &key_;
}

uint64_t
recall_task::get_identifier(void) const
{
	return key_.get_identifier();
}

local_gc_task::local_gc_task()
	: task(OT_LOCAL_GC), key_(OT_LOCAL_GC)
{
	memset(&date_of_death_, 0, sizeof(date_of_death_));
}

local_gc_task::local_gc_task(ifs_lin_t lin, ifs_snapid_t snapid)
	: task(OT_LOCAL_GC), key_(OT_LOCAL_GC, lin, snapid)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);
	date_of_death_.tv_sec = 0;
	date_of_death_.tv_usec = 0;
}

local_gc_task::~local_gc_task()
{
}

void
local_gc_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ =
	    key_.get_packed_size() +
	    object_id_.get_packed_size() +
	    sizeof date_of_death_;

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);
	object_id_.pack(&temp);
	pack(&temp, &date_of_death_, sizeof date_of_death_);

	task::serialize();
}

void
local_gc_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	object_id_.unpack(&temp);
	unpack(&temp, &date_of_death_, sizeof date_of_death_);

	correct_serialized_size =
	    key_.get_packed_size() +
	    object_id_.get_packed_size() +
	    sizeof date_of_death_;

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
local_gc_task::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	key_.set_lin(lin);
	serialization_up_to_date_ = false;
}

void
local_gc_task::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	key_.set_snapid(snapid);
	serialization_up_to_date_ = false;
}

void
local_gc_task::set_object_id(const isi_cloud_object_id &object_id)
{
	object_id_ = object_id;
	serialization_up_to_date_ = false;
}

void
local_gc_task::set_date_of_death(struct timeval date_of_death)
{
	date_of_death_ = date_of_death;
	serialization_up_to_date_ = false;
}

ifs_lin_t
local_gc_task::get_lin(void) const
{
	return key_.get_lin();
}

ifs_snapid_t
local_gc_task::get_snapid(void) const
{
	return key_.get_snapid();
}

const isi_cloud_object_id &
local_gc_task::get_object_id(void) const
{
	return object_id_;
}

struct timeval
local_gc_task::get_date_of_death(void) const
{
	return date_of_death_;
}

void
local_gc_task::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	char timebuf_local[80];
	struct tm timeinfo_local;
	localtime_r(&(date_of_death_.tv_sec), &timeinfo_local);
	strftime(timebuf_local, sizeof(timebuf_local),
	    "%a %b %d %H:%M:%S %Y %Z", &timeinfo_local);

	fmt_print(fmt, "[%{}]/%s/%s",
	    cpool_task_key_fmt(get_key()),
	    object_id_.to_c_string(), timebuf_local);
}

const task_key *
local_gc_task::get_key(void) const
{
	return &key_;
}

uint64_t
local_gc_task::get_identifier(void) const
{
	return key_.get_identifier();
}

cloud_gc_task::cloud_gc_task()
	: task(OT_CLOUD_GC), key_(OT_CLOUD_GC)
{
}

cloud_gc_task::cloud_gc_task(const isi_cloud_object_id &object_id)
	: task(OT_CLOUD_GC), key_(OT_CLOUD_GC, object_id)
{
}

cloud_gc_task::~cloud_gc_task()
{
}

void
cloud_gc_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ = key_.get_packed_size();

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);

	task::serialize();
}

void
cloud_gc_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	correct_serialized_size =
	    key_.get_packed_size();

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
cloud_gc_task::set_cloud_object_id(const isi_cloud_object_id &object_id)
{
	key_.set_object_id(object_id);
	serialization_up_to_date_ = false;
}

const isi_cloud_object_id &
cloud_gc_task::get_cloud_object_id(void) const
{
	return key_.get_object_id();
}

void
cloud_gc_task::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "[%{}]", cpool_task_key_fmt(get_key()));
}

const task_key *
cloud_gc_task::get_key(void) const
{
	return &key_;
}

uint64_t
cloud_gc_task::get_identifier(void) const
{
	return key_.get_identifier();
}

writeback_task::writeback_task()
	: task(OT_WRITEBACK), key_(OT_WRITEBACK, INVALID_LIN, INVALID_SNAPID)
{
}

writeback_task::writeback_task(ifs_lin_t lin, ifs_snapid_t snapid)
	: task(OT_WRITEBACK), key_(OT_WRITEBACK, lin, snapid)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);
}

writeback_task::~writeback_task()
{
}

void
writeback_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ =
	    key_.get_packed_size();

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);

	task::serialize();
}

void
writeback_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	correct_serialized_size =
	    key_.get_packed_size();

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}
 out:
	isi_error_handle(error, error_out);
}

void
writeback_task::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	key_.set_lin(lin);
	serialization_up_to_date_ = false;
}

void
writeback_task::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	key_.set_snapid(snapid);
	serialization_up_to_date_ = false;
}

ifs_lin_t
writeback_task::get_lin(void) const
{
	return key_.get_lin();
}

ifs_snapid_t
writeback_task::get_snapid(void) const
{
	return key_.get_snapid();
}

void
writeback_task::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "[%{}]", cpool_task_key_fmt(get_key()));
}

const task_key *
writeback_task::get_key(void) const
{
	return &key_;
}

uint64_t
writeback_task::get_identifier(void) const
{
	return key_.get_identifier();
}

cache_invalidation_task::cache_invalidation_task()
	: task(OT_CACHE_INVALIDATION), key_(OT_CACHE_INVALIDATION)
{
}

cache_invalidation_task::cache_invalidation_task(ifs_lin_t lin,
    ifs_snapid_t snapid)
	: task(OT_CACHE_INVALIDATION), key_(OT_CACHE_INVALIDATION, lin, snapid)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);
}

cache_invalidation_task::~cache_invalidation_task()
{
}

void
cache_invalidation_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ =
	    key_.get_packed_size();

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);

	task::serialize();
}

void
cache_invalidation_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	correct_serialized_size =
	    key_.get_packed_size();

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
cache_invalidation_task::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	key_.set_lin(lin);
	serialization_up_to_date_ = false;
}

void
cache_invalidation_task::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	key_.set_snapid(snapid);
	serialization_up_to_date_ = false;
}

ifs_lin_t
cache_invalidation_task::get_lin(void) const
{
	return key_.get_lin();
}

ifs_snapid_t
cache_invalidation_task::get_snapid(void) const
{
	return key_.get_snapid();
}

void
cache_invalidation_task::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "[%{}]", cpool_task_key_fmt(get_key()));
}

const task_key *
cache_invalidation_task::get_key(void) const
{
	return &key_;
}

uint64_t
cache_invalidation_task::get_identifier(void) const
{
	return key_.get_identifier();
}

restore_coi_task::restore_coi_task()
	: task(OT_RESTORE_COI), key_(OT_RESTORE_COI)
{
	memset(&dod_, 0, sizeof(dod_));
}

restore_coi_task::restore_coi_task(const task_account_id &account_id)
	: task(OT_RESTORE_COI), key_(OT_RESTORE_COI, account_id)
{
	memset(&dod_, 0, sizeof(dod_));
}

restore_coi_task::~restore_coi_task()
{
}

void
restore_coi_task::serialize(void) const
{
	free(serialized_data_);

	serialized_data_size_ = key_.get_packed_size() + sizeof(dod_);

	serialized_data_ = malloc(serialized_data_size_);
	ASSERT(serialized_data_ != NULL);

	void *temp = serialized_data_;

	key_.pack(&temp);
	::pack(&temp, &dod_, sizeof(dod_));

	task::serialize();
}

void
restore_coi_task::deserialize(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *temp;
	size_t correct_serialized_size;

	task::deserialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	temp = serialized_data_;

	key_.unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	::unpack(&temp, &dod_, sizeof(dod_));

	correct_serialized_size =
	    key_.get_packed_size() + sizeof(dod_);

	if (serialized_data_size_ != correct_serialized_size) {
		error = isi_system_error_new(EINVAL,
		    "received size (%zu) does not match expected size (%zu)",
		    serialized_data_size_, correct_serialized_size);
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
restore_coi_task::set_account_id(const task_account_id &account_id)
{
	key_.set_account_id(account_id);
	serialization_up_to_date_ = false;
}

const task_account_id &
restore_coi_task::get_account_id(void) const
{
	return key_.get_account_id();
}

void
restore_coi_task::set_dod(struct timeval dod)
{
	dod_ = dod;
	serialization_up_to_date_ = false;
}

struct timeval
restore_coi_task::get_dod(void) const
{
	return dod_;
}

void
restore_coi_task::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "[%{}]", cpool_task_key_fmt(get_key()));
}

const task_key *
restore_coi_task::get_key(void) const
{
	return &key_;
}

uint64_t
restore_coi_task::get_identifier(void) const
{
	return key_.get_identifier();
}
