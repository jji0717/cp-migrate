#include <sys/stat.h>

#include <unistd.h>

#include <isi_util/isi_assert.h>
#include <isi_util_cpp/isi_exception.h>

#include "daemon_job_request.h"

daemon_job_request::daemon_job_request()
	: op_type_(NULL), archive_policy_name_(NULL), recall_filter_id_(0)
	, description_(NULL)
{
}

daemon_job_request::~daemon_job_request()
{
	free(archive_policy_name_);

	std::vector<char *>::iterator iter;

	iter = filenames_.begin();
	for( ; iter != filenames_.end(); ++iter)
		free(*iter);

	iter = directories_.begin();
	for (; iter != directories_.end(); ++iter)
		free(*iter);

	free(description_);
}

const cpool_operation_type *
daemon_job_request::get_type(void) const
{
	return op_type_;
}

void
daemon_job_request::set_type(const cpool_operation_type *op_type)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);

	op_type_ = op_type;
}

void
daemon_job_request::set_type(task_type type)
{
	OT_FOREACH(op_type_) {
		if (op_type_->get_type() == type)
			break;
	}

	/* Failure to match means op_type_ == NULL. */
	ASSERT(op_type_ != NULL);
}

const char *
daemon_job_request::get_policy(void) const
{
	return archive_policy_name_;
}

void
daemon_job_request::set_policy(const char *policy_name)
{
	ASSERT(policy_name != NULL);

	free(archive_policy_name_);
	archive_policy_name_ = strdup(policy_name);
}

uint32_t
daemon_job_request::get_file_filter(void) const
{
	return recall_filter_id_;
}

void
daemon_job_request::set_file_filter(uint32_t file_filter)
{
	recall_filter_id_ = file_filter;
}

const std::vector<char *> &
daemon_job_request::get_files(void) const
{
	return filenames_;
}

void
daemon_job_request::add_file(const char *filename,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat stat_buf;

	if (filename == NULL || filename[0] == '\0') {
		error = isi_exception_error_new("empty filename");
		goto out;
	}

	if (filenames_.size() == MAX_FILES_PER_JOB) {
		error = isi_exception_error_new(
		    "Maximum number of files exceeded for job: %d",
		    MAX_FILES_PER_JOB);
		goto out;
	}

	if (stat(filename, &stat_buf) != 0) {
		error = isi_system_error_new(errno,
		    "Failed to stat file (\"%s\")", filename);
		goto out;
	}

	if (!S_ISREG(stat_buf.st_mode)) {
		error = isi_exception_error_new("\"%s\" is not a regular file",
		    filename);
		goto out;
	}

	filenames_.push_back(strdup(filename));

 out:
	isi_error_handle(error, error_out);
}

const std::vector<char *> &
daemon_job_request::get_directories(void) const
{
	return directories_;
}

void
daemon_job_request::add_directory(const char *dirname,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat stat_buf;

	if (dirname == NULL || dirname[0] == '\0') {
		error = isi_exception_error_new("empty filename");
		goto out;
	}

	if (directories_.size() == MAX_DIRECTORIES_PER_JOB) {
		error = isi_exception_error_new(
		    "Maximum number of directories exceeded for job: %d",
		    MAX_DIRECTORIES_PER_JOB);
		goto out;
	}

	if (stat(dirname, &stat_buf) != 0) {
		error = isi_system_error_new(errno,
		    "Failed to stat directory (\"%s\")", dirname);
		goto out;
	}

	if (!S_ISDIR(stat_buf.st_mode)) {
		error = isi_exception_error_new("\"%s\" is not a directory",
		    dirname);
		goto out;
	}

	directories_.push_back(strdup(dirname));

 out:
	isi_error_handle(error, error_out);
}

const char *
daemon_job_request::get_description(void) const
{
	return description_;
}

void
daemon_job_request::set_description(const char *description)
{
	ASSERT(description != NULL);

	free(description_);
	description_ = strdup(description);
}

