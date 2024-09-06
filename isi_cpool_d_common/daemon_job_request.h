#ifndef __CPOOL_DAEMON_JOB_REQUEST_H__
#define __CPOOL_DAEMON_JOB_REQUEST_H__

#include<string>
#include <vector>

#include "operation_type.h"

#define MAX_FILES_PER_JOB		10000
#define MAX_DIRECTORIES_PER_JOB		10000

class daemon_job_request
{
protected:
	const cpool_operation_type	*op_type_;
	char				*archive_policy_name_;
	uint32_t			recall_filter_id_;
	std::vector<char *>		filenames_;
	std::vector<char *>		directories_;
	char				*description_;

	/* Copy construction and assignment are not currently implemented. */
	daemon_job_request(const daemon_job_request &);
	daemon_job_request &operator=(const daemon_job_request &);

public:
	daemon_job_request();
	~daemon_job_request();

	const cpool_operation_type *get_type(void) const;
	void set_type(const cpool_operation_type *type);
	void set_type(task_type type);

	const char *get_policy(void) const;
	void set_policy(const char *policy_name);

	uint32_t get_file_filter(void) const;
	void set_file_filter(uint32_t file_filter);

	const std::vector<char *> &get_files(void) const;
	void add_file(const char *filename, struct isi_error **error_out);

	const std::vector<char *> &get_directories(void) const;
	void add_directory(const char *dirname, struct isi_error **error_out);

	const char *get_description(void) const;
	void set_description(const char *description);
};

#endif // __CPOOL_DAEMON_JOB_REQUEST_H__
