#ifndef __CPOOL_SCOPED_OBJECT_LIST_H__
#define __CPOOL_SCOPED_OBJECT_LIST_H__

#include <list>
#include <set>

namespace isi_cloud {
	class task;
}

class daemon_job_record;
class daemon_job_configuration;
class daemon_job_member;

/*
 * The scoped object list class collects pointers to objects that will be
 * deleted when the scoped list is destroyed.
 */
class scoped_object_list
{
protected:
	std::list<isi_cloud::task *> locked_tasks_;
	std::list<isi_cloud::task *> tasks_;
	std::list<daemon_job_record *> djob_recs_;
	std::list<daemon_job_configuration *> djob_cfgs_;
	std::list<daemon_job_member *> djob_members_;
	std::set<djob_id_t> locked_djob_ids_;
	std::list<int> member_sbt_fds_;
	bool locked_djob_config_;

	/* Copy construction and assignment are currently not implemented. */
	scoped_object_list(const scoped_object_list &);
	const scoped_object_list &operator=(const scoped_object_list &);

public:
	scoped_object_list();
	~scoped_object_list();

	void add_locked_task(isi_cloud::task *t);
	void add_task(isi_cloud::task *t);
	void add_daemon_job_record(daemon_job_record *object);
	void add_daemon_job_config(const daemon_job_configuration *cfg);
	void add_daemon_job_member(daemon_job_member *object);
	void add_locked_daemon_job_record(djob_id_t djob_id);
	void add_daemon_job_member_sbt(int member_sbt_fd);
	void add_locked_daemon_job_configuration(void);
};

#endif // __CPOOL_SCOPED_OBJECT_LIST_H__
