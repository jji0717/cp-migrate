#include <isi_ilog/ilog.h>
#include <isi_util_cpp/isi_exception.h>

#include "cpool_d_common.h"
#include "daemon_job_configuration.h"
#include "daemon_job_manager.h"
#include "daemon_job_member.h"
#include "daemon_job_record.h"
#include "task.h"
#include "task_map.h"

#include "scoped_object_list.h"

using namespace isi_cloud::daemon_job_manager;

scoped_object_list::scoped_object_list()
	: locked_djob_config_(false)
{
}

scoped_object_list::~scoped_object_list()
{
	std::list<isi_cloud::task *>::const_iterator locked_task_iter =
	    locked_tasks_.begin();
	for(; locked_task_iter != locked_tasks_.end(); ++locked_task_iter) {
		task_map tm((*locked_task_iter)->get_key()->get_op_type());
		tm.unlock_task((*locked_task_iter)->get_key(),
		    isi_error_suppress(IL_ERR));
	}

	std::list<isi_cloud::task *>::iterator task_iter = tasks_.begin();
	for(; task_iter != tasks_.end(); ++task_iter)
		delete *task_iter;

	std::list<daemon_job_record *>::iterator djob_rec_iter =
	    djob_recs_.begin();
	for(; djob_rec_iter != djob_recs_.end(); ++djob_rec_iter)
		delete *djob_rec_iter;

	std::list<daemon_job_configuration *>::iterator djob_cfg_iter =
	    djob_cfgs_.begin();
	for(; djob_cfg_iter != djob_cfgs_.end(); ++djob_cfg_iter)
		delete *djob_cfg_iter;

	std::list<daemon_job_member *>::iterator djob_mem_iter =
	    djob_members_.begin();
	for(; djob_mem_iter != djob_members_.end(); ++djob_mem_iter)
		delete *djob_mem_iter;

	std::set<djob_id_t>::const_iterator djob_id_iter =
	    locked_djob_ids_.begin();
	for (; djob_id_iter != locked_djob_ids_.end(); ++djob_id_iter) {
		unlock_daemon_job_record(*djob_id_iter,
		    isi_error_suppress(IL_NOTICE));
	}

	std::list<int>::iterator sbt_fd_iter = member_sbt_fds_.begin();
	for (; sbt_fd_iter != member_sbt_fds_.end(); ++sbt_fd_iter)
		close_sbt(*sbt_fd_iter);

	if (locked_djob_config_)
		unlock_daemon_job_config(isi_error_suppress(IL_NOTICE));
}

void
scoped_object_list::add_locked_task(isi_cloud::task *t)
{
	ASSERT(t != NULL);
	locked_tasks_.push_back(t);
}

void
scoped_object_list::add_task(isi_cloud::task *t)
{
	ASSERT(t != NULL);
	tasks_.push_back(t);
}

void
scoped_object_list::add_daemon_job_record(daemon_job_record *object)
{
	ASSERT(object != NULL);
	djob_recs_.push_back(object);
}

void
scoped_object_list::add_daemon_job_config(const daemon_job_configuration *cfg)
{
	ASSERT(cfg != NULL);

	daemon_job_configuration *non_const =
	    const_cast<daemon_job_configuration *>(cfg);
	ASSERT(non_const != NULL);

	djob_cfgs_.push_back(non_const);
}

void
scoped_object_list::add_daemon_job_member(daemon_job_member *object)
{
	ASSERT(object != NULL);
	djob_members_.push_back(object);
}

void
scoped_object_list::add_locked_daemon_job_record(djob_id_t djob_id)
{
	ASSERT(djob_id != DJOB_RID_INVALID);

	std::pair< std::set<djob_id_t>::iterator, bool > ret =
	    locked_djob_ids_.insert(djob_id);

	ASSERT(ret.second,
	    "daemon job %d is already in the set of locked daemon jobs",
	    djob_id);
}

void
scoped_object_list::add_daemon_job_member_sbt(int member_sbt_fd)
{
	ASSERT(member_sbt_fd > 0);
	member_sbt_fds_.push_back(member_sbt_fd);
}

void
scoped_object_list::add_locked_daemon_job_configuration(void)
{
	locked_djob_config_ = true;
}
