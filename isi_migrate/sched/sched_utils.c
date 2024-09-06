#include "sched_includes.h"
#include <isi_config/array.h>
#include <isi_config/ifsconfig.h>
#include <isi_flexnet/isi_flexnet.h>
#include <isi_net/isi_nic.h>
#include <isi_net/isi_lni.h>
#include "isi_migrate/migr/restrict.h"
#include "isi_migrate/config/siq_source_types.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/isirep.h"

#define DEFAULT_MAX_WORKERS_PER_NODE	16  /* Assume 4 cores w/ 4 workers per
					     * core */

typedef int (*state_change_func_t)(struct sched_ctx *, enum siq_state);

static int no_act(struct sched_ctx *, enum siq_state old_state);
static int err_act(struct sched_ctx *, enum siq_state old_state);
static int off_act(struct sched_ctx *, enum siq_state old_state);
static int pause_act(struct sched_ctx *, enum siq_state old_state);
static int resume_act(struct sched_ctx *, enum siq_state old_state);
static int remove_file(const char *work_path, const char *remove_path);
static enum sched_ret off_dirs(struct sched_ctx *);
static enum sched_ret dir_cancel(struct sched_ctx *, const char *dir_name);

static state_change_func_t change_func[SIQ_ST_TOTAL][SIQ_ST_TOTAL] =
{
	{
		no_act,		/* SIQ_ST_OFF     -> SIQ_ST_OFF */
		err_act,	/* SIQ_ST_OFF     -> SIQ_ST_ON */
		err_act		/* SIQ_ST_OFF     -> SIQ_ST_PAUSED */
	},
	{
		off_act,	/* SIQ_ST_ON	  -> SIQ_ST_OFF */
		no_act,		/* SIQ_ST_ON      -> SIQ_ST_ON */
		pause_act	/* SIQ_ST_ON	  -> SIQ_ST_PAUSED */
	},
	{
		off_act,	/* SIQ_ST_PAUSED  -> SIQ_ST_OFF */
		resume_act,	/* SIQ_ST_PAUSED  -> SIQ_ST_ON */
		no_act		/* SIQ_ST_PAUSED  -> SIQ_ST_PAUSED */
	}
};

int
sched_remove_dir(struct sched_ctx *sctx, const char *remove_dir,
    const char *work_dir)
{
	int res = -1;

	log(TRACE, "%s %s, %s", __func__, remove_dir, work_dir);
	sched_unlink(sctx, work_dir);
	if (rename(remove_dir, work_dir) == 0) {
		if (sched_unlink(sctx, work_dir) != 0) {
			log(ERROR, "%s: sched_unlink(%s)", __func__, work_dir);
			goto out;
		}
		res = 0;
		goto out;
	} else if (errno == ENOENT) {
		res = 0;
		goto out;
	}
	log(ERROR, "%s: rename(%s, %s) error: %s",
	    __func__, remove_dir, work_dir, strerror(errno));

out:
	return res;
}

bool
sched_check_status(const char *test_job_dir, const char *status_file)
{
	bool		result = false;
	char		path[MAXPATHLEN + 1];
	struct stat	st;

	log(TRACE, "%s %s", __func__, status_file);

	snprintf(path, sizeof(path), "%s/%s/%s", sched_get_run_dir(),
	    test_job_dir, status_file);
	if (stat(path, &st) != 0) {
		//
		//  There's always the possibility that the file doesn't
		//  exist or the directory was removed. Don't report
		//  either case.
		//
		if (errno != ENOENT)
			log(ERROR, "%s: stat(%s) error %s",
			    __func__, path, strerror(errno));
	} else if  (S_ISREG(st.st_mode)) {
		result = true;
	}

	return result;
}

/* @param dir_name - job dir for cancel
 * @return: SCH_YES - cancel OK,
 *          SCH_NO - no cancel (ENOENT),
 *          SCH_ERR - i/o error
 */
enum sched_ret
sched_job_cancel(struct sched_ctx *sctx, const char *dir_name)
{
	char	edit_job_cancel_name[MAXPATHLEN + 1];
	char	job_cancel_name[MAXPATHLEN + 1];
	int	pfd;
	int	ret;

	log(TRACE, "sched_job_cancel: enter %s", dir_name);
	sprintf(edit_job_cancel_name, "%s/%s", sctx->edit_run_dir,
	    SCHED_JOB_CANCEL);
	sprintf(job_cancel_name, "%s/%s/%s", sched_get_run_dir(),
	    dir_name, SCHED_JOB_CANCEL);
	unlink(edit_job_cancel_name);
	pfd = open(edit_job_cancel_name, O_CREAT | O_EXCL | O_RDWR, 0666);
	if (pfd < 0) {
		log(ERROR, "sched_job_cancel: Can not open(%s): %s",
		    edit_job_cancel_name, strerror(errno));
		return SCH_ERR;
	}
	close(pfd);
	ret = rename(edit_job_cancel_name, job_cancel_name);
	if (ret == 0) {
		return SCH_YES;
	}
	if (errno == ENOENT) {
		return SCH_NO;
	}
	log(ERROR, "sched_job_cancel: rename(%s, %s) error %s",
	    edit_job_cancel_name, job_cancel_name, strerror(errno));
	return SCH_ERR;
}

/* @param dir_name - job dir for preemption
 * @return: True if a job was preempted, false if not.
 */
bool
sched_job_preempt(struct sched_ctx *sctx, const char *run_dir,
    const char *job_dir, struct isi_error **error_out)
{
	char	job_preempt_name[MAXPATHLEN + 1];
	int	pfd;
	bool	ret = false;
	struct isi_error *error = NULL;

	log(TRACE, "sched_job_preempt: enter");
	
	error = safe_snprintf(job_preempt_name, sizeof(job_preempt_name),
	    "%s/%s/%s", run_dir, job_dir, SCHED_JOB_PREEMPT);
	if (error)
		goto out;

	/* The combination of O_CREAT and O_EXCL flags causes open to:
	 * 1. Be an atomic operation.
	 * 2. Fail if the file already exists.
	 * Basically, we will fail if the job has already been preempted.
	 */

	pfd = open(job_preempt_name, O_CREAT | O_EXCL | O_RDWR, 0666);
	if (pfd < 0) {
		/* This was already preempted - not an error just a failure */
		if (errno == EEXIST)
			goto out;

		error = isi_system_error_new(errno, "sched_job_preempt: Can "
		    "not open(%s): %s", job_preempt_name, strerror(errno));
		goto out;
	}
	close(pfd);

	ret = true;

out:
	isi_error_handle(error, error_out);
	return ret;
}

/* @return: SUCCESS, FAIL */
int
sched_my_jobs_remove(struct sched_ctx *sctx)
{
	DIR		*dirp;
	struct dirent	*dp;
	char		job_name[MAXPATHLEN + 1];
	char		node_num_str[MAXPATHLEN + 1];
	char		work_buf[MAXPATHLEN + 1];
	char		remove_buf[MAXPATHLEN + 1];
	bool		locked = false;
	int		count;
	int		ret = SUCCESS;
	struct isi_error *error = NULL;

	log(TRACE, "sched_my_jobs_remove: enter");
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "sched_my_jobs_remove: opendir(%s) error %s",
		    sched_get_run_dir(), strerror(errno));
		return FAIL;
	} 
	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;
		
		count = sched_decomp_name(dp->d_name, job_name,
		    node_num_str, NULL);
		if ((count == 2) && (atoi(node_num_str) ==
		    sctx->node_num)) {
			if (locked) {
				siq_job_unlock();
				locked = false;
			}
			locked = siq_job_lock(job_name, &error);
			if (error) {
				log(ERROR, "%s", isi_error_get_message(error));
				isi_error_free(error);
				error = NULL;
				continue;
			}
			sprintf(work_buf, "%s/%s",
			    sctx->edit_run_dir, dp->d_name);
			sprintf(remove_buf, "%s/%s",
			    sched_get_run_dir(), dp->d_name);
			ret = sched_remove_dir(sctx, remove_buf, work_buf);
			if (ret == FAIL)
				continue;
		}
	}
	if (locked)
		siq_job_unlock();
	closedir(dirp);
	return ret;
}

int
sched_check_state(struct sched_ctx *sctx)
{
	enum siq_state	old_state;
	int		ret;

	old_state = sctx->siq_state;
	sctx->siq_state = sched_get_siq_state();
	ret = change_func[old_state][sctx->siq_state](sctx, old_state);
	return ret;
}

int
sched_unlink(struct sched_ctx *sctx, const char *remove_dir)
{
	char	edit_dir[MAXPATHLEN + 8];
	int	ret;

	log(TRACE, "sched_unlink: enter %s", remove_dir);
	sprintf(edit_dir, "rm -rf %s", remove_dir);
	ret = system(edit_dir);
	if (ret != 0) {
		log(ERROR, "sched_unlink(%s) error: %s", remove_dir,
		    strerror(errno));
	}
	return ret;
}

static int
no_act(struct sched_ctx *sctx, enum siq_state old_state)
{
	log(TRACE, "no_act: %d -> %d", old_state, sctx->siq_state);
	return 0;
}

static int
err_act(struct sched_ctx *sctx, enum siq_state old_state)
{
	log(ERROR, "err_act: error %d -> %d", old_state, sctx->siq_state);
	return -1;
}

static int
pause_act(struct sched_ctx *sctx, enum siq_state old_state)
{
	char		job_name[MAXPATHLEN + 1];
	char		pause_name[MAXPATHLEN + 1];
	char		target_name[MAXPATHLEN + 1];
	DIR		*dirp;
	struct dirent	*dp;
	int		ret = SUCCESS;
	int		count;
	int		pause_fd = SUCCESS;

	log(TRACE, "pause_act: %d -> %d", old_state, sctx->siq_state);
	sprintf(pause_name, "%s/%s", sctx->edit_run_dir, SCHED_JOB_PAUSE);
	unlink(pause_name);

	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "pause_act: opendir(%s) error %s",
		    sched_get_run_dir(), strerror(errno));
		return FAIL;
	}

	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;
		
		count = sched_decomp_name(dp->d_name, job_name, NULL, NULL);
		if ((strlen(job_name) == POLICY_ID_LEN &&
		    (dp->d_type == DT_DIR))) {
			pause_fd = open(pause_name, O_CREAT | O_WRONLY, 0666);
			if (pause_fd < 0) {
				log(ERROR, "pause_act: open(%s) error %s",
				    pause_name, strerror(errno));
				ret = FAIL;
				break;
			}
			close(pause_fd);
			sprintf(target_name, "%s/%s/%s",
			    sched_get_run_dir(), dp->d_name, SCHED_JOB_PAUSE);
			ret = siq_file_move(pause_name, target_name);
			if (ret != 0)
				break;
			unlink(pause_name);
		}
	}
	closedir(dirp);
	unlink(pause_name);
	return ret;
}

static int
resume_act(struct sched_ctx *sctx, enum siq_state old_state)
{
	char		job_name[MAXPATHLEN + 1];
	char		pause_name[MAXPATHLEN + 1];
	char		target_name[MAXPATHLEN + 1];
	DIR		*dirp = NULL;
	struct dirent	*dp;
	int res = -1;

	log(TRACE, "%s: %d -> %d", __func__, old_state, sctx->siq_state);

	sprintf(pause_name, "%s/%s", sctx->edit_run_dir, SCHED_JOB_PAUSE);
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "resume_act: opendir(%s) error %s",
		    sched_get_run_dir(), strerror(errno));
		goto out;
	}

	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;
		
		sched_decomp_name(dp->d_name, job_name, NULL, NULL);
		if ((strlen(job_name) == POLICY_ID_LEN &&
		    (dp->d_type == DT_DIR))) {
			sprintf(target_name, "%s/%s/%s",
			    sched_get_run_dir(), dp->d_name, SCHED_JOB_PAUSE);

			if (access(target_name, F_OK) == 0) {
				if (remove_file(pause_name,
				    target_name) != 0) {
					goto out;
				}
			} else if (errno != ENOENT) {
				log(ERROR, "%s: access error: %s", __func__,
				    strerror(errno));
				goto out;
			}
		}
	}
	res = 0;

out:
	if (dirp)
		closedir(dirp);
	return res;
}

static int
remove_file(const char *work_path, const char *remove_path)
{
	int	ret;

	log(TRACE, "remove_file enter");
	unlink(work_path);
	ret = siq_file_move(remove_path, work_path);
	if (ret != 0) {
		log(ERROR, "remove_file: siq_file_move(%s, %s) error %s",
		    work_path, remove_path, strerror(errno));
	}
	unlink(work_path);
	return ret;
}

static int
off_act(struct sched_ctx *sctx, enum siq_state old_state)
{
	int		ret = SUCCESS;
	enum sched_ret	off_ret;
	int		retry = 0; /* WTF?! */

	do {
		off_ret = off_dirs(sctx);
		switch (off_ret) {
			case SCH_YES:
				break;
			case SCH_RETRY:
				if (retry == 0) {
					retry++;
				} else {
					ret = SUCCESS;
					retry = 0;
				}
				break;
			case SCH_ERR:
				ret = FAIL;
				break;
			case SCH_NO:
			default:
				log(ERROR, "off_act: switch(%d) error",
				    off_ret);
				ret = FAIL;
				break;
		}
	} while (retry != 0);

	return ret;
}

/* 
 * @return: SCH_YES - job canceled,
 *          SCH_RETRY - retry opendir,
 *          SCH_ERR - i/o error
 */
static enum sched_ret
off_dirs(struct sched_ctx *sctx)
{
	DIR		*dirp;
	struct dirent	*dp;
	enum sched_ret	rm_ret;
	enum sched_ret	func_ret = SCH_YES;

	log(TRACE, "off_dirs: enter");
	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		log(ERROR, "off_dirs: opendir(%s) error %s",
			sched_get_run_dir(), strerror(errno));
		return SCH_ERR;
	}

	while ((dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		rm_ret = dir_cancel(sctx, dp->d_name);
		switch (rm_ret) {
		case SCH_YES:
		case SCH_RETRY:
		case SCH_ERR:
			func_ret = rm_ret;
			break;
		case SCH_NO:
		default:
			log(ERROR, "off_dirs: switch(%d) error", rm_ret);
			func_ret = SCH_ERR;
			break;
		}
		if (func_ret == SCH_ERR)
			break;
	}
	closedir(dirp);
	return func_ret;
}

/* 
 * @dir_name - test job dir
 * @return: SCH_YES - job canceled,
 *          SCH_RETRY - retry opendir,
 *          SCH_ERR - i/o error
 */
static enum sched_ret
dir_cancel(struct sched_ctx *sctx, const char *dir_name)
{
	char		job_name[MAXPATHLEN + 1];
	char		other_node_num_str[MAXPATHLEN + 1];
	char		policy_id_my_node_id[MAXPATHLEN + 1];
	char		edit_dir[MAXPATHLEN + 1];
	int		count;
	enum sched_ret	cancel_ret;
	enum sched_ret	func_ret = SCH_YES;
	int		ret;

	log(TRACE, "dir_cancel: enter");
	count = sched_decomp_name(dir_name, job_name, other_node_num_str,
	    NULL);
	if (strlen(job_name) != POLICY_ID_LEN) {
		return SCH_YES;
	}
	if (count == 1) {
		sprintf(policy_id_my_node_id, "%s/%s", sched_get_run_dir(),
		    dir_name);
		sprintf(edit_dir, "%s/%s", sctx->edit_run_dir, dir_name);
		sched_unlink(sctx, edit_dir);
		ret = sched_remove_dir(sctx, policy_id_my_node_id, edit_dir);
		if (ret == FAIL) {
			func_ret = SCH_ERR;
		}
		if (errno == ENOENT) {
			return SCH_RETRY;
		}
		return func_ret;
	}
	if (sched_check_status(dir_name, SCHED_JOB_CANCEL)) {
		return SCH_YES;
	}
	cancel_ret = sched_job_cancel(sctx, dir_name);
	switch (cancel_ret) {
		case SCH_YES:
		case SCH_ERR:
			func_ret = cancel_ret;
			break;
		case SCH_NO:
			func_ret = SCH_RETRY;
			break;
		default:
		log(ERROR, "dir_cancel: cancel_ret error (%d)", cancel_ret);
			func_ret = SCH_ERR;
			break;
	}

	return func_ret;
}

time_t
get_next_policy_start_time(struct sched_ctx *sctx,
    struct isi_error **error_out)
{
	struct siq_policy *cur_pol = NULL;
	char *cur_pid = NULL;
	enum siq_job_action job_act = SIQ_JOB_NONE;
	time_t next_start = 0;
	struct siq_source_record *srec = NULL;
	time_t start_time = 0;
	time_t end_time = 0;
	time_t tmp;
	struct isi_error *error = NULL;

	/* find the policy that is due to start soonest */
	SLIST_FOREACH(cur_pol, &sctx->policy_list->root->policy, next) {
		cur_pid = cur_pol->common->pid;

		siq_source_record_free(srec);
		srec = NULL;

		srec = siq_source_record_load(cur_pid, &error);
		if (error) {
			log(ERROR, "Error loading source record for policy %s:"
			    " %s", cur_pid, isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			srec = NULL;
			continue;
		}

		if (siq_source_is_unrunnable(srec))
			continue;

		siq_source_get_sched_times(srec, &start_time, &end_time);

		if (cur_pol->common->state != SIQ_ST_ON &&
		    !siq_source_is_policy_deleted(srec))
			continue;

		siq_source_get_pending_job(srec, &job_act);
		if (job_act != SIQ_JOB_NONE)
			continue;

		if (cur_pol->scheduler->schedule == NULL ||
		    strcmp(cur_pol->scheduler->schedule, "") == 0) {
			continue;
		}

		/* ignore policies that are in the process of starting but the
		 * coordinator hasn't changed the job action yet */
		if (siq_job_exists(cur_pid)) {
			log(DEBUG,
			    "%s: scheduled policy %s already in run directory",
			    __func__, cur_pid);
			continue;
		}

		siq_job_get_next_run(cur_pol, start_time,
		    end_time, /* unused?! */ 0, &tmp);

		if (next_start == 0 || tmp < next_start)
			next_start = tmp;
	}

	siq_source_record_free(srec);
	isi_error_handle(error, error_out);
	return next_start;
}

static int
sched_oneFS_test(struct sched_ctx *sctx)
{
	int     test_fd;
	ssize_t ret_len;
	int     test_value = 123456789;
	static bool no_space_before = false;

	log(TRACE, "sched_oneFS_test: enter");
	test_fd = open(sctx->test_file, O_RDWR);
	if (test_fd == -1) {
		log(ERROR, "sched_oneFS_test: can not open(%s): %s",
		    sctx->test_file, strerror(errno));
		return FAIL;
	}

	ret_len = write(test_fd, &test_value, sizeof(int));
	if (ret_len < 0) {
		/* If cluster is read-only, write will initially fail with
		 * ENOSPC, then fail with EROFS */
		if (errno == ENOSPC || errno == EROFS) {
			if (!no_space_before) {
				log(ERROR, "Source cluster has no storage "
				    "space left or is in the read-only state. "
				    "Can't run policies.");
				no_space_before = true;
			}
		}
		else {
			log(ERROR, "sched_oneFS_test: cannot write %s: %s",
			    sctx->test_file, strerror(errno));
		}
		close(test_fd);
		return FAIL;
	}
	if (no_space_before) {
		no_space_before = false;
		log(ERROR,
		    "Source cluster is now writable and policies can be run");
	}
	if (ret_len != sizeof(int)) {
		log(ERROR, "sched_oneFS_test: can not write(%s):"
			" ret_len(%zd) != sizeof(int)",
			sctx->test_file, ret_len);
		close(test_fd);
		return FAIL;
	}
	close(test_fd);
	return SUCCESS;
}

/* check to see if there is a quorum in the current group.
 * @return 0 if we have quorum, -1 otherwise */
int
sched_quorum_wait(struct sched_ctx *sctx)
{
	struct timeval  timeout;
	int	     quorum_ret;
	int	     rc;
	int res = -1;

	log(TRACE, "%s", __func__);
	timeout.tv_sec = sched_get_quorum_poll_interval();
	timeout.tv_usec = 0;

	while (1) {
		quorum_ret = group_has_quorum();
		if (quorum_ret == -1)
			goto out;

		if (quorum_ret == 1) {
			if (sched_oneFS_test(sctx) == 0) {
				res = 0;
				goto out;
			}
		}

		rc = select(0, NULL, NULL, NULL, &timeout);
	}

out:
	return res;
}

static void
insert_elem(struct ptr_vec *queue, void *elem, size_t elem_size, int index)
{
	void *new_elem = NULL;

	new_elem = calloc(1, elem_size);
	ASSERT(new_elem);
	memcpy(new_elem, elem, elem_size);
	pvec_insert(queue, index, new_elem);
}

/*
 * A generic function to add an element to a ptr_vec. It will enforce a
 * comparator (sorting) function and optional max_elements.
 *
 * PARAMS:
 * queue:        the ptr_vec that the element is added to
 * element:      a pointer to the structure to be added to queue
 * element_size: the size of the structure being added.
 * max_elements: the max size the queue should grow to, or -1 for no limit.
 *               Elements will be freed from the tail end of the queue if this
 *               size is reached.
 * insert_first: a comparator function that is used to determine the order in
 *               which to insert elements. it must accept two void * that can
 *               then be casted to the correct structure inside the function.
 *               the function should return true when e1 should come before e2
 *               and false otherwise. If NULL, elements will be added to the
 *               end of the queue.
 *
 * RETURN:
 * Returns true if the element was added to the queue and false otherwise.
 *
 * IMPORTANT:
 * Elements may be freed from the queue if max_elements is reached. This is
 * done using a basic call to free(). Therefore, elements in the queue must
 * be completely free-able by a call to free(), they should not contain
 * fields that have been dynamically allocated.
 */
bool
add_vec_elem(struct ptr_vec *queue, void *element, size_t element_size,
    int max_elements, bool (*insert_first)(void *e1, void *e2))
{
	int queue_size, i;
	bool added = false;
	void *cur_elem = NULL;
	void *del_elem = NULL;
	void **elem_pp;

	ASSERT(queue != NULL);
	ASSERT(element != NULL);

	queue_size = pvec_size(queue);
	for (i = 0; i < queue_size; i++) {
		/* returns a pointer to our desired job pointer */
		elem_pp = pvec_element(queue, i);
		cur_elem = *elem_pp;

		/* See if job belongs in the middle of the queue */
		if (insert_first != NULL &&
		    insert_first(element, cur_elem)) {
			insert_elem(queue, element, element_size, i);
			queue_size++;

			/* If needed, delete the last one */
			if (queue_size > max_elements && max_elements != -1) {
				ASSERT(pvec_pop(queue, (void**)&del_elem));
				free(del_elem);
				queue_size--;
			}

			ASSERT(max_elements == -1 ||
			    queue_size <= max_elements);

			/* Done */
			added = true;
			break;
		}
	}

	if (!added && (queue_size < max_elements || max_elements == -1)) {
		/* Add to the end of the queue */
		insert_elem(queue, element, element_size, queue_size);
		added = true;
	}

	return added;
}

struct sched_node *
sched_get_node_info(struct sched_ctx *sctx, int node_id)
{
	void **nodepp = NULL;

	nodepp = int_map_find(&sctx->node_info, node_id);

	return nodepp ? (struct sched_node *)*nodepp : NULL;
}

static void
clear_node_info(struct sched_ctx *sctx)
{
	int const *devid;
	struct sched_node *node = NULL;
	void **nodepp;

	INT_MAP_FOREACH_NC(devid, nodepp, &sctx->node_info) {
		node = (struct sched_node *)*nodepp;
		free(node);
	}
	int_map_truncate(&sctx->node_info);
}

void
update_node_max_workers(struct sched_ctx *sctx, struct isi_error **error_out)
{
	int res;
	char *path = NULL;
	void **ptr = NULL;
	struct ptr_vec up_nodes = PVEC_INITIALIZER;
	struct node_ids *current = NULL;
	struct sched_node *node = NULL;
	struct isi_error *error = NULL;

	clear_node_info(sctx);

	get_up_node_ids(&up_nodes, &error);
	if (error) {
		log(ERROR, "Failed to get up node info: %s",
		    isi_error_get_message(error));
		goto out;
	}

	PVEC_FOREACH(ptr, &up_nodes) {
		current = *ptr;
		node = calloc(1, sizeof(struct sched_node));
		ASSERT(node != NULL);
		node->lnn = current->lnn;
		node->devid = current->devid;
		asprintf(&path, "%s/%d", SIQ_WORKER_POOL_DIR, node->lnn);
		res = read_lnn_file(path, &node->max_workers);
		if (res == -1)
			node->max_workers = DEFAULT_MAX_WORKERS_PER_NODE;
		int_map_add(&sctx->node_info, node->devid, (void **)&node);
		free(path);
		path = NULL;

	}
out:
	free_and_clean_pvec(&up_nodes);
	isi_error_handle(error, error_out);
}
