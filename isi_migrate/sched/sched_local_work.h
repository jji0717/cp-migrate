#ifndef SCHED_LOCAL_WORK_H
#define SCHED_LOCAL_WORK_H

struct sched_ctx;

struct sched_job {
	enum job_progress progress;
	time_t job_birth;
	char policy_id[MAXPATHLEN + 1];
	uint64_t ino;
	pid_t coord_pid;
	int node_num;
	char current_dir[MAXPATHLEN + 1];
	char available_dir[MAXPATHLEN + 1];
	char local_grabbed_dir[MAXPATHLEN + 1];
	bool local;
	bool restoring;

	/* From policy specification */
	char policy_name[MAXPATHLEN + 1];
	char target[MAXPATHLEN + 1];
	bool is_delete;
	char restrict_by[MAXPATHLEN + 1];
	bool force_interface;
	int priority;
	/* The address to use, if forcing the interface */
	struct inx_addr forced_addr;
	struct siq_policy *policy;
};

/* Used for preempting抢占) running jobs; inteded to represent a job that is
 * currently running (has a coordinator running). */
struct sched_running_job {
	time_t start_time;
	char job_dir[MAXPATHLEN + 1]; /* relative to sched run dir */
	char policy_name[MAXPATHLEN + 1];
	int priority;
};

struct sched_pool {
	char restrict_by[MAXPATHLEN +1];
	int num_jobs;
};

enum sched_action {
	ACTION_SKIP,
	ACTION_GRAB,
	ACTION_ERR
};

enum grab_result {
	GRAB_RESULT_START,
	GRAB_RESULT_ALREADY_STARTED,
	GRAB_RESULT_NO_SPACE,
	GRAB_RESULT_NO_POOL_SPACE,
	GRAB_RESULT_ERROR,
	GRAB_RESULT_KILLED,
};

static const int SCHED_GRABBED_TIMEOUT = 60;

int sched_local_node_work(struct sched_ctx *);

enum sched_ret is_job_grabbed_or_running(const char *policy_id_str);
void delete_or_kill_job(char *policy_id, char *policy_name, 
    struct siq_policy *policy, int node_num, char *err_msg, bool is_delete);
int get_running_jobs_count(struct sched_ctx *sctx);
int get_job_count(struct sched_ctx *sctx, int min_progress);

enum sched_action grabbed_action(struct sched_job *job);
enum sched_action remote_action(struct sched_job *job,
    struct int_set *up_nodes);
enum sched_ret check_race_with_other_node(struct sched_ctx *sctx,
    struct sched_job *job);
enum sched_ret job_make_grabbed(struct sched_ctx *, struct sched_job *job);
enum sched_ret job_make_available(struct sched_job *job);
enum grab_result job_grab(struct sched_ctx *, struct sched_job *job,
    bool cancel);
int coord_test(struct sched_ctx *, struct sched_job *job);

void queue_job(struct ptr_vec *jobs, struct sched_job *job, int max_jobs);
void queue_running_job(struct ptr_vec *running_jobs,
    struct sched_running_job *job, int max_jobs);
int preempt_lower_priority_job(struct sched_ctx *sctx, const char *run_dir,
    int priority, int num_to_preempt, char *restrict_by_pool);

#endif /* SCHED_LOCAL_WORK_H */
