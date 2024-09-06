#include <sys/types.h>

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/sched/sched_utils.h"
#include "isi_migrate/sched/siq_atomic_primitives.h"
#include "isi_migrate/sched/siq_glob_param_load.h"
#include "siq_log.h"

#include "start_policy.h"

int
make_job_summary(struct siq_job_summary *summary, enum siq_action_type action)
{
	time_t curr_time, end_time;

	curr_time = time((time_t *)NULL);
	end_time = curr_time - 1000;

	summary->action = action;
	summary->assess = false;
	summary->total->state = SIQ_JS_SCHEDULED;
	summary->total->start = curr_time;
	summary->retry = false;
	summary->total->end = end_time;

	return 0;
}

// Specifying action as SIQ_ACT_NOP in this case uses policy's default action.
static int
create_job_spec_and_report(char *policy_id, char *edit_dir, 
    enum siq_action_type action)
{
	struct siq_job_summary	*summary = NULL;
	struct siq_gc_job_summary *gcs = NULL;
	struct siq_policies	*policies;
	struct siq_policy	*policy;
	char			report_file[MAXPATHLEN+1];
	char			spec_file[MAXPATHLEN+1];
	struct isi_error *isierror = NULL;

	ASSERT(strlen(policy_id) <= POLICY_ID_LEN, "%s (len = %zu)", 
	    policy_id, strlen(policy_id));

	policies = siq_policies_load_readonly(&isierror);

	if (!policies) {
		if (isierror) 
			log(ERROR,"Failed to get policy:%s",
			isi_error_get_message(isierror));
		isi_error_free(isierror);
		return -1;
	}

	policy = siq_policy_get(policies, policy_id, &isierror);
	if (policy == NULL) {
		if (isierror) 
			log(ERROR,"Failed to get policy: %s",
			    isi_error_get_message(isierror));
		isi_error_free(isierror);
		return -1;
	}

	if (action != SIQ_ACT_NOP)
		policy->common->action = action;

	if (policy->common->action == SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK ||
	    policy->common->action == SIQ_ACT_FAILBACK_PREP_RESTORE) {
		FREE_AND_NULL(policy->target->dst_cluster_name);
		policy->target->dst_cluster_name = strdup("localhost");

		FREE_AND_NULL(policy->target->path);
		policy->target->path = strdup(policy->datapath->root_path);
	}

	summary = siq_job_summary_create();
	make_job_summary(summary, policy->common->action);
	sprintf(&report_file[0], "%s/%s", edit_dir, REPORT_FILENAME);
	gcs = siq_job_summary_save_new(summary, report_file, &isierror);
	siq_gc_job_summary_free(gcs);
	if (isierror) {
		log(ERROR, "Failed to save report: %s", 
		    isi_error_get_message(isierror));
		isi_error_free(isierror);
		return -1;
	}

	sprintf(&spec_file[0], "%s/%s", edit_dir, SPEC_FILENAME);
	siq_spec_save(policy, spec_file, &isierror);
	if (isierror) {
		log(ERROR, "Failed to save spec: %s", 
		    isi_error_get_message(isierror));
		isi_error_free(isierror);
		return -1;
	}

	siq_policies_free(policies);

	return 0;
}

static int
start_coordinator(char *run_dir, char *additional_args)
{
	pid_t	coord_pid;
	char coord_dir[MAXPATHLEN + 1];
	int i;
	int ret = -1;
	int status = 0;

	coord_pid = fork();
	if (coord_pid == (pid_t)-1)
		goto out;

	if (coord_pid != 0) {
		/* wait for coord to rename (grab) job dir */
		sprintf(coord_dir, "%s_%d", run_dir, coord_pid);
		//Poll every 100ms for 10s to see if rename has happend
		for (i = 0; i < 100; i++) {
			siq_nanosleep(0, 100000000);
			if (access(coord_dir, F_OK) == 0)
				break;
			//Verify that the coordinator is actually running
			if (waitpid(coord_pid, &status, WNOHANG) != 0)
				return -1;
		}
		ret = coord_pid;
		goto out;
	}

	if (additional_args) {
		execl(COORDNAME, COORDNAME, "-D", run_dir, "-f",
		    additional_args, (char *)NULL);
	} else {
		execl(COORDNAME, COORDNAME, "-D", run_dir, "-f", (char *)NULL);
	}

out:
	return ret;
}

pid_t
start_policy(char *policy_id, char *additional_args)
{
	char			edit_buffer[MAXPATHLEN+1];
	char			work_buffer[MAXPATHLEN+1];
	int			nodenum;
	int			ret;
	pid_t			child_pid = -1;
	struct isi_error *error = NULL;
	bool locked = false;

	nodenum = sched_get_my_node_number();

	sprintf(edit_buffer, "%s/%s", sched_get_edit_dir(), &policy_id[0]);
	sprintf(work_buffer, "%s/%s_%d", sched_get_run_dir(),
	    policy_id, nodenum);

	/* scratch directory so scheduler doesn't pick it up */
	ret = sched_make_directory(edit_buffer);
	if (ret != 0)
		goto out;

	/* SIQ_ACT_NOP == use policy's specified action */
	create_job_spec_and_report(policy_id, edit_buffer, SIQ_ACT_NOP);

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* atomic move from edit to run dir */
	siq_dir_move(edit_buffer, work_buffer);
	
	child_pid = start_coordinator(work_buffer, additional_args);

out:
	isi_error_free(error);
	if (locked)
		siq_job_unlock();
	return child_pid;
}

// Quick start failover/revert for python siq tests.
pid_t
start_failover(char *policy_id, bool revert, char *additional_args)
{
	int child_pid = -1;
	int nodenum = 0;
	struct isi_error *error = NULL;
	bool locked = false;
	char run_dir[MAXPATHLEN + 1];

	nodenum = sched_get_my_node_number();

	sprintf(run_dir, "%s/%s_%d", sched_get_run_dir(), policy_id, nodenum);

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	if (!revert) 
		siq_start_failover_job(policy_id, INFO, 3, true, &error);
	else
		siq_start_failover_revert_job(policy_id, INFO, 3, true, &error);

	if (error)
		goto out;

	child_pid = start_coordinator(run_dir, additional_args);

out:
	if (locked)
		siq_job_unlock();
	if (error)
		isi_error_free(error);

	return child_pid;
}

pid_t
start_failback_prep(char *policy_id, enum siq_action_type action,
    char *additional_args)
{
	char			edit_buffer[MAXPATHLEN+1];
	char			work_buffer[MAXPATHLEN+1];
	int			nodenum;
	int			ret;
	pid_t			child_pid = -1;
	struct isi_error *error = NULL;
	bool locked = false;

	nodenum = sched_get_my_node_number();

	sprintf(edit_buffer, "%s/%s", sched_get_edit_dir(), &policy_id[0]);
	sprintf(work_buffer, "%s/%s_%d", sched_get_run_dir(),
	    policy_id, nodenum);

	/* scratch directory so scheduler doesn't pick it up */
	ret = sched_make_directory(edit_buffer);
	if (ret != 0)
		goto out;

	create_job_spec_and_report(policy_id, edit_buffer,
	    action);

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* atomic move from edit to run dir */
	siq_dir_move(edit_buffer, work_buffer);
	
	child_pid = start_coordinator(work_buffer, additional_args);

out:
	isi_error_free(error);
	if (locked)
		siq_job_unlock();
	return child_pid;
}

pid_t
start_snap_revert(ifs_snapid_t snapid, bool verbose, char *additional_args)
{
	int child_pid = -1;
	struct isi_error *error = NULL;
	int nodenum;
	bool locked = false;
	char run_dir[MAXPATHLEN + 1];
	int status;
	char policy_id[POLICY_ID_LEN + 1];

	/* cleanup in case previous run failed */
	siq_cleanup_snap_revert_job(snapid, &error);
	if (error)
		goto out;
	
	nodenum = sched_get_my_node_number();

	siq_snap_revert_policy_id(snapid, policy_id, &error);
	if (error)
		goto out;
	
	sprintf(run_dir, "%s/%s_%d", sched_get_run_dir(), policy_id, nodenum);

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* get the job ready to run. the job lock prevents the scheduler from
	 * picking it up so we can start the coordinator ourselves */
	siq_start_snap_revert_job(snapid, verbose ? TRACE : INFO, 3,
	    true, &error);
	if (error)
		goto out;

	child_pid = start_coordinator(run_dir, additional_args);
	siq_job_unlock();
	locked = false;

	/* wait for the coordinator pid to exit */
	waitpid(child_pid, &status, 0);

out:
	if (locked)
		siq_job_unlock();
	if (error) {
		printf("%s\n", isi_error_get_message(error));
		isi_error_free(error);
	}
	return child_pid;
}

pid_t
start_domain_mark(char *root, enum domain_type type, bool verbose,
    char *additional_args)
{
	int child_pid = -1;
	struct isi_error *error = NULL;
	int nodenum;
	bool locked = false;
	char run_dir[MAXPATHLEN + 1];
	int status;
	char policy_id[POLICY_ID_LEN + 1];

	/* cleanup in case previous run failed */
	siq_cleanup_domain_mark_job(root, &error);
	if (error)
		goto out;

	nodenum = sched_get_my_node_number();

	siq_domain_mark_policy_id(root, policy_id, &error);
	if (error)
		goto out;

	sprintf(run_dir, "%s/%s_%d", sched_get_run_dir(), policy_id, nodenum);

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* get the job ready to run. the job lock prevents the scheduler from
	 * picking it up so we can start the coordinator ourselves */
	siq_start_domain_mark_job(root, type, verbose ? TRACE : INFO, 3, 
	    true, &error);
	if (error)
		goto out;

	child_pid = start_coordinator(run_dir, additional_args);
	siq_job_unlock();
	locked = false;
	
	waitpid(child_pid, &status, 0);

out:
	if (locked)
		siq_job_unlock();
	if (error) {
		printf("%s\n", isi_error_get_message(error));
		isi_error_free(error);
	}
	return child_pid;
}
