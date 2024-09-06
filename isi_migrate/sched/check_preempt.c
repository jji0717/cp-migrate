#include "check_needed.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(preempt,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

#define POL_ID "12345678912345678912345678912345"
#define POL_ID_2 "22345678912345678912345678912345"
#define POL_ID_3 "32345678912345678912345678912345"
#define POOL_1 "pool1"
#define POOL_2 "pool2"
#define POOL_3 "pool3"
#define NODE_ID 1
#define COORD_PID 1

static struct sched_ctx c_ctx = {};
static struct sched_ctx *sctx = &c_ctx;
static struct siq_policies *policy_list = NULL;

static void
clean_policy_list(void)
{
	struct siq_policy *current = NULL;
	while (!SLIST_EMPTY(&policy_list->root->policy)) {
		current = SLIST_FIRST(&policy_list->root->policy);
		SLIST_REMOVE_HEAD(&policy_list->root->policy, next);
		siq_policy_free(current);
	}
}

TEST_FIXTURE(suite_setup)
{
	char cmd[MAXPATHLEN + 1];
	SZSPRINTF(cmd, "mkdir -p %s", TEST_RUN_DIR);
	system(cmd);

	policy_list = calloc(1, sizeof(*policy_list));
	policy_list->root = calloc(1, sizeof(*(policy_list->root)));
	sctx->policy_list = policy_list;
}

TEST_FIXTURE(suite_teardown)
{
	char cmd[MAXPATHLEN + 1];
	SZSPRINTF(cmd, "rm -rf %s", TEST_RUN_DIR);
	system(cmd);
	free(policy_list->root);
	free(policy_list);
}

TEST_FIXTURE(test_setup)
{
	char cmd[MAXPATHLEN +1];
	SZSPRINTF(cmd, "rm -rf %s/*", TEST_RUN_DIR);
	system(cmd);

	clean_policy_list();
}

TEST_FIXTURE(test_teardown)
{
	char cmd[MAXPATHLEN +1];
	SZSPRINTF(cmd, "rm -rf %s/*", TEST_RUN_DIR);
	system(cmd);

	clean_policy_list();
}

static void
create_pol_and_job(struct check_dir *dir, const char *pol_id, int node_id,
    int coord_pid, int priority, enum siq_action_type act, const char *pool) {
	struct siq_policy *policy = NULL;
	char spec_filename[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	/* Create policy with relevant fields */
	policy = siq_policy_create();
	policy->common->pid = strdup(pol_id);
	policy->common->action = act;
	policy->scheduler->priority = priority;
	if (pool)
		policy->target->restrict_by = strdup(pool);
	SLIST_INSERT_HEAD(&policy_list->root->policy, policy, next);

	/* Create the run dir */
	(void) create_run_dir(dir, TEST_RUN_DIR, pol_id, node_id, coord_pid);

	/* Create spec in run dir */
	snprintf(spec_filename, MAXPATHLEN + 1, "%s/%s", dir->dir,
	    SPEC_FILENAME);
	siq_spec_save(policy, spec_filename, &error);
	fail_unless(error == NULL);
}

static bool
is_preempted(struct check_dir *dir)
{
	char preempt_file[MAXPATHLEN + 1];

	snprintf(preempt_file, MAXPATHLEN + 1, "%s/%s", dir->dir,
	    SCHED_JOB_PREEMPT);
	return !access(preempt_file, F_OK);
}

/* Preempt a job that is valid for preemption */
TEST(test_preempt_valid_job)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create valid policy/job for preemption */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);

	/* Should successfully preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 1);
}

/*
 * NEGATIVE CASES
 */

/* Try to preempt a job that isn't running */
TEST(test_preempt_not_running)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create a valid policy/job, but don't start the job
	 * (progress != PROGRESS_STARTED) */
	create_pol_and_job(&dir, POL_ID, 0, 0, 0, SIQ_ACT_SYNC, NULL);

	/* Should fail to preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 0);
}

/* Try to preempt a job whose policy doesn't exist
 * (preempting during a cancel job) */
TEST(test_preempt_nonexistent_policy)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create a valid policy/job for preemption */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);

	/* Delete the policy we just created, leave the run dir */
	clean_policy_list();

	/* Should fail to preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 0);
}

/* Try to preempt a job that has priority */
TEST(test_preempt_priority_job)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create a policy/job that has priority */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, NULL);

	/* Should fail to preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 0);
}

/* Try to preempt a job that is not running a sync or copy job (doing fofb) */
TEST(test_preempt_failover_job)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create a policy/job that has a fofb action */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_FAILOVER, NULL);

	/* Should fail to preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 0);
}

/* Try to preempt a job that has already been preempted */
TEST(test_preempt_job_twice)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create a policy/job that is valid for preemption */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);

	/* Should successfully preempt the job */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 1);

	/* Try to preempt the job again - should fail */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 1,
	    NULL);
	fail_unless(num_preempted == 0);
}

/* Preempt two jobs at once */
TEST(test_preempt_two_jobs)
{
	int num_preempted = 0;
	struct check_dir dir = {};

	/* Create two policies/jobs that are valid for preemption */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);
	create_pol_and_job(&dir, POL_ID_2, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);

	/* Should successfully preempt both jobs */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 1, 2,
	    NULL);
	fail_unless(num_preempted == 2);
}

/* Preempt two jobs with three available to preempt.
 * Make one policy have a higher priority, so the 2 lower priority jobs should
 * be the ones that are preempted. */
TEST(test_preempt_two_jobs_three_running)
{
	int num_preempted = 0;
	struct check_dir dir = {}, dir2 = {}, dir3 = {};

	/* Create one job that has priority 1 */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, NULL);

	/* Create two jobs with priority 0 */
	create_pol_and_job(&dir2, POL_ID_2, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);
	create_pol_and_job(&dir3, POL_ID_3, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, NULL);

	/* Preempt 2 jobs, all jobs are valid for preemption (priority < 2),
	 * the two lower priority ones should be preempted */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 2, 2,
	    NULL);
	fail_unless(num_preempted == 2);
	fail_unless(!is_preempted(&dir));
	fail_unless(is_preempted(&dir2));
	fail_unless(is_preempted(&dir3));
}

/* Preempt from a specific pool, with no jobs running in that pool.
 * Nothing should get preempted. */
TEST(test_preempt_from_pool_none_running)
{
	int num_preempted = 0;
	struct check_dir dir = {}, dir2 = {}, dir3 = {};

	/* Priority 0 pool 1 */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_1);

	/* Priority 0 pool 1 */
	create_pol_and_job(&dir2, POL_ID_2, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_3);

	/* Priority 1 pool 1 */
	create_pol_and_job(&dir3, POL_ID_3, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, POOL_3);


	/* Preempt from pool 2. No jobs running in that pool, so nothing
	 * should be preempted. */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 2, 1,
	    POOL_2);
	fail_unless(num_preempted == 0);
	fail_unless(!is_preempted(&dir));
	fail_unless(!is_preempted(&dir2));
	fail_unless(!is_preempted(&dir3));
}

/* Preempt from a specific pool.
 * Make 3 jobs in separate pools and preempt from one of the pools. Only the
 * job in the preempted pool should get preempted. */
TEST(test_preempt_from_pool_one_running)
{
	int num_preempted = 0;
	struct check_dir dir = {}, dir2 = {}, dir3 = {};

	/* Priority 0 pool 1 */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_1);

	/* Priority 0 pool 2 */
	create_pol_and_job(&dir2, POL_ID_2, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_2);

	/* Priority 1 pool 3 */
	create_pol_and_job(&dir3, POL_ID_3, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, POOL_3);

	/* Preempt from pool 3. Even though the policy in pool 3 has higher
	 * priority than the other 2 policies, it should be the only one that
	 * is preempted because it's the only one in pool 3. */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 2, 2,
	    POOL_3);
	fail_unless(num_preempted == 1);
	fail_unless(!is_preempted(&dir));
	fail_unless(!is_preempted(&dir2));
	fail_unless(is_preempted(&dir3));
}

/* Preempt one job from a pool with 2 running in the pool. */
TEST(test_preempt_from_pool_two_running)
{
	int num_preempted = 0;
	struct check_dir dir = {}, dir2 = {}, dir3 = {};

	/* Priority 0 pool 1 */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_1);

	/* Priority 0 pool 2 */
	create_pol_and_job(&dir2, POL_ID_2, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_2);

	/* Priority 1 pool 2 */
	create_pol_and_job(&dir3, POL_ID_3, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, POOL_2);


	/* Preempt from pool 2. The lowest priority job in the pool should
	 * be preempted */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 2, 1,
	    POOL_2);
	fail_unless(num_preempted == 1);
	fail_unless(!is_preempted(&dir));
	fail_unless(is_preempted(&dir2));
	fail_unless(!is_preempted(&dir3));
}

/* Preempt a job from any pool with jobs running in different pools. */
TEST(test_preempt_from_any_pool)
{
	int num_preempted = 0;
	struct check_dir dir = {}, dir2 = {}, dir3 = {};

	/* Priority 0 pool 1 */
	create_pol_and_job(&dir, POL_ID, NODE_ID, COORD_PID, 0,
	    SIQ_ACT_SYNC, POOL_1);

	/* Priority 0 pool 2 */
	create_pol_and_job(&dir2, POL_ID_2, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, POOL_2);

	/* Priority 1 pool 3 */
	create_pol_and_job(&dir3, POL_ID_3, NODE_ID, COORD_PID, 1,
	    SIQ_ACT_SYNC, POOL_2);


	/* Preempt from any pool. The lowest priority job across all pools
	 * should be preempted */
	num_preempted = preempt_lower_priority_job(sctx, TEST_RUN_DIR, 2, 1,
	    NULL);
	fail_unless(num_preempted == 1);
	fail_unless(is_preempted(&dir));
	fail_unless(!is_preempted(&dir2));
	fail_unless(!is_preempted(&dir3));
}
