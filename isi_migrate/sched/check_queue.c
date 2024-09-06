#include "check_needed.h"

TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(queue,
    .test_setup = test_setup,
    .test_teardown = test_teardown);

/* Constants to represent a timestamp when a job was started. If the birth
 * time of a job is bigger it means it was started more recently */
#define BIRTH_FIRST	1
#define BIRTH_SECOND	2

#define POLICY_1 "policy_1"
#define POLICY_2 "policy_2"

static struct ptr_vec jobs;

TEST_FIXTURE(test_setup)
{
	pvec_init(&jobs);
}

TEST_FIXTURE(test_teardown)
{
	struct sched_job *job = NULL;

	while (pvec_pop(&jobs, (void **)&job)) {
		free(job);
	}

	pvec_clean(&jobs);
}

static bool
job_in_queue(struct sched_job *job, int *index)
{
	struct sched_job *cur_job = NULL;
	void **job_pp;
	int size, i;
	bool ret = false;

	*index = -1;

	size = pvec_size(&jobs);
	for (i = 0; i < size; i++) {
		job_pp = pvec_element(&jobs, i);
		cur_job = (struct sched_job *)*job_pp;

		if (!strcmp(cur_job->policy_name, job->policy_name)) {
			*index = i;
			ret = true;
			break;
		}
	}

	return ret;
}

static void
create_sched_job(struct sched_job *job, int priority, time_t job_birth,
    char *policy_name)
{
	job->job_birth = job_birth;
	job->priority = priority;
	SZSPRINTF(job->policy_name, "%s", policy_name);
}

TEST(queue_job_basic)
{
	struct sched_job job = {};
	bool queued;
	int index;
	create_sched_job(&job, 0, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job, 5);

	queued = job_in_queue(&job, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Case 1: Queue two non priority jobs
 * Subcases:
 * 1. pol_1 started first.
 * 2. pol_2 started first.
 * 3. Both started at same time.
 */

/* Subcase 1 - pol_1 should be first in queue */
TEST(queue_two_non_priority_1)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 0, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 0, BIRTH_SECOND, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Subcase 2 - pol_2 should be first in queue */
TEST(queue_two_non_priority_2)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, false, BIRTH_SECOND, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, false, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 1);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Subcase 3 - First queued policy should be first in queue */
TEST(queue_two_non_priority_3)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 0, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 0, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Case 2: Queue one priority and one non priority job
 * Subcases:
 * 1. pol_1 started first, pol_2 has priority.
 * 2. pol_1 started first and has priority.
 * 3. pol_2 started first, pol_1 has priority.
 * 4. pol_2 started first and has priority.
 * 5. Both started at same time, pol_1 has priority.
 * 6. Both started at same time, pol_2 has priority.
 * Note: Priority policy should come first in all cases.
 */

/* Subcase 1 - Priority policy should be first in queue */
TEST(queue_two_one_priority_1)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 0, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_SECOND, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 1);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Subcase 2 - Priority policy should be first in queue */
TEST(queue_two_one_priority_2)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 0, BIRTH_SECOND, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Subcase 3 - Priority policy should be first in queue */
TEST(queue_two_one_priority_3)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_SECOND, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 0, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Subcase 4 - Priority policy should be first in queue */
TEST(queue_two_one_priority_4)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 0, BIRTH_SECOND, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 1);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Subcase 5 - Priority policy should be first in queue */
TEST(queue_two_one_priority_5)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 0, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Subcase 6 - Priority policy should be first in queue */
TEST(queue_two_one_priority_6)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 0, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 1);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Case 3: Queue two priority jobs
 * Subcases:
 * 1. pol_1 started first.
 * 2. pol_2 started first.
 * 3. Both started at same time.
 */

/* Subcase 1 - Earliest birth time should be first in queue */
TEST(queue_two_priority_1)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_SECOND, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Subcase 2 - Earliest birth time should be first in queue */
TEST(queue_two_priority_2)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_SECOND, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 1);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
}

/* Subcase 3 - First policy queued should be first in queue */
TEST(queue_two_priority_3)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	bool queued;
	int index;
	create_sched_job(&job_1, 1, BIRTH_FIRST, POLICY_1);
	queue_job(&jobs, &job_1, 5);

	create_sched_job(&job_2, 1, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	
	queued = job_in_queue(&job_1, &index);
	fail_unless(queued);
	fail_unless(index == 0);


	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 1);
}

/* Make sure queue size does not exceed max queue size */
TEST(queue_max_jobs)
{
	struct sched_job job_1 = {};
	struct sched_job job_2 = {};
	struct siq_policy *pol_to_free = NULL;
	bool queued;
	int index, i;

	/* Queue 5 low priority jobs */
	for (i = 0; i < 5; i++) {
		create_sched_job(&job_1, 0, BIRTH_FIRST, POLICY_1);
		queue_job(&jobs, &job_1, 5);
	}

	/* When the next job is queued, the last job currently in the queue
	 * will be popped and freed (so queue size doesn't exceed 5). We need
	 * to free the policy ourselves since the scheduler doesn't free the
	 * job->policy during normal execution (that pointer is tracked and
	 * freed later during normal scheduler execution).*/
	pol_to_free = job_1.policy;

	fail_unless(pvec_size(&jobs) == 5);

	create_sched_job(&job_2, 1, BIRTH_FIRST, POLICY_2);
	queue_job(&jobs, &job_2, 5);

	queued = job_in_queue(&job_2, &index);
	fail_unless(queued);
	fail_unless(index == 0);
	fail_unless(pvec_size(&jobs) == 5);

	siq_policy_free(pol_to_free);
}
