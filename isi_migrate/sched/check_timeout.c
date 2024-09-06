#include "check_needed.h"
#include "sched_local_work.h"
#include "sched_timeout.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(timeout,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown,
    .timeout = 60);

#define FAKE_BASE_DIR "/ifs/fake_sched.XXXXXX"
static char fake_base_dir[sizeof(FAKE_BASE_DIR)];

#define FAKE_POL_ID "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
static char fake_pol_id[sizeof(FAKE_POL_ID)];

static void
make_fake_dirs(const char *fake_base_dir)
{
	fail_if(sched_make_directory(fake_base_dir));
	fail_if(sched_make_directory(sched_get_siq_dir()));
	fail_if(sched_make_directory(sched_get_sched_dir()));
	fail_if(sched_make_directory(sched_get_jobs_dir()));
	fail_if(sched_make_directory(sched_get_run_dir()));
	fail_if(sched_make_directory(sched_get_reports_dir()));
	fail_if(sched_make_directory(sched_get_log_dir()));
	fail_if(sched_make_directory(sched_get_edit_dir()));
}

static void
rm_fake_base_dir(void)
{
	char cmd[MAXPATHLEN + 1];

	SZSPRINTF(cmd, "rm -rf %s", fake_base_dir);
	system(cmd);
}

static void
rm_fake_run_dirs(void)
{
	char cmd[MAXPATHLEN + 1];

	SZSPRINTF(cmd, "rm -rf %s/*", sched_get_run_dir());
	system(cmd);
}

static void
make_fake_pol_id()
{
	strncpy(fake_pol_id, FAKE_POL_ID, sizeof(FAKE_POL_ID));
	mktemp(fake_pol_id);
}

TEST_FIXTURE(suite_teardown)
{
	rm_fake_base_dir();
}

TEST_FIXTURE(test_teardown)
{
	rm_fake_run_dirs();
}

TEST_FIXTURE(test_setup)
{
	rm_fake_run_dirs();

	make_fake_pol_id();
}

TEST_FIXTURE(suite_setup)
{
	// Set up the fake run directory
	strncpy(fake_base_dir, FAKE_BASE_DIR, sizeof(FAKE_BASE_DIR));
	mktemp(fake_base_dir);
	set_fake_dirs(fake_base_dir);
	rm_fake_base_dir();
	make_fake_dirs(fake_base_dir);
}

TEST(test_deferring_paused_job)
{
	struct check_dir job_dir = {};
	struct sched_job job = {};

	/* Create a job in the run dir */
	create_run_dir(&job_dir, sched_get_run_dir(), fake_pol_id, 0, 0);

	/* Add necessary fields to the job struct */
	strncpy(job.policy_id, fake_pol_id, sizeof(FAKE_POL_ID));

	/* Verify an unpaused job can be added */
	fail_unless(add_to_deferred_list(&job, false));
	clear_deferred_list();

	/* Pause the job */
	siq_job_pause(job_dir.policy_id);

	/* Verify a paused job is not added */
	fail_if(add_to_deferred_list(&job, false));

	/* Verify the force flag causes the job to be added */
	fail_unless(add_to_deferred_list(&job, true));
	clear_deferred_list();
}
