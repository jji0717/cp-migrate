#include "check_needed.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(job,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .test_setup = test_setup,
    .test_teardown = test_teardown,
    .timeout = 60);

const char *FAKE_BASE_DIR = "/ifs/fake";

struct sched_ctx c_ctx = {};
struct sched_ctx *sctx = &c_ctx;

static void
make_fake_dirs(void)
{
	char cmd[MAXPATHLEN + 1];

	SZSPRINTF(cmd, "mkdir -p %s %s %s %s",
	    sched_get_edit_dir(),
	    sched_get_run_dir(),
	    sched_get_jobs_dir(),
	    sched_get_done_dir());
	system(cmd);
}

static void
rm_fake_base_dir(void)
{
	char cmd[MAXPATHLEN + 1];

	SZSPRINTF(cmd, "rm -rf %s", FAKE_BASE_DIR);
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
clear_grabbed_list(void)
{
	grabbed_list_clear(&g_grabbed_list);
}

TEST_FIXTURE(suite_teardown)
{
	rm_fake_base_dir();
}

TEST_FIXTURE(test_teardown)
{
	rm_fake_run_dirs();
	clear_grabbed_list();
}

TEST_FIXTURE(test_setup)
{
	rm_fake_run_dirs();
	clear_grabbed_list();
}

TEST_FIXTURE(suite_setup)
{
	struct isi_error *ierr = NULL;

	if (UFAIL_POINT_INIT("fake_sched") < 0)
		printf("Failed to init ufp (%s)", strerror(errno));

	// Set up the fake run directory
	set_fake_dirs(FAKE_BASE_DIR);
	rm_fake_base_dir();
	make_fake_dirs();
	
	// Globals that need to be set before running
	sctx->node_num = 1;
	sctx->siq_state = SIQ_ST_ON;

	sched_make_directory(sched_get_siq_dir());
	sched_make_directory(sched_get_sched_dir());
	sched_make_directory(sched_get_jobs_dir());
	sched_make_directory(sched_get_run_dir());
	sched_make_directory(sched_get_done_dir());
	sched_make_directory(sched_get_reports_dir());
	sched_make_directory(sched_get_log_dir());
	sched_make_directory(sched_get_edit_dir());
	sched_make_directory(SIQ_IFS_CONFIG_DIR);
	sprintf(sctx->edit_dir, "%s/%d", sched_get_edit_dir(), sctx->node_num);
	sched_unlink(sctx, sctx->edit_dir);
	sched_make_directory(sctx->edit_dir);
	sprintf(sctx->edit_jobs_dir, "%s/jobs", sctx->edit_dir);
	sched_make_directory(sctx->edit_jobs_dir);
	sprintf(sctx->edit_run_dir, "%s/run", sctx->edit_dir);
	sched_make_directory(sctx->edit_run_dir);
	sprintf(sctx->edit_done_dir, "%s/done", sctx->edit_dir);
	sched_make_directory(sctx->edit_done_dir);
	sctx->job_list.prev = sctx->job_list.next =
	    (struct sched_j_descr *)&sctx->job_list;
}
  
// A job on a down node should be grabbed. A started job on a remote up node
// should be skipped. Not testing here a grabbed job on an up remote node here
// (which involves the grabbed_list).
TEST(get_remote_action_test, .attrs="overnight")
{
	struct int_set INT_SET_INIT_CLEAN(up_nodes);
	enum sched_action act;
	struct sched_job *j1;

	int_set_add(&up_nodes, 1);
	int_set_add(&up_nodes, 2);

	// Create a started job on a remote up node
	j1 = create_job("X", PROGRESS_STARTED, 2, 99, 1);
	act = remote_action(j1, &up_nodes);
	fail_unless(act == ACTION_SKIP);
	free(j1);

	// Create a grabbed job on down node
	j1 = create_job("Y", PROGRESS_GRABBED, 3, 0, 1);
	act = remote_action(j1, &up_nodes);
	fail_unless(act == ACTION_GRAB);
	free(j1);

	// Create a started job on a down node
	j1 = create_job("Z", PROGRESS_STARTED, 3, 99, 1);
	act = remote_action(j1, &up_nodes);
	fail_unless(act == ACTION_GRAB);
	free(j1);
}
 
// If the number of jobs have reached the limit, don't allow a new job to be
// grabbed.
TEST(check_max_jobs_exceeded, .attrs="overnight")
{  
	struct check_dir dir, new_dir; 
	struct sched_job *job;
	int  max = sched_get_max_concurrent_jobs(), j;
	char name[10]; 
 
	// Create the max number of jobs. Create them on nodes other than
	// the local node (1), so the local node doesn't do any tests to see
	// if the jobs are up.
	for (j = 1; j <= max; j++) {  
		SZSPRINTF(name, "%03d", j);
		fail_unless(create_run_dir(&dir, name, 3, j) == 0);
	}  
 
	// Create an AVAILABLE run dir that this node will try to grab
	fail_unless(create_run_dir(&dir, "MJE", 0, 0) == 0);
	job = create_job_from_dir(&dir, 0);
	
	// There should be no space for the new job
	fail_unless(job_grab(sctx, job, false) == GRAB_RESULT_NO_SPACE);

	// And it should be in the available state
	fail_unless(check_run_dir(&dir, &new_dir) == 0); 
	fail_unless(confirm_dir_state(&new_dir, PROGRESS_AVAILABLE, 0) == 0);
	
	free(job);
} 
 
// Test the function is_job_grabbed_or_running()
TEST(check_is_job_grabbed_or_running, .attrs="overnight")
{  
	struct check_dir dir;
	
	// An AVAILABLE job is not grabbed or running
	fail_unless(create_run_dir(&dir, "P", 0, 0) == 0);
	fail_unless(is_job_grabbed_or_running(dir.policy_id) == SCH_NO);
	
	// A GRABBED job is yes
	fail_unless(create_run_dir(&dir, "Q", 1, 0) == 0);
	fail_unless(is_job_grabbed_or_running(dir.policy_id) == SCH_YES);
	
	// A STARTED job is yes
	fail_unless(create_run_dir(&dir, "R", 1, 99) == 0);
	fail_unless(is_job_grabbed_or_running(dir.policy_id) == SCH_YES);
} 
 
// Confirm that the get_running_job_counts() counts the number of jobs
// correctly.
TEST(check_int_get_running_jobs_count, .attrs="overnight")
{ 
	struct check_dir dir;

	// No dirs, count should be zero.
	fail_unless(get_running_jobs_count() == 0);

	// An available dir shouldn't count
	fail_unless(create_run_dir(&dir, "S", 0, 0) == 0);
	fail_unless(get_running_jobs_count() == 0);

	// A grabbed dir should count
	fail_unless(create_run_dir(&dir, "S", 1, 0) == 0);
	fail_unless(get_running_jobs_count() == 1);

	// A started dir should count
	fail_unless(create_run_dir(&dir, "T", 2, 99) == 0);
	fail_unless(get_running_jobs_count() == 2);
} 
 
// In the case with two jobs for the same policy, but held by different nodes,
// the node with lowest ID wins, and the other node's policy gets canceled.
TEST(race_local_wins, .attrs="overnight")
{ 
	struct check_dir dir_local, dir_remote, dirs[2];
	struct sched_job *job;
	int local = 0, remote = 0;

	// Create START dir on local
	fail_unless(create_run_dir(&dir_local, "O", 1, 22) == 0);
	job = create_job(dir_local.policy_id, dir_local.progress, 1, 22, 0);
	
	// Create STARTED dir on other node for same ID
	fail_unless(create_run_dir(&dir_remote, "O", 2, 99) == 0);

	fail_unless(check_race_with_other_node(sctx, job) == SCH_YES);

	// If identical IDs on different nodes, the lowest node ID wins. In
	// this case with the local node as 1, the first dir should remain,
	// and the second should still exist, but have a cancel file in it

	// There should be two directories with same ID
	fail_unless(check_run_dir_array(&dir_local, dirs, 2) == 2);

	// Figure out which is the local and which is the remote
	if (dirs[0].node_id == 1) { 
		local = 0;
		remote = 1;
	} 
	else if (dirs[1].node_id == 1) { 
		local = 1;
		remote = 0;
	} 
	else { 
		printf("\nError find directories:\n");
		print_dir(sched_get_run_dir());
		fail_unless(0);
	} 

	// The AVAILABLE dir should have been STARTED locally
	fail_unless(confirm_dir_state(&dirs[local], PROGRESS_STARTED, 1) == 0);

	// The remote STARTED dir should have been canceled.
	// First check to see if the dir is still there
	fail_unless(confirm_dir_state(&dirs[remote], PROGRESS_STARTED, 2) == 0);

	// Then confirm the existence of the cancel file
	fail_unless(confirm_run_dir_file(&dirs[remote], SCHED_JOB_CANCEL) == 0);

	// And make sure local doesn't have cancel file
	fail_unless(confirm_run_dir_file(&dirs[local], SCHED_JOB_CANCEL) == -1);
	free(job);
} 
 
// In the case with two jobs for the same policy, but held by different nodes,
// the node with the highest ID loses, and its run directory needs to be
// deleted.
TEST(race_remote_wins, .attrs="overnight")
{ 
	struct check_dir dir_local, dir_remote, new_dir;
	struct sched_job *job;

	// Switch from our usual node number of 1
	sctx->node_num = 2;

	// Create GRABBED dir on local
	fail_unless(create_run_dir(&dir_local, "RRW", 2, 0) == 0);
	job = create_job(dir_local.policy_id, dir_local.progress, 2, 0, 0);
	

	// Create STARTED dir on remote node for same ID
	fail_unless(create_run_dir(&dir_remote, "RRW", 1, 99) == 0);

	fail_unless(check_race_with_other_node(sctx, job) == SCH_NO);

	// If identical IDs on different nodes, the lowest node ID wins. In
	// this case with the local node as 2, and the remote as 1, our run
	// directory should be wiped out, and the other remains.

	// Only the remote dir should be there.
	fail_unless(check_run_dir(&dir_remote, &new_dir) == 0);
	fail_unless(confirm_dir_state(&new_dir, PROGRESS_STARTED, 1) == 0);
	fail_unless(confirm_run_dir(&dir_local) == -1);
	 
	// Revert to our usual node number of 1
	sctx->node_num = 1;
	 
	free(job);
} 
 
// Test that job_grab() will grab an available run directory
TEST(check_job_grab, .attrs="overnight")
{ 
	struct check_dir dir, new_dir; 
	struct sched_job *job;
	 
	// Create AVAILABLE dir
	fail_unless(create_run_dir(&dir, "P", 0, 0) == 0);
	job = create_job(dir.policy_id, dir.progress, 0, 0, 0);
 
	fail_unless(job_grab(sctx, job, false) == GRAB_RESULT_START);

	// The AVAILABLE dir should have been GRABBED
	fail_unless(check_run_dir(&dir, &new_dir) == 0); 
	fail_unless(confirm_dir_state(&new_dir, PROGRESS_GRABBED, 1) == 0);
	
	free(job);
} 
 
// Test to see if a job with an existing PID will test for being up
TEST(check_coord_test, .attrs="overnight")
{ 
	struct sched_job *job;

	// Use out check process's pid
	job = create_job("foo", PROGRESS_STARTED, 1, getpid(), 0);
	fail_unless(coord_test(sctx, job) == SUCCESS);
	free(job);
} 
 
// Test job_make_available()
TEST(check_job_make_available, .attrs="overnight")
{ 
	struct check_dir dir, new_dir; 
	struct sched_job *job;
	
	fail_unless(create_run_dir(&dir, "Q", 1, 0) == 0);
	job = create_job_from_dir(&dir, 0);
	fail_unless(job_make_available(job) == SCH_YES);
	fail_unless(check_run_dir(&dir, &new_dir) == 0); 
	fail_unless(confirm_dir_state(&new_dir, PROGRESS_AVAILABLE, 0) == 0);

	free(job);
} 

// Test job_make_grabbed()
TEST(check_job_make_grabbed, .attrs="overnight")
{ 
	struct check_dir dir, new_dir; 
	struct sched_job *job;

	fail_unless(create_run_dir(&dir, "R", 0, 0) == 0);
	job = create_job_from_dir(&dir, 0);
	fail_unless(job_make_grabbed(sctx, job) == SCH_YES);
	fail_unless(check_run_dir(&dir, &new_dir) == 0); 
	fail_unless(confirm_dir_state(&new_dir, PROGRESS_GRABBED, 1) == 0);
	
	free(job);
} 
 
/////
// The tests below here are to test sched_local_work.c:get_grabbed_action() to
// see if jobs stuck in the grabbed stated are properly
// handled. grabbed_action() will return ACTION_SKIP if not stuck in the
// grabbed state, and ACTION_GRAB is stuck in the grabbed state.
//
// These test functions with g_grabbed_list, a global defined in
// sched_local_work.c

// Test that inserting a job in the grabbed list for the first time results in
// an ACTION_SKIP.
TEST(grabbed_insert, .attrs="overnight")
{ 
	enum sched_action act;
	struct sched_job *j1;
 
	j1 = create_job("A", PROGRESS_GRABBED, 1, 0, 1);
	act = grabbed_action(j1);
	fail_unless(act == ACTION_SKIP);
	grabbed_list_clear(&g_grabbed_list);
	free(j1);
} 
 
// Test that checking on a job that's already in a the grabbed_list, but
// before time out results in a ACTION_SKIP.
TEST(grabbed_no_timeout, .attrs="overnight")
{ 
	enum sched_action act;
	struct sched_job *j1;
 
	j1 = create_job("A", PROGRESS_GRABBED, 1, 0, 1);
	act = grabbed_action(j1);
	fail_unless(act == ACTION_SKIP);
 
	age_grabbed_list(SCHED_GRABBED_TIMEOUT - 2);
 
	act = grabbed_action(j1);
	fail_unless(act == ACTION_SKIP);
 
	grabbed_list_clear(&g_grabbed_list);
	free(j1);
} 
 
// Test that checking on a job that's already in a the grabbed_list, but
// before time out results in a ACTION_SKIP.
TEST(grabbed_timeout, .attrs="overnight")
{ 
	enum sched_action act;
	struct sched_job *j1;
 
	j1 = create_job("A", PROGRESS_GRABBED, 1, 0, 1);
	act = grabbed_action(j1);
	fail_unless(act == ACTION_SKIP);
 
	// "sleep" beyond timeout
	age_grabbed_list(SCHED_GRABBED_TIMEOUT + 1);
 
	act = grabbed_action(j1);
	fail_unless(act == ACTION_GRAB);
 
	grabbed_list_clear(&g_grabbed_list);
	free(j1);
} 
 
// Test that checking on a job that's already in a the grabbed_list, but
// before time out results in a ACTION_SKIP.
TEST(grabbed_replace, .attrs="overnight")
{ 
	enum sched_action act;
	struct sched_job *j1, *j2;
 
	j1 = create_job("A", PROGRESS_GRABBED, 1, 0, 1);
	act = grabbed_action(j1);
	fail_unless(act == ACTION_SKIP);
 
	// "sleep" less than timeout
	age_grabbed_list(SCHED_GRABBED_TIMEOUT - 2);
 
	// Create job, duplicate except for inode.
	j2 = create_job("A", PROGRESS_GRABBED, 1, 0, 2);
 
	// j1 should replace j2 in in grabbed_list, and have its timestamp
	// reset. 
	act = grabbed_action(j2);
	fail_unless(act == ACTION_SKIP);
 
	// "sleep" past the original j1's timeout
	age_grabbed_list(10);
 
	// j2 should not timeout
	act = grabbed_action(j2);
	fail_unless(act == ACTION_SKIP);
 
	// "sleep" past j2's timeout
	age_grabbed_list(SCHED_GRABBED_TIMEOUT);
 		
	// j2 should timeout now
	act = grabbed_action(j2);
	fail_unless(act == ACTION_GRAB);
 		
	grabbed_list_clear(&g_grabbed_list);
	free(j1);
	free(j2);
} 
