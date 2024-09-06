#include <unistd.h>
#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "bandwidth.c"

TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(siq_worker_throttle, .test_setup = test_setup,
    .test_teardown = test_teardown);

static struct context wctx = {};
static int next_lnn = 1;

TEST_FIXTURE(test_setup)
{
	memset(&wctx, 0, sizeof(wctx));
	pvec_init(&wctx.jobs);
	pvec_init(&wctx.wp_nodes);
	next_lnn = 1;
	max_wper_cluster = 500;
	max_wper_job = 500;
	max_wper_node = 500;
	wper_cpu = 4;
	max_wpn_per_job = 500;
	num_nodes = 0;
}

TEST_FIXTURE(test_teardown)
{
	struct job *job;
	struct wp_node *node = NULL;
	void **ptr;

	PVEC_FOREACH(ptr, &wctx.jobs) {
		job = *ptr;
		free(job);
	}
	pvec_clean(&wctx.jobs);

	PVEC_FOREACH(ptr, &wctx.wp_nodes) {
		node = *ptr;
		free(node);
	}
	pvec_clean(&wctx.wp_nodes);
}

static struct job *
add_job(struct context *ctx, char *name, char *allowed_nodes, int num_workers)
{
	struct job *job = NULL;
	
	job = calloc(1, sizeof(struct job));
	
	strncpy(job->name, name, strlen(name)+1);
	job->socket = SOCK_LOCAL_OPERATION;
	strncpy(job->possible_nodes, allowed_nodes, SIQ_MAX_NODES + 1);
	strncpy(job->allowed_nodes, allowed_nodes, SIQ_MAX_NODES + 1);
	job->num_workers = num_workers;
	job->ctx = ctx;
	pvec_insert(&ctx->jobs, 0, (void *)job);
	
	return job;
}

static void
add_jobs(struct context *ctx, int num_jobs)
{
	int i;
	char name[MAXNAMELEN];
	char allowed_nodes[SIQ_MAX_NODES + 1];
	int num_nodes = 0;

	num_nodes = pvec_size(&ctx->wp_nodes);
	// Mark node i as not being able to create new workers and job i
	// having 4 * i current workers for testing
	for (i = 0; i < num_jobs; i++) {
		snprintf(name, sizeof(name), "job-%d", i + 1);
		memset(allowed_nodes, '.',
		    sizeof(allowed_nodes) - 1);
		allowed_nodes[i] = 'F';
		allowed_nodes[num_nodes] = '\0';
		add_job(ctx, name, allowed_nodes, 4 * i);
	}
}

static void
clear_wp_nodes(struct context *ctx)
{
	struct wp_node *node = NULL;

	while(pvec_pop(&ctx->wp_nodes, (void **)&node))
		free(node);
	next_lnn = 1;
}

static void
add_wp_node(struct context *ctx, int max_workers)
{
	struct wp_node *new_node = NULL;

	new_node = calloc(1, sizeof(*new_node));
	new_node->lnn = next_lnn++;
	new_node->max_workers = max_workers;
	new_node->rationed_workers = max_workers;
	pvec_append(&ctx->wp_nodes, new_node);
	num_nodes++;
}

static void
update_ration(struct context *ctx, int ration)
{
	ctx->update_node_rations = true;
	ctx->ration = ration;
	remove_excess_workers(ctx);
}

#define CHECK_VALUES(expected, actual) \
    fail_unless((expected) == (actual), "%d != %d", expected, actual)

TEST(worker_check_no_ration, .attrs="overnight")
{
	int max_workers = 16;

	// Add a single node
	add_wp_node(&wctx, max_workers);

	// No limit is set - use 100% max workers
	update_ration(&wctx, -1);
	reallocate_workers(&wctx);
	//printf("total_workers = %d\n", wctx.total_rationed_workers);
	CHECK_VALUES(max_workers, wctx.total_rationed_workers);
	CHECK_VALUES(max_workers, wctx.per_job_ration);

	// No limit is set - use 100% max workers
	update_ration(&wctx, 100);
	reallocate_workers(&wctx);
	CHECK_VALUES(max_workers, wctx.total_rationed_workers);
	CHECK_VALUES(max_workers, wctx.per_job_ration);
}

TEST(worker_check_no_jobs, .attrs="overnight")
{
	int max_workers = 16;

	// Add a single node
	add_wp_node(&wctx, max_workers);

	// 50% of max workers
	update_ration(&wctx, 50);
	reallocate_workers(&wctx);
	CHECK_VALUES((int)(max_workers * 0.5), wctx.total_rationed_workers);
	CHECK_VALUES((int)(max_workers * 0.5), wctx.per_job_ration);
}

TEST(worker_check_multiple_jobs, .attrs="overnight")
{
	int max_workers = 16;

	// Add a single node
	add_wp_node(&wctx, max_workers);

	// Add two jobs
	add_jobs(&wctx, 2);

	// 50% of max workers
	update_ration(&wctx, 50);
	reallocate_workers(&wctx);
	CHECK_VALUES((int)(max_workers * 0.5), wctx.total_rationed_workers);
	CHECK_VALUES((int)(max_workers * 0.25), wctx.per_job_ration);

	clear_wp_nodes(&wctx);
	add_wp_node(&wctx, max_workers);

	// 20% of max workers
	update_ration(&wctx, 20);
	reallocate_workers(&wctx);
	CHECK_VALUES((int)(max_workers * 0.2), wctx.total_rationed_workers);
	CHECK_VALUES((int)(max_workers * 0.1),  wctx.per_job_ration);
}

TEST(worker_check_low_workers, .attrs="overnight")
{
	int max_workers = 4;

	// Add a single node
	add_wp_node(&wctx, max_workers);

	// Add 5 jobs
	add_jobs(&wctx, 5);

	// 75% of max workers
	update_ration(&wctx, 75);
	reallocate_workers(&wctx);
	CHECK_VALUES(5, wctx.total_rationed_workers);
	CHECK_VALUES(1, wctx.per_job_ration);
}

TEST(worker_check_node_mask, .attrs="overnight")
{
	struct job *job = NULL;
	int i = 0, max_workers = 1;
	void **ptr;

	// Add two nodes
	add_wp_node(&wctx, max_workers);
	add_wp_node(&wctx, max_workers);

	// Add 2 jobs
	add_jobs(&wctx, 2);

	// No limit is set - use 100% max workers
	update_ration(&wctx, -1);
	reallocate_workers(&wctx);

	// Check the node mask of each job
	PVEC_FOREACH(ptr, &wctx.jobs) {
		job = *ptr;
		if (i == 0) {
			fail_unless(strncmp(".F", job->allowed_nodes, 2)
			    == 0, "allowed_nodes: %s", job->allowed_nodes);
		} else {
			fail_unless(strncmp("..", job->allowed_nodes, 2)
			    == 0, "allowed_nodes: %s", job->allowed_nodes);
		}
		i++;
	}
}

TEST(worker_check_multiple_nodes, .attrs="overnight")
{
	// Add three nodes
	add_wp_node(&wctx, 12);
	add_wp_node(&wctx, 16);
	add_wp_node(&wctx, 24);

	// Add 3 jobs
	add_jobs(&wctx, 3);

	// 40% of max workers
	update_ration(&wctx, 40);
	reallocate_workers(&wctx);
	CHECK_VALUES(20, wctx.total_rationed_workers);
	CHECK_VALUES(6, wctx.per_job_ration);
}

TEST(worker_check_simple, .attrs="overnight")
{
	char allowed_nodes[SIQ_MAX_NODES + 1];
	
	//Init
	memset(allowed_nodes, '.', sizeof(allowed_nodes) - 1);
	allowed_nodes[3] = '\0';

	//Add three nodes
	add_wp_node(&wctx, 10);
	add_wp_node(&wctx, 10);
	add_wp_node(&wctx, 10);
	
	//Add 1 job
	add_job(&wctx, "one", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(30, wctx.per_job_ration);
	
	//Add 1 job
	add_job(&wctx, "two", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(15, wctx.per_job_ration);
	
	//Add 1 job
	add_job(&wctx, "three", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	
	//Add 1 job
	add_job(&wctx, "four", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(7, wctx.per_job_ration); 
}

TEST(worker_check_simple_node_mask, .attrs="overnight")
{
	char allowed_nodes[SIQ_MAX_NODES + 1];
	struct job *one;
	struct job *two;
	struct job *three;
	
	//Init
	memset(allowed_nodes, '.', sizeof(allowed_nodes) - 1);

	//Add three nodes
	add_wp_node(&wctx, 10);
	add_wp_node(&wctx, 10);
	add_wp_node(&wctx, 10);
	
	//Add 1 job, only run on node 1
	allowed_nodes[1] = 'F';
	allowed_nodes[2] = 'F';
	allowed_nodes[3] = '\0';
	one = add_job(&wctx, "one", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(10, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	
	//Add 1 job, only run on node 2
	one->num_workers = 30;
	memset(allowed_nodes, '.', sizeof(allowed_nodes) - 1);
	allowed_nodes[0] = 'F';
	allowed_nodes[2] = 'F';
	allowed_nodes[3] = '\0';
	two = add_job(&wctx, "two", allowed_nodes, 0);
	
	//Check for correctness
	reallocate_workers(&wctx);
	CHECK_VALUES(20, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, ".F.", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	
	//Change worker counts the other way
	one->num_workers = 1;
	two->num_workers = 29;
	reallocate_workers(&wctx);
	CHECK_VALUES(20, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "F..", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	
	//Both worker counts are under
	one->num_workers = 1;
	two->num_workers = 1;
	reallocate_workers(&wctx);
	CHECK_VALUES(20, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	    
	//Both worker counts are over
	one->num_workers = 50;
	two->num_workers = 50;
	reallocate_workers(&wctx);
	CHECK_VALUES(20, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	    
	//Add 1 job, only run on node 3
	memset(allowed_nodes, '.', sizeof(allowed_nodes) - 1);
	allowed_nodes[0] = 'F';
	allowed_nodes[1] = 'F';
	allowed_nodes[3] = '\0';
	three = add_job(&wctx, "three", allowed_nodes, 0);
	
	//Check for correctness
	one->num_workers = 15;
	two->num_workers = 15;
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "..F", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "..F", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	fail_unless(strncmp(three->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", three->allowed_nodes);
	
	//one and two are under, three is over
	one->num_workers = 5;
	two->num_workers = 5;
	three->num_workers = 20;
	reallocate_workers(&wctx);
	CHECK_VALUES(30, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
	fail_unless(strncmp(one->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", one->allowed_nodes);
	fail_unless(strncmp(two->allowed_nodes, "...", 3) == 0, 
	    "allowed_nodes = %s", two->allowed_nodes);
	fail_unless(strncmp(three->allowed_nodes, "FF.", 3) == 0, 
	    "allowed_nodes = %s", three->allowed_nodes);
	
	//Make sure possible nodes did not change
	fail_unless(strncmp(one->possible_nodes, ".FF", 3) == 0, 
	    "allowed_nodes = %s", one->possible_nodes);
	fail_unless(strncmp(two->possible_nodes, "F.F", 3) == 0, 
	    "allowed_nodes = %s", two->possible_nodes);
	fail_unless(strncmp(three->possible_nodes, "FF.", 3) == 0, 
	    "allowed_nodes = %s", three->possible_nodes);
}

//Memory check disabled since isi_stats has globals/statics
TEST(worker_populate_node_info, .attrs="overnight", .mem_check = CK_MEM_DISABLE)
{
	char allowed_nodes[SIQ_MAX_NODES + 1];

	allowed_nodes[0] = '.';
	allowed_nodes[1] = '\0';
	
	//Add 1 node
	add_wp_node(&wctx, 10);
	
	//Add 1 job
	add_job(&wctx, "one", allowed_nodes, 0);
	
	//Ensure that caching node info works
	uint16_set_init(&(wctx.isi_stats_keys));
	uint16_set_add(&(wctx.isi_stats_keys), isk_node_cpu_count);
	uint16_set_add(&(wctx.isi_stats_keys), isk_node_disk_count);
	wctx.update_node_info = true;
	reallocate_workers(&wctx);
	fail_unless(wctx.update_node_info == false, 
	    "update_node_info == false");
}

TEST(worker_remove_excess_workers, .attrs="overnight")
{
	struct wp_node *node = NULL;
	void **ptr;

	// Add three nodes
	add_wp_node(&wctx, 24);
	add_wp_node(&wctx, 24);
	add_wp_node(&wctx, 24);

	max_wper_node = 10;
	max_wper_cluster = 9;

	// Calls remove_excess_workers
	update_ration(&wctx, 50);

	// We should end up with 3 workers per node:
	// 1. The ration should cut the workers to 12 per node.
	// 2. Then the max_per_node should cut it further to 10 per node.
	// 3. Then max_wper_cluster should cut it further to 3 per node.
	PVEC_FOREACH(ptr, &wctx.wp_nodes) {
		node = *ptr;
		fail_unless(node->rationed_workers == 3);
	}
}

TEST(worker_check_node_mask_single_node, .attrs="overnight")
{
	int max_workers = 16;
	char allowed_nodes[SIQ_MAX_NODES + 1];
	struct job *one;
	struct job *two;
	struct job *three;
	
	//Init
	memset(allowed_nodes, 'F', sizeof(allowed_nodes) - 1);

	// Add three nodes
	add_wp_node(&wctx, max_workers);
	add_wp_node(&wctx, max_workers);
	add_wp_node(&wctx, max_workers);
	
	//Add 2 jobs, only run on nodes 2 and 3
	allowed_nodes[1] = '.';
	allowed_nodes[2] = '.';
	allowed_nodes[3] = '\0';
	one = add_job(&wctx, "one", allowed_nodes, 0);
	two = add_job(&wctx, "two", allowed_nodes, 0);
	
	// No limit is set - use 100% max workers
	wctx.ration = -1;
	reallocate_workers(&wctx);
	CHECK_VALUES(32, wctx.total_rationed_workers);
	CHECK_VALUES(16, wctx.per_job_ration);
	
	//Add 3rd job, also only run on nodes 2 and 3
	three = add_job(&wctx, "three", allowed_nodes, 0);
	
	//Test
	reallocate_workers(&wctx);
	CHECK_VALUES(32, wctx.total_rationed_workers);
	CHECK_VALUES(10, wctx.per_job_ration);
}

