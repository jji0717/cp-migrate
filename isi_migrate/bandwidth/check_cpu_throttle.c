#include <unistd.h>
#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "bandwidth.c"

//TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);
SUITE_DEFINE_FOR_FILE(siq_cpu_throttle, .test_setup = test_setup,
    .test_teardown = test_teardown);

struct context ctx = {};
int dummy_socket = -2;

TEST_FIXTURE(test_setup)
{
	memset(&ctx, 0, sizeof(ctx));
	pvec_init(&ctx.workers);
}

TEST_FIXTURE(test_teardown)
{
	struct worker *worker;
	void **ptr;

	PVEC_FOREACH(ptr, &ctx.workers) {
		worker = *ptr;
		free(worker);
	}
	pvec_clean(&ctx.workers);
}

static void
add_workers(int num_workers)
{
	struct worker *worker = NULL;
	int i = 0;

	for (i = 0; i < num_workers; i++) {
		worker = calloc(1, sizeof(*worker));
		worker->socket = SOCK_LOCAL_OPERATION;
		pvec_insert(&ctx.workers, 0, (void *)worker);
	}
}

TEST(adjust_ration_check_no_ration, .attrs="overnight")
{
	/* No limit is set - adjusted ration should not change */
	ctx.ration = -1;
	adjust_cpu_ration(&ctx, 50);
	fail_unless(ctx.adjusted_ration == 0);

	/* No limit is set - adjusted ration should not change */
	ctx.ration = 100;
	adjust_cpu_ration(&ctx, 50);
	fail_unless(ctx.adjusted_ration == 0);
}

TEST(adjust_ration_check_no_workers, .attrs="overnight")
{
	/* No workers (pvec_size(ctx.workers) == 0) - adjusted ration should
	 * be reset == to ration */
	ctx.ration = 10;
	adjust_cpu_ration(&ctx, 50);
	fail_unless(ctx.adjusted_ration == 10);
}

TEST(adjust_ration_check_close_enough, .attrs="overnight")
{
	/* Actual cpu usage is within 2% of ration, adjusted ration should not
	 * change */
	ctx.ration = 10;
	ctx.adjusted_ration = 10;
	add_workers(3);
	adjust_cpu_ration(&ctx, 11);
	fail_unless(ctx.adjusted_ration == 10);
	adjust_cpu_ration(&ctx, 9);
	fail_unless(ctx.adjusted_ration == 10);
}

TEST(adjust_ration_check_not_close_enough, .attrs="overnight")
{
	/* Actual cpu usage is <= 2% of ration, ration should be
	 * adjusted */
	ctx.ration = 10;
	ctx.adjusted_ration = 10;
	add_workers(3);
	adjust_cpu_ration(&ctx, 8);
	fail_unless(ctx.adjusted_ration != 10);
}

TEST(adjust_ration_check_upper_limit, .attrs="overnight")
{
	/* Adjusted ration should never exceed num_workers * ration (no single
	 * worker should ever get more than the ration). Set up extreme
	 * conditions and make sure we don't over-adjust and give a single
	 * worker a higher ration than is allowed */

	/* Ration is set to 50, we have adjusted the ration to also be 50,
	 * and there is one worker who only used 1% of cpu time. We should
	 * not adjust further and allow the worker to have more than 50. */
	ctx.ration = 50;
	ctx.adjusted_ration = 50;
	add_workers(1);
	adjust_cpu_ration(&ctx, 1);
	fail_unless(ctx.ration == 50);
	fail_unless(ctx.adjusted_ration == 50);
}

TEST(adjust_ration_check_lower_limit, .attrs="overnight")
{
	/* The improbable scenario is that a worker
	 * used much more cpu (90%) than allotted (20%), and a sane response is to lower
	 * the adjusted ration, but make sure it doesn't go lower than the
	 * actual ration (10%) */
	ctx.ration = 10;
	ctx.adjusted_ration = 20;
	add_workers(1);
	adjust_cpu_ration(&ctx, 90);
	fail_unless(ctx.ration == 10);
	fail_unless(ctx.adjusted_ration == 10);
}

TEST(adjust_ration_check_control_case, .attrs="overnight")
{
	/* 5 workers, 50% limit, 50% actual CPU usage = 10% per worker */
	ctx.ration = 50;
	ctx.adjusted_ration = 50;
	add_workers(5);
	adjust_cpu_ration(&ctx, 50);

	/* We expect no change, the usage == ration */
	fail_unless(ctx.adjusted_ration == 50);

	/* Now make sure each worker gets distributed a fair share */
	reallocate_cpu(&ctx);

	/* Per worker ration is cycles per 1000 */
	fail_unless(ctx.per_worker_ration == 100);
}

TEST(adjust_ration_check_low_usage, .attrs="overnight")
{
	/* 10 workers, 10% limit, 5% actual CPU usage = 2% per worker */
	ctx.ration = 10;
	ctx.adjusted_ration = 10;
	add_workers(10);
	adjust_cpu_ration(&ctx, 5);
	fail_unless(ctx.ration == 10);
	fail_unless(ctx.adjusted_ration == 20);

	/* Per worker ration is cycles per 1000 */
	fail_unless(ctx.per_worker_ration == 20);
}

TEST(adjust_ration_check_min_ration_1, .attrs="overnight")
{
	/* 100 workers, 1% limit, 1% actual CPU usage = .1% per worker (minimum
	 * allowed to prevent starvation) */
	ctx.ration = 1;
	ctx.adjusted_ration = 1;
	add_workers(100);
	adjust_cpu_ration(&ctx, 1);

	/* Expect no change, usage == ration */
	fail_unless(ctx.ration == 1);
	fail_unless(ctx.adjusted_ration == 1);

	/* Now make sure each worker gets distributed a fair share */
	reallocate_cpu(&ctx);

	/* Per worker ration is cycles per 1000 */
	fail_unless(ctx.per_worker_ration == 1);
}

TEST(adjust_ration_check_min_ration_2, .attrs="overnight")
{
	/* 100 workers, 1% limit, 0% actual CPU usage = .1% per worker (minimum
	 * allowed to prevent starvation and maximum allowed upwards
	 * adjustment) */
	ctx.ration = 1;
	ctx.adjusted_ration = 1;
	add_workers(100);
	adjust_cpu_ration(&ctx, 0);

	/* Expect no change, usage == ration */
	fail_unless(ctx.ration == 1);
	fail_unless(ctx.adjusted_ration == 1);

	/* Now make sure each worker gets distributed a fair share */
	reallocate_cpu(&ctx);

	/* Per worker ration is cycles per 1000 */
	fail_unless(ctx.per_worker_ration == 1);
}

TEST(adjust_ration_check_invalid_actual_usage, .attrs="overnight")
{
	/* 0 workers, 1% limit, 1% actual CPU usage = No change to any values,
	 * we should respond gracefully by ignoring this strange set of
	 * values */
	ctx.ration = 1;
	ctx.adjusted_ration = 1;
	adjust_cpu_ration(&ctx, 1);

	/* Expect no change, usage == ration */
	fail_unless(ctx.ration == 1);
	fail_unless(ctx.adjusted_ration == 1);

	/* Now make sure reallocation responds sanely */
	reallocate_cpu(&ctx);

	/* No workers, expect per_worker_ration to remain unchanged (it is
	 * initialized to 0). */
	fail_unless(ctx.per_worker_ration == 0);
}
