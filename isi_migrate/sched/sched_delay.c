#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_report.h"
#include "isi_migrate/sched/siq_jobctl.h"
#include "isi_migrate/sched/siq_log.h"
#include "isi_ufp/isi_ufp.h"

#include "sched_delay.h"

#define ARRAY_SIZE(a) (sizeof (a) / sizeof ((a)[0]))

const time_t	policy_backoff_delays[] = {
                    //  Consecutive errors      Job delay
    1  * 60,        //          1                1 minute
    2  * 60,        //          2                2 minutes
    4  * 60,        //          3                4 minutes
    8  * 60,        //          4                8 minutes
    15 * 60,        //          5               15 minutes
    30 * 60,        //          6               30 minutes
    1  * 60 * 60,   //          7                1 hour
    2  * 60 * 60,   //          8                2 hours
    4  * 60 * 60,   //          9                4 hours
    8  * 60 * 60,   //         10+               8 hours
};

/*
 *  Checks whether a policy is delayed due to previous failures.
 *  If the preceding run (if any) was something other than "failed", then
 *  the job can run immediately. However, if the preceding run failed, then
 *  a delay is imposed based on the number of consecutive failures.
 *
 *  @param pol          (IN) The policy object.
 *  @param delay_ctx    (INOUT) The policy's delay context.
 *  @param error_out    (OUT) Resulting error, if any.
 *
 *  @return True if the policy is delayed, false otherwise.
 */
bool
check_for_backoff_delay(struct siq_policy *pol,
    struct siq_policy_delay *delay_ctx, struct isi_error **error_out)
{
	int	ret, i, j, count;
	bool	result = true;
	time_t	delay = 0, now = time(NULL);

	struct isi_error		*error = NULL;
	struct siq_job_summary		*summary;
	struct siq_composite_report	**reports = NULL;


	UFAIL_POINT_CODE(sched_delay_roll_ahead,
	    now += RETURN_VALUE;
	    log(INFO, "Backoff delay rolled ahead by %d seconds to %ld",
		RETURN_VALUE, now););

	//  We are not actively finishing a delay yet so set the finished_delay
	//  flag to false.
	if (delay_ctx->finished_delay)
		delay_ctx->finished_delay = false;

	//  If a delayed starttime hasn't been reached then don't start a job.
	if (delay_ctx->delayed_start_time > now)
		goto out;

	//  Retrieve the last batch of reports to check job completion status.
	//  N.B. Job lock is owned by sched_main_node_work().
	ret = siq_policy_report_load_recent(
	    pol->common->pid, 1, &reports, &count);

	if (ret != 0) {
		error = isi_system_error_new(ret,
		    "Error loading recent reports for policy %s (id %s)",
		    pol->common->name, pol->common->pid);
		goto out;
	}

	//  If a delayed start time has been reached and there
	//  is no newer report then it's ok to start.
	if (delay_ctx->delayed_start_time > 0 && count > 0) {
		//  Locate the newest report.
		i = reports[0]->num_reports - 1;
		summary = reports[0]->elements[i];
		if (delay_ctx->last_report_end == summary->total->end) {
			result = false;
			delay_ctx->finished_delay = true;
			goto out;
		}
	}

	//  Examine the reports for failures, working from newest to oldest,
	//  and stopping when something other than a failure is found.
	//  The delay is a function of the back-to-back failure count.
	for (i = (count == 0 ? -1 : reports[0]->num_reports - 1), j = 0;
	    i >= 0 && j < ARRAY_SIZE(policy_backoff_delays);
	    i--, j++) {
		summary = reports[0]->elements[i];
		if (summary->total->state == SIQ_JS_NONE) {
			//  The latest report is not fully populated
			//  so error out and another scheduler
			//  will retry later.
			error = isi_siq_error_new(E_SIQ_BACKOFF_RETRY,
			    "Error loading recent reports for policy %s "
			    "(id %s): Report %d out of %d has invalid state",
			    pol->common->name, pol->common->pid, i,
			    reports[0]->num_reports - 1);
			goto out;
		} else if (summary->total->state != SIQ_JS_FAILED) {
			break;
		}
		delay = policy_backoff_delays[j];
	}

	if (!delay) {
		result = false;
		goto out;
	}

	//  A delay is needed - calculate a start time relative
	//  to the newest report to keep things consistent across
	//  all schedulers.
	i = reports[0]->num_reports - 1;
	summary = reports[0]->elements[i];
	delay_ctx->last_report_end = summary->total->end;
	delay_ctx->delayed_start_time = summary->total->end + delay;

	if (delay_ctx->delayed_start_time > now) {
		log(INFO, "policy %s: %d consecutive failure "
		    "reports, delay run by %ld seconds from most "
		    "recent report (i.e. %ld seconds)",
		    pol->common->name, j, delay,
		    delay_ctx->delayed_start_time - now);
	} else {
		result = false;
		delay_ctx->finished_delay = true;
	}

out:
	if (reports)
		siq_composite_reports_free(reports);

	if (!result) {
		delay_ctx->last_report_end = 0;
		delay_ctx->delayed_start_time = 0;
	}

	isi_error_handle(error, error_out);
	return result;
}
