#include "siq_jobctl_getnextrun_private.h"

int 
siq_job_get_next_run(const struct siq_policy *policy, time_t prev_start,
    time_t prev_end, time_t since, time_t *next_start)
{
	int ret = -1;	 
		
	if (next_start == NULL)
		return -1;
	*next_start = 0;
	if (policy == NULL)
		return -1;
	
	ret = sched_check_for_work(policy, prev_start, prev_end, since, 
	    next_start, 0);

	return ret;
}

static int
sched_check_for_work(const struct siq_policy *policy, time_t prev_start, 
    time_t prev_end, time_t since, time_t *pnext_start, int once)
{
	time_t		base_time;
	time_t		mcur_time = INT_MAX-33536000; /* magic number */
	time_t		cur_next_start;
	char		*recur_str = NULL;
	struct date_pat *recur_date_pat;
	struct dp_record *date_record;
	struct isi_error *ie = NULL;
	int	rc;


	if (!get_schedule_info(policy->scheduler->schedule,
		&recur_str, &base_time)) {
		if (recur_str != NULL) {
			free(recur_str);
		}
		return -1;
	}

	//  Change-based and snapshot-triggered policies require relatively
	//  short time intervals to check for changes.
	if (strcasecmp(recur_str, SCHED_ALWAYS) == 0 &&
	    (policy->common->action == SIQ_ACT_SYNC ||
	    policy->common->action == SIQ_ACT_REPLICATE)) {
		*pnext_start = (policy->common->state == SIQ_ST_ON ?
		    time(NULL) + CHANGE_BASED_POLICY_INTERVAL : 0);
		free(recur_str);
		return 0;
	}

	if (strcasecmp(recur_str, SCHED_SNAP) == 0 &&
	    (policy->common->action == SIQ_ACT_SYNC ||
	    policy->common->action == SIQ_ACT_REPLICATE)) {
		*pnext_start = (policy->common->state == SIQ_ST_ON ?
		    time(NULL) + SNAP_TRIG_POLICY_INTERVAL : 0);
		free(recur_str);
		return 0;
	}

	/* rpo alerts should be checked every RPO_ALERT_POLICY_INTERVAL */
	if (policy->scheduler->rpo_alert > 0) {
		*pnext_start = (policy->common->state == SIQ_ST_ON ?
		    time(NULL) + RPO_ALERT_POLICY_INTERVAL : 0);
	}

	/* schedule changed */
        if (prev_start < base_time) {
                prev_start = 0;
        }

	parse_date(recur_str, &base_time, &recur_date_pat,
	    	&date_record, &ie);
	if (ie) {
		isi_error_free(ie);
		free(recur_str);
		return -1;
	}
	free(recur_str);
	
	if (prev_start == (time_t)0) {
		if (date_record->type != DP_REC_NON_RECURRING) {
			prev_start = base_time;
		}
	}
	else {
		prev_start += 60; // XXX: isi_date rounding
		if (once) {
			date_pat_free(recur_date_pat);
			dp_record_free(date_record);
			return 1; 
		}
	}

	rc = date_pat_get_next_match(recur_date_pat, prev_start,
	    mcur_time, &cur_next_start);
	if (rc != 0) {
		if (cur_next_start < base_time) {
			cur_next_start = base_time;
		}
	} 
	date_pat_free(recur_date_pat);
	dp_record_free(date_record);

	if (rc == 0) {
		return 1;
	}
	
	if ((cur_next_start < *pnext_start) || (*pnext_start == 0)) {
		*pnext_start = cur_next_start;
	}

	return 0;
}

static int
get_schedule_info(char *schedule,
    char **recur_str, time_t *base_time)
{
	char *recur_buf = NULL, *recur_ptr = NULL;
	int rc = 1;

	recur_buf = strdup(schedule);
	if (recur_buf == NULL) {
		return 0;
	}

	/*
	 * Recurring string for a schedule entry has the format:
	 *  UTC time | recurring date
	 */
	if ((recur_ptr = strchr(recur_buf, '|')) == NULL) {
		rc = 0;
		goto out;
	}

	*recur_ptr = '\0';
	recur_ptr++;
	*base_time = strtoul(recur_buf, (char **)NULL, 10);
	*recur_str = strdup(recur_ptr);
	if (*recur_str == NULL) {
		rc = 0;
		goto out;
	}
		
out:
	if (recur_buf)
		free(recur_buf);
	return rc;
}
