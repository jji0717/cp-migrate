#ifndef SIQ_POLICY_DELAY_H
#define SIQ_POLICY_DELAY_H

struct siq_policy_delay {
	/* The end time of the most recently examined job report. */
	time_t last_report_end;

	/* For policies which are delayed due to previous run failures,
	 * the time at which the policy will be ready to run. */
	time_t delayed_start_time;

	/* True iff we have just finished processing a backoff delay.
	 * If true, on the next call to check_for_backoff_delay, this
	 * flag will be set to false. */
	bool finished_delay;
};

bool
check_for_backoff_delay(struct siq_policy *pol, struct siq_policy_delay *delay,
    struct isi_error **error_out);


#endif /* SIQ_POLICY_DELAY_H */
