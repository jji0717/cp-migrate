#ifndef SCHED_CHANGE_BASED_H
#define SCHED_CHANGE_BASED_H

bool is_policy_change_based(struct siq_policy *pol, char *recur_str);

bool check_for_change_based_work(struct sched_ctx *sctx,
    struct siq_policy *pol, struct siq_source_record *srec,
    time_t curr_time, bool skip_backoff_delays,
    struct isi_error **error_out);

void reap_stale_pol_ctxs(struct sched_ctx *sctx, time_t curr_time);

#endif /* SCHED_CHANGE_BASED_H */
