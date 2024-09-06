#ifndef SCHED_SNAP_TRIG_H
#define SCHED_SNAP_TRIG_H

bool is_policy_snap_trig(struct siq_policy *pol, char *recur_str);

bool check_for_snap_trig_work(struct sched_ctx *sctx,
    struct siq_policy *pol, struct siq_source_record *srec,
    struct isi_error **error_out);

void reap_stale_snap_ctxs(struct sched_ctx *sctx);

#endif /* SCHED_SNAP_TRIG_H */
