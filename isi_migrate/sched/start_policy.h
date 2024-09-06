#ifndef _START_POLICY_H_
#define _START_POLICY_H_

int make_job_summary(struct siq_job_summary *summary, 
    enum siq_action_type action);
pid_t start_policy(char *policy_id, char* additional_args);
pid_t start_failover(char *policy_id, bool revert, char* additional_args);
pid_t start_failback_prep(char *policy_id, enum siq_action_type action, 
    char *additional_args);
pid_t start_domain_mark(char *root, enum domain_type type, bool verbose,
    char *additional_args);
pid_t start_snap_revert(ifs_snapid_t snapid, bool verbose,
    char *additional_args);

#endif
