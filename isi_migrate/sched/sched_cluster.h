#ifndef __SCHED_CLUSTER_H__
#define __SCHED_CLUSTER_H__

#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/sched/siq_log.h"
#include "sched_local_work.h"

/* For return value of below function check_pool_for_up_node */
enum check_pool_t {CHECKING_FAILED, CHECKING_DONE};

/* What happens after trying to connect to target cluster */
enum start_status {SCHED_TARGET_OK,
		   SCHED_TARGET_INVALID,
		   SCHED_TIMEOUT,
		   SCHED_DEFER_TO_OTHER_NODE,
		   SCHED_NODE_OK,
		   SCHED_NO_NODE_OK};

struct sched_cluster_health {
	bool has_at_least_one_node_ok;
	bool local_node_ok;
	bool local_in_pool;
	bool local_node_conn_failed;
};

void cluster_check(struct sched_job *job,
    struct sched_cluster_health *health_out, struct siq_source_record *srec,
    struct isi_error **error_out);

enum start_status policy_local_node_check(struct sched_job *job,
    struct sched_cluster_health *health, struct isi_error **error_out);

void get_restricted_nodes(char *restrict_by, struct int_set *nodes_in_pool,
    struct isi_error **error_out);

#endif // __SCHED_CLUSTER_H__
