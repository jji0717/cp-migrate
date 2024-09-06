#include <netdb.h>
#include <arpa/inet.h>

#include <isi_config/array.h>
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/siq_source.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/restrict.h"
#include "sched_includes.h"
#include "sched_cluster.h"
#include "isi_ufp/isi_ufp.h"

/*
 * Check if a there are any interfaces in a pool that are up and that the
 * if the local node is in the pool, if it has an up interface.
 *
 * Sets these boolean variables:
 *   has_at_least_one_node_ok: at least one node in pool has an up interface
 *   local_node_ok: if local node is in pool, is its interface up?
 *
 * Returns:
 *   CHECKING_DONE:    No more work to do looking for up nodes
 *   CHECKING_FAILED:  No node in the pool has an up interface
 */
static enum check_pool_t
check_pool_for_up_node(
    struct flx_config *fnc,
    int local_id,
    struct int_set *restricted_nodes,
    struct flx_pool *pool,
    struct sched_cluster_health *health_out)
{
	struct flx_iface_id const *id;
	struct inx_addr_set const *node_ips;
	struct flx_node_info const *node;
	struct lni_list *lni_list;
	struct lni_list_iter *lni_iter = NULL;
	struct lni_info *lni;
	enum check_pool_t ret = CHECKING_FAILED;
	
	FLX_IFACE_INX_MAP_FOREACH(id, node_ips, flx_pool_members(pool)) {

		/* Skip checking for up interfaces on any node not in the
		 * restriction list */
		if (restricted_nodes && int_set_size(restricted_nodes) > 0 &&
		    !int_set_contains(restricted_nodes, id->devid)) {
			log(DEBUG, "%s: Skipping node %d", __func__, id->devid);
			continue;
		}

		/* Determine if the node has an interface that matches the
		 * interface listed in the pool. */
		node = flx_config_find_node(fnc, id->devid);
		lni_list = flx_node_info_get_lni_list(node);
		lni_iter = lni_list_iterator(lni_list);
		while (lni_list_has_next(lni_iter)) {
			lni = lni_list_next(lni_iter);
			if (flx_iface_class_lni_equal(&id->klass, lni)) {

				log(TRACE, "Interface devid=%d lni=%s  "
				    "local=%d up=%d", id->devid,
				    lni_get_name(lni), id->devid == local_id,
				    nic_get_stat(lni_get_nic(lni)) ==
				    NIC_STAT_UP);

				if (nic_get_stat(lni_get_nic(lni)) ==
				    NIC_STAT_UP) {
					health_out->has_at_least_one_node_ok =
					    true;
					if (id->devid == local_id) {
						/* We have all of our
						 * answers. No more work to
						 * do. */
						health_out->local_node_ok =
						    true;
						ret = CHECKING_DONE;
						goto done;
					}
					/* We have the answer for this node,
					 * go on to next node in pool. */
					break;
				}
			}
					
		}
		lni_list_iter_free(lni_iter);
		lni_iter = NULL;

		/* If we're the local node, and we've determined we're
		 * not OK, but we know other nodes are OK,
		 * then no need to continue. */
		if (id->devid == local_id &&
		    !health_out->local_node_ok &&
		    health_out->has_at_least_one_node_ok) {
			ret = CHECKING_DONE;
			goto done;
		}
	}
done:
	if (lni_iter)
		lni_list_iter_free(lni_iter);
	return ret;
}

/*
 * Determine if any node has a valid external IP, and if the local node has a
 * valid external IP. Return -1 if subnet:pool definition is bad, 0 otherwise.
 */
void
cluster_check(struct sched_job *job, struct sched_cluster_health *health_out,
    struct siq_source_record *srec, struct isi_error **error_out)
{
	int local_id;
	struct arr_config *cfg = NULL;
	struct arr_device *dev = NULL;
	struct flx_config *fnc = NULL;
	struct isi_error *error = NULL;
	struct flx_subnet *subnet;
	struct flx_pool *pool, *other_pool;
	struct flx_groupnet *groupnet;
	void *const *p, *const *pp, *const *g;
	enum restrict_find found;
	struct int_set INT_SET_INIT_CLEAN(nodes_in_pool);
	void **dppv;
	bool all_nodes_conn_fail = true;
	bool ignore_conn_fail = false;
	int const *node_id;

	UFAIL_POINT_CODE(ignore_tgt_conn_failure, ignore_conn_fail = true);
	if (ignore_conn_fail)
	    log(DEBUG, "ignoring target connection failures");

	log(TRACE, "%s: job->restrict_by='%s'", __func__, job->restrict_by);

	health_out->has_at_least_one_node_ok = false;
	health_out->local_node_ok = false;
	health_out->local_in_pool = false;

	cfg = arr_config_load(&error);
	if (error) {
		/* Fatal, but wait five minutes to exit to prevent
		 * getting in a tight restart loop. */
		log(ERROR, "Unable to retrieve cluster config: %s. Exiting.",
		    isi_error_get_message(error));
		siq_nanosleep(300, 0);
		exit(EXIT_FAILURE);
	}
	local_id = arr_config_get_local_devid(cfg);

	fnc = flx_config_open_read_only(NULL, &error);
	if (error) {
		/* Fatal, but wait five minutes to exit to prevent
		 * getting in a tight restart loop. */
		log(ERROR, "Unable to retrieve flexnet config: %s. Exiting.",
		    isi_error_get_message(error));
		siq_nanosleep(300, 0);
		exit(EXIT_FAILURE);
	}

	if (*job->restrict_by) {
		found = get_nodes_in_pool(fnc, &nodes_in_pool,
		    job->restrict_by, &pool);

		if (found != SIQ_RESTRICT_FOUND) {
			if (found == SIQ_RESTRICT_NOT_FOUND) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,
				    "Policy %s has an invalid subnet:pool "
				    "restriction of %s. Use the policy resolve"
				    " command when the restriction is fixed.",
				    job->policy_name, job->restrict_by);
			} else {
				/* == SIQ_RESTRICT_FOUND_BUT_DYNAMIC */
				error = isi_siq_error_new(E_SIQ_GEN_CONF,
				    "Policy %s has a dynamic subnet:pool "
				    "restriction of %s. Only static pools are "
				    "allowed. Use the policy resolve"
				    " command when the restriction is fixed.",
				    job->policy_name, job->restrict_by);
			}

			log(ERROR, "%s", isi_error_get_message(error));
			goto done;
		}

		if (int_set_contains(&nodes_in_pool, local_id))
			health_out->local_in_pool = true;

		/* With force_interface, we only check this pool's nodes'
		 * interfaces to see if they're up */
		if (job->force_interface) {
			/* No need to check for return value as we're looking
			 * in a single pool. */
			check_pool_for_up_node(fnc, local_id, NULL,
			    pool, health_out);
			goto done;
		}
	} else {
		/* If no node restrictions, local node is always in pool */
		health_out->local_in_pool = true;
	}

	/* If the job has failed on all viable nodes due to target connectivity
	 * issues, disable the policy */
	if (int_set_empty(&nodes_in_pool)) {
		/* No restrictions. Check for conn failure on all nodes */
		ARR_DEVICE_FOREACH(dppv, dev, arr_config_get_devices(cfg)) {
			if (!siq_source_get_node_conn_failed(srec,
			    arr_device_get_devid(dev))) {
				all_nodes_conn_fail = false;
				break;
			}
		}

		if (all_nodes_conn_fail && !ignore_conn_fail) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "No node on source cluster was able to connect to "
			    "target cluster.");
			goto done;
		}
	} else {
		/* Only check for conn failures on nodes in subnet:pool */
		INT_SET_FOREACH(node_id, &nodes_in_pool) {
			if (!siq_source_get_node_conn_failed(srec, *node_id)) {
			    	all_nodes_conn_fail = false;
				break;
			}
		}

		if (all_nodes_conn_fail && !ignore_conn_fail) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "No node in specified subnet:pool was able to "
			    "connect to target cluster.");
			goto done;
		}
	}

	health_out->local_node_conn_failed = (ignore_conn_fail) ? false :
	    siq_source_get_node_conn_failed(srec, local_id);

	/* Check to see if any node in the cluster or the pool (if a pool was
	 * specified) has at least one up external interface. The kernel
	 * chooses which interface to use on a node, so it doesn't matter if
	 * the up interface on a node is in the specified pool, or if the pool
	 * with the up interface is a static or dynamic pool. */
	FLX_GROUPNET_FOREACH(g, groupnet, fnc) {
	    FLX_SUBNET_FOREACH(p, subnet, groupnet) {
		FLX_POOL_FOREACH(pp, other_pool, subnet) {
			log(TRACE, "%s: Checking nodes in "
			    "subnet:pool %s:%s",
			    __func__, flx_subnet_get_name(subnet),
			    flx_pool_get_name(other_pool));
			if (check_pool_for_up_node(fnc, local_id, 
				&nodes_in_pool,
				other_pool, health_out) == CHECKING_DONE)
				goto done;
		}
	    }
	}

done:
	if (fnc)
		flx_config_free(fnc);
	if (cfg)
		arr_config_free(cfg);
	isi_error_handle(error, error_out);

	log(TRACE, "%s: has_at_least_one_node_ok=%d local_node_ok=%d "
	    "local_in_pool=%d", __func__, health_out->has_at_least_one_node_ok,
	    health_out->local_node_ok, health_out->local_in_pool);
}

/*
 * Check to see if a policy can run on the local node
 */
enum start_status
policy_local_node_check(struct sched_job *job,
    struct sched_cluster_health *health, struct isi_error **error_out)
{
	bool has_restrict;
	const struct inx_addr *addr = NULL;
	struct isi_error *error = NULL;
	enum start_status result = SCHED_NODE_OK;

	/* A node can always support a policy whose target is the same cluster,
	 * it will use internal IPs instead of external. */
	if (strcmp(job->target, "localhost") == 0 ||
	    strcmp(job->target, "127.0.0.1") == 0) {
		memset(&job->forced_addr, 0, sizeof(struct inx_addr));
		goto out;
	}

	has_restrict = job->restrict_by[0];

	if (!health->has_at_least_one_node_ok) {
		if (job->force_interface) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "None of the interfaces in pool %s have external "
			    "IPs. Can't run policy %s to cluster %s",
			    job->restrict_by,
			    job->policy_name,
			    job->target);
		} else {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "No nodes in %s%s have external IPs. "
			    "Can't run policy %s to cluster %s",
			    has_restrict ? "pool " : "cluster",
			    has_restrict ? job->restrict_by : "",
			    job->policy_name,
			    job->target);
		}

		result = SCHED_NO_NODE_OK;
		goto out;
	}

	/* At this point, at least one node in the policy's pool is OK. */

	if (!health->local_in_pool) {
		log(DEBUG, "This node is not in pool %s for policy %s; "
		    "not candidate to run it.", job->restrict_by,
		    job->policy_name);
		result = SCHED_DEFER_TO_OTHER_NODE;
		goto out;
	}

	if (!health->local_node_ok) {
		log(INFO, "This node can't connect to external network; not "
		    "candidate to run policy %s to cluster %s",
		    job->policy_name, job->target);
		result = SCHED_DEFER_TO_OTHER_NODE;
		goto out;
	}

	if (has_restrict && job->force_interface) {
		addr = get_local_node_pool_addr(job->restrict_by, &error);
		if (error) {
			log(ERROR, "Node has no IPs in pool %s (%s); not "
			    "a candidate to run policy %s to cluster %s",
			    job->restrict_by, isi_error_get_message(error),
			    job->policy_name, job->target);
			isi_error_free(error);
			error = NULL;
			result = SCHED_DEFER_TO_OTHER_NODE;
			goto out;
		}
		inx_addr_copy(&job->forced_addr, addr);
		log(TRACE, "%s: forced_addr = %s", __func__,
		    inx_addr_to_str(&job->forced_addr));
	} else
		memset(&job->forced_addr, 0, sizeof(struct inx_addr));

	/* Everything is cool with this node */

out:
	if (addr)
		free((struct inx_addr *)addr);
	isi_error_handle(error, error_out);
	return result;
}

/* Given a subnet:pool name (restrict_by), nodes_in_pool is populated with
 * the set of node devids that are in the subnet:pool. If restrict_by is
 * NULL or "", every node is considered to be in the pool. */
void
get_restricted_nodes(char *restrict_by, struct int_set *nodes_in_pool,
    struct isi_error **error_out)
{
	struct arr_config *cfg = NULL;
	struct flx_config *fnc = NULL;
	struct arr_device *dev = NULL;
	struct isi_error *error = NULL;
	enum restrict_find found;
	void **dppv;

	cfg = arr_config_load(&error);
	if (error) {
		log(ERROR, "%s: Unable to retrieve cluster config: %s.",
		    __func__, isi_error_get_message(error));
		goto out;
	}

	fnc = flx_config_open_read_only(NULL, &error);
	if (error) {
		log(ERROR, "%s: Unable to retrieve flexnet config: %s.",
		    __func__, isi_error_get_message(error));
		goto out;
	}

	if (restrict_by && restrict_by[0]) {
		found = get_nodes_in_pool(fnc, nodes_in_pool, restrict_by,
		    NULL);

		if (found != SIQ_RESTRICT_FOUND) {
			if (found == SIQ_RESTRICT_NOT_FOUND) {
				log(ERROR,
				    "%s: Invalid pool %s supplied for "
				    "restriction", __func__, restrict_by);
			} else {
				/* == SIQ_RESTRICT_FOUND_BUT_DYNAMIC */
				log(ERROR,
				    "%s: Specified pool '%s' is a dynamic "
				    "pool. Only static pools allowed",
				    __func__, restrict_by);
			}

			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "Could not find a valid subnet:pool restriction "
			    "for %s: %s", restrict_by,
			    found == SIQ_RESTRICT_NOT_FOUND ? "Pool doesn't "
			    "exist" : "Pool is a dynamic pool");
			goto out;
		}
	} else {
		/* No restrictions, so every node is in the pool */
		ARR_DEVICE_FOREACH(dppv, dev, arr_config_get_devices(cfg)) {
			int_set_append(nodes_in_pool,
			    arr_device_get_devid(dev));
		}
	}


out:
	if (fnc)
		flx_config_free(fnc);
	if (cfg)
		arr_config_free(cfg);
	isi_error_handle(error, error_out);
}
