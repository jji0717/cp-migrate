#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <arpa/inet.h>
#include <errno.h>

#include <isi_config/array.h>
#include <isi_config/ifsconfig.h>
#include <isi_flexnet/isi_flexnet.h>
#include <isi_net/isi_nic.h>
#include <isi_net/isi_lni.h>
#include <isi_util/util_adt.h>

#include "isirep.h"
#include "restrict.h"
#include "siq_workers_utils.h"

enum ip_find {
	FOUND_INIT,
	NOT_FOUND,
	FOUND_DOWN,
	FOUND_UP
};

static void
get_nodes(struct int_set *set, const char *sysctl)
{
	size_t len1, len2;
	char *buf = NULL, *buf_to_scan = NULL, *tok;

	do {
		free(buf);
		buf = NULL;
		if (sysctlbyname(sysctl, NULL, &len1, 0, 0) == -1) {
			log(ERROR, "%s : Can't get %s length", 
			    __func__, sysctl);
			goto out;
		}
		buf = calloc(1, len1 + 1);
		ASSERT(buf);
		len2 = len1;
		if (sysctlbyname(sysctl, buf, &len2, 0, 0) == -1) {
			log(ERROR, "%s : Can't read %s", __func__, sysctl);
			goto out;
		}
		/* Read again if the sysctl size changed resulting in
		 * truncation */
	} while (len1 < len2);
	buf_to_scan = buf;
	while ((tok = strsep(&buf_to_scan, "{}, "))) {
		if (*tok) {
			int_set_add(set, atoi(tok));
		}
	}
	
 out:
	free(buf);
}

/*
 * Determine which nodes are up by getting the sysctl efs.gmp.up_nodes. The
 * returned set is a list of device ids of up nodes.
 */
void
get_up_devids(struct int_set *devids)
{
	const char SYSCTL_UP_NODES[] = "efs.gmp.up_nodes";
	return get_nodes(devids, SYSCTL_UP_NODES);
}

/*
 * Get a ptr_vec of nodes that are up. Each element in the ptr_vec is a
 * a pointer to a struct node_ids, which contains the node's devid and lnn.
 */
void
get_up_node_ids(struct ptr_vec *nodes_out, struct isi_error **error_out)
{
	int i;
	IfsConfig ic;
	struct int_set INT_SET_INIT_CLEAN(up_devids);
	struct node_ids *node = NULL;
	struct isi_error *error = NULL;

	/* Get the up devids */
	get_up_devids(&up_devids);

	/* Get the cluster lnns */
	memset(&ic, 0, sizeof(ic));
	arr_config_load_as_ifsconf(&ic, &error);
	if (error)
		goto out;

	/* Add only the lnns that are up, based on the up devids */
	for (i = 0; i < ic.arrLen; i++) {
		if (!int_set_contains(&up_devids, ic.array[i].arrayId))
			continue;
		node = calloc(1, sizeof(struct node_ids));
		node->devid = ic.array[i].arrayId;
		node->lnn = ic.array[i].lnn;
		pvec_append(nodes_out, (void *)node);
	}
out:
	ifsConfFree(&ic);
	isi_error_handle(error, error_out);

}

void
get_not_dead_storage_nodes(struct int_set *set)
{
	const char SYSCTL_UP_NODES[] = "efs.gmp.not_dead_storage_nodes";
	return get_nodes(set, SYSCTL_UP_NODES);
}

static enum ip_find
interface_up_on_node(struct flx_iface_id const *iface,
    struct flx_node_info const *node)
{
	struct lni_list *lni_list;
	struct lni_list_iter *lni_iter = NULL;
	struct lni_info const *lni;
	struct nic_info const *nic;

	lni_list = flx_node_info_get_lni_list(node);
	lni_iter = lni_list_iterator(lni_list);
	while (lni_list_has_next(lni_iter)) {
		lni = lni_list_next(lni_iter);
		if (flx_iface_class_lni_equal(&iface->klass, lni)) {
			nic = lni_get_nic(lni);
			log(TRACE, "%s: Interface devid=%d  lni=%s  nic=%s "
			    " up=%d", __func__, iface->devid,
			    lni_get_name(lni), nic_get_interface(nic),
			    nic_get_stat(nic) == NIC_STAT_UP);
			
			lni_list_iter_free(lni_iter);
			
			if (nic_get_stat(nic) == NIC_STAT_UP) {
				return FOUND_UP;
			}
			return FOUND_DOWN;
		}
	}
	lni_list_iter_free(lni_iter);
	log(ERROR, "%s: Didn't find interface for node id=%d",
	    __func__, iface->devid);
	return NOT_FOUND;
}


static enum ip_find
check_addr_on_subnet_status(struct flx_config *fnc, int devid,
    struct flx_subnet *subnet, const struct inx_addr *addr_wanted)
{
	struct flx_pool *pool;
	void *const *pp;
	struct flx_iface_id const *iface;
	struct inx_addr_set const *iface_on_node_ips;
	struct flx_node_info const *node;
	enum ip_find status = FOUND_INIT;

	FLX_POOL_FOREACH(pp, pool, subnet) {
		FLX_IFACE_INX_MAP_FOREACH(iface, iface_on_node_ips,
		    flx_pool_members(pool)) {

			if (iface->devid != devid ||
			    !inx_addr_set_contains(iface_on_node_ips,
			    *addr_wanted))
				continue;
			
			log(TRACE, "%s: Found interface of node %d in %s:%s",
			    __func__, iface->devid,
			    flx_subnet_get_name(subnet),
			    flx_pool_get_name(pool));

			node = flx_config_find_node(fnc, iface->devid);

			status = interface_up_on_node(iface, node);
			if (status != NOT_FOUND) {
				log(TRACE, "%s:   Found %s %s",
				    __func__, inx_addr_to_str(addr_wanted),
				    status == FOUND_UP ? "UP": "DOWN");
				return status;
			}
		}
	}
	return NOT_FOUND;
}

static enum ip_find
check_addr_status(struct flx_config *fnc, int devid, bool use_internal,
    const struct inx_addr *addr_wanted)
{
	struct flx_subnet *subnet;
	struct flx_groupnet *groupnet;
	void * const *p, * const *g;
	enum ip_find status = FOUND_INIT;

	log(TRACE,"%s: looking for %s on node %d", __func__,
	    inx_addr_to_str(addr_wanted), devid);

	if (use_internal) {
		FLX_INTERNAL_SUBNET_FOREACH(p, subnet, fnc) {
			status = check_addr_on_subnet_status(fnc, devid,
			    subnet, addr_wanted);
			if (status != NOT_FOUND)
				return status;
		}
	}
	else {
	    FLX_GROUPNET_FOREACH(g, groupnet, fnc) {
		FLX_SUBNET_FOREACH(p, subnet, groupnet) {
			if (subnet->_family != addr_wanted->inx_family)
				continue;
			status = check_addr_on_subnet_status(fnc, devid,
			    subnet, addr_wanted);
			if (status != NOT_FOUND)
				return status;
		}
	    }
	}
	log(INFO, "%s: Didn't find expected %s on node %d", __func__,
	    inx_addr_to_str(addr_wanted), devid);
	return NOT_FOUND;
}

static const struct inx_addr *
get_addr_of_node_in_pool(struct flx_config *fnc, int devid, bool v4_only,
    struct flx_pool *pool)
{
	struct flx_iface_id const *iface;
	struct inx_addr_set const *node_ips;
	struct flx_node_info const *node_info;
	struct inx_addr const *addr = NULL;
	enum ip_find status = FOUND_INIT;

	if (v4_only && pool->_subnet->_family != AF_INET)
		goto out;
	
	/* Grab the first IP on the first up interface for the node in the
	 * pool */
	FLX_IFACE_INX_MAP_FOREACH(iface, node_ips, flx_pool_members(pool)) {
		if (iface->devid != devid || inx_addr_set_size(node_ips) == 0)
			continue;

		node_info = flx_config_find_node(fnc, iface->devid);
		status = interface_up_on_node(iface, node_info);
		if (status != FOUND_UP)
			continue;

		INX_ADDR_SET_FOREACH(addr, node_ips)
			goto out;
	}

	/* As pools can have no IPs, this is acceptable */
	if (status == FOUND_INIT)
		log(DEBUG,
		    "In pool restriction of %s, no IP assigned to node id %d",
		    flx_pool_get_name(pool), devid);

out:
	return addr;
}

int
get_local_lnn(struct isi_error **error_out)
{
	IfsConfig ic;
	struct isi_error *error = NULL;
	int to_return = -1, i;

	memset(&ic, 0, sizeof(ic));
	arr_config_load_as_ifsconf(&ic, &error);
	if (error)
		goto out;

	for (i = 0; i < ic.arrLen; i++) {
		if (ic.array[i].arrayId == ic.arrayId) {
			to_return = ic.array[i].lnn;
			break;
		}
	}

	if (to_return == -1)
		error = isi_system_error_new(ENOENT, "Failed to find local "
		    "lnn in array config.");

out:
	ifsConfFree(&ic);
	isi_error_handle(error, error_out);
	return to_return;
}

const struct inx_addr *
get_local_node_pool_addr(const char *subnet_pool, struct isi_error **error_out)
{
	IfsConfig ic;
	struct flx_config *fnc = NULL;
	struct flx_pool *pool = NULL;
	struct int_set INT_SET_INIT_CLEAN(nodes_in_pool);
	enum restrict_find found;
	const struct inx_addr *addr = NULL;
	struct inx_addr *tmp_addr = NULL;
	struct isi_error *error = NULL;

	memset(&ic, 0, sizeof(ic));
	arr_config_load_as_ifsconf(&ic, &error);
	if (error)
		goto out;

	fnc = flx_config_open_read_only(NULL, &error);
	if (error)
		goto out;
	
	log(DEBUG, "%s: subnet_pool='%s'", __func__, subnet_pool);
	found = get_nodes_in_pool(fnc, &nodes_in_pool, subnet_pool, &pool);
	if (found != SIQ_RESTRICT_FOUND) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Can't restrict to pool %s because it %s", subnet_pool,
		    found == SIQ_RESTRICT_NOT_FOUND ?
		    "doesn't exist" : "is a dynamic pool");
		goto out;
	}

	if (!int_set_contains(&nodes_in_pool, ic.arrayId)) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Can't run as this node isn't in pool %s", subnet_pool);
		goto out;
	}

	addr = get_addr_of_node_in_pool(fnc, ic.arrayId, false, pool);
	if (!addr) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Can't run as this node doesn't have an "
		    "up interface for pool %s", subnet_pool);
	} else {
		tmp_addr = calloc(1, sizeof(struct inx_addr));
		ASSERT(tmp_addr);
		inx_addr_copy(tmp_addr, addr);
		addr = tmp_addr;
		tmp_addr = NULL;
		log(DEBUG, "%s: addr=%s", __func__, inx_addr_to_str(addr));
	}

out:
	isi_error_handle(error, error_out);
	if (fnc)
		flx_config_free(fnc);
	ifsConfFree(&ic);
	return addr;
}

static struct inx_addr_set *
get_addr_set(struct flx_config *fnc, int array_id, bool use_internal,
    bool have_restrict_by, sa_family_t family)
{
	struct inx_addr_set *addrs = NULL;
	ASSERT(family == AF_INET || family == AF_INET6 || family == AF_UNSPEC);

	if (use_internal) {
		/* Get only addresses from internal failback interface
		 * (LOOPBACK = failback interface) */
		addrs = flx_get_node_inx_addrs(fnc, array_id,
		    FLX_ALLOC_METHOD_STATIC, LNI_ANY_IDX,
		    IFFLAG_USEADDR | IFFLAG_LOOPBACK);
	} else {
		switch (family) {
		case AF_INET:
			addrs = flx_get_node_inx_addrs(fnc, array_id,
			    FLX_ALLOC_METHOD_STATIC, LNI_ANY_IDX,
			    IFFLAG_EXTERNAL);
			break;
		case AF_INET6:
			addrs = flx_get_node_inx_addrs6(fnc, array_id,
			    FLX_ALLOC_METHOD_STATIC, LNI_ANY_IDX,
			    IFFLAG_EXTERNAL);
			break;
		default:
			/*When family is unspecified*/
			addrs = flx_get_node_inx_addrs_by_type(fnc, array_id,
			    FLX_ALLOC_METHOD_STATIC, LNI_ANY_IDX,
			    IFFLAG_EXTERNAL, AF_UNSPEC);
		}

		/* Normally only static pools are allowed. But if there's not
		 * a static pool and there's not a restrict_by, and the
		 * default pool is dynamic, using a dynamic pool is allowed.*/
		if (!have_restrict_by && inx_addr_set_size(addrs) == 0) {
			inx_addr_set_clean(addrs);
			free(addrs);
			switch (family) {
			case AF_INET:
				addrs = flx_get_node_inx_addrs(fnc, array_id,
				    FLX_ALLOC_METHOD_DYNAMIC, LNI_ANY_IDX,
				    IFFLAG_EXTERNAL);
				break;
			case AF_INET6:
				addrs = flx_get_node_inx_addrs6(fnc, array_id,
				    FLX_ALLOC_METHOD_DYNAMIC, LNI_ANY_IDX,
				    IFFLAG_EXTERNAL);
				break;
			default:
				/*When family is unspecified*/
				addrs = flx_get_node_inx_addrs_by_type(fnc, array_id,
				    FLX_ALLOC_METHOD_DYNAMIC, LNI_ANY_IDX,
				    IFFLAG_EXTERNAL, AF_UNSPEC);
			}

			log(TRACE, "%s: In default pool, using "
			    "dynamic addresses", __func__);
		}
	}

	/* OK to return with empty IP set if none are found */
	return addrs;
}

char **
allocate_ip_list(int n)
{
	char **ip_list = NULL;
	int i;

	ASSERT(n >= 0);
	ASSERT(n < 10000);      /* sanity! */

	if (n == 0)
		goto out;

	ip_list = calloc(n + 1, sizeof(char *));
	ASSERT(ip_list);

	for (i = 0; i < n; i++) {
		/* Max of 46 chars in ASCII IP address including ending NULL.
		 * IPv4 has a max of 16 (including NULL) but IPv6 has a max of
		 * 46 (with NULL) if IPv4 tunneled addresses are included. */
		ip_list[i] = calloc(1, INET6_ADDRSTRLEN);
		ASSERT(ip_list[i]);
	}

	ip_list[n] = NULL;

out:
	return ip_list;
}

void
free_ip_list(char **list)
{
	int i;

	ASSERT(list);

	for (i = 0; list[i] != NULL; i++) {
		free(list[i]);
		list[i] = NULL;
	}

	free(list);
}

static bool 
in_same_subnet(struct flx_subnet *subnet, struct inx_addr const *addr)
{
	struct inx_addr base = flx_subnet_get_base_addr(subnet);
	struct inx_netmask inxnm = flx_subnet_get_netmask(subnet);
	struct inx_addr netmask, calced_base;

	if (subnet->_family != addr->inx_family)
		return false;

	inx_netmask_to_addr(&inxnm, &netmask);

	inx_addr_calc_base_addr(addr, &netmask, &calced_base);

	if (inx_addr_cmp(&base, &calced_base) == 0)
		return true;

	return false;
}

/*
 * Returns the number of nodes found on local cluster.
 *
 * If error or local cluster has no nodes available, return 0 and set error_out
 * with an error message. If no error and msg_out was passed in, set msg_out to
 * a message listing nodes and IPs. The v4_only argument limits the results to
 * IPv4 (AF_INET) if true, otherwise, results are unrestricted.
 */
int
get_local_cluster_info(struct nodex **nodes_out, char ***ip_list,
    int **lnn_list, size_t *lnn_list_len, struct inx_addr *target_addr,
    char *restrict_by_name, bool use_internal, bool v4_only, char **msg_out,
    struct isi_error **error_out)
{
	IfsConfig ic;
	struct flx_config *fnc = NULL;
	struct flx_subnet *subnet = NULL;
	struct isi_error *error = NULL, *arrerr = NULL;
	struct inx_addr_set *addrs = NULL;
	struct inx_addr const *addr = NULL;
	struct nodex *nodes;
	struct flx_pool *pool = NULL;
	int i, n = 0;
	struct int_set INT_SET_INIT_CLEAN(up_nodes);
	struct int_set INT_SET_INIT_CLEAN(restricted_nodes);
	struct in_addr tmp_addr;
	struct fmt FMT_INIT_CLEAN(fmt);
	enum restrict_find ret;
	char *addr_str = NULL;
	sa_family_t family;

	log(TRACE, "%s", __func__);

	if (msg_out)
		*msg_out = NULL;
	if (use_internal)
		v4_only = true;

	memset(&ic, 0, sizeof(ic));

	arr_config_load_as_ifsconf(&ic, &arrerr);
	if (arrerr) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Unable to retrieve cluster config: "
		    "%#{}. Exiting", isi_error_fmt(arrerr));
		goto out;
	}

	fnc = flx_config_open_read_only(NULL, &error);
	if (error)
		goto out;

	get_up_devids(&up_nodes);
	if (int_set_size(&up_nodes) == 0) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "There are no nodes that are up on the cluster");
		goto out;
	}

	if (target_addr && 
	   ((target_addr->inx_family == AF_INET &&
	    target_addr->inx_addr4 == htonl(INADDR_LOOPBACK)) ||
	    (target_addr->inx_family == AF_INET6  &&
	    IN6_IS_ADDR_LOOPBACK(&target_addr->uinx_addr.inx_v6)))) {
		/* local request */
		log(TRACE, "%s: request for localhost", __func__);

	/* If local info requested remotely from source cluster */
	} else if (target_addr) {
		log(TRACE, "%s: Target specified by IP %s", __func__,
		    inx_addr_to_str(target_addr));
		subnet = flx_config_find_subnet(fnc, target_addr);
		if (subnet == NULL) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "Unable to find a subnet with IP %s",
			    inx_addr_to_str(target_addr));
			goto out;
		}
		/* If there's a restrict_by_name use it as the zone to
		 * restrict nodes to. */
		if (restrict_by_name && *restrict_by_name &&
		    !inet_pton(AF_INET, restrict_by_name, &tmp_addr) &&
		    !inet_pton(AF_INET6, restrict_by_name, &tmp_addr)) {
			log(TRACE,
			    "%s: Nodes are restricted by target zone %s",
			    __func__, restrict_by_name);
			ret = get_nodes_in_subnet_zone(fnc, subnet,
			    &restricted_nodes, restrict_by_name, &pool);
			if (ret == SIQ_RESTRICT_NOT_FOUND) {
				error = isi_siq_error_new(E_SIQ_GEN_NET,
				    "Target %s is not a zone name. "
				    "No IPs for target cluster returned",
				    restrict_by_name);
				goto out;
			}
			else if (ret == SIQ_RESTRICT_FOUND_BUT_DYNAMIC) {
				error = isi_siq_error_new(E_SIQ_GEN_NET,
				    "Target %s is zone with dynamic IP "
				    "allocation. Only zones with static "
				    "IP allocation allowed. "
				    "No IPs for target cluster returned",
				    restrict_by_name);
				goto out;
			}
			/* Static zone name found */
			else if (int_set_size(&restricted_nodes) == 0) {
				error = isi_siq_error_new(E_SIQ_GEN_NET,
				    "Target zone %s has no nodes in it. "
				    "No IPs for target cluster returned",
				    restrict_by_name);
				goto out;
			}
		}
		else {
			/* Change restrict_by_name to NULL in case it's an IP
			 * address to ensure we don't exclude any node
			 * below */
			restrict_by_name = NULL;
			log(TRACE, "%s: No target node zone restrictions",
			    __func__);
		}
	}

	/* Local request restricting by pool */
	else if (restrict_by_name && *restrict_by_name) {
		log(TRACE, "%s: getting info only on nodes in %s",
		    __func__, restrict_by_name);
		ret = get_nodes_in_pool(fnc, &restricted_nodes,
		    restrict_by_name, &pool);
		if (ret == SIQ_RESTRICT_NOT_FOUND) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "Can't run policy with invalid subnet:pool "
			    "restriction of %s", restrict_by_name);
			    goto out;
		}
		else if (ret == SIQ_RESTRICT_FOUND_BUT_DYNAMIC) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "Restriction of %s has dynamic IP allocation. "
			    "Only pools with static IP allocation allowed",
			    restrict_by_name);
			    goto out;
		}
		/* Static pool found */
		else if (int_set_size(&restricted_nodes) == 0) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "Can't run policy with no nodes and IPs found for "
			    "subnet:pool restriction of %s",
			    restrict_by_name);
			    goto out;
		}
	}
	else {
		log(TRACE, "%s: No source node pool restrictions", __func__);
	}

	/* Build an array of nodes to send back. */

	nodes = calloc(ic.arrLen, sizeof *nodes);
	ASSERT(nodes);
	if (ip_list)
		*ip_list = allocate_ip_list(ic.arrLen);
	if (lnn_list) {
		if (*lnn_list != NULL) {
			free(*lnn_list);
			*lnn_list = NULL;
			*lnn_list_len = 0;
		}
		*lnn_list = calloc(ic.arrLen, sizeof(int));
		ASSERT(*lnn_list);
		if (lnn_list_len)
			*lnn_list_len = ic.arrLen;
	}

	log(TRACE, "%s: number of nodes=%d", __func__, ic.arrLen);

	for (i = 0; i < ic.arrLen; i++) {

		if (!int_set_contains(&up_nodes, ic.array[i].arrayId)) {
			log(INFO, "%s: Skipping down node (devid=%d, lnn=%d)",
			    __func__, ic.array[i].arrayId, ic.array[i].lnn);
			continue;
		}
		if (restrict_by_name && *restrict_by_name &&
		    !int_set_contains(&restricted_nodes,
			ic.array[i].arrayId)) {
			log(TRACE, "%s: Excluding node (devid=%d, lnn=%d)",
			    __func__, ic.array[i].arrayId, ic.array[i].lnn);
			continue;
		}
		log(TRACE, "%s: Getting IP for node (devid=%d, lnn=%d)",
		    __func__, ic.array[i].arrayId, ic.array[i].lnn);

		nodes[n].devid = ic.array[i].arrayId;
		nodes[n].lnn = ic.array[i].lnn;

		/* 
		 * If the flexnet config doesn't have entry due to
		 * race between gmp and fnc, skip the node
		 */
		if (flx_config_find_node(fnc, ic.array[i].arrayId) == NULL) {
			log(INFO,
			    "%s: Skip node with no entry (devid=%d, lnn=%d)",
			    __func__, ic.array[i].arrayId, ic.array[i].lnn);
			continue;
		}

		/* Find an IP associated with this node */
		if (!use_internal && pool) {
			addr = get_addr_of_node_in_pool(fnc,
			    ic.array[i].arrayId, v4_only, pool);
			if (addr)
				inx_addr_copy(&nodes[n].addr, addr);
		}
					    
		/* If we're looking for internal IPs, or there's no pool/zone
		 * restriction, or if there is one but we failed to find an
		 * IP, look for any up static IP on on the node */
		if (use_internal || !pool || !addr) {
			if (v4_only)
				family = AF_INET;
			else if (target_addr)
				family = target_addr->inx_family;
			else if (pool)
				family = pool->_subnet->_family;
			else {
				/*When family is unspecified*/
				family = AF_UNSPEC;
			}

			addrs = get_addr_set(fnc, ic.array[i].arrayId,
			    use_internal, pool != NULL, family);

			log(TRACE, "%s: # addrs for node %d = %d (pool=%s)",
			    __func__,
			    ic.array[i].arrayId, inx_addr_set_size(addrs),
			    pool ? flx_pool_get_name(pool): "(no pool)");

			INX_ADDR_SET_FOREACH(addr, addrs) {
				/* 1) If getting internal IPs, just grab the
				 *    first one, and we know it's up (because
				 *    if it wasn't, the node wouldn't be part
				 *    of the cluster)
				 *
				 * 2) If no subnet restrictions, only check to
				 *    see if the interface is up.
				 *
				 * 3) If a subnet restriction, make sure the
				 *    interface is in the subnet and check to
				 *    see if the interface is up
				 */
				if (use_internal || ((!subnet || (subnet &&
				    in_same_subnet(subnet, addr))) &&
				    check_addr_status(fnc, ic.array[i].arrayId,
				    use_internal, addr) == FOUND_UP)) {
					inx_addr_copy(&nodes[n].addr, addr);
					log(TRACE, "%s: node (id=%d,lnn=%d) "
					    "chose ip=%s", __func__,
					    nodes[n].devid, nodes[n].lnn,
					    inx_addr_to_str(addr));
					break;
				}
			}
			inx_addr_set_clean(addrs);
			free(addrs);
		}

		/*
		 * We only return a node in the response if we were
		 * able to find an IP address for that node.
		 */
		if (nodes[n].addr.inx_family) {
			if (ip_list) {
				addr_str = inx_addr_to_str(&nodes[n].addr);
				if (!addr_str ||
				    strlen(addr_str) >= INET6_ADDRSTRLEN) {
					error = isi_siq_error_new(
					    E_SIQ_GEN_NET, "Error converting "
					    "addr to human readable format");
					goto out;
				}
				strncpy((*ip_list)[n], addr_str,
				    INET6_ADDRSTRLEN);
				if (lnn_list) {
					(*lnn_list)[n] = ic.array[i].lnn;
				}
			}
			n++;
		} else {
			log(INFO, "%s: Found no IP for node (id=%d, lnn=%d)",
			    __func__, nodes[n].devid, nodes[n].lnn);
		}
	}

	/*
	 * By definition, we should have at least found this node, e.g.,
	 * the one that the client managed to connect to, so if we're not
	 * returning any nodes, bail out.
	 */
	if (n == 0) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Local cluster has no external IP addresses");
		free(nodes);
		if (ip_list && *ip_list) {
			free_ip_list(*ip_list);
			*ip_list = NULL;
		}
		if (lnn_list && *lnn_list) {
			free(*lnn_list);
			*lnn_list = NULL;
			*lnn_list_len = 0;
		}
		goto out;
	}

	/* May be returning fewer ips than ips allocated for if not all nodes
	 * are included. */
	if (ip_list) {
		for (i = n; i < ic.arrLen; i++) {
			free((*ip_list)[i]);
			(*ip_list)[i] = NULL;
		}
	}

	if (lnn_list)
		*lnn_list_len = n;

	/* Create string for message with the nodes' lnns and IPs */
	if (msg_out) {
		for (i = 0; i < n; i++)
			fmt_print(&fmt, "%d)%{} ", nodes[i].lnn,
			    inx_addr_fmt(&nodes[i].addr));

		if (restrict_by_name && *restrict_by_name)
			asprintf(msg_out, "Restriction '%s' has %d nodes: %s",
			    restrict_by_name, n, fmt_string(&fmt));
		else
			asprintf(msg_out, "%d nodes available: %s",
			    n, fmt_string(&fmt));
	}

	/* Only keep the node info if it was asked for */
	if (nodes_out)
		*nodes_out = nodes;
	else
		free(nodes);
	
out:
	isi_error_handle(error, error_out);
	if (fnc)
		flx_config_free(fnc);
	if (arrerr)
		isi_error_free(arrerr);
	ifsConfFree(&ic);

	return n;
}

bool
max_down_nodes(int max_down)
{
	int up_storage;
	int not_dead_storage;

	up_storage = get_num_nodes();
	not_dead_storage = get_num_up_nodes();

	log(DEBUG, "TDN: %d - %d > %d: %d", not_dead_storage, up_storage,
	    max_down, not_dead_storage - up_storage > max_down);
	return not_dead_storage - up_storage > max_down;
}

int
lowest_node()
{
	struct nodex *nodes;
	char buf[128], search[16], scratch[2];
	int i, j, n, res;
	size_t len;
	struct isi_error *error = NULL;

	len = sizeof(buf);
	res = sysctlbyname("efs.gmp.up_nodes", buf, &len, 0, 0);
	if (res && errno != ENOMEM) {
		log(ERROR, "Could not retrieve efs.gmp.up_nodes");
		return 0;
	}
	buf[sizeof(buf) - 1] = 0;

	/* Retrieve a list of devids with external interfaces, then search for
	 * each of these at every position in the efs.gmp.up_nodes buf.  This
	 * is very loopy, but we wont tend to get too far into either loop. */
	n = get_local_cluster_info(&nodes, NULL, NULL, NULL, 0, NULL, true,
	    false, NULL, &error);
	if (n == 0) {
		isi_error_free(error);
		return 0;
	}
	isi_error_free(error);

	for (i = 0; i < n; i++) {
		sprintf(search, " %d%%1[, ]", (res = nodes[i].devid));
		for (j = 0; buf[j]; j++)
			if (sscanf(buf + j, search, scratch))
				break;
		if (buf[j])
			break;
	}
	free(nodes);
	if (i == n) /* None found. */
		return 0;

	len = sizeof(buf);
	if (sysctlbyname("efs.gmp.has_quorum", buf, &len, 0, 0)) {
		log(ERROR, "Could not retrieve efs.gmp.has_quorum");
		return 0;
	}
	if (!buf[0]) {
		log(DEBUG, "Master is %d, but group doesn't have quorum", res);
		return 0;
	}

	return res;
}

/**
 * Do we have a quorum in this qroup?
 * Check to see if there is a quorum in the current group.
 * @return State 1 - has quorum, 0 - do not have a quorum, -1 - error.
 */
int
group_has_quorum(void)
{
	int     quorum;
	size_t  lquorum;

	log(TRACE, "group_has_quorum: enter");
	lquorum = sizeof(quorum);
	if (sysctlbyname("efs.gmp.has_quorum", &quorum, &lquorum,
	    NULL, 0) != 0) {
		log(ERROR, "group_has_quorum: Could not determine"
		    " quorum state: %s", strerror(errno));
		return -1;
        }

	if (quorum != 1) {
		return 0;
	}
	return 1;
} /* group_has_quorum */

int
get_num_nodes(void)
{
	size_t len;
	int up_storage;
	char buf[16];
	
	/* Find count of storage nodes. */
	len = sizeof(buf);
	if (sysctlbyname("efs.gmp.num_up_storage_nodes", buf, &len, 0, 0)
	    || len != 4) {
		log(ERROR, "Could not retrieve efs.gmp.num_up_storage_nodes");
		return -1;
	}
	up_storage = *(int *)buf;
	
	return up_storage;
}

int
get_num_up_nodes(void)
{
	size_t len;
	int not_dead_storage;
	char buf[16];
	
	/* Find count of not dead storage nodes. */
	len = sizeof(buf);
	if (sysctlbyname("efs.gmp.num_not_dead_storage_nodes", buf, &len, 0, 0)
	    || len != 4) {
		log(ERROR, 
		    "Could not retrieve efs.gmp.num_not_dead_storage_nodes");
		return -1;
	}
	not_dead_storage = *(int *)buf;
	
	return not_dead_storage;
}

/*
 * Given a list of nodex list and SyncIQ version, return char * list
 * of inx_addr.
 */
void
construct_inx_addr_list(struct nodex *nodes, int node_count, unsigned curr_ver,
    char **pairs_out, unsigned *len_out)
{
	int i = 0;
	unsigned len = 0;
	struct lnn_ipx_pair *pairs = NULL;
	struct lnn_ipx_pair_v_3 *pairs_v_3 = NULL;

	if (curr_ver >= FLAG_VER_3_5) {
		pairs = calloc(node_count, sizeof(struct lnn_ipx_pair));
		len = node_count * sizeof(struct lnn_ipx_pair);
		for (i = 0; i < node_count; i++) {
			pairs[i].lnn = nodes[i].lnn;
			memcpy(&pairs[i].addr, &nodes[i].addr,
			    sizeof(struct inx_addr));
		}
		*pairs_out = (char *) pairs;
	} else {
		pairs_v_3 = calloc(node_count,
		    sizeof(struct lnn_ipx_pair_v_3));
		len = node_count * sizeof(struct lnn_ipx_pair_v_3);
		for (i = 0; i < node_count; i++) {
			pairs_v_3[i].lnn = nodes[i].lnn;
			memcpy(&pairs_v_3[i].addr, &nodes[i].addr,
			    sizeof(struct inx_addr_v_3));
		}
		*pairs_out = (char *) pairs_v_3;
	}
	*len_out = len;
}

/*
 * Given a char * list of lnn_ipx_pair, we need return a copied/upgraded
 * list of lnn_ipx_pair list.
 */
void
copy_or_upgrade_inx_addr_list(char *pairs_in, unsigned ipslen,
    unsigned curr_ver, struct lnn_ipx_pair **pairs_out, int *count_out)
{
	int i = 0;
	unsigned count = 0;
	struct lnn_ipx_pair *pairs = NULL;
	struct lnn_ipx_pair *tpairs_in = NULL;
	struct lnn_ipx_pair_v_3 *pairs_v_3 = NULL;

	if (curr_ver >= FLAG_VER_3_5) {
		count = ipslen / sizeof(struct lnn_ipx_pair);
		pairs = *pairs_out = calloc(count, sizeof(struct lnn_ipx_pair));
		tpairs_in = (struct lnn_ipx_pair *) pairs_in;
		for (i = 0; i < count; i++) {
			memcpy(&pairs[i], &tpairs_in[i],
			    sizeof(struct lnn_ipx_pair));
		}
	} else {
		count = ipslen / sizeof(struct lnn_ipx_pair_v_3);
		pairs = *pairs_out = calloc(count, sizeof(struct lnn_ipx_pair));
		pairs_v_3 = (struct lnn_ipx_pair_v_3 *) pairs_in;
		for (i = 0; i < count; i++) {
			memcpy(&pairs[i], &pairs_v_3[i],
			    sizeof(struct lnn_ipx_pair_v_3));
		}
	}

	*count_out = count;
}

/* Get the caller's ip address. The first valid IP address that is found
 * is the one that is returned. */
void
get_ip_address(char **ip, int devid, bool get_internal,
    struct isi_error **error_out)
{
	int n = 0, i = 0;
	char *addr = NULL;
	struct nodex *nodes = NULL;
	struct isi_error *error = NULL;

	n = get_local_cluster_info(&nodes, NULL, NULL, NULL, 0, NULL, get_internal,
	    false, NULL, &error);
	if (error)
		goto out;

	for (i = 0; i < n; i++) {
		if (nodes[i].devid == devid &&
		    nodes[i].addr.inx_family) {
			addr = inx_addr_to_str(&nodes[i].addr);
			if (!addr ||
			    strlen(addr) >= INET6_ADDRSTRLEN) {
				error = isi_siq_error_new(E_SIQ_GEN_NET,
				    "Error converting addr to human readable "
				    "format");
				goto out;
			}
			*ip = calloc(strlen(addr) + 1, 1);
			strncpy(*ip, addr, strlen(addr));
			break;
		}
	}

out:
	free(nodes);
	isi_error_handle(error, error_out);
}
