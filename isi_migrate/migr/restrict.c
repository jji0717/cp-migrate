#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>

#include <isi_config/config_util.h>
#include <isi_flexnet/isi_flexnet.h>
#include <isi_util/util_adt.h>
#include "isirep.h"
#include "restrict.h"

/*
 * Remove any DNS domain search name from the end of zone name. 
 * Returns a value that needs to be freed.
 */
static char *
lop_off_dns_domain(const char *zone_name, struct flx_config *fnc)
{
	char *mod_zone_name, *end;
	struct fmt const *f;

	mod_zone_name = strdup(zone_name);
	FMT_VEC_FOREACH(f, flx_config_dns_domains(fnc)) {

		/* If the passed in zone name ends with a DNS domain,
		 * and ... */
		if ((end = ends_with(mod_zone_name, fmt_string(f))) &&
		    /* If The passed in zone name doesn't consist only of the
		     * DNS domain name, and ... */
		    end != mod_zone_name &&

		    /* There's a dot right before the domain name, then ... */
		    *(--end) == '.') {

			/* Lop off the domain name by changing the dot to the
			 * end of string. */
			*(end) = '\0';
			return mod_zone_name;
		}
	}
	free(mod_zone_name);
	return NULL;
}
	
enum restrict_find
get_nodes_in_subnet_zone(
    struct flx_config *fnc,
    struct flx_subnet *subnet,
    struct int_set *restricted_nodes,
    const char *zone_name,
    struct flx_pool **pool_out)
{
	struct flx_pool *pool;
	struct flx_iface_id const *id;
	struct inx_addr_set const *ips;
	void *const *pp;
	char *mod_zone_name, *zone_of_pool, *mod_zone_of_pool;
	enum restrict_find ret = SIQ_RESTRICT_NOT_FOUND;

	/* If passed in zone name ends with any DNS domain name, remove it. */
	if ((mod_zone_name = lop_off_dns_domain(zone_name, fnc)))
		zone_name = mod_zone_name;

	FLX_POOL_FOREACH(pp, pool, subnet) {
		
		zone_of_pool = flx_pool_get_sc_zone(pool);

		/* Skip pools with no zone name */
		if (!zone_of_pool)
			continue;
		
		/* If this pool's zone name ends with any DNS domain name,
		 * remove it. */
		if ((mod_zone_of_pool = lop_off_dns_domain(zone_of_pool, fnc)))
		    zone_of_pool = mod_zone_of_pool;

		if (strcasecmp(zone_of_pool, zone_name) != 0) {
			free(mod_zone_of_pool);
			continue;
		}
		free(mod_zone_of_pool);
		if (flx_pool_is_dynamic_pool(pool)) {
			/* If there's more than one pool and this one is
			 * dynamic, we don't allow it */
			if (pvec_size(&subnet->_pools) > 1) {
				ret = SIQ_RESTRICT_FOUND_BUT_DYNAMIC;
				break;
			}
			/* But with only one pool, keep on going as it's
			 * allowed to be dynamic.*/
		}
		ret = SIQ_RESTRICT_FOUND;
		FLX_IFACE_INX_MAP_FOREACH(id, ips, flx_pool_members(pool)) {
			/* Must have IPs in this zone */
			if (!inx_addr_set_empty(ips) &&
			    !int_set_contains(restricted_nodes, id->devid)) {
				log(TRACE, "%s: Added node %d",
				    __func__, id->devid);
				int_set_add(restricted_nodes, id->devid);
			}
		}
		break;
	}
	free(mod_zone_name);
	if (pool_out) {
		if (ret != SIQ_RESTRICT_NOT_FOUND)
			*pool_out = pool;
		else
			*pool_out = NULL;
	}
	return ret;
}

enum restrict_find
get_nodes_in_pool(
    struct flx_config *config,
    struct int_set *restricted_nodes,
    const char *subnet_and_pool_name,
    struct flx_pool **pool_out)
{
	struct flx_pool *pool;
	struct flx_iface_id const *id;
	struct inx_addr_set const *ips;
	struct isi_error *error = NULL;

	if (!(pool = get_pool(config, subnet_and_pool_name, &error))) {
		log(ERROR, "%s", isi_error_get_message(error));
		isi_error_free(error);
		if (pool_out)
			*pool_out = NULL;
		return SIQ_RESTRICT_NOT_FOUND;
	}

	if (pool_out)
		*pool_out = pool;

	if (flx_pool_is_dynamic_pool(pool))
		return SIQ_RESTRICT_FOUND_BUT_DYNAMIC;

	FLX_IFACE_INX_MAP_FOREACH(id, ips, flx_pool_members(pool)) {
		/* It's OK if the node has no IPs in this pool. All we want
		 * are the node numbers */
		if (!int_set_contains(restricted_nodes,	id->devid)) {
			log(TRACE, "%s: Added node %d", __func__, id->devid);
			int_set_add(restricted_nodes,
			    id->devid);
		}
	}
	return SIQ_RESTRICT_FOUND;
}
