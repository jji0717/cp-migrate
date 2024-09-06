#ifndef __RESTRICT_H__
#define __RESTRICT_H__

enum restrict_find {
	SIQ_RESTRICT_NOT_FOUND,
	SIQ_RESTRICT_FOUND,
	SIQ_RESTRICT_FOUND_BUT_DYNAMIC
};

enum restrict_find get_nodes_in_subnet_zone(
    struct flx_config *fnc,
    struct flx_subnet *subnet,
    struct int_set *restricted_nodes,
    const char *zone_name,
    struct flx_pool **pool_out);

enum restrict_find get_nodes_in_pool(
    struct flx_config *config,
    struct int_set *restricted_nodes,
    const char *subnet_and_pool_name,
    struct flx_pool **pool_out);

#endif
