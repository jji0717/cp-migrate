#ifndef __ISI_CPH_POLICYINFO__H__
#define __ISI_CPH_POLICYINFO__H__

#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_proxy/isi_proxy.h>

__BEGIN_DECLS
/*
 * utility functions to gather information from the various configuration
 * files for accessing cloud pools policy, provider and account information.
 */
/**
 * Initialize the contexts needed to access the policy, provider, and account
 * information needed to turn a policy into the account to utilize when
 * archiving a file.
 */

struct isi_cfm_policy_provider {
	struct smartpools_context *sp_context;
	struct cpool_config_context *cp_context;
	struct isi_proxy_context *proxy_context;
};

/**
 *  open the configuration information needed to resolve a policy name to an
 *  account for archiving a file.
 *
 *  Note:  Creates a new config instance.   Use scoped_ppi_reader/writer
 *  in isi_cbm_scoped_ppi.h/cpp for access to the shared instance.
 *
 */
void isi_cfm_open_policy_provider_info(struct isi_cfm_policy_provider *,
    struct isi_error **);

/**
 *  close the configuration information needed to resolve a policy name to an
 *  account for archiving a file.
 */
void isi_cfm_close_policy_provider_info(struct isi_cfm_policy_provider *,
    struct isi_error **);

__END_DECLS
#endif // __ISI_CPH_POLICYINFO__H__
