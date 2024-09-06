#ifndef __ISI_CBM_ACCOUNT_UTIL_H__
#define __ISI_CBM_ACCOUNT_UTIL_H__

#include <unistd.h>
#include "io_helper/isi_cbm_ioh_base.h"

struct isi_cbm_file;
struct isi_error;
class isi_cbm_id;

/**
 * Given policy provider context, find appropriate account to archive a file.
 * @param ppi[in] policy provider config context
 * @param provider_id[in] the provider id from which account is selected
 * @param req_sz[in] estimate size to be consumed
 * @param account[out] the account structure returned
 * @param error_out[out] isi_error object if failed
 */
void isi_cfm_select_writable_account(struct isi_cfm_policy_provider *ppi,
    const isi_cbm_id &provider_id, const isi_cbm_id &policy_id, size_t req_sz,
    isi_cbm_account &account, struct isi_error **error_out);

/*
 * Get the cloud IO helper for given account id.
 *
 * @param account_id[in]: an account id.
 * @param error_out[out]: error if failed.
 */
isi_cbm_ioh_base_sptr isi_cpool_get_io_helper(
    const isi_cbm_id& account_id,
    isi_error **error_out);

/*
 * Get the cloud IO helper for given account
 *
 * @param account_id[in]: an account id.
 * @param error_out[out]: error if failed.
 */
isi_cbm_ioh_base_sptr isi_cpool_get_io_helper(
    const struct isi_cbm_account &account,
    isi_error **error_out);

/**
 * Get account information by given account id and context
 * @param ppi[in] policy provider config context
 * @param account_id[in] the account id
 * @param account[out] the account structure returned
 * @param error_out[out] isi_error object if failed
 */
void
isi_cfm_get_account(struct isi_cfm_policy_provider *ppi,
    const isi_cbm_id &account_id, struct isi_cbm_account &account,
    struct isi_error **error_out);

/**
 * Convert from a cpool_account to an isi_cbm_account
 * @param cpool_acct[in] the instance of cpool_account to be converted
 * @param cbm_acct[out] the instance of cbm_acct to be populated
 * @param context[in] proxy context for resolving referenced proxy config
 * @param error_out[out] isi_error object if failed
 */
void
isi_cfm_cpool_acct_to_cbm_acct(const cpool_account &cpool_acct,
    isi_cbm_account &cbm_acct, const struct cpool_config_context *context,
    struct isi_proxy_context *proxy_context, struct isi_error **error_out);

/**
 * Get account authentication capability information by given account id
 * and context
 * @param ppi[in] policy provider config context
 * @param account_id[in] the account id
 * @param acct_auth_capability[out] the account authentication capability
 * returned
 * @param error_out[out] isi_error object if failed
 */
void
isi_cfm_get_acct_provider_auth_cap(struct isi_cfm_policy_provider *ppi,
    const isi_cbm_id &account_id, uint64_t *acct_auth_capability,
    struct isi_error **error_out);

#endif // __ISI_CBM_ACCOUNT_UTIL_H__
