#include <sstream>
#include <string>

#include <sys/isi_celog_eventids.h>

#include "isi_cbm_account_util.h"
#include "isi_cbm_error_util.h"

#include <isi_ilog/ilog.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_proxy/isi_proxy.h>

#include "isi_cbm_data_stream.h"
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include "isi_cbm_id.h"
#include "isi_cbm_policyinfo.h"
#include "io_helper/isi_cbm_ioh_base.h"
#include "io_helper/isi_cbm_ioh_creator.h"
#include "isi_cbm_scoped_ppi.h"

using namespace isi_cloud;
using namespace std;


/**
 * This routine is to be supported by account statistics module
 */
static bool
is_account_good_for_write(struct cpool_account *acct, size_t req_sz)
{
	ASSERT(acct);

	// information of account
	bool enabled = acct->account_enabled;

	// TODO: the followings are calculated or pulled from account statistics

	// run-time access error flag (hint not to use)
	bool access_error = false;

	bool account_readonly = false;

	// indicating unlimited space if space_total = 0
	uint64_t space_total = size_constants::SIZE_100TB;
	uint64_t space_used = 0;

	if (!enabled || account_readonly || access_error)
		return false;

	if (space_total) {
		// if space is very low; should have alerted to customer
		// when it reaches low_threshold
		uint64_t very_low = acct->space_low_threshold / 10;
		if (space_total - space_used <= very_low)
			return false;
		if (space_used + req_sz > space_total)
			return false;
	}
	return true;
}

/**
 * copy config account info to cbm account
 *
 * Note: there is a redundant, we will remove the need of isi_cbm_account
 * as a separate step
 */
void
isi_cfm_cpool_acct_to_cbm_acct(const cpool_account &acct,
    isi_cbm_account &account, const struct cpool_config_context *context,
    struct isi_proxy_context *proxy_context, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	account.url =  acct.uri;
	account.check_cert = acct.check_cert;
	account.name = acct.user_name;
	account.key = acct.key;
	account.id = acct.user_id;
	account.account_id.set_from_account(acct);
	account.container_cdo = acct.bucket;
	account.container_cmo = acct.metadata_bucket;
	account.type = acct.ptype;
	account.chunk_size = acct.chunk_size;
	account.read_size = acct.read_size;
	account.sparse_resolution = acct.sparse_resolution;
	account.master_cdo_size = acct.master_cdo_size;
	account.account_key = acct.local_key;
	if (acct.capacity_uri)
		account.capacity_url = acct.capacity_uri;
	account.max_capacity = acct.max_capacity;
	if (acct.location_constraint) {
		account.location_constraint = acct.location_constraint;
	}

	account.auth_type = cpool_provider_auth_cap_resolve_auth_type(
	    context, acct.auth_type, acct.ptype, acct.location_constraint,
	    acct.uri, &error);
	ON_ISI_ERROR_GOTO(out, error);

	account.proxy.url.clear();
	account.proxy.username.clear();
	account.proxy.password.clear();
	account.proxy.type = ISI_PROXY_TYPE_NONE;
	account.proxy.port = 0;

	if (acct.proxy_id && acct.proxy_id[0] != '\0') {
		struct isi_proxy *proxy = NULL;
		isi_proxy_get_by_id(proxy_context,acct.proxy_id,&proxy,&error);

		ON_ISI_ERROR_GOTO(out, error, "cannot find proxy (%s) for "
		    "account %s", acct.proxy_id, acct.user_name);

		account.proxy.type = proxy->type;
		account.proxy.url = proxy->host;
		account.proxy.username = proxy->username ? proxy->username :"";
		account.proxy.password = proxy->password ? proxy->password :"";
		account.proxy.port = proxy->port;

		isi_proxy_free(&proxy);
	}

	account.timeouts.connect_timeout = context->cfg->curl_connect_timeout;
	account.timeouts.low_speed_limit = context->cfg->curl_low_speed_limit;
	account.timeouts.low_speed_time = context->cfg->curl_low_speed_time;

out:
	isi_error_handle(error, error_out);
}

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
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpool_account *acct = NULL;
	ASSERT(ppi && error_out);

	acct = cpool_account_get_by_id_and_cluster(ppi->cp_context,
	    account_id.get_id(),
	    account_id.get_cluster_id(), &error);

	ON_ISI_ERROR_GOTO(out, error, "cannot get the account"
	    "(%d, %s)", account_id.get_id(), account_id.get_cluster_id());

	isi_cfm_cpool_acct_to_cbm_acct(*acct, account, ppi->cp_context,
	    ppi->proxy_context, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

void
isi_cfm_select_writable_account(struct isi_cfm_policy_provider *ppi,
    const isi_cbm_id &provider_id, const isi_cbm_id &policy_id, size_t req_sz,
    isi_cbm_account &account, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	struct cpool_provider_instance *pinst = NULL;
	struct cpool_accountid **pacctid = NULL;
	struct cpool_account *acct = NULL;
	bool found = false;
	stringstream os_pid, os_provid;

	ASSERT(ppi && error_out);

	pinst = cpool_provider_get_by_id_and_cluster(ppi->cp_context,
	    provider_id.get_id(), provider_id.get_cluster_id(), &error);

	ON_ISI_ERROR_GOTO(out, error, "failed to get provider instance");

	CPOOL_ACCOUNTID_VEC_FOREACH(pacctid, &pinst->account_ids) {

		isi_cbm_id account_id;
		account_id.set_from_accountid((**pacctid));

		acct = cpool_account_get_by_id_and_cluster(ppi->cp_context,
		    account_id.get_id(), account_id.get_cluster_id(),
		    &error);
		ON_ISI_ERROR_GOTO(out, error, "failed to get account by id");

		if (!is_account_good_for_write(acct, req_sz))
			continue;

		found = true;
		break;
	}


	os_pid << (policy_id.get_id());
	os_provid << (provider_id.get_id());

	if (!found) {
		error = isi_system_error_new(ENOENT,
		    "Cannot write to Cloud Provider as "
		    "valid account was not found");
		celog_event_create(NULL, CCF_NONE,
		    CPOOL_NO_USABLE_ACCOUNT_FOUND, CS_CRIT, 0,
		    "CloudPools no usable account found for "
		    "policyid '{policyid}', poolid '{poolid}', "
		    "poolname '{poolname}', clusterid '{clusterid}' ",
		    "policyid %s, poolid %s, poolname %s, clusterid %s ",
		    (os_pid.str()).c_str(), (os_provid.str()).c_str(),
		    pinst->provider_instance_name, policy_id.get_cluster_id());
		goto out;
	}
	//////把acct的内容拷贝到account中
	isi_cfm_cpool_acct_to_cbm_acct(*acct, account, ppi->cp_context,
	    ppi->proxy_context, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:

	if (error) {
		isi_error_handle(error, error_out);
	}

};

isi_cbm_ioh_base_sptr
isi_cpool_get_io_helper(const isi_cbm_id& account_id,
    isi_error **error_out)
{
	isi_error *error = NULL;
	isi_cbm_ioh_base_sptr io_helper;
	isi_cbm_account account;
	bool retry = false;

	while (1) {
		if (retry) {
			// Ensure we retry with fresh config.
			close_ppi();
		}

		scoped_ppi_reader ppi_rdr;

		const isi_cfm_policy_provider *cbm_cfg =
		    ppi_rdr.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error,
		    "failed to get cpool config context");

		isi_cfm_get_account((isi_cfm_policy_provider *)cbm_cfg,
		    account_id, account, &error);
		if (error) {
			isi_error_add_context(error,
			    "failed to retrieve account, cluster = %s, id = %d",
			    account_id.get_cluster_id(), account_id.get_id());
			if (retry) // Retry also failed
				goto out;

			// We could have failed because the config was stale;
			// Refer bug 217940. Retry.
			ilog(IL_DEBUG, "Retrying. %#{}", isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
			retry = true;
		} else {
			if (retry) // Retry succeeded
				ilog(IL_DEBUG,
				    "succeeded in retrieving account, "
				    "cluster = %s, id = %d",
				    account_id.get_cluster_id(),
				    account_id.get_id());

			break;
		}
	}

	io_helper = isi_cpool_get_io_helper(account, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to isi_cpool_get_io_helper");

 out:
	isi_error_handle(error, error_out);
	return io_helper;
}


isi_cbm_ioh_base_sptr
isi_cpool_get_io_helper(const isi_cbm_account& account,
    isi_error **error_out)
{
	isi_error *error = NULL;
	isi_cbm_ioh_base *io_helper = NULL;

	io_helper = isi_cbm_ioh_creator::get_io_helper(
	    account.type, account);
	if (io_helper == NULL) {
		error = isi_cbm_error_new(CBM_INVALID_ACCOUNT,
		    "unsupported account type %d for account id: (%d@%s)",
		    account.type, account.account_id.get_id(),
		    account.account_id.get_cluster_id());
		goto out;
	}
 out:
	isi_error_handle(error, error_out);
	return isi_cbm_ioh_base_sptr(io_helper);
}

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
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpool_account *acct = NULL;
	struct cpool_provider_auth_capability *pac = NULL;

	ASSERT(ppi && error_out);

	*acct_auth_capability = 0;

	acct = cpool_account_get_by_id_and_cluster(ppi->cp_context,
	    account_id.get_id(),
	    account_id.get_cluster_id(), &error);

	ON_ISI_ERROR_GOTO(out, error, "cannot get the account"
	    "(%d, %s)", account_id.get_id(), account_id.get_cluster_id());

	if (!isi_cloud_common_is_provider_s3_like(acct->ptype))
		goto out;

	pac = cpool_provider_auth_cap_find(ppi->cp_context,
	    cpool_account_get_type(acct),
	    cpool_account_get_location_constraint(acct),
	    cpool_account_get_uri(acct), &error);

	ON_ISI_ERROR_GOTO(out, error, "cannot get the provider auth capability"
	    "(%d, %s)", cpool_account_get_type(acct),
	    cpool_account_get_location_constraint(acct));

	*acct_auth_capability = pac->auth_capability;

out:
	isi_error_handle(error, error_out);
}

