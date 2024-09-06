#include "isi_cbm_test_util.h"

#include <unistd.h>
#include <check.h>
#include <openssl/evp.h>
#include <openssl/aes.h>

#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cloud_api/curl_session_pool.h>

#include <isi_cpool_cbm/check_cbm_common.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_config/cpool_initializer.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>

#include <isi_gconfig/main_gcfg.h>
#include <isi_gconfig/gconfig_unit_testing.h>
#include <isi_pools_fsa_gcfg/isi_pools_fsa_gcfg.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_snapshot/snapshot.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_str.h>
#include <isi_cpool_cbm/check_hardening.h>

#include "isi_cbm_id.h"
#include "isi_cbm_account_util.h"
using namespace isi_cloud;

int
file_compare(const char *src, const char *dest)
{
	struct fmt FMT_INIT_CLEAN(cmdstr);
	int ret = 0;
	fmt_print(&cmdstr, "/usr/bin/diff -r %s %s", src, dest);
	ret = system(fmt_string(&cmdstr));
	return ret;
}

struct cbm_test_object_cleaner {
	struct cpool_account *acct;
	isi_cbm_ioh_base_sptr io_helper;
	bool cmo;
};

bool
cbm_test_list_object_cb(void *caller_context,
   const struct cl_list_object_info &obj)
{
	cbm_test_object_cleaner *cleaner =
	    (cbm_test_object_cleaner *)caller_context;

	if (cleaner->cmo) {
		cleaner->io_helper.get()->delete_object(
		    cleaner->acct->metadata_bucket, obj.name, obj.snapid);
	} else {
		cleaner->io_helper.get()->delete_object(
		    cleaner->acct->bucket, obj.name, obj.snapid);
	}
	return true;
}

struct cloud_service_provider *
cbm_test_get_cloud_service_provider(enum cl_provider_type ptype)
{
	struct cloud_service_provider *csp = NULL;
	struct isi_error *error = NULL;

	// Now first create an account.
	cpool_config_context *ctx = cpool_context_open(&error);

	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	csp = cloud_service_provider_get_by_type(ctx, ptype, &error);
	fail_if(error, "Failed to get the cloud service provider: %#{}.\n",
	    isi_error_fmt(error));

	cpool_context_close(ctx);

	return csp;
}


uint64_t
get_csp_max_capacity(enum cl_provider_type ptype)
{
	struct cloud_service_provider *csp =
	    cbm_test_get_cloud_service_provider(ptype);

	return csp->max_account_capacity;
}

void
cbm_test_remove_account(const char *acct_name)
{
	struct isi_error *error = NULL;

	// Now first create an account.
	cpool_config_context *ctx = cpool_context_open(&error);
	printf("\n****************\n");
	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	struct cpool_account *acct = cpool_account_get_by_name(ctx,
	    acct_name, &error);

	if (acct) {
		uint32_t acct_id = cpool_account_get_id(acct);
		const char *cluster = cpool_account_get_cluster(acct);
		isi_cbm_id account_id(acct_id, cluster);
		cl_provider_type cl_type = cpool_account_get_type(acct);

		if (cl_type == CPT_AWS || cl_type == CPT_AZURE) {
			isi_cbm_ioh_base_sptr io_helper =
			    isi_cpool_get_io_helper(account_id, &error);

			fail_if(error, "Failed to get IO helper for "
			    "account: %#{}.\n", isi_error_fmt(error));
			cbm_test_object_cleaner cleaner;
			cleaner.acct = acct;
			cleaner.io_helper = io_helper;
			cleaner.cmo = true;

			cl_list_object_param param;
			param.caller_cb = cbm_test_list_object_cb;
			param.caller_context = &cleaner;
			param.attribute_requested = true;
			param.include_snapshot = true;
			param.depth = 0;

			try {
				io_helper.get()->list_object(
				    acct->metadata_bucket, param);

				cleaner.cmo = false;
				io_helper.get()->list_object(acct->bucket, param);

				io_helper.get()->delete_object(
				    acct->metadata_bucket, "", 0);

				io_helper.get()->delete_object(acct->bucket,
				    "", 0);
			}
			catch (const std::exception &ex) {
				// we do not care about the errors here
				printf("\nExceptions while cleaning cloud "
				    "objects, %s\n", ex.what());
			}
		}
		printf("%s called: acct_id:%u, cluster:%s\n", __func__, acct_id, cluster);
		cpool_account_remove(ctx, acct, &error);
		fail_if(error, "Failed to remove the account: %#{}.\n",
		    isi_error_fmt(error));

		cpool_context_commit(ctx, &error);
		fail_if(error, "Failed to save the GC change: %#{}.\n",
		    isi_error_fmt(error));

		cpool_account_free(acct);
	}

	cpool_context_close(ctx);
}

void
cbm_test_create_account(cl_provider_type ptype,
    const char *account_name,
    const char *vendor_name,
    const char *url,
    const char *user_name,
    const char *key,
    const char *capacity_url,
    const uint64_t user_id /* = 0 */,
    size_t chunksize /* = 0 */,
    size_t readsize /* = 0 */,
    uint32_t sparse_resolution /* = 0 */,
    size_t master_cdo_size /* = 0 */,
    size_t max_acct_capacity /* = 0 */)
{
	struct isi_error *error = NULL;
	struct cpool_account *acct = NULL;

	// Now first create an account.
	cpool_config_context *ctx = cpool_context_open(&error);

	// Make sure we have accepted the data collection eula
	cpool_telemetry_accept_license(ctx, true);

	acct = cpool_account_create(ctx, &error);

	fail_if(error, "Failed to create an account: %#{}.\n",
	    isi_error_fmt(error));

	/*
	 * account_id is used to generate a unique bucket name.  However, since
	 * the account_id sequence is reset between each unit test run, we may
	 * end up with a duplicate bucket name.  This can cause problems with
	 * Azure which may take 30+ seconds to delete an old bucket.  So, we
	 * guarantee a unique account_id by adding the lower half of the
	 * timestamp to it then we use the account_id to generate unique bucket
	 * and metadata_bucket names
	 */
	time_t timestamp = time(NULL);
	acct->account_id += (uint32_t)timestamp;

	isi_cbm_id acct_id;
	acct_id.set_from_account(*acct);

	struct fmt FMT_INIT_CLEAN(bucket_name);

	format_bucket_name(bucket_name, acct_id);

	free(acct->bucket);
	acct->bucket = strdup(fmt_string(&bucket_name));

	free(acct->metadata_bucket);
	acct->metadata_bucket = strdup(acct->bucket);
	acct->metadata_bucket[0] = 'm';

	cpool_account_set_type(acct, ptype);
	cpool_account_set_vendor(acct, vendor_name);
	cpool_account_set_name(acct, account_name);
	cpool_account_set_enabled(acct, true);
	cpool_account_set_user_name(acct, user_name);
	cpool_account_set_key(acct, key);
	cpool_account_set_uri(acct, url);
	if (user_id > 0)
		cpool_account_set_user_id(acct, user_id);
	cpool_account_set_capacity_uri(acct, capacity_url);
	if (chunksize > 0)
		cpool_account_set_chunksize(acct, chunksize);
	if (readsize > 0)
		cpool_account_set_readsize(acct, readsize);
	if (sparse_resolution > 0)
		cpool_account_set_sparse_resolution(acct, sparse_resolution);
	if (master_cdo_size > 0)
		cpool_account_set_master_cdo_size(acct, master_cdo_size);

	if (max_acct_capacity > 0)
		cpool_account_set_max_capacity(acct, max_acct_capacity);

	cpool_account_set_check_cert(acct, false);

	try {
		uint64_t acct_auth_cap = 0;
		struct isi_error *error = NULL;
		///////在云端创建一个利用account拼接组合而成的bucket,metadatabucket
		//////以后这个账户(account)所有的cdo,cmo都放在这个大的bucket,metadatabucket中
		cpool_initializer::initialize_new_account(ctx, *acct);
		cpool_initializer::build_provider_auth_cap_for_account(ctx,
		    acct, &acct_auth_cap, &error);
	}
	catch(cl_exception &cl_err) {
		// ignore if the container already exists...
		if (cl_err.get_error_code() != CL_CONTAINER_ALREADY_EXISTS) {
			error = cl_err.detach();
		}
	}
	catch(isi_exception &err) {
		error = err.detach();
	}
	catch(const std::exception &e) {
		error = isi_exception_error_new("%s", e.what());
	}
	fail_if(error, "Failed to initialize the account: %#{}.\n",
	    isi_error_fmt(error));

	cpool_account_append(ctx, acct, &error);
	fail_if(error, "Failed to append the account to the GC: %#{}.\n",
	    isi_error_fmt(error));

	acct = NULL; // from now on acct is taken care of by gconfig

	cpool_context_commit(ctx, &error);
	fail_if(error, "Failed to save the GC change: %#{}.\n",
	    isi_error_fmt(error));

	if (acct)
		cpool_account_free(acct);

	cpool_context_close(ctx);
}

void
cbm_test_remove_provider(const char *provider_name)
{
	struct isi_error *error = NULL;

	// Now first create an account.
	cpool_config_context *ctx = cpool_context_open(&error);

	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	struct cpool_provider_instance *prv = cpool_provider_get_by_name(ctx,
	    provider_name, &error);

	fail_if(error && !isi_error_is_a(error, CPOOL_NOT_FOUND_ERROR_CLASS),
	    "Failed to get the provider: %#{}.\n",
	    isi_error_fmt(error));

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	if (prv) {
		cpool_provider_remove(ctx, prv, &error);
		fail_if(error, "Failed to remove the provider: %#{}.\n",
		    isi_error_fmt(error));

		cpool_context_commit(ctx, &error);
		fail_if(error, "Failed to save the GC change: %#{}.\n",
		    isi_error_fmt(error));

		cpool_provider_free(prv);
	}

	cpool_context_close(ctx);
}

void
cbm_test_create_provider(cl_provider_type ptype, const char *provider_name,
    const char *vendor_name, const char *desc, const char *account_name)
{
	struct isi_error *error = NULL;
	struct cpool_provider_instance *prov = NULL;

	// Now first create an account.
	cpool_config_context *ctx = cpool_context_open(&error);
	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	prov = cpool_provider_create(ctx, &error);

	fail_if(error, "Failed to create an provider: %#{}.\n",
	    isi_error_fmt(error));

	cpool_provider_set_type(prov, ptype);
	cpool_provider_set_vendor(prov, vendor_name);
	cpool_provider_set_name(prov, provider_name);
	cpool_provider_set_description(prov, desc);

	struct cpool_account *acct = NULL;

	acct = cpool_account_get_by_name(ctx, account_name, &error);

	fail_if(error, "Failed to get the account: %#{}.\n",
	    isi_error_fmt(error));

	cpool_provider_add_account(ctx, prov, acct, &error);
	fail_if(error, "Failed to add the account to the provider: %#{}.\n",
	    isi_error_fmt(error));

	cpool_provider_append(ctx, prov, &error);
	fail_if(error, "Failed to append the provider to the GC: %#{}.\n",
	    isi_error_fmt(error));

	prov = NULL; // from now on prov is taken care of by gconfig

	cpool_context_commit(ctx, &error);
	fail_if(error, "Failed to save the GC change: %#{}.\n",
	    isi_error_fmt(error));

	cpool_context_close(ctx);
}

void
cbm_test_remove_policy(const char *policy_name)
{
	isi_error *error = NULL;
	smartpools_context *ctx = NULL;

	smartpools_open_context(&ctx, &error);
	fail_if(error, "Failed to open the filepool config: %#{}.\n",
	    isi_error_fmt(error));

	fp_policy *pol = NULL;
	smartpools_get_policy_by_name(policy_name, &pol, ctx, &error);

	fail_if(error && !isi_error_is_a(error, ISI_EXCEPTION_ERROR_CLASS),
	    "Failed to get the policy: %#{}.\n",
	    isi_error_fmt(error));

	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	if (pol) {
		smartpools_remove_policy(pol, ctx, &error);
		fail_if(error, "Failed to remove the filepool policy: %#{}.\n",
		    isi_error_fmt(error));

		smartpools_save_all_policies(ctx, &error);
		fail_if(error, "Failed to save the filepool policy: %#{}.\n",
		    isi_error_fmt(error));

		smartpools_commit_changes(ctx, &error);
		fail_if(error, "Failed to commit the filepool policy: %#{}.\n",
		    isi_error_fmt(error));

		smartpools_free_policy(&pol);
	}

	smartpools_close_context(&ctx, &error);
	fail_if(error, "Failed to close the filepool config: %#{}.\n",
	    isi_error_fmt(error));
}

void
cbm_test_create_policy(const char *pol_name, const char *desc,
    const char *provider_name, bool final_match, bool compress,
    bool encrypt, bool random_io, bool archive_with_snapshots,
    fp_attr_cp_caching_strategy cache,
    fp_attr_cp_caching_readahead_strategy read_ahead,
    uint64_t invalidation_stime, uint64_t writeback_stime,
    std::list<simple_filter> *filters,
    uint64_t inc_bkup_retention, uint64_t full_bkup_retention,
    uint64_t delayed_inval_time)
{
	isi_error *error = NULL;
	smartpools_context *ctx = NULL;

	smartpools_open_context(&ctx, &error);
	fail_if(error, "Failed to open the filepool config: %#{}.\n",
	    isi_error_fmt(error));

	// Now first create an account.
	cpool_config_context *cp_ctx = cpool_context_open(&error);

	fail_if(error, "Failed to open the cpool config: %#{}.\n",
	    isi_error_fmt(error));

	fp_policy *pol = NULL;
	smartpools_create_policy(pol_name, desc, &pol, ctx,
	    &error);

	fail_if(error, "Failed to create the filepool policy: %#{}.\n",
	    isi_error_fmt(error));

	struct cpool_provider_instance *prv = cpool_provider_get_by_name(
	    cp_ctx, provider_name, &error);

	fail_if(error, "Failed to get the provider: %#{}.\n",
	    isi_error_fmt(error));

	int prov_id = cpool_provider_get_id(prv);
	const char *cluster_id = cpool_provider_get_cluster(prv);

	fp_cloudpool_action *action = pol->attributes->cloudpool_action;
	pol->attributes->cloudpool_action_is_set = true;

	action->cloud_encryption_enabled = encrypt;
	action->cloud_compression_enabled = compress;
	action->cloud_io_access_pattern =
	    random_io ? FP_ALP_RANDOM : FP_ALP_STREAMING_ACCESS;
	action->archive_snapshot_files = archive_with_snapshots;

	action->cloud_caching_strategy = cache;
	action->cloud_readahead_strategy = read_ahead;
	action->cloud_provider_id = prov_id;
	action->provider_birth_cluster = cluster_id ?
	    strdup(cluster_id) : NULL;

	cpool_context_close(cp_ctx);

	// TBD: make the following in the input
	action->cloud_garbage_retention = 0;
	action->cloud_inc_backup_retention = inc_bkup_retention;
	action->cloud_full_backup_retention = full_bkup_retention;
	action->cloud_cache_expiration_age = invalidation_stime;
	action->cloud_writeback_settle_time = writeback_stime;
	action->delayed_invalidation_time = delayed_inval_time;

	smartpools_append_policy(pol, ctx, &error);

	fail_if(error, "Failed to append the filepool policy: %#{}.\n",
	    isi_error_fmt(error));

	smartpools_save_all_policies(ctx, &error);
	fail_if(error, "Failed to save the filepool policy: %#{}.\n",
	    isi_error_fmt(error));
	// the following statement is causing a leak...
	smartpools_commit_changes(ctx, &error);
	fail_if(error, "Failed to commit the filepool policy: %#{}.\n",
	    isi_error_fmt(error));

	smartpools_close_context(&ctx, &error);
	fail_if(error, "Failed to close the filepool config: %#{}.\n",
	    isi_error_fmt(error));
}

#define DEFAULT_EXPIRE_TIME (10*60)
ifs_snapid_t
take_snapshot(const std::string &path, const std::string &snap,
    bool want_system, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_str snap_name, *paths[2];
	ifs_snapid_t snapid;
	time_t now, expire_time;

	isi_str_init(&snap_name, (char *)snap.c_str(), snap.length() + 1,
	    ENC_DEFAULT, ISI_STR_NO_MALLOC);
	paths[0] = isi_str_alloc((char *)path.c_str(), path.length() + 1,
	    ENC_DEFAULT, ISI_STR_NO_MALLOC);
	paths[1] = NULL;

	expire_time = 0;
	if (time(&now) != (time_t)-1)
		expire_time = now + DEFAULT_EXPIRE_TIME;

	snapid = snapshot_create(&snap_name, expire_time, paths, NULL,
	    &error);
	if (error != NULL) {
		isi_error_add_context(error);
		goto out;
	}
	if (snapid <= INVALID_SNAPID) {
		error = isi_system_error_new(EINVAL,
		    "snapshot_create(%s) succeeded "
		    "but returned an invalid snapid",
		    snap.c_str());
		goto out;
	}

	if (want_system) {
		snapshot_destroy(&snap_name, &error);
		if (error != NULL) {
			isi_error_add_context(error,
			    "snapshot_destroy(%s) failed: %{}",
			    snap.c_str(), isi_error_fmt(error));
			++snapid;
			goto out;
		}
	}

 out:
	isi_str_free(paths[0]);
	isi_error_handle(error, error_out);
	return snapid;
}

void
remove_snapshot(const std::string &snap)
{
	struct isi_str snap_name;
	isi_str_init(&snap_name, (char *)snap.c_str(),
	    snap.length() + 1, ENC_DEFAULT,
	    ISI_STR_NO_MALLOC);
	/* Ignore errors. */
	snapshot_destroy(&snap_name, NULL);
}

struct isi_error *
cbm_test_archive_file_with_pol_and_ckpt(uint64_t lin, const char *pol_name,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    bool retry_on_ESTALE)
{
	struct isi_error *error = NULL;
	int tries = (retry_on_ESTALE ? 2 : 1);

	while (tries--) {
		isi_cbm_archive_by_name(lin, (char *)pol_name,
		    NULL, NULL, get_ckpt_data_func,
		    set_ckpt_data_func, false, &error);
		if (error && tries &&
		    isi_cbm_error_is_a(error, CBM_STALE_STUB_ERROR)) {
			isi_error_free(error);
			error = NULL;
		} else {
			tries = 0;
		}
	}

	return error;
}

struct isi_error *
cbm_test_archive_file_with_pol(uint64_t lin, const char *pol_name,
    bool retry_on_ESTALE)
{
	return cbm_test_archive_file_with_pol_and_ckpt(lin, pol_name,
	    NULL, NULL, retry_on_ESTALE);
}

bool
cbm_test_encrypt_string(const char *in, size_t len,
    unsigned char *out,
    size_t *outlen,
    unsigned char *key,
    unsigned char *iv)
{
	int buflen, tmplen;

	EVP_CIPHER_CTX ctx;
	EVP_CIPHER_CTX_init(&ctx);
	EVP_EncryptInit_ex(&ctx, EVP_aes_256_cfb(), NULL, key, iv);

	if(!EVP_EncryptUpdate(&ctx, out, &buflen,
	    (unsigned char*)in, len))
	{
		return false;
	}

	if(!EVP_EncryptFinal_ex(&ctx, out + buflen, &tmplen))
	{
		return false;
	}

	buflen += tmplen;
	*outlen = buflen;
	EVP_CIPHER_CTX_cleanup(&ctx);

	return true;
}

const char *cbm_test_provider_str_from_type(cl_provider_type ptype)
{
	switch (ptype) {
	case CPT_AZURE:
		return "az";
	case CPT_RAN:
		return "ran";
	case CPT_AWS:
		return "s3";
	default:
		return "unknown";
	}
}

/********************************************************************
 * default test accounts for RAN, AZURE and AWS S3.
 *
 *********************************************************************/

static isi_cloud::cl_account ran_account = {
	RAN_TEST_ACCOUNT_URL,
	RAN_TEST_ACCOUNT_NAME,
	RAN_TEST_ACCOUNT_KEY,
	CPT_RAN,
	RAN_TEST_ACCOUNT_CAPACITY_URL,
	RAN_TEST_ACCOUNT_ID,
	"",
	AUTH_DEFAULT,
	false,
	{
		"",
		"",
		"",
		ISI_PROXY_TYPE_NONE,
		0
	},
	TEST_ACCOUNT_TIMEOUTS
};

static isi_cloud::cl_account az_account = {
	AZURE_TEST_ACCOUNT_URL,
	AZURE_TEST_ACCOUNT_NAME,
	AZURE_TEST_ACCOUNT_KEY,
	CPT_AZURE,
	AZURE_TEST_ACCOUNT_CAPACITY_URL,
	AZURE_TEST_ACCOUNT_ID,
	"",
	AUTH_DEFAULT,
	false,
	{
		"",
		"",
		"",
		ISI_PROXY_TYPE_NONE,
		0
	},
	TEST_ACCOUNT_TIMEOUTS
};

static isi_cloud::cl_account s3_account = {
	S3_TEST_ACCOUNT_URL,
	S3_TEST_ACCOUNT_NAME,
	S3_TEST_ACCOUNT_KEY,
	CPT_AWS,
	S3_TEST_ACCOUNT_CAPACITY_URL,
	S3_TEST_ACCOUNT_ID,
	"",
	AUTH_DEFAULT,
	false,
	{
		"",
		"",
		"",
		ISI_PROXY_TYPE_NONE,
		0
	},
	TEST_ACCOUNT_TIMEOUTS
};

cbm_test_env::cbm_test_env()
	: ran_acct_(ran_account), az_acct_(az_account), s3_acct_(s3_account)
{
	std::string test_url;

	if (test_is_hardened()) {
		if (test_make_url(test_url, "https", 8080, "/namespace/ifs") == 0) {
			ran_account.url = test_url;
		}
		else {
			fail("Unable to fetch external IP address");
		}
	}
}

void
cbm_test_env::setup_env(cbm_test_policy_spec *specs, size_t count)
{
	for (size_t i = 0; i < count; ++i){
		const cbm_test_policy_spec &spec = specs[i];
		cbm_test_remove_policy(spec.policy_name_.c_str());
	}

	for (size_t i = 0; i < count; ++i){
		const cbm_test_policy_spec &spec = specs[i];
		if (policies_.find(spec.policy_name_) == policies_.end()) {
			add_policy(spec);
		}
	}
}

void
cbm_test_env::cleanup()
{
	std::set<std::string>::iterator it;

	for (it = policies_.begin(); it != policies_.end(); ++it)
		cbm_test_remove_policy((*it).c_str());

	for (it = pools_.begin(); it != pools_.end(); ++it)
		cbm_test_remove_provider((*it).c_str());

	for (it = accounts_.begin(); it != accounts_.end(); ++it)
		cbm_test_remove_account((*it).c_str());

	policies_.clear();
	pools_.clear();
	accounts_.clear();
}

void
cbm_test_env::add_account(const cbm_test_policy_spec &spec)
{
	isi_cloud::cl_account *acct = NULL;
	if (spec.ptype_ == CPT_RAN) {
		acct = &ran_acct_;
	}
	else if (spec.ptype_ == CPT_AZURE) {
		acct = &az_acct_;
	}
	else if (spec.ptype_ == CPT_AWS) {
		acct = &s3_acct_;
	} else
		fail("Unsupported cloud type: %d", spec.ptype_);

	cbm_test_remove_provider(spec.pool_name_.c_str());

	cbm_test_remove_account(spec.account_name_.c_str());
	cbm_test_create_account(spec.ptype_,
	    spec.account_name_.c_str(),
	    "my cloud account",
	    acct->url.c_str(),
	    acct->name.c_str(),
	    acct->key.c_str(),
	    acct->capacity_url.c_str(),
	    acct->id,
	    spec.size_.chunksize_,
	    spec.size_.readsize_,
	    spec.size_.sparse_resolution_,
	    spec.size_.master_cdo_size_,
	    spec.size_.max_acct_capacity_);

	accounts_.insert(spec.account_name_);

	cbm_test_create_provider(spec.ptype_,
	    spec.pool_name_.c_str(),
	    "my cloud provider",
	    "nothing special",
	    spec.account_name_.c_str());

	pools_.insert(spec.pool_name_);
}

const char *cache_opt_to_str(fp_attr_cp_caching_strategy cache_opt)
{
	switch (cache_opt) {
	case FP_A_CP_CACHING_LOCAL:
		return "local";
	case FP_A_CP_CACHING_NOINLINE:
		return "ninline";
	case FP_A_CP_CACHING_PASSTHRU:
		return "passthru";
	default:
		return "unknown";
	}
}
const char *read_ahead_opt_to_str(fp_attr_cp_caching_readahead_strategy
    read_ahead_opt)
{
	switch (read_ahead_opt) {
	case FP_A_CP_CACHING_RA_PARTIAL:
		return "partial";
	case FP_A_CP_CACHING_RA_FULL:
		return "full";
	case FP_A_CP_CACHING_RA_DISABLED:
		return "disabled";
	default:
		return "unknown";
	}
}

void
cbm_test_env::add_policy(const cbm_test_policy_spec &spec)
{
	cbm_test_remove_policy(spec.policy_name_.c_str());
	if (accounts_.find(spec.account_name_) == accounts_.end())
		add_account(spec);

	cbm_test_create_policy(spec.policy_name_.c_str(), "my test policy",
	    spec.pool_name_.c_str(), true, spec.compress_, spec.encrypt_,
	    true, true, spec.cache_opt_, spec.read_ahead_opt_,
	    spec.invalidation_stime_, spec.writeback_stime_,
	    NULL, THREE_MONTHS, SEVEN_YEARS, spec.delayted_trunc_time_);
	policies_.insert(spec.policy_name_);
}

cbm_test_policy_spec::cbm_test_policy_spec(cl_provider_type ptype,
    bool compress /* = false */,
    bool encrypt /* = false */ , isi_cbm_test_sizes size /*= standard_size*/,
    fp_attr_cp_caching_strategy cache_opt, /*=
    FP_A_CP_CACHING_LOCAL, */
    fp_attr_cp_caching_readahead_strategy read_ahead_opt /* =
    FP_A_CP_CACHING_RA_PARTIAL*/,
    uint64_t invalidation_stime /* = 0 */,
    uint64_t writeback_stime /*= 0 */,
    uint64_t delayted_trunc_time /* = 0 */)
    : ptype_(ptype), compress_(compress),
    encrypt_(encrypt), size_(size),
    cache_opt_(cache_opt), read_ahead_opt_(read_ahead_opt),
    invalidation_stime_(invalidation_stime),
    writeback_stime_(writeback_stime),
    delayted_trunc_time_(delayted_trunc_time)
{
	char buf[1024];
	sprintf(buf, "pol_%s_%s_%s_%ld_%ld_%ld_%s_%s_%ld_%ld_%ld",
	    cbm_test_provider_str_from_type(ptype),
	    compress? "c" : "nc",
	    encrypt? "e" : "ne",
	    size_.readsize_, size_.chunksize_, size_.master_cdo_size_,
	    cache_opt_to_str(cache_opt),
	    read_ahead_opt_to_str(read_ahead_opt),
	    invalidation_stime_, writeback_stime_, delayted_trunc_time_);

	policy_name_ = buf;

	sprintf(buf, "poo_%s_%ld_%ld_%ld",
	    cbm_test_provider_str_from_type(ptype),
	    size_.readsize_, size_.chunksize_, size_.master_cdo_size_);
	pool_name_ = buf;

	sprintf(buf, "acc_%s_%ld_%ld_%ld",
	    cbm_test_provider_str_from_type(ptype),
	    size_.readsize_, size_.chunksize_, size_.master_cdo_size_);
	account_name_ = buf;
}

cbm_sparse_test_file_helper::cbm_sparse_test_file_helper(
    const char *fname, size_t fsize,
    size_t seg_count, struct segment *segments) : file_(fname)
{
	cbm_test_create_sparse_file(
	    fname, fsize, seg_count, segments, &stat_);
}

cbm_sparse_test_file_helper::~cbm_sparse_test_file_helper()
{
	//
	// We do NOT remove the sparse file here on purpose.
	// It is up to the test to clean up and unlink the file.
	// This is done to allow us to leave the file around for
	// inspection after a failure.
	//
}

cbm_test_file_helper::cbm_test_file_helper(const char *fname,
    size_t fsize, const char *txt) : file_(fname)
{
	create_and_fill(fsize, std::string(txt));
}

cbm_test_file_helper::cbm_test_file_helper(const char *fname,
    size_t fsize, const std::string &txt) : file_(fname)
{
	create_and_fill(fsize, txt);
}

void
cbm_test_file_helper::create_and_fill(size_t fsize, const std::string &txt)
{
	int fd_tmp = -1;
	int retc = 0;
	ssize_t this_wrt = 0;
	size_t total_wrt = 0;
	const char *ptxt = NULL;

	std::string filltxt = (txt.length() != 0) ? txt :
	    "01234567890123456789012345678901234567890123456789\n";
	size_t buf_sz = filltxt.length();

	while (fsize > 2 * buf_sz && buf_sz < 512000) {
		filltxt += filltxt;
		buf_sz = filltxt.length();
	}
	ptxt = filltxt.c_str();

	retc = remove(file_.c_str());
	fail_if((retc < 0 && errno != ENOENT),
	    "removing file failed %s", strerror(errno));

	retc = 0;

	fd_tmp = open(file_.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0600);

	fail_if(fd_tmp < 0, "cannot create file for write");

	while (total_wrt < fsize) {
		ssize_t sz_to_wrt = fsize - total_wrt;
		if (sz_to_wrt > (ssize_t) buf_sz)
			sz_to_wrt = buf_sz;
		this_wrt = write(fd_tmp, ptxt, sz_to_wrt);
		total_wrt += this_wrt;

		fail_if(this_wrt < sz_to_wrt, "failed to write to file");
	}

	retc = fstat(fd_tmp, &stat_);
	fail_if(retc < 0, "failed to stat file: %s", strerror(errno));

	fsync(fd_tmp);
	close(fd_tmp);
}

cbm_test_file_helper::~cbm_test_file_helper()
{
	//int retc = remove(file_.c_str());  jjz test
	// file may be removed by caller explicitly
	// fail_if(retc < 0 && errno != ENOENT,
	//     "removing file failed %s", strerror(errno));
}

bool cbm_test_cpool_ufp_init()
{
	static bool fail_point_init = false;
	if (fail_point_init)
		return true;
	if (!UFAIL_POINT_INIT("isi_check")) {
		fail_point_init = true;
		return true;
	}
	return false;
}

void
cbm_test_enable_atime_updates()
{
	struct isi_error *error = NULL;
	isi_cbm_enable_atime_updates(&error);
	fail_if(error, "Could not enable atime updates: %#{}",
	    isi_error_fmt(error));
}

void
cbm_test_disable_atime_updates()
{
	struct isi_error *error = NULL;
	isi_cbm_disable_atime_updates(&error);
	fail_if(error, "Could not disable atime updates: %#{}",
	    isi_error_fmt(error));
}

void
cbm_test_ensure_permit_for_local_cluster()
{
	struct isi_error *error = NULL;

	// Pre-load this cluster's guid in the permit list
	struct smartpools_context *sp_context;
	smartpools_open_context(&sp_context, &error);
	fail_if (error != NULL, "unexpected error opening context: %#{}",
	    isi_error_fmt(error));

	char *current_guid = NULL;
	cpool_get_cluster_guid(&current_guid, &error);
	fail_if (error != NULL, "unexpected error getting cluster guid: %#{}",
	    isi_error_fmt(error));

	smartpools_set_guid_state(sp_context, CM_A_ST_ENABLED, current_guid);
	smartpools_save_permit_list(sp_context, &error);
	fail_if (error != NULL, "unexpected error saving cluster guid: %#{}",
	    isi_error_fmt(error));

	smartpools_commit_changes(sp_context, &error);
	fail_if(error, "error on smartpools commit %#{}",
	    isi_error_fmt(error));

	smartpools_close_context(&sp_context, &error);
	fail_if (error != NULL, "unexpected error closing context: %#{}",
	    isi_error_fmt(error));

	delete current_guid;
}

