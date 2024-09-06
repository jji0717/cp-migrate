#ifndef _ISI_CBM_TEST_UTIL_H_
#define _ISI_CBM_TEST_UTIL_H_

#include <errno.h>
#include <list>

#include <ifs/ifs_types.h>
#include <isi_cloud_api/cl_provider_common.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/cpool_test_fail_point.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_file_filter/file_filter.h>
#include <isi_pools_fsa_gcfg/isi_pools_fsa_gcfg.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_util/isi_error.h>
#include "check_cbm_common.h"

/**
 * routine to archive a file specified by lin with the policy
 */
struct isi_error *cbm_test_archive_file_with_pol(uint64_t lin,
    const char *pol_name, bool retry_on_ESTALE);

/**
 * routine to archive a file specified by lin with the policy
 * and allowing for checkpointing options.
 */
struct isi_error *cbm_test_archive_file_with_pol_and_ckpt(uint64_t lin,
    const char *pol_name, 
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    bool retry_on_ESTALE);

/**
 * compare two files' content
 * @return 0 if two files has same content; non-zero otherwise
 */
int file_compare(const char *src, const char *dest);

/**
 * get cloud service provider from gconfig.
 * @return csp cloud service provider struct for the cloud provider type.
 */
struct cloud_service_provider *
cbm_test_get_cloud_service_provider(enum cl_provider_type ptype);


/**
 * get max account capacity based on the cloud provider type.
 * @return max_acct_capacity: max account capacity supported by the cloud
 *    service provider.
 */
uint64_t get_csp_max_capacity(enum cl_provider_type ptype);


/**
 * remove the specified account identified by acct_name
 */
void cbm_test_remove_account(const char *acct_name);

/**
 * create a account with the name, vendor, url, user name and key given.
 */
void cbm_test_create_account(cl_provider_type ptype,
    const char *account_name,
    const char *vendor_name,
    const char *url,
    const char *user_name,
    const char *key,
    const char *capacity_url,
    const uint64_t user_id = 0,
    size_t chunksize = 0,
    size_t readsize = 0,
    uint32_t sparse_resolution = 0,
    size_t master_cdo_size = 0,
    size_t max_acct_capacity = 0 );

/**
 * remove the provider specified by the provider_name.
 */
void cbm_test_remove_provider(const char *provider_name);

/**
 * create a provider instance with the name, vendor and description.
 * It requires the account with the account_name be existing.
 */
void cbm_test_create_provider(cl_provider_type ptype,
    const char *provider_name, const char *vendor_name,
    const char *desc, const char *account_name);

/**
 * remove the policy specified by policy_name.
 */
void cbm_test_remove_policy(const char *policy_name);

/**
 * Ensure local cluster is in the permitted list to
 * access the stubs.
 */
void cbm_test_ensure_permit_for_local_cluster();

// simple filter doing
struct simple_filter {
	fp_filter_type filter_type;
	const char *value;
};

#define THREE_MONTHS 3*30*24*60*60
#define SEVEN_YEARS 7*12*30*24*60*60

/**
 * Create cloud pool policy. The provider instance specified by provider_name
 * must have been created.
 */
void cbm_test_create_policy(const char *pol_name, const char *desc,
    const char *provider_name, bool final_match, bool compress,
    bool encrypt, bool random_io, bool archive_with_snapshots,
    fp_attr_cp_caching_strategy cache,
    fp_attr_cp_caching_readahead_strategy read_ahead,
    uint64_t invalidation_stime = 0, uint64_t writeback_stime = 0,
    std::list<simple_filter> *filters = NULL,
    uint64_t inc_bkup_retention = THREE_MONTHS,	// 3 months
    uint64_t full_bkup_retention = SEVEN_YEARS,	// 7 years
    uint64_t delayted_trunc_time = 0
);

/**
 * Utility funciton to fill a buffer with encrypted content. This is helpful
 * to fill the buffer with non-compressible data.
 */
bool cbm_test_encrypt_string(const char *in, size_t len,
    unsigned char *out,
    size_t *outlen,
    unsigned char *key,
    unsigned char *iv);

/**
 * Utility to take a snapshot of a file.
 */
#define DEFAULT_EXPIRE_TIME (10*60)
ifs_snapid_t
take_snapshot(const std::string &path, const std::string &snap,
    bool want_system, struct isi_error **error_out);

/**
 * Utility to remove a snapshot on a file.
 */
void
remove_snapshot(const std::string &snap);

struct isi_cbm_test_sizes {
	size_t chunksize_;
	size_t readsize_;
	size_t master_cdo_size_;
	size_t max_acct_capacity_;
	uint32_t sparse_resolution_;

	explicit isi_cbm_test_sizes(size_t chunksize = 0, size_t readsize = 0,
	    size_t master_cdo_size = 0, size_t max_acct_capacity = 0,
	    uint32_t sparse_resolution = 0) :
	        chunksize_(chunksize), readsize_(readsize),
		master_cdo_size_(master_cdo_size),
		max_acct_capacity_(max_acct_capacity),
		sparse_resolution_(sparse_resolution) {}
};

const isi_cbm_test_sizes standard_size;

/**
 * A policy specification
 */
struct cbm_test_policy_spec {

	cbm_test_policy_spec(cl_provider_type ptype, bool compress = false,
	    bool encrypt = false, isi_cbm_test_sizes size =  standard_size,
	    fp_attr_cp_caching_strategy cache_opt =
	    FP_A_CP_CACHING_LOCAL,
	    fp_attr_cp_caching_readahead_strategy read_ahead_opt_ =
	    FP_A_CP_CACHING_RA_PARTIAL,
	    uint64_t invalidation_stime = 0,
	    uint64_t writeback_stime = 0,
	    uint64_t delayted_trunc_time = 0);

	cl_provider_type ptype_;
	bool compress_;
	bool encrypt_;
	isi_cbm_test_sizes size_;
	fp_attr_cp_caching_strategy cache_opt_;
	/**
	 * read ahead policy
	 */
	fp_attr_cp_caching_readahead_strategy read_ahead_opt_;
	uint64_t invalidation_stime_;
	uint64_t writeback_stime_;
	uint64_t delayted_trunc_time_;
	std::string policy_name_;
	std::string account_name_;
	std::string pool_name_;

};

/**
 * class encapsualting the policy need.
 */
class cbm_test_env {
public:
	/**
	 * construct a test env using default account information
	 */
	cbm_test_env();

	/**
	 * construct the test env using the given account information
	 */
	cbm_test_env(isi_cloud::cl_account &ran_acct,
	    isi_cloud::cl_account &az_acct, isi_cloud::cl_account &s3_acct) :
	    ran_acct_(ran_acct), az_acct_(az_acct), s3_acct_(s3_acct) { }

	// add a list of specs
	void setup_env(cbm_test_policy_spec *specs, size_t count);

	/*
	 * cleanup the env
	 */
	void cleanup();

private:

	/**
	 * add a policy
	 */
	void add_policy(const cbm_test_policy_spec &spec);

	/**
	 * add an account and the pool
	 */
	void add_account(const cbm_test_policy_spec &spec);

	std::set<std::string> accounts_;
	std::set<std::string> pools_;
	std::set<std::string> policies_;
	isi_cloud::cl_account &ran_acct_;
	isi_cloud::cl_account &az_acct_;
	isi_cloud::cl_account &s3_acct_;

};


class cbm_sparse_test_file_helper {
public:
	/**
	 * Create a sparse file of given name, size, and description
	 * Fill the non-sparse areas with a known nonzero pattern.
	 * @param fname[IN] name of the test file
	 * @param fsize[IN] size of file to be created
	 * @param seg_count[IN] segment count for the segments parameter
	 * @param segments[IN] decription of the segments to write
	 */
	cbm_sparse_test_file_helper(const char *fname,
	    size_t filesize, size_t num_segments, struct segment *segments);

	/**
	 * We do NOT remove the sparse file in the destructor on
	 * purpose. It is up to the test to clean up and unlink the
	 * file. This is done to allow us to leave the file around for
	 * inspection after a failure.
	 */
	~cbm_sparse_test_file_helper();

	ifs_lin_t get_lin() { return stat_.st_ino; }
	struct stat get_stat() { return stat_; }
	const char *get_filename() { return file_.c_str(); }
	int remove_file() {
		return remove(file_.c_str());
	}

private:
	std::string file_;
	struct stat stat_;
};



class cbm_test_file_helper {
public:
	/**
	 * Create a file of given name and size, fill it with given text
	 * @param fname[IN] name of the test file
	 * @param fsize[IN] size of file to be created
	 * @param txt[IN] content for the file. If it is smaller than file
	 *                size, then it is written repeatedly up to file size
	 */
	cbm_test_file_helper(const char *fname, size_t fsize,
	    const std::string &txt);
	cbm_test_file_helper(const char *fname, size_t fsize,
	    const char *txt);
	~cbm_test_file_helper();

	ifs_lin_t get_lin() { return stat_.st_ino; }
	struct stat get_stat() { return stat_; }
	const char *get_filename() { return file_.c_str(); }

private:
	void create_and_fill(size_t fsize, const std::string &txt);

private:
	std::string file_;
	struct stat stat_;
};

bool cbm_test_cpool_ufp_init();

#endif //


