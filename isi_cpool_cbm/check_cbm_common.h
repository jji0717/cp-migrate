#ifndef _CHECK_CBM_COMMON_H_
#define _CHECK_CBM_COMMON_H_

/*
 * This is C header file.
 */

#include <errno.h>

#include <ifs/ifs_types.h>
#include <isi_pools_fsa_gcfg/isi_pools_fsa_gcfg.h>

__BEGIN_DECLS

#define CBM_TEST_COMMON_ACCT_RAN "cbm_test_common_ran_acct"
#define CBM_TEST_COMMON_POOL_RAN "cbm_test_common_ran_pool"
#define CBM_TEST_COMMON_POLICY_RAN "cbm_test_common_ran_policy"
#define CBM_TEST_COMMON_POLICY_RAN_C "cbm_test_common_ran_policy_c"
#define CBM_TEST_COMMON_POLICY_RAN_E "cbm_test_common_ran_policy_e"
#define CBM_TEST_COMMON_POLICY_RAN_BOTH "cbm_test_common_ran_policy_c_e"
#define CBM_TEST_COMMON_POLICY_RAN_NO_SNAPSHOT "cbm_test_common_ran_no_snapshot"
#define CBM_TEST_COMMON_POLICY_RAN_INVALIDATE "cbm_test_common_ran_policy_inval"
#define CBM_TEST_COMMON_POLICY_RAN_FULL_RA "cbm_test_common_ran_policy_full_ra"

#define CBM_TEST_COMMON_ACCT_AZ "cbm_test_common_az_acct"
#define CBM_TEST_COMMON_POOL_AZ "cbm_test_common_az_pool"
#define CBM_TEST_COMMON_POLICY_AZ "cbm_test_common_az_policy"
#define CBM_TEST_COMMON_POLICY_AZ_C "cbm_test_common_az_policy_c"
#define CBM_TEST_COMMON_POLICY_AZ_E "cbm_test_common_az_policy_e"
#define CBM_TEST_COMMON_POLICY_AZ_BOTH "cbm_test_common_az_policy_c_e"

#define CBM_TEST_COMMON_ACCT_AWS_S3 "cbm_test_common_aws_s3_acct"
#define CBM_TEST_COMMON_POOL_AWS_S3 "cbm_test_common_aws_s3_pool"
#define CBM_TEST_COMMON_POLICY_AWS_S3 "cbm_test_common_aws_s3_policy"

#define FIVE_MINUTES	60*5
#define TWO_MINUTES	60*2

#ifdef CHECK_CBM_ENABLE_TRACE
#ifdef ENABLE_ILOG_USE
#define CHECK_P(args...) {\
	fflush(stderr);\
	fprintf(stderr, args); \
	fflush(stderr);\
	ilog(IL_DEBUG, args);\
	}
#else
#define CHECK_P(args...) fflush(stderr);fprintf(stderr, args);fflush(stderr);
#endif
#else
#define CHECK_P(args...)
#endif

#define CHECK_TRACE(args...) fflush(stderr);fprintf(stdout, args);fflush(stderr);

enum copy_file_vals {
	REPLACE_FILE = 1,
	REMOVE_FILE,
	LEAVE_FILE,
};

struct segment {
	int byte_offset;
	int byte_len;
};

#define SVAL_BLK(o, l)  {((o) * IFS_BSIZE), ((l) * IFS_BSIZE)}
#define SVAL_BYTE(o, l) {(o), (l)}
#define SEGMENTS(s) {s},
#define SEGMENT_VECTOR(nme, s) struct segment nme[] = {s}
#define MAX_EXPECTED_MAPS	5
#define MAX_SPARSE_MAP_SIZE	64
#define MAX_SPARSE_MAP_SEGMENTS	32
#define SIZE_128K_BYTES			(128*1024)
#define SIZE_1M_BYTES			(1024*1024)

struct  sparse_file_info {
	const char *spf_name;
	size_t resolution;
	unsigned char expected_map[MAX_EXPECTED_MAPS][MAX_SPARSE_MAP_SIZE];
	size_t map_size;
	size_t filesize;    // for these tests this should be <= region_size
	size_t region_size; // multiple of block resolution size
	size_t chunk_size;  // chunk size represented by each map
	size_t num_segments;// number of segments below
	struct segment segments[MAX_SPARSE_MAP_SEGMENTS];
};

struct  test_file_info {
	const char *input_file;
	const char *output_file;
	enum copy_file_vals copy_file;
};

//
// references to globally available sparse file definitions
//
extern struct sparse_file_info sparse_1M;
extern struct sparse_file_info sparse_1Ma;
extern struct sparse_file_info sparse_none;
extern struct sparse_file_info sparse_all;
extern struct sparse_file_info sparse_alt_odd;
extern struct sparse_file_info sparse_alt_even;
extern struct sparse_file_info sparse_short;
extern struct sparse_file_info sparse_alt_odd_byte;
extern struct sparse_file_info sparse_alt_even_byte;
extern struct sparse_file_info sparse_alt_odd_byte_2m;
extern struct sparse_file_info sparse_alt_even_byte_2m;

/**
 * A group of helper routines for setting up test env.
 */

void cbm_test_common_delete_ran_provider_account(void);

void cbm_test_common_create_ran_provider_account(void);

void cbm_test_common_delete_az_provider_account(void);

void cbm_test_common_create_az_provider_account(void);

void cbm_test_common_delete_aws_s3_provider_account(void);

void cbm_test_common_create_aws_s3_provider_account(void);

void cbm_test_common_add_policy(const char *provider_name,
    const char *pol_name, bool final_match, bool compress, bool encrypt,
    bool random_io, bool archive_with_snapshots,
    enum fp_attr_cp_caching_strategy cache,
    enum fp_attr_cp_caching_readahead_strategy read_ahead);

void cbm_test_common_delete_policy(const char *pol_name);

void cbm_test_common_delete_all_policy(bool ran_only);

/**
 * A routine to avoid false positive "leak"
 */
void cbm_test_leak_check_primer(bool access_cloud);

/**
 * A routine to exercise access cloud to avoid false positive "leak"
 */
void cbm_test_access_cloud_object(void);

/**
 * routine expect to be called by setup_suite
 */
void cbm_test_common_setup(bool ran_only, bool create_policies);

/**
 * routine expect to be called by cleanup_suite
 */
void cbm_test_common_cleanup(bool ran_only, bool clean_policies);

/**
 * routine to point gconfig to a unit test namespace (preserves live config)
 */
void cbm_test_common_setup_gconfig(void);

/**
 * routine to cleanup gconfig unit test namespace
 */
void cbm_test_common_cleanup_gconfig(void);

void cbm_test_enable_stub_access(void);
void cbm_test_disable_stub_access(void);
void cbm_test_create_sparse_file(const char *fname,
    size_t filesize, size_t num_segments,
    struct segment *segments, struct stat *stat_buf);
bool compare_maps_to_spf(struct sparse_file_info *spf,
    const char *filename, bool expect_spamap_match);
void cbm_test_enable_atime_updates(void);
void cbm_test_disable_atime_updates(void);

/**
 * routines for temporarily disabling/enabling cloud_api level network logging
 * and reverting logging to previous state
 */
void override_clapi_verbose_logging_enabled(bool allow_logging);
void revert_clapi_verbose_logging(void);

__END_DECLS


#define CBM_ACCESS_ON	"/usr/bin/isi_run -c "
#define CBM_ACCESS_OFF	""

#endif //


