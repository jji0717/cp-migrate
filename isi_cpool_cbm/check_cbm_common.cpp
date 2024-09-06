#include "check_cbm_common.h"

#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cloud_api/test_stream.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_cbm/isi_cbm_test_util.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_access.h>
#include <isi_cpool_cbm/isi_cbm_file.h>
#include <isi_cpool_cbm/isi_cbm_sparse.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_stats/cpool_stats_unit_test_helper.h>
#include <isi_cpool_stats/cpool_stat_manager.h>
#include <isi_cpool_cbm/check_hardening.h>

#include <isi_util/isi_error.h>
#include <check.h>

using namespace isi_cloud;

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

static callback_params cb_params;

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

//
// references to globally available sparse file definitions
//

#define TF_1M \
	SVAL_BLK(0, 4), \
	SVAL_BLK(8, 4), \
	SVAL_BLK(32,2), \
	SVAL_BLK(68,1),

struct sparse_file_info sparse_1M = {
	"1M",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0xf0,0xf0,0xff,0xff,0xfc,0xff,0xff,0xff,
	  0xef,0xff,0xff,0xff,0xff,0xff,0xff,0xff},{0},{0},{0},{0}},
	16,			// 16 bytes in map
	SIZE_1M_BYTES,		// filesise = 1M
	SIZE_128K_BYTES,	// 128K region size
	SIZE_1M_BYTES,		// 1M chunk size
	4,			// number of segments to write
	SEGMENTS(TF_1M)		// the segments
};

#define TF_1Ma \
	SVAL_BLK(0, 4), \
	SVAL_BLK(8, 4), \
	SVAL_BLK(32,2), \
	SVAL_BLK(68,1), \
	SVAL_BLK(88,8),

struct sparse_file_info sparse_1Ma = {
	"1Ma",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0xf0,0xf0,0xff,0xff,0xfc,0xff,0xff,0xff,
	  0xef,0xff,0xff,0x00,0xff,0xff,0xff,0xff},{0},{0},{0},{0}},
	16,			// 16 bytes in map
	SIZE_1M_BYTES,		// filesise = 1M
	SIZE_128K_BYTES,	// 128K region size
	SIZE_1M_BYTES,		// 1M chunk size
	5,			// number of segments to write
	SEGMENTS(TF_1Ma)	// the segments
};

#define TF_NONE \
	SVAL_BLK(0,16),

struct sparse_file_info sparse_none = {
	"NONE",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0x00, 0x00},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	1,			// number of segments to write
	SEGMENTS(TF_NONE)	// the segments
};

#define TF_ALL \
	SVAL_BLK(0,0),

struct sparse_file_info sparse_all = {
	"ALL",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0xff, 0xff},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	1,			// number of segments to write
	SEGMENTS(TF_ALL)	// the segments
};

#define TF_ALT_ODD  \
	SVAL_BLK(1,1),  \
	SVAL_BLK(3,1),  \
	SVAL_BLK(5,1),  \
	SVAL_BLK(7,1),  \
	SVAL_BLK(9,1),  \
	SVAL_BLK(11,1), \
	SVAL_BLK(13,1), \
	SVAL_BLK(15,1),

struct sparse_file_info sparse_alt_odd = {
	"ODD",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0x55, 0x55},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	8,			// number of segments to write
	SEGMENTS(TF_ALT_ODD)	// the segments
};

#define TF_ALT_EVEN \
	SVAL_BLK(0,1),  \
	SVAL_BLK(2,1),  \
	SVAL_BLK(4,1),  \
	SVAL_BLK(6,1),  \
	SVAL_BLK(8,1),  \
	SVAL_BLK(10,1), \
	SVAL_BLK(12,1), \
	SVAL_BLK(14,1),

struct sparse_file_info sparse_alt_even = {
	"EVEN",			// sparse file info name
	IFS_BSIZE,		// resolution
	{{0xaa, 0xaa},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	8,			// number of segments to write
	SEGMENTS(TF_ALT_EVEN)	// the segments
};

#define TF_SHORT    \
	SVAL_BLK(0,1),  \
	SVAL_BLK(3,1),  \
	SVAL_BLK(6,3),  \
	SVAL_BLK(10,4),

struct sparse_file_info sparse_short = {
	"SHORT",		// sparse file info name
	IFS_BSIZE,		// resolution
	{{0x36, 0x02},{0},{0},{0},{0}},
	2,			// bytes in map
	14 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	4,			// number of segments to write
	SEGMENTS(TF_SHORT)
};

#define TF_ALT_ODD_BYTE  \
	SVAL_BYTE((  1 * IFS_BSIZE), 1),  \
	SVAL_BYTE((( 3 * IFS_BSIZE) + 1), 1),  \
	SVAL_BYTE((( 5 * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 1),  \
	SVAL_BYTE((( 7 * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((( 9 * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE(((11 * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE(((13 * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE(((15 * IFS_BSIZE)), 10), \
	SVAL_BYTE(((15 * IFS_BSIZE) + (IFS_BSIZE - 10)), 10),

struct sparse_file_info sparse_alt_odd_byte = {
	"ODD_BYTE",		// sparse file info name
	IFS_BSIZE,		// resolution
	{{0x55, 0x55},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	9,			// number of segments to write
	SEGMENTS(TF_ALT_ODD_BYTE) // the segments
};

#define TF_ALT_EVEN_BYTE \
	SVAL_BYTE((  0 * IFS_BSIZE), 1),  \
	SVAL_BYTE((( 2 * IFS_BSIZE) + 1), 1),  \
	SVAL_BYTE((( 4 * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 1),  \
	SVAL_BYTE((( 6 * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((( 8 * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE(((10 * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE(((12 * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE(((14 * IFS_BSIZE) + 1), 10), \
	SVAL_BYTE(((14 * IFS_BSIZE) + (IFS_BSIZE - 10)), 10),

struct sparse_file_info sparse_alt_even_byte  = {
	"EVEN_BYTE",		// sparse file info name
	IFS_BSIZE,		// resolution
	{{0xaa, 0xaa},{0},{0},{0},{0}},
	2,			// bytes in map
	16 * IFS_BSIZE,		// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	9,			// number of segments to write
	SEGMENTS(TF_ALT_EVEN_BYTE) // the segments
};

#define TF_ALT_ODD_BYTE_2M  \
	SVAL_BYTE((  1 * IFS_BSIZE), 3),  \
	SVAL_BYTE((( 3 * IFS_BSIZE) + 1), 4),  \
	SVAL_BYTE((( 5 * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 3),  \
	SVAL_BYTE((( 7 * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((( 9 * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE(((11 * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE(((13 * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE(((15 * IFS_BSIZE)), 10), \
	SVAL_BYTE(((15 * IFS_BSIZE) + (IFS_BSIZE - 10)), 10), \
	SVAL_BYTE(( (128  + 1) * IFS_BSIZE), 5),  \
	SVAL_BYTE((((128  + 3) * IFS_BSIZE) + 1), 7),  \
	SVAL_BYTE((((128  + 5) * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 1),  \
	SVAL_BYTE((((128  + 7) * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((((128  + 9) * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE((((128 + 11) * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE((((128 + 13) * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE((((128 + 15) * IFS_BSIZE)), 10), \
	SVAL_BYTE((((128 + 15) * IFS_BSIZE) + (IFS_BSIZE - 10)), 10)

struct sparse_file_info sparse_alt_odd_byte_2m = {
	"ODD_BYTE_2M",		// sparse file info name
	IFS_BSIZE,		// resolution
	{
	{0x55, 0x55, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{0x55, 0x55, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{0},{0},{0}},
	16,			// bytes in map
	(2 * SIZE_1M_BYTES),	// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	18,			// number of segments to write
	SEGMENTS(TF_ALT_ODD_BYTE_2M) // the segments
};

#define TF_ALT_EVEN_BYTE_2M  \
	SVAL_BYTE((  0 * IFS_BSIZE), 2),  \
	SVAL_BYTE((( 2 * IFS_BSIZE) + 1), 3),  \
	SVAL_BYTE((( 4 * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 4),  \
	SVAL_BYTE((( 6 * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((( 8 * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE(((10 * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE(((12 * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE(((14 * IFS_BSIZE)), 10), \
	SVAL_BYTE(((14 * IFS_BSIZE) + (IFS_BSIZE - 10)), 10), \
	SVAL_BYTE(( (128  + 0) * IFS_BSIZE), 1),  \
	SVAL_BYTE((((128  + 2) * IFS_BSIZE) + 1), 1),  \
	SVAL_BYTE((((128  + 4) * IFS_BSIZE) + ((IFS_BSIZE + 1) / 2)), 1),  \
	SVAL_BYTE((((128  + 6) * IFS_BSIZE) + (IFS_BSIZE - 1)), 1),  \
	SVAL_BYTE((((128  + 8) * IFS_BSIZE) + 1), 5),  \
	SVAL_BYTE((((128 + 10) * IFS_BSIZE) + (IFS_BSIZE / 2)), 5),  \
	SVAL_BYTE((((128 + 12) * IFS_BSIZE) + (IFS_BSIZE - 5)), 5),  \
	SVAL_BYTE((((128 + 14) * IFS_BSIZE)), 10), \
	SVAL_BYTE((((128 + 14) * IFS_BSIZE) + (IFS_BSIZE - 10)), 10)

struct sparse_file_info sparse_alt_even_byte_2m = {
	"EVEN_BYTE_2M",		// sparse file info name
	IFS_BSIZE,		// resolution
	{
	{0xaa, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{0xaa, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{0},{0},{0}},
	16,			// bytes in map
	(2 * SIZE_1M_BYTES),	// Filesize
	SIZE_128K_BYTES,	// region size
	SIZE_1M_BYTES,		// 1M chunk size
	18,			// number of segments to write
	SEGMENTS(TF_ALT_EVEN_BYTE_2M) // the segments
};



void
cbm_test_common_delete_ran_provider_account()
{
	cbm_test_remove_provider(CBM_TEST_COMMON_POOL_RAN);
	cbm_test_remove_account(CBM_TEST_COMMON_ACCT_RAN);
}

void
cbm_test_common_create_ran_provider_account()
{
	std::string test_url;

	cbm_test_common_delete_ran_provider_account();

	if (test_is_hardened()) {
		if (test_make_url(test_url, "https", 8080, "/namespace/ifs") == 0) {
			ran_account.url = test_url;
		}
		else {
			fail("Unable to fetch external IP address");
		}
	}

	cbm_test_create_account(ran_account.type, CBM_TEST_COMMON_ACCT_RAN,
	    "my ran cloud account",
	    ran_account.url.c_str(), ran_account.name.c_str(),
	    ran_account.key.c_str(), "", ran_account.id);

	cbm_test_create_provider(ran_account.type, CBM_TEST_COMMON_POOL_RAN,
	    "my ran provider",
	    "nothing special", CBM_TEST_COMMON_ACCT_RAN);
}

void
cbm_test_common_delete_az_provider_account()
{
	cbm_test_remove_provider(CBM_TEST_COMMON_POOL_AZ);
	cbm_test_remove_account(CBM_TEST_COMMON_ACCT_AZ);
}

void
cbm_test_common_create_az_provider_account()
{
	cbm_test_common_delete_az_provider_account();

	uint64_t azure_max_acct_capacity = get_csp_max_capacity(az_account.type);

	cbm_test_create_account(az_account.type, CBM_TEST_COMMON_ACCT_AZ,
	    "my azure cloud account",
	    az_account.url.c_str(), az_account.name.c_str(),
	    az_account.key.c_str(), az_account.capacity_url.c_str(),
	    az_account.id, 0, 0, 0, 0, (size_t)azure_max_acct_capacity);

	cbm_test_create_provider(az_account.type, CBM_TEST_COMMON_POOL_AZ,
	    "my azure provider",
	    "nothing special", CBM_TEST_COMMON_ACCT_AZ);
}


void
cbm_test_common_delete_aws_s3_provider_account()
{
	cbm_test_remove_provider(CBM_TEST_COMMON_POOL_AWS_S3);
	cbm_test_remove_account(CBM_TEST_COMMON_ACCT_AWS_S3);
}

void
cbm_test_common_create_aws_s3_provider_account()
{
	cbm_test_common_delete_aws_s3_provider_account();

	uint64_t aws_s3_max_acct_capacity = get_csp_max_capacity(s3_account.type);

	cbm_test_create_account(s3_account.type, CBM_TEST_COMMON_ACCT_AWS_S3,
	    "my aws S3 cloud account",
	    s3_account.url.c_str(), s3_account.name.c_str(),
	    s3_account.key.c_str(), s3_account.capacity_url.c_str(),
	    s3_account.id, 0, 0, 0, 0, (size_t)aws_s3_max_acct_capacity);

	cbm_test_create_provider(s3_account.type, CBM_TEST_COMMON_POOL_AWS_S3,
	    "my aws s3 provider",
	    "nothing special", CBM_TEST_COMMON_ACCT_AWS_S3);
}


void
cbm_test_common_add_policy_time(const char *provider_name,
    const char *pol_name, bool final_match, bool compress, bool encrypt,
    bool random_io, bool archive_with_snapshots, uint64_t invalidate_stime,
    uint64_t writeback_stime)
{
	cbm_test_remove_policy(pol_name);
	cbm_test_create_policy(pol_name, "test policy", provider_name,
	    final_match, compress, encrypt, random_io, archive_with_snapshots,
	    FP_A_CP_CACHING_LOCAL, FP_A_CP_CACHING_RA_PARTIAL,
	    invalidate_stime, writeback_stime);
}

void
cbm_test_common_add_policy(const char *provider_name, const char *pol_name,
    bool final_match, bool compress, bool encrypt, bool random_io,
    bool archive_with_snapshots, fp_attr_cp_caching_strategy cache,
    fp_attr_cp_caching_readahead_strategy read_ahead)
{
	cbm_test_remove_policy(pol_name);
	cbm_test_create_policy(pol_name, "test policy", provider_name,
	    final_match, compress, encrypt, random_io, archive_with_snapshots,
	    cache, read_ahead);
}

void
cbm_test_common_delete_policy(const char *pol_name)
{
	cbm_test_remove_policy(pol_name);
}

void
cbm_test_common_delete_all_policy(bool ran_only)
{
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_C);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_E);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_BOTH);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_NO_SNAPSHOT);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_INVALIDATE);
	cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_RAN_FULL_RA);

	if (!ran_only) {
		cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_AZ);
		cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_AZ_C);
		cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_AZ_E);
		cbm_test_common_delete_policy(CBM_TEST_COMMON_POLICY_AZ_BOTH);
	}
}

void cbm_test_leak_check_primer(bool access_cloud)
{
	isi_cloud_object_id a_id(0, 0);
	get_handling_node_id();
	if (access_cloud)
		cbm_test_access_cloud_object();

	cpool_stat_manager::get_new_instance(isi_error_suppress());

	// the following singletons leak in loading the devid
	isi_cbm_get_wbi(isi_error_suppress());
	isi_cbm_get_coi(isi_error_suppress());
	isi_cbm_get_csi(isi_error_suppress());

	cbm_test_cpool_ufp_init();
}

void
cbm_test_common_setup(bool ran_only, bool create_policies)
{
	CHECK_P("\ncbm_test_common_setup\n");
	struct isi_error *error = NULL;

	bool final_match = true;
	bool compress = false;
	bool encrypt = false;
	bool random_io = true;
	bool archive_with_snapshots = true;
	fp_attr_cp_caching_strategy cache = FP_A_CP_CACHING_LOCAL;
	fp_attr_cp_caching_readahead_strategy read_ahead = FP_A_CP_CACHING_RA_PARTIAL;

	// Put gconfig into 'unit test mode' to avoid dirtying primary gconfig
	cbm_test_common_setup_gconfig();

	if (create_policies) {
		cbm_test_common_create_ran_provider_account();

		cache = FP_A_CP_CACHING_LOCAL;
		read_ahead = FP_A_CP_CACHING_RA_PARTIAL;
		compress = false, encrypt = false; archive_with_snapshots = true;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);

		compress = true, encrypt = false; archive_with_snapshots = true;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_C,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);

		compress = false, encrypt = true; archive_with_snapshots = true;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_E,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);

		compress = true, encrypt = true; archive_with_snapshots = true;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_BOTH,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);

		compress = true, encrypt = true; archive_with_snapshots = false;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_NO_SNAPSHOT,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);

		compress = false, encrypt = false; archive_with_snapshots = true;
		cbm_test_common_add_policy_time(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_INVALIDATE,
		    final_match, compress, encrypt, random_io, archive_with_snapshots,
		    FIVE_MINUTES, TWO_MINUTES);

		cache = FP_A_CP_CACHING_LOCAL;
		read_ahead = FP_A_CP_CACHING_RA_FULL;
		compress = false, encrypt = false; archive_with_snapshots = true;
		cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_RAN,
		    CBM_TEST_COMMON_POLICY_RAN_FULL_RA,
		    final_match, compress, encrypt, random_io,
		    archive_with_snapshots, cache, read_ahead);


		// if (!ran_only) {
		// 	cbm_test_common_create_az_provider_account();

		// 	cache = FP_A_CP_CACHING_LOCAL;
		// 	read_ahead = FP_A_CP_CACHING_RA_PARTIAL;
		// 	compress = false, encrypt = false; archive_with_snapshots = true;
		// 	cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_AZ,
		// 	    CBM_TEST_COMMON_POLICY_AZ,
		// 	    final_match, compress, encrypt, random_io,
		// 	    archive_with_snapshots, cache, read_ahead);

		// 	compress = true, encrypt = false; archive_with_snapshots = true;
		// 	cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_AZ,
		// 	    CBM_TEST_COMMON_POLICY_AZ_C,
		// 	    final_match, compress, encrypt, random_io,
		// 	    archive_with_snapshots, cache, read_ahead);

		// 	compress = false, encrypt = true; archive_with_snapshots = true;
		// 	cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_AZ,
		// 	    CBM_TEST_COMMON_POLICY_AZ_E,
		// 	    final_match, compress, encrypt, random_io,
		// 	    archive_with_snapshots, cache, read_ahead);

		// 	compress = true, encrypt = true; archive_with_snapshots = true;
		// 	cbm_test_common_add_policy(CBM_TEST_COMMON_POOL_AZ,
		// 	    CBM_TEST_COMMON_POLICY_AZ_BOTH,
		// 	    final_match, compress, encrypt, random_io,
		// 	    archive_with_snapshots, cache, read_ahead);
		// }
	}
	isi_cbm_reload_config(&error);

	fail_if(error, "failed to load config %#{}", isi_error_fmt(error));

	cbm_test_leak_check_primer(true);

	cpool_stats_unit_test_helper::on_suite_setup();
}

void
cbm_test_common_setup_gconfig()
{
	struct isi_error *error = NULL;

	// Setup test environment gconfig
	cpool_unit_test_setup(&error);
	fail_if(error, "Error calling cpool_unit_test_setup: %#{}",
	    isi_error_fmt(error));

	cbm_test_ensure_permit_for_local_cluster();
}

void
cbm_test_common_cleanup_gconfig()
{
	struct isi_error *error = NULL;

	// Cleanup any mess remaining in gconfig
	cpool_unit_test_cleanup(&error);
	fail_if(error, "Error calling cpool_unit_test_cleanup: %#{}",
	    isi_error_fmt(error));
}

void
cbm_test_common_cleanup(bool ran_only, bool clean_policies)
{
	CHECK_P("\ncbm_test_common_cleanup\n");

	if (clean_policies) {
		cbm_test_common_delete_all_policy(ran_only);

		cbm_test_common_delete_ran_provider_account();

		if (!ran_only) {
			cbm_test_common_delete_az_provider_account();
		}
	}
	// ensure good for next test since each test may have destroyed it
	cl_provider_module::init();
	isi_cbm_unload_conifg();

	// Cleanup any mess remaining in gconfig
	cbm_test_common_cleanup_gconfig();
}

void
cbm_test_access_cloud_object()
{
	std::string test_url;

	cl_provider &ran = cl_provider_creator::get_provider(CPT_RAN);
	callback_params cb_params;

	if (test_is_hardened()) {
		if (test_make_url(test_url, "https", 8080, "/namespace/ifs") == 0) {
			ran_account.url = test_url;
		}
		else {
			fail("Unable to fetch external IP address");
		}
	}

	ran.put_container(ran_account, "cpool_test_folder", cb_params);

	std::map<std::string, std::string> map;

	map["USER_ATTRIBUTE"] = "darkest blue";
	fail_if(map.empty(), "Map should not empty");

	cl_test_istream strm(1024*1024, 'c');
	std::string md5; // empty for test
 	bool overwrite = true;
	ran.put_object(ran_account, "/cpool_test_folder", "test_object1.dat",
	    map, strm.get_size(), overwrite, false, md5, cb_params, &strm, 0);

	std::map<std::string, std::string> map_out;
	cl_properties prop_out; // dummy need for test
	cl_test_ostream strm_out;

	ran.get_object(ran_account, "/cpool_test_folder", "test_object1.dat", 0,
	    1024*1024, -1, map_out, prop_out, cb_params, strm_out);

	// Temporarily disable the logging of failures by cloud_api
	override_clapi_verbose_logging_enabled(false);
	try {
		isi_cloud::cl_account bad_account = {
			"0.0.0.0",
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
		ran.get_object(bad_account, "/cpool_t_fld", "t_obj1.dat", 0,
		    1024*1024, -1, map_out, prop_out, cb_params, strm_out);
	} catch (isi_cloud::cl_exception &cl_err) {
	}
	revert_clapi_verbose_logging();

	ran.delete_object(ran_account, "/cpool_test_folder",
	    "test_object1.dat", -1, cb_params);
	ran.delete_object(ran_account, "/cpool_test_folder", "",
	    -1, cb_params);
}

void
cbm_test_enable_stub_access()
{
	struct isi_error *error = NULL;
	isi_cbm_enable_stub_access(&error);
	fail_if(error, "Could not enable stub access: %#{}",
	    isi_error_fmt(error));
}

void
cbm_test_disable_stub_access()
{
	struct isi_error *error = NULL;
	isi_cbm_disable_stub_access(&error);
	fail_if(error, "Could not enable stub access: %#{}",
	    isi_error_fmt(error));
}

void
cbm_test_create_sparse_file(const char *fname,
    size_t filesize, size_t num_segments, struct segment *segments,
    struct stat *stat_buf)
{
	int	out_fd		= -1;
	int	retc		= 0;
	ssize_t	wr		= 0;
	size_t	segment_count	= 0;
	char	*buf		= NULL;
	off_t	offset		= 0;
	off_t	indx		= 0;
	off_t	found_offset	= 0;
	ssize_t	total_written	= 0;

	// Allocate memory in the heap.
	buf = (char *) malloc(IFS_BSIZE);

	// Open the file.
	out_fd = open(fname, (O_RDWR | O_CREAT | O_TRUNC), 0644);
	fail_if (out_fd < 0, "Could not open file %s:(%d) %s",
	    fname, errno, strerror(errno));

	//CHECK_TRACE("\n\nTruncating %s up to filesize %llx\n", fname, filesize);
	fail_if(ftruncate(out_fd, filesize),
	    "Failed to set filesize to %ld for %s:(%d) %s",
	    fname, filesize, errno, strerror(errno));

	// Write to the desired locations, leave the rest sparse.
	while (segment_count < num_segments) {
		offset = segments[segment_count].byte_offset;
		//CHECK_TRACE("  Seeking to offset %llx\n", offset);
		found_offset = lseek(out_fd, offset, SEEK_SET);
		fail_if(found_offset < 0,
		    "Could not seek to %ld for file %s:(%d) %s",
		    offset, fname, errno, strerror(errno));

		total_written = 0;
		while (total_written < segments[segment_count].byte_len) {

			ssize_t writelen = MIN(IFS_BSIZE,
			    segments[segment_count].byte_len - total_written);

			for (indx = 0; indx < IFS_BSIZE; indx++) {
				buf[indx] = (offset + indx) % 256;
			}

			//CHECK_TRACE("    Writing %lld bytes at offset %llx\n", writelen, offset);
			fail_if((wr = write(out_fd, buf, writelen)) < 0);
			fail_if(wr != writelen,
			    "Bytes written : %d "
			    "is not the same as requested %d",
			    wr, writelen);
			total_written += wr;
			offset += wr;
		}
		segment_count++;
	}

	if (stat_buf) {
		retc = fstat(out_fd, stat_buf);
		fail_if(retc < 0, "failed to stat file: %s", strerror(errno));
	}

	// Close the file.
	fsync(out_fd);
	close(out_fd);

	// Free the allocated memory.
	free(buf);
	return;
}

bool
compare_maps_to_spf(struct sparse_file_info *spf,
    const char *filename, bool expect_spamap_match)
{
	int	mapi = 0;
	int	bytei = 0;
	int	status = 0;
	bool	match = false;
	int	map_size = spf->map_size;
	off_t	curr_offset = 0;

	struct stat	statbuf;
	struct isi_error *error = NULL;
	unsigned char	spa_map[64];

	status = stat(filename, &statbuf);
	if (status != 0) {
		fail_if((status != 0), "Could not stat %s for %s: %s (%d)",
		    filename, spf->spf_name, strerror(errno), errno);
		goto out;
	}

	while (curr_offset < statbuf.st_size) {
		off_t gen_len =
		    MIN(spf->chunk_size, (statbuf.st_size - curr_offset));

		memset(spa_map, 0x42, 64);
		isi_cbm_build_sparse_area_map(statbuf.st_ino,
		    statbuf.st_snapid, curr_offset, gen_len,
		    spf->resolution, spf->chunk_size,
		    spa_map, map_size, &error);
		if (error != 0) {
			fail_if((error != 0), "Failed to create sparse area map "
			    "for %s filesize %lld, offset %lld, length %lld: %{}",
			    filename, statbuf.st_size,
			    curr_offset, gen_len, isi_error_fmt(error));
			goto out;
		}

		status = memcmp(
		    spa_map, &(spf->expected_map[mapi][0]), map_size);
		if (expect_spamap_match) {
			match = (status == 0);
		} else {
			match = (status != 0);
		}
		if (!match) {
			CHECK_TRACE("\n\nBuild sparse map for %s: offset %lld, "
			    "length %lld, resolution %lld, region size %lld, "
			    "chunk size %lld, map_size %d\n",
			    filename, curr_offset, gen_len,
			    spf->resolution, spf->region_size,
			    spf->chunk_size, map_size);
			CHECK_TRACE(
			    "maps for chunk %d %s:\n"
			    "    expected: ",
			    mapi, (expect_spamap_match) ?
			    "do not match" : "match, but should not");
			for (bytei = 0; bytei < map_size; bytei++) {
				CHECK_TRACE("%02x ",
				    spf->expected_map[mapi][bytei]);
				if ((bytei % 32) == 31) {
					CHECK_TRACE("\n              ");
				}
			}
			CHECK_TRACE("\n");
			CHECK_TRACE(
			    "      actual: ");
			for (bytei = 0; bytei < map_size; bytei++) {
				CHECK_TRACE("%02x ", spa_map[bytei]);
				if ((bytei % 32) == 31) {
					CHECK_TRACE("\n              ");
				}
			}
			CHECK_TRACE("\n");
		}
		mapi++;
		curr_offset += gen_len;
	}
out:
	return match;
}

bool __clapi_logging_was_enabled = false;
bool __clapi_logging_override_on = false;

void override_clapi_verbose_logging_enabled(bool allow_logging)
{
	ASSERT(!__clapi_logging_override_on, "Cannot override clapi logging "
	    "more than once.  Call REVERT_CLAPI_VERBOSE_LOGGING");
	__clapi_logging_was_enabled = cl_provider_wrap::get_retry_debug();
	cl_provider_wrap::set_retry_debug(allow_logging);
	__clapi_logging_override_on = true;
}

void revert_clapi_verbose_logging()
{
	ASSERT(__clapi_logging_override_on, "Cannot revert clapi logging "
	    "without first overriding.  Call "
	    "override_clapi_verbose_logging_enabled");
	cl_provider_wrap::set_retry_debug(__clapi_logging_was_enabled);
	__clapi_logging_override_on = false;
}

// Include provider retry enable functionality
#include <isi_cloud_api/cl_test_util.cpp>
