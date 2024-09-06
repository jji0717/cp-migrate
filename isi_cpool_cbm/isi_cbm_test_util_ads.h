#ifndef _ISI_CBM_TEST_UTIL_ADS_H_
#define _ISI_CBM_TEST_UTIL_ADS_H_

#include <errno.h>
#include <list>

#include <ifs/ifs_types.h>
#include <isi_cloud_api/cl_provider_common.h>
#include <isi_cloud_api/test_common_acct.h>
#include <isi_cpool_config/cpool_config.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_file_filter/file_filter.h>
#include <isi_pools_fsa_gcfg/isi_pools_fsa_gcfg.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_invalidate.h>

#include <isi_util/isi_error.h>

/**
 * routine to pre-create the cacheinfo to avoid ctime change during testing
 */
struct isi_error *cbm_test_precreate_cacheinfo(uint64_t lin);

#endif
