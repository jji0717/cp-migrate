#ifndef __ISI_CBM_WRITE__H__
#define __ISI_CBM_WRITE__H__

#include <sys/stat.h>
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"

using namespace isi_cloud;

struct isi_cmo_info {
	uint32_t version;
	ifs_lin_t lin;
	const struct stat *stats;
	isi_cfm_mapinfo *mapinfo;
};

///isi_cbm_archive_common中使用
struct isi_cpool_blobinfo {
	size_t chunksize;
	size_t readsize;
	ifs_lin_t lin;
	isi_cbm_id account_id;
	cl_object_name cloud_name;
	uint64_t index; // chunk index, 0 == metadata...
	std::map<std::string, std::string> attr_map;
	uint32_t sparse_resolution;
};
//////isi_cbm_archive_common中使用
struct isi_cbm_chunkinfo {
	int fd;
	uint64_t index;
	off_t file_offset;
	off_t object_offset;
	size_t len;
	bool compress;
	bool checksum;
	encrypt_ctx_p ectx;
};


/*
 * using the policy name provided, determine the information needed to 
 * represent the data in the a stub map.
 */
void isi_cpool_api_get_info(isi_cbm_ioh_base *ioh, const isi_cbm_id &,
    ifs_lin_t, bool, struct isi_cpool_blobinfo &, struct isi_error **);

void isi_cph_write_md_blob(int, isi_cbm_ioh_base *ioh, 
    struct isi_cpool_blobinfo &, struct isi_cmo_info &, std::string &, 
    struct isi_error **);

ssize_t isi_cph_write_data_blob(int, off_t offset, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &, struct isi_cbm_chunkinfo &,
    struct isi_error **);

/**
 * start writing the master CDO
 * param ioh[in] the io helper
 * param master_cdo[in] the master cdo to write
 * param error_out[out] errors if any
 */
void isi_cph_start_write_master_cdo(ifs_lin_t lin, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &master_cdo, struct isi_error **error_out);

/**
 * mark the end of writing the master CDO
 * param ioh[in] the io helper
 * param master_cdo[in] the master cdo written
 * param commit[in] whether to commit
 * param error_out[out] errors if any
 */
void isi_cph_end_write_master_cdo(ifs_lin_t lin, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &master_cdo, bool commit,
    struct isi_error **error_out);

/**
 * Given a policy ID, retrieve the provider ID.
 * @param policy_id[in] id of policy
 * @param ppi[in] policy provider config context
 * @param ppolicy_id[out] the policy id returned
 * @param pprovider_id[out] the provider id returned
 * @param compress[out] whether compression is enabled
 * @param checksum[out] whether to use checksum
 * @param encrypt[out] whether encryption is enabled
 * @param archive_snaps[out] whether to archive files with snapshot
 * @param error_out[out] isi_error object if failed
 */
void isi_cfm_get_policy_info(uint32_t pol_id,
    struct isi_cfm_policy_provider *ppi,
    isi_cbm_id *ppolicy_id, isi_cbm_id *pprovider_id,
    bool *compress, bool *checksum, bool *encrypt,
    bool *archive_snaps, struct isi_error **error_out);

/**
 *  Common wrapper to add COI entry after archive/sync
 */
void isi_cph_add_coi_entry(isi_cfm_mapinfo &mapinfo, ifs_lin_t lin,
    bool ignore_exist, struct isi_error **error_out);

#endif // __ISI_CBM_WRITE__H__
