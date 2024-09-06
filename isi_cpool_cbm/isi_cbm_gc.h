#ifndef __ISI_CBM_GC_H__
#define __ISI_CBM_GC_H__

#include <ifs/ifs_types.h>

#include <isi_cpool_d_common/cpool_d_common.h>

#include "isi_cbm_account_util.h"
#include "isi_cpool_cbm.h"

class isi_cloud_object_id;
class isi_cloud_object_id_raw;
class isi_cfm_mapinfo;

/////kdq threadpool负责
void isi_cbm_local_gc(ifs_lin_t lin, ifs_snapid_t snapid,
    isi_cloud_object_id object_id, const struct timeval &deleted_stub_dod,
    struct isi_cfm_policy_provider *ppi, void *tch,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out);

void isi_cbm_cloud_gc_ex(const isi_cloud_object_id &object_id,
    struct isi_cfm_policy_provider *ppi, void *tch,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out);
/////ooi scanning threadpool负责
void isi_cbm_cloud_gc(const isi_cloud_object_id &object_id,
    struct isi_cfm_policy_provider *ppi, void *tch,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out);

void isi_cbm_remove_cloud_objects(const isi_cfm_mapinfo &stub_map,
    const isi_cfm_mapinfo *stub_map_prev,
    const isi_cfm_mapinfo *stub_map_next,
    struct cl_object_name &cloud_object_name,
    isi_cbm_ioh_base *cmo_io_helper, bool sync_rollback,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    void *opaque,
    struct isi_error **error_out);

void update_io_helper(const isi_cfm_mapentry &entry,
    isi_cbm_ioh_base_sptr &current_ioh, struct isi_error **error_out);

/**
 * dump the version chain of the cloud object
 * @param object_id[IN}   object id
 */
void isi_cbm_dump_version_chain(const isi_cloud_object_id &object_id);

/**
 * dump the CDOs of the version
 * @param object_id[in]  object id
 */
void isi_cbm_dump_cdo(const isi_cloud_object_id &object_id);


#endif // __ISI_CBM_GC_H__
