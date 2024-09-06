#ifndef __ISI_CBM_INVALIDATE__H__
#define __ISI_CBM_INVALIDATE__H__

#include <isi_cpool_cbm/isi_cbm_cache.h>

/**
 * Invalidate the local cache for a file
 * @param fd[IN]: the file descriptor for the file do the cache flush
 * @param lin[IN]: the lin for the file (for error reporting)
 * @param dirty_data[OUT]: pointer to boolean which will be set to true if
 *        dirty data is found when trying to invalidate the cache.
 */
void isi_cbm_invalidate_cache(ifs_lin_t lin, ifs_snapid_t snap_id,
    bool *dirty_data, struct isi_error **error_out);

void isi_cbm_invalidate_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    void *opaque, struct isi_error **error_out);


void isi_cbm_invalidate_cached_file_opt(ifs_lin_t lin, ifs_snapid_t snapid,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    void *opaque, bool override_settle, bool daemon, bool *dirty_data,
    struct isi_error **error_out);

/**
 * Perform the actual invalidate operation.
 * @param file_obj[IN]: CBM open file object describing the file
 * @param lin[IN]: the lin for the file object (for error reporting)
 * @param cacheinfo[IN]: cache object for file
 * @param update_cache_info[IN]: true if cacheinfo needs to be processed.
 *        Used for the case when invaldation is done for restart of stubbing
 *        file and no cache has been created.
 */
void isi_cbm_invalidate_cache_restart(struct isi_cbm_file *file_obj,
    ifs_lin_t lin, cpool_cache *cacheinfo, bool update_cache_header,
    struct isi_error **error_out);

#endif /* __ISI_CBM_INVALIDATE__H__*/
