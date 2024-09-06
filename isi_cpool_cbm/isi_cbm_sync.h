#pragma once
#include <ifs/ifs_types.h>
#include "isi_cpool_cbm.h"
/**
 * Sync trigger before writing the mapping info to the cloud
 */
typedef void(*pre_persist_map_callback)(int value);

/**
 * private interfaces for sync
 */

void cbm_sync_register_pre_persist_map_callback(
    pre_persist_map_callback callback);

struct isi_cbm_sync_option {
	// indicate if to do blocking mode lock when it needs to
	// acquire resources which support both blocking and non-blocking
	// mode.
	bool blocking;
	// indicate if settle time check should be skipped
	bool skip_settle;
};

/**
 * Do sync with option.
 */
void isi_cbm_sync_opt(uint64_t lin, uint64_t snapid,
    get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb,
    void *ctx, isi_cbm_sync_option &opt, struct isi_error **error_out);
