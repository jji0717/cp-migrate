#pragma once
#include <ifs/ifs_types.h>

#include "isi_cpool_cbm.h"

__BEGIN_DECLS

/**
 * Restore the COI information for the account specified by (cluster_id,
 * acct_id).
 * @param cluster_id[IN] the cluster id of the account
 * @param acct_id[IN] the id of the account
 * @param time_t[IN} the date of death of the cloud object
 * @param ckpt_ctx[IN] the checkpointing context
 * @param ckpt_get[IN] interface to get the checkpointing data
 * @param ckpt_set[IN] interface to set the checkpointing data
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_restore_coi_for_acct(const char *cluster_id, uint32_t acct_id,
    struct timeval dod, void *ckpt_ctx, get_ckpt_data_func_t get_ckpt_cb,
    set_ckpt_data_func_t set_ckpt_cb, struct isi_error **error_out);

__END_DECLS
