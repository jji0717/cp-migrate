#pragma once
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>

#include "isi_cbm_file.h"

/**
 * Function to check if the CBM file with mapinfo is referenced
 * by other local snapshots with the same LIN.
 */
bool cbm_snap_used_by_other_snaps(struct isi_cbm_file *file,
    const isi_cfm_mapinfo *mapinfo,
    struct isi_error **error_out);

bool cbm_snap_used_by_other_snaps(ifs_lin_t lin, ifs_snapid_t snapid,
    const isi_cbm_id &account, const isi_cloud_object_id &object_id,
    struct isi_error **error_out);

/**
 * Function to check if the LIN has other snapshots
 * by other local snapshots with the same LIN.
 */
bool cbm_snap_has_snaps(ifs_lin_t lin, int fd,
    struct isi_error **error_out);
