/**
 *  @file iobj_snapshot.h
 *
 * A module for snapshot access in the object library.
 *
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 */
#ifndef __IOBJ_SNAPSHOT_H__
#define __IOBJ_SNAPSHOT_H__

#include <stdio.h>
#include <stdbool.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_error.h>
#include <isi_util/isi_str.h>
#include <isi_util/isi_guid.h>
#include <isi_licensing/licensing.h>
#include <isi_snapshot/snapshot.h>
#include <isi_snapshot/snapshot_utils.h>
#include <isi_snapshot/snapshot_error.h>

#define GUID_SIZE       37

#ifdef __cplusplus
extern "C" {
#endif

/**
 * This routine initializes the isi_str struct snapshot name .
 * @param snapshot_name[in]     the snapshot from which clone is made.
 * @param s_name[out]		the isi_str struct for snashot name
 * @param store_name[in]	the store name of source
 * @param src_path[in]          source path in ifs
 * @param err_out[out]          error
 * @return                      snapshot id if new snapshot created, else 
                                returns INVALID_SNAPID
 */
ifs_snapid_t iobj_init_clone_snap(char *snapshot_name, struct isi_str *s_name,
     char  *snap_src_path, const char *store_name, const char *src_path,
     struct isi_error **err_out);

/**
 * Routine to free isi_str struct for snapshot name .
 * @param s_name[in]		the isi_str struct for snashot name
 * @param snap_id[in]	        snapshot id
 * @return                      returns isi_error ptr.
 */

struct isi_error *iobj_free_snapshot(struct isi_str *s_name,
    ifs_snapid_t *snap_id);

#ifdef __cplusplus
}
#endif

#endif /* __IOBJ_SNAPSHOT_H__ */
