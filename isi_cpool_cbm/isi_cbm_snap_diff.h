#ifndef _ISI_CBM_SNAP_DIFF_H_
#define _ISI_CBM_SNAP_DIFF_H_

/**
 * Snap diff support extended to handle stub files
 */

__BEGIN_DECLS

/**
 * The opaque file snap diff iterator; can be used for regular or CBM files
 */
struct isi_cbm_snap_diff_iterator; 

/**
 * Create the file iterator
 * 
 * @param lin[IN]: lin of the file to be diffed
 * @param start[IN]: the offset at which the file needs to be diffed
 * @param snap1[IN]: the first [older] snap
 * @param snap1[IN]: the second [newer] snap
 * @param local-only[IN]:  whether to diff only the OneFS snap content
 *                         and not the cloud content if applicable
 * 
 * @return an allocated, initialized snap-diff iterator on success; must
 *         be destroyed after all use
 */
struct isi_cbm_snap_diff_iterator * isi_cbm_snap_diff_create(ifs_lin_t lin,
    off_t start, ifs_snapid_t snap1, ifs_snapid_t snap2, bool local_only);

/**
 * Destroy the file iterator
 * 
 * @param iter[IN]: the iterator to be destroyed
 * 
 */
void isi_cbm_snap_diff_destroy(struct isi_cbm_snap_diff_iterator *iter);

/**
 * Obtain next differing region for the file snaps
 *
 * @param iter[IN]: the iterator bound to the file
 * @param out_region[OUT]: the next region for a file; out_region.byte_count
 *                         is set to 0 when there are no remaining regions 
 *
 * @return 0 on success; -1 on failure with errno set
 */
int isi_cbm_snap_diff_next(struct isi_cbm_snap_diff_iterator *iter,
    struct snap_diff_region *out_region);

__END_DECLS

#endif // _ISI_CBM_SNAP_DIFF_H_
