#ifndef __ISI_CBM_SPARSE__H__
#define __ISI_CBM_SPARSE__H__

#include <isi_cpool_cbm/io_helper/sparse_area_header.h>

#define BITS_PER_CHAR	8

/**
 * Gather the sparse region information for a specified lin/snap combination
 * @param lin[IN]: the lin for the file to be scanned
 * @param snapid[IN]: the snapid for the instance to be scanned
 * @param offset[IN]
 * @param length[IN]
 * @param resolution[IN]
 * @param region_size[IN]
 * @param spa_map[OUT]
 * @param mapsize[IN]
 * @param error_out[OUT]
 */
void isi_cbm_build_sparse_area_map(ifs_lin_t lin, ifs_snapid_t snapid,
    off_t offset, size_t length, size_t resolution, size_t region_size,
    const void *spa_map, int mapsize, struct isi_error **error_out);

/**
 * Gather the sparse region information for a specified lin/snap combination
 * @param lin[IN]: the lin for the file to be scanned
 * @param snapid[IN]: the snapid for the instance to be scanned
 * @param offset[IN]: offset in the file to start the map determination
 * @param length[IN]: length to generate map for (<= region_size)
 * @param region_size[IN] maximum area that the map will represent (chunksize)
 * @param spa[IN] sparse area header to update the map in
 * @param error_out[OUT]
 */
void isi_cbm_build_sparse_area_map(ifs_lin_t lin, ifs_snapid_t snapid,
    off_t offset, size_t length, size_t region_size,
    sparse_area_header_t *spa, struct isi_error **error_out);

/**
 * Given a data buffer, a sparse map and the resolution for each bit in the
 * map, create a corresponding iovec to represent the data that is present in
 * the file.
 */
void isi_cbm_sparse_map_to_iovec(isi_cbm_file const *file, void *sparse_map,
    int map_size, off_t offset,
    size_t length, void *data, size_t resolution, size_t chunk_size,
    struct iovec *vec, int *vec_count, size_t *bytes_to_write,
    struct isi_error **error_out);

void isi_cbm_sparse_map_to_iovec(isi_cbm_file const *file,
    sparse_area_header_t &spa, off_t offset,
    size_t length, void *data, size_t chunk_size, struct iovec *vec,
    int *vec_count, size_t *bytes_to_write, struct isi_error **error_out);

void isi_cbm_get_sparse_map(struct isi_cbm_file *file_obj, off_t offset,
    sparse_area_header_t &spa, struct isi_error **error_out);

#endif /* __ISI_CBM_SPARSE__H__*/
