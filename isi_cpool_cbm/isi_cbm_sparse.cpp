#include <unistd.h>

#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include "sys/fcntl.h"
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/isi_enc.h>
#include <ifs/ifs_types.h>
#include "ifs/ifs_syscalls.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_sparse.h"
#include "isi_cpool_cbm/io_helper/sparse_area_header.h"

#ifdef OVERRIDE_ILOG_FOR_DEBUG
#if defined(ilog)
#undef ilog
#endif
#define ilog(a, args...) \
	{\
	struct fmt FMT_INIT_CLEAN(thisfmt);\
	fmt_print(&thisfmt, args);\
	fprintf(stderr, "%s\n", fmt_string(&thisfmt));	\
	fflush(stderr); \
	}
#endif

const char INVALID[]	= "Invalid";
const char SPARSE[]	= "Sparse";
const char DATA[]	= "Data";
const char UNCHANGED[]	= "Ditto";

const char * region_type_to_text(int region_type)
{
	switch (region_type) {
	case RT_SPARSE:
		return SPARSE;
	case RT_DATA:
		return DATA;
	case RT_UNCHANGED:
		return UNCHANGED;
	default:
		return INVALID;
	}
}

static bool
set_bits(const void *sparse_map, int map_size, int first_bit, int last_bit)
{
	ASSERT(first_bit >= 0);
	ASSERT(sparse_map);

	unsigned char *map = (unsigned char *)sparse_map;
	int this_byte = last_bit / BITS_PER_CHAR;
	int this_bit;
	int i;

	ilog(IL_TRACE, "Setting sparse map of size %d from %d to %d",
	    map_size, first_bit, last_bit);

	if (this_byte > map_size)
		return false;

	for (i = first_bit,
		 this_byte = first_bit / BITS_PER_CHAR;
	     i <= last_bit; this_byte++) {

		for (this_bit = i % BITS_PER_CHAR;
		     i <= last_bit && this_bit < BITS_PER_CHAR;
		     i++, this_bit++) {
			ilog(IL_TRACE, "Setting bit %d in byte %d\n",
			    this_bit, this_byte);
		map[this_byte] |= 1 << this_bit;
		}
	}
	return true;
}

void
isi_cbm_build_sparse_area_map(ifs_lin_t lin, ifs_snapid_t snapid,
    off_t offset, size_t length, size_t resolution, size_t chunk_size,
    const void *sparse_map, int map_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct snap_diff_iterator iter;
	struct snap_diff_region reg;
	off_t sparse_offset;
	size_t sparse_end = 0;
        size_t sparse_bytes = 0;
        size_t bytes_processed = 0;
	int count = 0;
	int first_bit;
	int last_bit;

	/* Be sure that the resolution is a multiple of the FS block size */
	//ASSERT(!(resolution % IFS_BSIZE));

	/*
	 * Be sure we start with a clean map
	 */
	memset((void *)sparse_map, 0, map_size);

	UFAIL_POINT_CODE(cbm_set_all_sparse, {
	    memset((void *)sparse_map, 0xff, map_size);
	    goto out;
	    });

	ilog(IL_TRACE,
	    "Finding sparse info for %{}:%{}, offset %ld, length %ld, "
	    "resolution: %lld, chunk_size %ld, map_size %d",
	    lin_fmt(lin), snapid_fmt(snapid), offset, length,
	    resolution, chunk_size, map_size);

	/*
	 * Initialize snap_diff with the null snapshot
	 */
	snap_diff_init(lin, offset, 0, snapid, &iter);

	/*
	 * Get the first data region
	 */
	do {
		if (ifs_snap_diff_next(&iter, &reg) != 0) {
			error = isi_system_error_new(errno,
				"ifs_snap_diff_next failed");
			ON_ISI_ERROR_GOTO(out, error);
		}
		if (reg.byte_count != 0 || count == 0) {

			ilog(IL_TRACE, "lin: %{} snapid: %{}, "
			    "region %4d: type: %6s, "
			    "offset %10ld, count %10ld\n",
			    lin_fmt(lin), snapid_fmt(snapid),
			    count, region_type_to_text(reg.region_type),
			    reg.start_offset,
			    reg.byte_count);

			count++;

			bytes_processed += reg.byte_count;
			if (reg.region_type != RT_SPARSE) {
				sparse_bytes = 0;
				if (bytes_processed > chunk_size)
					goto out;
			} else {
				sparse_bytes = reg.byte_count;
				sparse_offset = reg.start_offset % chunk_size;
				sparse_end = MIN((sparse_offset + sparse_bytes),
				    chunk_size) - 1;
				first_bit = sparse_offset / resolution;
				last_bit = sparse_end / resolution;
				ilog(IL_TRACE,
				    "for %{}:%{} sparse found file offset %ld, "
				    "file length %ld, sparse_offset %ld, "
				    "sparse size %ld, sparse end %ld, "
				    "first %d last %d",
				    lin_fmt(lin), snapid_fmt(snapid),
				    reg.start_offset, reg.byte_count,
				    sparse_offset, sparse_bytes, sparse_end,
				    first_bit, last_bit);

				set_bits(sparse_map, map_size,
				    first_bit, last_bit);
			}
		}
	} while ((reg.byte_count != 0) &&
	    ((reg.start_offset + reg.byte_count) < (offset + length)));

 out:

	isi_error_handle(error, error_out);
}

void
isi_cbm_build_sparse_area_map(struct isi_cbm_file *file, off_t offset,
    size_t length, size_t resolution, size_t chunk_size, void *sparse_map,
    int map_size, struct isi_error **error_out)
{

	isi_cbm_build_sparse_area_map(file->lin, file->snap_id,
	    offset, length, resolution, chunk_size, sparse_map, map_size,
	    error_out);
}

void
isi_cbm_build_sparse_area_map(ifs_lin_t lin, ifs_snapid_t snapid,
    off_t offset, size_t length, size_t chunk_size,
    sparse_area_header_t *spa,
    struct isi_error **error_out)
{

	spa->trace_spa("before map is built", __func__, __LINE__);

	isi_cbm_build_sparse_area_map(lin, snapid, offset, length,
	    spa->get_sparse_resolution(), chunk_size,
	    spa->get_sparse_map_ptr(), SPARSE_MAP_SIZE,
	    error_out);

	/*
	 * XXXegc: TBD set this to the actual bytes used rather than the whole
	 * map.
	 */
	spa->set_sparse_map_length(SPARSE_MAP_SIZE);

	spa->trace_spa("after map is built", __func__, __LINE__);

}

inline static void
handle_data_region(bool *data_found, struct iovec *vec, int *vec_index,
    size_t resolution, size_t *bytes_vectored, unsigned char *buf,
    size_t *data_size, int vec_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	if (*data_found) {
		/* last area was also a data region */
		vec[*vec_index].iov_len += resolution;
	} else {
		/*
		 * new data region, check for room in array and then
		 * start new vector.
		 */
		if (++*vec_index > vec_size) {
			/*
			 * should we just force the last vector to cover the
			 * rest of the space in the chunk?  As it stands this
			 * case should never happen, worst case scenerio is
			 * passed in at this point.
			 */
			error = isi_cbm_error_new(CBM_SPARSE_VEC_TOO_BIG,
			    "iov generation exceeds size of allocated iov %d",
			    vec_size);
			goto out;
		}
		vec[*vec_index].iov_len = resolution;
		vec[*vec_index].iov_base = buf + *data_size;
		*data_found = true;
	}

	/* increase actual byte count */
	*bytes_vectored += resolution;
	*data_size += resolution;
 out:
	isi_error_handle(error, error_out);
}

static inline void
log_iovec(const char *msg, struct iovec *vec, int vec_index)
{
	if (vec_index >= 0)
		ilog(IL_TRACE, "%s: iovec[%d] (base, len) (%p, %ld 0x%0lx)",
		    msg, vec_index, vec[vec_index].iov_base,
		    vec[vec_index].iov_len, vec[vec_index].iov_len);
	else
		ilog(IL_TRACE, "%s: vec_index=%d", msg, vec_index);
}

static void
handle_sparse_region(isi_cbm_file const *file, bool *data_found,
    size_t *data_size, struct iovec *vec, int *vec_index, int vec_size,
    size_t resolution, int byte, bool entire_byte, size_t *bytes_vectored,
    unsigned char *buf, struct isi_error **error_out)
{
	// goes to .bss, does not take space in executable
	static char const zeroes[64 * 1024] = { 0 };

	struct isi_error *error = NULL;
	size_t const region_size = resolution * (entire_byte ? BITS_PER_CHAR : 1);
	ifs_lin_t lin = file ? file->lin : 0;
	ifs_snapid_t snapid = file ? file->snap_id : 0;

	ASSERT(region_size <= sizeof(zeroes));

	log_iovec(entire_byte ? "all Sparse" : "bit by bit", vec, *vec_index);
	ilog(IL_TRACE, "Byte: %d", byte);

	if (memcmp(buf + *data_size, zeroes, region_size)) {
		ilog(IL_ERR, "Sparse map does not match CDO data, ignoring"
		    " sparse map: %{} data_size=%llu resolution=%llu "
		    "bytes_vectored=%llu", lin_snapid_fmt(lin, snapid),
		    *data_size, resolution, *bytes_vectored);

		handle_data_region(data_found, vec, vec_index, region_size,
		    bytes_vectored, buf, data_size, vec_size, &error);
		ON_ISI_ERROR_GOTO(out, error);

		goto out;
	}

	*data_found = false;
	*data_size += region_size;

out:
	isi_error_handle(error, error_out);
}

/*
 * for each bit or consecutive bits set in the sparse map, a hole needs to be
 * left in the iovec to allow for sparse areas to be created in the file.
 */
void
isi_cbm_sparse_map_to_iovec(isi_cbm_file const *file, void *sparse_map,
    int map_size,
    off_t offset, size_t length, void *data, size_t resolution,
    size_t chunk_size, struct iovec *vec, int *vec_count,
    size_t *bytes_to_write, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int vec_index = -1;
	int vec_size = *vec_count;
	size_t data_size = 0;
	size_t bytes_vectored = 0;
	unsigned char *map = (unsigned char *)sparse_map;
	unsigned char *buf = (unsigned char *)data;
	bool data_found = false;
	int i, j;

	int byte_start = (offset / resolution) / BITS_PER_CHAR;
	int bit_start  = (offset / resolution) % BITS_PER_CHAR;

	ilog(IL_TRACE,
	    "Map offset specified as %ld, length %ld, data buffer %p, "
	    "chunk_size %ld, resolution %ld, starting byte/bit are: %d/%d",
	    offset, length, data, chunk_size, resolution,
	    byte_start, bit_start);

	for (i = byte_start;
	     i < map_size && data_size < chunk_size && data_size < length;
	     i++) {
		if (bit_start == 0 && map[i] == 0x00) {
			/*
			 * Entire byte represents non-sparse area, i.e.,
			 * 8 consecutive "resolution" size portions of the file.
			 */
			handle_data_region(&data_found, vec, &vec_index,
			    (BITS_PER_CHAR * resolution),
			    &bytes_vectored, buf,
			    &data_size, vec_size, &error);
			ON_ISI_ERROR_GOTO(out, error);
			ilog(IL_TRACE, "Byte: %d - all Data", i);

		} else if (bit_start == 0 && map[i] == 0xff) {
			handle_sparse_region(file, &data_found, &data_size, vec,
    			    &vec_index, vec_size, resolution, i, true,
			    &bytes_vectored, buf, &error);
			ON_ISI_ERROR_GOTO(out, error);
		} else {
			/*
			 * handle it bit by bit...
			 */

			for (j = bit_start;
			     j < BITS_PER_CHAR && data_size < chunk_size &&
				 data_size < length;
			     j++) {
				int bitmask = 1 << j;
				int sparse = map[i] & bitmask;
				ilog(IL_TRACE, "byte: %d, bit %d, sparse: %s",
				    i, j, sparse ? "True" : "False");
				if (!sparse)
					// not a sparse area
					handle_data_region(&data_found,
					    vec, &vec_index, resolution,
					    &bytes_vectored, buf,
					    &data_size, vec_size,
					    &error);
				else
					handle_sparse_region(file, &data_found, &data_size,
					    vec, &vec_index, vec_size, resolution, i,
					    false, &bytes_vectored, buf, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
		}
		bit_start = 0;
	}
	if (data_size < length) {
		/*
		 * Another case were we could just return an iovec that
		 * covered all the data and not and error, but this shouldn't
		 * happen
		 */
		error = isi_cbm_error_new(CBM_ERROR_SHORT_SPARSE_DATA,
		    "Found %ld bytes using sparse map when expecting %ld",
		    data_size, length);
		goto out;
	} else if (data_size > length && data_found) {
		/*
		 * need to trim the last vector to reflect the case where the
		 * fill operation was at the end of the file and not a
		 * complete chunk.   Need to adjust the last iovec possibly.
		 */
		vec[vec_index].iov_len -= (data_size - length);
		log_iovec("adjusted for short data length", vec, vec_index);
	}

	ilog(IL_TRACE, "data_size: %ld, chunk_size: %ld, bytes_to_write %ld",
	    data_size, chunk_size, bytes_vectored);
	*vec_count = vec_index + 1;
	if (bytes_to_write)
		*bytes_to_write = bytes_vectored;
 out:
	isi_error_handle(error, error_out);
}

void
isi_cbm_sparse_map_to_iovec(isi_cbm_file const *file,
    sparse_area_header_t &spa, off_t offset,
    size_t length, void *data, size_t chunk_size,
    struct iovec *vec, int *vec_count,
    size_t *bytes_to_write, struct isi_error **error_out)
{
	isi_cbm_sparse_map_to_iovec(file, (void *)spa.get_sparse_map_ptr(),
	    spa.get_sparse_map_length(),
	    offset, length, data,
	    spa.get_sparse_resolution(),
	    chunk_size, vec, vec_count, bytes_to_write,
	    error_out);
}
