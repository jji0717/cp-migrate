#pragma once
#include <ifs/ifs_types.h>
#include "isi_cbm_cache.h"
#include "isi_cpool_cbm.h"

// The read-only cache iterator.
// MT-unsafe
class cbm_cache_iterator {
public:

	/**
	 * instantiate a cache iterator to traverse through
	 * the cache ranges for file fd that enclosing [begin, end)
	 * @param fd[in] the stub file's file descriptor
	 * @param beign[in] the begin offset
	 * @param end[in] the end offset
	 * @param rt[in] the region type mask of isi_cbm_cachemask
	 * @param whole_range[in] whether the whole range (multiple,
	 *        contiguous cache regions of the same type) can be
	 *        returned as a single cache record. The
	 *        default is to return a single cache region per record.
	 */
	cbm_cache_iterator(struct isi_cbm_file *file, off_t begin, off_t end,
	    int rt, bool whole_range = false);

	/** get the next cache info record
	 * @param record[in] the cache record information if found.
	 * @param error_out[out] error if any
	 * @return if an entry is returned.
	 */
	bool next(isi_cbm_cache_record_info *record,
	    struct isi_error **error_out);

	virtual ~cbm_cache_iterator();

	/**
	 * Get cahe manager. TBD: refactor this iterator
	 * to take the cache as an input.
	 */
	cpool_cache &cache() { return cache_; }
private:
	struct isi_cbm_file *file_;
	off_t begin_;
	off_t end_;
	int rt_;
	cpool_cache &cache_;
	off_t start_region_;
	off_t end_region_;
	off_t current_region_;
	off_t range_start_;
	off_t range_end_;
	isi_cbm_cache_status cs_;
	bool whole_range_;
};

