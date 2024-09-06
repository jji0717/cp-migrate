#include "isi_cbm_cache_iterator.h"

#include <errno.h>
#include <isi_util/isi_error.h>
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include <isi_ilog/ilog.h>

cbm_cache_iterator::cbm_cache_iterator(struct isi_cbm_file *file,
    off_t begin, off_t end, int rt, bool whole_range) :
    file_(file), begin_(begin), end_(end), rt_(rt),
    cache_(*file->cacheinfo),
    start_region_(-1), end_region_(-1), current_region_(-1),
    range_start_(-1), range_end_(-1), cs_(ISI_CPOOL_CACHE_INVALID),
    whole_range_(whole_range)
{
}

// TBD-nice: remove this once we consolidate the definitions.
isi_cbm_cache_region_type
region_type_from_cache_status(isi_cbm_cache_status cs)
{
	switch (cs) {
	case ISI_CPOOL_CACHE_NOTCACHED:
		return RT_NOT_CACHED;
	case ISI_CPOOL_CACHE_CACHED:
		return RT_CACHED;
	case ISI_CPOOL_CACHE_DIRTY:
	case ISI_CPOOL_CACHE_INPROGRESS:
		return RT_MODIFIED;
	default:
		return RT_INVALID;
	}
}

bool
cbm_cache_iterator::next(
    isi_cbm_cache_record_info *record,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	bool found = false;
	bool ch_locked = false;
	size_t region_size = 0;
	off_t ret_start, ret_end;

	ilog(IL_DEBUG, "cbm_cache_iterator::next() begin current_region: %lld, end_region: %lld", current_region_, end_region_);

	if (!cache_.is_cache_open()) {

		cache_.cache_open(file_->fd,
		    false, false, &error);
		if (error && isi_system_error_is_a(error, ENOENT)) {
			// The whole file is not cached.
			isi_error_free(error);
			error = NULL;
			goto out;
		}
		if (error) {
			isi_error_add_context(error,
			    "Failed to call cpool_cache::cache_open "
			    "for %{}", isi_cbm_file_fmt(file_));
			goto out;
		}
	}

	if (current_region_ > end_region_)
		goto out;

	region_size = cache_.get_regionsize();

	if (start_region_ == -1) {
		start_region_ = begin_ / region_size;
		end_region_ = ((end_ > 0 ? end_ : 1) - 1) / region_size;
	}

	if (current_region_ == -1 || current_region_ > range_end_) {

		if (current_region_ == -1)
			current_region_ = start_region_;

		range_start_ = current_region_;

		// we are potentially looking at the whole file, lock it down
		cache_.cacheheader_lock(false, &error);
		ON_ISI_ERROR_GOTO(out, error);
		ch_locked = true;

		cache_.get_status_range(&range_start_, &range_end_, rt_, &cs_,
		    &error);

		if (error && isi_cbm_error_is_a(error,
		    CBM_NO_REGION_ERROR)) {
			isi_error_free(error);
			error = NULL;
			goto out;
		}

		if (error && isi_cbm_error_is_a(error,
			CBM_CACHEINFO_SIZE_ERROR)) {
			ilog(IL_DEBUG, "Reading cache state error for %{}, "
			    "due to truncation. Details: %#{}",
			    isi_cbm_file_fmt(file_),
			    isi_error_fmt(error));
			isi_error_free(error);
			error = isi_cbm_error_new(CBM_FILE_TRUNCATED,
			    "The file was truncated before reading the cache "
			    "status for %{}, start: %ld, end: %ld.",
			    isi_cbm_file_fmt(file_), range_start_, range_end_);
			goto out;
		}

		if (error) {
			isi_error_add_context(error,
			    "Failed to call cpool_cache::get_status_range "
			    "for %{}", isi_cbm_file_fmt(file_));
			goto out;
		}

                if (range_start_ > end_region_)
                        goto out;
                if (range_end_ > end_region_)
                        range_end_ = end_region_;

		current_region_ = range_start_;
	}

	// now we are good to get the region info
	ret_start = whole_range_ ? range_start_ : current_region_;
	ret_end = whole_range_ ? range_end_ : current_region_;

        record->offset = ret_start * region_size;
        record->length = (ret_end != end_region_) ? ((ret_end + 1) * region_size) : end_;
        record->length -= record->offset;

	record->region_type = cs_;

	// advance the cursor for the next iteration
	current_region_ = ret_end + 1;

	found = true;
	ilog(IL_DEBUG, "cbm_cache_iterator::next() end current_region: %llu, end_region: %llu",	current_region_, end_region_);

 out:
	if (ch_locked)
		cache_.cacheheader_unlock(isi_error_suppress());

	isi_error_handle(error, error_out);
	return found;
}


cbm_cache_iterator::~cbm_cache_iterator()
{
}
