
#include "checksum_filter.h"
#include <isi_ufp/isi_ufp.h>

namespace{
	inline void advance(void *&buff, size_t &len, size_t bytes)
	{
		buff = (char *)buff + bytes;
		len -= bytes;
	}
}

checksum_filter::checksum_filter(odata_stream &ostrm, const checksum_header &cs,
    off_t offset, size_t readsz, size_t headsz, bool check)
    : ostrm_(ostrm), cs_(cs), offset_(offset),
    readsz_(readsz), headsz_(headsz), head_taken_(0), bytes_written_(0),
    match_(true), ctx_(NULL), to_check_(check), log_(false)
{
	ASSERT(offset % readsz == 0, "%ld %ld", offset, readsz);
}

checksum_filter::~checksum_filter()
{
	if (ctx_)
		cpool_cksum_destroy(ctx_);
}

size_t
checksum_filter::write(void *buff, size_t len)
{
	// bytes taken in this round
	size_t bytes_taken = 0;

	// cmo object is read back in whole including the 128 byte
	// checksum header, we mark it in headsz_, and ignore it
	if (headsz_ > head_taken_) {
		size_t bytes = MIN(len, headsz_ - head_taken_);

		advance(buff, len, bytes);
		bytes_taken = bytes;
		head_taken_ += bytes;
	}

	// no checksum at all
	if (cs_.size() == 0 || !to_check_)
		return bytes_taken + ostrm_.write(buff, len);

	if (!ctx_) {
		ctx_ = cpool_cksum_create();

		if (!ctx_) {
			ilog(IL_ERR, "failed to create cksum_ctx\n");
			return CODE_ABORT;
		}
	}

	while (len > 0) {
		size_t subchunk_bytes = bytes_written_ % readsz_;
		// bytes to take on subchunk boundary aligned
		size_t bytes = MIN(len, readsz_ - subchunk_bytes);

		// start a new subchunk
		if (subchunk_bytes == 0) {
			if (!cpool_cksum_init(ctx_)) {
				ilog(IL_ERR,
				    "failed to initialize checksum context");
				return CODE_ABORT;
			}
		}

		if (!cpool_cksum_update(ctx_, buff, bytes)) {
			ilog(IL_ERR, "failed to calculate the checksum");
			return CODE_ABORT;
		}

		// end a subchunk
		if ((bytes_written_ + bytes) % readsz_ == 0) {
			int idx = (bytes_written_ + bytes - 1) / readsz_;

			if (!checksum_match(idx)) {
				if (log_) {
					ilog(IL_ERR,
					    "failed to match the checksum "
					    "at offset %ld",
					    offset_ + bytes_written_ + bytes);
				}
				return CODE_ABORT;
			}
		}

		if (ostrm_.write(buff, bytes) != bytes) {
			ilog(IL_ERR, "failed to stream down %ld bytes", bytes);
			return CODE_ABORT;
		}
		bytes_taken += bytes;
		bytes_written_ += bytes;
		advance(buff, len, bytes);
	}

	return bytes_taken;
}

bool
checksum_filter::reset()
{
	if (ctx_) {
		cpool_cksum_destroy(ctx_);
		ctx_ = NULL;
	}
	head_taken_ = 0;
	bytes_written_ = 0;

	return ostrm_.reset();
}

void
checksum_filter::flush()
{
	int idx = (bytes_written_ - 1) / readsz_;
	bool match = true;

	if (bytes_written_ % readsz_ != 0)
	       match =	checksum_match(idx);

	if (!match) {
		// throw cl exception by purpose
		throw cl_exception(CL_ABORTED_BY_CALLBACK,
		    "failed to match the checksum");
	}
}

bool
checksum_filter::checksum_match(int idx)
{
	bool match = true;
	checksum_t checksum;

	idx += offset_ / readsz_;

	if (!cpool_cksum_final(ctx_, &checksum)) {
		ilog(IL_ERR, "failed to calculate the checksum");
		return false;
	}

	UFAIL_POINT_CODE(cpool_checksum_fail, {
		match = false;
	});

	if (memcmp(&checksum, &cs_[idx], MAX_CHECKSUM_SIZE))
		match = false;

	match_ &= match;

	return match;
}
