
#include "sparse_area_filter.h"
#include <isi_ufp/isi_ufp.h>

sparse_area_filter::sparse_area_filter(odata_stream &ostrm,
    off_t offset, size_t readsz, size_t headsz)
    : ostrm_(ostrm), readsz_(readsz), headsz_(headsz),
    head_taken_(0), bytes_written_(0)
{
	ASSERT(offset % readsz == 0, "%ld %ld", offset, readsz);
}

sparse_area_filter::~sparse_area_filter()
{

}

size_t
sparse_area_filter::write(void *buff, size_t len)
{
	// bytes taken in this round
	size_t bytes_taken = 0;

	// cmo object is read back in whole including the 128 byte
	// checksum header, we mark it in headsz_, and ignore it
	if (headsz_ > head_taken_) {
		size_t bytes = MIN(len, headsz_ - head_taken_);

		buff = (char *)buff + bytes;
		len -= bytes;
		bytes_taken = bytes;
		head_taken_ += bytes;
	}

	while (len > 0) {
		size_t subchunk_bytes = bytes_written_ % readsz_;
		// bytes to take on subchunk boundary aligned
		size_t bytes = MIN(len, readsz_ - subchunk_bytes);

		if (ostrm_.write(buff, bytes) != bytes) {
			ilog(IL_ERR, "failed to stream down %ld bytes", bytes);
			return CODE_ABORT;
		}
		bytes_taken += bytes;
		bytes_written_ += bytes;
		buff = (char *)buff + bytes;
		len -= bytes;
	}

	return bytes_taken;
}

bool
sparse_area_filter::reset()
{
	head_taken_ = 0;
	bytes_written_ = 0;

	return ostrm_.reset();
}

void
sparse_area_filter::flush()
{

}

