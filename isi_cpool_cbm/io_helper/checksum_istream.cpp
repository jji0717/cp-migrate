#include "checksum_istream.h"

#define BUF_SZ  (1024 * 16)

checksum_istream::checksum_istream(
    idata_stream &istrm, size_t length, size_t readsz,
    off_t offset_in_obj, checksum_header &cksum)
    : istrm_(istrm), length_(length), readsz_(readsz),
    subchunk_start_idx_(offset_in_obj / readsz),
    subchunk_idx_(subchunk_start_idx_),
    subchunk_bytes_read_(0),
    cksum_(cksum), ctx_(NULL)
{
	// checksum unit ought to be subchunk aligned
	ASSERT(offset_in_obj % readsz == 0);
}

checksum_istream::~checksum_istream()
{
	if (ctx_)
		cpool_cksum_destroy(ctx_);
}

// by streaming the data through the checksum_istream, data read out
// and checksum is calculated with alignment on each subchunk
size_t
checksum_istream::read(void *buff, size_t len)
{
	const size_t read_bytes = istrm_.read(buff, len);

	if (!ctx_) {
		if ((ctx_ = cpool_cksum_create()) == NULL) {
			throw isi_exception(
			    "failed to initialize checksum context");
		}
	}

	for (size_t n = 0; n < read_bytes; ) {
		size_t bytes =
		    MIN(readsz_ - subchunk_bytes_read_, read_bytes - n);

		// reinitialize for each subchunk
		if (subchunk_bytes_read_ == 0) {
			if (!cpool_cksum_init(ctx_)) {
				throw isi_exception(
				    "failed to initialize checksum context");
			}
		}

		if (!cpool_cksum_update(ctx_, (char *)buff + n, bytes))
			throw isi_exception("failed to calculate checksum");

		subchunk_bytes_read_ += bytes;
		if (subchunk_bytes_read_ == readsz_) {
			if (!cpool_cksum_final(ctx_, &cksum_[subchunk_idx_])) {
				throw isi_exception(
				    "failed to calculate checksum");
			}

			++subchunk_idx_;
			subchunk_bytes_read_ = 0;
		}

		n += bytes;
	}

	return read_bytes;
}

bool
checksum_istream::reset()
{
	if (subchunk_bytes_read_ > 0)
		cpool_cksum_final(ctx_, &cksum_[subchunk_idx_]);

	subchunk_idx_ = subchunk_start_idx_;
	subchunk_bytes_read_ = 0;

	return istrm_.reset();
}

void
checksum_istream::calc_checksum()
{
	std::vector<char> buf(MIN(length_, BUF_SZ));

	for (size_t n = 0; n < length_; ) {
		size_t bytes = MIN(buf.size(), length_ - n);
		size_t ret = read(buf.data(), bytes);

		if (bytes != ret)
			break;

		n += bytes;
	}

	reset();
}
