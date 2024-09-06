
#include "encrypt_filter.h"
#include <isi_ilog/ilog.h>

namespace {
	inline void advance(void *&buff, size_t &len, size_t bytes)
	{
		buff = (char *)buff + bytes;
		len -= bytes;
	}
}

encrypt_filter::encrypt_filter(odata_stream &ostrm,
    encrypt_ctx_p ectx, size_t readsz)
    : ostrm_(ostrm), readsz_(readsz),
    bytes_written_(0), ectx_(ectx), ctx_(NULL)
{
	ASSERT(ectx, "decryption parameters are expected");
}

encrypt_filter::~encrypt_filter()
{
	if (ctx_)
		cpool_enc_free(ctx_);
}

size_t
encrypt_filter::write(void *buff, size_t len)
{
	size_t bytes_taken = 0;

	if (!ctx_) {
		ctx_ = cpool_enc_new();

		if (!ctx_) {
			ilog(IL_ERR, "failed to initialize decryption context");
			return CODE_ABORT;
		}
	}

	while (len > 0) {
		size_t subchunk_bytes = bytes_written_ % readsz_;
		// subchunk aligned bytes
		size_t bytes = MIN(len, readsz_ - subchunk_bytes);

		// start a new subchunk
		if (subchunk_bytes == 0) {
			iv_t iv = ectx_->get_chunk_iv();

			if (!cpool_dec_init(ctx_, &ectx_->dek, &iv)) {
				ilog(IL_ERR, "failed to initialize the "
				    "decryption context");
				return CODE_ABORT;
			}
		}

		// make sure we have enough output space
		if (local_buf_.size() < bytes)
			local_buf_.resize(bytes);

		if (!cpool_dec_update(ctx_, buff, local_buf_.data(), bytes)) {
			ilog(IL_ERR, "failed to decrypt the data");
			return CODE_ABORT;
		}

		if (ostrm_.write(local_buf_.data(), bytes) != bytes) {
			ilog(IL_ERR, "failed to down stream %ld bytes data", bytes);
			return CODE_ABORT;
		}

		advance(buff, len, bytes);
		bytes_taken += bytes;
		bytes_written_ += bytes;
	}

	return bytes_taken;
}

bool
encrypt_filter::reset()
{
	bytes_written_ = 0;

	if (ctx_) {
		cpool_enc_free(ctx_);
		ctx_ = NULL;
	}

	return ostrm_.reset();
}

void
encrypt_filter::flush()
{
}
