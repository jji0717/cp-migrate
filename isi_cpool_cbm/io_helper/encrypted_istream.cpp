
#include <isi_ilog/ilog.h>
#include "encrypted_istream.h"

#define BUF_SZ (16 * 1024)

encrypted_istream::encrypted_istream(encrypt_ctx_p ectx,
    idata_stream &istrm, size_t length, size_t readsz)
    : ectx(ectx), istrm(istrm), length(length), readsz(readsz),
    ctx_(NULL), read_bytes(0), subchunk_bytes(0), raw_buffer(BUF_SZ)
{
	ASSERT(ectx);
}

encrypted_istream::~encrypted_istream()
{
	if (ctx_) {
		cpool_enc_free(ctx_);
		ctx_ = NULL;
	}
}

size_t
encrypted_istream::read(void *buff, size_t len)
{
	size_t n = 0;

	if (!ctx_ && !(ctx_ = cpool_enc_new())) {
		ilog(IL_ERR, "failed to initialize encryption context");
		return CODE_ABORT;
	}

	while (n < len && read_bytes < length) {
		size_t bytes = MIN(readsz - subchunk_bytes, len - n);

		if (bytes > raw_buffer.size())
			bytes = raw_buffer.size();

		if (subchunk_bytes == 0) {
			iv_t iv = ectx->get_chunk_iv();

			if (!cpool_enc_init(ctx_, &ectx->dek, &iv)) {
				ilog(IL_ERR, "failed to initialize the encryption");
				return CODE_ABORT;
			}
		}

		bytes = istrm.read(raw_buffer.data(), bytes);

		// in case of padding for azure
		if (bytes == 0)
			break;

		read_bytes += bytes;
		subchunk_bytes += bytes;

		if (!cpool_enc_update(ctx_, raw_buffer.data(),
		    (char *)buff + n, bytes)) {
			ilog(IL_ERR, "failed to encrypt data");
			return CODE_ABORT;
		}

		n += bytes;

		// next subchunk
		if (subchunk_bytes == readsz)
			subchunk_bytes = 0;
	}

	return n;
}

bool
encrypted_istream::reset()
{
	if (ctx_) {
		cpool_enc_free(ctx_);
		ctx_ = NULL;
	}

	read_bytes = 0;
	subchunk_bytes = 0;

	return istrm.reset();
}
