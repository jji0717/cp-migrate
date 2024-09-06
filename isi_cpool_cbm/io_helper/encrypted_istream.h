
#ifndef __ENCRYPTED_ISTREAM_H__
#define __ENCRYPTED_ISTREAM_H__

#include "encrypt_ctx.h"

#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_security/cpool_protect.h>

using namespace isi_cloud;

class encrypted_istream : public idata_stream
{
 public:
	// mapinfo file name encryption defaults to 128K unit
	// CMO/CDO will be be read size
	encrypted_istream(encrypt_ctx_p ectx, idata_stream &istrm,
	    size_t length, size_t readsz);

	~encrypted_istream();

	virtual size_t read(void *buff, size_t len);

	virtual bool reset();

 private:
	// encryption parameters: key/iv
	encrypt_ctx_p ectx;

	// input stream and data descriptions
	idata_stream &istrm;

	const size_t length;

	const size_t readsz;

	// encrytion algorithm context
	enc_ctx *ctx_;

	// bytes that has been read from input stream
	size_t read_bytes;

	// bytes that has been read from the subchunk
	size_t subchunk_bytes;

	// memory to read in data from input stream
	std::vector<unsigned char> raw_buffer;
};

#endif
