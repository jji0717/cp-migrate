
#ifndef __ENCRYPT_FILTER_H__
#define __ENCRYPT_FILTER_H__

#include <isi_cloud_api/isi_cloud_api.h>
#include <vector>
#include "encrypt_ctx.h"

using namespace isi_cloud;

class encrypt_filter : public odata_stream
{
public:
	encrypt_filter(odata_stream &ostrm, encrypt_ctx_p ectx, size_t readsz);

	~encrypt_filter();

	virtual size_t write(void *buff, size_t len);

	virtual bool reset();

	void flush();

private:
	odata_stream &ostrm_;

	std::vector<unsigned char> local_buf_;

	const size_t readsz_;

	size_t bytes_written_;

	// encryption parameters
	encrypt_ctx_p ectx_;

	// encryption algo context
	enc_ctx *ctx_;
};

#endif
