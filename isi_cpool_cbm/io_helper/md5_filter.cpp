
#include "md5_filter.h"

md5_filter::md5_filter(odata_stream &out)
    : out_(out), md5_ready_(false)
{
	MD5_Init(&ctx_);
}

size_t
md5_filter::write(void *buff, size_t len)
{
	size_t sz_wr = out_.write(buff, len);

	if (sz_wr != 0)
		MD5_Update(&ctx_, buff, sz_wr);
	else
		final(); // end of output

	return sz_wr;
}

void
md5_filter::final()
{
	if (md5_ready_)
		return;

	unsigned char digest[MD5_DIGEST_LENGTH];
	MD5_Final(digest, &ctx_);
	md5_.append((char *) digest, MD5_DIGEST_LENGTH);
	md5_ready_ = true;
}
