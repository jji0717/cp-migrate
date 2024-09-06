
#ifndef __RANGE_FILTER__
#define __RANGE_FILTER__

#include <isi_cloud_api/isi_cloud_api.h>

using namespace isi_cloud;

/**
 * stream class having capability of truncating
 * the output stream from either ends and the only
 * desired scope is returned.
 */
class range_filter : public odata_stream
{
public:
	/**
	 * stream to handle aligned read
	 * @param out[in] the lower stream
	 * @param offset[in] the user's offset
	 * @param adj_off[in] adjusted offset due to read alignment
	 * @param user_len[in] total length to read by the user
	 */
	range_filter(odata_stream &out,
	    size_t offset, size_t adj_off, size_t len);

	virtual size_t write(void *buff, size_t len);

	virtual bool reset();

private:
	odata_stream &out_;
	const size_t offset_;
	const size_t adj_off_;
	const size_t user_len_;
	size_t cur_;
	size_t written_;
};

#endif
