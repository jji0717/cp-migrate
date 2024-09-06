
#ifndef __COMPRESS_FILTER__
#define __COMPRESS_FILTER__
#include <isi_cloud_api/isi_cloud_api.h>
#include <zlib.h>
#include <vector>

using namespace isi_cloud;

class compress_filter : public odata_stream
{
 public:
	compress_filter(odata_stream &ostrm);

	/*
	 * decompress the data and write back to the ostrm
	 */
	virtual size_t write(void *buff, size_t len);

	virtual bool reset();

	void flush();

 private:
	bool do_inflate();

 private:
	odata_stream &ostrm;
	std::vector<char> local_buf;

	z_stream d_strm;
};

#endif
