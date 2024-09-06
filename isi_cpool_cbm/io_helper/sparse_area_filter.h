
#ifndef __SPA_FILTER_H__
#define __SPA_FILTER_H__

#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_security/cpool_protect.h>
#include "sparse_area_header.h"

using namespace isi_cloud;

class sparse_area_filter : public odata_stream
{
public:
	sparse_area_filter(odata_stream &ostrm,
	    off_t offset, size_t readsz, size_t headsz);

	~sparse_area_filter();

	/**
	 * check the sparse area on the data buffer
	 * compression/encryption pass in each subchunk, otherwise whole
	 * data buffer is passed in
	 * for CMO, we also deduct the header, since whole object is returned
	 *
	 * @param buff[in]   data buffer
	 * @param len[in]    length of the data buffer
	 */
	virtual size_t write(void *buff, size_t len);

	virtual bool reset();

	void flush();

private:
	odata_stream &ostrm_;

	const size_t readsz_;

	// for cmo, the whole object read back, we need to remove
	// 128 byte checksum header explicityly
	const size_t headsz_;

	size_t head_taken_;
	size_t bytes_written_;
};

#endif
