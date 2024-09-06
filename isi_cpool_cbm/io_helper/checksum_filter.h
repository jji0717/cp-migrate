
#ifndef __CHECKSUM_FILTER_H__
#define __CHECKSUM_FILTER_H__

#include "checksum_header.h"
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_security/cpool_protect.h>

using namespace isi_cloud;

class checksum_filter : public odata_stream
{
public:
	checksum_filter(odata_stream &ostrm, const checksum_header &cs,
	    off_t offset, size_t readsz, size_t headsz, bool check);

	~checksum_filter();

	/**
	 * check the checksum on the data buffer
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

	inline bool is_match()
	{
		return match_;
	}

	inline void log_mismatch()
	{
		log_ = true;
	}

private:
	/**
	 * do checksum validation
	 *
	 * @param idx[in]      subchunk index within the requested range
	 * @return             true on checksum match
	 */
	bool checksum_match(int idx);

private:
	odata_stream &ostrm_;

	const checksum_header &cs_;

	// start offset for the chunk, it should subchunk aligned
	const off_t offset_;

	const size_t readsz_;

	// for cmo, the whole object read back, we need to remove
	// 128 byte checksum header explicityly
	const size_t headsz_;

	size_t head_taken_;
	size_t bytes_written_;

	bool match_;

	// checksum algo context
	cksum_ctx *ctx_;

	// if we need to enforce the checksum
	bool to_check_;

	bool log_;
};

#endif
