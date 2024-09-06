#pragma once

#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_security/cpool_protect.h>
#include "checksum_header.h"

using namespace isi_cloud;

/**
 * calculates the checksum of the streamed data
 */
class checksum_istream : public idata_stream
{
 public:
	checksum_istream(idata_stream &istrm, size_t length, size_t readsz,
	    off_t offset_in_obj, checksum_header &cksum);

	~checksum_istream();

	/**
	 * data streamed to buff, meanwhile checksum is calculated
	 */
	virtual size_t read(void *buff, size_t len);

	virtual bool reset();

	void calc_checksum();

 private:
	idata_stream &istrm_;
	// length of the input stream
	const size_t length_;
	// subchunk size
	const size_t readsz_;
	// start sunchunk idx, calulated from the offset
	const int    subchunk_start_idx_;

	// start subchunk index
	int subchunk_idx_;
	// bytes for subchunk that has been cs calculated
	size_t subchunk_bytes_read_;

	// checksum output destination
	checksum_header &cksum_;

	cksum_ctx *ctx_;
};
