
#ifndef __HEADERED_ISTREAM_H__
#define __HEADERED_ISTREAM_H__

#include "compress_header.h"
#include "checksum_header.h"
#include "sparse_area_header.h"
#include <isi_cloud_api/isi_cloud_api.h>

using namespace isi_cloud;

class headered_istream : public idata_stream
{
public:
	virtual size_t read(void *buf, size_t len);

	virtual bool reset();

	inline bool has_compress_header() { return (is_cmo || with_compress); }

	inline bool has_checksum_header() { return (is_cmo || with_checksum); }

	inline bool has_header() {
		// CMO's always have a header, and now CDO's
		// always have at least the sparse area header
		return true;
	}

	inline size_t get_header_size() {
		size_t  total_size = 0;
		if (!is_cmo) {
			total_size += sizeof(spa_header);
		}
		if (has_compress_header()) {
			total_size += sizeof(cp_header);
		}
		if (has_checksum_header()) {
			total_size += (cs_header.size() * sizeof(checksum_t));
		}
		return total_size;
	}

	inline size_t get_aligned_size() {
		return MAX((size_t)alignment, get_header_size());
	}

	inline checksum_header &get_checksum() {
		return  cs_header;
	}

	inline void set_compression(bool compressed) {
		if (with_compress)
			cp_header.set_compression(compressed);
	}

	inline void set_compressed_len(size_t length) {
		if (with_compress)
			cp_header.len = length;
	}

	void set_checksum(const checksum_header &cs);

	size_t apply(idata_stream *istream, size_t length);

	headered_istream(bool is_cmo, bool with_compress, bool with_checksum,
	    int alignment, int num_subchunk, uint32_t sparse_resolution,
	    int sparse_map_len, const uint8_t  *sparse_map);
private:
	bool is_cmo;
	// data stream
	idata_stream *pstrm;

	bool with_compress;
	bool with_checksum;

	// azure header needs 512 byte alignment
	const int alignment;

	// bytes that has been read out
	size_t taken_bytes;

	// header added to the data stream
	compress_header cp_header;
	checksum_header cs_header;
	sparse_area_header_t spa_header;
};

#endif
