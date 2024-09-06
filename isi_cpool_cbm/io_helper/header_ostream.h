
#ifndef __HEADER_OSTREAM_H__
#define __HEADER_OSTREAM_H__

#include "compress_header.h"
#include "checksum_header.h"
#include "sparse_area_header.h"
#include "isi_cbm_ioh_base.h"
#include <isi_cloud_api/isi_cloud_api.h>

using namespace isi_cloud;

class header_ostream : public odata_stream
{
public:
	header_ostream(bool cmo, isi_tri_bool with_compress,
	    isi_tri_bool with_checksum, int alignment, int num_subchunk);

	virtual size_t write(void *buf, size_t len);

	virtual bool reset();

	inline bool has_checksum() {
		if (with_checksum != TB_UNKNOWN) {
			return with_checksum;
		}
		return cp_header.has_chksum();
	}

	inline bool has_compress() {
		if (with_compress != TB_UNKNOWN) {
			return with_compress;
		}
		return cp_header.is_compressed();
	}

	inline size_t get_header_size() {
		size_t total_size = 0;
		if (!is_cmo) {
			total_size += sizeof(spa_header);
		}
		if (is_cmo || with_compress) {
			total_size += sizeof(compress_header);
		}
		if (is_cmo || with_checksum) {
			total_size += (cs_header.size() * sizeof(checksum_t));
		}
		return total_size;
	}

	inline size_t get_aligned_size() {
		return MAX((size_t)alignment, get_header_size());
	}

	inline bool is_compressed() {
		return with_compress && cp_header.is_compressed();
	}

	inline size_t get_compressed_len() {
		return cp_header.len;
	}

	inline checksum_header &get_checksum() {
		return cs_header;
	}

	inline sparse_area_header_t &get_spa_header() {
		return spa_header;
	}

private:
	bool is_cmo;
	compress_header cp_header;
	checksum_header cs_header;
	sparse_area_header_t spa_header;

	isi_tri_bool with_compress;

	isi_tri_bool with_checksum;

	const int alignment;

	size_t bytes_written;
};

#endif
