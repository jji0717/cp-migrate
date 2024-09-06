
#include "headered_istream.h"

size_t
headered_istream::read(void *buf, size_t len)
{
	size_t bytes_from_stream = 0;
	bool expect_compress_hdr = (is_cmo || with_compress);
	bool expect_chksum_hdr   = (is_cmo || with_checksum);
	bool expect_spa_hdr      = (!is_cmo);

	size_t cs_offset =  0;
	size_t cs_len = 0;
	size_t spa_offset = 0;
	char *buf_to_fill = (char *) buf;

	if (expect_compress_hdr) {
		cs_offset = sizeof(cp_header);
	}
	if (expect_chksum_hdr) {
		cs_len = cs_header.size() * sizeof(checksum_t);
	}
	spa_offset = cs_offset + cs_len;

	// read in the compression header
	if (expect_compress_hdr && taken_bytes < sizeof(cp_header)) {
		size_t bytes_to_take =
		    MIN(len, sizeof(cp_header) - taken_bytes);

		if (bytes_to_take > 0) {
			memcpy(buf_to_fill, 
			    (char *)&cp_header + taken_bytes,
			    bytes_to_take);

			len -= bytes_to_take;
			taken_bytes += bytes_to_take;
			buf_to_fill = buf_to_fill + bytes_to_take;
		}
	}

	// read in the checksum header
	if (expect_chksum_hdr && taken_bytes < cs_offset + cs_len) {
		size_t bytes_to_take =
		    MIN(len, cs_offset + cs_len - taken_bytes);

		if (bytes_to_take > 0) {
			memcpy(buf_to_fill, 
			    (char *)cs_header.data() + taken_bytes - cs_offset,
			    bytes_to_take);

			len -= bytes_to_take;
			taken_bytes += bytes_to_take;
			buf_to_fill = buf_to_fill + bytes_to_take;
		}
	}

	// read in the sparse area header
	if (expect_spa_hdr &&
	    taken_bytes < (spa_offset + sizeof(spa_header))) {

		size_t bytes_to_take = MIN(len,
		    spa_offset + sizeof(sparse_area_header_t) - taken_bytes);

		if (bytes_to_take > 0) {
			memcpy(buf_to_fill,
			    (char *)&spa_header + taken_bytes - spa_offset,
			    bytes_to_take);

			len -= bytes_to_take;
			taken_bytes += bytes_to_take;
			buf_to_fill = buf_to_fill + bytes_to_take;
		}
	}

	// alignment need for azure
	if (taken_bytes >= get_header_size() &&
	    taken_bytes < get_aligned_size()) {
		size_t bytes_to_take =
		    MIN(len, get_aligned_size() - taken_bytes);

		if (bytes_to_take > 0) {
			memset(buf_to_fill, 0x00, bytes_to_take);

			len -= bytes_to_take;
			taken_bytes += bytes_to_take;
			buf_to_fill = buf_to_fill + bytes_to_take;
		}
	}

	// take real data
	if (len > 0 && pstrm) {
		bytes_from_stream += pstrm->read(buf_to_fill, len);
		taken_bytes += bytes_from_stream;
	}


	return (buf_to_fill - (char *)buf) + bytes_from_stream;
}

bool
headered_istream::reset()
{
	taken_bytes = 0;

	if (pstrm != NULL)
		pstrm->reset();

	return true;
}

void
headered_istream::set_checksum(const checksum_header &cs)
{
	ASSERT(with_checksum && cs.size() == cs_header.size());

	for (unsigned i=0; i<cs.size(); i++)
		cs_header[i] = cs[i];

	cp_header.set_with_chksum(true);
}

size_t
headered_istream::apply(idata_stream *istream, size_t length)
{
	// data stream
	this->pstrm = istream;

	this->taken_bytes = 0;

	// return the whole length
	return get_aligned_size() + length;
}

headered_istream::headered_istream(bool cmo,
    bool with_compress, bool with_checksum, int alignment, int num_subchunk,
    uint32_t sparse_resolution,  int sparse_map_len, const uint8_t *sparse_map) :
    is_cmo(cmo), pstrm(NULL), with_compress(with_compress),
    with_checksum(with_checksum),
    alignment(alignment), taken_bytes(0),
    cs_header(with_compress ? 1 : num_subchunk)
{
	spa_header.spa_version = CPOOL_SPA_VERSION;
	if (cmo) {
		spa_header.sparse_map_length = 0;
		spa_header.sparse_resolution = 0;
		memset(spa_header.sparse_map, 0, SPARSE_MAP_SIZE);
	} else {
		int i;
		if (SPARSE_MAP_SIZE > sparse_map_len) {
			memset(spa_header.sparse_map, 0, SPARSE_MAP_SIZE);
		}
		spa_header.sparse_map_length =
			MIN(SPARSE_MAP_SIZE, sparse_map_len);
		spa_header.sparse_resolution = sparse_resolution;
		for (i = 0; i < spa_header.sparse_map_length; i++) {
			spa_header.sparse_map[i] = sparse_map[i];
		}
	}
	memset(&cp_header, 0x00, sizeof(cp_header));

	// default alignment to 0 if not compress/checksum
	if (!with_compress && !with_checksum && !is_cmo)
		alignment = 0;
}
