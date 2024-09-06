
#include <isi_ilog/ilog.h>
#include "header_ostream.h"

header_ostream::header_ostream(bool cmo, isi_tri_bool with_compress,
    isi_tri_bool with_checksum,
    int alignment, int num_subchunk)
    : is_cmo(cmo), cs_header(num_subchunk), with_compress(with_compress),
    with_checksum(with_checksum), alignment(alignment), bytes_written(0)
{
	reset();
}

size_t
header_ostream::write(void *buf, size_t len)
{
	// in case of CMO, we always expect the object headers.
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

	// write out the compression header
	if (expect_compress_hdr && bytes_written < sizeof(cp_header)) {
		size_t bytes_to_write =
		    MIN(len, sizeof(cp_header) - bytes_written);

		if (bytes_to_write > 0) {
			memcpy((char *) &cp_header + bytes_written,
			    buf_to_fill, bytes_to_write);

			len -= bytes_to_write;
			bytes_written += bytes_to_write;
			buf_to_fill += bytes_to_write;
		}
	}

	// write out the checksum header
	if (expect_chksum_hdr && bytes_written < cs_offset + cs_len) {
		size_t bytes_to_write =
		    MIN(len, cs_offset + cs_len - bytes_written);

		if (bytes_to_write > 0) {
			memcpy((char *) cs_header.data() +
			    bytes_written - cs_offset,
			    buf_to_fill, bytes_to_write);

			len -= bytes_to_write;
			bytes_written += bytes_to_write;
			buf_to_fill += bytes_to_write;
		}
	}

	// write out the sparse area header
	if (expect_spa_hdr &&
	    bytes_written < (spa_offset + sizeof(spa_header))) {

		size_t bytes_to_write = MIN(len,
		    spa_offset + sizeof(spa_header) - bytes_written);

		if (bytes_to_write > 0) {
			memcpy((char *)&spa_header +
			    bytes_written - spa_offset,
			    buf_to_fill, bytes_to_write);

			len -= bytes_to_write;
			bytes_written += bytes_to_write;
			buf_to_fill += bytes_to_write;
		}
	}

	// something wrong
	if (len > 0) {
		ilog(IL_ERR,
		    "header stream expects %ld bytes, but got %ld bytes "
		    "(%lld + %lld)",
		    get_header_size(), len + bytes_written,
		    len, bytes_written);
	}

	// return the taken bytes
	return (size_t)(buf_to_fill - (char *)buf);
}

bool
header_ostream::reset()
{
	bytes_written = 0;

	memset(&cp_header, 0x00, sizeof(cp_header));
	memset(cs_header.data(), 0x00, cs_header.size() * sizeof(checksum_t));
	memset(&spa_header, 0x00, sizeof(spa_header));

	return true;
}
