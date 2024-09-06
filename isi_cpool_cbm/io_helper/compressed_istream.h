#ifndef __COMPRESSED_ISTREAM_H__
#define __COMPRESSED_ISTREAM_H__

#include <unistd.h>

#include <isi_cloud_api/isi_cloud_api.h>
#include <zlib.h>

#include "isi_cbm_buffer.h"

void cloudpools_cleanup_compression_tmp_file();

using namespace isi_cloud;

class compressed_istream : public idata_stream
{
public:
	compressed_istream(const std::string &fname,
	    idata_stream &istrm, size_t len);

	~compressed_istream();

	inline void *ptr()
	{
		return compressed_buf.get() ?
		    compressed_buf->ptr() : NULL;
	}

	inline bool is_compressed()
	{
		return compressed;
	}

	inline size_t get_length()
	{
		return length;
	}

	virtual size_t read(void *buff, size_t len)
	{
		return tmpfd > 0 ? ::read(tmpfd, buff, len) :
		    compressed_buf->read(buff, len);
	}

	virtual bool reset()
	{
		if (tmpfd > 0)
			lseek(tmpfd, 0, SEEK_SET);
		else
			compressed_buf->rewind();

		return true;
	}

	static void set_threshold(int sz);

	static void reset_threshold();

private:
	int deflate_to_file(int flush, void *buf, int buf_sz);

	void compress_to_file(idata_stream &istrm, size_t len);

	void compress_to_mem(idata_stream &istrm, size_t len);

private:
	z_stream c_strm;

	// either cache the compressed data
	std::auto_ptr<isi_cbm_buffer> compressed_buf;
	// or foward to a tmp file
	int tmpfd;
	std::string tmp_fpath;

	bool compressed;
	size_t length;
};

#endif
