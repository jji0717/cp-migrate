#pragma once

#include <string>
#include <isi_cloud_api/isi_cloud_api.h>

using namespace isi_cloud;

/**
 * simple buffer that supports consecutive writes, after which
 * with consecutive reads
 * mixing the write/read is not guaranteed to work
 */
class isi_cbm_buffer
{
	typedef unsigned char byte;
public:
	/**
	 * construct a buffer with specified capacity
	 */
	isi_cbm_buffer(size_t capacity) :
	    capacity(capacity), pbuf_start((byte *)malloc(capacity)), //////初始化buff的容量
	    pcur(pbuf_start), data_sz(0)
	{
		ASSERT(pbuf_start);
	}

	/**
	 * construct buffer with specified capacity
	 */
	isi_cbm_buffer(size_t capacity, size_t data_sz) :
	    capacity(capacity), pbuf_start((byte *)malloc(capacity)),
	    pcur(pbuf_start), data_sz(data_sz)
	{
		ASSERT(pbuf_start);
		ASSERT(data_sz <= capacity);
	}

	~isi_cbm_buffer()
	{
		if (pbuf_start)
			free(pbuf_start);
	}

	/**
	 * return the data
	 */
	void *ptr()
	{
		return pcur;
	}

	/**
	 * return the left capacity of thebuffer
	 */
	inline size_t left_capacity()
	{
		return capacity - data_sz;
	}

	/**
	 * return the size of the data in the buffer
	 */
	inline size_t get_size()
	{
		return data_sz;
	}

	/**
	 * set the size of the data in the buffer
	 */
	inline void set_data_size(size_t sz)
	{
		data_sz = sz;
	}

	/**
	 * append a string to the buffer
	 */
	isi_cbm_buffer &operator<<(const std::string &str)
	{
		ASSERT(left_capacity() >= str.size());

		memcpy(pbuf_start + data_sz, str.c_str(), str.size());
		data_sz += str.size();

		return *this;
	}

	/**
	 * append an object (primitive or plain structres only)
	 */
	template<typename T>
	isi_cbm_buffer &operator<<(const T &t)
	{
		ASSERT(left_capacity() >= sizeof(t));

		memcpy(pbuf_start + data_sz, &t, sizeof(t));
		data_sz += sizeof(t);

		return *this;
	}

	/**
	 * append a memory
	 */
	isi_cbm_buffer &append(void *src, size_t len)
	{
		ASSERT(left_capacity() >= len);

		// ensured enough space
		memcpy(pbuf_start + data_sz, src, len);
		data_sz += len;

		return *this;
	}

	/**
	 * append a stream
	 */
	isi_cbm_buffer &append(idata_stream &istrm, size_t len)
	{
		ASSERT(left_capacity() >= len);

		data_sz += istrm.read(pbuf_start + data_sz, len);
		return *this;
	}

	/**
	 * append a memory, reallocate the memory if out of capacity
	 */
	isi_cbm_buffer &append_resizable(void *src, size_t len)
	{
		if (left_capacity() < len) {
			size_t gap = pcur - pbuf_start;

			pbuf_start =
			    (byte *) realloc(pbuf_start, len + capacity);
			pcur = pbuf_start + gap;
			capacity += len;
		}

		append(src, len);
		return *this;
	}

	/**
	 * read out the data, return the actual size of the read
	 */
	size_t read(void *dest, size_t len)
	{
		size_t bytes = MIN(len, data_sz - (pcur - pbuf_start));

		if (bytes > 0) {
			memcpy(dest, pcur, bytes);
			pcur += bytes;
		}

		return bytes;
	}

	inline void reset()
	{
		pcur = pbuf_start;
		data_sz = 0;
	}

	inline void rewind()
	{
		pcur = pbuf_start;
	}
private:
	size_t capacity;
	byte *pbuf_start;////char *pbuf_start
	byte *pcur;  ///char *pcur
	size_t data_sz;
};
