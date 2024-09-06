#ifndef __ISI_CBM_DATA_STREAM_H__
#define  __ISI_CBM_DATA_STREAM_H__

#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h>

/**
 * output data stream based on memory buffer
 */
////在recall时用到,从cloud拿回数据,把数据memcpy到vec,然后恢复成regular file
class databuffer_ostream : public isi_cloud::odata_stream 
{
public:
	databuffer_ostream(void *buffer, size_t buf_len)
		: buffer_((char *)buffer), buf_len_(buf_len),
		  bytes_write_(0) {}

	void reset(void *buffer, size_t buf_len) {
		buffer_      = (char *)(buffer);
		buf_len_     = buf_len;
		bytes_write_ = 0;
	}

	/** see odata_stream::write() */
	virtual size_t write(void *buf, size_t len);

	/** see odata_stream::reset() */
	virtual bool reset();

private:
	char *buffer_;
	size_t buf_len_;
	size_t bytes_write_;

};

/**
 * output data stream based on file descriptor
 */
// class data_ostream : public isi_cloud::odata_stream  /////用不到,不用考虑
// {
// public:

// 	data_ostream(int fd) : fd_(fd) { }
// 	data_ostream() : fd_(-1) {}

// 	void setfd(int fd) { fd_ = fd; }

// 	virtual size_t write(void *buf, size_t len);

// 	virtual bool reset() { return false; }

// private:
// 	int fd_;
// };

#endif /*  __ISI_CBM_DATA_STREAM_H__ */
