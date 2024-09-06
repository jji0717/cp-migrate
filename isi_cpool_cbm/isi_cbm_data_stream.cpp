#include <isi_cpool_cbm/isi_cbm_data_stream.h>

#include <unistd.h>
#include <isi_ilog/ilog.h>

using namespace isi_cloud;

size_t
databuffer_ostream::write(void *buf, size_t len)
{
	if (bytes_write_ >= buf_len_) {
		ilog(IL_ERR, "Received more data, but cannot accept it as buf full");
		return len;// received more data, but cannot accept it
	}

	if (len + bytes_write_ >= buf_len_)
		len = buf_len_ - bytes_write_;
	memcpy(buffer_ + bytes_write_, buf, len);
	bytes_write_ += len;
	return len;
}

bool
databuffer_ostream::reset()
{
	// so to write from the beginning
	bytes_write_ = 0;

	return true;
}

// size_t /////data_ostream用不到,不用关心
// data_ostream::write(void *buf, size_t len)
// {
// 	char *buffer = static_cast<char *>(buf);
// 	ASSERT(buffer != NULL);

// 	size_t data_written = 0;
// 	size_t data_left_to_write = len;
// 	ssize_t data_written_this_pass = 0;

// 	while (data_written < len) {
// 		data_written_this_pass = ::write(fd_, &buffer[data_written],
// 		    data_left_to_write);

// 		if (data_written_this_pass == 0)
// 			break;

// 		if (data_written_this_pass < 0) {
// 			data_written = -1;
// 			break;
// 		}

// 		data_written += data_written_this_pass;
// 		data_left_to_write -= data_written_this_pass;
// 	}

// 	return data_written;
// }
