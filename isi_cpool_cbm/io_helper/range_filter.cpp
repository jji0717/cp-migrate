
#include "range_filter.h"

range_filter::range_filter(odata_stream &out, size_t offset,
    size_t adj_off, size_t user_len) : out_(out), offset_(offset),
    adj_off_(adj_off), user_len_(user_len),
    cur_(adj_off_), written_(0)
{

}

size_t
range_filter::write(void *buff, size_t len)
{
	size_t to_skip = MIN((offset_ - cur_), len);
	char *buf_to_write = (char *)buff;
	buf_to_write += to_skip;
	size_t to_write = len - to_skip;
	to_write = MIN((user_len_ - written_), to_write);
	size_t written = 0;
	if (to_write > 0)
		written = out_.write(buf_to_write, to_write);

	written_ += written;
	cur_ += to_skip;
	if (written_ == user_len_) {
		// we are all set, tell the caller all written
		return len;
	}

	return (written + to_skip); // actually written + skipped
}

bool
range_filter::reset()
{
	cur_ = adj_off_;
	written_ = 0;

	return out_.reset();
}
