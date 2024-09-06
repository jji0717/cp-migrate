#include "base64enc.h"
#include "resume_token.h"

#define to_digit(c)	((c) - '0')
#define is_digit(c)	((unsigned)to_digit(c) <= 9)
#define to_char(n)	((n) + '0')

namespace {

const char LEN_TERMINATOR = '-';

}

resume_input_token::resume_input_token(const std::string &str, unsigned vers)
	: cur_(&str[0]), end_(cur_ + str.size())
{
	if (decode_len() != vers)
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");
}

resume_input_token &
resume_input_token::operator>>(std::string &out)
{
	decode_next(out);
	return *this;
}

resume_input_token &
resume_input_token::operator>>(char * &out)
{
	std::string tmp;
	decode_next(tmp);

	char *new_str = strdup(tmp.c_str());
	std::swap(new_str, out);

	if (new_str) {
		free(new_str);
	}

	return *this;
}

size_t
resume_input_token::decode_len()
{
	size_t size = 0;

	for ( ; cur_ < end_ && is_digit(*cur_); ++cur_) {
		size *= 10;
		size += to_digit(*cur_);
	}

	if (*cur_++ != LEN_TERMINATOR)
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");

	return size;
}

void
resume_input_token::decode_next(std::string &out)
{
	size_t size = decode_len();

	if (size == 0) {
		out.clear();
		return;
	}

	if (size_t(end_ - cur_) < ((size + 3) & ~3))
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");

	out.resize(size);
	cur_ = base64_decode(&out[0], cur_, size);
}

void
resume_input_token::decode_next(char *out, size_t len)
{
	size_t size = decode_len();

	if (size != len)
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");

	if (size_t(end_ - cur_) < ((size + 3) & ~3))
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");

	cur_ = base64_decode(out, cur_, size);
}

resume_output_token::resume_output_token(unsigned version)
{
	encode_len(version);
}

resume_output_token &
resume_output_token::operator<<(const std::string &str)
{
	encode_data(str.data(), str.size());
	return *this;
}

void
resume_output_token::encode_len(size_t size)
{
	char buf[32];
	sprintf(buf, "%llu", size);
	out_ += buf;
	out_ += LEN_TERMINATOR;
}

void
resume_output_token::encode_data(const void *data, size_t size)
{
	encode_len(size);

	size_t out_len = out_.size();
	out_.resize(out_len + base64_encode_length(size));
	base64_encode(&out_[out_len], data, size);
}
