#ifndef __RESUME_TOKEN_H__
#define __RESUME_TOKEN_H__

#include <sstream>
#include <string>

#include "api_error.h"

/** Wrapper class used to force serialization of pod types via
 * direct route rather than string serialization.
 *
 * See @ref resume_input_token::operator>>
 * and @ref resume_output_token::operator<<
 */
template<typename T>
struct resume_token_blob {
	resume_token_blob(T &t) : t_(&t) { }

	T *t_;
};

/** Convenience function for creating pod wrapper. */
template<typename T>
resume_token_blob<T>
resume_blob(T &t)
{
	return resume_token_blob<T>(t);
}

/** Resume token decoder. */
struct resume_input_token {
	/** Construct from user input string.
	 *
	 * Input string is borrowed so should not be modified while
	 * decoding calls are being made.
	 *
	 * @param str input string containing encoded token
	 * @param ver encoder/decoder version
	 */
	resume_input_token(const std::string &str, unsigned ver);

	/** Decode next item into string @a v */
	resume_input_token &operator>>(std::string &v);

	/** Decode next item into a char * @a v, freeing any existing value */
	resume_input_token &operator>>(char * &v);

	/** Decode next item into binary pod object @a v wrapping type @a T */
	template<typename T>
	inline resume_input_token &operator>>(resume_token_blob<T> v);

	/** Decode next item into streamable variable @a v of type @a T */
	template<typename T>
	inline resume_input_token &operator>>(T &v);

private:
	size_t decode_len();
	void decode_next(std::string &out);
	void decode_next(char *out, size_t len);

	const char  *cur_;
	const char  *end_;
	std::string  buf_;
};

/** Resume token encoder. */
struct resume_output_token {
	/** Construct for token creation.
	 *
	 * @param ver encoder/decoder version
	 */
	explicit resume_output_token(unsigned ver);

	/** Retrieve encoded string for sending to user */
	inline const std::string &str() const;

	/** Add encoded item for string @a v */
	resume_output_token &operator<<(const std::string &v);

	/** Add encoded item for binary POD object @a v wrapping type @a T */
	template<typename T>
	inline resume_output_token &operator<<(const resume_token_blob<T> v);

	/** Add encoded item for streamable variable @a v of type @a T */
	template<typename T>
	inline resume_output_token &operator<<(const T &v);

	/** Reset the output token */
	inline void reset();

private:
	void encode_len(size_t sz);
	void encode_data(const void *d, size_t sz);

	std::string out_;
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

template<typename T>
resume_input_token &
resume_input_token::operator>>(resume_token_blob<T> t)
{
	decode_next(reinterpret_cast<char *>(t.t_), sizeof(T));
	return *this;
}

template<typename T>
resume_input_token &
resume_input_token::operator>>(T &t)
{
	decode_next(buf_);
	std::istringstream iss(buf_);
	if (buf_.empty())
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");
	iss >> t;
	if (iss.fail() || !iss.eof())
		throw api_exception(AEC_BAD_REQUEST, "Invalid resume token");
	return *this;
}

const std::string &
resume_output_token::str() const
{
	return out_;
}

template<typename T>
resume_output_token &
resume_output_token::operator<<(const resume_token_blob<T> t)
{
	encode_data(static_cast<const void *>(t.t_), sizeof(T));
	return *this;
}

template<typename T>
resume_output_token &
resume_output_token::operator<<(const T &t)
{
	std::ostringstream oss;
	oss << t;
	return operator<<(oss.str());
}

void
resume_output_token::reset()
{
	out_.clear();
}

#endif
