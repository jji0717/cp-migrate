#ifndef INC_BASE64ENC_H__
#define INC_BASE64ENC_H__

#include <string>

#include <isi_rest_server/api_error.h>

/** Return length of string needed to encode an object of @a size bytes. */
static inline unsigned
base64_encode_length(unsigned size)
{
	return 4 * ((size + 3 - 1) / 3);
}

/** Encode an object.
 *
 * @param output start of encoded string
 * @param input  binary input object
 * @param size   length in bytes of @a input
 *
 * @pre output has space for base64_encode_length(@a size) bytes.
 *
 * @return one past the end of the encoded object in @a output
 *
 */
char *
base64_encode(char *output, const void *input, unsigned size);

/** Decode an object.
 *
 * @param output decoded data
 * @param input  encoded string
 * @param size   length of decoded data (input must be longer)
 *
 * @return one past the end of the encoded object in @a input
 */
const char *
base64_decode(void *output, const char *input, unsigned size);

template<typename T>
inline std::string
base64_encode(const T &t);

/** Decode into object checking decoded size against input size.
 *
 * @param output object to decode into
 * @param input  encoded data
 * @param res    if true, context is resource lookup (controls exception type)
 *
 * @throw api_exception AEC_NOT_FOUND or AEC_BAD_REQUEST depending on @a res.
 */
template<typename T>
inline void
base64_decode_obj(T &output, const std::string &input, bool res);

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

template<typename T>
std::string
base64_encode(const T &t)
{
	std::string ret;
	ret.resize(base64_encode_length(sizeof t));
	base64_encode(&ret[0], &t, sizeof t);
	return ret;
}

template<typename T>
void
base64_decode_obj(T &output, const std::string &input, bool res)
{
	if (input.size() != base64_encode_length(sizeof output))
		throw api_exception(res ? AEC_NOT_FOUND : AEC_BAD_REQUEST,
		    "Invalid encoded object: '%s'", input.c_str());
	base64_decode(&output, &input[0], sizeof output);
}

#endif
