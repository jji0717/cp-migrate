#include "base64enc.h"

namespace {

const char ENC_TABLE[65] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const char DEC_TABLE[256] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 15
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 31
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, // 47
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, // 63
	-1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, // 79
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, 63, // 95
	-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // 111
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, // 127
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 143
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 159
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 175
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 191
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 207
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 223
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 239
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 255
};

}


char *
base64_encode(char *o, const void *input, unsigned size)
{
	char        buf[3];
	const char *p;

	for (p = (const char *)input; size >= 3; size -= 3, p += 3) {
		*o++ = ENC_TABLE[ (p[0] & 0xfc) >> 2                        ];
		*o++ = ENC_TABLE[((p[0] & 0x03) << 4) + ((p[1] & 0xf0) >> 4)];
		*o++ = ENC_TABLE[((p[1] & 0x0f) << 2) + ((p[2] & 0xc0) >> 6)];
		*o++ = ENC_TABLE[  p[2] & 0x3f                              ];
	}

	if (size == 0)
		return o;

	buf[0] = *p;
	buf[1] = size == 2 ? *(p + 1) : '\0';
	buf[2] = '\0';

	p = buf;

	*o++ = ENC_TABLE[ (p[0] & 0xfc) >> 2                        ];
	*o++ = ENC_TABLE[((p[0] & 0x03) << 4) + ((p[1] & 0xf0) >> 4)];
	*o++ = ENC_TABLE[((p[1] & 0x0f) << 2) + ((p[2] & 0xc0) >> 6)];
	*o++ = ENC_TABLE[  p[2] & 0x3f                              ];

	return o;
}

const char *
base64_decode(void *output, const char *input, unsigned size)
{
	char buf[4];

	char *o = (char *)output;

	for ( ; size >= 3; size -= 3, input += 4) {
		buf[0] = DEC_TABLE[(unsigned)input[0]];
		buf[1] = DEC_TABLE[(unsigned)input[1]];
		buf[2] = DEC_TABLE[(unsigned)input[2]];
		buf[3] = DEC_TABLE[(unsigned)input[3]];

		*o++ = (char)( (buf[0] << 2) +        ((buf[1] & 0x30) >> 4));
		*o++ = (char)(((buf[1] & 0xf) << 4) + ((buf[2] & 0x3c) >> 2));
		*o++ = (char)(((buf[2] & 0x3) << 6) +   buf[3]);
	}

	if (size == 0)
		return input;

	buf[0] = DEC_TABLE[(unsigned)input[0]];
	buf[1] = DEC_TABLE[(unsigned)input[1]];
	buf[2] = DEC_TABLE[(unsigned)input[2]];
	buf[3] = DEC_TABLE[(unsigned)input[3]];

	if (size > 0)
		*o++ = (char)( (buf[0] << 2) +        ((buf[1] & 0x30) >> 4));
	if (size > 1)
		*o++ = (char)(((buf[1] & 0xf) << 4) + ((buf[2] & 0x3c) >> 2));

	input += 4;

	return input;
}
