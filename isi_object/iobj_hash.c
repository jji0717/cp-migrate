/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	iobj_hash.c __ 2009/07/17 13:57:52 PDT __ lwang2
 */
/** @file
 * @ingroup ostore
 */

/*
 * This hash function code is taken from Robert Jenkins. See
 * http://burtleburtle.net/bob/hash/evahash.html
 * It is implemented in sys/kern and copied here.
 */

#include "iobj_hash.h"

#include <stdlib.h>


/**
 * @param k		the key
 * @param len		the length of the key in bytes
 * @param initval	the previous hash, or an arbitrary value
 */
uint32_t
iobj_hash32(const void *_k, size_t len, uint32_t initval)
{
#define mix(a, b, c)				\
	do {					\
		a -= b; a -= c; a ^= (c >> 13);	\
		b -= c; b -= a; b ^= (a << 8);	\
		c -= a; c -= b; c ^= (b >> 13);	\
		a -= b; a -= c; a ^= (c >> 12);	\
		b -= c; b -= a; b ^= (a << 16);	\
		c -= a; c -= b; c ^= (b >> 5);	\
		a -= b; a -= c; a ^= (c >> 3);	\
		b -= c; b -= a; b ^= (a << 10);	\
		c -= a; c -= b; c ^= (b >> 15);	\
	} while (0)

	const uint8_t *k = _k;
	uint32_t a = 0x9e3779b9, b = 0x9e3779b9, c = initval;

	/* Hash chunks of 12 bytes. */
	while (len >= 12) {
		a += *(const uint32_t *)&k[0];
		b += *(const uint32_t *)&k[4];
		c += *(const uint32_t *)&k[8];
		mix(a, b, c);
		k += 12;
		len -= 12;
	}

	/* 
	 * Hash the remainder byte-by-byte, until it reaches a multiple of
	 * 4 bytes.
	 */
	switch (len) {
	case 11:
		c += (uint32_t)k[10] << 16;
	case 10:
		c += (uint32_t)k[9] << 8;
	case 9:
		c += (uint32_t)k[8];
	case 8:
		b += *(const uint32_t *)&k[4];
		a += *(const uint32_t *)&k[0];
		break;
	case 7:
		b += (uint32_t)k[6] << 16;
	case 6:
		b += (uint32_t)k[5] << 8;
	case 5:
		b += (uint32_t)k[4];
	case 4:
		a += *(const uint32_t *)&k[0];
		break;
	case 3:
		a += (uint32_t)k[2] << 16;
	case 2:
		a += (uint32_t)k[1] << 8;
	case 1:
		a += (uint32_t)k[0];
	}
	mix(a, b, c);

	return c;

#undef mix
}
