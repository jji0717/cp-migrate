#include <string.h>

#include "isi_cbm_pack_unpack.h"

void
pack(void **dst, const void *src, int size)
{
	memcpy(*dst, src, size);
	*(char **)dst += size;
}

void
unpack(void **src, void *dst, int size)
{
	memcpy(dst, *src, size);
	*(char **)src += size;
}
