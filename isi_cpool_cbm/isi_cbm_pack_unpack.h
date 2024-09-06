#ifndef __ISI_CBM_PACK_UNPACK_H__
#define __ISI_CBM_PACK_UNPACK_H__

void
pack(void **dst, const void *src, int size);

void
unpack(void **src, void *dst, int size);

#endif // __ISI_CBM_PACK_UNPACK_H__
