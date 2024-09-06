#ifndef __LIB__ISI_OBJECT__HASH__H_
#define __LIB__ISI_OBJECT__HASH__H_

#include <stdint.h>
#include <unistd.h>

/**
 * @param k		the key
 * @param len		the length of the key in bytes
 * @param initval	the previous hash, or an arbitrary value
 */
uint32_t iobj_hash32(const void *_k, size_t len, uint32_t initval);

#endif /* __LIB__ISI_OBJECT__HASH__H_ */
