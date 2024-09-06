#ifndef _MIGR_ADT_H_
#define	_MIGR_ADT_H_

#include <sys/param.h>
#include <sys/isi_enc.h>
#include <sys/isi_vector.h>

ISI_VECTOR_DECLARE(enc_vec, enc_t, encs, 2, enc_vec);
ISI_VECTOR_DECLARE_FMT(enc_vec, enc_vec);
#define	ENC_VEC_FOREACH		VECTOR_FOREACH
#define	ENC_VEC_INITIALIZER	VECTOR_INITIALIZER
#define	ENC_VEC_INIT_CLEAN(v)	v __cleanup(enc_vec_clean) = {}

#endif /* _MIGR_ADT_H_ */
