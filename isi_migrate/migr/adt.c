#include <sys/param.h>
#include <sys/isi_enc.h>
#include <sys/isi_format.h>
#include <sys/isi_vector.h>

#include <string.h>

#include "adt.h"

static int
enc_cmp_void(const void *_1, const void *_2)
{
	const enc_t *e1 = _1;
	const enc_t *e2 = _2;

	return (int)*e1 - (int)*e2;
}

ISI_VECTOR_DEFINE(enc_vec, enc_vec, enc_cmp_void);
ISI_VECTOR_DEFINE_FMT(enc_vec, enc_vec, int_fmt);
