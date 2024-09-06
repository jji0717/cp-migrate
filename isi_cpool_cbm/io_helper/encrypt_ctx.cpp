
#include "encrypt_ctx.h"

#include <isi_cpool_security/cpool_protect.h>

encrypt_ctx::encrypt_ctx()
    : chunk_idx(0)
{
}

void
encrypt_ctx::initialize(struct isi_error **error_out)
{
	cpool_cbm_get_keys_for_encryption(
	    (char *) &this->mek_id, 
	    (unsigned char *) &this->dek, 
	    (unsigned char *) &this->master_iv,
	    (unsigned char *) &this->edek,
	    error_out);

	chunk_idx = 0;
}

void
encrypt_ctx::initialize(const mek_id_t &mek_id,
    const dek_t &edek, const iv_t &master_iv,
    struct isi_error **error_out)
{
	this->mek_id = mek_id;
	this->edek = edek;
	this->master_iv = master_iv;

	cpool_get_keys_for_decryption(
	    (char *) &mek_id,
	    (unsigned char *) &edek,
	    (unsigned char *) &this->dek,
	    error_out);

	chunk_idx = 0;
}
