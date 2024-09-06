
#ifndef __ENCRYPT_CTX_H__
#define __ENCRYPT_CTX_H__

#include <ifs/ifs_types.h>
#include <isi_cpool_security/cpool_protect.h>

typedef struct {
	char t_[MEK_ID_SIZE];
} mek_id_t;

typedef struct {
	char t_[MAX_DEK_SIZE];
} dek_t;

typedef struct {
	char t_[MAX_IV_SIZE];
} iv_t;

typedef struct encrypt_ctx
{
	encrypt_ctx();

	void initialize(struct isi_error **error_out);

	void initialize(const mek_id_t &mek_id,
	    const dek_t &edek,const iv_t &master_iv,
	    struct isi_error **error_out);

	inline void set_chunk_idx(int idx)
	{
		this->chunk_idx = idx;
	}

	inline iv_t get_chunk_iv()
	{
		iv_t ret = master_iv;

		cpool_get_dek_child_iv_from_master_iv(
		    chunk_idx,
		    (unsigned char *) &master_iv,
		    (unsigned char *) &ret);

		return ret;
	}

	mek_id_t mek_id;
	dek_t    dek;
	iv_t     master_iv;
	dek_t    edek;

	int      chunk_idx;
} encrypt_ctx, *encrypt_ctx_p;

#endif
