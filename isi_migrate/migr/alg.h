#ifndef __ALG_H__
#define __ALG_H__

struct hash_ctx {
	void *hash_alg;
	enum hash_type type;
};

struct hash_ctx *hash_alloc(enum hash_type type);
void hash_init(struct hash_ctx *ctx);
void hash_update(struct hash_ctx *ctx, const void *data, unsigned int len);
char *hash_end(struct hash_ctx *ctx);
void hash_free(struct hash_ctx *ctx);

#endif // __ALG_H__
