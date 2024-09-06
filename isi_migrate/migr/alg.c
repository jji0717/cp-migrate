#include <sys/types.h>
#include <md4.h>
#include <md5.h>
#include <stdlib.h>
#include "isirep.h"
#include "alg.h"

struct hash_ctx *
hash_alloc(enum hash_type type)
{
	MD4_CTX *md4_ctx;
	MD5_CTX *md5_ctx;
	struct hash_ctx *ctx;
	
	ctx = calloc(1, sizeof(struct hash_ctx));
	ASSERT(ctx);
	ctx->type = type;
	switch (type) {
	case HASH_MD4:
		md4_ctx = calloc(1, sizeof(MD4_CTX));
		ASSERT(md4_ctx);
		ctx->hash_alg = (void *) md4_ctx;
		return ctx;
	case HASH_MD5:
		md5_ctx = calloc(1, sizeof(MD5_CTX));
		ASSERT(md5_ctx);
		ctx->hash_alg = (void *) md5_ctx;
		return ctx;
	default:
		log(ERROR, "Unknown hash type of %d", type);
		abort();
	}
}

void
hash_free(struct hash_ctx *ctx)
{
	if (!ctx)
		return;

	if (ctx->hash_alg)
		free(ctx->hash_alg);

	free(ctx);
}

void
hash_init(struct hash_ctx *ctx)
{
	switch (ctx->type) {
	case HASH_MD4:
		MD4Init((MD4_CTX *)ctx->hash_alg);
		break;
	case HASH_MD5:
		MD5Init((MD5_CTX *)ctx->hash_alg);
		break;
	default:
		log(ERROR, "Unknown hash type of %d", ctx->type);
		abort();
	}
}

void
hash_update(struct hash_ctx *ctx, const void *data, unsigned int len)
{
	switch (ctx->type) {
	case HASH_MD4:
		MD4Update((MD4_CTX *)ctx->hash_alg, data, len);
		break;
	case HASH_MD5:
		MD5Update((MD5_CTX *)ctx->hash_alg, data, len);
		break;
	default:
		log(ERROR, "Unknown hash type of %d", ctx->type);
		abort();
	}
}

char *
hash_end(struct hash_ctx *ctx)
{
	char *out;

	switch (ctx->type) {
	case HASH_MD4:
		out = MD4End((MD4_CTX *)ctx->hash_alg, NULL);
		break;
	case HASH_MD5:
		out = MD5End((MD5_CTX *)ctx->hash_alg, NULL);
		break;
	default:
		log(ERROR, "Unknown hash type of %d", ctx->type);
		abort();
	}
	/* String is allocated in call to end function; must be freed later.*/
	return out;
}
