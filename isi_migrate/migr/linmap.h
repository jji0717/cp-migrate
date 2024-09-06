#ifndef LINMAP_H
#define LINMAP_H
#include "compliance_map.h"

/*
 * Going above MAX_COMP_MAP_LOOKUP_LOOP when doing
 * comp map lookup chain suggests a valid bug.
 */
#define MAX_COMP_MAP_LOOKUP_LOOP	10000

struct map_ctx {
	int map_fd;
	bool dynamic;
	ifs_domainid_t domain_id;
	/*
	 * We are not responsible to close the
	 * comp map context.
	 */
	struct compliance_map_ctx *comp_map_ctx;
};

struct map_iter {
	struct map_ctx *ctx;
	btree_key_t key;
};

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */
enum lmap_oper {
	LMAP_SET = 1,
	LMAP_REM
};

struct lmap_log_entry {
	uint64_t slin;
	uint64_t dlin;
	enum lmap_oper oper;
};

struct lmap_log {
	int index;
	struct lmap_log_entry entries[MAX_BLK_OPS];
};

void create_linmap(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);
void open_linmap(const char *policy, char *name, struct map_ctx **ctx_out,
    struct isi_error **error_out);
bool linmap_exists(const char *policy, char *name,
    struct isi_error **error_out);
void close_linmap(struct map_ctx *);
void remove_linmap(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

void set_mapping(uint64_t, uint64_t, struct map_ctx *, struct isi_error **);
bool set_mapping_cond(uint64_t, uint64_t, struct map_ctx *, bool, uint64_t,
    struct isi_error **);
bool get_mapping(uint64_t, uint64_t *, struct map_ctx *, struct isi_error **);
bool get_mapping_with_domain_info(uint64_t, uint64_t *, struct map_ctx *,
    bool *, struct isi_error **);
bool remove_mapping(uint64_t, struct map_ctx *, struct isi_error **);
bool mapping_exists(uint64_t, struct map_ctx *, struct isi_error **);
bool linmap_empty(struct map_ctx *, struct isi_error **);

struct map_iter *new_map_iter(struct map_ctx *);
bool map_iter_next(struct map_iter *, uint64_t *, uint64_t *,
    struct isi_error **);
void close_map_iter(struct map_iter *);

void
flush_lmap_log(struct lmap_log *logp, struct map_ctx *ctx,
    struct isi_error **error_out);
void
set_dynamic_lmap_lookup(struct map_ctx *ctx, ifs_domainid_t domain_id);
void
get_lmap_name(char *buf, const char *policy, bool restore);
void
get_mirrored_lmap_name(char *buf, const char *policy);
void
get_tmp_working_map_name(char *buf, const char *policy);
void
lmap_log_add_entry(struct map_ctx *ctx, struct lmap_log *logp, ifs_lin_t slin,
    ifs_lin_t dlin, enum lmap_oper oper, int source_fd, 
    struct isi_error **error_out);
void
set_comp_map_lookup(struct map_ctx *ctx,
    struct compliance_map_ctx *comp_map_ctx);

#endif
