#ifndef COMPLIANCE_MAP_H
#define COMPLIANCE_MAP_H

struct compliance_map_ctx {
	int map_fd;
};

struct compliance_map_iter {
	struct compliance_map_ctx *ctx;
	btree_key_t key;
};

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */
enum compliance_map_oper {
	CMAP_SET = 1,
	CMAP_REM
};

struct compliance_map_log_entry {
	uint64_t old_lin;
	uint64_t new_lin;
	enum compliance_map_oper oper;
};

struct compliance_map_log {
	int index;
	struct compliance_map_log_entry entries[MAX_BLK_OPS];
};

void
create_compliance_map(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);

void
open_compliance_map(const char *policy, char *name,
    struct compliance_map_ctx **ctx_out, struct isi_error **error_out);

bool
compliance_map_exists(const char *policy, char *name,
    struct isi_error **error_out);

void
close_compliance_map(struct compliance_map_ctx *);

void
remove_compliance_map(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

bool
set_compliance_mapping(uint64_t old_lin, uint64_t new_lin,
    struct compliance_map_ctx *ctx, struct isi_error **error_out);

bool
get_compliance_mapping(uint64_t old_lin, uint64_t *new_lin,
    struct compliance_map_ctx *ctx, struct isi_error **error_out);

bool
remove_compliance_mapping(uint64_t old_lin, struct compliance_map_ctx *ctx,
    struct isi_error **error_out);

bool
compliance_mapping_exists(uint64_t old_lin, struct compliance_map_ctx *ctx,
    struct isi_error **error_out);

bool
compliance_map_empty(struct compliance_map_ctx *ctx,
    struct isi_error **error_out);

bool
get_compliance_mapkey_at_loc(unsigned num, unsigned den,
    struct compliance_map_ctx *ctx, uint64_t *lin,
    struct isi_error **error_out);

ifs_lin_t
get_compliance_map_max_lin(struct compliance_map_ctx *ctx,
    struct isi_error **error_out);

ifs_lin_t
get_compliance_map_min_lin(struct compliance_map_ctx *ctx,
    struct isi_error **error_out);

struct compliance_map_iter *
new_compliance_map_iter(struct compliance_map_ctx *);

struct compliance_map_iter *
new_compliance_map_range_iter(struct compliance_map_ctx *ctx, uint64_t lin);

bool
compliance_map_iter_next(struct compliance_map_iter *iter, uint64_t *old_lin,
    uint64_t *new_lin, struct isi_error **error_out);

void
close_compliance_map_iter(struct compliance_map_iter *iter);

bool
get_compliance_map_lin_in_range(struct compliance_map_ctx *ctx,
    ifs_lin_t min_lin, ifs_lin_t max_lin, struct isi_error **error_out);

void
flush_compliance_map_log(struct compliance_map_log *logp,
    struct compliance_map_ctx *ctx, struct isi_error **error_out);

void
get_compliance_map_name(char *buf, size_t buf_len, const char *policy,
    bool src);

void
compliance_map_log_add_entry(struct compliance_map_ctx *ctx,
    struct compliance_map_log *logp, ifs_lin_t old_lin, ifs_lin_t new_lin,
    enum compliance_map_oper oper, int source_fd,
    struct isi_error **error_out);

#endif
