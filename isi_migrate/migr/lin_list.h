#ifndef LIN_LIST_H
#define LIN_LIST_H
#include "work_item.h"
#include "siq_btree.h"
#include <isi_domain/dom.h>

#define LIN_LIST_INFO_LIN 0x1
#define LIN_LIST_NAME_WORM_COMMIT "worm_commit"
#define LIN_LIST_NAME_DIR_DELS "worm_dir_dels"

__BEGIN_DECLS

struct lin_list_entry {
	bool is_dir : 1;
	bool commit : 1;  //Reused in dir_dels as done
};

enum lin_list_type {
	RESYNC = 0,
	COMMIT,
	DIR_DELS,
	MAX_LIN_LIST
};

struct lin_list_info_entry {
	enum lin_list_type type;
};

struct lin_list_ctx {
	int lin_list_fd;
};

struct lin_list_iter {
	struct lin_list_ctx *ctx;
	btree_key_t key;
};

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */

struct lin_list_log_entry {
	uint64_t lin;
	bool is_new; /* Is it new in lin_list */
	struct lin_list_entry old_ent;
	struct lin_list_entry new_ent;
};

struct lin_list_log {
	int index;
	struct lin_list_log_entry entries[MAX_BLK_OPS];
};


void
create_lin_list(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);

void
open_lin_list(const char *policy, char *name,
    struct lin_list_ctx **ctx_out, struct isi_error **error_out);

void
close_lin_list(struct lin_list_ctx *ctx);

void
remove_lin_list(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

void
set_lin_list_entry(u_int64_t lin, struct lin_list_entry ent,
    struct lin_list_ctx *ctx, struct isi_error **error_out);

bool
get_lin_list_entry(u_int64_t lin, struct lin_list_entry *ent,
    struct lin_list_ctx *ctx, struct isi_error **error_out);

bool
get_lin_listkey_at_loc(unsigned num, unsigned den,
    struct lin_list_ctx *ctx, uint64_t *lin, struct isi_error **error_out);

ifs_lin_t
get_lin_list_max_lin(struct lin_list_ctx *ctx,
    struct isi_error **error_out);

ifs_lin_t
get_lin_list_min_lin(struct lin_list_ctx *ctx,
    struct isi_error **error_out);

bool
get_lin_list_lin_in_range(struct lin_list_ctx *ctx, ifs_lin_t min_lin,
    ifs_lin_t max_lin, struct isi_error **error_out);

bool
remove_lin_list_entry(u_int64_t lin, struct lin_list_ctx *ctx,
    struct isi_error **error_out);

bool
lin_list_entry_exists(u_int64_t lin, struct lin_list_ctx *ctx,
    struct isi_error **error_out);

struct lin_list_iter *
new_lin_list_iter(struct lin_list_ctx *ctx);

struct lin_list_iter *
new_lin_list_range_iter(struct lin_list_ctx *ctx, uint64_t lin);

bool
lin_list_iter_next(struct lin_list_iter *iter, u_int64_t *lin,
    struct lin_list_entry *ent, struct isi_error **error_out);

void
close_lin_list_iter(struct lin_list_iter *iter);

void
get_resync_name(char *buf, size_t buf_len, const char *policy, bool src);

void
get_worm_commit_name(char *buf, size_t buf_len, const char *policy);

char *
get_valid_lin_list_path(uint64_t lin, ifs_snapid_t snap);

bool
lin_list_exists(const char *policy, char *name,
    struct isi_error **error_out);

void
add_lin_list_bulk_entry(struct lin_list_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, bool is_new, struct lin_list_entry new_ent,
    struct lin_list_entry old_ent);

void
flush_lin_list_log(struct lin_list_log *logp, struct lin_list_ctx *ctx,
    struct isi_error **error_out);

void
copy_lin_list_entries(struct lin_list_ctx *ctx, ifs_lin_t *lins,
    struct lin_list_entry *entries, int num_entries,
    struct isi_error **error_out);

void
lin_list_log_add_entry(struct lin_list_ctx *ctx, struct lin_list_log *logp,
    ifs_lin_t lin, bool is_new, struct lin_list_entry *new_ent,
    struct lin_list_entry *old_ent, struct isi_error **error_out);

void
prune_old_lin_list(const char *policy, char *name_root,
    struct isi_error **error_out);

bool
get_lin_list_info(struct lin_list_info_entry *llie, struct lin_list_ctx *ctx,
    struct isi_error **error_out);

void
set_lin_list_info(struct lin_list_info_entry *llie, struct lin_list_ctx *ctx,
    bool force, struct isi_error **error_out);

__END_DECLS

#endif
