#ifndef WORKLIST_H
#define WORKLIST_H

struct wl_ctx {
	int wl_fd;
};

struct wl_iter {
	struct wl_ctx *ctx;
	uint64_t higher;
	btree_key_t key;		//Next key
	btree_key_t current_key;	//Current key
	bool has_last_entry;
};

struct wl_log_entry {
	uint64_t lin;
	enum sbt_bulk_op_type operation;
};

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */
struct wl_log {
	int index;
	struct wl_log_entry entries[MAX_BLK_OPS];
};

struct wl_entry {
	/* Snap1 version dirent cookie */
	uint64_t dircookie1;
	/* Snap2 version dirent cookie */
	uint64_t dircookie2;
};

void
prune_old_worklist(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out);

void
create_worklist(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);

void
open_worklist(const char *policy, char *name, struct wl_ctx **ctx,
    struct isi_error **error_out);

void
close_worklist(struct wl_ctx *ctx);

void
remove_worklist(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

void
wl_move_entries(struct wl_ctx *ctx, btree_key_t *key, uint64_t count, 
    uint64_t src_wlid, uint64_t tgt_wlid, struct isi_error **error_out);

void
free_worklist(struct wl_ctx *ctx);

uint64_t
get_wi_lin_from_btree_key(btree_key_t *key);

unsigned int
get_level_from_btree_key(btree_key_t *key);

ifs_lin_t
get_lin_from_btree_key(btree_key_t *key);

void
set_wl_key( btree_key_t *key, uint64_t wi_lin, unsigned int cur_level,
    ifs_lin_t lin);

struct wl_iter *
new_wl_iter(struct wl_ctx *ctx, btree_key_t *key);

void close_wl_iter(struct wl_iter *iter);

int
raw_wl_checkpoint(int wl_fd, unsigned int wi_lin, unsigned int level,
    ifs_lin_t lin, uint64_t dircookie1, uint64_t dircookie2);

int
wl_checkpoint(struct wl_log *logp, struct wl_iter *iter,
    uint64_t dircookie1, uint64_t dircookie2, struct isi_error **error_out);

bool
wl_iter_raw_next(struct wl_log *logp, struct wl_iter *iter, uint64_t *lin,
    unsigned int *level, struct wl_entry **entry, struct isi_error **error_out);

bool
wl_iter_next(struct wl_log *logp, struct wl_iter *iter, uint64_t *lin,
    unsigned int *level, struct wl_entry **entry, struct isi_error **error_out);
void
get_worklist_name(char *buf, const char *policy, ifs_snapid_t snap,
    bool restore);
void
get_worklist_pattern(char *buf, const char *policy, bool restore);
void
flush_wl_log(struct wl_log *logp, struct wl_ctx *wl, uint64_t wi_lin,
    unsigned int level, struct isi_error **error_out);
void
wl_log_add_entry_1(struct wl_log *logp, struct wl_ctx *wl, uint64_t wi_lin,
    unsigned int level, ifs_lin_t lin, struct isi_error **error_out);
void
wl_log_del_entry(struct wl_log *logp, struct wl_ctx *wl,
    btree_key_t key, struct isi_error **error_out);
void
wl_remove_entries (struct wl_ctx *wl, uint64_t wi_lin, int level,
    struct isi_error **error_out);
bool
wl_log_entries(struct wl_log *logp);
void
wl_remove_entries_parent_range(struct wl_ctx *wl, uint64_t wi_lin, int level,
    ifs_snapid_t snap, uint64_t range1, uint64_t range2,
    struct isi_error **error_out);
bool
wl_iter_has_entries(struct wl_iter *iter);

#endif
