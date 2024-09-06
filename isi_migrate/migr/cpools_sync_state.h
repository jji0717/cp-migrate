#ifndef CPOOLS_SYNC_STATE_H
#define CPOOLS_SYNC_STATE_H
#include <isi_cpool_bkup/isi_cpool_bkup_sync.h>

#include "siq_btree.h"

__BEGIN_DECLS

#define CPSE_VER_MAX 1

struct cpss_entry_v1_ondisk {
	uint16_t ver;
	uint8_t sync_type;
} __packed;

CTASSERT(sizeof(struct cpss_entry_v1_ondisk) == 3);

#define SYNCSTATE_BUF_SIZE	sizeof(struct cpss_entry_v1_ondisk)

struct cpss_entry {
	enum isi_cbm_backup_sync_type sync_type;
	bool del_entry;
} __packed;

CTASSERT(sizeof(struct cpss_entry) == 5);

struct cpss_ctx {
	int cpss_fd;
};

struct cpss_iter {
	struct cpss_ctx *ctx;
	btree_key_t key;
};

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */

struct cpss_log_entry {
	uint64_t lin;
	bool is_new; /* Is it new in differential cpools sync state */
	struct cpss_entry old_entry;
	struct cpss_entry new_entry;
};

struct cpss_log {
	int index;
	struct cpss_log_entry entries[MAX_BLK_OPS];
};


void create_cpss(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);
void open_cpss(const char *policy, char *name, struct cpss_ctx **ctx_out,
    struct isi_error **error_out);
void close_cpss(struct cpss_ctx *);
void remove_cpss(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

void set_cpss_entry(u_int64_t, struct cpss_entry *, struct cpss_ctx *,
    struct isi_error **error_out);
bool
set_cpss_entry_cond(u_int64_t lin, struct cpss_entry *, struct cpss_ctx *ctx,
    bool is_new, struct cpss_entry *old_cpss, struct isi_error **error_out);

bool get_cpss_entry(u_int64_t lin, struct cpss_entry *, struct cpss_ctx *ctx,
    struct isi_error **error_out);
bool
get_cpsskey_at_loc(unsigned num, unsigned den, struct cpss_ctx *ctx,
    uint64_t *lin, struct isi_error **error_out);
ifs_lin_t
get_cpss_max_lin(struct cpss_ctx *ctx, struct isi_error **error_out);

bool remove_cpss_entry(u_int64_t, struct cpss_ctx *, struct isi_error **);
bool cpss_entry_exists(u_int64_t, struct cpss_ctx *, struct isi_error **error_out);

struct cpss_iter *new_cpss_iter(struct cpss_ctx *);
struct cpss_iter *new_cpss_range_iter(struct cpss_ctx *, uint64_t);
bool cpss_iter_next(struct cpss_iter *, u_int64_t *, struct cpss_entry *,
    struct isi_error **);
void close_cpss_iter(struct cpss_iter *);
void get_base_cpss_name(char *buf, const char *policy, bool restore);
void
get_repeated_cpss_name(char *buf, const char *policy, uint64_t snapid,
    bool restore);
void
get_repeated_cpss_pattern(char *buf, const char *policy, bool restore);

char *
get_valid_cpss_path( uint64_t lin, ifs_snapid_t snap);

bool
cpss_exists(const char *policy, char *name, struct isi_error **error_out);
void
add_cpss_bulk_entry(struct cpss_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, bool is_new, struct cpss_entry *cpse,
    char *sbt_buf, struct btree_flags *sbt_flags,
    struct cpss_entry *old_cpse, char *sbt_old_buf,
    struct btree_flags *sbt_old_flags,
    const size_t sbt_buf_size);

bool
sync_cpss_entry(struct cpss_ctx *ctx, ifs_lin_t lin, struct cpss_entry *newp,
    struct isi_error **error_out);
void
flush_cpss_log(struct cpss_log *logp, struct cpss_ctx *ctx,
    struct isi_error **error_out);
void
copy_cpss_entries(struct cpss_ctx *ctx, ifs_lin_t *lins,
    struct cpss_entry *entries, int num_entries, struct isi_error **error_out);

void prune_old_cpss(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out);
void
get_mirrored_cpss_name(char *buf, const char *policy);
void
get_changelist_cpss_name(char *buf, ifs_snapid_t old_snapid,
    ifs_snapid_t new_snapid);
bool
decomp_changelist_cpss_name(const char *name, ifs_snapid_t *sid_old_out,
    ifs_snapid_t *sid_new_out);

__END_DECLS

#endif // defined CPOOLS_SYNC_STATE_H
