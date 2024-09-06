#ifndef SUMM_STF_H
#define SUMM_STF_H

#include <isi_util/isi_error.h>

__BEGIN_DECLS

#define LINS_IN_PASS	10000
#define DIRBLOCKS       5

#define CHANGELIST_SUMM_DIR "changelist"

#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */
struct stf_log {
	int index;
	uint64_t lin[MAX_BLK_OPS];
};

#define STF_COUNT 20	/* Number of STFs considered in each iteration */
#define LIN_STF_CNT 50	/* Number of lins read from each stf at any time */
struct stf_entry {
	int fd;
	ifs_lin_t *lins;
	int lin_cnt;
};

struct summ_stf_ctx {
	/* fd to hold summary btree */
	int summ_stf_fd;
	/* store list of snapids between two synciq snapshots */
	struct snapid_set *snapset;
	/* Summary STF log */
	struct stf_log stf_log;
	/* Array holding stf fd and its lins */
	struct stf_entry stfs[STF_COUNT];
	/* count of stfs in the STFs array */
	int stfs_cnt;
	/* last lin returned from stf array block */
	ifs_lin_t prev_lin;
	/* Minimum lin value to be returned  to caller*/
	ifs_lin_t min_lin;
	/* Maximum lin value that could be returned to caller */
	ifs_lin_t max_lin;
};

struct summ_stf_iter {
	/* Pointer to summ_stf context */
	struct summ_stf_ctx *ctx;
	btree_key_t key;
};

struct summ_stf_ctx *
summ_stf_ctx_new(ifs_snapid_t snap1, ifs_snapid_t snap2, 
    struct isi_error **error_out);
int
get_stf_next_lins(struct summ_stf_ctx *summ_ctx, ifs_lin_t *lins,
    int entries, struct isi_error **error_out);
void
set_stf_entry(u_int64_t lin, struct summ_stf_ctx *ctx,
    struct isi_error **error_out);
bool
remove_stf_entry(u_int64_t lin, struct summ_stf_ctx *ctx, 
    struct isi_error **error_out);
int
open_stf(ifs_snapid_t snapid, struct isi_error **error_out);
void
summ_stf_free(struct summ_stf_ctx *summ_ctx);
void
reset_summ_stf_iter(struct summ_stf_ctx *summ_ctx);
int
get_summ_stf_next_lins(struct summ_stf_iter *iter, ifs_lin_t *lins,
    int entries, struct isi_error **error_out);
void
open_summary_stf(const char *policy, char *name, struct summ_stf_ctx **ctx,
    struct isi_error **error_out);
struct summ_stf_ctx *
summ_stf_alloc(void);
bool
get_summ_stf_at_loc(unsigned num, unsigned den, struct summ_stf_ctx *summ_ctx,
    uint64_t *lin, struct isi_error **error_out);
ifs_lin_t
get_summ_stf_max_lin(struct summ_stf_ctx *stf_ctx,
		struct isi_error **error_out);
struct summ_stf_iter *
new_summ_stf_iter(struct summ_stf_ctx *summ_ctx, ifs_lin_t resume_lin);
void
close_summ_stf_iter(struct summ_stf_iter *iter);
int
stf_iter_next_lins(struct summ_stf_ctx *ctx, ifs_snapid_t snap1,
    ifs_snapid_t snap2, ifs_lin_t *lins, int count,
    struct isi_error **error_out);
void
stf_iter_status(struct summ_stf_ctx *ctx, ifs_lin_t *prev_lin);
void
get_summ_stf_name(char *buf, const char *policy, ifs_snapid_t snap,
    bool restore);
void
get_changelist_summ_stf_name(char *buf, ifs_snapid_t snap1,
    ifs_snapid_t snap2);
void
get_summ_stf_pattern(char *buf, const char *job_name, bool restore);
void
prune_old_summary_stf(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out);
void
create_summary_stf(const char *policy, char *name, bool exist_ok, struct isi_error **error_out);
void
flush_summ_stf_log(struct summ_stf_ctx *ctx, struct isi_error **error_out);
bool
summ_stf_log_entries(struct summ_stf_ctx *ctx);
bool
summ_stf_log_minimum(struct summ_stf_ctx *ctx, ifs_lin_t *min_lin);
void
snapset_min_max( ifs_snapid_t snap1, ifs_snapid_t snap2, ifs_lin_t *min,
    ifs_lin_t *max, struct isi_error **error_out);
void
set_stf_lin_range(struct summ_stf_ctx *ctx, ifs_lin_t min_lin, ifs_lin_t max_lin);
bool summ_stf_entry_exists(struct summ_stf_ctx *summ_ctx,
    uint64_t lin, struct isi_error **error_out);
void
snapset_alloc(struct summ_stf_ctx *ctx, ifs_snapid_t snap1,
    ifs_snapid_t snap2, struct isi_error **error_out);

__END_DECLS

#endif
