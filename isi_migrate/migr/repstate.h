#ifndef REPSTATE_H
#define REPSTATE_H
#include "work_item.h"
#include "siq_btree.h"
#include <isi_domain/dom.h>

#define REP_RES_RANGE_MIN 8000
#define REP_RES_RANGE_MAX (1<<30LL)
#define REP_INFO_LIN 0x1

#define CHANGELIST_REP_DIR "changelist"

__BEGIN_DECLS

struct rep_entry {
	/*
	 * PD How many links are in the replication set.
	 * This count includes excluded links.
	 * Used in Permanent and Diffential repstate
	 */
	u_int32_t lcount;
	/*
	 * 
	 * For files, Indicates file data hash mismatch happened
	 * during delta transfer of file. The non_hashed should
	 * be set when this flag is set.
	 * For directories, it indicates that link exception has
	 * happened.
	 * Used in Differential repstate.
	 */
	bool exception;
	/* Indicates the LIN is a directory that is
	 * only partially in the replication set because
	 * of a descendent included directory.
	 * Used in Permanent and Diffential repstate
	 */
	bool incl_for_child;

	/*
	 * Set when hash is computed for file.
	 * Used in Differential repstate.
	 */
	bool hash_computed;

	/*
	 * Used to track what files have been fully hashed
	 * under v3 (and thus don't have to be fully hashed
	 * in the future due to read only protection).
	 * Used in Permanent and Diffential repstate
	 */
	bool non_hashed;
	
	/* Indicates the LIN is a directory.
	 * Used in Permanent and Diffential repstate
	 */
	bool is_dir;

	/*
	 * Indicates that this LIN was moved into a new
	 * directory.  Only used for Directories.	
	 * Used in Differential repstate
	 */
	bool new_dir_parent;
	/*
	 * Indicates that this LIN had not been transferred
	 * to the target as of the beginning of this replication
	 * pass. 
	 * Used in Differential repstate
	 */
	bool not_on_target;
	/*
	 * Indicates the LIN was modified this pass.
	 * Used in Differential repstate
	 */
	bool changed;
	/* Indicates the LIN has been moved from the
	 * differential to the permanent repstate.
	 * Used in ???
	 */
	bool flipped;
	/*
	 * Indicates that lcount is already set to its
	 * correct value. So no more modifications required.
	 */
	bool lcount_set;
	/*
	 * Indicates that lcount is reset for previous snapshot version
	 * So no more lcount decrement modifications required.
	 */
	bool lcount_reset;

	/*
	 * This directory must be scanned for possible changes
	 * due to a changed path.
	 */
	bool for_path;
	/*
	 * Indicate the dirent that is responsible for transferring
	 * the contents of a multiply linked file.
	 * Used in Differential repstate
	 */
	uint64_t hl_dir_lin;
	uint64_t hl_dir_offset;

	/*
	 * count of excluded links to this file.
	 * Used in Permanent and Diffential repstate
	 */
	uint32_t excl_lcount;
};

struct rep_info_entry
{
	ifs_snapid_t initial_sync_snap;
	uint8_t is_sync_mode;
} __packed;



struct rep_ctx {
	int rep_fd;
	ifs_snapid_t snapid;
	ifs_domainid_t domain_id;
	bool dynamic;
	struct isi_hash *sel_hash;
};

struct rep_iter {
	struct rep_ctx *ctx;
	btree_key_t key;
};

/*
 *		SELECT TREE ENTRIES
 *
 * Select tree can have three types of entries
 * 1) Excludes : There are pure exclusion file/directory entries.
 *
 * 2) Includes : These are pure inclusion file/directory entries.
 *
 * 3) for_child: There are used as link between exclusion and inclusion
 *	entries. Note that A exclusion entry which is also for_child is 
 * 	only represented as for_child and exlcusion entry is removed. 
 */
	
struct sel_entry {
	/* Parent directory lin containing this lin */
	uint64_t plin;
	/* Name of the dirent in the parent directory lin */
	char name[256];
	/* Encoding of the dirent */
	int enc;
	/* Select entry protperty, seee above */
	bool includes;
	bool excludes;
	bool for_child;
};

struct sel_hash_entry {
	struct isi_hash_entry entry;
	uint64_t lin;
	/* Parent directory lin containing this lin */
	uint64_t plin;
	/* Name of the dirent in the parent directory lin */
	char name[256];
	/* Encoding of the dirent */
	int enc;
	/* Select entry property, see above */
	bool includes;
	bool excludes;
	bool for_child;
};


#define MAX_BLK_OPS SBT_BULK_MAX_OPS	/* Current limit is 100 */

struct rep_log_entry {
	uint64_t lin;
	enum sbt_bulk_op_type oper; /* Create new, modify old, or delete? */
	bool set_value; /* Always set the new value */
	struct rep_entry old_entry;
	struct rep_entry new_entry;
};

struct rep_log {
	int index;
	struct rep_log_entry entries[MAX_BLK_OPS];
};


void create_repstate(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out);
void open_repstate(const char *policy, char *name, struct rep_ctx **ctx_out,
    struct isi_error **error_out);
void close_repstate(struct rep_ctx *);
void remove_repstate(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out);

void set_entry(u_int64_t, struct rep_entry *, struct rep_ctx *,
    u_int64_t wi_lin, struct work_restart_state *rstart,
    struct isi_error **error_out);
bool
set_entry_cond(u_int64_t lin, struct rep_entry *rep, struct rep_ctx *ctx,
    bool is_new, struct rep_entry *old_rep, u_int64_t wi_lin,
    struct work_restart_state *restart, void *tw_data, size_t tw_size,
    struct isi_error **error_out);

bool get_entry(u_int64_t lin, struct rep_entry *rep, struct rep_ctx *ctx,
    struct isi_error **error_out);
bool
get_repkey_at_loc(unsigned nu, unsigned deb, uint64_t *lin, 
    struct rep_entry *rep, struct rep_ctx *ctx, struct isi_error **error_out);
ifs_lin_t
get_rep_max_lin(struct rep_ctx *ctx, struct isi_error **error_out);

bool remove_entry(u_int64_t, struct rep_ctx *, struct isi_error **);
bool entry_exists(u_int64_t, struct rep_ctx *,
    struct isi_error **error_out);

struct rep_iter *new_rep_iter(struct rep_ctx *);
struct rep_iter *new_rep_range_iter(struct rep_ctx *, uint64_t );
bool rep_iter_next(struct rep_iter *, u_int64_t *, struct rep_entry *,
    struct isi_error **);
void close_rep_iter(struct rep_iter *);
void
set_work_entry(u_int64_t lin, struct work_restart_state *ws,
    void *tw_data, size_t tw_size, struct rep_ctx *ctx,
    struct isi_error **error_out);
bool
get_work_entry(u_int64_t lin, struct work_restart_state *ws,
    char **tw_data, size_t *tw_size, struct rep_ctx *ctx,
    struct isi_error **error_out);
void
update_work_entries(struct rep_ctx *ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, struct isi_error **error_out);
void
update_work_entries_cond(struct rep_ctx *ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, bool check_disk, struct isi_error **error_out);
void
upgrade_work_entries(struct rep_ctx *ctx, struct isi_error **error_out);
void
get_base_rep_name(char *buf, const char *policy, bool restore);
void
get_repeated_rep_name(char *buf, const char *policy, uint64_t snapid,
    bool restore);
void
get_repeated_rep_pattern(char *buf, const char *policy, bool restore);

bool
set_job_phase_cond(struct rep_ctx *ctx, u_int64_t phase_lin,
    char *jstate, size_t jstate_size, char *old_jstate,
    size_t old_jstate_size, struct isi_error **error_out);
bool
update_job_phase(struct rep_ctx *ctx, u_int64_t phase_lin,
    char *jstate, size_t jstate_size, char *oldjstate, size_t old_jstate_size,
    u_int64_t wi_lin, struct work_restart_state *work, 
    void *tw_data, size_t tw_size, bool job_init,
    struct isi_error **error_out);

ifs_lin_t
get_first_work_lin(struct rep_ctx *ctx, struct isi_error **error_out);
void
get_state_entry(u_int64_t lin, char *buf, size_t *size, struct rep_ctx *ctx,
    struct isi_error **error_out);
void
refresh_job_state(struct stf_job_state *jstate, uint64_t phase_lin,
    struct rep_ctx *ctx, struct isi_error **error_out);

bool
work_iter_next(struct rep_iter *iter, u_int64_t *lin,
    struct work_restart_state *work, struct isi_error **error_out);
bool
get_sel_entry(u_int64_t lin, struct sel_entry *sel, struct rep_ctx *ctx,
    struct isi_error **error_out);
void
set_sel_entry(u_int64_t lin, struct sel_entry *sel,
    struct rep_ctx *ctx, struct isi_error **error_out);
void
get_select_name(char *buf, const char *policy, uint64_t snapid, bool restore);
void
get_select_pattern(char *buf, const char *policy, bool restore);


bool
get_sel_hash_entry(u_int64_t lin, struct sel_hash_entry *sel,
    struct rep_ctx *ctx, struct isi_error **error_out);


bool
sel_iter_next(struct rep_iter *iter, u_int64_t *lin, struct sel_entry *sel,
    struct isi_error **error_out);
void
remove_work_entry(u_int64_t wi_lin, struct work_restart_state *work,
    struct rep_ctx *ctx, struct isi_error **error_out);
void
remove_work_entry_cond(u_int64_t wi_lin, struct work_restart_state *work,
    struct rep_ctx *ctx, bool check_disk, struct isi_error **error_out);

bool
stf_job_phase_cond_enable( uint64_t phase_lin, enum stf_job_phase phase,
    struct rep_ctx *ctx, struct isi_error **error_out);
void
check_if_in_repdir(uint64_t lin, struct rep_ctx *sel, ifs_snapid_t snap,
    bool has_excludes, bool *inrep, struct isi_error **error_out);

char *
get_valid_rep_path( uint64_t lin, ifs_snapid_t snap);

bool
repstate_exists(const char *policy, char *name, struct isi_error **error_out);
void
add_rep_bulk_entry(struct rep_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, enum sbt_bulk_op_type oper,
    struct rep_entry *rep, char *sbt_buf, struct btree_flags *sbt_flags,
    struct rep_entry *old_rep, char *sbt_old_buf,
    struct btree_flags *sbt_old_flags,
    const size_t sbt_buf_size);

bool
sync_rep_entry(struct rep_ctx *ctx, ifs_lin_t lin, struct rep_entry *oldp,
    struct rep_entry *newp, bool set_value, struct isi_error **error_out);
void
flush_rep_log(struct rep_log *logp, struct rep_ctx *ctx,
    struct isi_error **error_out);
void
copy_rep_entries(struct rep_ctx *ctx, ifs_lin_t *lins,
    struct rep_entry *entries, int num_entries, struct isi_error **error_out);
void
rep_log_add_entry(struct rep_ctx *ctx, struct rep_log *logp,
    ifs_lin_t lin, struct rep_entry *new_re, struct rep_entry *old,
    enum sbt_bulk_op_type oper, bool set_value, struct isi_error **error_out);
void prune_old_repstate(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out);

bool
get_rep_info(struct rep_info_entry *rei, struct rep_ctx *ctx,
    struct isi_error **error_out);
void
set_rep_info(struct rep_info_entry *rei, struct rep_ctx *ctx,
    bool force, struct isi_error **error_out);
void
set_dynamic_rep_lookup(struct rep_ctx *ctx, ifs_snapid_t snapid,
    ifs_domainid_t domain_id);
bool
dynamic_entry_lookup(u_int64_t lin, ifs_snapid_t snap, ifs_domainid_t domain_id,
    struct rep_entry *rep, struct isi_error **error_out);
void
get_mirrored_rep_name(char *buf, const char *policy);
void
get_changelist_rep_name(char *buf, ifs_snapid_t old_snapid,
    ifs_snapid_t new_snapid);
bool
decomp_changelist_rep_name(const char *name, ifs_snapid_t *sid_old_out,
    ifs_snapid_t *sid_new_out);

__END_DECLS

#endif
