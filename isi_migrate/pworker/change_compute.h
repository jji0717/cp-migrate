#ifndef CHANGE_COMPUTE_H
#define CHANGE_COMPUTE_H

#include "isi_migrate/migr/summ_stf.h"

#define CC_DIRENTS_NUM 100
#define CC_DIRENTS_SIZE (CC_DIRENTS_NUM * sizeof(struct dirent))

#define CC_PROCESS_WL_SIZE 100

struct migr_pworker_ctx;

enum lin_transfer_state {
	STF_CONT_LIN_DATA = 1,
	STF_CONT_LIN_COMMIT,
	STF_CONT_LIN_ADS
};

enum dirent_mod_type {
	DENT_ADDED = 1,
	DENT_REMOVED,
	DENT_FILT_ADDED,
	DENT_FILT_REMOVED 
};

struct changeset_ctx {
	ifs_snapid_t snap1;
	ifs_snapid_t snap2;
	struct rep_ctx *rep1; /* Repstate */
	struct rep_ctx *rep2; /* Incremental Repstate */

	/* Summary STF btree */
	struct summ_stf_ctx *summ_stf;
	struct summ_stf_iter *summ_iter;

	/* Worklist context */
	struct wl_ctx *wl;
	struct wl_iter *wl_iter;

	/* sel btree context*/
	struct rep_ctx *sel1;
	struct rep_ctx *sel2;

	/* Set if the directory is included for child */
	bool incl_for_child1;
	bool incl_for_child2;

	/* Used when iterating a directory */
	ifs_lin_t dir_lin;

	/* Iterator of the snap1 version of the directory */
	struct migr_dir *md_dir1;

	/* Iterator of the snap2 version of the directory */
	struct migr_dir *md_dir2;

	/* Cloudpools sync state btree */
	struct cpss_ctx *cpss1;
	struct cpss_ctx *cpss2;

	/* Resync lin list btree for compliance mode resyncs */
	struct lin_list_ctx *resync;
};

struct changeset_ctx *create_changeset(const char *policy,
    ifs_snapid_t snap1, ifs_snapid_t snap2, bool restore, 
    struct isi_error **error_out);

struct changeset_ctx *create_changeset_resync(const char *policy,
    ifs_snapid_t snap1, ifs_snapid_t snap2, bool restore, bool src,
    bool compliance_v2, struct isi_error **error_out);

void free_changeset(struct changeset_ctx *cc_ctx);

void create_dummy_begin(const char *policy, ifs_snapid_t snap,
    ifs_lin_t root_lin, struct isi_error **error_out);

void check_parent_change(struct changeset_ctx *cc_ctx,
    ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    bool *pchanged, bool *removed, struct isi_error **error_out);

bool process_dirent_and_file_mods(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);

void
process_single_lin_modify(struct migr_pworker_ctx *pw_ctx, 
    ifs_lin_t lin, bool newdir, int num_operations,
    struct isi_error **error_out);

bool
process_removed_dirs(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);

bool
process_removed_dirs_worklist(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);
bool
process_added_dirs_worklist(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out);
bool
process_ppath_worklist(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out);

void
setup_migr_dir(struct changeset_ctx *cc_ctx, ifs_lin_t dir_lin, bool newdir,
    uint64_t *resume_cookie2, bool removedir, uint64_t *resume_cookie1, 
    bool force, bool has_excludes, bool need_stat,
    struct isi_error **error_out);

struct migr_dirent *
find_modified_dirent(struct migr_pworker_ctx *pw_ctx, ifs_lin_t dir_lin, 
    bool newdir, uint64_t *resume_cookie2, bool removedir,
    uint64_t *resume_cookie1, enum dirent_mod_type *modtype,
    struct migr_dir **md_out, bool need_stat, struct isi_error **error_out);

bool
process_stf_lins(struct migr_pworker_ctx *pw_ctx, bool force_set,
    struct isi_error **error_out);

bool
check_select_entries(struct changeset_ctx *cc_ctx, struct rep_ctx *sel,
    bool add_to_stf, struct isi_error **error_out);

void
clear_migr_dir(struct migr_pworker_ctx *pw_ctx, ifs_lin_t dir_lin);
bool
wl_get_entry(struct migr_pworker_ctx *pw_ctx, uint64_t *lin,
    struct wl_entry **entry, struct isi_error **error_out);
void
wl_log_add_entry(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin, 
    struct isi_error **error_out);
void
stf_rep_log_add_entry(struct migr_pworker_ctx *pw_ctx, struct rep_ctx *ctx,
    ifs_lin_t lin, struct rep_entry *new, struct rep_entry *old, bool is_new,
    bool set_value, struct isi_error **error_out);
void
cpss_log_add_entry(struct migr_pworker_ctx *pw_ctx, struct cpss_ctx *ctx,
    ifs_lin_t lin, struct cpss_entry *new, struct cpss_entry *old, bool is_new,
    struct isi_error **error_out);

bool stat_lin_snap(ifs_lin_t lin, ifs_snapid_t snap, struct stat *stat,
    bool missing_ok, struct isi_error **error_out);

#endif
