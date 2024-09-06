#ifndef STF_WORK_H
#define STF_WORK_H

#include "isi_migrate/config/siq_gconfig_gcfg.h"

/* These work items have special meaning, they initialize each phase */
enum stf_phase_init_work_items
{
	STF_JOB_PHASE_LIN = 8000,
};

/* Make sure this is greater than STF_WORK_MAX */
#define WORK_RES_RANGE_MIN 9000
#define WORK_RES_RANGE_MAX (1<<30LL)

/* Only split files >= to 20mb */
#define MIN_FILE_SPLIT_SIZE 20 * 1024 * 1024

enum work_restart_type
{
	WORK_SNAP_TW = 1,
	WORK_SUMM_TREE,
	WORK_CC_PPRED_WL,
	WORK_CC_DIR_DEL,
	WORK_CC_DIR_DEL_WL,
	WORK_CC_DIR_CHGS,
	WORK_CC_DIR_CHGS_WL,
	WORK_CT_LIN_UPDTS,
	WORK_CT_LIN_HASH_EXCPT,
	WORK_CT_FILE_DELS,
	WORK_CT_DIR_DELS,
	WORK_CT_DIR_LINKS,
	WORK_CT_DIR_LINKS_EXCPT,
	WORK_CT_STALE_DIRS,
	WORK_FLIP_LINS,
	WORK_IDMAP_SEND,
	WORK_DOMAIN_MARK,
	WORK_DATABASE_CORRECT,
	WORK_DATABASE_RESYNC,
	WORK_CC_COMP_RESYNC,
	WORK_COMP_CONFLICT_CLEANUP,
	WORK_COMP_CONFLICT_LIST,
	WORK_CT_COMP_DIR_LINKS,
	WORK_COMP_COMMIT,
	WORK_COMP_MAP_PROCESS,
	WORK_CT_COMP_DIR_DELS,
	WORK_FINISH
};

#define MAX_TW_ENTRY_SIZE 6144     /* For now we just allow 6K per btree entry */
#define WORK_LIN_RESOLUTION 1000     /* Work items can only be multiple of 1000s */

/*
 * This describes each stf phase and whether the phase is
 * (is enabled or disabled for current type of job (treewalk and stf 
 * lin based types)
 */
struct stf_phase_entry {
	int stf_phase;
	bool enabled;
};

/**
 * Entry representing a lin or lin/dirent based
 * work item and associated checkpoint state.
 * Add new fields to the end of the structure, on disk
 * work items will be upgraded on coordinator start up.
 */
struct work_restart_state
{
	enum work_restart_type rt;

	/* Where are we in a current work item */
	ifs_lin_t lin;
	/* Last checkpoint lin */
	ifs_lin_t chkpt_lin;

	/* Snap1 version dirent cookie */
	uint64_t dircookie1;
	/* Snap2 version dirent cookie */
	uint64_t dircookie2;

	/* The current range in btree */
	unsigned int num1;
	unsigned int den1;
	unsigned int num2;
	unsigned int den2;

	/* What is the current work item */
	ifs_lin_t min_lin;
	ifs_lin_t max_lin;

	/* Current worklist level */
	unsigned int wl_cur_level;
	/* Total number of extension blocks storing tw data */
	unsigned int tw_blk_cnt; /* It cannot go beyond WORK_LIN_RESOLUTION */

	/* For file splitting - current byte range and file lin */
	uint64_t f_low;
	uint64_t f_high;
	ifs_lin_t fsplit_lin;

	/* Revision number for the number of coordinator restarts while
	 * processing this work item */
	uint32_t num_coord_restarts;
}__packed;

enum job_type {
	JOB_TW = 1,
	JOB_STF_LIN,
	JOB_TW_V3_5,
	JOB_STF_LIN_V3_5,
	JOB_TW_COMP,
	JOB_STF_LIN_COMP
};

/*
 * This structure stores the current job phase and 
 * entire state transition states
 */
struct stf_job_state
{
	int jtype;
	int cur_phase;
	struct stf_phase_entry *states;
};

void allocate_entire_work(struct work_restart_state *work,
    enum work_restart_type rt);
int 
get_total_phase_num(int job_type);
int
get_total_enabled_phases(struct stf_job_state *job_phase);
int
next_job_phase_valid(struct stf_job_state *job_phase);
bool
is_phase_before(struct stf_job_state *job_phase, int current_phase,
    int phase_to_check);
enum work_restart_type
get_work_restart_type(struct stf_job_state *job_phase);
void
next_job_phase(struct stf_job_state *job_phase);
uint64_t
get_next_work_lin(uint64_t work_lin);
void
find_first_enabled_phase(struct stf_job_state *job_phase);
void
init_job_phase_ctx(enum job_type jtype, struct stf_job_state *job_phase);
bool
stf_job_phase_disabled(struct stf_job_state *job_phase,
   enum stf_job_phase phase);
void
stf_job_phase_enable(struct stf_job_state *job_phase,
	enum stf_job_phase phase);
char *
fill_job_state(struct stf_job_state *jstate, size_t *size);
void
read_job_state(struct stf_job_state *jstate, char *buf, size_t size);
size_t 
job_phase_max_size(void);
int
map_work_to_error(enum work_restart_type rt);
char *
job_phase_str(struct stf_job_state *job_state);
char *
work_type_str(enum work_restart_type rt);
void
validate_work_lin(uint64_t work_lin);
void
stf_job_phase_disable(struct stf_job_state *job_phase,
	enum stf_job_phase phase);
void
disable_all_phases(struct stf_job_state *job_phase);
bool
contains_phase(struct stf_job_state *job_phase, enum stf_job_phase phase);
bool
is_fsplit_work_item(struct work_restart_state *work);

int
lock_work_item(char *pol_id, int timeout, struct isi_error **error_out);
int
lock_all_work_items(char *pol_id, int timeout, struct isi_error **error_out);
void
delete_work_item_lock(char *pol_id, struct isi_error **error_out);
int
lock_work_item_target(char *pol_id, int wi_lock_mod, uint64_t wi_lin,
    int timeout, struct isi_error **error_out);
void
release_target_work_item_lock(int *wi_lock_fd, char *pol_id, int wi_lock_mod,
    uint64_t wi_lin, struct isi_error **error_out);

#endif

