#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>

#include "sbtree.h"
#include "isirep.h"
#include "treewalk.h"
#include "work_item.h"

/*
 * Treewalk state transition
 */
struct stf_phase_entry tw_phase[] = {
	{STF_PHASE_TW, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_IDMAP_SEND, false}
};

/*
 * Treewalk state transition
 */
struct stf_phase_entry tw_phase_v3_5[] = {
	{STF_PHASE_TW, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_IDMAP_SEND, false},
	{STF_PHASE_DATABASE_RESYNC, false},
	{STF_PHASE_DOMAIN_MARK, false},
};

/*
 * Treewalk state transition for compliance
 */
struct stf_phase_entry tw_phase_comp[] = {
	{STF_PHASE_TW, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_COMP_COMMIT, false},
	{STF_PHASE_IDMAP_SEND, false},
	{STF_PHASE_DATABASE_RESYNC, false},
	{STF_PHASE_DOMAIN_MARK, false},
};


/*
 * STF Lin based state transition
 */
struct stf_phase_entry lin_phase[] = {
	{STF_PHASE_SUMM_TREE, true},
	{STF_PHASE_CC_DIR_DEL, true},
	{STF_PHASE_CC_DIR_CHGS, true},
	{STF_PHASE_CT_FILE_DELS, true},
	{STF_PHASE_CT_DIR_DELS, true},
	{STF_PHASE_CT_DIR_LINKS, true},
	{STF_PHASE_CT_DIR_LINKS_EXCPT, false},
	{STF_PHASE_CT_LIN_UPDTS, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_CT_STALE_DIRS, false},
	{STF_PHASE_FLIP_LINS, true},
	{STF_PHASE_IDMAP_SEND, false}
};

/*
 * STF Lin based state transition for SyncIQ V3.5
 */
struct stf_phase_entry lin_phase_v3_5[] = {
	{STF_PHASE_SUMM_TREE, true},
	{STF_PHASE_CC_DIR_DEL, true},
	{STF_PHASE_CC_DIR_CHGS, true},
	{STF_PHASE_CT_FILE_DELS, true},
	{STF_PHASE_CT_DIR_DELS, true},
	{STF_PHASE_CT_DIR_LINKS, true},
	{STF_PHASE_CT_DIR_LINKS_EXCPT, false},
	{STF_PHASE_CT_LIN_UPDTS, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_CT_STALE_DIRS, false},
	{STF_PHASE_FLIP_LINS, true},
	{STF_PHASE_DATABASE_CORRECT, false},
	{STF_PHASE_IDMAP_SEND, false},
	{STF_PHASE_DATABASE_RESYNC, false},
};

/*
 * STF Lin based state transition for SyncIQ compliance
 */
struct stf_phase_entry lin_phase_comp[] = {
	{STF_PHASE_COMP_FINISH_COMMIT, false},
	{STF_PHASE_SUMM_TREE, true},
	{STF_PHASE_CC_COMP_RESYNC, false},
	{STF_PHASE_CC_DIR_DEL, true},
	{STF_PHASE_CC_DIR_CHGS, true},
	{STF_PHASE_COMP_CONFLICT_CLEANUP, false},
	{STF_PHASE_COMP_CONFLICT_LIST, false},
	{STF_PHASE_CT_FILE_DELS, true},
	{STF_PHASE_CT_DIR_DELS, true},
	{STF_PHASE_CT_COMP_DIR_DELS, false},
	{STF_PHASE_CT_COMP_DIR_LINKS, false},
	{STF_PHASE_CT_DIR_LINKS, true},
	{STF_PHASE_CT_DIR_LINKS_EXCPT, false},
	{STF_PHASE_CT_LIN_UPDTS, true},
	{STF_PHASE_CT_LIN_HASH_EXCPT, false},
	{STF_PHASE_CT_STALE_DIRS, false},
	{STF_PHASE_COMP_COMMIT, false},
	{STF_PHASE_FLIP_LINS, true},
	{STF_PHASE_DATABASE_CORRECT, false},
	{STF_PHASE_IDMAP_SEND, false},
	{STF_PHASE_DATABASE_RESYNC, false},
	{STF_PHASE_COMP_MAP_PROCESS, false},
};


void
allocate_entire_work(struct work_restart_state *ws, enum work_restart_type rt)
{
	ASSERT(ws != NULL);
	memset(ws, 0, sizeof(struct work_restart_state));
	ws->rt = rt;
	ws->num1 = 0;
	ws->den1 = 100;
	ws->num2 = 100;
	ws->den2 = 100;
	ws->min_lin = ROOT_LIN;
	ws->max_lin = -1; /* MAX value */
	ws->dircookie1 = ws->dircookie2 = DIR_SLICE_MIN;
	ws->lin = 0;
	ws->chkpt_lin = 0;
	ws->f_low = 0;
	ws->f_high = 0;
	ws->fsplit_lin = 0;
	ws->num_coord_restarts = 0;
}

void
find_first_enabled_phase(struct stf_job_state *job_phase)
{
	int i;
	int n;

	n = sizeof(job_phase->states);
	//Find an enabled phase, which is not necessarily the first phase
	for (i = 0; i < n; i++) {
		if (job_phase->states[i].enabled) {
			job_phase->cur_phase =
			    job_phase->states[i].stf_phase;
			break;
		}
	}
	ASSERT(job_phase->cur_phase);
}

void
init_job_phase_ctx(enum job_type jtype, struct stf_job_state *job_phase)
{

	int n;
	char *p = NULL;

	ASSERT(job_phase != NULL);
	switch(jtype) {

	case JOB_TW:
		job_phase->states = tw_phase;
		n = sizeof(tw_phase);
		p = (char *)tw_phase;
		break;

	case JOB_TW_V3_5:
		job_phase->states = tw_phase_v3_5;
		n = sizeof(tw_phase_v3_5);
		p = (char *)tw_phase_v3_5;
		break;

	case JOB_TW_COMP:
		job_phase->states = tw_phase_comp;
		n = sizeof(tw_phase_comp);
		p = (char *)tw_phase_comp;
		break;

	case JOB_STF_LIN:
		job_phase->states = lin_phase;
		n = sizeof(lin_phase);
		p = (char *)lin_phase;
		break;

	case JOB_STF_LIN_V3_5:
		job_phase->states = lin_phase_v3_5;
		n = sizeof(lin_phase_v3_5);
		p = (char *)lin_phase_v3_5;
		break;

	case JOB_STF_LIN_COMP:
		job_phase->states = lin_phase_comp;
		n = sizeof(lin_phase_comp);
		p = (char *)lin_phase_comp;
		break;

	default:
		ASSERT(0);
	}
	job_phase->jtype = jtype;

	find_first_enabled_phase(job_phase);

	job_phase->states = malloc(n);
	memcpy(job_phase->states, p, n);

}

char *
job_phase_str(struct stf_job_state *job_state)
{
	ASSERT(job_state != NULL);
	return job_phase_enum_str(job_state->cur_phase);
}

char *
work_type_str(enum work_restart_type rt)
{
	switch(rt) {
	case WORK_SNAP_TW:
		return "WORK_SNAP_TW";
		break;
	case WORK_SUMM_TREE:
		return "WORK_SUMM_TREE";
		break;
	case WORK_CC_PPRED_WL:
		return "WORK_CC_PPRED_WL";
		break;
	case WORK_CC_DIR_DEL:
		return "WORK_CC_DIR_DEL";
		break;
	case WORK_CC_DIR_DEL_WL:
		return "WORK_CC_DIR_DEL_WL";
		break;
	case WORK_CC_DIR_CHGS:
		return "WORK_CC_DIR_CHGS";
		break;
	case WORK_CC_DIR_CHGS_WL:
		return "WORK_CC_DIR_CHGS_WL";
		break;
	case WORK_CT_LIN_UPDTS:
		return "WORK_CT_LIN_UPDTS";
		break;
	case WORK_CT_LIN_HASH_EXCPT:
		return "WORK_CT_LIN_HASH_EXCPT";
		break;
	case WORK_CT_FILE_DELS:
		return "WORK_CT_FILE_DELS";
		break;
	case WORK_CT_DIR_DELS:
		return "WORK_CT_DIR_DELS";
		break;
	case WORK_CT_DIR_LINKS:
		return "WORK_CT_DIR_LINKS";
		break;
	case WORK_CT_DIR_LINKS_EXCPT:
		return "WORK_CT_DIR_LINKS_EXCPT";
		break;
	case WORK_CT_STALE_DIRS:
		return "WORK_CT_STALE_DIRS";
		break;
	case WORK_FLIP_LINS:
		return "WORK_FLIP_LINS";
		break;
	case WORK_IDMAP_SEND:
		return "WORK_IDMAP_SEND";
		break;
	case WORK_DATABASE_CORRECT:
		return "WORK_DATABASE_CORRECT";
		break;
	case WORK_DATABASE_RESYNC:
		return "WORK_DATABASE_RESYNC";
		break;
	case WORK_DOMAIN_MARK:
		return "WORK_DOMAIN_MARK";
		break;
	case WORK_CC_COMP_RESYNC:
		return "WORK_CC_COMP_RESYNC";
		break;
	case WORK_COMP_CONFLICT_CLEANUP:
		return "WORK_COMP_CONFLICT_CLEANUP";
		break;
	case WORK_COMP_CONFLICT_LIST:
		return "WORK_COMP_CONFLICT_LIST";
		break;
	case WORK_CT_COMP_DIR_LINKS:
		return "WORK_CT_COMP_DIR_LINKS";
		break;
	case WORK_COMP_COMMIT:
		return "WORK_COMP_COMMIT";
		break;
	case WORK_COMP_MAP_PROCESS:
		return "WORK_COMP_MAP_PROCESS";
		break;
	case WORK_CT_COMP_DIR_DELS:
		return "WORK_CT_COMP_DIR_DELS";
		break;

	default:
		return "Unknown";
	}
}

int
get_total_phase_num(int job_type)
{
	if (job_type == JOB_TW)
		return sizeof(tw_phase) / sizeof(struct stf_phase_entry);
	else if (job_type == JOB_STF_LIN)
		return sizeof(lin_phase) / sizeof(struct stf_phase_entry);
	else if (job_type == JOB_TW_V3_5)
		return sizeof(tw_phase_v3_5) / sizeof(struct stf_phase_entry);
	else if (job_type == JOB_STF_LIN_V3_5)
		return sizeof(lin_phase_v3_5) / sizeof(struct stf_phase_entry);
	else if (job_type == JOB_TW_COMP)
		return sizeof(tw_phase_comp) / sizeof(struct stf_phase_entry);
	else if (job_type == JOB_STF_LIN_COMP)
		return sizeof(lin_phase_comp) / sizeof(struct stf_phase_entry);

		ASSERT(0);
}

int
get_total_enabled_phases(struct stf_job_state *job_phase)
{
	int count = 0;
	int i;
	int total = 0;
	
	total = get_total_phase_num(job_phase->jtype);
	for (i = 0; i < total; i++) {
		if (job_phase->states[i].enabled) {
			count++;
		}
	}
	
	return count;
	
}

void
disable_all_phases(struct stf_job_state *job_phase)
{
	int i;
	int jtype;
	int n;

	ASSERT(job_phase != NULL);

	jtype = job_phase->jtype;
	n = get_total_phase_num(jtype);

	/* Find the current index in the table */
	for (i = 0; i < n ; i++) {
		job_phase->states[i].enabled = false;
	}
}

static int
get_phase_index(struct stf_job_state *job_phase, enum stf_job_phase phase,
    int *n)
{
	int i;
	int jtype;

	log(TRACE, "get_phase_index enter");

	ASSERT(job_phase != NULL);

	jtype = job_phase->jtype;
	*n = get_total_phase_num(jtype);
	log(TRACE, "get_phase_index total_phase_num=%d, jtype=%d", *n, jtype);

	/* Find the current index in the table */
	for (i = 0; i < *n ; i++) {
	log(TRACE, "get_phase_index i=%d", i);
		if (job_phase->states[i].stf_phase == phase)
			break;
	}

	return i;
}

/*
 * Check if the state after cur phase is valid
 */
int
next_job_phase_valid(struct stf_job_state *job_phase)
{
	int i;
	int n;
	int index = 0;

	log(TRACE, "next_job_phase_valid");
	ASSERT(job_phase != NULL);
	i = get_phase_index(job_phase, job_phase->cur_phase, &n);
	ASSERT(i < n);
	
	/* Now check if there is enabled state after our state */
	i++;
	for (; i < n ; i++) {
		if (job_phase->states[i].enabled == true) {
			index = i;
			break;
		}
	}
	
	return index;
}

bool
is_phase_before(struct stf_job_state *job_phase, int current_phase,
    int phase_to_check)
{
	int i;
	int n;

	log(TRACE, "%s: current_phase: %d phase_to_check: %d", __func__,
	    current_phase, phase_to_check);
	//Bail early
	if (current_phase == phase_to_check) {
		return false;
	}
	n = get_total_phase_num(job_phase->jtype);
	for (i = 0; i < n; i++) {
		log(TRACE, "%s: i: %d phase: %d", __func__, i,
		    job_phase->states[i].stf_phase);
		if (job_phase->states[i].stf_phase == current_phase) {
			return true;
		}
		if (job_phase->states[i].stf_phase == phase_to_check) {
			return false;
		}
	}
	//Something went wrong and there are no phases?!
	ASSERT(false);
}

static enum work_restart_type
map_phase_to_work(enum stf_job_phase job_phase)
{
	enum work_restart_type rt;
	switch (job_phase)
	{
		case STF_PHASE_TW:
			rt = WORK_SNAP_TW;
			break;
		case STF_PHASE_CT_LIN_HASH_EXCPT:
			rt = WORK_CT_LIN_HASH_EXCPT;
			break;
		case STF_PHASE_SUMM_TREE:
			rt = WORK_SUMM_TREE;
			break;
		case STF_PHASE_CC_DIR_DEL:
			rt = WORK_CC_DIR_DEL;
			break;
		case STF_PHASE_CC_DIR_CHGS:
			rt = WORK_CC_DIR_CHGS;
			break;
		case STF_PHASE_CT_LIN_UPDTS:
			rt = WORK_CT_LIN_UPDTS;
			break;
		case STF_PHASE_CT_FILE_DELS:
			rt = WORK_CT_FILE_DELS;
			break;
		case STF_PHASE_CT_DIR_DELS:
			rt = WORK_CT_DIR_DELS;
			break;
		case STF_PHASE_CT_DIR_LINKS:
			rt = WORK_CT_DIR_LINKS;
			break;
		case STF_PHASE_CT_DIR_LINKS_EXCPT:
			rt = WORK_CT_DIR_LINKS_EXCPT;
			break;
		case STF_PHASE_CT_STALE_DIRS:
			rt = WORK_CT_STALE_DIRS;
			break;
		case STF_PHASE_FLIP_LINS:
			rt = WORK_FLIP_LINS;
			break;
		case STF_PHASE_IDMAP_SEND:
			rt = WORK_IDMAP_SEND;
			break;
		case STF_PHASE_DATABASE_CORRECT:
			rt = WORK_DATABASE_CORRECT;
			break;
		case STF_PHASE_DATABASE_RESYNC:
			rt = WORK_DATABASE_RESYNC;
			break;
		case STF_PHASE_DOMAIN_MARK:
			rt = WORK_DOMAIN_MARK;
			break;
		case STF_PHASE_CC_COMP_RESYNC:
			rt = WORK_CC_COMP_RESYNC;
			break;
		case STF_PHASE_COMP_CONFLICT_CLEANUP:
			rt = WORK_COMP_CONFLICT_CLEANUP;
			break;
		case STF_PHASE_COMP_CONFLICT_LIST:
			rt = WORK_COMP_CONFLICT_LIST;
			break;
		case STF_PHASE_CT_COMP_DIR_LINKS:
			rt = WORK_CT_COMP_DIR_LINKS;
			break;
		case STF_PHASE_COMP_COMMIT:
		case STF_PHASE_COMP_FINISH_COMMIT:
			rt = WORK_COMP_COMMIT;
			break;
		case STF_PHASE_COMP_MAP_PROCESS:
			rt = WORK_COMP_MAP_PROCESS;
			break;
		case STF_PHASE_CT_COMP_DIR_DELS:
			rt = WORK_CT_COMP_DIR_DELS;
			break;
		default : ASSERT(0, "Invalid STF phase %d", job_phase);
	}
	return rt;
}

enum work_restart_type
get_work_restart_type(struct stf_job_state *job_phase)
{
	ASSERT(job_phase != NULL);
	return map_phase_to_work(job_phase->cur_phase);
}

uint64_t
get_next_work_lin(uint64_t work_lin)
{
	ASSERT((work_lin % WORK_LIN_RESOLUTION) == 0);
	work_lin += WORK_LIN_RESOLUTION;
	/* 
	 * XXX We may have to write code to reset the work_lin
	 * in future if it reaches WORK_RES_RANGE_MAX.
	 * XXX*/
	ASSERT(work_lin < WORK_RES_RANGE_MAX);
	return work_lin;

}

void
validate_work_lin(uint64_t work_lin)
{
	ASSERT((work_lin % WORK_LIN_RESOLUTION) == 0);
	ASSERT(work_lin >= WORK_RES_RANGE_MIN, "work_lin: %llx", work_lin);
	ASSERT(work_lin <= WORK_RES_RANGE_MAX, "work_lin: %llx", work_lin);
}

void
next_job_phase(struct stf_job_state *job_phase)
{
	int index;
	log(TRACE, "next_job_phase");
	ASSERT(job_phase != NULL);

	index = next_job_phase_valid(job_phase);
	/*
	 * Caller always check for state validity before calling
	 * next_job_phase
	 */
	ASSERT(index != 0);
	job_phase->cur_phase = job_phase->states[index].stf_phase;
}

bool
stf_job_phase_disabled(struct stf_job_state *job_phase,
   enum stf_job_phase phase)
{
	int i;
	int n;

	ASSERT(job_phase != NULL);
	i = get_phase_index(job_phase, phase, &n);
	ASSERT( i < n);

	if (job_phase->states[i].enabled == false)
		return true;
	else
		return false;
}

void
stf_job_phase_enable(struct stf_job_state *job_phase,
    enum stf_job_phase phase)
{
	int i;
	int n;

	log(DEBUG, "Enable phase %{}", enum_stf_job_phase_fmt(phase));

	ASSERT(job_phase != NULL);
	i = get_phase_index(job_phase, phase, &n);
	log(DEBUG, "Enable phase i=%d", i);
	ASSERT( i < n);

	job_phase->states[i].enabled = true;
}

void
stf_job_phase_disable(struct stf_job_state *job_phase,
    enum stf_job_phase phase)
{
	int i;
	int n;

	log(DEBUG, "Disable phase %{}", enum_stf_job_phase_fmt(phase));

	ASSERT(job_phase != NULL);
	i = get_phase_index(job_phase, phase, &n);
	ASSERT( i < n);

	job_phase->states[i].enabled = false;
}

bool
contains_phase(struct stf_job_state *job_phase, enum stf_job_phase phase)
{
	int i;
	int n;

	ASSERT(job_phase != NULL);
	i = get_phase_index(job_phase, phase, &n);
	return (i < n);
}

/*
 * Function to return size of job state transition.
 */
int static
job_state_transition_count(int jtype)
{
	int n = 0;

	switch(jtype) {
		case JOB_TW :
		    n = sizeof(tw_phase);
		    break;

		case JOB_STF_LIN:
		    n = sizeof(lin_phase);
		    break;

		case JOB_TW_V3_5:
		    n = sizeof(tw_phase_v3_5);
		    break;

		case JOB_STF_LIN_V3_5:
		    n = sizeof(lin_phase_v3_5);
		    break;

		case JOB_TW_COMP:
		    n = sizeof(tw_phase_comp);
		    break;

		case JOB_STF_LIN_COMP:
		    n = sizeof(lin_phase_comp);
		    break;

		default:
		    ASSERT(0, "Invalid job type %d", jtype);
	}

	return n;
}

char *
fill_job_state(struct stf_job_state *jstate, size_t *size)
{
	int n;
	int jtype;
	char *p;
	char *buf;

	log(TRACE, "fill_job_state");
	jtype = jstate->jtype;
	n = job_state_transition_count(jtype);

	p = buf = malloc(sizeof(int) + sizeof(int) + n);
	memset(p, 0, sizeof(int) + sizeof(int) + n);
	
	memcpy(p, &jstate->jtype, sizeof(int));
	p += sizeof(int);
	memcpy(p, &jstate->cur_phase, sizeof(int));
	p += sizeof(int);
	memcpy(p, jstate->states, n);
	*size = n + 2 * sizeof(int);

	return buf;
}

void
read_job_state(struct stf_job_state *jstate, char *buf, size_t size)
{
	int n;
	char *p;

	log(TRACE, "read_job_state");
	p = buf;	
	memcpy(&jstate->jtype, p, sizeof(int));
	p += sizeof(int);
	memcpy(&jstate->cur_phase, p, sizeof(int));
	p += sizeof(int);

	n = job_state_transition_count(jstate->jtype);

	jstate->states = malloc(n);
	memcpy(jstate->states, p, n);
	ASSERT(size == n + 2 * sizeof(int));	
}

size_t 
job_phase_max_size(void)
{
	return (sizeof(lin_phase_comp) + 2 * sizeof(int));
}

int
map_work_to_error(enum work_restart_type rt)
{

	int siq_error = 0;

	switch(rt) {
	case WORK_SUMM_TREE:
		siq_error = E_SIQ_SUMM_TREE;
		break;
	case WORK_CC_PPRED_WL:
		siq_error = E_SIQ_PPRED_REC;
		break;
	case WORK_CC_DIR_DEL:
	case WORK_CC_DIR_DEL_WL:
		siq_error = E_SIQ_CC_DEL_LIN;
		break;
	case WORK_CC_DIR_CHGS:
	case WORK_CC_DIR_CHGS_WL:
		siq_error = E_SIQ_CC_DIR_CHG;
		break;
	case WORK_CT_LIN_UPDTS:
	case WORK_CT_LIN_HASH_EXCPT:
		siq_error = E_SIQ_LIN_UPDT;
		break;
	case WORK_CT_FILE_DELS:
	case WORK_CT_DIR_DELS:
	case WORK_CT_COMP_DIR_DELS:
		siq_error = E_SIQ_DEL_LIN;
		break;
	case WORK_CT_DIR_LINKS:
	case WORK_CT_DIR_LINKS_EXCPT:
		siq_error = E_SIQ_CC_DIR_CHG;
		break;
	case WORK_FLIP_LINS:
		siq_error = E_SIQ_FLIP_LINS;
		break;
	case WORK_DATABASE_CORRECT:
	case WORK_DATABASE_RESYNC:
		siq_error = E_SIQ_DATABASE_ENTRIES;
		break;
	case WORK_IDMAP_SEND:
		siq_error = E_SIQ_IDMAP_SEND;
		break;
	case WORK_DOMAIN_MARK:
		siq_error = E_SIQ_DOMAIN_MARK;
		break;
	case WORK_CC_COMP_RESYNC:
		siq_error = E_SIQ_CC_COMP_RESYNC;
		break;
	case WORK_COMP_CONFLICT_CLEANUP:
		siq_error = E_SIQ_COMP_CONFLICT_CLEANUP;
		break;
	case WORK_COMP_CONFLICT_LIST:
		siq_error = E_SIQ_COMP_CONFLICT_LIST;
		break;
	case WORK_CT_COMP_DIR_LINKS:
		siq_error = E_SIQ_CT_COMP_DIR_LINKS;
		break;
	case WORK_COMP_COMMIT:
		siq_error = E_SIQ_COMP_COMMIT;
		break;
	case WORK_COMP_MAP_PROCESS:
		siq_error = E_SIQ_COMP_MAP_PROCESS;
		break;
	default:
		ASSERT(0, "Invalid work type %d", rt);
	}
	return siq_error;
}

bool
is_fsplit_work_item(struct work_restart_state *work) {
	ASSERT(work != NULL);
	return (work->fsplit_lin != 0);
}

static int
grab_work_item_lock(char *pol_id, char *lock_path, int lock_type, int timeout,
    struct isi_error **error_out)
{
	int fd = -1;
	time_t cur_time, end_time;
	struct isi_error *error = NULL;

	cur_time = time(0);
	end_time = cur_time + timeout;
	while (cur_time <= end_time) {
		fd = acquire_lock(lock_path, lock_type | O_NONBLOCK, &error);
		if (error) {
			if (isi_system_error_get_error(
			    (struct isi_system_error *)error) != EWOULDBLOCK) {
				goto out;
			}
			isi_error_free(error);
			error = NULL;

			if (timeout)
				siq_nanosleep(1, 0);
			cur_time = time(0);
			continue;
		}
		break;
	}

	if (fd < 0)
		error = isi_system_error_new(EWOULDBLOCK, "Failed to grab "
		    "work item lock for policy id %s after %d second timeout.",
		    pol_id, timeout);

out:
	isi_error_handle(error, error_out);
	return fd;

}

/* Pworkers should call this when they get a new work item. This grabs a shared
 * lock on the policy's work item lock file. A policy's work item lock file
 * should have 1 shared lock on it for each work item that is currently being
 * worked on. */
int
lock_work_item(char *pol_id, int timeout, struct isi_error **error_out)
{
	int fd = -1;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(lock_path, MAXPATHLEN, "%s/%s.lock", SIQ_WORK_LOCK_DIR, pol_id);

	fd = grab_work_item_lock(pol_id, lock_path, O_SHLOCK, timeout, &error);

	isi_error_handle(error, error_out);
	return fd;

}

/* Coordinators should call this before loading restart work items into memory.
 * This grabs an exclusive lock on the policy's work item lock file. If a
 * previous coordinator has died, there may be some dangling pworkers who have
 * not died yet. In such a case, the policy's work item lock file will
 * have >= 1 shared lock, causing this function to block until all dangling
 * pworkers have died. */
int
lock_all_work_items(char *pol_id, int timeout, struct isi_error **error_out)
{
	int fd = -1;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(lock_path, MAXPATHLEN, "%s/%s.lock", SIQ_WORK_LOCK_DIR, pol_id);

	fd = grab_work_item_lock(pol_id, lock_path, O_EXLOCK, timeout, &error);

	isi_error_handle(error, error_out);
	return fd;

}

/* This should be called when a policy is deleted. This will clean up the lock
 * file for the given policy. */
void
delete_work_item_lock(char *pol_id, struct isi_error **error_out)
{
	int ret;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(lock_path, MAXPATHLEN, "%s/%s.lock", SIQ_WORK_LOCK_DIR,
	    pol_id);

	ret = unlink(lock_path);
	if (ret == -1)
		error = isi_system_error_new(errno, "Failed to delete work item "
		    "lock file %s", lock_path);

	isi_error_handle(error, error_out);
}

/*
 * Sworkers should call this function when they receive a work item lock
 * message from the pworker with a boolean of true. This grabs an exclusive
 * lock on a work item's lock file for a specified policy.
 */
int
lock_work_item_target(char *pol_id, int wi_lock_mod, uint64_t wi_lin,
    int timeout, struct isi_error **error_out)
{
	int mod, fd = -1;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	mod = (wi_lin / WORK_LIN_RESOLUTION) % wi_lock_mod;

	snprintf(lock_path, sizeof(lock_path), "%s/%s/%d/%llx.lock",
	    SIQ_TARGET_WORK_LOCK_DIR, pol_id, mod, wi_lin);

	fd = grab_work_item_lock(pol_id, lock_path, O_EXLOCK, timeout, &error);

	isi_error_handle(error, error_out);
	return fd;

}

/*
 * Sworkers should call this function when they receive a work item lock
 * message from the pworker with a boolean of false. This releases the
 * exclusive lock on a work item's lock file for a specified policy. It will
 * also delete the work item's lock file.
 */
void
release_target_work_item_lock(int *wi_lock_fd, char *pol_id, int wi_lock_mod,
    uint64_t wi_lin, struct isi_error **error_out)
{
	int mod, ret;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	if (*wi_lock_fd < 0)
		goto out;

	close(*wi_lock_fd);
	*wi_lock_fd = -1;

	mod = (wi_lin / WORK_LIN_RESOLUTION) % wi_lock_mod;

	snprintf(lock_path, sizeof(lock_path), "%s/%s/%d/%llx.lock",
	    SIQ_TARGET_WORK_LOCK_DIR, pol_id, mod, wi_lin);

	ret = unlink(lock_path);
	if (ret == -1)
		error = isi_system_error_new(errno, "Failed to delete "
		    "work item lock file %s", lock_path);

out:
	isi_error_handle(error, error_out);
}

