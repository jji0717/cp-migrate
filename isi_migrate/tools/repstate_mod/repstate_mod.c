#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include <sys/stat.h>
#include <ifs/ifs_lin_open.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/treewalk.h"
#include "isi_migrate/migr/worklist.h"
#include "sbtree.h"

#define REPDIR "synciq/src"

void usage(void);

enum actions {
	ACT_INVALID = 0,
	ACT_LL = 'l'*256+'l',
	ACT_RC = 'r'*256+'c',
	ACT_RP = 'r'*256+'p',
	ACT_RK = 'r'*256+'k',
	ACT_RX = 'r'*256+'x',
	ACT_RA = 'r'*256+'a',
	ACT_RS = 'r'*256+'s',
	ACT_RM = 'r'*256+'m',
	ACT_RD = 'r'*256+'d',
	ACT_RR = 'r'*256+'r',
	ACT_RL = 'r'*256+'l',
	ACT_IX = 'i'*256+'x',
	ACT_IS = 'i'*256+'s',
	ACT_WX = 'w'*256+'x',
	ACT_WA = 'w'*256+'a',
	ACT_WS = 'w'*256+'s',
	ACT_WM = 'w'*256+'m',
	ACT_WD = 'w'*256+'d',
	ACT_WF = 'w'*256+'f',
	ACT_WT = 'w'*256+'t',
	ACT_WK = 'w'*256+'k',
	ACT_SS = 's'*256+'s',
	ACT_SA = 's'*256+'a',
	ACT_TL = 't'*256+'l',
	ACT_TA = 't'*256+'a',
	ACT_TS = 't'*256+'s',
	ACT_TC = 't'*256+'c',
	ACT_TD = 't'*256+'d',
	ACT_TK = 't'*256+'k'
};

enum rep_opts {
	OPT_LC = -2,
	OPT_NH = -3,
	OPT_ID = -4,
	OPT_IFC = -5,
	OPT_NDP = -6,
	OPT_NOT = -7,
	OPT_CH = -8,
	OPT_FL = -9,
	OPT_LCS = -10,
	OPT_FP = -11,
	OPT_XLC = -12,
	OPT_MAX = -13,
	OPT_EXCPT = -14,
	OPT_HC = -15,
	OPT_LCR = -16
};

static struct option longopts[] = {
	{"lcount", required_argument, NULL, OPT_LC},
	{"non_hashed", required_argument, NULL, OPT_NH},
	{"is_dir", required_argument, NULL, OPT_ID},
	{"exception", required_argument, NULL, OPT_EXCPT},
	{"incl_for_child", required_argument, NULL, OPT_IFC},
	{"hash_computed", required_argument, NULL, OPT_HC},
	{"new_dir_parent", required_argument, NULL, OPT_NDP},
	{"not_on_target", required_argument, NULL, OPT_NOT},
	{"changed", required_argument, NULL, OPT_CH},
	{"flipped", required_argument, NULL, OPT_FL},
	{"lcount_set", required_argument, NULL, OPT_LCS},
	{"for_path", required_argument, NULL, OPT_FP},
	{"excl_lcount", required_argument, NULL, OPT_XLC},
	{"lcount_reset", required_argument, NULL, OPT_LCR},
	{NULL, 0, NULL, 0}
};

enum sel_opts {
	OPT_PLIN = -2,
	OPT_NAME = -3,
	OPT_ENC = -4,
	OPT_INCL = -5,
	OPT_EXCL = -6,
	OPT_CHILD = -7,
};

static struct option selopts[] = {
	{"plin", required_argument, NULL, OPT_PLIN},
	{"name", required_argument, NULL, OPT_NAME},
	{"enc", required_argument, NULL, OPT_ENC},
	{"includes", required_argument, NULL, OPT_INCL},
	{"excludes", required_argument, NULL, OPT_EXCL},
	{"child", required_argument, NULL, OPT_CHILD},
	{NULL, 0, NULL, 0}
};

enum repinfo_opts {
	OPT_SNAP1 = -2,
	OPT_MODE = -3,
};

static struct option repinfoopts[] = {
	{"snap1", required_argument, NULL, OPT_SNAP1},
	{"mode", required_argument, NULL, OPT_MODE},
	{NULL, 0, NULL, 0}
};

enum work_opts {
	OPT_WIMINLIN = -2,
	OPT_WIMAXLIN = -3,
	OPT_CHECKPTLIN = -4,
	OPT_FSPLITLIN = -5,
	OPT_RT = -6
};

static struct option workitemopts[] = {
	{"min_lin", required_argument, NULL, OPT_WIMINLIN},
	{"max_lin", required_argument, NULL, OPT_WIMAXLIN},
	{"chkpt_lin", required_argument, NULL, OPT_CHECKPTLIN},
	{"fsplit_lin", required_argument, NULL, OPT_FSPLITLIN},
	{"rt", required_argument, NULL, OPT_RT},
	{NULL, 0, NULL, 0}
};

enum worklist_opts {
	OPT_WLWORKID = -2,
	OPT_WLLEVEL = -3,
	OPT_WLLIN = -4,
	OPT_WLDIRCOOKIE1 = -5,
	OPT_WLDIRCOOKIE2 = -6
};

static struct option worklistopts[] = {
	{"work_id", required_argument, NULL, OPT_WLWORKID},
	{"level", required_argument, NULL, OPT_WLLEVEL},
	{"lin", required_argument, NULL, OPT_WLLIN},
	{"dircookie1", required_argument, NULL, OPT_WLDIRCOOKIE1},
	{"dircookie2", required_argument, NULL, OPT_WLDIRCOOKIE2},
	{NULL, 0, NULL, 0}
};

static void
print_entry(uint64_t lin, struct rep_entry *entry)
{
	ASSERT(entry);
	printf("%llx lcount:%d non_hashed:%d is_dir:%d exception:%d "
	    "incl_for_child:%d hash_computed:%d new_dir_parent:%d "
	    "not_on_target:%d changed:%d flipped:%d lcount_set:%d "
	    "lcount_reset:%d for_path:%d excl_lcount:%d hl_dir_lin:%llx "
	    "hl_dir_offset:%llx\n",
	    lin, entry->lcount, entry->non_hashed, entry->is_dir,
	    entry->exception, entry->incl_for_child, entry->hash_computed,
	    entry->new_dir_parent, entry->not_on_target, entry->changed,
	    entry->flipped, entry->lcount_set, entry->lcount_reset, 
	    entry->for_path, entry->excl_lcount, entry->hl_dir_lin,
	    entry->hl_dir_offset);
}

static void
print_sel_entry(struct rep_ctx *ctx, uint64_t lin, struct sel_entry *entry)
{
	ASSERT(entry);
	printf("\n----------------------------\n");
	printf("LIN: %llx\n", lin);
	printf("Parent LIN: %llx\n", entry->plin);
	printf("Name: %s\n", entry->name);
	printf("Enc: %d\n", entry->enc);
	printf("Includes: %s\n", entry->includes ? "true" : "false");
	printf("Excludes: %s\n", entry->excludes ? "true" : "false");
	printf("For Child: %s\n", entry->for_child ? "true" : "false");
}


static void
print_work_entry(struct rep_ctx *ctx, uint64_t lin,
    struct isi_error **error_out)
{
	int i;
	char *tw_data = NULL;
	size_t tw_size = 0;
	bool exists;
	struct migr_tw treewalk;
	struct migr_dir *dir;
	struct isi_error *error = NULL;
	struct work_restart_state twork;

	exists = get_work_entry(lin, &twork, &tw_data, &tw_size, ctx, &error);
	if (error)
		goto out;
	if (!exists) {
		error = isi_system_error_new(ENOENT,
		    "Unable to read work item %llx", lin);
		goto out;
	}
	printf("\n----------------------------\n");
	printf("workitem key 0x%016llx\n", lin);
	printf("work->rt : %d\n", twork.rt);
	printf("work->lin : 0x%llx\n", twork.lin);
	printf("work->dircookie1 : 0x%016llx\n", twork.dircookie1);
	printf("work->dircookie2 : 0x%016llx\n", twork.dircookie2);
	printf("work->num1  : %u\n", twork.num1);
	printf("work->den1  : %u\n", twork.den1);
	printf("work->num2  : %u\n", twork.num2);
	printf("work->den2  : %u\n", twork.den2);
	printf("work->min_lin: 0x%llx\n", twork.min_lin);
	printf("work->max_lin: 0x%llx\n", twork.max_lin);
	printf("work->chkpt_lin: 0x%llx\n", twork.chkpt_lin);
	printf("work->wl_cur_level: %u\n", twork.wl_cur_level);
	printf("work->tw_blk_cnt: %u\n", twork.tw_blk_cnt);
	printf("work->f_low: %llu\n", twork.f_low);
	printf("work->f_high: %llu\n", twork.f_high);
	printf("work->fsplit_lin: %llx\n", twork.fsplit_lin);
	printf("work->num_coord_restarts: %u\n", twork.num_coord_restarts);
	if (twork.tw_blk_cnt > 0) {
		if (tw_data == NULL) {
			error = isi_system_error_new(EIO,
			    "Unable to read treewalk data");
			goto out;
		}
		migr_tw_init_from_data(&treewalk, tw_data,
		    tw_size, &error);
		if (error)
			goto out;
		if (pvec_size(&treewalk._dirs) <= 0) {
			error = isi_system_error_new(EINVAL,
			    "Invalid treewalk data");
			goto out;
		}
		printf("\n-----------TW Data-----------\n");
		printf("Path : %s\n", treewalk._path.path);
		i = 0;
		while (i < pvec_size(&treewalk._dirs)) {
			dir = treewalk._dirs.ptrs[i];
			printf("dir slice: lin: 0x%llx slice.begin: 0x%016llx "
			    "slice.end: 0x%016llx checkpoint: 0x%016llx\n",
			    dir->_stat.st_ino, dir->_slice.begin,
			    dir->_slice.end, dir->_checkpoint);
			i++;
		}
		migr_tw_clean(&treewalk);
	}
		
out:
	if (tw_data)
		free(tw_data);
	isi_error_handle(error, error_out);
}

static void
move_or_remove_workitem_helper(uint64_t dst_lin, uint64_t src_lin,
    struct rep_ctx *ctx, struct isi_error **error_out)
{
	char *tw_data = NULL;
	size_t tw_size = 0;
	bool exists;
	struct isi_error *error = NULL;
	struct work_restart_state twork = {};

	exists = get_work_entry(src_lin, &twork, &tw_data, &tw_size, ctx,
	    &error);
	if (error)
		goto out;
	if (!exists) {
		error = isi_system_error_new(ENOENT,
		    "Unable to read work item %llx", src_lin);
		goto out;
	}
	if (dst_lin) {
		set_work_entry(dst_lin, &twork, tw_data, tw_size, ctx, &error);
		if (error)
			goto out;
	}
	remove_work_entry(src_lin, &twork, ctx, &error);
	if (error)
		goto out;

out:
	if (tw_data)
		free(tw_data);
	isi_error_handle(error, error_out);
}

static bool
to_bool(const char *arg)
{
	bool res = false;
	
	if (strlen(arg) != 1)
		usage();
	if (*arg == '1')
		res = true;
	else if (*arg == '0')
		res = false;
	else
		usage();
	return res;
}

static void
set_sel_helper(uint64_t lin, struct rep_ctx *ctx, int argc, char **argv,
    bool modify_existing, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct sel_entry entry = {};
	bool found;
	int ret;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	/* if this is a modify operation, populate entry with existing values.
	 * if modify is specified and the entry doesn't exist, error out */
	if (modify_existing) {
		found = get_sel_entry(lin, &entry, ctx, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(EINVAL,
			    "entry does not exist for lin %llx", lin);
			goto out;
		}
	}
	
	/* skip to the flag/values and parse them. note that getopt_long needs
	 * to start on the argument preceding the first option flag. normally,
	 * this would be the name of the executable */ 
	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", selopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		errno = 0;
		switch (ret) {
		case OPT_PLIN:
			entry.plin = strtoll(optarg, NULL, 16);
			break;

		case OPT_NAME:
			strcpy(entry.name, optarg);
			break;

		case OPT_ENC:
			entry.enc = 0;
			break;

		case OPT_INCL:
			entry.includes = to_bool(optarg);
			break;

		case OPT_EXCL:
			entry.excludes = to_bool(optarg);
			break;

		case OPT_CHILD:
			entry.for_child = to_bool(optarg);
			break;
		default:
			ASSERT(0);
		}

		if (errno) {
			error = isi_system_error_new(errno,
			    "error processing arg %s", optarg);
			goto out;
		}
	}

	print_sel_entry(ctx, lin, &entry);
	/* store entry with mix of existing/default and modified values */
	set_sel_entry(lin, &entry, ctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
set_repinfo_helper(uint64_t lin, struct rep_ctx *ctx, int argc, char **argv,
    bool modify_existing, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_info_entry entry = {};
	bool found;
	int ret;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	/* if this is a modify operation, populate entry with existing values.
	 * if modify is specified and the entry doesn't exist, error out */
	if (modify_existing) {
		found = get_rep_info(&entry, ctx, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(EINVAL,
			    "entry does not exist for lin %llx", lin);
			goto out;
		}
	}
	
	/* skip to the flag/values and parse them. note that getopt_long needs
	 * to start on the argument preceding the first option flag. normally,
	 * this would be the name of the executable */ 
	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", repinfoopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		switch (ret) {
		case OPT_SNAP1:
			entry.initial_sync_snap = atoll(optarg);
			break;

		case OPT_MODE: entry.is_sync_mode = to_bool(optarg);
			break;

		default:
			ASSERT(0);
		}

	}

	/* store entry with mix of existing/default and modified values */
	set_rep_info(&entry, ctx, false, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
set_workitem_helper(uint64_t lin, struct rep_ctx *ctx, int argc, char **argv,
    bool modify_existing, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct work_restart_state twork = {};
	bool found;
	int ret, rt = -1;

	/* valid call will include at least one flag and value */
	if (argc < 7)
		usage();

	if (modify_existing) {
		found = get_work_entry(lin, &twork, NULL, 0, ctx, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(EINVAL,
			    "work item does not exist for lin %llx", lin);
			goto out;
		}
	} else
		allocate_entire_work(&twork, WORK_FINISH);

	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", workitemopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		errno = 0;
		switch (ret) {
		case OPT_WIMINLIN:
			twork.min_lin = strtoll(optarg, NULL, 16);
			break;

		case OPT_WIMAXLIN:
			twork.max_lin = strtoll(optarg, NULL, 16);
			break;

		case OPT_CHECKPTLIN:
			twork.chkpt_lin = strtoll(optarg, NULL, 16);
			break;

		case OPT_FSPLITLIN:
			twork.fsplit_lin = strtoll(optarg, NULL, 16);
			break;

		case OPT_RT:
			rt = atoi(optarg);
			if (rt < WORK_SNAP_TW || rt > WORK_FINISH)
				usage();
			twork.rt = rt;
			break;

		default:
			ASSERT(0);
		}

		if (errno) {
			error = isi_system_error_new(errno,
			    "error processing arg %s", optarg);
			goto out;
		}
	}
	/* --rt is required when creating a new work entry */
	if (!modify_existing && rt == -1)
		usage();

	set_work_entry(lin, &twork, NULL, 0, ctx, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
set_helper(uint64_t lin, struct rep_ctx *ctx, int argc, char **argv,
    bool modify_existing, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry entry = {};
	bool found;
	int ret;

	/* valid call will include at least one flag and value */
	if (argc < 6)
		usage();

	/* if this is a modify operation, populate entry with existing values.
	 * if modify is specified and the entry doesn't exist, error out */
	if (modify_existing) {
		found = get_entry(lin, &entry, ctx, &error);
		if (error)
			goto out;
		if (!found) {
			error = isi_system_error_new(EINVAL,
			    "entry does not exist for lin %llx", lin);
			goto out;
		}
	}
	
	/* skip to the flag/values and parse them. note that getopt_long needs
	 * to start on the argument preceding the first option flag. normally,
	 * this would be the name of the executable */ 
	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", longopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		switch (ret) {
		case OPT_LC:
			entry.lcount = strtoul(optarg, NULL, 10);
			if (entry.lcount == 0 && errno == EINVAL) {
				error = isi_system_error_new(EINVAL,
				    "bad argument %s", optarg);
				goto out;
			}
			break;

		case OPT_NH:
			entry.non_hashed = to_bool(optarg);
			break;

		case OPT_ID:
			entry.is_dir = to_bool(optarg);
			break;

		case OPT_IFC:
			entry.incl_for_child = to_bool(optarg);
			break;

		case OPT_HC:
			entry.hash_computed = to_bool(optarg);
			break;

		case OPT_EXCPT:
			entry.exception = to_bool(optarg);
			break;

		case OPT_NDP:
			entry.new_dir_parent = to_bool(optarg);
			break;

		case OPT_NOT:
			entry.not_on_target = to_bool(optarg);
			break;

		case OPT_CH:
			entry.changed = to_bool(optarg);
			break;

		case OPT_FL:
			entry.flipped = to_bool(optarg);
			break;

		case OPT_LCS:
			entry.lcount_set = to_bool(optarg);
			break;

		case OPT_FP:
			entry.for_path = to_bool(optarg);
			break;

		case OPT_XLC:
			entry.excl_lcount = strtoul(optarg, NULL, 10);
			if (entry.lcount == 0 && errno == EINVAL) {
				error = isi_system_error_new(EINVAL,
				    "bad argument %s", optarg);
				goto out;
			}
			break;

		case OPT_LCR:
			entry.lcount_reset = to_bool(optarg);
			break;

		default:
			ASSERT(0);
		}

	}

	/* store entry with mix of existing/default and modified values */
	set_entry(lin, &entry, ctx, 0, NULL, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

static void
print_btree_entries(int fd, char *type, char *pol_id)
{
	DIR *dir = NULL;
	struct dirent *de = NULL;
	bool one_or_more = false;

	dir = fdopendir(fd);
	if (dir == NULL) {
		printf("error opening %s dir%s%s\n", type,
		    pol_id ? " for policy " : "", pol_id ? : "");
		goto out;
	}
	/* fdopendir() takes ownership of dir_fd on success. */
	fd = -1;

	for (;;) {
		de = readdir(dir);
		if (de == NULL)
			break;

		if (!strcmp(de->d_name, ".") ||
		    !strcmp(de->d_name, "..") ||
		    !strcmp(de->d_name, ".snapshot"))
			continue;

		printf("%s\n", de->d_name);
		one_or_more = true;
	}

	if (one_or_more == false) {
		if (pol_id)
			printf("no repstates found in repstate dir for policy "
			    "%s\n", pol_id);
		else
			printf("no repstate dirs found\n");
	}

out:
	if (fd != -1)
		close(fd);
	if (dir != NULL)
		closedir(dir);
}

static void
list_repstates(char *pol_id)
{
	int root_fd = -1;
	int dir_fd = -1;
	struct isi_error *error = NULL;

	if (pol_id) {
		/* list reps in a specific policy directory */
		dir_fd = siq_btree_open_dir(siq_btree_repstate, pol_id,
		    &error);
		if (error) {
			printf("%s\n", isi_error_get_message(error));
			isi_error_free(error);
			goto out;
		}
		ASSERT(dir_fd >= 0);
	} else {
		/* list policy directories */
		root_fd = sbt_open_root_dir(O_RDONLY);
		if (root_fd == -1)
			goto out;

		dir_fd = enc_openat(root_fd, REPDIR, ENC_DEFAULT, O_RDONLY);
		if (dir_fd == -1) {
			printf("synciq repstate directory does not exist\n");
			goto out;
		}
	}

	print_btree_entries(dir_fd, "repstate", pol_id);

out:
	if (root_fd != -1)
		close(root_fd);
}

static void
list_worklists(char *pol_id)
{
	int dir_fd = -1;
	struct isi_error *error = NULL;

	if (!pol_id) {
		printf("policy id is required\n");
		return;
	}
	
	/* list worklists in a specific policy directory */
	dir_fd = siq_btree_open_dir(siq_btree_worklist, pol_id, &error);
	if (error) {
		printf("%s\n", isi_error_get_message(error));
		isi_error_free(error);
		return;
	}
	ASSERT(dir_fd >= 0);

	print_btree_entries(dir_fd, "worklist", pol_id);
}

static void
print_work_state(struct rep_ctx *ctx)
{
	struct stf_job_state job_phase = {};
	char *jbuf = NULL;
	size_t jsize;
	int i, n;
	struct isi_error *error = NULL;

	jsize = job_phase_max_size();
	jbuf = malloc(jsize);
	ASSERT(jbuf);
	memset(jbuf, 0, jsize);
	get_state_entry(STF_JOB_PHASE_LIN, jbuf, &jsize, ctx, &error);
	if (error) {
		printf("Unable to get state entry: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		goto out;
	}
	read_job_state(&job_phase, jbuf, jsize);
	printf("\n----------------------------\n");
	printf("jtype: %d\n", job_phase.jtype);
	printf("cur_phase: %d\n", job_phase.cur_phase);
	printf("states:\n");
	n = get_total_phase_num(job_phase.jtype);
	for (i = 0; i < n; i++) {
		printf("  %i: stf_phase: %d enabled: %d\n", i,
		    job_phase.states[i].stf_phase, job_phase.states[i].enabled);
	}
out:
	free(jbuf);
	return;
}

void
usage(void)
{
	fprintf(stderr,
"\n"
"usage:\n"
"    isi_repstate_mod -ll                          (list rep directories)\n"
"    isi_repstate_mod -ll pol_id                   (list reps in a directory)\n"
"\n"
"repstate:\n"
"    isi_repstate_mod -rc pol_id rep_name          (create a new repstate)\n"
"    isi_repstate_mod -rp pol_id rep_name          (reprotect repstate)\n"
"    isi_repstate_mod -rk pol_id rep_name          (kill a repstate)\n"
"    isi_repstate_mod -rx pol_id rep_name lin      (print entry)\n"
"    isi_repstate_mod -ra pol_id rep_name          (print all entries)\n"
"    isi_repstate_mod -rs pol_id rep_name lin -x y (set entry)\n"
"    isi_repstate_mod -rm pol_id rep_name lin -x y (modify entry)\n"
"    isi_repstate_mod -rd pol_id rep_name lin      (delete entry)\n"
"    isi_repstate_mod -rr pol_id rep_name low high (print inclusive range)\n"
"    isi_repstate_mod -rl pol_id rep_name low high snapid (check lcount)\n"
"\n"
"repstate info:\n"
"    isi_repstate_mod -ix pol_id rep_name          (print repinfo entry)\n"
"    isi_repstate_mod -is pol_id rep_name lin -x y (set repinfo entry)\n"
"\n"
"work item:\n"
"    isi_repstate_mod -wx pol_id rep_name key      (print work entry)\n"
"    isi_repstate_mod -wa pol_id rep_name          (print all work entries)\n"
"    isi_repstate_mod -wt pol_id rep_name          (print work state)\n"
"    isi_repstate_mod -ws pol_id rep_name key -x y (set work entry)\n"
"    isi_repstate_mod -wm pol_id rep_name key -x y (modify work entry)\n"
"    isi_repstate_mod -wd pol_id rep_name key      (delete work entry)\n"
"    isi_repstate_mod -wf pol_id rep_name          (defragment work entries)\n"
"    isi_repstate_mod -wk pol_id rep_name          (update split checkpoint)\n"
"\n"
"work list:\n"
"    isi_repstate_mod -tl pol_id                        (list all work lists)\n"
"    isi_repstate_mod -tl pol_id wl_name                (list work lists)\n"
"    isi_repstate_mod -ta pol_id wl_name wlin level     (print all work list entries)\n"
"    isi_repstate_mod -ts pol_id wl_name -x y           (set work list entry)\n"
"    isi_repstate_mod -tc pol_id wl_name                (create work list entry)\n"
"    isi_repstate_mod -td pol_id wl_name wlin lin level (delete work list entry)\n"
"    isi_repstate_mod -tk pol_id wl_name                (kill work list)\n"
"\n"
"select tree:\n"
"    isi_repstate_mod -ss pol_id rep_name lin -x y (set sel entry)\n"
"    isi_repstate_mod -sa pol_id rep_name          (print sel tree)\n"
"\n"
"For repstate set and modify operations, lin is followed by one or more \n"
"flag/val pairs where flag is one of the following:\n"
"    --lcount         (integer)\n"
"    --non_hashed     (bool)\n"
"    --is_dir         (bool)\n"
"    --incl_for_child (bool)\n"
"    --hash_computed  (bool)\n"
"    --exception      (bool)\n"
"    --new_dir_parent (bool)\n"
"    --not_on_target  (bool)\n"
"    --changed        (bool)\n"
"    --flipped        (bool)\n"
"    --lcount_set     (bool)\n"
"    --for_path       (bool)\n"
"    --excl_lcount    (integer)\n"
"    --lcount_reset   (bool)\n"
"and value is an appropriate value for the field (0 or 1 for bool). for set\n"
"operations, unspecified fields are set to 0 or false. for mod operations,\n"
"unspecified fields retain the existing value. attempting to mod a non-\n"
"existing entry will result in failure.\n"
"\n"
"example:\n"
"    isi_repstate_mod pol_snap_3 -s 1001b0008 --lcount 15 --is_dir 0 --changed 1\n"
"\n"
"For select entry, the following flags are available:\n"
"    --plin           (hexadecimal integer)\n"
"    --name           (string)\n"
"    --includes       (bool)\n"
"    --excludes       (bool)\n"
"    --child          (bool)\n"
"\n"
"For repinfo entry (in repstate), the following flags are available:\n"
"    --snap1          (integer)\n"
"    --mode           (bool)\n"
"\n"
"For workitem entry, the following flags are avaliable:\n"
"    --min_lin        (hexadecimal integer)\n"
"    --max_lin        (hexadecimal integer)\n"
"    --chkpt_lin      (hexadecimal integer)\n"
"    --fsplit_lin     (hexadecimal integer)\n"
"    --rt             (integer)\n"
"\n"
"For worklist entry, the following flags are avaliable:\n"
"    --work_id        (decimal integer)\n"
"    --level          (decimal integer)\n"
"    --lin            (hexadecimal integer)\n"
"    --dircookie1     (hexadecimal integer)\n"
"    --dircookie2     (hexadecimal integer)\n"
"\n");
	exit(EXIT_FAILURE);
}

static void
rep_log_add(struct rep_log *logp, struct rep_ctx *ctx,
    u_int64_t lin, struct rep_entry *old_entry, struct rep_entry *new_entry,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	logp->entries[logp->index].oper = SBT_SYS_OP_MOD;
	logp->entries[logp->index].set_value = false;
	logp->entries[logp->index].old_entry = *old_entry;

	logp->entries[logp->index].lin = lin;
	logp->entries[logp->index].new_entry = *new_entry;
	print_entry(lin, new_entry);
	logp->index++;
	if (logp->index == MAX_BLK_OPS) {
		flush_rep_log(logp, ctx, &error);
		if (error)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
print_worklist_entry(btree_key_t *key, uint64_t wllin,
    unsigned int wllevel, struct wl_entry *wlentry)
{
	uint64_t wlid = 0;

	wlid = get_wi_lin_from_btree_key(key);
	printf("Worklist entry:\n");
	printf("  Worklist ID: %lld\n", wlid);
	printf("  Worklist Level: %d\n", wllevel);
	printf("  Worklist LIN: %llx\n", wllin);

	if (wlentry) {
		printf("  Checkpoint:\n");
		printf("    Dircookie1: %llx\n", wlentry->dircookie1);
		printf("    Dircookie2: %llx\n", wlentry->dircookie2);
	}
}

static void
set_worklist_helper(int argc, char **argv, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int ret;
	struct wl_ctx *wlctx = NULL;
	unsigned int wlid = 0;
	unsigned int wllevel = 0;
	uint64_t wllin = 0;
	bool dircookie1 = false;
	bool dircookie2 = false;
	struct wl_entry wlentry = {};
	struct wl_log wllog = {};

	/* valid call will include at least one flag and value */
	if (argc < 7)
		usage();

	open_worklist(argv[2], argv[3], &wlctx, &error);
	if (error) {
		printf("invalid worklist %s %s specified\n", argv[2], argv[3]);
		goto out;
	}

	argv += 3;
	argc -= 3;
	while (true) {
		ret = getopt_long(argc, argv, "", worklistopts, NULL);
		if (ret == ':' || ret == '?')
			usage();
		if (ret == -1)
			break;

		switch (ret) {
		case OPT_WLWORKID:
			wlid = strtoul(optarg, NULL, 10);
			break;

		case OPT_WLLEVEL:
			wllevel = strtoul(optarg, NULL, 10);
			break;

		case OPT_WLLIN:
			wllin = strtoll(optarg, NULL, 16);
			break;

		case OPT_WLDIRCOOKIE1:
			wlentry.dircookie1 = strtoll(optarg, NULL, 16);
			dircookie1 = true;
			break;

		case OPT_WLDIRCOOKIE2:
			wlentry.dircookie2 = strtoll(optarg, NULL, 16);
			dircookie2 = true;
			break;

		default:
			ASSERT(0);
		}

		if (errno) {
			error = isi_system_error_new(errno,
			    "error processing arg %s", optarg);
			goto out;
		}
	}

	if (dircookie1 != dircookie2) {
		printf("Both dircookies must be set or unset\n");
		goto out;
	}

	if (dircookie1) {
		ret = raw_wl_checkpoint(wlctx->wl_fd, wlid, wllevel, wllin,
		    wlentry.dircookie1, wlentry.dircookie2);
		if (ret != 0) {
			printf("Failed to write worklist item with checkpoint\n");
			goto out;
		}

	} else {
		//Compensate for the fact that new items are written to level+1
		wl_log_add_entry_1(&wllog, wlctx, wlid, wllevel, wllin, &error);
		if (error)
			goto out;
		flush_wl_log(&wllog, wlctx, wlid, wllevel-1, &error);
		if (error)
			goto out;
	}

out:
	if (wlctx)
		close_worklist(wlctx);

	isi_error_handle(error, error_out);
}

int
main(int argc, char **argv)
{
	struct rep_entry entry, new_entry;
	struct rep_ctx *ctx = NULL, dummy_ctx = {};
	uint64_t lin = 0, tmp_lin = 0;
	uint64_t low, high;
	char c, d;
	char *pol_id = NULL;
	char *rep_name = NULL;
	bool found;
	struct isi_error *error = NULL;
	struct rep_iter *iter;
	struct work_restart_state work;
	struct rep_info_entry rie;
	struct sel_entry sentry;
	enum actions action = ACT_INVALID;
	int fd = -1;
	struct stat st;
	struct rep_log logp;
	ifs_snapid_t snapid;
	struct wl_iter *wliter = NULL;
	struct wl_ctx *wlctx = NULL;
	btree_key_t wlbtkey = {};
	uint64_t wlid = 0;
	uint64_t wllin = 0;
	unsigned int wllevel = 0;
	struct wl_entry *wlentry = NULL;
	struct wl_log wllog = {};

	if (argc < 2)
		usage();

	if (sscanf(argv[1], "-%c%c", &c, &d) != 2)
		usage();
	action = c * 256 + d;

	if (argc >= 3)
		pol_id = argv[2];

	if (c != 't') {
		if (argc >= 4) {
			rep_name = argv[3];
			open_repstate(pol_id, rep_name, &ctx, &error);
			if (error && action != ACT_RC) {
				printf("invalid pol_id %s or map_name %s "
				    "specified\n", pol_id, rep_name);
				isi_error_free(error);
				error = NULL;
				goto out;
			}
			error = NULL;
		}

		if (argc >= 5 && (sscanf(argv[4], "%llx", &lin) != 1)) {
			printf("invalid argument %s specified\n", argv[3]);
			goto out;
		}
	}

	/* 
	 * Add new fields to work entries. We can run into problems if the
	 * customer upgrades to a new version and uses this tool before on
	 * disk work items have been upgraded (by the coordinator).
	 */
	if (ctx) {
		upgrade_work_entries(ctx, &error);
		if (error)
			goto out;
	} else {
		/* Placate static analysis. */
		ctx = &dummy_ctx;
	}

	switch (action) {
	case ACT_LL:
		if (argc > 3)
			usage();
		list_repstates(pol_id);
		break;

	case ACT_RC:
		if (argc != 4)
			usage();
		if (ctx != NULL && ctx != &dummy_ctx) {
			printf("repstate %s already exists\n", rep_name);
			goto out;
		}
		create_repstate(pol_id, rep_name, false, &error);
		if (error) {
			printf("failed to create repstate %s\n", rep_name);
			goto out;
		}
		break;

	case ACT_RP:
		if (argc != 4)
			usage();
		if (ctx != NULL && ctx != &dummy_ctx) {
			close_repstate(ctx);
			ctx = NULL;
			create_repstate(pol_id, rep_name, true, &error);
			if (error) {
				printf("failed to reprotect repstate %s\n",
				    rep_name);
				goto out;
			}
		}
		break;

	case ACT_SA:
		if (argc != 4)
			usage();
		iter = new_rep_iter(ctx);
		while (true) {
			found = sel_iter_next(iter, &lin, &sentry, &error);
			if (error || !found)
				break;
			print_sel_entry(ctx, lin, &sentry);
		}
		close_rep_iter(iter);
		break;

	case ACT_SS:
		set_sel_helper(lin, ctx, argc, argv, false, &error);
		if (error)
			goto out;
		break;
		
	case ACT_IS:
		set_repinfo_helper(lin, ctx, argc, argv, false, &error);
		if (error)
			goto out;
		break;

	case ACT_RK:
		if (argc != 4)
			usage();
		remove_repstate(pol_id, rep_name, false, &error);
		if (error)
			goto out;
		break;

	case ACT_RX:
		if (argc != 5)
			usage();
		found = get_entry(lin, &entry, ctx, &error);
		if (error)
			goto out;
		if (found)
			print_entry(lin, &entry);
		else
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_WX:
		if (argc != 5)
			usage();
		found = entry_exists(lin, ctx, &error);
		if (error)
			goto out;
		if (!found || (lin % WORK_LIN_RESOLUTION) != 0) {
			printf("entry for work %llx does not exist\n", lin);
			goto out;
		}

		print_work_entry(ctx, lin, &error);
		if (error)
			goto out;
		break;

	case ACT_RS:
		set_helper(lin, ctx, argc, argv, false, &error);
		if (error)
			goto out;
		break;

	case ACT_RM:
		set_helper(lin, ctx, argc, argv, true, &error);
		if (error)
			goto out;
		break;

	case ACT_WS:
		set_workitem_helper(lin, ctx, argc, argv, false, &error);
		if (error)
			goto out;
		break;

	case ACT_WM:
		set_workitem_helper(lin, ctx, argc, argv, true, &error);
		if (error)
			goto out;
		break;

	case ACT_WD:
		if (argc != 5)
			usage();
		move_or_remove_workitem_helper(0, lin, ctx, &error);
		if (error)
			goto out;
		break;

	case ACT_RD:
		if (argc != 5)
			usage();
		found = remove_entry(lin, ctx, &error);
		if (!found)
			printf("entry for lin %llx does not exist\n", lin);
		break;

	case ACT_RA:
		if (argc != 4)
			usage();
		iter = new_rep_iter(ctx);
		while (true) {
			found = rep_iter_next(iter, &lin, &entry, &error);
			if (error || !found)
				break;
			print_entry(lin, &entry);
		}
		close_rep_iter(iter);
		break;

	case ACT_WA:
		if (argc != 4)
			usage();
		iter = new_rep_iter(ctx);
		while (true) {
			found = work_iter_next(iter, &lin, &work, &error);
			if (error || !found)
				break;
			print_work_entry(ctx, lin, &error);
			if (error)
				goto out;
		}
		close_rep_iter(iter);
		break;

	case ACT_WF:
		if (argc != 4)
			usage();
		iter = new_rep_iter(ctx);
		tmp_lin = WORK_RES_RANGE_MIN;
		while (true) {
			found = work_iter_next(iter, &lin, &work, &error);
			if (error || !found)
				break;
			if (lin > tmp_lin) {
				move_or_remove_workitem_helper(tmp_lin, lin,
				    ctx, &error);
				if (error)
					goto out;
				printf("Moved work item %llx to work item "
				    "%llx.\n", lin, tmp_lin);
			} else {
				printf("Left work item %llx in place.\n",lin);
				/* if (lin < tmp_lin)
				 * don't increment the tmp_lin */
				if (lin < tmp_lin)
					continue;
			}
			do {
				tmp_lin += WORK_LIN_RESOLUTION;
				/* short-circuit if tmp_lin reaches
				 * WORK_RES_RANGE_MAX */
				if (tmp_lin >= WORK_RES_RANGE_MAX) {
					close_rep_iter(iter);
					goto out;
				}
				found = entry_exists(tmp_lin, ctx, &error);
				if (error)
					goto out;
			} while (found);
		}
		close_rep_iter(iter);
		break;

	case ACT_WK:
		if (argc != 4)
			usage();
		iter = new_rep_iter(ctx);
		while (true) {
			found = work_iter_next(iter, &lin, &work, &error);
			if (error || !found)
				break;
			if (work.fsplit_lin != 0 &&
			    work.fsplit_lin != work.chkpt_lin) {
				tmp_lin = work.chkpt_lin;
				work.chkpt_lin = work.fsplit_lin;
				set_work_entry(lin, &work, NULL, 0, ctx, &error);
				if (error)
					goto out;
				printf("Updated work item %llx: "
				    "chkpt_lin %llx => %llx\n", lin,
				    tmp_lin, work.fsplit_lin);
			}
		}
		close_rep_iter(iter);
		break;

	case ACT_RR:
		if (argc != 6)
			usage();
		iter = new_rep_iter(ctx);
		low = lin;
		if (sscanf(argv[5], "%llx", &high) != 1) {
			printf("invalid argument %s specified\n", argv[5]);
			goto out;
		}
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		while (true) {
			found = rep_iter_next(iter, &lin, &entry, &error);
			if (error || !found || lin > high)
				break;
			print_entry(lin, &entry);
		}
		close_rep_iter(iter);
		break;

	case ACT_RL:
		if (argc != 7)
			usage();
		iter = new_rep_iter(ctx);
		low = lin;
		if (sscanf(argv[5], "%llx", &high) != 1) {
			printf("invalid argument %s specified\n", argv[5]);
			goto out;
		}
		if (sscanf(argv[6], "%llu", &snapid) != 1) {
			printf("invalid argument %s specified\n", argv[6]);
			goto out;
		}
		iter->key.keys[0] = low;
		iter->key.keys[1] = 0;
		bzero(&logp, sizeof(struct rep_log));
		while (true) {
			found = rep_iter_next(iter, &lin, &entry, &error);
			if (error || !found || lin > high)
				break;
			new_entry = entry;
			if (entry.lcount > 0) {
				found = false;
				ifs_check_in_snapshot(lin, snapid, 0,
				    &found);
				if (!found) {
					    new_entry.lcount = 0;
					    new_entry.lcount_reset = 1;
					    new_entry.changed = 1;
					    rep_log_add(&logp, ctx, lin, &entry,
						&new_entry, &error);
					    if (error)
						    break;
					    continue;
				}
				fd = ifs_lin_open(lin, snapid, O_RDONLY);
				if (fd <= 0) {
					if (errno == ENOENT) {
						new_entry.lcount = 0;
						new_entry.lcount_reset = 1;
						new_entry.changed = 1;
						rep_log_add(&logp, ctx, lin, &entry,
						    &new_entry, &error);
						if (error)
							break;
						continue;
					} else {
						printf("open error for lin "
						    "%llx::%llu: %s\n", lin,
						    snapid, strerror(errno));
						break;
					}
				}

				if (fstat(fd, &st) == -1) {
					printf("fstat error for lin "
					    "%llx::%llu: %s\n", lin, snapid,
					    strerror(errno));
					close(fd);
					fd = -1;
					break;
				} else if (S_ISREG(st.st_mode) &&
				    st.st_nlink < entry.lcount) {
					new_entry.lcount = st.st_nlink;
					new_entry.changed = 1;
					rep_log_add(&logp, ctx, lin, &entry,
					    &new_entry, &error);
				}
				if (fd > 0) {
					close(fd);
					fd = -1;
				}
			}
		}
		if (logp.index)
			flush_rep_log(&logp, ctx, &error);
		close_rep_iter(iter);
		break;

	case ACT_IX:
		if (argc != 4)
			usage();
		get_rep_info(&rie, ctx, &error);
		if (error)
			goto out;
		printf("Repstate info snap: %llu is_sync_mode: %u\n",
		    rie.initial_sync_snap, (unsigned)rie.is_sync_mode);
		break;

	case ACT_WT:
		if (argc != 4)
			usage();
		print_work_state(ctx);
		break;

	case ACT_TL:
		if (argc > 4)
			usage();
		if (argc == 4) {
			open_worklist(pol_id, argv[3], &wlctx, &error);
			if (error) {
				printf("invalid worklist %s %s specified\n",
				    pol_id, argv[3]);
				goto out;
			}
			set_wl_key(&wlbtkey, wlid, wllevel, 0);
			wliter = new_wl_iter(wlctx, &wlbtkey);
			found = wl_iter_raw_next(NULL, wliter, &wllin,
			    &wllevel, &wlentry, &error);
			while (found) {
				print_worklist_entry(&wliter->current_key,
				    wllin, wllevel, wlentry);
				//Iterate
				wllin = 0;
				wllevel = 0;
				free(wlentry);
				wlentry = NULL;
				found = wl_iter_raw_next(NULL, wliter,
				    &wllin, &wllevel, &wlentry, &error);
			}
			if (error)
				goto out;
			printf("End of worklist entries\n");
			break;
		} else {
			list_worklists(pol_id);
		}
		break;

	case ACT_TA:
		if (argc != 6)
			usage();
		open_worklist(pol_id, argv[3], &wlctx, &error);
		if (error) {
			printf("invalid worklist %s %s specified\n",
			    pol_id, argv[3]);
			goto out;
		}
		if (sscanf(argv[4], "%lld", &wlid) != 1) {
			printf("invalid id specified");
			goto out;
		}
		if (sscanf(argv[5], "%d", &wllevel) != 1) {
			printf("invalid level specified");
			goto out;
		}
		set_wl_key(&wlbtkey, wlid, wllevel, 0);
		wliter = new_wl_iter(wlctx, &wlbtkey);
		found = wl_iter_next(NULL, wliter, &wllin, &wllevel,
		    &wlentry, &error);
		while (found) {
			print_worklist_entry(&wliter->current_key, wllin,
			    wllevel, wlentry);
			//Iterate
			wllin = 0;
			wllevel = 0;
			free(wlentry);
			wlentry = NULL;
			found = wl_iter_next(NULL, wliter, &wllin, &wllevel,
			    &wlentry, &error);
		}
		if (error)
			goto out;
		printf("End of worklist entries\n");
		break;

	case ACT_TS:
		set_worklist_helper(argc, argv, &error);
		if (error)
			goto out;
		break;

	case ACT_TC:
		if (argc != 4)
			usage();
		create_worklist(pol_id, argv[3], false, &error);
		if (error)
			goto out;
		break;

	case ACT_TD:
		if (argc != 7)
			usage();
		open_worklist(pol_id, argv[3], &wlctx, &error);
		if (error) {
			printf("invalid worklist %s %s specified\n",
			    pol_id, argv[3]);
			goto out;
		}
		if (sscanf(argv[4], "%lld", &wlid) != 1) {
			printf("invalid id specified");
			goto out;
		}
		if (sscanf(argv[5], "%lld", &wllin) != 1) {
			printf("invalid lin specified");
			goto out;
		}
		if (sscanf(argv[6], "%d", &wllevel) != 1) {
			printf("invalid level specified");
			goto out;
		}
		set_wl_key(&wlbtkey, wlid, wllevel, wllin);
		wl_log_del_entry(&wllog, wlctx, wlbtkey, &error);
		if (error) {
			printf("failed to wl_log_delete_entry\n");
			goto out;
		}
		flush_wl_log(&wllog, wlctx, wlid, wllevel, &error);
		if (error) {
			printf("failed to flush_wl_log\n");
			goto out;
		}
		break;

	case ACT_TK:
		if (argc != 4)
			usage();
		remove_worklist(pol_id, argv[3], true, &error);
		if (error) {
			goto out;
		}
		break;

	default:
		usage();
	}

out:
	if (error) {
		printf("%s", isi_error_get_message(error));
		isi_error_free(error);
	}
	if (ctx != &dummy_ctx) {
		close_repstate(ctx);
	}
	if (wlctx) {
		close_worklist(wlctx);
	}
}
