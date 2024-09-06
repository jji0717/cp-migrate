#include <stdio.h>
#include <limits.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/param.h>
#include <sys/types.h>
#include <errno.h>
#include <md5.h>
#include <ctype.h>

#include <ifs/ifs_lin_open.h>
#include <ifs/ifs_types.h>
#include <isi_util/isi_assert.h>
#include <isi_upgrade_api/isi_upgrade_api.h>

#include "coord.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_policy.h"
#include "isi_migrate/sched/sched_utils.h"

#define TIME_FMT "%Y-%m-%d::%H:%M:%S"

#define SNAPSHOT_LEASE_RENEW 3
#define SNAPSHOT_LEASE_LENGTH 15
#define MIN_POLICIES_POL_INTERVAL 30

struct siq_gc_conf *global_config = NULL;
struct siq_gc_spec *gc_job_spec;
struct siq_gc_job_summary *gc_job_summ;

static int
_strcmp(const void *_a, const void *_b)
{
	const char *a = strchr(*(char **)_a, '/'),
		   *b = strchr(*(char **)_b, '/');
	return strcmp(a, b);
}

/*  ____  _      _   _                              
 * |  _ \(_) ___| |_(_) ___  _ __   __ _ _ __ _   _ 
 * | | | | |/ __| __| |/ _ \| '_ \ / _` | '__| | | |
 * | |_| | | (__| |_| | (_) | | | | (_| | |  | |_| |
 * |____/|_|\___|\__|_|\___/|_| |_|\__,_|_|   \__, |
 *                                            |___/ 
 */

void toLowerStr(char *str);
int dFindPos(char *str, const char *dic[]);

void toLowerStr(char *str)
{
	int i = 0;
	if(str == NULL)
		return;
	while (str[i] != 0)	{
		str[i] = tolower(str[i]);
		i++;
	}
}

int dFindPos(char * str, const char *dic[])
{
	int i = 0;
	if (str == NULL)
		return -1;
	toLowerStr(str);
	while (dic[i] != NULL) {
		if(strcmp(str, dic[i]) == 0)
			return i;
		i++;
	}
	return -1;
}

/** 
 * Spec macro expansion; belongs to config unit
 */
#define DAISY_MACRO(name) "%{" #name "}"
static char *daisy_macro[] = {
	DAISY_MACRO(SrcCluster),
	DAISY_MACRO(PolicyName),
	NULL
};

static char *daisy_macro_exp[2];

static int
init_macros(const char *policy_name) {
	char name[MAXHOSTNAMELEN];
	char *dash;

	if (!policy_name) {
		return EINVAL;
	}
	if (gethostname(name, sizeof(name) - 1) < 0) {
		return errno;
	}
	if ((dash = strrchr(name, '-')) != NULL)
		*dash = '\0';
	else
		return EINVAL;

	daisy_macro_exp[0] = strdup(name);
	ASSERT(daisy_macro_exp[0]);

	daisy_macro_exp[1] = strdup(policy_name);
	ASSERT(daisy_macro_exp[1]);

	return 0;
}

static char *
subst(char **buf, char *where, char *what, char *with) {
	char *p;
	p = malloc(strlen(*buf) - strlen(what) + strlen(with) + 1);
	ASSERT(p);

	strncpy(p, *buf, where - *buf);
	strcpy(p + (where - *buf), with); 
	strcpy(p + (where - *buf) + strlen(with), where + strlen(what));
	free(*buf); *buf = p;
	return p;
}

static char *
expand(char **entry) {
	char **p, *where = NULL;
	int found = 1;

	while (entry && *entry && found) {
		for (found = 0, p = &daisy_macro[0]; *p; p++) {
			if ((found = ((where = strstr(*entry, *p)) != NULL))) {
				break;
			}
		}
		if (found && !subst(entry, where, *p, *(&daisy_macro_exp[0] + 
						 (p - &daisy_macro[0])))) {
			found = 0;
		}
	}
	return entry ? *entry : NULL;
}

/*
 *  Expands instances of %{SrcCluster} or %{PolicyName} in entry.
 *  Note this will modify entry when called.
 */
char *
expand_snapshot_patterns(char **entry, const char *policy_name,
    const char *src_cluster_name)
{
	char *entry_ptr = NULL;
	char *old_policy_name = NULL;
	char *old_cluster_name = NULL;

	if (!policy_name || !src_cluster_name)
		return entry_ptr;

	old_cluster_name = daisy_macro_exp[0];
	daisy_macro_exp[0] = strdup(src_cluster_name);
	ASSERT(daisy_macro_exp[0]);

	old_policy_name = daisy_macro_exp[1];
	daisy_macro_exp[1] = strdup(policy_name);
	ASSERT(daisy_macro_exp[1]);

	entry_ptr = expand(entry);

	free(daisy_macro_exp[0]);
	daisy_macro_exp[0] = old_cluster_name;
	free(daisy_macro_exp[1]);
	daisy_macro_exp[1] = old_policy_name;

	return entry_ptr;
}

/*
 * Loads job_spec and job_summ global variables from
 * job info dir
 */
static int
load_job_description_from_dir(char *dir)
{
	int ret = -1;
	char fullpath[MAXPATHLEN + 1];
	char js_path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	sprintf(fullpath, "%s/%s", dir, SPEC_FILENAME);
	gc_job_spec = siq_gc_spec_load(fullpath, &error);
	if (error) {
		log(ERROR, "Failed to load spec from %s: %s",
		    fullpath, isi_error_get_message(error));
		gc_job_summ = NULL;
		goto done;
	}

	sprintf(js_path, "%s/%s", dir, REPORT_FILENAME);
	gc_job_summ = siq_gc_job_summary_load(js_path, &error);
	if (error) {
		log(ERROR, "Failed to load report from %s: %s",
		    js_path, isi_error_get_message(error));
		goto done;
	}

	/* Copy the job spec for reports */
	siq_report_spec_free(JOB_SUMM->job_spec);
	JOB_SUMM->job_spec = siq_spec_copy(JOB_SPEC);
	JOB_SUMM->action = JOB_SPEC->common->action;

	ret = 0;

done:
	isi_error_free(error);
	return ret;
}

#define POPT(opt,para,desc)						\
	printf("  -" opt " %s%s" desc "\n",				\
	    strlen(para) ? "<"para">" : "\t",				\
	    strlen(para) > 8 ? "\t" : "\t\t")

#define POPN(notice) printf("*\t\t\t" notice "\n")

static void
usage(void)
{
	printf("This command is for Isilon use only\n");
	/*
	printf("\nUsage: %s [OPTION]\n", PROGNAME);
	printf("Options\n");
	POPT("a", "action", "specify job action");
	POPT("A", "", "set assess (dry-run) mode");
	POPT("b", "host", "specify bandwidth reservation host");
	//POPT("C", "", "clear archive bit where set (QA only)");
	POPT("d", "num", "set recurse depth");
	POPT("D", "dir", "specify job working directory");
	POPT("e", "path", "specify path to exclude");
	POPT("E", "num", "expiration time of snapshots on target in seconds");
	POPT("g", "", "log removal of migrated files");
	POPT("h", "", "show this message");
	POPT("i", "path", "specify path to include");
	POPT("I", "", "set snapshot mode to PRESET");
	POPT("j", "name", "specify job name");
	POPT("k", "", "create url links to migrated files");
	POPT("l", "loglevel", "specify logging level (literal form)");
	POPT("m", "host", "specify (SmartConnect) windows link target");
	POPT("M", "num", "specify max errors");
	POPT("N", "num", "number of snapshots on target cluster for keep");
	POPT("p", "password", "set password to authenticate with secondary");
	POPT("P", "", "create snapshots on primary");
	POPT("r", "predicate", "set file selection criteria");
	POPT("t", "host[:path]", "target host/path");
	POPT("T", "name", "Snapshot name on secondary");
	POPT("w", "num", "number of worker processes per host");
	POPT("x", "","turn data integrity (checksum) mode off");
	//POPT("y", "type","set policy type (defaults to user)");
	POPT("v", "", "print version and exit");
	*/
	_exit(EXIT_FAILURE);
}

/*
 * Find most common path with spath in path's list gps
 * return -1 if such path wasn't found
 * 0 if the path is include (starts with +)
 * 1 if the path is exclude (starts with -)
 * paths from global config and spath
 * should finished at '/' (and stat with + or -)
 */
static int
find_cont_path(char **gps, char *spath, int num)
{
	int i = 0, res = -1, cmpres;
	while (i < num) {
		cmpres = strncmp(gps[i] + 1, spath + 1, strlen(gps[i]) - 1);
		if (cmpres == 0) {
			if (gps[i][0] == '+')
				res = 0;
			else
				res = 1;
		} else if (cmpres > 0) {
			return res;
		}
		i++;
	}
	return res;
}

/**
 * Path validation
 */
static void
eat_single_dots(char *path) {
	const char sdot[] = "/.";
	char *p;

	while ((p = strstr(path, sdot)) && (p[2] == 0 || p[2] == '/'))
		strcpy(p, p + 2);
}

static bool
eat_double_dots(char *path) {
	const char ddot[] = "/..";
	char *p, *q;

	while ((p = strstr(path, ddot)) && (p[3] == 0 || p[3] == '/')) {
		for (q = p - 1; q >= path && *q != '/'; q--);
		if (q < path)
			return false;
		if (p[3] != 0)
			strcpy(q + 1, p + 4);
		else
			*q = 0;
	}

	return true;
}

static const char valid_root[] = "/ifs";

static int
assure_prefix(const char *p) {
	return strncmp(p, valid_root, sizeof(valid_root) - 1);
}

static int
validate_path(const char *path)
{
	char *wp = NULL;
	int err = 0;

	if (!path || !*path) {
		log(ERROR, "Path may not be empty");
		err = EINVAL;
		goto out;
	}

	wp = strdup(path);
	ASSERT(wp);

	if (wp[0] != '/') {
		log(ERROR, "Path %s is not absolute", path);
		err = EINVAL;
		goto out;
	}

	eat_single_dots(wp);
	if (!strlen(wp)) {
		log(ERROR, "Path %s empty after single dot removal", path);
		err = EINVAL;
		goto out;
	}

	if (!eat_double_dots(wp)) {
		log(ERROR, "..'s in %s cross mount-point", path);
		err = EINVAL;
		goto out;
	}

	if (assure_prefix(wp)) {
		log(ERROR, "%s is outside %s", path, valid_root);
		err = EINVAL;
		goto out;
	}

out:
	if (wp != NULL)
		free(wp);

	return err;
}

static bool
contains_include_paths(struct migrconf *rc)
{
	if (rc->paths.npaths) {
		for (int i = 0; i < rc->paths.npaths; i++) {
			if (rc->paths.paths[i][0] == '+') {
				return true;
			}
		}
	}
	return false;
}

static bool
contains_include_path_list(struct siq_policy_path_head *list)
{
	struct siq_policy_path *current;
	
	SLIST_FOREACH(current, list, next) {
		if (current->type == SIQ_INCLUDE) {
		    return true;
		}
	}
	return false;
}

/*
 * This will dynamically realloc rc->paths as the number of paths
 * grows. Also, because all paths go through here, we check for
 * legality of specification.
 * _path should end with '/'
 */
#define ADD_DEPEND 0
#define ADD_ANYWAY 1

static void
_add_path(struct conf_paths *paths, char *path)
{
	if (paths->npaths >= paths->size - 1) {
	    paths->paths = realloc(paths->paths, 
		(paths->size *= 2) * sizeof path);
	    ASSERT(paths->paths);
	}
	paths->paths[paths->npaths++] = path;
}

static void
add_path(struct conf_paths *global_paths, struct conf_paths *paths, char *path,
    int add_anyway, ifs_snapid_t snapid)
{
	char *rp;
	int skip = 0;
	struct path valid_path;
	int ret;
	int root_fd;

	if (!path || !*path || validate_path(path + 1)) {
		log(ERROR, "Invalid path %s skipped", path);
		skip = 1;
	}

	rp = path + 1;
	if (!skip) {
		root_fd = ifs_lin_open(ROOT_LIN, snapid, O_RDONLY);
		if (root_fd < 0) {
			fatal_error(false, SIQ_ALERT_SOURCE_SNAPSHOT,
			    "Failed to open /ifs snapid: %llu: %s",
			    snapid, strerror(errno));
		}

		path_init(&valid_path, rp, 0);
		ret = fd_rel_to_ifs(root_fd, &valid_path);
		if (ret == -1) {
			log(ERROR, "Path %s is invalid or has symlinks,"
			    " skipped", rp);
			skip = 1;
		} else
			close(ret);
		close(root_fd);
	}
	if (!skip && ((path != NULL) && !add_anyway &&
	    (find_cont_path(global_paths->paths, path, global_paths->npaths) != 0))) {
		log(INFO, "Path %s excluded by global config rules", rp);
		skip = 1;
	}
	if (!skip) {
		_add_path(paths, path);
	}
}

static int
add_global_path(struct conf_paths *global_paths, char *path)
{
	if (path && (!strchr("+/-", *path) || 
	    abs(strchr(path, '/') - path) > 1)) {
		log(ERROR, 
		    "Global configuration error, path must be absolute: %s", 
		    path);
		return 1;
	}
	_add_path(global_paths, path);
	return 0;
}

#define USER_PATHS 0
#define GLOBAL_PATHS 1

#define SIQ_IPATH(type) ((type) == SIQ_INCLUDE)
static int
add_paths(struct conf_paths *global_paths, struct conf_paths *paths, 
    struct siq_policy_path_head *head, int paths_type, ifs_snapid_t snapid)
{
	struct siq_policy_path *cpath = NULL;
	int ret = 0;
	
	SLIST_FOREACH(cpath, head, next) {
		int len = strlen(cpath->path);
		char *cp = calloc(1, len + 3);
		ASSERT(cp);
		cp[0] = SIQ_IPATH(cpath->type) ? '+' : '-';
		strcpy(cp + 1, cpath->path);
		normalize_path(cp, ADD_SLASH_TO_PATH);
		if (paths_type == GLOBAL_PATHS) {
			ret = add_global_path(global_paths, cp);
			if (ret != 0) {
				return ret;
			}
		} else {
			add_path(global_paths, paths, cp, ADD_DEPEND, snapid);
			log(TRACE, "Added usr path \"%s\"",cp);
		}
	}
	return 0;
}

void
process_paths(struct siq_conf *gc, struct migrconf *rc, ifs_snapid_t snapid)
{
	int i;
	int j;
	struct conf_paths global_paths = {
		.npaths = 0,
		.size = 16
	};

	/* Global includes/excludes */
	global_paths.paths = calloc(global_paths.size, sizeof(char *));
	ASSERT(global_paths.paths);
	add_paths(&global_paths, NULL, &gc->coordinator->paths, GLOBAL_PATHS,
	    snapid);

	/* Force the default root-path include for domain treewalk. */
	if (JOB_SPEC->common->action !=
	    SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK) {
		/* Include/exclude paths */
		add_paths(&global_paths, &rc->paths, &JOB_SPEC->datapath->paths,
		    USER_PATHS, snapid);
	}

	/* Includes/excludes from isi_migrate arguments */
	for (i = 0; i < rc->tmp_paths.npaths; i++) {
		add_path(&global_paths, &rc->paths, rc->tmp_paths.paths[i],
		    ADD_DEPEND, snapid);
	}

	if (contains_include_path_list(&JOB_SPEC->datapath->paths) &&
	    !contains_include_paths(rc) &&
	    JOB_SPEC->common->action != SIQ_ACT_FAILBACK_PREP_DOMAIN_MARK) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Job %s has no valid include paths", SIQ_JNAME(rc));
	}

	/* Search paths, if there are no includes, add root path */
	if (!contains_include_paths(rc)) {
		char *toAdd = malloc(strlen(rc->path) + 3);
		ASSERT(toAdd);
		toAdd[0] = '+';
		strcpy(toAdd + 1, rc->path);
		normalize_path(toAdd, ADD_SLASH_TO_PATH);
		add_path(&global_paths, &rc->paths, toAdd, ADD_ANYWAY, snapid);
		qsort(rc->paths.paths, rc->paths.npaths, sizeof(char *), _strcmp);
	}

	/* At the minimum, there should be at least 1 include path here.
	 * If not, that is because root_path is not a valid path. */
	if (!contains_include_paths(rc)) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Job has no valid include paths");
	}

	/* Add exclude paths from config */
	for (i = 0; i < global_paths.npaths; i++) {
		if ((global_paths.paths[i][0] == '-') &&
		    (find_cont_path(rc->paths.paths, global_paths.paths[i],
		    rc->paths.npaths) == 0)) {
			/*
			 * it is exclude path form global config that
			 * sets additional condition
			 */
			add_path(&global_paths, &rc->paths,
			    global_paths.paths[i], ADD_ANYWAY, snapid);
		} else
			free(global_paths.paths[i]);
	}

	/* Sort the paths */
	qsort(rc->paths.paths, rc->paths.npaths, sizeof(char *), _strcmp);

	/* Zero end, b/c of realloc. Must be after qsort */
	if (rc->paths.npaths) {
		ASSERT(rc->paths.npaths < rc->paths.size);

		/* We should really get rid of this code since we already
		 * maintain npaths.*/
		if (rc->paths.paths[rc->paths.npaths])
			rc->paths.paths[rc->paths.npaths++] = NULL;
	}

	/* Remove last '/' */
	for (i = 0; rc->paths.paths[i] != NULL; i++) {
		j = strlen(rc->paths.paths[i]);
		if (rc->paths.paths[i][j - 1] == '/')
			rc->paths.paths[i][j - 1] = '\0';
	}
}

static void
check_for_mirror_policy(char *pol_id)
{
	struct siq_policies *pl = NULL;
	struct siq_policy *pol = NULL;
	struct isi_error *error = NULL;

	ASSERT(pol_id);

	pl = siq_policies_load_nolock(&error);
	if (pl == NULL)
		goto out;

	pol = siq_policy_get_by_pid(pl, pol_id);
	if (pol == NULL)
		goto out;

	if (pol->common->mirror_state == SIQ_MIRROR_ON) {
		ASSERT(pol->common->mirror_pid != NULL);
		/*
		 * Fail the policy since the mirror policy is active
		 */
		fatal_error(false, SIQ_ALERT_POLICY_FAIL, 
		    "Mirror(failback) policy ID %s is currently active. "
		    "Please perform either a failback procedure on mirror "
		    "policy or delete the mirror policy and reset the "
		    "current policy.",
		    pol->common->mirror_pid);
	}

out:
	if (pl)
		siq_policies_free(pl);

	if (error) {
		fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
		    "Failed to load policies: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}
}

/*  __  __ _              ____                _                  __ 
 * |  \/  (_) __ _ _ __  |  _ \ ___  __ _  __| | ___ ___  _ __  / _|
 * | |\/| | |/ _` | '__| | |_) / _ \/ _` |/ _` |/ __/ _ \| '_ \| |_ 
 * | |  | | | (_| | |    |  _ <  __/ (_| | (_| | (_| (_) | | | |  _|
 * |_|  |_|_|\__, |_|    |_| \_\___|\__,_|\__,_|\___\___/|_| |_|_|  
 *           |___/                                                  
 *
 * Read job configuration from job info dir(-D option of command line 
 * defines it) and from command line to struct migrconf *rc
 */
int
migr_readconf(int argc, char **argv, struct siq_conf *gc, struct migrconf *rc,
    struct siq_source_record **record)
{
	int len, ret;
	char *cp;
	int err;
	struct conf_paths global_paths = {
		.npaths = 0,
		.size = 16
	};
	char c;
	bool exists;
	struct isi_error *error = NULL;
	const char* siq_action_type_valid_options[2] = {
		"copy",
		"sync"
	};
	enum siq_job_action pending_job = SIQ_JOB_NONE;
	int compliance_dom_ver = 0;
	struct stat st;

	if (argc >= 4 && (!strcmp(argv[3], "-f"))) {
		/*
		 * Fast start is used only through test framework.
		 * log this behavior so that all the fd's are initialized
		 * through the first log call.
		 */
		log(NOTICE, "Starting coordinator through test");
	}

	memset(rc, 0, sizeof(*rc));
	rc->paths.size = 16;
	rc->paths.paths = calloc(rc->paths.size, sizeof(cp));
	ASSERT(rc->paths.paths);
	rc->tmp_paths.size = 16;
	rc->tmp_paths.paths = calloc(rc->tmp_paths.size, sizeof(cp));
	ASSERT(rc->tmp_paths.paths);
	global_paths.paths = calloc(global_paths.size, sizeof(cp));
	ASSERT(global_paths.paths);

	rc->bwhost = gc->bwt->host;
	rc->disable_bw = gc->coordinator->skip_bw_host;
	rc->disable_th = gc->coordinator->skip_th_host;
	rc->disable_work = gc->coordinator->skip_work_host;
	rc->force_wpn = gc->coordinator->force_workers_per_node;

	/* Global includes/excludes */
	add_paths(&global_paths, NULL, &gc->coordinator->paths, GLOBAL_PATHS,
	    HEAD_SNAPID);

	//set_job_state(SIQ_JS_RUNNING);
	/*
	 * Load job configuration from job_ino_dir
	 * Job dir should be the first option
	 */
	if (argc >= 3 && (!strcmp(argv[1], "-D") || !strcmp(argv[1], "-j"))) {
		/* Specified by policy ID (hash number) */
		if (argv[1][1] == 'D') {
			/* arg for -D is run directory based on policy
			 * id and node number:
			 * /ifs/.ifsvar/modules/tsm/sched/run/ID_NODENUM */
			siq_job_grab(argv[2], &rc->job_id, &rc->job_dir);
		}
		/* Specified by policy/job name ('j') */
		else {
			ilog_running_app_stderr_set(true);
			log(INFO, "Job specified by name %s", argv[2]);
			if (siq_job_create(argv[2], &rc->job_id,
				&rc->job_dir) != 0) {
				log(ERROR, "Failed to create job for policy %s",
				    argv[2]);
				_exit(EXIT_FAILURE);
			}
		}

		rc->conf_load_error =
		    load_job_description_from_dir(rc->job_dir);
		if (rc->conf_load_error != 0) {
			log(FATAL,
			    "Failed to set-up job from %s, error: %s",
			    rc->job_dir, strerror(rc->conf_load_error));
		}

		if (JOB_SPEC->common->action == SIQ_ACT_NOP) {
			log(FATAL, "Failed to determine job action");
		}

		if ((err = init_macros(JOB_SPEC->common->name))) {
			log(FATAL, "Failed to init macro expansion: %s",
			    strerror(err));
		}

		set_job_state(SIQ_JS_RUNNING);
		rc->target = strdup(JOB_SPEC->target->dst_cluster_name);
		rc->targetpath = strdup(JOB_SPEC->target->path);

		rc->wperhost = JOB_SPEC->coordinator->workers_per_node;
		/* XXX-M5 Real job ID goes here */
		if (JOB_SPEC->common->name != NULL)
			rc->job_name = strdup(JOB_SPEC->common->name);
		else
			rc->job_name = "noname";

		rc->loglevel = JOB_SPEC->common->log_level;

		if (JOB_SPEC->datapath->predicate != NULL) {
			rc->predicate = strdup(JOB_SPEC->datapath->predicate);
		}

		rc->action = JOB_SPEC->common->action;

		if (!JOB_SPEC->coordinator->check_integrity) {
			rc->flags |= FLAG_NO_INTEGRITY;
		}

		if (JOB_SPEC->coordinator->diff_sync) {
			rc->flags |= FLAG_DIFF_SYNC;
		}

		if (!JOB_SPEC->datapath->disable_stf) {
			rc->flags |= FLAG_STF_SYNC;
		}

		if (JOB_SPEC->coordinator->expected_dataloss) {
			rc->flags |= FLAG_EXPECTED_DATALOSS;
			rc->flags &= ~FLAG_STF_SYNC;
		}

		if (JOB_SPEC->datapath->burst_mode) {
			rc->flags |= FLAG_BURST_SOCK;
		}

		if (!JOB_SPEC->coordinator->disable_file_split &&
		    ISI_UPGRADE_API_DISK_ENABLED(JAWS_RU)) {
			rc->flags |= FLAG_FILE_SPLIT;
		}

		rc->disable_fofb = JOB_SPEC->datapath->disable_fofb;

		rc->pause_threshold =
			gc->coordinator->pause_limit;

		if (JOB_SPEC->coordinator->log_removed_files) {
			rc->flags |= FLAG_LOGDELETES;
			rc->log_removed_files_name = 
			    strdup(SIQ_SCHED_DELETED_FILES_LOG_FILE);
		}

		rc->limit_work_item_retry =
		    JOB_SPEC->coordinator->limit_work_item_retry;

		rc->expiration = JOB_SPEC->coordinator->snapshot_expiration;
		if (JOB_SPEC->target->password == NULL) {
			/*NULL password is equal to "" password*/
			rc->passwd = strdup("");
		} else {
			rc->passwd = JOB_SPEC->target->password;
		}

		if (JOB_SPEC->target->enable_hash_tmpdir) {
			rc->flags |= FLAG_ENABLE_HASH_TMPDIR;
		}

		/*check snapshot options*/
		rc->source_snapmode = JOB_SPEC->datapath->src_snapshot_mode;

		switch (JOB_SPEC->coordinator->dst_snapshot_mode) {
		case SIQ_SNAP_MAKE:
			rc->target_snapshot = true;
			rc->snap_sec_name =
			    strdup(JOB_SPEC->coordinator->snapshot_pattern);
			expand(&rc->snap_sec_name);
			log(DEBUG, "snap_sec_name is set to '%s'",
			    rc->snap_sec_name);
			if (JOB_SPEC->coordinator->snapshot_alias) {
				rc->snapshot_alias = strdup(
				    JOB_SPEC->coordinator->snapshot_alias);
				if (rc->snapshot_alias &&
				    !strlen(rc->snapshot_alias)) {
					rc->snapshot_alias = NULL;
				}
			} else
                                rc->snapshot_alias = NULL;
			expand(&rc->snapshot_alias);
			log(DEBUG, "snapshot_alias is set to '%s'",
			    rc->snapshot_alias);

			break;
		default:
			rc->snap_sec_name = NULL;
			break;
		}

		rc->skip_bb_hash = JOB_SPEC->coordinator->skip_bb_hash;

		if (JOB_SPEC->coordinator->rename_pattern == NULL)  {
			rc->rename_snap = strdup("");
		} else {
			rc->rename_snap = 
			    strdup(JOB_SPEC->coordinator->rename_pattern);
		}
		expand(&rc->rename_snap);

		/* XXXJDH: handle upgrade */
		exists = siq_source_has_record(rc->job_id);
		if (!exists) {
			fatal_error(false, SIQ_ALERT_POLICY_UPGRADE, "Policy "
			    "has not been upgraded: source record missing");
		}

		*record = siq_source_record_open(rc->job_id, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Failed to load job state file: %s",
			    isi_error_get_message(error));

		if (siq_source_get_database_mirrored(*record))
			rc->database_mirrored = true;

		if (!siq_source_has_owner(*record)) {
			siq_source_take_ownership(*record);
			siq_source_record_write(*record, &error);
			if (error) {
				siq_source_record_unlock(*record);
				fatal_error(false, SIQ_ALERT_POLICY_FAIL,
				    "Failed to save job ownership: %s",
				    isi_error_get_message(error));
			}
		}

		siq_source_record_unlock(*record);

		JOB_SUMM->job_id = siq_reports_status_check_inc_job_id(
		    rc->job_id, &error);
		if (error) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    "Failed to get job_id (%s): %s",
			    rc->job_id, isi_error_get_message(error));
		}

		rc->is_delete_job = siq_source_is_policy_deleted(*record);
		if (rc->is_delete_job)
			return 0;

		/*
		 * If the mirror policy is enabled AND is not a delete, then
		 * fail the policy and disable it.
		 */
		if (!g_ctx.rc.is_delete_job)
			check_for_mirror_policy(rc->job_id);

		siq_source_get_pending_job(*record, &pending_job);
		rc->job_type = pending_job;
		if (pending_job == SIQ_JOB_ASSESS) {
			rc->flags |= FLAG_ASSESS;
			JOB_SUMM->assess = true;
		}

		/* Pick out root path. */
		if (!JOB_SPEC->datapath->root_path ||
		    JOB_SPEC->datapath->root_path[0] == '\0' ||
		    (JOB_SPEC->datapath->root_path[0] == '/' &&
		    JOB_SPEC->datapath->root_path[1] == '\0')) {
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Job has no root path");
		}

		rc->path = strdup(JOB_SPEC->datapath->root_path);
		ASSERT(rc->path != NULL);
		len = strlen(rc->path);
		if (rc->path[len - 1] == '/')
			rc->path[len - 1] = '\0';

		/* Check if the root path exists. */
		if (lstat(rc->path, &st)) {
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Job has no valid root path");
		}

		compliance_dom_ver = get_compliance_dom_ver(rc->path, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    isi_error_get_message(error));

		/* Add tmp-working-dir_restore to the excludes list for compliance v2
		 * syncs. */
		if (compliance_dom_ver == 2 &&
		    JOB_SPEC->common->action == SIQ_ACT_SYNC) {
			char root_path[MAXPATHLEN];
			char tmp_working_dir[MAXPATHLEN];
			char *tmp_working_path;

			strncpy(root_path, rc->path, sizeof(root_path));
			normalize_path(root_path, STANDARD_NORMALIZE);
			get_tmp_working_dir_name(tmp_working_dir, true);
			asprintf(&tmp_working_path, "-%s/%s/%s/%s",
			    root_path, COMPLIANCE_STORE_ROOT_NAME,
			    get_cluster_guid(), tmp_working_dir);
			ASSERT(tmp_working_path);

			log(NOTICE, "%s: excluding tmp restore dir: %s",
			    __func__, tmp_working_path);
			add_path(&global_paths, &rc->paths, tmp_working_path,
			    ADD_ANYWAY, HEAD_SNAPID);
		}

		rc->skip_lookup = JOB_SPEC->target->skip_lookup;
		rc->disable_bulk_lin_map = 
		    JOB_SPEC->coordinator->disable_bulk_lin_map;

		rc->deep_copy = JOB_SPEC->coordinator->deep_copy;
	} else {
		if (argc == 1 || (argc >= 2 && !strcmp(argv[1], "-h")))
			usage();
		/*
		 * reaction if not CONFIG_EMULATE => FATAL error, else
		 * init some fields
		 */
	}

	/* command line options, propagate to summary */
	while ((c = getopt(argc, argv,
	    "a:Ab:d:D:e:E:fghi:j:l:N:p:Pr:st:T:w:x")) != -1){
		switch (c) {
		case 'a': /* Action */
			ret = dFindPos(optarg, siq_action_type_valid_options);
			if (ret == -1) {
				log(ERROR, "Unsupported action %s", optarg);
				_exit(EXIT_FAILURE);
			}
			rc->action = ret;
			JOB_SPEC->common->action = rc->action;	
			break;
		case 'A': /* Assess flag */
			rc->flags |= FLAG_ASSESS;
			JOB_SUMM->assess = true;
			break;
		/*case 'C': //Clear archive bit before operation
			rc->flags |= FLAG_CLEAR;
			break;*/
		case 'd':
			rc->depth = atoi(optarg);
			break;
		case 'D': /* Dir to store reports */
			if (!rc->job_dir) rc->job_dir = optarg;
			break;
		case 'e':
			cp = malloc(strlen(optarg) + 3);
			ASSERT(cp);
			cp[0] = '-';
			strcpy(cp + 1, optarg);
			normalize_path(cp, ADD_SLASH_TO_PATH);
			_add_path(&rc->tmp_paths, cp);
			cp = NULL;
			break;
		case 'E':
			rc->expiration = atoi(optarg);
			break;
		case 'f':
			// We dont this flag anywhere in the code
			//rc->flags |= FLAG_FAST_START;
			break;
		case 'g':
			rc->flags |= FLAG_LOGDELETES;
			if(rc->log_removed_files_name == NULL) {
				rc->log_removed_files_name = "deleted-files";
			}
			break;
		case 'h':
			usage();
			break;
		case 'i':
			cp = malloc(strlen(optarg) + 3);
			ASSERT(cp);
			cp[0] = '+';
			strcpy(cp + 1, optarg);
			normalize_path(cp, ADD_SLASH_TO_PATH);
			_add_path(&rc->tmp_paths, cp);
			cp = NULL;
			break;
		case 'j':
			rc->job_name = optarg;
			break;
		case 'l':
			rc->loglevel = dFindPos(optarg, siq_log_level_strs_gp);
			if (rc->loglevel == -1) {
				log(ERROR, "Invalid log level '%s'\n", 
				    optarg);
				usage();
			}
			break;
		case 'n': /* hidden option... */
			rc->noop = 1;
			break;
		case 'N':
			/*Operation with snapshots on secondary*/
			rc->flags |= FLAG_SNAPSHOT_SEC;
			break;
		case 'p':
			rc->passwd = optarg;
			break;
		case 'P':
			/*Operation with snapshots*/
			rc->flags |= FLAG_SNAPSHOT_PRI;
			break;
		case 'r':
			rc->predicate = strdup(optarg);
			break;
		case 't': /* Destination cluster and path */
			rc->target = optarg;
			if ((cp = strchr(rc->target, ':'))) {
				rc->targetpath = cp + 1;
				*cp = 0;
			}
			break;
		case 'T':
			rc->snap_sec_name = strdup(optarg);
			break;
		case 'w':
			rc->wperhost = atoi(optarg);
			break;
		case 'x':
			rc->flags |= FLAG_NO_INTEGRITY;
			break;
		/*case 'y':
			JOB_SPEC->type = dFindPos(optarg, siq_ptype_strs_gp);
			if (JOB_SPEC->type == -1) {
				log(ERROR, "Invalid policy type '%s'\n", 
				    optarg);
				usage();
			}
			break;
		*/
		default:
			usage();
		}
	}

	if (optind != argc) {
		log(ERROR, "Invalid arg %s", optarg);
		usage();
	}

	if (!rc->job_dir) {
		rc->job_dir = ".";
	}
	
	if (!gc_job_summ) {
		char js_path[MAXPATHLEN + 1];
		
		sprintf(js_path, "%s/%s", rc->job_dir, REPORT_FILENAME);
		gc_job_summ = siq_gc_job_summary_load(js_path, &error);
		if (error) {
			fatal_error(false, SIQ_ALERT_POLICY_FAIL, 
			    "Failed to load report from %s: %s",
			    js_path, isi_error_get_message(error));
		}
	}

	/* Validate target path */
	if (! rc->target || ! rc->targetpath) {
		log(FATAL, "No target or target path set for job %s",
		    SIQ_JNAME(rc));
	}
	if (validate_path(rc->targetpath)) {
		log(FATAL, "Job %s had invalid target path %s",
		    SIQ_JNAME(rc), rc->targetpath);
	}

	normalize_path(rc->targetpath, STANDARD_NORMALIZE);

	/*Check if target path is resolved by global config*/
	cp = malloc(strlen(rc->targetpath) + 3);
	ASSERT(cp);
	cp[0] = '+';
	strcpy(cp + 1, rc->targetpath);
	normalize_path(cp, ADD_SLASH_TO_PATH);
	if (find_cont_path(global_paths.paths, cp, global_paths.npaths) != 0) {
		log(FATAL, "Target path %s excluded by global config",
		    rc->targetpath);
	}
	free(cp);
	cp = NULL;

	/* Failover/Failback is disabled for compliance V1 domains. */
	switch (JOB_SPEC->common->action) {
	case SIQ_ACT_FAILBACK_PREP:
	case SIQ_ACT_FAILBACK_PREP_RESTORE:
	case SIQ_ACT_FAILBACK_PREP_FINALIZE:
		if (compliance_dom_ver == 1)
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Cannot perform resync-prep on a Compliance v1 "
			    "directory.");
		break;

	case SIQ_ACT_FAILOVER:
		if (compliance_dom_ver == 1)
			fatal_error(false, SIQ_ALERT_POLICY_CONFIG,
			    "Cannot perform allow-write on a Compliance v1 "
			    "directory.");
		break;

	default:
		break;
	}

	/* Add snapshot flags for WORK_INIT_MSG (information for workers) */
	rc->flags |= FLAG_SNAPSHOT_PRI;

	if (rc->flags & FLAG_ASSESS)
		rc->target_snapshot = false;

	if (rc->target_snapshot)
		rc->flags |= FLAG_SNAPSHOT_SEC;

	if (rc->wperhost <= 0)
		rc->wperhost = 1;

	/* If we didn't have a per-job setting, override with global default.
	   Note: we don't support per-job settings yet. */
	if (rc->max_wperpolicy <= 0)
		rc->max_wperpolicy = gc->coordinator->max_wperpolicy;
	/* if we didn't have a global default (missing from xml?), default it */
	if (rc->max_wperpolicy <= 0)
		rc->max_wperpolicy = 40;

	rc->target_wi_lock_mod = JOB_SPEC->target->work_item_lock_mod;

	return 0;
}
