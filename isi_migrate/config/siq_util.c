#include "siq_util_p.h"
#include "isi_domain/dom_util.h"
#include "isi_migrate/siq_error.h"
#include "isi_migrate/sched/siq_log.h"
#include "isi_migrate/config/siq_job.h"
#include "isi_config/array.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_snapshot/snapshot.h"

#define COMPLIANCE_FLAGS (DOM_COMPLIANCE | DOM_PRIVDEL_DISABLED)
#define COMPLIANCE_V2_FLAGS (DOM_COMPLIANCE | DOM_COMPLIANCE_V2 | DOM_PRIVDEL_DISABLED)

const char *
siq_enum_to_str(const siq_enum_string_map_t map, int val)
{
	if (val < 0 || val >= map.nvals)
		return map.def_str;
	return map.strs[val];
}

int
siq_str_to_enum(const siq_enum_string_map_t map, const char *str)
{
	int i;

	for (i = 0; i < map.nvals; ++i) {
		if (!strcasecmp(map.strs[i], str))
			return i;
	}
	return -1;
}

const char *
siq_log_level_to_str(int log_level)
{
	return siq_enum_to_str(siq_log_level_str_map_gp, log_level);
}

int
siq_str_to_log_level(const char *log_level_str)
{
	return siq_str_to_enum(siq_log_level_str_map_gp, log_level_str);
}

int
extract_subnet_and_pool_name(const char *subnet_and_pool_name,
    char **p_subnet_name, char **p_pool_name)
{
	char *colon;

	colon = strchr(subnet_and_pool_name, ':');

	/* Invalid if no colon, colon is first character, colon is last
	 * character, or more than one colon */
	if (!colon ||
	    colon == subnet_and_pool_name ||
	    strlen(colon) == 1 ||
	    strchr(colon + 1, ':'))
		return -1;
	
	*p_pool_name = strdup(colon + 1);
	*p_subnet_name = strdup(subnet_and_pool_name);
	(*p_subnet_name)[colon - subnet_and_pool_name] = '\0';
	return 0;
}

struct flx_pool *
get_pool(struct flx_config *config, const char *subnet_and_pool_name,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct flx_subnet *subnet;
	struct flx_pool *pool = NULL;
	char *subnet_name = NULL, *pool_name = NULL;

	if (extract_subnet_and_pool_name(subnet_and_pool_name,
		&subnet_name, &pool_name) == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Bad subnet and pool restriction '%s'",
		    subnet_and_pool_name);
		goto out;
	}
	subnet = flx_config_find_subnet_by_name(config, subnet_name);
	if (!subnet) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Unknown subnet '%s' in subnet:pool "
		    "specification of %s", subnet_name, subnet_and_pool_name);
		goto out;
	}
	else {
		pool = flx_subnet_find_pool_by_name(subnet, pool_name);
		if (!pool) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "Unknown pool '%s' in subnet:pool specification "
			    "of %s", pool_name, subnet_and_pool_name);
			goto out;
		}
	}

out:
	if (subnet_name)
		free(subnet_name);
	if (pool_name)
		free(pool_name);
	isi_error_handle(error, error_out);
	return pool;
}

bool
text2bool(const char *text)
{
	if (strcasecmp(text, "true") == 0 ||
	    strcasecmp(text, "on") == 0 ||
	    strcmp(text, "1") == 0)
		return true;
	return false;
}

bool
text2bool_err(const char *text, bool *err)
{
	if (strcasecmp(text, "true") == 0 ||
	    strcasecmp(text, "on") == 0 ||
	    strcmp(text, "1") == 0) {
		*err = false;
		return true;
	}
	if (strcasecmp(text, "false") == 0 ||
	    strcasecmp(text, "off") == 0 ||
	    strcmp(text, "0") == 0) {
		*err = false;
		return true;
	}
	*err = true;
	return false;
}

char *
bool2text(bool value)
{
	return value ? "true" : "false";
}

char *
get_cluster_guid(void)
{
	char *buffer = NULL;
	int buffer_size = 0;
	char *to_return = NULL;
	const char *idstr;
	struct fmt FMT_INIT_CLEAN(fmt);
	struct isi_error *error_out;
	
	arr_cluster_guid_load((uint8_t**)&buffer, (uint8_t*)&buffer_size, 
	    &error_out);
	if (error_out != NULL) {
		//Stuff
	}
	
	fmt_print(&fmt, "%{}", arr_guid_fmt(buffer));
	idstr = fmt_string(&fmt);
	to_return = strdup(idstr);
	
	free(buffer);
	fmt_clean(&fmt);

	return to_return;
}

int
acquire_lock(const char *lock_file, int lock_type,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(lock_type == O_EXLOCK ||
	    lock_type == O_SHLOCK ||
	    lock_type == (O_NONBLOCK | O_EXLOCK) ||
	    lock_type == (O_NONBLOCK | O_SHLOCK));

	fd = open(lock_file, lock_type | O_RDONLY | O_CREAT, 0400);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open lock file: %s", lock_file);
	}

	isi_error_handle(error, error_out);
	return fd;
}

char *
job_state_to_text(enum siq_job_state state, bool friendly)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	char *to_return = NULL;
	
	if (friendly) {
		fmt_print(&fmt, "%{}", enum_siq_job_state_fmt(state));
		asprintf(&to_return, "%s", fmt_string(&fmt));
		fmt_clean(&fmt);
	} else {
		switch (state) {
		case SIQ_JS_NONE:
			to_return = strdup("SIQ_JS_NONE");
			break;
		case SIQ_JS_SCHEDULED:
			to_return = strdup("SIQ_JS_SCHEDULED");
			break;
		case SIQ_JS_RUNNING:
			to_return = strdup("SIQ_JS_RUNNING");
			break;
		case SIQ_JS_PAUSED:
			to_return = strdup("SIQ_JS_PAUSED");
			break;
		case SIQ_JS_SUCCESS:
			to_return = strdup("SIQ_JS_SUCCESS");
			break;
		case SIQ_JS_FAILED:
			to_return = strdup("SIQ_JS_FAILED");
			break;
		case SIQ_JS_CANCELLED:
			to_return = strdup("SIQ_JS_CANCELLED");
			break;
		case SIQ_JS_SUCCESS_ATTN:
			to_return = strdup("SIQ_JS_SUCCESS_ATTN");
			break;
		case SIQ_JS_TOTAL:
			to_return = strdup("SIQ_JS_TOTAL");
			break;
		case SIQ_JS_SKIPPED:
			to_return = strdup("SIQ_JS_SKIPPED");
			break;
		case SIQ_JS_PREEMPTED:
			to_return = strdup("SIQ_JS_PREEMPTED");
			break;
		default:
			ASSERT(0, "Invalid job state %d", state);
		}
	}
	ASSERT(to_return);
	
	return to_return;
}

enum siq_job_state
text_to_job_state(const char *txt)
{
	if (strcmp("SIQ_JS_INVALID", txt) == 0)
		return SIQ_JS_INVALID;
	if (strcmp("SIQ_JS_NONE", txt) == 0)
		return SIQ_JS_NONE;
	if (strcmp("SIQ_JS_SCHEDULED", txt) == 0)
		return SIQ_JS_SCHEDULED;
	if (strcmp("SIQ_JS_RUNNING", txt) == 0)
		return SIQ_JS_RUNNING;
	if (strcmp("SIQ_JS_PAUSED", txt) == 0)
		return SIQ_JS_PAUSED;
	if (strcmp("SIQ_JS_SUCCESS", txt) == 0)
		return SIQ_JS_SUCCESS;
	if (strcmp("SIQ_JS_FAILED", txt) == 0)
		return SIQ_JS_FAILED;
	if (strcmp("SIQ_JS_CANCELLED", txt) == 0)
		return SIQ_JS_CANCELLED;
	if (strcmp("SIQ_JS_SUCCESS_ATTN", txt) == 0)
		return SIQ_JS_SUCCESS_ATTN;
	if (strcmp("SIQ_JS_TOTAL", txt) == 0)
		return SIQ_JS_TOTAL;
	if (strcmp("SIQ_JS_SKIPPED", txt) == 0)
		return SIQ_JS_SKIPPED;
	if (strcmp("SIQ_JS_PREEMPTED", txt) == 0)
		return SIQ_JS_PREEMPTED;
	return SIQ_JS_INVALID;
}

int
get_local_node_id()
{
	struct arr_config *ac;
	struct isi_error *error = NULL;
	int node_id;

	ac = arr_config_load(&error);
	if (error) {
		log(ERROR, "%s: Can't get arr_config: %s",
		    __func__, isi_error_get_message(error));
		isi_error_free(error);
		return -1;
	}
	node_id = arr_config_get_local_devid(ac);
	arr_config_free(ac);

	return node_id;
}

bool
delete_snapshot_helper(ifs_snapid_t delete_id)
{
	struct isi_error *error = NULL;
	SNAP *snap = NULL;
	struct isi_str *name = NULL;
	bool success = true;

	if (delete_id == INVALID_SNAPID)
		goto out;

	snap = snapshot_open_by_sid(delete_id, &error);
	if (error) {
		log(NOTICE, "Missing delete snapshot: %lld",
		    delete_id);
		goto out;
	}

	if ((name = snapshot_get_name(snap)) == NULL) {
		log(NOTICE, "Snapshot id %lld is already being deleted",
		    delete_id);
		goto out;
	}

	snapshot_destroy(name, &error);
	if (error && !isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS)) {
		/* Note that we don't actually fail */
		log(ERROR, "Failed to remove snapshot: %s: %{}", name->str,
		    isi_error_fmt(error));
		success = false;
		goto out;
	}

out:
	if (snap)
		snapshot_close(snap);
	if (name)
		isi_str_free(name);
	if (error)
		isi_error_free(error);

	return success;
}

/*
 * normalize path, if add_slash option is on, add '/' to the end 
 * of path (memory should be enough to do this, 
 * user should take care about memory)
 */
void
normalize_path(char *path, int add_slash)
{
	int i, j;

	for (i = j = 0; path[i]; i++)
		if (!j || path[j - 1] != '/' || path[i] != '/')
			path[j++] = path[i];

	if (add_slash == ADD_SLASH_TO_PATH) {
		if (add_slash && (path[j-1] != '/')) {
			path[j++] = '/';
		}
		path[j] = '\0';
	} else {
		path[j--] = '\0';
		for (; j && path[j] == '/'; j--)
			path[j] = '\0';
	};
	return;
}

/* NOTE: Not encoding safe!!! 
 * Also, assumes that paths do not have trailing slash and that an empty path
 * overlaps with any other path.*/
enum path_overlap_result
path_overlap(const char *a, const char *b)
{
	while (*a && *b) {
		if (*a == *b) {
			a++;
			b++;
			continue;
		}
		return NO_OVERLAP;
	}
	
	/* An overlap if one is shorter than the other and the longer one's
	 * character pointer is on a '/' (meaning there was a full directory
	 * name match at the previous level), or if both end at the same time
	 * (meaning the same directory) */
	if (*a == '\0') {
		if (*b == '/') {
			return PATH_A_PARENT;
		}
	}
	if (*b == '\0') {
		if (*a == '/') {
			return PATH_B_PARENT;
		}
	}
	if (!*a && !*b) {
		return PATHS_EQUAL;
	}

	return NO_OVERLAP;
}

int
set_siq_log_level(int siq_level)
{
	if (siq_level >= SEVERITY_TOTAL) {
		fprintf(stderr, "Invalid SyncIQ log level of %d. "
		    "Range is 0 to %d\n", siq_level, SEVERITY_TOTAL-1);
		return -1;
	}
	ilog_running_app_set_level(siq_to_islog_severity_map[siq_level]);
	return 0;
}

int
set_siq_log_level_str(char *val)
{
	if (!isdigit(val[0])) {
		fprintf(stderr, "Invalid SyncIQ log level of '%s'\n", val);
		return -1;
	}
	return set_siq_log_level(atoi(val));
}

void
siq_nanosleep(int seconds, long nanoseconds)
{
	struct timespec tosleep = {};
	int ret = -1;
	int divisor = 0;
	
	divisor = nanoseconds / 1000000000;
	if (divisor > 0) {
		seconds += divisor;
		nanoseconds = nanoseconds % 1000000000;
	}
	tosleep.tv_sec = seconds;
	tosleep.tv_nsec = nanoseconds;
	while (ret != 0) {
		ret = nanosleep(&tosleep, &tosleep);
		if (ret == -1 && errno != EINTR) {
			return;
		}
	}
}

char *
job_phase_enum_friendly_str(enum stf_job_phase phase)
{
	switch(phase) {
	case STF_PHASE_TW:
		return "Treewalk";
	case STF_PHASE_IDMAP_SEND:
		return "ID map backup";
	case STF_PHASE_SUMM_TREE:
		return "Enumerate changed LINs";
	case STF_PHASE_CC_DIR_DEL:
		return "Enumerate deleted LINs";
	case STF_PHASE_CC_DIR_CHGS:
		return "Enumerate added/changed directories";
	case STF_PHASE_CT_LIN_UPDTS:
		return "Transfer added/changed files";
	case STF_PHASE_CT_LIN_HASH_EXCPT:
		return "Transfer hash exception files";
	case STF_PHASE_CT_FILE_DELS:
		return "Transfer list of deleted files";
	case STF_PHASE_CT_DIR_DELS:
		return "Transfer list of deleted directories";
	case STF_PHASE_CT_DIR_LINKS:
		return "Transfer LIN link/relinks";
	case STF_PHASE_CT_DIR_LINKS_EXCPT:
		return "Transfer LIN link/relink exceptions";
	case STF_PHASE_CT_STALE_DIRS:
		return "Clean up stale directories";
	case STF_PHASE_FLIP_LINS:
		return "Commit new policy state";
	case STF_PHASE_DOMAIN_MARK:
		return "Adding files to a domain";
	case STF_PHASE_DATABASE_CORRECT:
		return "Transferring source/target LIN database changes";
	case STF_PHASE_DATABASE_RESYNC:
		return "Resyncing source/target LIN databases";
	case STF_PHASE_CC_COMP_RESYNC:
		return "Merge resync list into the differential repstate";
	case STF_PHASE_COMP_CONFLICT_CLEANUP:
		return "Cleaning up conflicting files pre-restore";
	case STF_PHASE_COMP_CONFLICT_LIST:
		return "Building up conflicting files list";
	case STF_PHASE_CT_COMP_DIR_LINKS:
		return "Transfer compliance resync LIN link/relinks";
	case STF_PHASE_COMP_COMMIT:
		return "Committing compliance files";
	case STF_PHASE_COMP_MAP_PROCESS:
		return "Processing target compliance map into linmaps";
	case STF_PHASE_COMP_FINISH_COMMIT:
		return "Finishing committing compliance files";
	case STF_PHASE_CT_COMP_DIR_DELS:
		return "Deleting directories in compliance mode";
	case STF_PHASE_FINISH:
		return "Job complete";
	default:
		return "Unknown";
	}
}

char *
job_phase_enum_str(enum stf_job_phase phase) 
{
	switch(phase) {
	case STF_PHASE_TW:
		return "STF_PHASE_TW";
	case STF_PHASE_SUMM_TREE:
		return "STF_PHASE_SUMM_TREE";
	case STF_PHASE_CC_DIR_DEL:
		return "STF_PHASE_CC_DIR_DEL";
	case STF_PHASE_CC_DIR_CHGS:
		return "STF_PHASE_CC_DIR_CHGS";
	case STF_PHASE_CT_LIN_UPDTS:
		return "STF_PHASE_CT_LIN_UPDTS";
	case STF_PHASE_CT_LIN_HASH_EXCPT:
		return "STF_PHASE_CT_LIN_HASH_EXCPT";
	case STF_PHASE_CT_FILE_DELS:
		return "STF_PHASE_CT_FILE_DELS";
	case STF_PHASE_CT_DIR_DELS:
		return "STF_PHASE_CT_DIR_DELS";
	case STF_PHASE_CT_DIR_LINKS:
		return "STF_PHASE_CT_DIR_LINKS";
	case STF_PHASE_CT_DIR_LINKS_EXCPT:
		return "STF_PHASE_CT_DIR_LINKS_EXCPT";
	case STF_PHASE_CT_STALE_DIRS:
		return "STF_PHASE_CT_STALE_DIRS";
	case STF_PHASE_FLIP_LINS:
		return "STF_PHASE_FLIP_LINS";
	case STF_PHASE_IDMAP_SEND:
		return "STF_PHASE_IDMAP_SEND";
	case STF_PHASE_DOMAIN_MARK:
		return "STF_PHASE_DOMAIN_MARK";
	case STF_PHASE_DATABASE_CORRECT:
		return "STF_PHASE_DATABASE_CORRECT";
	case STF_PHASE_DATABASE_RESYNC:
		return "STF_PHASE_DATABASE_RESYNC";
	case STF_PHASE_CC_COMP_RESYNC:
		return "STF_PHASE_CC_COMP_RESYNC";
	case STF_PHASE_COMP_CONFLICT_CLEANUP:
		return "STF_PHASE_COMP_CONFLICT_CLEANUP";
	case STF_PHASE_COMP_CONFLICT_LIST:
		return "STF_PHASE_COMP_CONFLICT_LIST";
	case STF_PHASE_CT_COMP_DIR_LINKS:
		return "STF_PHASE_CT_COMP_DIR_LINKS";
	case STF_PHASE_COMP_COMMIT:
		return "STF_PHASE_COMP_COMMIT";
	case STF_PHASE_COMP_MAP_PROCESS:
		return "STF_PHASE_COMP_MAP_PROCESS";
	case STF_PHASE_COMP_FINISH_COMMIT:
		return "STF_PHASE_COMP_FINISH_COMMIT";
	case STF_PHASE_CT_COMP_DIR_DELS:
		return "STF_PHASE_COMP_DIR_DELS";
	case STF_PHASE_FINISH:
		return "STF_PHASE_FINISH";
	default:
		return "Unknown";
	}
}

bool
siq_job_exists(const char *pol_id)
{
	DIR *dirp = NULL;
	struct dirent *dp = NULL;
	bool found = false;

	ASSERT(strlen(pol_id) == POLICY_ID_LEN);

	dirp = opendir(sched_get_run_dir());
	if (dirp == NULL) {
		/* run dir doesn't exist yet */
		goto out;
	}

	while ((dp = readdir(dirp))) {
		if (dp->d_type != DT_DIR)
			continue;

		if (strncmp(dp->d_name, pol_id, POLICY_ID_LEN) == 0) {
			found = true;
			break;
		}
	}

out:
	if (dirp)
		closedir(dirp);
	return found;
}

struct isi_error *
safe_snprintf(char *s, size_t n, const char *format, ...)
{
	int ret;
	struct isi_error *error = NULL;
	va_list arguments;
	
	va_start(arguments, format);
	ret = vsnprintf(s, n, format, arguments);
	va_end(arguments);

	if (ret < 0 || (size_t)ret >= n) {
		error = isi_system_error_new(EINVAL, "Failed to snprintf: "
		    "buffer too small");
	}
	return error;
}

/* Wake up the scheduler. Used to tell the scheduler that there is something
 * to do, and it should check for jobs to run immediately. */
void
siq_wake_sched()
{
	FILE *f = NULL;

	f = fopen(SIQ_SCHED_EVENT_FILE, "w");

	if (f != NULL)
		fclose(f);
}

void
free_and_clean_pvec(struct ptr_vec *vec)
{
	void *elem_pp = NULL;
	while (!pvec_empty(vec)) {
		pvec_pop(vec, &elem_pp);
		free(elem_pp);
		elem_pp = NULL;
	}
	pvec_clean(vec);
}

/**
 * Returns the compliance domain version of complain domain
 */
int
get_compliance_dom_ver(const char *path, struct isi_error **error_out)
{
	int compliance_dom_ver = 0;
	int ret = -1;
	struct isi_error *error = NULL;
	struct domain_set doms;

	ifs_domainid_t worm_id;
	struct domain_entry worm_entry = {};

	domain_set_init(&doms);

	/*
	 * Skip compliance check for ifs root.
	 * We never set domain on root lin.
	 */
	if (strcmp(path, "/ifs") == 0 || strcmp(path, "/ifs/") == 0)
		goto out;

	ret = dom_get_info_by_path(path, &doms, NULL, NULL);
	if (ret) {
		error = isi_system_error_new(errno,
		    "Could not get domain info for %s", path);
		goto out;
	}

	ret = dom_get_matching_domains(&doms, DOM_WORM, 1, &worm_id,
	    &worm_entry);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Could not get domain info for %s", path);
		goto out;
	}

	if (ret == 0) {
		goto out;
	}

	if ((worm_entry.d_flags & COMPLIANCE_FLAGS) == COMPLIANCE_FLAGS)
		compliance_dom_ver =
		    (worm_entry.d_flags & COMPLIANCE_V2_FLAGS) ==
		    COMPLIANCE_V2_FLAGS ? 2 : 1;

out:
	domain_set_clean(&doms);
	isi_error_handle(error, error_out);
	return compliance_dom_ver;
}

/* Returns the appropriate tmp dir name based on if this is
 * a restore job or not. */
void
get_tmp_working_dir_name(char *buf, bool restore)
{
	ASSERT(buf != NULL);

	if (restore) {
		snprintf(buf, MAXNAMELEN, "%s_restore", TMP_WORKING_DIR);
	} else {
		snprintf(buf, MAXNAMELEN, "%s", TMP_WORKING_DIR);
	}
}

/*
 * This function ensures that the policy is rooted at a compliance domain
 * root if we are dealing with a compliance domain.
 */
unsigned
check_compliance_domain_root(char *policy_path, struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;
	struct domain_set doms;
	ifs_domainid_t compdom_id = 0;
	struct domain_entry compdom_entry = {};
	uint32_t d_flags = 0;
	struct stat st;
	bool is_has_compliance = false;

	domain_set_init(&doms);

	/*
	 * Skip compliance check for ifs root.
	 * We never set domain on root lin.
	 */
	if (strcmp(policy_path, "/ifs") == 0 ||
	    strcmp(policy_path, "/ifs/") == 0) {
		goto out;
	}

	is_has_compliance = is_or_has_compliance_dir(policy_path, &error);
	if (error || !is_has_compliance)
		goto out;

	if (stat(policy_path, &st) != 0) {
		error = isi_system_error_new(errno,
		    "Failed to stat the path %s", policy_path);
		goto out;
	}

	ret = dom_get_info_by_lin(st.st_ino, st.st_snapid, &doms, NULL, NULL);
	if (ret) {
		error = isi_system_error_new(errno,
		    "Error reading domain info for %s", policy_path);
		goto out;
	}

	ret = dom_get_matching_domains(&doms, DOM_COMPLIANCE, 1,
	    &compdom_id, &compdom_entry);

	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Error reading domain info for %s", policy_path);
		goto out;
	}

	/*
	 * Check if the SyncIQ policy is rooted at a compliance
	 * domain root. If not, error out.
	 */
	if (ret == 0 || st.st_ino != compdom_entry.d_root) {
		error = isi_system_error_new(EPERM,
		    "SyncIQ policy for %s has to be rooted at a"
		    " compliance domain root.", policy_path);
		goto out;
	}
	d_flags = compdom_entry.d_flags;

out:
	domain_set_clean(&doms);
	isi_error_handle(error, error_out);
	return d_flags;
}


/*
 * Checks to see if a policy is allowed to perform failback.
 */
void
check_failback_allowed(struct siq_policy *policy, struct isi_error **error_out)
{
	struct siq_policy_path *path = NULL;
	struct isi_error *error = NULL;

	// Check for invalid run cases.
	if (policy->datapath) {
		if (policy->datapath->predicate &&
		    strlen(policy->datapath->predicate) > 0) {
			error = isi_system_error_new(EINVAL, "Policy has "
			    "predicates which are not supported for Failback");
			goto out;
		}

		SLIST_FOREACH(path, &policy->datapath->paths, next) {
			if (path->type == SIQ_INCLUDE ||
			    path->type == SIQ_EXCLUDE) {
				error = isi_system_error_new(EINVAL, "Policy "
				    "has includes/excludes which are not "
				    "supported for Failback");
				goto out;
			}
		}
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Bug 270757 and PSCALE-13515
 * check if target_base has continous '/' such as /ifs////.ifsvar
 * under this case although the job shall fail with error
 * "Failure to mkdir is unrecoverable" but it may keep retry
 *
 * we should know that target path contains continous '/'
 * was already blocked through CLI
 * so any target_base reaching sworker still has '//' path was abnormal
 */

bool
passed_further_check(const char *target_base) {
    int len = strlen(target_base);
    // block target path that was purly '/ifs/'
    if (len <= 5)
        return false;
    for (int i = 0; i < len - 1; ++i) {
        if (target_base[i] == '/') {
            if (target_base[i + 1] == '/') {
                return false;
            }
        }
    }
    return true;
}
