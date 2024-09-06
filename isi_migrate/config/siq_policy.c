#include <sys/types.h>
#include <unistd.h>
#include <md5.h>

#include "siq_source.h"
#include "siq_target.h"
#include "isi_date/date.h"
#include "isi_migrate/sched/siq_jobctl.h"
#include "isi_migrate/sched/sched_utils.h"
#include "isi_migrate/migr/isirep.h"

#include "isi_util/isi_format.h"
#include "isi_util/isi_guid.h"
#include "isi_migrate/config/siq_alert.h"
#include "isi_snapshot/snapshot.h"
#include "isi_snapshot/snapshot_utils.h"
#include "siq_btree_public.h"

#define SIQ_POLICIES_LOCK_FILE	SIQ_IFS_CONFIG_DIR ".tsm-policies.xml.lock"
#define MAX_REPORT_COUNT 2000
#define ROTATE_PERIOD 31536000

#define SAFE_STRDUP(dst, src) if (src) { dst = strdup(src); ASSERT(dst); }

struct gci_tree siq_gc_policies_gci_root = {
	.root = &gci_ivar_siq_policies_root,
	.primary_config = "/ifs/.ifsvar/modules/tsm/config/siq-policies.gc",
	.fallback_config_ro = NULL,
	.change_log_file = NULL
};

struct siq_policies * 
siq_policies_create() 
{
	struct siq_policies *to_return = NULL;
	
	to_return = calloc(1, sizeof(struct siq_policies));
	ASSERT(to_return);
	
	return to_return;
}

static struct siq_policies * 
siq_policies_load_internal(bool needs_lock, int lock_type, 
    struct isi_error **error_out)
{
	struct siq_policies *policies = NULL;
	struct isi_error *error = NULL;

	policies = siq_policies_create();

	if (needs_lock) {
		policies->lock_fd = acquire_lock(SIQ_POLICIES_LOCK_FILE, 
		    lock_type, &error);
		if (error) {
			goto out;
		}
	} else {
		policies->lock_fd = -1;
	}
	policies->internal_lock = needs_lock;
	
	//Load policies from gconfig
	policies->gci_base = gcfg_open(&siq_gc_policies_gci_root, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	policies->gci_ctx = gci_ctx_new(policies->gci_base, true, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(policies->gci_ctx, "", &policies->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}

out:
	if (error) {
		siq_policies_free(policies);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return policies;
	}
}

struct siq_policies *
siq_policies_load(struct isi_error **error_out) 
{
	return siq_policies_load_internal(true, O_EXLOCK, error_out);
}

struct siq_policies *
siq_policies_load_nolock(struct isi_error **error_out)
{
	return siq_policies_load_internal(false, -1, error_out);
}

struct siq_policies * 
siq_policies_load_readonly(struct isi_error **error_out)
{
	struct siq_policies *policies;
	struct isi_error *isierror = NULL;
	
	policies = siq_policies_load_internal(true, O_SHLOCK, &isierror);
	if (policies == NULL) {
		isi_error_handle(isierror, error_out);
		return NULL;
	}
	
	if (policies->lock_fd >= 0) {
		close(policies->lock_fd);
		policies->lock_fd = -1;
	}
	return policies;
}

void
siq_policies_set_upgrade(struct siq_policies *pl)
{
	pl->upgrading = true;
}

bool 
siq_policy_can_add(struct siq_policies *policies, struct siq_policy *policy, 
    struct isi_error **error_out)
{
	struct siq_policy *found = NULL;
	struct isi_error *isierror = NULL;
       
	if (!siq_policy_is_valid(policy, &isierror))
		goto errout;

	found = siq_policy_get_by_name(policies, policy->common->name);
	if (found) {
		isi_error_free(isierror);
		isierror = isi_siq_error_new(E_SIQ_CONF_EXIST,
				"ERROR : found policy with same name = %s",
				policy->common->name);
		goto errout;
	} else {
		return true;
	}

errout:
	isi_error_handle(isierror, error_out);
	return false;
}

/*
 * Given a schedule string, return a pointer to the recurring date
 * and a pointer to the UTC time.
 *
 * Given schedule = "123456|Tomorrow at noon",
 * the recurring date is "Tomorrow at noon" and the UTC time is 123456.
 *
 * @input:
 * schedule	SIQ schedule string in the form: UTC time | recurring date
 * @output:
 * recur_str	recurring date from schedule
 * base_time	UTC time from schedule
 * error_out	contains any error information
 */
static void
get_schedule_info(char *schedule, char **recur_str, time_t *base_time,
    struct isi_error **error_out);

bool 
siq_policy_is_valid(const struct siq_policy *policy, 
    struct isi_error **error_out)
{
	siq_plan_t *plan;
	bool res = false;
	time_t base_time;
	char *recur_str = NULL;
	struct dp_record *date_record = NULL;
	struct isi_error *isierror = NULL;
	
	if (!policy->common->name || *policy->common->name == '\0') {
		isierror = isi_siq_error_new(E_SIQ_CONF_INVAL, 
		    "policy with id='%s' has an empty name", 
		    policy->common->pid);
		goto errout;
	}
	
	if (policy->common->name[0] == '-') {
		isierror = isi_siq_error_new(E_SIQ_CONF_INVAL,
		    "policy name cannot begin with a dash");
		goto errout;
	}

	/* XXX siq_src_t src; */

	if (!policy->target->dst_cluster_name ||
		*policy->target->dst_cluster_name == '\0') {
		isierror = isi_siq_error_new(E_SIQ_CONF_INVAL, "policy '%s' "
		    "has an empty destination cluster", policy->common->name);
		goto errout;
	}

	if (policy->datapath->predicate && 
	    strlen(policy->datapath->predicate) > 0) {
		plan = siq_parser_parse(policy->datapath->predicate, time(NULL));
		if (!plan) {
			isierror = isi_siq_error_new(E_SIQ_CONF_INVAL,
			    "policy '%s' has an incorrect predicate",
			    policy->common->name, policy->datapath->predicate);
			goto errout;
		}
		siq_plan_free(plan);
		plan = NULL;
	}

	if (policy->common->log_level < 0 ||
		policy->common->log_level >= SEVERITY_TOTAL) {
		isierror = isi_siq_error_new(E_SIQ_CONF_INVAL, "policy '%s' "
			"has an illegal log_level '%d'", policy->common->name,
			policy->common->log_level);
		goto errout;
	}

	if (policy->scheduler->schedule &&
	    strlen(policy->scheduler->schedule) > 0) {
		get_schedule_info(policy->scheduler->schedule,
		    &recur_str, &base_time, &isierror);
		if (isierror) {
			goto errout;
		}

		if (strcasecmp(recur_str, SCHED_ALWAYS) != 0 &&
		    strcasecmp(recur_str, SCHED_SNAP)) {
			parse_date(recur_str, &base_time, NULL,
			    &date_record, &isierror);
			if (isierror) {
				isi_error_free(isierror);
				isierror = isi_siq_error_new(E_SIQ_CONF_INVAL,
				    "policy '%s' has an illegal schedule '%s': "
				    "unable to parse", policy->common->name,
				    policy->scheduler->schedule);
				goto errout;
			}

			if (date_record->type == DP_REC_NON_RECURRING) {
				isierror = isi_siq_error_new(E_SIQ_CONF_INVAL,
				    "policy '%s' has an illegal schedule '%s':"
				    " nonrecurring date", policy->common->name,
				policy->scheduler->schedule);
				goto errout;
			}
		}
	}

	res = true;

errout:
	free(recur_str);
	dp_record_free(date_record);
	isi_error_handle(isierror, error_out);
	return res;
}

struct siq_pids_list *
siq_get_pids(const struct siq_policies *pl)
{
	struct siq_pids_list *pids_list;
	int count = 0;
	struct siq_policy *current = NULL;

	ASSERT(pl);
	
	pids_list = calloc(1, sizeof(struct siq_pids_list));
	ASSERT(pids_list);
	SLIST_FOREACH(current, &pl->root->policy, next) {
		pids_list->size++;
	}

	pids_list->ids = calloc(pids_list->size, sizeof(char *));
	ASSERT(pids_list->ids);
	SLIST_FOREACH(current, &pl->root->policy, next) {
		pids_list->ids[count] = current->common->pid;
		count++;
	}

	return pids_list;
}

void
siq_free_pids(struct siq_pids_list *pidsl)
{
	if (!pidsl)
		return;

	free(pidsl->ids);
	pidsl->ids = NULL;
	free(pidsl);
}

static int
siq_policy_datapath_compare(const void *a, const void *b)
{
	struct siq_policy_path *a_in = *(struct siq_policy_path **)a;
	struct siq_policy_path *b_in = *(struct siq_policy_path **)b;
	
	//We do not need to compare the type, because paths are guaranteed
	//to not be both an include and an exclude
	
	return strcmp(a_in->path, b_in->path);
}

static char * 
siq_calc_datapath_hash(const struct siq_policy *policy) 
{
	MD5_CTX md;
	int len;
	struct siq_policy_datapath *dp = policy->datapath;
	struct siq_policy_path *src_path;
	char* root_path = NULL;
	char *to_return = NULL;
	int sorted_len = 0;
	int pointer = 0;
	struct siq_policy_path **sorted = NULL;

	MD5Init(&md);

	root_path = siq_root_path_calc(policy);
	if (strcmp(root_path, dp->root_path)) {
		MD5Update(&md, dp->root_path, 
		    strlen(dp->root_path));
	}
	free(root_path);
	root_path = NULL;
	
	
	if (policy->target->path && (len = strlen(policy->target->path))) {
		MD5Update(&md, policy->target->path, len);
	}
	
	//Sort include/exclude paths first, since order matters
	SLIST_FOREACH(src_path, &dp->paths, next) {
		sorted_len++;
	}
	sorted = calloc(sizeof(struct siq_policy_path *), sorted_len);
	ASSERT(sorted);
	SLIST_FOREACH(src_path, &dp->paths, next) {
		sorted[pointer] = src_path;
		pointer++;
	}
	qsort(sorted, sorted_len, sizeof(struct siq_policy_path *), 
	    siq_policy_datapath_compare);
	pointer = 0;
	while (pointer < sorted_len) {
		MD5Update(&md, (unsigned char *)&sorted[pointer]->type,
				sizeof sorted[pointer]->type);
		if (sorted[pointer]->path && 
		    (len = strlen(sorted[pointer]->path)))
			MD5Update(&md, sorted[pointer]->path, len);
		pointer++;
	}
	free(sorted);

	if (dp->predicate && (len = strlen(dp->predicate)))
		MD5Update(&md, dp->predicate, len);

	to_return = calloc(1, POLICY_ID_LEN + 1);
	ASSERT(to_return);
	MD5End(&md, to_return);
 
 	return to_return;
}

static size_t 
siq_policies_save_hash_entry_key_size(const void *key)
{
	return strlen(key);
}

static void 
siq_policies_save_hash_entry_free(struct siq_policies_save_hash_entry *entry) 
{
	free(entry);
	entry = NULL;
}

static void
siq_policies_save_hash_empty(struct isi_hash *hash)
{
	struct siq_policies_save_hash_entry *elem = NULL;
	
	ISI_HASH_FOREACH(elem, hash, hash_entry) {
		isi_hash_remove(hash, &elem->hash_entry);
		siq_policies_save_hash_entry_free(elem);
	}
}

static void
siq_policies_save_hash_add(struct isi_hash *hash, char *key)
{
	struct siq_policies_save_hash_entry *to_add = NULL;
	
	to_add = calloc(1, sizeof(struct siq_policies_save_hash_entry) + 
	    strlen(key) + 1);
	ASSERT(to_add);
	strcpy(to_add->hash_key, key);
	isi_hash_add(hash, &to_add->hash_entry);
}

void
siq_policy_force_update_datapath_hash(struct siq_policies *policies, 
    struct siq_policy *policy)
{
	ASSERT(policies->upgrading);
	
	if (policy->datapath->datapath_hash)
		free(policy->datapath->datapath_hash);
	policy->datapath->datapath_hash = siq_calc_datapath_hash(policy);
}

int 
siq_policies_save(struct siq_policies* policies, struct isi_error **error_out)
{
	int ret = -1;
	struct siq_policy *current = NULL;
	struct isi_hash pid_hash;
	struct isi_hash name_hash;
	struct siq_source_record *srec = NULL;
	char *new_datapath_hash = NULL;
	struct isi_error *isierror = NULL;

	ASSERT(policies);
	ASSERT(policies->lock_fd >= 0 || !policies->internal_lock);
	
	//Verify that IDs and names are unique (use struct isi_hash)
	ISI_HASH_VAR_INIT(&pid_hash, siq_policies_save_hash_entry, 
	    hash_key, hash_entry, siq_policies_save_hash_entry_key_size, 0);
	ISI_HASH_VAR_INIT(&name_hash, siq_policies_save_hash_entry, 
	    hash_key, hash_entry, siq_policies_save_hash_entry_key_size, 0);

	SLIST_FOREACH(current, &policies->root->policy, next) {
		struct isi_hash_entry *found = NULL;
		
		found = isi_hash_get(&pid_hash, current->common->pid);
		if (found) {
			isierror = isi_siq_error_new(E_SIQ_CONF_EXIST, 
			    "Policy with id '%s' already exists",
			    current->common->pid);
			goto out;
		}
		siq_policies_save_hash_add(&pid_hash, current->common->pid);
		found = NULL;
		
		found = isi_hash_get(&name_hash, current->common->name);
		if (found) {
			isierror = isi_siq_error_new(E_SIQ_CONF_EXIST, 
			    "Policy with name '%s' already exists",
			    current->common->name);
			goto out;
		}
		siq_policies_save_hash_add(&name_hash, current->common->name);

		if (!siq_source_has_record(current->common->pid)) {
			siq_source_create(current->common->pid, &isierror);
			if (isierror)
				goto out;
		}
		
		if (!policies->upgrading) {
			//Check to see if any policies need full sync 
			//(root_path, includes, excludes, predicate)
			new_datapath_hash = siq_calc_datapath_hash(current);
			if (!current->datapath->datapath_hash || 
			    strcmp(new_datapath_hash, 
			    current->datapath->datapath_hash) != 0) {
				srec =
				    siq_source_record_load(current->common->pid,
				        &isierror);
				if (isierror)
					goto out;

				siq_source_force_full_sync(srec, &isierror);
				if (isierror)
					goto out;

				siq_source_record_save(srec, &isierror);
				if (isierror)
					goto out;

				siq_source_record_free(srec);
				srec = NULL;
				free(current->datapath->datapath_hash);
				current->datapath->datapath_hash = 
				    new_datapath_hash;
			} else {
				free(new_datapath_hash);
				new_datapath_hash = NULL;
			}
		}

		if (current->coordinator->deep_copy >=
		    SIQ_DEEP_COPY_CHANGED_FLAG) {
			// The deep copy setting has been changed and
			// requires a full sync.
			srec = siq_source_record_load(current->common->pid,
			    &isierror);
			if (isierror)
				goto out;

			siq_source_force_full_sync(
			    srec, &isierror);
			if (isierror)
				goto out;

			siq_source_record_save(srec, &isierror);
			if (isierror)
				goto out;

			siq_source_record_free(srec);
			srec = NULL;

			current->coordinator->deep_copy -=
			    SIQ_DEEP_COPY_CHANGED_FLAG;
			ASSERT(current->coordinator->deep_copy >=
			    SIQ_DEEP_COPY_DENY &&
			    current->coordinator->deep_copy <=
			    SIQ_DEEP_COPY_FORCE);
		}
	}

	gci_ctx_write_path(policies->gci_ctx, "", policies->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_commit(policies->gci_ctx, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_free(policies->gci_ctx);
	policies->gci_ctx = gci_ctx_new(policies->gci_base, true, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_read_path(policies->gci_ctx, "", &policies->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	ret = 0;

out:
	if (srec != NULL)
		siq_source_record_free(srec);

	//Iterate and remove everything from hash
	siq_policies_save_hash_empty(&pid_hash);
	siq_policies_save_hash_empty(&name_hash);
	//Destroy
	isi_hash_destroy(&pid_hash);
	isi_hash_destroy(&name_hash);
	
	isi_error_handle(isierror, error_out);
	return ret;
}

void 
siq_policies_free(struct siq_policies *policies)
{
	if (!policies)
		return;

	if (policies->gci_ctx)
		gci_ctx_free(policies->gci_ctx);
	if (policies->gci_base)
		gcfg_close(policies->gci_base);
	
	if (policies->lock_fd >= 0) {
		close(policies->lock_fd);
		policies->lock_fd = -1;
	}
	
	free(policies);
}

static struct siq_policy_common *
_siq_policy_common_create(void)
{
	struct siq_policy_common *spc = NULL;

	CALLOC_AND_ASSERT(spc, struct siq_policy_common);
	spc->log_level = NOTICE;
	spc->creation_time = time(NULL);
	spc->state = SIQ_ST_ON;

	return spc;
}

static struct siq_policy_scheduler *
_siq_policy_scheduler_create(void)
{
	struct siq_policy_scheduler *sps = NULL;

	CALLOC_AND_ASSERT(sps, struct siq_policy_scheduler);
	SAFE_STRDUP(sps->schedule, "");
	sps->max_reports = MAX_REPORT_COUNT;
	sps->rotate_report_period = ROTATE_PERIOD;
	SAFE_STRDUP(sps->snapshot_sync_pattern, DEFAULT_SNAPSHOT_SYNC_PATTERN);

	return sps;
}

static struct siq_policy_coordinator *
_siq_policy_coordinator_create(void)
{
	struct siq_policy_coordinator *spc = NULL;
	
	CALLOC_AND_ASSERT(spc, struct siq_policy_coordinator);
	spc->workers_per_node = 3;
	spc->check_integrity = 1;
	spc->limit_work_item_retry = 1;
	SAFE_STRDUP(spc->snapshot_pattern, DEFAULT_SNAP_PATTERN);
	SAFE_STRDUP(spc->snapshot_alias, DEFAULT_SNAP_ALIAS);
	return spc;
}

static struct siq_policy_datapath *
_siq_policy_datapath_create(void)
{
	struct siq_policy_datapath *spd = NULL;

	CALLOC_AND_ASSERT(spd, struct siq_policy_datapath);
	SAFE_STRDUP(spd->predicate, "");
	spd->src_snapshot_mode = SIQ_SNAP_MAKE;

	return spd;
}

static struct siq_policy_target *
_siq_policy_target_create(void)
{
	struct siq_policy_target *spt = NULL;

	CALLOC_AND_ASSERT(spt, struct siq_policy_target);
	spt->work_item_lock_mod = 10;
	SAFE_STRDUP(spt->restrict_by, "");

	return spt;
}

struct siq_policy *
siq_policy_create(void)
{
	struct siq_policy *policy;
	policy = calloc(1, sizeof(struct siq_policy));
	ASSERT(policy);

	policy->common = _siq_policy_common_create();
	policy->scheduler =_siq_policy_scheduler_create();
	policy->coordinator = _siq_policy_coordinator_create();
	policy->datapath = _siq_policy_datapath_create();
	policy->target = _siq_policy_target_create();

	return policy;
}

struct siq_policy *
siq_policy_get_by_pid(struct siq_policies *policies, const char *pid)
{
	struct siq_policy *current = NULL;
	
	ASSERT(policies);
	SLIST_FOREACH(current, &policies->root->policy, next) {
		if (strcmp(pid, current->common->pid) == 0) {
			return current;
		}
	}
	return NULL;
}

struct siq_policy *
siq_policy_get_by_mirror_pid(struct siq_policies *policies, const char *pid)
{
	struct siq_policy *current = NULL;
	
	ASSERT(policies);
	SLIST_FOREACH(current, &policies->root->policy, next) {
		if (current->common->mirror_pid &&
		    strcmp(pid, current->common->mirror_pid) == 0) {
			return current;
		}
	}
	return NULL;
}

struct siq_policy *
siq_policy_get_by_name(struct siq_policies *policies, const char *name)
{
	struct siq_policy *current = NULL;
	
	ASSERT(policies);
	SLIST_FOREACH(current, &policies->root->policy, next) {
		if (strcmp(name, current->common->name) == 0) {
			return current;
		}
	}
	return NULL;
}

struct siq_policy *
siq_policy_get(struct siq_policies *pl, const char *input, 
    struct isi_error **error_out)
{
	struct isi_error *isierror = NULL;
	struct siq_policy *found = NULL;

	ASSERT(pl);
	if (!input) {
		isierror = isi_siq_error_new(E_SIQ_CONF_NOENT, 
		    "policy selector is empty (no name, no id)");
		goto errout;
	}
	
	found = siq_policy_get_by_pid(pl, input);
	if (!found) {
		found = siq_policy_get_by_name(pl, input);
	}
	
	if (!found) {
		isierror = isi_siq_error_new(E_SIQ_CONF_NOENT, 
		    "a policy with a name or id of '%s' was not found",
		    input);
		goto errout;
	}
	return found;

errout:
	isi_error_handle(isierror, error_out);
	return NULL;
}

int
siq_policy_remove(struct siq_policies *policies, struct siq_policy *policy, 
    struct isi_error **error_out)
{
	char buffer[SIQ_MAX_TARGET_GUID_LEN];
	char empty_guid[SIQ_MAX_TARGET_GUID_LEN];
	struct isi_error *isierror = NULL;
	int ret = -1;
	bool need_remove_assocs = false;
	struct siq_source_record *srec = NULL;
	
	ASSERT(policies);
	ASSERT(policy);

	// If a policy is or has an associated failback/original policy, we
	// want to remove the association on the target.
	if (policy->common->mirror_pid)
		need_remove_assocs = true;

	if (!siq_source_has_record(policy->common->pid) &&
	    !need_remove_assocs) {
		siq_policy_remove_local(policies, policy, NULL, &isierror);
		goto out;
	}

	memset(empty_guid, 0, SIQ_MAX_TARGET_GUID_LEN);

	if (siq_source_has_record(policy->common->pid)) {
		srec = siq_source_record_load(policy->common->pid, &isierror);
		if (isierror)
			goto out;

		siq_source_get_target_guid(srec, buffer);
	}

	if (memcmp(buffer, empty_guid, SIQ_MAX_TARGET_GUID_LEN) == 0 &&
	    !need_remove_assocs) {
		siq_policy_remove_local(policies, policy, NULL, &isierror);
		goto out;
	}

	/*
	 * If the policy is already queued for deletion, dont try 
	 * to set delete flag.
	 */
	if (srec && !siq_source_is_policy_deleted(srec)) {
		siq_source_set_policy_delete(srec);
		siq_source_record_save(srec, &isierror);
		if (isierror) {
			goto out;
		}
	}

	siq_policy_run(policies, policy->common->pid, INVALID_SNAPID,
	    SIQ_JOB_RUN, &isierror);
	if (isierror) {
		goto out;
	}
	
	ret = 0;
out:
	siq_source_record_free(srec);
	isi_error_handle(isierror, error_out);
	return ret;
}

int
siq_policy_remove_by_pid(struct siq_policies *policies, char *pid, 
    struct isi_error **error_out)
{
	struct siq_policy *policy = NULL;
	
	ASSERT(policies);
	policy = siq_policy_get_by_pid(policies, pid);
	return siq_policy_remove(policies, policy, error_out);
}

int
siq_policy_remove_by_name(struct siq_policies *policies, char *name, 
    struct isi_error **error_out)
{
	struct siq_policy *policy = NULL;
	
	ASSERT(policies);
	policy = siq_policy_get_by_name(policies, name);
	return siq_policy_remove(policies, policy, error_out);
}

/**
 * Helper function that deletes directories in a Safe Manner(tm).
 */
static int
remove_directory_helper(const char *directory, const char *job_id)
{
	char work_dir[MAXPATHLEN + 1];
	char rm_work_dir[MAXPATHLEN + 8];
	int node = -1;
	int ret = 0;
	
	node = sched_get_my_node_number();
	
	sprintf(work_dir, "%s/%d/%s", sched_get_edit_dir(), node, job_id);
	sprintf(rm_work_dir, "rm -rf %s", work_dir);
	ret = system(rm_work_dir);
	if (ret != 0 && ret != ENOENT) {
		goto done;
	}
	ret = rename(directory, work_dir);
	if (ret == ENOENT) {
		ret = 0;
		goto done;
	} else if (ret != 0) {
		goto done;
	}
	ret = system(rm_work_dir);
	
done:
	log(DEBUG, "remove_directory_helper: Returning %d (%s)", ret, work_dir);
	return ret;
}

static void
delete_policy_reportdb_entries(char *policy_id, struct isi_error **error_out)
{
	sqlite3 *db = NULL;
	struct isi_error *error = NULL;
	
	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (error) {
		log(ERROR, "%s: Failed to open report db", __func__);
		goto out;
	}
	
	siq_reportdb_delete_by_policyid(db, policy_id, &error);
	if (error) {
		log(ERROR, "%s: Failed to clear reports from db for policy %s",
		    __func__, policy_id);
		goto out;
	}

	siq_reports_status_delete_by_policyid(db, policy_id, &error);
	if (error) {
		log(ERROR, "%s: Failed to clear reports status from db for "
		    "policy %s", __func__, policy_id);
		goto out;
	}

out:
	if (db)
		siq_reportdb_close(db);
	isi_error_handle(error, error_out);
}

// Remove temporary target record created for failback prep restore, if not a 
// local policy.
void
delete_prep_restore_trec(const char *policy_id, struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;	

	lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
	if (lock_fd == -1 || error)
		goto out;

	if (!siq_target_record_exists(policy_id))
		goto out;
	
	trec = siq_target_record_read(policy_id, &error);
	if (error)
		goto out;

	// A temporary restore target record doesn't have any domain info 
	// filled in. Source cluster name/guid are also missing. If local
	// policy, we are reusing the target record and these values are
	// filled in.
	if (!trec->domain_id) {
		siq_target_delete_record(policy_id, true, true, &error);
		if (error)
			goto out;
	}
	
out:
	if (lock_fd != -1)
		close(lock_fd);

	siq_target_record_free(trec);

	isi_error_handle(error, error_out);
}

int
siq_policy_remove_local(struct siq_policies *policies,
    struct siq_policy *policy, char *job_dir, struct isi_error **error_out)
{
	char remove_dir[MAXPATHLEN + 10];
	ifs_snapid_t *delete_id = NULL;
	ifs_snapid_t latest_id;
	ifs_snapid_t new_id;
	int ret = 0;
	int i = 0;
	bool res = false;
	struct siq_source_record *srec = NULL;
	DIR *dirp;
	struct dirent *d;
	char fn[MAXPATHLEN + 1];
	struct isi_error *isierror = NULL;

	ASSERT(policies);

	/* Clean up the entries from reports DB */
	delete_policy_reportdb_entries(policy->common->pid, &isierror);
	if (isierror)
		goto errout;

	/* Clean up report dir */
	snprintf(remove_dir, sizeof(remove_dir),  "%s/%s",
	    sched_get_reports_dir(), policy->common->pid);
	ret = remove_directory_helper(remove_dir, policy->common->pid);
	if (ret != 0) {
		log(INFO, "siq_policy_remove_local: "
		    "failed to delete report dir");
	}

	/* Cleanup job dir */
	sprintf(remove_dir, "%s/%s", sched_get_jobs_dir(), policy->common->pid);
	ret = remove_directory_helper(remove_dir, policy->common->pid);
	if (ret != 0) {
		log(INFO, "siq_policy_remove_local: "
		    "failed to delete job dir");
	}

	/* Delete btrees */
	siq_btree_remove_source(policy->common->pid, &isierror);
	if (isierror) {
		log(INFO, "siq_policy_remove_local: "
			    "failed to delete btree dir");
		isi_error_free(isierror);	
		isierror = NULL;
	}

	/*
	 * Remove any temporary target record entries associated due to
	 * resync prep command.
	 */
	delete_prep_restore_trec(policy->common->pid, &isierror);
	if (isierror)
		goto errout;
	
	SLIST_REMOVE(&policies->root->policy, policy, siq_policy, next);

	ret = siq_policies_save(policies, &isierror);
	if (isierror)
		goto errout;

	/* Remove the source db entry */
	if (siq_source_has_record(policy->common->pid)) {
		srec = siq_source_record_load(policy->common->pid, &isierror);
		if (isierror)
			goto errout;

		/* Delete snapshots */
		res = siq_source_should_del_latest_snap(srec, &isierror); 
		if (isierror)
			goto errout;

		if (res) {
			siq_source_get_latest(srec, &latest_id);
			delete_snapshot_helper(latest_id);
		}

		res = siq_source_should_del_new_snap(srec, &isierror);
		if (isierror)
			goto errout;

		if (res) {
			siq_source_get_new(srec, &new_id);
			delete_snapshot_helper(new_id);
		}

		siq_source_get_delete(srec, &delete_id);
		if (delete_id) {
			for (i = 0; i < SIQ_SOURCE_RECORD_DELETE_SNAP_SIZE; i++)
				delete_snapshot_helper(delete_id[i]);
		}

		siq_source_delete(policy->common->pid, &isierror);
		if (isierror)
			goto errout;
	}

	/* If policy is removed, no point in keeping any alerts related to
	 * running the policy */
	if (policy->common->name) {
		siq_cancel_all_alerts_for_policy(policy->common->name,
		    policy->common->pid);
	}

	/* Remove policy's alerts gconfig file and corresponding locks */
	siq_gc_active_alert_cleanup(policy->common->pid);

	/* Clear out the scheduler running job dir */
	if (job_dir != NULL) {
		ret = remove_directory_helper(job_dir, policy->common->pid);
		if (ret != 0) {
			log(INFO, "siq_policy_remove_local: "
			    "failed to delete run dir");
		}
	}

	/* Clear out the scheduler edit job dirs */
	dirp = opendir(sched_get_edit_dir());
	if (dirp != NULL) {
		while ((d = readdir(dirp))) {
			/* We are only looking for the node number dirs */
			if (atoi(d->d_name) <= 0) {
				continue;
			}
			snprintf(remove_dir, sizeof(remove_dir), 
			    "rm -rf %s/%s/%s", sched_get_edit_dir(), 
			    d->d_name, policy->common->pid);
			system(remove_dir);
		}
		closedir(dirp);
	}

	/* Bug 186554 - Remove idmap file */
	snprintf(fn, sizeof(fn), "%s/%s_source.gz", SIQ_IFS_IDMAP_DIR,
	    policy->common->pid);
	ret = unlink(fn);
	if (ret == -1 && errno != ENOENT)
		log(INFO, "%s: failed to delete idmap file %s: %s", __func__,
		    fn, strerror(errno));

	/* Remove job lock file */
	siq_job_rm_lock_file(policy->common->pid);

	/* Free orphaned policy */
	siq_policy_free(policy);

errout:
	siq_source_record_free(srec);

	isi_error_handle(isierror, error_out);
	return 0;
}

bool
siq_policy_add(struct siq_policies *policies, struct siq_policy *policy, 
    struct isi_error **error_out)
{
	struct isi_error *isierror = NULL;
	char *policy_new_hash = NULL;
	char *current_hash = NULL;
	struct siq_policy *current = NULL;
	bool result = false;

	ASSERT(policies, "siq_policy_add: policies cannot be NULL!");
	ASSERT(policy, "siq_policy_add: policy cannot be NULL!");

	if (!siq_policy_is_valid(policy, &isierror)) {
		goto out;
	}
	
	/* If the ID is blank, create a new one, otherwise, leave it */
	if (!policy->common->pid) {
		policy->common->pid = siq_create_policy_id(false);
	}
	
	/* Compute policy hash */
	policy_new_hash = siq_calc_policy_hash(policy);
	/* Iterate over all existing policies */
	SLIST_FOREACH(current, &policies->root->policy, next) {
		/* Skip checking against any existing policy with same id */
		if (strcmp(policy->common->pid, current->common->pid) == 0) {
			isierror = isi_siq_error_new(E_SIQ_CONF_EXIST,
			    "Policy with id '%s' already exists",
			    policy->common->pid);
			goto out;
		}
		
		/* Ensure that policy name is unique */
		if (strcmp(policy->common->name, 
		    current->common->name) == 0) {
			isierror = isi_siq_error_new(E_SIQ_CONF_EXIST,
			    "Policy with name '%s' already exists",
			    current->common->name);
			goto out;
		}
		/* Compute policy hash */
		current_hash = siq_calc_policy_hash(current);
		if (strcmp(policy_new_hash, current_hash) == 0) {
			isierror = isi_siq_error_new(E_SIQ_CONF_EXIST,
			    "No duplicate policy information sets allowed.\n"
			    "Policy '%s' already contains this information "
			    "set.\nAn information set consists of the source "
			    "path(s) and the file selection criteria.",
			    current->common->name);
			goto out;
		}
		free(current_hash);
		current_hash = NULL;
	}
	
	/* okay to add */
	SLIST_INSERT_HEAD(&policies->root->policy, policy, next);
	result = true;
	
out:
	free(policy_new_hash);
	free(current_hash);
	isi_error_handle(isierror, error_out);
	return result;
}

char * 
siq_root_path_calc(const struct siq_policy *policy)
{
	char* output = NULL;
	struct siq_policy_path *src_path;
	int len = 0;
	int i;
	
	SLIST_FOREACH(src_path, &policy->datapath->paths, next) {
		if (src_path->type != SIQ_INCLUDE) {
			continue;
		}
		if (output == NULL) {
			output = strdup(src_path->path);
			len = strlen(output);
			continue;
		}
		if (!strncmp(output, src_path->path, len) && 
		    src_path->path[len] == '/')
			continue;
		
		for (i = 0; src_path->path[i] == output[i]; i++);
		
		for (; src_path->path[i] != '/'; i--);
		output[i] = 0;
		len = i;
	}
	if (output == NULL) {
		return strdup("");
	} else {
		return output;
	}
}

char * 
siq_calc_policy_hash(const struct siq_policy *policy) 
{
	MD5_CTX md;
	char *buffer = NULL;
	int len;
	int type = 0;
	struct siq_policy_path *src_path;
	char* root_path = NULL;

	MD5Init(&md);

	//We got rid of the SYSTEM/USER policy types, so hardcode this
	MD5Update(&md, &type, sizeof(int));

	if (policy->datapath->src_cluster_name && 
	    (len = strlen(policy->datapath->src_cluster_name)))
		MD5Update(&md, policy->datapath->src_cluster_name, len);
	/* If the calculated root_path is the same as the root_path, then skip
	 * using root_root path for MD5.  This is for backwards compatibility.
	 * Note that root_path might be NULL on upgrade. */
	if (policy->datapath->root_path != NULL) {
		root_path = siq_root_path_calc(policy);
		if (strcmp(root_path, policy->datapath->root_path)) {
			MD5Update(&md, policy->datapath->root_path, 
			    strlen(policy->datapath->root_path));
		}
		free(root_path);
		root_path = NULL;
	}
	SLIST_FOREACH(src_path, &policy->datapath->paths, next) {
		MD5Update(&md, (unsigned char *)&src_path->type,
				sizeof src_path->type);
		if (src_path->path && (len = strlen(src_path->path)))
			MD5Update(&md, src_path->path, len);
	}
	
	if (policy->target->dst_cluster_name && 
	    (len = strlen(policy->target->dst_cluster_name)))
 		MD5Update(&md, policy->target->dst_cluster_name, len);
 	if (policy->target->path && (len = strlen(policy->target->path)))
 		MD5Update(&md, policy->target->path, len);

	if (policy->datapath->predicate && 
	    (len = strlen(policy->datapath->predicate)))
		MD5Update(&md, policy->datapath->predicate, len);

	buffer = calloc(1, POLICY_ID_LEN + 1);
	MD5End(&md, buffer);
 
 	return buffer;
}

char *
siq_create_policy_id(bool timestamp) {
        unsigned char *buffer = NULL;
        const char *idstr;
        struct fmt FMT_INIT_CLEAN(fmt);
        char *to_return = NULL;

        buffer = calloc(1, POLICY_ID_LEN);
        ASSERT(buffer != NULL);
	if (timestamp) {
		isi_guid_generate(buffer, POLICY_ID_LEN);
	} else {
	        isi_guid_random_generate(buffer, POLICY_ID_LEN);
	}
        fmt_print(&fmt, "%{}", arr_guid_fmt(buffer));
        idstr = fmt_string(&fmt);
        to_return = calloc(1, POLICY_ID_LEN + 1);
        strncpy(to_return, idstr, POLICY_ID_LEN);
        
        free(buffer);
        fmt_clean(&fmt);

        return to_return;
}

static struct siq_policy_common *
siq_policy_common_copy(struct siq_policy_common *src_pc)
{
	struct siq_policy_common *dst_pc = NULL;
	
	dst_pc = calloc(1, sizeof(struct siq_policy_common));
	ASSERT(dst_pc);
	
	memcpy(dst_pc, src_pc, sizeof(struct siq_policy_common));
	SAFE_STRDUP(dst_pc->name, src_pc->name);
	SAFE_STRDUP(dst_pc->desc, src_pc->desc);
	SAFE_STRDUP(dst_pc->pid, src_pc->pid);
	SAFE_STRDUP(dst_pc->mirror_pid, src_pc->mirror_pid);
	
	return dst_pc;
}

static struct siq_policy_scheduler *
siq_policy_scheduler_copy(struct siq_policy_scheduler *src_ps)
{
	struct siq_policy_scheduler *dst_ps = NULL;
	
	dst_ps = calloc(1, sizeof(struct siq_policy_scheduler));
	ASSERT(dst_ps);

	memcpy(dst_ps, src_ps, sizeof(struct siq_policy_scheduler));
	SAFE_STRDUP(dst_ps->schedule, src_ps->schedule);
	SAFE_STRDUP(dst_ps->snapshot_sync_pattern,
	    src_ps->snapshot_sync_pattern);

	return dst_ps;
}

static struct siq_policy_coordinator *
siq_policy_coordinator_copy(struct siq_policy_coordinator *src_pc)
{
	struct siq_policy_coordinator *dst_pc = NULL;
	
	dst_pc = calloc(1, sizeof(struct siq_policy_coordinator));
	ASSERT(dst_pc);
	
	memcpy(dst_pc, src_pc, sizeof(struct siq_policy_coordinator));
	SAFE_STRDUP(dst_pc->rename_pattern, src_pc->rename_pattern);
	SAFE_STRDUP(dst_pc->snapshot_pattern, src_pc->snapshot_pattern);
	SAFE_STRDUP(dst_pc->snapshot_alias, src_pc->snapshot_alias);
	
	return dst_pc;
}

static void
siq_policy_paths_copy(struct siq_policy_path_head *dst, 
    struct siq_policy_path_head *src)
{
	struct siq_policy_path *current_src = NULL;
	struct siq_policy_path *last_added = NULL;
	struct siq_policy_path *to_add = NULL;
	
	SLIST_FOREACH(current_src, src, next) {
		to_add = calloc(1, sizeof(struct siq_policy_path));
		ASSERT(to_add);
		
		to_add->type = current_src->type;
		SAFE_STRDUP(to_add->path, current_src->path);
		
		if (last_added) {
			SLIST_INSERT_AFTER(last_added, to_add, next);
		} else {
			SLIST_INSERT_HEAD(dst, to_add, next);
		}
		last_added = to_add;
	}
}

static struct siq_policy_datapath *
siq_policy_datapath_copy(struct siq_policy_datapath *src_pd)
{
	struct siq_policy_datapath *dst_pd = NULL;
	
	dst_pd = calloc(1, sizeof(struct siq_policy_datapath));
	ASSERT(dst_pd);
	
	memcpy(dst_pd, src_pd, sizeof(struct siq_policy_datapath));
	SAFE_STRDUP(dst_pd->src_cluster_name, src_pd->src_cluster_name);
	SAFE_STRDUP(dst_pd->datapath_hash, src_pd->datapath_hash);
	SAFE_STRDUP(dst_pd->root_path, src_pd->root_path);
	dst_pd->paths.slh_first = NULL;
	siq_policy_paths_copy(&dst_pd->paths, &src_pd->paths);
	SAFE_STRDUP(dst_pd->predicate, src_pd->predicate);
	
	return dst_pd;
}

static struct siq_policy_target *
siq_policy_target_copy(struct siq_policy_target *src_pt)
{
	struct siq_policy_target *dst_pt = NULL;
	
	dst_pt = calloc(1, sizeof(struct siq_policy_target));
	ASSERT(dst_pt);
	
	memcpy(dst_pt, src_pt, sizeof(struct siq_policy_target));
	SAFE_STRDUP(dst_pt->dst_cluster_name, src_pt->dst_cluster_name);
	SAFE_STRDUP(dst_pt->password, src_pt->password);
	SAFE_STRDUP(dst_pt->path, src_pt->path);
	SAFE_STRDUP(dst_pt->restrict_by, src_pt->restrict_by);
	
	return dst_pt;
}

struct siq_policy *
siq_policy_copy(struct siq_policy *src_policy)
{
	struct siq_policy *dst_policy = NULL;
	
	dst_policy = calloc(1, sizeof(struct siq_policy));
	ASSERT(dst_policy);
	
	dst_policy->common = siq_policy_common_copy(src_policy->common);
	dst_policy->scheduler =
	    siq_policy_scheduler_copy(src_policy->scheduler);
	dst_policy->coordinator = 
	    siq_policy_coordinator_copy(src_policy->coordinator);
	dst_policy->datapath = siq_policy_datapath_copy(src_policy->datapath);
	dst_policy->target = siq_policy_target_copy(src_policy->target);
	
	return dst_policy;
}

void 
siq_policy_common_free(struct siq_policy_common *common)
{
	if (common) {
		FREE_AND_NULL(common->name);
		FREE_AND_NULL(common->desc);
		FREE_AND_NULL(common->pid);
		FREE_AND_NULL(common->mirror_pid);
	}
}

void
siq_policy_scheduler_free(struct siq_policy_scheduler *scheduler)
{
	if (scheduler) {
		FREE_AND_NULL(scheduler->schedule);
		FREE_AND_NULL(scheduler->snapshot_sync_pattern);
	}
}

void
siq_policy_coordinator_free(struct siq_policy_coordinator *coordinator)
{
	if (coordinator) {
		FREE_AND_NULL(coordinator->rename_pattern);
		FREE_AND_NULL(coordinator->snapshot_pattern);
		FREE_AND_NULL(coordinator->snapshot_alias);
	}
}

void 
siq_policy_paths_free(struct siq_policy_path_head *paths)
{
	struct siq_policy_path *path = NULL;
	while (!SLIST_EMPTY(paths)) {
		path = SLIST_FIRST(paths);
		SLIST_REMOVE_HEAD(paths, next);
		FREE_AND_NULL(path->path);
		FREE_AND_NULL(path);
	}
}

void 
siq_policy_datapath_free(struct siq_policy_datapath *datapath)
{
	if (datapath) {
		FREE_AND_NULL(datapath->src_cluster_name);
		FREE_AND_NULL(datapath->datapath_hash);
		FREE_AND_NULL(datapath->root_path);
		siq_policy_paths_free(&datapath->paths);
		FREE_AND_NULL(datapath->predicate);
	}
}

void 
siq_policy_target_free(struct siq_policy_target *target)
{
	if (target) {
		FREE_AND_NULL(target->dst_cluster_name);
		FREE_AND_NULL(target->password);
		FREE_AND_NULL(target->path);
		FREE_AND_NULL(target->restrict_by);
	}
}

void 
siq_policy_free(struct siq_policy* policy)
{
	if (policy) {
		siq_policy_common_free(policy->common);
		FREE_AND_NULL(policy->common);
		siq_policy_scheduler_free(policy->scheduler);
		FREE_AND_NULL(policy->scheduler);
		siq_policy_coordinator_free(policy->coordinator);
		FREE_AND_NULL(policy->coordinator);
		siq_policy_datapath_free(policy->datapath);
		FREE_AND_NULL(policy->datapath);
		siq_policy_target_free(policy->target);
		FREE_AND_NULL(policy->target);
		
		FREE_AND_NULL(policy);
	}
}

static ifs_snapid_t
validate_snap_for_policy(struct siq_policy *policy,
    struct siq_source_record *srec, char *manual_snap,
    struct isi_error **error_out)
{
	ifs_snapid_t manual_sid = INVALID_SNAPID;
	ifs_snapid_t latest_sid = INVALID_SNAPID;
	SNAP *snap = NULL;
	char *root_path = NULL;
	char *cur_path = NULL;
	int i;
	bool snap_contains_root = false;
	struct isi_str isisnapstr;
	struct isi_str *snapname = NULL;
	struct isi_str **snap_paths = NULL;
	struct isi_error *error = NULL;

	if (name_all_digits(manual_snap)) {
		manual_sid = strtoull(manual_snap, 0, 10);
		snap = snapshot_open_by_sid(manual_sid, NULL);
	} else {
		isi_str_init(&isisnapstr, manual_snap, strlen(manual_snap) + 1,
		    ENC_DEFAULT, ISI_STR_NO_MALLOC);
		snap = snapshot_open_by_name(&isisnapstr, NULL);
		if (snap)
			snapshot_get_snapid(snap, &manual_sid);
	}
	
	if (!snap) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Unable to open manually specified snapshot %s",
		    manual_snap);
		goto out;
	}

	snapname = snapshot_get_name(snap);
	if (strncmp(ISI_STR_GET_STR(snapname),
	    "SIQ-", 4) == 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Manually specifying a snapshot generated by SyncIQ (%s) "
		    "is not supported",
		    ISI_STR_GET_STR(snapname));
		goto out;
	}

	/* snapid must be greater than the one used for previous sync */
	siq_source_get_latest(srec, &latest_sid);
	if (latest_sid >= manual_sid) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Manually specified snapshot (snapid %llu) is the same or "
		    "older than snapshot (snapid %llu) used for previous sync",
		    manual_sid, latest_sid);
		goto out;
	}

	/* snapshot must contain policy root directory */
	root_path = policy->datapath->root_path;
	snap_paths = snapshot_get_paths(snap);
	for (i = 0; snap_paths[i]; i++) {
		cur_path = ISI_STR_GET_STR(snap_paths[i]);
		if (strncmp(root_path, cur_path, strlen(cur_path)) != 0)
			continue;

		/* if snapshot path is /ifs/a and policy root path is /ifs/ab,
		 * snapshot does *not* contain policy root! */
		if ((strlen(root_path) > strlen(cur_path)) &&
		    (root_path[strlen(cur_path)] != '/'))
			continue;

		snap_contains_root = true;
	}
	if (!snap_contains_root) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Manually specified snapshot %s does not contain policy "
		    "root path %s", manual_snap, root_path);
		goto out;
	}

out:
	if (snap)
		snapshot_close(snap);

	if (error)
		manual_sid = INVALID_SNAPID;

	if (snap_paths) {
		for (i = 0; snap_paths[i]; i++)
			isi_str_free(snap_paths[i]);
		free(snap_paths);
	}

	if (snapname) {
		isi_str_free(snapname);
	}

	isi_error_handle(error, error_out);
	return manual_sid;
}

static int
siq_policy_execnow(struct siq_policies *pl, char *pid, 
    enum siq_job_action job_type, char *manual_snap, 
    struct isi_error** error_out)
{
	struct isi_error* error = NULL;
	struct siq_policy *policy = NULL;
	struct siq_source_record *srec = NULL;
	enum siq_job_action pending_job;
	int res = -1;
	char *sched = NULL;
	ifs_snapid_t man_snapid = INVALID_SNAPID;
	ifs_snapid_t tmp_snapid = INVALID_SNAPID;

	policy = siq_policy_get_by_pid(pl, pid);
	ASSERT(policy);

	srec = siq_source_record_load(pid, &error);
	if (error)
		goto out;

	ASSERT(srec);

	/* Check conditions depending on the schedule */
	if (strcmp(policy->scheduler->schedule, "")) {
		sched = strchr(policy->scheduler->schedule, '|');
		if (!sched) {
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
			    "Policy schedule is in an unexpected format, "
			    "missing separator '|'");
			goto out;
		}

		/* Snapshot-triggered sync conditions */
		if (!strcasecmp(sched + 1, SCHED_SNAP)) {
			if (job_type == SIQ_JOB_RUN &&
			    !siq_source_is_policy_deleted(srec)) {
				error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
				    "Snapshot-triggered syncs cannot be"
				    " started manually");
				goto out;
			}
		}
	}

	if (manual_snap) {
		/* validate manually specified snapshot for policy */
		man_snapid = validate_snap_for_policy(policy, srec,
		    manual_snap, &error);
		if (error)
			goto out;

		siq_source_get_new(srec, &tmp_snapid);
		if (tmp_snapid != INVALID_SNAPID) {
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
			    "Previous policy run used snapid %llu and did "
			    "not complete. Job must either complete with "
			    "previously used snapshot, or the policy may be "
			    "reset to use a different snapshot", tmp_snapid);
			goto out;
		}

		siq_source_get_manual(srec, &tmp_snapid);
		if (tmp_snapid == INVALID_SNAPID) {
			siq_source_set_manual_snap(srec, man_snapid);
		} else if (tmp_snapid != man_snapid) {
			printf("error\n");
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
			    "Previous policy run specified manual snapid %llu "
			    "which does not match currently specified manual "
			    "snapid %llu. Job must either complete with "
			    "previously used snapshot, or the policy may be "
			    "reset to use a different snapshot",
			    tmp_snapid, man_snapid);
			goto out;
		}
	}

	siq_source_get_pending_job(srec, &pending_job);

	if (pending_job != SIQ_JOB_NONE)
		goto out;

	siq_source_set_pending_job(srec, job_type);

	//Save source record
	siq_source_record_save(srec, &error);
	if (error)
		goto out;

	res = 0;

out:
	siq_source_record_free(srec);

	isi_error_handle(error, error_out);
	return res;
}

/* Runs a policy immediately, given a desired job type. 
 * SIQ_JOB_RUN -> standard policy run. Also supported: SIQ_JOB_ASSESS,
 * SIQ_JOB_FAILBACK_PREP, SIQ_JOB_FAILBACK_PREP_RESTORE,
 * SIQ_JOB_FAILBACK_PREP_FINALIZE.
 */
int
siq_policy_run(struct siq_policies *pl, char *pid, char *manual_snap,
    enum siq_job_action job_type, struct isi_error** error_out)
{
	return siq_policy_execnow(pl, pid, job_type, manual_snap, error_out);
}

int
siq_policy_make_runnable(const struct siq_policies *pl, char *pid,
    struct isi_error **error_out)
{
	struct isi_error* error = NULL;
	struct siq_policy *policy = NULL;
	struct siq_source_record *srec = NULL;
	int success = -1;

	srec = siq_source_record_load(pid, &error);
	if (error)
		goto out;

	siq_source_set_runnable(srec);
	siq_source_set_pending_job(srec, SIQ_JOB_NONE);
	siq_source_clear_node_conn_failed(srec);

	siq_source_record_save(srec, &error);
	if (!error) {
		success = 0;
	}
	
	policy = siq_policy_get_by_pid((struct siq_policies *)pl, pid);
	ASSERT(policy);
	log_syslog(LOG_INFO, "API: Policy %s has been resolved.", policy->common->name);

out:
	siq_source_record_free(srec);

	isi_error_handle(error, error_out);
	return success;
}

int
siq_policy_set_manual_failback_prep(const struct siq_policies *pl, char *pid,
    struct isi_error **error_out)
{
	struct isi_error* error = NULL;
	struct siq_source_record *srec = NULL;
	int success = -1;

	log(NOTICE, "%s", __func__);

	srec = siq_source_record_load(pid, &error);
	if (error)
		goto out;

	siq_source_set_manual_failback_prep(srec, true);
	siq_source_record_save(srec, &error);
	if (!error) {
		success = 0;
	}
out:
	siq_source_record_free(srec);

	isi_error_handle(error, error_out);
	return success;
}

int
siq_policy_force_full_sync(const struct siq_policies *pl, char *pid,
    struct isi_error **error_out)
{
	struct isi_error* error = NULL;
	struct siq_policy *policy = NULL;
	struct siq_source_record *srec = NULL;
	int success = -1;

	srec = siq_source_record_load(pid, &error);
	if (error)
		goto out;

	siq_source_force_full_sync(srec, &error);
	if (error)
		goto out;

	siq_source_record_save(srec, &error);
	if (error)
		goto out;
out:
	siq_source_record_free(srec);

	if (!error) {
		success = 0;
		policy = siq_policy_get_by_pid((struct siq_policies *)pl, pid);
		ASSERT(policy);
		log_syslog(LOG_INFO, "API: Policy '%s' has been reset.", policy->common->name);
	}

	isi_error_handle(error, error_out);
	return success;
}

struct siq_policy *
siq_policy_create_from_spec(struct siq_spec *spec)
{
	struct siq_policy *to_return = siq_policy_create();
	
	to_return->common = siq_policy_common_copy(spec->common);
	to_return->coordinator =
	    siq_policy_coordinator_copy(spec->coordinator);
	to_return->datapath = siq_policy_datapath_copy(spec->datapath);
	to_return->target = siq_policy_target_copy(spec->target);
	
	return to_return;
}

struct siq_spec *
siq_spec_create(void)
{
	struct siq_spec *to_return = NULL;
	
	CALLOC_AND_ASSERT(to_return, struct siq_spec);
	
	to_return->common = _siq_policy_common_create();
	to_return->coordinator = _siq_policy_coordinator_create();
	to_return->datapath = _siq_policy_datapath_create();
	to_return->target = _siq_policy_target_create();
	
	return to_return;
}

struct siq_spec *
siq_spec_create_from_policy(struct siq_policy *policy)
{
	struct siq_spec *to_return = NULL;
	
	CALLOC_AND_ASSERT(to_return, struct siq_spec);
	
	to_return->common = policy->common;
	to_return->coordinator = policy->coordinator;
	to_return->datapath = policy->datapath;
	to_return->target = policy->target;
	
	return to_return;
}

struct siq_spec *
siq_spec_copy_from_policy(struct siq_policy *policy)
{
	struct siq_spec *to_return = NULL;

	CALLOC_AND_ASSERT(to_return, struct siq_spec);

	to_return->common = siq_policy_common_copy(policy->common);
	to_return->coordinator =
	    siq_policy_coordinator_copy(policy->coordinator);
	to_return->datapath = siq_policy_datapath_copy(policy->datapath);
	to_return->target = siq_policy_target_copy(policy->target);

	return to_return;
}

struct siq_spec *
siq_spec_copy(struct siq_spec *src)
{
	struct siq_spec *dst = NULL;
	
	CALLOC_AND_ASSERT(dst, struct siq_spec);
	
	dst->common = siq_policy_common_copy(src->common);
	dst->coordinator = siq_policy_coordinator_copy(src->coordinator);
	dst->datapath = siq_policy_datapath_copy(src->datapath);
	dst->target = siq_policy_target_copy(src->target);
	
	return dst;
}

void
siq_spec_free(struct siq_spec *spec)
{
	//Do not free any of the components, 
	//it is just a copy of structs from the policies object
	free(spec);
}

void
siq_report_spec_free(struct siq_spec *spec)
{
	//Full free
	if (!spec) {
		return;
	}
	siq_policy_common_free(spec->common);
	FREE_AND_NULL(spec->common);
	siq_policy_coordinator_free(spec->coordinator);
	FREE_AND_NULL(spec->coordinator);
	siq_policy_datapath_free(spec->datapath);
	FREE_AND_NULL(spec->datapath);
	siq_policy_target_free(spec->target);
	FREE_AND_NULL(spec->target);
	
	FREE_AND_NULL(spec);
}

void
siq_spec_save(struct siq_policy *policy, char *path, 
    struct isi_error **error_out)
{
	struct gci_tree siq_spec_gci_root = {
	    .root = &gci_ivar_siq_spec,
	    .primary_config = path,
	    .fallback_config_ro = NULL
	};
	struct gci_base *gcib = NULL;
	struct gci_ctx *gcic = NULL;
	struct siq_spec spec;
	struct isi_error *error = NULL;
	
	gcib = gcfg_open(&siq_spec_gci_root, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	gcic = gci_ctx_new(gcib, false, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	spec.common = policy->common;
	spec.coordinator = policy->coordinator;
	spec.datapath = policy->datapath;
	spec.target = policy->target;
	
	gci_ctx_write_path(gcic, "", &spec, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_commit(gcic, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
out:
	isi_error_handle(error, error_out);
	if (gcic) {
		gci_ctx_free(gcic);
	}
	if (gcib) {
		gcfg_close(gcib);
	}
}

struct siq_gc_spec *
siq_gc_spec_load(char *path, struct isi_error **error_out)
{
	struct siq_gc_spec *to_return = NULL;
	struct isi_error *error = NULL;
	
	to_return = calloc(1, sizeof(struct siq_gc_spec));
	ASSERT(to_return);
	
	to_return->tree.root = &gci_ivar_siq_spec;
	to_return->tree.primary_config = strdup(path);
	
	to_return->gci_base = gcfg_open(&to_return->tree, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	to_return->gci_ctx = gci_ctx_new(to_return->gci_base, true, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(to_return->gci_ctx, "", &to_return->spec, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}

out:
	if (error) {
		siq_gc_spec_free(to_return);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return to_return;
	}
}

void
siq_gc_spec_free(struct siq_gc_spec *spec)
{
	if (!spec)
		return;
	if (spec->gci_ctx)
		gci_ctx_free(spec->gci_ctx);
	if (spec->gci_base)
		gcfg_close(spec->gci_base);
	if (spec->tree.primary_config) {
		free((char *)spec->tree.primary_config);
	}
	FREE_AND_NULL(spec)
}

void
siq_policy_paths_insert(struct siq_policy_path_head *head, const char *path,
    enum siq_src_pathtype type)
{
	struct siq_policy_path *to_add = NULL;
	
	CALLOC_AND_ASSERT(to_add, struct siq_policy_path);
	to_add->path = strdup(path);
	to_add->type = type;
	SLIST_INSERT_HEAD(head, to_add, next);
}


// Create a dummy policy for Failover using the target record to write out
// a basic job spec.

struct siq_policy *
siq_failover_policy_create_from_target_record(
    const struct target_record *record, const bool revert)
{
	ASSERT(record && record->id);
	
	struct siq_policy *pol = NULL;
	char *local_cluster_name;
	if (!record->policy_name)
		goto out;

	if (!record->target_dir)
		goto out;

	local_cluster_name = strdup("localhost");
	pol = siq_policy_create();

	pol->common->pid = strdup(record->id);
	pol->common->name = strdup(record->policy_name);
	pol->common->action = revert ? SIQ_ACT_FAILOVER_REVERT :
	    SIQ_ACT_FAILOVER;
	pol->datapath->root_path = strdup(record->target_dir);
	pol->datapath->src_cluster_name = local_cluster_name; 
	pol->target->dst_cluster_name = strdup(local_cluster_name);
	pol->target->path = strdup(record->target_dir);
	pol->coordinator->dst_snapshot_mode = SIQ_SNAP_NONE;

out:
	return pol;
}

char *
siq_generate_failback_policy_name(char *root_name, 
    struct siq_policies *policies)
{
	char *res = NULL;

	int i = 1;

	static const uint32_t STRLEN_MAXUINT32 = 9;
	static const uint32_t STRLEN_FAILBACK = 8;

	if (!root_name)
		goto out;

	res = malloc(strlen(root_name) + 1 + STRLEN_FAILBACK + 1 
	    + STRLEN_MAXUINT32);
	if (!res)
		goto out;

	sprintf(res, "%s_mirror", root_name);
	if (!siq_policy_get_by_name(policies, res)) {
		goto out;
	}
	
	do {
		sprintf(res, "%s_mirror_%u", root_name, i++);
	} while (siq_policy_get_by_name(policies, res));

out:
	return res;
}

static void
get_schedule_info(char *schedule, char **recur_str, time_t *base_time,
    struct isi_error **error_out)
{
	char *recur_buf = NULL, *recur_ptr = NULL;
	struct isi_error *isierror = NULL;

	recur_buf = strdup(schedule);
	ASSERT(recur_buf);

	/* Recurring string for a schedule entry has the format:
	 * UTC time | recurring date
	 */
	if ((recur_ptr = strchr(recur_buf, '|')) == NULL) {
		isierror = isi_siq_error_new(E_SIQ_CONF_INVAL,
		    "illegal format for recurring date: %s",
		    recur_buf);
		goto out;
	}

	*recur_ptr = '\0';
	recur_ptr++;
	*base_time = strtoul(recur_buf, (char **)NULL, 10);
	*recur_str = strdup(recur_ptr);
	ASSERT(*recur_str);

out:
	free(recur_buf);
	isi_error_handle(isierror, error_out);
}
