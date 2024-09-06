#include "siq_target.h"
#include "siq_btree_public.h"

#include <isi_xml/xmlutil.h>
#include <isi_migrate/migr/isirep.h>

#include <sys/stat.h>
#include <md5.h>

#define SIQ_TARGET_RECORDS_DIR SIQ_IFS_CONFIG_DIR "target_records"
#define SIQ_TARGET_RECORDS_LOCK_DIR SIQ_IFS_CONFIG_DIR "target_record_locks"

#define SIQ_XML_VERSION "1.0"
#define SIQ_XML_ENCODING "UTF-8"

#define NODE_TARGET_STATUS "target-status"
#define NODE_POLICY "policy"
#define NODE_ID "id"
#define NODE_POLICY_NAME "policy-name"
#define NODE_SRC_CLUSTER_NAME "src-cluster-name"
#define NODE_SRC_CLUSTER_ID "src-cluster-id"
#define NODE_TARGET_DIR "target-dir"
#define NODE_LAST_SRC_COORD_IP "last-src-coord-ip"
#define NODE_JOB_STATUS "job-status"
#define NODE_JOB_STATUS_TIMESTAMP "job-status-timestamp"
#define NODE_READ_ONLY "read-only"
#define NODE_MONITOR_NODE_ID "monitor-node-id"
#define NODE_MONITOR_PID "monitor-pid"
#define NODE_CANCEL_STATE "cancel-state"
#define NODE_CANT_CANCEL "cant-cancel"
#define NODE_ALLOW_TARGET_DIR_OVERLAP "allow-target-dir-overlap"
#define NODE_LINMAP_CREATED "linmap-created"
#define NODE_DOMAIN_ID "domain-id"
#define NODE_DOMAIN_GENERATION "domain-generation"
#define NODE_LATEST_SNAP "latest-snap-id"
#define NODE_FOFB_STATE "fofb-state"
#define NODE_ENABLE_HASH_TMPDIR "enable-hash-tmpdir"
#define NODE_LATEST_ARCHIVE_SNAP_ALIAS "latest-archive-snap-alias"
#define NODE_LATEST_ARCHIVE_SNAP "latest-archive-snap"
#define NODE_COMPOSITE_JOB_VER_COMMON "composite-job-ver-common"
#define NODE_COMPOSITE_JOB_VER_LOCAL "composite-job-ver-local"
#define NODE_PREV_COMPOSITE_JOB_VER_COMMON "prev-composite-job-ver-common"
#define NODE_PREV_COMPOSITE_JOB_VER_LOCAL "prev-composite-job-ver-local"
#define NODE_DO_COMP_COMMIT_WORK "do-comp-commit-work"

/* 5.0 policy id's have the naming convention
 * <src_cluster>.<policy_name>.<tgt_path>, whereas 5.5+ id's are limited
 * to hexadecimal characters. We just need to locate a '.' to determine if
 * it's a 5.0 policy id. */
static inline bool
is_5_0_policy_id(const char *pid)
{
	return (strchr(pid, '.') != NULL);
}

/* Get the absolute path to the target record for the given policy id.
 * If the policy id is in the 5.0 format, we will hash it. This allows us
 * to store/access 5.0 style target records based on a hash of their pseudo
 * policy id. We need to store/access 5.0 records in this way because their
 * pseudo policy id's contain special characters, so naming their target
 * record <policy_id>.xml is not ideal, as we would have to escape all special
 * characters when we access it. It's also messy to create file names with
 * special characters present. */
static void
get_record_path(const char *pid, char *filename, size_t max_len,
    bool get_lock_path, struct isi_error **error_out)
{
	MD5_CTX md;
	char hashed_pid[POLICY_ID_LEN + 1];
	const char *safe_pid = pid;
	int ret = 0;
	struct isi_error *error = NULL;

	if (is_5_0_policy_id(pid)) {
		MD5Init(&md);
		MD5Update(&md, pid, strlen(pid));
		MD5End(&md, hashed_pid);
		safe_pid = hashed_pid;
	}

	if (strlen(safe_pid) != POLICY_ID_LEN) {
		error = isi_system_error_new(EINVAL, "Failed to get target "
		    " record path for invalid pid: %s", safe_pid);
		goto out;
	}

	if (get_lock_path) {
		ret = snprintf(filename, max_len, "%s/.%s.lock",
		    SIQ_TARGET_RECORDS_LOCK_DIR, safe_pid);
	} else {
		ret = snprintf(filename, max_len, "%s/%s.xml",
		    SIQ_TARGET_RECORDS_DIR, safe_pid);
	}

	if (ret < 0 || (size_t)ret >= max_len) {
		error = isi_system_error_new(EINVAL, "Failed to get path "
		    " to target record%s : path too long (dir = %s, pid = %s)",
		    get_lock_path ? "lock" : "",
		    get_lock_path ? SIQ_TARGET_RECORDS_LOCK_DIR :
		    SIQ_TARGET_RECORDS_DIR, pid);
	}

out:
	isi_error_handle(error, error_out);

}

static void
read_policy(xmlNodePtr policy_node, struct target_record* record)
{
	xmlNodePtr policy_child_node = NULL;
	
	ASSERT(record != NULL);
	
	policy_child_node = policy_node->children;
	while (policy_child_node != NULL) {
		xmlChar *value = NULL;
		
		value = xmlNodeGetContent(policy_child_node);
	
		if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_ID) == 0) {
			//<id>...</id>
			record->id = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_POLICY_NAME) == 0) {
			//<policy-name>...</policy-name>
			record->policy_name = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_SRC_CLUSTER_NAME) == 0) {
			//<src-cluster-name>...</src-cluster-name>
			record->src_cluster_name = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_SRC_CLUSTER_ID) == 0) {
			//<src-cluster-id>...</src-cluster-id>
			record->src_cluster_id = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_TARGET_DIR) == 0) {
			//<target-dir>...</target-dir>
			record->target_dir = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_LAST_SRC_COORD_IP) == 0) {
			//<last-src-coord-ip>...</last-src-coord-ip>
			record->last_src_coord_ip = strdup(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_JOB_STATUS) == 0) {
			//<job-status>...</job-status>
			record->job_status = text_to_job_state(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_JOB_STATUS_TIMESTAMP) == 0) {
			//<job-status-timestamp>...</job-status-timestamp>
			record->job_status_timestamp = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_READ_ONLY) == 0) {
			//<read-only>...</read-only>
			record->read_only = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_MONITOR_NODE_ID) == 0) {
			//<monitor-node-id>...</monitor-node-id>
			record->monitor_node_id = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_MONITOR_PID) == 0) {
			//<monitor-pid>...</monitor-pid>
			record->monitor_pid = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_CANCEL_STATE) == 0) {
			//<cancel-state>...</cancel-state>
			record->cancel_state = text_to_siq_target_cancel_state(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_CANT_CANCEL) == 0) {
			//<cant-cancel>...</cant-cancel>
			record->cant_cancel = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_ALLOW_TARGET_DIR_OVERLAP) == 0) {
			//<allow-target-dir-overlap>...</allow-target-dir-overlap>
			record->allow_target_dir_overlap = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_LINMAP_CREATED) == 0) {
			//<linmap-created>...</linmap-created>
			record->linmap_created = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_DOMAIN_ID) == 0) {
			//<domain-id>...</domain-id>
			record->domain_id = atoll(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_DOMAIN_GENERATION) == 0) {
			//<domain-generation>...</domain-generation>
			record->domain_generation = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name, 
		    (const xmlChar *)NODE_LATEST_SNAP) == 0) {
			//<latest-snap-id>...</latest-snap-id>
			record->latest_snap = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_FOFB_STATE) == 0) {
			//<fofb-state>...</fofb-state>
			record->fofb_state =
			    text_to_siq_target_fofb_state(value);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_ENABLE_HASH_TMPDIR) == 0) {
			//<enable-hash-tmpdir>...</enable-hash-tmpdir>
			record->enable_hash_tmpdir = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_LATEST_ARCHIVE_SNAP_ALIAS) == 0) {
			//<latest-archive-snap-alias>...</latest-archive-snap...
			record->latest_archive_snap_alias = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_LATEST_ARCHIVE_SNAP) == 0) {
			//<latest-archive-snap>...</latest-arhive-snap>
			record->latest_archive_snap = atoi(value);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_COMPOSITE_JOB_VER_LOCAL) == 0) {
			//<composite-job-ver-local>...</composite-job-ver-...
			record->composite_job_ver.local = strtoul(value,
			    NULL, 0);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_COMPOSITE_JOB_VER_COMMON) == 0) {
			//<composite-job-ver-common>...</composite-job-ver-...
			record->composite_job_ver.common = strtoul(value,
			    NULL, 0);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_PREV_COMPOSITE_JOB_VER_LOCAL) == 0) {
			//<prev-composite-job-ver-local>...</prev-composite-...
			record->prev_composite_job_ver.local = strtoul(value,
			    NULL, 0);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_PREV_COMPOSITE_JOB_VER_COMMON) == 0) {
			//<prev-composite-job-ver-common>...</prev-composite-...
			record->prev_composite_job_ver.common = strtoul(value,
			    NULL, 0);
		} else if (xmlStrcmp(policy_child_node->name,
		    (const xmlChar *)NODE_DO_COMP_COMMIT_WORK) == 0) {
			//<do-comp-commit-work>...</do-comp-commit-work>
			record->do_comp_commit_work = atoi(value);
		} else {
			//Unknown - log and keep going
			log(DEBUG, "read_policy: Unknown field encountered");
		}

		//Cleanup
		xmlFree(value);

		//Iterate
		policy_child_node = policy_child_node->next;
	}
}

struct target_record *
siq_target_record_read(const char *pid, struct isi_error **error_out)
{
	XmlTree tree = {};
	XmlResult *res = NULL;
	xmlNodePtr child;
	char record_filename[MAXPATHLEN + 1];
	struct target_record *rec = NULL;
	struct isi_error *error = NULL;

	get_record_path(pid, record_filename, sizeof(record_filename), false,
	    &error);
	if (error)
		goto out;

	res = configRead(&tree, record_filename);
	if (configErrCode(res) != CONF_OK)
		goto out;

	CONFIG_XML_FINDCHILD(child, tree.root, NODE_POLICY, res, out);
	ASSERT(child != NULL);

	rec = calloc(1, sizeof(*rec));
	ASSERT(rec);

	read_policy(child, rec);

out:
	if (configErrCode(res) != CONF_OK) {
		char *errmsg = configError(res);

		if (configErrCode(res) == CONF_NO_SUCH_FILE) {
			error = isi_siq_error_new(E_SIQ_CONF_NOENT, "%s",
			    errmsg);
		} else {
			error = isi_siq_error_new(E_SIQ_CONF_PARSE, "%s",
			    errmsg);
		}

		free(errmsg);
	}

	configFinish(&tree);
	configDestroyResult(res);

	if (error != NULL && rec != NULL) {
		siq_target_record_free(rec);
		rec = NULL;
	}
	
	isi_error_handle(error, error_out);
	return rec;
}

static void
write_policy(xmlNodePtr root_node, struct target_record* record)
{
	xmlNodePtr policy_node = NULL;
	xmlNodePtr current_node = NULL;
	char buffer[50];
	char *state = NULL;
	
	//<policy>
	policy_node = xmlNewChild(root_node, NULL, NODE_POLICY, NULL);
	ASSERT(policy_node != NULL);
	
	//<id>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_ID, record->id);
	ASSERT(current_node != NULL);
	
	//<name>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_POLICY_NAME, 
	    record->policy_name);
	ASSERT(current_node != NULL);
	
	//<src-cluster-name>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_SRC_CLUSTER_NAME, 
	    record->src_cluster_name);
	ASSERT(current_node != NULL);
	
	//<src-cluster-id>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_SRC_CLUSTER_ID, 
	    record->src_cluster_id);
	ASSERT(current_node != NULL);
	
	//<target-dir>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_TARGET_DIR,
	    record->target_dir);
	ASSERT(current_node != NULL);
	
	//<last-coord-ip>
	current_node = xmlNewTextChild(policy_node, NULL, NODE_LAST_SRC_COORD_IP, 
	    record->last_src_coord_ip);
	ASSERT(current_node != NULL);

	//<job-status>
	memset(buffer, 0, sizeof(buffer));
	state = job_state_to_text(record->job_status, false);
	sprintf(buffer, "%s", state);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_JOB_STATUS, buffer);
	ASSERT(current_node != NULL);
	free(state);
	
	//<job-status-timestamp>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%ld", record->job_status_timestamp);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_JOB_STATUS_TIMESTAMP, 
	    buffer);
	ASSERT(current_node != NULL);
	
	//<read-only>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->read_only);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_READ_ONLY, buffer);
	ASSERT(current_node != NULL);
	
	//<monitor-node-id>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->monitor_node_id);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_MONITOR_NODE_ID,
	    buffer);
	ASSERT(current_node != NULL);

	//<monitor-pid>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->monitor_pid);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_MONITOR_PID, buffer);
	ASSERT(current_node != NULL);

	//<cancel-state>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%s", siq_target_cancel_state_to_text(record->cancel_state));
	current_node = xmlNewTextChild(policy_node, NULL, NODE_CANCEL_STATE, buffer);
	ASSERT(current_node != NULL);

	//<cant-cancel>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->cant_cancel);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_CANT_CANCEL, buffer);
	ASSERT(current_node != NULL);
	
	//<allow-target-dir-overlap>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->allow_target_dir_overlap);
	current_node = xmlNewTextChild(policy_node, NULL, 
	    NODE_ALLOW_TARGET_DIR_OVERLAP, buffer);
	ASSERT(current_node != NULL);

	//<linmap-created>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->linmap_created);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_LINMAP_CREATED, buffer);
	ASSERT(current_node != NULL);
	
	//<domain-id>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%lld", record->domain_id);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_DOMAIN_ID, buffer);
	ASSERT(current_node != NULL);
	
	//<domain-generation>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->domain_generation);
	current_node = xmlNewTextChild(policy_node, NULL, NODE_DOMAIN_GENERATION, 
	    buffer);
	ASSERT(current_node != NULL);

	//<latest-snap-id>
	if (record->latest_snap) {
		memset(buffer, 0, sizeof(buffer));
		sprintf(buffer, "%d", record->latest_snap);
		current_node = xmlNewTextChild(policy_node, NULL, 
		    NODE_LATEST_SNAP, buffer);
		ASSERT(current_node != NULL);
	}

	//<fofb-state>
	if (record->fofb_state > FOFB_NONE) {
		memset(buffer, 0, sizeof(buffer));
		sprintf(buffer, "%s", 
		    siq_target_fofb_state_to_text(record->fofb_state));
		current_node = xmlNewTextChild(policy_node, NULL,
		    NODE_FOFB_STATE, buffer);
		ASSERT(current_node != NULL);
	}

	//<enable-hash-tmpdir>
	if (record->enable_hash_tmpdir) {
		memset(buffer, 0, sizeof(buffer));
		sprintf(buffer, "%d", record->enable_hash_tmpdir);
		current_node = xmlNewTextChild(policy_node, NULL,
		    NODE_ENABLE_HASH_TMPDIR, buffer);
		ASSERT(current_node != NULL);
	}

	//<latest-archive-snap-alias>
	if (record->latest_archive_snap_alias) {
		memset(buffer, 0, sizeof(buffer));
		sprintf(buffer, "%d", record->latest_archive_snap_alias);
		current_node = xmlNewChild(policy_node, NULL,
		    NODE_LATEST_ARCHIVE_SNAP_ALIAS, buffer);
		ASSERT(current_node != NULL);
	}

	//<latest-archive-snap>
	if (record->latest_archive_snap) {
		memset(buffer, 0, sizeof(buffer));
		sprintf(buffer, "%d", record->latest_archive_snap);
		current_node = xmlNewChild(policy_node, NULL,
		    NODE_LATEST_ARCHIVE_SNAP, buffer);
		ASSERT(current_node != NULL);
	}

	//<composite-job-ver-local>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%u", record->composite_job_ver.local);
	current_node = xmlNewTextChild(policy_node, NULL,
	    NODE_COMPOSITE_JOB_VER_LOCAL, buffer);
	ASSERT(current_node != NULL);

	//<composite-job-version-common>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%u", record->composite_job_ver.common);
	current_node = xmlNewTextChild(policy_node, NULL,
	    NODE_COMPOSITE_JOB_VER_COMMON, buffer);
	ASSERT(current_node != NULL);

	//<prev-composite-job-ver-local>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%u", record->prev_composite_job_ver.local);
	current_node = xmlNewTextChild(policy_node, NULL,
	    NODE_PREV_COMPOSITE_JOB_VER_LOCAL, buffer);
	ASSERT(current_node != NULL);

	//<prev-composite-job-version-common>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%u", record->prev_composite_job_ver.common);
	current_node = xmlNewTextChild(policy_node, NULL,
	    NODE_PREV_COMPOSITE_JOB_VER_COMMON, buffer);
	ASSERT(current_node != NULL);
	
	//<do-comp-commit-work>
	memset(buffer, 0, sizeof(buffer));
	sprintf(buffer, "%d", record->do_comp_commit_work);
	current_node = xmlNewTextChild(policy_node, NULL, 
	    NODE_DO_COMP_COMMIT_WORK, buffer);
	ASSERT(current_node != NULL);
}

void
siq_target_record_write(struct target_record *record, 
    struct isi_error **error_out)
{
	XmlTree ftree = {};
	XmlResult *res = NULL;
	char filename[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	ASSERT(record != NULL);

	get_record_path(record->id, filename, sizeof(filename), false, &error);
	if (error)
		goto out;

	ftree.doc = xmlNewDoc(SIQ_XML_VERSION);
	ASSERT(ftree.doc != NULL);

	ftree.doc->children = xmlNewDocNode(ftree.doc, NULL,
	    NODE_TARGET_STATUS, NULL);
	ASSERT(ftree.doc->children != NULL);

	write_policy(ftree.doc->children, record);

	res = configWrite(&ftree, filename, 0640);
	if (configErrCode(res) != CONF_OK) {
		char *errmsg = configError(res);
		error = isi_siq_error_new(E_SIQ_CONF_COMPOSE, "Error writing "
		    "%s: %s", filename, errmsg);
		free(errmsg);
	}

out:
	configFinish(&ftree);
	configDestroyResult(res);
	isi_error_handle(error, error_out);
}

void
siq_target_record_free(struct target_record* record)
{
	if (record == NULL)
		return;

	free(record->id);
	free(record->policy_name);
	free(record->src_cluster_name);
	free(record->src_cluster_id);
	free(record->target_dir);
	free(record->last_src_coord_ip);
	free(record);
}

void
siq_target_records_read(struct target_records **records,
    struct isi_error **error_out)
{
	struct target_records *head = NULL;
	struct target_record *cur = NULL, *rec = NULL;
	DIR *dir = NULL;
	struct dirent *entry;
	char polid[POLICY_ID_LEN + 1];
	struct isi_error *error = NULL;
	int lock_fd = -1;

	head = calloc(1, sizeof(struct target_records));
	ASSERT(head != NULL);

	/* Open the target records directory */
	dir = opendir(SIQ_TARGET_RECORDS_DIR);
	if (dir == NULL) {
		error = isi_siq_error_new(E_SIQ_CONF_PARSE,
		    "Error opening target records dir: %s",
		    SIQ_TARGET_RECORDS_DIR);
		goto out;
	}

	for (entry = readdir(dir); entry != NULL; entry = readdir(dir)) {
		/* Make sure it looks like a target record - 
		 * <polid>.xml */
		if (strlen(entry->d_name) != POLICY_ID_LEN + 4)
			continue;

		strncpy(polid, entry->d_name, POLICY_ID_LEN);
		polid[POLICY_ID_LEN] = '\0';

		lock_fd = siq_target_acquire_lock(polid, O_SHLOCK, &error);
		if (error)
			goto out;
		ASSERT(lock_fd != -1);

		/* Ignore non-existence, records can be deleted as we
		 * iterate */
		if (siq_target_record_exists(polid)) {
			rec = siq_target_record_read(polid, &error);
			if (error)
				goto out;

			/* Add it to the list */
			if (cur == NULL) {
				head->record = rec;
				cur = head->record;
			} else {
				cur->next = rec;
				cur = cur->next;
			}
		}

		close(lock_fd);
		lock_fd = -1;
	}

out:
	if (cur != NULL)
		cur->next = NULL;

	if (lock_fd != -1)
		close(lock_fd);

	if (dir != NULL)
		closedir(dir);

	if (error) {
		siq_target_records_free(head);
		head = NULL;
	}

	*records = head;

	isi_error_handle(error, error_out);
}

void
siq_target_records_free(struct target_records *records)
{
	struct target_record *current = NULL;
	struct target_record *next = NULL;
	
	if (records == NULL) {
		return;
	}
	
	current = records->record;
	
	while (current != NULL) {
		next = current->next;
		siq_target_record_free(current);
		current = next;
	}
	
	free(records);
}

int
siq_target_acquire_lock(const char *pid, int lock_type,
    struct isi_error **error_out)
{
	int lock = -1;
	char path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;

	get_record_path(pid, path, sizeof(path), true, &error);
	if (error)
		goto out;

	lock = acquire_lock(path, lock_type, &error);
	
out:
	isi_error_handle(error, error_out);
	return lock;
}

struct target_record *
siq_target_record_find_by_id(struct target_records *records, const char *id)
{
	struct target_record *record = records->record;
	while (record) {
		if (record->id != NULL && strcmp(record->id, id) == 0)
			return record;
		record = record->next;
	}
	return NULL;
}

bool
siq_target_record_exists(const char *pid)
{
	char filename[MAXPATHLEN + 1];
	bool result = false;
	struct isi_error *error = NULL;

	get_record_path(pid, filename, sizeof(filename), false, &error);
	if (error)
		goto out;

	if (access(filename, F_OK) != -1)
		result = true;

out:
	/* If error, we were passed an invalid pid. The return value is still
	 * valid (no, the record doesn't exist) but we should at least log
	 * the error */
	if (error) {
		log(ERROR, "siq_target_record_exists failed for pid: %s : %s",
		    pid, isi_error_get_message(error));
		isi_error_free(error);
	}
	return result;
}

// Allow changing the job state and cancel state of a target record with a
// single lock.
int
siq_target_states_set(const char *pid, int chosen_states,
    enum siq_job_state new_jstate, enum siq_target_cancel_state new_cstate,
    bool have_ex_lock, bool restore, struct isi_error **error_out)
{
	int lock_fd = -1, ret = 0;
	struct target_record *trecord = NULL;
	struct isi_error *error = NULL;
	int changes = 0;
	char *state1 = NULL;
	char *state2 = NULL;

	if (!have_ex_lock) {
		lock_fd = siq_target_acquire_lock(pid, O_EXLOCK, &error);
		if (lock_fd == -1) {
			ret = -1;
			goto out;
		}
	}

	trecord = siq_target_record_read(pid, &error);
	if (error) {
		ret = -1;
		goto out;
	}

	if (chosen_states & CHANGE_JOB_STATE) {
		state1 = job_state_to_text(trecord->job_status, false);
		state2 = job_state_to_text(new_jstate, false);
		log(DEBUG,
		    "%s: Policy %s updating job state from %s to %s",
		    __func__,  trecord->policy_name,
		    state1, state2);
		free(state1);
		free(state2);

		trecord->job_status = new_jstate;
		trecord->job_status_timestamp = time(0);

		/* Slide the job version window forward on non-restore
		 * success. */
		if (new_jstate == SIQ_JS_SUCCESS && !restore &&
		    trecord->composite_job_ver.local > 0 &&
		    trecord->composite_job_ver.common > 0) {
			memcpy(&trecord->prev_composite_job_ver,
			    &trecord->composite_job_ver,
			    sizeof(trecord->composite_job_ver));
			memset(&trecord->composite_job_ver, 0,
			    sizeof(trecord->composite_job_ver));
		}

		changes++;
	}

	if (chosen_states & CHANGE_CANCEL_STATE) {
		if (trecord->cancel_state == new_cstate) {
			log(DEBUG, "%s: Policy %s already in cancel state %s",
			    __func__,  trecord->policy_name,
			    siq_target_cancel_state_to_text(new_cstate));
		}
		else {
			log(DEBUG,
			    "%s: Policy %s changing cancel state from %s to %s",
			    __func__,  trecord->policy_name,
			    siq_target_cancel_state_to_text(trecord->cancel_state),
			    siq_target_cancel_state_to_text(new_cstate));
			changes++;
			trecord->cancel_state = new_cstate;
		}
	}

	if (changes > 0) {
		siq_target_record_write(trecord, &error);
		if (error)
			ret = -1;
	}
 out:
	if (lock_fd != -1)
		close(lock_fd);
	siq_target_record_free(trecord);
	isi_error_handle(error, error_out);
	return ret;
}

enum siq_target_cancel_state
siq_target_cancel_state_get(const char *pid,
    bool have_sh_lock, struct isi_error **error_out)
{
	int lock_fd = -1;
	enum siq_target_cancel_state cstate = CANCEL_INVALID;
	struct target_record *trecord = NULL;
	struct isi_error *error = NULL;

	if (!have_sh_lock) {
		lock_fd = siq_target_acquire_lock(pid, O_SHLOCK, &error);
		if (lock_fd == -1)
			goto out;
	}

	trecord = siq_target_record_read(pid, &error);
	if (error)
		goto out;

	cstate = trecord->cancel_state;
 out:
	if (lock_fd != -1)
		close(lock_fd);
	isi_error_handle(error, error_out);
	siq_target_record_free(trecord);
	return cstate;
}


enum siq_target_cancel_resp
siq_target_cancel_request(const char *pid, struct isi_error **error_out)
{
	int lock_fd = -1;
	struct target_record *trec = NULL;
	struct isi_error *error = NULL;
	enum siq_target_cancel_resp ret;

	lock_fd = siq_target_acquire_lock(pid, O_EXLOCK, &error);
	if (lock_fd == -1) {
		ret = CLIENT_CANCEL_ERROR;
		goto out;
	}

	trec = siq_target_record_read(pid, &error);
	if (error) {
		ret = CLIENT_CANCEL_ERROR;
		goto out;
	}

	log(INFO, "Requesting cancel of policy %s running from cluster %s",
	    trec->policy_name, trec->src_cluster_name);
	
	if (trec->cant_cancel) {
		error = isi_siq_error_new(E_SIQ_TGT_CANCEL,
		    "Policy %s's source cluster is running an older version "
		    "of SyncIQ and is unable to take cancel requests from the "
		    "target cluster. Please cancel the job on the source "
		    "cluster %s",
		    trec->policy_name, trec->src_cluster_name);
		ret = CLIENT_CANCEL_ERROR;
		goto out;
	}

	if (trec->job_status == SIQ_JS_PAUSED) {
		error = isi_siq_error_new(E_SIQ_TGT_CANCEL,
		    "Policy %s is currently paused or pending and cannot be "
		    "cancelled from the target. Please cancel the job on "
		    "the source cluster %s", trec->policy_name,
		    trec->src_cluster_name);
		ret = CLIENT_CANCEL_NOT_RUNNING;
		goto out;
	}

	if (trec->job_status != SIQ_JS_RUNNING) {
		error = isi_siq_error_new(E_SIQ_TGT_CANCEL,
		    "Policy %s is not currently running with this cluster as "
		    "a target", trec->policy_name);
		ret = CLIENT_CANCEL_NOT_RUNNING;
		goto out;
	}

	if (trec->cancel_state == CANCEL_REQUESTED ||
	    trec->cancel_state == CANCEL_PENDING ||
	    trec->cancel_state == CANCEL_ACKED) {
		error = isi_siq_error_new(E_SIQ_TGT_CANCEL,
		    "There has already been a request to cancel policy %s",
		    trec->policy_name);
		ret = CLIENT_CANCEL_ALREADY_IN_PROGRESS;
		goto out;
	}

	if (trec->cancel_state != CANCEL_WAITING_FOR_REQUEST) {
		error = isi_siq_error_new(E_SIQ_TGT_CANCEL,
		    "Policy %s is in unexpected cancel state %s",
		    trec->policy_name,
		    siq_target_cancel_state_to_text(trec->cancel_state));
		ret = CLIENT_CANCEL_ERROR;
		goto out;
	}

	trec->cancel_state = CANCEL_REQUESTED;

	siq_target_record_write(trec, &error);
	if (error) {
		ret = CLIENT_CANCEL_ERROR;
		goto out;
	}
	
	ret = CLIENT_CANCEL_SUCCESS;
 out:
	if (lock_fd != -1)
		close(lock_fd);

	if (error)
		log(ERROR, "Error canceling policy %s (%s): %s",
		    trec ? trec->policy_name : "", pid,
		    isi_error_get_message(error));
	isi_error_handle(error, error_out);
	siq_target_record_free(trec);
	return ret;
}

static int
delete_record(const char *pid)
{
	char path[MAXPATHLEN];
	int ret = 0;
	struct isi_error *error = NULL;

	/* Delete the record */
	get_record_path(pid, path, sizeof(path), false, &error);
	if (error)
		goto out;

	ret = unlink(path);
	if (ret == -1)
		goto out;

	/* Remove the backup <polid>.xml~ file if it exists - not worthy of
	 * failing upon error */
	strcat(path, "~");
	unlink(path);

	/* Delete the lock file - not worthy of failing upon error */
	get_record_path(pid, path, sizeof(path), true, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	} else {
		unlink(path);
	}

out:
	if (error) {
		ret = -1;
		isi_error_free(error);
	}

	return ret;
}

void
siq_target_delete_record(const char *policy_id, bool record_only, bool have_lock,
    struct isi_error **error_out)
{
	struct target_record *record = NULL;
	int ret = ACK_OK;
	struct isi_error *error = NULL;
	struct isi_error *siq_error = NULL;
	int lock_fd = -1;
	char *message = "";

	log(TRACE, "siq_target_delete_record: %s", policy_id);

	/* Get a lock if the caller doesn't have one */
	if (!have_lock) {
		lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
		if (error) {
			message = "Failed to acquire lock on target record";
			ret = ACK_ERR;
			goto out;
		}
	}

	/* Just remove the record - don't do other cleanup */
	if (record_only) {
		ret = delete_record(policy_id);
		if (ret == -1) {
			message = "Error unlinking target record";
			ret = ACK_ERR;
		}
		goto out;
	}

	record = siq_target_record_read(policy_id, &error);
	if (error) {
		message = "Failed to read target record";
		ret = ACK_ERR;
		goto out;
	}

	log_syslog(LOG_INFO, "Breaking target association for policy '%s'.",
	    record->policy_name);

	/* Remove the linmap, if it exists */
	if (record->linmap_created) {
		siq_btree_remove_target(policy_id, &error);
		/* Ignore any errors here */
		isi_error_free(error);
		error = NULL;
	}


	/* Remove the domain, if it exists */
	if (record->domain_id != 0) {
		ret = dom_remove_domain(record->domain_id);
		if (ret != 0) {
			log(NOTICE, "Failed to remove domain %lld (%d)", 
				    record->domain_id, errno);
			if (errno != ENOENT) {
				message = "Failed to remove domain";
				ret = ACK_WARN;
				goto out;
			}
		}
	}

	/* Delete the record */
	ret = delete_record(policy_id);
	if (ret == -1) {
		message = "Error unlinking target record";
		ret = ACK_ERR;
		goto out;
	}
		
	ret = ACK_OK;

out:
	if (lock_fd != -1)
		close(lock_fd);

	/* Send ack */
	if (*message) {
		int errcode;
		
		ASSERT(ret != ACK_OK);
		if (ret == ACK_WARN) {
			errcode = E_SIQ_TDB_WARN;
		} else {
			errcode = E_SIQ_TDB_ERR;
		}
		log(ERROR, "siq_target_delete_record policy_id = %s : %s",
		    policy_id, message);
		siq_error = isi_siq_error_new(errcode, "%s%s%s", 
			message, error ? ": " : "", 
			error ? isi_error_get_message(error) : "");
	}
	
	/* Cleanup */
	isi_error_free(error);
	siq_target_record_free(record);
	
	isi_error_handle(siq_error, error_out);
}

void
siq_target_get_current_generation(char *policy_id, uint32_t *gen_out, 
    struct isi_error **error_out)
{
	struct target_record *record = NULL;
	struct isi_error *error = NULL;
	int lock_fd = -1;

	log(TRACE, "siq_target_get_current_generation: begin %s", policy_id);

	lock_fd = siq_target_acquire_lock(policy_id, O_SHLOCK, &error);
	if (error)
		goto done;

	record = siq_target_record_read(policy_id, &error);
	if (error)
		goto done;

	*gen_out = record->domain_generation;

done:
	if (lock_fd != -1)
		close(lock_fd);
	
	/* Cleanup */
	siq_target_record_free(record);
	
	isi_error_handle(error, error_out);
	
	log(TRACE, "siq_target_get_current_generation: end");
}

const char *
siq_target_cancel_state_to_text(enum siq_target_cancel_state cstate)
{
	switch(cstate) {
	case CANCEL_WAITING_FOR_REQUEST:
		return "CANCEL_WAITING_FOR_REQUEST";
	case CANCEL_REQUESTED:
		return "CANCEL_REQUESTED";
	case CANCEL_PENDING:
		return "CANCEL_PENDING";
	case CANCEL_ACKED:
		return "CANCEL_ACKED";
	case CANCEL_ERROR:
		return "CANCEL_ERROR";
	case CANCEL_INVALID:
		return "CANCEL_INVALID";
	case CANCEL_DONE:
		return "CANCEL_DONE";
	default:
		return "UNKNOWN";
	}
}

enum siq_target_cancel_state
text_to_siq_target_cancel_state(const char *ctext)
{
	if (strcasecmp(ctext, "CANCEL_WAITING_FOR_REQUEST") == 0)
		return CANCEL_WAITING_FOR_REQUEST;
	else if (strcasecmp(ctext, "CANCEL_REQUESTED") == 0)
		return CANCEL_REQUESTED;
	else if (strcasecmp(ctext, "CANCEL_PENDING") == 0)
		return CANCEL_PENDING;
	else if (strcasecmp(ctext, "CANCEL_ACKED") == 0)
		return CANCEL_ACKED;
	else if (strcasecmp(ctext, "CANCEL_REQUESTED") == 0)
		return CANCEL_REQUESTED;
	else if (strcasecmp(ctext, "CANCEL_ERROR") == 0)
		return CANCEL_ERROR;
	else if (strcasecmp(ctext, "CANCEL_DONE") == 0)
		return CANCEL_DONE;
	return CANCEL_INVALID;
}	

const char *
siq_target_fofb_state_to_text(enum siq_target_fofb_state state)
{
	switch (state) {
	case FOFB_NONE:
		return "NONE";
	case FOFB_FAILOVER_STARTED:
		return "FAILOVER_STARTED";
	case FOFB_FAILOVER_REVERT_STARTED:
		return "FAILOVER_REVERT_STARTED";
	case FOFB_FAILOVER_DONE:
		return "FAILOVER_DONE";
	case FOFB_FAILBACK_PREP_STARTED:
		return "FAILBACK_PREP_STARTED";
	case FOFB_FAILBACK_PREP_DONE:
		return "FAILBACK_PREP_DONE";
	default:
		return "UNKNOWN";
	}
}

enum siq_target_fofb_state
text_to_siq_target_fofb_state(const char *text)
{
	if (!strcasecmp(text, "FAILOVER_STARTED"))
		return FOFB_FAILOVER_STARTED;
	else if (!strcasecmp(text, "FAILOVER_DONE"))
		return FOFB_FAILOVER_DONE;
	else if (!strcasecmp(text, "FAILOVER_REVERT_STARTED"))
		return FOFB_FAILOVER_REVERT_STARTED;
	else if (!strcasecmp(text, "FAILBACK_PREP_STARTED"))
		return FOFB_FAILBACK_PREP_STARTED;
	else if (!strcasecmp(text, "FAILBACK_PREP_DONE"))
		return FOFB_FAILBACK_PREP_DONE;

	//XXXDPL Should we error on garbage data?	
	return FOFB_NONE;
}
