/**
 * @file siq_job.c
 * TSM Job Statistics/Progress Implementation
 * (c) 2006 Isilon Systems
 */

#include <ifs/ifs_lin_open.h>
#include <isi_snapshot/snapshot_manage.h>
#include <md5.h>

#include "isi_snapshot/snapshot.h"

#include "siq_job.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/repstate.h"
#include "isi_migrate/migr/cpools_sync_state.h"

#include "isi_migrate/config/siq_gconfig_gcfg.h"
#include "isi_migrate/sched/siq_atomic_primitives.h"
#include "isi_migrate/sched/start_policy.h"
#include "isi_migrate/sched/sched_main.h"
#include "isi_migrate/sched/sched_utils.h"

#include "siq_source.h"
#include "siq_target.h"
#include "siq_btree_public.h"

#define IFSPATH "/ifs"
#define IFSVARPATH "/ifs/.ifsvar"
#define IFSSNAPSHOTPATH "/ifs/.snapshot"

char *
domain_type_to_text(enum domain_type type)
{
	switch (type) {
	case DT_SNAP_REVERT:
		return "SnapRevert";

	case DT_SYNCIQ:
		return "SyncIQ";

	case DT_WORM:
		return "SmartLock";

	default:
		return "Unknown";
	}
}

struct siq_stat *
siq_stat_populate(struct siq_stat *s)
{
	CREATE_IF_NULL(s->dirs, struct siq_stat_dirs);
	CREATE_IF_NULL(s->dirs->src, struct siq_stat_dirs_src);
	CREATE_IF_NULL(s->dirs->dst, struct siq_stat_dirs_dst);
	CREATE_IF_NULL(s->dirs->created, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->dirs->deleted, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->dirs->linked, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->dirs->unlinked, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->files, struct siq_stat_files);
	CREATE_IF_NULL(s->files->replicated, struct siq_stat_files_replicated);
	CREATE_IF_NULL(s->files->deleted, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->files->linked, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->files->unlinked, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->files->skipped, struct siq_stat_files_skipped);
	CREATE_IF_NULL(s->bytes, struct siq_stat_bytes);
	CREATE_IF_NULL(s->bytes->recovered, struct siq_stat_srcdst);
	CREATE_IF_NULL(s->bytes->data, struct siq_stat_bytes_data);
	CREATE_IF_NULL(s->bytes->network, struct siq_stat_bytes_network);
	CREATE_IF_NULL(s->change_compute, struct siq_stat_change_compute);
	CREATE_IF_NULL(s->change_transfer, struct siq_stat_change_transfer);
	CREATE_IF_NULL(s->compliance, struct siq_stat_compliance);
	
	return s;
}

struct siq_stat *
siq_stat_create()
{
	struct siq_stat *s = NULL;
	
	CALLOC_AND_ASSERT(s, struct siq_stat);
	return siq_stat_populate(s);
}

struct siq_job_summary *
siq_job_summary_populate(struct siq_job_summary *job_summ)
{
	CREATE_IF_NULL(job_summ->total, struct siq_job_total);
	
	CREATE_IF_NULL(job_summ->total->stat, struct siq_stat);
	job_summ->total->stat = siq_stat_populate(job_summ->total->stat);
	
	CREATE_IF_NULL(job_summ->chunks, struct siq_job_chunks);
	CREATE_IF_NULL(job_summ->snapshots, struct siq_job_snapshots);
	if (!job_summ->job_spec)
		job_summ->job_spec = siq_spec_create();
	
	return job_summ;
}

struct siq_job_summary *
siq_job_summary_create()
{
	struct siq_job_summary *job_summ = NULL;
	
	CALLOC_AND_ASSERT(job_summ, struct siq_job_summary);
	return siq_job_summary_populate(job_summ);
}

#define SAFE_MEMCPY(dst, src, type)		\
	if (src) {				\
		ASSERT(dst);			\
		memcpy(dst, src, sizeof(type));	\
	}					\

static void
siq_stat_dirs_copy(struct siq_stat_dirs *dst, struct siq_stat_dirs *src)
{
	if (src) {
		ASSERT(dst);
	
		SAFE_MEMCPY(dst->src, src->src, struct siq_stat_dirs_src);
		SAFE_MEMCPY(dst->dst, src->dst, struct siq_stat_dirs_dst);
		SAFE_MEMCPY(dst->created, src->created, struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->deleted, src->deleted, struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->linked, src->linked, struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->unlinked, src->unlinked, struct siq_stat_srcdst);
		
		dst->replicated = src->replicated;
	}
}

static void
siq_stat_files_copy(struct siq_stat_files *dst, struct siq_stat_files *src)
{
	if (src) {
		ASSERT(dst);
		
		dst->total = src->total;
		
		SAFE_MEMCPY(dst->replicated, src->replicated, 
		    struct siq_stat_files_replicated);
		SAFE_MEMCPY(dst->deleted, src->deleted, struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->linked, src->linked, struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->unlinked, src->unlinked, 
		    struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->skipped, src->skipped, 
		    struct siq_stat_files_skipped);
	}
}

static void
siq_stat_bytes_copy(struct siq_stat_bytes *dst, struct siq_stat_bytes *src)
{
	if (src) {
		ASSERT(dst);
		
		dst->transferred = src->transferred;
		dst->recoverable = src->recoverable;
		
		SAFE_MEMCPY(dst->recovered, src->recovered, 
		    struct siq_stat_srcdst);
		SAFE_MEMCPY(dst->data, src->data, struct siq_stat_bytes_data);
		SAFE_MEMCPY(dst->network, src->network, 
		    struct siq_stat_bytes_network);
	}
}

static void
siq_stat_change_compute_copy(struct siq_stat_change_compute *dst, 
    struct siq_stat_change_compute *src)
{
	if (src) {
		ASSERT(dst);
		
		dst->lins_total = src->lins_total;
		
		dst->dirs_new = src->dirs_new;
		dst->dirs_deleted = src->dirs_deleted;
		dst->dirs_moved = src->dirs_moved;
		dst->dirs_changed = src->dirs_changed;
		
		dst->files_new = src->files_new;
		dst->files_linked = src->files_linked;
		dst->files_unlinked = src->files_unlinked;
		dst->files_changed = src->files_changed;
	}
}

static void
siq_stat_change_transfer_copy(struct siq_stat_change_transfer *dst,
    struct siq_stat_change_transfer *src)
{
	if (src) {
		ASSERT(dst);
		
		dst->hash_exceptions_found = src->hash_exceptions_found;
		dst->hash_exceptions_fixed = src->hash_exceptions_fixed;
		dst->flipped_lins = src->flipped_lins;
		dst->corrected_lins = src->corrected_lins;
		dst->resynced_lins = src->resynced_lins;
	}
}

static void
siq_stat_compliance_copy(struct siq_stat_compliance *dst,
    struct siq_stat_compliance *src)
{
	if (src) {
		ASSERT(dst);

		dst->resync_compliance_dirs_new =
		    src->resync_compliance_dirs_new;
		dst->resync_compliance_dirs_linked =
		    src->resync_compliance_dirs_linked;
		dst->resync_conflict_files_new = src->resync_conflict_files_new;
		dst->resync_conflict_files_linked =
		    src->resync_conflict_files_linked;
		dst->conflicts = src->conflicts;
		dst->committed_files = src->committed_files;
	}
}

void
siq_stat_copy(struct siq_stat *dst, struct siq_stat *src)
{
	ASSERT(src);
	ASSERT(dst);
	
	siq_stat_dirs_copy(dst->dirs, src->dirs);
	siq_stat_files_copy(dst->files, src->files);
	siq_stat_bytes_copy(dst->bytes, src->bytes);
	siq_stat_change_compute_copy(dst->change_compute, src->change_compute);
	siq_stat_change_transfer_copy(dst->change_transfer,
	    src->change_transfer);
	siq_stat_compliance_copy(dst->compliance, src->compliance);
}

void
siq_stat_free(struct siq_stat *s)
{
	if (!s) {
		return;
	}
	if (s->dirs) {
		FREE_AND_NULL(s->dirs->src);
		FREE_AND_NULL(s->dirs->dst);
		FREE_AND_NULL(s->dirs->created);
		FREE_AND_NULL(s->dirs->deleted);
		FREE_AND_NULL(s->dirs->linked);
		FREE_AND_NULL(s->dirs->unlinked);
		FREE_AND_NULL(s->dirs);
	}
	if (s->files) {
		FREE_AND_NULL(s->files->replicated);
		FREE_AND_NULL(s->files->deleted);
		FREE_AND_NULL(s->files->linked);
		FREE_AND_NULL(s->files->unlinked);
		FREE_AND_NULL(s->files->skipped);
		FREE_AND_NULL(s->files);
	}
	if (s->bytes) {
		FREE_AND_NULL(s->bytes->recovered);
		FREE_AND_NULL(s->bytes->data);
		FREE_AND_NULL(s->bytes->network);
		FREE_AND_NULL(s->bytes);
	}
	if (s->change_compute) {
		FREE_AND_NULL(s->change_compute);
	}
	if (s->change_transfer) {
		FREE_AND_NULL(s->change_transfer);
	}
	if (s->compliance) {
		FREE_AND_NULL(s->compliance);
	}
	FREE_AND_NULL(s);
}

void
siq_job_total_free(struct siq_job_total *t)
{
	if (!t) {
		return;
	}
	siq_stat_free(t->stat);
	FREE_AND_NULL(t);
}

void
siq_string_list_free(struct siq_string_list_head *list)
{
	struct siq_string_list *item = NULL;
	while (!SLIST_EMPTY(list)) {
		item = SLIST_FIRST(list);
		SLIST_REMOVE_HEAD(list, next);
		FREE_AND_NULL(item->msg);
		FREE_AND_NULL(item);
	}
}

void
siq_policy_path_free(struct siq_policy_path_head *list)
{
	struct siq_policy_path *item = NULL;
	while (!SLIST_EMPTY(list)) {
		item = SLIST_FIRST(list);
		SLIST_REMOVE_HEAD(list, next);
		FREE_AND_NULL(item->path);
		FREE_AND_NULL(item);
	}
}

void
siq_phase_history_free(struct siq_phase_history_head *phases)
{
	struct siq_phase_history *item = NULL;
	while (!SLIST_EMPTY(phases)) {
		item = SLIST_FIRST(phases);
		SLIST_REMOVE_HEAD(phases, next);
		FREE_AND_NULL(item);
	}
}

void
siq_workers_free(struct siq_worker_head *workers)
{
	struct siq_worker *item = NULL;
	while (!SLIST_EMPTY(workers)) {
		item = SLIST_FIRST(workers);
		SLIST_REMOVE_HEAD(workers, next);
		FREE_AND_NULL(item->source_host);
		FREE_AND_NULL(item->target_host);
		FREE_AND_NULL(item);
	}
}

static void
_siq_job_summary_free(struct siq_job_summary *js, bool free_spec)
{
	if (!js) {
		return;
	}
	siq_job_total_free(js->total);
	if (js->chunks) {
		FREE_AND_NULL(js->chunks);
	}
	if (js->error) {
		FREE_AND_NULL(js->error);
	}
	//String lists
	siq_string_list_free(&js->errors);
	siq_string_list_free(&js->warnings);
	if (js->snapshots) {
		siq_string_list_free(&js->snapshots->target);
		FREE_AND_NULL(js->snapshots);
	}
	siq_string_list_free(&js->retransmitted_files);
	siq_phase_history_free(&js->phases);
	siq_workers_free(&js->workers);
	if (free_spec) {
		siq_report_spec_free(js->job_spec);
	} else {
		siq_spec_free(js->job_spec);
	}
	//free sra_id allocated before
	if (js->sra_id) {
		FREE_AND_NULL(js->sra_id);
	}
	FREE_AND_NULL(js);
}

void
siq_job_summary_free(struct siq_job_summary *js)
{
	_siq_job_summary_free(js, true);
}

void
siq_job_summary_free_nospec(struct siq_job_summary *js)
{
	_siq_job_summary_free(js, false);
}

struct siq_gc_job_summary *
siq_gc_job_summary_load(char *path, struct isi_error **error_out)
{
	struct siq_gc_job_summary *js = NULL;
	struct isi_error *error = NULL;
	
	CALLOC_AND_ASSERT(js, struct siq_gc_job_summary);
	js->gci_tree.root = &gci_ivar_siq_job_summary_root;
	js->path = strdup(path);
	js->gci_tree.primary_config = js->path;
	js->gci_tree.fallback_config_ro = NULL;
	
	js->gci_base = gcfg_open(&js->gci_tree, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	js->gci_ctx = gci_ctx_new(js->gci_base, true, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	gci_ctx_read_path(js->gci_ctx, "", &js->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
out:
	if (error) {
		siq_gc_job_summary_free(js);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return js;
	}
}

struct siq_job_summary *
siq_job_summary_load_readonly(char *path, struct isi_error **error_out)
{
	struct siq_gc_job_summary *js = NULL;
	struct siq_job_summary *jsr = NULL;
	struct isi_error *error = NULL;
	
	CALLOC_AND_ASSERT(js, struct siq_gc_job_summary);
	js->gci_tree.root = &gci_ivar_siq_job_summary_root;
	js->path = strdup(path);
	js->gci_tree.primary_config = js->path;
	js->gci_tree.fallback_config_ro = NULL;
	
	js->gci_base = gcfg_open(&js->gci_tree, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	js->gci_ctx = gci_ctx_new(js->gci_base, false, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	gci_ctx_read_path(js->gci_ctx, "", &js->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	//For reports, ensure that all fields are populated, even if they are
	//empty.
	siq_job_summary_populate(js->root->summary);
	
out:
	if (error) {
		//fixed bug 99238
		FREE_AND_NULL(js->root);
		siq_gc_job_summary_free(js);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		//fixed bug 99238
		jsr = js->root->summary;
		FREE_AND_NULL(js->root);
		siq_gc_job_summary_free(js);
		return jsr;
	}
}

void
siq_gc_job_summary_save(struct siq_gc_job_summary *job_summ, 
    struct isi_error **error_out)
{
	struct isi_error *isierror = NULL;

	gci_ctx_write_path(job_summ->gci_ctx, "", job_summ->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_commit(job_summ->gci_ctx, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_free(job_summ->gci_ctx);
	job_summ->gci_ctx = gci_ctx_new(job_summ->gci_base, true, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	gci_ctx_read_path(job_summ->gci_ctx, "", &job_summ->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	//For reports, ensure that all fields are populated, even if they are
	//empty.
	siq_job_summary_populate(job_summ->root->summary);
	
out:
	isi_error_handle(isierror, error_out);
}

struct siq_gc_job_summary *
siq_job_summary_save_new(struct siq_job_summary *root, char *path, 
    struct isi_error **error_out)
{
	struct siq_gc_job_summary *job_summ = NULL;
	struct isi_error *error = NULL;
	
	job_summ = siq_gc_job_summary_load(path, &error);
	if (error) {
		log(ERROR, "%s: Failed to siq_job_summary_load from %s: %s", 
		    __func__, path, isi_error_get_message(error));
		goto out;
	}
	
	siq_job_summary_free(job_summ->root->summary);
	job_summ->root->summary = root;
	
	siq_gc_job_summary_save(job_summ, &error);
	if (error) {
		log(ERROR, "%s: Failed to siq_job_summary_save: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}

out:
	if (error) {
		siq_gc_job_summary_free(job_summ);
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return job_summ;
	}
}

void
siq_gc_job_summary_free(struct siq_gc_job_summary *job_summ)
{
	if (!job_summ) {
		return;
	}
	if (job_summ->gci_ctx) {
		gci_ctx_free(job_summ->gci_ctx);
	}
	if (job_summ->gci_base) {
		gcfg_close(job_summ->gci_base);
	}
	
	FREE_AND_NULL(job_summ->path);
	FREE_AND_NULL(job_summ);
}

void
siq_job_summary_free_array(struct siq_job_summary **js, int count)
{
	int i;

	for (i = 0; i < count; i++) {
		siq_job_summary_free(js[i]);
		js[i] = NULL;
	}

	return;
}

static void
_siq_job_string_list_append(struct siq_string_list_head *head, char* msg)
{
	struct siq_string_list *new = NULL;
	
	CALLOC_AND_ASSERT(new, struct siq_string_list);
	new->msg = strdup(msg);
	
	SLIST_INSERT_HEAD(head, new, next);
}

void
siq_job_error_msg_append(struct siq_job_summary *js, char* msg)
{
	return _siq_job_string_list_append(&js->errors, msg);
}

void
siq_job_warning_msg_append(struct siq_job_summary *js, char* msg)
{
	return _siq_job_string_list_append(&js->warnings, msg);
}

void
siq_job_retransmitted_file_append(struct siq_job_summary *js, char* msg)
{
	return _siq_job_string_list_append(&js->retransmitted_files, msg);
}

void
siq_snapshots_target_append(struct siq_job_summary *js, char* msg)
{
	return _siq_job_string_list_append(&js->snapshots->target, msg);
}

void
siq_failover_snap_rename_new_to_latest(const char *policy_id, 
    const char *policy_name, struct isi_error **error_out)
{
	SNAP *new_snap = NULL;

	struct isi_str new_snap_name;
	struct isi_str latest_snap_name;

	struct fmt FMT_INIT_CLEAN(fmt_latest);
	struct fmt FMT_INIT_CLEAN(fmt_new);	

	struct isi_error *error = NULL;

	fmt_print(&fmt_new, "SIQ-%s-restore-new", policy_id);

	isi_str_init(&new_snap_name, __DECONST(char *, 
	    fmt_string(&fmt_new)), fmt_length(&fmt_new) + 1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);

	new_snap = snapshot_open_by_name(&new_snap_name, NULL);
	if (!new_snap) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF, "Couldn't find "
		    "restore checkpoint for %s.", policy_name);
		goto out;
	}

	fmt_print(&fmt_latest, "SIQ-%s-restore-latest", policy_id);

	isi_str_init(&latest_snap_name, 
	    __DECONST(char *, fmt_string(&fmt_latest)), 
	    fmt_length(&fmt_latest) + 1, ENC_UTF8, ISI_STR_NO_MALLOC);

	log(DEBUG, "Failover: Renaming snapshot %s -> %s", new_snap_name.str,
	    latest_snap_name.str);

	snapshot_rename(&new_snap_name, &latest_snap_name, &error);
	if (error)
		goto out;

out:
	if (new_snap)
		snapshot_close(new_snap);

	isi_error_handle(error, error_out);
}

static void 
grab_target_record(const char *policy_id, int *tlock_fd,
    struct target_record **trec, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	*tlock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
	if (-1 == *tlock_fd || error) 
		goto out;

	*trec = siq_target_record_read(policy_id, &error);	

out:
	isi_error_handle(error, error_out);
}

static void
create_failover_job(struct target_record *trec, int log_level,
    int workers_per_node, bool revert, bool grab_local,
    struct isi_error **error_out)
{
	int ret = 0;
	struct isi_error *error = NULL;
	struct siq_policy *pol = NULL;
	struct siq_job_summary *summary = NULL;
	struct siq_gc_job_summary *gcs = NULL;
	int edit_dfd = -1;
	int report_dfd = -1;
	int run_dfd = -1;

	char edit_dir[MAXPATHLEN + 1];
	char run_dir[MAXPATHLEN + 1];
	char spec_file[MAXPATHLEN + 1];
	char report_file[MAXPATHLEN + 1];
	char report_dir[MAXPATHLEN + 1];

	int nodenum = -1;

	pol = siq_failover_policy_create_from_target_record(trec, revert);
	if (!pol)
		goto out;

	sprintf(edit_dir, "%s/%s", sched_get_edit_dir(), pol->common->pid);
	sprintf(spec_file, "%s/%s", edit_dir, SPEC_FILENAME);
	sprintf(report_file, "%s/%s", edit_dir, REPORT_FILENAME);
	sprintf(report_dir, "%s/%s", sched_get_reports_dir(),
	    pol->common->pid);

	// Create the job as if grabbed locally by the scheduler. For 
	// python tests.
	if (grab_local) {
		nodenum = sched_get_my_node_number();
		ASSERT(nodenum != -1);
		sprintf(run_dir, "%s_%d", pol->common->pid, nodenum);
	} else {
		sprintf(run_dir, "%s", pol->common->pid);
	}

	edit_dfd = open(sched_get_edit_dir(), O_RDONLY);
	if (edit_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open edit dir %s", sched_get_edit_dir());
		goto out;
	}
	if (enc_mkdirat(edit_dfd, pol->common->pid, ENC_UTF8, 0755) != 0 &&
	    errno != EEXIST) {
		error = isi_system_error_new(errno, "Could not create "
		    "temporary working dir for %s.", pol->common->name);
		goto out;
	}

	report_dfd = open(sched_get_reports_dir(), O_RDONLY);
	if (report_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open reports dir %s", sched_get_reports_dir());
		goto out;
	}
	if (enc_mkdirat(report_dfd, pol->common->pid, ENC_UTF8, 0755) != 0 &&
	    errno != EEXIST) {
		error = isi_system_error_new(errno, "Could not create "
		    "target report dir for %s.", pol->common->name);
		goto out;
	}

	pol->coordinator->workers_per_node = workers_per_node;
	pol->common->log_level = log_level;

	siq_spec_save(pol, spec_file, &error);
	if (error)
		goto out;

	summary = siq_job_summary_create();

	ret = make_job_summary(summary, pol->common->action);
	ASSERT(!ret); 

	gcs = siq_job_summary_save_new(summary, report_file, 
	    &error);
	if (error)
		goto out;

	run_dfd = open(sched_get_run_dir(), O_RDONLY);
	if (run_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open run dir %s", sched_get_run_dir());
		goto out;
	}
	if (enc_renameat(edit_dfd, pol->common->pid, ENC_UTF8,
	    run_dfd, run_dir, ENC_UTF8) != 0 && errno != ENOTEMPTY) {
		log(ERROR, "Failed to move temp job dir %s to run dir",
		    edit_dir);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	if (edit_dfd != -1)
		close(edit_dfd);
	if (report_dfd != -1)
		close(report_dfd);
	if (run_dfd != -1)
		close(run_dfd);

	siq_gc_job_summary_free(gcs);
	siq_policy_free(pol);
}

enum siq_action_type
get_spec_action(const char *run_dir, const char *job_dirname)
{
	enum siq_action_type act_type = SIQ_ACT_NOP;
	char spec_path[MAXPATHLEN + 1];
	struct isi_error *error = NULL;
	struct siq_gc_spec *gcspec = NULL;

	snprintf(spec_path, MAXPATHLEN, "%s/%s/%s", run_dir, 
	    job_dirname, SPEC_FILENAME);
	spec_path[MAXPATHLEN] = '\0';

	gcspec = siq_gc_spec_load(spec_path, &error);

	// There's a run dir, but no spec.
	if (!gcspec || error) {
		log(ERROR, "%s: unable to grab job action from "
		    "run dir %s", __func__, job_dirname);
		goto out;
	}

	act_type = gcspec->spec->common->action;
	
out:
	if (gcspec)
		siq_gc_spec_free(gcspec);

	isi_error_free(error);
	return act_type;
}

// Get the action type for a given job if it is running or scheduled. Else 
// return SIQ_ACT_NOP if it is not.

static enum siq_action_type
get_running_job_action(const char *policy_id)
{
	DIR* dirp = NULL;
	struct dirent *dp = NULL;
	enum siq_action_type act_type = SIQ_ACT_NOP;
	enum job_progress progress = PROGRESS_NONE;
	struct isi_error *error = NULL;

	bool found = false;
	char job_id[MAXPATHLEN + 1];

	dirp = opendir(sched_get_run_dir());

	if (!dirp) {
		log(ERROR, "%s: unable to open run dir: %s", __func__,
		    strerror(errno));
		goto out;
	}

	while (!found && (dp = readdir(dirp)) != NULL) {
		if (dp->d_type != DT_DIR)
			continue;

		progress = sched_decomp_name(dp->d_name, job_id, NULL, NULL);

		if (strcmp(job_id, policy_id))
			continue;

		if (progress == PROGRESS_NONE)
			goto out;

		act_type = get_spec_action(sched_get_run_dir(), dp->d_name);
	}
		
out:	
	if (dirp)
		closedir(dirp);

	isi_error_free(error);

	return act_type;
}

// Cleanup currently running job info from the source record.

static void
source_cleanup_prev_job(const char *policy_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL; 
	siq_reports_status_set_inc_job_id(policy_id, &error);
	if (error)
		goto out;

	siq_reports_status_check_inc_job_id(policy_id, &error);
	if (error)
		goto out;
out:
	isi_error_handle(error, error_out);
}


/*  _____     _ _                       _____     _ _ _                _    
 * |  ___|_ _(_) | _____   _____ _ __  |  ___|_ _(_) | |__   __ _  ___| | __
 * | |_ / _` | | |/ _ \ \ / / _ \ '__| | |_ / _` | | | '_ \ / _` |/ __| |/ /
 * |  _| (_| | | | (_) \ V /  __/ |    |  _| (_| | | | |_) | (_| | (__|   < 
 * |_|  \__,_|_|_|\___/ \_/ \___|_|    |_|  \__,_|_|_|_.__/ \__,_|\___|_|\_\
 */

void 
siq_start_failover_job(const char *policy_id, int log_level, 
    int workers_per_node, bool grab_local, struct isi_error **error_out)
{
	int ret = 0;
	struct isi_error *error = NULL;
	struct target_record *trec = NULL;
	struct version_status ver_stat;
	struct siq_job_version *job_ver, tmp_ver;
	struct siq_source_record *srec = NULL;
	enum siq_job_action pending_job = SIQ_JOB_NONE;
	enum siq_action_type running_act = SIQ_ACT_NOP;

	SNAP *last_good_snap = NULL;
	int lock_fd = -1;

	grab_target_record(policy_id, &lock_fd, &trec, &error);
	if (error)
		goto out;

	running_act = get_running_job_action(policy_id);
	if (running_act == SIQ_ACT_FAILOVER) {
		error = isi_siq_error_new(E_SIQ_SCHED_RUNNING, 
		    "Cannot start failover job for %s. Already running.",
		    trec->policy_name);
		goto out;
	} 
	
	if (running_act == SIQ_ACT_FAILOVER_REVERT || 
	    trec->fofb_state == FOFB_FAILOVER_REVERT_STARTED) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, 
		    "Cannot start failover job for %s. Failover revert in "
		    "progress. Failover revert must complete before sync or "
		    "failover jobs may be started again.", trec->policy_name);
		goto out;
	} 

	if (trec->fofb_state >= FOFB_FAILOVER_DONE) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Cannot start failover job for %s. Failover already done.",
		    trec->policy_name);
		goto out;
	}
	
	if (trec->latest_snap == INVALID_SNAPID) {
		error = isi_siq_error_new(E_SIQ_CONF_NOENT,
		    "Cannot start failover job for %s. Target snapshot not "
		    "found. Is FOFB enabled for this policy?",
		    trec->policy_name);
		goto out;
	}

	last_good_snap = snapshot_open_by_sid(trec->latest_snap, NULL);
	if (!last_good_snap) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Cannot start failover job for %s. Target snapshot not "
		    "found.", trec->policy_name);
		goto out;
	}

	trec->fofb_state = FOFB_FAILOVER_STARTED;

	// Hard-cancel any running sync jobs, in addition to cancel request
	// from CLI.
	ret = dom_inc_generation(trec->domain_id, &(trec->domain_generation));

	if (ret) {
		error = isi_system_error_new(errno, 
		    "Cannot start failover job for %s. Could not hard-cancel "
		    "running sync job.", trec->policy_name);
		goto out;
	}

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	if (!siq_source_has_record(policy_id))
		siq_source_create(policy_id, &error);
	if (error)
		goto out;

	srec = siq_source_record_open(policy_id, &error);
	if (error)
		goto out;

	ASSERT(srec != NULL);

	siq_source_get_pending_job(srec, &pending_job);
	
	if (pending_job != SIQ_JOB_NONE)
		source_cleanup_prev_job(policy_id, &error);
	if (error) 
		goto out;

	siq_source_set_pending_job(srec, SIQ_JOB_FAILOVER);
	siq_source_set_pol_stf_ready(srec);
	siq_source_set_restore_latest(srec, trec->latest_snap);

	if (trec->composite_job_ver.common != 0) {
		// The last non-restore job failed; use that job's version.
		job_ver = &trec->composite_job_ver;
	} else if (trec->prev_composite_job_ver.common != 0) {
		// The last non-restore job succeeded; use that job's version.
		job_ver =  &trec->prev_composite_job_ver;
	} else {
		// No previous job history; use the committed version.
		get_version_status(false, &ver_stat);
		tmp_ver.common = tmp_ver.local = ver_stat.committed_version;
		job_ver = &tmp_ver;
	}

	siq_source_set_restore_composite_job_ver(srec, job_ver);
	siq_source_set_restore_prev_composite_job_ver(srec,
	    &trec->prev_composite_job_ver);

	siq_source_record_write(srec, &error);
	if (error)
		goto out;

	create_failover_job(trec, log_level, workers_per_node, false, 
	    grab_local, &error);
	if (error)
		goto out;

out:
	if (-1 != lock_fd)
		close(lock_fd);

	siq_target_record_free(trec);

	if (srec) {
		siq_source_record_unlock(srec);
		siq_source_record_free(srec);
	}

	if (last_good_snap)
		snapshot_close(last_good_snap);

	isi_error_handle(error, error_out);
}

void 
siq_start_failover_revert_job(const char *policy_id, int log_level, 
    int workers_per_node, bool grab_local, struct isi_error **error_out)
{
	int ret = 0;
	struct isi_error *error = NULL;
	struct target_record *trec = NULL;
	struct version_status ver_stat;
	struct siq_job_version *job_ver, tmp_ver;
	struct siq_source_record *srec = NULL;
	enum siq_action_type running_act = SIQ_ACT_NOP;

	int lock_fd = -1;

	struct isi_str new_snap_name;
	struct isi_str latest_snap_name;

	struct fmt FMT_INIT_CLEAN(fmt_new);
	struct fmt FMT_INIT_CLEAN(fmt_latest);

	SNAP *new_snap = NULL;
	SNAP *latest_snap = NULL;
	ifs_snapid_t latest_snap_id = INVALID_SNAPID;

	grab_target_record(policy_id, &lock_fd, &trec, &error);
	if (error)
		goto out;

	running_act = get_running_job_action(policy_id);
	if (running_act == SIQ_ACT_FAILOVER_REVERT) {
		error = isi_siq_error_new(E_SIQ_SCHED_RUNNING,
			"Cannot start failover revert job for %s. Already " 
			"running.", trec->policy_name);
		goto out;
	}

	if (running_act == SIQ_ACT_FAILOVER) {
		error = isi_siq_error_new(E_SIQ_SCHED_RUNNING, 
		    "Cannot start failover revert for %s while failover job "
		    "is running. Please cancel the currently running "
		    "failover job before starting revert",
		    trec->policy_name);
		goto out;
	} 

	if (trec->fofb_state == FOFB_NONE) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, 
		    "Cannot start failover revert job for %s. Failover not "
		    "performed yet.", trec->policy_name);
		goto out;
	}

	if (trec->fofb_state >= FOFB_FAILBACK_PREP_STARTED) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Cannot start failover revert job for %s. Failback "
		    "process already started.", trec->policy_name);
		goto out;
	}

	trec->job_status = SIQ_JS_NONE; // Still need to do this?

	ret = dom_set_readonly(trec->domain_id, true);
	if (ret) {
		error = isi_system_error_new(errno, 
		    "Cannot start failover revert job for %s. "
		    "Could not set restricted-writer flag", trec->policy_name);
		goto out;
	}

	// Hard-cancel any running failover jobs, in addition to cancel 
	// request from CLI.
	ret = dom_inc_generation(trec->domain_id, &(trec->domain_generation));
	if (ret) {
		error = isi_system_error_new(errno, 
		    "Cannot start failover revert job for %s. Could not "
		    "hard-cancel running failover job", trec->policy_name);
		goto out;
	}

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	if (!siq_source_has_record(policy_id))
		siq_source_create(policy_id, &error);
	if (error)
		goto out;

	srec = siq_source_record_open(policy_id, &error);
	if (error)
		goto out;

	ASSERT(srec != NULL);

	fmt_print(&fmt_new, "SIQ-%s-restore-new", policy_id);
	fmt_print(&fmt_latest, "SIQ-%s-restore-latest", policy_id);

	isi_str_init(&new_snap_name, __DECONST(char *, 
	    fmt_string(&fmt_new)), fmt_length(&fmt_new) + 1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	new_snap = snapshot_open_by_name(&new_snap_name, NULL);

	// Failover was interrupted while running. Rename new to latest so 
	// we can re-use the 'new' snapname for revert. Failover typically
	// renames new to latest on success.
	if (new_snap && trec->fofb_state == FOFB_FAILOVER_STARTED) {
		siq_failover_snap_rename_new_to_latest(policy_id, 
		    trec->policy_name, &error);
		siq_source_clear_restore_new(srec);
		if (error)
			goto out;
	}

	isi_str_init(&latest_snap_name, __DECONST(char *, 
	    fmt_string(&fmt_latest)), fmt_length(&fmt_latest) + 1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	latest_snap = snapshot_open_by_name(&latest_snap_name, NULL);

	if (!latest_snap) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, 
		    "Cannot start failover revert job for %s. Couldn't find "
		    "restore checkpoint.", trec->policy_name);
		goto out;
	}

	snapshot_get_snapid(latest_snap, &latest_snap_id);
	ASSERT(latest_snap_id != INVALID_SNAPID);

	if (trec->fofb_state != FOFB_FAILOVER_REVERT_STARTED)
	       source_cleanup_prev_job(policy_id, &error);	
	if (error)
		goto out;

	siq_source_set_pending_job(srec, SIQ_JOB_FAILOVER_REVERT);
	siq_source_set_pol_stf_ready(srec);
	siq_source_set_restore_latest(srec, latest_snap_id);

	if (trec->composite_job_ver.common != 0) {
		// The last non-restore job failed; use that job's version.
		job_ver = &trec->composite_job_ver;
	} else if (trec->prev_composite_job_ver.common != 0) {
		// The last non-restore job succeeded; use that job's version.
		job_ver =  &trec->prev_composite_job_ver;
	} else {
		// No previous job history; use the committed version.
		get_version_status(false, &ver_stat);
		tmp_ver.common = tmp_ver.local = ver_stat.committed_version;
		job_ver = &tmp_ver;
	}

	siq_source_set_restore_composite_job_ver(srec, job_ver);
	siq_source_set_restore_prev_composite_job_ver(srec,
	    &trec->prev_composite_job_ver);

	siq_source_record_write(srec, &error);
	if (error)
		goto out;


	create_failover_job(trec, log_level, workers_per_node, true, 
	    grab_local, &error);
	if (error)
		goto out;

	trec->fofb_state = FOFB_FAILOVER_REVERT_STARTED;

	// Written twice. First time due to domain generation inc, which needs
	// to persist regardless of whether revert starts or not. Only set
	// revert-in-progress if we successfully start the revert.
	siq_target_record_write(trec, &error);
	if (error) {
		printf("%s", isi_error_get_message(error));
		fflush(stdout);
		goto out;
	}


out:
	if (-1 != lock_fd)
		close(lock_fd);

	siq_target_record_free(trec);

	if (srec) {
		siq_source_record_unlock(srec);
		siq_source_record_free(srec);
	}

	// fixed bug 99238
	if (new_snap)
		snapshot_close(new_snap);

	if (latest_snap)
		snapshot_close(latest_snap);

	isi_error_handle(error, error_out);
}


/*  ____                        _         __  __            _    
 * |  _ \  ___  _ __ ___   __ _(_)_ __   |  \/  | __ _ _ __| | __
 * | | | |/ _ \| '_ ` _ \ / _` | | '_ \  | |\/| |/ _` | '__| |/ /
 * | |_| | (_) | | | | | | (_| | | | | | | |  | | (_| | |  |   < 
 * |____/ \___/|_| |_| |_|\__,_|_|_| |_| |_|  |_|\__,_|_|  |_|\_\
 */


// Cleanup reports created by temporary policies (domain mark/snap revert).
static void
cleanup_temp_reports(char *policy_id)
{
	char rm_report_dir[MAXPATHLEN + 10];
	sqlite3 *db = NULL;
	struct isi_error *error = NULL;

	db = siq_reportdb_open(SIQ_REPORTS_REPORT_DB_PATH, &error);
	if (!error && db != NULL)
		siq_reportdb_delete_by_policyid(db, policy_id, NULL);

	isi_error_free(error);
	error = NULL;

	sprintf(rm_report_dir, "rm -rf %s/%s", sched_get_reports_dir(),
	    policy_id);
	system(rm_report_dir);
	if (db != NULL)
		siq_reportdb_close(db);
}

static void
check_domain_path(const char *path, struct stat *st_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(st_out);

	/* make sure path does not contain /../ or /./ or // */
	if (strstr(path, "/../") || strstr(path, "/./") ||
	    strstr(path, "//")) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Path format is illegal");
		goto out;
	}

	/* make sure path is below (not equal to) /ifs */
	if (strncmp(path, IFSPATH, strlen(IFSPATH)) != 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Domain must be rooted below /ifs");
		goto out;
	}
	if (strlen(path) <= 5) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Creating a domain rooted at /ifs is not supported");
		goto out;
	}

	/* verify path is not at or below .ifsvar or .snapshot */
	if (strncmp(path, IFSVARPATH, strlen(IFSVARPATH)) == 0 ||
	    strncmp(path, IFSSNAPSHOTPATH, strlen(IFSSNAPSHOTPATH)) == 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Creating a "
		    "domain rooted at or below %s or %s is not supported",
		    IFSVARPATH, IFSSNAPSHOTPATH);
		goto out;
	}

	if (stat(path, st_out) != 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat path %s", path);
		goto out;
	}

	if (S_ISDIR(st_out->st_mode) == false) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Specified path is not a directory");
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/* generate a policy id based on the LIN of the root path. this can be used
 * to look up the policy when only the LIN or root path are known */
void
siq_domain_mark_policy_id(char *root, char *pol_id_out,
    struct isi_error **error_out)
{
	MD5_CTX md;
	struct isi_error *error = NULL;
	char md5in[64];
	struct stat st;

	ASSERT(pol_id_out);

	check_domain_path(root, &st, &error);
	if (error)
		goto out;

	snprintf(md5in, 64, "%llu_DOMAIN_MARK", st.st_ino);

	MD5Init(&md);
	MD5Update(&md, md5in, strlen(md5in));
	MD5End(&md, pol_id_out);

out:
	isi_error_handle(error, error_out);
}

/* clean up domain mark job. ignore errors if any and keep cleaning up */
void
siq_cleanup_domain_mark_job(char *root, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char policy_id[POLICY_ID_LEN + 1];

	siq_domain_mark_policy_id(root, policy_id, &error);
	if (error)
		goto out;

	/* trash temporary repstates and linmaps */
	siq_btree_remove_source(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
	siq_btree_remove_target(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	/* cleanup source record */
	siq_source_delete(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}		

	/* cleanup target record */
	siq_target_delete_record(policy_id, true, false, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	cleanup_temp_reports(policy_id);

	/* cleanup job lock file */
	siq_job_rm_lock_file(policy_id);

out:
	isi_error_handle(error, error_out);
}

static void
siq_find_domain(char *root, enum domain_type type, ifs_domainid_t *dom_id_out,
    uint32_t *dom_gen_out, bool *dom_ready_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;
	struct domain_entry dom_ent;
	ifs_domainid_t dom_id = 0;
	uint32_t dom_gen = 0;
	bool dom_ready = false;
	
	if (dom_id_out)
		*dom_id_out = 0;
	if (dom_gen_out)
		*dom_gen_out = 0;
	if (dom_ready_out)
		*dom_ready_out = false;

	check_domain_path(root, &st, &error);
	if (error)
		goto out;

	/* see if path already has an appropriate domain, and if it is 'ready'
	 * or if it hasn't been fully marked, i.e. in case of job restart */
	find_domain_by_lin(st.st_ino, type, false, &dom_id, &dom_gen, &error);
	if (error)
		goto out;
	if (dom_id == 0)
		goto out;

	if (dom_get_entry(dom_id, &dom_ent) == -1) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Failed to retrieve entry for domain %llu", dom_id);
		goto out;
	}

	ASSERT(dom_ent.d_flags & type);

	if (!!(dom_ent.d_flags & DOM_READY))
		dom_ready = true;

	if (dom_id_out)
		*dom_id_out = dom_id;
	if (dom_gen_out)
		*dom_gen_out = dom_gen;
	if (dom_ready_out)
		*dom_ready_out = dom_ready;
	
out:
	isi_error_handle(error, error_out);
}

void
siq_remove_domain(char *root, enum domain_type type,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct target_records *trecs = NULL;
	struct target_record *trec = NULL;
	struct stat sta, stb;
	bool match = false;
	ifs_domainid_t dom_id = 0;

	check_domain_path(root, &sta, &error);
	if (error)
		goto out;

	siq_find_domain(root, type, &dom_id, NULL, NULL, &error);
	if (dom_id == 0) {
		isi_error_free(error);
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Unable to locate a %s domain at root %s",
		    domain_type_to_text(type), root);
		goto out;
	}

	/* if type is synciq, don't allow removal if path matches a target
	 * path in a target record */
	if (type == DT_SYNCIQ) {
		siq_target_records_read(&trecs, &error);
		if (error)
			goto out;

		for (trec = trecs->record; trec; trec = trec->next) {
			if (stat(trec->target_dir, &stb) != 0)
				continue;

			if (sta.st_ino == stb.st_ino) {
				match = true;
				break;
			}
		}

		if (match == true) {
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
			    "Cannot delete a synciq type domain with a root "
			    "that matches the target path of a SyncIQ policy "
			    "(policy %s)", trec->policy_name);
			goto out;
		}
	}

	if (dom_remove_domain(dom_id) == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,
		    "Failed to remove domain with id %llu", dom_id);
		goto out;
	}

out:
	siq_target_records_free(trecs);
	isi_error_handle(error, error_out);
}

/* TODO stub for function to check for illegal domain overlap before we
 * start a domain mark job */
static void
domain_conflict(char *root, enum domain_type type,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	/* check for domains governing this root. if this domain would be
	 * contained by another domain and both domains are not snaprevert,
	 * the new domain is not allowed */

	/* check for domains below this root. if this domain would contain
	 * another domain and both domains are not snaprevert, the new
	 * domain would not be allowed */

	isi_error_handle(error, error_out);
}

void
siq_start_domain_mark_job(char *root, enum domain_type type, int log_level,
    int workers_per_node, bool grab_local, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat sta, stb;
	struct siq_source_record *srec = NULL;
	struct siq_policies *pols = NULL;
	struct siq_policy *pol = NULL;
	struct siq_job_summary *summary = NULL;
	struct siq_gc_job_summary *gcs = NULL;
	char edit_dir[MAXPATHLEN + 1];
	char run_dir[MAXPATHLEN + 1];
	char report_dir[MAXPATHLEN + 1];
	char spec_file[MAXPATHLEN + 1];
	char summary_file[MAXPATHLEN + 1];
	int nodenum = -1;
	bool dom_ready = false;
	bool locked = false;
	bool match = false;
	char rep_name[MAXNAMELEN];
	char cpss_name[MAXNAMELEN];
	char policy_id[POLICY_ID_LEN + 1];
	int edit_dfd = -1;
	int report_dfd = -1;
	int run_dfd = -1;

	check_domain_path(root, &sta, &error);
	if (error)
		goto out;

	siq_domain_mark_policy_id(root, policy_id, &error);
	if (error)
		goto out;

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* make sure a snap revert job doesn't already exist */
	if (siq_job_exists(policy_id)) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "A domain mark run directory already exists");
		goto out;
	}

	/* check for existing domain at same location */
	siq_find_domain(root, type, NULL, NULL, &dom_ready, &error);
	if (error)
		goto out;

	if (dom_ready) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "A ready %s domain already exists for path %s",
		    domain_type_to_text(type), root);
		goto out;
	}

	/* if type is synciq, only allow domain creation if the path matches
	 * a root path of an existing synciq policy on this cluster */
	if (type == DT_SYNCIQ) {
		pols = siq_policies_load_readonly(&error);
		if (error)
			goto out;

		SLIST_FOREACH(pol, &pols->root->policy, next) {
			if (stat(pol->datapath->root_path, &stb) != 0)
				continue;

			if (sta.st_ino == stb.st_ino) {
				match = true;
				break;
			}
		}

		siq_policies_free(pols);
		pols = NULL;
		pol = NULL;

		if (match == false) {
			error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
			    "Creation of a synciq type domain is only allowed "
			    "at the root path of an existing synciq policy");
			goto out;
		}
	}

	/* fail if this domain would illegally overlap an existing domain */
	domain_conflict(root, type, &error);
	if (error)
		goto out;

	/* create run dir and spec for the domain mark job */
	pol = siq_policy_create();
	pol->common->pid = strdup(policy_id);
	asprintf(&pol->common->name, "domain-mark-%llx", sta.st_ino);
	
	switch (type) {
	case DT_SYNCIQ:
		pol->common->action = SIQ_ACT_SYNCIQ_DOMAIN_MARK;
		break;

	case DT_SNAP_REVERT:
		pol->common->action = SIQ_ACT_SNAP_REVERT_DOMAIN_MARK;
		break;

	case DT_WORM:
		log(NOTICE, "WORM domain mark job found!");
		pol->common->action = SIQ_ACT_WORM_DOMAIN_MARK;
		break;
	
	default:
		ASSERT(!"Unexpected domain type for domain mark job");
	}

	pol->datapath->root_path = strdup(root);
	pol->datapath->src_cluster_name = strdup("localhost");
	pol->target->path = strdup(root);
	pol->target->dst_cluster_name = strdup("localhost");
	pol->coordinator->dst_snapshot_mode = SIQ_SNAP_NONE;
	pol->coordinator->workers_per_node = workers_per_node;
	pol->common->log_level = log_level;

	/* create job dir in edit dir */
	sprintf(edit_dir, "%s/%s", sched_get_edit_dir(), pol->common->pid);
	sprintf(report_dir, "%s/%s", sched_get_reports_dir(),
	    pol->common->pid);
	sprintf(spec_file, "%s/%s", edit_dir, SPEC_FILENAME);
	sprintf(summary_file, "%s/%s", edit_dir, REPORT_FILENAME);

	if (grab_local) {
		nodenum = sched_get_my_node_number();
		ASSERT(nodenum != -1);
		sprintf(run_dir, "%s_%d", pol->common->pid, nodenum);
	} else {
		sprintf(run_dir, "%s", pol->common->pid);
	}

	edit_dfd = open(sched_get_edit_dir(), O_RDONLY);
	if (edit_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open edit dir %s", sched_get_edit_dir());
		goto out;
	}
	if (enc_mkdirat(edit_dfd, pol->common->pid, ENC_UTF8, 0755) != 0 &&
	    errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Failed to make temp job dir %s", edit_dir);
		goto out;
	}

	report_dfd = open(sched_get_reports_dir(), O_RDONLY);
	if (report_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open report dir %s", sched_get_reports_dir());
		goto out;
	}
	if (enc_mkdirat(report_dfd, pol->common->pid, ENC_UTF8, 0755) != 0 &&
	    errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Failed to make report dir %s", report_dir);
		goto out;
	}

	siq_spec_save(pol, spec_file, &error);
	if (error)
		goto out;

	summary = siq_job_summary_create();
	ASSERT(make_job_summary(summary, pol->common->action) == 0);

	gcs = siq_job_summary_save_new(summary, summary_file, &error);
	if (error)
		goto out;

	run_dfd = open(sched_get_run_dir(), O_RDONLY);
	if (run_dfd == -1) {
		error = isi_system_error_new(errno,
		    "Failed to open run dir %s", sched_get_run_dir());
		goto out;
	}
	if (enc_renameat(edit_dfd, pol->common->pid, ENC_UTF8,
	    run_dfd, run_dir, ENC_UTF8) != 0 && errno != ENOTEMPTY) {
		log(ERROR, "Failed to move temp job dir %s to run dir",
		    edit_dir);
		goto out;
	}

	/* create source record for job */
	if (siq_source_has_record(policy_id) == false) {
		siq_source_create(policy_id, &error);
		if (error)
			goto out;
	}

	srec = siq_source_record_open(policy_id, &error);
	if (error)
		goto out;

	siq_source_set_pol_stf_ready(srec);

	siq_source_record_write(srec, &error);
	if (error)
		goto out;

	/* create repstate for job */
	get_base_rep_name(rep_name, policy_id, false);
	create_repstate(policy_id, rep_name, true, &error);
	if (error)
		goto out;

	get_base_cpss_name(cpss_name, policy_id, false);
	create_cpss(policy_id, cpss_name, true, &error);
	if (error)
		goto out;

out:
	if (locked)
		siq_job_unlock();
	if (srec) {
		siq_source_record_unlock(srec);
		siq_source_record_free(srec);
	}
	if (gcs)
		siq_gc_job_summary_free(gcs);
	if (pol)
		siq_policy_free(pol);
	if (edit_dfd != -1)
		close(edit_dfd);
	if (report_dfd != -1)
		close(report_dfd);
	if (run_dfd != -1)
		close(run_dfd);
	isi_error_handle(error, error_out);
}


/*  ____                      ____                     _   
 * / ___| _ __   __ _ _ __   |  _ \ _____   _____ _ __| |_ 
 * \___ \| '_ \ / _` | '_ \  | |_) / _ \ \ / / _ \ '__| __|
 *  ___) | | | | (_| | |_) | |  _ <  __/\ V /  __/ |  | |_ 
 * |____/|_| |_|\__,_| .__/  |_| \_\___| \_/ \___|_|   \__|
 *                   |_|                                   
 */

/* generate a policy id based on the snapshot id. this can be used
 * to look up the policy when only the snapshot id is known */
void
siq_snap_revert_policy_id(ifs_snapid_t snapid, char *pol_id_out,
    struct isi_error **error_out)
{
	MD5_CTX md;
	char md5in[64];
	struct isi_error *error = NULL;
	
	ASSERT(pol_id_out);
	if (snapid == INVALID_SNAPID) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Invalid snapshot id specified: %llu", snapid);
		goto out;
	}

	snprintf(md5in, 64, "%llu_SNAP_REVERT", snapid);

	MD5Init(&md);
	MD5Update(&md, md5in, strlen(md5in));
	MD5End(&md, pol_id_out);

out:
	isi_error_handle(error, error_out);
}

void
siq_cleanup_snap_revert_job(ifs_snapid_t snapid, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char policy_id[POLICY_ID_LEN + 1];

	siq_snap_revert_policy_id(snapid, policy_id, &error);
	if (error)
		goto out;

	/* trash temporary repstates and linmaps */
	siq_btree_remove_source(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
	siq_btree_remove_target(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	/* cleanup source record */
	siq_source_delete(policy_id, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	/* cleanup target record */
	siq_target_delete_record(policy_id, true, false, &error);
	if (error) {
		isi_error_free(error);
		error = NULL;
	}

	cleanup_temp_reports(policy_id);

	/* cleanup job lock file */
	siq_job_rm_lock_file(policy_id);

out:
	isi_error_handle(error, error_out);
}

void
siq_start_snap_revert_job(ifs_snapid_t snapid, int log_level,
    int workers_per_node, bool grab_local, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_str **snap_paths = NULL;
	struct stat st_snap;
	struct stat st_head;
	struct target_record *trec = NULL;
	int i;
	int tdb_lock_fd = -1;
	int snap_base_fd = -1;
	char *base_path = NULL;
	char *tmp = NULL;
	SNAP *snap = NULL;
	ifs_domainid_t dom_id = 0;
	uint32_t dom_gen = 0;
	bool locked = false;
	char policy_id[POLICY_ID_LEN + 1];
	int compliance_dom_ver;

	siq_snap_revert_policy_id(snapid, policy_id, &error);
	if (error)
		goto out;

	locked = siq_job_lock(policy_id, &error);
	if (error)
		goto out;

	/* make sure a snap revert job doesn't already exist */
	if (siq_job_exists(policy_id)) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "A snapshot revert run directory already exists");
		goto out;
	}

	/* verify snapshot exists and open it. reject system snapshots */
	snap = snapshot_open_by_sid(snapid, NULL);
	if (snap == NULL || snap->snap_stat.system) {
		error = isi_siq_error_new(E_SIQ_CONF_NOENT,
		    "Cannot start snapshot revert job. Invalid snapshot id %d",
		    snapid);
		goto out;
	}

	/* get snapshot base path. multiple paths not supported */
	snap_paths = snapshot_get_paths(snap);
	for (i = 0; snap_paths[i]; i++) {
		base_path = ISI_STR_GET_STR(snap_paths[i]);
		log(DEBUG, "path: %s", base_path);
	}
	ASSERT(i > 0);
	if (i > 1) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Snapshot "
		    "revert does not support snapshots with multiple paths");
		goto out;
	}

	/* verify path is within (not equal to) /ifs */
	ASSERT(strncmp(base_path, IFSPATH, strlen(IFSPATH)) == 0);
	if (strlen(base_path) <= 5) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Reverting a "
		    "snapshot based at /ifs is unsupported");
		goto out;
	}

	/* verify path is not at or below .ifsvar or .snapshot */
	if (strncmp(base_path, IFSVARPATH, strlen(IFSVARPATH)) == 0 ||
	    strncmp(base_path, IFSSNAPSHOTPATH, strlen(IFSSNAPSHOTPATH))
	    == 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Reverting a "
		    "snapshot based at or below %s or %s is unsupported",
		    IFSVARPATH, IFSSNAPSHOTPATH);
		goto out;
	}

	/* verify base path exists in head */
	if (stat(base_path, &st_head) != 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Error "
		    "accessing snapshot base path %s in head: %s", base_path,
		    strerror(errno));
		goto out;
	}
	if (S_ISDIR(st_head.st_mode) == false) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Snapshot base "
		    "path %s is not a directory in head.", base_path);
		goto out;
	}

	/* verify snapshot and head base path LINs match */
	tmp = base_path + strlen(IFSPATH) + 1;
	snap_base_fd = ifs_lin_open(ROOT_LIN, snapid, O_RDONLY);
	if (enc_fstatat(snap_base_fd, tmp, ENC_DEFAULT, &st_snap,
	    AT_SYMLINK_NOFOLLOW) == -1) {
	    	error = isi_system_error_new(errno, "Error getting stat info "
		    "for snapshot base path %s", base_path);
		goto out;
	}
	if (st_snap.st_ino != st_head.st_ino) {
		error = isi_system_error_new(errno, "Snapshot base path LIN "
		    "%llu does not match LIN %llu in head", st_snap.st_ino,
		    st_head.st_ino);
		goto out;
	}

	/* Disable non-SyncIQ snap revert jobs for compliance. */
	compliance_dom_ver = get_compliance_dom_ver(base_path, &error);
	if (error) {
		/* No domain is okay. */
		if (!isi_system_error_is_a(error, ENOENT))
			goto out;

		isi_error_free(error);
		error = NULL;
	}

	if (compliance_dom_ver > 0) {
		error = isi_siq_error_new(E_SIQ_CONF_INVAL, "Reverting a "
		    "snapshot based in a compliance domain is unsupported");
		goto out;
	}

	/* Bug 138780
	 * find a domain governing the base path.
	 * search order should match setup_restore_domain() so that we agree on
	 * what domain is being locked */
	find_domain_by_lin(st_snap.st_ino, DT_SYNCIQ, true, &dom_id, &dom_gen,
	    &error);
	if (error)
		goto out;
	if (!dom_id) {
		find_domain_by_lin(st_snap.st_ino, DT_SNAP_REVERT, true,
		    &dom_id, &dom_gen, &error);
		if (error)
			goto out;
	}
	if (dom_id == 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT, "Error locating "
		    "ready snapshot revert or synciq domain for snapshot");
		goto out;
	}

	/* lock the domain if it isn't already */
	if (dom_set_readonly(dom_id, true) != 0) {
		error = isi_siq_error_new(E_SIQ_CONF_CONTEXT,
		    "Error locking revert domain");
		goto out;
	}

	/* create dummy record for revert job */
	tdb_lock_fd = siq_target_acquire_lock(policy_id, O_EXLOCK, &error);
	if (error)
		goto out;
	ASSERT(tdb_lock_fd != -1);
	trec = calloc(1, sizeof(struct target_record));
	ASSERT(trec);

	trec->id = strdup(policy_id);
	trec->policy_name = strdup("snapshot_revert");
	trec->fofb_state = FOFB_FAILOVER_STARTED;
	trec->job_status = SIQ_JS_NONE;
	trec->domain_id = dom_id;
	trec->domain_generation = dom_gen;
	trec->latest_snap = snapid;
	trec->target_dir = strdup(base_path);

	siq_target_record_write(trec, &error);
	if (error)
		goto out;

	/* cleanup before handing off to failover code */
	snapshot_close(snap);
	snap = NULL;
	close(tdb_lock_fd);
	tdb_lock_fd = -1;

	/* failover takes over from here */
	siq_start_failover_job(policy_id, log_level, workers_per_node,
	    grab_local, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
	if (snap)
		snapshot_close(snap);
	if (snap_paths)
		free(snap_paths);
	siq_target_record_free(trec);
	if (tdb_lock_fd != -1)
		close(tdb_lock_fd);
	if (snap_base_fd != -1)
		close(snap_base_fd);
	if (locked)
		siq_job_unlock();
}

