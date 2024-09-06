#include <ifs/bam/bam_pctl.h>

#include "isi_migrate/config/siq_job.h"
#include "isi_migrate/config/siq_report.h"
#include "isi_migrate/config/siq_target.h"
#include "isi_migrate/config/migrate_adt.h"
#include "isi_migrate/migr/isirep.h"

#include "sched_reports.h"

static bool
remove_report(sqlite3 *report_db, char *lin_str)
{
	ifs_lin_t lin;
	int ret = 0;
	char rel_path[MAXPATHLEN + 1];
	char path[MAXPATHLEN + 1];
	size_t path_len = 0;
	bool del_lock_file = false;
	struct isi_error *error = NULL;

	if (!ends_with(lin_str, ".xml")) {
		//This is a LIN, get a path out of it
		lin = atoll(lin_str);
		ret = pctl2_lin_get_path_plus(ROOT_LIN, lin, HEAD_SNAPID, 0, 0,
		    ENC_DEFAULT, sizeof(rel_path), rel_path, &path_len, 0,
		    NULL, NULL, 0, NULL, NULL,
		    PCTL2_LIN_GET_PATH_NO_PERM_CHECK);  // bug 220379
		if (ret != 0) {
			log(ERROR, "%s: LIN %s not found", __func__, lin_str);
			goto del_out;
		}
		snprintf(path, sizeof(path), "/ifs/%s", rel_path);
		del_lock_file = true;
	} else {
		//This is already a path
		snprintf(path, sizeof(path), "%s", lin_str);
	}
	
	log(TRACE, "%s: unlinking %s", __func__, path);
	if (unlink(path) == -1) {
		log(ERROR, "%s: Unable to unlink %s: %s", __func__,
		    path, strerror(errno));
		return false;
	}
	if (del_lock_file) {
		snprintf(path, sizeof(path), "/ifs/%s.lck", rel_path);
		log(TRACE, "%s: unlinking %s", __func__, path);
		unlink(path);
	}

del_out:
	siq_reportdb_delete_by_lin(report_db, (char *)lin_str, &error);
	if (error) {
		log(ERROR, "%s: Unable to delete from database %s: %s", 
		    __func__, path, isi_error_get_message(error));
		isi_error_free(error);
		return false;
	}
	return true;
}

static void
policy_reports_rotate(sqlite3 *report_db, struct siq_conf *config, 
    struct siq_policies *policies, struct isi_error **error_out)
{
	int max_reports = 0;
	time_t rotate_older_than_time;
	struct siq_pids_list *pids_list = NULL;
	char *pid;
	int i = 0;
	struct siq_policy *policy = NULL;
	int j = 0;
	struct reportdb_record **records = NULL;
	int num_records = 0;
	int num_reports_span = 0;
	char buffer[MAXPATHLEN + 1];
	bool res = false;
	struct isi_error *error = NULL;
	
	pids_list = siq_get_pids(policies);
	
	for (i = 0; i < pids_list->size; i++) {
		pid = pids_list->ids[i];
		policy = siq_policy_get(policies, pid, &error);
		if (error) {
			goto out;
		}
		max_reports = policy->scheduler->max_reports;

		/* Either get the rotation period from the policy... */
		if (policy->scheduler->rotate_report_period)
			rotate_older_than_time = time(NULL) - 
			    policy->scheduler->rotate_report_period;
		/* ... or globally */
		else
			rotate_older_than_time = time(0) -
			    siq_get_date(config->reports->rotation_period, 0);
	
		/* Date based rotation */
		siq_reportdb_update_dirty_bit_by_policy_id_and_time(report_db, 
		    1, pid, rotate_older_than_time, &error);
		if (error) {
			goto out;
		}
		
		num_records = siq_reportdb_get_by_policyid_dirtybit(report_db, 
		    &records, pid, 1, &error);
		if (error) {
			goto out;
		}
		
		for (j = 0; j < num_records; j++) {
			siq_reportdb_get_count_by_pid_jid_dirty_bit(report_db,
			    records[j]->policy_id, records[j]->job_id, 1, 
			    &num_reports_span, &error);
			if (error) {
				goto out;
			}
			if (num_reports_span != 0) {
				/* This report spans the rotation, don't delete */
				siq_reportdb_update_dirty_bit_by_lin(
				    report_db, 0, records[j]->lin, 
				    &error);
				if (error) {
					goto out;
				}
			} else {
				memset(&buffer, 0, sizeof(buffer));
				strcpy(buffer, records[j]->lin);
				res = remove_report(report_db, buffer);
				if (!res) {
					goto out;
				}
			}
		}
		siq_reportdb_records_free(records);
		records = NULL;
		
		/* Max num based rotation */ 
		siq_reportdb_get_count_by_policy_id(report_db, 
		    pid, &num_records, &error);
		if (error) {
			goto out;
		}
		
		if (max_reports <= 0)
			max_reports = config->reports->max;

		if (num_records > max_reports) {
			siq_reportdb_update_dirty_bit_by_policy_id_and_max_num(
			    report_db, 1, pid, max_reports, 
			    &error);
			
		}
		num_records = siq_reportdb_get_by_policyid_dirtybit(report_db, 
		    &records, pid, 1, &error);
		if (error) {
			goto out;
		}
		for (j = 0; j < num_records; j++) {
			memset(&buffer, 0, sizeof(buffer));
			strcpy(buffer, records[j]->lin);
			res = remove_report(report_db, buffer);
			if (!res) {
				goto out;
			}
		}
		
		siq_reportdb_records_free(records);
		records = NULL;
		policy = NULL;
	}

out:
	if (records) {
		siq_reportdb_records_free(records);
		records = NULL;
	}
	siq_free_pids(pids_list);
	isi_error_handle(error, error_out);
}

static void
policy_reports_sync(sqlite3 *report_db, char *policy_id, char *policy_name)
{
	char reports_dir[PATH_MAX + 1];
	enum report_type rtype;
	DIR *dirP;
	struct dirent *d;
	char path[MAXPATHLEN + 1];
	char buffer[30];
	struct siq_job_summary *job_summary = NULL;
	int count = 0;
	int legacy_count = 0;
	int res = 0;
	struct stat report_stat;
	char lin_str[30];
	struct isi_error *error = NULL;

	snprintf(reports_dir, sizeof(reports_dir),
	    "%s/%s", sched_get_reports_dir(), policy_id);
	log(TRACE,"%s: scanning policy <<%s>> report dir %s",  __func__,
	    policy_name, reports_dir);

	dirP = opendir(reports_dir);
	if (!dirP) {
		log(ERROR, "%s: opendir('%s') error %s", __func__,
		    reports_dir, strerror(errno));
		return;
	}

	while ((d = readdir(dirP))) {
		rtype = get_report_type(d->d_name);
		if (rtype != FILE_REPORT && rtype != FILE_LEGACY) {
			continue;
		}

		snprintf(path, sizeof(path), "%s/%s", reports_dir, d->d_name);
		res = stat(path, &report_stat);
		if (res != 0) {
			log(ERROR, "%s: Failed to stat report %s", __func__,
			    d->d_name);
			goto out;
		}
		snprintf(lin_str, sizeof(lin_str), "%lld", report_stat.st_ino);
		siq_reportdb_get_count_by_lin(report_db, lin_str, &count, 
		    &error);
		if (error) {
			log(ERROR, "%s: "
			    "Failed siq_reportdb_get_count_by_lin: %s",
			    __func__, isi_error_get_message(error));
			goto out;
		}
		if (count == 0) {
			/* DB entry missing, need to insert */
			job_summary = siq_job_summary_load_readonly(path, &error);
			if (error) {
				log(ERROR, "%s: "
				    "Failed siq_job_summary_load_readonly: %s", 
				    __func__, isi_error_get_message(error));
				goto out;
			}
			if (job_summary->job_id == 0) {
				siq_reportdb_get_count_by_legacy_job_id(
				    report_db, &legacy_count, &error);
				if (error) {
					log(ERROR, "%s: Failed "
					    "siq_reportdb_get_count_by_legacy_job_id: %s",
					     __func__, isi_error_get_message(error));
					goto out;
				}
				snprintf(buffer, sizeof(buffer), "LEGACY-%d", 
				    legacy_count + 1);
			} else {
				snprintf(buffer, sizeof(buffer), "%d", 
				    job_summary->job_id);
			}
			siq_reportdb_insert(report_db, lin_str, policy_id, 
			    buffer, job_summary->total->end, &error);
			if (error) {
				log(ERROR, "%s: "
				    "Failed siq_reportdb_insert: %s",
				    __func__, isi_error_get_message(error));
				goto out;
			}
			siq_job_summary_free(job_summary);
			job_summary = NULL;
		} else {
			/* Update dirty bit */
			siq_reportdb_update_dirty_bit_by_lin(report_db, 0, 
			    lin_str, &error);
			if (error) {
				log(ERROR, "%s: Failed "
				    "siq_reportdb_update_dirty_bit_by_lin: "
				    "%s", __func__, isi_error_get_message(error));
				goto out;
			}
		}
	}
out:
	if (job_summary)
		siq_job_summary_free(job_summary);
	isi_error_free(error);
	closedir(dirP);
}

int
rotate_report_periodic_cleanup(bool force)
{
	struct siq_policies *pl = NULL;
	struct siq_policy *p = NULL;
	struct target_records *trecs = NULL;
	struct target_record *trec = NULL;
	DIR		*dirp;
	struct dirent	*dp;
	struct siq_gc_conf *config;
	int ret = 0; 
	sqlite3 *report_db = NULL;
	struct isi_error *error = NULL;

	log(TRACE,"rotate_report_periodic_cleanup");

	config = siq_gc_conf_load(&error);
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		ret = FAIL;
		goto errout;
	}
	siq_gc_conf_close(config);

	pl = siq_policies_load_readonly(&error);
	if (error) {
		log(ERROR, "%s", isi_error_get_message(error));
		ret = FAIL;
		goto errout;
	}

	siq_target_records_read(&trecs, &error);
	if (error) {
		log(ERROR, "%s: failed to read target records: %s", __func__,
		    isi_error_get_message(error));
		ret = FAIL;
		goto errout;
	}

	report_db = siq_reportdb_open_config(SIQ_REPORTS_REPORT_DB_PATH, config,
	    &error);
	if (error) {
		log(ERROR, "%s: failed to open reports database: %s", __func__, 
		    isi_error_get_message(error));
		ret = FAIL;
		goto errout;
	}
	
	if (config->root->reports->policy_sync || force) {
		/* Set dirty bit on all database entries */
		siq_reportdb_update_dirty_bit(report_db, 1, &error);
		if (error) {
			log(ERROR, "%s: failed to update dirty bit: %s", 
			    __func__, isi_error_get_message(error));
			ret = FAIL;
			goto errout;
		}

		/*
		 * Go through the list of report policy folders
		 * Every policy has single folder with policy ID as name
		 */
		dirp = opendir(sched_get_reports_dir());
		if (dirp == NULL) {
			log(ERROR, "rotate_report: opendir(%s) error %s",
			    sched_get_reports_dir(), strerror(errno));
			ret = FAIL;
			goto errout;
		}

		while ((dp = readdir(dirp)) != NULL) {
			trec = NULL;

			p = siq_policy_get(pl, dp->d_name, &error);
			if (p) {
				policy_reports_sync(report_db, p->common->pid,
				    p->common->name);
			} else {
				trec = siq_target_record_find_by_id(trecs, 
				    dp->d_name);
			}

			if (trec) {
				policy_reports_sync(report_db, trec->id,
				    trec->policy_name);
			}

			if (error)
				isi_error_free(error);
			error = NULL;
		}
		closedir(dirp);
	
		/* Delete all database entries that still have dirty bit set */
		siq_reportdb_delete_by_dirty_bit(report_db, &error);
		if (error) {
			log(ERROR, "%s: failed delete by dirty bit: %s", 
			    __func__, isi_error_get_message(error));
			ret = FAIL;
			goto errout;
		}
	}
	
	/* Reports have been sync'ed, now do rotation */
	policy_reports_rotate(report_db, config->root, pl, &error);
	
errout:
	if (trecs)
		siq_target_records_free(trecs);

	siq_reportdb_close(report_db);
	if (config)
		siq_gc_conf_free(config);
	if (pl)
		siq_policies_free(pl);

	isi_error_free(error);
	return ret;
			    	
}

