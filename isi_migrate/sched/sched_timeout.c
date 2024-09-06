#include "sched_includes.h"
#include "sched_timeout.h"

static const int JOB_TIMEOUT_SECS = 300; /*  5 minutes */
static const int JOB_EXPIRE_SECS = 600;  /* 10 minutes */

/* The list of jobs being watched for a timeout */
struct job_list g_watched = SLIST_HEAD_INITIALIZER(g_watched);

/* The list of jobs newly found to be deterred. Will be transferred to watched
 * list after done processing run directory */
struct job_list g_deferred = SLIST_HEAD_INITIALIZER(g_deferred);

static void 
remove_job_entry(struct job_list *j_list, struct job_entry *j_entry)
{ 
	log(TRACE, "%s: Removing from %s %s", __func__,
	    j_list == &g_watched ? "watched" : "deferred",
	    j_entry->policy_name);

	free(j_entry->id);
	free(j_entry->policy_name);
	SLIST_REMOVE(j_list, j_entry, job_entry, ENTRIES);
	free(j_entry);
}

static void
clear_job_list(struct job_list *j_list)
{
	struct job_entry *j_entry, *tmp;

	log(TRACE, "%s: clearing %s", __func__,
	    j_list == &g_watched ? "watched" : "deferred");

	
	SLIST_FOREACH_SAFE(j_entry, j_list, ENTRIES, tmp) {
		remove_job_entry(j_list, j_entry);
	}
}

void
clear_deferred_list()
{
	clear_job_list(&g_deferred);
}

static void
add_to_job_list(struct job_list *j_list, const char *id,
    const char *policy_name, uint64_t ino, bool is_delete)
{
	struct job_entry *j_entry, *tmp;
	bool already_in_list = false;

	log(TRACE, "%s: adding %s to %s", __func__, policy_name,
	    j_list == &g_watched ? "watched" : "deferred");

	/* No duplicate entries allowed */
	SLIST_FOREACH_SAFE(j_entry, j_list, ENTRIES, tmp) {
		if (!strcmp(j_entry->id, id)) {
			log(INFO, "%s / %s already in list", policy_name, id);
			already_in_list = true;
		}	
	}

	if (!already_in_list)
	{
		j_entry = calloc(1, sizeof(struct job_entry));
		ASSERT(j_entry);
		j_entry->id = strdup(id);
		j_entry->policy_name = strdup(policy_name);
		j_entry->time_added = time(0);
		j_entry->ino = ino;
		j_entry->is_delete = is_delete;
	    	SLIST_INSERT_HEAD(j_list, j_entry, ENTRIES);
	}
}

bool
add_to_deferred_list(struct sched_job *job, bool force)
{
	bool added = false;

	/* Check the conditions for deferring a job. */
	if (!force) {
		if (sched_check_status(job->policy_id, SCHED_JOB_PAUSE))
			goto out;
	}

	add_to_job_list(&g_deferred, job->policy_id, job->policy_name,
	    job->ino, job->is_delete);
	added = true;

out:
	return added;
}

static void
remove_from_job_list(struct job_list *j_list, const char *id)
{
	struct job_entry *j_entry;

	log(TRACE, "%s: removing from %s id %s", __func__,
	    j_list == &g_watched ? "watched" : "deferred", id);

	SLIST_FOREACH(j_entry, j_list, ENTRIES) {
		if (strcmp(j_entry->id, id) == 0) {
			remove_job_entry(j_list, j_entry);
			break;
		}
	}
}	

void
remove_from_watched(const char *id)
{
	remove_from_job_list(&g_watched, id);
}

void
timeout_deferred_jobs(bool space_for_jobs)
{
	struct job_entry *new_job, *old_job, *tmp1, *tmp2;
	bool found;
	char *err_msg = NULL;

	/* If no space for new jobs, then the watched list is cleared out
	 * (we're watching only jobs that have the capability of being started
	 * on other nodes). */
	if (!space_for_jobs) {
		log(TRACE, "%s: no space for jobs. Clearing watched list",
		    __func__);
		clear_job_list(&g_watched);
		return;
	}

	if (SLIST_EMPTY(&g_watched))
		log(TRACE, "%s: watch list is empty", __func__);
	else
		log(TRACE, "%s: watch list has:", __func__);

	/* First remove no-longer deferred jobs from watched list */
	SLIST_FOREACH_SAFE(old_job, &g_watched, ENTRIES, tmp2) {
		log(TRACE, "%s:     %s  (%ld secs)", __func__,
		    old_job->policy_name, time(0) - old_job->time_added);
		found = false;
		SLIST_FOREACH_SAFE(new_job, &g_deferred, ENTRIES, tmp1) {
			if (strcmp(new_job->id, old_job->id) == 0 &&
			    new_job->ino == old_job->ino) {
				found = true;
				break;
			}
		}
		if (!found) {
			log(TRACE,
			    "%s: job %s has been started. No need to watch",
			    __func__, old_job->policy_name);
			remove_job_entry(&g_watched, old_job);
		}
	}

	if (SLIST_EMPTY(&g_deferred))
		log(TRACE, "%s: deferred list is empty", __func__);
	else
		log(TRACE, "%s: deferred list has:", __func__);

	/* Add new entries to watched list, and timout any entries in watched
	 * list */
	SLIST_FOREACH_SAFE(new_job, &g_deferred, ENTRIES, tmp1) {
		
		log(TRACE, "%s:     %s", __func__, new_job->policy_name);
		found = false;

		SLIST_FOREACH_SAFE(old_job, &g_watched, ENTRIES, tmp2) {

			if (strcmp(new_job->id, old_job->id) == 0) {

				found = true;

				/* If inodes don't match, it's really a new
				 * job */
				if (new_job->ino != old_job->ino) {
					log(INFO,"%s: Matching id (%s), "
					    "but new inode",
					    __func__, old_job->policy_name);
					/* Get rid of the old one */
					remove_job_entry(&g_watched, old_job);
					/* And replace with new one */
					add_to_job_list(&g_watched,
					    new_job->id, new_job->policy_name,
					    new_job->ino, new_job->is_delete);
				}
				
				/* This job's already in the watched
				 * list. Check for timeout */
				else if (time(0) - old_job->time_added >
				    JOB_TIMEOUT_SECS) {
					asprintf(&err_msg, "Timing out policy "
					    "%s after %ld secs",
					    old_job->policy_name,
					    time(0) - old_job->time_added);
					log(ERROR, "%s", err_msg);
					delete_or_kill_job(old_job->id,
					    old_job->policy_name, NULL, 0,
					    err_msg, old_job->is_delete);
					/* old_job is removed in kill_job() */
					free(err_msg);
					err_msg = NULL;
				}
				/* Otherwise, job has not yet expired
				 * Just leave in g_watched list. */

				/* Job already found. No need to keep
				 * searching through g_watched for the job */
				break;
			}
		}

		/* New deferred job. Add to watched list */
		if (!found) {
			log(TRACE, "%s: New watched job %s", __func__,
			    new_job->policy_name);
			add_to_job_list(&g_watched, new_job->id,
			    new_job->policy_name, new_job->ino,
			    new_job->is_delete);
		}
	}

	/* No job should linger on the watched list. If for some programming
	 * error, a job do lingers, expire it by clearing it off the list and
	 * log the error (but keep on going). */
	SLIST_FOREACH_SAFE(old_job, &g_watched, ENTRIES, tmp1) {
		if (time(0) - old_job->time_added > JOB_EXPIRE_SECS) {
			log(ERROR, "Expiring watched job %s after %ld secs",
			    old_job->policy_name,
			    time(0) - old_job->time_added);
			remove_job_entry(&g_watched, old_job);
		}
	}

}
