#ifndef __SCHED_TIMEOUT_H__
#define __SCHED_TIMEOUT_H__

/* Used to track jobs so they can be timed out if they don't start */
struct job_entry {
	char *id;
	char *policy_name;
	uint64_t ino;
	time_t time_added;
	bool is_delete;
	SLIST_ENTRY(job_entry) ENTRIES;
};

SLIST_HEAD(job_list, job_entry);

void clear_deferred_list(void);
bool add_to_deferred_list(struct sched_job *job, bool force);
void remove_from_watched(const char *id);
void timeout_deferred_jobs(bool space_for_jobs);

#endif // __SCHED_TIMEOUT_H__
