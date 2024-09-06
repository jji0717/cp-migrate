#ifndef SCHED_UTILS_H
#define SCHED_UTILS_H

struct sched_ctx;

int sched_remove_dir(struct sched_ctx *sctx, const char *remove_dir,
    const char *work_dir);
bool sched_check_status(const char *test_job_dir, const char *status_file);
enum sched_ret sched_job_cancel(struct sched_ctx *, const char *dir_name);
bool sched_job_preempt(struct sched_ctx *, const char *job_dir,
    const char *dir_name, struct isi_error **error_out);
int sched_my_jobs_remove(struct sched_ctx *);
int sched_check_state(struct sched_ctx *);
int sched_unlink(struct sched_ctx *, const char *remove_dir);
time_t get_next_policy_start_time(struct sched_ctx *, struct isi_error **);
int sched_quorum_wait(struct sched_ctx *sctx);
bool add_vec_elem(struct ptr_vec *queue, void *element, size_t element_size,
    int max_elements, bool (*insert_first)(void *e1, void *e2));
void update_node_max_workers(struct sched_ctx *sctx,
    struct isi_error **error_out);
struct sched_node * sched_get_node_info(struct sched_ctx *sctx, int node_id);

/**
 * After the initial opening of a directory, this reads in "." and ".."
 * which in most cases are not needed.
 */
static inline void
opendir_skip_dots(DIR *dirp)
{
	readdir(dirp);
	readdir(dirp);
}

#endif /* SCHED_UTILS_H */
