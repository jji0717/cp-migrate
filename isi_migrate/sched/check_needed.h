#ifndef __CHECK_NEEDED__
#define __CHECK_NEEDED__

#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <isi_util/check_helper.h>
#include <stdbool.h>
#include "sched_includes.h"
#include <isi_ufp/isi_ufp.h>
#include "sched_local_work.h"
#include "sched_grabbed.h"

#define TEST_RUN_DIR "/ifs/siq_test_run_dir"

extern struct grabbed_list g_grabbed_list;

struct check_dir {
	enum job_progress progress;
	char policy_id[MAXPATHLEN + 1];
	char dir[MAXPATHLEN + 1];
	int node_id;
	int pid;
};

void age_grabbed_list(int secs);
struct sched_job *
create_job(char *id, enum job_progress progress, int node, int pid,
    uint64_t ino);
void print_dir(const char *dir_name);
int
create_run_dir(struct check_dir *dir_out, const char *run_dir,
    const char *policy_id, int node_id, int coord_pid);
int create_file(const char *dir, const char *file);
int confirm_run_dir(struct check_dir *dir);
int confirm_run_dir_file(struct check_dir *dir, const char *file);
int check_run_dir_array(struct check_dir *dir,
    struct check_dir *dirs_out, int size);
int
check_run_dir(struct check_dir *dir, struct check_dir *dir_out);
int
confirm_dir_state(struct check_dir *dir, enum job_progress expected_progress,
    int expected_node);
struct sched_job *create_job_from_dir(struct check_dir *dir, uint64_t ino);

#endif //  __CHECK_NEEDED__
