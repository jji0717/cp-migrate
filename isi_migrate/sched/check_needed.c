#include "check_needed.h"

struct grabbed_list g_grabbed_list;

// To avoid having to do long sleeps during unit tests with the grabbed list,
// back set the grabbed list entries to look like they were inserted earlier.
// Uses the g_grabbed_list defined in sched_local_work.c
void
age_grabbed_list(int secs)
{
	struct grabbed_entry *g;

	SLIST_FOREACH(g, &g_grabbed_list, next)
		g->timestamp -= secs;
}

struct sched_job *
create_job(char *id, enum job_progress progress, int node, int pid,
    uint64_t ino)
{
	struct sched_job *job;

	job = calloc(1, sizeof(struct sched_job));
	strcpy(job->policy_id, id);
	strcpy(job->policy_name, id);
	job->progress = progress;
	job->node_num = node;
	job->coord_pid = pid;
	job->ino = ino;
	
	SZSPRINTF(job->available_dir, "%s/%s", TEST_RUN_DIR, id);
	SZSPRINTF(job->local_grabbed_dir, "%s/%s_1", TEST_RUN_DIR, id);
	if (progress == PROGRESS_AVAILABLE) {
		SZSPRINTF(job->current_dir, "%s/%s", TEST_RUN_DIR, id);
	}
	else if (progress == PROGRESS_GRABBED) {
		SZSPRINTF(job->current_dir, "%s/%s_%d", TEST_RUN_DIR, id,
		    node);
	}
	else
		SZSPRINTF(job->current_dir, "%s/%s_%d_%d", TEST_RUN_DIR,
		    id, node, pid);

	// localhost as target insures that we can always confirm a connection
	// to the fake target. But using "localhost" causes a leak in
	// getaddrinfo(). So use the loopback address istead.
	SZSPRINTF(job->target, "127.0.0.1");
	job->restrict_by[0] = '\0';
	job->force_interface = false;

	return job;
}

struct sched_job *
create_job_from_dir(struct check_dir *dir, uint64_t ino)
{
	return create_job(dir->policy_id, dir->progress, dir->node_id, dir->pid, ino);
}


void
print_dir(const char *dir_name)
{
	static int depth;
	DIR *dirp;
	struct dirent *dp;
	char path[MAXPATHLEN + 1];
	int i;

	if (depth == 0)
		printf("\n");

	dirp = opendir(dir_name);
	if (!dirp) {
		printf("%s: Can't open '%s'\n", __func__, dir_name);
		return;
	}
	    
	readdir(dirp);
	readdir(dirp);

	while ((dp = readdir(dirp))) {
		for (i = 0; i < depth; i++)
			printf("    ");

		printf("%s", dp->d_name);
		if (dp->d_type == DT_DIR) {
			printf("/\n");
			depth++;
			SZSPRINTF(path, "%s/%s", dir_name, dp->d_name);
			print_dir(path);
			depth--;
		}
		else
			printf("\n");
	}
	closedir(dirp);
}

int
create_run_dir(struct check_dir *dir_out, const char *run_dir,
    const char *policy_id, int node_id, int coord_pid)
{
	// IDs must be 32 characters long
	char filled_id[] = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; 
	int ret;

	bzero(dir_out, sizeof(*dir_out));
	strncpy(filled_id, policy_id, strlen(policy_id));
	SZSPRINTF(dir_out->policy_id, filled_id);
	if (node_id == 0) {
		SZSPRINTF(dir_out->dir, "%s/%s", run_dir,
		    filled_id);
		dir_out->progress = PROGRESS_AVAILABLE;
	}
	else if (coord_pid == 0) {
		SZSPRINTF(dir_out->dir, "%s/%s_%d", run_dir,
		    filled_id, node_id);
		dir_out->progress = PROGRESS_GRABBED;
		dir_out->node_id = node_id;
	}
	else {
		SZSPRINTF(dir_out->dir, "%s/%s_%d_%d", run_dir,
		    filled_id, node_id, coord_pid);
		dir_out->progress = PROGRESS_STARTED;
		dir_out->node_id = node_id;
		dir_out->pid = coord_pid;
	}

	ret = mkdir(dir_out->dir, 0755);
	return ret;
}


int
create_file(const char *dir, const char *file)
{
	char path[MAXPATHLEN + 1];
	int fd;

	SZSPRINTF(path, "%s/%s", dir, file);

	fd = open(path, O_RDONLY | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	close(fd);
	return 0;
}


int
confirm_run_dir(struct check_dir *dir)
{
	int ret;

	ret = access(dir->dir, F_OK);
	return ret;
}


int
confirm_run_dir_file(struct check_dir *dir, const char *file)
{
	char path[MAXPATHLEN + 1];
	int ret;

	SZSPRINTF(path, "%s/%s", dir->dir, file);
	ret = access(path, F_OK);
	return ret;
}

int
check_run_dir_array(struct check_dir *dir, struct check_dir *dirs_out, int size)
{
	DIR *dirp;
	struct dirent *dp;
	int i = 0;
	char node_str[MAXPATHLEN + 1] = "";
	char pid_str[MAXPATHLEN + 1] = "";

	bzero(dirs_out, size * sizeof(struct check_dir));
	dirp = opendir(TEST_RUN_DIR);
	if (!dirp)
		return -1;
	    
	readdir(dirp);
	readdir(dirp);
	while ((dp = readdir(dirp))) {
		if (strncmp(dp->d_name, dir->policy_id, POLICY_ID_LEN) == 0) {
			dirs_out[i].progress = sched_decomp_name(dp->d_name,
			    dirs_out[i].policy_id, node_str, pid_str);
			SZSPRINTF(dirs_out[i].dir, "%s/%s",
			    TEST_RUN_DIR, dp->d_name);
			dirs_out[i].node_id = atoi(node_str);
			dirs_out[i].pid = atoi(pid_str);
			// If array is full, don't look for any more
			if (++i == size)
				break;
		}
	}
	closedir(dirp);
	return i;
}

int
check_run_dir(struct check_dir *dir, struct check_dir *dir_out)
{
	return !(check_run_dir_array(dir, dir_out, 1) == 1);
}

int
confirm_dir_state(struct check_dir *dir, enum job_progress expected_progress,
    int expected_node)
{
	if (dir->progress != expected_progress)
		    return -1;
	if (expected_progress == PROGRESS_AVAILABLE) {
		if (dir->node_id != 0 || dir->pid != 0)
			return -1;
		return 0;
	}
	
	if (expected_progress == PROGRESS_GRABBED) {
		if (dir->node_id != expected_node || dir->pid != 0)
			return -1;
		return 0;
	}

	if (expected_progress == PROGRESS_STARTED) {
		// Any non-zero pid considered OK (at least for the current
		// test framework).
		if (dir->node_id != expected_node || dir->pid == 0)
			return -1;
		return 0;
	}

	return -1;
}
