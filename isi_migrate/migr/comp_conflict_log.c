#include "comp_conflict_log.h"

#define BUFSIZE 1024

struct comp_conflict_log_ctx log_ctx = {};

/* Create the log dir for the given policy_id and run_id. This will create
 * the log dir SIQ_CONFLICT_LOG_DIR/<policy_id>/<run_id>. */
void
create_comp_conflict_log_dir(char *policy_id, uint64_t run_id,
    char **log_path_out, struct isi_error **error_out)
{
	int ret;
	char log_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(log_path, MAXPATHLEN, "%s/%s", SIQ_CONFLICT_LOG_DIR,
	    policy_id);

	ret = mkdir(log_path, (mode_t)(S_IRWXU | S_IRWXG | S_IRWXO));
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to make "
		    "compliance conflict log dir %s", log_path);
		goto out;
	}

	snprintf(log_path + strlen(log_path), MAXPATHLEN - strlen(log_path),
	    "/%llu", run_id);

	ret = mkdir(log_path, (mode_t)(S_IRWXU | S_IRWXG | S_IRWXO));
	if (ret == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to make "
		    "compliance conflict log dir %s", log_path);
		goto out;
	}

	if (log_path_out)
		*log_path_out = strdup(log_path);

out:
	isi_error_handle(error, error_out);
}

/* Open and lock the given filename. If an error is returned, the caller is
 * responsible for closing any file or lock_fd that may have been opened. */
static void
open_and_lock(char *filename, int lock_type, char *mode, FILE **fp,
    int *lock_fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	*lock_fd = lock_file(filename, lock_type);
	if (*lock_fd < 0) {
		error = isi_system_error_new(errno, "Failed to get a lock for "
		    "file %s", filename);
		goto out;
	}

	*fp = fopen(filename, mode);
	if (*fp == NULL) {
		error = isi_system_error_new(errno, "Failed to open file %s",
		    filename);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

void
finalize_comp_conflict_log(char *log_dir_in, char **final_log_out,
    struct isi_error **error_out)
{
	int final_lock_fd = -1, cur_lock_fd = -1;
	int num_read, num_written;
	DIR *dir = NULL;
	char buf[BUFSIZE];
	char log_dir[MAXPATHLEN];
	char final_log_path[MAXPATHLEN], cur_log_path[MAXPATHLEN];
	FILE *final_log_file = NULL, *cur_log_file = NULL;
	struct dirent *entry;
	struct isi_error *error = NULL;

	/* Append a '/' to the given log_dir_in if necessary. */
	snprintf(log_dir, sizeof(log_dir), "%s%s", log_dir_in,
	    log_dir_in[strlen(log_dir_in) - 1] == '/' ? "":"/");

	/* Construct the path to the final coalesced log and open/lock it. */
	snprintf(final_log_path, sizeof(final_log_path), "%s%s", log_dir,
	    SIQ_CONFLICT_LOG);
	open_and_lock(final_log_path, O_EXLOCK, "w+", &final_log_file,
	    &final_lock_fd, &error);
	if (error)
		goto out;

	dir = opendir(log_dir);
	if (dir == NULL) {
		error = isi_system_error_new(errno, "Failed to open "
		    "compliance conflict log dir %s", log_dir);
		goto out;
	}

	/* Iterate all of the per-node logs and coalesce them into one final
	 * cluster-wide log at final_log_path. */
	while ((entry = readdir(dir)) != NULL) {
		/* Skip dots, dirs, lock files, and any file that doesn't
		 * start with "lnn". */
		if (entry->d_name[0] == '.' ||
		    entry->d_type != DT_REG ||
		    strstr(entry->d_name, ".lock") ||
		    strncmp(entry->d_name, "lnn", 3))
			continue;

		/* Get the path to the current log and open/lock it. */
		snprintf(cur_log_path, sizeof(cur_log_path), "%s%s", log_dir,
		    entry->d_name);
		open_and_lock(cur_log_path, O_SHLOCK, "r", &cur_log_file,
		    &cur_lock_fd, &error);
		if (error)
			goto out;

		/* Append contents from current log file to the final log
		 * file. */
		while (!feof(cur_log_file)) {
			num_read = fread(buf, 1, BUFSIZE, cur_log_file);
			if (num_read != BUFSIZE && !feof(cur_log_file)) {
				error = isi_system_error_new(errno, "Failed "
				    "to read from log file %s", cur_log_path);
				goto out;
			}

			num_written = fwrite(buf, 1, num_read, final_log_file);
			if (num_written != num_read) {
				error = isi_system_error_new(errno, "Failed "
				    "to write to log file %s", final_log_path);
				goto out;
			}
		}

		fclose(cur_log_file);
		cur_log_file = NULL;

		close(cur_lock_fd);
		cur_lock_fd = -1;
	}

out:
	if (dir != NULL)
		closedir(dir);

	if (final_log_file != NULL)
		fclose(final_log_file);

	if (cur_log_file != NULL)
		fclose(cur_log_file);

	if (cur_lock_fd != -1)
		close(cur_lock_fd);

	if (final_lock_fd != -1)
		close(final_lock_fd);

	if (final_log_out)
		*final_log_out = strdup(final_log_path);

	isi_error_handle(error, error_out);
}


/* Initialize the global log_ctx. This initializes the path to the log file
   that the caller will write to with subsequent calls to log_comp_conflict. */
void
init_comp_conflict_log(char *policy_id, uint64_t run_id, int lnn)
{
	if (log_ctx.log_path != NULL) {
		free(log_ctx.log_path);
		log_ctx.log_path = NULL;
	}

	asprintf(&log_ctx.log_path, "%s/%s/%llu/lnn_%d", SIQ_CONFLICT_LOG_DIR,
	    policy_id, run_id, lnn);
	ASSERT(log_ctx.log_path);
}

/* Return true if the caller has initializaed the log_ctx. */
bool
has_comp_conflict_log(void)
{
	return (log_ctx.log_path != NULL);
}

/* Open the log file SIQ_CONFLICT_LOG_DIR/<policy_id>/<run_id>/<lnn>
 * and append the the conflict_path and the link_path. */
void
log_comp_conflict(char *conflict_path, char *link_path,
    struct isi_error **error_out)
{
	int lock_fd = -1;
	FILE *file = NULL;
	struct isi_error *error = NULL;

	ASSERT(log_ctx.log_path);

	lock_fd = lock_file(log_ctx.log_path, O_EXLOCK);
	if (lock_fd < 0) {
		error = isi_system_error_new(errno, "Failed to get a lock for "
		    "compliance conflict log %s", log_ctx.log_path);
		goto out;
	}

	file = fopen(log_ctx.log_path, "a+");
	if (!file) {
		error = isi_system_error_new(errno, "Failed to open "
		    "compliance conflict log %s", log_ctx.log_path);
		goto out;
	}

	fprintf(file, "Conflicted path: %s\nResolved path: %s\n\n",
	    conflict_path, link_path);


out:
	if (file)
		fclose(file);

	if (lock_fd >= 0)
		close(lock_fd);

	isi_error_handle(error, error_out);
}
