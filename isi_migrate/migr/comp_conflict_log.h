#ifndef __COMP_CONFLICT_LOG_H_
#define __COMP_CONFLICT_LOG_H_

#include <stdbool.h>

#include "isirep.h"

struct comp_conflict_log_ctx {
	char *log_path;
};

void create_comp_conflict_log_dir(char *policy_id, uint64_t run_id,
    char **log_path_out, struct isi_error **error_out);

void
finalize_comp_conflict_log(char *log_dir_in, char **final_log_out,
    struct isi_error **error_out);

void init_comp_conflict_log(char *policy_id, uint64_t run_id, int lnn);

bool has_comp_conflict_log(void);

void log_comp_conflict(char *conflict_path, char *link_path,
    struct isi_error **error_out);

#endif
