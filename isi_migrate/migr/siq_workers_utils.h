#ifndef SIQ_WORKERS_UTIL_H_
#define SIQ_WORKERS_UTIL_H_

#include <stdbool.h>

extern void
ioerror(int, char *dir, char *file, enc_t enc, u_int64_t objectlin,
    char *fmt, ...) __attribute__ ((format (__printf__, 6, 7)));

void init_statistics(struct siq_stat **stat);

/* sent from pworker to coord for treewalk based syncs */
void send_pworker_tw_stat(int fd, struct siq_stat *tw_cur_stats,
    struct siq_stat *tw_last_stats, uint64_t dirlin, uint64_t filelin,
    uint32_t ver);

/* sent from sworker to coord (via pworker) for treewalk based syncs */
void send_sworker_tw_stat(int fd, struct siq_stat *tw_cur_stats,
    struct siq_stat *tw_last_stats, uint32_t ver);

/* sent from pworker to coord for STF based update syncs */
void send_pworker_stf_stat(int fd, struct siq_stat *stf_cur_stats,
    struct siq_stat *stf_last_stats, uint32_t ver);

/* sent from pworker to coord (via pworker) for STF based update syncs */
void send_sworker_stf_stat(int fd, struct siq_stat *stf_cur_stats,
    struct siq_stat *stf_last_stats, uint32_t ver);

void cluster_name_from_node_name(char *host_name);

#define DONT_RUN_FILE "/tmp/siqdontrun"
bool check_for_dont_run(bool do_sleep);

void
send_comp_source_error(int fd, char *error_msg, enum siqerrors siq_error,
    struct isi_error *error);

void
send_comp_target_error(int fd, char *error_msg, enum siqerrors siq_error,
    struct isi_error *error);

void
send_compliance_map_entries(int fd, uint32_t num_entries,
    struct lmap_update *entries, ifs_lin_t chkpt_lin);

#define DS_MAX_CACHE_SIZE 20

#define NHL_FLAG "NHL"

#endif /* SIQ_WORKERS_UTIL_H_ */
