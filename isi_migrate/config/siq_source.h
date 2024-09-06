#ifndef __SIQ_SOURCE_H__
#define __SIQ_SOURCE_H__

#include "isi_migrate/config/siq_policy.h"

#include "isi_migrate/config/migrate_adt.h"

/*
 *      _                                                                        _ 
 *  ___(_) __ _      ___  ___  _   _ _ __ ___ ___     _ __ ___  ___ ___  _ __ __| |
 * / __| |/ _` |    / __|/ _ \| | | | '__/ __/ _ \   | '__/ _ \/ __/ _ \| '__/ _` |
 * \__ \ | (_| |    \__ \ (_) | |_| | | | (_|  __/   | | |  __/ (_| (_) | | | (_| |
 * |___/_|\__, |____|___/\___/ \__,_|_|  \___\___|___|_|  \___|\___\___/|_|  \__,_|
 *           |_|_____|                          |_____|                            
 */


#if defined(__cplusplus)
extern "C" {
#endif

/*
 * The siq_source_record has two locking paradigms.
 *    - Using open/write/unlock allows for synchronous modification under an
 *      exclusive lock held in open and not explicitly released until unlock.
 *    - Using load/save releases any locks before leaving each call, allowing
 *      record-level optimistic locking to protect any writes.
 */

struct siq_source_record *siq_source_record_open(const char *,
    struct isi_error **);
void siq_source_record_write(struct siq_source_record *, struct isi_error **);
void siq_source_record_unlock(struct siq_source_record *);

struct siq_source_record *siq_source_record_load(const char *,
    struct isi_error **);
void siq_source_record_save(struct siq_source_record *, struct isi_error **);

void siq_source_record_free(struct siq_source_record *);

/*      _                                         
 *  ___(_) __ _      ___  ___  _   _ _ __ ___ ___ 
 * / __| |/ _` |    / __|/ _ \| | | | '__/ __/ _ \
 * \__ \ | (_| |    \__ \ (_) | |_| | | | (_|  __/
 * |___/_|\__, |____|___/\___/ \__,_|_|  \___\___|
 *           |_|_____|                            
 *                             _   _                 
 *   ___  _ __   ___ _ __ __ _| |_(_) ___  _ __  ___ 
 *  / _ \| '_ \ / _ \ '__/ _` | __| |/ _ \| '_ \/ __|
 * | (_) | |_) |  __/ | | (_| | |_| | (_) | | | \__ \
 *  \___/| .__/ \___|_|  \__,_|\__|_|\___/|_| |_|___/
 *       |_|                                         
 */

void siq_source_create(const char *, struct isi_error **);

bool siq_source_has_record(const char *);

void siq_source_delete(const char *, struct isi_error **);

bool siq_source_should_del_latest_snap(struct siq_source_record *,
    struct isi_error **);

bool siq_source_should_del_new_snap(struct siq_source_record *,
    struct isi_error **);

void siq_source_detach_latest_snap(struct siq_source_record *);

void siq_source_finish_job(struct siq_source_record *, struct isi_error **);

void siq_source_finish_assess(struct siq_source_record *, struct isi_error **);

void siq_source_force_full_sync(struct siq_source_record *,
    struct isi_error **);

void siq_source_set_new_snap(struct siq_source_record *, ifs_snapid_t);

void siq_source_set_manual_snap(struct siq_source_record *, ifs_snapid_t);

void siq_source_add_restore_new_snap(struct siq_source_record *,
    ifs_snapid_t);

void siq_source_clear_delete_snaps(struct siq_source_record *);

void siq_source_clear_reset_pending(struct siq_source_record *);

void siq_source_set_database_mirrored(struct siq_source_record *);

void siq_source_clear_database_mirrored(struct siq_source_record *);

void siq_source_set_target_guid(struct siq_source_record *, const char *);
void siq_source_set_pending_job(struct siq_source_record *,
    const enum siq_job_action);

void siq_source_set_failback_state(struct siq_source_record *,
    const enum siq_failback_state);

bool siq_source_set_inc_job_id(struct siq_source_record *);

void siq_source_set_policy_delete(struct siq_source_record *);

void siq_source_set_unrunnable(struct siq_source_record *);

void siq_source_set_runnable(struct siq_source_record *);

void siq_source_set_pol_stf_ready(struct siq_source_record *);

void siq_source_clear_pol_stf_ready(struct siq_source_record *);

void siq_source_set_sched_times(struct siq_source_record *,
    time_t, time_t);

void siq_source_set_merge_snaps(struct siq_source_record *record1,
    struct siq_source_record *record2, ifs_snapid_t new_snap);

void siq_source_take_ownership(struct siq_source_record *);

void siq_source_release_ownership(struct siq_source_record *);

void siq_source_set_node_conn_failed(struct siq_source_record *, int);

void siq_source_clear_node_conn_failed(struct siq_source_record *);

void siq_source_set_latest(struct siq_source_record *record, 
    ifs_snapid_t latest_snap);

void siq_source_set_restore_latest(struct siq_source_record *record, 
    ifs_snapid_t latest_snap);

void siq_source_clear_restore_latest(struct siq_source_record *);

void siq_source_clear_restore_new(struct siq_source_record *);

void siq_source_set_manual_failback_prep(struct siq_source_record *,
    const bool);

void siq_source_set_job_delay_start(struct siq_source_record *, time_t);

void siq_source_clear_job_delay_start(struct siq_source_record *);

void siq_source_set_last_known_good_time(struct siq_source_record *, time_t);

const char *siq_source_failback_state_to_text(enum siq_failback_state);

/*      _                                         
 *  ___(_) __ _      ___  ___  _   _ _ __ ___ ___ 
 * / __| |/ _` |    / __|/ _ \| | | | '__/ __/ _ \
 * \__ \ | (_| |    \__ \ (_) | |_| | | | (_|  __/
 * |___/_|\__, |____|___/\___/ \__,_|_|  \___\___|
 *           |_|_____|                            
 *   __ _  ___ ___ ___  ___ ___  ___  _ __ ___ 
 *  / _` |/ __/ __/ _ \/ __/ __|/ _ \| '__/ __|
 * | (_| | (_| (_|  __/\__ \__ \ (_) | |  \__ \
 *  \__,_|\___\___\___||___/___/\___/|_|  |___/
 */

void siq_source_get_new(struct siq_source_record *, ifs_snapid_t *);

bool siq_source_retain_new(struct siq_source_record *, const char *);

void siq_source_get_latest(struct siq_source_record *, ifs_snapid_t *);

bool siq_source_retain_latest(struct siq_source_record *);

void siq_source_get_manual(struct siq_source_record *, ifs_snapid_t *);

void siq_source_get_restore_new(struct siq_source_record *, ifs_snapid_t *);

void siq_source_get_restore_latest(struct siq_source_record *, ifs_snapid_t *);

void siq_source_get_delete(struct siq_source_record *, ifs_snapid_t **);

void siq_source_get_pending_job(struct siq_source_record *,
    enum siq_job_action *);

void siq_source_get_failback_state(struct siq_source_record *,
    enum siq_failback_state *);

void siq_source_get_target_guid(struct siq_source_record *, char *);

void siq_source_get_next_action(struct siq_source_record *,
    enum siq_policy_action *);

void siq_source_get_sched_times(struct siq_source_record *,
    time_t *, time_t *);

bool siq_source_is_policy_deleted(struct siq_source_record *);

bool siq_source_has_associated(struct siq_source_record *);

bool siq_source_has_owner(struct siq_source_record *);

bool siq_source_is_unrunnable(struct siq_source_record *);

bool siq_source_should_run(struct siq_source_record *);

bool siq_source_pol_stf_ready(struct siq_source_record *);

bool siq_source_get_node_conn_failed(struct siq_source_record *, int);

bool siq_source_get_reset_pending(struct siq_source_record *);

bool siq_source_manual_failback_prep(struct siq_source_record *);

bool siq_source_get_database_mirrored(struct siq_source_record *);

void siq_source_get_job_delay_start(struct siq_source_record *, time_t *);

void siq_source_get_used_snapids(struct snapid_set *, struct isi_error **);

void siq_source_get_last_known_good_time(struct siq_source_record *, time_t *);

void siq_source_clear_composite_job_ver(struct siq_source_record *);

void siq_source_get_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_set_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_get_prev_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_set_prev_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_clear_restore_composite_job_ver(struct siq_source_record *);

void siq_source_get_restore_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_set_restore_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_get_restore_prev_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

void siq_source_set_restore_prev_composite_job_ver(struct siq_source_record *,
    struct siq_job_version *);

/*  _____                        ____ _                         
 * | ____|_ __ _ __ ___  _ __   / ___| | __ _ ___ ___  ___  ___ 
 * |  _| | '__| '__/ _ \| '__| | |   | |/ _` / __/ __|/ _ \/ __|
 * | |___| |  | | | (_) | |    | |___| | (_| \__ \__ \  __/\__ \
 * |_____|_|  |_|  \___/|_|     \____|_|\__,_|___/___/\___||___/
 */

ISI_ERROR_CLASS_DECLARE(SIQ_SOURCE_PID_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(SIQ_SOURCE_STALE_ERROR_CLASS);
ISI_ERROR_CLASS_DECLARE(SIQ_SOURCE_DUPLICATE_ERROR_CLASS);

struct isi_error *_siq_source_stale_error_new(const char *function,
    const char *file, int line, const char *pid);
struct isi_error *_siq_source_duplicate_error_new(const char *function,
    const char *file, int line, const char *pid);

#define siq_source_stale_error_new(pid)					\
	_siq_source_stale_error_new(__FUNCTION__, __FILE__, __LINE__, pid)

#define siq_source_duplicate_error_new(pid)				\
	_siq_source_duplicate_error_new(__FUNCTION__, __FILE__, __LINE__, pid)

#ifdef __cplusplus
}
#endif

#endif /* __SIQ_SOURCE_H__ */
