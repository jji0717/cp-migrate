#ifndef __ISI_CPOOL_D_UT_HELPERS_H__
#define __ISI_CPOOL_D_UT_HELPERS_H__

#include <stdlib.h>

class task_key;

void
add_sbt_entry_ut(int sbt_fd, const task_key *key, const void *entry_buf,
    size_t entry_len);

void
get_sbt_entry_ut(int sbt_fd, const task_key *key, void *entry_buf,
    size_t entry_size, size_t *entry_size_out, bool ignore_ENOENT);

/**
 * An idempotent wrapper around UFAIL_POINT_INIT to initialize failpoints used
 * in unit tests.
 */
void
init_failpoints_ut(void);

/**
 * Enable or disable isi_cpool_d
 */
void
enable_cpool_d(bool enable);

#endif // __ISI_CPOOL_D_UT_HELPERS_H__

