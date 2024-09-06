#ifndef __ISI_CBM_UTIL__H__
#define __ISI_CBM_UTIL__H__

#include <unistd.h>
#include <fcntl.h>
#include <ifs/ifs_types.h>
struct isi_error;
struct isi_cfm_policy_provider;
struct isi_cbm_file;

enum isi_cpool_lock_state {
	ISI_CPOOL_LK_EXCLUSIVE = F_WRLCK,
	ISI_CPOOL_LK_SHARED = F_RDLCK,
	ISI_CPOOL_LK_UNLOCK = F_UNLCK
};

bool isi_cpool_lock_file(int, bool, off_t, size_t, bool);
bool isi_cpool_unlock_file(int, off_t, size_t);

/**
 * This utility copy a stub file from src to tgt
 */
void isi_cbm_copy_stub_file(const char *src, const char *tgt,
    isi_error **error_out);

struct fmt_conv_ctx
process_time_fmt(const struct timeval &process_time);

void cluster_permission_check(const char *cluster_id,
    const isi_cfm_policy_provider *ppi, bool ok_if_guid_being_removed,
    struct isi_error **error_out);

void cluster_permission_check(struct isi_cbm_file *file_obj,
    const isi_cfm_policy_provider *ppi, bool ok_if_guid_being_removed,
    struct isi_error **error_out);

bool linsnap_is_write_restricted(ifs_lin_t lin, ifs_snapid_t snapid);

#endif // __ISI_CBM_UTIL__H__
