#ifndef __ISI_CBM_ACCESS__H__
#define __ISI_CBM_ACCESS__H__

/**
 * Helper routine to enable stub access for files.
 */
void isi_cbm_enable_stub_access(struct isi_error **error_out);
void isi_cbm_disable_stub_access(struct isi_error **error_out);
bool isi_cbm_is_stub_access_enabled(struct isi_error **error_out);
void isi_cbm_disable_atime_updates(struct isi_error **error_out);
void isi_cbm_enable_atime_updates(struct isi_error **error_out);

#endif /* __ISI_CBM_INVALIDATE__H__*/
