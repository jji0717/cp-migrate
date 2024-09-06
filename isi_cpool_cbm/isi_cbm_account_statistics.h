#ifndef __ISI_CBM_ACCOUNT_STATISTICS_H__
#define __ISI_CBM_ACCOUNT_STATISTICS_H__

#include <unistd.h>
#include "io_helper/isi_cbm_ioh_base.h"

struct isi_cbm_file;
struct isi_error;
class isi_cbm_id;

/**
 * Get the account capacity usage from cloud.
 *
 * account [in]: Has the account information for which statistics is requested.
 * acct_stats_param [in]: Parameters to parse the response from cloud.
 * acct_stats [out]: The struct that has the statistics information.
 * error_out [out]: isi_error if failed.
 */

void
isi_cbm_get_account_statistics(const struct isi_cbm_account &account,
    isi_cloud::cl_account_statistics_param &acc_stats_param,
    isi_cloud::cl_account_statistics &acct_stats,
    struct isi_error **error_out);


#endif // __ISI_CBM_ACCOUNT_STATISTICS_H__
