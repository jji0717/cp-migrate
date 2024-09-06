#ifndef __ISI_CPOOL_D_CPP_API_H__
#define __ISI_CPOOL_D_CPP_API_H__

#ifndef __cplusplus
#error "This header can only be included by C++ source/header files.  Try "\
    "including cpool_api_c.h instead."
#endif

#include <vector>

#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/operation_type.h>
#include <isi_cpool_d_common/resume_key.h>

#include "cpool_api_c.h"

struct job_member;
class scoped_object_list;

/**
 * Writeback the dirty cache of a file
 * @param[in]	lin		the lin of the file to be written back
 * @param[out]	error_out	holds information on any encountered errors
 */
void writeback_file(ifs_lin_t lin, struct isi_error **error_out);

/**
 * Writeback the dirty cache of a file
 * @param[in]	file_name	the name of the file to be written back
 * @param[out]	error_out	holds information on any encountered errors
 */
void writeback_file(const char *file_name, struct isi_error **error_out);

/**
 * Respond to a stubbed file delete notification from the kernel.
 * @param[in]  lin		the lin of the deleted file
 * @param[in]  snap_id		the snapshot ID of the deleted file
 * @param[in]  object_id	the object id that identifies the set of
 * backing cloud objects
 * @param[in]  date_of_death	the time before which the backing cloud
 * objects must not be deleted
 * @param[out] error_out	any error encountered during the operation
 */
void
stub_file_delete(ifs_lin_t lin, ifs_snapid_t snap_id,
    const isi_cloud_object_id &object_id, const struct timeval &date_of_death,
    struct isi_error **error_out);

/**
 * Delete a cloud object, including all referenced objects.
 * @param[in]  object_id	the ID of the cloud objects to be deleted
 * @param[out] error_out	holds information on any encountered error
 */
void delete_cloud_object(const isi_cloud_object_id &object_id,
    struct isi_error **error_out);

/**
 * Helper function for deleting job members returned by query_job_for_members.
 * @param[in]  member		the member to delete
 */
void delete_djob_member(struct job_member *&member);

/**
 * Query a daemon job for its members.
 * @param[in]  daemon_job_id	identifies the daemon job for which members
 * will be queried
 * @param[in]  start		determines the starting entry of members to return;
 * valid only when start_key is empty.
 * @param[in/out] start_key	starting key used for next database search; if
 * empty, then parameter start is used to find the starting entry. A new
 * start_key is constructed upon returning of the this function.
 * @param[in]  max		determines max number of members to return
 * @param[out] members		a collection of members, each of which must be
 * destroyed using delete_djob_member by the caller
 * @param[out] op_type		a reference to the operation type used by the
 * given job id
 * @param[out] error		any error encountered during the operation
 */
void query_job_for_members(djob_id_t daemon_job_id, unsigned int start,
    resume_key &start_key,
    unsigned int max, std::vector<struct job_member *> &members,
    const cpool_operation_type *&op_type, struct isi_error **error_out);

#endif // __ISI_CPOOL_D_CPP_API_H__
