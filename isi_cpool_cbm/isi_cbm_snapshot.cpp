#include "isi_cbm_snapshot.h"

#include <ifs/ifs_adt.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <isi_snapshot/snapshot_utils.h>
#include <isi_snapshot/snapshot.h>
__BEGIN_DECLS
#include <isi_util/isi_extattr.h>
__END_DECLS
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"

/**
 * Module handling snapshot interaction.
 */

bool
cbm_snap_used_by_other_snaps(struct isi_cbm_file *file,
    const isi_cfm_mapinfo *mapinfo,
    struct isi_error **error_out)
{
	ASSERT(file != NULL);
	ASSERT(mapinfo != NULL);

	struct isi_error *error = NULL;

	bool ret = cbm_snap_used_by_other_snaps(file->lin, file->snap_id,
	    mapinfo->get_account(), mapinfo->get_object_id(), &error);

	isi_error_handle(error, error_out);

	return ret;
}

bool
cbm_snap_used_by_other_snaps(ifs_lin_t lin, ifs_snapid_t snapid,
    const isi_cbm_id &account, const isi_cloud_object_id &object_id,
    struct isi_error **error_out)
{
	struct snapid_set SNAPID_SET_INIT_CLEAN(snaps);
	bool used = false;
	bool retry = false;
	bool estale_again = false;
	bool skip = false;
	struct isi_error *error = NULL;
	const ifs_snapid_t *p_snapid = 0;
	struct isi_cbm_file *snap = NULL;
	int flags = 0;

	// TBD-nice: to pass in the birth snapid for the CMO.
	// we are interested in the snaps >= than it
	get_snaps_for_lin(lin, 0, &snaps, &error);

	if (error) {
		isi_error_add_context(error,
		    "Failed to get_snaps_for_lin "
		    "for %{}",
		    lin_snapid_fmt(lin, snapid));
		goto out;
	}

	SNAPID_SET_FOREACH(p_snapid, &snaps) {
		ifs_snapid_t current_snapid = *p_snapid;
		isi_cfm_mapinfo snap_mapinfo;

		if (current_snapid == snapid)
			continue;

		estale_again = false;
		skip = false;

		do {
			retry = false;
			snap = isi_cbm_file_open(lin, current_snapid,
			    flags, &error);

			if (error && (isi_system_error_is_a(error, ENOENT) ||
			    isi_cbm_error_is_a(error, CBM_NOT_A_STUB) ||
			    isi_system_error_is_a(error, ESTALE))) {
				// it might have been deleted or not a stub
				// ignore.
				isi_error_free(error);
				error = NULL;
				skip = true;
				break;
			}

			if (error) {
				isi_error_add_context(error,
				    "Failed to isi_cbm_file_open for "
				    "for file %{}",
				    lin_snapid_fmt(lin, current_snapid));
				goto out;
			}

			// now load the mapping info and check if it is
			// referencing the same cloud object as this file version

			isi_cph_get_stubmap(snap->fd, snap_mapinfo, &error);

			if (error) {
				/*
				 * If the system error here is ESTALE,
				 * there might be a possibility the snap no
				 * longer exists. To confirm, we need to retry
				 * opening the file again. If at that point,
				 * we get an ENOENT, then it should be handled
				 * as such. Else, handle the ESTALE.
				 */
				if (isi_system_error_is_a(error, ESTALE) &&
				    (estale_again == false)) {
					isi_error_free(error);
					error = NULL;
					isi_cbm_file_close(snap,
					    isi_error_suppress());
					retry = true;
					estale_again = true;
				}
				else {
					isi_error_add_context(error,
					    "Failed to get stub mapinfo "
					    "for snap %{} for %{}",
					    lin_snapid_fmt(snap->lin, snap->snap_id),
					    lin_snapid_fmt(lin, snapid));
					isi_cbm_file_close(snap,
					    isi_error_suppress());
					goto out;
				}
			}
		} while (retry);

		if (skip == true) {
			continue;
		}

		if (snap_mapinfo.has_same_cmo(account, object_id)) {
			ilog(IL_DEBUG, "%{} is referencing (%s, %ld) for %{}",
			    lin_snapid_fmt(lin, current_snapid),
			    object_id.to_c_string(),
			    object_id.get_snapid(),
			    lin_snapid_fmt(lin, snapid));
			used = true;
			isi_cbm_file_close(snap,
			    isi_error_suppress(IL_NOTICE));
			break;
		}

		isi_cbm_file_close(snap, isi_error_suppress(IL_NOTICE));
	}

 out:
	isi_error_handle(error, error_out);
	return used;
}

bool cbm_snap_has_snaps(ifs_lin_t lin, int fd,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	int status = -1;
	bool has_snapshot = false;
	struct isi_extattr_blob eb = {NULL, 0, 0};
	ASSERT(fd != -1);

	status = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "live_snapids", &eb);

	if (status == -1) {
		error = isi_system_error_new(errno,
		    "getting live snapids Lin %{}",
		    lin_fmt(lin));
		goto out;
	}

	// If eb.used is more than zero there is a snapshot
	if (eb.used > 0)
		has_snapshot = true;
	else
		has_snapshot = false;
 out:
	 isi_error_handle(error, error_out);

	 if (eb.data)
		 free(eb.data);

	 return has_snapshot;
}

