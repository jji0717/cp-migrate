#include <isi_util/isi_error.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cbm_index_types.h"

#include "isi_cbm_csi.h"
#include "isi_cbm_error.h"

using namespace isi_cloud;

csi_entry::csi_entry()
	: time_based_index_entry<isi_cloud::lin_snapid>(CPOOL_CSI_VERSION)
{
}

csi_entry::csi_entry(uint32_t version)
	: time_based_index_entry<isi_cloud::lin_snapid>(version)
{
}

csi_entry::csi_entry(btree_key_t key, const void *serialized,
    size_t serialized_size, isi_error **error_out)
	: time_based_index_entry<isi_cloud::lin_snapid>(
	    CPOOL_CSI_VERSION, key, serialized, serialized_size, error_out)
{
    	if (error_out) {
	      if (isi_cbm_error_is_a(*error_out, CBM_VERSION_MISMATCH)) {
		      isi_error_add_context(*error_out, " CSI. ");
	      }
	}
}

csi_entry::~csi_entry()
{
}

const std::set<isi_cloud::lin_snapid> &
csi_entry::get_cached_files(void) const
{
	return get_items();
}

void
csi_entry::add_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	struct isi_error *error = NULL;

	isi_cloud::lin_snapid item(lin, snapid);
	add_item(item, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{} snapid: %{}", lin_fmt(lin), snapid_fmt(snapid));

 out:
	isi_error_handle(error, error_out);
}

void
csi_entry::remove_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	struct isi_error *error = NULL;

	isi_cloud::lin_snapid item(lin, snapid);
	remove_item(item, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{} snapid: %{}", lin_fmt(lin), snapid_fmt(snapid));

 out:
	isi_error_handle(error, error_out);
}

void
csi::add_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    const struct timeval &process_time, struct isi_error **error_out)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	struct isi_error *error = NULL;

	isi_cloud::lin_snapid item(lin, snapid);
	add_item(CPOOL_CSI_VERSION, item, process_time, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{} snapid: %{}", lin_fmt(lin), snapid_fmt(snapid));

 out:
	isi_error_handle(error, error_out);
}

void
csi::remove_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    const struct timeval &process_time, struct isi_error **error_out)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	struct isi_error *error = NULL;

	isi_cloud::lin_snapid item(lin, snapid);
	remove_item(item, process_time, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "lin: %{} snapid: %{}", lin_fmt(lin), snapid_fmt(snapid));

 out:
	isi_error_handle(error, error_out);
}
