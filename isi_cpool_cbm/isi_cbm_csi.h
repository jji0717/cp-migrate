#ifndef __ISI_CBM_CSI_H__
#define __ISI_CBM_CSI_H__

#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_d_common/cpool_d_common.h>

#include "isi_cbm_time_based_index_entry.h"
#include "isi_cbm_time_based_index.h"

namespace isi_cloud {

class csi_entry : public time_based_index_entry<isi_cloud::lin_snapid>
{
protected:
	/* Copy and assignment are not allowed. */
	csi_entry(const csi_entry&);
	csi_entry& operator=(const csi_entry&);

public:
	csi_entry();
	csi_entry(uint32_t version);
	csi_entry(btree_key_t key, const void *serialized,
	    size_t serialized_size, isi_error **error_out);
	~csi_entry();

	const std::set<isi_cloud::lin_snapid> &get_cached_files(void) const;

	/**
	 * Add a cached file to this entry.
	 *
	 * @param[in]  lin		lin of the cached file to add
	 * @param[in]  snapid		snapid of the cached file to add
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void add_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    struct isi_error **error_out);

	/**
	 * Remove a cached file from this entry.
	 *
	 * @param[in]  lin		lin of the cached file to remove
	 * @param[in]  snapid		snapid of the cached file to remove
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    struct isi_error **error_out);
};

class csi : public time_based_index<csi_entry, isi_cloud::lin_snapid>
{
protected:
	const char *get_sbt_name(void) const
	{
		return get_csi_sbt_name();
	}

	const char *get_lock_filename(void) const
	{
		return get_csi_lock_filename();
	}

	/* Copy and assignment are not allowed. */
	csi(const csi&);
	csi& operator=(const csi&);

public:
	csi() : time_based_index<csi_entry, isi_cloud::lin_snapid>(
	    CPOOL_CSI_VERSION) { }
	~csi() { }

	/**
	 * Add a cached file to the index.
	 *
	 * @param[in]  lin		the lin of the cached file to add
	 * @param[in]  snapid		the snap ID of the cached file to add
	 * @param[in]  process_time	determines where in the index the
	 * cached file will be added
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void add_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    const struct timeval &process_time, struct isi_error **error_out);

	/**
	 * Remove a cached file from the index.  This function removes a cached
	 * file for a specific process time; in other words, it does not
	 * perform an exhaustive search for the specified cached file.
	 *
	 * @param[in]  lin		the lin of the cached file to remove
	 * @param[in]  snapid		the snap ID of the cached file to
	 * remove
	 * @param[in]  process_time	specifies where to look for the cached
	 * file to remove
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    const struct timeval &process_time, struct isi_error **error_out);
	
};

} // end namespace isi_cloud

#endif // __ISI_CBM_CSI_H__
