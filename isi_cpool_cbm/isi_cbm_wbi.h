#ifndef __ISI_CBM_WBI_H__
#define __ISI_CBM_WBI_H__

#include <isi_cpool_cbm/isi_cbm_index_types.h>
#include <isi_cpool_d_common/cpool_d_common.h>

#include "isi_cbm_time_based_index_entry.h"
#include "isi_cbm_time_based_index.h"

namespace isi_cloud {

class wbi_entry : public time_based_index_entry<isi_cloud::lin_snapid>
{
protected:
	/* Copy and assignment are not allowed. */
	wbi_entry(const wbi_entry&);
	wbi_entry& operator=(const wbi_entry&);

public:
	wbi_entry();
	wbi_entry(uint32_t version);
	wbi_entry(btree_key_t key, const void *serialized,
	    size_t serialized_size, isi_error **error_out);
	~wbi_entry();

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

class wbi : public time_based_index<wbi_entry, isi_cloud::lin_snapid>
{
protected:
	const char *get_sbt_name(void) const
	{
		return get_wbi_sbt_name();
	}

	const char *get_lock_filename(void) const
	{
		return get_wbi_lock_filename();
	}

	/* Copy and assignment are not allowed. */
	wbi(const wbi&);
	wbi& operator=(const wbi&);

public:
	wbi() : time_based_index<wbi_entry, isi_cloud::lin_snapid>(
	    CPOOL_WBI_VERSION) { }
	~wbi() { }

	/**
	 * Add a cached file to the index.
	 *
	 * @param[in]  lin		the lin of the cached file to add
	 * @param[in]  snapid		the snap ID of the cached file to add
	 * @param[in]  process_time_tv	determines where in the index the
	 * cached file will be added
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void add_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    struct timeval process_time_tv, struct isi_error **error_out);

	/**
	 * Remove a cached file from the index.  This function removes a cached
	 * file for a specific process time; in other words, it does not
	 * perform an exhaustive search for the specified cached file.
	 *
	 * @param[in]  lin		the lin of the cached file to remove
	 * @param[in]  snapid		the snap ID of the cached file to
	 * remove
	 * @param[in]  process_time_tv	specifies where to look for the cached
	 * file to remove
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
	    struct timeval process_time_tv, struct isi_error **error_out);
	
};

} // end namespace isi_cloud

#endif // __ISI_CBM_WBI_H__
