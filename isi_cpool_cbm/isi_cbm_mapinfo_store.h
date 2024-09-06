
#pragma once

#include <isi_cpool_d_common/cpool_d_common.h>
#include "isi_cbm_mapentry.h"

struct tx_op {
	enum { INVALID, ADD, DEL, MOD } op;

	isi_cfm_mapentry entry;
};

/**
 * mapinfo callback interface
 */
class isi_cfm_mapinfo_store_cb
{
public:
	/**
	 * get the object id of the mapinfo
	 */
	virtual const isi_cloud_object_id &get_object_id() const = 0;

	/**
	 * get the overflow threshold
	 */
	virtual int get_overflow_threshold() const = 0;

	/**
	 * get the mapentry pack size
	 */
	virtual int get_mapentry_pack_size(bool pack_overflow) const = 0;

	/**
	 * get the type of the mapinfo
	 */
	virtual int get_type() const = 0;

	/**
	 * get the name of the store
	 */
	virtual std::string get_store_name() const = 0;
	/**
	 * get the suffix if any of the name of the store
	 */
	virtual std::string get_store_suffix() const = 0;
};

/**
 * interface for mapinfo entry persistent
 */
/////这个类主要负责将mapentry写入kernel
class isi_cfm_mapinfo_store
{
 public:
	enum { AT, AT_OR_BEFORE, AT_OR_AFTER };


	virtual ~isi_cfm_mapinfo_store() { };

	/**
	 * move the store if it has a new name
	 *
	 * @param error_out[out] return the error
	 */
	virtual void move(struct isi_error **error_out) = 0;

	/**
	 * open the store
	 *
	 * @param create_if_not_found[in] if to create if it is not existing
	 * @param overwrite_if_exist if to overwrite any existing store
	 * @param created[out]    return true when the store is created on open
	 * @param error_out[out]  return the error
	 */
	virtual void open(bool create_if_not_found, bool overwrite_if_exist,
		bool *created, struct isi_error **error_out) = 0;

	/**
	 * put the entry to the offset
	 *
	 * @param off[in]     the offset of the entry
	 * @param entry[in]   entry to be persistent
	 * @param error_out[out]  return the error
	 */
	virtual void put(off_t off, isi_cfm_mapentry &entry,
	    struct isi_error **error_out) = 0;

	/**
	 * bulk put entries to the store
	 *
	 * @param entries[in] entry array to be persistent
	 * @param entry[in]   num of entries
	 * @param error_out[out]  return the error
	 */
	virtual void put(isi_cfm_mapentry entries[], int cnt,
	    struct isi_error **error_out) = 0;

	/**
	 * get the entry at specified offset
	 *
	 * @param off[in/out] offset of the entry
	 * @param entry[out]  entry to hold the data
	 * @param opt[in]     AT - the entry is exactly at the offset
	 *                    AT_OR_BEFORE - the entry is at or before the offset
	 *                    AT_OR_AFTER  - the entry is at or after the offset
	 * @param error_out[out]  return the error
	 *
	 * @return            true on success, false on failure
	 */
	virtual bool get(off_t &off, isi_cfm_mapentry &entry, int opt,
	    struct isi_error **error_out) const = 0;

	/**
	 * get a couple of entries following the offset, for bulk loading
	 *
	 * @param off[in]      start offset of the entries
	 * @param entries[out] array entries to be return
	 * @param cnt[in]      size of the entry array
	 * @param error_out[out]  return the error
	 *
	 * @return             num of enteries returned
	 */
	virtual int get(off_t off, isi_cfm_mapentry entries[], int cnt,
	    struct isi_error **error_out) const = 0;

	/**
	 * remove the entry at offset, when offset is -1, remove all
	 * the entries
	 *
	 * @param off[in]     offset of the entry to be removed
	 * @param error_out[out]  return the error
	 */
	virtual void remove(off_t off, struct isi_error **error_out) = 0;

	/**
	 * remove the sbt file
	 *
	 * @param error_out[out]  return the error
	 */
	virtual void remove(struct isi_error **error_out) = 0;

	/**
	 * cleanup the entries and copy over the entries under the source
	 *
	 * @param src[in]     source store to be copied
	 * @param error_out[out]  return the error
	 *
	 * @return            num of coped entries
	 */
	virtual int copy_from(isi_cfm_mapinfo_store &src,
	    struct isi_error **error_out) = 0;

	/**
	 * compare the two stores
	 *
	 * @param to[in]      target store to be compared with
	 * @param error_out[out]  return the error
	 *
	 * @return            true to be identical, false to be different
	 */
	virtual bool equals(const isi_cfm_mapinfo_store &to,
	    struct isi_error **error_out) = 0;

	/**
	 * check if the store with the name exists
	 *
	 * @param error_out[out]  return the error
	 *
	 * @return            true if the store exists, false otherwise
	 */
	virtual bool exists(struct isi_error **error_out) = 0;

	/**
	 * commit a list of operations transactionally
	 *
	 * @param ops[in]        a list of operations
	 * @param nops[in]       number of operations in the list
	 * @param error_out[out] return the error
	 *
	 * @return           entry number change after the operations
	 */
	virtual int tx_commit(struct tx_op ops[], int nops,
	    struct isi_error **error_out) = 0;

	/**
	 * get path of the store
	 *
	 * @param str[out] path of the store
	 */
	virtual void get_store_path(std::string &str) = 0;
};
