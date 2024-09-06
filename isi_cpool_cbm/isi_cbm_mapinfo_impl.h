
#pragma once

#include <vector>
#include <boost/noncopyable.hpp>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include "isi_cbm_mapinfo_factory.h"
#include "isi_cbm_mapentry.h"
#include "isi_cbm_mapentry_iterator.h"

class isi_cfm_mapinfo;

class isi_cfm_mapinfo_impl :
    public isi_cfm_mapinfo_store_cb, boost::noncopyable
{
 public:
	typedef isi_cfm_mapentry_iterator iterator;
	typedef isi_cfm_mapinfo_store store_t;
	typedef std::map<off_t, isi_cfm_mapentry> memory_t;

	/**
	 * default constructor, reference the type and count from the
	 * mapinfo
	 */
	isi_cfm_mapinfo_impl(isi_cfm_mapinfo *mapinfo);

	/**
	 * open the store, set created to be true if it is a new store
	 */
	void open_store(bool create_if_not_exist, bool overwrite_if_exist,
	    bool *created, struct isi_error **error_out);

	/**
	 * move the store if it has a new store name
	 */
	void move_store_on_name_change(struct isi_error **error_out);

	/**
	 * get the iterator that starts at the first entry
	 */
	iterator begin(struct isi_error **error_out);

	/**
	 * get the iterator that starts at the first entry
	 */
	iterator begin(struct isi_error **error_out) const;

	/**
	 * get the iterator that points to the end of the entries
	 */
	iterator end();

	/**
	 * get the iterator that points to the nend of the entries
	 */
	iterator end() const;

	/**
	 * get the iterator that points to the last entry
	 */
	iterator last(struct isi_error **error_out);

	/**
	 * get the iterator that points to the last entry
	 */
	iterator last(struct isi_error **error_out) const;

	/**
	 * find the iterator that points to the entry with offset
	 * return end() offset doesn't exist
	 */
	iterator find(off_t offset, struct isi_error **error_out);

	/**
	 * find the iterator that points to the entry with offset
	 * return end() in case offset doesn't exist
	 */
	iterator find(off_t offset, struct isi_error **error_out) const;

	/**
	 * returns the iterator pointing to the entry whose key goes
	 * either equivalent or greater than offset
	 */
	iterator lower_bound(off_t offset, struct isi_error **error_out);

	/**
	 * returns the iterator pointing to the entry whose key goes
	 * either equivalent or goes after offset
	 */
	iterator lower_bound(off_t offset, struct isi_error **error_out) const;

	/**
	 * returns the iterator pointing to the entry whose key goes
	 * after offset
	 */
	iterator upper_bound(off_t offset, struct isi_error **error_out);

	/**
	 * returns the iterator pointing to the entry whose key goes
	 * after offset
	 */
	iterator upper_bound(off_t offset, struct isi_error **error_out) const;

	/**
	 * erases the entry pointed by the iterator, returns true on success
	 */
	void erase(iterator &it, struct isi_error **error_out);

	/**
	 * erase the entry pointed by the offset, returns true on success
	 */
	void erase(off_t offset, struct isi_error **error_out);

	/**
	 * queue the erase operation for later commit
	 *
	 * @param offset[in] offset to be queued
	 */
	void tx_erase(off_t offset)
	{
		struct tx_op op = { .op = tx_op::DEL };

		op.entry.set_offset(offset);

		tx_ops.push_back(op);
	}

	/**
	 * removes store if any
	 */
	void remove(struct isi_error **error_out);

	/**
	 * put the entry to store
	 *
	 * @param entry[in]      entry to be put
	 * @param error_out[out] error to return
	 */
	void put(isi_cfm_mapentry &entry, struct isi_error **error_out);

	/**
	 * queue the put operation for later commit
	 *
	 * @param entry[in] entry to be queued
	 */
	void tx_put(isi_cfm_mapentry &entry)
	{
		struct tx_op op = { .op = tx_op::ADD, .entry = entry };

		tx_ops.push_back(op);
	}

	/**
	 * copies the entries from the specified source, all existing entries
	 * are removed
	 *
	 * @param src[in]        source of the copy
	 * @param error_out[out] error to return
	 *
	 * @return               num of entries copyed
	 */
	int copy_from(const isi_cfm_mapinfo_impl &src,
	    struct isi_error **error_out);

	/**
	 * compare the entries, return true if they are identical
	 *
	 * @param to[in]         target to be compared with
	 * @param error_out[out] error to return
	 *
	 * @return               true on identical, false on difference
	 */
	bool equals(const isi_cfm_mapinfo_impl &to,
	    struct isi_error **error_out) const;

	/**
	 * is the entries in memory
	 *
	 * @return               true if entries are kept in memory
	 */
	inline bool is_in_memory() const
	{
		return store_.get() == NULL;
	}

	/**
	 * commit the queued operations transactionally
	 *
	 * @param error_out[out] error to return
	 *
	 * @return		 true on sucess, false on failure
	 */
	bool tx_commit(struct isi_error **error_out);

	/**
	 * get the path of the store
	 *
	 * @param str[out]	path of the store
	 */
	inline void get_store_path(std::string &str) const
	{
		if (store_.get())
			store_->get_store_path(str);
		else
			str = "";
	}

	/**
	 * get the store object
	 */
	inline isi_cfm_mapinfo_store *get_store()
	{
		return store_.get();
	}

	/**
	 * get the object id of the mapinfo
	 */
	virtual const isi_cloud_object_id &get_object_id() const;

	/**
	 * get the overflow threshold
	 * @return   number of entries threshold to overflow to external store
	 */
	virtual int get_overflow_threshold() const;

	/**
	 * get the mapentry pack size
	 */
	virtual int get_mapentry_pack_size(bool pack_overflow) const;

	/**
	 * get the type of the mapinfo
	 */
	virtual int get_type() const;

	/**
	 * get the name of the store
	 */
	virtual std::string get_store_name() const;

	virtual std::string get_store_suffix() const;

 private:
	void perform_overflow(struct isi_error **error_out);

	std::auto_ptr<isi_cfm_mapinfo_store_factory> factory_;

	// lazy intialized when threshold is exceeded
	std::auto_ptr<isi_cfm_mapinfo_store> store_;

	// container to hold entries before threshould exceeded
	std::map<off_t, isi_cfm_mapentry> in_memory_;

	std::vector<struct tx_op> tx_ops;

	isi_cfm_mapinfo *mapinfo_;
};

