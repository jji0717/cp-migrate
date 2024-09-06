#ifndef __ISI_CBM_TIME_BASED_INDEX_ENTRY_H__
#define __ISI_CBM_TIME_BASED_INDEX_ENTRY_H__

#include <ifs/ifs_types.h>

#include <errno.h>
#include <set>

#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cbm_pack_unpack.h"
#include "isi_cbm_util.h"
#include "isi_cbm_error.h"

#define CPOOL_SBT_SIZE			8149

template <class item_type>
class time_based_index_entry
{
protected:
	struct timeval			process_time_;
	ifs_devid_t			devid_;
	uint32_t			index_;

	uint32_t			version_;
	std::set<item_type>		items_;

	mutable bool			serialization_up_to_date_;
	mutable size_t			serialized_entry_size_;
	mutable void			*serialized_entry_;
	mutable size_t			saved_serialization_size_;
	mutable void			*saved_serialization_;

	void serialize(void) const;
	void deserialize(uint32_t expected_version, isi_error **error_out);

	/* Default/copy construction and assignment are not allowed. */
	time_based_index_entry();
	time_based_index_entry(const time_based_index_entry &);
	time_based_index_entry& operator=(const time_based_index_entry &);

public:
	time_based_index_entry(uint32_t version);
	time_based_index_entry(uint32_t expected_version, btree_key_t key,
	    const void *serialized, size_t serialized_size,
	    isi_error **error_out);
	virtual ~time_based_index_entry();

	struct timeval get_process_time(void) const;
	void set_process_time(const struct timeval &in);

	ifs_devid_t get_devid(void) const;
	void set_devid(ifs_devid_t in);

	uint32_t get_index(void) const;
	void set_index(uint32_t in);

	uint32_t get_version(void) const;
	// intentionally no set_version, see ctor

	const std::set<item_type> &get_items(void) const;

	static btree_key_t to_btree_key(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index);
	static void from_btree_key(btree_key_t key,
	    struct timeval &process_time, ifs_devid_t &devid, uint32_t &index);

	btree_key_t to_btree_key(void) const;
	void from_btree_key(btree_key_t key);

	void add_item(const item_type &item, struct isi_error **error_out);
	void remove_item(const item_type &item, struct isi_error **error_out);

	void set_serialized_entry(uint32_t expected_version,
	    const void *entry, size_t size, isi_error **error_out);
	void set_serialized_entry(uint32_t expected_version, btree_key_t key,
	    const void *entry, size_t size, isi_error **error_out);

	void get_serialized_entry(const void *&entry, size_t &size);

	void save_serialization(void) const;
	void get_saved_serialization(const void *&serialization,
	    size_t &size) const;
};

/*
 *
 * TEMPLATE IMPLEMENTATIONS
 *
 */
template<class item_type>
btree_key_t
time_based_index_entry<item_type>::to_btree_key(
    const struct timeval &process_time, ifs_devid_t devid, uint32_t index)
{
	btree_key_t key = {{ 0, 0 }};

	/*
	 *   0 -  19: process time (microseconds)
	 *  20 -  63: process time (seconds)
	 *  64 -  79: (unused)
	 *  80 -  95: devid
	 *  96 - 127: index
	 */

	key.keys[0] |= (process_time.tv_sec & 0xfffffffffff) << 20;
	key.keys[0] |= (process_time.tv_usec & 0xfffff);

	key.keys[1] = devid;
	key.keys[1] <<= 32;
	key.keys[1] |= index;

	return key;
}

template<class item_type>
void
time_based_index_entry<item_type>::from_btree_key(btree_key_t key,
    struct timeval &process_time, ifs_devid_t &devid, uint32_t &index)
{
	process_time.tv_sec = ((key.keys[0] & 0xfffffffffff00000) >> 20);
	process_time.tv_usec = (key.keys[0] & 0xfffff);
	devid = (key.keys[1] & 0xffff00000000) >> 32;
	index = (key.keys[1] & 0xffffffff);
}

template<class item_type>
time_based_index_entry<item_type>::time_based_index_entry(uint32_t version)
	: devid_(INVALID_DEVID), index_(0), version_(version)
	, serialization_up_to_date_(false), serialized_entry_size_(0)
	, serialized_entry_(NULL), saved_serialization_size_(0)
	, saved_serialization_(NULL)
{
	memset(&process_time_, 0, sizeof(process_time_));
}

template<class item_type>
time_based_index_entry<item_type>::time_based_index_entry(
    uint32_t expected_version, btree_key_t key, const void *serialized,
    size_t serialized_size, struct isi_error **error_out)
	: devid_(INVALID_DEVID), index_(0), version_(CPOOL_INVALID_VERSION)
	, serialization_up_to_date_(false), serialized_entry_size_(0)
	, serialized_entry_(NULL), saved_serialization_size_(0)
	, saved_serialization_(NULL)
{
	memset(&process_time_, 0, sizeof(process_time_));
	from_btree_key(key);
	set_serialized_entry(expected_version, serialized,
	    serialized_size, error_out);
}

template<class item_type>
time_based_index_entry<item_type>::~time_based_index_entry()
{
	free(serialized_entry_);
	free(saved_serialization_);
}

template<class item_type>
struct timeval
time_based_index_entry<item_type>::get_process_time(void) const
{
	return process_time_;
}

template<class item_type>
void
time_based_index_entry<item_type>::set_process_time(
    const struct timeval &process_time)
{
	process_time_.tv_sec = process_time.tv_sec;
	process_time_.tv_usec = process_time.tv_usec;

	/*
	 * Since process_time_ isn't serialized, no need to change
	 * serialization_up_to_date_ to false.
	 */
}

template<class item_type>
ifs_devid_t
time_based_index_entry<item_type>::get_devid(void) const
{
	return devid_;
}

template<class item_type>
void
time_based_index_entry<item_type>::set_devid(ifs_devid_t devid)
{
	devid_ = devid;

	/*
	 * Since devid_ isn't serialized, no need to change
	 * serialization_up_to_date_ to false.
	 */
}

template<class item_type>
uint32_t
time_based_index_entry<item_type>::get_index(void) const
{
	return index_;
}

template<class item_type>
void
time_based_index_entry<item_type>::set_index(uint32_t index)
{
	ASSERT(index != 0, "index 0 is a reserved value");

	index_ = index;

	/*
	 * Since index_ isn't serialized, no need to change
	 * serialization_up_to_date_ to false.
	 */
}

template<class item_type>
uint32_t
time_based_index_entry<item_type>::get_version(void) const
{
	return version_;
}

template<class item_type>
const std::set<item_type> &
time_based_index_entry<item_type>::get_items(void) const
{
	return items_;
}

template<class item_type>
btree_key_t
time_based_index_entry<item_type>::to_btree_key(void) const
{
	return to_btree_key(process_time_, devid_, index_);
}

template<class item_type>
void
time_based_index_entry<item_type>::from_btree_key(btree_key_t key)
{
	from_btree_key(key, process_time_, devid_, index_);
}

template<class item_type>
void
time_based_index_entry<item_type>::add_item(const item_type &item,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	const void *serialized_entry;
	size_t serialized_entry_size;

	if (items_.find(item) != items_.end()) {
		error = isi_system_error_new(EEXIST,
		    "item already exists in this entry");
		goto out;
	}

	get_serialized_entry(serialized_entry, serialized_entry_size);
	if (serialized_entry_size + item.get_packed_size() >=
	    CPOOL_SBT_SIZE) {
		error = isi_system_error_new(ENOSPC,
		    "not enough space to add item to this entry");
		goto out;
	}

	items_.insert(item);
	serialization_up_to_date_ = false;

 out:
	isi_error_handle(error, error_out);
}

template<class item_type>
void
time_based_index_entry<item_type>::remove_item(const item_type &item,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (items_.erase(item) == 0) {
		error = isi_system_error_new(ENOENT,
		    "item does not exist in this entry");
		goto out;
	}

	serialization_up_to_date_ = false;

 out:
	isi_error_handle(error, error_out);
}

template<class item_type>
void
time_based_index_entry<item_type>::set_serialized_entry(
    uint32_t expected_version, const void *entry, size_t size,
    isi_error **error_out)
{
	isi_error *error = NULL;
	ASSERT(entry != NULL);
	ASSERT(size > 0, "size: %zu", size);

	if (size > serialized_entry_size_) {
		free(serialized_entry_);
		serialized_entry_ = malloc(size);
		ASSERT(serialized_entry_ != NULL);
	}

	serialized_entry_size_ = size;
	memcpy(serialized_entry_, entry, serialized_entry_size_);

	deserialize(expected_version, &error);
	ON_ISI_ERROR_GOTO(out, error);

	serialization_up_to_date_ = true;
	
out:
	isi_error_handle(error, error_out);
}

template<class item_type>
void
time_based_index_entry<item_type>::set_serialized_entry(
    uint32_t expected_version, btree_key_t key, const void *entry, size_t size,
    isi_error **error_out)
{
	from_btree_key(key);
	set_serialized_entry(expected_version, entry, size, error_out);
}

template<class item_type>
void
time_based_index_entry<item_type>::get_serialized_entry(const void *&entry,
    size_t &size)
{
	if (!serialization_up_to_date_)
		serialize();

	entry = serialized_entry_;
	size = serialized_entry_size_;
}

template<class item_type>
void
time_based_index_entry<item_type>::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialized_entry_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialized_entry_,
	    serialized_entry_size_);
	saved_serialization_size_ = serialized_entry_size_;
}

template<class item_type>
void
time_based_index_entry<item_type>::get_saved_serialization(
    const void *&serialization, size_t &size) const
{
	serialization = saved_serialization_;
	size = saved_serialization_size_;
}

template<class item_type>
void
time_based_index_entry<item_type>::serialize(void) const
{
	free(serialized_entry_);

	typename std::set<item_type>::const_iterator iter;
	size_t num_items = items_.size();

	serialized_entry_size_ =
	    sizeof version_ +
	    sizeof num_items +
	    num_items * item_type::get_packed_size();

	serialized_entry_ = malloc(serialized_entry_size_);
	ASSERT(serialized_entry_ != NULL);

	/*
	 * Since pack() advances the destination pointer, use a copy to
	 * maintain the correct serialized_entry_ pointer.
	 */
	void *temp = serialized_entry_;

	pack(&temp, &version_, sizeof version_);
	pack(&temp, &num_items, sizeof num_items);

	iter = items_.begin();
	for (; iter != items_.end(); ++iter)
		iter->pack(&temp);

	serialization_up_to_date_ = true;
}

template<class item_type>
void
time_based_index_entry<item_type>::deserialize(uint32_t expected_version,
	isi_error **error_out)
{
	isi_error *error = NULL;
	size_t num_items;
	item_type temp_item;
	size_t all_items_packed_size = 0;
	size_t entry_size = 0;

	/*
	 * Clear the set of items since it doesn't get overwritten like the
	 * rest of the member data.
	 */
	items_.clear();

	/*
	 * Like pack(), unpack() advances the destination pointer, so use a
	 * copy to maintain the correct serialized_entry_ pointer.
	 */
	void *temp = serialized_entry_;

	unpack(&temp, &version_, sizeof version_);
	if (version_ != expected_version) {
		error = isi_cbm_error_new(CBM_VERSION_MISMATCH,
		    "Version mismatch. Expected version %x, Found version %x",
		    expected_version, version_);
		goto out;
	}

	unpack(&temp, &num_items, sizeof num_items);

	for (size_t i = 0; i < num_items; ++i) {
		temp_item.unpack(&temp);
		all_items_packed_size += temp_item.get_packed_size();
		items_.insert(temp_item);
	}

	/*
	 * Verify that we have a coherent entry by checking the size of the
	 * supplied buffer and the size of this entry.
	 */
	entry_size =
	    sizeof version_ +
	    sizeof num_items +
	    all_items_packed_size;

	ASSERT(serialized_entry_size_ == entry_size,
	    "size mismatch (expected: %zu actual: %zu)",
	    serialized_entry_size_, entry_size);
	
out:
	isi_error_handle(error, error_out);
}

#endif // __ISI_CBM_TIME_BASED_INDEX_ENTRY_H__
