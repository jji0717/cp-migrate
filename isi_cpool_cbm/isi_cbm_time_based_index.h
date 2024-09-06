#ifndef __ISI_CBM_TIME_BASED_INDEX_H__
#define __ISI_CBM_TIME_BASED_INDEX_H__

#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>

#include <errno.h>

#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/ifs_cpool_flock.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>

#include "isi_cbm_time_based_index_entry.h"

template <class entry_type, class item_type>
class time_based_index
{
protected:
	bool				initialized_;
	ifs_devid_t			devid_;
	int				sbt_fd_;
	int				lock_fd_;
	uint32_t			release_version_;

	virtual const char *get_sbt_name(void) const = 0;
	virtual const char *get_lock_filename(void) const = 0;

	/**
	 * Calculate a "lock hash" - i.e. the value used to distinguish
	 * entities when locking entries or entry groups.  Note that an entry
	 * group is identified by an index value of 0.
	 *
	 * @param[in]  process_time	\
	 * @param[in]  devid		| identify the entity to lock
	 * @param[in]  index		/
	 *
	 * @return     the "lock hash"
	 */
	uint64_t get_lock_identifier(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index) const;

	void _lock_entry(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index, bool block, bool exclusive,
	    struct isi_error **error_out) const;
	void _unlock_entry(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index,
	    struct isi_error **error_out) const;

	void _lock_entry_group(const struct timeval &process_time,
	    ifs_devid_t devid, bool block, bool exclusive,
	    struct isi_error **error_out) const;
	void _unlock_entry_group(const struct timeval &process_time,
	    ifs_devid_t devid, struct isi_error **error_out) const;

	/**
	 * Retrieve an entry given a specified process time, devid, and index.
	 * If entry is successfully retrieved it must be deleted by the caller.
	 *
	 * @param[in]  process_time     \
	 * @param[in]  devid		    | define the key of the desired entry
	 * @param[in]  index		    /
	 * @param[out] error_out	any error encountered during the
	 * operation
	 *
	 * @return			an entry_type entry which must be
	 * deleted by the caller or an error (but never both)
	 */
	entry_type *get_entry(const struct timeval &process_time,
	    ifs_devid_t devid, uint32_t index, struct isi_error **error_out);

	/**
	 * Retrieve an entry given its btree key.
	 *
	 * @param[in]  key		the entry's key
	 * @param[out] error_out	any error encountered during the
	 * operation
	 *
	 * @return			an entry_type entry which must be
	 * deleted by the caller or an error (but never both)
	 */
	entry_type *get_entry(const btree_key_t &key,
	    struct isi_error **error_out);

	/**
	 * Remove an entry from the index.  The entry's group must be locked
	 * prior to calling this function.
	 *
	 * @param[in]  ent		the entry to remove
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_entry(entry_type *ent, struct isi_error **error_out);

	/**
	 * Attempts to remove ent_to_remove and add ent_to_add to the index in
	 * a single bulk operation.  This guarantees that exactly one of the
	 * two entries exists in the index both before and after the
	 * transaction.
	 *
	 * @param[in] ent_to_remove	the entry to be removed from the index;
	 * must not be NULL
	 * @param[in] ent_to_add	the entry to be added to the index;
	 * must not be NULL
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void
	bulk_remove_and_add_entries(entry_type *ent_to_remove,
	    entry_type *ent_to_add, struct isi_error **error_out);


public:
	time_based_index(uint32_t version);
	virtual ~time_based_index();

	/**
	 * Initialize the index object.  Must be performed before any operation
	 * on the object.
	 *
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void initialize(struct isi_error **error_out);

	/**
	 * Get the current version number of the time based index.
	 * It could be the ooi, wbi, csi versions depending on the 
	 * type of the time based index.
	 * @param[out] release_version_ Version of ooi, wbi, csi based 
	 * on the time based index type.
	 */
	uint32_t get_release_version(void);
	
	/**
	 * Set the current version number of the time based index.
	 * @param[in] in_release_version.
	 */
	 void set_release_version(uint32_t in_release_version);

	/**
	 * Lock/unlock an entry group for adding/removing items or entries.
	 */
	void lock_entry_group(const struct timeval &process_time,
	    struct isi_error **error_out) const;
	void unlock_entry_group(const struct timeval &process_time,
	    struct isi_error **error_out) const;

	/**
	 * Lock/unlock an entry group for the specified devid
	 * for the purpose of adding/removing items to it.
	 */
	void lock_entry_group(const struct timeval &process_time,
	    ifs_devid_t devid, struct isi_error **error_out) const;
	void unlock_entry_group(const struct timeval &process_time,
	    ifs_devid_t devid, struct isi_error **error_out) const;

	/**
	 * Lock/unlock an entry for reading items.
	 */
	void lock_entry(const entry_type *entry,
	    struct isi_error **error_out) const;
	void unlock_entry(const entry_type *entry,
	    struct isi_error **error_out) const;

	/**
	 * Add an item to the index at the specified process time.
	 *
	 * @param[in]  item_version    the version # of the item.
	 * @param[in]  item		       the item to add
	 * @param[in]  process_time	   determines where in the index the item
	 * will be added
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void add_item(uint32_t item_version, const item_type &item,
	    const struct timeval &process_time, struct isi_error **error_out);

	/**
	 * Remove an item from the index at the specified process time.
	 *
	 * @param[in]  item		    the item to remove
	 * @param[in]  process_time	determines from where in the index the
	 * item will be removed; note that an exhaustive search is NOT
	 * performed
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_item(const item_type &item, const struct timeval &process_time,
	    struct isi_error **error_out);

	/**
	 * If an unlocked entry with process time less than or equal to
	 * upper_limit exists, lock it and return the entry.  The entry must be
	 * unlocked once processing is finished.
	 *
	 * @param[in]  upper_limit	no entry will be returned if its
	 * process time exceeds this value
	 * @param[in]  more_eligible_entries	false if the function
	 * determines that no entry with a process time less than or equal to
	 * upper_limit exists in the index, else true
	 * @param[out]  isi_error	any error encountered during the
	 * operation
	 *
	 * @return			if it exists, a locked entry with
	 * process time less than or equal to upper_limit which must be
	 * unlocked and deleted once processed, else NULL
	 */
	entry_type *get_entry_before_or_at(time_t upper_limit,
	    bool &more_eligible_entries, struct isi_error **error_out);

	/**
	 * Removes original_entry.  If failed_objs_entry is non-empty, it is
	 * added to the index in a bulk op with the removal of original_entry.
	 * The original entry's group must be locked, but the failed objects
	 * entry's group must NOT be locked.
	 *
	 * @param[in] original_entry	the entry to be removed
	 * @param[in] failed_objs_entry	a new entry holding individual items
	 * which need to be added back to index; if empty it is not added
	 * @param[out] error_out	any error encountered during the
	 * operation
	 */
	void remove_entry_and_handle_failures(entry_type *original_entry,
	    entry_type *failed_objs_entry, struct isi_error **error_out);
};

/*
 *
 * TEMPLATE IMPLEMENTATIONS
 *
 */

template <class entry_type, class item_type>
time_based_index<entry_type, item_type>::time_based_index(uint32_t version)
	: initialized_(false), devid_(INVALID_DEVID), sbt_fd_(-1), lock_fd_(-1)
	, release_version_(version)
{
}

template <class entry_type, class item_type>
time_based_index<entry_type, item_type>::~time_based_index()
{
	if (sbt_fd_ != -1)
		close_sbt(sbt_fd_);

	if (lock_fd_ != -1)
		close_sbt(lock_fd_);
}

template <class entry_type, class item_type>
uint64_t
time_based_index<entry_type, item_type>::get_lock_identifier(
    const struct timeval &process_time, ifs_devid_t devid,
    uint32_t index) const
{
	ASSERT(devid != INVALID_DEVID);

	uint64_t identifier = 0;

	identifier |= (process_time.tv_sec & 0xffffffff) << 32;
	identifier |= (devid & 0xffff) << 16;
	identifier |= (index & 0xffff);

	return identifier;
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::_lock_entry(
    const struct timeval &process_time, ifs_devid_t devid, uint32_t index,
    bool block, bool exclusive, struct isi_error **error_out) const
{
	ASSERT(devid != INVALID_DEVID);

	struct isi_error *error = NULL;

	uint64_t identifier = get_lock_identifier(process_time, devid, index);
	struct flock params;

	populate_flock_params(identifier, &params);
	if (!exclusive)
		params.l_type = CPOOL_LK_SR;

	if (ifs_cpool_flock(lock_fd_, LOCK_DOMAIN_CPOOL_JOBS,
	    F_SETLK, &params, block ? F_SETLKW : 0) != 0) {
		error = isi_system_error_new(errno,
		    "failed to lock entry "
		    "(lock_fd_: %d process_time: %{} devid: %u index: %u "
		    "block? %s exclusive? %s)",
		    lock_fd_, process_time_fmt(process_time), devid, index,
		    block ? "yes" : "no", exclusive ? "yes" : "no");
	}

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::_unlock_entry(
    const struct timeval &process_time, ifs_devid_t devid, uint32_t index,
    struct isi_error **error_out) const
{
	ASSERT(devid != INVALID_DEVID);

	struct isi_error *error = NULL;

	uint64_t identifier = get_lock_identifier(process_time, devid, index);
	struct flock params;

	populate_flock_params(identifier, &params);

	if (ifs_cpool_flock(lock_fd_, LOCK_DOMAIN_CPOOL_JOBS, F_UNLCK, &params,
	    0) != 0) {
		error = isi_system_error_new(errno,
		    "failed to unlock entry "
		    "(lock_fd_: %d process_time: %{} devid: %u index: %u)",
		    lock_fd_, process_time_fmt(process_time), devid, index);
	}

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::_lock_entry_group(
    const struct timeval &process_time, ifs_devid_t devid, bool block,
    bool exclusive, struct isi_error **error_out) const
{
	ASSERT(devid != INVALID_DEVID);

	struct isi_error *error = NULL;

	/*
	 * Index values start at 1 and using an index of 0 ensures proper lock
	 * ordering since the entry group is locked before the entry.
	 */
	_lock_entry(process_time, devid, 0, block, exclusive, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::_unlock_entry_group(
    const struct timeval &process_time, ifs_devid_t devid,
	struct isi_error **error_out) const
{
	ASSERT(devid != INVALID_DEVID);

	struct isi_error *error = NULL;

	_unlock_entry(process_time, devid, 0, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
entry_type *
time_based_index<entry_type, item_type>::get_entry(   //////key通过process_time, devid获得
    const struct timeval &process_time, ifs_devid_t devid, uint32_t index,
    struct isi_error **error_out)
{
	ASSERT(devid != INVALID_DEVID);
	ASSERT(index != 0, "index 0 is a reserved value");
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	btree_key_t key = entry_type::to_btree_key(process_time, devid, index);
	entry_type *entry = get_entry(key, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "(process_time: %{} devid: %u index: %u)",
	    process_time_fmt(process_time), devid, index);

 out:
	/* Ensure we're returning an entry or an error, but not both. */
	ASSERT((entry == NULL) != (error == NULL),
	    "failed post-condition (%snull entry, %snull error)",
	    entry != NULL ? "non-" : "", error != NULL ? "non-" : "");

	isi_error_handle(error, error_out);

	return entry;
}

template <class entry_type, class item_type>
entry_type *
time_based_index<entry_type, item_type>::get_entry(const btree_key_t &key,
    struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	entry_type *entry = NULL;
	char entry_buf[8192];
	size_t entry_size_out;

	if (ifs_sbt_get_entry_at(sbt_fd_, const_cast<btree_key_t *>(&key),
	    entry_buf, sizeof entry_buf, NULL, &entry_size_out) != 0) {
		error = isi_system_error_new(errno,
		    "failed to get OOI entry (key: 0x%016lx/0x%016lx)",
		    key.keys[0], key.keys[1]);
		goto out;
	}

	entry = new entry_type(key, entry_buf, entry_size_out, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ASSERT(entry != NULL);

 out:
	/* Ensure we're returning an entry or an error, but not both. */
	ASSERT((entry == NULL) != (error == NULL),
	    "failed post-condition (%snull entry, %snull error)",
	    entry != NULL ? "non-" : "", error != NULL ? "non-" : "");

	isi_error_handle(error, error_out);

	return entry;
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::remove_entry(entry_type *ent,
    struct isi_error **error_out)
{
	ASSERT(ent != NULL);
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	btree_key_t key = ent->to_btree_key();

	if (ifs_sbt_remove_entry(sbt_fd_, &key) != 0)
		error = isi_system_error_new(errno,
		    "failed to remove entry from index "
		    "(process_time: %{} devid: %u index: %u)",
		    process_time_fmt(ent->get_process_time()),
		    ent->get_devid(), ent->get_index());

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::bulk_remove_and_add_entries(
    entry_type *ent_to_remove, entry_type *ent_to_add,
    struct isi_error **error_out)
{
	ASSERT(ent_to_add != NULL);
	ASSERT(ent_to_remove != NULL);
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	bool dst_entry_group_locked = false;
	struct sbt_bulk_entry bulk_op[2];
	const void *rem_ent_serialization, *add_ent_serialization;
	size_t rem_ent_serialized_size, add_ent_serialized_size;
	struct timeval process_time = ent_to_add->get_process_time();

	/*
	 * As multiple threads (on this node) can scan the index, lock the
	 * destination entry group.  (The source entry group should already be
	 * locked.)
	 */
	lock_entry_group(process_time, &error);
	ON_ISI_ERROR_GOTO(out, error);
	dst_entry_group_locked = true;

	/* Get an available index for the new entry. */
	for (uint32_t index = 1; true; ++index) {
		entry_type *entry = get_entry(process_time, devid_, index,
		    &error);

		/* (No interest in the entry itself.) */
		if (entry != NULL)
			delete entry;

		/* No error means existing entry; try another index value. */
		if (error == NULL)
			continue;

		/* Only expecting ENOENT here, otherwise bail. */
		if (!isi_system_error_is_a(error, ENOENT)) {
			isi_error_add_context(error,
			    "failed to get entry "
			    "(process_time: %{} devid: %u index: %u)",
			    process_time_fmt(process_time), devid_, index);
			goto out;
		}

		ASSERT(isi_system_error_is_a(error, ENOENT),
		    "unexpected non-ENOENT error: %#{}", isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;

		/* ENOENT means this index is available. */
		ent_to_add->set_index(index);
		ent_to_add->set_devid(devid_);

		break;
	}

	memset(&bulk_op, 0, sizeof bulk_op);

	ent_to_remove->get_serialized_entry(rem_ent_serialization,
	    rem_ent_serialized_size);
	ent_to_add->get_serialized_entry(add_ent_serialization,
	    add_ent_serialized_size);

	bulk_op[0].fd = sbt_fd_;
	bulk_op[0].op_type = SBT_SYS_OP_DEL;
	bulk_op[0].key = ent_to_remove->to_btree_key();
	bulk_op[0].cm = BT_CM_BUF;
	bulk_op[0].old_entry_buf = const_cast<void*>(rem_ent_serialization);
	bulk_op[0].old_entry_size = rem_ent_serialized_size;

	bulk_op[1].fd = sbt_fd_;
	bulk_op[1].op_type = SBT_SYS_OP_ADD;
	bulk_op[1].key = ent_to_add->to_btree_key();
	bulk_op[1].entry_buf = const_cast<void*>(add_ent_serialization);
	bulk_op[1].entry_size = add_ent_serialized_size;

	if (ifs_sbt_bulk_op(2, bulk_op) != 0) {
		error = isi_system_error_new(errno,
		    "failed to bulk remove and add entries "
		    "(to_remove process_time: %{} devid: %u index: %u) "
		    "(to_add process_time: %{} devid: %u index: %u) ",
		    process_time_fmt(ent_to_remove->get_process_time()),
		    ent_to_remove->get_devid(), ent_to_remove->get_index(),
		    process_time_fmt(ent_to_add->get_process_time()),
		    ent_to_add->get_devid(), ent_to_add->get_index());
		goto out;
	}

out:
	if (dst_entry_group_locked)
		unlock_entry_group(process_time, isi_error_suppress(IL_ERR));

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::initialize(
    struct isi_error **error_out)
{
	ASSERT(!initialized_);

	struct isi_error *error = NULL;

	devid_ = get_devid(&error);
	if (error != NULL) {
		isi_error_add_context(error, "failed to initialize index");
		goto out;
	}

	sbt_fd_ = open_sbt(get_sbt_name(), true, &error);
	if (error != NULL) {
		isi_error_add_context(error, "failed to initialize index");
		goto out;
	}

	lock_fd_ = open(get_lock_filename(), O_RDONLY | O_CREAT, 0755);
	if (lock_fd_ < 0) {
		error = isi_system_error_new(errno, "error opening lock file");
		goto out;
	}

	initialized_ = true;

 out:
	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
uint32_t
time_based_index<entry_type, item_type>::get_release_version(void)
{
	return release_version_;
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::set_release_version(
    uint32_t in_release_version)
{
	release_version_ = in_release_version;
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::lock_entry_group(
    const struct timeval &process_time, struct isi_error **error_out) const
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	/* true: block / true: exclusive */
	_lock_entry_group(process_time, devid_, true, true, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::unlock_entry_group(
    const struct timeval &process_time, struct isi_error **error_out) const
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	_unlock_entry_group(process_time, devid_, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::lock_entry_group(
    const struct timeval &process_time, ifs_devid_t devid,
    struct isi_error **error_out) const
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	/* true: block / true: exclusive */
	_lock_entry_group(process_time, devid, true, true, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::unlock_entry_group(
    const struct timeval &process_time, ifs_devid_t devid,
    struct isi_error **error_out) const
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	_unlock_entry_group(process_time, devid, &error);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::lock_entry(const entry_type *entry,
    struct isi_error **error_out) const
{
	ASSERT(entry != NULL);

	/*
	 * To lock a specific entry, first take a shared lock on the entry's
	 * group and then an exclusive lock on the entry itself; this upholds
	 * lock ordering rules.
	 */

	struct isi_error *error = NULL;

	/* true: block / false: shared */
	_lock_entry_group(entry->get_process_time(), entry->get_devid(),
	    true, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* true: block / true: exclusive */
	_lock_entry(entry->get_process_time(), entry->get_devid(),
	    entry->get_index(), true, true, &error);
	if (error != NULL) {
		_unlock_entry_group(entry->get_process_time(),
		    entry->get_devid(), isi_error_suppress(IL_ERR));

		ON_ISI_ERROR_GOTO(out, error);
	}

 out:
	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::unlock_entry(const entry_type *entry,
    struct isi_error **error_out) const
{
	ASSERT(entry != NULL);

	struct isi_error *error = NULL;

	_unlock_entry(entry->get_process_time(), entry->get_devid(),
	    entry->get_index(), &error);
	_unlock_entry_group(entry->get_process_time(), entry->get_devid(),
	    error == NULL ? &error : isi_error_suppress(IL_ERR));

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::add_item(uint32_t item_version,
    const item_type &item, const struct timeval &process_time,
    struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	bool entry_group_locked = false, existing_entry;
	entry_type *entry = NULL;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;
	btree_key_t key;

	lock_entry_group(process_time, &error);
	ON_ISI_ERROR_GOTO(out, error);
	entry_group_locked = true;

	/*
	 * Find space to add the item to the index.  Start at an index value of
	 * 1 as 0 is reserved.
	 */
	for (uint32_t index = 1; true; ++index) {
		entry = get_entry(process_time, devid_, index, &error);
		if (error != NULL && isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

		existing_entry = true;
		if (entry == NULL) {
			entry = new entry_type(item_version);
			ASSERT(entry != NULL);

			entry->set_process_time(process_time);
			entry->set_devid(devid_);
			entry->set_index(index);

			existing_entry = false;
		}

		entry->save_serialization();
		entry->add_item(item, &error);
		if (error == NULL)
			break;
		else if (isi_system_error_is_a(error, EEXIST)) {
			isi_error_free(error);
			error = NULL;

			break;
		}
		else if (isi_system_error_is_a(error, ENOSPC)) {
			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

		delete entry;
		entry = NULL;
	}

	/*
	 * Having created/modified an entry to contain the added item, persist
	 * the addition/modification of the index.
	 */
	key = entry->to_btree_key();
	entry->get_serialized_entry(new_serialization, new_serialization_size);

	if (existing_entry) {
		entry->get_saved_serialization(old_serialization,
		    old_serialization_size);

		if (ifs_sbt_cond_mod_entry(sbt_fd_, &key, new_serialization,
		    new_serialization_size, NULL, BT_CM_BUF, old_serialization,
		    old_serialization_size, NULL) != 0) {
			error = isi_system_error_new(errno,
			    "failed to add item (%{}) "
			    "to index at process time %{}",
			    item.get_print_fmt(),
			    process_time_fmt(process_time));
		}
	}
	else if (ifs_sbt_add_entry(sbt_fd_, &key, new_serialization,
	    new_serialization_size, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "failed to add item (%{}) to index at process time %{}",
		    item.get_print_fmt(), process_time_fmt(process_time));
	}

 out:
	if (entry_group_locked)
		unlock_entry_group(process_time, isi_error_suppress(IL_ERR));

	if (entry != NULL)
		delete entry;

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::remove_item(const item_type &item,
    const struct timeval &process_time, struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	bool entry_group_locked = false;
	entry_type *entry = NULL;
	btree_key_t key = entry_type::to_btree_key(process_time, 0, 0);
	char *entry_buf = NULL;
	struct get_sbt_entry_ctx *ctx = NULL;
	struct sbt_entry *sbt_ent = NULL;
	struct timeval entry_process_time;

	lock_entry_group(process_time, &error);
	ON_ISI_ERROR_GOTO(out, error);
	entry_group_locked = true;

	/*
	 * We need to search all entries in this entry group in order to
	 * remove the item or definitively say that it does not exist for the
	 * specified process time.
	 */
	while (true) {
		/* Read an entry from the SBT. */
		sbt_ent = get_sbt_entry(sbt_fd_, &key, &entry_buf, &ctx,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
		if (sbt_ent == NULL)
			break;

		entry = new entry_type(sbt_ent->key, sbt_ent->buf,
		    sbt_ent->size_out, &error);
		ON_ISI_ERROR_GOTO(out, error);
		ASSERT(entry != NULL);

		/*
		 * If the entry's process time is not equal to the specified
		 * value, stop looking.
		 */
		entry_process_time = entry->get_process_time();
		if (timercmp(&entry_process_time, &process_time, !=))
			break;

		/* Try to remove the item from the entry, ignoring ENOENT. */
		entry->remove_item(item, &error);
		if (error == NULL || !isi_system_error_is_a(error, ENOENT)) {
			/*
			 * Either the item has succcessfully been removed or
			 * there was an unexpected error; bail out either way.
			 */
			goto out;
		}

		/*
		 * The item wasn't found in this entry, but might exist in a
		 * later entry in this entry group, so reset before retrieving
		 * the next entry.
		 */
		isi_error_free(error);
		error = NULL;

		free(entry_buf);
		entry_buf = NULL;

		delete entry;
		entry = NULL;
	}

	/*
	 * We haven't found the entry and have searched the entire entry group.
	 */
	error = isi_system_error_new(ENOENT,
	    "failed to remove item (%{}) from index as it is "
	    "not associated with the specified process time (%{})",
	    item.get_print_fmt(), process_time_fmt(process_time));

 out:
	if (entry_group_locked)
		unlock_entry_group(process_time, isi_error_suppress(IL_ERR));

	if (entry != NULL)
		delete entry;

	free(entry_buf);
	get_sbt_entry_ctx_destroy(&ctx);

	isi_error_handle(error, error_out);
}

template <class entry_type, class item_type>
entry_type *
time_based_index<entry_type, item_type>::get_entry_before_or_at(
    time_t upper_limit, bool &more_eligible_entries,
    struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	entry_type *ent = NULL, *ret_ent = NULL;

	btree_key_t key = {{ 0, 0 }};
	char *entry_buf = NULL;
	struct get_sbt_entry_ctx *ctx = NULL;
	struct sbt_entry *sbt_ent = NULL;

	bool entry_group_locked = false, entry_locked = false;

	char reread_entry_buf[8192];
	size_t reread_entry_size;

	while (ret_ent == NULL) {
		/*
		 * Assume there are no eligible entries until we have seen one.
		 */
		more_eligible_entries = false;

		/* Read an entry from the SBT. */
		sbt_ent = get_sbt_entry(sbt_fd_, &key, &entry_buf, &ctx,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
		if (sbt_ent == NULL)
			goto out;

		ent = new entry_type(sbt_ent->key, sbt_ent->buf,
		    sbt_ent->size_out, &error);
		ON_ISI_ERROR_GOTO(out, error);
		ASSERT(ent != NULL);

		/*
		 * If the entry's process time is later than the specified
		 * limit, then there's no more work to do.
		 */
		if (ent->get_process_time().tv_sec > upper_limit)
			goto out;

		/*
		 * Since we've read an entry that is eligible for processing
		 * (based on its process time), we must assume that there are
		 * additional eligible entries, or more precisely, we can't
		 * assume that there aren't.  In other words, we have to assume
		 * that there are more entries to process until we have
		 * verified that there aren't any.
		 */
		more_eligible_entries = true;

		/*
		 * Attempt to take a shared lock on this entry's group.  If we
		 * fail due to EWOULDBLOCK, another thread is updating one or
		 * more entries in this group, so skip it and attempt to read
		 * another entry; it's possible that this next entry is in the
		 * same entry group as the first but that's OK.
		 */
		/* false: don't block / false: shared lock */
		_lock_entry_group(ent->get_process_time(), ent->get_devid(),
		    false, false, &error);
		if (error != NULL &&
		    isi_system_error_is_a(error, EWOULDBLOCK)) {
			isi_error_free(error);
			error = NULL;

			free(entry_buf);
			entry_buf = NULL;

			delete ent;
			ent = NULL;

			continue;
		}
		ON_ISI_ERROR_GOTO(out, error);
		entry_group_locked = true;

		/*
		 * With a shared lock held on the entry group, attempt to take
		 * an exclusive lock on this entry.  If we fail due to
		 * EWOULDBLOCK, another thread is already working on this entry
		 * so skip it and attempt to read another.
		 */
		/* false: don't block / true: exclusive */
		_lock_entry(ent->get_process_time(), ent->get_devid(),
		    ent->get_index(), false, true, &error);
		if (error != NULL &&
		    isi_system_error_is_a(error, EWOULDBLOCK)) {
			isi_error_free(error);
			error = NULL;

			free(entry_buf);
			entry_buf = NULL;

			_unlock_entry_group(ent->get_process_time(),
			    ent->get_devid(), isi_error_suppress(IL_ERR));
			entry_group_locked = false;

			delete ent;
			ent = NULL;

			continue;
		}
		ON_ISI_ERROR_GOTO(out, error);
		entry_locked = true;

		/*
		 * Having successfully taken the necessary locks, re-read the
		 * entry as it may have changed on-disk between the time we
		 * originally read it and the time at which we took the locks.
		 * It's also possible that another thread may have processed
		 * this entry, causing it to be removed - this is not an error
		 * and we'll try to get another entry.
		 */
		if (ifs_sbt_get_entry_at(sbt_fd_, &(sbt_ent->key),
		    reread_entry_buf, sizeof reread_entry_buf, NULL,
		    &reread_entry_size) != 0) {
                    if (errno == ENOENT) {
				_unlock_entry(ent->get_process_time(),
				    ent->get_devid(), ent->get_index(),
				    isi_error_suppress(IL_ERR));
				entry_locked = false;

				_unlock_entry_group(ent->get_process_time(),
				    ent->get_devid(),
				    isi_error_suppress(IL_ERR));
				entry_group_locked = false;

				free(entry_buf);
				entry_buf = NULL;

				delete ent;
				ent = NULL;

				continue;
			}

			ON_SYSERR_GOTO(out, errno, error,
			    "failed to re-read SBT entry for entry "
			    "(process_time: %{} devid: %u index: %u)",
			    process_time_fmt(ent->get_process_time()),
			    ent->get_devid(), ent->get_index());
		}

		/*
		 * Compare the original version with the re-read version, and
		 * update the entry if necessary.
		 */
		ent->save_serialization();

		if (sbt_ent->size_out != reread_entry_size ||
		    memcmp(sbt_ent->buf, reread_entry_buf,
		    sbt_ent->size_out) != 0) {
			ent->set_serialized_entry(get_release_version(),
			    reread_entry_buf,
			    reread_entry_size, &error);
			ON_ISI_ERROR_GOTO(out, error);
			ent->save_serialization();
		}

		ret_ent = ent;
	}

 out:
	if (error != NULL) {
		if (entry_group_locked)
			_unlock_entry_group(ent->get_process_time(),
			    ent->get_devid(), isi_error_suppress(IL_ERR));

		if (entry_locked)
			_unlock_entry(ent->get_process_time(),
			    ent->get_devid(), ent->get_index(),
			    isi_error_suppress(IL_ERR));
	}

	if (ret_ent != ent)
		delete ent;

	free(entry_buf);
	get_sbt_entry_ctx_destroy(&ctx);

	isi_error_handle(error, error_out);

	return ret_ent;
}

template <class entry_type, class item_type>
void
time_based_index<entry_type, item_type>::remove_entry_and_handle_failures(
    entry_type *original_entry, entry_type *failed_objs_entry,
    struct isi_error **error_out)
{
	ASSERT(original_entry != NULL);
	ASSERT(failed_objs_entry != NULL);
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	if (failed_objs_entry->get_items().empty()) {
		/* No failed items, just remove original. */
		remove_entry(original_entry, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	else {
		/*
		 * Failed items exist, transactionally remove the original
		 * entry and add the one containing the failed objects.
		 */
		bulk_remove_and_add_entries(original_entry, failed_objs_entry,
		    &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

 out:
	isi_error_handle(error, error_out);
}

#endif // __ISI_CBM_TIME_BASED_INDEX_H__
