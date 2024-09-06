#ifndef __ISI_CBM_COI_H__
#define __ISI_CBM_COI_H__

#include <ifs/ifs_types.h>

#include <set>
#include <vector>

#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_cpool_d_common/isi_tri_bool.h>

#include "isi_cbm_ooi.h"
#include "isi_cbm_id.h"

struct coi_sbt_key;
void object_id_to_hsbt_key(const isi_cloud_object_id &id, coi_sbt_key &key);

namespace isi_cloud
{

	/**
	 * The coi_entry class describes entries stored in the Cloud Object Index.
	 */
	////coi entry保存在coi中
	class coi_entry
	{
	protected:
		/* value (serialized) */
		uint32_t version_;

		/* key (serialized) */
		isi_cloud_object_id object_id_;

		uint8_t archiving_cluster_id_[IFSCONFIG_GUID_SIZE];
		isi_cbm_id archive_account_id_;
		char *cmo_container_;
		isi_tri_bool compressed_;
		isi_tri_bool checksum_;
		bool backed_;
		struct timeval co_date_of_death_;
		std::set<ifs_lin_t> referencing_lins_; //////referencing_lins_中的每一个lin,都是正在引用当前这个coi entry
		btree_key_t ooi_entry_key_;

		/* bookkeeping (not serialized) */
		bool serialization_up_to_date_;
		size_t serialized_entry_size_;
		void *serialized_entry_;
		mutable size_t saved_serialization_size_;
		mutable void *saved_serialization_;

		void serialize(void);
		void deserialize(uint32_t expected_version, isi_error **error_out);

		/* Copy and assignment are not allowed. */
		coi_entry(const coi_entry &);
		coi_entry &operator=(const coi_entry &);

	public:
		coi_entry();
		~coi_entry();

		/**
		 * Set the cloud object ID of this entry.  Never throws.
		 *
		 * @param[in]  id		the cloud object ID
		 */
		void set_object_id(const isi_cloud_object_id &id);

		/**
		 * Retrieve the cloud object ID of this entry.  Never throws.
		 *
		 * @return			this entry's cloud object ID
		 */
		const isi_cloud_object_id &get_object_id(void) const;

		// no set_version, see ctor

		/**
		 * Retrieve the version of this entry.  Never throws.
		 *
		 * @return			this entry's version
		 */
		uint32_t get_version(void) const;

		/**
		 * Set the guid of the cluster used to archive the cloud object.  Never
		 * throws.
		 *
		 * @param[in]  id		the archiving cluster's guid
		 */
		void set_archiving_cluster_id(const uint8_t id[IFSCONFIG_GUID_SIZE]);

		/**
		 * Retrieve the guid of the cluster that archived the cloud object.
		 * Never throws.
		 *
		 * @return			the archiving cluster's guid
		 */
		const uint8_t *get_archiving_cluster_id(void) const;

		/**
		 * Set the account ID used to archive the cloud object.  Never throws.
		 *
		 * @param[in]  id		the account ID used to archive the
		 * cloud object
		 */
		void set_archive_account_id(const isi_cbm_id &id);

		/**
		 * Retrieve the account ID used to archive the cloud object.  Never
		 * throws.
		 *
		 * @return			the account ID used to archive the
		 * cloud object
		 */
		const isi_cbm_id &get_archive_account_id(void) const;

		/**
		 * Set the CMO container used to archive the cloud object.  Never
		 * throws.
		 *
		 * @param[in]  id		the CMO container
		 */
		void set_cmo_container(const char *cmo_container);

		/**
		 * Retrieve the CMO container of this cloud object.  Never throws.
		 *
		 * @return			the CMO container
		 */
		const char *get_cmo_container(void) const;

		/**
		 * Set whether or not a stub referring to a cloud object is backed-up.
		 * Never throws.
		 *
		 * @param[in]  backed	true if a stub referring to cloud object is
		 * backed-up, else false
		 */
		void set_backed(bool backed);

		/**
		 * Retrieve whether or not a stub referring to a cloud object is
		 * backed-up.  Never throws.
		 *
		 * @return	true if a stub referring to cloud object is
		 * backed-up, else false
		 */
		bool is_backed(void) const;

		/**
		 * Set whether or not the cloud object is compressed.  Never throws.
		 *
		 * @param[in]  compressed	true if cloud object is compressed,
		 * else false
		 */
		void set_compressed(isi_tri_bool compressed);

		/**
		 * Retrieve whether or not the cloud object is compressed.  Never
		 * throws.
		 *
		 * @return			true if cloud object is compressed,
		 * else false
		 */
		isi_tri_bool is_compressed(void) const;

		/**
		 * Set whether of not the cloud object is with checksum
		 *
		 * @param[in] checksum		true if the cloud object is with checksum
		 */
		void set_checksum(isi_tri_bool checksum);

		/**
		 * Retrieve whether or not the cloud object contains checksum
		 *
		 * @return			true if cloud object has checksum
		 */
		isi_tri_bool has_checksum() const;

		/**
		 * Update the cloud object's date of death, which is the (absolute)
		 * time before which the cloud object will not be garbage collected.
		 * The cloud object's date of death will be the larger of the current value
		 * and the input value, unless the override parameter is set.
		 * Never throws.
		 *
		 * @param[in]  date_of_death	the cloud object's potentially new date
		 * of death
		 * @param[in]  force	force to update the cloud object's date of death
		 * to any datetime
		 */
		void update_co_date_of_death(const timeval &date_of_death, bool force);

		/**
		 * Retrieve the cloud object's date of death.  The cloud object will
		 * not be garbage collected before this (absolute) time.  Never throws.
		 *
		 * @return			the cloud object's date of death
		 * time
		 */
		struct timeval get_co_date_of_death(void) const;

		/**
		 * Retrieve the reference count for this entry.  Never throws.
		 *
		 * @return			the current reference count
		 */
		int get_reference_count(void) const;

		/**
		 * Get the set of LINs that reference the cloud object represented by
		 * this entry.  Never throws.
		 *
		 * @return			the set of referencing LINs
		 */
		const std::set<ifs_lin_t> &get_referencing_lins(void) const;

		/**
		 * Add a referencing LIN to this entry.  Reference count will be
		 * incremented.  Never throws.
		 *
		 * @param[in]  lin		referencing LIN to add
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void add_referencing_lin(ifs_lin_t lin, struct isi_error **error_out);

		/**
		 * Remove a referencing LIN from this entry, unless this LIN other
		 * versions using the same COID.  Never throws.
		 *
		 * @param[in]  lin		referencing LIN to remove
		 * @param[in]  snapid		the snapid to remove
		 * @param[out] error_out	error, if any
		 *
		 * @return			whether or not the specified LIN was
		 * actually removed from the set of references
		 */
		bool remove_referencing_lin(ifs_lin_t lin, ifs_snapid_t snapid,
									struct isi_error **error_out);

		/**
		 * An invalid  key value used to indicate absence of an associated
		 * OOI entry.
		 */
		static const btree_key_t NULL_BTREE_KEY;

		/**
		 * An invalid key value used to indicate that the cloud object
		 * associated with this entry is being garbage collected.
		 */
		static const btree_key_t GC_BTREE_KEY;

		/**
		 * Set the key used to store the cloud object ID associated with this
		 * entry in the OOI.
		 *
		 * @param[in]  key		the OOI entry key
		 */
		void set_ooi_entry_key(const btree_key_t &key);

		/**
		 * Reset the key used to store the cloud object ID associated with this
		 * entry in the OOI.
		 */
		void reset_ooi_entry_key(void);

		/**
		 * Retrieve the OOI entry key.
		 *
		 * @return                      the OOI entry key
		 */
		const btree_key_t &get_ooi_entry_key(void) const;

		/**
		 * Reinitialize this object using a serialized version.  Never throws.
		 *
		 * @param[in]  expected_version expected coi version.
		 * @param[in]  entry		the serialized entry
		 * @param[in]  size		the size of the serialized entry
		 * @param[out] error            any error encountered during the
		 * operation
		 */
		void set_serialized_entry(uint32_t expected_version, const void *entry,
								  size_t size, isi_error **error_out);

		/**
		 * Retrieve this entry in serialized form.  Caller should neither
		 * modify nor deallocate contents.  Never throws.
		 *
		 * @param[out] entry		the serialized entry
		 * @param[out] size		the size of the serialized entry
		 */
		void get_serialized_entry(const void *&entry, size_t &size);

		/**
		 * Save the current serialization such that it can be retrieved later.
		 * Only one serialization can be saved (although it can be overwritten
		 * by a later save).
		 */
		void save_serialization(void) const;

		/**
		 * Retrieve the saved serialization.
		 *
		 * @param[out] serialization    the saved serialization
		 * @param[out] size             the saved serialization's size
		 */
		void get_saved_serialization(const void *&serialization,
									 size_t &size) const;
	};

	const uint32_t CMOI_ENTRY_MAGIC = 0x434D4F49; //'CMOI'
	/**
	 * Class modeling an entry in the CMO index.
	 */
	class cmoi_entry
	{
		friend class coi;
		friend class coi_entry;

	private:
		uint32_t magic_;
		uint32_t version_;

		/**
		 * The count of the versions referencing this CMO
		 */
		uint64_t version_count_;
		/**
		 * Bit indicating if any version of the CMO is under sync
		 */
		bool syncing_;

		/**
		 * Highest version number ever recorded for this
		 * CMO
		 */
		uint64_t highest_ver_;

		/**
		 * Next syncer_id
		 */
		uint64_t next_syncer_id_;

		/**
		 * Claimed syncer_id
		 */
		uint64_t syncer_id_;

		/**
		 * set the highest version number stored in this entry
		 */
		void set_highest_ver(uint64_t highest_ver)
		{
			highest_ver_ = highest_ver;
		}

		/**
		 * increment version count
		 */
		void increment_version_count();

		/**
		 * decrement version count
		 */
		void decrement_version_count();

		/**
		 * set the syncing status for the cloud object
		 */
		void set_syncing(bool syncing)
		{
			syncing_ = syncing;
		}

		/**
		 * increment the syncer id
		 */
		void increment_next_syncer_id();

		/**
		 * Set the syncer id
		 */
		void set_syncer_id(uint64_t next_syncer_id);

		const static size_t get_packed_size();

	public:
		/**
		 * c-tor
		 */
		cmoi_entry();

		/**
		 * get the structure version
		 * @return the structure version
		 */
		uint32_t get_version() const
		{
			return version_;
		}

		/**
		 * Get the highest version number stored in this entry
		 */
		uint64_t get_highest_ver() const
		{
			return highest_ver_;
		}

		/**
		 * Get the count of versions referencing this CMO
		 */
		uint64_t get_version_count() const
		{
			return version_count_;
		}

		/**
		 * check if the cloud object is under synchronization
		 */
		bool get_syncing() const
		{
			return syncing_;
		}

		/**
		 * @return the next syncer id for the cloud object
		 */
		uint64_t get_next_syncer_id() const
		{
			return next_syncer_id_;
		}

		/**
		 * @return the syncer id for the cloud object
		 */
		uint64_t get_syncer_id() const
		{
			return syncer_id_;
		}

		/**
		 * deserialize from a blob
		 */
		void deserialize(const void *buffer, size_t len,
						 struct isi_error **error_out);

		/**
		 * serialize the data to a blob
		 */
		void serialize(std::vector<char> &buffer);
	};

	/**
	 * The Cloud Object Index (COI) holds information about the references of cloud
	 * metadata objects (CMOs) to local stub files.  This information is used in the
	 * context of garbage collection.
	 */
	////coi类用做gc  ooi继承于time_based_index<ooi_entry, isi_cloud_object_id>
	class coi
	{
	protected:
		bool initialized_;
		int sbt_fd_;	  /////创建coi sbt
		int cmo_sbt_fd_;  /// 创建cmoi sbt
		int coi_lock_fd_; ////关于coi 的文件锁
		uint32_t release_version_;

		/* Copy and assignment are not allowed. */
		coi(const coi &);
		coi &operator=(const coi &);

	public:
		coi();
		~coi();

		/**
		 * Initialize the index object.  Must be performed before any operation
		 * on the object.  Never throws.
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void initialize(struct isi_error **error_out);

		/**
		 * Get the release version.
		 * @param[out] release_version.
		 */
		uint32_t get_release_version(void);

		/**
		 * Set the release version.
		 * @param[in] release version.
		 */
		void set_release_version(uint32_t in_release_version);

		/**
		 * Adds a new cloud object to the index.  Reference count will be
		 * initialized to 1.  An error is returned if an entry with the
		 * specified object id already exists.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  archiving_cluster_id	the guid of the archiving
		 * cluster
		 * @param[in]  archive_account_id	the id of the account used to
		 * archive the cloud object
		 * @param[in]  cmo_container	the container used to store the CMO
		 * @param[in]  is_compressed	indicate the compressed state
		 * @param[in]  checksum		indicate the checksum state
		 * @param[in]  referencing_lin	the LIN of the archived file
		 * @param[out] error_out	any error encountered during the
		 * operation
		 * @param[in]  date_of_death	the time past which cloud object  can
		 * @param[in]  backed_up	optional flag indicate if backed up
		 * be garbage collected if it is no longer referred-to
		 *
		 */
		//////archive,restore
		void add_object(const isi_cloud_object_id &object_id,
						const uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE],
						const isi_cbm_id &archive_account_id,
						const char *cmo_container, isi_tri_bool is_compressed,
						isi_tri_bool checksum,
						ifs_lin_t referencing_lin, struct isi_error **error_out,
						struct timeval *date_of_death_ptr = NULL,
						bool *backed_up = NULL);

		/**
		 * Restore a cloud object to the COI. It is not an error if
		 * the object is already found in the COI index. If the reference
		 * count for the COI entry is 0, this routine also ensures an entry
		 * is created in the OOI. COID lock must have been taken for the given
		 * object_id before calling this function.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  archiving_cluster_id	the guid of the archiving
		 * cluster
		 * @param[in]  archive_account_id	the id of the account used to
		 * archive the cloud object
		 * @param[in]  cmo_container	the container used to store the CMO
		 * @param[in]  is_compressed	indicate the compressed state
		 * @param[in]  checksum		indicate the checksum state
		 * @param[in]  date_of_death	the time past which cloud object  can
		 * @param[out] error_out	any error encountered during the
		 * operation
		 * be garbage collected if it is no longer referred-to
		 *
		 */
		///isi_cbm_restore_coi.cpp
		void restore_object(const isi_cloud_object_id &object_id,
							uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE],
							const isi_cbm_id &archive_account_id,
							const char *cmo_container, isi_tri_bool is_compressed,
							isi_tri_bool checksum,
							struct timeval date_of_death,
							struct isi_error **error_out);

		/**
		 * Mark that a cloud object is in the process of being removed
		 * from the index. An error is returned if an entry with the
		 * specified object id does not exist or if the reference
		 * count is non-zero.  Marking an object for removal prevents
		 * new references from being added to it.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		//////标记这个需要删除的cloud object  isi_cbm_gc.cpp
		void removing_object(const isi_cloud_object_id &object_id,
							 struct isi_error **error_out);

		/**
		 * Remove a cloud object from the index.  Reference count must be zero
		 * unless ignore_ref_count is true.  An error is returned if cloud
		 * object does not exist.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  ignore_ref_count	if false, operation will fail if
		 * reference count of cloud object to be removed is non-zero
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		/////从index中删除这个cloud object  isi_cbm_gc.cpp
		void remove_object(const isi_cloud_object_id &object_id,
						   bool ignore_ref_count,
						   struct isi_error **error_out);

		/**
		 * Add a referencing LIN to the existing index entry.  Reference count
		 * will be incremented.  An error is returned if an entry with the
		 * specified object id does not exist.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  referencing_lin	the new LIN
		 * @param[out] error_out	any error encountered during the
		 * operation
		 * @param[in] add_ref_dod	the time past which cloud object  can
		 * 				be garbage collected if it is
		 * 				no longer referred-to
		 */
		/////除了check_coi.cpp，没用到
		void add_ref(const isi_cloud_object_id &object_id,
					 ifs_lin_t referencing_lin, struct isi_error **error_out,
					 struct timeval *add_ref_dod_ptr = NULL);

		/**
		 * Remove a referencing LIN from the existing index entry.  Reference
		 * count will be decremented, and date of death will be updated if
		 * current value is less than that of the referencing LIN to be
		 * removed.  An error is returned if an entry with the specified object
		 * id does not exist or if the specified LIN does not reference the
		 * specified object.
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  lin		the LIN to remove
		 * @param[in]  snapid		the snapid for the object being removed
		 * @param[in]  removed_ref_dod	the date of death associated with the
		 * referencing LIN to be removed
		 * @param[out] effective_dod	the effective date of death after
		 * accounting for removed_ref_dod; can be NULL
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		////isi_cbm_recall.cpp中的isi_cbm_file_stub_purge(file_obj, ckpt_found, false使用
		void remove_ref(const isi_cloud_object_id &object_id, ifs_lin_t lin,
						ifs_snapid_t snapid,
						struct timeval removed_ref_dod, struct timeval *effective_dod,
						struct isi_error **error_out);

		/**
		 * Retrieve an entry given a specified object ID.
		 *
		 * @param[in]  object_id	the key of the desired entry
		 * @param[out] error_out	any error encountered during the
		 * operation
		 * @return			if desired entry is found, a pointer to
		 * a coi_entry which must be deallocated (using delete) after use,
		 * otherwise NULL
		 */
		coi_entry *get_entry(const isi_cloud_object_id &object_id,
							 struct isi_error **error_out);

		/**
		 * Get the CMO specified by object_id
		 * @param object_id[id]		the object_id
		 * @param cmo_entry		The cmo object
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void get_cmo(const isi_cloud_object_id &object_id,
					 cmoi_entry &cmo_entry,
					 struct isi_error **error_out);
		/**
		 * Get the immediate next version of the given cloud object version
		 * @param[in]  object_id	the key of the desired entry
		 * @param[in]  next		the key of the next version entry
		 * @param[out] error_out	any error encountered during the
		 * @return if the next version is found, true, otherwise false
		 */
		bool get_next_version(const isi_cloud_object_id &object_id,
							  isi_cloud_object_id &next,
							  struct isi_error **error_out);

		/**
		 * Get the immediate previous version of the given cloud object version
		 * @param[in]  object_id	the key of the desired entry
		 * @param[in]  prev		the key of the previous version entry
		 * @param[out] error_out	any error encountered during the
		 * @return if the previous version is found, true, otherwise false
		 */
		bool get_prev_version(const isi_cloud_object_id &object_id,
							  isi_cloud_object_id &prev,
							  struct isi_error **error_out);

		/**
		 * Get the last version of the given cloud object
		 * @param[in]  object_id	the key of the desired entry
		 * @param[in]  last		the key of the last version entry
		 * @param[out] error_out	any error encountered during the
		 * @return if the last version is found, true, otherwise false
		 */
		bool get_last_version(const isi_cloud_object_id &object_id,
							  isi_cloud_object_id &last,
							  struct isi_error **error_out);

		/**
		 * Update the backed up status of an existing entry.  An error
		 * is returned if an entry with the specified object id does not exist.
		 * Sets the date_of_death to be the larger of the existing value
		 * and the 'retain_time'
		 *
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  retain_time	the time to which this object
		 * should be retained
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void update_backed(const isi_cloud_object_id &object_id,
						   struct timeval retain_time, struct isi_error **error_out);

		/**
		 * update the sync status
		 * @param[in]  object_id	the CMO object ID
		 * @param[in]  syncer_id	The syncer id, obtained by earlier call
		 * of generate_sync_id.
		 * @param[in]  syncing		the sync status
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void update_sync_status(const isi_cloud_object_id &object_id,
								uint64_t syncer_id, bool syncing, struct isi_error **error_out);

		/**
		 * Get the referencing lins for the specified object_id
		 * @param[in]  object_id	the CMO object ID for the entry to be
		 * @param[out] lins		The lins referencing the object
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void get_references(const isi_cloud_object_id &object_id,
							std::set<ifs_lin_t> &lins, struct isi_error **error_out);

		/**
		 * lock the mentioned cloud object_id exclusively
		 * @param[in]	object_id	the cloud object id
		 * @param[in]	block		To block or not to block
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void lock_coid(const isi_cloud_object_id &object_id, bool block,
					   struct isi_error **error_out);

		/**
		 * release the lock on the object_id acquired by lock_coid
		 * @param[in]	object_id	the cloud object id
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void unlock_coid(const isi_cloud_object_id &object_id,
						 struct isi_error **error_out);

		/**
		 * Generate a new sync id for object_id. The COID lock on the object
		 * must be held.
		 * @param object_id[in] The object id to sync for
		 * @param sync_id[out] The sync id
		 * @param[out] error_out Error if any. If it is under sync already
		 * , will return CBM error CBM_SYNC_ALREADY_IN_PROGRESS. A new Id
		 * will not be generated.
		 */
		void generate_sync_id(const isi_cloud_object_id &object_id,
							  uint64_t &sync_id, struct isi_error **error_out);

		/**
		 * Confirm that the coi entry for object_id has the specified
		 * serialization content. If not, return what is different as error.
		 */
		void validate_entry(
			const isi_cloud_object_id &object_id,
			const void *serialization, size_t serialization_size,
			struct isi_error **error_out);

		/**
		 * Returns true if the coi has any entries and false if it is empty
		 * @param error_out Error if any (Note, ENOENT does not manifest as an
		 * error)
		 */
		bool has_entries(struct isi_error **error_out) const;
	};

} // end namespace isi_cloud

/**
 * A convenience function to perform the ooi/coi bulk op and
 * handle emitted error
 * @param[in]  ooi the instance of isi_cloud::ooi
 * @param[in]  coi the instance of isi_cloud::coi
 * @param[in]  coi_sbt_bulk_entry[] the array of coi_sbt_bulk_entry to perform
 * SBT bulk operation
 * The COI entry has to be the first element in the array coi_sbt_ops and all
 * the OOI entry comes after it because the implementation of the SBT bulk op
 * function requires all operations on a given SBT be consecutive in the array
 * of operations
 * It can support one COI entry and one or two OOI entries
 * @param[in]  sbt_ops_num number of sbt operations
 * @param[in]  valid_ooi_entry_keys[] the keys of ooi entry which is used to
 * valid whether the ooi entry is updated
 * @param[in]  object_id the cloud object id of coi entry
 * @param[out] error_out any error encountered during the operation
 */
void ooi_coi_bulk_op_wrap(isi_cloud::ooi &ooi, isi_cloud::coi &coi,
						  struct coi_sbt_bulk_entry coi_sbt_ops[],
						  const size_t sbt_ops_num,
						  const btree_key_t valid_ooi_entry_keys[], const isi_cloud_object_id &object_id,
						  struct isi_error **error_out);

#endif // __ISI_CBM_COI_H__
