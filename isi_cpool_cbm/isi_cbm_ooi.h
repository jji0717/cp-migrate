#ifndef __ISI_CBM_OOI_H__
#define __ISI_CBM_OOI_H__

#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cbm_time_based_index_entry.h"
#include "isi_cbm_time_based_index.h"

struct coi_sbt_bulk_entry;

struct fmt_conv_ctx
date_of_death_fmt(const struct timeval &date_of_death);

namespace isi_cloud
{

	class ooi_entry : public time_based_index_entry<isi_cloud_object_id>
	{
	protected:
		/* Copy and assignment are not allowed. */
		ooi_entry(const ooi_entry &);
		ooi_entry &operator=(const ooi_entry &);

	public:
		ooi_entry();
		ooi_entry(uint32_t version);
		ooi_entry(btree_key_t key, const void *serialized,
				  size_t serialized_size, isi_error **error_out);
		~ooi_entry();

		/* The process time is known as the date of death in OOI-speak. */
		struct timeval get_date_of_death(void) const { return get_process_time(); }
		void set_date_of_death(const struct timeval &in) { set_process_time(in); }

		const std::set<isi_cloud_object_id> &get_cloud_objects(void) const;

		/**
		 * Add a cloud object ID to this entry.  Never throws.
		 *
		 * @param[in]  cloud_object_id	cloud object ID to add
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void add_cloud_object(const isi_cloud_object_id &cloud_object_id,
							  struct isi_error **error_out);

		/**
		 * Remove a cloud object ID from this entry.  Never throws.
		 *
		 * @param[in]  cloud_object_id	cloud object ID to remove
		 * @param[out] error_out	any error encountered during the
		 * operation
		 */
		void remove_cloud_object(const isi_cloud_object_id &cloud_object_id,
								 struct isi_error **error_out);
	};

	class ooi : public time_based_index<ooi_entry, isi_cloud_object_id>
	{
	protected:
		const char *get_sbt_name(void) const
		{
			return get_ooi_sbt_name();
		}

		const char *get_lock_filename(void) const
		{
			return get_ooi_lock_filename();
		}

		/**
		 * Prepare a bulk operation entry for adding a cloud object to the OOI.
		 * Assumes the destination entry group is locked.
		 *
		 * @param[in]  date_of_death	with devid, determines where in the OOI
		 * the cloud object should be added
		 * @param[in]  devid		with date_of_death, determines where in
		 * the OOI the cloud object should be added
		 * @param[in]  object_id	the cloud object ID to add
		 * @param[out] coi_sbt_op	the coi_sbt bulk operation entry to
		 * prepare
		 * @param[out] error_out	any error encountered during the
		 * preparation
		 *
		 * @return			either an OOI entry (revised or newly
		 * created for the cloud object ID addition) which must be deleted by
		 * caller after bulk operation has been executed or an error, but never
		 * both
		 */
		ooi_entry *
		_prepare_for_cloud_object_addition(const struct timeval &date_of_death,
										   ifs_devid_t devid, const isi_cloud_object_id &object_id,
										   struct coi_sbt_bulk_entry &coi_sbt_op,
										   struct isi_error **error_out);

		/* Copy and assignment are not allowed. */
		ooi(const ooi &);
		ooi &operator=(const ooi &);

	public:
		ooi() : time_based_index<ooi_entry, isi_cloud_object_id>(
					CPOOL_OOI_VERSION) {}
		~ooi() {}

		/**
		 * Prepare a bulk operation entry for adding a cloud object to the OOI.
		 * Assumes the destination entry group is locked.
		 *
		 * @param[in]  date_of_death	determines where in the OOI the cloud
		 * object should be added
		 * @param[in]  object_id	the cloud object ID to add
		 * @param[out] coi_sbt_op	the coi_sbt bulk operation entry to
		 * prepare
		 * @param[out] error_out	any error encountered during the
		 * preparation
		 *
		 * @return			either an OOI entry (revised or newly
		 * created for the cloud object ID addition) which must be deleted by
		 * caller after bulk operation has been executed or an error, but never
		 * both
		 */
		////////coi.remove_ref(recall时,从stub变成regular file)中,当ref_count=0时,需要add object_id到ooi entry中
		ooi_entry *
		prepare_for_cloud_object_addition(const struct timeval &date_of_death,
										  const isi_cloud_object_id &object_id,
										  struct coi_sbt_bulk_entry &coi_sbt_op,
										  struct isi_error **error_out);

		/**
		 * Prepare a bulk operation entry for removing a cloud object from the
		 * OOI.  Assumes the entry group from which the cloud object will be
		 * removed is locked.
		 *
		 * @param[in]  object_id	the cloud object ID to remove
		 * @param[in]  key		the key of the OOI entry containing the
		 * cloud object ID to remove
		 * @param[out] coi_sbt_op	the coi_sbt bulk operation entry to
		 * prepare
		 * @param[out] error_out	any error encountered during the
		 * preparation
		 *
		 * @return			either an OOI entry (revised or newly
		 * created for the cloud object ID addition) which must be deleted by
		 * caller after bulk operation has been executed or an error, but never
		 * both
		 */
		////////coi.add_ref时,当ref_count==1,需要将ooi中的这个cloud_object删除
		ooi_entry *
		prepare_for_cloud_object_removal(const isi_cloud_object_id &object_id,
										 const btree_key_t &key, struct coi_sbt_bulk_entry &coi_sbt_op,
										 struct isi_error **error_out);

		/**
		 * Confirm that the ooi entry for for the btree key has the specified
		 * serialization content. If not, return what is different as error.
		 * If existing_entry is passed as false, it returns error if the entry found
		 */
		void validate_entry(const btree_key_t &key,
							const void *serialization, size_t serialization_size,
							bool existing_entry,
							struct isi_error **error_out);
	};

} // end namespace isi_cloud

#endif // __ISI_CBM_OOI_H__
