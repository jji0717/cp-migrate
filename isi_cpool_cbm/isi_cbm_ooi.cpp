#include <ifs/ifs_syscalls.h>

#include <isi_sbtree/sbtree.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cbm_coi_sbt.h"

#include "isi_cbm_ooi.h"
#include "isi_cbm_error.h"

using namespace isi_cloud;

struct fmt_conv_ctx
date_of_death_fmt(const struct timeval &date_of_death)
{
	return process_time_fmt(date_of_death);
}

ooi_entry::ooi_entry()
	: time_based_index_entry<isi_cloud_object_id>(CPOOL_OOI_VERSION)
{
}

ooi_entry::ooi_entry(uint32_t version)
	: time_based_index_entry<isi_cloud_object_id>(version)
{
}

ooi_entry::ooi_entry(btree_key_t key, const void *serialized,
					 size_t serialized_size, isi_error **error_out)
	: time_based_index_entry<isi_cloud_object_id>(
		  CPOOL_OOI_VERSION, key, serialized, serialized_size, error_out)
{
	if (error_out)
	{
		if (isi_cbm_error_is_a(*error_out, CBM_VERSION_MISMATCH))
		{
			isi_error_add_context(*error_out, " OOI. ");
		}
	}
}

ooi_entry::~ooi_entry()
{
}

const std::set<isi_cloud_object_id> &
ooi_entry::get_cloud_objects(void) const
{
	return get_items();
}

void ooi_entry::add_cloud_object(const isi_cloud_object_id &cloud_object_id,
								 struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	add_item(cloud_object_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "cloud object ID: %{}", isi_cloud_object_id_fmt(&cloud_object_id));

out:
	isi_error_handle(error, error_out);
}

void ooi_entry::remove_cloud_object(const isi_cloud_object_id &cloud_object_id,
									struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	remove_item(cloud_object_id, &error);  /////从ooi entry中erase这个object_id
	ON_ISI_ERROR_GOTO(out, error,
					  "cloud object ID: %{}", isi_cloud_object_id_fmt(&cloud_object_id));

out:
	isi_error_handle(error, error_out);
}

ooi_entry *
ooi::_prepare_for_cloud_object_addition(const struct timeval &date_of_death,
										ifs_devid_t devid, const isi_cloud_object_id &object_id,
										struct coi_sbt_bulk_entry &coi_sbt_op, struct isi_error **error_out)
{
	ASSERT(devid != INVALID_DEVID);
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	ooi_entry *entry = NULL;
	bool existing_entry;
	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;

	/*
	 * First, find space to add the object ID to the OOI.  Start at an
	 * index value of 1 as 0 is reserved.
	 *
	 * (Note: We can iterate over the index chain without worrying about
	 * "holes" because we're looking for a place to add an entry.  If we
	 * were searching for a specific cloud object ID, then we either need
	 * to guard against holes when adding / consuming entries or use a more
	 * robust mechanism for reading entries (such as ifs_sbt_get_entries).)
	 */
	for (uint32_t index = 1; true; ++index)
	{
		entry = get_entry(date_of_death, devid, index, &error);
		if (error != NULL && isi_system_error_is_a(error, ENOENT))
		{
			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

		existing_entry = true;
		if (entry == NULL)
		{
			entry = new ooi_entry;
			ASSERT(entry != NULL);

			entry->set_date_of_death(date_of_death);
			entry->set_devid(devid);
			entry->set_index(index);

			existing_entry = false;
		}

		entry->save_serialization();
		entry->add_cloud_object(object_id, &error); //////把object_id 插入entry(insert 到set中)
		if (error == NULL)
			break;
		else if (isi_system_error_is_a(error, EEXIST))
		{
			isi_error_free(error);
			error = NULL;

			break;
		}
		else if (isi_system_error_is_a(error, ENOSPC))
		{
			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);

		delete entry;
		entry = NULL;
	}

	/* Next, create the appropriate bulk operation. */
	entry->get_serialized_entry(new_serialization, new_serialization_size);

	memset(&coi_sbt_op, 0, sizeof coi_sbt_op);

	coi_sbt_op.is_hash_btree = false;
	coi_sbt_op.bulk_entry.fd = sbt_fd_;
	coi_sbt_op.bulk_entry.key = entry->to_btree_key();
	coi_sbt_op.bulk_entry.entry_buf =
		const_cast<void *>(new_serialization);
	coi_sbt_op.bulk_entry.entry_size = new_serialization_size;

	if (existing_entry)
	{
		entry->get_saved_serialization(old_serialization,
									   old_serialization_size);

		coi_sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
		coi_sbt_op.bulk_entry.cm = BT_CM_BUF;
		coi_sbt_op.bulk_entry.old_entry_buf =
			const_cast<void *>(old_serialization);
		coi_sbt_op.bulk_entry.old_entry_size = old_serialization_size;
	}
	else
	{
		coi_sbt_op.bulk_entry.op_type = SBT_SYS_OP_ADD;
		coi_sbt_op.bulk_entry.cm = BT_CM_NONE;
	}

out:
	/* Ensure we're returning an entry or an error, but not both. */
	ASSERT((entry == NULL) != (error == NULL),
		   "failed post-condition (%snull entry, %snull error)",
		   entry != NULL ? "non-" : "", error != NULL ? "non-" : "");

	isi_error_handle(error, error_out);

	return entry;
}

////在coi.remove_ref时,当remove_referencing_lin之后,ref_count==0,那么调用ooi.prepare_for_cloud_object_addition(object_id)
ooi_entry *
ooi::prepare_for_cloud_object_addition(const struct timeval &date_of_death,
									   const isi_cloud_object_id &object_id,
									   struct coi_sbt_bulk_entry &coi_sbt_op, struct isi_error **error_out)
{
	printf("\n%s called\n",__func__);
	struct isi_error *error = NULL;

	ooi_entry *entry = _prepare_for_cloud_object_addition(date_of_death,
														  devid_, object_id, coi_sbt_op, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);

	return entry;
}
/////在coi.add_ref时,当add_referencing_lin之后,ref_count==1,那么调用ooi.prepare_for_cloud_object_removal(object_id)
ooi_entry *
ooi::prepare_for_cloud_object_removal(const isi_cloud_object_id &object_id,
									  const btree_key_t &key, struct coi_sbt_bulk_entry &coi_sbt_op,
									  struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	const void *old_serialization, *new_serialization;
	size_t old_serialization_size, new_serialization_size;

	ooi_entry *entry = get_entry(key, &error);
	ON_ISI_ERROR_GOTO(out, error);

	entry->save_serialization();

	entry->remove_cloud_object(object_id, &error);
	if (error != NULL)
	{
		delete entry;
		entry = NULL;
		ON_ISI_ERROR_GOTO(out, error);
	}

	entry->get_saved_serialization(old_serialization,
								   old_serialization_size);

	memset(&coi_sbt_op, 0, sizeof coi_sbt_op);

	coi_sbt_op.is_hash_btree = false;
	coi_sbt_op.bulk_entry.fd = sbt_fd_;
	coi_sbt_op.bulk_entry.key = key;

	if (entry->get_cloud_objects().size() == 0)
	{
		coi_sbt_op.bulk_entry.op_type = SBT_SYS_OP_DEL;
		coi_sbt_op.bulk_entry.cm = BT_CM_BUF;
		coi_sbt_op.bulk_entry.old_entry_buf =
			const_cast<void *>(old_serialization);
		coi_sbt_op.bulk_entry.old_entry_size = old_serialization_size;
	}
	else
	{
		entry->get_serialized_entry(new_serialization,
									new_serialization_size);

		coi_sbt_op.bulk_entry.op_type = SBT_SYS_OP_MOD;
		coi_sbt_op.bulk_entry.entry_buf =
			const_cast<void *>(new_serialization);
		coi_sbt_op.bulk_entry.entry_size = new_serialization_size;
		coi_sbt_op.bulk_entry.cm = BT_CM_BUF;
		coi_sbt_op.bulk_entry.old_entry_buf =
			const_cast<void *>(old_serialization);
		coi_sbt_op.bulk_entry.old_entry_size = old_serialization_size;
	}

out:
	/* Ensure we're returning an entry or an error, but not both. */
	ASSERT((entry == NULL) != (error == NULL),
		   "failed post-condition (%snull entry, %snull error)",
		   entry != NULL ? "non-" : "", error != NULL ? "non-" : "");

	isi_error_handle(error, error_out);

	return entry;
}

static std::string
objects_to_str(const std::set<isi_cloud_object_id> &objects)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	size_t num = objects.size();

	fmt_print(&fmt, "num:%zu, objects:", num);
	if (num == 0)
		fmt_print(&fmt, "none");
	else
	{
		for (std::set<isi_cloud_object_id>::const_iterator iter =
				 objects.begin();
			 iter != objects.end(); ++iter)
		{
			fmt_print(&fmt, "%{}, ",
					  isi_cloud_object_id_fmt(&*iter));
		}
	}

	return fmt_string(&fmt);
}

/**
 * Update error_in with any ooi entry differences
 */
static void
ooi_entry_difference_set(struct isi_error *error_in,
						 const isi_cloud::ooi_entry &o1, const isi_cloud::ooi_entry &o2)
{
	struct timeval dod[2] = {o1.get_date_of_death(),
							 o2.get_date_of_death()};
	if (timercmp(&dod[0], &dod[1], !=))
	{
		isi_error_add_context(error_in,
							  "Date of death different, curr: %{} expected: %{}",
							  date_of_death_fmt(dod[0]),
							  date_of_death_fmt(dod[1]));
	}
	if (o1.get_devid() != o2.get_devid())
	{
		isi_error_add_context(error_in,
							  "Device ID different, curr: %u expected: %u",
							  o1.get_devid(),
							  o2.get_devid());
	}
	if (o1.get_index() != o2.get_index())
	{
		isi_error_add_context(error_in,
							  "Index different, curr: %u expected: %u",
							  o1.get_index(),
							  o2.get_index());
	}
	std::set<isi_cloud_object_id> cloud_objects[2] =
		{o1.get_cloud_objects(), o2.get_cloud_objects()};
	if (cloud_objects[0] != cloud_objects[1])
	{
		isi_error_add_context(error_in,
							  "Associated object IDs different, curr: %s expected: %s",
							  objects_to_str(cloud_objects[0]).c_str(),
							  objects_to_str(cloud_objects[1]).c_str());
	}
}

void ooi::validate_entry(
	const btree_key_t &key,
	const void *serialization, size_t serialization_size,
	bool existing_entry,
	struct isi_error **error_out)
{
	isi_cloud::ooi_entry *curr_entry = NULL, *entry = NULL;
	const void *curr_serialization;
	size_t curr_serialization_size;
	struct isi_error *error = NULL;

	/*
	 * Discover what changed: compare the current
	 * sbt content for this entry with the one we think
	 * it should be
	 */
	curr_entry = this->get_entry(key, &error);

	if (error != NULL && !existing_entry &&
		isi_system_error_is_a(error, ENOENT))
	{
		/* if there's an error, but it's expected,
		 * free it and bail out (no more work to do)
		 */
		isi_error_free(error);
		error = NULL;
		goto out;
	}
	else if (error == NULL && !existing_entry)
	{
		/* if there is no error, but there should have been,
		 * create it now
		 */
		error = isi_system_error_new(EDOOFUS,
									 "retrieved unexpected entry: %p", curr_entry);
	}

	ON_ISI_ERROR_GOTO(out, error);

	curr_entry->get_serialized_entry(curr_serialization,
									 curr_serialization_size);

	entry = new isi_cloud::ooi_entry(key,
									 serialization, serialization_size, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ASSERT(entry);

	if ((curr_serialization_size != serialization_size) ||
		memcmp(curr_serialization, serialization, serialization_size))
	{
		error = isi_system_error_new(ERANGE,
									 "Current ooi entry does not match expected");
		/*
		 * Get details
		 */
		ooi_entry_difference_set(error, *curr_entry, *entry);
		goto out;
	}

out:
	isi_error_handle(error, error_out);

	if (entry)
		delete entry;
	if (curr_entry)
		delete curr_entry;
}
