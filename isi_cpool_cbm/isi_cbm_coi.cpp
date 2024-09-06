#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>

#include <time.h>
#include <stdlib.h>

#include <isi_config/ifsconfig.h>
#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_cpool_d_common/ifs_cpool_flock.h>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util_cpp/isi_exception.h>

#include "isi_cbm_coi_sbt.h"
#include "isi_cbm_error.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_ooi.h"
#include "isi_cbm_pack_unpack.h"
#include "isi_cbm_snapshot.h"

#include "isi_cbm_coi.h"

using namespace isi_cloud;
static const size_t MAX_CMOI_ENTRY_SZ = 256;
void object_id_to_hsbt_key(const isi_cloud_object_id &id, coi_sbt_key &key)
{
	key.high_ = id.get_high();
	key.low_ = id.get_low();
	key.snapid_ = id.get_snapid();
}

void hsbt_key_to_object_id(const coi_sbt_key &key, isi_cloud_object_id &id)
{
	id.set(key.high_, key.low_);
	id.set_snapid(key.snapid_);
}

void object_id_to_sbt_key(const isi_cloud_object_id &id, btree_key_t &key)
{
	// Only the object ID prefix is interesting to us:
	key.keys[0] = id.get_high();
	key.keys[1] = id.get_low();
}

void hsbt_key_to_object_id(const btree_key_t &key, isi_cloud_object_id &id)
{
	id.set(key.keys[0], key.keys[1]);
	id.set_snapid(0);
}

const btree_key_t coi_entry::NULL_BTREE_KEY = {{0, 0}};
const btree_key_t coi_entry::GC_BTREE_KEY = {{-1, -1}};

coi_entry::coi_entry()
	: version_(CPOOL_COI_VERSION), archive_account_id_(-1, ""), cmo_container_(NULL), compressed_(false), checksum_(false), backed_(false), serialization_up_to_date_(false), serialized_entry_size_(0), serialized_entry_(NULL), saved_serialization_size_(0), saved_serialization_(NULL)
{
	object_id_.set(0, 0);
	bzero(archiving_cluster_id_, sizeof archiving_cluster_id_);
	memset(&co_date_of_death_, 0, sizeof(co_date_of_death_));
	reset_ooi_entry_key();
}

coi_entry::~coi_entry()
{
	if (cmo_container_ != NULL)
		delete[] cmo_container_;

	if (serialized_entry_)
		free(serialized_entry_);

	if (saved_serialization_)
		free(saved_serialization_);
}

void coi_entry::set_object_id(const isi_cloud_object_id &id)
{
	object_id_ = id;

	serialization_up_to_date_ = false;
}

const isi_cloud_object_id &
coi_entry::get_object_id(void) const
{
	return object_id_;
}

uint32_t
coi_entry::get_version(void) const
{
	return version_;
}

void coi_entry::set_archiving_cluster_id(const uint8_t id[IFSCONFIG_GUID_SIZE])
{
	memcpy(archiving_cluster_id_, id, IFSCONFIG_GUID_SIZE);

	serialization_up_to_date_ = false;
}

const uint8_t *
coi_entry::get_archiving_cluster_id(void) const
{
	return archiving_cluster_id_;
}

void coi_entry::set_archive_account_id(const isi_cbm_id &id)
{
	archive_account_id_ = id;

	serialization_up_to_date_ = false;
}

const isi_cbm_id &
coi_entry::get_archive_account_id(void) const
{
	return archive_account_id_;
}

void coi_entry::set_cmo_container(const char *cmo_container)
{
	ASSERT(cmo_container != NULL);

	if (cmo_container_ != NULL)
		delete[] cmo_container_;

	cmo_container_ = new char[strlen(cmo_container) + 1];
	ASSERT(cmo_container_ != NULL);
	strncpy(cmo_container_, cmo_container, strlen(cmo_container) + 1);

	serialization_up_to_date_ = false;
}

const char *
coi_entry::get_cmo_container(void) const
{
	return cmo_container_;
}

void coi_entry::set_compressed(isi_tri_bool compressed)
{
	if (compressed_ != compressed)
	{
		compressed_ = compressed;

		serialization_up_to_date_ = false;
	}
}

bool coi_entry::is_backed(void) const
{
	return backed_;
}

void coi_entry::set_backed(bool backed)
{
	if (backed_ != backed)
	{
		backed_ = backed;

		serialization_up_to_date_ = false;
	}
}

isi_tri_bool
coi_entry::is_compressed(void) const
{
	return compressed_;
}

void coi_entry::set_checksum(isi_tri_bool checksum)
{
	if (checksum_ != checksum)
	{
		checksum_ = checksum;

		serialization_up_to_date_ = false;
	}
}

isi_tri_bool
coi_entry::has_checksum() const
{
	return checksum_;
}

void coi_entry::update_co_date_of_death(const timeval &date_of_death, bool force)
{
	if (force || timercmp(&date_of_death, &co_date_of_death_, >))
	{
		co_date_of_death_ = date_of_death;

		serialization_up_to_date_ = false;
	}
}

struct timeval
coi_entry::get_co_date_of_death(void) const
{
	return co_date_of_death_;
}

int coi_entry::get_reference_count(void) const
{
	return referencing_lins_.size();
}

const std::set<ifs_lin_t> &
coi_entry::get_referencing_lins(void) const
{
	return referencing_lins_;
}

void coi_entry::add_referencing_lin(ifs_lin_t lin, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	std::pair<std::set<ifs_lin_t>::iterator, bool> ret =
		referencing_lins_.insert(lin);
	if (!ret.second)
	{
		/*
		 * (This really should be EEXIST, but EALREADY was being used
		 * by the pre-refactored code.)
		 */
		error = isi_system_error_new(EALREADY,
									 "specified LIN (%#{}) already exists "
									 "in set of referencing LINs",
									 lin_fmt(lin));
		goto out;
	}

	serialization_up_to_date_ = false;

out:
	isi_error_handle(error, error_out);
}

bool coi_entry::remove_referencing_lin(ifs_lin_t lin, ifs_snapid_t snap_id,
									   struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	bool reference_removed = false, used_by_other_snaps = false;

	/*
	 * First determine if the LIN has snapshots; if it does, we can't
	 * remove the LIN from the set of LINs which reference the cloud
	 * objects.
	 */
	used_by_other_snaps = cbm_snap_used_by_other_snaps(lin, snap_id,
													   archive_account_id_, object_id_, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!used_by_other_snaps)
	{
		if (referencing_lins_.erase(lin) == 0)
		{
			error = isi_cbm_error_new(CBM_LIN_DOES_NOT_EXIST,
									  "specified LIN (%{}) does not exist "
									  "in set of referencing LINs",
									  lin_fmt(lin));
			goto out;
		}

		reference_removed = true;
		serialization_up_to_date_ = false;
	}
	else
	{
		ilog(IL_DEBUG, "specified LIN/snapID (%{}) "
					   "won't be removed from the set of referencing LINs "
					   "for object ID (%s %lu) because "
					   "it is being used by other snaps",
			 lin_snapid_fmt(lin, snap_id), object_id_.to_c_string(),
			 object_id_.get_snapid());
	}

out:
	isi_error_handle(error, error_out);

	return reference_removed;
}

void coi_entry::set_ooi_entry_key(const btree_key_t &key)
{
	ooi_entry_key_ = key;

	serialization_up_to_date_ = false;
}

void coi_entry::reset_ooi_entry_key(void)
{
	set_ooi_entry_key(NULL_BTREE_KEY);
}

const btree_key_t &
coi_entry::get_ooi_entry_key(void) const
{
	return ooi_entry_key_;
}

void coi_entry::set_serialized_entry(uint32_t expected_version, const void *entry,
									 size_t size, isi_error **error_out)
{
	isi_error *error = NULL;

	if (size > serialized_entry_size_)
	{
		if (serialized_entry_size_ != 0)
			free(serialized_entry_);
		serialized_entry_ = malloc(size);
		ASSERT(serialized_entry_ != NULL);
	}

	serialized_entry_size_ = size;
	memcpy(serialized_entry_, entry, serialized_entry_size_);

	deserialize(expected_version, &error);
	if (error)
	{
		free(serialized_entry_);
		serialized_entry_ = 0;
		serialized_entry_size_ = 0;
		goto out;
	}

	serialization_up_to_date_ = true;

out:
	isi_error_handle(error, error_out);
}

void coi_entry::get_serialized_entry(const void *&entry, size_t &size)
{
	if (!serialization_up_to_date_)
		serialize();

	entry = serialized_entry_;
	size = serialized_entry_size_;
}

void coi_entry::save_serialization(void) const
{
	free(saved_serialization_);

	saved_serialization_ = malloc(serialized_entry_size_);
	ASSERT(saved_serialization_ != NULL);

	memcpy(saved_serialization_, serialized_entry_,
		   serialized_entry_size_);
	saved_serialization_size_ = serialized_entry_size_;
}

void coi_entry::get_saved_serialization(const void *&serialization,
										size_t &size) const
{
	serialization = saved_serialization_;
	size = saved_serialization_size_;
}

void coi_entry::serialize(void)
{
	if (serialized_entry_ != NULL)
	{
		free(serialized_entry_);
		serialized_entry_ = NULL;
	}

	size_t cmo_container_length = strlen(cmo_container_);
	size_t ref_count = referencing_lins_.size();

	serialized_entry_size_ =
		sizeof version_ +
		object_id_.get_packed_size() +
		sizeof archiving_cluster_id_ +
		archive_account_id_.get_pack_size() +
		sizeof(cmo_container_length) +
		cmo_container_length +
		compressed_.get_pack_size() +
		checksum_.get_pack_size() +
		sizeof backed_ +
		sizeof co_date_of_death_ +
		sizeof ref_count +
		ref_count * sizeof(ifs_lin_t) +
		sizeof ooi_entry_key_;

	serialized_entry_ = malloc(serialized_entry_size_);
	ASSERT(serialized_entry_ != NULL);

	// since pack advances the dst pointer, use a copy to maintain
	// the correct serialized_task pointer
	void *temp = serialized_entry_;

	pack(&temp, &version_, sizeof version_);
	object_id_.pack(&temp);
	pack(&temp, archiving_cluster_id_, sizeof archiving_cluster_id_);
	archive_account_id_.pack((char **)&temp);
	pack(&temp, &cmo_container_length, sizeof cmo_container_length);
	pack(&temp, cmo_container_, cmo_container_length);
	compressed_.pack(&temp);
	checksum_.pack(&temp);
	pack(&temp, &backed_, sizeof backed_);
	pack(&temp, &co_date_of_death_,
		 sizeof co_date_of_death_);
	pack(&temp, &ref_count, sizeof ref_count);
	std::set<ifs_lin_t>::const_iterator iter = referencing_lins_.begin();
	for (; iter != referencing_lins_.end(); ++iter)
		pack(&temp, &(*iter), sizeof(*iter));
	pack(&temp, &(ooi_entry_key_.keys[0]), sizeof ooi_entry_key_.keys[0]);
	pack(&temp, &(ooi_entry_key_.keys[1]), sizeof ooi_entry_key_.keys[1]);

	/*
	 * Verify that the number of bytes we've packed equals the expected
	 * number.
	 */
	size_t num_packed_bytes = (char *)temp - (char *)serialized_entry_;
	ASSERT(num_packed_bytes == serialized_entry_size_);

	serialization_up_to_date_ = true;
}

void coi_entry::deserialize(uint32_t expected_version, isi_error **error_out)
{
	isi_error *error = NULL;
	size_t cmo_container_length, temp_ref_count;
	ifs_lin_t temp_lin;
	size_t correct_serialized_size = 0;
	if (cmo_container_ != NULL)
		delete[] cmo_container_;

	// clear the referencing lin set since that doesn't get overwritten
	// like the rest of the member data
	referencing_lins_.clear();

	// since unpack advances the dst pointer, use a copy to maintain
	// the correct serialized_task pointer
	void *temp = serialized_entry_;

	unpack(&temp, &version_, sizeof version_);
	// Check if the version number is compatible.
	if (expected_version != get_version())
	{
		error = isi_cbm_error_new(CBM_COI_VERSION_MISMATCH,
								  "COI Version mismatch. expected version %d found version %d",
								  expected_version, get_version());
		goto out;
	}

	object_id_.unpack(&temp);
	unpack(&temp, archiving_cluster_id_, sizeof archiving_cluster_id_);
	archive_account_id_.unpack((char **)&temp);

	// we need to know the size of cmo_container_ before we
	// can deserialize, since we need to allocate
	unpack(&temp, &cmo_container_length, sizeof cmo_container_length);
	cmo_container_ = new char[cmo_container_length + 1];
	ASSERT(cmo_container_ != NULL);
	unpack(&temp, cmo_container_, cmo_container_length);
	cmo_container_[cmo_container_length] = '\0';

	compressed_.unpack(&temp);
	checksum_.unpack(&temp);
	unpack(&temp, &backed_, sizeof backed_);
	unpack(&temp, &co_date_of_death_, sizeof co_date_of_death_);
	unpack(&temp, &temp_ref_count, sizeof temp_ref_count);
	for (size_t i = 0; i < temp_ref_count; ++i)
	{
		unpack(&temp, &temp_lin, sizeof temp_lin);
		add_referencing_lin(temp_lin, &error);
		ASSERT(error == NULL, "failed to add LIN (i = %zu): %#{}",
			   i, isi_error_fmt(error));
	}
	unpack(&temp, &(ooi_entry_key_.keys[0]),
		   sizeof ooi_entry_key_.keys[0]);
	unpack(&temp, &(ooi_entry_key_.keys[1]),
		   sizeof ooi_entry_key_.keys[1]);

	// verify that we have a coherent COI entry by checking the size of the
	// supplied buffer and the size of this COI entry (which we now know
	// because we have the sizes of the serialized data and checkpoint
	// data)
	correct_serialized_size =
		sizeof version_ +
		object_id_.get_packed_size() +
		sizeof archiving_cluster_id_ +
		archive_account_id_.get_pack_size() +
		sizeof cmo_container_length +
		cmo_container_length +
		compressed_.get_pack_size() +
		checksum_.get_pack_size() +
		sizeof backed_ +
		sizeof co_date_of_death_ +
		sizeof temp_ref_count +
		temp_ref_count * sizeof(ifs_lin_t) +
		sizeof ooi_entry_key_;

	ASSERT(serialized_entry_size_ >= correct_serialized_size);

out:
	isi_error_handle(error, error_out);
}

cmoi_entry::cmoi_entry()
	: magic_(CMOI_ENTRY_MAGIC), version_(CPOOL_CMOI_VERSION), version_count_(0), syncing_(false), highest_ver_(0), next_syncer_id_(0), syncer_id_(0)
{
}
const size_t
cmoi_entry::get_packed_size()
{
	static cmoi_entry dummy;

	return sizeof(dummy.magic_) +
		   sizeof(dummy.version_) + sizeof(dummy.version_count_) +
		   sizeof(dummy.syncing_) + sizeof(dummy.highest_ver_) +
		   sizeof(dummy.next_syncer_id_) +
		   sizeof(dummy.syncer_id_);
}

void cmoi_entry::deserialize(const void *buffer, size_t len,
							 struct isi_error **error_out)
{
	isi_error *error = NULL;
	void *temp = (void *)buffer;
	const size_t pack_sz = get_packed_size();

	if (len != pack_sz)
	{
		error = isi_cbm_error_new(CBM_CMOI_WRONG_LENGTH,
								  "CMOI Entry Length is not expected. The input size %lu "
								  "is different from smaller the expected %lu",
								  len, pack_sz);
		goto out;
	}

	unpack(&temp, &magic_, sizeof magic_);

	if (CMOI_ENTRY_MAGIC != magic_)
	{
		error = isi_cbm_error_new(CBM_CMOI_CORRUPT_ENTRY,
								  "CMOI Entry magic numbers mismatch. expected: %x "
								  "input: %x",
								  CMOI_ENTRY_MAGIC, magic_);
		goto out;
	}

	unpack(&temp, &version_, sizeof version_);

	if (CPOOL_CMOI_VERSION != version_)
	{
		error = isi_cbm_error_new(CBM_CMOI_VERSION_MISMATCH,
								  "CMOI Entry Version mismatch. Version expected: %u, "
								  "input version:  %u",
								  CPOOL_CMOI_VERSION, version_);
		goto out;
	}

	unpack(&temp, &version_count_, sizeof version_count_);
	unpack(&temp, &syncing_, sizeof syncing_);
	unpack(&temp, &highest_ver_, sizeof highest_ver_);
	unpack(&temp, &next_syncer_id_, sizeof next_syncer_id_);
	unpack(&temp, &syncer_id_, sizeof syncer_id_);

out:
	isi_error_handle(error, error_out);
}

void cmoi_entry::serialize(std::vector<char> &buffer)
{
	const size_t pack_sz = get_packed_size();

	buffer.resize(pack_sz);
	void *temp = (void *)buffer.data();

	pack(&temp, &magic_, sizeof magic_);
	pack(&temp, &version_, sizeof version_);
	pack(&temp, &version_count_, sizeof version_count_);
	pack(&temp, &syncing_, sizeof syncing_);
	pack(&temp, &highest_ver_, sizeof highest_ver_);
	pack(&temp, &next_syncer_id_, sizeof next_syncer_id_);
	pack(&temp, &syncer_id_, sizeof syncer_id_);
}

void cmoi_entry::increment_version_count()
{
	++version_count_;
}

void cmoi_entry::decrement_version_count()
{
	--version_count_;
}

void cmoi_entry::increment_next_syncer_id()
{
	++next_syncer_id_;
}

void cmoi_entry::set_syncer_id(uint64_t syncer_id)
{
	syncer_id_ = syncer_id;
}

coi::coi()
	: initialized_(false), sbt_fd_(-1), cmo_sbt_fd_(-1), coi_lock_fd_(-1), release_version_(CPOOL_COI_VERSION)
{
}

coi::~coi()
{
	if (sbt_fd_ != -1)
		close_sbt(sbt_fd_);
	if (cmo_sbt_fd_ != -1)
		close_sbt(cmo_sbt_fd_);
	if (coi_lock_fd_ != -1)
	{
		close(coi_lock_fd_);
		coi_lock_fd_ = -1;
	}
}

void coi::initialize(struct isi_error **error_out)
{
	ASSERT(!initialized_);

	struct isi_error *error = NULL;
	const char *coi_sbt_name = get_coi_sbt_name();
	const char *cmoi_sbt_name = get_cmoi_sbt_name();
	static const char *lock_file_buf =
		CPOOL_LOCKS_DIRECTORY "isi_cpool_d.coi.lock";
	sbt_fd_ = open_sbt(coi_sbt_name, true, &error); ////创建coi这个sbt树
	if (error != NULL)
	{
		isi_error_add_context(error, "failed to initialize COI");
		goto out;
	}

	cmo_sbt_fd_ = open_sbt(cmoi_sbt_name, true, &error); ////创建cmoi这个sbt树
	if (error != NULL)
	{
		isi_error_add_context(error, "failed to initialize CMOI");
		goto out;
	}

	coi_lock_fd_ = open(lock_file_buf, O_RDONLY | O_CREAT, 0744); /////创建关于coi的文件锁

	if (coi_lock_fd_ < 0)
	{
		error = isi_system_error_new(errno,
									 "Error opening move lock file %s",
									 lock_file_buf);
		goto out;
	}

	initialized_ = true;
out:
	if (!initialized_)
	{
		if (cmo_sbt_fd_ != -1)
		{
			close_sbt(cmo_sbt_fd_);
			cmo_sbt_fd_ = -1;
		}
		if (sbt_fd_ != -1)
		{
			close_sbt(sbt_fd_);
			sbt_fd_ = -1;
		}
		if (coi_lock_fd_ != -1)
		{
			close(coi_lock_fd_);
			coi_lock_fd_ = -1;
		}
	}
	isi_error_handle(error, error_out);
}

uint32_t
coi::get_release_version(void)
{
	return release_version_;
}

void coi::set_release_version(uint32_t in_release_version)
{
	release_version_ = in_release_version;
}

//////拿coi entry: 将cloud object id转化为coi_sbt_key, 使用这个key和sbt_fd调用coi_sbt_get_entry
coi_entry *
coi::get_entry(const isi_cloud_object_id &object_id,
			   struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	coi_entry *ent = NULL;

	bool entry_found = false;
	size_t entry_size = MAX_CMOI_ENTRY_SZ;
	char *entry_buf = NULL;
	struct coi_sbt_key hsbt_key;

	object_id_to_hsbt_key(object_id, hsbt_key);

	do
	{
		entry_buf = new char[entry_size];
		ASSERT(entry_buf != NULL);

		entry_found = coi_sbt_get_entry(sbt_fd_, &hsbt_key,
										entry_buf, &entry_size, NULL, &error);
		if (entry_found && error == NULL)
		{
			ent = new coi_entry;
			ASSERT(ent != NULL);

			ent->set_serialized_entry(CPOOL_COI_VERSION, entry_buf,
									  entry_size, &error);
			if (error != NULL)
			{
				delete ent;
				ent = NULL;
			}
			break;
		}

		/*
		 * We can handle two types of errors: ENOENT and non-ENOSPC.
		 * Since hsbt_get_entry doesn't set error when the entry isn't
		 * found, treat that as ENOENT.
		 */
		if (error == NULL)
		{
			error = isi_system_error_new(ENOENT,
										 "no COI entry found for object ID (%s, %ld)",
										 object_id.to_c_string(), object_id.get_snapid());
			break;
		}

		if (isi_system_error_is_a(error, ENOENT))
		{
			isi_error_add_context(error,
								  "no COI entry found for object ID (%s, %ld)",
								  object_id.to_c_string(), object_id.get_snapid());
			break;
		}

		if (!isi_system_error_is_a(error, ENOSPC))
		{
			isi_error_add_context(error,
								  "unanticipated error "
								  "while attempting to retrieve COI entry "
								  "for cloud object ID (%s, %ld)",
								  object_id.to_c_string(), object_id.get_snapid());
			break;
		}

		/*
		 * ENOSPC means we need a bigger buffer, but we know exactly
		 * how big, thanks to entry_size.  All we need to do here in
		 * preparation for the next loop is free the error and delete
		 * the too-small buffer.
		 */
		isi_error_free(error);
		error = NULL;

		delete[] entry_buf;
	} while (true);

	delete[] entry_buf;
	entry_buf = NULL;

	isi_error_handle(error, error_out);

	return ent;
}

void coi::get_cmo(const isi_cloud_object_id &object_id, cmoi_entry &cmo_entry,
				  struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;

	size_t entry_size = MAX_CMOI_ENTRY_SZ;
	btree_key_t sbt_key;
	struct btree_flags flags = {0};
	std::vector<char> entry_buf;
	size_t sz_out = 0;

	object_id_to_sbt_key(object_id, sbt_key);

	entry_buf.resize(entry_size);
	int ret = ifs_sbt_get_entry_at(cmo_sbt_fd_, &sbt_key,
								   entry_buf.data(), entry_size, &flags, &sz_out);

	if (ret)
	{
		error = isi_system_error_new(errno,
									 "Error in finding the object ID %#{}",
									 isi_cloud_object_id_fmt(&object_id));
		goto out;
	}

	cmo_entry.deserialize(entry_buf.data(), sz_out, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:

	isi_error_handle(error, error_out);
}

/////根据cloud object id,把coi entry和cmo entry加入到coi中
void coi::add_object(const isi_cloud_object_id &object_id,
					 const uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE],
					 const isi_cbm_id &archive_account_id, const char *cmo_container,
					 isi_tri_bool is_compressed, isi_tri_bool checksum,
					 ifs_lin_t referencing_lin,
					 struct isi_error **error_out,
					 struct timeval *date_of_death_ptr, bool *backed_up)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	coi_entry new_entry;
	const void *serialized_new_entry;
	size_t serialized_new_entry_size;

	coi_sbt_key hsbt_key;
	btree_key_t cmo_key;

	object_id_to_hsbt_key(object_id, hsbt_key);
	object_id_to_sbt_key(object_id, cmo_key);
	struct coi_sbt_bulk_entry bulk_ops[2];
	struct btree_flags flags = {};

	struct timeval date_of_death = {0};
	bool new_cmo = false;
	std::vector<char> cmo_entry_buf;
	cmoi_entry cmoi_entry;

	if (date_of_death_ptr)
	{
		date_of_death = *date_of_death_ptr;
	}

	get_cmo(object_id, cmoi_entry, &error);

	if (error && isi_system_error_is_a(error, ENOENT))
	{
		// The CMO index is not added yet
		isi_error_free(error);
		error = NULL;
		new_cmo = true;
	}

	ON_ISI_ERROR_GOTO(out, error);

	if (cmoi_entry.get_highest_ver() < object_id.get_snapid())
	{
		cmoi_entry.set_highest_ver(object_id.get_snapid());
	}

	cmoi_entry.increment_version_count();

	new_entry.set_object_id(object_id);
	new_entry.set_archiving_cluster_id(archiving_cluster_id);
	new_entry.set_archive_account_id(archive_account_id);
	new_entry.set_cmo_container(cmo_container);
	new_entry.set_compressed(is_compressed);
	new_entry.set_checksum(checksum);
	new_entry.update_co_date_of_death(date_of_death, false);
	if (referencing_lin > 0)
	{
		new_entry.add_referencing_lin(referencing_lin, &error);
		if (error != NULL && isi_system_error_is_a(error, EALREADY))
		{
			isi_error_free(error);
			error = NULL;
		}
		ON_ISI_ERROR_GOTO(out, error);
	}
	if (backed_up)
	{
		new_entry.set_backed(*backed_up);
	}
	new_entry.get_serialized_entry(serialized_new_entry,
								   serialized_new_entry_size);

	bzero(&bulk_ops[0], sizeof bulk_ops);

	// add operation for the COI entry
	////添加coi entry(包括coi这颗sbt的fd,coi entry的字节流,coi entry的key(由cloud object id转化而来))
	bulk_ops[0].bulk_entry.fd = sbt_fd_;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_ADD;
	bulk_ops[0].bulk_entry.entry_buf = (void *)serialized_new_entry;
	bulk_ops[0].bulk_entry.entry_size = serialized_new_entry_size;
	bulk_ops[0].bulk_entry.entry_flags = flags;
	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &hsbt_key;
	bulk_ops[0].key_len = sizeof(hsbt_key);

	cmoi_entry.serialize(cmo_entry_buf);
	// operation for the CMO
	////添加cmo entry到cmo sbt中: 需要包括cmo sbt的fd,cloud object id转化成的cmo key, cmo entry的字节流
	bulk_ops[1].bulk_entry.fd = cmo_sbt_fd_;
	bulk_ops[1].bulk_entry.key = cmo_key;

	if (new_cmo)
		bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_ADD;
	else
		bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_MOD;

	bulk_ops[1].bulk_entry.entry_buf = (void *)cmo_entry_buf.data();
	bulk_ops[1].bulk_entry.entry_size = cmo_entry_buf.size();
	bulk_ops[1].bulk_entry.entry_flags = flags;
	bulk_ops[1].is_hash_btree = false;

	coi_sbt_bulk_op(2, bulk_ops, &error);

out:
	if (error != NULL)
	{
		isi_error_add_context(error,
							  "Failed to add object (%s, %ld) to COI for lin: %{}, "
							  "new cmo: %d",
							  object_id.to_c_string(), object_id.get_snapid(),
							  lin_fmt(referencing_lin), new_cmo);
	}
	else
	{
		ilog(IL_DEBUG, "Added object (%s, %ld) to COI for lin: %{}, "
					   "new cmo: %d",
			 object_id.to_c_string(), object_id.get_snapid(),
			 lin_fmt(referencing_lin), new_cmo);
	}

	isi_error_handle(error, error_out);
}

void coi::remove_object(const isi_cloud_object_id &object_id, bool ignore_ref_count,
						struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	coi_sbt_key hsbt_key;
	btree_key_t cmo_key;

	coi_entry *entry = NULL;
	cmoi_entry cmoi_entry;
	struct coi_sbt_bulk_entry bulk_ops[2];

	object_id_to_hsbt_key(object_id, hsbt_key);
	object_id_to_sbt_key(object_id, cmo_key);

	bool delete_cmoi_entry = false;
	std::vector<char> cmo_entry_buf;

	if (!ignore_ref_count) ////考虑引用这个coi entry的reference count,那么引用计数>0,就不删除
	{
		entry = get_entry(object_id, &error);
		if (error != NULL)
		{
			isi_error_add_context(error,
								  "failed to get COI entry for object ID (%s, %ld)",
								  object_id.to_c_string(), object_id.get_snapid());
			goto out;
		}

		/*
		 * get_entry guarantees that an entry is returned if error is
		 * NULL, and since we've already accounted for the non-NULL
		 * error above, we can safely dereference entry.
		 */
		if (entry->get_reference_count() != 0)
		{
			error = isi_system_error_new(EINVAL,
										 "Failed to get COI entry for object ID (%s, %ld), "
										 "reference count is non-zero: %d",
										 object_id.to_c_string(), object_id.get_snapid(),
										 entry->get_reference_count());
			goto out;
		}
	}

	get_cmo(object_id, cmoi_entry, &error);

	if (error && isi_system_error_is_a(error, ENOENT))
	{
		ilog(IL_DEBUG, "CMOI entry for object (%s, %ld) is not found."
					   "Skipped.",
			 object_id.to_c_string(), object_id.get_snapid());
		isi_error_free(error);
		error = NULL;
		goto out;
	}
	ON_ISI_ERROR_GOTO(out, error);

	cmoi_entry.decrement_version_count();

	if (cmoi_entry.get_version_count() == 0)
	{
		// when reference count drops to 0, delete
		delete_cmoi_entry = true;
	}

	bzero(&bulk_ops[0], sizeof bulk_ops);

	// delete operation for the COI entry
	////删除COI entry
	bulk_ops[0].bulk_entry.fd = sbt_fd_;
	bulk_ops[0].bulk_entry.op_type = SBT_SYS_OP_DEL;
	bulk_ops[0].is_hash_btree = true;
	bulk_ops[0].key = &hsbt_key;
	bulk_ops[0].key_len = sizeof(hsbt_key);

	cmoi_entry.serialize(cmo_entry_buf);
	// operation for the CMO
	////删除CMO entry,也可能不删除
	bulk_ops[1].bulk_entry.fd = cmo_sbt_fd_;
	bulk_ops[1].bulk_entry.key = cmo_key;

	if (delete_cmoi_entry)
	{
		bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_DEL;
	}
	else
	{
		bulk_ops[1].bulk_entry.op_type = SBT_SYS_OP_MOD;
		bulk_ops[1].bulk_entry.entry_buf = (void *)cmo_entry_buf.data();
		bulk_ops[1].bulk_entry.entry_size = cmo_entry_buf.size();
	}
	bulk_ops[1].is_hash_btree = false;

	coi_sbt_bulk_op(2, bulk_ops, &error);

	if (error && isi_system_error_is_a(error, ENOENT))
	{
		ilog(IL_DEBUG, "COI entry for object (%s, %ld) is not found."
					   "Skipped.",
			 object_id.to_c_string(), object_id.get_snapid());
		isi_error_free(error);
		error = NULL;
		goto out;
	}

	if (error != NULL)
	{
		isi_error_add_context(error);
		goto out;
	}
	else
	{
		ilog(IL_DEBUG, "Removed object (%s, %ld) from COI.",
			 object_id.to_c_string(), object_id.get_snapid());
	}
out:
	if (entry != NULL)
		delete entry;

	isi_error_handle(error, error_out);
}

void ooi_coi_bulk_op_wrap(isi_cloud::ooi &ooi, isi_cloud::coi &coi,
						  struct coi_sbt_bulk_entry coi_sbt_ops[],
						  const size_t sbt_ops_num,
						  const btree_key_t valid_ooi_entry_keys[], const isi_cloud_object_id &object_id,
						  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t ooi_entry_key = valid_ooi_entry_keys[0];
	btree_key_t ooi_entry_key_to_remove;
	int i = 0;
	// only expect one or two ooi sbt ops
	ASSERT(sbt_ops_num == 2 || sbt_ops_num == 3, "sbt_ops_num is %lu", sbt_ops_num);

	ASSERT(coi_sbt_ops[0].is_hash_btree == true);
	ASSERT(coi_sbt_ops[1].is_hash_btree == false);

	if (sbt_ops_num == 3)
	{
		ASSERT(coi_sbt_ops[2].is_hash_btree == false);
		ooi_entry_key_to_remove = valid_ooi_entry_keys[1];
	}
	/////执行add_ref, remove_ref
	coi_sbt_bulk_op(sbt_ops_num, coi_sbt_ops, &error);
	if (error)
	{
		const void *ooi_old_serialization, *coi_old_serialization;
		size_t ooi_old_serialization_size, coi_old_serialization_size;
		struct isi_error *error1 = NULL;

		isi_error_add_context(error,
							  "failed to perform OOI & COI bulk operation"
							  " for object ID (%s %lu)",
							  object_id.to_c_string(), object_id.get_snapid());
		if (!isi_system_error_is_a(error, ERANGE))
			goto out;

		// check if coi entry has changed
		coi_old_serialization =
			coi_sbt_ops[0].bulk_entry.old_entry_buf;
		coi_old_serialization_size =
			coi_sbt_ops[0].bulk_entry.old_entry_size;
		coi.validate_entry(object_id, coi_old_serialization,
						   coi_old_serialization_size, &error1);
		if (error1)
		{
			isi_error_add_context(error, ": %s",
								  isi_error_get_detailed_message(error1));
			isi_error_free(error1);
			error1 = NULL;
		}

		// check if all the ooi has changed
		for (i = 1; i < sbt_ops_num; ++i)
		{
			if (coi_sbt_ops[i].bulk_entry.op_type == SBT_SYS_OP_DEL)
			{
				ooi.validate_entry(ooi_entry_key_to_remove, NULL,
								   0, false, &error1);
				if (error1)
				{
					isi_error_add_context(error, ": %s",
										  isi_error_get_detailed_message(error1));
					isi_error_free(error1);
					error1 = NULL;
				}
			}
			else
			{
				ooi_old_serialization =
					coi_sbt_ops[i].bulk_entry.old_entry_buf;
				ooi_old_serialization_size =
					coi_sbt_ops[i].bulk_entry.old_entry_size;
				ooi.validate_entry(ooi_entry_key, ooi_old_serialization,
								   ooi_old_serialization_size, true, &error1);
				if (error1)
				{
					isi_error_add_context(error, ": %s",
										  isi_error_get_detailed_message(error1));
					isi_error_free(error1);
					error1 = NULL;
				}
			}
		}

		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * A convenience function to perform coi conditional mod operation and
 * handle emitted error
 */
static void
coi_sbt_cond_mod_entry_wrap(isi_cloud::coi &coi,
							const isi_cloud_object_id &object_id, int fd,
							const struct coi_sbt_key &key,
							const void *entry_buf, size_t entry_size,
							const void *old_entry_buf, size_t old_entry_size,
							struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	coi_sbt_cond_mod_entry(fd, &key,
						   entry_buf, entry_size, NULL,
						   BT_CM_BUF, old_entry_buf,
						   old_entry_size, NULL, &error);
	if (error)
	{
		struct isi_error *error1 = NULL;

		isi_error_add_context(error,
							  "failed to update COI entry for object ID (%s %lu)",
							  object_id.to_c_string(), object_id.get_snapid());
		if (!isi_system_error_is_a(error, ERANGE))
			goto out;

		coi.validate_entry(object_id, old_entry_buf,
						   old_entry_size, &error1);
		if (error1)
		{
			isi_error_add_context(error, ": %s",
								  isi_error_get_detailed_message(error1));
			isi_error_free(error1);
			error1 = NULL;
		}
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}
//////被isi_cbm_gc.cpp调用 对将要被删除(reference count==0)的cloud object做一个标记: 设置coi_entry::GC_BTREE_KEY
void coi::removing_object(const isi_cloud_object_id &object_id,
						  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	coi_entry *coi_entry = NULL;
	coi_sbt_key coi_sbt_key;
	btree_key_t ooi_entry_key;

	const void *coi_old_serialization = NULL, *coi_new_serialization = NULL;
	size_t coi_old_serialization_size = 0, coi_new_serialization_size = 0;

	coi_entry = get_entry(object_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get COI entry for object ID (%s, %ld)",
					  object_id.to_c_string(), object_id.get_snapid());

	if (coi_entry->get_reference_count() != 0)
	{
		error = isi_system_error_new(EINVAL,
									 "Failed to mark removal for object ID (%s, %ld), "
									 "reference count is non-zero: %d",
									 object_id.to_c_string(), object_id.get_snapid(),
									 coi_entry->get_reference_count());
		goto out;
	}

	coi_entry->save_serialization();

	object_id_to_hsbt_key(object_id, coi_sbt_key);
	ooi_entry_key = coi_entry->get_ooi_entry_key();

	// Check if we have already marked the entry for removal
	if (ooi_entry_key.keys[0] == coi_entry::GC_BTREE_KEY.keys[0] &&
		ooi_entry_key.keys[1] == coi_entry::GC_BTREE_KEY.keys[1])
	{
		ilog(IL_DEBUG,
			 "object ID (%s, %ld) is already marked for removal",
			 object_id.to_c_string(), object_id.get_snapid());
		goto out;
	}

	// [Else] mark it here
	//////做标记
	coi_entry->set_ooi_entry_key(coi_entry::GC_BTREE_KEY); /////????

	coi_entry->get_saved_serialization(coi_old_serialization,
									   coi_old_serialization_size);
	coi_entry->get_serialized_entry(coi_new_serialization,
									coi_new_serialization_size);

	coi_sbt_cond_mod_entry_wrap(*this, object_id, sbt_fd_,
								coi_sbt_key,
								coi_new_serialization, coi_new_serialization_size,
								coi_old_serialization, coi_old_serialization_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);

	if (coi_entry)
		delete coi_entry;
}

/////通过object id获得coi_entry,coi_entry->add_referencing_lin(referencing_lin);
void coi::add_ref(const isi_cloud_object_id &object_id, ifs_lin_t referencing_lin,
				  struct isi_error **error_out, struct timeval *date_of_death_ptr)
{
	printf("\n%s called\n", __func__);
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	bool coid_locked = false;
	coi_entry *coi_entry = NULL;

	coi_sbt_key coi_sbt_key;
	btree_key_t ooi_entry_key;

	const void *coi_old_serialization, *coi_new_serialization;
	size_t coi_old_serialization_size, coi_new_serialization_size;
	struct timeval date_of_death = {0};

	if (date_of_death_ptr)
	{
		date_of_death = *date_of_death_ptr;
	}

	/* true: block */
	lock_coid(object_id, true, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to lock COID (object ID: %s %lu LIN %#{})",
					  object_id.to_c_string(), object_id.get_snapid(),
					  lin_fmt(referencing_lin));
	coid_locked = true;

	coi_entry = get_entry(object_id, &error); /////拿这个object_id对应的coi entry
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get COI entry for object ID (%s %lu)",
					  object_id.to_c_string(), object_id.get_snapid());

	object_id_to_hsbt_key(object_id, coi_sbt_key);
	ooi_entry_key = coi_entry->get_ooi_entry_key();

	/*
	 * Don't add a reference if garbage collection has already
	 * begun. An attempt to add a reference  at this point indicates
	 * usage of the cloud object at another cluster beyond the retention
	 * time for the [unreferenced] cloud object as known to this cluster.
	 */
	////coi->removing_reference()标记正在remove
	//////如果这个coi entry已经在被gc,那么就不要做add reference了
	if (ooi_entry_key.keys[0] == coi_entry::GC_BTREE_KEY.keys[0] &&
		ooi_entry_key.keys[1] == coi_entry::GC_BTREE_KEY.keys[1])
	{
		ASSERT(coi_entry->get_reference_count() == 0, // for the new ref
			   "coi entry for object ID (%s %lu) ref count is %lu",
			   object_id.to_c_string(), object_id.get_snapid(),
			   coi_entry->get_reference_count());
		error = isi_cbm_error_new(CBM_INTEGRITY_FAILURE,
								  "failed to add LIN %#{} to coi entry for object ID (%s %lu)"
								  " since it is being garbage collected",
								  lin_fmt(referencing_lin), object_id.to_c_string(),
								  object_id.get_snapid());
		goto out;
	}

	coi_entry->save_serialization();

	coi_entry->add_referencing_lin(referencing_lin, &error); //////把引用加入到std::set中
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to add LIN %#{} to COI entry for object ID (%s %lu)",
					  lin_fmt(referencing_lin), object_id.to_c_string(),
					  object_id.get_snapid());

	coi_entry->update_co_date_of_death(date_of_death, false);

	coi_entry->get_saved_serialization(coi_old_serialization,
									   coi_old_serialization_size);

	while (coi_entry->get_reference_count() == 1 &&
		   (ooi_entry_key.keys[0] != coi_entry::NULL_BTREE_KEY.keys[0] ||
			ooi_entry_key.keys[1] != coi_entry::NULL_BTREE_KEY.keys[1]))
	{
		/*
		 * Since we have a new LIN that references the set of cloud
		 * objects, resurrect the set by removing the cloud object ID
		 * from the OOI.
		 */
		struct timeval ooi_ent_process_time;
		ifs_devid_t ooi_ent_devid;
		uint32_t ooi_ent_index;
		isi_cloud::ooi ooi;

		ooi.initialize(&error);
		ON_ISI_ERROR_GOTO(out, error, "failed to initialize OOI");

		/*
		 * Obtain the devid and process_time for the ooi entry group
		 * we want to lock and lock it.  This is needed to ensure that
		 * we obtain the same lock regardless of which node and for
		 * which coi_entry (that is associated with the same ooi_entry)
		 * this operation is being performed.
		 */
		ooi_entry::from_btree_key(ooi_entry_key, ooi_ent_process_time,
								  ooi_ent_devid, ooi_ent_index);
		ooi.lock_entry_group(ooi_ent_process_time, ooi_ent_devid,
							 &error);
		ON_ISI_ERROR_GOTO(out, error,
						  "failed to lock entry group (object ID: %s %lu lin %#{}) "
						  "devid %u",
						  object_id.to_c_string(), object_id.get_snapid(),
						  lin_fmt(referencing_lin), ooi_ent_devid);

		/* Prepare the bulk operation. */
		struct coi_sbt_bulk_entry coi_sbt_ops[2];
		memset(coi_sbt_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

		/*
		 * Prepare the "OOI-side" of the bulk operation: removing the
		 * cloud object with the specified date of death.
		 */
		ooi_entry *ooi_entry =
			ooi.prepare_for_cloud_object_removal(object_id,
												 ooi_entry_key, coi_sbt_ops[1], &error);
		if (error != NULL)
		{
			ooi.unlock_entry_group(ooi_ent_process_time,
								   ooi_ent_devid, isi_error_suppress(IL_ERR));

			if (!isi_system_error_is_a(error, ENOENT))
			{
				ON_ISI_ERROR_GOTO(out, error,
								  "failed to prepare ooi entry "
								  "for removing object ID (%s %lu) from ooi",
								  object_id.to_c_string(),
								  object_id.get_snapid());
			}

			/*
			 * No OOI entry found in the OOI; clear OOI entry key in
			 * the COI entry to reflect that.
			 * This can happen if the OOI entry has been deleted
			 * from the OOI and moved to a cloud_gc SBT.
			 */
			isi_error_free(error);
			error = NULL;
			ilog(IL_DEBUG,
				 "clearing missing ooi entry in "
				 "coi entry for object ID (%s %lu)",
				 object_id.to_c_string(), object_id.get_snapid());
			coi_entry->reset_ooi_entry_key();
			break;
		}
		ASSERT(ooi_entry != NULL);

		/*
		 * Reset the OOI entry's key to the COI entry, then prepare the
		 * "COI-side" of the bulk operation: modifying the COI entry.
		 */
		coi_entry->reset_ooi_entry_key();

		coi_entry->get_serialized_entry(coi_new_serialization,
										coi_new_serialization_size);

		coi_sbt_ops[0].is_hash_btree = true;
		coi_sbt_ops[0].key = &coi_sbt_key;
		coi_sbt_ops[0].key_len = sizeof coi_sbt_key;
		// ^ is this right?  shouldn't it take just the key (and not as a void *)
		coi_sbt_ops[0].bulk_entry.fd = sbt_fd_;
		coi_sbt_ops[0].bulk_entry.op_type = SBT_SYS_OP_MOD;
		coi_sbt_ops[0].bulk_entry.entry_buf =
			const_cast<void *>(coi_new_serialization);
		coi_sbt_ops[0].bulk_entry.entry_size =
			coi_new_serialization_size;
		coi_sbt_ops[0].bulk_entry.cm = BT_CM_BUF;
		coi_sbt_ops[0].bulk_entry.old_entry_buf =
			const_cast<void *>(coi_old_serialization);
		coi_sbt_ops[0].bulk_entry.old_entry_size =
			coi_old_serialization_size;

		/*
		 * Execute the bulk operation
		 */
		ooi_coi_bulk_op_wrap(ooi, *this, coi_sbt_ops, 2, &ooi_entry_key,
							 object_id, &error);

		ooi.unlock_entry_group(ooi_ent_process_time, ooi_ent_devid,
							   isi_error_suppress(IL_ERR));
		delete ooi_entry;
		goto out;
	}

	/*
	 * We aren't "resurrecting" any cloud objects, so just update
	 * the COI entry.
	 */
	coi_entry->get_serialized_entry(coi_new_serialization,
									coi_new_serialization_size);

	coi_sbt_cond_mod_entry_wrap(*this, object_id, sbt_fd_,
								coi_sbt_key,
								coi_new_serialization, coi_new_serialization_size,
								coi_old_serialization, coi_old_serialization_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);

	if (coid_locked)
		unlock_coid(object_id, isi_error_suppress(IL_ERR));

	if (coi_entry != NULL)
		delete coi_entry;
}

//////获得coi_entry,调用coi_entry->remove_referencing_lin(lin);
void coi::remove_ref(const isi_cloud_object_id &object_id, ifs_lin_t lin,
					 ifs_snapid_t snapid, struct timeval removed_ref_dod,
					 struct timeval *effective_dod,
					 struct isi_error **error_out)
{
	printf("%s called\n", __func__);
	ASSERT(initialized_);
	/* effective_dod may be NULL */

	struct isi_error *error = NULL;

	bool coid_locked = false, reference_removed;
	coi_entry *coi_entry = NULL;
	struct timeval date_of_death;

	coi_sbt_key coi_sbt_key;

	const void *coi_old_serialization, *coi_new_serialization;
	size_t coi_old_serialization_size, coi_new_serialization_size;

	/* true: block */
	lock_coid(object_id, true, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to lock COID (object ID: %s %lu LIN %#{})",
					  object_id.to_c_string(), object_id.get_snapid(), lin_fmt(lin));
	coid_locked = true;

	ilog(IL_DEBUG, "Dereferencing LIN %#{} from object (%s, %lu)",
		 lin_snapid_fmt(lin, snapid),
		 object_id.to_c_string(), object_id.get_snapid());

	coi_entry = get_entry(object_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get COI entry for object ID (%s %lu)",
					  object_id.to_c_string(), object_id.get_snapid());

	coi_entry->save_serialization();

	reference_removed = coi_entry->remove_referencing_lin(lin, snapid,
														  &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to remove LIN (%#{}) from set of referencing LINs "
					  "(object ID: %s %lu)",
					  lin_fmt(lin), object_id.to_c_string(), object_id.get_snapid());

	coi_entry->update_co_date_of_death(removed_ref_dod, false);
	if (effective_dod != NULL)
		*effective_dod = coi_entry->get_co_date_of_death();

	date_of_death = coi_entry->get_co_date_of_death();

	object_id_to_hsbt_key(object_id, coi_sbt_key);

	coi_entry->get_saved_serialization(coi_old_serialization,
									   coi_old_serialization_size);

	if (coi_entry->get_reference_count() == 0) /////如果此时reference count==0,那么创建ooi_entry,加入到coi_entry中
	{
		/*
		 * With no more LINs referencing this set of cloud objects,
		 * perform a bulk operation to transactionally commit the
		 * updated COI entry and add the cloud object ID to the OOI.
		 */
		/////已经没有LIN引用这个coi entry了,因此把这个object id 加入到ooi中
		isi_cloud::ooi ooi;
		ooi.initialize(&error);
		ON_ISI_ERROR_GOTO(out, error, "failed to initialize OOI");

		/* Lock the entry group for our device id */
		ooi.lock_entry_group(date_of_death, &error);
		ON_ISI_ERROR_GOTO(out, error,
						  "failed to lock entry group (object ID: %s %lu lin %#{})",
						  object_id.to_c_string(), object_id.get_snapid(),
						  lin_fmt(lin));

		/* Prepare the bulk operation. */
		struct coi_sbt_bulk_entry coi_sbt_ops[2];
		memset(coi_sbt_ops, 0, 2 * sizeof(struct coi_sbt_bulk_entry));

		/*
		 * Prepare the "OOI-side" of the bulk operation: adding the
		 * cloud object with the specified date of death.
		 */
		ooi_entry *ooi_entry =
			ooi.prepare_for_cloud_object_addition(date_of_death,
												  object_id, coi_sbt_ops[1], &error);
		if (error != NULL)
		{
			ooi.unlock_entry_group(date_of_death,
								   isi_error_suppress(IL_ERR));

			ON_ISI_ERROR_GOTO(out, error,
							  "failed to prepare ooi entry "
							  "for adding object ID (%s %lu) to ooi",
							  object_id.to_c_string(), object_id.get_snapid());
		}
		ASSERT(ooi_entry != NULL);

		/*
		 * Add the OOI entry's key to the COI entry for use in the
		 * future if/when a reference is restored.  Then prepare the
		 * "COI-side" of the bulk operation: modifying the COI entry.
		 */
		btree_key_t ooi_entry_key = ooi_entry->to_btree_key();
		coi_entry->set_ooi_entry_key(ooi_entry_key);

		coi_entry->get_serialized_entry(coi_new_serialization,
										coi_new_serialization_size);

		coi_sbt_ops[0].is_hash_btree = true;
		coi_sbt_ops[0].key = &coi_sbt_key;
		coi_sbt_ops[0].key_len = sizeof coi_sbt_key;
		// ^ is this right?  shouldn't it take just the key (and not as a void *)
		coi_sbt_ops[0].bulk_entry.fd = sbt_fd_;
		coi_sbt_ops[0].bulk_entry.op_type = SBT_SYS_OP_MOD;
		coi_sbt_ops[0].bulk_entry.entry_buf =
			const_cast<void *>(coi_new_serialization);
		coi_sbt_ops[0].bulk_entry.entry_size =
			coi_new_serialization_size;
		coi_sbt_ops[0].bulk_entry.cm = BT_CM_BUF;
		coi_sbt_ops[0].bulk_entry.old_entry_buf =
			const_cast<void *>(coi_old_serialization);
		coi_sbt_ops[0].bulk_entry.old_entry_size =
			coi_old_serialization_size;

		/*
		 * Execute the bulk operation
		 */
		ooi_coi_bulk_op_wrap(ooi, *this, coi_sbt_ops, 2, &ooi_entry_key,
							 object_id, &error);

		ooi.unlock_entry_group(date_of_death,
							   isi_error_suppress(IL_ERR));
		delete ooi_entry;
	}
	else
	{
		/*
		 * As there are still LINs which reference this set of cloud
		 * objects, update the COI entry.
		 */
		coi_entry->get_serialized_entry(coi_new_serialization,
										coi_new_serialization_size);

		coi_sbt_cond_mod_entry_wrap(*this, object_id, sbt_fd_,
									coi_sbt_key,
									coi_new_serialization, coi_new_serialization_size,
									coi_old_serialization, coi_old_serialization_size, &error);
	}

	if (error)
		goto out;

	if (reference_removed)
	{
		ilog(IL_DEBUG,
			 "removed reference of LIN/snap %{} on object ID (%s %lu)",
			 lin_snapid_fmt(lin, snapid), object_id.to_c_string(),
			 object_id.get_snapid());
	}

out:
	/*
	 * We're suppressing because we can't do anything about the error, but
	 * since this lock would still be held, no thread can do anything to
	 * this COI entry, so log the error as an actual error.
	 */
	if (coid_locked)
		unlock_coid(object_id, isi_error_suppress(IL_ERR));

	if (coi_entry != NULL)
		delete coi_entry;

	isi_error_handle(error, error_out);
}

void coi::update_backed(const isi_cloud_object_id &object_id,
						struct timeval retain_time, struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	coi_entry *entry = NULL;
	const void *serialized_entry;
	const void *old_serialized_entry = NULL;
	size_t old_serialized_entry_size, serialized_entry_size;
	struct btree_flags flags = {};
	coi_sbt_key hsbt_key;
	struct timeval coi_dod;
	bool coid_locked = false;

	object_id_to_hsbt_key(object_id, hsbt_key);
	/* true: block */
	lock_coid(object_id, true, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to lock COID (object ID: %s %lu)",
					  object_id.to_c_string(), object_id.get_snapid());
	coid_locked = true;

	entry = get_entry(object_id, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get entry COID (object ID: %s %lu)",
					  object_id.to_c_string(), object_id.get_snapid());

	entry->save_serialization();

	/*
	 * Check if the entry is already backed up and with a dod
	 * later than retention time
	 */
	coi_dod = entry->get_co_date_of_death();
	if (entry->is_backed() && timercmp(&coi_dod, &retain_time, >))
	{
		// nothing more to do
		goto out;
	}

	entry->set_backed(true);
	entry->update_co_date_of_death(retain_time, false);

	entry->get_serialized_entry(serialized_entry,
								serialized_entry_size);

	entry->get_saved_serialization(old_serialized_entry,
								   old_serialized_entry_size);

	coi_sbt_cond_mod_entry(sbt_fd_, &hsbt_key,
						   serialized_entry, serialized_entry_size, &flags,
						   BT_CM_BUF, old_serialized_entry, old_serialized_entry_size,
						   NULL, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to  update entry COID (object ID: %s %lu)",
					  object_id.to_c_string(), object_id.get_snapid());

out:
	if (coid_locked)
		unlock_coid(object_id, isi_error_suppress(IL_ERR));

	if (entry != NULL)
	{
		delete entry;
		entry = NULL;
	}

	isi_error_handle(error, error_out);
}

void coi::generate_sync_id(const isi_cloud_object_id &object_id,
						   uint64_t &sync_id, struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	cmoi_entry entry;
	struct btree_flags flags = {};
	btree_key_t sbt_key;
	int ret = 0;
	object_id_to_sbt_key(object_id, sbt_key);
	std::vector<char> old_serialized_entry;
	std::vector<char> serialized_entry;

	get_cmo(object_id, entry, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (entry.get_syncing())
	{
		error = isi_cbm_error_new(CBM_SYNC_ALREADY_IN_PROGRESS,
								  "Object %#{} is already under sync.",
								  isi_cloud_object_id_fmt(&object_id));
		goto out;
	}

	/*
	 * We need a deep copy of serialized_entry before we change the
	 * entry and re-serialize.
	 */
	entry.serialize(old_serialized_entry);
	entry.increment_next_syncer_id();
	entry.serialize(serialized_entry);

	ret = ifs_sbt_cond_mod_entry(cmo_sbt_fd_, &sbt_key,
								 serialized_entry.data(), serialized_entry.size(), &flags,
								 BT_CM_BUF, old_serialized_entry.data(),
								 old_serialized_entry.size(),
								 NULL);

	if (ret == 0)
	{
		sync_id = entry.get_next_syncer_id();
		goto out;
	}

	// ifs_sbt_cond_mod_entry returns ERANGE if the mod fails
	// because the expected old version doesn't match the existing
	// version
	if (errno == ERANGE)
	{
		error = isi_cbm_error_new(CBM_SYNC_ALREADY_IN_PROGRESS,
								  "Object %#{} is already under sync.",
								  isi_cloud_object_id_fmt(&object_id));
		goto out;
	}

	error = isi_system_error_new(errno,
								 "Running into unexpected errors updating sync status "
								 "for object %#{} is already under sync.",
								 isi_cloud_object_id_fmt(&object_id));

out:

	isi_error_handle(error, error_out);
}

void coi::update_sync_status(const isi_cloud_object_id &object_id,
							 uint64_t syncer_id, bool syncing, struct isi_error **error_out)
{
	ASSERT(initialized_);

	struct isi_error *error = NULL;
	cmoi_entry entry;
	struct btree_flags flags = {};
	btree_key_t sbt_key;
	int ret = 0;
	object_id_to_sbt_key(object_id, sbt_key);
	std::vector<char> old_serialized_entry;
	std::vector<char> serialized_entry;

	get_cmo(object_id, entry, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// If already under syncing
	if (entry.get_syncing() &&
		syncer_id != entry.get_syncer_id())
	{
		error = isi_cbm_error_new(CBM_SYNC_ALREADY_IN_PROGRESS,
								  "Object %#{} is already under sync. syncing: %d "
								  "syncing in CMOI: %d, syncer_id: %lu syncer_id in "
								  "CMOI: %lu",
								  isi_cloud_object_id_fmt(&object_id), syncing,
								  entry.get_syncing(), syncer_id, entry.get_syncer_id());
		goto out;
	}

	// already done
	if (syncing == entry.get_syncing() &&
		syncer_id == entry.get_syncer_id())
	{
		goto out;
	}

	/*
	 * We need a deep copy of serialized_entry before we change the
	 * entry and re-serialize.
	 */
	entry.serialize(old_serialized_entry);
	entry.set_syncing(syncing);
	entry.set_syncer_id(syncer_id);
	entry.serialize(serialized_entry);

	ret = ifs_sbt_cond_mod_entry(cmo_sbt_fd_, &sbt_key,
								 serialized_entry.data(), serialized_entry.size(), &flags,
								 BT_CM_BUF, old_serialized_entry.data(),
								 old_serialized_entry.size(),
								 NULL);

	if (ret == 0)
	{
		goto out;
	}

	// ifs_sbt_cond_mod_entry returns ERANGE if the mod fails
	// because the expected old version doesn't match the existing
	// version
	if (errno == ERANGE)
	{
		error = isi_cbm_error_new(CBM_SYNC_ALREADY_IN_PROGRESS,
								  "Object %#{} is already under sync.",
								  isi_cloud_object_id_fmt(&object_id));
		goto out;
	}

	error = isi_system_error_new(errno,
								 "Running into unexpected errors updating sync status "
								 "for object %#{} is already under sync.",
								 isi_cloud_object_id_fmt(&object_id));
out:

	if (error == NULL)
	{
		ilog(IL_DEBUG, "Updated sync status for object (%s, %ld) "
					   "syncer_id: %ld status: %d",
			 object_id.to_c_string(), object_id.get_snapid(),
			 syncer_id, syncing);
	}
	isi_error_handle(error, error_out);
}

void coi::get_references(const isi_cloud_object_id &object_id,
						 std::set<ifs_lin_t> &lins, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	coi_entry *entry = NULL;

	entry = get_entry(object_id, &error);
	if (error != NULL)
	{
		isi_error_add_context(error,
							  "failed to get COI entry for object ID (%s, %ld)",
							  object_id.to_c_string(), object_id.get_snapid());
		goto out;
	}

	lins = entry->get_referencing_lins();

out:
	if (entry != NULL)
		delete entry;

	isi_error_handle(error, error_out);
}
/////文件锁
void coi::lock_coid(const isi_cloud_object_id &object_id, bool block,
					struct isi_error **error_out)
{
	isi_error *error = NULL;
	coi_sbt_key hsbt_key;
	struct flock params;
	errno = 0;
	object_id_to_hsbt_key(object_id, hsbt_key);

	uint64_t key = coi_sbt_get_hashed_key(&hsbt_key);

	populate_flock_params(key, &params);

	int result = ifs_cpool_flock(coi_lock_fd_, LOCK_DOMAIN_CPOOL_JOBS,
								 F_SETLK, &params, block ? F_SETLKW : 0);

	if (result != 0)
	{
		error = isi_system_error_new(errno,
									 "Failed to lock the object id (%s).",
									 object_id.to_c_string());
		goto out;
	}

out:

	isi_error_handle(error, error_out);
}

void coi::unlock_coid(const isi_cloud_object_id &object_id,
					  struct isi_error **error_out)
{
	isi_error *error = NULL;
	coi_sbt_key hsbt_key;
	struct flock params;
	object_id_to_hsbt_key(object_id, hsbt_key);

	uint64_t key = coi_sbt_get_hashed_key(&hsbt_key);

	populate_flock_params(key, &params);

	int result = ifs_cpool_flock(coi_lock_fd_, LOCK_DOMAIN_CPOOL_JOBS,
								 F_UNLCK, &params, 0);

	if (result != 0)
	{
		error = isi_system_error_new(errno,
									 "Failed to unlock the object id (%s).",
									 object_id.to_c_string());
		goto out;
	}

out:

	isi_error_handle(error, error_out);
}

bool coi::get_next_version(const isi_cloud_object_id &object_id,
						   isi_cloud_object_id &next,
						   struct isi_error **error_out)
{

	ASSERT(initialized_);

	bool found = false;

	struct coi_sbt_key hsbt_key;
	struct coi_sbt_key next_key;

	object_id_to_hsbt_key(object_id, hsbt_key);

	found = coi_sbt_get_next_version(sbt_fd_, &hsbt_key,
									 NULL, 0, NULL, &next_key, error_out);

	if (found)
		hsbt_key_to_object_id(next_key, next);

	return found;
}

bool coi::get_prev_version(const isi_cloud_object_id &object_id,
						   isi_cloud_object_id &prev,
						   struct isi_error **error_out)
{

	ASSERT(initialized_);

	bool found = false;

	struct coi_sbt_key hsbt_key;
	struct coi_sbt_key prev_key;

	object_id_to_hsbt_key(object_id, hsbt_key);

	found = coi_sbt_get_prev_version(sbt_fd_, &hsbt_key,
									 NULL, 0, NULL, &prev_key, error_out);

	if (found)
		hsbt_key_to_object_id(prev_key, prev);
	return found;
}

bool coi::get_last_version(const isi_cloud_object_id &object_id,
						   isi_cloud_object_id &last,
						   struct isi_error **error_out)
{

	ASSERT(initialized_);

	bool found = false;

	struct coi_sbt_key hsbt_key;
	struct coi_sbt_key last_key;

	object_id_to_hsbt_key(object_id, hsbt_key);

	found = coi_sbt_get_last_version(sbt_fd_, &hsbt_key,
									 NULL, 0, NULL, &last_key, error_out);

	if (found)
		hsbt_key_to_object_id(last_key, last);
	return found;
}

void coi::restore_object(const isi_cloud_object_id &object_id,
						 uint8_t archiving_cluster_id[IFSCONFIG_GUID_SIZE],
						 const isi_cbm_id &archive_account_id,
						 const char *cmo_container, isi_tri_bool is_compressed,
						 isi_tri_bool checksum,
						 struct timeval date_of_death,
						 struct isi_error **error_out)
{
	isi_error *error = NULL;
	bool found = false;

	isi_cbm_ooi_sptr ooi;
	std::auto_ptr<coi_entry> entry(get_entry(object_id, &error));
	if (error && isi_system_error_is_a(error, ENOENT))
	{
		isi_error_free(error);
		error = NULL;
		found = false;
	}
	else if (error == NULL)
	{
		found = true;
	}

	ON_ISI_ERROR_GOTO(out, error);

	ooi = isi_cbm_get_ooi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!found) /////发现coi中没有包含这个object的entry
	{
		// We do not know if it was really backed up,
		// just be conservative and mark so during COI restore
		bool backed_up = true;
		add_object(object_id,
				   archiving_cluster_id, archive_account_id,
				   cmo_container,
				   is_compressed,
				   checksum, 0,
				   &error, &date_of_death, &backed_up);
		ON_ISI_ERROR_GOTO(out, error);
	}
	else
	{
		// we need to update the DOD
		update_backed(object_id, date_of_death, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);
}

static const char *
bool_to_str(bool val)
{
	return val ? "TRUE" : "FALSE";
}

static std::string
time_to_str(time_t time)
{
	struct tm timeinfo_local;
	char timebuf_local[80];

	localtime_r(&time, &timeinfo_local);
	strftime(timebuf_local, sizeof(timebuf_local),
			 "%a %b %d %H:%M:%S %Y %Z", &timeinfo_local);

	return timebuf_local;
}

static std::string
lins_to_str(const std::set<ifs_lin_t> &lins)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	size_t num = lins.size();

	fmt_print(&fmt, "num:%zu, lins:", num);
	if (num == 0)
		fmt_print(&fmt, "none");
	else
	{
		for (std::set<ifs_lin_t>::const_iterator iter =
				 lins.begin();
			 iter != lins.end(); ++iter)
		{
			fmt_print(&fmt, "%{}, ", lin_fmt(*iter));
		}
	}

	return fmt_string(&fmt);
}

/**
 * Update error_in with any coi entry differences
 */
static void
coi_entry_difference_set(struct isi_error *error_in,
						 const isi_cloud::coi_entry &c1, const isi_cloud::coi_entry &c2)
{
	if (!(c1.get_object_id() == c2.get_object_id()))
	{
		isi_error_add_context(error_in,
							  "CMO ID::snap different, curr: %s::%llu expected: %s::%llu",
							  c1.get_object_id().to_c_string(),
							  c1.get_object_id().get_snapid(),
							  c2.get_object_id().to_c_string(),
							  c2.get_object_id().get_snapid());
	}
	const uint8_t *id[2] = {c1.get_archiving_cluster_id(),
							c2.get_archiving_cluster_id()};
	if (memcmp(id[0], id[1], IFSCONFIG_GUID_SIZE))
	{
		char cluster_id_buf[2][IFSCONFIG_GUID_STR_SIZE + 1];
		for (int i = 0; i < 2; ++i)
		{
			getGuid(id[i], cluster_id_buf[i],
					IFSCONFIG_GUID_STR_SIZE);
			cluster_id_buf[i][IFSCONFIG_GUID_STR_SIZE] = '\0';
		}
		isi_error_add_context(error_in,
							  "Archiving cluster ID different, curr: %s expected: %s",
							  cluster_id_buf[0], cluster_id_buf[1]);
	}
	if (!(c1.get_archive_account_id() == c2.get_archive_account_id()))
	{
		isi_error_add_context(error_in,
							  "Archiving account ID different, curr: %u expected: %u",
							  c1.get_archive_account_id().get_id(),
							  c2.get_archive_account_id().get_id());
	}
	if (strcmp(c1.get_cmo_container(), c2.get_cmo_container()))
	{
		isi_error_add_context(error_in,
							  "CMO container different, curr: %s expected: %s",
							  c1.get_cmo_container(),
							  c2.get_cmo_container());
	}
	if (c1.is_compressed() != c2.is_compressed())
	{
		isi_error_add_context(error_in,
							  "Compressed different, curr: %s expected: %s",
							  c1.is_compressed().c_str(),
							  c2.is_compressed().c_str());
	}
	if (c1.has_checksum() != c2.has_checksum())
	{
		isi_error_add_context(error_in,
							  "Checksum different, curr: %s expected: %s",
							  c1.has_checksum().c_str(),
							  c2.has_checksum().c_str());
	}
	if (c1.is_backed() != c2.is_backed())
	{
		isi_error_add_context(error_in,
							  "Backed Up different, curr: %s expected: %s",
							  bool_to_str(c1.is_backed()),
							  bool_to_str(c2.is_backed()));
	}
	struct timeval dod[2] = {c1.get_co_date_of_death(),
							 c2.get_co_date_of_death()};
	if (timercmp(&dod[0], &dod[1], !=))
	{
		isi_error_add_context(error_in,
							  "Cloud Object dod different, curr: %s expected: %s",
							  time_to_str(dod[0].tv_sec).c_str(),
							  time_to_str(dod[1].tv_sec).c_str());
	}
	std::set<ifs_lin_t> ref_lins[2] = {
		c1.get_referencing_lins(), c2.get_referencing_lins()};
	if (ref_lins[0] != ref_lins[1])
	{
		isi_error_add_context(error_in,
							  "Referencing lins different, curr: %s expected: %s",
							  lins_to_str(ref_lins[0]).c_str(),
							  lins_to_str(ref_lins[1]).c_str());
	}
	btree_key_t keys[2] = {c1.get_ooi_entry_key(), c2.get_ooi_entry_key()};
	if ((keys[0].keys[0] != keys[1].keys[0]) ||
		(keys[0].keys[1] != keys[1].keys[1]))
	{
		isi_error_add_context(error_in,
							  "Associated OOI Entry different, curr: 0x%016lx/0x%016lx "
							  "expected: 0x%016lx/0x%016lx",
							  keys[0].keys[0], keys[0].keys[1],
							  keys[1].keys[0], keys[1].keys[1]);
	}
}

void coi::validate_entry(
	const isi_cloud_object_id &object_id,
	const void *serialization, size_t serialization_size,
	struct isi_error **error_out)
{
	isi_cloud::coi_entry *curr_entry = NULL, *entry = NULL;
	const void *curr_serialization;
	size_t curr_serialization_size;
	struct isi_error *error = NULL;

	/*
	 * Discover what changed: compare the current
	 * sbt content for this entry with the one we think
	 * it should be
	 */
	curr_entry = this->get_entry(object_id, &error);
	ON_ISI_ERROR_GOTO(out, error);
	curr_entry->get_serialized_entry(curr_serialization,
									 curr_serialization_size);

	entry = new isi_cloud::coi_entry;
	ASSERT(entry);
	entry->set_serialized_entry(CPOOL_COI_VERSION, serialization,
								serialization_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if ((curr_serialization_size != serialization_size) ||
		memcmp(curr_serialization, serialization, serialization_size))
	{
		error = isi_system_error_new(ERANGE,
									 "Current coi entry does not match expected");
		/*
		 * Get details
		 */
		coi_entry_difference_set(error, *curr_entry, *entry);
		goto out;
	}

out:
	isi_error_handle(error, error_out);

	if (entry)
		delete entry;
	if (curr_entry)
		delete curr_entry;
}

bool coi::has_entries(struct isi_error **error_out) const
{
	ASSERT(initialized_, "COI has not been initialized");

	struct isi_error *error = NULL;
	bool entries_exist = false;

	entries_exist = coi_sbt_has_entries(sbt_fd_, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:

	isi_error_handle(error, error_out);
	return entries_exist;
}
