#ifndef __ISI_CBM_MAPPER__H__
#define __ISI_CBM_MAPPER__H__
/*
 * XXXegc: the following structures need to be defined elsewhere
 */

#include <list>
#include "isi_cloud_api/isi_cloud_api.h"
#include <isi_cpool_cbm/io_helper/encrypt_ctx.h>
#include <isi_cloud_common/isi_cpool_version.h>
#include <ifs/bam/bam_cpool.h>
#include <isi_cpool_cbm/io_helper/sparse_area_header.h>

#include "isi_cbm_mapentry.h"
#include "isi_cbm_mapinfo_store.h"
#include "isi_cbm_mapinfo_impl.h"

#define ISI_CFM_MAP_MAGIC (IFS_CPOOL_CFM_MAP_MAGIC)

#define ISI_CFM_MAP_MIN_VERSION (IFS_CPOOL_CFM_MAP_MIN_VERSION)

// #define ISI_CBM_USE_NAMESPACE_USER
#ifndef ISI_CBM_USE_NAMESPACE_USER
#define ISI_CFM_USER_ATTR_SIZE IFS_CPOOL_ATTR_FSIZE
#define ISI_CFM_USER_ATTR_MTIME IFS_CPOOL_ATTR_MTIME
#define ISI_CFM_USER_ATTR_FLAGS IFS_CPOOL_ATTR_FLAGS

#define ISI_CFM_MAP_ATTR_NAME IFS_CPOOL_MAP_NAME
#define ISI_CFM_MAP_ATTR_SIZE IFS_CPOOL_MAP_SIZE_NAME

#define ISI_CFM_MAP_ATTR_NAMESPACE EXTATTR_NAMESPACE_IFS
#define ISI_CFM_MAP_EXTATTR_THRESHOLD IFS_USERATTR_MAXSIZE
#else
#define ISI_CFM_MAP_ATTR_NAME "SYS_CPOOL_MAP"
#define ISI_CFM_MAP_ATTR_SIZE "SYS_CPOOL_MAP_SIZE"

#define ISI_CFM_MAP_ATTR_NAMESPACE EXTATTR_NAMESPACE_USER
#define ISI_CFM_MAP_EXTATTR_THRESHOLD IFS_USERATTR_VAL_MAXSIZE
#endif

/*
 * Code as it currently exists will allow the stub map to overflow from an
 * extended attribute to an ADS.  These next two definitions determine how the
 * extended attributes and ADS will be utilized.
 * NB: If enabled, extattr will always be utilized first.
 */
#define ISI_CFM_MAP_USE_EXTATTR true
#define ISI_CFM_MAP_USE_ADS false

#define ISI_CFM_MAP_ADS_NAME "$SYS_CPOOL_MAP"

enum isi_cfm_map_type
{
	ISI_CFM_MAP_TYPE_NONE = 0x0000,
	ISI_CFM_MAP_TYPE_SIMPLE_EXTENT = 0x0001,
	ISI_CFM_MAP_TYPE_CHECKINFO = 0x0010,
	ISI_CFM_MAP_TYPE_OBJECT = 0x0020,
	ISI_CFM_MAP_TYPE_CONTAINER = 0x0040,
	ISI_CFM_MAP_TYPE_MAPCHECK = 0x0080,
	ISI_CFM_MAP_TYPE_ENCRYPTKEY = 0x0100,
	ISI_CFM_MAP_TYPE_CONTAINER_CMO = 0x0200,
	ISI_CFM_MAP_TYPE_COMPRESSED = 0x0400,
	ISI_CFM_MAP_TYPE_ENCRYPTED = 0x1000,
	ISI_CFM_MAP_TYPE_CHECKSUM = 0x2000,
	ISI_CFM_MAP_TYPE_OVERFLOW = 0x4000,
	ISI_CFM_MAP_TYPE_WIP = 0x10000,
	ISI_CFM_MAP_TYPE_OBJECTINFO = ISI_CFM_MAP_TYPE_SIMPLE_EXTENT |
								  ISI_CFM_MAP_TYPE_CONTAINER |
								  ISI_CFM_MAP_TYPE_OBJECT |
								  ISI_CFM_MAP_TYPE_CONTAINER_CMO,
	ISI_CFM_MAP_TYPE_OBJECTCHECKMDO = ISI_CFM_MAP_TYPE_OBJECTINFO |
									  ISI_CFM_MAP_TYPE_MAPCHECK,
	ISI_CFM_MAP_TYPE_VERIFIER = ISI_CFM_MAP_TYPE_OBJECTINFO |
								ISI_CFM_MAP_TYPE_CHECKINFO,
	ISI_CFM_MAP_TYPE_VERIFIERCHECKMDO = ISI_CFM_MAP_TYPE_VERIFIER |
										ISI_CFM_MAP_TYPE_MAPCHECK,
};

#define COMPARE_GUID(a, b) (((a).ig_time == (b).ig_time) && \
							((a).ig_random == (b).ig_random))

#define MAPINFO_SPACE (isi_cfm_mapinfo::get_upbound_pack_size())
#define ENTRY_SPACE (IFS_USERATTR_MAXSIZE - MAPINFO_SPACE)

struct verifyer
{
	isi_cbm_id account_id;
	off_t offset;
	size_t length;
	isi_cloud_object_id object;
};

void format_bucket_name(struct fmt &bucket_fmt, const isi_cbm_id &acct_id);

class isi_cfm_mapinfo
{
	friend class isi_cfm_mapinfo_impl;

private:
	/*
	 * *packed is not part of the map, just convenient to have defined here
	 */
	void *packed_map;
	int packed_size;

	/*
	 * The following entries make up the map and are packed for storage
	 * with the stub.  Maybe we should use something other than list,
	 * maybe a tree for faster access if entries are not chunk aligned..
	 */
	////////这些属性组成了mapinfo,当stub时,需要将这些属性打包,写入kernel
	int version;
	int count;
	int size;
	int header_size;
	int type;
	size_t chunksize;
	size_t readsize;
	uint32_t sparse_resolution;
	off_t filesize;
	ifs_guid checkinfo;
	ifs_lin_t lin;
	isi_cbm_id policy_id;	//////policy id
	isi_cbm_id provider_id; //////provider id
	std::string container_cmo;
	isi_cbm_id account;				/////account id
	isi_cloud_object_id object_id_; /////object id

	// defined in encrypt_ctx.h
	mek_id_t master_key_id;
	dek_t encrypted_key;
	iv_t master_iv; // to decode the data

	isi_cfm_mapinfo_impl impl;

	/* define this here so pack and unpack use the same size for this */
	short packingsz;

	/* internal use */
	bool convert_on_pack;
	std::string suffix_;

	void clear()
	{
		if (packed_map)
			free(packed_map);
		packed_map = NULL;
		container_cmo.clear();
		count = 0;
		suffix_.clear();
	}

	///////stub时,调用把mapinfo写入kernel
	void write_map_core(int fd, u_int64_t, ifs_cpool_stub_opflags,
						struct isi_error **error);

	int get_pack_size_header() const;
	int get_pack_size_entries(isi_error **error) const;

public:
	typedef isi_cfm_mapinfo_impl::iterator iterator;
	typedef isi_cfm_mapinfo_impl::iterator map_iterator;

	int get_pack_size(isi_error **error) const;

	/**
	 * Serialize the object to *my_packed_map
	 * @param my_packed_size[out] the packed size in bytes
	 * @param my_packed_map[out] points to the buffer containing the
	 *        packed data.
	 */
	void pack(int &my_packed_size, void **my_packed_map,
			  struct isi_error **error_out) const;

	isi_cfm_mapinfo() : policy_id(), provider_id(), container_cmo(), account(),
						impl(this)
	{
		count = size = 0;
		packed_map = NULL;
		packed_size = 0;
		version = CPOOL_CFM_MAP_VERSION;
		type = ISI_CFM_MAP_TYPE_NONE;
		chunksize = 0;
		filesize = 0;
		readsize = 0;
		sparse_resolution = 0;
		lin = 0;
		header_size = -1;
		convert_on_pack = true;
		packingsz = 0;
		memset(&checkinfo, '\0', sizeof(ifs_guid));
	}

	isi_cfm_mapinfo(int itype)
		: policy_id(), provider_id(), container_cmo(), account(),
		  impl(this)
	{
		count = size = 0;
		packed_map = NULL;
		packed_size = 0;
		version = CPOOL_CFM_MAP_VERSION;
		chunksize = 0;
		filesize = 0;
		readsize = 0;
		sparse_resolution = 0;
		type = itype;
		lin = 0;
		header_size = -1;
		convert_on_pack = true;
		packingsz = 0;
		memset(&checkinfo, '\0', sizeof(ifs_guid));
	}

	isi_cfm_mapinfo(size_t csz /*chunksize*/, size_t fsz /*filesize*/) : impl(this)
	{
		count = size = 0;
		packed_map = NULL;
		packed_size = 0;
		version = CPOOL_CFM_MAP_VERSION;
		type = 0;
		chunksize = csz;
		filesize = fsz;
		readsize = 0;
		sparse_resolution = 0;
		lin = 0;
		header_size = -1;
		convert_on_pack = true;
		packingsz = 0;
		memset(&checkinfo, '\0', sizeof(ifs_guid));
	}

	isi_cfm_mapinfo(size_t csz, size_t fsz,
					size_t rsz, uint32_t sas) : impl(this)
	{
		count = size = 0;
		packed_map = NULL;
		packed_size = 0;
		version = CPOOL_CFM_MAP_VERSION;
		type = 0;
		chunksize = csz;
		filesize = fsz;
		readsize = rsz;
		sparse_resolution = sas;
		lin = 0;
		header_size = -1;
		convert_on_pack = true;
		packingsz = 0;
		memset(&checkinfo, '\0', sizeof(ifs_guid));
	}

	isi_cfm_mapinfo(const isi_cfm_mapinfo &that,
					struct isi_error **error_out) : impl(this)
	{
		copy(that, error_out);
	}

	~isi_cfm_mapinfo()
	{
		clear();
	}

	bool equals(const isi_cfm_mapinfo &that, struct isi_error **error_out)
		const;

	// copy will do deep copy of the mapinfo
	// It creates an identical map.
	void copy(const isi_cfm_mapinfo &that, struct isi_error **error_out);

	bool add(isi_cfm_mapentry &mapentry, struct isi_error **error_out);

	void remove(const isi_cfm_mapentry &mapentry,
				struct isi_error **error_out);

	/**
	 * utility function to truncate the mapping to the new file size
	 *
	 */
	void truncate(off_t new_size, struct isi_error **error_out);
	void dump(bool dump_map = true, bool dump_paths = false) const;
	void dump_to_ilog(bool dump_paths = false) const;
	void pack(struct isi_error **error_out);

	inline void remove_store(struct isi_error **error_out)
	{
		impl.remove(error_out);
	}

	inline iterator begin(struct isi_error **error_out) const
	{
		return impl.begin(error_out);
	}

	inline iterator end() const
	{
		return impl.end();
	}

	inline iterator last(struct isi_error **error_out) const
	{
		return impl.last(error_out);
	}

	inline iterator find(off_t offset, struct isi_error **error_out) const
	{
		return impl.find(offset, error_out);
	}

	inline iterator lower_bound(off_t offset,
								struct isi_error **error_out) const
	{
		return impl.lower_bound(offset, error_out);
	}

	inline void erase(off_t offset, struct isi_error **error_out)
	{
		impl.erase(offset, error_out);
	}

	/**
	 * Unpack the data from the given
	 * @param map: the map buffer
	 * @param buf_size: the buffer size
	 * @param error_out: error if any
	 */
	void unpack(void *map, size_t buf_size, isi_error **error_out);
	void unpack_header(void *map, isi_error **error_out);
	void unpack(void *map, size_t buf_size, bool header_only,
				isi_error **error_out);
	int get_version(void) const;
	void set_version(int);
	void *get_packed_map() { return packed_map; }
	int get_packed_size() { return packed_size; }
	void add_entry(isi_cfm_mapentry &mapentry);
	int get_count() const { return count; };
	void set_count(int c) { count = c; }
	size_t get_chunksize() const { return chunksize; }
	void set_chunksize(size_t csz) { chunksize = csz; }
	off_t get_filesize() const { return filesize; }
	void set_filesize(off_t fsz) { filesize = fsz; }
	size_t get_readsize() const { return readsize; }
	void set_readsize(size_t rsz) { readsize = rsz; }
	uint32_t get_sparse_resolution() const { return sparse_resolution; }
	void set_sparse_resolution(uint32_t res, size_t chnk)
	{
		sparse_resolution = MIN(ROUND_UP_SPA_RESOLUTION(res),
								MAX_SPA_RESOLUTION(chnk));
	}
	ifs_guid get_checkinfo() const { return checkinfo; }
	void set_checkinfo(ifs_guid *chk) { checkinfo = *chk; }
	const isi_cbm_id &get_provider_id() const { return provider_id; }
	void set_provider_id(const isi_cbm_id &pid) { provider_id.copy(pid); }
	const isi_cbm_id &get_policy_id() const { return policy_id; }
	void set_policy_id(const isi_cbm_id &pid) { policy_id.copy(pid); }
	void set_container(char *cmo) { container_cmo = cmo; }
	void set_container(const std::string &cmo) { container_cmo = cmo; }
	const char *get_container() const { return container_cmo.c_str(); }
	void set_account(const isi_cbm_id &in_account) { account = in_account; }
	const isi_cbm_id &get_account() const { return account; }
	void set_object_id(const isi_cloud_object_id &object_id)
	{
		object_id_ = object_id;
	}
	const isi_cloud_object_id &get_object_id(void) const
	{
		return object_id_;
	}
	void set_lin(ifs_lin_t l) { lin = l; }
	ifs_lin_t get_lin() const { return lin; }

	int get_type() const { return type; }
	void set_type(int t) { type = t; }

	void move_store_on_name_change(struct isi_error **error_out)
	{
		impl.move_store_on_name_change(error_out);
	}

	/**
	 * get the mapentry overflow threshold
	 */
	int get_overflow_threshold() const
	{
		// EA stores the offset for each entry
		// checinfo is dynamic
		int entry_sz;
		isi_cfm_mapentry::get_pack_size(get_type(), entry_sz, false);

		return ENTRY_SPACE / entry_sz;
	}
	int get_mapentry_pack_size(bool pack_overflow) const
	{
		int entry_sz = 0;
		isi_cfm_mapentry::get_pack_size(get_type(), entry_sz, pack_overflow);

		return entry_sz;
	}

	bool has_checkinfo() const
	{
		return type & ISI_CFM_MAP_TYPE_CHECKINFO;
	}

	bool has_object() const
	{
		return type & ISI_CFM_MAP_TYPE_OBJECT;
	}

	/**
	 *  Get the size for all the map entries.
	 */
	int get_pack_size_mapentries(isi_error **error_out) const;

	/**
	 * get the total size of the mapinfo, including the external entries
	 * with the offset of each entry counted in
	 */
	size_t get_total_size();

	/**
	 * mark the mapping tpye. The existing type is added with the
	 * given trait. Used during archiving.
	 */
	void mark_mapping_type(isi_cfm_map_type map_type);

	/**
	 * check if the other mapping info refer to the same
	 * cloud CMO object
	 */
	bool has_same_cmo(const isi_cfm_mapinfo &other) const;
	bool has_same_cmo(const isi_cbm_id &account,
					  const isi_cloud_object_id &object_id) const;

	/**
	 * Check if the map is valid and internally consistent
	 * Return an error with EINVAL otherwise
	 */
	void verify_map(struct isi_error **error_out);

	void write_map(int fd, u_int64_t, bool update, bool delayed_inval,
				   struct isi_error **error);
	void restore_map(int fd, struct isi_error **error); /////isi_cpool_bkup使用
	bool data_match(isi_cfm_mapentry &, isi_cfm_mapentry &);
	bool verify(struct verifyer *, int);

	/**
	 * get the map iterator whose entries enclosing the begin offset
	 * i.e. range >= offset.
	 */
	bool get_map_iterator_for_offset(map_iterator &iter, off_t offset,
									 struct isi_error **error_out) const;
	/**
	 * Get the iterator to the map entry that contains the offset
	 * given. If the offset is not contained by any map entry it will
	 * return false and the iterator will point to
	 * impl.end()
	 */
	bool get_containing_map_iterator_for_offset(map_iterator &it,
												off_t offset, struct isi_error **error_out) const;

	bool get_convert_to_current() { return convert_on_pack; }
	void set_convert_to_current(bool tf) { convert_on_pack = tf; }
	int get_headersize() { return header_size; }
	void set_headersize(int sz) { header_size = sz; }

	bool compare_fields(const isi_cfm_mapinfo &, bool should_assert = false)
		const;

	void set_compression(bool compression)
	{
		if (compression)
			type |= ISI_CFM_MAP_TYPE_COMPRESSED;
		else
			type &= ~ISI_CFM_MAP_TYPE_COMPRESSED;
	}
	bool is_compressed() const
	{
		return type & ISI_CFM_MAP_TYPE_COMPRESSED;
	}

	void set_checksum(bool checksum)
	{
		if (checksum)
			type |= ISI_CFM_MAP_TYPE_CHECKSUM;
		else
			type &= ~ISI_CFM_MAP_TYPE_CHECKSUM;
	}
	bool has_checksum() const
	{
		return type & ISI_CFM_MAP_TYPE_CHECKSUM;
	}

	void set_encryption_ctx(encrypt_ctx &ctx)
	{
		type |= ISI_CFM_MAP_TYPE_ENCRYPTED;

		this->master_key_id = ctx.mek_id;
		this->encrypted_key = ctx.edek;
		this->master_iv = ctx.master_iv;
	}
	void get_encryption_ctx(encrypt_ctx &ctx,
							struct isi_error **error_out) const
	{
		if (type & ISI_CFM_MAP_TYPE_ENCRYPTED)
		{
			ctx.initialize(this->master_key_id,
						   this->encrypted_key, this->master_iv, error_out);
		}
	}
	const mek_id_t &get_mek_id() const { return master_key_id; }
	void set_mek_id(const mek_id_t &mek) { master_key_id = mek; }

	const dek_t &get_dek() const { return encrypted_key; }
	void set_dek(const dek_t &dek) { encrypted_key = dek; }

	const iv_t &get_iv() const { return master_iv; }
	void set_iv(const iv_t &iv) { master_iv = iv; }

	bool is_encrypted() const
	{
		return type & ISI_CFM_MAP_TYPE_ENCRYPTED;
	}

	void set_overflow(bool overflow)
	{
		if (overflow)
			type |= ISI_CFM_MAP_TYPE_OVERFLOW;
		else
			type &= ~ISI_CFM_MAP_TYPE_OVERFLOW;
	}

	void set_store_suffix(const std::string &suffix)
	{
		suffix_ = suffix;
	}

	const std::string &get_store_suffix() const
	{
		return suffix_;
	}

	bool is_overflow() const
	{
		return type & ISI_CFM_MAP_TYPE_OVERFLOW;
	}

	isi_cfm_mapinfo_store *get_store()
	{
		return impl.get_store();
	}

	isi_cfm_mapinfo_store *open_store(bool create_if_not_found,
									  bool overwrite_if_exist, bool *created,
									  struct isi_error **error_out)
	{
		if (is_overflow())
			impl.open_store(create_if_not_found, overwrite_if_exist, created,
							error_out);

		return impl.get_store();
	}

	/**
	 * check if the mapping info contains the object id
	 * at or after the offset.
	 * @param offset[in] the object's offset
	 * @param id[in] the object id
	 * @param error_out[out] errors if any
	 * @return if containing the object, true, otherwise false.
	 */
	bool contains(off_t offset, const isi_cloud_object_id &id,
				  struct isi_error **error_out) const;

	static int get_upbound_pack_size();

	/**
	 * get mapinfo pack size field offset in the packed mapinfo
	 */
	static int get_size_offset();
};

const uint64_t g_minimum_chunksize = 256;//jjz 8192; // minimum 8k
class isi_cpool_master_cdo;
/**
 * Utility function to initialize the master CDO information from the
 * mapping entry and the mapping information.
 * @param master_cdo[out] the master CDO
 * @param mapinfo[in] the map info object
 * @param entry[in] the mapping entry
 */
void master_cdo_from_map_entry(
	isi_cpool_master_cdo &master_cdo,
	const isi_cfm_mapinfo &mapinfo,
	const isi_cfm_mapentry *entry);

/**
 * Utility to convert a kernel delete notification to lin/snap/mapinfo
 * @param kernel_queue_entry[in] - kernel delete info (kdi)
 * @param kernel_queue_entry_size[in] - size of kdi
 * @param lin[out] - lin from kdi
 * @param snapid[out] - snapid from kdi
 * @param mapinfo[out] - map as extracted and unpacked from kdi
 * @param error_out[out] - NULL if no error returned
 */
void isi_cbm_get_gcinfo_from_pq(const void *kernel_queue_entry,
								size_t kernel_queue_entry_size,
								ifs_lin_t &lin,
								ifs_snapid_t &snapid,
								isi_cfm_mapinfo &mapinfo,
								struct isi_error **error_out);

#endif // __ISI_CBM_MAPPER__H__
