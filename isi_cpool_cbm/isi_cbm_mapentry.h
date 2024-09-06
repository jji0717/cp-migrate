
#pragma once

#include <string>

#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include "isi_cbm_id.h"


class isi_cfm_mapentry
{
	friend class isi_cfm_mapinfo;
 private:
	uint32_t version_;
	off_t offset;
	size_t length;
	size_t plength;
	isi_cbm_id account_id;
	std::string container;
	isi_cloud_object_id object_id_;
	int index; // the index of master CDO.
	void *packed_data;
	int packed_size;
	ifs_guid check_info; // md5 sum would fit here :-)
 public:
	void pack(int itype, void **m_packed, int &my_packed_size,
	    bool pack_overflow = false) const;

	isi_cfm_mapentry()
	{
		version_ = CPOOL_CFM_MAP_VERSION;
		offset = -1;
		length = -1;
		plength = -1;
		index = 0;
		packed_data = NULL;
		packed_size = 0;
		bzero(&check_info, sizeof(check_info));
	}
	~isi_cfm_mapentry()
	{
		if (packed_data && packed_size) {
			free(packed_data);
			packed_data = NULL;
			packed_size = 0;
		}
	}

	isi_cfm_mapentry(const isi_cfm_mapentry &other) :
            version_(other.version_),
	    offset(other.offset), length(other.length),
	    plength(other.plength),
	    account_id(other.account_id), container(other.container),
	    object_id_(other.object_id_),
	    index(other.index),
	    packed_data(NULL), packed_size(0)
	{
		bzero(&check_info, sizeof(check_info));
	}
	isi_cfm_mapentry &operator=(const isi_cfm_mapentry &other)
	{
		version_ = other.version_;
		offset = other.offset;
		length = other.length;
		plength = other.plength;
		account_id = other.account_id;
		container = other.container;
		object_id_ = other.object_id_;
		index = other.index;
		if (packed_data && packed_size) {
			free(packed_data);
			packed_data = NULL;
			packed_size = 0;
		}
		return *this;
	}

	bool operator==(const isi_cfm_mapentry &other) const
	{
		return version_ == other.version_ &&
		    offset == other.offset &&
		    length == other.length &&
		    plength == other.plength &&
		    account_id == other.account_id &&
		    container == other.container &&
		    object_id_ == other.object_id_ &&
		    index == other.index;
	}

	bool operator!=(const isi_cfm_mapentry &other) const
	{
		return !operator==(other);
	}


	void setentry(uint32_t, const isi_cbm_id &, off_t, size_t);
	void setentry(uint32_t, const isi_cbm_id &, off_t, size_t, std::string,
	    const isi_cloud_object_id &);
	void setentry(uint32_t, const isi_cbm_id &, off_t, size_t, std::string,
	    const isi_cloud_object_id &, ifs_guid);
	const isi_cbm_id &get_account_id() const {return account_id;}
	uint32_t get_version() const { return version_; }
	void set_version(uint32_t version) { version_ = version; }
	off_t get_offset() const {return offset;}
	void set_offset(off_t off) { offset = off; }
	size_t get_length() const {return length;}
	void set_length(size_t len) { length = len; }
	size_t get_plength() const {return plength;}
	void set_plength(size_t plen) {plength = plen;}
	const std::string &get_container() const {return container;}
	void set_container(const std::string &container)
	{
		this->container = container;
	}
	const isi_cloud_object_id &get_object_id(void) const
	{
		return object_id_;
	}
	void set_object_id(const isi_cloud_object_id &obj_id)
	{
		object_id_ = obj_id;
	}
	const ifs_guid &get_checkinfo() const {return check_info;}
	int get_packed_size() const {return packed_size;}
	void *get_packed_entry() const {return packed_data;}
	void set_index(int idx) { index = idx; }
	int get_index() const { return index; }

	void set_packed_entry(void *entry, int sz)
	{
		free_packed_entry();
		packed_data = calloc(1, sz);
		ASSERT(packed_data);
		memcpy(packed_data, entry, sz);
		packed_size = sz;
	};
	void free_packed_entry() {
		if (packed_data && packed_size) {
			free(packed_data);
			packed_data = NULL;
			packed_size = 0;
		}
	}
	void update(off_t o, size_t l) {offset = o;length = l;}

	void pack(int itype, bool pack_overflow = false)
	{
		free_packed_entry();
		pack(itype, &packed_data, packed_size, pack_overflow);
	}

	void *unpack(void *entry, int itype, isi_error **error_out,
	    bool pack_overflow);

	static void get_pack_size(int itype, int &pack_size,
	    bool pack_overflow);
};
