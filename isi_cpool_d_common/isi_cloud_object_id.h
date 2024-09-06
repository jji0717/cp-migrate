#ifndef __ISI_CLOUD_OBJECT_ID_H__
#define __ISI_CLOUD_OBJECT_ID_H__

#include <ifs/ifs_types.h>

#include <string>

class isi_cloud_object_id
{
public:
	static const unsigned int GUID_SIZE_ =		16;
	static const int MAX_INDEX_ =			99999999;
	static const int CMO_INDEX_ =			0;

protected:
	/*
	 * A cloud object ID is 128 bits.
	 */
	uint64_t			high_;
	uint64_t			low_;
	/**
	 * the cloud snapshot id part
	 */
	uint64_t			snapid_;

	mutable std::string		string_form_;

	void create_string_form(uint64_t high, uint64_t low) const;
	void create_string_form(unsigned char guid[GUID_SIZE_]) const;

public:
	isi_cloud_object_id();
	isi_cloud_object_id(uint64_t high, uint64_t low);
	explicit isi_cloud_object_id(const btree_key_t &key);
	isi_cloud_object_id(const isi_cloud_object_id &id);
	~isi_cloud_object_id();

	std::string get_cdo_name(int index) const;
	std::string get_cmo_name(void) const;

	void set(uint64_t high, uint64_t low);

	void set_snapid(uint64_t snapid) { snapid_ = snapid; }

	void generate(void);

	bool from_string_form(const char *str);

	isi_cloud_object_id &operator=(const isi_cloud_object_id &rhs);
	bool operator==(const isi_cloud_object_id &rhs) const;
	bool operator!=(const isi_cloud_object_id &rhs) const;
	bool operator<(const isi_cloud_object_id &rhs) const;

	static size_t get_packed_size(void);
	void pack(void **stream) const;
	void unpack(void **stream);

	const std::string &to_string(void) const;
	const char *to_c_string(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	// XXXjrw: these are temporary, and are only here for
	// isi_cpool_d_api::delete_cloud_object to use when assigning a
	// taskmap_key_t based on an isi_cloud_object_id_t - in the future,
	// once taskmap_key_t becomes class taskmap_key (a legitimate class
	// with derived classes for taskmap_file_key and
	// taskmap_cloud_object_key), there will be an assignment method on
	// taskmap_key that takes an isi_cloud_object_key
	uint64_t get_high(void) const { return high_; }
	uint64_t get_low(void) const { return low_; }
	uint64_t get_snapid(void) const { return snapid_; }

	struct fmt_conv_ctx get_print_fmt(void) const;
};

struct fmt_conv_ctx isi_cloud_object_id_fmt(const isi_cloud_object_id *);

#endif // __ISI_CLOUD_OBJECT_ID_H__
