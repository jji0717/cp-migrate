#include <isi_util/isi_guid.h>

#include "isi_cloud_object_id.h"

static void
object_id_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const isi_cloud_object_id *object_id =
	    static_cast<const isi_cloud_object_id *>(arg.ptr);
	ASSERT(object_id != NULL);

	object_id->fmt_conv(fmt, args);
}

struct fmt_conv_ctx
isi_cloud_object_id_fmt(const isi_cloud_object_id *object_id)
{
	struct fmt_conv_ctx ctx = { object_id_fmt_conv, { ptr: object_id } };
	return ctx;
}

isi_cloud_object_id::isi_cloud_object_id() : snapid_(0)
{
	set(0, 0);
}

isi_cloud_object_id::isi_cloud_object_id(uint64_t high, uint64_t low) :
    snapid_(0)
{
	set(high, low);
}

isi_cloud_object_id::isi_cloud_object_id(const btree_key &key)
	: snapid_(0)
{
	set(key.keys[0], key.keys[1]);
}

isi_cloud_object_id::isi_cloud_object_id(const isi_cloud_object_id &id) :
    snapid_(id.snapid_)
{
	set(id.high_, id.low_);
}

isi_cloud_object_id::~isi_cloud_object_id()
{
}

std::string
isi_cloud_object_id::get_cdo_name(int index) const
{
	char const* format = "_%08d";
	const int size = 10;	// including null termination
	char s_idx[size] = { 0 };

	ASSERT(index >= 0 && index < MAX_INDEX_);
	snprintf(s_idx, size, format, index);

	return string_form_ + s_idx;
}

std::string
isi_cloud_object_id::get_cmo_name(void) const
{
	return get_cdo_name(CMO_INDEX_);
}

void
isi_cloud_object_id::set(uint64_t high, uint64_t low)
{
	high_ = high;
	low_ = low;

	create_string_form(high_, low_);
}

void
isi_cloud_object_id::create_string_form(uint64_t high, uint64_t low) const
{
	unsigned char guid[GUID_SIZE_];////16个字节 char guid[16]
	memcpy(guid, &high_, sizeof(high_));
	memcpy(guid + sizeof(high_), &low_, sizeof(low_));

	create_string_form(guid);
}

void
isi_cloud_object_id::create_string_form(unsigned char guid[GUID_SIZE_]) const
{
	int d1, d2;
	char guid_string[GUID_SIZE_ * 2];

	for (unsigned int i = 0, j = 0; i < GUID_SIZE_; ++i) {
		d1 = guid[i] & 0x0F;
		d2 = (guid[i] >> 4) & 0x0F;

		guid_string[j++] = d1 > 9 ? ('a' + d1 - 10) : ('0' + d1);
		guid_string[j++] = d2 > 9 ? ('a' + d2 - 10) : ('0' + d2);
	}

	string_form_.assign(guid_string, GUID_SIZE_ * 2);
}

static inline void
pack(void **dst, const void *src, int size)
{
	memcpy(*dst, src, size);
	*(char **)dst += size;
}

static inline void
unpack(void **dst, void *src, int size)
{
	memcpy(src, *dst, size);
	*(char **)dst += size;
}

void
isi_cloud_object_id::generate(void)
{
	unsigned char guid[GUID_SIZE_];

	isi_guid_generate(guid, GUID_SIZE_);

	create_string_form(guid);

	memcpy(&high_, guid, sizeof(high_));
	memcpy(&low_, guid + sizeof(high_), sizeof(low_));
}

bool isi_cloud_object_id::from_string_form(const char *str)
{
	unsigned char guid[GUID_SIZE_];

	if (strlen(str) != GUID_SIZE_ * 2)
		return false;

#define HEX_IN_BIN(h) (h - ((h >= '0' && h <= '9') ? '0' : ('a' - 10)))
	bzero(guid, GUID_SIZE_);
	for (unsigned i = 0; i < GUID_SIZE_; ++i) {
		guid[i] = HEX_IN_BIN(str[i * 2]) |
		    HEX_IN_BIN(str[i * 2 + 1]) << 4;
	}

	string_form_ = str;
	memcpy(&high_, guid, sizeof(high_));
	memcpy(&low_, guid + sizeof(high_), sizeof(low_));

	return true;
}


isi_cloud_object_id &
isi_cloud_object_id::operator=(const isi_cloud_object_id &rhs)
{
	high_ = rhs.high_;
	low_ = rhs.low_;
	snapid_ = rhs.snapid_;
	string_form_.assign(rhs.string_form_);

	return *this;
}

bool
isi_cloud_object_id::operator==(const isi_cloud_object_id& rhs) const
{
	return ((high_ == rhs.high_) && (low_ == rhs.low_) &&
	    (snapid_ == rhs.snapid_));
}

bool
isi_cloud_object_id::operator!=(const isi_cloud_object_id& rhs) const
{
	return !(operator==(rhs));
}

bool
isi_cloud_object_id::operator<(const isi_cloud_object_id& rhs) const
{
	if (high_ < rhs.high_)
		return true;
	if (high_ > rhs.high_)
		return false;

	if (low_ < rhs.low_)
		return true;
	if (low_ > rhs.low_)
		return false;

	return snapid_ < rhs.snapid_;
}

size_t
isi_cloud_object_id::get_packed_size(void)
{
	return sizeof(uint64_t) +	// high_
	    sizeof(uint64_t) +		// low_
	    sizeof(uint64_t);		// snapid_
}

void
isi_cloud_object_id::pack(void **stream) const
{
	ASSERT(stream != NULL);

	::pack(stream, &high_, sizeof(high_));
	::pack(stream, &low_, sizeof low_);
	::pack(stream, &snapid_, sizeof snapid_);
}

void
isi_cloud_object_id::unpack(void **stream)
{
	ASSERT(stream != NULL);

	::unpack(stream, &high_, sizeof(high_));
	::unpack(stream, &low_, sizeof low_);
	::unpack(stream, &snapid_, sizeof snapid_);
	create_string_form(high_, low_);
}

const std::string &
isi_cloud_object_id::to_string(void) const
{
	return string_form_;
}

const char *
isi_cloud_object_id::to_c_string(void) const
{
	return to_string().c_str();
}

struct fmt_conv_ctx
isi_cloud_object_id::get_print_fmt(void) const
{
	return isi_cloud_object_id_fmt(this);
}

void
isi_cloud_object_id::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	if (args->pound_flag)
		fmt_print(fmt, "object ID: %s cloud snapID: %lu",
		    string_form_.c_str(), snapid_);
	else
		fmt_print(fmt, "%s/%lu",
		    string_form_.c_str(), snapid_);
}
