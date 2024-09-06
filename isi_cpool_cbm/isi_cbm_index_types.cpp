#include "isi_cbm_index_types.h"

using namespace isi_cloud;

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

lin_snapid::lin_snapid()
        : lin_(INVALID_LIN), snapid_(INVALID_SNAPID)
{
}

lin_snapid::lin_snapid(ifs_lin_t lin, ifs_snapid_t snapid)
        : lin_(lin), snapid_(snapid)
{
}

lin_snapid::lin_snapid(const lin_snapid &rhs)
        : lin_(rhs.lin_), snapid_(rhs.snapid_)
{
}

lin_snapid &lin_snapid::operator=(const lin_snapid &rhs)
{
	lin_ = rhs.lin_;
	snapid_ = rhs.snapid_;

	return *this;
}

lin_snapid::~lin_snapid()
{
}

bool
lin_snapid::operator<(const lin_snapid &rhs) const
{
	if (lin_ < rhs.lin_)
		return true;
	if (lin_ == rhs.lin_ && snapid_ < rhs.snapid_)
		return true;

	return false;
}

size_t
lin_snapid::get_packed_size(void)
{
	return sizeof(ifs_lin_t) + sizeof(ifs_snapid_t);
}

void
lin_snapid::pack(void **stream) const
{
	::pack(stream, &lin_, sizeof lin_);
	::pack(stream, &snapid_, sizeof snapid_);
}

void
lin_snapid::unpack(void **stream)
{
	::unpack(stream, &lin_, sizeof lin_);
	::unpack(stream, &snapid_, sizeof snapid_);
}

struct fmt_conv_ctx
lin_snapid::get_print_fmt(void) const
{
	return lin_snapid_fmt(lin_, snapid_);
}
