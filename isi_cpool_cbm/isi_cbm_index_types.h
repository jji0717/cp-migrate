#ifndef __ISI_CBM_INDEX_TYPES_H__
#define __ISI_CBM_INDEX_TYPES_H__

#include <ifs/ifs_types.h>

namespace isi_cloud {

class lin_snapid
{
private:
	ifs_lin_t lin_;
	ifs_snapid_t snapid_;

public:
	lin_snapid();
	lin_snapid(ifs_lin_t lin, ifs_snapid_t snapid);
	lin_snapid(const lin_snapid &rhs);
	lin_snapid& operator=(const lin_snapid &rhs);
	~lin_snapid();

	inline ifs_lin_t get_lin(void) const { return lin_; }
	inline ifs_snapid_t get_snapid(void) const { return snapid_; }

	/* for std::set */
	bool operator<(const lin_snapid &rhs) const;

	/* for [de]serialization */
	static size_t get_packed_size(void);
	void pack(void **stream) const;
	void unpack(void **stream);

	struct fmt_conv_ctx get_print_fmt(void) const;
};

} // end namespace isi_cloud

#endif // __ISI_CBM_INDEX_TYPES_H__
