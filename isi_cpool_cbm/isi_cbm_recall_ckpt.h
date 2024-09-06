#ifndef __ISI_CBM_RECALL_CKPT_H__
#define __ISI_CBM_RECALL_CKPT_H__

#include <string>
#include <boost/utility.hpp>
#include <isi_cloud_common/isi_cpool_version.h>
#include "isi_cpool_cbm/isi_cbm_error.h"

//CKPT MAGIC "rckp"
#define CPOOL_CBM_RECALL_CKPT_MAGIC 0x72636b70

// A class modeling the checkpoint information for recall
class recall_ckpt : boost::noncopyable {
public:
	recall_ckpt() :
	    magic_(CPOOL_CBM_RECALL_CKPT_MAGIC),
	    version_(CPOOL_CBM_RECALL_CKPT_VERSION),
	    lin_(0),
	    snapid_(0),
	    offset_(0)
	{
	}

	/**
	 * deserialize from a blob
	 */
	void deserialize(const void *buffer, size_t len,
	    struct isi_error **error_out);

	/**
	 * serialize the data to a blob
	 */
	void serialize(void **serialized_buff, int *serialized_size);

	uint32_t get_magic()
	{
		return (magic_);
	}

	uint32_t get_version()
	{
		return (version_);
	}

	uint64_t get_lin()
	{
		return (lin_);
	}

	void set_lin(uint64_t lin)
	{
		lin_ = lin;
	}

	uint64_t get_snapid()
	{
		return (snapid_);
	}

	void set_snapid(uint64_t snapid)
	{
		snapid_ = snapid;
	}
	off_t get_offset()
	{
		return (offset_);
	}

	void set_offset(off_t offset)
	{
		offset_ = offset;
	}

private:
	inline void
	pack(void **dst, const void *src, int sz)
	{
		if (sz > 0) {
			memcpy(*dst, src, sz);
			*(char **)dst += sz;
		}
	}

	inline void
	unpack(void **src, void *dst, int sz)
	{
		if (sz > 0) {
			memcpy(dst, *src, sz);
			*(char **)src += sz;
		}
	}

	inline int
	get_ckpt_size()
	{
		return (sizeof(magic_) + sizeof(version_) +
		    sizeof(lin_) + sizeof(snapid_) + sizeof(offset_));
	}

	uint32_t magic_;
	uint32_t version_;
	uint64_t lin_;
	uint64_t snapid_;
	off_t offset_;
};

/**
 * class managing the recall ckpt
 */
class recall_ckpt_mgr {
public:

	recall_ckpt_mgr(void *opaque, get_ckpt_data_func_t get_ckpt_data_func,
	    set_ckpt_data_func_t set_ckpt_data_func) :
	    get_ckpt_cb_(get_ckpt_data_func),
		set_ckpt_cb_(set_ckpt_data_func),
		task_ckpt_opaque_(opaque)
	{
	}

	/**
	 * Load the ckpt
	 * @return true if ckpt is found, false if error or no ckpt
	 */
	bool load_ckpt(struct isi_error **error_out);

	/**
	 * save the ckpt
	 */
	void save_ckpt(uint64_t lin, uint64_t snapid, off_t offset,
	    struct isi_error **error_out);

	uint64_t get_recall_ckpt_lin()
	{
		return (recall_ckpt_.get_lin());
	}

	uint64_t get_recall_ckpt_snapid()
	{
		return (recall_ckpt_.get_snapid());
	}

	off_t get_recall_ckpt_offset()
	{
		return (recall_ckpt_.get_offset());
	}

private:
	get_ckpt_data_func_t get_ckpt_cb_;
	set_ckpt_data_func_t set_ckpt_cb_;
	void *task_ckpt_opaque_;
	recall_ckpt recall_ckpt_;
};

#endif
