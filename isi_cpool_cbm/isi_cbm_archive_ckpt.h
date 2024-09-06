#ifndef __ISI_CBM_ARCHIVE_CKPT_H__
#define __ISI_CBM_ARCHIVE_CKPT_H__

#include <isi_cloud_common/isi_cpool_version.h>
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/isi_cbm_error.h"

/*
 * ckpt_state values
 */
#define CKPT_STATE_NOT_SET	0x0000
#define	CKPT_INVALIDATE_NEEDED	0x0001
#define CKPT_COI_NEEDED		0x0002
////在isi_cbm_archive中使用 set_ckpt_data_func, get_ckpt_data_func
////在isi_cbm_gc中的cloud gc中两次使用到 set_ckpt_data_func,get_ckpt_data_func
class isi_cbm_archive_ckpt 
{
public:
	isi_cbm_archive_ckpt():
		version_(CPOOL_CBM_ARCHIVE_CKPT_VERSION),
		filerev(0),
		packed_size(0),
		mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO),
		serialized_ckpt(NULL),
		ckpt_state(CKPT_STATE_NOT_SET),
		offset(0)
	{
	}

	isi_cbm_archive_ckpt(u_int64_t in_filerev,
	    off_t in_offset,
	    const isi_cfm_mapinfo& in_mapinfo,
	    struct isi_error **error_out) :
		version_(CPOOL_CBM_ARCHIVE_CKPT_VERSION),
		packed_size(0),
		mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO),
		serialized_ckpt(NULL),
		ckpt_state(CKPT_STATE_NOT_SET)
	{
		mapinfo.copy(in_mapinfo, error_out);
		filerev = in_filerev;
		offset = in_offset;

	}

	~isi_cbm_archive_ckpt()
	{
		if (serialized_ckpt != NULL) {
			free (serialized_ckpt);
			serialized_ckpt = NULL;
		}
	}

	void pack(struct isi_error **error_out)
	{
		void *minfo_data = NULL;
		isi_error *error = NULL;
		int mapinfo_packed_size_1 = 0;
		int mapinfo_packed_size_2 = 0;

		int ckpt_hdr_size =  sizeof(version_) +
		    sizeof(filerev) +
		    sizeof(ckpt_state) +
		    sizeof(offset);

		char *my_data = NULL;

		if (serialized_ckpt != NULL) {
			free (serialized_ckpt);
			serialized_ckpt = NULL;
		}
		
		mapinfo_packed_size_1 = mapinfo.get_pack_size(&error);
		ON_ISI_ERROR_GOTO(out, error);
		
		serialized_ckpt = calloc(1, mapinfo_packed_size_1 +
		    ckpt_hdr_size);
		ASSERT(serialized_ckpt != NULL);
		
		// Construct the serialized buffer with version and other info before the
		// mapinfo.
		my_data = (char *)serialized_ckpt;
		packit((void **) &my_data, &version_, sizeof(version_));
		packit((void **) &my_data, &filerev, sizeof(filerev));
		packit((void **) &my_data, &ckpt_state, sizeof(ckpt_state));/////在cloud gc中只关心ckpt_state, offset
		packit((void **) &my_data, &offset, sizeof(offset)); 

		mapinfo.pack(mapinfo_packed_size_2, &minfo_data, &error); /////在cloud gc中假设用mapentry替代mapinfo
		ON_ISI_ERROR_GOTO(out, error);
		
		// The pack size we calculated before to allocate the serialized buffer space
		// and pack size returned when actually constructing the map info should be the same.
		ASSERT(mapinfo_packed_size_1 == mapinfo_packed_size_2);
		
		packit((void **) &my_data, minfo_data, mapinfo_packed_size_1);
		if (minfo_data != NULL) {
			free(minfo_data);
			minfo_data = NULL;
		}

		packed_size = mapinfo_packed_size_1 + ckpt_hdr_size;///长度: offset+state+mapinfo

	 out:
		if (error && (serialized_ckpt != NULL)) {
			free(serialized_ckpt);
			serialized_ckpt = NULL;
		}
		if (minfo_data != NULL) {
			free(minfo_data);
			minfo_data = NULL;
		}
		isi_error_handle(error, error_out);
	}

	void unpack(const void *ckpt_data, size_t buf_size,
	    isi_error **error_out)
	{
		isi_error *error = NULL;
		char *my_data = (char *)ckpt_data;

		unpackit((void **)&my_data, &version_, sizeof(version_));
		if (CPOOL_CBM_ARCHIVE_CKPT_VERSION != get_version()) {
			error = isi_cbm_error_new(CBM_ARCHIVE_CKPT_VERSION_MISMATCH,
			    "Archive checkpoint version mismatch. expected version %d "
			    "found version %d", CPOOL_CBM_ARCHIVE_CKPT_VERSION,
			    get_version());
			goto out;
		}

		unpackit((void **)&my_data, &filerev, sizeof(filerev));
		unpackit((void **)&my_data, &ckpt_state, sizeof(ckpt_state)); /////在cloud gc中只要考虑ckpt_state, offset
		unpackit((void **)&my_data, &offset, sizeof(offset));

		mapinfo.unpack((void *) my_data, buf_size, error_out);
		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to unpack the mapinfo");
			return;
		}

		mapinfo.pack(error_out);
		if (*error_out) {
			isi_error_add_context(*error_out,
			    "Failed to pack the mapinfo");
			return;
		}

	 out:
		isi_error_handle(error, error_out);
		return;
	}
	uint32_t get_version()
	{
		return version_;
	}
	u_int64_t get_filerev()
	{
		return filerev;
	}
	isi_cfm_mapinfo &get_mapinfo()
	{
		return mapinfo;
	}
	int get_packed_size()
	{
		return packed_size;
	}

	void *get_serialized_ckpt()
	{
		return serialized_ckpt;
	}

	u_int32_t get_ckpt_state()
	{
		return ckpt_state;
	}

	void set_ckpt_state(u_int32_t st)
	{
		ckpt_state = st;
	}

	off_t get_offset()
	{
		return offset;
	}

	void set_offset(off_t in_offset)
	{
		offset = in_offset;
	}

private:
	inline void
	packit(void **dst, const void *src, int sz)
	{
		if (sz > 0) {
			memcpy(*dst, src, sz);
			*(char **)dst += sz;
		}
	}

	inline void
	unpackit(void **src, void *dst, int sz)
	{
		if (sz > 0) {
			memcpy(dst, *src, sz);
			*(char **)src += sz;
		}
	}

	u_int32_t version_;
	u_int64_t filerev;
	int packed_size;
	isi_cfm_mapinfo mapinfo;
	void *serialized_ckpt;
	u_int32_t ckpt_state;
	off_t offset;
};
#endif
