#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cpool_cbm/isi_cbm_error.h"
#include "isi_cpool_cbm/isi_cbm_error_util.h"
#include "isi_cpool_cbm/isi_cbm_read.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "isi_cpool_cbm/io_helper/encrypt_filter.h"
#include <memory>

#define BUF_SIZE (16 * 1024)
#define BULK_PUT_CNT  100

namespace {
	// get primitive data at the specified address
	template<typename T>
	inline T value_at_off(void *buf, int off = 0)
	{
		return ((T *)((char *)buf + off))[0];
	}

	// move forward the buff some bytes
	inline void advance(void *&buff, size_t &len, int bytes)
	{
		buff = (char *)buff + bytes;
		len -= bytes;
	}
}

cl_cmo_ostream::cl_cmo_ostream() :
    version_streamed(false),
    meta_streamed(false), mapinfo_streamed(false), fill_in_store_(false),
	auto_delete_(false), local_buffer(BUF_SIZE/*16K字节*/), overwrite_(false)
{
	version_ = CPOOL_INVALID_VERSION;
	memset(&stats_, 0, sizeof(stats_));
}

cl_cmo_ostream::~cl_cmo_ostream()
{
	try {
		reset();
	}
	catch (...) {
		// Do not care about exceptions in this case.
	}
}
///local_buffer起的作用相当于是缓冲区.用来解析一个字段.
///当读到一部分字节流,还不够完全解析一个字段时,把这部分字节流保存到local_buffer中,下次再读取下一部分字节流,和local_buffer中的字节流一起解析
//    4byte  4byte标识path-len path-len长度的byte    1byte    sizeof(stats_) 4byte标识mapinfo长度  mapinfo
//  |-------|----------------|------------------|----------|---------------|------------------|----------|
//   version                                     空出1个字节
/////mapinfo的解析在isi_cfg_mapinfo::unpack中
size_t
cl_cmo_ostream::write(void *buff, size_t len)
{
	struct isi_error *error = NULL;
	// bytes take from the input buff
	size_t taken_bytes = 0;

	/*
	 * CMO version need to be streamed first.
	 */
	if (!version_streamed) {
		// parse cmo version.
		// If incoming buff stores only partial version information
		// (len < sizeof (version_), we will have to store partial
		// version in the local_buff and complete it in the next
		// cl_cmo_ostream::write
		ASSERT(buff);
		/////当前写入buff的字节数 + local_buf中的字节数 < sizeof(version_), 所以还不够解析version字段
		if (len + local_buffer.get_size() < sizeof(version_)) { 
			local_buffer.append(buff, len);
			taken_bytes += len;
			printf("%s called not enough byte to stream_version\n", __func__);
			goto out;
		}

		size_t bytes;
		if (local_buffer.get_size() == 0) { /////当前写入buff的字节数已经足够解析字段了,且local_buf中没有字节，那么可以直接用buff解析
			bytes = stream_version(buff);
		} else {////说明前一次拿到的字节数不够解析version,这一次拿到足够字节数了
			ASSERT(local_buffer.get_size() < sizeof (version_));
			bytes = sizeof(version_) - local_buffer.get_size();
			local_buffer.append(buff, bytes);///把剩下需要的字节数memcpy到local_buffer中
			stream_version(local_buffer.ptr());
		}
		advance(buff, len, bytes); ////把buff指针forward bytes个字节数, len -= bytes
		taken_bytes += bytes;
		local_buffer.reset(); /////重新让local_buf的指针指到local_buf的最前端:相当于清空local_buffer
		version_streamed = true;

		if (CPOOL_CMO_VERSION != get_version()) {
			error = isi_cbm_error_new(CBM_CMO_VERSION_MISMATCH,
			    "CMO Version mismatch. "
			    "Expected version %d found version %d",
			    CPOOL_CMO_VERSION, get_version());
			goto out;
		}
	}

	if (!meta_streamed) {
		int path_len = 0;
		size_t meta_len = 0;

		if (len + local_buffer.get_size() < sizeof(int)) {////读到的字节数不够解析,那么就把这些字节数暂时保存到local_buffer中
			local_buffer.append(buff, len);
			taken_bytes += len;
			printf("%s called not enough byte to meta_len\n",__func__);
			goto out;
		}

		// sizeof(int) bytes ready
		if (local_buffer.get_size() == 0)
			path_len = value_at_off<int>(buff);
		else {
			if (local_buffer.get_size() < sizeof(int)) {
				int bytes =
				    sizeof(int) - local_buffer.get_size();
				local_buffer.append(buff, bytes);
				advance(buff, len, bytes);
				taken_bytes += bytes;
			}
			path_len = value_at_off<int>(local_buffer.ptr());
		}
		meta_len = sizeof(int) + path_len + 1 + sizeof(stats_);

		if (local_buffer.get_size() + len < meta_len) {
			local_buffer.append(buff, len);
			taken_bytes += len;
			printf("%s called not enough byte to meta_stream\n", __func__);
			goto out;
		}

		// meta part of CMO all ready for parse
		size_t bytes = stream_meta(buff, path_len);

		advance(buff, len, bytes);
		taken_bytes += bytes;
		local_buffer.reset(); ///////meta解析完成,重置local_buffer
		meta_streamed = true;
	}

	if (!mapinfo_streamed) {
		// offset that mapinfo size was packed
		int size_off = isi_cfm_mapinfo::get_size_offset();
		size_t mapinfo_len = 0;
		if (len + local_buffer.get_size() < size_off + sizeof(int)) {
			local_buffer.append(buff, len);
			taken_bytes += len;
			printf("%s called not enough byte to stream_mapinfo\n", __func__);
			goto out;
		}

		// make sure 4 bytes int is either in local_buff or buff
		if (local_buffer.get_size() > 0 &&
		    local_buffer.get_size() < size_off + sizeof(int)) {
			int bytes = size_off + sizeof(int) -
			    local_buffer.get_size();

			local_buffer.append(buff, bytes);
			advance(buff, len, bytes);
			taken_bytes += bytes;
		}
		/////先用一个字节读出接下来的mapinfo的长度
		mapinfo_len = value_at_off<int>(local_buffer.get_size() > 0 ?
		    local_buffer.ptr() : buff, size_off);
		printf("%s called mapinfo_len:%ld size_off:%d len:%ld\n", __func__, mapinfo_len, size_off,len);
		if (len + local_buffer.get_size() < mapinfo_len) {
			local_buffer.append(buff, len);
			taken_bytes += len;
			printf("%s called len+local_buf < mapinfo_len\n", __func__);
			goto out;
		}

		// mapinfo all ready to parse
		size_t bytes = stream_mapinfo(buff, mapinfo_len, &error);

		ON_ISI_ERROR_GOTO(out, error);
		advance(buff, len, bytes);
		taken_bytes += bytes;
		local_buffer.reset();
		mapinfo_streamed = true;
	}

	if (fill_in_store_ && len > 0) {
		taken_bytes += stream_entries(buff, len, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	// store exists, we have to count in the entrties' size
	else if (!fill_in_store_) {
		taken_bytes += len;
	}

 out:
	if (error) {
		ilog(IL_ERR,
		    "failed to streaming CMO: %#{}", isi_error_fmt(error));
		isi_error_free(error);
	}

	if (taken_bytes == 0) {
		ilog(IL_ERR,
		    "CMO write failed to write any byte");
	}
	return taken_bytes;
}

bool
cl_cmo_ostream::reset()
{
	local_buffer.reset();
	path_ = "";
	version_streamed = false;
	meta_streamed = false;
	mapinfo_streamed = false;
	overwrite_ = false;
	fill_in_store_ = false;
	if (auto_delete_) {
		struct isi_error *error = NULL;
		mapinfo_.remove_store(&error);
		auto_delete_ = false;

		if (error)
			throw isi_exception(error);
	}

	return true;
}

size_t
cl_cmo_ostream::stream_version(void *buff)
{
	memcpy(&version_, buff, sizeof(version_));
	return sizeof(version_);
}

size_t
cl_cmo_ostream::stream_meta(void *buff, size_t path_len)
{
	char *meta = (char *)buff;
	int stat_off = sizeof(int) + path_len + 1;
	size_t meta_len = stat_off + sizeof(stats_);
	size_t taken_bytes = meta_len - local_buffer.get_size();

	ASSERT(taken_bytes > 0);

	// concatenate the meta part
	if (local_buffer.get_size() > 0) {
		local_buffer.append(buff, taken_bytes);
		meta = (char *)local_buffer.ptr();
	}

	path_buf_.resize(path_len);
	// encrypted path is to be decrypted later on
	memcpy(path_buf_.data(), meta + sizeof(int), path_len);
	memcpy(&stats_, meta + stat_off, sizeof(stats_));

	// bytes taken from input buff
	return taken_bytes;
}

size_t
cl_cmo_ostream::stream_mapinfo(
    void *buff, size_t mapinfo_len, struct isi_error **error_out)
{
	char *pmapinfo = (char *)buff;
	size_t taken_bytes = mapinfo_len - local_buffer.get_size();
	isi_cloud_object_id empty_id;

	ASSERT(taken_bytes > 0);

	// concatenate the mapinfo data if possible
	if (local_buffer.get_size() > 0) {
		local_buffer.append_resizable(buff, taken_bytes);
		pmapinfo = (char *)local_buffer.ptr();
	}
	mapinfo_.unpack(pmapinfo, mapinfo_len, error_out);/////isi_cbm_mapper.cpp:isi_cfm_mapinfo::unpack中反序列化
	ON_ISI_ERROR_GOTO(out, *error_out);

	// set the correct object id (CMO on Azure has obsolete object_id)
	if (object_id_ != empty_id)/////RAN不会调用,RAN的object_id在mapinfo::unpack中解析获得
		mapinfo_.set_object_id(object_id_);

	path_.append(path_buf_.data(), path_buf_.size());

 out:
	// bytes taken from input buff
	return taken_bytes;
}

size_t
cl_cmo_ostream::stream_entries(
    void *buff, size_t len, struct isi_error **error_out)
{
	// false: offset packed
	int entry_sz = mapinfo_.get_mapentry_pack_size(false);
	std::vector<isi_cfm_mapentry> entries(BULK_PUT_CNT);
	int cnt = 0;
	size_t bytes_taken = 0;

	// concatenate the first entry
	if (local_buffer.get_size() > 0) {
		size_t bytes = MIN(len,
		    entry_sz - local_buffer.get_size() % entry_sz);
		// concatenate the splitted entry
		local_buffer.append(buff, bytes);
		advance(buff, len, bytes);
		bytes_taken += bytes;

		if (local_buffer.get_size() == (size_t)entry_sz) {
			entries[0].unpack(local_buffer.ptr(),
			    mapinfo_.get_type(), error_out,
			    false /*offset packed*/);
			ON_ISI_ERROR_GOTO(out, *error_out);
			++cnt;
			local_buffer.reset();
		}
	}

	for (size_t i = 0; i < len / entry_sz; ++i) {
		entries[cnt].unpack(buff, mapinfo_.get_type(), error_out, false);
		ON_ISI_ERROR_GOTO(out, *error_out);

		if (++cnt == sizeof(entries.size())) {
			mapinfo_.get_store()->put(
			    entries.data(), cnt, error_out);
			ON_ISI_ERROR_GOTO(out, *error_out);
			cnt = 0;
		}

		buff = (char *)buff + entry_sz;
		bytes_taken += entry_sz;
	}

	if (cnt > 0) {
		mapinfo_.get_store()->put(entries.data(), cnt, error_out);
		ON_ISI_ERROR_GOTO(out, *error_out);
	}

	if (len % entry_sz) {
		local_buffer.append(buff, len % entry_sz);
		bytes_taken += len % entry_sz;
	}

 out:
	return bytes_taken;
}

size_t  //////从云端读数据到本地
isi_cph_read_data_blob(isi_cloud::odata_stream *pstrm, ifs_lin_t lin,
    size_t chunk_size,
    isi_cpool_master_cdo &master_cdo,
    off_t offset, size_t read_size,
    bool compressed, bool checksum, encrypt_ctx_p ectx,
    isi_cbm_ioh_base *ioh, struct isi_error **error_out)
{
	size_t bytes_read = 0, bytes_to_read = read_size;
	int index = -1;
	cl_data_range range;
	isi_cloud::str_map_t cdo_attrs;
	cl_object_name cloud_object_name;
	std::string entityname;
	cpool_events_attrs *cev_attrs = new cpool_events_attrs;
	struct isi_error *error = NULL;
	cloud_object_name = master_cdo.get_cloud_object_name();

	try {
		while (bytes_to_read > 0) {
			/*
			 * Determine which CDO and the range within that CDO to
			 * retrieve.
			 */
			index = (offset + bytes_read) / chunk_size + 1;
			range.start = (offset + bytes_read) % chunk_size;
			range.length = MIN(chunk_size - range.start,
			    bytes_to_read);

			entityname = get_entityname(
			    cloud_object_name.container_cdo,
			    ioh->get_cdo_name(cloud_object_name.obj_base_name,
			    index));

			set_cpool_events_attrs(cev_attrs, lin, offset,
			    ioh->get_account().account_id, entityname);

			ioh->get_cdo(master_cdo, index, compressed, checksum,
			    ectx, &range, NULL, cdo_attrs, *pstrm);

			bytes_read += range.length;
			bytes_to_read -= range.length;
		}

		// This call will trigger fail point and
		// generate error for the generation of
		// cloudpool_event.
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
	delete cev_attrs;

	isi_error_handle(error, error_out);
	return bytes_read;
}

void
isi_cph_read_md_blob(isi_cloud::odata_stream &pstrm,
    ifs_lin_t lin,
    const cl_object_name &cloud_object_name, isi_tri_bool compressed,
    isi_tri_bool checksum,
    isi_cbm_ioh_base *ioh, bool send_celog_event,
    struct isi_error **error_out)
{
	isi_cloud::str_map_t attr_map;
	struct isi_error *error = NULL;
	try {
		//////读到并且解析好的数据保存在pstrm中
		ioh->get_cmo(cloud_object_name, compressed, checksum, attr_map, pstrm);
	}

	isi_error_handle(error, error_out);
}

