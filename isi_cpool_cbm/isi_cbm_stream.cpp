
#include "isi_cbm_stream.h"
#include "isi_cbm_mapinfo_store.h"
#include "io_helper/encrypted_istream.h"

#include <string.h>

namespace {
	const size_t BUF_SZ = 64 * 1024;
}

size_t
data_istream::read(void *buf, size_t len)//////负责cdo的上传
{
	size_t dataread = 0;	// data read so far
	size_t dataleft = len;  // how much we have left to read
	ssize_t thisread = 0;	// how much we read in this pass.

	char *buffer = (char *) buf;

	while(dataread < len) {
		thisread = ::read(fd, &buffer[dataread], dataleft);
		/*
		 * Hit the end of the file, all done so just break out of
		 * the loop
		 */
		if (thisread == 0)
			break;

		/*
		 * Not quite as good.  Hit some type of unexpexcted error while
		 * trying to read the file.
		 */
		if (thisread < 0) {
			dataread = 0;
			break;
		}
		dataread += thisread;
		dataleft -= thisread;
	}

	if (dataread != len) {
		ilog(IL_NOTICE, "data read %ld expected, actual %ld, error: %s",
		    len, dataread, strerror(errno));
	}

	return dataread;
}


meta_istream::meta_istream() : version(CPOOL_INVALID_VERSION), mapinfo(NULL),
    local_buf(BUF_SZ), version_streamed(false),
    meta_streamed(false), mapinfo_streamed(false), ent_off(0)
{
	bzero(&m_stat, sizeof(m_stat));
}

/**
 * meta, mapinfo, entries are streamed to a local buffer which
 * will be copied out
 */
//////先把字节流写入local_buf(append, <<),再用local_buf.read(dest_buf + this_bytes, len) 即把local_buf中保存的字节流按字段返回到buf中。
/////这样就做到了把buf中的字节流写到libcurl中
size_t 
meta_istream::read(void *buf, size_t len/*总共期望读到的字节数量*/)  //////负责cmo(metadata, mapinfo)的上传
{
	struct isi_error *error = NULL;

	ASSERT(mapinfo);

	char *dest_buf = (char *)buf;
	bool encrypt = mapinfo->is_encrypted();
	int metadata_len = metadata.size();
	// actual bytes for this read call
	size_t this_bytes = 0;

	// drain the buffered data
	if (local_buf.get_size() > 0)
		this_bytes = local_buf.read(dest_buf, len);///把local_buf中剩下的字节拷贝到dest_buf中

	// Get the CMO version.
	if (this_bytes < len && !version_streamed) {
	  	local_buf.reset();
		local_buf << version;

		this_bytes += local_buf.read(
		    dest_buf + this_bytes, len - this_bytes);
		version_streamed = true;
	}


	// buffer and drain the meta data
	//////先放一个metadata_len,再放一个string的metadata,放一个0x00,放一个stat,最后全部memcpy到dest_buf中
	if (this_bytes < len && !meta_streamed) {
		ASSERT(sizeof(metadata_len) + metadata_len + 1 <= BUF_SZ);
		local_buf.reset();
		local_buf << metadata_len;

		if (metadata_len > 0) {
			if (encrypt) {
				encrypt_ctx ectx;
				iv_t iv;
				unsigned char emd[metadata_len];
				unsigned char *pmd = (unsigned char *)
				    const_cast<char *>(metadata.c_str());

				mapinfo->get_encryption_ctx(ectx, &error);
				ON_ISI_ERROR_GOTO(out, error);

				iv = ectx.get_chunk_iv();

				cpool_cbm_encrypt(pmd, metadata_len,
				    (unsigned char *)&ectx.dek,
				    (unsigned char *)&iv,
				    emd, &error);

				ON_ISI_ERROR_GOTO(out, error,
				    "failed to encrypt data");

				// encrypt file name
				local_buf.append(emd, metadata_len);
			}
			else
				local_buf << metadata;
		}

		// fill in 0x00 to make it consistent, this is a legacy space
		local_buf << (char) 0x00; /////在metadata和m_stat之间有一个空格
		local_buf << m_stat;

		this_bytes += local_buf.read(
		    dest_buf + this_bytes, len - this_bytes);
		meta_streamed = true;
	}

	// buffer the mapinfo data
	////先放一个packed的mapinfo,再memcpy到dest_buf中
	if (this_bytes < len && !mapinfo_streamed) {
		if (!mapinfo->get_packed_map() || (mapinfo->get_type() &
		    ISI_CFM_MAP_TYPE_WIP) != 0) {
			// Do not persist WIP info in the Cloud
			int map_type = mapinfo->get_type();
			mapinfo->set_type(map_type & (~ISI_CFM_MAP_TYPE_WIP));
			// no error expected, pack will not touch the store
			// the interface should be refactored.
			mapinfo->pack(isi_error_suppress());   ////先把mapinfo序列化
			mapinfo->set_type(map_type);
		}

		ASSERT((size_t)mapinfo->get_packed_size() < BUF_SZ);
		printf("%s called mapinfo->get_packed_size:%ld\n", __func__, mapinfo->get_packed_size());
		local_buf.reset();
		local_buf.append(
		    mapinfo->get_packed_map(), mapinfo->get_packed_size()); ////把序列化后的mapinfo字节流复制进buffer

		this_bytes += local_buf.read(
		    dest_buf + this_bytes, len - this_bytes);
		mapinfo_streamed = true;
	}

	if (mapinfo->is_overflow()) {
		// We are streaming the mapping entry for external storage, so
		// we need to record the offset in the mapping entry
		isi_cfm_mapinfo_store *store = mapinfo->get_store();
		int cnt = BUF_SZ / mapinfo->get_mapentry_pack_size(false);
		std::vector<isi_cfm_mapentry> entries(cnt);

		if (store == NULL) {
			store = mapinfo->open_store(false, false, NULL, &error);
			ON_ISI_ERROR_GOTO(out, error, "failed to open_store");
		}
		// buffer the entries
		while (this_bytes < len) {
			int ret_cnt = store->get(ent_off, entries.data(),
			    cnt, &error);
			ON_ISI_ERROR_GOTO(out, error,
			    "failed to get entries, off: %ld cnt: %d",
			    ent_off, cnt);

			local_buf.reset();

			for (int i = 0; i < ret_cnt; ++i) {
				entries[i].pack(mapinfo->get_type(), false);

				local_buf.append(entries[i].get_packed_entry(),
				    entries[i].get_packed_size());
			}

			if (ret_cnt > 0) {
				ent_off = entries[ret_cnt - 1].get_offset() +
				    entries[ret_cnt - 1].get_length();
			}

			this_bytes += local_buf.read(
			    dest_buf + this_bytes, len - this_bytes);

			if (ret_cnt < cnt)
				break;
		}
	}

 out:
	if (error) {
		ilog(IL_ERR, "failed to stream metadata %#{}",
		    isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
		return CODE_ABORT;
	}

	return this_bytes;
}
