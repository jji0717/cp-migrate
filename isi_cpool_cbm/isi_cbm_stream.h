#pragma once

#include "isi_cbm_mapper.h"
#include "isi_cloud_api/isi_cloud_api.h"
#include "io_helper/isi_cbm_buffer.h"

using namespace isi_cloud;

/**
 * CDO upload stream implementation
 */
class data_istream : public idata_stream ////isi_cloud_api/cl_provider.h
{
public:
	virtual size_t read(void *buf, size_t len);
	void setfd(int fdin) { fd = fdin; }
	off_t setoffset(off_t offset)
	{
		if (fd < 0) {
			errno = EBADF;
			return -1;
		}
		set_pos = offset;
		return lseek(fd, offset, SEEK_SET);
	};
	virtual bool reset() { return setoffset(set_pos) != -1; }

	data_istream(int fdin) : fd (fdin), set_pos(0) {}
	data_istream() : fd (-1), set_pos(0) {}
private:
	int   fd;
	off_t set_pos; /**< stream position can be rewinded to via reset */
};

/**
 * CMO upload stream implementation
 */

//class cl_cmo_ostream : public isi_cloud::odata_stream
//这个类用来下载cmo

////isi_cbm_write.h 中定义了cmo的结构,通过meta_stream上传cmo,和libcurl交互
// struct isi_cmo_info {
// 	uint32_t version;
// 	ifs_lin_t lin;
// 	const struct stat *stats;
// 	isi_cfm_mapinfo *mapinfo;
// };

class meta_istream : public idata_stream  ////这个类用来上传cmo
{
public:
	virtual size_t read(void *buf, size_t len);

	virtual bool reset() {
		version_streamed = false;
		meta_streamed = false;
		mapinfo_streamed = false;
		ent_off = 0;
		local_buf.reset();
		return true;
	}

	void setmd(char *md) { metadata = md; } ////std::string metadata
	void setmi(isi_cfm_mapinfo &in_mapinfo)
	{
		mapinfo = &in_mapinfo;
	}
	void setvers(uint32_t in_version)
	{
		version = in_version;
	}

	void setst(const struct stat &in_stat) { m_stat = in_stat;}
	size_t length()
	{
		// The size of CMO version is added.
		//  metadata.length() + 1 because we are writing the string
		// with '/0' in the end.
		return sizeof(version) + sizeof(int) + metadata.size() + 1 +
		    sizeof(m_stat) + mapinfo->get_total_size();
	}

	meta_istream();

private:
	uint32_t version;
	std::string metadata;
	struct stat m_stat;
	isi_cfm_mapinfo *mapinfo; /////上传cmo到云端时,需要上传mapinfo

	// buffer to be streamed
	isi_cbm_buffer local_buf;
	
	// flag that version of the CMO was streamed.
	bool version_streamed;

	// flag that meta part of the CMO was streamed
	bool meta_streamed;
	// flag that mapinfo part of the CMO was streamed
	bool mapinfo_streamed;
	// offset of the next mapentry to be streamed
	off_t ent_off;
};


