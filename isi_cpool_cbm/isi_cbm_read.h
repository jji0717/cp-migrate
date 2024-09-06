#ifndef __ISI_CBM_READ_H__
#define __ISI_CBM_READ_H__

#include <cstddef>

#include <ifs/ifs_types.h>
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/isi_cbm_data_stream.h"
#include "io_helper/encrypt_ctx.h"
#include "io_helper/isi_cbm_buffer.h"

// pre-declaration
struct cl_object_name;
class isi_cbm_ioh_base;
struct isi_error;

/** TODO: consider remove this as we have a version accepts data stream */
/**
 * @param[in] fd		a file descriptor to an open file where the data
 * 				will be written as it is retrieved from the
 * 				cloud
 * @param[in] chunk_size	the chunk size used to archive the data
 * @param[in] object_name	the base name of the object(s) to retrieve
 * @param[in] offset		the offset into the file from which to start
 *				retrieving data
 * @param[in] read_size		the amount of data to read
 * @param[in] ioh		io helper
 * @param[out] error_out	error information
 */
size_t isi_cph_read_data_blob(int fd, size_t chunk_size, const char* cmo_container,
    std::string cdo_container, std::string object_name, off_t offset,
    size_t read_size, isi_cbm_ioh_base *ioh, struct isi_error **error_out);


/**
 * @param pstrm[IN/OUT]: 	the data output stream object to receive the
 *                    		content read from cloud. It must be capable to
 *                    		accept @length of bytes
 * @param[in] lin               lin of the file
 * @param[in] chunk_size	the chunk size used to archive the data
 * @param[in] master_cdo	the master CDO
 * @param[in] offset		the offset into the file from which to start
 *				retrieving data
 * @param[in] read_size		the amount of data to read
 * @param[in] compressed	whether the object is compressed
 * @param[in] checksum		whether the object is with checksum
 * @param[in] ectx		decryption parameters, NULL on no encryption
 * @param[in] ioh		io helper
 * @param[out] error_out	error information
 */
size_t isi_cph_read_data_blob(isi_cloud::odata_stream *pstrm, ifs_lin_t lin, 
    size_t chunk_size,
    isi_cpool_master_cdo &master_cdo,
    off_t offset, size_t read_size,
    bool compressed, bool checksum, encrypt_ctx_p ectx,
    isi_cbm_ioh_base *ioh, struct isi_error **error_out);

class cl_cmo_ostream : public isi_cloud::odata_stream { ///////继承自cl_provider.h 从云端把数据拿下来
public:
	cl_cmo_ostream();
	~cl_cmo_ostream();
	virtual size_t write(void *buff, size_t len);/////cl_provider_common:curl_write_callback_resp里面调用write

	virtual bool reset();

	const std::string &get_path()
	{
		return path_;
	}

	const uint32_t &get_version()
	{
		return version_;
	}
	
	const struct stat &get_stats()
	{
		return stats_;
	}

	const isi_cfm_mapinfo &get_mapinfo()
	{
		return mapinfo_;
	}

	void set_object_id(const isi_cloud_object_id &object_id)
	{
		this->object_id_ = object_id;
	}

	void set_suffix(const std::string &suffix) {
		suffix_ = suffix;
	}

	const std::string &get_suffix()
	{
		return suffix_;
	}

	void set_auto_delete(bool auto_delete) {
		auto_delete_ = auto_delete;
	}

	bool get_auoto_delete() {
		return auto_delete_;
	}

	void set_overwrite(bool overwrite) {
		overwrite_ = overwrite;
	}

	bool get_overwrite() {
		return overwrite_;
	}

private:
	size_t stream_version(void *buff);
	size_t stream_meta(void *buff, size_t path_len);

	size_t stream_mapinfo(void *buff, size_t mapinfo_len,
	    struct isi_error **error_out);

	size_t stream_entries(void *buff, size_t len,
	    struct isi_error **error_out);

private:
  	uint32_t version_;
	std::vector<char> path_buf_;
	std::string path_;
	struct stat stats_;
	isi_cfm_mapinfo mapinfo_; //////理解为一个std::map<>

	// flag that version part of the CMO was streamed.
	bool version_streamed;
	
	// flag that meta part of the CMO was streamed
	bool meta_streamed;
	// flag that mapinfo part of the CMO was streamed
	bool mapinfo_streamed;
	// flag that store is going to be re-generated
	bool fill_in_store_;
	// flag that store is going to be re-auto-deleted
	bool auto_delete_;

	isi_cbm_buffer local_buffer;

	isi_cloud_object_id object_id_;

	// if the mapping info overflows to an SBT, what is the customizable
	// name
	std::string suffix_;
	bool overwrite_; // if to overwrite existing ones
};

void
isi_cph_read_md_blob(isi_cloud::odata_stream &pstrm, ifs_lin_t lin,
    const cl_object_name &cloud_name, isi_tri_bool compressed,
    isi_tri_bool checksum,
    isi_cbm_ioh_base *ioh, bool send_celog_event,
    struct isi_error **error_out);
#endif // __ISI_CBM_READ_H__
