
/**
 * @file
 *
 * Cloud IO helper base class for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include <sstream>

#include "isi_cbm_ioh_base.h"
#include "md5_filter.h"
#include "range_filter.h"
#include "checksum_filter.h"
#include "sparse_area_filter.h"
#include "checksum_cache.h"
#include "checksum_istream.h"
#include "compress_filter.h"
#include "encrypt_filter.h"
#include "compressed_istream.h"
#include "encrypted_istream.h"
#include "headered_istream.h"
#include "checksum_header.h"
#include "header_ostream.h"
#include "isi_cpool_cbm/isi_cbm_error.h"
#include <isi_cpool_stats/isi_cpool_stats_api_cb.h>
#include <isi_cloud_api/verify_etag.h>

#include <isi_ilog/ilog.h>
#include <isi_util/isi_guid.h>
#include <memory>
#include <isi_ufp/isi_ufp.h>

using namespace isi_cloud;

namespace {

// the max and min size selected to fit well-known providers
const unsigned int MAX_CONTAINER_NAME_SIZE = 63;
const unsigned int MIN_CONTAINER_NAME_SIZE = 3;

checksum_cache cached_checksum;

} // end of anonymous namespace

/**
 * Cleanup the checksum, dedicated to pass the unit test
 */
void checksum_cache_cleanup()
{
	cached_checksum.cleanup();
}

/**
 * Get a checksum cache entry. This function is for the use of
 * the unit tests
 */
bool checksum_cache_get(const std::string &container,
    const std::string &object_id, off_t off,
    checksum_value &value, sparse_area_header_t &spa)
{
	bool cache_hit;

	cache_hit = cached_checksum.get(container, object_id, off,
	    value, spa);

	if (cache_hit) {
		spa.trace_spa("cache hit in checksum_cache_get",
		    __FUNCTION__, __LINE__);
	}
	return cache_hit;
}

/**
 * Constructs the entity name from the container and the object name.
 * @param input[in] container name
 * @param input[in] object name
 * @param entityname[out] entityname with container and object name.
 */
std::string
get_entityname(std::string container, std::string objname)
{
	std::string entityname;
	std::stringstream temp;
	temp << container;
	temp << "/";
	temp << objname;
	entityname.clear();
	entityname.append(temp.str());
	return entityname;
}


/** Check if HTTPS is used for the account */
bool
is_https_account(const isi_cbm_account &acct)
{
	static const char *HTTPS = "HTTPS://";
	if (!strncasecmp(acct.url.c_str(), HTTPS, strlen(HTTPS)))
		return true;
	return false;
}

///////////////////////////////////////////////////////////////////////////////

isi_cbm_exception::~isi_cbm_exception() throw()
{
}


///////////////////////////////////////////////////////////////////////////////

isi_cbm_ioh_base::isi_cbm_ioh_base(const isi_cbm_account &acct)
	: acct_(acct)
{
	// CBM has 8K minimize readsize, we can't round it up to 0
	// and make it default to 128K

	// multiple of 256K
	// acct_.chunk_size &= ~(size_constants::SIZE_256KB - 1);
 	// multiple of 128K
	// acct_.read_size &= ~(size_constants::SIZE_128KB - 1);
	int i = 0;
	i++;
	ilog(IL_INFO, "%s called", __func__);
}

isi_cbm_ioh_base::~isi_cbm_ioh_base()
{
	int i = 0;
	i++;
	ilog(IL_INFO, "%s called", __func__);
}



bool
isi_cbm_ioh_base::validate_container_name(const std::string &container)
{
	size_t length = container.length();

	if (length > MAX_CONTAINER_NAME_SIZE ||
	    length < MIN_CONTAINER_NAME_SIZE)
		return false;

	const char *pc = container.c_str();

	if (*pc == '-' || *(pc + length -1) == '-')
		return false;
	if (container.find("--") != std::string::npos)
		return false;

	for (size_t i = 0; i < length; ++i, ++pc) {
		if (islower(*pc) || isdigit(*pc) || *pc == '-')
			continue;
		else
			return false;
	}

	return true;
}


// default implementations

void
isi_cbm_ioh_base::generate_cloud_name(const ifs_lin_t lin,
    cl_object_name &cloud_name)
{
	//  TBD: lin may be used in derived class implementation

	cloud_name.obj_base_name.generate();//////调用isi_cloud_object_id.cpp::generate产生object_id
        cloud_name.container_cmo = acct_.container_cmo;///m005056a0916e53b01466e019940bbe79f8e4i2
        cloud_name.container_cdo = acct_.container_cdo;////d005056a0916e53b01466e019940bbe79f8e4i2
}

void
isi_cbm_ioh_base::write_object_with_retry(const std::string &container,
    const std::string &object_id,
    off_t sub_object_offset,
    const str_map_t &attr_map,
    size_t length,
    bool overwrite,
    bool random_io,
    bool compress,
    bool with_checksum,
    encrypt_ctx_p ectx,
    size_t chunksize,
    size_t readsize,
    bool is_cmo,
    idata_stream &istrm,  ////////读到的本地数据,将要被上传到云端
    uint32_t sparse_resolution,
    int sparse_map_len,
    const uint8_t *sparse_map)
{
	int repeated = 0;
	bool retry = false;
	bool encrypt = (ectx != NULL);
	bool compressed = false;
	int num_subchunk = compress ? 1 : chunksize / readsize;
	size_t sz_subchunk = compress ? chunksize : readsize;

	std::string md5;

	std::auto_ptr<encrypted_istream> e_strm;
	std::auto_ptr<compressed_istream> c_strm;
	std::auto_ptr<checksum_istream> cksum_strm;
	idata_stream *pstrm = &istrm;

	std::auto_ptr<headered_istream> h_strm;

	// CMO and CDO always has the header
	// cancel the alignment on compression
	h_strm.reset(new headered_istream(is_cmo, compress, with_checksum,
	    compress? 0 : get_alignment(), num_subchunk, sparse_resolution,
	    sparse_map_len, sparse_map));

	if (h_strm.get()) {
		h_strm->set_compression(compressed);
		h_strm->set_compressed_len(length);
	}

	length = h_strm->apply(pstrm, length);
	pstrm = h_strm.get();

	if (!is_https_account(acct_)) {
		// if we compress or encrypt the input, then do it after.
		md5_calc(*pstrm, length, md5);
		pstrm->reset();
	}

	// Remove the cache entry containing the sparse map if it exists
	cached_checksum.invalidate(container, object_id, sub_object_offset);

	while (true) {
		try {
			put_object(container, object_id, attr_map,
			    length, overwrite, random_io, md5, pstrm);
			break;
		} catch (cl_exception &exp) {
			// This is lower layer md5 mismatch error by provider
			// If we don't use md5 in lower layer, we won't be here
			// We will also retry for failed etag matches.
			if (((exp.get_error_code() == CL_MD5_MISMATCH) ||
			    (exp.get_error_code() == CL_ETAG_MD5_MISMATCH) ||
			    (exp.get_error_code() == CL_ETAG_INVALID_FORMAT))
			    && repeated == 0 && pstrm->reset()) {
				retry = true;
				++repeated;
				ilog(IL_INFO, "To retry due to "
				    "error code=%d, msg=%s",
				    exp.get_error_code(), exp.what());
				continue;
			}
			throw;
		}
	}
}

void
isi_cbm_ioh_base::write_cmo(const cl_object_name &cloud_name,
    const isi_cloud::str_map_t &attr_map,
    size_t length, isi_cloud::idata_stream &istrm,
    bool overwrite, bool compress, bool checksum)
{
	// set chunk/read size to length so to calculate a single checksum
	// and encrypt the whole cmo without slicing it
	size_t chunk_sz = length;
	size_t read_sz = length;

	// we don't encrypt cmo, that's why encrypt_ctx is not
	// passed in, and default to NULL
	// file name encryption is done before code here
	write_object_with_retry(cloud_name.container_cmo,
	    get_cmo_name(cloud_name.obj_base_name), 0, attr_map,
	    length, overwrite, false, compress, checksum,
	    NULL, chunk_sz, read_sz, true, istrm, 0, 0, NULL);
}


void
isi_cbm_ioh_base::write_cdo(isi_cpool_master_cdo &blob_info,
    int index, size_t length, isi_cloud::idata_stream &istrm,
    bool overwrite, bool random_io, bool compress, bool checksum,
    encrypt_ctx_p ectx)
{
	// set the chunk index here
	if (ectx)
		ectx->set_chunk_idx(index);

	write_object_with_retry(blob_info.cloud_name.container_cdo, /////container_path
	    get_cdo_name(blob_info.cloud_name.obj_base_name, index), /////obj_base_name是一个isi_cloud_object_id
	    get_cdo_physical_offset(blob_info, index, compress,
	    checksum),
	    blob_info.attr_map,
	    length, overwrite, random_io, compress, checksum, ectx,
	    blob_info.get_chunksize(), blob_info.get_readsize(), false,
	    istrm, blob_info.get_sparse_resolution(),
	    blob_info.get_sparse_map_length(), blob_info.get_sparse_map_ptr());
}


void
isi_cbm_ioh_base::clone_object(const cl_object_name &src_name,
    const cl_object_name &tgt_name, int index)
{
	std::string snap_name;
	std::string tgt_container_path("");
	std::string src_container_path("");
	std::string tgt_object_id = get_cdo_name(tgt_name.obj_base_name, index);
	std::string src_object_id = get_cdo_name(src_name.obj_base_name, index);
	callback_params cb_params;
	int new_container_levels;

	new_container_levels = build_container_path(
	    tgt_name.container_cdo,
	    tgt_object_id,
	    tgt_container_path);
	build_container_path(
	    src_name.container_cdo,
	    src_object_id,
	    src_container_path);

	return get_provider()->clone_object(get_account(),
	    tgt_container_path,
	    tgt_object_id,
	    src_container_path,
	    src_object_id,
	    true,
	    snap_name,
	    cb_params,
	    new_container_levels);
}

void
isi_cbm_ioh_base::update_cdo(const isi_cpool_master_cdo &blob_info, int index,
    const cl_data_range &range, isi_cloud::idata_stream *istrm, bool compress,
    bool checksum, encrypt_ctx_p ectx)
{
}

void
isi_cbm_ioh_base::get_cmo(const cl_object_name &cloud_name,
    isi_tri_bool compressed,
    isi_tri_bool with_checksum, isi_cloud::str_map_t &attr_map,
    isi_cloud::odata_stream &ostrm)
{
	get_object_with_retry(cloud_name.container_cmo,
	    get_cmo_name(cloud_name.obj_base_name),
	    0, 0, 0, cloud_name.obj_base_name.get_snapid(),
	    compressed, with_checksum, NULL,
	    get_chunk_size(false), get_read_size(), true,
	    attr_map, ostrm);

}

inline off_t
get_chunk_len(off_t length, int index, off_t chunksize)
{
	return  MIN(length - index * chunksize, chunksize);
}

void
isi_cbm_ioh_base::get_cdo_headers(
    const isi_cpool_master_cdo &master_cdo, int index,
    bool compressed, bool with_checksum,
    checksum_header &cs, sparse_area_header_t &spa)
{
	const cl_object_name &cloud_name = master_cdo.get_cloud_object_name();

	get_object_headers_with_retry(cloud_name.container_cdo,
	    get_physical_cdo_name(master_cdo, index),
	    get_cdo_physical_offset(master_cdo, index, compressed,
	    with_checksum),
	    master_cdo.get_snapid(),
	    compressed, with_checksum,
	    master_cdo.get_chunksize(), master_cdo.get_readsize(),
	    false, cs, spa);
}

void
isi_cbm_ioh_base::get_cdo(const isi_cpool_master_cdo &master_cdo, int index,
    bool compressed, bool with_checksum, encrypt_ctx_p ectx,
    const cl_data_range *range,
    const str_map_t *cmo_attr_to_match,
    isi_cloud::str_map_t &attr_map,
    isi_cloud::odata_stream &ostrm)
{
	const cl_object_name &cloud_name = master_cdo.get_cloud_object_name();
	off_t offset = 0;
	size_t length = 0;

	if (range) {
		offset = range->start;
		length = range->length;
	}
	if (cmo_attr_to_match && cmo_attr_to_match->size() > 0)
		this->match_cmo_attr(cmo_attr_to_match, cloud_name);

	if (ectx)
		ectx->set_chunk_idx(index);

	if (range && (with_checksum || ectx || compressed)) {
		// make sure read to the cloud is properly aligned
		size_t read_size =  compressed ?
		    master_cdo.get_chunksize() : master_cdo.get_readsize();
		int start_idx = offset / read_size;
		size_t adj_off = start_idx * read_size;
		size_t adj_end = ((offset + length - 1) / read_size + 1) *
		    read_size;

		size_t chunk_len = get_chunk_len((size_t)(
		    master_cdo.get_offset() +
		    master_cdo.get_length()), index - 1,
		    master_cdo.get_chunksize());
		adj_end = MIN(adj_end, chunk_len);
		size_t adj_len = adj_end - adj_off;

		range_filter tstrm(ostrm, offset, adj_off, length);
		get_object_with_retry(cloud_name.container_cdo,
		    get_physical_cdo_name(master_cdo, index),
		    get_cdo_physical_offset(master_cdo, index,
		    compressed, with_checksum),
		    adj_off, adj_len, master_cdo.get_snapid(),
		    compressed, with_checksum, ectx,
		    master_cdo.get_chunksize(), master_cdo.get_readsize(),
		    false, attr_map, tstrm);
	} else {
		// hassle-free read
		get_object_with_retry(cloud_name.container_cdo,
		    get_physical_cdo_name(master_cdo, index),
		    get_cdo_physical_offset(master_cdo, index, compressed,
		    with_checksum),
		    offset, length, master_cdo.get_snapid(),
		    compressed, with_checksum, ectx,
		    master_cdo.get_chunksize(), master_cdo.get_readsize(),
		    false, attr_map, ostrm);
	}
}

void
isi_cbm_ioh_base::delete_cmo(const cl_object_name &cloud_name, uint64_t snapid)
{
	delete_object(cloud_name.container_cmo,
	    get_cmo_name(cloud_name.obj_base_name), snapid);
}

void
isi_cbm_ioh_base::delete_cdo(const cl_object_name &cloud_name, int index,
    uint64_t snapid)
{
	delete_object(cloud_name.container_cdo,
	    get_cdo_name(cloud_name.obj_base_name, index), snapid);
}

void
isi_cbm_ioh_base::get_cmo_attrs(const cl_object_name &cloud_name,
    str_map_t &attr_map)
{
	callback_params cb_params;
	std::string container_path("");
	std::string object_id = get_cmo_name(cloud_name.obj_base_name);
	build_container_path(cloud_name.container_cmo,
	    object_id,
	    container_path);

	return get_provider()->head_object(get_account(),
	    container_path,
	    object_id,
	    attr_map,
	    cb_params);
}

void
isi_cbm_ioh_base::get_cdo_attrs(const cl_object_name &cloud_name,
    int index, str_map_t &attr_map)
{
	callback_params cb_params;
	std::string container_path("");
	std::string object_id = get_cdo_name(cloud_name.obj_base_name, index);
	build_container_path(cloud_name.container_cdo,
	    object_id,
	    container_path);

	return get_provider()->head_object(get_account(),
	    container_path,
	    object_id,
	    attr_map,
	    cb_params);
}

size_t
isi_cbm_ioh_base::get_chunk_size(bool random_io)
{
	cl_account_cap acct_cap;
	callback_params cb_params;

	get_provider()->get_account_cap(get_account(), acct_cap, cb_params);
	const isi_cbm_account &acct = get_account();
	if (random_io) {
		// limit the chunk size to max size per upload
		if (acct.chunk_size > acct_cap.max_random_io_upload_size)
			return acct_cap.max_random_io_upload_size;
	}

	if (acct.chunk_size > acct_cap.max_upload_size)
		return acct_cap.max_upload_size;

	return acct.chunk_size > 0 ? acct.chunk_size :
	    acct_cap.default_chunk_size;   ///1MB
	//return 512;  //jjz
}

size_t
isi_cbm_ioh_base::get_read_size()
{
	if (acct_.read_size > 0)
		return acct_.read_size;
	//return 256;  //jjz
	return size_constants::SIZE_128KB;
}

uint32_t
isi_cbm_ioh_base::get_sparse_resolution()
{
	if (acct_.sparse_resolution > 0)
		return acct_.sparse_resolution;
	return ROUND_UP_SPA_RESOLUTION(size_constants::SIZE_8KB);
	//return ROUND_UP_SPA_RESOLUTION(1); ///jjz
}



/**
 * Get account usage by given account
 * acct_stats_param [in]: Parameters to parse the response from cloud.
 * acct_stats [out]: The struct that has the statistics information..
 */

void
isi_cbm_ioh_base::get_account_statistics(
	const isi_cloud::cl_account_statistics_param &acct_stats_param,
	isi_cloud::cl_account_statistics &acct_stats)
{
	get_provider()->get_account_statistics(get_account(),
	    acct_stats_param, acct_stats);
}


bool
isi_cbm_ioh_base::check_random_io_support(size_t &io_write_alignment)
{
	cl_account_cap acct_cap;
	callback_params cb_params;
	get_provider()->get_account_cap(get_account(), acct_cap,
	    cb_params);
	if (acct_cap.support_random_io)
		io_write_alignment = acct_cap.random_io_write_alignment;

	return acct_cap.support_random_io;
}

void
isi_cbm_ioh_base::get_master_cdo_sz(off_t &master_cdo_sz)
{
	cl_account_cap acct_cap;
	callback_params cb_params;
	get_provider()->get_account_cap(get_account(), acct_cap,
	    cb_params);

	if (acct_cap.support_random_io)
		master_cdo_sz = acct_cap.max_random_io_object_size;
	else
		master_cdo_sz = acct_cap.max_object_size;
}

void
isi_cbm_ioh_base::put_container(const std::string &container)
{
	callback_params cb_params;
	///调用ran_provider::put_container(...);
	get_provider()->put_container(get_account(), container,
	    cb_params);
}

// not yet supported
void
isi_cbm_ioh_base::delete_container(const std::string &container)
{
	cpool_callback_ctx cb_ctx = {acct_.account_key, CLOUD_DELETE};
	callback_params cb_params(&cb_ctx ,update_stats_operations);
	///调用ran_provider.cpp中的ran_provider::delete_object(...);
	get_provider()->delete_object(get_account(), container, "", 0,
	    cb_params);
}

void
isi_cbm_ioh_base::snapshot_object(const std::string &container,
    const std::string &object_id,
    uint64_t &snap_id)
{
	callback_params cb_params;
	get_provider()->snapshot_object(get_account(),
	    container, object_id, snap_id, cb_params);
}

void
isi_cbm_ioh_base::put_object(const std::string &container,
    const std::string &object_id,
    const str_map_t &attr_map,
    size_t length,
    bool overwrite,
    bool random_io,
    const std::string &md5,
    idata_stream *istrm)
{
	cpool_callback_ctx cb_ctx = {acct_.account_key, CLOUD_PUT};
	callback_params cb_params(&cb_ctx, update_stats_operations);  ////设置回调函数

	std::string container_path("");
	///////生成container: "d0007433042f00be23464b510af308c44dc04i4/0186/0135"
	/////利用container+ hash(object_id)
	int create_levels = build_container_path(
	    container, object_id, container_path);
	////调用ran_provider.cpp中的ran_provider::put_object(...);
	get_provider()->put_object(get_account(),
	    container_path, object_id,  attr_map, length,
	    overwrite, random_io, md5, cb_params, istrm,
	    create_levels);
}

void
isi_cbm_ioh_base::update_object(const std::string &container,
    const std::string &object_id,
    off_t offset,
    size_t length,
    isi_cloud::idata_stream *istrm)
{
}

void
isi_cbm_ioh_base::get_object_headers(const std::string &container,
    const std::string &object_id, off_t sub_object_offset,
    uint64_t snapid, isi_tri_bool compressed,
    isi_tri_bool with_checksum, size_t chunksize,
    size_t readsize, bool is_cmo,
    checksum_header &cs, sparse_area_header_t &spa)
{
	int		num_subchunk;
	bool		cache_hit = false;
	bool		cs_cache_hit = false;
	str_map_t	attr_map;

	if (compressed || is_cmo) {
		// single subchunk for CMO
		num_subchunk = 1;
	} else {
		num_subchunk = (chunksize + readsize - 1)/ readsize;
	}

	// azure has same object_id for the CDOs, offset used
	// here to reduce the collision
	cache_hit = cached_checksum.get(container, object_id,
	    sub_object_offset, cs, spa);
	if (cache_hit) {
		cs_cache_hit = (with_checksum && (cs.size() != 0));
	}

	// in case of compression, we always refresh the checksum
	// CMO always has the compression flag
	if (!cache_hit || !cs_cache_hit) {

		header_ostream h_ostrm(is_cmo, compressed, with_checksum,
		    compressed ? 0 : get_alignment(), num_subchunk);

		// failpoint doesn't allow goto / throw
		int ufp_ret = 0;
		UFAIL_POINT_CODE(cbm_ioh_get_obj_head_fail_read, {
			/*
			 * fail the request at the frequency specified
			 * by the return value of the failpoint
			 */
			static int getobjhead_fail = 0;
			getobjhead_fail++;
			if (((int)RETURN_VALUE <= 1) ||
			    (getobjhead_fail % (int)RETURN_VALUE) == 0)
				ufp_ret = RETURN_VALUE;
		});
		if (ufp_ret) {
			throw cl_exception(CL_READ_ERROR,
			    "Failpoint generated Error, frequency %d",
			    ufp_ret);
		}

		// get the header
		get_object_from_adapter(container, object_id,
		    sub_object_offset,
		    h_ostrm.get_header_size(), snapid,
		    attr_map, h_ostrm);

		// cache the checksum and spa
		spa = h_ostrm.get_spa_header();

		cs = h_ostrm.get_checksum();

		bool do_cache_put = true;
		UFAIL_POINT_CODE(cbm_ioh_get_obj_head_nocacheput, {
			ilog(IL_DEBUG,
			    "Failpoint skipping put of header info "
			    "to cache for obj_id %s, sub_obj_offset %lld",
			    object_id.c_str(), sub_object_offset);
			do_cache_put = false;
		});
		if (do_cache_put) {
			cached_checksum.put(container,
			    object_id, sub_object_offset, cs, spa);
		}
	}
}


void
isi_cbm_ioh_base::get_object(const std::string &container,
    const std::string &object_id, off_t sub_object_offset,
    off_t offset, size_t length, uint64_t snapid,
    isi_tri_bool compressed, isi_tri_bool with_checksum, encrypt_ctx_p ectx,
    size_t chunksize, size_t readsize, bool is_cmo,
    str_map_t &attr_map, odata_stream &ostrm)
{
	size_t r_offset = 0;
	size_t r_len = 0;

	odata_stream *pstrm = &ostrm;

	bool do_cache_put = true;
	UFAIL_POINT_CODE(cbm_ioh_get_object_nocacheput, {
		ilog(IL_DEBUG,
		    "Failpoint skipping put of header info "
		    "to cache for obj_id %s, sub_obj_offset %lld",
		    object_id.c_str(), sub_object_offset);
		do_cache_put = false;
	});

	// single subchunk for CMO
	int num_subchunk;
	if (compressed || is_cmo) {
		num_subchunk = 1;
	} else {
		num_subchunk = (chunksize + readsize - 1)/ readsize;
	}
	// CMO size unknown, set it to a large number
	size_t sz_subchunk = readsize;

	if (is_cmo)
		sz_subchunk = std::numeric_limits<size_t>::max();
	else if (compressed)
		sz_subchunk = chunksize;

	header_ostream h_ostrm(is_cmo, compressed, with_checksum,
	    compressed ? 0 : get_alignment(), num_subchunk);

	checksum_header cs;
	sparse_area_header_t spa;
	bool cs_cache_hit = false;

	// azure has same object_id for the CDOs, offset used
	// here to reduce the collision
	bool cache_hit = cached_checksum.get(container, object_id,
	    sub_object_offset, cs, spa);
	if (cache_hit) {
		cs_cache_hit = (with_checksum && (cs.size() != 0));
	}

	// in case of compression, we always refresh the checksum
	// CMO always has the compression flag
	if (is_cmo || compressed || (!cache_hit) || (!cs_cache_hit)) {

		// failpoint doesn't allow goto / throw
		int ufp_ret = 0;
		UFAIL_POINT_CODE(cbm_ioh_get_object_fail_read, {
			/*
			 * fail the request at the frequency specified
			 * by the return value of the failpoint
			 */
			static int getobj_fail = 0;
			getobj_fail++;
			if (((int)RETURN_VALUE <= 1) ||
			    (getobj_fail % (int)RETURN_VALUE) == 0)
				ufp_ret = RETURN_VALUE;
		});
		if (ufp_ret) {
			throw cl_exception(CL_REMOTE_ACCESS_DENIED,
			    "Failpoint generated Error, frequency %d",
			    ufp_ret);
		}
		// get the header
		printf("\n%s called get header get_object_from_adapter\n", __func__);
		get_object_from_adapter(container, object_id,
		    sub_object_offset,
		    h_ostrm.get_header_size(), snapid,
		    attr_map, h_ostrm);

		// cache the checksum and spa
		spa = h_ostrm.get_spa_header();

		cs = h_ostrm.get_checksum();

		if (do_cache_put) {
			cached_checksum.put(container,
			    object_id, sub_object_offset, cs, spa);
		}

		// check if header if really compressed
		if (is_cmo || compressed)
			compressed = h_ostrm.is_compressed();
		if (is_cmo || with_checksum)
			with_checksum = h_ostrm.has_checksum();
	}

	if (is_cmo || with_checksum) {
		// for non-compressed cmo, its size is unknown,
		// we read back the whole object, and we need to remove
		// the checksum header in the filter
		size_t headsz = 0;
		if ((length == 0) && !compressed) {
			headsz = h_ostrm.get_aligned_size();
		}
		checksum_f.reset(new checksum_filter(ostrm, cs, offset,
		    sz_subchunk, headsz, with_checksum));
		pstrm = checksum_f.get();
	} else {
		// The header always contains the sparse map.
		size_t headsz = 0;
		if ((length == 0) && !compressed) {
			headsz = h_ostrm.get_aligned_size();
		}
		spa_f.reset(new sparse_area_filter(ostrm, offset,
		    sz_subchunk, headsz));
		pstrm = spa_f.get();
	}
	// now get the data from the cloud, with one change retry on checksum
	// failure
	for (int i = 0; i < 2; ++i) {
		try {
			printf("%s called now get the data from the cloud\n", __func__);
			get_object_from_adapter(container, object_id,
			    r_offset/*0*/, r_len/*0*/, snapid, attr_map, *pstrm);

			// all data are retrieved, flush out the data kept
			// within the algorithms
			break;
		}
		catch (cl_exception &cl_excp) {
			// cleanup the data, as it is exception (checksum)
			ostrm.reset();

			// retry failed, or it is not checksum failure
			if (i == 1 ||
			    !(cs_cache_hit && !checksum_f->is_match()))
				throw;

			// reset header stream and retrieve the header again
			h_ostrm.reset();
			get_object_from_adapter(container, object_id,
			    sub_object_offset,
			    h_ostrm.get_header_size(), snapid,
			    attr_map, h_ostrm);

			// cache the checksum
			cs  = h_ostrm.get_checksum();
			spa = h_ostrm.get_spa_header();
			if (do_cache_put) {
				cached_checksum.put(container,
				    object_id, sub_object_offset, cs, spa);
			}

			if (encrypt_f.get())
				encrypt_f->reset();

			if (compress_f.get())
				compress_f->reset();

			if (checksum_f.get()) {
				checksum_f->reset();
				checksum_f->log_mismatch();
			}
		}
	}
}

static inline bool
is_retryable_error(enum cl_error_code error)
{
	switch (error) {
		case CL_GOT_NOTHING:
		case CL_PARTIAL_FILE:
		case CL_SVR_INTERNAL_ERROR:
			return true;
		default:
			return false;
	}
}

void
isi_cbm_ioh_base::get_object_headers_with_retry(const std::string &container,
    const std::string &object_id, off_t sub_object_offset,
    uint64_t snapid, isi_tri_bool compressed, isi_tri_bool checksum,
    size_t chunksize, size_t readsize, bool is_cmo,
    checksum_header &cs, sparse_area_header_t &spa)
{
#define RETRY_CNT  5
	for (int i = 0; i < RETRY_CNT; ++i) {
		try {
			bool ufp_exception = false;

			get_object_headers(container, object_id,
			    sub_object_offset, snapid, compressed, checksum,
			    chunksize, readsize, is_cmo, cs, spa);

			UFAIL_POINT_CODE(get_object_headers_fail, {
				ilog(IL_DEBUG,
				    "Failpoint generating "
				    "exception CL_GOT_NOTHING"
				    "for obj_id %s, sub_obj_offset %lld",
				    object_id.c_str(), sub_object_offset);
				ufp_exception = true;
				i = RETRY_CNT;
			});
			if (ufp_exception) {
				throw isi_cloud::cl_exception(
				    CL_GOT_NOTHING, "this is a fail point");
			}
			break;
		}
		catch (isi_cloud::cl_exception &cl_err) {
			// no more retry, throw the exception
			if (i >= RETRY_CNT - 1 ||
			    !is_retryable_error(cl_err.get_error_code()))
				throw;
		}
	}
}

void
isi_cbm_ioh_base::get_object_with_retry(const std::string &container,
    const std::string &object_id, off_t sub_object_offset,
    off_t offset, size_t length, uint64_t snapid,
    isi_tri_bool compressed, isi_tri_bool checksum, encrypt_ctx_p ectx,
    size_t chunksize, size_t readsize, bool is_cmo,
    isi_cloud::str_map_t &attr_map,
    isi_cloud::odata_stream &ostrm)
{
#define RETRY_CNT  5
	for (int i = 0; i < RETRY_CNT; ++i) {
		try {
			bool ufp_exception = false;
			printf("%s called container:%s cmo_name:%s\n", __func__, container.c_str(), object_id.c_str());
			get_object(container, object_id, sub_object_offset,
			    offset, length, snapid, compressed, checksum, ectx,
			    chunksize, readsize, is_cmo, attr_map, ostrm);

			UFAIL_POINT_CODE(get_object_fail, {
				if (i < RETURN_VALUE)
					ufp_exception = true;
			});
			if (ufp_exception) {
				throw isi_cloud::cl_exception(
				    CL_GOT_NOTHING, "this is a fail point");
			}
			break;
		}
		catch (isi_cloud::cl_exception &cl_err) {
			// no more retry, throw the exception
			if (i >= RETRY_CNT - 1 ||
			    !is_retryable_error(cl_err.get_error_code()))
				throw;

			if (!ostrm.reset())
				throw;

			ilog(IL_INFO, "Retry %d due to error code: %d, msg: %s",
			    i, cl_err.get_error_code(), cl_err.what());
		}
	}
}

void
isi_cbm_ioh_base::get_object_from_adapter(const std::string &container,
    const std::string &object_id, off_t offset/*0*/, size_t length, uint64_t snapid,
    str_map_t &attr_map, odata_stream &ostrm)
{
	cl_properties prop;
	cpool_callback_ctx cb_ctx = {acct_.account_key, CLOUD_GET};
	callback_params cb_params(&cb_ctx, update_stats_operations);
	std::string container_path("");

	// failpoint doesn't allow goto / throw
	int ufp_ret = 0;
	UFAIL_POINT_CODE(cbm_ioh_get_retryable_exception, {
		/*
		 * fail the request at the frequency specified by the return
		 * value of the failpoint
		 */
		static int intermittent_fail = 0;
		intermittent_fail++;
		if (((int)RETURN_VALUE <= 1) ||
		    (intermittent_fail % (int)RETURN_VALUE) == 0)
			ufp_ret = RETURN_VALUE;
	});
	if (ufp_ret) {
		throw cl_exception(CL_PARTIAL_FILE,
		    "Failpoint generated Error, frequency %d", ufp_ret);
	}

	// No md5 for HTTPS.
	// also expect RAN server to add support, skip it for now
	// AWS does not have MD5 in get response either.
	build_container_path(container, object_id, container_path);////ran_io_helper::build_container_path
	printf("%s called container:%s container_path:%s object_id:%s\n", __func__, 
			container.c_str(), container_path.c_str(), object_id.c_str());
	if (is_https_account(acct_) || acct_.type != CPT_AZURE ) {
		get_provider()->get_object(get_account(),
		    container_path, object_id, offset, length, 0,
		    attr_map, prop, cb_params, ostrm);

	} else {
		md5_filter ioh_ostrm(ostrm);

		get_provider()->get_object(get_account(),
		    container_path, object_id, offset, length, 0,
		    attr_map, prop, cb_params, ioh_ostrm);

		if (ioh_ostrm.get_md5() != prop.content_md5)
			throw cl_exception(CL_MD5_MISMATCH,
			    "Mismatched MD5");
	}
}

void
isi_cbm_ioh_base::match_cmo_attr(const str_map_t *cmo_attr_to_match,
    const cl_object_name &cloud_name)
{
	str_map_t cmo_attr;
	str_map_t::const_iterator itr;

	if (!cmo_attr_to_match || cmo_attr_to_match->size() == 0)
		return;

	get_cmo_attrs(cloud_name, cmo_attr);
	itr = cmo_attr_to_match->begin();

	for (; itr != cmo_attr_to_match->end(); ++itr) {
		if (cmo_attr[itr->first] == itr->second)
			continue;

		// mismatched value if reaching here
		std::string msg(itr->first);
		msg.append("'s value mismatched with CMO");
		throw isi_cbm_exception(
		    CBM_IOH_MISMATCHED_CMO_ATTR, msg);
	}
}

void
isi_cbm_ioh_base::delete_object(const std::string &container,
    const std::string &object_id, uint64_t snapid)
{
	cpool_callback_ctx cb_ctx = {acct_.account_key, CLOUD_DELETE};
	callback_params cb_params(&cb_ctx/*回调函数参数1*/, update_stats_operations/*回调函数*/); /*回调函数参数2:data*/

	std::string container_path("");
	build_container_path(container, object_id, container_path);

	get_provider()->delete_object(get_account(),
	    container_path, object_id, 0, cb_params);
//	printf("\n%s called container_path:%s\n", __func__, container_path.c_str());
}

void
isi_cbm_ioh_base::calc_master_cdo_plength(off_t master_cdo_size,
    size_t chunksize, size_t readsize,
    bool compress, bool checksum, off_t &len, off_t &plen)
{
        get_master_cdo_sz(len);
	plen = -1;
}

int
isi_cbm_ioh_base::build_container_path(
    const std::string &container,
    const std::string &object_id,
    std::string &container_path)
{
	container_path = container;
	return 0;
}

void
isi_cbm_ioh_base::get_new_master_cdo(off_t offset,
    isi_cpool_master_cdo &prev_mcdo,
    cl_object_name &cloud_name,
    bool compress,
    bool checksum,
    size_t chunksize,
    size_t readsize,
    uint32_t sparse_resolution,
    std::map<std::string, std::string> &attr_map,
    isi_cpool_master_cdo &master_cdo,
    off_t master_cdo_size)
{
	master_cdo.cloud_name = cloud_name; /////把cdo, cmo的container_path赋值,把object_id赋值
	master_cdo.chunksize = chunksize;
	master_cdo.attr_map = attr_map;
	calc_master_cdo_plength(master_cdo_size, chunksize, readsize,
	    compress, checksum, master_cdo.length,
	    master_cdo.physical_len);
	master_cdo.offset = offset - offset % master_cdo.length;
	master_cdo.readsize = readsize;
	master_cdo.index = 0;
	master_cdo.set_spa_version(CPOOL_SPA_VERSION);
	master_cdo.set_sparse_resolution(sparse_resolution, chunksize);
	master_cdo.set_sparse_map_length(0);
	master_cdo.clear_sparse_map();
 }

void
isi_cbm_ioh_base::start_write_master_cdo(isi_cpool_master_cdo &master_cdo)
{

}


void
isi_cbm_ioh_base::end_write_master_cdo(isi_cpool_master_cdo &master_cdo,
    bool commit)
{

}

bool
isi_cbm_ioh_base::has_cloud_snap()
{
	cl_account_cap acct_cap;
	callback_params cb_params;
	get_provider()->get_account_cap(get_account(), acct_cap,
	    cb_params);

	return acct_cap.support_cloud_snap;
}

bool
isi_cbm_ioh_base::has_cloud_clone()
{
	cl_account_cap acct_cap;
	callback_params cb_params;

	get_provider()->get_account_cap(get_account(), acct_cap, cb_params);

	return acct_cap.support_cloud_clone;
}

void
isi_cbm_ioh_base::move_cdo(const cl_object_name &src_cloud_name,
    const cl_object_name &tgt_cloud_name, int index, bool overwrite)
{
	callback_params cb_params;
	std::string tgt_container_path("");
	std::string src_container_path("");
	std::string tgt_object_id =
		get_cdo_name(tgt_cloud_name.obj_base_name, index);
	std::string src_object_id =
		get_cdo_name(src_cloud_name.obj_base_name, index);
	int new_container_levels;

	new_container_levels = build_container_path(
	    tgt_cloud_name.container_cdo,
	    tgt_object_id,
	    tgt_container_path);
	build_container_path(
	    src_cloud_name.container_cdo,
	    src_object_id,
	    src_container_path);

	get_provider()->move_object(get_account(),
		tgt_container_path,
		tgt_object_id,
		src_container_path,
		src_object_id,
		overwrite,
		cb_params,
		new_container_levels);
}

off_t
isi_cbm_ioh_base::get_cdo_physical_offset(
    const isi_cpool_master_cdo &master_cdo, int index,
    bool compress, bool checksum)
{
	return 0;
}

off_t
isi_cbm_ioh_base::get_alignment()
{
	// originally intended to handle Azure page blob case.
	// Now, we don't not need special alignment any more.
	return 0;
}


void
isi_cpool_master_cdo::assign(const cl_object_name &obj_name, uint64_t snap,
    size_t csize,
    off_t off, off_t len, uint64_t idx,
    const std::map<std::string, std::string> *attrs,
    bool master	)
{
	cloud_name = obj_name;
	chunksize = csize;
	offset = off;
	length = len;
	index = idx;
	snapid = snap;
	if (attrs)
		attr_map = *attrs;
	concrete_master = master;
}

/**
 * Check if the two master blob are the same
 */
bool
operator ==(const isi_cpool_master_cdo &m1, const isi_cpool_master_cdo &m2)
{
	bool retval = m1.cloud_name == m2.cloud_name &&
	    m1.chunksize == m2.chunksize &&
	    m1.readsize == m2.readsize &&
	    m1.offset == m2.offset &&
	    m1.length == m2.length &&
	    m1.index == m2.index &&
	    m1.snapid == m2.snapid &&
	    m1.spa.spa_version == m2.spa.spa_version &&  //TDB, have to match?
	    m1.spa.sparse_resolution == m2.spa.sparse_resolution &&
	    m1.spa.sparse_map_length == m2.spa.sparse_map_length &&
	    m1.concrete_master == m2.concrete_master;
	if (retval) {
		retval = memcmp(m1.spa.sparse_map,
		    m2.spa.sparse_map, m1.spa.sparse_map_length) == 0;
	}
	return retval;
}

void
isi_cpool_master_cdo::isi_cpool_copy_master_cdo (
    const isi_cpool_master_cdo &source, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	UFAIL_POINT_CODE(cbm_ioh_cpy_cdo_vers_mismatch, {
		/*
		 * fail the request at the frequency specified by the return
		 * value of the failpoint
		 */
		static int intermittent_cdo_cpy_fail = 0;
		intermittent_cdo_cpy_fail++;
		if (((int)RETURN_VALUE <= 1) ||
		    (intermittent_cdo_cpy_fail % (int)RETURN_VALUE) == 0)
			error = isi_cbm_error_new(CBM_VERSION_MISMATCH,
			    "Cannot copy master CDO, "
			    "Error generated by failpoint");
	});
	if (error) {
		goto out;
	}
	if (spa.get_spa_version() != source.get_spa_version()) {
		error = isi_cbm_error_new(CBM_VERSION_MISMATCH,
		    "Cannot copy master CDO, spa version mismatch");
	} else {
		cloud_name = source.cloud_name;
		chunksize = source.chunksize;
		offset = source.offset;
		length = source.length;
		physical_len = source.physical_len;
		index = source.index;
		attr_map = source.attr_map;
		concrete_master = source.concrete_master;
		snapid = source.snapid;
		readsize = source.readsize;
		spa.set_spa_version(source.get_spa_version());
		spa.set_sparse_resolution(
		    source.get_sparse_resolution(), chunksize);
		spa.set_sparse_map(source.get_sparse_map_ptr(),
		    source.get_sparse_map_length());
		spa.set_sparse_map_length(source.get_sparse_map_length());
	}
out:
	isi_error_handle(error, error_out);
	return;
}

/*
 * This get the actual CDO's name based on the master CDO.
 * @param index the index of the logical CDO
 */
std::string
isi_cbm_ioh_base::get_physical_cdo_name(const isi_cpool_master_cdo &mcdo,
    int idx) const
{
	return get_cdo_name(mcdo.cloud_name.obj_base_name, mcdo.concrete_master?
	    mcdo.index : idx);
}

void
isi_cbm_ioh_base::list_object(const std::string &container,
    cl_list_object_param &list_obj_param)
{
	cpool_callback_ctx cb_ctx = {acct_.account_key, CLOUD_GET};
	callback_params cb_params(&cb_ctx, update_stats_operations);

	get_provider()->list_object(get_account(),
	    container, list_obj_param, cb_params);
}

void
isi_cbm_ioh_base::get_object_id_from_cmo_name(const std::string &cmo_name,
    uint64_t snapid, isi_cloud_object_id &object_id)
{
	std::string prefix, idx_str, snap_str;
	uint64_t cl_snap = 0;


	size_t pos = cmo_name.find_first_of('_');

	if (pos == std::string::npos) {
		throw isi_cbm_exception(CBM_IOH_BAD_CMO_NAME, cmo_name);
	}

	prefix = cmo_name.substr(0, pos);

	size_t pos2 = cmo_name.find_first_of('_', pos + 1);

	if (pos2 == std::string::npos) {
		throw isi_cbm_exception(CBM_IOH_BAD_CMO_NAME, cmo_name);
	}

	idx_str = cmo_name.substr(pos + 1, pos2 - pos);

	snap_str = cmo_name.substr(pos2 + 1);

	if (!object_id.from_string_form(prefix.c_str())) {
		throw isi_cbm_exception(CBM_IOH_BAD_CMO_NAME, cmo_name);
	}

	std::istringstream strm(snap_str);
	strm >> cl_snap;
	if ((strm.rdstate() & strm.failbit ) != 0) {
		throw isi_cbm_exception(CBM_IOH_BAD_CMO_NAME, cmo_name);
	}
	object_id.set_snapid(cl_snap);
}

/** Construct CDO name for given base name and index */
std::string
isi_cbm_ioh_base::get_cdo_name(const isi_cloud_object_id &object_id, int index) const
{
	// in S3 model, we explicitly append the version info in the object name
	std::stringstream strm;
	strm << object_id.get_cdo_name(index);
	strm << "_";
	strm << object_id.get_snapid();
	return  strm.str();
}

/** Construct CMO name for given base name will always use
 *  CMO_INDEX */
std::string
isi_cbm_ioh_base::get_cmo_name(const isi_cloud_object_id &object_id) const
{
	// in S3 model, we explicitly append the version info in the object name
	std::stringstream strm;
	strm << object_id.get_cmo_name();
	strm << "_";
	strm << object_id.get_snapid();
	return strm.str();
}
