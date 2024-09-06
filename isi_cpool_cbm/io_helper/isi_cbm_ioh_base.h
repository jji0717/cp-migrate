#ifndef __ISI_CBM_IOH_BASE_H__
#define __ISI_CBM_IOH_BASE_H__

/**
 * @file
 *
 * This defines interface for Cloud IO helper for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include <map>
#include <memory>
#include <string>

#include <ifs/ifs_types.h>
#include <isi_cloud_api/cl_exception.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_cpool_cbm/isi_cbm_id.h>
#include <isi_cpool_d_common/isi_tri_bool.h>
#include <isi_util_cpp/scoped_lock.h>
#include "encrypt_ctx.h"
#include "checksum_cache.h"
#include "sparse_area_header.h"

#include <isi_cpool_d_common/isi_cloud_object_id.h>

using namespace isi_cloud;

/** data structure for cloud object name */
struct cl_object_name {
	std::string container_cmo; /**< the container for meta object */
	std::string container_cdo; /**< the container for data object */
	isi_cloud_object_id obj_base_name; /**< the base name for the object */ /////这个就是archive时,传给mapinfo的object_id
};


/** structure representing a byte range section [start, start + length) */
struct cl_data_range {
	off_t start;
	size_t length;
};

struct isi_cbm_account : public isi_cloud::cl_account {
	isi_cbm_id account_id;     /**< account id for this account */
	account_key_t account_key; /**< unique account key for cpool_stats */
	std::string container_cmo; /**< cloud metadata container */
	std::string container_cdo; /**< cloud regular data container */
	size_t chunk_size;         /**< if 0 use provider default value */
	size_t read_size;          /**< if 0 use provider default value */
	size_t master_cdo_size;    /**< if 0 use provider default value */
	uint64_t max_capacity;       /**< max capacity for this account */
	uint32_t sparse_resolution; /**< if 0 use provider default value */
};


inline bool operator ==(const cl_object_name &o1, const cl_object_name &o2)
{
	return o1.container_cmo == o2.container_cmo &&
	    o1.container_cdo == o2.container_cdo &&
	    o1.obj_base_name == o2.obj_base_name;
}

/**
 * Cleanup the checksum, dedicated to pass the unit test
 */
void checksum_cache_cleanup();

/**
 * Get a checksum cache entry. This function is for the use of
 * the unit tests
 */
bool checksum_cache_get(const std::string &container,
    const std::string &object_id, off_t off,
    checksum_value &value, sparse_area_header_t &spa);

/* utility functions */
/** Check if HTTPS is used for the account */
bool is_https_account(const struct isi_cbm_account &acct);

/**
 * Returns the entity name of the object.
 * @param input[in] container name.
 * @param input[in] object name;.
 * out The concatenated path.
 */
std::string
get_entityname(std::string container, std::string objname);

// a master blob can contain sub blobs modelling CDOs...
class isi_cpool_master_cdo {
public:

	friend bool operator ==(const isi_cpool_master_cdo &m1,
	    const isi_cpool_master_cdo &m2);

	/**
	 * instantiate the master_blob during initial archiving
	 */
	isi_cpool_master_cdo() : chunksize(0), offset(0),
	    length(0), physical_len(0), index(0), concrete_master(false),
	    snapid(0), readsize(0), spa() {}

	isi_cpool_master_cdo(uint32_t resolution) : chunksize(0), offset(0),
	    length(0), physical_len(0), index(0), concrete_master(false),
	    snapid(0), readsize(0), spa(resolution) {}

	/**
	 * copy method
	 */
	void isi_cpool_copy_master_cdo (
	    const isi_cpool_master_cdo &source, struct isi_error **error_out);

	/**
	 * assign values for the master cdo, used during
	 * write-back
	 */
	void assign(const cl_object_name &obj_name, uint64_t snap,
	    size_t csize,
	    off_t off, off_t len, uint64_t idx,
	    const std::map<std::string, std::string> *attr_map,
	    bool master	);

	/**
	 * Indicate if it is a concrete master CDO
	 */
	bool is_concrete() const {return concrete_master; }

	size_t get_chunksize() const { return chunksize; }

	off_t get_length() const { return length; }
	void set_length(off_t in_length) { length = in_length; }

	off_t get_physical_length() const { return physical_len; }
	void set_physical_length(off_t plen) {
		physical_len = plen;
	}

	/**
	 * get the cloud object name
	 */
	const cl_object_name &get_cloud_object_name() const {
		return cloud_name;
	}

	/**
	 * set the cloud object name
	 */
	void set_cloud_object_name(cl_object_name &obj_name) {
		cloud_name = obj_name;
	}

	off_t get_offset() const { return offset; }

	int get_index() const { return index; }
	void set_index(uint64_t idx) {
		index = idx;
	}

	uint64_t get_snapid() const { return snapid; }
	uint64_t &get_snapid() { return snapid; }

	void set_snapid(uint64_t ver) {
		cloud_name.obj_base_name.set_snapid(ver);
		snapid = ver;
	}

	friend class isi_cbm_ioh_base;

	void set_concrete_master(bool concrete) {
		concrete_master = concrete;
	}

	size_t get_readsize() const { return readsize; }
	void set_readsize(size_t read_sz) { readsize = read_sz; }

	const std::map<std::string, std::string>& get_attrmap() const {
		return attr_map;
	}

	uint16_t get_spa_version() const {
		return spa.get_spa_version();
	}
	uint16_t get_sparse_map_length() const {
		return spa.get_sparse_map_length();
	}
	uint32_t get_sparse_resolution() const {
		return spa.get_sparse_resolution();
	}
	const uint8_t *get_sparse_map_ptr() const {
		return spa.get_sparse_map_ptr();
	}
	uint8_t get_sparse_map_elem(int indx) {
		return spa.get_sparse_map_elem(indx);
	}
	sparse_area_header_t get_sparse_area_header() const {
		return spa;
	}
	sparse_area_header_t *get_sparse_area_header_ptr() {
		return &spa;
	}


	void set_spa_version(uint16_t vers) {
		spa.set_spa_version(vers);
	}
	void set_sparse_map_length(uint16_t len) {
		spa.set_sparse_map_length(len);
	}
	void set_sparse_resolution(uint32_t res, size_t chnk) {
		spa.set_sparse_resolution(res, chnk);
	}
	void set_sparse_map(const uint8_t *smap, int len) {
		spa.set_sparse_map(smap, len);
	}
	void set_sparse_map_elem(int indx, uint8_t val) {
		spa.set_sparse_map_elem(indx, val);
	}
	void clear_sparse_map() {
		spa.clear_sparse_map();
	}

	void trace_spa(
	    const char *msg, const char *fnct, const int lne) const {
		spa.trace_spa(msg, fnct, lne);
	}

private:

	/*
	 * Disallow assignment operator and copy constructor.
	 * We need to be able to return an error in the sparse map sizes
	 * are not compatible
	 */
	isi_cpool_master_cdo(const isi_cpool_master_cdo &other);
	isi_cpool_master_cdo &operator=(const isi_cpool_master_cdo &other);

	cl_object_name cloud_name;
	size_t chunksize; // CDO chunk size
	off_t offset; // logical offset
	off_t length; // logical length of the master blob
	off_t physical_len; // physical length of the master blob
	uint64_t index; // the index number of the master blob, starts with 1
	std::map<std::string, std::string> attr_map;
	bool concrete_master;
	uint64_t snapid;
	size_t readsize;
	sparse_area_header_t spa;
};

bool operator ==(const isi_cpool_master_cdo &m1,
    const isi_cpool_master_cdo &m2);

/*
 * CBM error codes
 */
enum isi_cbm_error_code {
	CBM_IOH_MIN			= 0,
	CBM_IOH_MISMATCHED_CMO_ATTR	= 0,
	CBM_IOH_BAD_CMO_NAME		= 1,
	CBM_IOH_MAX			= CBM_IOH_BAD_CMO_NAME
};

class isi_cbm_exception : public std::exception {

public:
	isi_cbm_exception(isi_cbm_error_code ec, const std::string &msg)
		: ec_(ec), msg_(msg) { }

	virtual ~isi_cbm_exception() throw();

	// See comment in eh_exception.cc.
	virtual const char* what() const throw() { return msg_.c_str(); }

	isi_cbm_error_code get_error_code() const { return ec_; }

private:
	isi_cbm_error_code ec_;
	std::string        msg_;
};

class isi_cbm_ioh_base {

public:

	/**
	 * Validates container name for cloud storages.
	 *
	 * The name must meet the following most common conditions to be valid.
	 *
	 *  - Starts and ends with a letter or number, and can contain only
	 *    letters, numbers, and the dash (-) character.
	 *  - All letters are lower case.
	 *  - Every dash (-) character must be immediately preceded and
	 *    followed by a letter or number; consecutive dashes are not
	 *    permitted in container names.
	 *  - Be from 3 through 63 characters long.
	 */
	static bool validate_container_name(const std::string &container);

public:
	/** Destructor */
	virtual ~isi_cbm_ioh_base();

	/**
	 * Generate a cloud object name for given file lin
	 *
	 * @param lin[in] the lin number for file
	 * @param cloud_name[out] the name for the cloud object
	 */
	virtual void generate_cloud_name(const ifs_lin_t lin,
	    cl_object_name &cloud_name);

	/** Upload meta data object to cloud
	 *
	 * It fails if overwrite is true and target exists.
	 *
	 * @param cloud_name[in] the name for the cloud object
	 * @param attr_map[in] the user defined attributes
	 * @param length[in] the length of the write
	 * @param istrm[in] object data stream to be pulled from
	 * @param overwrite[in] whether to overwrite if target object exists
	 * @param compress[in] whether to apply compression
	 * @param checksum[in] whether to apply checksum
	 */
	virtual void write_cmo(const cl_object_name &cloud_name,
	    const isi_cloud::str_map_t &attr_map,
	    size_t length, isi_cloud::idata_stream &istrm,
	    bool overwrite, bool compress, bool checksum);

	/** Upload regular file data chunk to cloud
	 *
	 * It fails if overwrite is true and target exists.
	 *
	 * @param cdo_info[in] the master cdo info
	 * @param index[in] the index for the file chunk, it starts at 1.
	 * @param length[in] the length of the write
	 * @param istrm[in] object data stream to be pulled from
	 * @param overwrite[in] whether to overwrite if target object exists
	 * @param random_io[in] whether object type in cloud needs
	 *                      to support random I/O write
	 * @param compress[in] whether to apply compression
	 * @param checksum[in] whether to apply checksum
	 * @param ectx[in] encryption parameters, NULL for no encryption
	 */
	virtual void write_cdo(isi_cpool_master_cdo &cdo_info, int index,
	    size_t length, isi_cloud::idata_stream &istrm,
	    bool overwrite, bool random_io,
	    bool compress, bool checksum, encrypt_ctx_p ectx);

	/**
	 * clone an object
	 * @param src_name[in] the source object name
	 * @param tgt_name[in] the target object name
	 * @param index[in] the index of the object to copy
	 */
	virtual void clone_object(const cl_object_name &src_name,
	    const cl_object_name &tgt_name, int index);

	/**
	 * Upload partial file data chunk to cloud
	 *
	 * This only works for object type support random I/O write and
	 * compression is not used.
	 *
	 * When istrm is NULL, the operation zeros out (clears) the content
	 * to the specified range.
	 *
	 * @param cdo_info[in] the master cdo info
	 * @param index[in] the index for the file chunk
	 * @param range[in] the data range within the chunk to be updated
	 * @param istrm[in] object data stream to be pulled from
	 * @param compress[in] if to compress the object
	 * @param checksum[in] if to put the checksum
	 * @param ectx[in] encryption parameters, NULL if encryption is off
	 */
	virtual void update_cdo(const isi_cpool_master_cdo &cdo_info,
	    int index, const cl_data_range &range,
	    isi_cloud::idata_stream *istrm,
	    bool compress, bool checksum, encrypt_ctx_p ectx);

	/**
	 * Download meta data object from cloud
	 *
	 * @param cloud_name[in] the name for the cloud object
	 * @param compressed[in] whether the object is compressed
	 * @param checksum[in] whether to do checksum verification
	 * @param attr_map[in] the user defined attributes
	 * @param attr_to_match[in] the user defined attributes
	 * @param ostrm[out] output stream to receive data
	 */
	virtual void get_cmo(const cl_object_name &cloud_name,
	    isi_tri_bool compressed, isi_tri_bool checksum,
	    isi_cloud::str_map_t &attr_map,
	    isi_cloud::odata_stream &ostrm);

	/**
	 * Download regular file data chunk headers from cloud
	 *
	 * @param master_cdo[IN] the master CDO
	 * @param index[in] the index for the file chunk
	 * @param compressed[in] whether the object is compressed
	 * @param checksum[in] whether to do checksum verification
	 * @param cs[out] checksum header
	 * @param spa[out] sparse area header
	 */
	void get_cdo_headers(
	    const isi_cpool_master_cdo &master_cdo, int index,
	    bool compressed, bool with_checksum,
	    checksum_header &cs, sparse_area_header_t &spa);

	/**
	 * Download regular file data chunk from cloud
	 *
	 * @param master_cdo[IN] the master CDO
	 * @param index[in] the index for the file chunk
	 * @param compressed[in] whether the object is compressed
	 * @param checksum[in] whether to do checksum verification
	 * @param ectx[in] decryption parameters, NULL on no encryption
	 * @param range[in] the data range within the chunk to be downloaded
	 * @param cmo_attr_to_match[in] the CMO attributes to be matched
	 * @param attr_map[out] the user defined attributes
	 * @param ostrm[out] output stream to receive data
	 *
	 * @note use NULL for range to download whole chunk
	 */
	virtual void get_cdo(const isi_cpool_master_cdo &master_cdo, int index,
	    bool compressed, bool checksum, encrypt_ctx_p ectx,
	    const cl_data_range *range,
	    const isi_cloud::str_map_t *cmo_attr_to_match,
	    isi_cloud::str_map_t &attr_map,
	    isi_cloud::odata_stream &ostrm);

	/**
	 * Delete meta data object from cloud
	 *
	 * @param cloud_name[in] the name for the cloud object
	 */
	virtual void delete_cmo(const cl_object_name &cloud_name,
	    uint64_t snapid);

	/**
	 * Delete regular file data chunk from cloud
	 *
	 * @param cloud_name[in] the name for the cloud object
	 * @param index[in] the index for the file chunk
	 */
	virtual void delete_cdo(const cl_object_name &cloud_name, int index,
	    uint64_t snapid);

	/**
	 * Get user defined attributes of meta data object from cloud
	 *
	 * @param cloud_name[in] the name for the cloud object
	 * @param attr_map[out] the user defined attributes
	 * @throws cl_exception in case of error
	 */
	virtual void get_cmo_attrs(const cl_object_name &cloud_name,
	    isi_cloud::str_map_t &attr_map);

	/**
	 * Get user defined attributes of data chunk object from cloud
	 *
	 * @param cloud_name[in] the name for the cloud object
	 * @param index[in] the index for the file chunk
	 * @param attr_map[out] the user defined attributes
	 * @throws cl_exception in case of error
	 */
	virtual void get_cdo_attrs(const cl_object_name &cloud_name, int index,
	    isi_cloud::str_map_t &attr_map);

	/**
	 * Return whether random I/O is supported
	 * @param io_write_alignment[out] the byte of alignment required for
	 *            random I/O write
	 */
	virtual bool check_random_io_support(size_t &io_write_alignment);

	/**
	 * Return size of master cdo
	 * @master_cdo_sz[out] size of master cdo
	 */
	virtual void get_master_cdo_sz(off_t &master_cdo_sz);
	/**
	 * Get the chunk size good for splitting and uploading file to cloud
	 * in this account. The size returned is always multiples of 128K.
	 *
	 * @param random_io[in] indicates if target object type accepts
	 *                      random I/O write
	 */
	virtual size_t get_chunk_size(bool random_io);

	/**
	 * Get the size good for reading cloud object in this account.
	 * The size returned is always multiples of 64K
	 */
	virtual size_t get_read_size();

	/**
	 * Get the size for each sparse areas tracked in the sparse map
	 * in this account
	 */
	virtual uint32_t get_sparse_resolution();


        /**
	 * Get account usage by given account
	 * acct_stats_param [in]: Parameters to parse the response from cloud.
	 * acct_stats [out]: The struct that has the statistics information..
	 */
	virtual void get_account_statistics(
		const isi_cloud::cl_account_statistics_param &acct_stats_param,
		isi_cloud::cl_account_statistics &acct_stats);


       /**
	 * Create a non-public container in the specified account.
	 * Error is reported if container already exists.
	 *
	 * @param container[in] the container name
	 * @throws cl_exception in case of error
	 *
	 */
	void put_container(const std::string &container);

	/**
	 * Delete a container from given account.
	 * @param container[in] the container name
	 * @throws cl_exception in case of error
	 */
	void delete_container(const std::string &container);

	/**
	 * Check if the provider support cloud snapshot per object
	 */
	virtual bool has_cloud_snap();

	/**
	 * Check if the provider support server side clone
	 */
	virtual bool has_cloud_clone();

	/**
	 * Given the offset and the blob info get the corresponding master cdo
	 * information.
	 * @param offset[IN] file offset
	 * @param old_mcdo[IN] the previous master CDO
	 * @param cloud_name[IN] the cloud name information
	 * @param compress[IN] if the compression is requested
	 * @param checksum[IN] if checksum is requested
	 * @param chunksize[IN] the chunk size of the sub CDOs
	 * @param readsize[IN] the readsize
	 * @param attr_map[IN] any attributes to set on the CDOs
	 * @param master_cdo[OUT] the master blob
	 * @param master_cdo_size[in] the master CDO hint, 0, use default
	 *        This shall be kept constant for all calls for a particular
	 *        stub.
	 */

	virtual void get_new_master_cdo(off_t offset,
	    isi_cpool_master_cdo &old_mcdo,
	    cl_object_name &cloud_name,
	    bool compress,
	    bool checksum,
	    size_t chunksize,
	    size_t readsize,
	    uint32_t sparse_resolution,
	    std::map<std::string, std::string> &attr_map,
	    isi_cpool_master_cdo &master_cdo,
	    off_t master_cdo_size);

	/**
	 * given the chunksize, the master_cdo_size logical limit, compression
	 * and checksum need, calculate the master cdo size to satisfy the
	 * requirements.
	 */
	virtual void calc_master_cdo_plength(off_t master_cdo_size,
	    size_t chunksize, size_t readsize,
	    bool compress, bool checksum, off_t &len, off_t &plen);

	/**
	 * This starts a session to create master blob.
	 * @param cdo_info[IN] the master cdo to start a mdification
	 * @throws cl_exception in case of error
	 */
	virtual void start_write_master_cdo(isi_cpool_master_cdo &cdo_info);
	/**
	 * Commit write the master blob.
	 * @param cdo_info[IN] the master cdo to end a mdification
	 * @param commit[IN] indicate if to commit the master CDO.
	 */
	virtual void end_write_master_cdo(isi_cpool_master_cdo &cdo_info,
	    bool commit);

	/**
	 * Create a snapshot of cloud object.
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param snap_id[out] the id of snapshot created
	 * @throws cl_exception in case of error
	 *
	 */
	virtual void snapshot_object(const std::string &container,
	    const std::string &object_id, uint64_t &snap_id);

	/**
	 * move the cloud object identified by (src_cloud_name, index) to
	 * (tgt_cloud_name, index)
	 * @param src_cloud_name[in] the source cloud object name
	 * @param tgt_cloud_name[in] the target cloud object name
	 * @param index[in] the cloud object to be renamed
	 * @param overwrite[in] if to overwrite the existing target
	 */
	virtual void move_cdo(const cl_object_name &src_cloud_name,
	    const cl_object_name &tgt_cloud_name, int index, bool overwrite);

	/**
	 * Listing object in the specified container.
	 */
	virtual void list_object(const std::string &container,
	    cl_list_object_param &list_obj_param);
public:

	/**
	 * Get cloud provider
	 * Derived class must return a valid provider
	 */
	virtual isi_cloud::cl_provider *get_provider() = 0;

	/** constructor */
	isi_cbm_ioh_base(const isi_cbm_account &acct);

	/** Get account object the helper serves */
	isi_cbm_account &get_account() { return acct_; }

         /**
	 * Create an object in the specified account and container.
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param attr_map[in] the user defined attribute in name value map
	 * @param length[in] the length of the write
	 * @param overwrite[in] whether to overwrite if target object exists
	 * @param random_io[in] whether object type in cloud needs
	 *                      to support random io
	 * @param istrm[in] object data stream to be pulled from
	 * @throws cl_exception in case of error
	 *
	 */
	virtual void put_object(const std::string &container,
	    const std::string &object_id,
	    const isi_cloud::str_map_t &attr_map,
	    size_t length,
	    bool overwrite,
	    bool random_io,
	    const std::string &md5,
	    isi_cloud::idata_stream *istrm);

	/**
	 * Update an object in the specified account and container.
	 * Supported only by provider with random I/O feature.
	 *
	 * When @a istrm is NULL, the operation zeros out (clears) the content
	 * starting from @a offset for @a length bytes.
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param offset[in] offset into the object to write data
	 * @param length[in] the length of the write.
	 * @param istrm[in] object data stream to be pulled from
	 * @throws cl_exception in case of error
	 *
	 * @note: offset and length must be aligned to provider's
	 * random_io_write_alignment property (see cl_account_cap)
	 */
	virtual void update_object(const std::string &container,
	    const std::string &object_id,
	    off_t offset,
	    size_t length,
	    isi_cloud::idata_stream *istrm);

	/**
	 * Get the physical offset of a CDO identified by its index (starting
	 * from one) in its containing master CDO. The offset is influenced
	 * by the chunksize, and if compress and checksum is present.
	 * @param master_cdo[in] the master CDO
	 * @param index[in] the index of the CDO: it is the index in the file
	 *        starting from 1.
	 * @param compress[in] if the file is compressed
	 * @param checksum[in] if there is checksum information embedded
	 */
	virtual off_t get_cdo_physical_offset(
	    const isi_cpool_master_cdo &master_cdo,
	    int index, bool compress, bool checksum);

	/**
	 * Build the full container path from the base container
	 * name and the object id
	 * Return value is a count of the number of levels added after
	 * the container name
	 * @param containder[in] - RAN directory name
	 * @param object_id[in]  - RAN object or file
	 * @param container_path[out] - The full container path
	 */
	virtual int build_container_path(
	    const std::string &container,
	    const std::string &object_id,
	    std::string &container_path);

	/**
	 * Get the alignment, RAN returns 0, Azure returns 512
	 */
	virtual off_t get_alignment();

	/** Construct CDO name for given base name and index */
	virtual std::string get_cdo_name(const isi_cloud_object_id &object_id,
	    int index) const;

	/** Construct CMO name for given base name will always use
	 *  CMO_INDEX */
	virtual std::string get_cmo_name(const isi_cloud_object_id &object_id)
	    const;

	/**
	 * get the physical cdo's name
	 * @param mcdo the master CDO
	 * @param idx the index of the logical CDO
	 */
	virtual std::string get_physical_cdo_name(
	    const isi_cpool_master_cdo &mcdo,
	    int idx) const;

	/**
	 * Delete an object from given account and container.
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param snapid[in] the snapshot id
	 * @throws cl_exception in case of error
	 */
	void delete_object(const std::string &container,
	    const std::string &object_id, uint64_t snapid);

	/**
	 * From the physical CMO name and cloud snapid construct
	 * isi_cloud_object_id
	 * @param cmo_name[in] the CMO name
	 * @param snapid[in] the cloud snapid
	 * @param object_id[out] the cloud snapid
	 */
	virtual void get_object_id_from_cmo_name(const std::string &cmo_name,
	    uint64_t snapid, isi_cloud_object_id &object_id);

	/**
	 * Get the cloud object data without checking the compression header
	 * get_object checks compression header and calls this function
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param offset[in] offset into the object to read data
	 * @param length[in] the length of the read.
	 * @param attr_map[out] the user defined attributes
	 * @param ostrm[out] output stream to receive data
	 * @throws cl_exception in case of error
	 */
	void get_object_from_adapter(const std::string &container,
	    const std::string &object_id,
	    off_t offset/*0*/,
	    size_t length/*0*/,
	    uint64_t snapid,
	    isi_cloud::str_map_t &attr_map,
	    isi_cloud::odata_stream &ostrm);

private:
	/**
	 * Get the cloud object spa and checksum headers
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param sub_object_offset[in] the physical offset into the object
	 *        which models a sub object
	 * @param snapid[in] the snapshot id
	 * @param compressed[in] whether the object is compressed
	 * @param checksum[in] whether to do checksum
	 * @param chunksize[in] chunk size
	 * @param readsize[in]  read size
	 * @param is_cmo[in]  is it an cmo object
	 * @param cs[out] checksum header
	 * @param spa[out] sparse area header
	 * @throws cl_exception in case of error
	 */
	void get_object_headers(const std::string &container,
	    const std::string &object_id, off_t sub_object_offset,
	    uint64_t snapid, isi_tri_bool compressed,
	    isi_tri_bool with_checksum, size_t chunksize,
	    size_t readsize, bool is_cmo,
	    checksum_header &cs, sparse_area_header_t &spa);

	/**
	 * Get the cloud object data, checks the compress header
	 * when compressed/encrypted flag is on
	 *
	 * @param container[in] the container name
	 * @param object_id[in] the object id
	 * @param sub_object_offset[in] the physical offset into the object
	 *        which models a sub object
	 * @param offset[in] logical offset into the object to read data
	 * @param length[in] the length of the read.
	 * @param snapid[in] the snapshot id
	 * @param compressed[in] whether the object is compressed
	 * @param checksum[in] whether to do checksum
	 * @param ectx[in] decryption parameters, NULL on no ecryption
	 * @param chunksize[in] chunk size
	 * @param readsize[in]  read size
	 * @param is_cmo[in]  is it an cmo object
	 * @param attr_map[out] the user defined attributes
	 * @param ostrm[out] output stream to receive data
	 * @throws cl_exception in case of error
	 */
	void get_object(const std::string &container,
	    const std::string &object_id,
	    off_t sub_object_offset,
	    off_t offset,
	    size_t length,
	    uint64_t snapid,
	    isi_tri_bool compressed,
	    isi_tri_bool checksum,
	    encrypt_ctx_p ectx,
	    size_t chunksize,
	    size_t readsize,
	    bool is_cmo,
	    isi_cloud::str_map_t &attr_map,
	    isi_cloud::odata_stream &ostrm);

	/**
	 * retry version of get_object_headers(...)
	 */
	void get_object_headers_with_retry(const std::string &container,
	    const std::string &object_id, off_t sub_object_offset,
	    uint64_t snapid, isi_tri_bool compressed, isi_tri_bool checksum,
	    size_t chunksize, size_t readsize, bool is_cmo,
	    checksum_header &cs, sparse_area_header_t &spa);

	/**
	 * retry version of get_object(...)
	 */
	void get_object_with_retry(const std::string &container,
	    const std::string &object_id,
	    off_t sub_object_offset,
	    off_t offset,
	    size_t length,
	    uint64_t snapid,
	    isi_tri_bool compressed,
	    isi_tri_bool checksum,
	    encrypt_ctx_p ectx,
	    size_t chunksize,
	    size_t readsize,
	    bool is_cmo,
	    isi_cloud::str_map_t &attr_map,
	    isi_cloud::odata_stream &ostrm);

	/**
	 * This is a version of put_object, but will create container if it
	 * does not exist and retry
	 *
	 * @see put_object()
	 */
	void write_object_with_retry(const std::string &container,
	    const std::string &object_id,
	    off_t sub_object_offset,
	    const isi_cloud::str_map_t &attr_map,
	    size_t length,
	    bool overwrite,
	    bool random_io,
	    bool compress,
	    bool checksum,
	    encrypt_ctx_p ectx,
	    size_t chunksize,
	    size_t readsize,
	    bool is_cmo,
	    isi_cloud::idata_stream &istrm,
	    uint32_t sparse_resolution,
	    int sparse_map_len,
	    const uint8_t *sparse_map);

 protected:
	/**
	 * Matches the expected attributes to the CMO object attributes
	 * throws exception if they don't match. This implementation is
	 * case sensitive. S3 should has its own implementation
	 */
	virtual void match_cmo_attr(const str_map_t *cmo_attr_to_match,
	    const cl_object_name &cloud_name);

	struct isi_cbm_account acct_;
};

// shared pointer
typedef std::shared_ptr<class isi_cbm_ioh_base> isi_cbm_ioh_base_sptr;

#endif //__ISI_CBM_IOH_BASE_H__
