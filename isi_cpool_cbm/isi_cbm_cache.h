#ifndef __ISI_CBM_CACHE__H__
#define __ISI_CBM_CACHE__H__

#include "isi_cpool_cbm.h"

#define ISI_CBM_CACHE_BITS_PER_STATUS		8 // char
#define ISI_CBM_CACHE_BITS_PER_ENTRY		2
#define ISI_CBM_CACHE_ENTRIES_PER_STATUS	4
#define ISI_CBM_CACHE_ENTRYMASK			0x3

#define ISI_CBM_CACHE_DEFAULT_FLAGS		0
#define ISI_CBM_CACHE_DEFAULT_DATAOFFSET	0
#define ISI_CBM_CACHE_DEFAULT_SEQUENCE		0
#define ISI_CBM_CACHE_DEFAULT_DATAOFFSET	0
#define ISI_CBM_CACHE_DEFAULT_CACHEOFFSET	0
#define ISI_CBM_CACHE_DEFAULT_TRUNCATEUP_STATE  ISI_CPOOL_CACHE_DIRTY
#define ISI_CBM_CACHE_DEFAULT_BULKOP_SIZE       16384

#define ISI_CBM_CACHE_CACHEINFO_NAME		IFS_CPOOL_ATTR_CACHE_INFO_NAME
#define ISI_CBM_CACHE_MAGIC			0xacec1cac

#define ISI_CBM_CACHE_READSIZE	\
	((ISI_CBM_CACHE_BITS_PER_ENTRY + \
	    ISI_CBM_CACHE_BITS_PER_STATUS - 1) / \
	    ISI_CBM_CACHE_BITS_PER_STATUS)

#define ISI_CBM_CACHE_BYTES_PER_ENTRY		ISI_CBM_CACHE_READSIZE

#define ISI_CBM_CACHE_DEFAULT_REGIONSIZE	(128 * 1024) // default to 128K

#define ISI_CBM_CACHE_HEADER_LOCK_OFFSET	0
#define ISI_CBM_CACHE_LOCK_FIRSTRANGE		16 // reserve before this byte
#define ISI_CBM_CACHE_LOCK_OFFSET(r) \
	((r)/ISI_CBM_CACHE_ENTRIES_PER_STATUS + ISI_CBM_CACHE_LOCK_FIRSTRANGE)


/*
 * If any of the following values are change (ISI_CBM_CACHE_FLAG_*) or if any
 * are added, be sure to change the corresponding information in
 * cpool_cache_flags_val in the .cpp
 */
#define ISI_CBM_CACHE_FLAG_NONE			0x0000 // No flag to set
#define ISI_CBM_CACHE_FLAG_INVALIDATE		0x0001 // invalidate in progress
#define ISI_CBM_CACHE_FLAG_TRUNCATE		0x0002 // truncate in progress
#define ISI_CBM_CACHE_FLAG_CACHEDDATA		0x0004 // cached data possible
/*
 * The following set of flags are used to synchronize cache IO operations with
 * the  isi_cpool_d jobs that synchronize dirty data to the cloud and
 * invalidate cached data.  The flags allow for a single queue entry to be
 * made for each case (writeback and invalidate) and to ensure that we don't
 * miss a write or read that needs to add a queue entry.
 */
#define ISI_CBM_CACHE_FLAG_DIRTY		0x0008 // cache dirty data flag
#define ISI_CBM_CACHE_FLAG_SYNC			0x0010 // sync-in-progress
#define ISI_CBM_CACHE_FLAG_CACHED		0x0020 // cache data flag
#define ISI_CBM_CACHE_FLAG_INVALIDATE_IP	0x0040 // invalidate-in-progress

/*
 * Mapping of status values back to mask values for matching status to mask
 * input.  This is fairly obvious except that a status of INPROGRESS matches
 * both DIRTY and INPROGRESS mask values since INPROGRESS implies a dirty
 * region as well.
 */

const int isi_cbm_cache_mask_map[] = {
	ISI_CPOOL_CACHE_MASK_NOTCACHED,
	ISI_CPOOL_CACHE_MASK_CACHED,
	ISI_CPOOL_CACHE_MASK_DIRTY,
	ISI_CPOOL_CACHE_MASK_DIRTY|ISI_CPOOL_CACHE_MASK_INPROGRESS,
};

/*
 * Header for cacheinfo
 */
struct cpool_cache_header {
	int version;
	int flags;
	ifs_lin_t lin;
	size_t old_file_size; // unused - here for on-disk compatibility
	size_t region_size;
	off_t data_offset;
	off_t last_region;
	uint64_t sequence;
	uint64_t foot;
};

struct isi_cbm_file;
/*
 * class for cache status information.  This describes the information in the
 * cache on a per region basis as well as the general header information
 */
class cpool_cache {
private:
	int fd_;		/* file descriptor for cpool cache */
	int dir_fd_;		/* file descriptor for cpool ADS stream dir */
	struct isi_cbm_file *file_obj_;/* file object for main file */
	bool create_on_open_;	/* flag to be set during initialize. */
	bool read_only_;	/* flag to indicate if the cache is read only.
				   Such a cache can never grow, shrink, or be
				   modified in any way */
	bool open_complete_;
	cpool_lock_type ch_lock_type_;	/* If locked, this is the lock mode */
	struct cpool_cache_header cache_header_;
	bool fully_cached_;	/* indicate if the file is fully cached */

	/**
	 * common routine to read and write just the cacheinfo
	 * header. Typically the fd will be -1 which will cause the routine to
	 * use the value found in the cacheinfo object itself.  Obviously
	 * there is a catch-22 on the intial open of the cache.
	 */
	void read_header(int fd, struct isi_error **error_out);
	void write_header(struct isi_error **error_out);

	int  cache_trunc_ext_internal(int state,
	    size_t new_file_size, size_t old_file_size,
	    struct isi_error **error_out);

	void cacheheader_lock_norefresh(bool exclusive,
	    struct isi_error **error_out);

	void cacheheader_lock_common(bool exclusive, bool refresh,
	    int function, struct isi_error **error_out);

	/*
	 * private routine to do bulk updates to header, regions should not be
	 * available for use while this is going on (exclusive lock on header
	 * should be sufficient, or range lock on entire range).
	 */
	void cache_bulk_update(off_t start_region, off_t end_region,
	    enum isi_cbm_cache_status status, struct isi_error **error_out);

	/**
	 * private routine to initialize the cache
	 * pre-condition: the cache file has been opened,
	 * the cache header has been locked exclusively.
	 */
	void cache_initialize_internal(size_t region_size, size_t fsize,
	    ifs_lin_t lin, struct isi_error **error_out);

	/**
	 * Refresh the fully cache state.
	 * pre-condition: the cache header must be locked
	 * @param fd[in] The fd of the main file.
	 * @param error_out[out] The error if any.
	 */
	void refresh_fully_cached_state(int fd, struct isi_error **error_out);

	/**
	 * Given the file descriptor for a file, open the cacheinfo.  If
	 * the open does not correspond to a create operation, read the
	 * cacheinfo header. Returns true if cache was created and needs
	 * initialization.
	 * @param fd[IN]: the file descriptor for the file to do the
	 *          cache open
	 * @param already_locked[IN]: The cache header is already lock
	 *	on entry
	 * @param keep_locked[IN]: hold the cache header locked on
	 *	return, unless there is an error in which case it will
	 * be in the same state as it was on entry.
	 * @param create[IN]: attempt to create the cache so it can
	 *	then be initialized.  If another thread
	 *	beats us to the create, then just open
	 * @param error_out[OUT]: errors if any
	 */
	bool cache_open_common(int fd, bool already_locked, bool keep_locked,
	    bool create, struct isi_error **error_out);

	/**
	 * Given the file descriptor for a file, open the cacheinfo.
	 * Create the cacheinfo if it does not already exist. Returns
	 * true if cache was created and needs initialization.
	 * @param fd[IN]: the file descriptor for the file to do the
	 *          cache open
	 * @param already_locked[IN]: The cache header is already lock
	 *	on entry
	 * @param keep_locked[IN]: hold the cache header locked on
	 *	return, unless there is an error in which case it will
	 * be in the same state as it was on entry.
	 * @param create[IN]: attempt to create the cache so it can
	 *	then be initialized.  If another thread
	 *	beats us to the create, then just open
	 * @param error_out[OUT]: errors if any
	 */
	bool cache_open_create(int fd, bool already_locked, bool keep_locked,
	    struct isi_error **error_out);

	/*
	 * Double check to be sure that the invalidate from the initial
	 * archive did not leave the stub file in a bad state.
	 *
	 * @param error_out[OUT]: errors if any
	 */
	void check_invalidate(struct isi_error **error_out);

 public:
	 void refresh_header(struct isi_error **error_out);

	 struct isi_cbm_file * get_file_obj(void) {
		 return file_obj_;
	 }
	/**
	 * Initialize the cacheinfo for a given file represented by fd. This
	 * will leave the cacheinfo open upon completion.  Any previous
	 * information in the cacheinfo will be destroyed.
	 */
	void cache_initialize(int fd, size_t region_size, size_t file_size,
	    ifs_lin_t lin, struct isi_error **error_out);

	void cache_initialize_override(int fd, size_t region_size,
	    size_t file_size, ifs_lin_t lin,
	    struct isi_error **error_out);

	/**
	 * already_locked: The cache header is already locked on entry
	 *
	 * keep_locked: hold the cache header locked on return,
	 *     unless there is an error in which case it will be
	 *     in the same state as it was on entry.
	 */
	void cache_initialize_common(int fd, size_t region_size,
	    size_t file_size, ifs_lin_t lin,
	    bool already_locked, bool keep_locked,
	    struct isi_error **error_out);

	/**
	 * Destroy the cache info.
	 * @param fd[in] the file descriptor for the stub
	 * @param error_out[out] The error if any.
	 */
	void cache_destroy(int fd, struct isi_error **error_out);

	/**
	 * Given the file descriptor for a file, open the cacheinfo.  If
	 * the open does not correspond to a create operation, read the
	 * cacheinfo header.
	 * @param fd[IN]: the file descriptor for the file to do the
	 *          cach open
	 * @param already_locked[IN]: The cache header is already locked
	 *	on entry
	 * @param keep_locked[IN]: hold the cache header locked on
	 *	return, unless there is an error in which
	 *	case it will be in the same state as it was
	 *	on entry.
	 * @param error_out[OUT]: errors if any
	 */
	void cache_open(int fd, bool already_locked, bool keep_locked,
	    struct isi_error **error_out);

	/**
	 * Close the cacheinfo, typically only called during the destructor.
	 */
	void cache_close();

	/**
	 * Return the cache status for a given region
	 */
	void cache_info_read(off_t region, enum isi_cbm_cache_status *status,
	    struct isi_error **error_out);

	/**
	 * Update the cache status for a given region
	 */
	void cache_info_write(off_t region, enum isi_cbm_cache_status status,
	    struct isi_error **error_out);

	/**
	 * Return the start and end range values for consecutive regions that
	 * have the same range as returned in status.  Default behavior is to
	 * return the status for the range starting at start_region and ending
	 * at ending_region where ending_region is >= starting_region.
	 * Future implementation will allow for selection of regions based on
	 * status values specified in the mask parameter, where the search for
	 * matching regions will begin at start_region and the next continous
	 * region starting there or after which meets the criteria will be
	 * returned.
	 */
	void get_status_range(off_t *start_region, off_t *end_region,
	    int mask, enum isi_cbm_cache_status *status,
	    struct isi_error **error_out);

	/**
	 * Allow for shrinking and growing of the cacheinfo based on filesize
	 * changes.  Return any state set in cacheinfo header's flag field,
	 * which needs to be cleared at the end of the larger truncate
	 * operation.
	 */
	int cache_truncate(size_t filesize, struct isi_error **error_out);
	int cache_extend(size_t filesize, struct isi_error **error_out);
	int  cache_truncate_extend(size_t new_file_size, size_t old_file_size,
	    struct isi_error **error_out);


	/**
	 * return header for cacheinfo
	 */
	struct cpool_cache_header get_header() {return cache_header_;};


	/**
	 * return version info for cacheheader
	 */
	int get_version() {return cache_header_.version;};

	/**
	 * return region size for the cached file
	 */
	size_t get_regionsize() {return cache_header_.region_size;};

	/**
	 * Convience routine to dump the current cacheinfo. If dump_status is
	 * false, only the header is dumped.
	 */
	void dump(bool dump_status, struct isi_error **error_out);

	/**
	 * Check to see if the has been opened.
	 */
	bool is_cache_open() {
		return (fd_ != -1 &&
		    cache_header_.foot == ISI_CBM_CACHE_MAGIC &&
		    open_complete_ == true);
	}

	bool is_fully_cached() const {
		return fully_cached_;
	}

	/**
	 * validate that the lin specified matches the lin stored in the header
	 */
	bool is_valid_lin(ifs_lin_t l) { return (l == cache_header_.lin); }

	/**
	 * Return the lin as found in the cache header.
	 */
	ifs_lin_t get_lin() { return (cache_header_.lin); }

	/**
	 * Return the read only status
	 */
	bool is_read_only() {
		return read_only_;
	}

	off_t get_last_region() { return (cache_header_.last_region);}

	/**
	 * Return true if the region specified is a valid region in the cache
	 * according to the information in the cacheheader.
	 */
	bool is_valid_region(off_t r) {
		return (r >= 0 && r <= cache_header_.last_region);
	}

	/**
	 * Return true if the region specified is the last region in the cache
	 * according to the cacheheader.
	 */
	bool is_last_region(off_t r) {
		return (r == cache_header_.last_region);
	}

	/**
	 * Return the size of the file as recorded in the cacheinfo header.
	 */
	off_t get_filesize(struct isi_error **error);

	cpool_cache(struct isi_cbm_file *file_obj):
	    file_obj_(file_obj)
	{
		fd_ = dir_fd_ = -1;
		ch_lock_type_ = (cpool_lock_type)-1;
		create_on_open_ = false;
		open_complete_ = false;
		read_only_ = false;
		bzero(&cache_header_, sizeof(struct cpool_cache_header));
		fully_cached_ = false;
	}

	/**
	 * Convience function to convert a given offset and length to
	 * beginning and ending regions. The method returns true upon
	 * successfull conversion.   If offset set and/or length specify
	 * invalid regions of the cache, -1 will be returned for the
	 * corresponding beginning and/or ending region.
	 */
	bool convert_bytes_to_regions(off_t offset, size_t length,
	    off_t *start, off_t *end, struct isi_error **error_out);

	/**
	 * Convience function to convert given starting and ending regions to
	 * offset and length.  The method returns true upon the succesful
	 * conversion.  XXXegc NOTE: length is not guarenteed to be
	 * consistant with file length at this point as there is no protection
	 * on EOF at the moment. It is currently be returned as the end of the
	 * last region.
	 */
	bool convert_regions_to_bytes(off_t start, off_t end,
	    off_t *offset, size_t *length, struct isi_error **error_out);

	/**
	 * clear/set/test any state that may have been set/cleared
	 * during a cache operation.
	 */
	void set_state(int state, struct isi_error **error_out);
	void clear_state(int state, struct isi_error **error_out);
	bool test_state(int state, struct isi_error **error_out);

	/**
	 * Get the sync state information.
	 */
	void get_sync_state(bool &dirty, bool &sync,
	    struct isi_error **error_out);

	/**
	 * set the sync state information.
	 */
	void set_sync_state(bool dirty, bool sync,
	     struct isi_error **error_out);

	/**
	 * Get the invalidate state information.
	 */
	void get_invalidate_state(bool &cached, bool &invalidate,
	    struct isi_error **error_out);

	/**
	 * set the invalidate state information.
	 */
	void set_invalidate_state(bool cached, bool invalidate,
	     struct isi_error **error_out);

	/*
	 * Perform the clearing of the cache flags during a cache
	 * invalidation. This will set the cache to uncached state. Returns
	 * state set which needs to be cleared at the end of the invalidation
	 * using the clear_state() method.
	 */
	int cache_reset(struct isi_error **error_out);

	/**
	 * Cache region locking functions lock, unlock, downgrade
	 */
	void cache_lock_region(size_t region, bool exclusive,
	    struct isi_error **error_out);

	void cache_lock_range(
	    size_t start_region,
	    size_t end_region,
	    bool exclusive,
	    struct isi_error **error_out);

	void cache_unlock_region(size_t region,
	    struct isi_error **error_out);

	void cache_unlock_range(
	    size_t start_region,
	    size_t end_region,
	    struct isi_error **error_out);

	void cache_downgrade_region_to_shlock(size_t region,
	    struct isi_error **error_out);

	void cache_downgrade_range_to_shlock(
	    size_t start_region,
	    size_t end_region,
	    struct isi_error **error_out);

	/**
	 * Conditionally mark the cache state. Under lock, if the current state
	 * is the same as the expected current state, the state is set
	 * to the destination state.
	 * @param region[in] the cache region to mark the state for
	 * @param current[in] expected current state
	 * @param dest[in] the state to set to
	 * @param changed[out] indicate if the change has been made
	 * @param error_out[out] the error if any. If the value found is not
	 *        the expected value, it will not return the error.
	 * @return the current cache status if no error, otherwise invalid.
	 *
	 */
	isi_cbm_cache_status cond_mark_cache_state(size_t region,
	    isi_cbm_cache_status current, isi_cbm_cache_status dest,
	    bool &changed, struct isi_error **error_out);

	/*
	 * The cachehead lock is used to protect the global information in the
	 * cache, most importantly the number of regions.  It also needs to be
	 * utilized when the cache is being initialized to be sure that no
	 * information is being modified during the intial creation.   The
	 * lock is to held exclusively during the initialization of the cache
	 * and when any information in the header is being updated.  All other
	 * access should be shared.
	 */
	void cacheheader_lock(bool exclusive, struct isi_error **error_out);
	void cacheheader_unlock(struct isi_error **error_out);
	void cacheheader_lock_extended(bool exclusive,
	    int function, struct isi_error **error_out);
	/**
	 * Downgrade the cache header lock from exclusive to shared.
	 * The caller must have acquired the lock exclusively before calling
	 * this.
	 */
	void cacheheader_downgrade_to_shlock(struct isi_error **error_out);

	/**
	 * Upgrade the cache header lock from shared to exclusive.
	 * The caller must have acquired the lock shared before calling
	 * this.
	 * @param locked[out] indicate if the the cache header lock is held
	 * @param error_out[out] error if any.
	 */
	void cacheheader_upgrade_to_exlock(bool &locked,
	    struct isi_error **error_out);

	/**
	 * Needs to be set before calling cache_open() to take effect
	 * TBD: Should be removed when cache_open() can be called
	 * with an 'isi_cbm_io_cache_option' arg
	 */
	void set_read_only(bool value) {
		read_only_ = value;
	}

	bool cacheheader_lock_type_is_exclusive() {
		return(ch_lock_type_ == CPOOL_LK_X);
	}

	~cpool_cache() { cache_close(); };
};

#endif /* __ISI_CBM_CACHE__H__ */
