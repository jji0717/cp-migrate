#ifndef __ISI_CBM_FILE_H__
#define  __ISI_CBM_FILE_H__

#include <memory>

#include <sys/types.h>
#include <ifs/ifs_types.h>
#include <isi_cpool_cbm/isi_cbm_id.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_coi.h>
#include <isi_cpool_cbm/isi_cbm_wbi.h>
#include <isi_cpool_cbm/isi_cbm_csi.h>
#include <isi_cpool_cbm/isi_cbm_ooi.h>

class isi_cbm_ioh_base;
class isi_cfm_mapinfo;
class cpool_cache;
class isi_cfm_mapentry;
struct isi_cfm_policy_provider;

namespace isi_cloud {
class odata_stream;
}
using isi_cloud::wbi;
using isi_cloud::coi;
using isi_cloud::csi;
using isi_cloud::ooi;

/**
 * returns the size of the file described by isi_cbm_file object specified
 * @param file_obj{in] cbm file object
 * @param error_out[out] error if any
 */
off_t isi_cbm_file_get_filesize(struct isi_cbm_file *file_obj,
    struct isi_error **error_out);

ssize_t
isi_cbm_file_read_with_retry(isi_cbm_file *file_obj, void *buf,
    uint64_t offset, size_t length, enum isi_cbm_io_cache_option cache_opt,
    bool do_cache_read, bool ch_lock_held,
    struct isi_error **error_out);
/**
 * opaque file handle for cbm client
 */
struct isi_cbm_file {

public:
	isi_cbm_file() : data_cached(true),
	    fd(-1), lin(0),
	    snap_id(0),
	    mapinfo(NULL), cacheinfo(NULL),
	    cache_full_queued(false)
	{
		pthread_mutex_init(&cache_open_mtx, NULL);
	}

	~isi_cbm_file()
	{
		pthread_mutex_destroy(&cache_open_mtx);
		if (cacheinfo) {
			delete cacheinfo;
		}
	}

private:
	bool data_cached; // flag for data in cache

public:
	int fd;
	uint64_t lin;
	uint64_t snap_id;
	isi_cfm_mapinfo *mapinfo; // map info of the stub
	cpool_cache *cacheinfo; // information for cache state
	pthread_mutex_t cache_open_mtx; // protect race during cache open operation
	/**
	 * indicate if the read ahead full has been queued
	 * for thsi file object
	 */
	bool cache_full_queued;

	bool
	isi_cbm_file_is_cached(void)
	{
		return (data_cached);
	}

	void
	isi_cbm_set_file_NOTcached(void)
	{
		data_cached = false;
		return;
	}

	void
	isi_cbm_set_file_NOT_cached(void)
	{
		data_cached = false;
		return;
	}

};

/**
 * define the types of the various settle times
 */
enum isi_cbm_settle_time_type {
	STT_WRITEBACK, // settle time for write back
	STT_INVALIDATE, // settle time for invalidation after cache
	// settle time for invalidation after archive:
	STT_DELAYED_INVALIDATE,
};

struct fmt_conv_ctx isi_cbm_settle_time_type_fmt(enum isi_cbm_settle_time_type);

/**
 * reference counted shared pointer for WBI
 */
typedef std::shared_ptr<wbi> isi_cbm_wbi_sptr;

/**
 * reference counted shared pointer for CSI
 */
typedef std::shared_ptr<csi> isi_cbm_csi_sptr;

/**
 * reference counted shared pointer for COI
 */
typedef std::shared_ptr<coi> isi_cbm_coi_sptr;

/**
 * reference counted shared pointer for OOI
 */
typedef std::shared_ptr<ooi> isi_cbm_ooi_sptr;

/**
 * get the shared WBI pointer
 */
isi_cbm_wbi_sptr isi_cbm_get_wbi(struct isi_error **error_out);

/**
 * get the shared CSI pointer
 */
isi_cbm_csi_sptr isi_cbm_get_csi(struct isi_error **error_out);

/**
 * get the shared COI pointer
 */
isi_cbm_coi_sptr isi_cbm_get_coi(struct isi_error **error_out);

/**
 * get the shared OOI pointer
 */
isi_cbm_ooi_sptr isi_cbm_get_ooi(struct isi_error **error_out);

/**
 * Open or initialize the cache for the specified CBM file.
 * The cache header is NOT locked on entry, locked exclusive on
 * successful exit
 * @param file[in] the CBM file
 * @return true if the file was opened, false if already open
 * or error
 */
bool
isi_cbm_file_open_cache_lock_excl(struct isi_cbm_file *file,
    struct isi_error **error_out);

/**
 * Open or initialize the cache information. The read flag indicates the purpose
 * of the opening the cache info for. In the case that the file is already
 * fully cached and the purpose of opening the cache is to do read, the cache
 * will not be initialized if not being done already.
 *
 * @param file[in] the CBM file
 * @param keep_locked[in] hold the cache header locked on return.
 * The cache must NOT be locked on entry or we can deadlock with
 * the mutex due to ordering issues
 * @param read[in] indicate if the purpose of opening the cache
 * is for read
 * @param error_out[out] errors if any
 * @return true if the file was opened, false if already open
 * or error
 */
bool isi_cbm_file_open_and_or_init_cache(struct isi_cbm_file *file,
    bool keep_locked, bool read,
    struct isi_error **error_out);

/**
 * Remove the cache info for the file. The file must be no longer a
 * stub file.
 * @param file[in] the CBM file
 * @param lock indicate if to aquire the cache lock
 * @param error_out[out] errors if any
 */
void isi_cbm_file_remove_cache_info(struct isi_cbm_file *file,
    struct isi_error **error_out);

/**
 * dump the mapping info
 * @param fd[in] the file descriptor of the stub
 * @param dump_map[in] dump all the map info
 * @param dump_paths[in] dump the cmo and cdo paths
 * @param error_out[out] errors if any
 */
void isi_cbm_dump_map(int fd, bool dump_map, bool dump_paths,
    struct isi_error **error_out);

/**
 * verify the mapping info
 * @param fd[in] the file descriptor of the stub
 * @param error_out[out] errors if any
 */
void isi_cbm_verify_map(int fd, struct isi_error **error_out);

/**
 * Retrieve the mapping information from an already opened file object.
 * @param[in]  file	the opened file object
 */
const isi_cfm_mapinfo *isi_cbm_file_get_stubmap(struct isi_cbm_file *file);

/**
 * Retrieve the mapping information for the file
 * @param fd[in] the file descriptor of the stub
 * @param mapinfo[out] the mapping information
 * @param error_out[out] errors if any
 */
void isi_cph_get_stubmap(int fd, isi_cfm_mapinfo &mapinfo,
    struct isi_error **);


/**
 * An variant of file truncate called when the cache header is
 * already locked.
 *
 * @param file_obj[IN]: pointer to cbm file object
 * @param size[IN]: the offset to truncate the file to
 * @param cacheinfo[IN]: the cacheinfo object
 * @param error_out[OUT]: error if any
 */
void
isi_cbm_file_ftruncate_restart(struct isi_cbm_file *file_obj, off_t offset,
    cpool_cache *cacheinfo, struct isi_error **error_out);

/**
 * Perform file truncate/extend on a file handling both cacheinfo and file,
 * allowing for the cacheheader lock to be taken out prior to this routine
 * being called.  This routine requires that if @param locked is true the
 * cacheheader being locked exclusively.
 *
 * @param file_obj[IN]:   cbm open file object
 * @param offset[IN]:     length to truncate the file to
 * @param error_out[OUT]: address to place isi_error if error occurs
 */
void
isi_cbm_file_ftruncate_base(struct isi_cbm_file *file_obj, off_t offset,
    struct isi_error **error_out);

/**
 * Private function to queue the write or invalidate for a file
 * Pre-condition: the cache header must have been acquired exclusively
 */
void isi_cbm_file_queue_daemon(struct isi_cbm_file *file, int type,
    isi_error **error_out);

/**
 * Get the writeback settings. This function anticipates
 * the error when the policy is not available. In that case, it uses
 * the default options. The function obtains the interval specified in the
 * policy then adds it to the passed in stime and the result is
 * returned in stime as output. If stime is unspecified, the current sys
 * time is obtained.
 * @param lin[in] the lin of the file
 * @param snapid[in] the snapid of the file
 * @param policyid[in] the stub policy
 * @param type[in] the settle time type
 * @param stime[in/out] the settle time for the writeback
 */
void isi_cbm_file_get_settle_time(ifs_lin_t lin, ifs_snapid_t snap_id,
    const isi_cbm_id &policy_id, isi_cbm_settle_time_type type,
    struct timeval &stime, struct isi_error **error_out);

/**
 * purge stub information
 * @param file_obj[in] the cbm file
 * @param force[in] true to swallow error ENOENT
 * @param wait[in] true block on lock
 * @param delete_cacheinfo
 * @param error_out[out] errors if any
*/
void isi_cbm_file_stub_purge(
    struct isi_cbm_file *file_obj,
    bool force,
    bool wait,
    bool delete_cacheinfo,
    struct isi_error **error_out);

/**
 * checks if the file is worm commited (no matter expired or not)
 * @param lin[in] file id
 * @param snap_id[in] snapshot id
 * @param error_out[out] error if any
 * Return - true if the file is worm commited, false if not committed or
 *          there is an error
 */
bool is_cbm_file_worm_committed(ifs_lin_t lin, ifs_snapid_t snap_id,
    struct isi_error **error_out);

/**
 * Get the Garbage collection date of death also known as
 * Cloud Data Retention time for the file from the archive
 * policy.
 * @param file [in] - The CBM file object
 * Return - The stored retention time OR the default retention time
 */
struct timeval isi_cbm_get_cloud_data_retention_time(struct isi_cbm_file *file,
    struct isi_error **error_out);

struct timeval isi_cbm_get_cloud_data_retention_time(ifs_lin_t lin,
    ifs_snapid_t snapid, const isi_cfm_mapinfo &mapinfo,
    struct isi_error **error_out);

/*
 * Test to make sure the region is cached, if not fill the cache
 * and return the data in a buffer for possible use by the caller.
 *
 * NOTE: The region must be share locked on entry, if necessary the
 * lock may be released and reobtained in exclusive mode, then we attempt
 * to downgraded back to shared on exit, but it will still be locked on exit.
 * region_locked will reflect the lock state.
 *
 * NOTE: If fillbuf is null, and we need a fill we will allocate the buffer.
 * The caller may then reuse the same buffer for subsequent calls, but it
 * is up to the user to free the buffer when finished.
 */
ssize_t
isi_cbm_cache_test_and_fill_region(
    struct isi_cbm_file		*file_obj,
    off_t			region,
    char			**fillbuf,
    size_t			&fillbufsize,
    enum isi_cbm_io_cache_option cache_opt,
    bool			exclusive_lock,
    bool			mark_dirty,
    bool			enforce_no_cache,
    struct isi_error		**error_out);

int
isi_cbm_file_cache_write_common(
    struct isi_cbm_file		*file_obj,
    uint64_t			in_offset,
    size_t			in_length,
    bool			&ch_locked,
    bool			&cache_extended,
    cpool_cache			**out_cacheinfo,
    struct isi_error		**error_out);

/**
 * Get the caching option from the policy. This function anticipates
 * the error when the policy is not available. In that case, it uses
 * the default options.
 * @param file_obj[in] the cloud pool policy
 * @param cache_opt[in/out] the cache option
 * @param cache_full[out] if to do full cache
 * @param cloud_io_allowed[out] if cloud IO is allowed.
 */
void
isi_cbm_file_get_caching_opt(struct isi_cbm_file *file,
    isi_cbm_io_cache_option	&cache_opt,
    bool			&cache_full,
    bool			cloud_io_allowed,
    struct isi_error		**error_out);

void
isi_cbm_file_queue_full_cache(
    struct isi_cbm_file		*file,
    struct isi_error		**error_out);


/**
 * Read data from the CBM file with cacheheader lock option
 *   The cacheheader lock must be held exclusive if the ch_lock_held is true.
 * @param file[IN]: the CBM file
 * @param buf[IN/OUT]: the buffer to receive the content of the file
 *                     Its size must be  >= length
 * @param offset[IN]: the offset of the file
 * @param length[IN]: the length of the read
 * @param cache_opt[IN]: the cache option see the enum documentation.
 * @param ch_lock_held[IN]: set to true iff the cacheheader lock has been
 *                          taken exclusively before calling, caller must 
 *			    release on done.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the number of bytes actually being read. 0 indicates end of file.
 */
ssize_t isi_cbm_file_read_extended(struct isi_cbm_file *file, void *buf,
    uint64_t offset, size_t length,
    enum isi_cbm_io_cache_option cache_opt, bool ch_lock_held,
    struct isi_error **error_out);

/**
 * Fill the cache region containing a given offset
 * @param file[IN]: the CBM file
 * @param offset[IN]: the offset in the file
 * @param fillbuf[IN]: buffer to use for fill operation
 * @param fillbuflen[IN]: size in bytes of fillbuf
 * @param ch_lock_held[IN]: true iff exclusive cacheheader lock currently held.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 */
void isi_cbm_cache_fill_offset_region(struct isi_cbm_file *file_obj,
    off_t offset, char *fillbuf, size_t fillbuflen,
    struct isi_error **error_out);

/**
 * Queue CBM file writeback and/or invalidation, if needed
 *
 * @param file_obj[IN]: the CBM file
 * @param cached_regions[IN]: whether file has a cached region
 * @param dirty_region[IN]: whether file has a dirty region
 * @param error_out[OUT]: errors if any, see above for detailed information.
 *
 * If 'dirty_region' is set,  'file' will be queued for both writeback
 * and invalidation.  If 'cached' is set,  'file' will be queued for
 * invalidation.
 * Used in the restore workflow.
 */
void isi_cbm_file_queue_daemons(struct isi_cbm_file *file,
    bool cached_region,
    bool dirty_region,
    struct isi_error **error_out);

#endif /*  __ISI_CBM_FILE_H__ */

