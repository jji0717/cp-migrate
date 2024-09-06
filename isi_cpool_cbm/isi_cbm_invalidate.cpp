#include <sys/stat.h>
#include <sys/extattr.h>

#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include <isi_cpool_d_api/cpool_api.h>
#include <isi_ilog/ilog.h>
#include <isi_cpool_cbm/isi_cbm_invalidate.h>
#include <isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h>
#include <isi_cpool_cbm/isi_cbm_error.h>
#include <isi_cpool_cbm/isi_cbm_data_stream.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_read.h>
#include <isi_cpool_cbm/isi_cbm_scoped_flock.h>
#include <isi_cpool_cbm/isi_cbm_util.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_file.h>

#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_util/base64enc.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_ufp/isi_ufp.h>

#ifdef OVERRIDE_ILOG_FOR_DEBUG
#if defined(ilog)
#undef ilog
#endif
#define ilog(a, args...) \
	{\
	struct fmt FMT_INIT_CLEAN(thisfmt);\
	fmt_print(&thisfmt, args);\
	fprintf(stderr, "%s\n", fmt_string(&thisfmt));	\
	}
#endif

void
isi_cbm_invalidate_cache_i(struct isi_cbm_file *file_obj, ifs_lin_t lin,
    bool *dirty_data, bool override, bool daemon_task,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	scoped_sync_lock sync_lk;

	enum isi_cbm_cache_status status;
	off_t start = 0;
	off_t end = -1;
	cpool_cache *cacheinfo = NULL;
	scoped_sync_lock sync_lock;
	bool process_cache_header = true;
	bool locked = false;

	ASSERT(file_obj != NULL);
	cacheinfo = file_obj->cacheinfo;
	ASSERT(cacheinfo != NULL);

	*dirty_data = false;

	/*
	 * Grab the sync lock to be sure there is no race on invalidate and
	 * sync itself.  Sync may need to fill regions to handle the writeback
	 * that it needs and we don't want to trash them as we go.  THis wil
	 * be a try lock attempt as we don't want to block.
	 */
	sync_lock.lock_ex(file_obj->fd, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/**
	 * cacheheader lock uses the file fd, so we can lock BEFORE
	 * opening the cache ADS
	 */
	cacheinfo->cacheheader_lock_extended(true,
	    ISI_CBM_CACHE_FLAG_INVALIDATE, &error);
	if (error) {
		isi_error_add_context(error,
		    "Cannot lock cacheheader for invalidation of %{}",
		    lin_fmt(lin));
		goto out;
	}
	locked = true;

	ilog(IL_DEBUG, "Attempting invalidate for %{}", lin_fmt(lin));

	cacheinfo->cache_open(file_obj->fd, true, true, &error);
	if (error) {
		if (isi_system_error_is_a(error, ENOENT)) {
			int status;
			u_int32_t cpool_flags;
			/**
			 * The header does not exist so there cannot be any
			 * dirty blocks.  We may be in this state because of
			 * a failure while stubbing the file.  We will now get
			 * the stub flags and test the state.
			 */
			status = extattr_get_fd(file_obj->fd,
			    ISI_CFM_MAP_ATTR_NAMESPACE,
			    ISI_CFM_USER_ATTR_FLAGS,
			    &cpool_flags,
			    sizeof(u_int32_t));
			if (status != -1) {
			    if (!(cpool_flags & IFS_CPOOL_FLAGS_STUB_MARKED) ||
				((cpool_flags & (IFS_CPOOL_FLAGS_TRUNCATED) &&
				 (cpool_flags & IFS_CPOOL_FLAGS_INVALIDATED)))
				) {
					/**
					 * The file is not stubbed, or
					 * it is stubbed and both truncate
					 * and invalidate are set.  The ENOENT
					 * is a real error
					 */
				         ilog(IL_ERR, "Missing cache header "
					      "discovered during invalidate of"
					      " %{}, the file is not stubbed, "
					      " or both truncate and invalidate"
					      "are set."
					      "continuing invalidate, "
					      "error: %#{}",
					 lin_fmt(lin), isi_error_fmt(error));

					isi_error_add_context(error,
					    "Cannot open cache for invalidation"
					    " of %{}", lin_fmt(lin));
				} else {
					/**
					 * The file is stubbed, but at least
					 * one of truncate or invalidate is
					 * clear, so the operation did not
					 * complete
					 */
					ilog(IL_DEBUG, "Missing cache header "
					     "discovered during invalidate of"
					     " %{}, the file stubbed, "
					     " but either truncate or invalidate"
					     "are not set."
					     "continuing invalidate, "
					     "error: %#{}",
					lin_fmt(lin), isi_error_fmt(error));
					isi_error_free(error);
					error = NULL;
					ilog(IL_DEBUG, "Found cpool_flags for"
					    " %{}, invalidating cache",
					    lin_fmt(lin));
					isi_cbm_invalidate_cache_restart(
					    file_obj, lin, cacheinfo,
					    false, &error);
				}
			} else {
				/**
				 * We failed to get the attributes.
				 * An ENOATTR error indicates that the stubbing
				 * happened but the attributes were not yet set
				 * so treat it as an incomplete operation and
				 * restart the invalidate, otherwise the ENOENT
				 * is a real error.
				 */
				if (errno != ENOATTR) {
					ilog(IL_ERR, "Missing cache header "
					    "Failed to get attributes of:"
					    " %{}, but the error we get is"
					    " not ENOATTR but %d %s"
					    "error: %#{}",
					    lin_fmt(lin),
					    errno, strerror(errno),
					    isi_error_fmt(error)); 

					isi_error_add_context(error,
					    "Cannot open cache for invalidation"
					    " of %{}", lin_fmt(lin));
				} else {
					ilog(IL_DEBUG, "Missing cache header "
					    "discovered during invalidate of"
					    " %{}, an ENOATTR occured, "
					    " indicating that the stubbing occured"
					    ", but the attributes were not yet set"
					    " continuing invalidate, "
					    "error: %#{}",
					    lin_fmt(lin), isi_error_fmt(error)); 

					isi_error_free(error);
					error = NULL;
					ilog(IL_DEBUG, "No cpool_flags for"
					    " %{}, invalidating cache",
					    lin_fmt(lin));
					isi_cbm_invalidate_cache_restart(
						file_obj, lin, cacheinfo, false,
						&error);
				}
			}
		} else {
			isi_error_add_context(error,
			    "Cannot open cache for invalidation of %{}",
			    lin_fmt(lin));
		}
		goto out;
	}

	if (cacheinfo->is_fully_cached() && !cacheinfo->is_cache_open()) {
		// fully cached and no cache info
		process_cache_header = false;
		goto clean;
	}

	cacheinfo->refresh_header(&error);
	if (error) {
		isi_error_add_context(error,
		    "Error reading cache header for invalidation of %{}",
		    lin_fmt(lin));
	}

	if (!cacheinfo->is_valid_lin(lin)) {
		error = isi_cbm_error_new(CBM_CACHEINFO_ERROR,
		    "LIN found %{} does not match lin in cache header %{}",
		    lin_fmt(lin), lin_fmt(cacheinfo->get_lin()));
		goto out;
	}

	cacheinfo->get_status_range(&start, &end, ISI_CPOOL_CACHE_MASK_DIRTY,
	    &status, &error);
	if (!error) {
		*dirty_data = true;
		if (!override) {
			ilog(IL_DEBUG, "Cannot invalidate file %{} "
			    "with dirty data", lin_fmt(lin));
			goto out;
		} else {
			ilog(IL_TRACE, "Dirty data found during invalidate of "
			    "%{}, override specifed, continuing invalidate",
			    lin_fmt(lin));
		}
	} else {
		if (isi_cbm_error_is_a(error, CBM_NO_REGION_ERROR)) {
			isi_error_free(error);
			error = NULL;
		} else {
			isi_error_add_context(error,
			    "Error looking for dirty regions in %{}",
			    lin_fmt(lin));
			goto out;
		}
	}

	UFAIL_POINT_CODE(cbm_invalidate_cache_fail_1, {
		error = isi_system_error_new(EINVAL,
		    "Generated by failpoint after lock: LIN %{}",
		    lin_fmt(lin));
		goto out;
	});

	if (daemon_task) {
		/*
		 * if we are performing this invalidation on behalf of a
		 * cpool_d task, then we should clear the cached flag to allow
		 * for a new entry to be added to the CSI.  If we fail the
		 * invalidate, the worst case is that we have an extra entry.
		 */
		cacheinfo->clear_state(ISI_CBM_CACHE_FLAG_CACHED, &error);
		if (error) {
			isi_error_add_context(error,
			    "Could not reset CACHED state flag for %{}",
			    lin_fmt(lin));
			goto out;
		}
	}
 clean:
	isi_cbm_invalidate_cache_restart(file_obj, lin, cacheinfo,
	    process_cache_header, &error);
	if  (error)
		goto out;

out:
        if (error)
		isi_error_handle(error, error_out);

	if (locked)
		cacheinfo->cacheheader_unlock(isi_error_suppress());
}

void
isi_cbm_invalidate_cache_restart(struct isi_cbm_file *file_obj, ifs_lin_t lin,
    cpool_cache *cacheinfo, bool process_cache_header,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int state = 0;

	ASSERT(file_obj == cacheinfo->get_file_obj());

	if (process_cache_header) {
		/*
		 * Verified that there is no dirty data in the cache so it is
		 * now okay to invalidate the file. Catch any state flags that
		 * get set during this operation so they can be cleared later.
		 */
		state = cacheinfo->cache_reset(&error);
		if (error) {
			isi_error_add_context(error,
			    "Could not reset cacheinfo during invalidation of "
			    "%{}", lin_fmt(lin));
			goto out;
		}
		ilog(IL_DEBUG, "cache_reset was successful for lin %{}",
		    lin_fmt(lin));

		UFAIL_POINT_CODE(cbm_invalidate_cache_fail_2, {
			    error = isi_system_error_new(EINVAL,
				"Generated by failpoint after cache_reset: LIN "
				"%{}",
				lin_fmt(lin));
			    goto out;
		    });
	}

	/*
	 * Perform the actual invalidate of the date in the cache file,
	 * returning -1 on failure.
	 */
	if (ifs_cpool_stub_ops(file_obj->fd, IFS_CPOOL_STUB_INVALIDATE_DATA,
		0, NULL, NULL, NULL) == -1) {
		if (errno == EEXIST)
			error = isi_cbm_error_new(CBM_STUB_INVALIDATE_ERROR,
			    "Could not invalidate blocks for stub file; lin: "
			    "%{}", lin_fmt(lin));
		else
			error = isi_system_error_new(errno,
			    "Unable to invalidate cache for %{}", lin_fmt(lin));
		goto out;
	}
	ilog(IL_DEBUG, "ifs_cpool_stub_ops was successful for lin %{}",
	    lin_fmt(lin));

	UFAIL_POINT_CODE(cbm_invalidate_cache_fail_3, {
		error = isi_system_error_new(EINVAL,
		    "Generated by failpoint after invalidate data: LIN %{}",
		    lin_fmt(lin));
		goto out;
	});

	if (process_cache_header) {
		/*
		 * If the invalidation succeeded, then clear the state in the
		 * cacheheader to show successful operation.
		 */
		cacheinfo->clear_state(state, &error);
		if (error) {
			isi_error_add_context(error,
			    "Unable to clear cache reset state for %{}",
			    lin_fmt(lin));
			goto out;
		}
	}
	ilog(IL_DEBUG, "Invalidate completed for %{}", lin_fmt(lin));

 out:
	if (error)
		isi_error_handle(error, error_out);

}

void
isi_cbm_invalidate_cache(ifs_lin_t lin, ifs_snapid_t snapid,
    bool *dirty_data, struct isi_error **error_out)
{
	struct isi_cbm_file *file_obj = NULL;
	struct isi_error *error = NULL;
	int flags = O_SYNC;

	if (snapid != HEAD_SNAPID) {
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
		    "invalid snap id (lin %{} snapid %{})",
		    lin_fmt(lin), snapid_fmt(snapid));
		goto out;
	}

	file_obj = isi_cbm_file_open(lin, snapid, flags, &error);
	if (error) {
		isi_error_add_context(error, "Error opening Lin %{}",
		    lin_fmt(lin));
		goto out;
	}
	isi_cbm_invalidate_cache_i(file_obj, lin, dirty_data, false, false,
	    &error);

 out:
        if (file_obj)
		isi_cbm_file_close(file_obj, isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);
}


/**
 * invalidate the cache for the specified file.  If the cache is found to be
 * dirty, no work will be done and an error will be returned.
 * NB:  At this point there is no checkpoint-restart work as the operation
 * either succeeds or fails as a single operation.  If it fails, it is a
 * single operation on the next attempt.
 */
void
isi_cbm_invalidate_cached_file_opt(ifs_lin_t lin, ifs_snapid_t snapid,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    void *opaque, bool override_settle, bool daemon, bool *pdirty_data,
    struct isi_error **error_out)
{

	bool dirty_data = false; // returns whether dirty data was in cache

	struct timeval stime = {0};
	struct timeval now = {0};
	struct stat stats;

	struct isi_cbm_file *file_obj = NULL;
	struct isi_error *error = NULL;
	int flags = O_SYNC;

	ilog(IL_DEBUG, "Invalidating file lin %{} snapid %{}",
	    lin_fmt(lin), snapid_fmt(snapid));
	/*
	 * We currently can't invalidate snapshots or write-restricted lins
	 */
	if (snapid != HEAD_SNAPID) {
		error = isi_cbm_error_new(CBM_PERM_ERROR,
		    "invalid snap id (lin %{} snapid %{})",
		    lin_fmt(lin), snapid_fmt(snapid));
		goto out;
	}
	if (linsnap_is_write_restricted(lin, snapid)) {
		error = isi_cbm_error_new(CBM_RESTRICTED_MODIFY,
		    "Restricted file (lin %{} snapid %{})",
		    lin_fmt(lin), snapid_fmt(snapid));
		goto out;
	}

	file_obj = isi_cbm_file_open(lin, snapid, flags, &error);
	if (error) {
		isi_error_add_context(error, "Error opening Lin %{}",
		    lin_fmt(lin));
		goto out;
	}

	if (!override_settle) {
		/*
		 * Grab stats so that we can get the atime for the file.
		 */
		if (fstat(file_obj->fd, &stats)) {
			error = isi_system_error_new(errno,
			    "Getting stat information to obtain time for %{}",
			    isi_cbm_file_fmt(file_obj));
			goto out;
		}

		stime.tv_sec = stats.st_atime;
		isi_cbm_file_get_settle_time(file_obj->lin, file_obj->snap_id,
		    file_obj->mapinfo->get_policy_id(),
		    STT_INVALIDATE, stime, &error);
		if (error) {
			isi_error_add_context(error,
			    "Failed to calculate settle time for CBM file %{}",
			    isi_cbm_file_fmt(file_obj));
			goto out;
		}
		gettimeofday(&now, NULL);

		if (now.tv_sec < stime.tv_sec) {
			isi_cbm_csi_sptr cs;
			cs = isi_cbm_get_csi(&error);
			if (error) {
				isi_error_add_context(error,
				    "Cannot get CSI entry for %{}",
				    isi_cbm_file_fmt(file_obj));
				goto out;
			}

			cs->add_cached_file(file_obj->lin, file_obj->snap_id,
			    stime, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to requeue CSI entry for %{}",
				    isi_cbm_file_fmt(file_obj));
				goto out;
			}

			static const char *TIME_FMT = "%Y-%m-%d %H:%M:%S GMT";
			time_t tr = stime.tv_sec;
			struct tm gmt;
			gmtime_r(&tr, &gmt);
			char timebuf[64];
			strftime(timebuf, sizeof(timebuf),TIME_FMT, &gmt);
			// the file has not settled yet, schedule for later...
			ilog(IL_TRACE,
			    "The file has not settled yet, "
			    "invalidate requeued for %s for %{}.",
			    timebuf, isi_cbm_file_fmt(file_obj));
			goto out;
		}
	}

	isi_cbm_invalidate_cache_i(file_obj, lin, &dirty_data, false, daemon,
	    &error);
	if (error)
		goto out;

	if (dirty_data) {
		error = isi_cbm_error_new(CBM_INVALIDATE_FILE_DIRTY,
		    "Cannot invalidate file with dirty cached data: %{}",
		    isi_cbm_file_fmt(file_obj));
		ilog(IL_TRACE, "%{}", isi_error_fmt(error));
		goto out;
	}

 out:
	if (pdirty_data)
		*pdirty_data = dirty_data;

	ilog(IL_DEBUG, "Finished Invalidating file lin %{} snapid %{}",
	    lin_fmt(lin), snapid_fmt(snapid));

	if (file_obj)
		isi_cbm_file_close(file_obj, isi_error_suppress(IL_NOTICE));

	if (error)
		isi_error_handle(error, error_out);
}

void
isi_cbm_invalidate_cached_file(ifs_lin_t lin, ifs_snapid_t snapid,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func,
    void *opaque, struct isi_error **error_out)
{

	isi_cbm_invalidate_cached_file_opt(lin, snapid,
	    get_ckpt_data_func, set_ckpt_data_func, opaque,
	    false, true, NULL, error_out);
}


