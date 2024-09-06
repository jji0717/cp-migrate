#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/extattr.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <isi_ilog/ilog.h>
#include <isi_cpool_d_common/cpool_d_common.h>
#include <isi_ufp/isi_ufp.h>

#include "isi_cpool_cbm/isi_cbm_account_util.h"
#include "isi_cpool_cbm/isi_cbm_cache_iterator.h"
#include "isi_cpool_cbm/isi_cbm_coi.h"
#include "isi_cpool_cbm/isi_cbm_ooi.h"
#include "isi_cpool_cbm/isi_cbm_data_stream.h"
#include "isi_cbm_error.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/isi_cbm_policyinfo.h"
#include "isi_cpool_cbm/isi_cbm_read.h"
#include "isi_cpool_cbm/isi_cbm_snapshot.h"
#include "isi_cpool_cbm/isi_cbm_scoped_flock.h"
#include "isi_cpool_cbm/isi_cbm_util.h"
#include "isi_cpool_cbm/isi_cbm_write.h"
#include "isi_cpool_cbm/isi_cpool_cbm.h"
#include "isi_cpool_cbm/isi_cbm_scoped_ppi.h"
#include "isi_cpool_cbm/isi_cbm_recall_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_sparse.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"


using namespace isi_cloud;

void
recall_ckpt::deserialize(const void *buffer, size_t len,
    struct isi_error **error_out)
{
	isi_error *error = NULL;
	char *my_data = (char *)buffer;

	int ckpt_size =  get_ckpt_size();

	if (buffer == NULL || len == 0)
		goto out;

	if ((int)len < ckpt_size) {
		error = isi_cbm_error_new(CBM_RECALL_CKPT_INVALID,
		    "Invalid recall checkpoint data (len %zu, ckpt_size %d)",
		    len, ckpt_size);
		goto out;
	}

	unpack((void **)&my_data, &magic_, sizeof(magic_));
	if (CPOOL_CBM_RECALL_CKPT_MAGIC != get_magic()) {
		error = isi_cbm_error_new(CBM_RECALL_CKPT_MAGIC_MISMATCH,
		    "Recall checkpoint magic mismatch. expected magic %d "
		    "found magic 0x%x", CPOOL_CBM_RECALL_CKPT_MAGIC,
		    get_magic());
		goto out;
	}

	unpack((void **)&my_data, &version_, sizeof(version_));
	if (CPOOL_CBM_RECALL_CKPT_VERSION != get_version()) {
		error = isi_cbm_error_new(CBM_RECALL_CKPT_VERSION_MISMATCH,
		    "Recall checkpoint version mismatch. expected version %d "
		    "found version %d", CPOOL_CBM_RECALL_CKPT_VERSION,
		    get_version());
		goto out;
	}

	unpack((void **)&my_data, &lin_, sizeof(lin_));
	unpack((void **)&my_data, &snapid_, sizeof(snapid_));

 out:
	isi_error_handle(error, error_out);
	return;
}

void
recall_ckpt::serialize(void **serialized_buff, int *serialized_size)
{
	int ckpt_size =  get_ckpt_size();

	char *my_data = NULL;

	if (*serialized_buff != NULL) {
		free (*serialized_buff);
		*serialized_buff = NULL;
	}

	*serialized_buff = calloc(1, ckpt_size);
	ASSERT(*serialized_buff != NULL);

	// Construct the serialized buffer
	my_data = (char *)(*serialized_buff);
	pack((void **) &my_data, &magic_, sizeof(magic_));
	pack((void **) &my_data, &version_, sizeof(version_));
	pack((void **) &my_data, &lin_, sizeof(lin_));
	pack((void **) &my_data, &snapid_, sizeof(snapid_));

	*serialized_size = ckpt_size;
}

bool
recall_ckpt_mgr::load_ckpt(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool ok = false;
	void *data = NULL;
	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	size_t ckpt_sz = 0;
	if (!get_ckpt_cb_) {
		goto out;
	}

	ok = get_ckpt_cb_(task_ckpt_opaque_, &data, &ckpt_sz, &t_state, 0);

	if (data == NULL || ckpt_sz == 0) {
		ok = false;
	}

	if (!ok) {
		goto out;
	}

	recall_ckpt_.deserialize(data, ckpt_sz, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (data) {
		free(data);
		data = NULL;
	}
	isi_error_handle(error, error_out);
	return ok;
}

void
recall_ckpt_mgr::save_ckpt(uint64_t lin, uint64_t snapid,
    off_t offset, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *serialized_buff = NULL;
	int serialized_size;
	djob_op_job_task_state t_state = DJOB_OJT_NONE;
	if (!set_ckpt_cb_) {
		goto out;
	}

	recall_ckpt_.set_lin(lin);
	recall_ckpt_.set_snapid(snapid);
	recall_ckpt_.set_offset(offset);
	recall_ckpt_.serialize(&serialized_buff, &serialized_size);

	set_ckpt_cb_(task_ckpt_opaque_, serialized_buff, serialized_size,
	    &t_state, 0);

	if (t_state == DJOB_OJT_PAUSED) {
		error = isi_cbm_error_new(CBM_FILE_PAUSED,
		    "Recall Paused for file %{}",
		    lin_fmt(lin));
		goto out;
	}

	if (t_state == DJOB_OJT_STOPPED) {
		error = isi_cbm_error_new(CBM_FILE_STOPPED,
		    "Recall Stopped for file %{}",
		    lin_fmt(lin));
		goto out;
	}

out:
	if (serialized_buff) {
		free(serialized_buff);
		serialized_buff = NULL;
	}
	isi_error_handle(error, error_out);
}

static void
isi_cph_fetch_cmo(struct isi_cbm_file *file_obj,
    isi_cloud::odata_stream *pstrm, struct isi_error **error_out)
{
	const isi_cfm_mapinfo& mapinfo = *(file_obj->mapinfo);
	isi_cfm_mapentry map_entry;
	isi_error *error = NULL;
	ifs_lin_t lin = file_obj->lin;

	cl_object_name cloud_object_name;
	bool send_celog_event = true;

	isi_cbm_ioh_base_sptr io_helper =
	    isi_cpool_get_io_helper(mapinfo.get_account(), &error);
	if (error != NULL) {
		isi_error_add_context(error);
		goto out;
	}

	if (!io_helper) {
		ilog(IL_DEBUG, "failed to find io helper for cmo object =%s\n",
		    mapinfo.get_object_id().to_c_string());
		goto out;
	}
	// TODO: better have a get name routine from io helper
	// io_helper->get_cloud_name(object_id, cloud_object_name);
	io_helper->generate_cloud_name(0, cloud_object_name);
	cloud_object_name.obj_base_name = mapinfo.get_object_id();

	isi_cph_read_md_blob(*pstrm, lin, cloud_object_name,  //////ran_provider中使用回调函数将收到的字节流写回到本地.odata_stream->write(ptr,w_left);
	    mapinfo.is_compressed(), mapinfo.has_checksum(),
	    io_helper.get(), send_celog_event, &error);

	if (error) {
		isi_error_add_context(error);
		ilog(IL_DEBUG, "isi_cph_read_cmo_blob failed"
		    "cmo_container =%s object=%s\n",
		    cloud_object_name.container_cmo.c_str(),
		    mapinfo.get_object_id().to_c_string());
		goto out;
	}
 out:

	isi_error_handle(error, error_out);

}


static void
fetch_uncached_file_data(struct isi_cbm_file *file,
    void *opaque,
    get_ckpt_data_func_t get_ckpt_data_func,
    struct isi_error **error_out)
{
	isi_cfm_mapinfo 	&mapinfo = *file->mapinfo;
	void			*ckpt_data = NULL;
	size_t			ckpt_data_size = -1;
	djob_op_job_task_state	task_state;
	uint64_t		current_chunk = 0;
	char			*fillbuf = NULL;
	off_t			filesize = 0;
	isi_cbm_cache_record_info record;

	ASSERT(*error_out == NULL);

	filesize = isi_cbm_file_get_filesize(file, error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get file size "
		    "for %{}", isi_cbm_file_fmt(file));
		goto out;
	}

	fillbuf = (char *)malloc(file->cacheinfo->get_regionsize());
	if (!fillbuf) {
		*error_out = isi_cbm_error_new(CBM_CLAPI_OUT_OF_MEMORY,
		    "Could fetch uncached data for %{}",
		    isi_cbm_file_fmt(file));
		goto out;
	}

	{
		// We need a new block so that we can initialise cache_iter
		// with filesize.  Getting filesize may return an error so if
		// we handle the error by jumping to out we skip this
		// initialization and the compiler gets upset.
		cbm_cache_iterator	cache_iter(file, 0, filesize,
		     ISI_CPOOL_CACHE_MASK_NOTCACHED);
		while (cache_iter.next(&record, error_out)) {
			if (get_ckpt_data_func != NULL &&
			    (current_chunk <=
			     record.offset / mapinfo.get_chunksize())) {
				current_chunk = (record.offset /
					mapinfo.get_chunksize()) + 1;
				get_ckpt_data_func(opaque, &ckpt_data,
				    &ckpt_data_size, &task_state, 0);
				/*
				 * free ckpt_data right away since it is
				 * not used
				 */
				if (ckpt_data) {
					free((void *)ckpt_data);
					ckpt_data = NULL;
				}
				if (task_state == DJOB_OJT_PAUSED) {
					*error_out  =
					    isi_cbm_error_new(CBM_FILE_PAUSED,
					    "Recall Paused for file %{}",
					    lin_fmt(file->lin));
					goto out;

				}
				if (task_state == DJOB_OJT_CANCELLED) {
					*error_out =
					    isi_cbm_error_new(
						CBM_FILE_CANCELLED,
					        "Recall Canceled for file %{}",
					        lin_fmt(file->lin));
					goto out;

				}
				if (task_state == DJOB_OJT_STOPPED) {
					*error_out =
					    isi_cbm_error_new(CBM_FILE_STOPPED,
					        "Recall Stopped for file %{}",
					        lin_fmt(file->lin));
					goto out;
				}
			}

			isi_cbm_cache_fill_offset_region(file,
			    record.offset, fillbuf,
			    file->cacheinfo->get_regionsize(),
			    error_out);

			if (*error_out) {
				isi_error_add_context(*error_out,
				    "Failed to fill cache region "
				    "for %{}, offset: %ld",
				    isi_cbm_file_fmt(file), record.offset);
				goto out;
			}
		}
	}

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to cache_iter.next "
		    "for %{}", isi_cbm_file_fmt(file));
	}
out:
	if (fillbuf) {
		free(fillbuf);
	}
	return;
}

static void
gc_cloud_object(struct isi_cbm_file *file, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cloud::ooi ooi;
	isi_cbm_coi_sptr coi;
	struct timeval date_of_death_tv = {0};
	const isi_cfm_mapinfo *mapinfo = file->mapinfo;
	isi_cloud_object_id object_id = mapinfo->get_object_id();

	bool used = cbm_snap_used_by_other_snaps(file, mapinfo, &error);
	if (error != NULL) {
		isi_error_add_context(error,
		    "Failed to cbm_snap_used_by_other_snaps "
		    "for %{}", isi_cbm_file_fmt(file));
		goto out;
	}

	if (used)
		goto out;

	date_of_death_tv = isi_cbm_get_cloud_data_retention_time(file, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ilog(IL_DEBUG,
	    "File: %{}, Recall DOD: %lu ",
	    isi_cbm_file_fmt(file), date_of_death_tv.tv_sec);

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi->remove_ref(
	    object_id,
	    file->lin,
	    file->snap_id,
	    date_of_death_tv,
	    NULL,
	    &error);

	if (error != NULL) {
		if (isi_cbm_error_is_a(error, CBM_LIN_DOES_NOT_EXIST)) {
			ilog(IL_DEBUG, "%{} is not found in the COI for "
			    "object: %s. Maybe already removed by last recall."
			    "Error ignored from lower level: %#{}",
			    isi_cbm_file_fmt(file),
			    mapinfo->get_object_id().to_c_string(),
			    isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}
		else {
			isi_error_add_context(error,
			    "Failed to coi::remove_ref "
			    "for %{}",
			    isi_cbm_file_fmt(file));
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
}

/**
 *
 *
 * @author ssasson (4/15/2015)
 * Writing a checkpoint after each region retrieval from the
 * cloud.
 *
 * @param file_obj The file to retrieve from the cloud
 * @param offset offet byte to the file that marks the next byte
 * to retrieve.
 * @param get_ckpt_data_func Function for getting the checkpoint
 * @param set_ckpt_data_func Function to save the checkpoint
 * @param opaque object to be used with checkpointing
 * @param error_out
 */
static void
write_checkpoint(isi_cbm_file *file_obj,
    off_t offset,
    recall_ckpt_mgr *ckpt_mgr,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ckpt_mgr->save_ckpt(file_obj->lin, file_obj->snap_id, offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	 isi_error_handle(error, error_out);

}

static ssize_t
cache_pwritev(const char *func, int fd, const struct iovec *iov, int iovcnt,
    off_t off, bool no_time_update, int extra_flags)
{
	ssize_t len = 0;
	int i;
	for (i = 0; i < iovcnt; i++) {
		len += iov->iov_len;
	}
	ilog(IL_CP_EXT,
	    "%s: pwrite called with fd: %d, iovcnt: %d, len: %ld "
	    "offset: %ld, extra flags: %d",
	    func, fd, iovcnt, len, off, extra_flags);
	return ifs_cpool_pwritev(fd, iov, iovcnt, off,
	    extra_flags | (no_time_update ? (O_SYNC | O_IGNORETIME) : 0));
}

/**
 *
 *
 * @author ssasson (4/15/2015)
 *
 * Recalling a file without using the cache.
 * This is specifically for dealing with the case where the ADS
 * directory cannot be created.
 * The case for that is when the file is flooded with ACLs
 *
 * @param file_obj The file that needs to be recalled.
 * @param opaque Opaque object to be used with checkpointing
 * @param get_ckpt_data_func Function for getting the checkpoint
 * @param set_ckpt_data_func Function to save the checkpoint
 * @param error_out
 */
void isi_cbm_recall_without_cache(isi_cbm_file *file_obj,
    recall_ckpt_mgr *ckpt_mgr, struct isi_error **error_out)
{
	off_t offset = 0;
	isi_error *error = NULL;
	size_t	regionsize	= 0;
	ssize_t	this_write	= 0;
	char *databuf = NULL;
	off_t filesize = 0;
	ssize_t	total_read	= 0;
	size_t	read_size	= 0;
	off_t bytes_left_to_read_from_cloud = 0;
	isi_cfm_mapinfo &mapinfo = *file_obj->mapinfo;
	bool loaded_ckpt = false;
	spa_header spa;
	struct iovec *vec = NULL;
	int vec_count = -1;
	off_t	this_offset;
	int i;

	regionsize = mapinfo.get_readsize();
	loaded_ckpt = ckpt_mgr->load_ckpt(&error);
	ON_ISI_ERROR_GOTO(out, error, "Error occurred in loading ckptdata for %{}",
	    isi_cbm_file_fmt(file_obj));
	if (loaded_ckpt) {
		offset = ckpt_mgr->get_recall_ckpt_offset();
	}

	filesize = isi_cbm_file_get_filesize(file_obj, error_out);
	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to get file size "
		    "for %{}", isi_cbm_file_fmt(file_obj));
		goto out;
	}

	databuf = (char *)malloc(regionsize);
	if (databuf == NULL) {
		*error_out = isi_cbm_error_new(CBM_CLAPI_OUT_OF_MEMORY,
		    "Could fetch uncached data for %{}",
		    isi_cbm_file_fmt(file_obj));
		goto out;
	}

	// read, but NOT from the cache

	for (bytes_left_to_read_from_cloud = filesize - offset;
	      bytes_left_to_read_from_cloud > 0;
	      bytes_left_to_read_from_cloud -= total_read)
	{
		read_size = bytes_left_to_read_from_cloud > regionsize ?
			 regionsize : bytes_left_to_read_from_cloud;
		total_read = isi_cbm_file_read_with_retry(file_obj, databuf, offset,
		    read_size, CO_NOCACHE, false, false, &error);

		ON_ISI_ERROR_GOTO(out, error);

		if (total_read < (ssize_t)read_size) {
			ilog(IL_DEBUG,
			    "Did not get all the requsted data from cloud "
			    "requested: %ld, got: %ld for %{}. "
			    "Will not mark as cached",
			    read_size, total_read, isi_cbm_file_fmt(file_obj));
			error = isi_cbm_error_new(CBM_SIZE_MISMATCH,
			    "Fetched %lld bytes, requested %lld",
			    total_read, read_size);
			goto out;
		}
		/*
		 * Get the sparse map related to this range (CDO).  Hopefully this is
		 * still cached with the checksum information so that we are not
		 * forced to read from the cloud again...
		 */
		isi_cbm_get_sparse_map(file_obj, offset, spa, &error);
		if (error) {
			isi_error_add_context(error,
			    "Could not get sparse area header for %{}",
			    isi_cbm_file_fmt(file_obj));
			goto out;
		}

		spa.trace_spa("Output data using spa", __func__, __LINE__);

		/*
		 * Note the /2 is the worst case number of iovec is when every other
		 * block is sparse and we only are writing the non-sparse blocks.
		 */
		vec_count = (spa.get_sparse_map_length() * BITS_PER_CHAR) / 2;
		if (vec_count == 0) {
			/*
			 * No sparse map length is 0 (no sparse map info) so do the
			 * whole databuf as a single vector
			 */
			vec = (struct iovec *)malloc(sizeof(struct iovec));
			if (vec == NULL) {
				*error_out = isi_cbm_error_new(CBM_CLAPI_OUT_OF_MEMORY,
				    "Could fetch uncached data for %{}",
				    isi_cbm_file_fmt(file_obj));
				goto out;
			}

			vec->iov_base = databuf;
			vec->iov_len = read_size;
		} else {
			/*
			 * Only write the regions that where not marked as sparse.
			 * First get the space for the io vectors.
			 */
			vec = (struct iovec *)calloc(sizeof(struct iovec), vec_count);
			if (vec == NULL) {
				*error_out = isi_cbm_error_new(CBM_CLAPI_OUT_OF_MEMORY,
				    "Could fetch uncached data for %{}",
				    isi_cbm_file_fmt(file_obj));
				goto out;
			}

			/*
			 * Next convert the sparse map information into an iovec for
			 * use in writing the non-sparse information.
			 */
			isi_cbm_sparse_map_to_iovec(file_obj, spa,
			    offset % mapinfo.get_chunksize(),
			    read_size, databuf, mapinfo.get_chunksize(),
			    vec, &vec_count, NULL, &error);
			if (error) {
				isi_error_add_context(error,
				    "Could not generate iovec from sparse area header"
				    "for %{}",
				    isi_cbm_file_fmt(file_obj));
				goto out;
			}
		}

		ILOG_START(IL_TRACE) {
			fmt_print(ilogfmt,
			    "next_offset: %ld, iovec count: %d, iovec (base,len):",
			    offset, vec_count);
			for (i = 0; i < vec_count; i ++) {
				fmt_print(ilogfmt, " (%p, %ld [0x%0lx])",
				    vec[i].iov_base, vec[i].iov_len,
				    vec[i].iov_len);
			}
		} ILOG_END;


		/*
		 * loop through the iov entries that were generated as these are the
		 * non-sparse areas of the file as defined in the sparse map retrieved
		 * with the CDO corresponding to the region requested.
		 */
		for (i = 0; i < vec_count; i++) {
			/* adjust offset for pwrite based on base address in iovec */
			this_offset = ((char *)(vec[i].iov_base) - databuf) +
			    offset;

			/* write this section */
			this_write = cache_pwritev(__func__, file_obj->fd,
			    &vec[i], 1, this_offset, true,
			    0);

			if (this_write < (ssize_t)vec[i].iov_len) {
				error = isi_system_error_new(errno,
				    "Failed write to cache file for"
				    "offset %ld, length %ld, result %ld "
				    "with O_ENFORCE_QUOTA %s",
				    this_offset, vec[i].iov_len, this_write,
				    "CLEAR");
				goto out;
			}

		}
		offset += read_size;

		if (vec) {
			free(vec);
			vec = NULL;
		}

		write_checkpoint(file_obj, offset, ckpt_mgr, &error);
		if (error != NULL) {
			if (isi_cbm_error_is_a(error, CBM_FILE_PAUSED) ||
			    isi_cbm_error_is_a(error, CBM_FILE_CANCELLED) ||
			    isi_cbm_error_is_a(error, CBM_FILE_STOPPED))
				goto out;
			isi_error_add_context(error, "Failed to "
			    "update_map_checkpoint "
			    "for lin %{}",
			    isi_cbm_file_fmt(file_obj));
			goto out;
		}
	}

 out:

	 if (databuf != NULL) {
		 free(databuf);
		 databuf = NULL;
	 }

	 if (vec) {
		 free(vec);
		 vec = NULL;
	 }

	 isi_error_handle(error, error_out);
}

#define MAX_RETRIES 10

/**
 * Recall an entire file, and convert from stub file back to regular file.
 */
void  ////////把stub文件变回regular file
isi_cbm_recall(ifs_lin_t lin, ifs_snapid_t snap_id, bool retain_stub,
    struct isi_cfm_policy_provider *p_ppi, void *opaque,
    get_ckpt_data_func_t get_ckpt_data_func,
    set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;
	cl_cmo_ostream out_stream;
	struct isi_cbm_file *file_obj = NULL;

	isi_cloud_object_id objectid;
	size_t synced_offset = 0;
	scoped_sync_lock sync_lock;
	scoped_stub_lock stub_lock;
	bool ch_locked = false;
	bool ch_opened = false;
	off_t start_region = 0;
	off_t end_region = 0;
	enum isi_cbm_cache_status status;
	bool is_committed = false;
	bool equals = false;
	int flags = 0;
	bool dont_use_cache = true;

	isi_cloud_object_id temp_id;
	recall_ckpt_mgr *ckpt_mgr = new recall_ckpt_mgr(opaque,
	    get_ckpt_data_func, set_ckpt_data_func);
	bool loaded_ckpt = false;
	bool ckpt_found = false; //true means we are continuing an interrupted
	 			 //recall job
	int num_retries = 0;
	bool testing_no_cache = false;

	// start with stub recall
	ilog(IL_DEBUG, "Recalling %{}", lin_snapid_fmt(lin, snap_id));

	file_obj = isi_cbm_file_open(lin, snap_id, flags, &error);
	if (error)
		goto out;

	// WORM commited (expired or not) check before recall the file
	is_committed =
	    is_cbm_file_worm_committed(file_obj->lin, file_obj->snap_id, &error);

	ON_ISI_ERROR_GOTO(out, error, "Failed WORM check for %{}",
	    isi_cbm_file_fmt(file_obj));

	if (is_committed) {
		error = isi_cbm_error_new(CBM_PERM_ERROR,
		    "File %{} is WORM committed",
		    isi_cbm_file_fmt(file_obj));

		goto out;
	}

	loaded_ckpt = ckpt_mgr->load_ckpt(&error);
	ON_ISI_ERROR_GOTO(out, error, "Err occurred in loading ckptdata for %{}",
	    isi_cbm_file_fmt(file_obj));

	if (loaded_ckpt) {
		if (ckpt_mgr->get_recall_ckpt_lin() == file_obj->lin &&
		    ckpt_mgr->get_recall_ckpt_snapid() == file_obj->snap_id) {
			ilog(IL_DEBUG, "previously interrupted recall is restarted "
			    "for %{}", isi_cbm_file_fmt(file_obj));
			ckpt_found = true;
		} else {
			ilog(IL_ERR, "mismatched checkpoint data for %{}",
			    isi_cbm_file_fmt(file_obj));
		}
	}

	fd = file_obj->fd;
	// CMO on Azure has obsolete object_id
	out_stream.set_object_id(file_obj->mapinfo->get_object_id());

	sync_lock.lock_ex(fd, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// avoid colliding with the existing one, automatically delete it
	// after done
	temp_id.generate();
	out_stream.set_suffix(".recall." + temp_id.to_string());
	out_stream.set_auto_delete(true);
	out_stream.set_overwrite(true);
	/*
	 * If there is mismatch in the mapinfo between the inode and
	 * what is on the cloud fail the operation.
	 */

	isi_cph_fetch_cmo(file_obj, &out_stream, &error);
	ON_ISI_ERROR_GOTO(out, error);

	{
		isi_cfm_mapinfo &inode_mapinfo = *file_obj->mapinfo;
		isi_cfm_mapinfo &cloud_mapinfo =
		    const_cast<isi_cfm_mapinfo &>(out_stream.get_mapinfo());

		// comparing the CMO snapid is pointless as it is taken
		// after CMO is written. We got the cloud cmo using the recorded
		// snapid, set it to that so that compare does not complain.
		objectid = cloud_mapinfo.get_object_id();
		objectid.set_snapid(inode_mapinfo.get_object_id().get_snapid());
		cloud_mapinfo.set_object_id(objectid);

		equals = inode_mapinfo.equals(cloud_mapinfo, &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (!equals) {
			inode_mapinfo.compare_fields(cloud_mapinfo, false);
			error = isi_cbm_error_new(CBM_STUBMAP_MISMATCH,
			    "mapinfo's for inode stub and the cloud cmo are "
			    "different for %{}",
			    lin_snapid_fmt(lin, snap_id));
			goto out;
		}

		synced_offset = inode_mapinfo.get_filesize();
	}

	/*
	 * Make sure the cache is open, if we did not have to do any fills
	 * it will still be closed.
	 */
	UFAIL_POINT_CODE(cbm_recall_fail_to_open_cache, {
		testing_no_cache = true;
	});
	while (true) {

		if (!testing_no_cache) {
			ch_opened = isi_cbm_file_open_and_or_init_cache(file_obj,
			    true, true, &error);
		} else {
			error = isi_cbm_error_new(CBM_LIMIT_EXCEEDED,
						  "Generated by failpoint "
						  "Emulating error to open cache");
		}

		if (error == NULL) {
			break;
		} else {
			num_retries++;
			if (num_retries >= MAX_RETRIES) {
				goto out;
			}
			isi_error_free(error);
			error = NULL;
			file_obj->cacheinfo->cacheheader_lock(false, &error);
			ON_ISI_ERROR_GOTO(out, error);
			ch_locked = true;
			file_obj->cacheinfo->cache_open(file_obj->fd,
			    true, true, &error);
			if (error) {
				isi_error_free(error);
				error = NULL;
				dont_use_cache = true;
				break;
			} else {
				file_obj->cacheinfo->cache_close();
				file_obj->cacheinfo->cacheheader_unlock(&error);
				ON_ISI_ERROR_GOTO(out, error);
				ch_locked = false;
				continue;

			}

		}

	}

	if (dont_use_cache && retain_stub) {
		error = isi_cbm_error_new(CBM_PERM_ERROR,
		    "dont_use_cache and retain stub "
		    "are set together for %{}",
		    isi_cbm_file_fmt(file_obj));
		goto out;

	}

	// if (!dont_use_cache) {
	// 	ch_locked = true;

	// 	if (file_obj->cacheinfo->is_fully_cached()) {
	// 		// data is already fully cached. do the clean up only
	// 		if (retain_stub) {
	// 			// done, no need to do the rest
	// 			goto out;
	// 		}
	// 		else
	// 			goto clean;
	// 	}

	// 	if (synced_offset > 0) {
	// 		// release the lock for cloud operations

	// 		file_obj->cacheinfo->cacheheader_unlock(&error);
	// 		ON_ISI_ERROR_GOTO(out, error);
	// 		ch_locked = false;

	// 		fetch_uncached_file_data(file_obj,
	// 		    opaque,
	// 		    get_ckpt_data_func,
	// 		    &error);
	// 		ON_ISI_ERROR_GOTO(out, error);

	// 		// locked again.
	// 		file_obj->cacheinfo->cacheheader_lock(false, &error);
	// 		ON_ISI_ERROR_GOTO(out, error);
	// 		ch_locked = true;
	// 	}

	// 	ilog(IL_DEBUG, "Data recall completed for %{}, retain stub is %s",
	// 	    lin_snapid_fmt(lin, snap_id), retain_stub?"True":"False");

	// 	if (retain_stub) {
	// 		// done, no need to do the rest
	// 		goto out;
	// 	}

	// 	if (fsync(fd) < 0) {
	// 		error = isi_system_error_new(errno,
	// 		    "Failed to sync after object fetch for %{}",
	// 		    lin_snapid_fmt(lin, snap_id));
	// 		goto out;
	// 	}

	// 	/*
	// 	 * Check for uncached blocks from region 0 to
	// 	 * whatever the current last region is
	// 	 */
	// 	end_region = file_obj->cacheinfo->get_last_region();
	// 	file_obj->cacheinfo->get_status_range(&start_region,
	// 	    &end_region, ISI_CPOOL_CACHE_MASK_NOTCACHED,
	// 	    &status, &error);
	// 	if ((!error) ||
	// 	    (!isi_cbm_error_is_a(error, CBM_NO_REGION_ERROR))) {
	// 		/*
	// 		 * We found a region that is NOT cached, or got a
	// 		 * general error.
	// 		 * Do not revert the stub setting
	// 		 */
	// 		if (!error) {
	// 			error = isi_cbm_error_new(CBM_STALE_STUB_ERROR,
	// 			    "Failure reverting stub file "
	// 			    "not all regions cached for %{}",
	// 			    isi_cbm_file_fmt(file_obj));
	// 		}
	// 		goto out;
	// 	}
	// }
 clean:
	/*
	 * Everything is cached, we are good to go.
	 */
	if (error) {
		isi_error_free(error);
		error = NULL;
	}
	if (ch_opened) {
		file_obj->cacheinfo->cache_close();
		ch_opened = false;
	}
	if (dont_use_cache) {
		isi_cbm_recall_without_cache(file_obj, ckpt_mgr, &error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to recall straight from the cloud %{}",
		    isi_cbm_file_fmt(file_obj));
	}
	if (ch_locked) {
		file_obj->cacheinfo->cacheheader_unlock(
		    isi_error_suppress());
		ch_locked = false;
	}

	/*
	 * checkpoint recall data in case purge failed
	 */
	ckpt_mgr->save_ckpt(file_obj->lin, file_obj->snap_id,
	    ckpt_mgr->get_recall_ckpt_offset(), &error);
	ON_ISI_ERROR_GOTO(out, error);

	// purge the stub file, pass force = false to
	// stop the process on error ENOENT
	isi_cbm_file_stub_purge(file_obj, ckpt_found, false,
	    !dont_use_cache, &error);

 out:
        if (ch_opened) {
		file_obj->cacheinfo->cache_close();
		ch_opened =  false;
        }

        if (ch_locked) {
		file_obj->cacheinfo->cacheheader_unlock(isi_error_suppress());
		ch_locked = false;
        }

	if (file_obj) {
		sync_lock.unlock();
		struct isi_error *error2 = NULL;
		isi_cbm_file_close(file_obj, &error2);

		if (error2 && error == NULL)
			error = error2;
		else if (error2)
			isi_error_free(error2);

		file_obj = NULL;
	}

	if (ckpt_mgr) {
		delete ckpt_mgr;
	}

	if (error) {
		ilog(IL_DEBUG, "Finished recalling %{}, error: %#{}",
		    lin_snapid_fmt(lin, snap_id), isi_error_fmt(error));
		isi_error_handle(error, error_out);
	} else
		ilog(IL_DEBUG, "Finished recalling %{} successfully",
		    lin_snapid_fmt(lin, snap_id));

}


static int timespec_to_str(char *buf, uint len, const struct timespec *ts)
{
	int ret;
	struct tm t;

	tzset();
	if (localtime_r(&(ts->tv_sec), &t) == NULL)
		return 1;

	ret = strftime(buf, len, "%F %T", &t);
	if (ret <= 0)
		return 2;
	len -= ret - 1;
	if (len <= 0)
		return 2;
	ret = snprintf(&buf[strlen(buf)], len, ".%09ld", ts->tv_nsec);
	if ((uint)ret >= len)
		return 3;

	return 0;
}

void dump(const struct stat &stats, ifs_lin_t lin)
{
	const uint TIME_FMT_STR = strlen("2012-12-31 12:59:59.123456789") + 1;
	char timestr[TIME_FMT_STR];
	int ret = 0;
	std::cout << "Stats Structure:\n";
	std::cout << "inode dev: " << stats.st_dev << std::endl;
	std::cout << "inode number: " << stats.st_ino << std::endl;
	std::cout << "inode mode: " << stats.st_mode << std::endl;
	std::cout << "hard links: "  << stats.st_nlink << std::endl;
	std::cout << "user ID: "  << stats.st_uid << std::endl;
	std::cout << "group ID: "  << stats.st_gid << std::endl;
	std::cout << "device type: "  << stats.st_rdev << std::endl;
	ret = timespec_to_str(timestr, TIME_FMT_STR, &stats.st_atimespec);
	if (ret == 0) {
		std::cout << "last access time: "  <<  timestr << std::endl;
	}

	ret = timespec_to_str(timestr, TIME_FMT_STR, &stats.st_mtimespec);
	if (ret == 0) {
		std::cout << "data mod time: "  << timestr << std::endl;
	}
	ret = timespec_to_str(timestr, TIME_FMT_STR, &stats.st_ctimespec);
	if (ret == 0) {
		std::cout << "file status change: "  << timestr << std::endl;
	}
	std::cout << "file size, in bytes: " << stats.st_size << std::endl;
	std::cout << "blocks allocated: " << stats.st_blocks << std::endl;
	std::cout << "optimal blocksize: " << stats.st_blksize <<  std::endl;
	std::cout << "user def flags: " << stats.st_flags << std::endl;
	std::cout << "file gen num: " << stats.st_gen << std::endl;
	std::cout << "lspare: " << stats.st_lspare << std::endl;
	ret = timespec_to_str(timestr, TIME_FMT_STR, &stats.st_birthtimespec);
	if (ret == 0) {
		/* time of file creation */
		std::cout << "file birthtime: " << timestr << std::endl;
	}
	struct fmt FMT_INIT_CLEAN(fmt);
	fmt_print(&fmt, "Snapshot ID: %{}",
	    lin_snapid_fmt(lin, stats.st_snapid));
	std::cout << fmt_string(&fmt) << std::endl; /* snapshot ID (or 0) */
}

/**
*
* Dump a CMO from the cloud, used for diagnostics/ testing.
*/
void
isi_cbm_dump_cmo(ifs_lin_t lin, ifs_snapid_t snap_id,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cl_cmo_ostream out_stream;
	const isi_cfm_mapinfo *inode_mapinfo;
	const isi_cfm_mapinfo *cloud_mapinfo;
	struct stat stats;
	struct stat stat_buf;
	bool equals = false;
	int flags = 0;
	isi_cloud_object_id temp_id;

	// Variables for Lin/Path processing
	std::string	strPathData;
	size_t		pos = -1;

	std::cout << "Looking at both inode stub & cloud cmo ...\n";
	struct isi_cbm_file *file_obj =
	    isi_cbm_file_open(lin, snap_id, flags, &error);

	if (error) {
		ilog(IL_ERR, "Failed to open a file...  %{}\n",
		    lin_snapid_fmt(lin, snap_id));
		std::cerr << "Failed to open file\n";

		isi_error_add_context(error,
		    "failed to open file %{}",
		    lin_snapid_fmt(lin, snap_id));
		goto out;
	}

	if (file_obj == NULL || fstat(file_obj->fd, &stat_buf) != 0) {
		error = isi_system_error_new(errno,
		    "failed to fstat file");
		goto out;
	}

	if ((stat_buf.st_flags & SF_FILE_STUBBED) == 0) {
		error = isi_cbm_error_new(CBM_NOT_A_STUB,
		    "file has not been archived");
		goto out;
	}

	// CMO on Azure has obsolete object_id
	out_stream.set_object_id(file_obj->mapinfo->get_object_id());

	// avoid colliding with the existing one
	temp_id.generate();
	out_stream.set_suffix(".dump." + temp_id.to_string());
	out_stream.set_auto_delete(true);
	out_stream.set_overwrite(true);

	isi_cph_fetch_cmo(file_obj, &out_stream, &error);
	if (error) {
		const isi_cfm_mapinfo& mapinfo = *(file_obj->mapinfo);
		ilog(IL_ERR, "Failed to fetch CMO\n");
		std::cerr << "Failed to fetch CMO account:"
		    << mapinfo.get_account().get_id() << std::endl
		    << "CMO Container: " << mapinfo.get_container()
		    << std::endl
		    << "CMO Object:"
		    << mapinfo.get_object_id().to_c_string() << std::endl;

		isi_error_add_context(error,
		    "failed to fetch CMO");
		goto out;
	}
	if (out_stream.get_mapinfo().is_overflow()) {
		const_cast<isi_cfm_mapinfo &>(out_stream.get_mapinfo())
		    .open_store(false, false, NULL, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	cloud_mapinfo = &out_stream.get_mapinfo();

	inode_mapinfo = file_obj->mapinfo;

	equals = inode_mapinfo->equals(*cloud_mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (!equals) {
		inode_mapinfo->compare_fields(*cloud_mapinfo, false);
		ilog(IL_ERR, "inode mapinfo and cloud mapinfo differ!\n");
		std::cout << "Inode and Cloud mapinfo are: different:\n";
		inode_mapinfo->dump();
		std::cout << "End of the Inode Map\n";
	}
	else {
		std::cout << "Inode and Cloud mapinfo are: identical\n";
	}

	//
	// Distinct Lin and Path
	//	The Lin and Path are both returned by get_path(). Need
	//  to get the data and the split into the two pieces. A space splits the //	string into two parts.
	//
	strPathData = out_stream.get_path();
	pos			= strPathData.find(' ');

	if (pos > 0)
	{
		//
		// Lin was found!
		//
		std::cout << "Lin: ";
		std::cout << strPathData.substr(0, pos);
		std::cout << std::endl;

		//
		// Path follows
		//
		std::cout << "Path: ";
		std::cout << strPathData.substr(pos + 1, std::string::npos);
	}
	else
	{
		//
		// For some reason the Lin was not found in the data, display the data
		// and let the user parse it.
		//
		std::cout << "Lin and Path are: ";
		std::cout << out_stream.get_path();
	}

	std::cout << "Cloud Map Info is:\n";
	cloud_mapinfo->dump();
	stats = out_stream.get_stats();
	dump(stats, lin);
 out:
	isi_error_handle(error, error_out);
	if (file_obj)
		isi_cbm_file_close(file_obj, isi_error_suppress());
}

void
isi_cbm_file_stub_purge(
    struct isi_cbm_file *file_obj,
    bool force,
    bool wait,
    bool delete_cacheinfo,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	u_int64_t filerev;
	int result;
	int fd = file_obj->fd;

	scoped_stub_lock stub_lock;

	stub_lock.lock_ex(fd, wait, &error);
	ON_ISI_ERROR_GOTO(out, error);

	gc_cloud_object(file_obj, &error);
	if (error) {
		if (force && isi_system_error_is_a(error, ENOENT)) {
			/*
			 * when force is set to true, it is expected
			 * that file can be purged without finding corresponding
			 * coi entry.
			 * In the case of stub file recall case, force is set
			 * to true if checkpointed data can be retrieved. This
			 * indicates that current recall job is continued from
			 * previously interruption and hence the corresponding
			 * coi entry could be GCed if previous recall job was
			 * interrupted after the file is dereferenced from COI.
			 */
			ilog(IL_DEBUG, "unable to find coi entry, ignore error "
			    "for %{}", lin_fmt(file_obj->lin));
			isi_error_free(error);
			error = NULL;
		}
		else
			goto out;
	}

	UFAIL_POINT_CODE(fail_recall_with_gc_scheduled, {ASSERT(0);});

	/*
	 * We will use the file rev to validate that no changes have been made
	 * to the file while we are converting it back. Note that the
	 * filerev WILL ALWAYS change while writing to the file using
	 * standard system calls (write, pwrite, writev, pwritev).
	 *
	 * XXXegc: this only really protects the system call.  We need to add
	 * more to determine if the recall is valid or not....
	 */
	if (getfilerev(file_obj->fd, &filerev)) {
		error = isi_system_error_new(errno,
		    "error getting initial filerev for %{}",
		    lin_fmt(file_obj->lin));
		goto out;
	}
	else {
		/*
		* With the file retrieved, revert it back from a stub
		* to a regular file.
		*/
		result = ifs_cpool_stub_ops(
		    file_obj->fd, IFS_CPOOL_STUB_RECALL, filerev, NULL,
			NULL, NULL);

		if (result != 0 && !(force && errno == ENOENT)) {
			error = isi_system_error_new(errno,
			    "Failure reverting stub file to regular file "
			    "for %{}",
			    lin_fmt(file_obj->lin));
			goto out;
		}
	}
	stub_lock.unlock();

	/**
	 * Now blow away the cache info. After a successful full cache,
	 * we no longer care what happens to the file after that.
	 */
	if (delete_cacheinfo) {
		isi_cbm_file_remove_cache_info(file_obj, &error);
	}

	if (error) {
		if (force && isi_system_error_is_a(error, ENOENT)) {
			isi_error_free(error);
			error = NULL;
		}
		else
			goto out;
	}

 out:
	isi_error_handle(error, error_out);
}
