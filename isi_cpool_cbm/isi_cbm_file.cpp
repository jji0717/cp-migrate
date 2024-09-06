#include "isi_cbm_file.h"

#include <sys/stat.h>
#include <sys/extattr.h>
#include <vector>

#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include "isi_cbm_error.h"
#include <isi_cloud_common/isi_cpool_version.h>

#include <isi_cpool_d_api/cpool_api.h>
#include <isi_ilog/ilog.h>
#include <isi_cpool_cbm/isi_cbm_account_util.h>
#include <isi_cpool_cbm/isi_cpool_cbm.h>
#include <isi_cpool_cbm/isi_cbm_data_stream.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_cache.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_cpool_cbm/isi_cbm_read.h>
#include <isi_cpool_cbm/isi_cbm_util.h>
#include <isi_cpool_cbm/isi_cbm_write.h>
#include <isi_cpool_cbm/isi_cbm_csi.h>
#include <isi_cpool_cbm/isi_cbm_wbi.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cbm_sparse.h>
#include <isi_cpool_cbm/isi_cbm_error_util.h>
#include <isi_cpool_cbm/isi_cbm_file_versions.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_domain/dom.h>

#ifdef OVERRIDE_ILOG_FOR_DEBUG
#if defined(ilog)
#undef ilog
#endif
#define ilog(a, args...)                               \
	{                                                  \
		struct fmt FMT_INIT_CLEAN(thisfmt);            \
		fmt_print(&thisfmt, args);                     \
		fprintf(stderr, "%s\n", fmt_string(&thisfmt)); \
	}
#endif

extern struct fmt_conv_ctx isi_cbm_cache_status_fmt(enum isi_cbm_cache_status value);

static ssize_t
cache_pwritev(const char *func, int fd, const struct iovec *iov, int iovcnt,
			  off_t off, bool no_time_update, int extra_flags)
{
	ssize_t len = 0;
	int i;
	for (i = 0; i < iovcnt; i++)
	{
		len += iov->iov_len;
	}
	ilog(IL_CP_EXT,
		 "%s: pwrite called with fd: %d, iovcnt: %d, len: %ld "
		 "offset: %ld, extra flags: %d",
		 func, fd, iovcnt, len, off, extra_flags);
	return ifs_cpool_pwritev(fd, iov, iovcnt, off,
							 extra_flags | (no_time_update ? (O_SYNC | O_IGNORETIME) : 0));
}

static ssize_t
cache_pwrite(const char *func, int fd, const void *buf,
			 size_t sz, off_t off, bool no_time_update, int extra_flags)
{
	struct iovec vec;

	vec.iov_base = (void *)buf;
	vec.iov_len = sz;

	ilog(IL_CP_EXT,
		 "%s: pwrite called with fd: %d, len: %ld, "
		 "offset: %ld, extra flags: %d",
		 func, fd, sz, off, extra_flags);
	return cache_pwritev(func, fd, &vec, 1, off, no_time_update,
						 extra_flags);
}

static ssize_t
isi_cbm_cache_fill_region(
	struct isi_cbm_file *file,
	off_t region,
	char *databuf,
	size_t datasize,
	enum isi_cbm_io_cache_option cache_opt,
	bool enforce_no_cache,
	struct isi_error **error_out);

namespace
{

	// static scoped_mutex g_cbm_cfg_mtx;

	// static isi_cbm_cfg_ctx_sptr g_cbm_cfg_ctx;

	static scoped_mutex g_cbm_wbi_mtx;
	static scoped_mutex g_cbm_csi_mtx;
	static scoped_mutex g_cbm_coi_mtx;
	static scoped_mutex g_cbm_ooi_mtx;

	static isi_cbm_wbi_sptr g_cbm_wbi;
	static isi_cbm_csi_sptr g_cbm_csi;
	static isi_cbm_coi_sptr g_cbm_coi;
	static isi_cbm_ooi_sptr g_cbm_ooi;

	void
	isi_cbm_file_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
						  union fmt_conv_arg arg)
	{
		const struct isi_cbm_file *file = (struct isi_cbm_file *)arg.ptr;

		if (!file)
		{
			fmt_print(fmt, "%s", "(null)");
			return;
		}

		fmt_print(fmt, "file lin %{} snapid %{}",
				  lin_fmt(file->lin), snapid_fmt(file->snap_id));
	}

	/**
	 * Create a new instance of isi_cbm_file object and initialize it
	 *
	 * @param lin[IN]: The LIN number of the file
	 * @param snap_id[IN]: the snapshot id of the file
	 * @param error_out[OUT]: errors if any, see above for detailed information.
	 */
	struct isi_cbm_file *
	alloc_cbm_file(uint64_t lin, uint64_t snap_id, isi_error **error_out)
	{
		struct isi_error *error = NULL;
		struct isi_cbm_file *file_obj = NULL;
		bool read_only = false;

		ASSERT(error_out && !(*error_out));

		if (snap_id == HEAD_SNAPID)
		{
			// check if file is worm committed and so read-only
			read_only = is_cbm_file_worm_committed(
				lin, snap_id, &error);
			ON_ISI_ERROR_GOTO(out, error);
		}
		else
		{
			// snaps are always read-only
			read_only = true;
		}

		file_obj = new struct isi_cbm_file;

		file_obj->lin = lin;
		file_obj->snap_id = snap_id;
		file_obj->fd = -1;

		file_obj->mapinfo = new isi_cfm_mapinfo();
		// here without exception catch,  equivalent to ASSERT(mapinfo != NULL)

		file_obj->cacheinfo = new cpool_cache(file_obj);
		if (read_only)
		{
			file_obj->cacheinfo->set_read_only(true);
		}

	out:
		isi_error_handle(error, error_out);
		return file_obj;
	}

	void
	close_cbm_file_suppress_error(struct isi_cbm_file *file_obj)
	{
		ASSERT(file_obj);
		isi_cbm_file_close(file_obj, isi_error_suppress(IL_ERR));
	}

	/**
	 * helper routine to get policy by id
	 * if not found without error, NULL is returned
	 */
	static fp_policy *
	get_policy_by_id(const isi_cbm_id &policy_id,
					 isi_cfm_policy_provider *policy_ctx,
					 isi_error **error_out)
	{
		struct fp_policy *cp_policy = NULL;
		isi_error *error = NULL;
		smartpools_get_policy_by_id_and_cluster(policy_id.get_id(),
												policy_id.get_cluster_id(), &cp_policy,
												policy_ctx->sp_context, &error);
		if (error != NULL &&
			!isi_error_is_a(error, ISI_EXCEPTION_ERROR_CLASS))
		{
			isi_error_add_context(error, "failed to lookup policy "
										 "using id (%u) and cluster id (%s)",
								  policy_id.get_id(), policy_id.get_cluster_id());
			goto out;
		}

		if (error)
		{
			isi_error_free(error);
			error = NULL;
		}
	out:
		isi_error_handle(error, error_out);
		return cp_policy;
	}
} // end of anonymous namespace

bool is_cbm_file_worm_committed(
	ifs_lin_t lin, ifs_snapid_t snap_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct worm_state worm = {};
	bool committed = false;

	if (dom_get_info_by_lin(lin, snap_id, NULL, NULL,
							&worm) == 0)
	{
		committed = worm.w_committed;
	}
	else if (errno != ENOENT)
	{
		error = isi_system_error_new(errno,
									 "unable to get dom info for file %{}",
									 lin_snapid_fmt(lin, snap_id));
	}

	isi_error_handle(error, error_out);

	return committed;
}

void isi_cbm_reload_config(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	close_ppi();

	scoped_ppi_reader ppi_rdr;

	ppi_rdr.get_ppi(&error); // force ppi creation here
	if (error)
		goto cleanup;

cleanup:
	isi_error_handle(error, error_out);
}

void isi_cbm_unload_conifg()
{
	close_ppi();
}

static ssize_t
isi_cbm_file_cache_read(struct isi_cbm_file *file,
						off_t offset, size_t length,
						void *databuf, enum isi_cbm_io_cache_option cache_opt,
						bool ch_lock_held, struct isi_error **error_out);

static ssize_t
isi_cbm_file_cache_write(struct isi_cbm_file *file,
						 uint64_t offset, size_t length,
						 const void *databuf, struct isi_error **error_out);

/*
isi_cbm_cfg_ctx_sptr
isi_cbm_get_cfg_contexts(struct isi_error **error_out)
{
	if (!g_cbm_cfg_ctx) {
		struct isi_error *error = NULL;
		scoped_lock lock(*g_cbm_cfg_mtx);
		if (g_cbm_cfg_ctx)
			return g_cbm_cfg_ctx;

		isi_cbm_reload_config(&error);
		if (error)
			isi_error_handle(error, error_out);
	}

	return g_cbm_cfg_ctx;
}
*/

isi_cbm_wbi_sptr
isi_cbm_get_wbi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	if (!g_cbm_wbi)
	{
		scoped_lock lock(*g_cbm_wbi_mtx);
		if (g_cbm_wbi)
			goto out;

		wbi *wb = new wbi();
		wb->initialize(&error);
		if (error != NULL)
		{
			isi_error_add_context(error,
								  "Could not initialize WBI.");
			delete wb;
			goto out;
		}
		wb->set_release_version(CPOOL_WBI_VERSION);
		g_cbm_wbi.reset(wb);
	}
out:
	isi_error_handle(error, error_out);
	return g_cbm_wbi;
}

isi_cbm_csi_sptr
isi_cbm_get_csi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	if (!g_cbm_csi)
	{
		scoped_lock lock(*g_cbm_csi_mtx);
		if (g_cbm_csi)
			goto out;

		csi *cs = new csi();
		cs->initialize(&error);
		if (error != NULL)
		{
			isi_error_add_context(error,
								  "Could not initialize CSI.");
			delete cs;
			goto out;
		}
		cs->set_release_version(CPOOL_CSI_VERSION);
		g_cbm_csi.reset(cs);
	}
out:
	isi_error_handle(error, error_out);
	return g_cbm_csi;
}

isi_cbm_coi_sptr
isi_cbm_get_coi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	if (!g_cbm_coi)
	{
		scoped_lock lock(*g_cbm_coi_mtx);
		if (g_cbm_coi)
			goto out;

		coi *co = new coi();
		co->initialize(&error);
		if (error)
		{
			isi_error_add_context(error,
								  "Could not initialize COI.");
			delete co;
			goto out;
		}
		co->set_release_version(CPOOL_COI_VERSION);
		g_cbm_coi.reset(co);
	}
out:
	isi_error_handle(error, error_out);
	return g_cbm_coi;
}

isi_cbm_ooi_sptr
isi_cbm_get_ooi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	if (!g_cbm_ooi)
	{
		scoped_lock lock(*g_cbm_ooi_mtx);
		if (g_cbm_ooi)
			goto out;

		ooi *oo = new ooi();
		oo->initialize(&error);
		if (error)
		{
			isi_error_add_context(error,
								  "Could not initialize OOI.");
			delete oo;
			goto out;
		}
		oo->set_release_version(CPOOL_OOI_VERSION);
		g_cbm_ooi.reset(oo);
	}
out:
	isi_error_handle(error, error_out);
	return g_cbm_ooi;
}

void isi_cbm_get_sparse_map(struct isi_cbm_file *file_obj, off_t offset,
							struct spa_header &spa, struct isi_error **error_out)
{
	isi_cfm_mapinfo &mapinfo = *(file_obj->mapinfo);
	isi_cfm_mapentry map_entry;
	isi_cfm_mapinfo::iterator itr;
	checksum_header cs_header;
	spa_header spa_header;

	struct isi_error *error = NULL;

	isi_cpool_master_cdo master_cdo;
	isi_cbm_ioh_base_sptr io_helper;
	isi_cbm_ioh_base *ioh_base = NULL;

	int index = 0;

	mapinfo.get_containing_map_iterator_for_offset(itr, offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (itr == mapinfo.end())
	{
		mapinfo.dump_to_ilog();
		error = isi_cbm_error_new(CBM_MAP_ENTRY_NOT_FOUND,
								  "cannot find map entry for starting read position "
								  "%ld for %{}",
								  offset, isi_cbm_file_fmt(file_obj));
		goto out;
	}

	map_entry = itr->second;

	io_helper = isi_cpool_get_io_helper(map_entry.get_account_id(),
										&error);
	if (!io_helper)
		goto out;

	ioh_base = io_helper.get();

	master_cdo_from_map_entry(master_cdo, mapinfo, &map_entry);

	index = offset / mapinfo.get_chunksize() + 1;

	try
	{
		ioh_base->get_cdo_headers(master_cdo, index,
								  mapinfo.is_compressed(), mapinfo.has_checksum(),
								  cs_header, spa_header);
	}
	catch (isi_cloud::cl_exception &cl_err)
	{
		error = convert_cl_exception_to_cbm_error(cl_err);
	}
	catch (isi_exception &e)
	{
		error = e.get() ? e.detach() : isi_cbm_error_new(CBM_ISI_EXCEPTION_FAILURE, "%s", e.what());
	}
	catch (const std::exception &e)
	{
		error = isi_cbm_error_new(
			CBM_STD_EXCEPTION_FAILURE, "%s", e.what());
	}
	ON_ISI_ERROR_GOTO(out, error);

	spa_header.trace_spa("retrived sparse area header", __func__, __LINE__);

	spa = spa_header;
out:
	isi_error_handle(error, error_out);
}

/**
 * Given the lin and snap_id, open an existing stub file. The caller must have
 * the privilege to open the file by the LIN number.
 * @param lin[IN]: The LIN number of the file
 * @param snap_id[IN]: the snapshot id of the file
 * @param s_flags[IN]: the sync flags for opening file
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the opaque isi_cbm_file, this object must be closed by calling
 *	   isi_cbm_close.
 */
static struct isi_cbm_file *
isi_cbm_file_open_i(uint64_t lin, uint64_t snap_id, int s_flags,
					struct isi_error **error_out)
{
	struct isi_cbm_file *file_obj = NULL;
	struct isi_error *error = NULL;
	int fd = -1;
	struct stat stat_buf;
	int open_flags = s_flags | O_RDWR;
	printf("%s called begin ifs_lin_open\n", __func__ );
	/*
	 * Get a file descriptor based on the lin and snapid.
	 */
	if ((fd = ifs_lin_open(lin, snap_id, open_flags)) < 0)
	{
		error = isi_system_error_new(errno,
									 "cannot open file for lin %{} snapid %{}",
									 lin_fmt(lin), snapid_fmt(snap_id));
		printf("%s called can not get fd\n", __func__ );
		goto out;
	}
	printf("%s called fd:%d\n", __func__, fd);
	if (fstat(fd, &stat_buf) != 0)
	{
		error = isi_system_error_new(errno,
									 "failed to fstat file (lin %{} snapid %{})",
									 lin_fmt(lin), snapid_fmt(snap_id));
		printf("%s called can not get fstat\n", __func__);
		goto out;
	}

	/*
	 * Verify that the file to be open is in fact a stub.
	 * The following error code can be confusing. We should
	 * use CP specific error code here.
	 */
	if (!(stat_buf.st_flags & SF_FILE_STUBBED))
	{
		error = isi_cbm_error_new(CBM_NOT_A_STUB,
								  "lin %{} snapid %{} is not a stub",
								  lin_fmt(lin), snapid_fmt(snap_id));
		printf("%s called is not a stub file\n", __func__);
		goto out;
	}

	file_obj = alloc_cbm_file(lin, snap_id, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(file_obj);
	file_obj->fd = fd;
	isi_cph_get_stubmap(file_obj->fd, *(file_obj->mapinfo), &error);
	if (error)
	{
		isi_error_add_context(error,
							  "get stubmap failed for lin %{} snapid %{}",
							  lin_fmt(lin), snapid_fmt(snap_id));
		printf("%s called isi_cph_get_stubmap failed\n", __func__);
		goto out;
	}

	ilog(IL_DEBUG, "Open requested for %{} (%p)",
		 isi_cbm_file_fmt(file_obj), file_obj);

out:

	if (error)
	{
		isi_error_handle(error, error_out);

		if (file_obj)
		{
			close_cbm_file_suppress_error(file_obj);
			file_obj = NULL;
		}
		else if (fd > 0)
		{
			close(fd);
			fd = -1;
		}
	}
	return file_obj;
}

/**
 * Read data from the CBM file.
 * @param file_obj[IN]: the CBM file
 * @param pstrm[IN/OUT]: the data output stream object to receive the content
 *		       It must be capable to receive @length of bytes
 * @param offset[IN]: the offset of the file
 * @param length[IN]: the length of the read
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the number of bytes actually being read. 0 indicates end of file.
 */
static ssize_t
isi_cbm_file_fetch_range(struct isi_cbm_file *file_obj,
						 isi_cloud::odata_stream *pstrm, off_t offset, size_t length,
						 struct isi_error **error_out)
{
	ASSERT(file_obj && pstrm && offset >= 0 && !(*error_out));
	size_t bytes_to_read = length;
	size_t bytes_read = 0;
	size_t bytes_read_this_entry = 0;
	off_t total_read = 0;
	size_t read_size = 0;
	struct isi_error *error = NULL;

	ifs_lin_t lin = file_obj->lin;
	ifs_snapid_t snap_id = file_obj->snap_id;

	isi_cfm_mapinfo &mapinfo = *(file_obj->mapinfo);
	bool encrypted = mapinfo.is_encrypted();
	encrypt_ctx ectx;

	isi_cfm_mapentry map_entry, prev_map_entry;
	isi_cfm_mapinfo::iterator itr;

	if (encrypted)
	{
		mapinfo.get_encryption_ctx(ectx, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	while ((bytes_to_read > 0) &&
		   ((offset + total_read) < mapinfo.get_filesize()))
	{
		isi_cbm_ioh_base_sptr io_helper;
		mapinfo.get_containing_map_iterator_for_offset(
			itr, offset + total_read, &error);

		ON_ISI_ERROR_GOTO(out, error);

		if (itr == mapinfo.end())
		{
			mapinfo.dump_to_ilog();
			error = isi_cbm_error_new(CBM_MAP_ENTRY_NOT_FOUND,
									  "cannot find map entry for starting read position "
									  "%ld (lin %{} snapid %{})",
									  offset + total_read, lin_fmt(lin),
									  snapid_fmt(snap_id));
			goto out;
		}

		map_entry = itr->second;

		if (map_entry != prev_map_entry)
		{
			/*
			 * We've crossed over into a new map entry, so reset the
			 * counter of how many bytes within this entry we've
			 * read; we use this value later to determine how many
			 * bytes to read from the entry.
			 */
			bytes_read_this_entry = 0;
			prev_map_entry = map_entry;
		}

		io_helper = isi_cpool_get_io_helper(map_entry.get_account_id(),
											&error);
		if (!io_helper)
			goto out;

		/*
		 * How much data to read from this chunk?  The smaller of the
		 * remaining bytes to read overall and the remaining bytes in
		 * this map entry.
		 */
		read_size = MIN(bytes_to_read,
						map_entry.get_offset() + map_entry.get_length() -
							offset - bytes_read_this_entry);

		/*
		 * Read the data from the cloud.
		 * We adjust the read size to avoid exceeding total file size
		 */
		///////从云端读取数据
		isi_cpool_master_cdo master_cdo;
		master_cdo_from_map_entry(master_cdo, mapinfo, &map_entry);

		bytes_read = isi_cph_read_data_blob(pstrm, lin,
											mapinfo.get_chunksize(),
											master_cdo,
											offset + total_read, read_size,
											mapinfo.is_compressed(),
											mapinfo.has_checksum(),
											encrypted ? &ectx : NULL,
											io_helper.get(), &error);

		if (error)
		{
			isi_error_add_context(error, "failed to read data:"
										 "lin %{} snapid %{}, offset %lld, size %lld",
								  lin_fmt(lin), snapid_fmt(snap_id),
								  offset + total_read, read_size);
			goto out;
		}

		if (bytes_read <= 0)
			goto out;

		bytes_to_read -= bytes_read;
		total_read += bytes_read;
		bytes_read_this_entry += bytes_read;
	}

out:
	if (error)
	{
		isi_error_handle(error, error_out);
		return -1;
	}

	return total_read;
}

/*
 * Open the file for accessing data
 */
struct isi_cbm_file *
isi_cbm_file_open(uint64_t lin, uint64_t snap_id, int s_flags,
				  struct isi_error **error_out)
{
	return isi_cbm_file_open_i(lin, snap_id, s_flags, error_out);
}

/*
 * For now just return the value stored in the map.   If there is no size
 * specified, default it to the default of ISI_CBM_CACHE_DEFAULT_REGIONSIZE
 */
static size_t
get_regionsize(struct isi_cbm_file *file)
{
	ASSERT(file && file->mapinfo);

	size_t rz = file->mapinfo->get_readsize();

	if (rz < 1)
		rz = ISI_CBM_CACHE_DEFAULT_REGIONSIZE;

	return rz;
}

bool isi_cbm_file_open_cache_lock_excl(struct isi_cbm_file *file,
									   struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool retval = false;
	bool ch_locked = false;

	retval = isi_cbm_file_open_and_or_init_cache(file,
												 false, false, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to open cache for %{}", isi_cbm_file_fmt(file));

	/* now get the lock exclusively */
	file->cacheinfo->cacheheader_lock(true, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to lock cache header for %{}", isi_cbm_file_fmt(file));

	ch_locked = true;

out:
	if (error)
	{
		/* Should never have been able to lock in the case of error */
		ASSERT(!ch_locked);
		ilog(IL_ERR, "%s %{}, error %#{}", __FUNCTION__,
			 isi_cbm_file_fmt(file), isi_error_fmt(error));
	}

	isi_error_handle(error, error_out);
	return ch_locked;
}

/*
 * Open or initialize the cache information. The read flag indicates
 * the purpose of the opening the cache info. If the file
 * is already fully cached and the purpose of opening the cache is to
 * read, the cache will not be initialized if not being done already.
 *
 * keep_locked: hold the cache header locked on return.  The cache must NOT
 * be locked on entry.
 *
 */
bool isi_cbm_file_open_and_or_init_cache(struct isi_cbm_file *file,
										 bool keep_locked, bool read, struct isi_error **error_out)
{
	bool retval = false;
	bool ch_locked = false;
	bool init_cache = !read;
	bool excl_mode = false;
	int fd = file->fd;
	cpool_cache *cacheinfo = file->cacheinfo;
	struct isi_error *error = NULL;

	ASSERT(cacheinfo);
	ASSERT(fd != -1);

	/*
	 * handle the race between threads trying to open the cache when
	 * sharing the isi_cbm_file object to avoid multiple initializers
	 * later.
	 */
	cacheinfo->cacheheader_lock(excl_mode, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ch_locked = true;

	if (!cacheinfo->is_cache_open())
	{
		cacheinfo->cache_open(fd, true, true, &error);

		if (error)
		{
			if (isi_system_error_is_a(error, ENOENT) &&
				!cacheinfo->is_read_only())
			{
				ilog(IL_DEBUG,
					 "Cache not found for %{} (%p), error: %#{} "
					 "initializing...",
					 isi_cbm_file_fmt(file), file,
					 isi_error_fmt(error));

				isi_error_free(error);
				error = NULL;
				// if we are in the not found state,
				// we need to initialize the cache.
				if (!cacheinfo->is_fully_cached())
				{
					init_cache = true;
				}
			}
			ON_ISI_ERROR_GOTO(out, error);
		}
		else
		{
			// no error and already opened, done.
			if (cacheinfo->is_cache_open())
			{
				retval = true;
				goto out;
			}

			if (!init_cache &&
				cacheinfo->is_fully_cached())
			{
				retval = true;
				goto out;
			}
		}
		if (init_cache)
		{
			struct stat buf;

			cacheinfo->cacheheader_unlock(isi_error_suppress());
			ch_locked = false;

			excl_mode = true;
			cacheinfo->cacheheader_lock(excl_mode, &error);
			ON_ISI_ERROR_GOTO(out, error);
			ch_locked = true;

			if (fstat(fd, &buf) != -1)
			{
				// initialize common will cache_call open_create
				cacheinfo->cache_initialize_common(fd,
												   get_regionsize(file),
												   buf.st_size, file->lin,
												   true, true, &error);

				ON_ISI_ERROR_GOTO(out, error);
				retval = true;
			}
			else
			{
				error = isi_system_error_new(errno,
											 "Cannot stat file %{} "
											 "for cache initialize",
											 isi_cbm_file_fmt(file));
				goto out;
			}
		}
	}

	retval = true;

out:
	if (error)
	{
		if (ch_locked)
		{
			/*
			 * We got an error release the lock
			 */
			cacheinfo->cacheheader_unlock(isi_error_suppress());
			ch_locked = false;
		}
	}
	else if (!keep_locked && ch_locked)
	{
		cacheinfo->cacheheader_unlock(isi_error_suppress());
		ch_locked = false;
	}
	else if (keep_locked && ch_locked && excl_mode)
	{
		cacheinfo->cacheheader_downgrade_to_shlock(
			isi_error_suppress());
	}

	isi_error_handle(error, error_out);
	return retval;
}

void isi_cbm_file_remove_cache_info(struct isi_cbm_file *file,
									struct isi_error **error_out)
{
	cpool_cache *cacheinfo = file->cacheinfo;
	cacheinfo->cache_destroy(file->fd, error_out);
	if (*error_out)
	{
		isi_error_add_context(*error_out,
							  "failed to open file for %{}",
							  isi_cbm_file_fmt(file));
	}
}

MAKE_ENUM_FMT(isi_cbm_settle_time_type, enum isi_cbm_settle_time_type,
			  ENUM_VAL(STT_WRITEBACK, "writeback"),
			  ENUM_VAL(STT_INVALIDATE, "invalidation"),
			  ENUM_VAL(STT_DELAYED_INVALIDATE, "delayed invalidation"), );

void isi_cbm_file_get_settle_time(uint64_t lin, uint64_t snapid,
								  const isi_cbm_id &policy_id, isi_cbm_settle_time_type type,
								  struct timeval &stime, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fp_policy *cp_policy = NULL;
	struct timeval tv2 = {0};
	struct timeval tv = stime;

	scoped_ppi_reader ppi_rdr;

	const isi_cfm_policy_provider *cbm_cfg = ppi_rdr.get_ppi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cp_policy = get_policy_by_id(policy_id,
								 (isi_cfm_policy_provider *)cbm_cfg, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (cp_policy)
	{
		fp_cloudpool_action *cl_opt =
			cp_policy->attributes->cloudpool_action;
		if (type == STT_WRITEBACK)
			tv2.tv_sec = cl_opt->cloud_writeback_settle_time;
		else if (type == STT_INVALIDATE)
		{
			// UI is not storing this in days, so treat as seconds
			tv2.tv_sec =
				cl_opt->cloud_cache_expiration_age;
		}
		else if (type == STT_DELAYED_INVALIDATE)
		{
			// UI is not storing this in days, so treat as seconds
			tv2.tv_sec = cl_opt->delayed_invalidation_time;
		}
		tv2.tv_usec = 0;
		ilog(IL_DEBUG, "Settle time of %ld (seconds) found for "
					   "type %{} policy (%s, %d) for %{}",
			 tv2.tv_sec,
			 isi_cbm_settle_time_type_fmt(type),
			 policy_id.get_cluster_id(), policy_id.get_id(),
			 lin_snapid_fmt(lin, snapid));
	}
	else
	{
		tv2.tv_sec = SECONDS_IN_A_DAY; // seconds in a day
		tv2.tv_usec = 0;
		ilog(IL_DEBUG,
			 "Could not find policy for (%s, %d), using %ld for settle time for "
			 "%{}, type %{}",
			 policy_id.get_cluster_id(), policy_id.get_id(),
			 tv2.tv_sec, lin_snapid_fmt(lin, snapid),
			 isi_cbm_settle_time_type_fmt(type));
	}

	if (tv.tv_sec == 0 && tv.tv_usec == 0)
		gettimeofday(&tv, NULL);

	timeradd(&tv, &tv2, &stime);

	ilog(IL_DEBUG, "New settle time for %{} is %ld seconds",
		 lin_snapid_fmt(lin, snapid), stime.tv_sec);
out:
	isi_error_handle(error, error_out);
}

/*
 * queue the write-back/invalidate item when the file is modified/cached
 */
void isi_cbm_file_queue_daemon(struct isi_cbm_file *file,
							   int type, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = file->cacheinfo;
	ASSERT(cacheinfo);

	struct timeval stime = {0};

	isi_cbm_file_get_settle_time(file->lin, file->snap_id,
								 file->mapinfo->get_policy_id(),
								 (type == ISI_CBM_CACHE_FLAG_DIRTY ? STT_WRITEBACK : STT_INVALIDATE), stime, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// to queue file for writeback or invalidate
	if (type == ISI_CBM_CACHE_FLAG_DIRTY)
	{
		isi_cbm_wbi_sptr wb;

		wb = isi_cbm_get_wbi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		// to queue modified file for writeback
		wb->add_cached_file(file->lin, file->snap_id, stime, &error);
	}
	else
	{
		isi_cbm_csi_sptr cs;

		cs = isi_cbm_get_csi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		// to queue cached file for invalidation
		cs->add_cached_file(file->lin, file->snap_id, stime, &error);
	}

	if (error)
	{
		ilog(IL_ERR, "failed to queue file for %s %{}",
			 ((type == ISI_CBM_CACHE_FLAG_DIRTY) ? "writeback" : "invalidate"),
			 isi_cbm_file_fmt(file));
		// return no error and allow the write to proceed
		// the worst is we may miss a write-back or invalidate
		isi_error_free(error);
		error = NULL;
	}
out:
	isi_error_handle(error, error_out);
}

static bool
isi_cbm_queue_daemon_needed(struct isi_cbm_file *file, int queue_for_state)
{
	isi_error *error = NULL;
	bool state_set = false;
	cpool_cache *cacheinfo = file->cacheinfo;

	ASSERT(cacheinfo);

	// test state takes an error param, but doesn't use it.
	// no error will ever come back.
	state_set = cacheinfo->test_state(queue_for_state, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	if (error)
	{
		isi_error_free(error);
		error = NULL;
	}
	return !state_set;
}

/**
 * internal rountine to queue the writeback or invalidate and mark the state
 * appropriately.  @param queue_for_state is the definition from cache flags,
 * currently ISI_CBM_CACHE_FLAG_DIRTY or ISI_CBM_CACHE_FLAG_CACHED
 */
void isi_cbm_file_queue_daemon_internal(struct isi_cbm_file *file, int queue_for_state,
										isi_error **error_out)
{
	isi_error *error = NULL;
	bool queued = false;
	cpool_cache *cacheinfo = file->cacheinfo;

	ASSERT(cacheinfo);

	queued = cacheinfo->test_state(queue_for_state, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (queued == false)
	{
		// to queue file for queue_for_state(cached or dirty)
		isi_cbm_file_queue_daemon(file, queue_for_state, &error);
		ON_ISI_ERROR_GOTO(out, error);

		cacheinfo->set_state(queue_for_state, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
out:
	isi_error_handle(error, error_out);
}

/**
 * External routine to queue writeback or invalidation for a file, if needed
 */
void isi_cbm_file_queue_daemons(struct isi_cbm_file *file,
								bool cached_region, bool dirty_region,
								struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache &cacheinfo = *file->cacheinfo;
	bool ch_locked = false;
	int qtype;

	cacheinfo.cacheheader_lock(true, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ch_locked = true;

	qtype = ISI_CBM_CACHE_FLAG_CACHED;
	if (cached_region || dirty_region)
	{
		isi_cbm_file_queue_daemon_internal(file, qtype, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	qtype = ISI_CBM_CACHE_FLAG_DIRTY;
	if (dirty_region)
	{
		isi_cbm_file_queue_daemon_internal(file, qtype, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	if (ch_locked)
		cacheinfo.cacheheader_unlock(&error);
	isi_error_handle(error, error_out);
}
/*
 * Truncate a file specified by lin and snapid to the size specified.
 */
void isi_cbm_file_truncate(uint64_t lin, uint64_t snap_id, off_t offset,
						   struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int flags = 0;

	struct isi_cbm_file *file_obj =
		isi_cbm_file_open(lin, snap_id, flags, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "failed to open file for lin %{} snapid %{}",
							  lin_fmt(lin), snapid_fmt(snap_id));
		goto out;
	}
	isi_cbm_file_ftruncate(file_obj, offset, &error);

out:
	if (file_obj)
		close_cbm_file_suppress_error(file_obj);

	isi_error_handle(error, error_out);
}

/**
 * Common routine to handle the truncation case when the file is either
 * explicitly truncated up/down or truncated up due to a write.
 * If we are truncating up, we need to take care of the
 * old last region, and if we are truncating down we need to
 * take care of the new last region. We need to check if the region's
 * status will be changed to modified. And if the current
 * state is non-cached, we need to do a cache fill first similar to
 * the situation when the reigion is overwritten.
 * Precondition: the cache header lock must have been taken.
 * @param file[in] the CBM file
 * @param cacheinfo[in] the cacheinfo
 * @param new_file_size[in] the new file size.
 * @param error_out[out] error if any
 * @return indicate if the file size will be changed due to the truncation.
 */
static void
isi_cbm_file_cache_last_region(struct isi_cbm_file *file,
							   cpool_cache *cacheinfo, off_t new_file_size,
							   struct isi_error **error_out)
{
	isi_error *error = NULL;
	size_t regionsize = cacheinfo->get_regionsize();
	cpool_cache_header header = cacheinfo->get_header();
	off_t new_region = new_file_size / regionsize;
	off_t region_to_fix_up = MIN(new_region, header.last_region);
	bool region_locked = false;
	std::vector<char> fillbuf;
	enum isi_cbm_cache_status status;

	fillbuf.resize(regionsize);

	ASSERT(new_file_size >= 0);
	ASSERT(new_region >= 0);
	if (header.last_region < 0)
	{
		/* There are no regions to cache! */
		goto out;
	}

	cacheinfo->cache_lock_region(region_to_fix_up, true, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "Could not lock region %ld", region_to_fix_up);

	region_locked = true;

	cacheinfo->cache_info_read(region_to_fix_up, &status, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (status != ISI_CPOOL_CACHE_NOTCACHED)
	{
		goto out;
	}

	ilog(IL_TRACE, "Cache miss on truncation for region %ld "
				   "for %{}",
		 region_to_fix_up, isi_cbm_file_fmt(file));
	isi_cbm_cache_fill_region(file, region_to_fix_up,
							  fillbuf.data(), regionsize, CO_CACHE, false, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	if (region_locked)
	{
		cacheinfo->cache_unlock_region(region_to_fix_up,
									   isi_error_suppress());
		region_locked = false;
	}
	isi_error_handle(error, error_out);
	return;
}

void isi_cbm_file_ftruncate(struct isi_cbm_file *file_obj, off_t offset,
							struct isi_error **error_out)
{
	isi_error *error = NULL;
	bool ch_locked = false;
	/* set keep locked to true so unless there is an error
	 * we will hold the lock on exit.
	 */
	isi_cbm_file_open_cache_lock_excl(file_obj, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to open cache for %{}",
					  isi_cbm_file_fmt(file_obj));

	ch_locked = true;

	isi_cbm_file_ftruncate_base(file_obj, offset, error_out);

out:
	if (ch_locked)
	{
		file_obj->cacheinfo->cacheheader_unlock(isi_error_suppress());
	}
	isi_error_handle(error, error_out);
}

void isi_cbm_file_ftruncate_base(struct isi_cbm_file *file_obj, off_t offset,
								 struct isi_error **error_out)
{
	/*
	 * Need to both truncate the data file and perform a truncate on the
	 * cacheinfo as well.
	 */
	struct isi_error *error = NULL;

	ASSERT(file_obj && file_obj->fd != -1);

	ASSERT(file_obj->cacheinfo->cacheheader_lock_type_is_exclusive());

	file_obj->cacheinfo->set_state(ISI_CBM_CACHE_FLAG_TRUNCATE, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "failed to set truncate state for cache header for "
							  "%{}",
							  isi_cbm_file_fmt(file_obj));
		goto out;
	}

	UFAIL_POINT_CODE(cbm_file_ftruncate_fail_1, {
		error = isi_system_error_new(EINVAL,
									 "Generated by failpoint after lock: LIN %{}",
									 lin_fmt(file_obj->cacheinfo->get_header().lin));
	});
	if (error)
	{
		isi_error_add_context(error,
							  "Failpoint error for %{}",
							  isi_cbm_file_fmt(file_obj));
		goto out;
	}

	isi_cbm_file_queue_daemon_internal(file_obj, ISI_CBM_CACHE_FLAG_DIRTY,
									   &error);
	ON_ISI_ERROR_GOTO(out, error);

	isi_cbm_file_ftruncate_restart(file_obj, offset,
								   file_obj->cacheinfo, &error);
out:
	isi_error_handle(error, error_out);
}

static int
cache_truncate_extend_wrap(cpool_cache *cacheinfo, off_t offset,
						   size_t curr_file_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int state = ISI_CBM_CACHE_FLAG_NONE;

	state = cacheinfo->cache_truncate_extend(
		(size_t)offset, curr_file_size, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "Could not truncate cacheinfo for lin %{}",
							  lin_fmt(cacheinfo->get_header().lin));
		goto out;
	}
	ilog(IL_DEBUG,
		 "Truncated cache header for lin %{} to size %lld",
		 lin_fmt(cacheinfo->get_header().lin), offset);

	UFAIL_POINT_CODE(cbm_file_ftruncate_fail_2, {
		error = isi_system_error_new(EINVAL,
									 "Generated by failpoint "
									 "after cache_truncate: LIN %{}",
									 lin_fmt(cacheinfo->get_header().lin));
	});

out:
	isi_error_handle(error, error_out);
	return state;
}

static void
file_truncate_extend_wrap(struct isi_cbm_file *file_obj, off_t offset,
						  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = file_obj->cacheinfo;

	if (ftruncate(file_obj->fd, offset) == -1)
	{
		error = isi_system_error_new(errno,
									 "Unable to truncate lin %{} to %lld",
									 lin_fmt(cacheinfo->get_header().lin),
									 offset);
		goto out;
	}
	ilog(IL_DEBUG, "Truncated lin %{} to size %lld",
		 lin_fmt(cacheinfo->get_header().lin), offset);

	UFAIL_POINT_CODE(cbm_file_ftruncate_fail_3, {
		error = isi_system_error_new(EINVAL,
									 "Generated by failpoint "
									 "after ftruncate: LIN %{}",
									 lin_fmt(cacheinfo->get_header().lin));
	});
out:
	isi_error_handle(error, error_out);
}

void isi_cbm_file_ftruncate_restart(struct isi_cbm_file *file_obj, off_t offset,
									cpool_cache *cacheinfo, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int state = ISI_CBM_CACHE_FLAG_NONE;
	size_t curr_file_size = 0;

	ASSERT(file_obj == cacheinfo->get_file_obj());
	ASSERT(file_obj->cacheinfo->cacheheader_lock_type_is_exclusive());

	/*
	 * Make sure the new last (partial) region is cached
	 */
	isi_cbm_file_cache_last_region(file_obj, cacheinfo,
								   offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * Truncate the cache header first and then the cached data.
	 * Catch any state flags that get set during this operation so
	 * they can be cleared later.
	 */
	curr_file_size = isi_cbm_file_get_filesize(file_obj, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (curr_file_size < offset)
	{
		/*
		 * If we are truncating up, then perform the
		 * cache operation first followed by the file.
		 * The cache extend will mark any new regions
		 * as dirty.
		 */
		state = cache_truncate_extend_wrap(cacheinfo, offset,
										   curr_file_size, &error);
		ON_ISI_ERROR_GOTO(out, error);
		file_truncate_extend_wrap(file_obj, offset, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	else
	{
		/*
		 * Otherwise shrink the file first.
		 * We have already made sure that the new last region
		 * is cached.  If we die and have to restart, the file
		 * may already be the new size so we just have
		 * to adjust the cache. The lock call that gets us
		 * here in that case will use the current file size
		 * as the new size.
		 */
		file_truncate_extend_wrap(file_obj, offset, &error);
		ON_ISI_ERROR_GOTO(out, error);
		state = cache_truncate_extend_wrap(cacheinfo, offset,
										   curr_file_size, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	/*
	 * If the truncation succeeded, then clear the state in the
	 * cacheheader to show successful operation.
	 */
	if (state != ISI_CBM_CACHE_FLAG_NONE)
	{
		cacheinfo->clear_state(state, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Unable to clear cache truncation state, lin %{}",
								  lin_fmt(cacheinfo->get_header().lin));
			goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}

/**
 * Get the caching option from the policy. This function anticipates
 * the error when the policy is not available. In that case, it uses
 * the default options.
 * @param file_obj[in] the cloud pool policy
 * @param cache_opt[in/out] the cache option
 * @param cache_full[out] if to do full cache
 * @param cloud_io_allowed[out] if cloud IO is allowed.
 */
void isi_cbm_file_get_caching_opt(struct isi_cbm_file *file,
								  isi_cbm_io_cache_option &cache_opt, bool &cache_full,
								  bool cloud_io_allowed,
								  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_id policy_id;

	struct fp_policy *cp_policy = NULL;
	const isi_cfm_mapinfo &mapinfo = *file->mapinfo;
	isi_cbm_io_cache_option cache_pol_op = CO_CACHE;

	cache_full = false;		 // default no full cache
	cloud_io_allowed = true; // default cloud io allowed

	scoped_ppi_reader ppi_rdr;

	const isi_cfm_policy_provider *cbm_cfg = ppi_rdr.get_ppi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	policy_id = mapinfo.get_policy_id();

	cp_policy = get_policy_by_id(policy_id,
								 (isi_cfm_policy_provider *)cbm_cfg, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (cp_policy)
	{
		fp_cloudpool_action *cl_opt =
			cp_policy->attributes->cloudpool_action;
		fp_attr_cp_caching_strategy cache_pol =
			cl_opt->cloud_caching_strategy;
		fp_attr_cp_caching_readahead_strategy rah =
			cl_opt->cloud_readahead_strategy;
		if (cache_pol == FP_A_CP_CACHING_LOCAL)
			cache_full = (rah == FP_A_CP_CACHING_RA_FULL);
		else
			cache_pol_op = CO_NOCACHE;
		if (cache_pol == FP_A_CP_CACHING_NOINLINE)
			cloud_io_allowed = false;
	}

	if (cache_opt == CO_DEFAULT)
		cache_opt = cache_pol_op;
out:
	isi_error_handle(error, error_out);
}

void isi_cbm_file_queue_full_cache(struct isi_cbm_file *file,
								   struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	djob_id_t djob_id = DJOB_RID_INVALID;

	if (file->cache_full_queued)
		goto out;

	UFAIL_POINT_CODE(isi_cbm_file_queue_full_cache_fail, {
		ilog(IL_DEBUG,
			 "Failpoint forcing EBUSY for %{}",
			 isi_cbm_file_fmt(file));
		error = isi_system_error_new(EBUSY,
									 "too many concurrent daemon jobs (failpoint)");
	});

	ON_ISI_ERROR_GOTO(out, error);

	// TBD-must: cache the context, it is heavy
	recall_file(&djob_id, file->lin, file->snap_id, NULL,
				ISI_CPOOL_D_PRI_HIGH, true, true, &error);

	if (error && !isi_system_error_is_a(error, EEXIST))
	{
		// unkown error
		ON_ISI_ERROR_GOTO(out, error);
	}
	if (error)
	{
		isi_error_free(error);
		error = NULL;
	}
	file->cache_full_queued = true;
out:
	isi_error_handle(error, error_out);
}

/*
 * Read data from the file
 * If the cache is locked it must be locked in exclusive mode.
 * The lock will be in the same state on return.
 */
#define MAX_CLOUD_READ_RETRY (5)
ssize_t
isi_cbm_file_read_with_retry(struct isi_cbm_file *file_obj, void *buf,
							 uint64_t offset, size_t length, enum isi_cbm_io_cache_option cache_opt,
							 bool do_cache_read, bool ch_lock_held,
							 struct isi_error **error_out)
{
	ssize_t this_read = 0;
	size_t total_read = 0;
	int retry_count = 0;
	struct isi_error *error = NULL;
	databuffer_ostream buf_strm(NULL, 0);

	ASSERT(file_obj->cacheinfo != NULL);

	while ((total_read < length) && (!error))
	{
		char *buf1 = (char *)buf + total_read;
		size_t length1 = length - total_read;
		uint64_t offset1 = offset + total_read;
		/*
		 * Need to refine this case a bit more. Probably should be able
		 * to narrow this down to case of when region is not cached
		 * rather than whole file....
		 * Also need to add policy check as well (from ppi)
		 */
		////cache code
		// if (do_cache_read && (file_obj->isi_cbm_file_is_cached())) {
		// 	this_read = isi_cbm_file_cache_read(file_obj,
		// 	    offset1, length1, (char *)buf1, cache_opt,
		// 	    ch_lock_held, &error);

		// 	if (error && (isi_system_error_is_a(error, EROFS) ||
		// 	    isi_system_error_is_a(error, ENOENT) ||
		// 	    isi_system_error_is_a(error, EMSGSIZE) ||
		// 	    isi_cbm_error_is_a(error, CBM_CACHE_NO_HEADER))) {
		// 		ASSERT(this_read <= 0, "Read : %ld",
		// 		    this_read);
		// 		ilog(IL_DEBUG, "Could not do cached read for "
		// 		    "%{}, offset %lu, length %ld. Switch to "
		// 		    "non cached mode. Error: %#{}",
		// 		    isi_cbm_file_fmt(file_obj), offset1,
		// 		    length1, isi_error_fmt(error));
		// 		isi_error_free(error);
		// 		error = NULL;
		// 		file_obj->isi_cbm_set_file_NOT_cached();
		// 		continue;
		// 	}
		// } else {
		buf_strm.reset(buf1, length1);
		this_read = isi_cbm_file_fetch_range(file_obj,
											 &buf_strm, offset1, length1, &error);
		if (error)
		{
			ilog(IL_DEBUG, "Uncached read for %{}, "
						   "offset %lu, length %ld, "
						   "data read %ld, retry %d: "
						   "Error: %#{}",
				 isi_cbm_file_fmt(file_obj),
				 offset1, length1,
				 this_read, retry_count,
				 isi_error_fmt(error));
			if ((retry_count < MAX_CLOUD_READ_RETRY) &&
				isi_cbm_error_is_a(
					error, CBM_CLAPI_PARTIAL_FILE))
			{
				isi_error_free(error);
				error = NULL;
				retry_count++;
				continue;
			}
		}
		else
		{
			retry_count = 0;
			ilog(IL_TRACE, "Uncached read for %{}, "
						   "offset %lu, length %ld, data read %ld: "
						   "Success",
				 isi_cbm_file_fmt(file_obj),
				 offset1, length1, this_read);
		}
		ASSERT((this_read >= 0) || error);
		if ((this_read > 0) && !error)
		{
			total_read += this_read;
		}
		else if ((this_read == 0) && !error)
		{
			// End of file found
			break;
		}
	}
	if (this_read >= 0 && !error)
	{
		ASSERT(total_read == length || this_read == 0);
	}

	isi_error_handle(error, error_out);

	return total_read;
}

/*
 * If the cache is locked it must be locked in exclusive mode.
 * The lock will be in the same state on return.
 */
ssize_t
isi_cbm_file_read_extended(struct isi_cbm_file *file_obj, void *buf,
						   uint64_t offset, size_t length, enum isi_cbm_io_cache_option cache_opt,
						   bool ch_lock_held, struct isi_error **error_out)
{
	ssize_t bytes_read = -1;
	struct isi_error *error = NULL;
	ssize_t total_read = 0;
	bool cache_full = false;
	bool allow_cloud_io = true;
	size_t filesize = 0;

	ASSERT(file_obj && buf && !(*error_out));
	ASSERT(file_obj->cacheinfo != NULL);
	/*
	 * If we are already locked
	 * we either need the cache open and initialized
	 * or be in read only mode
	 */
	ASSERT((!ch_lock_held) ||
		   (file_obj->cacheinfo->is_read_only()) ||
		   (file_obj->cacheinfo->is_cache_open()));

	if (length == 0)
	{
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "invalid length: length %lld, offset %lld",
								  length, offset);
		ON_ISI_ERROR_GOTO(out, error);
	}

	isi_cbm_file_get_caching_opt(file_obj, cache_opt, cache_full,
								 allow_cloud_io, &error);
	if (error)
	{
		ilog(IL_ERR, "Unable to get cache option for %{}, will "
					 "proceed with default options. Error: %#{}",
			 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}

	/* check retention when cache is on
	 * set cache_opt to be read only when file is under retention
	 * so that we do not to overwrite the cache. Do this only
	 * if the file is not already marked read-only.
	 */
	if (file_obj->isi_cbm_file_is_cached() &&
		!(file_obj->cacheinfo->is_read_only()))
	{

		bool is_committed = is_cbm_file_worm_committed(
			file_obj->lin, file_obj->snap_id, &error);

		ON_ISI_ERROR_GOTO(out, error, "Failed WORM check for %{}",
						  isi_cbm_file_fmt(file_obj));

		if (is_committed)
			file_obj->cacheinfo->set_read_only(true);
	}

	if ((cache_opt != CO_NOCACHE) && (cache_full))
	{
		isi_cbm_file_queue_full_cache(file_obj, &error);

		// we shouldn't fail the read because of an error doing the full cache
		if (error)
		{
			ilog(IL_ERR, "Unable to queue full cache of %{}, "
						 "Error: %#{}",
				 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
			isi_error_free(error);
			error = NULL;
		}
	}

	filesize = isi_cbm_file_get_filesize(file_obj, &error);
	ON_ISI_ERROR_GOTO(out, error);

	total_read = isi_cbm_file_read_with_retry(file_obj, buf, offset,
											  length, cache_opt, true, ch_lock_held, &error);

	if ((total_read >= 0) && !error)
	{
		bytes_read = total_read;
	}
	// else error and bytes_read remains -1

out:
	if (error)
		isi_error_handle(error, error_out);

	return bytes_read;
}

ssize_t
isi_cbm_file_read(struct isi_cbm_file *file_obj, void *buf,
				  uint64_t offset, size_t length, enum isi_cbm_io_cache_option cache_opt,
				  struct isi_error **error_out)
{
	return isi_cbm_file_read_extended(file_obj, buf, offset, length,
									  cache_opt, false, error_out);
}

/*
 * Fill the cache region containing a given offset
 */
void isi_cbm_cache_fill_offset_region(struct isi_cbm_file *file_obj,
									  off_t offset, char *fillbuf, size_t fillbuflen,
									  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache_header header;
	enum isi_cbm_cache_status status;

	bool is_committed = false;
	bool ch_locked = false;
	bool region_locked = false;
	cpool_cache *cacheinfo = NULL;
	size_t regionsize = 0;
	ssize_t data_fetched = 0;
	off_t region = -1;

	if (!file_obj->isi_cbm_file_is_cached())
	{
		/**
		 * The file is not cached, there is nothing to do
		 */
		goto out;
	}

	if (!file_obj->cacheinfo)
	{
		file_obj->cacheinfo = new cpool_cache(file_obj);
	}
	cacheinfo = file_obj->cacheinfo;

	regionsize = cacheinfo->get_regionsize();
	if (!fillbuf || (fillbuflen < regionsize))
	{
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "Invalid buffer paramater for %{}",
								  isi_cbm_file_fmt(file_obj));
		goto out;
	}

	is_committed =
		is_cbm_file_worm_committed(file_obj->lin, file_obj->snap_id, &error);

	ON_ISI_ERROR_GOTO(out, error, "Failed WORM check for %{}",
					  isi_cbm_file_fmt(file_obj));

	if (is_committed)
	{
		error = isi_cbm_error_new(CBM_READ_ONLY_ERROR,
								  "Cannot recall file %{} that is WORM committed",
								  isi_cbm_file_fmt(file_obj));
		goto out;
	}

	cacheinfo->cacheheader_lock(false, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to lock header for %{}",
					  isi_cbm_file_fmt(file_obj));

	ch_locked = true;

	region = offset / regionsize;
	header = cacheinfo->get_header();

	/*
	 * If attempting to read before the start or end of the file,
	 * read 0 bytes
	 */
	if (!cacheinfo->is_valid_region(region))
	{
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "Could not fill region %ld, offset %ld",
								  region, offset);
		goto out;
	}

	cacheinfo->cache_lock_region(region, true, &error);
	ON_ISI_ERROR_GOTO(out, error, "Could not lock region %ld", region);

	region_locked = true;

	cacheinfo->cache_info_read(region, &status, &error);
	ON_ISI_ERROR_GOTO(out, error,
					  "Could not read info for region %ld", region);

	if (status != ISI_CPOOL_CACHE_NOTCACHED)
	{
		/*
		 * Someone may have squeeked in and cached this region for us
		 * before we took the cache lock.  If so, there is nothing
		 * to do
		 */
		if (status == ISI_CPOOL_CACHE_INVALID)
		{
			error = isi_cbm_error_new(CBM_CACHEINFO_ERROR,
									  "Could not fill region %ld, offset %ld, "
									  "cache invalid",
									  region, offset);
		}
		goto out;
	}

	data_fetched = isi_cbm_cache_fill_region(file_obj, region, fillbuf,
											 regionsize, CO_CACHE, false, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "Could not fill region %ld", region);
		goto out;
	}

out:
	if (region_locked)
	{
		cacheinfo->cache_unlock_region(region, isi_error_suppress());
		region_locked = false;
	}

	/*
	 * NB: ch_locked will be set ONLY if the cacheheader was taken out in
	 * this routine.  If the input parameters state that the lock was
	 * already held, then there will be no attempt to release the
	 * cacheheader lock here.
	 */
	if (ch_locked)
	{
		cacheinfo->cacheheader_unlock(isi_error_suppress());
		ch_locked = false;
	}

	isi_error_handle(error, error_out);
}

/*
 * Write data to the file object
 */
ssize_t
isi_cbm_file_write(struct isi_cbm_file *file_obj, const void *buf,
				   uint64_t offset, size_t length, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(file_obj && buf && !(*error_out));

	ssize_t this_wrt = 0;
	size_t total_wrt = 0;

	// check rentention. Is it too expensive to check each time??
	bool is_committed = is_cbm_file_worm_committed(
		file_obj->lin, file_obj->snap_id, &error);

	ON_ISI_ERROR_GOTO(out, error, "Failed WORM check for %{}",
					  isi_cbm_file_fmt(file_obj));

	if (is_committed)
	{
		error = isi_cbm_error_new(CBM_PERM_ERROR,
								  "File %{} is WORM committed", isi_cbm_file_fmt(file_obj));
		goto out;
	}

	if (length == 0)
	{
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "invalid arguments");
		ON_ISI_ERROR_GOTO(out, error);
	}

	ilog(IL_DEBUG, "Write requested of %ld bytes at offset %lu "
				   "for %{} (%p)",
		 length, offset, isi_cbm_file_fmt(file_obj), file_obj);

	isi_cbm_file_update_mtime(file_obj, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to update mtime");

	while (total_wrt < length)
	{
		char *buf1 = (char *)buf + total_wrt;
		size_t length1 = length - total_wrt;
		uint64_t offset1 = offset + total_wrt;

		this_wrt = isi_cbm_file_cache_write(file_obj, offset1,
											length1, buf1, &error);

		ASSERT(this_wrt >= 0 || error);

		if (this_wrt > 0 && !error)
			total_wrt += this_wrt;
		else
			break;
	}
	ASSERT(total_wrt == length || error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (error)
	{
		isi_error_handle(error, error_out);
		return -1;
	}

	ilog(IL_DEBUG, "Requested %ld bytes written at offset %lu "
				   "for %{} (%p), returning %ld",
		 length, offset, isi_cbm_file_fmt(file_obj), file_obj, total_wrt);
	return total_wrt;
}

int isi_cbm_file_flush(struct isi_cbm_file *file_obj, off_t offset, size_t length,
					   struct isi_error **error_out)
{
	ASSERT(file_obj && !(*error_out));
	int result;
	struct isi_error *error = NULL;

	ilog(IL_DEBUG, "Flush to local storage requested for file %{},"
				   " offset %ld, length %ld",
		 isi_cbm_file_fmt(file_obj), offset,
		 length);

	result = length > 0 ? fsync_range(file_obj->fd, FDATASYNC, offset, length) : fsync(file_obj->fd);

	if (result < 0)
	{
		error = isi_system_error_new(errno,
									 "flush failed for cbm file");

		ilog(IL_ERR, "failed to flush file to local storage"
					 " for %{}, offset %ld, length %ld",
			 isi_cbm_file_fmt(file_obj), offset, length);
	}

	if (error)
	{
		isi_error_handle(error, error_out);
	}

	return result;
}

/*
 * Close the file object and release associated resource
 * file_obj cannot be used any more to access file
 */
void isi_cbm_file_close(struct isi_cbm_file *file_obj, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ASSERT(file_obj);

	ilog(IL_DEBUG, "close called for lin %{}, snapid %{} (%p)",
		 lin_fmt(file_obj->lin), snapid_fmt(file_obj->snap_id), file_obj);

	if (file_obj->fd >= 0)
	{
		if (close(file_obj->fd) < 0)
		{
			error = isi_system_error_new(errno,
										 "close file failed for fd %d, "
										 "lin %{} snapid %{}",
										 file_obj->fd, lin_fmt(file_obj->lin),
										 snapid_fmt(file_obj->snap_id));
			isi_error_handle(error, error_out);
		}
		file_obj->fd = -1;
	}

	if (file_obj->mapinfo)
	{
		delete file_obj->mapinfo;
		file_obj->mapinfo = NULL;
	}

	if (file_obj->cacheinfo)
	{
		delete file_obj->cacheinfo;
		file_obj->cacheinfo = NULL;
	}

	delete file_obj;
}

void isi_cph_get_stubmap(int fd, isi_cfm_mapinfo &mapinfo,
						 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int status = -1;
	std::vector<char> buf(ISI_CFM_MAP_EXTATTR_THRESHOLD);
	size_t stub_map_size = 0;
	size_t stub_data_size = ISI_CFM_MAP_EXTATTR_THRESHOLD;

	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ, 0,
								&stub_map_size, buf.data(), &stub_data_size);
	if (status < 0)
	{
		error = isi_system_error_new(errno,
									 "Fetching stub map from EA");
		goto out;
	}
	mapinfo.unpack(buf.data(), stub_map_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (mapinfo.is_overflow())
	{
		mapinfo.open_store(true, false, NULL, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);
}

/**
 * Dump the map associated with a file specified by its fd.
 */
void isi_cbm_dump_map(int fd, bool dump_map, bool dump_paths,
					  struct isi_error **error_out)
{
	isi_cfm_mapinfo mapinfo;
	struct isi_error *error = NULL;

	isi_cph_get_stubmap(fd, mapinfo, &error);
	if (error)
		goto out;

	mapinfo.dump(dump_map, dump_paths);

out:
	if (error)
		isi_error_handle(error, error_out);
}

/**
 * Verify the map associated with a file specified by its fd.
 */
void isi_cbm_verify_map(int fd, struct isi_error **error_out)
{
	isi_cfm_mapinfo mapinfo;
	struct isi_error *error = NULL;

	isi_cph_get_stubmap(fd, mapinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mapinfo.verify_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
}

struct fmt_conv_ctx
isi_cbm_file_fmt(const struct isi_cbm_file *file)
{
	struct fmt_conv_ctx fmt_ctx;
	fmt_ctx.func = isi_cbm_file_fmt_conv;
	fmt_ctx.arg.ptr = file;
	return fmt_ctx;
}

/*
 * Test to make sure the region is cached, if not fill the cache
 * and return the data in a buffer for possible use by the caller.
 *
 * NOTE: The region must be locked on entry, if it is determined that
 * the lock is needed in exclusive mode a CBM_NEED_LOCK_UPGRADE error
 * will be returned to signal to the caller that the lock should be
 * converted to exclusive mode and the operation restarted.
 *
 * NOTE: If fillbuf is null, and we need a fill we will allocate the buffer.
 * The caller may then reuse the same buffer for subsequent calls, but it
 * is up to the user to free the buffer when finished.
 */
ssize_t
isi_cbm_cache_test_and_fill_region(
	struct isi_cbm_file *file_obj,
	off_t region,
	char **fillbuf,
	size_t &fillbufsize,
	enum isi_cbm_io_cache_option cache_opt,
	bool exclusive_lock,
	bool mark_dirty,
	bool enforce_no_cache,
	struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = file_obj->cacheinfo;
	ssize_t data_fetched = -1;
	enum isi_cbm_cache_status status = ISI_CPOOL_CACHE_INVALID;
	size_t regionsize = 0;

	ilog(IL_TRACE, "Entered %s for %{}, region %lld, "
				   "exclusive lock %d, mark_dirty %d, enforce_no_cache %d",
		 __FUNCTION__, isi_cbm_file_fmt(file_obj),
		 region, exclusive_lock, mark_dirty, enforce_no_cache);

	if (!cacheinfo->is_cache_open())
	{
		regionsize = get_regionsize(file_obj);
	}
	else
	{
		regionsize = cacheinfo->get_regionsize();
	}

	ASSERT((fillbuf != NULL) &&
		   ((*fillbuf == NULL) || (fillbufsize >= regionsize)));
	if ((fillbuf == NULL) ||
		((*fillbuf != NULL) && (fillbufsize < regionsize)))
	{
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "Invalid buffer size");
		goto out;
	}

	cacheinfo->cache_info_read(region, &status, &error);
	if (error)
	{
		ilog(IL_DEBUG, "Unable to read cache info for %{}, "
					   "Error: %#{}",
			 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
		goto out;
	}

	ilog(IL_DEBUG, "%s: Checking current cache region status for lock "
				   "determination for %{}, region %ld, status %{}, mark_dirty: %s",
		 __FUNCTION__, isi_cbm_file_fmt(file_obj), region,
		 isi_cbm_cache_status_fmt(status), mark_dirty ? "True" : "False");

	if (((status == ISI_CPOOL_CACHE_NOTCACHED) ||
		 (mark_dirty && (status != ISI_CPOOL_CACHE_DIRTY))) &&
		(!exclusive_lock))
	{
		/*
		 * The cache lock must be converted to exclusive mode so
		 * that we only have one thread reading the data from
		 * the cloud ane we are protecting the status setting.
		 * The lock was passed down to us, so return a special
		 * error back up the stack to signal that the lock should
		 * be upgraded and the and we can continue.
		 */
		ilog(IL_DEBUG, "%s needs fill or status change "
					   "for region %ld %{}",
			 __FUNCTION__, region, isi_cbm_file_fmt(file_obj));

		error = isi_cbm_error_new(CBM_NEED_LOCK_UPGRADE,
								  "Cache range lock upgrade is needed");

		goto out;
	}

	/*
	 * Make sure we need a fill.
	 */
	if (status == ISI_CPOOL_CACHE_NOTCACHED)
	{
		if (*fillbuf == NULL)
		{
			*fillbuf = (char *)malloc(regionsize);
			fillbufsize = regionsize;
		}
		ASSERT(*fillbuf);
		ilog(IL_DEBUG, "%s Cache miss for region %ld %{} ",
			 __FUNCTION__, region, isi_cbm_file_fmt(file_obj));

		data_fetched = isi_cbm_cache_fill_region(
			file_obj,
			region,
			*fillbuf,
			regionsize,
			cache_opt,
			enforce_no_cache,
			&error);
		if (error)
		{
			ilog(IL_DEBUG, "Unable to fill region %ld for %{}, "
						   "after exclusive lock, Error: %#{}",
				 region,
				 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
			goto out;
		}
	}

	if (mark_dirty && (status != ISI_CPOOL_CACHE_DIRTY))
	{
		UFAIL_POINT_CODE(cbm_file_cache_write_info_eio, {
			error = isi_system_error_new(EIO,
										 "Generated by failpoint before setting cache dirty");
		});
		if (!error)
		{
			cacheinfo->cache_info_write(
				region, ISI_CPOOL_CACHE_DIRTY, &error);
		}
		if (error)
		{
			ilog(IL_DEBUG, "Unable to set region %lld dirty "
						   "for %{}, Error: %#{}",
				 region,
				 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
			goto out;
		}
	}

out:
	if (error)
	{
		isi_error_handle(error, error_out);
	}

	return data_fetched;
}

/*
 * Fetch data from the cloud and fill the corresponding region.   If we
 * successfully read from the cloud, this routine returns true even if the
 * write to the cache fails.  This allows for the data to be returned to the
 * user despite file system failures (quota exceeded, etc.)
 * NB: Cacheheader must be locked for at least read when this routine
 * is called.
 * NB: Region to be filled  must be locked exclusive when this routine
 * is called
 * If the return value is greater than 0 then we fetched data from the
 * cloud into databuf.  The error_out return will indicate if it was
 * successfully written to the cache.
 *
 */
ssize_t
isi_cbm_cache_fill_region(
	struct isi_cbm_file *file,
	off_t region,
	char *databuf,
	size_t datasize,
	enum isi_cbm_io_cache_option cache_opt,
	bool enforce_no_cache,
	struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = file->cacheinfo;
	isi_cfm_mapinfo *mapinfo = file->mapinfo;
	size_t regionsize = 0;
	ssize_t this_write = 0;
	off_t next_offset;
	off_t this_offset;
	bool data_fetched_from_cloud = false;
	ssize_t total_read = 0;
	size_t read_size = datasize;
	off_t last_map_region = 0;

	spa_header spa;
	struct iovec *vec = NULL;
	int vec_count = -1;

	int i;

	if (!cacheinfo)
	{
		error = isi_cbm_error_new(CBM_CACHEINFO_ERROR,
								  "No cache found or cacheinfo not open");
		goto out;
	}

	if (!cacheinfo->is_cache_open())
	{
		regionsize = get_regionsize(file);
	}
	else
	{
		regionsize = cacheinfo->get_regionsize();
	}

	ASSERT(datasize == regionsize);

	/*
	 * Check to see if we need to override the datasize
	 * passed in
	 */
	if (mapinfo->get_filesize() > 0)
	{
		last_map_region = (mapinfo->get_filesize() - 1) / regionsize;
		if ((last_map_region == region) &&
			(mapinfo->get_filesize() % regionsize))
		{
			// read the partial last region, but
			// not more than has been written to cloud
			read_size = mapinfo->get_filesize() % regionsize;
		}
		else if (last_map_region < region)
		{
			read_size = 0;
		}
	}
	else
	{
		read_size = 0;
	}
	if (read_size == 0)
	{
		goto out;
	}

	next_offset = region * regionsize;

	ilog(IL_TRACE, "Fetching %ld bytes from cloud at %ld "
				   "region %ld, last_map_region: %ld, map_filesize %ld "
				   "regionsize %ld, for %{}",
		 read_size, next_offset,
		 region, last_map_region, mapinfo->get_filesize(),
		 regionsize, isi_cbm_file_fmt(file));

	// read, but NOT from the cache
	total_read = isi_cbm_file_read_with_retry(file, databuf, next_offset,
											  read_size, CO_DEFAULT, false, false, &error);

	if (error)
	{
		goto out;
	}

	if (total_read < (ssize_t)read_size)
	{
		ilog(IL_DEBUG,
			 "Did not get all the requsted data from cloud "
			 "requested: %ld, got: %ld for %{}. "
			 "Will not mark as cached",
			 read_size, total_read, isi_cbm_file_fmt(file));
		error = isi_cbm_error_new(CBM_SIZE_MISMATCH,
								  "Fetched %lld bytes, requested %lld",
								  total_read, read_size);
		goto out;
	}

	data_fetched_from_cloud = true;

	if ((enforce_no_cache && (cache_opt == CO_NOCACHE)) ||
		cacheinfo->is_read_only())
	{
		ilog(IL_DEBUG, "Do not write cache for %{}",
			 isi_cbm_file_fmt(file));
		error = isi_cbm_error_new(CBM_READ_ONLY_ERROR,
								  "Read only node, cache not written");
		goto out;
	}

	UFAIL_POINT_CODE(cbm_file_cache_fill_write_nospace, {
		error = isi_system_error_new(ENOSPC,
									 "Generated by failpoint before writing region %ld",
									 region);
	});
	if (error)
	{
		isi_error_add_context(error,
							  "Failpoint error for %{}", isi_cbm_file_fmt(file));
		goto out;
	}

	/*
	 * Get the sparse map related to this range (CDO).  Hopefully this is
	 * still cached with the checksum information so that we are not
	 * forced to read from the cloud again...
	 */
	isi_cbm_get_sparse_map(file, next_offset, spa, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "Could not get sparse area header for %{}",
							  isi_cbm_file_fmt(file));
		goto out;
	}

	spa.trace_spa("Output data using spa", __func__, __LINE__);

	/*
	 * Note the /2 is the worst case number of iovec is when every other
	 * block is sparse and we only are writing the non-sparse blocks.
	 */
	vec_count = (spa.get_sparse_map_length() * BITS_PER_CHAR) / 2;
	if (vec_count == 0)
	{
		/*
		 * No sparse map length is 0 (no sparse map info) so do the
		 * whole databuf as a single vector
		 */
		vec = (struct iovec *)malloc(sizeof(struct iovec));
		vec->iov_base = databuf;
		vec->iov_len = read_size;
	}
	else
	{
		/*
		 * Only write the regions that where not marked as sparse.
		 * First get the space for the io vectors.
		 */
		vec = (struct iovec *)calloc(sizeof(struct iovec), vec_count);
		ASSERT(vec); // if we can't malloc we bail

		/*
		 * Next convert the sparse map information into an iovec for
		 * use in writing the non-sparse information.
		 */
		isi_cbm_sparse_map_to_iovec(file, spa,
									next_offset % mapinfo->get_chunksize(),
									read_size, databuf, mapinfo->get_chunksize(),
									vec, &vec_count, NULL, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Could not generate iovec from sparse area header"
								  "for %{}",
								  isi_cbm_file_fmt(file));
			goto out;
		}
	}

	ILOG_START(IL_TRACE)
	{
		fmt_print(ilogfmt,
				  "next_offset: %ld, iovec count: %d, iovec (base,len):",
				  next_offset, vec_count);
		for (i = 0; i < vec_count; i++)
		{
			fmt_print(ilogfmt, " (%p, %ld [0x%0lx])",
					  vec[i].iov_base, vec[i].iov_len,
					  vec[i].iov_len);
		}
	}
	ILOG_END;

	/*
	 * loop through the iov entries that were generated as these are the
	 * non-sparse areas of the file as defined in the sparse map retrieved
	 * with the CDO corresponding to the region requested.
	 */
	for (i = 0; i < vec_count; i++)
	{
		/* adjust offset for pwrite based on base address in iovec */
		this_offset = ((char *)(vec[i].iov_base) - databuf) +
					  next_offset;

		/* write this section */
		this_write = cache_pwritev(__func__, file->fd,
								   &vec[i], 1, this_offset, true,
								   (enforce_no_cache) ? O_ENFORCE_QUOTA : 0);

		if (this_write < (ssize_t)vec[i].iov_len)
		{
			error = isi_system_error_new(errno,
										 "Failed write to cache file for region %ld "
										 "offset %ld, length %ld, result %ld "
										 "with O_ENFORCE_QUOTA %s",
										 region, this_offset, vec[i].iov_len, this_write,
										 (enforce_no_cache) ? "SET" : "CLEAR");
			goto out;
		}
	}

	UFAIL_POINT_CODE(cbm_file_cache_fill_write_info_eio, {
		error = isi_system_error_new(EIO,
									 "Generated by failpoint before writing cache info");
	});
	if (error)
	{
		isi_error_add_context(error,
							  "Failpoint error for %{}", isi_cbm_file_fmt(file));
		goto out;
	}

	cacheinfo->cache_info_write(region, ISI_CPOOL_CACHE_CACHED, &error);
	if (error)
		goto out;

	ilog(IL_TRACE, "Cached region %ld for %{}",
		 region, isi_cbm_file_fmt(file));

out:
	if (vec)
	{
		free(vec);
		vec = NULL;
	}

	if (error)
	{
		isi_error_handle(error, error_out);
	}

	// if data read from cloud failed, returns -1
	return (data_fetched_from_cloud) ? total_read : -1;
}

/*
 * If the cache is locked it must be locked in exclusive mode.
 * The lock will be in the same state on return.
 */
static ssize_t
isi_cbm_file_cache_read(struct isi_cbm_file *file_obj,
						off_t offset, size_t length,
						void *databuf, enum isi_cbm_io_cache_option cache_opt,
						bool ch_lock_held, struct isi_error **error_out)
{
	ASSERT(file_obj);

	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = NULL;
	ssize_t bytes_done = 0;
	size_t iosize = 0;
	off_t next_offset = 0;
	ssize_t this_io = -1;

	size_t regionsize = 0;
	off_t region = -1;
	off_t read_offset = -1;
	ssize_t read_size = 0;

	char *fillbuf = NULL;
	size_t fillbufsize = 0;
	char *pdata = (char *)databuf;
	ssize_t data_fetched = -1;
	bool ch_locked = ch_lock_held;
	bool region_locked = false;
	bool update_cached_flag = false;
	size_t region_actual = 0;
	off_t filesize;

	ASSERT(file_obj->cacheinfo != NULL);
	cacheinfo = file_obj->cacheinfo;

	if (!ch_locked)
	{
		// open/lock the cache if it is not already locked.
		isi_cbm_file_open_and_or_init_cache(file_obj,
											true, true, &error);
		ON_ISI_ERROR_GOTO(out, error);
		ch_locked = true;
	}
	else
	{
		// If it is already locked, then it must be locked exclusive
		// in case we need to update the cached flag, we cannot
		// break synchronization to upgrade it.
		// The header is already locked, but if it is not already
		// open we cannot open it now or we may cause a deadlock
		// due to ordering of the file lock and the open mutex
		ASSERT(cacheinfo->cacheheader_lock_type_is_exclusive() ||
			   cacheinfo->is_read_only());
	}

	if (cacheinfo->is_fully_cached())
	{
		// the data is fully cached, read from the file and return
		// immediately
		bytes_done = pread(file_obj->fd, databuf, length, offset);

		if (bytes_done < 0)
		{
			error = isi_system_error_new(errno,
										 "Failed read from cache file for "
										 "file offset %ld, length %ld",
										 offset, length);
		}
		goto out;
	}

	/*
	 * Need to check and possible set the file's CACHED flag in the
	 * header.  We only do this if it is not a readonly file and if the
	 * flag is not already set.  If the flag does need to be set, we need
	 * to take the lock out exclusive during that time and then downgrade
	 * after the update.
	 */
	if (!cacheinfo->is_read_only())
	{
		update_cached_flag =
			!cacheinfo->test_state(ISI_CBM_CACHE_FLAG_CACHED, &error);
		if (error)
			goto out;
	}

	if (update_cached_flag)
	{
		if (!cacheinfo->cacheheader_lock_type_is_exclusive())
		{
			cacheinfo->cacheheader_unlock(&error);
			if (error)
				goto out;
			ch_locked = false;

			cacheinfo->cacheheader_lock(true, &error);
			if (error)
				goto out;
			ch_locked = true;
		}

		/*
		 * Queue the invalidation job for this file and mark file as
		 * cached in header....
		 */
		isi_cbm_file_queue_daemon_internal(file_obj,
										   ISI_CBM_CACHE_FLAG_CACHED, &error);
		if (error)
			goto out;
	}

	/*
	 * How did we get to cache_read with the cache not open?  If we have
	 * an IOVEC read on a RO file system that does not have a cache
	 * created yet, we cannot open/create it.  The code falls back to a
	 * buffered read which goes through the normal read path, but with the
	 * cache not open.  When we find the region not cached we read it from
	 * the cloud for the fill, but we have no cache so we return the data
	 * directly from the cloud.
	 */
	if (!cacheinfo->is_cache_open())
	{
		regionsize = get_regionsize(file_obj);
	}
	else
	{
		regionsize = cacheinfo->get_regionsize();
	}

	region = offset / regionsize;

	/*
	 * adjusted read_offset and read_size to match the the region
	 * boundary.
	 */
	read_offset = offset % regionsize;
	// the last region might be full-sized, reflect that:

	filesize = isi_cbm_file_get_filesize(file_obj, &error);
	ON_ISI_ERROR_GOTO(out, error);

	region_actual =
		MIN(filesize - (region * regionsize), regionsize);

	if (region_actual >= (read_offset + length))
	{
		// The entire read fits within the region, we will not need to
		// overflow to the next region.
		read_size = length;
	}
	else
	{
		if (offset < filesize)
		{
			// Just read to the end of the region
			read_size = region_actual - read_offset;
		}
		else
		{
			//
			// We are starting the read past EOF, so
			// there is no sense in issuing the actual read
			// call and incurring the overhead, simply
			// return success and a zero length read.
			// At one point we computed the length as negative
			// here and ended up returning an invalid param error.
			// No error and zero length read is the correct
			// response.
			//
			goto out;
		}
	}

	if (!cacheinfo->is_cache_open())
	{
		/*
		 * If attempting to read before the start or
		 * after end of the file read 0 bytes
		 */
		if ((region < 0) || ((offset + read_size) > filesize))
		{
			goto out;
		}

		fillbuf = (char *)malloc(regionsize);
		fillbufsize = regionsize;
		ASSERT(fillbuf);
		ilog(IL_DEBUG, "%s Cloud read, no cache, for region %ld %{} ",
			 __FUNCTION__, region, isi_cbm_file_fmt(file_obj));

		data_fetched = isi_cbm_cache_fill_region(
			file_obj,
			region,
			fillbuf,
			regionsize,
			cache_opt,
			true,
			&error);
	}
	else
	{
		bool locked_excl = false;
		/*
		 * If attempting to read before the start or end of the file,
		 * read 0 bytes
		 */
		if (!cacheinfo->is_valid_region(region))
		{
			goto out;
		}
	retry:
		cacheinfo->cache_lock_region(region, locked_excl, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Cache read cannot get region %lld lock in %s mode",
								  region, (locked_excl) ? "exclusive" : "shared");
			goto out;
		}
		region_locked = true;

		data_fetched = isi_cbm_cache_test_and_fill_region(
			file_obj, region, &fillbuf, fillbufsize, cache_opt,
			locked_excl, false, true, &error);

		if ((error) &&
			(!locked_excl) &&
			isi_cbm_error_is_a(error, CBM_NEED_LOCK_UPGRADE))
		{
			isi_error_free(error);
			error = NULL;
			cacheinfo->cache_unlock_region(
				region, &error);
			if (error)
			{
				isi_error_add_context(error,
									  "Cache unlock region %lld from %s mode",
									  region,
									  (locked_excl) ? "exclusive" : "shared");
				goto out;
			}
			region_locked = false;
			locked_excl = true;
			goto retry;
		}
		if (region_locked && locked_excl)
		{
			/*
			 * We do not need the exclusive
			 * lock any more, so downgrade
			 */
			cacheinfo->cache_downgrade_region_to_shlock(
				region, isi_error_suppress(IL_ERR));
		}
	}

	if (data_fetched < 0)
	{
		if (error)
		{
			ilog(IL_DEBUG,
				 "Unable to fill region %ld  %{}, "
				 "Error: %#{}",
				 region, isi_cbm_file_fmt(file_obj),
				 isi_error_fmt(error));
			isi_error_add_context(error,
								  "Unable to fill for region %ld  %{}, "
								  "Error: %#{}",
								  region, isi_cbm_file_fmt(file_obj),
								  isi_error_fmt(error));
			goto out;
		}
		else
		{
			ilog(IL_DEBUG,
				 "Cache test and fill for region %ld  %{}, "
				 "no data read from the cloud",
				 region, isi_cbm_file_fmt(file_obj));
		}
	}
	else
	{
		// We got the data from the cloud so we do
		// not need the error
		if (error)
		{
			ilog(IL_DEBUG,
				 "Cache miss for region %ld  %{}, "
				 "data returned but NOT written to cache",
				 region, isi_cbm_file_fmt(file_obj));
			isi_error_free(error);
			error = NULL;
		}
	}

	if (data_fetched > 0)
	{
		/*
		 * we got the data from the cloud, so just return
		 * it even it it wasn't written to the cache
		 */
		bytes_done = MIN(read_size, (data_fetched > read_offset) ? (data_fetched - read_offset) : 0);

		if (bytes_done > 0)
			memcpy(databuf, (void *)&fillbuf[read_offset],
				   bytes_done);

		ilog(IL_TRACE, "Cache miss on read for region %ld for %{}",
			 region, isi_cbm_file_fmt(file_obj));
	}
	else
	{
		/*
		 * read for the already cached file, using the already
		 * adjusted read size (to the end of the region) and the
		 * true offset (offset in cache is same as offset in file).
		 */
		for (this_io = 1, iosize = read_size,
			bytes_done = 0, next_offset = offset;
			 bytes_done < read_size && iosize > 0 && this_io > 0;
			 bytes_done += this_io, iosize -= bytes_done,
			pdata += this_io, next_offset += bytes_done)
		{

			this_io = pread(file_obj->fd, pdata, iosize, next_offset);
			if (this_io == 0)
				break;
			if (this_io < 0)
			{
				error = isi_system_error_new(errno,
											 "Failed read from cache file for "
											 "file offset %ld, length %ld, "
											 "using region %ld, read size %ld "
											 "next_offset %ld, iosize %ld "
											 "filesize %ld",
											 offset, length, region, read_size,
											 next_offset, iosize,
											 filesize);
				goto out;
			}
			ilog(IL_DEBUG, "Cache hit on read for region %ld "
						   "for %{} offset: %ld, length: %ld, "
						   "read_offset: %ld, read_size: %ld "
						   "iosize: %ld, this_io: %ld, bytes_done: %ld, "
						   "next_offset: %ld",
				 region, isi_cbm_file_fmt(file_obj),
				 offset, length, read_offset, read_size,
				 iosize, this_io, bytes_done, next_offset);
		}
		ilog(IL_TRACE, "Cache hit on read for region %ld for %{}",
			 region, isi_cbm_file_fmt(file_obj));
	}
out:
	if (fillbuf)
	{
		free(fillbuf);
		fillbuf = NULL;
	}

	if (region_locked)
		cacheinfo->cache_unlock_region(region, isi_error_suppress());

	if (ch_locked & !ch_lock_held)
		cacheinfo->cacheheader_unlock(isi_error_suppress());

	if (error)
	{
		isi_error_handle(error, error_out);
		bytes_done = -1;
	}

	return bytes_done;
}

struct timeval
isi_cbm_get_cloud_data_retention_time(struct isi_cbm_file *file,
									  struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	struct timeval date_of_death =
		isi_cbm_get_cloud_data_retention_time(file->lin, file->snap_id,
											  *(file->mapinfo), &error);
	ON_ISI_ERROR_GOTO(out, error, "file: %{}", isi_cbm_file_fmt(file));

out:
	isi_error_handle(error, error_out);

	return date_of_death;
}

struct timeval
isi_cbm_get_cloud_data_retention_time(ifs_lin_t lin, ifs_snapid_t snapid,
									  const isi_cfm_mapinfo &mapinfo, struct isi_error **error_out)
{
	ASSERT(lin != INVALID_LIN);
	ASSERT(snapid != INVALID_SNAPID);

	struct isi_error *error = NULL;

	struct timeval now, retention_time_tv = {0};
	uint64_t retention_time = 0;
	struct fp_attributes *attributes = NULL;
	struct fp_policy *archive_policy = NULL;
	isi_cbm_id policy_id = mapinfo.get_policy_id();

	{
		scoped_ppi_reader rdr;
		const isi_cfm_policy_provider *cbm_cfg = rdr.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		archive_policy = get_policy_by_id(policy_id,
										  (isi_cfm_policy_provider *)cbm_cfg, &error);
		ON_ISI_ERROR_GOTO(out, error);

		/*
		 * Get the retention time from the archive policy;
		 * if there is none, get the default retention time
		 */
		if (archive_policy != NULL)
		{
			retention_time = archive_policy->attributes->cloudpool_action->cloud_garbage_retention;
			ilog(IL_DEBUG,
				 "File: %{}, CloudPools Policy Retention Time: %lu",
				 lin_snapid_fmt(lin, snapid), retention_time);
		}
		else
		{
			smartpools_get_default_attributes(&attributes,
											  ((isi_cfm_policy_provider *)cbm_cfg)->sp_context,
											  &error);
			ON_ISI_ERROR_GOTO(out, error,
							  "Failed to get the default CloudPools "
							  "Policy attributes");

			retention_time = attributes->cloudpool_action->cloud_garbage_retention;
			ilog(IL_DEBUG,
				 "File: %{}, Default Retention Time: %lu",
				 lin_snapid_fmt(lin, snapid), retention_time);
		}
	}

	gettimeofday(&now, NULL);
	ilog(IL_DEBUG,
		 "File: %{}, Current Time: %lu",
		 lin_snapid_fmt(lin, snapid), now.tv_sec);

	retention_time_tv.tv_sec = now.tv_sec + 10;///retention_time/*这个值默认在isi_pools_fsa.gcfg定义为7天*/; jji

out:
	isi_error_handle(error, error_out);

	return retention_time_tv;
}

int isi_cbm_file_cache_write_common(
	struct isi_cbm_file *file_obj,
	uint64_t in_offset,
	size_t in_length,
	bool &ch_locked,
	bool &cache_extended,
	cpool_cache **out_cacheinfo,
	struct isi_error **error_out)
{
	int state = 0;
	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = NULL;
	size_t end = 0;
	off_t last_region = -1;
	bool ch_exclusive_lk = false;

	cache_extended = false;
	if (in_length <= 0)
	{
		error = isi_system_error_new(EINVAL,
									 "Illegal write length for %{}",
									 isi_cbm_file_fmt(file_obj));
		goto out;
	}
	end = in_offset + (uint64_t)in_length;

	/*
	 * XXXegc:  Do we need a lock in the isi_cbm__file structure here to
	 * make this change or is it protected above this?  To go along with
	 * this question, when the file size changes, who is responsible for
	 * changing the st_size in this stucture and how is EOF protected?
	 */
	if (!file_obj->cacheinfo)
	{
		file_obj->cacheinfo = new cpool_cache(file_obj);
	}

	if (!file_obj->cacheinfo)
	{
		error = isi_system_error_new(ENOMEM,
									 "Cannot allocate cacheinfo for %{}",
									 isi_cbm_file_fmt(file_obj));
		goto out;
	}

	cacheinfo = file_obj->cacheinfo;
	*out_cacheinfo = cacheinfo;

	isi_cbm_file_open_and_or_init_cache(file_obj,
										false, false, &error);
	if (error)
	{
		ilog(IL_DEBUG,
			 "Failed to init cache for %{} "
			 "on write to %lu, length %ld "
			 "Error: %#{}",
			 isi_cbm_file_fmt(file_obj),
			 in_offset, in_length, isi_error_fmt(error));
		ON_ISI_ERROR_GOTO(out, error);
	}

	last_region = (end - 1) / cacheinfo->get_regionsize();

retry_lock:

	cacheinfo->cacheheader_lock(ch_exclusive_lk, &error);
	ON_ISI_ERROR_GOTO(out, error);
	ch_locked = true;

	if (isi_cbm_queue_daemon_needed(file_obj, ISI_CBM_CACHE_FLAG_CACHED) ||
		isi_cbm_queue_daemon_needed(file_obj, ISI_CBM_CACHE_FLAG_DIRTY) ||
		!cacheinfo->is_valid_region(last_region))
	{

		/*
		 * need to update one of cache header attributes so drop the
		 * lock and reaquire exclusive while we change it.
		 */
		if (!ch_exclusive_lk)
		{
			ch_exclusive_lk = true;
			cacheinfo->cacheheader_unlock(&error);
			ON_ISI_ERROR_GOTO(out, error);
			ch_locked = false;
			goto retry_lock;
		}

		isi_cbm_file_queue_daemon_internal(
			file_obj, ISI_CBM_CACHE_FLAG_CACHED, &error);
		if (error)
		{
			ilog(IL_DEBUG,
				 "Failed to set ISI_CBM_CACHE_FLAG_CACHED for %{} "
				 "on write to %lu, length %ld "
				 "Error: %#{}",
				 isi_cbm_file_fmt(file_obj),
				 in_offset, in_length, isi_error_fmt(error));
			ON_ISI_ERROR_GOTO(out, error);
		}

		isi_cbm_file_queue_daemon_internal(
			file_obj, ISI_CBM_CACHE_FLAG_DIRTY, &error);
		if (error)
		{
			ilog(IL_DEBUG,
				 "Failed to set ISI_CBM_CACHE_FLAG_DIRTY for %{} "
				 "on write to %lu, length %ld "
				 "Error: %#{}",
				 isi_cbm_file_fmt(file_obj),
				 in_offset, in_length, isi_error_fmt(error));
			ON_ISI_ERROR_GOTO(out, error);
		}
		/*
		 * check to see if this operation is going beyond the current
		 * cache end.  If so, then extend the cache using the truncate
		 * call. NB, this is being done with the cacheheader lock
		 * being held exclusive still.
		 */
		if (!cacheinfo->is_valid_region(last_region))
		{
			// handle the last impacted region first, we need to
			// fetch from the cloud if necessary.
			isi_cbm_file_cache_last_region(
				file_obj, cacheinfo, end, &error);
			if (error)
			{
				ilog(IL_DEBUG,
					 "Failed to cache last region for %{} "
					 "on write to %lu, length %ld "
					 "Error: %#{}",
					 isi_cbm_file_fmt(file_obj),
					 in_offset, in_length, isi_error_fmt(error));
				ON_ISI_ERROR_GOTO(out, error);
			}

			state = cacheinfo->cache_extend(end, &error);
			if (error)
			{
				ilog(IL_DEBUG,
					 "Failed extend for %{} "
					 "on write to %lu, length %ld "
					 "Error: %#{}",
					 isi_cbm_file_fmt(file_obj),
					 in_offset, in_length, isi_error_fmt(error));
				ON_ISI_ERROR_GOTO(out, error);
			}

			cache_extended = true;
			ilog(IL_TRACE,
				 "File extended to %ld for %{} on write to %lu, "
				 "length %ld returned ch state %d",
				 end,
				 isi_cbm_file_fmt(file_obj),
				 in_offset, in_length, state);
		}
	}

	/*
	 * Cache header has now been initialize and the cache expanded if
	 * needed, so drop back to shared lock to allow other cache operations
	 * on this file to move forward.
	 */
	if (!state && ch_exclusive_lk)
	{

		// downgrade here
		cacheinfo->cacheheader_downgrade_to_shlock(&error);
		if (error)
		{
			ilog(IL_DEBUG,
				 "Failed to downgrade cache header lock for %{} "
				 "on write to %lu, length %ld "
				 "Error: %#{}",
				 isi_cbm_file_fmt(file_obj),
				 in_offset, in_length, isi_error_fmt(error));
			ON_ISI_ERROR_GOTO(out, error);
		}
	}

out:
	if (error)
	{
		isi_error_handle(error, error_out);
	}

	return state;
}

static ssize_t
isi_cbm_file_cache_write(struct isi_cbm_file *file_obj, uint64_t offset,
						 size_t length, const void *databuf, struct isi_error **error_out)
{
	ASSERT(file_obj);

	struct isi_error *error = NULL;
	cpool_cache *cacheinfo = NULL;
	int state = 0;
	bool region_locked = false;
	bool locked_excl = false;
	bool ch_locked = false;
	bool cache_extended = false;
	size_t bytes_done = 0;
	size_t iosize = 0;
	off_t next_offset = 0;
	ssize_t this_io = -1;

	size_t regionsize = 0;
	off_t region = -1;
	off_t region_offset = -1;
	size_t write_size = 0;

	char *fillbuf = NULL;
	size_t fillbufsize = 0;
	char *pdata = (char *)databuf;

	state = isi_cbm_file_cache_write_common(
		file_obj, offset, length,
		ch_locked, cache_extended,
		&cacheinfo, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(cacheinfo != NULL);

	regionsize = cacheinfo->get_regionsize();
	region = (off_t)(offset / (uint64_t)regionsize);
	if ((region < 0))
	{
		// The only way to get here would be to
		// have a negative region size, negative length,
		// or an unsigned offset that is so large that
		// after the division the sign bit is still set.
		// In any case it is invalid.
		// regionsize and length are defined here as unsigned
		// values so they can not be < 0 and we do not
		// specifically test them here
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
								  "Attempt to write negative offset: "
								  "offset %lld, regionsize %lld, length %ld",
								  offset, regionsize, length);
		ilog(IL_DEBUG, "Unable to perform vectored write for %{}, "
					   "Error: %#{}",
			 isi_cbm_file_fmt(file_obj), isi_error_fmt(error));
		goto out;
	}

	/*
	 * adjusted region_offset and write_size to match the the region
	 * boundary.
	 */
	region_offset = offset % regionsize;
	write_size = ((region_offset + length) <= regionsize) ? length : (regionsize - region_offset);

	ASSERT(cacheinfo->is_valid_region(region));
retry:
	cacheinfo->cache_lock_region(region, locked_excl, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "Cache write cannot lock region %lld in %s mode",
							  region, (locked_excl) ? "exclusive" : "shared");
		goto out;
	}

	region_locked = true;

	UFAIL_POINT_CODE(cbm_file_cache_write_nospace, {
		error = isi_system_error_new(ENOSPC,
									 "Generated by failpoint before filling region %ld",
									 region);
	});
	// possible error from failpoint
	if (error)
	{
		isi_error_add_context(error, "Failpoint error");
		goto out;
	}

	isi_cbm_cache_test_and_fill_region(
		file_obj, region, &fillbuf, fillbufsize, CO_CACHE,
		locked_excl, true, false, &error);

	if ((error) &&
		(!locked_excl) &&
		isi_cbm_error_is_a(error, CBM_NEED_LOCK_UPGRADE))
	{
		isi_error_free(error);
		error = NULL;
		cacheinfo->cache_unlock_region(
			region, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Cache unlock region %lld from %s mode",
								  region,
								  (locked_excl) ? "exclusive" : "shared");
			goto out;
		}
		region_locked = false;
		locked_excl = true;
		goto retry;
	}
	if (error)
	{
		isi_error_add_context(error, "Cannot fill cache for write");
		goto out;
	}

	if (region_locked && locked_excl)
	{
		/*
		 * We do not need the exclusive
		 * lock any more, so downgrade
		 */
		cacheinfo->cache_downgrade_region_to_shlock(
			region, isi_error_suppress(IL_ERR));
	}

	iosize = write_size;
	bytes_done = 0;
	next_offset = offset;
	while ((bytes_done < write_size) && (iosize > 0))
	{
		this_io = cache_pwrite(__func__, file_obj->fd, pdata, iosize,
							   next_offset, false, O_ENFORCE_QUOTA);
		if (this_io < 0)
		{
			error = isi_system_error_new(errno,
										 "Failed write to cache file for "
										 "file offset %ld, length %ld, "
										 "using region %ld, write size %ld",
										 offset, length, region, write_size);
			goto out;
		}
		bytes_done += this_io;
		iosize -= this_io;
		pdata += this_io;
		next_offset += this_io;
	}

	ilog(IL_TRACE, "Cache hit on write for region %ld for %{}",
		 region, isi_cbm_file_fmt(file_obj));

	if (region_locked)
	{
		cacheinfo->cache_unlock_region(region, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Could not unlock region %ld for %{}",
								  region, isi_cbm_file_fmt(file_obj));
			goto out;
		}
		region_locked = false;
	}

	if (state)
	{
		cacheinfo->clear_state(state, &error);
		if (error)
		{
			isi_error_add_context(error,
								  "Unable to clear cache extend state for %{}",
								  isi_cbm_file_fmt(file_obj));
			goto out;
		}
	}

out:
	if (fillbuf)
	{
		free(fillbuf);
		fillbuf = NULL;
	}

	if (region_locked)
		cacheinfo->cache_unlock_region(region, isi_error_suppress());

	if (cache_extended && error)
	{
		/*
		 * We encountered an error after the cache was extended.
		 * Reset the size based on the filesize.
		 */
		struct isi_error *cleanup_error = NULL;
		off_t filesize = -1;
		off_t new_last_region = -1;

		ilog(IL_DEBUG,
			 "Error occurred after extending cache for %{} "
			 "Error: %{}",
			 isi_cbm_file_fmt(file_obj),
			 isi_error_fmt(error));

		if (ch_locked)
		{
			cacheinfo->cacheheader_upgrade_to_exlock(
				ch_locked, &cleanup_error);
			ON_ISI_ERROR_GOTO(cleanup_failed, cleanup_error);

			/*
			 * we released and relocked the header
			 * so refresh to be sure we have the
			 * most recent information
			 */
			cacheinfo->refresh_header(&cleanup_error);
			ON_ISI_ERROR_GOTO(cleanup_failed, cleanup_error);
		}
		else
		{
			/* In reality, cache_extended should not be true
			 * unless ch_locked is also true, but this is here
			 * in case of a change in the future.
			 */
			cacheinfo->cacheheader_lock(true, &cleanup_error);
			ON_ISI_ERROR_GOTO(cleanup_failed, cleanup_error);
			ch_locked = true;
		}
		filesize = cacheinfo->get_filesize(&cleanup_error);
		ON_ISI_ERROR_GOTO(cleanup_failed, cleanup_error);

		new_last_region =
			((filesize) ? ((filesize - 1) / regionsize) : -1);
		ilog(IL_DEBUG,
			 "Calling isi_cbm_file_ftruncate_restart for %{} "
			 "with filesize %llu, last region old/new %llu/%llu",
			 isi_cbm_file_fmt(file_obj),
			 filesize, cacheinfo->get_last_region(),
			 new_last_region);

		isi_cbm_file_ftruncate_restart(
			file_obj,
			filesize,
			cacheinfo,
			&cleanup_error);
		ON_ISI_ERROR_GOTO(cleanup_failed, cleanup_error);

	cleanup_failed:
		if (cleanup_error)
		{
			ilog(IL_NOTICE,
				 "Failed to reset the cacheinfo size for %{} "
				 "after error, Failure: %{}",
				 isi_cbm_file_fmt(file_obj),
				 isi_error_fmt(cleanup_error));
			isi_error_free(cleanup_error);
			cleanup_error = NULL;
		}
	}

	if (ch_locked)
	{
		cacheinfo->cacheheader_unlock(isi_error_suppress());
	}

	if (error)
	{
		isi_error_handle(error, error_out);
		bytes_done = -1;
	}

	return bytes_done;
}

const isi_cfm_mapinfo *
isi_cbm_file_get_stubmap(struct isi_cbm_file *file)
{
	ASSERT(file != NULL);
	return file->mapinfo;
}

void isi_cbm_file_update_mtime(struct isi_cbm_file *file_obj,
							   struct timespec *time_spec, struct isi_error **error_out)
{
	ASSERT(file_obj != NULL);
#if 0 /* Not currently using shadow mtime */
	struct isi_error *error = NULL;
	struct timespec cur_time;

	int ret = 0;
	if (time_spec == NULL) {
		struct timeval tv;
		if ((ret = gettimeofday(&tv, NULL)) < 0) {
			error = isi_system_error_new(errno, "%s",
			    "cannot get current time");
			goto out;
		}
		cur_time.tv_sec = tv.tv_sec;
		cur_time.tv_nsec = tv.tv_usec * 1000;
		time_spec = &cur_time;
	}

	ret = extattr_set_fd(file_obj->fd, EXTATTR_NAMESPACE_IFS,
	    IFS_CPOOL_ATTR_MTIME, time_spec, sizeof(struct timespec));
	if (ret < 0) {
		error = isi_system_error_new(errno, "%s",
		    "cannot set modification time (EA)");
	}

 out:
	isi_error_handle(error, error_out);
#endif
}

const char *
isi_cbm_stub_cluster_id(struct isi_cbm_file *file)
{
	return file->mapinfo->get_provider_id().get_cluster_id();
}

/*
 * Given the fd, open an existing stub file. The caller must have
 * the privilege to open the file by the LIN number.
 * @param fd[IN]: The file descriptor. The fd must have been opened.
 * @param error_out[OUT]: errors if any, see above for detailed information.
 * @return the opaque isi_cbm_file, this object must be closed by calling
 *	   isi_cbm_close.
 */
struct isi_cbm_file *
isi_cbm_file_get(int fd, struct isi_error **error_out)
{
	struct isi_cbm_file *file = NULL;
	struct stat sb = {0};
	struct isi_error *error = NULL;
	int flags = 0;
	if (fstat(fd, &sb) == -1)
	{
		error = isi_system_error_new(errno, "Could not get stat for %d"
											"\n",
									 fd);
		goto out;
	}

	/**
	 * TBD: Need to lock down stub to prevent mapping change.
	 */
	file = isi_cbm_file_open(sb.st_ino, sb.st_snapid, flags, &error);

	if (error)
	{
		isi_error_add_context(error,
							  "Failed to open cbm file for lin %{} snapid %{}",
							  lin_fmt(sb.st_ino), snapid_fmt(sb.st_snapid));

		goto out;
	}

out:
	isi_error_handle(error, error_out);

	return file;
}

off_t isi_cbm_file_get_filesize(struct isi_cbm_file *file, struct isi_error **error_out)
{
	off_t offset = -1;
	struct isi_error *error = NULL;
	struct stat buf;

	if (fstat(file->fd, &buf) < 0)
		error = isi_system_error_new(errno,
									 "Cannot get file size for %{}", isi_cbm_file_fmt(file));
	else
		offset = buf.st_size;

	isi_error_handle(error, error_out);

	return offset;
}

void isi_cbm_file_version_get(struct isi_cbm_file *file, isi_cbm_file_version *ver,
							  struct isi_error **error_out)
{
	ASSERT(file && ver && error_out);
	struct isi_error *error = NULL;

	// Riptide only supports one stub file ver (our first)
	// so this(our) file must also be that ver.
	ver->_num = CBM_FILE_VER_RIPTIDE_NUM;

	// Check and set what capabilities are used by this file
	const isi_cfm_mapinfo &mapinfo = *(file->mapinfo);
	const isi_cbm_id &account_id = mapinfo.get_account();
	struct isi_cbm_account account;
	uint64_t acct_auth_capability = 0;

	{
		scoped_ppi_reader ppi_rdr;

		const isi_cfm_policy_provider *ppi = ppi_rdr.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		isi_cfm_get_account((isi_cfm_policy_provider *)ppi, account_id,
							account, &error);
		ON_ISI_ERROR_GOTO(out, error);

		isi_cfm_get_acct_provider_auth_cap(
			(isi_cfm_policy_provider *)ppi, account_id,
			&acct_auth_capability, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	ver->_flags = 0;

	if (account.type == CPT_RAN)
		ver->_flags |= HAS_RAN_PROVIDER;
	else if (account.type == CPT_AZURE)
		ver->_flags |= HAS_AZURE_PROVIDER;
	else if (account.type == CPT_AWS)
		ver->_flags |= HAS_S3_PROVIDER;
	else if (account.type == CPT_ECS || account.type == CPT_ECS2)
		ver->_flags |= HAS_ECS_PROVIDER;
	else if (account.type == CPT_GOOGLE_XML)
		ver->_flags |= HAS_GOOGLE_XML_PROVIDER;
	else
		ASSERT(0);

	if (acct_auth_capability == CAP_AUTH_AWS_V4)
	{
		ver->_flags |= HAS_V4_AUTH_VER_ONLY;
	}

	// Only 1 kind of compression supported by Riptide
	if (mapinfo.is_compressed())
		ver->_flags |= HAS_COMP_V1;

	// Only 1 kind of encryption supported by Riptide
	if (mapinfo.is_encrypted())
		ver->_flags |= HAS_ENCR_V1;

out:
	isi_error_handle(error, error_out);
}
