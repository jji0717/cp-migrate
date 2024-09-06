#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>

#include <isi_ilog/ilog.h>

#include "isi_cbm_file.h"
#include "isi_cbm_error.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_cache_iterator.h"

#include "isi_cbm_snap_diff_int.h"

struct isi_cbm_snap_diff_iterator {
	bool local_only;  // only diff the OneFS snap content, not cloud content
	struct snap_diff_iterator sdit;
	struct isi_cbm_file *file_snap1;
	struct isi_cbm_file *file_snap2;
	size_t filesize_snap1;
	size_t filesize_snap2;
};

static void
isi_cbm_file_close_ne(struct isi_cbm_file *file)
{
	struct isi_error *error = NULL;

	isi_cbm_file_close(file, &error);
	if (error) {
		ilog(IL_ERR, "isi_cbm_file_close() failed: %s\n",
		    isi_error_get_detailed_message(error));
		isi_error_free(error);
		error = NULL;
	}
}

// Obtain statbuf for a lin:snap
static void
sd_lin_snap_stat(ifs_lin_t lin, ifs_snapid_t snap_id,
    struct stat *sb, isi_error **err_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(sb);

	fd = ifs_lin_open(lin, snap_id, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "ifs_lin_open failed for lin %{} snapid %{}",
		    lin_fmt(lin), snapid_fmt(snap_id));
		goto out;
	}
	if (fstat(fd, sb) != 0) {
		error = isi_system_error_new(errno,
		    "failed to fstat for lin %{} snapid %{}",
		    lin_fmt(lin), snapid_fmt(snap_id));
		goto out;
	}

 out:
	isi_error_handle(error, err_out);
	if (fd >= 0)
		close(fd);
}

struct isi_cbm_snap_diff_iterator *
isi_cbm_snap_diff_create(ifs_lin_t lin, off_t start, ifs_snapid_t snap1,
    ifs_snapid_t snap2, bool local_only) // , struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_cbm_snap_diff_iterator cit = {0};
	struct isi_cbm_snap_diff_iterator *pcit = NULL;
	int flags = 0;
	bool snap2_stub = false;

	if (snap2 != INVALID_SNAPID) {
		if (local_only) {
			// check if snap2 is stub file.
			struct stat sb;
			sd_lin_snap_stat(lin, snap2, &sb, &error);
			if (error) {
				isi_error_add_context(error, "open snap2 lin");
				goto out;
			}
			if (sb.st_flags & SF_FILE_STUBBED)
				snap2_stub = true;
		} else {
			cit.file_snap2 = isi_cbm_file_open(lin, snap2, flags,
					     &error);
			if (error) {
				if (!isi_cbm_error_is_a(error,
				    CBM_NOT_A_STUB)) {
					isi_error_add_context(error,
					    "open snap2");
					goto out;
				}
				isi_error_free(error);
				error = NULL;
			}
			if (cit.file_snap2) {
				cit.filesize_snap2 = isi_cbm_file_get_filesize(
				    cit.file_snap2, &error);
				if (error) {
					isi_error_add_context(error,
					    "size snap2");
					goto out;
				}
				snap2_stub = true;
			}
		}
	}

	if (snap1 != INVALID_SNAPID) {
		if (local_only) {
			// check if snap1 is stub file.  If so, and for a
			// (local_only) diff against a snap2 which is a regular
			// file, get all data regions that are captured by snap2
			// on oneFS. This allows us to handle the case where
			// uncached (per cacheinfo) regions in snap1 may
			// actually contain data (see bug 168319).
			struct stat sb;
			sd_lin_snap_stat(lin, snap1, &sb, &error);
			if (error) {
				isi_error_add_context(error, "open snap1 lin");
				goto out;
			}
			if ((sb.st_flags & SF_FILE_STUBBED) && !snap2_stub) {
				snap1 = INVALID_SNAPID;
			}
		} else { // !local_only
			cit.file_snap1 = isi_cbm_file_open(lin, snap1, flags,
					     &error);
			if (error) {
				if (!isi_cbm_error_is_a(error,
				    CBM_NOT_A_STUB)) {
					isi_error_add_context(error,
					    "open snap1");
					goto out;
				}
				isi_error_free(error);
				error = NULL;
			}
			if (cit.file_snap1) {
				cit.filesize_snap1 = isi_cbm_file_get_filesize(
				    cit.file_snap1, &error);
				if (error) {
					isi_error_add_context(error,
					    "size snap1");
					goto out;
				}
			}
		}
	}

	// init oneFS snapdiff iterator
	snap_diff_init(lin, start, snap1, snap2, &cit.sdit);

	// unless overridden by the caller passing 'local_only == true",
	// deduce whether we need to be local_only by checking if we have
	// snapped a stub file in any snap
	cit.local_only = local_only || !(cit.file_snap1  || cit.file_snap2);

	// all good
	pcit = (isi_cbm_snap_diff_iterator *) malloc(sizeof(cit));
	ASSERT(pcit);
	memcpy(pcit, &cit, sizeof(*pcit));

 out:
	if (error) {
		ilog(IL_ERR, "isi_cbm_snap_diff_init() failed: %s\n",
		    isi_error_get_detailed_message(error));
		isi_error_free(error);
		error = NULL;

		if (cit.file_snap1)
			isi_cbm_file_close_ne(cit.file_snap1);
		if (cit.file_snap2)
			isi_cbm_file_close_ne(cit.file_snap2);
		isi_error_free(error);
		error = NULL;

	}
	return pcit;
}

void
isi_cbm_snap_diff_destroy(struct isi_cbm_snap_diff_iterator *iter)
{
	ASSERT(iter);

	if (iter->file_snap1)
		isi_cbm_file_close_ne(iter->file_snap1);

	if (iter->file_snap2)
		isi_cbm_file_close_ne(iter->file_snap2);

	free(iter);
}

isi_cbm_file_region_type
isi_cbm_file_region_type_get(int val)
{
	isi_cbm_file_region_type frt;

	if (val == ISI_CPOOL_CACHE_INVALID)
		frt = FRT_INVALID;
	else if (val == ISI_CPOOL_CACHE_NOTCACHED)
		frt = FRT_UNCACHED;
	else if (val == ISI_CPOOL_CACHE_CACHED)
		frt = FRT_CACHE_READ;
	else if ((val == ISI_CPOOL_CACHE_DIRTY) ||
	    (val == ISI_CPOOL_CACHE_INPROGRESS))
		frt = FRT_CACHE_WRITE;
	else
		ASSERT(0, "Cannot map %d to a valid file_region_type", val);

	return frt;
}

// Set file region based on mapentry comparison for the two snaps
static void
mapent_sdr_set(struct isi_cbm_snap_diff_iterator *pcit,
    isi_cbm_file_region fr[],
    snap_diff_region &dr, struct isi_error **err_out)
{
	struct isi_error *error = NULL;
	bool same_ent = false, got_ent0 = false, got_ent1 = false;
	isi_cfm_mapinfo::iterator it0, it1;
	isi_cfm_mapentry *entry0 = NULL, *entry1 = NULL;

	ASSERT(pcit->file_snap1);
	ASSERT(pcit->file_snap2);
	// Obtain map entries
	isi_cfm_mapinfo &m0 = *(pcit->file_snap1->mapinfo);
	isi_cfm_mapinfo &m1 = *(pcit->file_snap2->mapinfo);
	off_t offset = dr.start_offset;

	got_ent0 = m0.get_containing_map_iterator_for_offset(it0, offset,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	got_ent1 = m1.get_containing_map_iterator_for_offset(it1, offset,
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	// Compare the entries, not just iterators/offsets thereof
	// but also other attributes.
	if (got_ent0 && got_ent1) {
		entry0 = &it0->second;
		entry1 = &it1->second;
		same_ent = (*entry0 == *entry1);
	}
	if  (same_ent) {
		dr.region_type =  RT_UNCHANGED;

		// find length of the file region that overlaps with the
		// corresponding mapentry and set the overlap length
		// as the new file region length
		size_t m0_length = entry0->get_length() -
		    (offset - entry0->get_offset());
		size_t m1_length = entry1->get_length() -
		    ( offset - entry1->get_offset());;
		fr[0].length = MIN(fr[0].length, m0_length);
		fr[1].length = MIN(fr[1].length, m1_length);
	} else {
		dr.region_type =  RT_DATA;
	}
	dr.byte_count = MIN(fr[0].length, fr[1].length);
 out:
	isi_error_handle(error, err_out);
}

// Set file region based on snapdiff of two snaps
static void
sd_sdr_set(struct isi_cbm_snap_diff_iterator *pcit,
    isi_cbm_file_region fr[], snap_diff_region &dr, struct isi_error **err_out)
{
	struct isi_error *error = NULL;
	int ret;
	uint64_t lin = pcit->sdit.lin;
	uint64_t snap1 = pcit->sdit.snap1;
	uint64_t snap2 = pcit->sdit.snap2;
	struct snap_diff_iterator diff_iter;
	off_t curr_off = pcit->sdit.next_start;
	size_t byte_count = MIN(fr[0].length, fr[1].length);

	snap_diff_init(lin, curr_off, snap1, snap2, &diff_iter);
	ret = ifs_snap_diff_next(&diff_iter, &dr);
	if (ret) {
		error = isi_system_error_new(errno,
		    "Failed to diff lin %016llx "
		    "snap1 0x%016llx "
		    "snap2 0x%016llx", lin, snap1, snap2);
		goto out;
	}

	// Adjust byte count to the region byte count
	dr.byte_count = MIN(byte_count, dr.byte_count);
 out:
	isi_error_handle(error, err_out);
}

// Set file region first based on snapdiff and for any DATA (i.e. changed)
// regions identified, confirmed via mapent comparison
static void
combo_sdr_set(struct isi_cbm_snap_diff_iterator *pcit,
    isi_cbm_file_region fr[], snap_diff_region &dr, struct isi_error **err_out)
{
	struct isi_error *error = NULL;

	// snap diff first
	sd_sdr_set(pcit, fr, dr, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (dr.region_type != RT_DATA)
		goto out;

	// else check if there is a mapent
	// change.
	// The order is important: since
	// in some cases cache diff may yield
	// a change but mapent may not have changed
	mapent_sdr_set(pcit, fr, dr, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, err_out);

}


// This function is used when we encounter the invalid region.  It ensures the
// same snap_diff behavior for stub files as is seen for regular file.
static void
set_snap2_invalid(struct snap_diff_region *dr, bool inode_match)
{
	dr->region_type = (inode_match ? RT_UNCHANGED : RT_SPARSE);
	dr->byte_count = 0; // required to end iteration
}

// check if the file handles are the same revision of the same file
static int
check_inode_match(struct isi_cbm_file *file1, struct isi_cbm_file *file2,
    bool *inode_match)
{
	int ret = -1;
	uint64_t filerev1 = 0, filerev2 = 0;

	ASSERT(file1 || file2); // atleast one stub

	if (file1 == NULL || file2 == NULL) {
		// a non-stub and a stub file exist, so inodes will not match
		*inode_match = false;
		ret = 0;
		goto out;
	}

	// Both stub files. Compare filerev.
	// XXXPD ensure file*->fd have been fsynced before
	// filerev is obtained. Will the snap-diff caller
	// ensure this or we need to do it...perhaps in snap_diff_init?
	ret = getfilerev(file1->fd, &filerev1);
	if (ret)
		goto out;
	ret = getfilerev(file2->fd, &filerev2);
	if (ret)
		goto out;
	*inode_match = (filerev1 == filerev2);

 out:
	return ret;
}


int
isi_cbm_snap_diff_next(struct isi_cbm_snap_diff_iterator *pcit,
    struct snap_diff_region *dr)
{
	ASSERT(pcit);
	ASSERT(dr);

	struct isi_error *error = NULL;
	int mask, ret = -1;
	isi_cbm_file_region fr[2];
	ifs_lin_t lin = pcit->sdit.lin;
	struct isi_cbm_file * files[2]= {pcit->file_snap1, pcit->file_snap2};
	size_t filesizes[2] = {pcit->filesize_snap1, pcit->filesize_snap2};
	ifs_snapid_t snaps[2] = {pcit->sdit.snap1, pcit->sdit.snap2};
	off_t start_offset = pcit->sdit.next_start;
	bool inode_match = false;


	// LOCAL ONLY
	if (pcit->local_only) {
		ret = ifs_snap_diff_next(&pcit->sdit, dr);
		goto out;
	}

	// LOCAL AND CLOUD
	ASSERT(!pcit->local_only);


	// Do argument validation
	if (snaps[1] == INVALID_SNAPID || snaps[0] > snaps[1]) {
		errno = EINVAL;
		goto out;
	}

	// Check for inode match
	if (check_inode_match(files[0], files[1], &inode_match)) {
		goto out;
	}

	// Allow any type of cache region to be retrieved
	mask = ISI_CPOOL_CACHE_MASK_NOTCACHED | ISI_CPOOL_CACHE_MASK_CACHED |
	    ISI_CPOOL_CACHE_MASK_DIRTY | ISI_CPOOL_CACHE_MASK_INPROGRESS;

	// Get the next file region from both files
	for (int i = 0; i < 2; i++) {
		bool eof = false;

		if (files[i]) {	// stub file
			isi_cbm_cache_record_info cri;
			bool whole_range = true, no_cache = false;
			cbm_cache_iterator ccit(files[i], start_offset,
			    filesizes[i], mask, whole_range);

			if (((size_t) start_offset) >= filesizes[i]) {
				// avoid creating a file region for part of the
				// cache region that lies outside the file size
				eof = true;
			} else if (!ccit.next(&cri, &error)) {
				// check if the file has a cache
				no_cache =
				    !(files[i]->cacheinfo->is_cache_open()) ||
				    (error &&
				    (isi_system_error_is_a(error, ENOENT) ||
				    isi_system_error_is_a(error, EROFS)));
				if (no_cache) {
					if (error) {
						isi_error_free(error);
						error = NULL;
					}
				} else if (error) {
					goto out;
				} else { // no error, true eof
					eof = true;
				}
			}

			if (!eof) {
				fr[i].offset = start_offset;
				if (no_cache) {
					fr[i].frt = FRT_UNCACHED;
					fr[i].length = filesizes[i] -
					    start_offset;
				} else {
					fr[i].frt =
					    isi_cbm_file_region_type_get(
					    cri.region_type);
					fr[i].length = cri.length -
					    (start_offset - cri.offset);
				}
			}
		} else { 	 // regular file or invalid snap
			struct stat sb;

			if (snaps[i] == INVALID_SNAPID) {
				ASSERT(i == 0);
				// Treat the first, invalid, snap as an
				// invalid region
				eof = true;
			} else {
				sd_lin_snap_stat(lin, snaps[i], &sb, &error);
				ON_ISI_ERROR_GOTO(out, error);
				eof = (start_offset >= sb.st_size);
			}

			if (!eof) {
				fr[i].offset =  start_offset;
				fr[i].frt = FRT_ORDINARY;
				fr[i].length = sb.st_size - start_offset;
			}
		}
		// create an invalid region after the filesize is exceeded
		if (eof) {
			fr[i].frt = FRT_INVALID;
			fr[i].offset = start_offset;
			// if start_offset is zero we
			// will overflow max by adding 1
			fr[i].length = (start_offset) ?
				((OFF_MAX - start_offset) + 1) :
			        OFF_MAX ;
		}
	}

	// Compare file regions
	ASSERT(fr[0].offset == fr[1].offset);
	dr->start_offset = start_offset;

	switch (fr[0].frt) {
		case FRT_INVALID:
		case FRT_ORDINARY:
		{
			switch (fr[1].frt) {
				case FRT_INVALID:
				{
					set_snap2_invalid(dr, inode_match);
					break;
				}
				case FRT_UNCACHED:
				{
					dr->region_type = RT_DATA;
					dr->byte_count = MIN(fr[0].length,
					    fr[1].length);
					break;
				}
				case FRT_ORDINARY:
				case FRT_CACHE_READ:
				case FRT_CACHE_WRITE:
				{
					sd_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
			}
			break;
		}
		case FRT_UNCACHED:
		case FRT_CACHE_READ:
		{
			switch (fr[1].frt) {
				case FRT_INVALID:
				{
					set_snap2_invalid(dr, inode_match);
					break;
				}
				case FRT_UNCACHED:
				{
					mapent_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
				case FRT_ORDINARY:
				case FRT_CACHE_WRITE:
				{
					sd_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
				case FRT_CACHE_READ:
				{
					combo_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
			}
			break;
		}
		case FRT_CACHE_WRITE:
		{
			switch (fr[1].frt) {
				case FRT_INVALID:
				{
					set_snap2_invalid(dr, inode_match);
					break;
				}
				case FRT_UNCACHED:
				{
					// using mapent comparison instead
					// of min (fr[0].length, fr[1].length)
					// to cover override invalidation of
					// dirty regions
					mapent_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
				case FRT_ORDINARY:
				case FRT_CACHE_READ:
				case FRT_CACHE_WRITE:
				{
					sd_sdr_set(pcit, fr, *dr, &error);
					ON_ISI_ERROR_GOTO(out, error);
					break;
				}
			}
			break;
		}
	}

	// Success
	ret = 0;

	// Adjust iterator for next starting offset
	pcit->sdit.next_start += dr->byte_count;
 out:
	if (error) {
		ASSERT(ret < 0);
		ilog(IL_ERR, "isi_cbm_snap_diff_next() failed: %s\n",
		    isi_error_get_detailed_message(error));
		isi_error_free(error);
		error = NULL;
	}
	// isi_error_handle(error, err_out);
	return ret;
}

