#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <ifs/dfm/dfm_on_disk.h>

#include <ifs/ifs_lin_open.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_malloc_ostream.h>
#include <isi_util/isi_printf.h>
#include <isi_ufp/isi_ufp.h>

#include "treewalk.h"
#include "isi_migrate/migr/selectors.h"

/*
 * We set it inside worker`s work_init_callback 
 * we can use this flag inside read_buffer to skip any EIO
 */
bool _g_expected_dataloss = false;

/*
 * to let migr_continue_work () know that
 * entire directory is corrupted, readdirplus () fails
 */
bool _g_directory_corrupted = false;

/*
 * Constants for controlling the amount of file descriptors left open during
 * operation.
 */
#define MIGR_THAW_HIGH_WATER 30
#define MIGR_THAW_SKIP 10
#define MIGR_THAW_LOW_WATER (MIGR_THAW_HIGH_WATER - MIGR_THAW_SKIP)

/* Two different forms of the base HIGH > SKIP + HIGH - LOW */
CTASSERT(MIGR_THAW_HIGH_WATER > 2 * MIGR_THAW_SKIP);
CTASSERT(MIGR_THAW_LOW_WATER > MIGR_THAW_SKIP);

/**
 * Used as a header to the treewalker serialized format.
 */
#define MIGR_TW_SERIALIZE_MAGIC 0x57545153

/**
 * Packed structure for storing dir slice info for serialized treewalkers.
 */
struct migr_slice_od {
	uint64_t	lin;
	uint64_t	snapid;
	uint64_t	begin;
	uint64_t	end;
	uint64_t	checkpoint;
} __packed;

static void
dir_slice_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const struct dir_slice *slice = (const void *)arg.ptr;

	fmt_print(fmt, "(0x%016llx-0x%016llx)", slice->begin, slice->end);
}

MAKE_FMT_CONV(dir_slice, const struct dir_slice *);

struct migr_dir *
migr_dir_new(int fd, const struct dir_slice *slice, bool need_stat,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;

	dir = calloc(1, sizeof(*dir));
	ASSERT(dir);

	if (fstat(fd, &dir->_stat) == -1) {
		error = isi_system_error_new(errno, "Failed to stat dir");
		goto out;
	}

	dir->_fd = fd;
	if (slice) {
		dir->_slice = *slice;
		dir->prev_sent_cookie = slice->begin;
	} else {
		dir->_slice.begin = DIR_SLICE_MIN;
		dir->_slice.end = DIR_SLICE_MAX;
		dir->prev_sent_cookie = DIR_SLICE_MIN;
	}
	dir->_resume = dir->_checkpoint = dir->_slice.begin;
	dir->_select_tree = NULL;
	dir->selected = true;
	dir->name = strdup("BASE");
	dir->_need_stat = need_stat;

out:
	if (error) {
		free(dir);
		dir = NULL;
	}

	isi_error_handle(error, error_out);
	return dir;
}

static void
populate_migr_dirent(struct migr_dirent *mdirent, struct dirent *dirent,
    struct stat *stat, uint64_t cookie)
{
	mdirent->dirent = dirent;
	mdirent->stat = stat;
	mdirent->cookie = cookie;
}

static struct migr_dir_buffer *
read_buffer(struct migr_dir *dir, uint64_t resume, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir_buffer *buffer = NULL;
	uint64_t new_resume;
	int ret, i, skip;
	char *c;
	struct dirent *td;
	struct stat *statp = NULL;
	int fd;
	unsigned int read_dirents_num = READ_DIRENTS_NUM;
	bool need_stat;

	ASSERT(dir != NULL);

	UFAIL_POINT_CODE(change_num_dirents_read,
	    read_dirents_num = RETURN_VALUE;
	);

	fd = dir->_fd;
	need_stat = dir->_need_stat;

	if (resume == DIR_SLICE_MAX)
		goto out;

	buffer = calloc(1, sizeof(*buffer));
	ASSERT(buffer);

	buffer->_count = read_dirents_num;

	new_resume = resume;
	if (need_stat)
		statp = buffer->_stats;

	/* 
	 * Set the resume cookie to the last read cookie or 0 if no reads have
	 * been performed yet.
	 */
	ret = readdirplus(fd, RDP_NOTRANS, &new_resume,
	    READ_DIRENTS_SIZE, buffer->_dirents, READ_DIRENTS_SIZE,
	    &buffer->_count, statp, buffer->_cookies);
	if (ret == -1) {
		if (!_g_expected_dataloss)
			/* normal code path, bail out*/
			error = isi_system_error_new(errno, "Failed readdirplus");
		else {
			/* behave as if we were at the end of the directory */
			free(buffer);
			buffer = NULL;

			/* 
			 * let migr_continue_work know 
			 * that directory is corrupted
			 * so that directory entry can be
			 * logged in worker log file            
			 */
			_g_directory_corrupted = true;
		}
		goto out;
	}

	/* XXXJDH: workaround for bug 50892 */
	for (skip = 0, c = buffer->_dirents; skip < buffer->_count; skip++) {

		if (buffer->_cookies[skip] > resume)
			break;

		c = c + ((struct dirent *)c)->d_reclen;
	}

	if (skip > 0) {
		log(NOTICE, "Skipping %d dirents from readdirplus", skip);
		memmove(buffer->_dirents, c, sizeof(buffer->_dirents) -
		    (c - buffer->_dirents));
		if (need_stat)
			memmove(buffer->_stats, &buffer->_stats[skip],
			    sizeof(*buffer->_stats) * (buffer->_count - skip));
		memmove(buffer->_cookies, &buffer->_cookies[skip],
		    sizeof(*buffer->_cookies) * (buffer->_count - skip));

		buffer->_count -= skip;
	}

	/* An empty buffer should really just return NULL instead. */
	if (buffer->_count == 0) {

		/*
		 * This ASSERT is meant to catch the case where we're skipping
		 * so many dirents, its likely that this isn't actually the end
		 * of the directory, but rather a really long streak of dirents
		 * in hash buckets that readdirplus shouldn't have been giving
		 * to us in the first place (bug 50892).
		 */
		ASSERT(skip == 0 || (buffer->_count + skip) <
		    (read_dirents_num / 2));

		free(buffer);
		buffer = NULL;
		goto out;
	}

	/*
	 * Dirents are packed into the buffer so this walks the compact format
	 * and sets up direct pointers to be indexable.
	 */
	for (i = 0, c = buffer->_dirents; i < buffer->_count; i++) {
		td = (struct dirent *)c;
		if (need_stat && buffer->_stats[i].st_ino == 0) {
			ret = enc_fstatat(fd, td->d_name, td->d_encoding,
			    buffer->_stats + i, AT_SYMLINK_NOFOLLOW);
			if (ret == -1) {
				/* 
				 * check for _g_expected_dataloss flag
				 * if _g_expected_dataloss set
				 * than ignore error.
				 */ 
				if (!_g_expected_dataloss) {
					error = isi_system_error_new(errno,
						"Failed stat after readdir");
					goto out;
				}
			}
		}
		populate_migr_dirent(buffer->_migr_dirents + i,
		    td, buffer->_stats + i, buffer->_cookies[i]);

		c = c + ((struct dirent *)c)->d_reclen;
	}

out:
	if (error && buffer) {
		free(buffer);
		buffer = NULL;
	}

	isi_error_handle(error, error_out);
	return buffer;
}

static void
read_next_buffer(struct migr_dir *dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir_buffer *buffer;

	/* Bug 203702 - quick path if resume cookie set to DIR_SLICE_MAX */
	if (dir->_resume == DIR_SLICE_MAX)
		goto out;

	buffer = read_buffer(dir, dir->_resume, &error);
	if (error)
		goto out;

	/*
	 * Bug 203702
	 * Set the resume cookie to DIR_SLICE_MAX when we get to EOF.
	 */
	if (buffer == NULL) {
		ASSERT_DEBUG(dir->_resume != DIR_SLICE_MAX);
		dir->_resume = DIR_SLICE_MAX;
		goto out;
	}

	/* Prepend the new buffer. */
	buffer->_next = dir->_buffers;
	if (dir->_buffers)
		dir->_buffers->_prev = buffer;
	dir->_buffers = buffer;

	dir->_resume = buffer->_cookies[buffer->_count - 1];
out:
	isi_error_handle(error, error_out);
}

static bool
has_next_dirent(const struct migr_dir_buffer *buffer)
{
	return buffer != NULL && buffer->_index < buffer->_count;
}

static bool
is_next_less_than(const struct migr_dir_buffer *buffer, uint64_t end)
{
	return buffer->_cookies[buffer->_index] <= end;
}

struct migr_dirent *
migr_dir_read(struct migr_dir *dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dirent *next = NULL;


	ASSERT(dir->_fd != -1);

	if (dir->_has_been_unread) {
		dir->_has_been_unread = false;
		return dir->_last_read_md;
	}

	if (!has_next_dirent(dir->_buffers)) {
		read_next_buffer(dir, &error);
		if (error)
			goto out;

		if (!has_next_dirent(dir->_buffers))
			goto out;
	}

	if (!is_next_less_than(dir->_buffers, dir->_slice.end))
		goto out;

	next = &dir->_buffers->_migr_dirents[dir->_buffers->_index];
	dir->_last_read_md = next;

	dir->_buffers->_index++;
	dir->_buffers->_ref_count++;

out:
	isi_error_handle(error, error_out);
	return next;
}

void
migr_dir_unread(struct migr_dir *dir)
{
	/* Can't do more than one unread at a time */
	ASSERT(!dir->_has_been_unread);
	dir->_has_been_unread = true;
}

void
migr_dir_unref(struct migr_dir *dir, struct migr_dirent *dirent)
{
	struct migr_dir_buffer *buffer;

	ASSERT(dir->_buffers);

	for (buffer = dir->_buffers; buffer; buffer = buffer->_next) {
		/*
		 * This checks to see if the dirent in question belongs to the
		 * array of dirents stored by this buffer.
		 */
		if (dirent >= buffer->_migr_dirents &&
		    dirent < buffer->_migr_dirents + buffer->_count) {
			/* 
			 * Reference tracking is done on a buffer level
			 * granularity.  So individual double unrefs won't be
			 * detected until the ref count goes negative and
			 * missing frees won't be detected until freeing the
			 * dir.  Here is the check for double frees.
			 */
			ASSERT(buffer->_ref_count != 0);
			buffer->_ref_count--;
			break;
		}
	}

	ASSERT(buffer != NULL);

	/* Advance the pointer to the tail for cleanup */
	while (buffer->_next)
		buffer = buffer->_next;

	/*
	 * Cleans up the tail end of the queue if possible.  Note that one
	 * buffer is always kept around to track last read cookie.  Only
	 * buffers at the end are freed, making out of order unrefs wait until
	 * the last buffer is completely free to act.
	 */
	while (buffer->_ref_count == 0 && buffer->_next == NULL &&
	    buffer->_prev != NULL) {

		if (buffer->_count > 0) {
			dir->last_unref_cookie =
			    buffer->_cookies[buffer->_count - 1];
		}
		buffer = buffer->_prev;
		free(buffer->_next);
		buffer->_next = NULL;
	}
}

static off_t
base_key(uint32_t hash)
{
	return dfm_key(hash, 0);
}

enum migr_split_result
migr_dir_split(struct migr_dir *dir, struct dir_slice *slice,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir_buffer *buffer = NULL;
	enum migr_split_result result = MIGR_SPLIT_UNAVAILABLE;
	bool was_frozen;
	uint64_t lower, upper, mid,
	    next_avail, real_next_avail;

	was_frozen = migr_dir_thaw(dir, &error);
	if (error)
		goto out;

	/*
	 * Determine the current working item.  If no buffer is available, this
	 * is the lower bound of the directory slice and we're unable to split.
	 */
	if (dir->_buffers == NULL || dir->_buffers->_index < 1) {
		result = MIGR_SPLIT_UNKNOWN;
		goto out;
	}

	lower = dir->_buffers->_cookies[dir->_buffers->_index - 1];
	upper = dir->_slice.end;

	/*
	 * Bug 189993
	 * Start at lower bound in case there are directories with colliding
	 * name hashes.
	 */
	buffer = read_buffer(dir, lower, &error);

	/*
	 * An error is an obvious reason to return, but no buffer simply
	 * implies that nothing is actually splittable since there's no dirents
	 * beyond the current hash.
	 */
	if (error || buffer == NULL)
		goto out;

	next_avail = buffer->_cookies[0];

	free(buffer);
	buffer = NULL;

	if (next_avail > dir->_slice.end)
		goto out;

	/*
	 * Keep splitting the upper hash space in half until we find a half
	 * that contains values.
	 */
	for (;;) {
		/*
		 * Find the _hash_ half way between the upper and lower.  This
		 * ensures that we don't split individual hash buckets.
		 */
		mid = base_key((dfm_key2hash(upper) + dfm_key2hash(lower)) / 2);

		/*
		 * If our theoretical half way split is less than the next
		 * available dirent, then break since we can't get a more
		 * optimal split than the next dirent.
		 */
		if (mid <= next_avail)
			break;

		/* Testing for an actual dirent at the theoretical split. */
		buffer = read_buffer(dir, mid, &error);
		if (error)
			goto out;

		if (buffer && buffer->_cookies[0] <= upper)
			break;

		if (buffer) {
			free(buffer);
			buffer = NULL;
		}

		/*
		 * At this point, there was no dirents at the theoretical hash
		 * split, so eliminate upper half of the search and try the
		 * split again.
		 */
		upper = mid + 1;
	}

	ASSERT(next_avail > dir->_slice.begin);

	/* Fill in the slice with the actual split information. */
	result = MIGR_SPLIT_SUCCESS;
	real_next_avail = (buffer ? buffer->_cookies[0] : next_avail) - 1;
	ASSERT(real_next_avail > dir->_slice.begin);
	slice->begin = real_next_avail;
	slice->end = dir->_slice.end;

	dir->_slice.end = real_next_avail;

out:
	if (!error && was_frozen)
		migr_dir_freeze(dir);

	if (buffer)
		free(buffer);
	isi_error_handle(error, error_out);
	return result;
}

bool
migr_dir_freeze(struct migr_dir *dir)
{
	if (dir->_fd == -1)
		return false;

	close(dir->_fd);
	dir->_fd = -1;
	return true;
}

bool
migr_dir_thaw(struct migr_dir *dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool thawed = false;

	if (dir->_fd != -1)
		goto out;

	dir->_fd = ifs_lin_open(dir->_stat.st_ino, dir->_stat.st_snapid,
	    O_RDONLY);
	if (dir->_fd == -1) {
		error = isi_system_error_new(errno, "Failed to thaw dir");
		goto out;
	}

	thawed = true;
out:
	isi_error_handle(error, error_out);
	return thawed;
}

void
migr_dir_free(struct migr_dir *dir)
{
	struct migr_dir_buffer *tmp, *buffer;

	/* Bug 259765 - add a NULL check to short circuit */
	if (dir == NULL)
		return;

	buffer = dir->_buffers;

	while (buffer) {
		/*
		 * By this time all dirents should be unrefed and the only
		 * buffer left should have a zero ref count.
		 * OR refcount is 1 and we have unread the dirent
		 */
		ASSERT_CONT(buffer->_ref_count == 0
		    || (buffer->_ref_count == 1 && dir->_has_been_unread));
		tmp = buffer;
		buffer = buffer->_next;
		free(tmp);
	}

	if (dir->name)
		free(dir->name);

	if (dir->_fd != -1)
		close(dir->_fd);
	free(dir);
}

/* Return the cookie that is one before the given cookie. Return -1 if the
 * given cookie isn't found. Return dir->_slice.begin if there is no previous
 * cookie. */
uint64_t
migr_dir_get_prev_cookie(struct migr_dir *dir, uint64_t cookie)
{
	struct migr_dir_buffer *buffer = NULL;
	bool found = false;
	uint64_t result = -1;
	int i = 0;

	if (dir->_buffers == NULL)
		goto out;

	/* Iterate all cookies from highest to lowest looking for the given
	 * cookie. Stop if we get to a cookie < the given cookie, as there is
	 * no chance of finding it after that point */
	for (buffer = dir->_buffers; buffer; buffer = buffer->_next) {
		if (buffer->_count == 0)
			continue;

		if (buffer->_cookies[0] > cookie)
			continue;

		for (i = buffer->_count - 1; i >= 0; i--) {
			if (buffer->_cookies[i] == cookie) {
				found = true;
				break;
			}

			if (buffer->_cookies[i] < cookie)
				goto out;
		}
		if (found)
			break;
	}

	if (!found)
		goto out;

	/* If here, we're guaranteed i is a valid index in a valid buffer.
	 * Return the next lowest cookie, or the beginning of the slice if the
	 * given cookie is the lowest cookie. */
	if (i == 0) {
		buffer = buffer->_next;
		if (buffer == NULL || buffer->_count == 0) {
			if (dir->last_unref_cookie != 0) {
				result = dir->last_unref_cookie;
			} else {
				result = dir->_slice.begin;
			}
		} else {
			result = buffer->_cookies[buffer->_count - 1];
		}
	} else {
		result = buffer->_cookies[i - 1];
	}

out:
	return result;
}

void
print_migr_dir(FILE *f, struct migr_dir *mdir, char *indent)
{
	if (!mdir) {
		fprintf(f, "%sNULL\n", indent);
		return;
	}
	fprintf(f, "%s_fd: %d\n", indent, mdir->_fd);
	fprintf(f, "%sslice begin: %llx\n", indent, mdir->_slice.begin);
	fprintf(f, "%sslice end: %llx\n", indent, mdir->_slice.end);
	fprintf(f, "%slin: %llx\n", indent, mdir->_stat.st_ino);
	fprintf(f, "%s_checkpoint: %llx\n", indent, mdir->_checkpoint);
	fprintf(f, "%s_selected: %s\n", indent, PRINT_BOOL(mdir->selected));
	fprintf(f, "%s_name: %s\n", indent, mdir->name);
}

static void
migr_tw_thaw(struct migr_tw *tw, struct migr_dir *dir,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool thawed;

	thawed = migr_dir_thaw(dir, &error);
	if (error)
		goto out;

	if (thawed)
		tw->_thawed_dirs++;
out:
	isi_error_handle(error, error_out);
}

static void
migr_tw_freeze(struct migr_tw *tw, struct migr_dir *dir)
{
	if (migr_dir_freeze(dir))
		tw->_thawed_dirs--;
}

static void
migr_tw_append_dir(struct migr_tw *tw, struct migr_dir *dir)
{
	int i;

	ASSERT(tw->_thawed_dirs <= pvec_size(&tw->_dirs));

	tw->_thawed_dirs++;

	pvec_append(&tw->_dirs, dir);

	if (tw->_thawed_dirs < MIGR_THAW_HIGH_WATER)
		return;


	/* 
	 * Freeze dirs that are somewhere in the middle of the dir vector to
	 * avoid freezing dirs with locality to what we're currently using.
	 */
	i = pvec_size(&tw->_dirs) - MIGR_THAW_SKIP;

	while (tw->_thawed_dirs >= MIGR_THAW_LOW_WATER) {

		/* 
		 * Already prevented underflow via the invariants defined with
		 * the constants.  Notably, HIGH > SKIP + HIGH - LOW.
		 */
		ASSERT(i > 0);

		dir = tw->_dirs.ptrs[i--];

		migr_tw_freeze(tw, dir);
	}
}

static void
migr_tw_init_base(struct migr_tw *tw, struct path *path, char *selectors)
{
	path_copy(&tw->_path, path);
	pvec_init(&tw->_dirs);

	tw->_select_tree = (selectors) ? sel_import(selectors) : NULL;
	tw->_thawed_dirs = 0;
}

void
migr_tw_init(struct migr_tw *tw, int parent_fd, struct path *path,
    struct dir_slice *slice, char *selectors, bool full_transfer,
    bool resolve_path, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;
	int dirfd;

	log(TRACE, "migr_tw_init");

	migr_tw_init_base(tw, path, selectors);
	tw->full_transfer = full_transfer;

	if (resolve_path) {
		dirfd = fd_rel_to_ifs(parent_fd, path);
		if (dirfd == -1) {
			error = isi_system_error_new(errno,
			    "Failed to get fd for dir "
			    "%s under ifs fd %d (migr_tw_init)",
			    path->path, parent_fd);
			goto out;
		}
	} else {
		dirfd = parent_fd;
	}

	dir = migr_dir_new(dirfd, slice, true, &error);
	if (error)
		goto out;

	migr_tw_append_dir(tw, dir);

	dir->_select_tree = tw->_select_tree;
	dir->selected = dir->_select_tree ? dir->_select_tree->select : true;

out:
	isi_error_handle(error, error_out);
}

/**
 * A helper function for reading the format of a uint32 signifying the number
 * of following elements of size specified as the width parameter.
 *
 * Only has a binary failure.  Not really interested in the specific decoding
 * error since the file can be considered corrupted and it may have been
 * something earlier in the format that led to this particular error.
 */
static bool
get_length_plus_array(struct isi_buf_istream *bis, int width, void **dataout,
    int *sizeout)
{
	struct isi_istream *is;
	uint32_t size = 0;
	void *data = NULL;
	bool finished = false;

	is = &bis->super;

	if (!isi_istream_get_bytes(is, &size, sizeof(size)))
		goto out;

	/*
	 * A small sanity check to make sure we don't try to allocate a
	 * massive chunk of memory just based on a bad field.
	 */
	if (size * width > isi_buf_istream_left(bis))
		goto out;

	/* No reason to allocate memory or read anything in the zero case. */
	if (size == 0) {
		finished = true;
		goto out;
	}

	data = malloc(size * width);
	ASSERT(data);

	if (!isi_istream_get_bytes(is, data, size * width))
		goto out;

	finished = true;
out:
	if (finished) {
		*dataout = data;
		if (sizeout)
			*sizeout = size;
	} else
		free(data);

	return finished;
}

/**
 * Helper for the basic reading and decoding of serialized format.
 */
bool
unserialize_tw_data(void *data, size_t size, struct path *path,
    bool *full_transfer, char **selectors, struct migr_slice_od **slices,
    int *numslices)
{
	struct isi_buf_istream bis;
	struct isi_istream *is;
	struct enc_vec ENC_VEC_INIT_CLEAN(evec);
	uint32_t magic, flags;
	bool finished = false;
	char *raw_path = NULL, *evec_array = NULL;
	int s;

	/*
	 * Using the isi_buf_istream interface to do the pointer math of
	 * reading pieces out of the buffer.
	 */
	isi_buf_istream_init(&bis, data, size);
	is = &bis.super;

	if (!isi_istream_get_bytes(is, &magic, sizeof(magic)))
		goto out;

	if (magic != MIGR_TW_SERIALIZE_MAGIC)
		goto out;

	if (!get_length_plus_array(&bis, 1, (void **)&raw_path, NULL))
		goto out;

	if (!get_length_plus_array(&bis, sizeof(*evec.encs),
	    (void **)&evec_array, &s))
		goto out;

	enc_vec_copy_from_array(&evec, (enc_t *)evec_array, s);

	if (!isi_istream_get_bytes(is, &flags, sizeof(flags)))
		goto out;

	*full_transfer = flags;

	if (!get_length_plus_array(&bis, 1, (void **)selectors, NULL))
		goto out;

	if (!get_length_plus_array(&bis, sizeof(**slices), (void **)slices,
	    numslices))
		goto out;

	path_init(path, raw_path, &evec);

	finished = true;
out:
	free(raw_path);
	free(evec_array);
	return finished;
}

void
migr_tw_init_from_data(struct migr_tw *tw, void *data, size_t size,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct path path = {};
	struct migr_slice_od *slices = NULL;
	struct migr_dir *dir;
	struct migr_dirent *de;
	struct select *select;
	char *selectors = NULL, *sel_path = NULL, *curr, *next = NULL;
	int s, i, fd = -1;
	bool full_transfer = false;

	memset(tw, 0, sizeof(*tw));

	if (!unserialize_tw_data(data, size, &path, &full_transfer,
	    &selectors, &slices, &s)) {
		error = isi_system_error_new(EINVAL, "Failed to unserialize "
		    "treewalk progress");
		goto out;
	}

	/* if this is a recovery item from before .snapshot paths use was
	 * eliminated, need to convert back to a normal path */
	snap_path_to_normal_path(&path);

	migr_tw_init_base(tw, &path, selectors);
	tw->full_transfer = full_transfer;

	/*
	 * The sel_path is going to used for walking the select tree to setup
	 * the directories into the right place.  To do so it needs to start
	 * at the first directory referenced by the selectors.  The selectors
	 * only pertain to directories with slices; hence the usage of
	 * num_evecs - num_slices boundary condition.
	 */
	sel_path = strdup(path.path);
	curr = sel_path + 1;
	for (i = 0; i < enc_vec_size(&path.evec) - s + 1; i++) {
		curr = strchr(curr, '/');
		if (curr) {
			ASSERT(*curr);
			*curr++ = 0;
		} else
			ASSERT(i == enc_vec_size(&path.evec) - s);
	}

	/*
	 * Reconstructs all the migr_dirs for the treewalker.
	 */
	select = tw->_select_tree;
	for (i = 0; i < s; i++) {
		struct dir_slice slice;

		fd = ifs_lin_open(slices[i].lin, slices[i].snapid, O_RDONLY);
		if (fd < 0) {
			/**
			 * Bug 172633
			 * If the treewalk is on HEAD, then ignore removed
			 * directories
			 */
			if (slices[i].snapid == HEAD_SNAPID && errno == ENOENT)
				continue;
			error = isi_system_error_new(errno, "Failed to open "
			    "serialized directory");
			goto out;
		}

		/* Initialize the slice begin to read the last checkpoint. */
		slice.begin = slices[i].begin;
		slice.end = slices[i].end;

		dir = migr_dir_new(fd, &slice, true, &error);
		if (error) {
			ASSERT(dir == NULL);
			close(fd);
			goto out;
		}

		migr_tw_append_dir(tw, dir);
		dir->_select_tree = select;
		dir->selected = select ? select->select : true;

		/* Prep the dir object to start reading at the checkpoint. */
		dir->_resume = dir->_checkpoint = dir->prev_sent_cookie = 
		    slices[i].checkpoint;

		/*
		 * If the checkpoint is at some point in the middle of slice,
		 * the migr_dir needs to be pre-loaded with a dir_buffer for
		 * the split algorithm to work properly.
		 */
		if (dir->_checkpoint != dir->_slice.begin) {

			/*
			 * The checkpoint should never be outside the dir
			 * slice, but just make sure.
			 */
			ASSERT(dir->_checkpoint > dir->_slice.begin);

			/* 
			 * Back up the resume key to actually include the
			 * checkpointed dirent.
			 */
			dir->_resume--;

			de = migr_dir_read(dir, &error);
			if (error)
				goto out;

			if (de == NULL) {
				/*
				 * Bug 188996
				 * If de is NULL, we must be on HEAD.
				 */
				ASSERT(slices[i].snapid == HEAD_SNAPID);
			} else {
				/*
				 * Bug 172633
				 * If we are reading from a snapshot, then
				 * we better have read the checkpointed entry.
				 */
				if (de->cookie == slices[i].checkpoint)
					migr_dir_unref(dir, de);
				else {
					ASSERT(
					    de->cookie > slices[i].checkpoint
					    &&
					    slices[i].snapid == HEAD_SNAPID);
					/*
					 * Bug 188996
					 * If we're on HEAD and the checkpoint
					 * entry has been deleted, then we
					 * need to process the found directory
					 * entry.
					 */
					migr_dir_unread(dir);
				}
			}
		}

		if (curr) {
			/* Walks the appropriate branch of the select tree. */
			next = strchr(curr, '/');
			if (next)
				*next = 0;

			select = sel_find(select, curr);
			curr = next ? next + 1 : NULL;
		} else {
			/* 
			 * End of the string denotes the last path element.
			 * There's no select object for the next, nonexistent
			 * path element.
			 */
			select = NULL;
		}
	}

out:
	if (error)
		migr_tw_clean(tw);

	path_clean(&path);
	free(selectors);
	free(sel_path);
	free(slices);
	isi_error_handle(error, error_out);
}

struct migr_dir *
migr_tw_get_dir(struct migr_tw *tw, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;

	ASSERT(pvec_size(&tw->_dirs) != 0);
	dir = tw->_dirs.ptrs[pvec_size(&tw->_dirs) - 1];

	ASSERT(dir);

	migr_tw_thaw(tw, dir, &error);

	isi_error_handle(error, error_out);
	return dir;
}

bool
migr_tw_get_work_desc(struct migr_tw *tw, struct migr_work_desc *wd)
{
	struct migr_dir *top;

	if (pvec_empty(&tw->_dirs))
		return false;

	top = tw->_dirs.ptrs[0];

	wd->lin = top->_stat.st_ino;
	wd->slice = top->_slice;

	return true;
}

bool
migr_tw_push(struct migr_tw *tw, struct migr_dirent *d,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *new_dir, *cur_dir;
	int at_fd, new_fd;
	struct select *new_select_tree = NULL;
	bool new_dir_selected;
	bool result = false;

	cur_dir = migr_tw_get_dir(tw, &error);
	if (error)
		goto out;

	ASSERT(cur_dir);

	at_fd = migr_dir_get_fd(cur_dir);

	/* XXXJDH: FIX ERRORS */
	new_fd = enc_openat(at_fd, d->dirent->d_name, d->dirent->d_encoding,
	    O_RDONLY);
	if (new_fd == -1) {
		log(ERROR, "%s: Failed to open dir %s: %s (%d)", __func__,
		    d->dirent->d_name, strerror(errno), errno);
		log(ERROR, "%s:    at_fd=%d. cur_dir name=%s, lin=%llx "
		    "snapid=%llu",__func__, at_fd, cur_dir->name,
		    cur_dir->_stat.st_ino, cur_dir->_stat.st_snapid);
		/**
		 * Bug 172633
		 * If the treewalk is on HEAD, then ignore removed
		 * directories
		 */
		if (errno != ENOENT || cur_dir->_stat.st_snapid != HEAD_SNAPID)
			error = isi_system_error_new(errno,
			    "Failed to open dir %s", d->dirent->d_name);
		goto out;
	}

	/*
	 * find subtree within select_tree corresponding to the new dir. if
	 * none exists, inherit the parent's selection value
	 */
	new_select_tree = sel_find(cur_dir->_select_tree, d->dirent->d_name);
	new_dir_selected = new_select_tree ?
	    new_select_tree->select : cur_dir->selected;

	/*
	 * if the new directory isn't selected, and there is no selection
	 * subtree associated with it, then nothing within it is selected and
	 * we can skip the directory. if it isn't selected but it has a select
	 * subtree which has entry which is selected then we must enter it
	 * since it contains selected child directories
	 */
	if (!new_dir_selected &&
	    (!new_select_tree || !has_selected_node(new_select_tree->child))) {
		close(new_fd);
		goto out;
	}

	new_dir = migr_dir_new(new_fd, NULL, true, &error);
	if (error) {
		close(new_fd);
		goto out;
	}

	migr_tw_append_dir(tw, new_dir);
	new_dir->_select_tree = new_select_tree;
	new_dir->selected = new_dir_selected;
	free(new_dir->name);
	new_dir->name = strdup(d->dirent->d_name);

	log(TRACE, "%s: path %s gets pushed with %s", __func__, tw->_path.path,
	    new_dir->name);

	path_add(&tw->_path, d->dirent->d_name, d->dirent->d_namlen,
	    d->dirent->d_encoding);

	result = true;

out:
	isi_error_handle(error, error_out);
	return result;
}

bool
migr_tw_pop(struct migr_tw *tw, struct migr_dir *dir)
{
	struct migr_dir *pop_dir;
	bool pop;

	log(TRACE, "%s: dlin=%llx, popping %s, removing %s",
	    __func__, dir->_stat.st_ino, dir->name, tw->_path.path);

	path_rem(&tw->_path, 0);

	pop = pvec_pop(&tw->_dirs, (void **)&pop_dir);
	ASSERT(pop);
	ASSERT(pop_dir == dir);

	/* To keep accounting proper, we can just attempt to freeze the dir. */
	migr_tw_freeze(tw, dir);

	migr_dir_free(dir);

	return !pvec_empty(&tw->_dirs);
}

enum migr_split_result
migr_tw_split(struct migr_tw *tw, struct path *path, struct migr_work_desc *wd,
    struct migr_dir **split_dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct path tmp_path = {};
	enum migr_split_result result = MIGR_SPLIT_UNAVAILABLE;
	int i, j;

	ASSERT(split_dir);
	*split_dir = NULL;

	log(TRACE, "migr_tw_split");

	/*
	 * Tree splitting works by starting with the top level directory in the
	 * iteration and doing a directory split.  If the top level split
	 * fails, continue down to the next level of directory and try again.
	 * If all directories in the treewalk fail, then the tree must be
	 * exhausted of splits.
	 */
	for (i = 0; i < pvec_size(&tw->_dirs); i++) {
		path_clean(&tmp_path);
		path_copy(&tmp_path, &tw->_path);

		/* Build the new possible split path. */
		for (j = 0; j < pvec_size(&tw->_dirs) - i - 1; j++)
			path_rem(&tmp_path, 0);

		/* Try splitting at this directory level */
		result = migr_dir_split(tw->_dirs.ptrs[i], &wd->slice, &error);
		if (error)
			goto out;
		if (result == MIGR_SPLIT_SUCCESS) {
			path_copy(path, &tmp_path);
			*split_dir = tw->_dirs.ptrs[i];
			wd->lin = (*split_dir)->_stat.st_ino;

			goto out;
		} else if (result == MIGR_SPLIT_UNKNOWN) {
			/* 
			 * The unknown state should only be reached when we've
			 * just entered a directory and hence it being the
			 * lowest level.
			 */
			ASSERT(i == pvec_size(&tw->_dirs) - 1);
		}
	}

out:
	path_clean(&tmp_path);
	isi_error_handle(error, error_out);
	return result;
}

void
migr_tw_clean(struct migr_tw *tw)
{
	int i;
	for (i = 0; i < pvec_size(&tw->_dirs); i++)
		migr_dir_free(tw->_dirs.ptrs[i]);
	path_clean(&tw->_path);
	pvec_clean(&tw->_dirs);

	/* 
	 * Sel_free isn't used here because all select structures within the
	 * pworker come from sel_import.  Sel_import does not create multiple
	 * structures that all need to be freed.
	 */
	free(tw->_select_tree);
	tw->_select_tree = NULL;
}

struct migr_dir *
migr_tw_find_dir(struct migr_tw *tw, ifs_lin_t dlin)
{
	void **dir;

	PVEC_FOREACH(dir, &tw->_dirs) {
		if ((*(struct migr_dir **)dir)->_stat.st_ino == dlin)
			return *dir;
	}

	return NULL;
}

/**
 * The serialization format for the treewalk contains the following:
 *    - Four byte magic number
 *    - uint32_t length of the full path including NUL character
 *    - Full path in native encodings followed by a NUL character
 *    - uint32 length encoding array
 *    - Encodings as uint32
 *    - uint32 of flags (only full transfer right now)
 *    - uint32 length of the encoded select tree
 *    - The sel_export of the select tree.
 *    - uint32 length of the LIN/dir key list.
 *    - List of LIN and snapid plus begin and end dir keys written as 4 uint64s
 */
void *
migr_tw_serialize(struct migr_tw *tw, size_t *outsize)
{
	struct isi_malloc_ostream mos;
	struct isi_ostream *os;
	uint32_t magic = MIGR_TW_SERIALIZE_MAGIC;
       	int size;
	void *select_export;
	int count, i;

	isi_malloc_ostream_init(&mos, NULL);
	os = &mos.super;

	isi_ostream_put_bytes(os, &magic, sizeof(magic));

	/*
	 * This is the number of directories that include any checkpointing and
	 * and should be include in the serialization.
	 */
	count = pvec_size(&tw->_dirs);

	size = tw->_path.used + 1;
	isi_ostream_put_bytes(os, &size, sizeof(size));
	isi_ostream_put_bytes(os, tw->_path.path, size);

	size = enc_vec_size(&tw->_path.evec);
	isi_ostream_put_bytes(os, &size, sizeof(size));
	isi_ostream_put_bytes(os, tw->_path.evec.encs,
	    enc_vec_size(&tw->_path.evec) * sizeof(*tw->_path.evec.encs));

	size = tw->full_transfer ? 1 : 0;
	isi_ostream_put_bytes(os, &size, sizeof(size));

	if (tw->_select_tree) {
		select_export = sel_export(tw->_select_tree, &size);
		isi_ostream_put_bytes(os, &size, sizeof(size));
		isi_ostream_put_bytes(os, select_export, size);
		free(select_export);
	} else {
		size = 0;
		isi_ostream_put_bytes(os, &size, sizeof(size));
	}

	size = count;
	isi_ostream_put_bytes(os, &size, sizeof(size));

	for (i = 0; i < count; i++) {
		struct migr_slice_od sod;
		struct migr_dir *dir = tw->_dirs.ptrs[i];

		sod.lin = dir->_stat.st_ino;
		sod.snapid = dir->_stat.st_snapid;
		sod.begin = dir->_slice.begin;
		sod.end = dir->_slice.end;
		sod.checkpoint = dir->_checkpoint;

		isi_ostream_put_bytes(os, &sod, sizeof(sod));
	}

	if (outsize)
		*outsize = isi_malloc_ostream_used(&mos);

	return isi_malloc_ostream_detach(&mos);
}

bool
migr_tw_has_slices(struct migr_tw *tw)
{
	return (!pvec_empty(&tw->_dirs));
}

/* Returns true if the given dir has any unread dirents, false otherwise.
 * 'dir' is not modified by this function, it is just peeking at its contents
 */
static bool
migr_dir_has_dirent(struct migr_dir *dir, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dirent *next = NULL;
	struct migr_dir_buffer *buffer = dir->_buffers;
	bool res = false, was_frozen, allocated_buffer = false;

	was_frozen = migr_dir_thaw(dir, &error);
	if (error)
		goto out;

	ASSERT(dir->_fd != -1);

	/* If the current buffer doesn't have another dirent, read more
	 * entries from the dir and see if we find another dirent */
	if (!has_next_dirent(buffer)) {
		buffer = read_buffer(dir, dir->_resume, &error);
		if (error || buffer == NULL)
			goto out;

		allocated_buffer = true;
		if (!has_next_dirent(buffer))
			goto out;
	}

	/* We found another dirent, now make sure it's in our slice */
	if (!is_next_less_than(buffer, dir->_slice.end))
		goto out;

	res = true;

out:
	if (was_frozen)
		migr_dir_freeze(dir);

	if (allocated_buffer && buffer)
		free(buffer);

	isi_error_handle(error, error_out);
	return res;
}

/* Iterate this treewalk context's dirs and look for an unread dirent.
 * Return true if one is found, false otherwise. */
bool
migr_tw_has_unread_dirents(struct migr_tw *tw, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int i = 0;
	bool result = false;

	for (i = 0; i < pvec_size(&tw->_dirs); i++) {
		result = migr_dir_has_dirent(tw->_dirs.ptrs[i], &error);
		if (error)
			goto out;
		if (result)
			goto out;
	}

out:
	isi_error_handle(error, error_out);
	return result;
}

/* Find the last migr_work_desc in the tw stack. */
void
get_last_migr_work(struct migr_tw *tw, struct migr_work_desc *wd,
    char **selectbuf, int *selectlen, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *md = NULL;
	int i;
	bool was_frozen;

	i = pvec_size(&tw->_dirs) - 1;
	md = tw->_dirs.ptrs[i];
	was_frozen = migr_dir_thaw(md, &error);
	if (error)
		goto out;
	ASSERT(wd);
	bzero(wd, sizeof(struct migr_work_desc));
	wd->slice = md->_slice;
	wd->lin = md->_stat.st_ino;
	if (md->_select_tree)
		*selectbuf = sel_export(md->_select_tree,
		    selectlen);
out:
	if (was_frozen)
		migr_dir_freeze(md);

	isi_error_handle(error, error_out);
}

