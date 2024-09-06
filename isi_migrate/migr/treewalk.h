#ifndef TREEWALK_H
#define TREEWALK_H

#include <dirent.h>
#include <stdint.h>

#include <sys/isi_macro.h>

#include <isi_migrate/migr/isirep.h>
#include <isi_util/isi_error.h>

/** 
 * Used to describe a subset of a directory's contents as defined by directory
 * keys.  The beginning and end are non-inclusive and inclusive, respectively.
 */
struct dir_slice {
	uint64_t begin;
	uint64_t end;
};

/**
 * Describes a unique piece of work within a running treewalk.
 */
struct migr_work_desc {
	ifs_lin_t		lin;
	struct dir_slice	slice;
};


/* XXXJDH: the DFM should probably export SKIP_VIRTUAL. */
#define DIR_SLICE_MIN (1000)
#define DIR_SLICE_MAX (INT64_MAX)
#define DIR_SLICE_INITIALIZER { DIR_SLICE_MIN, DIR_SLICE_MAX }

struct fmt_conv_ctx dir_slice_fmt(const struct dir_slice *);

struct migr_dirent {
	struct dirent	*dirent;
	struct stat	*stat;
	uint64_t	 cookie;
};

#define READ_DIRENTS_NUM 100
#define READ_DIRENTS_SIZE (READ_DIRENTS_NUM * sizeof(struct dirent))
/* readdirplus will skip "." and ".." when called with resume_cookie 1000 */
#define READ_DIRENTS_MIN_COOKIE 1000

/* set as true when expected_dataloss_flag is set*/
extern bool _g_expected_dataloss;
/* 
 * set as true when directory read fails i.e. when entire 
 * directory is screwed up. (e.g. readdirplus () fails)
 */
extern bool _g_directory_corrupted;

/*  ____  _       __        __    _ _             
 * |  _ \(_)_ __  \ \      / /_ _| | | _____ _ __ 
 * | | | | | '__|  \ \ /\ / / _` | | |/ / _ \ '__|
 * | |_| | | |      \ V  V / (_| | |   <  __/ |   
 * |____/|_|_|       \_/\_/ \__,_|_|_|\_\___|_|   
 */

enum migr_split_result {
	MIGR_SPLIT_SUCCESS,
	MIGR_SPLIT_UNAVAILABLE,
	MIGR_SPLIT_UNKNOWN,
};

struct migr_dir_buffer {
	char			 _dirents[READ_DIRENTS_SIZE];
	struct stat		 _stats[READ_DIRENTS_NUM];
	uint64_t		 _cookies[READ_DIRENTS_NUM];

	struct migr_dirent	 _migr_dirents[READ_DIRENTS_NUM];

	unsigned int		 _count;
	int			 _index;
	int			 _ref_count;
	struct migr_dir_buffer	*_next;
	struct migr_dir_buffer	*_prev;
};

struct migr_dir {
	int			 _fd;
	struct dir_slice	 _slice;

	struct stat		 _stat;

	uint64_t		 _resume;
	uint64_t		 _checkpoint;
	bool			 _need_stat;
	struct migr_dir_buffer	*_buffers;

	bool			 _has_been_unread;
	struct migr_dirent	*_last_read_md;

	struct select		*_select_tree;
	bool			 selected;
	char			*name;

	bool			 is_new_dir;
	bool			 parent_is_new_dir;
	int			 visits;
	uint64_t		 prev_sent_cookie;
	uint64_t		 last_unref_cookie;
};

/**
 * Creates a new migr_dir object.  The fd is owned by the migr_dir object
 * after this call and will be closed as part of the migr_dir_free.
 */
struct migr_dir *migr_dir_new(int fd, const struct dir_slice *,
    bool need_stat, struct isi_error **);

/**
 * Read the next entry in the dir.  The entry is available until
 * migr_dir_unref is called on it.
 */
struct migr_dirent *migr_dir_read(struct migr_dir *, struct isi_error **);

/**
 * Unread what was just read from migr_dir_read(). That is, next time
 * migr_dir_read() is called, return the last read value.
 */
void migr_dir_unread(struct migr_dir *dir);

/** 
 * Unreferences the migr_dirent to allow the underlying storage to free
 * resources associated with the entry.  
 */
void migr_dir_unref(struct migr_dir *, struct migr_dirent *);

/**
 * Finds a split of the directory, internally updating the remaining slice
 * within the migr_dir structure and return the other half of the split in the
 * dir_slice.  MIGR_SPLIT_SUCCESS is returned if a split was found, otherwise
 * MIGR_SPLIT_UNKNOWN if nothing would be left for the current dir or
 * MIGR_SPLIT_UNAVAILABLE if no split was available.
 */
enum migr_split_result migr_dir_split(struct migr_dir *, struct dir_slice *,
    struct isi_error **);

/**
 * Closes the migr_dir's file descriptor.  If the FD was actually open, true is
 * returned, otherwise false is returned.
 */
bool migr_dir_freeze(struct migr_dir *);

/**
 * Attempts to re-open the migr_dir's FD.  If the FD was not open and it was
 * successfully opened true is returned, otherwise false is returned.
 */
bool migr_dir_thaw(struct migr_dir *, struct isi_error **);

/** Returns true if this dir object will read the last dirent in the dir. */
inline static bool migr_dir_is_end(struct migr_dir *dir)
{
	return dir->_slice.end == DIR_SLICE_MAX;
}

/**
 * Just updates the checkpoint within the directory.  The checkpoint is used
 * when serializing the known good state.
 */
inline static void migr_dir_set_checkpoint(struct migr_dir *dir, uint64_t new)
{
	ASSERT(new > dir->_checkpoint, "lin=%{} %016llx > %016llx",
	    lin_fmt(dir->_stat.st_ino), new, dir->_checkpoint);
	dir->_checkpoint = new;
}

inline static void migr_dir_set_prev_sent_cookie(struct migr_dir *dir)
{
	dir->prev_sent_cookie = dir->_buffers->_cookies[dir->_buffers->_index-1];
}

/** Simple getter for the checkpoint. */
DEF_GET(migr_dir_get_checkpoint, migr_dir, _checkpoint);
DEF_TRANSLATE(migr_dir_stat, migr_dir_stat_nc, migr_dir, _stat);

/**
 * Free all resources associated with a given migr_dir.  All dirents should be
 * unreferenced individually before calling this.
 *
 * XXXJDH: If a referenced dirent still exists an ASSERT_CONT will be triggered
 * right now.  This should probably be less of an error case in the future, but
 * it keeps people honest when using the API right now.
 */
void migr_dir_free(struct migr_dir *);


/* Return the cookie that is one before the given cookie in the given dir, or:
 * -1 if the given cookie isn't found.
 * dir->_slice.begin if the given cookie is the lowest cookie in the dir.
 */
uint64_t
migr_dir_get_prev_cookie(struct migr_dir *dir, uint64_t cookie);

/* Debugging output for migr_dir structs */
void print_migr_dir(FILE *f, struct migr_dir *mdir, char *indent);

/** Simple getter function for the file descriptor of the directory. */
DEF_GET(migr_dir_get_fd, migr_dir, _fd);

/*  _____               __        __    _ _             
 * |_   _| __ ___  ___  \ \      / /_ _| | | _____ _ __ 
 *   | || '__/ _ \/ _ \  \ \ /\ / / _` | | |/ / _ \ '__|
 *   | || | |  __/  __/   \ V  V / (_| | |   <  __/ |   
 *   |_||_|  \___|\___|    \_/\_/ \__,_|_|_|\_\___|_|   
 */

struct migr_tw {
	struct path		_path;

	struct ptr_vec		_dirs;

	struct select		*_select_tree;

	int			 _thawed_dirs;

	bool			 full_transfer;
};

/**
 * Fresh initialization functions.  Opens the path and assumes the slice
 * applies to base directory.
 */
void migr_tw_init(struct migr_tw *tw, int ifs_fd, struct path *path,
    struct dir_slice *slice, char *select, bool full_transfer,
    bool resolve_path, struct isi_error **error_out);

/**
 * Initializes a treewalker from one previously serialized by
 * migr_tw_serialize.  Note that the serialized format can only be used from
 * the same cluster that still contains the original snapshot serialized from.
 */
void migr_tw_init_from_data(struct migr_tw *tw, void *data, size_t size,
    struct isi_error **error_out);

/**
 * Frees all memory associated with the treewalker.
 */
void migr_tw_clean(struct migr_tw *tw);

/**
 * Gets the lowest level migr_dir struct.  The caller is borrowing a reference
 * that is no longer valid after any push or pop operations.
 */
struct migr_dir *migr_tw_get_dir(struct migr_tw *tw,
    struct isi_error **error_out);

/**
 * Returns the work item description for the given treewalker.  This is the LIN
 * and dir_slice associated with the top level directory.  If the treewalk
 * does not contain any directories it will return false.
 */
bool migr_tw_get_work_desc(struct migr_tw *tw, struct migr_work_desc *wd);

/**
 * Opens the named directory and push it onto the stack of currently walked
 * directories.  Callers should use the value of migr_tw_get_dir to retrieve
 * the new migr_dir object.
 *
 * This functional will return false without an error if the named directory is
 * not selected by the selectors supplied in the initialization.
 */
bool migr_tw_push(struct migr_tw *tw, struct migr_dirent *d,
    struct isi_error **error_out);

/**
 * Pops the lowest directory off the directory stack and frees it.  Any
 * references to the dir should no longer be used.
 *
 * This functional will return true if this the last available directory was
 * popped, leaving nothing left in the treewalk.
 */
bool migr_tw_pop(struct migr_tw *tw, struct migr_dir *dir);

/**
 * Returns a path and slice and removes the governance of this path and slice
 * from the treewalk.
 *
 * Return values are similar to migr_dir_split.
 */
enum migr_split_result migr_tw_split(struct migr_tw *tw, struct path *path,
    struct migr_work_desc *desc, struct migr_dir **split_dir,
    struct isi_error **error_out);

/**
 * 
 */
struct migr_dir *migr_tw_find_dir(struct migr_tw *tw, ifs_lin_t dlin);

/**
 * Returns a malloc'd region of memory containing a serialization of the
 * treewalker's state.  The treewalker can be resurrected using
 * migr_tw_init_from_data.
 */
void *migr_tw_serialize(struct migr_tw *tw, size_t *size);

/**
 * Declarations to support unit tests.
 */
struct migr_slice_od;
bool unserialize_tw_data(void *, size_t, struct path *, bool *, char **,
    struct migr_slice_od **, int *);

bool
migr_tw_has_slices(struct migr_tw *tw);

bool
migr_tw_has_unread_dirents(struct migr_tw *tw, struct isi_error **error);

void
get_last_migr_work(struct migr_tw *tw, struct migr_work_desc *wd,
    char **selectbuf, int *selectlen, struct isi_error **error_out);

#endif /* TREEWALK_H */
