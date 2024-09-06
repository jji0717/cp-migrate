#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <dirent.h>
#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/util/isi_compliance.h>
#include <isi_celog/isi_celog.h>
#include <isi_util/isi_dir.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_extattr.h>
#include <isi_util/isi_sysctl.h>
#include <isi_domain/dom_util.h>
#include <isi_domain/dom.h>
#include <isi_domain/worm.h>
#include <ifs/ifm_adt.h>
#include <sys/extattr.h>
#include <sys/isi_format.h>

#include "isi_snapshot/snapshot.h"
#include "isi_domain/dom.h"
#include "isi_domain/worm.h"
#include "isirep.h"
#include "treewalk.h"
#include "repstate.h"
#include "siq_workers_utils.h"
#include "comp_conflict_log.h"

static bool filter_path_error_logging = false;

static struct siq_track_op siq_track_ops[SIQ_OP_TOTAL_OPS];
void siq_track_op_beg(int op) {
	if (cur_loglevel() >= INFO) {
		ASSERT (op >= SIQ_OP_FILE_DATA && op < SIQ_OP_TOTAL_OPS);
		gettimeofday(&siq_track_ops[op].time_delta, NULL);
		siq_track_ops[op].started = true;
	}
}

void siq_track_op_end(int op) {
	struct timeval temp;
	if (cur_loglevel() >= INFO) {
		ASSERT (op >= SIQ_OP_FILE_DATA && op < SIQ_OP_TOTAL_OPS);
		gettimeofday(&temp, NULL);
		siq_track_ops[op].time_total +=
		    (temp.tv_sec - siq_track_ops[op].time_delta.tv_sec)
		    * 1000 * 1000
		    + (temp.tv_usec - siq_track_ops[op].time_delta.tv_usec);
		siq_track_ops[op].total++;
		siq_track_ops[op].started = false;
	}
}

void siq_track_op_end_nocount(int op) {
	struct timeval temp;
	if (cur_loglevel() >= INFO) {
		ASSERT (op >= SIQ_OP_FILE_DATA && op < SIQ_OP_TOTAL_OPS);
		gettimeofday(&temp, NULL);
		siq_track_ops[op].time_total +=
		    (temp.tv_sec - siq_track_ops[op].time_delta.tv_sec)
		    * 1000 * 1000
		    + (temp.tv_usec - siq_track_ops[op].time_delta.tv_usec);
		siq_track_ops[op].started = false;
	}
}

void siq_track_op_bump_count(int op) {
	if (cur_loglevel() >= INFO) {
		siq_track_ops[op].total++;
	}
}

void log_siq_ops(void)
{
	log(DEBUG, "%s %llu %llu - %s %llu %llu - %s %llu %llu - %s %llu %llu "
	    "- %s %llu %llu \n",
	    "FILE_DATA", siq_track_ops[SIQ_OP_FILE_DATA].total,
	    siq_track_ops[SIQ_OP_FILE_DATA].time_total,
	    "FILE_ACKS", siq_track_ops[SIQ_OP_FILE_ACKS].total,
	    siq_track_ops[SIQ_OP_FILE_ACKS].time_total,
	    "DIR_DEL", siq_track_ops[SIQ_OP_DIR_DEL].total,
	    siq_track_ops[SIQ_OP_DIR_DEL].time_total,
	    "DIR_FINISH", siq_track_ops[SIQ_OP_DIR_FINISH].total,
	    siq_track_ops[SIQ_OP_DIR_FINISH].time_total,
	    "RPC_SEND_SLEEP", siq_track_ops[SIQ_OP_RPC_SEND].total,
	    siq_track_ops[SIQ_OP_RPC_SEND].time_total);
}

/**************
 * PATH UTILS *
 **************/

void
_path_init(struct path *p, const char *path, const struct enc_vec *evec)
{
	char *s;

	memset(p, 0, sizeof *p);
	p->used = path ? strlen(path) : 0;
	if (p->used && path[p->used - 1] == '/')
		p->used--;
	ensurelen(p->path, p->used + 1);
	if (path)
		memcpy(p->path, path, p->used);
	p->path[p->used] = 0;
	enc_vec_init(&p->evec);
	if (evec)
		enc_vec_copy(&p->evec, evec);
	else
		for (s = p->path; (s = strchr(s, '/')); s++)
			enc_vec_push(&p->evec, ENC_DEFAULT);
}

void
path_init(struct path *p, const char *path, const struct enc_vec *evec)
{
	_path_init(p, path, evec);

	if (!path) {
		p->utf8used = 0;
		ensurelen(p->utf8path, 1);
		p->utf8path[p->utf8used] = 0;
		return;
	}

	if (evec) {
		/* need conversion */
		p->utf8path = path_utf8(p, &p->utf8pathlen, &p->utf8used);
		if (!p->utf8path) {
			log(FATAL, "Failed to convert path %s to UTF8.",
			    p->path);
		}
	} else {
		p->utf8pathlen = p->used;
		p->utf8path = (char *) calloc(1, p->utf8pathlen + 1);
		memcpy(p->utf8path, p->path, p->used + 1);
		p->utf8used = p->used;
	}
	log(TRACE, "Path: %s, utf-8-path: %s", p->path, p->utf8path);
}

void
path_copy(struct path *to, const struct path *from)
{
	ASSERT(to);
	ASSERT(from);
	ensurelen(to->path, from->used + 1);
	memcpy(to->path, from->path, from->used + 1);
	to->used = from->used;

	ensurelen(to->utf8path, from->utf8used + 1);
	memcpy(to->utf8path, from->utf8path, from->utf8used + 1);
	to->utf8used = from->utf8used;

	enc_vec_copy(&to->evec, &from->evec);
}

void
_path_add(struct path *p, const char *name, int namlen, enc_t enc)
{
	if (!namlen) /* Just in case. */
		namlen = strlen(name);
	ensurelen(p->path, p->used + namlen + 2);
	if (p->used && (p->path[p->used - 1] != '/')) /* Add slash if needed*/
		p->path[p->used++] = '/';
	memcpy(p->path + p->used, name, namlen);
	p->used += namlen;
	*(p->path + p->used) = 0; /* Null-terminate, even if 'name' wasn't. */
	if (namlen != 1 || name[0] != '/')
		enc_vec_push(&p->evec, enc);
}


void
path_add(struct path *p, const char *name, int namlen, enc_t enc)
{
	static char utf8name[MAXNAMELEN + 1];
	size_t elen;
	int inutf8 = 1;
	_path_add(p, name, namlen, enc);

	if (!namlen)
		namlen = strlen(name);

	if (enc != ENC_UTF8) {
		elen = MAXNAMELEN + 1;
		if (enc_conv(name, enc, utf8name, &elen, ENC_UTF8) == -1) {
			log(DEBUG,
			"Failed to translate file name %s from %s to %s",
			name, enc_type2name(enc), enc_type2name(ENC_UTF8));
			return;
		}
		namlen = elen - 1;
		inutf8 = 0;
	}

	ensurelen(p->utf8path, p->utf8used + namlen + 2);
	if (p->utf8used && (p->utf8path[p->utf8used - 1] != '/'))
		p->utf8path[p->utf8used++] = '/';
	memcpy(p->utf8path + p->utf8used, inutf8 ? name : utf8name, namlen);
	p->utf8used += namlen;
	*(p->utf8path + p->utf8used) = 0;
}

void
path_rem(struct path *p, int newlen)
{
	char *c;
	enc_t enc;

	while (p->utf8used>0 && p->utf8path[p->utf8used] != '/')
		p->utf8used--;

	if (!newlen) {
		/* find last '/' in path
		 * and calculate new pathlen(new p->used)
		 */
		newlen = p->used;
		for (c = p->path + p->used;
			c > p->path && *c != '/'; c--, newlen--);
	}

	p->used = newlen;
	p->path[p->used] = 0;

	p->utf8path[p->utf8used] = 0;

	enc_vec_pop(&p->evec, &enc);
}

void
path_truncate(struct path *p, int len)
{
	char *c = p->path + len, *e = c, *d;
	int i, j, nl;
	p->used = len;
	if (len) {
		for (i = 0; len && (c = strchr(c, '/')); i++, c++);
		if (i) {
			nl = enc_vec_size(&p->evec) - i;
			enc_vec_truncate(&p->evec, nl);
			if (p->path[0] != '/')
				nl--;
			for (j = 0, c = d = p->utf8path; (c =
			    strchr(c, '/')) && j <= nl; j++, d = c, c++);
			*d = 0;
			p->utf8used = d - p->utf8path;
		}
	} else {
		enc_vec_truncate(&p->evec, 0);
		p->utf8used = 0;
		p->utf8path[0] = 0;
	}
	*e = 0;
}

const char *
path_fmt(const struct path *p)
{
	static struct fmt fmt = FMT_INITIALIZER;

	fmt_truncate(&fmt);

	if (p == NULL) {
		fmt_print(&fmt, "empty path");
		goto out;
	}

	fmt_print(&fmt, "%s", p->path);

	if (enc_vec_size(&p->evec))
		fmt_print(&fmt, "%{}", enc_vec_fmt(&p->evec));

out:
	return fmt_string(&fmt);
}

char *
path_utf8(struct path *p, int *utf8pathlen, int *utf8used)
{
	char *start, *end, c;
	/*
	 * allocate four times memory for utf8 path
	 * (4 bytes for each character maximum) to avoid reallocs
	 */
	int _utf8pathlen;
	char *res, *eres;
	size_t elen;
	enc_t *enc;

	if (!p || !p->used)
		return (char *) NULL;
	_utf8pathlen = p->used * 4 + 1;
	res = (char *)calloc(1, _utf8pathlen);
	ASSERT(res);

	eres = res;
	start = p->path;
	/* if path is absolute then skip first '/' */
	if (*start == '/') {
		start++;
		*res='/';
		eres++;
	}

	ENC_VEC_FOREACH(enc, &p->evec)	{
		if (!(end = strchr(start, '/')) !=
		    (enc == enc_vec_last_const(&p->evec))) {
			log(ERROR, "Slashes not consistent with evec %d in %s",
			    enc_vec_size(&p->evec), p->path);
			ASSERT(0);
			goto error;
		}

		if (!end)
			end = &c;

		/* Temporarily replace ending '/' with a nul */
		c = *end;
		*end = 0;

		/*
		 * I hope it's not possible to have a zero byte in name
		 * and '/' is always encoded as '/'
		 */
		if (*enc != ENC_UTF8) {
			elen = MAXNAMELEN + 1;
			if (enc_conv(start, *enc, eres, &elen,
			    ENC_UTF8) == -1) {
				log(ERROR, "Failed to translate file name"
				    "%s from %s to %s", start,
				    enc_type2name(*enc),
				    enc_type2name(ENC_UTF8));
				*end = c;
				goto error;
			}

			/* shift pointer to the end of string, add slash */
			eres += elen - 1;
			*eres = '/';
			eres++;
		} else {
			/* just copy string, no need to convert */
			strcpy(eres, start);
			while (*eres)
				eres++;
			*eres = '/';
			eres++;
		}
		*end = c;
		start = end + 1;

	}
	/*
	 * strip last '/' if needed and
	 * reallocate memory to actual needs
	 */
	if (p->path[p->used - 1]!='/') {
		eres--;
		*eres = '\0';
	}

	if (!utf8pathlen) {
		res = realloc(res, eres - res + 1);
		ASSERT(res);
	}
	else
		*utf8pathlen = _utf8pathlen;

	if (utf8used)
		*utf8used = eres-res;
	return res;
error:
	free(res);
	return (char *)NULL;
}

void
path_clean(struct path *p)
{
	free(p->path);
	if(p->utf8path != NULL)
		free(p->utf8path);
	enc_vec_clean(&p->evec);
	memset(p, 0, sizeof *p);
}

/*********
 * UTILS *
 *********/

/*
 * Attempt to acquire one of max_concurrent snapshot locks.
 * Returns the lock_fd on successful and -1 if there are currently no locks
 * available.
 */
int
get_concurrent_snapshot_lock(int max_concurrent, struct isi_error **error_out)
{
	char lock_name[MAXPATHLEN + 1];
	int i;
	int lock_fd = -1;
	int lock_num;
	struct isi_error *error = NULL;

	ASSERT(max_concurrent > 0);

	lock_num = rand() % max_concurrent;
	for (i = 0; i < max_concurrent; i++) {
		snprintf(lock_name, sizeof(lock_name), "%s/%d",
		    SIQ_SNAPSHOT_LOCK_DIR, lock_num);
		lock_fd = acquire_lock(lock_name, O_NONBLOCK | O_EXLOCK,
		    &error);
		if (error) {
			/* Actual return is EAGAIN == EWOULDBLOCK for POSIX. */
			if (!isi_system_error_is_a(error, EWOULDBLOCK))
				goto out;

			isi_error_free(error);
			error = NULL;
			lock_num = (lock_num + 1) % max_concurrent;
			continue;
		}

		log(NOTICE,
		    "%s: Acquired SyncIQ lock to execute snapshot create: %s",
		    __func__, lock_name);
		break;
	}

out:
	isi_error_handle(error, error_out);

	return lock_fd;
}

ifs_snapid_t
takesnap(const char *name, char *path, time_t expires, char *alias,
    struct isi_error **error_out)
{
	bool ufp_sleep = false;
	int lock_fd = -1;
	int max_snapshots;
	struct isi_error *error = NULL;
	struct isi_str snap_str, *paths[2] = {0};
	struct isi_str *aliases[2] = {0};
	struct siq_gc_conf *conf = NULL;
	ifs_snapid_t snapid = INVALID_SNAPID;

	do {
		ufp_sleep = false;

		UFAIL_POINT_CODE(pause_takesnap,
		    ufp_sleep = true;
		);

		if (ufp_sleep) {
			log(DEBUG, "%s: ufp sleep", __func__);
			siq_nanosleep(1, 0);
		}
	} while (ufp_sleep);

	conf = siq_gc_conf_load(&error);
	if (error)
		goto out;

	siq_gc_conf_close(conf);

	max_snapshots = conf->root->application->max_concurrent_snapshots;

	/* If support sets an invalid max, make it is that we can at least
	 * take one snapshot at a time. */
	if (max_snapshots <= 0)
		max_snapshots = 1;

	lock_fd = get_concurrent_snapshot_lock(max_snapshots, &error);
	if (error)
		goto out;

	while (lock_fd < 0) {
		log(NOTICE,
		    "%s: Waiting on SyncIQ lock prior to taking a snapshot. "
		    "Max locks (%d)",
		    __func__, max_snapshots);
		siq_nanosleep(1, 0);
		lock_fd = get_concurrent_snapshot_lock(max_snapshots, &error);
		if (error)
			goto out;
	}

	paths[0] = isi_str_alloc(path, strlen(path)+1, ENC_UTF8,
	    ISI_STR_NO_MALLOC);
	paths[1] = NULL;

	if (alias) {
		aliases[0] = isi_str_alloc(alias, strlen(alias)+1, ENC_UTF8,
		    ISI_STR_NO_MALLOC);
		aliases[1] = NULL;
	}

	isi_str_init(&snap_str, __DECONST(char *, name), strlen(name)+1,
	    ENC_UTF8, ISI_STR_NO_MALLOC);

	snapid = snapshot_create(&snap_str, expires, paths,
	    alias ? aliases : NULL, &error);
	if (error)
		goto out;

out:
	isi_str_free(paths[0]);
	if (alias)
		isi_str_free(aliases[0]);

	if (lock_fd >= 0)
		close(lock_fd);

	isi_error_handle(error, error_out);
	return snapid;
}

time_t
get_snap_ctime(ifs_snapid_t snapid)
{
	SNAP *snap;
	time_t ctime;

	snap = snapshot_open_by_sid(snapid, NULL);
	if (!snap) {
		log(ERROR, "Failed to open snapshot XXX");
		return 0;
	}

	snapshot_get_ctime(snap, &ctime);
	snapshot_close(snap);
	return ctime;
}

struct path *
relpath(const struct path *_path, const struct path *_hint)
{
	static struct path path, hint, todo;
	static int init = 0;
	char *p, *h, *lp, *lh;
	int i = 0;

	if (!init) {
		path_init(&path, 0, 0);
		path_init(&hint, 0, 0);
		path_init(&todo, 0, 0);
		init = 1;
	}

	path_truncate(&todo, 0);
	path_copy(&path, _path); /* Safely copy to locals. */
	path_copy(&hint, _hint);
	if (!hint.used)
		path_add(&todo, "/", 1, ENC_DEFAULT);

	p = strtok_r(path.path, "/", &lp);
	h = strtok_r(hint.path, "/", &lh);
	for (; p || h; p = strtok_r(0, "/", &lp), h = strtok_r(0, "/", &lh)) {
		if (!p)
			path_add(&todo, "..", 2, ENC_DEFAULT);

		else if (!h)
			path_add(&todo, p, 0, path.evec.encs[i]);

		else if (strcmp(p, h)) {
			do
				path_add(&todo, "..", 2, ENC_DEFAULT);
			while (strtok_r(0, "/", &lh));

			do
				path_add(&todo, p, 0, path.evec.encs[i++]);
			while ((p = strtok_r(0, "/", &lp)));
			break;
		}
		i++;
	}

	log(DEBUG, "hint: %s", path_fmt(_hint));
	log(DEBUG, "path: %s", path_fmt(_path));
	log(DEBUG, " todo: %s", path_fmt(&todo));
	return &todo;
}

/*
 * Drop leading "/ifs" from the path if it's there
 */
struct path *
path_drop_ifs(const struct path *_path)
{
	static struct path path_ifs;
	static struct path path;
	static bool init = false;

	if (!init) {
		path_init(&path_ifs, "/ifs", NULL);
		path_init(&path, NULL, NULL);
		init = true;
	}

	path_copy(&path, _path);

	if (strncmp(path.path, "/ifs", 4))
		return &path;

	return relpath(&path, &path_ifs);
}

/*
 * Change begining of the path all_path: old to path new
 */
struct path *
path_changebeg(const struct path *all_path, const struct path *_old,
		const struct path *_new)
{
	static struct path res, all, old;
	static int init;
	char *a, *o, *la, *lo;
	int i = 0;

#define IF_EMPTY(path) ((path == NULL) || (path->used == 0))
	if(IF_EMPTY(_new) || IF_EMPTY (_old) || IF_EMPTY(all_path)) {
		return NULL;
	}
#undef IF_EMPTY

	if (!init) {
		path_init(&res, 0, 0);
		path_init(&all, 0, 0);
		path_init(&old, 0, 0);
		init = 1;
	}

	path_copy(&all, all_path);
	path_copy(&old, _old);
	path_copy(&res, _new);

	a = strtok_r(all.path, "/", &la);
	o = strtok_r(old.path, "/", &lo);

	for(; a || o; a = strtok_r(0, "/", &la), o = strtok_r(0, "/", &lo)) {
		ASSERT(a, "%s is longer than %s", _old->path, all_path->path);

		if(!o) {
			path_add(&res, a, 0, all.evec.encs[i]);
		}
		i++;
	};

	return &res;
}

/**
 * Takes an absolute path (in /ifs), an fd for /ifs @ some snapid, and returns
 * a new fd at the path location. This wrapper takes care of some oddities when
 * using schdirfd with an absolute path and a fd (/ifs).
 */
int
fd_rel_to_ifs(int ifs_fd, struct path *path)
{
	int fd_out = -1;

	ASSERT(ifs_fd > 0);
	ASSERT(strncmp(path->path, "/ifs", 4) == 0);

	if (enc_vec_len(&path->evec) > 1) {
		if (schdirfd(path_drop_ifs(path), ifs_fd, &fd_out) == -1)
			fd_out = -1;
	} else {
		/* changing to /ifs -- just return a copy of ifs_fd */
		fd_out = dup(ifs_fd);
	}

	return fd_out;
}

/**
 * Takes a path into a .snapshot directory and converts it into a normal path
 * by dropping the .snapshot/snapname entries from the path. does nothing if
 * .snapshot is not part of the path.
 */
void
snap_path_to_normal_path(struct path *path)
{
	struct path copy;
	char *c = NULL, *lc;
	int depth, i;

	ASSERT(strncmp(path->path, "/ifs", 4) == 0 &&
	    (path->path[4] == 0 || path->path[4] == '/'));

	/* determine depth of ".snapshot/xxx" in the path */
	for (depth = 0, c = path->path; c; c = strchr(++c, '/'), depth++)
		if (strncmp(c, "/.snapshot/", 11) == 0)
			break;

	if (!c)
		goto out;

	path_init(&copy, NULL, NULL);
	path_copy(&copy, path);
	path_clean(path);
	path_add(path, "/ifs", 0, 0);

	/* rebuild path less .snapshot and snapname entries */
	for (i = 0, c = strtok_r(copy.path, "/", &lc); c;
	    c = strtok_r(NULL, "/", &lc), i++) {
		if (i == 0 || (i >= depth && i < depth + 2))
			continue;
		path_add(path, c, 0, copy.evec.encs[i]);
	}

	path_clean(&copy);

out:
	return;
}

/*
 * Helper for chdir_single.  This does a simple open, checking
 * that the opened fd is a directory.  Fails with ENOENT if
 * there was no dirent for the name, ENOTDIR if the type wasn't
 * a directory, and any other open or stat errno otherwise.
 */
static int
simple_dir_open(int fd_parent, int *fd_out, char *fname, enc_t enc,
    int *errno_out)
{
	struct stat st;
	int kept_errno = 0;
	int newfd = -1;
	int error = 0;

	newfd = enc_openat(fd_parent, fname, enc, O_RDONLY |
	    O_NOFOLLOW | O_OPENLINK);
	kept_errno = errno;
	ASSERT(newfd != 0);
	if (newfd == -1)
		error = -1;

	/* If successful, make sure it's a directory. */
	if (error == 0) {
		error = fstat(newfd, &st);
		kept_errno = errno;

		if (error == 0) {
			/* Exit if successful */
			if (S_ISDIR(st.st_mode))
				goto out;
			kept_errno = ENOTDIR;
			error = -1;
		}
		/* Uninteresting return value */
		close(newfd);
		newfd = -1;
	}

 out:
	ASSERT(error != 0 || (newfd >= 0));
	ASSERT(error == 0 || (newfd == -1));
	*fd_out = newfd;
	*errno_out = kept_errno;
	return error;
}

/*
 * Do one component of a chdir operation.  This attempts to
 * open a directory named fname/enc in fd_parent.  If mk is
 * true, a directory is made if it doesn't already exists, with
 * any non-directory entries removed if necessary to make way.
 */
static int
chdir_single(int fd_parent, bool mk, int *fd_out,
    char *fname, enc_t enc, char *printable_path, int *errno_out)
{
	int kept_errno = 0;
	int error = 0;
	int newfd = -1;

	/*
	 * Try a simple open of the directory with a check that the
	 * returned fd points at a directory.
	 */
	error = simple_dir_open(fd_parent, &newfd, fname, enc, &kept_errno);
	if (!error)
		goto out;
	ASSERT(kept_errno);

	/*
	 * Fail if creation disallowed or we got an error other
	 * than ENOENT and ENOTDIR
	 */
	if (!mk || (kept_errno != ENOENT && kept_errno != ENOTDIR))
		goto out;

	/*
	 * If a file is in the way try to blow it away.  Ignore
	 * ENOENT errors because that probably probably just
	 * idicates a race with another worker.
	 */
	if (kept_errno == ENOTDIR) {
		error = enc_unlinkat(fd_parent, fname, enc, 0);
		kept_errno = errno;
		if (error != 0 && kept_errno != ENOENT) {
			log(INFO, "Unable to unlink "
			    "%s of %s. errno = %d (%s)",
			    printable_path, fname, kept_errno,
			    strerror(kept_errno));
			goto out;
		}
		error = 0;
	}

	/* Try making the new directory. */
	error = enc_mkdirat(fd_parent, fname, enc, 0755);
	kept_errno = errno;

	if (error) {
		/* Ignore EEXIST since we're likely racing another worker */
		if (kept_errno == EEXIST)
			error = 0;
		else {
			log(INFO, "Unable to mkdir %s in %s. "
			    "errno = %d (%s)", fname, printable_path,
			    kept_errno, strerror(kept_errno));
			goto out;
		}
	}

	/* Try opening the new directory */
	error = simple_dir_open(fd_parent, &newfd, fname, enc, &kept_errno);
	if (!error)
		goto out;
	ASSERT(kept_errno);

 out:
	ASSERT(error != 0 || (newfd >= 0));
	ASSERT(error == 0 || (newfd == -1));
	*fd_out = newfd;
	*errno_out = kept_errno;
	return error;
}

static __inline int
_schdirfd(struct path *dir, bool mk, int fd_in, uint64_t domain_id,
    int *pfd_out)
{
	char *start = dir->path, *end, c, *dupe_path = NULL;
	int fd = fd_in;
	int newfd = -1;
	int error = 0;
	enc_t *enc;
	int ret;
	int kept_errno = 0;
	struct stat st;

	/* evec (or path) length should never be 0 here */
	ASSERT(!enc_vec_empty(&dir->evec));

	log(TRACE, "Chdir to %s", dir->path);
	/* If dir is absolute, go to /.  This also enables enc/slash checks. */
	if (*start == '/') {
		if ((fd = open("/", O_RDONLY)) == -1) {
			kept_errno = errno;
			error = -1;
			goto out;
		}
		start++;
	}
	dupe_path = strdup(dir->path);

	/* This routine could be made more syscall-efficient by combining
	 * adjacent path-parts with identical encodings into one syscall, but
	 * for now we'll do one path-part at a time. */
	ENC_VEC_FOREACH(enc, &dir->evec) {
		if (!(end = strchr(start, '/')) !=
		    (enc == enc_vec_last(&dir->evec)))
			log(FATAL, "Slashes not consistent with evec %d in %s",
			    enc_vec_size(&dir->evec), dir->path);
		if (!end)
			end = &c;
		/* Temporarily replace ending '/' with a nul */
		c = *end;
		*end = 0;

		/* Try opening the next component */
		error = chdir_single(fd, mk, &newfd, start, *enc,
		    dupe_path, &kept_errno);

		/* Restore ending slash in path */
		*end = c;

		/* Don't close the fd if it was passed in (or invalid) */
		if (fd >= 0 && fd != fd_in)
		{
			/* Uninteresting error code to return */
			ret = close(fd);
			if (ret != 0 && errno == EBADF)
				log(FATAL, "_schdir: close failed with EBADF");
			fd = -1;
		}

		if (error) {
			/*
			 * Error on trying to make a directory is serious and
			 * warrants a log message. An error on just changing
			 * directory just means the directory doesn't exist,
			 * which is expected to happen on occasion.
			 */
			if (mk) {
				log(INFO, "Failed to open dir %s in %s. "
				    "errno=%d (%s)", start, dupe_path,
				    kept_errno, strerror(kept_errno));
			}
			goto out;
		} else if (domain_id != 0) {
			/* STF Read only Domain Upgrade */
			ret = fstat(newfd, &st);
			if (ret != 0) {
				kept_errno = errno;
				log(ERROR, "Failed to enc_fstatat "
				    "%s in schdirfd %d", start, errno);
				error = -1;
				goto out;
			}
			ret = ifs_domain_add_bylin(st.st_ino, domain_id);
			if (ret != 0) {
				if (errno == EEXIST) {
					log(DEBUG, "ifs_domain_add_bylin "
					    "returned (%d): ignoring", errno);
				} else {
					kept_errno = errno;
					log(ERROR, "Failed to "
					    "ifs_domain_add_bylin %s (%lld) in "
					    "schdirfd: %d", start, st.st_ino,
					    errno);
					error = -1;
					goto out;
				}
			}
		}

		fd = newfd;
		start = end + 1;
	}

	if (pfd_out == NULL) {
		log(TRACE, "Chdiring, fd = %d", fd);
		error = fchdir(fd);
		kept_errno = errno;

		/* Uninteresting error code to return */
		ret = close(fd);
		if (ret != 0 && errno == EBADF)
			log(FATAL, "_schdir: close failed with EBADF");
		fd = -1;
	} else {
		error = 0;
		*pfd_out = fd;
	}

 out:
	free(dupe_path);
	log(TRACE, "%s: Return with error=%d, errno=%d", __func__,
	    error, kept_errno);
	if (error) {
		ASSERT(kept_errno);
		errno = kept_errno;
		if (newfd != -1)
			close(newfd);
	}
	return error;
}

int
schdir(struct path *dir)
{
	return _schdirfd(dir, false, AT_FDCWD, 0, NULL);
}

int
schdirfd(struct path *dir, int fd, int *pfd)
{
	return _schdirfd(dir, false, fd, 0, pfd);
}

/*
 * TODO: change to not unlink file to create dir on SIQ_ACT_MIGRATE operation
 * Now it is NOT TODO, only document it and take it in mind for future
 */
int
smkchdir(struct path *dir)
{
	return _schdirfd(dir, true, AT_FDCWD, 0, NULL);
}

int
smkchdirfd(struct path *dir, int fd_in, int *pfd_out)
{
	return _schdirfd(dir, true, fd_in, 0, pfd_out);
}

int
smkchdirfd_upgrade(struct path *dir, int fd, uint64_t domain_id, int *pfd)
{
	return _schdirfd(dir, true, fd, domain_id, pfd);
}

/**
 * Predicate-based directory & file selection.
 *
 * One or more arguments may not be necessary depending on the predicate.
 * Consult the plan structure before calling to determine what info is required
 * (file name, dir name, path, date, size, type). Passing in non-required data
 * is safe and will be easier for callers that already have the data ready.
 *
 * Plan must be defined in terms of files that are selected (not the opposite
 * sense).
 *
 * @param plan 	        compiled predicate expression
 *
 * @param name		dir/file name		(NULL if not required by plan)
 * @param enc		encoding                (-1 if not required by plan)
 * @param st		stat                    (NULL if not required by plan)
 * @param t		time at wich predicate should be checked
 * @param utf8path 	path to the item        (NULL if not required by plan)
 * @param type		file type		(required. use helpers
 * 						 stat_mode_to_file_type or
 * 						 dirent_type_to_file_type to
 * 						 convert to internal enum)
 *
 * @param error_out	output error parameter if any errors are encountered
 *
 * @returns
 * 1			if directory/file satisfies the selection criteria
 * 0			if it fails to satisfy the criteria
 * undefined		if error_out is set
 */
int
siq_select(siq_plan_t *plan, char *name, enc_t enc, struct stat *st, time_t t,
    char *utf8path, enum file_type type, struct isi_error **error_out)
{
	static char ename[MAXNAMELEN + 1];
	size_t elen = MAXNAMELEN + 1;
	int selected = 0;
	struct isi_error *error = NULL;
	select_entry_t entry = {0};
	ifs_lin_t plin = 0;

	/* reject unknown and unsupported file types */
	if (type == SIQ_FT_UNKNOWN)
		goto out;

	/* verify we have all the data needed to execute the plan */

	if (plan && (plan->need_size || plan->need_date))
		ASSERT(st);
	entry.st = st;

	/* need type for the default predicate */
	entry.type = file_type_to_dirent_type(type);

	/* need name regardless of plan requirements if it's a dir */
	if ((type == SIQ_FT_REG && plan && plan->need_fname) ||
	    type == SIQ_FT_DIR)
		ASSERT(name && name[0]);

	if (name && enc != ENC_UTF8) {
		if (enc_conv(name, enc, ename, &elen, ENC_UTF8) == -1) {
			log(DEBUG,
			    "Failed to translate file name %s from %s to %s",
			    name, enc_type2name(enc), enc_type2name(ENC_UTF8));
			error = isi_system_error_new(EIO,
			    "Failed to translate file name %s from %s to %s",
			    name, enc_type2name(enc), enc_type2name(ENC_UTF8));
			goto out;
		}
		ASSERT(elen > 0 && elen <= MAXNAMELEN + 1, "Bad elen %zu", elen);
		ASSERT(0 == ename[elen - 1]);
		name = ename;
	}
	entry.name = name;

	if (plan && plan->need_path)
		ASSERT(utf8path);
	entry.path = utf8path;

	/*
	 * by default, an item is rejected if:
	 *
	 * 1) it is an unknown or unsupported file type
	 * 2) it is a dir named ".snapshot"
	 * 3) it is a dir named ".ifsvar"
	 */
	if (type == SIQ_FT_DIR) {
		ASSERT(st);
		/* predicates do not apply to directories. all directories are
		 * selected (except those named .ifsvar or .snapshot) */
		if (strcmp(name, ".snapshot") && strcmp(name, ".ifsvar")) {
			selected = 1;
		} else {
			get_dir_parent(st->st_ino, st->st_snapid, &plin, false,
			    &error);
			if (error)
				goto out;
			/*
			 * We only consider snapshot and ifsvar present in 
			 * the root directory.
			 */
			if (plin != ROOT_LIN)
				selected = 1;
		}
		goto out;
	}

	/* selected if the item was not excluded by the default criteria *and*
	 * a plan was not provided */
	if (plan == NULL) {
		selected = 1;
		goto out;
	}

	/* check the item against the supplied plan */
	selected = siq_parser_execute(plan, &entry, t);
	if (selected <= 0)
		log(DEBUG, "%s skipped by selection criteria", name);

 out:
	isi_error_handle(error, error_out);
	return selected;
}

void
create_syncrun_file(const char *sync_id, uint64_t run_id,
    struct isi_error **error_out)
{
	int fd = -1;
	int ret = 0;
	char sync_file_name[MAXNAMELEN + 1];
	uint64_t file_run_id;
	struct isi_error *error = NULL;

	if (sync_id == NULL) {
		log(TRACE, "No syncid for sync %s, run %llu", sync_id, run_id);
		goto out;
	}

	log(TRACE, "Creating policy_id/timestamp file %s, run %llu",
	    sync_id, run_id);

	ret = mkdir(SYNCRUN_FILE_DIR, 0700);
	if (ret != 0 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Error creating sync dir for sync %s, run %llu",
		    sync_id, run_id);
		goto out;
	}
	sprintf(sync_file_name, SYNCRUN_FILE_DIR"%s", sync_id);

	/* if a file already exists with the current id, leave it alone */
	if (access(sync_file_name, F_OK) == 0) {
		fd = open(sync_file_name, O_RDONLY|O_NOFOLLOW);
		if (fd >= 0) {
			ret = read(fd, &file_run_id, sizeof(file_run_id));
			if (ret == sizeof(file_run_id) &&
			    file_run_id == run_id) {
				log(INFO, "sync file already exists with "
				    "current id (%llu)", run_id);
				goto out;
			}
			close(fd);
		}
		fd = -1;
	}

	fd = open(sync_file_name, O_CREAT|O_TRUNC|O_FSYNC|O_WRONLY, 0600);
	ASSERT(fd != 0);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Error creating sync file for sync %s, run %llu",
		    sync_id, run_id);
		goto out;
	}

	ret = pwrite(fd, &run_id, sizeof(run_id), 0);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Error writing start for sync %s, run %llu",
		    sync_id, run_id);
		goto out;
	}

out:
	if (fd > 0)
		close(fd);
	isi_error_handle(error, error_out);
}

void
remove_syncrun_file(char *sync_id)
{
	char sync_file_name[MAXNAMELEN + 1];
	int run_dir_fd = -1;
	if (sync_id) {
		sprintf(sync_file_name, "%s", sync_id);
		run_dir_fd = open(SYNCRUN_FILE_DIR, O_RDONLY);
		if (run_dir_fd == -1) {
			log(ERROR, "Unable to open run dir '%s': %s",
			    SYNCRUN_FILE_DIR, strerror(errno));
			goto errout;
		}
		enc_unlinkat(run_dir_fd, sync_file_name, ENC_DEFAULT, 0);
	}
errout:
	if (run_dir_fd > 0)
		close(run_dir_fd);
}

/*
 * Creates the work item locking directories for the specified policy id.
 * This function is meant to be called on the target side by the sworker.
 */
void
create_wi_locking_dir(char *pol_id, int mod, struct isi_error **error_out)
{
	int i;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(lock_path, sizeof(lock_path), "%s/%s",
	    SIQ_TARGET_WORK_LOCK_DIR, pol_id);
	if (mkdir(lock_path, S_IRWXU | S_IRWXG | S_IRWXO) != 0 &&
	    errno != EEXIST) {
		error = isi_system_error_new(errno, "Failed to create "
		    "the target work lock directory %s for policy id %s.",
		    lock_path, pol_id);
		goto out;
	}

	for (i = 0; i < mod; i++) {
		snprintf(lock_path, sizeof(lock_path), "%s/%s/%d",
		    SIQ_TARGET_WORK_LOCK_DIR, pol_id, i);
		if (mkdir(lock_path, S_IRWXU | S_IRWXG | S_IRWXO) != 0 &&
		    errno != EEXIST) {
			error = isi_system_error_new(errno, "Failed to create "
			    "the target work lock directory %s for policy id "
			    "%s", lock_path, pol_id);
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Deletes the work item locking directories for the specified policy id.
 * This function is meant to be called on the target side by the sworker.
 */

void
delete_wi_locking_dir(char *pol_id, struct isi_error **error_out)
{
	int ret;
	char lock_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(lock_path, sizeof(lock_path), "rm -rf %s/%s",
	    SIQ_TARGET_WORK_LOCK_DIR, pol_id);

	ret = system(lock_path);
	if (ret != 0 && ret != ENOENT)
		error = isi_system_error_new(errno, "Failed to delete "
		    "the target work lock directory %s", lock_path);

	isi_error_handle(error, error_out);
}

int
try_force_delete_entry(
    int dirfd,
    char *d_name,
    enc_t enc)
{
	int ret = -1;
	struct stat st;
	int fd = -1;
	bool worm;

	ifs_lin_t parent_lin;
	ifs_lin_t lin;

	fd = enc_openat(dirfd, d_name, enc,
	    O_RDONLY | O_NOFOLLOW | O_OPENLINK);
	if (fd == -1)
		goto out;

	ret = fstat(fd, &st);
	if (ret)
		goto out;

	lin = st.st_ino;

	/* Bug 205081 - Only try force delete in a worm domain */
	worm = get_worm_state(lin, HEAD_SNAPID, NULL, NULL, NULL);
	if (!worm) {
		ret = -1;
		goto out;
	}

	ret = fstat(dirfd, &st);
	if (ret)
		goto out;

	parent_lin = st.st_ino;

	log(NOTICE, "WARNING: Possible unexpired WORM file (%s %llx). "
	    "Attempting privileged delete.", d_name, lin);
	ret = worm_override_delete_lin(lin, parent_lin, enc);
	if (ret)
		goto out;

out:
	if (fd != -1)
		close(fd);

	return ret;
}

/*
 * Type of link operation for function add_or_delete_links().
 */
enum link_op {
	ADD_LINKS = 1,
	REMOVE_LINKS
};

/*
 * Function to detect all the links for child_lin in source_dirfd
 * and either add/remove them in tgt_dirfd. link_count specifies
 * number of links within the same source_dirfd. It could be used
 * to stop the search once link_count is 0.
 *
 * Function returns total number of link/unlink operations performed.
 */
static int
add_or_delete_links(int source_dirfd, ifs_lin_t child_lin, int link_count,
    int link_op, int tgt_dirfd, ifs_lin_t tgt_dirlin, ifs_lin_t tgt_child_lin,
    bool skip_cstore, struct isi_error **ie_out)
{
	struct dirent *entryp = NULL;
	struct isi_error *error = NULL;
	unsigned int item_count = READ_DIRENTS_NUM;
	uint64_t cookie_out[READ_DIRENTS_NUM];
	char dirents[READ_DIRENTS_SIZE];
	int i, nbytes, ret = -1;
	int count = link_count;
	uint64_t resume_cookie = 0;
	int op_count = 0;
	bool removed = false;

	do {
		item_count = READ_DIRENTS_NUM;
		nbytes = readdirplus(source_dirfd, RDP_NOTRANS, &resume_cookie,
		    READ_DIRENTS_SIZE, dirents,
		    READ_DIRENTS_SIZE, &item_count, NULL, cookie_out);

		/* We are not getting required information, fail */
		if (nbytes < 0) {
			error = isi_system_error_new(errno,
			    "readdirplus lookup failure: %s", strerror(errno));
			break;
		}

		/* There is nothing to read */
		if (nbytes == 0)
			break;

		entryp = (struct dirent *) dirents;
		for (i = 0; i < item_count ; i++) {
			/*
			 * Check that lin number matches with dirent lin
			 * returned by readdirplus. If so return TRUE.
			 */
			if (child_lin == entryp->d_fileno) {
				if (link_op == ADD_LINKS) {
					ret = enc_lin_linkat(tgt_child_lin,
					    tgt_dirfd, entryp->d_name,
					    entryp->d_encoding);
					if (ret < 0 && errno != EEXIST) {
						error = isi_system_error_new(
						    errno,
						    "Unable to link  lin %{}: "
						    "at dir %{}",
						    lin_fmt(child_lin),
						    lin_fmt(tgt_dirlin));
					}
					if (ret == 0)
						op_count ++;
				} else if (link_op == REMOVE_LINKS) {
					    removed = remove_dirent_entry(
					    tgt_dirfd, entryp->d_name,
					    entryp->d_encoding,
					    entryp->d_type, skip_cstore,
					    &error);
					if (error)
						goto out;
					if (removed)
						op_count ++;
				} else {
					ASSERT(0, "Invalid link operation %d "
					    "for lin %{}", link_op,
					    lin_fmt(tgt_dirlin));
				}

				count--;
			}
			entryp = (struct dirent*) ((char*) entryp +
			    entryp->d_reclen);
		}
		resume_cookie = cookie_out[item_count - 1];
	} while(count > 0);

out:
	isi_error_handle(error, ie_out);
	return op_count;
}

/*
 * Given a dirent, remove it.
 */
bool
remove_dirent_entry(
    int dirfd,
    char *d_name,
    unsigned d_enc,
    unsigned d_type,
    bool skip_cstore,
    struct isi_error **error_out)
{
	int flag = 0;
	int ret;
	bool removed = false;
	struct isi_error *error = NULL;
	ASSERT (dirfd > 0);
	bool comp_store = false;

	comp_store = is_compliance_store(dirfd, &error);
	if (error || (comp_store && skip_cstore))
		goto out;

	UFAIL_POINT_CODE(log_conflict_on_delete,
		if (has_comp_conflict_log()) {
			log_comp_conflict("fake/delete/path",
			    "faker/delete/path", &error);
			if (error)
				goto out;
		}
	);

	/*
	 * The directory entry could be directory or file.
	 * Pass appropriate flag to unlink syscall
	 */
	if (d_type == DT_DIR) {
		flag |= AT_REMOVEDIR;
	} else {
		log_conflict_path(dirfd, d_name, d_enc, &error);
		if (error)
			goto out;
	}


	ret = enc_unlinkat(dirfd, d_name, d_enc, flag);
	if (ret && errno == EROFS && d_type != DT_DIR) {
		ret = try_force_delete_entry(dirfd, d_name, d_enc);
	}

	if (ret != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to unlink dirent %s: %s", d_name, strerror(errno));
		goto out;
	} else if (!ret) {
		removed = true;
	}

out:
	isi_error_handle(error, error_out);
	return removed;
}

/*
 * Function to go through the snapshot version of source lin
 * and re-create all the links found in the snapshot version
 * to the same set of links on the dest lin but on the HEAD
 * version. We assume that all the parent directory must exist
 * on the HEAD version.
 * XXX TODO: Function needs to skip compliance store.
 */
void recreate_links_from_snap_version(ifs_lin_t source_lin, ifs_snapid_t snapid,
    ifs_lin_t dest_lin, bool is_compliance, struct isi_error **error_out)
{

	int ret = -1, i = 0;
	int dirfd_snap = -1;
	int dirfd_head = -1;
	unsigned int expected_parent_entries = 10;
	unsigned int expected_parent_entries_in;
	struct parent_entry *entryp;
	struct isi_error *error = NULL;

	ASSERT(source_lin > 0);

	/*
	 * Get the parent_entry for the (lin, snapid) pair. If
	 * struct parent_entry is too small, retry multiple times.
	 */
	while (1) {
		entryp = malloc(sizeof (struct parent_entry) *
		    expected_parent_entries);
		ASSERT(entryp);
		expected_parent_entries_in = expected_parent_entries;
		ret = pctl2_lin_get_parents(source_lin, snapid, entryp,
			expected_parent_entries_in, &expected_parent_entries);
		if (ret && errno == ENOSPC) {
			free(entryp);
			entryp = NULL;
			/* Not enough expected_parent_entries, retry  */
			continue;
		}
		if (ret) {
			error = isi_system_error_new(errno,
			    "pctl2_lin_get_parents failed for lin %{}",
			    lin_fmt(source_lin));
			goto out;
		}
		/* We should have a valid entryp at this point, so break. */
		break;
	}

	for (i = 0; i < expected_parent_entries ; i++) {
		dirfd_snap = ifs_lin_open(entryp[i].lin, snapid, O_RDONLY);
		if (dirfd_snap < 0) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %{}: %s",
			    lin_fmt(entryp[i].lin), strerror(errno));
			goto out;
		}

		ASSERT(dirfd_snap != 0);

		/*
		 * Create an entry on the HEAD version.
		 */
		dirfd_head = ifs_lin_open(entryp[i].lin, HEAD_SNAPID,
		    O_RDONLY);
		if (dirfd_head < 0) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %{}: %s",
			    lin_fmt(entryp[i].lin), strerror(errno));
			goto out;
		}

		add_or_delete_links(dirfd_snap, source_lin, entryp[i].count,
		    ADD_LINKS, dirfd_head, entryp[i].lin, dest_lin, true,
		    &error);
		if (error)
			goto out;

		close(dirfd_snap);
		close(dirfd_head);
	}

out:
	if (dirfd_snap > 0)
		close(dirfd_snap);

	if (dirfd_head > 0)
		close(dirfd_head);

	if (entryp)
		free(entryp);

	isi_error_handle(error, error_out);
}

/*
 *	Lookup Dirent by Lin
 *	Given (lin, resume_cookie), lookup the dirent
 *	If resume_cookie != 0, then it looks only at specific
 *	hash bin for dirent. Otherwise the whole directory
 *	is scanned to check for dirent.
 *	Returns the dirent structure to the caller
 *
 *	Return value:
 *	true if found
 *	false if not found or there was an error (in which case
 *	   error_out will be filled in too).
 */

bool
dirent_lookup(
    int dirfd,
    uint64_t resume_cookie,
    uint64_t dirent_lin,
    struct dirent *d_out,
    struct isi_error **error_out)
{
	bool found = false;
	struct dirent *entryp;
	struct isi_error *error = NULL;
	unsigned int item_count = READ_DIRENTS_NUM;
	uint64_t cookie_out[READ_DIRENTS_NUM];
	char dirents[READ_DIRENTS_SIZE];
	int i, nbytes;
	uint32_t hash = 0;

	ASSERT (dirfd > 0);
	ASSERT (dirent_lin > 0);

	if (resume_cookie)
		hash = rdp_cookie63_to_hash(resume_cookie);

	do {
		item_count = READ_DIRENTS_NUM;
		nbytes = readdirplus(dirfd, RDP_NOTRANS, &resume_cookie,
		    READ_DIRENTS_SIZE, dirents,
		    READ_DIRENTS_SIZE, &item_count, NULL, cookie_out);

		/* We are not getting required information, fail */
		if (nbytes < 0) {
			error = isi_system_error_new(errno,
			    "readdirplus lookup failure: %s", strerror(errno));
			break;
		}

		/* There is nothing to read */
		if (nbytes == 0)
			break;

		entryp = (struct dirent *) dirents;
		for (i = 0; i < item_count ; i++) {
			/*
			 * Check that lin number matches with dirent lin
			 * returned by readdirplus. If so return TRUE.
			 */
			if (dirent_lin == entryp->d_fileno) {
				*d_out = *entryp;
				found = true;
				log(TRACE, "found dirent name: %s "
				    "enc %d lin %llx\n",
				    entryp->d_name, entryp->d_encoding,
				    entryp->d_fileno);
				goto out;
			}
			entryp = (struct dirent*) ((char*) entryp +
			    entryp->d_reclen);
		}
		/* XXX: Do we have to increment this?? */
		resume_cookie = cookie_out[item_count - 1];

		/*XXX: EML Not sure this is a valid test */
		/* If the hash value is not same, then exit loop */
		if (hash && (hash != rdp_cookie63_to_hash(resume_cookie))) {
			break;
		}

	} while(1);

out:
	isi_error_handle(error, error_out);
	return found;
}

static void
get_parent_hashes(
    const int fd,
    struct parent_hash_vec *parents,
    struct isi_error **error_out)
{
	struct isi_extattr_blob eb = {};
	struct isi_buf_istream bis = {};
	int ret = -1;
	struct isi_error *error = NULL;
	static const size_t DEF_EXTATTR_BLOB_SIZE = 8192;

	ASSERT(fd >= 0);

	eb.data = malloc(DEF_EXTATTR_BLOB_SIZE);
	ASSERT(eb.data);

	eb.allocated = DEF_EXTATTR_BLOB_SIZE;

	ret = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS, "parent_hash_vec",
	    &eb);
	if (ret != 0) {
		if (errno == ENOATTR)
			goto out;
		ON_SYSERR_DEXIT_GOTO(out, errno, error,
		    "could not get parent hashes");
	}

	parent_hash_vec_clean(parents);
	isi_buf_istream_init(&bis, eb.data, eb.used);
	if (!parent_hash_vec_decode(parents, &bis.super)) {
		ON_SYSERR_GOTO(out, errno, error,
		    "could not decode parent hashes");
		goto out;
	}

out:
	free(eb.data);
	isi_error_handle(error, error_out);
}

static ifs_lin_t
get_first_available_parent(
    const int fd,
    const ifs_snapid_t snap,
    int *dirfd_out,
    uint32_t *hash_out,
    struct isi_error **error_out)
{
	int dirfd = -1;
	uint32_t hash;
	ifs_lin_t plin = INVALID_LIN;
	ifs_lin_t ret_plin = INVALID_LIN;
	struct isi_error *error = NULL;
	const struct parent_hash_entry *p_hash_entry;
	struct parent_hash_vec PARENT_HASH_VEC_INIT_CLEAN(parents);
	int count = 0;
	bool found = false;
	ASSERT(dirfd_out != NULL);
	ASSERT(hash_out != NULL);
	ASSERT(fd >= 0);
	ASSERT(snap != INVALID_SNAPID);

	get_parent_hashes(fd, &parents, &error);
	if (error)
		goto out;

	PARENT_HASH_VEC_FOREACH(p_hash_entry,
		&parents) {
		count = uint_vec_size(
		    &p_hash_entry->hash_vec);
		ASSERT(count);
		hash = p_hash_entry->hash_vec.uints[0];
		plin = p_hash_entry->lin;
		if (snap != HEAD_SNAPID){
			check_in_snap(plin, snap, &found, true, &error);
			if (error)
				goto out;
			if (!found)
				continue;
		}
		dirfd = ifs_lin_open(plin, snap, O_RDWR);
		if (dirfd > 0) {
			ret_plin = plin;
			*hash_out = hash;
			*dirfd_out = dirfd;
			goto out;
		}

	}

out:
	isi_error_handle(error, error_out);
	return ret_plin;
}

/*
 *	Dirent Lookup by Lin
 *	If flag == FASTPATH_PNLY, then
 *		Do lookup based on only parent hash value
 *	Else
 *		Do lookup using both parent hash and
 *		Full scan of directory
 *
 *	Returns parent lin and dirent structure
 *	Return Value
 *	true if found
 *	false if child dirent or parent lin not found or
 *	  there was an error (in which case *error_out will
 *	  be filled in too).
*/
bool
dirent_lookup_by_lin(
    ifs_snapid_t snap,
    uint64_t direntlin,
    int flag,
    uint64_t *parentlin,
    struct dirent *d_out,
    struct isi_error **error_out)
{
	int dirfd = -1, fd = -1;
	bool found = false;
	bool found_lin = false;
	int ret = -1;
	uint32_t hash;
	ifs_lin_t plin = INVALID_LIN;
	struct isi_error *error = NULL;
	struct isi_error *tmp_error = NULL;
	uint64_t resume_cookie;
	ASSERT(direntlin > 0);
	ASSERT(d_out != NULL);
	ASSERT(snap != INVALID_SNAPID);

	fd = ifs_lin_open(direntlin, snap, O_RDWR);
	if (fd < 0) {
		if (errno != ENOENT && errno != ESTALE) {
			ON_SYSERR_DEXIT_GOTO(out, errno, error,
			    "Unable to open lin %{}", lin_snapid_fmt(direntlin, snap));
		}
	}
	ASSERT(fd != 0);

	/*
	 * Try first_parent_lin first
	 */
	ret = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "first_parent_lin",(void *)&plin, sizeof(plin));
	if (ret != sizeof(plin)) {
		ON_SYSERR_DEXIT_GOTO(out, errno, error,
		    "Failed to read first_parent_lin for lin %{}",
		    lin_snapid_fmt(direntlin, snap));

	}

	ret = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "first_parent_hash",(void *)&hash, sizeof(hash));
	if (ret != sizeof(hash)) {
		ON_SYSERR_DEXIT_GOTO(out, errno, error,
		    "Failed to read first_parent_hash for lin %{}",
		    lin_snapid_fmt(direntlin, snap));
	}

	if (snap != HEAD_SNAPID) {
		check_in_snap(plin, snap, &found_lin, true, &error);
		if (error)
			goto out;
		if (!found_lin)
			goto lookup_parents;
	}

	dirfd = ifs_lin_open(plin, snap, O_RDWR);
	if (dirfd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %{}: %s", lin_fmt(plin),
		    strerror(errno));

		if (errno != ENOENT)
			goto out;

		/*
		 * the first parent maybe out of the snap
		 * try lookup by all parents
		 */
lookup_parents:
		plin = get_first_available_parent(fd, snap, &dirfd, &hash, &tmp_error);
		if (tmp_error) {
			log(ERROR, "%s failed for direntlin %{}",
				__func__, lin_fmt(direntlin));
			isi_error_free(error);
			error = tmp_error;
			goto out;
		}
		if (plin == INVALID_LIN) {
			/*
			 * still cannot find available parent
			 * goto out with the error
			 */
			goto out;
		}

		isi_error_free(error);
		error = NULL;

	}

	ASSERT(dirfd != 0);
	*parentlin = plin;
	/*
	 * Hash is not valid, We need to look for dirent by reading all
	 * entries. pass resume_cookie as 0 to do search from begining.
	 */
	if (hash <= 0) {
		resume_cookie = 0;
	} else {
		resume_cookie = rdp_cookie63(hash, 0);
	}

	found = dirent_lookup(dirfd, resume_cookie, direntlin, d_out,
	    &error);
	if (error)
		goto out;

	/*
	 * If the fastpath lookup fails and flag =0 which means that
	 * caller wants both fastpath and normal lookups to be done.
	 */
	if (!found && resume_cookie != 0 && flag == 0) {
		found = dirent_lookup(dirfd, 0, direntlin, d_out,
				    &error);
		if (error)
			goto out;
	}

out:

	if (fd > 0)
		close(fd);

	if (dirfd > 0)
		close(dirfd);

	isi_error_handle(error, error_out);
	return found;
}


/*
 * SLOW PATH
 * This does removal of entries for lin dlin by scanning
 * entire directory for each parent.
 */

int
remove_all_entries_fullscan( uint64_t dlin, bool skip_cstore,
    struct isi_error **error_out)
{

	int ret, i = 0;
	int dirfd = -1;
	unsigned int expected_parent_entries = 10;
	unsigned int expected_parent_entries_in;
	struct parent_entry *entryp;
	struct isi_error *error = NULL;
	int unlink_cnt = 0;
	int op_count = 0;

	ASSERT (dlin > 0);

	/*
	 * Get the parent_entry for the (lin, HEAD_SNAPID) pair. If
	 * struct parent_entry is too small, retry multiple times.
	 */
	while (1) {
		entryp = malloc(sizeof (struct parent_entry) *
		    expected_parent_entries);
		ASSERT(entryp);
		expected_parent_entries_in = expected_parent_entries;
		ret = pctl2_lin_get_parents(dlin, HEAD_SNAPID, entryp,
			expected_parent_entries_in, &expected_parent_entries);
		if (ret && errno == ENOSPC) {
			free(entryp);
			entryp = NULL;
			/* Not enough expected_parent_entries, retry  */
			continue;
		}
		if (ret) {
			error = isi_system_error_new(errno,
			    "pctl2_lin_get_parents failed");
			goto out;
		}
		/* We should have a valid entryp at this point, so break. */
		break;
	}

	for (i = 0; i < expected_parent_entries ; i++) {
		dirfd = ifs_lin_open(entryp[i].lin, HEAD_SNAPID, O_RDWR);
		if (dirfd < 0 && (errno != ENOENT)) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %llx: %s", entryp[i].lin,
			    strerror(errno));
			goto out;
		} else if (dirfd < 0) {
			/* Someone else already deleted this parent */
			continue;
		}
		ASSERT(dirfd != 0);

		op_count = add_or_delete_links(dirfd, dlin, entryp[i].count,
		    REMOVE_LINKS, dirfd, entryp[i].lin, 0, skip_cstore,
		    &error);
		if (error)
			goto out;

		unlink_cnt += op_count;
		close(dirfd);
	}

out:
	if (dirfd > 0)
		close(dirfd);

	if (entryp)
		free(entryp);

	isi_error_handle(error, error_out);
	return unlink_cnt;
}

/*
 * Function to remove a directory entry given hash value and lin number
 * of entry. Returns true on success.
 */
static bool
remove_entry_from_parent(ifs_lin_t plin, uint32_t hash, ifs_lin_t direntlin,
    bool skip_cstore, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	uint64_t resume_cookie;
	struct dirent entry;
	int dirfd = -1;
	bool removed = false;
	bool found;

	/*
	 * Bug 144299
	 * exit if dirent is unlinked and plin is 0
	 */
	if (plin == 0)
		goto out;

	dirfd = ifs_lin_open(plin, HEAD_SNAPID, O_RDWR);
	if (dirfd < 0 && (errno != ENOENT)) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx: %s", plin,
		    strerror(errno));
		goto out;
	} else if (dirfd < 0) {
		goto out;
	}

	ASSERT(dirfd != 0);
	
	/*
	 * If the hash is not valid, then we need to look for dirent
	 * in entire directory.
	 */
	if (hash <= 0) {
		resume_cookie = 0;
	} else {
		resume_cookie = rdp_cookie63(hash, 0);
	}

	found = dirent_lookup(dirfd, resume_cookie, direntlin, &entry,
	    &error);
	if (error)
		goto out;

	/* Try to search entire directory now since hash may not be valid */
	if (!found && resume_cookie != 0 ) {
		found = dirent_lookup(dirfd, 0, direntlin, &entry, &error);
		if (error)
			goto out;
	}
	if (!found)
		goto out;

	removed = remove_dirent_entry(dirfd, entry.d_name, entry.d_encoding,
	    entry.d_type, skip_cstore, &error);
	if (error)
		goto out;

out:
	if (dirfd > 0)
		close(dirfd);
	isi_error_handle(error, error_out);
	return removed;
}


/*
 * Function to remove directory entry for files with nlink=1 or directory entry
 * Returns 1 if entry is removed else 0.
 */
static bool
remove_single_entry(int fd, struct stat *st, bool skip_cstore,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool removed = false;
	ifs_lin_t plin;
	uint32_t hash;
	int ret;

	ret = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "first_parent_lin",(void *)&plin, sizeof(plin));
	if (ret != sizeof(plin)) {
		error = isi_system_error_new(errno,
		    "Failed to read first_parent_lin for lin %llx: %s",
		    st->st_ino, strerror(errno));
		goto out;
	}
	ret = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
	    "first_parent_hash",(void *)&hash, sizeof(hash));
	if (ret != sizeof(hash)) {
		error = isi_system_error_new(errno,
		    "Failed to read first_parent_hash for lin %llx: %s",
		    st->st_ino, strerror(errno));
		goto out;
	}

	removed = remove_entry_from_parent(plin, hash, st->st_ino, skip_cstore,
	    &error);
	if (error)
		goto out;
out:
	isi_error_handle(error, error_out);
	return removed;
}


/*
 * 	Remove all dirents associated with this Lin
 *	This looks at each parent lin and does search
 *	of the directory to find required lin and delete it.
 * 	Returns total number of entries removed.
 */

int
remove_all_parent_dirents( int fd, struct stat *st, bool skip_cstore,
     struct isi_error **error_out)
{

	int i = 0;
	int removed = 0;
	int unlink_cnt = 0;
	char *path = NULL;
	bool decode = false;
	int terrno;
	int count;
	struct isi_error *error = NULL;
	struct isi_buf_istream bis;
	struct isi_extattr_blob eb = {};
	const struct parent_hash_entry *p_hash_entry;
	struct parent_hash_vec PARENT_HASH_VEC_INIT_CLEAN(parents);

	ASSERT (fd > 0);
	ASSERT(st != NULL);

	if (S_ISDIR(st->st_mode) || st->st_nlink == 1) {
		removed = remove_single_entry(fd, st, skip_cstore, &error);
		if (error)
			goto out;
		if (removed)
			unlink_cnt = 1;
	} else {
		terrno = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS,
		    "parent_hash_vec", &eb);
		if (terrno == ENOATTR) {
			unlink_cnt = remove_all_entries_fullscan(
			    st->st_ino, skip_cstore, &error);
			if (error)
				goto out;
		} else if (terrno != 0) {
			path = get_valid_rep_path(st->st_ino, HEAD_SNAPID);
			error = isi_system_error_new(terrno,
			    "Unable to get parent hash vec for %s", path);
			free(path);
			path = NULL;
			goto out;
		} else {
			isi_buf_istream_init(&bis, eb.data, eb.used);
			decode = parent_hash_vec_decode(&parents, &bis.super);
			if (decode) {
				PARENT_HASH_VEC_FOREACH(p_hash_entry,
				    &parents) {
					count = uint_vec_size(
					    &p_hash_entry->hash_vec);
					for (i = 0; i < count; i++) {
						removed = 
						    remove_entry_from_parent(
						    p_hash_entry->lin,
						    p_hash_entry->hash_vec.
						    uints[i], st->st_ino,
						    skip_cstore, &error);
						if (error)
							goto out;
						if (removed)
							unlink_cnt++;
					}
				}
			}
		}
	}

out:
	if (eb.data)
		free(eb.data);

	isi_error_handle(error, error_out);
	return unlink_cnt;
}

enum file_type
dirent_type_to_file_type(unsigned char d_type)
{
	switch(d_type) {
	case DT_REG:
		return SIQ_FT_REG;
	case DT_LNK:
		return SIQ_FT_SYM;
	case DT_CHR:
		return SIQ_FT_CHAR;
	case DT_BLK:
		return SIQ_FT_BLOCK;
	case DT_DIR:
		return SIQ_FT_DIR;
	case DT_SOCK:
		return SIQ_FT_SOCK;
	case DT_FIFO:
		return SIQ_FT_FIFO;
	case DT_UNKNOWN:
	default:
		return SIQ_FT_UNKNOWN;
	}
}

int
dtype_to_ifstype(unsigned char d_type)
{
	switch(d_type) {
	case DT_REG:
		return IFREG;
	case DT_LNK:
		return IFLNK;
	case DT_CHR:
		return IFCHR;
	case DT_BLK:
		return IFBLK;
	case DT_DIR:
		return IFDIR;
	case DT_SOCK:
		return IFSOCK;
	case DT_FIFO:
		return IFIFO;
	case DT_UNKNOWN:
	default:
		ASSERT(0);
		
	}
}

enum file_type
stat_mode_to_file_type(mode_t mode)
{
	return dirent_type_to_file_type(IFTODT(mode));
}

unsigned char
file_type_to_dirent_type(enum file_type type)
{
	switch(type) {
	case SIQ_FT_REG:
		return DT_REG;
	case SIQ_FT_SYM:
		return DT_LNK;
	case SIQ_FT_CHAR:
		return DT_CHR;
	case SIQ_FT_BLOCK:
		return DT_BLK;
	case SIQ_FT_SOCK:
		return DT_SOCK;
	case SIQ_FT_DIR:
		return DT_DIR;
	case SIQ_FT_FIFO:
		return DT_FIFO;
	case SIQ_FT_UNKNOWN:
	default:
		return DT_UNKNOWN;
	}
}

bool
supported_special_file(struct stat *st)
{
	mode_t m;
	ASSERT(st != NULL);
	m = st->st_mode;
	if (S_ISCHR(m) || S_ISBLK(m) || S_ISFIFO(m) || S_ISSOCK(m))
		return true;
	else
		return false;
}

const char *
file_type_to_name(enum file_type type)
{
	switch(type) {
	case SIQ_FT_REG:
		return FT_REG;
	case SIQ_FT_SYM:
		return FT_SYM;
	case SIQ_FT_CHAR:
		return FT_CHAR;
	case SIQ_FT_BLOCK:
		return FT_BLOCK;
	case SIQ_FT_SOCK:
		return FT_SOCK;
	case SIQ_FT_DIR:
		return FT_DIR;
	case SIQ_FT_FIFO:
		return FT_FIFO;
	default:
		return FT_UNKNOWN;
	}
}

/**
 * Warning: returns a static char array
 */
char *
inx_addr_to_str(const struct inx_addr *addr) {
	struct fmt FMT_INIT_CLEAN(addr_fmt);
	static char addr_str[INET6_ADDRSTRLEN];

	fmt_print(&addr_fmt, "%{}", inx_addr_fmt(addr));
	snprintf(addr_str, sizeof(addr_str), "%s", fmt_string(&addr_fmt));
	return addr_str;
}

/**
 * Helper function for ifs_check_in_snapshot().
 * missing_ok determines whether or not presence in snapshot should be
 * considered a fatal error.
 */
void
check_in_snap(ifs_lin_t lin, ifs_snapid_t snap, bool *found, bool missing_ok,
    struct isi_error **error_out)
{
	struct ifs_lin_snapid ls = {.lin = lin, .snapid = snap};
	struct isi_error *error = NULL;
	int rv;

	/* Bug 190724 - check that the lin exists in the snapshot */
	rv = ifs_check_in_snapshot(lin, snap, 0, found);
	if (rv < 0) {
		error = isi_system_error_new(errno,
		    "Error checking snapshot lin %{}",
		    ifs_lin_snapid_fmt(&ls));
		goto out;
	} else if (!*found) {
		if (!missing_ok)
			error = isi_system_error_new(ENOENT,
			    "Unable to open lin %{}", ifs_lin_snapid_fmt(&ls));

		goto out;
	}
out:
	isi_error_handle(error, error_out);
}

/**
 * Return the parent of the directory specified by lin/snap.
 * Ugly interface.  if missing_ok is set to true, the precense
 * or abscense of a lin (based on ESTALE or ENOENT returns)
 * is indicated by a true or false return value rather than an
 * error return.  Otherwise these and all other errors are returned
 * through error_out.
 */
bool
get_dir_parent(ifs_lin_t lin, ifs_snapid_t snap, ifs_lin_t *lin_out,
    bool missing_ok, struct isi_error **error_out)
{
	struct parent_entry pe;
	unsigned num_entries = 1;
	struct isi_error *error = NULL;
	int rv;
	int fd = -1;
	struct stat st;
	bool found = false;
	bool in_snap = false;

	/* Bug 190724 - check that the lin exists in the snapshot */
	if (snap != HEAD_SNAPID) {
		check_in_snap(lin, snap, &in_snap, missing_ok, &error);
		if (error || !in_snap)
			goto out;
	}
	rv = pctl2_lin_get_parents(lin, snap, &pe, 1, &num_entries);
	if (!rv) {
		/*
		 * Even if it is found in snap version, its possible that
		 * it could already be deleted. Make sure we double check
		 * the lin by looking at nlink value.
		 */
		fd = ifs_lin_open(lin, snap, O_RDONLY);
		ASSERT(fd != 0);
		if (fd < 0 && (!missing_ok ||
		    (errno != ESTALE && errno != ENOENT))) {
			error = isi_system_error_new(errno,
			    "Unable to open lin %llx", lin);
			goto out;
		}
		if (fd == -1) {
			/* We just found out that its missing in snap */
			goto out;
		}
		rv = fstat(fd, &st);
		if (rv == -1) {
			error = isi_system_error_new(errno,
			 "Unable to fstat lin %llx\n", lin);
			goto out;
		}
		/* I think now we can be sure that its present in snap */
		if (st.st_nlink > 0) {
			found = true;
			*lin_out = pe.lin;
		}
	} else if (!missing_ok || (errno != ESTALE && errno != ENOENT)) {
		error = isi_system_error_new(errno,
		    "Failed to find lin %llx:%lld", lin,
		    snap);
	}
out:
	if (fd > 0) 
		close(fd);
	isi_error_handle(error, error_out);
	return found;
}

/**
 * Whether to filter the logging of certain types of errors in
 * dir_get_dirent_path(). Filtering is desirable in the context
 * of the ChangelistCreate job when running in summary-stf mode
 * because the summary stf it creates can contain "false positives",
 * i.e. lins which passed the ifs_lin_open() test but don't actually
 * exist in the specified snapshot (the fd of a HEAD version is
 * returned instead). Since the job has no notion of domain id's
 * it relies on snapshot-based path lookups for pruning false
 * positives, avoiding non-interesting and potentially spammy
 * error logging is beneficial.
 */
void
set_filter_path_error_logging(bool filter)
{
	filter_path_error_logging = filter;
}

/**
 * Simple wrapper around enc_lin_get_path that converts it's error return
 * to an isi_error.
 */
void
dir_get_dirent_path(ifs_lin_t root_lin, ifs_lin_t lin, ifs_snapid_t snap,
    ifs_lin_t parent_lin, char **path_dirents_out, int *path_len_out,
    struct isi_error **error_out)
{
	char *path = NULL;
	int len;
	int rv, fd = -1;
	struct ifs_lin_snapid ls = {.lin = lin, .snapid = snap};
	struct isi_extattr_blob eb = { .allocated = 0 };
	struct ifs_dinode *dip;
	u_int32_t name_hash = 0;
	struct isi_error *error = NULL;
	int kept_errno;

	/*
	 * Bug 189397
	 * Get the parent hash to aid enc_lin_get_path.
	 * XXX: this doesn't try to look for the correct parent for multiply
	 *      linked files
	 */
	fd = ifs_lin_open(lin, snap, O_RDONLY);
	if (fd == -1) {
		kept_errno = errno;
		error = isi_system_error_new(kept_errno);
		if (!filter_path_error_logging || !(kept_errno == ENOENT ||
		    kept_errno == EIO || kept_errno == ESTALE)) {
			log(ERROR, "Error %d finding path of %{}",
			    kept_errno, ifs_lin_snapid_fmt(&ls));
		}
		goto out;
	}
	rv = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS, "dinode", &eb);
	if (rv) {
		kept_errno = errno;
		error = isi_system_error_new(kept_errno);
		if (!filter_path_error_logging || !(kept_errno == ENOENT ||
		    kept_errno == EIO || kept_errno == ESTALE)) {
			log(ERROR, "Error %d finding path of %{}",
			    kept_errno, ifs_lin_snapid_fmt(&ls));
		}
		goto out;
	}
	close(fd);
	fd = -1;
	dip = eb.data;
	if (parent_lin == dip->di_parent_lin && !(dip->di_flags & UF_HASADS))
		name_hash = dip->di_parent_hash;

	rv = enc_lin_get_path(root_lin, lin, snap, parent_lin, name_hash,
	    ENC_DEFAULT, &path, &len);

	if (rv) {
		kept_errno = errno;
		error = isi_system_error_new(kept_errno);
		if (!filter_path_error_logging || !(kept_errno == ENOENT ||
		    kept_errno == EIO || kept_errno == ESTALE)) {
			log(ERROR, "Error %d finding path of %{}",
			    kept_errno, ifs_lin_snapid_fmt(&ls));
		}
		goto out;
	} else {
		*path_dirents_out = path;
		*path_len_out = len;
		path = NULL;
	}

 out:
	if (eb.data) {
		free(eb.data);
		eb.data = dip = NULL;
	}
	if (path)
		free(path);
	if (fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
}

/**
 * Produce the utf8 path starting at root_lin and reaching the directory
 * specified by lin, snap.  This path walks up the snapshot tree (ie, it
 * never jumps out through a .snapshot) and does NOT include the name OF
 * rootlin, but instead starts at the name pointing OUT of rootlin (ie,
 * if root_lin is 2, the returned path does NOT include /ifs).  The path
 * does NOT include a trailing slash.  If provided, prefix is string
 * prepended to the result (it is the responsibility of the caller to add
 * the prepended "/" if desired)
 */
void
dir_get_utf8_str_path_2(char *prefix, ifs_lin_t root_lin, ifs_lin_t lin,
    ifs_snapid_t snap, ifs_lin_t parent_lin, char **utf8_str_out,
    uint32_t *path_depth, struct isi_error **error_out)
{
	int rv;
	struct isi_error *error = NULL;
	char *path_dirents = NULL;
	int path_dirents_len = 0;
	char *utf8 = NULL;
	int utf8_alloc_len = 0;
	int utf8_bytes_used = 0;
	size_t conv_bytes_out;
	int offset = 0;
	int kept_errno;
	bool first = true;

	if (!prefix)
		prefix = "";

	*path_depth = 0;

	dir_get_dirent_path(root_lin, lin, snap, parent_lin, &path_dirents,
	    &path_dirents_len, &error);
	if (error)
		goto out;
	ASSERT_IMPLIES(lin != ROOT_LIN, path_dirents_len > 0);
	/* Estimate based on the dirent len plus an extra buffer (enc_conv
	 * always needs MAXNAMLEN+1 space to work with).  This will
	 * be grown inside the loop as necessary
	 */
	utf8_alloc_len = path_dirents_len + strlen(prefix) + 1 + MAXNAMLEN + 1;
	utf8 = malloc(utf8_alloc_len);
	ASSERT(utf8);
	strcpy(utf8, prefix);
	utf8_bytes_used = strlen(prefix);
	while (offset < path_dirents_len) {
		struct dirent *de;
		de = (struct dirent *)(path_dirents + offset);
		/* Alloced - (Current bytes used + slash + terminator) */
		if ((utf8_alloc_len - (utf8_bytes_used + 2)) <
		    (MAXNAMLEN + 1)) {
			utf8_alloc_len *= 2;
			utf8 = realloc(utf8, utf8_alloc_len);
			ASSERT(utf8);
		}
		if (!first) {
			utf8[utf8_bytes_used] = '/';
			utf8_bytes_used++;
		}
		first = false;
		if (de->d_encoding == ENC_UTF8) {
			memcpy(utf8 + utf8_bytes_used, de->d_name,
			    de->d_namlen + 1);
			utf8_bytes_used += de->d_namlen;
		} else {
			conv_bytes_out = MAXNAMLEN + 1;
			rv = enc_conv(de->d_name, de->d_encoding,
			    utf8 + utf8_bytes_used, &conv_bytes_out, ENC_UTF8);
			if (rv) {
				kept_errno = errno;
				error = isi_system_error_new(kept_errno);
				log(ERROR,
				    "Error %d %d converting name of lin %lld",
				    de->d_encoding, kept_errno, de->d_fileno);
				goto out;
			}
			/* Ignore the null */
			utf8_bytes_used += (conv_bytes_out - 1);
		}
		offset += de->d_reclen;
		*path_depth += 1;
	}

	(*utf8_str_out) = utf8;
	utf8 = NULL;

 out:
	if (path_dirents)
		free(path_dirents);
	if (utf8)
		free(utf8);
	isi_error_handle(error, error_out);
}

void
dir_get_utf8_str_path(char *prefix, ifs_lin_t root_lin, ifs_lin_t lin,
    ifs_snapid_t snap, ifs_lin_t parent_lin, char **utf8_str_out,
    struct isi_error **error_out)
{
	uint32_t depth;

	dir_get_utf8_str_path_2(prefix, root_lin, lin, snap, parent_lin,
	    utf8_str_out, &depth, error_out);
}

/*
 * Clear all the ADS entries for a file or directory fd passed
 */
void
clear_ads_entries(int fd, struct stat *st, struct isi_error **error_out)
{
	int ret;
	int dfd = -1;
	struct isi_error *error = NULL;
	struct migr_dir *ads_dir = NULL;
	struct migr_dirent *md = NULL;

	/*
	 * No ADS entries present.
	 */
	if (!(st->st_flags & UF_HASADS))
		return;

	dfd = enc_openat(fd, ".", ENC_DEFAULT, O_RDONLY | O_XATTR);
	if (dfd == -1) {
		error = isi_system_error_new(errno,
		    "Unable to open ADS directory for lin %llx",
		    st->st_ino);
		goto out;
	}

	ads_dir = migr_dir_new(dfd, NULL, true, &error);
	if (error)
		goto out;

	while(1) {
		md = migr_dir_read(ads_dir, &error);
		if (error || !md)
			break;
		ret = enc_unlinkat(dfd, md->dirent->d_name,
		    md->dirent->d_encoding, 0);
		if (ret != 0 && errno != ENOENT) {
			error = isi_system_error_new(errno, "Unable to unlink"
			    " ADS entry %s from lin %llx", md->dirent->d_name,
			    st->st_ino);
			goto out;
		}
		migr_dir_unref(ads_dir, md);
		md = NULL;
	}

out:
	if (md != NULL)
		migr_dir_unref(ads_dir, md);
	if (ads_dir)
		migr_dir_free(ads_dir);
	if (dfd != -1)
		close(dfd);

	isi_error_handle(error, error_out);
}

bool
ip_lists_match(int num_a, char **ips_a, int num_b, char **ips_b)
{
	int i, j;
	bool found;
	bool match = false;

	if (num_a != num_b || !ips_a || !ips_b)
		goto out;

	/* this is an O(n^2) list comparison, so not great, but it's only used
	 * on source or target group changes to see if node configurations
	 * changed. most of the time the mismatch will be detected above when
	 * comparing old and new node counts. we can improve this function
	 * later if necessary */
	for (i = 0; i < num_a; i++) {
		found = false;
		for (j = 0; j < num_b; j++) {
			if (strcmp(ips_a[i], ips_b[j]) == 0) {
				found = true;
				break;
			}
		}
		if (found == false)
			goto out;
	}
	match = true;
	
out:
	return match;
}

void
find_domain_by_lin(ifs_lin_t lin, enum domain_type type, bool ready_only,
    ifs_domainid_t *dom_id, uint32_t *dom_gen, struct isi_error **error_out)
{
	int i = 0;
	u_int dom_set_size = 0;
	struct isi_error *error = NULL;
	struct domain_set dom_set;
	struct domain_entry dom_entry;
	ifs_domainid_t cur_id;
	bool match = false;

	domain_set_init(&dom_set);

	if (dom_get_info_by_lin(lin, HEAD_SNAPID, &dom_set, NULL, NULL) != 0) {
		error = isi_system_error_new(errno,
		    "Could not get domain info");
		goto out;
	}

	dom_set_size = domain_set_size(&dom_set);
	for (i = 0; i < dom_set_size; i++) {
		match = false;
		cur_id = domain_set_atindex(&dom_set, i);
		if (dom_get_entry(cur_id, &dom_entry) != 0)
			continue;

		if (dom_entry.d_root != lin)
			continue;

		if (ready_only && !(dom_entry.d_flags & DOM_READY))
			continue;

		if (type == DT_SYNCIQ) {
			if (!!(dom_entry.d_flags & DOM_SYNCIQ)) {
				match = true;
			} else if (!!(dom_entry.d_flags & DOM_RESTRICTED_WRITE)
			    && !(dom_entry.d_flags & DOM_SNAPREVERT)) {
				match = true;
			}
		} else if (!!(dom_entry.d_flags & type)) {
			match = true;
		}

		if (!match)
			continue;


		if (dom_id)
			*dom_id = cur_id;

		if (dom_gen)
			*dom_gen = dom_entry.d_generation;

		break;
	}

out:
    domain_set_clean(&dom_set);
    isi_error_handle(error, error_out);
}

void
find_domain(const char *root, enum domain_type type, bool ready_only,
    ifs_domainid_t *dom_id, uint32_t *dom_gen, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stat st;

	if (!root) {
		error = isi_siq_error_new(E_SIQ_CONF_NOENT,
		    "%s: no path specified", __func__);
		goto out;
	}

	if (stat(root, &st) != 0) {
		error = isi_system_error_new(errno,
		    "Could not open path %s", root);
		goto out;
	}

	find_domain_by_lin(st.st_ino, type, ready_only, dom_id, dom_gen,
	    &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

void
get_failover_snap_name(char *pol_name, char *ssname)
{
	char failover_snap[MAXPATHLEN];
	time_t create_time;
	struct tm *create_tm;

	sprintf(failover_snap, "SIQ-Failover-%s-", pol_name);
	strcat(failover_snap, "%Y-%m-%d_%H-%M-%S");
	time(&create_time);
	create_tm = localtime(&create_time);
	strftime(ssname, MAXPATHLEN, failover_snap, create_tm);
	return;
}

void
get_changelist_snap_name(char *pol_name, char *ssname)
{
	char changelist_snap[MAXPATHLEN];
	time_t create_time;
	struct tm *create_tm;

	sprintf(changelist_snap, "SIQ-Changelist-%s-", pol_name);
	strcat(changelist_snap, "%Y-%m-%d_%H-%M-%S");
	time(&create_time);
	create_tm = localtime(&create_time);
	strftime(ssname, MAXPATHLEN, changelist_snap, create_tm);
	return;
}

/*
 * If pattern being used for policy snapshots on the target
 * cluster is not unique, then try to  add
 * a count to make the name unique (BUG 110822)
 *
 * @param base_name the name we want the snapshot to be
 * @param error_out output for errors if they occur.
 */
struct isi_str *
get_unique_snap_name(char *base_name, struct isi_error **error_out)
{
	int retry_count = 2;
	struct isi_str *snap_str = NULL;
	SNAP *snap = NULL;
	struct isi_error *error = NULL;

        snap_str = isi_str_alloc(base_name,
                base_name ? (strlen(base_name)+1) : 0,
		ENC_UTF8, ISI_STR_NO_MALLOC);

	if (!base_name)
		goto out;

	snap = snapshot_open_by_name(snap_str, &error);
	for (retry_count = 2; error == NULL; retry_count++) {
		log(TRACE, "Snapshot Name Collision detected with %s "
		    "trying %s(%d) next.", ISI_STR_GET_STR(snap_str),
		    base_name, retry_count);

		if (snap)
			snapshot_close(snap);
		isi_str_free(snap_str);
		snap_str = isi_str_init_fmt(NULL, "%s(%d)", base_name,
		    retry_count);

		snap = snapshot_open_by_name(snap_str, &error);
	}

	if (isi_error_is_a(error, SNAP_NOTFOUND_ERROR_CLASS)) {
		/* Exactly what we want, remove the error */
		isi_error_free(error);
		error = NULL;
		log(INFO, "Snapshot Name Collision detected creating "
		    "snapshot %s, renaming to %s instead.",
		    base_name, ISI_STR_GET_STR(snap_str));
	}
out:
	if (snap)
	    snapshot_close(snap);
	isi_error_handle(error, error_out);
	return snap_str;
}

bool
get_worm_state(ifs_lin_t lin, ifs_snapid_t snapid, struct worm_state *worm_out,
    bool *compliance, struct isi_error **error_out)
{
        int ret = 0;
        struct domain_set doms;
        struct worm_state worm = {};
        struct isi_error *error = NULL;
        bool have_worm = false;
        struct domain_entry *dom_entries = NULL;
        u_int dom_set_size = 0;
        int i = 0;

        domain_set_init(&doms);

        ret = dom_get_info_by_lin(lin, snapid, &doms, NULL, &worm);
        if (ret) {
                error = isi_system_error_new(errno,
                    "Could not get domain info for %llx", lin);
                goto out;
        }

        dom_set_size = domain_set_size(&doms);

        if (compliance) {
                dom_entries = calloc((size_t)dom_set_size,
                    sizeof(struct domain_entry));
                ASSERT(dom_entries);
        }

        ret = dom_get_matching_domains(&doms, DOM_WORM | DOM_READY,
            dom_set_size, NULL, dom_entries);
        if (ret < 0) {
                error = isi_system_error_new(errno,
                    "Could not get domain info for %llx", lin);
                goto out;
        }

        have_worm = (ret > 0);

        if (compliance) {
                *compliance = false;
                for (i = 0; i < ret; i++) {
                        if ((dom_entries[i].d_flags & DOM_COMPLIANCE)
                            == DOM_COMPLIANCE) {
                                *compliance = true;
                                break;
                        }
                }
        }

out:
        if (!error && worm_out && have_worm)
                *worm_out = worm;

        domain_set_clean(&doms);

        if (dom_entries)
                free(dom_entries);

        isi_error_handle(error, error_out);

        return have_worm;
}

bool
is_worm_committed(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{
	int ret;
	enum worm_commit_status worm_status = WORM_NOT_COMMITTED;
	struct isi_error *error = NULL;

	ret = get_worm_commit_status(lin, snapid, &worm_status);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Failed to get worm_commit_status");
		goto out;
	}

out:
	isi_error_handle(error, error_out);

	return worm_status != WORM_NOT_COMMITTED;
}

/*
 * Conflict resolution is only needed if a file is committed in s2
 * and is not committed (or doesn't exist) in s1.
 */
bool
is_conflict_res_needed(ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    struct isi_error **error_out)
{
	bool committed;
	bool res = false;
	struct isi_error *error = NULL;

	committed = is_worm_committed(lin, snap2, &error);
	if (error || !committed)
		goto out;

	committed = is_worm_committed(lin, snap1, &error);
	if (error && isi_system_error_is_a(error, ENOENT)) {
		isi_error_free(error);
		error = NULL;
	} else if (error || committed)
		goto out;

	res = true;

out:
	isi_error_handle(error, error_out);

	return res;
}


/*
 * If a LIN is committed in s1 and s2, then skip sending the ADS on restore.
 * Since a LIN is committed in both snapshots, the ADS could not have changed.
 */
bool
comp_skip_ads(ifs_lin_t lin, ifs_snapid_t snap1, ifs_snapid_t snap2,
    struct isi_error **error_out)
{
	bool committed;
	bool res = false;
	struct isi_error *error = NULL;

	committed = is_worm_committed(lin, snap2, &error);
	if (error || !committed)
		goto out;

	committed = is_worm_committed(lin, snap1, &error);
	if (error && isi_system_error_is_a(error, ENOENT)) {
		isi_error_free(error);
		error = NULL;
		goto out;
	} else if (error || !committed)
		goto out;

	res = true;

out:
	isi_error_handle(error, error_out);

	return res;
}

#define DEXIT_SYSCTL "efs.dexitcode_ring_thread_verbose"
void
dump_exitcode_ring(int error)
{
	struct fmt FMT_INIT_CLEAN(piderrfmt);
	FILE *fstream = NULL;
	char *buf = NULL;
	char *line = NULL;
	const char *piderr = NULL;
	size_t linelen;
	size_t piderrlen;
	int ret = 0;
	int lines;
	bool match = false;

	if (error <= 0)
		goto out;

	ret = isi_sysctl_getstring(DEXIT_SYSCTL, &buf);
	if (ret != 0) {
		log(ERROR, "failed to get sysctl %s: %s", DEXIT_SYSCTL,
		    strerror(ret));
		goto out;
	}

	/* this should match dexitcode_ring_fmt_conv() */
	fmt_print(&piderrfmt, "%5d: %15{} from ", getpid(), error_fmt(error));
	piderr = fmt_string(&piderrfmt);
	piderrlen = fmt_length(&piderrfmt);

	fstream = fmemopen(buf, strlen(buf), "r");
	if (fstream == NULL) {
		log(ERROR, "failed to open stream: %s", strerror(errno));
		goto out;
	}

	lines = 0;
	/* according to fgetln docs, line doesn't need to be freed */
	while ((line = fgetln(fstream, &linelen)) != NULL) {
		lines++;
		if (linelen < 1)
			continue;
		if (line[linelen - 1] == '\n')
			linelen--;
		/*
		 * This assumes that the first line of output and every other
		 * line thereafter always starts with a pid.
		 */
		if (lines % 2 != 0) {
			/* skip partial matches */
			if (linelen < piderrlen)
				continue;
			/* look for line that matches calling PID and error */
			if (strncmp(line, piderr, piderrlen) == 0) {
				log(NOTICE, "%.*s", (int)linelen, line);
				match = true;
			}
		} else if (match) {
			/* print the line after the line with the PID */
			log(NOTICE, "%.*s", (int)linelen, line);
			match = false;
		}
	}

out:
	if (fstream != NULL)
		fclose(fstream);
	fmt_clean(&piderrfmt);
	free(buf);
}

void
init_node_mask(char *mask, char initial, int size)
{
	memset(mask, initial, size);
	mask[size - 1] = '\0';
}

void
merge_and_flip_node_mask(char *tgt, char *src, size_t tgt_size)
{
	int i = 0;
	int len = 0;
	
	len = strlen(src);
	ASSERT(len < tgt_size);

	log(TRACE, "merging node mask: '%s' into '%s'", src, tgt);

	for (i = 0; i < len; i++) {
		if (src[i] == '.') {
			tgt[i] = 'F';
		}
	}
}

bool
is_or_has_compliance_dir(const char *path, struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;

	struct domain_set doms;
	struct domain_set ancs;

	ifs_domainid_t compdom_id = 0;
	ifs_domainid_t domid = 0;
	struct domain_entry compdom_entry = {};
	const struct domain_entry *entry = NULL;

	struct dom_iterator iter = {};

	bool is_has_comp = false;

	domain_set_init(&doms);
	domain_set_init(&ancs);

	dom_iter_init(&iter);

	if (strcmp(path, "/ifs") == 0 || strcmp(path, "/ifs/") == 0) {
		for (entry = dom_iter_next(&iter, &domid); entry;
			entry = dom_iter_next(&iter, &domid)) {
			if (entry->d_flags &
			    (DOM_COMPLIANCE | DOM_WORM | DOM_READY)) {
				is_has_comp = true;
				goto out;
			}
		}
	}

	ret = dom_get_info_by_path(path, &doms, &ancs, NULL);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Error reading domain info for %s", path);
		goto out;
	}

	ret = dom_get_matching_domains(&doms, DOM_COMPLIANCE, 1, &compdom_id,
	    &compdom_entry);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Error reading domain info for %s", path);
		goto out;
	}

	if (ret > 0) {
		is_has_comp = true;
		goto out;
	}

	ret = dom_get_matching_domains(&ancs, DOM_COMPLIANCE, 1, &compdom_id,
	    &compdom_entry);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Error reading domain info for %s", path);
		goto out;
	}

	if (ret > 0) {
		is_has_comp = true;
		goto out;
	}

out:
	domain_set_clean(&ancs);
	domain_set_clean(&doms);
	isi_error_handle(error, error_out);

	return is_has_comp;
}

/*
 * Check if the fd has compliance store set.
 */
bool
is_compliance_store(int fd, struct isi_error **error_out)
{
	int res = -1;
	struct isi_error *error =NULL;
	struct worm_dir_info wdi = {};
	bool is_comp_store = false;

	UFAIL_POINT_CODE(skip_cstore_lookup,
		return false;
	);

	res = extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS, "worm_dir_info", &wdi,
	    sizeof wdi);

	if (res < 0 && errno == ENOATTR)
		goto out;

	if (res != sizeof wdi) {
		error = isi_system_error_new(errno,
		    "Failed to get worm dir info ");
		goto out;
	}

	is_comp_store = wdi.wdi_is_cstore;

out:
	isi_error_handle(error, error_out);
	return is_comp_store;
}

/*
 * Wrapper around is_compliance_store() to accpet lins.
 */
bool
is_compliance_store_lin(ifs_lin_t lin, ifs_snapid_t snapid,
    struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	int fd = -1;
	bool is_comp_store = false;

	fd = ifs_lin_open(lin, snapid, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %{}",
		    lin_fmt(lin));
		goto out;
	}

	is_comp_store = is_compliance_store(fd, &error);
	if (error)
		goto out;

out:
	if (fd != -1)
		close(fd);
	isi_error_handle(error, error_out);
	return is_comp_store;
}

/*
 * NOTE: function is executed for compliance modes only
 * since there is an assumption that log would only be initialized
 * for compliance syncs.
 * Input: primary namespace directory fd, and dirent which
 * needs to be logged before conflict resolution.
 * The function finds the peer lin on the HEAD version and
 * then checks to make sure dirent is committed. Then it ensures
 * that entry exists in compliance store before logging
 * the primary namespace and compliance store path.
 */
void
log_conflict_path(int dir_fd, char *d_name, unsigned d_enc,
    struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(cstore_name);
	struct worm_dir_info wdi = {};
	struct stat p_dir_st = {};
	struct stat p_ent_st = {};
	struct stat c_ent_st = {};
	char *p_path = NULL;
	char *c_path = NULL;
	bool committed = false;
	int cdir_fd = -1;

	if (!has_comp_conflict_log())
		goto out;

	/*
	 * Do a stat to retirve the inode number of primary namespace
	 * directory.
	 */
	ret = fstat(dir_fd, &p_dir_st);
	if (ret != 0) {
		error = isi_system_error_new(errno,
		    "Failed to stat directory just before conflict "
		    "resolution for entry %s", d_name);
		goto out;
	}

	/*
	 * Find the worm dir info to retrive the compliance store peer.
	 */
	ret = extattr_get_fd(dir_fd, EXTATTR_NAMESPACE_IFS, "worm_dir_info",
	    &wdi, sizeof wdi);

	/*
	 * Skip if we are not dealing with compliance mode.
	 */
	if (ret < 0 && errno == ENOATTR)
		goto out;

	if (ret != sizeof(wdi)) {
		error = isi_system_error_new(errno,
		    "Failed to get worm dir info  for lin %{}",
		    lin_fmt(p_dir_st.st_ino));
		goto out;
	}

	if (wdi.wdi_is_cstore) {
		error = isi_system_error_new(EINVAL,
		    "Conflicts cannot be resolved in compliance store directory"
		    " lin %{}", lin_fmt(p_dir_st.st_ino));
		goto out;
	}

	ret = enc_fstatat(dir_fd, d_name, d_enc, &p_ent_st,
	    AT_SYMLINK_NOFOLLOW);
	if (ret != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Unable to stat entry %s at lin %{}", d_name,
		    lin_fmt(p_dir_st.st_ino));
		goto out;
	}

	/*
	 * Its possible that we had already done delete and crashed.
	 * In that case we should have already logged the conflict path.
	 */
	if (ret != 0 && errno == ENOENT)
		goto out;

	/*
	 * We dont log for directories since they cannot be committed.
	 */
	if (S_ISDIR(p_ent_st.st_mode))
		goto out;

	committed = is_worm_committed(p_ent_st.st_ino, HEAD_SNAPID, &error);
	if (error || !committed)
		goto out;

	/*
	 * Now construct the cstore name from the lin number and d_name.
	 */
	get_file_cstore_name(d_name, p_ent_st.st_ino, d_enc, &cstore_name);

	/*
	 * Check that cstore link exists prior to logging and actual
	 * unlink.
	 */
	cdir_fd = ifs_lin_open(wdi.wdi_peer_lin, HEAD_SNAPID, O_RDONLY);
	if (cdir_fd < 0) {
		error = isi_system_error_new(errno,
		    "Unable to open compliance store directory lin %{}",
		    lin_fmt(wdi.wdi_peer_lin));
		goto out;
	}

	/*
	 * Encoding for cstore entry should be same as primary name space one.
	 */
	ret = enc_fstatat(cdir_fd, fmt_string(&cstore_name), d_enc,
	    &c_ent_st, AT_SYMLINK_NOFOLLOW);
	if (ret != 0 ) {
		error = isi_system_error_new(errno,
		    "Unable to stat entry %s at lin %{}",
		    fmt_string(&cstore_name), lin_fmt(wdi.wdi_peer_lin));
		goto out;
	}

	/*
	 * Now get the path for primary namespace directory.
	 */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, p_dir_st.st_ino, HEAD_SNAPID,
	    0, &p_path, &error);
	if (error)
		goto out;

	p_path = realloc(p_path, strlen(p_path) + MAXNAMELEN + 2);
	strncat(p_path, "/", 1);
	strncat(p_path, d_name, MAXNAMELEN);

	/*
	 * Now get the path for compliance store directory.
	 */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, wdi.wdi_peer_lin, HEAD_SNAPID,
	    0, &c_path, &error);
	if (error)
		goto out;

	c_path = realloc(c_path, strlen(c_path) + MAXNAMELEN + 2);
	strncat(c_path, "/", 1);
	strncat(c_path, fmt_string(&cstore_name), MAXNAMELEN);

	log_comp_conflict(p_path, c_path, &error);
	if (error)
		goto out;

out:
	if (cdir_fd != -1)
		close(cdir_fd);
	if (p_path)
		free(p_path);
	if (c_path)
		free(c_path);
	isi_error_handle(error, error_out);
}

void
create_fp_file(char *fp_file, struct isi_error **error_out)
{
	int fd;
	char file_path[MAXPATHLEN];
	struct isi_error *error = NULL;

	snprintf(file_path, MAXPATHLEN, "%s/%s", SIQ_FP_DIR, fp_file);

	fd = open(file_path, O_RDONLY | O_CREAT, 0400);
	if (fd < 0) {
		error = isi_system_error_new(errno, "Failed to open failpoint "
		    "file: %s", file_path);
		goto out;
	}
	close(fd);

out:
	isi_error_handle(error, error_out);
}

void
wait_for_deleted_fp_file(char *fp_file)
{
	char file_path[MAXPATHLEN];

	snprintf(file_path, MAXPATHLEN, "%s/%s", SIQ_FP_DIR, fp_file);

	while (access(file_path, F_OK) == 0)
		siq_nanosleep(1, 0);
}
