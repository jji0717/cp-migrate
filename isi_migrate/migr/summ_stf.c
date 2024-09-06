#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_lin_open.h>
#include <isi_sbtree/sbtree.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "summ_stf.h"


struct summ_stf_ctx *
summ_stf_alloc(void)
{

	struct summ_stf_ctx *ctx;
	ctx = calloc(1, sizeof(struct summ_stf_ctx));
	ASSERT(ctx != NULL);
	ctx->summ_stf_fd = -1;
	ctx->snapset = NULL;
	return ctx;
}

void
summ_stf_free(struct summ_stf_ctx *ctx)
{
	int i;
	if (ctx->summ_stf_fd > 0) 
		close(ctx->summ_stf_fd);
	if (ctx->snapset) {
		snapid_set_clean(ctx->snapset);
		free(ctx->snapset);
		ctx->snapset = NULL;
	}
	for (i = 0; i < STF_COUNT; i++) {
		if (ctx->stfs[i].lins) {
			free(ctx->stfs[i].lins);
			ctx->stfs[i].lins = NULL;
		}
	}
	free(ctx);
}

void
prune_old_summary_stf(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_prune_old(siq_btree_summ_stf, policy, name_root, snap_kept,
	    num_kept, &error);

	isi_error_handle(error, error_out);
}

void
create_summary_stf(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	
	siq_btree_create(siq_btree_summ_stf, policy, name, exist_ok, &error);

	isi_error_handle(error, error_out);
}

void
open_summary_stf(const char *policy, char *name, struct summ_stf_ctx **ctx,
     struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(ctx);

	fd = siq_btree_open(siq_btree_summ_stf, policy, name, &error);
	if (!error) {
		*ctx = summ_stf_alloc();
		ASSERT(*ctx);
		(*ctx)->summ_stf_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);

}

/*
 * Load list of snap ids between snap1 and snap2 into summary context
 */
static void
snapids_load_list(ifs_snapid_t snap1, ifs_snapid_t snap2, 
    struct snapid_set *snapset, struct isi_error **error_out)
{
	struct dirent	*dp;
	int	snapdir_fd = -1;
	struct	stat	sb;
	size_t  bufsize;
	char	*buf = NULL, *ebuf, *cp;
	int	nbytes;
	off_t	base;
	ifs_snapid_t	sid;
	struct isi_error *error = NULL;

	ASSERT(snapset != NULL);

	if ((snapdir_fd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID,
	    O_RDONLY)) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to open STF root dir");
		goto out;
	}

	if (fstat(snapdir_fd, &sb) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to stat STF root dir");
		goto out;
	}

	bufsize = DIRBLOCKS * sb.st_blksize;

	buf = malloc(bufsize);
	assert(buf != NULL);

	while ((nbytes = enc_getdirentries(snapdir_fd, buf,
	    bufsize, &base)) > 0) {

		ebuf = buf + nbytes;
		cp = buf;
		while (cp < ebuf) {
			dp = (struct dirent *)cp;

			if (strcmp(dp->d_name, ".") == 0 ||
			    strcmp(dp->d_name, "..") == 0) { 

				cp = cp + dp->d_reclen;
				continue;
			}
		
			sid = strtoull(dp->d_name, (char **)NULL, 10);
			/* In case its in SyncIQ range, copy it */
			if (sid >= snap1 && sid < snap2)
				snapid_set_add(snapset, sid);

			cp = cp + dp->d_reclen;
		}
	}

	if (nbytes < 0) {
		error = isi_system_error_new(errno,
		    "Failed to read snapids ");
	} 
out:
	if (buf)
		free(buf);
	if (snapdir_fd > 0)
		close(snapdir_fd);
	isi_error_handle(error, error_out);
	return;
}

void
snapset_alloc(struct summ_stf_ctx *ctx, ifs_snapid_t snap1,
    ifs_snapid_t snap2, struct isi_error **error_out)
{
	int i;
	struct isi_error *error = NULL;

	ctx->snapset = malloc(sizeof(struct snapid_set));
	snapid_set_init(ctx->snapset);
	/*
	 * Now we will build list of snapshot Ids between two 
	 * SyncIQ snapshots
	 */
	snapids_load_list(snap1, snap2, ctx->snapset, &error);
	if (error)
		goto out;

	if (snapid_set_empty(ctx->snapset) ||
	    !snapid_set_contains(ctx->snapset, snap1)) {
		error = isi_system_error_new(ENOENT,
		    "Valid snapshots not found between %llu and %llu",
		    snap1, snap2);
		goto out;
	}
	/* Make sure we initialize all stf entries */
	for (i = 0; i < STF_COUNT; i++) {
		ASSERT(ctx->stfs[i].lins == NULL);
		ctx->stfs[i].lins = calloc(LIN_STF_CNT, sizeof(ifs_lin_t));
		ctx->stfs[i].fd = -1;
		ctx->stfs[i].lin_cnt = 0;
	}
	ctx->prev_lin = ctx->min_lin;
	ctx->stfs_cnt = 0;

out:
	isi_error_handle(error, error_out);

}

static void
snapset_done(struct summ_stf_ctx *ctx)
{
	int i;
	ASSERT(ctx->summ_stf_fd > 0);
	 if (ctx->snapset) {
		snapid_set_clean(ctx->snapset);
		free(ctx->snapset);
		ctx->snapset = NULL;
	}
	for (i = 0; i < STF_COUNT; i++) {
		if (ctx->stfs[i].lins) {
			free(ctx->stfs[i].lins);
			ctx->stfs[i].lins = NULL;
		}
	}
}
/*
 * Add  STF_COUNT stf entries into out stf list 
 */
static bool
build_stfs(struct summ_stf_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_snapid_t snapid;
	bool found = false;
	int fd;
	int i = 0;

	ASSERT(!snapid_set_empty(ctx->snapset));

	while ((i < STF_COUNT) && !snapid_set_empty(ctx->snapset)) {
		snapid = *snapid_set_find_max(ctx->snapset);
		fd = open_stf(snapid, &error);
		if (error)
			goto out;
		ASSERT(fd != 0);
		if (fd > 0) {
			ctx->stfs[i].fd = fd;
			i++;
		} else {
			log(DEBUG, "Skipping snapshot id %llu", snapid);
		}
		snapid_set_remove(ctx->snapset, snapid);
	}

	if (i > 0) {
		found = true;
		ctx->stfs_cnt = i;
	}

out:	
	isi_error_handle(error, error_out);
	return found;
}

static void
finish_stfs(struct summ_stf_ctx *ctx)
{
	int i;
	
	for (i = 0; i < STF_COUNT ; i++) {
		if (ctx->stfs[i].fd > 0) {
			close(ctx->stfs[i].fd);
		}
		ctx->stfs[i].fd = -1;
		ctx->stfs[i].lin_cnt = 0;
	}
	ctx->prev_lin = ctx->min_lin;
	ctx->stfs_cnt = 0;
}

static void
get_last_stf_lin(int fd, ifs_lin_t *lin, struct isi_error **error_out)
{
	
	ifs_lin_t prev_lin = *lin;
	ifs_lin_t lins[100];
	int count = 100;
	int j, res;
	struct isi_error *error = NULL;

	while(1) {
		res = ifs_snap_get_lins(fd, &prev_lin, lins, count,
		    NULL, NULL);
		if (res) {
			error = isi_system_error_new(errno,
			    "Error reading lins from stf ");
			goto out;
		}
			
		for (j = 0; j < count; j++) {
			if (0 == lins[j])
				break;
		}
		if (!j)
			break;
		ASSERT(lins[j - 1] > *lin);
		*lin = lins[j - 1];
		prev_lin = lins[j - 1];
	}
out:
	isi_error_handle(error, error_out);
}

static void
find_min_max_lin(int fd, ifs_snapid_t snapid, ifs_lin_t *min, ifs_lin_t *max,
    struct isi_error **error_out)
{
	
	int res = 0;
	struct isi_error *error = NULL;
	ifs_lin_t lin_in = 0;
	ifs_lin_t lin_out = 0;

	log(TRACE, "find_min_max_lin");

	*min = *max = 0;

	res = ifs_snap_get_lins(fd, &lin_in, &lin_out, 1, NULL, NULL);
	if (res) {
		error = isi_system_error_new(errno,
		    "Error reading first lin from stf %llu", snapid);
		goto out;
	} else {
		*min = lin_out;
	}

	lin_out = 0;
	res = ifs_snap_get_lin_at(fd, 100, 100, &lin_out);
	/* The stf does not have any lins */
	if (res == -1 && errno == ENOENT) {
		*max = *min;
		goto out;
	}

	if (res) {
		error = isi_system_error_new(errno,
		    "Error reading stf %llu at loc %u/%u", snapid, 100, 100);
		goto out;
	} else {
		/*
		 * We are still not sure its last lin, so 
		 * iterate the last few lins.
		 */
		if (lin_out > 0)
			get_last_stf_lin(fd, &lin_out, &error);
		*max = lin_out;
		
	}
	log(TRACE, "snapid %llu min %llu max %llu", snapid, *min, *max);	
out:
	isi_error_handle(error, error_out);
}


/*
 * Routine to find minimum and maximum lin value in the list
 * of stfs between two SyncIQ snapshots.
 */

void
snapset_min_max( ifs_snapid_t snap1, ifs_snapid_t snap2, ifs_lin_t *min,
    ifs_lin_t *max, struct isi_error **error_out)
{
	int fd = -1;
	struct isi_error *error = NULL;
	struct snapid_set *snapset = NULL;
	ifs_snapid_t snapid;
	ifs_lin_t tmin = 0, tmax = 0;

	ASSERT(min != NULL);
	ASSERT(max != NULL);
	*min = *max = 0;
	snapset = malloc(sizeof(struct snapid_set));
	ASSERT(snapset != NULL);

	/* First find all the snapshot Ids between SyncIQ snapshots */
	snapid_set_init(snapset);
	snapids_load_list(snap1, snap2, snapset, &error);
	if (error)
		goto out;

	if (snapid_set_empty(snapset)) {
		error = isi_system_error_new(ENOENT,
		    "No snapshots found between %llu and %llu",
		    snap1, snap2);
		goto out;
	}

	/* Now open each snapid stf and find min and max lin value */
	while(!snapid_set_empty(snapset)) {
		snapid = *snapid_set_find_max(snapset);
		fd = open_stf(snapid, &error);
		if (error)
			goto out;
		ASSERT(fd != 0);
		if (fd > 0) {
			find_min_max_lin(fd, snapid, &tmin, &tmax, &error);
			if (error)
				goto out;

			if (*min == 0 || (tmin > 0 && tmin < *min))
				*min = tmin;
			if (*max == 0 || tmax > *max)
				*max = tmax;
		} else {
			log(DEBUG, "Skipping snapshot id %llu", snapid);
		}
		snapid_set_remove(snapset, snapid);
		if (fd > 0)
			close(fd);
		fd = -1;
	}
	log(TRACE, "For snapshot set [%llu - %llu] min_lin = %llx max_lin "
	    "is %llx", snap1, snap2, *min, *max);
out:
	if (fd > 0)
		close(fd);
	snapid_set_clean(snapset);
	free(snapset);
	isi_error_handle(error, error_out);
}

int
stf_iter_next_lins(struct summ_stf_ctx *ctx, ifs_snapid_t snap1,
    ifs_snapid_t snap2, ifs_lin_t *lins, int count,
    struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	int found = 0;

	if (!ctx->snapset) {
		snapset_alloc(ctx, snap1, snap2, &error);
		if (error)
			goto out;	
	}

	while(1) {
		if (ctx->stfs_cnt == 0) {
			/* stf set empty we can stop now */
			if (snapid_set_empty(ctx->snapset)) {
				snapset_done(ctx);
				found = 0;
				*lins = 0;
				goto out;
			}

			found = build_stfs(ctx, &error);
			if (!found || error) 
				goto out;
		}

		found = get_stf_next_lins(ctx, lins, count, &error);
		if (error)
			goto out;

		if (found)
			break;

		/* We are done with current stf block */
		finish_stfs(ctx);
	}

out:
	isi_error_handle(error, error_out);
	return found;

}

void
stf_iter_status(struct summ_stf_ctx *ctx, ifs_lin_t *prev_lin)
{
	*prev_lin = ctx->prev_lin;
}

/*
 * Add num_entries to summary stf sbt 
 */
static void
add_stf_entries(struct sbt_bulk_entry *stf_entries, int num_entries,
    struct summ_stf_ctx *ctx, struct isi_error **error_out)
{
	int res;
	int i = 0;
	struct isi_error *error = NULL;

	log(TRACE, "add_stf_entries");	
	/*
	 * First lets try to add all entries.
	 * It could fail sometimes due to restarts.
	 * i.e we might have added few of those entries
	 * in previous run, but we can take chance since
	 * restarts are minimal.
	 */

	res = ifs_sbt_bulk_op(num_entries, stf_entries);
	if (res == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d stf entries begining with "
		    " %llx from log to disk", num_entries,
		    stf_entries[0].key.keys[0]);
		goto out;
	}
	
	if (res == 0)
		goto out;

	/* There are some entries already in sbt */
	while (i < num_entries) {
		res = ifs_sbt_get_entry_at(ctx->summ_stf_fd,
		    &stf_entries[i].key, NULL, 0, NULL, NULL);
		if (res == -1 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Error while reading summary stf entry %llx",
			    stf_entries[i].key.keys[0]);
			goto out;
		}
			
		if (res == 0) {
		    stf_entries[i].op_type = SBT_SYS_OP_MOD;
		}
		i++;
	}

	res = ifs_sbt_bulk_op(num_entries, stf_entries);
	if (res == -1) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d stf entries begining with "
		    " %llx from log to disk", num_entries,
		    stf_entries[0].key.keys[0]);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

bool
summ_stf_log_entries(struct summ_stf_ctx *ctx)
{
	ASSERT(ctx != NULL);
	if (ctx->stf_log.index > 0)
		return true;
	else
		return false;
}

bool
summ_stf_log_minimum(struct summ_stf_ctx *ctx, ifs_lin_t *min_lin)
{
	struct stf_log *logp = &(ctx->stf_log);

	ASSERT(ctx != NULL);
	if (ctx->stf_log.index > 0) {
		*min_lin = logp->lin[0];
		return true;
	} else {
		*min_lin = 0;
		return false;
	}
}

/*
 * This function does flush of all entries from in-memory
 * summary stf log to on-disk summary stf sbt.
 */
void
flush_summ_stf_log(struct summ_stf_ctx *ctx, struct isi_error **error_out)
{

	int i = 0;
	struct isi_error *error = NULL;
	int j = 0;
	struct sbt_bulk_entry stf_entries[SBT_BULK_MAX_OPS];
	struct btree_flags sbt_flags = {};
	struct stf_log *logp = &(ctx->stf_log);

	log(TRACE, "flush_stf_log");
	bzero(stf_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	while(i < logp->index) {

		stf_entries[j].fd = ctx->summ_stf_fd;
		stf_entries[j].op_type = SBT_SYS_OP_ADD;
		stf_entries[j].key.keys[0] = logp->lin[i];
		stf_entries[j].key.keys[1] = 0;
		stf_entries[j].entry_buf = NULL;
		stf_entries[j].entry_size = 0;
		stf_entries[j].entry_flags = sbt_flags;
		stf_entries[j].cm = BT_CM_NONE;
		stf_entries[j].old_entry_buf = NULL;
		stf_entries[j].old_entry_size = 0;
		j++;
	
		if (j == SBT_BULK_MAX_OPS) {
			add_stf_entries(stf_entries, j, ctx, &error);
			if (error)
				goto out;

			j = 0;
		}

		i++;
	}

	if (j) {
		add_stf_entries(stf_entries, j, ctx, &error);
		if (error)
			goto out;
	}

	logp->index = 0;
out:
	isi_error_handle(error, error_out);
}

/*
 * Set an entry into summary stf btree
 */
void
set_stf_entry(u_int64_t lin, struct summ_stf_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct stf_log *logp = &(ctx->stf_log);
	int i;

	log(TRACE, "set_stf_entry");
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		/* Log is full, so flush it */
		flush_summ_stf_log(ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}
	/*
	 * Check if the entry is already there in log.
	 * This could happen when we see lins repeated
	 * between two stf btree. This check could be 
	 * avoided in future by looking for EINVAL 
	 * errors in add_stf_entries.
	 */
	for(i = 0; i < logp->index; i++) {
		if (logp->lin[i] == lin)
			goto out;
	}

	logp->lin[logp->index] = lin;
	logp->index++;

out:
	isi_error_handle(error, error_out);
}

/*
 * Remove an entry from the summary stf btree.
 */
bool
remove_stf_entry(u_int64_t lin, struct summ_stf_ctx *ctx, 
    struct isi_error **error_out)
{
	int res = 0;
	bool exists = false;
	btree_key_t key;
	struct isi_error *error = NULL;

	/* Removes are not supported while adds are pending. */
	ASSERT(ctx->stf_log.index == 0);

	key.keys[0] = lin;
	key.keys[1] = 0;
	res = ifs_sbt_remove_entry(ctx->summ_stf_fd, &key);
	if (res == -1) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno,
			    "Error %d removing lin %llx", errno, lin);
		goto out;
	}

	exists = true;

out:
	isi_error_handle(error, error_out);
	return exists;
}

/**
 * Get utmost 'entries' number of lins from current STF set.
 * Last lin returned is stored in ctx->prev_lin.
 * Returns the number of lins read.
 */
int
get_stf_next_lins(struct summ_stf_ctx *ctx, ifs_lin_t *lins,
    int entries, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int rv;
	int i, j, found = 0;
	ifs_lin_t min_lin;
	ifs_lin_t prev_lin;
	int index[LIN_STF_CNT];
	int min_index = -1;

	ASSERT(entries <= LIN_STF_CNT);
	ASSERT(lins != NULL);

	/*
	 * First read LIN_STF_CNT lins from each stf.
	 * Always start from last prev lin
	 */
	for (i = 0; i < ctx->stfs_cnt; i++) {
		prev_lin = ctx->prev_lin;
		rv = ifs_snap_get_lins(ctx->stfs[i].fd, &prev_lin,
		    ctx->stfs[i].lins, entries, NULL, NULL);
		if (rv) {
			error = isi_system_error_new(errno,
			    "Error %d reading lins starting at %llx",
			    errno, prev_lin);
			goto out;
		}
		for (j = 0; j < entries; j++) {
			if (0 == ctx->stfs[i].lins[j] || 
			    ctx->stfs[i].lins[j] >= ctx->max_lin)
				break;
		}
		ctx->stfs[i].lin_cnt = j;
	}

	/*
	 * Read the 'entries' lins in sorted order.
	 * We keep index[] array which contains the 
	 * index of next lin to be read from each stf.
	 */ 
	bzero(index, sizeof(int) * LIN_STF_CNT);
	found = 0;
	while (found < entries) {
		min_lin = -1;
		for (i = 0; i < ctx->stfs_cnt; i++) {
			/* We can skip STFs for which we have read all lins */
			if (index[i] >= ctx->stfs[i].lin_cnt)
				continue;
			if (ctx->stfs[i].lins[index[i]] < min_lin) {
				min_lin = ctx->stfs[i].lins[index[i]];
				min_index = i;
			} else if (ctx->stfs[i].lins[index[i]] == min_lin) {
				/* We can skip duplicate lins. */
				index[i]++;
			}
		}

		if (min_lin == -1)
			break;

		lins[found] = min_lin;
		index[min_index]++;
		found++;
	}
	
	if (found > 0)
		ctx->prev_lin = lins[found - 1];
	
 out:
	isi_error_handle(error, error_out);
	if (!error)
		return found;
	else
		return 0;
}

void
set_stf_lin_range(struct summ_stf_ctx *ctx, ifs_lin_t min_lin, ifs_lin_t max_lin)
{
	ctx->min_lin = min_lin;
	ctx->max_lin = max_lin;
}

/**
 * Open the STF for the provided snapid.
 */
int
open_stf(ifs_snapid_t snapid, struct isi_error **error_out)
{
	int ret;
	int dfd = -1, sfd = -1;
	char buff[80];
	struct isi_error *error = NULL;
	struct stf_stat stfstat = {};

	ASSERT (snapid != INVALID_SNAPID && snapid != HEAD_SNAPID &&
	    snapid != VD_SNAPID);

	dfd = ifs_lin_open(STF_SNAPIDS_LIN, HEAD_SNAPID, O_RDONLY);
	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "Error %d opening snapids dir", errno);
		goto out;
	}

	sprintf(buff, "%llu", snapid);
	sfd = enc_openat(dfd, buff, ENC_DEFAULT, O_RDONLY);
	if (sfd == -1 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Error %d opening STF %llu", errno, snapid);
		goto out;
	}

	if (sfd == -1)
		goto out;

	ret = ifs_snap_stat(sfd, &stfstat);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Unable to stat snap %llu stf file", snapid);
		goto out;
	}

	/* Skip Non STF files */
	if (stfstat.sf_type != SF_STF) {
		close(sfd);
		sfd = -1;
		goto out;
	}

 out:
	if (dfd != -1)
		close(dfd);

	isi_error_handle(error, error_out);
	return sfd;
}

/*
 * If resume_lin is nonzero, then we iterate from that point 
 */
struct summ_stf_iter *
new_summ_stf_iter(struct summ_stf_ctx *summ_ctx, ifs_lin_t resume_lin)
{
	struct summ_stf_iter *iter;

	log(TRACE, "new_summ_stf_iter");
	ASSERT(summ_ctx != NULL);

	iter = calloc(1, sizeof(struct summ_stf_iter));
	ASSERT(iter);

	iter->ctx = summ_ctx;
	iter->key.keys[0] = resume_lin;
	iter->key.keys[1] = 0;
	return iter;
}

void
close_summ_stf_iter(struct summ_stf_iter *iter)
{
	log(TRACE, "close_summ_stf_iter");

	if (iter)
		free(iter);
}

/*
 * Load list of summary stf lins 
 */
int
get_summ_stf_next_lins(struct summ_stf_iter *iter, ifs_lin_t *lins,
    int count, struct isi_error **error_out)
{
	int i;
	struct isi_error *error = NULL;
	int res;
	char buf[256];
	size_t num_out = 0;
	struct sbt_entry *ent = NULL;
	btree_key_t next_key;

	ASSERT(lins != NULL);
	memset(lins, 0, sizeof(uint64_t) * count);

	i = 0;
	while (i < count) {
		/* last item has already been read if key was set to -1 on 
		 * previous call
		 */
		if (iter->key.keys[0] == -1) {
			break;
		}

		res = ifs_sbt_get_entries(iter->ctx->summ_stf_fd, &iter->key,
		    &next_key, 256, buf, 1, &num_out);
		iter->key = next_key;	
		/* num_out of 0 indicates end of non-empty list. EFAULT
		 * indicates empty list
		 */
		if (num_out == 0 || (res == -1 && errno == EFAULT)) {
			break;
		}
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Error getting next summ stf lin");
			goto out;
		}
		ent = (struct sbt_entry *)buf;
		lins[i] = ent->key.keys[0];
		i++;
	}

out:
	isi_error_handle(error, error_out);
	return i;
}

bool
get_summ_stf_at_loc(unsigned num, unsigned den, struct summ_stf_ctx *summ_ctx,
    uint64_t *lin, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "get_entry");

	res = ifs_sbt_get_entry_pct(summ_ctx->summ_stf_fd,  num, den, 
	    &key, NULL, 0, NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno);
			log(ERROR, "Error reading repstate at loc %u/%u: %s",
			    num, den, strerror(tmp_errno));
		}
	} else {
		exists = true;
		*lin = key.keys[0];
		log(DEBUG, "got repstate entry for lin %llx", *lin);
	}
	isi_error_handle(error, error_out);
	return exists;
}

ifs_lin_t
get_summ_stf_max_lin(struct summ_stf_ctx *stf_ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct summ_stf_iter *stf_iter = NULL;
	ifs_lin_t max_lin = -1, cur_lin = -1;
	int num_found = 0;
	bool found = false;

	log(TRACE, "%s", __func__);

	ASSERT(stf_ctx != NULL);

	/* Get the estimated largest lin in the btree */
	found = get_summ_stf_at_loc(100, 100, stf_ctx, &cur_lin, &error);	
	if (error)
		goto out;	
	if (!found) {
		error = isi_system_error_new(errno,
		    "Error reading summary stf at loc 100/100:"
		    "Entry not found");
		goto out;
	}

	/* Iterate to the last lin in the btree */
	stf_iter = new_summ_stf_iter(stf_ctx, cur_lin);
	num_found = 1;
	while(num_found) {
		max_lin = cur_lin;
		num_found = get_summ_stf_next_lins(stf_iter, &cur_lin, 1, &error);
		if (error)
			goto out;
	}

out:
	close_summ_stf_iter(stf_iter);
	isi_error_handle(error, error_out);
	return max_lin;
}

bool
summ_stf_entry_exists(struct summ_stf_ctx *summ_ctx,
    uint64_t lin, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "summ_stf_get_entry");

	key.keys[0]= lin;
	key.keys[1] = 0;

	res = ifs_sbt_get_entry_at(summ_ctx->summ_stf_fd, &key, NULL, 0,
	    NULL, NULL);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading summ stf entry %llx", lin);
		}
	} else {
		exists = true;
	}

	log(DEBUG, "summ stf entry for lin %llx %s", lin,
	    (exists) ? "exists" : "does not exist");
	isi_error_handle(error, error_out);
	return exists;
}

void
get_summ_stf_name(char *buf, const char *job_name, ifs_snapid_t snap,
    bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(job_name != NULL);
	ASSERT(snap > 0);
	if (restore) {
		sprintf(buf, "%s_summ_stf_restore_%llu", job_name, snap);
	} else {	
		sprintf(buf, "%s_summ_stf_%llu", job_name, snap);
	}
}

void
get_changelist_summ_stf_name(char *buf, ifs_snapid_t snap1, ifs_snapid_t snap2)
{
	ASSERT(buf != NULL);
	ASSERT(snap1 > 0);
	ASSERT(snap2 > snap1);
	sprintf(buf, "%llu_%llu_summ_stf", snap1, snap2);
}

void
get_summ_stf_pattern(char *buf, const char *job_name, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(job_name != NULL);
	if (restore) {
		sprintf(buf, "%s_summ_stf_restore_", job_name);
	} else {
		sprintf(buf, "%s_summ_stf_", job_name);
	}
}

