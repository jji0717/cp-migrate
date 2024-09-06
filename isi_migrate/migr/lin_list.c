#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_malloc_ostream.h>
#include <isi_util/syscalls.h>
#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/btree/btree.h>
#include <ifs/sbt/sbt.h>
#include <isi_sbtree/sbtree.h>
#include "isirep.h"
#include "work_item.h"
#include "lin_list.h"

#define LIN_LIST_BUF_SIZE 256

/* LIN List entry flag definitions */
#define FL_IS_DIR	(1 << 0)
#define FL_COMMIT	(1 << 1)
#define FL_UNUSED	(1 << 2)


/*  _   _      _
 * | | | | ___| |_ __   ___ _ __ ___
 * | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * |  _  |  __/ | |_) |  __/ |  \__ \
 * |_| |_|\___|_| .__/ \___|_|  |___/
 *              |_|
 */

static btree_key_t *
lin_to_key(uint64_t lin, btree_key_t *key)
{
	ASSERT(key);
	key->keys[0] = lin;
	key->keys[1] = 0;
	return key;
}

static void
pack_entry(struct btree_flags *bt_flags, struct lin_list_entry *ent)
{
	if (ent->is_dir)
		bt_flags->flags |= FL_IS_DIR;

	if (ent->commit)
		bt_flags->flags |= FL_COMMIT;

}

static void
unpack_entry(struct btree_flags *bt_flags, struct lin_list_entry *ent)
{
	ASSERT(ent != NULL);
	memset(ent, 0, sizeof(*ent));

	ent->is_dir = (bt_flags->flags & FL_IS_DIR);
	ent->commit = (bt_flags->flags & FL_COMMIT);
}

void
prune_old_lin_list(const char *policy, char *name_root,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	siq_btree_prune_old(siq_btree_lin_list, policy, name_root,
	    NULL, 0, &error);

	isi_error_handle(error, error_out);
}

void
create_lin_list(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s %s", __func__, name);

	siq_btree_create(siq_btree_lin_list, policy, name, exist_ok,
	    &error);

	isi_error_handle(error, error_out);
}

void
open_lin_list(const char *policy, char *name,
    struct lin_list_ctx **ctx_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	log(TRACE, "%s %s", __func__, name);

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_lin_list, policy, name, &error);
	if (!error) {
		(*ctx_out) = calloc(1, sizeof(struct lin_list_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->lin_list_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);
}

/**
 * Return whether a file with the appropriate name exists.
 */
bool
lin_list_exists(const char *policy, char *name,
    struct isi_error **error_out)
{
	struct lin_list_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	log(TRACE, "%s %s", __func__, name);

	open_lin_list(policy, name, &ctx, &error);
	if (!error) {
		exists = true;
		close_lin_list(ctx);
	} else if (isi_error_is_a(error, ISI_SYSTEM_ERROR_CLASS)) {
		if (ENOENT == isi_system_error_get_error(
		    (const struct isi_system_error *)error)) {
			log(DEBUG, "%s does not exist", name);
			isi_error_free(error);
			error = NULL;
		}
	}
	isi_error_handle(error, error_out);
	return exists;
}

void
close_lin_list(struct lin_list_ctx *ctx)
{
	log(TRACE, "%s", __func__);

	if (ctx == NULL)
		return;

	if (ctx->lin_list_fd > 0)
		close(ctx->lin_list_fd);

	free(ctx);
}

void
remove_lin_list(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s %s", __func__, name);

	siq_btree_remove(siq_btree_lin_list, policy, name, missing_ok,
	    &error);

	isi_error_handle(error, error_out);
}

bool
get_lin_list_info(struct lin_list_info_entry *llie, struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	bool found = false;
	int res;
	int tmp_errno;
	btree_key_t key;
	size_t size;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->lin_list_fd,
	    lin_to_key(LIN_LIST_INFO_LIN, &key), llie, sizeof(*llie), NULL,
	    &size);

	if (res < 0 && errno != ENOENT) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error getting lin list info");
		log(ERROR, "Error getting lin list info: %s",
		    strerror(tmp_errno));
	} else if (res >= 0)
		found = true;

	isi_error_handle(error, error_out);
	return found;
}

/**
 * Set the lin list info. Fail if the lin list info is already set and
 * doesn't equal the value we're setting it to.
 */
void
set_lin_list_info(struct lin_list_info_entry *llie, struct lin_list_ctx *ctx,
    bool force, struct isi_error **error_out)
{
	int res;
	int tmp_errno;
	btree_key_t key;
	struct lin_list_info_entry existing_info;
	size_t size;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (llie->type >= MAX_LIN_LIST) {
		error = isi_system_error_new(EINVAL,
		    "Invalid lin list type: %d", llie->type);
		goto out;
	}

	res = ifs_sbt_add_entry(ctx->lin_list_fd,
	    lin_to_key(LIN_LIST_INFO_LIN, &key), llie, sizeof(*llie), NULL);
	if (res < 0 && errno == EEXIST) {
		res = ifs_sbt_get_entry_at(ctx->lin_list_fd,
		    lin_to_key(LIN_LIST_INFO_LIN, &key), &existing_info,
		    sizeof(existing_info), NULL, &size);
		if (res == 0) {
			if (size != sizeof(struct lin_list_info_entry)) {
				error = isi_system_error_new(EIO,
				    "Bad lin list info size %zu", size);
				log(ERROR, "Bad lin list info size %zu", size);
				goto out;
			}
			if (llie->type != existing_info.type && !force) {
				error = isi_system_error_new(EIO,
				    "Unexpected lin list info type %d, expected"
				    " %d", existing_info.type, llie->type);
				log(ERROR,
				    "Unexpected lin list info type %d, expected"
				    " %d", existing_info.type, llie->type);
				goto out;
			}
			res = ifs_sbt_cond_mod_entry(ctx->lin_list_fd,
			    lin_to_key(LIN_LIST_INFO_LIN, &key),
			    llie, sizeof(*llie), NULL, BT_CM_BUF,
			    &existing_info, sizeof(existing_info), NULL);
		}
	}

	if (res < 0) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting lin list info");
		log(ERROR, "Error setting lin list info: %s",
		    strerror(tmp_errno));
	}

 out:
	isi_error_handle(error, error_out);
}

void
set_lin_list_entry(u_int64_t lin, struct lin_list_entry ent,
    struct lin_list_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	btree_key_t key;
	struct btree_flags flags = {};
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	pack_entry(&flags, &ent);

	exists = lin_list_entry_exists(lin, ctx, &error);
	if (error)
		goto out;

	if (exists)
		res = ifs_sbt_mod_entry(ctx->lin_list_fd, lin_to_key(lin, &key),
		    NULL, 0, &flags);
	else {
		res = ifs_sbt_add_entry(ctx->lin_list_fd, lin_to_key(lin, &key),
		    NULL, 0, &flags);
		if (res < 0) {
			if (errno == EEXIST)
				res = ifs_sbt_mod_entry(ctx->lin_list_fd,
				    lin_to_key(lin, &key), NULL, 0, &flags);
		}
	}

	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting lin_list state lin %llx: %d", lin,
		    flags.flags);
		log(ERROR, "Error setting lin_list state lin %llx: %d: %s",
		    lin, flags.flags, strerror(tmp_errno));
	} else {
		log(DEBUG, "lin_list state entry set for lin %llx: %d", lin,
		    flags.flags);
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * Helper routine to create bulk entry based on old and new lin_list entries
 */
void
add_lin_list_bulk_entry(struct lin_list_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, bool is_new, struct lin_list_entry new_ent,
    struct lin_list_entry old_ent)
{
	log(TRACE, "%s", __func__);

	bentry->fd = ctx->lin_list_fd;

	if (!is_new) {
		bentry->op_type = SBT_SYS_OP_MOD;
		bentry->cm = BT_CM_FLAG;
		pack_entry(&bentry->old_entry_flags, &old_ent);
	} else {
		bentry->op_type = SBT_SYS_OP_ADD;
		bentry->cm = BT_CM_NONE;
	}

	lin_to_key(lin, &(bentry->key));

	pack_entry(&bentry->entry_flags, &new_ent);
}

bool
get_lin_list_entry(u_int64_t lin, struct lin_list_entry *ent,
    struct lin_list_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	bool exists = false;
	btree_key_t key;
	struct btree_flags flags;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->lin_list_fd, lin_to_key(lin, &key),
	    NULL, 0, &flags, NULL);
	if (res == -1) {
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading lin_list state lin %llx", lin);
			log(ERROR, "Error reading lin_list state lin %llx:"
			    " %s", lin, strerror(tmp_errno));
		}
		goto out;
	}

	exists = true;
	unpack_entry(&flags, ent);
	log(DEBUG, "got lin_list state entry for lin %llx", lin);

 out:
	isi_error_handle(error, error_out);
	return exists;
}

bool
get_lin_listkey_at_loc(unsigned num, unsigned den,
    struct lin_list_ctx *ctx, uint64_t *lin, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_pct(ctx->lin_list_fd, num, den,
	    &key, NULL, 0, NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading lin_list state at loc %u/%u", num,
			    den);
			log(ERROR, "Error reading lin_list state at loc "
			    "%u/%u: %s", num, den, strerror(tmp_errno));
		}
	} else {
		exists = true;
		*lin = key.keys[0];
		log(DEBUG, "got lin_list state entry for lin %llx", *lin);
	}
	isi_error_handle(error, error_out);
	return exists;
}

ifs_lin_t
get_lin_list_max_lin(struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct lin_list_iter *iter = NULL;
	struct lin_list_entry ent;
	ifs_lin_t max_lin = -1, cur_lin = -1;
	bool found = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	/* Get the estimated largest lin in the btree */
	found = get_lin_listkey_at_loc(100, 100, ctx, &cur_lin, &error);
	if (error)
		goto out;
	if (!found) {
		/* Empty list. */
		max_lin = INVALID_LIN;
		goto out;
	}

	/* Iterate to the last lin in the btree */
	iter = new_lin_list_range_iter(ctx, cur_lin);
	cur_lin = INVALID_LIN;
	while (found) {
		found = lin_list_iter_next(iter, &cur_lin, &ent, &error);
		if (error)
			goto out;
	}

	max_lin = cur_lin;

 out:
	close_lin_list_iter(iter);
	isi_error_handle(error, error_out);
	return max_lin;
}

ifs_lin_t
get_lin_list_min_lin(struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	bool found = false;
	ifs_lin_t min_lin = INVALID_LIN;
	struct lin_list_iter *iter = NULL;
	struct lin_list_entry ent;
	struct isi_error *error = NULL;

	iter = new_lin_list_iter(ctx);
	found = lin_list_iter_next(iter, &min_lin, &ent, &error);
	if (error)
		goto out;

	ASSERT_IMPLIES(found, min_lin != INVALID_LIN);

out:
	close_lin_list_iter(iter);
	isi_error_handle(error, error_out);
	return min_lin;
}

bool
get_lin_list_lin_in_range(struct lin_list_ctx *ctx, ifs_lin_t min_lin,
    ifs_lin_t max_lin, struct isi_error **error_out)
{
	bool ret = false;
	bool to_return = false;
	ifs_lin_t cur_lin = -1;
	struct lin_list_iter *iter = NULL;
	struct lin_list_entry ent;
	struct isi_error *error = NULL;

	iter = new_lin_list_range_iter(ctx, min_lin);
	ret = lin_list_iter_next(iter, &cur_lin, &ent, &error);
	if (error) {
		goto out;
	}
	if (ret && cur_lin < max_lin) {
		to_return = true;
	} else {
		to_return = false;
	}

out:
	close_lin_list_iter(iter);
	isi_error_handle(error, error_out);
	return to_return;
}

bool
remove_lin_list_entry(u_int64_t lin, struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	struct isi_error *error = NULL;
	bool exists;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_remove_entry(ctx->lin_list_fd, lin_to_key(lin, &key));
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing lin_list state lin %llx", lin);
			log(ERROR, "Error removing lin_list state lin %llx: "
			    "%s", lin, strerror(tmp_errno));
			goto out;
		}
	} else {
		exists = true;
		log(DEBUG, "removed lin_list state entry for lin %llx", lin);
	}

 out:
	isi_error_handle(error, error_out);
	return exists;
}

bool
lin_list_entry_exists(u_int64_t lin, struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	btree_key_t key;
	bool exists;
	struct isi_error *error = NULL;
	int tmp_errno;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->lin_list_fd, lin_to_key(lin, &key),
	    NULL, 0, NULL, NULL);
	if (res == 0) {
		exists = true;
	} else {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading lin_list state lin %llx", lin);
			log(ERROR, "Error reading lin_list state lin %llx: "
			    "%s", lin, strerror(tmp_errno));
			goto out;
		}
	}

	log(DEBUG, "lin_list entry for lin %llx %s", lin,
	    (exists) ? "exists" : "does not exist");

 out:
	isi_error_handle(error, error_out);
	return exists;
}

/*  ___ _                 _
 * |_ _| |_ ___ _ __ __ _| |_ ___  _ __
 *  | || __/ _ \ '__/ _` | __/ _ \| '__|
 *  | || ||  __/ | | (_| | || (_) | |
 * |___|\__\___|_|  \__,_|\__\___/|_|
 */

struct lin_list_iter *
new_lin_list_iter(struct lin_list_ctx *ctx)
{
	struct lin_list_iter *iter;

	log(TRACE, "%s", __func__);

	iter = calloc(1, sizeof(struct lin_list_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	iter->key.keys[0] = 0;
	iter->key.keys[1] = 0;

	return iter;
}

struct lin_list_iter *
new_lin_list_range_iter(struct lin_list_ctx *ctx, uint64_t lin)
{
	struct lin_list_iter *iter;

	log(TRACE, "%s", __func__);

	iter = calloc(1, sizeof(struct lin_list_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	iter->key.keys[0] = lin;
	iter->key.keys[1] = 0;

	return iter;
}

bool
lin_list_iter_next(struct lin_list_iter *iter, u_int64_t *lin,
    struct lin_list_entry *lent, struct isi_error **error_out)
{
	int res = 0;
	char buf[LIN_LIST_BUF_SIZE];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	btree_key_t next_key;
	errno = 0;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	/* last item has already been read if key was set to -1 on previous
	 * call */
	if (iter->key.keys[0] == -1)
		goto out;

	do {
		res = ifs_sbt_get_entries(iter->ctx->lin_list_fd, &iter->key,
		    &next_key, LIN_LIST_BUF_SIZE, buf, 1, &num_out);
		iter->key = next_key;
		if (res == -1) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno, "Error getting "
			    "next lin_list mapping");
			log(ERROR, "Error getting next lin_list mapping: %s",
			    strerror(tmp_errno));
			goto out;
		} else if (num_out == 0)
			goto out;

		ent = (struct sbt_entry *)buf;
	} while (ent->key.keys[0] == LIN_LIST_INFO_LIN);

	got_one = true;
	*lin = ent->key.keys[0];
	unpack_entry(&ent->flags, lent);
	ASSERT(ent->size_out == 0);

 out:
	isi_error_handle(error, error_out);
	return got_one;
}

void
close_lin_list_iter(struct lin_list_iter *iter)
{
	log(TRACE, "%s", __func__);

	if (iter)
		free(iter);
}

void
get_resync_name(char *buf, size_t buf_len, const char *policy, bool src)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	snprintf(buf, buf_len, "%s_%s_%s", policy, "resync",
	    src ? "src" : "dst");
}

void
get_worm_commit_name(char *buf, size_t buf_len, const char *policy)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	snprintf(buf, buf_len, "%s_%s", policy, "commit");
}

/*
 * This routine gets valid path inside lin_list directory.
 * NOTE : Currently we dont handle exclusions, predicates
 */
char *
get_valid_lin_list_path(uint64_t lin, ifs_snapid_t snap)
{

	struct isi_error *error = NULL;
	char *path = NULL;

	/* Now get utf8 path for the lin */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, lin, snap, 0, &path, &error);
	if (error)
		goto out;

 out:
	if (error || !path) {
		/* Construct default path as lin information*/
		asprintf(&path, "Lin %llx", lin);
		if (error)
			isi_error_free(error);
	}

	ASSERT(path != NULL);
	return path;
}

static void
add_lin_list_entries(struct sbt_bulk_entry *lin_list_entries, int num_entries,
    struct lin_list_ctx *ctx, int log_index, struct lin_list_log *logp,
    struct isi_error **error_out)
{
	int res;
	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	/*
	 * First lets try to add all entries.
	 * It could fail if there are concurrent updates by
	 * another worker or stale old lin_list entry. So in case of
	 * bulk operation failure, we try to update entries
	 * one transaction at a time.
	 */
	res = ifs_sbt_bulk_op(num_entries, lin_list_entries);
	if (res == -1 && errno != EEXIST && errno != ERANGE) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d lin_list state entries from log "
		    "to disk with beginning lin %llx", num_entries,
		    lin_list_entries[0].key.keys[0]);
		goto out;
	}

	if (res == 0)
		goto out;

	i = log_index - num_entries;
	/* Bulk transaction failed, lets try to do update one at a time */
	while (j < num_entries) {
		set_lin_list_entry(logp->entries[i].lin,
		    logp->entries[i].new_ent, ctx, &error);
		if (error)
			goto out;

		i++;
		j++;
	}

 out:
	isi_error_handle(error, error_out);
}

static int
lin_list_compr(const void *elem1, const void *elem2)
{
	struct lin_list_log_entry *entry1 =
	    (struct lin_list_log_entry *) elem1;
	struct lin_list_log_entry *entry2 =
	    (struct lin_list_log_entry *) elem2;

	if (entry1->lin < entry2->lin)
		return -1;
	else if (entry1->lin == entry2->lin)
		return 0;
	else
		return 1;
}

/*
 * This function does flush of all entries from in-memory
 * log to on-disk lin_list state sbt.
 */
void
flush_lin_list_log(struct lin_list_log *logp, struct lin_list_ctx *ctx,
    struct isi_error **error_out)
{
	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry lin_list_entries[SBT_BULK_MAX_OPS];
	struct lin_set lset;

	log(TRACE, "%s", __func__);

	/* First lets sort the the rep entries */
	qsort(logp->entries, logp->index, sizeof(struct lin_list_log_entry),
	    lin_list_compr);

	bzero(lin_list_entries,
	    sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	lin_set_init(&lset);

	while (i < logp->index) {
		if (lin_set_contains(&lset, logp->entries[i].lin) ||
		    (j == SBT_BULK_MAX_OPS)) {
			ASSERT (i != 0);
			ASSERT (j != 0);
			add_lin_list_entries(lin_list_entries, j, ctx, i,
			    logp, &error);
			if (error)
				goto out;

			j = 0;
			lin_set_truncate(&lset);
		}

		add_lin_list_bulk_entry(ctx, logp->entries[i].lin,
		    &lin_list_entries[j], logp->entries[i].is_new,
		    logp->entries[i].new_ent, logp->entries[i].old_ent);

		lin_set_add(&lset, logp->entries[i].lin);
		j++;
		i++;
	}

	if (j) {
		add_lin_list_entries(lin_list_entries, j, ctx, i, logp,
		    &error);
		if (error)
			goto out;
	}

	logp->index = 0;
 out:
	lin_set_clean(&lset);
	isi_error_handle(error, error_out);
}

void
copy_lin_list_entries(struct lin_list_ctx *ctx, ifs_lin_t *lins,
    struct lin_list_entry *entries, int num_entries,
    struct isi_error **error_out)
{
	int i = 0;
	int res;
	bool exists;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry bentries[SBT_BULK_MAX_OPS];

	log(TRACE, "%s", __func__);

	ASSERT(num_entries <= SBT_BULK_MAX_OPS);
	ASSERT(num_entries > 0 && entries != NULL && lins != NULL);
	bzero(bentries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);

	while (i < num_entries) {
		bentries[i].fd = ctx->lin_list_fd;
		lin_to_key(lins[i], &(bentries[i].key));
		exists = lin_list_entry_exists(lins[i], ctx, &error);
		if (error)
			goto out;

		if (exists)
			bentries[i].op_type = SBT_SYS_OP_MOD;
		else
			bentries[i].op_type = SBT_SYS_OP_ADD;

		bentries[i].cm = BT_CM_NONE;
		pack_entry(&bentries[i].entry_flags, &entries[i]);
		i++;
	}

	if (i) {
		res = ifs_sbt_bulk_op(i, bentries);
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Unable to copy entries with key range %llx to"
			    " %llx to lin_list state", lins[0],
			    lins[num_entries - 1]);
			goto out;
		}
	}
 out:
	isi_error_handle(error, error_out);
}

/*
 * Add lin_list_entry to in-memory log. If the log is full, then we would
 * flush the log to disk before adding a new entry.
 */
void
lin_list_log_add_entry(struct lin_list_ctx *ctx, struct lin_list_log *logp,
    ifs_lin_t lin, bool is_new, struct lin_list_entry *new_ent,
    struct lin_list_entry *old_ent, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct lin_list_entry null_entry = {};

	log(TRACE, "%s", __func__);

	ASSERT(new_ent != NULL);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		flush_lin_list_log(logp, ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}

	logp->entries[logp->index].is_new = is_new;
	ASSERT(is_new || old_ent != NULL);

	logp->entries[logp->index].old_ent = (old_ent != NULL) ? *old_ent :
	    null_entry;

	logp->entries[logp->index].lin = lin;
	logp->entries[logp->index].new_ent = *new_ent;

	logp->index++;

out:
	isi_error_handle(error, error_out);
}
