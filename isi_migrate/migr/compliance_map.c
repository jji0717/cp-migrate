#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "ifs/ifs_types.h"
#include <ifs/ifs_lin_open.h>
#include <isi_sbtree/sbtree.h>
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "compliance_map.h"

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

/*
 *   ____                      _ _                        __  __
 *  / ___|___  _ __ ___  _ __ | (_) __ _ _ __   ___ ___  |  \/  | __ _ _ __
 * | |   / _ \| '_ ` _ \| '_ \| | |/ _` | '_ \ / __/ _ \ | |\/| |/ _` | '_ \
 * | |__| (_) | | | | | | |_) | | | (_| | | | | (_|  __/ | |  | | (_| | |_) |
 *  \____\___/|_| |_| |_| .__/|_|_|\__,_|_| |_|\___\___| |_|  |_|\__,_| .__/
 *                      |_|                                           |_|
 *  ____      _____
 * | __ )    |_   _| __ ___  ___
 * |  _ \ _____| || '__/ _ \/ _ \
 * | |_) |_____| || | |  __/  __/
 * |____/      |_||_|  \___|\___|
 */

void
create_compliance_map(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s %s", __func__, name);

	siq_btree_create(siq_btree_compliance_map, policy, name, exist_ok,
	    &error);

	isi_error_handle(error, error_out);
}

void
open_compliance_map(const char *policy, char *name,
    struct compliance_map_ctx **ctx_out, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	log(TRACE, "%s %s", __func__, name);

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_compliance_map, policy, name, &error);
	if (!error) {
		(*ctx_out) = calloc(1, sizeof(struct compliance_map_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->map_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);
}

bool
compliance_map_exists(const char *policy, char *name,
    struct isi_error **error_out)
{
	struct compliance_map_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	log(TRACE, "%s", __func__);

	open_compliance_map(policy, name, &ctx, &error);
	if (!error) {
		exists = true;
		close_compliance_map(ctx);
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
close_compliance_map(struct compliance_map_ctx *ctx)
{
	log(TRACE, "%s", __func__);

	if (ctx == NULL)
		return;

	if (ctx->map_fd > 0)
		close(ctx->map_fd);

	free(ctx);
}

void
remove_compliance_map(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s %s", __func__, name);

	siq_btree_remove(siq_btree_compliance_map, policy, name, missing_ok,
	    &error);

	isi_error_handle(error, error_out);
}

void
get_compliance_map_name(char *buf, size_t buf_len, const char *policy, bool src)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	snprintf(buf, buf_len, "%s_%s_%s", policy, "comp_map",
	    src ? "src" : "dst");
}

bool
set_compliance_mapping(uint64_t old_lin, uint64_t new_lin,
    struct compliance_map_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	bool exists;
	bool success = false;
	btree_key_t key;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	exists = compliance_mapping_exists(old_lin, ctx, &error);
	if (error)
		goto out;

	if (exists)
		res = ifs_sbt_mod_entry(ctx->map_fd, lin_to_key(old_lin, &key),
		    &new_lin, sizeof(new_lin), NULL);
	else
		res = ifs_sbt_add_entry(ctx->map_fd, lin_to_key(old_lin, &key),
		    &new_lin, sizeof(new_lin), NULL);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting compliance mapping %llx->%llx", old_lin,
		    new_lin);
		log(ERROR, "Error setting compliance mapping lin %llx->%llx: "
		    "%s", old_lin, new_lin, strerror(tmp_errno));
	} else if (!res) {
		success = true;
		log(DEBUG, "Compliance mapping set %llx->%llx", old_lin,
		    new_lin);
	}

 out:
	isi_error_handle(error, error_out);
	return success;
}

bool
get_compliance_mapping(uint64_t old_lin, uint64_t *new_lin,
    struct compliance_map_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	bool found;
	int tmp_errno;
	btree_key_t key;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->map_fd, lin_to_key(old_lin, &key),
	    new_lin, sizeof(*new_lin), NULL, &size);

	if (res == -1) {
		*new_lin = 0;
		found = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading compliance map lin %llx", old_lin);
			log(ERROR, "Error reading compliance map lin %llx: %s",
			    old_lin, strerror(tmp_errno));
		}
	} else {
		found = true;
		log(DEBUG, "Got compliance mapping for lin %llx", old_lin);
	}

	isi_error_handle(error, error_out);
	return found;
}

bool
remove_compliance_mapping(uint64_t old_lin, struct compliance_map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	bool found;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_remove_entry(ctx->map_fd, lin_to_key(old_lin, &key));

	if (res == -1) {
		found = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing compliance mapping lin %llx",
			    old_lin);
			log(ERROR, "Error removing compliance mapping lin "
			    "%llx: %s", old_lin, strerror(tmp_errno));
		}
	} else {
		found = true;
		log(DEBUG, "Removed compliance mapping for lin %llx", old_lin);
	}

	isi_error_handle(error, error_out);
	return found;
}

bool
compliance_mapping_exists(uint64_t old_lin, struct compliance_map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	btree_key_t key;
	bool exists = false;
	struct isi_error *error = NULL;
	int tmp_errno;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->map_fd, lin_to_key(old_lin, &key),
	    NULL, 0, NULL, NULL);
	if (res == 0)
		exists = true;
	else {
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading compliance map lin %llx", old_lin);
			log(ERROR, "Error reading compliance map lin %llx: %s",
			    old_lin, strerror(tmp_errno));
		}
	}

	log(DEBUG, "Compliance mapping for lin %llx %s", old_lin,
	    (exists) ? "exists" : "does not exist");

	isi_error_handle(error, error_out);
	return exists;
}

bool
compliance_map_empty(struct compliance_map_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct compliance_map_iter *iter;
	bool found = true;
	uint64_t old_lin, new_lin;

	log(TRACE, "%s", __func__);

	iter = new_compliance_map_iter(ctx);
	found = compliance_map_iter_next(iter, &old_lin, &new_lin, &error);

	isi_error_handle(error, error_out);
	close_compliance_map_iter(iter);
	return !found;
}

bool
get_compliance_mapkey_at_loc(unsigned num, unsigned den,
    struct compliance_map_ctx *ctx, uint64_t *lin, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_pct(ctx->map_fd, num, den, &key, NULL, 0, NULL,
	    &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading compliance map at loc %u/%u", num,
			    den);
			log(ERROR, "Error reading compliance map at loc "
			    "%u/%u: %s", num, den, strerror(tmp_errno));
		}
		goto out;
	}

	exists = true;
	*lin = key.keys[0];
	log(DEBUG, "got compliance map entry for lin %llx", *lin);

 out:
	isi_error_handle(error, error_out);
	return exists;
}

ifs_lin_t
get_compliance_map_max_lin(struct compliance_map_ctx *ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct compliance_map_iter *iter = NULL;
	ifs_lin_t max_lin = -1, old_lin = -1, new_lin = -1;
	bool found = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	/* Get the estimated largest lin in the btree */
	found = get_compliance_mapkey_at_loc(100, 100, ctx, &old_lin, &error);
	if (error)
		goto out;
	if (!found) {
		/* Empty map. */
		max_lin = INVALID_LIN;
		goto out;
	}

	/* Iterate to the last lin in the btree */
	iter = new_compliance_map_range_iter(ctx, old_lin);
	old_lin = INVALID_LIN;
	while(found) {
		found = compliance_map_iter_next(iter, &old_lin, &new_lin,
		    &error);
		if (error)
			goto out;
	}

	max_lin = old_lin;

 out:
	close_compliance_map_iter(iter);
	isi_error_handle(error, error_out);
	return max_lin;
}

ifs_lin_t
get_compliance_map_min_lin(struct compliance_map_ctx *ctx,
    struct isi_error **error_out)
{
	bool found = false;
	ifs_lin_t old_lin = INVALID_LIN;
	ifs_lin_t new_lin;
	struct compliance_map_iter *iter = NULL;
	struct isi_error *error = NULL;

	ASSERT(ctx);

	iter = new_compliance_map_iter(ctx);
	found = compliance_map_iter_next(iter, &old_lin, &new_lin, &error);
	if (error)
		goto out;

	ASSERT_IMPLIES(found, old_lin != INVALID_LIN);

out:
	close_compliance_map_iter(iter);
	isi_error_handle(error, error_out);
	return old_lin;
}

/*  ___ _                 _
 * |_ _| |_ ___ _ __ __ _| |_ ___  _ __
 *  | || __/ _ \ '__/ _` | __/ _ \| '__|
 *  | || ||  __/ | | (_| | || (_) | |
 * |___|\__\___|_|  \__,_|\__\___/|_|
 */

struct compliance_map_iter *
new_compliance_map_iter(struct compliance_map_ctx *ctx)
{
	struct compliance_map_iter *iter;

	log(TRACE, "%s", __func__);

	iter = calloc(1, sizeof(struct compliance_map_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	iter->key.keys[0] = 0;
	iter->key.keys[1] = 0;

	return iter;
}

struct compliance_map_iter *
new_compliance_map_range_iter(struct compliance_map_ctx *ctx, uint64_t lin)
{
	struct compliance_map_iter *iter;

	log(TRACE, "%s", __func__);

	iter = calloc(1, sizeof(struct compliance_map_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	iter->key.keys[0] = lin;
	iter->key.keys[1] = 0;

	return iter;
}

bool
compliance_map_iter_next(struct compliance_map_iter *iter, uint64_t *old_lin,
    uint64_t *new_lin, struct isi_error **error_out)
{
	int res = 0;
	char buf[256];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	errno = 0;
	struct isi_error *error = NULL;
	btree_key_t next_key;

	log(TRACE, "%s", __func__);

	/* last item has already been read if key was set to -1 on previous
	 * call */
	if (iter->key.keys[0] == -1)
		goto out;

	res = ifs_sbt_get_entries(iter->ctx->map_fd, &iter->key, &next_key,
	    256, buf, 1, &num_out);
	iter->key = next_key;
	/* num_out of 0 indicates end of non-empty list. EFAULT indicates empty
	 * list */
	if (num_out == 0 || (res == -1 && errno == EFAULT)) {
		res = 0;
		goto out;
	}
	if (res == -1)
		goto out;

	got_one = true;
	ent = (struct sbt_entry *)buf;
	*old_lin = ent->key.keys[0];
	ASSERT(ent->size_out == sizeof(uint64_t));
	memcpy(new_lin, buf + sizeof(struct sbt_entry), sizeof(uint64_t));

 out:
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error getting next compliance mapping");
		log(ERROR, "Error getting next compliance mapping: %s",
		    strerror(tmp_errno));
	}

	isi_error_handle(error, error_out);
	return got_one;
}

void
close_compliance_map_iter(struct compliance_map_iter *iter)
{
	log(TRACE, "%s", __func__);

	if (iter)
		free(iter);
}

bool
get_compliance_map_lin_in_range(struct compliance_map_ctx *ctx,
    ifs_lin_t min_lin, ifs_lin_t max_lin, struct isi_error **error_out)
{
	bool found = false;
	bool to_return = false;
	ifs_lin_t cur_lin = -1;
	ifs_lin_t new_lin;
	struct compliance_map_iter *iter = NULL;
	struct isi_error *error = NULL;

	iter = new_compliance_map_range_iter(ctx, min_lin);
	found = compliance_map_iter_next(iter, &cur_lin, &new_lin, &error);
	if (error)
		goto out;

	to_return = (found && cur_lin < max_lin);

out:
	close_compliance_map_iter(iter);
	isi_error_handle(error, error_out);
	return to_return;
}

static void
sync_compliance_map_entry(struct compliance_map_ctx *ctx, uint64_t old_lin,
    uint64_t new_lin, enum compliance_map_oper oper,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (oper == CMAP_SET) {
		set_compliance_mapping(old_lin, new_lin, ctx, &error);
		if (error)
			goto out;
	} else if (oper == CMAP_REM) {
		remove_compliance_mapping(old_lin, ctx, &error);
		if (error)
			goto out;
	}
 out:
	isi_error_handle(error, error_out);
}

static void
update_compliance_map_entries(struct sbt_bulk_entry *cmap_entries,
    int num_entries, struct compliance_map_ctx *ctx, int log_index,
    struct compliance_map_log *logp, struct isi_error **error_out)
{
	int res;
	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	/*
	 * First lets try to add all entries. In case of
	 * bulk operation failure, we try to update entries
	 * one transaction at a time.
	 */
	res = ifs_sbt_bulk_op(num_entries, cmap_entries);
	if (res == -1 && errno != EEXIST  && errno != ERANGE  &&
	    errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d compliance map entries from log "
		    "to disk  with begining lin %llx", num_entries,
		    cmap_entries[0].key.keys[0]);
		goto out;
	}

	if (res == 0)
		goto out;

	i = log_index - num_entries;
	/* Bulk transaction failed, lets try to do update one at a time */
	while (j < num_entries) {
		sync_compliance_map_entry(ctx, logp->entries[i].old_lin,
		    logp->entries[i].new_lin, logp->entries[i].oper,
		    &error);
		if (error)
			goto out;

		i++;
		j++;
	}

 out:
	isi_error_handle(error, error_out);
}

static int
compliance_map_compr(const void *elem1, const void *elem2)
{
	struct compliance_map_log_entry *entry1 =
	    (struct compliance_map_log_entry *)elem1;
	struct compliance_map_log_entry *entry2 =
	    (struct compliance_map_log_entry *)elem2;

	if (entry1->old_lin < entry2->old_lin) {
		return -1;
	} else if (entry1->old_lin == entry2->old_lin) {
		return 0;
	} else {
		return 1;
	}
}

/*
 * This function does flush of all entries from in-memory
 * log to on-disk linmap sbt.
 */
void
flush_compliance_map_log(struct compliance_map_log *logp,
    struct compliance_map_ctx *ctx, struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry cmap_entries[SBT_BULK_MAX_OPS];
	struct lin_set lset;
	struct btree_flags flags = {};

	log(TRACE, "%s", __func__);

	/* First lets sort the the rep entries */
	qsort(logp->entries, logp->index,
	    sizeof(struct compliance_map_log_entry), compliance_map_compr);

	bzero(cmap_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	lin_set_init(&lset);

	while (i < logp->index) {

		if (lin_set_contains(&lset, logp->entries[i].old_lin) ||
		    (j == SBT_BULK_MAX_OPS)) {
			ASSERT (i != 0);
			ASSERT (j != 0);
			update_compliance_map_entries(cmap_entries, j, ctx, i,
			    logp, &error);
			if (error)
				goto out;

			j = 0;
			lin_set_truncate(&lset);
		}

		cmap_entries[j].fd = ctx->map_fd;
		lin_to_key(logp->entries[i].old_lin, &(cmap_entries[j].key));
		cmap_entries[j].cm = BT_CM_NONE;
		if (logp->entries[i].oper == CMAP_SET) {
			cmap_entries[j].op_type = SBT_SYS_OP_ADD;
			cmap_entries[j].entry_buf = &(logp->entries[i].new_lin);
			cmap_entries[j].entry_size = sizeof(uint64_t);
			cmap_entries[j].entry_flags = flags;
		} else if (logp->entries[i].oper == CMAP_REM) {
			cmap_entries[j].op_type = SBT_SYS_OP_DEL;
		} else {
			ASSERT(0);
		}

		lin_set_add(&lset, logp->entries[i].old_lin);
		j++;
		i++;
	}

	if (j) {
		update_compliance_map_entries(cmap_entries, j, ctx, i, logp,
		    &error);
		if (error)
			goto out;
	}

	logp->index = 0;
 out:
	lin_set_clean(&lset);
	isi_error_handle(error, error_out);
}

/*
 * Add compliance map entry to in-memory log. If the log is full, then we
 * would flush the log to disk before adding new entry.
 */
void
compliance_map_log_add_entry(struct compliance_map_ctx *ctx,
    struct compliance_map_log *logp, ifs_lin_t old_lin, ifs_lin_t new_lin,
    enum compliance_map_oper oper, int source_fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);
	ASSERT(logp != NULL);
	ASSERT(old_lin != 0);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		/*
		 * Before flush , make sure we check connection
		 * with the source worker. Otherwise it could
		 * lead to extremely odd race condtions.
		 */
		if (source_fd) {
			check_connection_with_peer(source_fd, true,
			    1, &error);
			if (error)
				goto out;
		}

		flush_compliance_map_log(logp, ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}

	logp->entries[logp->index].old_lin = old_lin;
	logp->entries[logp->index].new_lin = new_lin;
	logp->entries[logp->index].oper = oper;
	ASSERT(oper == CMAP_REM || new_lin > 0);

	logp->index++;

 out:
	isi_error_handle(error, error_out);
}
