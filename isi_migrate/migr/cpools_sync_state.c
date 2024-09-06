#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_malloc_ostream.h>
#include <isi_util/syscalls.h>
#include <ifs/bam/bam_pctl.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/btree/btree.h>
#include <ifs/sbt/sbt.h>
#include "sbtree.h"
#include "isirep.h"
#include "work_item.h"
#include "cpools_sync_state.h"

/*
 * HELPER FNs
 */

static btree_key_t *
lin_to_key(uint64_t lin, btree_key_t *key)
{
	ASSERT(key);
	btree_key_set(key, lin, 0);
	return key;
}

static void
pack_entry(char *buf, size_t *len, struct cpss_entry *cpse)
{
	struct cpss_entry_v1_ondisk v1od = {};
	uint16_t ver = CPSE_VER_MAX;

	ASSERT(buf && len && cpse);

	UFAIL_POINT_CODE(cpse_write_ver, ver = RETURN_VALUE;);

	switch (ver) {
	case 1:
		v1od.ver = 1;
		v1od.sync_type = (uint8_t)(cpse->sync_type & 0xFF);
		memcpy(buf, &v1od, sizeof(v1od));
		*len = sizeof(v1od);
		break;
	default:
		ASSERT(false, "Invalid cpools sync state ondisk version.");
	}
}

static void
unpack_entry(char *buf, size_t len, struct cpss_entry *cpse)
{
	uint16_t ver = *(uint16_t *)buf;
	struct cpss_entry_v1_ondisk v1od = {};

	ASSERT(buf && cpse);

	switch (ver) {
	case 1:
		memcpy(&v1od, buf, sizeof(v1od));
		cpse->sync_type = v1od.sync_type;
		cpse->del_entry = 0;
		break;
	}
}

/*
 * CLOUDPOOLS SYNC STATE SBT
 */

void
prune_old_cpss(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_prune_old(siq_btree_cpools_sync_state, policy, name_root,
	    snap_kept, num_kept, &error);

	isi_error_handle(error, error_out);
}

void
create_cpss(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_create(siq_btree_cpools_sync_state, policy, name, exist_ok,
	    &error);

	isi_error_handle(error, error_out);
}

void
open_cpss(const char *policy, char *name, struct cpss_ctx **ctx_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_cpools_sync_state, policy, name, &error);
	if (!error) {
		(*ctx_out) = calloc(1, sizeof(struct cpss_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->cpss_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);
}

/**
 * Return whether a file with the appropriate name exists.
 */
bool
cpss_exists(const char *policy, char *name, struct isi_error **error_out)
{
	struct cpss_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	open_cpss(policy, name, &ctx, &error);
	if (!error) {
		exists = true;
		close_cpss(ctx);
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
close_cpss(struct cpss_ctx *ctx)
{
	log(TRACE, "close_cpss");

	if (ctx == NULL)
		return;

	if (ctx->cpss_fd > 0)
		close(ctx->cpss_fd);

	free(ctx);
}

void
remove_cpss(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	siq_btree_remove(siq_btree_cpools_sync_state, policy, name, missing_ok,
	    &error);
	isi_error_handle(error, error_out);
}

void
set_cpss_entry(u_int64_t lin, struct cpss_entry *cpse, struct cpss_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry bentries[1];
	int num_entries = 1;
	char sbt_buf[SYNCSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = SYNCSTATE_BUF_SIZE;
	struct btree_flags sbt_flags = {};

	log(TRACE, "set_cpss_entry");

	/*
	 * This is to avoid any issues with pointer field
	 * which is defined as 64 bit but probably is used
	 * as 32 bit in userspace
	 */
	memset(bentries, 0, sizeof(struct sbt_bulk_entry));

	exists = cpss_entry_exists(lin, ctx, &error);
	if (error)
		goto out;

	/* Cloudpools sync state entry */
	bentries[0].fd = ctx->cpss_fd;
	if (exists)
		bentries[0].op_type = SBT_SYS_OP_MOD;
	else
		bentries[0].op_type = SBT_SYS_OP_ADD;
	lin_to_key(lin, &bentries[0].key);

	pack_entry(sbt_buf, &sbt_buf_size, cpse);
	bentries[0].entry_buf = sbt_buf;
	bentries[0].entry_size = sbt_buf_size;
	bentries[0].entry_flags = sbt_flags;
	bentries[0].cm = BT_CM_NONE;
	bentries[0].old_entry_buf = NULL;
	bentries[0].old_entry_size = 0;

	//XXXDPL Do we need to bulk_op anymore?
	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting cloudpools sync state lin %llx", lin);
		log(ERROR, "Error setting cloudpools sync state lin %llx: %s",
		    lin, strerror(tmp_errno));
	} else {
		log(DEBUG, "cloudpools sync state entry set for lin %llx",
		    lin);
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * Helper routine to create bulk entry based on old and new rep entries passed
 */
void
add_cpss_bulk_entry(struct cpss_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, bool is_new, struct cpss_entry *cpse,
    char *sbt_buf, struct btree_flags *sbt_flags,
    struct cpss_entry *old_cpse, char *sbt_old_buf,
    struct btree_flags *sbt_old_flags,
    const size_t sbt_buf_size)
{

	size_t sbt_new_buf_size = sbt_buf_size;
	size_t sbt_old_buf_size = sbt_buf_size;


	bentry->fd = ctx->cpss_fd;

	if (!is_new) {
		bentry->op_type = SBT_SYS_OP_MOD;
		bentry->cm = BT_CM_BOTH;
		pack_entry(sbt_old_buf, &sbt_old_buf_size, old_cpse);
		bentry->old_entry_buf = sbt_old_buf;
		bentry->old_entry_size = sbt_old_buf_size;
		bentry->old_entry_flags = *sbt_old_flags;
	} else {
		bentry->op_type = SBT_SYS_OP_ADD;
		bentry->cm = BT_CM_NONE;
		bentry->old_entry_buf = NULL;
		bentry->old_entry_size = 0;
	}

	lin_to_key(lin, &(bentry->key));

	pack_entry(sbt_buf, &sbt_new_buf_size, cpse);
	bentry->entry_buf = sbt_buf;
	bentry->entry_size = sbt_new_buf_size;
	bentry->entry_flags = *sbt_flags;
}

/**
 * Return true if modded, false if not modded due to the key
 * not existing or the conditional mod failing.
 */
bool
set_cpss_entry_cond(u_int64_t lin, struct cpss_entry *cpse,
    struct cpss_ctx *ctx, bool is_new, struct cpss_entry *old_cpse,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	bool success = false;
	struct sbt_bulk_entry bentry = {};
	char sbt_buf[SYNCSTATE_BUF_SIZE] = "";
	char sbt_old_buf[SYNCSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = SYNCSTATE_BUF_SIZE;
	struct btree_flags sbt_flags = {};
	struct btree_flags sbt_old_flags = {};

	log(TRACE, "set_entry_cond");

	add_cpss_bulk_entry(ctx, lin, &bentry, is_new, cpse, sbt_buf,
	    &sbt_flags, old_cpse, sbt_old_buf, &sbt_old_flags, sbt_buf_size);

	/* Perform the transaction */
	res = ifs_sbt_bulk_op(1, &bentry);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting cpools syncstate lin %llx", lin);
		log(ERROR, "Error %d setting cpools syncstate lin %llx",
		    tmp_errno, lin);
	} else if (!res) {
		success = true;
		log(DEBUG, "cloudpools syncstate entry set for lin %llx", lin);
	}

	isi_error_handle(error, error_out);
	return success;
}

bool
get_cpss_entry(u_int64_t lin, struct cpss_entry *cpse, struct cpss_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists = false;
	char sbt_buf[SYNCSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = 0;
	struct btree_flags bt_flags = {};

	log(TRACE, "get_entry");

	lin_to_key(lin, &key);
	res = ifs_sbt_get_entry_at(ctx->cpss_fd, lin_to_key(lin, &key),
	    sbt_buf, SYNCSTATE_BUF_SIZE, &bt_flags, &sbt_buf_size);
	if (res == -1) {
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading cpools syncstate lin %llx",
			    lin);
			log(ERROR, 
			    "Error reading cpools syncstate lin %llx:"
			    " %s", lin, strerror(tmp_errno));
		}
		goto out;
	}

	exists = true;
	unpack_entry(sbt_buf, sbt_buf_size, cpse);

	log(DEBUG, "got cpools syncstate entry for lin %llx", lin);

out:
	isi_error_handle(error, error_out);
	return exists;
}

bool
get_cpsskey_at_loc(unsigned num, unsigned den, struct cpss_ctx *ctx,
    uint64_t *lin, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "get_cpsskey_at_loc");

	res = ifs_sbt_get_entry_pct(ctx->cpss_fd, num, den,
	    &key, NULL, 0, NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading cpools syncstate at loc %u/%u", num, den);
			log(ERROR, "Error reading cpools syncstate at loc %u/%u: %s",
			    num, den, strerror(tmp_errno));
		}
	} else {
		exists = true;
		*lin = key.keys[0];
		log(DEBUG, "got cpools syncstate entry for lin %llx", *lin);
	}
	isi_error_handle(error, error_out);
	return exists;
}

ifs_lin_t
get_cpss_max_lin(struct cpss_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct cpss_iter *cpss_iter = NULL;
	struct cpss_entry cpse;
	ifs_lin_t max_lin = -1, cur_lin = -1;
	bool found = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	/* Get the estimated largest lin in the btree */
	found = get_cpsskey_at_loc(100, 100, ctx, &cur_lin, &error);
	if (error)
		goto out;
	if (!found) {
		error = isi_system_error_new(errno,
		    "Error reading cpools syncstate at loc 100/100:"
		    "Entry not found");
		goto out;
	}

	/* Iterate to the last lin in the btree */
	cpss_iter = new_cpss_range_iter(ctx, cur_lin);
	while (found) {
		found = cpss_iter_next(cpss_iter, &cur_lin, &cpse, &error);
		if (error)
			goto out;
	}

	max_lin = cur_lin;

out:
	close_cpss_iter(cpss_iter);
	isi_error_handle(error, error_out);
	return max_lin;
}

bool
remove_cpss_entry(u_int64_t lin, struct cpss_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	struct isi_error *error = NULL;
	bool exists;

	log(TRACE, "remove_cpss_entry");

	res = ifs_sbt_remove_entry(ctx->cpss_fd, lin_to_key(lin, &key));
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing cpools syncstate lin %llx", lin);
			log(ERROR,
			    "Error removing cpools syncstate lin %llx: %s",
			    lin, strerror(tmp_errno));
			goto out;
		}
	} else {
		exists = true;
		log(DEBUG, "removed cpools syncstate entry for lin %llx", lin);
	}

out:
	isi_error_handle(error, error_out);
	return exists;
}

bool
cpss_entry_exists(u_int64_t lin, struct cpss_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	btree_key_t key;
	bool exists;
	struct isi_error *error = NULL;
	int tmp_errno;

	res = ifs_sbt_get_entry_at(ctx->cpss_fd, lin_to_key(lin, &key), NULL,
	    0, NULL, NULL);
	if (res == 0) {
		exists = true;
	} else {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading cpools syncstate lin %llx", lin);
			log(ERROR,
			    "Error reading cpools syncstate lin %llx: %s", lin,
			    strerror(tmp_errno));
			goto out;
		}
	}

	log(DEBUG, "entry for lin %llx %s", lin,
	    (exists) ? "exists" : "does not exist");

out:
	isi_error_handle(error, error_out);
	return exists;
}

/*
 * ITERATOR
 */

struct cpss_iter *
new_cpss_iter(struct cpss_ctx *ctx)
{
	struct cpss_iter *iter;

	log(TRACE, "new_cpss_iter");

	iter = calloc(1, sizeof(struct cpss_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	lin_to_key(0, &iter->key);

	return iter;
}

struct cpss_iter *
new_cpss_range_iter(struct cpss_ctx *ctx, uint64_t lin)
{
	struct cpss_iter *iter;

	log(TRACE, "new_cpss_range_iter");

	iter = calloc(1, sizeof(struct cpss_iter));
	ASSERT(iter);
	iter->ctx = ctx;
	lin_to_key(lin, &iter->key);

	return iter;
}

bool
cpss_iter_next(struct cpss_iter *iter, u_int64_t *lin, struct cpss_entry *cpse,
    struct isi_error **error_out)
{
	int res = 0;
	char buf[SYNCSTATE_BUF_SIZE + sizeof(struct sbt_entry)];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	btree_key_t next_key;
	errno = 0;
	struct isi_error *error = NULL;


	log(TRACE, "cpss_iter_next");

	/* last item has already been read if key was set to -1 on previous
	 * call */
	if (iter->key.keys[0] == -1)
		goto out;

	res = ifs_sbt_get_entries(iter->ctx->cpss_fd, &iter->key,
	    &next_key, SYNCSTATE_BUF_SIZE + sizeof(struct sbt_entry),
	    buf, 1, &num_out);
	iter->key = next_key;
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error getting next mapping");
		log(ERROR, "Error getting next mapping: %s",
		    strerror(tmp_errno));
		goto out;
	}

	if (num_out == 0) {
		goto out;
	}

	ent = (struct sbt_entry *)buf;

	got_one = true;
	*lin = btree_key_high(ent->key);
	unpack_entry(ent->buf, ent->size_out, cpse);

out:
	isi_error_handle(error, error_out);
	return got_one;
}

void
close_cpss_iter(struct cpss_iter *iter)
{
	log(TRACE, "close_cpss_iter");

	if (iter)
		free(iter);
}

void
get_base_cpss_name(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_cpools_state_base_restore", policy);
	} else {
		sprintf(buf, "%s_cpools_state_base", policy);
	}
}

void
get_mirrored_cpss_name(char *buf, const char *policy)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	sprintf(buf, "%s_cpools_state_base_mirrored", policy);
}

void
get_repeated_cpss_name(char *buf, const char *policy, uint64_t snapid,
    bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	ASSERT(snapid > 0);
	if (restore) {
		sprintf(buf, "%s_snap_restore_%llu", policy, snapid);
	} else {
		sprintf(buf, "%s_snap_%llu", policy, snapid);
	}
}

void
get_repeated_cpss_pattern(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_snap_restore_", policy);
	} else {
		sprintf(buf, "%s_snap_", policy);
	}
}

/*
 *
 * Find the old cpss entry and update it with new operations done using
 * the oldp and newp cpss entries.
 */
bool
sync_cpss_entry(struct cpss_ctx *ctx, ifs_lin_t lin, struct cpss_entry *newp,
    struct isi_error **error_out)
{

	int res;
	bool exists;
	bool success = false;
	struct isi_error *error = NULL;
	struct cpss_entry old_entry = {};
	struct cpss_entry new_entry = {};
	struct sbt_bulk_entry bentry = {};
	char sbt_buf[SYNCSTATE_BUF_SIZE] = "";
	char sbt_old_buf[SYNCSTATE_BUF_SIZE] = "";
	struct btree_flags sbt_flags = {};
	struct btree_flags sbt_old_flags = {};
	size_t sbt_buf_size = SYNCSTATE_BUF_SIZE;
	size_t sbt_old_buf_size = SYNCSTATE_BUF_SIZE;
	int num_entries = 1;

	ASSERT(newp != NULL);

	bzero(&bentry, sizeof(struct sbt_bulk_entry));

	/* Find the old entry (if it exists) */
	exists = get_cpss_entry(lin, &old_entry, ctx, &error);
	if (error)
		goto out;

	bentry.fd = ctx->cpss_fd;
	if (exists) {
		bentry.op_type = SBT_SYS_OP_MOD;
		bentry.cm = BT_CM_BOTH;
		pack_entry(sbt_old_buf, &sbt_old_buf_size, &old_entry);
		bentry.old_entry_buf = sbt_old_buf;
		bentry.old_entry_size = sbt_old_buf_size;
		bentry.old_entry_flags = sbt_old_flags;
		new_entry = *newp;
	} else {
		bentry.op_type = SBT_SYS_OP_ADD;
		bentry.cm = BT_CM_NONE;
		bentry.old_entry_buf = NULL;
		bentry.old_entry_size = 0;
		new_entry = *newp;
	}

	lin_to_key(lin, &bentry.key);

	pack_entry(sbt_buf, &sbt_buf_size, &new_entry);
	bentry.entry_buf = sbt_buf;
	bentry.entry_size = sbt_buf_size;
	bentry.entry_flags = sbt_flags;

	/* Perform the transaction */
	res = ifs_sbt_bulk_op(num_entries, &bentry);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		error = isi_system_error_new(errno,
		    "Error setting cpstate lin %llx", lin);
		goto out;
	} else if (!res) {
		success = true;
		log(DEBUG, "cpstate entry set for lin %llx", lin);
	}

out:
	isi_error_handle(error, error_out);
	return success;
}


static void
add_cpss_entries(struct sbt_bulk_entry *cpss_entries, int num_entries,
    struct cpss_ctx *cpse, int log_index, struct cpss_log *logp,
    struct isi_error **error_out)
{
	int res;
	int i = 0;
	int j = 0;
	bool success = false;
	struct isi_error *error = NULL;

	log(TRACE, "add_cpss_entries");	
	/*
	 * First lets try to add all entries.
	 * It could fail if there are concurrent updates by
	 * another worker or stale old rep entry. So in case of
	 * bulk operation failure, we try to update entries
	 * one transaction at a time.
	 */

	res = ifs_sbt_bulk_op(num_entries, cpss_entries);
	if (res == -1 && errno != EEXIST && errno != ERANGE) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d cpools syncstate entries from "
		    "log to disk with begining lin %llx", num_entries,
		    cpss_entries[0].key.keys[0]);
		goto out;
	}
	
	if (res == 0)
		goto out;

	i = log_index - num_entries;
	/* Bulk transaction failed, lets try to do update one at a time */
	while (j < num_entries) {
		success = false;
		while (!success) {
			success = sync_cpss_entry(cpse, logp->entries[i].lin,
			    &(logp->entries[i].new_entry), &error);
			if (error)
				goto out;
		}

		i++;
		j++;
	}

out:
	isi_error_handle(error, error_out);
}

static int
cpss_compr(const void *elem1, const void *elem2)
{
	struct cpss_log_entry *entry1 = (struct cpss_log_entry *)elem1;
	struct cpss_log_entry *entry2 = (struct cpss_log_entry *)elem2;

	if (entry1->lin < entry2->lin) {
		return -1;
	} else if (entry1->lin == entry2->lin) {
		return 0;
	} else {
		return 1;
	}
}
/*
 * This function does flush of all entries from in-memory
 * log to on-disk repstate sbt.
 */
void
flush_cpss_log(struct cpss_log *logp, struct cpss_ctx *ctx,
    struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry cpss_entries[SBT_BULK_MAX_OPS];
	char sbt_buf[SBT_BULK_MAX_OPS][SYNCSTATE_BUF_SIZE];
	struct btree_flags sbt_flags[SBT_BULK_MAX_OPS];
	char sbt_old_buf[SBT_BULK_MAX_OPS][SYNCSTATE_BUF_SIZE];
	struct btree_flags sbt_old_flags[SBT_BULK_MAX_OPS];
	struct lin_set lset;

	log(TRACE, "flush_cpss_log");

	/* First lets sort the the cpss entries */
	qsort(logp->entries, logp->index, sizeof(struct cpss_log_entry),
	    cpss_compr);

	bzero(cpss_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	bzero(sbt_buf, SBT_BULK_MAX_OPS * SYNCSTATE_BUF_SIZE);
	bzero(sbt_old_buf, SBT_BULK_MAX_OPS * SYNCSTATE_BUF_SIZE);
	bzero(sbt_flags, SBT_BULK_MAX_OPS * sizeof(struct btree_flags));
	bzero(sbt_old_flags, SBT_BULK_MAX_OPS * sizeof(struct btree_flags));
	lin_set_init(&lset);

	while (i < logp->index) {
		if (lin_set_contains(&lset, logp->entries[i].lin) ||
		    (j == SBT_BULK_MAX_OPS)) {
			ASSERT (i != 0);
			ASSERT (j != 0);
			add_cpss_entries(cpss_entries, j, ctx, i, logp,
			    &error);
			if (error)
				goto out;

			bzero(cpss_entries,
			    j * sizeof(struct sbt_bulk_entry));
			bzero(sbt_buf,
			    j * SYNCSTATE_BUF_SIZE);
			bzero(sbt_old_buf,
			    j * SYNCSTATE_BUF_SIZE);
			bzero(sbt_flags,
			    j * sizeof(struct btree_flags));
			bzero(sbt_old_flags,
			    j * sizeof(struct btree_flags));

			j = 0;
			lin_set_truncate(&lset);
		}

		add_cpss_bulk_entry(ctx, logp->entries[i].lin,
		    &cpss_entries[j], logp->entries[i].is_new,
		    &(logp->entries[i].new_entry), sbt_buf[j],
		    &sbt_flags[j], &(logp->entries[i].old_entry),
		    sbt_old_buf[j], &sbt_old_flags[j],
		    sizeof(struct cpss_entry));

		lin_set_add(&lset, logp->entries[i].lin);
		j++;
		i++;
	}

	if (j) {
		add_cpss_entries(cpss_entries, j, ctx, i, logp, &error);
		if (error)
			goto out;
	}

	logp->index = 0;
out:
	lin_set_clean(&lset);
	isi_error_handle(error, error_out);
}

void
copy_cpss_entries(struct cpss_ctx *ctx, ifs_lin_t *lins,
    struct cpss_entry *entries, int num_entries, struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	int res;
	bool exists;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry bentries[SBT_BULK_MAX_OPS];
	char sbt_buf[SBT_BULK_MAX_OPS][SYNCSTATE_BUF_SIZE];
	struct btree_flags sbt_flags = {};
	size_t sbt_buf_size = SYNCSTATE_BUF_SIZE;

	log(TRACE, "copy_cpss_entries");

	ASSERT(num_entries <= SBT_BULK_MAX_OPS);
	ASSERT(num_entries > 0 && entries != NULL && lins != NULL);
	bzero(bentries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	bzero(sbt_buf, SBT_BULK_MAX_OPS * SYNCSTATE_BUF_SIZE);

	while (i < num_entries) {
		bentries[j].fd = ctx->cpss_fd;
		lin_to_key(lins[i], &(bentries[j].key));
		exists = cpss_entry_exists(lins[i], ctx, &error);
		if (error)
			goto out;

		if (entries[i].del_entry) {
			if (exists) {
				bentries[j].op_type = SBT_SYS_OP_DEL;
				j++;
			}
		} else if (entries[i].sync_type != BST_NONE) {
			bentries[j].op_type = exists ? SBT_SYS_OP_MOD :
			    SBT_SYS_OP_ADD;
			bentries[j].cm = BT_CM_NONE;
			sbt_buf_size = sizeof(struct cpss_entry);
			bzero(&sbt_flags, sizeof(struct btree_flags));
			pack_entry(sbt_buf[j], &sbt_buf_size, &entries[i]);
			ASSERT(sbt_buf_size <= sizeof(struct cpss_entry));
			bentries[j].entry_buf = sbt_buf[j];
			bentries[j].entry_size = sbt_buf_size;
			bentries[j].entry_flags = sbt_flags;
			j++;
		}

		i++;
	}
	if (j) {
		res = ifs_sbt_bulk_op(j, bentries);
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Unable to copy entries with key range %llx to %llx"
			    " to base cpools syncstate", lins[0],
			    lins[num_entries - 1]);
			goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}

