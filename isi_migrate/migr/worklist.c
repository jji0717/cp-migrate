#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ifs/ifs_syscalls.h>
#include <isi_sbtree/sbtree.h>

#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_btree.h"
#include "isi_migrate/migr/work_item.h"
#include "worklist.h"

#define LV_WIDTH 32
#define LV_MASK  0xffffffff

void
prune_old_worklist(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_prune_old(siq_btree_worklist, policy, name_root, snap_kept,
	    num_kept, &error);

	isi_error_handle(error, error_out);
}

void
create_worklist(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	
	siq_btree_create(siq_btree_worklist, policy, name, exist_ok, &error);

	isi_error_handle(error, error_out);
}

/*
 * Open the worklist btree file and use wid to refrence future entries 
 */
void
open_worklist(const char *policy, char *name, struct wl_ctx **ctx_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_worklist, policy, name, &error);
	if (!error) {
		*ctx_out = calloc(1, sizeof(struct wl_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->wl_fd = fd;
		fd = -1;
	}
	isi_error_handle(error, error_out);
}

void
close_worklist(struct wl_ctx *ctx)
{
	log(TRACE, "close_worklist");

	if (ctx == NULL) {
		goto out;
	}

	if (ctx->wl_fd > 0)
		close(ctx->wl_fd);

	free(ctx);

out:
	return;
}

void
remove_worklist(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_remove(siq_btree_worklist, policy, name, missing_ok, &error);

	isi_error_handle(error, error_out);
}

uint64_t
get_wi_lin_from_btree_key(btree_key_t *key)
{
	unsigned int to_return = 0;

	to_return = key->keys[0] >> LV_WIDTH;
	return to_return;
}

unsigned int
get_level_from_btree_key(btree_key_t *key)
{
	unsigned int to_return = 0;

	to_return = key->keys[0] & LV_MASK;
	return to_return;
}

ifs_lin_t
get_lin_from_btree_key(btree_key_t *key)
{
	return key->keys[1];
}

void
set_wl_key( btree_key_t *key, uint64_t wi_lin, unsigned int cur_level,
    ifs_lin_t lin)
{

	uint64_t higher;

	/*
	 * Worklist Key format (128 bit) is
	 *      32 bits        32 bits              64 bits
	 * 127..........96................63........................0
	 * workitem ID | cur tree level  |       Lin entry
	 */
	higher = wi_lin << LV_WIDTH;
	higher = higher | cur_level;
	key->keys[0] = higher;
	key->keys[1] = lin;
	return;
}

static int
wl_bulk_compr(const void *elem1, const void *elem2)
{
	struct sbt_bulk_entry *entry1 = (struct sbt_bulk_entry *)elem1;
	struct sbt_bulk_entry *entry2 = (struct sbt_bulk_entry *)elem2;
	uint64_t key1, key2;

	/*
	 * If higher order bits are different, then use it for comparision.
	 * Else use the lower order bits (lin numbers).
	 */
	if (entry1->key.keys[0] != entry2->key.keys[0]) {
		key1 = entry1->key.keys[0];
		key2 = entry2->key.keys[0];
	} else {
		key1 = entry1->key.keys[1];
		key2 = entry2->key.keys[1];
	}

	if (key1 < key2) {
		return -1;
	} else if (key1 == key2) {
		return 0;
	} else {
		return 1;
	}	
}

/*
 * Move X lins(==count) from current worklist to new worklist
 * indexed by tgt_wid.
 */
void
wl_move_entries(struct wl_ctx *ctx, btree_key_t *key, uint64_t count, 
    uint64_t src_wlid, uint64_t tgt_wlid, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	int res = 0;
	uint64_t i;
	int j = 0;
	struct sbt_bulk_entry blk_ent[SBT_BULK_MAX_OPS];
	uint64_t lin = 0;
	bool exists = false;
	struct wl_iter *iter = NULL;
	btree_key_t tgt_key;
	
	log(TRACE, "wl_move_entries");

	/* This would clear most of fields which we dont use */
	memset(blk_ent, 0, SBT_BULK_MAX_OPS * sizeof(struct sbt_bulk_entry));
	set_wl_key(&tgt_key, tgt_wlid, 0, 0);

	iter = new_wl_iter(ctx, key);
	i = count;
	/* Start from end to begin , reverse order */
	while(i > 0) {

		exists = wl_iter_next(NULL, iter, &lin, NULL, NULL, &error);
		if (error)
			goto out;

		if (!exists) {
			error = isi_system_error_new(errno,
			    "Failed to read worklist %llu entry", src_wlid);
			goto out;
		}
		
		blk_ent[j].fd = ctx->wl_fd;
		blk_ent[j].op_type = SBT_SYS_OP_DEL;
		blk_ent[j].key.keys[0] = key->keys[0];	
		blk_ent[j].key.keys[1] = lin;
		blk_ent[j].cm = BT_CM_NONE;

		j++;
		blk_ent[j].fd = ctx->wl_fd;
		blk_ent[j].op_type = SBT_SYS_OP_ADD;
		blk_ent[j].key.keys[0] = tgt_key.keys[0];	
		blk_ent[j].key.keys[1] = lin;
		blk_ent[j].cm = BT_CM_NONE;

		j++;

		if (j > SBT_BULK_MAX_OPS - 1) {
			/* Sort the bulk ops before passing it to kernel */
			qsort(blk_ent, j, sizeof(struct sbt_bulk_entry),
			    wl_bulk_compr);
			res = ifs_sbt_bulk_op(j, blk_ent);
			if (res == -1) {
				error = isi_system_error_new(errno,
				    "Error while moving %d lins from worklist "
				    "%llu to worklist %llu", j, src_wlid,
				    tgt_wlid);
				goto out;
			}
			j = 0;
		}

		i--;
	}

	if (j) {
		/* Sort the bulk ops before passing it to kernel */
		qsort(blk_ent, j, sizeof(struct sbt_bulk_entry),
		    wl_bulk_compr);
		res = ifs_sbt_bulk_op(j, blk_ent);
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Error while moving %d lins from worklist "
			    "%llu to worklist %llu", j, src_wlid,
			    tgt_wlid);
			goto out;
		}
	}


	log(DEBUG, "Moved %llu lins from worklist %llu to worklist %llu",
	    count, src_wlid, tgt_wlid);
out:
	if (iter)
		close_wl_iter(iter);

	isi_error_handle(error, error_out);

	return;
}

void
free_worklist(struct wl_ctx *ctx)
{
	ASSERT(ctx->wl_fd > 0);
	close(ctx->wl_fd);
	free(ctx);
}

static bool
match_higher(uint64_t key_higher, uint64_t iter_higher)
{
	uint64_t mask = LV_MASK;
	bool match = true;

	/* First compare worklist ID */
	if ((key_higher & ~mask) != 
	    (iter_higher & ~mask)) {
		match = false;
		goto out;
	}

	/* Compare level if required , -1(all f's) indicate invalid level*/
	if (((iter_higher & mask) != mask ) &&
	    ((key_higher & mask) != (iter_higher & mask))) {
		match = false;
		goto out;
	}
	
out:
	return match;

}

struct wl_iter *
new_wl_iter(struct wl_ctx *ctx, btree_key_t *key)
{
	struct wl_iter *iter;

	log(TRACE, "new_wl_iter");
	ASSERT( key != NULL);

	iter = calloc(1, sizeof(struct wl_iter));
	ASSERT(iter);
	
	iter->ctx = ctx;
	memcpy(&iter->key, key, sizeof(btree_key_t));

	iter->higher = key->keys[0];

	iter->has_last_entry = false;

	return iter;
}

void
close_wl_iter(struct wl_iter *iter)
{
	log(TRACE, "close_wl_iter");

	if (iter)
		free(iter);
}

bool
wl_iter_has_entries(struct wl_iter *iter)
{
	if (iter->key.keys[0] == -1 ||
	    !match_higher(iter->key.keys[0], iter->higher)) {
		return false;
	} else {
		return true;
	}
}

static void
read_uint64_from_buf(char *buf, int *offset, uint64_t *value)
{
	memcpy(value, &buf[*offset], sizeof(uint64_t));
	*offset += sizeof(uint64_t);
}

static void
write_uint64_to_buf(char *buf, int *offset, uint64_t value)
{
	memcpy(&buf[*offset], &value, sizeof(uint64_t));
	*offset += sizeof(uint64_t);
}

static void
read_wl_checkpoint(char *buf, struct wl_entry *entry)
{
	int offset = 0;

	ASSERT(buf && entry);

	//We expect to be able to populate wl_entry with 2 cookies
	read_uint64_from_buf(buf, &offset, &entry->dircookie1);
	read_uint64_from_buf(buf, &offset, &entry->dircookie2);
}

static char *
write_wl_checkpoint(uint64_t dircookie1, uint64_t dircookie2)
{
	int offset = 0;
	char *buf = NULL;

	buf = calloc(1, sizeof(struct wl_entry));
	ASSERT(buf);
	write_uint64_to_buf(buf, &offset, dircookie1);
	write_uint64_to_buf(buf, &offset, dircookie2);
	return buf;
}

int
raw_wl_checkpoint(int wl_fd, unsigned int wi_lin, unsigned int level,
    ifs_lin_t lin, uint64_t dircookie1, uint64_t dircookie2)
{
	btree_key_t new_key;
	char *buf = NULL;
	int ret = -1;

	//Write checkpoint to the currently referenced item
	set_wl_key(&new_key, wi_lin, level, lin);
	buf = write_wl_checkpoint(dircookie1, dircookie2);
	ret = ifs_sbt_cond_mod_entry(wl_fd, &new_key, buf,
	    sizeof(struct wl_entry), 0, BT_CM_NONE, NULL, 0, 0);

	if (buf)
		free(buf);
	return ret;
}

int
wl_checkpoint(struct wl_log *logp, struct wl_iter *iter,
    uint64_t dircookie1, uint64_t dircookie2, struct isi_error **error_out)
{
	unsigned int wi_lin = 0;
	unsigned int level = 0;
	ifs_lin_t lin = 0;
	struct isi_error *error = NULL;
	int ret = -1;

	//Separate out the components of the current key
	wi_lin = get_wi_lin_from_btree_key(&iter->current_key);
	level = get_level_from_btree_key(&iter->current_key);
	lin = get_lin_from_btree_key(&iter->current_key);

	//Flush bulk buffer
	flush_wl_log(logp, iter->ctx, wi_lin, level, &error);
	if (error) {
		goto out;
	}

	ret = raw_wl_checkpoint(iter->ctx->wl_fd, wi_lin, level, lin,
	    dircookie1, dircookie2);
	if (ret != 0) {
		//Errno should come from ifs_sbt_cond_mod_entry
		error = isi_system_error_new(errno,
		    "Error writing wl_checkpoint %x %d %llx",
		    wi_lin, level, lin);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
	return ret;
}

static bool
wl_iter_common_next(struct wl_log *logp, struct wl_iter *iter, uint64_t *lin,
    unsigned int *level, struct wl_entry **entry, struct isi_error **error_out,
    bool raw)
{
	int res = 0;
	char buf[256];
	struct sbt_entry *ent = NULL;
	struct wl_entry *new_entry = NULL;
	size_t num_out = 0;
	bool got_one = false;
	errno = 0;
	struct isi_error *error = NULL;
	btree_key_t next_key;

	log(TRACE, "wl_iter_next");
	ASSERT(lin != NULL);

	/*
	 * last item has already been read if key was set to -1
	 * on previous call
	 */
	if (iter->key.keys[0] == -1)
		goto out;
	else if (!raw && logp != NULL && iter->has_last_entry) {
		//Mark the current item as done
		wl_log_del_entry(logp, iter->ctx, iter->current_key, &error);
		if (error) {
			goto out;
		}
	}

	res = ifs_sbt_get_entries(iter->ctx->wl_fd, &iter->key,
	    &next_key, 256, buf, 1, &num_out);
	iter->key = next_key;
	if (res == -1) {
		error = isi_system_error_new(errno,
		    "Error getting next worklist key %llx:%llx",
		    iter->key.keys[0], iter->key.keys[1]);
		goto out;
	}

	if (num_out == 0) {
		goto out;
	}

	ent = (struct sbt_entry *)buf;
	/*
	 * The following indicates we have reached
	 * end of current level of worklist
	 */
	if (!raw && !match_higher(ent->key.keys[0], iter->higher)) {
		goto out;
	}

	got_one = true;
	iter->has_last_entry = true;
	iter->current_key = ent->key;
	*lin = ent->key.keys[1];
	if (level) {
		*level = get_level_from_btree_key(&ent->key);
	}
	//Read the checkpoint if it exists (and the caller wants it)
	if (ent->size_out > 0 && entry != NULL) {
		new_entry = calloc(1, sizeof(struct wl_entry));
		ASSERT(new_entry);
		read_wl_checkpoint(ent->buf, new_entry);
		*entry = new_entry;
	}

out:
	isi_error_handle(error, error_out);
	return got_one;
}

bool
wl_iter_raw_next(struct wl_log *logp, struct wl_iter *iter, uint64_t *lin,
    unsigned int *level, struct wl_entry **entry, struct isi_error **error_out)
{
	return wl_iter_common_next(logp, iter, lin, level, entry, error_out,
	    true);
}

bool
wl_iter_next(struct wl_log *logp, struct wl_iter *iter, uint64_t *lin,
    unsigned int *level, struct wl_entry **entry, struct isi_error **error_out)
{
	return wl_iter_common_next(logp, iter, lin, level, entry, error_out,
	    false);
}

void
get_worklist_name(char *buf, const char *policy, ifs_snapid_t snap,
    bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_worklist_restore_%llu", policy, snap);
	} else {
		sprintf(buf, "%s_worklist_%llu", policy, snap);
	}
}

void
get_worklist_pattern(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_worklist_restore_", policy);
	} else {
		sprintf(buf, "%s_worklist_", policy);
	}
}

/*
 * Add num_entries to worklist sbt 
 */
static void
add_wl_entries(struct sbt_bulk_entry *wl_entries, int num_entries,
    struct wl_ctx *ctx, struct isi_error **error_out)
{
	int res;
	int i = 0;
	struct isi_error *error = NULL;

	log(TRACE, "add_wl_entries");	
	/*
	 * First lets try to add all entries.
	 * It could fail sometimes due to restarts.
	 * i.e we might have added few of those entries
	 * in previous run, but we can take chance since
	 * restarts are minimal.
	 */

	res = ifs_sbt_bulk_op(num_entries, wl_entries);
	if (res == -1 && errno != EEXIST) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d worklist entries begining with "
		    " %llx:%llx from log to disk", num_entries,
		    wl_entries[0].key.keys[0], wl_entries[0].key.keys[1]);
		goto out;
	}
	
	if (res == 0)
		goto out;

	/* There are some entries already in sbt */
	while (i < num_entries) {
		res = ifs_sbt_get_entry_at(ctx->wl_fd, &wl_entries[i].key,
		    NULL, 0, NULL, NULL);
		if (res == -1 && errno != ENOENT) {
			error = isi_system_error_new(errno,
			    "Error while reading worklist entry %llx:%llx",
			    wl_entries[i].key.keys[0],
			    wl_entries[i].key.keys[1]);
			goto out;
		}
			
		if (res == 0) {
		    wl_entries[i].op_type = SBT_SYS_OP_MOD;
		}

		i++;
	}

	res = ifs_sbt_bulk_op(num_entries, wl_entries);
	if (res == -1) {
		error = isi_system_error_new(errno,
		    "Error while syncing %d worklist entries begining with "
		    " %llx:%llx from log to disk (attempt modify)", num_entries,
		    wl_entries[0].key.keys[0], wl_entries[0].key.keys[1]);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

bool
wl_log_entries(struct wl_log *logp)
{
	ASSERT(logp != NULL);
	if (logp->index > 0)
		return true;
	else
		return false;
}

static int
wl_compr(const void *elem1, const void *elem2)
{
	struct wl_log_entry *entry1 = (struct wl_log_entry *)elem1;
	struct wl_log_entry *entry2 = (struct wl_log_entry *)elem2;
	uint64_t lin1 = entry1->lin;
	uint64_t lin2 = entry2->lin;
	
	if (lin1 < lin2) {
		return -1;
	} else if (lin1 == lin2) {
		return 0;
	} else {
		return 1;
	}	
}

/*
 * This function does flush of all entries from in-memory
 * worklist log to on-disk worklist sbt.
 */
void
flush_wl_log(struct wl_log *logp, struct wl_ctx *wl, uint64_t wi_lin,
    unsigned int level, struct isi_error **error_out)
{

	int i = 0;
	struct isi_error *error = NULL;
	int j = 0;
	int entry_level = 0;
	struct sbt_bulk_entry wl_entries[SBT_BULK_MAX_OPS];
	struct btree_flags sbt_flags = {};
	struct wl_log_entry *current_entry = NULL;

	log(TRACE, "flush_wl_log");
	
	/* First lets sort the the worklist entries */
	qsort(logp->entries, logp->index, sizeof(struct wl_log_entry), wl_compr);

	bzero(wl_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	while(i < logp->index) {

		wl_entries[j].fd = wl->wl_fd;
		current_entry = &logp->entries[i];
		wl_entries[j].op_type = current_entry->operation;
		if (current_entry->operation == SBT_SYS_OP_ADD) {
			/* Get the key based on next level of tree */
			entry_level = level + 1;
		} else {
			/* Get the key based on specified level */
			entry_level = level;
		}
		set_wl_key(&wl_entries[j].key, wi_lin,
		    entry_level, current_entry->lin);
		wl_entries[j].entry_buf = NULL;
		wl_entries[j].entry_size = 0;
		wl_entries[j].entry_flags = sbt_flags;
		wl_entries[j].cm = BT_CM_NONE;
		wl_entries[j].old_entry_buf = NULL;
		wl_entries[j].old_entry_size = 0;
		j++;
	
		if (j == SBT_BULK_MAX_OPS) {
			add_wl_entries(wl_entries, j, wl, &error);
			if (error)
				goto out;

			j = 0;
		}

		i++;
	}

	if (j) {
		add_wl_entries(wl_entries, j, wl, &error);
		if (error)
			goto out;
	}

	logp->index = 0;
out:
	isi_error_handle(error, error_out);
}

/*
 * This function adds lin entry to worklist log.
 * If the log is full, then we first flush the log
 */
static void
wl_log_mod_entry_common(struct wl_log *logp, struct wl_ctx *wl, uint64_t wi_lin,
    unsigned int level, ifs_lin_t lin, enum sbt_bulk_op_type op,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter", __func__);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		/* Log is full, so flush it */
		flush_wl_log(logp, wl, wi_lin, level, &error);
		if (error)
			goto out;
	}

	logp->entries[logp->index].lin = lin;
	logp->entries[logp->index].operation = op;
	logp->index++;

out:
	isi_error_handle(error, error_out);
}
void
wl_log_add_entry_1(struct wl_log *logp, struct wl_ctx *wl, uint64_t wi_lin,
    unsigned int level, ifs_lin_t lin, struct isi_error **error_out)
{
	wl_log_mod_entry_common(logp, wl, wi_lin, level, lin, SBT_SYS_OP_ADD,
	    error_out);
}

void
wl_log_del_entry(struct wl_log *logp, struct wl_ctx *wl,
    btree_key_t key, struct isi_error **error_out)
{
	uint64_t wi_lin;
	ifs_lin_t lin;
	int level;

	wi_lin = get_wi_lin_from_btree_key(&key);
	lin = get_lin_from_btree_key(&key);
	level = get_level_from_btree_key(&key);

	wl_log_mod_entry_common(logp, wl, wi_lin, level, lin, SBT_SYS_OP_DEL,
	    error_out);
}

/*
 * This function removes any worklist lins from specified level whose parent
 * lins do not fall in the lin range specified.
 */
void
wl_remove_entries_parent_range(struct wl_ctx *wl, uint64_t wi_lin, int level,
    ifs_snapid_t snap, uint64_t range1, uint64_t range2,
    struct isi_error **error_out)
{

	struct sbt_bulk_entry wl_entries[SBT_BULK_MAX_OPS];
	struct isi_error *error = NULL;
	struct btree_flags sbt_flags = {};
	unsigned int lin_level;
	bool exists = false;
	struct wl_iter *iter = NULL;
	struct parent_entry pe;
	unsigned num_entries = 0;
	btree_key_t key;
	int i = 0;
	ifs_lin_t lin = 0;
	int res;
	

	log(TRACE, "wl_remove_entries_parent_range");
	set_wl_key(&key, wi_lin, level, 0);
	iter = new_wl_iter(wl, &key);
	bzero(wl_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	while(1) {

		exists = wl_iter_next(NULL, iter, &lin, &lin_level, NULL,
		    &error);
		if (error)
			goto out;

		if (!exists)
			break;
		num_entries = 1;
		res = pctl2_lin_get_parents(lin, snap, &pe, 1, &num_entries);
		if (res) {
			error = isi_system_error_new(errno,
			    "Failed to find parent of lin %llx:%lld", lin,
			    snap);
			goto out;
		}
		/* Parent within range */
		if (pe.lin >= range1 &&  pe.lin <= range2)
			continue;
		ASSERT(level == lin_level);
		wl_entries[i].fd = wl->wl_fd;
		wl_entries[i].op_type = SBT_SYS_OP_DEL;
		/* Get the key based on next level of tree */
		set_wl_key(&wl_entries[i].key, wi_lin,
		    lin_level, lin);
		wl_entries[i].entry_buf = NULL;
		wl_entries[i].entry_size = 0;
		wl_entries[i].entry_flags = sbt_flags;
		wl_entries[i].cm = BT_CM_NONE;
		wl_entries[i].old_entry_buf = NULL;
		wl_entries[i].old_entry_size = 0;
		i++;
	
		if (i == SBT_BULK_MAX_OPS) {
			res = ifs_sbt_bulk_op(i, wl_entries);
			if (res == -1) {
				error = isi_system_error_new(errno,
				    "Unable to delete worklist lins in level"
				    " %d of %llx", level, wi_lin);
				goto out;
			}
			i = 0;
		}
	}

	if (i) {
		res = ifs_sbt_bulk_op(i, wl_entries);
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Unable to delete worklist lins in level"
			    " %d of %llx", level, wi_lin);
			goto out;
		}
	}

out:
	if (iter)
		close_wl_iter(iter);

	isi_error_handle(error, error_out);

}

void
wl_remove_entries(struct wl_ctx *wl, uint64_t wi_lin, int level,
    struct isi_error **error_out)
{

	struct sbt_bulk_entry wl_entries[SBT_BULK_MAX_OPS];
	struct isi_error *error = NULL;
	struct btree_flags sbt_flags = {};
	unsigned int lin_level;
	bool exists = false;
	struct wl_iter *iter = NULL;
	btree_key_t key;
	int i = 0;
	ifs_lin_t lin = 0;
	int res;
	

	log(TRACE, "wl_remove_entries");
	set_wl_key(&key, wi_lin, level, 0);
	iter = new_wl_iter(wl, &key);
	bzero(wl_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	while(1) {

		exists = wl_iter_next(NULL, iter, &lin, &lin_level, NULL,
		    &error);
		if (error)
			goto out;

		if (!exists)
			break;

		wl_entries[i].fd = wl->wl_fd;
		wl_entries[i].op_type = SBT_SYS_OP_DEL;
		/* Get the key based on next level of tree */
		set_wl_key(&wl_entries[i].key, wi_lin,
		    lin_level, lin);
		wl_entries[i].entry_buf = NULL;
		wl_entries[i].entry_size = 0;
		wl_entries[i].entry_flags = sbt_flags;
		wl_entries[i].cm = BT_CM_NONE;
		wl_entries[i].old_entry_buf = NULL;
		wl_entries[i].old_entry_size = 0;
		i++;
	
		if (i == SBT_BULK_MAX_OPS) {
			res = ifs_sbt_bulk_op(i, wl_entries);
			if (res == -1) {
				error = isi_system_error_new(errno,
				    "Unable to delete worklist lins in level"
				    " %d of %llx", level, wi_lin);
				goto out;
			}
			i = 0;
		}
	}

	if (i) {
		res = ifs_sbt_bulk_op(i, wl_entries);
		if (res == -1) {
			error = isi_system_error_new(errno,
			    "Unable to delete worklist lins in level"
			    " %d of %llx", level, wi_lin);
			goto out;
		}
	}

out:
	if (iter)
		close_wl_iter(iter);

	isi_error_handle(error, error_out);

}

