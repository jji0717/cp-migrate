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
#include "repstate.h"

#define REPSTATE_BUF_SIZE 8192

/* sbt entry flag definitions */
#define FL_NON_HASHED	(1 << 0)
#define FL_IS_DIR 	(1 << 1)
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

enum rep_field {
	LCOUNT = 0,
	EXCEPTION,
	INCL_FOR_CHILD,
	HASH_COMPUTED,
	NEW_DIR_PARENT,
	NOT_ON_TARGET,
	CHANGED,
	FLIPPED,
	LCOUNT_SET,
	LCOUNT_RESET,
	FOR_PATH,
	HL_DIR_LIN,
	HL_DIR_OFFSET,
	EXCL_LCOUNT,
	MAX_REP
};

enum rep_item_type {
	RT_BOOL,
	RT_UINT16,
	RT_UINT32,
	RT_UINT64,
	RT_VARIABLE
};

struct rep_item {
	enum rep_field field;
	uint32_t type;
	uint32_t bit;			/* bit mask */
	uint64_t def;			/* default value */
};

static struct rep_item rep_table[] = {
	{LCOUNT,	 RT_UINT32,	(1 << LCOUNT),		1},
	{EXCEPTION, 	 RT_BOOL,	(1 << EXCEPTION),	0},
	{INCL_FOR_CHILD, RT_BOOL,	(1 << INCL_FOR_CHILD),	0},
	{HASH_COMPUTED,	 RT_BOOL,	(1 << HASH_COMPUTED),	0},
	{NEW_DIR_PARENT, RT_BOOL,	(1 << NEW_DIR_PARENT),	0},
	{NOT_ON_TARGET,  RT_BOOL,	(1 << NOT_ON_TARGET),	0},
	{CHANGED,	 RT_BOOL,	(1 << CHANGED),		0},
	{FLIPPED,	 RT_BOOL,	(1 << FLIPPED),		0},
	{LCOUNT_SET,     RT_BOOL,	(1 << LCOUNT_SET),	1},
	{LCOUNT_RESET,   RT_BOOL,	(1 << LCOUNT_RESET),	0},
	{FOR_PATH,       RT_BOOL,	(1 << FOR_PATH),	0},
	{HL_DIR_LIN,     RT_UINT64,	(1 << HL_DIR_LIN),	0},
	{HL_DIR_OFFSET,  RT_UINT64,	(1 << HL_DIR_OFFSET),	0},
	{EXCL_LCOUNT,    RT_UINT32,	(1 << EXCL_LCOUNT),	0}
};

static int
type_to_size(enum rep_item_type type) {
	switch(type) {
	case RT_BOOL:
		return sizeof(bool);
	case RT_UINT16:
		return sizeof(uint16_t);
	case RT_UINT32:
		return sizeof(uint32_t);
	case RT_UINT64:
		return sizeof(uint64_t);
	default:
		ASSERT(!"invalid repstate data type");
	}
}

static void
set_value(enum rep_field field, uint64_t value, uint32_t *flags, char *buf,
    int *off, int buf_len)
{
	int size = 0;

	ASSERT(field < MAX_REP);
	ASSERT(buf && off);

	/* if value is default for field, do nothing */
	if (value == rep_table[field].def)
		goto out;
	
	/* value isn't default so set its flag bit */
	*flags |= rep_table[field].bit;

	if (rep_table[field].type == RT_BOOL) {
		/* nothing to do since value is not default, so value is
		 * literally equal to !rep_table[field].def */
		goto out;
	} 

	size = type_to_size(rep_table[field].type);
	ASSERT(*off + size < buf_len);
	memcpy(&buf[*off], &value, size);
	*off += size;

out:
	return;
}

static void
get_value(enum rep_field field, void *value, uint32_t flags, char *buf,
    int *off, int buf_len)
{
	int size;

	ASSERT(field < MAX_REP);
	ASSERT(buf && off);
	
	if (rep_table[field].type == RT_BOOL) {
		/* for bool, value is either default (clear bit) or not
		 * default (set bit) */
		*((bool *)value) = ((flags & rep_table[field].bit) == 0) ?
		    rep_table[field].def : !rep_table[field].def;
		goto out;
	}

	/* if flag is unset, value is the default for the field */
	if ((flags & rep_table[field].bit) == 0) {
		memcpy(value, &rep_table[field].def,
		    type_to_size(rep_table[field].type));
		goto out;
	}

	size = type_to_size(rep_table[field].type);
	ASSERT(*off + size <= buf_len);
	memcpy(value, &buf[*off], size);
	*off += size;

out:
	return;
}

/* for each repstate data item, a "0" in the corresponding bit field position
 * indicates that the value is at its default, so no value is recorded in the
 * repstate data section.
 *
 * for bools, if the flag is unset, the value is the default
 * (rep_table[field].def), otherwise it is the opposite.
 *
 * for other types, if the flag is unset, the value is the the default,
 * otherwise it is stored to/retrieved from the the data section.
 *
 * if all fields in the rep entry are at their default value, the 'all default'
 * sbt entry flag is set and no data is written to the entry. */

static void
pack_entry(char *buf, size_t *len, struct btree_flags *bt_flags,
    struct rep_entry *rep)
{
	uint32_t flags = 0;
	int off = 4;			/* start past bit field (word) */

	set_value(LCOUNT, rep->lcount, &flags, buf, &off, *len);
	set_value(EXCEPTION, rep->exception, &flags, buf, &off, *len);
	set_value(INCL_FOR_CHILD, rep->incl_for_child, &flags, buf, &off, *len);
	set_value(HASH_COMPUTED, rep->hash_computed, &flags, buf, &off, *len);
	set_value(NEW_DIR_PARENT, rep->new_dir_parent, &flags, buf, &off, *len);
	set_value(NOT_ON_TARGET, rep->not_on_target, &flags, buf, &off, *len);
	set_value(CHANGED, rep->changed, &flags, buf, &off, *len);
	set_value(FLIPPED, rep->flipped, &flags, buf, &off, *len);
	set_value(LCOUNT_SET, rep->lcount_set, &flags, buf, &off, *len);
	set_value(LCOUNT_RESET, rep->lcount_reset, &flags, buf, &off, *len);
	set_value(FOR_PATH, rep->for_path, &flags, buf, &off, *len);
	set_value(HL_DIR_LIN, rep->hl_dir_lin, &flags, buf, &off, *len);
	set_value(HL_DIR_OFFSET, rep->hl_dir_offset, &flags, buf, &off, *len);
	set_value(EXCL_LCOUNT, rep->excl_lcount, &flags, buf, &off, *len);

	if (rep->is_dir)
		bt_flags->flags |= FL_IS_DIR;

	if (rep->non_hashed)
		bt_flags->flags |= FL_NON_HASHED;

	/* if entry has all default values, set the all_default flag and
	 * don't write anything inside the sbt entry */
	if (flags == 0) {
		*buf = '\0';
		*len = 0;
	} else {
		/* copy flags into buffer */
		memcpy(buf, &flags, sizeof(flags));
		*len = off;
	}
}

static void
unpack_entry(char *buf, size_t len, struct btree_flags *bt_flags,
    struct rep_entry *rep)
{
	uint32_t flags;
	int off = 4;			/* start past bit field word) */

	ASSERT(buf && rep);
	memset(rep, 0, sizeof(struct rep_entry));

	if (len == 0) {
		/* populate entry with all default values. there is no data
		 * in the buffer in the all default case */
		flags = 0;
	} else {
		/* copy flags out of buffer */
		memcpy(&flags, buf, sizeof(flags));
	}

	if (bt_flags->flags & FL_IS_DIR)
		rep->is_dir = true;

	if (bt_flags->flags & FL_NON_HASHED)
		rep->non_hashed = true;

	get_value(LCOUNT, &rep->lcount, flags, buf, &off, len);
	get_value(EXCEPTION, &rep->exception, flags, buf, &off, len);
	get_value(INCL_FOR_CHILD, &rep->incl_for_child, flags, buf, &off, len);
	get_value(HASH_COMPUTED, &rep->hash_computed, flags, buf, &off, len);
	get_value(NEW_DIR_PARENT, &rep->new_dir_parent, flags, buf, &off, len);
	get_value(NOT_ON_TARGET, &rep->not_on_target, flags, buf, &off, len);
	get_value(CHANGED, &rep->changed, flags, buf, &off, len);
	get_value(FLIPPED, &rep->flipped, flags, buf, &off, len);
	get_value(LCOUNT_SET, &rep->lcount_set, flags, buf, &off, len);
	get_value(LCOUNT_RESET, &rep->lcount_reset, flags, buf, &off, len);
	get_value(FOR_PATH, &rep->for_path, flags, buf, &off, len);
	get_value(HL_DIR_LIN, &rep->hl_dir_lin, flags, buf, &off, len);
	get_value(HL_DIR_OFFSET, &rep->hl_dir_offset, flags, buf, &off, len);
	get_value(EXCL_LCOUNT, &rep->excl_lcount, flags, buf, &off, len);
}

/* hash based in memory cache of the select tree */

static size_t
sel_lin_keysize(const void *x)
{
	const uint64_t *key = x;
	return sizeof(*key);
}

static void
sel_hash_free(struct isi_hash *sel_hash)
{
	struct sel_hash_entry *se = NULL;

	ISI_HASH_FOREACH(se, sel_hash, entry) {
		isi_hash_remove(sel_hash, &se->entry);
		free(se);
	}
	isi_hash_destroy(sel_hash);
	free(sel_hash);
}

static void
sel_hash_init(struct rep_ctx *ctx, struct isi_error **error_out)
{
	struct isi_hash *sel_hash = NULL;
	struct sel_hash_entry *se = NULL;
	struct rep_iter *iter = NULL;
	struct sel_entry sentry;
	uint64_t sel_lin = 0;
	bool found;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	sel_hash = calloc(1, sizeof(struct isi_hash));
	ASSERT(sel_hash);
	ISI_HASH_VAR_INIT(sel_hash, sel_hash_entry, lin, entry,
	    sel_lin_keysize, NULL);

	iter = new_rep_iter(ctx);
	while (true) {
		found = sel_iter_next(iter, &sel_lin, &sentry, &error);
		if (error)
			goto out;
		else if (!found)
			break;
		se = calloc(1, sizeof(struct sel_hash_entry));
		ASSERT(se);
		se->lin = sel_lin;
		se->plin = sentry.plin;
		strlcpy(se->name, sentry.name, 256);
		se->enc = sentry.enc;
		se->includes = sentry.includes;
		se->excludes = sentry.excludes;
		se->for_child = sentry.for_child;

		isi_hash_add(sel_hash ,&se->entry);
	}
	ctx->sel_hash = sel_hash;
	sel_hash = NULL;
out:
	close_rep_iter(iter);
	if (sel_hash) {
		sel_hash_free(sel_hash);
		sel_hash = NULL;
	}

	isi_error_handle(error, error_out);
}

bool
get_sel_hash_entry(u_int64_t lin, struct sel_hash_entry *sel,
    struct rep_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool exists = false;
	struct sel_hash_entry *se;
	struct isi_hash_entry *he;

	log(TRACE, "%s", __func__);
	/* populate the hash if it doesn't yet exist */
	if (ctx->sel_hash == NULL) {
		sel_hash_init(ctx, &error);
		if (error)
			goto out;
	}

	he = isi_hash_get(ctx->sel_hash, &lin);
	if (he) {
		se = container_of(he, struct sel_hash_entry, entry);
		*sel = *se;
		exists = true;
		log(TRACE, "%s: found select hash entry lin: %llx plin: %llx",
		    __func__, lin, sel->plin);
	}
out:
	isi_error_handle(error, error_out);
	return exists;
}


/*  ____                _        _         ____      _____
 * |  _ \ ___ _ __  ___| |_ __ _| |_ ___  | __ )    |_   _| __ ___  ___
 * | |_) / _ \ '_ \/ __| __/ _` | __/ _ \ |  _ \ _____| || '__/ _ \/ _ \
 * |  _ <  __/ |_) \__ \ || (_| | ||  __/ | |_) |_____| || | |  __/  __/
 * |_| \_\___| .__/|___/\__\__,_|\__\___| |____/      |_||_|  \___|\___|
 *           |_|
 */

void
prune_old_repstate(const char *policy, char *name_root,
    ifs_snapid_t *snap_kept, int num_kept, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_prune_old(siq_btree_repstate, policy, name_root, snap_kept,
	    num_kept, &error);

	isi_error_handle(error, error_out);
}

void
create_repstate(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	
	siq_btree_create(siq_btree_repstate, policy, name, exist_ok, &error);

	isi_error_handle(error, error_out);
}

void
open_repstate(const char *policy, char *name, struct rep_ctx **ctx_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_repstate, policy, name, &error);
	if (!error) {
		(*ctx_out) = calloc(1, sizeof(struct rep_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->rep_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);
}

void
set_dynamic_rep_lookup(struct rep_ctx *ctx, ifs_snapid_t snapid,
    ifs_domainid_t domain_id)
{
	ASSERT(ctx != NULL);
	ctx->dynamic = true;
	ctx->snapid = snapid;
	ctx->domain_id = domain_id;
}

/**
 * Return whether a file with the appropriate name exists.
 */
bool
repstate_exists(const char *policy, char *name, struct isi_error **error_out)
{
	struct rep_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	open_repstate(policy, name, &ctx, &error);
	if (!error) {
		exists = true;
		close_repstate(ctx);
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
close_repstate(struct rep_ctx *ctx)
{
	log(TRACE, "close_repstate");

	if (ctx == NULL)
		return;

	if (ctx->rep_fd > 0)
		close(ctx->rep_fd);

	if (ctx->sel_hash)
		sel_hash_free(ctx->sel_hash);

	free(ctx);
}

void
remove_repstate(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_remove(siq_btree_repstate, policy, name, missing_ok, &error);

	isi_error_handle(error, error_out);
}

static void
add_tw_entries( struct rep_ctx *ctx, uint64_t lin, int index, int num_entries, 
    struct sbt_bulk_entry *bentries, char *tw_data, size_t tw_size,
    struct isi_error **error_out)
{
	size_t tw_left;
	int i;
	char *marker;
	bool exists;
	struct isi_error *error = NULL;
	int next_key = 1;
	struct btree_flags flags = {};
	
	log(TRACE, "add_tw_entries");
	tw_left = tw_size;
	i = index;
	marker = tw_data;

	while (tw_left > 0) {
		ASSERT(next_key <= num_entries);
		exists = entry_exists(lin + next_key, ctx, &error);
		if (error)
			goto out;
		
		bentries[i].fd = ctx->rep_fd;
		if (exists)
			bentries[i].op_type = SBT_SYS_OP_MOD;
		else
			bentries[i].op_type = SBT_SYS_OP_ADD;
		lin_to_key(lin + next_key, &(bentries[i].key));
		bentries[i].entry_buf = marker;
		bentries[i].entry_size = MIN(tw_left, MAX_TW_ENTRY_SIZE);
		bentries[i].entry_flags = flags;
		bentries[i].cm = BT_CM_NONE;
		bentries[i].old_entry_buf = NULL;
		bentries[i].old_entry_size = 0;
		
		tw_left -= bentries[i].entry_size;
		marker += bentries[i].entry_size;
		next_key++;
		i++;
	}

out:
	isi_error_handle(error, error_out);
	return;

}

bool
get_rep_info(struct rep_info_entry *rei, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res;
	int tmp_errno;
	btree_key_t key;
	size_t size;
	struct isi_error *error = NULL;
	bool found = false;

	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(REP_INFO_LIN, &key),
	    rei, sizeof(struct rep_info_entry), NULL,
	    &size);

	if (res < 0 && errno != ENOENT) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error getting repstate info");
		log(ERROR, "Error getting repstate info: %s",
		    strerror(tmp_errno));
	} else if (res >= 0) {
		found = true;
	}

	isi_error_handle(error, error_out);
	return found;
}

/**
 * Set the rep info.  Fail if the rep info is already set and
 * doesn't equal the value we're setting it to.  
 */
void
set_rep_info(struct rep_info_entry *rei, struct rep_ctx *ctx,
    bool force, struct isi_error **error_out)
{
	int res;
	int tmp_errno;
	btree_key_t key;
	struct rep_info_entry existing_info;
	size_t size;
	struct isi_error *error = NULL;

	res = ifs_sbt_add_entry(ctx->rep_fd, lin_to_key(REP_INFO_LIN, &key),
	    rei, sizeof(struct rep_info_entry), NULL);
	if (res < 0 && errno == EEXIST) {
		res = ifs_sbt_get_entry_at(ctx->rep_fd,
		    lin_to_key(REP_INFO_LIN, &key), &existing_info,
		    sizeof(struct rep_info_entry), NULL, &size);
		if (res == 0) {
			if (size != sizeof(struct rep_info_entry)) {
				error = isi_system_error_new(EIO,
				    "Bad rep info size %zu", size);
				log(ERROR, "Bad rep info size %zu", size);
				goto out;
			}
			if (rei->initial_sync_snap !=
			   existing_info.initial_sync_snap && !force) {
				error = isi_system_error_new(EIO,
				    "Unexpected rep info snap %lld, expected"
				    " %lld", existing_info.initial_sync_snap,
				    rei->initial_sync_snap);
				log(ERROR,
				    "Unexpected rep info snap %lld, expected"
				    " %lld", existing_info.initial_sync_snap,
				    rei->initial_sync_snap);
				goto out;
			}
			res = ifs_sbt_cond_mod_entry(ctx->rep_fd,
			    lin_to_key(REP_INFO_LIN, &key),
	    		    rei, sizeof(struct rep_info_entry), NULL,
			    BT_CM_BUF, &existing_info,
			    sizeof(struct rep_info_entry), NULL);
		}
	}

	if (res < 0) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting repstate info");
		log(ERROR, "Error setting repstate info: %s",
		    strerror(tmp_errno));
	}

 out:
	isi_error_handle(error, error_out);
}

void
set_entry(u_int64_t lin, struct rep_entry *rep, struct rep_ctx *ctx,
    u_int64_t wi_lin, struct work_restart_state *restart,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry bentries[2];
	int num_entries = 1;
	char sbt_buf[REPSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = REPSTATE_BUF_SIZE;
	struct btree_flags sbt_flags = {};

	log(TRACE, "set_entry");

	/*
	 * This is to avoid any issues with pointer field
	 * which is defined as 64 bit but probably is used 
	 * as 32 bit in userspace
	 */
	memset(bentries, 0, 2 * sizeof(struct sbt_bulk_entry));

	exists = entry_exists(lin, ctx, &error);
	if (error)
		goto out;

	/* Repstate entry */
	bentries[0].fd = ctx->rep_fd;
	if (exists)
		bentries[0].op_type = SBT_SYS_OP_MOD;
	else
		bentries[0].op_type = SBT_SYS_OP_ADD;
	lin_to_key(lin, &bentries[0].key);

	pack_entry(sbt_buf, &sbt_buf_size, &sbt_flags, rep);
	bentries[0].entry_buf = sbt_buf;
	bentries[0].entry_size = sbt_buf_size;
	bentries[0].entry_flags = sbt_flags;
	bentries[0].cm = BT_CM_NONE;
	bentries[0].old_entry_buf = NULL;
	bentries[0].old_entry_size = 0;

	/* Restart entry */
	if (restart) {
		num_entries = 2;
		bentries[1].fd = ctx->rep_fd;
		bentries[1].op_type = SBT_SYS_OP_MOD;
		ASSERT(wi_lin > 0);
		lin_to_key(wi_lin, &(bentries[1].key));
		bentries[1].entry_buf = restart;
		bentries[1].entry_size = sizeof(struct work_restart_state);
		bentries[1].cm = BT_CM_NONE;
		bentries[1].old_entry_buf = NULL;
		bentries[1].old_entry_size = 0;
	}
	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting repstate lin %llx", lin);
		log(ERROR, "Error setting repstate lin %llx: %s",
		    lin, strerror(tmp_errno));
	} else {
		log(DEBUG, "repstate entry set for lin %llx", lin);
	}

 out:
	isi_error_handle(error, error_out);
}

/*
 * Helper routine to create bulk entry based on old and new rep entries passed
 */
void
add_rep_bulk_entry(struct rep_ctx *ctx, ifs_lin_t lin,
    struct sbt_bulk_entry *bentry, enum sbt_bulk_op_type oper,
    struct rep_entry *rep, char *sbt_buf, struct btree_flags *sbt_flags,
    struct rep_entry *old_rep, char *sbt_old_buf,
    struct btree_flags *sbt_old_flags,
    const size_t sbt_buf_size)
{

	size_t sbt_new_buf_size = sbt_buf_size;
	size_t sbt_old_buf_size = sbt_buf_size;

	bentry->fd = ctx->rep_fd;

	bentry->op_type = oper;
	if (oper == SBT_SYS_OP_ADD) {
		bentry->cm = BT_CM_NONE;
		bentry->old_entry_buf = NULL;
		bentry->old_entry_size = 0;
	} else if (oper == SBT_SYS_OP_MOD) {
		bentry->cm = BT_CM_BOTH;
		pack_entry(sbt_old_buf, &sbt_old_buf_size, sbt_old_flags,
		    old_rep);
		ASSERT(sbt_old_buf_size <= sbt_buf_size);
		bentry->old_entry_buf = sbt_old_buf;
		bentry->old_entry_size = sbt_old_buf_size;
		bentry->old_entry_flags = *sbt_old_flags;
	} else
		bentry->op_type = SBT_SYS_OP_DEL;

	lin_to_key(lin, &(bentry->key));

	pack_entry(sbt_buf, &sbt_new_buf_size, sbt_flags, rep);
	ASSERT(sbt_new_buf_size <= sbt_buf_size);
	bentry->entry_buf = sbt_buf;
	bentry->entry_size = sbt_new_buf_size;
	bentry->entry_flags = *sbt_flags;
}

/**
 * Return true if modded, false if not modded due to the key
 * not existing or the conditional mod failing.
 */
bool
set_entry_cond(u_int64_t lin, struct rep_entry *rep, struct rep_ctx *ctx,
    bool is_new, struct rep_entry *old_rep, u_int64_t wi_lin,
    struct work_restart_state *restart, void *tw_data, size_t tw_size,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	bool success = false;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 1;
	char sbt_buf[REPSTATE_BUF_SIZE] = "";
	char sbt_old_buf[REPSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = REPSTATE_BUF_SIZE;
	size_t sbt_old_buf_size = REPSTATE_BUF_SIZE;
	struct btree_flags sbt_flags = {};
	struct btree_flags sbt_old_flags = {};
	int tw_entries = 0;
	char buf[SBT_MAX_ENTRY_SIZE_64] = {};
	struct work_restart_state *wrs_pad =
	    (struct work_restart_state *)buf;

	log(TRACE, "%s: enter lin: %llx wi_lin: %llx", __func__, lin, wi_lin);

	if (restart)
		num_entries++;

	if (tw_data) {
		ASSERT(tw_size > 0);
		tw_entries = (tw_size  / MAX_TW_ENTRY_SIZE) + 
		    (tw_size % MAX_TW_ENTRY_SIZE ? 1 : 0);
		ASSERT(tw_entries < WORK_LIN_RESOLUTION);
		/* Set the tw_blk_cnt value */
		ASSERT(restart != NULL);
		restart->tw_blk_cnt = tw_entries;
		num_entries += tw_entries;
	}

	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	/* Repstate entry */
	bentries[0].fd = ctx->rep_fd;
	if (!is_new) {
		bentries[0].op_type = SBT_SYS_OP_MOD;
		bentries[0].cm = BT_CM_BOTH;
		pack_entry(sbt_old_buf, &sbt_old_buf_size, &sbt_old_flags,
		    old_rep);
		bentries[0].old_entry_buf = sbt_old_buf;
		bentries[0].old_entry_size = sbt_old_buf_size;
		bentries[0].old_entry_flags = sbt_old_flags;
	} else {
		bentries[0].op_type = SBT_SYS_OP_ADD;
		bentries[0].cm = BT_CM_NONE;
		bentries[0].old_entry_buf = NULL;
		bentries[0].old_entry_size = 0;
	}
	lin_to_key(lin, &bentries[0].key);

	pack_entry(sbt_buf, &sbt_buf_size, &sbt_flags, rep);
	bentries[0].entry_buf = sbt_buf;
	bentries[0].entry_size = sbt_buf_size;
	bentries[0].entry_flags = sbt_flags;

	/* Restart entry */
	if (restart) {
		/* pad work item to sbt block size to avoid work item
		 * contention */
		memcpy(wrs_pad, restart, sizeof(struct work_restart_state));

		bentries[1].fd = ctx->rep_fd;
		bentries[1].op_type = SBT_SYS_OP_MOD;
		ASSERT(wi_lin > 0);
		lin_to_key(wi_lin, &(bentries[1].key));
		bentries[1].entry_buf = wrs_pad;
		bentries[1].entry_size = SBT_MAX_ENTRY_SIZE_64;
		bentries[1].cm = BT_CM_NONE;
		bentries[1].old_entry_buf = NULL;
		bentries[1].old_entry_size = 0;
		if (tw_data) {
			add_tw_entries(ctx, wi_lin, 2, num_entries, bentries,
			    tw_data, tw_size, &error);
			if (error)
				goto out;
		}
	}
	/* Perform the transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting repstate lin %llx", lin);
		log(ERROR, "Error %d setting repstate lin %llx",
		    tmp_errno, lin);
	} else if (!res) {
		success = true;
		log(DEBUG, "repstate entry set for lin %llx", lin);
	}

out:
	free(bentries);

	isi_error_handle(error, error_out);
	return success;
}

static bool
belongs_rep_domain(const struct domain_set *doms, ifs_domainid_t domain_id)
{
	int i, size;
	bool found = false;
	size = domain_set_size(doms);

	if (domain_id == 0) {
		found = true;
		goto out;
	}

	for (i = 0; i < size; i++) {
		if (domain_id == domain_set_atindex(doms, i)) {
			found = true;
			break;
		}
	}
out:
	return found;
}

/*
 * Do lookup of lin in snapshot to look for link count.
 */

bool
dynamic_entry_lookup(u_int64_t lin, ifs_snapid_t snap,
    ifs_domainid_t domain_id, struct rep_entry *rep,
    struct isi_error **error_out)
{
	int fd = -1;
	int res = 0;
	bool exists = false;
	bool in_snap = false;
	struct stat st;
	struct isi_error *error = NULL;
	struct domain_set doms = {}, ancestors = {};
	struct domain_entry dom_ent;

	memset(rep, 0, sizeof(struct rep_entry));
	/*
	 * Even if it is found in snap version, its possible that
	 * it could already be deleted. Make sure we double check 
	 * the lin by looking at nlink value.
	 */
	fd = ifs_lin_open(lin, snap, O_RDONLY);
	ASSERT(fd != 0);
	if (fd < 0 && (errno != ESTALE && errno != ENOENT)) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx", lin);
		goto out;
	}
	if (fd == -1) {
		/* We just found out that its missing in snap */
		goto out;
	}

	res = fstat(fd, &st);
	if (res == -1) {
		system("sysctl efs.dexitcode_ring >>/var/crash/bug_96018.txt");	
		error = isi_system_error_new(errno,
		 "Unable to fstat lin %llx\n", lin);
		goto out;
	}

	/* If the link count is not greater than 0, then file not present */
	if (st.st_nlink <= 0 || (st.st_flags & UF_ADS)) {
		goto out;
	}
	/* Check if the domain ID matches with one stored in context */
	res = dom_get_info_by_lin(lin, snap, &doms, &ancestors, NULL);
	if (res != 0) {
		error = isi_system_error_new(errno,
		    "Unable to get domain IDs for lin %llx", lin);
		goto out;
	}

	if (!belongs_rep_domain(&doms, domain_id)) {
		if (dom_get_entry(domain_id, &dom_ent) != 0) {
			error = isi_system_error_new(errno,
			    "failed to get domain entry for domain %llu",
			    domain_id);
			goto out;
		}
		/* ifs_check_in_snapshot is expensive and is only needed for
		 * snapshot restore jobs. skip it for other domain types */
		if ((dom_ent.d_flags & DOM_SNAPREVERT) == false)
			goto out;

		if (ifs_check_in_snapshot(lin, snap, 0, &in_snap) != 0) {
			error = isi_system_error_new(errno,
			    "ifs_check_in_snapshot failed for lin %llx", lin);
			goto out;
		}
		if (!in_snap)
			goto out;
	}

	exists = true;
	if (S_ISDIR(st.st_mode)) {
		rep->lcount = 1;
		rep->is_dir = true;
	} else {
		rep->lcount = st.st_nlink;
	}

out:
	if (fd > 0)
		close(fd);
	domain_set_clean(&doms);
	domain_set_clean(&ancestors);
	isi_error_handle(error, error_out);
	return exists;
}

bool
get_entry(u_int64_t lin, struct rep_entry *rep, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists = false;
	char sbt_buf[REPSTATE_BUF_SIZE] = "";
	size_t sbt_buf_size = 0;
	struct btree_flags bt_flags = {};

	log(TRACE, "get_entry");

	if (ctx->dynamic) {
		ASSERT(ctx->snapid > 0);
		/* Dynamic lookup expected. Hence directly read snapshot */
		exists = dynamic_entry_lookup(lin, ctx->snapid, ctx->domain_id,
		    rep, &error);
		if (error)
			goto out;
	} else {
		res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(lin, &key),
		    sbt_buf, REPSTATE_BUF_SIZE, &bt_flags, &sbt_buf_size);
		if (res == -1) {
			if (errno != ENOENT) {
				tmp_errno = errno;
				error = isi_system_error_new(tmp_errno,
				    "Error reading repstate lin %llx", lin);
				log(ERROR, "Error reading repstate lin %llx:"
				    " %s", lin, strerror(tmp_errno));
			}
			goto out;
		}
		
		exists = true;
		unpack_entry(sbt_buf, sbt_buf_size, &bt_flags, rep);
	}
	log(DEBUG, "got repstate entry for lin %llx", lin);

out:
	isi_error_handle(error, error_out);
	return exists;
}

/*
 * This gets the inclusion/exclusion entries from select tree
 */
bool
get_sel_entry(u_int64_t lin, struct sel_entry *sel, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "get_sel_entry");
	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(lin, &key), sel,
	    sizeof(struct sel_entry), NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading select tree lin %llx", lin);
			log(ERROR, "Error reading select tree lin %llx: %s",
			    lin, strerror(tmp_errno));
		}
	} else {
		exists = true;
		log(DEBUG, "got select tree entry for lin %llx", lin);
	}
	isi_error_handle(error, error_out);
	return exists;
}

bool
get_work_entry(u_int64_t lin, struct work_restart_state *ws,
    char **tw_data, size_t *tw_size, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;
	unsigned int i;
	struct isi_malloc_ostream mos;
	struct isi_ostream *os;
	char *buf = NULL;
	char pad_buf[SBT_MAX_ENTRY_SIZE_64] = {};
	struct work_restart_state *wrs_pad =
	    (struct work_restart_state *)pad_buf;

	log(TRACE, "get_work_entry");
	ASSERT((lin % WORK_LIN_RESOLUTION) == 0);

	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(lin, &key), wrs_pad,
	    SBT_MAX_ENTRY_SIZE_64, NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading repstate lin %llx", lin);
			log(ERROR, "Error reading repstate lin %llx: %s",
			    lin, strerror(tmp_errno));
		}
		goto out;
	} else {
		memcpy(ws, wrs_pad, sizeof(struct work_restart_state));
		exists = true;
		log(DEBUG, "got work entry for lin %llx", lin);

		if (!tw_data || !tw_size)
			goto out;
	
		if (!ws->tw_blk_cnt) {
			*tw_data = NULL;
			*tw_size = 0;
			goto out;
		}
				
		ASSERT(ws->tw_blk_cnt < WORK_LIN_RESOLUTION);
		isi_malloc_ostream_init(&mos, NULL);
		os = &mos.super;
		buf = malloc(MAX_TW_ENTRY_SIZE);
		ASSERT(buf != NULL);
		ASSERT(ws->tw_blk_cnt < WORK_LIN_RESOLUTION);
		for(i = 1; i <= ws->tw_blk_cnt; i++) {
			key.keys[0] = lin + i;
			key.keys[1] = 0;	
			res = ifs_sbt_get_entry_at(ctx->rep_fd, &key, buf,
			    MAX_TW_ENTRY_SIZE, NULL, &size);
			if (res == -1) {
				tmp_errno = errno;
				error = isi_system_error_new(tmp_errno,
				    "Error reading %u slice of tw data for"
				    " work %llx", i, lin);
				log(ERROR, "Error reading %u slice of tw data"
				    " for work %llx: %s", i,
				    lin, strerror(tmp_errno));
				goto out;
			}
			ASSERT(size <= MAX_TW_ENTRY_SIZE);
			isi_ostream_put_bytes(os, buf, size);
		}
		
		*tw_size = isi_malloc_ostream_used(&mos);
		*tw_data = (char *)isi_malloc_ostream_detach(&mos);
	}
out:
	if (buf)
		free(buf);
	isi_error_handle(error, error_out);
	return exists;
}

void
get_state_entry(u_int64_t lin, char *buf, size_t *size, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;

	log(TRACE, "get_state_entry");

	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(lin, &key), buf,
	    *size, NULL, size);
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error reading job phase lin %llx", lin);
		log(ERROR, "Error reading job phase lin %llx: %s",
		    lin, strerror(tmp_errno));
	} else {
		ASSERT(*size <= job_phase_max_size());
		log(DEBUG, "got job phase entry for lin %llx", lin);
	}
	isi_error_handle(error, error_out);
	return;
}

bool
get_repkey_at_loc(unsigned num, unsigned den, uint64_t *lin, 
    struct rep_entry *rep, struct rep_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	int tmp_errno;
	struct isi_error *error = NULL;
	btree_key_t key;
	bool exists;

	log(TRACE, "get_repkey_at_loc");

	res = ifs_sbt_get_entry_pct(ctx->rep_fd, num, den, 
	    &key, NULL, 0, NULL, &size);
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading repstate at loc %u/%u", num, den);
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
get_rep_max_lin(struct rep_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_iter *rep_iter = NULL;
	struct rep_entry rep_ent;
	ifs_lin_t max_lin = -1, cur_lin = -1;
	bool found = false;

	log(TRACE, "%s", __func__);

	ASSERT(ctx != NULL);

	/* Get the estimated largest lin in the btree */
	found = get_repkey_at_loc(100, 100, &cur_lin, NULL, ctx, &error);
	if (error)
		goto out;	
	if (!found) {
		error = isi_system_error_new(errno,
		    "Error reading repstate at loc 100/100:"
		    "Entry not found");
		goto out;
	}

	/* Iterate to the last lin in the btree */
	rep_iter = new_rep_range_iter(ctx, cur_lin);
	while(found) {
		found = rep_iter_next(rep_iter, &cur_lin, &rep_ent, &error);
		if (error)
			goto out;
	}

	max_lin = cur_lin;

out:
	close_rep_iter(rep_iter);
	isi_error_handle(error, error_out);
	return max_lin;
}

/*
 * This sets the inclusion/exclusion entries in select tree
 */
void
set_sel_entry(u_int64_t lin, struct sel_entry *sel,
    struct rep_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	btree_key_t key;
	struct isi_error *error = NULL;

	log(TRACE, "set_sel_entry");

	exists = entry_exists(lin, ctx, &error);
	if (error)
		goto out;

	if (exists) {
		res = ifs_sbt_mod_entry(ctx->rep_fd, lin_to_key(lin, &key),
		    sel, sizeof(struct sel_entry), NULL);
	} else {
		res = ifs_sbt_add_entry(ctx->rep_fd, lin_to_key(lin, &key),
		    sel, sizeof(struct sel_entry), NULL);
	}
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting select tree lin %llx", lin);
		log(ERROR, "Error setting select tree lin %llx: %s",
		    lin, strerror(tmp_errno));
	} else {
		log(DEBUG, "select tree  entry set for lin %llx", lin);
	}

 out:
	isi_error_handle(error, error_out);
}

void
set_work_entry(u_int64_t lin, struct work_restart_state *restart,
    void *tw_data, size_t tw_size, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	bool exists;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 1;
	int tw_entries = 0;
	struct btree_flags flags = {};
	char buf[SBT_MAX_ENTRY_SIZE_64] = {};
	struct work_restart_state *wrs_pad =
	    (struct work_restart_state *)buf;

	log(TRACE, "set_work_entry");

	if (tw_data) {
		ASSERT(tw_size > 0);
		tw_entries = (tw_size  / MAX_TW_ENTRY_SIZE) + 
		    (tw_size % MAX_TW_ENTRY_SIZE ? 1 : 0);
		ASSERT(tw_entries < WORK_LIN_RESOLUTION);
		/* Set the tw_blk_cnt value */
		restart->tw_blk_cnt = tw_entries;
		num_entries += tw_entries;
	}

	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	exists = entry_exists(lin, ctx, &error);
	if (error)
		goto out;

	/* pad work item to sbt block size to avoid work item contention */
	memcpy(wrs_pad, restart, sizeof(struct work_restart_state));

	/* Repstate entry */
	bentries[0].fd = ctx->rep_fd;
	if (exists) {
		bentries[0].op_type = SBT_SYS_OP_MOD;
	} else {
		bentries[0].op_type = SBT_SYS_OP_ADD;
	}
	ASSERT(lin > 0);
	lin_to_key(lin, &bentries[0].key);
	bentries[0].entry_buf = wrs_pad;
	bentries[0].entry_size = SBT_MAX_ENTRY_SIZE_64;
	bentries[0].entry_flags = flags;
	bentries[0].cm = BT_CM_NONE;
	bentries[0].old_entry_buf = NULL;
	bentries[0].old_entry_size = 0;

	if (tw_data) {
		add_tw_entries(ctx, lin, 1, num_entries, bentries,
		    tw_data, tw_size, &error);
		if (error)
			goto out;
	}

	/* Perform the transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting work item %llx", lin);
		log(ERROR, "Error %d setting work item %llx",
		    tmp_errno, lin);
	} else if (!res) {
		log(DEBUG, "work item %llx updated ", lin);
	}

out:
	free(bentries);

	isi_error_handle(error, error_out);
	return;

}

void
update_work_entries(struct rep_ctx *ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, struct isi_error **error_out)
{
	update_work_entries_cond(ctx, p_lin, parent, p_tw_data, p_tw_size,
	     c_lin, child, c_tw_data, c_tw_size, false, error_out);
}

void
update_work_entries_cond(struct rep_ctx *ctx, u_int64_t p_lin,
    struct work_restart_state *parent, void *p_tw_data, size_t p_tw_size,
     u_int64_t c_lin, struct work_restart_state *child, void *c_tw_data,
    size_t c_tw_size, bool check_disk, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 2;
	int p_tw_entries = 0, c_tw_entries = 0;
	struct btree_flags flags = {};
	int i = 0;
	btree_key_t check_key;
	size_t check_size;
	char parent_buf[SBT_MAX_ENTRY_SIZE_64] = {};
	char parent_check_buf[SBT_MAX_ENTRY_SIZE_64] = {};
	char child_buf[SBT_MAX_ENTRY_SIZE_64] = {};
	struct work_restart_state *parent_pad =
	    (struct work_restart_state *)parent_buf;
	struct work_restart_state *parent_check_pad =
	    (struct work_restart_state *)parent_check_buf;
	struct work_restart_state *child_pad =
	    (struct work_restart_state *)child_buf;

	log(TRACE, "update_work_entries_cond");
	ASSERT(parent != NULL && child != NULL);
	ASSERT(p_lin > 0 && c_lin > 0);
	ASSERT( (p_lin % WORK_LIN_RESOLUTION) == 0);
	ASSERT( (c_lin % WORK_LIN_RESOLUTION) == 0);

	if (p_tw_data) {
		ASSERT(p_tw_size > 0);
		p_tw_entries += (p_tw_size  / MAX_TW_ENTRY_SIZE) + 
		    (p_tw_size % MAX_TW_ENTRY_SIZE ? 1 : 0);
		ASSERT(p_tw_entries < WORK_LIN_RESOLUTION);
		num_entries += p_tw_entries;
		parent->tw_blk_cnt = p_tw_entries;
	}

	if (c_tw_data) {
		ASSERT(c_tw_size > 0);
		c_tw_entries += (c_tw_size  / MAX_TW_ENTRY_SIZE) + 
		    (c_tw_size % MAX_TW_ENTRY_SIZE ? 1 : 0);
		ASSERT(c_tw_entries < WORK_LIN_RESOLUTION);
		num_entries += c_tw_entries;
		child->tw_blk_cnt = c_tw_entries;
	}
	
	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	/* Repstate entry */

	/* pad work item to sbt block size to avoid work item contention */
	memcpy(parent_pad, parent, sizeof(struct work_restart_state));

	bentries[i].fd = ctx->rep_fd;
	bentries[i].op_type = SBT_SYS_OP_MOD;
	lin_to_key(p_lin, &bentries[i].key);
	bentries[i].entry_buf = parent_pad;
	bentries[i].entry_size = SBT_MAX_ENTRY_SIZE_64;
	bentries[i].entry_flags = flags;
	bentries[i].cm = BT_CM_NONE;
	bentries[i].old_entry_buf = NULL;
	bentries[i].old_entry_size = 0;

	/* Only create the new item if the existing parent work item's
	   restart count hasn't been bumped by the coordinator. */
	if (check_disk) {
		/* Read the parent work item from disk */
		res = ifs_sbt_get_entry_at(ctx->rep_fd,
		    lin_to_key(p_lin, &check_key), parent_check_pad,
		    sizeof(parent_check_buf), NULL, &check_size);
		if (res == -1) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading work item lin %llx", p_lin);
			log(ERROR, "Error reading work item lin %llx: %s",
			    p_lin, strerror(tmp_errno));
			goto out;
		}

		/* Set the parent work item read from disk's coord_restarts
		 * value to what we think it should be */
		parent_check_pad->num_coord_restarts =
			parent->num_coord_restarts;

		/* These parameters will cause the bulk operation to fail if
		 * the on-disk version of the parent work item's
		 * num_coord_restarts value does not match what we have in
		 * memory */
		bentries[i].cm = BT_CM_BUF;
		bentries[i].old_entry_buf = parent_check_pad;
		bentries[i].old_entry_size = check_size;
	}

	i++;
	if (p_tw_data) {
		add_tw_entries(ctx, p_lin, i, p_tw_entries, bentries,
		    p_tw_data, p_tw_size, &error);
		if (error)
			goto out;
	}

	i += p_tw_entries;

	/* pad work item to sbt block size to avoid work item contention */
	memcpy(child_pad, child, sizeof(struct work_restart_state));

	bentries[i].fd = ctx->rep_fd;
	bentries[i].op_type = SBT_SYS_OP_ADD;
	lin_to_key(c_lin, &(bentries[i].key));
	bentries[i].entry_buf = child_pad;
	bentries[i].entry_size = SBT_MAX_ENTRY_SIZE_64;
	bentries[i].entry_flags = flags;
	bentries[i].cm = BT_CM_NONE;
	bentries[i].old_entry_buf = NULL;
	bentries[i].old_entry_size = 0;

	if (c_tw_data) {
		i++;
		add_tw_entries(ctx, c_lin, i, c_tw_entries, bentries,
		    c_tw_data, c_tw_size, &error);
		if (error)
			goto out;
	}

	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1) {
		tmp_errno = errno;
		if (check_disk && tmp_errno == ERANGE) {
			error = isi_system_error_new(tmp_errno,
			    "Unable to update work item lin %llx due "
			    "to conflicting num_coord_restarts", p_lin);
			log(ERROR, "Unable to update work item lin %llx due "
			    "to conflicting num_coord_restarts", p_lin);
		} else {
			error = isi_system_error_new(tmp_errno,
			    "Error setting work item lin %llx", p_lin);
			log(ERROR, "Error setting work item lin %llx: %s",
			    p_lin, strerror(tmp_errno));
		}
		goto out;
	} else {
		log(DEBUG, "work items updated for lin %llx", p_lin);
	}

out:
	free(bentries);

	isi_error_handle(error, error_out);
}

bool
remove_entry(u_int64_t lin, struct rep_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	struct isi_error *error = NULL;
	bool exists;

	log(TRACE, "remove_entry");

	res = ifs_sbt_remove_entry(ctx->rep_fd, lin_to_key(lin, &key));
	if (res == -1) {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing repstate lin %llx", lin);
			log(ERROR, "Error removing repstate lin %llx: %s",
			    lin, strerror(tmp_errno));
			goto out;
		}
	} else {
		exists = true;
		log(DEBUG, "removed repstate entry for lin %llx", lin);
	}

out:
	isi_error_handle(error, error_out);
	return exists;
}

void
remove_work_entry(u_int64_t wi_lin, struct work_restart_state *work,
    struct rep_ctx *ctx, struct isi_error **error_out)
{
	log(TRACE, "remove_work_entry");
	remove_work_entry_cond(wi_lin, work, ctx, false, error_out);
}

void
remove_work_entry_cond(u_int64_t wi_lin, struct work_restart_state *work,
    struct rep_ctx *ctx, bool check_disk, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 1;
	size_t size = 0;
	btree_key_t key;
	char buf[SBT_MAX_ENTRY_SIZE_64];
	struct work_restart_state *wrs_pad =
	    (struct work_restart_state *) buf;
	int i;

	log(TRACE, "remove_work_entry_cond");

	ASSERT(work != NULL);
	ASSERT(wi_lin > 0);
	ASSERT( (wi_lin % WORK_LIN_RESOLUTION) == 0);

	num_entries += work->tw_blk_cnt;

	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	for (i = 0; i < num_entries; i++) {
		/* Repstate entry */
		bentries[i].fd = ctx->rep_fd;
		bentries[i].op_type = SBT_SYS_OP_DEL;
		lin_to_key(wi_lin + i, &bentries[i].key);
	}

	if (check_disk) {
		/* Read the work item from disk and use all of the values
		 * from the disk version with the restart count of that
		 * in memory as a check to see if there is a difference
		 * in restart counts between disk and memory. */
		res = ifs_sbt_get_entry_at(ctx->rep_fd,
		    lin_to_key(wi_lin, &key), wrs_pad, sizeof(buf), NULL,
		    &size);
		if (res == -1) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading work item lin %llx", wi_lin);
			log(ERROR, "Error reading work item lin %llx: %s",
			    wi_lin, strerror(tmp_errno));
			goto out;
		}

		wrs_pad->num_coord_restarts = work->num_coord_restarts;

		bentries[0].cm = BT_CM_BUF;
		bentries[0].old_entry_buf = wrs_pad;
		bentries[0].old_entry_size = size;
	}

	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1) {
		if (check_disk && errno == ERANGE) {
			/* Mask the error since the new coordinator will
			 * just redo the last few seconds of work that
			 * this pworker just did. */
			log(DEBUG, "Unable to delete work item lin %llx due "
			   "to conflicting num_coord_restarts", wi_lin);
		} else {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing work item lin %llx", wi_lin);
			log(ERROR, "Error removing work item lin %llx: %s",
			    wi_lin, strerror(tmp_errno));
		}
		goto out;
	} else {
		log(DEBUG, "work item %llx removed", wi_lin);
	}

 out:
	free(bentries);
	
	isi_error_handle(error, error_out);
	return;
}

bool
set_job_phase_cond(struct rep_ctx *ctx, u_int64_t phase_lin,
    char *jstate, size_t jstate_size, char *old_jstate,
    size_t old_jstate_size, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	bool success = false;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 1;
	struct btree_flags flags = {};
	int i = 0;

	log(TRACE, "set_job_phase");
	ASSERT(jstate != NULL);
	ASSERT(phase_lin > 0);

	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	exists = entry_exists(phase_lin, ctx, &error);
	if (error)
		goto out;

	/* Repstate entry */
	bentries[i].fd = ctx->rep_fd;
	if (exists) {
		bentries[i].op_type = SBT_SYS_OP_MOD;
		bentries[i].cm = BT_CM_BUF;
		bentries[i].old_entry_buf = old_jstate;	
		bentries[i].old_entry_size = old_jstate_size;
	} else {
		bentries[i].op_type = SBT_SYS_OP_ADD;
		bentries[i].cm = BT_CM_NONE;
		bentries[i].old_entry_buf = NULL;
		bentries[i].old_entry_size = 0;
	}
	lin_to_key(phase_lin, &(bentries[i].key));

	bentries[i].entry_buf = jstate;
	bentries[i].entry_size = jstate_size;
	bentries[i].entry_flags = flags;

	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting job phase lin %llx", phase_lin);
		log(ERROR, "Error %d setting job phase lin %llx",
		    tmp_errno, phase_lin);
	} else if (!res) {
		success = true;
		log(DEBUG, "job phase lin %llx updated ", phase_lin);
	}

 out:
	free(bentries);
	
	isi_error_handle(error, error_out);
	return success;
}

bool
update_job_phase(struct rep_ctx *ctx, u_int64_t phase_lin,
    char *jstate, size_t jstate_size, char *oldjstate, size_t old_jstate_size,
    u_int64_t wi_lin, struct work_restart_state *work, 
    void *tw_data, size_t tw_size, bool job_init,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	bool exists;
	bool success = false;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry *bentries = NULL;
	int num_entries = 2;
	int tw_entries = 0;
	struct btree_flags flags = {};
	int i;
	char buf[SBT_MAX_ENTRY_SIZE_64] = {};
	struct work_restart_state *wrs_pad =
	    (struct work_restart_state *)buf;

	log(TRACE, "update_job_phase");
	ASSERT(jstate != NULL && work != NULL);
	ASSERT(phase_lin > 0 && wi_lin > 0);
	ASSERT( (wi_lin % WORK_LIN_RESOLUTION) == 0);

	if (tw_data) {
		ASSERT(tw_size > 0);
		tw_entries = (tw_size  / MAX_TW_ENTRY_SIZE) + 
		    (tw_size % MAX_TW_ENTRY_SIZE ? 1 : 0);
		ASSERT(tw_entries < WORK_LIN_RESOLUTION);
		work->tw_blk_cnt = tw_entries;
		num_entries += tw_entries;
	}
	
	bentries = malloc(sizeof(struct sbt_bulk_entry) * num_entries);
	ASSERT(bentries != NULL);

	memset(bentries, 0, num_entries * sizeof(struct sbt_bulk_entry));

	/* pad work item to sbt block size to avoid work item contention */
	memcpy(wrs_pad, work, sizeof(struct work_restart_state));

	/* Repstate entry */
	bentries[0].fd = ctx->rep_fd;
	bentries[0].op_type = SBT_SYS_OP_ADD;
	lin_to_key(wi_lin, &bentries[0].key);
	bentries[0].entry_buf = wrs_pad;
	bentries[0].entry_size = SBT_MAX_ENTRY_SIZE_64;
	bentries[0].entry_flags = flags;
	bentries[0].cm = BT_CM_NONE;
	bentries[0].old_entry_buf = NULL;
	bentries[0].old_entry_size = 0;

	if (tw_data) {
		add_tw_entries(ctx, wi_lin, 1, num_entries, bentries,
		    tw_data, tw_size, &error);
		if (error)
			goto out;
	}

	/* If child work exists, then only update it */
	exists = entry_exists(phase_lin, ctx, &error);
	if (error)
		goto out;
	ASSERT((job_init && !exists) || (!job_init && exists),
	    "job_init: %d, exists: %d", job_init, exists);

	i = num_entries - 1;

	bentries[i].fd = ctx->rep_fd;
	if (exists) {
		bentries[i].op_type = SBT_SYS_OP_MOD;
		bentries[i].cm = BT_CM_BUF;
		bentries[i].old_entry_buf = oldjstate;
		bentries[i].old_entry_size = old_jstate_size;
	} else {
		bentries[i].op_type = SBT_SYS_OP_ADD;
		bentries[i].cm = BT_CM_NONE;
		bentries[i].old_entry_buf = NULL;
		bentries[i].old_entry_size = 0;
	}

	lin_to_key(phase_lin, &(bentries[i].key));
	
	bentries[i].entry_buf = jstate;
	bentries[i].entry_size = jstate_size;
	bentries[i].entry_flags = flags;

	/* Execute transaction */
	res = ifs_sbt_bulk_op(num_entries, bentries);
	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting job phase lin %llx", phase_lin);
		log(ERROR, "Error %d setting job phase lin %llx",
		    tmp_errno, phase_lin);
	} else if (!res) {
		success = true;
		log(DEBUG, "job phase lin %llx updated ", phase_lin);
	}

 out:
	free(bentries);
	
	isi_error_handle(error, error_out);
	return success;
}

bool
stf_job_phase_cond_enable( uint64_t phase_lin, enum stf_job_phase phase,
    struct rep_ctx *ctx, struct isi_error **error_out)
{

	struct stf_job_state jstate = {};
	char *old_jbuf = NULL, *jbuf = NULL;
	size_t jsize, old_jsize;
	struct isi_error *error = NULL;
	bool success = false;

	log(TRACE, "stf_job_phase_cond_enable");
	old_jsize = job_phase_max_size();
	old_jbuf = malloc(old_jsize);
	memset(old_jbuf, 0, old_jsize);
	get_state_entry(phase_lin, old_jbuf, &old_jsize,
	    ctx, &error);
	if (error)
		goto out;

	read_job_state(&jstate, old_jbuf, old_jsize);

	if (stf_job_phase_disabled(&jstate, phase)) {
		stf_job_phase_enable(&jstate, phase);
		jbuf = fill_job_state(&jstate, &jsize);
		success = set_job_phase_cond(ctx,
		    phase_lin, jbuf, jsize, old_jbuf,
		    old_jsize, &error);
		if (error)
			goto out;
	} else {
		success = true;
	}

out:
	if (jstate.states)
		free(jstate.states);

	if (jbuf)
		free(jbuf);

	if (old_jbuf)
		free(old_jbuf);

	isi_error_handle(error, error_out);
	return success;
}

void
refresh_job_state(struct stf_job_state *jstate, uint64_t phase_lin,
    struct rep_ctx *ctx, struct isi_error **error_out)
{

	char *jbuf = NULL;
	size_t jsize;
	struct isi_error *error = NULL;
	struct stf_job_state tstate = {};

	log(TRACE, "refresh_job_state");
	tstate = *jstate;
	ASSERT(jstate->states != NULL);
	if (jstate->states)
		free(jstate->states);
	jstate->states = NULL;

	jsize = job_phase_max_size();
	jbuf = malloc(jsize);
	memset(jbuf, 0, jsize);
	get_state_entry(phase_lin, jbuf, &jsize, ctx, &error);
	if (error)
		goto out;

	read_job_state(jstate, jbuf, jsize);

	ASSERT(jstate->cur_phase == tstate.cur_phase);
	ASSERT(jstate->jtype == tstate.jtype);

out:
	if (jbuf)
		free(jbuf);

	isi_error_handle(error, error_out);

}

bool
entry_exists(u_int64_t lin, struct rep_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	btree_key_t key;
	bool exists;
	struct isi_error *error = NULL;
	int tmp_errno;

	res = ifs_sbt_get_entry_at(ctx->rep_fd, lin_to_key(lin, &key), NULL, 0,
	    NULL, NULL);
	if (res == 0) {
		exists = true;
	} else {
		exists = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading repstate lin %llx", lin);
			log(ERROR, "Error reading repstate lin %llx: %s",
			    lin, strerror(tmp_errno));
			goto out;
		}
	}

	log(DEBUG, "entry for lin %llx %s", lin,
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

struct rep_iter *
new_rep_iter(struct rep_ctx *ctx)
{
	struct rep_iter *iter;

	log(TRACE, "new_rep_iter");

	iter = calloc(1, sizeof(struct rep_iter));
	ASSERT(iter);
	
	iter->ctx = ctx;
	iter->key.keys[0] = 0;
	iter->key.keys[1] = 0;

	return iter;
}

struct rep_iter *
new_rep_range_iter(struct rep_ctx *ctx, uint64_t lin)
{
	struct rep_iter *iter;

	log(TRACE, "new_rep_iter");

	iter = calloc(1, sizeof(struct rep_iter));
	ASSERT(iter);
	
	iter->ctx = ctx;
	iter->key.keys[0] = lin;
	iter->key.keys[1] = 0;

	return iter;
}

bool
rep_iter_next(struct rep_iter *iter, u_int64_t *lin, struct rep_entry *rep,
    struct isi_error **error_out)
{
	int res = 0;
	char buf[REPSTATE_BUF_SIZE];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	btree_key_t next_key;
	errno = 0;
	struct isi_error *error = NULL;

	log(TRACE, "rep_iter_next");

	/* last item has already been read if key was set to -1 on previous
	 * call */
	if (iter->key.keys[0] == -1)
		goto out;
	/* Return next key, skipping range being used for work item state */
	do {
		res = ifs_sbt_get_entries(iter->ctx->rep_fd, &iter->key,
		    &next_key, REPSTATE_BUF_SIZE, buf, 1, &num_out);
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
		if (ent->key.keys[0] == ROOT_LIN)
			break;
	} while ((ent->key.keys[0] == REP_INFO_LIN) ||
	    (ent->key.keys[0] >= REP_RES_RANGE_MIN &&
	    ent->key.keys[0] <= REP_RES_RANGE_MAX));

	got_one = true;
	*lin = ent->key.keys[0];
	unpack_entry(ent->buf, ent->size_out, &ent->flags, rep);

out:
	isi_error_handle(error, error_out);
	return got_one;
}

/*
 * XXX Select tree iterator , Needs some cleanup XXX
 */
bool
sel_iter_next(struct rep_iter *iter, u_int64_t *lin, struct sel_entry *sel,
    struct isi_error **error_out)
{
	int res = 0;
	char buf[512];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	btree_key_t next_key;
	errno = 0;
	struct isi_error *error = NULL;

	log(TRACE, "sel_iter_next");

	/* last item has already been read if key was set to -1 on previous
	 * call */
	if (iter->key.keys[0] == -1)
		goto out;
	/* Return next key, skipping range being used for work item state */
	do {
		res = ifs_sbt_get_entries(iter->ctx->rep_fd, &iter->key,
		    &next_key, 512, buf, 1, &num_out);
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
		if (ent->key.keys[0] == ROOT_LIN)
			break;
	} while (ent->key.keys[0] >= REP_RES_RANGE_MIN &&
	    ent->key.keys[0] <= REP_RES_RANGE_MAX);

	got_one = true;
	*lin = ent->key.keys[0];
	ASSERT(ent->size_out == sizeof(struct sel_entry));
	memcpy(sel, buf + sizeof(struct sbt_entry), sizeof(struct sel_entry));

out:
	isi_error_handle(error, error_out);
	return got_one;
}

/*
 * This iterator only traverses work item entries 
 */
bool
work_iter_next(struct rep_iter *iter, u_int64_t *lin,
    struct work_restart_state *work, struct isi_error **error_out)
{
	int res = 0;
	char buf[8192];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	size_t num_out = 0;
	bool got_one = false;
	errno = 0;
	struct isi_error *error = NULL;
	btree_key_t next_key;

	log(TRACE, "work_iter_next");

	while (!got_one) {

		if (iter->key.keys[0] == -1)
			goto out;

		res = ifs_sbt_get_entries(iter->ctx->rep_fd, &iter->key,
		    &next_key, 8192, buf, 1, &num_out);
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

		ASSERT(num_out == 1);
		ent = (struct sbt_entry *)buf;

		if (ent->key.keys[0] == STF_JOB_PHASE_LIN)
			continue;
		if (ent->key.keys[0] == REP_INFO_LIN)
			continue;
		if (ent->key.keys[0] > REP_RES_RANGE_MAX)
			break;
		if ((ent->key.keys[0] % WORK_LIN_RESOLUTION) != 0)
			continue;

		got_one = true;
		*lin = ent->key.keys[0];
		ASSERT(*lin >= REP_RES_RANGE_MIN);
		ASSERT(ent->size_out == sizeof(struct work_restart_state) ||
		   ent->size_out == SBT_MAX_ENTRY_SIZE_64);
		memcpy(work, buf + sizeof(struct sbt_entry),
		    sizeof(struct work_restart_state));
	}
out:
	isi_error_handle(error, error_out);
	return got_one;
}
void
close_rep_iter(struct rep_iter *iter)
{
	log(TRACE, "close_rep_iter");

	if (iter)
		free(iter);
}

void
get_base_rep_name(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_snap_rep_base_restore", policy);
	} else {
		sprintf(buf, "%s_snap_rep_base", policy);
	}
}

void
get_mirrored_rep_name(char *buf, const char *policy)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	sprintf(buf, "%s_snap_rep_base_mirrored", policy);
}

void
get_repeated_rep_name(char *buf, const char *policy, uint64_t snapid,
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
get_repeated_rep_pattern(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_snap_restore_", policy);
	} else {
		sprintf(buf, "%s_snap_", policy);
	}
}

void
get_changelist_rep_name(char *buf, ifs_snapid_t old_snapid,
    ifs_snapid_t new_snapid)
{
	ASSERT(buf != NULL);
	ASSERT(old_snapid > 0);
	ASSERT(new_snapid > old_snapid);
	sprintf(buf, "%llu_%llu", old_snapid, new_snapid);
}

bool
decomp_changelist_rep_name(const char *name, ifs_snapid_t *sid_old_out,
    ifs_snapid_t *sid_new_out)
{
	char c;
	bool result = false;
	ifs_snapid_t sid_old, sid_new;

	if (sscanf(name, "%llu_%llu%c", &sid_old, &sid_new, &c) == 2
	    && sid_old > 0 && sid_old < sid_new) {
		result = true;
	}

	if (!result) {
		sid_old = sid_new = INVALID_SNAPID;
	}

	*sid_old_out = sid_old;
	*sid_new_out = sid_new;

	return result;
}

void
get_select_name(char *buf, const char *policy, uint64_t snapid, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	ASSERT(snapid > 0);
	if (restore) {
		sprintf(buf, "%s_select_restore_%llu", policy, snapid);
	} else {
		sprintf(buf, "%s_select_%llu", policy, snapid);
	}
}

void
get_select_pattern(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_select_restore_", policy);
	} else {
		sprintf(buf, "%s_select_", policy);
	}
}

/*
 * Get the first work item from rep2 
 */
ifs_lin_t
get_first_work_lin(struct rep_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	char buf[8192];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	btree_key_t key, next_key;
	ifs_lin_t lin = 0;
	size_t num_out = 0;

	log(TRACE, "get_first_work_lin");

	key.keys[0] = 0;
	key.keys[1] = 0;
	
	while (!lin) {
		res = ifs_sbt_get_entries(ctx->rep_fd, &key, &next_key,
		    8192, buf, 1, &num_out);
		key = next_key;
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
		if (ent->key.keys[0] == STF_JOB_PHASE_LIN)
			continue;
		if (ent->key.keys[0] == REP_INFO_LIN)
			continue;

		if (ent->key.keys[0] > REP_RES_RANGE_MAX)
			goto out;
		if (ent->key.keys[0] < WORK_RES_RANGE_MIN)
			continue;

		if ((ent->key.keys[0] % WORK_LIN_RESOLUTION) != 0)
			continue;

		lin = ent->key.keys[0];
		log(TRACE, "First work item lin is %llx",lin);
		ASSERT(lin >= REP_RES_RANGE_MIN);
	}
out:
	isi_error_handle(error, error_out);
	return lin;
}

/*
 * This routine gets valid path inside rep directory.
 * NOTE : Currently we dont handle exclusions, predicates
 */
char *
get_valid_rep_path( uint64_t lin, ifs_snapid_t snap)
{

	struct isi_error *error = NULL;
	char *path = NULL;

	/* Now get utf8 path for the lin */
	dir_get_utf8_str_path("/ifs/", ROOT_LIN, lin, snap,
	    0, &path, &error);
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

/*
 * Looks through the repstate for work items that are from older
 * versions. If found, new work item fields will be added, initialized
 * and saved.
 */
void
upgrade_work_entries(struct rep_ctx *ctx, struct isi_error **error_out)
{
	int res = 0;
	char buf[8192];
	struct sbt_entry *ent = NULL;
	int tmp_errno = 0;
	struct isi_error *error = NULL;
	btree_key_t key, next_key;
	ifs_lin_t lin = 0;
	size_t num_out = 0;
	struct work_restart_state work;

	log(TRACE, "%s", __func__);

	key.keys[0] = 0;
	key.keys[1] = 0;

	/* Iterate through work item entries only */
	while (1) {
		res = ifs_sbt_get_entries(ctx->rep_fd, &key, &next_key,
		    8192, buf, 1, &num_out);
		key = next_key;
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
		if (ent->key.keys[0] == STF_JOB_PHASE_LIN)
			continue;
		if (ent->key.keys[0] == REP_INFO_LIN)
			continue;

		if (ent->key.keys[0] > REP_RES_RANGE_MAX)
			goto out;
		if (ent->key.keys[0] < WORK_RES_RANGE_MIN)
			continue;

		if ((ent->key.keys[0] % WORK_LIN_RESOLUTION) != 0)
			continue;

		lin = ent->key.keys[0];
		ASSERT(lin >= REP_RES_RANGE_MIN);

		/* 
		 * The patch for bug 112062 caused work items to be saved in a
		 * buffer of size SBT_MAX_ENTRY_SIZE_64. The remaining portion
		 * of the buffer (> sizeof(struct work_restart_state)) is
		 * saved as 0's. When we get saved work items, we read
		 * sizeof(struct_work_restart_state) out of the saved buffer.
		 * Therefore, we can add new fields to the end of
		 * work_restart_state and they will be initialized to 0 when
		 * existing work items are read after an upgrade. If the
		 * customer did not have this patch, we just need to re-save
		 * the work item in the SBT_MAX_ENTRY_SIZE_64 buffer.
		*/
		if (ent->size_out < SBT_MAX_ENTRY_SIZE_64) {
			ASSERT(ent->size_out <= sizeof(struct work_restart_state));
			log(DEBUG, "Upgrading work item: lin=%llu", lin);

			/*
			 * Read the existing work item into the new work item.
			 * New work item fields should be added to the end of
			 * the struct.
			 */
			memset(&work, 0, sizeof(struct work_restart_state));
			memcpy(&work, buf + sizeof(struct sbt_entry),
			    ent->size_out);

			/* Update the work item with new fields == 0 */
			set_work_entry(lin, &work, NULL, 0, ctx, &error);
			if (error)
				goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}

/*
 * Update the cur entry with the rep entry operations done
 * This routine uses old and new rep entries to figure out
 * the operations that need to be done.
 */
static void
calculate_new_rep(struct rep_entry *cur, struct rep_entry *new,
    struct rep_entry *old)
{

	int lcount_diff;
	bool lcount_set;
	bool lcount_reset;

	if (new->exception != old->exception) {
		cur->exception = new->exception;
	}

	if (new->incl_for_child != old->incl_for_child) {
		cur->incl_for_child = new->incl_for_child;
	}

	if (new->hash_computed != old->hash_computed) {
		cur->hash_computed = new->hash_computed;
	}

	if (new->non_hashed != old->non_hashed) {
		cur->non_hashed = new->non_hashed;
	}

	if (new->new_dir_parent != old->new_dir_parent) {
		cur->new_dir_parent = new->new_dir_parent;
	}

	if (new->not_on_target != old->not_on_target) {
		cur->not_on_target = new->not_on_target;
	}

	if (new->changed != old->changed) {
		cur->changed = new->changed;
	}

	if (new->flipped != old->flipped) {
		cur->flipped = new->flipped;
	}		

	lcount_set = cur->lcount_set;
	if (new->lcount_set != old->lcount_set) {
		cur->lcount_set = new->lcount_set;
	}

	lcount_reset = cur->lcount_reset;
	if (new->lcount_reset != old->lcount_reset) {
		cur->lcount_reset = new->lcount_reset;
	}

	if (new->for_path != old->for_path) {
		cur->for_path = new->for_path;
	}
	
	if (new->hl_dir_lin != old->hl_dir_lin) {
		cur->hl_dir_lin = new->hl_dir_lin;
	}

	if (new->hl_dir_offset != old->hl_dir_offset) {
		cur->hl_dir_offset = new->hl_dir_offset;
	}

	if (new->is_dir != old->is_dir) {
		cur->is_dir = new->is_dir;
	}

	/* We dont have to update lcount if the following is already set */
	if (lcount_set) 
		goto out;


	if (cur->is_dir) {
		cur->lcount = new->lcount;
	} else if (new->lcount_set && !old->lcount_set) {
		cur->lcount = new->lcount;
		cur->excl_lcount = new->excl_lcount;
	} else {
		lcount_diff = (new->lcount - old->lcount);
		ASSERT(abs(lcount_diff) <= 1);
		
		if (lcount_diff == -1) {
			if (lcount_reset) {
				log(FATAL,
				    "repstate entries are being updated "
				    "twice (probably due to coordinator "
				    "restart)");
			} else {
				ASSERT (cur->lcount > 0);
			}
		}

		/* counts are calculated based on old and new */
		cur->lcount += lcount_diff;

		lcount_diff = (new->excl_lcount - old->excl_lcount);
		ASSERT(abs(lcount_diff) <= 1);

		if (lcount_diff == -1)
			ASSERT (cur->excl_lcount > 0);

		cur->excl_lcount += lcount_diff;
	}
out:
	return;
}

/*
 *
 * Find the old rep entry and update it with new operations done using
 * the oldp and newp rep entries.
 */
bool
sync_rep_entry(struct rep_ctx *ctx, ifs_lin_t lin, struct rep_entry *oldp,
    struct rep_entry *newp, bool set_value, struct isi_error **error_out)
{

	int res;
	bool exists;
	bool success = false;
	struct isi_error *error = NULL;
	struct rep_entry old_entry = {};
	struct rep_entry new_entry = {};
	struct sbt_bulk_entry bentry = {};
	char sbt_buf[REPSTATE_BUF_SIZE] = "";
	char sbt_old_buf[REPSTATE_BUF_SIZE] = "";
	struct btree_flags sbt_flags = {};
	struct btree_flags sbt_old_flags = {};
	size_t sbt_buf_size = REPSTATE_BUF_SIZE;
	size_t sbt_old_buf_size = REPSTATE_BUF_SIZE;
	int num_entries = 1;

	ASSERT( oldp != NULL);
	ASSERT( newp != NULL);

	bzero(&bentry, sizeof(struct sbt_bulk_entry));

	/* Find the old entry (if it exists) */
	exists = get_entry(lin, &old_entry, ctx, &error);
	if (error)
		goto out;

	bentry.fd = ctx->rep_fd;
	if (exists) {
		bentry.op_type = SBT_SYS_OP_MOD;
		bentry.cm = BT_CM_BOTH;
		pack_entry(sbt_old_buf, &sbt_old_buf_size, &sbt_old_flags,
		    &old_entry);
		bentry.old_entry_buf = sbt_old_buf;
		bentry.old_entry_size = sbt_old_buf_size;
		bentry.old_entry_flags = sbt_old_flags;
		if (set_value) {
			/* Always set new value */
			new_entry = *newp;

			/* Check for bug 171325 */
			if (abs(newp->lcount - oldp->lcount) > 1) {
				log(ERROR, "%s: LIN: %{} newlc: %u "
				    "oldlc: %u", __func__, lin_fmt(lin),
				    newp->lcount, oldp->lcount);
			}
		} else {
			new_entry = old_entry;
			/* Calculate new rep value based on operation done */
			calculate_new_rep(&new_entry, newp, oldp);
		}
	} else {
		bentry.op_type = SBT_SYS_OP_ADD;
		bentry.cm = BT_CM_NONE;
		bentry.old_entry_buf = NULL;
		bentry.old_entry_size = 0;
		new_entry = *newp;
	}

	lin_to_key(lin, &bentry.key);

	pack_entry(sbt_buf, &sbt_buf_size, &sbt_flags, &new_entry);
	bentry.entry_buf = sbt_buf;
	bentry.entry_size = sbt_buf_size;
	bentry.entry_flags = sbt_flags;
	
	/* Perform the transaction */
	res = ifs_sbt_bulk_op(num_entries, &bentry);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		error = isi_system_error_new(errno,
		    "Error setting repstate lin %llx", lin);
		goto out;
	} else if (!res) {
		success = true;
		log(DEBUG, "repstate entry set for lin %llx", lin);
	}

out:
	isi_error_handle(error, error_out);
	return success;
}

static void
add_rep_entries(struct sbt_bulk_entry *rep_entries, int num_entries,
    struct rep_ctx *rep, int log_index, struct rep_log *logp,
    struct isi_error **error_out)
{
	int res;
	int i = 0;
	int j = 0;
	bool success = false;
	struct isi_error *error = NULL;

	log(TRACE, "add_rep_entries");	
	/*
	 * First lets try to add all entries.
	 * It could fail if there are concurrent updates by
	 * another workeror stale old rep entry. So in case of 
	 * bulk operation failure, we try to update entries 
	 * one transaction at a time.
	 */

	res = ifs_sbt_bulk_op(num_entries, rep_entries);
	if (res == -1 && errno != EEXIST  && errno != ERANGE ) { 
		error = isi_system_error_new(errno,
		    "Error while syncing %d repstate entries from log to disk"
		    "  with begining lin %llx", num_entries,
		    rep_entries[0].key.keys[0]);
		goto out;
	}
	
	if (res == 0)
		goto out;

	i = log_index - num_entries;
	/* Bulk transaction failed, lets try to do update one at a time */
	while (j < num_entries) {
		success = false;
		while (!success) {
			success = sync_rep_entry(rep, logp->entries[i].lin,
		 	   &(logp->entries[i].old_entry),
			    &(logp->entries[i].new_entry),
			    logp->entries[i].set_value, &error);
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
rep_compr(const void *elem1, const void *elem2)
{
	struct rep_log_entry *entry1 = (struct rep_log_entry *)elem1;
	struct rep_log_entry *entry2 = (struct rep_log_entry *)elem2;
	
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
flush_rep_log(struct rep_log *logp, struct rep_ctx *ctx,
    struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry rep_entries[SBT_BULK_MAX_OPS];
	char sbt_buf[SBT_BULK_MAX_OPS][sizeof(struct rep_entry)];
	struct btree_flags sbt_flags[SBT_BULK_MAX_OPS];
	char sbt_old_buf[SBT_BULK_MAX_OPS][sizeof(struct rep_entry)];
	struct btree_flags sbt_old_flags[SBT_BULK_MAX_OPS];
	struct lin_set lset;

	log(TRACE, "flush_rep_log");

	/* First lets sort the the rep entries */
	qsort(logp->entries, logp->index, sizeof(struct rep_log_entry),
	    rep_compr);

	bzero(rep_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	bzero(sbt_buf, SBT_BULK_MAX_OPS * sizeof(struct rep_entry));
	bzero(sbt_old_buf, SBT_BULK_MAX_OPS * sizeof(struct rep_entry));
	bzero(sbt_flags, SBT_BULK_MAX_OPS * sizeof(struct btree_flags));
	bzero(sbt_old_flags, SBT_BULK_MAX_OPS * sizeof(struct btree_flags));
	lin_set_init(&lset);

	while (i < logp->index) {
		if (lin_set_contains(&lset, logp->entries[i].lin) ||
		    (j == SBT_BULK_MAX_OPS)) {
			ASSERT (i != 0);
			ASSERT (j != 0);
			add_rep_entries(rep_entries, j, ctx, i, logp,
			    &error);
			if (error)
				goto out;
			
			bzero(rep_entries,
			    j * sizeof(struct sbt_bulk_entry));
			bzero(sbt_buf,
			    j * sizeof(struct rep_entry));
			bzero(sbt_old_buf,
			    j * sizeof(struct rep_entry));
			bzero(sbt_flags,
			    j * sizeof(struct btree_flags));
			bzero(sbt_old_flags,
			    j * sizeof(struct btree_flags));

			j = 0;
			lin_set_truncate(&lset);
		}

		add_rep_bulk_entry(ctx, logp->entries[i].lin,
		    &rep_entries[j], logp->entries[i].oper,
		    &(logp->entries[i].new_entry), sbt_buf[j],
		    &sbt_flags[j], &(logp->entries[i].old_entry),
		    sbt_old_buf[j], &sbt_old_flags[j],
		    sizeof(struct rep_entry));

		lin_set_add(&lset, logp->entries[i].lin);
		j++;
		i++;
	}

	if (j) {
		add_rep_entries(rep_entries, j, ctx, i, logp, &error);
		if (error)
			goto out;
	}

	logp->index = 0;
out:
	lin_set_clean(&lset);
	isi_error_handle(error, error_out);
}

void
copy_rep_entries(struct rep_ctx *ctx, ifs_lin_t *lins,
    struct rep_entry *entries, int num_entries, struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	int res;
	bool exists;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry bentries[SBT_BULK_MAX_OPS];
	char sbt_buf[SBT_BULK_MAX_OPS][sizeof(struct rep_entry)];
	struct btree_flags sbt_flags = {};
	size_t sbt_buf_size = sizeof(struct rep_entry);

	log(TRACE, "copy_rep_entries");

	ASSERT(num_entries <= SBT_BULK_MAX_OPS);
	ASSERT(num_entries > 0 && entries != NULL && lins != NULL);
	bzero(bentries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	bzero(sbt_buf, SBT_BULK_MAX_OPS * sizeof(struct rep_entry));

	while (i < num_entries) {
		bentries[j].fd = ctx->rep_fd;
		lin_to_key(lins[i], &(bentries[j].key));
		exists = entry_exists(lins[i], ctx, &error);
		if (error)
			goto out;
		if (entries[i].lcount == 0) {
			if (exists) {
				bentries[j].op_type = SBT_SYS_OP_DEL;
				j++;
			}
		} else {
			if (exists)
				bentries[j].op_type = SBT_SYS_OP_MOD;
			else
				bentries[j].op_type = SBT_SYS_OP_ADD;
			bentries[j].cm = BT_CM_NONE;
			sbt_buf_size = sizeof(struct rep_entry);
			bzero(&sbt_flags, sizeof(struct btree_flags));
			pack_entry(sbt_buf[j], &sbt_buf_size, &sbt_flags,
			    &entries[i]);
			ASSERT(sbt_buf_size <= sizeof(struct rep_entry));
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
			    " to base repstate", lins[0],
			    lins[num_entries - 1]);
			goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}

/*
 * Add rep_entry to in-memory log. If the log is full, then we
 * would flush the log to disk before adding new entry.
 */
void
rep_log_add_entry(struct rep_ctx *ctx, struct rep_log *logp,
    ifs_lin_t lin, struct rep_entry *new_re, struct rep_entry *old,
    enum sbt_bulk_op_type oper, bool set_value, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct rep_entry null_entry = {};

	log(TRACE, "%s", __func__);

	ASSERT(new_re || oper == SBT_SYS_OP_DEL);
	ASSERT(logp->index <= MAX_BLK_OPS);
	if (logp->index == MAX_BLK_OPS) {
		// Log is full, so we need flush it
		flush_rep_log(logp, ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}

	logp->entries[logp->index].oper = oper;
	logp->entries[logp->index].set_value = set_value;
	ASSERT(oper == SBT_SYS_OP_ADD || oper == SBT_SYS_OP_DEL || old);
	if (old) {
		logp->entries[logp->index].old_entry = *old;
	} else {
		logp->entries[logp->index].old_entry = null_entry;
	}

	logp->entries[logp->index].lin = lin;

	if (new_re)
		logp->entries[logp->index].new_entry = *new_re;

	logp->index++;

out:
	isi_error_handle(error, error_out);
}


