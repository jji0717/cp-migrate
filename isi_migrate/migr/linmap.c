#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "ifs/ifs_types.h"
#include <ifs/ifs_lin_open.h>
#include <isi_sbtree/sbtree.h>
#include "isi_migrate/migr/isirep.h"
#include "linmap.h"
#include "isi_migrate/migr/siq_btree.h"

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


/*  _     _       __  __               ____      _____              
 * | |   (_)_ __ |  \/  | __ _ _ __   | __ )    |_   _| __ ___  ___ 
 * | |   | | '_ \| |\/| |/ _` | '_ \  |  _ \ _____| || '__/ _ \/ _ \
 * | |___| | | | | |  | | (_| | |_) | | |_) |_____| || | |  __/  __/
 * |_____|_|_| |_|_|  |_|\__,_| .__/  |____/      |_||_|  \___|\___|
 *                            |_|                                   
 */

void
create_linmap(const char *policy, char *name, bool exist_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "create_linmap %s", name);

	siq_btree_create(siq_btree_linmap, policy, name, exist_ok, &error);

	isi_error_handle(error, error_out);
}

void
open_linmap(const char *policy, char *name, struct map_ctx **ctx_out,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fd = -1;

	log(TRACE, "open_linmap %s", name);

	ASSERT(ctx_out);

	fd = siq_btree_open(siq_btree_linmap, policy, name, &error);
	if (!error) {
		(*ctx_out) = calloc(1, sizeof(struct map_ctx));
		ASSERT(*ctx_out);
		(*ctx_out)->map_fd = fd;
		fd = -1;
	}

	isi_error_handle(error, error_out);
}

bool
linmap_exists(const char *policy, char *name, struct isi_error **error_out)
{
	struct map_ctx *ctx = NULL;
	struct isi_error *error = NULL;
	bool exists = false;

	open_linmap(policy, name, &ctx, &error);
	if (!error) {
		exists = true;
		close_linmap(ctx);
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
close_linmap(struct map_ctx *ctx)
{
	log(TRACE, "close_linmap");

	if (ctx == NULL)
		return;

	if (ctx->map_fd > 0)
		close(ctx->map_fd);

	free(ctx);
}

void
remove_linmap(const char *policy, char *name, bool missing_ok,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	siq_btree_remove(siq_btree_linmap, policy, name, missing_ok, &error);

	isi_error_handle(error, error_out);
}

void
set_dynamic_lmap_lookup(struct map_ctx *ctx, ifs_domainid_t domain_id)
{
	ctx->dynamic = true;
	ctx->domain_id = domain_id;
}

void
set_comp_map_lookup(struct map_ctx *ctx,
    struct compliance_map_ctx *comp_map_ctx)
{
	ASSERT(comp_map_ctx != NULL, "Null compliance map context");
	ctx->comp_map_ctx = comp_map_ctx;
}

void
get_lmap_name(char *buf, const char *policy, bool restore)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	if (restore) {
		sprintf(buf, "%s_restore", policy);
	} else {
		sprintf(buf, "%s", policy);
	}
}

void
get_mirrored_lmap_name(char *buf, const char *policy)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	sprintf(buf, "%s_mirrored", policy);
}

void
get_tmp_working_map_name(char *buf, const char *policy)
{
	ASSERT(buf != NULL);
	ASSERT(policy != NULL);
	sprintf(buf, "%s_tmp_working_map", policy);
}

void
set_mapping(uint64_t slin, uint64_t dlin, struct map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno;
	bool exists;
	btree_key_t key;
	uint64_t prev_dlin;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	exists = get_mapping(slin, &prev_dlin, ctx, &error);
	if (error)
		goto out;

	if (exists) {
		if (prev_dlin == dlin) // nothing to be done
			goto out;
		log(NOTICE, "replacing linmap entry for slin: %llx "
		    "old dlin: %llx new dlin: %llx", slin, prev_dlin, dlin);
		res = ifs_sbt_mod_entry(ctx->map_fd, lin_to_key(slin, &key),
		    &dlin, sizeof(uint64_t), NULL);
	} else
		res = ifs_sbt_add_entry(ctx->map_fd, lin_to_key(slin, &key),
		    &dlin, sizeof(uint64_t), NULL);

	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting mapping %llx->%llx", slin, dlin);
		log(ERROR, "Error setting mapping lin %llx->%llx: %s",
		    slin, dlin, strerror(tmp_errno));
	} else
		log(DEBUG, "Mapping set %llx->%llx", slin, dlin);

out:
	isi_error_handle(error, error_out);
}

/**
 * Return true if modded, false if not modded due to the key not existing or
 * the conditional mod failing.
 */
bool
set_mapping_cond(uint64_t slin, uint64_t dlin, struct map_ctx *ctx,
    bool is_new, uint64_t old_dlin, struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	struct isi_error *error = NULL;
	bool success = false;

	log(TRACE, "%s", __func__);

	if (!is_new)
		res = ifs_sbt_cond_mod_entry(ctx->map_fd,
		    lin_to_key(slin, &key), &dlin, sizeof(uint64_t),
		    NULL, BT_CM_BUF, (old_dlin == 0) ? NULL : &old_dlin,
		    sizeof(uint64_t), NULL);
	else
		res = ifs_sbt_add_entry(ctx->map_fd, lin_to_key(slin, &key),
		    &dlin, sizeof(uint64_t), NULL);

	if (res == -1 && errno != EEXIST && errno != ENOENT &&
	    errno != ERANGE) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error setting mapping %llx->%llx", slin, dlin);
		log(ERROR, "Error setting mapping %llx->%llx: %s",
		    slin, dlin, strerror(tmp_errno));
	} else if (!res) {
		success = true;
		log(DEBUG, "Mapping set %llx->%llx", slin, dlin);
	}

	isi_error_handle(error, error_out);
	return success;
}

static void
get_dynamic_lmap_entry(struct map_ctx *ctx, ifs_lin_t slin, ifs_lin_t *dlin,
    bool *outside_domain, struct isi_error **error_out)
{

	int fd = -1;
	struct stat st;
	int res = -1;
	struct isi_error *error = NULL;
	struct domain_set doms = {}, ancestors = {};

	if (outside_domain)
		*outside_domain = false;

	/*
	 * Even if it is found in snap version, its possible that
	 * it could already be deleted. Make sure we double check 
	 * the lin by looking at nlink value.
	 */
	fd = ifs_lin_open(slin, HEAD_SNAPID, O_RDONLY);
	ASSERT(fd != 0);
	if (fd < 0 && (errno != ESTALE && errno != ENOENT)) {
		error = isi_system_error_new(errno,
		    "Unable to open lin %llx", slin);
		goto out;
	}
	if (fd == -1) {
		/* We just found out that its missing in snap */
		goto out;
	}

	res = fstat(fd, &st);
	if (res == -1) {
		error = isi_system_error_new(errno,
		 "Unable to fstat lin %llx\n", slin);
		goto out;
	}

	/*
	 * If the link count is not greater than 0, then file not present
	 * Or if its ADS file,  or domain ID is not set on the head version,
	 * then we do not keep entry in repstate
	 */
	if (st.st_nlink <= 0 || (st.st_flags & UF_ADS)) {
		goto out;
	}

	/*
	 * If there is valid domain then make sure the lin 
	 * belongs to the domain. Otherwise we will recreate
	 * them and copy the data.
	 */
	if (ctx->domain_id) {
		/* Check if the domain ID matches with one stored in context */
		res = dom_get_info_by_lin(slin, HEAD_SNAPID, &doms,
		    &ancestors, NULL);
		if (res != 0) {
			error = isi_system_error_new(errno,
			    "Unable to get domain IDs for lin %llx", slin);
			goto out;
		}

		if (!domain_set_contains(&doms, ctx->domain_id)) {
			if (outside_domain)
				*outside_domain = true;
			goto out;
		}
	}

	*dlin = slin;

out:
	if (fd > 0)
		close(fd);
	isi_error_handle(error, error_out);
}

/*
 * Lookup for compliance map for lin mapping.
 */
static void
comp_map_lookup(struct map_ctx *ctx,
    ifs_lin_t *dlin, struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	bool found = false;
	ifs_lin_t last_dlin = *dlin;
	ifs_lin_t cur_lin = 0;
	ifs_lin_t tmp_lin = 0;
	int count = 0;

	if (!ctx->comp_map_ctx)
		goto out;

	ASSERT(last_dlin > 0);

	/*
	 * Lookup up the comp map until we reach end of map chain.
	 */
	do {
		found = get_compliance_mapping(last_dlin, &tmp_lin,
		    ctx->comp_map_ctx, &error);
		if (error)
			goto out;
		if (!found)
			break;
		last_dlin = tmp_lin;
		cur_lin = tmp_lin;
		count++;
		if (count >= MAX_COMP_MAP_LOOKUP_LOOP) {
			error = isi_system_error_new(EINVAL,
			    "Probably invalid comp map chain for lin %{}",
			    lin_fmt(*dlin));
			goto out;
		}
	} while(1);

	if (cur_lin > 0) {
		log(DEBUG, "Got comp maping for lin %{}", lin_fmt(*dlin));
		*dlin = cur_lin;
	}
out:
	isi_error_handle(error, error_out);
}

bool
get_mapping_with_domain_info(uint64_t slin, uint64_t *dlin, struct map_ctx *ctx,
    bool *outside_domain, struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	bool found = false;
	int tmp_errno;
	btree_key_t key;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->map_fd, lin_to_key(slin, &key), dlin,
	    sizeof(uint64_t), NULL, &size);

	if (res == -1) {
		*dlin = 0;
		found = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading linmap lin %llx", slin);
			log(ERROR, "Error reading linmap lin %llx: %s",
			    slin, strerror(tmp_errno));
		}
		if (ctx->dynamic) {
			get_dynamic_lmap_entry(ctx, slin, dlin, outside_domain,
			    &error);
			if (error)
				goto out;
			if (*dlin > 0)
				found = true;
		}
	} else {
		if (!ctx->dynamic) {
			comp_map_lookup(ctx, dlin, &error);
			if (error)
				goto out;
		}
		found = true;
		log(DEBUG, "Got mapping for lin %llx", slin);
	}
out:
	isi_error_handle(error, error_out);
	return found;
}

bool
get_mapping(uint64_t slin, uint64_t *dlin, struct map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	size_t size = 0;
	bool found = false;
	int tmp_errno;
	btree_key_t key;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	res = ifs_sbt_get_entry_at(ctx->map_fd, lin_to_key(slin, &key), dlin,
	    sizeof(uint64_t), NULL, &size);

	if (res == -1) {
		*dlin = 0;
		found = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading linmap lin %llx", slin);
			log(ERROR, "Error reading linmap lin %llx: %s",
			    slin, strerror(tmp_errno));
		}
		if (ctx->dynamic) {
			get_dynamic_lmap_entry(ctx, slin, dlin, NULL, &error);
			if (error)
				goto out;
			if (*dlin > 0)
				found = true;
		}
	} else {
		if (!ctx->dynamic) {
			comp_map_lookup(ctx, dlin, &error);
			if (error)
				goto out;
		}
		found = true;
		log(DEBUG, "Got mapping for lin %llx", slin);
	}
out:
	isi_error_handle(error, error_out);
	return found;
}

bool
remove_mapping(uint64_t slin, struct map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	int tmp_errno = 0;
	btree_key_t key;
	bool found;
	struct isi_error *error = NULL;

	log(TRACE, "remove_mapping");

	res = ifs_sbt_remove_entry(ctx->map_fd, lin_to_key(slin, &key));

	if (res == -1) {
		found = false;
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error removing mapping lin %llx", slin);
			log(ERROR, "Error removing mapping lin %llx: %s",
			    slin, strerror(tmp_errno));
		}
	} else {
		found = true;
		log(DEBUG, "Removed mapping for lin %llx", slin);
	}

	isi_error_handle(error, error_out);
	return found;
}

bool
mapping_exists(uint64_t slin, struct map_ctx *ctx,
    struct isi_error **error_out)
{
	int res = 0;
	btree_key_t key;
	bool exists = false;
	struct isi_error *error = NULL;
	int tmp_errno;

	res = ifs_sbt_get_entry_at(ctx->map_fd, lin_to_key(slin, &key), NULL, 0,
	    NULL, NULL);
	if (res == 0)
		exists = true;
	else {
		if (errno != ENOENT) {
			tmp_errno = errno;
			error = isi_system_error_new(tmp_errno,
			    "Error reading linmap lin %llx", slin);
			log(ERROR, "Error reading linmap lin %llx: %s",
			    slin, strerror(tmp_errno));
		}
	}

	log(DEBUG, "mapping for lin %llx %s", slin,
	    (exists) ? "exists" : "does not exist");

	isi_error_handle(error, error_out);
	return exists;
}

bool
linmap_empty(struct map_ctx *ctx, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct map_iter *iter;
	bool found = true;
	uint64_t slin, dlin;

	iter = new_map_iter(ctx);
	found = map_iter_next(iter, &slin, &dlin, &error);

	isi_error_handle(error, error_out);
	close_map_iter(iter);
	return !found;
}

/*  ___ _                 _             
 * |_ _| |_ ___ _ __ __ _| |_ ___  _ __ 
 *  | || __/ _ \ '__/ _` | __/ _ \| '__|
 *  | || ||  __/ | | (_| | || (_) | |   
 * |___|\__\___|_|  \__,_|\__\___/|_|   
 */

struct map_iter *
new_map_iter(struct map_ctx *ctx)
{
	struct map_iter *iter;

	log(TRACE, "new_map_iter");

	iter = calloc(1, sizeof(struct map_iter));
	ASSERT(iter);

	iter->ctx = ctx;
	iter->key.keys[0] = 0;
	iter->key.keys[1] = 0;

	return iter;
}

bool
map_iter_next(struct map_iter *iter, uint64_t *slin, uint64_t *dlin,
    struct isi_error **error_out)
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

	log(TRACE, "map_iter_next");

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
	*slin = ent->key.keys[0];
	ASSERT(ent->size_out == sizeof(uint64_t));
	memcpy(dlin, buf + sizeof(struct sbt_entry), sizeof(uint64_t));

out:
	if (res == -1) {
		tmp_errno = errno;
		error = isi_system_error_new(tmp_errno,
		    "Error getting next mapping");
		log(ERROR, "Error getting next mapping: %s",
		    strerror(tmp_errno));
	}

	isi_error_handle(error, error_out);
	return got_one;
}

void
close_map_iter(struct map_iter *iter)
{
	log(TRACE, "close_map_iter");

	if (iter)
		free(iter);
}

static void
sync_lmap_entry( struct map_ctx *ctx, uint64_t slin, uint64_t dlin,
    enum lmap_oper oper, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (oper == LMAP_SET) {
		set_mapping(slin, dlin, ctx, &error);
		if (error)
			goto out;
	} else if (oper == LMAP_REM) {
		remove_mapping(slin, ctx, &error);
		if (error)
			goto out;
	}
out:
	isi_error_handle(error, error_out);
}

static void
update_lmap_entries(struct sbt_bulk_entry *lmap_entries, int num_entries,
    struct map_ctx *ctx, int log_index, struct lmap_log *logp,
    struct isi_error **error_out)
{
	int res;
	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;

	log(TRACE, "update_lmap_entries");
	/*
	 * First lets try to add all entries. In case of 
	 * bulk operation failure, we try to update entries 
	 * one transaction at a time.
	 */

	res = ifs_sbt_bulk_op(num_entries, lmap_entries);
	if (res == -1 && errno != EEXIST  && errno != ERANGE  &&
	    errno != ENOENT) { 
		error = isi_system_error_new(errno,
		    "Error while syncing %d lmap entries from log to disk"
		    "  with begining lin %llx", num_entries,
		    lmap_entries[0].key.keys[0]);
		goto out;
	}

	if (res == 0)
		goto out;

	i = log_index - num_entries;
	/* Bulk transaction failed, lets try to do update one at a time */
	while (j < num_entries) {
		sync_lmap_entry(ctx, logp->entries[i].slin,
		    logp->entries[i].dlin, logp->entries[i].oper,
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
lmap_compr(const void *elem1, const void *elem2)
{
	struct lmap_log_entry *entry1 = (struct lmap_log_entry *)elem1;
	struct lmap_log_entry *entry2 = (struct lmap_log_entry *)elem2;

	if (entry1->slin < entry2->slin) {
		return -1;
	} else if (entry1->slin == entry2->slin) {
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
flush_lmap_log(struct lmap_log *logp, struct map_ctx *ctx,
    struct isi_error **error_out)
{

	int i = 0;
	int j = 0;
	struct isi_error *error = NULL;
	struct sbt_bulk_entry lmap_entries[SBT_BULK_MAX_OPS];
	struct lin_set lset;
	struct btree_flags flags = {};

	log(TRACE, "flush_lmap_log");

	/* First lets sort the the rep entries */
	qsort(logp->entries, logp->index, sizeof(struct lmap_log_entry),
	    lmap_compr);

	bzero(lmap_entries, sizeof(struct sbt_bulk_entry) * SBT_BULK_MAX_OPS);
	lin_set_init(&lset);

	while (i < logp->index) {

		if (lin_set_contains(&lset, logp->entries[i].slin) ||
		    (j == SBT_BULK_MAX_OPS)) {
			ASSERT (i != 0);
			ASSERT (j != 0);
			update_lmap_entries(lmap_entries, j, ctx, i, logp,
			    &error);
			if (error)
				goto out;

			j = 0;
			lin_set_truncate(&lset);
		}

		lmap_entries[j].fd = ctx->map_fd;
		lin_to_key(logp->entries[i].slin, &(lmap_entries[j].key));
		lmap_entries[j].cm = BT_CM_NONE;
		if (logp->entries[i].oper == LMAP_SET) {
			lmap_entries[j].op_type = SBT_SYS_OP_ADD;
			lmap_entries[j].entry_buf = &(logp->entries[i].dlin);
			lmap_entries[j].entry_size = sizeof(uint64_t);
			lmap_entries[j].entry_flags = flags;
		} else if (logp->entries[i].oper == LMAP_REM) {
			lmap_entries[j].op_type = SBT_SYS_OP_DEL;
		} else {
			ASSERT(0);
		}

		lin_set_add(&lset, logp->entries[i].slin);
		j++;
		i++;
	}

	if (j) {
		update_lmap_entries(lmap_entries, j, ctx, i, logp, &error);
		if (error)
			goto out;
	}

	logp->index = 0;
out:
	lin_set_clean(&lset);
	isi_error_handle(error, error_out);
}

/*
 * Add linmap entry to in-memory log. If the log is full, then we 
 * would flush the log to disk before adding new entry.
 */
void
lmap_log_add_entry(struct map_ctx *ctx, struct lmap_log *logp, ifs_lin_t slin,
    ifs_lin_t dlin, enum lmap_oper oper, int source_fd, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "lmap_log_add_entry");
	ASSERT(logp != NULL);
	ASSERT(slin != 0);
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

		flush_lmap_log(logp, ctx, &error);
		if (error)
			goto out;
		ASSERT(logp->index == 0);
	}

	logp->entries[logp->index].slin = slin;
	logp->entries[logp->index].dlin = dlin;
	logp->entries[logp->index].oper = oper;
	ASSERT(oper == LMAP_REM || dlin > 0);

	logp->index++;

out:
	isi_error_handle(error, error_out);
}
